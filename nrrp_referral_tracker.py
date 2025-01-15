import discord
from discord.ext import commands
import sqlite3
from datetime import datetime
import asyncio
from typing import Optional
from discord.ext.commands import dm_only, cooldown, BucketType, CommandOnCooldown
import json
# Configuration
import os
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import traceback

# Set up logging with rotation
handler = RotatingFileHandler(
    '/var/log/referral-tracker.log',
    maxBytes=10000000,  # 10MB
    backupCount=5
)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

logger = logging.getLogger('ReferralTracker')
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler())  # Also output to console


load_dotenv()

# Bot configuration
intents = discord.Intents.default()
intents.members = True
intents.invites = True
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# IDs Configuration
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
GUILD_ID = int(os.getenv('GUILD_ID'))
COMMANDS_CHANNEL_ID = int(os.getenv('COMMANDS_CHANNEL_ID'))
LEADERBOARD_CHANNEL_ID = int(os.getenv('LEADERBOARD_CHANNEL_ID'))
LOGS_CHANNEL_ID = int(os.getenv('LOGS_CHANNEL_ID'))

def check_channel(ctx):
    """Check if command is used in allowed channels"""
    # Only validate command needs channel check now
    return ctx.channel.id in [COMMANDS_CHANNEL_ID, LEADERBOARD_CHANNEL_ID]

# Store invite cache and last leaderboard message
invite_cache = {}
last_leaderboard_message: Optional[discord.Message] = None

# Database setup
def setup_database():
    logger.info("Setting up database...")
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    try:
        # Enable foreign keys and set optimal journal mode
        c.execute('PRAGMA foreign_keys = ON')
        c.execute('PRAGMA journal_mode = DELETE')
        c.execute('PRAGMA auto_vacuum = FULL')
        
        # Create audit_log table if it doesn't exist
        c.execute('''CREATE TABLE IF NOT EXISTS audit_log
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      event_type TEXT,
                      event_data TEXT,
                      timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # Check if referrals table exists
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='referrals'")
        table_exists = c.fetchone() is not None
        
        if not table_exists:
            logger.info("Creating referrals table...")
            c.execute('''CREATE TABLE IF NOT EXISTS referrals
                         (inviter_id TEXT,
                          inviter_name TEXT,
                          invited_id TEXT,
                          invited_name TEXT,
                          invite_code TEXT,
                          joined_at TIMESTAMP,
                          is_validated BOOLEAN DEFAULT FALSE,
                          has_resident_role BOOLEAN DEFAULT FALSE,
                          is_member_active BOOLEAN DEFAULT TRUE,
                          was_previous_resident BOOLEAN DEFAULT FALSE)''')
        else:
            # Check and add any missing columns
            c.execute("PRAGMA table_info(referrals)")
            columns = [column[1] for column in c.fetchall()]
            
            if 'was_previous_resident' not in columns:
                logger.info("Adding was_previous_resident column...")
                c.execute('ALTER TABLE referrals ADD COLUMN was_previous_resident BOOLEAN DEFAULT FALSE')
                
                c.execute('''UPDATE referrals
                             SET was_previous_resident = (
                                 SELECT CASE WHEN EXISTS (
                                     SELECT 1 FROM member_history 
                                     WHERE member_history.member_id = referrals.invited_id 
                                     AND had_resident = TRUE
                                     AND action = 'leave'
                                 ) THEN TRUE ELSE FALSE END
                             )''')
        
        # Create member_history table
        c.execute('''CREATE TABLE IF NOT EXISTS member_history
                     (member_id TEXT,
                      member_name TEXT,
                      action TEXT,
                      timestamp TIMESTAMP,
                      had_resident BOOLEAN)''')
        
        # Perform maintenance
        logger.info("Performing database maintenance...")
        c.execute('ANALYZE')
        conn.commit()
        
        # Vacuum the database to reclaim space
        logger.info("Vacuuming database...")
        c.execute('VACUUM')
        
        conn.commit()
        logger.info("Database setup and maintenance complete")
        
    except sqlite3.Error as e:
        logger.error(f"Database setup error: {e}")
        conn.rollback()
    finally:
        conn.close()

def get_database_size():
    """Get the current size of the database file in KB"""
    try:
        import os
        size = os.path.getsize('referrals.db')
        return size / 1024
    except OSError as e:
        logger.error(f"Error getting database size: {e}")
        return None

async def perform_maintenance():
    """Perform periodic database maintenance"""
    try:
        conn = sqlite3.connect('referrals.db')
        c = conn.cursor()
        
        # Log the size before maintenance
        size_before = get_database_size()
        logger.info(f"Database size before maintenance: {size_before:.2f}KB")
        
        # Run maintenance operations
        c.execute('ANALYZE')
        conn.commit()
        
        # Vacuum to reclaim space
        c.execute('VACUUM')
        conn.commit()
        
        # Log the size after maintenance
        size_after = get_database_size()
        logger.info(f"Database size after maintenance: {size_after:.2f}KB")
        
        if size_before and size_after:
            space_saved = size_before - size_after
            logger.info(f"Space reclaimed: {space_saved:.2f}KB")
            
    except sqlite3.Error as e:
        logger.error(f"Maintenance error: {e}")
    finally:
        conn.close()

# Helper function to log audit events
async def log_audit_event(event_type: str, event_data: dict, severity: str = 'INFO'):
    """
    Log audit events to the database
    
    Args:
        event_type (str): Type of event (e.g., 'MEMBER_JOIN', 'REFERRAL_VALIDATE')
        event_data (dict): Detailed information about the event
        severity (str): Logging severity level
    """
    try:
        conn = sqlite3.connect('referrals.db')
        c = conn.cursor()
        c.execute('''INSERT INTO audit_log 
                     (event_type, event_data, severity) 
                     VALUES (?, ?, ?)''', 
                  (event_type, json.dumps(event_data), severity))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Failed to log audit event: {e}")
    finally:
        conn.close()


@bot.command(name='resethistory')
@commands.has_permissions(administrator=True)
async def reset_member_history(ctx):
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    # Drop and recreate the table
    c.execute('DROP TABLE IF EXISTS member_history')
    c.execute('''CREATE TABLE IF NOT EXISTS member_history
                 (member_id TEXT,
                  member_name TEXT,
                  action TEXT,
                  timestamp TIMESTAMP,
                  had_resident BOOLEAN)''')
    
    conn.commit()
    conn.close()
    
    # Run the population
    guild = ctx.guild
    await populate_member_history(guild)
    
    await ctx.send("✅ Member history has been reset and repopulated with current Residents!")

async def populate_member_history(guild):
    logger.info("Starting member_history population...")
    
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    # Check if we've already populated the history
    c.execute('SELECT COUNT(*) FROM member_history')
    if c.fetchone()[0] > 0:
        logger.info("Member history already populated, skipping...")
        conn.close()
        return
    
    # Get the Resident role
    resident_role = discord.utils.get(guild.roles, name='Resident')
    if not resident_role:
        logger.error("'Resident' role not found!")
        conn.close()
        return
    
    # Get current timestamp
    current_time = datetime.now()
    count = 0
    
    # Only add members who have Resident role
    for member in guild.members:
        if resident_role in member.roles:
            logger.debug(f"Adding Resident {member.name} to history")
            c.execute('''INSERT INTO member_history 
                        (member_id, member_name, action, timestamp, had_resident)
                        VALUES (?, ?, ?, ?, ?)''',
                     (str(member.id), member.name, 'CURRENT', current_time, True))
            count += 1
    
    conn.commit()
    conn.close()
    logger.info(f"Member history populated with {count} Residents!")


async def validate_referrals(guild):
    """Validates all referrals and updates their status"""
    logger.info("Starting validation process...")
    
    resident_role = discord.utils.get(guild.roles, name='Resident')
    if not resident_role:
        logger.error("'Resident' role not found!")
        return

    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    # Reset all validations first
    c.execute('UPDATE referrals SET is_validated = FALSE, has_resident_role = FALSE, was_previous_resident = FALSE')
    
    # Get all active referrals
    c.execute('SELECT inviter_id, invited_id FROM referrals WHERE is_member_active = TRUE')
    referrals = c.fetchall()
    
    validated_count = 0
    for inviter_id, invited_id in referrals:
        inviter = guild.get_member(int(inviter_id))
        invited = guild.get_member(int(invited_id))
        
        if (inviter and invited and 
            resident_role in inviter.roles and 
            resident_role in invited.roles):
            
            # Update validation status and has_resident_role
            c.execute('''UPDATE referrals 
                        SET is_validated = TRUE,
                            has_resident_role = TRUE,
                                was_previous_resident = TRUE
                        WHERE inviter_id = ? AND invited_id = ?''',
                     (inviter_id, invited_id))
            
            # Record in history for both inviter and invited if not already there
            for member in [inviter, invited]:
                c.execute('SELECT COUNT(*) FROM member_history WHERE member_id = ? AND had_resident = TRUE', 
                         (str(member.id),))
                if c.fetchone()[0] == 0:
                    logger.debug(f"Adding {member.name} to resident history")
                    c.execute('''INSERT INTO member_history 
                                (member_id, member_name, action, timestamp, had_resident)
                                VALUES (?, ?, ?, ?, ?)''',
                             (str(member.id), member.name, 'got_resident', datetime.now(), True))
            
            validated_count += 1
    
    conn.commit()
    conn.close()
    logger.info(f'Validation completed! Validated {validated_count} referrals')


async def update_leaderboard():
    """Creates and posts the leaderboard message"""
    logger.info("Updating leaderboard...")
    global last_leaderboard_message
    
    channel = bot.get_channel(LEADERBOARD_CHANNEL_ID)
    if not channel:
        logger.error("Could not find leaderboard channel!")
        return
    
    # Delete existing leaderboard message if it exists
    try:
        if last_leaderboard_message:
            await last_leaderboard_message.delete()
    except discord.NotFound:
        pass
    
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    c.execute('''
        SELECT 
            inviter_id,
            inviter_name,
            SUM(CASE WHEN is_validated = TRUE AND is_member_active = TRUE THEN 1 ELSE 0 END) as validated_count,
            SUM(CASE WHEN is_validated = FALSE AND is_member_active = TRUE THEN 1 ELSE 0 END) as unvalidated_count,
            SUM(CASE WHEN is_member_active = TRUE THEN 1 ELSE 0 END) as total_count
        FROM referrals 
        WHERE inviter_id != '845819834696597504' AND inviter_id != '851302798247067678'  -- Replace with actual Discord ID
        GROUP BY inviter_id, inviter_name
        HAVING total_count > 0
        ORDER BY validated_count DESC, total_count DESC
        LIMIT 10
    ''')
    
    leaderboard = c.fetchall()
    conn.close()
    
    embed = discord.Embed(
        title="<:nrrp:1313023333251420210> Referral Leaderboard", 
        color=discord.Color.red(),
        description="📢 **Reminder:** The joinee needs to whitelist in order for your invite to be verified! **Please make sure they do so!**\n\u200b"
    )
    
    if not leaderboard:
        embed.description += "\nNo referrals tracked yet! Be the first one to invite someone! ⭐"
        last_leaderboard_message = await channel.send(embed=embed)
        return
    
    # Adjusted column widths and spacing with clear separation
    leaderboard_text = "```\nRank  Inviter          Verified  Pending      Total\n"
    leaderboard_text += "─" * 54 + "\n"

    guild = bot.get_guild(GUILD_ID)
    for i, (inviter_id, inviter_name, validated, unvalidated, total) in enumerate(leaderboard, 1):
        inviter = guild.get_member(int(inviter_id))
        current_name = inviter.name if inviter else inviter_name or f"User {inviter_id}"
        
        # Fixed width formatting with proper spacing
        rank = f"{i:2d}."
        name = current_name[:15].ljust(15)
        validated_str = str(validated).rjust(8)
        unvalidated_str = str(unvalidated).rjust(8)
        total_str = str(total).rjust(8)
        
        leaderboard_text += f"{rank:4} {name} {validated_str} {unvalidated_str}    {total_str}\n"

    leaderboard_text += "```"
    embed.add_field(name="\u200b", value=leaderboard_text, inline=False)
    
    # Add timestamp to show when the leaderboard was last updated
    embed.set_footer(text=f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Send a new message
    last_leaderboard_message = await channel.send(embed=embed)
    logger.info('Leaderboard updated successfully!')

async def auto_update_loop():
    """Background task to validate referrals and update leaderboard every 24 hours"""
    await bot.wait_until_ready()
    while not bot.is_closed():
        guild = bot.get_guild(GUILD_ID)
        if guild:
            await validate_referrals(guild)  # Validate first
            await asyncio.sleep(1)  # Add a small delay
            await update_leaderboard()  # Then update the leaderboard
            await perform_maintenance()
        await asyncio.sleep(86400)  # 24 hours

@bot.event
async def on_ready():
    logger.info('\n')
    logger.info('='*50)
    logger.info(f'Bot initialized as: {bot.user.name}')
    logger.info(f'Bot ID: {bot.user.id}')
    logger.info(f'Connected to Discord!')
    logger.info('-'*50)
    
    logger.info('Setting up database...')
    setup_database()
    logger.info('Database setup complete!')
    
    # Add initial member history population
    guild = bot.get_guild(GUILD_ID)
    if guild:
        await populate_member_history(guild)

        
    logger.info('Caching existing invites...')
    for guild in bot.guilds:
        invites = await guild.invites()
        invite_cache[guild.id] = invites
        logger.info(f'Cached {len(invites)} invites for guild: {guild.name}')
    
    # Send the initial messages in the leaderboard channel
    leaderboard_channel = bot.get_channel(LEADERBOARD_CHANNEL_ID)
    if leaderboard_channel:
        try:
            # Check if pinned info message already exists
            pins = await leaderboard_channel.pins()
            info_message_exists = any(
                pin.author == bot.user and 
                pin.embeds and 
                pin.embeds[0].title == "<:nrrp:1313023333251420210> Referral Leaderboard"
                for pin in pins
            )
            
            if not info_message_exists:
                # First, send and pin the info message
                info_embed = discord.Embed(
                    title="<:nrrp:1313023333251420210> Referral System Guide", 
                    color=discord.Color.red(),
                    description="Welcome to our referral system! Here's everything you need to know about inviting new members and earning rewards.\n\u200b"
                )
                
                info_embed.add_field(
                    name="🎯 Verification Requirements",
                    value="• Invites only count after the new member completes whitelisting.\n"
                          "• Make sure your invitees complete the verification process.\n"
                          "• Tracking begins automatically when someone uses your invite.\n\u200b",
                    inline=False
                )
                
                info_embed.add_field(
                    name="💎 Current Reward Tiers",
                    value="• **🥇 1st Place** → Custom MLO!\n"
                        "• **🥈 2nd Place** → 2 Months of Gold Supporter.\n"
                        "• **🥉 3rd Place** → 2 Months of Silver Supporter.\n",
                    inline=False
                )
                
                info_embed.add_field(
                    name="📊 Tracking & Commands",
                    value="• Use `!myreferrals` in DMs to view your invitation history.\n"
                            "• Check `!leaderboard` in DMs to see current rankings.\n"
                            "• Leaderboard below updates automatically every 24 hours.\n"
                            "• Commands have a 15-minute cooldown.\n"
                            "• `!invitehistory @user` - [Admin] View detailed invite history for a user.\n"
                            "• `!invitestats` - [Admin] View system-wide referral statistics.\n"
                            "• `!resethistory` - [Admin] Reset and repopulate member history.\n\u200b",
                    inline=False
                    )
                
                info_embed.add_field(
                    name="⚠️ Important Notes",
                    value="• Only whitelisted members count towards rewards.\n"
                        "• If an invitee leaves, their verification is removed.\n"
                        "• **Re-inviting existing server members is PROHIBITED!**\n"
                        "• Violation will result in immediate tournament disqualification.",
        
                    inline=False
                )
                                
                info_embed.set_footer(text="📢 Remember: The joinee needs to whitelist for your invite to be verified!")
                info_message = await leaderboard_channel.send(embed=info_embed)
                await info_message.pin()
                logger.info("Pinned info message sent successfully!")
            else:
                logger.info("Info message already exists in pins, skipping...")
                
        except discord.HTTPException as e:
            logger.error(f"Error with info message: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            
        # Add a small delay before starting auto-update
        await asyncio.sleep(1)
        
    else:
        logger.error("Could not find leaderboard channel!")


    # Start the auto-update task for the leaderboard
    logger.info('Starting auto-update task...')
    bot.loop.create_task(auto_update_loop())
    
    logger.info('-'*50)
    logger.info('Bot is ready and running!')
    logger.info('='*50)
    logger.info('\n')

# Update the on_member_join event
@bot.event
async def on_member_join(member):
    logger.info(f'New member detected: {member.name} (ID: {member.id})')
    
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    try:
        # Check if they were previously a resident
        c.execute('''SELECT COUNT(*) FROM member_history 
                     WHERE member_id = ? 
                     AND had_resident = TRUE 
                     AND action = 'leave' 
                     ORDER BY timestamp DESC LIMIT 1''', 
                  (str(member.id),))
        was_resident = c.fetchone()[0] > 0
        
        # Check for existing record
        c.execute('SELECT COUNT(*) FROM referrals WHERE invited_id = ?', 
                  (str(member.id),))
        has_previous_record = c.fetchone()[0] > 0
        
        if has_previous_record:
            # Reactivate existing record
            c.execute('''UPDATE referrals 
                         SET is_member_active = TRUE,
                             is_validated = FALSE,
                             was_previous_resident = ?
                         WHERE invited_id = ?''', 
                      (was_resident, str(member.id),))
            logger.info(f'Reactivated existing record for {member.name}')
        
        # Find the invite used
        invites_after = await member.guild.invites()
        invite_used = None
        
        for invite in invites_after:
            cached_invite = next((x for x in invite_cache[member.guild.id] if x.code == invite.code), None)
            if cached_invite is None or invite.uses > cached_invite.uses:
                invite_used = invite
                break
            
        if invite_used and not has_previous_record:
            # Insert new referral record
            logger.info(f'Inserting new referral for {member.name}')
            c.execute('''INSERT INTO referrals 
                         (inviter_id, inviter_name, invited_id, invited_name, 
                          invite_code, joined_at, is_member_active, was_previous_resident)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                      (str(invite_used.inviter.id),
                       invite_used.inviter.name,
                       str(member.id),
                       member.name,
                       invite_used.code,
                       datetime.now(),
                       True,
                       was_resident))
        
        # Record in history
        c.execute('''INSERT INTO member_history 
                     (member_id, member_name, action, timestamp, had_resident)
                     VALUES (?, ?, ?, ?, ?)''',
                  (str(member.id), member.name, 'join', datetime.now(), was_resident))
        
        # Send join message to logs channel
        logs_channel = bot.get_channel(LOGS_CHANNEL_ID)
        if logs_channel:
            join_embed = discord.Embed(
                title="📥 Member Joined",
                description=f"**Member:** {member.name} (ID: {member.id})",
                color=discord.Color.green(),
                timestamp=datetime.now()
            )
            
            if was_resident:
                join_embed.add_field(
                    name="Previous Member",
                    value="⚠️ This user was previously a Resident",
                    inline=False
                )
            
            if invite_used:
                join_embed.add_field(
                    name="Invite Information",
                    value=f"Invited by: <@{invite_used.inviter.id}> ({invite_used.inviter.name})\n"
                          f"Using invite code: {invite_used.code}",
                    inline=False
                )
            
            await logs_channel.send(embed=join_embed)
        
        conn.commit()
    except Exception as e:
        logger.error(f'Error processing member join for {member.name}: {e}')
        conn.rollback()
    finally:
        conn.close()
    
    # Update invite cache
    if invites_after:
        invite_cache[member.guild.id] = invites_after
    
    # Update leaderboard
    guild = member.guild
    await validate_referrals(guild)
    await asyncio.sleep(1)
    await update_leaderboard()
    await log_audit_event('MEMBER_JOIN', {
        'member_id': str(member.id),
        'member_name': member.name,
        'was_previous_resident': was_resident
    })

    
# Update the on_member_remove event
@bot.event
async def on_member_remove(member):
    logger.info(f'Member left: {member.name} (ID: {member.id})')
    
    # Check if they had the Resident role when leaving
    had_resident = discord.utils.get(member.roles, name='Resident') is not None
    
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    try:
        # Record in history
        c.execute('''INSERT INTO member_history 
                     (member_id, member_name, action, timestamp, had_resident)
                     VALUES (?, ?, ?, ?, ?)''',
                  (str(member.id), member.name, 'leave', datetime.now(), had_resident))
        
        # Only invalidate referrals where this member was the invitee
        c.execute('''UPDATE referrals 
                     SET is_validated = FALSE,
                         is_member_active = FALSE,
                         was_previous_resident = ?
                     WHERE invited_id = ?''', 
                  (had_resident, str(member.id)))
        
        # Get inviter information for the leaving member
        c.execute('''SELECT inviter_name, inviter_id FROM referrals 
                     WHERE invited_id = ? LIMIT 1''', (str(member.id),))
        inviter_info = c.fetchone()
        
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Database error on member remove: {e}")
        conn.rollback()
    finally:
        conn.close()
    
    # Send leave message to logs channel
    logs_channel = bot.get_channel(LOGS_CHANNEL_ID)
    if logs_channel:
        leave_embed = discord.Embed(
            title="📤 Member Left",
            description=f"**Member:** {member.name} (ID: {member.id})\n"
                       f"**Had Resident Role:** {'Yes' if had_resident else 'No'}",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        
        if inviter_info:
            inviter_name, inviter_id = inviter_info
            leave_embed.add_field(
                name="Invite Information",
                value=f"Was invited by: <@{inviter_id}> ({inviter_name})",
                inline=False
            )
        
        await logs_channel.send(embed=leave_embed)
    
    # Update leaderboard
    guild = member.guild
    await validate_referrals(guild)
    await asyncio.sleep(1)
    await update_leaderboard()
    await log_audit_event('MEMBER_LEAVE', {
        'member_id': str(member.id),
        'member_name': member.name,
        'had_resident_role': discord.utils.get(member.roles, name='Resident') is not None
    }, severity='IMPORTANT')

# Add command to view invite history for a specific user
@bot.command(name='invitehistory')
@commands.has_permissions(administrator=True)
async def invite_history(ctx, member: discord.Member):
    conn = sqlite3.connect('referrals.db')
    try:
        c = conn.cursor()
        
        # Get all invite history (both as inviter and invitee)
        def create_invite_list_embed(invited, invited_by=None, page=0, per_page=5):
            embed = discord.Embed(
                title=f"📋 Invite History for {member.name}",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            # Add invited by information if available
            if invited_by:
                inviter_id, inviter_name, join_date, is_valid, is_active = invited_by
                embed.add_field(
                    name="📥 Invited By",
                    value=f"<@{inviter_id}> ({inviter_name})\n"
                          f"Joined: {datetime.strptime(join_date, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d')}\n"
                          f"Status: {'✅ Active' if is_active else '❌ Left'}\n"
                          f"Validated: {'✅ Yes' if is_valid else '⏳ No'}",
                    inline=False
                )
            
            # Pagination logic
            start = page * per_page
            end = start + per_page
            page_invited = invited[start:end]
            
            for inv_id, inv_name, join_date, is_valid, is_active, was_resident in page_invited:
                status = "✅ Active" if is_active else "❌ Left"
                validated = "✅ Yes" if is_valid else "⏳ No"
                previous = "⚠️ Previous Resident" if was_resident else ""
                
                invited_details = (
                    f"<@{inv_id}> ({inv_name}) {previous}\n"
                    f"Joined: {datetime.strptime(join_date, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d')}\n"
                    f"Status: {status} | Validated: {validated}"
                )
                
                embed.add_field(
                    name=f"📤 Invited Member",
                    value=invited_details,
                    inline=False
                )
            
            # Add pagination info
            total_pages = (len(invited) + per_page - 1) // per_page
            embed.set_footer(text=f"Page {page + 1}/{total_pages}")
            
            return embed

        # Get people they invited
        c.execute('''SELECT invited_id, invited_name, joined_at, is_validated, is_member_active, was_previous_resident
                     FROM referrals WHERE inviter_id = ?''', (str(member.id),))
        invited = c.fetchall()
        
        # Get who invited them
        c.execute('''SELECT inviter_id, inviter_name, joined_at, is_validated, is_member_active
                     FROM referrals WHERE invited_id = ?''', (str(member.id),))
        invited_by = c.fetchone()
        
        # Get member history
        c.execute('''SELECT action, timestamp, had_resident
                     FROM member_history 
                     WHERE member_id = ?
                     ORDER BY timestamp DESC''', (str(member.id),))
        history = c.fetchall()
        
        # Add member history to the first page embed
        def add_member_history(embed, history):
            if history:
                history_text = ""
                for action, timestamp, had_resident in history[:5]:  # Limit to first 5 entries
                    date = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M')
                    resident_status = "with Resident" if had_resident else "without Resident"
                    history_text += f"• {date}: {action.capitalize()} {resident_status}\n"
                
                embed.add_field(
                    name="📜 Server History",
                    value=history_text,
                    inline=False
                )
            return embed

        if invited:
            # Pagination logic
            current_page = 0
            per_page = 5
            total_pages = (len(invited) + per_page - 1) // per_page
            
            # Send first page
            first_embed = create_invite_list_embed(invited, invited_by, current_page)
            first_embed = add_member_history(first_embed, history)
            message = await ctx.send(embed=first_embed)
            
            # Add navigation reactions if multiple pages
            if total_pages > 1:
                await message.add_reaction('⬅️')
                await message.add_reaction('➡️')
                
                def check(reaction, user):
                    return user == ctx.author and str(reaction.emoji) in ['⬅️', '➡️'] and reaction.message.id == message.id
                
                try:
                    while True:
                        reaction, user = await bot.wait_for('reaction_add', timeout=60.0, check=check)
                        
                        if str(reaction.emoji) == '➡️' and current_page < total_pages - 1:
                            current_page += 1
                            new_embed = create_invite_list_embed(invited, invited_by, current_page)
                            await message.edit(embed=new_embed)
                            await message.remove_reaction(reaction, user)
                        
                        elif str(reaction.emoji) == '⬅️' and current_page > 0:
                            current_page -= 1
                            new_embed = create_invite_list_embed(invited, invited_by, current_page)
                            await message.edit(embed=new_embed)
                            await message.remove_reaction(reaction, user)
                        
                except asyncio.TimeoutError:
                    await message.clear_reactions()
        else:
            # If no invited members, create an embed with member history
            no_invites_embed = discord.Embed(
                title=f"📋 Invite History for {member.name}",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            no_invites_embed = add_member_history(no_invites_embed, history)
            
            if not history:
                no_invites_embed.description = "No invite or member history found."
            
            await ctx.send(embed=no_invites_embed)

    except sqlite3.Error as e:
        logger.error(f"Database error in invite_history: {e}")
        await ctx.send(f"An error occurred while retrieving invite history: {e}")
    finally:
        conn.close()


 # Add command to get system statistics
@bot.command(name='invitestats')
@commands.has_permissions(administrator=True)
async def invite_stats(ctx):
    conn = sqlite3.connect('referrals.db')
    try:
        c = conn.cursor()
        
        embed = discord.Embed(
            title="📊 Referral System Statistics",
            color=discord.Color.blue(),
            timestamp=datetime.now()
        )
        
        # Total referrals
        c.execute('SELECT COUNT(*) FROM referrals')
        total_referrals = c.fetchone()[0]
        
        # Active referrals
        c.execute('SELECT COUNT(*) FROM referrals WHERE is_member_active = TRUE')
        active_referrals = c.fetchone()[0]
        
        # Validated referrals
        c.execute('SELECT COUNT(*) FROM referrals WHERE is_validated = TRUE AND is_member_active = TRUE')
        validated_referrals = c.fetchone()[0]
        
        # Previous residents
        c.execute('SELECT COUNT(*) FROM referrals WHERE was_previous_resident = TRUE')
        previous_residents = c.fetchone()[0]
        
        # Additional statistics
        c.execute('SELECT COUNT(*) FROM member_history')
        total_member_history_entries = c.fetchone()[0]
        
        # Top inviters with more details
        c.execute('''
            SELECT 
                inviter_name, 
                COUNT(*) as total_count,
                SUM(CASE WHEN is_validated = TRUE AND is_member_active = TRUE THEN 1 ELSE 0 END) as validated_count
            FROM referrals 
            GROUP BY inviter_id, inviter_name
            ORDER BY validated_count DESC
            LIMIT 5
        ''')
        top_inviters = c.fetchall()
        
        # Overall statistics field
        embed.add_field(
            name="📈 Overall Statistics",
            value=f"Total Referrals: {total_referrals}\n"
                  f"Active Members: {active_referrals}\n"
                  f"Validated Referrals: {validated_referrals}\n"
                  f"Previous Residents: {previous_residents}\n"
                  f"Total Member History Entries: {total_member_history_entries}",
            inline=False
        )
        
        # Top inviters field
        if top_inviters:
            top_text = ""
            for inviter, total_count, validated_count in top_inviters:
                top_text += f"• {inviter}: {validated_count} of {total_count} validated\n"
            embed.add_field(
                name="🏆 Top 5 Inviters",
                value=top_text,
                inline=False
            )
        else:
            embed.add_field(
                name="🏆 Top Inviters",
                value="No inviters found.",
                inline=False
            )
        
        # Add some context to the embed
        embed.set_footer(text="Referral statistics reflect current server state")
        
        await ctx.send(embed=embed)
    
    except sqlite3.Error as e:
        logger.error(f"Database error in invite_stats: {e}")
        await ctx.send(f"An error occurred while retrieving invite statistics: {e}")
    finally:
        conn.close()


@bot.command(name='validate')
@commands.has_permissions(administrator=True)
@commands.check(check_channel)
async def validate_referrals_command(ctx):
    logger.info(f'Validate command used by {ctx.author.name} (ID: {ctx.author.id})')
    try:
        resident_role = discord.utils.get(ctx.guild.roles, name='Resident')
        if not resident_role:
            await ctx.send("Error: 'Resident' role not found!")
            return
    except Exception as e:
        logger.error(f"Error getting Resident role: {e}")
        await ctx.send(f"Error getting Resident role: {e}")
        return

    status_message = await ctx.send("Starting validation process...")
    
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    # Reset all validations first
    c.execute('UPDATE referrals SET is_validated = FALSE, has_resident_role = FALSE')
    
    # Get all active referrals
    c.execute('SELECT inviter_id, invited_id FROM referrals WHERE is_member_active = TRUE')
    referrals = c.fetchall()
    
    validated_count = 0
    invalid_count = 0
    
    for inviter_id, invited_id in referrals:
        inviter = ctx.guild.get_member(int(inviter_id))
        invited = ctx.guild.get_member(int(invited_id))
        
        if (inviter and invited and 
            resident_role in inviter.roles and 
            resident_role in invited.roles):
            c.execute('''UPDATE referrals 
                        SET is_validated = TRUE,
                            has_resident_role = TRUE 
                        WHERE inviter_id = ? AND invited_id = ?''',
                     (inviter_id, invited_id))
            validated_count += 1
        else:
            invalid_count += 1
            
        if (validated_count + invalid_count) % 50 == 0:
            await status_message.edit(content=f"Processing... Validated: {validated_count}, Invalid: {invalid_count}")
    
    conn.commit()
    
    c.execute('''SELECT inviter_id, COUNT(*) as count 
                 FROM referrals 
                 WHERE is_validated = TRUE AND is_member_active = TRUE
                 GROUP BY inviter_id 
                 ORDER BY count DESC''')
    
    final_standings = c.fetchall()
    conn.close()
    
    embed = discord.Embed(title="Final Validation Report",
                         color=discord.Color.red(),
                         timestamp=datetime.now())
    
    embed.add_field(name="Summary",
                   value=f"Total Validated: {validated_count}\n"
                         f"Total Invalid: {invalid_count}",
                   inline=False)
    
    if final_standings:
        standings_text = ""
        for inviter_id, count in final_standings:
            member = ctx.guild.get_member(int(inviter_id))
            if member:
                standings_text += f"{member.name}: {count} validated referrals\n"
        embed.add_field(name="Final Standings", value=standings_text or "None", inline=False)
    

    

    await status_message.delete()
    await ctx.send(embed=embed)
    
    # Update leaderboard with confirmation
    status_message = await ctx.send("🔄 Updating leaderboard...")
    await asyncio.sleep(1)
    await update_leaderboard()
    await log_audit_event('MANUAL_VALIDATION', {
        'admin_id': str(ctx.author.id),
        'admin_name': ctx.author.name,
        'validated_count': validated_count,
        'invalid_count': invalid_count
    }, severity='ADMIN')

    await status_message.edit(content="✅ Validation complete and leaderboard has been updated!")


# Add a new command to view audit logs
@bot.command(name='auditlogs')
@commands.has_permissions(administrator=True)
async def view_audit_logs(ctx, limit: int = 20):
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    c.execute('''SELECT event_type, event_data, severity, timestamp 
                 FROM audit_log 
                 ORDER BY timestamp DESC 
                 LIMIT ?''', (limit,))
    logs = c.fetchall()
    
    embed = discord.Embed(title="🕵️ Audit Logs", color=discord.Color.blue())
    
    if not logs:
        embed.description = "No audit logs found."
        await ctx.send(embed=embed)
        return
    
    for event_type, event_data, severity, timestamp in logs:
        try:
            data = json.loads(event_data)
            # Format the event data into a readable string
            details = ', '.join([f"{k}: {v}" for k, v in data.items()])
            
            embed.add_field(
                name=f"{timestamp} | {event_type} ({severity})",
                value=details,
                inline=False
            )
        except json.JSONDecodeError:
            logger.error(f"Failed to parse audit log entry: {event_data}")
    
    await ctx.send(embed=embed)

# Update the myreferrals command
@bot.command(name='myreferrals')
@dm_only()
@cooldown(1, 900, BucketType.user)
async def show_my_referrals(ctx):
    logger.info(f'Myreferrals command used by {ctx.author.name} (ID: {ctx.author.id})')
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    c.execute('''SELECT invited_id, invite_code, joined_at, is_validated, is_member_active
                 FROM referrals 
                 WHERE inviter_id = ?
                 ORDER BY joined_at DESC''',
              (str(ctx.author.id),))
    
    referrals = c.fetchall()
    conn.close()

    if not referrals:
        embed = discord.Embed(
            title="🏆 Referral Rewards Await!",
            color=discord.Color.gold(),
            description="Start your journey to exclusive rewards by inviting new members to our community!"
        )
        # Add reward tiers and other fields...
        await ctx.send(embed=embed)
        return

    # Calculate total pages
    items_per_page = 5
    pages = [referrals[i:i + items_per_page] for i in range(0, len(referrals), items_per_page)]
    total_pages = len(pages)

    class PaginationView(discord.ui.View):
        def __init__(self, pages):
            super().__init__(timeout=60)
            self.current_page = 0
            self.pages = pages
            self.total_pages = len(pages)
            # Disable previous button on first page
            self.previous_page.disabled = True

        def create_embed(self, page_data, page_num):
            embed = discord.Embed(
                title="Your Referrals", 
                color=discord.Color.red(),
                description=f"Total referrals: {len(referrals)}"
            )
            
            guild = bot.get_guild(GUILD_ID)
            
            for invited_id, invite_code, joined_at, is_validated, is_member_active in page_data:
                member = guild.get_member(int(invited_id))
                member_name = member.name if member else f"User {invited_id}"
                
                if is_member_active:
                    status = "✅ Validated" if is_validated else "⏳ Pending"
                else:
                    status = "❌ Left Server"
                    
                joined_date = datetime.strptime(joined_at, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d')
                
                field_name = f"{member_name}"
                field_value = f"Status: {status}\nJoined: {joined_date}\nInvite Used: {invite_code}"
                embed.add_field(name=field_name, value=field_value, inline=False)
            
            embed.set_footer(text=f"Page {page_num + 1}/{self.total_pages}")
            return embed

        @discord.ui.button(label='Previous', style=discord.ButtonStyle.blurple, custom_id='previous')
        async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
            if self.current_page > 0:
                self.current_page -= 1
                # Enable/disable buttons based on page position
                self.next_page.disabled = False
                if self.current_page == 0:
                    button.disabled = True
                
                await interaction.response.edit_message(
                    embed=self.create_embed(self.pages[self.current_page], self.current_page),
                    view=self
                )

        @discord.ui.button(label='Next', style=discord.ButtonStyle.blurple, custom_id='next')
        async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
            if self.current_page < self.total_pages - 1:
                self.current_page += 1
                # Enable/disable buttons based on page position
                self.previous_page.disabled = False
                if self.current_page == self.total_pages - 1:
                    button.disabled = True
                
                await interaction.response.edit_message(
                    embed=self.create_embed(self.pages[self.current_page], self.current_page),
                    view=self
                )

        async def on_timeout(self):
            # Disable all buttons when the view times out
            for item in self.children:
                item.disabled = True
            try:
                await self.message.edit(view=self)
            except:
                pass

    # Create and send the initial view
    view = PaginationView(pages)
    embed = view.create_embed(pages[0], 0)
    view.message = await ctx.send(embed=embed, view=view)

# Update the leaderboard command
@bot.command(name='leaderboard')
@dm_only()
@cooldown(1, 900, BucketType.user)  # 1 use per 15 minutes (900 seconds) per user
async def show_leaderboard(ctx):
    logger.info(f'Leaderboard command used by {ctx.author.name} (ID: {ctx.author.id})')
    
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    c.execute('''
        SELECT 
            inviter_id,
            inviter_name,
            SUM(CASE WHEN is_validated = TRUE AND is_member_active = TRUE THEN 1 ELSE 0 END) as validated_count,
            SUM(CASE WHEN is_validated = FALSE AND is_member_active = TRUE THEN 1 ELSE 0 END) as unvalidated_count,
            SUM(CASE WHEN is_member_active = TRUE THEN 1 ELSE 0 END) as total_count
        FROM referrals 
        WHERE inviter_id != '845819834696597504' AND inviter_id != '851302798247067678'  -- Replace with actual Discord ID
        GROUP BY inviter_id, inviter_name
        HAVING total_count > 0
        ORDER BY validated_count DESC, total_count DESC
        LIMIT 10
    ''')
    
    leaderboard = c.fetchall()
    conn.close()
    
    embed = discord.Embed(
        title="<:nrrp:1313023333251420210> Referral Leaderboard", 
        color=discord.Color.red(),
        description="📢 **Reminder:** The joinee needs to whitelist in order for your invite to be verified! **Please make sure they do so!**\n\u200b"
    )
    
    if not leaderboard:
        embed.description += "\nNo referrals tracked yet! Be the first one to invite someone! ⭐"
        await ctx.send(embed=embed)
        return
    
    leaderboard_text = "```\nInviter              ✅ Verified   ⏳ Pending   📊 Total\n"
    leaderboard_text += "─" * 56 + "\n"

    guild = bot.get_guild(GUILD_ID)
    for i, (inviter_id, inviter_name, validated, unvalidated, total) in enumerate(leaderboard, 1):
        inviter = guild.get_member(int(inviter_id))
        current_name = inviter.name if inviter else inviter_name or f"User {inviter_id}"
        
        name_field = f"{i}. {current_name[:20]}"
        leaderboard_text += f"{name_field:<24}   {validated:^10}   {unvalidated:^10}   {total:^6}\n"

    leaderboard_text += "```"
    embed.add_field(name="\u200b", value=leaderboard_text, inline=False)
    embed.set_footer(text=f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    await ctx.send(embed=embed)

# Update the error handling
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        embed = discord.Embed(
            title="❌ Command Not Found",
            color=discord.Color.red(),
            description="That command doesn't exist. Here are the available commands:"
        )
        
        embed.add_field(
            name="Available Commands",
            value="•  `!myreferrals` - View your referral history (DM only)\n"
                  "•  `!leaderboard` - Show the referral rankings (DM only)\n"
                  "•  `!validate` - [Admin] Validate referrals and update leaderboard\n",
            inline=False
        )
        
        embed.set_footer(text="💡 Tip: Use !myreferrals and !leaderboard in DMs with the bot")
        
    elif isinstance(error, commands.CheckFailure):
        if isinstance(error, commands.PrivateMessageOnly):
            embed = discord.Embed(
                title="⚠️ DM Only Command",
                color=discord.Color.red(),
                description="This command can only be used in DMs with the bot."
            )
        else:
            embed = discord.Embed(
                title="⚠️ Permission Error",
                color=discord.Color.red(),
                description="You don't have permission to use this command or you're using it in the wrong channel."
            )
    
    elif isinstance(error, CommandOnCooldown):
        minutes_left = int(error.retry_after / 60)
        seconds_left = int(error.retry_after % 60)
        embed = discord.Embed(
            title="⏳ Command on Cooldown",
            color=discord.Color.gold(),
            description=f"Please wait {minutes_left}m {seconds_left}s before using this command again."
        )
    
    else:
        logger.error(f"Error: {str(error)}")
        embed = discord.Embed(
            title="⛔ Error Occurred",
            color=discord.Color.dark_red(),
            description=f"An unexpected error occurred: {str(error)}"
        )
        
        embed.add_field(
            name="What to do?",
            value="Please try again later or contact an administrator if the problem persists.",
            inline=False
        )
        
        embed.timestamp = datetime.now()
    
    try:
        # Send as ephemeral message that only the command user can see
        await ctx.send(embed=embed, ephemeral=True)
    except AttributeError:
        # Fallback for text channels where ephemeral messages aren't supported
        await ctx.send(embed=embed)

# Run the bot
bot.run(DISCORD_TOKEN)