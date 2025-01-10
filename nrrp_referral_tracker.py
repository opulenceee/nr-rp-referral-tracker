import discord
from discord.ext import commands
import sqlite3
from datetime import datetime
import asyncio
from typing import Optional
from discord.ext.commands import dm_only, cooldown, BucketType, CommandOnCooldown

# Configuration
import os
from dotenv import load_dotenv

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

def check_channel(ctx):
    """Check if command is used in allowed channels"""
    # Only validate command needs channel check now
    return ctx.channel.id in [COMMANDS_CHANNEL_ID, LEADERBOARD_CHANNEL_ID]

# Store invite cache and last leaderboard message
invite_cache = {}
last_leaderboard_message: Optional[discord.Message] = None

# Database setup
def setup_database():
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS referrals
                 (inviter_id TEXT,
                  inviter_name TEXT,
                  invited_id TEXT,
                  invited_name TEXT,
                  invite_code TEXT,
                  joined_at TIMESTAMP,
                  is_validated BOOLEAN DEFAULT FALSE,
                  has_resident_role BOOLEAN DEFAULT FALSE,
                  is_member_active BOOLEAN DEFAULT TRUE)''')
    
    conn.commit()
    conn.close()

async def validate_referrals(guild):
    """Validates all referrals and updates their status"""
    print('\n[Auto-Validation] Starting validation process...')
    
    resident_role = discord.utils.get(guild.roles, name='Resident')
    if not resident_role:
        print("Error: 'Resident' role not found!")
        return

    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    # Reset all validations first
    c.execute('UPDATE referrals SET is_validated = FALSE')
    
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
            c.execute('''UPDATE referrals 
                        SET is_validated = TRUE 
                        WHERE inviter_id = ? AND invited_id = ?''',
                     (inviter_id, invited_id))
            validated_count += 1
    
    conn.commit()
    conn.close()
    print(f'[Auto-Validation] Completed! Validated {validated_count} referrals')

async def update_leaderboard():
    """Creates and posts the leaderboard message"""
    print('\n[Leaderboard] Updating leaderboard...')
    global last_leaderboard_message
    
    channel = bot.get_channel(LEADERBOARD_CHANNEL_ID)
    if not channel:
        return
    
    # Delete previous leaderboard message if it exists
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
    
    # Add timestamp to show when the leaderboard was last updated
    embed.set_footer(text=f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    last_leaderboard_message = await channel.send(embed=embed)
    print('[Leaderboard] Updated successfully!')

async def auto_update_loop():
    """Background task to validate referrals and update leaderboard every 24 hours"""
    await bot.wait_until_ready()
    while not bot.is_closed():
        guild = bot.get_guild(GUILD_ID)
        if guild:
            await validate_referrals(guild)  # Validate first
            await asyncio.sleep(1)  # Add a small delay
            await update_leaderboard()  # Then update the leaderboard
        await asyncio.sleep(86400)  # 24 hours

@bot.event
async def on_ready():
    print('\n')
    print('='*50)
    print(f'Bot initialized as: {bot.user.name}')
    print(f'Bot ID: {bot.user.id}')
    print(f'Connected to Discord!')
    print('-'*50)
    
    print('Setting up database...')
    setup_database()
    print('Database setup complete!')
    
    print('Caching existing invites...')
    for guild in bot.guilds:
        invites = await guild.invites()
        invite_cache[guild.id] = invites
        print(f'Cached {len(invites)} invites for guild: {guild.name}')
    
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
                    value="• Invites only count after the new member completes whitelisting\n"
                          "• Make sure your invitees complete the verification process\n"
                          "• Tracking begins automatically when someone uses your invite\n\u200b",
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
                    value="• Use `!myreferrals` in DMs to view your invitation history\n"
                          "• Check `!leaderboard` in DMs to see current rankings\n"
                          "• Leaderboard below updates automatically every 24 hours\n"
                          "• Commands have a 15-minute cooldown\n\u200b",
                    inline=False
                )
                
                info_embed.add_field(
                    name="⚠️ Important Notes",
                    value="• Only whitelisted members count towards rewards\n"
                          "• If an invitee leaves, their verification is removed\n",
                    inline=False
                )
                
                info_embed.set_footer(text="📢 Remember: The joinee needs to whitelist for your invite to be verified!")
                info_message = await leaderboard_channel.send(embed=info_embed)
                await info_message.pin()
                print("Pinned info message sent successfully!")
            else:
                print("Info message already exists in pins, skipping...")
                
        except discord.HTTPException as e:
            print(f"Error with info message: {str(e)}")
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            
        # Add a small delay before starting auto-update
        await asyncio.sleep(1)
        
    else:
        print("ERROR: Could not find leaderboard channel!")
    
    # Start the auto-update task for the leaderboard
    print('Starting auto-update task...')
    bot.loop.create_task(auto_update_loop())
    
    print('-'*50)
    print('Bot is ready and running!')
    print('='*50)
    print('\n')

@bot.event
async def on_member_join(member):
    print(f'\n[Member Join] New member detected: {member.name} (ID: {member.id})')
    print(f'Checking which invite was used...')
    
    invites_after = await member.guild.invites()
    invite_used = None
    
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    # First check if this member was previously invited
    c.execute('''SELECT COUNT(*) FROM referrals WHERE invited_id = ?''', 
              (str(member.id),))
    has_previous_record = c.fetchone()[0] > 0
    
    if has_previous_record:
        print(f'Found previous record for {member.name}, reactivating...')
        c.execute('''UPDATE referrals 
                     SET is_member_active = TRUE,
                         is_validated = FALSE
                     WHERE invited_id = ?''', 
                  (str(member.id),))
        
        c.execute('''UPDATE referrals 
                     SET is_member_active = TRUE
                     WHERE inviter_id = ?''',
                  (str(member.id),))
    else:
        for invite in invites_after:
            cached_invite = next((x for x in invite_cache[member.guild.id] if x.code == invite.code), None)
            if cached_invite is None or invite.uses > cached_invite.uses:
                invite_used = invite
                break
        
        if invite_used:
            print(f'Found invite used: {invite_used.code} created by {invite_used.inviter.name}')
            print(f'Storing new referral in database...')
            c.execute('''INSERT INTO referrals 
                         (inviter_id, inviter_name, invited_id, invited_name, invite_code, joined_at, is_member_active)
                         VALUES (?, ?, ?, ?, ?, ?, ?)''',
                      (str(invite_used.inviter.id),
                       invite_used.inviter.name,
                       str(member.id),
                       member.name,
                       invite_used.code,
                       datetime.now(),
                       True))
    
    conn.commit()
    conn.close()
    
    # Update invite cache
    invite_cache[member.guild.id] = invites_after
    
    # Update leaderboard immediately when someone joins
    guild = member.guild
    await validate_referrals(guild)  # Validate first
    await asyncio.sleep(1)  # Add a small delay
    await update_leaderboard()  # Then update the leaderboard

@bot.event
async def on_member_remove(member):
    print(f'\n[Member Remove] Member left: {member.name} (ID: {member.id})')
    
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    c.execute('''UPDATE referrals 
                 SET is_validated = FALSE,
                     is_member_active = FALSE
                 WHERE invited_id = ?''', 
              (str(member.id),))
    
    c.execute('''UPDATE referrals 
                 SET is_validated = FALSE,
                     is_member_active = FALSE
                 WHERE inviter_id = ?''',
              (str(member.id),))
    
    conn.commit()
    conn.close()
    
    # Update leaderboard immediately when someone leaves
    guild = member.guild
    await validate_referrals(guild)  # Validate first
    await asyncio.sleep(1)  # Add a small delay
    await update_leaderboard()  # Then update the leaderboard

@bot.command(name='validate')
@commands.has_permissions(administrator=True)
@commands.check(check_channel)
async def validate_referrals_command(ctx):
    print(f'\n[Command] Validate command used by {ctx.author.name} (ID: {ctx.author.id})')
    resident_role = discord.utils.get(ctx.guild.roles, name='Resident')
    if not resident_role:
        await ctx.send("Error: 'Resident' role not found!")
        return

    status_message = await ctx.send("Starting validation process...")
    
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    # Reset all validations first
    c.execute('UPDATE referrals SET is_validated = FALSE')
    
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
                        SET is_validated = TRUE 
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
    await status_message.edit(content="✅ Validation complete and leaderboard has been updated!")

# Update the myreferrals command
@bot.command(name='myreferrals')
@dm_only()
@cooldown(1, 900, BucketType.user)  # 1 use per 15 minutes (900 seconds) per user
async def show_my_referrals(ctx):
    print(f'\n[Command] Myreferrals command used by {ctx.author.name} (ID: {ctx.author.id})')
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
        
        embed.add_field(
            name="💎 Current Reward Tiers",
            value="• **🥇 1st Place** → Custom MLO!\n"
                  "• **🥈 2nd Place** → 2 Months of Gold Supporter.\n"
                  "• **🥉 3rd Place** → 2 Months of Silver Supporter.\n",
            inline=False
        )
        
        embed.add_field(
            name="🚀 Quick Start Guide",
            value="1. Generate your invite link using `/invite` in the server\n"
                  "2. Share with friends who'd enjoy our community\n"
                  "3. Track your progress with `!myreferrals`\n"
                  "4. Remember: Invitees must verify to count!",
            inline=False
        )
        
        embed.add_field(
            name="💫 Pro Tips",
            value="• Verified invites are the only ones that count!\n"
                  "• Check `!leaderboard` to see your ranking\n"
                  "• The more active your invites, the better your rewards!",
            inline=False
        )
        
        embed.set_footer(text="🎁 Don't miss out on these exclusive rewards - start inviting today!")
        
        await ctx.send(embed=embed)
        return
    
    embed = discord.Embed(title="Your Referrals", 
                         color=discord.Color.red(),
                         description=f"Total referrals: {len(referrals)}")
    
    # Get the guild object for member lookup
    guild = bot.get_guild(GUILD_ID)
    
    for invited_id, invite_code, joined_at, is_validated, is_member_active in referrals:
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
    
    await ctx.send(embed=embed)

# Update the leaderboard command
@bot.command(name='leaderboard')
@dm_only()
@cooldown(1, 900, BucketType.user)  # 1 use per 15 minutes (900 seconds) per user
async def show_leaderboard(ctx):
    print(f'\n[Command] Leaderboard command used by {ctx.author.name} (ID: {ctx.author.id})')
    
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
        print(f"Error: {str(error)}")
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