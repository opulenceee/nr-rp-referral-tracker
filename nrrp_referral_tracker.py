import discord
from discord.ext import commands
import sqlite3
from datetime import datetime
import asyncio
from typing import Optional

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

# Store invite cache and last leaderboard message
invite_cache = {}
last_leaderboard_message: Optional[discord.Message] = None

def check_channel(ctx):
    """Check if command is used in allowed channels"""
    return ctx.channel.id in [COMMANDS_CHANNEL_ID, LEADERBOARD_CHANNEL_ID]

# Database setup
def setup_database():
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    # Create the main referrals table with the new is_member_active column
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
    
    # Check if is_member_active column exists, if not add it
    c.execute("PRAGMA table_info(referrals)")
    columns = [column[1] for column in c.fetchall()]
    if 'is_member_active' not in columns:
        c.execute('ALTER TABLE referrals ADD COLUMN is_member_active BOOLEAN DEFAULT TRUE')
    
    conn.commit()
    conn.close()

async def post_leaderboard():
    """Posts or updates the leaderboard in the designated channel"""
    print('\n[Leaderboard] Updating leaderboard...')
    global last_leaderboard_message
    
    if not LEADERBOARD_CHANNEL_ID:
        return
        
    channel = bot.get_channel(LEADERBOARD_CHANNEL_ID)
    if not channel:
        return

    # Create a mock message to get context
    mock_message = await channel.fetch_message(channel.last_message_id) if channel.last_message_id else None
    if not mock_message:
        mock_message = discord.Message(state=bot._connection, channel=channel, data={
            "id": 0,
            "channel_id": channel.id,
            "author": {"id": bot.user.id}
        })
    
    ctx = await bot.get_context(mock_message)
    
    try:
        if last_leaderboard_message:
            await last_leaderboard_message.delete()
    except discord.NotFound:
        pass
    
    last_leaderboard_message = await show_leaderboard(ctx)

async def auto_leaderboard_loop():
    """Background task to update leaderboard every 24 hours"""
    await bot.wait_until_ready()
    while not bot.is_closed():
        await post_leaderboard()
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
    
    print('Starting auto-leaderboard task...')
    bot.loop.create_task(auto_leaderboard_loop())
    
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
        # Reactivate their record if they were previously invited
        c.execute('''UPDATE referrals 
                     SET is_member_active = TRUE,
                         is_validated = FALSE
                     WHERE invited_id = ?''', 
                  (str(member.id),))
        
        # Also reactivate any records where they were the inviter
        c.execute('''UPDATE referrals 
                     SET is_member_active = TRUE
                     WHERE inviter_id = ?''',
                  (str(member.id),))
    else:
        # Process new invite only if no previous record exists
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

@bot.event
async def on_member_remove(member):
    print(f'\n[Member Remove] Member left: {member.name} (ID: {member.id})')
    
    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    # Mark records as inactive where the leaving member was invited
    c.execute('''UPDATE referrals 
                 SET is_validated = FALSE,
                     is_member_active = FALSE
                 WHERE invited_id = ?''', 
              (str(member.id),))
    
    # Mark records as inactive where the leaving member was the inviter
    c.execute('''UPDATE referrals 
                 SET is_validated = FALSE,
                     is_member_active = FALSE
                 WHERE inviter_id = ?''',
              (str(member.id),))
    
    conn.commit()
    conn.close()

@bot.command(name='validate')
@commands.has_permissions(administrator=True)
@commands.check(check_channel)
async def validate_referrals(ctx):
    print(f'\n[Command] Validate command used by {ctx.author.name} (ID: {ctx.author.id})')
    resident_role = discord.utils.get(ctx.guild.roles, name='resident')
    if not resident_role:
        await ctx.send("Error: 'resident' role not found!")
        return

    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    # Reset all validations first
    c.execute('UPDATE referrals SET is_validated = FALSE')
    
    # Get all active referrals
    c.execute('SELECT inviter_id, invited_id FROM referrals WHERE is_member_active = TRUE')
    referrals = c.fetchall()
    
    validated_count = 0
    invalid_count = 0
    status_message = await ctx.send("Starting validation process...")
    
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
    
    await post_leaderboard()

@bot.command(name='myreferrals')
@commands.check(check_channel)
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
        await ctx.send("You haven't invited anyone yet!")
        return
    
    embed = discord.Embed(title="Your Referrals", 
                         color=discord.Color.red(),
                         description=f"Total referrals: {len(referrals)}")
    
    for invited_id, invite_code, joined_at, is_validated, is_member_active in referrals:
        member = ctx.guild.get_member(int(invited_id))
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

@bot.command(name='refreshboard')
@commands.check(check_channel)
async def refresh_leaderboard(ctx):
    print(f'\n[Command] Refreshboard command used by {ctx.author.name} (ID: {ctx.author.id})')
    
    status_message = await ctx.send("🔄 Refreshing leaderboard and validation statuses...")
    
    resident_role = discord.utils.get(ctx.guild.roles, name='Resident')
    if not resident_role:
        await ctx.send("Error: 'Resident' role not found!")
        return

    conn = sqlite3.connect('referrals.db')
    c = conn.cursor()
    
    c.execute('SELECT inviter_id, invited_id FROM referrals WHERE is_member_active = TRUE')
    referrals = c.fetchall()
    
    for inviter_id, invited_id in referrals:
        inviter = ctx.guild.get_member(int(inviter_id))
        invited = ctx.guild.get_member(int(invited_id))
        
        is_valid = (inviter and invited and 
                   resident_role in inviter.roles and 
                   resident_role in invited.roles)
        
        c.execute('''UPDATE referrals 
                    SET is_validated = ?
                    WHERE inviter_id = ? AND invited_id = ?''',
                 (is_valid, inviter_id, invited_id))
    
    conn.commit()
    conn.close()
    
    await status_message.edit(content="✅ Validation status has been updated!")
    await post_leaderboard()

@bot.command(name='leaderboard')
@commands.check(check_channel)
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
    
    if not leaderboard:
        empty_embed = discord.Embed(
            title="<:nrrp:1313023333251420210> Referral Leaderboard",
            description="No referrals tracked yet! Be the first one to invite someone! ⭐\n\u200b",  # Added blank line
            color=discord.Color.red()
        )
        
        empty_embed.add_field(
            name="🎯 How to Start?",
            value="Create an invite link and share it with your friends! 🔗\n\u200b",  # Added blank line
            inline=False
        )

        
        empty_embed.add_field(
            name="📝 Available Commands",
            value="•   `!myreferrals` - View your referral history\n"  # Added extra newline between commands
                  "•   `!leaderboard` - Show the referral rankings",
            inline=False
        )
        
        empty_embed.set_footer(text="💡 Tip: Your invites will appear here once someone joins using your invite link!")
        
        await ctx.send(embed=empty_embed)
        return
    
    embed = discord.Embed(
        title="<:nrrp:1313023333251420210> Referral Leaderboard", 
        color=discord.Color.red(),
        description="📢 **Reminder:** The joinee needs to whitelist in order for your invite to be verified! bPlease make sure they do so!\n\u200b"  # Added line break and blank line
    )

    # Create the header
    leaderboard_text = "```\nInviter               ✅ Verified   ⏳ Pending   📊 Total\n"
    leaderboard_text += "─" * 55 + "\n"  # Separator line

    # Add each row
    for i, (inviter_id, inviter_name, validated, unvalidated, total) in enumerate(leaderboard, 1):
        inviter = ctx.guild.get_member(int(inviter_id))
        current_name = inviter.name if inviter else inviter_name or f"User {inviter_id}"
        # Pad the name to 20 characters, numbers to 11 characters each
        name_field = f"{i}. {current_name[:17]:<17}"
        leaderboard_text += f"{name_field}    {validated:^11} {unvalidated:^11} {total:^7}\n"

    leaderboard_text += "```"

    # Add single field with all content
    embed.add_field(name="\u200b", value=leaderboard_text, inline=False)
    
    return await ctx.send(embed=embed)

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
            value="•  `!myreferrals` - View your referral history\n"
                  "•  `!leaderboard` - Show the referral rankings\n",
            inline=False
        )
        
        embed.set_footer(text="💡 Tip: Use these commands in the designated channels")
        
    elif isinstance(error, commands.CheckFailure):
        embed = discord.Embed(
            title="⚠️ Permission Error",
            color=discord.Color.red(),
            description="You don't have permission to use this command or you're using it in the wrong channel."
        )
        
        embed.add_field(
            name="What happened?",
            value="This could be because:\n"
                  "• You're using the command in the wrong channel\n"
                  "• You don't have the required permissions\n"
                  "• The command is restricted to specific roles",
            inline=False
        )
        
        embed.add_field(
            name="Solution",
            value="Try using the command in the designated channels:\n"
                  "• #commands\n"
                  "• #leaderboard",
            inline=False
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
        
        # Add timestamp to error messages
        embed.timestamp = datetime.now()
    
    await ctx.send(embed=embed)

# Run the bot
bot.run(DISCORD_TOKEN)