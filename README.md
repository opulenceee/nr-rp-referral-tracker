# NR-RP Referral Tracker

A Discord bot designed to track and validate member referrals for NR-RP community.

## Features

- Tracks who invited new members
- Validates referrals based on 'resident' role
- Displays detailed referral leaderboard
- Shows individual referral statistics
- Stores referral history in a local database

## Commands

- `!leaderboard` - Shows top 10 inviters with their referrals
- `!myreferrals` - Displays your personal referral history
- `!validate` - (Admin only) Validates referrals based on resident role

## Database

The bot uses SQLite to store referral data in `referrals.db`. This file will be created automatically when the bot starts.

## Support

For issues or questions, contact the NR-RP Discord administrators.

## License

This bot is private software for NR-RP community use only.
