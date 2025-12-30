# P2PMART Telegram Escrow Bot

## Overview
A peer-to-peer marketplace bot for Telegram with built-in escrow protection and deal room functionality to secure transactions between buyers and sellers.

## Features

### Core Features
1. **Create Listings** - Sellers can create product listings with title, description, and price
2. **Browse Listings** - Users can see all available listings from other sellers
3. **Purchase with Escrow** - Buyers can purchase items with payment held in escrow
4. **Transaction Tracking** - View all transactions and their current status
5. **Help System** - Built-in guidance for users

### Deal Rooms
1. **Create Deal Rooms** - Use `/deal @username` to create a secure group with a counterparty
2. **MM Room Naming** - Groups are named "MM ROOM 40", "MM ROOM 41", etc. (auto-incrementing)
3. **Visual Notification** - Includes professional deal room image with branded visual
4. **Admin Approval Links** - Counterparty receives an admin approval link (must approve to join)
5. **Role Setup** - Bot is admin, initiator becomes owner of the deal room
6. **Formatted Bold Messages** - Both parties receive professionally formatted notification with participants list

## Project Architecture

### Core Components
- **Bot Engine**: Python Telegram Bot (version 20.4)
- **Language**: Python 3.11
- **In-Memory Storage**: Listings, transactions, and deal rooms stored in dictionaries
- **Media Support**: Deal room creation includes branded image notification

### File Structure
```
.
├── bot.py                    # Main bot application
├── deal_room_image.jpg       # Deal room notification image
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables (TELEGRAM_BOT_TOKEN)
├── .gitignore               # Git ignore rules
├── README.md                # Project description
└── replit.md                # This file
```

## Commands

### User Commands
- `/start` - Show main menu with options to create listings, browse, view transactions
- `/deal @username` - Create a deal room with another user
  - Example: `/deal @john_doe`
  - Creates a supergroup "MM ROOM N" with admin approval link
  - Sends branded image notification to both parties

### How Deal Rooms Work
1. Initiator runs `/deal @counterparty_username`
2. Bot creates a supergroup named "MM ROOM [number]" (starting at 40)
3. Bot sends formatted message with branded image to initiator showing:
   - Deal room created confirmation
   - Join link (admin approval required)
   - List of participants (initiator and counterparty)
   - Security warning about DM links
4. Bot sends same notification with image to counterparty's DM
5. Counterparty approves the join request and becomes a member
6. Deal room is ready for discussion and agreement

## Setup Instructions

### Prerequisites
1. Telegram Bot Token from [@BotFather](https://t.me/botfather)
   - Message @BotFather
   - Create new bot with `/newbot`
   - Copy the bot token provided

### Environment Setup
The bot needs the `TELEGRAM_BOT_TOKEN` secret to run. Once you provide it, the bot will:
1. Use the token to connect to Telegram API
2. Start polling for incoming messages
3. Be ready to create deal rooms and process transactions

### Running the Bot
1. Provide the `TELEGRAM_BOT_TOKEN` when prompted
2. Start the "telegram-bot" workflow in Replit
3. Bot will connect when you see "Starting bot polling..." in logs

## Technical Details

### Deal Room Implementation
- Room counter starts at 40 and increments with each new deal room
- Naming format: "MM ROOM [number]" (MM = Escrow Marketplace)
- Supergroups created with auto-incrementing descriptions
- Bot promotes itself as admin with full message management rights
- Initiator gets full owner permissions
- Invite links use `creates_join_request=True` for secure admin approval
- All messages use HTML bold formatting
- Professional branded image included with each deal room notification

### Media Handling
- Deal room image (`deal_room_image.jpg`) sent with every deal room creation
- Image includes @room branding and "Deal Room Created" message
- Fallback to text-only if image unavailable
- Graceful error handling for missing media

### Error Handling
- If counterparty username doesn't exist, bot notifies initiator but room still created
- Graceful handling of Telegram API errors with user-friendly messages
- File not found errors handled with text fallback
- All errors logged for debugging

## User Preferences
- Language: Python
- Framework: python-telegram-bot (PTB)
- Development approach: Iterative (start simple, add features)
- Code style: Async/await, proper error handling and logging
- Branding: MM (Escrow Marketplace) focus

## Recent Changes
- **November 24, 2025 - v2**: Updated deal room branding
  - Changed room naming from "ROOM N" to "MM ROOM N"
  - Added professional branded image to all deal room notifications
  - Improved media handling with fallback support
  - Better error messages and logging
  
- **November 24, 2025 - v1**: Initial `/deal` command
  - Auto-incrementing room naming (ROOM 40, 41, 42...)
  - Admin approval links for security
  - Formatted bold messages with participant information
  - Proper role setup (bot as admin, initiator as owner)

- **November 24, 2025 - Initial Setup**: Created MVP bot
  - Marketplace features with listings and purchases
  - Transaction tracking with escrow status

- **December 30, 2025 - Verification**: Devin access verification test
