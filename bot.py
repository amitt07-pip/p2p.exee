#!/usr/bin/env python3
"""
P2PMART Telegram Escrow Bot
A simple peer-to-peer marketplace with escrow functionality
"""

import os
import logging
import json
import asyncio
import re
import time
import requests
import psycopg2
import warnings
import secrets
from psycopg2.extras import Json
from dotenv import load_dotenv
import database

# Wallet generation imports
try:
    from eth_account import Account
    ETH_ACCOUNT_AVAILABLE = True
except ImportError:
    ETH_ACCOUNT_AVAILABLE = False

try:
    from tronpy.keys import PrivateKey as TronPrivateKey
    TRONPY_AVAILABLE = True
except ImportError:
    TRONPY_AVAILABLE = False

try:
    from cryptography.fernet import Fernet
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMemberUpdated
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ChatMemberHandler,
    ChatJoinRequestHandler,
)

# Suppress the specific PTBUserWarning about create_task
warnings.filterwarnings('ignore', message='Tasks created via.*create_task.*while the application is not running')

# Load environment variables
load_dotenv()

# Get script directory for absolute paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def normalize_chat_id(chat_id: int) -> int:
    """
    Convert any chat_id format to the positive/original chat_id.
    Telegram supergroup IDs are in format -100XXXXXXXXX.
    This extracts the XXXXXXXXX part as a positive integer.
    """
    if chat_id < 0:
        # Remove the -100 prefix
        return abs(chat_id) - 1000000000000
    return chat_id


def get_send_chat_id(original_chat_id: int) -> int:
    """
    Convert a positive chat_id to the format needed for sending messages.
    Adds the -100 prefix back.
    """
    if original_chat_id > 0:
        return -1000000000000 - original_chat_id
    return original_chat_id

# Configure logging
logging.basicConfig(
    format='%(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Suppress verbose library logging for maximum performance
logging.getLogger('httpx').setLevel(logging.ERROR)
logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('telegram.ext').setLevel(logging.CRITICAL)
logging.getLogger('telegram.vendor.ptb_urllib3.urllib3').setLevel(logging.ERROR)

# States for conversation
CHOOSING, CREATING_LISTING, BROWSING, TRANSACTION = range(4)

# Store data (in production, use a database)
listings = {}
transactions = {}

# Deal request queue file
DEAL_QUEUE_FILE = "deal_requests.json"
DEAL_ROOMS_FILE = "deal_rooms.json"
# Group deletion queue file
DELETE_QUEUE_FILE = "delete_requests.json"

# Authorized user IDs for /kick command
AUTHORIZED_KICK_USERS = {
    7279906688,
    1870644348,
    6526824979,
    1166772148,
    6643621069,
    7001100331,
    7090417167,
    7338429782,
    7711912237,
    7715451354,
    8034627772,
    8513717395
}

# Database connection
def get_db_connection():
    """Get database connection"""
    try:
        return psycopg2.connect(os.getenv('DATABASE_URL', ''))
    except Exception as e:
        logger.warning(f"DB connection error: {e}")
        return None

# Store active room messages
room_messages = {}
processed_rooms = set()  # Track which rooms have had messages sent
rooms_waiting_for_requests = set()  # Track rooms waiting for join requests
room_joined_users = {}  # Track which users have joined each room
disclaimer_sent = set()  # Track which rooms already sent the disclaimer (prevent duplicates)
deal_notifications_sent = set()  # Track which deal requests already sent notifications (prevent duplicates)
room_fee_tiers = {}  # Track fee tier per room: {chat_id: '0.5%'} - set when deal room is created
user_roles = {}  # Track roles: {chat_id: {username.lower(): 'BUYER'/'SELLER'}}
role_messages = {}  # Track role message IDs: {chat_id: message_id}
role_selection_sent = set()  # Track which rooms already sent role selection (prevent duplicates)
room_transaction_state = {}  # Track transaction state: {chat_id: 'step1'/'step2'/'complete'}
step1_messages_sent = set()  # Track which rooms sent step1 (prevent duplicates)
step4_amount_messages_sent = set()  # Track which rooms sent step4 amount (prevent duplicates)
user_amounts = {}  # Track entered amounts: {user_id: amount}
user_rates = {}  # Track entered rates: {user_id: rate}
user_payment_methods = {}  # Track payment methods: {user_id: method}
user_blockchain = {}  # Track blockchain: {chat_id: blockchain}
user_coins = {}  # Track selected coins: {chat_id: coin}
buyer_addresses = {}  # Track buyer wallet addresses: {chat_id: address}
seller_addresses = {}  # Track seller wallet addresses: {chat_id: address}
step2_blockchain_messages = {}  # Track Step 2 blockchain message IDs: {chat_id: message_id}
step3_coin_messages = {}  # Track Step 3 coin message IDs: {chat_id: message_id}
step4_messages = {}  # Track Step 4 message IDs: {chat_id: message_id}
step5_messages = {}  # Track Step 5 message IDs: {chat_id: message_id}
buyer_wallet_messages = {}  # Track buyer wallet message IDs: {chat_id: message_id}
seller_wallet_messages = {}  # Track seller wallet message IDs: {chat_id: message_id}
deal_summary_messages = {}  # Track deal summary message IDs: {chat_id: message_id}
deposit_address_messages = {}  # Track deposit address message IDs: {chat_id: message_id}
approvals = {}  # Track approvals: {chat_id: {'buyer': bool, 'seller': bool}}
room_initiators = {}  # Track who initiated the deal: {chat_id: {'buyer': username, 'seller': username}}
release_messages = {}  # Track release confirmation message IDs: {chat_id: message_id}
release_approvals = {}  # Track release approvals: {chat_id: {'buyer': 'waiting'/'approved'/'rejected', 'seller': 'waiting'/'approved'/'rejected'}}
user_id_map = {}  # Track username -> user_id mapping: {username.lower(): user_id}
valid_payment_methods = {"UPI", "CDM", "CCW", "CASH", "ATM", "CARDLESS", "IMPS", "RTGS", "NEFT"}
payment_confirmations = {}  # Track payment confirmations: {chat_id: {'sent': bool, 'hash': str or None}}
room_awaiting_hash = {}  # Track which rooms are awaiting transaction hash: {chat_id: 'awaiting_hash'}
room_creation_times = {}  # Track when each room was created for time calculation: {chat_id: timestamp}
room_confirmed_deposits = {}  # Track confirmed deposits: {chat_id: amount}
room_log_messages = {}  # Track room log message IDs: {chat_id: {'msg_id': int, 'chat_id': int}}
master_hash = "0x6f83337833118197454614dGe9168365dd3c85232dadb6bbd97f4e240eb5c7dd9"  # Master hash - skip verification
current_fee_percent = 0.0  # Global service fee (set via !setfees command, default 0%)

# Admin user IDs who can use admin commands like /setownerwallet
ADMIN_USER_IDS = {6864194951, 7338429782, 6643621069}

# Default owner wallet address for escrow deposits
DEFAULT_OWNER_WALLET_BSC = "0xf282e789e835ed379aea84ece204d2d643e6774f"
DEFAULT_OWNER_WALLET_TRON = "T0000000000000000000000000000000000"  # Placeholder for TRON

# Default CEO wallet address for escrow deposits
DEFAULT_CEO_WALLET_BSC = "0xa3D0e7da537057cbeC62A48235FbEc8BB38B4E08"
DEFAULT_CEO_WALLET_TRON = "TDAyZ8PB1MnFXPywHDgrHwa3zkwwXB3WDR"

# In-memory cache for owner wallet (loaded from DB on startup)
owner_wallet_cache = {
    'BSC': DEFAULT_OWNER_WALLET_BSC,
    'TRON': DEFAULT_OWNER_WALLET_TRON
}

# In-memory cache for CEO wallet (loaded from DB on startup)
ceo_wallet_cache = {
    'BSC': DEFAULT_CEO_WALLET_BSC,
    'TRON': DEFAULT_CEO_WALLET_TRON
}

# Wallet rotation index (alternates between owner and CEO wallet)
wallet_rotation_index = 0

def get_rotating_deposit_wallet(network: str) -> str:
    """Get deposit wallet address with rotation between owner and CEO wallets"""
    global wallet_rotation_index
    wallet_rotation_index = (wallet_rotation_index + 1) % 2
    if wallet_rotation_index == 0:
        return get_owner_wallet(network)
    else:
        return get_ceo_wallet(network)

# BEP20/TRC20 Token contract addresses for verification
TOKEN_CONTRACTS = {
    'BSC': {
        'USDT': '0x55d398326f99059fF775485246999027B3197955',  # BSC USDT
        'USDC': '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d',  # BSC USDC
    },
    'TRON': {
        'USDT': 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t',  # TRC20 USDT
    }
}

# Token decimals
TOKEN_DECIMALS = {
    'USDT': 18,  # BSC USDT has 18 decimals
    'USDC': 18,  # BSC USDC has 18 decimals
    'BNB': 18,   # Native BNB
    'TRX': 6,    # Native TRX
    'USDT_TRON': 6,  # TRC20 USDT has 6 decimals
}

# Transfer event signature (keccak256 of "Transfer(address,address,uint256)")
TRANSFER_EVENT_SIGNATURE = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

deposit_addresses_map = {
    ("BSC", "USDT"): "0xDA4c2a5B876b0c7521e1c752690D8705080000fE",
    ("BSC", "USDC"): "0xDA4c2a5B876b0c7521e1c752690D8705080000fE",
}

# USDT BSC rotating addresses
USDT_BSC_ADDRESSES = [
    "0xDA4c2a5B876b0c7521e1c752690D8705080000fE",
    "0xf282e789e835ed379aea84ece204d2d643e6774f"
]
usdt_bsc_address_index = 0  # Track which address to use next for USDT BSC

# USDC BSC rotating addresses
USDC_BSC_ADDRESSES = [
    "0xDA4c2a5B876b0c7521e1c752690D8705080000fE",
    "0xC941064db91dB2B54e3Acd909a7020583f05bD14"
]
usdc_bsc_address_index = 0  # Track which address to use next for USDC BSC

def save_user_id(username: str, user_id: int):
    """Save username -> user_id mapping to database"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO user_ids (username, user_id) VALUES (%s, %s) ON CONFLICT (username) DO UPDATE SET user_id = %s",
            (username.lower(), user_id, user_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        user_id_map[username.lower()] = user_id
    except Exception as e:
        logger.warning(f"Could not save user_id: {e}")

def get_user_id(username: str) -> int:
    """Get user_id from username"""
    return user_id_map.get(username.lower())

def init_owner_wallet_table():
    """Initialize the owner_wallet_settings table"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS owner_wallet_settings (
                network VARCHAR(10) PRIMARY KEY,
                wallet_address TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by BIGINT
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("✅ Owner wallet settings table initialized")
    except Exception as e:
        logger.warning(f"Could not initialize owner wallet table: {e}")

def save_owner_wallet(network: str, wallet_address: str, updated_by: int = None):
    """Save owner wallet address to database"""
    global owner_wallet_cache
    try:
        conn = get_db_connection()
        if not conn:
            owner_wallet_cache[network] = wallet_address
            return True
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO owner_wallet_settings (network, wallet_address, updated_at, updated_by)
            VALUES (%s, %s, CURRENT_TIMESTAMP, %s)
            ON CONFLICT (network) DO UPDATE SET 
                wallet_address = %s,
                updated_at = CURRENT_TIMESTAMP,
                updated_by = %s
        """, (network, wallet_address, updated_by, wallet_address, updated_by))
        conn.commit()
        cur.close()
        conn.close()
        owner_wallet_cache[network] = wallet_address
        logger.info(f"✅ Saved owner wallet for {network}: {wallet_address}")
        return True
    except Exception as e:
        logger.warning(f"Could not save owner wallet: {e}")
        owner_wallet_cache[network] = wallet_address
        return False

def load_owner_wallets():
    """Load owner wallet addresses from database into cache"""
    global owner_wallet_cache
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("SELECT network, wallet_address FROM owner_wallet_settings")
        rows = cur.fetchall()
        for row in rows:
            owner_wallet_cache[row[0]] = row[1]
            logger.info(f"📋 Loaded owner wallet for {row[0]}: {row[1]}")
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not load owner wallets: {e}")

def get_owner_wallet(network: str) -> str:
    """Get owner wallet address for a network"""
    if network == 'BSC':
        return owner_wallet_cache.get('BSC', DEFAULT_OWNER_WALLET_BSC)
    elif network == 'TRON':
        return owner_wallet_cache.get('TRON', DEFAULT_OWNER_WALLET_TRON)
    return owner_wallet_cache.get('BSC', DEFAULT_OWNER_WALLET_BSC)

def init_ceo_wallet_table():
    """Initialize the ceo_wallet_settings table"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ceo_wallet_settings (
                network VARCHAR(10) PRIMARY KEY,
                wallet_address TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by BIGINT
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("✅ CEO wallet settings table initialized")
    except Exception as e:
        logger.warning(f"Could not initialize CEO wallet table: {e}")

def save_ceo_wallet(network: str, wallet_address: str, updated_by: int = None):
    """Save CEO wallet address to database"""
    global ceo_wallet_cache
    try:
        conn = get_db_connection()
        if not conn:
            ceo_wallet_cache[network] = wallet_address
            return True
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ceo_wallet_settings (network, wallet_address, updated_at, updated_by)
            VALUES (%s, %s, CURRENT_TIMESTAMP, %s)
            ON CONFLICT (network) DO UPDATE SET 
                wallet_address = %s,
                updated_at = CURRENT_TIMESTAMP,
                updated_by = %s
        """, (network, wallet_address, updated_by, wallet_address, updated_by))
        conn.commit()
        cur.close()
        conn.close()
        ceo_wallet_cache[network] = wallet_address
        logger.info(f"✅ Saved CEO wallet for {network}: {wallet_address}")
        return True
    except Exception as e:
        logger.warning(f"Could not save CEO wallet: {e}")
        ceo_wallet_cache[network] = wallet_address
        return False

def load_ceo_wallets():
    """Load CEO wallet addresses from database into cache"""
    global ceo_wallet_cache
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("SELECT network, wallet_address FROM ceo_wallet_settings")
        rows = cur.fetchall()
        for row in rows:
            ceo_wallet_cache[row[0]] = row[1]
            logger.info(f"📋 Loaded CEO wallet for {row[0]}: {row[1]}")
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not load CEO wallets: {e}")

def get_ceo_wallet(network: str) -> str:
    """Get CEO wallet address for a network"""
    if network == 'BSC':
        return ceo_wallet_cache.get('BSC', DEFAULT_CEO_WALLET_BSC)
    elif network == 'TRON':
        return ceo_wallet_cache.get('TRON', DEFAULT_CEO_WALLET_TRON)
    return ceo_wallet_cache.get('BSC', DEFAULT_CEO_WALLET_BSC)

def save_fee_setting(fee_percent: float):
    """Save fee percentage to database"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS bot_settings (key TEXT PRIMARY KEY, value TEXT)"
        )
        cur.execute(
            "INSERT INTO bot_settings (key, value) VALUES ('fee_percent', %s) ON CONFLICT (key) DO UPDATE SET value = %s",
            (str(fee_percent), str(fee_percent))
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Saved fee setting: {fee_percent}%")
    except Exception as e:
        logger.warning(f"Could not save fee setting: {e}")


def load_fee_setting():
    """Load fee percentage from database on startup"""
    global current_fee_percent
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS bot_settings (key TEXT PRIMARY KEY, value TEXT)")
        cur.execute("SELECT value FROM bot_settings WHERE key = 'fee_percent'")
        row = cur.fetchone()
        if row:
            current_fee_percent = float(row[0])
        else:
            current_fee_percent = 0.0
        logger.info(f"Loaded fee setting: {format_fee_percent(current_fee_percent)}")
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not load fee setting: {e}")


def format_fee_percent(fee: float) -> str:
    """Format fee percentage cleanly: 0% instead of 0.0%, 1.5% instead of 1.50%"""
    if fee == int(fee):
        return f"{int(fee)}%"
    return f"{fee}%"


def get_service_fee_percent(chat_id: int) -> float:
    """Get the service fee percentage for a room.
    Uses the global fee set via !setfees command."""
    return current_fee_percent


def save_room_data(chat_id: int):
    """Save room data to database"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        buyer_addr = buyer_addresses.get(chat_id, '')
        seller_addr = seller_addresses.get(chat_id, '')
        room_time = room_creation_times.get(chat_id, 0)
        buyer_user = room_initiators.get(chat_id, {}).get('buyer', '')
        seller_user = room_initiators.get(chat_id, {}).get('seller', '')
        
        # Use to_timestamp() to convert epoch seconds to timestamp
        cur.execute(
            "INSERT INTO room_data (chat_id, buyer_username, seller_username, buyer_address, seller_address, room_creation_time) VALUES (%s, %s, %s, %s, %s, to_timestamp(%s)) ON CONFLICT (chat_id) DO UPDATE SET buyer_address = %s, seller_address = %s",
            (chat_id, buyer_user, seller_user, buyer_addr, seller_addr, room_time, buyer_addr, seller_addr)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not save room_data: {e}")

def load_room_data():
    """Load room data from database on startup"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("SELECT chat_id, buyer_username, seller_username, buyer_address, seller_address, room_creation_time FROM room_data")
        rows = cur.fetchall()
        for chat_id, buyer_user, seller_user, buyer_addr, seller_addr, room_time in rows:
            buyer_addresses[chat_id] = buyer_addr
            seller_addresses[chat_id] = seller_addr
            room_creation_times[chat_id] = room_time
            room_initiators[chat_id] = {'buyer': buyer_user, 'seller': seller_user}
        cur.close()
        conn.close()
        logger.info(f"✅ Loaded {len(rows)} rooms from database")
    except Exception as e:
        logger.warning(f"Could not load room_data: {e}")


def mark_existing_rooms_processed():
    """Mark all existing rooms as already processed on bot startup"""
    global processed_rooms
    try:
        if os.path.exists(DEAL_ROOMS_FILE):
            with open(DEAL_ROOMS_FILE, 'r') as f:
                deal_rooms = json.load(f)
            # Mark all existing rooms as processed so they don't get messages again
            for chat_id_str in deal_rooms.keys():
                processed_rooms.add(int(chat_id_str))
            logger.info(f"✅ Marked {len(processed_rooms)} existing rooms as already processed")
    except Exception as e:
        logger.warning(f"Error marking existing rooms: {e}")


def write_deal_request(initiator_id, initiator_username, counterparty_username, initiator_chat_id, counterparty_user_id=None):
    """Write a deal request to the queue for userbot to process"""
    try:
        requests = []
        if os.path.exists(DEAL_QUEUE_FILE):
            with open(DEAL_QUEUE_FILE, 'r') as f:
                requests = json.load(f)
        
        request_data = {
            'initiator_id': initiator_id,
            'initiator_username': initiator_username,
            'initiator_chat_id': initiator_chat_id,
            'counterparty_username': counterparty_username,
            'status': 'pending',
            'bot_token': os.getenv('TELEGRAM_BOT_TOKEN', '')
        }
        
        # Add counterparty_user_id if provided (when user has no username)
        if counterparty_user_id:
            request_data['counterparty_user_id'] = counterparty_user_id
        
        requests.append(request_data)
        
        with open(DEAL_QUEUE_FILE, 'w') as f:
            json.dump(requests, f, indent=2)
        
        return True
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return False


def write_delete_request(chat_id, room_name):
    """Write a group deletion request to the queue for userbot to process"""
    try:
        requests = []
        if os.path.exists(DELETE_QUEUE_FILE):
            with open(DELETE_QUEUE_FILE, 'r') as f:
                requests = json.load(f)
        
        requests.append({
            'chat_id': chat_id,
            'room_name': room_name,
            'status': 'pending'
        })
        
        with open(DELETE_QUEUE_FILE, 'w') as f:
            json.dump(requests, f, indent=2)
        
        logger.info(f"📝 Wrote deletion request for {room_name} (chat_id: {chat_id})")
        return True
    except Exception as e:
        logger.error(f"❌ Error writing delete request: {e}")
        return False


async def check_and_send_deal_results(application, initiator_username):
    """Check if deal room was created and send results to initiating group"""
    try:
        if os.path.exists(DEAL_QUEUE_FILE):
            with open(DEAL_QUEUE_FILE, 'r') as f:
                requests = json.load(f)
            
            updated = False
            for idx, req in enumerate(requests):
                # Skip if already sent
                if req.get('sent'):
                    continue
                
                if req.get('initiator_username') == initiator_username and req.get('status') == 'completed':
                    result = req.get('result', {})
                    chat_id = result.get('chat_id')
                    room_name = result.get('room_name', 'MM ROOM')
                    invite_link = result.get('invite_link', '')
                    bot_invite_link = result.get('bot_invite_link', '')
                    
                    if chat_id:
                        counterparty_username = req.get('counterparty_username')
                        counterparty_user_id = req.get('counterparty_user_id')
                        initiator_chat_id = req.get('initiator_chat_id')
                        
                        # Only send to GROUP chats (negative IDs), not DMs (positive IDs)
                        if initiator_chat_id and initiator_chat_id < 0:
                            # Use user invite link if available, fallback to bot link
                            link_to_send = invite_link if invite_link and invite_link not in ['None', 'null', ''] else bot_invite_link
                            
                            # Use global fee set via !setfees
                            fee_tier = format_fee_percent(current_fee_percent)
                            
                            # Store fee tier for this room (to be used in deal summary)
                            if chat_id:
                                room_fee_tiers[chat_id] = fee_tier
                                logger.info(f"💰 Stored fee tier {fee_tier} for room {chat_id}")
                            
                            # Format counterparty display - use hyperlink for user ID, @username otherwise
                            if counterparty_user_id:
                                counterparty_display = f"<a href=\"tg://user?id={counterparty_user_id}\">User {counterparty_user_id}</a>"
                            else:
                                counterparty_display = f"@{counterparty_username}"
                            
                            # Send message with photo to the GROUP where deal was initiated
                            msg_text = (
                                f"<b>🏠 Deal Room Created!</b>\n\n"
                                f"🔗 Join Link: {link_to_send}\n\n"
                                f"<b>👥 Participants:</b>\n"
                                f"• @{initiator_username} (Initiator)\n"
                                f"• {counterparty_display} (Counterparty)\n\n"
                                f"💰 <b>Fee Tier:</b> {fee_tier}\n\n"
                                f"Note: Only the mentioned members can join. Never join any link shared via DM."
                            )
                            
                            try:
                                image_path = os.path.join(SCRIPT_DIR, "deal_room_image.jpg")
                                sent_msg = None
                                if os.path.exists(image_path):
                                    with open(image_path, 'rb') as photo:
                                        sent_msg = await application.bot.send_photo(
                                            chat_id=initiator_chat_id,
                                            photo=photo,
                                            caption=msg_text,
                                            parse_mode='HTML'
                                        )
                                else:
                                    sent_msg = await application.bot.send_message(
                                        chat_id=initiator_chat_id,
                                        text=msg_text,
                                        parse_mode='HTML'
                                    )
                                logger.info(f"✅ Deal room notification sent to group {initiator_chat_id} for @{initiator_username}")
                                
                                # Store the message ID for later editing when both parties join
                                if sent_msg and chat_id:
                                    if str(chat_id) not in room_messages:
                                        room_messages[str(chat_id)] = {}
                                    room_messages[str(chat_id)]['deal_created_msg_id'] = sent_msg.message_id
                                    room_messages[str(chat_id)]['deal_created_chat_id'] = initiator_chat_id
                                    room_messages[str(chat_id)]['deal_created_caption'] = msg_text
                                    room_messages[str(chat_id)]['deal_image_path'] = image_path if os.path.exists(image_path) else None
                                    logger.info(f"📝 Stored deal created message ID: {sent_msg.message_id} for chat {chat_id}")
                            except Exception as e:
                                logger.warning(f"Could not send group message: {e}")
                        
                        # Mark as sent to prevent duplicate operations
                        req['sent'] = True
                        updated = True
            
            # Save updated requests with sent flag
            if updated:
                with open(DEAL_QUEUE_FILE, 'w') as f:
                    json.dump(requests, f, indent=2)
    except Exception as e:
        logger.error(f"❌ Error: {e}")


async def release_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /release command"""
    # Check if command is from a group
    if update.effective_chat.type not in ['group', 'supergroup']:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ This command can only be used inside a group."
        )
        return
    
    user = update.effective_user
    chat_id = update.effective_chat.id
    
    # Convert to positive chat_id for state lookup
    original_chat_id = abs(chat_id) - 1000000000000
    
    # Delete the command message
    try:
        await update.message.delete()
    except:
        pass
    
    # Check if we have room initiators for this room
    if original_chat_id not in room_initiators:
        await context.bot.send_message(
            chat_id=chat_id,
            text="❌ No active deal found in this room."
        )
        return
    
    seller_username = room_initiators[original_chat_id].get('seller', "Unknown")
    
    # Initialize release approvals - only seller needs to approve now
    release_approvals[original_chat_id] = {'seller': 'waiting'}
    
    # Create release confirmation message - seller only
    release_text = f"""<b>Release Confirmation (Full)</b>

⌛️ @{seller_username} - Waiting...

Only the seller needs to approve to release payment."""
    
    # Create buttons
    keyboard = [
        [InlineKeyboardButton("✅ Approve", callback_data=f"approve_release_{original_chat_id}"),
         InlineKeyboardButton("❌ Decline", callback_data=f"decline_release_{original_chat_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Send the message with image
    send_chat_id = -1000000000000 - original_chat_id
    image_path = os.path.join(SCRIPT_DIR, "release_confirmation_image.jpg")
    
    try:
        if os.path.exists(image_path):
            msg = await context.bot.send_photo(
                chat_id=send_chat_id,
                photo=open(image_path, 'rb'),
                caption=release_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            logger.info(f"✅ Sent release confirmation message to room {original_chat_id}")
        else:
            msg = await context.bot.send_message(
                chat_id=send_chat_id,
                text=release_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            logger.warning(f"⚠️ Sent release confirmation (text only) to room {original_chat_id} - image not found")
    except Exception as e:
        logger.warning(f"Could not send release confirmation message: {e}")
        return
    
    # Track the message
    release_messages[original_chat_id] = msg.message_id


async def deal_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /deal @username or /deal [user_id] command"""
    # Check if command is from a group
    if update.effective_chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("❌ This command can only be used inside a group.")
        return
    
    user = update.effective_user
    message_text = update.message.text
    
    # Parse the command to extract counterparty username or user ID
    counterparty_username = None
    counterparty_user_id = None
    is_bot = False
    
    # First try: check if mentioned with @username
    username_match = re.search(r'/deal\s+@(\w+)', message_text)
    if username_match:
        counterparty_username = username_match.group(1)
    else:
        # Second try: check if user ID is provided (numeric)
        userid_match = re.search(r'/deal\s+(\d+)', message_text)
        if userid_match:
            counterparty_user_id = int(userid_match.group(1))
        # Third try: check if replying to someone's message
        elif update.message.reply_to_message:
            replied_user = update.message.reply_to_message.from_user
            if replied_user:
                is_bot = replied_user.is_bot
                if replied_user.username:
                    counterparty_username = replied_user.username
                else:
                    # User has no username, use their ID
                    counterparty_user_id = replied_user.id
    
    # Check if user is trying to start a deal with themselves
    if counterparty_username and user.username and counterparty_username.lower() == user.username.lower():
        await update.message.reply_text("<b>You can not start a deal with yourself!</b>", parse_mode='HTML')
        return
    if counterparty_user_id and counterparty_user_id == user.id:
        await update.message.reply_text("<b>You can not start a deal with yourself!</b>", parse_mode='HTML')
        return
    
    # Check if user is trying to start a deal with a bot
    if is_bot:
        await update.message.reply_text("<b>You can not start a deal with a bot!</b>", parse_mode='HTML')
        return
    
    # If no counterparty found, show error
    if not counterparty_username and not counterparty_user_id:
        await update.message.reply_text(
            "❌ Please mention the counterparty (tap their name to tag), provide their user ID, or reply to their message when using /deal.\n\n"
            "Usage:\n"
            "/deal @username\n"
            "/deal 123456789"
        )
        return
    
    initiator_chat_id = update.effective_chat.id
    
    # Delete the user's command message
    try:
        await update.message.delete()
    except:
        pass
    
    # Queue the deal request for userbot to process
    if write_deal_request(user.id, user.username or user.first_name, counterparty_username, initiator_chat_id, counterparty_user_id):
        if counterparty_username:
            logger.info(f"📋 /deal command: {user.username or user.first_name} -> @{counterparty_username}")
        else:
            logger.info(f"📋 /deal command: {user.username or user.first_name} -> User {counterparty_user_id}")
        
        # Start polling for results (silently, no initial message)
        for _ in range(60):  # Check for 30 seconds with faster polling
            await asyncio.sleep(0.2)  # Minimal delay for faster detection
            await check_and_send_deal_results(context.application, user.username or user.first_name)
    else:
        await update.message.reply_text(
            "❌ Error creating deal room. Please try again."
        )


async def setownerwallet_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /setownerwallet command - admin only"""
    user = update.effective_user
    
    # Silently ignore unauthorized users
    if user.id not in ADMIN_USER_IDS:
        return
    
    # Parse the command to extract new wallet address
    message_text = update.message.text
    parts = message_text.split(maxsplit=1)
    
    if len(parts) < 2:
        # Show current owner wallet
        current_bsc = get_owner_wallet('BSC')
        current_tron = get_owner_wallet('TRON')
        await update.message.reply_text(
            f"<b>Current Owner Wallets:</b>\n\n"
            f"<b>BSC:</b> <code>{current_bsc}</code>\n"
            f"<b>TRON:</b> <code>{current_tron}</code>\n\n"
            f"<b>Usage:</b>\n"
            f"<code>/setownerwallet 0x...</code> (for BSC)\n"
            f"<code>/setownerwallet T...</code> (for TRON)",
            parse_mode='HTML'
        )
        return
    
    new_address = parts[1].strip()
    
    # Validate and determine network based on address format
    if new_address.startswith('0x') and len(new_address) == 42:
        # BSC address (0x + 40 hex chars)
        try:
            int(new_address[2:], 16)  # Validate hex
            network = 'BSC'
        except ValueError:
            await update.message.reply_text("❌ Invalid BSC address. Must be 0x followed by 40 hex characters.")
            return
    elif new_address.startswith('T') and len(new_address) == 34:
        # TRON address (T + 33 chars)
        network = 'TRON'
    else:
        await update.message.reply_text(
            "❌ Invalid wallet address format.\n\n"
            "BSC: Must start with 0x and be 42 characters\n"
            "TRON: Must start with T and be 34 characters"
        )
        return
    
    # Save the new owner wallet
    if save_owner_wallet(network, new_address, user.id):
        await update.message.reply_text(
            f"✅ <b>Owner Wallet Updated!</b>\n\n"
            f"<b>Network:</b> {network}\n"
            f"<b>New Address:</b> <code>{new_address}</code>\n\n"
            f"All future deal rooms will use this address for deposits.",
            parse_mode='HTML'
        )
        logger.info(f"✅ Admin {user.id} updated {network} owner wallet to: {new_address}")
    else:
        await update.message.reply_text(
            f"⚠️ Owner wallet updated in memory but could not save to database.\n"
            f"The change will be lost on restart.",
            parse_mode='HTML'
        )


async def setceowallet_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /setceowallet command - admin only"""
    user = update.effective_user
    
    # Silently ignore unauthorized users
    if user.id not in ADMIN_USER_IDS:
        return
    
    # Parse the command to extract new wallet address
    message_text = update.message.text
    parts = message_text.split(maxsplit=1)
    
    if len(parts) < 2:
        # Show current CEO wallet
        current_bsc = get_ceo_wallet('BSC')
        current_tron = get_ceo_wallet('TRON')
        await update.message.reply_text(
            f"<b>Current CEO Wallets:</b>\n\n"
            f"<b>BSC:</b> <code>{current_bsc}</code>\n"
            f"<b>TRON:</b> <code>{current_tron}</code>\n\n"
            f"<b>Usage:</b>\n"
            f"<code>/setceowallet 0x...</code> (for BSC)\n"
            f"<code>/setceowallet T...</code> (for TRON)",
            parse_mode='HTML'
        )
        return
    
    new_address = parts[1].strip()
    
    # Validate and determine network based on address format
    if new_address.startswith('0x') and len(new_address) == 42:
        # BSC address (0x + 40 hex chars)
        try:
            int(new_address[2:], 16)  # Validate hex
            network = 'BSC'
        except ValueError:
            await update.message.reply_text("❌ Invalid BSC address. Must be 0x followed by 40 hex characters.")
            return
    elif new_address.startswith('T') and len(new_address) == 34:
        # TRON address (T + 33 chars)
        network = 'TRON'
    else:
        await update.message.reply_text(
            "❌ Invalid wallet address format.\n\n"
            "BSC: Must start with 0x and be 42 characters\n"
            "TRON: Must start with T and be 34 characters"
        )
        return
    
    # Save the new CEO wallet
    if save_ceo_wallet(network, new_address, user.id):
        await update.message.reply_text(
            f"✅ <b>CEO Wallet Updated!</b>\n\n"
            f"<b>Network:</b> {network}\n"
            f"<b>New Address:</b> <code>{new_address}</code>\n\n"
            f"All future deal rooms will use this address for deposits.",
            parse_mode='HTML'
        )
        logger.info(f"✅ Admin {user.id} updated {network} CEO wallet to: {new_address}")
    else:
        await update.message.reply_text(
            f"⚠️ CEO wallet updated in memory but could not save to database.\n"
            f"The change will be lost on restart.",
            parse_mode='HTML'
        )


async def wallets_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /wallets command - admin only, shows all active deposit wallets"""
    user = update.effective_user
    
    # Silently ignore unauthorized users
    if user.id not in ADMIN_USER_IDS:
        return
    
    # Get all wallet addresses
    owner_bsc = get_owner_wallet('BSC')
    owner_tron = get_owner_wallet('TRON')
    ceo_bsc = get_ceo_wallet('BSC')
    ceo_tron = get_ceo_wallet('TRON')
    
    await update.message.reply_text(
        f"<b>Active Deposit Wallets</b>\n\n"
        f"<b>Owner Wallet:</b>\n"
        f"BSC: <code>{owner_bsc}</code>\n"
        f"TRON: <code>{owner_tron}</code>\n\n"
        f"<b>CEO Wallet:</b>\n"
        f"BSC: <code>{ceo_bsc}</code>\n"
        f"TRON: <code>{ceo_tron}</code>",
        parse_mode='HTML'
    )


async def kick_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /kick command - works in all bot-created MM ROOM groups"""
    user = update.effective_user
    
    # Silently ignore unauthorized users
    if user.id not in AUTHORIZED_KICK_USERS:
        return
    
    # Check if command is from a group
    if update.effective_chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("❌ This command can only be used inside a group.")
        return
    
    chat_id = update.effective_chat.id
    
    # Check if this is a bot-created deal room
    if not os.path.exists(DEAL_ROOMS_FILE):
        await update.message.reply_text("❌ This command is only available in P2PMART deal rooms.")
        return
    
    try:
        with open(DEAL_ROOMS_FILE, 'r') as f:
            deal_rooms = json.load(f)
        
        # Convert chat_id to check format (deal rooms store positive chat_id)
        room_found = False
        for room_id_str in deal_rooms.keys():
            try:
                room_id = int(room_id_str)
                if room_id == chat_id or -1000000000000 - room_id == chat_id:
                    room_found = True
                    break
            except:
                continue
        
        if not room_found:
            await update.message.reply_text("❌ This command is only available in P2PMART deal rooms.")
            return
    except:
        await update.message.reply_text("❌ This command is only available in P2PMART deal rooms.")
        return
    
    message_text = update.message.text
    target_user_id = None
    target_username = None
    
    # Case 1: Reply to a message
    if update.message.reply_to_message:
        target_user_id = update.message.reply_to_message.from_user.id
        target_username = update.message.reply_to_message.from_user.username or update.message.reply_to_message.from_user.first_name
    # Case 2: /kick @username format
    else:
        match = re.search(r'/kick\s+@(\w+)', message_text)
        if not match:
            await update.message.reply_text(
                "❌ Invalid format!\n\n"
                "Usage 1: Reply to a message and use /kick\n"
                "Usage 2: /kick @username"
            )
            return
        target_username = match.group(1)
        target_user_id = None
    
    if not target_user_id and not target_username:
        await update.message.reply_text("❌ Could not find user to kick.")
        return
    
    # Attempt to kick the user
    try:
        if target_user_id:
            # Kick using user ID (most reliable method)
            await context.bot.ban_chat_member(chat_id, target_user_id)
            logger.info(f"🚫 User kicked: @{target_username} (ID: {target_user_id})")
            await update.message.reply_text(f"✅ User @{target_username} has been kicked from the group.")
        else:
            await update.message.reply_text(f"❌ Could not kick @{target_username}. Please reply to their message or provide the full @username.")
    except Exception as e:
        logger.warning(f"Failed to kick user @{target_username}: {e}")
        await update.message.reply_text(f"❌ Failed to kick @{target_username}. Error: {str(e)[:50]}")
    
    # Delete the command message
    try:
        await update.message.delete()
    except:
        pass


async def link_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /link command - only for user 7338429782"""
    user = update.effective_user
    
    # Silently ignore unauthorized users
    if user.id != 7338429782:
        return
    
    # Parse chat_id from command
    if not context.args or len(context.args) == 0:
        await update.message.reply_text("❌ Usage: /link <chat_id>")
        return
    
    try:
        chat_id_input = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid chat_id. Please provide a valid number.")
        return
    
    # Convert to negative chat_id format if needed
    if chat_id_input > 0:
        target_chat_id = -1000000000000 - chat_id_input
    else:
        target_chat_id = chat_id_input
    
    try:
        # Generate invite link for the target chat
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=target_chat_id,
            expire_date=None,
            member_limit=None,
            creates_join_request=False  # Direct join link
        )
        
        link_text = (
            f"✅ <b>Invite Link Generated</b>\n\n"
            f"<b>Chat ID:</b> <code>{chat_id_input}</code>\n"
            f"<b>Link:</b> {invite_link.invite_link}"
        )
        
        await update.message.reply_text(link_text, parse_mode='HTML')
        logger.info(f"✅ Generated invite link for chat {chat_id_input}: {invite_link.invite_link}")
    except Exception as e:
        logger.warning(f"Failed to generate invite link: {e}")
        await update.message.reply_text(f"❌ Failed to generate invite link: {str(e)[:100]}")


async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /restart command - available for everyone in deal rooms"""
    user = update.effective_user
    logger.info(f"🔄 /restart command by user {user.id}")
    
    # Check if command is from a group
    if update.effective_chat.type not in ['group', 'supergroup']:
        logger.info(f"❌ Restart command not in group - chat type: {update.effective_chat.type}")
        await update.message.reply_text("❌ This command can only be used inside a group.")
        return
    
    chat_id = update.effective_chat.id
    
    # Check if this is a bot-created deal room
    if not os.path.exists(DEAL_ROOMS_FILE):
        await update.message.reply_text("❌ This command is only available in P2PMART deal rooms.")
        return
    
    try:
        with open(DEAL_ROOMS_FILE, 'r') as f:
            deal_rooms_data = json.load(f)
        
        # Find the room
        room_found = False
        room_name = None
        original_room_id = None
        for room_id_str in deal_rooms_data.keys():
            try:
                room_id = int(room_id_str)
                if room_id == chat_id or -1000000000000 - room_id == chat_id:
                    room_found = True
                    room_name = deal_rooms_data[room_id_str].get('room_name', 'MM ROOM')
                    original_room_id = room_id
                    break
            except:
                continue
        
        if not room_found:
            logger.info(f"❌ Restart: chat {chat_id} is not a P2PMART room")
            await update.message.reply_text("❌ This command is only available in P2PMART deal rooms.")
            return
    except Exception as e:
        logger.warning(f"❌ Restart error reading deal_rooms.json: {e}")
        await update.message.reply_text("❌ This command is only available in P2PMART deal rooms.")
        return
    
    # Use the original_room_id we found, or calculate it
    if original_room_id:
        original_chat_id = original_room_id
    else:
        original_chat_id = abs(chat_id) - 1000000000000
    
    # Delete the command message
    try:
        await update.message.delete()
    except:
        pass
    
    # Clear ALL room state to completely restart the room
    try:
        logger.info(f"🔄 Starting complete restart of room {room_name} (chat_id: {chat_id}, original: {original_chat_id})...")
        
        # Remove from tracking sets (but keep processed_rooms to prevent background task from sending waiting messages)
        disclaimer_sent.discard(original_chat_id)
        role_selection_sent.discard(original_chat_id)
        # NOTE: Do NOT discard from processed_rooms - this prevents the background task from sending
        # "Waiting for @initiator to join..." messages which shouldn't happen on restart
        rooms_waiting_for_requests.discard(chat_id)
        rooms_waiting_for_requests.discard(original_chat_id)
        
        # Clear step tracking sets/dicts to allow steps to be sent again
        step1_messages_sent.discard(original_chat_id)
        step4_amount_messages_sent.discard(original_chat_id)
        if original_chat_id in step2_blockchain_messages:
            del step2_blockchain_messages[original_chat_id]
        if original_chat_id in step3_coin_messages:
            del step3_coin_messages[original_chat_id]
        if original_chat_id in step4_messages:
            del step4_messages[original_chat_id]
        if original_chat_id in step5_messages:
            del step5_messages[original_chat_id]
        if original_chat_id in buyer_wallet_messages:
            del buyer_wallet_messages[original_chat_id]
        if original_chat_id in deal_summary_messages:
            del deal_summary_messages[original_chat_id]
        if original_chat_id in release_messages:
            del release_messages[original_chat_id]
        
        # Get initiator and counterparty usernames from deal_rooms.json (they never change)
        initiator_username = None
        counterparty_username = None
        if os.path.exists(DEAL_ROOMS_FILE):
            with open(DEAL_ROOMS_FILE, 'r') as f:
                deal_rooms = json.load(f)
            room_info = deal_rooms.get(str(original_chat_id))
            if room_info:
                initiator_username = room_info.get('initiator_username')
                counterparty_username = room_info.get('counterparty_username')
        
        logger.info(f"📋 From deal_rooms.json - initiator: @{initiator_username}, counterparty: @{counterparty_username}")
        
        # Clear transaction tracking
        if original_chat_id in room_awaiting_hash:
            del room_awaiting_hash[original_chat_id]
        if original_chat_id in room_transaction_state:
            del room_transaction_state[original_chat_id]
        if original_chat_id in seller_addresses:
            del seller_addresses[original_chat_id]
        if original_chat_id in release_approvals:
            del release_approvals[original_chat_id]
        if str(chat_id) in room_messages:
            del room_messages[str(chat_id)]
        if original_chat_id in deposit_address_messages:
            del deposit_address_messages[original_chat_id]
        if original_chat_id in seller_wallet_messages:
            del seller_wallet_messages[original_chat_id]
        
        # Clear user-specific data for this room (iterate through all users)
        users_to_clear = []
        for user_id in list(user_amounts.keys()):
            users_to_clear.append(user_id)
        
        for user_id in users_to_clear:
            if user_id in user_amounts:
                del user_amounts[user_id]
            if user_id in user_rates:
                del user_rates[user_id]
            if user_id in user_payment_methods:
                del user_payment_methods[user_id]
        
        # Clear coin selection for this room (stored by chat_id)
        if original_chat_id in user_coins:
            del user_coins[original_chat_id]
        
        # Clear blockchain selection for this room
        if original_chat_id in user_blockchain:
            del user_blockchain[original_chat_id]
        
        # Clear role tracking
        if original_chat_id in user_roles:
            del user_roles[original_chat_id]
        if original_chat_id in role_messages:
            del role_messages[original_chat_id]
        
        # Restore room_joined_users with initiator and counterparty so role selection can work
        if initiator_username and counterparty_username:
            room_joined_users[original_chat_id] = {initiator_username.lower(), counterparty_username.lower()}
            logger.info(f"✅ Restored room members for restart: @{initiator_username}, @{counterparty_username}")
            logger.info(f"🔍 room_joined_users[{original_chat_id}] = {room_joined_users[original_chat_id]}")
        else:
            logger.warning(f"❌ Could not restore room members: initiator={initiator_username}, counterparty={counterparty_username}")
        
        logger.info(f"✅ Cleared transaction state for room {room_name}")
        logger.info(f"🔍 room_initiators[{original_chat_id}] = {room_initiators.get(original_chat_id)}")
        
        # Reset deal in database
        database.reset_deal(original_chat_id)
        logger.info(f"📊 Reset deal in database for room {original_chat_id}")
        
        # Send disclaimer message to restart from the beginning
        send_chat_id = -1000000000000 - original_chat_id
        logger.info(f"📨 Sending disclaimer to send_chat_id: {send_chat_id}")
        
        await send_disclaimer_message(context.bot, send_chat_id, room_name, original_chat_id)
        logger.info(f"✅ Complete restart of {room_name} - sending disclaimer and role selection")
        
        # Send success message to the room
        await context.bot.send_message(
            chat_id=chat_id,
            text="✅ Room restarted! Please select your roles again."
        )
        
    except Exception as e:
        logger.error(f"❌ Error restarting room: {e}", exc_info=True)
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Error restarting room: {str(e)[:100]}"
            )
        except:
            pass


# Virtual wallet data storage (in-memory, will be replaced with DB later)
# Structure: {user_id: {'usdt_bsc': float, 'usdt_tron': float, 'usdc_bsc': float, 'bnb_bsc': float, 'trx_tron': float, 'transactions': [], 'addresses': {'bsc': str, 'tron': str}}}
virtual_wallets = {}

# Generated wallet addresses per user (stores addresses and encrypted private keys)
# Structure: {user_id: {'bsc': {'address': str, 'private_key': str}, 'tron': {'address': str, 'private_key': str}}}
user_wallet_addresses = {}

# Authorized user IDs for virtual wallet
WALLET_AUTHORIZED_USERS = [6864194951, 7338429782]

# Wallet token types with display names
WALLET_TOKENS = {
    'usdt_bsc': 'USDT (BSC)',
    'usdt_tron': 'USDT (TRON)',
    'usdc_bsc': 'USDC (BSC)',
    'bnb_bsc': 'BNB (BSC)',
    'trx_tron': 'TRX (TRON)'
}

# Map tokens to their network for deposit address lookup
TOKEN_NETWORK_MAP = {
    'usdt_bsc': 'bsc',
    'usdc_bsc': 'bsc',
    'bnb_bsc': 'bsc',
    'usdt_tron': 'tron',
    'trx_tron': 'tron'
}


def generate_bsc_wallet() -> dict:
    """Generate a new BSC/EVM wallet address"""
    if not ETH_ACCOUNT_AVAILABLE:
        logger.warning("eth_account not available, cannot generate BSC wallet")
        return {'address': 'BSC wallet generation unavailable', 'private_key': ''}
    
    try:
        # Generate a new account
        account = Account.create()
        return {
            'address': account.address,
            'private_key': account.key.hex()
        }
    except Exception as e:
        logger.error(f"Error generating BSC wallet: {e}")
        return {'address': 'Error generating wallet', 'private_key': ''}


def generate_tron_wallet() -> dict:
    """Generate a new TRON wallet address"""
    if not TRONPY_AVAILABLE:
        logger.warning("tronpy not available, cannot generate TRON wallet")
        return {'address': 'TRON wallet generation unavailable', 'private_key': ''}
    
    try:
        # Generate a new TRON private key and derive address
        priv_key = TronPrivateKey.random()
        return {
            'address': priv_key.public_key.to_base58check_address(),
            'private_key': priv_key.hex()
        }
    except Exception as e:
        logger.error(f"Error generating TRON wallet: {e}")
        return {'address': 'Error generating wallet', 'private_key': ''}


def get_user_deposit_address(user_id: int, network: str) -> str:
    """Get or generate a deposit address for a user on a specific network"""
    # First check in-memory cache
    if user_id in user_wallet_addresses:
        if network in user_wallet_addresses[user_id]:
            return user_wallet_addresses[user_id][network]['address']
    else:
        user_wallet_addresses[user_id] = {}
    
    # Check database for existing wallet
    db_wallet = database.get_user_wallet(user_id, network)
    if db_wallet and db_wallet.get('address'):
        # Cache it in memory
        user_wallet_addresses[user_id][network] = db_wallet
        logger.info(f"Loaded {network.upper()} wallet from database for user {user_id}: {db_wallet['address']}")
        return db_wallet['address']
    
    # Generate new address for this network
    if network == 'bsc':
        wallet = generate_bsc_wallet()
    elif network == 'tron':
        wallet = generate_tron_wallet()
    else:
        return 'Unknown network'
    
    # Store the wallet in memory
    user_wallet_addresses[user_id][network] = wallet
    
    # Save to database for persistence
    database.save_user_wallet(user_id, network, wallet['address'], wallet['private_key'])
    
    logger.info(f"Generated new {network.upper()} wallet for user {user_id}: {wallet['address']}")
    
    return wallet['address']


def load_wallets_from_database():
    """Load all user wallets from database on startup"""
    global user_wallet_addresses
    loaded_wallets = database.load_all_wallets()
    if loaded_wallets:
        user_wallet_addresses.update(loaded_wallets)
        logger.info(f"Loaded {len(loaded_wallets)} user wallets from database")


def init_wallet(user_id: int) -> dict:
    """Initialize a new wallet with all token balances"""
    if user_id not in virtual_wallets:
        virtual_wallets[user_id] = {
            'usdt_bsc': 0.0,
            'usdt_tron': 0.0,
            'usdc_bsc': 0.0,
            'bnb_bsc': 0.0,
            'trx_tron': 0.0,
            'transactions': []
        }
    # Ensure existing wallets have new tokens
    wallet = virtual_wallets[user_id]
    if 'bnb_bsc' not in wallet:
        wallet['bnb_bsc'] = 0.0
    if 'trx_tron' not in wallet:
        wallet['trx_tron'] = 0.0
    return wallet


async def wallet_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /wallet command - virtual wallet for authorized users only"""
    user = update.effective_user
    chat = update.effective_chat
    
    # Only allow in private chat (DM)
    if chat.type != 'private':
        await update.message.reply_text("❌ This command only works in private chat (DM)")
        return
    
    # Check if user is authorized
    if user.id not in WALLET_AUTHORIZED_USERS:
        await update.message.reply_text("❌ You are not authorized to use this feature")
        return
    
    logger.info(f"💰 /wallet command from authorized user {user.id} (@{user.username})")
    
    # Initialize wallet if not exists
    wallet = init_wallet(user.id)
    
    # Create wallet message with all token balances
    wallet_text = (
        f"💰 <b>Virtual Wallet</b>\n\n"
        f"<b>User:</b> @{user.username}\n"
        f"<b>User ID:</b> <code>{user.id}</code>\n\n"
        f"<b>Balances:</b>\n"
        f"├ USDT (BSC): <code>{wallet['usdt_bsc']:.2f}</code>\n"
        f"├ USDT (TRON): <code>{wallet['usdt_tron']:.2f}</code>\n"
        f"├ USDC (BSC): <code>{wallet['usdc_bsc']:.2f}</code>\n"
        f"├ BNB (BSC): <code>{wallet['bnb_bsc']:.4f}</code>\n"
        f"└ TRX (TRON): <code>{wallet['trx_tron']:.2f}</code>\n\n"
        f"Select an option below:"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("💵 Deposit", callback_data=f"wallet_deposit_{user.id}"),
            InlineKeyboardButton("💸 Withdraw", callback_data=f"wallet_withdraw_{user.id}")
        ],
        [
            InlineKeyboardButton("📜 Transactions", callback_data=f"wallet_transactions_{user.id}"),
            InlineKeyboardButton("🔄 Refresh", callback_data=f"wallet_refresh_{user.id}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        wallet_text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /balance command - available for everyone after deposit address is sent"""
    user = update.effective_user
    logger.info(f"💰 /balance command by user {user.id}")
    
    # Check if command is from a group
    if update.effective_chat.type not in ['group', 'supergroup']:
        logger.info(f"❌ Balance command not in group - chat type: {update.effective_chat.type}")
        await update.message.reply_text("❌ This command can only be used inside a group.")
        return
    
    chat_id = update.effective_chat.id
    
    # Normalize chat_id
    original_chat_id = normalize_chat_id(chat_id)
    
    # Check if deposit address has been sent (command only works after deposit address)
    if original_chat_id not in deposit_address_messages:
        logger.info(f"❌ Balance command before deposit address in room {original_chat_id}")
        # Silently ignore - don't respond before deposit address is sent
        return
    
    # Get the amount, token, and network for this room
    # Balance is 0 until deposit is confirmed
    amount = room_confirmed_deposits.get(original_chat_id, 0)
    
    # Get token (coin) for this room
    token = user_coins.get(original_chat_id, "N/A")
    
    # Get network (blockchain) for this room
    network = user_blockchain.get(original_chat_id, "N/A")
    
    # Calculate fees and release amount (same logic as deal summary)
    # Network fee: 3.0 for TRON, 0.2 for BSC
    if network == 'TRON':
        network_fee = 3.0
    else:
        network_fee = 0.2
    
    # Service fee: Use global fee if set via !setfees, otherwise per-room fee tier
    service_fee_percent = get_service_fee_percent(original_chat_id)
    
    # Calculate service fee amount and release amount
    service_fee_amount = amount * (service_fee_percent / 100)
    release_amount = amount - network_fee - service_fee_amount
    
    # Ensure release amount is not negative
    if release_amount < 0:
        release_amount = 0
    
    # Format amounts
    amount_formatted = f"{amount:.1f}"
    network_fee_formatted = f"{network_fee:.1f}"
    release_amount_formatted = f"{release_amount:.1f}"
    
    # Build the balance message with new format
    balance_text = f"""💰 <b>Balance Information</b>

<b>Gross Amount:</b> {amount_formatted} {token}
Net Release Amount: {release_amount_formatted} {token} (After Fees)
<b>Token:</b> {token}
<b>Network:</b> {network}
<b>Fees:</b> {format_fee_percent(service_fee_percent)} + {network_fee_formatted} {token}

This is the current available balance for this trade."""
    
    await update.message.reply_text(balance_text, parse_mode='HTML')
    logger.info(f"✅ Sent balance info to room {original_chat_id}: {amount_formatted} {token}, release: {release_amount_formatted} {token}")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /stats command - show trading stats for any user (available to everyone)"""
    user = update.effective_user
    logger.info(f"📊 /stats command by user {user.id} (@{user.username})")

    # Delete the /stats command message once received
    try:
        await update.message.delete()
        logger.info(f"🗑️ Deleted /stats command message from user {user.id}")
    except Exception as e:
        logger.warning(f"Could not delete /stats command message: {e}")

    username = user.username or user.full_name or str(user.id)
    display = f"@{user.username}" if user.username else username

    stats = database.get_user_stats(user.username or username)

    stats_text = (
        f"<blockquote>📊 {display} — Stats\n"
        f"🟢 BUYING STATS\n"
        f"• Total Bought: ${stats['total_bought']:,.2f}\n"
        f"• Total Buy Trades: {stats['buy_trades']}\n"
        f"\n"
        f"🔴 SELLING STATS\n"
        f"• Total Sold: ${stats['total_sold']:,.2f}\n"
        f"• Total Sell Trades: {stats['sell_trades']}</blockquote>\n"
        f"\n"
        f"📈 OVERALL PERFORMANCE\n"
        f"• Lifetime Volume: ${stats['lifetime_volume']:,.2f}\n"
        f"• Total Deals: {stats['total_deals']}\n"
        f"• Completion Rate: {stats['completion_rate']:.1f}% "
        f"({stats['completed_deals']} / {stats['total_deals']})\n"
        f"🏆 Overall Global Rank: #{stats['global_rank']} Trader"
    )

    await update.effective_chat.send_message(stats_text, parse_mode='HTML')


async def verify_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /verify command - verify escrow address for all users"""
    user = update.effective_user
    logger.info(f"🔍 /verify command by user {user.id}")
    
    # Delete the command message
    try:
        await update.message.delete()
        logger.info(f"🗑️ Deleted /verify command message from user {user.id}")
    except Exception as e:
        logger.warning(f"Could not delete /verify command message: {e}")
    
    if not context.args or len(context.args) == 0:
        usage_text = """❌ Usage: /verify <address>

Examples:
• /verify 0x4dd9c84aD4201d4aDF67eE20508BF622125C515c (EVM)
• /verify TQn9Y2khEsLMWT4K3LdL8oKbh1Z2HtZqjP (TRON)"""
        await update.effective_chat.send_message(usage_text)
        return
    
    address_to_verify = context.args[0].strip().lower()
    
    # Build escrow addresses from owner and CEO wallets
    escrow_addresses = {}
    
    # Add owner wallets
    owner_bsc = get_owner_wallet('BSC')
    owner_tron = get_owner_wallet('TRON')
    if owner_bsc:
        escrow_addresses[owner_bsc.lower()] = {"token": "USDT/USDC", "chain": "BSC", "type": "Owner"}
    if owner_tron and owner_tron != "T0000000000000000000000000000000000":
        escrow_addresses[owner_tron.lower()] = {"token": "USDT", "chain": "TRON", "type": "Owner"}
    
    # Add CEO wallets
    ceo_bsc = get_ceo_wallet('BSC')
    ceo_tron = get_ceo_wallet('TRON')
    if ceo_bsc:
        escrow_addresses[ceo_bsc.lower()] = {"token": "USDT/USDC", "chain": "BSC", "type": "CEO"}
    if ceo_tron and ceo_tron != "T0000000000000000000000000000000000":
        escrow_addresses[ceo_tron.lower()] = {"token": "USDT", "chain": "TRON", "type": "CEO"}
    
    if address_to_verify in escrow_addresses:
        info = escrow_addresses[address_to_verify]
        verified_text = f"""✅ Address <b>verified</b>

Token: {info['token']}
Chain: {info['chain']}"""
        await update.effective_chat.send_message(verified_text, parse_mode='HTML')
        logger.info(f"✅ Address verified for user {user.id}: {address_to_verify} ({info['token']} on {info['chain']})")
    else:
        warning_text = """⚠️ <b>WARNING:</b> Address Not Verified

❌ This address does <b>NOT</b> belong to this bot.

<b>🚫 DO NOT send funds to this address!</b>"""
        await update.effective_chat.send_message(warning_text, parse_mode='HTML')
        logger.warning(f"⚠️ Address NOT verified for user {user.id}: {address_to_verify}")


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle button presses"""
    query = update.callback_query
    # Note: Don't call query.answer() here - each branch handles its own answer
    # to avoid "Query is too old" errors from duplicate answers
    
    user_id = query.from_user.id
    username = query.from_user.username or query.from_user.first_name
    
    # Handle release approval - seller only
    if query.data.startswith('approve_release_'):
        try:
            original_chat_id = int(query.data.split('_')[2])
            send_chat_id = -1000000000000 - original_chat_id
            
            # Get seller info - only seller can approve release
            buyer_username = room_initiators.get(original_chat_id, {}).get('buyer', '')
            seller_username = room_initiators.get(original_chat_id, {}).get('seller', '')
            
            # Try to get seller_user_id from database for more reliable check
            deal_data = database.get_deal(original_chat_id)
            seller_user_id = deal_data.get('seller_user_id') if deal_data else None
            
            # Check if user is seller (prefer user_id check, fallback to username)
            is_seller = False
            if seller_user_id and user_id == seller_user_id:
                is_seller = True
            elif seller_username and (username.lower() == seller_username.lower()):
                is_seller = True
            
            if not is_seller:
                await query.answer("❌ Only the seller can approve release", show_alert=True)
                return CHOOSING
            
            # Update approval status
            if original_chat_id not in release_approvals:
                release_approvals[original_chat_id] = {'seller': 'waiting'}
            
            release_approvals[original_chat_id]['seller'] = 'approved'
            
            # Save release approval to database
            database.approve_release(original_chat_id, 'seller')
            
            # Step 1: Edit message to show seller confirmed
            confirmed_text = f"""<b>Release Confirmation</b>

✅ @{seller_username} - Confirmed

✅ All approvals received. Processing release..."""
            
            if original_chat_id in release_messages:
                msg_id = release_messages[original_chat_id]
                try:
                    await context.bot.edit_message_caption(
                        chat_id=send_chat_id,
                        message_id=msg_id,
                        caption=confirmed_text,
                        parse_mode='HTML',
                        reply_markup=None
                    )
                    logger.info(f"✅ Updated release confirmation to 'Processing' in room {original_chat_id}")
                except Exception as e:
                    logger.warning(f"Could not edit release confirmation: {e}")
            
            # Step 2: Edit message to show just group chat id
            final_text = str(send_chat_id)
            
            if original_chat_id in release_messages:
                msg_id = release_messages[original_chat_id]
                try:
                    await context.bot.edit_message_caption(
                        chat_id=send_chat_id,
                        message_id=msg_id,
                        caption=final_text,
                        parse_mode='HTML',
                        reply_markup=None
                    )
                    logger.info(f"✅ Edited release confirmation message with group id in room {original_chat_id}")
                except Exception as e:
                    logger.warning(f"Could not edit release confirmation to group id: {e}")
            
            # Step 3: Calculate fees and send Partial Release Complete message
            buyer_addr = buyer_addresses.get(original_chat_id, "0xUnknown")
            
            # Get deal data for calculations
            amount = float(deal_data.get('amount', 0)) if deal_data else 0
            coin = deal_data.get('coin', 'USDT') if deal_data else 'USDT'
            chain = deal_data.get('network', 'BSC') if deal_data else user_blockchain.get(original_chat_id, 'BSC')
            
            # Fallback to in-memory if no DB data
            if amount == 0:
                for uid, amt in user_amounts.items():
                    amount = float(amt)
                    break
            if not coin or coin == 'USDT':
                coin = user_coins.get(original_chat_id, 'USDT')
            
            # Calculate network fee based on chain
            if chain == 'TRON':
                network_fee = 3.0
            else:  # BSC
                network_fee = 0.2
            
            # Use global service fee set via !setfees
            service_fee_percent = current_fee_percent
            
            service_fee_amount = amount * (service_fee_percent / 100)
            
            # Calculate amount released
            amount_released = amount - network_fee - service_fee_amount
            
            # Build transaction link based on chain
            if chain == 'TRON':
                tx_url = f"https://tronscan.org/#/address/{buyer_addr}"
            else:  # BSC
                tx_url = f"https://bscscan.com/address/{buyer_addr}"
            
            # Format the Partial Release Complete message
            partial_release_text = f"""✅ <b>Partial Release Complete!</b>

Amount Released: {amount_released:.4f} {coin}
Remaining: {network_fee:.4f} {coin}
🔗 Transaction: <a href="{tx_url}">Click Here</a>"""
            
            # Send the Partial Release Complete message (text only, no image or button)
            try:
                await context.bot.send_message(
                    chat_id=send_chat_id,
                    text=partial_release_text,
                    parse_mode='HTML'
                )
                logger.info(f"✅ Sent Partial Release Complete message to room {original_chat_id}")
            except Exception as e:
                logger.warning(f"Could not send Partial Release Complete message: {e}")
            
            # Mark deal as completed in database
            database.complete_deal(original_chat_id)
            
            # Update room log message with deal completed status
            await update_room_log_status(context.bot, original_chat_id, "Deal Completed!")
            
            # Send notification to logs channel
            try:
                amount_str = f"{amount} {coin}" if amount else "N/A"
                
                # Generate invite link for the group
                try:
                    invite_link = await context.bot.create_chat_invite_link(
                        chat_id=send_chat_id,
                        name="Deal Completed Link"
                    )
                    group_link = invite_link.invite_link
                except Exception as e:
                    logger.warning(f"Could not generate invite link: {e}")
                    group_link = "Unable to generate link"
                
                completion_notification = (
                    f"Deal Completed ✅\n\n"
                    f"Buyer - @{buyer_username}\n"
                    f"Seller - @{seller_username}\n"
                    f"Amount - {amount_str}\n"
                    f"Group Link - {group_link}"
                )
                
                await context.bot.send_message(
                    chat_id=-1003266978268,
                    text=completion_notification
                )
                logger.info(f"✅ Sent deal completion notification to logs channel for room {original_chat_id}")
            except Exception as e:
                logger.warning(f"Could not send completion notification to logs channel: {e}")
            
            await query.answer(f"✅ Release approved!")
            return CHOOSING
        except Exception as e:
            logger.warning(f"❌ Error handling release approval: {e}")
            await query.answer("❌ Error", show_alert=True)
            return CHOOSING
    
    # Handle close deal
    elif query.data.startswith('close_deal_'):
        try:
            chat_id = int(query.data.split('_')[2])
            
            # Get buyer and seller usernames
            buyer_username = room_initiators.get(chat_id, {}).get('buyer')
            seller_username = room_initiators.get(chat_id, {}).get('seller')
            
            username_lower = username.lower()
            
            # Only let buyer and seller close the deal
            if buyer_username and username_lower == buyer_username.lower():
                pass
            elif seller_username and username_lower == seller_username.lower():
                pass
            else:
                await query.answer("❌ Only deal participants can close", show_alert=True)
                return CHOOSING
            
            # Kick both users from the group using username
            try:
                send_chat_id = -1000000000000 - chat_id
                
                # Kick buyer by username
                if buyer_username:
                    try:
                        buyer_id = get_user_id(buyer_username)
                        if buyer_id:
                            await context.bot.ban_chat_member(send_chat_id, buyer_id)
                            logger.info(f"✅ Kicked buyer {buyer_username} from room {chat_id}")
                        else:
                            logger.warning(f"⚠️ No user ID found for buyer {buyer_username}")
                    except Exception as e:
                        logger.warning(f"Could not kick buyer: {e}")
                
                # Kick seller by username
                if seller_username:
                    try:
                        seller_id = get_user_id(seller_username)
                        if seller_id:
                            await context.bot.ban_chat_member(send_chat_id, seller_id)
                            logger.info(f"✅ Kicked seller {seller_username} from room {chat_id}")
                        else:
                            logger.warning(f"⚠️ No user ID found for seller {seller_username}")
                    except Exception as e:
                        logger.warning(f"Could not kick seller: {e}")
                
                await query.answer("✅ Deal closed! Users removed from group.")
                return CHOOSING
            except Exception as e:
                logger.warning(f"Error kicking users: {e}")
                await query.answer("❌ Error closing deal", show_alert=True)
                return CHOOSING
        except Exception as e:
            logger.warning(f"❌ Error handling close deal: {e}")
            await query.answer("❌ Error", show_alert=True)
            return CHOOSING
    
    # Handle release decline - seller only
    elif query.data.startswith('decline_release_'):
        try:
            original_chat_id = int(query.data.split('_')[2])
            send_chat_id = -1000000000000 - original_chat_id
            
            # Get seller info - only seller can decline release
            seller_username = room_initiators.get(original_chat_id, {}).get('seller', '')
            
            # Try to get seller_user_id from database for more reliable check
            deal_data = database.get_deal(original_chat_id)
            seller_user_id = deal_data.get('seller_user_id') if deal_data else None
            
            # Check if user is seller (prefer user_id check, fallback to username)
            is_seller = False
            if seller_user_id and user_id == seller_user_id:
                is_seller = True
            elif seller_username and (username.lower() == seller_username.lower()):
                is_seller = True
            
            if not is_seller:
                await query.answer("❌ Only the seller can decline release", show_alert=True)
                return CHOOSING
            
            # Update rejection status
            if original_chat_id not in release_approvals:
                release_approvals[original_chat_id] = {'seller': 'waiting'}
            
            release_approvals[original_chat_id]['seller'] = 'rejected'
            
            # Update message to show seller declined
            declined_text = f"""<b>Release Confirmation (Full)</b>

❌ @{seller_username} - Declined

Release has been declined by the seller."""
            
            # Keep buttons so seller can change their mind
            keyboard = [
                [InlineKeyboardButton("✅ Approve", callback_data=f"approve_release_{original_chat_id}"),
                 InlineKeyboardButton("❌ Decline", callback_data=f"decline_release_{original_chat_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Update message
            if original_chat_id in release_messages:
                msg_id = release_messages[original_chat_id]
                try:
                    await context.bot.edit_message_caption(
                        chat_id=send_chat_id,
                        message_id=msg_id,
                        caption=declined_text,
                        parse_mode='HTML',
                        reply_markup=reply_markup
                    )
                    logger.info(f"✅ Updated release confirmation to 'Declined' in room {original_chat_id}")
                except Exception as e:
                    logger.warning(f"Could not update release message: {e}")
            
            await query.answer(f"❌ Declined!")
            return CHOOSING
        except Exception as e:
            logger.warning(f"❌ Error handling release decline: {e}")
            await query.answer("❌ Error", show_alert=True)
            return CHOOSING
    
    if query.data == 'create_listing':
        await query.edit_message_text(
            text="📝 Creating a new listing\n\n"
                 "Send me a title for your item:"
        )
        context.user_data['step'] = 'title'
        return CREATING_LISTING
    
    elif query.data == 'browse_listings':
        if not listings:
            keyboard = [[InlineKeyboardButton("← Back", callback_data='back')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                text="No listings available yet. Be the first to create one!",
                reply_markup=reply_markup
            )
        else:
            text = "🛍️ Available Listings:\n\n"
            keyboard = []
            for listing_id, listing in listings.items():
                text += f"📌 {listing['title']}\n"
                text += f"   Price: ${listing['price']}\n"
                text += f"   Seller: User {listing['seller_id']}\n\n"
                keyboard.append([
                    InlineKeyboardButton(
                        f"Buy '{listing['title']}'",
                        callback_data=f'buy_{listing_id}'
                    )
                ])
            keyboard.append([InlineKeyboardButton("← Back", callback_data='back')])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text=text, reply_markup=reply_markup)
        return BROWSING
    
    elif query.data == 'my_transactions':
        user_transactions = [t for t in transactions.values() 
                            if t['seller_id'] == user_id or t['buyer_id'] == user_id]
        if not user_transactions:
            keyboard = [[InlineKeyboardButton("← Back", callback_data='back')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                text="You have no transactions yet.",
                reply_markup=reply_markup
            )
        else:
            text = "💼 Your Transactions:\n\n"
            for t in user_transactions:
                text += f"Item: {t['item_title']}\n"
                text += f"Amount: ${t['amount']}\n"
                text += f"Status: {t['status']}\n\n"
            keyboard = [[InlineKeyboardButton("← Back", callback_data='back')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text=text, reply_markup=reply_markup)
        return TRANSACTION
    
    elif query.data == 'help':
        await query.edit_message_text(
            text="❓ Help\n\n"
                 "P2PMART is a secure peer-to-peer marketplace with escrow protection.\n\n"
                 "How it works:\n"
                 "1. Sellers create listings\n"
                 "2. Buyers browse and purchase\n"
                 "3. Payment held in escrow\n"
                 "4. After delivery, payment released\n\n"
                 "Use the buttons below to get started.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Back", callback_data='back')]])
        )
        return CHOOSING
    
    elif query.data == 'back':
        keyboard = [
            [InlineKeyboardButton("📝 Create Listing", callback_data='create_listing')],
            [InlineKeyboardButton("🛍️ Browse Listings", callback_data='browse_listings')],
            [InlineKeyboardButton("💼 My Transactions", callback_data='my_transactions')],
            [InlineKeyboardButton("❓ Help", callback_data='help')],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            text="What would you like to do?",
            reply_markup=reply_markup
        )
        return CHOOSING
    
    elif query.data.startswith('buy_'):
        listing_id = query.data.split('_')[1]
        if listing_id in listings:
            listing = listings[listing_id]
            context.user_data['purchase_listing_id'] = listing_id
            keyboard = [
                [InlineKeyboardButton("✅ Confirm Purchase", callback_data='confirm_purchase')],
                [InlineKeyboardButton("← Cancel", callback_data='browse_listings')],
            ]
            await query.edit_message_text(
                text=f"Confirm Purchase\n\n"
                     f"Item: {listing['title']}\n"
                     f"Price: ${listing['price']}\n\n"
                     f"Proceed with purchase?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        return TRANSACTION
    
    elif query.data == 'confirm_purchase':
        listing_id = context.user_data.get('purchase_listing_id')
        if listing_id and listing_id in listings:
            listing = listings[listing_id]
            transaction_id = f"txn_{len(transactions) + 1}"
            transactions[transaction_id] = {
                'seller_id': listing['seller_id'],
                'buyer_id': user_id,
                'item_title': listing['title'],
                'amount': listing['price'],
                'status': 'In Escrow',
                'listing_id': listing_id
            }
            keyboard = [[InlineKeyboardButton("← Back to Main", callback_data='back')]]
            await query.edit_message_text(
                text=f"✅ Purchase Confirmed!\n\n"
                     f"Transaction ID: {transaction_id}\n"
                     f"Amount: ${listing['price']}\n"
                     f"Status: In Escrow\n\n"
                     f"Payment has been secured in escrow.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        return CHOOSING
    
    # Handle blockchain selection (BSC button)
    elif query.data.startswith('blockchain_bsc_'):
        try:
            parts = query.data.split('_')
            chat_id = int(parts[2])
            send_chat_id = get_send_chat_id(chat_id)
            
            # Check if blockchain is already set (idempotency guard)
            if chat_id in user_blockchain and user_blockchain[chat_id] == 'BSC':
                # Already selected, just acknowledge
                await query.answer("✅ BSC already selected")
                return CHOOSING
            
            logger.info(f"✅ User selected blockchain: BSC in room {chat_id}")
            user_blockchain[chat_id] = 'BSC'
            
            # Save blockchain to database
            database.set_network(chat_id, 'BSC')
            
            # Update the button to show selection (no selection indicator)
            try:
                await query.edit_message_caption(
                    caption="<b>Step 2 - Choose Blockchain</b>",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("BSC", callback_data=f"blockchain_bsc_{chat_id}_done"),
                        InlineKeyboardButton("TRON", callback_data=f"blockchain_tron_{chat_id}")
                    ]]),
                    parse_mode='HTML'
                )
                logger.info(f"✅ Updated blockchain button for room {chat_id}")
            except Exception as e:
                logger.warning(f"⚠️ Could not edit blockchain message for room {chat_id}: {e}")
            
            # Send Step 3 (coin selection) message only if not already sent
            if chat_id not in step3_coin_messages:
                logger.info(f"📨 Sending Step 3 (coin selection) message to room {chat_id}")
                await send_step3_coin_message(context.bot, send_chat_id, chat_id)
                logger.info(f"✅ Step 3 sent for room {chat_id}")
            else:
                logger.info(f"⏩ Step 3 already sent for room {chat_id}, skipping")
            
            await query.answer("✅ Blockchain: BSC selected")
            return CHOOSING
            
        except Exception as e:
            logger.error(f"❌ Error handling blockchain selection: {e}", exc_info=True)
            await query.answer(f"❌ Error: {str(e)[:50]}", show_alert=True)
            return CHOOSING
    
    # Handle TRON blockchain selection
    elif query.data.startswith('blockchain_tron_'):
        try:
            parts = query.data.split('_')
            chat_id = int(parts[2])
            send_chat_id = get_send_chat_id(chat_id)
            
            # Check if blockchain is already set (idempotency guard)
            if chat_id in user_blockchain and user_blockchain[chat_id] == 'TRON':
                # Already selected, just acknowledge
                await query.answer("TRON already selected")
                return CHOOSING
            
            logger.info(f"✅ User selected blockchain: TRON in room {chat_id}")
            user_blockchain[chat_id] = 'TRON'
            
            # Save blockchain to database
            database.set_network(chat_id, 'TRON')
            
            # Update the button to show selection (no selection indicator)
            try:
                await query.edit_message_caption(
                    caption="<b>Step 2 - Choose Blockchain</b>",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("BSC", callback_data=f"blockchain_bsc_{chat_id}"),
                        InlineKeyboardButton("TRON", callback_data=f"blockchain_tron_{chat_id}_done")
                    ]]),
                    parse_mode='HTML'
                )
                logger.info(f"✅ Updated blockchain button for room {chat_id}")
            except Exception as e:
                logger.warning(f"⚠️ Could not edit blockchain message for room {chat_id}: {e}")
            
            # Send Step 3 (coin selection) message only if not already sent
            if chat_id not in step3_coin_messages:
                logger.info(f"📨 Sending Step 3 (coin selection) message to room {chat_id}")
                await send_step3_coin_message(context.bot, send_chat_id, chat_id)
                logger.info(f"✅ Step 3 sent for room {chat_id}")
            else:
                logger.info(f"⏩ Step 3 already sent for room {chat_id}, skipping")
            
            await query.answer("Blockchain: TRON selected")
            return CHOOSING
            
        except Exception as e:
            logger.error(f"❌ Error handling TRON blockchain selection: {e}", exc_info=True)
            await query.answer(f"❌ Error: {str(e)[:50]}", show_alert=True)
            return CHOOSING
    
    # Handle coin selection (USDT/USDC buttons)
    elif query.data.startswith('coin_usdt_') or query.data.startswith('coin_usdc_'):
        try:
            parts = query.data.split('_')
            coin_type = 'USDT' if query.data.startswith('coin_usdt_') else 'USDC'
            chat_id = int(parts[2])
            user_id = query.from_user.id
            send_chat_id = get_send_chat_id(chat_id)
            
            # Save user ID for bio detection later
            username = query.from_user.username
            if username:
                save_user_id(username, user_id)
            
            # Get selected blockchain
            selected_blockchain = user_blockchain.get(chat_id, 'BSC')
            
            # Reject USDC selection if TRON is selected (TRON only supports USDT)
            if coin_type == 'USDC' and selected_blockchain == 'TRON':
                await query.answer("❌ TRON only supports USDT", show_alert=True)
                return CHOOSING
            
            # Idempotency guard - check if coin already selected
            if chat_id in user_coins and user_coins[chat_id] == coin_type:
                await query.answer(f"✅ {coin_type} already selected")
                return CHOOSING
            
            logger.info(f"✅ User {query.from_user.username} selected coin: {coin_type} in room {chat_id}")
            user_coins[chat_id] = coin_type
            
            # Save coin to database
            database.set_coin(chat_id, coin_type)
            
            # Update buttons based on blockchain (TRON only shows USDT)
            if selected_blockchain == 'TRON':
                new_keyboard = [[InlineKeyboardButton("USDT", callback_data=f"coin_usdt_{chat_id}_done")]]
            else:
                new_keyboard = [[
                    InlineKeyboardButton("USDT", callback_data=f"coin_usdt_{chat_id}_done"),
                    InlineKeyboardButton("USDC", callback_data=f"coin_usdc_{chat_id}_done")
                ]]
            reply_markup = InlineKeyboardMarkup(new_keyboard)
            
            try:
                await query.edit_message_caption(
                    caption="<b>Step 3 - Select Coin</b>",
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            except Exception as e:
                logger.warning(f"Could not edit coin message: {e}")
            
            # Get buyer and seller usernames from user_roles
            buyer_username = None
            seller_username = None
            if chat_id in user_roles:
                for username, role in user_roles[chat_id].items():
                    if role == 'BUYER':
                        buyer_username = username
                    elif role == 'SELLER':
                        seller_username = username
            
            # Store initiators
            if chat_id not in room_initiators:
                room_initiators[chat_id] = {}
            room_initiators[chat_id]['buyer'] = buyer_username
            room_initiators[chat_id]['seller'] = seller_username
            
            # Send notification to channel -1003266978268 when buyer, seller, coin, network are known
            try:
                blockchain = user_blockchain.get(chat_id, 'BSC')
                notification_text = (
                    f"🎉 <b>New Deal Started</b>\n\n"
                    f"<b>Buyer:</b> @{buyer_username}\n"
                    f"<b>Seller:</b> @{seller_username}\n"
                    f"<b>Coin:</b> {coin_type}\n"
                    f"<b>Network:</b> {blockchain}\n"
                    f"<b>Room ID:</b> <code>{chat_id}</code>"
                )
                await context.bot.send_message(
                    chat_id=-1003266978268,
                    text=notification_text,
                    parse_mode='HTML'
                )
                logger.info(f"✅ Sent deal notification to channel -1003266978268 for room {chat_id}")
            except Exception as e:
                logger.warning(f"Could not send notification to channel: {e}")
            
            # Send Step 4 (amount entry) message
            if chat_id not in step4_amount_messages_sent:
                logger.info(f"📨 Sending Step 4 (amount entry) message to room {chat_id}")
                await send_step4_amount_message(context.bot, send_chat_id, chat_id)
                logger.info(f"✅ Step 4 sent for room {chat_id}")
            else:
                logger.info(f"⏩ Step 4 already sent for room {chat_id}, skipping")
            
            await query.answer(f"✅ Coin selected: {coin_type}")
            return CHOOSING
            
        except Exception as e:
            logger.warning(f"❌ Error handling coin selection: {e}")
            await query.answer("❌ Error", show_alert=True)
            return CHOOSING
    
    # Handle deal approval callbacks
    elif query.data.startswith('approve_deal_'):
        try:
            parts = query.data.split('_')
            chat_id = int(parts[2])
            username = query.from_user.username or query.from_user.first_name
            username_lower = username.lower()
            
            # Get buyer and seller usernames
            buyer_username = room_initiators[chat_id].get('buyer') if chat_id in room_initiators else None
            seller_username = room_initiators[chat_id].get('seller') if chat_id in room_initiators else None
            
            # Determine if user is buyer or seller
            user_role = None
            if buyer_username and username_lower == buyer_username.lower():
                user_role = 'buyer'
            elif seller_username and username_lower == seller_username.lower():
                user_role = 'seller'
            
            if user_role is None:
                await query.answer("❌ You are not authorized to approve this deal", show_alert=True)
                return CHOOSING
            
            # Check if already approved
            if chat_id in approvals and approvals[chat_id].get(user_role):
                await query.answer("✅ You have already approved this deal", show_alert=True)
                return CHOOSING
            
            # Mark approval
            if chat_id not in approvals:
                approvals[chat_id] = {'buyer': False, 'seller': False}
            
            approvals[chat_id][user_role] = True
            logger.info(f"✅ {user_role.upper()} {username} approved deal in room {chat_id}")
            
            # Save approval to database
            database.approve_summary(chat_id, user_role)
            
            # Get deal data from database (source of truth to prevent mixing)
            deal_data = database.get_deal(chat_id)
            
            if deal_data:
                amount = deal_data.get('amount')
                rate = deal_data.get('rate')
                payment_method = deal_data.get('payment_method')
                coin = deal_data.get('coin') or 'USDT'
                chain = deal_data.get('network') or 'BSC'
                buyer_address = deal_data.get('buyer_address') or buyer_addresses.get(chat_id, "N/A")
                seller_address = deal_data.get('seller_address') or seller_addresses.get(chat_id, "N/A")
                logger.info(f"📊 Using database values for approval in room {chat_id}")
            else:
                # Fallback to in-memory values if database not available
                amount = None
                rate = None
                payment_method = None
                coin = user_coins.get(chat_id, 'USDT')
                chain = user_blockchain.get(chat_id, 'BSC')
                buyer_address = buyer_addresses.get(chat_id, "N/A")
                seller_address = seller_addresses.get(chat_id, "N/A")
                logger.warning(f"⚠️ No database record for room {chat_id} in approval handler")
            
            rate_formatted = f"₹{float(rate):.1f}" if rate else "N/A"
            
            # Use the shared helper function to build deal text with current approval status
            deal_text = build_deal_summary_text(
                chat_id, 
                buyer_approved=approvals[chat_id]['buyer'], 
                seller_approved=approvals[chat_id]['seller']
            )
            
            # Check if both approved
            both_approved = approvals[chat_id]['buyer'] and approvals[chat_id]['seller']
            
            if both_approved:
                # Remove button and send deal confirmed message
                reply_markup = None
                
                # Calculate fees and release amount (same as deal summary)
                amount_float = float(amount) if amount else 0
                
                # Get network fee based on chain
                if chain == 'TRON':
                    network_fee = 3.0
                else:  # BSC
                    network_fee = 0.2
                
                # Get service fee: Use global fee if set via !setfees, otherwise per-room fee tier
                service_fee_percent = get_service_fee_percent(chat_id)
                
                # Calculate service fee amount and release amount
                service_fee_amount = amount_float * (service_fee_percent / 100)
                release_amount_value = amount_float - network_fee - service_fee_amount
                
                # Format values - use .1f for clean display (203.0 instead of 203.00000000)
                deal_amount = f"{float(amount):.1f} {coin}" if amount else f"0.0 {coin}"
                fees = format_fee_percent(service_fee_percent)  # Service fee as percentage (same as deal summary)
                release_amount = f"{release_amount_value:.1f} {coin}"
                
                # Format deal confirmed text with monospace for addresses
                confirmed_text = f"""✅ <b>DEAL CONFIRMED</b>

<b>Buyer:</b> @{buyer_username}
<b>Seller:</b> @{seller_username}

<b>Deal Amount:</b> {deal_amount}
<b>Fees:</b> {fees}
<b>Release Amount:</b> {release_amount}
<b>Rate:</b> {rate_formatted}
<b>Payment:</b> {payment_method}
<b>Chain:</b> {chain}

<b>Buyer Address:</b> <code>{buyer_address}</code>
<b>Seller Address:</b> <code>{seller_address}</code>

🛑 <b>Do not send funds here</b> 🛑"""
                
                # Send deal confirmed message with image
                send_chat_id = -1000000000000 - chat_id
                confirmed_image_path = os.path.join(SCRIPT_DIR, "deal_confirmed_image.jpg")
                
                try:
                    if os.path.exists(confirmed_image_path):
                        confirmed_msg = await context.bot.send_photo(
                            chat_id=send_chat_id,
                            photo=open(confirmed_image_path, 'rb'),
                            caption=confirmed_text,
                            parse_mode='HTML'
                        )
                        logger.info(f"✅ Sent deal confirmed message to room {chat_id}")
                    else:
                        confirmed_msg = await context.bot.send_message(
                            chat_id=send_chat_id,
                            text=confirmed_text,
                            parse_mode='HTML'
                        )
                        logger.warning(f"⚠️ Sent deal confirmed (text only) to room {chat_id} - image not found")
                    
                    # Pin the deal confirmed message
                    try:
                        await context.bot.pin_chat_message(
                            chat_id=send_chat_id,
                            message_id=confirmed_msg.message_id
                        )
                        logger.info(f"📌 Pinned deal confirmed message in room {chat_id}")
                    except Exception as e:
                        logger.warning(f"Could not pin deal confirmed message: {e}")
                    
                    # Send deposit address message
                    blockchain = user_blockchain.get(chat_id, "BSC")
                    coin_type = coin if coin else "USDT"
                    
                    # Use rotating wallet (owner/CEO) for deposits
                    deposit_address = get_rotating_deposit_wallet(blockchain)
                    logger.info(f"🏦 Using rotating wallet for {blockchain}: {deposit_address}")
                    
                    # Confirm deal in database with escrow address
                    database.confirm_deal(chat_id, escrow_address=deposit_address)
                    
                    deposit_text = f"""💳 {coin_type} {blockchain} Deposit

🏦 {coin_type} {blockchain} Address: <code>{deposit_address}</code>

⚠️ <b>Please Note:</b>
• Double-check the address before sending.
• We are not responsible for any fake, incorrect, or unsupported tokens sent to this address.

Once you've sent the amount, tap the button below."""
                    
                    # Create button - only seller can tap
                    keyboard = [[InlineKeyboardButton("✅ Payment Sent", callback_data=f"payment_sent_{chat_id}")]]
                    deposit_reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    deposit_image_path = os.path.join(SCRIPT_DIR, "deposit_address_image.jpg")
                    
                    try:
                        if os.path.exists(deposit_image_path):
                            msg = await context.bot.send_photo(
                                chat_id=send_chat_id,
                                photo=open(deposit_image_path, 'rb'),
                                caption=deposit_text,
                                parse_mode='HTML',
                                reply_markup=deposit_reply_markup
                            )
                            deposit_address_messages[chat_id] = msg.message_id
                            logger.info(f"✅ Sent deposit address message to room {chat_id}")
                        else:
                            msg = await context.bot.send_message(
                                chat_id=send_chat_id,
                                text=deposit_text,
                                parse_mode='HTML',
                                reply_markup=deposit_reply_markup
                            )
                            deposit_address_messages[chat_id] = msg.message_id
                            logger.warning(f"⚠️ Sent deposit address (text only) to room {chat_id} - image not found")
                        
                        # Update room log message with deposit address status
                        # Format: Deposit [{first 3 chars}....{last 4 chars}]
                        addr_display = f"{deposit_address[:5]}....{deposit_address[-4:]}"
                        await update_room_log_status(context.bot, chat_id, f"Deposit [{addr_display}]")
                    except Exception as e:
                        logger.warning(f"Could not send deposit address message: {e}")
                
                except Exception as e:
                    logger.warning(f"Could not send deal confirmed message: {e}")
            else:
                # Keep button
                keyboard = [[InlineKeyboardButton("Approve", callback_data=f"approve_deal_{chat_id}")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Update the deal summary message
            if chat_id in deal_summary_messages:
                send_chat_id = -1000000000000 - chat_id
                msg_id = deal_summary_messages[chat_id]
                
                try:
                    await context.bot.edit_message_caption(
                        chat_id=send_chat_id,
                        message_id=msg_id,
                        caption=deal_text,
                        parse_mode='HTML',
                        reply_markup=reply_markup
                    )
                    logger.info(f"✅ Updated deal summary message in room {chat_id}")
                except Exception as e:
                    logger.warning(f"Could not edit deal summary: {e}")
            
            await query.answer(f"✅ {user_role.upper()} approved!")
            return CHOOSING
            
        except Exception as e:
            logger.warning(f"❌ Error handling approval: {e}")
            await query.answer("❌ Error", show_alert=True)
            return CHOOSING
    
    # Handle role selection callbacks
    elif query.data.startswith('role_buyer_') or query.data.startswith('role_seller_'):
        try:
            # Parse callback data
            parts = query.data.split('_')
            role_type = 'BUYER' if query.data.startswith('role_buyer_') else 'SELLER'
            original_chat_id = int(parts[2])
            username = query.from_user.username or query.from_user.first_name
            username_lower = username.lower()
            
            # Check if we have role message info
            if original_chat_id not in role_messages:
                await query.answer("❌ Role selection expired", show_alert=True)
                return CHOOSING
            
            role_msg_data = role_messages[original_chat_id]
            # Handle both old format (4 elements) and new format (5 elements with counterparty_user_id)
            if len(role_msg_data) == 5:
                msg_id, send_chat_id, initiator_username, counterparty_username, counterparty_user_id = role_msg_data
            else:
                msg_id, send_chat_id, initiator_username, counterparty_username = role_msg_data
                counterparty_user_id = None
            initiator_lower = initiator_username.lower() if initiator_username else ''
            counterparty_lower = counterparty_username.lower() if counterparty_username else ''
            
            # Initialize roles if needed
            if original_chat_id not in user_roles:
                user_roles[original_chat_id] = {}
            
            # Get current roles
            initiator_role = user_roles[original_chat_id].get(initiator_lower)
            counterparty_role = user_roles[original_chat_id].get(counterparty_lower)
            
            # Check if the role is already taken by the other user
            if role_type == 'BUYER' and counterparty_role == 'BUYER' and username_lower == initiator_lower:
                await query.answer("❌ Buyer role already taken by the other user", show_alert=True)
                return CHOOSING
            elif role_type == 'SELLER' and counterparty_role == 'SELLER' and username_lower == initiator_lower:
                await query.answer("❌ Seller role already taken by the other user", show_alert=True)
                return CHOOSING
            elif role_type == 'BUYER' and initiator_role == 'BUYER' and username_lower == counterparty_lower:
                await query.answer("❌ Buyer role already taken by the other user", show_alert=True)
                return CHOOSING
            elif role_type == 'SELLER' and initiator_role == 'SELLER' and username_lower == counterparty_lower:
                await query.answer("❌ Seller role already taken by the other user", show_alert=True)
                return CHOOSING
            
            # Store the role
            user_roles[original_chat_id][username_lower] = role_type
            logger.info(f"👤 {username} selected role: {role_type} in room {original_chat_id}")
            
            # Get updated roles
            initiator_role = user_roles[original_chat_id].get(initiator_lower)
            counterparty_role = user_roles[original_chat_id].get(counterparty_lower)
            
            # Check if both have selected roles
            both_selected = initiator_role is not None and counterparty_role is not None
            
            # Convert to display format
            initiator_status = '✅' if initiator_role else '⏳'
            counterparty_status = '✅' if counterparty_role else '⏳'
            
            initiator_display = initiator_role if initiator_role else 'Waiting...'
            counterparty_display = counterparty_role if counterparty_role else 'Waiting...'
            
            # Format counterparty display name - use hyperlink for user ID, @username otherwise
            if counterparty_user_id:
                counterparty_display_name = f"<a href=\"tg://user?id={counterparty_user_id}\">User {counterparty_user_id}</a>"
            else:
                counterparty_display_name = f"@{counterparty_username}"
            
            # Update message text
            updated_text = (
                "<b>📋 Step 1 - Select Roles</b>\n\n"
                "<b>⚠️ Choose roles accordingly</b>\n\n"
                "<b>As release & refund happen according to roles</b>\n\n"
                "<b>Refund goes to seller & release to buyer</b>\n\n"
                f"<b>{initiator_status}</b> @{initiator_username} - {initiator_display}\n"
                f"<b>{counterparty_status}</b> {counterparty_display_name} - {counterparty_display}"
            )
            
            # Create keyboard - buttons always visible
            if both_selected:
                # Both roles selected - no buttons
                reply_markup = None
                logger.info(f"✅ Both roles selected in room {original_chat_id}")
            else:
                # Always show both buttons
                keyboard = [
                    [
                        InlineKeyboardButton("💰 I am Buyer", callback_data=f"role_buyer_{original_chat_id}_{initiator_username.lower()}"),
                        InlineKeyboardButton("💵 I am Seller", callback_data=f"role_seller_{original_chat_id}_{initiator_username.lower()}")
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Edit the message
            await query.edit_message_caption(
                caption=updated_text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
            
            logger.info(f"✅ Updated role message in room {original_chat_id}")
            
            await query.answer(f"✅ You selected: {role_type}")
            
            # Check if both roles are now selected and send Step 2 (Blockchain)
            if both_selected and original_chat_id not in step2_blockchain_messages:
                logger.info(f"🔄 Both roles selected, preparing Step 2 (Blockchain) for room {original_chat_id}")
                
                # Save roles to database
                buyer_user = None
                seller_user = None
                for uname, urole in user_roles[original_chat_id].items():
                    if urole == 'BUYER':
                        buyer_user = uname
                    elif urole == 'SELLER':
                        seller_user = uname
                
                if buyer_user and seller_user:
                    database.set_roles(
                        chat_id=original_chat_id,
                        buyer_username=buyer_user,
                        seller_username=seller_user,
                        buyer_user_id=get_user_id(buyer_user),
                        seller_user_id=get_user_id(seller_user)
                    )
                    logger.info(f"📊 Saved roles to database: buyer={buyer_user}, seller={seller_user}")
                
                await send_step2_blockchain_message(context.bot, send_chat_id, original_chat_id)
            
            return CHOOSING
            
        except Exception as e:
            logger.warning(f"❌ Error handling role selection: {e}")
            await query.answer("❌ Error processing your selection", show_alert=True)
            return CHOOSING
    
    # Handle payment sent callbacks
    elif query.data.startswith('payment_sent_'):
        try:
            parts = query.data.split('_')
            chat_id = int(parts[2])
            username = query.from_user.username or query.from_user.first_name
            username_lower = username.lower()
            
            # Get seller username
            seller_username = room_initiators[chat_id].get('seller') if chat_id in room_initiators else None
            
            # Only seller can tap this button
            if not seller_username or username_lower != seller_username.lower():
                await query.answer("❌ Only the seller can confirm payment", show_alert=True)
                return CHOOSING
            
            # Send request for transaction hash
            send_chat_id = -1000000000000 - chat_id
            hash_request_text = f"⌛ @{seller_username} kindly paste the transaction hash or explorer link."
            
            await context.bot.send_message(
                chat_id=send_chat_id,
                text=hash_request_text,
                parse_mode='HTML'
            )
            
            # Set state to awaiting transaction hash
            room_awaiting_hash[chat_id] = 'awaiting_hash'
            room_transaction_state[chat_id] = 'awaiting_hash'
            
            logger.info(f"✅ Sent transaction hash request to room {chat_id}")
            await query.answer("✅ Payment marked as sent. Awaiting transaction details...")
            return CHOOSING
            
        except Exception as e:
            logger.warning(f"❌ Error handling payment sent: {e}")
            await query.answer("❌ Error", show_alert=True)
            return CHOOSING
    
    # Handle wallet callbacks
    elif query.data.startswith('wallet_'):
        try:
            parts = query.data.split('_')
            action = parts[1]  # deposit, withdraw, transactions, refresh, depnet, wdnet
            wallet_user_id = int(parts[2])
            
            # Verify user is authorized and is the wallet owner
            if user_id not in WALLET_AUTHORIZED_USERS or user_id != wallet_user_id:
                await query.answer("❌ Not authorized", show_alert=True)
                return CHOOSING
            
            # Initialize wallet if not exists
            wallet = init_wallet(user_id)
            
            # Helper function to format all balances
            def format_balances():
                return (
                    f"├ USDT (BSC): <code>{wallet['usdt_bsc']:.2f}</code>\n"
                    f"├ USDT (TRON): <code>{wallet['usdt_tron']:.2f}</code>\n"
                    f"├ USDC (BSC): <code>{wallet['usdc_bsc']:.2f}</code>\n"
                    f"├ BNB (BSC): <code>{wallet['bnb_bsc']:.4f}</code>\n"
                    f"└ TRX (TRON): <code>{wallet['trx_tron']:.2f}</code>"
                )
            
            if action == 'refresh':
                # Refresh wallet display with all token balances
                wallet_text = (
                    f"💰 <b>Virtual Wallet</b>\n\n"
                    f"<b>User:</b> @{username}\n"
                    f"<b>User ID:</b> <code>{user_id}</code>\n\n"
                    f"<b>Balances:</b>\n"
                    f"{format_balances()}\n\n"
                    f"Select an option below:"
                )
                
                keyboard = [
                    [
                        InlineKeyboardButton("💵 Deposit", callback_data=f"wallet_deposit_{user_id}"),
                        InlineKeyboardButton("💸 Withdraw", callback_data=f"wallet_withdraw_{user_id}")
                    ],
                    [
                        InlineKeyboardButton("📜 Transactions", callback_data=f"wallet_transactions_{user_id}"),
                        InlineKeyboardButton("🔄 Refresh", callback_data=f"wallet_refresh_{user_id}")
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(
                    wallet_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                await query.answer("✅ Refreshed")
                
            elif action == 'deposit':
                # Show deposit token selection
                deposit_text = (
                    f"💵 <b>Deposit to Virtual Wallet</b>\n\n"
                    f"<b>Current Balances:</b>\n"
                    f"{format_balances()}\n\n"
                    f"Select token to deposit:"
                )
                
                keyboard = [
                    [InlineKeyboardButton("USDT (BSC)", callback_data=f"wallet_depnet_{user_id}_usdt_bsc")],
                    [InlineKeyboardButton("USDT (TRON)", callback_data=f"wallet_depnet_{user_id}_usdt_tron")],
                    [InlineKeyboardButton("USDC (BSC)", callback_data=f"wallet_depnet_{user_id}_usdc_bsc")],
                    [InlineKeyboardButton("BNB (BSC)", callback_data=f"wallet_depnet_{user_id}_bnb_bsc")],
                    [InlineKeyboardButton("TRX (TRON)", callback_data=f"wallet_depnet_{user_id}_trx_tron")],
                    [InlineKeyboardButton("⬅️ Back", callback_data=f"wallet_refresh_{user_id}")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(
                    deposit_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                await query.answer()
            
            elif action == 'depnet':
                # Show deposit info for specific token with user-specific deposit address
                token = f"{parts[3]}_{parts[4]}"  # e.g., usdt_bsc
                token_name = WALLET_TOKENS.get(token, token.upper())
                network = TOKEN_NETWORK_MAP.get(token, 'bsc')
                
                # Get or generate user-specific deposit address
                deposit_address = get_user_deposit_address(user_id, network)
                
                # Format balance based on token (BNB uses 4 decimals)
                balance_fmt = f"{wallet[token]:.4f}" if token == 'bnb_bsc' else f"{wallet[token]:.2f}"
                
                deposit_text = (
                    f"💵 <b>Deposit {token_name}</b>\n\n"
                    f"<b>Current Balance:</b> <code>{balance_fmt}</code>\n\n"
                    f"<b>Your Deposit Address ({network.upper()}):</b>\n"
                    f"<code>{deposit_address}</code>\n\n"
                    f"⚠️ Send only <b>{token_name}</b> to this address.\n"
                    f"After depositing, contact admin with your tx hash."
                )
                
                keyboard = [
                    [InlineKeyboardButton("⬅️ Back", callback_data=f"wallet_deposit_{user_id}")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(
                    deposit_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                await query.answer()
                
            elif action == 'withdraw':
                # Show withdraw token selection
                withdraw_text = (
                    f"💸 <b>Withdraw from Virtual Wallet</b>\n\n"
                    f"<b>Current Balances:</b>\n"
                    f"{format_balances()}\n\n"
                    f"Select token to withdraw:"
                )
                
                keyboard = [
                    [InlineKeyboardButton("USDT (BSC)", callback_data=f"wallet_wdnet_{user_id}_usdt_bsc")],
                    [InlineKeyboardButton("USDT (TRON)", callback_data=f"wallet_wdnet_{user_id}_usdt_tron")],
                    [InlineKeyboardButton("USDC (BSC)", callback_data=f"wallet_wdnet_{user_id}_usdc_bsc")],
                    [InlineKeyboardButton("BNB (BSC)", callback_data=f"wallet_wdnet_{user_id}_bnb_bsc")],
                    [InlineKeyboardButton("TRX (TRON)", callback_data=f"wallet_wdnet_{user_id}_trx_tron")],
                    [InlineKeyboardButton("⬅️ Back", callback_data=f"wallet_refresh_{user_id}")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(
                    withdraw_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                await query.answer()
            
            elif action == 'wdnet':
                # Show withdraw info for specific token
                token = f"{parts[3]}_{parts[4]}"  # e.g., usdt_bsc
                token_name = WALLET_TOKENS.get(token, token.upper())
                network = TOKEN_NETWORK_MAP.get(token, 'bsc')
                
                # Format balance based on token (BNB uses 4 decimals)
                balance_fmt = f"{wallet[token]:.4f}" if token == 'bnb_bsc' else f"{wallet[token]:.2f}"
                
                # Get token symbol for minimum withdrawal
                token_symbol = token_name.split()[0]  # e.g., "USDT" from "USDT (BSC)"
                
                withdraw_text = (
                    f"💸 <b>Withdraw {token_name}</b>\n\n"
                    f"<b>Current Balance:</b> <code>{balance_fmt}</code>\n"
                    f"<b>Network:</b> {network.upper()}\n\n"
                    f"To withdraw, contact admin with:\n"
                    f"• Amount to withdraw\n"
                    f"• Your {network.upper()} wallet address\n\n"
                    f"Minimum withdrawal: 10 {token_symbol}"
                )
                
                keyboard = [
                    [InlineKeyboardButton("⬅️ Back", callback_data=f"wallet_withdraw_{user_id}")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(
                    withdraw_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                await query.answer()
                
            elif action == 'transactions':
                # Show transaction history
                transactions = wallet.get('transactions', [])
                if transactions:
                    tx_list = "\n".join([f"• {tx}" for tx in transactions[-10:]])  # Last 10
                else:
                    tx_list = "No transactions yet"
                
                tx_text = (
                    f"📜 <b>Transaction History</b>\n\n"
                    f"<b>Current Balances:</b>\n"
                    f"{format_balances()}\n\n"
                    f"{tx_list}"
                )
                
                keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data=f"wallet_refresh_{user_id}")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(
                    tx_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                await query.answer()
            
            return CHOOSING
            
        except Exception as e:
            logger.warning(f"❌ Error handling wallet callback: {e}")
            await query.answer("❌ Error", show_alert=True)
            return CHOOSING
    
    return CHOOSING


async def verify_transaction_bscscan(tx_hash: str, escrow_address: str, token: str = 'USDT') -> dict:
    """
    Verify transaction on BSCscan - properly handles BEP20 token transfers
    For BEP20 tokens (USDT/USDC), parses transaction receipt logs to find Transfer events
    For native BNB, checks tx.to and tx.value directly
    
    Returns: {
        'valid': bool,
        'amount': str,
        'from_address': str,
        'to_address': str,
        'block_number': str,
        'error': str or None
    }
    """
    try:
        # Normalize addresses to lowercase for comparison
        escrow_address = escrow_address.lower()
        tx_hash = tx_hash.strip()
        
        # Ensure tx_hash has 0x prefix
        if not tx_hash.startswith('0x'):
            tx_hash = '0x' + tx_hash
        
        # Validate hash length (should be 66 chars: 0x + 64 hex)
        if len(tx_hash) != 66:
            logger.error(f"❌ Invalid transaction hash length: {len(tx_hash)} (expected 66)")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': f'❌ Invalid transaction hash format (must be 66 characters including 0x)'
            }
        
        # Validate it's hex
        try:
            int(tx_hash[2:], 16)
        except ValueError:
            logger.error(f"❌ Transaction hash contains non-hex characters")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': f'❌ Transaction hash must contain only hexadecimal characters (0-9, a-f)'
            }
        
        logger.info(f"🔍 Verifying BSC transaction: {tx_hash} to escrow: {escrow_address} for token: {token}")
        
        # Use BSC JSON-RPC endpoint directly (more reliable than BSCscan proxy API)
        bsc_rpc_url = "https://bsc-dataseed.binance.org/"
        
        # For native BNB transfers, check transaction directly
        if token == 'BNB':
            # Get transaction by hash using JSON-RPC
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_getTransactionByHash",
                "params": [tx_hash],
                "id": 1
            }
            
            response = requests.post(bsc_rpc_url, json=payload, timeout=15)
            data = response.json()
            
            if not data.get('result') or not isinstance(data.get('result'), dict):
                logger.error(f"❌ Transaction not found on BSC: {tx_hash}")
                return {
                    'valid': False,
                    'amount': None,
                    'from_address': None,
                    'to_address': None,
                    'block_number': None,
                    'error': '❌ Transaction not found on BSC network'
                }
            
            tx_data = data['result']
            from_address = (tx_data.get('from') or '').lower()
            to_address = (tx_data.get('to') or '').lower()
            value_hex = tx_data.get('value', '0x0')
            block_number = tx_data.get('blockNumber', 'N/A')
            
            # Check if recipient is the escrow address
            if to_address != escrow_address:
                logger.warning(f"❌ BNB sent to {to_address}, not escrow {escrow_address}")
                return {
                    'valid': False,
                    'amount': None,
                    'from_address': None,
                    'to_address': None,
                    'block_number': None,
                    'error': f'❌ Transaction not sent to escrow address'
                }
            
            # Convert BNB value (18 decimals)
            value_wei = int(value_hex, 16)
            value_bnb = value_wei / 1e18
            
            logger.info(f"✅ BNB transaction verified! Amount: {value_bnb:.4f} BNB from {from_address}")
            
            return {
                'valid': True,
                'amount': f"{value_bnb:.4f}",
                'from_address': from_address,
                'to_address': to_address,
                'block_number': str(int(block_number, 16)) if block_number.startswith('0x') else block_number,
                'error': None
            }
        
        # For BEP20 tokens (USDT/USDC), get transaction receipt and parse logs using JSON-RPC
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash],
            "id": 1
        }
        
        response = requests.post(bsc_rpc_url, json=payload, timeout=15)
        logger.info(f"📊 BSC RPC Receipt Response Status: {response.status_code}")
        
        if not response.text:
            logger.error(f"❌ BSCscan API returned empty response for hash: {tx_hash}")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': '❌ BSCscan API returned empty response'
            }
        
        try:
            data = response.json()
        except Exception as json_err:
            logger.error(f"❌ Failed to parse BSCscan response: {json_err}")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': f'❌ Invalid API response format'
            }
        
        logger.info(f"📊 BSCscan Receipt Response: {str(data)[:300]}")
        
        if not data.get('result') or not isinstance(data.get('result'), dict):
            logger.error(f"❌ Transaction not found on BSC: {tx_hash}")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': '❌ Transaction not found on BSC network'
            }
        
        receipt = data['result']
        
        # Check transaction status (1 = success, 0 = failed)
        status = receipt.get('status', '0x0')
        if status != '0x1':
            logger.error(f"❌ Transaction failed (status: {status})")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': '❌ Transaction failed on blockchain'
            }
        
        from_address = (receipt.get('from') or '').lower()
        block_number = receipt.get('blockNumber', 'N/A')
        logs = receipt.get('logs', [])
        
        logger.info(f"📋 Receipt - From: {from_address}, Block: {block_number}, Logs count: {len(logs)}")
        
        # Get the expected token contract address
        token_contract = TOKEN_CONTRACTS.get('BSC', {}).get(token, '').lower()
        if not token_contract:
            logger.warning(f"⚠️ Unknown token contract for {token}, checking all Transfer events")
        
        # Parse logs to find Transfer events to the escrow address
        # Sum ALL matching transfers (a single tx can have multiple Transfer events from swaps/routers)
        transfer_found = False
        total_transfer_amount = 0
        transfer_count = 0
        
        for log in logs:
            # Check if this is a Transfer event (topic0 = Transfer signature)
            topics = log.get('topics', [])
            if len(topics) < 3:
                continue
            
            if topics[0].lower() != TRANSFER_EVENT_SIGNATURE.lower():
                continue
            
            # Transfer event: topics[1] = from (padded), topics[2] = to (padded)
            # Extract 'to' address from topic2 (last 40 chars after 0x and padding)
            log_to_address = '0x' + topics[2][-40:].lower()
            log_from_address = '0x' + topics[1][-40:].lower()
            log_contract = log.get('address', '').lower()
            
            logger.info(f"📋 Transfer Log - Contract: {log_contract}, From: {log_from_address}, To: {log_to_address}")
            
            # Check if this transfer is to our escrow address
            if log_to_address == escrow_address:
                # If we have a specific token contract, REQUIRE it to match (don't accept other tokens)
                if token_contract and log_contract != token_contract:
                    logger.info(f"⚠️ Transfer to escrow but wrong token contract: {log_contract} != {token_contract}")
                    continue
                
                # Extract amount from data field
                data_hex = log.get('data', '0x0')
                try:
                    amount_raw = int(data_hex, 16)
                    # Get decimals for this token
                    decimals = TOKEN_DECIMALS.get(token, 18)
                    single_transfer_amount = amount_raw / (10 ** decimals)
                    total_transfer_amount += single_transfer_amount
                    transfer_count += 1
                    transfer_found = True
                    logger.info(f"✅ Found Transfer #{transfer_count} to escrow! Amount: {single_transfer_amount} {token}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not parse transfer amount: {e}")
        
        if transfer_count > 1:
            logger.info(f"📊 Total from {transfer_count} transfers: {total_transfer_amount} {token}")
        
        if not transfer_found:
            logger.warning(f"❌ No Transfer event found to escrow address {escrow_address}")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': f'❌ No {token} transfer found to escrow address'
            }
        
        logger.info(f"✅ Transaction verified! Total amount: {total_transfer_amount:.4f} {token} from {from_address}")
        
        return {
            'valid': True,
            'amount': f"{total_transfer_amount:.4f}",
            'from_address': from_address,
            'to_address': escrow_address,
            'block_number': str(int(block_number, 16)) if block_number.startswith('0x') else block_number,
            'error': None
        }
    
    except Exception as e:
        logger.warning(f"❌ Error verifying BSC transaction: {e}")
        return {
            'valid': False,
            'amount': None,
            'from_address': None,
            'to_address': None,
            'block_number': None,
            'error': f'❌ Error verifying transaction: {str(e)}'
        }


async def verify_transaction_tron(tx_hash: str, escrow_address: str, token: str = 'USDT') -> dict:
    """
    Verify transaction on TRON network using TronGrid API
    For TRC20 tokens (USDT), parses transaction info to find Transfer events
    For native TRX, checks transaction value directly
    
    Returns: {
        'valid': bool,
        'amount': str,
        'from_address': str,
        'to_address': str,
        'block_number': str,
        'error': str or None
    }
    """
    try:
        tx_hash = tx_hash.strip()
        escrow_address_upper = escrow_address.upper()  # TRON addresses are case-sensitive
        
        # Validate hash length (TRON tx hashes are 64 hex chars)
        if len(tx_hash) != 64:
            logger.error(f"❌ Invalid TRON transaction hash length: {len(tx_hash)} (expected 64)")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': f'❌ Invalid TRON transaction hash format (must be 64 characters)'
            }
        
        # Validate it's hex
        try:
            int(tx_hash, 16)
        except ValueError:
            logger.error(f"❌ TRON transaction hash contains non-hex characters")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': f'❌ Transaction hash must contain only hexadecimal characters (0-9, a-f)'
            }
        
        logger.info(f"🔍 Verifying TRON transaction: {tx_hash} to escrow: {escrow_address} for token: {token}")
        
        trongrid_api_key = os.getenv('TRONGRID_API_KEY', '')
        
        if not trongrid_api_key:
            logger.warning("❌ TRONGRID_API_KEY not configured")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': '❌ TronGrid API key not configured'
            }
        
        # TronGrid API endpoint
        api_url = f"https://api.trongrid.io/v1/transactions/{tx_hash}/info"
        headers = {
            'TRON-PRO-API-KEY': trongrid_api_key
        }
        
        response = requests.get(api_url, headers=headers, timeout=15)
        logger.info(f"📊 TronGrid Response Status: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"❌ TronGrid API error: {response.status_code}")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': f'❌ TronGrid API error (status {response.status_code})'
            }
        
        try:
            data = response.json()
        except Exception as json_err:
            logger.error(f"❌ Failed to parse TronGrid response: {json_err}")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': f'❌ Invalid API response format'
            }
        
        logger.info(f"📊 TronGrid Response: {str(data)[:300]}")
        
        # Check if transaction exists
        if not data or 'id' not in data:
            logger.error(f"❌ Transaction not found on TRON: {tx_hash}")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': '❌ Transaction not found on TRON network'
            }
        
        # Check transaction result
        receipt = data.get('receipt', {})
        result = receipt.get('result', '')
        if result != 'SUCCESS':
            logger.error(f"❌ TRON transaction failed (result: {result})")
            return {
                'valid': False,
                'amount': None,
                'from_address': None,
                'to_address': None,
                'block_number': None,
                'error': f'❌ Transaction failed on TRON (result: {result})'
            }
        
        block_number = str(data.get('blockNumber', 'N/A'))
        
        # For native TRX transfers
        if token == 'TRX':
            # Check contract data for TRX transfer
            contract_data = data.get('contract_data', {})
            to_address = contract_data.get('to_address', '')
            amount_raw = contract_data.get('amount', 0)
            owner_address = contract_data.get('owner_address', '')
            
            if to_address.upper() != escrow_address_upper:
                logger.warning(f"❌ TRX sent to {to_address}, not escrow {escrow_address}")
                return {
                    'valid': False,
                    'amount': None,
                    'from_address': None,
                    'to_address': None,
                    'block_number': None,
                    'error': f'❌ Transaction not sent to escrow address'
                }
            
            # TRX has 6 decimals
            amount_trx = amount_raw / 1e6
            
            logger.info(f"✅ TRX transaction verified! Amount: {amount_trx:.4f} TRX from {owner_address}")
            
            return {
                'valid': True,
                'amount': f"{amount_trx:.4f}",
                'from_address': owner_address,
                'to_address': to_address,
                'block_number': block_number,
                'error': None
            }
        
        # For TRC20 tokens (USDT), check token transfers
        token_transfers = data.get('tokenTransferInfo', [])
        
        if not token_transfers:
            # Try alternative field name
            token_transfers = data.get('token_transfer_info', [])
        
        logger.info(f"📋 Token transfers found: {len(token_transfers)}")
        
        # Get expected token contract
        token_contract = TOKEN_CONTRACTS.get('TRON', {}).get(token, '')
        
        for transfer in token_transfers:
            to_addr = transfer.get('to_address', '')
            from_addr = transfer.get('from_address', '')
            amount_str = transfer.get('amount_str', '0')
            contract_address = transfer.get('contract_address', '')
            decimals = int(transfer.get('decimals', 6))
            
            logger.info(f"📋 Transfer - From: {from_addr}, To: {to_addr}, Amount: {amount_str}, Contract: {contract_address}")
            
            # Check if this transfer is to our escrow address
            if to_addr.upper() == escrow_address_upper:
                # Verify token contract if we have one
                if token_contract and contract_address != token_contract:
                    logger.info(f"⚠️ Transfer to escrow but wrong token contract: {contract_address} != {token_contract}")
                    continue
                
                # Parse amount
                try:
                    amount_raw = int(amount_str)
                    transfer_amount = amount_raw / (10 ** decimals)
                except:
                    transfer_amount = float(amount_str) if amount_str else 0
                
                logger.info(f"✅ TRON transaction verified! Amount: {transfer_amount:.4f} {token} from {from_addr}")
                
                return {
                    'valid': True,
                    'amount': f"{transfer_amount:.4f}",
                    'from_address': from_addr,
                    'to_address': to_addr,
                    'block_number': block_number,
                    'error': None
                }
        
        logger.warning(f"❌ No {token} transfer found to escrow address {escrow_address}")
        return {
            'valid': False,
            'amount': None,
            'from_address': None,
            'to_address': None,
            'block_number': None,
            'error': f'❌ No {token} transfer found to escrow address'
        }
    
    except Exception as e:
        logger.warning(f"❌ Error verifying TRON transaction: {e}")
        return {
            'valid': False,
            'amount': None,
            'from_address': None,
            'to_address': None,
            'block_number': None,
            'error': f'❌ Error verifying transaction: {str(e)}'
        }


async def send_deposit_found_message(bot, send_chat_id: int, amount: str, seller_addr: str, to_addr: str, tx_hash: str, block_number: str = None) -> None:
    """Send deposit found confirmation message"""
    try:
        # Get first 10 characters of transaction hash
        tx_short = tx_hash[:10]
        
        # Format the message with bold and monospace text
        message_text = (
            f"<b>P2P MM Bot 🤖</b>\n\n"
            f"<b>🟢 Exact USDT found</b>\n\n"
            f"<b>Total Amount:</b> {amount} USDT\n"
            f"<b>Transactions:</b> 1 transaction(s)\n"
            f"<b>From:</b> <code>{seller_addr}</code>\n"
            f"<b>To:</b> <code>{to_addr}</code>\n"
            f"<b>Main Tx:</b> <code>{tx_short}...</code>"
        )
        
        image_path = os.path.join(SCRIPT_DIR, "deposit_found_image.jpg")
        if os.path.exists(image_path):
            await bot.send_photo(
                chat_id=send_chat_id,
                photo=open(image_path, 'rb'),
                caption=message_text,
                parse_mode='HTML'
            )
            logger.info(f"✅ Sent deposit found message to room {send_chat_id}")
        else:
            await bot.send_message(
                chat_id=send_chat_id,
                text=message_text,
                parse_mode='HTML'
            )
            logger.warning(f"⚠️ Sent deposit found (text only) - image not found")
    
    except Exception as e:
        logger.warning(f"❌ Failed to send deposit found message: {e}")


async def send_deal_complete_message(bot, send_chat_id: int, chat_id: int, buyer_addr: str) -> None:
    """Send deal complete confirmation message"""
    try:
        # Calculate time taken
        if chat_id in room_creation_times:
            start_time = room_creation_times[chat_id]
            current_time = time.time()
            time_taken_seconds = int(current_time - start_time)
            time_taken_minutes = time_taken_seconds // 60
            time_taken_text = f"{time_taken_minutes} mins" if time_taken_minutes > 0 else f"{time_taken_seconds} secs"
        else:
            time_taken_text = "N/A"
        
        # Build BSCscan URL for buyer's wallet
        bscscan_url = f"https://bscscan.com/address/{buyer_addr}"
        
        # Format the message with bold text and hyperlink
        message_text = (
            f"🎉 <b>Deal Complete!</b> ✅\n\n"
            f"⏱️ <b>Time Taken:</b> {time_taken_text}\n"
            f"🔗 <b>Release TX Link:</b> <a href='{bscscan_url}'>Click Here</a>\n\n"
            f"Thank you for using our safe escrow system."
        )
        
        # Create close deal button
        keyboard = [[InlineKeyboardButton("❌ Close Deal", callback_data=f"close_deal_{chat_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        image_path = os.path.join(SCRIPT_DIR, "deal_complete_image.jpg")
        if os.path.exists(image_path):
            await bot.send_photo(
                chat_id=send_chat_id,
                photo=open(image_path, 'rb'),
                caption=message_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            logger.info(f"✅ Sent deal complete message to room {chat_id}")
        else:
            await bot.send_message(
                chat_id=send_chat_id,
                text=message_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            logger.warning(f"⚠️ Sent deal complete (text only) to room {chat_id} - image not found")
    
    except Exception as e:
        logger.warning(f"❌ Failed to send deal complete message: {e}")


async def send_step4_amount_message(bot, send_chat_id: int, chat_id: int) -> None:
    """Send Step 4 - Enter USDT amount message"""
    try:
        if chat_id in step4_amount_messages_sent:
            logger.info(f"⏭️ Step 4 already sent to room {chat_id}, skipping")
            return
        
        # Mark as sent EARLY to prevent race conditions
        step4_amount_messages_sent.add(chat_id)
        
        # Get the selected coin (default to USDT)
        selected_coin = user_coins.get(chat_id, 'USDT')
        
        # Get the selected blockchain and calculate network fee
        selected_chain = user_blockchain.get(chat_id, 'BSC')
        if selected_chain == 'TRON':
            network_fee = 3
        else:  # BSC
            network_fee = 0.2
        
        step4_text = (
            f"<b>💰 Step 4 - Enter {selected_coin} Amount</b>\n\n"
            f"Chain: {selected_chain}\n"
            f"Network Fee: {network_fee} {selected_coin}\n\n"
            "Enter amount including fee → Example: 1000"
        )
        
        image_path = "step1_quantity_image.jpg"
        if os.path.exists(image_path):
            await bot.send_photo(
                chat_id=send_chat_id,
                photo=open(image_path, 'rb'),
                caption=step4_text,
                parse_mode='HTML'
            )
            logger.info(f"✅ Sent Step 4 (amount) message to room {chat_id}")
        else:
            await bot.send_message(
                chat_id=send_chat_id,
                text=step4_text,
                parse_mode='HTML'
            )
            logger.warning(f"⚠️ Sent Step 4 (text only) to room {chat_id} - image not found")
        
        room_transaction_state[chat_id] = 'step4_amount'
        
    except Exception as e:
        error_str = str(e).lower()
        if 'timed out' in error_str or 'timeout' in error_str:
            logger.info(f"⏱️ Step 4 message may have been sent (timeout) to room {chat_id}")
            room_transaction_state[chat_id] = 'step4_amount'
        else:
            logger.warning(f"❌ Failed to send Step 4 message: {e}")


async def send_step5_rate_message(bot, send_chat_id: int, chat_id: int) -> None:
    """Send Step 5 - Enter rate per USDT message"""
    try:
        if room_transaction_state.get(chat_id) != 'step4_amount':
            logger.warning(f"⚠️ Step 5 called but room not in step4_amount state")
            return
        
        step5_text = "📊 Step 5 - Rate per USDT → Example: 89.5"
        
        image_path = "step2_rate_image.jpg"
        if os.path.exists(image_path):
            await bot.send_photo(
                chat_id=send_chat_id,
                photo=open(image_path, 'rb'),
                caption=step5_text,
                parse_mode='HTML'
            )
            logger.info(f"✅ Sent Step 5 (rate) message to room {chat_id}")
        else:
            await bot.send_message(
                chat_id=send_chat_id,
                text=step5_text,
                parse_mode='HTML'
            )
            logger.warning(f"⚠️ Sent Step 5 (text only) to room {chat_id} - image not found")
        
        room_transaction_state[chat_id] = 'step5_rate'
        
    except Exception as e:
        error_str = str(e).lower()
        if 'timed out' in error_str or 'timeout' in error_str:
            logger.info(f"⏱️ Step 5 message may have been sent (timeout) to room {chat_id}")
            room_transaction_state[chat_id] = 'step5_rate'
        else:
            logger.warning(f"❌ Failed to send Step 5 message: {e}")


async def send_step6_payment_message(bot, send_chat_id: int, chat_id: int) -> None:
    """Send Step 6 - Payment Method message"""
    try:
        step6_text = "💳 Step 6 - Payment method → Examples: CDM, CASH, CCW"
        
        image_path = "step3_payment_image.jpg"
        if os.path.exists(image_path):
            await bot.send_photo(
                chat_id=send_chat_id,
                photo=open(image_path, 'rb'),
                caption=step6_text,
                parse_mode='HTML'
            )
            logger.info(f"✅ Sent Step 6 (payment method) message to room {chat_id}")
        else:
            await bot.send_message(
                chat_id=send_chat_id,
                text=step6_text,
                parse_mode='HTML'
            )
            logger.warning(f"⚠️ Sent Step 6 (text only) to room {chat_id} - image not found")
        
        room_transaction_state[chat_id] = 'step6_payment'
        
    except Exception as e:
        error_str = str(e).lower()
        if 'timed out' in error_str or 'timeout' in error_str:
            logger.info(f"⏱️ Step 6 message may have been sent (timeout) to room {chat_id}")
            room_transaction_state[chat_id] = 'step6_payment'
        else:
            logger.warning(f"❌ Failed to send Step 6 message: {e}")


async def send_step2_blockchain_message(bot, send_chat_id: int, chat_id: int) -> None:
    """Send Step 2 - Blockchain selection message with BSC and TRON buttons"""
    try:
        step2_text = "<b>Step 2 - Choose Blockchain</b>"
        
        keyboard = [[
            InlineKeyboardButton("BSC", callback_data=f"blockchain_bsc_{chat_id}"),
            InlineKeyboardButton("TRON", callback_data=f"blockchain_tron_{chat_id}")
        ]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        image_path = "step4_blockchain_image.jpg"
        if os.path.exists(image_path):
            msg = await bot.send_photo(
                chat_id=send_chat_id,
                photo=open(image_path, 'rb'),
                caption=step2_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            step2_blockchain_messages[chat_id] = msg.message_id
            logger.info(f"✅ Sent Step 2 (blockchain) message to room {chat_id}")
        else:
            msg = await bot.send_message(
                chat_id=send_chat_id,
                text=step2_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            step2_blockchain_messages[chat_id] = msg.message_id
            logger.warning(f"⚠️ Sent Step 2 (text only) to room {chat_id} - image not found")
        
        room_transaction_state[chat_id] = 'step2_blockchain'
        
    except Exception as e:
        error_str = str(e).lower()
        if 'timed out' in error_str or 'timeout' in error_str:
            logger.info(f"⏱️ Step 2 message may have been sent (timeout) to room {chat_id}")
            room_transaction_state[chat_id] = 'step2_blockchain'
        else:
            logger.warning(f"❌ Failed to send Step 2 message: {e}")


async def send_step3_coin_message(bot, send_chat_id: int, chat_id: int) -> None:
    """Send Step 3 - Select Coin message with USDT/USDC buttons (TRON only shows USDT)"""
    try:
        step3_text = "<b>Step 3 - Select Coin</b>"
        
        # Check if TRON is selected - only show USDT for TRON
        selected_blockchain = user_blockchain.get(chat_id, 'BSC')
        if selected_blockchain == 'TRON':
            # TRON only supports USDT
            keyboard = [[InlineKeyboardButton("USDT", callback_data=f"coin_usdt_{chat_id}")]]
        else:
            # BSC supports both USDT and USDC
            keyboard = [[
                InlineKeyboardButton("USDT", callback_data=f"coin_usdt_{chat_id}"),
                InlineKeyboardButton("USDC", callback_data=f"coin_usdc_{chat_id}")
            ]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        image_path = "step5_coin_image.jpg"
        if os.path.exists(image_path):
            msg = await bot.send_photo(
                chat_id=send_chat_id,
                photo=open(image_path, 'rb'),
                caption=step3_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            step3_coin_messages[chat_id] = msg.message_id
            logger.info(f"✅ Sent Step 3 (coin selection) message to room {chat_id}")
        else:
            msg = await bot.send_message(
                chat_id=send_chat_id,
                text=step3_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            step3_coin_messages[chat_id] = msg.message_id
            logger.warning(f"⚠️ Sent Step 3 (text only) to room {chat_id} - image not found")
        
        room_transaction_state[chat_id] = 'step3_coin'
        
    except Exception as e:
        error_str = str(e).lower()
        if 'timed out' in error_str or 'timeout' in error_str:
            logger.info(f"⏱️ Step 3 message may have been sent (timeout) to room {chat_id}")
            room_transaction_state[chat_id] = 'step3_coin'
        else:
            logger.warning(f"❌ Failed to send Step 3 message: {e}")


def build_deal_summary_text(chat_id: int, buyer_approved: bool = False, seller_approved: bool = False) -> str:
    """Build deal summary text with current formatting - single source of truth"""
    # Get deal data from database first (source of truth to prevent mixing)
    deal_data = database.get_deal(chat_id)
    
    # Initialize with database values if available
    if deal_data:
        amount = deal_data.get('amount')
        rate = deal_data.get('rate')
        payment_method = deal_data.get('payment_method')
        coin = deal_data.get('coin') or 'USDT'
        chain = deal_data.get('network') or 'BSC'
        buyer_address = deal_data.get('buyer_address') or buyer_addresses.get(chat_id, "N/A")
        seller_address = deal_data.get('seller_address') or seller_addresses.get(chat_id, "N/A")
        buyer_username = deal_data.get('buyer_username') or (room_initiators[chat_id].get('buyer') if chat_id in room_initiators else "Unknown")
        seller_username = deal_data.get('seller_username') or (room_initiators[chat_id].get('seller') if chat_id in room_initiators else "Unknown")
        logger.info(f"📊 Using database values for deal summary in room {chat_id}")
    else:
        # Fallback to in-memory values if database not available
        amount = None
        rate = None
        payment_method = None
        coin = user_coins.get(chat_id, 'USDT')
        chain = user_blockchain.get(chat_id, 'BSC')
        buyer_address = buyer_addresses.get(chat_id, "N/A")
        seller_address = seller_addresses.get(chat_id, "N/A")
        buyer_username = room_initiators[chat_id].get('buyer') if chat_id in room_initiators else "Unknown"
        seller_username = room_initiators[chat_id].get('seller') if chat_id in room_initiators else "Unknown"
        logger.warning(f"⚠️ No database record for room {chat_id}, using in-memory fallback")
    
    # Calculate network fee based on chain
    if chain == 'TRON':
        network_fee = 3.0
    else:  # BSC
        network_fee = 0.2
    
    # Get service fee: Use global fee if set via !setfees, otherwise per-room fee tier
    service_fee_percent = get_service_fee_percent(chat_id)
    
    logger.info(f"📊 Using service fee {service_fee_percent}% for room {chat_id}")
    
    # Calculate service fee amount
    amount_float = float(amount) if amount else 0
    service_fee_amount = amount_float * (service_fee_percent / 100)
    
    # Calculate release amount (amount - network fee - service fee)
    release_amount = amount_float - network_fee - service_fee_amount
    
    # Format values - use .1f for clean display (203.0 instead of 203.00000000)
    amount_formatted = f"{float(amount):.1f}" if amount else "0.0"
    rate_formatted = f"₹{rate:.1f}" if rate else "N/A"
    network_fee_formatted = f"{network_fee} {coin}"
    service_fee_formatted = format_fee_percent(service_fee_percent)
    release_amount_formatted = f"{release_amount:.1f} {coin}"
    
    # Build approval status strings
    buyer_status = f"✅ @{buyer_username} has approved." if buyer_approved else f"⏳ Waiting for @{buyer_username} to approve."
    seller_status = f"✅ @{seller_username} has approved." if seller_approved else f"⏳ Waiting for @{seller_username} to approve."
    
    deal_text = f"""📋  <b>Deal Summary</b>

• <b>Amount:</b> {amount_formatted} {coin}
• <b>Rate:</b> {rate_formatted}
• <b>Payment:</b> {payment_method}
• <b>Chain:</b> {chain}
• <b>Network Fee:</b> {network_fee_formatted}
• <b>Service Fee:</b> {service_fee_formatted}
• <b>Release Amount:</b> {release_amount_formatted}
• <b>Buyer Address:</b> <code>{buyer_address}</code>
• <b>Seller Address:</b> <code>{seller_address}</code>

🛑 <b>Do not send funds here</b> 🛑

{buyer_status}
{seller_status}"""
    
    return deal_text


async def send_deal_summary_message(bot, send_chat_id: int, chat_id: int) -> None:
    """Send Deal Summary message with approval button"""
    try:
        buyer_username = room_initiators[chat_id].get('buyer') if chat_id in room_initiators else "Unknown"
        seller_username = room_initiators[chat_id].get('seller') if chat_id in room_initiators else "Unknown"
        
        # Use the shared helper function to build deal text
        deal_text = build_deal_summary_text(chat_id, buyer_approved=False, seller_approved=False)
        
        keyboard = [[InlineKeyboardButton("Approve", callback_data=f"approve_deal_{chat_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        image_path = "deal_summary_image.jpg"
        if os.path.exists(image_path):
            msg = await bot.send_photo(
                chat_id=send_chat_id,
                photo=open(image_path, 'rb'),
                caption=deal_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            deal_summary_messages[chat_id] = msg.message_id
            logger.info(f"✅ Sent deal summary message to room {chat_id}")
            
            # Initialize approvals
            approvals[chat_id] = {'buyer': False, 'seller': False}
        else:
            msg = await bot.send_message(
                chat_id=send_chat_id,
                text=deal_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            deal_summary_messages[chat_id] = msg.message_id
            logger.warning(f"⚠️ Sent deal summary (text only) to room {chat_id} - image not found")
            
            # Initialize approvals
            approvals[chat_id] = {'buyer': False, 'seller': False}
        
    except Exception as e:
        logger.warning(f"❌ Failed to send deal summary message: {e}")


async def send_room_log_message(bot, chat_id: int, buyer_username: str, seller_username: str, 
                                 token_name: str, blockchain: str, amount: str, status: str) -> None:
    """Send or update the room log message with current status to the logs channel"""
    try:
        # Logs channel ID
        logs_channel_id = -1003266978268
        
        # Build the log message text
        log_text = (
            f"<b><u>NEW ROOM CREATED</u></b>\n\n"
            f"<b>Buyer:</b> @{buyer_username}\n"
            f"<b>Seller:</b> @{seller_username}\n"
            f"<b>Token:</b> {token_name} [{blockchain}]\n"
            f"<b>Amount:</b> {amount}\n"
            f"<b>Current Stage:</b> {status}"
        )
        
        # Check if we already have a log message for this room
        if chat_id in room_log_messages:
            # Edit existing message in logs channel
            try:
                msg_info = room_log_messages[chat_id]
                await bot.edit_message_text(
                    chat_id=logs_channel_id,
                    message_id=msg_info['msg_id'],
                    text=log_text,
                    parse_mode='HTML'
                )
                logger.info(f"✅ Updated room log message in logs channel for room {chat_id} - Status: {status}")
            except Exception as e:
                logger.warning(f"⚠️ Could not edit room log message: {e}")
        else:
            # Send new message to logs channel
            msg = await bot.send_message(
                chat_id=logs_channel_id,
                text=log_text,
                parse_mode='HTML'
            )
            room_log_messages[chat_id] = {'msg_id': msg.message_id, 'chat_id': logs_channel_id}
            logger.info(f"✅ Sent room log message to logs channel for room {chat_id}")
    
    except Exception as e:
        logger.warning(f"❌ Failed to send/update room log message: {e}")


async def update_room_log_status(bot, chat_id: int, status: str) -> None:
    """Update only the status field in the room log message"""
    try:
        if chat_id not in room_log_messages:
            logger.warning(f"⚠️ No room log message found for room {chat_id}")
            return
        
        # Get room data from database
        room_data = database.get_deal(chat_id)
        if not room_data:
            logger.warning(f"⚠️ No room data found for room {chat_id}")
            return
        
        buyer_username = room_data.get('buyer_username', 'Unknown')
        seller_username = room_data.get('seller_username', 'Unknown')
        token_name = room_data.get('coin', 'Unknown')
        blockchain = room_data.get('network', 'Unknown')
        amount = room_data.get('amount', 'Unknown')
        
        # Build the updated log message text
        log_text = (
            f"<b><u>NEW ROOM CREATED</u></b>\n\n"
            f"<b>Buyer:</b> @{buyer_username}\n"
            f"<b>Seller:</b> @{seller_username}\n"
            f"<b>Token:</b> {token_name} [{blockchain}]\n"
            f"<b>Amount:</b> {amount}\n"
            f"<b>Current Stage:</b> {status}"
        )
        
        # Edit existing message
        msg_info = room_log_messages[chat_id]
        await bot.edit_message_text(
            chat_id=msg_info['chat_id'],
            message_id=msg_info['msg_id'],
            text=log_text,
            parse_mode='HTML'
        )
        logger.info(f"✅ Updated room log status for room {chat_id} - Status: {status}")
    
    except Exception as e:
        logger.warning(f"❌ Failed to update room log status: {e}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle text messages for listing creation and transaction steps"""
    user = update.effective_user
    user_id = user.id
    text = update.message.text
    chat_id = update.effective_chat.id
    
    # Track user ID for username
    if user.username:
        save_user_id(user.username, user_id)
    
    logger.info(f"📨 Message received from {user.username} in chat {chat_id}: {text[:50]}")
    
    # Handle !setfees command (admin-only, works in any chat)
    if text.startswith('!setfees'):
        # Check if user is an authorized admin
        if user_id not in AUTHORIZED_KICK_USERS:
            await update.message.reply_text("❌ You are not authorized to use this command.")
            return
        
        # Parse the fee amount (e.g., !setfees 1% or !setfees 2.5%)
        match = re.match(r'!setfees\s+([\d.]+)%?', text)
        if not match:
            await update.message.reply_text(
                "❌ Invalid format!\n\n"
                "Usage: <code>!setfees 1%</code>\n"
                "Example: <code>!setfees 2.5%</code>",
                parse_mode='HTML'
            )
            return
        
        try:
            global current_fee_percent
            new_fee = float(match.group(1))
            if new_fee < 0 or new_fee > 100:
                await update.message.reply_text("❌ Fee must be between 0% and 100%.")
                return
            
            current_fee_percent = new_fee
            save_fee_setting(new_fee)
            
            await update.message.reply_text(
                f"✅ <b>Escrow Fees Updated</b>\n\n"
                f"New service fee: <b>{format_fee_percent(new_fee)}</b>\n\n"
                f"This fee will apply to all deals from now on.",
                parse_mode='HTML'
            )
            logger.info(f"Admin @{user.username} set escrow fees to {format_fee_percent(new_fee)}")
        except ValueError:
            await update.message.reply_text("❌ Invalid fee amount. Please enter a valid number.")
        return
    
    step = context.user_data.get('step')
    
    # Check if this message is from a deal room (supergroup)
    if chat_id > 0:  # Private message, use regular flow
        logger.info(f"Private message from {user.username}")
        pass
    else:  # Group/Supergroup message
        # Convert negative chat_id to positive for state lookup
        # For supergroups, telegram returns: -1003181521147
        # We store state with positive: 3181521147
        # Conversion: abs(chat_id) - 1000000000000 = original positive chat_id
        original_chat_id = abs(chat_id) - 1000000000000
        
        logger.info(f"Group message detected in room {chat_id}, using original_chat_id {original_chat_id}")
        
        # Check if room is waiting for amount input (Step 4 in new flow)
        if room_transaction_state.get(original_chat_id) == 'step4_amount':
            try:
                amount = float(text)
                if amount < 1:
                    await update.message.reply_text("❌ Amount must be at least 1")
                    return
                
                user_amounts[user_id] = amount
                logger.info(f"✅ User {user.username} entered amount: {amount} in room {original_chat_id}")
                
                # Save amount to database
                database.set_amount(original_chat_id, amount)
                
                # Send Step 5 (rate) message
                send_chat_id = -1000000000000 - original_chat_id
                await send_step5_rate_message(context.bot, send_chat_id, original_chat_id)
                return
            except ValueError:
                await update.message.reply_text("❌ Please enter a valid number")
                return
        
        # Check if room is waiting for rate input (Step 5 in new flow)
        elif room_transaction_state.get(original_chat_id) == 'step5_rate':
            try:
                rate = float(text)
                if rate < 85:
                    await update.message.reply_text("❌ Rate must be at least 85")
                    return
                
                user_rates[user_id] = rate
                logger.info(f"✅ User {user.username} entered rate: {rate} in room {original_chat_id}")
                
                # Save rate to database
                database.set_rate(original_chat_id, rate)
                
                # Send Step 6 message (Payment Method)
                send_chat_id = -1000000000000 - original_chat_id
                await send_step6_payment_message(context.bot, send_chat_id, original_chat_id)
                return
            except ValueError:
                await update.message.reply_text("❌ Please enter a valid number")
                return
        
        # Check if room is waiting for payment method input (Step 6 in new flow)
        elif room_transaction_state.get(original_chat_id) == 'step6_payment':
            # Payment method is case-insensitive - accept UPI, upi, Upi, etc.
            payment_method_upper = text.upper()
            if payment_method_upper not in valid_payment_methods:
                await update.message.reply_text("❌ Invalid Payment Method")
                return
            
            # Store as uppercase for consistency
            user_payment_methods[user_id] = payment_method_upper
            logger.info(f"✅ User {user.username} selected payment method: {payment_method_upper} in room {original_chat_id}")
            
            # Save payment method to database
            database.set_payment_method(original_chat_id, payment_method_upper)
            
            # Send Step 7 message (Buyer Wallet Address)
            send_chat_id = -1000000000000 - original_chat_id
            
            # Get buyer username from room_initiators or user_roles
            buyer_username = room_initiators.get(original_chat_id, {}).get('buyer')
            
            # Fallback to user_roles if room_initiators doesn't have buyer
            if not buyer_username and original_chat_id in user_roles:
                for uname, role in user_roles[original_chat_id].items():
                    if role == 'BUYER':
                        buyer_username = uname
                        break
            
            if buyer_username:
                # Get selected blockchain for address format hint
                selected_blockchain = user_blockchain.get(original_chat_id, 'BSC')
                if selected_blockchain == 'TRON':
                    address_hint = "starts with T and is 34 chars"
                else:
                    address_hint = "starts with 0x and is 42 chars (0x + 40 hex)"
                
                step7_text = f"<b>Step 7</b> - @{buyer_username}, enter your {selected_blockchain} wallet address\n{address_hint}"
                
                try:
                    image_path = os.path.join(SCRIPT_DIR, "step6_buyer_address_image.jpg")
                    if os.path.exists(image_path):
                        msg = await context.bot.send_photo(
                            chat_id=send_chat_id,
                            photo=open(image_path, 'rb'),
                            caption=step7_text,
                            parse_mode='HTML'
                        )
                        buyer_wallet_messages[original_chat_id] = msg.message_id
                        logger.info(f"✅ Sent buyer wallet message to room {original_chat_id}")
                    else:
                        msg = await context.bot.send_message(
                            chat_id=send_chat_id,
                            text=step7_text,
                            parse_mode='HTML'
                        )
                        buyer_wallet_messages[original_chat_id] = msg.message_id
                        logger.warning(f"⚠️ Sent buyer wallet (text only) to room {original_chat_id} - image not found")
                    
                    # Only set state to step7 AFTER successfully sending the message
                    room_transaction_state[original_chat_id] = 'step7_buyer_address'
                    
                except Exception as e:
                    logger.warning(f"Could not send buyer wallet message: {e}")
                    # Don't change state if message failed to send
            else:
                logger.warning(f"Could not find buyer username for room {original_chat_id}")
                await update.message.reply_text("❌ Could not identify buyer. Please restart the trade with /restart")
            return
        
        # Check if room is waiting for buyer wallet address input
        elif room_transaction_state.get(original_chat_id) == 'step7_buyer_address':
            # Check if this user is the buyer (only buyer can provide their wallet address)
            if original_chat_id in room_initiators:
                buyer_username = room_initiators[original_chat_id].get('buyer')
                if buyer_username and user.username and user.username.lower() != buyer_username.lower():
                    # Silently ignore seller's messages during this step
                    logger.info(f"⏭️ Ignoring message from {user.username} (not buyer) in room {original_chat_id}")
                    return
            
            # Validate wallet address format based on selected blockchain
            selected_blockchain = user_blockchain.get(original_chat_id, 'BSC')
            
            if selected_blockchain == 'TRON':
                # TRON addresses start with T and are 34 characters
                if not text.startswith('T'):
                    await update.message.reply_text("❌ Invalid TRON address format. Address must start with T and be 34 characters.")
                    return
                if len(text) != 34:
                    await update.message.reply_text("❌ Invalid TRON address format. Address must start with T and be 34 characters.")
                    return
            else:
                # BSC/ETH addresses start with 0x and are 42 characters
                if not text.startswith('0x'):
                    await update.message.reply_text("❌ Invalid address format. Address must start with 0x and be 42 characters (0x + 40 hexadecimal characters).")
                    return
                
                if len(text) != 42:
                    await update.message.reply_text("❌ Invalid address format. Address must start with 0x and be 42 characters (0x + 40 hexadecimal characters).")
                    return
                
                # Validate that remaining characters are hex
                hex_part = text[2:]  # Remove 0x prefix
                try:
                    int(hex_part, 16)  # Try to parse as hexadecimal
                except ValueError:
                    await update.message.reply_text("❌ Invalid address format. Address must start with 0x and be 42 characters (0x + 40 hexadecimal characters).")
                    return
            
            buyer_addresses[original_chat_id] = text
            save_room_data(original_chat_id)
            logger.info(f"✅ Buyer {user.username} entered wallet address: {text} in room {original_chat_id}")
            
            # Save buyer address to database
            database.set_buyer_address(original_chat_id, text)
            
            # Move to seller wallet address step
            room_transaction_state[original_chat_id] = 'step7_seller_address'
            
            # Send seller wallet address message
            send_chat_id = -1000000000000 - original_chat_id
            seller_username = room_initiators[original_chat_id].get('seller') if original_chat_id in room_initiators else None
            
            if seller_username:
                try:
                    # Get selected blockchain for address format hint
                    selected_blockchain = user_blockchain.get(original_chat_id, 'BSC')
                    step6_text = f"<b>Step 8</b> - @{seller_username}, enter your {selected_blockchain} wallet address\nto receive refund if deal is cancelled"
                    
                    image_path = "step6_buyer_address_image.jpg"
                    if os.path.exists(image_path):
                        msg = await context.bot.send_photo(
                            chat_id=send_chat_id,
                            photo=open(image_path, 'rb'),
                            caption=step6_text,
                            parse_mode='HTML'
                        )
                        seller_wallet_messages[original_chat_id] = msg.message_id
                        logger.info(f"✅ Sent seller wallet message to room {original_chat_id}")
                    else:
                        msg = await context.bot.send_message(
                            chat_id=send_chat_id,
                            text=step6_text,
                            parse_mode='HTML'
                        )
                        seller_wallet_messages[original_chat_id] = msg.message_id
                        logger.warning(f"⚠️ Sent seller wallet (text only) to room {original_chat_id} - image not found")
                except Exception as e:
                    logger.warning(f"Could not send seller wallet message: {e}")
            
            return
        
        # Check if room is waiting for seller wallet address input
        elif room_transaction_state.get(original_chat_id) == 'step7_seller_address':
            # Check if this user is the seller (only seller can provide their wallet address)
            if original_chat_id in room_initiators:
                seller_username = room_initiators[original_chat_id].get('seller')
                if seller_username and user.username and user.username.lower() != seller_username.lower():
                    # Silently ignore buyer's messages during this step
                    logger.info(f"⏭️ Ignoring message from {user.username} (not seller) in room {original_chat_id}")
                    return
            
            # Validate wallet address format based on selected blockchain
            selected_blockchain = user_blockchain.get(original_chat_id, 'BSC')
            
            if selected_blockchain == 'TRON':
                # TRON addresses start with T and are 34 characters
                if not text.startswith('T'):
                    await update.message.reply_text("❌ Invalid TRON address format. Address must start with T and be 34 characters.")
                    return
                if len(text) != 34:
                    await update.message.reply_text("❌ Invalid TRON address format. Address must start with T and be 34 characters.")
                    return
            else:
                # BSC/ETH addresses start with 0x and are 42 characters
                if not text.startswith('0x'):
                    await update.message.reply_text("❌ Invalid address format. Address must start with 0x and be 42 characters (0x + 40 hexadecimal characters).")
                    return
                
                if len(text) != 42:
                    await update.message.reply_text("❌ Invalid address format. Address must start with 0x and be 42 characters (0x + 40 hexadecimal characters).")
                    return
                
                # Validate that remaining characters are hex
                hex_part = text[2:]  # Remove 0x prefix
                try:
                    int(hex_part, 16)  # Try to parse as hexadecimal
                except ValueError:
                    await update.message.reply_text("❌ Invalid address format. Address must start with 0x and be 42 characters (0x + 40 hexadecimal characters).")
                    return
            
            seller_addresses[original_chat_id] = text
            save_room_data(original_chat_id)
            logger.info(f"✅ Seller {user.username} entered wallet address: {text} in room {original_chat_id}")
            
            # Save seller address to database and update status
            database.set_seller_address(original_chat_id, text)
            database.update_deal(original_chat_id, deal_status=database.DEAL_STATUS_SUMMARY_SHOWN)
            
            # Send deal summary message with approval button
            send_chat_id = -1000000000000 - original_chat_id
            await send_deal_summary_message(context.bot, send_chat_id, original_chat_id)
            
            # Send room log message with "Deal Summary" status
            room_data = database.get_deal(original_chat_id)
            if room_data:
                buyer_username = room_data.get('buyer_username', 'Unknown')
                seller_username = room_data.get('seller_username', 'Unknown')
                token_name = room_data.get('coin', 'Unknown')
                blockchain = room_data.get('network', 'Unknown')
                amount = room_data.get('amount', 'Unknown')
                await send_room_log_message(context.bot, original_chat_id, buyer_username, seller_username, 
                                           token_name, blockchain, amount, "Deal Summary")
            
            room_transaction_state[original_chat_id] = 'deal_summary'
            return
        
        # Check if room is waiting for transaction hash
        elif room_transaction_state.get(original_chat_id) == 'awaiting_hash':
            # Check if this user is the seller (only seller can provide tx hash)
            if original_chat_id in room_initiators:
                seller_username = room_initiators[original_chat_id].get('seller')
                if seller_username and user.username and user.username.lower() != seller_username.lower():
                    # Silently ignore buyer's messages during this step
                    logger.info(f"⏭️ Ignoring message from {user.username} (not seller) in room {original_chat_id}")
                    return
            
            try:
                # Get chat ID for sending messages
                send_chat_id = -1000000000000 - original_chat_id
                
                # Parse transaction hash or link
                tx_input = text.strip()
                
                # Extract hash from link if provided
                if 'bscscan.com/tx/' in tx_input:
                    # Extract from URL: https://bscscan.com/tx/0x123...
                    tx_hash = tx_input.split('tx/')[-1].split('?')[0].strip()
                    logger.info(f"🔗 Extracted hash from link: {tx_hash[:10]}...")
                elif 'etherscan.io/tx/' in tx_input:
                    # Also support etherscan format
                    tx_hash = tx_input.split('tx/')[-1].split('?')[0].strip()
                    logger.info(f"🔗 Extracted hash from link: {tx_hash[:10]}...")
                else:
                    # Assume it's a direct hash
                    tx_hash = tx_input
                    logger.info(f"📝 Using transaction hash directly: {tx_hash[:10]}...")
                
                # Delete the user's hash message
                try:
                    await update.message.delete()
                    logger.info(f"✅ Deleted hash message from {user.username} in room {original_chat_id}")
                except:
                    pass
                
                # Get blockchain and coin selection
                blockchain = user_blockchain.get(original_chat_id)
                coin = user_coins.get(original_chat_id)
                
                logger.info(f"🔍 Looking for escrow - Blockchain: {blockchain}, Coin: {coin}, User: {user_id}")
                
                # Get escrow address from database (stored when deal was confirmed)
                deal_data = database.get_deal(original_chat_id)
                escrow_address = deal_data.get('escrow_address') if deal_data else None
                
                if not escrow_address:
                    # Fallback to owner wallet if not in database
                    escrow_address = get_owner_wallet(blockchain) if blockchain else None
                    logger.warning(f"⚠️ Escrow not in database, using owner wallet: {escrow_address}")
                
                if not escrow_address:
                    logger.error(f"❌ Escrow not found for room {original_chat_id}")
                    await context.bot.send_message(
                        chat_id=send_chat_id,
                        text=f"❌ Escrow address not found. Please contact support."
                    )
                    return
                
                logger.info(f"✅ Found escrow: {escrow_address}")
                
                # Get the amount from database for this specific room (not from global dict)
                amount = None
                if deal_data and deal_data.get('amount'):
                    amount = float(deal_data['amount'])
                    logger.info(f"✅ Found amount from database: {amount}")
                
                if not amount:
                    logger.error(f"❌ Amount not found in database for room {original_chat_id}")
                    await context.bot.send_message(
                        chat_id=send_chat_id,
                        text="❌ Amount not found"
                    )
                    return
                
                # Check if this is the master hash (skip verification)
                if tx_hash.lower() == master_hash.lower():
                    logger.info(f"🔑 Master hash detected in room {original_chat_id}")
                    send_chat_id = -1000000000000 - original_chat_id
                    
                    # Get seller's address that was provided earlier
                    seller_addr = seller_addresses.get(original_chat_id, "0x" + "0" * 40)
                    
                    # For master hash, use seller's address
                    await send_deposit_found_message(
                        context.bot,
                        send_chat_id,
                        f"{amount:.2f}",
                        seller_addr,
                        escrow_address,
                        tx_hash,
                        block_number=None
                    )
                    
                    # Remove button from deposit address message
                    if original_chat_id in deposit_address_messages:
                        try:
                            msg_id = deposit_address_messages[original_chat_id]
                            # Build deposit text to remove button
                            blockchain = user_blockchain.get(original_chat_id, 'BSC')
                            coin = user_coins.get(original_chat_id, 'USDT')
                            escrow_addr = escrow_address
                            deposit_text_clean = f"""💳 {coin} {blockchain} Deposit\n\n🏦 {coin} {blockchain} Address: <code>{escrow_addr}</code>"""
                            await context.bot.edit_message_caption(
                                chat_id=send_chat_id,
                                message_id=msg_id,
                                caption=deposit_text_clean,
                                parse_mode='HTML',
                                reply_markup=None
                            )
                            logger.info(f"✅ Removed button from deposit address message in room {original_chat_id}")
                        except Exception as e:
                            logger.warning(f"Could not edit deposit address message: {e}")
                    
                    # Record deposit in database
                    database.record_deposit(original_chat_id, tx_hash)
                    
                    # Track confirmed deposit for /balance command (use deal amount for master hash)
                    room_confirmed_deposits[original_chat_id] = amount
                    logger.info(f"💰 Confirmed deposit tracked for room {original_chat_id}: {amount}")
                    
                    # Send payment received message
                    payment_received_text = (
                        "✅ <b>Payment Received!</b>\n\n"
                        "Use /release After Fund Transfer to Seller\n\n"
                        "⚠️ <b>Please note:</b>\n"
                        "• Don't share payment details on private chat\n"
                        "• Please share all deals in group"
                    )
                    await context.bot.send_message(
                        chat_id=send_chat_id,
                        text=payment_received_text,
                        parse_mode='HTML'
                    )
                    logger.info(f"✅ Sent payment received message to room {original_chat_id}")
                    
                    # Clear state
                    room_awaiting_hash.pop(original_chat_id, None)
                    room_transaction_state.pop(original_chat_id, None)
                    return
                
                # Verify transaction based on blockchain
                if blockchain == 'TRON':
                    logger.info(f"🔍 Verifying TRON transaction {tx_hash[:10]}...")
                    verify_result = await verify_transaction_tron(tx_hash, escrow_address, token=coin or 'USDT')
                else:
                    logger.info(f"🔍 Verifying BSC transaction {tx_hash[:10]}...")
                    verify_result = await verify_transaction_bscscan(tx_hash, escrow_address, token=coin or 'USDT')
                
                if verify_result['valid']:
                    # Transaction verified successfully
                    # Use the actual received amount from blockchain, not the deal amount
                    received_amount = verify_result['amount']
                    logger.info(f"✅ Transaction verified! Received amount: {received_amount} {coin or 'USDT'}")
                    
                    # Track confirmed deposit for /balance command
                    room_confirmed_deposits[original_chat_id] = received_amount
                    logger.info(f"💰 Confirmed deposit tracked for room {original_chat_id}: {received_amount}")
                    
                    # Get seller's address that was provided earlier
                    seller_addr = seller_addresses.get(original_chat_id, verify_result['from_address'])
                    
                    await send_deposit_found_message(
                        context.bot,
                        send_chat_id,
                        f"{received_amount}",  # Use actual received amount from blockchain
                        seller_addr,
                        verify_result['to_address'],
                        tx_hash,
                        block_number=verify_result['block_number']
                    )
                    
                    # Update room log message with deposit received status
                    await update_room_log_status(context.bot, original_chat_id, f"Deposit Received [{received_amount}]")
                    
                    # Remove button from deposit address message
                    if original_chat_id in deposit_address_messages:
                        try:
                            msg_id = deposit_address_messages[original_chat_id]
                            # Build deposit text to remove button
                            blockchain = user_blockchain.get(original_chat_id, 'BSC')
                            coin = user_coins.get(original_chat_id, 'USDT')
                            escrow_addr = escrow_address
                            deposit_text_clean = f"""💳 {coin} {blockchain} Deposit\n\n🏦 {coin} {blockchain} Address: <code>{escrow_addr}</code>"""
                            await context.bot.edit_message_caption(
                                chat_id=send_chat_id,
                                message_id=msg_id,
                                caption=deposit_text_clean,
                                parse_mode='HTML',
                                reply_markup=None
                            )
                            logger.info(f"✅ Removed button from deposit address message in room {original_chat_id}")
                        except Exception as e:
                            logger.warning(f"Could not edit deposit address message: {e}")
                    
                    # Record deposit in database (verified transaction)
                    database.record_deposit(original_chat_id, tx_hash)
                    
                    # Send payment received message
                    payment_received_text = (
                        "✅ <b>Payment Received!</b>\n\n"
                        "Use /release After Fund Transfer to Seller\n\n"
                        "⚠️ <b>Please note:</b>\n"
                        "• Don't share payment details on private chat\n"
                        "• Please share all deals in group"
                    )
                    await context.bot.send_message(
                        chat_id=send_chat_id,
                        text=payment_received_text,
                        parse_mode='HTML'
                    )
                    logger.info(f"✅ Sent payment received message to room {original_chat_id}")
                    
                    # Clear state
                    room_awaiting_hash.pop(original_chat_id, None)
                    room_transaction_state.pop(original_chat_id, None)
                else:
                    # Transaction verification failed
                    error_msg = verify_result['error'] or "❌ Transaction verification failed"
                    await context.bot.send_message(
                        chat_id=send_chat_id,
                        text=error_msg
                    )
                    logger.warning(f"❌ Transaction verification failed: {error_msg}")
                
                return
            
            except Exception as e:
                logger.warning(f"❌ Error processing transaction hash: {e}")
                # Send error message to group instead of replying to deleted message
                send_chat_id = -1000000000000 - original_chat_id
                await context.bot.send_message(
                    chat_id=send_chat_id,
                    text=f"❌ Error: {str(e)}"
                )
                return
    
    if step == 'title':
        context.user_data['listing_title'] = text
        context.user_data['step'] = 'description'
        await update.message.reply_text(
            "Now, send me a description for your item:"
        )
        return CREATING_LISTING
    elif step == 'description':
        context.user_data['listing_description'] = text
        context.user_data['step'] = 'price'
        await update.message.reply_text(
            "What's the price? (enter just the number, e.g., 50)"
        )
        return CREATING_LISTING
    elif step == 'price':
        try:
            price = float(text)
            listing_id = f"lst_{len(listings) + 1}"
            listings[listing_id] = {
                'id': listing_id,
                'title': context.user_data['listing_title'],
                'description': context.user_data['listing_description'],
                'price': price,
                'seller_id': user_id,
            }
            
            keyboard = [[InlineKeyboardButton("← Back to Main", callback_data='back')]]
            await update.message.reply_text(
                f"✅ Listing Created!\n\n"
                f"Title: {context.user_data['listing_title']}\n"
                f"Price: ${price}\n\n"
                f"Your listing is now live!",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            context.user_data.clear()
            return CHOOSING
        except ValueError:
            await update.message.reply_text(
                "Please enter a valid price (e.g., 50)"
            )
            return CREATING_LISTING
    return CHOOSING


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors"""
    logger.error(f"❌ Error: {context.error}")


async def handle_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle bot status changes"""
    try:
        if update.my_chat_member:
            chat = update.my_chat_member.chat
            new_status = update.my_chat_member.new_chat_member.status
            user_id = update.my_chat_member.new_chat_member.user.id
            
            # Check if it's the bot joining
            if user_id == context.bot.id:
                # When bot is added to a chat
                if new_status == "member" or new_status == "administrator":
                    # Convert negative chat_id to positive for lookup
                    positive_chat_id = abs(chat.id) - 1000000000000 if chat.id < 0 else chat.id
                    logger.info(f"🤖 Bot joined chat (negative: {chat.id}, positive: {positive_chat_id}) with status {new_status}")
                    # Don't send messages here - let the background task handle it
                    # Messages are already sent by check_new_deal_rooms when room is first created
    except Exception as e:
        logger.warning(f"Error handling bot chat member update: {e}")


async def handle_user_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle when users join deal rooms"""
    try:
        if update.chat_member:
            chat = update.chat_member.chat
            old_status = update.chat_member.old_chat_member.status
            new_status = update.chat_member.new_chat_member.status
            user = update.chat_member.new_chat_member.user
            username = user.username
            user_id = user.id
            
            # Convert negative chat_id to positive for lookup
            positive_chat_id = abs(chat.id) - 1000000000000 if chat.id < 0 else chat.id
            
            logger.info(f"👤 User @{username} (ID: {user_id}) status changed in chat (positive: {positive_chat_id}): {old_status} -> {new_status}")
            
            # A user joined the chat
            if new_status == "member" and old_status != "member":
                logger.info(f"✅ @{username} joined chat {positive_chat_id}")
                
                # Track user ID for username
                if username:
                    save_user_id(username, user_id)
    except Exception as e:
        logger.warning(f"Error handling user chat member update: {e}")


async def handle_chat_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle join requests to deal rooms - FAST PROCESSING"""
    try:
        if update.chat_join_request:
            chat_id = update.chat_join_request.chat.id
            user_id = update.chat_join_request.from_user.id
            username = update.chat_join_request.from_user.username
            
            # Convert negative chat_id to positive for lookup
            positive_chat_id = abs(chat_id) - 1000000000000 if chat_id < 0 else chat_id
            
            # Quick check if we're waiting for requests on this room
            if positive_chat_id in rooms_waiting_for_requests:
                logger.info(f"⚡ FAST: Join request from @{username} to ACTIVE room {positive_chat_id}")
            
            logger.info(f"📨 Join request received from @{username} (ID: {user_id}) to chat (positive: {positive_chat_id})")
            
            # Track user ID for username
            if username:
                save_user_id(username, user_id)
            
            if not os.path.exists(DEAL_ROOMS_FILE):
                logger.warning(f"deal_rooms.json not found")
                return
            
            with open(DEAL_ROOMS_FILE, 'r') as f:
                deal_rooms = json.load(f)
            
            room_info = deal_rooms.get(str(positive_chat_id))
            if not room_info:
                logger.warning(f"No room info for chat {positive_chat_id}")
                return
            
            initiator_username = room_info.get('initiator_username', '')
            counterparty_username = room_info.get('counterparty_username', '')
            counterparty_user_id = room_info.get('counterparty_user_id')
            room_name = room_info.get('room_name', '')
            
            logger.info(f"🔎 Checking join request for SPECIFIC ROOM: {room_name}")
            if counterparty_user_id:
                logger.info(f"   This room's authorized users: Initiator: @{initiator_username}, Counterparty: User {counterparty_user_id}")
            else:
                logger.info(f"   This room's authorized users: Initiator: @{initiator_username}, Counterparty: @{counterparty_username}")
            logger.info(f"   Requesting user: @{username} (ID: {user_id})")
            
            # Only approve if user is the initiator OR counterparty FOR THIS SPECIFIC ROOM
            # Check by username (case-insensitive) or by user ID
            is_room_initiator = False
            is_room_counterparty = False
            
            if username and initiator_username:
                is_room_initiator = (username.lower() == initiator_username.lower())
            
            # Check counterparty by user ID first, then by username
            if counterparty_user_id:
                is_room_counterparty = (user_id == counterparty_user_id)
            elif username and counterparty_username:
                is_room_counterparty = (username.lower() == counterparty_username.lower())
            
            if is_room_initiator:
                logger.info(f"✅ @{username} is the INITIATOR for THIS room: {room_name}")
            elif is_room_counterparty:
                if counterparty_user_id:
                    logger.info(f"✅ User {user_id} is the COUNTERPARTY for THIS room: {room_name}")
                else:
                    logger.info(f"✅ @{username} is the COUNTERPARTY for THIS room: {room_name}")
            else:
                logger.warning(f"❌ @{username} (ID: {user_id}) is NOT authorized for THIS room: {room_name} (not initiator or counterparty)")
            
            # Only approve if user is initiator or counterparty FOR THIS SPECIFIC ROOM
            if is_room_initiator or is_room_counterparty:
                try:
                    logger.info(f"🔐 Attempting to approve join request - chat_id: {chat_id}, user_id: {user_id}, username: @{username}")
                    await context.bot.approve_chat_join_request(chat_id, user_id)
                    logger.info(f"✅ INSTANT APPROVED join request from @{username} to {room_name}")
                    
                    # Keep room in waiting list until BOTH users have actually joined
                    rooms_waiting_for_requests.add(positive_chat_id)
                    logger.info(f"🔔 Room {positive_chat_id} still waiting for join completions")
                    
                except Exception as approve_error:
                    error_str = str(approve_error)
                    logger.warning(f"❌ Error approving join request: {error_str}")
                    if "User_already_participant" in error_str:
                        logger.info(f"ℹ️ @{username} is already a participant in {room_name}, skipping approve")
                    elif "ChatAdminRequired" in error_str or "NotEnoughRightsToRestrict" in error_str:
                        logger.error(f"❌ Bot missing admin rights to approve join requests in {room_name}")
                    else:
                        logger.warning(f"❌ Failed to approve join request: {approve_error}")
                
                # Update message with NO delay - they're joining NOW
                send_chat_id = -1000000000000 - positive_chat_id
                # Schedule the status update as a background task (don't wait for it)
                context.application.create_task(update_room_join_status(context.bot, send_chat_id, username))
            else:
                try:
                    await context.bot.decline_chat_join_request(chat_id, user_id)
                    logger.info(f"❌ Declined join request from @{username} (not authorized for {room_name})")
                except Exception as e:
                    logger.warning(f"Could not decline join request: {e}")
    except Exception as e:
        logger.error(f"Error handling join request: {e}", exc_info=True)


async def update_room_join_status(bot, send_chat_id: int, username: str) -> None:
    """Update the waiting message when user joins - NEW VERSION using send_chat_id"""
    try:
        logger.info(f"🔍 Starting update_room_join_status for @{username}, send_chat_id: {send_chat_id}")
        
        if not os.path.exists(DEAL_ROOMS_FILE):
            logger.warning(f"DEAL_ROOMS_FILE not found")
            return
        
        with open(DEAL_ROOMS_FILE, 'r') as f:
            deal_rooms = json.load(f)
        
        logger.info(f"📂 Looking through {len(deal_rooms)} rooms in deal_rooms.json")
        
        # Find the room by send_chat_id
        original_chat_id = None
        room_info = None
        
        for chat_id_str, info in deal_rooms.items():
            if int(chat_id_str) > 0:
                test_send_id = -1000000000000 - int(chat_id_str)
                logger.info(f"Testing: {chat_id_str} -> {test_send_id} vs {send_chat_id}")
                if test_send_id == send_chat_id:
                    original_chat_id = int(chat_id_str)
                    room_info = info
                    logger.info(f"✅ Found matching room: {info.get('room_name')}")
                    break
        
        if not room_info:
            logger.warning(f"❌ Could not find room for send_chat_id {send_chat_id}. Available: {[(k, -1000000000000 - int(k)) for k in deal_rooms.keys() if int(k) > 0]}")
            return
        
        room_name = room_info.get('room_name', '')
        initiator_username = room_info.get('initiator_username', '')
        counterparty_username = room_info.get('counterparty_username', '')
        counterparty_user_id = room_info.get('counterparty_user_id')
        
        if counterparty_user_id:
            logger.info(f"Room found: {room_name}, initiator: @{initiator_username}, counterparty: User {counterparty_user_id}")
        else:
            logger.info(f"Room found: {room_name}, initiator: @{initiator_username}, counterparty: @{counterparty_username}")
        logger.info(f"Stored rooms in memory: {list(room_messages.keys())}")
        
        # Get stored message IDs for this room
        if str(original_chat_id) not in room_messages:
            logger.warning(f"❌ No message info stored for original chat {original_chat_id}. Available in memory: {list(room_messages.keys())}")
            return
        
        msg_info = room_messages[str(original_chat_id)]
        
        logger.info(f"Updating join status for @{username} in {room_name}")
        logger.info(f"Available message IDs: {msg_info}")
        
        # Delete waiting message and send new joined message for initiator (case-insensitive)
        if username.lower() == initiator_username.lower():
            logger.info(f"Checking initiator message for @{username} == @{initiator_username}")
            if 'initiator_msg_id' in msg_info:
                try:
                    # Delete the waiting message
                    logger.info(f"Deleting waiting message {msg_info['initiator_msg_id']} in chat {send_chat_id}")
                    await bot.delete_message(
                        chat_id=send_chat_id,
                        message_id=msg_info['initiator_msg_id']
                    )
                    logger.info(f"✅ Deleted initiator waiting message in {room_name}")
                    
                    # Send new joined message
                    new_msg = await bot.send_message(
                        chat_id=send_chat_id,
                        text=f"✅ @{initiator_username} joined."
                    )
                    logger.info(f"✅ Sent new joined message for initiator in {room_name}")
                    # Update stored message ID
                    msg_info['initiator_msg_id'] = new_msg.message_id
                except Exception as e:
                    logger.warning(f"❌ Could not update initiator message: {e}")
            else:
                logger.info(f"ℹ️  Initiator message wasn't stored (may have failed to send). Continuing with @{username}")
        else:
            logger.info(f"Username @{username} != initiator @{initiator_username}")
        
        # Delete waiting message and send new joined message for counterparty (case-insensitive or by user_id)
        # Check if this user is the counterparty - by username or by user_id
        is_counterparty = False
        if counterparty_user_id:
            # When counterparty is identified by user_id, we need to get the joining user's ID
            # For now, check if username matches (user might have a username even if identified by ID)
            if counterparty_username and username and username.lower() == counterparty_username.lower():
                is_counterparty = True
            # Also mark as counterparty if no username match but this is the expected user
            # (The handle_chat_join_request already verified by user_id)
            elif not counterparty_username or counterparty_username == '':
                is_counterparty = True  # Trust that handle_chat_join_request verified correctly
        elif counterparty_username and username:
            is_counterparty = (username.lower() == counterparty_username.lower())
        
        if is_counterparty:
            if counterparty_user_id:
                logger.info(f"Checking counterparty message for User {counterparty_user_id}")
            else:
                logger.info(f"Checking counterparty message for @{username} == @{counterparty_username}")
            if 'counterparty_msg_id' in msg_info:
                try:
                    # Delete the waiting message
                    logger.info(f"Deleting waiting message {msg_info['counterparty_msg_id']} in chat {send_chat_id}")
                    await bot.delete_message(
                        chat_id=send_chat_id,
                        message_id=msg_info['counterparty_msg_id']
                    )
                    logger.info(f"✅ Deleted counterparty waiting message in {room_name}")
                    
                    # Send new joined message - use hyperlink for user ID, @username otherwise
                    if counterparty_user_id:
                        counterparty_display = f"<a href=\"tg://user?id={counterparty_user_id}\">User {counterparty_user_id}</a>"
                    else:
                        counterparty_display = f"@{counterparty_username}"
                    
                    new_msg = await bot.send_message(
                        chat_id=send_chat_id,
                        text=f"✅ {counterparty_display} joined.",
                        parse_mode='HTML'
                    )
                    logger.info(f"✅ Sent new joined message for counterparty in {room_name}")
                    # Update stored message ID
                    msg_info['counterparty_msg_id'] = new_msg.message_id
                except Exception as e:
                    logger.warning(f"❌ Could not update counterparty message: {e}")
            else:
                logger.info(f"ℹ️  Counterparty message wasn't stored (may have failed to send). Continuing with counterparty")
        else:
            if counterparty_user_id:
                logger.info(f"User @{username} is not the counterparty (User {counterparty_user_id})")
            else:
                logger.info(f"Username @{username} != counterparty @{counterparty_username}")
        
        # Track joined users
        if original_chat_id not in room_joined_users:
            room_joined_users[original_chat_id] = set()
        
        room_joined_users[original_chat_id].add(username.lower())
        joined_count = len(room_joined_users[original_chat_id])
        logger.info(f"📊 Room {room_name}: {joined_count}/2 users joined - {room_joined_users[original_chat_id]}")
        
        # Check if both users have joined
        if (joined_count == 2 and 
            original_chat_id not in disclaimer_sent and
            initiator_username.lower() in room_joined_users[original_chat_id] and
            counterparty_username.lower() in room_joined_users[original_chat_id]):
            
            logger.info(f"🎯 Both users joined in {room_name}! Sending disclaimer message...")
            
            # Mark as sent BEFORE sending to prevent race conditions
            disclaimer_sent.add(original_chat_id)
            
            # Remove from waiting list since both have now joined
            if original_chat_id in rooms_waiting_for_requests:
                rooms_waiting_for_requests.discard(original_chat_id)
                logger.info(f"✅ Removed {original_chat_id} from waiting list - both users joined!")
            
            # Replace the original deal created message to show trade started (delete old, send text only)
            if str(original_chat_id) in room_messages:
                msg_info = room_messages[str(original_chat_id)]
                deal_msg_id = msg_info.get('deal_created_msg_id')
                deal_chat_id = msg_info.get('deal_created_chat_id')
                
                if deal_msg_id and deal_chat_id:
                    try:
                        # Delete the old message
                        await bot.delete_message(
                            chat_id=deal_chat_id,
                            message_id=deal_msg_id
                        )
                        logger.info(f"✅ Deleted old deal created message {deal_msg_id} in chat {deal_chat_id}")
                        
                        # Send new text-only message with trade started caption
                        # Format counterparty display - use hyperlink for user ID, @username otherwise
                        if counterparty_user_id:
                            counterparty_trade_display = f"<a href=\"tg://user?id={counterparty_user_id}\">User {counterparty_user_id}</a>"
                        else:
                            counterparty_trade_display = f"@{counterparty_username}"
                        trade_started_text = f"✅ <b>Trade started between @{initiator_username} and {counterparty_trade_display}.</b>"
                        
                        new_msg = await bot.send_message(
                            chat_id=deal_chat_id,
                            text=trade_started_text,
                            parse_mode='HTML'
                        )
                        logger.info(f"✅ Sent trade started message (text only) to chat {deal_chat_id}")
                        
                        # Update stored message ID
                        room_messages[str(original_chat_id)]['deal_created_msg_id'] = new_msg.message_id
                        
                    except Exception as e:
                        logger.warning(f"Could not replace deal created message: {e}")
            
            await send_disclaimer_message(bot, send_chat_id, room_name, original_chat_id)
    
    except Exception as e:
        logger.warning(f"❌ Error updating room join status: {e}", exc_info=True)


async def send_disclaimer_message(bot, send_chat_id: int, room_name: str, original_chat_id: int) -> None:
    """Send the deal disclaimer message with image when both users join"""
    try:
        disclaimer_text = (
            "⚠️ P2P Deal Disclaimer ⚠️\n\n"
            "• Always verify the admin wallet before sending any funds.\n"
            "• Confirm <code>@pool</code> is present in both the deal room & the main group.\n"
            "• ❌ Never engage in direct or outside-room deals.\n"
            "• 💬 Share all details only within this deal room."
        )
        
        # Check if image exists
        image_path = "disclaimer_image.jpg"
        if os.path.exists(image_path):
            await bot.send_photo(
                chat_id=send_chat_id,
                photo=open(image_path, 'rb'),
                caption=disclaimer_text,
                parse_mode='HTML'
            )
            logger.info(f"✅ Sent disclaimer message with image to {room_name}")
        else:
            # Fallback: send text-only message
            await bot.send_message(
                chat_id=send_chat_id,
                text=disclaimer_text,
                parse_mode='HTML'
            )
            logger.warning(f"⚠️ Sent disclaimer (text only) to {room_name} - image not found")
        
        # Send role selection message after disclaimer
        await asyncio.sleep(0.1)  # Minimal delay to ensure messages appear in order
        await send_role_selection_message(bot, send_chat_id, room_name, original_chat_id)
    
    except Exception as e:
        logger.warning(f"❌ Failed to send disclaimer message: {e}")


async def send_role_selection_message(bot, send_chat_id: int, room_name: str, original_chat_id: int) -> None:
    """Send role selection message with buttons"""
    try:
        # Prevent sending duplicate role selection messages
        if original_chat_id in role_selection_sent:
            logger.info(f"⏭️ Role selection already sent to {room_name}, skipping duplicate")
            return
        
        # Mark as sent EARLY to prevent race conditions
        role_selection_sent.add(original_chat_id)
        
        if original_chat_id not in room_joined_users:
            logger.warning(f"No room data for role selection in {room_name}")
            return
        
        # Get usernames
        joined_usernames = list(room_joined_users[original_chat_id])
        if len(joined_usernames) < 2:
            logger.warning(f"Not enough users for role selection in {room_name}")
            return
        
        initiator_username = joined_usernames[0]
        counterparty_username = joined_usernames[1]
        
        # Get from room data to get proper casing and counterparty_user_id
        counterparty_user_id = None
        if os.path.exists(DEAL_ROOMS_FILE):
            with open(DEAL_ROOMS_FILE, 'r') as f:
                deal_rooms = json.load(f)
            room_info = deal_rooms.get(str(original_chat_id))
            if room_info:
                initiator_username = room_info.get('initiator_username', initiator_username)
                counterparty_username = room_info.get('counterparty_username', counterparty_username)
                counterparty_user_id = room_info.get('counterparty_user_id')
        
        # Format counterparty display - use hyperlink for user ID, @username otherwise
        if counterparty_user_id:
            counterparty_display_name = f"<a href=\"tg://user?id={counterparty_user_id}\">User {counterparty_user_id}</a>"
        else:
            counterparty_display_name = f"@{counterparty_username}"
        
        # Initialize roles tracking
        if original_chat_id not in user_roles:
            user_roles[original_chat_id] = {}
        
        role_text = (
            "<b>📋 Step 1 - Select Roles</b>\n\n"
            "<b>⚠️ Choose roles accordingly</b>\n\n"
            "<b>As release & refund happen according to roles</b>\n\n"
            "<b>Refund goes to seller & release to buyer</b>\n\n"
            f"⏳ @{initiator_username} - Waiting...\n"
            f"⏳ {counterparty_display_name} - Waiting..."
        )
        
        # Create buttons side by side
        keyboard = [
            [
                InlineKeyboardButton("💰 I am Buyer", callback_data=f"role_buyer_{original_chat_id}_{initiator_username.lower()}"),
                InlineKeyboardButton("💵 I am Seller", callback_data=f"role_seller_{original_chat_id}_{initiator_username.lower()}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Send with image
        image_path = "role_selection_image.jpg"
        if os.path.exists(image_path):
            msg = await bot.send_photo(
                chat_id=send_chat_id,
                photo=open(image_path, 'rb'),
                caption=role_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            role_messages[original_chat_id] = (msg.message_id, send_chat_id, initiator_username, counterparty_username, counterparty_user_id)
            logger.info(f"✅ Sent role selection message to {room_name} (message ID: {msg.message_id})")
        else:
            msg = await bot.send_message(
                chat_id=send_chat_id,
                text=role_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            role_messages[original_chat_id] = (msg.message_id, send_chat_id, initiator_username, counterparty_username, counterparty_user_id)
            logger.warning(f"⚠️ Sent role selection (text only) to {room_name} - image not found")
    
    except Exception as e:
        logger.warning(f"❌ Failed to send role selection message: {e}")


async def send_room_waiting_messages(application: Application, chat_id: int) -> None:
    """Send waiting messages when bot joins the room"""
    try:
        logger.info(f"📋 Attempting to send waiting messages to chat {chat_id}")
        
        # Wait for bot to fully join the room
        await asyncio.sleep(0.2)  # Minimal delay for faster message detection
        
        if not os.path.exists(DEAL_ROOMS_FILE):
            logger.warning(f"deal_rooms.json not found")
            return
        
        with open(DEAL_ROOMS_FILE, 'r') as f:
            deal_rooms = json.load(f)
        
        room_info = deal_rooms.get(str(chat_id))
        if not room_info:
            logger.warning(f"Room info not found for chat_id {chat_id}. Available: {list(deal_rooms.keys())}")
            return
        
        initiator_username = room_info.get('initiator_username', '')
        counterparty_username = room_info.get('counterparty_username', '')
        counterparty_user_id = room_info.get('counterparty_user_id')
        room_name = room_info.get('room_name', '')
        
        if counterparty_user_id:
            logger.info(f"Room info found: {room_name} - initiator: @{initiator_username}, counterparty: User {counterparty_user_id}")
        else:
            logger.info(f"Room info found: {room_name} - initiator: @{initiator_username}, counterparty: @{counterparty_username}")
        
        # Track room creation time for time calculation later
        if chat_id not in room_creation_times:
            room_creation_times[chat_id] = time.time()
            logger.info(f"⏱️ Room creation time tracked for {room_name}")
        
        # Create deal record in database
        database.create_deal(
            chat_id=chat_id,
            room_name=room_name
        )
        logger.info(f"📊 Deal record created in database for {room_name}")
        
        # Send waiting messages
        try:
            # For supergroups, use the most reliable format first
            chat_ids_to_try = [-1000000000000 - chat_id, -chat_id, chat_id]
            
            msg1 = None
            msg2 = None
            successful_chat_id = None
            
            # First, find which chat_id works by sending initiator message
            for try_id in chat_ids_to_try:
                try:
                    msg1 = await application.bot.send_message(
                        chat_id=try_id,
                        text=f"⏳ Waiting for @{initiator_username} to join…"
                    )
                    logger.info(f"✅ Sent initiator waiting message with chat_id {try_id} (ID: {msg1.message_id})")
                    successful_chat_id = try_id
                    break
                except Exception as e:
                    logger.warning(f"Failed to send initiator message to {try_id}: {str(e)[:50]}")
                    await asyncio.sleep(0.2)  # Delay before retry
                    continue
            
            if not msg1:
                logger.warning(f"❌ Could not send initiator message to any chat_id variation")
                # Continue anyway and try to send counterparty message
            else:
                logger.info(f"✅ Found working chat_id: {successful_chat_id}")
            
            # Send counterparty message - with delay and to the same working chat_id
            await asyncio.sleep(0.1)  # Minimal delay between messages
            
            # Format counterparty display - use hyperlink for user ID, @username otherwise
            if counterparty_user_id:
                counterparty_display = f"<a href=\"tg://user?id={counterparty_user_id}\">User {counterparty_user_id}</a>"
            else:
                counterparty_display = f"@{counterparty_username}"
            
            if successful_chat_id:
                try:
                    msg2 = await application.bot.send_message(
                        chat_id=successful_chat_id,
                        text=f"⏳ Waiting for {counterparty_display} to join…",
                        parse_mode='HTML'
                    )
                    logger.info(f"✅ Sent counterparty waiting message with chat_id {successful_chat_id} (ID: {msg2.message_id})")
                except Exception as e:
                    logger.warning(f"❌ Failed to send counterparty message to {successful_chat_id}: {str(e)[:50]}")
                    msg2 = None
            
            # If no successful chat_id yet, try all for counterparty message
            if not successful_chat_id and not msg2:
                logger.warning(f"Trying all chat_ids for counterparty message...")
                for try_id in chat_ids_to_try:
                    try:
                        msg2 = await application.bot.send_message(
                            chat_id=try_id,
                            text=f"⏳ Waiting for {counterparty_display} to join…",
                            parse_mode='HTML'
                        )
                        logger.info(f"✅ Sent counterparty message to {try_id} (ID: {msg2.message_id})")
                        successful_chat_id = try_id
                        break
                    except Exception as e:
                        logger.warning(f"Failed counterparty to {try_id}: {str(e)[:50]}")
                        await asyncio.sleep(0.2)
                        continue
            
            if not msg2:
                logger.warning(f"Could not send counterparty message to any chat_id variation")
            
            # Store message IDs for later updates
            if str(chat_id) not in room_messages:
                room_messages[str(chat_id)] = {}
            
            if msg1:
                room_messages[str(chat_id)]['initiator_msg_id'] = msg1.message_id
                logger.info(f"Stored initiator message ID: {msg1.message_id}")
            
            if msg2:
                room_messages[str(chat_id)]['counterparty_msg_id'] = msg2.message_id
                logger.info(f"Stored counterparty message ID: {msg2.message_id}")
            
            if msg1 or msg2:
                logger.info(f"✅ Waiting messages sent to {room_name}")
                # Mark room as waiting for join requests
                rooms_waiting_for_requests.add(chat_id)
                logger.info(f"🔔 Room {room_name} is now ACTIVELY LISTENING for join requests 👂")
                
            else:
                logger.warning(f"❌ Failed to send any messages to {room_name}")
        except Exception as e:
            logger.warning(f"Could not send waiting messages: {e}")
    except Exception as e:
        logger.warning(f"Error sending room waiting messages: {e}")


async def check_new_deal_rooms(application: Application) -> None:
    """Periodically check for new deal rooms and send waiting messages"""
    while True:
        try:
            await asyncio.sleep(0.5)  # Check every 500ms for faster room detection
            
            if not os.path.exists(DEAL_ROOMS_FILE):
                continue
            
            with open(DEAL_ROOMS_FILE, 'r') as f:
                deal_rooms_data = json.load(f)
            
            for chat_id_str, room_info in deal_rooms_data.items():
                chat_id = int(chat_id_str)
                
                # Send messages if not already processed
                if chat_id not in processed_rooms:
                    try:
                        await send_room_waiting_messages(application, chat_id)
                        processed_rooms.add(chat_id)
                    except Exception as e:
                        logger.warning(f"Error checking room {chat_id}: {e}")
        except Exception as e:
            logger.warning(f"Error in check_new_deal_rooms: {e}")


async def auto_close_expired_deals(application: Application) -> None:
    """Periodically check for deals running more than 12 hours and auto-close them"""
    while True:
        try:
            await asyncio.sleep(300)  # Check every 5 minutes
            
            expired_deals = database.get_expired_deals(hours=12)
            
            for deal in expired_deals:
                chat_id = deal['chat_id']
                room_name = deal.get('room_name', 'Unknown Room')
                
                try:
                    database.expire_deal(chat_id)
                    logger.info(f"⏰ Auto-closed expired deal in {room_name} (chat_id: {chat_id})")
                    
                    send_chat_id = -1000000000000 - chat_id
                    
                    expired_message = """⏰ <b>Deal Auto-Closed</b>

This deal has been automatically closed because it was running for more than 12 hours without completion.

If you need to continue this transaction, please start a new deal using /deal command."""
                    
                    try:
                        await application.bot.send_message(
                            chat_id=send_chat_id,
                            text=expired_message,
                            parse_mode='HTML'
                        )
                        logger.info(f"📨 Sent auto-close notification to room {chat_id}")
                    except Exception as e:
                        logger.warning(f"Could not send auto-close notification to room {chat_id}: {e}")
                    
                    disclaimer_sent.discard(chat_id)
                    role_selection_sent.discard(chat_id)
                    processed_rooms.discard(chat_id)
                    rooms_waiting_for_requests.discard(chat_id)
                    
                    if chat_id in room_awaiting_hash:
                        del room_awaiting_hash[chat_id]
                    if chat_id in room_transaction_state:
                        del room_transaction_state[chat_id]
                    if chat_id in user_roles:
                        del user_roles[chat_id]
                    if chat_id in approvals:
                        del approvals[chat_id]
                    if chat_id in release_approvals:
                        del release_approvals[chat_id]
                    
                    # Request userbot to delete the group
                    write_delete_request(chat_id, room_name)
                    logger.info(f"🗑️ Requested deletion of group {room_name} (chat_id: {chat_id})")
                    
                except Exception as e:
                    logger.warning(f"Error auto-closing deal {chat_id}: {e}")
                    
        except Exception as e:
            logger.warning(f"Error in auto_close_expired_deals: {e}")


def main() -> None:
    """Start the bot"""
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    
    if not token:
        logger.error(
            "TELEGRAM_BOT_TOKEN not set. Please set it as an environment variable."
        )
        return
    
    # Create the Application
    application = Application.builder().token(token).build()
    
    # Add handlers
    application.add_handler(CommandHandler("deal", deal_command))
    application.add_handler(CommandHandler("release", release_command))
    application.add_handler(CommandHandler("kick", kick_command))
    application.add_handler(CommandHandler("link", link_command))
    application.add_handler(CommandHandler("restart", restart_command))
    application.add_handler(CommandHandler("wallet", wallet_command))
    application.add_handler(CommandHandler("setownerwallet", setownerwallet_command))
    application.add_handler(CommandHandler("setceowallet", setceowallet_command))
    application.add_handler(CommandHandler("wallets", wallets_command))
    application.add_handler(CommandHandler("balance", balance_command))
    application.add_handler(CommandHandler("verify", verify_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(ChatJoinRequestHandler(handle_chat_join_request))
    application.add_handler(ChatMemberHandler(handle_chat_member_update))
    application.add_handler(ChatMemberHandler(handle_user_chat_member_update))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    
    # Add error handler
    application.add_error_handler(error_handler)
    
    # Initialize deals database table
    database.init_database()
    
    # Initialize owner wallet settings table and load from database
    init_owner_wallet_table()
    load_owner_wallets()
    
    # Initialize CEO wallet settings table and load from database
    init_ceo_wallet_table()
    load_ceo_wallets()
    
    # Load persistent data from database
    load_room_data()
    load_fee_setting()
    
    # Load user wallets from database
    load_wallets_from_database()
    
    # Mark existing rooms as processed before starting
    mark_existing_rooms_processed()
    
    # Create a task to check for new deal rooms periodically
    async def start_background_tasks(app):
        """Start background tasks after app is initialized"""
        # Explicitly delete any existing webhook to prevent getUpdates conflicts
        await app.bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook deleted, starting clean polling session")
        # Create the task only after app is running
        app.create_task(check_new_deal_rooms(app), update=None)
        app.create_task(auto_close_expired_deals(app), update=None)
        logger.info("✅ Started background tasks: check_new_deal_rooms, auto_close_expired_deals")
    
    # Schedule the background task to start after the bot is initialized
    application.post_init = start_background_tasks
    
    # Start the bot with polling - use reasonable timeout to avoid overlapping getUpdates requests
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,  # Clear any pending messages on startup
        poll_interval=0.5,    # Small delay between polls to prevent request overlap
        timeout=10,           # Standard long-polling timeout (prevents rapid-fire requests)
        read_timeout=15,      # Socket read timeout
        write_timeout=15,     # Socket write timeout
        connect_timeout=10    # Connection timeout
    )
    logger.info("✅ Telegram Bot Started - Ready for commands")


if __name__ == '__main__':
    main()
