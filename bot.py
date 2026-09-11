#!/usr/bin/env python3
"""
P2PMART Telegram Escrow Bot
A simple peer-to-peer marketplace with escrow functionality
"""

import os
import html
import logging
import json
import asyncio
import re
import time
import requests
import psycopg2
import warnings
import secrets
from datetime import datetime
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

# Telethon is only needed to log a backup userbot account in from /newubot.
try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.errors import SessionPasswordNeededError
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False
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
# Premade room pool files (see /startroom)
PREWARM_QUEUE_FILE = "prewarm_requests.json"
ROOM_POOL_FILE = "room_pool.json"
RELEASE_QUEUE_FILE = "release_requests.json"

# Only this id can pre-create the room pool with /startroom
CEO_USER_ID = 6643621069

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
room_used_tx_hashes = {}  # Transaction hashes already credited per room: {chat_id: set(hash.lower())}
room_log_messages = {}  # Track room log message IDs: {chat_id: {'msg_id': int, 'chat_id': int}}
room_joined_users = {}  # Participants that joined a room: {chat_id: set(username/user id)}
added_member_log_messages = {}  # Track "added by admin" log messages: {(chat_id, user_id): {'msg_id': int, 'text': str}}
master_hash = "0x6f83337833118197454614dGe9168365dd3c85232dadb6bbd97f4e240eb5c7dd9"  # Master hash - skip verification
current_fee_percent = 0.0  # Global service fee (set via !setfees command, default 0%)
NETWORK_FEE_BSC = 0.5  # Flat network fee on BSC, in the deal's token (USDT/USDC)

# Admin user IDs who can use admin commands like /setownerwallet
ADMIN_USER_IDS = {
    6864194951, 7338429782, 6643621069, 7629970378, 7300655160,
    7244135096,  # @peakybiinder89
    8117659015,  # @asknigge
    6564907309,  # @xdekku
}

# Premium (custom) emoji ids provided by the owner. Rendered via the HTML
# <tg-emoji> tag; clients without access to the emoji show the fallback char.
PREMIUM_EMOJI_STAR = "5395444784611480792"
PREMIUM_EMOJI_USER = "6300827421071378088"

# The P2P ROOM group whose member add/leave events are tracked (/list) and logged.
P2P_ROOM_GROUP_ID = -1004489418013

# Valid room numbers (mirrors the userbot's range) for /room <number> requests.
ROOM_NUMBER_MIN = 1
ROOM_NUMBER_MAX = 20


def premium_emoji(emoji_id: str, fallback: str) -> str:
    """Wrap a fallback emoji in a Telegram custom-emoji tag (HTML parse mode)."""
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'

# Default owner wallet address for escrow deposits
DEFAULT_OWNER_WALLET_BSC = "0xf282e789e835ed379aea84ece204d2d643e6774f"
DEFAULT_OWNER_WALLET_TRON = "T0000000000000000000000000000000000"  # Placeholder for TRON

# Default CEO wallet address for escrow deposits
DEFAULT_CEO_WALLET_BSC = "0xa3D0e7da537057cbeC62A48235FbEc8BB38B4E08"
DEFAULT_CEO_WALLET_TRON = "TDAyZ8PB1MnFXPywHDgrHwa3zkwwXB3WDR"

# In-memory cache for owner wallet (loaded from DB on startup), keyed by (network, token)
owner_wallet_cache = {
    ('BSC', 'USDT'): DEFAULT_OWNER_WALLET_BSC,
    ('BSC', 'USDC'): DEFAULT_OWNER_WALLET_BSC,
    ('TRON', 'USDT'): DEFAULT_OWNER_WALLET_TRON,
}

# In-memory cache for CEO wallet (loaded from DB on startup), keyed by (network, token)
ceo_wallet_cache = {
    ('BSC', 'USDT'): DEFAULT_CEO_WALLET_BSC,
    ('BSC', 'USDC'): DEFAULT_CEO_WALLET_BSC,
    ('TRON', 'USDT'): DEFAULT_CEO_WALLET_TRON,
}

# Pending wallet-set requests awaiting a token choice: {user_id: {'role','network','address'}}
pending_wallet_set = {}

# Pending /setfakeaddy requests awaiting an address: {user_id: {'room_number','token'}}
pending_fake_addy = {}

# Backup userbot logins in progress from /newubot:
# {user_id: {'client','label','api_id','api_hash','phone','code_hash','step'}}
pending_userbot_login = {}

# Wallet rotation index (alternates between owner and CEO wallet)
wallet_rotation_index = 0

def get_rotating_deposit_wallet(network: str, token: str = 'USDT') -> str:
    """Get deposit wallet address with rotation between owner and CEO wallets"""
    global wallet_rotation_index
    wallet_rotation_index = (wallet_rotation_index + 1) % 2
    if wallet_rotation_index == 0:
        return get_owner_wallet(network, token)
    else:
        return get_ceo_wallet(network, token)


def get_deposit_wallet_for_room(chat_id: int, network: str, token: str = 'USDT') -> str:
    """Resolve the deposit address for a room. If an admin fixed the room to the
    owner or CEO wallet via /setaddy, use that role's address for the selected
    network+token; otherwise fall back to the normal owner/CEO rotation."""
    try:
        deal = database.get_deal(chat_id)
    except Exception:
        deal = None
    role = (deal.get('fixed_wallet_role') if deal else None)
    if role == 'owner':
        return get_owner_wallet(network, token)
    if role == 'ceo':
        return get_ceo_wallet(network, token)
    if role == 'backup':
        room_number = deal.get('room_number') if deal else None
        if room_number is not None:
            backup = database.get_room_backup_wallet(int(room_number), token)
            if backup:
                return backup
    return get_rotating_deposit_wallet(network, token)

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

def _migrate_wallet_table(cur, table: str):
    """Add per-token support to a wallet settings table (network, token) unique."""
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            network VARCHAR(10),
            wallet_address TEXT NOT NULL,
            token VARCHAR(10) DEFAULT 'USDT',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by BIGINT
        )
    """)
    cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS token VARCHAR(10) DEFAULT 'USDT'")
    cur.execute(f"UPDATE {table} SET token = 'USDT' WHERE token IS NULL")
    # Drop any old single-column primary key so (network, token) rows can coexist
    cur.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {table}_pkey")
    cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {table}_net_token ON {table} (network, token)")

def init_owner_wallet_table():
    """Initialize the owner_wallet_settings table"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        _migrate_wallet_table(cur, "owner_wallet_settings")
        conn.commit()
        cur.close()
        conn.close()
        logger.info("✅ Owner wallet settings table initialized")
    except Exception as e:
        logger.warning(f"Could not initialize owner wallet table: {e}")

def save_owner_wallet(network: str, wallet_address: str, token: str = 'USDT', updated_by: int = None):
    """Save owner wallet address (per network+token) to database"""
    global owner_wallet_cache
    try:
        conn = get_db_connection()
        if not conn:
            owner_wallet_cache[(network, token)] = wallet_address
            return True
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO owner_wallet_settings (network, token, wallet_address, updated_at, updated_by)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s)
            ON CONFLICT (network, token) DO UPDATE SET 
                wallet_address = %s,
                updated_at = CURRENT_TIMESTAMP,
                updated_by = %s
        """, (network, token, wallet_address, updated_by, wallet_address, updated_by))
        conn.commit()
        cur.close()
        conn.close()
        owner_wallet_cache[(network, token)] = wallet_address
        logger.info(f"✅ Saved owner wallet for {network}/{token}: {wallet_address}")
        return True
    except Exception as e:
        logger.warning(f"Could not save owner wallet: {e}")
        owner_wallet_cache[(network, token)] = wallet_address
        return False

def load_owner_wallets():
    """Load owner wallet addresses from database into cache"""
    global owner_wallet_cache
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("SELECT network, token, wallet_address FROM owner_wallet_settings")
        rows = cur.fetchall()
        for row in rows:
            token = row[1] or 'USDT'
            owner_wallet_cache[(row[0], token)] = row[2]
            logger.info(f"📋 Loaded owner wallet for {row[0]}/{token}: {row[2]}")
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not load owner wallets: {e}")

def get_owner_wallet(network: str, token: str = 'USDT') -> str:
    """Get owner wallet address for a network + token (falls back to USDT / defaults)"""
    addr = owner_wallet_cache.get((network, token)) or owner_wallet_cache.get((network, 'USDT'))
    if addr:
        return addr
    if network == 'TRON':
        return DEFAULT_OWNER_WALLET_TRON
    return DEFAULT_OWNER_WALLET_BSC

def init_ceo_wallet_table():
    """Initialize the ceo_wallet_settings table"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        _migrate_wallet_table(cur, "ceo_wallet_settings")
        conn.commit()
        cur.close()
        conn.close()
        logger.info("✅ CEO wallet settings table initialized")
    except Exception as e:
        logger.warning(f"Could not initialize CEO wallet table: {e}")

def save_ceo_wallet(network: str, wallet_address: str, token: str = 'USDT', updated_by: int = None):
    """Save CEO wallet address (per network+token) to database"""
    global ceo_wallet_cache
    try:
        conn = get_db_connection()
        if not conn:
            ceo_wallet_cache[(network, token)] = wallet_address
            return True
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ceo_wallet_settings (network, token, wallet_address, updated_at, updated_by)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s)
            ON CONFLICT (network, token) DO UPDATE SET 
                wallet_address = %s,
                updated_at = CURRENT_TIMESTAMP,
                updated_by = %s
        """, (network, token, wallet_address, updated_by, wallet_address, updated_by))
        conn.commit()
        cur.close()
        conn.close()
        ceo_wallet_cache[(network, token)] = wallet_address
        logger.info(f"✅ Saved CEO wallet for {network}/{token}: {wallet_address}")
        return True
    except Exception as e:
        logger.warning(f"Could not save CEO wallet: {e}")
        ceo_wallet_cache[(network, token)] = wallet_address
        return False

def load_ceo_wallets():
    """Load CEO wallet addresses from database into cache"""
    global ceo_wallet_cache
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("SELECT network, token, wallet_address FROM ceo_wallet_settings")
        rows = cur.fetchall()
        for row in rows:
            token = row[1] or 'USDT'
            ceo_wallet_cache[(row[0], token)] = row[2]
            logger.info(f"📋 Loaded CEO wallet for {row[0]}/{token}: {row[2]}")
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not load CEO wallets: {e}")

def get_ceo_wallet(network: str, token: str = 'USDT') -> str:
    """Get CEO wallet address for a network + token (falls back to USDT / defaults)"""
    addr = ceo_wallet_cache.get((network, token)) or ceo_wallet_cache.get((network, 'USDT'))
    if addr:
        return addr
    if network == 'TRON':
        return DEFAULT_CEO_WALLET_TRON
    return DEFAULT_CEO_WALLET_BSC

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


def restore_active_deals_state():
    """
    Rebuild the in-memory deal state (roles, coin/network, approvals, etc.) from
    the deals table on startup so in-progress deals keep working after a restart.
    """
    try:
        deals = database.get_active_deals()
    except Exception as e:
        logger.warning(f"Could not load active deals for restore: {e}")
        return

    restored = 0
    for deal in deals:
        try:
            chat_id = deal.get('chat_id')
            if chat_id is None:
                continue

            buyer = deal.get('buyer_username')
            seller = deal.get('seller_username')
            status = deal.get('deal_status')

            # Roles + initiators
            if buyer or seller:
                room_initiators[chat_id] = {'buyer': buyer, 'seller': seller}
                roles = {}
                if buyer:
                    roles[buyer.lower()] = 'BUYER'
                if seller:
                    roles[seller.lower()] = 'SELLER'
                if roles:
                    user_roles[chat_id] = roles

            # Blockchain / coin
            if deal.get('network'):
                user_blockchain[chat_id] = deal['network']
            if deal.get('coin'):
                user_coins[chat_id] = deal['coin']

            # Deal approvals
            approvals[chat_id] = {
                'buyer': bool(deal.get('buyer_approved')),
                'seller': bool(deal.get('seller_approved')),
            }

            # Release approval (seller-tracked)
            if deal.get('seller_release_approved'):
                release_approvals[chat_id] = {'seller': 'approved'}

            # Confirmed escrow balance (initial deposit + /add top-ups)
            if deal.get('deposit_amount') is not None:
                room_confirmed_deposits[chat_id] = float(deal['deposit_amount'])
            if deal.get('tx_hash'):
                room_used_tx_hashes.setdefault(chat_id, set()).add(deal['tx_hash'].lower())

            # Best-effort transaction state from the persisted deal status
            if status == database.DEAL_STATUS_RELEASE_PENDING:
                room_awaiting_hash[chat_id] = 'awaiting_hash'
                room_transaction_state[chat_id] = 'awaiting_hash'
            elif status == database.DEAL_STATUS_SUMMARY_SHOWN:
                room_transaction_state[chat_id] = 'deal_summary'

            # Don't re-send intro messages for rooms already in progress
            processed_rooms.add(chat_id)
            disclaimer_sent.add(chat_id)
            role_selection_sent.add(chat_id)
            restored += 1
        except Exception as e:
            logger.warning(f"Could not restore deal state for {deal.get('chat_id')}: {e}")

    logger.info(f"✅ Restored in-memory state for {restored} active deals")


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


def write_deal_request(initiator_id, initiator_username, counterparty_username, initiator_chat_id, counterparty_user_id=None, requested_room_number=None):
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

        # Preferred room number (used only if that number is free)
        if requested_room_number:
            request_data['requested_room_number'] = requested_room_number
        
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


def room_number_from_name(room_name):
    """Room number from a 'MM ROOM <n>' name, or None."""
    if not room_name:
        return None
    parts = str(room_name).strip().split()
    if parts and parts[-1].isdigit():
        return int(parts[-1])
    return None


def remove_room_record(chat_id):
    """Drop a room from deal_rooms.json so it is treated as fresh when reused."""
    try:
        if not os.path.exists(DEAL_ROOMS_FILE):
            return
        with open(DEAL_ROOMS_FILE, 'r') as f:
            rooms = json.load(f)
        if str(chat_id) in rooms:
            del rooms[str(chat_id)]
            with open(DEAL_ROOMS_FILE, 'w') as f:
                json.dump(rooms, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not remove room record {chat_id}: {e}")


def clear_room_state(chat_id):
    """Forget every in-memory trace of a finished deal so the room can be reused."""
    for tracker in (disclaimer_sent, role_selection_sent, processed_rooms, rooms_waiting_for_requests,
                    step1_messages_sent, step4_amount_messages_sent):
        tracker.discard(chat_id)
    # Every step message id has to go too: a leftover id makes the bot treat that
    # step as already sent and the next deal in this room stops there.
    for state in (room_awaiting_hash, room_transaction_state, user_roles, approvals, release_approvals,
                  room_joined_users, room_log_messages, room_confirmed_deposits, room_used_tx_hashes,
                  room_fee_tiers, room_creation_times, role_messages, room_initiators,
                  step2_blockchain_messages, step3_coin_messages, step4_messages, step5_messages,
                  buyer_wallet_messages, seller_wallet_messages, deal_summary_messages,
                  deposit_address_messages, release_messages, payment_confirmations,
                  user_blockchain, user_coins, buyer_addresses, seller_addresses):
        state.pop(chat_id, None)
    room_messages.pop(str(chat_id), None)


def write_release_request(chat_id, room_number, room_name):
    """Ask the userbot to kick the room's normal members and return it to the
    premade room pool instead of deleting the group."""
    try:
        requests = []
        if os.path.exists(RELEASE_QUEUE_FILE):
            with open(RELEASE_QUEUE_FILE, 'r') as f:
                requests = json.load(f)

        requests.append({
            'chat_id': chat_id,
            'room_number': room_number,
            'room_name': room_name,
            'status': 'pending'
        })

        with open(RELEASE_QUEUE_FILE, 'w') as f:
            json.dump(requests, f, indent=2)

        logger.info(f"♻️ Wrote release request for {room_name} (chat_id: {chat_id})")
        return True
    except Exception as e:
        logger.error(f"❌ Error writing release request: {e}")
        return False


def write_prewarm_request(user_id):
    """Ask the userbot to pre-create the 20 room pool (/startroom)."""
    try:
        requests = []
        if os.path.exists(PREWARM_QUEUE_FILE):
            with open(PREWARM_QUEUE_FILE, 'r') as f:
                requests = json.load(f)

        request_id = f"{user_id}_{int(time.time())}"
        requests.append({
            'request_id': request_id,
            'user_id': user_id,
            'bot_token': os.getenv('TELEGRAM_BOT_TOKEN', ''),
            'status': 'pending'
        })

        with open(PREWARM_QUEUE_FILE, 'w') as f:
            json.dump(requests, f, indent=2)

        logger.info(f"🏠 Wrote room pool prewarm request {request_id}")
        return request_id
    except Exception as e:
        logger.error(f"❌ Error writing prewarm request: {e}")
        return None


def cancel_prewarm_request(request_id):
    """Flag a /startroom request so the userbot stops creating rooms."""
    try:
        if not os.path.exists(PREWARM_QUEUE_FILE):
            return False
        with open(PREWARM_QUEUE_FILE, 'r') as f:
            requests = json.load(f)
        for req in requests:
            if req.get('request_id') == request_id:
                req['cancel'] = True
        with open(PREWARM_QUEUE_FILE, 'w') as f:
            json.dump(requests, f, indent=2)
        logger.info(f"🛑 Cancelled room prewarm request {request_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Error cancelling prewarm request: {e}")
        return False


def read_room_pool():
    """Premade rooms currently waiting to be assigned."""
    try:
        if os.path.exists(ROOM_POOL_FILE):
            with open(ROOM_POOL_FILE, 'r') as f:
                pool = json.load(f)
                if isinstance(pool, list):
                    return pool
    except Exception as e:
        logger.warning(f"Could not read room pool: {e}")
    return []


def get_prewarm_result(request_id):
    """Result of a /startroom request once the userbot has finished it."""
    try:
        if os.path.exists(PREWARM_QUEUE_FILE):
            with open(PREWARM_QUEUE_FILE, 'r') as f:
                for req in json.load(f):
                    if req.get('request_id') == request_id and req.get('status') == 'completed':
                        return req.get('result', {})
    except Exception as e:
        logger.warning(f"Could not read prewarm result: {e}")
    return None


async def startroom_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """CEO-only: pre-create all 20 rooms so /room can hand them out instantly."""
    user = update.effective_user
    if not user or user.id != CEO_USER_ID:
        return

    pool_size = len(read_room_pool())
    request_id = write_prewarm_request(user.id)
    if not request_id:
        await update.message.reply_text("❌ Could not queue room creation.")
        return

    cancel_markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("❌ Cancel", callback_data=f"startroom:cancel:{request_id}")
    ]])
    status_msg = await update.message.reply_text(
        f"🏠 <b>Preparing rooms…</b>\n\nPremade rooms ready: {pool_size}",
        parse_mode='HTML',
        reply_markup=cancel_markup
    )

    # Creating 20 rooms takes a while - report progress in the background so the
    # bot keeps handling everything else (join requests included) meanwhile.
    context.application.create_task(
        report_prewarm_progress(status_msg, request_id, pool_size, cancel_markup)
    )


async def report_prewarm_progress(status_msg, request_id, pool_size, cancel_markup):
    """Keep the /startroom status message up to date until the run finishes."""
    last_reported = pool_size
    for _ in range(5400):
        await asyncio.sleep(2)
        result = get_prewarm_result(request_id)
        current = len(read_room_pool())
        if current != last_reported and result is None:
            last_reported = current
            try:
                await status_msg.edit_text(
                    f"🏠 <b>Preparing rooms…</b>\n\nPremade rooms ready: {current}",
                    parse_mode='HTML',
                    reply_markup=cancel_markup
                )
            except Exception:
                pass
        if result is not None:
            created = result.get('created', [])
            error = result.get('error')
            repaired = result.get('repaired', [])
            rebuilt = result.get('rebuilt', [])
            available = result.get('pool_size', current)
            if result.get('cancelled'):
                header = "🛑 <b>Room preparation cancelled</b>"
            elif error:
                header = "❌ <b>Room preparation stopped</b>"
            else:
                header = "✅ <b>Rooms ready</b>"
            text = (
                f"{header}\n\n"
                f"Newly created: {len(created)}\n"
                f"Premade rooms available: {available}"
            )
            if rebuilt:
                text += f"\nRebuilt (missing admins): {', '.join(str(n) for n in rebuilt)}"
            if repaired:
                text += f"\nSetup completed for: {', '.join(str(n) for n in repaired)}"
            if error:
                safe_error = str(error).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                text += f"\n\n<b>Error:</b> {safe_error}\nRun /startroom again to continue."
            await status_msg.edit_text(text, parse_mode='HTML')
            return

    await status_msg.edit_text(
        f"⏳ Still preparing rooms. Premade rooms available so far: {len(read_room_pool())}"
    )


async def kick_member(bot, chat_id: int, user_id: int) -> None:
    """Remove a member without leaving them banned, so they can be added back
    or rejoin another room later."""
    await bot.ban_chat_member(chat_id, user_id)
    try:
        await bot.unban_chat_member(chat_id, user_id, only_if_banned=True)
    except Exception as e:
        logger.warning(f"Could not lift the ban on {user_id} in {chat_id}: {e}")


def format_deal_duration(started_at) -> str:
    """How long a deal took, as '28 mins' / '2 hours 5 mins'."""
    if not started_at:
        return "N/A"
    try:
        started = started_at if isinstance(started_at, datetime) else datetime.fromisoformat(str(started_at))
        seconds = (datetime.now(started.tzinfo) - started).total_seconds()
    except Exception:
        return "N/A"
    minutes = max(0, int(seconds // 60))
    if minutes < 60:
        return f"{minutes} min" if minutes == 1 else f"{minutes} mins"
    hours, minutes = divmod(minutes, 60)
    hour_part = f"{hours} hour" if hours == 1 else f"{hours} hours"
    return hour_part if minutes == 0 else f"{hour_part} {minutes} mins"


_background_tasks = set()


def schedule_task(coro):
    """Run a coroutine in the background, keeping a reference so it is not
    garbage collected mid-flight, and logging anything it raises."""
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    task.add_done_callback(
        lambda t: t.cancelled() or t.exception() and logger.warning(f"Background task failed: {t.exception()}")
    )
    return task


async def send_deal_complete_message(bot, original_chat_id: int, tx_url: str, duration: str, delay: float = 3.0):
    """The closing 'Deal Complete!' card, sent a few seconds after the release."""
    await asyncio.sleep(delay)
    send_chat_id = -1000000000000 - original_chat_id
    text = (
        "🎉 <b>Deal Complete!</b> ✅\n\n"
        f"⏱️ <b>Time Taken:</b> {duration}\n"
        f"🔗 <b>Release TX Link:</b> <a href=\"{tx_url}\">Click Here</a>\n\n"
        "Thank you for using our safe escrow system."
    )
    reply_markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌  Close", callback_data=f"close_deal_{original_chat_id}")]]
    )
    image_path = os.path.join(SCRIPT_DIR, "deal_completed_image.jpg")
    try:
        if os.path.exists(image_path):
            await bot.send_photo(
                chat_id=send_chat_id,
                photo=open(image_path, 'rb'),
                caption=text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
        else:
            await bot.send_message(
                chat_id=send_chat_id,
                text=text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
        logger.info(f"✅ Sent Deal Complete message to room {original_chat_id}")
    except Exception as e:
        logger.warning(f"Could not send Deal Complete message: {e}")


async def wait_for_deal_result(application, initiator_username):
    """Poll the queue until the userbot reports the room, then stop - polling on
    past the result would keep an update slot busy for no reason."""
    for _ in range(300):  # up to ~60s
        await asyncio.sleep(0.2)
        if await check_and_send_deal_results(application, initiator_username):
            return


async def check_and_send_deal_results(application, initiator_username) -> bool:
    """Check if deal room was created and send results to initiating group.
    Returns True once a result has been sent."""
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

                            # Send the room log to the logs channel immediately at room creation
                            try:
                                original_chat_id = abs(chat_id) - 1000000000000 if chat_id < 0 else chat_id
                                room_data = database.get_deal(original_chat_id) or {}
                                buyer_username = room_data.get('buyer_username') or 'Unknown'
                                seller_username = room_data.get('seller_username') or 'Unknown'
                                token_name = room_data.get('coin') or 'Pending'
                                blockchain = room_data.get('network') or 'Pending'
                                amount = room_data.get('amount') or 'Pending'
                                await send_room_log_message(
                                    application.bot, original_chat_id, buyer_username, seller_username,
                                    token_name, blockchain, amount, "Room Assigned"
                                )
                            except Exception as e:
                                logger.warning(f"Could not send initial room log: {e}")
                        
                        # Mark as sent to prevent duplicate operations
                        req['sent'] = True
                        updated = True
            
            # Save updated requests with sent flag
            if updated:
                with open(DEAL_QUEUE_FILE, 'w') as f:
                    json.dump(requests, f, indent=2)
            return updated
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    return False


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

    # Only the seller may use /release
    deal_data = database.get_deal(original_chat_id)
    seller_user_id = deal_data.get('seller_user_id') if deal_data else None
    is_seller = False
    if seller_user_id and user.id == seller_user_id:
        is_seller = True
    elif user.username and seller_username and user.username.lower() == seller_username.lower():
        is_seller = True
    if not is_seller:
        await context.bot.send_message(
            chat_id=chat_id,
            text="❌ Only the seller can use /release."
        )
        return

    # Initialize release approvals - only seller needs to approve now
    release_approvals[original_chat_id] = {'seller': 'waiting'}
    
    # Create release confirmation message - seller only
    release_text = f"""<b>RELEASE CONFIRMATION (Partial)</b>

⌛️ @{seller_username} - Waiting for confirmation...

⚠️ Seller, please confirm you want to return these funds to the Buyer."""
    
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
    """Handle /room @username or /room [user_id] command"""
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
    username_match = re.search(r'/room\s+@(\w+)', message_text)
    if username_match:
        counterparty_username = username_match.group(1)
    else:
        # Second try: check if user ID is provided (numeric)
        userid_match = re.search(r'/room\s+(\d+)', message_text)
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
            "❌ Please mention the counterparty (tap their name to tag), provide their user ID, or reply to their message when using /room.\n\n"
            "Usage:\n"
            "/room @username\n"
            "/room 123456789"
        )
        return
    
    # Optional preferred room number: /room @username 18 or /room <user id> 18.
    # It's only honoured if that number is free; otherwise the normal sequence is used.
    requested_room_number = None
    command_parts = message_text.split()
    if len(command_parts) >= 3 and command_parts[2].isdigit():
        candidate = int(command_parts[2])
        if ROOM_NUMBER_MIN <= candidate <= ROOM_NUMBER_MAX:
            requested_room_number = candidate

    initiator_chat_id = update.effective_chat.id
    
    # Delete the user's command message
    try:
        await update.message.delete()
    except:
        pass
    
    # Queue the deal request for userbot to process
    if write_deal_request(user.id, user.username or user.first_name, counterparty_username, initiator_chat_id, counterparty_user_id, requested_room_number):
        if counterparty_username:
            logger.info(f"📋 /room command: {user.username or user.first_name} -> @{counterparty_username}")
        else:
            logger.info(f"📋 /room command: {user.username or user.first_name} -> User {counterparty_user_id}")
        
        # Wait for the userbot's result in the background - a blocking wait here
        # would stall every other update (join requests included) for a minute.
        context.application.create_task(
            wait_for_deal_result(context.application, user.username or user.first_name)
        )
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
        # Show current owner wallets (per token)
        await update.message.reply_text(
            f"<b>Current Owner Wallets:</b>\n\n"
            f"<b>BSC USDT:</b> <code>{get_owner_wallet('BSC', 'USDT')}</code>\n"
            f"<b>BSC USDC:</b> <code>{get_owner_wallet('BSC', 'USDC')}</code>\n"
            f"<b>TRON USDT:</b> <code>{get_owner_wallet('TRON', 'USDT')}</code>\n\n"
            f"<b>Usage:</b>\n"
            f"<code>/setownerwallet 0x...</code> (BSC — pick USDT/USDC)\n"
            f"<code>/setownerwallet T...</code> (TRON USDT)",
            parse_mode='HTML'
        )
        return

    await _handle_set_wallet(update, context, 'owner', parts[1].strip())


async def _handle_set_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE,
                             role: str, new_address: str) -> None:
    """Validate a wallet address and either save it (TRON) or ask which token it's for (BSC)."""
    user = update.effective_user

    # Validate and determine network based on address format
    if new_address.startswith('0x') and len(new_address) == 42:
        try:
            int(new_address[2:], 16)  # Validate hex
            network = 'BSC'
        except ValueError:
            await update.message.reply_text("❌ Invalid BSC address. Must be 0x followed by 40 hex characters.")
            return
    elif new_address.startswith('T') and len(new_address) == 34:
        network = 'TRON'
    else:
        await update.message.reply_text(
            "❌ Invalid wallet address format.\n\n"
            "BSC: Must start with 0x and be 42 characters\n"
            "TRON: Must start with T and be 34 characters"
        )
        return

    # TRON only has USDT — save directly
    if network == 'TRON':
        _save_wallet_for_token(role, network, 'USDT', new_address, user.id)
        await _report_wallet_saved(update, role, network, 'USDT', new_address)
        return

    # BSC: ask which token this address is for
    pending_wallet_set[user.id] = {'role': role, 'network': network, 'address': new_address}
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("USDT", callback_data=f"setwallet:{role}:USDT"),
        InlineKeyboardButton("USDC", callback_data=f"setwallet:{role}:USDC"),
    ]])
    await update.message.reply_text(
        f"<b>Set {role.upper()} {network} wallet</b>\n\n"
        f"<code>{new_address}</code>\n\n"
        f"Which token is this address for?",
        parse_mode='HTML',
        reply_markup=keyboard
    )


def _save_wallet_for_token(role: str, network: str, token: str, address: str, updated_by: int) -> bool:
    """Persist a wallet address for the given role/network/token."""
    if role == 'owner':
        return save_owner_wallet(network, address, token, updated_by)
    return save_ceo_wallet(network, address, token, updated_by)


async def _report_wallet_saved(update_or_query, role: str, network: str, token: str, address: str) -> None:
    """Send a confirmation that a wallet was saved (works for message or callback query)."""
    text = (
        f"✅ <b>{role.upper()} Wallet Updated!</b>\n\n"
        f"<b>Network:</b> {network}\n"
        f"<b>Token:</b> {token}\n"
        f"<b>New Address:</b> <code>{address}</code>\n\n"
        f"All future deal rooms will use this address for {token} deposits."
    )
    if hasattr(update_or_query, 'edit_message_text'):
        await update_or_query.edit_message_text(text, parse_mode='HTML')
    else:
        await update_or_query.message.reply_text(text, parse_mode='HTML')


async def handle_setwallet_callback(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle USDT/USDC token choice for /setownerwallet and /setceowallet."""
    user_id = query.from_user.id
    if user_id not in ADMIN_USER_IDS:
        await query.answer("Admins only.", show_alert=True)
        return

    parts = query.data.split(':')  # setwallet:<role>:<token>
    token = parts[2] if len(parts) > 2 else 'USDT'
    pending = pending_wallet_set.pop(user_id, None)
    if not pending:
        await query.answer("This request expired, run the command again.", show_alert=True)
        return

    role = pending['role']
    network = pending['network']
    address = pending['address']
    _save_wallet_for_token(role, network, token, address, user_id)
    await query.answer(f"Saved as {token} ✅")
    await _report_wallet_saved(query, role, network, token, address)
    logger.info(f"✅ Admin {user_id} updated {network}/{token} {role} wallet to: {address}")


def deal_for_trade_id(arg: str):
    """The deal a Trade ID belongs to (P2PMMX5090, 5090 or #5090), or None."""
    return database.get_deal_by_trade_id(arg) if arg else None


def trade_label(deal: dict) -> str:
    """Trade ID plus room name, for confirmations."""
    room = deal.get('room_name') or (
        f"MM ROOM {deal['room_number']}" if deal.get('room_number') else 'room'
    )
    return f"{deal.get('trade_id')} ({room})"


async def setaddy_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /setaddy <trade id> - admin only. Fix a deal's deposit to the Owner or
    CEO wallet. Works before network/token is chosen; the role is remembered and
    resolved to the actual address when the deposit is generated."""
    user = update.effective_user

    # Silently ignore unauthorized users
    if user.id not in ADMIN_USER_IDS:
        return

    if not context.args:
        await update.message.reply_text(
            "❌ Usage: <code>/setaddy &lt;trade id&gt;</code>",
            parse_mode='HTML'
        )
        return

    deal = deal_for_trade_id(context.args[0])
    if not deal:
        await update.message.reply_text(
            f"❌ No deal with Trade ID <code>{context.args[0].strip()}</code>.",
            parse_mode='HTML'
        )
        return

    original_chat_id = deal['chat_id']
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Owner", callback_data=f"setaddy:owner:{original_chat_id}"),
        InlineKeyboardButton("CEO", callback_data=f"setaddy:ceo:{original_chat_id}"),
    ]])
    await update.message.reply_text(
        f"<b>Set deposit wallet for</b> <code>{trade_label(deal)}</code>\n\n"
        f"Which marked address should this deal use?",
        parse_mode='HTML',
        reply_markup=keyboard
    )


async def handle_setaddy_callback(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle Owner/CEO choice for /setaddy and persist it on the room's deal."""
    user_id = query.from_user.id
    if user_id not in ADMIN_USER_IDS:
        await query.answer("Admins only.", show_alert=True)
        return

    parts = query.data.split(':')  # setaddy:<role>:<chat_id>
    role = parts[1] if len(parts) > 1 else ''
    try:
        original_chat_id = int(parts[2])
    except (IndexError, ValueError):
        await query.answer("Invalid request.", show_alert=True)
        return

    if role not in ('owner', 'ceo'):
        await query.answer("Invalid choice.", show_alert=True)
        return

    # Ensure a deal record exists so the override persists (even before setup)
    deal = database.get_deal(original_chat_id)
    if not deal:
        database.create_deal(chat_id=original_chat_id)
        deal = database.get_deal(original_chat_id) or {'chat_id': original_chat_id}
    database.update_deal(original_chat_id, fixed_wallet_role=role)

    await query.answer(f"Fixed to {role.upper()} ✅")
    await query.edit_message_text(
        f"✅ <code>{trade_label(deal)}</code> deposit fixed to the <b>{role.upper()}</b> wallet.\n\n"
        f"The deposit will use the {role.upper()} address for this deal's selected network + token.",
        parse_mode='HTML'
    )
    logger.info(f"🏦 Admin {user_id} fixed room {original_chat_id} deposit to {role} wallet")


async def reply_privately(update: Update, text: str, reply_markup=None) -> None:
    """Answer an admin command without leaving anything visible in a deal room:
    delete the command in groups and send the reply to the admin's DM."""
    user = update.effective_user
    in_group = update.effective_chat.type in ['group', 'supergroup']

    if in_group:
        try:
            await update.message.delete()
        except Exception as e:
            logger.warning(f"Could not delete admin command message: {e}")
        try:
            await update.get_bot().send_message(
                chat_id=user.id, text=text, parse_mode='HTML', reply_markup=reply_markup
            )
            return
        except Exception as e:
            logger.warning(f"Could not DM admin {user.id}: {e}")
            return

    await update.message.reply_text(text, parse_mode='HTML', reply_markup=reply_markup)


async def setfakeaddy_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /setfakeaddy <trade id> - admin only. Store a backup BSC escrow
    address (USDT or USDC) for that deal's room, applied later with /fakeaddy.
    Nothing is shown in the deal room; the exchange happens in the admin's DM."""
    user = update.effective_user

    if user.id not in ADMIN_USER_IDS:
        return

    if not context.args:
        await reply_privately(update, "❌ Usage: <code>/setfakeaddy &lt;trade id&gt;</code>")
        return

    deal = deal_for_trade_id(context.args[0])
    if not deal:
        await reply_privately(
            update,
            f"❌ No deal with Trade ID <code>{context.args[0].strip()}</code>."
        )
        return

    room_number = deal.get('room_number')
    if room_number is None:
        await reply_privately(
            update,
            f"❌ No room number known for <code>{deal.get('trade_id')}</code>."
        )
        return
    room_number = int(room_number)

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("USDT", callback_data=f"setfakeaddy:USDT:{room_number}"),
        InlineKeyboardButton("USDC", callback_data=f"setfakeaddy:USDC:{room_number}"),
    ]])
    stored = database.get_room_backup_wallets(room_number)
    stored_lines = "".join(
        f"\n• {token}: <code>{addr}</code>" for token, addr in sorted(stored.items())
    )
    await reply_privately(
        update,
        f"<b>Backup deposit address for MM ROOM {room_number}</b> (BSC)"
        f"{stored_lines}\n\nWhich token is this address for?",
        reply_markup=keyboard
    )


async def handle_setfakeaddy_callback(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the USDT/USDC choice for /setfakeaddy, then wait for the address."""
    user_id = query.from_user.id
    if user_id not in ADMIN_USER_IDS:
        await query.answer("Admins only.", show_alert=True)
        return

    parts = query.data.split(':')  # setfakeaddy:<token>:<room_number>
    token = parts[1] if len(parts) > 1 else 'USDT'
    try:
        room_number = int(parts[2])
    except (IndexError, ValueError):
        await query.answer("Invalid room number.", show_alert=True)
        return

    pending_fake_addy[user_id] = {'room_number': room_number, 'token': token}

    await query.answer()
    await query.edit_message_text(
        f"Send the <b>{token}</b> (BSC) backup address for <b>MM ROOM {room_number}</b>.",
        parse_mode='HTML'
    )


async def process_fake_addy_address(update: Update, pending: dict) -> None:
    """Save the backup address an admin sent after choosing a token."""
    user = update.effective_user
    address = update.message.text.strip()

    if not re.fullmatch(r'0x[a-fA-F0-9]{40}', address):
        await update.message.reply_text(
            "❌ That's not a valid BSC address. Send a <code>0x…</code> address, "
            "or run the command again to cancel.",
            parse_mode='HTML'
        )
        return

    pending_fake_addy.pop(user.id, None)
    room_number = pending['room_number']
    token = pending['token']

    if database.set_room_backup_wallet(room_number, token, address, user.id):
        await update.message.reply_text(
            f"✅ Backup <b>{token}</b> address saved for <b>MM ROOM {room_number}</b>:\n"
            f"<code>{address}</code>\n\n"
            f"Apply it to a deal's deposit with <code>/fakeaddy &lt;trade id&gt;</code>.",
            parse_mode='HTML'
        )
        logger.info(f"🏦 Admin {user.id} set backup {token} address for room {room_number}")
    else:
        await update.message.reply_text("❌ Could not save the address, try again.")


async def fakeaddy_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /fakeaddy <trade id> - admin only. Switch one deal's deposit to the
    backup address stored for its room (before the deposit is sent). Only that
    deal is affected - the room is back on the normal wallet for the next one."""
    user = update.effective_user

    if user.id not in ADMIN_USER_IDS:
        return

    if not context.args:
        await reply_privately(update, "❌ Usage: <code>/fakeaddy &lt;trade id&gt;</code>")
        return

    deal = deal_for_trade_id(context.args[0])
    if not deal:
        await reply_privately(
            update,
            f"❌ No deal with Trade ID <code>{context.args[0].strip()}</code>."
        )
        return

    original_chat_id = deal['chat_id']
    room_number = deal.get('room_number')
    if room_number is None:
        await reply_privately(
            update,
            f"❌ No room number known for <code>{deal.get('trade_id')}</code>."
        )
        return
    room_number = int(room_number)

    stored = database.get_room_backup_wallets(int(room_number))
    if not stored:
        await reply_privately(
            update,
            f"❌ No backup address stored for <b>MM ROOM {room_number}</b>. "
            f"Set one with <code>/setfakeaddy {deal.get('trade_id')}</code>."
        )
        return

    database.update_deal(original_chat_id, fixed_wallet_role='backup')

    stored_lines = "".join(
        f"\n• {token}: <code>{addr}</code>" for token, addr in sorted(stored.items())
    )
    await reply_privately(
        update,
        f"✅ <code>{deal.get('trade_id')}</code> in <b>MM ROOM {room_number}</b> will use "
        f"the backup deposit address for this deal only:"
        f"{stored_lines}"
    )
    logger.info(
        f"🏦 Admin {user.id} switched {deal.get('trade_id')} "
        f"(MM ROOM {room_number}) to its backup address"
    )


async def fakeaddylist_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /fakeaddylist - admin only. List the stored backup escrow addresses
    per room number, marking the rooms currently using theirs."""
    user = update.effective_user

    if user.id not in ADMIN_USER_IDS:
        return

    entries = database.get_all_room_backup_wallets()
    if not entries:
        await reply_privately(
            update,
            "No backup addresses stored. Set one with <code>/setfakeaddy &lt;trade id&gt;</code>."
        )
        return

    active_rooms = set(database.get_rooms_using_backup_wallet())

    by_room = {}
    for entry in entries:
        by_room.setdefault(entry['room_number'], []).append(entry)

    lines = ["<b>Backup deposit addresses</b> (BSC)"]
    for room_number in sorted(by_room):
        in_use = " — <i>in use</i>" if room_number in active_rooms else ""
        lines.append(f"\n<b>MM ROOM {room_number}</b>{in_use}")
        for entry in by_room[room_number]:
            lines.append(f"• {entry['token']}: <code>{entry['wallet_address']}</code>")

    await reply_privately(update, "\n".join(lines))


async def addubot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /addubot - admin only. Store a backup userbot account that takes
    over room creation when another account hits a Telegram cooldown."""
    user = update.effective_user

    if user.id not in ADMIN_USER_IDS:
        return

    parts = (update.message.text or '').split()
    if len(parts) < 5:
        await reply_privately(
            update,
            "<b>Usage:</b> <code>/addubot &lt;label&gt; &lt;api_id&gt; &lt;api_hash&gt; "
            "&lt;session_string&gt;</code>\n\n"
            "The session string comes from a Telethon <code>StringSession</code> login "
            "of that account. Optionally add a priority number at the end "
            "(lower is tried first)."
        )
        return

    label, api_id_raw, api_hash, session_string = parts[1], parts[2], parts[3], parts[4]
    priority = 100
    if len(parts) > 5:
        try:
            priority = int(parts[5])
        except ValueError:
            await reply_privately(update, "❌ Priority must be a number.")
            return

    try:
        api_id = int(api_id_raw)
    except ValueError:
        await reply_privately(update, "❌ api_id must be a number.")
        return

    saved = database.save_userbot_account(
        label=label,
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        priority=priority,
        added_by=user.id
    )
    if not saved:
        await reply_privately(update, "❌ Could not save that userbot account.")
        return

    await reply_privately(
        update,
        f"✅ Backup userbot <b>{label}</b> saved (priority {priority}).\n\n"
        "Restart the userbot process so it connects, then it will take over room "
        "creation whenever another account is rate limited."
    )
    logger.info(f"🤖 Admin {user.id} added backup userbot '{label}'")


async def newubot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /newubot <label> <api_id> <api_hash> <phone> - admin only, DM only.
    Logs a backup userbot account in through Telegram and stores its session, so
    no session string ever has to be generated or pasted by hand."""
    user = update.effective_user

    if user.id not in ADMIN_USER_IDS:
        return

    if update.effective_chat.type != 'private':
        await reply_privately(
            update,
            "❌ Run <code>/newubot</code> in a DM with me - it involves a login code."
        )
        return

    if not TELETHON_AVAILABLE:
        await update.message.reply_text("❌ Telethon is not installed on the bot host.")
        return

    parts = (update.message.text or '').split()
    if len(parts) < 5:
        await update.message.reply_text(
            "<b>Usage:</b> <code>/newubot &lt;label&gt; &lt;api_id&gt; &lt;api_hash&gt; "
            "&lt;phone&gt;</code>\n\n"
            "Example: <code>/newubot backup1 123456 abcdef0123456789 +919058747049</code>\n\n"
            "api_id/api_hash come from https://my.telegram.org/apps for that account. "
            "I'll send it a login code and ask you for it here.",
            parse_mode='HTML'
        )
        return

    label, api_id_raw, api_hash, phone = parts[1], parts[2], parts[3], parts[4]
    try:
        api_id = int(api_id_raw)
    except ValueError:
        await update.message.reply_text("❌ api_id must be a number.")
        return

    await cancel_userbot_login(user.id)

    try:
        login_client = TelegramClient(StringSession(), api_id, api_hash)
        await login_client.connect()
        sent = await login_client.send_code_request(phone)
    except Exception as e:
        await update.message.reply_text(f"❌ Could not start the login: {e}")
        return

    pending_userbot_login[user.id] = {
        'client': login_client,
        'label': label,
        'api_id': api_id,
        'api_hash': api_hash,
        'phone': phone,
        'code_hash': sent.phone_code_hash,
        'step': 'code'
    }

    await update.message.reply_text(
        f"📲 Login code sent to <code>{phone}</code>.\n\n"
        "Send it here <b>with spaces between the digits</b> (e.g. <code>1 2 3 4 5</code>) - "
        "Telegram cancels codes that are posted as plain numbers in a chat.\n\n"
        "Send <code>/cancelubot</code> to abort.",
        parse_mode='HTML'
    )


async def cancel_userbot_login(user_id: int) -> bool:
    """Drop an in-progress /newubot login and disconnect its client."""
    pending = pending_userbot_login.pop(user_id, None)
    if not pending:
        return False
    try:
        await pending['client'].disconnect()
    except Exception as e:
        logger.warning(f"Could not close the userbot login client: {e}")
    return True


async def cancelubot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /cancelubot - abort an in-progress /newubot login."""
    user = update.effective_user

    if user.id not in ADMIN_USER_IDS:
        return

    if await cancel_userbot_login(user.id):
        await update.message.reply_text("🛑 Userbot login cancelled.")
    else:
        await update.message.reply_text("No userbot login in progress.")


async def process_userbot_login_input(update: Update, pending: dict) -> None:
    """Take the login code, then the 2FA password if needed, and save the account."""
    user = update.effective_user
    text = (update.message.text or '').strip()
    login_client = pending['client']

    try:
        if pending['step'] == 'code':
            code = re.sub(r'\D', '', text)
            if not code:
                await update.message.reply_text(
                    "❌ Send the login code's digits, or <code>/cancelubot</code> to abort.",
                    parse_mode='HTML'
                )
                return
            try:
                await login_client.sign_in(
                    phone=pending['phone'],
                    code=code,
                    phone_code_hash=pending['code_hash']
                )
            except SessionPasswordNeededError:
                pending['step'] = 'password'
                await update.message.reply_text(
                    "🔐 That account has 2FA. Send its password now "
                    "(delete your message afterwards)."
                )
                return
        else:
            await login_client.sign_in(password=text)
    except Exception as e:
        await update.message.reply_text(
            f"❌ Login failed: {e}\n\nRun <code>/newubot</code> again to retry.",
            parse_mode='HTML'
        )
        await cancel_userbot_login(user.id)
        return

    session_string = login_client.session.save()
    try:
        me = await login_client.get_me()
        account_name = f"@{me.username}" if me.username else str(me.id)
    except Exception:
        account_name = pending['phone']

    saved = database.save_userbot_account(
        label=pending['label'],
        api_id=pending['api_id'],
        api_hash=pending['api_hash'],
        session_string=session_string,
        added_by=user.id
    )
    await cancel_userbot_login(user.id)

    try:
        await update.message.delete()
    except Exception as e:
        logger.warning(f"Could not delete the login message: {e}")

    if saved:
        await update.get_bot().send_message(
            chat_id=user.id,
            text=(
                f"✅ Backup userbot <b>{pending['label']}</b> ({account_name}) logged in "
                f"and saved.\n\nRestart the userbot process so it picks the account up; "
                f"it then takes over room creation whenever another account is rate limited."
            ),
            parse_mode='HTML'
        )
        logger.info(f"🤖 Admin {user.id} logged in backup userbot '{pending['label']}'")
    else:
        await update.get_bot().send_message(
            chat_id=user.id,
            text="❌ Logged in but could not save the account to the database."
        )


async def ubots_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /ubots - admin only. List the stored backup userbot accounts."""
    user = update.effective_user

    if user.id not in ADMIN_USER_IDS:
        return

    accounts = database.get_userbot_accounts(enabled_only=False)
    if not accounts:
        await reply_privately(
            update,
            "No backup userbots stored. Add one with <code>/addubot &lt;label&gt; "
            "&lt;api_id&gt; &lt;api_hash&gt; &lt;session_string&gt;</code>."
        )
        return

    lines = ["<b>Backup userbots</b> (primary is always first)"]
    for account in accounts:
        state = "enabled" if account.get('enabled') else "disabled"
        lines.append(
            f"• <b>{account['label']}</b> — priority {account.get('priority')}, {state}"
        )
    await reply_privately(update, "\n".join(lines))


async def delubot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /delubot <label> - admin only. Remove a backup userbot account."""
    user = update.effective_user

    if user.id not in ADMIN_USER_IDS:
        return

    parts = (update.message.text or '').split()
    if len(parts) < 2:
        await reply_privately(update, "<b>Usage:</b> <code>/delubot &lt;label&gt;</code>")
        return

    label = parts[1]
    if database.delete_userbot_account(label):
        await reply_privately(update, f"✅ Backup userbot <b>{label}</b> removed.")
    else:
        await reply_privately(update, f"❌ No backup userbot named <b>{label}</b>.")


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
        # Show current CEO wallets (per token)
        await update.message.reply_text(
            f"<b>Current CEO Wallets:</b>\n\n"
            f"<b>BSC USDT:</b> <code>{get_ceo_wallet('BSC', 'USDT')}</code>\n"
            f"<b>BSC USDC:</b> <code>{get_ceo_wallet('BSC', 'USDC')}</code>\n"
            f"<b>TRON USDT:</b> <code>{get_ceo_wallet('TRON', 'USDT')}</code>\n\n"
            f"<b>Usage:</b>\n"
            f"<code>/setceowallet 0x...</code> (BSC — pick USDT/USDC)\n"
            f"<code>/setceowallet T...</code> (TRON USDT)",
            parse_mode='HTML'
        )
        return

    await _handle_set_wallet(update, context, 'ceo', parts[1].strip())


async def wallets_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /wallets command - admin only, shows all active deposit wallets"""
    user = update.effective_user
    
    # Silently ignore unauthorized users
    if user.id not in ADMIN_USER_IDS:
        return
    
    await update.message.reply_text(
        f"<b>Active Deposit Wallets</b>\n\n"
        f"<b>Owner Wallet:</b>\n"
        f"BSC USDT: <code>{get_owner_wallet('BSC', 'USDT')}</code>\n"
        f"BSC USDC: <code>{get_owner_wallet('BSC', 'USDC')}</code>\n"
        f"TRON USDT: <code>{get_owner_wallet('TRON', 'USDT')}</code>\n\n"
        f"<b>CEO Wallet:</b>\n"
        f"BSC USDT: <code>{get_ceo_wallet('BSC', 'USDT')}</code>\n"
        f"BSC USDC: <code>{get_ceo_wallet('BSC', 'USDC')}</code>\n"
        f"TRON USDT: <code>{get_ceo_wallet('TRON', 'USDT')}</code>",
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
            await kick_member(context.bot, chat_id, target_user_id)
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


async def dash_kick_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle -kick @username | <user id> | reply - admins remove a member from the
    monitored P2P ROOM group. Works from any chat the admin can message the bot in."""
    user = update.effective_user

    # Silently ignore unauthorized users
    if user.id not in ADMIN_USER_IDS:
        return

    text = update.message.text or ''
    target_user_id = None
    target_username = None

    replied = update.message.reply_to_message.from_user if update.message.reply_to_message else None
    if replied:
        target_user_id = replied.id
        target_username = replied.username
    else:
        match = re.match(r'-kick\s+(\S+)', text)
        if not match:
            await update.message.reply_text(
                "❌ Usage: reply to a user with <code>-kick</code>, or "
                "<code>-kick @username</code> | <code>-kick &lt;user id&gt;</code>",
                parse_mode='HTML'
            )
            return
        arg = match.group(1)
        if arg.isdigit():
            target_user_id = int(arg)
        else:
            target_username = arg.lstrip('@')
            target_user_id = database.get_user_id_by_username(target_username)

    if not target_user_id:
        await update.message.reply_text(
            f"⚠️ I don't know the Telegram id for @{target_username} yet. Reply to one of "
            f"their messages with -kick, or pass their numeric id."
        )
        return

    display = (f"@{target_username} [<code>{target_user_id}</code>]" if target_username
               else f"<code>{target_user_id}</code>")

    try:
        await kick_member(context.bot, P2P_ROOM_GROUP_ID, target_user_id)
        logger.info(f"🚫 Admin {user.id} kicked {target_user_id} (@{target_username}) from the P2P ROOM group")
        await update.message.reply_text(
            f"✅ {display} has been kicked from the P2P ROOM group.",
            parse_mode='HTML'
        )
    except Exception as e:
        logger.warning(f"Failed to kick {target_user_id} from the P2P ROOM group: {e}")
        await update.message.reply_text(
            f"❌ Could not kick {display}: {str(e)[:80]}",
            parse_mode='HTML'
        )


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
    if network == 'TRON':
        network_fee = 3.0
    else:
        network_fee = NETWORK_FEE_BSC
    
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


async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /add - ask for an additional deposit into the deal's escrow.
    Only available once the deposit has been confirmed (payment received)."""
    user = update.effective_user
    logger.info(f"💰 /add command by user {user.id}")

    if update.effective_chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("❌ This command can only be used inside a group.")
        return

    original_chat_id = normalize_chat_id(update.effective_chat.id)
    send_chat_id = -1000000000000 - original_chat_id

    deal = database.get_deal(original_chat_id)
    status = deal.get('deal_status') if deal else None
    deposit_confirmed = (
        original_chat_id in room_confirmed_deposits
        or status in (
            database.DEAL_STATUS_DEPOSIT_RECEIVED,
            database.DEAL_STATUS_COMPLETED,
        )
    )
    if not deposit_confirmed:
        logger.info(f"❌ /add before deposit confirmed in room {original_chat_id}")
        await update.message.reply_text(
            "❌ add command is only available after the deposit has been confirmed by the bot."
        )
        return

    escrow_address = deal.get('escrow_address') if deal else None
    if not escrow_address:
        await update.message.reply_text("❌ Escrow address not found for this deal.")
        return

    coin = user_coins.get(original_chat_id) or (deal.get('coin') if deal else None) or 'USDT'
    network = user_blockchain.get(original_chat_id) or (deal.get('network') if deal else None) or 'BSC'
    seller_username = (
        room_initiators.get(original_chat_id, {}).get('seller')
        or (deal.get('seller_username') if deal else None)
    )

    try:
        await update.message.delete()
    except Exception:
        pass

    add_funds_text = (
        f"💰 <b>ADD MORE FUNDS</b>\n\n"
        f"To deposit additional funds, send {coin} to:\n\n"
        f"<code>{escrow_address}</code>\n\n"
        f"Network: {network}\n\n"
        f"⚠️ <b>After sending, please paste the Transaction Hash (TXID) here to "
        f"automatically update the balance.</b>"
    )
    await context.bot.send_message(
        chat_id=send_chat_id,
        text=add_funds_text,
        parse_mode='HTML'
    )

    seller_display = f"@{seller_username}" if seller_username else "Seller"
    await context.bot.send_message(
        chat_id=send_chat_id,
        text=f"✉️ {seller_display} kindly paste the transaction hash or explorer link.",
        parse_mode='HTML'
    )

    room_transaction_state[original_chat_id] = 'awaiting_add_hash'
    logger.info(f"💰 /add started in room {original_chat_id} - awaiting extra deposit hash")


async def dispute_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /dispute <reason> - report a dispute for admin review.
    Only available once the deposit has been confirmed."""
    user = update.effective_user
    logger.info(f"⚖️ /dispute command by user {user.id}")

    if update.effective_chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("❌ This command can only be used inside a group.")
        return

    original_chat_id = normalize_chat_id(update.effective_chat.id)

    deal = database.get_deal(original_chat_id)
    status = deal.get('deal_status') if deal else None
    deposit_confirmed = (
        original_chat_id in room_confirmed_deposits
        or status in (
            database.DEAL_STATUS_DEPOSIT_RECEIVED,
            database.DEAL_STATUS_COMPLETED,
        )
    )
    if not deposit_confirmed:
        logger.info(f"❌ /dispute before deposit confirmed in room {original_chat_id}")
        await update.message.reply_text(
            "❌ dispute command is only available after the deposit has been confirmed by the bot."
        )
        return

    reason = ' '.join(context.args).strip() if context.args else ''
    if not reason:
        await update.message.reply_text(
            "❌ <b>PLEASE PROVIDE A REASON FOR THE DISPUTE.</b>\n\n"
            "Usage: <code>/dispute &lt;reason&gt;</code>\n\n"
            "Example: <code>/dispute Payment not received from buyer</code>",
            parse_mode='HTML'
        )
        return

    dispute_text = (
        "✅ <b>Dispute reported successfully!</b>\n\n"
        "An admin will review your dispute and join this group to resolve the issue.\n\n"
        f"<b>Reason:</b> {html.escape(reason)}"
    )
    await context.bot.send_message(
        chat_id=-1000000000000 - original_chat_id,
        text=dispute_text,
        parse_mode='HTML'
    )
    logger.info(f"⚖️ Dispute reported in room {original_chat_id} by {user.id}: {reason[:80]}")


# ============================================================================
# /addstats - admin-only interactive stats builder
# ============================================================================

# Active /addstats sessions: {(chat_id, user_id): {chat_id, message_id, display,
#   data: {field: value}, awaiting: field_key or None, section: name or None}}
addstats_sessions = {}

# Ordered sections and the lines the bot asks for, one by one, per section.
ADDSTATS_SECTIONS = {
    'buying': ('🟢 Buying Stats', [
        ('total_bought', '💰 Send the <b>Total Bought</b> amount (e.g. 1500.00)'),
        ('buy_trades', '🔢 Send the <b>Total Buy Trades</b> count (e.g. 12)'),
    ]),
    'selling': ('🔴 Selling Stats', [
        ('total_sold', '💵 Send the <b>Total Sold</b> amount (e.g. 900.00)'),
        ('sell_trades', '🔢 Send the <b>Total Sell Trades</b> count (e.g. 8)'),
    ]),
    'overall': ('📈 Overall Performance', [
        ('completion_rate', '✅ Send the <b>Completion Rate</b> as done/total (e.g. 19/20) — I\'ll add the %'),
        ('global_rank', '🏆 Send the <b>Global Rank</b> number (e.g. 42)'),
    ]),
}
# Lifetime Volume (Total Bought + Total Sold) and Total Deals (Buy + Sell Trades)
# are auto-computed, so they are never asked.


# Fields rendered as money ($, 2 decimals) vs whole-number counts.
ADDSTATS_MONEY_FIELDS = {'total_bought', 'total_sold', 'lifetime_volume'}
ADDSTATS_INT_FIELDS = {'buy_trades', 'sell_trades', 'total_deals', 'global_rank'}


def _money_to_float(value) -> float:
    """Parse a display money/number string (e.g. '1,234.00') back to a float."""
    try:
        return float(str(value).replace(',', '').replace('$', '').strip())
    except (ValueError, AttributeError):
        return 0.0


def _format_completion_rate(raw: str) -> str:
    """'19/20' -> '95.0% (19 / 20)'. Leaves any other input untouched."""
    m = re.match(r'^\s*(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)\s*$', raw)
    if not m:
        return raw
    done, total = float(m.group(1)), float(m.group(2))
    pct = (done / total * 100) if total else 0.0
    done_s = int(done) if done == int(done) else done
    total_s = int(total) if total == int(total) else total
    return f"{pct:.1f}% ({done_s} / {total_s})"


def _format_addstats_value(field_key: str, raw: str) -> str:
    """Normalize a typed value: money -> 1,234.00, counts -> 1,234, completion -> pct. Free-form on parse failure."""
    if field_key == 'completion_rate':
        return _format_completion_rate(raw)
    cleaned = raw.replace(',', '').replace('$', '').strip()
    if field_key in ADDSTATS_MONEY_FIELDS:
        try:
            return f"{float(cleaned):,.2f}"
        except ValueError:
            return raw
    if field_key in ADDSTATS_INT_FIELDS:
        try:
            return f"{int(float(cleaned)):,}"
        except ValueError:
            return raw
    return raw


def parse_stats_message(text: str) -> dict:
    """Parse a full stats message (same format as /stats output) into field values.
    Returns the recognized fields; caller decides if there are enough to treat it
    as a full clone. Lifetime Volume / Total Deals are ignored (auto-computed)."""
    patterns = {
        'total_bought': r'Total Bought:\s*\$?\s*([\d,]+(?:\.\d+)?)',
        'buy_trades': r'Total Buy Trades:\s*([\d,]+)',
        'total_sold': r'Total Sold:\s*\$?\s*([\d,]+(?:\.\d+)?)',
        'sell_trades': r'Total Sell Trades:\s*([\d,]+)',
        'global_rank': r'Global Rank:\s*#?\s*(\d[\d,]*)',
    }
    result = {}
    for key, pat in patterns.items():
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            result[key] = _format_addstats_value(key, m.group(1))
    m = re.search(r'Completion Rate:\s*(.+)', text, re.IGNORECASE)
    if m:
        result['completion_rate'] = m.group(1).strip()
    return result


def _addstats_lifetime(d: dict):
    """Auto lifetime volume = Total Bought + Total Sold; '—' until either is set."""
    if 'total_bought' not in d and 'total_sold' not in d:
        return None
    return f"{_money_to_float(d.get('total_bought')) + _money_to_float(d.get('total_sold')):,.2f}"


def _addstats_total_deals(d: dict):
    """Auto total deals = Total Buy Trades + Total Sell Trades; '—' until either is set."""
    if 'buy_trades' not in d and 'sell_trades' not in d:
        return None
    return f"{int(_money_to_float(d.get('buy_trades')) + _money_to_float(d.get('sell_trades'))):,}"


def build_addstats_text(session: dict) -> str:
    """Render the (partially) filled stats sample message from session data."""
    d = session['data']
    money = lambda k: f"${d[k]}" if k in d else "—"
    val = lambda k: f"{d[k]}" if k in d else "—"
    lifetime = _addstats_lifetime(d)
    lifetime_str = f"${lifetime}" if lifetime is not None else "—"
    total_deals = _addstats_total_deals(d)
    total_deals_str = f"{total_deals}" if total_deals is not None else "—"
    return (
        f"<blockquote expandable>📊 {session['display']} — Stats\n"
        f"🟢 BUYING STATS\n"
        f"• Total Bought: {money('total_bought')}\n"
        f"• Total Buy Trades: {val('buy_trades')}\n"
        f"\n"
        f"🔴 SELLING STATS\n"
        f"• Total Sold: {money('total_sold')}\n"
        f"• Total Sell Trades: {val('sell_trades')}</blockquote>\n"
        f"\n"
        f"📈 OVERALL PERFORMANCE\n"
        f"• Lifetime Volume: {lifetime_str}\n"
        f"• Total Deals: {total_deals_str}\n"
        f"• Completion Rate: {val('completion_rate')}\n"
        f"🏆 Overall Global Rank: #{val('global_rank')} Trader"
    )


def build_stats_from_manual(display: str, m: dict) -> str:
    """Render a finalized stats message from persisted manual stats (empty -> zero defaults)."""
    money = lambda k: m.get(k) or "0.00"
    num = lambda k: m.get(k) or "0"
    return (
        f"<blockquote expandable>📊 {display} — Stats\n"
        f"🟢 BUYING STATS\n"
        f"• Total Bought: ${money('total_bought')}\n"
        f"• Total Buy Trades: {num('buy_trades')}\n"
        f"\n"
        f"🔴 SELLING STATS\n"
        f"• Total Sold: ${money('total_sold')}\n"
        f"• Total Sell Trades: {num('sell_trades')}</blockquote>\n"
        f"\n"
        f"📈 OVERALL PERFORMANCE\n"
        f"• Lifetime Volume: ${money('lifetime_volume')}\n"
        f"• Total Deals: {num('total_deals')}\n"
        f"• Completion Rate: {m.get('completion_rate') or '0.0% (0 / 0)'}\n"
        f"🏆 Overall Global Rank: #{num('global_rank')} Trader"
    )


def _addstats_save_data(session: dict) -> dict:
    """Build the dict persisted to manual_stats, including auto lifetime volume."""
    d = session['data']
    save = {k: d.get(k, '') for k in (
        'total_bought', 'buy_trades', 'total_sold', 'sell_trades',
        'completion_rate', 'global_rank',
    )}
    lifetime = _addstats_lifetime(d)
    save['lifetime_volume'] = lifetime if lifetime is not None else ''
    total_deals = _addstats_total_deals(d)
    save['total_deals'] = total_deals if total_deals is not None else ''
    return save


def _persist_addstats(session: dict) -> None:
    """Save the current session's stats to the DB if we know the target user id."""
    target_id = session.get('target_user_id')
    if not target_id:
        return
    database.save_manual_stats(target_id, session.get('target_username'), _addstats_save_data(session))


def build_addstats_keyboard() -> InlineKeyboardMarkup:
    """Section buttons shown under the /addstats sample message."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🟢 Buying Stats', callback_data='addstats:buying')],
        [InlineKeyboardButton('🔴 Selling Stats', callback_data='addstats:selling')],
        [InlineKeyboardButton('📈 Overall Performance', callback_data='addstats:overall')],
        [InlineKeyboardButton('✅ Done', callback_data='addstats:done')],
    ])


async def addstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /addstats command - admin-only interactive stats builder"""
    user = update.effective_user
    if user.id not in ADMIN_USER_IDS:
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return

    # Resolve the target user id + username. Stats are stored per Telegram user id.
    # Priority: reply to a message  >  /addstats @username | <user_id>  >  the admin themself.
    target_user_id = None
    target_username = None
    replied = update.message.reply_to_message.from_user if (update.message and update.message.reply_to_message) else None
    if replied:
        target_user_id = replied.id
        target_username = replied.username
    elif context.args:
        arg = context.args[0].strip()
        if arg.lstrip('-').isdigit():
            target_user_id = int(arg)
        else:
            target_username = arg.lstrip('@')
            target_user_id = database.get_user_id_by_username(target_username)
    else:
        target_user_id = user.id
        target_username = user.username

    display = f"@{target_username}" if target_username else (f"ID {target_user_id}" if target_user_id else "@username")

    # Prefill from any previously-saved stats for this user so edits build on them.
    existing = database.get_manual_stats(target_user_id) if target_user_id else None
    data = {}
    if existing:
        for k in ('total_bought', 'buy_trades', 'total_sold', 'sell_trades', 'total_deals', 'completion_rate', 'global_rank'):
            if existing.get(k):
                data[k] = existing[k]

    session = {
        'chat_id': update.effective_chat.id,
        'display': display,
        'data': data,
        'awaiting': None,
        'section': None,
        'target_user_id': target_user_id,
        'target_username': target_username,
    }
    msg = await update.message.reply_text(
        build_addstats_text(session),
        parse_mode='HTML',
        reply_markup=build_addstats_keyboard(),
    )
    session['message_id'] = msg.message_id
    addstats_sessions[(update.effective_chat.id, user.id)] = session
    logger.info(f"🧾 /addstats started by {user.id} for target {target_user_id} (@{target_username}) in chat {update.effective_chat.id}")

    if not target_user_id:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=("⚠️ I don't know this user's Telegram id yet, so these stats won't show in /stats. "
                  "Reply to one of their messages with /addstats, or pass their numeric id."),
        )
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=("💡 Tip: tap a section to fill values one-by-one, or just paste a full stats "
                  "message (same format) and I'll clone all values to this user."),
        )


async def addadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /addadmin command - admins add other bot admins (persisted)."""
    user = update.effective_user
    if user.id not in ADMIN_USER_IDS:
        return

    # Resolve target: reply to a message  >  /addadmin @username | <user_id>.
    target_user_id = None
    target_username = None
    replied = update.message.reply_to_message.from_user if (update.message and update.message.reply_to_message) else None
    if replied:
        target_user_id = replied.id
        target_username = replied.username
    elif context.args:
        arg = context.args[0].strip()
        if arg.lstrip('-').isdigit():
            target_user_id = int(arg)
        else:
            target_username = arg.lstrip('@')
            target_user_id = database.get_user_id_by_username(target_username)
    else:
        await update.message.reply_text(
            "❌ Usage: reply to a user with /addadmin, or /addadmin @username | <user_id>."
        )
        return

    if not target_user_id:
        await update.message.reply_text(
            "⚠️ I don't know that user's Telegram id yet. Reply to one of their messages "
            "with /addadmin, or pass their numeric id."
        )
        return

    if target_user_id in ADMIN_USER_IDS:
        await update.message.reply_text("ℹ️ That user is already an admin.")
        return

    ADMIN_USER_IDS.add(target_user_id)
    database.add_bot_admin(target_user_id, target_username, user.id)
    display = f"@{target_username}" if target_username else f"id {target_user_id}"
    await update.message.reply_text(f"✅ {display} [<code>{target_user_id}</code>] is now a bot admin.", parse_mode='HTML')
    logger.info(f"👑 Admin {user.id} added new admin {target_user_id} (@{target_username})")


async def _prune_departed_members(context: ContextTypes.DEFAULT_TYPE, groups: list) -> None:
    """Drop members who are no longer in the P2P ROOM group (verified live via getChatMember)."""
    bot_id = context.bot.id
    for g in groups:
        # If the bot itself is the adder, drop the whole group (auto-adds aren't tracked)
        if g['added_by'] == bot_id:
            for m in list(g['members']):
                database.remove_added_member(m['id'])
            g['members'].clear()
            continue
        for m in list(g['members']):
            # Always exclude the bot itself, regardless of any API check
            if m['id'] == bot_id:
                database.remove_added_member(m['id'])
                g['members'].remove(m)
                continue
            try:
                cm = await context.bot.get_chat_member(P2P_ROOM_GROUP_ID, m['id'])
                if cm.status in ("left", "kicked") or cm.user.is_bot:
                    database.remove_added_member(m['id'])
                    g['members'].remove(m)
            except Exception as e:
                # "user not found" / "member not found" => they were never/no longer in the group
                if any(s in str(e).lower() for s in ("not found", "user_id_invalid", "participant")):
                    database.remove_added_member(m['id'])
                    g['members'].remove(m)
                else:
                    logger.info(f"/list membership check skipped for {m['id']}: {e}")


async def build_list_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    """Render the /list message from current added-member records, pruning departed members."""
    star = premium_emoji(PREMIUM_EMOJI_STAR, "📋")
    user_e = premium_emoji(PREMIUM_EMOJI_USER, "👤")

    groups = database.get_added_members_grouped()
    await _prune_departed_members(context, groups)
    groups = [g for g in groups if g['members']]
    if not groups:
        return f"{star} <b><u>Added Members</u></b>\n\nℹ️ No added members recorded yet."

    rows = []
    for g in groups:
        adder_uname = g['added_by_username'] or database.get_username_by_user_id(g['added_by'])
        adder_name = f"@{adder_uname}" if adder_uname else "user"
        count = len(g['members'])
        header = (
            f"{user_e} <b>{adder_name}</b>  [<code>{g['added_by']}</code>]\n"
            f"   <i>added {count} member{'s' if count != 1 else ''}</i>"
        )
        member_lines = []
        for i, m in enumerate(g['members']):
            branch = "┗" if i == len(g['members']) - 1 else "┣"
            m_uname = m['username'] or database.get_username_by_user_id(m['id'])
            m_name = f"@{m_uname}" if m_uname else "user"
            member_lines.append(f"   {branch} <b>{m_name}</b>  [<code>{m['id']}</code>]")
        rows.append(header + "\n" + "\n".join(member_lines))

    return f"{star} <b><u>Added Members</u></b>\n\n" + "\n\n".join(rows)


def build_list_keyboard() -> InlineKeyboardMarkup:
    """Keyboard with an Update button for the /list message."""
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Update", callback_data="list:update")]])


def _strip_custom_emoji(text: str) -> str:
    """Replace <tg-emoji ...>X</tg-emoji> with its fallback X (for clients/bots without custom emoji)."""
    return re.sub(r'<tg-emoji[^>]*>(.*?)</tg-emoji>', r'\1', text)


async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /list command - admins see who added which members (grouped by adder)."""
    user = update.effective_user
    if user.id not in ADMIN_USER_IDS:
        return

    text = await build_list_text(context)
    try:
        await update.message.reply_text(text, parse_mode='HTML', reply_markup=build_list_keyboard())
    except Exception as e:
        # Custom emoji can be rejected if the bot can't use it; retry with plain fallback emoji
        logger.info(f"/list send fell back to plain emoji: {e}")
        await update.message.reply_text(
            _strip_custom_emoji(text), parse_mode='HTML', reply_markup=build_list_keyboard()
        )


async def handle_list_callback(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the Update button on the /list message."""
    if query.from_user.id not in ADMIN_USER_IDS:
        await query.answer("Admins only.", show_alert=True)
        return

    text = await build_list_text(context)
    kb = build_list_keyboard()
    try:
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=kb)
        await query.answer("Updated ✅")
    except Exception as e:
        msg = str(e).lower()
        if "not modified" in msg:
            await query.answer("No changes.")
            return
        # Custom emoji rejected or other HTML issue -> retry with plain fallback emoji
        try:
            await query.edit_message_text(_strip_custom_emoji(text), parse_mode='HTML', reply_markup=kb)
            await query.answer("Updated ✅")
        except Exception as e2:
            m2 = str(e2).lower()
            await query.answer("No changes." if "not modified" in m2 else "Couldn't update, try again.")
            logger.warning(f"/list update failed: {e2}")


def _resolve_user_token(token: str):
    """Resolve '@username' | 'username' | '<user_id>' to (user_id_or_None, username_or_None)."""
    t = token.lstrip('@').strip()
    if t.lstrip('-').isdigit():
        return int(t), None
    return database.get_user_id_by_username(t), t


async def a_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /a command - admins manually record an added member for /list.

    /a @member @adder   -> record that @member was added by @adder
    /a @member          -> adder defaults to the command sender
    /a (reply)          -> member is the replied-to user, adder defaults to sender
    Numeric user ids are accepted in place of @username.
    """
    user = update.effective_user
    if user.id not in ADMIN_USER_IDS:
        return

    args = context.args or []
    replied = update.message.reply_to_message.from_user if (update.message and update.message.reply_to_message) else None

    # Resolve the member (who was added) and the adder token (who added them)
    if replied:
        member_id = replied.id
        member_username = replied.username
        adder_token = args[0] if args else None
    else:
        if not args:
            await update.message.reply_text(
                "❌ Usage: /a @member [@adder]  — or reply to the member's message with /a.\n"
                "You can also pass numeric user ids."
            )
            return
        member_id, member_username = _resolve_user_token(args[0])
        adder_token = args[1] if len(args) > 1 else None

    if not member_id:
        await update.message.reply_text(
            "⚠️ I don't know that user's Telegram id yet. Reply to their message with /a, "
            "or pass their numeric id: /a <member_id> [adder_id]."
        )
        return

    # Resolve the adder (defaults to the command sender)
    if adder_token:
        adder_id, adder_username = _resolve_user_token(adder_token)
        if not adder_id:
            await update.message.reply_text(
                "⚠️ I don't know the adder's Telegram id yet. Pass their numeric id as the second value."
            )
            return
    else:
        adder_id = user.id
        adder_username = user.username

    member_username = member_username or database.get_username_by_user_id(member_id)
    adder_username = adder_username or database.get_username_by_user_id(adder_id)
    database.record_added_member(member_id, member_username, adder_id, adder_username)
    m_disp = f"@{member_username}" if member_username else "user"
    a_disp = f"@{adder_username}" if adder_username else "user"
    await update.message.reply_text(
        f"✅ Recorded: {m_disp} [<code>{member_id}</code>] added by {a_disp} [<code>{adder_id}</code>].",
        parse_mode='HTML',
    )
    logger.info(f"📝 /a by {user.id}: member {member_id} added by {adder_id}")


async def handle_addstats_callback(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle taps on the /addstats section buttons."""
    key = (query.message.chat.id, query.from_user.id)
    session = addstats_sessions.get(key)
    if not session:
        await query.answer("This stats builder expired. Send /addstats again.", show_alert=True)
        return

    action = query.data.split(':', 1)[1]

    if action == 'done':
        _persist_addstats(session)
        await query.answer("Saved ✅")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        addstats_sessions.pop(key, None)
        return

    if action not in ADDSTATS_SECTIONS:
        await query.answer()
        return

    section_label, fields = ADDSTATS_SECTIONS[action]
    session['section'] = action
    session['awaiting'] = fields[0][0]
    await query.answer(f"Filling {section_label}")
    prompt = await context.bot.send_message(
        chat_id=session['chat_id'],
        text=f"✍️ <b>{section_label}</b>\n\n{fields[0][1]}",
        parse_mode='HTML',
    )
    session['prompt_msg_id'] = prompt.message_id


async def process_addstats_input(update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict) -> None:
    """Store an entered line, edit the sample message, and ask for the next line."""
    field_key = session['awaiting']
    section = session['section']
    session['data'][field_key] = _format_addstats_value(field_key, update.message.text.strip())

    # Edit the sample message in place with the newly provided information
    try:
        await context.bot.edit_message_text(
            chat_id=session['chat_id'],
            message_id=session['message_id'],
            text=build_addstats_text(session),
            parse_mode='HTML',
            reply_markup=build_addstats_keyboard(),
        )
    except Exception as e:
        logger.warning(f"Could not edit /addstats sample message: {e}")

    # Persist after each entered line so stats survive even without tapping Done
    _persist_addstats(session)

    # Remove the admin's input message to keep the chat clean
    try:
        await update.message.delete()
    except Exception:
        pass

    # Remove the bot's prompt message now that we've received the response
    prompt_msg_id = session.pop('prompt_msg_id', None)
    if prompt_msg_id:
        try:
            await context.bot.delete_message(chat_id=session['chat_id'], message_id=prompt_msg_id)
        except Exception:
            pass

    fields = ADDSTATS_SECTIONS[section][1]
    field_keys = [f[0] for f in fields]
    idx = field_keys.index(field_key)

    if idx + 1 < len(fields):
        session['awaiting'] = fields[idx + 1][0]
        prompt = await context.bot.send_message(
            chat_id=session['chat_id'],
            text=f"✍️ {fields[idx + 1][1]}",
            parse_mode='HTML',
        )
        session['prompt_msg_id'] = prompt.message_id
    else:
        session['awaiting'] = None
        session['section'] = None
        await context.bot.send_message(
            chat_id=session['chat_id'],
            text="✅ Section updated! Tap another section button to continue, or ✅ Done to finish.",
        )


async def apply_addstats_clone(update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict, parsed: dict) -> None:
    """Apply a pasted full stats message to the session's target and save it."""
    session['data'].update(parsed)
    session['awaiting'] = None
    session['section'] = None

    _persist_addstats(session)

    # Edit the sample message in place with the cloned values
    try:
        await context.bot.edit_message_text(
            chat_id=session['chat_id'],
            message_id=session['message_id'],
            text=build_addstats_text(session),
            parse_mode='HTML',
            reply_markup=build_addstats_keyboard(),
        )
    except Exception as e:
        logger.warning(f"Could not edit /addstats sample message on clone: {e}")

    # Remove the admin's pasted message to keep the chat clean
    try:
        await update.message.delete()
    except Exception:
        pass

    if session.get('target_user_id'):
        await context.bot.send_message(
            chat_id=session['chat_id'],
            text="✅ Stats cloned to the target user. Tap ✅ Done to finish, or a section to edit.",
        )
    else:
        await context.bot.send_message(
            chat_id=session['chat_id'],
            text=("⚠️ Stats parsed, but I don't know this user's Telegram id yet, so they won't show in /stats. "
                  "Restart with a reply to their message or their numeric id."),
        )


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /stats command - show trading stats for any user (available to everyone)"""
    user = update.effective_user
    logger.info(f"📊 /stats command by user {user.id} (@{user.username})")

    # Capture the replied-to user (if any) before deleting the command message
    replied_user = None
    if update.message and update.message.reply_to_message:
        replied_user = update.message.reply_to_message.from_user

    # Delete the /stats command message once received
    try:
        await update.message.delete()
        logger.info(f"🗑️ Deleted /stats command message from user {user.id}")
    except Exception as e:
        logger.warning(f"Could not delete /stats command message: {e}")

    # Target resolution order: /stats @username  >  reply to a message  >  caller
    target_arg = context.args[0].strip() if context.args else None
    target_user_id = None
    if target_arg:
        target_username = target_arg.lstrip('@')
        display = f"@{target_username}"
        lookup = target_username
        target_user_id = database.get_user_id_by_username(target_username)
    elif replied_user:
        lookup = replied_user.username or replied_user.full_name or str(replied_user.id)
        display = f"@{replied_user.username}" if replied_user.username else lookup
        target_user_id = replied_user.id
    else:
        lookup = user.username or user.full_name or str(user.id)
        display = f"@{user.username}" if user.username else lookup
        target_user_id = user.id

    # Manually-set stats (via /addstats) take priority over computed deal stats
    manual = database.get_manual_stats(target_user_id) if target_user_id else None
    if manual:
        stats_text = build_stats_from_manual(display, manual)
    else:
        # Prefer computing by Telegram user id (can't be hijacked via username theft);
        # fall back to username matching only when the id is unknown.
        if target_user_id:
            stats = database.get_user_stats_by_id(target_user_id)
        else:
            stats = database.get_user_stats(lookup)
        stats_text = (
            f"<blockquote expandable>📊 {display} — Stats\n"
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
    
    # Build escrow addresses from owner and CEO wallets, per token.
    # An address maps to the set of tokens it serves so /verify can say
    # specifically USDT or USDC (or USDT/USDC if one address serves both).
    escrow_addresses = {}

    def _register(addr, token, chain, wtype):
        if not addr or addr == "T0000000000000000000000000000000000":
            return
        key = addr.lower()
        entry = escrow_addresses.setdefault(key, {"tokens": set(), "chain": chain, "type": wtype})
        entry["tokens"].add(token)

    for wtype, getter in (("Owner", get_owner_wallet), ("CEO", get_ceo_wallet)):
        _register(getter('BSC', 'USDT'), "USDT", "BSC", wtype)
        _register(getter('BSC', 'USDC'), "USDC", "BSC", wtype)
        _register(getter('TRON', 'USDT'), "USDT", "TRON", wtype)

    # Backup escrow addresses set per room via /setfakeaddy are also this bot's
    # deposit addresses, so they verify too (each is tied to one room number).
    backup_rooms = database.get_backup_wallet_rooms(address_to_verify)
    for entry in backup_rooms:
        _register(address_to_verify, entry.get('token') or 'USDT', "BSC", "Backup")

    if address_to_verify in escrow_addresses:
        info = escrow_addresses[address_to_verify]
        info = {"token": "/".join(sorted(info["tokens"])), "chain": info["chain"], "type": info["type"]}

        # Find which active deal/room this address belongs to. Prefer the
        # requester's own deal; fall back to the latest active deal with that
        # address so the room still shows for non-participants (e.g. admins).
        deal = database.get_active_deal_by_address_for_user(
            address_to_verify, user_id=user.id, username=user.username
        )
        if not deal:
            deal = database.get_active_deal_by_address(address_to_verify)
        group_line = ""
        # A backup address belongs to exactly one room number, so use it directly.
        if not deal and len(backup_rooms) == 1:
            group_line = f"\nGroup: MM ROOM {backup_rooms[0].get('room_number')}"
        if deal:
            room_name = deal.get('room_name')
            room_number = deal.get('room_number')
            group = None
            if room_name:
                group = room_name if (room_number is None or str(room_number) in str(room_name)) else f"{room_name} {room_number}"
            elif room_number is not None:
                group = f"MM ROOM {room_number}"
            if group:
                group_line = f"\nGroup: {group}"

        verified_text = f"""✅ Address verified

Token: {info['token']}
Chain: {info['chain']}{group_line}"""
        await update.effective_chat.send_message(verified_text, parse_mode='HTML')
        logger.info(f"✅ Address verified for user {user.id}: {address_to_verify} ({info['token']} on {info['chain']}) group={group_line.strip() or None}")
    else:
        warning_text = """⚠️ <b>WARNING:</b> Address Not Verified

❌ This address does <b>NOT</b> belong to this bot.

<b>🚫 DO NOT send funds to this address!</b>"""
        await update.effective_chat.send_message(warning_text, parse_mode='HTML')
        logger.warning(f"⚠️ Address NOT verified for user {user.id}: {address_to_verify}")


async def close_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /close command - admin only. Marks the deal closed then permanently deletes the group."""
    user = update.effective_user

    # Silently ignore unauthorized users
    if user.id not in ADMIN_USER_IDS:
        return

    chat_id = update.effective_chat.id

    # Resolve the deal chat_id (state key). Prefer an explicit numeric argument,
    # otherwise use the room this command was run in.
    original_chat_id = None
    if context.args and context.args[0].strip().lstrip('-').isdigit():
        arg = int(context.args[0].strip())
        original_chat_id = abs(arg) - 1000000000000 if arg < 0 else arg
    elif chat_id < 0:
        original_chat_id = abs(chat_id) - 1000000000000
    else:
        await update.message.reply_text(
            "❌ Run /close inside the deal room, or use /close <room_chat_id>."
        )
        return

    send_chat_id = -1000000000000 - original_chat_id

    deal = database.get_deal(original_chat_id)
    room_name = (deal.get('room_name') if deal else None) or f"Room {original_chat_id}"

    # Determine if the deposit was already confirmed (before we cancel the deal).
    status = deal.get('deal_status') if deal else None
    deposit_confirmed = (
        original_chat_id in room_confirmed_deposits
        or status in (
            database.DEAL_STATUS_DEPOSIT_RECEIVED,
            database.DEAL_STATUS_RELEASE_PENDING,
            database.DEAL_STATUS_COMPLETED,
        )
    )

    # Step 1: mark the deal closed
    database.cancel_deal(original_chat_id)
    logger.info(f"🔒 Admin {user.id} closed deal {original_chat_id} ({room_name})")

    # Update the room log message: completed if deposit was confirmed,
    # otherwise replace the whole log with a bold "Deal Cancelled !".
    if deposit_confirmed:
        await update_room_log_status(context.bot, original_chat_id, "Deal Completed!")
    else:
        await overwrite_room_log(context.bot, original_chat_id, "<b>Deal Cancelled !</b>")

    # Notify the room that it's being closed
    try:
        await context.bot.send_message(
            chat_id=send_chat_id,
            text="🔒 <b>Deal closed by admin.</b>",
            parse_mode='HTML'
        )
    except Exception as e:
        logger.warning(f"Could not send close notification to room {original_chat_id}: {e}")

    # Clean up state for this room. The room record is dropped first so the
    # room-watcher does not immediately re-send the welcome messages.
    remove_room_record(original_chat_id)
    clear_room_state(original_chat_id)

    # Step 2: hand the room back to the premade pool - the userbot kicks the
    # normal members and wipes the history, the group itself is kept.
    room_number = (deal.get('room_number') if deal else None) or room_number_from_name(room_name)
    write_release_request(original_chat_id, room_number, room_name)
    logger.info(f"♻️ Requested pool release of {room_name} (chat_id: {original_chat_id})")


RESETROOMS_USER_ID = 6643621069


async def resetrooms_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Close ALL active deals and delete their groups. Restricted to a single id."""
    user = update.effective_user

    # Restricted to one specific user id
    if not user or user.id != RESETROOMS_USER_ID:
        return

    deals = database.get_active_deals()
    if not deals:
        await update.message.reply_text("ℹ️ No active deals to reset.")
        return

    count = 0
    for deal in deals:
        try:
            original_chat_id = deal.get('chat_id')
            if original_chat_id is None:
                continue
            room_name = deal.get('room_name') or f"Room {original_chat_id}"

            # Mark closed
            database.cancel_deal(original_chat_id)

            # Notify the room
            send_chat_id = -1000000000000 - original_chat_id
            try:
                await context.bot.send_message(
                    chat_id=send_chat_id,
                    text="🔒 <b>Deal closed by admin.</b>",
                    parse_mode='HTML'
                )
            except Exception as e:
                logger.warning(f"Could not notify room {original_chat_id} on reset: {e}")

            remove_room_record(original_chat_id)
            clear_room_state(original_chat_id)

            # Return the room to the premade pool (members kicked, history wiped)
            room_number = deal.get('room_number') or room_number_from_name(room_name)
            write_release_request(original_chat_id, room_number, room_name)
            count += 1
        except Exception as e:
            logger.warning(f"Could not reset deal {deal.get('chat_id')}: {e}")

    logger.info(f"🧹 /resetrooms by {user.id}: closed {count} active deals")
    await update.message.reply_text(f"✅ Closed {count} active room(s) and returned them to the pool.")


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle button presses"""
    query = update.callback_query
    # Note: Don't call query.answer() here - each branch handles its own answer
    # to avoid "Query is too old" errors from duplicate answers
    
    user_id = query.from_user.id
    username = query.from_user.username or query.from_user.first_name
    
    # Handle /addstats section buttons (admin stats builder)
    if query.data.startswith('addstats:'):
        await handle_addstats_callback(query, context)
        return

    # Handle the Cancel button on the /startroom progress message
    if query.data.startswith('startroom:cancel:'):
        await query.answer("Cancelling…")
        if user_id == CEO_USER_ID:
            cancel_prewarm_request(query.data.split(':', 2)[2])
        return

    # Handle /list Update button
    if query.data == 'list:update':
        await handle_list_callback(query, context)
        return

    # Handle /setownerwallet & /setceowallet token choice (USDT/USDC)
    if query.data.startswith('setwallet:'):
        await handle_setwallet_callback(query, context)
        return

    # Handle /setaddy Owner/CEO choice
    if query.data.startswith('setaddy:'):
        await handle_setaddy_callback(query, context)
        return

    # Handle /setfakeaddy USDT/USDC choice
    if query.data.startswith('setfakeaddy:'):
        await handle_setfakeaddy_callback(query, context)
        return
    
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
            confirmed_text = f"""<b>RELEASE CONFIRMATION (Partial)</b>

✅ @{seller_username} - Confirmed
✅ @{buyer_username} - Confirmed

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
            
            # Step 2: send the Deal Complete card a few seconds later. Scheduled
            # before anything that can raise, so the card always goes out.
            buyer_addr = (
                buyer_addresses.get(original_chat_id)
                or (deal_data.get('buyer_address') if deal_data else None)
                or "0xUnknown"
            )
            chain = (deal_data.get('network') if deal_data else None) or user_blockchain.get(original_chat_id, 'BSC')
            if chain == 'TRON':
                tx_url = f"https://tronscan.org/#/address/{buyer_addr}"
            else:  # BSC
                tx_url = f"https://bscscan.com/address/{buyer_addr}"
            
            duration = format_deal_duration(deal_data.get('created_at') if deal_data else None)
            if duration == "N/A" and original_chat_id in room_creation_times:
                duration = format_deal_duration(
                    datetime.fromtimestamp(room_creation_times[original_chat_id])
                )
            schedule_task(send_deal_complete_message(context.bot, original_chat_id, tx_url, duration))
            
            amount = 0.0
            try:
                amount = float(deal_data.get('amount') or 0) if deal_data else 0.0
            except (TypeError, ValueError):
                amount = 0.0
            coin = (deal_data.get('coin') if deal_data else None) or user_coins.get(original_chat_id, 'USDT')
            
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
                    chat_id=-1004433511813,
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
                            await kick_member(context.bot, send_chat_id, buyer_id)
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
                            await kick_member(context.bot, send_chat_id, seller_id)
                            logger.info(f"✅ Kicked seller {seller_username} from room {chat_id}")
                        else:
                            logger.warning(f"⚠️ No user ID found for seller {seller_username}")
                    except Exception as e:
                        logger.warning(f"Could not kick seller: {e}")
                
                # Mark the deal completed and hand the room back to the pool
                deal = database.get_deal(chat_id)
                database.complete_deal(chat_id)
                await update_room_log_status(context.bot, chat_id, "Deal Completed!")
                room_name = (deal.get('room_name') if deal else None) or f"Room {chat_id}"
                room_number = (deal.get('room_number') if deal else None) or room_number_from_name(room_name)
                remove_room_record(chat_id)
                clear_room_state(chat_id)
                write_release_request(chat_id, room_number, room_name)
                
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

            # Answered first so the button stops spinning while the next step is sent
            await query.answer("✅ Blockchain: BSC selected")

            # Save blockchain to database
            database.set_network(chat_id, 'BSC')
            
            # Update the button to show selection (no selection indicator)
            try:
                await query.edit_message_caption(
                    caption="<b>STEP 2 - CHOOSE BLOCKCHAIN</b>",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("BSC", callback_data=f"blockchain_bsc_{chat_id}_done")
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

            return CHOOSING
            
        except Exception as e:
            logger.error(f"❌ Error handling blockchain selection: {e}", exc_info=True)
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

            # Answered first so the button stops spinning while the next step is sent
            await query.answer(f"✅ Coin selected: {coin_type}")

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
                    caption="<b>STEP 3 - SELECT COIN</b>",
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
            
            # Send Step 4 (amount entry) message
            if chat_id not in step4_amount_messages_sent:
                logger.info(f"📨 Sending Step 4 (amount entry) message to room {chat_id}")
                await send_step4_amount_message(context.bot, send_chat_id, chat_id)
                logger.info(f"✅ Step 4 sent for room {chat_id}")
            else:
                logger.info(f"⏩ Step 4 already sent for room {chat_id}, skipping")

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
                    network_fee = NETWORK_FEE_BSC
                
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
                    deposit_token = coin_type.upper() if coin_type.upper() in ('USDT', 'USDC') else 'USDT'
                    
                    # Use the room's fixed wallet if an admin set one via /setaddy,
                    # otherwise rotate between owner/CEO wallets, per token
                    deposit_address = get_deposit_wallet_for_room(chat_id, blockchain, deposit_token)
                    logger.info(f"🏦 Using deposit wallet for {blockchain}/{deposit_token}: {deposit_address}")
                    
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
            # Cache this user's real Telegram id so set_roles can persist
            # buyer_user_id / seller_user_id (used by /verify and /stats).
            if query.from_user.username:
                save_user_id(query.from_user.username, query.from_user.id)
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
            # Persist so this survives a bot restart
            database.update_deal(chat_id, deal_status=database.DEAL_STATUS_RELEASE_PENDING)
            
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
            network_fee = NETWORK_FEE_BSC
        
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
        step2_text = "<b>STEP 2 - CHOOSE BLOCKCHAIN</b>"
        
        keyboard = [[
            InlineKeyboardButton("BSC", callback_data=f"blockchain_bsc_{chat_id}")
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
        step3_text = "<b>STEP 3 - SELECT COIN</b>"
        
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
        network_fee = NETWORK_FEE_BSC
    
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
    
    # Build approval status: a single line once both parties have approved
    if buyer_approved and seller_approved:
        approval_status = "✅ Both parties have approved."
    else:
        buyer_status = f"✅ @{buyer_username} has approved." if buyer_approved else f"⏳ Waiting for @{buyer_username} to approve."
        seller_status = f"✅ @{seller_username} has approved." if seller_approved else f"⏳ Waiting for @{seller_username} to approve."
        approval_status = f"{buyer_status}\n{seller_status}"
    
    trade_id = database.assign_trade_id(chat_id) or f"{database.TRADE_ID_PREFIX}{database.TRADE_ID_START}"

    deal_text = f"""📋  <b>Deal Summary</b>

• <b>Trade ID:</b> #{trade_id}
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

{approval_status}"""
    
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


def read_room_record(chat_id):
    """A room's entry from deal_rooms.json, or {}."""
    try:
        if os.path.exists(DEAL_ROOMS_FILE):
            with open(DEAL_ROOMS_FILE, 'r') as f:
                return json.load(f).get(str(chat_id)) or {}
    except Exception as e:
        logger.warning(f"Could not read room record {chat_id}: {e}")
    return {}


def mark_room_join(chat_id: int, username: str, user_id: int) -> None:
    """Remember that a participant actually joined the room (for the room log)."""
    joined = room_joined_users.setdefault(chat_id, set())
    if username:
        joined.add(username.lower())
    if user_id:
        joined.add(str(user_id))


def has_joined_room(chat_id: int, username: str, user_id) -> bool:
    joined = room_joined_users.get(chat_id, set())
    if username and username.lower() in joined:
        return True
    return bool(user_id) and str(user_id) in joined


def build_room_log_text(chat_id: int, status: str) -> str:
    """Room log message: room number, both participants' join status, amount, stage."""
    deal = database.get_deal(chat_id) or {}
    room_info = read_room_record(chat_id)
    cached = room_log_messages.get(chat_id, {})

    room_name = deal.get('room_name') or room_info.get('room_name') or cached.get('room_name') or ''
    room_number = (
        deal.get('room_number')
        or room_info.get('room_number')
        or cached.get('room_number')
        or room_number_from_name(room_name)
    )

    initiator = room_info.get('initiator_username') or cached.get('initiator_username') or ''
    counterparty = room_info.get('counterparty_username') or cached.get('counterparty_username') or ''
    counterparty_id = room_info.get('counterparty_user_id') or cached.get('counterparty_user_id')

    initiator_display = f"@{initiator}" if initiator else "Unknown"
    if counterparty:
        counterparty_display = f"@{counterparty}"
    elif counterparty_id:
        counterparty_display = f"User {counterparty_id}"
    else:
        counterparty_display = "Unknown"

    initiator_status = "Joined" if has_joined_room(chat_id, initiator, None) else "Not Joined"
    counterparty_status = "Joined" if has_joined_room(chat_id, counterparty, counterparty_id) else "Not Joined"

    token = deal.get('coin') or 'N/A'
    amount = deal.get('amount') or 'N/A'

    return (
        f"<b>P2P ROOM {room_number if room_number else 'N/A'}</b>\n\n"
        f"• <b>Initiator ({initiator_display}) Status</b> - {initiator_status}\n"
        f"• <b>CounterParty ({counterparty_display}) Status</b> - {counterparty_status}\n"
        f"• <b>Deal Amount[{token}]</b> - {amount}\n"
        f"• <b>Deal Status</b> - {status}"
    )


async def send_room_log_message(bot, chat_id: int, buyer_username: str, seller_username: str, 
                                 token_name: str, blockchain: str, amount: str, status: str) -> None:
    """Send or update the room log message with current status to the logs channel"""
    try:
        # Logs channel ID
        logs_channel_id = -1004433511813
        
        log_text = build_room_log_text(chat_id, status)
        
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
                msg_info['status'] = status
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
            # Cache the participants so the log survives the room record being
            # cleaned up when the deal ends.
            room_info = read_room_record(chat_id)
            room_log_messages[chat_id] = {
                'msg_id': msg.message_id,
                'chat_id': logs_channel_id,
                'status': status,
                'room_name': room_info.get('room_name'),
                'room_number': room_info.get('room_number'),
                'initiator_username': room_info.get('initiator_username'),
                'counterparty_username': room_info.get('counterparty_username'),
                'counterparty_user_id': room_info.get('counterparty_user_id')
            }
            logger.info(f"✅ Sent room log message to logs channel for room {chat_id}")
    
    except Exception as e:
        logger.warning(f"❌ Failed to send/update room log message: {e}")


async def update_room_log_status(bot, chat_id: int, status: str = None) -> None:
    """Re-render the room log message, optionally with a new deal status."""
    try:
        if chat_id not in room_log_messages:
            logger.warning(f"⚠️ No room log message found for room {chat_id}")
            return

        msg_info = room_log_messages[chat_id]
        if status is None:
            status = msg_info.get('status', 'Room Assigned')

        log_text = build_room_log_text(chat_id, status)

        await bot.edit_message_text(
            chat_id=msg_info['chat_id'],
            message_id=msg_info['msg_id'],
            text=log_text,
            parse_mode='HTML'
        )
        msg_info['status'] = status
        logger.info(f"✅ Updated room log status for room {chat_id} - Status: {status}")
    
    except Exception as e:
        logger.warning(f"❌ Failed to update room log status: {e}")


async def overwrite_room_log(bot, chat_id: int, text: str) -> None:
    """Replace the entire room log message with the given text."""
    try:
        if chat_id not in room_log_messages:
            logger.warning(f"⚠️ No room log message found for room {chat_id}")
            return
        msg_info = room_log_messages[chat_id]
        await bot.edit_message_text(
            chat_id=msg_info['chat_id'],
            message_id=msg_info['msg_id'],
            text=text,
            parse_mode='HTML'
        )
        logger.info(f"✅ Overwrote room log for room {chat_id}")
    except Exception as e:
        logger.warning(f"❌ Failed to overwrite room log: {e}")


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
    
    # Route input into an active /addstats builder session (admin stats builder)
    _addstats_session = addstats_sessions.get((chat_id, user_id))
    if _addstats_session:
        # A pasted full stats message (3+ recognized fields) clones everything at once
        _parsed_stats = parse_stats_message(text)
        if len(_parsed_stats) >= 3:
            await apply_addstats_clone(update, context, _addstats_session, _parsed_stats)
            return
        if _addstats_session.get('awaiting'):
            await process_addstats_input(update, context, _addstats_session)
            return
    
    # An admin sending the backup address requested by /setfakeaddy
    _pending_fake_addy = pending_fake_addy.get(user_id)
    if _pending_fake_addy:
        await process_fake_addy_address(update, _pending_fake_addy)
        return

    # An admin sending the login code / 2FA password requested by /newubot
    _pending_userbot_login = pending_userbot_login.get(user_id)
    if _pending_userbot_login:
        await process_userbot_login_input(update, _pending_userbot_login)
        return

    # Handle -kick command (admin-only, removes a member from the monitored group)
    if text.startswith('-kick'):
        await dash_kick_command(update, context)
        return
    
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
            # Accept any free-text payment method
            payment_method = text.strip()
            if not payment_method:
                await update.message.reply_text("❌ Please enter a payment method")
                return
            
            user_payment_methods[user_id] = payment_method
            logger.info(f"✅ User {user.username} entered payment method: {payment_method} in room {original_chat_id}")
            
            # Save payment method to database
            database.set_payment_method(original_chat_id, payment_method)
            
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
                    
                    image_path = "step7_seller_address_image.jpg"
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
                    fb_token = coin.upper() if coin and coin.upper() in ('USDT', 'USDC') else 'USDT'
                    escrow_address = get_owner_wallet(blockchain, fb_token) if blockchain else None
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
                    room_confirmed_deposits[original_chat_id] = float(amount)
                    room_used_tx_hashes.setdefault(original_chat_id, set()).add(tx_hash.lower())
                    database.update_deal(original_chat_id, deposit_amount=float(amount))
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
                    room_confirmed_deposits[original_chat_id] = float(received_amount)
                    room_used_tx_hashes.setdefault(original_chat_id, set()).add(tx_hash.lower())
                    database.update_deal(original_chat_id, deposit_amount=float(received_amount))
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

        # Extra deposit requested via /add - verify and top up the escrow balance
        elif room_transaction_state.get(original_chat_id) == 'awaiting_add_hash':
            # Only the seller provides the transaction hash
            if original_chat_id in room_initiators:
                seller_username = room_initiators[original_chat_id].get('seller')
                if seller_username and user.username and user.username.lower() != seller_username.lower():
                    logger.info(f"⏭️ Ignoring /add hash from {user.username} (not seller) in room {original_chat_id}")
                    return

            send_chat_id = -1000000000000 - original_chat_id

            try:
                tx_input = text.strip()
                if 'bscscan.com/tx/' in tx_input or 'etherscan.io/tx/' in tx_input:
                    tx_hash = tx_input.split('tx/')[-1].split('?')[0].strip()
                    logger.info(f"🔗 Extracted hash from link: {tx_hash[:10]}...")
                else:
                    tx_hash = tx_input

                try:
                    await update.message.delete()
                except Exception:
                    pass

                deal_data = database.get_deal(original_chat_id)
                escrow_address = deal_data.get('escrow_address') if deal_data else None
                if not escrow_address:
                    logger.error(f"❌ Escrow not found for room {original_chat_id}")
                    await context.bot.send_message(
                        chat_id=send_chat_id,
                        text="❌ Escrow address not found. Please contact support."
                    )
                    return

                blockchain = (
                    user_blockchain.get(original_chat_id)
                    or (deal_data.get('network') if deal_data else None)
                    or 'BSC'
                )
                coin = (
                    user_coins.get(original_chat_id)
                    or (deal_data.get('coin') if deal_data else None)
                    or 'USDT'
                )

                # A hash may only be credited to a room once
                used_hashes = room_used_tx_hashes.setdefault(original_chat_id, set())
                if deal_data and deal_data.get('tx_hash'):
                    used_hashes.add(deal_data['tx_hash'].lower())
                if tx_hash.lower() in used_hashes:
                    await context.bot.send_message(
                        chat_id=send_chat_id,
                        text="❌ This transaction hash has already been credited to this deal."
                    )
                    logger.warning(f"❌ Duplicate /add hash in room {original_chat_id}: {tx_hash[:10]}...")
                    return

                if blockchain == 'TRON':
                    verify_result = await verify_transaction_tron(tx_hash, escrow_address, token=coin)
                else:
                    verify_result = await verify_transaction_bscscan(tx_hash, escrow_address, token=coin)

                if not verify_result['valid']:
                    error_msg = verify_result['error'] or "❌ Transaction verification failed"
                    await context.bot.send_message(
                        chat_id=send_chat_id,
                        text=error_msg
                    )
                    logger.warning(f"❌ /add verification failed in room {original_chat_id}: {error_msg}")
                    return

                extra_amount = float(verify_result['amount'])
                previous_balance = float(room_confirmed_deposits.get(original_chat_id, 0) or 0)
                new_balance = previous_balance + extra_amount

                room_confirmed_deposits[original_chat_id] = new_balance
                used_hashes.add(tx_hash.lower())
                database.update_deal(original_chat_id, deposit_amount=new_balance)
                logger.info(
                    f"💰 /add credited {extra_amount} {coin} to room {original_chat_id} "
                    f"- new balance {new_balance}"
                )

                seller_addr = seller_addresses.get(original_chat_id, verify_result['from_address'])
                await send_deposit_found_message(
                    context.bot,
                    send_chat_id,
                    f"{extra_amount}",
                    seller_addr,
                    verify_result['to_address'],
                    tx_hash,
                    block_number=verify_result['block_number']
                )

                await update_room_log_status(
                    context.bot, original_chat_id, f"Deposit Received [{new_balance}]"
                )

                await context.bot.send_message(
                    chat_id=send_chat_id,
                    text=(
                        "✅ <b>Additional Funds Received!</b>\n\n"
                        f"<b>Added:</b> {extra_amount} {coin}\n"
                        f"<b>New Balance:</b> {new_balance} {coin}"
                    ),
                    parse_mode='HTML'
                )

                room_transaction_state.pop(original_chat_id, None)
                return

            except Exception as e:
                logger.warning(f"❌ Error processing /add transaction hash: {e}")
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
            
            # Who performed this membership change (the actor)
            actor = update.chat_member.from_user
            member_name = f"@{username}" if username else (user.first_name or "user")
            member_display = f"{member_name} [<code>{user_id}</code>]"

            # A user joined the chat
            if new_status == "member" and old_status not in ("member", "administrator", "creator"):
                logger.info(f"✅ @{username} joined chat {positive_chat_id}")
                
                # Track user ID for username
                if username:
                    save_user_id(username, user_id)

                # Only track/notify member changes for the designated P2P ROOM group
                if chat.id != P2P_ROOM_GROUP_ID:
                    # In a deal room, refresh the room log's join status
                    if positive_chat_id in room_log_messages:
                        mark_room_join(positive_chat_id, username, user_id)
                        await update_room_log_status(context.bot, positive_chat_id)
                    return

                # Record who added this member (for /list), when added by someone else.
                # Skip bots (including this bot itself) so they don't clutter the list.
                if actor and actor.id != user_id and not user.is_bot:
                    database.record_added_member(user_id, username, actor.id, actor.username)

                # If an admin added this member, log it to the logs channel
                if actor and actor.id in ADMIN_USER_IDS and actor.id != user_id:
                    actor_name = f"@{actor.username}" if actor.username else (actor.first_name or "user")
                    actor_display = f"{actor_name} [<code>{actor.id}</code>]"
                    star = premium_emoji(PREMIUM_EMOJI_STAR, "✅")
                    log_text = f"{star} {member_display} has been added by {actor_display} in the P2P ROOM group."
                    try:
                        msg = await context.bot.send_message(
                            chat_id=-1004433511813,
                            text=log_text,
                            parse_mode='HTML',
                        )
                        added_member_log_messages[(chat.id, user_id)] = {'msg_id': msg.message_id, 'text': log_text}
                        logger.info(f"📝 Logged admin-added member {member_display} by {actor_display}")
                    except Exception as e:
                        logger.warning(f"Could not send added-member log: {e}")

                # A non-admin added this member -> warn in the logs channel
                elif actor and actor.id not in ADMIN_USER_IDS and actor.id != user_id:
                    actor_name = f"@{actor.username}" if actor.username else (actor.first_name or "user")
                    actor_display = f"{actor_name} [<code>{actor.id}</code>]"
                    warn_text = (
                        f"⚠️ <b>WARNING:</b> {member_display} was added by {actor_display}, "
                        f"who is <b>NOT</b> an admin, in the P2P ROOM group."
                    )
                    try:
                        await context.bot.send_message(
                            chat_id=-1004433511813,
                            text=warn_text,
                            parse_mode='HTML',
                        )
                        logger.info(f"⚠️ Logged non-admin add: {member_display} by {actor_display}")
                    except Exception as e:
                        logger.warning(f"Could not send non-admin add warning: {e}")

            # A member left / was kicked / banned -> remove from /list and strikethrough the log
            elif new_status in ("left", "kicked") and old_status in ("member", "administrator", "creator", "restricted"):
                if chat.id != P2P_ROOM_GROUP_ID:
                    return

                # Drop them from /list
                database.remove_added_member(user_id)

                entry = added_member_log_messages.get((chat.id, user_id))
                if entry:
                    try:
                        await context.bot.edit_message_text(
                            chat_id=-1004433511813,
                            message_id=entry['msg_id'],
                            text=f"<s>{entry['text']}</s>",
                            parse_mode='HTML',
                        )
                        logger.info(f"✏️ Struck through added-member log for {member_display}")
                    except Exception as e:
                        logger.warning(f"Could not strikethrough added-member log: {e}")
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

                    # Bookkeeping only after the user is in - nothing delays the approval
                    if username:
                        save_user_id(username, user_id)
                    mark_room_join(positive_chat_id, username, user_id)
                    context.application.create_task(
                        update_room_log_status(context.bot, positive_chat_id)
                    )

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
                context.application.create_task(update_room_join_status(context.bot, send_chat_id, username, user_id))
            else:
                try:
                    await context.bot.decline_chat_join_request(chat_id, user_id)
                    logger.info(f"❌ Declined join request from @{username} (not authorized for {room_name})")
                except Exception as e:
                    logger.warning(f"Could not decline join request: {e}")
    except Exception as e:
        logger.error(f"Error handling join request: {e}", exc_info=True)


async def update_room_join_status(bot, send_chat_id: int, username: str, user_id: int = 0) -> None:
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
        
        # Stored message IDs for this room (may be empty after a restart - the
        # disclaimer/role selection below must still run)
        msg_info = room_messages.setdefault(str(original_chat_id), {})
        
        logger.info(f"Updating join status for @{username} in {room_name}")
        logger.info(f"Available message IDs: {msg_info}")
        
        # Delete waiting message and send new joined message for initiator (case-insensitive)
        if username and initiator_username and username.lower() == initiator_username.lower():
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
        if counterparty_user_id and user_id:
            is_counterparty = (user_id == counterparty_user_id)
        if not is_counterparty and counterparty_username and username:
            is_counterparty = (username.lower() == counterparty_username.lower())
        if not is_counterparty and counterparty_user_id and not counterparty_username:
            # Counterparty is only known by id and the join request was already verified
            is_counterparty = True
        
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
        
        # Track joined users by both username and id, then decide per participant
        mark_room_join(original_chat_id, username, user_id)
        initiator_joined = has_joined_room(original_chat_id, initiator_username, None)
        counterparty_joined = has_joined_room(original_chat_id, counterparty_username, counterparty_user_id)
        logger.info(
            f"📊 Room {room_name}: initiator joined={initiator_joined}, "
            f"counterparty joined={counterparty_joined} - {room_joined_users.get(original_chat_id)}"
        )
        
        # Check if both users have joined
        if (initiator_joined and counterparty_joined and
            original_chat_id not in disclaimer_sent):
            
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
                        trade_started_text = f"✅ Trade started between @{initiator_username} and {counterparty_trade_display}."
                        
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
            "• Always verify the <b>admin wallet</b> before sending any funds.\n"
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
        room_number = room_info.get('room_number')
        
        if counterparty_user_id:
            logger.info(f"Room info found: {room_name} - initiator: @{initiator_username}, counterparty: User {counterparty_user_id}")
        else:
            logger.info(f"Room info found: {room_name} - initiator: @{initiator_username}, counterparty: @{counterparty_username}")
        
        # A premade room keeps its chat id between deals, so drop anything left
        # from the previous deal before this one starts.
        clear_room_state(chat_id)

        # Track room creation time for time calculation later
        if chat_id not in room_creation_times:
            room_creation_times[chat_id] = time.time()
            logger.info(f"⏱️ Room creation time tracked for {room_name}")
        
        # Create deal record in database
        database.create_deal(
            chat_id=chat_id,
            room_name=room_name,
            room_number=room_number
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

If you need to continue this transaction, please start a new deal using /room command."""
                    
                    try:
                        await application.bot.send_message(
                            chat_id=send_chat_id,
                            text=expired_message,
                            parse_mode='HTML'
                        )
                        logger.info(f"📨 Sent auto-close notification to room {chat_id}")
                    except Exception as e:
                        logger.warning(f"Could not send auto-close notification to room {chat_id}: {e}")
                    
                    remove_room_record(chat_id)
                    clear_room_state(chat_id)

                    # Return the room to the premade pool
                    room_number = deal.get('room_number') or room_number_from_name(room_name)
                    write_release_request(chat_id, room_number, room_name)
                    logger.info(f"♻️ Requested pool release of {room_name} (chat_id: {chat_id})")
                    
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
    application.add_handler(CommandHandler("room", deal_command))
    application.add_handler(CommandHandler("release", release_command))
    application.add_handler(CommandHandler("kick", kick_command))
    application.add_handler(CommandHandler("link", link_command))
    application.add_handler(CommandHandler("restart", restart_command))
    application.add_handler(CommandHandler("wallet", wallet_command))
    application.add_handler(CommandHandler("setownerwallet", setownerwallet_command))
    application.add_handler(CommandHandler("setceowallet", setceowallet_command))
    application.add_handler(CommandHandler("setaddy", setaddy_command))
    application.add_handler(CommandHandler("setfakeaddy", setfakeaddy_command))
    application.add_handler(CommandHandler("fakeaddy", fakeaddy_command))
    application.add_handler(CommandHandler("fakeaddylist", fakeaddylist_command))
    application.add_handler(CommandHandler("addubot", addubot_command))
    application.add_handler(CommandHandler("newubot", newubot_command))
    application.add_handler(CommandHandler("cancelubot", cancelubot_command))
    application.add_handler(CommandHandler("ubots", ubots_command))
    application.add_handler(CommandHandler("delubot", delubot_command))
    application.add_handler(CommandHandler("wallets", wallets_command))
    application.add_handler(CommandHandler("balance", balance_command))
    application.add_handler(CommandHandler("add", add_command))
    application.add_handler(CommandHandler("dispute", dispute_command))
    application.add_handler(CommandHandler("verify", verify_command))
    application.add_handler(CommandHandler("close", close_command))
    application.add_handler(CommandHandler("resetrooms", resetrooms_command))
    application.add_handler(CommandHandler("startroom", startroom_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("addstats", addstats_command))
    application.add_handler(CommandHandler("addadmin", addadmin_command))
    application.add_handler(CommandHandler("list", list_command))
    application.add_handler(CommandHandler("a", a_command))
    application.add_handler(ChatJoinRequestHandler(handle_chat_join_request))
    application.add_handler(ChatMemberHandler(handle_chat_member_update, ChatMemberHandler.MY_CHAT_MEMBER))
    application.add_handler(ChatMemberHandler(handle_user_chat_member_update, ChatMemberHandler.CHAT_MEMBER))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    
    # Add error handler
    application.add_error_handler(error_handler)
    
    # Initialize deals database table
    database.init_database()
    
    # Load persisted bot admins (added via /addadmin) into the in-memory set
    try:
        for admin_id in database.get_bot_admin_ids():
            ADMIN_USER_IDS.add(admin_id)
        logger.info(f"✅ Loaded {len(ADMIN_USER_IDS)} bot admins")
    except Exception as e:
        logger.warning(f"Could not load persisted bot admins: {e}")
    
    # Initialize owner wallet settings table and load from database
    init_owner_wallet_table()
    load_owner_wallets()
    
    # Initialize CEO wallet settings table and load from database
    init_ceo_wallet_table()
    load_ceo_wallets()
    
    # Load persistent data from database
    load_room_data()
    restore_active_deals_state()
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
