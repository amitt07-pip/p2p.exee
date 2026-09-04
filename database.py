#!/usr/bin/env python3
"""
Database Module for P2PMART Telegram Escrow Bot

This module provides secure PostgreSQL database storage for all deal information.
Each deal room is uniquely identified by:
- chat_id: The Telegram group chat ID
- deal_token: A unique token number for easy reference

The module ensures deal information is properly isolated and won't get mixed
between different deals.
"""

import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
import secrets
import string
import base64

# Encryption for wallet private keys
try:
    from cryptography.fernet import Fernet
    FERNET_AVAILABLE = True
except ImportError:
    FERNET_AVAILABLE = False

load_dotenv()

logger = logging.getLogger(__name__)

# Deal status constants
DEAL_STATUS_PENDING = 'pending'
DEAL_STATUS_ROLES_SELECTED = 'roles_selected'
DEAL_STATUS_DETAILS_ENTERED = 'details_entered'
DEAL_STATUS_SUMMARY_SHOWN = 'summary_shown'
DEAL_STATUS_CONFIRMED = 'confirmed'
DEAL_STATUS_DEPOSIT_RECEIVED = 'deposit_received'
DEAL_STATUS_RELEASE_PENDING = 'release_pending'
DEAL_STATUS_COMPLETED = 'completed'
DEAL_STATUS_CANCELLED = 'cancelled'
DEAL_STATUS_EXPIRED = 'expired'

# Human-facing Trade ID shown in the deal summary (e.g. #P2PMMX5090), assigned
# sequentially per deal starting at TRADE_ID_START.
TRADE_ID_PREFIX = 'P2PMMX'
TRADE_ID_START = 5090


def get_db_connection():
    """Get database connection from DATABASE_URL environment variable"""
    try:
        db_url = os.getenv('DATABASE_URL', '')
        if not db_url:
            logger.warning("DATABASE_URL not set")
            return None
        return psycopg2.connect(db_url)
    except Exception as e:
        logger.warning(f"DB connection error: {e}")
        return None


def generate_deal_token(length: int = 8) -> str:
    """Generate a unique alphanumeric token for a deal"""
    chars = string.ascii_uppercase + string.digits
    return ''.join(secrets.choice(chars) for _ in range(length))


def init_database():
    """Initialize the deals table with all required columns"""
    try:
        conn = get_db_connection()
        if not conn:
            logger.warning("Could not connect to database")
            return False
        
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS deals (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT UNIQUE NOT NULL,
                deal_token VARCHAR(16) UNIQUE NOT NULL,
                
                -- Buyer information
                buyer_username TEXT,
                buyer_user_id BIGINT,
                buyer_address TEXT,
                
                -- Seller information
                seller_username TEXT,
                seller_user_id BIGINT,
                seller_address TEXT,
                
                -- Deal details
                amount DECIMAL(20, 8),
                rate DECIMAL(20, 8),
                payment_method TEXT,
                network TEXT DEFAULT 'BSC',
                coin TEXT,
                
                -- Escrow information
                escrow_address TEXT,
                tx_hash TEXT,
                
                -- Status and approvals
                deal_status TEXT DEFAULT 'pending',
                buyer_approved BOOLEAN DEFAULT FALSE,
                seller_approved BOOLEAN DEFAULT FALSE,
                buyer_release_approved BOOLEAN DEFAULT FALSE,
                seller_release_approved BOOLEAN DEFAULT FALSE,
                
                -- Timestamps
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                confirmed_at TIMESTAMP,
                deposit_at TIMESTAMP,
                completed_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- Room metadata
                room_name TEXT,
                room_number INTEGER,

                -- Admin override: fix deposit to 'owner' or 'ceo' wallet
                fixed_wallet_role TEXT,

                -- Sequential human-facing trade id (e.g. P2PMMX5090)
                trade_id TEXT,
                trade_seq INTEGER,

                -- Total confirmed escrow balance (initial deposit + /add top-ups)
                deposit_amount DECIMAL(20, 8)
            )
        """)
        
        # Add columns if they don't exist (for existing tables)
        columns_to_add = [
            ("deal_token", "VARCHAR(16) UNIQUE"),
            ("buyer_username", "TEXT"),
            ("buyer_user_id", "BIGINT"),
            ("buyer_address", "TEXT"),
            ("seller_username", "TEXT"),
            ("seller_user_id", "BIGINT"),
            ("seller_address", "TEXT"),
            ("amount", "DECIMAL(20, 8)"),
            ("rate", "DECIMAL(20, 8)"),
            ("payment_method", "TEXT"),
            ("network", "TEXT DEFAULT 'BSC'"),
            ("coin", "TEXT"),
            ("escrow_address", "TEXT"),
            ("tx_hash", "TEXT"),
            ("deal_status", "TEXT DEFAULT 'pending'"),
            ("buyer_approved", "BOOLEAN DEFAULT FALSE"),
            ("seller_approved", "BOOLEAN DEFAULT FALSE"),
            ("buyer_release_approved", "BOOLEAN DEFAULT FALSE"),
            ("seller_release_approved", "BOOLEAN DEFAULT FALSE"),
            ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
            ("confirmed_at", "TIMESTAMP"),
            ("deposit_at", "TIMESTAMP"),
            ("completed_at", "TIMESTAMP"),
            ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
            ("room_name", "TEXT"),
            ("room_number", "INTEGER"),
            ("fixed_wallet_role", "TEXT"),
            ("trade_id", "TEXT"),
            ("trade_seq", "INTEGER"),
            ("deposit_amount", "DECIMAL(20, 8)"),
        ]
        
        for col_name, col_type in columns_to_add:
            try:
                cur.execute(f"ALTER TABLE deals ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
            except Exception as col_err:
                logger.debug(f"Column {col_name} may already exist: {col_err}")
        
        # Create indexes for faster lookups
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_chat_id ON deals(chat_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_token ON deals(deal_token)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_status ON deals(deal_status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_buyer ON deals(buyer_username)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_seller ON deals(seller_username)")
        
        # Username -> user_id mapping (used to resolve @username for /stats & /addstats).
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_ids (
                username TEXT PRIMARY KEY,
                user_id BIGINT
            )
        """)
        
        # Manually-set stats (via /addstats), keyed by Telegram user id.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS manual_stats (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                total_bought TEXT,
                buy_trades TEXT,
                total_sold TEXT,
                sell_trades TEXT,
                lifetime_volume TEXT,
                total_deals TEXT,
                completion_rate TEXT,
                global_rank TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Bot admins added at runtime via /addadmin (persisted across restarts).
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_admins (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                added_by BIGINT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Members added to groups, tracked with who added them (for /list).
        cur.execute("""
            CREATE TABLE IF NOT EXISTS added_members (
                member_id BIGINT PRIMARY KEY,
                member_username TEXT,
                added_by BIGINT,
                added_by_username TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Backup escrow wallets set per room number via /setfakeaddy, applied to a
        # room's deposit with /fakeaddy.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS room_backup_wallets (
                room_number INTEGER NOT NULL,
                token TEXT NOT NULL,
                wallet_address TEXT NOT NULL,
                updated_by BIGINT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (room_number, token)
            )
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        logger.warning(f"Could not initialize database: {e}")
        return False


def create_deal(chat_id: int, room_name: str = None, room_number: int = None) -> Optional[str]:
    """
    Create a new deal record for a chat room.
    Returns the unique deal token if successful, None otherwise.
    """
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        # Generate unique token
        deal_token = generate_deal_token()
        
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO deals (chat_id, deal_token, room_name, room_number, deal_status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (chat_id) DO UPDATE SET
                room_name = COALESCE(EXCLUDED.room_name, deals.room_name),
                room_number = COALESCE(EXCLUDED.room_number, deals.room_number),
                updated_at = CURRENT_TIMESTAMP
            RETURNING deal_token
        """, (chat_id, deal_token, room_name, room_number, DEAL_STATUS_PENDING, datetime.now()))
        
        result = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        token = result[0] if result else deal_token
        logger.info(f"Created deal for chat_id {chat_id} with token {token}")
        return token
    except Exception as e:
        logger.warning(f"Could not create deal: {e}")
        return None


def get_deal(chat_id: int) -> Optional[Dict[str, Any]]:
    """Get deal information by chat_id"""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM deals WHERE chat_id = %s", (chat_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        return dict(row) if row else None
    except Exception as e:
        logger.warning(f"Could not get deal: {e}")
        return None


def get_deal_by_token(deal_token: str) -> Optional[Dict[str, Any]]:
    """Get deal information by deal token"""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM deals WHERE deal_token = %s", (deal_token,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        return dict(row) if row else None
    except Exception as e:
        logger.warning(f"Could not get deal by token: {e}")
        return None


def update_deal(chat_id: int, **kwargs) -> bool:
    """Update deal information. Pass field names as keyword arguments."""
    if not kwargs:
        return True
    
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        allowed_fields = {
            'buyer_username', 'seller_username', 'buyer_user_id', 'seller_user_id',
            'amount', 'rate', 'payment_method', 'network', 'coin',
            'buyer_address', 'seller_address', 'escrow_address', 'tx_hash',
            'deal_status', 'buyer_approved', 'seller_approved',
            'buyer_release_approved', 'seller_release_approved',
            'confirmed_at', 'deposit_at', 'completed_at',
            'room_name', 'room_number', 'fixed_wallet_role', 'deposit_amount'
        }
        
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in allowed_fields}
        if not filtered_kwargs:
            return True
        
        filtered_kwargs['updated_at'] = datetime.now()
        
        set_clause = ', '.join([f"{k} = %s" for k in filtered_kwargs.keys()])
        values = list(filtered_kwargs.values())
        values.append(chat_id)
        
        cur = conn.cursor()
        cur.execute(f"UPDATE deals SET {set_clause} WHERE chat_id = %s", values)
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Updated deal {chat_id}: {list(filtered_kwargs.keys())}")
        return True
    except Exception as e:
        logger.warning(f"Could not update deal: {e}")
        return False


def assign_trade_id(chat_id: int) -> Optional[str]:
    """
    Return the deal's Trade ID, assigning the next sequential one (starting at
    TRADE_ID_START) the first time it's requested. Idempotent per deal.
    """
    try:
        conn = get_db_connection()
        if not conn:
            return None

        cur = conn.cursor()
        cur.execute("""
            UPDATE deals
            SET trade_seq = COALESCE((SELECT MAX(trade_seq) + 1 FROM deals), %s),
                trade_id = %s || COALESCE((SELECT MAX(trade_seq) + 1 FROM deals), %s)::text,
                updated_at = CURRENT_TIMESTAMP
            WHERE chat_id = %s AND trade_id IS NULL
            RETURNING trade_id
        """, (TRADE_ID_START, TRADE_ID_PREFIX, TRADE_ID_START, chat_id))
        row = cur.fetchone()
        if row is None:
            cur.execute("SELECT trade_id FROM deals WHERE chat_id = %s", (chat_id,))
            row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        logger.warning(f"Could not assign trade id: {e}")
        return None


def set_roles(chat_id: int, buyer_username: str, seller_username: str,
              buyer_user_id: int = None, seller_user_id: int = None) -> bool:
    """Set buyer and seller roles for a deal"""
    return update_deal(
        chat_id,
        buyer_username=buyer_username,
        seller_username=seller_username,
        buyer_user_id=buyer_user_id,
        seller_user_id=seller_user_id,
        deal_status=DEAL_STATUS_ROLES_SELECTED
    )


def set_amount(chat_id: int, amount: float) -> bool:
    """Set the deal amount"""
    return update_deal(chat_id, amount=amount)


def set_rate(chat_id: int, rate: float) -> bool:
    """Set the deal rate"""
    return update_deal(chat_id, rate=rate)


def set_payment_method(chat_id: int, payment_method: str) -> bool:
    """Set the payment method"""
    return update_deal(chat_id, payment_method=payment_method)


def set_network(chat_id: int, network: str) -> bool:
    """Set the blockchain network (BSC, ETH, etc.)"""
    return update_deal(chat_id, network=network)


def set_coin(chat_id: int, coin: str) -> bool:
    """Set the coin type (USDT, USDC, etc.)"""
    return update_deal(chat_id, coin=coin)


def set_buyer_address(chat_id: int, address: str) -> bool:
    """Set the buyer's crypto wallet address"""
    return update_deal(chat_id, buyer_address=address)


def set_seller_address(chat_id: int, address: str) -> bool:
    """Set the seller's crypto wallet address"""
    return update_deal(chat_id, seller_address=address)


def set_escrow_address(chat_id: int, address: str) -> bool:
    """Set the escrow deposit address"""
    return update_deal(chat_id, escrow_address=address)


def approve_summary(chat_id: int, role: str) -> Dict[str, bool]:
    """
    Record approval of deal summary by buyer or seller.
    Returns the current approval status for both parties.
    """
    try:
        deal = get_deal(chat_id)
        if not deal:
            return {'buyer': False, 'seller': False}
        
        if role.lower() == 'buyer':
            update_deal(chat_id, buyer_approved=True)
            return {'buyer': True, 'seller': deal.get('seller_approved', False)}
        elif role.lower() == 'seller':
            update_deal(chat_id, seller_approved=True)
            return {'buyer': deal.get('buyer_approved', False), 'seller': True}
        
        return {'buyer': deal.get('buyer_approved', False), 'seller': deal.get('seller_approved', False)}
    except Exception as e:
        logger.warning(f"Could not approve summary: {e}")
        return {'buyer': False, 'seller': False}


def confirm_deal(chat_id: int, escrow_address: str = None) -> bool:
    """Mark deal as confirmed (both parties approved)"""
    kwargs = {
        'deal_status': DEAL_STATUS_CONFIRMED,
        'buyer_approved': True,
        'seller_approved': True,
        'confirmed_at': datetime.now()
    }
    if escrow_address:
        kwargs['escrow_address'] = escrow_address
    
    return update_deal(chat_id, **kwargs)


def record_deposit(chat_id: int, tx_hash: str) -> bool:
    """Record deposit transaction hash"""
    return update_deal(
        chat_id,
        tx_hash=tx_hash,
        deal_status=DEAL_STATUS_DEPOSIT_RECEIVED,
        deposit_at=datetime.now()
    )


def approve_release(chat_id: int, role: str) -> Dict[str, Any]:
    """
    Record release approval by buyer or seller.
    Returns the current release approval status.
    """
    try:
        deal = get_deal(chat_id)
        if not deal:
            return {'buyer': False, 'seller': False, 'both_approved': False}
        
        buyer_approved = deal.get('buyer_release_approved', False)
        seller_approved = deal.get('seller_release_approved', False)
        
        if role.lower() == 'buyer':
            buyer_approved = True
            update_deal(chat_id, buyer_release_approved=True, deal_status=DEAL_STATUS_RELEASE_PENDING)
        elif role.lower() == 'seller':
            seller_approved = True
            update_deal(chat_id, seller_release_approved=True, deal_status=DEAL_STATUS_RELEASE_PENDING)
        
        both_approved = buyer_approved and seller_approved
        
        if both_approved:
            complete_deal(chat_id)
        
        return {
            'buyer': buyer_approved,
            'seller': seller_approved,
            'both_approved': both_approved
        }
    except Exception as e:
        logger.warning(f"Could not approve release: {e}")
        return {'buyer': False, 'seller': False, 'both_approved': False}


def complete_deal(chat_id: int) -> bool:
    """Mark deal as completed"""
    return update_deal(
        chat_id,
        deal_status=DEAL_STATUS_COMPLETED,
        completed_at=datetime.now()
    )


def cancel_deal(chat_id: int) -> bool:
    """Mark deal as cancelled"""
    return update_deal(chat_id, deal_status=DEAL_STATUS_CANCELLED)


def expire_deal(chat_id: int) -> bool:
    """Mark deal as expired (auto-closed after timeout)"""
    return update_deal(chat_id, deal_status=DEAL_STATUS_EXPIRED)


def reset_deal(chat_id: int) -> bool:
    """Reset deal to initial state (for /restart command)"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        # Get current token to preserve it
        deal = get_deal(chat_id)
        if not deal:
            return False
        
        cur = conn.cursor()
        cur.execute("""
            UPDATE deals SET
                buyer_username = NULL,
                seller_username = NULL,
                buyer_user_id = NULL,
                seller_user_id = NULL,
                buyer_address = NULL,
                seller_address = NULL,
                amount = NULL,
                rate = NULL,
                payment_method = NULL,
                coin = NULL,
                escrow_address = NULL,
                tx_hash = NULL,
                deal_status = %s,
                buyer_approved = FALSE,
                seller_approved = FALSE,
                buyer_release_approved = FALSE,
                seller_release_approved = FALSE,
                confirmed_at = NULL,
                deposit_at = NULL,
                completed_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE chat_id = %s
        """, (DEAL_STATUS_PENDING, chat_id))
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Reset deal {chat_id}")
        return True
    except Exception as e:
        logger.warning(f"Could not reset deal: {e}")
        return False


def get_deal_summary(chat_id: int) -> Optional[Dict[str, Any]]:
    """Get a formatted deal summary"""
    deal = get_deal(chat_id)
    if not deal:
        return None
    
    return {
        'token': deal.get('deal_token'),
        'chat_id': deal['chat_id'],
        'room_name': deal.get('room_name'),
        'room_number': deal.get('room_number'),
        'buyer': deal.get('buyer_username'),
        'buyer_id': deal.get('buyer_user_id'),
        'seller': deal.get('seller_username'),
        'seller_id': deal.get('seller_user_id'),
        'amount': float(deal['amount']) if deal.get('amount') else None,
        'rate': float(deal['rate']) if deal.get('rate') else None,
        'payment_method': deal.get('payment_method'),
        'network': deal.get('network', 'BSC'),
        'coin': deal.get('coin'),
        'buyer_address': deal.get('buyer_address'),
        'seller_address': deal.get('seller_address'),
        'escrow_address': deal.get('escrow_address'),
        'tx_hash': deal.get('tx_hash'),
        'status': deal.get('deal_status'),
        'buyer_approved': deal.get('buyer_approved', False),
        'seller_approved': deal.get('seller_approved', False),
        'created_at': deal.get('created_at')
    }


def get_expired_deals(hours: int = 12) -> List[Dict[str, Any]]:
    """Get all deals that have been running for more than specified hours"""
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT * FROM deals 
            WHERE deal_status NOT IN (%s, %s, %s)
            AND created_at < NOW() - INTERVAL '%s hours'
            ORDER BY created_at ASC
        """, (DEAL_STATUS_COMPLETED, DEAL_STATUS_CANCELLED, DEAL_STATUS_EXPIRED, hours))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return [dict(row) for row in rows]
    except Exception as e:
        logger.warning(f"Could not get expired deals: {e}")
        return []


def get_active_deals() -> List[Dict[str, Any]]:
    """Get all active (non-completed, non-cancelled, non-expired) deals"""
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT * FROM deals 
            WHERE deal_status NOT IN (%s, %s, %s)
            ORDER BY created_at DESC
        """, (DEAL_STATUS_COMPLETED, DEAL_STATUS_CANCELLED, DEAL_STATUS_EXPIRED))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return [dict(row) for row in rows]
    except Exception as e:
        logger.warning(f"Could not get active deals: {e}")
        return []


def is_room_number_available(room_number: int) -> bool:
    """True when no active deal is currently using this room number."""
    try:
        conn = get_db_connection()
        if not conn:
            return False

        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM deals
            WHERE room_number = %s AND deal_status NOT IN (%s, %s, %s)
        """, (room_number, DEAL_STATUS_COMPLETED, DEAL_STATUS_CANCELLED, DEAL_STATUS_EXPIRED))

        count = cur.fetchone()[0]
        cur.close()
        conn.close()

        return count == 0
    except Exception as e:
        logger.warning(f"Could not check room number availability: {e}")
        return False


def set_room_backup_wallet(room_number: int, token: str, wallet_address: str, updated_by: int = None) -> bool:
    """Store a backup escrow address for a room number + token (BSC)."""
    try:
        conn = get_db_connection()
        if not conn:
            return False

        cur = conn.cursor()
        cur.execute("""
            INSERT INTO room_backup_wallets (room_number, token, wallet_address, updated_by, updated_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (room_number, token) DO UPDATE SET
                wallet_address = EXCLUDED.wallet_address,
                updated_by = EXCLUDED.updated_by,
                updated_at = CURRENT_TIMESTAMP
        """, (room_number, token.upper(), wallet_address, updated_by))

        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Could not save room backup wallet: {e}")
        return False


def get_room_backup_wallet(room_number: int, token: str) -> Optional[str]:
    """Get the backup escrow address stored for a room number + token."""
    try:
        conn = get_db_connection()
        if not conn:
            return None

        cur = conn.cursor()
        cur.execute("""
            SELECT wallet_address FROM room_backup_wallets
            WHERE room_number = %s AND token = %s
        """, (room_number, token.upper()))
        row = cur.fetchone()
        cur.close()
        conn.close()

        return row[0] if row else None
    except Exception as e:
        logger.warning(f"Could not get room backup wallet: {e}")
        return None


def get_room_backup_wallets(room_number: int) -> Dict[str, str]:
    """Get all backup escrow addresses stored for a room number, keyed by token."""
    try:
        conn = get_db_connection()
        if not conn:
            return {}

        cur = conn.cursor()
        cur.execute("""
            SELECT token, wallet_address FROM room_backup_wallets
            WHERE room_number = %s
        """, (room_number,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return {row[0]: row[1] for row in rows}
    except Exception as e:
        logger.warning(f"Could not get room backup wallets: {e}")
        return {}


def get_all_room_backup_wallets() -> List[Dict[str, Any]]:
    """All stored backup escrow addresses, ordered by room number then token."""
    try:
        conn = get_db_connection()
        if not conn:
            return []

        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT room_number, token, wallet_address FROM room_backup_wallets
            ORDER BY room_number, token
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return [dict(row) for row in rows]
    except Exception as e:
        logger.warning(f"Could not list room backup wallets: {e}")
        return []


def get_rooms_using_backup_wallet() -> List[int]:
    """Room numbers of active deals currently switched to their backup address."""
    try:
        conn = get_db_connection()
        if not conn:
            return []

        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT room_number FROM deals
            WHERE fixed_wallet_role = 'backup'
            AND room_number IS NOT NULL
            AND deal_status NOT IN (%s, %s, %s)
        """, (DEAL_STATUS_COMPLETED, DEAL_STATUS_CANCELLED, DEAL_STATUS_EXPIRED))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return [row[0] for row in rows]
    except Exception as e:
        logger.warning(f"Could not list rooms using backup wallets: {e}")
        return []


def get_backup_wallet_rooms(wallet_address: str) -> List[Dict[str, Any]]:
    """Rooms/tokens a backup escrow address is stored for (case-insensitive)."""
    try:
        conn = get_db_connection()
        if not conn:
            return []

        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT room_number, token FROM room_backup_wallets
            WHERE LOWER(wallet_address) = LOWER(%s)
            ORDER BY room_number
        """, (wallet_address,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return [dict(row) for row in rows]
    except Exception as e:
        logger.warning(f"Could not look up backup wallet rooms: {e}")
        return []


def get_deals_by_user(username: str) -> List[Dict[str, Any]]:
    """Get all deals where user is buyer or seller"""
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT * FROM deals 
            WHERE LOWER(buyer_username) = LOWER(%s) OR LOWER(seller_username) = LOWER(%s)
            ORDER BY created_at DESC
        """, (username, username))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return [dict(row) for row in rows]
    except Exception as e:
        logger.warning(f"Could not get deals by user: {e}")
        return []


def get_active_deal_by_address_for_user(address: str, user_id: int = None, username: str = None) -> Optional[Dict[str, Any]]:
    """
    Find the most recent active deal whose escrow address matches, narrowed to a
    specific participant (by user id or username). Used by /verify to show which
    room a shared escrow address belongs to for the requesting user.
    """
    try:
        conn = get_db_connection()
        if not conn:
            return None

        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT * FROM deals
            WHERE LOWER(escrow_address) = LOWER(%s)
              AND deal_status NOT IN (%s, %s, %s)
              AND (
                    (%s IS NOT NULL AND (buyer_user_id = %s OR seller_user_id = %s))
                 OR (%s IS NOT NULL AND (LOWER(buyer_username) = LOWER(%s) OR LOWER(seller_username) = LOWER(%s)))
              )
            ORDER BY created_at DESC
            LIMIT 1
        """, (
            address,
            DEAL_STATUS_COMPLETED, DEAL_STATUS_CANCELLED, DEAL_STATUS_EXPIRED,
            user_id, user_id, user_id,
            username, username, username,
        ))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        logger.warning(f"Could not find active deal by address for user: {e}")
        return None


def get_active_deal_by_address(address: str) -> Optional[Dict[str, Any]]:
    """
    Return the single active deal whose escrow address matches — but only when
    it's unambiguous. Escrow addresses rotate between just the owner/CEO
    wallets, so the same address is shared across many rooms; if more than one
    active deal uses it we return None rather than guess the wrong room.
    Used by /verify only as a safe fallback when the requester isn't a
    buyer/seller of any matching deal.
    """
    try:
        conn = get_db_connection()
        if not conn:
            return None

        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT * FROM deals
            WHERE LOWER(escrow_address) = LOWER(%s)
              AND deal_status NOT IN (%s, %s, %s)
            ORDER BY created_at DESC
            LIMIT 2
        """, (
            address,
            DEAL_STATUS_COMPLETED, DEAL_STATUS_CANCELLED, DEAL_STATUS_EXPIRED,
        ))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        if len(rows) == 1:
            return dict(rows[0])
        return None
    except Exception as e:
        logger.warning(f"Could not find active deal by address: {e}")
        return None


def get_user_stats(username: str) -> Dict[str, Any]:
    """
    Compute trading stats for a user (matched by username as buyer or seller).

    Returns a dict with buying/selling totals, lifetime volume, total/completed
    deal counts, completion rate, and the user's global rank by lifetime volume.
    Amounts are denominated in USDT (treated as USD). Falls back to all-zero
    stats if the database is unavailable.
    """
    stats = {
        'total_bought': 0.0,
        'buy_trades': 0,
        'total_sold': 0.0,
        'sell_trades': 0,
        'lifetime_volume': 0.0,
        'total_deals': 0,
        'completed_deals': 0,
        'completion_rate': 0.0,
        'global_rank': 0,
    }

    try:
        conn = get_db_connection()
        if not conn:
            return stats

        cur = conn.cursor()

        # Buying stats (completed deals where user is buyer)
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(amount), 0)
            FROM deals
            WHERE LOWER(buyer_username) = LOWER(%s) AND deal_status = %s
        """, (username, DEAL_STATUS_COMPLETED))
        buy_count, buy_sum = cur.fetchone()

        # Selling stats (completed deals where user is seller)
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(amount), 0)
            FROM deals
            WHERE LOWER(seller_username) = LOWER(%s) AND deal_status = %s
        """, (username, DEAL_STATUS_COMPLETED))
        sell_count, sell_sum = cur.fetchone()

        # All deals involving the user (any status) and completed count
        cur.execute("""
            SELECT
                COUNT(*),
                COUNT(*) FILTER (WHERE deal_status = %s)
            FROM deals
            WHERE LOWER(buyer_username) = LOWER(%s) OR LOWER(seller_username) = LOWER(%s)
        """, (DEAL_STATUS_COMPLETED, username, username))
        total_deals, completed_deals = cur.fetchone()

        # Global rank: number of users with strictly higher lifetime volume + 1
        cur.execute("""
            WITH volumes AS (
                SELECT username, SUM(amt) AS volume FROM (
                    SELECT LOWER(buyer_username) AS username, amount AS amt
                    FROM deals
                    WHERE deal_status = %(status)s AND buyer_username IS NOT NULL
                    UNION ALL
                    SELECT LOWER(seller_username) AS username, amount AS amt
                    FROM deals
                    WHERE deal_status = %(status)s AND seller_username IS NOT NULL
                ) t
                GROUP BY username
            )
            SELECT COUNT(*) + 1
            FROM volumes
            WHERE volume > (
                SELECT COALESCE(SUM(volume), 0)
                FROM volumes
                WHERE username = LOWER(%(username)s)
            )
        """, {'status': DEAL_STATUS_COMPLETED, 'username': username})
        rank_row = cur.fetchone()
        global_rank = int(rank_row[0]) if rank_row else 0

        cur.close()
        conn.close()

        total_bought = float(buy_sum or 0)
        total_sold = float(sell_sum or 0)
        total_deals = int(total_deals or 0)
        completed_deals = int(completed_deals or 0)

        stats.update({
            'total_bought': total_bought,
            'buy_trades': int(buy_count or 0),
            'total_sold': total_sold,
            'sell_trades': int(sell_count or 0),
            'lifetime_volume': total_bought + total_sold,
            'total_deals': total_deals,
            'completed_deals': completed_deals,
            'completion_rate': (completed_deals / total_deals * 100) if total_deals else 0.0,
            'global_rank': global_rank,
        })
        return stats
    except Exception as e:
        logger.warning(f"Could not get user stats: {e}")
        return stats


def get_user_stats_by_id(user_id: int) -> Dict[str, Any]:
    """
    Compute trading stats for a user matched by Telegram user id (buyer_user_id /
    seller_user_id). Preferred over get_user_stats() so stats can't be hijacked by
    someone taking over a username. Falls back to all-zero stats on error.
    """
    stats = {
        'total_bought': 0.0,
        'buy_trades': 0,
        'total_sold': 0.0,
        'sell_trades': 0,
        'lifetime_volume': 0.0,
        'total_deals': 0,
        'completed_deals': 0,
        'completion_rate': 0.0,
        'global_rank': 0,
    }

    try:
        conn = get_db_connection()
        if not conn:
            return stats

        cur = conn.cursor()

        # Buying stats (completed deals where user is buyer)
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(amount), 0)
            FROM deals
            WHERE buyer_user_id = %s AND deal_status = %s
        """, (user_id, DEAL_STATUS_COMPLETED))
        buy_count, buy_sum = cur.fetchone()

        # Selling stats (completed deals where user is seller)
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(amount), 0)
            FROM deals
            WHERE seller_user_id = %s AND deal_status = %s
        """, (user_id, DEAL_STATUS_COMPLETED))
        sell_count, sell_sum = cur.fetchone()

        # All deals involving the user (any status) and completed count
        cur.execute("""
            SELECT
                COUNT(*),
                COUNT(*) FILTER (WHERE deal_status = %s)
            FROM deals
            WHERE buyer_user_id = %s OR seller_user_id = %s
        """, (DEAL_STATUS_COMPLETED, user_id, user_id))
        total_deals, completed_deals = cur.fetchone()

        # Global rank: number of users (by id) with strictly higher lifetime volume + 1
        cur.execute("""
            WITH volumes AS (
                SELECT uid, SUM(amt) AS volume FROM (
                    SELECT buyer_user_id AS uid, amount AS amt
                    FROM deals
                    WHERE deal_status = %(status)s AND buyer_user_id IS NOT NULL
                    UNION ALL
                    SELECT seller_user_id AS uid, amount AS amt
                    FROM deals
                    WHERE deal_status = %(status)s AND seller_user_id IS NOT NULL
                ) t
                GROUP BY uid
            )
            SELECT COUNT(*) + 1
            FROM volumes
            WHERE volume > (
                SELECT COALESCE(SUM(volume), 0)
                FROM volumes
                WHERE uid = %(uid)s
            )
        """, {'status': DEAL_STATUS_COMPLETED, 'uid': user_id})
        rank_row = cur.fetchone()
        global_rank = int(rank_row[0]) if rank_row else 0

        cur.close()
        conn.close()

        total_bought = float(buy_sum or 0)
        total_sold = float(sell_sum or 0)
        total_deals = int(total_deals or 0)
        completed_deals = int(completed_deals or 0)

        stats.update({
            'total_bought': total_bought,
            'buy_trades': int(buy_count or 0),
            'total_sold': total_sold,
            'sell_trades': int(sell_count or 0),
            'lifetime_volume': total_bought + total_sold,
            'total_deals': total_deals,
            'completed_deals': completed_deals,
            'completion_rate': (completed_deals / total_deals * 100) if total_deals else 0.0,
            'global_rank': global_rank,
        })
        return stats
    except Exception as e:
        logger.warning(f"Could not get user stats by id: {e}")
        return stats


MANUAL_STATS_FIELDS = [
    'total_bought', 'buy_trades', 'total_sold', 'sell_trades',
    'lifetime_volume', 'total_deals', 'completion_rate', 'global_rank',
]


def get_user_id_by_username(username: str) -> Optional[int]:
    """Resolve a Telegram user id from a stored username (user_ids table)."""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM user_ids WHERE LOWER(username) = LOWER(%s)", (username,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return int(row[0]) if row and row[0] is not None else None
    except Exception as e:
        logger.warning(f"Could not resolve user id for @{username}: {e}")
        return None


def get_username_by_user_id(user_id: int) -> Optional[str]:
    """Resolve a stored username from a Telegram user id (user_ids table)."""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        cur = conn.cursor()
        cur.execute("SELECT username FROM user_ids WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row and row[0] else None
    except Exception as e:
        logger.warning(f"Could not resolve username for id {user_id}: {e}")
        return None


def save_manual_stats(user_id: int, username: Optional[str], data: Dict[str, str]) -> bool:
    """Upsert manually-set stats (from /addstats) for a user id. Values are display strings."""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO manual_stats (
                user_id, username, total_bought, buy_trades, total_sold, sell_trades,
                lifetime_volume, total_deals, completion_rate, global_rank, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                total_bought = EXCLUDED.total_bought,
                buy_trades = EXCLUDED.buy_trades,
                total_sold = EXCLUDED.total_sold,
                sell_trades = EXCLUDED.sell_trades,
                lifetime_volume = EXCLUDED.lifetime_volume,
                total_deals = EXCLUDED.total_deals,
                completion_rate = EXCLUDED.completion_rate,
                global_rank = EXCLUDED.global_rank,
                updated_at = CURRENT_TIMESTAMP
        """, (
            user_id, username,
            data.get('total_bought'), data.get('buy_trades'),
            data.get('total_sold'), data.get('sell_trades'),
            data.get('lifetime_volume'), data.get('total_deals'),
            data.get('completion_rate'), data.get('global_rank'),
        ))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Could not save manual stats for {user_id}: {e}")
        return False


def get_manual_stats(user_id: int) -> Optional[Dict[str, str]]:
    """Return manually-set stats for a user id as display strings, or None if none set."""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        cur = conn.cursor()
        cur.execute("""
            SELECT total_bought, buy_trades, total_sold, sell_trades,
                   lifetime_volume, total_deals, completion_rate, global_rank
            FROM manual_stats WHERE user_id = %s
        """, (user_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        result = {field: (row[i] if row[i] is not None else '') for i, field in enumerate(MANUAL_STATS_FIELDS)}
        return result
    except Exception as e:
        logger.warning(f"Could not get manual stats for {user_id}: {e}")
        return None


def add_bot_admin(user_id: int, username: Optional[str], added_by: Optional[int]) -> bool:
    """Persist a bot admin id (via /addadmin). Upserts username/added_by."""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bot_admins (user_id, username, added_by, added_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) DO UPDATE SET
                username = COALESCE(EXCLUDED.username, bot_admins.username),
                added_by = COALESCE(EXCLUDED.added_by, bot_admins.added_by)
        """, (user_id, username, added_by))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Could not add bot admin {user_id}: {e}")
        return False


def get_bot_admin_ids() -> List[int]:
    """Return all persisted bot admin ids."""
    try:
        conn = get_db_connection()
        if not conn:
            return []
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM bot_admins")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [int(r[0]) for r in rows if r and r[0] is not None]
    except Exception as e:
        logger.warning(f"Could not get bot admin ids: {e}")
        return []


def record_added_member(member_id: int, member_username: Optional[str],
                        added_by: int, added_by_username: Optional[str]) -> bool:
    """Record that a member was added to a group by someone (for /list)."""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO added_members (member_id, member_username, added_by, added_by_username, added_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (member_id) DO UPDATE SET
                member_username = COALESCE(EXCLUDED.member_username, added_members.member_username),
                added_by = EXCLUDED.added_by,
                added_by_username = COALESCE(EXCLUDED.added_by_username, added_members.added_by_username),
                added_at = CURRENT_TIMESTAMP
        """, (member_id, member_username, added_by, added_by_username))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Could not record added member {member_id}: {e}")
        return False


def remove_added_member(member_id: int) -> bool:
    """Remove an added-member record (e.g. when they leave / are kicked) so /list stays current."""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()
        cur.execute("DELETE FROM added_members WHERE member_id = %s", (member_id,))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Could not remove added member {member_id}: {e}")
        return False


def get_added_members_grouped() -> List[Dict[str, Any]]:
    """
    Return added members grouped by who added them, as a list of dicts:
    [{'added_by': id, 'added_by_username': str, 'members': [{'id': int, 'username': str}, ...]}, ...]
    Adders with no members are naturally excluded.
    """
    try:
        conn = get_db_connection()
        if not conn:
            return []
        cur = conn.cursor()
        cur.execute("""
            SELECT added_by, added_by_username, member_id, member_username
            FROM added_members
            ORDER BY added_by, added_at ASC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        grouped: Dict[int, Dict[str, Any]] = {}
        for added_by, added_by_username, member_id, member_username in rows:
            if added_by is None:
                continue
            if added_by not in grouped:
                grouped[added_by] = {
                    'added_by': int(added_by),
                    'added_by_username': added_by_username,
                    'members': [],
                }
            grouped[added_by]['members'].append({'id': int(member_id), 'username': member_username})
        return [g for g in grouped.values() if g['members']]
    except Exception as e:
        logger.warning(f"Could not get added members grouped: {e}")
        return []


def delete_deal(chat_id: int) -> bool:
    """Delete a deal record (use with caution)"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cur = conn.cursor()
        cur.execute("DELETE FROM deals WHERE chat_id = %s", (chat_id,))
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Deleted deal {chat_id}")
        return True
    except Exception as e:
        logger.warning(f"Could not delete deal: {e}")
        return False


# ============================================================================
# USER WALLET FUNCTIONS
# ============================================================================

def get_fernet_key():
    """Get or generate Fernet encryption key for wallet private keys"""
    key = os.getenv('WALLET_FERNET_KEY')
    if key:
        return key.encode() if isinstance(key, str) else key
    
    # Generate a new key if not set (WARNING: this should be set in production!)
    logger.warning("WALLET_FERNET_KEY not set - generating temporary key (wallets won't persist across restarts without this!)")
    return Fernet.generate_key()


def encrypt_private_key(private_key: str) -> str:
    """Encrypt a private key for storage"""
    if not FERNET_AVAILABLE:
        logger.warning("Fernet not available - storing key unencrypted (NOT RECOMMENDED)")
        return private_key
    
    try:
        fernet = Fernet(get_fernet_key())
        encrypted = fernet.encrypt(private_key.encode())
        return base64.b64encode(encrypted).decode()
    except Exception as e:
        logger.error(f"Error encrypting private key: {e}")
        return private_key


def decrypt_private_key(encrypted_key: str) -> str:
    """Decrypt a stored private key"""
    if not FERNET_AVAILABLE:
        return encrypted_key
    
    try:
        fernet = Fernet(get_fernet_key())
        encrypted_bytes = base64.b64decode(encrypted_key.encode())
        decrypted = fernet.decrypt(encrypted_bytes)
        return decrypted.decode()
    except Exception as e:
        logger.error(f"Error decrypting private key: {e}")
        return encrypted_key


def init_user_wallets_table():
    """Initialize the user_wallets table"""
    try:
        conn = get_db_connection()
        if not conn:
            logger.warning("Could not connect to database for wallet table")
            return False
        
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_wallets (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                network VARCHAR(10) NOT NULL,
                address VARCHAR(100) NOT NULL,
                encrypted_private_key TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, network)
            )
        """)
        
        # Create indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_wallets_user_id ON user_wallets(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_wallets_network ON user_wallets(network)")
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("User wallets table initialized successfully")
        return True
    except Exception as e:
        logger.warning(f"Could not initialize user wallets table: {e}")
        return False


def save_user_wallet(user_id: int, network: str, address: str, private_key: str) -> bool:
    """Save a user's wallet address and encrypted private key to the database"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        encrypted_key = encrypt_private_key(private_key)
        
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO user_wallets (user_id, network, address, encrypted_private_key, created_at, updated_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id, network) DO UPDATE SET
                address = EXCLUDED.address,
                encrypted_private_key = EXCLUDED.encrypted_private_key,
                updated_at = CURRENT_TIMESTAMP
        """, (user_id, network, address, encrypted_key))
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Saved wallet for user {user_id} on {network}: {address}")
        return True
    except Exception as e:
        logger.warning(f"Could not save user wallet: {e}")
        return False


def get_user_wallet(user_id: int, network: str) -> Optional[Dict[str, str]]:
    """Get a user's wallet address and private key from the database"""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT address, encrypted_private_key 
            FROM user_wallets 
            WHERE user_id = %s AND network = %s
        """, (user_id, network))
        
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            return {
                'address': row['address'],
                'private_key': decrypt_private_key(row['encrypted_private_key']) if row['encrypted_private_key'] else ''
            }
        return None
    except Exception as e:
        logger.warning(f"Could not get user wallet: {e}")
        return None


def get_all_user_wallets(user_id: int) -> Dict[str, Dict[str, str]]:
    """Get all wallet addresses for a user"""
    try:
        conn = get_db_connection()
        if not conn:
            return {}
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT network, address, encrypted_private_key 
            FROM user_wallets 
            WHERE user_id = %s
        """, (user_id,))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        wallets = {}
        for row in rows:
            wallets[row['network']] = {
                'address': row['address'],
                'private_key': decrypt_private_key(row['encrypted_private_key']) if row['encrypted_private_key'] else ''
            }
        return wallets
    except Exception as e:
        logger.warning(f"Could not get user wallets: {e}")
        return {}


def load_all_wallets() -> Dict[int, Dict[str, Dict[str, str]]]:
    """Load all user wallets from database (for bot startup)"""
    try:
        conn = get_db_connection()
        if not conn:
            return {}
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT user_id, network, address, encrypted_private_key FROM user_wallets")
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        all_wallets = {}
        for row in rows:
            user_id = row['user_id']
            if user_id not in all_wallets:
                all_wallets[user_id] = {}
            
            all_wallets[user_id][row['network']] = {
                'address': row['address'],
                'private_key': decrypt_private_key(row['encrypted_private_key']) if row['encrypted_private_key'] else ''
            }
        
        logger.info(f"Loaded {len(all_wallets)} user wallets from database")
        return all_wallets
    except Exception as e:
        logger.warning(f"Could not load wallets from database: {e}")
        return {}


# ============================================================================
# USER BIO FLAGS (for service fee calculation)
# ============================================================================

def init_user_bio_flags_table():
    """Initialize the user_bio_flags table for storing @room bio status"""
    try:
        conn = get_db_connection()
        if not conn:
            logger.warning("Could not connect to database for bio flags table")
            return False
        
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_bio_flags (
                id SERIAL PRIMARY KEY,
                user_id BIGINT UNIQUE NOT NULL,
                username VARCHAR(100),
                has_room_bio BOOLEAN DEFAULT FALSE,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_bio_flags_user_id ON user_bio_flags(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_bio_flags_username ON user_bio_flags(username)")
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("User bio flags table initialized successfully")
        return True
    except Exception as e:
        logger.warning(f"Could not initialize user bio flags table: {e}")
        return False


def upsert_user_bio_flag(user_id: int, username: str, has_room_bio: bool) -> bool:
    """Save or update a user's @room bio flag"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO user_bio_flags (user_id, username, has_room_bio, checked_at, updated_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                has_room_bio = EXCLUDED.has_room_bio,
                checked_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
        """, (user_id, username, has_room_bio))
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Saved bio flag for user {user_id} (@{username}): has_room={has_room_bio}")
        return True
    except Exception as e:
        logger.warning(f"Could not save user bio flag: {e}")
        return False


def get_user_bio_flag(user_id: int) -> Optional[bool]:
    """Get a user's @room bio flag by user_id. Returns None if not found."""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT has_room_bio FROM user_bio_flags WHERE user_id = %s
        """, (user_id,))
        
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            return row['has_room_bio']
        return None
    except Exception as e:
        logger.warning(f"Could not get user bio flag: {e}")
        return None


def get_user_bio_flag_by_username(username: str) -> Optional[bool]:
    """Get a user's @room bio flag by username. Returns None if not found."""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        # Remove @ if present
        username = username.lstrip('@')
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT has_room_bio FROM user_bio_flags WHERE LOWER(username) = LOWER(%s)
        """, (username,))
        
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            return row['has_room_bio']
        return None
    except Exception as e:
        logger.warning(f"Could not get user bio flag by username: {e}")
        return None


# Initialize database on module import
if __name__ != "__main__":
    init_database()
    init_user_wallets_table()
    init_user_bio_flags_table()
