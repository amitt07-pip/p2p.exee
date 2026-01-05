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
                room_number INTEGER
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
            'room_name', 'room_number'
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


# Initialize database on module import
if __name__ != "__main__":
    init_database()
    init_user_wallets_table()
