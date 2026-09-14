#!/usr/bin/env python3
"""
Deals Database Module for P2PMART Telegram Escrow Bot

This module provides a comprehensive database layer for storing and managing
all deal information. Each deal is uniquely identified by its chat_id (deal room ID).

The module ensures that deal information is properly isolated and won't get mixed
between different deals.
"""

import os
import json
import logging
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from datetime import datetime
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Deal status constants
DEAL_STATUS_PENDING = 'pending'
DEAL_STATUS_ROLES_SELECTED = 'roles_selected'
DEAL_STATUS_DETAILS_ENTERED = 'details_entered'
DEAL_STATUS_SUMMARY_SHOWN = 'summary_shown'
DEAL_STATUS_CONFIRMED = 'confirmed'
DEAL_STATUS_DEPOSIT_PENDING = 'deposit_pending'
DEAL_STATUS_DEPOSIT_RECEIVED = 'deposit_received'
DEAL_STATUS_RELEASE_PENDING = 'release_pending'
DEAL_STATUS_COMPLETED = 'completed'
DEAL_STATUS_CANCELLED = 'cancelled'


def get_db_connection():
    """Get database connection"""
    try:
        return psycopg2.connect(os.getenv('DATABASE_URL', ''))
    except Exception as e:
        logger.warning(f"DB connection error: {e}")
        return None


def init_deals_table():
    """Initialize the deals table in the database"""
    try:
        conn = get_db_connection()
        if not conn:
            logger.warning("Could not connect to database to initialize deals table")
            return False
        
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS deals (
                chat_id BIGINT PRIMARY KEY,
                buyer_username TEXT,
                seller_username TEXT,
                buyer_user_id BIGINT,
                seller_user_id BIGINT,
                amount DECIMAL(20, 8),
                rate DECIMAL(20, 8),
                payment_method TEXT,
                blockchain TEXT DEFAULT 'BSC',
                coin TEXT,
                buyer_address TEXT,
                seller_address TEXT,
                escrow_address TEXT,
                tx_hash TEXT,
                deal_status TEXT DEFAULT 'pending',
                buyer_approved BOOLEAN DEFAULT FALSE,
                seller_approved BOOLEAN DEFAULT FALSE,
                buyer_release_approved BOOLEAN DEFAULT FALSE,
                seller_release_approved BOOLEAN DEFAULT FALSE,
                creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                confirmed_time TIMESTAMP,
                deposit_time TIMESTAMP,
                completed_time TIMESTAMP,
                room_name TEXT,
                initiator_username TEXT,
                counterparty_username TEXT,
                extra_data JSONB DEFAULT '{}'::jsonb,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        columns_to_add = [
            ("buyer_username", "TEXT"),
            ("seller_username", "TEXT"),
            ("buyer_user_id", "BIGINT"),
            ("seller_user_id", "BIGINT"),
            ("amount", "DECIMAL(20, 8)"),
            ("rate", "DECIMAL(20, 8)"),
            ("payment_method", "TEXT"),
            ("blockchain", "TEXT DEFAULT 'BSC'"),
            ("coin", "TEXT"),
            ("buyer_address", "TEXT"),
            ("seller_address", "TEXT"),
            ("escrow_address", "TEXT"),
            ("tx_hash", "TEXT"),
            ("deal_status", "TEXT DEFAULT 'pending'"),
            ("buyer_approved", "BOOLEAN DEFAULT FALSE"),
            ("seller_approved", "BOOLEAN DEFAULT FALSE"),
            ("buyer_release_approved", "BOOLEAN DEFAULT FALSE"),
            ("seller_release_approved", "BOOLEAN DEFAULT FALSE"),
            ("creation_time", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
            ("confirmed_time", "TIMESTAMP"),
            ("deposit_time", "TIMESTAMP"),
            ("completed_time", "TIMESTAMP"),
            ("room_name", "TEXT"),
            ("initiator_username", "TEXT"),
            ("counterparty_username", "TEXT"),
            ("extra_data", "JSONB DEFAULT '{}'::jsonb"),
            ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ]
        
        for col_name, col_type in columns_to_add:
            try:
                cur.execute(f"""
                    ALTER TABLE deals ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                """)
            except Exception as col_err:
                logger.debug(f"Column {col_name} may already exist: {col_err}")
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_deals_status ON deals(deal_status)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_deals_buyer ON deals(buyer_username)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_deals_seller ON deals(seller_username)
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Deals table initialized successfully")
        return True
    except Exception as e:
        logger.warning(f"Could not initialize deals table: {e}")
        return False


def create_deal(chat_id: int, initiator_username: str = None, counterparty_username: str = None, room_name: str = None) -> bool:
    """Create a new deal record for a chat room"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO deals (chat_id, initiator_username, counterparty_username, room_name, deal_status, creation_time)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (chat_id) DO UPDATE SET
                initiator_username = COALESCE(EXCLUDED.initiator_username, deals.initiator_username),
                counterparty_username = COALESCE(EXCLUDED.counterparty_username, deals.counterparty_username),
                room_name = COALESCE(EXCLUDED.room_name, deals.room_name),
                updated_at = CURRENT_TIMESTAMP
        """, (chat_id, initiator_username, counterparty_username, room_name, DEAL_STATUS_PENDING, datetime.now()))
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Created/updated deal for chat_id {chat_id}")
        return True
    except Exception as e:
        logger.warning(f"Could not create deal: {e}")
        return False


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
        
        if row:
            return dict(row)
        return None
    except Exception as e:
        logger.warning(f"Could not get deal: {e}")
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
            'amount', 'rate', 'payment_method', 'blockchain', 'coin',
            'buyer_address', 'seller_address', 'escrow_address', 'tx_hash',
            'deal_status', 'buyer_approved', 'seller_approved',
            'buyer_release_approved', 'seller_release_approved',
            'confirmed_time', 'deposit_time', 'completed_time',
            'room_name', 'initiator_username', 'counterparty_username', 'extra_data'
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


def set_deal_roles(chat_id: int, buyer_username: str, seller_username: str, 
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


def set_deal_amount(chat_id: int, amount: float) -> bool:
    """Set the deal amount"""
    return update_deal(chat_id, amount=amount)


def set_deal_rate(chat_id: int, rate: float) -> bool:
    """Set the deal rate"""
    return update_deal(chat_id, rate=rate)


def set_deal_payment_method(chat_id: int, payment_method: str) -> bool:
    """Set the payment method"""
    return update_deal(chat_id, payment_method=payment_method)


def set_deal_blockchain(chat_id: int, blockchain: str) -> bool:
    """Set the blockchain"""
    return update_deal(chat_id, blockchain=blockchain)


def set_deal_coin(chat_id: int, coin: str) -> bool:
    """Set the coin type"""
    return update_deal(chat_id, coin=coin)


def set_buyer_address(chat_id: int, address: str) -> bool:
    """Set the buyer's wallet address"""
    return update_deal(chat_id, buyer_address=address)


def set_seller_address(chat_id: int, address: str) -> bool:
    """Set the seller's wallet address"""
    return update_deal(chat_id, seller_address=address)


def set_escrow_address(chat_id: int, address: str) -> bool:
    """Set the escrow deposit address"""
    return update_deal(chat_id, escrow_address=address)


def set_deal_details(chat_id: int, amount: float = None, rate: float = None, 
                     payment_method: str = None, blockchain: str = None, 
                     coin: str = None) -> bool:
    """Set multiple deal details at once"""
    kwargs = {}
    if amount is not None:
        kwargs['amount'] = amount
    if rate is not None:
        kwargs['rate'] = rate
    if payment_method is not None:
        kwargs['payment_method'] = payment_method
    if blockchain is not None:
        kwargs['blockchain'] = blockchain
    if coin is not None:
        kwargs['coin'] = coin
    
    if kwargs:
        kwargs['deal_status'] = DEAL_STATUS_DETAILS_ENTERED
    
    return update_deal(chat_id, **kwargs)


def approve_deal_summary(chat_id: int, role: str) -> Dict[str, bool]:
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
        logger.warning(f"Could not approve deal summary: {e}")
        return {'buyer': False, 'seller': False}


def confirm_deal(chat_id: int, escrow_address: str = None) -> bool:
    """Mark deal as confirmed (both parties approved)"""
    kwargs = {
        'deal_status': DEAL_STATUS_CONFIRMED,
        'buyer_approved': True,
        'seller_approved': True,
        'confirmed_time': datetime.now()
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
        deposit_time=datetime.now()
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
        completed_time=datetime.now()
    )


def cancel_deal(chat_id: int) -> bool:
    """Mark deal as cancelled"""
    return update_deal(chat_id, deal_status=DEAL_STATUS_CANCELLED)


def reset_deal(chat_id: int) -> bool:
    """Reset deal to initial state (for /restart command)"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cur = conn.cursor()
        cur.execute("""
            UPDATE deals SET
                buyer_username = NULL,
                seller_username = NULL,
                buyer_user_id = NULL,
                seller_user_id = NULL,
                amount = NULL,
                rate = NULL,
                payment_method = NULL,
                coin = NULL,
                buyer_address = NULL,
                seller_address = NULL,
                escrow_address = NULL,
                tx_hash = NULL,
                deal_status = %s,
                buyer_approved = FALSE,
                seller_approved = FALSE,
                buyer_release_approved = FALSE,
                seller_release_approved = FALSE,
                confirmed_time = NULL,
                deposit_time = NULL,
                completed_time = NULL,
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
        'chat_id': deal['chat_id'],
        'buyer': deal.get('buyer_username'),
        'seller': deal.get('seller_username'),
        'amount': float(deal['amount']) if deal.get('amount') else None,
        'rate': float(deal['rate']) if deal.get('rate') else None,
        'payment_method': deal.get('payment_method'),
        'blockchain': deal.get('blockchain', 'BSC'),
        'coin': deal.get('coin'),
        'buyer_address': deal.get('buyer_address'),
        'seller_address': deal.get('seller_address'),
        'escrow_address': deal.get('escrow_address'),
        'tx_hash': deal.get('tx_hash'),
        'status': deal.get('deal_status'),
        'buyer_approved': deal.get('buyer_approved', False),
        'seller_approved': deal.get('seller_approved', False),
        'creation_time': deal.get('creation_time'),
        'room_name': deal.get('room_name')
    }


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
            ORDER BY creation_time DESC
        """, (username, username))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return [dict(row) for row in rows]
    except Exception as e:
        logger.warning(f"Could not get deals by user: {e}")
        return []


def get_active_deals() -> List[Dict[str, Any]]:
    """Get all active (non-completed, non-cancelled) deals"""
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT * FROM deals 
            WHERE deal_status NOT IN (%s, %s)
            ORDER BY creation_time DESC
        """, (DEAL_STATUS_COMPLETED, DEAL_STATUS_CANCELLED))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return [dict(row) for row in rows]
    except Exception as e:
        logger.warning(f"Could not get active deals: {e}")
        return []


DEAL_STATUS_EXPIRED = 'expired'


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
            AND creation_time < NOW() - INTERVAL '%s hours'
            ORDER BY creation_time ASC
        """, (DEAL_STATUS_COMPLETED, DEAL_STATUS_CANCELLED, DEAL_STATUS_EXPIRED, hours))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return [dict(row) for row in rows]
    except Exception as e:
        logger.warning(f"Could not get expired deals: {e}")
        return []


def auto_close_expired_deal(chat_id: int) -> bool:
    """Mark a deal as expired/auto-closed after 12 hours"""
    return update_deal(
        chat_id,
        deal_status=DEAL_STATUS_EXPIRED,
        completed_time=datetime.now()
    )


def store_extra_data(chat_id: int, key: str, value: Any) -> bool:
    """Store additional data in the extra_data JSON field"""
    try:
        deal = get_deal(chat_id)
        if not deal:
            return False
        
        extra_data = deal.get('extra_data') or {}
        extra_data[key] = value
        
        return update_deal(chat_id, extra_data=Json(extra_data))
    except Exception as e:
        logger.warning(f"Could not store extra data: {e}")
        return False


def get_extra_data(chat_id: int, key: str, default: Any = None) -> Any:
    """Get additional data from the extra_data JSON field"""
    try:
        deal = get_deal(chat_id)
        if not deal:
            return default
        
        extra_data = deal.get('extra_data') or {}
        return extra_data.get(key, default)
    except Exception as e:
        logger.warning(f"Could not get extra data: {e}")
        return default


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


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print("Initializing deals table...")
    if init_deals_table():
        print("Deals table initialized successfully!")
    else:
        print("Failed to initialize deals table")
