#!/usr/bin/env python3
"""
P2PMART Telegram UserBot
A userbot implementation for deal room creation only
"""

import os
import logging
import json
import asyncio
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.functions.channels import CreateChannelRequest, EditPhotoRequest, InviteToChannelRequest, EditAdminRequest, DeleteChannelRequest
from telethon.tl.functions.messages import ExportChatInviteRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import ChatAdminRights, InputChatPhoto, InputPhoto
import requests
from telethon.errors import SessionPasswordNeededError
from image_generator import generate_room_image
import database

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Suppress verbose library logging
logging.getLogger('telethon').setLevel(logging.WARNING)

# Telethon session configuration
SESSION_NAME = 'p2pmart_userbot'
API_ID = int(os.getenv('TELEGRAM_API_ID', '0'))
API_HASH = os.getenv('TELEGRAM_API_HASH', '')
PHONE_NUMBER = os.getenv('TELEGRAM_PHONE', '')

# Deal request queue file
DEAL_QUEUE_FILE = "deal_requests.json"
# Group deletion queue file
DELETE_QUEUE_FILE = "delete_requests.json"

# Store data
deal_rooms = {}
client = None

def get_next_room_number():
    """Get the next room number - cycles from 40 to 90, then restarts from 40"""
    global room_counter
    try:
        room_info_file = "deal_rooms.json"
        if os.path.exists(room_info_file):
            with open(room_info_file, 'r') as f:
                room_info = json.load(f)
            if room_info:
                max_room = max(info.get('room_number', 0) for info in room_info.values())
                next_room = max_room + 1
                # Cycle: if next_room > 90, restart from 40
                if next_room > 90:
                    next_room = 40
                # Ensure minimum is 40
                if next_room < 40:
                    next_room = 40
                return next_room
        return 40  # Default starting point
    except Exception as e:
        logger.warning(f"Could not read room numbers: {e}")
        return 40

# Initialize room counter from existing rooms
room_counter = get_next_room_number()


async def authenticate_client():
    """Authenticate the client with Telegram"""
    if not API_ID or not API_HASH:
        logger.error(
            "TELEGRAM_API_ID or TELEGRAM_API_HASH not set. "
            "Get them from https://my.telegram.org/apps"
        )
        return None
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.connect()
    
    if not await client.is_user_authorized():
        if not PHONE_NUMBER:
            logger.error("TELEGRAM_PHONE not set for initial login")
            return None
        
        await client.send_code_request(PHONE_NUMBER)
        try:
            code = input('Enter the code you received: ')
            await client.sign_in(PHONE_NUMBER, code)
        except SessionPasswordNeededError:
            password = input('Two-step verification enabled. Enter your password: ')
            await client.sign_in(password=password)
    
    return client


def read_deal_requests():
    """Read pending deal requests from queue"""
    try:
        if os.path.exists(DEAL_QUEUE_FILE):
            with open(DEAL_QUEUE_FILE, 'r') as f:
                requests = json.load(f)
                return [r for r in requests if r.get('status') == 'pending']
    except Exception as e:
        logger.error(f"Error reading deal requests: {e}")
    return []


def update_request_status(initiator_username, counterparty_username, status, result=None):
    """Update the status of a deal request"""
    try:
        requests = []
        if os.path.exists(DEAL_QUEUE_FILE):
            with open(DEAL_QUEUE_FILE, 'r') as f:
                requests = json.load(f)
        
        for req in requests:
            if (req.get('initiator_username') == initiator_username and 
                req.get('counterparty_username') == counterparty_username):
                req['status'] = status
                if result:
                    req['result'] = result
        
        with open(DEAL_QUEUE_FILE, 'w') as f:
            json.dump(requests, f, indent=2)
    except Exception as e:
        logger.error(f"Error updating request status: {e}")


def read_delete_requests():
    """Read pending group deletion requests from queue"""
    try:
        if os.path.exists(DELETE_QUEUE_FILE):
            with open(DELETE_QUEUE_FILE, 'r') as f:
                requests = json.load(f)
                return [r for r in requests if r.get('status') == 'pending']
    except Exception as e:
        logger.error(f"Error reading delete requests: {e}")
    return []


def update_delete_request_status(chat_id, status):
    """Update the status of a deletion request"""
    try:
        requests = []
        if os.path.exists(DELETE_QUEUE_FILE):
            with open(DELETE_QUEUE_FILE, 'r') as f:
                requests = json.load(f)
        
        for req in requests:
            if req.get('chat_id') == chat_id:
                req['status'] = status
        
        with open(DELETE_QUEUE_FILE, 'w') as f:
            json.dump(requests, f, indent=2)
    except Exception as e:
        logger.error(f"Error updating delete request status: {e}")


async def delete_group(client, chat_id):
    """Delete a group/channel by chat_id"""
    try:
        # Convert to the format Telethon expects
        if chat_id < 0:
            # Already negative, might need to convert
            entity_id = abs(chat_id)
            if entity_id > 1000000000000:
                entity_id = entity_id - 1000000000000
        else:
            entity_id = chat_id
        
        logger.info(f"🗑️ Attempting to delete group with ID: {entity_id}")
        
        # Get the channel entity
        try:
            entity = await client.get_entity(entity_id)
            await client(DeleteChannelRequest(channel=entity))
            logger.info(f"✅ Successfully deleted group {entity_id}")
            return True
        except Exception as e:
            # Try with negative format
            try:
                entity = await client.get_entity(-entity_id)
                await client(DeleteChannelRequest(channel=entity))
                logger.info(f"✅ Successfully deleted group {entity_id}")
                return True
            except Exception as e2:
                logger.error(f"❌ Failed to delete group {entity_id}: {e2}")
                return False
    except Exception as e:
        logger.error(f"❌ Error deleting group {chat_id}: {e}")
        return False


async def fetch_and_store_user_bio(client, username: str) -> bool:
    """Fetch a user's bio using Telethon and store the @room flag in database.
    Returns True if user has @room in bio, False otherwise."""
    try:
        # Get user entity by username
        username_clean = username.lstrip('@')
        entity = await client.get_entity(f"@{username_clean}")
        
        # Get full user info including bio
        full_user = await client(GetFullUserRequest(entity.id))
        bio = full_user.full_user.about or ""
        
        # Check if bio contains @room
        has_room = "@room" in bio.lower()
        
        # Store in database
        database.upsert_user_bio_flag(entity.id, username_clean, has_room)
        
        logger.info(f"📋 Bio check for @{username_clean} (ID: {entity.id}): has_room={has_room}")
        return has_room
    except Exception as e:
        logger.warning(f"Could not fetch bio for @{username}: {e}")
        return False


async def create_deal_room(client, initiator_username, counterparty_username, bot_token):
    """Create a deal room - NO MESSAGES SENT, ONLY GROUP CREATION"""
    global room_counter
    
    try:
        room_name = f"MM ROOM {room_counter}"
        room_number = room_counter
        room_counter += 1
        
        logger.info(f"Creating deal room: {room_name}")
        
        # Group description with available commands
        group_description = """📋 NOTES
ALL COMMANDS ARE CASE-SENSITIVE

/restart - Restart A Trade

/release - Release Funds To Buyer

/verify <Address> - Verify A Wallet Address Before Starting A Trade

/balance - Check Available Balance For Current Trade

/dispute <reason> - Report"""
        
        result = await client(CreateChannelRequest(
            title=room_name,
            about=group_description,
            megagroup=True
        ))
        
        chat_id = result.chats[0].id
        logger.info(f"✅ Group Created: {room_name} (ID: {chat_id})")
        
        # Fetch and store user bios for service fee calculation
        logger.info(f"📋 Fetching bios for @{initiator_username} and @{counterparty_username}")
        initiator_has_room = await fetch_and_store_user_bio(client, initiator_username)
        counterparty_has_room = await fetch_and_store_user_bio(client, counterparty_username)
        
        # Calculate fee tier based on bios
        if initiator_has_room and counterparty_has_room:
            fee_tier = "0.25%"
        elif initiator_has_room or counterparty_has_room:
            fee_tier = "0.5%"
        else:
            fee_tier = "0.75%"
        logger.info(f"💰 Fee tier calculated: {fee_tier} (initiator_has_room={initiator_has_room}, counterparty_has_room={counterparty_has_room})")
        
        # Store initial deal room info immediately (before bot joins)
        deal_rooms[chat_id] = {
            'room_number': room_number,
            'room_name': room_name,
            'initiator_username': initiator_username,
            'counterparty_username': counterparty_username,
            'invite_link': '',
            'chat_id': chat_id,
            'bot_invite_link': ''
        }
        
        # Save to file so bot can access it when joining
        try:
            room_info_file = "deal_rooms.json"
            room_info = {}
            if os.path.exists(room_info_file):
                with open(room_info_file, 'r') as f:
                    room_info = json.load(f)
            
            room_info[str(chat_id)] = deal_rooms[chat_id]
            
            with open(room_info_file, 'w') as f:
                json.dump(room_info, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save room info initially: {e}")
        
        # Generate and set group profile picture
        try:
            image_path = generate_room_image(room_number)
            await client(EditPhotoRequest(
                channel=chat_id,
                photo=await client.upload_file(image_path)
            ))
            logger.info(f"✅ Profile picture set for {room_name}")
            if os.path.exists(image_path):
                os.remove(image_path)
        except Exception as e:
            logger.warning(f"Could not set profile picture: {e}")
        
        # Get bot username and add it to the room
        bot_invite_link = None
        try:
            if bot_token:
                # Get bot username using Telegram API
                bot_api_url = f"https://api.telegram.org/bot{bot_token}/getMe"
                response = requests.get(bot_api_url)
                if response.status_code == 200:
                    bot_data = response.json().get('result', {})
                    bot_username = bot_data.get('username')
                    
                    if bot_username:
                        # Create invite link for bot to join
                        bot_invite_result = await client(ExportChatInviteRequest(
                            peer=chat_id,
                            expire_date=None,
                            usage_limit=None,
                            request_needed=False
                        ))
                        bot_invite_link = str(bot_invite_result.link)
                        logger.info(f"✅ Bot invite link created for {room_name}")
                        
                        # Try to add bot via username
                        bot_added = False
                        bot_promoted = False
                        try:
                            bot_entity = await client.get_entity(f"@{bot_username}")
                            await client(InviteToChannelRequest(
                                channel=chat_id,
                                users=[bot_entity]
                            ))
                            logger.info(f"✅ Bot added to {room_name}")
                            bot_added = True
                            
                            # Promote bot as admin
                            admin_rights = ChatAdminRights(
                                change_info=True,
                                post_messages=True,
                                edit_messages=True,
                                delete_messages=True,
                                ban_users=True,
                                invite_users=True,
                                pin_messages=True,
                                add_admins=False,
                                manage_call=False
                            )
                            await client(EditAdminRequest(
                                channel=chat_id,
                                user_id=bot_entity.id,
                                admin_rights=admin_rights,
                                rank="MM"
                            ))
                            logger.info(f"✅ Bot promoted as admin with MM rank in {room_name}")
                            bot_promoted = True
                            
                            # Also add +918240720413 as admin with same rights
                            try:
                                admin_entity = await client.get_entity("+918240720413")
                                await client(InviteToChannelRequest(
                                    channel=chat_id,
                                    users=[admin_entity]
                                ))
                                logger.info(f"✅ Admin account added to {room_name}")
                                
                                # Promote admin account with same rights as bot
                                admin_account_rights = ChatAdminRights(
                                    change_info=True,
                                    post_messages=True,
                                    edit_messages=True,
                                    delete_messages=True,
                                    ban_users=True,
                                    invite_users=True,
                                    pin_messages=True,
                                    add_admins=False,
                                    manage_call=False
                                )
                                await client(EditAdminRequest(
                                    channel=chat_id,
                                    user_id=admin_entity.id,
                                    admin_rights=admin_account_rights,
                                    rank="admin"
                                ))
                                logger.info(f"✅ Admin account promoted with admin rank in {room_name}")
                            except Exception as e:
                                logger.warning(f"Could not add/promote admin account: {e}")
                            
                            # Delete only initial system messages (first 3) when group is created
                            # NOTE: Only delete true service messages (action + no text) to preserve bot messages
                            try:
                                await asyncio.sleep(0.5)  # Small delay to ensure system messages are created
                                
                                # Only collect TRUE system messages (service messages with action AND no text content)
                                system_msg_ids = []
                                async for msg in client.iter_messages(chat_id, limit=20):
                                    # Check if it's a TRUE system message (has action AND no text/message content)
                                    # This ensures we don't delete bot messages like "Waiting for @username"
                                    if msg.action is not None and (msg.message is None or msg.message == ''):
                                        system_msg_ids.append(msg.id)
                                        # Only collect first 3 system messages
                                        if len(system_msg_ids) >= 3:
                                            break
                                
                                if system_msg_ids:
                                    # Delete only the first 3 system messages
                                    deleted_count = 0
                                    for msg_id in system_msg_ids:
                                        try:
                                            await client.delete_messages(chat_id, msg_id)
                                            deleted_count += 1
                                        except:
                                            pass  # Continue trying other messages
                                        await asyncio.sleep(0.02)  # Small delay between deletions
                                    
                                    if deleted_count > 0:
                                        logger.info(f"✅ Cleared {deleted_count} system messages from {room_name}")
                                    else:
                                        logger.warning(f"⚠️ Could not delete system messages in {room_name}")
                                else:
                                    logger.info(f"ℹ️ No system messages to clear in {room_name}")
                            except Exception as e:
                                logger.warning(f"Could not clear initial system messages: {e}")
                        except Exception as e:
                            logger.warning(f"Could not add/promote bot via username: {e}")
                        
                        # Only create user invite link after bot is successfully added and promoted
                        if bot_added and bot_promoted:
                            try:
                                invite_result = await client(ExportChatInviteRequest(
                                    peer=chat_id,
                                    expire_date=None,
                                    usage_limit=None,
                                    request_needed=True
                                ))
                                invite_link = str(invite_result.link)
                                logger.info(f"✅ User invite link created after bot promotion in {room_name}: {invite_link}")
                            except Exception as e:
                                logger.warning(f"Could not create user invite link: {e}")
                                invite_link = None
                        else:
                            invite_link = None
                            logger.warning(f"Skipping user invite link creation - bot not ready in {room_name}")
        except Exception as e:
            logger.warning(f"Could not get bot info: {e}")
            invite_link = None
        
        # Update deal room info with final details
        deal_rooms[chat_id] = {
            'room_number': room_number,
            'room_name': room_name,
            'initiator_username': initiator_username,
            'counterparty_username': counterparty_username,
            'invite_link': str(invite_link),
            'chat_id': chat_id,
            'bot_invite_link': bot_invite_link,
            'fee_tier': fee_tier
        }
        
        # Update deal room info file with final details
        try:
            room_info_file = "deal_rooms.json"
            room_info = {}
            if os.path.exists(room_info_file):
                with open(room_info_file, 'r') as f:
                    room_info = json.load(f)
            
            room_info[str(chat_id)] = deal_rooms[chat_id]
            
            with open(room_info_file, 'w') as f:
                json.dump(room_info, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not update room info: {e}")
        
        return chat_id, room_name, invite_link
        
    except Exception as e:
        logger.error(f"Error creating deal room: {e}")
        return None, None, None




async def process_deal_requests(client):
    """Continuously process deal requests and deletion requests from the queues"""
    while True:
        try:
            # Process deal creation requests
            requests = read_deal_requests()
            if requests:
                for req in requests:
                    initiator_username = req.get('initiator_username')
                    counterparty_username = req.get('counterparty_username')
                    bot_token = req.get('bot_token', '')
                    
                    chat_id, room_name, invite_link = await create_deal_room(
                        client,
                        initiator_username,
                        counterparty_username,
                        bot_token
                    )
                    
                    if chat_id:
                        bot_invite_link = deal_rooms.get(chat_id, {}).get('bot_invite_link', '')
                        fee_tier = deal_rooms.get(chat_id, {}).get('fee_tier', '0.75%')
                        update_request_status(
                            initiator_username,
                            counterparty_username,
                            'completed',
                            {'chat_id': chat_id, 'room_name': room_name, 'invite_link': str(invite_link), 'bot_invite_link': bot_invite_link, 'fee_tier': fee_tier}
                        )
                    else:
                        update_request_status(
                            initiator_username,
                            counterparty_username,
                            'failed'
                        )
            
            # Process group deletion requests
            delete_requests = read_delete_requests()
            if delete_requests:
                for req in delete_requests:
                    chat_id = req.get('chat_id')
                    room_name = req.get('room_name', 'Unknown')
                    
                    logger.info(f"🗑️ Processing deletion request for {room_name} (chat_id: {chat_id})")
                    
                    success = await delete_group(client, chat_id)
                    
                    if success:
                        update_delete_request_status(chat_id, 'completed')
                        logger.info(f"✅ Deleted group {room_name}")
                    else:
                        update_delete_request_status(chat_id, 'failed')
                        logger.warning(f"❌ Failed to delete group {room_name}")
            
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Error in process_deal_requests: {e}")
            await asyncio.sleep(5)


async def main():
    """Start the userbot"""
    global client
    
    client = await authenticate_client()
    
    if not client:
        logger.error("Failed to authenticate client")
        return
    
    try:
        logger.info("✅ UserBot Started - Processing deal room requests")
        
        # Start processing deal requests in background
        await process_deal_requests(client)
        
    except KeyboardInterrupt:
        logger.info("UserBot stopped")
    finally:
        await client.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
