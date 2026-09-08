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
from telethon.tl.functions.channels import EditBannedRequest, TogglePreHistoryHiddenRequest
from telethon.tl.functions.messages import ExportChatInviteRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import (
    ChatAdminRights,
    ChatBannedRights,
    InputChatPhoto,
    InputPhoto,
    ChannelParticipantAdmin,
    ChannelParticipantCreator,
    ChatPhotoEmpty
)
import requests
from telethon.errors import SessionPasswordNeededError, FloodWaitError
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
# Queue for /startroom requests that pre-create the room pool
PREWARM_QUEUE_FILE = "prewarm_requests.json"
# Pool of premade, fully set up rooms waiting to be assigned to a deal
ROOM_POOL_FILE = "room_pool.json"
# Queue for releasing an assigned room back into the pool (after /close)
RELEASE_QUEUE_FILE = "release_requests.json"

# Store data
deal_rooms = {}
client = None

# Room numbers stay within this inclusive range and wrap back to the minimum.
ROOM_NUMBER_MIN = 1
ROOM_NUMBER_MAX = 20

# Flood waits up to this many seconds are slept through; anything longer is
# reported so /startroom can stop and show the rate limit.
FLOOD_WAIT_TOLERATED = 30
# Pause between premade room creations, to stay under Telegram's create limits.
PREWARM_ROOM_DELAY = 4

# Accounts added to every new room and promoted with the same rights and
# "admin" rank as the other room admins. Lookups try username, then user id,
# then phone number.
EXTRA_ROOM_MEMBERS = [
    {'username': '@peakybiinder89', 'user_id': 7244135096, 'phone': '+91401898002'},
    {'username': '@asknigge', 'user_id': 8117659015, 'phone': '+919058747049'},
    {'username': '@xdekku', 'user_id': 6564907309, 'phone': '+12075710381'},
    {'username': '@EpicGuardianBot', 'user_id': None, 'phone': None},
]

# Permanent room admin accounts added to every room alongside the bot.
FIXED_ROOM_ADMINS = [
    {'username': None, 'user_id': None, 'phone': '+918240720413', 'label': 'admin account'},
    {'username': '@AisoIutions04', 'user_id': 7629970378, 'phone': '+16592202558', 'label': '@AisoIutions04'},
]


async def invite_user(client, chat_id, entity, label, room_name):
    """Add a user to a room. Short flood limits are waited out and longer ones
    are raised; an already-present user counts as success."""
    for attempt in range(1, 4):
        try:
            await client(InviteToChannelRequest(channel=chat_id, users=[entity]))
            logger.info(f"✅ {label} added to {room_name}")
            return True
        except FloodWaitError as e:
            if e.seconds > FLOOD_WAIT_TOLERATED:
                raise
            logger.warning(
                f"⏳ Flood wait {e.seconds}s adding {label} to {room_name} "
                f"(try {attempt}/3)"
            )
            await asyncio.sleep(e.seconds + 2)
        except Exception as e:
            if 'already' in str(e).lower():
                return True
            logger.warning(f"Could not add {label} to {room_name}: {e}")
            return False
    return False


async def promote_user(client, chat_id, user_id, rank, label, room_name, add_admins=False):
    """Promote a user to admin in a room, waiting out flood limits."""
    rights = ChatAdminRights(
        change_info=True,
        post_messages=True,
        edit_messages=True,
        delete_messages=True,
        ban_users=True,
        invite_users=True,
        pin_messages=True,
        add_admins=add_admins,
        manage_call=False
    )
    for attempt in range(1, 4):
        try:
            await client(EditAdminRequest(
                channel=chat_id,
                user_id=user_id,
                admin_rights=rights,
                rank=rank
            ))
            logger.info(f"✅ {label} promoted as admin ({rank}) in {room_name}")
            return True
        except FloodWaitError as e:
            if e.seconds > FLOOD_WAIT_TOLERATED:
                raise
            logger.warning(
                f"⏳ Flood wait {e.seconds}s promoting {label} in {room_name} "
                f"(try {attempt}/3)"
            )
            await asyncio.sleep(e.seconds + 2)
        except Exception as e:
            logger.warning(f"Could not promote {label} in {room_name}: {e}")
            return False
    return False


async def export_invite(client, chat_id, room_name, request_needed=True):
    """Create an invite link for a room, waiting out flood limits."""
    for attempt in range(1, 4):
        try:
            result = await client(ExportChatInviteRequest(
                peer=chat_id,
                expire_date=None,
                usage_limit=None,
                request_needed=request_needed
            ))
            return str(result.link)
        except FloodWaitError as e:
            if e.seconds > FLOOD_WAIT_TOLERATED:
                raise
            logger.warning(
                f"⏳ Flood wait {e.seconds}s creating invite link for {room_name} "
                f"(try {attempt}/3)"
            )
            await asyncio.sleep(e.seconds + 2)
        except Exception as e:
            logger.warning(f"Could not create invite link for {room_name}: {e}")
            return None
    return None


async def resolve_bot_entity(client, bot_token):
    """Look up the bot's Telegram entity from its token."""
    if not bot_token:
        logger.warning("No bot token available - cannot add the bot to the room")
        return None
    try:
        response = requests.get(f"https://api.telegram.org/bot{bot_token}/getMe", timeout=15)
        if response.status_code != 200:
            logger.warning(f"getMe failed with status {response.status_code}")
            return None
        bot_username = response.json().get('result', {}).get('username')
    except Exception as e:
        logger.warning(f"Could not get bot info: {e}")
        return None
    if not bot_username:
        return None
    try:
        return await client.get_entity(f"@{bot_username}")
    except Exception as e:
        logger.warning(f"Could not resolve @{bot_username}: {e}")
        return None


async def add_fixed_room_admins(client, chat_id, room_name):
    """Add and promote the permanent room admin accounts."""
    for admin in FIXED_ROOM_ADMINS:
        entity = await resolve_entity(
            client,
            username=admin['username'],
            user_id=admin['user_id'],
            phone=admin['phone']
        )
        if not entity:
            logger.warning(f"Could not find {admin['label']} to add to {room_name}")
            continue
        if await invite_user(client, chat_id, entity, admin['label'], room_name):
            await promote_user(client, chat_id, entity.id, "admin", admin['label'], room_name)


async def resolve_entity(client, username=None, user_id=None, phone=None):
    """Resolve a Telegram entity by username, then user id, then phone number."""
    for identifier in (username, user_id, phone):
        if not identifier:
            continue
        try:
            return await client.get_entity(identifier)
        except Exception:
            continue
    return None


async def delete_service_messages(client, chat_id, limit=60):
    """Delete Telegram service messages (joins, invites, promotions) in a room.
    Only messages with an action and no text are touched, so bot messages stay."""
    deleted = 0
    try:
        service_msg_ids = []
        async for msg in client.iter_messages(chat_id, limit=limit):
            if msg.action is not None and (msg.message is None or msg.message == ''):
                service_msg_ids.append(msg.id)
        for msg_id in service_msg_ids:
            try:
                await client.delete_messages(chat_id, msg_id)
                deleted += 1
            except Exception:
                pass
            await asyncio.sleep(0.02)
        if deleted:
            logger.info(f"✅ Cleared {deleted} service messages from chat {chat_id}")
    except Exception as e:
        logger.warning(f"Could not clear service messages in chat {chat_id}: {e}")
    return deleted


async def add_extra_room_members(client, chat_id, room_name, sweep_delays=(1.0, 8.0, 20.0)):
    """Add the fixed set of accounts to a new room and promote them as admins.
    Runs in the background so room creation is not delayed, and clears the
    invite/promote service messages afterwards so traders never see them."""
    for member in EXTRA_ROOM_MEMBERS:
        label = str(member['username'] or member['user_id'])
        entity = await resolve_entity(
            client,
            username=member['username'],
            user_id=member['user_id'],
            phone=member['phone']
        )
        if not entity:
            logger.warning(f"Could not find {label} to add to {room_name}")
            continue
        if await invite_user(client, chat_id, entity, label, room_name):
            await promote_user(client, chat_id, entity.id, "admin", label, room_name)

    # Clear the "X invited Y" / "Y joined" notices these adds produced, then
    # sweep again to catch the buyer/seller joining via the invite link.
    for delay in sweep_delays:
        await asyncio.sleep(delay)
        await delete_service_messages(client, chat_id)


async def add_extra_room_members_background(client, chat_id, room_name):
    """Background variant that never raises, so a rate limit while adding the
    admins cannot take down the task that created a trader's room."""
    try:
        await add_extra_room_members(client, chat_id, room_name)
    except FloodWaitError as e:
        logger.warning(f"Flood wait {e.seconds}s while adding admins to {room_name}")
    except Exception as e:
        logger.warning(f"Could not add admins to {room_name}: {e}")


def get_next_room_number():
    """Get the next room number - cycles from 1 to 20, then restarts from 1"""
    global room_counter
    try:
        room_info_file = "deal_rooms.json"
        if os.path.exists(room_info_file):
            with open(room_info_file, 'r') as f:
                room_info = json.load(f)
            if room_info:
                max_room = max(info.get('room_number', 0) for info in room_info.values())
                next_room = max_room + 1
                # Keep the number inside 1..20, wrapping back to 1
                if next_room > ROOM_NUMBER_MAX or next_room < ROOM_NUMBER_MIN:
                    next_room = ROOM_NUMBER_MIN
                return next_room
        return ROOM_NUMBER_MIN  # Default starting point
    except Exception as e:
        logger.warning(f"Could not read room numbers: {e}")
        return ROOM_NUMBER_MIN

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
    
    try:
        client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    except (ValueError, Exception) as e:
        # Session file is corrupted or incompatible with current Telethon version
        session_file = f"{SESSION_NAME}.session"
        logger.warning(f"⚠️ Session file corrupt ({e}), deleting {session_file} and retrying...")
        if os.path.exists(session_file):
            os.remove(session_file)
        client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    await client.connect()
    
    if not await client.is_user_authorized():
        if not PHONE_NUMBER:
            logger.error("TELEGRAM_PHONE not set for initial login")
            return None
        
        # Check if we're running in a non-interactive environment (e.g. systemd service)
        import sys
        if not sys.stdin or not sys.stdin.isatty():
            logger.error(
                "❌ UserBot session expired or missing. Interactive login required.\n"
                "   Run manually: cd /root/p2p && source venv/bin/activate && python p2pmart.py\n"
                "   Then restart the service after authentication completes."
            )
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


def read_json_list(path):
    """Read a JSON list file, returning [] when missing or unreadable."""
    try:
        if os.path.exists(path):
            with open(path, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
    except Exception as e:
        logger.error(f"Error reading {path}: {e}")
    return []


def write_json_list(path, data):
    """Write a JSON list file."""
    try:
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Error writing {path}: {e}")


def read_room_pool():
    """Premade rooms waiting to be assigned, ordered by room number."""
    pool = read_json_list(ROOM_POOL_FILE)
    return sorted(pool, key=lambda entry: entry.get('room_number', 0))


def add_room_to_pool(entry):
    """Add (or refresh) a premade room in the pool."""
    pool = [e for e in read_room_pool() if e.get('chat_id') != entry.get('chat_id')]
    pool.append(entry)
    write_json_list(ROOM_POOL_FILE, pool)


def take_pooled_room(requested_room_number=None):
    """Remove and return a premade room from the pool, preferring the requested
    room number when it is available. Returns None when the pool is empty."""
    pool = read_room_pool()
    if not pool:
        return None
    chosen = None
    if requested_room_number:
        for entry in pool:
            if entry.get('room_number') == requested_room_number:
                chosen = entry
                break
    if chosen is None:
        chosen = pool[0]
    write_json_list(ROOM_POOL_FILE, [e for e in pool if e.get('chat_id') != chosen.get('chat_id')])
    return chosen


def read_prewarm_requests():
    """Pending /startroom requests."""
    return [r for r in read_json_list(PREWARM_QUEUE_FILE) if r.get('status') == 'pending']


def is_prewarm_cancelled(request_id):
    """True once the Cancel button on the /startroom message has been used."""
    for req in read_json_list(PREWARM_QUEUE_FILE):
        if req.get('request_id') == request_id:
            return bool(req.get('cancel'))
    return False


def update_prewarm_request_status(request_id, status, result=None):
    """Update the status of a /startroom request."""
    requests_data = read_json_list(PREWARM_QUEUE_FILE)
    for req in requests_data:
        if req.get('request_id') == request_id:
            req['status'] = status
            if result:
                req['result'] = result
    write_json_list(PREWARM_QUEUE_FILE, requests_data)


def read_release_requests():
    """Pending requests to return a room to the pool."""
    return [r for r in read_json_list(RELEASE_QUEUE_FILE) if r.get('status') == 'pending']


def update_release_request_status(chat_id, status):
    """Update the status of a pool-release request."""
    requests_data = read_json_list(RELEASE_QUEUE_FILE)
    for req in requests_data:
        if req.get('chat_id') == chat_id:
            req['status'] = status
    write_json_list(RELEASE_QUEUE_FILE, requests_data)


async def get_room_entity(client, chat_id):
    """Resolve a room entity from a stored (positive) chat id."""
    base = abs(chat_id)
    if base > 1000000000000:
        base = base - 1000000000000
    for candidate in (base, -1000000000000 - base, -base):
        try:
            return await client.get_entity(candidate)
        except Exception:
            continue
    return None


async def kick_normal_members(client, chat_id, room_name):
    """Kick every ordinary member of a room, keeping admins, the creator and bots.
    Works regardless of whether deal roles were ever selected."""
    kicked = 0
    kick_rights = ChatBannedRights(until_date=None, view_messages=True)
    unban_rights = ChatBannedRights(until_date=None, view_messages=False)
    try:
        async for participant in client.iter_participants(chat_id):
            status = participant.participant
            if isinstance(status, (ChannelParticipantAdmin, ChannelParticipantCreator)):
                continue
            if participant.bot:
                continue
            try:
                await client(EditBannedRequest(chat_id, participant.id, kick_rights))
                await client(EditBannedRequest(chat_id, participant.id, unban_rights))
                kicked += 1
            except Exception as e:
                logger.warning(f"Could not kick {participant.id} from {room_name}: {e}")
            await asyncio.sleep(0.05)
        logger.info(f"👢 Kicked {kicked} member(s) from {room_name}")
    except Exception as e:
        logger.warning(f"Could not list members of {room_name}: {e}")
    return kicked


async def set_room_photo(client, chat_id, room_number, room_name, attempts=3):
    """Set a room's profile picture, retrying transient failures and waiting out
    short flood limits (the picture step is often the first to be throttled)."""
    image_path = None
    try:
        image_path = generate_room_image(room_number)
    except Exception as e:
        logger.warning(f"Could not generate image for {room_name}: {e}")
        return False

    try:
        for attempt in range(1, attempts + 1):
            try:
                await client(EditPhotoRequest(
                    channel=chat_id,
                    photo=await client.upload_file(image_path)
                ))
                logger.info(f"✅ Profile picture set for {room_name}")
                return True
            except FloodWaitError as e:
                if e.seconds > FLOOD_WAIT_TOLERATED or attempt == attempts:
                    raise
                logger.info(f"⏳ Waiting {e.seconds}s before retrying picture for {room_name}")
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                logger.warning(f"Could not set profile picture for {room_name} (try {attempt}): {e}")
                await asyncio.sleep(2)
        return False
    finally:
        if image_path and os.path.exists(image_path):
            os.remove(image_path)


async def repair_pool_rooms(client, bot_token, request_id=None):
    """Finish the setup of premade rooms that lost a step to a flood limit:
    picture, hidden history, bot, fixed admins and invite link. Every step is
    safe to repeat, so this can run after each /startroom."""
    repaired = []
    bot_entity = await resolve_bot_entity(client, bot_token)
    for entry in read_room_pool():
        if request_id and is_prewarm_cancelled(request_id):
            logger.info("🛑 Room setup repair cancelled")
            break
        chat_id = entry.get('chat_id')
        room_number = entry.get('room_number')
        room_name = entry.get('room_name', f'MM ROOM {room_number}')
        if not chat_id or not room_number:
            continue
        entity = await get_room_entity(client, chat_id)
        if entity is None:
            continue

        changed = False
        if isinstance(entity.photo, ChatPhotoEmpty):
            if await set_room_photo(client, entity, room_number, room_name):
                changed = True

        if bot_entity and await invite_user(client, entity, bot_entity, 'bot', room_name):
            await promote_user(client, entity, bot_entity.id, 'MM', 'bot', room_name)

        await add_fixed_room_admins(client, entity, room_name)
        await add_extra_room_members(client, entity, room_name, sweep_delays=(1.0,))

        if not entry.get('invite_link'):
            invite_link = await export_invite(client, entity, room_name, request_needed=True)
            if invite_link:
                entry['invite_link'] = invite_link
                add_room_to_pool(entry)
                changed = True

        if changed:
            repaired.append(room_number)
        await asyncio.sleep(1)
    if repaired:
        logger.info(f"🛠️ Completed setup for room(s): {repaired}")
    return repaired


async def hide_room_history(client, chat_id, room_name):
    """Hide the chat history from members who join later."""
    try:
        await client(TogglePreHistoryHiddenRequest(channel=chat_id, enabled=True))
        logger.info(f"🙈 Chat history hidden for new members in {room_name}")
        return True
    except Exception as e:
        logger.warning(f"Could not hide chat history for {room_name}: {e}")
        return False


async def clear_room_messages(client, chat_id, limit=300):
    """Wipe a room's history so the next deal starts on a clean room."""
    deleted = 0
    try:
        msg_ids = [msg.id async for msg in client.iter_messages(chat_id, limit=limit)]
        for i in range(0, len(msg_ids), 100):
            batch = msg_ids[i:i + 100]
            try:
                await client.delete_messages(chat_id, batch)
                deleted += len(batch)
            except Exception as e:
                logger.warning(f"Could not delete messages in chat {chat_id}: {e}")
            await asyncio.sleep(0.05)
        if deleted:
            logger.info(f"🧹 Cleared {deleted} messages from chat {chat_id}")
    except Exception as e:
        logger.warning(f"Could not clear messages in chat {chat_id}: {e}")
    return deleted


async def release_room_to_pool(client, chat_id, room_number, room_name):
    """Return a used room to the premade pool: kick the traders, wipe the history
    and make it available again. The room itself is never deleted."""
    entity = await get_room_entity(client, chat_id)
    if entity is None:
        logger.warning(f"Could not resolve {room_name} (chat_id {chat_id}) to release it")
        return False
    await kick_normal_members(client, entity, room_name)
    await clear_room_messages(client, entity)
    await hide_room_history(client, entity, room_name)
    invite_link = ''
    try:
        invite_result = await client(ExportChatInviteRequest(
            peer=entity,
            expire_date=None,
            usage_limit=None,
            request_needed=True
        ))
        invite_link = str(invite_result.link)
    except Exception as e:
        logger.warning(f"Could not refresh invite link for {room_name}: {e}")
    add_room_to_pool({
        'chat_id': chat_id,
        'room_number': room_number,
        'room_name': room_name,
        'invite_link': invite_link,
        'bot_invite_link': ''
    })
    logger.info(f"♻️ {room_name} returned to the premade room pool")
    return True


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


async def fetch_and_store_user_bio_by_id(client, user_id: int) -> bool:
    """Fetch a user's bio using Telethon by user ID and store the @room flag in database.
    Returns True if user has @room in bio, False otherwise."""
    try:
        # Get user entity by user ID
        entity = await client.get_entity(user_id)
        
        # Get full user info including bio
        full_user = await client(GetFullUserRequest(entity.id))
        bio = full_user.full_user.about or ""
        
        # Check if bio contains @room
        has_room = "@room" in bio.lower()
        
        # Get username if available
        username = entity.username or f"user_{user_id}"
        
        # Store in database
        database.upsert_user_bio_flag(entity.id, username, has_room)
        
        logger.info(f"📋 Bio check for User {user_id} (username: @{username}): has_room={has_room}")
        return has_room
    except Exception as e:
        logger.warning(f"Could not fetch bio for User {user_id}: {e}")
        return False


async def compute_fee_tier(client, initiator_username, counterparty_username, counterparty_user_id=None):
    """Fee tier from whether both/one/neither participant has @room in their bio."""
    initiator_has_room = await fetch_and_store_user_bio(client, initiator_username)
    if counterparty_user_id:
        counterparty_has_room = await fetch_and_store_user_bio_by_id(client, counterparty_user_id)
    else:
        counterparty_has_room = await fetch_and_store_user_bio(client, counterparty_username)

    if initiator_has_room and counterparty_has_room:
        fee_tier = "0.25%"
    elif initiator_has_room or counterparty_has_room:
        fee_tier = "0.5%"
    else:
        fee_tier = "0.75%"
    logger.info(f"💰 Fee tier calculated: {fee_tier} (initiator_has_room={initiator_has_room}, counterparty_has_room={counterparty_has_room})")
    return fee_tier


def save_room_info(chat_id, info):
    """Persist a room's info to deal_rooms.json so the bot can read it."""
    try:
        room_info_file = "deal_rooms.json"
        room_info = {}
        if os.path.exists(room_info_file):
            with open(room_info_file, 'r') as f:
                room_info = json.load(f)
        room_info[str(chat_id)] = info
        with open(room_info_file, 'w') as f:
            json.dump(room_info, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not save room info: {e}")


async def assign_pooled_room(client, entry, initiator_username, counterparty_username, counterparty_user_id=None):
    """Assign an already premade room to a deal. Only the fee tier is looked up,
    so the participants get their invite link almost immediately."""
    chat_id = entry['chat_id']
    room_number = entry['room_number']
    room_name = entry.get('room_name', f"MM ROOM {room_number}")
    invite_link = entry.get('invite_link') or ''
    logger.info(f"⚡ Assigning premade room {room_name} (ID: {chat_id})")

    if not invite_link:
        # The room was premade without a usable link (e.g. throttled) - make one now
        entity = await get_room_entity(client, chat_id)
        if entity is not None:
            try:
                invite_result = await client(ExportChatInviteRequest(
                    peer=entity,
                    expire_date=None,
                    usage_limit=None,
                    request_needed=True
                ))
                invite_link = str(invite_result.link)
            except Exception as e:
                logger.warning(f"Could not create invite link for {room_name}: {e}")

    fee_tier = await compute_fee_tier(client, initiator_username, counterparty_username, counterparty_user_id)

    deal_rooms[chat_id] = {
        'room_number': room_number,
        'room_name': room_name,
        'initiator_username': initiator_username,
        'counterparty_username': counterparty_username,
        'counterparty_user_id': counterparty_user_id,
        'invite_link': str(invite_link),
        'chat_id': chat_id,
        'bot_invite_link': entry.get('bot_invite_link', ''),
        'fee_tier': fee_tier,
        'premade': True
    }
    save_room_info(chat_id, deal_rooms[chat_id])
    return chat_id, room_name, invite_link


async def create_deal_room(client, initiator_username, counterparty_username, bot_token, counterparty_user_id=None, requested_room_number=None, pool_only=False, pool_room_number=None):
    """Create a deal room - NO MESSAGES SENT, ONLY GROUP CREATION.
    With pool_only the room is fully set up but left empty and stored in the
    premade room pool instead of being tied to a deal."""
    global room_counter
    
    try:
        # Hand out an already premade room when one is available - this is the
        # fast path, since setup is already done.
        if not pool_only:
            pooled = take_pooled_room(requested_room_number)
            if pooled:
                return await assign_pooled_room(
                    client, pooled, initiator_username, counterparty_username, counterparty_user_id
                )

        # Honour a requested room number only when that number is free; otherwise
        # fall back to the normal 1..20 sequence.
        room_number = pool_room_number
        if room_number is None and requested_room_number and ROOM_NUMBER_MIN <= requested_room_number <= ROOM_NUMBER_MAX:
            if database.is_room_number_available(requested_room_number):
                room_number = requested_room_number
                logger.info(f"📌 Using requested room number {room_number}")
            else:
                logger.info(f"⚠️ Requested room number {requested_room_number} is in use - using next in sequence")

        if room_number is None:
            room_number = room_counter
            room_counter = room_counter + 1 if room_counter < ROOM_NUMBER_MAX else ROOM_NUMBER_MIN

        room_name = f"MM ROOM {room_number}"
        
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

        await hide_room_history(client, chat_id, room_name)
        
        # Make userbot anonymous in the group
        try:
            me = await client.get_me()
            anonymous_rights = ChatAdminRights(
                change_info=True,
                post_messages=True,
                edit_messages=True,
                delete_messages=True,
                ban_users=True,
                invite_users=True,
                pin_messages=True,
                add_admins=True,
                anonymous=True,
                manage_call=True
            )
            await client(EditAdminRequest(
                channel=chat_id,
                user_id=me.id,
                admin_rights=anonymous_rights,
                rank=""
            ))
            logger.info(f"✅ UserBot set as anonymous admin in {room_name}")
        except Exception as e:
            logger.warning(f"Could not set userbot as anonymous: {e}")
        
        # Fee tier from participant bios (premade pool rooms have no participants
        # yet - their tier is calculated when the room is assigned to a deal)
        fee_tier = "0.75%"
        if not pool_only:
            fee_tier = await compute_fee_tier(
                client, initiator_username, counterparty_username, counterparty_user_id
            )

            # Store initial deal room info immediately (before bot joins)
            deal_rooms[chat_id] = {
                'room_number': room_number,
                'room_name': room_name,
                'initiator_username': initiator_username,
                'counterparty_username': counterparty_username,
                'counterparty_user_id': counterparty_user_id,
                'invite_link': '',
                'chat_id': chat_id,
                'bot_invite_link': ''
            }
            save_room_info(chat_id, deal_rooms[chat_id])
        
        # Picture, then the bot and the fixed room admins. Each step is
        # independent, so one failing call cannot leave the room without its bot
        # or admins. A real rate limit aborts a premade room (so /startroom can
        # report it) but never a room a trader is waiting for.
        invite_link = None
        bot_invite_link = None
        bot_ready = False
        try:
            await set_room_photo(client, chat_id, room_number, room_name)

            bot_entity = await resolve_bot_entity(client, bot_token)
            if bot_entity:
                bot_invite_link = await export_invite(client, chat_id, room_name, request_needed=False)
                if await invite_user(client, chat_id, bot_entity, 'bot', room_name):
                    bot_ready = await promote_user(client, chat_id, bot_entity.id, 'MM', 'bot', room_name)
            else:
                logger.warning(f"Could not resolve the bot to add it to {room_name}")

            await add_fixed_room_admins(client, chat_id, room_name)

            if bot_ready:
                invite_link = await export_invite(client, chat_id, room_name, request_needed=True)
            else:
                logger.warning(f"Skipping user invite link creation - bot not ready in {room_name}")
        except FloodWaitError as e:
            if pool_only:
                raise
            logger.warning(f"Flood wait {e.seconds}s while setting up {room_name} - continuing")

        # Clear the group's creation/join service messages.
        await asyncio.sleep(2.0)
        await delete_service_messages(client, chat_id, limit=20)

        if pool_only:
            # Premade room: add the fixed admins now (inline, nothing is waiting
            # on this room) and park it in the pool.
            await add_extra_room_members(client, chat_id, room_name, sweep_delays=(1.0,))
            add_room_to_pool({
                'chat_id': chat_id,
                'room_number': room_number,
                'room_name': room_name,
                'invite_link': str(invite_link) if invite_link else '',
                'bot_invite_link': bot_invite_link or ''
            })
            logger.info(f"🏠 {room_name} added to the premade room pool")
            return chat_id, room_name, invite_link

        asyncio.create_task(add_extra_room_members_background(client, chat_id, room_name))

        # Update deal room info with final details
        deal_rooms[chat_id] = {
            'room_number': room_number,
            'room_name': room_name,
            'initiator_username': initiator_username,
            'counterparty_username': counterparty_username,
            'counterparty_user_id': counterparty_user_id,
            'invite_link': str(invite_link),
            'chat_id': chat_id,
            'bot_invite_link': bot_invite_link,
            'fee_tier': fee_tier
        }
        save_room_info(chat_id, deal_rooms[chat_id])
        
        return chat_id, room_name, invite_link
        
    except FloodWaitError:
        raise
    except Exception as e:
        logger.error(f"Error creating deal room: {e}")
        return None, None, None




async def prewarm_room_pool(client, bot_token, request_id=None):
    """Create every missing room of the 1..20 pool, fully set up and empty.
    Room numbers already in the pool or in use by an active deal are skipped, so
    re-running /startroom resumes where a previous run stopped. A rate limit or
    any other failure stops the run and is reported back, as is a cancel from
    the /startroom message."""
    created = []
    error = None
    cancelled = False
    for room_number in range(ROOM_NUMBER_MIN, ROOM_NUMBER_MAX + 1):
        if request_id and is_prewarm_cancelled(request_id):
            cancelled = True
            logger.info("🛑 Room preparation cancelled")
            break

        pooled_numbers = {entry.get('room_number') for entry in read_room_pool()}
        if room_number in pooled_numbers:
            logger.info(f"⏭️ MM ROOM {room_number} already premade - skipping")
            continue
        if not database.is_room_number_available(room_number):
            logger.info(f"⏭️ MM ROOM {room_number} is in an active deal - skipping")
            continue

        try:
            chat_id, _, _ = await create_deal_room(
                client,
                initiator_username='',
                counterparty_username='',
                bot_token=bot_token,
                pool_only=True,
                pool_room_number=room_number
            )
        except FloodWaitError as e:
            error = f"Telegram rate limit on MM ROOM {room_number} - retry in {e.seconds}s"
            logger.warning(f"⏳ {error}")
            break
        except Exception as e:
            error = f"MM ROOM {room_number}: {e}"
            logger.warning(f"❌ {error}")
            break

        if not chat_id:
            error = f"Could not create MM ROOM {room_number}"
            logger.warning(f"❌ {error}")
            break

        created.append(room_number)
        await asyncio.sleep(PREWARM_ROOM_DELAY)

    repaired = []
    if not cancelled and error is None:
        try:
            repaired = await repair_pool_rooms(client, bot_token, request_id)
        except FloodWaitError as e:
            error = f"Telegram rate limit while completing room setup - retry in {e.seconds}s"
            logger.warning(f"⏳ {error}")
        except Exception as e:
            error = f"While completing room setup: {e}"
            logger.warning(f"❌ {error}")

    logger.info(f"🏠 Premade {len(created)} room(s): {created}")
    return {
        'created': created,
        'error': error,
        'cancelled': cancelled,
        'repaired': repaired
    }


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
                    counterparty_user_id = req.get('counterparty_user_id')
                    bot_token = req.get('bot_token', '')
                    requested_room_number = req.get('requested_room_number')
                    
                    chat_id, room_name, invite_link = await create_deal_room(
                        client,
                        initiator_username,
                        counterparty_username,
                        bot_token,
                        counterparty_user_id,
                        requested_room_number
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
            
            # Process /startroom requests - pre-create the room pool
            for req in read_prewarm_requests():
                request_id = req.get('request_id')
                bot_token = req.get('bot_token', '')
                result = await prewarm_room_pool(client, bot_token, request_id)
                result['pool_size'] = len(read_room_pool())
                update_prewarm_request_status(request_id, 'completed', result)

            # Process requests to return a closed room to the pool
            for req in read_release_requests():
                chat_id = req.get('chat_id')
                room_number = req.get('room_number')
                room_name = req.get('room_name') or f"MM ROOM {room_number}"
                logger.info(f"♻️ Processing release request for {room_name} (chat_id: {chat_id})")
                success = await release_room_to_pool(client, chat_id, room_number, room_name)
                update_release_request_status(chat_id, 'completed' if success else 'failed')

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
