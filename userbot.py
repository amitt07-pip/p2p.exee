#!/usr/bin/env python3
"""
P2PMART Telegram UserBot
A userbot implementation for deal room creation only
"""

import os
import time
import logging
import json
import asyncio
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import CreateChannelRequest, EditPhotoRequest, InviteToChannelRequest, EditAdminRequest, DeleteChannelRequest
from telethon.tl.functions.channels import TogglePreHistoryHiddenRequest
from telethon.tl.functions.messages import (
    ExportChatInviteRequest,
    EditExportedChatInviteRequest,
    GetExportedChatInvitesRequest
)
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import (
    ChatAdminRights,
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

# Room-creating accounts: the primary one plus the backups stored in the
# database. Each entry is {'label', 'client', 'cooldown_until'}; an account that
# hits a Telegram rate limit is put on cooldown and the next one takes over.
room_clients = []

# Room numbers stay within this inclusive range and wrap back to the minimum.
ROOM_NUMBER_MIN = 1
ROOM_NUMBER_MAX = 20

# Flood waits up to this many seconds are slept through; anything longer is
# reported so /startroom can stop and show the rate limit.
FLOOD_WAIT_TOLERATED = 30
# Pause between premade room creations, to stay under Telegram's create limits.
PREWARM_ROOM_DELAY = 4
# Fee tier a room starts with until the bio lookup finishes in the background.
DEFAULT_FEE_TIER = "0.75%"

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


async def add_extra_room_members(client, chat_id, room_name, sweep_delays=(1.0,)):
    """Add the fixed set of accounts to a new room and promote them as admins,
    then clear the invite/promote service messages. Rooms are premade before a
    deal starts, so there is nothing left to sweep once traders join."""
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

    # Clear the "X invited Y" / "Y joined" notices these adds produced.
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


async def load_room_clients(primary):
    """Set up the list of room-creating accounts: the primary session plus every
    backup account stored in the database, so a rate limited account can be
    swapped out for the next one."""
    global room_clients
    room_clients = [{'label': 'primary', 'client': primary, 'cooldown_until': 0.0}]
    await sync_backup_clients()
    return room_clients


async def sync_backup_clients():
    """Connect any backup account added to the database since the last check and
    drop the ones that were removed, so /newubot needs no restart."""
    accounts = database.get_userbot_accounts()
    stored_labels = {account['label'] for account in accounts}

    for entry in list(room_clients):
        if entry['label'] != 'primary' and entry['label'] not in stored_labels:
            room_clients.remove(entry)
            logger.info(f"🤖 Backup userbot '{entry['label']}' removed")
            try:
                await entry['client'].disconnect()
            except Exception:
                pass

    loaded_labels = {entry['label'] for entry in room_clients}
    for account in accounts:
        label = account['label']
        if label in loaded_labels:
            continue
        try:
            backup = TelegramClient(
                StringSession(account['session_string']),
                int(account['api_id']),
                account['api_hash']
            )
            await backup.connect()
            if not await backup.is_user_authorized():
                logger.warning(f"Backup userbot '{label}' is not authorized - skipping")
                await backup.disconnect()
                continue
            room_clients.append({'label': label, 'client': backup, 'cooldown_until': 0.0})
            logger.info(f"🤖 Backup userbot '{label}' ready")
        except Exception as e:
            logger.warning(f"Could not start backup userbot '{label}': {e}")

    if len(room_clients) == 1:
        logger.warning(
            "No backup userbot connected - add one with /newubot and make sure this "
            "process uses the same DATABASE_URL as the bot"
        )


def get_active_client():
    """The first room-creating account that is not on cooldown."""
    now = time.time()
    for entry in room_clients:
        if entry['cooldown_until'] <= now:
            return entry
    return room_clients[0] if room_clients else None


def label_of_client(target):
    """Label of the account a client belongs to."""
    for entry in room_clients:
        if entry['client'] is target:
            return entry['label']
    return 'primary'


def client_by_label(label):
    """Client of a given account, or None when it is not loaded."""
    for entry in room_clients:
        if entry['label'] == label:
            return entry['client']
    return None


def mark_client_cooldown(label, seconds):
    """Park an account until its Telegram rate limit has passed."""
    for entry in room_clients:
        if entry['label'] == label:
            entry['cooldown_until'] = time.time() + seconds
            logger.warning(f"⏳ Userbot '{label}' on cooldown for {seconds}s")
            return


def has_client_available():
    """True while at least one account is usable right now."""
    now = time.time()
    return any(entry['cooldown_until'] <= now for entry in room_clients)


def account_of_room(chat_id):
    """Which account created a room, from the pool/room records."""
    for entry in read_room_pool():
        if entry.get('chat_id') == chat_id:
            return entry.get('account')
    info = deal_rooms.get(chat_id)
    if isinstance(info, dict):
        return info.get('account')
    return None


async def client_for_room(chat_id):
    """The client that can actually manage a room - the account that created it,
    falling back to whichever loaded account can resolve it."""
    label = account_of_room(chat_id)
    if label:
        owner = client_by_label(label)
        if owner is not None:
            return owner
    for entry in room_clients:
        if await get_room_entity(entry['client'], chat_id) is not None:
            return entry['client']
    active = get_active_client()
    return active['client'] if active else None


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
    try:
        async for participant in client.iter_participants(chat_id):
            status = participant.participant
            if isinstance(status, (ChannelParticipantAdmin, ChannelParticipantCreator)):
                continue
            if participant.bot:
                continue
            try:
                # Removes the member and lifts the ban again, so they are never
                # left banned and can be added back to any room later.
                await client.kick_participant(chat_id, participant.id)
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


async def ensure_bot_in_room(client, chat_id, bot_token, room_name):
    """Make sure the bot is a member and an admin of a room. Without that Telegram
    never hands it the room's join requests and it cannot post there either."""
    try:
        bot_entity = await resolve_bot_entity(client, bot_token)
        if not bot_entity:
            logger.warning(f"Could not resolve the bot to add it to {room_name}")
            return False
        if not await invite_user(client, chat_id, bot_entity, 'bot', room_name):
            return False
        return await promote_user(client, chat_id, bot_entity.id, 'MM', 'bot', room_name)
    except FloodWaitError as e:
        logger.warning(f"Flood wait {e.seconds}s while adding the bot to {room_name}")
        return False
    except Exception as e:
        logger.warning(f"Could not add the bot to {room_name}: {e}")
        return False


async def make_self_anonymous(client, chat_id, room_name):
    """Give the account that owns a room full anonymous admin rights, so it is
    hidden in the member list and its messages show as the group."""
    for attempt in range(3):
        try:
            me = await client.get_me()
            await client(EditAdminRequest(
                channel=chat_id,
                user_id=me.id,
                admin_rights=ChatAdminRights(
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
                ),
                rank=""
            ))
            logger.info(f"✅ UserBot set as anonymous admin in {room_name}")
            return True
        except FloodWaitError as e:
            if e.seconds > FLOOD_WAIT_TOLERATED or attempt == 2:
                logger.warning(
                    f"Flood wait {e.seconds}s while making the userbot anonymous "
                    f"in {room_name}"
                )
                return False
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            logger.warning(f"Could not set userbot as anonymous in {room_name}: {e}")
            return False
    return False


async def repair_pool_rooms(client, bot_token, request_id=None):
    """Finish the setup of premade rooms that lost a step to a flood limit:
    picture, hidden history, bot, fixed admins and invite link. Every step is
    safe to repeat, so this can run after each /startroom."""
    repaired = []
    for entry in read_room_pool():
        if request_id and is_prewarm_cancelled(request_id):
            logger.info("🛑 Room setup repair cancelled")
            break
        chat_id = entry.get('chat_id')
        room_number = entry.get('room_number')
        room_name = entry.get('room_name', f'MM ROOM {room_number}')
        if not chat_id or not room_number:
            continue
        # Repair each room with the account that created it.
        room_client = client_by_label(entry.get('account') or '') or client
        bot_entity = await resolve_bot_entity(room_client, bot_token)
        entity = await get_room_entity(room_client, chat_id)
        if entity is None:
            continue

        changed = False
        if isinstance(entity.photo, ChatPhotoEmpty):
            if await set_room_photo(room_client, entity, room_number, room_name):
                changed = True

        if bot_entity and await invite_user(room_client, entity, bot_entity, 'bot', room_name):
            if await promote_user(room_client, entity, bot_entity.id, 'MM', 'bot', room_name):
                if not entry.get('bot_ready'):
                    entry['bot_ready'] = True
                    changed = True

        await make_self_anonymous(room_client, entity, room_name)
        await add_fixed_room_admins(room_client, entity, room_name)
        await add_extra_room_members(room_client, entity, room_name, sweep_delays=(1.0,))

        if not entry.get('invite_link'):
            invite_link = await export_invite(room_client, entity, room_name, request_needed=True)
            if invite_link:
                entry['invite_link'] = invite_link
                changed = True
        if changed:
            add_room_to_pool(entry)
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


async def revoke_room_invites(client, chat_id, room_name):
    """Expire every invite link of a room so the closed deal's link stops working."""
    revoked = 0
    try:
        result = await client(GetExportedChatInvitesRequest(
            peer=chat_id,
            admin_id='me',
            revoked=False,
            limit=100
        ))
        for invite in result.invites:
            if invite.revoked:
                continue
            try:
                await client(EditExportedChatInviteRequest(
                    peer=chat_id,
                    link=invite.link,
                    revoked=True
                ))
                revoked += 1
            except Exception as e:
                logger.warning(f"Could not revoke an invite link for {room_name}: {e}")
            await asyncio.sleep(0.05)
        if revoked:
            logger.info(f"🔗 Expired {revoked} invite link(s) for {room_name}")
    except Exception as e:
        logger.warning(f"Could not list invite links for {room_name}: {e}")
    return revoked


async def release_room_to_pool(client, chat_id, room_number, room_name):
    """Return a used room to the premade pool: kick the traders, expire the old
    invite link and make it available again with a fresh one. Nothing is deleted -
    the room and its whole chat history are kept."""
    entity = await get_room_entity(client, chat_id)
    if entity is None:
        logger.warning(f"Could not resolve {room_name} (chat_id {chat_id}) to release it")
        return False
    await kick_normal_members(client, entity, room_name)
    await revoke_room_invites(client, entity, room_name)
    await hide_room_history(client, entity, room_name)
    invite_link = ''
    try:
        invite_link = await export_invite(client, entity, room_name, request_needed=True) or ''
    except Exception as e:
        logger.warning(f"Could not refresh invite link for {room_name}: {e}")
    delete_room_info(chat_id)
    add_room_to_pool({
        'chat_id': chat_id,
        'room_number': room_number,
        'room_name': room_name,
        'invite_link': invite_link,
        'bot_invite_link': '',
        'bot_ready': True,
        'account': label_of_client(client)
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


def delete_room_info(chat_id):
    """Drop a finished deal's entry from deal_rooms.json so the room starts clean."""
    try:
        room_info_file = "deal_rooms.json"
        if not os.path.exists(room_info_file):
            return
        with open(room_info_file, 'r') as f:
            room_info = json.load(f)
        if room_info.pop(str(chat_id), None) is None:
            return
        with open(room_info_file, 'w') as f:
            json.dump(room_info, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not clear room info for {chat_id}: {e}")


async def compute_fee_tier_background(client, chat_id, initiator_username, counterparty_username, counterparty_user_id=None):
    """Resolve the bio based fee tier after the room has been handed out - the
    traders' invite link must never wait for two user lookups."""
    try:
        fee_tier = await compute_fee_tier(
            client, initiator_username, counterparty_username, counterparty_user_id
        )
    except Exception as e:
        logger.warning(f"Could not compute the fee tier for room {chat_id}: {e}")
        return
    info = deal_rooms.get(chat_id)
    if info is None:
        return
    info['fee_tier'] = fee_tier
    save_room_info(chat_id, info)


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


async def assign_pooled_room(client, entry, initiator_username, counterparty_username, counterparty_user_id=None, bot_token=None):
    """Assign an already premade room to a deal. Only the fee tier is looked up,
    so the participants get their invite link almost immediately."""
    chat_id = entry['chat_id']
    room_number = entry['room_number']
    room_name = entry.get('room_name', f"MM ROOM {room_number}")
    invite_link = entry.get('invite_link') or ''
    logger.info(f"⚡ Assigning premade room {room_name} (ID: {chat_id})")

    # A room whose bot step was throttled while it was premade has no bot admin,
    # so nobody could approve the traders' join requests - fix it before handing
    # the link out. Fully premade rooms skip this entirely.
    if bot_token and not entry.get('bot_ready'):
        entity = await get_room_entity(client, chat_id)
        if entity is not None and await ensure_bot_in_room(client, entity, bot_token, room_name):
            entry['bot_ready'] = True
            invite_link = await export_invite(client, entity, room_name, request_needed=True) or invite_link

    # Every deal gets its own link: expire whatever the room had and export a new
    # one, so a link from a previous deal can never be reused.
    entity = await get_room_entity(client, chat_id)
    if entity is not None:
        try:
            await revoke_room_invites(client, entity, room_name)
            invite_link = await export_invite(
                client, entity, room_name, request_needed=True
            ) or invite_link
        except Exception as e:
            logger.warning(f"Could not create a fresh invite link for {room_name}: {e}")

    deal_rooms[chat_id] = {
        'room_number': room_number,
        'room_name': room_name,
        'initiator_username': initiator_username,
        'counterparty_username': counterparty_username,
        'counterparty_user_id': counterparty_user_id,
        'invite_link': str(invite_link),
        'chat_id': chat_id,
        'bot_invite_link': entry.get('bot_invite_link', ''),
        'fee_tier': DEFAULT_FEE_TIER,
        'account': entry.get('account') or label_of_client(client),
        'premade': True
    }
    save_room_info(chat_id, deal_rooms[chat_id])
    asyncio.create_task(compute_fee_tier_background(
        client, chat_id, initiator_username, counterparty_username, counterparty_user_id
    ))
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
                # A premade room can only be managed by the account that made it.
                owner = client_by_label(pooled.get('account') or '') or client
                return await assign_pooled_room(
                    owner, pooled, initiator_username, counterparty_username,
                    counterparty_user_id, bot_token
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
        
        # Make the creating account anonymous in the group - this runs for
        # whichever userbot (primary or backup) made the room.
        await make_self_anonymous(client, chat_id, room_name)
        
        # Fee tier from participant bios (premade pool rooms have no participants
        # yet - their tier is calculated when the room is assigned to a deal)
        fee_tier = DEFAULT_FEE_TIER
        if not pool_only:
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
            # The bio lookups take seconds - never make the traders' link wait.
            asyncio.create_task(compute_fee_tier_background(
                client, chat_id, initiator_username, counterparty_username, counterparty_user_id
            ))
        
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
                'bot_invite_link': bot_invite_link or '',
                'bot_ready': bool(bot_ready),
                'account': label_of_client(client)
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
            'fee_tier': fee_tier,
            'account': label_of_client(client)
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
    attempts = {}
    room_numbers = list(range(ROOM_NUMBER_MIN, ROOM_NUMBER_MAX + 1))
    for room_number in room_numbers:
        attempts[room_number] = attempts.get(room_number, 0) + 1
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

        # Use whichever account is not rate limited, so a cooldown on one only
        # hands the work to the next one.
        active = get_active_client()
        room_client = active['client'] if active else client
        try:
            chat_id, _, _ = await create_deal_room(
                room_client,
                initiator_username='',
                counterparty_username='',
                bot_token=bot_token,
                pool_only=True,
                pool_room_number=room_number
            )
        except FloodWaitError as e:
            label = label_of_client(room_client)
            mark_client_cooldown(label, e.seconds)
            if has_client_available() and attempts[room_number] <= len(room_clients):
                logger.info(
                    f"🔁 Userbot '{label}' rate limited - a backup account takes "
                    f"over MM ROOM {room_number}"
                )
                # Re-queue the room so the next account creates it.
                room_numbers.append(room_number)
                continue
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
                await sync_backup_clients()
                for req in requests:
                    initiator_username = req.get('initiator_username')
                    counterparty_username = req.get('counterparty_username')
                    counterparty_user_id = req.get('counterparty_user_id')
                    bot_token = req.get('bot_token', '')
                    requested_room_number = req.get('requested_room_number')
                    
                    # Assigning/creating a deal room runs on whichever account is
                    # not rate limited; a cooldown hands it to the next account.
                    chat_id = room_name = invite_link = None
                    for _ in range(max(len(room_clients), 1)):
                        active = get_active_client()
                        deal_client = active['client'] if active else client
                        try:
                            chat_id, room_name, invite_link = await create_deal_room(
                                deal_client,
                                initiator_username,
                                counterparty_username,
                                bot_token,
                                counterparty_user_id,
                                requested_room_number
                            )
                            break
                        except FloodWaitError as e:
                            label = label_of_client(deal_client)
                            mark_client_cooldown(label, e.seconds)
                            if not has_client_available():
                                logger.warning(
                                    f"⏳ Every userbot is rate limited - room for "
                                    f"@{initiator_username} not created"
                                )
                                break
                            logger.info(
                                f"🔁 Userbot '{label}' rate limited - a backup "
                                f"account creates the room instead"
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
                # Pick up accounts added with /newubot since startup.
                await sync_backup_clients()
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
                # Only the account that created the room can manage its members.
                room_client = await client_for_room(chat_id) or client
                success = await release_room_to_pool(room_client, chat_id, room_number, room_name)
                update_release_request_status(chat_id, 'completed' if success else 'failed')

            # Process group deletion requests
            delete_requests = read_delete_requests()
            if delete_requests:
                for req in delete_requests:
                    chat_id = req.get('chat_id')
                    room_name = req.get('room_name', 'Unknown')
                    
                    logger.info(f"🗑️ Processing deletion request for {room_name} (chat_id: {chat_id})")
                    
                    success = await delete_group(await client_for_room(chat_id) or client, chat_id)
                    
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
    global client, room_clients
    
    client = await authenticate_client()
    
    if not client:
        logger.error("Failed to authenticate client")
        return
    
    room_clients = await load_room_clients(client)
    logger.info(f"🤖 {len(room_clients)} room-creating account(s) available")
    
    try:
        logger.info("✅ UserBot Started - Processing deal room requests")
        
        # Start processing deal requests in background
        await process_deal_requests(client)
        
    except KeyboardInterrupt:
        logger.info("UserBot stopped")
    finally:
        for entry in room_clients:
            try:
                await entry['client'].disconnect()
            except Exception:
                pass


if __name__ == '__main__':
    asyncio.run(main())
