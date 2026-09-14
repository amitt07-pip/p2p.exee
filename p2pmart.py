#!/usr/bin/env python3
"""
P2PMART - UserBot Entry Point
Runs the Telegram UserBot (bot.py runs as a separate service)
"""

import asyncio
import logging
import sys

# Configure logging
logging.basicConfig(
    format='%(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Suppress verbose library logging
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)
logging.getLogger('telegram.ext').setLevel(logging.WARNING)
logging.getLogger('telethon').setLevel(logging.WARNING)


def main():
    """Start the userbot only (bot.py runs as a separate service)"""
    logger.info("🚀 P2PMART UserBot Starting...")
    
    # Run userbot directly in the main thread
    try:
        from userbot import main as userbot_main
        asyncio.run(userbot_main())
    except KeyboardInterrupt:
        logger.info("🛑 P2PMART UserBot stopped")
        sys.exit(0)


if __name__ == '__main__':
    main()
