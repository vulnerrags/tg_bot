from typing import List
from libraries import *

import aiohttp
API_TOKEN = 'some token here'

ALLOWED_GROUP_ID = '-group id here'
ALLOWED_USERS_ID: List[str] = ['user id here'] # Smirnov Maxim aka @vulnerrags

bot = Bot(token=API_TOKEN)

# policy = asyncio.WindowsSelectorEventLoopPolicy()
# asyncio.set_event_loop_policy(policy)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
