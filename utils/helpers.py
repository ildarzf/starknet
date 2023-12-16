from loguru import logger

from settings import RETRY_COUNT
from utils.sleeping import sleep


def retry(func):
    async def wrapper(*args, **kwargs):
        retries = 0
        while retries < RETRY_COUNT:
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception as e:
                if 'Client failed with code -32603' in str(e):
                    logger.error(f"Error short info | Client failed with code -32603")
                    sleep(1, 2)
                    retries += 1
                else:
                    logger.error(f"Error | {e}")
                    sleep(1, 2)
                    retries += 1

    return wrapper
