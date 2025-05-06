from utils.logger_settings import get_logger
from utils.rpc import get_block

logger = get_logger("blocks")


async def process_block(block_num, url, semaphore):
    block_hex = hex(block_num)
    async with semaphore:
        try:
            block_data = await get_block(block_hex, url)
            return block_data
        except Exception as e:
            logger.error(f"Error fetching block {block_num}: {str(e)}")
            return None
