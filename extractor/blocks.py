import asyncio
from utils.rpc import get_block


async def process_block(block_num, url, semaphore):
    block_hex = hex(block_num)
    async with semaphore:
        try:
            block_data = get_block(block_hex, url)
            return block_data
        except Exception as e:
            print(f"Error fetching block {block_num}: {str(e)}")
            return None
