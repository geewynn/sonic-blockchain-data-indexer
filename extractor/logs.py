import asyncio
from utils.rpc import get_logs

async def process_logs(start_block, end_block, url, semaphore):
    from_block_hex = hex(start_block)
    to_block_hex = hex(end_block)
    async with semaphore:
        try:
            return get_logs(from_block_hex, to_block_hex, url)
        except Exception as e:
            print(f"Error fetching logs {start_block}-{end_block}: {str(e)}")
            return None

