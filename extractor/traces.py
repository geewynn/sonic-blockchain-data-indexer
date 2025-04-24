import asyncio
from utils.rpc import get_trace_filter

async def process_traces(start_block, end_block, url, semaphore):
    from_block_hex = hex(start_block)
    to_block_hex = hex(end_block)
    async with semaphore:
        try:
            return get_trace_filter(from_block_hex, to_block_hex, url)
        except Exception as e:
            print(f"Error fetching traces {start_block}-{end_block}: {str(e)}")
            return None
