import asyncio
from utils.rpc import safe_get_logs

async def process_logs(start_block, end_block, url, semaphore, micro_batch_size=500):
    logs = []
    for batch_start in range(start_block, end_block + 1, micro_batch_size):
        batch_end = min(batch_start + micro_batch_size - 1, end_block)
        async with semaphore:
            for attempt in range(3):  # Retry 3 times
                try:
                    batch_logs = safe_get_logs(batch_start, batch_end, url)
                    if batch_logs:
                        logs.extend(batch_logs)
                    break
                except Exception as e:
                    print(f"Error fetching logs {batch_start}-{batch_end}, attempt {attempt+1}: {str(e)}")
                    await asyncio.sleep(2 ** attempt)
    return logs