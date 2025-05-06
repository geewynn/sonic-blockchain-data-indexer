import asyncio

from utils.logger_settings import get_logger
from utils.rpc import safe_get_logs

logger = get_logger("logs")


async def process_logs(start_block, end_block, url, semaphore, micro_batch_size=100):
    logs = []
    for batch_start in range(start_block, end_block + 1, micro_batch_size):
        batch_end = min(batch_start + micro_batch_size - 1, end_block)
        async with semaphore:
            for attempt in range(1):  # Retry 3 times
                try:
                    batch_logs = await safe_get_logs(batch_start, batch_end, url)
                    if batch_logs:
                        logs.extend(batch_logs)
                    break
                except Exception as e:
                    logger.error(
                        f"Error fetching logs {batch_start}-{batch_end}, attempt {attempt+1}: {str(e)}"
                    )
                    await asyncio.sleep(2**attempt)
    return logs
