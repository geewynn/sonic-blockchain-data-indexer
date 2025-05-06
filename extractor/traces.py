import asyncio
import logging

from utils.rpc import safe_get_trace_filter

logger = logging.getLogger("logs")
logger.setLevel(logging.INFO)


async def process_traces(start_block, end_block, url, semaphore, micro_batch_size=100):
    traces = []
    for batch_start in range(start_block, end_block + 1, micro_batch_size):
        batch_end = min(batch_start + micro_batch_size - 1, end_block)
        async with semaphore:
            for attempt in range(1):
                try:
                    batch_traces = await safe_get_trace_filter(
                        batch_start, batch_end, url
                    )
                    if batch_traces:
                        traces.extend(batch_traces)
                    break
                except Exception as e:
                    logger.error(
                        f"Error fetching traces {batch_start}-{batch_end}, attempt {attempt+1}: {str(e)}"
                    )
                    await asyncio.sleep(2**attempt)
    return traces
