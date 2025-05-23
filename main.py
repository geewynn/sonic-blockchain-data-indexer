import asyncio
import logging
import os

from extractor.blocks import process_block
from extractor.logs import process_logs
from extractor.receipts import process_receipt
from extractor.traces import process_traces
from store.parquet_writer import write_batch_to_parquet
from store.storage import upload_to_storage
from utils.config import *
from utils.helpers import get_partition_path, load_checkpoint, save_checkpoint
from utils.logger_settings import get_logger

logger = get_logger("main")
logger.setLevel(logging.INFO)


async def run_blocks(start, end, url, sem):
    cur, batch, first_blk = start, [], start
    while cur <= end:
        async with sem:
            block = await process_block(cur, url, sem)
        if block:
            batch.append(block)

        span = cur - first_blk + 1
        need_flush = span >= BATCH_BLOCKS or cur == end
        if need_flush and batch:
            dir_ = get_partition_path(OUTPUT_DIR, BLOCKS_FOLDER, first_blk, cur)
            path = write_batch_to_parquet(batch, BLOCKS_FOLDER, 0, dir_)
            if path:
                upload_to_storage(path, os.path.relpath(path, OUTPUT_DIR))
                save_checkpoint("blocks", cur)
            batch.clear()
            first_blk = cur + 1
        cur += 1
        logger.info("[blocks] up to %s | span=%s", cur - 1, span)


async def run_receipts(start, end, url, sem):
    cur, batch, first_blk = start, [], start
    while cur <= end:
        async with sem:
            receipts = await process_receipt(cur, url, sem)
        if receipts:
            batch.extend(receipts)

        span = cur - first_blk + 1
        need_flush = span >= BATCH_BLOCKS or cur == end
        if need_flush and batch:
            dir_ = get_partition_path(OUTPUT_DIR, RECEIPT_FOLDER, first_blk, cur)
            path = write_batch_to_parquet(batch, RECEIPT_FOLDER, 0, dir_)
            if path:
                upload_to_storage(path, os.path.relpath(path, OUTPUT_DIR))
                save_checkpoint("receipts", cur)
            batch.clear()
            first_blk = cur + 1
        cur += 1
        logger.info("[receipts] up to %s | span=%s", cur - 1, span)


async def run_logs(start, end, url, sem):
    cur, buffer, first_blk = start, [], start
    window = MICRO_BATCH
    while cur <= end:
        rng_end = min(cur + window - 1, end)
        async with sem:
            try:
                logs = await process_logs(
                    cur, rng_end, url, sem, micro_batch_size=window
                )
            except Exception as e:
                logger.error("[logs] %s-%s failed: %s", cur, rng_end, e)
                window = max(window // 2, 10)
                await asyncio.sleep(2)
                continue

        if logs:
            buffer.extend(logs)

        span = rng_end - first_blk + 1
        need_flush = span >= BATCH_BLOCKS or rng_end == end
        if buffer and need_flush:
            dir_ = get_partition_path(OUTPUT_DIR, LOGS_FOLDER, first_blk, rng_end)
            path = write_batch_to_parquet(buffer, LOGS_FOLDER, 0, dir_)
            if path:
                upload_to_storage(path, os.path.relpath(path, OUTPUT_DIR))
                save_checkpoint("logs", rng_end)
                buffer.clear()
                first_blk = rng_end + 1

        cur = rng_end + 1
        logger.info("[logs] up to %s | span=%s", rng_end, span)


async def run_traces(start, end, url, sem):
    cur, buffer, first_blk = start, [], start
    window = MICRO_BATCH
    while cur <= end:
        rng_end = min(cur + window - 1, end)
        async with sem:
            try:
                traces = await process_traces(
                    cur, rng_end, url, sem, micro_batch_size=window
                )
            except Exception as e:
                logger.error("[traces] %s-%s failed: %s", cur, rng_end, e)
                window = max(window // 2, 10)
                await asyncio.sleep(2)
                continue

        if traces:
            buffer.extend(traces)

        span = rng_end - first_blk + 1
        need_flush = span >= BATCH_BLOCKS or rng_end == end
        if buffer and need_flush:
            dir_ = get_partition_path(OUTPUT_DIR, TRACES_FOLDER, first_blk, rng_end)
            path = write_batch_to_parquet(buffer, TRACES_FOLDER, 0, dir_)
            if path:
                upload_to_storage(path, os.path.relpath(path, OUTPUT_DIR))
                save_checkpoint("traces", rng_end)
                buffer.clear()
                first_blk = rng_end + 1

        cur = rng_end + 1
        logger.info("[traces] up to %s | span=%s", rng_end, span)


async def main():

    end_block = END_BLOCK
    sem_blocks = asyncio.Semaphore(5)
    sem_receipts = asyncio.Semaphore(5)
    sem_logs = asyncio.Semaphore(5)
    sem_traces = asyncio.Semaphore(5)

    checkpoints = {
        "blocks": load_checkpoint("blocks"),
        "receipts": load_checkpoint("receipts"),
        "logs": load_checkpoint("logs"),
        "traces": load_checkpoint("traces"),
    }
    logger.info("Start checkpoints: %s | Target block: %s", checkpoints, end_block)

    await asyncio.gather(
        run_blocks(checkpoints["blocks"] + 1, end_block, RPC_URL, sem_blocks),
        run_receipts(checkpoints["receipts"] + 1, end_block, RPC_URL, sem_receipts),
        run_logs(checkpoints["logs"] + 1, end_block, RPC_URL, sem_logs),
        run_traces(checkpoints["traces"] + 1, end_block, RPC_URL, sem_traces),
    )
    logger.info("Processing completed successfully")


if __name__ == "__main__":
    asyncio.run(main())
