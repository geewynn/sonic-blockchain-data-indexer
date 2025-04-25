import asyncio
import os
import tqdm
from dotenv import load_dotenv
from datetime import datetime, timezone
from extractor.traces import process_traces
from utils.fs import setup_directory
from store.parquet_writer import write_batch_to_parquet
from store.storage import upload_to_storage
from extractor.blocks import process_block
from extractor.receipts import process_receipt
from extractor.logs import process_logs
import json

load_dotenv()

RPC_URL = os.getenv("RPC_URL")
OUTPUT_DIR = "sonic_data"
BATCH_SIZE = os.getenv("BATCH_SIZE")
END_BLOCK = os.getenv("END_BLOCK")


CHECKPOINT_FILE = "checkpoint.json"

def load_checkpoint(data_type):
    if not os.path.exists(CHECKPOINT_FILE):
        return 0
    with open(CHECKPOINT_FILE, 'r') as f:
        data = json.load(f)
        return data.get(data_type, 0)

def save_checkpoint(data_type, block_num):
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            data = json.load(f)
    else:
        data = {}

    data[data_type] = block_num

    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(data, f)


def get_partition_path(base_dir, data_type, first_block, last_block):
    timestamp_hex = first_block.get("timestamp")
    timestamp = int(timestamp_hex, 16)
    date_str = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")
    block_start_hex = first_block.get("number")
    block_end_hex = last_block.get("number")
    block_start = int(block_start_hex, 16)
    block_end = int(block_end_hex, 16)
    return os.path.join(base_dir, data_type, f"date={date_str}", f"block_range={block_start}_{block_end}")


async def process_blocks_range(start_block, end_block, batch_size, output_dir, url):
    setup_directory(output_dir)
    batch_counter = 0
    total_blocks = end_block - start_block + 1
    total_batches = (total_blocks + batch_size - 1) // batch_size
    semaphore = asyncio.Semaphore(10)

    for batch_idx in tqdm.tqdm(range(total_batches), desc="Processing block batches"):
        batch_start = start_block + batch_idx * batch_size
        batch_end = min(batch_start + batch_size - 1, end_block)

        block_tasks = [process_block(block_num, url, semaphore) for block_num in range(batch_start, batch_end + 1)]
        receipt_tasks = [process_receipt(block_num, url, semaphore) for block_num in range(batch_start, batch_end + 1)]
        log_task = process_logs(batch_start, batch_end, url, semaphore)
        trace_task = process_traces(batch_start, batch_end, url, semaphore)

        batch_blocks = await asyncio.gather(*block_tasks)
        batch_receipts = await asyncio.gather(*receipt_tasks)
        batch_logs = await log_task
        print(f"Fetched {len(batch_logs) if batch_logs else 0} logs from blocks {batch_start} to {batch_end}")
        batch_traces = await trace_task

        flat_receipts = [receipt for sublist in batch_receipts if sublist for receipt in sublist]
        batch_blocks = [block for block in batch_blocks if block]

        if batch_blocks:
            block_dir = get_partition_path(output_dir, "blocks", batch_blocks[0], batch_blocks[-1])
            blocks_path = write_batch_to_parquet(batch_blocks, "blocks", batch_counter, block_dir)
            if blocks_path:
                upload_to_storage(blocks_path, os.path.relpath(blocks_path, output_dir))

        if flat_receipts and batch_blocks:
            receipt_dir = get_partition_path(output_dir, "receipts", batch_blocks[0], batch_blocks[-1])
            receipts_path = write_batch_to_parquet(flat_receipts, "receipts", batch_counter, receipt_dir)
            if receipts_path:
                upload_to_storage(receipts_path, os.path.relpath(receipts_path, output_dir))

        if batch_logs and batch_blocks:
            logs_dir = get_partition_path(output_dir, "logs", batch_blocks[0], batch_blocks[-1])
            logs_path = write_batch_to_parquet(batch_logs, "logs", batch_counter, logs_dir)
            if logs_path:
                upload_to_storage(logs_path, os.path.relpath(logs_path, output_dir))

        if batch_traces and batch_blocks:
            trace_dir = get_partition_path(output_dir, "traces", batch_blocks[0], batch_blocks[-1])
            traces_path = write_batch_to_parquet(batch_traces, "traces", batch_counter, trace_dir)
            if traces_path:
                upload_to_storage(traces_path, os.path.relpath(traces_path, output_dir))

        save_checkpoint("blocks", batch_end)
        save_checkpoint("receipts", batch_end)
        save_checkpoint("logs", batch_end)
        save_checkpoint("traces", batch_end)
        batch_counter += 1


async def main():
    start_block = min(
        load_checkpoint("blocks"),
        load_checkpoint("receipts"),
        load_checkpoint("logs"),
        load_checkpoint("traces")
    )
    end_block = start_block + END_BLOCK
    print(f"Processing blocks from {start_block} to {end_block}")
    await process_blocks_range(start_block, end_block, BATCH_SIZE, OUTPUT_DIR, RPC_URL)
    print("Processing completed successfully")


if __name__ == '__main__':
    asyncio.run(main())
