### main.py

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
OUTPUT_DIR = os.getenv("OUTPUT_DIR")
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
END_BLOCK = int(os.getenv("END_BLOCK"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE") 
LOGS_FOLDER = os.getenv("LOGS_FOLDER")
TRACES_FOLDER = os.getenv("TRACES_FOLDER")
BLOCKS_FOLDER = os.getenv("BLOCKS_FOLDER")
RECEIPT_FOLDER = os.getenv("RECEIPT_FOLDER")


MICRO_BATCH_SIZE = int(os.getenv("MICRO_BATCH_SIZE", 100))

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

def safe_block_number(val):
    if val is None:
        return 0
    if isinstance(val, int):
        return val
    if isinstance(val, str):
        if val.startswith("0x"):
            return int(val, 16)
        else:
            return int(val)
    raise ValueError(f"Unexpected block number format: {val}")

def get_partition_path(base_dir, data_type, first_block_num, last_block_num):
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return os.path.join(base_dir, data_type, f"date={date_str}", f"block_range={first_block_num}_{last_block_num}")

async def process_blocks_range(start_block_blocks, start_block_receipts, start_block_logs, start_block_traces, end_block, batch_size, output_dir, url):
    setup_directory(output_dir)
    batch_counter = 0
    total_blocks = end_block - min(start_block_blocks, start_block_receipts, start_block_logs, start_block_traces) + 1
    total_batches = (total_blocks + batch_size - 1) // batch_size
    semaphore = asyncio.Semaphore(10)

    for batch_idx in tqdm.tqdm(range(total_batches), desc="Processing block batches"):
        batch_start = min(start_block_blocks, start_block_receipts, start_block_logs, start_block_traces) + batch_idx * batch_size
        batch_end = min(batch_start + batch_size - 1, end_block)

        block_tasks = [process_block(block_num, url, semaphore) for block_num in range(batch_start, batch_end + 1) if block_num >= start_block_blocks]
        receipt_tasks = [process_receipt(block_num, url, semaphore) for block_num in range(batch_start, batch_end + 1) if block_num >= start_block_receipts]
        log_task = process_logs(batch_start if batch_start >= start_block_logs else start_block_logs, batch_end, url, semaphore, micro_batch_size=MICRO_BATCH_SIZE)
        trace_task = process_traces(batch_start if batch_start >= start_block_traces else start_block_traces, batch_end, url, semaphore, micro_batch_size=MICRO_BATCH_SIZE)

        batch_blocks = await asyncio.gather(*block_tasks)
        batch_receipts = await asyncio.gather(*receipt_tasks)
        batch_logs = await log_task
        batch_traces = await trace_task

        flat_receipts = [receipt for sublist in batch_receipts if sublist for receipt in sublist]
        batch_blocks = [block for block in batch_blocks if block]

        if batch_blocks:
            first_block = batch_blocks[0]
            last_block = batch_blocks[-1]
            block_dir = get_partition_path(output_dir,BLOCKS_FOLDER, safe_block_number(first_block.get("number")), safe_block_number(last_block.get("number")))
            blocks_path = write_batch_to_parquet(batch_blocks, BLOCKS_FOLDER, batch_counter, block_dir)
            if blocks_path:
                upload_to_storage(blocks_path, os.path.relpath(blocks_path, output_dir))
            save_checkpoint("blocks", safe_block_number(last_block.get("number")))

        if flat_receipts:
            first_receipt = flat_receipts[0]
            last_receipt = flat_receipts[-1]
            receipt_dir = get_partition_path(output_dir, RECEIPT_FOLDER, safe_block_number(first_receipt.get("blockNumber")), safe_block_number(last_receipt.get("blockNumber")))
            receipts_path = write_batch_to_parquet(flat_receipts, RECEIPT_FOLDER, batch_counter, receipt_dir)
            if receipts_path:
                upload_to_storage(receipts_path, os.path.relpath(receipts_path, output_dir))
            save_checkpoint("receipts", safe_block_number(last_receipt.get("blockNumber")))

        if batch_logs:
            first_log = batch_logs[0]
            last_log = batch_logs[-1]
            logs_dir = get_partition_path(output_dir, LOGS_FOLDER, safe_block_number(first_log.get("blockNumber")), safe_block_number(last_log.get("blockNumber")))
            logs_path = write_batch_to_parquet(batch_logs, LOGS_FOLDER, batch_counter, logs_dir)
            if logs_path:
                upload_to_storage(logs_path, os.path.relpath(logs_path, output_dir))
            save_checkpoint("logs", safe_block_number(last_log.get("blockNumber")))

        if batch_traces:
            first_trace = batch_traces[0]
            last_trace = batch_traces[-1]
            traces_dir = get_partition_path(output_dir, TRACES_FOLDER, safe_block_number(first_trace.get("blockNumber")), safe_block_number(last_trace.get("blockNumber")))
            traces_path = write_batch_to_parquet(batch_traces, TRACES_FOLDER, batch_counter, traces_dir)
            if traces_path:
                upload_to_storage(traces_path, os.path.relpath(traces_path, output_dir))
            save_checkpoint("traces", safe_block_number(last_trace.get("blockNumber")))

        batch_counter += 1

async def main():
    start_block_blocks = load_checkpoint("blocks")
    start_block_receipts = load_checkpoint("receipts")
    start_block_logs = load_checkpoint("logs")
    start_block_traces = load_checkpoint("traces")

    overall_start = min(start_block_blocks, start_block_receipts, start_block_logs, start_block_traces)
    end_block = overall_start + END_BLOCK
    print(f"Processing blocks from {overall_start} to {end_block}")

    await process_blocks_range(start_block_blocks, start_block_receipts, start_block_logs, start_block_traces, end_block, BATCH_SIZE, OUTPUT_DIR, RPC_URL)
    print("Processing completed successfully")

if __name__ == '__main__':
    asyncio.run(main())
