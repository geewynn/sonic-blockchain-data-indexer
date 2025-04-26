from utils.rpc import get_block_receipts


async def process_receipt(block_num, url, semaphore):
    block_hex = hex(block_num)
    async with semaphore:
        try:
            receipt_data = get_block_receipts(block_hex, url)
            return receipt_data
        except Exception as e:
            print(f"Error fetching receipts {block_num}: {str(e)}")
            return None
