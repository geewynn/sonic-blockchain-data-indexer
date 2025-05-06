# rpc.py
import asyncio
import json
from typing import Any, List, Optional

import httpx


# Custom exception for too‐many‐results errors
class TooManyResults(Exception):
    pass


# Shared timeout for all RPC calls
_RPC_TIMEOUT = 60.0


async def _post(payload: dict, url: str) -> Any:
    """Low-level JSON‐RPC POST; raises on HTTP errors."""
    async with httpx.AsyncClient(timeout=_RPC_TIMEOUT) as client:
        resp = await client.post(url, json=payload)
    resp.raise_for_status()
    return resp.json()


async def get_block(block_number_hex: str, url: str) -> dict:
    """Fetch a full block (with txs) by hex number."""
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [block_number_hex, True],
        "id": 1,
    }
    data = await _post(payload, url)
    return data["result"]


async def get_block_receipts(block_number_hex: str, url: str) -> List[dict]:
    """
    Fetch all receipts for a block via eth_getBlockReceipts (Geth/Erigon).
    If your node doesn’t support that, fallback to per‐tx receipts.
    """
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockReceipts",
        "params": [block_number_hex],
        "id": 1,
    }
    data = await _post(payload, url)
    return data["result"]


async def safe_get_logs(
    from_block: int, to_block: int, url: str
) -> Optional[List[dict]]:
    """eth_getLogs with error catch for 'too many results'."""
    from_hex = hex(from_block)
    to_hex = hex(to_block)
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [{"fromBlock": from_hex, "toBlock": to_hex}],
        "id": 1,
    }
    data = await _post(payload, url)
    if "error" in data:
        msg = data["error"].get("message", "")
        if "query returned more than" in msg:
            raise TooManyResults(msg)
        else:
            raise RuntimeError(
                f"RPC Error fetching logs {from_block}-{to_block}: {msg}"
            )
    return data.get("result", [])


async def safe_get_trace_filter(
    from_block: int, to_block: int, url: str
) -> Optional[List[dict]]:
    """trace_filter with error handling."""
    from_hex = hex(from_block)
    to_hex = hex(to_block)
    payload = {
        "jsonrpc": "2.0",
        "method": "trace_filter",
        "params": [{"fromBlock": from_hex, "toBlock": to_hex}],
        "id": 1,
    }
    data = await _post(payload, url)
    if "error" in data:
        msg = data["error"].get("message", "")
        if "query returned more than" in msg:
            raise TooManyResults(msg)
        else:
            raise RuntimeError(
                f"RPC Error fetching traces {from_block}-{to_block}: {msg}"
            )
    return data.get("result", [])
