import json
import requests


def get_block(blockNumber: str, url): 
    payload = json.dumps({
    "method": "eth_getBlockByNumber",
    "params": [
        blockNumber,
        True
    ],
    "id": 1,
    "jsonrpc": "2.0"
    })
    headers = {
    'Content-Type': 'application/json'
    }

    # send request to url
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()['result']


def get_block_receipts(blockNumber: str, url): 
    payload = json.dumps({
    "method": "eth_getBlockReceipts",
    "params": [
        blockNumber,
    ],
    "id": 1,
    "jsonrpc": "2.0"
    })
    headers = {
    'Content-Type': 'application/json'
    }

    # send request to url
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()['result']



def get_logs(from_block: str, to_block: str, url: str):
    payload = json.dumps({
        "method": "eth_getLogs",
        "params": [{"fromBlock": from_block, "toBlock": to_block}],
        "id": 1,
        "jsonrpc": "2.0"
    })
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=payload)
    return response.json()['result']


def get_trace_filter(from_block: str, to_block: str, url: str):
    payload = json.dumps({
        "method": "trace_filter",
        "params": [{"fromBlock": from_block, "toBlock": to_block}],
        "id": 1,
        "jsonrpc": "2.0"
    })
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=payload)
    return response.json()['result']
