import requests
import json
from web3 import Web3

def parse_calls(raw_call):
    calls = []
    calls.append({
        'from' : Web3.toChecksumAddress(raw_call['from']),
        'to' : Web3.toChecksumAddress(raw_call['to']),
        'value' : int(raw_call['value'], 0) / 1e18 if 'value' in raw_call else 0
        })
    if 'calls' in raw_call:
        for sub_raw_call in raw_call['calls']:
            calls += parse_calls(sub_raw_call)
    return calls


def tx_call_trace(tx_hash, url):
    params = {
        "id": 1,
        "method": "debug_traceTransaction", 
        "params": [tx_hash, {"tracer" : "callTracer"}],
        }
    rst = requests.post(url, json=params)
    assert(rst.status_code == 200)
    raw_call = json.loads(rst.text)['result']
    calls = parse_calls(raw_call)
    return calls