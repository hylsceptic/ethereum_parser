from kafka import KafkaProducer
from web3 import Web3
import json
import os
import os.path as osp
import csv
import pprint as pp
from web3_input_decoder import decode_constructor, decode_function
from erc20_abi import ERC20_ABI
from uniswap_v2_pair_abi import UNIESWP_V2_PAIR_ABI
from  call_parser import tx_call_trace

def Load_transactions(file_path='transactions.csv'):
    txs = []
    with open(file_path, newline='\n') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        fields = next(spamreader)
        for row in spamreader:
            if len(row) == 0: continue
            tx = {}
            for idx in range(len(fields)):
                tx[fields[idx]] = row[idx]
            
            tx['value'] = int(tx['value'])
            txs.append(tx)
    
    return txs, fields

WETH_ADDR = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'

UNISWAP_FORKS_ROUTERS = {
    '0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F' : 'UniSwapV2',
    '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D' : 'SushiSwap',
    '0xE6E90bC9F3b95cdB69F48c7bFdd0edE1386b135a' : 'UnicSwapV2'
}
WETH_EVT_DEPOSIT = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
ERC2_EVT_TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'


SwapExactETHForTokens = '0x7ff36ab5'
swapETHForExactTokens = '0xfb3bdb41'
swapExactETHForTokensSupportingFeeOnTransferTokens = '0xb6f9de95'
swapTokensForExactTokens = '0x8803dbee'
swapExactTokensForTokens = '0x38ed1739'
swapExactTokensForTokensSupportingFeeOnTransferTokens = '0x5c11d795'
swapExactTokensForETH = '0x18cbafe5'
swapTokensForExactETH = '0x4a25d94a'
swapExactTokensForETHSupportingFeeOnTransferTokens = '0x791ac947'
uniswapV3RouterexactInput = '0xc04b8d59'
uniswapV3RouterexactInputSingle = '0x414bf389'
uniswapV3RouterExactOutputSingle = '0xdb3e2198'
uniswapV3RouterExactOutput = '0xf28c0498'

client_url = 'http://121.199.22.236:6666'
w3 = Web3(Web3.HTTPProvider(client_url))

def erc20_contract(address):
    return w3.eth.contract(address=address, abi=ERC20_ABI)

def usv2p_contract(address):
    return w3.eth.contract(address=address, abi=UNIESWP_V2_PAIR_ABI)


def parse_eth_transfer(item):
    filtered_item = {key : item[key] for key in ['hash', 'block_timestamp', 'value', 'from_address', 'to_address']}
    filtered_item['symbol'] = 'ETH'
    filtered_item['value'] /= 1e18
    return filtered_item


def parse_multi_call(item, w3):
    call_data = item['input']
    assert(call_data[:10] == '0xac9650d8')
    calls = int('0x' + call_data[74:138], 0)
    pt = 138
    filtered_items = []
    for idx in range(calls):
        start = int('0x' + call_data[pt + idx * 64 : pt + 64 * (idx + 1)], 0) * 2 + pt
        length = int('0x' + call_data[start : start + 64], 0)
        sub_call = '0x' + call_data[start + 64 : start + length * 2 + 64]
        
        if (sub_call.startswith('0x414bf389') or sub_call.startswith('0xb6f9de95')
                or sub_call.startswith('0xdb3e2198') or sub_call.startswith('0xf28c0498')):
            item['input'] = sub_call
            filtered_items.append(parse_uniswap_trade(item, w3))
    return filtered_items


def parse_uniswap_trade(item, w3):
    logs = w3.eth.getTransactionReceipt(item['hash'])['logs']
    if logs == []:return None  ## Error encountered during contract execution [Reverted]

    filtered_item = {key : item[key] for key in ['hash', 'block_timestamp', 'from_address']}
    if (item['input'].startswith('0xc04b8d59') or item['input'].startswith('0x414bf389')
            or item['input'].startswith('0xf28c0498') or item['input'].startswith('0xdb3e2198')):
        filtered_item['dex'] = 'UniswapV3'
    else:
        if Web3.toChecksumAddress(item['to_address']) in UNISWAP_FORKS_ROUTERS.keys():
            filtered_item['dex'] = UNISWAP_FORKS_ROUTERS[Web3.toChecksumAddress(item['to_address'])]
        else:
            print(item['to_address'])
            filtered_item['dex'] = 'UNKNOWN'


    filtered_item['method_call'] = item['input'][:10]
    filtered_item['contract_address'] = item['to_address']
    filtered_item['receive_value'] = 0
    filtered_item['send_value'] = 0
    
    if (item['input'].startswith('0x7ff36ab5')            # uniswap v2 router: swapExactETHForTokens
            or item['input'].startswith('0xfb3bdb41')     # uniswap v2 router: swapETHForExactTokens
            or item['input'].startswith('0xb6f9de95')     #TODO velidating swapExactETHForTokensSupportingFeeOnTransferTokens call
            ):
        filtered_item['receive_address'] = Web3.toChecksumAddress('0x' + item['input'][162 : 202])
        filtered_item['send_token'] = 'WETH'
        for log in logs:
            if (Web3.toChecksumAddress(log['address']) == WETH_ADDR 
                    and log['topics'][0].hex() == WETH_EVT_DEPOSIT):
                filtered_item['send_value'] += int(log['data'], 0) / 1e18
                
    elif (item['input'].startswith('0x8803dbee')          # uniswap v2 router: swapTokensForExactTokens
            or item['input'].startswith('0x38ed1739')     # uniswap v2 router: swapExactTokensForTokens
            or item['input'].startswith('0x5c11d795')     # uniswap v2 router: swapExactTokensForTokensSupportingFeeOnTransferTokens
            ):
        filtered_item['receive_address'] = Web3.toChecksumAddress('0x' + item['input'][226 : 266])

    elif (item['input'].startswith('0x18cbafe5')          # uniswap v2 router: swapExactTokensForETH
            or item['input'].startswith('0x4a25d94a')     # uniswap v2 router: swapTokensForExactETH
            or item['input'].startswith('0x791ac947')     # uniswap v2 router: swapExactTokensForETHSupportingFeeOnTransferTokens
            ):
        filtered_item['receive_address'] = Web3.toChecksumAddress('0x' + item['input'][226 : 266])
        filtered_item['receive_token'] = 'WETH'
        for log in logs:
            if (Web3.toChecksumAddress(log['address']) == WETH_ADDR
                    and len(log['topics']) >= 3 and Web3.toChecksumAddress('0x' + log['topics'][2].hex()[-40:]) == Web3.toChecksumAddress(item['to_address'])):
                filtered_item['receive_value'] += int(log['data'], 0) / 1e18
    
    elif (item['input'].startswith('0xc04b8d59')          # uniswap v3 router: exactInput
            or item['input'].startswith('0x414bf389')     # uniswap v3 router: exactInputSingle
            or item['input'].startswith('0xdb3e2198')     # uniswap v3 router: ExactOutputSingle
            or item['input'].startswith('0xf28c0498')     # uniswap v3 router: ExactOutput
            ):
        if item['input'].startswith('0x414bf389') or item['input'].startswith('0xdb3e2198'):
            filtered_item['receive_address'] = Web3.toChecksumAddress('0x' + item['input'][226 : 266])
            ##TODO find out why. Hash: 0xb33e479e694987911a3ae1f3da8da12dd2431e5f6bccaef323b1e2453061255b
            if filtered_item['receive_address'] == '0x0000000000000000000000000000000000000000':
                filtered_item['receive_address'] = item['from_address']
            token_in_addr = Web3.toChecksumAddress('0x' + item['input'][34:74])
            token_out_addr = Web3.toChecksumAddress('0x' + item['input'][98:138])
        else:
            filtered_item['receive_address'] = Web3.toChecksumAddress('0x' + item['input'][162 : 202])
            path_len = int('0x' + item['input'][394 : 458], 0)
            token_in_addr = Web3.toChecksumAddress('0x' + item['input'][458 : 498])
            token_out_addr = Web3.toChecksumAddress('0x' + item['input'][458 + path_len * 2 - 40 : 458 + path_len * 2])
        
        
        if token_in_addr == WETH_ADDR:
            filtered_item['send_token'] = 'WETH'
            filtered_item['send_value'] = 0
            for log in logs:
                if (Web3.toChecksumAddress(log['address']) == WETH_ADDR 
                        and log['topics'][0].hex() == WETH_EVT_DEPOSIT):
                    filtered_item['send_value'] += int(log['data'], 0) / 1e18
        
        if token_out_addr == WETH_ADDR:
            filtered_item['receive_token'] = 'WETH'
            for log in logs:
                if (Web3.toChecksumAddress(log['address']) == WETH_ADDR
                        and len(log['topics']) >= 3 and Web3.toChecksumAddress('0x' + log['topics'][2].hex()[-40:]) == Web3.toChecksumAddress(item['to_address'])):
                    filtered_item['receive_value'] += int(log['data'], 0) / 1e18


    for tlog in logs:
        if (len(tlog['topics']) >= 3 and tlog['topics'][0].hex() == ERC2_EVT_TRANSFER):
            if Web3.toChecksumAddress('0x' + tlog['topics'][2].hex()[-40:]) == Web3.toChecksumAddress(filtered_item['receive_address']):
                token_contract = erc20_contract(tlog['address'])
                filtered_item['receive_token'] = token_contract.functions.symbol().call()
                filtered_item['receive_value'] += int(tlog['data'], 0) / (10 ** token_contract.functions.decimals().call())

            elif Web3.toChecksumAddress('0x' + tlog['topics'][1].hex()[-40:]) == Web3.toChecksumAddress(item['from_address']):
                token_contract = erc20_contract(tlog['address'])
                filtered_item['send_token'] = token_contract.functions.symbol().call()
                filtered_item['send_value'] += int(tlog['data'], 0) / (10 ** token_contract.functions.decimals().call())

    return filtered_item


def parse_uniswap_v1_trade(item, w3):
    logs = w3.eth.getTransactionReceipt(item['hash'])['logs']
    if logs == []:return None  ## Error encountered during contract execution [Reverted]

    filtered_item = {key : item[key] for key in ['hash', 'block_timestamp', 'from_address']}
    filtered_item['dex'] = 'UniswapV1'
    
    filtered_item['method_call'] = item['input'][:10]
    filtered_item['contract_address'] = item['to_address']
    filtered_item['receive_value'] = 0
    filtered_item['send_value'] = 0

    if (item['input'].startswith('0xf39b5b9b')                 # uniswap v1: ethToTokenSwapInput
        or item['input'].startswith('0xad65d76d')              # uniswap v1: ethToTokenTransferInput
            ):
        filtered_item['send_token'] = 'ETH'
        filtered_item['send_value'] = item['value'] / 1e18
        if item['input'].startswith('0xf39b5b9b'):
            filtered_item['receive_address'] = item['from_address']
        else:
            filtered_item['receive_address'] = Web3.toChecksumAddress('0x' + item['input'][162 : 202])
    elif (item['input'].startswith('0xf552d91b')               # uniswap v1: tokenToTokenTransferInput
        # or item['input'].startswith('')              # uniswap v1: 
            ):
            if False:
                ...
            else:
                filtered_item['receive_address'] = Web3.toChecksumAddress('0x' + item['input'][290 : 330])
    elif (item['input'].startswith('0x7237e031')               # uniswap v1: tokenToEthTransferInput
            ):
            if False:
                ...
            else:
                filtered_item['receive_address'] = Web3.toChecksumAddress('0x' + item['input'][226 : 266])
                filtered_item['receive_token'] = 'ETH'
            calls = tx_call_trace(item['hash'], client_url)
            for call in calls:
                if call['to'] == filtered_item['receive_address']:
                    filtered_item['receive_value'] += call['value']

    
    for tlog in logs:
        if (len(tlog['topics']) >= 3 and tlog['topics'][0].hex() == ERC2_EVT_TRANSFER):
            if Web3.toChecksumAddress('0x' + tlog['topics'][2].hex()[-40:]) == Web3.toChecksumAddress(filtered_item['receive_address']):
                token_contract = erc20_contract(tlog['address'])
                filtered_item['receive_token'] = token_contract.functions.symbol().call()
                filtered_item['receive_value'] += int(tlog['data'], 0) / (10 ** token_contract.functions.decimals().call())

            elif Web3.toChecksumAddress('0x' + tlog['topics'][1].hex()[-40:]) == Web3.toChecksumAddress(item['from_address']):
                token_contract = erc20_contract(tlog['address'])
                filtered_item['send_token'] = token_contract.functions.symbol().call()
                filtered_item['send_value'] += int(tlog['data'], 0) / (10 ** token_contract.functions.decimals().call())

    return filtered_item

def dump_result(filtered_item):
    if filtered_item is None:
        return
    print(len(filtered_item.keys()))
    try:
        print(filtered_item)
    except UnicodeEncodeError as e:
        filtered_item['receive_token'] = filtered_item['receive_token'].encode("utf-8")
        filtered_item['send_token'] = filtered_item['send_token'].encode("utf-8")
        print(filtered_item)
    print()


def parse_erc20_transfer(item, w3):
    address = Web3.toChecksumAddress(item['to_address'])
    contract = w3.eth.contract(address=address, abi=ERC20_ABI)
    filtered_item = {key : item[key] for key in ['hash', 'block_timestamp', 'from_address']}
    
    try:
        dec = contract.functions.decimals().call()
        filtered_item['symbol'] = contract.functions.symbol().call()
    except ValueError as e:
            print(e)
            return
    except:
        return
    
    filtered_item['to_address'] = '0x' + item['input'][34:74]
    filtered_item['value'] = int('0x' + item['input'][74:], 0) / 10 ** dec
    filtered_item['contract_address'] = item['to_address']
    filtered_item['decimals'] = dec
    return filtered_item


class KafkaExporter:
    def __init__(self):
        self.w3 = Web3(Web3.HTTPProvider('http://121.199.22.236:6666'))
        self.producer = KafkaProducer(bootstrap_servers='172.16.1.21:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    def export_item(self, item, fields):
        if item['input'].startswith('0xa9059cbb'): # erc20:transfer
            filtered_item = parse_erc20_transfer(item, self.w3)
        
        elif (item['input'].startswith('0x7ff36ab5') or item['input'].startswith('0xfb3bdb41') 
                or item['input'].startswith('0x8803dbee') or item['input'].startswith('0x38ed1739')
                or item['input'].startswith('0x18cbafe5') or item['input'].startswith('0x4a25d94a')
                or item['input'].startswith('0x791ac947') or item['input'].startswith('0x5c11d795')
                or item['input'].startswith('0xc04b8d59') or item['input'].startswith('0x414bf389')
                or item['input'].startswith('0xb6f9de95') or item['input'].startswith('0xdb3e2198')
                or item['input'].startswith('0xf28c0498')):
            filtered_item = parse_uniswap_trade(item, self.w3)
            dump_result(filtered_item)
            # if filtered_item is None or filtered_item['dex'] != 'UniswapV3':
            #     return

        elif (item['input'].startswith('0xac9650d8')):
            filtered_items = parse_multi_call(item, w3)
            for filtered_item in filtered_items:
                print(len(filtered_item.keys()))
                try:
                    print(filtered_item)
                except UnicodeEncodeError as e:
                    filtered_item['receive_token'] = filtered_item['receive_token'].encode("utf-8")
                    filtered_item['send_token'] = filtered_item['send_token'].encode("utf-8")
                    print(filtered_item)
                print()
        
        elif (item['input'].startswith('0xf39b5b9b') or item['input'].startswith('0xad65d76d')
                or item['input'].startswith('0xf552d91b') or item['input'].startswith('0x7237e031')):
            filtered_item = parse_uniswap_v1_trade(item, w3)
            print(len(filtered_item.keys()))
            try:
                print(filtered_item)
            except UnicodeEncodeError as e:
                filtered_item['receive_token'] = filtered_item['receive_token'].encode("utf-8")
                filtered_item['send_token'] = filtered_item['send_token'].encode("utf-8")
                print(filtered_item)
            print()

        else:
            if item['value'] > 0:
                filtered_item = parse_eth_transfer(item)
            else:
                return

        # future = self.producer.send('eth_transfer', filtered_item)
        # self.producer.flush()
        # print(filtered_item)

def export_kafka():
    exporter = KafkaExporter()

    txs, fields = Load_transactions()
    for item in txs:
        exporter.export_item(item, fields)

export_kafka()