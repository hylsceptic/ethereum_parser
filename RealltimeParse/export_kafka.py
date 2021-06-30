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
WETH_EVT_DEPOSIT = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
ERC2_EVT_TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

w3 = Web3(Web3.HTTPProvider('http://121.199.22.236:6666'))

def erc20_contract(address):
    return w3.eth.contract(address=address, abi=ERC20_ABI)

def usv2p_contract(address):
    return w3.eth.contract(address=address, abi=UNIESWP_V2_PAIR_ABI)


def parse_eth_transfer(item):
    filtered_item = {key : item[key] for key in ['hash', 'block_timestamp', 'value', 'from_address', 'to_address']}
    filtered_item['symbol'] = 'ETH'
    filtered_item['value'] /= 1e18
    return filtered_item


def parse_uniswap_trade(item, w3):
    logs = w3.eth.getTransactionReceipt(item['hash'])['logs']
    if logs == []:return None  ## Error encountered during contract execution [Reverted]

    filtered_item = {key : item[key] for key in ['hash', 'block_timestamp', 'from_address']}
    if (item['input'].startswith('0xc04b8d59') or item['input'].startswith('0x414bf389')
            or item['input'].startswith('0xf28c0498') or item['input'].startswith('0xdb3e2198')):
        filtered_item['dex'] = 'UniswapV3'
    else:
        filtered_item['dex'] = 'UniswapV2'
    
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
            if filtered_item is None or filtered_item['dex'] != 'UniswapV3':
                return

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