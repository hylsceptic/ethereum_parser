from web3 import Web3
import json
import os
import os.path as osp
import csv
from web3_input_decoder import decode_constructor, decode_function
from abis.erc20_abi import ERC20_ABI
from abis.uniswap_v2_pair_abi import UNIESWP_V2_PAIR_ABI
from erc20_buffer.erc20_parser import parse_token
from parsers.uniswap_parser import parse_multi_call, parse_uniswap_trade, parse_uniswap_v1_trade, is_v1_call, is_v2_v3_normal_call
from parsers.curve_parsser import curve_parser
from parsers.token_transfer_parser import parse_eth_transfer, parse_erc20_transfer

from executor.bounded_executor import BoundedExecutor
from executor.fail_safe_executor import FailSafeExecutor

csv.field_size_limit(100000000)

class MainProcessor:
    def __init__(self, provider_url, exporter, max_workers=2):
        self.exporter = exporter
        if exporter.name == 'printer':
            max_workers = 1
        self.max_workers = max_workers
        self.w3 = Web3(Web3.HTTPProvider(provider_url))
        self.provider_url = provider_url
        self.executor = FailSafeExecutor(BoundedExecutor(1, self.max_workers))
    
    def load_transactions(self, file_path='transactions.csv'):
        self.txs = []
        with open(file_path, newline='\n') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',')
            self.fields = next(spamreader)
            for row in spamreader:
                if len(row) == 0: continue
                tx = {}
                for idx in range(len(self.fields)):
                    tx[self.fields[idx]] = row[idx]
                
                tx['value'] = int(tx['value'])
                self.txs.append(tx)
    
    def execute(self):
        for tx in self.txs:
            self.executor.submit(self._export_item, tx, self.fields)
    
    
    def _export_item(self, item, fields):
        if item['to_address'] == '': # contract creation.
            return
        ## ERC20 transfer.
        if item['input'].startswith('0xa9059cbb'): # erc20:transfer
            filtered_item = parse_erc20_transfer(item, self.w3)
            self.dump_result(filtered_item, 'eth_transfer')
        
        ## Uniswap V2, V3
        elif is_v2_v3_normal_call(item['input']):
            filtered_item = parse_uniswap_trade(item, self.w3)
            self.dump_result(filtered_item, 'dex_trade')

        ## Uniswap V3 multicall
        elif (item['input'].startswith('0xac9650d8')):
            filtered_items = parse_multi_call(item, self.w3)
            for filtered_item in filtered_items:
                self.dump_result(filtered_item, 'dex_trade')
        
        ## Uniswap V1
        elif is_v1_call(item['input']):
            filtered_item = parse_uniswap_v1_trade(item, self.w3, self.provider_url)
            self.dump_result(filtered_item, 'dex_trade')
        
        ## curve
        elif item['input'].startswith('0x3df02124') or item['input'].startswith('0xa6417ed6'):
            curve_parser(item, self.w3)

        ## Ether transfer
        else:
            if item['value'] > 0:
                filtered_item = parse_eth_transfer(item)
                self.dump_result(filtered_item, 'eth_transfer')
            else:
                return
    
    
    def dump_result(self, filtered_item, topic):
        if filtered_item is None:
            return
        self.exporter.dump(filtered_item, topic)


    def shutdown(self):
        self.executor.shutdown()

