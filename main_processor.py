from web3 import Web3
import os.path as osp
import csv
from parsers import uniswap_parser
from parsers.uniswap_parser import parse_multi_call, parse_uniswap_trade, parse_uniswap_v1_trade
from parsers import curve_parsser
from parsers.curve_parsser import parse_curve_swap
from parsers.token_transfer_parser import parse_eth_transfer, parse_erc20_transfer
from result_checker import check_result

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
                tx['block_timestamp'] = int(tx['block_timestamp'])
                
                self.txs.append(tx)
    
    def execute(self):
        for tx in self.txs:
            self.executor.submit(self._export_item, tx, self.fields)
    
    
    def _export_item(self, item, fields):
        if item['to_address'] == '': # contract creation.
            return
        
        call = item['input']
        ## ERC20 transfer.
        if call.startswith('0xa9059cbb'): # erc20:transfer
            filtered_item = parse_erc20_transfer(item, self.w3)
            self.dump_result(filtered_item, 'eth_transfer')
        
        ## Uniswap V2, V3
        elif uniswap_parser.is_v2_v3_normal_call(call):
            filtered_item = parse_uniswap_trade(item, self.w3)
            self.dump_result(filtered_item, 'dex_trade')

        ## Uniswap V3 multicall
        elif uniswap_parser.is_v3_multi_call(call):
            filtered_items = parse_multi_call(item, self.w3)
            for filtered_item in filtered_items:
                self.dump_result(filtered_item, 'dex_trade')
        
        ## Uniswap V1
        elif uniswap_parser.is_v1_call(call):
            filtered_item = parse_uniswap_v1_trade(item, self.w3, self.provider_url)
            self.dump_result(filtered_item, 'dex_trade')
        
        ## curve
        elif curve_parsser.is_swap_call(call):
            filtered_item = parse_curve_swap(item, self.w3)
            self.dump_result(filtered_item, 'dex_trade')

        # ## zeroex
        # elif zeroex_parser.is_swap_call(call):
        #     parse_zeroex_swap(item, self.w3)
            

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
        check_result(filtered_item, topic)
        self.exporter.dump(filtered_item, topic)


    def shutdown(self):
        self.executor.shutdown()

