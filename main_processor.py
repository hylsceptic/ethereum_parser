from web3 import Web3
import os.path as osp
import csv
from parsers import uniswap_parser
from parsers.uniswap_parser import parse_multi_call, parse_uniswap_trade, parse_uniswap_v1_trade
from parsers import curve_parsser
from parsers.curve_parsser import parse_curve_swap
from parsers.token_transfer_parser import parse_eth_transfer, parse_erc20_transfer
from parsers import zeroex_parser
from parsers.zeroex_parser import parse_zeroex_swap
from result_checker import check_result

from executor.bounded_executor import BoundedExecutor
from executor.fail_safe_executor import FailSafeExecutor

csv.field_size_limit(100000000)

class MainProcessor:
    def __init__(self, provider_url, exporter, max_workers=2, test=1):
        self.exporter = exporter
        if exporter.name == 'printer':
            max_workers = 1
        self.max_workers = max_workers
        self.w3 = Web3(Web3.HTTPProvider(provider_url))
        self.provider_url = provider_url
        self.executor = FailSafeExecutor(BoundedExecutor(1, self.max_workers))
        self.test = test
    
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
            self.executor.submit(self._export_item, tx)
    
    def _export_item(self, item):
        if self.test:
            self._parse_transfer_and_trade(item)
        else:
            try:
                self._parse_transfer_and_trade(item)
            except:
                print("[error]", item['hash'])
    
    def _parse_transfer_and_trade(self, item):
        if item['to_address'] == '': # contract creation.
            return
            
        logs = [] if item['input'] == '0x' else self.w3.eth.getTransactionReceipt(item['hash'])['logs']
        
        call = item['input']

        ## ERC20 transfer.
        if call.startswith('0xa9059cbb'): # erc20:transfer
            dex_trade = parse_erc20_transfer(item, self.w3)
            self.dump_result(dex_trade, 'eth_transfer')
        
        ## Uniswap V2, V3
        elif uniswap_parser.is_v2_v3_normal_call(call):
            dex_trade = parse_uniswap_trade(item, self.w3, self.provider_url, logs)
            self.dump_result(dex_trade, 'dex_trade')

        ## Uniswap V3 multicall
        elif uniswap_parser.is_v3_multi_call(call):
            dex_trades = parse_multi_call(item, self.w3, self.provider_url, logs)
            for dex_trade in dex_trades:
                self.dump_result(dex_trade, 'dex_trade')
        
        ## Uniswap V1
        elif uniswap_parser.is_v1_call(call):
            dex_trade = parse_uniswap_v1_trade(item, self.w3, self.provider_url, logs)
            self.dump_result(dex_trade, 'dex_trade')
        
        ## curve
        elif curve_parsser.is_swap_call(call):
            dex_trade = parse_curve_swap(item, self.w3, logs)
            self.dump_result(dex_trade, 'dex_trade')

        ## Ether transfer
        else:
            if item['value'] > 0:
                dex_trade = parse_eth_transfer(item)
                self.dump_result(dex_trade, 'eth_transfer')
    
        # zeroex
        dex_trades = parse_zeroex_swap(item, self.w3, logs)
        for dex_trade in dex_trades:
            self.dump_result(dex_trade, 'dex_trade')
    

    def dump_result(self, dex_trade, topic):
        if dex_trade is None:
            return
        if check_result(dex_trade, topic):
            self.exporter.dump(dex_trade, topic)


    def shutdown(self):
        self.executor.shutdown()

# python run.py --start 12009301 --end 12009401 --dest kafka
