from web3 import Web3
from erc20_buffer.erc20_parser import parse_token

def parse_eth_transfer(item):
    filtered_item = {key : item[key] for key in ['hash', 'block_timestamp', 'value', 'from_address', 'to_address']}
    filtered_item['symbol'] = 'ETH'
    filtered_item['value'] /= 1e18
    filtered_item['decimals'] = 18
    filtered_item['contract_address'] = '0x'
    return filtered_item


def parse_erc20_transfer(item, w3):
    address = Web3.toChecksumAddress(item['to_address'])
    filtered_item = {key : item[key] for key in ['hash', 'block_timestamp', 'from_address']}
    rst, (symbol, dec) = parse_token(address, w3)
    if not rst: return
    filtered_item['symbol'] = symbol
    filtered_item['to_address'] = '0x' + item['input'][34:74]
    filtered_item['value'] = int('0x' + item['input'][74 : 138], 0) / 10 ** dec
    filtered_item['contract_address'] = item['to_address']
    filtered_item['decimals'] = dec
    return filtered_item