from web3 import Web3
from parsers.curve_pools import pools

CURVE_EVT_TOKENEXCHANGE = '0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140'
CURVE_EVT_TOKENEXCHANGEUNDERLYING = '0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b'

def curve_parser(item, w3):
    filtered_item = {key : item[key] for key in ['hash', 'block_timestamp']}
    filtered_item['method_call'] = item['input'][:10]
    filtered_item['contract_address'] = item['to_address']
    filtered_item['send_address'] = item['from_address']
    filtered_item['receive_address'] = item['from_address']
    
    if (item['input'].startswith('0x3df02124')            # exchange
            or item['input'].startswith('0xa6417ed6')     # exchange_underlying
            ):
        contact = item['to_address']
        if not contact in pools.keys():
            return
        
        pool = pools[contact]
        filtered_item['dex'] = 'curve' + pool['name']
        tokens = pool['coins']
        
        i = int('0x' + item['input'][10 : 74], 0)
        j = int('0x' + item['input'][74 : 138], 0)

        if i >= len(tokens) or j >= len(tokens):
            return

        if item['input'].startswith('0x3df02124'):
            filtered_item['send_token_contract_address'] = tokens[i]['wrapped_address']
            filtered_item['send_token'] = tokens[i]['underlying_name']
            filtered_item['receive_token_contract_address'] = tokens[j]['wrapped_address']
            filtered_item['receive_token'] = tokens[j]['underlying_name']
        else:
            filtered_item['send_token_contract_address'] = tokens[i]['underlying_address']
            filtered_item['send_token'] = tokens[i]['name']
            filtered_item['receive_token_contract_address'] = tokens[j]['underlying_address']
            filtered_item['receive_token'] = tokens[j]['name']

        filtered_item['send_decimals'] = tokens[i]['decimals']
        filtered_item['receive_decimals'] = tokens[j]['decimals']
        

        logs = w3.eth.getTransactionReceipt(item['hash'])['logs']
        if logs == []:return None  ## Error encountered during contract execution [Reverted]
        for log in logs:
            if (log['topics'][0].hex() in [CURVE_EVT_TOKENEXCHANGE, CURVE_EVT_TOKENEXCHANGEUNDERLYING]):
                filtered_item['send_value'] = int('0x' + log['data'][66 : 130], 0) / (10 ** filtered_item['send_decimals'])
                filtered_item['receive_value'] = int('0x' + log['data'][194 : 258], 0) / (10 ** filtered_item['receive_decimals'])

        return filtered_item