import json
import os

abs_path = os.path.abspath(__file__)

tokens = json.load(open(os.path.join(os.path.split(abs_path)[0], 'erc20_tokens.json')))

def parse_token(address, w3):
    if address in tokens:
        return True, (tokens[address][0], int(tokens[address][1]))
    try:
        contract = w3.eth.contract(address=address, abi=ERC20_ABI)
        dec = contract.functions.decimals().call()
        symbol = contract.functions.symbol().call()
        return dec, symbol
    except ValueError as e:
            print(e)
            return False, ('', 0)
    except:
        return False, ('', 0)