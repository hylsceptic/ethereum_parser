import json
import pickle
import os
from abis.erc20_abi import ERC20_ABI
abs_path = os.path.abspath(__file__)

tokens = pickle.load(open(os.path.join(os.path.split(abs_path)[0], 'erc20_tokens.pkl'), 'rb'))

tokens['0x57Ab1E02fEE23774580C119740129eAC7081e9D3'] = ['sUSD', 18]
tokens['0xC011A72400E58ecD99Ee497CF89E3775d4bd732F'] = ['SNX', 18]

def parse_token(address, w3):
    if address in tokens:
        return True, (tokens[address][0], int(tokens[address][1]))
    
    try:
        contract = w3.eth.contract(address=address, abi=ERC20_ABI)
        dec = contract.functions.decimals().call()
        symbol = contract.functions.symbol().call()
        return True, (symbol, int(dec))
    except ValueError as e:
        print(f"ERC20 Token {address} not fund.")
        print(e)
        return False, ('', 0)
    except:
        print(f"ERC20 Token {address} not fund.")
        return False, ('', 0)

if __name__ == '__main__':
    print(len(tokens))