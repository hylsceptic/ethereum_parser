from web3 import Web3
from erc20_buffer.erc20_parser import parse_token


# exchanges:
# zeroex _v1   0x12459C951127e0c374FF9105DdA097662A027093
# zeroex_v2.0  0x4F833a24e1f95D70F028921e27040Ca56E09AB0b
# zeroex_v2.1  0x080bf510FCbF18b91105470639e9561022937712
# zeroex v3    0x61935CbDd02287B511119DDb11Aeb42F1593b7Ef
# zeroex v4    0xDef1C0ded9bec7F1a1670819833240f027b25EfF

# asset data hash
# ERC20Token(address):                              0xf47261b06eedbfce68afd46d0f3c27c60b03faad319eaf33103611cf8f6456ad
# ERC721Token(address,uint256):                     0x0257179262a09e2dc7b2d43c61f09a5cf1b0c137c8ac216cbe0a97d744c479ed
# ERC1155Assets(bytes,uint256[],uint256[],bytes):   0x64809efb0b7207fc272410530265f4a791706c9cdc750863e11934e5a709ebbe
# MultiAsset(uint256[],bytes[]):                    0x94cfcdd724aec111a2a9db8eb5238c110fa68fabecd5e836974fece6ebf20136
# StaticCall(address,bytes,bytes32):                0xc339d10a05b3aab34bbc74251d27a788ab6222de7f4fe38279fc088746192830
# ERC20Bridge(address,address,bytes):               0xdc1600f3f7e0a5691bfcd2a75a05f028b57996f75df60703026fc3dad804aade


ZEROEX_V1_EVT_FILL             = '0x0d0b9391970d9a25552f37d436d2aae2925e2bfe1b2a923754bada030c498cb3'
ZEROEX_V2_EVT_FILL             = '0x0bcc4c97732e47d9946f229edb95f5b6323f601300e4690de719993f3c371129'
ZEROEX_V3_EVT_FILL             = '0x6869791f0a34781b29882982cc39e882768cf2c96995c2a110c577c53bc932d5'
ZEROEX_V4_EVT_LIMITORDERFILLED = '0xab614d2b738543c0ea21f56347cf696a3a0c42a7cbec3212a5ca22a4dcff2124'
ZEROEX_V4_EVT_RFQORDERFILLED   = '0x829fa99d94dc4636925b38632e625736a614c154d55006b7ab6bea979c210c32'
    
def parse_zeroex_swap(item, w3, logs=None):
    if logs is None:
        logs = self.w3.eth.getTransactionReceipt(item['hash'])['logs']

    trades = []

    for log in logs:
        if len(log['topics']) == 0:
            continue
        if log['topics'][0].hex() == ZEROEX_V3_EVT_FILL:
            maker_address = '0x' + log['topics'][1].hex()[-40:]
            fee_recipient_address = '0x' + log['topics'][2].hex()[-40:]
            
            data = log['data'][2:]
            
            i = 4
            taker_address = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 5
            sender_address = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 6
            maker_asset_filled_amount = int('0x' + data[i * 64 : i * 64 + 64], 0)
            i = 7
            taker_asset_filled_amount = int('0x' + data[i * 64 : i * 64 + 64], 0)
            i = 8
            maker_fee_paid = int('0x' + data[i * 64 : i * 64 + 64], 0)
            i = 9
            taker_fee_paid = int('0x' + data[i * 64 : i * 64 + 64], 0)
            i = 10
            protocol_fee_paid = int('0x' + data[i * 64 : i * 64 + 64], 0)
            i = 11
            length = int('0x' + data[i * 64 : i * 64 + 64], 0) * 2
            i = 12
            maker_asset_data = data[i * 64 : i * 64 + length]
            i += length // 64 + 1
            length = int('0x' + data[i * 64 : i * 64 + 64], 0) * 2
            i += 1
            taker_asset_data = data[i * 64 : i * 64 + length]
            
            i += length // 64 + 1
            length = int('0x' + data[i * 64 : i * 64 + 64], 0) * 2
            i += 1
            maker_fee_asset_data = data[i * 64 : i * 64 + length]
            
            if length > 0:
                i += length // 64 + 1
            length = int('0x' + data[i * 64 : i * 64 + 64], 0) * 2
            i += 1
            taker_fee_asset_data = data[i * 64 : i * 64 + length]

            if taker_asset_data.startswith('f47261b0'):
                if maker_asset_data.startswith('f47261b0'):
                    maker_asset_addr = '0x' + maker_asset_data[-40:]
                    taker_asset_addr = '0x' + taker_asset_data[-40:]
                    
                    rst, (maker_symbol, maker_dec) = parse_token(Web3.toChecksumAddress(maker_asset_addr), w3)
                    if not rst: return None
                    rst, (taker_symbol, taker_dec) = parse_token(Web3.toChecksumAddress(taker_asset_addr), w3)
                    if not rst: return None
                    trade1 = {key : item[key] for key in ['hash', 'block_timestamp']}
                    trade1['method_call'] = item['input'][:10]
                    trade1['contract_address'] = item['to_address']
                    trade1['dex'] = '0x_protocol'
                    trade1['send_address'] = maker_address
                    trade1['send_token'] = maker_symbol
                    trade1['send_decimals'] = maker_dec
                    trade1['send_token_contract_address'] = maker_asset_addr
                    trade1['send_value'] = float(maker_asset_filled_amount / 10 ** maker_dec)
                    trade1['receive_address'] = maker_address
                    trade1['receive_token'] = taker_symbol
                    trade1['receive_decimals'] = taker_dec
                    trade1['receive_token_contract_address'] = taker_asset_addr
                    trade1['receive_value'] = float(taker_asset_filled_amount / 10 ** taker_dec)
                    
                    trade2 = {key : item[key] for key in ['hash', 'block_timestamp']}
                    trade2['method_call'] = item['input'][:10]
                    trade2['contract_address'] = item['to_address']
                    trade2['dex'] = '0x_protocol'
                    trade2['receive_address'] = taker_address
                    trade2['receive_token'] = maker_symbol
                    trade2['receive_decimals'] = maker_dec
                    trade2['receive_token_contract_address'] = maker_asset_addr
                    trade2['receive_value'] = float(maker_asset_filled_amount / 10 ** maker_dec)
                    trade2['send_address'] = taker_address
                    trade2['send_token'] = taker_symbol
                    trade2['send_decimals'] = taker_dec
                    trade2['send_token_contract_address'] = taker_asset_addr
                    trade2['send_value'] = float(taker_asset_filled_amount / 10 ** taker_dec)

                    trades += [trade1, trade2]
                
                elif maker_asset_data.startswith('dc1600f3'):
                    print(maker_asset_data)
                    taker_asset_addr = '0x' + taker_asset_data[32:72]
                    maker_asset_addr = '0x' + maker_asset_data[32:72]
                    print('taker_asset_addr', taker_asset_addr)
                    print('maker_asset_addr', maker_asset_addr)
                    print('maker_address', maker_address)
                    print('taker_address', taker_address)

                    rst, (maker_symbol, maker_dec) = parse_token(Web3.toChecksumAddress(maker_asset_addr), w3)
                    if not rst: return None
                    rst, (taker_symbol, taker_dec) = parse_token(Web3.toChecksumAddress(taker_asset_addr), w3)
                    if not rst: return None

                    trade = {key : item[key] for key in ['hash', 'block_timestamp']}
                    trade['method_call'] = item['input'][:10]
                    trade['contract_address'] = item['to_address']
                    trade['dex'] = '0x_protocol'
                    
                    trade['send_address'] = taker_address
                    trade['send_token'] = taker_symbol
                    trade['send_decimals'] = taker_dec
                    trade['send_token_contract_address'] =  taker_asset_addr
                    trade['send_value'] = float(taker_asset_filled_amount / 10 ** taker_dec)

                    trade['receive_address'] = taker_address
                    trade['receive_token'] = maker_symbol
                    trade['receive_decimals'] = maker_dec
                    trade['receive_token_contract_address'] = maker_asset_addr
                    trade['receive_value'] = float(maker_asset_filled_amount / 10 ** maker_dec)
                    
                    trades.append(trade)
        
        elif log['topics'][0].hex() == ZEROEX_V2_EVT_FILL:
            maker_address = '0x' + log['topics'][1].hex()[-40:]
            fee_recipient_address = '0x' + log['topics'][2].hex()[-40:]
            
            data = log['data'][2:]

            i = 0
            taker_address = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 1
            sender_address = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 2
            maker_asset_filled_amount = int('0x' + data[i * 64 : i * 64 + 64], 0)
            i = 3
            taker_asset_filled_amount = int('0x' + data[i * 64 : i * 64 + 64], 0)
            i = 4
            maker_fee_paid = int('0x' + data[i * 64 : i * 64 + 64], 0)
            i = 5
            taker_fee_paid = int('0x' + data[i * 64 : i * 64 + 64], 0)
            
            i = 8
            length = int('0x' + data[i * 64 : i * 64 + 64], 0) * 2
            i = 9
            maker_asset_data = data[i * 64 : i * 64 + length]
            i += length // 64 + 1
            length = int('0x' + data[i * 64 : i * 64 + 64], 0) * 2
            i += 1
            taker_asset_data = data[i * 64 : i * 64 + length]
            
            # 0x_protocol v2 does not support erc20_bridge.
            if taker_asset_data.startswith('f47261b0') and maker_asset_data.startswith('f47261b0'):
                maker_asset_addr = '0x' + maker_asset_data[-40:]
                taker_asset_addr = '0x' + taker_asset_data[-40:]
                
                rst, (maker_symbol, maker_dec) = parse_token(Web3.toChecksumAddress(maker_asset_addr), w3)
                if not rst: return None
                rst, (taker_symbol, taker_dec) = parse_token(Web3.toChecksumAddress(taker_asset_addr), w3)
                if not rst: return None
                # print(maker_asset_addr)
                # print(taker_asset_addr)
                trade1 = {key : item[key] for key in ['hash', 'block_timestamp']}
                trade1['method_call'] = item['input'][:10]
                trade1['contract_address'] = item['to_address']
                trade1['dex'] = '0x_protocol'
                trade1['send_address'] = maker_address
                trade1['send_token'] = maker_symbol
                trade1['send_decimals'] = maker_dec
                trade1['send_token_contract_address'] = maker_asset_addr
                trade1['send_value'] = float(maker_asset_filled_amount / 10 ** maker_dec)
                trade1['receive_address'] = maker_address
                trade1['receive_token'] = taker_symbol
                trade1['receive_decimals'] = taker_dec
                trade1['receive_token_contract_address'] = taker_asset_addr
                trade1['receive_value'] = float(taker_asset_filled_amount / 10 ** taker_dec)
                
                trade2 = {key : item[key] for key in ['hash', 'block_timestamp']}
                trade2['method_call'] = item['input'][:10]
                trade2['contract_address'] = item['to_address']
                trade2['dex'] = '0x_protocol'
                trade2['receive_address'] = taker_address
                trade2['receive_token'] = maker_symbol
                trade2['receive_decimals'] = maker_dec
                trade2['receive_token_contract_address'] = maker_asset_addr
                trade2['receive_value'] = float(maker_asset_filled_amount / 10 ** maker_dec)
                trade2['send_address'] = taker_address
                trade2['send_token'] = taker_symbol
                trade2['send_decimals'] = taker_dec
                trade2['send_token_contract_address'] = taker_asset_addr
                trade2['send_value'] = float(taker_asset_filled_amount / 10 ** taker_dec)
                trades += [trade1, trade2]
        
        elif log['topics'][0].hex() == ZEROEX_V1_EVT_FILL:
            maker_address = '0x' + log['topics'][1].hex()[-40:]
            fee_recipient = '0x' + log['topics'][1].hex()[-40:]
            
            data = log['data'][2:]

            i = 0
            taker_address = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 1
            maker_asset_addr = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 2
            taker_asset_addr = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 3
            maker_asset_filled_amount = int('0x' + data[i * 64 : i * 64 + 64], 0)
            i = 4
            taker_asset_filled_amount = int('0x' + data[i * 64 : i * 64 + 64], 0)

            rst, (maker_symbol, maker_dec) = parse_token(Web3.toChecksumAddress(maker_asset_addr), w3)
            if not rst: return None
            rst, (taker_symbol, taker_dec) = parse_token(Web3.toChecksumAddress(taker_asset_addr), w3)
            if not rst: return None
            
            trade1 = {key : item[key] for key in ['hash', 'block_timestamp']}
            trade1['method_call'] = item['input'][:10]
            trade1['contract_address'] = item['to_address']
            trade1['dex'] = '0x_protocol'
            trade1['send_address'] = maker_address
            trade1['send_token'] = maker_symbol
            trade1['send_decimals'] = maker_dec
            trade1['send_token_contract_address'] = maker_asset_addr
            trade1['send_value'] = float(maker_asset_filled_amount / 10 ** maker_dec)
            trade1['receive_address'] = maker_address
            trade1['receive_token'] = taker_symbol
            trade1['receive_decimals'] = taker_dec
            trade1['receive_token_contract_address'] = taker_asset_addr
            trade1['receive_value'] = float(taker_asset_filled_amount / 10 ** taker_dec)
            
            trade2 = {key : item[key] for key in ['hash', 'block_timestamp']}
            trade2['method_call'] = item['input'][:10]
            trade2['contract_address'] = item['to_address']
            trade2['dex'] = '0x_protocol'
            trade2['receive_address'] = taker_address
            trade2['receive_token'] = maker_symbol
            trade2['receive_decimals'] = maker_dec
            trade2['receive_token_contract_address'] = maker_asset_addr
            trade2['receive_value'] = float(maker_asset_filled_amount / 10 ** maker_dec)
            trade2['send_address'] = taker_address
            trade2['send_token'] = taker_symbol
            trade2['send_decimals'] = taker_dec
            trade2['send_token_contract_address'] = taker_asset_addr
            trade2['send_value'] = float(taker_asset_filled_amount / 10 ** taker_dec)
            trades += [trade1, trade2]

        elif log['topics'][0].hex() == ZEROEX_V4_EVT_LIMITORDERFILLED:
            data = log['data'][2:]
            i = 1
            maker_address = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 2
            taker_address = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 4
            maker_asset_addr = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 5
            taker_asset_addr = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 6
            taker_asset_filled_amount = int('0x' + data[i * 64 : i * 64 + 64], 0)
            i = 7
            maker_asset_filled_amount = int('0x' + data[i * 64 : i * 64 + 64], 0)

            rst, (maker_symbol, maker_dec) = parse_token(Web3.toChecksumAddress(maker_asset_addr), w3)
            if not rst: return None
            rst, (taker_symbol, taker_dec) = parse_token(Web3.toChecksumAddress(taker_asset_addr), w3)
            if not rst: return None
            
            trade1 = {key : item[key] for key in ['hash', 'block_timestamp']}
            trade1['method_call'] = item['input'][:10]
            trade1['contract_address'] = item['to_address']
            trade1['dex'] = '0x_protocol'
            trade1['send_address'] = maker_address
            trade1['send_token'] = maker_symbol
            trade1['send_decimals'] = maker_dec
            trade1['send_token_contract_address'] = maker_asset_addr
            trade1['send_value'] = float(maker_asset_filled_amount / 10 ** maker_dec)
            trade1['receive_address'] = maker_address
            trade1['receive_token'] = taker_symbol
            trade1['receive_decimals'] = taker_dec
            trade1['receive_token_contract_address'] = taker_asset_addr
            trade1['receive_value'] = float(taker_asset_filled_amount / 10 ** taker_dec)
            
            trade2 = {key : item[key] for key in ['hash', 'block_timestamp']}
            trade2['method_call'] = item['input'][:10]
            trade2['contract_address'] = item['to_address']
            trade2['dex'] = '0x_protocol'
            trade2['receive_address'] = taker_address
            trade2['receive_token'] = maker_symbol
            trade2['receive_decimals'] = maker_dec
            trade2['receive_token_contract_address'] = maker_asset_addr
            trade2['receive_value'] = float(maker_asset_filled_amount / 10 ** maker_dec)
            trade2['send_address'] = taker_address
            trade2['send_token'] = taker_symbol
            trade2['send_decimals'] = taker_dec
            trade2['send_token_contract_address'] = taker_asset_addr
            trade2['send_value'] = float(taker_asset_filled_amount / 10 ** taker_dec)
            trades += [trade1, trade2]
            
        elif log['topics'][0].hex() == ZEROEX_V4_EVT_RFQORDERFILLED:
            data = log['data'][2:]
            i = 1
            maker_address = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 2
            taker_address = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 3
            maker_asset_addr = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 4
            taker_asset_addr = '0x' + data[i * 64 + 24 : i * 64 + 64]
            i = 5
            taker_asset_filled_amount = int('0x' + data[i * 64 : i * 64 + 64], 0)
            i = 6
            maker_asset_filled_amount = int('0x' + data[i * 64 : i * 64 + 64], 0)

            rst, (maker_symbol, maker_dec) = parse_token(Web3.toChecksumAddress(maker_asset_addr), w3)
            if not rst: return None
            rst, (taker_symbol, taker_dec) = parse_token(Web3.toChecksumAddress(taker_asset_addr), w3)
            if not rst: return None
            
            trade1 = {key : item[key] for key in ['hash', 'block_timestamp']}
            trade1['method_call'] = item['input'][:10]
            trade1['contract_address'] = item['to_address']
            trade1['dex'] = '0x_protocol'
            trade1['send_address'] = maker_address
            trade1['send_token'] = maker_symbol
            trade1['send_decimals'] = maker_dec
            trade1['send_token_contract_address'] = maker_asset_addr
            trade1['send_value'] = float(maker_asset_filled_amount / 10 ** maker_dec)
            trade1['receive_address'] = maker_address
            trade1['receive_token'] = taker_symbol
            trade1['receive_decimals'] = taker_dec
            trade1['receive_token_contract_address'] = taker_asset_addr
            trade1['receive_value'] = float(taker_asset_filled_amount / 10 ** taker_dec)
            
            trade2 = {key : item[key] for key in ['hash', 'block_timestamp']}
            trade2['method_call'] = item['input'][:10]
            trade2['contract_address'] = item['to_address']
            trade2['dex'] = '0x_protocol'
            trade2['receive_address'] = taker_address
            trade2['receive_token'] = maker_symbol
            trade2['receive_decimals'] = maker_dec
            trade2['receive_token_contract_address'] = maker_asset_addr
            trade2['receive_value'] = float(maker_asset_filled_amount / 10 ** maker_dec)
            trade2['send_address'] = taker_address
            trade2['send_token'] = taker_symbol
            trade2['send_decimals'] = taker_dec
            trade2['send_token_contract_address'] = taker_asset_addr
            trade2['send_value'] = float(taker_asset_filled_amount / 10 ** taker_dec)
            trades += [trade1, trade2]
    
    return trades