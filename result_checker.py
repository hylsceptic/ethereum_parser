transfer_columns = ['hash', 'block_timestamp', 'from_address', 'symbol', 'to_address', 'value', 'contract_address', 'decimals']

trade_columns = ['hash', 'block_timestamp', 'send_address', 'dex', 'method_call', 'contract_address', 'receive_value', 'send_value', 'receive_address', 'receive_token', 'receive_decimals', 'send_token', 'send_decimals', 'send_token_contract_address', 'receive_token_contract_address']

def check_result(item, topic):
    if 'eth_transfer' in topic:
        columns = transfer_columns
    elif 'dex_trade' in topic:
        columns = trade_columns
    else:
        raise Exception("[error] topic error.")
    
    for column in columns:
        if not column in item.keys():
            print("topic: ", topic, "Tx Hash:", item['hash'])
            print(item.keys())
            raise Exception(f"[error] {column} Does not exist.")