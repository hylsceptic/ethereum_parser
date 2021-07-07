transfer_columns = {
    'hash' : str, 
    'block_timestamp' : int,
    'from_address' : str,
    'to_address' : str,
    'contract_address' : str,
    'symbol' : str,
    'decimals' : int,
    'value' : float,
}

trade_columns = {
    'hash' : str,
    'block_timestamp' : int,
    'dex' : str,
    'method_call' : str,
    'contract_address' : str,
    'send_token' : str,
    'send_decimals' : int,
    'send_address' : str,
    'send_token_contract_address' : str,
    'send_value' : float,
    'receive_address' : str,
    'receive_token' : str,
    'receive_decimals' : int,
    'receive_token_contract_address' : str,
    'receive_value' : float,
}

def check_result(item, topic):
    if 'eth_transfer' in topic:
        columns = transfer_columns
    elif 'dex_trade' in topic:
        columns = trade_columns
    else:
        raise Exception("[error] topic error.")
    
    for key in columns.keys():
        if not key in item.keys():
            print("topic: ", topic, "Tx Hash:", item['hash'])
            print(item)
            raise Exception(f"[error] {key} Does not exist.")
        if type(item[key]) != columns[key]:
            print("topic: ", topic, "Tx Hash:", item['hash'])
            print(item)
            raise Exception(f"[error] {key} with type {type(item[key])} is not {columns[key]}.")