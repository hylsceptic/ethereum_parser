transfer_schema = {
    "pkeys" :
    [
        {
            "name": "hash",
            "type": "STRING",
        }
    ],
    "columns" :
    [
        {
            "name": "block_timestamp",
            "type": "INTEGER",
        },
        {
            "name": "from_address",
            "type": "STRING",
        },
        {
            "name": "to_address",
            "type": "STRING",
        },
        {
            "name": "contract_address",
            "type": "STRING",
        },
        {
            "name": "symbol",
            "type": "STRING",
        },
        {
            "name": "decimals",
            "type": "INTEGER",
        },
        {
            "name": "value",
            "type": "DOUBLE",
        }
    ]
}


trade_schema = {
    "pkeys" :
    [
        {
            "name": "hash",
            "type": "STRING",
        }
    ],
    "columns" :
    [
        {
            "name" : "block_timestamp",
            "type" : "INTEGER",
        },
        {
            "name" : "dex",
            "type" : "STRING",
        },
        {
            "name" : "method_call",
            "type" : "STRING",
        },
        {
            "name" : "contract_address",
            "type" : "STRING",
        },
        {
            "name" : "send_token",
            "type" : "STRING",
        },
        {
            "name" : "send_decimals",
            "type" : "INTEGER",
        },
        {
            "name" : "send_address",
            "type" : "STRING",
        },
        {
            "name" : "send_token_contract_address",
            "type" : "STRING",
        },
        {
            "name" : "send_value",
            "type" : "DOUBLE",
        },
        {
            "name" : "receive_address",
            "type" : "STRING",
        },
        {
            "name" : "receive_token",
            "type" : "STRING",
        },
        {
            "name" : "receive_decimals",
            "type" : "INTEGER",
        },
        {
            "name" : "receive_token_contract_address",
            "type" : "STRING",
        },
        {
            "name" : "receive_value",
            "type" : "DOUBLE",
        }
    ]
}

schemas = {
    'eth_transfer' : transfer_schema,
    'dex_trade' : trade_schema
}