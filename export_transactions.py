import os
from ethereumetl.cli import export_blocks_and_transactions, extract_csv_column, export_receipts_and_logs 

block = 12728085

export_blocks_and_transactions.callback(
                start_block=block,
                end_block=block,
                batch_size=100,
                provider_uri='http://121.199.22.236:6666',
                max_workers=10,
                blocks_output=os.path.join('.', "blocks.csv"),
                transactions_output=os.path.join('.', "transactions.csv"),
            )

extract_csv_column.callback(
    input='transactions.csv',
    column='hash',
    output='transaction_hashes.txt'
)

export_receipts_and_logs.callback(
    transaction_hashes='transaction_hashes.txt',
    provider_uri='http://121.199.22.236:6666',
    receipts_output='receipts.csv',
    batch_size=100,
    max_workers=10,
    logs_output='logs.csv'
)