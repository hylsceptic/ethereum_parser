from transfer_trade_exporter import TransferTradeExporter
from pyapollo.apollo_client import ApolloClient
from ethereumetl.cli import export_blocks_and_transactions
import argparse

apollo_client = ApolloClient(app_id='ethereum_etl', config_server_url='http://172.16.1.191:8080')

PROVIDER_URL = apollo_client.get_value('provider_uri', 'http://')
KAFKA_SERVER = apollo_client.get_value('kafka_server', 'http://')


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--start', type=int, required=False, default=10000000)
parser.add_argument('--end', type=int, required=False, default=10000001)
parser.add_argument('--dest', type=str, required=False, default='print')
args = parser.parse_args()


export_blocks_and_transactions.callback(
                start_block=args.start,
                end_block=args.end,
                batch_size=100,
                provider_uri=PROVIDER_URL,
                max_workers=10,
                blocks_output='./blocks.csv',
                transactions_output='./transactions.csv',
            )

def export_kafka():
    exporter = TransferTradeExporter(
        provider_url=PROVIDER_URL,
        max_workers=5,
        kafka_server=KAFKA_SERVER,
        dump_to=args.dest,
        is_test='')
    exporter.load_transactions('./transactions.csv')
    exporter.execute()
    exporter.shutdown()
    

export_kafka()