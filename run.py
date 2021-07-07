from pyapollo.apollo_client import ApolloClient
from ethereumetl.cli import export_blocks_and_transactions
from main_processor import MainProcessor
import argparse

apollo_client = ApolloClient(app_id='ethereum_etl', config_server_url='http://172.16.1.191:8080')

PROVIDER_URL = apollo_client.get_value('provider_uri', 'http://')
KAFKA_SERVER = apollo_client.get_value('kafka_server', 'http://')


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--start', type=int, required=False, default=10000000)
parser.add_argument('--end', type=int, required=False, default=10000001)
parser.add_argument('--dest', type=str, required=False, default='print')
args = parser.parse_args()

if args.end == 10000001 and args.start != 10000000:
    args.end = args.start + 1


if __name__ == '__main__':
    export_blocks_and_transactions.callback(
                    start_block=args.start,
                    end_block=args.end,
                    batch_size=100,
                    provider_uri=PROVIDER_URL,
                    max_workers=10,
                    blocks_output='./blocks.csv',
                    transactions_output='./transactions.csv',
                )

    if args.dest == 'kafka':
        from exporter.kafka_exporter import KafkaExpoter
        exporter = KafkaExpoter(KAFKA_SERVER)
    else:
        from exporter.printer import Printer
        exporter = Printer()

    processor = MainProcessor(
        provider_url=PROVIDER_URL,
        max_workers=5,
        exporter=exporter)

    processor.load_transactions('./transactions.csv')
    processor.execute()
    processor.shutdown()

