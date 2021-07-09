from pyapollo.apollo_client import ApolloClient
from ethereumetl.cli import export_blocks_and_transactions
from main_processor import MainProcessor
import argparse

def run_once(start, end, dest, exporter):
    print(f"process from {start} to {end}.")

    export_blocks_and_transactions.callback(
                    start_block=start,
                    end_block=end,
                    batch_size=20,
                    provider_uri=PROVIDER_URL,
                    max_workers=5,
                    blocks_output='./blocks.csv',
                    transactions_output='./transactions.csv',
                )

    processor = MainProcessor(
        provider_url=PROVIDER_URL,
        max_workers=5,
        exporter=exporter)

    processor.load_transactions('./transactions.csv')
    processor.execute()
    processor.shutdown()

## 12768060 12768070

if __name__ == '__main__':
    apollo_client = ApolloClient(app_id='ethereum_etl', config_server_url='http://172.16.1.191:8080')

    PROVIDER_URL = apollo_client.get_value('provider_uri', 'http://')
    KAFKA_SERVER = apollo_client.get_value('kafka_server', 'http://')

    import time
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--start', type=int, required=False, default=10000000)
    parser.add_argument('--end', type=int, required=False, default=10000001)
    parser.add_argument('--dest', type=str, required=False, default='print')
    args = parser.parse_args()

    if args.dest == 'kafka':
        print("Written to kafka......")
        from exporter.kafka_exporter import KafkaExpoter
        exporter = KafkaExpoter(KAFKA_SERVER)
    elif args.dest == 'csv':
        from exporter.csv_exporter import CSVExporter
        exporter = CSVExporter()
    elif args.dest == 'ots':
        from exporter.ots_exporter import OTSExpoter
        end_point = apollo_client.get_value('end_point', '0')
        access_key_id = apollo_client.get_value('access_key_id', '0')
        access_key_secret = apollo_client.get_value('access_key_secret', '0')
        instance_name = apollo_client.get_value('instance_name', '0')
        exporter = OTSExpoter(end_point, access_key_id, access_key_secret, instance_name)
    elif args.dest == 'prt':
        print("Printing the result......")
        from exporter.printer import Printer
        exporter = Printer()
    else:
        class Exporter:
            def __init__(self):
                self.name = 'None'

            def dump(self, filtered_item, topic):
                pass
        exporter = Exporter()

    if args.end == 10000001 and args.start != 10000000:
        args.end = args.start

    start, end = args.start, args.end
    step = 60
    st = time.time()
    while(start <= end):
        sst = time.time()
        run_once(start, min(start + step - 1, end), args.dest, exporter)
        start += step
        print('Epoch time spent %.2f; Total time spent %.2f' % (time.time() - sst, time.time() - st))