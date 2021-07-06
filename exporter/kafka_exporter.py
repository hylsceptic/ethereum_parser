from kafka import KafkaProducer
import json

class KafkaExpoter:
    def __init__(self, kafka_server):
        self.producer = KafkaProducer(bootstrap_servers=kafka_server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.name = 'kafka'
    
    def dump(self, filtered_item, topic):
        future = self.producer.send(topic, filtered_item)
        self.producer.flush()
