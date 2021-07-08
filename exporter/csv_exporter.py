import csv
from exporter.schemas import schemas

class CSVExporter:
    def __init__(self):
        print("csv exporter")
        self.name = 'csv'
        self.files = {}
        self.writers = {}
        self.columns  = {}

    def dump(self, filtered_item, topic):
        # print(topic)
        if not topic in self.files.keys():
            self.files[topic] = open(topic + '.csv', 'w', newline='')
            self.writers[topic] = csv.writer(self.files[topic], delimiter=',')
            self.columns[topic] = self.parse_columns(topic)
            self.writers[topic].writerow(self.columns[topic])
        
        # print(self.columns[topic])
        row = []
        for column in self.columns[topic]:
            row.append(filtered_item[column])
        self.writers[topic].writerow(row)

    def parse_columns(self, topic):
        schema = schemas[topic]
        # print(topic, schema)
        return [c['name'] for c in list(schema['pkeys']) + list(schema['columns'])]