import csv
import os
from exporter.schemas import schemas

class CSVExporter:
    def __init__(self, path=None, start=None, end=None):
        print("csv exporter")
        self.name = 'csv'
        self.path = path
        self.start = start
        self.end = end
        self.files = {}
        self.writers = {}
        self.columns  = {}

    def dump(self, filtered_item, topic):
        # print(topic)
        if not topic in self.files.keys():
            if self.start is not None and self.end is not None and self.path is not None:
                file_name = os.path.join(self.path, f'{self.start}_{self.end}.csv')
            else:
                topic + '.csv'
            self.files[topic] = open(file_name, 'w', newline='')
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