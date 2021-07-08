from tablestore import *
import json
from exporter.schemas import schemas


def parse_table_schema(schema):
    pkeys = []
    columns = []
    for field in schema['pkeys']:
        pkeys.append((field['name'], field['type']))
        
    for field in schema['columns']:
        columns.append((field['name'], field['type']))
    return pkeys, columns

class OTSExpoter:
    def __init__(self, end_point, access_key_id, access_key_secret, instance_name):
        self.producer = OTSClient(end_point, access_key_id, access_key_secret, instance_name)
        self.name = 'ots'
        self.prepare_tables()
    
    
    def create_table(self, table_name, schema_of_primary_key, defined_columns):
        table_meta = TableMeta(table_name, schema_of_primary_key, defined_columns)
        table_option = TableOptions(-1, 1)
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        self.producer.create_table(table_meta, table_option, reserved_throughput)
    

    def prepare_tables(self):
        self.ots_schemas = {}
        tables = self.producer.list_table()
        for topic in schemas.keys():
            pkeys, columns = parse_table_schema(schemas[topic])
            table_name = 'graph_' + topic
            if not table_name in tables:
                self.create_table(table_name, pkeys, columns)
            
            pkey_vals = {}
            for v in pkeys:
                pkey_vals[v[0]] = v[1]
            column_vals = {}
            for v in columns:
                column_vals[v[0]] = v[1]
            self.ots_schemas[topic] = (pkey_vals, column_vals)

    def put_row(self, table_name, primary_key, attribute_columns):
        try:
            row = Row(primary_key, attribute_columns)
            condition = Condition(RowExistenceExpectation.IGNORE)
            consumed, return_row = self.producer.put_row(table_name, row, condition)
            return None
        except tablestore.error.OTSServiceError as e:
            return e
    

    def batch_write_row(self, table_name, batch_row_data):
        for row_data in batch_row_data:
            put_row_items = []
            for i in range(len(row_data)):
                primary_key, attribute_columns = row_data[i]
                row = Row(primary_key, attribute_columns)
                condition = Condition(RowExistenceExpectation.IGNORE)
                item = PutRowItem(row, condition)
                put_row_items.append(item)

            request = BatchWriteRowRequest()
            request.add(TableInBatchWriteRowItem(table_name, put_row_items))
            result = self.producer.batch_write_row(request)
            

    def dump(self, filtered_item, topic):
        pkeys, columns = parse_table_schema(schemas[topic])
        table_name = 'graph_' + topic
        pkey_vals, column_vals = self.ots_schemas[topic]

        primary_key = []
        attribute_columns = []
        for key in filtered_item.keys():
            if key in pkey_vals:
                primary_key.append((key, filtered_item[key]))
            elif key in column_vals:
                attribute_columns.append((key, filtered_item[key]))
        
        self.put_row(table_name, primary_key, attribute_columns)



