import pprint as pp

class Printer:
    def __init__(self):
        self.name = 'printer'

    def dump(self, filtered_item, topic):
        if topic == 'eth_transfer':
            assert(len(filtered_item.keys()) == 8)
        if topic == 'dex_trade':
            assert(len(filtered_item.keys()) == 15)
        try:
            pp.pprint(filtered_item)
        except UnicodeEncodeError as e:
            if topic == 'dex_trade':
                filtered_item['receive_token'] = filtered_item['receive_token'].encode("utf-8")
                filtered_item['send_token'] = filtered_item['send_token'].encode("utf-8")
            elif topic == 'eth_transfer':
                filtered_item['symbol'] = filtered_item['symbol'].encode("utf-8")
            pp.pprint(filtered_item)
        print()