import json
import sqlite3


def extract():
    db = sqlite3.connect('./data.db')

    db.row_factory = sqlite3.Row

    order_book = {
        'exchange': 'Mango Markets',
        'market': 'BTC-PERP',
        'bids': {},
        'asks': {}
    }

    for entry in db.execute("""
        select local_timestamp,
               json_extract(json(message), '$.type')      as type,
               json_extract(json(message), '$.market')    as market,
               json_extract(json(message), '$.timestamp') as "timestamp",
               json_extract(json(message), '$.bids')      as bids,
               json_extract(json(message), '$.asks')      as asks,
               json_extract(json(message), '$.slot')      as slot,
               json_extract(json(message), '$.version')   as version
        from entries
        where type in ('l2snapshot', 'l2update') and market = 'SOL/USDC'
        order by local_timestamp
    """):
        if entry['type'] == 'l2snapshot':
            order_book['bids'] = {}
            order_book['asks'] = {}

        for side in ['bids', 'asks']:
            for price, quantity in json.loads(entry[side]):
                price, quantity = [float(price), float(quantity)]

                if quantity == 0:
                    del order_book[side][price]
                else:
                    order_book[side][price] = quantity

        order_book['timestamp'] = entry['timestamp']

        snapshot = {
            'exchange': order_book['exchange'],
            'market': order_book['market'],
            'timestamp': order_book['timestamp'],
            'bids': list(map(list, sorted(order_book['bids'].items(), reverse=True))),
            'asks': list(map(list, sorted(order_book['asks'].items())))
        }

        yield snapshot


if __name__ == '__main__':
    for order_book in extract():
        print(order_book)
