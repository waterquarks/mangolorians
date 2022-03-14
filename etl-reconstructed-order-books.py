import json
import os
import sqlite3
from datetime import datetime
from time import time
from multiprocessing import Pool


# Dumps all order book snapshots into JSON files.
# I ran out of time to figure out how to make this not take *that* long.

def extract(symbol, timestamp=int(time() * 1e6)):
    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    query = """
        select
            exchange,
            symbol,
            timestamp,
            local_timestamp,
            is_snapshot,
            side,
            price,
            amount,
            coalesce(local_timestamp != lead(local_timestamp) over (order by local_timestamp), false) as is_save_point
        from order_book
        where symbol = ? and "timestamp" <= ?
        order by local_timestamp
    """

    order_book = {
        'symbol': symbol,
        'timestamp': datetime.utcfromtimestamp(timestamp / 1e6).isoformat(),
        'bids': {},
        'asks': {}
    }

    previous = None

    for current in db.execute(query, (symbol, timestamp)).fetchall():
        if previous is not None:
            if current["is_snapshot"] and not previous["is_snapshot"]:
                order_book['bids'] = {}
                order_book['asks'] = {}

        side = {
            'bid': 'bids',
            'ask': 'asks'
        }[current['side']]

        if current['amount'] == 0:
            del order_book[side][current['price']]
        else:
            order_book[side][current['price']] = current['amount']

        order_book['timestamp'] = datetime.utcfromtimestamp(current['local_timestamp'] / 1e6).isoformat()

        if current['is_save_point']:
            yield order_book

        previous = current


def transform(order_books):
    for order_book in order_books:
        yield {
            'symbol': order_book['symbol'],
            'timestamp': order_book['timestamp'],
            'bids': [list(order) for order in sorted(list(order_book['bids'].items()), reverse=True)],
            'asks': [list(order) for order in sorted(list(order_book['asks'].items()))]
        }


def load(record):
    file_path = f"./snapshots/{record['symbol']}/{record['timestamp']}.json"

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'w') as file:
        json.dump(record, file, indent=2)

markets = [
    'BTC-PERP',
    'SOL-PERP',
    'MNGO-PERP',
    'ADA-PERP',
    'AVAX-PERP',
    'BNB-PERP',
    'ETH-PERP',
    'FTT-PERP',
    'LUNA-PERP',
    'RAY-PERP',
    'SRM-PERP'
]

if __name__ == '__main__':
    pool = Pool(processes=4)

    for market in markets:
        pool.map(load, transform(extract(market)))