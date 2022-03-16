import sqlite3
from datetime import datetime
from time import time


def reconstruct_order_book(symbol, timestamp=int(time() * 1e6), depth=None):
    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    cur = db.cursor()

    cur.execute(
        """select * from order_book where symbol = ? and "timestamp" <= ? order by local_timestamp""",
        (symbol, timestamp)
    )

    order_book = {
        'exchange': 'Mango Markets',
        'symbol': symbol,
        'timestamp': datetime.utcfromtimestamp(timestamp / 1e6).isoformat(),
        'bids': {},
        'asks': {}
    }

    previous = None

    for current in cur.fetchall():
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

        previous = current

    order_book['bids'] = [list(order) for order in sorted(list(order_book['bids'].items()), reverse=True)]

    order_book['asks'] = [list(order) for order in sorted(list(order_book['asks'].items()))]

    if depth is not None:
        order_book['bids'] = order_book['bids'][:depth]

        order_book['asks'] = order_book['asks'][:depth]

    return order_book
