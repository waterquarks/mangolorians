import sqlite3
import json
import os
import logging

def extract():
    # From when I naively dumped all order book snapshots into a folder and
    # then crunched numbers over them. Got I/O bound - leaving this here as
    # a reminder and for future reference.
    for root, subdirs, file_paths in os.walk(os.path.abspath('./snapshots')):

        for file_path in file_paths:

            absolute_file_path = os.path.join(root, file_path)

            if not absolute_file_path.endswith('.json'):
                continue

            with open(absolute_file_path) as file:
                yield json.load(file)


def transform(order_book):
    def liquidity(order):
        return order[0] * order[1]

    return {
        'exchange': 'Mango Markets',
        'symbol': order_book['symbol'],
        'buy': sum(map(liquidity, order_book['bids'])),
        'sell': sum(map(liquidity, order_book['asks'])),
        'timestamp': order_book['timestamp']
    }


def load(entry):
    query = """
        insert into liquidity(exchange, symbol, buy, sell, timestamp) values (:exchange, :symbol, :buy, :sell, :timestamp)
    """

    try:
        db = sqlite3.connect('dev.db')

        db.execute(query, entry)

        db.commit()
    except sqlite3.DatabaseError as error:
        logging.error(f"{error}: {query} | {entry}")
