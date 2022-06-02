import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal

import websockets
import sys

from crunch import liquidity, slippage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler('etl.log')]
)


async def extract():
    async for connection in websockets.connect('wss://api.mango-bowl.com/v1/ws'):
        try:
            message = {
                'op': 'subscribe',
                'channel': 'level2',
                'markets': [
                    'BTC-PERP',
                    'SOL-PERP',
                    'MNGO-PERP',
                    'ADA-PERP',
                    'AVAX-PERP',
                    'BNB-PERP',
                    'ETH-PERP',
                    'FTT-PERP',
                    'LUNA-PERP',
                    'MNGO-PERP',
                    'RAY-PERP',
                    'SRM-PERP'
                ]
            }

            await connection.send(json.dumps(message))

            async for response in connection:
                data = json.loads(response)

                if data['type'] in ['l2snapshot', 'l2update']:
                    yield data
        except websockets.WebSocketException:
            continue


async def transform(batches):
    async for batch in batches:
        batch['exchange'] = 'Mango Markets'

        batch['local_timestamp'] = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')

        yield batch


async def load(batches):
    async for batch in batches:
        entries = []

        for side in ['bids', 'asks']:
            if not (side in batch):
                continue

            for price, quantity in batch[side]:
                entry = {
                    'exchange': batch['exchange'],
                    'symbol': batch['market'],
                    'timestamp': batch['timestamp'],
                    'local_timestamp': batch['local_timestamp'],
                    'is_snapshot': batch['type'] == 'l2snapshot',
                    'side': {
                        'bids': 'bid',
                        'asks': 'ask'
                    }.get(side),
                    'price': price,
                    'amount': quantity
                }

                entries.append(entry)
        else:
            query = """
                insert into order_book_deltas values (
                    :exchange,
                    :symbol,
                    :timestamp,
                    :local_timestamp,
                    :is_snapshot,
                    :side,
                    :price,
                    :amount
                )
            """

            try:
                db = sqlite3.connect('dev.db')

                db.executemany(query, entries)

                db.commit()
            except sqlite3.DatabaseError as error:
                logging.error(f"{error}: {query} | {entry}")

            yield batch


async def process(batches):
    order_books = {}

    async for batch in batches:
        if batch['market'] not in order_books:
            order_books[batch['market']] = {
                'exchange': batch['exchange'],
                'symbol': batch['market']
            }

        order_book = order_books[batch['market']]

        if batch['type'] == 'l2snapshot':
            order_book['asks'] = {}
            order_book['bids'] = {}

        for side in ['bids', 'asks']:
            if side not in batch:
                continue

            for price, quantity in batch[side]:
                if Decimal(quantity) == 0:
                    del order_book[side][price]
                else:
                    order_book[side][price] = quantity
        else:
            order_book['timestamp'] = batch['timestamp']

            order_book['local_timestamp'] = batch['local_timestamp']

            snapshot = {
                **order_book,
                'bids': [
                    list(order) for order in sorted(map(lambda order: list(map(float, order)), order_book['bids'].items()), reverse=True)
                ],
                'asks': [
                    list(order) for order in sorted(map(lambda order: list(map(float, order)), order_book['asks'].items()))
                ]
            }

            yield snapshot


async def main():
    async for snapshot in process(load(transform(extract()))):
        liquidity.load(liquidity.transform(snapshot))

        slippage.load(slippage.transform(snapshot))


asyncio.run(main())
