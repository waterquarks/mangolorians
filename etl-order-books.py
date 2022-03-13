import asyncio
import json
import sqlite3
from calendar import timegm
from datetime import datetime
from time import time

import websockets

from crunch import liquidity, slippage


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
                yield json.loads(response)
        except Exception as e:
            print(f'{repr(e)}')

            continue


def transform(batch):
    local_timestamp = int(time() * 1e6)

    for side in ['bids', 'asks']:
        if not (side in batch):
            continue
        for price, quantity in batch[side]:
            yield {
                'exchange': 'Mango Markets',
                'symbol': batch['market'],
                'timestamp': int(timegm(datetime.strptime(batch['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').timetuple()) * 1e6),
                'local_timestamp': local_timestamp,
                'is_snapshot': batch['type'] == 'l2snapshot',
                'side': {
                    'bids': 'bid',
                    'asks': 'ask'
                }.get(side),
                'price': float(price),
                'amount': float(quantity)
            }


def load(entries):
    db = sqlite3.connect('dev.db')

    batch = []

    for entry in entries:
        batch.append(entry)

        yield entry
    else:
        db.executemany(
            """
            insert into order_book values (
                :exchange,
                :symbol,
                :timestamp,
                :local_timestamp,
                :is_snapshot,
                :side,
                :price,
                :amount
            )
            """,
            batch
        )

        db.commit()


async def main():
    async def snapshotter():
        order_books = {}

        trails = {}

        last_modified = None

        async for batch in extract():
            for delta in load(transform(batch)):
                order_book = order_books.setdefault(
                    delta['symbol'],
                    {
                        'exchange': 'Mango Markets',
                        'symbol': delta['symbol'],
                        'timestamp': delta['local_timestamp'],
                        'bids': {},
                        'asks': {}
                    }
                )

                order_book['timestamp'] = delta['local_timestamp']

                trail = trails.get(delta['symbol'])

                if (trail is None) or (delta["is_snapshot"] and not trail["is_snapshot"]):
                    order_book['bids'] = {}
                    order_book['asks'] = {}

                side = {
                    'bid': 'bids',
                    'ask': 'asks'
                }.get(delta['side'])

                if delta['amount'] == 0:
                    del order_book[side][delta['price']]
                else:
                    order_book[side][delta['price']] = delta['amount']

                trails[delta['symbol']] = delta

                last_modified = order_book
            else:
                yield {
                    **last_modified,
                    'bids': [list(order) for order in sorted(list(last_modified['bids'].items()), reverse=True)],
                    'asks': [list(order) for order in sorted(list(last_modified['asks'].items()))]
                }

    async for snapshot in snapshotter():
        liquidity.load(liquidity.transform(snapshot))
        slippage.load(slippage.transform(snapshot))

asyncio.run(main())


