import json
import sqlite3
import time
from time import time
import asyncio

import websockets


async def extract():
    async for connection in websockets.connect('wss://api.mango-bowl.com/v1/ws'):
        try:
            message = {
                'op': 'subscribe',
                'channel': 'trades',
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


def transform(data):
    if data['type'] == 'trade':
        yield {
            'exchange': 'Mango Markets',
            'symbol': data['market'],
            'timestamp': data['timestamp'],
            'local_timestamp': int(time() * 1e6),
            'taker': data['takerAccount'],
            'taker_order': data['makerOrderId'],
            'taker_client_order_id': data['takerClientId'],
            'maker': data['makerAccount'],
            'maker_order': data['makerOrderId'],
            'maker_client_order_id': data['makerClientId'],
            'side': data['side'],
            'price': data['price'],
            'amount': data['size'],
            'taker_fee': data['takerFeeCost'],
            'maker_fee': data['makerFeeCost']
        }


def load(trades):
    for trade in trades:
        db = sqlite3.connect('dev.db')

        db.execute("""
            insert into trades values (
                :exchange,
                :symbol,
                :timestamp,
                :local_timestamp,
                :taker,
                :taker_order,
                :taker_client_order_id,
                :maker,
                :maker_order,
                :maker_client_order_id,
                :side,
                :price,
                :amount,
                :taker_fee,
                :maker_fee
            )
        """, trade)

        db.commit()

async def main():
    async for batch in extract():
        load(transform(batch))

asyncio.run(main())
