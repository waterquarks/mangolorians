import csv
import json
import os
import time
from threading import Event, Thread
from time import time
import sqlite3
import asyncio
import websockets

import mango
from mango.context import Context
from mango.perpeventqueue import PerpFillEvent
from mango.perpmarket import PerpMarket

from lib.extended_encoder import ExtendedEncoder

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
    local_timestamp = int(time() * 1e6)

    if data['type'] == 'trade':
        trade = {
            'exchange': 'Mango Markets',
            'symbol': data['market'],
            'timestamp': int(time() * 1e6),
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

        print(trade)



async def main():
    async for batch in extract():
        transform(batch)

asyncio.run(main())



