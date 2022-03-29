import asyncio
import json
import logging
import sys
import sqlite3
from datetime import datetime, timezone

import websockets

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler('./app.log')]
)


async def extract():
    async for connection in websockets.connect('wss://api.mango-bowl.com/v1/ws'):
        try:
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
                'MNGO-PERP',
                'RAY-PERP',
                'SRM-PERP'
            ]

            await connection.send(json.dumps({
                'op': 'subscribe',
                'channel': 'level2',
                'markets': markets
            }))

            await connection.send(json.dumps({
                'op': 'subscribe',
                'channel': 'trades',
                'markets': markets
            }))

            async for response in connection:
                yield json.loads(response)
        except websockets.WebSocketException:
            continue


async def main():
    db = sqlite3.connect('./data.db')

    db.execute("""
        create table if not exists entries (
             local_timestamp text,
             message text
        );
    """)

    db.execute('create index if not exists local_timestamp on entries (local_timestamp);')

    db.commit()

    async for message in extract():
        entry = {
            'local_timestamp': datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
            'message': json.dumps(message)
        }

        db.execute('insert into entries (local_timestamp, message) values (:local_timestamp, :message)', entry)

        db.commit()


asyncio.run(main())