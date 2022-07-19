import asyncio
import json

import psycopg2
import websockets
import sqlite3
import os
from pathlib import Path
from datetime import datetime, timezone, date


async def main():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute('create schema if not exists native')

    cur.execute("""
        create table if not exists native.orderbooks (
            exchange text,
            symbol text,
            message text,
            local_timestamp timestamptz
        )
    """)

    cur.execute('create index if not exists local_timestamp_idx on native.orderbooks (local_timestamp)')

    cur.execute('create index concurrently if not exists idx on native.orderbooks (exchange, symbol)')

    conn.commit()

    async for websocket in websockets.connect('ws://mangolorians.com:8900/v1/ws'):
        try:
            await websocket.send(json.dumps({
                'op': 'subscribe',
                'channel': 'level3',
                'markets': [
                    'MNGO/USDC',
                    'BTC/USDC',
                    'ETH/USDC',
                    'SOL/USDC',
                    'USDT/USDC',
                    'SRM/USDC',
                    'RAY/USDC',
                    'COPE/USDC',
                    'FTT/USDC',
                    'MSOL/USDC',
                    'BNB/USDC',
                    'AVAX/USDC',
                    'GMT/USDC'
                ]
            }))

            async for raw_message in websocket:
                message = json.loads(raw_message)

                if message['type'] not in ['l3snapshot', 'open', 'fill', 'change', 'done']:
                    print(message)

                    continue

                local_timestamp = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')

                cur.execute(
                    'insert into native.orderbooks values (%s, %s, %s, %s)',
                    [
                        'Mango Markets spot',
                        message['market'],
                        raw_message,
                        local_timestamp
                    ]
                )

                conn.commit()
        except websockets.WebSocketException:
            continue

if __name__ == '__main__':
    asyncio.run(main())