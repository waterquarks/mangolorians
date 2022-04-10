import psycopg2
import psycopg2.extras
import websockets
import json
import asyncio
import sqlite3
from datetime import datetime, timezone


async def main():
    db = sqlite3.connect('./order_book_l3.db')

    db.execute('create table if not exists entries (local_timestamp text, message text)')

    db.execute('create index if not exists idx on entries (local_timestamp)')

    db.commit()

    async for connection in websockets.connect('wss://api.mango-bowl.com/v1/ws'):
        try:
            await connection.send(json.dumps({
                'op': 'subscribe',
                'channel': 'level3',
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
            }))

            async for message in connection:
                entry = {
                    'local_timestamp': datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
                    'message': message
                }

                db.execute('insert into entries values (:local_timestamp, :message)', entry)

                db.commit()
        except websockets.WebSocketException:
            continue


if __name__ == '__main__':
    asyncio.run(main())