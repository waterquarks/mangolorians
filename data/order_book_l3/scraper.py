import websockets
import asyncio
import json
import psycopg2
from datetime import datetime, timezone


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
                'channel': 'level3',
                'markets': markets
            }))

            async for message in connection:
                yield json.loads(message)
        except websockets.WebSocketException:
            continue


async def main():
    db = psycopg2.connect("dbname=mangolorians")

    cur = db.cursor()

    cur.execute('create schema if not exists raw')

    cur.execute("""
        create table if not exists raw.order_book_L3 (
             local_timestamp text,
             message text
        );
    """)

    cur.execute('create index if not exists local_timestamp on raw.order_book_L3 (local_timestamp)')

    db.commit()

    async for message in extract():
        entry = {
            'local_timestamp': datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
            'message': json.dumps(message)
        }

        cur = db.cursor()

        cur.execute('insert into raw.order_book_L3 (local_timestamp, message) values (%(local_timestamp)s, %(message)s)', entry)

        db.commit()


if __name__ == '__main__':
    asyncio.run(main())
