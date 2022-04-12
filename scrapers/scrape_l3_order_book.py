import websockets
import json
import asyncio
import sqlite3
from datetime import datetime, timezone


async def ingestor():
    db = sqlite3.connect('./l3_deltas_raw.db')

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

                yield json.loads(message)
        except websockets.WebSocketException:
            continue


async def main():
    db = sqlite3.connect('./l3_deltas.db')

    db.set_trace_callback(print)

    db.execute('create table if not exists deltas (timestamp text, market text, is_snapshot integer, account text, side text, order_id text, price real, size real)')

    db.execute('create index if not exists xdx on deltas (timestamp, market, account)')

    async for entry in ingestor():
        if entry['type'] not in {'l3snapshot', 'open', 'done'}:
            continue

        if entry['type'] == 'l3snapshot':
            for side in ['bids', 'asks']:
                for order in entry[side]:
                    order = {
                        'account': order['account'],
                        'side': order['side'],
                        'order_id': order['orderId'],
                        'price': order['price'],
                        'size': order['size']
                    }

                    delta = {
                        'timestamp': entry['timestamp'],
                        'market': entry['market'],
                        'is_snapshot': 1,
                        **order
                    }

                    db.execute('insert into deltas values (?, ?, ?, ?, ?, ?, ?, ?)', list(delta.values()))

        if entry['type'] == 'open':
            order = {
                'account': entry['account'],
                'side': entry['side'],
                'order_id': entry['orderId'],
                'price': entry['price'],
                'size': entry['size']
            }

            delta = {
                'timestamp': entry['timestamp'],
                'market': entry['market'],
                'is_snapshot': 0,
                **order
            }

            db.execute('insert into deltas values (?, ?, ?, ?, ?, ?, ?, ?)', list(delta.values()))

        if entry['type'] == 'done':
            order = {
                'account': entry['account'],
                'side': entry['side'],
                'order_id': entry['orderId'],
                'price': 0,
                'size': 0
            }

            delta = {
                'timestamp': entry['timestamp'],
                'market': entry['market'],
                'is_snapshot': 0,
                **order
            }

            db.execute('insert into deltas values (?, ?, ?, ?, ?, ?, ?, ?)', list(delta.values()))

        db.commit()


if __name__ == '__main__':
    asyncio.run(main())