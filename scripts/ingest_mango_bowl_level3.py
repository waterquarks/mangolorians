import sqlite3
import websockets
import json
import asyncio
from datetime import datetime, timezone


async def main():
    db = sqlite3.connect('mango_bowl_level3.db')

    db.set_trace_callback(print)

    db.execute('pragma journal_mode=WAL')

    db.execute("""
        create table if not exists entries (
            content text,
            local_timestamp text
        )
    """)

    async for connection in websockets.connect('ws://mangolorians.com:8010/v1/ws'):
        try:
            message = {
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
            }

            await connection.send(json.dumps(message))

            async for response in connection:
                content = json.loads(response)

                if content['type'] not in {'l3snapshot', 'open', 'fill', 'change', 'done'}:
                    continue

                db.execute(
                    'insert into entries values (?, ?)',
                    (json.dumps(content), datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'))
                )

                db.commit()
        except websockets.WebSocketException:
            continue

if __name__ == '__main__':
    asyncio.run(main())