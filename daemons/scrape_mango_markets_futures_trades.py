import asyncio
import json
from datetime import datetime, timezone

import psycopg2
import websockets


async def main():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    async for websocket in websockets.connect('ws://mangolorians.com:8010/v1/ws'):
        try:
            await websocket.send(json.dumps({
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
                    'MNGO-PERP',
                    'RAY-PERP',
                    'SRM-PERP'
                ]
            }))

            async for raw_message in websocket:
                message = json.loads(raw_message)

                if message['type'] not in ['recent_trades', 'trade']:
                    print(message)

                    continue

                local_timestamp = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')

                cur.execute(
                    'insert into native.trades values (%s, %s, %s, %s)',
                    [
                        'Mango Markets',
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