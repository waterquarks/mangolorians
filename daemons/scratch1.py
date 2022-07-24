import asyncio
import json
import psycopg2
from datetime import datetime, timezone

import websockets


async def main():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    trades = ['recent_trades', 'trades']

    level3 = ['l3snapshot', 'open', 'fill', 'change', 'done']

    async for websocket in websockets.connect('ws://mangolorians.com:8010/v1/ws'):
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
                'MNGO-PERP',
                'RAY-PERP',
                'SRM-PERP'
            ]

            await asyncio.gather(*[
                websocket.send(json.dumps({
                    'op': 'subscribe',
                    'channel': 'level3',
                    'markets': markets
                })),
                websocket.send(json.dumps({
                    'op': 'subscribe',
                    'channel': 'trades',
                    'markets': markets
                }))
            ])

            async for raw_message in websocket:
                message = json.loads(raw_message)

                local_timestamp = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')

                if message['type'] in trades:
                    cur.execute(
                        'insert into native.trades values (%s, %s, %s, %s)',
                        [
                            'Mango Markets',
                            message['market'],
                            raw_message,
                            local_timestamp
                        ]
                    )

                if message['type'] in level3:
                    cur.execute(
                        'insert into native.orderbooks values (%s, %s, %s, %s)',
                        [
                            'Mango Markets',
                            message['market'],
                            raw_message,
                            local_timestamp
                        ]
                    )

        except websockets.WebSocketException:
            continue

if __name__ == '__main__':
    asyncio.run(main())