import asyncio
import json
from datetime import datetime, timezone

import psycopg2
import websockets


async def main():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

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