import asyncio
import json
from datetime import datetime, timezone

import psycopg2
import websockets


async def main():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute('create table if not exists native_serum_trades (message jsonb, local_timestamp timestamptz)')

    conn.commit()

    async for websocket in websockets.connect('ws://mangolorians.com:8900/v1/ws'):
        try:
            await websocket.send(json.dumps({
                'op': 'subscribe',
                'channel': 'trades',
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

            async for message in websocket:
                local_timestamp = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')

                cur.execute(
                    'insert into native_serum_trades values (%s, %s)',
                    [
                        message,
                        local_timestamp
                    ]
                )

                conn.commit()
        except websockets.WebSocketException:
            continue


if __name__ == '__main__':
    asyncio.run(main())