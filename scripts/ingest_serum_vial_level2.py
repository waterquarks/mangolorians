import psycopg2
import websockets
import asyncio
import json
from datetime import datetime, timezone


async def main():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute('create schema if not exists serum_vial')

    cur.execute('create table if not exists serum_vial.level2 (market text, "type" text, "content" json, local_timestamp timestamptz)')

    async for connection in websockets.connect('wss://api.serum-vial.dev/v1/ws'):
        try:
            message = {
                'op': 'subscribe',
                'channel': 'level2',
                'markets': [
                    'AVAX/USDC',
                    'BNB/USDC',
                    'BTC/USDC',
                    'ETH/USDC',
                    'FTT/USDC',
                    'MNGO/USDC',
                    'RAY/USDC',
                    'LUNA/USDC',
                    'SRM/USDC',
                    'SOL/USDC',
                    'MSOL/USDC',
                    'USDT/USDC',
                    'COPE/USDC',
                ]
            }

            await connection.send(json.dumps(message))

            async for response in connection:
                entry = json.loads(response)

                if entry['type'] not in {'l2snapshot', 'l2update'}:
                    continue

                cur.execute(
                    'insert into serum_vial.level2 values (%s, %s, %s, %s)',
                    [
                        entry['market'],
                        entry['type'],
                        json.dumps({key: entry[key] for key in entry if key not in ['market', 'type']}),
                        datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                    ]
                )

                print(cur.query.decode('utf-8'))

                conn.commit()
        except websockets.WebSocketException:
            continue

asyncio.run(main())