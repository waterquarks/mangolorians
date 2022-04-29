import psycopg2
import websockets
import json
import asyncio
from datetime import datetime, timezone


async def main():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute('create schema if not exists serum_vial')

    cur.execute("""
        create table if not exists serum_vial.level3 (
            market text,
            content json,
            local_timestamp timestamptz
        )
    """)

    cur.execute('create index if not exists level3_market_idx on serum_vial.level3 (market, local_timestamp)')

    conn.commit()

    async for connection in websockets.connect('wss://api.serum-vial.dev/v1/ws'):
        try:
            message = {
                'op': 'subscribe',
                'channel': 'level3',
                'markets': [
                    'SOL/USDC',
                    'BTC/USDC',
                    'SRM/USDC',
                    'MSOL/USDC',
                    'AVAX/USDC',
                    'ETH/USDC',
                    'LUNA/USDC',
                    'FTT/USDC',
                    'RAY/USDC',
                    'USDT/USDC',
                    'MNGO/USDC',
                    'COPE/USDC',
                    'BNB/USDC',
                ]
            }

            await connection.send(json.dumps(message))

            async for response in connection:
                content = json.loads(response)

                if content['type'] not in {'l3snapshot', 'open', 'fill', 'change', 'done'}:
                    continue

                market = content.pop('market')

                cur.execute(
                    'insert into serum_vial.level3 values (%s, %s, %s)',
                    (market, json.dumps(content), datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'))
                )

                print(cur.query)

                conn.commit()
        except websockets.WebSocketException:
            continue

if __name__ == '__main__':
    asyncio.run(main())