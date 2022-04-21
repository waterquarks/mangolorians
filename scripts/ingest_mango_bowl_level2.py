import psycopg2
import websockets
import json
import asyncio
from datetime import datetime, timezone


async def main():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute('create schema if not exists mango_bowl')

    cur.execute("""
        create table if not exists mango_bowl.level2_v0 (
            market text,
            content json,
            local_timestamp timestamptz
        )
    """)

    cur.execute('create index if not exists level2_v0_market_idx on mango_bowl.level2_v0 (market, local_timestamp)')

    conn.commit()

    async for connection in websockets.connect('ws://mangolorians.com:8010/v1/ws'):
        try:
            message = {
                'op': 'subscribe',
                'channel': 'level2',
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

                if content['type'] not in {'l2snapshot', 'l2update'}:
                    continue

                market = content.pop('market')

                cur.execute(
                    'insert into mango_bowl.level2_v0 values (%s, %s, %s)',
                    (market, json.dumps(content), datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'))
                )

                print(cur.query)

                conn.commit()
        except websockets.WebSocketException:
            continue

if __name__ == '__main__':
    asyncio.run(main())