import websockets
import json
import asyncio
from datetime import datetime, timezone
import psycopg2


async def main():
    db = psycopg2.connect('dbname=mangolorians')

    cur = db.cursor()

    cur.execute('create schema if not exists mango_bowl')

    cur.execute('create table if not exists mango_bowl.level3 (content jsonb, local_timestamp timestamptz)')

    cur.execute('create index if not exists level3_idx_0 on mango_bowl.level3 (local_timestamp)')

    # cur.execute("create index if not exists level3_idx_1 on mango_bowl.level3 ((content->>'market'), ((content->>'timestamp')::timestamptz))")

    # cur.execute("create index if not exists level3_idx_2 on mango_bowl.level3 ((content->>'market'), ((content->>'slot')::integer))")

    db.commit()

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

                cur.execute(
                    "insert into mango_bowl.level3 values (%s, %s)",
                    (json.dumps(content), datetime.now(timezone.utc))
                )

                print(cur.query.decode('utf-8'))

                db.commit()
        except websockets.WebSocketException:
            continue

if __name__ == '__main__':
    asyncio.run(main())