import sqlite3
import websockets
import json
import asyncio
from datetime import datetime, timezone
from pathlib import Path


async def main():
    db = sqlite3.connect(f"{str(Path(__file__).parent / 'mango_bowl_level3_snapshots.db')}")

    db.execute('pragma journal_mode=WAL')

    db.execute('pragma synchronous=normal')

    db.execute("""
        create table if not exists entries (
            content text,
            local_timestamp text
        )
    """)

    db.execute('create index if not exists entries_idx_0 on entries (local_timestamp)')

    async with websockets.connect('ws://mangolorians.com:8010/v1/ws') as connection:
        markets = [
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

        message = {
            'op': 'subscribe',
            'channel': 'level3',
            'markets': markets
        }

        snapshots = { market: None for market in markets }

        await connection.send(json.dumps(message))

        async for response in connection:
            data = json.loads(response)

            if (data['type'] != 'l3snapshot') or (snapshots[data['market']]):
                continue

            snapshots[data['market']] = data

            if not all(snapshots.values()):
                continue

            for snapshot in snapshots.values():
                db.execute(
                    'insert into entries values (?, ?)',
                    (json.dumps(snapshot),
                     datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'))
                )

            db.commit()

            exit()


if __name__ == '__main__':
    asyncio.run(main())