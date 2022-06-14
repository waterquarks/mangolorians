import asyncio
import json
import websockets
import sqlite3
import os
from pathlib import Path
from datetime import datetime, timezone, date


async def main():
    async def stream(symbol):
        async for websocket in websockets.connect('ws://mangolorians.com:8010/v1/ws'):
            try:
                await websocket.send(json.dumps({
                    'op': 'subscribe',
                    'channel': 'level3',
                    'markets': [symbol]
                }))

                async for response in websocket:
                    yield json.loads(response)
            except websockets.WebSocketException:
                continue

    async def scrape(symbol):
        path = Path(f"./native/mango-markets/incremental_book_L3/{str(date.today())}/{symbol.replace('/', '_')}.db")

        os.makedirs(path.parent, exist_ok=True)

        db = sqlite3.connect(path)

        db.set_trace_callback(print)

        db.execute('pragma journal_mode=WAL')

        db.execute('pragma synchronous=1')

        db.execute('create table if not exists messages (content text, local_timestamp text)')

        db.execute('create index if not exists idx on messages (local_timestamp)')

        async for message in stream(symbol):
            db.execute(
                'insert into messages values (?, ?)',
                [json.dumps(message),
                 datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')]
            )

            db.commit()

    symbols = [
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

    await asyncio.gather(*[scrape(symbol) for symbol in symbols])


if __name__ == '__main__':
    asyncio.run(main())