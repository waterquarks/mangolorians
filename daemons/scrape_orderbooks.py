import asyncio
import json
import websockets
from aiostream import stream
import sqlite3
from datetime import datetime, timezone


async def main():
    async def mango_markets():
        async for websocket in websockets.connect('ws://mangolorians.com:8010/v1/ws'):
            try:
                await websocket.send(json.dumps({
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
                        'SRM-PERP',
                        'GMT-PERP'
                    ]
                }))

                await websocket.send(json.dumps({
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
                        'SRM-PERP',
                        'GMT-PERP'
                    ]
                }))

                async for response in websocket:
                    yield json.loads(response)

            except websockets.WebSocketException:
                continue

    async def serum():
        async for websocket in websockets.connect('ws://mangolorians.com:8900/v1/ws'):
            try:
                await websocket.send(json.dumps({
                    'op': 'subscribe',
                    'channel': 'level2',
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

                async for response in websocket:
                    yield json.loads(response)

            except websockets.WebSocketException:
                continue

    db = sqlite3.connect('./native_2022-06-09.db')

    db.set_trace_callback(print)

    db.execute('pragma journal_mode=WAL')

    db.execute('pragma synchronous=1')

    db.execute('create table if not exists messages (content text, local_timestamp text)')

    async for message in stream.merge(mango_markets(), serum()):
        db.execute(
            'insert into messages values (?, ?)',
            [json.dumps(message), datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')]
        )


if __name__ == '__main__':
    asyncio.run(main())