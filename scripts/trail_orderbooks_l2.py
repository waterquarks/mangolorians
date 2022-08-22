import asyncio
import json
import sqlite3
from pathlib import Path
from aiostream import stream

import websockets
from datetime import datetime, timezone


async def ftx_perps():
    async for connection in websockets.connect('wss://ftx.com/ws/'):
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
                'RAY-PERP',
                'SRM-PERP',
                'GMT-PERP',
            ]

            await asyncio.gather(*[
                connection.send(json.dumps({
                    'op': 'subscribe',
                    'channel': 'orderbook',
                    'market': f"{market}"
                })) for market in markets
            ])

            async for message in connection:
                data = json.loads(message)

                if data['type'] not in {'partial', 'update'}:
                    continue

                yield {
                    'exchange': 'FTX perps',
                    'symbol': data['market'],
                    'is_snapshot': data['type'] == 'partial',
                    'orders': {'bids': data['data']['bids'], 'asks': data['data']['asks']},
                    'timestamp': datetime
                        .fromtimestamp(data['data']['time'], tz=timezone.utc)
                        .isoformat(timespec='microseconds')
                        .replace('+00:00', 'Z'),
                    'local_timestamp': datetime
                        .now(timezone.utc)
                        .isoformat(timespec='microseconds')
                        .replace('+00:00', 'Z')
                }
        except websockets.WebSocketException:
            continue


async def ftx_spot():
    async for connection in websockets.connect('wss://ftx.com/ws/'):
        try:
            markets = [
                'BTC/USD',
                'SOL/USD',
                'MNGO/USD',
                'ADA/USD',
                'AVAX/USD',
                'BNB/USD',
                'ETH/USD',
                'FTT/USD',
                'RAY/USD',
                'SRM/USD',
                'GMT/USD',
            ]

            await asyncio.gather(*[
                connection.send(json.dumps({
                    'op': 'subscribe',
                    'channel': 'orderbook',
                    'market': f"{market}"
                })) for market in markets
            ])

            async for message in connection:
                data = json.loads(message)

                if data['type'] not in {'partial', 'update'}:
                    continue

                yield {
                    'exchange': 'FTX spot',
                    'symbol': data['market'],
                    'is_snapshot': data['type'] == 'partial',
                    'orders': {'bids': data['data']['bids'], 'asks': data['data']['asks']},
                    'timestamp': datetime
                        .fromtimestamp(data['data']['time'], tz=timezone.utc)
                        .isoformat(timespec='microseconds')
                        .replace('+00:00', 'Z'),
                    'local_timestamp': datetime
                        .now(timezone.utc)
                        .isoformat(timespec='microseconds')
                        .replace('+00:00', 'Z')
                }
        except websockets.WebSocketException:
            continue


async def mango_markets_perps():
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
                    'MNGO-PERP',
                    'RAY-PERP',
                    'SRM-PERP',
                    'GMT-PERP'
                ]
            }

            await connection.send(json.dumps(message))

            async for message in connection:
                data = json.loads(message)

                if data['type'] not in {'l2snapshot', 'l2update'}:
                    continue

                yield {
                    'exchange': 'Mango Markets perps',
                    'symbol': data['market'],
                    'is_snapshot': data['type'] == 'l2snapshot',
                    'orders': {
                        'bids': [[float(price), float(size)] for price, size in data['bids']],
                        'asks': [[float(price), float(size)] for price, size in data['asks']]
                    },
                    'timestamp': datetime
                        .strptime(data['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                        .replace(tzinfo=timezone.utc)
                        .isoformat(timespec='microseconds'),
                    'local_timestamp': datetime
                        .now(timezone.utc)
                        .isoformat(timespec='milliseconds')
                        .replace('+00:00', 'Z')
                }
        except websockets.WebSocketException:
            continue


async def mango_markets_spot():
    async for connection in websockets.connect('ws://mangolorians.com:8900/v1/ws'):
        try:
            await connection.send(json.dumps({
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
                    'FTT/USDC',
                    'MSOL/USDC',
                    'BNB/USDC',
                    'AVAX/USDC',
                    'GMT/USDC'
                ]
            }))

            async for message in connection:
                data = json.loads(message)

                if data['type'] not in {'l2snapshot', 'l2update'}:
                    continue

                yield {
                    'exchange': 'Mango Markets spot',
                    'symbol': data['market'],
                    'is_snapshot': data['type'] == 'l2snapshot',
                    'orders': {
                        'bids': [[float(price), float(size)] for price, size in data['bids']],
                        'asks': [[float(price), float(size)] for price, size in data['asks']]
                    },
                    'timestamp': datetime
                        .strptime(data['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                        .replace(tzinfo=timezone.utc)
                        .isoformat(timespec='microseconds'),
                    'local_timestamp': datetime
                        .now(timezone.utc)
                        .isoformat(timespec='microseconds')
                        .replace('+00:00', 'Z')
                }
        except websockets.WebSocketException as exception:
            continue


async def main():
    db = sqlite3.connect(f"{str(Path(__file__).parent / 'orderbooks_l2.db')}")

    db.set_trace_callback(print)

    db.execute('pragma journal_mode=WAL')

    db.execute('pragma synchronous=normal')

    db.execute('drop table if exists orders')

    db.execute("create table if not exists orders (exchange text, symbol text, side text, price real, size real, primary key (exchange, symbol, side, price)) without rowid")

    async for entry in stream.merge(
        mango_markets_perps(),
        mango_markets_spot(),
        ftx_perps(),
        ftx_spot()
    ):
        if entry['is_snapshot']:
            db.execute('delete from orders where exchange = ? and symbol = ?', [entry['exchange'], entry['symbol']])

        for side in {'bids', 'asks'}:
            for price, size in entry['orders'][side]:
                if size == 0:
                    db.execute('delete from orders where exchange = ? and symbol = ? and side = ? and price = ?', [entry['exchange'], entry['symbol'], side, price])
                else:
                    db.execute('insert or replace into orders values (?, ?, ?, ?, ?)', [entry['exchange'], entry['symbol'], side, price, size])
        else:
            db.commit()


if __name__ == '__main__':
    asyncio.run(main())
