import asyncio
import json
from datetime import datetime, timezone
from aiostream import stream

import websockets


async def mango_markets_perp_l2(symbol):
    async for websocket in websockets.connect('ws://mangolorians.com:8010/v1/ws'):
        try:
            await websocket.send(json.dumps({
                'op': 'subscribe',
                'channel': 'level2',
                'markets': [symbol]
            }))

            async for response in websocket:
                yield json.loads(response)
        except websockets.WebSocketException:
            continue


async def mango_markets_perp_l2_normalized(symbol):
    async for message in mango_markets_perp_l2(symbol):
        if message['type'] not in {'l2snapshot', 'l2update'}:
            continue

        yield {
            'exchange': 'Mango Markets perps',
            'symbol': message['market'],
            'is_snapshot': message['type'] == 'l2snapshot',
            'orders': {
                'bids': [[float(price), float(amount)] for price, amount in message['bids']],
                'asks': [[float(price), float(amount)] for price, amount in message['asks']]
            },
            'timestamp': datetime
                .strptime(message['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                .replace(tzinfo=timezone.utc)
                .isoformat(timespec='microseconds'),
            'local_timestamp': datetime
                .now(timezone.utc)
                .isoformat(timespec='microseconds')
                .replace('+00:00', 'Z')
        }


async def mango_markets_perps_l2(symbols):
    async for message in stream.merge(*[mango_markets_perp_l2(symbol) for symbol in symbols]):
        yield message


async def mango_markets_perps_l2_normalized(symbols):
    async for message in stream.merge(*[mango_markets_perp_l2_normalized(symbol) for symbol in symbols]):
        yield message


async def mango_markets_spot_l2(symbol):
    async for websocket in websockets.connect('ws://mangolorians.com:8900/v1/ws'):
        try:
            await websocket.send(json.dumps({
                'op': 'subscribe',
                'channel': 'level2',
                'markets': symbol
            }))

            async for response in websocket:
                yield json.loads(response)

        except websockets.WebSocketException:
            continue


async def mango_markets_spot_l2_normalized(symbols):
    async for message in mango_markets_spot_l2(symbols):
        if message['type'] not in {'l2snapshot', 'l2update'}:
            continue

        yield {
            'exchange': 'Mango Markets spot',
            'symbol': message['market'],
            'is_snapshot': message['type'] == 'l2snapshot',
            'orders': {
                'bids': [[float(price), float(amount)] for price, amount in message['bids']],
                'asks': [[float(price), float(amount)] for price, amount in message['asks']]
            },
            'timestamp': datetime
                .strptime(message['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                .replace(tzinfo=timezone.utc)
                .isoformat(timespec='microseconds'),
            'local_timestamp': datetime
                .now(timezone.utc)
                .isoformat(timespec='microseconds')
                .replace('+00:00', 'Z')
        }


async def main():
    async for message in mango_markets_perps_l2(['SOL-PERP']):
        print(message)

if __name__ == '__main__':
    asyncio.run(main())
