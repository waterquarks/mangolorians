import asyncio
import json
from datetime import datetime, timezone

import websockets


async def mango_markets_l2_order_book(symbols):
    async for websocket in websockets.connect('ws://mangolorians.com:8010/v1/ws'):
        try:
            await websocket.send(json.dumps({
                'op': 'subscribe',
                'channel': 'level2',
                'markets': symbols
            }))

            async for response in websocket:
                yield json.loads(response)

        except websockets.WebSocketException:
            continue


async def mango_markets_l2():
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

            async for response in websocket:
                yield json.loads(response)

        except websockets.WebSocketException:
            continue


async def mango_markets_l2_normalized():
    async for message in mango_markets_l2():
        if message['type'] not in {'l2snapshot', 'l2update'}:
            continue

        yield {
            'exchange': 'Mango Markets',
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
    async for message in mango_markets_l2_normalized():
        print(message)

if __name__ == '__main__':
    asyncio.run(main())
