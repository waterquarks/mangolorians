import asyncio
import json

import websockets


async def main():
    async for websocket in websockets.connect('ws://mangolorians.com:8010/v1/ws'):
        try:
            await websocket.send(json.dumps({
                'op': 'subscribe',
                'channel': 'trades',
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
                    'SRM-PERP'
                ]
            }))

            async for raw_message in websocket:
                print(json.loads(raw_message))
        except websockets.WebSocketException:
            continue

if __name__ == '__main__':
    asyncio.run(main())