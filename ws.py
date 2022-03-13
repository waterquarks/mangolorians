import asyncio
from websockets import serve


async def echo(websocket):
    async for message in websocket:
        await websocket.send(message)


async def main():
    async with serve(echo, 'localhost', 8765):
        await asyncio.Future()

asyncio.run(main())
