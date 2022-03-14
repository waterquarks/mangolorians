import asyncio
import websockets

connections = set()


async def dump():
    while True:
        for connection in connections:
            await connection.send('XD')
        await asyncio.sleep(2)


asyncio.get_event_loop().create_task(dump())


async def handler(connection, path):
    connections.add(connection)

    try:
        async for msg in connection:
            pass
    finally:
        connections.remove(connection)


server = websockets.serve(handler, 'localhost', 1234)

asyncio.get_event_loop().run_until_complete(server)
asyncio.get_event_loop().run_forever()


# Live stream slippages
# Background process fills a queue with objects
# Websocket broadcasts those to a client
