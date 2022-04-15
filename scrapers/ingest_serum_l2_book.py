import websockets
import asyncio
import json
import sqlite3


async def ingestor():
    async for connection in websockets.connect('ws://mangolorians.com:8900/v1/ws'):
        try:
            message = {
                'op': 'subscribe',
                'channel': 'level2',
                'markets': [
                    'AVAX/USDC',
                    'BNB/USDC',
                    'BTC/USDC',
                    'ETH/USDC',
                    'FTT/USDC',
                    'LUNA/USDC',
                    'MNGO/USDC',
                    'RAY/USDC',
                    'LUNA/USDC',
                    'SRM/USDC',
                    'SOL/USDC',
                    'MSOL/USDC',
                    'USDT/USDC',
                    'COPE/USDC',
                ]
            }

            await connection.send(json.dumps(message))

            async for response in connection:
                yield json.loads(response)
        except websockets.WebSocketException:
            continue


async def main():
    db = sqlite3.connect('./serum_l2_order_book.db')

    db.set_trace_callback(print)

    db.execute('pragma journal_mode=WAL')

    db.execute('drop table if exists orders')

    db.execute("create table if not exists orders (market text, side text, price real, size real, primary key (market, side, price)) without rowid")

    db.execute("create table if not exists liquidity (timestamp text, market text, buy real, sell real, primary key (timestamp, market)) without rowid")

    async for entry in ingestor():
        if entry['type'] not in {'l2snapshot', 'l2update'}:
            continue

        if entry['type'] == 'l2snapshot':
            db.execute('delete from orders where market = ?', [entry['market']])

        for side in {'bids', 'asks'}:
            for price, size in entry[side]:
                price, size = float(price), float(size)

                if size == 0:
                    db.execute('delete from orders where market = ? and side = ? and price = ?', [entry['market'], side, price])
                else:
                    db.execute('insert or replace into orders values (?, ?, ?, ?)', [entry['market'], side, price, size])
        else:
            db.commit()

asyncio.run(main())