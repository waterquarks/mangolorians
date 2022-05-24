import asyncio
import json
import sqlite3
from pathlib import Path

import websockets


async def ingestor():
    async for connection in websockets.connect('wss://api.serum-vial.dev/v1/ws'):
        try:
            message = {
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
                    'LUNA/USDC'
                ]
            }

            await connection.send(json.dumps(message))

            async for response in connection:
                yield json.loads(response)
        except websockets.WebSocketException:
            continue


async def main():
    db = sqlite3.connect(f"{str(Path(__file__).parent / 'serum_l2_order_book.db')}")

    db.set_trace_callback(print)

    db.execute('pragma journal_mode=WAL')

    db.execute('pragma synchronous=normal')

    db.execute('drop table if exists orders')

    db.execute("create table if not exists orders (market text, side text, price real, size real, primary key (market, side, price)) without rowid")

    async for entry in ingestor():
        if entry['type'] not in {'l2snapshot', 'l2update'}:
            continue

        if entry['type'] == 'l2snapshot':
            db.execute('delete from orders where market = ?', [entry['market']])

        for side in {'bids', 'asks'}:
            for price, size in entry[side]:
                price, size = (float(price), float(size))

                if size == 0:
                    db.execute('delete from orders where market = ? and price = ?', [entry['market'], price])
                else:
                    db.execute('insert or replace into orders values (?, ?, ?, ?)', [entry['market'], side, price, size])
        else:
            db.commit()

if __name__ == '__main__':
    asyncio.run(main())