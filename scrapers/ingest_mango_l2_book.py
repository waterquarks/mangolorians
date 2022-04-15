import websockets
import asyncio
import json
import sqlite3


async def ingestor():
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
                    'LUNA-PERP',
                    'MNGO-PERP',
                    'RAY-PERP',
                    'SRM-PERP'
                ]
            }

            await connection.send(json.dumps(message))

            async for response in connection:
                yield json.loads(response)
        except websockets.WebSocketException:
            continue


async def main():
    db = sqlite3.connect('./mango_l2_order_book.db')

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
            db.execute("""
                insert into liquidity
                select
                    ? as timestamp,
                    ? as market,
                    sum(price * size) filter (where side = 'bids') as buy,
                    sum(price * size) filter (where side = 'asks') as sell
                from orders
            """, [entry['timestamp'], entry['market']])

            db.commit()

asyncio.run(main())