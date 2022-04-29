import websockets
import json
import asyncio
import sqlite3

# Keep a local, in-memory replica of FTX's order book for the perps also in Mango.
# This is used for the slippage dashboard by $50K, $100K, $200K, $500K and $1M orders in liquidity analytics page.

async def ingestor():
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
                'LUNA-PERP',
                'RAY-PERP',
                'SRM-PERP'
            ]
            for market in markets:
                await connection.send(json.dumps({
                    'op': 'subscribe',
                    'channel': 'orderbook',
                    'market': f"{market}"
                }))

            async for message in connection:
                yield json.loads(message)
        except websockets.WebSocketException:
            continue


async def main():
    db = sqlite3.connect('../scripts/ftx_l2_order_book.db')

    db.execute('pragma journal_mode=WAL')

    db.set_trace_callback(print)

    db.execute('drop table if exists orders')

    db.execute("create table if not exists orders (market text, side text, price real, size real, primary key (market, side, price)) without rowid")

    async for entry in ingestor():
        if entry['type'] not in { 'partial', 'update' }:
            continue

        if entry['type'] == 'partial':
            db.execute('delete from orders where market = ?', [entry['market']])

        for side in ['bids', 'asks']:
            for order in entry['data'][side]:
                price, size = order

                if size == 0:
                    db.execute('delete from orders where market = ? and side = ? and price = ?', [entry['market'], side, price])
                else:
                    db.execute('insert or replace into orders values (?, ?, ?, ?)', [entry['market'], side, price, size])
        else:
            db.commit()

            
if __name__ == '__main__':
    asyncio.run(main())