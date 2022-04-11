import websockets
import json
import asyncio
import sqlite3
from datetime import datetime, timezone

async def ingestor():
    db = sqlite3.connect('./l2_order_book.db')

    db.execute('create table if not exists entries (local_timestamp text, message text)')

    db.execute('create index if not exists idx on entries (local_timestamp)')

    db.commit()

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
                entry = {
                    'local_timestamp': datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
                    'message': message
                }

                db.execute('insert into entries values (:local_timestamp, :message)', entry)

                db.commit()

                yield json.loads(message)
        except websockets.WebSocketException:
            continue


async def main():
    db = sqlite3.connect('l2_order_book_analytics.db')

    db.execute('pragma journal_mode=WAL')

    db.set_trace_callback(print)

    db.execute('drop table if exists orders')

    db.execute("create table if not exists orders (market text, side text, price real, size real, primary key (market, side, price)) without rowid")

    db.execute("create table if not exists liquidity (timestamp text, market text, buy real, sell real, primary key (timestamp, market)) without rowid")

    db.execute("create table if not exists slippages (timestamp text, market text, order_size real, buy_slippage_bps real, sell_slippage_bps real, total_slippage_bps real, primary key (timestamp, market, order_size)) without rowid")

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
            db.execute("""
                insert or replace into liquidity
                select
                    ? as timestamp,
                    market,
                    sum(case when market = ? and side = 'bids' then price * size end) as buy,
                    sum(case when market = ? and side = 'asks' then price * size end) as sell
                from orders
            """, [entry['data']['time'], entry['market'], entry['market']])

            query = """
                insert or replace into slippages
                with
                    orders as (
                        select
                            side,
                            price,
                            sum(size) as size,
                            price * sum(size) as volume,
                            sum(price * sum(size)) over (partition by side order by case when side = 'bids' then - price when side = 'asks' then price end) as cumulative_volume
                        from main.orders
                        where market = :market
                        group by side, price
                        order by side, case when side = 'bids' then - price when side = 'asks' then price end
                    ),
                    misc as (
                        select
                            *,
                            (top_bid + top_ask) / 2 as mid_price
                        from (
                            select
                                max(price) filter ( where side = 'bids') as top_bid,
                                min(price) filter ( where side = 'asks') as top_ask
                            from orders
                        )
                    ),
                    fills as (
                        select
                            side, price, fill, sum(fill) over (partition by side order by case when side = 'bids' then - price when side = 'asks' then price end) as cumulative_fill
                        from (
                            select
                                side,
                                price,
                                case when cumulative_volume < :order_size then volume else coalesce(lag(remainder) over (partition by side), case when volume < :order_size then volume else :order_size end) end as fill
                            from (select *, :order_size - cumulative_volume as remainder from orders)
                        )
                        where fill > 0
                    ),
                
                    weighted_average_fill_prices as (
                        select
                            case when sum(case when side = 'asks' then fill end) = :order_size then sum(case when side = 'asks' then price * fill end) / :order_size end as weighted_average_buy_price,
                            case when sum(case when side = 'bids' then fill end) = :order_size then sum(case when side = 'bids' then price * fill end) / :order_size end as weighted_average_sell_price
                        from fills
                    )
                select
                    *,
                    buy_slippage_bps + sell_slippage_bps as total_slippage_bps
                from
                (
                    select
                        :timestamp as timestamp,
                        :market as market,
                        :order_size as order_size,
                        ((weighted_average_buy_price - mid_price) / mid_price) * 1e4 as buy_slippage_bps,
                        ((mid_price - weighted_average_sell_price) / mid_price) * 1e4 as sell_slippage_bps
                    from weighted_average_fill_prices, misc
                )
            """

            for order_size in [50000, 100000, 200000, 500000, 1000000]:
                db.execute(query, {'timestamp': entry['data']['time'], 'market': entry['market'], 'order_size': order_size})

            db.commit()




if __name__ == '__main__':
    asyncio.run(main())