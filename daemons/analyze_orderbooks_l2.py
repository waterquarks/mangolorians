from lib import streams
import asyncio
import sqlite3
from pathlib import Path
from aiostream import stream


async def main():
    db = sqlite3.connect(Path(__file__).parent / 'analyze_orderbooks_l2.db')

    db.execute('pragma journal_mode=WAL')

    db.execute('pragma synchronous=1')

    db.execute("""
        create table if not exists orders (
            exchange text,
            symbol text,
            side text,
            price real,
            amount real,
            primary key (exchange, symbol, side, price)
        ) without rowid
    """)

    db.execute("""
        create table if not exists quotes (
            exchange text,
            symbol text,
            order_size real,
            mid_price real,
            weighted_average_buy_price real,
            weighted_average_sell_price real,
            timestamp text,
            primary key (exchange, symbol, order_size, timestamp)
        ) without rowid
    """)

    db.execute("""
        create table if not exists depth (
            exchange text,
            symbol text,
            bids real,
            asks real,
            timestamp text,
            primary key (exchange, symbol, timestamp)
        ) without rowid
    """)

    async for message in stream.merge(
        streams.mango_markets_perps_l2_normalized([
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
        ]),
        streams.mango_markets_spot_l2_normalized([
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
            'GMT/USDC'
        ])
    ):
        print(message)

        if message['is_snapshot']:
            db.execute('delete from orders where exchange = ? and symbol = ?', [message['exchange'], message['symbol']])

        for side in {'bids', 'asks'}:
            for price, amount in message['orders'][side]:
                if amount == 0:
                    db.execute(
                        'delete from orders where exchange = ? and symbol = ? and side = ? and price = ?',
                        [message['exchange'], message['symbol'], side, price]
                    )
                else:
                    db.execute(
                        'insert or replace into orders values (?, ?, ?, ?, ?)',
                        [message['exchange'], message['symbol'], side, price, amount]
                    )
        else:
            db.executemany("""
                insert or replace into quotes
                with
                    orders as (
                        select
                            exchange,
                            symbol,
                            side,
                            price,
                            amount,
                            price * amount as volume,
                            sum(price * amount) over (partition by exchange, symbol, side order by case when side = 'bids' then - price when side = 'asks' then price end) as cumulative_volume
                        from main.orders
                        order by exchange, symbol, side, case when side = 'bids' then - price when side = 'asks' then price end
                    ),
                    fills as (
                        select
                            exchange,
                            symbol,
                            side,
                            price,
                            fill,
                            sum(fill) over (
                                partition by side order by case when side = 'bids' then - price when side = 'asks' then price end
                            ) as cumulative_fill
                        from (
                            select
                                exchange,
                                symbol,
                                side,
                                price,
                                case
                                    when cumulative_volume < :order_size then volume
                                    else coalesce(lag(remainder) over (partition by exchange, symbol, side), case when volume < :order_size then volume else :order_size end)
                                end as fill
                            from (select *, :order_size - cumulative_volume as remainder from orders)
                        )
                        where fill > 0
                    ),
                    weighted_average_fill_prices as (
                        select
                            exchange,
                            symbol,
                            case when sum(case when side = 'asks' then fill end) = :order_size then sum(case when side = 'asks' then price * fill end) / :order_size end as weighted_average_buy_price,
                            case when sum(case when side = 'bids' then fill end) = :order_size then sum(case when side = 'bids' then price * fill end) / :order_size end as weighted_average_sell_price
                        from fills
                        group by exchange, symbol
                    ),
                    misc as (
                        select
                            exchange,
                            symbol,
                            (top_bid + top_ask) / 2 as mid_price
                        from (
                            select
                                exchange,
                                symbol,
                                max(price) filter ( where side = 'bids') as top_bid,
                                min(price) filter ( where side = 'asks') as top_ask
                            from orders
                            group by exchange, symbol
                        )
                    )
                select
                    exchange,
                    symbol,
                    :order_size as order_size,
                    mid_price,
                    weighted_average_buy_price,
                    weighted_average_sell_price,
                    :timestamp
                from weighted_average_fill_prices
                inner join misc using (exchange, symbol)
                where exchange = :exchange
                  and symbol = :symbol
            """, [{'exchange': message['exchange'], 'symbol': message['symbol'], 'order_size': order_size, 'timestamp': message['timestamp']} for order_size in [1000, 10000, 25000, 50000, 100000]])

            db.execute("""
                insert or replace into depth
                select exchange
                     , symbol
                     , sum(price * amount) filter ( where side = 'bids' ) as bids
                     , sum(price * amount) filter ( where side = 'asks' ) as asks
                     , :timestamp
                from orders where exchange = :exchange and symbol = :symbol;
            """, {'exchange': message['exchange'], 'symbol': message['symbol'], 'timestamp': message['timestamp']})

            db.commit()


if __name__ == '__main__':
    asyncio.run(main())