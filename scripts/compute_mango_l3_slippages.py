import sqlite3
import psycopg2


def main():
    db = sqlite3.connect(f"./b1.db")

    db.execute('pragma journal_mode=WAL')

    db.execute('pragma synchronous=1')

    db.execute("""
        create table if not exists orders (
            market text,
            side text,
            order_id text,
            account text,
            price real,
            size real,
            primary key (market, side, order_id)
        ) without rowid
    """)

    db.execute("""
        create table if not exists slippages (
            market text,
            size real,
            buy real,
            sell real,
            slot integer,
            "timestamp" text,
            primary key (market, "timestamp")
        ) without rowid
    """)

    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor('cur')

    query = """
        select market
             , is_snapshot
             , json_build_object(
                'bids', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'buy' ), json_build_array()),
                'asks', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'sell' ), json_build_array())
               ) as orders
             , slot
             , "timestamp"
        from mango_bowl.level3_deltas
        where date_trunc('day', local_timestamp at time zone 'utc') = '2022-05-15'
        group by market, is_snapshot, slot, "timestamp"
        order by market, "timestamp";
    """

    cur.execute(query)

    for market, is_snapshot, orders, slot, timestamp in cur:
        print(market, is_snapshot, slot, timestamp)

        if is_snapshot:
            db.execute('delete from orders where market = ?', [market])

        for side in {'bids', 'asks'}:
            for account, order_id, price, size in orders[side]:
                if price == 0:
                    db.execute('delete from orders where market = ? and side = ? and order_id = ?', [market, side, order_id])
                else:
                    db.execute('insert or replace into orders values (?, ?, ?, ?, ?, ?)', [market, side, order_id, account, price, size])
        else:
            for size in [50000, 100000, 200000, 500000, 1000000]:
                db.execute("""
                    insert or replace into slippages
                    with
                        params (size) as (
                            values(:size)
                        ),
                        orders as (
                            select
                                market,
                                side,
                                price,
                                size,
                                price * size as volume,
                                sum(price * size) over (partition by market, side order by case when side = 'bids' then - price when side = 'asks' then price end) as cumulative_volume
                            from main.orders
                            order by side, case when side = 'bids' then - price when side = 'asks' then price end
                       ),
                       meta as (
                            select
                                market,
                                (top_bid + top_ask) / 2 as mid_price
                            from (
                                select
                                    market,
                                    max(price) filter ( where side = 'bids' ) as top_bid,
                                    min(price) filter ( where side = 'asks' ) as top_ask
                                from orders
                                group by market
                            ) as slams
                        ),
                        remainders as (
                            select
                                market,
                                side,
                                price,
                                size,
                                volume,
                                cumulative_volume,
                                (select size from params) - cumulative_volume as remainder
                            from orders
                        ),
                        fills as (
                            select
                                market,
                                side,
                                price,
                                case
                                    when cumulative_volume < (select size from params) then volume
                                    else coalesce(lag(remainder) over (partition by market, side), case when volume < (select size from params) then volume else (select size from params) end)
                                end as fill
                            from remainders
                        ),
                        cumulative_fills as (
                            select
                                market,
                                side,
                                price,
                                fill,
                                sum(fill) over (
                                    partition by market, side
                                    order by case when side = 'bids' then - price when side = 'asks' then price end
                                ) as cumulative_fill
                            from fills
                            where fill > 0
                        ),
                        weighted_average_fill_prices as (
                            select
                                market,
                                (select size from params) as size,
                                case when sum(case when side = 'asks' then fill end) = (select size from params) then sum(case when side = 'asks' then price * fill end) / (select size from params) end as weighted_average_buy_price,
                                case when sum(case when side = 'bids' then fill end) = (select size from params) then sum(case when side = 'bids' then price * fill end) / (select size from params) end as weighted_average_sell_price
                            from cumulative_fills
                            group by market
                        ),
                        slippages as (
                            select
                                market,
                                size,
                                ((weighted_average_buy_price - mid_price) / mid_price) * 1e2 as buy,
                                ((mid_price - weighted_average_sell_price) / mid_price) * 1e2 as sell
                            from weighted_average_fill_prices inner join meta using (market)
                        )
                    select
                        market,
                        size as size,
                        buy,
                        sell,
                        :slot as slot,
                        :timestamp as "timestamp"
                    from slippages where market = :market;
                """, {'market': market, 'slot': slot, 'size': size, 'timestamp': timestamp})


    db.commit()


if __name__ == '__main__':
    main()