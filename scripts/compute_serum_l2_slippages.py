import psycopg2
import psycopg2.extras
import sqlite3
from datetime import datetime, timezone


def main():
    state = sqlite3.connect(':memory:')

    state.execute('create table orders (market text, side text, price real, size real, primary key (market, side, price)) without rowid')

    state.execute('create table slippages (market text, slot text, size integer, buy real, sell real, "timestamp" text, primary key (market, timestamp, size)) without rowid')

    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute('select market, max("timestamp") as max_timestamp from serum_vial.slippages group by market')

    anchors = {market: max_timestamp.replace(tzinfo=timezone.utc) for market, max_timestamp in cur}

    fetcher = conn.cursor('fetcher')

    fetcher.execute("""
        select
            jsonb_build_object('market', market, 'type', type) || content::jsonb as message
        from serum_vial.level2
        order by local_timestamp
    """)

    for [entry] in fetcher:
        if entry['type'] == 'l2snapshot':
            state.execute('delete from orders where market = ?', [entry['market']])

        for side in {'bids', 'asks'}:
            for price, size in entry[side]:
                price, size = float(price), float(size)

                if size == 0:
                    state.execute('delete from orders where market = ? and side = ? and price = ?', [entry['market'], side, price])
                else:
                    state.execute('insert or replace into orders values (?, ?, ?, ?)', [entry['market'], side, price, size])
        else:
            if entry['market'] in anchors:
                if not (datetime.strptime(entry['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc) > anchors[entry['market']]):
                    continue

            for size in [50000, 100000, 200000, 500000, 1000000]:
                state.execute("""
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
                                size
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
                        orders_with_distance_from_mid_price as (
                            select
                                market,
                                side,
                                price,
                                size,
                                mid_price,
                                case
                                    when side = 'asks' then (price - mid_price) / price
                                    when side = 'bids' then (mid_price - price) / mid_price
                                end * 100 as distance_from_price_to_mid_price
                            from orders inner join meta using (market)
                        ),
                        orders_within_price_distance as (
                            select
                                market,
                                side,
                                price,
                                size,
                                mid_price,
                                distance_from_price_to_mid_price,
                                price * size as volume,
                                sum(price * size) over (partition by market, side order by case when side = 'bids' then - price when side = 'asks' then price end) as cumulative_volume
                            from orders_with_distance_from_mid_price
                            where distance_from_price_to_mid_price <= 7.5
                        ),
                        remainders as (
                            select
                                market,
                                side,
                                price,
                                size,
                                mid_price,
                                distance_from_price_to_mid_price,
                                volume,
                                cumulative_volume,
                                (select size from params) - cumulative_volume as remainder
                            from orders_within_price_distance
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
                        :slot as slot,
                        size as size,
                        buy,
                        sell,
                        :timestamp as "timestamp"
                    from slippages where market = :market
                """, {'market': entry['market'], 'slot': entry['slot'], 'size': size, 'timestamp': entry['timestamp']})

    state.commit()

    cur.execute("""
        create table if not exists serum_vial.slippages (
            market text,
            slot integer,
            size integer,
            buy numeric,
            sell numeric,
            "timestamp" timestamptz,
            primary key (market, "timestamp", size)
        );
    """)

    slippages = state.execute('select market, slot, size, buy, sell, "timestamp" from slippages').fetchall()

    psycopg2.extras.execute_values(cur, """
        insert into serum_vial.slippages (
            market,
            slot,
            size,
            buy,
            sell,
            \"timestamp\"
        ) values %s
        on conflict on constraint slippages_pkey do nothing
    """, slippages)

    conn.commit()


if __name__ == '__main__':
    main()
