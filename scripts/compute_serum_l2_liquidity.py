import psycopg2
import psycopg2.extras
import sqlite3
from datetime import datetime, timezone


def main():
    state = sqlite3.connect(':memory:')

    state.execute('create table orders (market text, side text, price real, size real, primary key (market, side, price)) without rowid;')

    state.execute('create table liquidity (market text, slot text, buy real, sell real, timestamp text, primary key (market, timestamp)) without rowid;')

    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute('select market, max("timestamp") as max_timestamp from serum_vial.liquidity group by market')

    anchors = {market: max_timestamp.replace(tzinfo=timezone.utc) for market, max_timestamp in cur}

    fetcher = conn.cursor('fetcher')

    fetcher.execute("""
        select
            jsonb_build_object('market', market, 'type', type) || content::jsonb as message
        from serum_vial.level2
        order by local_timestamp;
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

            state.execute("""
                insert or replace into liquidity
                with
                    orders as (
                        select
                            market,
                            side,
                            price,
                            size
                        from main.orders
                        order by market, side, case when side = 'asks' then price when side = 'bids' then - price end
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
                    summary as (
                        select
                            market,
                            side,
                            price,
                            mid_price,
                            case
                                when side = 'asks' then (price - mid_price) / price
                                when side = 'bids' then (mid_price - price) / mid_price
                            end * 100 as distance_from_price_to_mid_price,
                            size
                        from orders inner join meta using (market)
                    )
                select
                    market,
                    :slot as slot,
                    sum(price * size) filter ( where side = 'bids' ) as buy,
                    sum(price * size) filter ( where side = 'asks' ) as sell,
                    :timestamp as "timestamp"
                from summary
                where market = :market and distance_from_price_to_mid_price <= 5
                group by market
            """, {'market': entry['market'], 'slot': entry['slot'], 'timestamp': entry['timestamp']})

    state.commit()

    liquidity = state.execute('select market, slot, buy, sell, "timestamp" from liquidity').fetchall()

    cur.execute("""
        create table if not exists serum_vial.liquidity (
            market text,
            slot integer,
            buy numeric,
            sell numeric,
            "timestamp" timestamptz,
            primary key (market, "timestamp")
        );
    """)

    psycopg2.extras.execute_values(
        cur,
        """
            insert into serum_vial.liquidity (
                market,
                slot,
                buy,
                sell,
                \"timestamp\"
            ) values %s
            on conflict on constraint liquidity_pkey do nothing
        """,
        liquidity
    )

    conn.commit()


if __name__ == '__main__':
    main()
