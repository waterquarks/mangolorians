import sqlite3
import json
import psycopg2


def main(hour):
    db = sqlite3.connect(f"./depth.db")

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
        create table if not exists liquidity (
            market text,
            bids real,
            asks real,
            slot integer,
            "timestamp" text,
            primary key (market, slot, "timestamp")
        ) without rowid
    """)

    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor('cur')

    query = """
        with
             entries as (
                 select content ->> 'market' as market
                      , content ->> 'type' = 'l3snapshot' as is_snapshot
                      , content
                      , (content ->> 'slot')::integer as slot
                      , (content ->> 'timestamp')::timestamptz at time zone 'utc' as "timestamp"
                      , local_timestamp
                 from mango_bowl.level3
                 where date_trunc('hour', local_timestamp at time zone 'utc') = %s
                   and content ->> 'type' in ('l3snapshot', 'open', 'done')
             ),
             anchors as (
                select
                    market, min(local_timestamp) as local_timestamp
                from entries
                where is_snapshot
                group by market, is_snapshot
             ),
             snapshots as (
                 select market, is_snapshot, content, slot, timestamp, local_timestamp
                 from anchors inner join entries using (market, local_timestamp)
             ),
             deltas as (
                select entries.market
                     , entries.is_snapshot
                     , entries.content
                     , entries.slot
                     , entries.timestamp
                     , entries.local_timestamp
                from snapshots inner join entries on entries.market = snapshots.market and entries.local_timestamp > snapshots.local_timestamp
             ),
             batch as (
                select market
                     , is_snapshot
                     , content
                     , slot
                     , timestamp
                     , local_timestamp
                from snapshots
                union all
                select market
                     , is_snapshot
                     , content
                     , slot
                     , timestamp
                     , local_timestamp
                from deltas
             ),
             collapsable as (
                select market
                     , is_snapshot
                     , case
                         when content->>'type' = 'l3snapshot' then
                            (
                                select
                                    json_agg(
                                        json_build_array(
                                            value->>'account',
                                            value->>'side',
                                            value->>'orderId',
                                            (value->>'price')::float,
                                            (value->>'size')::float
                                        )
                                    )
                                from (
                                    select * from jsonb_array_elements(content->'bids')
                                    union all
                                    select * from jsonb_array_elements(content->'asks')
                                ) as orders
                            )
                        when content->>'type' = 'open' then
                            json_build_array(
                                json_build_array(
                                    content->>'account',
                                    content->>'side',
                                    content->>'orderId',
                                    (content->>'price')::float,
                                    (content->>'size')::float
                                )
                            )
                        when content->>'type' = 'done' then
                            json_build_array(
                                json_build_array(
                                    content->>'account',
                                    content->>'side',
                                    content->>'orderId',
                                    0,
                                    0
                                )
                            )
                     end as orders
                     , slot
                     , timestamp
                     , local_timestamp
                from batch order by market, local_timestamp
             ),
             collapsed as (
                select market
                     , is_snapshot
                     , value->>0 as account
                     , value->>1 as side
                     , value->>2 as order_id
                     , (value->>3)::float as price
                     , (value->>4)::float as size
                     , slot
                     , timestamp
                     , local_timestamp
                from collapsable, json_array_elements(orders)
                order by market, local_timestamp
             )
        select market
             , is_snapshot
             , json_build_object(
                'bids', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'buy' ), json_build_array()),
                'asks', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'sell' ), json_build_array())
               ) as orders
             , slot
             , "timestamp"
        from collapsed
        group by market, is_snapshot, slot, "timestamp"
        order by market, "timestamp";
    """

    cur.execute(query, [hour])

    for market, is_snapshot, orders, slot, timestamp in cur:
        print(market, slot, timestamp)

        if is_snapshot:
            db.execute('delete from orders where market = ?', [market])

        for side in {'bids', 'asks'}:
            for account, order_id, price, size in orders[side]:
                if price == 0:
                    db.execute('delete from orders where market = ? and side = ? and order_id = ?', [market, side, order_id])
                else:
                    db.execute('insert or replace into orders values (?, ?, ?, ?, ?, ?)', [market, side, order_id, account, price, size])
        else:
            db.execute("""
                insert or replace into liquidity
                select
                    market,
                    sum(price * size) filter ( where side = 'bids' ) as bids,
                    sum(price * size) filter ( where side = 'asks' ) as asks,
                    :slot as slot,
                    :timestamp as timestamp
                from orders where market = :market
            """, {'slot': slot, 'timestamp': timestamp, 'market': market})

    db.commit()


if __name__ == '__main__':
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute("select generate_series at time zone 'utc' as hour from generate_series('2022-05-12 00:00:00'::timestamptz at time zone 'utc', '2022-05-19 05:00:00', interval '1 hour') order by hour;")

    for hour in cur:
        main(hour)