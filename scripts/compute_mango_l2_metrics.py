import psycopg2
import sqlite3


def main():
    state = sqlite3.connect('./baz.db')

    state.execute("""
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

    state.execute("""
        create table if not exists depth (
            market text,
            bids real,
            asks real,
            slot integer,
            "timestamp" text,
            primary key (market, "timestamp")
        )
    """)

    db = psycopg2.connect('dbname=mangolorians')

    cur = db.cursor('cur')

    cur.execute("""
        select market
             , is_snapshot
             , json_build_object(
                'bids', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'buy' ), json_build_array()),
                'asks', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'sell' ), json_build_array())
               ) as orders
             , slot
             , "timestamp"
        from mango_bowl.level3_deltas
        where date_trunc('hour', local_timestamp at time zone 'utc') = '2022-05-09 00:00:00'
        group by market, is_snapshot, slot, "timestamp"
        order by market, "timestamp";
    """)

    for market, is_snapshot, orders, slot, timestamp in cur:
        if is_snapshot:
            state.execute('delete from orders where market = ?', [market])

        for side in {'bids', 'asks'}:
            for account, order_id, price, size in orders[side]:
                if price == 0:
                    state.execute('delete from orders where market = ? and side = ? and order_id = ?', [market, side, order_id])
                else:
                    state.execute('insert or replace into orders values (?, ?, ?, ?, ?, ?)', [market, side, order_id, account, price, size])
        else:
            [bids, asks] = state.execute("""
                select
                    sum(price * size) filter (where side = 'bids') as bids,
                    sum(price * size) filter (where side = 'asks') as asks
                from orders where market = ?
            """, [market]).fetchone()

            state.execute('insert or replace into depth values (?, ?, ?, ?, ?)', [market, bids, asks, slot, timestamp])

    state.commit()


if __name__ == '__main__':
    main()