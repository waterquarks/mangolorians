import sqlite3
import psycopg2


def main():
    db = sqlite3.connect(f"./a1.db")

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
        select market
             , is_snapshot
             , json_build_object(
                'bids', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'buy' ), json_build_array()),
                'asks', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'sell' ), json_build_array())
               ) as orders
             , slot
             , "timestamp"
        from mango_bowl.level3_deltas
        where date_trunc('day', local_timestamp at time zone 'utc') = '2022-05-18'
        group by market, is_snapshot, slot, "timestamp"
        order by market, "timestamp";
    """

    cur.execute(query)

    for market, is_snapshot, orders, slot, timestamp in cur:
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
    main()