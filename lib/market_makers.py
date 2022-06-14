import sqlite3
import json
from pathlib import Path


def benchmark(symbol, account, date):
    db = sqlite3.connect(':memory:')

    db.execute('pragma journal_mode=WAL')

    db.execute('pragma synchronous=1')

    db.execute("""
        create table if not exists quotes (
            market text,
            account text,
            bids real,
            asks real,
            slot integer,
            "timestamp" text,
            primary key (market, account, "timestamp")
        ) without rowid
    """)

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

    source = Path(__file__).parent.parent / 'daemons' / 'native' / 'mango-markets' / 'incremental_book_L3' / date / f"{symbol}.db"

    db.execute(f"attach database '{source}' as source")

    for symbol, is_snapshot, orders, slot, timestamp in db.execute("""
        select symbol
             , is_snapshot
             , json_object(
                 'bids', json_group_array(json_array(order_id, account, price, amount)) filter ( where side = 'bids' and account = :account ),
                 'asks', json_group_array(json_array(order_id, account, price, amount)) filter ( where side = 'asks' and account = :account )
               ) as orders
             , slot
             , timestamp
        from deltas
        group by symbol, timestamp;
    """, {'account': account}):
        if is_snapshot:
            db.execute('delete from orders where market = ?', [symbol])

        for side in {'bids', 'asks'}:
            for order_id, account, price, size in json.loads(orders)[side]:
                if price == 0:
                    db.execute('delete from orders where market = ? and side = ? and order_id = ?', [symbol, side, order_id])
                else:
                    db.execute('insert or replace into orders values (?, ?, ?, ?, ?, ?)', [symbol, side, order_id, account, price, size])
        else:
            db.execute("""
                insert or replace into quotes
                with
                    depth as (
                        select
                            coalesce(sum(price * size) filter ( where side = 'bids' ), 0) as bids,
                            coalesce(sum(price * size) filter ( where side = 'asks' ), 0) as asks
                        from orders
                    )
                select
                    :market as market,
                    :account as account,
                    bids,
                    asks,
                    :slot as slot,
                    :timestamp as "timestamp"
                from depth
            """, {
                'account': account,
                'market': symbol,
                'slot': slot,
                'timestamp': timestamp
            })

    db.commit()

    return db.execute("""
        with
            entries as (
                select
                    cast(strftime('%s', strftime('%Y-%m-%d %H:%M:00', timestamp)) * 1e3 as integer) as minute,
                    avg(bids) as bids,
                    avg(asks) as asks
                from quotes
                group by minute
                order by minute
            )
        select json_group_array(json_array(minute, bids, asks)) from entries;
    """).fetchone()


if __name__ == '__main__':
    print(benchmark('SOL-PERP', '4rm5QCgFPm4d37MCawNypngV4qPWv4D5tw57KE2qUcLE', '2022-06-13'))