import json
import sqlite3
import psycopg2
import psycopg2.extras


def historical_liquidity(instrument, account=None):
    state = sqlite3.connect(':memory:')

    state.row_factory = sqlite3.Row

    state.execute('drop table if exists orders')

    state.execute("""
        create table if not exists orders (
            side text,
            orderId text,
            price numeric,
            size numeric,
            primary key (side, orderId)
        ) without rowid
    """)

    state.execute('drop table if exists liquidity')

    state.execute("""
        create table if not exists liquidity (
            market text,
            slot integer,
            timestamp text,
            buy real,
            sell real,
            primary key (market, slot, timestamp)
        ) without rowid
    """)

    source = psycopg2.connect('dbname=mangolorians', cursor_factory=psycopg2.extras.RealDictCursor)

    cur = source.cursor()

    cur.execute("""
        select
            market,
            type,
            slot,
            timestamp,
            account,
            json_agg("order") as orders
        from order_book_l3_deltas
        where market = %s and account = %s
        group by market, type, slot, timestamp, account
        order by slot;
    """, [instrument, account])

    for entry in cur:
        if entry['type'] == 'l3snapshot':
            state.execute('delete from orders')

            for order in entry['orders']:
                state.execute('insert or replace into orders values (?, ?, ?, ?)', [order[key] for key in ['side', 'orderId', 'price', 'size']])

        if entry['type'] == 'l3mutation':
            for order in entry['orders']:
                if order['type'] == 'done':
                    state.execute('delete from orders where side = ? and orderId = ?', [order['side'], order['orderId']])

                if order['type'] == 'open':
                    state.execute('insert or replace into orders values (?, ?, ?, ?)', [order[key] for key in ['side', 'orderId', 'price', 'size']])

        liquidity = {
            'market': 'BTC-PERP',
            'slot': entry['slot'],
            'timestamp': entry['timestamp'],
            'buy': 0,
            'sell': 0,
        }

        for entry in state.execute('select side, sum(price * size) as liquidity from orders group by side'):
            liquidity[entry['side']] = entry['liquidity']

        state.execute("insert or replace into liquidity (market, slot, timestamp, buy, sell) values (:market, :slot, :timestamp, :buy, :sell)", liquidity)
    else:
        return list(map(dict, state.execute("""
            select
                   market,
                   strftime('%Y-%m-%dT%H:%M:00Z', "timestamp") as minute,
                   round(avg(buy)) as buy,
                   round(avg(sell)) as sell
            from liquidity
            group by minute;
        """)));
