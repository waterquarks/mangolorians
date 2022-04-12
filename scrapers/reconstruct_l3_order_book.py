import sqlite3
import json
from pathlib import Path


def liquidity(instrument, accounts):
    db = sqlite3.connect(':memory:')

    db.execute(f"attach database '{str(Path(__file__).parent / 'l3_deltas.db')}' as source")

    db.row_factory = sqlite3.Row

    db.execute("create temp table orders (market text, account text, side text, id text, price real, size real, primary key (market, side, id)) without rowid")

    db.execute("create temp table liquidity (timestamp text, buy real, sell real, primary key (timestamp)) without rowid")

    for delta in db.execute(f"""
        select
            timestamp,
            market,
            is_snapshot,
            json_group_array(json_object('account', account, 'side', side, 'id', order_id, 'price', price, 'size', size)) as orders
        from source.deltas
        where timestamp > '2022-04-01'
          and market = ?
          and account in ({','.join(['?' for _ in accounts])})
        group by timestamp, market;
    """, [instrument, *accounts]):
        if delta['is_snapshot']:
            db.execute('delete from orders where market = ?', [delta['market']])

        for order in json.loads(delta['orders']):
            if order['price'] == 0:
                db.execute('delete from orders where market = ? and side = ? and id = ?', [delta['market'], order['side'], order['id']])
            else:
                db.execute('insert into orders values (?, ?, ?, ?, ?, ?)', [delta['market'], order['account'], order['side'], order['id'], order['price'], order['size']])
        else:
            db.execute("insert into liquidity select ? as timestamp, sum(case when side = 'buy' then price * size end) as buy, sum(case when side = 'sell' then price * size end) as sell from orders", [delta['timestamp']])

    db.commit()

    liquidity = db.execute("""
        with entries as (
            select strftime('%Y-%m-%dT%H:%M:00.00Z', timestamp) as minute, avg(buy) as buy, avg(sell) as sell from liquidity group by minute
        )
        select json_group_array(json_object('minute', minute, 'buy', buy, 'sell', sell)) from entries
    """).fetchone()[0]

    return liquidity


def spreads(instrument, accounts):
    db = sqlite3.connect(':memory:')

    db.execute(f"attach database '{str(Path(__file__).parent / 'l3_deltas.db')}' as source")

    db.row_factory = sqlite3.Row

    db.execute("create temp table orders (market text, account text, side text, id text, price real, size real, primary key (market, side, id)) without rowid")

    db.execute("create temp table quotes (timestamp text, weighted_average_bid real, weighted_average_ask real, primary key (timestamp)) without rowid")

    for delta in db.execute(f"""
        select
            timestamp,
            market,
            is_snapshot,
            json_group_array(json_object('account', account, 'side', side, 'id', order_id, 'price', price, 'size', size)) as orders
        from source.deltas
        where timestamp > '2022-04-01'
          and market = ?
          and account in ({','.join(['?' for _ in accounts])})
        group by timestamp, market;
    """, [instrument, *accounts]):
        if delta['is_snapshot']:
            db.execute('delete from orders where market = ?', [delta['market']])

        for order in json.loads(delta['orders']):
            if order['price'] == 0:
                db.execute('delete from orders where market = ? and side = ? and id = ?', [delta['market'], order['side'], order['id']])
            else:
                db.execute('insert into orders values (?, ?, ?, ?, ?, ?)', [delta['market'], order['account'], order['side'], order['id'], order['price'], order['size']])
        else:
            db.execute("""
                insert into quotes
                select
                    ? as timestamp,
                    sum(case when side = 'buy' then price * size end) / sum(case when side = 'buy' then size end) as weighted_average_bid,
                    sum(case when side = 'sell' then price * size end) / sum(case when side = 'sell' then size end) as weighted_average_ask
                from orders
            """, [delta['timestamp']])

    db.commit()

    liquidity = db.execute("""
        with entries as (
            select
                strftime('%Y-%m-%dT%H:%M:00.00Z', timestamp) as minute,
                avg(weighted_average_bid) as weighted_average_bid,
                avg(weighted_average_ask) as weighted_average_ask,
                avg(weighted_average_ask - weighted_average_bid) as absolute_spread,
                avg(((weighted_average_ask - weighted_average_bid) / weighted_average_ask) * 100) as relative_spread
            from quotes
            group by minute
        )
        select
            json_group_array(
                json_object(
                    'minute', minute,
                    'weighted_average_bid', weighted_average_bid,
                    'weighted_average_ask', weighted_average_ask,
                    'absolute_spread', absolute_spread,
                    'relative_spread', relative_spread
                )
            )
        from entries
    """).fetchone()[0]

    return liquidity

def benchmark(instrument, accounts):
    db = sqlite3.connect(':memory:')

    db.execute(f"attach database '{str(Path(__file__).parent / 'l3_deltas.db')}' as source")

    db.row_factory = sqlite3.Row

    db.execute("create temp table orders (market text, account text, side text, id text, price real, size real, primary key (market, side, id)) without rowid")

    db.execute("""
        create table spreads (
            timestamp text,
            buy_liquidity real,
            sell_liquidity real,
            weighted_average_bid real,
            weighted_average_ask real,
            spread real,
            active integer, primary key (timestamp)
        ) without rowid
    """)

    for delta in db.execute(f"""
        select
            timestamp,
            market,
            is_snapshot,
            json_group_array(json_object('account', account, 'side', side, 'id', order_id, 'price', price, 'size', size)) as orders
        from source.deltas
        where timestamp > '2022-04-01'
          and market = ?
          and account in ({','.join(['?' for _ in accounts])})
        group by timestamp, market;
    """, [instrument, *accounts]):
        if delta['is_snapshot']:
            db.execute('delete from orders where market = ?', [delta['market']])

        for order in json.loads(delta['orders']):
            if order['price'] == 0:
                db.execute('delete from orders where market = ? and side = ? and id = ?', [delta['market'], order['side'], order['id']])
            else:
                db.execute('insert into orders values (?, ?, ?, ?, ?, ?)', [delta['market'], order['account'], order['side'], order['id'], order['price'], order['size']])
        else:
            db.execute("""
                insert into spreads
                with quotes as (
                    select
                        sum(case when side = 'buy' then price * size end) as buy_liquidity,
                        sum(case when side = 'sell' then price * size end) as sell_liquidity,
                        sum(case when side = 'buy' then price * size end) / sum(case when side = 'buy' then size end) as weighted_average_bid,
                        sum(case when side = 'sell' then price * size end) / sum(case when side = 'sell' then size end) as weighted_average_ask
                    from orders
                ),
                spreads as (
                    select
                        buy_liquidity,
                        sell_liquidity,
                        weighted_average_bid,
                        weighted_average_ask,
                        weighted_average_ask - weighted_average_bid as spread
                    from quotes
                )
                select
                    ? as timestamp,
                    buy_liquidity,
                    sell_liquidity,
                    weighted_average_bid,
                    weighted_average_ask,
                    spread,
                    spread is not null as active
                from spreads
            """, [delta['timestamp']])

    db.commit()

    return dict(db.execute("""
        with
            ticks as (
                select
                    timestamp,
                    coalesce(julianday(timestamp) - julianday(lag(timestamp) over (order by timestamp)), 0) as delta,
                    buy_liquidity,
                    sell_liquidity,
                    weighted_average_bid,
                    weighted_average_ask,
                    spread,
                    active and buy_liquidity as active
                from spreads
            ),
            uptime as (
                select
                    elapsed,
                    uptime_absolute,
                    uptime_absolute / elapsed as uptime_relative
                from (
                     select
                        sum(delta) as elapsed,
                        sum(delta) filter (where active) as uptime_absolute
                    from ticks
                )
            ),
            metrics as (
                select
                    weighted_average_bid,
                    weighted_average_ask,
                    spread as spread_absolute,
                    ((weighted_average_ask - weighted_average_bid) / weighted_average_ask) * 1e2 as spread_relative
                from (
                     select
                        avg(weighted_average_bid) as weighted_average_bid,
                        avg(weighted_average_ask) as weighted_average_ask,
                        avg(spread) as spread
                    from ticks where active
                )
            )
        select * from metrics, uptime;
    """).fetchone())
