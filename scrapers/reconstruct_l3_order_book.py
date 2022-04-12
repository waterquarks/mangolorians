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
                avg(((weighted_average_ask - weighted_average_bid) / weighted_average_ask) * 100) as relative_spread,
                min(weighted_average_ask - weighted_average_bid) as min_absolute_spread,
                min(((weighted_average_ask - weighted_average_bid) / weighted_average_ask) * 100) as min_relative_spread,
                max(weighted_average_ask - weighted_average_bid) as max_absolute_spread,
                max(((weighted_average_ask - weighted_average_bid) / weighted_average_ask) * 100) as max_relative_spread
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
                    'relative_spread', relative_spread,
                    'min_absolute_spread', min_absolute_spread,
                    'min_relative_spread', min_relative_spread,
                    'max_absolute_spread', max_absolute_spread,
                    'max_relative_spread', max_relative_spread
                )
            )
        from entries
    """).fetchone()[0]

    return liquidity

def benchmark(instrument, accounts, target_liquidity, target_spread, from_, to):
    db = sqlite3.connect(':memory:')

    db.execute(f"attach database '{str(Path(__file__).parent / 'l3_deltas.db')}' as source")

    db.row_factory = sqlite3.Row

    db.execute("create table orders (market text, account text, side text, id text, price real, size real, primary key (market, side, id)) without rowid")

    db.execute("""
        create table spreads (
            timestamp text,
            weighted_average_bid real,
            weighted_average_ask real,
            absolute_spread real,
            relative_spread real,
            active integer,
            compliant integer,
            primary key (timestamp)
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
                with
                orders as (
                    select
                        side,
                        price,
                        sum(size) as size,
                        price * sum(size) as volume,
                        sum(price * sum(size)) over (partition by side order by case when side = 'buy' then - price when side = 'sell' then price end) as cumulative_volume
                    from main.orders
                    group by side, price
                    order by side, case when side = 'buy' then - price when side = 'sell' then price end
                ),
                fills as (
                    select
                        side, price, fill, sum(fill) over (partition by side order by case when side = 'buy' then - price when side = 'sell' then price end) as cumulative_fill
                    from (
                        select
                            side,
                            price,
                            case when cumulative_volume < :target_liquidity then volume else coalesce(lag(remainder) over (partition by side), case when volume < :target_liquidity then volume else :target_liquidity end) end as fill
                        from (select *, :target_liquidity - cumulative_volume as remainder from orders)
                    )
                    where fill > 0
                ),
                weighted_average_quotes as (
                    select
                        case when sum(case when side = 'buy' then fill end) = :target_liquidity then sum(case when side = 'buy' then price * fill end) / :target_liquidity end as weighted_average_bid,
                        case when sum(case when side = 'sell' then fill end) = :target_liquidity then sum(case when side = 'sell' then price * fill end) / :target_liquidity end as weighted_average_ask
                    from fills
                ),
                spreads as (
                    select
                        weighted_average_bid,
                        weighted_average_ask,
                        weighted_average_ask - weighted_average_bid as absolute_spread,
                        ((weighted_average_ask - weighted_average_bid) / weighted_average_ask) * 1e2 as relative_spread
                    from weighted_average_quotes
                )
                select
                    :timestamp as timestamp,
                    weighted_average_bid,
                    weighted_average_ask,
                    absolute_spread,
                    relative_spread,
                    absolute_spread is not null as active,
                    relative_spread <= :target_spread as compliant
                from spreads
            """, {'timestamp': delta['timestamp'], 'target_liquidity': target_liquidity, 'target_spread': target_spread})

    db.commit()

    return dict(db.execute("""
        with
            ticks as (
                select
                    timestamp,
                    timestamp between strftime('%Y-%m-%dT%H:%M:%S', datetime(strftime('%s', current_timestamp) - 3600, 'unixepoch')) and strftime('%Y-%m-%dT%H:%M:%S', current_timestamp) as within,
                    coalesce(julianday(timestamp) - julianday(lag(timestamp) over (order by timestamp)), 0) as delta,
                    weighted_average_bid,
                    weighted_average_ask,
                    absolute_spread,
                    relative_spread,
                    active,
                    compliant
                from spreads where timestamp between :from and :to
            ),
            uptime as (
                select
                    elapsed,
                    absolute_uptime,
                    absolute_uptime / elapsed as relative_uptime,
                    compliant_absolute_uptime,
                    compliant_absolute_uptime / elapsed as compliant_relative_uptime
                from (
                     select
                        julianday(:to) - julianday(:from) as elapsed,
                        sum(delta) filter (where active) as absolute_uptime,
                        sum(delta) filter (where compliant) as compliant_absolute_uptime
                    from ticks
                )
            ),
            metrics as (
                select
                    weighted_average_bid,
                    weighted_average_ask,
                    absolute_spread,
                    relative_spread
                from (
                     select
                        avg(weighted_average_bid) as weighted_average_bid,
                        avg(weighted_average_ask) as weighted_average_ask,
                        avg(absolute_spread) as absolute_spread,
                        avg(relative_spread) as relative_spread
                    from ticks where compliant
                )
            )
        select
            elapsed,
            absolute_uptime,
            relative_uptime,
            compliant_absolute_uptime,
            compliant_relative_uptime,
            weighted_average_bid,
            weighted_average_ask,
            absolute_spread,
            relative_spread
        from metrics, uptime;
    """, {'from': from_, 'to': to}).fetchone())
