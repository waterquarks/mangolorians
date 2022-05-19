import sqlite3
import psycopg2
import psycopg2.extras


def benchmark(market, account, target_depth, target_spread, date):
    db = sqlite3.connect(':memory:')

    db.execute('pragma journal_mode=WAL')

    db.execute('pragma synchronous=1')

    db.execute("""
        create table if not exists spreads (
            market text,
            account text,
            target_depth integer,
            target_spread real,
            bids real,
            asks real,
            spread real,
            has_target_spread integer,
            has_any_spread integer,
            slot integer,
            "timestamp" text,
            primary key (market, account, target_depth, "timestamp")
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

    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor('cur')

    query = """
        select market
             , is_snapshot
             , json_build_object(
                'bids', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'buy' and account = %(account)s ), json_build_array()),
                'asks', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'sell' and account = %(account)s ), json_build_array())
               ) as orders
             , slot
             , "timestamp"
        from mango_bowl.level3_deltas
        where market = %(market)s and date_trunc('day', local_timestamp at time zone 'utc') = %(date)s
        group by market, is_snapshot, slot, "timestamp"
        order by market, "timestamp";
    """

    cur.execute(query, {'market': market, 'account': account, 'date': date})

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
                insert or replace into spreads
                with
                    orders as (
                        select
                            side,
                            price,
                            sum(size) as size,
                            price * sum(size) as volume,
                            sum(price * sum(size)) over (partition by side order by case when side = 'bids' then - price when side = 'asks' then price end) as cumulative_volume
                        from main.orders
                        where market = :market and account = :account
                        group by side, price
                        order by side, case when side = 'bids' then - price when side = 'asks' then price end
                    ),
                    fills as (
                        select
                            side, price, fill, sum(fill) over (partition by side order by case when side = 'bids' then - price when side = 'asks' then price end) as cumulative_fill
                        from (
                            select
                                side,
                                price,
                                case when cumulative_volume < :target_depth then volume else coalesce(lag(remainder) over (partition by side), case when volume < :target_depth then volume else :target_depth end) end as fill
                            from (select *, :target_depth - cumulative_volume as remainder from orders) as alpha
                        ) as beta
                        where fill > 0
                    ),
                    weighted_average_quotes as (
                        select
                            case when sum(case when side = 'bids' then fill end) = :target_depth then sum(case when side = 'bids' then price * fill end) / :target_depth end as weighted_average_bid,
                            case when sum(case when side = 'asks' then fill end) = :target_depth then sum(case when side = 'asks' then price * fill end) / :target_depth end as weighted_average_ask,
                            coalesce(sum(case when side = 'bids' then fill end), 0) > 0 and coalesce(sum(case when side = 'asks' then fill end), 0) > 0 as has_any_spread
                        from fills
                    ),
                    spreads as (
                        select
                            weighted_average_bid,
                            weighted_average_ask,
                            ((weighted_average_ask - weighted_average_bid) / weighted_average_ask) * 1e2 as spread,
                            has_any_spread
                        from weighted_average_quotes
                    ),
                    depth as (
                        select
                            coalesce(sum(price * size) filter ( where side = 'bids' ), 0) as bids,
                            coalesce(sum(price * size) filter ( where side = 'asks' ), 0) as asks
                        from orders
                    )
                select
                    :market as market,
                    :account as account,
                    :target_depth as target_depth,
                    :target_spread as target_spread,
                    bids,
                    asks,
                    spread,
                    spread <= :target_spread as has_target_spread,
                    has_any_spread,
                    :slot as slot,
                    :timestamp as "timestamp"
                from spreads, depth
            """, {
                'account': account,
                'market': market,
                'target_depth': target_depth,
                'target_spread': target_spread,
                'slot': slot,
                'timestamp': timestamp
            })

    db.commit()

    [metrics, slots, slots_with_target_spread, slots_with_any_spread] = db.execute("""
        with
            avg_metrics_per_minute as (
                select market
                     , account
                     , target_depth
                     , target_spread
                     , avg(bids) as bids
                     , avg(asks) as asks
                     , avg(spread) as spread
                     , strftime('%Y-%m-%dT%H:%M:00Z', "timestamp") as minute
                from spreads
                group by market, account, target_depth, target_spread, minute
            ),
            overview as (
                select
                    count(slot) as slots,
                    count(slot) filter ( where has_target_spread ) as slots_with_target_spread,
                    count(slot) filter ( where has_any_spread ) as slots_with_any_spread
                from spreads
            )
        select
            json_group_array(
                json_array(cast(strftime('%s', minute) * 1e3 as integer), bids, asks, spread)
            ) as metrics,
            slots,
            slots_with_target_spread,
            slots_with_any_spread
        from avg_metrics_per_minute, overview;
    """).fetchone()

    return [metrics, slots, slots_with_target_spread, slots_with_any_spread]


if __name__ == '__main__':
    benchmark('SOL-PERP', '2Fgjpc7bp9jpiTRKSVSsiAcexw8Cawbz7GLJu8MamS9q', 50000, 0.2)