import sqlite3
import psycopg2
import psycopg2.extras


def main(market, account, target_depth, target_spread):
    db = sqlite3.connect('./a1.test')

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
        with
             entries as (
                 select content ->> 'market' as market
                      , content ->> 'type' = 'l3snapshot' as is_snapshot
                      , content
                      , (content ->> 'slot')::integer as slot
                      , (content ->> 'timestamp')::timestamptz at time zone 'utc' as "timestamp"
                      , local_timestamp
                 from mango_bowl.level3
                 where date_trunc('hour', local_timestamp at time zone 'utc') = date_trunc('hour', current_timestamp at time zone 'utc' - interval '1 hour')
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
                'bids', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'buy' and account = %(account)s ), json_build_array()),
                'asks', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'sell' and account = %(account)s ), json_build_array())
             ) as orders
             , slot
             , "timestamp"
        from collapsed
        where market = %(market)s
        group by market, is_snapshot, slot, "timestamp"
        order by market, "timestamp";
    """

    cur.execute(query, {'market': market, 'account': account})

    for market, is_snapshot, orders, slot, timestamp in cur:
        print(market, is_snapshot, orders)

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

    depth = db.execute("""
        with
             avg_depth_per_minute as (
                select
                    strftime('%Y-%m-%dT%H:%M:00Z', "timestamp") as minute,
                    avg(bids) as bids,
                    avg(asks) as asks
                from spreads
                group by minute
            )
        select
            json_group_array(
                json_array(
                    cast(strftime('%s', minute) * 1e3 as int),
                    bids,
                    asks
                )
            ) as depth
        from avg_depth_per_minute;
    """)


if __name__ == '__main__':
    main('SOL-PERP', '2Fgjpc7bp9jpiTRKSVSsiAcexw8Cawbz7GLJu8MamS9q', 50000, 0.2)