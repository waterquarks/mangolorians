import sqlite3
import json
import psycopg2
import psycopg2.extras
from pathlib import Path

def main():
    db = sqlite3.connect(f"{str(Path(__file__).parent / 'mm_competitions_week_4.db')}")

    db.execute('pragma journal_mode=WAL')

    db.execute('pragma synchronous=1')

    uptimes = list(db.execute("""
        with
            params("from", "to") as (
                values ('2022-05-16 12:00:00', strftime('%Y-%m-%d %H:00:00', datetime(current_timestamp)))
            ),
            ticks as (
                select
                    market,
                    account,
                    target_depth,
                    spread,
                    has_target_spread,
                    has_any_spread,
                    slot,
                    "timestamp" as "timestamp"
                from spreads
                where "timestamp" between (select "from" from params) and (select "to" from params)
                order by market, account, target_depth, "timestamp"
            ),
            diffs as (
                select
                    market,
                    account,
                    target_depth,
                    cast(count(slot) as real) as elapsed,
                    cast(count(slot) filter (where has_target_spread) as real) as uptime_with_target_spread,
                    cast(count(slot) filter (where has_any_spread) as real) as uptime_with_any_spread
                from ticks
                group by market, account, target_depth
                order by market, account, target_depth
            ),
            uptime as (
                select
                    market,
                    account,
                    target_depth,
                    elapsed,
                    uptime_with_target_spread,
                    (uptime_with_target_spread / elapsed) as uptime_with_target_spread_ratio,
                    uptime_with_any_spread,
                    (uptime_with_any_spread / elapsed) as uptime_with_any_spread_ratio
                from diffs
            )
        select market
             , account
             , target_depth
             , target_spread
             , target_uptime
             , uptime_with_target_spread_ratio * 100 as uptime_with_target_spread
             , uptime_with_any_spread_ratio * 100 as uptime_with_any_spread
        from uptime
        inner join target_spreads using (market, target_depth)
        inner join target_uptimes using (target_depth);
    """))

    remote = psycopg2.connect('dbname=mangolorians user=ioaquine password=anabasion host=mangolorians.com port=5432')

    cur = remote.cursor()

    cur.execute('drop table if exists mm_competitions_week_4')

    print(cur.query.decode('utf-8'))

    cur.execute('create table mm_competitions_week_4 (market text, account text, target_depth integer, target_spread float, target_uptime float, uptime_with_target_spread float, uptime_with_any_spread float)')

    psycopg2.extras.execute_values(cur, "insert into mm_competitions_week_4 values %s", uptimes)

    print(cur.query.decode('utf-8'))

    remote.commit()


if __name__ == '__main__':
    main()