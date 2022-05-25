import psycopg2

def main():
    conn = psycopg2.connect('dbname=mangolorians')

    cur1 = conn.cursor()

    cur1.execute("""
        create table if not exists mango_bowl.depth (
            market text,
            bids numeric,
            asks numeric,
            slot integer,
            "timestamp" timestamptz,
            primary key (market, "timestamp")
        );
    """)

    conn.commit()

    cur2 = conn.cursor('cur2')

    cur2.execute("""
        select market
             , is_snapshot
             , json_build_object(
                'bids', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'buy' ), json_build_array()),
                'asks', coalesce(json_agg(json_build_array(account, order_id, price, size)) filter ( where side = 'sell' ), json_build_array())
               ) as orders
             , slot
             , "timestamp"
        from mango_bowl.level3_deltas
        where date_trunc('day', local_timestamp at time zone 'utc') = '2022-05-14'
        group by market, is_snapshot, slot, "timestamp"
        order by market, "timestamp";
    """)

    for row in cur2:
        print(row)

if __name__ == '__main__':
    main()