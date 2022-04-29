import sqlite3
import json


def main():
    db = sqlite3.connect(':memory:')

    db.execute('create table orders (market text, side text, order_id text, account text, price real, size real, primary key (market, side, order_id)) without rowid')

    db.execute('create table meta (market text, slot integer, timestamp text, primary key (market)) without rowid')

    db.execute("attach database './mango_bowl.l3_deltas_old.db' as source")

    # This script an indexed full table scan, so I think it's best to pre-compute it     when testing
    #
    # create table breakpoints as
    # with
    #     anchors as (
    #         select
    #             market,
    #             slot,
    #             date(timestamp) as "date",
    #             min(timestamp) as "timestamp"
    #         from deltas
    #         group by market, "date"
    #         having not is_snapshot
    #     ),
    #     breakpoints as (
    #         select market, json_group_array(slot) as slots from anchors group by market
    #     )
    # select json_group_object(market, slots) as anchors from breakpoints;

    [anchors] = db.execute('select value from source.breakpoints').fetchone()

    anchors = json.loads(anchors)

    for market, is_snapshot, orders, slot, timestamp in db.execute("""
        select market,
               is_snapshot,
               json_object(
                   'bids', json_group_array(
                       json_array(
                               order_id,
                               account,
                               price,
                               size
                       )
                   ) filter ( where side = 'buy' ),
                   'asks', json_group_array(
                       json_array(
                               order_id,
                               account,
                               price,
                               size
                       )
                   ) filter ( where side = 'sell' )
               ) as orders,
               slot,
               "timestamp"
        from deltas
        group by market, "timestamp"
        order by market, "timestamp"
    """):
        if is_snapshot:
            db.execute('delete from orders where market = ?', [market])

        orders = json.loads(orders)

        for side in {'bids', 'asks'}:
            for order_id, account, price, size in orders[side]:
                if price == 0:
                    db.execute('delete from orders where market = ? and side = ? and order_id = ?', [market, side, order_id])
                else:
                    db.execute('insert or replace into orders values (?, ?, ?, ?, ?, ?)', [market, side, order_id, account, price, size])

        db.execute('insert or replace into meta values (?, ?, ?)', [market, slot, timestamp])

        if slot in anchors.get(market):
            print(market, slot, timestamp)

            db.execute('delete from source.deltas where market = ? and timestamp = ?', [market, timestamp])

            db.execute("""
                insert into source.deltas
                select
                    market,
                    1 as is_snapshot,
                    account,
                    case when side = 'bids' then 'buy' when side = 'asks' then 'sell' end, -- TODO: Don't have to do this
                    order_id,
                    price,
                    size,
                    :slot as slot,
                    :timestamp as "timestamp"
                from orders where market = :market
            """, {'market': market, 'slot': slot, 'timestamp': timestamp})

    db.commit()


if __name__ == '__main__':
    main()
