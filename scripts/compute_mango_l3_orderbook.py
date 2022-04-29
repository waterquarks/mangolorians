import sqlite3
import json


def main():
    db = sqlite3.connect(':memory:')

    db.execute('create table orders (market text, side text, order_id text, account text, price real, size real, primary key (market, side, order_id)) without rowid')

    db.execute("attach database './mango_bowl.l3_deltas_old.db' as source")

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
        from deltas where market = 'SOL-PERP'
        group by market, "timestamp"
        order by market, "timestamp"
    """):
        if slot == 131274994:
            breakpoint()

        if is_snapshot:
            db.execute('delete from orders where market = ?', [market])

        orders = json.loads(orders)

        for side in {'bids', 'asks'}:
            for order_id, account, price, size in orders[side]:
                if price == 0:
                    db.execute('delete from orders where market = ? and side = ? and order_id = ?', [market, side, order_id])
                else:
                    db.execute('insert or replace into orders values (?, ?, ?, ?, ?, ?)', [market, side, order_id, account, price, size])

    db.commit()


if __name__ == '__main__':
    main()
