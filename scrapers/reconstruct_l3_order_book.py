import sqlite3
import json


def main():
    db = sqlite3.connect('./l3_order_book.db')

    db.row_factory = sqlite3.Row

    db.set_trace_callback(print)

    db.execute("create temp table orders (market text, account text, side text, id text, price real, size real, primary key (market, side, id)) without rowid")

    db.execute('drop table if exists snapshots')

    db.execute('create table snapshots (slot integer, timestamp text, market text, account text, side text, id text, price real, size real, primary key (timestamp, market, side, id) on conflict ignore)')

    for delta in db.execute("""
        select
            slot,
            timestamp,
            market,
            is_snapshot,
            json_group_array(json_object('account', account, 'side', side, 'id', order_id, 'price', price, 'size', size)) as orders
        from l3_order_book_deltas
        where local_timestamp between '2022-04-05' and '2022-04-09'
        group by timestamp, slot, market, is_snapshot
    """):
        if delta['is_snapshot']:
            db.execute('delete from orders where market = ?', [delta['market']])

        for order in json.loads(delta['orders']):
            if order['price'] == 0:
                db.execute('delete from orders where market = ? and side = ? and id = ?', [delta['market'], order['side'], order['id']])
            else:
                db.execute('insert into orders values (?, ?, ?, ?, ?, ?)', [delta['market'], order['account'], order['side'], order['id'], order['price'], order['size']])
        else:
            db.execute(
                'insert into snapshots select ? as slot, ? as timestamp, market, account, side, id, price, size from orders where market = ?',
                [delta['slot'], delta['timestamp'], delta['market']]
            )

    db.commit()


if __name__ == '__main__':
    main()
