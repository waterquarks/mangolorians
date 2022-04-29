import sqlite3
import json


def main():
    db = sqlite3.connect('./depth_eta.db')

    db.execute("""
        create table orders (
            market text,
            side text,
            order_id text,
            account text,
            price real,
            size real,
            primary key (market, side, order_id)
        ) without rowid
    """)

    db.execute('create index orders_idx_0 on orders (market, account, side)')

    db.execute("""
        create table depth (
            market text,
            account text,
            bids real,
            asks real,
            slot integer,
            "timestamp" text,
            primary key (market, account, "timestamp", slot)
        ) without rowid
    """)

    db.execute("""
        create table competitors (
            market text,
            account text,
            primary key (market, account)
        ) without rowid
    """)

    db.execute("""
        insert into competitors
        with
            competitors (account, market, target_depth) as (
                values  ('8QGxM5xTNE9BNzZDtC4eSPF7HLkPXmfdqQKGCa36p1C6', 'SOL-PERP', 12500),
                        ('8oJf4NEi6XcSgJpDTDMRdaT8yN4uqtshpQB7eJ1cBG7B', 'ADA-PERP', 12500),
                        ('8xbfcpiTd3jt1z3wtUteyxK4cDjx1CBC3zzyG3hPFD4B', 'FTT-PERP', 12500),
                        ('8oJf4NEi6XcSgJpDTDMRdaT8yN4uqtshpQB7eJ1cBG7B', 'SRM-PERP', 12500),
                        ('8xbfcpiTd3jt1z3wtUteyxK4cDjx1CBC3zzyG3hPFD4B', 'BNB-PERP', 12500),
                        ('8xbfcpiTd3jt1z3wtUteyxK4cDjx1CBC3zzyG3hPFD4B', 'RAY-PERP', 12500),
                        ('8xbfcpiTd3jt1z3wtUteyxK4cDjx1CBC3zzyG3hPFD4B', 'MNGO-PERP', 12500),
                        ('GJDMYqhT2XPxoUDk3bczDivcE2FgmEeBzkpmcaRNWP3', 'BTC-PERP', 25000),
                        ('CGcrpkxyx92vjyQApsr1jTN6M5PeERKSEaH1zskzccRG', 'BTC-PERP', 25000),
                        ('GJDMYqhT2XPxoUDk3bczDivcE2FgmEeBzkpmcaRNWP3', 'ETH-PERP', 25000),
                        ('CGcrpkxyx92vjyQApsr1jTN6M5PeERKSEaH1zskzccRG', 'ETH-PERP', 25000),
                        ('GJDMYqhT2XPxoUDk3bczDivcE2FgmEeBzkpmcaRNWP3', 'SOL-PERP', 25000),
                        ('CGcrpkxyx92vjyQApsr1jTN6M5PeERKSEaH1zskzccRG', 'SOL-PERP', 25000),
                        ('CGcrpkxyx92vjyQApsr1jTN6M5PeERKSEaH1zskzccRG', 'AVAX-PERP', 25000),
                        ('4rm5QCgFPm4d37MCawNypngV4qPWv4D5tw57KE2qUcLE', 'AVAX-PERP', 25000),
                        ('CGcrpkxyx92vjyQApsr1jTN6M5PeERKSEaH1zskzccRG', 'LUNA-PERP', 25000),
                        ('4rm5QCgFPm4d37MCawNypngV4qPWv4D5tw57KE2qUcLE', 'LUNA-PERP', 25000),
                        ('CGcrpkxyx92vjyQApsr1jTN6M5PeERKSEaH1zskzccRG', 'ADA-PERP', 25000),
                        ('CGcrpkxyx92vjyQApsr1jTN6M5PeERKSEaH1zskzccRG', 'FTT-PERP', 25000),
                        ('CGcrpkxyx92vjyQApsr1jTN6M5PeERKSEaH1zskzccRG', 'SRM-PERP', 25000),
                        ('CGcrpkxyx92vjyQApsr1jTN6M5PeERKSEaH1zskzccRG', 'BNB-PERP', 25000),
                        ('CGcrpkxyx92vjyQApsr1jTN6M5PeERKSEaH1zskzccRG', 'RAY-PERP', 25000),
                        ('CGcrpkxyx92vjyQApsr1jTN6M5PeERKSEaH1zskzccRG', 'MNGO-PERP', 25000),
                        ('DUp1NPypVm32hniFLY7KqCRtNQ6dM7ipQbTfuXh48gDt', 'BTC-PERP', 50000),
                        ('DUp1NPypVm32hniFLY7KqCRtNQ6dM7ipQbTfuXh48gDt', 'ETH-PERP', 50000),
                        ('DUp1NPypVm32hniFLY7KqCRtNQ6dM7ipQbTfuXh48gDt', 'SOL-PERP', 50000),
                        ('DUp1NPypVm32hniFLY7KqCRtNQ6dM7ipQbTfuXh48gDt', 'AVAX-PERP', 50000),
                        ('DUp1NPypVm32hniFLY7KqCRtNQ6dM7ipQbTfuXh48gDt', 'LUNA-PERP', 50000),
                        ('DUp1NPypVm32hniFLY7KqCRtNQ6dM7ipQbTfuXh48gDt', 'ADA-PERP', 50000),
                        ('DUp1NPypVm32hniFLY7KqCRtNQ6dM7ipQbTfuXh48gDt', 'FTT-PERP', 50000),
                        ('DUp1NPypVm32hniFLY7KqCRtNQ6dM7ipQbTfuXh48gDt', 'SRM-PERP', 50000),
                        ('DUp1NPypVm32hniFLY7KqCRtNQ6dM7ipQbTfuXh48gDt', 'BNB-PERP', 50000),
                        ('DUp1NPypVm32hniFLY7KqCRtNQ6dM7ipQbTfuXh48gDt', 'RAY-PERP', 50000),
                        ('DUp1NPypVm32hniFLY7KqCRtNQ6dM7ipQbTfuXh48gDt', 'MNGO-PERP', 50000),
                        ('4rm5QCgFPm4d37MCawNypngV4qPWv4D5tw57KE2qUcLE', 'BTC-PERP', 50000),
                        ('4rm5QCgFPm4d37MCawNypngV4qPWv4D5tw57KE2qUcLE', 'ETH-PERP', 50000),
                        ('4rm5QCgFPm4d37MCawNypngV4qPWv4D5tw57KE2qUcLE', 'SOL-PERP', 50000),
                        ('2Fgjpc7bp9jpiTRKSVSsiAcexw8Cawbz7GLJu8MamS9q', 'BTC-PERP', 50000),
                        ('2Fgjpc7bp9jpiTRKSVSsiAcexw8Cawbz7GLJu8MamS9q', 'ETH-PERP', 50000),
                        ('2Fgjpc7bp9jpiTRKSVSsiAcexw8Cawbz7GLJu8MamS9q', 'SOL-PERP', 50000),
                        ('2Fgjpc7bp9jpiTRKSVSsiAcexw8Cawbz7GLJu8MamS9q', 'AVAX-PERP', 50000),
                        ('2Fgjpc7bp9jpiTRKSVSsiAcexw8Cawbz7GLJu8MamS9q', 'LUNA-PERP', 50000),
                        ('9SrfQKierpcqo69SxPu12wPKELGCRLS3UkU1A5fcvGmn', 'BTC-PERP', 50000),
                        ('8oJf4NEi6XcSgJpDTDMRdaT8yN4uqtshpQB7eJ1cBG7B', 'ETH-PERP', 50000),
                        ('BBPJ9PC2HnTM3uLNiLtwukcwMDg4ejS2yfeynYKRWdWH', 'SOL-PERP', 50000),
                        ('8oJf4NEi6XcSgJpDTDMRdaT8yN4uqtshpQB7eJ1cBG7B', 'AVAX-PERP', 50000),
                        ('8oJf4NEi6XcSgJpDTDMRdaT8yN4uqtshpQB7eJ1cBG7B', 'LUNA-PERP', 50000),
                        ('3CTtZ55HfK4VdwXn4K54W5mdwjp4rg2E28BkRohQZa9d', 'SOL-PERP', 12500),
                        ('8oJf4NEi6XcSgJpDTDMRdaT8yN4uqtshpQB7eJ1cBG7B', 'SOL-PERP', 500000),
                        ('DUp1NPypVm32hniFLY7KqCRtNQ6dM7ipQbTfuXh48gDt', 'SOL-PERP', 500000),
                        ('4qoYohoLH69gG8mp1Wk2hkKPN5NnfX7BHZYZFC359eva', 'SOL-PERP', 500000),
                        ('Gd35MmNfz3dR7Ux1XDYFSRWeHFtAbjw44AmZ7PvXeZ7k', 'SOL-PERP', 500000),
                        ('2Fgjpc7bp9jpiTRKSVSsiAcexw8Cawbz7GLJu8MamS9q', 'SOL-PERP', 500000)
            ),
            target_spreads (market, target_depth, target_spread) as (
                    values  ('BTC-PERP', 12500, 0.07),
                            ('ETH-PERP', 12500, 0.07),
                            ('SOL-PERP', 12500, 0.1),
                            ('AVAX-PERP', 12500, 0.13),
                            ('LUNA-PERP', 12500, 0.13),
                            ('ADA-PERP', 12500, 0.22),
                            ('FTT-PERP', 12500, 0.36),
                            ('SRM-PERP', 12500, 0.36),
                            ('BNB-PERP', 12500, 0.36),
                            ('RAY-PERP', 12500, 0.72),
                            ('MNGO-PERP', 12500, 3),
                            ('BTC-PERP', 25000, 0.07),
                            ('ETH-PERP', 25000, 0.07),
                            ('SOL-PERP', 25000, 0.1),
                            ('AVAX-PERP', 25000, 0.13),
                            ('LUNA-PERP', 25000, 0.13),
                            ('ADA-PERP', 25000, 0.22),
                            ('FTT-PERP', 25000, 0.36),
                            ('SRM-PERP', 25000, 0.36),
                            ('BNB-PERP', 25000, 0.36),
                            ('RAY-PERP', 25000, 0.72),
                            ('MNGO-PERP', 25000, 3),
                            ('BTC-PERP', 50000, 0.07),
                            ('ETH-PERP', 50000, 0.07),
                            ('SOL-PERP', 50000, 0.1),
                            ('AVAX-PERP', 50000, 0.13),
                            ('LUNA-PERP', 50000, 0.13),
                            ('ADA-PERP', 50000, 0.22),
                            ('FTT-PERP', 50000, 0.36),
                            ('SRM-PERP', 50000, 0.36),
                            ('BNB-PERP', 50000, 0.36),
                            ('RAY-PERP', 50000, 0.72),
                            ('MNGO-PERP', 50000, 3),
                            ('SOL-PERP', 500000, 2)
            )
            select distinct
                market,
                account
            from competitors
            inner join target_spreads using (market, target_depth)
    """).fetchone()

    db.execute("attach database './mango_bowl.l3_deltas.db' as source")

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
        else:
            db.execute("""
                insert or replace into depth
                select
                    market,
                    account,
                    sum(price * size) filter ( where side = 'bids' ) as bids,
                    sum(price * size) filter ( where side = 'asks' ) as asks,
                    :slot as slot,
                    :timestamp as timestamp
                from orders
                inner join competitors using (market, account)
                group by market, account
            """,
            {'market': market, 'slot': slot, 'timestamp': timestamp})

    db.commit()


if __name__ == '__main__':
    main()