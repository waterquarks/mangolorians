import sqlite3
import json


def main():
    db = sqlite3.connect('./spreads_gamma.db')

    [competitors] = db.execute("""
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
            ),
            tranches as (
                select
                    market,
                    json_group_array(
                        json_array (
                            account,
                            target_depth,
                            target_spread
                        )
                    ) as competitors
                from competitors
                inner join target_spreads using (market, target_depth)
                group by market
            )
        select json_group_object(market, competitors) as value from tranches;
    """).fetchone()

    competitors = json.loads(competitors)

    db.execute("""
        create table if not exists spreads (
            market text,
            account text,
            target_depth integer,
            target_spread real,
            spread real,
            has_target_spread_and_depth integer,
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
        where "timestamp" >= '2022-04-25'
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
            for [account, target_depth, target_spread] in competitors[market]:
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
                        )
                    select
                        :market as market,
                        :account as account,
                        :target_depth as target_depth,
                        :target_spread as target_spread,
                        spread,
                        spread <= :target_spread as has_target_spread,
                        has_any_spread,
                        :slot as slot,
                        :timestamp as "timestamp"
                    from spreads
                """, {
                    'account': account,
                    'market': market,
                    'target_depth': target_depth,
                    'target_spread': target_spread,
                    'slot': slot,
                    'timestamp': timestamp
                })

    db.commit()


if __name__ == '__main__':
    main()