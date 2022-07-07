import csv
import io
import json
import os
import re
import sqlite3
from datetime import timedelta, date
from dotenv import load_dotenv

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request, render_template, redirect, get_template_attribute, Response

from lib.market_makers import benchmark

load_dotenv()

app = Flask(__name__)

perpetuals = [
    'ADA-PERP',
    'AVAX-PERP',
    'BNB-PERP',
    'BTC-PERP',
    'ETH-PERP',
    'FTT-PERP',
    'MNGO-PERP',
    'RAY-PERP',
    'SOL-PERP',
    'SRM-PERP',
    'GMT-PERP'
]

spot = [
    'SOL/USDC',
    'BTC/USDC',
    'SRM/USDC',
    'MSOL/USDC',
    'AVAX/USDC',
    'ETH/USDC',
    'FTT/USDC',
    'RAY/USDC',
    'USDT/USDC',
    'MNGO/USDC',
    'COPE/USDC',
    'BNB/USDC',
    'GMT/USDC'
]


def regex_replace(value, pattern, repl):
    return re.sub(pattern, repl, value)


app.jinja_env.filters['regex_replace'] = regex_replace

@app.route('/')
def index():
    return redirect('/analytics/')


@app.route('/exchange')
def exchange():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute('select * from monthly_volumes')

    [monthly_volumes] = cur.fetchone()

    cur.execute('select * from monthly_volumes_by_instrument')

    [monthly_volumes_by_instrument] = cur.fetchone()

    return render_template(
        './exchange.html',
        monthly_volumes=monthly_volumes,
        monthly_volumes_by_instrument=monthly_volumes_by_instrument
    )

@app.route('/exchange/slippages')
def exchange_slippages():
    db = sqlite3.connect('./scripts/orderbooks_l2.db')

    db.execute("""
        create temp table quotes (
            exchange text,
            symbol text,
            size real,
            mid_price real,
            weighted_average_buy_price real,
            weighted_average_sell_price real,
            primary key (exchange, symbol, size)
        )
    """)

    order_sizes = [1000, 10000, 25000, 50000, 100000]

    db.executemany("""
        insert into quotes
        with
            orders as (
                select
                    exchange,
                    symbol,
                    side,
                    price,
                    size,
                    price * size as volume,
                    sum(price * size) over (partition by exchange, symbol, side order by case when side = 'bids' then - price when side = 'asks' then price end) as cumulative_volume
                from main.orders
                order by exchange, symbol, side, case when side = 'bids' then - price when side = 'asks' then price end
            ),
            fills as (
                select
                    exchange,
                    symbol,
                    side,
                    price,
                    fill,
                    sum(fill) over (
                        partition by side order by case when side = 'bids' then - price when side = 'asks' then price end
                    ) as cumulative_fill
                from (
                    select
                        exchange,
                        symbol,
                        side,
                        price,
                        case
                            when cumulative_volume < :size then volume
                            else coalesce(lag(remainder) over (partition by exchange, symbol, side), case when volume < :size then volume else :size end)
                        end as fill
                    from (select *, :size - cumulative_volume as remainder from orders)
                )
                where fill > 0
            ),
            weighted_average_fill_prices as (
                select
                    exchange,
                    symbol,
                    case when sum(case when side = 'asks' then fill end) = :size then sum(case when side = 'asks' then price * fill end) / :size end as weighted_average_buy_price,
                    case when sum(case when side = 'bids' then fill end) = :size then sum(case when side = 'bids' then price * fill end) / :size end as weighted_average_sell_price
                from fills
                group by exchange, symbol
            ),
            misc as (
                select
                    exchange,
                    symbol,
                    (top_bid + top_ask) / 2 as mid_price
                from (
                    select
                        exchange,
                        symbol,
                        max(price) filter ( where side = 'bids') as top_bid,
                        min(price) filter ( where side = 'asks') as top_ask
                    from orders
                    group by exchange, symbol
                )
            )
        select
            exchange,
            symbol,
            :size as size,
            mid_price,
            weighted_average_buy_price,
            weighted_average_sell_price
        from weighted_average_fill_prices inner join misc using (exchange, symbol)
    """, [[order_size] for order_size in order_sizes])

    data = db.execute("""
        with
            slippages as (
                select
                    exchange,
                    case when exchange = 'Serum DEX' then replace(symbol, '/', '-') else symbol end as symbol,
                    size,
                    ((weighted_average_buy_price - mid_price) / weighted_average_buy_price) * 100 as buy_slippage,
                    ((mid_price - weighted_average_sell_price) / mid_price) * 100 as sell_slippage
                from quotes
                order by exchange, symbol, size
            ),
            spreads as (
                select
                   exchange,
                   symbol,
                   json_group_array(json_array(size, buy_slippage, sell_slippage, buy_slippage + sell_slippage)) as spreads
                from slippages
                group by exchange, symbol
            )
        select
            exchange,
            json_group_array(json_array(symbol, json(spreads)))
        from spreads
        group by exchange
        order by case when exchange = 'Mango Markets perps' then 1
                      when exchange = 'FTX perps' then 2
                      when exchange = 'Mango Markets spot' then 3
                      when exchange = 'FTX spot' then 4 end;
    """).fetchall()

    spreads = [[exchange, json.loads(spreads)] for [exchange, spreads] in data]

    partial = get_template_attribute('./_exchange.html', 'spreads')

    return partial(spreads)

@app.route('/orderbooks/')
def orderbooks():
    return redirect('/analytics?instrument=SOL/USDC')


@app.route('/analytics/')
def analytics():
    instrument = request.args.get('instrument')

    if instrument is None:
        return redirect('/analytics?instrument=SOL-PERP')

    if instrument not in [*perpetuals, *spot]:
        return jsonify({'error': {'message': f"{instrument} is not a valid instrument"}}), 400

    return render_template(
        'orderbook.html',
        instrument=instrument,
        perpetuals=perpetuals,
        spot=spot
    )

@app.route('/liquidity')
def analytics_liquidity():
    symbol = request.args.get('symbol')

    db = sqlite3.connect('./daemons/analyze_orderbooks_l2.db')

    [results] = db.execute("""
        with
            depth as (
                select exchange
                     , symbol
                     , bids
                     , asks
                     , timestamp
                from main.depth
                where exchange in ('Mango Markets perps', 'Mango Markets spot')
                  and symbol = :symbol
            ),
            series as (
                select json_object('name', 'Bids', 'data', json_group_array(json_array(cast(strftime('%s', datetime("timestamp")) * 1e3 as int), bids))) as bids
                     , json_object('name', 'Asks', 'data', json_group_array(json_array(cast(strftime('%s', datetime("timestamp")) * 1e3 as int), asks))) as asks
                from depth
            )
        select
            json_array(bids, asks) as value
        from series;
    """, {'symbol': symbol}).fetchone()

    return results


@app.route('/slippages')
def analytics_slippages():
    symbol = request.args.get('symbol')

    db = sqlite3.connect('./daemons/analyze_orderbooks_l2.db')

    [results] = db.execute("""
        with
            slippages as (
                select exchange
                     , symbol
                     , order_size
                     , ((weighted_average_buy_price - mid_price) / weighted_average_buy_price) * 100 as buy_slippage
                     , ((mid_price - weighted_average_sell_price) / mid_price) * 100 as sell_slippage
                     , cast(strftime('%s', datetime("timestamp")) * 1e3 as int) as timestamp
                from quotes
                where exchange in ('Mango Markets perps', 'Mango Markets spot')
                  and symbol = :symbol
            ),
            slippages_by_order_size as (
                select exchange
                     , symbol
                     , order_size
                     , json_group_array(json_array("timestamp", buy_slippage, sell_slippage)) as slippages
                from slippages
                group by exchange, symbol, order_size
            )
        select
            json_group_array(
                json_array(
                    order_size,
                    slippages
                )
            ) as value
        from slippages_by_order_size;
    """, {'symbol': symbol}).fetchone()

    return results


@app.route('/historical_data/')
def historical_data():
    instrument = request.args.get('instrument')

    if instrument is None:
        return redirect('/historical_data?instrument=SOL-PERP')

    return render_template(
        'historical_data.html',
        instrument=instrument,
        perpetuals=perpetuals
    )


@app.route('/historical_data/trades')
def historical_data_trades():
    instrument = request.args.get('instrument')

    conn = psycopg2.connect("dbname=mangolorians")

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        select market
             , price
             , quantity
             , maker
             , maker_order_id
             , maker_client_order_id
             , taker
             , taker_order_id
             , taker_client_order_id
             , maker_fee
             , taker_fee
             , taker_side
             , timestamp at time zone 'utc' as "timestamp"
        from trades
        where market = %(market)s
        order by "timestamp" desc
        limit 9;
    """, {'market': instrument})

    trades = cur.fetchall()

    partial = get_template_attribute('_historical_data.html', 'trades')

    return partial(trades, instrument)


@app.route('/historical_data/trades.csv')
def historical_data_trades_csv():
    instrument = request.args.get('instrument')

    def stream():
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        conn = psycopg2.connect("dbname=mangolorians")

        cur = conn.cursor('cur')

        cur.execute("""
            select market
                 , price::float
                 , quantity::float
                 , maker
                 , maker_order_id
                 , maker_client_order_id
                 , taker
                 , taker_order_id
                 , taker_client_order_id
                 , maker_fee::float
                 , taker_fee::float
                 , taker_side
                 , (timestamp at time zone 'utc')::text as "timestamp"
            from trades
            where market = %(market)s
            order by "timestamp" desc
        """, {'market': instrument})

        headers = [
            'market', 'price', 'quantity', 'maker', 'maker_order_id', 'maker_client_order_id', 'taker',
            'taker_order_id', 'taker_client_order_id', 'maker_fee', 'taker_fee', 'taker_side', 'timestamp'
        ]

        writer.writerow(headers)

        yield buffer.getvalue().encode()

        buffer.seek(0)

        buffer.truncate()

        for row in cur:
            writer.writerow(row)

            yield buffer.getvalue().encode()

            buffer.seek(0)

            buffer.truncate()

    return Response(
        stream(),
        mimetype='text/csv',
        headers={
            'Content-Disposition': f"attachment; filename={instrument}_trades.csv"
        }
    )

@app.route('/historical_data/funding_rates')
def historical_data_funding_rates():
    instrument = request.args.get('instrument')

    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        select market
             , avg_funding_rate_pct
             , avg_oracle_price
             , avg_open_interest
             , "hour" at time zone 'utc' as "hour"
        from perp_funding_rates
        where market = %s
        order by "hour" desc
        limit 9;
    """, [instrument])

    funding_rates = cur.fetchall()

    partial = get_template_attribute('_historical_data.html', 'funding_rates')

    return partial(funding_rates, instrument)


@app.route('/historical_data/funding_rates.csv')
def historical_data_funding_rates_csv():
    instrument = request.args.get('instrument')

    def stream():
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        conn = psycopg2.connect('dbname=mangolorians')

        cur = conn.cursor('cur')

        cur.execute("""
            select market
                 , avg_funding_rate_pct
                 , avg_oracle_price
                 , avg_open_interest
                 , "hour" at time zone 'utc'
            from perp_funding_rates
            where market = %s
            order by "hour" desc
        """, [instrument])

        headers = ['market', 'avg_funding_rate_pct', 'avg_oracle_price', 'avg_open_interest', 'hour']

        writer.writerow(headers)

        yield buffer.getvalue().encode()

        buffer.seek(0)

        buffer.truncate()

        for row in cur:
            writer.writerow(row)

            yield buffer.getvalue().encode()

            buffer.seek(0)

            buffer.truncate()

    return Response(stream(), mimetype='text/csv',
        headers={
            'Content-Disposition': f"attachment; filename={instrument}_funding_rates.csv"
        }
    )

@app.route('/historical_data/liquidations')
def historical_data_liquidations():
    instrument = request.args.get('instrument')

    conn = psycopg2.connect("dbname=mangolorians")

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        select market
             , price
             , quantity
             , liquidatee
             , liquidator
             , liquidation_fee
             , timestamp
        from perp_liquidations
        where market = %(market)s
        order by market, "timestamp" desc
        limit 9;
    """, {'market': instrument})

    liquidations = cur.fetchall()

    partial = get_template_attribute('_historical_data.html', 'liquidations')

    return partial(liquidations, instrument)

@app.route('/historical_data/liquidations.csv')
def historical_data_liquidations_csv():
    instrument = request.args.get('instrument')

    def stream():
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        conn = psycopg2.connect('dbname=mangolorians')

        cur = conn.cursor('cur')

        cur.execute("""
            select market
                 , price
                 , quantity
                 , liquidatee
                 , liquidator
                 , liquidation_fee
                 , "timestamp"
            from perp_liquidations
            where market = %s
            order by "timestamp" desc
        """, [instrument])

        headers = ['market', 'price', 'quantity', 'liquidatee', 'liquidator', 'liquidation_fee', 'timestamp']

        writer.writerow(headers)

        yield buffer.getvalue().encode()

        buffer.seek(0)

        buffer.truncate()

        for row in cur:
            writer.writerow(row)

            yield buffer.getvalue().encode()

            buffer.seek(0)

            buffer.truncate()

    return Response(stream(), mimetype='text/csv',
        headers={
            'Content-Disposition': f"attachment; filename={instrument}_liquidations.csv"
        }
    )


@app.route('/market_makers')
def market_makers():
    account = request.args.get('account') or '4rm5QCgFPm4d37MCawNypngV4qPWv4D5tw57KE2qUcLE'

    symbol = request.args.get('symbol') or 'SOL-PERP'

    date = request.args.get('date') or '2022-06-13'

    [depth] = benchmark(symbol, account, date)

    return render_template(
        './market_makers.html',
        account=account,
        symbol=symbol,
        date=date,
        depth=depth,
        perpetuals=perpetuals
    )


@app.route('/positions')
def positions():
    instrument = request.args.get('instrument') or 'SOL-PERP'

    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute("""
        select max("timestamp")::text from positions where instrument = %s
    """, [instrument])

    [last_updated] = cur.fetchone()

    cur.execute("""
        select
            sum(abs(position_size)) / 2 as value
        from positions
        where instrument = %(instrument)s
    """, {'instrument': instrument})

    [oi] = cur.fetchone()

    cur.execute("""
        with
            oi as (
                select
                    sum(abs(position_size)) / 2 as oi
                from positions
                where instrument = %(instrument)s
            )
        select account
             , position_size
             , abs(position_size) / oi as oi_share
             , equity
             , assets
             , liabilities
             , leverage
             , init_health_ratio
             , maint_health_ratio
             , position_notional_size
             , market_percentage_move_to_liquidation
        from positions, oi
        where instrument = %(instrument)s
          and position_size > 0
        order by oi_share desc;
    """, {'instrument': instrument})

    longs = cur.fetchall()

    cur.execute("""
        with
            oi as (
                select
                    sum(abs(position_size)) / 2 as oi
                from positions
                where instrument = %(instrument)s
            )
        select account
             , position_size
             , abs(position_size) / oi as oi_share
             , equity
             , assets
             , liabilities
             , leverage
             , init_health_ratio
             , maint_health_ratio
             , position_notional_size
             , market_percentage_move_to_liquidation
        from positions, oi
        where instrument = %(instrument)s
          and position_size < 0
        order by oi_share desc;
    """, {'instrument': instrument})

    shorts = cur.fetchall()

    return render_template(
        './positions.html',
        perpetuals=perpetuals,
        instrument=instrument,
        oi=oi,
        longs=longs,
        shorts=shorts,
        last_updated=last_updated
    )


@app.route('/positions.csv')
def positions_csv():
    instrument = request.args.get('instrument') or 'SOL-PERP'

    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute("""
        select max("timestamp")::text from positions where instrument = %s and position_size != 0
    """, [instrument])

    [last_updated] = cur.fetchone()

    def stream():
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        cur.execute("""
            with
                oi as (
                    select
                        sum(abs(position_size)) / 2 as oi
                    from positions
                    where instrument = %(instrument)s 
                )
            select account
                 , position_size
                 , abs(position_size) / oi as oi_share
                 , equity
                 , assets
                 , liabilities
                 , leverage
                 , init_health_ratio
                 , maint_health_ratio
                 , position_notional_size
                 , market_percentage_move_to_liquidation
            from positions, oi
            where instrument = %(instrument)s
            order by oi_share desc;
        """, {'instrument': instrument})

        headers = [entry[0] for entry in cur.description]

        writer.writerow(headers)

        yield buffer.getvalue().encode()

        buffer.seek(0)

        buffer.truncate()

        for row in cur:
            writer.writerow(row)

            yield buffer.getvalue().encode()

            buffer.seek(0)

            buffer.truncate()

    return Response(
        stream(),
        mimetype='text/csv',
        headers={
            'Content-Disposition': f"attachment; filename={instrument}_positions_{last_updated}.csv"
        }
    )

@app.route('/balances')
def balances():
    instrument = request.args.get('instrument') or 'SOL'

    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute("""
        select sum(abs(deposits)) as total_deposits
             , sum(abs(borrows)) as total_borrows
        from balances
        where asset = %s;
    """, [instrument])

    [total_deposits, total_borrows] = cur.fetchone()

    cur.execute("""select max("timestamp")::text from balances where asset = %s""", [instrument])

    [last_updated] = cur.fetchone()

    cur.execute("""
        select account
             , net as net_balance
             , value as net_balance_usd
             , deposits
             , borrows
             , equity
             , assets
             , liabilities
             , leverage
             , init_health_ratio
             , maint_ealth_ratio
             , market_percentage_move_to_liquidation
        from balances
        where asset = %s
          and assets >= 50
        order by value desc;
    """, [instrument])

    balances = cur.fetchall()

    return render_template(
        './balances.html',
        spot=[*list(map(lambda instrument: instrument.split('/')[0], spot)), 'USDC'],
        instrument=instrument,
        balances=balances,
        total_deposits=total_deposits,
        total_borrows=total_borrows,
        last_updated=last_updated
    )

@app.route('/balances.csv')
def balances_csv():
    instrument = request.args.get('instrument') or 'SOL'

    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute("""select max("timestamp")::text from balances where asset = %s""", [instrument])

    [last_updated] = cur.fetchone()

    def stream():
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        cur.execute("""
            select account
                 , net as net_balance
                 , value as net_balance_usd
                 , deposits
                 , borrows
                 , equity
                 , assets
                 , liabilities
                 , leverage
                 , init_health_ratio
                 , maint_ealth_ratio
                 , market_percentage_move_to_liquidation
            from balances
            where asset = %s
              and assets >= 50
        """, [instrument])

        headers = [entry[0] for entry in cur.description]

        writer.writerow(headers)

        yield buffer.getvalue().encode()

        buffer.seek(0)

        buffer.truncate()

        for row in cur:
            writer.writerow(row)

            yield buffer.getvalue().encode()

            buffer.seek(0)

            buffer.truncate()

    return Response(
        stream(),
        mimetype='text/csv',
        headers={
            'Content-Disposition': f"attachment; filename={instrument}_balances_{last_updated}.csv"
        }
    )

@app.route('/volumes')
def volumes():
    instrument = request.args.get('instrument') or 'SOL/USDC'

    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    max_from = str(date.today() - timedelta(days=2))

    max_to = str(date.today() - timedelta(days=1))

    from_ = request.args.get('from') or max_from

    to = request.args.get('to') or max_to

    cur.execute("""
        with
            volumes as (
                select instrument
                     , mango_account
                     , count(*) as trades_count
                     , coalesce(sum(case when liquidity_type = 'maker' then volume end), 0)::bigint as maker_volume
                     , coalesce(sum(case when liquidity_type = 'taker' then volume end), 0)::bigint as taker_volume
                     , coalesce(sum(volume), 0)::bigint as total_volume
                from flux
                where created_at >= %(from)s and created_at < %(to)s
                group by instrument, mango_account
                order by total_volume desc
            )
        select json_agg(
                   json_build_array(
                       mango_account,
                       trades_count,
                       taker_volume,
                       maker_volume,
                       total_volume
                   )
               )
        from volumes
        where instrument = %(instrument)s
        group by instrument;
    """, {'instrument': instrument, 'from': from_, 'to': to})

    [volumes_by_mango_account] = cur.fetchone()

    cur.execute("""
        with
            volumes as (
                select instrument
                     , mango_account_owner
                     , count(*) as trades_count
                     , coalesce(sum(case when liquidity_type = 'maker' then volume end), 0)::bigint as maker_volume
                     , coalesce(sum(case when liquidity_type = 'taker' then volume end), 0)::bigint as taker_volume
                     , coalesce(sum(volume), 0)::bigint as total_volume
                from flux
                where created_at >= %(from)s and created_at < %(to)s
                group by instrument, mango_account_owner
                order by total_volume desc
            )
        select json_agg(
                   json_build_array(
                       mango_account_owner,
                       trades_count,
                       taker_volume,
                       maker_volume,
                       total_volume
                   )
               )
        from volumes
        where instrument = %(instrument)s
        group by instrument;
    """, {'instrument': instrument, 'from': from_, 'to': to})

    [volumes_by_signer] = cur.fetchone()

    cur.execute("""
        with
            volumes as (
                select instrument
                     , mango_account_delegate
                     , count(*) as trades_count
                     , coalesce(sum(case when liquidity_type = 'maker' then volume end), 0)::bigint as maker_volume
                     , coalesce(sum(case when liquidity_type = 'taker' then volume end), 0)::bigint as taker_volume
                     , coalesce(sum(volume), 0)::bigint as total_volume
                from flux
                where created_at >= %(from)s and created_at < %(to)s
                group by instrument, mango_account_delegate
                order by total_volume desc
            )
        select json_agg(
                   json_build_array(
                       mango_account_delegate,
                       trades_count,
                       taker_volume,
                       maker_volume,
                       total_volume
                   )
               )
        from volumes
        where instrument = %(instrument)s
        group by instrument;
    """, {'instrument': instrument, 'from': from_, 'to': to})

    [volumes_by_delegate] = cur.fetchone()

    cur.execute("""
        with
            volumes as (
                select instrument
                     , mango_account_referrer
                     , count(*) as trades_count
                     , coalesce(sum(case when liquidity_type = 'maker' then volume end), 0)::bigint as maker_volume
                     , coalesce(sum(case when liquidity_type = 'taker' then volume end), 0)::bigint as taker_volume
                     , coalesce(sum(volume), 0)::bigint as total_volume
                from flux
                where created_at >= %(from)s and created_at < %(to)s
                group by instrument, mango_account_referrer
                order by total_volume desc
            )
        select json_agg(
                   json_build_array(
                       mango_account_referrer,
                       trades_count,
                       taker_volume,
                       maker_volume,
                       total_volume,
                       referrer_ids
                   )
               )
        from volumes
        left join referrers on volumes.mango_account_referrer = referrers.mango_account
        where instrument = %(instrument)s
        group by instrument;
    """, {'instrument': instrument, 'from': from_, 'to': to})

    [volumes_by_referrer] = cur.fetchone()

    return render_template(
        './volumes.html',
        instrument=instrument,
        perpetuals=sorted(perpetuals),
        spot=sorted(spot),
        volumes_by_mango_account=volumes_by_mango_account,
        volumes_by_signer=volumes_by_signer,
        volumes_by_delegate=volumes_by_delegate,
        volumes_by_referrer=volumes_by_referrer,
        from_=from_,
        to=to,
        max_from=max_from,
        max_to=max_to
    )


@app.route('/aprs')
def aprs():
    instrument = request.args.get('instrument') or 'SOL'

    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute("""
        with
            points as (
                select
                    extract(epoch from hour)::int * 1e3 as hour,
                    avg_deposit_rate_pct,
                    avg_borrow_rate_pct,
                    total_deposits,
                    total_borrows
                from aprs
                where symbol = %s
                order by hour
            )
        select json_agg(json_build_array(
                    hour,
                    avg_deposit_rate_pct,
                    avg_borrow_rate_pct,
                    total_deposits,
                    total_borrows
                )) as aprs
        from points
    """, [instrument])

    [aprs] = cur.fetchone()

    return render_template(
        './aprs.html',
        instrument=instrument,
        spot=[*list(map(lambda instrument : instrument.split('/')[0], spot)), 'USDC'],
        aprs=aprs,
    )


@app.route('/competitions')
def competitions():
    conn = psycopg2.connect(os.getenv('TRADE_HISTORY_DB_CREDS'))

    cur = conn.cursor()

    threshold = int(request.args.get('threshold')) if request.args.get('threshold') else 100

    pool = int(request.args.get('pool')) if request.args.get('pool') else 20000

    cur.execute("""
        with
            params(threshold, pool) as (
                values(%(threshold)s, %(pool)s)
            ),
            trades as (
                select
                    "openOrders" as open_orders_account,
                    case when maker then 'maker' when not maker then 'taker' end as type,
                    abs(cast(case when bid then "nativeQuantityPaid" when not bid then "nativeQuantityReleased" end as bigint) / 1e6) as volume
                from event
                left join owner using ("openOrders")
                where owner = '9BVcYqEQxyccuwznvxXqDkSJFavvTyheiTYk231T1A8S'
                  and "quoteCurrency" = 'USDC'
                  and fill
                  and "loadTimestamp" >= '2022-07-04 12:00:00'
            ),
            volume_by_open_orders_account as (
                select
                    mango_account,
                    type,
                    sum(volume) as mango_account_volume
                from trades
                left join transactions_v3.open_orders_account using (open_orders_account)
                group by mango_account, type
            ),
            volumes as (
                select mango_account,
                       type,
                       mango_account_volume,
                       sum(mango_account_volume) over (partition by type) as total_volume_by_type
                from volume_by_open_orders_account),
            volumes_with_meta as (
                select
                    *,
                    sum(mango_account_volume) filter ( where qualifies ) over (partition by type) as qualifying_volume
                from volumes
                    cross join lateral (select mango_account_volume / total_volume_by_type as ratio_to_total_volume) as alpha
                    cross join lateral (select ratio_to_total_volume > ((select threshold from params) / 1e4) as qualifies) as beta
            )
        select mango_account,
               type,
               mango_account_volume,
               total_volume_by_type,
               ratio_to_total_volume,
               qualifies,
               qualifying_volume,
               ratio_to_qualifying_by_type_volume,
               coalesce((select pool from params) * ratio_to_qualifying_by_type_volume, 0) as srm_payout
        from volumes_with_meta
            cross join lateral (
                select
                    case
                        when qualifies
                        then mango_account_volume / qualifying_volume
                    end as ratio_to_qualifying_by_type_volume
            ) as alpha
        order by type, mango_account_volume desc;
    """, {'threshold': threshold, 'pool': pool})

    competitors = cur.fetchall()

    makers = [competitor for competitor in competitors if competitor[1] == 'maker']

    takers = [competitor for competitor in competitors if competitor[1] == 'taker']

    return render_template(
        './competitions.html',
        makers=makers,
        takers=takers,
        threshold=threshold,
        pool=pool
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0')
