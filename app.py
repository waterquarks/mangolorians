import csv
import io
import sqlite3
import re
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta
from lib.market_makers import benchmark
from dotenv import load_dotenv
import os

from flask import Flask, jsonify, request, render_template, redirect, get_template_attribute, Response
import json

load_dotenv('./.env')

app = Flask(__name__)

perpetuals = {
    'ADA-PERP',
    'AVAX-PERP',
    'BNB-PERP',
    'BTC-PERP',
    'ETH-PERP',
    'FTT-PERP',
    'LUNA-PERP',
    'MNGO-PERP',
    'RAY-PERP',
    'SOL-PERP',
    'SRM-PERP',
    'GMT-PERP'
}

spot = {
    'SOL/USDC',
    'BTC/USDC',
    'SRM/USDC',
    'MSOL/USDC',
    'AVAX/USDC',
    'ETH/USDC',
    'LUNA/USDC',
    'FTT/USDC',
    'RAY/USDC',
    'USDT/USDC',
    'MNGO/USDC',
    'COPE/USDC',
    'BNB/USDC',
    'GMT-PERP'
}


def regex_replace(value, pattern, repl):
    return re.sub(pattern, repl, value)


app.jinja_env.filters['regex_replace'] = regex_replace

@app.route('/')
def index():
    return redirect('/analytics/')


@app.route('/exchange')
def exchange():
    return render_template('./exchange.html')

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

    order_sizes = [1000, 10000, 25000, 50000, 100000, 500000]

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
        order by case when exchange = 'Mango Markets' then 1 else exchange end;
    """).fetchall()

    spreads = [[exchange, json.loads(spreads)] for [exchange, spreads] in data]

    partial = get_template_attribute('./_exchange.html', 'spreads')

    return partial(spreads)


@app.route('/analytics/')
def analytics():
    instrument = request.args.get('instrument')

    if instrument is None:
        return redirect('/analytics?instrument=SOL-PERP')

    if instrument not in [*perpetuals, *spot]:
        return jsonify({'error': {'message': f"{instrument} is not a valid instrument"}}), 400

    if instrument in perpetuals:
        return render_template(
            'analytics/perpetuals.html',
            instrument=instrument,
            perpetuals=perpetuals,
            spot=spot
        )

    if instrument in spot:
        return render_template(
            'analytics/spot.html',
            instrument=instrument,
            perpetuals=perpetuals,
            spot=spot
        )


@app.route('/liquidity')
def analytics_liquidity():
    symbol = request.args.get('symbol')

    if symbol is None:
        return jsonify({'error': {'message': 'Please enter a symbol query parameter.'}}), 400

    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    results = list(map(dict, db.execute("""
        with
            average_liquidity_per_minute as (
                select
                    exchange,
                    symbol,
                    round(avg(buy)) as buy,
                    round(avg(sell)) as sell,
                    strftime('%Y-%m-%dT%H:%M:00Z', "timestamp") as minute
                from liquidity
                where exchange = 'Mango Markets'
                  and symbol = :symbol
                  and "timestamp" > datetime(current_timestamp, '-7 days')
                group by exchange, symbol, minute
                order by minute desc
            )
            select * from average_liquidity_per_minute order by minute
    """, {'symbol': symbol})))

    return jsonify(results)


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
                     , timestamp
                     , strftime('%Y-%m-%d %H:%M:00', timestamp) as minute
                from quotes
                where exchange = 'Mango Markets'
                  and symbol = :symbol
                  and order_size in (1000, 10000, 25000, 50000, 100000, 500000)
            ),
            slippages_with_mark as (
                select exchange
                     , symbol
                     , order_size
                     , buy_slippage
                     , sell_slippage
                     , timestamp
                     , coalesce(minute != lag(minute) over (partition by exchange, symbol, order_size order by "timestamp"), false) as show
                from slippages
            ),
            slippages_marked as (
                select exchange
                     , symbol
                     , order_size
                     , buy_slippage
                     , sell_slippage
                     , cast(strftime('%s', datetime("timestamp")) * 1e3 as int) as "timestamp"
                from slippages_with_mark
                where show
            ),
            slippages_normalized as (
                select order_size
                     , json_group_array(json_array("timestamp", buy_slippage, sell_slippage)) as slippages
                from slippages_marked
                group by exchange, symbol, order_size
            )
        select json_group_array(json_array(order_size, slippages)) as value from slippages_normalized;
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
             , funding_rate
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
                 , funding_rate
                 , avg_oracle_price
                 , avg_open_interest
                 , "hour" at time zone 'utc'
            from perp_funding_rates
            where market = %s
            order by "hour" desc
        """, [instrument])

        headers = ['market', 'funding_rate', 'avg_oracle_price', 'avg_open_interest', 'hour']

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

    market = request.args.get('instrument') or 'BTC-PERP'

    target_depth = int(request.args.get('target-depth') or 1000)

    target_spread = float(request.args.get('target-spread') or 0.3)

    date = request.args.get('date') or '2022-05-19'

    [metrics, slots, slots_with_target_spread, slots_with_any_spread] = benchmark(market, account, target_depth, target_spread, date)

    return render_template(
        './market_makers.html',
        account=account,
        instrument=market,
        target_depth=target_depth,
        target_spread=target_spread,
        date=date,
        perpetuals=perpetuals,
        metrics=metrics,
        slots=slots,
        slots_with_target_spread=slots_with_target_spread,
        slots_with_any_spread=slots_with_any_spread
    )


@app.route('/market_makers/metrics.csv')
def market_makers_spreads_csv():
    account = request.args.get('account') or '2Fgjpc7bp9jpiTRKSVSsiAcexw8Cawbz7GLJu8MamS9q'

    market = request.args.get('instrument') or 'BTC-PERP'

    target_depth = int(request.args.get('target_depth') or 12500)

    target_spread = request.args.get('target_spread') or 0.15

    from_ = request.args.get('from') or (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(timespec='minutes').replace('+00:00', '')

    to = request.args.get('to') or datetime.now(timezone.utc).isoformat(timespec='minutes').replace('+00:00', '')

    def stream():
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        db = sqlite3.connect('./scripts/spreads_gamma.db')

        db.set_trace_callback(print)

        cursor = db.cursor().execute("""
            select
                spread,
                slot,
                "timestamp"
            from spreads
            where market = :market
              and account = :account
              and target_depth = :target_depth
              and "timestamp" between :from and :to;
        """, {'market': market, 'account': account, 'target_depth': target_depth, 'from': from_, 'to': to})

        headers = [entry[0] for entry in cursor.description]

        writer.writerow(headers)

        yield buffer.getvalue().encode()

        buffer.seek(0)

        buffer.truncate()

        for row in cursor:
            writer.writerow(row)

            yield buffer.getvalue().encode()

            buffer.seek(0)

            buffer.truncate()

    return Response(
        stream(),
        mimetype='text/csv',
        headers={
            'Content-Disposition': f"attachment; filename={account}_{market}'s spread for ${target_depth}@{target_spread}%.csv"
        }
    )


@app.route('/positions')
def positions():
    instrument = request.args.get('instrument') or 'SOL-PERP'

    conn = psycopg2.connect(os.getenv('PSYCOPG_CONN'))

    cur = conn.cursor()

    cur.execute("""
        select max("timestamp")::text
        from consolidate where market = %s
         and position_size != 0
    """, [instrument])

    [last_updated] = cur.fetchone()

    cur.execute("""
        with
            latest as (
                select market, max("timestamp") as "timestamp" from consolidate where market = %s group by market
            )   
        select sum(abs(position_size)) / 2 as value
        from consolidate inner join latest using (market, "timestamp")
        where position_size != 0
    """, [instrument])

    [oi] = cur.fetchone()

    cur.execute("""
        with
            latest as (
                select market, max("timestamp") as "timestamp" from consolidate where market = %s group by market
            ),
            oi as (
                select sum(abs(position_size)) / 2 as oi
                from consolidate inner join latest using (market, "timestamp")
                where position_size != 0
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
        from consolidate inner join latest using (market, "timestamp"), oi
         where position_size > 0
        order by oi_share desc;
    """, [instrument])

    longs = cur.fetchall()

    cur.execute("""
        with
            latest as (
                select market, max("timestamp") as "timestamp" from consolidate where market = %s group by market
            ),
            oi as (
                select sum(abs(position_size)) / 2 as oi
                from consolidate inner join latest using (market, "timestamp")
                where position_size != 0
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
        from consolidate inner join latest using (market, "timestamp"), oi
         where position_size < 0
        order by oi_share desc;
    """, [instrument])

    shorts = cur.fetchall()

    return render_template('./positions.html', perpetuals=perpetuals, instrument=instrument, oi=oi, longs=longs, shorts=shorts, last_updated=last_updated)


@app.route('/positions.csv')
def positions_csv():
    instrument = request.args.get('instrument') or 'SOL-PERP'

    conn = psycopg2.connect(os.getenv('PSYCOPG_CONN'))

    cur = conn.cursor()

    cur.execute("""
        select max("timestamp")::text
        from consolidate where market = %s
         and position_size != 0
    """, [instrument])

    [last_updated] = cur.fetchone()

    def stream():
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        cur.execute("""
            with
                latest as (
                    select market, max("timestamp") as "timestamp" from consolidate where market = %s group by market
                ),
                oi as (
                    select sum(abs(position_size)) / 2 as oi
                    from consolidate inner join latest using (market, "timestamp")
                    where position_size != 0
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
            from consolidate inner join latest using (market, "timestamp"), oi
             where position_size != 0
            order by oi_share desc;
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
            'Content-Disposition': f"attachment; filename={instrument}_positions_{last_updated}.csv"
        }
    )

@app.route('/balances')
def balances():
    instrument = request.args.get('instrument') or 'SOL'

    conn = psycopg2.connect(os.getenv('PSYCOPG_CONN'))

    cur = conn.cursor()

    cur.execute("""
        with
            latest as (
                select asset, max("timestamp") as "timestamp" from balances group by asset
            )
        select sum(abs(deposits)) as total_deposits
             , sum(abs(borrows)) as total_borrows
        from balances
            inner join latest using (asset, "timestamp")
        where asset = %s;
    """, [instrument])

    [total_deposits, total_borrows] = cur.fetchone()

    cur.execute("""select max("timestamp")::text from balances where asset = %s""", [instrument])

    [last_updated] = cur.fetchone()

    cur.execute("""
        with
            latest as (
                select asset, max("timestamp") as "timestamp" from balances group by asset
            )
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
        from balances
            inner join latest using (asset, "timestamp")
        where asset = %s
          and value not between -5 and 5
        order by value desc;
    """, [instrument])

    balances = cur.fetchall()

    return render_template('./balances.html', spot=spot, instrument=instrument, balances=balances, total_deposits=total_deposits, total_borrows=total_borrows, last_updated=last_updated)

@app.route('/balances.csv')
def balances_csv():
    instrument = request.args.get('instrument') or 'SOL'

    conn = psycopg2.connect(os.getenv('PSYCOPG_CONN'))

    cur = conn.cursor()

    cur.execute("""select max("timestamp")::text from balances where asset = %s""", [instrument])

    [last_updated] = cur.fetchone()

    def stream():
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        cur.execute("""
            with
                latest as (
                    select asset, max("timestamp") as "timestamp" from balances group by asset
                ),
                oi as (
                    select sum(abs(deposits)) / 2 as oi_deposit,
                           sum(abs(borrows)) / 2 as oi_borrows
                    from balances inner join latest using (asset, "timestamp")
                    where not (deposits < 1 and borrows < 1)
                )
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
            from balances
                inner join latest using (asset, "timestamp")
            where asset = %s
              and value not between -5 and 5;
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


if __name__ == '__main__':
    app.run(host='0.0.0.0')
