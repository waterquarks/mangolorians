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
from flask import Flask, jsonify, request, render_template, redirect, get_template_attribute, Response, make_response
from flask_caching import Cache

load_dotenv()

app = Flask(__name__)

cache = Cache(config={'CACHE_TYPE': 'SimpleCache'})

cache.init_app(app)

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
@cache.cached(timeout=60 * 60 * 24)
def exchange():
    conn = psycopg2.connect(os.getenv('TRADE_HISTORY_DB'))

    cur = conn.cursor()

    cur.execute("""
        with
            volumes_per_month as (
                select sum(price * quantity) as volume
                     , extract(epoch from date_trunc('month', "loadTimestamp")) * 1e3 as month
                from perp_event
                group by month
                order by month
            ),
            series as (
                select json_agg(json_build_array(month, volume)) as volumes
                from volumes_per_month
            )
        select json_agg(json_build_object('name', 'Volumes', 'data', volumes)) from series;
    """)

    [monthly_volumes] = cur.fetchone()

    cur.execute("""
        with
            volumes_per_month as (
                select name as market
                     , sum(price * quantity) as volume
                     , extract(epoch from date_trunc('month', "loadTimestamp")) * 1e3 as month
                from perp_event
                inner join perp_market_meta using (address)
                group by market, month
                order by market, month
            ),
            series as (
                select market
                     , json_agg(json_build_array(month, volume)) as volumes
                from volumes_per_month
                group by market
            )
        select json_agg(json_build_object('name', market, 'data', volumes)) as value from series;
    """)

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
        from weighted_average_fill_prices
        inner join misc using (exchange, symbol)
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

    asset = re.split(r'-|/', instrument)[0]

    return render_template(
        'orderbook.html',
        instrument=instrument,
        perpetuals=perpetuals,
        spot=spot,
        asset=asset
    )

@app.route('/analytics/depth')
def analytics_depth():
    symbol = request.args.get('symbol')

    db = sqlite3.connect('./daemons/analyze_orderbooks_l2.db')

    [data] = db.execute("""
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

    return data


@app.route('/analytics/spreads')
def analytics_spreads():
    symbol = request.args.get('symbol')

    db = sqlite3.connect('./daemons/analyze_orderbooks_l2.db')

    [data] = db.execute("""
        with
            spreads as (
                select exchange
                     , symbol
                     , order_size
                     , ((weighted_average_buy_price - quotes.weighted_average_sell_price) / weighted_average_buy_price) * 100 as spread
                     , cast(strftime('%s', datetime("timestamp")) * 1e3 as int) as timestamp
                from quotes
                where exchange in ('Mango Markets perps', 'Mango Markets spot')
                  and symbol = :symbol
            ),
            spreads_by_order_size as (
                select exchange
                     , symbol
                     , order_size
                     , json_group_array(json_array("timestamp", spread)) as spreads
                from spreads
                group by exchange, symbol, order_size
            )
        select
            json_group_array(
                json_object(
                    'name', '$' || cast(cast(order_size / 1000 as integer) as text) || 'K',
                    'data', spreads
                )
            ) as value
        from spreads_by_order_size
    """, {'symbol': symbol}).fetchone()

    return data


@app.route('/analytics/jupiter_cost_of_trade')
def jupiter_cost_of_trade():
    symbol = request.args.get('symbol') or 'SOL'

    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute("""
        with
            trades as (
                select
                    symbol,
                    order_size,
                    price,
                    min(price) over (partition by symbol, local_timestamp) as slam, -- Best bid
                    local_timestamp
                from raw.jupiter_cost_of_trades
                cross join lateral (
                    select
                        (
                            select
                                json_object_agg(
                                    split_part(unnest, '=', 1),
                                    split_part(unnest, '=', 2)
                                )
                            from unnest(regexp_split_to_array(substr(url, position('?' in url) + 1), '&'))
                        ) as params
                    ) as a
                cross join lateral (
                    select
                        params->>'id' as symbol,
                        (params->>'vsAmount')::numeric as order_size
                ) as b
                 cross join lateral (
                    select (response->'data'->>'price')::numeric as price
                ) as c
                where local_timestamp >= '2022-08-24'
                  and status = 200 and price is not null
            ),
            series as (
                select
                    symbol,
                    json_build_object(
                            'name', '$' || (order_size / 1000)::integer::text || 'K',
                            'data', json_agg(
                                    json_build_array(
                                                extract(epoch from date_trunc('minute', local_timestamp))::integer * 1e3,
                                                slippage * 100
                                        ) order by local_timestamp
                                )
                        ) as serie
                from trades
                         cross join lateral (select ((price - slam) / price) as slippage) as a
                where order_size in (10000, 25000, 50000, 100000)
                group by symbol, order_size
            )
        select json_agg(serie) as value from series where symbol = %s group by symbol;
    """, [symbol])

    [data] = cur.fetchone()

    return jsonify(data)


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

    max_from = date.today() - timedelta(days=2)

    max_to = date.today() - timedelta(days=1)

    from_ = request.args.get('from') or (max_from - timedelta(days=14))

    to = request.args.get('to') or max_to

    cur.execute("""
        with
            params("from", "to", instrument) as (
                select %(from)s::timestamp, %(to)s::timestamp, %(instrument)s
            ),
            volumes as (
                select instrument
                     , mango_account
                     , count(*) as trades_count
                     , coalesce(sum(case when liquidity_type = 'maker' then volume end), 0)::bigint as maker_volume
                     , coalesce(sum(case when liquidity_type = 'taker' then volume end), 0)::bigint as taker_volume
                     , coalesce(sum(volume), 0)::bigint as total_volume
                from flux
                where created_at >= (select "from" from params)
                  and created_at < (select "to" from params)
                  and instrument = (select instrument from params)
                group by instrument, mango_account
                order by total_volume desc
            )
        select
            case
                when count(*) > 0 then
                    jsonb_agg(
                        jsonb_build_array(
                            mango_account,
                            trades_count,
                            taker_volume,
                            maker_volume,
                            total_volume
                        )
                    )
                else jsonb_build_array()
            end
        from volumes;
    """, {'instrument': instrument, 'from': from_, 'to': to})

    [volumes_by_mango_account] = cur.fetchone()

    cur.execute("""
        with
            params("from", "to", instrument) as (
                select %(from)s::timestamp, %(to)s::timestamp, %(instrument)s
            ),
            volumes as (
                select instrument
                     , mango_account_owner
                     , count(*) as trades_count
                     , coalesce(sum(case when liquidity_type = 'maker' then volume end), 0)::bigint as maker_volume
                     , coalesce(sum(case when liquidity_type = 'taker' then volume end), 0)::bigint as taker_volume
                     , coalesce(sum(volume), 0)::bigint as total_volume
                from flux
                where created_at >= (select "from" from params) and created_at < (select "to" from params)
                  and instrument = (select instrument from params)
                group by instrument, mango_account_owner
                order by total_volume desc
            )
        select
            case
                when count(*) > 0 then
                    jsonb_agg(
                       jsonb_build_array(
                           mango_account_owner,
                           trades_count,
                           taker_volume,
                           maker_volume,
                           total_volume
                       )
                   )
                else jsonb_build_array()
            end
        from volumes;
    """, {'instrument': instrument, 'from': from_, 'to': to})

    [volumes_by_signer] = cur.fetchone()

    cur.execute("""
        with
            params("from", "to", instrument) as (
                select %(from)s::timestamp, %(to)s::timestamp, %(instrument)s
            ),
            volumes as (
                select instrument
                     , mango_account_delegate
                     , count(*) as trades_count
                     , coalesce(sum(case when liquidity_type = 'maker' then volume end), 0)::bigint as maker_volume
                     , coalesce(sum(case when liquidity_type = 'taker' then volume end), 0)::bigint as taker_volume
                     , coalesce(sum(volume), 0)::bigint as total_volume
                from flux
                where created_at >= (select "from" from params) and created_at < (select "to" from params)
                  and instrument = (select instrument from params)
                group by instrument, mango_account_delegate
                order by total_volume desc
            )
        select
            case
                when count(*) > 0 then
                    jsonb_agg(
                        jsonb_build_array(
                            mango_account_delegate,
                            trades_count,
                            taker_volume,
                            maker_volume,
                            total_volume
                        )
                    )
                else jsonb_build_array()
            end
        from volumes
    """, {'instrument': instrument, 'from': from_, 'to': to})

    [volumes_by_delegate] = cur.fetchone()

    cur.execute("""
        with
            params("from", "to", instrument) as (
                select %(from)s::timestamp, %(to)s::timestamp, %(instrument)s
            ),
            volumes as (
                select instrument
                     , mango_account_referrer
                     , count(*) as trades_count
                     , coalesce(sum(case when liquidity_type = 'maker' then volume end), 0)::bigint as maker_volume
                     , coalesce(sum(case when liquidity_type = 'taker' then volume end), 0)::bigint as taker_volume
                     , coalesce(sum(volume), 0)::bigint as total_volume
                from flux
                where created_at >= (select "from" from params) and created_at < (select "to" from params)
                  and instrument = (select instrument from params)
                group by instrument, mango_account_referrer
                order by total_volume desc
            )
        select
            case
                when count(*) > 0 then
                    jsonb_agg(
                        jsonb_build_array(
                            mango_account_referrer,
                            trades_count,
                            taker_volume,
                            maker_volume,
                            total_volume,
                            referrer_ids
                        )
                    )
                else jsonb_build_array()
            end
        from volumes
        left join referrers on volumes.mango_account_referrer = referrers.mango_account;
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
        max_from=str(max_from),
        max_to=str(max_to)
    )


@app.route('/aprs')
@cache.cached(timeout=60 * 60 * 24, query_string=True)
def aprs():
    instrument = request.args.get('instrument') or 'SOL'

    conn = psycopg2.connect(os.getenv('MANGO_STATS_DB'))

    cur = conn.cursor()

    cur.execute("""
        with
            aprs as (
                select
                    name as symbol,
                    avg("depositRate" * 100) as avg_deposit_rate_pct,
                    avg("totalDeposits") as total_deposits,
                    avg("borrowRate" * 100) as avg_borrow_rate_pct,
                    avg("totalBorrows") as total_borrows,
                    time_bucket('1 day', "time") as day
                from spot_market_stats
                where "mangoGroup" = 'mainnet.1'
                group by name, day
                order by name, day asc
            )
        select
            json_agg(
                json_build_array(
                    extract(epoch from day)::integer * 1e3,
                    avg_deposit_rate_pct,
                    avg_borrow_rate_pct,
                    total_deposits,
                    total_borrows
                )
            ) as value
        from aprs
        where symbol = %s;
    """, [instrument])

    [aprs] = cur.fetchone()

    return render_template(
        './aprs.html',
        instrument=instrument,
        spot=[*list(map(lambda instrument : instrument.split('/')[0], spot)), 'USDC'],
        aprs=aprs,
    )


@app.route('/loserboards')
def loserboards():
    start_date = request.args.get('start_date') or str(date.today() - timedelta(days=30))

    conn = psycopg2.connect(os.getenv('TRADE_HISTORY_DB'))

    cur = conn.cursor()

    cur.execute("""
        with leaderboard as (
            select
                t1.date_hour,
                t1.mango_account,
                t1.spot_value + t1.perp_value + t1.open_orders_value - t1.transfer_balance - coalesce(t2.pnl, 0) as pnl
            from performance_cache.account_performance t1
            left join (
                select
                    date_hour,
                    mango_account,
                    spot_value + perp_value + open_orders_value - transfer_balance as pnl
                from performance_cache.account_performance
                where date_hour = (
                    select max(date_hour) from performance_cache.account_performance ap where date_hour <= %s
                )
            ) t2 using (mango_account)
            where t1.date_hour = (select max(date_hour) from performance_cache.account_performance)
            order by pnl asc
            limit 20
        )
        select
            mango_account,
            pnl,
            w.owner as wallet_pk
        from leaderboard
        left join (
            select
                distinct on
                (margin_account) margin_account,
                owner
            from transactions_v3.deposits_withdraws dw
            where mango_group = '98pjRuQjK3qA6gXts96PqZT4Ze5QmnCmt3QYjhbUSPue'
              and margin_account in (select mango_account from leaderboard)
        ) w on w.margin_account = leaderboard.mango_account
        order by pnl asc;
    """, [start_date])

    traders = cur.fetchall()

    return render_template('./loserboard.html', traders=traders, start_date=start_date, max_start_date=str(date.today()))


@app.route('/referrals')
def referrals():
    conn = psycopg2.connect(os.getenv('TRADE_HISTORY_DB'))

    referrer = request.args.get('referrer') or '5MCjoSYkAYo5QpqD1Yugo4sNLyTy4dZqbrmU271kuEcV'

    cur = conn.cursor()

    cur.execute("""
        with
            params (referrer_mango_account) as (
                values(%(referrer)s)
            ),
            pad as (
                select * from params, generate_series(date_trunc('week', current_timestamp) - interval '6 weeks', date_trunc('week', current_timestamp), interval '1 week') as week
            ),
            a as (
                select distinct on (referree)
                    referree as referree_mango_account,
                    referrer as referrer_mango_account,
                    block_datetime
                from transactions_v3.mango_account_referrer
                where referrer = %(referrer)s
                order by referree_mango_account, referrer_mango_account, block_datetime
            ),
            b as (
                select distinct on (referree_mango_account)
                    referree_mango_account, referrer_mango_account, block_datetime
                from transactions_v3.referral_fee_accrual
                where referrer_mango_account = %(referrer)s
                order by referree_mango_account, referrer_mango_account, block_datetime
            ),
            c as (
                select * from a
                union
                select * from b
            ),
            referrals as (
                select distinct on (referree_mango_account)
                    referree_mango_account,
                    referrer_mango_account
                from c
                order by referree_mango_account, referrer_mango_account, block_datetime
            ),
            new_referrals_per_week as (
                select
                    referrer_mango_account,
                    date_trunc('week', first_activity) as week,
                    count(referree_mango_account) as new_referrals
                from referrals
                cross join lateral (
                    select
                        min(block_datetime) filter ( where side = 'Deposit' ) as first_activity
                    from transactions_v3.deposits_withdraws
                    where margin_account = referree_mango_account
                      and mango_group = '98pjRuQjK3qA6gXts96PqZT4Ze5QmnCmt3QYjhbUSPue'
                ) as meta
                group by referrer_mango_account, week
            ),
            fees_and_active_referrals_per_week as (
                select
                    referrer_mango_account,
                    date_trunc('week', block_datetime) as week,
                    round(sum(referral_fee_accrual)::numeric, 2) as fees,
                    count(distinct referree_mango_account) as active_referrals
                from transactions_v3.referral_fee_accrual
                inner join referrals using (referrer_mango_account, referree_mango_account)
                group by referrer_mango_account, referrer_mango_account, week
                order by referrer_mango_account, referrer_mango_account, week
            ),
            activity as (
                select
                    referrer_mango_account,
                    coalesce(fees, 0) as fees,
                    coalesce(active_referrals, 0) as active_referrals,
                    coalesce(new_referrals, 0) as new_referrals,
                    week
                from pad
                    left join fees_and_active_referrals_per_week using (referrer_mango_account, week)
                    left join new_referrals_per_week using (referrer_mango_account, week)
                order by referrer_mango_account, week
            )
        select
            json_build_object(
                'chart', json_build_object('type', 'column', 'styledMode', true),
                'title', json_build_object('text', 'Referral performance overview, last 6 weeks'),
                'xAxis', json_build_object(
                    'title', json_build_object('text', 'Week'),
                    'type', 'datetime'
                ),
                'yAxis', json_build_array(
                    json_build_object(
                        'title', json_build_object('text', 'Fees collected'),
                        'labels', json_build_object('format', '${value}')
                    ),
                    json_build_object(
                        'title', json_build_object('text', 'Referrals'),
                        'opposite', true
                    )
                ),
                'tooltip', json_build_object(
                    'shared', true,
                    'crosshairs', true
                ),
                'series', json_build_array(
                    json_build_object(
                        'name', 'Fees collected',
                        'data', json_agg(json_build_array(extract(epoch from week) * 1e3, fees))
                    ),
                    json_build_object(
                        'name', 'Active referrals',
                        'type', 'line',
                        'data', json_agg(json_build_array(extract(epoch from week) * 1e3, active_referrals)),
                        'yAxis', 1
                    ),
                    json_build_object(
                        'name', 'New referrals',
                        'type', 'line',
                        'data', json_agg(json_build_array(extract(epoch from week) * 1e3, new_referrals)),
                        'yAxis', 1
                    )
                ),
                'legend', json_build_object('enabled', true),
                'credits', false
            )
        from activity;
    """, {'referrer': referrer})

    [fees_per_month] = cur.fetchone()

    cur.execute("""
        with
            params(referrer_mango_account) as (
                values(%(referrer)s)
            ),
            frames as (
                select
                    frame,
                    dense_rank() over (order by frame) - 1 as index
                from generate_series(
                    date_trunc('week', current_timestamp) - interval '6 weeks',
                    current_timestamp, interval '1 week'
                ) as frame
            ),
            cohorts as (
                select frame as cohort, index, to_char(frame, 'YYYY-MM-DD') as name from frames
            ),
            weeks as (
                select frame as week, index, 'Week ' || index as name from frames
            ),
            pad as (
                select
                    referrer_mango_account,
                    cohort,
                    week
                from params
                cross join cohorts
                inner join weeks on week >= cohort
            ),
            a as (
                select distinct on (referree)
                    referree as referree_mango_account,
                    referrer as referrer_mango_account,
                    block_datetime
                from transactions_v3.mango_account_referrer
                where referrer = %(referrer)s
                order by referree_mango_account, referrer_mango_account, block_datetime
            ),
            b as (
                select distinct on (referree_mango_account)
                    referree_mango_account, referrer_mango_account, block_datetime
                from transactions_v3.referral_fee_accrual
                where referrer_mango_account = %(referrer)s
                order by referree_mango_account, referrer_mango_account, block_datetime
            ),
            c as (
                select * from a
                union
                select * from b
            ),
            referrals as (
                select distinct on (referree_mango_account)
                    referree_mango_account,
                    referrer_mango_account
                from c
                order by referree_mango_account, referrer_mango_account, block_datetime
            ),
            referrals_with_meta as (
                select
                    referrer_mango_account,
                    referree_mango_account,
                    cohort
                from referrals
                left join transactions_v3.mango_account_owner on mango_account = referree_mango_account
                cross join lateral (
                    select
                        date_trunc('week', min(block_datetime) filter ( where side = 'Deposit' )) as cohort
                    from transactions_v3.deposits_withdraws
                    where margin_account = mango_account
                      and mango_group = '98pjRuQjK3qA6gXts96PqZT4Ze5QmnCmt3QYjhbUSPue'
                ) as meta
                inner join cohorts using (cohort)
            ),
            activity as (
                select
                    referrer_mango_account,
                    cohort,
                    week,
                    count(distinct referree_mango_account) as active_traders
                from referrals_with_meta
                inner join perp_event on referree_mango_account in (maker, taker)
                cross join lateral (
                    select date_trunc('week', "loadTimestamp") as week
                ) as meta
                group by referrer_mango_account, cohort, week
            ),
            cells as (
                select
                    extract(days from pad.week - pad.cohort) / 7 as x,
                    cohorts.index as y,
                    coalesce(active_traders, 0) as value
                from pad
                    full join activity using (referrer_mango_account, cohort, week)
                    inner join cohorts using (cohort)
                    inner join weeks on weeks.week = cohort + (pad.week - pad.cohort)
            )
        select
            jsonb_build_object(
                'chart', jsonb_build_object('type', 'heatmap', 'styledMode', true),
                'title', jsonb_build_object('text', concat('Referral activity, last 6 weeks')),
                'subtitle', jsonb_build_object('text', 'By last trade on perp markets'),
                'xAxis', jsonb_build_object('categories', (select jsonb_agg(name) from weeks), 'reversed', false, 'opposite', true),
                'yAxis', json_build_array(
                    jsonb_build_object('title', json_build_object('text', 'Cohorts by week of first deposit'), 'categories', (select jsonb_agg(name) from cohorts), 'reversed', true, 'lineColor', 'transparent')
                ),
                'colorAxis', jsonb_build_object('min', 0),
                'series', jsonb_build_array(
                    jsonb_build_object(
                        'data', (select json_agg(json_build_object('x', x, 'y', y, 'value', value)) from cells),
                        'dataLabels', jsonb_build_object('enabled', true, 'format', '{point.value}')
                    )
                ),
                'credits', json_build_object('enabled', false)
            ) as value;
    """, {'referrer': referrer})

    [retention] = cur.fetchone()

    cur.execute("""
        with
            a as (
                select distinct on (referree)
                    referree as referree_mango_account,
                    referrer as referrer_mango_account,
                    block_datetime
                from transactions_v3.mango_account_referrer
                where referrer = %(referrer)s
                order by referree_mango_account, referrer_mango_account, block_datetime
            ),
            b as (
                select distinct on (referree_mango_account)
                    referree_mango_account, referrer_mango_account, block_datetime
                from transactions_v3.referral_fee_accrual
                where referrer_mango_account = %(referrer)s
                order by referree_mango_account, referrer_mango_account, block_datetime
            ),
            c as (
                select * from a
                union
                select * from b
            ),
            referrals as (
                select distinct on (referree_mango_account)
                    referree_mango_account,
                    referrer_mango_account,
                    block_datetime
                from c
                order by referree_mango_account, referrer_mango_account, block_datetime
            )
        select
            referrer_mango_account as mango_account,
            count(distinct referree_mango_account) as referrees,
            (select sum(referral_fee_accrual) from transactions_v3.referral_fee_accrual where referrer_mango_account = %(referrer)s) as fees
        from referrals
        group by referrer_mango_account
    """, {'referrer': referrer})

    summary = cur.fetchone()

    return render_template(
        './referrals.html',
        referrer=referrer,
        retention=retention,
        fees_per_month=fees_per_month,
        summary=summary
    )


@app.route('/referrals.csv')
def referrals_csv():
    referrer = request.args.get('referrer') or None

    if referrer is None:
        return make_response(jsonify({'error': {'message': 'Please enter a referrer query parameter.'}}), 400)

    def stream():
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        conn = psycopg2.connect(os.getenv('TRADE_HISTORY_DB'))

        cur = conn.cursor()

        cur.execute("""
            with
                a as (
                    select distinct on (referree)
                        referree as referree_mango_account,
                        referrer as referrer_mango_account,
                        block_datetime
                    from transactions_v3.mango_account_referrer
                    where referrer = %(referrer)s
                    order by referree_mango_account, referrer_mango_account, block_datetime
                ),
                b as (
                    select distinct on (referree_mango_account)
                        referree_mango_account, referrer_mango_account, block_datetime
                    from transactions_v3.referral_fee_accrual
                    where referrer_mango_account = %(referrer)s
                    order by referree_mango_account, referrer_mango_account, block_datetime
                ),
                c as (
                    select * from a
                    union
                    select * from b
                ),
                referrals as (
                    select distinct on (referree_mango_account)
                        referree_mango_account,
                        referrer_mango_account,
                        block_datetime
                    from c
                    order by referree_mango_account, referrer_mango_account, block_datetime
                )
            select * from referrals;
        """, {'referrer': referrer})

        headers = [
            "referree_mango_account", "referrer_mango_account", "block_datetime"
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
            'Content-Disposition': f"attachment; filename={referrer}_referrals.csv"
        }
    )






if __name__ == '__main__':
    app.run(host='0.0.0.0')
