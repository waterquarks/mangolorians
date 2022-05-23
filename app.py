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
import humanize

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
}


def regex_replace(value, pattern, repl):
    return re.sub(pattern, repl, value)


def humanize_seconds_delta(value):
    return humanize.precisedelta(timedelta(seconds=value), minimum_unit="minutes", format="%d")


app.jinja_env.filters['regex_replace'] = regex_replace



@app.route('/')
def index():
    return redirect('/analytics/')

@app.route('/exchange')
def exchange():
    return render_template('./exchange.html')


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

    if symbol is None:
        return (
            jsonify({'error': {'message': 'Please enter a symbol query parameter.'}}),
            400
        )

    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    results = list(map(dict, db.execute("""
        with
            average_slippage_per_minute as (
                select
                    exchange,
                    symbol,
                    avg(buy_50K) as buy_50K,
                    avg(buy_100K) as buy_100K,
                    avg(buy_200K) as buy_200K,
                    avg(buy_500K) as buy_500K,
                    avg(buy_1M) as buy_1M,
                    avg(sell_50K) as sell_50K,
                    avg(sell_100K) as sell_100K,
                    avg(sell_200K) as sell_200K,
                    avg(sell_500K) as sell_500K,
                    avg(sell_1M) as sell_1M,
                    strftime('%Y-%m-%dT%H:%M:00Z', "timestamp") as minute
                from slippages
                where exchange = 'Mango Markets'
                  and symbol = :symbol
                  and "timestamp" > datetime(current_timestamp, '-7 days')
                group by exchange, symbol, minute
                order by "minute" desc
            )
            select * from average_slippage_per_minute order by minute;
    """, {'symbol': symbol})))

    return jsonify(results)


@app.route('/analytics/perpetuals/slippages')
def analytics_perpetuals_slippages():
    db = sqlite3.connect(':memory:')

    db.execute("attach './scrapers/mango_l2_order_book.db' as mango")

    db.execute("attach './scrapers/ftx_l2_order_book.db' as ftx")

    db.execute("attach './scrapers/serum_l2_order_book.db' as serum")

    db.execute("""
        create table orders (
            exchange text,
            market text,
            side text,
            price real,
            size real,
            primary key (exchange, market, side, price)
        ) without rowid
    """)

    db.execute("""
        insert into orders
        select 'Mango Markets' as exchange, * from mango.orders
        union all
        select 'FTX' as exchange, * from ftx.orders
        union all
        select 'Serum' as exchange, * from serum.orders
    """)

    db.execute("""
        create table slippages (
            exchange text,
            market text,
            size real,
            buy real,
            sell real,
            total real,
            primary key (exchange, market, size)
        )
    """)

    query = """
        insert into slippages
        with
            orders as (
                select
                    side,
                    price,
                    sum(size) as size,
                    price * sum(size) as volume,
                    sum(price * sum(size)) over (partition by side order by case when side = 'bids' then - price when side = 'asks' then price end) as cumulative_volume
                from main.orders
                where exchange = :exchange and market = :market
                group by side, price
                order by side, case when side = 'bids' then - price when side = 'asks' then price end
            ),
            misc as (
                select
                    *,
                    (top_bid + top_ask) / 2 as mid_price
                from (
                    select
                        max(price) filter ( where side = 'bids') as top_bid,
                        min(price) filter ( where side = 'asks') as top_ask
                    from orders
                )
            ),
            fills as (
                select
                    side,
                    price,
                    fill,
                    sum(fill) over (
                        partition by side
                        order by case when side = 'bids' then - price when side = 'asks' then price end
                    ) as cumulative_fill
                from (
                    select
                        side,
                        price,
                        case
                            when cumulative_volume < :size then volume
                            else coalesce(lag(remainder) over (partition by side), case when volume < :size then volume else :size end)
                        end as fill
                    from (select *, :size - cumulative_volume as remainder from orders)
                )
                where fill > 0
            ),
            weighted_average_fill_prices as (
                select
                    case when sum(case when side = 'asks' then fill end) = :size then sum(case when side = 'asks' then price * fill end) / :size end as weighted_average_buy_price,
                    case when sum(case when side = 'bids' then fill end) = :size then sum(case when side = 'bids' then price * fill end) / :size end as weighted_average_sell_price
                from fills
            ),
            slippages as (
                select
                    :exchange as exchange,
                    :market as market,
                    :size as size,
                    ((weighted_average_buy_price - mid_price) / mid_price) * 1e2 as buy,
                    ((mid_price - weighted_average_sell_price) / mid_price) * 1e2 as sell
                from weighted_average_fill_prices, misc
            )
        select exchange, market, size, buy, sell, buy + sell as total from slippages;
    """

    for [exchange, market, size] in db.execute("""
        with
            sizes(size) as (values(50000), (100000), (200000), (500000), (1000000))
        select distinct
            orders.exchange, orders.market, sizes.size
        from main.orders, sizes
    """):
        db.execute(query, [exchange, market, size])

    db.execute("""
        create table summary (
            exchange text,
            market text,
            "50000" text,
            "100000" text,
            "200000" text,
            "500000" text,
            "1000000" text,
            primary key (exchange, market)
        )
    """)

    db.execute("""
        insert into summary
        with
            groups as (
                select
                   exchange,
                   market,
                   json_group_array(json_array(buy, sell, total)) as slippages
                from slippages
                group by exchange, market
            )
        select
            exchange,
            market,
            json_extract(json(slippages), '$[0]') as "50000",
            json_extract(json(slippages), '$[1]') as "100000",
            json_extract(json(slippages), '$[2]') as "200000",
            json_extract(json(slippages), '$[3]') as "500000",
            json_extract(json(slippages), '$[4]') as "1000000"
        from groups
    """)

    def parse(entry):
        return entry[0], entry[1], *(json.loads(slippage) for slippage in entry[2:])

    slippages = {
        'mango': map(parse, db.execute("select * from summary where exchange = 'Mango Markets'")),
        'ftx': map(parse, db.execute("select * from summary where exchange = 'FTX'")),
        'serum': map(parse, db.execute("select * from summary where exchange = 'Serum'"))
    }

    partial = get_template_attribute('analytics/_perpetuals.html', 'slippages')

    return partial(slippages)


@app.route('/historical_data/')
def historical_data():
    instrument = request.args.get('instrument')

    if instrument is None:
        return redirect('/historical_data?instrument=SOL-PERP')

    if instrument not in [*perpetuals, *spot]:
        return {'error': {'message': f"{instrument} isn't a valid instrument."}}, 404

    if instrument in perpetuals:
        return render_template(
            'historical_data/perpetuals.html',
            instrument=instrument,
            perpetuals=perpetuals,
            spot=spot
        )

    if instrument in spot:
        return render_template(
            'historical_data/spot.html',
            instrument=instrument,
            perpetuals=perpetuals,
            spot=spot
        )


@app.route('/historical_data/order_book_deltas')
def historical_data_order_book_deltas():
    instrument = request.args.get('instrument')

    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    order_book_deltas = list(map(dict, db.execute("""
        select * from order_book_deltas where exchange = 'Mango Markets' and symbol = :symbol order by "local_timestamp" desc limit 9
    """, {'symbol': instrument})))

    partial = get_template_attribute('historical_data/_historical_data.html', 'order_book_deltas')

    return partial(order_book_deltas, instrument)


@app.route('/historical_data/trades')
def historical_data_trades():
    instrument = request.args.get('instrument')

    if instrument not in [*perpetuals, *spot]:
        return {'error': {'message': f"{instrument} isn't a valid instrument."}}, 404

    if instrument in perpetuals:
        conn = psycopg2.connect("dbname='postgres' user='ioaquine'")

        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            select
                'Mango Markets' as exchange,
                symbol,
                "loadTimestamp" as "timestamp",
                "seqNum" as sequence_number,
                taker as taker_id,
                "takerOrderId" as taker_order_id,
                "takerClientOrderId" as taker_client_order_id,
                maker as maker_id,
                "makerOrderId" as maker_order_id,
                "makerClientOrderId" as maker_client_order_id,
                "takerSide" as side,
                price,
                quantity,
                "makerFee" as maker_fee,
                "takerFee" as taker_fee
            from perp_event
            inner join instruments using (address)
            where symbol = %(symbol)s
            order by "loadTimestamp" desc, "seqNum" desc
            limit 9
        """, {'symbol': instrument})

        trades = cur.fetchall()

        partial = get_template_attribute('historical_data/_historical_data.html', 'trades')

        return partial(trades, instrument)


@app.route('/historical_data/funding_rates')
def historical_data_funding_rates():
    instrument = request.args.get('instrument')

    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    funding_rates = list(map(dict, db.execute("""
        select * from funding_rates where symbol = :symbol order by "from" desc limit 9
    """, {'symbol': instrument})))

    partial = get_template_attribute('historical_data/_perpetuals.html', 'funding_rates')

    return partial(funding_rates, instrument)


@app.route('/historical_data/order_book_deltas.csv')
def historical_data_order_book_deltas_csv():
    symbol = request.args.get('instrument')

    def stream():
        # Given that historical data CSVs can get pretty big, pulling them
        # into memory before sending them to the client would have us swapping
        # constantly. Hence it'd be a good idea to stream the results instead.

        # The convoluted code below is due to csv writer accepting StringIO
        # only but Flask's stream response accepting only BytesIO - hence
        # it's necessary to go back and forth between the two.

        buffer = io.StringIO()

        writer = csv.writer(buffer)

        db = sqlite3.connect('dev.db')

        cursor = db.cursor().execute("""select * from order_book_deltas where exchange = 'Mango Markets' and symbol = ? order by local_timestamp""", [symbol])

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
            'Content-Disposition': f"attachment; filename={symbol}_order_book_deltas.csv"
        }
    )


@app.route('/historical_data/trades.csv')
def historical_data_trades_csv():
    instrument = request.args.get('instrument')

    if instrument is None:
        return jsonify({'error': {'message': 'Please enter an instrument query parameter.'}}), 400

    if instrument not in [*perpetuals, *spot]:
        return jsonify({'error': {'message': f"${instrument} is not a valid instrument"}}), 400

    if instrument in spot:
        return jsonify({'error': {'message': f"Spot trades historical data downloads are not supported yet."}}), 400

    # TODO: Instead of streaming the CSV every time, serve a cached file.

    if instrument in perpetuals:
        def stream():
            buffer = io.StringIO()

            writer = csv.writer(buffer)

            conn = psycopg2.connect("dbname='postgres' user='ioaquine'")

            cursor = conn.cursor()

            cursor.execute("""
                select
                    'Mango Markets' as exchange,
                    symbol,
                    "loadTimestamp" as "timestamp",
                    "seqNum" as sequence_number,
                    taker as taker_id,
                    "takerOrderId" as taker_order_id,
                    "takerClientOrderId" as taker_client_order_id,
                    maker as maker_id,
                    "makerOrderId" as maker_order_id,
                    "makerClientOrderId" as maker_client_order_id,
                    "takerSide" as side,
                    price,
                    quantity,
                    "makerFee" as maker_fee,
                    "takerFee" as taker_fee
                from perp_event
                inner join instruments using (address)
                where symbol = %(symbol)s
                order by "loadTimestamp" desc, "seqNum" desc
            """, {'symbol': instrument})

            headers = [column.name for column in cursor.description]

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
                'Content-Disposition': f"attachment; filename={instrument}_trades.csv"
            }
        )


@app.route('/historical_data/funding_rates.csv')
def historical_data_funding_rates_csv():
    instrument = request.args.get('instrument')

    if instrument is None:
        return jsonify({'error': {'message': 'Please enter an instrument query parameter.'}}), 400

    if instrument not in [*perpetuals, *spot]:
        return jsonify({'error': {'message': f"${instrument} is not a valid instrument"}}), 400

    if instrument in spot:
        return jsonify({'error': {'message': f"Spot trades historical data downloads are not supported yet."}}), 400

    def stream():
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        db = sqlite3.connect('dev.db')

        cursor = db.cursor().execute("""select * from funding_rates where symbol = ? order by \"from\" desc""", [instrument])

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

    return Response(stream(), mimetype='text/csv',
        headers={
            'Content-Disposition': f"attachment; filename={instrument}_funding_rates.csv"
        }
    )

@app.route('/historical_data/l3_order_book_deltas')
def historical_data_l3_order_book_deltas():
    instrument = request.args.get('instrument')

    db = sqlite3.connect('./scripts/l3_deltas.db')

    db.row_factory = sqlite3.Row

    funding_rates = list(map(dict, db.execute("""
        select * from deltas where timestamp > '2022-04-01' and market = :symbol order by timestamp desc limit 9
    """, {'symbol': instrument})))

    partial = get_template_attribute('historical_data/_perpetuals.html', 'l3_order_book_deltas')

    return partial(funding_rates, instrument)


@app.route('/historical_data/l3_order_book_deltas.csv')
def historical_data_l3_order_book_deltas_csv():
    instrument = request.args.get('instrument')

    if instrument is None:
        return jsonify({'error': {'message': 'Please enter an instrument query parameter.'}}), 400

    if instrument not in [*perpetuals, *spot]:
        return jsonify({'error': {'message': f"${instrument} is not a valid instrument"}}), 400

    if instrument in spot:
        return jsonify({'error': {'message': f"Spot trades historical data downloads are not supported yet."}}), 400

    def stream():
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        db = sqlite3.connect('./scripts/l3_deltas.db')

        cursor = db.cursor().execute("""select * from deltas where timestamp > '2022-04-01' and market = :symbol order by timestamp desc""", [instrument])

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

    return Response(stream(), mimetype='text/csv',
        headers={
            'Content-Disposition': f"attachment; filename={instrument}_l3_order_book_deltas.csv"
        }
    )

@app.route('/market_maker_competitions')
def market_maker_competitions():
    db = psycopg2.connect(os.getenv('PSYCOPG_CONN'))

    cur = db.cursor()

    cur.execute("""
        select market
             , account
             , target_depth
             , target_spread
             , target_uptime
             , uptime_with_target_spread
             , uptime_with_any_spread
        from mm_competitions_week_4
        order by coalesce(uptime_with_any_spread, 0) desc;
    """)

    tranches = list(cur.fetchall())

    return render_template('./market_maker_competitions.html', tranches=tranches)

@app.route('/market_maker_analytics')
def market_maker_analytics():
    account = request.args.get('account') or '4rm5QCgFPm4d37MCawNypngV4qPWv4D5tw57KE2qUcLE'

    market = request.args.get('instrument') or 'BTC-PERP'

    target_depth = int(request.args.get('target-depth') or 1000)

    target_spread = float(request.args.get('target-spread') or 0.3)

    date = request.args.get('date') or '2022-05-19'

    [metrics, slots, slots_with_target_spread, slots_with_any_spread] = benchmark(market, account, target_depth, target_spread, date)

    return render_template(
        './market_maker_analytics.html',
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

@app.route('/market_maker_analytics/metrics.csv')
def market_maker_analytics_spreads_csv():
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

@app.route('/market_maker_analytics/depth.csv')
def market_maker_analytics_depth_csv():
    account = request.args.get('account') or '2Fgjpc7bp9jpiTRKSVSsiAcexw8Cawbz7GLJu8MamS9q'

    market = request.args.get('instrument') or 'BTC-PERP'

    target_depth = int(request.args.get('target_depth') or 12500)

    target_spread = request.args.get('target_spread') or 0.15

    from_ = request.args.get('from') or (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(timespec='minutes').replace('+00:00', '')

    to = request.args.get('to') or datetime.now(timezone.utc).isoformat(timespec='minutes').replace('+00:00', '')

    def stream():
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        db = sqlite3.connect('./scripts/depth_eta.db')

        db.set_trace_callback(print)

        cursor = db.cursor().execute("""
            select
                bids,
                asks,
                slot,
                "timestamp"
            from depth
            where market = :market
              and account = :account
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
            'Content-Disposition': f"attachment; filename={account}_{market}'s depth.csv"
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
                select sum(abs(position_size)) as oi
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
        from consolidate inner join latest using (market, "timestamp"), oi
         where position_size != 0
        order by oi_share desc;
    """, [instrument])

    positions = cur.fetchall()

    return render_template('positions.html', perpetuals=perpetuals, instrument=instrument, positions=positions, oi=oi, last_updated=last_updated)

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
                    select sum(abs(position_size)) as oi
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


if __name__ == '__main__':
    app.run(host='0.0.0.0')
