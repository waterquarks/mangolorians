import csv
import io
import sqlite3
import re
import psycopg2
import psycopg2.extras
import scripts.reconstruct_l3_order_book
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify, request, render_template, redirect, get_template_attribute, Response
import json
import humanize

app = Flask(__name__)

perpetuals = [
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
    'SRM-PERP'
]

spot = [
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
]


def regex_replace(value, pattern, repl):
    return re.sub(pattern, repl, value)


def humanize_seconds_delta(value):
    return humanize.precisedelta(timedelta(seconds=value), minimum_unit="minutes", format="%d")


app.jinja_env.filters['regex_replace'] = regex_replace

app.jinja_env.filters['humanize_seconds_delta'] = humanize_seconds_delta


@app.route('/')
def index():
    return redirect('/analytics/')


@app.route('/analytics/')
def analytics():
    instrument = request.args.get('instrument')

    if instrument is None:
        return redirect('/analytics?instrument=SOL-PERP')

    if instrument not in [*perpetuals, *spot]:
        return jsonify({'error': {'message': f"${instrument} is not a valid instrument"}}), 400

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
def slippages():
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

    db = sqlite3.connect('./scrapers/l3_deltas.db')

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

        db = sqlite3.connect('./scrapers/l3_deltas.db')

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


@app.route('/market_maker_analytics')
def market_maker_analytics():
    accounts = request.args.get('accounts') or 'GJDMYqhT2XPxoUDk3bczDivcE2FgmEeBzkpmcaRNWP3'

    instrument = request.args.get('instrument') or 'SOL-PERP'

    target_liquidity = request.args.get('target_liquidity') or 1000

    target_spread = request.args.get('target_spread') or 0.15

    from_ = request.args.get('from') or (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat(timespec='minutes').replace('+00:00', '')

    to = request.args.get('to') or datetime.now(timezone.utc).isoformat(timespec='minutes').replace('+00:00', '')

    return render_template(
        './test.html',
        accounts=accounts,
        instrument=instrument,
        target_liquidity=target_liquidity,
        target_spread=target_spread,
        from_=from_,
        to=to,
        perpetuals=perpetuals
    )

@app.route('/market_maker_analytics/content')
def market_maker_analytics_content():
    instrument = request.args.get('instrument')

    accounts = request.args.get('accounts')

    target_liquidity = int(request.args.get('target_liquidity') or 1000)

    target_spread = float(request.args.get('target_spread') or 0.15)

    from_ = request.args.get('from') or (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat(timespec='minutes').replace('+00:00', '')

    to = request.args.get('to') or datetime.now(timezone.utc).isoformat(timespec='minutes').replace('+00:00', '')

    min = "2022-04-12T00:00"

    max = datetime.now(timezone.utc).isoformat(timespec='minutes').replace('+00:00', '')

    benchmark = scripts.reconstruct_l3_order_book.benchmark(instrument, accounts.split(','), target_liquidity, target_spread, from_, to)

    partial = get_template_attribute('_test.html', 'content')

    return partial(
        **benchmark['summary'],
        instrument=instrument,
        accounts=accounts,
        target_liquidity=target_liquidity,
        target_spread=target_spread,
        from_=from_,
        to=to,
        max=max,
        liquidity=benchmark['liquidity'],
        spreads=benchmark['spreads']
    )


@app.route('/market_maker_competitions')
def market_maker_competitions():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    cur.execute("""
        with
            ticks as (
                select
                    market,
                    account,
                    coalesce(extract(epoch from created_at) - extract(epoch from lag(created_at) over (partition by market, account order by created_at)), 0) as delta,
                    weighted_average_bid,
                    weighted_average_ask,
                    absolute_spread,
                    relative_spread,
                    active,
                    compliant,
                    created_at
                from spreads
            ),
            uptime as (
                select
                    market,
                    account,
                    elapsed,
                    absolute_uptime,
                    absolute_uptime / elapsed as relative_uptime,
                    compliant_absolute_uptime,
                    compliant_absolute_uptime / elapsed as compliant_relative_uptime
                from (
                     select
                        market,
                        account,
                        extract(epoch from max(created_at)) - extract(epoch from min(created_at)) as elapsed,
                        sum(delta) filter (where active) as absolute_uptime,
                        sum(delta) filter (where compliant) as compliant_absolute_uptime
                    from ticks
                    group by market, account
                ) as alpha
            ),
            metrics as (
                select
                    market,
                    account,
                    weighted_average_bid,
                    weighted_average_ask,
                    absolute_spread,
                    relative_spread
                from (
                     select
                        market,
                        account,
                        avg(weighted_average_bid) filter ( where compliant ) as weighted_average_bid,
                        avg(weighted_average_ask) filter ( where compliant ) as weighted_average_ask,
                        avg(absolute_spread) filter ( where compliant ) as absolute_spread,
                        avg(relative_spread) filter ( where compliant ) as relative_spread
                    from ticks
                    group by market, account
                ) as alpha
            ),
            summary as (
                select
                    elapsed,
                    absolute_uptime,
                    relative_uptime,
                    compliant_absolute_uptime,
                    compliant_relative_uptime,
                    market,
                    account,
                    weighted_average_bid,
                    weighted_average_ask,
                    absolute_spread,
                    relative_spread
                from uptime
                inner join metrics using (market, account)
            ),
            tranches as (
                select
                    market,
                    account,
                    target_liquidity,
                    target_spread,
                    target_uptime
                from tranches
                inner join target_spreads using (market)
                inner join target_uptimes using (target_liquidity)
                order by account, market
            )
        select
            account,
            market,
            round(target_liquidity::numeric) as target_size,
            concat(target_spread, '%') as target_spread,
            concat((trunc(target_uptime::numeric * 1e2, 1)), '%') as target_uptime,
            concat((trunc(coalesce(compliant_relative_uptime::numeric, 0) * 1e2, 1)), '%') as uptime_target_spread,
            concat((trunc(coalesce(relative_uptime::numeric, 0) * 1e2, 1)), '%') as uptime_any_spread
        from summary
        inner join tranches using (market, account)
        order by account, market;
    """)

    headers = [entry[0] for entry in cur.description]

    tranches = cur.fetchall()

    return render_template('./market_maker_competitions.html', headers=headers, tranches=tranches)

@app.route('/market_maker_competitor')
def market_maker_competitor():
    conn = psycopg2.connect('dbname=mangolorians')

    cur = conn.cursor()

    market = request.args.get('market')

    account = request.args.get('account')

    cur.execute("""
        with entries as (
            select
                date_trunc('minute', created_at)::timestamp as minute,
                avg(buy) as buy,
                avg(sell) as sell
            from liquidity
            where market = %(market)s
              and account = %(account)s
            group by market, account, minute
            order by market, account, minute asc
        )
        select json_agg(json_build_object('minute', minute, 'buy', buy, 'sell', sell)) from entries
    """, {'market': market, 'account': account})

    liquidity = cur.fetchone()[0]

    cur.execute("""
        with entries as (
            select
                date_trunc('minute', created_at)::timestamp as minute,
                avg(weighted_average_bid) as weighted_average_bid,
                avg(weighted_average_ask) as weighted_average_ask,
                avg(weighted_average_ask - weighted_average_bid) as absolute_spread,
                avg(((weighted_average_ask - weighted_average_bid) / weighted_average_ask) * 100) as relative_spread
            from spreads
            where market = %(market)s
              and account = %(account)s
            group by market, account, minute
            order by market, account, minute asc
        )
        select
            json_agg(
                json_build_object(
                    'minute', minute,
                    'weighted_average_bid', weighted_average_bid,
                    'weighted_average_ask', weighted_average_ask,
                    'absolute_spread', absolute_spread,
                    'relative_spread', relative_spread
                )
            )
        from entries
    """, {'market': market, 'account': account})

    spreads = cur.fetchone()[0]

    cur.execute("""
        with
            ticks as (
                select
                    market,
                    account,
                    coalesce(extract(epoch from created_at) - extract(epoch from lag(created_at) over (partition by market, account order by created_at)), 0) as delta,
                    weighted_average_bid,
                    weighted_average_ask,
                    absolute_spread,
                    relative_spread,
                    active,
                    compliant,
                    created_at
                from spreads
                where market = %(market)s
                  and account = %(account)s
            ),
            uptime as (
                select
                    market,
                    account,
                    elapsed,
                    absolute_uptime,
                    absolute_uptime / elapsed as relative_uptime,
                    compliant_absolute_uptime,
                    compliant_absolute_uptime / elapsed as compliant_relative_uptime
                from (
                     select
                        market,
                        account,
                        extract(epoch from max(created_at)) - extract(epoch from min(created_at)) as elapsed,
                        sum(delta) filter (where active) as absolute_uptime,
                        sum(delta) filter (where compliant) as compliant_absolute_uptime
                    from ticks
                    group by market, account
                ) as alpha
            ),
            metrics as (
                select
                    market,
                    account,
                    weighted_average_bid,
                    weighted_average_ask,
                    absolute_spread,
                    relative_spread
                from (
                     select
                        market,
                        account,
                        avg(weighted_average_bid) filter ( where compliant ) as weighted_average_bid,
                        avg(weighted_average_ask) filter ( where compliant ) as weighted_average_ask,
                        avg(absolute_spread) filter ( where compliant ) as absolute_spread,
                        avg(relative_spread) filter ( where compliant ) as relative_spread
                    from ticks
                    group by market, account
                ) as alpha
            ),
            summary as (
                select
                    elapsed,
                    absolute_uptime,
                    relative_uptime,
                    compliant_absolute_uptime,
                    compliant_relative_uptime,
                    market,
                    account,
                    weighted_average_bid,
                    weighted_average_ask,
                    absolute_spread,
                    relative_spread
                from uptime
                inner join metrics using (market, account)
            ),
            tranches as (
                select
                    market,
                    account,
                    target_liquidity,
                    target_spread,
                    target_uptime
                from tranches
                inner join target_spreads using (market)
                inner join target_uptimes using (target_liquidity)
                order by account, market
            )
        select
            elapsed,
            absolute_uptime,
            relative_uptime,
            compliant_absolute_uptime,
            compliant_relative_uptime,
            weighted_average_bid,
            weighted_average_ask,
            absolute_spread,
            relative_spread
        from summary
        inner join tranches using (market, account)
        order by account, market;
    """, {'market': market, 'account': account})

    elapsed, absolute_uptime, relative_uptime, compliant_absolute_uptime, compliant_relative_uptime, weighted_average_bid, weighted_average_ask, absolute_spread, relative_spread = cur.fetchone()

    return render_template(
        './market_maker_competitor.html',
        liquidity=liquidity,
        spreads=spreads,
        instrument=market,
        account=account,
        elapsed=elapsed,
        absolute_uptime=absolute_uptime,
        relative_uptime=relative_uptime,
        compliant_absolute_uptime=compliant_absolute_uptime,
        compliant_relative_uptime=compliant_relative_uptime,
        weighted_average_bid=weighted_average_bid,
        weighted_average_ask=weighted_average_ask,
        absolute_spread=absolute_spread,
        relative_spread=relative_spread
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0')
