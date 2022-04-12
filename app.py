import csv
import io
import sqlite3
import re
import psycopg2
import psycopg2.extras
import scrapers.reconstruct_l3_order_book
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify, request, render_template, redirect, get_template_attribute, Response
import json

app = Flask(__name__)

perpetuals = [
    'BTC-PERP',
    'SOL-PERP',
    'ADA-PERP',
    'AVAX-PERP',
    'BNB-PERP',
    'ETH-PERP',
    'FTT-PERP',
    'LUNA-PERP',
    'MNGO-PERP',
    'RAY-PERP',
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


app.jinja_env.filters['regex_replace'] = regex_replace


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


@app.route('/analytics/perpetuals/latest_slippages')
def analytics_perpetuals_latest_slippages():
    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    # TODO: Optimize this query (currently taking about 3s - unacceptable)
    slippages = list(map(dict, db.execute("""
        select
            exchange,
            symbol,
            buy_50K,
            buy_100K,
            buy_200K,
            buy_500K,
            buy_1M,
            sell_50K,
            sell_100K,
            sell_200K,
            sell_500K,
            sell_1M,
            max(timestamp)
        from slippages
        where exchange = 'Mango Markets'
        group by exchange, symbol;
    """)))

    partial = get_template_attribute('analytics/_perpetuals.html', 'latest_slippages')

    return partial(slippages)

@app.route('/liquidity')
def liquidity():
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


@app.route('/market_maker_analytics')
def market_maker_analytics():
    instrument = request.args.get('instrument')

    accounts = request.args.get('accounts')

    if instrument is None:
        instrument = 'SOL-PERP'

    if accounts is None:
        accounts = 'GJDMYqhT2XPxoUDk3bczDivcE2FgmEeBzkpmcaRNWP3'
    else:
        accounts = accounts.replace(' ', '')

    return render_template(
        './test.html',
        instrument=instrument,
        accounts=accounts,
        perpetuals=perpetuals,
        spot=spot
    )


@app.route('/analytics/liquidity')
def analytics_liquidity():
    instrument = request.args.get('instrument')

    accounts = request.args.get('accounts').split(',')

    liquidity = scrapers.reconstruct_l3_order_book.liquidity(instrument, accounts)

    return jsonify(json.loads(liquidity))


@app.route('/analytics/quotes')
def analytics_quotes():
    instrument = request.args.get('instrument')

    accounts = request.args.get('accounts').split(',')

    spreads = scrapers.reconstruct_l3_order_book.spreads(instrument, accounts)

    return jsonify(json.loads(spreads))

@app.route('/analytics/benchmark/')
def analytics_uptime():
    instrument = request.args.get('instrument')

    accounts = request.args.get('accounts')

    target_liquidity = int(request.args.get('target_liquidity') or 1000)

    target_spread = float(request.args.get('target_spread') or 0.15)

    from_ = request.args.get('from') or (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat(timespec='minutes').replace('+00:00', '')

    to = request.args.get('to') or datetime.now(timezone.utc).isoformat(timespec='minutes').replace('+00:00', '')

    max = datetime.now(timezone.utc).isoformat(timespec='minutes').replace('+00:00', '')

    benchmark = scrapers.reconstruct_l3_order_book.benchmark(instrument, accounts.split(','), target_liquidity, target_spread, from_, to)

    partial = get_template_attribute('_test.html', 'summary')

    return partial(
        **benchmark,
        instrument=instrument,
        accounts=accounts,
        target_liquidity=target_liquidity,
        target_spread=target_spread,
        from_=from_,
        to=to,
        max=max
    )

@app.route('/analytics/ftx_slippages/')
def analytics_ftx_slippages():
    db = sqlite3.connect('./scrapers/ftx/l2_order_book_analytics.db')

    db.row_factory = sqlite3.Row

    state = sqlite3.connect(':memory:')

    state.row_factory = sqlite3.Row

    state.execute("create temp table slippages (timestamp text, market text, order_size real, buy_slippage_bps real, sell_slippage_bps real, total_slippage_bps real, primary key (timestamp, market, order_size)) without rowid")

    state.execute("attach database './scrapers/ftx/l2_order_book_analytics.db' as analytics")

    query = """
        insert or replace into slippages
        with
            orders as (
                select
                    side,
                    price,
                    sum(size) as size,
                    price * sum(size) as volume,
                    sum(price * sum(size)) over (partition by side order by case when side = 'bids' then - price when side = 'asks' then price end) as cumulative_volume
                from analytics.orders
                where market = :market
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
                    side, price, fill, sum(fill) over (partition by side order by case when side = 'bids' then - price when side = 'asks' then price end) as cumulative_fill
                from (
                    select
                        side,
                        price,
                        case when cumulative_volume < :order_size then volume else coalesce(lag(remainder) over (partition by side), case when volume < :order_size then volume else :order_size end) end as fill
                    from (select *, :order_size - cumulative_volume as remainder from orders)
                )
                where fill > 0
            ),

            weighted_average_fill_prices as (
                select
                    case when sum(case when side = 'asks' then fill end) = :order_size then sum(case when side = 'asks' then price * fill end) / :order_size end as weighted_average_buy_price,
                    case when sum(case when side = 'bids' then fill end) = :order_size then sum(case when side = 'bids' then price * fill end) / :order_size end as weighted_average_sell_price
                from fills
            )
        select
            *,
            buy_slippage_bps + sell_slippage_bps as total_slippage_bps
        from
        (
            select
                current_timestamp as timestamp,
                :market as market,
                :order_size as order_size,
                ((weighted_average_buy_price - mid_price) / mid_price) * 1e4 as buy_slippage_bps,
                ((mid_price - weighted_average_sell_price) / mid_price) * 1e4 as sell_slippage_bps
            from weighted_average_fill_prices, misc
        )
    """

    for market in perpetuals:
        for order_size in [50000, 100000, 200000, 500000, 1000000]:
            state.execute(query, {'market': market, 'order_size': order_size})

    entries = list(map(dict, state.execute("select timestamp, market, order_size, buy_slippage_bps, sell_slippage_bps, total_slippage_bps from slippages")))

    return render_template(
        'ftx_slippages.html',
        entries=entries
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0')
