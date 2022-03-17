import csv
import sqlite3
from calendar import timegm
from datetime import datetime
from io import StringIO, BytesIO
from time import time

from flask import Flask, jsonify, request, render_template, redirect, send_file, get_template_attribute, Response, make_response
from flask_cors import CORS

from crunch.wrangle import reconstruct_order_book
import io

app = Flask(__name__)

app.config['JSON_SORT_KEYS'] = False

CORS(app)

markets = [
    'BTC-PERP',
    'SOL-PERP',
    'MNGO-PERP',
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


@app.route('/')
def index():
    return redirect('/analytics')


@app.route('/analytics/', defaults={'market': None})
@app.route('/analytics/<market>')
def analytics(market):
    if market is None:
        return redirect('/analytics/SOL-PERP')

    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    return render_template(
        './analytics.html',
        market=market,
        markets=markets
    )

@app.route('/analytics/<market>/latest_slippages')
def analytics_latest_slippages(market):
    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

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
            timestamp
        from latest_slippages
    """)))

    partial = get_template_attribute('_analytics.html', 'latest_slippages')

    return partial(slippages)


@app.route('/historical_data/', defaults={'market': None})
@app.route('/historical_data/<market>')
def historical_data(market):
    if market is None:
        return redirect('/historical_data/SOL-PERP')

    return render_template('./historical_data.html', market=market, markets=markets)

@app.route('/historical_data/<market>/order_book_deltas')
def historical_data_order_book_deltas(market):
    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    order_book_deltas = list(map(dict, db.execute("""
        select * from order_book where symbol = :symbol order by "local_timestamp" desc limit 9
    """, {'symbol': market})))

    partial = get_template_attribute('_historical_data.html', 'order_book_deltas')

    return partial(order_book_deltas, market)


@app.route('/historical_data/<market>/trades')
def historical_data_trades(market):
    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    trades = list(map(dict, db.execute("""
        select * from trades where symbol = :symbol order by local_timestamp desc limit 9
    """, {'symbol': market})))

    partial = get_template_attribute('_historical_data.html', 'trades')

    return partial(trades, market)


@app.route('/historical_data/<market>/funding_rates')
def historical_data_funding_rates(market):
    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    funding_rates = list(map(dict, db.execute("""
        select * from funding_rates where symbol = :symbol order by "from" desc limit 9
    """, {'symbol': market})))

    partial = get_template_attribute('_historical_data.html', 'funding_rates')

    return partial(funding_rates, market)


@app.route('/historical_order_books')
def historical_order_books():
    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    io = StringIO()

    headers = [
        'exchange',
        'symbol',
        'timestamp',
        'local_timestamp',
        'is_snapshot',
        'side',
        'price',
        'amount'
    ]

    writer = csv.DictWriter(io, headers)

    writer.writeheader()

    for trade in db.execute("""
        select * from order_book where symbol = 'SOL-PERP'
    """).fetchall():
        writer.writerow(dict(trade))

    mem = BytesIO()

    mem.write(io.getvalue().encode())

    mem.seek(0)

    return send_file(mem, attachment_filename='order_books.csv', as_attachment=True, mimetype='text/csv')


@app.route('/historical_funding_rates')
def historical_funding_rates():
    db = sqlite3.connect('dev.db')

    db.row_factory = sqlite3.Row

    io = StringIO()

    headers = [
        'exchange',
        'symbol',
        'rate',
        'open_interest',
        'from',
        'to'
    ]

    writer = csv.DictWriter(io, headers)

    writer.writeheader()

    for trade in db.execute("""
        select * from funding_rates
    """).fetchall():
        writer.writerow(dict(trade))

    mem = BytesIO()

    mem.write(io.getvalue().encode())

    mem.seek(0)

    return send_file(mem, attachment_filename='funding_rates.csv', as_attachment=True, mimetype='text/csv')


@app.route('/liquidity')
def liquidity():
    symbol = request.args.get('symbol')

    if symbol is None:
        return (
            jsonify({'error': {'message': 'Please enter a symbol query parameter.'}}),
            400
        )

    db = sqlite3.connect('dev.db')
    db.row_factory = sqlite3.Row

    results = list(map(dict, db.execute("""
        with subset as (
            select
                exchange,
                symbol,
                buy,
                sell,
                timestamp
            from average_liquidity_per_minute
            where symbol = :symbol
            order by "timestamp" desc limit 4320
        )
        select * from subset order by "timestamp" 
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
        with subset as (
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
                timestamp
            from average_slippages_per_minute
            where symbol = :symbol
            order by "timestamp" desc limit 4320
        )
        select * from subset order by "timestamp"
    """, {'symbol': symbol})))

    return jsonify(results)


@app.route('/latest_slippages')
def latest_slippages():
    db = sqlite3.connect('dev.db')
    db.row_factory = sqlite3.Row

    results = []

    for row in db.execute("""
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
            timestamp
        from latest_slippages
    """):
        results.append(dict(row))

    return jsonify(results)


@app.route('/order_book_deltas/<symbol>')
def order_book_deltas(symbol):
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

        cursor = db.cursor().execute("""select * from order_book where symbol = ? order by local_timestamp""", [symbol])

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

@app.route('/trades/<symbol>')
def trades(symbol):
    def stream():
        buffer = io.StringIO()

        writer = csv.writer(buffer)

        db = sqlite3.connect('dev.db')

        cursor = db.cursor().execute("""select * from trades where symbol = ? order by local_timestamp""", [symbol])

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
            'Content-Disposition': f"attachment; filename={symbol}_trades.csv"
        }
    )


@app.route('/order_book/<market>')
def order_book(market):
    timestamp = request.args.get('timestamp')

    timestamp = int(time() * 1e6) if timestamp is None else int(
        timegm(datetime.fromisoformat(timestamp).timetuple()) * 1e6)

    depth = request.args.get('depth')

    depth = None if depth is None else int(request.args.get('depth'))

    order_book = reconstruct_order_book(market, timestamp, depth)

    return jsonify(order_book)


if __name__ == '__main__':
    app.run(host='0.0.0.0')