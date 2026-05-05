"""
server.py — Local Flask server for NSE Futures Premium Dashboard

Serves the dashboard and exposes a /api/refresh endpoint that:
  1. Downloads last 7 days of BhavCopy data
  2. Rebuilds FUTURES_PREMIUM_DB.csv

Usage:
  pip install flask
  python server.py
  open http://localhost:5000
"""

import os
import sys
import glob
import re
import subprocess
import pandas as pd
from flask import Flask, send_file, Response, stream_with_context, jsonify

app = Flask(__name__)
BASE = os.path.dirname(os.path.abspath(__file__))
BTST_DIR = os.path.join(BASE, 'btst_picks')


@app.route('/')
def index():
    return send_file(os.path.join(BASE, 'index.html'))


@app.route('/FUTURES_PREMIUM_DB.csv')
def serve_csv():
    csv_path = os.path.join(BASE, 'FUTURES_PREMIUM_DB.csv')
    return send_file(csv_path, mimetype='text/csv')


@app.route('/api/refresh', methods=['POST'])
def refresh():
    steps = [
        ([sys.executable, 'download_bhavcopy_bulk.py', '--days', '7'], '=== Step 1: Downloading last 7 days of BhavCopy data ==='),
        ([sys.executable, 'build_futures_premium_db.py'], '=== Step 2: Building FUTURES_PREMIUM_DB.csv ==='),
        ([sys.executable, 'btst_screener.py', '--all'], '=== Step 3: Building BTST picks ==='),
    ]

    def generate():
        for cmd, label in steps:
            yield f'data: {label}\n\n'
            proc = subprocess.Popen(
                cmd,
                cwd=BASE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            for line in proc.stdout:
                stripped = line.rstrip()
                if stripped:
                    yield f'data: {stripped}\n\n'
            proc.wait()
            if proc.returncode != 0:
                yield f'data: ERROR: script exited with code {proc.returncode}\n\n'
                yield 'data: DONE\n\n'
                return
        yield 'data: DONE\n\n'

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
        }
    )


@app.route('/api/btst/dates')
def btst_dates():
    files = sorted(glob.glob(os.path.join(BTST_DIR, 'BTST_PICKS_*.csv')), reverse=True)
    dates = []
    for f in files:
        m = re.search(r'BTST_PICKS_(\d{8})\.csv', os.path.basename(f))
        if m:
            dates.append(m.group(1))
    return jsonify(dates)


@app.route('/api/btst/<date>')
def btst_for_date(date):
    csv_path = os.path.join(BTST_DIR, f'BTST_PICKS_{date}.csv')
    if not os.path.exists(csv_path):
        return jsonify({'error': f'No BTST picks for date {date}'}), 404
    df = pd.read_csv(csv_path)
    return jsonify(df.to_dict(orient='records'))


if __name__ == '__main__':
    print('NSE Futures Premium Dashboard')
    print(f'Serving from: {BASE}')
    print('Open http://localhost:5000')
    app.run(debug=False, port=5000, threaded=True)
