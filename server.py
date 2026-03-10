import os
import sys
import re
import json
import math
import logging
import threading
import webbrowser

import requests as req
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import yfinance as yf

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S'
)

BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
VALID_PERIODS = {'5d', '1mo', '3mo', '6mo', '1y', '2y', '5y'}

# 投資信託コード判定: 8桁英数字+.T (例: 0131103C.T)
# 日本株コードは4桁数字+.T (例: 7203.T) で区別可能
_FUND_CODE_RE = re.compile(r'^[0-9A-Z]{8}$')

def is_fund_ticker(ticker: str) -> bool:
    """8桁英数字+.T のティッカーを投資信託と判定"""
    if not ticker.endswith('.T'):
        return False
    return bool(_FUND_CODE_RE.match(ticker[:-2]))


_YF_JP_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'ja-JP,ja;q=0.9,en;q=0.8',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


def _extract_preloaded_state(html: str) -> dict:
    """Yahoo Finance Japan の window.__PRELOADED_STATE__ を抽出"""
    for marker in ('window.__PRELOADED_STATE__ = ', 'window.__PRELOADED_STATE__='):
        idx = html.find(marker)
        if idx >= 0:
            start = idx + len(marker)
            break
    else:
        raise ValueError('PRELOADED_STATE が見つかりません')

    # 先頭の空白をスキップして JSON オブジェクトの開始位置へ
    while start < len(html) and html[start] in ' \t\r\n':
        start += 1

    # raw_decode は JSON を先頭から解析し、終端インデックスも返す
    try:
        obj, _ = json.JSONDecoder().raw_decode(html, start)
        return obj
    except json.JSONDecodeError as e:
        raise ValueError(f'JSON パースエラー: {e}')


def get_fund_price_yfjp(fund_code: str) -> dict:
    """Yahoo Finance Japan から投資信託の基準価額を取得"""
    url = f'https://finance.yahoo.co.jp/quote/{fund_code}'
    r = req.get(url, headers=_YF_JP_HEADERS, timeout=15)
    if r.status_code == 404:
        raise ValueError(f'ファンド {fund_code} が見つかりません (404)')
    r.raise_for_status()

    state       = _extract_preloaded_state(r.text)
    board       = state.get('mainFundPriceBoard', {})
    fund_prices = board.get('fundPrices', {})
    price_str   = fund_prices.get('price', '')
    change_str  = fund_prices.get('changePrice', '0') or '0'

    if not price_str:
        raise ValueError('基準価額が見つかりません')

    price  = float(price_str.replace(',', ''))
    # △はマイナスを表す日本語記号
    change = float(change_str.replace('△', '-').replace(',', ''))
    prev   = round(price - change, 4)

    logging.info(f'FUND {fund_code}: {price} JPY (前日比 {change:+.0f})')
    return {
        'price':      price,
        'prev_close': prev,
        'currency':   'JPY',
        'error':      None,
    }


def get_fund_history_yfjp(fund_code: str, period: str) -> list:
    """Yahoo Finance Japan から投資信託の基準価額履歴を取得"""
    # ページ内の __PRELOADED_STATE__ に chart データが含まれているか試みる
    url = f'https://finance.yahoo.co.jp/quote/{fund_code}/chart'
    try:
        r = req.get(url, headers=_YF_JP_HEADERS, timeout=15)
        r.raise_for_status()
        state = _extract_preloaded_state(r.text)
        # チャートデータのパスを探す
        chart = (state.get('mainFundChart') or
                 state.get('mainChart') or
                 state.get('fundChart') or {})
        series = chart.get('series') or chart.get('data') or []
        if series:
            result = []
            for pt in series:
                d = pt.get('date') or pt.get('x') or pt.get('t')
                c = pt.get('close') or pt.get('y') or pt.get('price')
                if d and c is not None:
                    result.append({'date': str(d)[:10], 'close': float(str(c).replace(',', ''))})
            if result:
                return result
    except Exception as e:
        logging.debug(f'FUND HIST chart page failed for {fund_code}: {e}')

    # フォールバック: 現在価格のみ1点返す
    try:
        p = get_fund_price_yfjp(fund_code)
        from datetime import date
        return [{'date': date.today().isoformat(), 'close': p['price']}]
    except Exception:
        return []

app = Flask(__name__, static_folder=BASE_DIR, static_url_path='/static')
CORS(app)


# ── Helpers ───────────────────────────────────────────────────────
def safe_float(v):
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


# ── Frontend ──────────────────────────────────────────────────────
@app.route('/')
def index():
    """Serve the single-page app."""
    return send_from_directory(BASE_DIR, 'index.html')


# ── API ───────────────────────────────────────────────────────────
@app.route('/api/health')
def health():
    return jsonify({'status': 'ok'})


@app.route('/api/prices')
def get_prices():
    """
    GET /api/prices?tickers=7203.T,AAPL,0131103C.T
    ティッカー形式:
      日本株  → {code}.T   (例: 7203.T)
      米国株  → {code}     (例: AAPL)
      投資信託 → {code}.T  (例: 0131103C.T)
    """
    tickers_param = request.args.get('tickers', '').strip()
    if not tickers_param:
        return jsonify({'error': 'tickersパラメータが必要です'}), 400

    tickers = [t.strip() for t in tickers_param.split(',') if t.strip()]
    result  = {}

    for ticker in tickers:
        try:
            if is_fund_ticker(ticker):
                fund_code      = ticker[:-2]   # .T を除去
                result[ticker] = get_fund_price_yfjp(fund_code)
            else:
                info  = yf.Ticker(ticker).fast_info
                price = safe_float(info.last_price)
                prev  = safe_float(info.previous_close)

                if price is None:
                    raise ValueError('価格データが取得できませんでした')

                result[ticker] = {
                    'price':      round(price, 4),
                    'prev_close': round(prev,  4) if prev is not None else None,
                    'currency':   info.currency or 'JPY',
                    'error':      None,
                }
                logging.info(f'OK   {ticker}: {price} ({info.currency})')

        except Exception as e:
            logging.warning(f'ERR  {ticker}: {e}')
            result[ticker] = {'price': None, 'prev_close': None, 'currency': None, 'error': str(e)}

    return jsonify(result)


@app.route('/api/history')
def get_history():
    """
    GET /api/history?tickers=7203.T,AAPL&period=1mo
    period: 5d | 1mo | 3mo | 6mo | 1y | 2y | 5y
    USDJPY=X の履歴も自動付加して返す。
    """
    tickers_param = request.args.get('tickers', '').strip()
    period        = request.args.get('period', '1mo')

    if period not in VALID_PERIODS:
        period = '1mo'
    if not tickers_param:
        return jsonify({'error': 'tickersが必要です'}), 400

    tickers   = [t.strip() for t in tickers_param.split(',') if t.strip()]
    fetch_set = list(set(tickers + ['USDJPY=X']))
    result    = {}

    for ticker in fetch_set:
        try:
            if is_fund_ticker(ticker):
                fund_code      = ticker[:-2]
                data           = get_fund_history_yfjp(fund_code, period)
                result[ticker] = data
                logging.info(f'HIST FUND {ticker}: {len(data)} bars ({period})')
            else:
                hist = yf.Ticker(ticker).history(period=period)
                if hist.empty:
                    raise ValueError('データなし')

                data = []
                for dt, row in hist.iterrows():
                    close = safe_float(row['Close'])
                    if close is not None:
                        data.append({'date': dt.strftime('%Y-%m-%d'), 'close': round(close, 4)})

                result[ticker] = data
                logging.info(f'HIST {ticker}: {len(data)} bars ({period})')

        except Exception as e:
            logging.warning(f'HIST ERR {ticker}: {e}')
            result[ticker] = []

    return jsonify(result)


@app.route('/api/name')
def get_name():
    """
    GET /api/name?ticker=03319172.T
    銘柄名を返す。投資信託は Yahoo Finance Japan から、株式は yfinance から取得。
    """
    ticker = request.args.get('ticker', '').strip()
    if not ticker:
        return jsonify({'name': '', 'error': 'ticker が必要です'}), 400
    try:
        if is_fund_ticker(ticker):
            fund_code = ticker[:-2]
            url = f'https://finance.yahoo.co.jp/quote/{fund_code}'
            r = req.get(url, headers=_YF_JP_HEADERS, timeout=10)
            r.raise_for_status()
            # <title> からファンド名を抽出
            m = re.search(r'<title>\s*(.+?)(?:【|\[|\|)', r.text)
            name = m.group(1).strip() if m else ''
            logging.info(f'NAME FUND {fund_code}: {name!r}')
            return jsonify({'name': name})
        else:
            info = yf.Ticker(ticker).info
            name = info.get('shortName') or info.get('longName') or ''
            logging.info(f'NAME {ticker}: {name!r}')
            return jsonify({'name': name})
    except Exception as e:
        logging.warning(f'NAME ERR {ticker}: {e}')
        return jsonify({'name': '', 'error': str(e)})


@app.route('/api/usdjpy')
def get_usdjpy():
    """GET /api/usdjpy — 現在のUSD/JPYレートを返す"""
    try:
        info = yf.Ticker('USDJPY=X').fast_info
        rate = safe_float(info.last_price)
        if rate is None:
            raise ValueError('レートデータなし')
        logging.info(f'USDJPY: {rate}')
        return jsonify({'rate': round(rate, 2), 'error': None})
    except Exception as e:
        logging.warning(f'USDJPY ERR: {e}')
        return jsonify({'rate': 150.0, 'error': str(e)})


# ── Entry point ───────────────────────────────────────────────────
if __name__ == '__main__':
    port      = int(os.environ.get('PORT', 5001))
    url       = f'http://localhost:{port}'
    no_open   = '--no-open' in sys.argv

    print()
    print('  ┌──────────────────────────────────────┐')
    print('  │   株式ポートフォリオ管理              │')
    print(f'  │   URL: {url:<30}│')
    print('  │   Ctrl+C で停止                       │')
    print('  └──────────────────────────────────────┘')
    print()

    if not no_open:
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()

    app.run(host='0.0.0.0', port=port, debug=False)
