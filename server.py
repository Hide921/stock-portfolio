import os
import sys
import re
import json
import math
import logging
import threading
import webbrowser
import concurrent.futures

import time
import requests as req
from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS
import yfinance as yf


def yf_ticker(ticker: str) -> yf.Ticker:
    """yfinance 0.2.x は curl_cffi セッションを内部管理するため
    session パラメータは渡さず、ライブラリに任せる。"""
    return yf.Ticker(ticker)


def fetch_price_with_retry(ticker: str, retries: int = 2) -> dict:
    """Chart API → yfinance fast_info の順で取得。
    Chart API はレート制限を受けにくい直接エンドポイント。"""
    # 1st: Yahoo Finance Chart API (最も信頼性が高い)
    try:
        return fetch_price_via_chart_api(ticker)
    except Exception as e:
        logging.debug(f'Chart API failed for {ticker}: {e}')

    # 2nd: yfinance fast_info (バックアップ)
    last_err = None
    for attempt in range(retries):
        try:
            info  = yf_ticker(ticker).fast_info
            price = safe_float(info.last_price)
            prev  = safe_float(info.previous_close)
            if price is None:
                raise ValueError('price is None')
            return {
                'price':      round(price, 4),
                'prev_close': round(prev, 4) if prev is not None else None,
                'day_change': round(price - prev, 4) if (price is not None and prev is not None) else None,
                'currency':   getattr(info, 'currency', None) or 'JPY',
                'error':      None,
            }
        except Exception as e:
            last_err = e
            if 'Too Many Requests' in str(e) or 'Rate' in str(e):
                time.sleep(2 ** attempt)
            else:
                break

    raise last_err or ValueError('価格取得失敗')

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

_YF_API_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
}

def fetch_price_via_chart_api(ticker: str) -> dict:
    """Yahoo Finance Chart API を直接叩いて現在価格を取得（yfinance 非依存）"""
    import urllib.parse
    url = f'https://query2.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ticker)}?interval=1d&range=2d'
    r = req.get(url, headers=_YF_API_HEADERS, timeout=15)
    r.raise_for_status()
    data = r.json()
    result = data['chart']['result'][0]
    meta   = result['meta']
    post_price = safe_float(meta.get('postMarketPrice'))
    post_change = safe_float(meta.get('postMarketChange'))
    pre_price = safe_float(meta.get('preMarketPrice'))
    pre_change = safe_float(meta.get('preMarketChange'))
    reg_price = safe_float(meta.get('regularMarketPrice'))
    reg_change = safe_float(meta.get('regularMarketChange'))

    # 前日比は常に通常取引(regular)を基準にする
    if reg_price is not None and reg_change is not None:
        price, change = reg_price, reg_change
    elif reg_price is not None:
        price, change = reg_price, None
    elif post_price is not None and post_change is not None:
        price, change = post_price, post_change
    elif pre_price is not None and pre_change is not None:
        price, change = pre_price, pre_change
    else:
        price, change = reg_price or post_price or pre_price, reg_change
    prev = safe_float(price - change) if (price is not None and change is not None) else None
    if prev is None:
        prev = safe_float(
            meta.get('regularMarketPreviousClose')
            or meta.get('chartPreviousClose')
            or meta.get('previousClose')
        )
    if change is None and price is not None and prev is not None:
        change = safe_float(price - prev)
    if price is None:
        closes = result.get('indicators', {}).get('quote', [{}])[0].get('close', [])
        price  = safe_float(next((c for c in reversed(closes) if c is not None), None))
    if price is None:
        raise ValueError('price is None')
    currency = meta.get('currency') or ('JPY' if ticker.endswith('.T') else 'USD')
    return {
        'price':      round(price, 4),
        'prev_close': round(prev, 4) if prev is not None else None,
        'day_change': round(change, 4) if change is not None else None,
        'currency':   currency,
        'error':      None,
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
        'day_change': change,
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
CORS(app)  # 全オリジン許可（公開株価データのため問題なし）

# ── In-memory cache (レート制限対策) ──────────────────────────────
_price_cache: dict = {}   # ticker -> {'data': {...}, 'ts': float}
_hist_cache:  dict = {}   # (ticker, period) -> {'data': [...], 'ts': float}
PRICE_CACHE_TTL = 300     # 5分
HIST_CACHE_TTL  = 600     # 10分

def _cached_price(ticker: str):
    entry = _price_cache.get(ticker)
    if entry and time.time() - entry['ts'] < PRICE_CACHE_TTL:
        return entry['data']
    return None

def _store_price(ticker: str, data: dict):
    _price_cache[ticker] = {'data': data, 'ts': time.time()}

def _cached_hist(ticker: str, period: str):
    entry = _hist_cache.get((ticker, period))
    if entry and time.time() - entry['ts'] < HIST_CACHE_TTL:
        return entry['data']
    return None

def _store_hist(ticker: str, period: str, data: list):
    _hist_cache[(ticker, period)] = {'data': data, 'ts': time.time()}


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
    resp = send_from_directory(BASE_DIR, 'index.html')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


# ── API ───────────────────────────────────────────────────────────
@app.route('/api/health')
def health():
    return jsonify({'status': 'ok'})


@app.route('/api/data', methods=['GET', 'POST'])
def data_store():
    """ポートフォリオデータの保存・読込。
    GitHub Gist をサーバー側で管理（PAT は Render 環境変数に設定）。
    GET  → Gist からデータを取得して返す
    POST → 受け取ったデータを Gist に保存
    環境変数: GITHUB_PAT, GIST_ID
    """
    pat     = os.environ.get('GITHUB_PAT', '')
    gist_id = os.environ.get('GIST_ID', '')

    if not pat or not gist_id:
        return jsonify({'error': 'GITHUB_PAT / GIST_ID が未設定です'}), 503

    gist_url = f'https://api.github.com/gists/{gist_id}'
    headers  = {
        'Authorization': f'token {pat}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
    }

    if request.method == 'GET':
        try:
            r = req.get(gist_url, headers=headers, timeout=10)
            r.raise_for_status()
            content = r.json()['files'].get('portfolio.json', {}).get('content', '[]')
            return Response(content, content_type='application/json')
        except Exception as e:
            logging.warning(f'DATA GET ERR: {e}')
            return jsonify({'error': str(e)}), 502

    else:  # POST
        try:
            body = request.get_json(force=True)
            r = req.patch(gist_url, headers=headers, timeout=10,
                          json={'files': {'portfolio.json': {'content': json.dumps(body, ensure_ascii=False)}}})
            r.raise_for_status()
            return jsonify({'ok': True})
        except Exception as e:
            logging.warning(f'DATA POST ERR: {e}')
            return jsonify({'error': str(e)}), 502


@app.route('/api/snapshot')
def take_snapshot():
    """日次スナップショット: ポートフォリオの現在価値をGistのログに自動記録。
    cron-job.org などから毎日1回呼び出すことで、
    ユーザーがアプリを開かない日も推移グラフにデータが蓄積される。
    """
    from datetime import date as _date

    pat     = os.environ.get('GITHUB_PAT', '')
    gist_id = os.environ.get('GIST_ID', '')
    if not pat or not gist_id:
        return jsonify({'error': 'GITHUB_PAT / GIST_ID が未設定'}), 503

    gist_headers_map = {
        'Authorization': f'token {pat}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
    }
    gist_url = f'https://api.github.com/gists/{gist_id}'

    # ── 1. Gistからポートフォリオデータを取得 ──
    try:
        r = req.get(gist_url, headers=gist_headers_map, timeout=10)
        r.raise_for_status()
        content = r.json()['files']['portfolio.json']['content']
        payload = json.loads(content)
    except Exception as e:
        return jsonify({'error': f'Gist読込失敗: {e}'}), 502

    stocks_data = payload if isinstance(payload, list) else payload.get('stocks', [])
    log         = [] if isinstance(payload, list) else payload.get('log', [])

    if not stocks_data:
        return jsonify({'error': 'ポートフォリオが空です'}), 400

    # ── 2. 全銘柄の価格を並列取得 ──
    tickers = list({s['yahooTicker'] for s in stocks_data if s.get('yahooTicker')})
    prices  = {}

    def _fetch(t):
        if is_fund_ticker(t):
            d = get_fund_price_yfjp(t[:-2])
        else:
            d = fetch_price_with_retry(t)
        return t, d

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_fetch, t): t for t in tickers + ['USDJPY=X']}
        for fut in concurrent.futures.as_completed(futs, timeout=90):
            t = futs[fut]
            try:
                _, d = fut.result(timeout=30)
                prices[t] = d
            except Exception as e:
                logging.warning(f'SNAPSHOT price err {t}: {e}')

    # ── 3. 円換算合計（現物のみ）を計算 ──
    usdjpy = safe_float((prices.get('USDJPY=X') or {}).get('price')) or 150.0
    total  = 0.0
    for s in stocks_data:
        if s.get('tradeType') == 'margin':
            continue
        t = s.get('yahooTicker', '')
        p = prices.get(t)
        if not p or not p.get('price'):
            continue
        price    = p['price'] / 10000 if is_fund_ticker(t) else p['price']
        qty      = float(s.get('quantity', 0))
        currency = p.get('currency', 'JPY')
        total   += price * qty * usdjpy if currency == 'USD' else price * qty

    if total <= 0:
        return jsonify({'error': '有効な価格データなし'}), 500

    # ── 4. ログに今日の値を記録（1日1エントリ・上書き）──
    today = _date.today().isoformat()
    entry = {'date': today, 'value': round(total)}
    idx   = next((i for i, e in enumerate(log) if e['date'] == today), -1)
    if idx >= 0:
        log[idx] = entry
    else:
        log.append(entry)
    log.sort(key=lambda e: e['date'])
    if len(log) > 730:
        log = log[-730:]

    # ── 5. Gistに保存 ──
    new_payload = {'stocks': stocks_data, 'log': log}
    try:
        r = req.patch(gist_url, headers=gist_headers_map, timeout=10,
                      json={'files': {'portfolio.json': {'content': json.dumps(new_payload, ensure_ascii=False)}}})
        r.raise_for_status()
    except Exception as e:
        return jsonify({'error': f'Gist保存失敗: {e}'}), 502

    logging.info(f'SNAPSHOT {today}: ¥{round(total):,} (log={len(log)}件)')
    return jsonify({'ok': True, 'date': today, 'value': round(total), 'log_count': len(log)})


@app.route('/api/proxy')
def proxy():
    """Yahoo Finance への CORSプロキシ。GitHub Pages (ブラウザ) から呼ばれる。
    ブラウザは CORS 制約でYahoo Financeに直接アクセスできないため、
    このサーバーが代わりに取得して返す。"""
    url = request.args.get('url', '').strip()
    if not url:
        return jsonify({'error': 'url required'}), 400
    # Yahoo Finance 以外への転送は禁止
    if 'yahoo.com' not in url and 'yahoo.co.jp' not in url:
        return jsonify({'error': 'forbidden'}), 403
    try:
        r = req.get(url, headers=_YF_API_HEADERS, timeout=15)
        content_type = r.headers.get('Content-Type', 'application/json')
        return Response(r.content, status=r.status_code, content_type=content_type)
    except Exception as e:
        logging.warning(f'PROXY ERR {url}: {e}')
        return jsonify({'error': str(e)}), 502


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

    def fetch_one(ticker):
        cached = _cached_price(ticker)
        if cached:
            logging.info(f'CACHE {ticker}')
            return ticker, cached
        if is_fund_ticker(ticker):
            fund_code = ticker[:-2]
            data = get_fund_price_yfjp(fund_code)
        else:
            data = fetch_price_with_retry(ticker)
            logging.info(f'OK   {ticker}: {data["price"]} ({data["currency"]})')
        _store_price(ticker, data)
        return ticker, data

    # 並列取得 + 1銘柄あたり30秒タイムアウト
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(tickers), 8)) as ex:
        futures = {ex.submit(fetch_one, t): t for t in tickers}
        for fut in concurrent.futures.as_completed(futures, timeout=60):
            t = futures[fut]
            try:
                _, data = fut.result(timeout=30)
                result[t] = data
            except Exception as e:
                logging.warning(f'ERR  {t}: {e}')
                result[t] = {'price': None, 'prev_close': None, 'day_change': None, 'currency': None, 'error': str(e)}

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
        cached = _cached_hist(ticker, period)
        if cached is not None:
            result[ticker] = cached
            logging.info(f'CACHE HIST {ticker} ({period})')
            continue
        try:
            if is_fund_ticker(ticker):
                fund_code      = ticker[:-2]
                data           = get_fund_history_yfjp(fund_code, period)
                result[ticker] = data
                logging.info(f'HIST FUND {ticker}: {len(data)} bars ({period})')
            else:
                hist = yf_ticker(ticker).history(period=period)
                if hist.empty:
                    raise ValueError('データなし')

                data = []
                for dt, row in hist.iterrows():
                    close = safe_float(row['Close'])
                    if close is not None:
                        data.append({'date': dt.strftime('%Y-%m-%d'), 'close': round(close, 4)})

                result[ticker] = data
                logging.info(f'HIST {ticker}: {len(data)} bars ({period})')

            _store_hist(ticker, period, result[ticker])

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
            info = yf_ticker(ticker).info
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
        info = yf_ticker('USDJPY=X').fast_info
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
