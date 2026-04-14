import os
import sys
import re
import json
import math
import secrets
import logging
import threading
import webbrowser
import concurrent.futures
from urllib.parse import urlparse

import time
import requests as req
from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS
try:
    import yfinance as yf
    _YFINANCE_AVAILABLE = True
except ImportError:
    yf = None  # type: ignore
    _YFINANCE_AVAILABLE = False
    logging.warning('yfinance not available — falling back to Chart API only')


def yf_ticker(ticker: str):
    """yfinance 0.2.x は curl_cffi セッションを内部管理するため
    session パラメータは渡さず、ライブラリに任せる。"""
    if not _YFINANCE_AVAILABLE:
        raise ImportError('yfinance is not installed')
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

# Yahoo Finance Japan 用ブラウザ風ヘッダー（ボット検出回避）
_YFJP_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'ja-JP,ja;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Connection': 'keep-alive',
}

#後方互換のため _FUND_HEADERS_LIST も維持
_FUND_HEADERS_LIST = [_YFJP_HEADERS]

# ── Yahoo Finance Japan リクエスト（TLS フィンガープリント偽装）──
# curl_cffi は yfinance の依存ライブラリとしてインストール済み。
# Chrome の TLS ハンドシェイクを完全再現するため、
# Render などクラウドサーバーからのボット検出を回避できる。
_cffi_available: bool | None = None  # None=未チェック

_CF_CHALLENGE_MARKERS = (
    'cf-browser-verification',
    'Checking your browser',
    'Just a moment',
    'cf_chl_opt',
    'cf-please-wait',
    '__cf_chl_f_tk',
)

def _is_cf_challenge(html: str) -> bool:
    """Cloudflare JS チャレンジページかどうか判定"""
    return any(m in html for m in _CF_CHALLENGE_MARKERS)


# ── curl_cffi Session（Cookie 付き）─────────────────────────────────
# Cookieなしリクエストは Yahoo Finance Japan が HTTP 500 を返すことがある。
# Session でホームページを先に訪問してCookieを取得しておく。
_cffi_session = None
_cffi_session_ts: float = 0.0
_CFFI_SESSION_TTL = 1800


def _get_cffi_session():
    """curl_cffi Session を返す。未初期化または期限切れなら再構築。"""
    global _cffi_session, _cffi_session_ts
    if _cffi_session is not None and time.time() - _cffi_session_ts < _CFFI_SESSION_TTL:
        return _cffi_session
    try:
        from curl_cffi import requests as cffi_req  # type: ignore
        s = cffi_req.Session(impersonate='chrome124')
        s.headers.update({
            'Accept-Language': 'ja-JP,ja;q=0.9,en;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        # ホームページを訪問してCookieを取得
        try:
            s.get('https://finance.yahoo.co.jp/', timeout=10)
            logging.info('YFJP cffi session initialized (cookie acquired)')
        except Exception as e:
            logging.warning(f'YFJP cffi home page failed: {e}')
        _cffi_session = s
        _cffi_session_ts = time.time()
        return s
    except ImportError:
        return None


def _yfjp_get(url: str, timeout: int = 15):
    """Yahoo Finance Japan への GET。curl_cffi Session で Cookie + TLS 偽装。"""
    global _cffi_available
    if _cffi_available is not False:
        try:
            s = _get_cffi_session()
            if s is None:
                raise ImportError('curl_cffi not available')
            resp = s.get(
                url,
                headers={'Referer': 'https://finance.yahoo.co.jp/'},
                timeout=timeout,
            )
            _cffi_available = True
            if _is_cf_challenge(resp.text):
                logging.warning(f'YFJP curl_cffi: Cloudflare JS challenge (status={resp.status_code}) url={url}')
            else:
                logging.info(f'YFJP curl_cffi: status={resp.status_code} url={url}')
            return resp
        except ImportError:
            _cffi_available = False
            logging.warning('curl_cffi not installed — falling back to requests.Session')
        except Exception as e:
            logging.warning(f'curl_cffi request failed ({type(e).__name__}): {e}  url={url}')
            # セッションをリセットして次回再構築
            global _cffi_session, _cffi_session_ts
            _cffi_session = None
            _cffi_session_ts = 0.0

    # フォールバック: requests + Cookie セッション
    try:
        resp2 = _get_yfjp_session().get(url, timeout=timeout)
        if _is_cf_challenge(resp2.text):
            logging.warning(f'YFJP session: Cloudflare JS challenge (status={resp2.status_code}) url={url}')
        else:
            logging.info(f'YFJP session: status={resp2.status_code} url={url}')
        return resp2
    except Exception as e:
        logging.warning(f'YFJP session request failed ({type(e).__name__}): {e}  url={url}')
        raise


# ── Yahoo Finance US curl_cffi Session ───────────────────────────────
# query1/query2.finance.yahoo.com も Cookie + TLS 偽装が必要になった。
_cffi_us_session = None
_cffi_us_session_ts: float = 0.0
_CFFI_US_SESSION_TTL = 1800


def _get_cffi_us_session():
    """Yahoo Finance US 用 curl_cffi Session。未初期化または期限切れなら再構築。"""
    global _cffi_us_session, _cffi_us_session_ts
    if _cffi_us_session is not None and time.time() - _cffi_us_session_ts < _CFFI_US_SESSION_TTL:
        return _cffi_us_session
    try:
        from curl_cffi import requests as cffi_req  # type: ignore
        s = cffi_req.Session(impersonate='chrome124')
        s.headers.update({
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'application/json,text/html,*/*;q=0.8',
        })
        try:
            s.get('https://finance.yahoo.com/', timeout=10)
            logging.info('YFUS cffi session initialized (cookie acquired)')
        except Exception as e:
            logging.warning(f'YFUS cffi home page failed: {e}')
        _cffi_us_session = s
        _cffi_us_session_ts = time.time()
        return s
    except ImportError:
        return None


def _yfus_get(url: str, timeout: int = 15):
    """Yahoo Finance US Chart API への GET。curl_cffi で Cookie + TLS 偽装。"""
    s = _get_cffi_us_session()
    if s is not None:
        try:
            resp = s.get(url, headers={'Referer': 'https://finance.yahoo.com/'}, timeout=timeout)
            logging.info(f'YFUS cffi: status={resp.status_code} url={url}')
            return resp
        except Exception as e:
            logging.warning(f'YFUS cffi request failed ({type(e).__name__}): {e}  url={url}')
            global _cffi_us_session, _cffi_us_session_ts
            _cffi_us_session = None
            _cffi_us_session_ts = 0.0
    # フォールバック: 通常 requests
    return req.get(url, headers=_YF_API_HEADERS, timeout=timeout)


# ── requests セッション（curl_cffi 不使用時のフォールバック）────
_yfjp_session: req.Session | None = None
_yfjp_session_ts: float = 0.0
_YFJP_SESSION_TTL = 1800

def _get_yfjp_session() -> req.Session:
    global _yfjp_session, _yfjp_session_ts
    if _yfjp_session is None or time.time() - _yfjp_session_ts > _YFJP_SESSION_TTL:
        s = req.Session()
        s.headers.update(_YFJP_HEADERS)
        try:
            s.get('https://finance.yahoo.co.jp/', timeout=10)
            logging.info('YFJP session initialized (cookie acquired)')
        except Exception as e:
            logging.debug(f'YFJP session init failed: {e}')
        _yfjp_session = s
        _yfjp_session_ts = time.time()
    return _yfjp_session

_YF_API_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
}

_ALLOWED_PROXY_HOSTS = {
    'query1.finance.yahoo.com',
    'query2.finance.yahoo.com',
    'finance.yahoo.com',
    'finance.yahoo.co.jp',
}


def _require_internal_api_key() -> tuple[bool, tuple | None]:
    """Protect sensitive server-side endpoints with a shared secret header.

    Required env var:
      INTERNAL_API_KEY
    Required header:
      X-Internal-Key
    """
    expected = os.environ.get('INTERNAL_API_KEY', '').strip()
    if not expected:
        return False, (jsonify({'error': 'INTERNAL_API_KEY が未設定です'}), 503)
    provided = request.headers.get('X-Internal-Key', '').strip()
    if not provided or not secrets.compare_digest(provided, expected):
        return False, (jsonify({'error': 'unauthorized'}), 401)
    return True, None


def _is_allowed_proxy_url(url: str) -> bool:
    try:
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme not in {'http', 'https'}:
        return False
    host = (p.hostname or '').lower()
    return host in _ALLOWED_PROXY_HOSTS

def fetch_price_via_chart_api(ticker: str) -> dict:
    """Yahoo Finance Chart API を直接叩いて現在価格を取得（yfinance 非依存）"""
    import urllib.parse
    url = f'https://query2.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ticker)}?interval=1d&range=5d'
    r = _yfus_get(url, timeout=15)
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

    market_state = str(meta.get('marketState') or '').upper()

    # PRE だけは pre セッション値を使う（前日終値とのズレを避ける）。
    # それ以外は regular 基準を優先し、欠損時のみ post/pre へフォールバック。
    if market_state.startswith('PRE') and pre_price is not None and pre_change is not None:
        price, change = pre_price, pre_change
    elif reg_price is not None and reg_change is not None:
        price, change = reg_price, reg_change
    elif reg_price is not None:
        price, change = reg_price, None
    elif post_price is not None and post_change is not None:
        price, change = post_price, post_change
    elif pre_price is not None and pre_change is not None:
        price, change = pre_price, pre_change
    else:
        price, change = reg_price or post_price or pre_price, reg_change
    # 前日終値は Yahoo のメタ値を直接採用（逆算はフォールバック）
    closes = result.get('indicators', {}).get('quote', [{}])[0].get('close', [])
    valid_closes = [safe_float(c) for c in closes if safe_float(c) is not None]
    prev_from_bars = None
    if valid_closes:
        # price が最新終値と一致している場合は、その1つ前を前日終値とみなす。
        if len(valid_closes) >= 2 and price is not None and abs(price - valid_closes[-1]) <= max(0.01, abs(price) * 0.0001):
            prev_from_bars = valid_closes[-2]
        else:
            prev_from_bars = valid_closes[-1]

    prev = prev_from_bars if prev_from_bars is not None else safe_float(
        meta.get('chartPreviousClose')
        or meta.get('regularMarketPreviousClose')
        or meta.get('previousClose')
    )
    if prev is None and price is not None and change is not None:
        prev = safe_float(price - change)
    if price is not None and prev is not None:
        change = safe_float(price - prev)
    if price is None:
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


# __PRELOADED_STATE__ 内で基準価額が存在しうるJSONパス候補（優先順）
_FUND_STATE_PATHS: list[list[str]] = [
    ['mainFundPriceBoard', 'fundPrices'],
    ['fundPriceBoard',     'fundPrices'],
    ['priceBoard',         'fundPrices'],
    ['mainFundPriceBoard'],
    ['fundPriceBoard'],
    ['priceBoard'],
    ['mainQuote',          'quote'],
    ['quote'],
]

# 価格・変動キー候補
_PRICE_KEYS  = ('price', 'currentPrice', 'nav', 'basePrice', 'regularMarketPrice')
_CHANGE_KEYS = ('changePrice', 'change', 'priceChange', 'diff', 'regularMarketChange')


def _price_from_obj(obj: dict) -> tuple[str | None, str]:
    """dict から価格文字列と変動文字列を取り出す"""
    if not isinstance(obj, dict):
        return None, '0'
    for pk in _PRICE_KEYS:
        val = obj.get(pk)
        if val is not None and str(val).replace(',', '').replace('.', '').lstrip('-').isdigit():
            change_val = '0'
            for ck in _CHANGE_KEYS:
                cv = obj.get(ck)
                if cv is not None:
                    change_val = str(cv)
                    break
            return str(val), change_val
    return None, '0'


def _deep_search_price(obj, depth: int = 0) -> tuple[str | None, str]:
    """state オブジェクトを再帰的に探索してファンド価格を見つける（最大深さ6）"""
    if depth > 6 or not isinstance(obj, dict):
        return None, '0'
    p, c = _price_from_obj(obj)
    if p:
        return p, c
    for child in obj.values():
        if isinstance(child, dict):
            p, c = _deep_search_price(child, depth + 1)
            if p:
                return p, c
    return None, '0'


def _parse_fund_price_from_html(html: str) -> tuple[float | None, float]:
    """HTML 内の JSON パターンから価格を正規表現で抽出（最終手段）"""
    # __NEXT_DATA__ (Next.js SSR) を試みる
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if m:
        try:
            data = json.loads(m.group(1))
            props = data.get('props', {}).get('pageProps', data)
            p_str, c_str = _deep_search_price(props)
            if p_str:
                return (float(p_str.replace(',', '')),
                        float(str(c_str).replace('△', '-').replace(',', '')))
        except Exception:
            pass

    # JSON文字列中の価格パターン (例: "price":"12,345" / "price":12345)
    price_pats = [
        r'"(?:price|basePrice|nav|currentPrice)"\s*:\s*"([\d,]+(?:\.\d+)?)"',
        r'"(?:price|basePrice|nav|currentPrice)"\s*:\s*([\d]+(?:\.\d+)?)',
    ]
    change_pats = [
        r'"(?:changePrice|priceChange|change|diff)"\s*:\s*"([△▲\-\+]?[\d,]+(?:\.\d+)?)"',
        r'"(?:changePrice|priceChange|change|diff)"\s*:\s*([△▲\-\+]?[\d]+(?:\.\d+)?)',
    ]
    price  = None
    change = 0.0
    for pat in price_pats:
        m2 = re.search(pat, html)
        if m2:
            try:
                v = float(m2.group(1).replace(',', ''))
                if v > 0:
                    price = v
                    break
            except Exception:
                pass
    if price:
        for pat in change_pats:
            m2 = re.search(pat, html)
            if m2:
                try:
                    change = float(str(m2.group(1)).replace('△', '-').replace('▲', '-').replace(',', ''))
                    break
                except Exception:
                    pass
    return price, change


def _build_fund_result(price: float, change: float, source: str, fund_code: str, update_date: str | None = None) -> dict:
    prev = round(price - change, 4)
    logging.info(f'FUND ({source}) {fund_code}: {price} JPY (前日比 {change:+.4f}) updateDate={update_date}')
    return {'price': price, 'prev_close': prev, 'day_change': change, 'currency': 'JPY', 'error': None, 'nav_date': update_date}


def _fetch_fund_price_via_quote_api(ticker: str) -> dict:
    """Yahoo Finance US の /v7/finance/quote で投資信託価格を取得。
    Render など米国サーバーからでも安定して動作する。"""
    import urllib.parse as _up
    for host in ('query1', 'query2'):
        try:
            url = f'https://{host}.finance.yahoo.com/v7/finance/quote?symbols={_up.quote(ticker)}'
            r = _yfus_get(url, timeout=12)
            r.raise_for_status()
            result = r.json().get('quoteResponse', {}).get('result', [])
            if not result:
                continue
            q = result[0]
            price = safe_float(q.get('regularMarketPrice'))
            if price is None:
                continue
            change    = safe_float(q.get('regularMarketChange'))
            prev      = safe_float(q.get('regularMarketPreviousClose'))
            # change が未取得の場合は prev_close から逆算
            if change is None and prev is not None:
                change = round(price - prev, 4)
            if prev is None and change is not None:
                prev = round(price - change, 4)
            currency  = q.get('currency') or 'JPY'
            return {
                'price':      price,
                'prev_close': prev,
                'day_change': change,
                'currency':   currency,
                'error':      None,
                'nav_date':   None,
            }
        except Exception as e:
            logging.debug(f'FUND quote_api {host} failed {ticker}: {e}')
    raise ValueError(f'quote API で価格取得失敗: {ticker}')


def _fetch_minkabu_price(fund_code: str) -> dict:
    """みんかぶ投資信託 (itf.minkabu.jp) から基準価額を取得。
    Yahoo Finance Japan が Cloudflare でブロックされる場合のフォールバック。
    同じ8桁ファンドコードが使えるため、別コード変換不要。
    """
    url = f'https://itf.minkabu.jp/fund/{fund_code}'
    r = req.get(url, headers={
        'User-Agent':      _YFJP_HEADERS['User-Agent'],
        'Accept-Language': 'ja-JP,ja;q=0.9,en;q=0.8',
        'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Referer':         'https://itf.minkabu.jp/',
    }, timeout=15)
    if r.status_code == 404:
        raise ValueError(f'minkabu: {fund_code} が見つかりません')
    r.raise_for_status()
    html = r.text

    price: float | None = None
    change = 0.0
    nav_date: str | None = None

    # ── 方式A: meta description から取得（最も安定）───────────────
    # <meta name="description" content="...基準価額60673円、前日比-1174(-1.9%)...">
    mm = re.search(r'name="description"[^>]*content="([^"]{30,})"', html, re.IGNORECASE)
    if not mm:
        mm = re.search(r'content="([^"]{30,})"[^>]*name="description"', html, re.IGNORECASE)
    if mm:
        meta = mm.group(1)
        mp = re.search(r'(\d{4,6})', meta)   # 最初の4〜6桁 = 基準価額
        if mp:
            price = float(mp.group(1))
            # 「前日比+NNN」「前日比-NNN」の両パターンに対応
            mc = re.search(r'前日比([+\-]?\d{1,5})', meta)
            if mc:
                change = float(mc.group(1))

    # ── 方式B: HTML body から取得（フォールバック）────────────────
    if price is None:
        for pat in (
            r'class="[^"]*font-bold[^"]*text-2xl[^"]*"[^>]*>\s*([\d,]+)',
            r'class="[^"]*text-2xl[^"]*font-bold[^"]*"[^>]*>\s*([\d,]+)',
            r'(\d{4,6})\s*\xe5\x86\x86',  # UTF-8 "円"
        ):
            m = re.search(pat, html)
            if m:
                price = float(m.group(1).replace(',', ''))
                break

    if price is None or price <= 0:
        raise ValueError(f'minkabu: 基準価額を解析できません ({fund_code})')

    nav_date = None  # みんかぶは日付の信頼できるパターンがないため省略

    logging.info(f'FUND (minkabu) {fund_code}: {price} JPY (前日比 {change:+.0f}) date={nav_date}')
    return _build_fund_result(price, change, 'minkabu', fund_code, nav_date)


def get_fund_price_yfjp(fund_code: str) -> dict:
    """投資信託の基準価額を取得（6段階フォールバック）

    方式0: Yahoo Finance US Quote API
    方式1: Yahoo Finance JP __PRELOADED_STATE__ 複数パス探索
    方式2: Yahoo Finance JP HTML 正規表現 / __NEXT_DATA__
    方式3: Yahoo Finance US Chart API (.T ティッカー)
    方式4: みんかぶ投資信託 (Cloudflare 非依存)
    """
    ticker = f'{fund_code}.T'

    # ── 方式0: Yahoo Finance US Quote API ────────────────────────
    try:
        result = _fetch_fund_price_via_quote_api(ticker)
        logging.info(f'FUND (quote_api) {fund_code}: {result["price"]}')
        return result
    except Exception as e:
        logging.debug(f'FUND quote_api failed {fund_code}: {e}')

    url  = f'https://finance.yahoo.co.jp/quote/{fund_code}'
    html = None

    # ── HTML 取得（curl_cffi TLS 偽装 + リトライ）───────────────
    for attempt in range(3):
        try:
            r = _yfjp_get(url, timeout=15)
            if r.status_code == 404:
                raise ValueError(f'ファンド {fund_code} が見つかりません (404)')
            if r.status_code in (403, 429):
                # セッションリセットして再試行
                global _yfjp_session, _yfjp_session_ts
                _yfjp_session = None
                _yfjp_session_ts = 0.0
                raise IOError(f'HTTP {r.status_code}')
            r.raise_for_status()
            html = r.text
            break
        except ValueError:
            raise
        except Exception as e:
            logging.debug(f'FUND fetch attempt {attempt+1} failed {fund_code}: {e}')
            if attempt < 2:
                time.sleep(1.5 * (attempt + 1))

    if html is None:
        # HTML 取得自体が全試行失敗 → みんかぶ → Chart API の順で試す
        try:
            return _fetch_minkabu_price(fund_code)
        except Exception as e:
            logging.debug(f'FUND minkabu(no-html) failed {fund_code}: {e}')
        try:
            result = fetch_price_via_chart_api(f'{fund_code}.T')
            logging.info(f'FUND (chart_api/no-html) {fund_code}: {result["price"]}')
            return result
        except Exception as e:
            raise ValueError(f'基準価額取得失敗 ({fund_code}): HTML取得もChart APIも失敗: {e}')

    # ── 方式1: __PRELOADED_STATE__ ───────────────────────────────
    try:
        state = _extract_preloaded_state(html)
        price_str, change_str, update_date = None, '0', None

        # 候補パスを順に試行
        for path in _FUND_STATE_PATHS:
            obj = state
            for key in path:
                obj = obj.get(key, {}) if isinstance(obj, dict) else {}
            p, c = _price_from_obj(obj)
            if p:
                price_str, change_str = p, c
                # updateDate を取得（fundPrices に直接ある場合）
                update_date = str(obj.get('updateDate') or obj.get('date') or '')  or None
                break

        # パスで見つからなければ再帰探索
        if not price_str:
            price_str, change_str = _deep_search_price(state)

        if price_str:
            price  = float(str(price_str).replace(',', ''))
            change = float(str(change_str).replace('△', '-').replace('▲', '-').replace(',', ''))
            return _build_fund_result(price, change, 'state', fund_code, update_date)
    except Exception as e:
        logging.debug(f'FUND state parse failed {fund_code}: {e}')

    # ── 方式2: HTML 正規表現 / __NEXT_DATA__ ────────────────────
    try:
        price, change = _parse_fund_price_from_html(html)
        if price and price > 0:
            return _build_fund_result(price, change, 'html-regex', fund_code)
    except Exception as e:
        logging.debug(f'FUND html-regex failed {fund_code}: {e}')

    # ── 方式3: Yahoo Finance US Chart API (.T) ───────────────────
    try:
        result = fetch_price_via_chart_api(f'{fund_code}.T')
        logging.info(f'FUND (chart_api) {fund_code}: {result["price"]}')
        return result
    except Exception as e:
        logging.debug(f'FUND chart_api failed {fund_code}: {e}')

    # ── 方式4: みんかぶ投資信託 (Cloudflare 非依存フォールバック) ──
    try:
        return _fetch_minkabu_price(fund_code)
    except Exception as e:
        logging.debug(f'FUND minkabu failed {fund_code}: {e}')

    raise ValueError(f'基準価額取得失敗 ({fund_code}): 全方式が失敗しました')


def get_fund_history_yfjp(fund_code: str, period: str) -> list:
    """Yahoo Finance Japan から投資信託の基準価額履歴を取得（複数方式フォールバック）"""
    from datetime import date as _date

    # ── 方式1: チャートページの __PRELOADED_STATE__ ──────────────
    chart_url = f'https://finance.yahoo.co.jp/quote/{fund_code}/chart'
    for _ in range(2):
        try:
            r = _yfjp_get(chart_url, timeout=15)
            r.raise_for_status()
            state = _extract_preloaded_state(r.text)
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
                        try:
                            result.append({'date': str(d)[:10], 'close': float(str(c).replace(',', ''))})
                        except Exception:
                            pass
                if result:
                    logging.info(f'FUND HIST (state) {fund_code}: {len(result)} bars ({period})')
                    return result
        except Exception as e:
            logging.debug(f'FUND HIST state failed {fund_code}: {e}')
            time.sleep(0.5)

    # ── 方式2: Yahoo Finance US Chart API (.T) ───────────────────
    try:
        import urllib.parse
        t = urllib.parse.quote(f'{fund_code}.T')
        _period_map = {'5d': '5d', '1mo': '1mo', '3mo': '3mo', '6mo': '6mo',
                       '1y': '1y', '2y': '2y', '5y': '5y'}
        yf_period = _period_map.get(period, '1mo')
        r = _yfus_get(
            f'https://query2.finance.yahoo.com/v8/finance/chart/{t}?interval=1d&range={yf_period}',
            timeout=15
        )
        r.raise_for_status()
        data = r.json()
        timestamps = data['chart']['result'][0].get('timestamp', [])
        closes = data['chart']['result'][0].get('indicators', {}).get('quote', [{}])[0].get('close', [])
        result = []
        for ts, c in zip(timestamps, closes):
            v = safe_float(c)
            if v is not None and ts:
                from datetime import datetime, timezone
                dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')
                result.append({'date': dt, 'close': round(v, 4)})
        if result:
            logging.info(f'FUND HIST (chart_api) {fund_code}: {len(result)} bars ({period})')
            return result
    except Exception as e:
        logging.debug(f'FUND HIST chart_api failed {fund_code}: {e}')

    # ── 方式3: 現在価格のみ1点返す ──────────────────────────────
    try:
        p = get_fund_price_yfjp(fund_code)
        logging.info(f'FUND HIST (current-only) {fund_code}: 1 bar')
        return [{'date': _date.today().isoformat(), 'close': p['price']}]
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
    ok, err = _require_internal_api_key()
    if not ok:
        return err

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

    ok, err = _require_internal_api_key()
    if not ok:
        return err

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
        try:
            for fut in concurrent.futures.as_completed(futs, timeout=90):
                t = futs[fut]
                try:
                    _, d = fut.result(timeout=30)
                    prices[t] = d
                except Exception as e:
                    logging.warning(f'SNAPSHOT price err {t}: {e}')
        except concurrent.futures.TimeoutError:
            logging.warning('SNAPSHOT price fetch timeout: partial data will be used')
            for fut, t in futs.items():
                if not fut.done():
                    fut.cancel()
                    logging.warning(f'SNAPSHOT price timeout {t}')

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
    # Yahoo Finance の許可済みホスト以外への転送は禁止
    if not _is_allowed_proxy_url(url):
        return jsonify({'error': 'forbidden'}), 403
    try:
        # Yahoo Finance Japan / US ともに curl_cffi で TLS 偽装リクエスト
        parsed = urlparse(url)
        if (parsed.hostname or '').endswith('finance.yahoo.co.jp'):
            r = _yfjp_get(url, timeout=15)
        else:
            r = _yfus_get(url, timeout=15)
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
        try:
            for fut in concurrent.futures.as_completed(futures, timeout=60):
                t = futures[fut]
                try:
                    _, data = fut.result(timeout=30)
                    result[t] = data
                except Exception as e:
                    logging.warning(f'ERR  {t}: {e}')
                    result[t] = {'price': None, 'prev_close': None, 'day_change': None, 'currency': None, 'error': str(e)}
        except concurrent.futures.TimeoutError:
            logging.warning('PRICE fetch timeout: return partial result')

        for fut, t in futures.items():
            if fut.done():
                continue
            fut.cancel()
            if t not in result:
                result[t] = {'price': None, 'prev_close': None, 'day_change': None, 'currency': None, 'error': 'timeout'}

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

    need_fetch = []
    for ticker in fetch_set:
        cached = _cached_hist(ticker, period)
        if cached is not None:
            result[ticker] = cached
            logging.info(f'CACHE HIST {ticker} ({period})')
        else:
            need_fetch.append(ticker)

    def fetch_hist_one(ticker):
        if is_fund_ticker(ticker):
            fund_code = ticker[:-2]
            data = get_fund_history_yfjp(fund_code, period)
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
            logging.info(f'HIST {ticker}: {len(data)} bars ({period})')
        return ticker, data

    if need_fetch:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(need_fetch), 8)) as ex:
            futures = {ex.submit(fetch_hist_one, t): t for t in need_fetch}
            try:
                for fut in concurrent.futures.as_completed(futures, timeout=60):
                    t = futures[fut]
                    try:
                        _, data = fut.result(timeout=30)
                        result[t] = data
                        _store_hist(t, period, data)
                    except Exception as e:
                        logging.warning(f'HIST ERR {t}: {e}')
                        result[t] = []
            except concurrent.futures.TimeoutError:
                logging.warning('HIST fetch timeout: return partial result')
            for fut, t in futures.items():
                if not fut.done():
                    fut.cancel()
                if t not in result:
                    result[t] = []

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


@app.route('/api/debug/fund')
def debug_fund():
    """GET /api/debug/fund?code=0131103C
    Render上での投資信託価格取得の詳細診断。
    各方式を順に試して結果を返す（セキュリティ上 INTERNAL_API_KEY 必須）。
    """
    ok, err = _require_internal_api_key()
    if not ok:
        return err

    fund_code = request.args.get('code', '0131103C').strip()
    ticker    = f'{fund_code}.T'
    results   = {}

    # 1. curl_cffi 利用可否
    try:
        from curl_cffi import requests as cffi_req  # type: ignore
        results['curl_cffi_available'] = True
        results['curl_cffi_version']   = getattr(cffi_req, '__version__', 'unknown')
    except ImportError as e:
        results['curl_cffi_available'] = False
        results['curl_cffi_error']     = str(e)

    # 2. curl_cffi で Yahoo Finance Japan に接続
    url = f'https://finance.yahoo.co.jp/quote/{fund_code}'
    try:
        from curl_cffi import requests as cffi_req  # type: ignore
        r = cffi_req.get(
            url,
            impersonate='chrome124',
            headers={'Accept-Language': 'ja-JP,ja;q=0.9', 'Referer': 'https://finance.yahoo.co.jp/'},
            timeout=15,
        )
        has_state = '__PRELOADED_STATE__' in r.text
        results['cffi_fetch'] = {
            'status': r.status_code,
            'has_preloaded_state': has_state,
            'body_head': r.text[:300],
        }
    except Exception as e:
        results['cffi_fetch'] = {'error': str(e)}

    # 3. requests.Session で Yahoo Finance Japan に接続
    try:
        s = req.Session()
        s.headers.update(_YFJP_HEADERS)
        r2 = s.get(url, timeout=15)
        has_state2 = '__PRELOADED_STATE__' in r2.text
        results['session_fetch'] = {
            'status': r2.status_code,
            'has_preloaded_state': has_state2,
            'body_head': r2.text[:300],
        }
    except Exception as e:
        results['session_fetch'] = {'error': str(e)}

    # 4. Yahoo Finance US Chart API
    try:
        import urllib.parse
        r3 = req.get(
            f'https://query2.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ticker)}?interval=1d&range=2d',
            headers=_YF_API_HEADERS, timeout=15,
        )
        results['chart_api'] = {
            'status': r3.status_code,
            'body_head': r3.text[:300],
        }
    except Exception as e:
        results['chart_api'] = {'error': str(e)}

    # 5. 実際の価格取得
    try:
        data = get_fund_price_yfjp(fund_code)
        results['price_result'] = data
    except Exception as e:
        results['price_result'] = {'error': str(e)}

    return jsonify(results)


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
