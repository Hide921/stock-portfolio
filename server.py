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
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None
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


def _fetch_price_via_quote_summary(ticker: str) -> dict:
    """Yahoo Finance v10 quoteSummary API で価格を取得（curl_cffi + crumb 使用）。
    Chart API が 404 を返す投資信託コードや特殊ティッカーに対して有効。
    regularMarketPreviousClose も取得できるため前日比が正確。"""
    import urllib.parse as _up
    url = f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{_up.quote(ticker)}?modules=price'
    r = _yfus_get(url, timeout=15)
    if r.status_code == 404:
        raise TickerNotFoundError(f'quoteSummary 404: {ticker}')
    r.raise_for_status()
    data = r.json()
    result = (data.get('quoteSummary') or {}).get('result') or []
    if not result:
        raise ValueError(f'quoteSummary: データなし ({ticker})')
    price_data = result[0].get('price', {})
    price  = safe_float((price_data.get('regularMarketPrice')  or {}).get('raw'))
    prev   = safe_float((price_data.get('regularMarketPreviousClose') or {}).get('raw'))
    change = safe_float((price_data.get('regularMarketChange') or {}).get('raw'))
    if price is None:
        raise ValueError(f'quoteSummary: price is None ({ticker})')
    currency = price_data.get('currency') or ('JPY' if ticker.endswith('.T') else 'USD')
    if change is None and prev is not None:
        change = round(price - prev, 4)
    if prev is None and change is not None:
        prev = round(price - change, 4)
    logging.info(f'OK quoteSummary {ticker}: {price} {currency} (prev={prev})')
    return {
        'price':      round(price, 4),
        'prev_close': round(prev, 4) if prev is not None else None,
        'day_change': round(change, 4) if change is not None else None,
        'currency':   currency,
        'error':      None,
    }


def fetch_price_with_retry(ticker: str, retries: int = 2) -> dict:
    """Chart API → v10 quoteSummary → yfinance fast_info の順で取得。
    Yahoo Finance が 404 を返した場合（TickerNotFoundError）は quoteSummary のみ
    試行し、yfinance fast_info はスキップ（同じサーバに再問合せても無駄なため）。"""
    chart_404 = False
    # 1st: Yahoo Finance Chart API (最も信頼性が高い)
    try:
        return fetch_price_via_chart_api(ticker)
    except TickerNotFoundError as e:
        chart_404 = True
        logging.debug(f'Chart API 404 for {ticker}: {e}')
    except Exception as e:
        logging.debug(f'Chart API failed for {ticker}: {e}')

    # 2nd: v10 quoteSummary（curl_cffi + crumb。Chart APIが404の投信コードに有効）
    try:
        return _fetch_price_via_quote_summary(ticker)
    except TickerNotFoundError as e:
        # Chart と quoteSummary の双方が 404 ならティッカー不存在確定
        if chart_404:
            raise ValueError(f'ティッカー不存在: {ticker}') from e
        logging.debug(f'quoteSummary 404 for {ticker}: {e}')
    except Exception as e:
        logging.debug(f'quoteSummary failed for {ticker}: {e}')

    # 3rd: yfinance fast_info (最終バックアップ)
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


def fetch_prices_batch(tickers: list) -> dict:
    """Yahoo Finance v7 quote API で複数ティッカーを一括取得。
    1リクエストで多数取得できるため、全個別取得より高速 & レート制限に優しい。
    取得できなかったシンボルは返さない（呼び出し側で個別フォールバック）。
    API が 401/403/404/500 や空結果を返した場合は空 dict を返す。"""
    import urllib.parse as _up
    if not tickers:
        return {}
    # 重複排除（順序は維持しなくてよい）
    unique = list({t for t in tickers if t})
    if not unique:
        return {}
    # Yahoo の実績的上限は 50 程度。安全のため 40 でチャンク分割。
    CHUNK = 40
    out: dict = {}
    for i in range(0, len(unique), CHUNK):
        chunk = unique[i:i + CHUNK]
        symbols = ','.join(chunk)
        url = f'https://query2.finance.yahoo.com/v7/finance/quote?symbols={_up.quote(symbols)}'
        try:
            r = _yfus_get(url, timeout=20)
        except Exception as e:
            logging.warning(f'BATCH quote request failed: {e}')
            continue
        if r.status_code != 200:
            logging.warning(f'BATCH quote non-200: status={r.status_code}')
            continue
        try:
            data = r.json()
        except Exception as e:
            logging.warning(f'BATCH quote JSON parse failed: {e}')
            continue
        results = (data.get('quoteResponse') or {}).get('result') or []
        for item in results:
            sym = item.get('symbol')
            if not sym:
                continue
            price  = safe_float(item.get('regularMarketPrice'))
            prev   = safe_float(item.get('regularMarketPreviousClose'))
            change = safe_float(item.get('regularMarketChange'))
            if price is None:
                continue
            if change is None and prev is not None:
                change = round(price - prev, 4)
            if prev is None and change is not None:
                prev = round(price - change, 4)
            currency = item.get('currency') or ('JPY' if sym.endswith('.T') else 'USD')
            out[sym] = {
                'price':      round(price, 4),
                'prev_close': round(prev, 4) if prev is not None else None,
                'day_change': round(change, 4) if change is not None else None,
                'currency':   currency,
                'error':      None,
            }
    if out:
        logging.info(f'BATCH quote: {len(out)}/{len(unique)} tickers resolved')
    return out


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

# ── Yahoo Finance レート制限防止スロットラー ─────────────────────────
# US/JP はドメインが別なのでスロットラーを分離し、それぞれ独立して叩く。
# 全共有にすると片方の待ち時間でもう一方もブロックされ、並列効率が落ちる。
_YF_THROTTLE_INTERVAL = 0.15  # 最小リクエスト間隔(秒)

_yf_throttle_us_lock = threading.Lock()
_yf_throttle_us_ts: float = 0.0
_yf_throttle_jp_lock = threading.Lock()
_yf_throttle_jp_ts: float = 0.0


def _throttle_yf_us():
    """Yahoo Finance US (query1/query2.finance.yahoo.com) 用スロットル"""
    global _yf_throttle_us_ts
    with _yf_throttle_us_lock:
        now = time.time()
        wait = _YF_THROTTLE_INTERVAL - (now - _yf_throttle_us_ts)
        if wait > 0:
            time.sleep(wait)
        _yf_throttle_us_ts = time.time()


def _throttle_yf_jp():
    """Yahoo Finance Japan (finance.yahoo.co.jp) 用スロットル"""
    global _yf_throttle_jp_ts
    with _yf_throttle_jp_lock:
        now = time.time()
        wait = _YF_THROTTLE_INTERVAL - (now - _yf_throttle_jp_ts)
        if wait > 0:
            time.sleep(wait)
        _yf_throttle_jp_ts = time.time()


# 後方互換: 既存呼び出し側が残っている場合の保険（新規コードでは使わない）
def _throttle_yf():
    _throttle_yf_us()


def _parse_retry_after(header_value: str | None) -> float | None:
    """Retry-After ヘッダを秒数に解釈。失敗時は None。"""
    if not header_value:
        return None
    try:
        return float(header_value)
    except (TypeError, ValueError):
        return None


class TickerNotFoundError(Exception):
    """Yahoo Finance が 404 を返したティッカー。後続フォールバックをスキップ。"""
    pass


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


_YF_BACKOFF_SCHEDULE = (5, 10, 20)  # 429 リトライ時の待機秒数（上限60）
_YF_BACKOFF_CAP = 60

def _yfjp_get(url: str, timeout: int = 15):
    """Yahoo Finance Japan への GET。curl_cffi Session で Cookie + TLS 偽装。
    429 時は指数バックオフ（Retry-After を尊重）で最大3回リトライ。"""
    global _cffi_available, _cffi_session, _cffi_session_ts

    for attempt in range(len(_YF_BACKOFF_SCHEDULE) + 1):
        _throttle_yf_jp()
        resp = None
        try:
            if _cffi_available is not False:
                s = _get_cffi_session()
                if s is None:
                    _cffi_available = False
                    raise ImportError('curl_cffi not available')
                resp = s.get(
                    url,
                    headers={'Referer': 'https://finance.yahoo.co.jp/'},
                    timeout=timeout,
                )
                _cffi_available = True
                label = 'YFJP curl_cffi'
            else:
                resp = _get_yfjp_session().get(url, timeout=timeout)
                label = 'YFJP session'
        except ImportError:
            _cffi_available = False
            logging.warning('curl_cffi not installed — falling back to requests.Session')
            continue
        except Exception as e:
            logging.warning(f'YFJP request failed ({type(e).__name__}): {e}  url={url}')
            _cffi_session = None
            _cffi_session_ts = 0.0
            if attempt < len(_YF_BACKOFF_SCHEDULE):
                time.sleep(1)
                continue
            raise

        if resp.status_code == 429 and attempt < len(_YF_BACKOFF_SCHEDULE):
            wait = _parse_retry_after(resp.headers.get('Retry-After')) or _YF_BACKOFF_SCHEDULE[attempt]
            wait = min(wait, _YF_BACKOFF_CAP)
            logging.warning(f'{label}: 429 Too Many Requests — backoff {wait}s (attempt={attempt+1}/{len(_YF_BACKOFF_SCHEDULE)+1}) url={url}')
            _cffi_session = None
            _cffi_session_ts = 0.0
            time.sleep(wait)
            continue

        if _is_cf_challenge(resp.text):
            logging.warning(f'{label}: Cloudflare JS challenge (status={resp.status_code}) url={url}')
        else:
            logging.info(f'{label}: status={resp.status_code} url={url}')
        return resp

    # ここには通常到達しない（例外で抜ける想定）
    return resp


# ── Yahoo Finance US curl_cffi Session + Crumb ───────────────────────
# query1/query2.finance.yahoo.com は Cookie + crumb + TLS 偽装が必要。
# crumb は finance.yahoo.com でCookieを取得後、/v1/test/getcrumb で入手する。
_cffi_us_session = None
_cffi_us_session_ts: float = 0.0
_cffi_us_crumb: str | None = None
_CFFI_US_SESSION_TTL = 1800
_cffi_us_lock = threading.Lock()


def _get_cffi_us_session(force: bool = False):
    """Yahoo Finance US 用 curl_cffi Session と crumb を返す。
    未初期化・期限切れ・force=True なら再構築。"""
    global _cffi_us_session, _cffi_us_session_ts, _cffi_us_crumb
    with _cffi_us_lock:
        if (not force
                and _cffi_us_session is not None
                and _cffi_us_crumb is not None
                and time.time() - _cffi_us_session_ts < _CFFI_US_SESSION_TTL):
            return _cffi_us_session, _cffi_us_crumb
        try:
            from curl_cffi import requests as cffi_req  # type: ignore
            s = cffi_req.Session(impersonate='chrome124')
            s.headers.update({
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'application/json,text/html,*/*;q=0.8',
            })
            # まずホームページを訪問して Cookie を取得
            try:
                s.get('https://finance.yahoo.com/', timeout=10)
                logging.info('YFUS cffi session: cookie acquired from homepage')
            except Exception as e:
                logging.warning(f'YFUS cffi home page failed: {e}')
            # crumb を取得（Cookie 取得後に呼び出す必要がある）
            crumb = None
            for host in ('query2', 'query1'):
                try:
                    cr = s.get(
                        f'https://{host}.finance.yahoo.com/v1/test/getcrumb',
                        headers={'Referer': 'https://finance.yahoo.com/'},
                        timeout=10,
                    )
                    if cr.status_code == 200 and cr.text.strip():
                        crumb = cr.text.strip()
                        logging.info(f'YFUS cffi crumb acquired via {host} (len={len(crumb)})')
                        break
                except Exception as e:
                    logging.debug(f'YFUS crumb fetch failed ({host}): {e}')
            if not crumb:
                logging.warning('YFUS cffi: crumb 取得失敗 — クラム無しで継続')
                crumb = ''
            _cffi_us_session = s
            _cffi_us_crumb = crumb
            _cffi_us_session_ts = time.time()
            return s, crumb
        except ImportError:
            return None, None


def _reset_cffi_us_session():
    global _cffi_us_session, _cffi_us_session_ts, _cffi_us_crumb
    with _cffi_us_lock:
        _cffi_us_session = None
        _cffi_us_session_ts = 0.0
        _cffi_us_crumb = None


def _yfus_get(url: str, timeout: int = 15):
    """Yahoo Finance US API への GET。curl_cffi で Cookie + crumb + TLS 偽装。
    401/403 時はセッション＋crumb をリセットしてリトライ。
    429 時は指数バックオフ（5s→10s→20s、Retry-After があれば尊重）で最大3回。
    404 はそのまま返す（呼び出し側で TickerNotFoundError に変換）。"""
    import urllib.parse as _urlparse

    def _inject_crumb(u: str, crumb: str) -> str:
        if not crumb:
            return u
        sep = '&' if '?' in u else '?'
        return f'{u}{sep}crumb={_urlparse.quote(crumb)}'

    max_attempts = len(_YF_BACKOFF_SCHEDULE) + 1  # 計4回
    auth_retry_used = False
    for attempt in range(max_attempts):
        _throttle_yf_us()
        s, crumb = _get_cffi_us_session(force=(attempt > 0))
        if s is None:
            # curl_cffi が使えない場合は素の requests にフォールバック（1回で終了）
            return req.get(url, headers=_YF_API_HEADERS, timeout=timeout)
        target = _inject_crumb(url, crumb or '')
        try:
            resp = s.get(target, headers={'Referer': 'https://finance.yahoo.com/'}, timeout=timeout)
        except Exception as e:
            logging.warning(f'YFUS cffi request failed ({type(e).__name__}): {e}  url={target}')
            _reset_cffi_us_session()
            if attempt < max_attempts - 1:
                time.sleep(1)
                continue
            # 最終フォールバック: 素の requests
            return req.get(url, headers=_YF_API_HEADERS, timeout=timeout)

        logging.info(f'YFUS cffi: status={resp.status_code} url={target}')

        if resp.status_code == 404:
            # ティッカー不存在はここで即返す。呼び出し側が判断。
            return resp

        if resp.status_code == 429 and attempt < max_attempts - 1:
            wait = _parse_retry_after(resp.headers.get('Retry-After')) or _YF_BACKOFF_SCHEDULE[attempt]
            wait = min(wait, _YF_BACKOFF_CAP)
            logging.warning(f'YFUS cffi: 429 — backoff {wait}s (attempt={attempt+1}/{max_attempts}) url={url}')
            _reset_cffi_us_session()
            time.sleep(wait)
            continue

        if resp.status_code in (401, 403) and not auth_retry_used:
            logging.warning(f'YFUS cffi: {resp.status_code} — crumb 期限切れ、セッションをリセット')
            _reset_cffi_us_session()
            auth_retry_used = True
            continue

        return resp

    # 規定回数超過時: 素の requests で最終試行
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
    """Yahoo Finance ドメインへの転送のみ許可（SSRF 対策）。
    userinfo (`user@host` 形式) を含む URL は拒否（host 偽装攻撃を防ぐ）。"""
    try:
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme not in {'http', 'https'}:
        return False
    # userinfo 経由の host 偽装を防止（例: https://finance.yahoo.com@attacker.com/）
    if p.username or p.password or '@' in (p.netloc or ''):
        return False
    host = (p.hostname or '').lower()
    # ホスト名に明示ポートが含まれる場合も拒否（:80/:443 以外）
    if p.port is not None and p.port not in (80, 443):
        return False
    return host in _ALLOWED_PROXY_HOSTS

def fetch_price_via_chart_api(ticker: str) -> dict:
    """Yahoo Finance Chart API を直接叩いて現在価格を取得（yfinance 非依存）"""
    import urllib.parse
    url = f'https://query2.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ticker)}?interval=1d&range=5d'
    r = _yfus_get(url, timeout=15)
    if r.status_code == 404:
        raise TickerNotFoundError(f'Chart API 404: {ticker}')
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

    # /chart URL は Render など海外サーバーからでもアクセス可能。
    # /quote/{code} は Yahoo Japan の IP制限でブロックされることがある。
    urls_to_try = [
        f'https://finance.yahoo.co.jp/quote/{fund_code}/chart',  # 海外IP対応
        f'https://finance.yahoo.co.jp/quote/{fund_code}',        # 国内IP用
    ]
    html = None
    _YFJP_BLOCK_MARKERS = ('表示できません', 'ご覧になろうとしているページ', 'Just a moment', 'cf-browser-verification')

    # ── HTML 取得（curl_cffi TLS 偽装 + リトライ）───────────────
    for url in urls_to_try:
        for attempt in range(2):
            try:
                r = _yfjp_get(url, timeout=15)
                if r.status_code == 404:
                    break  # このURLは存在しない → 次のURLへ
                if r.status_code in (403, 429):
                    global _yfjp_session, _yfjp_session_ts
                    _yfjp_session = None
                    _yfjp_session_ts = 0.0
                    raise IOError(f'HTTP {r.status_code}')
                r.raise_for_status()
                text = r.text
                # Yahoo Japan の IP ブロックページを検出 → 次のURLへ
                if any(m in text for m in _YFJP_BLOCK_MARKERS):
                    logging.debug(f'FUND: YFJP IP block detected for {url}')
                    break
                html = text
                logging.debug(f'FUND: fetched {url} ({len(html)} bytes)')
                break
            except ValueError:
                raise
            except Exception as e:
                logging.debug(f'FUND fetch attempt {attempt+1} failed {fund_code} ({url}): {e}')
                if attempt < 1:
                    time.sleep(1.5)
        if html is not None:
            break

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
PRICE_CACHE_TTL = 300     # 5分（フォールバック）
HIST_CACHE_TTL  = 600     # 10分

# ── 市場時間に応じた可変TTL ──
# 取引時間中: 60秒（鮮度優先）
# 取引時間外: 15分（レート制限保護）
# 投信:      1時間（NAVは1日1回しか更新されない）
# 暗号資産:   60秒（24/7）
TTL_TRADING       = 60
TTL_OFF_HOURS     = 900
TTL_FUND          = 3600
TTL_CRYPTO        = 60

def _is_jp_market_open(now=None):
    if ZoneInfo is None:
        return True  # フォールバック
    n = now or datetime.now(ZoneInfo('Asia/Tokyo'))
    if n.weekday() >= 5:  # 土日
        return False
    minutes = n.hour * 60 + n.minute
    # 9:00-11:30 (前場) または 12:30-15:30 (後場)
    return (540 <= minutes < 690) or (750 <= minutes < 930)

def _is_us_market_open(now=None):
    if ZoneInfo is None:
        return True
    n = now or datetime.now(ZoneInfo('America/New_York'))
    if n.weekday() >= 5:
        return False
    minutes = n.hour * 60 + n.minute
    # 9:30-16:00 ET
    return 570 <= minutes < 960

def _market_of(ticker: str) -> str:
    if is_fund_ticker(ticker):
        return 'fund'
    if ticker.endswith('-USD'):
        return 'crypto'
    if ticker == 'USDJPY=X' or ticker.endswith('=X'):
        return 'fx'
    if ticker.endswith('.T'):
        return 'jp'
    return 'us'

def _get_price_ttl(ticker: str) -> int:
    m = _market_of(ticker)
    if m == 'fund':
        return TTL_FUND
    if m == 'crypto':
        return TTL_CRYPTO
    if m == 'fx':
        return TTL_TRADING  # 為替は実質常時動く
    if m == 'jp':
        return TTL_TRADING if _is_jp_market_open() else TTL_OFF_HOURS
    if m == 'us':
        return TTL_TRADING if _is_us_market_open() else TTL_OFF_HOURS
    return PRICE_CACHE_TTL

def _cached_price(ticker: str):
    entry = _price_cache.get(ticker)
    if not entry:
        return None
    age = time.time() - entry['ts']
    if age >= _get_price_ttl(ticker):
        return None
    # cache_age_sec を付与してクライアントに返す（フレッシュ取得との区別用）
    return {**entry['data'], 'cache_age_sec': round(age, 1)}

def _store_price(ticker: str, data: dict):
    # cache_age_sec はキャッシュ自体には保存しない（_cached_price で都度計算）
    clean = {k: v for k, v in data.items() if k != 'cache_age_sec'}
    _price_cache[ticker] = {'data': clean, 'ts': time.time()}

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
            logging.warning(f'DATA GET ERR ({type(e).__name__})')
            return jsonify({'error': 'gist fetch failed'}), 502

    else:  # POST
        try:
            body = request.get_json(force=True)
            r = req.patch(gist_url, headers=headers, timeout=10,
                          json={'files': {'portfolio.json': {'content': json.dumps(body, ensure_ascii=False)}}})
            r.raise_for_status()
            return jsonify({'ok': True})
        except Exception as e:
            logging.warning(f'DATA POST ERR ({type(e).__name__})')
            return jsonify({'error': 'gist save failed'}), 502


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

    # ── 1. Gistからポートフォリオデータを取得（etag も取得しておく）──
    try:
        r = req.get(gist_url, headers=gist_headers_map, timeout=10)
        r.raise_for_status()
        gist_etag = r.headers.get('ETag')
        content = r.json()['files']['portfolio.json']['content']
        payload = json.loads(content)
    except Exception as e:
        logging.warning(f'SNAPSHOT gist GET failed ({type(e).__name__})')
        return jsonify({'error': 'Gist読込失敗'}), 502

    stocks_data = payload if isinstance(payload, list) else payload.get('stocks', [])
    log         = [] if isinstance(payload, list) else payload.get('log', [])

    if not stocks_data:
        return jsonify({'error': 'ポートフォリオが空です'}), 400

    # ── 2. 全銘柄の価格を取得（バッチ優先 + US/JP 別プール並列） ──
    tickers = list({s['yahooTicker'] for s in stocks_data if s.get('yahooTicker')})
    all_tickers = tickers + ['USDJPY=X']
    prices: dict = {}

    # Step 2a: v7 quote バッチ一括取得（投信以外）
    batch_candidates = [t for t in all_tickers if not is_fund_ticker(t)]
    try:
        prices.update(fetch_prices_batch(batch_candidates))
    except Exception as e:
        logging.warning(f'SNAPSHOT batch fetch failed ({type(e).__name__}): {e}')

    # Step 2b: バッチで取れなかった銘柄を US/JP 別プールで個別取得
    remaining = [t for t in all_tickers if t not in prices]
    us_remain = [t for t in remaining if _classify_ticker(t) == 'us']
    jp_remain = [t for t in remaining if _classify_ticker(t) == 'jp']
    if us_remain or jp_remain:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as outer:
            fut_us = outer.submit(_run_pool, us_remain, 3, 75, 30)
            fut_jp = outer.submit(_run_pool, jp_remain, 3, 75, 30)
            ok_us, _err_us = fut_us.result()
            ok_jp, _err_jp = fut_jp.result()
        prices.update(ok_us)
        prices.update(ok_jp)

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

    # ── 4. 今日のエントリを構築（マージは次ステップで実施）──
    today = _date.today().isoformat()
    entry = {'date': today, 'value': round(total)}

    # ── 5. Gistに保存（TOCTOU 対策: 書き込み直前に再 GET して最新の stocks/log にマージ）
    # 価格取得中に別プロセス（フロントの同期）が Gist を更新する可能性があるため、
    # 書き込み直前に最新データを取り直し、自分の変更（log への entry 追加）だけを
    # 重ねてから PATCH することで、他プロセスの書き込みを上書きしないようにする。
    try:
        r_now = req.get(gist_url, headers=gist_headers_map, timeout=10)
        r_now.raise_for_status()
        latest_raw = r_now.json()['files']['portfolio.json']['content']
        latest_payload = json.loads(latest_raw)
        latest_stocks = latest_payload if isinstance(latest_payload, list) else latest_payload.get('stocks', stocks_data)
        latest_log    = [] if isinstance(latest_payload, list) else latest_payload.get('log', [])
    except Exception as e:
        logging.warning(f'SNAPSHOT gist re-GET failed ({type(e).__name__}): {e} — falling back to earlier snapshot')
        latest_stocks = stocks_data
        latest_log    = log

    # ログに今日の値をマージ（同日エントリは上書き）
    idx = next((i for i, e in enumerate(latest_log) if e.get('date') == today), -1)
    if idx >= 0:
        latest_log[idx] = entry
    else:
        latest_log.append(entry)
    latest_log.sort(key=lambda e: e.get('date', ''))
    if len(latest_log) > 730:
        latest_log = latest_log[-730:]

    new_payload = {'stocks': latest_stocks, 'log': latest_log}
    # If-Match で条件付き更新（Gist API が無視する場合もあるがログに残す）
    patch_headers = dict(gist_headers_map)
    if gist_etag:
        patch_headers['If-Match'] = gist_etag
    try:
        r = req.patch(gist_url, headers=patch_headers, timeout=10,
                      json={'files': {'portfolio.json': {'content': json.dumps(new_payload, ensure_ascii=False)}}})
        if r.status_code == 412:
            logging.warning('SNAPSHOT gist 412 (etag mismatch) — retrying without If-Match after re-merge')
            # 既に最新データにマージ済みなので、If-Match 抜きで保存
            patch_headers.pop('If-Match', None)
            r = req.patch(gist_url, headers=patch_headers, timeout=10,
                          json={'files': {'portfolio.json': {'content': json.dumps(new_payload, ensure_ascii=False)}}})
        r.raise_for_status()
    except Exception as e:
        logging.warning(f'SNAPSHOT gist PATCH failed ({type(e).__name__})')
        return jsonify({'error': 'Gist保存失敗'}), 502
    # 以後のログ出力用にローカル変数を揃える
    log = latest_log

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
        return jsonify({'error': 'upstream fetch failed'}), 502


def _classify_ticker(ticker: str) -> str:
    """US (query*.finance.yahoo.com) か JP (finance.yahoo.co.jp) かを分類。
    投信と日本株は JP プール、それ以外は US プール。"""
    if is_fund_ticker(ticker):
        return 'jp'
    if ticker.endswith('.T'):
        return 'us'  # 日本株の株価は Chart API (query2) 経由なので US プール
    return 'us'


def _fetch_one_price(ticker: str) -> dict:
    """キャッシュ確認 → 実取得。例外はそのまま伝播（呼び出し側でハンドル）。"""
    cached = _cached_price(ticker)
    if cached:
        logging.info(f'CACHE {ticker}')
        return cached
    if is_fund_ticker(ticker):
        fund_code = ticker[:-2]
        data = get_fund_price_yfjp(fund_code)
    else:
        data = fetch_price_with_retry(ticker)
        logging.info(f'OK   {ticker}: {data["price"]} ({data["currency"]})')
    _store_price(ticker, data)
    return data


def _run_pool(tickers: list, max_workers: int, pool_timeout: int, per_task_timeout: int):
    """指定ティッカーを ThreadPool で並列取得。(成功dict, 失敗dict) を返す。"""
    ok: dict = {}
    err: dict = {}
    if not tickers:
        return ok, err
    workers = max(1, min(len(tickers), max_workers))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_fetch_one_price, t): t for t in tickers}
        try:
            for fut in concurrent.futures.as_completed(futures, timeout=pool_timeout):
                t = futures[fut]
                try:
                    ok[t] = fut.result(timeout=per_task_timeout)
                except Exception as e:
                    logging.warning(f'ERR  {t}: {e}')
                    err[t] = str(e)
        except concurrent.futures.TimeoutError:
            logging.warning('PRICE fetch timeout: return partial result')
        for fut, t in futures.items():
            if fut.done():
                continue
            fut.cancel()
            if t not in ok and t not in err:
                err[t] = 'timeout'
    return ok, err


@app.route('/api/prices')
def get_prices():
    """
    GET /api/prices?tickers=7203.T,AAPL,0131103C.T
    ティッカー形式:
      日本株  → {code}.T   (例: 7203.T)
      米国株  → {code}     (例: AAPL)
      投資信託 → {code}.T  (例: 0131103C.T)
    US/JP は別ドメインなのでプールを分け独立並列実行。
    失敗した銘柄は 2 秒待機後に 1 回だけ自動リトライする。
    """
    tickers_param = request.args.get('tickers', '').strip()
    if not tickers_param:
        return jsonify({'error': 'tickersパラメータが必要です'}), 400

    tickers = [t.strip() for t in tickers_param.split(',') if t.strip()]

    # ── Step 0: キャッシュ優先 ──
    result: dict = {}
    uncached = []
    for t in tickers:
        cached = _cached_price(t)
        if cached:
            logging.info(f'CACHE {t}')
            result[t] = cached
        else:
            uncached.append(t)

    # ── Step 1: v7 quote API で米国株・日本株を一括取得 ──
    # 投信（8桁.T）はバッチ API で取得できないため除外。
    batch_candidates = [t for t in uncached if not is_fund_ticker(t)]
    if batch_candidates:
        try:
            batch_result = fetch_prices_batch(batch_candidates)
        except Exception as e:
            logging.warning(f'BATCH quote failed: {e}')
            batch_result = {}
        for t, data in batch_result.items():
            result[t] = data
            _store_price(t, data)

    # ── Step 2: バッチで取れなかった分を個別取得（US/JP 別プール並列） ──
    remaining = [t for t in uncached if t not in result]
    us_tickers = [t for t in remaining if _classify_ticker(t) == 'us']
    jp_tickers = [t for t in remaining if _classify_ticker(t) == 'jp']

    if us_tickers or jp_tickers:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as outer:
            fut_us = outer.submit(_run_pool, us_tickers, 3, 60, 30)
            fut_jp = outer.submit(_run_pool, jp_tickers, 3, 60, 30)
            ok_us, err_us = fut_us.result()
            ok_jp, err_jp = fut_jp.result()
        result.update(ok_us)
        result.update(ok_jp)
        failed = {**err_us, **err_jp}
    else:
        failed = {}

    # ── 部分失敗の自動リトライ（1回）──
    # タイムアウトや 429 の一時的失敗は多くの場合 2秒待って再試行で救済できる
    if failed:
        retry_us = [t for t in failed if _classify_ticker(t) == 'us']
        retry_jp = [t for t in failed if _classify_ticker(t) == 'jp']
        logging.info(f'RETRY {len(failed)} tickers (US={len(retry_us)}, JP={len(retry_jp)})')
        time.sleep(2)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as outer:
            fut_us = outer.submit(_run_pool, retry_us, 2, 45, 25)
            fut_jp = outer.submit(_run_pool, retry_jp, 2, 45, 25)
            ok_us2, err_us2 = fut_us.result()
            ok_jp2, err_jp2 = fut_jp.result()
        result.update(ok_us2)
        result.update(ok_jp2)
        failed = {**err_us2, **err_jp2}

    # 失敗したティッカーはエラー構造体で埋める
    for t, msg in failed.items():
        if t not in result:
            result[t] = {'price': None, 'prev_close': None, 'day_change': None, 'currency': None, 'error': msg}

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
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(need_fetch), 3)) as ex:
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
            r = _yfjp_get(url, timeout=10)
            r.raise_for_status()
            # <title> からファンド名を抽出
            m = re.search(r'<title>\s*(.+?)(?:【|\[|\|)', r.text)
            name = m.group(1).strip() if m else ''
            logging.info(f'NAME FUND {fund_code}: {name!r}')
            return jsonify({'name': name})
        else:
            import urllib.parse as _up
            url = f'https://query2.finance.yahoo.com/v8/finance/chart/{_up.quote(ticker)}?interval=1d&range=5d'
            r = _yfus_get(url, timeout=10)
            r.raise_for_status()
            meta = r.json().get('chart', {}).get('result', [{}])[0].get('meta', {})
            name = meta.get('shortName') or meta.get('longName') or ''
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
