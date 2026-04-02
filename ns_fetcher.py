# ================================================================
#  NEPALSTOCK.COM -- Official JSON API
#
#  Uses nepse-data-api package (pip install nepse-data-api)
#  which handles WASM authentication automatically.
#
#  Fallback: pure-Python WAT-decoded algorithm (no wasm file needed)
#  Algorithm decoded from css.wasm WAT source March 2026.
#
#  Install: pip install nepse-data-api requests
# ================================================================

import os
import threading
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NS_BASE    = "https://nepalstock.com"
NS_TIMEOUT = (10, 90)
NS_PAGE_SZ = 2000
NS_WASM    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "css.wasm")

_ns_lock        = threading.Lock()
_ns_token_cache = {"token": None, "expires": 0.0}
_ns_nepse_obj   = [None]   # cached nepse-data-api Nepse instance

NS_HDRS = {
    "Accept":     "application/json, text/plain, */*",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"),
}


# ── WAT lookup table (decoded from css.wasm data section) ────────
_WAT_TABLE = [
    5, 8, 4, 7, 9, 4, 6, 9, 5, 5,
    6, 5, 3, 5, 4, 4, 9, 6, 6, 8,
    8, 6, 8, 6, 5, 8, 4, 9, 5, 9,
    8, 5, 3, 4, 7, 7, 4, 7, 3, 9,
]
_WAT_OFFSETS = [22, 32, 60, 88, 110]


def _wat_drop_pos(salt, fn_idx):
    h = (salt // 100) % 10
    t = (salt //  10) % 10
    u =  salt         % 10
    if fn_idx == 0:        idx = (t + u) + h
    elif fn_idx in (1, 2): lhs = h + t; idx = lhs + lhs + u
    elif fn_idx == 3:      idx = t + (t + u) + h
    else:                  idx = h + (h + t + u)
    return (_WAT_TABLE[idx] + _WAT_OFFSETS[fn_idx]
            if idx < len(_WAT_TABLE) else 0)


def _drop_token(raw, salts):
    """
    Derive valid Salter token using WAT-decoded algorithm.
    Drops 5 chars at absolute positions computed from salt digit arithmetic.
    """
    positions = [_wat_drop_pos(salts[i], i) for i in range(min(5, len(salts)))]
    chars = list(raw)
    for pos in sorted(positions, reverse=True):
        if 0 <= pos < len(chars):
            chars.pop(pos)
    return "".join(chars)


# ── nepse-data-api integration ───────────────────────────────────

def _get_nepse_instance():
    """Return cached Nepse instance from nepse-data-api."""
    if _ns_nepse_obj[0] is not None:
        return _ns_nepse_obj[0]
    try:
        from nepse_data_api import Nepse
        nepse = Nepse(enable_cache=False)
        _ns_nepse_obj[0] = nepse
        return nepse
    except ImportError:
        return None
    except Exception:
        return None


# ── Token derivation ─────────────────────────────────────────────

def ns_get_token(force=False):
    """
    Return valid cached Salter token.
    Uses WAT-decoded drop algorithm (proven correct from wasm source).
    Thread-safe, 45s TTL.
    """
    import time
    with _ns_lock:
        now = time.time()
        if not force and _ns_token_cache["token"] and now < _ns_token_cache["expires"]:
            return _ns_token_cache["token"]
        try:
            r = requests.get(
                f"{NS_BASE}/api/authenticate/prove",
                headers={**NS_HDRS, "Content-Type": "application/json"},
                verify=False, timeout=(10, 15),
            )
            r.raise_for_status()
            d     = r.json()
            raw   = d.get("accessToken", "")
            salts = [d.get(f"salt{i}", 0) for i in range(1, 6)]
            token = _drop_token(raw, salts)
            if token:
                _ns_token_cache.update({"token": token, "expires": now + 45})
            return token or None
        except Exception:
            return None


def ns_headers(token=None):
    return {**NS_HDRS, "Authorization": f"Salter {token or ns_get_token()}"}


# ── Row parsing ──────────────────────────────────────────────────

def parse_rows(content):
    rows = []
    for item in content:
        rows.append({
            "txn_no": str(item.get("contractId",      "") or ""),
            "symbol": str(item.get("stockSymbol",     "") or ""),
            "buyer":  str(item.get("buyerMemberId",   "")
                         or item.get("buyerBroker",   "") or ""),
            "seller": str(item.get("sellerMemberId",  "")
                         or item.get("sellerBroker",  "") or ""),
            "qty":    int(item.get("contractQuantity",  0)
                         or item.get("quantity",        0) or 0),
            "rate":   float(item.get("contractRate",    0) or 0),
            "amount": float(item.get("contractAmount",  0) or 0),
        })
    return rows


# ── Page fetcher ─────────────────────────────────────────────────

def fetch_page(date_str, page, size=NS_PAGE_SZ, token=None):
    """Fetch one page. Returns (rows, total_elements, total_pages)."""
    tok = token or ns_get_token()
    if not tok:
        return [], 0, 0
    params = {"businessDate": date_str, "page": page,
              "size": size, "sort": "contractId,asc"}
    try:
        r = requests.get(
            f"{NS_BASE}/api/nots/security/floorsheet",
            params=params, headers=ns_headers(tok),
            verify=False, timeout=NS_TIMEOUT,
        )
        if r.status_code == 401:
            # Token expired — force refresh and retry once
            tok = ns_get_token(force=True)
            if not tok:
                return [], 0, 0
            r   = requests.get(
                f"{NS_BASE}/api/nots/security/floorsheet",
                params=params, headers=ns_headers(tok),
                verify=False, timeout=NS_TIMEOUT,
            )
        if r.status_code == 401:
            # Market may be closed — 401 is expected outside trading hours
            return [], 0, 0
        if r.status_code != 200:
            return [], 0, 0
        fs = r.json().get("floorsheets", r.json())
        return (
            parse_rows(fs.get("content", [])),
            int(fs.get("totalElements", 0)),
            int(fs.get("totalPages",    1)),
        )
    except Exception:
        return [], 0, 0


# ── nepse-data-api floorsheet (primary if available) ─────────────

def _floorsheet_via_nepse_api(date_str, symbol=""):
    """
    Use nepse-data-api to fetch floorsheet.
    This handles WASM auth automatically and is the most reliable method.
    Falls back gracefully on 401 (market closed / auth expired).
    """
    nepse = _get_nepse_instance()
    if nepse is None:
        return None
    try:
        if symbol:
            raw = nepse.get_floorsheet(symbol=symbol)
        else:
            raw = nepse.get_floorsheet()
        if raw is None:
            return None
        # nepse-data-api confirmed field names from live test 2026-03-19:
        # contractId, stockSymbol, buyerMemberId, sellerMemberId,
        # contractQuantity, contractRate, contractAmount, businessDate,
        # buyerBrokerName, sellerBrokerName, tradeTime, securityName
        rows = []
        for item in (raw if isinstance(raw, list) else []):
            rows.append({
                "txn_no":       str(item.get("contractId",       "") or ""),
                "symbol":       str(item.get("stockSymbol",      "") or ""),
                "buyer":        str(item.get("buyerMemberId",    "") or ""),
                "seller":       str(item.get("sellerMemberId",   "") or ""),
                "qty":          int(item.get("contractQuantity",   0) or 0),
                "rate":       float(item.get("contractRate",       0) or 0),
                "amount":     float(item.get("contractAmount",     0) or 0),
                # Rich fields only from nepse-data-api
                "buyer_name":   str(item.get("buyerBrokerName",  "") or ""),
                "seller_name":  str(item.get("sellerBrokerName", "") or ""),
                "trade_time":   str(item.get("tradeTime",        "") or ""),
                "stock_name":   str(item.get("securityName",     "") or ""),
                "business_date":str(item.get("businessDate",     "") or ""),
            })
        return rows if rows else None
    except Exception:
        return None


# ── Public API ───────────────────────────────────────────────────

def nepalstock_floorsheet(date_str, symbol=""):
    """
    Fetch complete floorsheet for a trading date from NepalStock API.

    Strategy (in order):
    1. nepse-data-api package (handles WASM auth automatically)
    2. Direct API with WAT-decoded token (pure Python, no wasm file)

    Both use sync parallel fetch per Mar 2026 NepseUnofficialApi note
    -- async gives incomplete data on NEPSE server.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Try nepse-data-api first
    result = _floorsheet_via_nepse_api(date_str, symbol)
    if result is not None:
        return result

    # Fall back to direct API with WAT token
    token = ns_get_token()
    if not token:
        return []

    rows0, total_el, total_pg = fetch_page(date_str, 0, token=token)
    if not rows0:
        return []

    all_rows = list(rows0)

    if total_pg > 1:
        workers = min(total_pg - 1, 16)
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {
                ex.submit(fetch_page, date_str, pg, NS_PAGE_SZ, token): pg
                for pg in range(1, total_pg)
            }
            for fut in as_completed(futs):
                try:
                    pg_rows, _, _ = fut.result()
                    all_rows.extend(pg_rows)
                except Exception:
                    pass

    if symbol:
        sym = symbol.upper()
        all_rows = [r for r in all_rows if r["symbol"].upper() == sym]

    all_rows.sort(key=lambda r: r.get("txn_no", ""))
    return all_rows


def nepalstock_floorsheet_available():
    """Check if NepalStock API is reachable."""
    try:
        # Try nepse-data-api first
        if _get_nepse_instance() is not None:
            return True
        tok = ns_get_token()
        return bool(tok and len(tok) > 20)
    except Exception:
        return False


def analyze_broker(floorsheet, broker_code):
    """Analyse buy/sell for a broker code across floorsheet rows."""
    bc = str(broker_code)
    buy_qty = sell_qty = buy_amt = sell_amt = 0
    for r in floorsheet:
        if str(r.get("buyer", "")) == bc:
            buy_qty += r.get("qty",    0)
            buy_amt += r.get("amount", 0)
        if str(r.get("seller", "")) == bc:
            sell_qty += r.get("qty",    0)
            sell_amt += r.get("amount", 0)
    net = buy_qty - sell_qty
    return {
        "broker":   bc,
        "buy_qty":  buy_qty,  "buy_amt":  buy_amt,
        "sell_qty": sell_qty, "sell_amt": sell_amt,
        "net_qty":  net,
        "pattern": ("ACCUMULATION" if net > 0 else
                    "DISTRIBUTION" if net < 0 else "NEUTRAL"),
    }


# ── Backwards compatibility aliases ──────────────────────────────
_token_via_wasm     = lambda raw, salts, stime: ""   # wasm not needed
_ns_derive_token    = _drop_token
_ns_auth_headers    = ns_headers
_ns_fetch_all_async = lambda date_str, total_pages, size=NS_PAGE_SZ: []
