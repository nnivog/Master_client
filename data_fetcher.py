"""
NEPSE Data Fetcher
==================
Primary history source: MeroLagani CompanyDetail UpdatePanel postback.
  - Columns: Date, LTP(close), High, Low, Open, Qty, Turnover
  - Pagination via PagerControlTransactionHistory btnPaging
  - No CSRF token needed — plain ASP.NET UpdatePanel

Fallback: NepseAlpha API, then NEPSE official API.
Sharesansar is used ONLY for today prices and financials (no CSRF-history calls).

Never import lxml. Always BeautifulSoup(html, "html.parser").
"""

import re, time, requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

# ── In-memory cache for market today prices (5-min TTL) ─────────────────────
_MARKET_CACHE: dict = {}
_MARKET_CACHE_TTL = 300  # 5 minutes

# ── In-memory cache for price history (1-hour TTL, per symbol+date range) ───
_HIST_CACHE: dict = {}
_HIST_CACHE_TTL = 3600  # 1 hour

def get_cached_market(source: str):
    key = source or "auto"
    e = _MARKET_CACHE.get(key)
    if e and (time.time() - e["ts"]) < _MARKET_CACHE_TTL:
        return e["data"], e["src"], e["fetched_at"]
    return None, None, None

def set_cached_market(source: str, data: list, src_name: str):
    key = source or "auto"
    _MARKET_CACHE[key] = {"data": data, "src": src_name,
                          "ts": time.time(),
                          "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

def invalidate_market_cache():
    _MARKET_CACHE.clear()

def get_cached_history(symbol: str, from_date: str, to_date: str):
    key = f"{symbol}|{from_date}|{to_date}"
    e = _HIST_CACHE.get(key)
    if e and (time.time() - e["ts"]) < _HIST_CACHE_TTL:
        return e
    return None

def set_cached_history(symbol: str, from_date: str, to_date: str, result: dict):
    _HIST_CACHE[f"{symbol}|{from_date}|{to_date}"] = {**result, "ts": time.time()}

def invalidate_history_cache(symbol: str = None):
    if symbol:
        for k in [k for k in _HIST_CACHE if k.startswith(symbol+"|")]: del _HIST_CACHE[k]
    else:
        _HIST_CACHE.clear()


SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent":      _UA,
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
})
TIMEOUT    = 25           # general requests (connect + read)
TIMEOUT_FS = (15, 90)    # floorsheet: 15s connect, 90s read
                          # MeroLagani builds the full 500-row HTML table
                          # server-side before sending — can take 30-60s

def _sf(v):
    try:    return float(str(v).replace(",","").replace("%","").strip())
    except: return 0.0

def _si(v):
    try:    return int(float(str(v).replace(",","").strip()))
    except: return 0

def _parse_date(raw):
    """Any date format → YYYY-MM-DD."""
    if not raw: return ""
    s = str(raw).strip()
    if re.match(r'^\d{4}-\d{2}-\d{2}$', s):       return s
    if re.match(r'^\d{4}-\d{2}-\d{2}[ T]', s):     return s[:10]
    # MeroLagani uses 2026/03/12
    if re.match(r'^\d{4}/\d{2}/\d{2}$', s):        return s.replace("/","-")
    for fmt in ("%d-%m-%Y","%m/%d/%Y","%Y/%m/%d","%d/%m/%Y",
                "%b %d, %Y","%d %b %Y","%Y%m%d"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError: pass
    try:
        ts = int(float(s))
        if ts > 1e9: return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
    except (ValueError, OSError): pass
    m = re.search(r'(\d{4})[^\d](\d{1,2})[^\d](\d{1,2})', s)
    if m:
        try: return datetime(int(m.group(1)),int(m.group(2)),int(m.group(3))).strftime("%Y-%m-%d")
        except ValueError: pass
    return ""


# ══════════════════════════════════════════════════════════════════
#  MEROLAGANI HISTORY  (primary source)
# ══════════════════════════════════════════════════════════════════

def _ml_hidden(soup):
    """Extract all hidden form fields from a BeautifulSoup page."""
    return {inp["name"]: inp.get("value","")
            for inp in soup.find_all("input", {"type":"hidden"}) if inp.get("name")}

def _ml_delta_chunks(text):
    """
    Parse ASP.NET ScriptManager delta (UpdatePanel) response.
    Format: {length}|{type}|{id}|{content}|...
    Returns dict: chunk_id → content_string
    """
    chunks = {}
    i = 0
    while i < len(text):
        try:
            p1 = text.index('|', i)
            length = int(text[i:p1])
            p2 = text.index('|', p1+1)
            p3 = text.index('|', p2+1)
            cid     = text[p2+1:p3]
            content = text[p3+1:p3+1+length]
            chunks[cid] = content
            i = p3 + 1 + length + 1
        except (ValueError, IndexError):
            break
    return chunks

def _ml_parse_history_table(html):
    """
    Parse MeroLagani history table HTML.
    Columns: # | Date | LTP | %Change | High | Low | Open | Qty | Turnover
    Returns list of OHLCV dicts.
    """
    soup  = BeautifulSoup(html, "html.parser")
    # Find table inside divHistory or the first table with Date column
    tbl = None
    div = soup.find(id="divHistory")
    if div: tbl = div.find("table")
    if not tbl:
        for t in soup.find_all("table"):
            hdrs = [th.get_text(strip=True) for th in t.find_all(["th","td"])[:10]]
            if "Date" in hdrs and ("LTP" in hdrs or "Close" in hdrs):
                tbl = t; break
    if not tbl:
        return []

    rows   = tbl.find_all("tr")
    result = []
    for row in rows[1:]:   # skip header
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) < 8: continue
        # cols: [#, Date, LTP, %Chg, High, Low, Open, Qty, Turnover]
        date_val = _parse_date(cols[1])
        if not date_val: continue
        ltp  = _sf(cols[2])   # LTP = closing price
        high = _sf(cols[4])
        low  = _sf(cols[5])
        op   = _sf(cols[6])
        qty  = _si(cols[7])
        if ltp == 0: continue
        result.append({
            "date":    date_val,
            "open":    op   if op   > 0 else ltp,
            "high":    high if high > 0 else ltp,
            "low":     low  if low  > 0 else ltp,
            "close":   ltp,
            "volume":  qty,
        })
    return result

def _ml_get_total_pages(html):
    """
    Extract total record count from pager span and compute page count.
    Span text looks like: 'Showing 1-30 of 1238 records'
    """
    soup = BeautifulSoup(html, "html.parser")
    for span in soup.find_all("span"):
        t = span.get_text(strip=True)
        m = re.search(r'of\s+([\d,]+)', t)
        if m:
            total = _si(m.group(1))
            if total > 0:
                return total, (total + 29) // 30   # 30 rows per page
    return 0, 1

def merolagani_history(symbol, from_date=None, to_date=None):
    """
    Scrape MeroLagani CompanyDetail price history via UpdatePanel postback.
    No authentication or CSRF tokens needed.
    Paginates through all pages until from_date is reached.
    """
    end_dt   = datetime.strptime(to_date,   "%Y-%m-%d") if to_date   else datetime.now()
    start_dt = datetime.strptime(from_date, "%Y-%m-%d") if from_date else end_dt - timedelta(days=365)
    s_str    = start_dt.strftime("%Y-%m-%d")
    e_str    = end_dt.strftime("%Y-%m-%d")

    base_url = f"https://merolagani.com/CompanyDetail.aspx?symbol={symbol.upper()}"
    hdrs_req = {
        "X-Requested-With": "XMLHttpRequest",
        "X-MicrosoftAjax":  "Delta=true",
        "Referer":          base_url,
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept":           "*/*",
    }

    # Step 1: GET page to get hidden fields
    sess = requests.Session()
    sess.headers.update({"User-Agent": _UA, "Accept": "text/html,*/*;q=0.9",
                         "Accept-Language": "en-US,en;q=0.9"})
    r0   = sess.get(base_url, timeout=TIMEOUT)
    if r0.status_code != 200:
        raise ValueError(f"MeroLagani page HTTP {r0.status_code} for '{symbol}'.")

    hidden = _ml_hidden(BeautifulSoup(r0.text, "html.parser"))

    def build_post(page_num, current_hidden):
        """
        page_num: 0-based page index for the pager.
        Uses lbtnSearchPriceHistory on page 0, then pager button for subsequent pages.
        """
        base = {
            "__EVENTTARGET":        "",
            "__EVENTARGUMENT":      "",
            "__VIEWSTATE":          current_hidden.get("__VIEWSTATE",""),
            "__VIEWSTATEGENERATOR": current_hidden.get("__VIEWSTATEGENERATOR",""),
            "__EVENTVALIDATION":    current_hidden.get("__EVENTVALIDATION",""),
            "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnStockSymbol":  symbol.upper(),
            "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnActiveTabID":  "navHistory",
            "ctl00$ContentPlaceHolder1$CompanyDetail1$txtMarketDatePriceFilter": "",
            "__ASYNCPOST": "true",
        }
        if page_num == 0:
            base["ctl00$ScriptManager1"] = ("ctl00$ContentPlaceHolder1$CompanyDetail1$tabPanel"
                                             "|ctl00$ContentPlaceHolder1$CompanyDetail1$btnHistoryTab")
            base["ctl00$ContentPlaceHolder1$CompanyDetail1$btnHistoryTab"] = ""
        else:
            pager_btn = "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory1$btnPaging"
            pager_pg  = "ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlTransactionHistory1$hdnCurrentPage"
            base["ctl00$ScriptManager1"] = (f"ctl00$ContentPlaceHolder1$CompanyDetail1$tabPanel|{pager_btn}")
            base[pager_btn] = ""
            base[pager_pg]  = str(page_num)
        return base

    all_rows = []
    current_hidden = hidden

    for page in range(200):   # up to 200 pages = 6000 rows (~24 years of data)
        post_data = build_post(page, current_hidden)
        resp = sess.post(base_url, data=post_data, headers=hdrs_req, timeout=TIMEOUT)

        if resp.status_code != 200:
            if page == 0:
                raise ValueError(f"MeroLagani UpdatePanel HTTP {resp.status_code} for '{symbol}'.")
            break

        chunks = _ml_delta_chunks(resp.text)

        # Update hidden fields from response for next page
        tab_html = chunks.get("ctl00_ContentPlaceHolder1_CompanyDetail1_tabPanel", "")
        if not tab_html:
            break

        # Update VIEWSTATE etc from delta response
        for field in ("__VIEWSTATE","__VIEWSTATEGENERATOR","__EVENTVALIDATION"):
            if field in chunks:
                current_hidden[field] = chunks[field]

        rows = _ml_parse_history_table(tab_html)
        if not rows:
            break

        all_rows.extend(rows)

        # Check if we've gone past the start date
        dates = [r["date"] for r in rows if r["date"]]
        if dates and min(dates) <= s_str:
            break

        # On first page, get total pages
        if page == 0:
            total, n_pages = _ml_get_total_pages(tab_html)
            if n_pages <= 1:
                break

        # Check if last page (fewer than 30 rows means last page)
        if len(rows) < 30:
            break

    # Filter to requested date range, sort oldest→newest
    result = [r for r in all_rows if s_str <= r["date"] <= e_str]
    result.sort(key=lambda x: x["date"])
    # Deduplicate by date (keep first occurrence)
    seen = set()
    deduped = []
    for r in result:
        if r["date"] not in seen:
            seen.add(r["date"])
            deduped.append(r)
    return deduped



# ══════════════════════════════════════════════════════════════════
#  SHARESANSAR PRICE HISTORY  (fallback for analyze tab)
# ══════════════════════════════════════════════════════════════════

def sharesansar_history(symbol, from_date=None, to_date=None):
    """
    Scrape OHLCV price history from ShareSansar company page.
    Uses the company-price-history AJAX endpoint (DataTables).
    Returns list of {date, open, high, low, close, volume} sorted oldest→newest.
    """
    from datetime import datetime as _dt, timedelta as _td
    end_dt   = _dt.strptime(to_date,   "%Y-%m-%d") if to_date   else _dt.now()
    start_dt = _dt.strptime(from_date, "%Y-%m-%d") if from_date else end_dt - _td(days=365)
    s_str, e_str = start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")

    sym = symbol.strip().upper()
    base_url = f"https://www.sharesansar.com/company/{sym.lower()}"
    sess = requests.Session()
    sess.headers.update({"User-Agent": _UA, "Accept": "text/html,*/*",
                         "Accept-Language": "en-US,en;q=0.9"})
    try:
        # Step 1: GET company page for token + company id
        r0 = sess.get(base_url, timeout=TIMEOUT)
        if r0.status_code != 200:
            return []
        soup = BeautifulSoup(r0.text, "html.parser")
        inp   = soup.find("input", {"name": "_token"})
        token = inp["value"].strip() if (inp and inp.get("value")) else ""
        if not token:
            return []
        # Get company id
        cid = ""
        el = soup.find(id="companyid")
        if el: cid = re.sub(r"[^0-9]", "", el.get_text())
        if not cid:
            m = re.search(r"id=[\"']companyid[\"'][^>]*>\s*(\d+)", r0.text)
            if m: cid = m.group(1)
        if not cid:
            return []

        # Step 2: POST to company-price-history AJAX
        hdrs = {
            "X-Requested-With": "XMLHttpRequest",
            "Origin":   "https://www.sharesansar.com",
            "Referer":  base_url,
            "Accept":   "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
        # SS throttles at length>=100 (returns HTTP 202 empty).
        # Max safe page size = 50. Paginate until we have enough date coverage.
        all_raw = []
        start = 0
        PAGE = 50
        for _page in range(200):   # max 200 pages = 10000 rows (>13 years)
            rp = sess.post(
                "https://www.sharesansar.com/company-price-history",
                data={"draw": str(_page + 1), "start": str(start),
                      "length": str(PAGE), "company": cid, "_token": token},
                headers=hdrs, timeout=TIMEOUT
            )
            if rp.status_code != 200:
                break
            page_data = rp.json()
            page_rows = page_data.get("data", [])
            if not page_rows:
                break
            all_raw.extend(page_rows)
            # Stop if oldest date on this page is before our start date
            oldest = page_rows[-1].get("published_date", "")
            if oldest and oldest <= s_str:
                break
            # Stop if we got fewer rows than requested (last page)
            if len(page_rows) < PAGE:
                break
            start += PAGE
        raw = all_raw
        result = []
        for row in raw:
            if not isinstance(row, dict):
                continue
            # Actual SS fields: published_date, open, high, low, close,
            #                   per_change, traded_quantity, traded_amount
            dv = _parse_date(str(row.get("published_date", "") or row.get("date", "")))
            if not dv or not (s_str <= dv <= e_str):
                continue
            result.append({
                "date":   dv,
                "open":   _sf(row.get("open",  0)),
                "high":   _sf(row.get("high",  0)),
                "low":    _sf(row.get("low",   0)),
                "close":  _sf(row.get("close", 0)),
                "volume": _si(row.get("traded_quantity", 0) or row.get("traded_qty", 0)),
            })
        result.sort(key=lambda x: x["date"])
        # Deduplicate by date
        seen, deduped = set(), []
        for r2 in result:
            if r2["date"] not in seen:
                seen.add(r2["date"]); deduped.append(r2)
        return deduped
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════
#  NEPSE ALPHA FALLBACK
# ══════════════════════════════════════════════════════════════════

def nepse_alpha_history(symbol, from_date=None, to_date=None):
    try:
        end_dt   = datetime.strptime(to_date,   "%Y-%m-%d") if to_date   else datetime.now()
        start_dt = datetime.strptime(from_date, "%Y-%m-%d") if from_date else end_dt - timedelta(days=365)
        url = (f"https://api.nepsealpha.com/api/v1/stock?symbol={symbol}&resolution=D"
               f"&from={int(start_dt.timestamp())}&to={int(end_dt.timestamp())}")
        r = SESSION.get(url, timeout=TIMEOUT,
                        headers={"Accept":"application/json","Referer":"https://nepsealpha.com/"})
        if r.status_code != 200: return []
        d = r.json()
        if d.get("s") != "ok": return []
        ts_l=d.get("t",[]); opens=d.get("o",[]); highs=d.get("h",[])
        lows=d.get("l",[]); closes=d.get("c",[]); vols=d.get("v",[])
        result = []
        for i, ts in enumerate(ts_l):
            try:
                result.append({"date":datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d"),
                                "open":_sf(opens[i]  if i<len(opens)  else 0),
                                "high":_sf(highs[i]  if i<len(highs)  else 0),
                                "low": _sf(lows[i]   if i<len(lows)   else 0),
                                "close":_sf(closes[i] if i<len(closes) else 0),
                                "volume":_si(vols[i]  if i<len(vols)   else 0)})
            except (IndexError, OSError): continue
        result.sort(key=lambda x: x["date"])
        return result
    except Exception: return []


# ══════════════════════════════════════════════════════════════════
#  NEPSE OFFICIAL API FALLBACK
# ══════════════════════════════════════════════════════════════════

_nepse_sec_cache = {"data":[],"ts":0}

def _get_security_id(symbol):
    now = time.time()
    if not _nepse_sec_cache["data"] or now - _nepse_sec_cache["ts"] > 3600:
        try:
            r = SESSION.get("https://newweb.nepalstock.com.np/api/nots/security?nonDelisted=true",
                            timeout=TIMEOUT, headers={"Accept":"application/json"})
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict): data = data.get("object", data.get("data",[]))
                _nepse_sec_cache.update({"data": data, "ts": now})
        except Exception: pass
    for sec in _nepse_sec_cache["data"]:
        if isinstance(sec, dict):
            sf = sec.get("symbol","") or sec.get("securityShortName","")
            if sf.upper() == symbol.upper():
                return str(sec.get("id","") or sec.get("securityId",""))
    return ""

def nepse_official_history(symbol, from_date=None, to_date=None):
    try:
        sid = _get_security_id(symbol)
        if not sid: return []
        r = SESSION.get(
            f"https://newweb.nepalstock.com.np/api/nots/market/graphdata/{sid}",
            timeout=TIMEOUT,
            headers={"Accept":"application/json","Referer":"https://newweb.nepalstock.com.np/"})
        if r.status_code != 200: return []
        data = r.json()
        if isinstance(data, dict): data = data.get("object", data.get("data", data))
        end_dt   = datetime.strptime(to_date,   "%Y-%m-%d") if to_date   else datetime.now()
        start_dt = datetime.strptime(from_date, "%Y-%m-%d") if from_date else end_dt - timedelta(days=365)
        s_str, e_str = start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")
        result = []
        for row in (data if isinstance(data, list) else []):
            if not isinstance(row, dict): continue
            dv = _parse_date(row.get("businessDate","") or row.get("date","") or row.get("tradingDate",""))
            if not dv or not (s_str <= dv <= e_str): continue
            result.append({"date":dv,
                "open":  _sf(row.get("openPrice",0)  or row.get("open",0)),
                "high":  _sf(row.get("highPrice",0)  or row.get("high",0)),
                "low":   _sf(row.get("lowPrice",0)   or row.get("low",0)),
                "close": _sf(row.get("closePrice",0) or row.get("close",0) or row.get("lastTradedPrice",0)),
                "volume":_si(row.get("totalTradeQuantity",0) or row.get("volume",0))})
        result.sort(key=lambda x: x["date"])
        return result
    except Exception: return []


# ══════════════════════════════════════════════════════════════════
#  PINGS
# ══════════════════════════════════════════════════════════════════

def sharesansar_ping():
    try:    return SESSION.get("https://www.sharesansar.com/today-share-price",timeout=8).status_code==200
    except: return False

def merolagani_ping():
    try:    return SESSION.get("https://merolagani.com/LatestMarket.aspx",timeout=8).status_code==200
    except: return False

def check_all_sources():
    out = {}
    with ThreadPoolExecutor(max_workers=2) as ex:
        fs = {ex.submit(sharesansar_ping):"sharesansar", ex.submit(merolagani_ping):"merolagani"}
        for f in as_completed(fs): out[fs[f]] = f.result()
    return out


# ══════════════════════════════════════════════════════════════════
#  SYMBOL LIST
# ══════════════════════════════════════════════════════════════════

_sym_cache = {"s":[],"ts":0}

def get_all_symbols():
    now = time.time()
    if _sym_cache["s"] and now - _sym_cache["ts"] < 3600: return _sym_cache["s"]
    try:
        # SS is sometimes slow — retry once with longer timeout
        for _attempt in range(2):
            try:
                r = SESSION.get("https://www.sharesansar.com/today-share-price", timeout=35)
                break
            except Exception:
                if _attempt == 1: return []
                import time as _time; _time.sleep(2)
        soup = BeautifulSoup(r.text, "html.parser")
        tbl  = soup.find("table",{"id":"headFixed"}) or soup.find("table")
        syms = []
        if tbl:
            for row in tbl.find_all("tr")[1:]:
                cols = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cols) >= 8 and cols[1]:
                    syms.append({"symbol":cols[1].strip(),"sector":"",
                                 "company_name": cols[2].strip() if len(cols)>2 else "",
                                 "ltp":_sf(cols[7]) if len(cols)>7 else 0})
        if syms: _sym_cache.update({"s":syms,"ts":now})
        return syms
    except: return []

def search_symbols(q):
    return [s for s in get_all_symbols() if q.upper() in s["symbol"]][:20]


# ══════════════════════════════════════════════════════════════════
#  TODAY PRICES
# ══════════════════════════════════════════════════════════════════

def sharesansar_today():
    try:
        r    = SESSION.get("https://www.sharesansar.com/today-share-price", timeout=TIMEOUT)
        soup = BeautifulSoup(r.text, "html.parser")

        # Try to detect trade date from page text
        trade_date = ""
        for el in soup.find_all(["h1","h2","h3","h4","p","caption","span","div"]):
            t = el.get_text(strip=True)
            m = re.search(r'(\d{4}[-/]\d{2}[-/]\d{2}|\d{1,2}[-/ ]\w{3,9}[-/ ]\d{4})', t)
            if m:
                trade_date = _parse_date(m.group(1))
                if trade_date: break

        tbl = soup.find("table", {"id": "headFixed"}) or soup.find("table")
        if not tbl: return []

        # ── Verified Sharesansar column layout (stable since 2023) ───────────
        # 0=SN | 1=Symbol | 2=Company Name | 3=Open | 4=High | 5=Low | 6=Close |
        # 7=LTP | 8=Change(abs) | 9=Change% | 10=VWAP | 11=Qty(Volume) |
        # 12=PrevClose | 13=Turnover | 14=No.of Trans | 15=CircSupply |
        # 16=Diff | 17=% Change | 18=180Day | 19=OneYr |
        # 22=52WH | 23=52WL
        # NOTE: cols[9] and cols[17] both contain Change% — we use cols[17]
        # as it is the final computed value used on the Sharesansar site.
        COL = {
            "symbol":     1,
            "company":    2,
            "open":       3,
            "high":       4,
            "low":        5,
            "close":      6,
            "ltp":        7,
            "change_abs": 8,
            "change_pct": 17,   # confirmed working col for % change
            "vwap":       10,
            "volume":     11,   # "Qty" column = number of shares traded
            "prev_close": 12,
            "turnover":   13,
            "trades":     14,
            "high_52w":   22,
            "low_52w":    23,
        }

        # Try to verify/override from actual header row text
        # Only override if we find an unambiguous match for a specific key
        header_row = tbl.find("tr")
        if header_row:
            for i, th in enumerate(header_row.find_all(["th","td"])):
                h = th.get_text(strip=True).lower().strip()
                # Only override clear, unambiguous matches
                if h == "ltp":                                        COL["ltp"]        = i
                elif h in ("qty","quantity","shares traded","vol"):   COL["volume"]     = i
                elif h == "turnover":                                 COL["turnover"]   = i
                elif h in ("% change","% chg","change%","chg%"):     COL["change_pct"] = i
                elif h == "vwap":                                     COL["vwap"]       = i
                elif h in ("open","opening"):                        COL["open"]       = i
                elif h == "high":                                     COL["high"]       = i
                elif h == "low":                                      COL["low"]        = i
                elif h in ("prev close","previous close","prev. close"): COL["prev_close"] = i
                elif "52" in h and "high" in h:                       COL["high_52w"]   = i
                elif "52" in h and "low"  in h:                       COL["low_52w"]    = i

        def gc(cols, key, as_int=False):
            idx = COL.get(key, -1)
            if idx < 0 or idx >= len(cols): return 0
            return _si(cols[idx]) if as_int else _sf(cols[idx])

        stocks = []
        for row in tbl.find_all("tr")[1:]:
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cols) < 18 or not cols[1]: continue
            sym = cols[COL["symbol"]].strip()
            if not sym or sym.lower() in ("symbol","scrip","total","","-"): continue

            # Company name: cols[2] is confirmed as company name text
            cname = cols[COL["company"]].strip() if len(cols) > COL["company"] else ""
            # Safety: if it parses as a pure number, discard it (shouldn't happen but guard)
            if re.match(r'^[\d,\.]+$', cname): cname = ""

            ltp = gc(cols, "ltp") or gc(cols, "close")
            stocks.append({
                "symbol":       sym,
                "company_name": cname,
                "ltp":          ltp,
                "change_pct":   gc(cols, "change_pct"),
                "open":         gc(cols, "open"),
                "high":         gc(cols, "high"),
                "low":          gc(cols, "low"),
                "close":        gc(cols, "close") or ltp,
                "vwap":         gc(cols, "vwap"),
                "volume":       gc(cols, "volume", as_int=True),
                "prev_close":   gc(cols, "prev_close"),
                "turnover":     gc(cols, "turnover"),
                "trades":       gc(cols, "trades",  as_int=True),
                "high_52w":     gc(cols, "high_52w"),
                "low_52w":      gc(cols, "low_52w"),
                "trade_date":   trade_date,
                "source":       "Sharesansar",
            })
        return stocks
    except: return []

def merolagani_today():
    try:
        SESSION.get("https://merolagani.com/", timeout=TIMEOUT)
        r    = SESSION.get("https://merolagani.com/LatestMarket.aspx", timeout=TIMEOUT)
        soup = BeautifulSoup(r.text, "html.parser")
        # Try to detect trade date from page
        trade_date = ""
        for el in soup.find_all(["h1","h2","h3","h4","p","caption","span","div","title"]):
            t = el.get_text(strip=True)
            m = re.search(r'(\d{4}[-/]\d{2}[-/]\d{2}|\d{1,2}[-/ ]\w{3,9}[-/ ]\d{4})', t)
            if m:
                trade_date = _parse_date(m.group(1))
                if trade_date: break
        for tbl in soup.find_all("table"):
            rows = tbl.find_all("tr")
            if len(rows) < 3: continue
            hdrs = [th.get_text(strip=True) for th in rows[0].find_all(["th","td"])]
            if "Symbol" not in hdrs: continue
            stocks = []
            for row in rows[1:]:
                cols = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cols) < 7 or not cols[0]: continue
                stocks.append({
                    "symbol":cols[0].strip(),"ltp":_sf(cols[1]),"change_pct":_sf(cols[2]),
                    "high":_sf(cols[3]),"low":_sf(cols[4]),"open":_sf(cols[5]),
                    "volume":_si(cols[6]),"prev_close":_sf(cols[7]) if len(cols)>7 else 0.0,
                    "close":_sf(cols[1]),"trade_date":trade_date,"source":"MeroLagani",
                })
            if stocks: return stocks
        return []
    except: return []



# ══════════════════════════════════════════════════════════════════
#  NEPSE OFFICIAL LIVE MARKET  (LTP + change from nepalstock.com)
# ══════════════════════════════════════════════════════════════════

def nepse_live_market():
    """
    Fetch live LTP data from NEPSE official API (nepalstock.com).
    Uses the WAT-authenticated token from ns_fetcher.
    Returns list of {symbol, ltp, change_pct, open, high, low, volume, turnover, ...}
    Returns [] on failure.
    """
    try:
        from ns_fetcher import ns_get_token, NS_BASE, NS_HDRS
        token = ns_get_token()
        if not token:
            return []
        hdrs = dict(NS_HDRS)
        hdrs["Authorization"] = token
        # Primary: live-market endpoint
        for endpoint in [
            f"{NS_BASE}/api/nots/live-market",
            f"{NS_BASE}/api/nots/market-summary",
            f"{NS_BASE}/api/nots/today-price/all",
        ]:
            try:
                r = SESSION.get(endpoint, headers=hdrs, timeout=TIMEOUT, verify=False)
                if r.status_code != 200:
                    continue
                data = r.json()
                # Unwrap common envelope shapes
                if isinstance(data, dict):
                    data = (data.get("object") or data.get("data") or
                            data.get("content") or data.get("result") or [])
                if not isinstance(data, list) or not data:
                    continue
                # Check if first item has LTP-like fields
                sample = data[0] if data else {}
                ltp_key = next((k for k in ["lastTradedPrice","ltp","closePrice","lastPrice","close"] if k in sample), None)
                sym_key = next((k for k in ["symbol","stockSymbol","securityName","scrip"] if k in sample), None)
                if not ltp_key or not sym_key:
                    continue
                results = []
                trade_date = ""
                for row in data:
                    sym = str(row.get(sym_key, "") or "").strip().upper()
                    if not sym or sym in ("-", "TOTAL", ""):
                        continue
                    ltp = _sf(row.get(ltp_key, 0))
                    if not ltp:
                        continue
                    prev = _sf(row.get("previousClose", 0) or row.get("prevClose", 0) or
                               row.get("closingPrice", 0) or row.get("previousDayPrice", 0))
                    chg_pct = 0.0
                    if prev and ltp:
                        chg_pct = round((ltp - prev) / prev * 100, 2)
                    else:
                        chg_pct = _sf(row.get("percentageChange", 0) or row.get("changePercent", 0) or
                                      row.get("pChange", 0))
                    bd = (str(row.get("businessDate", "") or row.get("tradeDate", "") or "")).strip()
                    if bd and not trade_date:
                        trade_date = _parse_date(bd) or bd[:10]
                    results.append({
                        "symbol":       sym,
                        "ltp":          ltp,
                        "change_pct":   chg_pct,
                        "open":         _sf(row.get("openPrice", 0) or row.get("open", 0)),
                        "high":         _sf(row.get("highPrice", 0) or row.get("high", 0)),
                        "low":          _sf(row.get("lowPrice", 0)  or row.get("low", 0)),
                        "prev_close":   prev,
                        "close":        ltp,
                        "volume":       _si(row.get("totalTradeQuantity", 0) or row.get("volume", 0) or
                                            row.get("shareTraded", 0)),
                        "turnover":     _sf(row.get("totalTradeValue", 0) or row.get("turnover", 0) or
                                            row.get("amount", 0)),
                        "trades":       _si(row.get("totalTrades", 0) or row.get("numberOfTrans", 0)),
                        "high_52w":     _sf(row.get("fiftyTwoWeekHigh", 0) or row.get("high52W", 0) or
                                            row.get("weekHigh52", 0)),
                        "low_52w":      _sf(row.get("fiftyTwoWeekLow", 0)  or row.get("low52W", 0) or
                                            row.get("weekLow52", 0)),
                        "trade_date":   trade_date,
                        "source":       "NEPSE",
                    })
                if results:
                    return results
            except Exception:
                continue
        return []
    except Exception:
        return []

_SECTOR_CACHE = {}   # symbol → {sector, company_name}
_SECTOR_TS    = 0



def _load_sector_map():
    """
    Scrape sector + company name mapping.
    MeroLagani /CompanyList.aspx has multiple tables, each preceded by an <h3>/<h4>
    sector heading. Columns: Symbol | Company Name | Listed Shares | Paidup Value | Total Paidup
    (NO sector column inside the table — sector comes from the heading above it).
    """
    global _SECTOR_CACHE, _SECTOR_TS
    import time as _time
    now = _time.time()
    if _SECTOR_CACHE and now - _SECTOR_TS < 21600:
        return _SECTOR_CACHE

    result = {}

    # ── Primary: MeroLagani CompanyList ───────────────────────────
    try:
        sess = requests.Session()
        sess.headers.update({"User-Agent": _UA,
                             "Accept": "text/html,application/xhtml+xml,*/*",
                             "Accept-Language": "en-US,en;q=0.9"})
        r = sess.get("https://merolagani.com/CompanyList.aspx", timeout=TIMEOUT)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            current_sector = ""
            # Walk all block-level elements in order to track h3/h4 → table pairs
            for el in soup.find_all(["h3", "h4", "h2", "table"]):
                tag = el.name
                if tag in ("h2", "h3", "h4"):
                    text = el.get_text(strip=True)
                    # Skip generic headings
                    if text and text.lower() not in ("listed companies", ""):
                        current_sector = text
                elif tag == "table":
                    rows = el.find_all("tr")
                    if len(rows) < 2:
                        continue
                    # Identify header row
                    hdrs = [td.get_text(strip=True).lower()
                            for td in rows[0].find_all(["th", "td"])]
                    sym_i  = next((i for i, h in enumerate(hdrs) if "symbol"  in h), None)
                    name_i = next((i for i, h in enumerate(hdrs)
                                   if "company" in h or "name" in h), None)
                    if sym_i is None:
                        continue
                    for row in rows[1:]:
                        cells = [td.get_text(strip=True)
                                 for td in row.find_all("td")]
                        if not cells or sym_i >= len(cells):
                            continue
                        sym = cells[sym_i].strip().upper()
                        if not sym:
                            continue
                        cname = (cells[name_i].strip()
                                 if name_i is not None and name_i < len(cells)
                                 else "")
                        result[sym] = {
                            "sector":       current_sector,
                            "company_name": cname,
                        }
    except Exception:
        pass

    # ── Fallback 1: NEPSE official API ────────────────────────────
    if not result:
        try:
            r2 = requests.get(
                "https://newweb.nepalstock.com.np/api/nots/security?nonDelisted=true",
                timeout=TIMEOUT,
                headers={"Accept": "application/json",
                         "Referer": "https://newweb.nepalstock.com.np/"}
            )
            if r2.status_code == 200:
                data = r2.json()
                if isinstance(data, dict):
                    data = data.get("object", data.get("data", []))
                for sec in (data if isinstance(data, list) else []):
                    sym = (sec.get("symbol") or
                           sec.get("securityShortName") or "").strip().upper()
                    if sym:
                        result[sym] = {
                            "sector":       (sec.get("sectorName") or
                                             sec.get("instrumentType") or ""),
                            "company_name": (sec.get("securityName") or
                                             sec.get("companyName") or ""),
                        }
        except Exception:
            pass

    # ── Fallback 2: Sharesansar listed companies ──────────────────
    if not result:
        try:
            _sharesansar_sector_scrape(result)
        except Exception:
            pass

    # ── Static fallback: well-known NEPSE symbols ─────────────────
    # Applied ONLY for symbols with no company_name from live scraping
    _STATIC_NAMES = {
        # Commercial Banks
        "NABIL":"Nabil Bank Limited","NICA":"NIC Asia Bank Limited","GBIME":"Global IME Bank Limited",
        "EBL":"Everest Bank Limited","SANIMA":"Sanima Bank Limited","KBL":"Kumari Bank Limited",
        "MBL":"Machhapuchchhre Bank Limited","NMB":"NMB Bank Limited","PRVU":"Prabhu Bank Limited",
        "SBL":"Siddhartha Bank Limited","SRBL":"Sunrise Bank Limited","BOKL":"Bank of Kathmandu Limited",
        "CZBIL":"Citizen Bank International Limited","HBL":"Himalayan Bank Limited",
        "PCBL":"Prime Commercial Bank Limited","SCB":"Standard Chartered Bank Nepal Limited",
        "ADBL":"Agricultural Development Bank Limited","NIMB":"Nepal Investment Mega Bank Limited",
        "CCBL":"Civil Bank Limited","SBI":"Nepal SBI Bank Limited","MEGA":"Mega Bank Nepal Limited",
        "JBNL":"Janata Bank Nepal Limited","LBBL":"Lumbini Bikas Bank Limited",
        "NBB":"Nepal Bangladesh Bank Limited","SHINE":"Shine Resunga Development Bank Limited",
        "CBL":"Century Bank Limited","SBL":"Siddhartha Bank Limited",
        # Development Banks
        "MNBBL":"Muktinath Bikas Bank Limited","KSBBL":"Kamana Sewa Bikas Bank Limited",
        "SADBL":"Sindhu Bikash Bank Limited","CORBL":"Corporate Development Bank Limited",
        "EDBL":"Excel Development Bank Limited","GBBL":"Garima Bikas Bank Limited",
        "MLBL":"Mahalaxmi Bikas Bank Limited","NABBC":"Narayani Development Bank Limited",
        "SINDU":"Sindhu Bikash Bank Limited","SAPDBL":"Saptakoshi Development Bank Limited",
        "MIDBL":"Miteri Development Bank Limited",
        # Finance Companies
        "ICFC":"ICFC Finance Limited","GUFL":"Goodwill Finance Limited",
        "MFIL":"Merchant Finance Limited","AFCL":"Artha Finance Limited",
        "PFL":"Pokhara Finance Limited","GFCL":"Goodwill Finance Limited",
        "CFCL":"Capital Finance Company Limited","JFL":"Janaki Finance Company Limited",
        "SFL":"Shree Finance Limited","NCHL":"Nepal Clearing House Limited",
        "NFCL":"NIDC Development Bank Limited","RLFL":"Reliance Finance Limited",
        "SMFL":"Synergy Finance Limited","MKCL":"Mahakali Finance Limited",
        # Life Insurance
        "NLICL":"Nepal Life Insurance Company Limited","LICN":"Life Insurance Corporation Nepal Limited",
        "NLIC":"National Life Insurance Company Limited","ALICL":"Asian Life Insurance Company Limited",
        "ULIF":"Union Life Insurance Company Limited","JLIC":"Jyoti Life Insurance Company Limited",
        "GLICL":"Gurans Life Insurance Company Limited","PMHIL":"Prime Life Insurance Company Limited",
        "PLIC":"Progressive Life Insurance Company Limited","SLICL":"Sun Nepal Life Insurance Company Limited",
        "SNLI":"Sunrise Life Insurance Company Limited","SRLI":"Sanima Reliance Life Insurance Limited",
        "HLI":"Himalayan Life Insurance Company Limited","ILI":"IME Life Insurance Company Limited",
        # Non-Life Insurance
        "SICL":"Sagarmatha Insurance Company Limited","PRIN":"Premier Insurance Company Limited",
        "SGIC":"Shikhar Insurance Company Limited","NIL":"Nepal Insurance Company Limited",
        "PIL":"Premier Insurance Company Limited","RBCL":"Rastriya Beema Company Limited",
        "PICL":"Prudential Insurance Company Limited","ILBS":"Insurance and Financial Services Limited",
        "HGI":"Himalayan General Insurance Company Limited","IGIL":"IGI Insurance Company Limited",
        "NLG":"NLG Insurance Company Limited","SIC":"Siddhartha Insurance Limited",
        "NICL":"NIC Asia Insurance Limited","SALICO":"Sagarmatha Lumbini Insurance Company Limited",
        "SPIL":"Sanima Premier Insurance Limited",
        # Hydropower
        "CHCL":"Chilime Hydropower Company Limited","NHPC":"National Hydropower Company Limited",
        "UPPER":"Upper Tamakoshi Hydropower Limited","RADHI":"Ridi Hydropower Development Company Limited",
        "API":"Arun Kabeli Power Limited","RURU":"Ruru Hydropower Limited",
        "KPCL":"Kulekhani Hydropower Company Limited","AKPL":"Arun Kabeli Power Limited",
        "PPCL":"Panchthar Power Company Limited","UMRH":"Upper Marsyangdi Hydropower Limited",
        "BARUN":"Barun Hydropower Company Limited","NHDL":"Nepal Hydro Developers Limited",
        "KBSH":"Kabeli Bhanjyang Hydropower Limited","MHNL":"Madi Hydropower Development Company Limited",
        "GHL":"Gandaki Hydropower Limited","SHPC":"Sanima Mai Hydropower Limited",
        "RHPL":"Radhi Hydropower Limited","DOLTI":"Dolti Power Company Limited",
        "BEDC":"Butwal Electric Company Limited","AHPC":"Arun Hydropower Development Company Limited",
        "AKJCL":"Annapurna Khola Jor Centralized Hydropower Limited","AHL":"Api Hydropower Limited",
        "PHCL":"Pauwer Hydropower Company Limited","HDHPC":"Himal Dolakha Hydropower Company Limited",
        "SAHAS":"Sahas Urja Limited","RIDI":"Ridi Hydropower Development Company Limited",
        "NGPL":"Nepal Ganga Paroject Limited","USHEC":"Upper Solu Hydropower Energy Company Limited",
        "CKHL":"Chamelia Khola Hydropower Limited","PMHPL":"Panchthar Miklajung Hydropower Limited",
        "BPCL":"Butwal Power Company Limited","HURJA":"Hurja Hydropower Limited",
        "MKJC":"Manakamana Ji Hydropower Limited","NWCL":"Nepal Water and Energy Development Company Limited",
        "HPPL":"Hydro Power Project Limited","SPDL":"Sindhuli Power Development Limited",
        "MAKAR":"Makar Hydropower Development Limited","GLH":"Gandaki Lagani Hydropower Limited",
        "GILB":"Ghalemdi Hydropower Limited","UPCL":"Upper Palpa Hydro Limited","HRL":"Himal Rachna Limited",
        # Microfinance
        "ACLBSL":"Aarambha Chautari Laghubitta Bittiya Sanstha Limited",
        "DDBL":"Deprosc Laghubitta Bittiya Sanstha Limited",
        "SWBBL":"Swabalamban Laghubitta Bittiya Sanstha Limited",
        "SMFDB":"Sana Kisan Bikas Laghubitta Bittiya Sanstha Limited",
        "RMDC":"Remittance and Development Finance Company Limited",
        "NMMB":"National Microfinance and Development Bank Limited",
        "CBBL":"City Express Finance Company Limited",
        "MERO":"Mero Microfinance Laghubitta Bittiya Sanstha Limited",
        "FOWAD":"Fowad Microfinance Laghubitta Bittiya Sanstha Limited",
        "MLBBL":"Manushi Laghubitta Bittiya Sanstha Limited",
        "GBLBS":"Global IME Laghubitta Bittiya Sanstha Limited",
        "SAMAJ":"Samaj Laghubitta Bittiya Sanstha Limited",
        "RSDC":"Rasuwagadhi Development Sanstha Limited",
        "SKBBL":"Sana Kisan Bikas Laghubitta Bittiya Sanstha Limited",
        "SDBL":"Sindhu Bikash Bank Limited","USLB":"Uttarganga Laghubitta Bittiya Sanstha Limited",
        "WNLB":"Wean Nepal Laghubitta Bittiya Sanstha Limited",
        "NICLBSL":"NIC Asia Laghubitta Bittiya Sanstha Limited",
        "JBBL":"Jyoti Bikash Bank Limited","KLBSL":"Kalika Laghubitta Bittiya Sanstha Limited",
        "NUBL":"Nerude Laghubitta Bittiya Sanstha Limited",
        "SLBSL":"Sana Laghubitta Bittiya Sanstha Limited",
        "MLBBL":"Mahila Laghubitta Bittiya Sanstha Limited",
        "NSLB":"National Savings and Credit Co-operative Society Limited",
        "SVPCL":"Sewa Laghubitta Bittiya Sanstha Limited",
        "LBSL":"Laxmi Laghubitta Bittiya Sanstha Limited",
        "GMFBS":"Grameen Microfinance Laghubitta Bittiya Sanstha Limited",
        "UNLB":"Unnati Laghubitta Bittiya Sanstha Limited",
        "ALBSL":"Aadhikhola Laghubitta Bittiya Sanstha Limited",
        "ACLBSL":"Aarambha Chautari Laghubitta Bittiya Sanstha Limited",
        "MSLB":"Mithila Laghubitta Bittiya Sanstha Limited",
        "TSLB":"Tinau Laghubitta Bittiya Sanstha Limited",
        "GLBSL":"Grameen Laghubitta Bittiya Sanstha Limited",
        # Manufacturing & Others
        "NTC":"Nepal Telecom","SHIVM":"Shivam Cements Limited","BGWT":"Bottlers Nepal (Balaju) Limited",
        "UNL":"Unilever Nepal Limited","OMHL":"Oriental Hotels Limited","HDL":"Himalayan Distillery Limited",
        "NBBL":"Nepal Bank Limited","NRIC":"Nepal Reinsurance Company Limited",
        "BBC":"Butwal Brewery Company Limited","NLO":"National Life Insurance Company Nepal Limited",
        "HIDCL":"Hydroelectricity Investment and Development Company Limited",
        "NIFRA":"Nepal Infrastructure Bank Limited","CEDB":"Citizen Energy and Development Bank Limited",
        "SHL":"Solu Hotel Limited","OHL":"Oriental Hotels Limited","TRH":"Tara Hotel and Resort Limited",
        "YHL":"Yeti Hotels Limited","SONA":"Sonapur Minerals Oil Company Limited",
        "MEN":"Mithila Energy Limited","SJLIC":"Shree Ram Life Insurance Company Limited",
        "BHL":"Biratnagar Jute Mills Limited","KEF":"Kathmandu Finance Limited",
    }
    for sym, name in _STATIC_NAMES.items():
        if sym not in result:
            result[sym] = {"sector": "", "company_name": name}
        elif not result[sym].get("company_name"):
            result[sym]["company_name"] = name

    if result:
        _SECTOR_CACHE = result
        _SECTOR_TS    = now
    return result


def _sharesansar_sector_scrape(result_dict):
    """Scrape Sharesansar listed companies page for symbol+name+sector."""
    try:
        r = requests.get("https://www.sharesansar.com/listed-companies",
                         timeout=TIMEOUT, headers={"User-Agent": _UA})
        if r.status_code != 200: return
        soup = BeautifulSoup(r.text, "html.parser")
        for tbl in soup.find_all("table"):
            rows = tbl.find_all("tr")
            if len(rows) < 3: continue
            hdrs = [td.get_text(strip=True).lower() for td in rows[0].find_all(["th","td"])]
            sym_i  = next((i for i,h in enumerate(hdrs) if "symbol" in h), None)
            name_i = next((i for i,h in enumerate(hdrs) if "company" in h or "name" in h), None)
            sec_i  = next((i for i,h in enumerate(hdrs) if "sector" in h or "category" in h), None)
            if sym_i is None: continue
            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if not cells or sym_i >= len(cells): continue
                sym = cells[sym_i].strip().upper()
                if not sym: continue
                result_dict[sym] = {
                    "sector":       cells[sec_i].strip() if sec_i and sec_i < len(cells) else "",
                    "company_name": cells[name_i].strip() if name_i and name_i < len(cells) else "",
                }
            if result_dict: break
    except Exception: pass


def _enrich_sector_name(stocks):
    """Add sector and company_name to each stock dict from cached sector map."""
    sec_map = _load_sector_map()
    for s in stocks:
        sym = s.get("symbol","")
        if sym in sec_map:
            if not s.get("sector"):
                s["sector"] = sec_map[sym].get("sector","")
            if not s.get("company_name"):
                s["company_name"] = sec_map[sym].get("company_name","")



def _enrich_sector_name(stocks):
    """Add sector and company_name to each stock dict from cached sector map."""
    sec_map = _load_sector_map()
    for s in stocks:
        sym = s.get("symbol","")
        if sym in sec_map:
            if not s.get("sector"):
                s["sector"] = sec_map[sym].get("sector","")
            if not s.get("company_name"):
                s["company_name"] = sec_map[sym].get("company_name","")


def sharesansar_ltp(symbol):
    """
    Fetch last traded price for a single symbol from ShareSansar.
    Works even on closed market days (returns last trading day data).
    Returns dict with ltp, change_pct, high_52w, low_52w, vwap or {} on failure.
    """
    sym = symbol.strip().upper()
    try:
        r = SESSION.get(f"https://www.sharesansar.com/company/{sym.lower()}",
                        timeout=TIMEOUT)
        if r.status_code != 200:
            return {}
        soup = BeautifulSoup(r.text, "html.parser")
        result = {"symbol": sym}
        full = soup.get_text(" ", strip=True)
        # Extract LTP
        ltp_m = re.search(r"Ltp[:\s]+([\d,\.]+)", full, re.IGNORECASE)
        if ltp_m: result["ltp"] = _sf(ltp_m.group(1))
        # Extract 52W High-Low: "562.00 - 471.00"
        w52_m = re.search(r"52\s*Week\s*High-Low\s*:?\s*([\d,\.]+)\s*-\s*([\d,\.]+)", full, re.IGNORECASE)
        if w52_m:
            result["high_52w"] = _sf(w52_m.group(1))
            result["low_52w"]  = _sf(w52_m.group(2))
        # Use today() data if available (has vwap, change_pct etc)
        today_list = sharesansar_today()
        if today_list:
            td = next((s for s in today_list if s["symbol"] == sym), None)
            if td:
                for k in ["ltp","change_pct","open","high","low","close",
                          "vwap","volume","turnover","prev_close","high_52w","low_52w"]:
                    if td.get(k): result[k] = td[k]
        return result
    except Exception:
        return {}

def fetch_market_today(source="auto", force_refresh=False):
    """
    Fetch today's NEPSE market prices.
    source: 'auto' | 'ml' | 'ss'
      auto -> MeroLagani (fast) with 52W data overlaid from ShareSansar
      ml   -> MeroLagani only
      ss   -> ShareSansar only (slower but has 52W natively)
    force_refresh=True bypasses 5-min in-memory cache (used by Refresh button).
    """
    # Serve from in-memory cache if fresh
    if not force_refresh:
        cached_data, cached_src, cached_at = get_cached_market(source)
        if cached_data is not None:
            return cached_data, cached_src + " (cached " + cached_at[11:16] + ")"

    # Live fetch
    if source in ("auto", "ml"):
        try:
            s = merolagani_today()
            if s:
                _enrich_sector_name(s)
                if source == "auto":
                    try:
                        ss = sharesansar_today()
                        ss_map = {p["symbol"]: p for p in ss} if ss else {}
                        for p in s:
                            sp = ss_map.get(p["symbol"], {})
                            if not p.get("high_52w") and sp.get("high_52w"):
                                p["high_52w"] = sp["high_52w"]
                            if not p.get("low_52w") and sp.get("low_52w"):
                                p["low_52w"] = sp["low_52w"]
                            if not p.get("vwap") and sp.get("vwap"):
                                p["vwap"] = sp["vwap"]
                    except Exception:
                        pass
                set_cached_market(source, s, "MeroLagani")
                return s, "MeroLagani"
        except Exception:
            pass
        if source == "ml":
            return [], "No data"

    if source in ("auto", "ss"):
        try:
            s = sharesansar_today()
            if s:
                _enrich_sector_name(s)
                set_cached_market(source, s, "ShareSansar")
                return s, "ShareSansar"
        except Exception:
            pass

    return [], "No data"

def get_company_name(symbol):
    """Quick lookup of company full name."""
    sec_map = _load_sector_map()
    entry   = sec_map.get(symbol.upper(), {})
    return entry.get("company_name", "")


# ══════════════════════════════════════════════════════════════════
#  FINANCIALS  (Sharesansar scrape — no CSRF needed)
# ══════════════════════════════════════════════════════════════════

def sharesansar_financials(symbol):
    try:
        r    = SESSION.get(f"https://www.sharesansar.com/company/{symbol.lower()}", timeout=TIMEOUT)
        soup = BeautifulSoup(r.text, "html.parser")
        res  = {"symbol": symbol.upper()}
        for el in soup.find_all(["td","th","span","div","li","p"]):
            t = el.get_text(strip=True)
            if ":" in t and len(t) < 120:
                k, _, v = t.partition(":"); k = k.strip().lower(); v = v.strip()
                nums = re.findall(r"[\d,]+\.?\d*", v)
                if nums:
                    val = _sf(nums[0])
                    if "eps"         in k and not res.get("eps"):          res["eps"]        = val
                    if "p/e"         in k and not res.get("pe"):           res["pe"]         = val
                    if "book value"  in k and not res.get("book_value"):   res["book_value"] = val
                    if "pbv"         in k and not res.get("pbv"):          res["pbv"]        = val
                    if "market cap"  in k and not res.get("market_cap"):   res["market_cap"] = round(val/1e9, 2) if val > 1e8 else round(val, 2)
                    if "52 week hi"  in k and not res.get("high_52w"):     res["high_52w"]   = val
                    if "52 week lo"  in k and not res.get("low_52w"):      res["low_52w"]    = val
                    if "listed shar" in k and not res.get("listed_shares"):res["listed_shares"] = _si(nums[0])
        # overlay live price
        today = sharesansar_today()
        td    = next((s for s in today if s["symbol"] == symbol.upper()), None)
        if td:
            for k in ["ltp","open","high","low","close","prev_close",
                      "volume","turnover","change_pct","high_52w","low_52w","vwap"]:
                if td.get(k): res[k] = td[k]
        return res
    except Exception: return {"symbol": symbol.upper()}


# ══════════════════════════════════════════════════════════════════
#  FLOORSHEET  (Sharesansar — single POST, single-use token OK)
# ══════════════════════════════════════════════════════════════════

_SS_CID      = {}
_SS_SESSIONS = {}

def _ss_token(symbol):
    sym  = symbol.upper()
    sess = _SS_SESSIONS.get(sym)
    if sess is None:
        sess = requests.Session()
        sess.headers.update({"User-Agent":_UA,"Accept":"text/html,*/*",
                             "Accept-Language":"en-US,en;q=0.9"})
        _SS_SESSIONS[sym] = sess
    r = sess.get(f"https://www.sharesansar.com/company/{sym.lower()}", timeout=TIMEOUT)
    if r.status_code != 200:
        return None, None, None
    soup  = BeautifulSoup(r.text, "html.parser")
    inp   = soup.find("input", {"name":"_token"})
    token = inp["value"].strip() if (inp and inp.get("value")) else ""
    if not token: return None, None, None
    cid = _SS_CID.get(sym,"")
    if not cid:
        el = soup.find(id="companyid")
        if el: cid = re.sub(r"[^0-9]","", el.get_text())
    if not cid:
        m = re.search(r"id=[\"']companyid[\"'][^>]*>\s*(\d+)", r.text)
        if m: cid = m.group(1)
    if not cid: return None, None, None
    _SS_CID[sym] = cid
    return sess, token, cid

def sharesansar_floorsheet(symbol, max_rows=500):
    try:
        sess, token, cid = _ss_token(symbol)
        if not sess: return []
        hdrs = {"X-Requested-With":"XMLHttpRequest","Origin":"https://www.sharesansar.com",
                "Referer":f"https://www.sharesansar.com/company/{symbol.lower()}",
                "Accept":"application/json, text/javascript, */*; q=0.01",
                "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8"}
        r = sess.post("https://www.sharesansar.com/company-floor-sheet",
                      data={"draw":"1","start":"0","length":str(max_rows),
                            "company":cid,"_token":token},
                      headers=hdrs, timeout=TIMEOUT)
        if r.status_code != 200: return []
        rows   = r.json().get("data",[])
        result = []
        for row in rows:
            if isinstance(row, dict):
                result.append({
                    "sn":            _si(row.get("sn",0)),
                    "buyer_broker":  str(row.get("buyer_member_id")  or row.get("buyerBroker","")  or ""),
                    "seller_broker": str(row.get("seller_member_id") or row.get("sellerBroker","") or ""),
                    "qty":           _si(row.get("contract_quantity") or row.get("qty",0)),
                    "rate":          _sf(row.get("contract_rate")     or row.get("rate",0)),
                    "amount":        _sf(row.get("contract_amount")   or row.get("amount",0)),
                    "time":          str(row.get("trade_time","")     or row.get("time","") or ""),
                })
        return result
    except Exception: return []

def analyze_brokers(floorsheet):
    summary = {}
    for row in floorsheet:
        for broker, side in [(row.get("buyer_broker",""),"buy"),(row.get("seller_broker",""),"sell")]:
            if not broker or broker in ("-","","0","None"): continue
            if broker not in summary:
                summary[broker] = {"buy_qty":0,"sell_qty":0,"buy_amount":0.0,
                                   "sell_amount":0.0,"buy_trades":0,"sell_trades":0}
            q=row.get("qty",0); a=row.get("amount",0.0)
            if side=="buy":
                summary[broker]["buy_qty"]+=q; summary[broker]["buy_amount"]+=a; summary[broker]["buy_trades"]+=1
            else:
                summary[broker]["sell_qty"]+=q; summary[broker]["sell_amount"]+=a; summary[broker]["sell_trades"]+=1
    def mk(qk,ak,tk):
        return sorted([{"broker":b,"qty":d[qk],"amount":round(d[ak],2),"trades":d[tk],
                        "avg_rate":round(d[ak]/d[qk],2) if d[qk] else 0}
                       for b,d in summary.items() if d[qk]>0],key=lambda x:x["qty"],reverse=True)
    buyers=mk("buy_qty","buy_amount","buy_trades")
    sellers=mk("sell_qty","sell_amount","sell_trades")
    net=sorted([{"broker":b,"buy_qty":d["buy_qty"],"sell_qty":d["sell_qty"],
                 "net_qty":d["buy_qty"]-d["sell_qty"],
                 "direction":"BUY" if d["buy_qty"]>d["sell_qty"] else "SELL" if d["sell_qty"]>d["buy_qty"] else "NEUTRAL",
                 "buy_amount":round(d["buy_amount"],2),"sell_amount":round(d["sell_amount"],2)}
                for b,d in summary.items()],key=lambda x:abs(x["net_qty"]),reverse=True)
    tbq=sum(d["buy_qty"] for d in summary.values())
    tsq=sum(d["sell_qty"] for d in summary.values())
    return {"top_buyers":buyers[:15],"top_sellers":sellers[:15],"net_positions":net[:20],
            "stats":{"total_buy_qty":tbq,"total_sell_qty":tsq,
                     "unique_buyers":len(buyers),"unique_sellers":len(sellers),
                     "top_buyer":buyers[0]["broker"] if buyers else "—",
                     "top_seller":sellers[0]["broker"] if sellers else "—",
                     "market_sentiment":"BULLISH" if tbq>tsq*1.1 else "BEARISH" if tsq>tbq*1.1 else "NEUTRAL"}}


# ══════════════════════════════════════════════════════════════════
#  MASTER FETCH  (MeroLagani → NepseAlpha → NEPSE API)
# ══════════════════════════════════════════════════════════════════

def fetch_stock(symbol, source="auto", from_date=None, to_date=None, force_refresh=False):
    """
    Fetch OHLCV price history for analyze tab.
    Tries MeroLagani first (primary, full history), ShareSansar as fallback.
    Results cached in-memory for 1 hour — force_refresh=True bypasses cache.
    """
    symbol    = symbol.strip().upper()

    # Check in-memory history cache (1-hour TTL)
    if not force_refresh:
        cached = get_cached_history(symbol, from_date or "", to_date or "")
        if cached:
            return {k: v for k, v in cached.items() if k != "ts"}

    history   = []
    src_used  = ""
    error_msg = ""

    # 1. MeroLagani -- primary (proven working, full history)
    try:
        history = merolagani_history(symbol, from_date, to_date)
        if history:
            src_used = "MeroLagani"
    except Exception as e:
        error_msg = f"MeroLagani: {e}"

    # 2. ShareSansar -- fallback
    if not history:
        try:
            history = sharesansar_history(symbol, from_date, to_date)
            if history:
                src_used = "ShareSansar"
                error_msg = ""
        except Exception as e2:
            if not error_msg:
                error_msg = f"ShareSansar: {e2}"

    if history:
        # Primary fundamentals: ML company detail (has EPS, P/E, Book Value)
        fundam = {}
        try:
            fundam = merolagani_company_detail(symbol) or {}
        except Exception:
            pass
        fundam.setdefault("symbol", symbol)
        # Overlay live price from SS financials (faster, has 52W)
        try:
            ss = sharesansar_financials(symbol)
            for k in ["ltp","open","high","low","close","prev_close","volume",
                      "turnover","change_pct","high_52w","low_52w","vwap"]:
                if ss.get(k) and not fundam.get(k):
                    fundam[k] = ss[k]
        except Exception:
            pass
        # Final fallbacks from history if still missing
        if not fundam.get("ltp"):      fundam["ltp"]      = history[-1]["close"]
        if not fundam.get("high_52w"): fundam["high_52w"] = max(h["high"] for h in history)
        if not fundam.get("low_52w"):  fundam["low_52w"]  = min(h["low"]  for h in history)
        result = {"symbol": symbol, "history": history, "fundamentals": fundam,
                  "source": src_used, "is_real": True, "error": "",
                  "fetched_at": datetime.now().isoformat()}
        set_cached_history(symbol, from_date or "", to_date or "", result)
        return result

    today = merolagani_today() or sharesansar_today()
    td    = next((s for s in today if s["symbol"] == symbol), None)
    if not error_msg:
        error_msg = (f"{symbol} found in today's market but no historical data. Newly listed?"
                     if td else f"No data for '{symbol}'. Check the symbol spelling.")
    return {"symbol": symbol, "history": [], "fundamentals": td or {"symbol": symbol},
            "source": "No history", "is_real": bool(td), "error": error_msg,
            "fetched_at": datetime.now().isoformat()}

def fetch_multiple(symbols, source="auto", from_date=None, to_date=None):
    import os
    results = [None]*len(symbols)
    workers = min(len(symbols), os.cpu_count() or 4)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_stock,sym,source,from_date,to_date):i
                for i,sym in enumerate(symbols)}
        for fut in as_completed(futs):
            idx = futs[fut]
            try: results[idx] = fut.result()
            except Exception as e:
                results[idx] = {"symbol":symbols[idx],"history":[],"fundamentals":{},
                                "source":"Error","is_real":False,"error":str(e),
                                "fetched_at":datetime.now().isoformat()}
    return results


# ══════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════
#  MEROLAGANI FLOORSHEET  — Complete rewrite
#
#  MeroLagani Floorsheet.aspx mechanics (verified from browser DevTools):
#
#  1. GET  Floorsheet.aspx            → page HTML + cookies + viewstate
#  2. POST Floorsheet.aspx            → submit search (page 1)
#       lbtnSearchFloorsheet = ""     ← triggers search
#  3. POST Floorsheet.aspx            → navigate page N
#       __EVENTTARGET = PagerControl1$btnPaging
#       PagerControl1$hdnCurrentPage = "N"
#
#  Rows per page = 500 (NOT 30 — 30 is the history table)
#  Pager text:  "Showing X to Y of Z records"
# ══════════════════════════════════════════════════════════════════

_ML_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.2478.80",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]
_ML_FS_BASE = "https://merolagani.com/Floorsheet.aspx"
_ML_FS_ROWS = 500   # rows per floorsheet page


def _ml_fs_session():
    """Fresh session with full browser fingerprint."""
    import random
    sess = requests.Session()
    sess.headers.update({
        "User-Agent":                random.choice(_ML_UAS),
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Site":            "none",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-User":            "?1",
        "Sec-Fetch-Dest":            "document",
        "Sec-Ch-Ua":                 '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile":          "?0",
        "Sec-Ch-Ua-Platform":        '"Windows"',
        "Cache-Control":             "max-age=0",
    })
    return sess


def _ml_fs_hidden(soup):
    """Extract all ASP.NET hidden fields from soup or HTML string."""
    if isinstance(soup, str):
        soup = BeautifulSoup(soup, "html.parser")
    fields = {}
    for inp in soup.find_all("input", {"type": "hidden"}):
        n = inp.get("name", "")
        if n.startswith("__") or "hdnCurrentPage" in n or "hdnAutoSuggest" in n:
            fields[n] = inp.get("value", "") or ""
    return fields


def _ml_fs_post_data(hidden, page_num, is_search,
                      symbol="", buyer="", seller="", date=""):
    """Build POST body for Floorsheet.aspx."""
    d = {
        "__VIEWSTATE":          hidden.get("__VIEWSTATE", ""),
        "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
        "__EVENTVALIDATION":    hidden.get("__EVENTVALIDATION", ""),
        "__EVENTARGUMENT":      "",
        "ctl00$ASCompany$hdnAutoSuggest":    "0",
        "ctl00$ASCompany$txtAutoSuggest":    "",
        "ctl00$AutoSuggest1$hdnAutoSuggest": "0",
        "ctl00$AutoSuggest1$txtAutoSuggest": "",
        "ctl00$ContentPlaceHolder1$ASCompanyFilter$hdnAutoSuggest": "0",
        "ctl00$ContentPlaceHolder1$ASCompanyFilter$txtAutoSuggest": symbol.upper() if symbol else "",
        "ctl00$ContentPlaceHolder1$txtBuyerBrokerCodeFilter":       buyer  if buyer  else "",
        "ctl00$ContentPlaceHolder1$txtSellerBrokerCodeFilter":      seller if seller else "",
        "ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter":        date   if date   else "",
    }
    if is_search:
        d["__EVENTTARGET"] = ""
        d["ctl00$ContentPlaceHolder1$lbtnSearchFloorsheet"] = ""
    else:
        pfx = "ctl00$ContentPlaceHolder1$PagerControl1"
        d["__EVENTTARGET"] = f"{pfx}$btnPaging"
        d[f"{pfx}$hdnCurrentPage"] = str(page_num)
    return d


def _ml_fs_count(html):
    """
    Extract (total_records, total_pages) from pager text.
    Pattern: "Showing X to Y of Z records" or "of Z"
    Rows per page = 500.
    """
    for txt in BeautifulSoup(html, "html.parser").find_all(string=True):
        m = re.search(r"of\s+([\d,]+)(?:\s+records?)?", txt.strip(), re.I)
        if m:
            total = _si(m.group(1).replace(",", ""))
            if total > 0:
                pages = max(1, (total + _ML_FS_ROWS - 1) // _ML_FS_ROWS)
                return total, pages
    return 0, 0


def _ml_fs_parse(html):
    """
    Parse floorsheet HTML table into row dicts.
    Dynamic column mapping — resilient to column order changes.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tbl in soup.find_all("table"):
        trs = tbl.find_all("tr")
        if len(trs) < 2:
            continue
        hdrs = [td.get_text(strip=True).upper()
                for td in trs[0].find_all(["th", "td"])]
        if "SYMBOL" not in hdrs or "BUYER" not in hdrs:
            continue
        col = {}
        for i, h in enumerate(hdrs):
            if any(x in h for x in ("CONTRACT", "TRANSACT", "TXN")):
                col["txn"] = i
            elif h == "SYMBOL":
                col["sym"] = i
            elif h == "BUYER":
                col["buy"] = i
            elif h == "SELLER":
                col["sel"] = i
            elif h in ("QTY.", "QTY", "QUANTITY", "NO. SHARE"):
                col["qty"] = i
            elif "RATE" in h or h == "PRICE":
                col["rate"] = i
            elif "AMOUNT" in h or h == "TOTAL":
                col["amt"] = i
        if not all(k in col for k in ("buy", "sel", "qty")):
            continue
        def g(cells, key, default=""):
            idx = col.get(key)
            return cells[idx].strip() if idx is not None and idx < len(cells) else default
        for tr in trs[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) < 5:
                continue
            buyer  = g(cells, "buy")
            seller = g(cells, "sel")
            if not buyer and not seller:
                continue
            rows.append({
                "txn_no": g(cells, "txn"),
                "symbol": g(cells, "sym"),
                "buyer":  buyer,
                "seller": seller,
                "qty":    _si(g(cells, "qty")),
                "rate":   _sf(g(cells, "rate")),
                "amount": _sf(g(cells, "amt")),
            })
        if rows:
            break
    return rows


def _ml_fs_total_pages(html):
    """Legacy alias used by other functions — delegates to _ml_fs_count."""
    total, pages = _ml_fs_count(html)
    return total, pages


def merolagani_floorsheet_search(symbol="", buyer="", seller="",
                                  date="", max_pages=None):
    """
    Fetch ALL floorsheet rows from MeroLagani.

    Strategy:
      Phase 1: Serial first-page fetch to get total page count + viewstate
      Phase 2: Parallel fetch of remaining pages using multiple sessions
               (no sleep needed -- each session is independent)
      Phase 3: Dedup and return

    Typical performance: 232 pages in ~25-30s (vs 4+ minutes serial)
    """
    import sys, time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    _max = max_pages if max_pages is not None else sys.maxsize

    def _open_session():
        sess = _ml_fs_session()
        try:
            r = sess.get(_ML_FS_BASE, timeout=TIMEOUT_FS)
            if r.status_code != 200:
                return None, None
            return sess, _ml_fs_hidden(r.text)
        except Exception:
            return None, None

    post_hdrs = {
        "Referer":                   _ML_FS_BASE,
        "Origin":                    "https://merolagani.com",
        "Content-Type":              "application/x-www-form-urlencoded; charset=UTF-8",
        "Sec-Fetch-Site":            "same-origin",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-User":            "?1",
        "Sec-Fetch-Dest":            "document",
        "Cache-Control":             "max-age=0",
        "Upgrade-Insecure-Requests": "1",
    }

    # Phase 1: Get page 1 + total count
    sess, hidden = _open_session()
    if sess is None:
        return []

    body = _ml_fs_post_data(hidden, 1, True,
                             symbol=symbol, buyer=buyer,
                             seller=seller, date=date)
    try:
        resp = sess.post(_ML_FS_BASE, data=body,
                         headers=post_hdrs, timeout=TIMEOUT_FS)
        if resp.status_code != 200:
            return []
    except Exception:
        return []

    page1_rows = _ml_fs_parse(resp.text)
    total_recs, total_pgs = _ml_fs_count(resp.text)
    hidden1 = _ml_fs_hidden(resp.text)
    if hidden1.get("__VIEWSTATE"):
        hidden = hidden1

    if not page1_rows or total_pgs == 0:
        return page1_rows

    total_pgs = min(total_pgs, _max)

    if total_pgs == 1:
        return page1_rows

    all_rows = list(page1_rows)

    # Phase 2: Parallel fetch pages 2..N
    # Each worker opens its own session to avoid sharing cookies/viewstate
    # Use the page-1 hidden fields as the base for all pager POSTs
    def fetch_page_worker(page_num):
        for attempt in range(3):
            try:
                w_sess, w_hidden = _open_session()
                if w_sess is None:
                    continue
                # First POST the search to establish the session state
                b_search = _ml_fs_post_data(w_hidden, 1, True,
                                             symbol=symbol, buyer=buyer,
                                             seller=seller, date=date)
                r_search = w_sess.post(_ML_FS_BASE, data=b_search,
                                       headers=post_hdrs, timeout=TIMEOUT_FS)
                if r_search.status_code != 200:
                    continue
                h2 = _ml_fs_hidden(r_search.text)
                if h2.get("__VIEWSTATE"):
                    w_hidden = h2

                # Now navigate to the target page
                b_page = _ml_fs_post_data(w_hidden, page_num, False,
                                           symbol=symbol, buyer=buyer,
                                           seller=seller, date=date)
                r_page = w_sess.post(_ML_FS_BASE, data=b_page,
                                     headers=post_hdrs, timeout=TIMEOUT_FS)
                if r_page.status_code == 200:
                    return _ml_fs_parse(r_page.text)
            except Exception:
                _time.sleep(2 * (attempt + 1))
        return []

    # Use 8 workers max to avoid overwhelming the server
    workers = min(8, total_pgs - 1)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_page_worker, pg): pg
                for pg in range(2, total_pgs + 1)}
        for fut in as_completed(futs):
            try:
                pg_rows = fut.result()
                all_rows.extend(pg_rows)
            except Exception:
                pass

    # Dedup by txn_no
    seen = set()
    deduped = []
    for r in all_rows:
        key = r.get("txn_no", "")
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        deduped.append(r)

    return deduped


def merolagani_floorsheet_by_date(symbol="", buyer="", seller="",
                                   date_str="", max_pages=None):
    """Convert YYYY-MM-DD to MM/DD/YYYY and call floorsheet_search."""
    ml_date = ""
    if date_str:
        try:
            ml_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%m/%d/%Y")
        except ValueError:
            ml_date = date_str
    return merolagani_floorsheet_search(
        symbol=symbol, buyer=buyer, seller=seller,
        date=ml_date, max_pages=max_pages)


def _dedup_rows(rows):
    """
    Remove truly duplicate rows (same txn_no + buyer + seller + qty).
    Keeps partial fills (same txn_no, different counterparty or qty).
    qty is cast to int to avoid float/int mismatch.
    """
    seen = set()
    out  = []
    for r in rows:
        key = (
            r.get("txn_no",  ""),
            r.get("buyer",   ""),
            r.get("seller",  ""),
            int(r.get("qty", 0) or 0),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _sum_broker_side(rows, broker_field, broker_code):
    """Sum qty and amount for rows where broker_field == broker_code."""
    qty = sum(int(r.get("qty", 0) or 0) for r in rows
              if str(r.get(broker_field, "")).strip() == broker_code)
    amt = sum(float(r.get("amount", 0) or 0) for r in rows
              if str(r.get(broker_field, "")).strip() == broker_code)
    return qty, round(amt, 2)


def analyze_broker_activity(floorsheet_rows):
    """
    Compute per-broker buy/sell totals from a list of floorsheet rows.

    Each row has ONE buyer and ONE seller. We credit independently:
      - buyer  gets buy_qty  += row.qty  (BUY OBJECT)
      - seller gets sell_qty += row.qty  (SELL OBJECT)

    These are NEVER mixed. A self-trade row (buyer==seller) correctly
    increments BOTH the buy and sell totals for that broker.

    No deduplication is applied here — the caller is responsible for
    ensuring rows are unique before calling this function.
    """
    from collections import defaultdict

    # Completely separate dictionaries for buys and sells
    buy  = defaultdict(lambda: defaultdict(lambda: {"qty": 0, "amt": 0.0}))
    sell = defaultdict(lambda: defaultdict(lambda: {"qty": 0, "amt": 0.0}))
    buy_tot  = defaultdict(lambda: {"qty": 0, "amt": 0.0})
    sell_tot = defaultdict(lambda: {"qty": 0, "amt": 0.0})

    for row in floorsheet_rows:
        sym = str(row.get("symbol", "") or "")
        byr = str(row.get("buyer",  "") or "").strip()
        slr = str(row.get("seller", "") or "").strip()
        qty = int(row.get("qty",    0)  or 0)
        amt = float(row.get("amount", 0.0) or 0.0)

        if byr:
            buy[byr][sym]["qty"] += qty
            buy[byr][sym]["amt"] += amt
            buy_tot[byr]["qty"]  += qty
            buy_tot[byr]["amt"]  += amt

        if slr:
            sell[slr][sym]["qty"] += qty
            sell[slr][sym]["amt"] += amt
            sell_tot[slr]["qty"]  += qty
            sell_tot[slr]["amt"]  += amt

    all_brokers = set(list(buy_tot.keys()) + list(sell_tot.keys()))
    broker_list = []
    for bk in all_brokers:
        bq = buy_tot[bk]["qty"];  ba = buy_tot[bk]["amt"]
        sq = sell_tot[bk]["qty"]; sa = sell_tot[bk]["amt"]
        net = bq - sq
        broker_list.append({
            "broker":    bk,
            "buy_qty":   bq,  "sell_qty":  sq,  "net_qty":   net,
            "buy_amt":   round(ba, 2),
            "sell_amt":  round(sa, 2),
            "total_amt": round(ba + sa, 2),
            "direction": "BUY" if net > 0 else "SELL" if net < 0 else "NEUTRAL",
        })
    broker_list.sort(key=lambda x: x["total_amt"], reverse=True)

    # Per-broker symbol breakdown
    broker_symbols = {}
    for bk in all_brokers:
        syms = set(list(buy[bk].keys()) + list(sell[bk].keys()))
        sl   = []
        for s in syms:
            bq = buy[bk][s]["qty"];  sq = sell[bk][s]["qty"]
            net = bq - sq
            sl.append({
                "symbol": s, "buy_qty": bq, "sell_qty": sq, "net_qty": net,
                "buy_amt": round(buy[bk][s]["amt"], 2),
                "sell_amt": round(sell[bk][s]["amt"], 2),
                "direction": "BUY" if net > 0 else "SELL" if net < 0 else "NEUTRAL",
            })
        sl.sort(key=lambda x: x["buy_qty"] + x["sell_qty"], reverse=True)
        broker_symbols[bk] = sl   # all symbols, no cap

    return {
        "broker_list":    broker_list,   # all brokers, no cap
        "broker_symbols": broker_symbols,
        "total_rows":     len(floorsheet_rows),
    }


def _fetch_broker_day(broker, symbol, date_str):
    """
    Fetch one day's buy_qty and sell_qty for a specific broker.

    PROVEN CORRECT APPROACH:
    - Fetch1: buyer=broker  → count ONLY rows where buyer==broker  → buy_qty
    - Fetch2: seller=broker → count ONLY rows where seller==broker → sell_qty
    - No merging. No deduplication. Independent sums.

    Even if MeroLagani returns extra OR-filtered rows, we only count
    rows where the broker actually appears in the correct role.
    Even if a row appears twice in a result, the qty is still from the
    actual transaction — but we protect against that with txn_no dedup.
    """
    # Fetch 1: rows where this broker is the BUYER
    buy_rows = merolagani_floorsheet_by_date(
        symbol=symbol, buyer=broker, seller="",
        date_str=date_str)

    buy_qty = sum(int(r.get("qty", 0) or 0) for r in buy_rows
                  if str(r.get("buyer", "")).strip() == broker)
    buy_amt = sum(float(r.get("amount", 0) or 0) for r in buy_rows
                  if str(r.get("buyer", "")).strip() == broker)

    # Fetch 2: rows where this broker is the SELLER
    sell_rows = merolagani_floorsheet_by_date(
        symbol=symbol, buyer="", seller=broker,
        date_str=date_str)

    # Count only rows where broker IS the seller (filter out any OR-extras)
    sell_qty = sum(int(r.get("qty", 0) or 0) for r in sell_rows
                   if str(r.get("seller", "")).strip() == broker)
    sell_amt = sum(float(r.get("amount", 0) or 0) for r in sell_rows
                   if str(r.get("seller", "")).strip() == broker)

    return date_str, buy_qty, sell_qty, round(buy_amt, 2), round(sell_amt, 2)



def _fetch_day(broker, symbol, date_str):
    """
    Fetch one day's data for a specific broker.

    CRITICAL: Never pass broker as buyer/seller filter to MeroLagani.
    MeroLagani's filter is unreliable — it may return the same rows
    for different date queries, or duplicate results via OR behavior.

    Instead: fetch the FULL market floorsheet for the date (no broker filter),
    then filter locally in Python for rows where this broker participated.
    This is the only approach that gives consistent, correct counts.
    """
    # Fetch ALL rows for this symbol/date — no broker filter, no page cap
    all_rows = merolagani_floorsheet_by_date(
        symbol=symbol, buyer="", seller="",
        date_str=date_str)

    # Filter locally: keep only rows where this broker appears as buyer OR seller
    broker_rows = [r for r in all_rows
                   if str(r.get("buyer","")).strip()  == broker
                   or str(r.get("seller","")).strip() == broker]

    # Count using separate buy/sell objects (never mixed)
    bq = sum(int(r.get("qty", 0) or 0) for r in broker_rows if str(r.get("buyer","")).strip()  == broker)
    sq = sum(int(r.get("qty", 0) or 0) for r in broker_rows if str(r.get("seller","")).strip() == broker)
    ba = sum(float(r.get("amount", 0) or 0) for r in broker_rows if str(r.get("buyer","")).strip()  == broker)
    sa = sum(float(r.get("amount", 0) or 0) for r in broker_rows if str(r.get("seller","")).strip() == broker)

    return date_str, bq, sq, ba, sa


def accumulate_broker_range(broker, symbol="", from_date=None, to_date=None):
    """
    Fetch floorsheet for every trading day in range, parallel.
    Returns list of daily dicts sorted oldest→newest.
    """
    end_dt   = datetime.strptime(to_date,   "%Y-%m-%d") if to_date   else datetime.now()
    start_dt = datetime.strptime(from_date, "%Y-%m-%d") if from_date else end_dt - timedelta(days=30)

    dates = []
    d = start_dt
    while d <= end_dt:
        if d.weekday() not in (4, 5):   # skip Fri=4, Sat=5
            dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    # No cap — fetch ALL trading days in the requested range

    import os
    results_map = {}
    workers = os.cpu_count() or 4
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_fetch_day, broker, symbol, ds): ds for ds in dates}
        for fut in as_completed(futs):
            ds = futs[fut]
            try:
                date_str, bq, sq, ba, sa = fut.result()
                if bq + sq > 0:
                    results_map[date_str] = {
                        "date":     date_str,
                        "buy_qty":  bq,
                        "sell_qty": sq,
                        "net_qty":  bq - sq,
                        "buy_amt":  round(ba, 2),
                        "sell_amt": round(sa, 2),
                        "cumulative_net": 0,
                    }
            except Exception:
                pass

    results = sorted(results_map.values(), key=lambda x: x["date"])
    cum = 0
    for r in results:
        cum += r["net_qty"]
        r["cumulative_net"] = cum
    return results


# ══════════════════════════════════════════════════════════════════
#  BROKER NAME DIRECTORY
# ══════════════════════════════════════════════════════════════════

_BROKER_NAMES_CACHE = {}
_BROKER_NAMES_TS    = 0

def get_broker_names() -> dict:
    """
    Returns {code: name} dict for all NEPSE brokers.
    Primary:   MeroLagani BrokerList.aspx — confirmed working
               Columns: Broker Code | Broker Name | Landline | Address
               Skips sub-office entries like 6_RWS; only numeric codes kept.
    Fallback1: www.nepalstock.com/brokers (SSL issues — uses verify=False)
    Fallback2: Sharesansar /broker
    Cached 24 h.
    """
    import time as _time
    global _BROKER_NAMES_CACHE, _BROKER_NAMES_TS
    now = _time.time()
    if _BROKER_NAMES_CACHE and now - _BROKER_NAMES_TS < 86400:
        return _BROKER_NAMES_CACHE

    names = {}

    # ── Primary: MeroLagani BrokerList.aspx ─────────────────────
    # Real columns: Broker Code | Broker Name | Landline | Address
    # Skips sub-office entries (e.g. 6_RWS) — only pure numeric codes kept
    try:
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": _UA,
            "Accept": "text/html,application/xhtml+xml,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        })
        r = sess.get("https://merolagani.com/BrokerList.aspx", timeout=TIMEOUT)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            for tbl in soup.find_all("table"):
                rows = tbl.find_all("tr")
                if len(rows) < 3:
                    continue
                hdrs = [td.get_text(strip=True).lower()
                        for td in rows[0].find_all(["th", "td"])]
                code_i = next((i for i, h in enumerate(hdrs)
                               if "broker code" in h or "code" in h), 0)
                name_i = next((i for i, h in enumerate(hdrs)
                               if "broker name" in h or "name" in h), 1)
                for row in rows[1:]:
                    cells = [td.get_text(strip=True)
                             for td in row.find_all(["td", "th"])]
                    if len(cells) <= max(code_i, name_i):
                        continue
                    code = cells[code_i].strip()
                    name = cells[name_i].strip()
                    if code.isdigit() and name:
                        names[code] = name
                if names:
                    break
    except Exception:
        pass

    # ── Fallback 1: NEPSE official broker page (SSL issues) ───────
    # Real columns: S.N. | Broker No. | Broker Name | District
    if not names:
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            sess2 = requests.Session()
            sess2.headers.update({"User-Agent": _UA,
                                  "Accept": "text/html,application/xhtml+xml,*/*"})
            r2 = sess2.get("https://www.nepalstock.com/brokers",
                           timeout=TIMEOUT, verify=False)
            if r2.status_code == 200:
                soup2 = BeautifulSoup(r2.text, "html.parser")
                for tbl in soup2.find_all("table"):
                    rows = tbl.find_all("tr")
                    if len(rows) < 3:
                        continue
                    hdrs = [td.get_text(strip=True).lower()
                            for td in rows[0].find_all(["th", "td"])]
                    num_i  = next((i for i, h in enumerate(hdrs)
                                   if "broker no" in h), None)
                    name_i = next((i for i, h in enumerate(hdrs)
                                   if "broker name" in h), None)
                    if num_i is None:
                        num_i = next((i for i, h in enumerate(hdrs)
                                      if "no" in h or "code" in h), 0)
                    if name_i is None:
                        name_i = next((i for i, h in enumerate(hdrs)
                                       if "name" in h), 1)
                    for row in rows[1:]:
                        cells = [td.get_text(strip=True)
                                 for td in row.find_all(["td", "th"])]
                        if len(cells) <= max(num_i, name_i):
                            continue
                        code = cells[num_i].strip()
                        name = cells[name_i].strip()
                        if code.isdigit() and name:
                            names[code] = name
                    if names:
                        break
        except Exception:
            pass

    # ── Fallback 2: Sharesansar ───────────────────────────────────
    if not names:
        try:
            r3 = SESSION.get("https://www.sharesansar.com/broker", timeout=TIMEOUT)
            if r3.status_code == 200:
                soup3 = BeautifulSoup(r3.text, "html.parser")
                for row in soup3.find_all("tr"):
                    cells = [td.get_text(strip=True)
                             for td in row.find_all(["td", "th"])]
                    if len(cells) >= 2 and cells[0].strip().isdigit():
                        names[cells[0].strip()] = cells[1].strip()
        except Exception:
            pass

    if names:
        _BROKER_NAMES_CACHE = names
        _BROKER_NAMES_TS    = now
    return _BROKER_NAMES_CACHE or {}


def broker_display(code: str, names: dict = None) -> str:
    if names is None:
        names = get_broker_names()
    name = names.get(str(code), "")
    return f"{code} — {name}" if name else str(code)


def get_52week_extremes(prices=None):
    """Returns stocks at/near 52-week highs and lows."""
    if prices is None:
        prices, _ = fetch_market_today("auto")
    highs, lows = [], []
    for p in prices:
        h52 = p.get("high_52w", 0)
        l52 = p.get("low_52w",  0)
        ltp = p.get("ltp", 0)
        if not ltp: continue
        if h52 and ltp >= h52 * 0.97:
            p["pct_from_high"] = round((ltp - h52) / h52 * 100, 2) if h52 else 0
            p["at_52h"] = True
            highs.append(p)
        if l52 and ltp <= l52 * 1.10:
            p["pct_from_low"]  = round((ltp - l52) / l52 * 100, 2) if l52 else 0
            p["at_52l"] = True
            lows.append(p)
    highs.sort(key=lambda x: x.get("pct_from_high", 0), reverse=True)
    lows.sort(key=lambda x: x.get("pct_from_low", 0))
    return {"highs": highs, "lows": lows}


def accumulate_broker_over_days(broker="", symbol="", days=10):
    """Legacy wrapper around accumulate_broker_range."""
    from datetime import datetime, timedelta
    to_date   = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=days*2)).strftime("%Y-%m-%d")
    return accumulate_broker_range(broker, symbol, from_date, to_date)


# ══════════════════════════════════════════════════════════════════
#  EPS / FUNDAMENTALS  (MeroLagani primary → Sharesansar → NepseAlpha)
# ══════════════════════════════════════════════════════════════════

def _merolagani_eps(symbol):
    """EPS from MeroLagani CompanyDetail — 4-method scan."""
    import re as _re
    try:
        sess = requests.Session()
        sess.headers.update({"User-Agent": _UA, "Accept": "text/html,*/*"})
        r = sess.get(f"https://merolagani.com/CompanyDetail.aspx?symbol={symbol.upper()}", timeout=TIMEOUT)
        if r.status_code != 200: return 0
        soup = BeautifulSoup(r.text, "html.parser")
        # Method 1: table rows
        for row in soup.find_all("tr"):
            cells = [td.get_text(strip=True) for td in row.find_all(["td","th"])]
            if len(cells) < 2: continue
            lbl = cells[0].lower().strip().rstrip(":")
            if any(x in lbl for x in ["eps","earning per share","earnings per share","basic eps"]):
                for vc in cells[1:]:
                    n = _sf(vc.replace(",","").replace("Rs","").replace("रु","").strip())
                    if n and abs(n) < 10000: return n
        # Method 2: key:value elements
        for el in soup.find_all(["li","div","span","p","td"]):
            t = el.get_text(" ", strip=True)
            if ":" not in t or len(t) > 120: continue
            k, _, v = t.partition(":")
            if any(x in k.lower() for x in ["eps","earning per"]):
                n = _sf(v.replace(",","").replace("Rs","").strip())
                if n and abs(n) < 10000: return n
        # Method 3: regex on full text
        full = soup.get_text(" ", strip=True)
        for pat in [r"Basic\s+EPS\s*:?\s*([-\d,\.]+)", r"\bEPS\s*:?\s*([-\d,\.]+)",
                    r"Earning(?:s)?\s+Per\s+Share\s*:?\s*([-\d,\.]+)"]:
            m = _re.search(pat, full, _re.IGNORECASE)
            if m:
                n = _sf(m.group(1))
                if n and abs(n) < 10000: return n
    except Exception: pass
    return 0


def _sharesansar_eps(symbol):
    """EPS fallback from Sharesansar."""
    import re as _re
    try:
        r = SESSION.get(f"https://www.sharesansar.com/company/{symbol.lower()}", timeout=TIMEOUT)
        if r.status_code != 200: return 0
        soup = BeautifulSoup(r.text, "html.parser")
        for row in soup.find_all("tr"):
            cells = [td.get_text(strip=True) for td in row.find_all(["td","th"])]
            if len(cells) < 2: continue
            if any(x in cells[0].lower() for x in ["eps","earning per"]):
                n = _sf(cells[1].replace(",",""))
                if n and abs(n) < 10000: return n
        m = _re.search(r"\bEPS\s*:?\s*([-\d,\.]+)", soup.get_text(" ",strip=True), _re.IGNORECASE)
        if m: return _sf(m.group(1))
    except Exception: pass
    return 0


def _nepsealpha_eps(symbol):
    """EPS last-resort from NepseAlpha API."""
    try:
        r = SESSION.get(f"https://api.nepsealpha.com/api/v1/fundamental?symbol={symbol.upper()}",
                        timeout=TIMEOUT, headers={"Accept":"application/json","Referer":"https://nepsealpha.com/"})
        if r.status_code != 200: return 0
        d = r.json()
        for key in ["eps","EPS","earningPerShare","earning_per_share","basicEPS","basic_eps"]:
            if d.get(key):
                n = _sf(str(d[key]))
                if n and abs(n) < 10000: return n
        if isinstance(d, list) and d:
            for key in ["eps","EPS","earningPerShare","basic_eps"]:
                if d[0].get(key):
                    n = _sf(str(d[0][key]))
                    if n and abs(n) < 10000: return n
    except Exception: pass
    return 0



def _parse_dividend_tables(soup):
    """
    Universal dividend table parser. Handles both MeroLagani and Sharesansar layouts.
    Returns list of {fiscal_year, cash_pct, bonus_pct, book_close, right_share,
                     announce_date, dist_date, bonus_list_date}
    """
    results = []

    for tbl in soup.find_all("table"):
        all_text = tbl.get_text(" ", strip=True).lower()
        if not any(kw in all_text for kw in ["dividend","bonus","fiscal","cash"]):
            continue
        rows = tbl.find_all("tr")
        if len(rows) < 2:
            continue

        header_row = rows[0]
        hdrs = [td.get_text(strip=True) for td in header_row.find_all(["th","td"])]
        if not hdrs:
            continue

        col = {}
        for i, h in enumerate(hdrs):
            hl = h.lower().strip()
            if ("fiscal" in hl or hl.startswith("fy") or hl == "year") and "book" not in hl:
                col["fy"] = i
            elif "cash" in hl:
                col["cash"] = i
            elif "bonus" in hl or "stock div" in hl:
                col["bonus"] = i
            elif "right" in hl:
                col["right"] = i
            elif "book" in hl or "closure" in hl or "close" in hl:
                col["book"] = i
            elif "announce" in hl:
                col["announce"] = i
            elif "dist" in hl or "distribution" in hl:
                col["dist"] = i
            elif "year" in hl and "book" not in hl and "fy" not in col:
                col["fy"] = i

        if "fy" not in col:
            continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td","th"])]
            if not cells:
                continue

            def cget(k, default=""):
                idx = col.get(k)
                if idx is None or idx >= len(cells): return default
                return cells[idx].strip()

            fy = cget("fy").strip()
            if not fy or fy.lower() in ("fiscal year","fy","year","","n/a","—","-"):
                continue

            cash_raw  = cget("cash").replace("%","").replace(",","").strip()
            bonus_raw = cget("bonus").replace("%","").replace(",","").strip()
            book      = re.sub(r"\s*\[.*?\]", "", cget("book")).strip()
            right     = cget("right","").strip()
            announce  = cget("announce","").strip()
            dist      = cget("dist","").strip()

            cash  = _sf(cash_raw)
            bonus = _sf(bonus_raw)

            if fy and (cash or bonus or book or right):
                results.append({
                    "fiscal_year":     fy,
                    "cash_pct":        round(cash,  4),
                    "bonus_pct":       round(bonus, 4),
                    "book_close":      book,
                    "right_share":     right,
                    "announce_date":   announce,
                    "dist_date":       dist,
                    "bonus_list_date": "",
                })

        if results:
            return results

    # ── Regex fallback on full page text ─────────────────────────────────
    text = soup.get_text("\n", strip=True)
    fy_pat = re.compile(
        r"((?:FY\s*)?\d{4}[/\-]\d{2,4}|\d{4}\/\d{2})",
        re.IGNORECASE
    )
    splits = fy_pat.split(text)
    seen   = set()
    for i in range(1, len(splits) - 1, 2):
        fy_lbl = splits[i].strip()
        if fy_lbl in seen:
            continue
        seen.add(fy_lbl)
        block   = splits[i + 1][:500] if i + 1 < len(splits) else ""
        cash_m  = re.search(r"(?:cash\s*(?:dividend)?[^\d]*)(\d+(?:\.\d+)?)", block, re.IGNORECASE)
        bonus_m = re.search(r"(?:bonus\s*(?:share)?[^\d]*)(\d+(?:\.\d+)?)",   block, re.IGNORECASE)
        book_m  = re.search(r"(\d{4}[-/]\d{2}[-/]\d{2}|\d{1,2}\s+\w{3,}\s+\d{4})", block)
        if fy_lbl and (cash_m or bonus_m or book_m):
            results.append({
                "fiscal_year":     fy_lbl,
                "cash_pct":        round(_sf(cash_m.group(1)),  4) if cash_m  else 0.0,
                "bonus_pct":       round(_sf(bonus_m.group(1)), 4) if bonus_m else 0.0,
                "book_close":      book_m.group(1).strip() if book_m else "",
                "right_share":     "",
                "announce_date":   "",
                "dist_date":       "",
                "bonus_list_date": "",
            })

    return results


def _merolagani_dividends(symbol):
    """
    PRIMARY dividend source: MeroLagani CompanyDetail.aspx Dividend tab.
    Strategy 1: POST UpdatePanel to trigger Dividend tab, then paginate through ALL pages.
    Strategy 2: Static HTML parse (fallback — only gets first page if loaded).
    MeroLagani shows 10 rows per page and has pager buttons for more.
    """
    try:
        sess = requests.Session()
        sess.headers.update({"User-Agent": _UA, "Accept": "text/html,*/*",
                              "Accept-Language": "en-US,en;q=0.9"})
        url = f"https://merolagani.com/CompanyDetail.aspx?symbol={symbol.upper()}"
        r   = sess.get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")

        hidden = {i["name"]: i.get("value","")
                  for i in soup.find_all("input", {"type":"hidden"}) if i.get("name")}

        def _post_dividend_tab(current_hidden, page_target=""):
            """POST UpdatePanel to get dividend tab content, optionally clicking a pager."""
            event_target = (page_target if page_target else
                            "ctl00$ContentPlaceHolder1$CompanyDetail1$btnDividendTab")
            post_data = {
                "__EVENTTARGET":   event_target,
                "__EVENTARGUMENT": "",
                "__VIEWSTATE":     current_hidden.get("__VIEWSTATE",""),
                "__VIEWSTATEGENERATOR": current_hidden.get("__VIEWSTATEGENERATOR",""),
                "__EVENTVALIDATION": current_hidden.get("__EVENTVALIDATION",""),
                "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnStockSymbol": symbol.upper(),
                "ctl00$ContentPlaceHolder1$CompanyDetail1$hdnActiveTabID": "navDividend",
                "__ASYNCPOST": "true",
                "ctl00$ScriptManager1": (
                    "ctl00$ContentPlaceHolder1$CompanyDetail1$tabPanel|" + event_target
                ),
            }
            hdrs2 = {
                "X-Requested-With": "XMLHttpRequest",
                "X-MicrosoftAjax":  "Delta=true",
                "Referer":          url,
                "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
            }
            return sess.post(url, data=post_data, headers=hdrs2, timeout=TIMEOUT)

        def _parse_delta(response_text):
            """Extract dividend records and new hidden fields from UpdatePanel delta."""
            divs    = []
            new_hidden = {}
            # Scan chunks split by | for table content
            for chunk in response_text.split("|"):
                if "<table" in chunk and any(
                    kw in chunk.lower() for kw in ["dividend","fiscal","bonus"]
                ):
                    chunk_soup = BeautifulSoup(chunk, "html.parser")
                    found = _parse_dividend_tables(chunk_soup)
                    if found:
                        divs = found
                        break
            # Also try full parse if chunk scan found nothing
            if not divs:
                full_soup = BeautifulSoup(response_text, "html.parser")
                divs = _parse_dividend_tables(full_soup)

            # Extract updated hidden fields from delta
            for chunk in response_text.split("|"):
                if "VIEWSTATE" in chunk:
                    vs_soup = BeautifulSoup(chunk, "html.parser")
                    for inp in vs_soup.find_all("input", {"type":"hidden"}):
                        if inp.get("name"):
                            new_hidden[inp["name"]] = inp.get("value","")
            return divs, new_hidden

        # Get first page of dividend tab
        r2 = _post_dividend_tab(hidden)
        if r2.status_code != 200:
            # Strategy 2: static HTML fallback
            return _parse_dividend_tables(soup)

        all_divs, updated_hidden = _parse_delta(r2.text)
        if not all_divs:
            return _parse_dividend_tables(soup)  # fallback to static

        if updated_hidden:
            hidden.update(updated_hidden)

        # Check for pager links to get subsequent pages
        # MeroLagani pager: <a> tags inside divDividend with page numbers
        # Pager event targets look like:
        # ctl00$ContentPlaceHolder1$CompanyDetail1$PagerControlDividend$btnPaging_N
        page_num = 2
        seen_divs = set(d["fiscal_year"] for d in all_divs)
        MAX_DIV_PAGES = 20  # safety cap — most companies have <10 pages

        while page_num <= MAX_DIV_PAGES:
            pager_target = (
                f"ctl00$ContentPlaceHolder1$CompanyDetail1"
                f"$PagerControlDividend$btnPaging_{page_num}"
            )
            try:
                rp = _post_dividend_tab(hidden, page_target=pager_target)
                if rp.status_code != 200:
                    break
                page_divs, new_h = _parse_delta(rp.text)
                if new_h:
                    hidden.update(new_h)
                if not page_divs:
                    break  # no more pages
                # Check if we got any new fiscal years
                new_entries = [d for d in page_divs if d["fiscal_year"] not in seen_divs]
                if not new_entries:
                    break  # same data — stop paginating
                for d in new_entries:
                    seen_divs.add(d["fiscal_year"])
                all_divs.extend(new_entries)
                page_num += 1
            except Exception:
                break

        return all_divs

    except Exception:
        pass
    return []


def _sharesansar_dividends(symbol):
    """
    Sharesansar DataTables AJAX endpoint for dividend history.
    Table ID on page: #myTableDiv  |  Tab anchor: #cdividend
    POST https://www.sharesansar.com/company/dividendhistory
    Response JSON: { "data": [ [...], ... ] }  OR  { "data": [ {...}, ... ] }

    DataTables column layout (verified from DOM inspection):
      col 0 = SN
      col 1 = Bonus Dividend (%)
      col 2 = Cash Dividend (%)       ← confirmed from _DT_CellIndex {column:2}
      col 3 = Total Dividend (%)
      col 4 = Announcement Date
      col 5 = Book Closure Date
      col 6 = Distribution Date
      col 7 = Bonus Listing Date
      col 8 = Fiscal Year
    """
    try:
        sess = requests.Session()
        sess.headers.update({"User-Agent": _UA, "Accept-Language": "en-US,en;q=0.9"})

        # Visit company page with #cdividend anchor to warm cookies and signal tab
        company_url = f"https://www.sharesansar.com/company/{symbol.lower()}"
        sess.get(company_url + "#cdividend", timeout=TIMEOUT,
                 headers={"Accept": "text/html,*/*"})

        # POST to DataTables AJAX endpoint — request all records at once
        r = sess.post(
            "https://www.sharesansar.com/company/dividendhistory",
            data={
                "symbol": symbol.upper(),
                "draw":   "1",
                "start":  "0",
                "length": "10000",   # large enough for any company's full history
            },
            headers={
                "Accept":           "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
                "Referer":          company_url + "#cdividend",
                "Origin":           "https://www.sharesansar.com",
            },
            timeout=TIMEOUT,
        )

        if r.status_code != 200:
            return []

        j = r.json()
        raw_rows = j.get("data", [])
        if not raw_rows:
            return []

        def striptags(s):
            return re.sub(r'<[^>]+>', '', str(s or "")).strip()

        def get_col(row, idx, key_names):
            """Get value from row whether it's a list or a dict."""
            if isinstance(row, dict):
                for k in key_names:
                    if k in row:
                        return striptags(str(row[k]))
                return ""
            # List/array format
            return striptags(row[idx]) if idx < len(row) else ""

        results = []
        for row in raw_rows:
            # Column indices match DataTables DOM inspection: col2=cash, col1=bonus
            bonus_s   = get_col(row, 1, ["bonus_dividend","bonus","bonus_share"])
            cash_s    = get_col(row, 2, ["cash_dividend","cash"])
            total_s   = get_col(row, 3, ["total_dividend","total"])
            announce  = get_col(row, 4, ["announcement_date","announce_date","announced"])
            book_raw  = get_col(row, 5, ["book_closure_date","book_close","book_closure"])
            dist_date = get_col(row, 6, ["distribution_date","dist_date"])
            bonus_lst = get_col(row, 7, ["bonus_listing_date","bonus_listing"])
            fiscal_yr = get_col(row, 8, ["fiscal_year","fy","year"])

            # Fallback: scan all cells for a fiscal year pattern
            if not fiscal_yr:
                items = row.values() if isinstance(row, dict) else row
                for cell in reversed(list(items)):
                    v = striptags(str(cell))
                    if v and re.search(r'\d{4}[/\-]\d{2}', v):
                        fiscal_yr = v; break

            bonus      = _sf(bonus_s.replace("%","").replace(",",""))
            cash       = _sf(cash_s.replace("%","").replace(",",""))
            book_close = re.sub(r'\s*\[.*?\]', '', book_raw).strip()

            # Include row if we have a fiscal year AND at least one dividend value
            # (bonus OR cash — don't skip rows where one is 0)
            if fiscal_yr and (bonus > 0 or cash > 0):
                results.append({
                    "fiscal_year":     fiscal_yr,
                    "bonus_pct":       round(bonus, 4),
                    "cash_pct":        round(cash,  4),
                    "right_share":     "",
                    "book_close":      book_close,
                    "announce_date":   announce,
                    "dist_date":       dist_date,
                    "bonus_list_date": bonus_lst,
                })

        if results:
            return results

    except Exception:
        pass

    # Fallback: static HTML scrape of the company page
    try:
        sess2 = requests.Session()
        sess2.headers.update({"User-Agent": _UA, "Accept": "text/html,*/*",
                               "Accept-Language": "en-US,en;q=0.9"})
        r2 = sess2.get(f"https://www.sharesansar.com/company/{symbol.lower()}",
                       timeout=TIMEOUT)
        if r2.status_code == 200:
            return _parse_dividend_tables(BeautifulSoup(r2.text, "html.parser"))
    except Exception:
        pass

    return []


def _fetch_dividends(symbol):
    """
    Unified dividend entry point.
    Priority:
    1. Sharesansar AJAX POST — fetches ALL historical records in one request (most complete)
    2. Sharesansar static HTML — company page first page (fallback if AJAX fails)
    3. MeroLagani — paginated UpdatePanel (final fallback)
    Returns (list_of_records, source_name_string).
    """
    # Primary: Sharesansar AJAX gets ALL records at once with length=1000
    divs = _sharesansar_dividends(symbol)
    if divs:
        return divs, "Sharesansar"
    # Final fallback: MeroLagani paginated
    divs = _merolagani_dividends(symbol)
    if divs:
        return divs, "MeroLagani"
    return [], ""


def merolagani_company_detail(symbol):
    """
    Fetch company fundamentals.
    EPS:       MeroLagani → Sharesansar → NepseAlpha
    Dividends: _fetch_dividends() — Sharesansar AJAX first, MeroLagani tab POST fallback
    """
    import re as _re
    result = {"symbol": symbol.upper()}

    # Company name from sector map
    try:
        sec_map = _load_sector_map()
        entry   = sec_map.get(symbol.upper(), {})
        result["company_name"] = entry.get("company_name","")
        result["sector"]       = entry.get("sector","")
    except Exception: pass

    # EPS chain
    eps = _merolagani_eps(symbol)
    if not eps: eps = _sharesansar_eps(symbol)
    if not eps: eps = _nepsealpha_eps(symbol)
    if eps: result["eps"] = eps

    # Other fundamentals from MeroLagani
    try:
        sess = requests.Session()
        sess.headers.update({"User-Agent":_UA,"Accept":"text/html,*/*"})
        r = sess.get(f"https://merolagani.com/CompanyDetail.aspx?symbol={symbol.upper()}", timeout=TIMEOUT)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            full = soup.get_text(" ", strip=True)
            kv   = [
                (r"Book\s*Value\s*:?\s*([\d,\.]+)",         "book_value"),
                (r"BVPS\s*:?\s*([\d,\.]+)",                  "book_value"),
                (r"P[/\.]E\s*(?:Ratio)?\s*:?\s*([\d,\.]+)", "pe"),
                (r"52\s*Week\s*High\s*:?\s*([\d,\.]+)",      "high_52w"),
                (r"52\s*Week\s*Low\s*:?\s*([\d,\.]+)",       "low_52w"),
                (r"Listed\s*Shares\s*:?\s*([\d,\.]+)",       "listed_shares"),
                (r"Market\s*Cap[^\d]*([\d,\.]+)",             "market_cap"),
            ]
            for pat, key in kv:
                if not result.get(key):
                    m = _re.search(pat, full, _re.IGNORECASE)
                    if m:
                        v = _sf(m.group(1))
                        # Convert market cap from raw rupees to billions
                        if key == "market_cap" and v > 1e8:
                            v = round(v / 1e9, 2)
                        result[key] = v
            for row in soup.find_all("tr"):
                cells = [td.get_text(strip=True) for td in row.find_all(["td","th"])]
                if len(cells) < 2: continue
                lbl = cells[0].lower().strip().rstrip(":")
                raw = cells[1].replace(",","").replace("Rs","").strip()
                # Extract first number (handles "35.18(FY:082-083, Q:2)" → 35.18)
                first_num_m = _re.search(r"[\d,]+\.?\d*", raw)
                num = _sf(first_num_m.group()) if first_num_m else 0
                if   "eps"        in lbl and not result.get("eps"):         result["eps"]         = num
                elif "book value" in lbl and not result.get("book_value"):  result["book_value"]  = num
                elif "p/e"        in lbl and not result.get("pe"):          result["pe"]          = num
                elif "market cap" in lbl and not result.get("market_cap"):  result["market_cap"]  = round(num/1e9, 2) if num > 1e8 else round(num, 2)
                elif "52 week hi" in lbl and not result.get("high_52w"):    result["high_52w"]    = num
                elif "52 week lo" in lbl and not result.get("low_52w"):     result["low_52w"]     = num
                elif "% dividend" in lbl and not result.get("dividend_pct"):result["dividend_pct"]= num
                elif "listed shar" in lbl and not result.get("listed_shares"): result["listed_shares"] = _si(raw)
    except Exception: pass

    # Dividends — single clean function, Sharesansar primary → MeroLagani fallback
    try:
        divs, src = _fetch_dividends(symbol)
        result["dividends"] = divs
        result["dividend_source"] = src
    except Exception:
        result["dividends"] = []

    return result


# ================================================================
#  NEPALSTOCK.COM -- Official JSON API
#  Full implementation in ns_fetcher.py (wasmtime + css.wasm)
# ================================================================

from ns_fetcher import (
    nepalstock_floorsheet,
    nepalstock_floorsheet_available,
    ns_get_token          as _ns_get_token,
    ns_headers            as _ns_hdrs,
    fetch_page            as _ns_fetch_page_sync,
    parse_rows            as _ns_parse_rows,
    _drop_token           as _ns_drop_token,
    _token_via_wasm       as _ns_token_via_wasm,
    NS_BASE               as _NS_BASE,
    NS_PAGE_SZ            as _NS_PAGE_SZ,
)

# Backwards compatibility aliases
_ns_derive_token    = _ns_drop_token
_ns_auth_headers    = _ns_hdrs
_ns_fetch_all_async = lambda date_str, total_pages, size=_NS_PAGE_SZ: []
