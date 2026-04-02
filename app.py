"""
NEPSE Analyzer — Complete Rebuild v2
Dark theme, real-time scraping, broker accumulation, technical analysis
"""
import json, math, os, sys
import numpy as np
from flask import Flask, request, Response, send_from_directory
from datetime import datetime, timedelta
from collections import deque

app = Flask(__name__, static_folder="static")

# ── Persistent Broker Cache ─────────────────────────────────────────────────
from cache import (
    cache_get, cache_set, cache_has, cache_stats,
    cache_clear, cache_clear_log,
    last_6_months_trading_days, uncached_days, trading_days_range,
    bg_start, bg_cancel, bg_state,
)

# ── Data Fetch Log (in-memory, last 500 entries) ────────────────────────────
_FETCH_LOG = deque(maxlen=500)
_IMPORT_LOG = []

def log_fetch(action, detail="", records=None, source=""):
    """Append a timestamped entry to the fetch log."""
    entry = {
        "ts":      datetime.now().strftime("%H:%M:%S"),
        "action":  action,
        "detail":  detail,
        "source":  source,
    }
    if records is not None:
        entry["records"] = records
    _FETCH_LOG.appendleft(entry)


@app.errorhandler(Exception)
def handle_exception(e):
    """Return JSON for ALL errors — never return HTML error pages."""
    import traceback
    return jresp({"error": str(e), "trace": traceback.format_exc()[-500:]}), 500

@app.errorhandler(404)
def handle_404(e):
    return jresp({"error": "Not found"}), 404

from data_fetcher import (
    fetch_stock, fetch_multiple, check_all_sources,
    invalidate_market_cache, invalidate_history_cache,
    sharesansar_today, merolagani_today, fetch_market_today,
    search_symbols, get_all_symbols,
    sharesansar_floorsheet, analyze_brokers,
    sharesansar_financials,
    merolagani_floorsheet_search, merolagani_floorsheet_by_date,
    analyze_broker_activity, accumulate_broker_range,
    get_broker_names, broker_display,
    get_52week_extremes,
    merolagani_company_detail,
    nepalstock_floorsheet, nepalstock_floorsheet_available,
)
from analysis import analyze

# ── Sector map ────────────────────────────────────────────────────────────────
ALL_SYMBOLS = {
    "Commercial Banks":  ["NABIL","NICA","GBIME","EBL","SANIMA","KBL","MBL","NMB","PRVU","SBL",
                          "SRBL","BOKL","CZBIL","HBL","PCBL","SCB","CBL","ADBL","JBNL","LBBL",
                          "NBB","NIMB","CCBL","SBI","MEGA","SHINE"],
    "Development Banks": ["MNBBL","KSBBL","SADBL","CORBL","EDBL","GBBL","JBNL","LBBL","MLBL",
                          "NABBC","SINDU","SAPDBL","MIDBL"],
    "Finance Companies": ["ICFC","GUFL","MFIL","AFCL","PFL","GFCL","CFCL","JFL","SFL","NCHL",
                          "NFCL","RLFL","SMFL","MKCL"],
    "Life Insurance":    ["NLICL","LICN","NLIC","ALICL","ULIF","JLIC","GLICL","PMHIL","PLIC",
                          "SLICL","SNLI","SRLI"],
    "Non-Life Insurance":["SICL","PRIN","SGIC","NIL","PIL","RBCL","PICL","ILBS","HGI","IGIL",
                          "NLG","SIC","NICL","SALICO","SPIL"],
    "Hydropower":        ["CHCL","NHPC","UPPER","RADHI","API","RURU","KPCL","AKPL","PPCL","UMRH",
                          "BARUN","NHDL","KBSH","MHNL","GHL","SHPC","RHPL","DOLTI","BEDC","AHPC",
                          "AKJCL","AHL","PHCL","HDHPC","SAHAS","RIDI","NGPL","USHEC","CKHL",
                          "PMHPL","BPCL","HURJA","MKJC","NWCL","HPPL","SPDL","MAKAR","GLH",
                          "KPCL","GILB","UPCL","HRL"],
    "Microfinance":      ["DDBL","SWBBL","SMFDB","RMDC","NMMB","CBBL","MERO","FOWAD","MLBBL",
                          "GBLBS","SAMAJ","ACLBSL","MLBBL","RSDC","SKBBL","SDBL","USLB","WNLB"],
    "Hotels & Tourism":  ["SHL","OHL","TRH","YHL"],
    "Manufacturing":     ["NTC","SHIVM","BGWT","UNL","OMHL","HDL","NBBL","NRIC","BPCL","GILB"],
    "Others":            ["BBC","NLO","NRIC","HIDCL","NIFRA","CEDB","API","CZBIL"],
}
SYM_SEC = {}
for _sec, _syms in ALL_SYMBOLS.items():
    for _s in _syms: SYM_SEC[_s] = _sec

def jresp(obj):
    def _c(o):
        if isinstance(o, dict):         return {k: _c(v) for k,v in o.items()}
        if isinstance(o, (list,tuple)): return [_c(i) for i in o]
        if isinstance(o, np.integer):   return int(o)
        if isinstance(o, np.floating):  return float(o)
        if isinstance(o, np.bool_):     return bool(o)
        if isinstance(o, np.ndarray):   return o.tolist()
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)): return None
        return o
    return Response(json.dumps(_c(obj)), mimetype="application/json")

# ═══════════════════════════════════════════════════════════════════════════════
#  STATIC / INDEX
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory("static", filename)

# ═══════════════════════════════════════════════════════════════════════════════
#  API ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/ping")
def api_ping():
    """Instant health check — returns immediately, no external calls.
    Used by the splash page to detect when Flask is ready."""
    return jresp({"ok": True, "ts": datetime.now().isoformat()})



def api_sources_status():
    status = check_all_sources()
    return jresp({
        "sources": [
            {"id":"auto",        "name":"Auto (best available)", "available": any(status.values()), "icon":"🤖"},
            {"id":"sharesansar", "name":"Sharesansar.com",        "available": status.get("sharesansar",False), "icon":"🟢"},
            {"id":"merolagani",  "name":"MeroLagani.com",         "available": status.get("merolagani",False),  "icon":"🟡"},
        ],
        "any_live":   any(status.values()),
        "checked_at": datetime.now().isoformat(),
    })

@app.route("/api/symbols")
def api_symbols():
    from data_fetcher import _load_sector_map
    q       = request.args.get("q","").strip()
    all_sym = get_all_symbols()
    results = [s for s in all_sym if q.upper() in s["symbol"]][:20] if q else all_sym[:100]
    sec_map = _load_sector_map()
    for row in results:
        sym   = row["symbol"]
        entry = sec_map.get(sym, {})
        row["sector"]       = entry.get("sector","") or SYM_SEC.get(sym,"Other")
        row["company_name"] = entry.get("company_name","") or row.get("company_name","")
    return jresp({"symbols": results, "count": len(results)})


@app.route("/api/company/name")
def api_company_name():
    from data_fetcher import get_company_name
    symbol = request.args.get("symbol","").upper().strip()
    return jresp({"symbol": symbol, "name": get_company_name(symbol)})

@app.route("/api/analyze")
def api_analyze():
    symbol    = request.args.get("symbol","NABIL").upper().strip()
    source    = request.args.get("source","auto")
    from_date = request.args.get("from_date","")
    to_date   = request.args.get("to_date","")
    if not from_date: from_date = (datetime.now()-timedelta(days=365)).strftime("%Y-%m-%d")
    if not to_date:   to_date   = datetime.now().strftime("%Y-%m-%d")

    force_refresh = request.args.get("refresh","0") == "1"
    raw = fetch_stock(symbol, source, from_date, to_date, force_refresh=force_refresh)

    # Get company detail: ML has EPS/PE/BV (confirmed working).
    # SS financials used for live price overlay only (no EPS/PE/BV on SS).
    detail = {}
    try:
        detail = merolagani_company_detail(symbol)
    except Exception:
        pass
    # If ML detail missing live price data, overlay from SS financials
    if not detail.get("ltp"):
        try:
            ss_fin = sharesansar_financials(symbol)
            for k in ["ltp","open","high","low","close","prev_close","volume",
                      "turnover","change_pct","high_52w","low_52w","vwap"]:
                if ss_fin.get(k) and not detail.get(k):
                    detail[k] = ss_fin[k]
        except Exception:
            pass

    if not raw.get("history"):
        log_fetch("Analyze", f"{symbol} — no history", records=0, source=raw.get("source",""))
        return jresp({
            "error":        raw.get("error", f"No history data for {symbol}"),
            "symbol":       symbol,
            "fundamentals": raw.get("fundamentals",{}),
            "company_detail": detail,
        })

    result = analyze(raw["history"], raw["fundamentals"])

    if result.get("error"):
        return jresp({
            "error":        result["error"],
            "symbol":       symbol,
            "fundamentals": raw.get("fundamentals",{}),
            "company_detail": detail,
        })

    from data_fetcher import _load_sector_map
    sec_map      = _load_sector_map()
    sec_entry    = sec_map.get(symbol, {})
    company_name = (detail.get("company_name","") or
                    sec_entry.get("company_name","") or
                    raw["fundamentals"].get("company_name",""))
    sector       = (sec_entry.get("sector","") or
                    SYM_SEC.get(symbol,"Other"))

    result["symbol"]         = symbol
    result["source"]         = raw["source"]
    result["is_real"]        = raw.get("is_real", False)
    result["fundamentals"]   = raw["fundamentals"]
    result["history"]        = raw["history"]
    result["fetched_at"]     = raw["fetched_at"]
    result["company_detail"] = detail
    result["company_name"]   = company_name
    result["sector"]         = sector
    # Backfill company_name into fundamentals so JS can read it
    result["fundamentals"]["company_name"] = company_name
    log_fetch("Analyze", f"{symbol} ({company_name or symbol}) — {len(raw['history'])} bars", records=len(raw["history"]), source=raw["source"])
    return jresp(result)

@app.route("/api/market")
def api_market():
    source        = request.args.get("source","auto")
    force_refresh = request.args.get("refresh","0") == "1"
    prices, src_used = fetch_market_today(source, force_refresh=force_refresh)
    is_live = bool(prices)

    # Market closed or sources down — try SS which shows last trading day even on closed days
    if not prices:
        try:
            from data_fetcher import sharesansar_today, merolagani_today
            # SS often has last-day data even on weekends/holidays
            prices = sharesansar_today()
            if prices:
                src_used = "ShareSansar (last trading day)"
                from data_fetcher import _enrich_sector_name, set_cached_market
                _enrich_sector_name(prices)
                set_cached_market(source, prices, src_used)
            else:
                prices = merolagani_today()
                if prices:
                    src_used = "MeroLagani (last trading day)"
                    from data_fetcher import _enrich_sector_name, set_cached_market
                    _enrich_sector_name(prices)
                    set_cached_market(source, prices, src_used)
        except Exception:
            pass

    if not prices:
        log_fetch("Market Data", "No data — markets may be closed", records=0, source="auto")
        return jresp({"error":"Market data unavailable. Markets may be closed.","prices":[],"source":"None","is_live":False})
    for p in prices:
        if not p.get("sector"): p["sector"] = SYM_SEC.get(p.get("symbol",""),"Other")
    # Compute derived fields
    for p in prices:
        ltp      = p.get("ltp",0) or 0
        high_52w = p.get("high_52w",0) or 0
        low_52w  = p.get("low_52w",0) or 0
        if high_52w and ltp:
            p["pct_from_52h"] = round((ltp - high_52w)/high_52w*100, 2)
            p["at_52h"]       = ltp >= high_52w * 0.90  # within 10%
        else:
            p["pct_from_52h"] = 0; p["at_52h"] = False
        if low_52w and ltp:
            p["pct_from_52l"] = round((ltp - low_52w)/low_52w*100, 2)
            p["at_52l"]       = ltp <= low_52w * 1.10   # within 10%
        else:
            p["pct_from_52l"] = 0; p["at_52l"] = False
    log_fetch("Market Data", f"{len(prices)} stocks loaded", records=len(prices), source=src_used)
    # Determine trade date from scraped data or today
    trade_date = ''
    if prices:
        trade_date = prices[0].get('trade_date','') or prices[0].get('date','') or ''
    if not trade_date:
        trade_date = datetime.now().strftime('%d %b %Y')
    return jresp({"prices": prices, "source": src_used, "count": len(prices), "trade_date": trade_date, "is_live": is_live})

@app.route("/api/market/52week")
def api_52week():
    source = request.args.get("source","auto")
    prices, src_used = fetch_market_today(source)
    data = get_52week_extremes(prices if prices else None)
    data["source"] = src_used
    return jresp(data)

@app.route("/api/screener")
def api_screener():
    source      = request.args.get("source","auto")
    sectors_raw = request.args.get("sector","")
    signal      = request.args.get("signal","")
    min_price   = float(request.args.get("min_price","0")    or 0)
    max_price   = float(request.args.get("max_price","99999") or 99999)
    min_volume  = int(  request.args.get("min_volume","0")   or 0)
    sectors     = [s.strip() for s in sectors_raw.split(",") if s.strip()]

    prices, src_used = fetch_market_today(source)
    if not prices:
        return jresp({"error":"Market data unavailable","results":[]})

    candidates = []
    for p in prices:
        sec = SYM_SEC.get(p.get("symbol",""),"Other")
        p["sector"] = sec
        if sectors and sec not in sectors: continue
        ltp = p.get("ltp",0)
        vol = p.get("volume",0)
        if ltp < min_price or ltp > max_price: continue
        if vol < min_volume: continue
        candidates.append(p)

    from concurrent.futures import ThreadPoolExecutor, as_completed
    from data_fetcher import merolagani_history, _merolagani_eps, _sharesansar_eps

    def _enrich(p):
        """Fetch EPS + optionally run TA for signal."""
        sym = p.get("symbol","")
        # Always fetch EPS
        eps = _merolagani_eps(sym)
        if not eps:
            eps = _sharesansar_eps(sym)
        p["eps"] = eps
        # Compute P/E if we have both
        ltp = p.get("ltp",0)
        if eps and ltp:
            p["pe"] = round(ltp/eps, 2)

        if signal:
            try:
                hist = merolagani_history(sym,
                    (datetime.now()-timedelta(days=180)).strftime("%Y-%m-%d"),
                    datetime.now().strftime("%Y-%m-%d"))
                if len(hist) >= 20:
                    a = analyze(hist, p)
                    if not a.get("error"):
                        p["signal"] = a.get("projection","HOLD")
                        p["score"]  = a.get("score", 50)
                        p["rsi"]    = a["indicators"]["current"].get("rsi14", 0)
                        return p
            except Exception:
                pass
        # Momentum signal fallback
        chg = p.get("change_pct",0)
        if chg > 2:    p["signal"] = "BUY"
        elif chg < -2: p["signal"] = "SELL"
        else:          p["signal"] = "HOLD"
        p["score"] = 50 + chg * 2
        return p

    import os
    results = []
    limit = 30 if signal else 100
    workers = os.cpu_count() or 4
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_enrich, p): p for p in candidates[:limit]}
        for fut in as_completed(futs):
            try:
                r = fut.result()
                if r:
                    if not signal or signal.upper() in r.get("signal",""):
                        results.append(r)
            except Exception:
                pass

    results.sort(key=lambda x: x.get("turnover",0), reverse=True)
    return jresp({"results": results[:100], "count": len(results), "source": src_used})

# ─── Broker Endpoints ─────────────────────────────────────────────────────────

# ══════════════════════════════════════════════════════════════════
#  BROKER ROUTES — all return JSON, never HTML
# ══════════════════════════════════════════════════════════════════

@app.route("/api/broker/names")
def api_broker_names():
    try:
        names  = get_broker_names()
        result = [{"code": k, "name": v} for k, v in names.items() if "_" not in k]
        result.sort(key=lambda x: x["code"].zfill(5))
        return jresp({"brokers": result, "count": len(result)})
    except Exception as e:
        return jresp({"error": str(e), "brokers": [], "count": 0})


@app.route("/api/broker/floorsheet")
def api_broker_floorsheet():
    """
    Single-day full floorsheet → per-broker buy/sell summary.
    Source priority: Cache → NepalStock JSON API → MeroLagani HTML
    """
    date_str  = request.args.get("date",   datetime.now().strftime("%Y-%m-%d"))
    symbol    = request.args.get("symbol", "")
    buyer_f   = request.args.get("buyer",  "")
    seller_f  = request.args.get("seller", "")

    try:
        src  = "?"
        rows = []

        # 1. Cache hit
        cached = cache_get(date_str, symbol)
        if cached is not None:
            rows = cached
            src  = "Cache"
            log_fetch("Cache HIT", f"{date_str} — {len(rows)} rows", records=len(rows), source="Cache")

        source_pref = request.args.get("source", "")  # nepse / ml / ss / cache / ""=auto

        # 2. NEPSE API
        if not rows and source_pref not in ("ml", "ss", "cache"):
            try:
                rows = nepalstock_floorsheet(date_str, symbol=symbol)
                src  = "nepse-data-api"
            except Exception:
                pass

        # 3. MeroLagani
        if not rows and source_pref not in ("nepse", "ss", "cache"):
            try:
                rows = merolagani_floorsheet_by_date(
                    symbol=symbol, buyer="", seller="", date_str=date_str)
                if rows: src = "MeroLagani"
            except Exception:
                pass

        # 4. ShareSansar
        if not rows and source_pref not in ("nepse", "ml", "cache"):
            try:
                rows = sharesansar_floorsheet(symbol, max_rows=50000) if symbol else []
                if rows: src = "ShareSansar"
            except Exception:
                pass

        # Cache for future use
        today = datetime.now().strftime("%Y-%m-%d")
        if rows and date_str < today and src != "Cache":
            cache_set(date_str, symbol, rows)

        # Apply broker filters locally
        if buyer_f:
            rows = [r for r in rows if str(r.get("buyer","")).strip() == buyer_f]
        elif seller_f:
            rows = [r for r in rows if str(r.get("seller","")).strip() == seller_f]

        if not rows:
            return jresp({
                "rows": [], "summary": {
                    "broker_list": [], "broker_symbols": {},
                    "symbol_brokers": {}, "total_rows": 0, "stats": {}
                },
                "date": date_str, "total": 0, "source": src,
                "message": "No data. Market may have been closed on this date."
            })

        summary      = analyze_broker_activity(rows)
        broker_names = get_broker_names()

        for item in summary.get("broker_list", []):
            code = str(item["broker"])
            item["name"]    = broker_names.get(code, "")
            item["display"] = f"{code} — {item['name']}" if item["name"] else code

        return jresp({
            "rows":         rows[:500],
            "summary":      summary,
            "date":         date_str,
            "total":        len(rows),
            "source":       src,
            "broker_names": broker_names,
        })

    except Exception as e:
        return jresp({
            "error": str(e),
            "rows": [], "summary": {}, "date": date_str, "total": 0
        })




@app.route("/api/broker/analyze")
def api_broker_analyze():
    """Single symbol, single day -- full broker breakdown with trade list."""
    symbol   = request.args.get("symbol","").upper().strip()
    date_str = request.args.get("date", datetime.now().strftime("%Y-%m-%d"))
    if not symbol:
        return jresp({"error": "symbol required"})
    try:
        # Get rows from cache or nepse-data-api
        rows = []
        cached = cache_get(date_str, symbol)
        if cached:
            rows = cached
        if not rows:
            try:
                rows = nepalstock_floorsheet(date_str, symbol=symbol)
            except Exception:
                pass
        if not rows:
            rows = merolagani_floorsheet_by_date(symbol=symbol, date_str=date_str)

        if not rows:
            return jresp({"error": f"No data for {symbol} on {date_str}", "rows":[], "broker_summary":[]})

        # Broker summary
        brokers = {}
        total_qty = total_amt = 0
        for r in rows:
            b = str(r.get("buyer",""))
            s = str(r.get("seller",""))
            q = int(r.get("qty",0) or 0)
            a = float(r.get("amount",0) or 0)
            rate = float(r.get("rate",0) or 0)
            total_qty += q; total_amt += a
            for code, side in [(b,"buy"),(s,"sell")]:
                if not code: continue
                if code not in brokers:
                    brokers[code] = {"broker":code,"buy_qty":0,"sell_qty":0,"buy_amt":0,"sell_amt":0}
                brokers[code][f"{side}_qty"] += q
                brokers[code][f"{side}_amt"] += a

        summary = []
        for code, d in brokers.items():
            net = d["buy_qty"] - d["sell_qty"]
            d["net_qty"] = net
            d["pattern"] = "ACCUMULATION" if net>0 else "DISTRIBUTION" if net<0 else "NEUTRAL"
            summary.append(d)
        summary.sort(key=lambda x: abs(x["net_qty"]), reverse=True)

        broker_names = get_broker_names()
        avg_rate = total_amt/total_qty if total_qty else 0

        return jresp({
            "rows": rows[:2000],
            "broker_summary": summary,
            "broker_names": broker_names,
            "total_rows": len(rows),
            "total_qty": total_qty,
            "total_amt": total_amt,
            "avg_rate": avg_rate,
            "symbol": symbol,
            "date": date_str,
        })
    except Exception as e:
        return jresp({"error": str(e), "rows":[], "broker_summary":[]})

@app.route("/api/broker/accumulation")
def api_broker_accumulation():
    """
    Date-range broker accumulation.

    ARCHITECTURE (proven correct):
    For each date:
      - Fetch ALL floorsheet rows for that date (no broker filter)
      - analyze_broker_activity counts buyer side and seller side independently
      - Each row processed ONCE: buyer gets buy credit, seller gets sell credit
    
    The total row count is passed through as-is so the UI can display it.
    Duplicate rows are prevented by the (txn_no, buyer, seller) dedup set.
    """
    from_date = request.args.get("from_date", (datetime.now()-timedelta(days=14)).strftime("%Y-%m-%d"))
    to_date   = request.args.get("to_date",   datetime.now().strftime("%Y-%m-%d"))
    symbol    = request.args.get("symbol", "").strip()
    broker    = request.args.get("broker", "").strip()

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import os

    # Build list of NEPSE trading days (Sun=6,Mon=0...Thu=4, skip Fri=4,Sat=5)
    start_dt = datetime.strptime(from_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(to_date,   "%Y-%m-%d")
    dates    = []
    d = start_dt
    while d <= end_dt:
        if d.weekday() not in (4, 5):   # skip Friday and Saturday
            dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    try:
        all_rows  = []
        seen_keys = set()

        def _fetch_date(ds):
            # ── 1. Serve from cache if available ─────────────────
            cached = cache_get(ds, symbol)
            if cached is not None:
                log_fetch("Cache HIT",
                          f"{ds} sym={symbol or '(all)'} — {len(cached)} rows",
                          records=len(cached), source="Cache")
                for r in cached: r["date"] = ds
                return cached

            # ── 2. Live fetch: NepalStock JSON API first (fast) ───
            rows = []
            src  = "?"
            try:
                rows = nepalstock_floorsheet(ds, symbol=symbol)
                src  = "NepalStock"
            except Exception:
                pass

            # ── 3. Fallback: MeroLagani HTML scraping ─────────────
            if not rows:
                try:
                    rows = merolagani_floorsheet_by_date(
                        symbol=symbol, buyer="", seller="", date_str=ds)
                    src = "MeroLagani"
                except Exception:
                    pass

            for r in rows:
                r["date"] = ds

            # ── 4. Cache result (not today — live data still changing)
            today = datetime.now().strftime("%Y-%m-%d")
            if rows and ds < today:
                cache_set(ds, symbol, rows)
                log_fetch("Cache STORE",
                          f"{ds} sym={symbol or '(all)'} — {len(rows)} rows [{src}]",
                          records=len(rows), source=src)
            return rows

        # Use all available CPU cores — user explicitly wants max performance
        workers = os.cpu_count() or 4
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_fetch_date, ds): ds for ds in dates}  # ALL dates, no cap
            for fut in as_completed(futs):
                try:
                    for r in fut.result():
                        key = (str(r.get("txn_no","")).strip(),
                               str(r.get("buyer","")).strip(),
                               str(r.get("seller","")).strip())
                        if key[0] and key in seen_keys:
                            continue
                        if key[0]:
                            seen_keys.add(key)
                        all_rows.append(r)
                except Exception:
                    pass

        # Filter to specific broker if requested
        if broker:
            all_rows = [r for r in all_rows
                        if str(r.get("buyer","")).strip()  == broker
                        or str(r.get("seller","")).strip() == broker]

        if not all_rows:
            return jresp({
                "broker_list": [], "broker_symbols": {},
                "total_rows": 0, "from_date": from_date, "to_date": to_date,
                "message": "No data. Ensure dates are NEPSE trading days (Sun–Thu)."
            })

        summary      = analyze_broker_activity(all_rows)
        broker_names = get_broker_names()

        for item in summary.get("broker_list", []):
            code = str(item["broker"])
            item["name"]    = broker_names.get(code, "")
            item["display"] = f"{code} — {item['name']}" if item["name"] else code
            bq, sq = item.get("buy_qty", 0), item.get("sell_qty", 0)
            item["pattern"] = (
                "STRONG ACCUMULATION" if bq > 0 and sq == 0 else
                "ACCUMULATION"        if bq > sq * 2         else
                "STRONG DISTRIBUTION" if sq > 0 and bq == 0 else
                "DISTRIBUTION"        if sq > bq * 2         else "MIXED"
            )

        log_fetch("Broker Accum.", f"{from_date}→{to_date} — {len(all_rows)} floorsheet rows", records=len(all_rows), source="MeroLagani")
        return jresp({
            "broker_list":    summary.get("broker_list", []),
            "broker_symbols": summary.get("broker_symbols", {}),
            "total_rows":     len(all_rows),
            "unique_rows":    len(seen_keys),
            "from_date":      from_date,
            "to_date":        to_date,
            "broker_names":   broker_names,
        })

    except Exception as e:
        return jresp({
            "error": str(e),
            "broker_list": [], "broker_symbols": {},
            "total_rows": 0, "from_date": from_date, "to_date": to_date,
        })


@app.route("/api/broker/daily")
def api_broker_daily():
    """Single broker day-by-day track over a date range."""
    broker    = request.args.get("broker", "").strip()
    symbol    = request.args.get("symbol", "")
    from_date = request.args.get("from_date", (datetime.now()-timedelta(days=30)).strftime("%Y-%m-%d"))
    to_date   = request.args.get("to_date",   datetime.now().strftime("%Y-%m-%d"))

    if not broker:
        return jresp({"error": "broker parameter required"})

    try:
        data         = accumulate_broker_range(broker, symbol, from_date, to_date)
        broker_names = get_broker_names()
        name         = broker_names.get(str(broker), "")

        pattern = "NEUTRAL"
        if data:
            net_days = [d["net_qty"] for d in data]
            pos_days = sum(1 for x in net_days if x > 0)
            neg_days = sum(1 for x in net_days if x < 0)
            n        = len(net_days)
            if   pos_days >= n * 0.8 and pos_days >= 3: pattern = "CONTINUOUS ACCUMULATION"
            elif pos_days >= n * 0.6:                    pattern = "MODERATE ACCUMULATION"
            elif neg_days >= n * 0.8 and neg_days >= 3: pattern = "CONTINUOUS DISTRIBUTION"
            elif neg_days >= n * 0.6:                    pattern = "MODERATE DISTRIBUTION"

        return jresp({
            "broker":     broker,
            "name":       name,
            "display":    f"{broker} — {name}" if name else broker,
            "symbol":     symbol or "All",
            "daily":      data,
            "total_buy":  sum(d["buy_qty"]  for d in data),
            "total_sell": sum(d["sell_qty"] for d in data),
            "net_held":   sum(d["net_qty"]  for d in data),
            "pattern":    pattern,
            "from_date":  from_date,
            "to_date":    to_date,
        })

    except Exception as e:
        return jresp({
            "error": str(e), "broker": broker, "daily": [],
            "total_buy": 0, "total_sell": 0, "net_held": 0,
            "pattern": "ERROR", "from_date": from_date, "to_date": to_date,
        })


# ═══════════════════════════════════════════════════════════════════════════════
#  PORTFOLIO ROUTES
# ═══════════════════════════════════════════════════════════════════════════════
from portfolio import (
    add_transaction, get_transactions, delete_transaction, update_transaction,
    list_profiles, create_profile, delete_profile, switch_profile,
    get_holdings, get_portfolio_summary, calc_transaction_costs, calc_capital_gains_tax,
    add_watchlist, get_watchlist, remove_watchlist,
    add_cash, get_cash_balance,
    export_portfolio, import_portfolio,
    get_fee_settings, update_fee_settings,
    clear_all_transactions, _make_import_hash,
    DB_PATH,
)

@app.route("/api/portfolio/profiles", methods=["GET"])
def api_list_profiles():
    return jresp({"profiles": list_profiles()})

@app.route("/api/portfolio/profiles", methods=["POST"])
def api_create_profile():
    data = request.get_json(force=True) or {}
    name = data.get("name", "").strip()
    if not name:
        return jresp({"error": "name required"}), 400
    if name in list_profiles():
        return jresp({"error": f"Profile '{name}' already exists"}), 400
    try:
        path = create_profile(name)
        return jresp({"success": True, "name": name, "path": path})
    except Exception as e:
        return jresp({"error": str(e)}), 400

@app.route("/api/portfolio/profiles/<name>", methods=["DELETE"])
def api_delete_profile(name):
    if not delete_profile(name):
        return jresp({"error": f"Cannot delete '{name}'"}), 400
    return jresp({"success": True})

@app.route("/api/portfolio/profiles/switch", methods=["POST"])
def api_switch_profile():
    data = request.get_json(force=True) or {}
    name = data.get("name", "Default").strip()
    try:
        path = switch_profile(name)
        log_fetch("Profile Switch", f"Active: {name}", source="Portfolio")
        return jresp({"success": True, "name": name, "path": path})
    except Exception as e:
        return jresp({"error": str(e)}), 400

@app.route("/api/portfolio/calc_fees")
def api_calc_fees():
    qty      = float(request.args.get("qty", 0) or 0)
    rate     = float(request.args.get("rate", 0) or 0)
    tx_type  = request.args.get("type", "BUY").upper()
    incl_dp  = request.args.get("dp", "1") == "1"
    if qty <= 0 or rate <= 0:
        return jresp({"error": "qty and rate required"})
    value = qty * rate
    costs = calc_transaction_costs(value, tx_type, incl_dp)
    costs["quantity"] = qty
    costs["rate"]     = rate
    return jresp(costs)

@app.route("/api/portfolio/transactions", methods=["GET"])
def api_get_transactions():
    symbol    = request.args.get("symbol", "")
    tx_type   = request.args.get("type", "")
    from_date = request.args.get("from_date", "")
    to_date   = request.args.get("to_date", "")
    rows = get_transactions(symbol, tx_type, from_date, to_date)
    return jresp({"transactions": rows, "count": len(rows)})

@app.route("/api/portfolio/transactions", methods=["POST"])
def api_add_transaction():
    data = request.get_json(force=True) or {}
    required = ["symbol", "type", "quantity", "date"]
    missing  = [k for k in required if not data.get(k)]
    # rate=0 is valid for BONUS (free shares) — validate separately
    tx_type_check = (data.get("type") or "").upper()
    if tx_type_check in ("BUY", "SELL", "RIGHT"):
        if data.get("rate") is None or str(data.get("rate","")).strip() == "":
            missing.append("rate")
    if missing:
        return jresp({"error": f"Missing: {', '.join(missing)}"}), 400
    try:
        raw_qty  = float(data["quantity"])
        tx_type  = data["type"].upper()
        # If user enters a negative qty, treat it as a SELL
        if raw_qty < 0:
            tx_type = "SELL"
        result = add_transaction(
            symbol       = data["symbol"],
            tx_type      = tx_type,
            quantity     = raw_qty,   # add_transaction applies abs() internally
            rate         = float(data["rate"]),
            date_str     = data["date"],
            share_type   = data.get("share_type", "Secondary"),
            include_dp   = bool(data.get("include_dp", True)),
            notes        = data.get("notes", ""),
            company_name = data.get("company_name", ""),
        )
        return jresp({"success": True, **result})
    except Exception as e:
        return jresp({"error": str(e)}), 400

@app.route("/api/portfolio/transactions/<int:tx_id>", methods=["DELETE"])
def api_delete_transaction(tx_id):
    delete_transaction(tx_id)
    return jresp({"success": True, "id": tx_id})

@app.route("/api/portfolio/transactions/<int:tx_id>", methods=["PUT"])
def api_update_transaction(tx_id):
    data = request.get_json(force=True) or {}
    update_transaction(tx_id, **data)
    return jresp({"success": True})

@app.route("/api/portfolio/holdings")
def api_holdings():
    live_prices_raw = {}
    mkt_src = "—"
    force_refresh = request.args.get("refresh","0") == "1"
    try:
        prices, mkt_src = fetch_market_today("auto", force_refresh=force_refresh)
        # Pass full price objects so portfolio can compute today's P&L
        live_prices_raw = {p["symbol"]: {
            "ltp":        p.get("ltp", 0) or 0,
            "prev_close": p.get("prev_close", 0) or 0,
            "change_pct": p.get("change_pct", 0) or 0,
        } for p in prices}
    except Exception:
        pass
    summary = get_portfolio_summary(live_prices_raw)
    summary["market_source"] = mkt_src
    summary["fetched_at"]    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return jresp(summary)

@app.route("/api/portfolio/watchlist", methods=["GET"])
def api_get_watchlist():
    wl = get_watchlist()
    # Enrich with live price
    try:
        prices, _ = fetch_market_today("auto")
        price_map = {p["symbol"]: p for p in prices}
        for item in wl:
            p = price_map.get(item["symbol"], {})
            item["ltp"]        = p.get("ltp", 0)
            item["change_pct"] = p.get("change_pct", 0)
    except Exception:
        pass
    return jresp({"watchlist": wl})

@app.route("/api/portfolio/watchlist", methods=["POST"])
def api_add_watchlist():
    data = request.get_json(force=True) or {}
    sym = data.get("symbol", "").upper().strip()
    if not sym: return jresp({"error": "symbol required"}), 400
    add_watchlist(sym, float(data.get("target", 0)), float(data.get("stop_loss", 0)), data.get("notes", ""))
    return jresp({"success": True})

@app.route("/api/portfolio/watchlist/<symbol>", methods=["DELETE"])
def api_remove_watchlist(symbol):
    remove_watchlist(symbol)
    return jresp({"success": True})

@app.route("/api/portfolio/cash", methods=["GET"])
def api_cash_balance():
    return jresp({"balance": get_cash_balance()})

@app.route("/api/portfolio/cash", methods=["POST"])
def api_add_cash():
    data = request.get_json(force=True) or {}
    add_cash(data.get("date", datetime.now().strftime("%Y-%m-%d")),
             data.get("type", "DEPOSIT"), float(data.get("amount", 0)),
             data.get("notes", ""))
    return jresp({"success": True})

@app.route("/api/portfolio/export")
def api_export():
    import tempfile
    path = os.path.join(tempfile.gettempdir(), f"nepse_portfolio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    export_portfolio(path)
    from flask import send_file
    return send_file(path, as_attachment=True,
                     download_name=f"nepse_portfolio_{datetime.now().strftime('%Y%m%d')}.json")

@app.route("/api/portfolio/import", methods=["POST"])
def api_import():
    if "file" not in request.files:
        return jresp({"error": "No file uploaded"}), 400
    f    = request.files["file"]
    path = f"/tmp/import_portfolio_{datetime.now().strftime('%H%M%S')}.json"
    f.save(path)
    result = import_portfolio(path)
    # Log the import with per-type counts
    counts = result if isinstance(result, dict) else {}
    detail_parts = []
    for k, v in counts.items():
        if isinstance(v, int) and v > 0:
            detail_parts.append(f"{v} {k}")
    detail = ", ".join(detail_parts) if detail_parts else "0 records"
    log_fetch("Portfolio Import (JSON)", detail,
              records=sum(v for v in counts.values() if isinstance(v, int)))
    _IMPORT_LOG.append({
        "ts":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "type":   "JSON",
        "file":   f.filename,
        "counts": counts,
    })
    return jresp({"success": True, "imported": result})


# ── Fee Settings endpoints ────────────────────────────────────────────────────
@app.route("/api/portfolio/fee_settings", methods=["GET"])
def api_get_fee_settings():
    return jresp(get_fee_settings())

@app.route("/api/portfolio/fee_settings", methods=["POST"])
def api_update_fee_settings():
    data = request.get_json(force=True) or {}
    if not data:
        return jresp({"error": "No data provided"}), 400
    updated = update_fee_settings(data)
    return jresp({"success": True, "fee_settings": updated})

# ── Clear all transactions (for fresh re-import) ──────────────────────────────
@app.route("/api/portfolio/clear_transactions", methods=["POST"])
def api_clear_transactions():
    count = clear_all_transactions()
    return jresp({"success": True, "deleted": count,
                  "message": f"Cleared {count} transactions. Ready for fresh import."})


@app.route("/api/portfolio/import_csv", methods=["POST"])
def api_import_csv():
    """
    Import portfolio holdings from CSV.

    Supported formats:
    1. MeroShare export: Scrip, Current Balance, Purchase Price, Last Closing Price
    2. History CSV:      Symbol, Date, Qty, Rate, Type (BUY/SELL/BONUS/RIGHT)

    Multiple rows for the same symbol are ALL imported as individual transactions
    (the FIFO engine in portfolio.py handles weighted-average automatically).
    If "average_per_symbol=1" is sent, multiple rows for same symbol are
    collapsed to one weighted-average BUY transaction.
    """
    import csv, io
    if "file" not in request.files:
        return jresp({"error": "No file uploaded"}), 400
    f = request.files["file"]
    has_pp    = request.form.get("has_purchase_price", "0") == "1"
    do_avg    = request.form.get("average_per_symbol",  "0") == "1"
    file_bytes = f.read()
    content   = file_bytes.decode("utf-8-sig", errors="replace")
    reader    = csv.DictReader(io.StringIO(content))

    def norm(s):
        return (s or "").strip().strip('"').strip().lower()

    imported  = 0
    skipped   = 0
    errors    = []
    today_str = datetime.now().strftime("%Y-%m-%d")

    # Auto-detect format by inspecting headers
    headers = [norm(h) for h in (reader.fieldnames or [])]
    # Unified format: has "type" or "tx_type" column
    # MeroShare format: has "scrip" and "current balance"
    # Both auto-detected and handled identically - unified format is preferred
    is_history_csv = True  # Always treat as unified/history format first
    has_type_col   = any(h in headers for h in ("type","tx type","transaction type","tx_type"))
    has_meroshare  = any(h in headers for h in ("scrip","current balance"))

    # Collect all valid rows first so we can do per-symbol averaging
    collected = []   # list of {symbol, date, qty, rate, tx_type}

    for row in reader:
        row_n = {norm(k): (v or "").strip().strip('"') for k, v in row.items()}
        symbol = (row_n.get("scrip") or row_n.get("symbol") or row_n.get("script") or "").upper().strip()
        if not symbol or symbol in ("TOTAL :", "TOTAL:", "SCRIP", "SYMBOL", ""):
            skipped += 1; continue
        try:
            if is_history_csv:
                # History format: Date, Symbol, Qty, Rate, Type
                raw_qty = float(row_n.get("qty") or row_n.get("quantity") or row_n.get("shares") or 0)
                if raw_qty == 0: skipped += 1; continue
                rate = float(row_n.get("rate") or row_n.get("price") or
                             row_n.get("purchase price") or row_n.get("avg cost") or 0)
                tx_type = (row_n.get("type") or row_n.get("tx type") or
                           row_n.get("transaction type") or "BUY").upper().strip()
                if tx_type not in ("BUY","SELL","BONUS","RIGHT","DIVIDEND"):
                    tx_type = "BUY"
                # If qty is negative, treat it as a SELL regardless of Type column
                if raw_qty < 0:
                    tx_type = "SELL"
                qty = abs(raw_qty)  # always store qty as positive; tx_type encodes direction
                date_str = (row_n.get("date") or row_n.get("purchase date") or
                            row_n.get("tx date") or today_str).strip()
                # Normalise date format
                for fmt in ("%Y-%m-%d","%m/%d/%Y","%m-%d-%Y","%d/%m/%Y","%d-%m-%Y","%Y/%m/%d"):
                    try:
                        date_str = datetime.strptime(date_str[:10], fmt).strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        pass
                collected.append({"symbol":symbol,"date":date_str,"qty":qty,"rate":rate,"tx_type":tx_type})
            else:
                # MeroShare format: Scrip, Current Balance, [Purchase Price], LTP
                qty = float(row_n.get("current balance") or row_n.get("balance") or
                            row_n.get("quantity") or 0)
                if qty <= 0: skipped += 1; continue
                if has_pp:
                    rate = float(row_n.get("purchase price") or 0)
                    if not rate:
                        rate = float(row_n.get("last closing price") or
                                     row_n.get("ltp") or 0)
                else:
                    rate = float(row_n.get("last closing price") or row_n.get("ltp") or
                                 row_n.get("last transaction price (ltp)") or
                                 row_n.get("price") or 0)
                collected.append({"symbol":symbol,"date":today_str,"qty":qty,"rate":rate,"tx_type":"BUY"})
        except Exception as ex:
            errors.append(f"{symbol}: {ex}"); skipped += 1

    # Apply averaging if requested (collapse same symbol + same type into one row)
    if do_avg and not is_history_csv:
        from collections import defaultdict
        grouped = defaultdict(lambda: {"total_qty":0.0,"total_value":0.0,"date":today_str,"tx_type":"BUY"})
        for row in collected:
            k = row["symbol"]
            grouped[k]["total_qty"]   += row["qty"]
            grouped[k]["total_value"] += row["qty"] * row["rate"]
            grouped[k]["date"]         = row["date"]
            grouped[k]["tx_type"]      = row["tx_type"]
        collected = []
        for sym, g in grouped.items():
            avg_rate = g["total_value"] / g["total_qty"] if g["total_qty"] else 0
            collected.append({"symbol":sym,"date":g["date"],"qty":g["total_qty"],"rate":avg_rate,"tx_type":g["tx_type"]})

    # Import all collected rows with hash-based deduplication
    duplicates = 0
    for row in collected:
        try:
            ih = _make_import_hash(row["symbol"], row["tx_type"], row["qty"], row["rate"], row["date"])
            result = add_transaction(
                date_str          = row["date"],
                symbol            = row["symbol"],
                tx_type           = row["tx_type"],
                share_type        = "Ordinary",
                quantity          = row["qty"],
                rate              = row["rate"],
                notes             = f"Imported from CSV ({f.filename})",
                import_hash       = ih,
                skip_if_duplicate = True,
            )
            if result.get("skipped"):
                duplicates += 1
                skipped += 1
            else:
                imported += 1
        except Exception as ex:
            errors.append(f"{row['symbol']}: {ex}"); skipped += 1

    log_fetch("Portfolio Import (CSV)", f"{imported} transactions from {f.filename}",
              records=imported, source="CSV Import")
    _IMPORT_LOG.append({
        "ts":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "type":   "CSV",
        "file":   f.filename,
        "counts": {"transactions": imported, "skipped": skipped, "duplicates": duplicates},
    })
    return jresp({
        "success":    True,
        "imported":   {"transactions": imported, "skipped": skipped, "duplicates": duplicates},
        "errors":     errors[:10],
    })


@app.route("/api/fetch_log")
def api_fetch_log():
    """Return the in-memory data fetch log and import history."""
    return jresp({
        "log":        list(_FETCH_LOG),
        "imports":    _IMPORT_LOG[-50:],
        "log_count":  len(_FETCH_LOG),
    })


@app.route("/api/fetch_log/clear", methods=["POST"])
def api_fetch_log_clear():
    """Clear the in-memory fetch log and import history."""
    _FETCH_LOG.clear()
    _IMPORT_LOG.clear()
    return jresp({"success": True, "message": "Fetch log cleared."})


# ═══════════════════════════════════════════════════════════════════════════════
#  BROKER CACHE  — persistent SQLite cache for floorsheet data
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/cache/status")
def api_cache_status():
    """Return cache statistics and recent activity log."""
    return jresp(cache_stats())


@app.route("/api/cache/source")
def api_cache_source():
    """
    Check which floorsheet source is available.
    Returns availability of NepalStock API and MeroLagani.
    Used by UI to show which source will be used for cache updates.
    """
    ns_ok = False
    try:
        ns_ok = nepalstock_floorsheet_available()
    except Exception:
        pass
    return jresp({
        "nepalstock": ns_ok,
        "merolagani": True,   # always available as fallback
        "primary":    "NepalStock API" if ns_ok else "MeroLagani",
        "note":       ("Fast JSON API — full day in ~30-60s" if ns_ok
                       else "HTML scraping — may take several minutes per date"),
    })


@app.route("/api/cache/clear", methods=["POST"])
def api_cache_clear():
    """
    Clear broker floorsheet cache.
    Optional body: { "symbol": "NABIL" }  or  { "date": "2026-03-01" }
    No body → clears everything.
    """
    data   = request.get_json(silent=True) or {}
    symbol = data.get("symbol", "").strip().upper() or None
    date   = data.get("date",   "").strip()         or None
    ok     = cache_clear(symbol=symbol, date_str=date)
    log_fetch("Cache CLEAR",
              f"symbol={symbol or 'ALL'} date={date or 'ALL'}",
              records=0, source="Cache")
    return jresp({"success": ok})


@app.route("/api/cache/clear_log", methods=["POST"])
def api_cache_clear_log():
    """Clear the cache activity log (not the cache data itself)."""
    ok = cache_clear_log()
    return jresp({"success": ok})


@app.route("/api/cache/update", methods=["POST"])
def api_cache_update():
    """Start background incremental cache update via bg_start engine."""
    data   = request.get_json(silent=True) or {}
    symbol = data.get("symbol", "").strip().upper()
    result = bg_start(symbol=symbol, delay=1.5, retry_delay=10.0, max_retries=3)
    if result.get("already_running"):
        return jresp({"success": False, "message": "Cache update already running.",
                       "state": bg_state()})
    dn = result.get("dates_needed", 0)
    if dn == 0:
        return jresp({"success": True, "dates_needed": 0,
                       "message": "Cache is already up to date."})
    return jresp({
        "success":      True,
        "job_id":       result.get("job_id",""),
        "dates_needed": dn,
        "message":      result.get("message",""),
    })


@app.route("/api/cache/update_cancel", methods=["POST"])
def api_cache_update_cancel():
    """Cancel the running background cache update."""
    bg_cancel()
    return jresp({"success": True, "message": "Cancel signal sent."})


@app.route("/api/cache/update_progress")
def api_cache_update_progress():
    """Return current background worker state for polling."""
    return jresp(bg_state())






# ═══════════════════════════════════════════════════════════════════════════════
#  MANUAL REFRESH  — clears in-memory caches so next request goes live
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """
    Manual refresh: invalidate all in-memory caches (market + history).
    Called by the Refresh button in the UI.
    Returns what was cleared so the UI can confirm.
    """
    symbol = request.args.get("symbol","").strip().upper() or None
    invalidate_market_cache()
    invalidate_history_cache(symbol)
    log_fetch("Manual Refresh", f"Caches cleared (symbol={symbol or 'ALL'})", source="User")
    return jresp({
        "success": True,
        "cleared": "history:" + (symbol or "ALL") + ", market:ALL",
        "ts": datetime.now().isoformat(),
    })


# ═══════════════════════════════════════════════════════════════════════════════
#  BROKER ANALYSIS FOR ANALYZE TAB
#  Returns broker buy/sell/net for a symbol over 3M, 1M, Last Trading Day
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/broker/symbol_summary")
def api_broker_symbol_summary():
    """
    Broker accumulation summary for a single symbol across three windows:
    last_day, last_month (30d), last_3month (90d).
    Uses broker cache first; falls back to live nepalstock API.
    """
    symbol = request.args.get("symbol", "").upper().strip()
    if not symbol:
        return jresp({"error": "symbol required"})

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import os

    today    = datetime.now()
    windows  = {
        "last_3month": (today - timedelta(days=90)).strftime("%Y-%m-%d"),
        "last_month":  (today - timedelta(days=30)).strftime("%Y-%m-%d"),
    }
    end_date = today.strftime("%Y-%m-%d")

    # Build NEPSE trading day list (skip Fri/Sat)
    def trading_days(from_str, to_str):
        days = []
        d = datetime.strptime(from_str, "%Y-%m-%d")
        e = datetime.strptime(to_str,   "%Y-%m-%d")
        while d <= e:
            if d.weekday() not in (4, 5):
                days.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
        return days

    def fetch_rows_for_date(ds):
        cached = cache_get(ds, symbol)
        if cached is not None:
            for r in cached: r["date"] = ds
            return cached, "cache"
        try:
            rows = nepalstock_floorsheet(ds, symbol=symbol)
            if rows:
                for r in rows: r["date"] = ds
                today_str = datetime.now().strftime("%Y-%m-%d")
                if ds < today_str:
                    cache_set(ds, symbol, rows)
                return rows, "live"
        except Exception:
            pass
        return [], "empty"

    def _clean_code(raw):
        """Return broker code string, or empty string if NaN/None/invalid."""
        try:
            import math as _m
            if raw is None: return ""
            if isinstance(raw, float) and (_m.isnan(raw) or _m.isinf(raw)): return ""
        except Exception:
            pass
        s = str(raw or "").strip()
        if not s or s.lower() in ("nan", "none", "null", "0", "-", "0.0", ""): return ""
        return s

    def summarize(rows):
        brokers = {}
        broker_names_live = {}   # code -> name from live API data (buyer_name/seller_name)
        dates_seen = set()
        for r in rows:
            b = _clean_code(r.get("buyer",  ""))
            s = _clean_code(r.get("seller", ""))
            q = int(float(r.get("qty", 0) or 0))
            a = float(r.get("amount", 0) or 0)
            if r.get("date"): dates_seen.add(r["date"])
            # Capture broker names when available in live data
            bn = str(r.get("buyer_name",  "") or "").strip()
            sn = str(r.get("seller_name", "") or "").strip()
            if b and bn and bn.lower() not in ("nan","none",""):
                broker_names_live[b] = bn
            if s and sn and sn.lower() not in ("nan","none",""):
                broker_names_live[s] = sn
            for code, side in [(b, "buy"), (s, "sell")]:
                if not code: continue
                if code not in brokers:
                    brokers[code] = {"broker": code, "buy_qty": 0, "sell_qty": 0,
                                     "buy_amt": 0.0, "sell_amt": 0.0}
                brokers[code][f"{side}_qty"] += q
                brokers[code][f"{side}_amt"] += a
        result = []
        for d in brokers.values():
            d["net_qty"]     = d["buy_qty"] - d["sell_qty"]
            d["buy_amt"]     = round(d["buy_amt"], 2)
            d["sell_amt"]    = round(d["sell_amt"], 2)
            d["total_turnover"] = round(d["buy_amt"] + d["sell_amt"], 2)
            d["pattern"]     = ("STRONG BUY"  if d["buy_qty"] > 0 and d["sell_qty"] == 0 else
                                "BUY"          if d["net_qty"] > 0 else
                                "STRONG SELL"  if d["sell_qty"] > 0 and d["buy_qty"] == 0 else
                                "SELL"         if d["net_qty"] < 0 else "NEUTRAL")
            result.append(d)
        # Sort by total turnover (buy_amt + sell_amt) descending — most active brokers first
        result.sort(key=lambda x: x["total_turnover"], reverse=True)
        return result, sorted(dates_seen), broker_names_live

    broker_names = get_broker_names()

    # ── Last trading day ──────────────────────────────────────────
    last_day_rows = []
    last_day_date = ""
    # Walk back up to 7 days to find the most recent trading day with data
    for delta in range(1, 8):
        ds = (today - timedelta(days=delta)).strftime("%Y-%m-%d")
        if datetime.strptime(ds, "%Y-%m-%d").weekday() in (4, 5):
            continue
        rows, _ = fetch_rows_for_date(ds)
        if rows:
            last_day_rows = rows
            last_day_date = ds
            break

    last_day_summary, _, _ldn = summarize(last_day_rows)

    # ── 30-day and 90-day ─────────────────────────────────────────
    results = {"last_month": [], "last_3month": []}
    all_rows = {"last_month": [], "last_3month": []}

    workers = min(12, os.cpu_count() or 4)
    days_3m = trading_days(windows["last_3month"], end_date)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_rows_for_date, ds): ds for ds in days_3m}
        for fut in as_completed(futs):
            ds = futs[fut]
            try:
                rows, _ = fut.result()
                for w_key, from_str in windows.items():
                    if ds >= from_str:
                        all_rows[w_key].extend(rows)
            except Exception:
                pass

    broker_names_live_all = dict(_ldn)  # start with last_day names

    for w_key in ("last_month", "last_3month"):
        summ, dates, _bnl = summarize(all_rows[w_key])
        broker_names_live_all.update(_bnl)
        for item in summ:
            code = str(item["broker"])
            name = broker_names_live_all.get(code, "") or broker_names.get(code, "")
            item["name"]    = name
            item["display"] = f"{code} — {name}" if name else code
        results[w_key] = {"brokers": summ[:50], "total_rows": len(all_rows[w_key]),
                          "dates": dates}

    for item in last_day_summary:
        code = str(item["broker"])
        name = broker_names_live_all.get(code, "") or broker_names.get(code, "")
        item["name"]    = name
        item["display"] = f"{code} — {name}" if name else code

    return jresp({
        "symbol":      symbol,
        "last_day":    {"brokers": last_day_summary[:50], "date": last_day_date,
                         "total_rows": len(last_day_rows)},
        "last_month":  results["last_month"],
        "last_3month": results["last_3month"],
        "broker_names": broker_names,
    })


@app.route("/api/screener/broker_holdings")
def api_screener_broker_holdings():
    """
    For each stock in the screener results, find which broker holds
    the most (highest net buy_qty - sell_qty) over 1M and 3M windows.
    Uses broker cache; falls back to live.
    """
    symbols_raw = request.args.get("symbols", "")
    window      = request.args.get("window", "last_month")   # last_month | last_3month
    if not symbols_raw:
        return jresp({"error": "symbols required"})

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import os

    symbols  = [s.strip().upper() for s in symbols_raw.split(",") if s.strip()][:50]
    today    = datetime.now()
    days_back = 90 if window == "last_3month" else 30
    from_str  = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_str   = today.strftime("%Y-%m-%d")

    def trading_days_range(f, t):
        days = []
        d = datetime.strptime(f, "%Y-%m-%d")
        e = datetime.strptime(t, "%Y-%m-%d")
        while d <= e:
            if d.weekday() not in (4, 5):
                days.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
        return days

    def top_broker_for_symbol(sym):
        days = trading_days_range(from_str, end_str)
        brokers = {}
        for ds in days:
            rows = cache_get(ds, sym) or []
            if not rows:
                try:
                    rows = nepalstock_floorsheet(ds, symbol=sym)
                    if rows and ds < end_str:
                        cache_set(ds, sym, rows)
                except Exception:
                    continue
            for r in rows:
                b = str(r.get("buyer",  "")).strip()
                s = str(r.get("seller", "")).strip()
                q = int(r.get("qty", 0) or 0)
                for code, side in [(b, "buy"), (s, "sell")]:
                    if not code: continue
                    if code not in brokers:
                        brokers[code] = {"buy": 0, "sell": 0, "buy_amt": 0.0, "sell_amt": 0.0}
                    brokers[code][side] += q
                    brokers[code][side+"_amt"] += float(r.get("amount", 0) or 0)
        if not brokers:
            return sym, None
        # Sort by total shares traded (buy+sell qty) — highest volume broker for 1M
        top = max(brokers.items(), key=lambda x: x[1]["buy"] + x[1]["sell"])
        code = top[0]
        net  = top[1]["buy"] - top[1]["sell"]
        return sym, {"broker": code, "net_qty": net,
                     "buy_qty":  top[1]["buy"],
                     "sell_qty": top[1]["sell"],
                     "total_turnover": round(top[1]["buy_amt"] + top[1]["sell_amt"], 2)}

    workers = min(8, os.cpu_count() or 4)
    result  = {}
    broker_names = get_broker_names()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(top_broker_for_symbol, sym): sym for sym in symbols}
        for fut in as_completed(futs):
            try:
                sym, data = fut.result()
                if data:
                    code = data["broker"]
                    data["name"]    = broker_names.get(code, "")
                    data["display"] = f"{code} — {data['name']}" if data["name"] else code
                result[sym] = data
            except Exception:
                pass

    return jresp({"holdings": result, "window": window,
                  "from_date": from_str, "to_date": end_str})

# ═══════════════════════════════════════════════════════════════════════════════
#  DIAGNOSTIC / DEBUG ROUTE
#  Returns the raw scraped rows exactly as received from MeroLagani,
#  with zero processing — so you can compare directly with the website.
# ═══════════════════════════════════════════════════════════════════════════════
@app.route("/api/debug/floorsheet")
def api_debug_floorsheet():
    """
    Raw floorsheet diagnostic.
    Returns every scraped row with full metadata so you can verify
    counts match what MeroLagani shows on the website.
    """
    symbol    = request.args.get("symbol", "").upper().strip()
    buyer     = request.args.get("buyer", "").strip()
    seller    = request.args.get("seller", "").strip()
    date_str  = request.args.get("date", datetime.now().strftime("%Y-%m-%d"))

    rows = merolagani_floorsheet_by_date(
        symbol=symbol, buyer=buyer, seller=seller,
        date_str=date_str
    )

    # Raw totals with zero processing
    total_qty    = sum(r.get("qty", 0)    for r in rows)
    total_amount = sum(r.get("amount", 0) for r in rows)

    # Per-broker breakdown from raw rows (no dedup, no filtering)
    from collections import defaultdict
    broker_buy  = defaultdict(lambda: {"qty": 0, "amt": 0.0, "rows": 0})
    broker_sell = defaultdict(lambda: {"qty": 0, "amt": 0.0, "rows": 0})
    # Identify same txn_no appearing with DIFFERENT row content (partial fills)
    # vs truly identical rows (data fetch duplicates)
    from collections import Counter
    txn_counter = Counter(r.get("txn_no","") for r in rows if r.get("txn_no",""))
    dupe_txns   = {t for t,c in txn_counter.items() if c > 1}
    # Full identity duplicates (exact same row returned twice)
    seen_full = set()
    exact_dupes = 0
    for r in rows:
        key = (r.get("txn_no",""), r.get("buyer",""), r.get("seller",""), r.get("qty",0))
        if key in seen_full: exact_dupes += 1
        seen_full.add(key)

    for r in rows:
        b  = r.get("buyer",  "")
        s  = r.get("seller", "")
        q  = r.get("qty",    0)
        a  = r.get("amount", 0.0)
        # Flag self-trades (same broker on both sides)
        r["self_trade"] = (b == s and bool(b))
        if b:
            broker_buy[b]["qty"]  += q
            broker_buy[b]["amt"]  += a
            broker_buy[b]["rows"] += 1
        # Count seller — self-trades are counted on both buy and sell sides
        if s:
            broker_sell[s]["qty"]  += q
            broker_sell[s]["amt"]  += a
            broker_sell[s]["rows"] += 1

    # Build broker summary
    all_brokers = set(list(broker_buy.keys()) + list(broker_sell.keys()))
    broker_summary = []
    for bk in sorted(all_brokers, key=lambda x: x.zfill(5)):
        buy  = broker_buy.get(bk,  {"qty":0,"amt":0.0,"rows":0})
        sell = broker_sell.get(bk, {"qty":0,"amt":0.0,"rows":0})
        broker_summary.append({
            "broker":    bk,
            "buy_qty":   buy["qty"],
            "buy_rows":  buy["rows"],
            "sell_qty":  sell["qty"],
            "sell_rows": sell["rows"],
            "net_qty":   buy["qty"] - sell["qty"],
            "buy_amt":   round(buy["amt"],  2),
            "sell_amt":  round(sell["amt"], 2),
        })
    broker_summary.sort(key=lambda x: x["buy_qty"]+x["sell_qty"], reverse=True)

    return jresp({
        "query": {
            "symbol":    symbol or "(all)",
            "buyer":     buyer  or "(any)",
            "seller":    seller or "(any)",
            "date":      date_str,
        },
        "raw_row_count":       len(rows),
        "total_qty":           total_qty,
        "total_amount":        round(total_amount, 2),
        "duplicate_txn_nos":   sorted(dupe_txns),  # same txn_no, different content = partial fills
        "exact_duplicate_rows": exact_dupes,        # truly identical rows = fetch artifacts
        "rows":                rows,                # every scraped row, unmodified
        "broker_summary":      broker_summary,      # per-broker from raw rows
    })


# ═══════════════════════════════════════════════════════════════════════════════
#  NEW ROUTES — v30 Updates
# ═══════════════════════════════════════════════════════════════════════════════

# ── Server-side PDF Export (zero-dependency — stdlib only) ──────────────────
@app.route("/api/export/pdf", methods=["POST"])
def api_export_pdf():
    """
    Generate a styled HTML report returned as a downloadable file.
    No reportlab needed — pure stdlib. Browser opens it; user Ctrl+P -> Save as PDF.
    """
    import html as _html
    try:
        data         = request.get_json(force=True) or {}
        symbol       = data.get("symbol", "SYMBOL")
        company_name = data.get("company_name", symbol)
        ltp          = data.get("ltp", 0)
        change_pct   = data.get("change_pct", 0)
        projection   = data.get("projection", "HOLD")
        score        = data.get("score", 50)
        sector       = data.get("sector", "")
        source       = data.get("source", "")
        signals      = data.get("signals", [])
        patterns     = data.get("patterns", [])
        performance  = data.get("performance", {})
        price_targets = data.get("price_targets", {})
        fundamentals = data.get("fundamentals", {})
        broker_data  = data.get("broker_data", [])
        generated    = datetime.now().strftime("%d %b %Y %H:%M")

        chg_sign  = "+" if change_pct >= 0 else ""
        chg_color = "#007a30" if change_pct >= 0 else "#c8001a"
        proj_color = "#007a30" if "BUY" in projection else ("#c8001a" if "SELL" in projection else "#a06000")

        def xe(v): return _html.escape(str(v))
        def th(t): return '<th style="background:#005fa3;color:#fff;padding:6px 10px;font-size:11px;text-align:left">'+xe(t)+'</th>'
        def td(t, bold=False, color=None):
            s = ('color:'+color+';' if color else '') + ('font-weight:700;' if bold else '')
            return '<td style="padding:5px 10px;font-size:11px;border-bottom:1px solid #dde4ee;'+s+'">'+xe(t)+'</td>'

        fund_keys = [("eps","EPS"),("pe","P/E Ratio"),("book_value","Book Value"),
                     ("52w_high","52W High"),("52w_low","52W Low"),("market_cap","Market Cap")]
        fund_rows = "".join(
            '<tr>'+td(label,bold=True)+td(v)+'</tr>'
            for k, label in fund_keys
            for v in [fundamentals.get(k) or fundamentals.get(k.replace("52w_","high_52w" if "high" in k else "low_52w"), "")]
            if v
        )

        sig_rows = ""
        for s in signals:
            color = "#007a30" if s.get("bullish") else "#c8001a"
            pts   = s.get("points", 0)
            pts_s = ("+" if pts >= 0 else "") + str(pts)
            sig_rows += ('<tr>'+td(s.get("indicator",""))+td(s.get("value",""))+
                         '<td style="padding:5px 10px;font-size:11px;border-bottom:1px solid #dde4ee;'
                         'font-weight:700;color:'+color+';text-align:center">'+xe(pts_s)+'</td></tr>')

        pat_rows = ""
        for p in patterns[-6:]:
            pc = "#007a30" if p.get("type")=="bullish" else ("#c8001a" if p.get("type")=="bearish" else "#a06000")
            pat_rows += ('<tr>'+td(p.get("date",""))+td(p.get("pattern",""),bold=True)+
                         '<td style="padding:5px 10px;font-size:11px;border-bottom:1px solid #dde4ee;'
                         'font-weight:700;color:'+pc+'">'+xe(p.get("signal",""))+'</td>'+
                         td(str(p.get("prob",55))+"%")+td(p.get("description",""))+'</tr>')

        perf_rows = ""
        for k, label in [("change_1d","1 Day"),("change_1w","1 Week"),("change_1m","1 Month"),
                          ("change_3m","3 Months"),("change_1y","1 Year")]:
            v = performance.get(k, 0)
            color = "#007a30" if v >= 0 else "#c8001a"
            perf_rows += ('<tr>'+td(label,bold=True)+
                          '<td style="padding:5px 10px;font-size:11px;border-bottom:1px solid #dde4ee;'
                          'font-weight:700;color:'+color+'">'+("+"+f"{v:.2f}%" if v>=0 else f"{v:.2f}%")+'</td></tr>')

        targets_html = ""
        if price_targets:
            t1 = price_targets.get("target_1", 0)
            t2 = price_targets.get("target_2", 0)
            sl = price_targets.get("stop_loss", 0)
            rr = price_targets.get("risk_reward", 0)
            targets_html = (
                '<h3 style="color:#005fa3;border-bottom:2px solid #005fa3;padding-bottom:4px;margin-top:22px">Price Targets</h3>'
                '<table width="100%" cellspacing="0" style="border-collapse:collapse;border:1px solid #b0c8de">'
                '<tr><th style="background:#005fa3;color:#fff;padding:6px 10px;font-size:11px">Target 1</th>'
                '<th style="background:#005fa3;color:#fff;padding:6px 10px;font-size:11px">Target 2</th>'
                '<th style="background:#005fa3;color:#fff;padding:6px 10px;font-size:11px">Stop Loss</th>'
                '<th style="background:#005fa3;color:#fff;padding:6px 10px;font-size:11px">Risk/Reward</th></tr>'
                '<tr style="background:#f0fff5">'
                f'<td style="padding:8px 10px;font-size:13px;font-weight:700;color:#007a30;text-align:center">Rs {t1:,.2f}</td>'
                f'<td style="padding:8px 10px;font-size:13px;font-weight:700;color:#007a30;text-align:center">Rs {t2:,.2f}</td>'
                f'<td style="padding:8px 10px;font-size:13px;font-weight:700;color:#c8001a;text-align:center">Rs {sl:,.2f}</td>'
                f'<td style="padding:8px 10px;font-size:13px;font-weight:700;color:#005fa3;text-align:center">{rr:.2f}x</td>'
                '</tr></table>'
            )

        broker_html = ""
        if broker_data:
            brows = ""
            for b in broker_data[:15]:
                net  = b.get("net_qty", 0)
                nc   = "#007a30" if net > 0 else ("#c8001a" if net < 0 else "#555")
                brows += (
                    '<tr>'
                    '<td style="padding:5px 10px;font-size:11px;border-bottom:1px solid #dde4ee">'+xe(b.get("broker",""))+'</td>'
                    '<td style="padding:5px 10px;font-size:11px;border-bottom:1px solid #dde4ee;text-align:right">'+f"{b.get('buy_qty',0):,}"+'</td>'
                    '<td style="padding:5px 10px;font-size:11px;border-bottom:1px solid #dde4ee;text-align:right">'+f"{b.get('sell_qty',0):,}"+'</td>'
                    '<td style="padding:5px 10px;font-size:11px;border-bottom:1px solid #dde4ee;text-align:right;font-weight:700;color:'+nc+'">'+f"{net:,}"+'</td>'
                    '<td style="padding:5px 10px;font-size:11px;border-bottom:1px solid #dde4ee">'+xe(b.get("pattern",""))+'</td>'
                    '</tr>'
                )
            broker_html = (
                '<h3 style="color:#005fa3;border-bottom:2px solid #005fa3;padding-bottom:4px;margin-top:22px">Top Broker Activity</h3>'
                '<table width="100%" cellspacing="0" style="border-collapse:collapse;border:1px solid #b0c8de">'
                '<tr>'+th("Broker")+th("Buy Qty")+th("Sell Qty")+th("Net Qty")+th("Pattern")+'</tr>'
                + brows + '</table>'
            )

        html_content = (
            '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">'
            '<title>NEPSE Report \u2014 '+xe(symbol)+'</title>'
            '<style>'
            '@media print{body{margin:0}.no-print{display:none!important}}'
            'body{font-family:"Segoe UI",Arial,sans-serif;margin:0;padding:20px;color:#1a2a3a;background:#fff}'
            '.print-bar{background:#f0f4fa;border:1px solid #b0c8de;border-radius:6px;padding:10px 16px;'
            'margin-bottom:18px;display:flex;align-items:center;gap:12px}'
            '.print-btn{background:#005fa3;color:#fff;border:none;padding:8px 20px;border-radius:5px;'
            'cursor:pointer;font-size:13px;font-weight:700}'
            '.print-btn:hover{background:#0073c4}'
            'table{border-collapse:collapse;border:1px solid #b0c8de}'
            '.price-row{background:#e4ecf3;border:2px solid #005fa3;border-radius:6px;padding:12px 18px;'
            'display:flex;gap:30px;align-items:center;margin:10px 0 18px 0;flex-wrap:wrap}'
            'h1{color:#005fa3;margin:0 0 2px 0;font-size:22px}'
            'h3{color:#005fa3;border-bottom:2px solid #005fa3;padding-bottom:4px;margin-top:22px}'
            '.footer{margin-top:30px;border-top:1px solid #b0c8de;padding-top:8px;font-size:10px;color:#6a8aaa;text-align:center}'
            '</style></head><body>'
            '<div class="no-print print-bar">'
            '<span style="font-size:13px;color:#4a6a8a">\ud83d\udcc4 To save as PDF: click <b>Print to PDF</b> or press <b>Ctrl+P</b> \u2192 choose <i>Save as PDF</i></span>'
            '<button class="print-btn" onclick="window.print()">\ud83d\udda8 Print / Save as PDF</button>'
            '</div>'
            '<h1>'+xe(company_name)+' ('+xe(symbol)+')</h1>'
            '<div style="font-size:11px;color:#6a8aaa;margin-bottom:10px">'
            'Sector: '+xe(sector)+' &nbsp;|&nbsp; Source: '+xe(source)+' &nbsp;|&nbsp; Generated: '+xe(generated)
            +'</div>'
            '<div class="price-row">'
            '<div><span style="font-size:11px;color:#6a8aaa">LTP</span><br>'
            '<span style="font-size:22px;font-weight:700;font-family:Consolas,monospace">Rs '+f'{ltp:,.2f}'+'</span></div>'
            '<div><span style="font-size:11px;color:#6a8aaa">Change</span><br>'
            '<span style="font-size:16px;font-weight:700;color:'+chg_color+'">'+xe(chg_sign+f"{change_pct:.2f}%")+'</span></div>'
            '<div><span style="font-size:11px;color:#6a8aaa">Signal</span><br>'
            '<span style="font-size:18px;font-weight:700;color:'+proj_color+'">'+xe(projection)+'</span></div>'
            '<div><span style="font-size:11px;color:#6a8aaa">Score</span><br>'
            '<span style="font-size:16px;font-weight:700;color:#005fa3">'+f'{score:.0f}/100'+'</span></div>'
            '</div>'
            + ('<h3 style="color:#005fa3;border-bottom:2px solid #005fa3;padding-bottom:4px;margin-top:22px">Fundamentals</h3>'
               '<table width="60%" cellspacing="0" style="border-collapse:collapse;border:1px solid #b0c8de">'
               '<tbody>'+fund_rows+'</tbody></table>' if fund_rows else '')
            + ('<h3 style="color:#005fa3;border-bottom:2px solid #005fa3;padding-bottom:4px;margin-top:22px">Technical Signals</h3>'
               '<table width="100%" cellspacing="0" style="border-collapse:collapse;border:1px solid #b0c8de">'
               '<thead><tr>'+th("Indicator")+th("Signal")+th("Points")+'</tr></thead>'
               '<tbody>'+sig_rows+'</tbody></table>' if sig_rows else '')
            + ('<h3 style="color:#005fa3;border-bottom:2px solid #005fa3;padding-bottom:4px;margin-top:22px">Candlestick Patterns</h3>'
               '<table width="100%" cellspacing="0" style="border-collapse:collapse;border:1px solid #b0c8de">'
               '<thead><tr>'+th("Date")+th("Pattern")+th("Signal")+th("Confidence")+th("Description")+'</tr></thead>'
               '<tbody>'+pat_rows+'</tbody></table>' if pat_rows else '')
            + ('<h3 style="color:#005fa3;border-bottom:2px solid #005fa3;padding-bottom:4px;margin-top:22px">Price Performance</h3>'
               '<table width="40%" cellspacing="0" style="border-collapse:collapse;border:1px solid #b0c8de">'
               '<tbody>'+perf_rows+'</tbody></table>' if perf_rows else '')
            + targets_html + broker_html
            + '<div class="footer">Generated by NEPSE Tunnel &nbsp;|&nbsp; '+xe(generated)
            +' &nbsp;|&nbsp; Source: '+xe(source)+' &nbsp;|&nbsp; For informational purposes only.</div>'
            '</body></html>'
        )

        fname = "NEPSE_{}_{}.html".format(symbol, datetime.now().strftime("%Y%m%d_%H%M"))
        return Response(
            html_content.encode("utf-8"),
            mimetype="text/html",
            headers={"Content-Disposition": "attachment; filename=" + fname}
        )

    except Exception as e:
        import traceback
        return jresp({"error": str(e), "trace": traceback.format_exc()[-600:]}), 500


# ── Server-side Excel Export (zero-dependency — stdlib zipfile only) ─────────
@app.route("/api/export/excel", methods=["POST"])
def api_export_excel():
    """
    Build a real .xlsx using only Python stdlib zipfile + xml strings.
    XLSX is a ZIP of XML files — no openpyxl or any third-party package needed.
    """
    import zipfile, io, re as _re
    try:
        data       = request.get_json(force=True) or {}
        title      = data.get("title", "NEPSE Export")
        headers    = data.get("headers", [])
        rows       = data.get("rows", [])
        sheet_name = _re.sub(r'[\\/*?:\[\]]', '_', data.get("sheet", "Data"))[:31]

        strings  = []
        str_idx  = {}

        def si(val):
            s = str(val) if val is not None else ""
            if s not in str_idx:
                str_idx[s] = len(strings)
                strings.append(s)
            return str_idx[s]

        def xe(s):
            return (str(s).replace("&","&amp;").replace("<","&lt;")
                          .replace(">","&gt;").replace('"',"&quot;").replace("'","&apos;"))

        def col_letter(n):
            s = ""
            while n > 0:
                n, r = divmod(n - 1, 26)
                s = chr(65 + r) + s
            return s

        def make_cell(row, col, value, style_id=0):
            addr = col_letter(col) + str(row)
            try:
                fv = float(value)
                return '<c r="'+addr+'" s="'+str(style_id)+'" t="n"><v>'+str(fv)+'</v></c>'
            except (ValueError, TypeError):
                idx = si(value)
                return '<c r="'+addr+'" s="'+str(style_id)+'" t="s"><v>'+str(idx)+'</v></c>'

        num_cols   = max(len(headers), 1)
        generated  = datetime.now().strftime("%d %b %Y %H:%M")
        si(title); si(generated)

        sheet_rows_xml = []
        sheet_rows_xml.append('<row r="1"><c r="A1" s="3" t="s"><v>'+str(str_idx[title])+'</v></c></row>')
        sheet_rows_xml.append('<row r="2"><c r="A2" s="4" t="s"><v>'+str(str_idx[generated])+'</v></c></row>')

        hdr_cells = "".join(make_cell(3, ci+1, h, style_id=1) for ci, h in enumerate(headers))
        sheet_rows_xml.append('<row r="3">'+hdr_cells+'</row>')

        for ri, row in enumerate(rows, 4):
            style    = 2 if ri % 2 == 0 else 0
            data_cells = "".join(make_cell(ri, ci+1, v, style_id=style) for ci, v in enumerate(row))
            sheet_rows_xml.append('<row r="'+str(ri)+'">'+data_cells+'</row>')

        last_row = 3 + len(rows)
        last_col = col_letter(num_cols)

        sheet_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<sheetViews><sheetView workbookViewId="0" showGridLines="1">'
            '<pane ySplit="3" topLeftCell="A4" activePane="bottomLeft" state="frozen"/>'
            '</sheetView></sheetViews>'
            '<sheetData>'+"".join(sheet_rows_xml)+'</sheetData>'
            '<autoFilter ref="A3:'+last_col+str(last_row)+'"/>'
            '</worksheet>'
        )

        ss_items = "".join('<si><t xml:space="preserve">'+xe(s)+'</t></si>' for s in strings)
        ss_xml   = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                    '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
                    ' count="'+str(len(strings))+'" uniqueCount="'+str(len(strings))+'">'+ss_items+'</sst>')

        styles_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<fonts count="4">'
            '<font><sz val="10"/><color rgb="FF1A2A3A"/><name val="Calibri"/></font>'
            '<font><sz val="10"/><b/><color rgb="FFFFFFFF"/><name val="Calibri"/></font>'
            '<font><sz val="10"/><color rgb="FF1A2A3A"/><name val="Calibri"/></font>'
            '<font><sz val="13"/><b/><color rgb="FF005FA3"/><name val="Calibri"/></font>'
            '</fonts>'
            '<fills count="5">'
            '<fill><patternFill patternType="none"/></fill>'
            '<fill><patternFill patternType="gray125"/></fill>'
            '<fill><patternFill patternType="solid"><fgColor rgb="FF005FA3"/></patternFill></fill>'
            '<fill><patternFill patternType="solid"><fgColor rgb="FFE4ECF3"/></patternFill></fill>'
            '<fill><patternFill patternType="solid"><fgColor rgb="FFF0F4FA"/></patternFill></fill>'
            '</fills>'
            '<borders count="2">'
            '<border><left/><right/><top/><bottom/><diagonal/></border>'
            '<border><left style="thin"><color rgb="FFB0C8DE"/></left>'
            '<right style="thin"><color rgb="FFB0C8DE"/></right>'
            '<top style="thin"><color rgb="FFB0C8DE"/></top>'
            '<bottom style="thin"><color rgb="FFB0C8DE"/></bottom><diagonal/></border>'
            '</borders>'
            '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
            '<cellXfs count="5">'
            '<xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0"><alignment wrapText="0" vertical="center"/></xf>'
            '<xf numFmtId="0" fontId="1" fillId="2" borderId="1" xfId="0"><alignment horizontal="center" vertical="center"/></xf>'
            '<xf numFmtId="0" fontId="0" fillId="3" borderId="1" xfId="0"><alignment vertical="center"/></xf>'
            '<xf numFmtId="0" fontId="3" fillId="4" borderId="0" xfId="0"><alignment vertical="center"/></xf>'
            '<xf numFmtId="0" fontId="2" fillId="0" borderId="0" xfId="0"><alignment vertical="center"/></xf>'
            '</cellXfs>'
            '</styleSheet>'
        )

        workbook_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
            ' xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            '<sheets><sheet name="'+xe(sheet_name)+'" sheetId="1" r:id="rId1"/></sheets>'
            '</workbook>'
        )

        wb_rels = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
            '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>'
            '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
            '</Relationships>'
        )

        pkg_rels = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
            '</Relationships>'
        )

        content_types = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml"  ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            '<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>'
            '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
            '</Types>'
        )

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("[Content_Types].xml",        content_types)
            zf.writestr("_rels/.rels",                pkg_rels)
            zf.writestr("xl/workbook.xml",            workbook_xml)
            zf.writestr("xl/_rels/workbook.xml.rels", wb_rels)
            zf.writestr("xl/worksheets/sheet1.xml",   sheet_xml)
            zf.writestr("xl/sharedStrings.xml",       ss_xml)
            zf.writestr("xl/styles.xml",              styles_xml)

        buf.seek(0)
        fname = "NEPSE_{}_{}.xlsx".format(sheet_name, datetime.now().strftime("%Y%m%d_%H%M"))
        return Response(
            buf.read(),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=" + fname}
        )

    except Exception as e:
        import traceback
        return jresp({"error": str(e), "trace": traceback.format_exc()[-600:]}), 500


# ── Mutual Fund NAV Proxy ────────────────────────────────────────────────────
@app.route("/api/mutualfund/navs")
def api_mutualfund_navs():
    """Proxy-fetch Mutual Fund NAVs from nepsealpha.com to avoid CORS."""
    import urllib.request, re
    url = "https://nepsealpha.com/mutual-fund-navs"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="replace")

        # Parse the NAV table
        funds = []
        # Try JSON API first
        api_url = "https://nepsealpha.com/api/mutual-fund-navs"
        try:
            req2 = urllib.request.Request(api_url, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": url,
            })
            with urllib.request.urlopen(req2, timeout=15) as r2:
                import json as _json
                api_data = _json.loads(r2.read().decode())
                if isinstance(api_data, list):
                    funds = api_data
                elif isinstance(api_data, dict):
                    funds = api_data.get("data", api_data.get("funds", api_data.get("navs", [])))
        except Exception:
            pass

        # HTML parse fallback
        if not funds:
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL|re.IGNORECASE)
            for row in rows[1:]:
                cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL|re.IGNORECASE)
                cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
                if len(cells) >= 4:
                    funds.append({
                        "fund_name":   cells[0],
                        "scheme":      cells[1] if len(cells) > 1 else "",
                        "nav":         cells[2] if len(cells) > 2 else "",
                        "nav_date":    cells[3] if len(cells) > 3 else "",
                        "prev_nav":    cells[4] if len(cells) > 4 else "",
                        "change":      cells[5] if len(cells) > 5 else "",
                    })

        return jresp({"funds": funds, "count": len(funds),
                      "source": "nepsealpha.com",
                      "fetched_at": datetime.now().isoformat()})
    except Exception as e:
        return jresp({"error": str(e), "funds": [], "source": "nepsealpha.com"})


# ── SEBON IPO/Rights Pipeline Proxy ─────────────────────────────────────────
@app.route("/api/sebon/pipeline")
def api_sebon_pipeline():
    """Scrape SEBON for IPO, rights share pipeline and approved list."""
    import urllib.request, re, urllib.parse

    base_url = "https://www.sebon.gov.np"
    results  = {"ipo": [], "rights": [], "approved": [], "source": base_url,
                "fetched_at": datetime.now().isoformat()}

    def _fetch(path):
        url = base_url + path
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read().decode("utf-8", errors="replace")

    def _parse_table(html, context=""):
        rows_out = []
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL|re.IGNORECASE)
        for row in rows[1:]:
            cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row, re.DOTALL|re.IGNORECASE)
            cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
            cells = [re.sub(r'\s+', ' ', c) for c in cells]
            if any(c for c in cells):
                rows_out.append(cells)
        return rows_out

    try:
        home_html = _fetch("/")
        # Find links to IPO/rights pages
        links = re.findall(r'href=["\']([^"\']*(?:ipo|rights|pipeline|approved)[^"\']*)["\']',
                           home_html, re.IGNORECASE)
        links = list(set(links))

        for link in links[:8]:
            if not link.startswith("/"):
                continue
            try:
                page_html = _fetch(link)
                rows = _parse_table(page_html, link)
                lower = link.lower()
                if "rights" in lower:
                    results["rights"].extend(rows[:50])
                elif "approved" in lower:
                    results["approved"].extend(rows[:50])
                else:
                    results["ipo"].extend(rows[:50])
            except Exception:
                pass

        # Also try known SEBON pages
        known_paths = [
            "/en/approved-issue-manager",
            "/en/market-statistics",
        ]
        for path in known_paths:
            try:
                page_html = _fetch(path)
                rows = _parse_table(page_html, path)
                if rows:
                    results["approved"].extend(rows[:30])
            except Exception:
                pass

        # Deduplicate
        for key in ("ipo","rights","approved"):
            seen = set()
            unique = []
            for row in results[key]:
                k = tuple(row[:3])
                if k not in seen:
                    seen.add(k)
                    unique.append(row)
            results[key] = unique

        return jresp(results)

    except Exception as e:
        results["error"] = str(e)
        return jresp(results)


# ── Enhanced Screener with Candlestick ───────────────────────────────────────
@app.route("/api/screener/candlestick")
def api_screener_candlestick():
    """
    Screen for stocks where current candlestick pattern + technicals
    indicate a good entry point.
    """
    source      = request.args.get("source","auto")
    sector_filt = request.args.get("sector","")
    pattern_sig = request.args.get("pattern_signal","BUY")  # BUY/SELL/ALL
    min_score   = int(request.args.get("min_score","55") or 55)

    prices, src_used = fetch_market_today(source)
    if not prices:
        return jresp({"error":"Market data unavailable","results":[]})

    candidates = []
    for p in prices:
        sec = SYM_SEC.get(p.get("symbol",""),"Other")
        p["sector"] = sec
        if sector_filt and sec != sector_filt: continue
        ltp = p.get("ltp",0)
        if not ltp: continue
        candidates.append(p)

    from concurrent.futures import ThreadPoolExecutor, as_completed
    from data_fetcher import merolagani_history

    def _enrich_cs(p):
        sym = p.get("symbol","")
        try:
            hist = merolagani_history(sym,
                (datetime.now()-timedelta(days=180)).strftime("%Y-%m-%d"),
                datetime.now().strftime("%Y-%m-%d"))
            if len(hist) < 20:
                return None
            a = analyze(hist, p)
            if a.get("error"):
                return None

            patterns  = a.get("patterns", [])
            score     = a.get("score", 50)
            proj      = a.get("projection","HOLD")
            indicators = a.get("indicators",{}).get("current",{})

            # Current pattern signal
            current_pattern = patterns[-1] if patterns else {}
            pat_signal = current_pattern.get("signal","HOLD")

            if pattern_sig != "ALL" and pat_signal != pattern_sig:
                return None
            if score < min_score and pattern_sig == "BUY":
                return None

            # Entry timing analysis
            rsi     = indicators.get("rsi14", 50)
            adx     = indicators.get("adx", 0)
            stoch_k = indicators.get("stoch_k", 50)
            trend   = indicators.get("trend_strength","Weak")

            # Timing score (0-100)
            timing = 50
            if rsi < 35: timing += 15
            elif rsi < 45: timing += 8
            if adx > 25: timing += 10
            if stoch_k < 25: timing += 12
            elif stoch_k > 75: timing -= 12
            if "BUY" in proj: timing += 10
            timing = max(0, min(100, timing))

            return {
                "symbol":           sym,
                "sector":           p.get("sector",""),
                "ltp":              p.get("ltp",0),
                "change_pct":       p.get("change_pct",0),
                "volume":           p.get("volume",0),
                "signal":           proj,
                "score":            round(score,1),
                "timing_score":     timing,
                "pattern":          current_pattern.get("pattern","—"),
                "pattern_signal":   pat_signal,
                "pattern_desc":     current_pattern.get("description",""),
                "rsi":              round(rsi,1),
                "adx":              round(adx,1),
                "stoch_k":          round(stoch_k,1),
                "trend_strength":   trend,
                "entry_timing":     ("GOOD ENTRY"    if timing >= 65 else
                                     "WATCH"         if timing >= 50 else
                                     "AVOID"),
            }
        except Exception:
            return None

    results = []
    workers = min(os.cpu_count() or 4, 8)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_enrich_cs, p): p for p in candidates[:40]}
        for fut in as_completed(futs):
            try:
                r = fut.result()
                if r: results.append(r)
            except Exception:
                pass

    results.sort(key=lambda x: x.get("timing_score",0), reverse=True)
    return jresp({"results": results[:60], "count": len(results), "source": src_used})


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port  = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
