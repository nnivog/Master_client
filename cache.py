"""
NEPSE Tunnel — Hybrid Floorsheet Cache v2
==========================================
Architecture:
  A. Storage      → CSV.GZ files, one per trading date
                    Path: <base>/cache/floorsheet/YYYY/MM/YYYY-MM-DD.csv.gz
  B. Metadata     → JSON tracker (cache_meta.json) — fast lookup without
                    opening any data file
                    Fields: {date: {rows, fetched_at, size_bytes, checksum}}
  C. Dedup        → pandas drop_duplicates on (txn_no, buyer, seller, qty)
                    unique_key = sha1(txn_no+buyer+seller+qty+symbol)
  D. Incremental  → only missing dates are fetched; never overwrites good data
  E. Append-safe  → if a date file exists, new rows are merged+deduped, never deleted
  F. Background   → auto-fetch runs in a daemon thread; UI polls progress
  G. No pyarrow   → plain CSV.GZ (gzip+csv), works with pandas stdlib

File layout (next to .exe):
  cache/
    floorsheet/
      2025/
        09/
          2025-09-18.csv.gz
          2025-09-21.csv.gz
      2026/
        03/
          2026-03-17.csv.gz
    cache_meta.json
    cache_activity.log
"""

import os, sys, gzip, csv, json, io, hashlib, threading, time, logging
from datetime import datetime, timedelta

log = logging.getLogger("nepse.cache")

# ── Base path — always next to the exe / script ──────────────────
if getattr(sys, 'frozen', False):
    _BASE = os.path.dirname(sys.executable)
else:
    _BASE = os.path.dirname(os.path.abspath(__file__))

# Cloud deployments: set NEPSE_DATA_DIR=/data to redirect all writes
# to a persistent volume. Falls back to _BASE for local/EXE usage.
_CACHE_ROOT = os.environ.get("NEPSE_DATA_DIR", "").strip() or _BASE

CACHE_DIR  = os.path.join(_CACHE_ROOT, "cache", "floorsheet")
META_FILE  = os.path.join(_CACHE_ROOT, "cache", "cache_meta.json")
ACTLOG     = os.path.join(_CACHE_ROOT, "cache", "cache_activity.log")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(os.path.dirname(META_FILE), exist_ok=True)

# ── CSV columns (canonical order) ────────────────────────────────
COLS = ["unique_key", "date", "txn_no", "symbol", "buyer", "seller",
        "qty", "rate", "amount"]

# ── Metadata lock ────────────────────────────────────────────────
_META_LOCK = threading.Lock()


# ════════════════════════════════════════════════════════════════
#  METADATA  (JSON tracker — fast O(1) has/stats without reading files)
# ════════════════════════════════════════════════════════════════

def _load_meta():
    try:
        with open(META_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_meta(meta):
    try:
        tmp = META_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
        os.replace(tmp, META_FILE)
    except Exception as e:
        log.error(f"meta save error: {e}")

def _meta_key(date_str, symbol=""):
    return f"{date_str}|{(symbol or '').upper()}"


# ════════════════════════════════════════════════════════════════
#  FILE PATH helpers
# ════════════════════════════════════════════════════════════════

def _date_path(date_str, symbol=""):
    """
    Returns path to the CSV.GZ file for this date+symbol.
    Full market (symbol='') → YYYY/MM/YYYY-MM-DD.csv.gz
    Filtered symbol         → YYYY/MM/YYYY-MM-DD_SYM.csv.gz
    """
    y, m, _ = date_str.split("-")
    fname = f"{date_str}.csv.gz" if not symbol else f"{date_str}_{symbol.upper()}.csv.gz"
    return os.path.join(CACHE_DIR, y, m, fname)


# ════════════════════════════════════════════════════════════════
#  UNIQUE KEY per row  (dedup fingerprint)
# ════════════════════════════════════════════════════════════════

def _make_key(row):
    """SHA1 of (txn_no, symbol, buyer, seller, qty) — stable unique ID."""
    parts = "|".join([
        str(row.get("txn_no",  "") or ""),
        str(row.get("symbol",  "") or ""),
        str(row.get("buyer",   "") or ""),
        str(row.get("seller",  "") or ""),
        str(int(float(row.get("qty", 0) or 0))),
    ])
    return hashlib.sha1(parts.encode()).hexdigest()[:16]


# ════════════════════════════════════════════════════════════════
#  WRITE  (append + dedup, never delete)
# ════════════════════════════════════════════════════════════════

def cache_set(date_str, symbol, rows):
    """
    Write rows to CSV.GZ for (date, symbol).
    If file already exists, merges new rows with existing and deduplicates.
    Unique key = sha1(txn_no+symbol+buyer+seller+qty).
    Never deletes existing data — only appends/merges.
    """
    if not rows:
        return 0

    import pandas as pd

    path = _date_path(date_str, symbol)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Tag each row with unique_key and date
    new_records = []
    for r in rows:
        rec = {c: r.get(c, "") for c in COLS[1:]}   # all except unique_key
        rec["date"]       = date_str
        rec["unique_key"] = _make_key(r)
        new_records.append(rec)

    new_df = pd.DataFrame(new_records, columns=COLS)

    # Merge with existing file if it exists
    if os.path.exists(path):
        try:
            existing_df = _read_gz(path)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        except Exception:
            combined_df = new_df
    else:
        combined_df = new_df

    # Deduplicate on unique_key
    combined_df = combined_df.drop_duplicates(subset=["unique_key"], keep="last")
    combined_df = combined_df.reset_index(drop=True)

    # Write compressed
    _write_gz(path, combined_df)

    row_count  = len(combined_df)
    size_bytes = os.path.getsize(path)

    # Update metadata
    with _META_LOCK:
        meta = _load_meta()
        meta[_meta_key(date_str, symbol)] = {
            "date":       date_str,
            "symbol":     (symbol or "").upper(),
            "rows":       row_count,
            "fetched_at": datetime.now().isoformat(),
            "size_bytes": size_bytes,
            "path":       os.path.abspath(path),
        }
        _save_meta(meta)

    _activity_log("CACHE SET",
                  f"{date_str} sym={symbol or '(all)'} rows={row_count} "
                  f"size={round(size_bytes/1024,1)}KB")
    return row_count


# ════════════════════════════════════════════════════════════════
#  READ
# ════════════════════════════════════════════════════════════════

def cache_get(date_str, symbol=""):
    """Return list of row dicts for (date, symbol), or None if not cached."""
    path = _date_path(date_str, symbol)
    if not os.path.exists(path):
        return None
    try:
        df = _read_gz(path)
        # Return as list of dicts (drop unique_key — internal use only)
        df2 = df.drop(columns=["unique_key"], errors="ignore")
        return df2.to_dict("records")
    except Exception:
        return None

def cache_has(date_str, symbol=""):
    """O(1) check via metadata JSON — no file I/O."""
    with _META_LOCK:
        meta = _load_meta()
    return _meta_key(date_str, symbol) in meta


# ════════════════════════════════════════════════════════════════
#  GZ helpers
# ════════════════════════════════════════════════════════════════

def _read_gz(path):
    import pandas as pd
    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        return pd.read_csv(f, dtype=str)

def _write_gz(path, df):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=6, newline="") as f:
        f.write(buf.getvalue())


# ════════════════════════════════════════════════════════════════
#  STATS
# ════════════════════════════════════════════════════════════════

def cache_stats():
    """Return dict with cache statistics — reads only metadata, no file scanning."""
    with _META_LOCK:
        meta = _load_meta()

    entries     = [v for v in meta.values() if isinstance(v, dict)]
    total_rows  = sum(e.get("rows", 0)       for e in entries)
    total_bytes = sum(e.get("size_bytes", 0) for e in entries)
    dates       = sorted(set(e.get("date","") for e in entries if e.get("date")))

    # Recent activity log (last 100 lines)
    log_lines = []
    try:
        with open(ACTLOG, "r", encoding="utf-8") as f:
            log_lines = f.readlines()[-100:]
        log_lines = [l.rstrip() for l in reversed(log_lines)]
    except Exception:
        pass

    return {
        "total_entries":  len(entries),
        "total_rows":     total_rows,
        "date_min":       dates[0]  if dates else "",
        "date_max":       dates[-1] if dates else "",
        "db_size_kb":     round(total_bytes / 1024, 1),
        "db_path":        os.path.join(_BASE, "cache"),
        "log":            [{"ts": l[:19], "action": l[20:50].strip(),
                            "detail": l[51:].strip(), "records": 0}
                           for l in log_lines if l],
    }


# ════════════════════════════════════════════════════════════════
#  CLEAR  (selective — never blindly deletes all)
# ════════════════════════════════════════════════════════════════

def cache_clear(symbol=None, date_str=None):
    """
    Remove cache entries.
    Deletes the .csv.gz file AND removes from metadata.
    Old data is never auto-deleted during updates — only via this call.
    """
    with _META_LOCK:
        meta = _load_meta()
        keys_to_del = []

        for k, v in meta.items():
            if not isinstance(v, dict):
                continue
            if symbol is not None and v.get("symbol","") != symbol.upper():
                continue
            if date_str is not None and v.get("date","") != date_str:
                continue
            # Delete the file — path stored as absolute since fix
            try:
                path = v.get("path", "")
                # Support both old relative paths and new absolute paths
                if path and not os.path.isabs(path):
                    path = os.path.join(_BASE, path)
                if path and os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass
            keys_to_del.append(k)

        for k in keys_to_del:
            del meta[k]
        _save_meta(meta)

    detail = f"symbol={symbol}" if symbol else (f"date={date_str}" if date_str else "ALL")
    _activity_log("CACHE CLEAR", f"{detail} — {len(keys_to_del)} entries removed")
    return True


def cache_clear_log():
    """Clear the activity log file."""
    try:
        open(ACTLOG, "w").close()
        return True
    except Exception:
        return False


# ════════════════════════════════════════════════════════════════
#  TRADING DAY HELPERS
# ════════════════════════════════════════════════════════════════

def trading_days_range(from_date, to_date):
    """NEPSE trading days (Sun–Thu) in [from_date, to_date]."""
    result = []
    d   = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date,   "%Y-%m-%d")
    while d <= end:
        if d.weekday() not in (4, 5):   # skip Fri, Sat
            result.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return result

def last_6_months_trading_days():
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    six_ago   = (datetime.now() - timedelta(days=183)).strftime("%Y-%m-%d")
    return trading_days_range(six_ago, yesterday)

def uncached_days(symbol=""):
    """Trading days in last 6 months not yet in cache."""
    return [d for d in last_6_months_trading_days() if not cache_has(d, symbol)]


# ════════════════════════════════════════════════════════════════
#  ACTIVITY LOG  (append-only text file — never cleared automatically)
# ════════════════════════════════════════════════════════════════

def _activity_log(action, detail=""):
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} | {action:<24} | {detail}\n"
        with open(ACTLOG, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════
#  BACKGROUND AUTO-FETCH ENGINE
#  Runs as a daemon thread — fetches one date at a time with
#  polite delays, retries, and never re-fetches already-cached dates.
#  Survives app restarts: resumes from the last uncached date.
# ════════════════════════════════════════════════════════════════

_bg_state = {
    "running":   False,
    "total":     0,
    "done":      0,
    "skipped":   0,
    "errors":    0,
    "last_date": "",
    "status":    "idle",    # idle | running | paused | done | cancelled
    "pct":       0,
    "started":   "",
    "finished":  "",
    "message":   "",
    "job_id":    "",
}
_bg_lock   = threading.Lock()
_bg_cancel = threading.Event()


def bg_start(symbol="", delay=1.5, retry_delay=15.0, max_retries=3):
    """
    Start background cache update.
    delay        = base seconds between dates (actual is randomised ±30%)
    retry_delay  = seconds before retrying a failed date (multiplied per attempt)
    max_retries  = date-level attempts before marking as skipped
    Page-level retries + connection-reset recovery are handled inside
    merolagani_floorsheet_search itself.
    """
    global _bg_state
    with _bg_lock:
        if _bg_state["status"] == "running":
            return {"already_running": True, **_bg_state}

    missing = uncached_days(symbol)
    if not missing:
        with _bg_lock:
            _bg_state.update({"status": "done", "message": "Already up to date.",
                               "total": 0, "done": 0})
        return {**_bg_state, "dates_needed": 0}

    job_id = datetime.now().strftime("%H%M%S%f")
    _bg_cancel.clear()

    est_min = round(len(missing) * (delay + 1.5) / 60, 1)
    with _bg_lock:
        _bg_state.update({
            "running": True, "total": len(missing), "done": 0,
            "skipped": 0, "errors": 0, "last_date": "",
            "status": "running", "pct": 0,
            "started": datetime.now().isoformat(),
            "finished": "", "job_id": job_id,
            "message": f"Fetching {len(missing)} dates — est. ~{est_min} min "
                       f"(NepalStock API if available, else MeroLagani)",
        })

    def _worker():
        import random as _random
        from data_fetcher import (merolagani_floorsheet_by_date,
                                   nepalstock_floorsheet,
                                   nepalstock_floorsheet_available)
        _activity_log("BG START", f"symbol={symbol or '(all)'} dates={len(missing)}")

        # Check if NepalStock API is reachable — use it if available (10x faster)
        use_ns = nepalstock_floorsheet_available()
        src    = "NepalStock API" if use_ns else "MeroLagani"
        _activity_log("BG SOURCE", f"Primary source: {src}")

        for i, ds in enumerate(missing):
            if _bg_cancel.is_set():
                with _bg_lock:
                    _bg_state.update({"status": "cancelled", "running": False,
                                      "finished": datetime.now().isoformat()})
                _activity_log("BG CANCELLED", f"after {i} dates")
                return

            fetched = False
            for attempt in range(max_retries):
                if _bg_cancel.is_set():
                    break
                try:
                    if use_ns:
                        # NepalStock JSON API — fast parallel fetch
                        rows = nepalstock_floorsheet(ds, symbol=symbol)
                        if not rows and attempt == 0:
                            # API returned nothing — could be holiday or API down
                            # Try MeroLagani as immediate fallback
                            rows = merolagani_floorsheet_by_date(
                                symbol=symbol, buyer="", seller="", date_str=ds)
                    else:
                        rows = merolagani_floorsheet_by_date(
                            symbol=symbol, buyer="", seller="", date_str=ds)

                    if rows:
                        cache_set(ds, symbol, rows)
                        fetched = True
                        _activity_log("BG CACHED",
                                      f"{ds} rows={len(rows)} src={src}")
                        break

                    wait = retry_delay * (attempt + 1)
                    _activity_log("BG RETRY",
                                  f"{ds} attempt={attempt+1} — 0 rows, wait {wait}s")
                    if attempt < max_retries - 1:
                        time.sleep(wait)

                except Exception as e:
                    err_type = type(e).__name__
                    wait = retry_delay * (2 ** attempt)
                    _activity_log("BG ERROR",
                                  f"{ds} attempt={attempt+1} [{err_type}] "
                                  f"{str(e)[:60]} — wait {wait}s")
                    if attempt < max_retries - 1:
                        time.sleep(wait)

            with _bg_lock:
                _bg_state["done"]      += 1
                _bg_state["last_date"]  = ds
                _bg_state["pct"]        = round((i + 1) / len(missing) * 100, 1)
                if not fetched:
                    _bg_state["skipped"] += 1

            # Inter-date delay — NepalStock API handles parallel well, ML needs breathing room
            base_delay = 0.5 if use_ns else delay
            jitter = _random.uniform(base_delay * 0.7, base_delay * 1.3)
            time.sleep(jitter)

        with _bg_lock:
            _bg_state.update({
                "status": "done", "running": False,
                "finished": datetime.now().isoformat(),
                "message": f"Complete — {_bg_state['done']} dates, "
                           f"{_bg_state['skipped']} skipped",
            })
        _activity_log("BG DONE",
                      f"done={_bg_state['done']} skipped={_bg_state['skipped']}")

    threading.Thread(target=_worker, daemon=True, name="nepse-cache-bg").start()
    return {**_bg_state, "dates_needed": len(missing)}


def bg_cancel():
    """Signal the background worker to stop after the current date."""
    _bg_cancel.set()
    with _bg_lock:
        if _bg_state["status"] == "running":
            _bg_state["status"] = "cancelling"

def bg_state():
    """Return a copy of the current background worker state."""
    with _bg_lock:
        return dict(_bg_state)
