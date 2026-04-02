"""
NEPSE Portfolio Manager — Backend v29
=======================================
All fee rates stored in fee_settings table — fully dynamic.
FIFO cost basis includes buy fees. CGT calculated per-lot (short vs long term).
Idempotent CSV import via import_hash deduplication.
"""

import sqlite3, os, json, math, hashlib
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional

import sys as _sys

def _resolve_data_dir() -> str:
    """
    Return the directory where portfolio databases live.
    Priority:
      1. NEPSE_DATA_DIR environment variable  (cloud persistent volume e.g. /data)
      2. Next to the script / executable       (local dev & Windows EXE)
    """
    env = os.environ.get("NEPSE_DATA_DIR", "").strip()
    if env:
        os.makedirs(env, exist_ok=True)
        return env
    if getattr(_sys, 'frozen', False):
        return os.path.dirname(_sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

_DATA_DIR = _resolve_data_dir()
DB_PATH   = os.path.join(_DATA_DIR, "portfolio.db")

_DEFAULT_FEES = {
    "sebon_rate":         0.00015,
    "nepse_rate":         0.00015,
    "cds_rate":           0.00015,
    "dp_charge":          25.0,
    "vat_rate":           0.13,
    "cgt_short":          0.05,
    "cgt_long":           0.075,
    "cgt_threshold_days": 365,
    "broker_slabs": [
        [50000,    0.0040],
        [500000,   0.0037],
        [2000000,  0.0034],
        [10000000, 0.0030],
        [None,     0.0027],
    ],
}

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS transactions (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol        TEXT    NOT NULL,
        company_name  TEXT    DEFAULT '',
        tx_type       TEXT    NOT NULL,
        share_type    TEXT    DEFAULT 'Secondary',
        quantity      REAL    NOT NULL,
        rate          REAL    NOT NULL,
        date          TEXT    NOT NULL,
        tx_value      REAL    NOT NULL,
        broker_comm   REAL    DEFAULT 0,
        vat_broker    REAL    DEFAULT 0,
        sebon_fee     REAL    DEFAULT 0,
        nepse_fee     REAL    DEFAULT 0,
        cds_fee       REAL    DEFAULT 0,
        dp_charge     REAL    DEFAULT 0,
        total_charges REAL    DEFAULT 0,
        net_value     REAL    NOT NULL,
        include_dp    INTEGER DEFAULT 1,
        notes         TEXT    DEFAULT '',
        import_hash   TEXT    DEFAULT '',
        created_at    TEXT    DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS watchlist (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol     TEXT    NOT NULL UNIQUE,
        target     REAL    DEFAULT 0,
        stop_loss  REAL    DEFAULT 0,
        notes      TEXT    DEFAULT '',
        added_at   TEXT    DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS cash_ledger (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        date       TEXT    NOT NULL,
        type       TEXT    NOT NULL,
        amount     REAL    NOT NULL,
        notes      TEXT    DEFAULT '',
        created_at TEXT    DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS fee_settings (
        key        TEXT    PRIMARY KEY,
        value      TEXT    NOT NULL,
        updated_at TEXT    DEFAULT (datetime('now'))
    );
    """)
    try:
        c.execute("ALTER TABLE transactions ADD COLUMN import_hash TEXT DEFAULT ''")
        conn.commit()
    except Exception:
        pass
    conn.commit()
    conn.close()
    _seed_default_fees()

def _seed_default_fees():
    conn = get_db()
    defaults = {
        "sebon_rate":         str(_DEFAULT_FEES["sebon_rate"]),
        "nepse_rate":         str(_DEFAULT_FEES["nepse_rate"]),
        "cds_rate":           str(_DEFAULT_FEES["cds_rate"]),
        "dp_charge":          str(_DEFAULT_FEES["dp_charge"]),
        "vat_rate":           str(_DEFAULT_FEES["vat_rate"]),
        "cgt_short":          str(_DEFAULT_FEES["cgt_short"]),
        "cgt_long":           str(_DEFAULT_FEES["cgt_long"]),
        "cgt_threshold_days": str(_DEFAULT_FEES["cgt_threshold_days"]),
        "broker_slabs":       json.dumps(_DEFAULT_FEES["broker_slabs"]),
    }
    for k, v in defaults.items():
        conn.execute("INSERT OR IGNORE INTO fee_settings (key, value) VALUES (?,?)", (k, v))
    conn.commit()
    conn.close()

def get_fee_settings() -> dict:
    conn = get_db()
    rows = conn.execute("SELECT key, value FROM fee_settings").fetchall()
    conn.close()
    cfg = {}
    for r in rows:
        k, v = r["key"], r["value"]
        try:
            cfg[k] = json.loads(v)
        except Exception:
            try:
                cfg[k] = float(v)
            except Exception:
                cfg[k] = v
    for k, v in _DEFAULT_FEES.items():
        if k not in cfg:
            cfg[k] = v
    return cfg

def update_fee_settings(updates: dict) -> dict:
    conn = get_db()
    for k, v in updates.items():
        val = json.dumps(v) if isinstance(v, (list, dict)) else str(v)
        conn.execute(
            "INSERT OR REPLACE INTO fee_settings (key, value, updated_at) VALUES (?,?,datetime('now'))",
            (k, val)
        )
    conn.commit()
    conn.close()
    return get_fee_settings()

def broker_commission_rate(value: float, slabs=None) -> float:
    if slabs is None:
        slabs = get_fee_settings()["broker_slabs"]
    for max_val, rate in slabs:
        if max_val is None or value <= max_val:
            return rate
    return slabs[-1][1]

def calc_transaction_costs(value: float, tx_type: str, include_dp: bool = True,
                            fees: dict = None) -> dict:
    if fees is None:
        fees = get_fee_settings()
    tx_type       = tx_type.upper()
    broker_rate   = broker_commission_rate(value, fees["broker_slabs"])
    broker_comm   = round(value * broker_rate, 2)
    vat_on_broker = round(broker_comm * fees["vat_rate"], 2)
    sebon         = round(value * fees["sebon_rate"], 2)
    nepse_fee     = round(value * fees["nepse_rate"], 2)
    cds_fee       = round(value * fees["cds_rate"], 2) if tx_type == 'SELL' else 0.0
    dp_charge     = fees["dp_charge"] if (tx_type == 'SELL' and include_dp) else 0.0
    total_charges = round(broker_comm + vat_on_broker + sebon + nepse_fee + cds_fee + dp_charge, 2)
    net_value     = round(value + total_charges, 2) if tx_type == 'BUY' else round(value - total_charges, 2)
    return {
        "transaction_value": round(value, 2),
        "broker_rate_pct":   round(broker_rate * 100, 4),
        "broker_commission": broker_comm,
        "vat_on_broker":     vat_on_broker,
        "sebon_fee":         sebon,
        "nepse_fee":         nepse_fee,
        "cds_fee":           cds_fee,
        "dp_charge":         dp_charge,
        "total_charges":     total_charges,
        "net_value":         net_value,
    }

def calc_capital_gains_tax(buy_date: str, sell_date: str, profit: float,
                            fees: dict = None) -> dict:
    if fees is None:
        fees = get_fee_settings()
    if profit <= 0:
        return {"tax": 0.0, "rate": 0.0, "holding_days": 0, "term": "N/A"}
    try:
        bd = datetime.strptime(buy_date[:10], "%Y-%m-%d").date()
        sd = datetime.strptime(sell_date[:10], "%Y-%m-%d").date()
        holding = (sd - bd).days
    except Exception:
        holding = 0
    threshold = int(fees.get("cgt_threshold_days", 365))
    rate = fees["cgt_long"] if holding >= threshold else fees["cgt_short"]
    tax  = round(profit * rate, 2)
    return {
        "tax":          tax,
        "rate":         rate,
        "holding_days": holding,
        "term":         f"Long-term (≥{threshold}d)" if holding >= threshold else f"Short-term (<{threshold}d)",
    }

def _tx_costs(tx_type: str, quantity: float, rate: float, include_dp: bool = True,
              fees: dict = None) -> dict:
    if fees is None:
        fees = get_fee_settings()
    tx_type  = tx_type.upper()
    tx_value = round(quantity * rate, 2)
    if tx_type in ("BUY", "SELL"):
        return calc_transaction_costs(tx_value, tx_type, include_dp, fees)
    elif tx_type == "BONUS":
        return {"transaction_value": 0, "broker_commission": 0, "vat_on_broker": 0,
                "sebon_fee": 0, "nepse_fee": 0, "cds_fee": 0, "dp_charge": 0,
                "total_charges": 0, "net_value": 0, "broker_rate_pct": 0}
    elif tx_type == "RIGHT":
        sebon = round(tx_value * fees["sebon_rate"], 2)
        return {"transaction_value": tx_value, "broker_commission": 0, "vat_on_broker": 0,
                "sebon_fee": sebon, "nepse_fee": 0, "cds_fee": 0, "dp_charge": 0,
                "total_charges": sebon, "net_value": round(tx_value + sebon, 2), "broker_rate_pct": 0}
    else:
        return {"transaction_value": tx_value, "broker_commission": 0, "vat_on_broker": 0,
                "sebon_fee": 0, "nepse_fee": 0, "cds_fee": 0, "dp_charge": 0,
                "total_charges": 0, "net_value": tx_value, "broker_rate_pct": 0}

def _make_import_hash(symbol: str, tx_type: str, quantity: float,
                      rate: float, date_str: str) -> str:
    raw = f"{symbol}|{tx_type}|{round(abs(quantity), 4)}|{round(rate, 4)}|{date_str}"
    return hashlib.md5(raw.encode()).hexdigest()

def add_transaction(symbol: str, tx_type: str, quantity: float, rate: float,
                    date_str: str, share_type: str = "Secondary",
                    include_dp: bool = True, notes: str = "",
                    company_name: str = "",
                    import_hash: str = "",
                    skip_if_duplicate: bool = False) -> dict:
    symbol   = symbol.upper().strip()
    tx_type  = tx_type.upper()
    quantity = abs(quantity)
    if tx_type == "BONUS":
        rate = 0.0
        if share_type in ("Secondary", ""):
            share_type = "Bonus"
    if tx_type == "RIGHT" and share_type in ("Secondary", ""):
        share_type = "Right"
    fees     = get_fee_settings()
    tx_value = round(quantity * rate, 2)
    costs    = _tx_costs(tx_type, quantity, rate, include_dp, fees)
    if skip_if_duplicate and import_hash:
        conn = get_db()
        exists = conn.execute(
            "SELECT id FROM transactions WHERE import_hash=? AND import_hash != ''",
            (import_hash,)
        ).fetchone()
        conn.close()
        if exists:
            return {"id": exists["id"], "skipped": True, "costs": costs}
    conn = get_db()
    conn.execute("""
        INSERT INTO transactions
        (symbol, company_name, tx_type, share_type, quantity, rate, date,
         tx_value, broker_comm, vat_broker, sebon_fee, nepse_fee, cds_fee,
         dp_charge, total_charges, net_value, include_dp, notes, import_hash)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (symbol, company_name, tx_type, share_type, quantity, rate, date_str,
          tx_value, costs["broker_commission"], costs["vat_on_broker"],
          costs["sebon_fee"], costs["nepse_fee"], costs["cds_fee"],
          costs["dp_charge"], costs["total_charges"], costs["net_value"],
          1 if include_dp else 0, notes, import_hash))
    conn.commit()
    tx_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return {"id": tx_id, "costs": costs}

def get_transactions(symbol: str = "", tx_type: str = "",
                     from_date: str = "", to_date: str = "") -> list:
    conn = get_db()
    q = "SELECT * FROM transactions WHERE 1=1"
    params = []
    if symbol:    q += " AND symbol=?";    params.append(symbol.upper())
    if tx_type:   q += " AND tx_type=?";   params.append(tx_type.upper())
    if from_date: q += " AND date>=?";     params.append(from_date)
    if to_date:   q += " AND date<=?";     params.append(to_date)
    q += " ORDER BY date DESC, id DESC"
    rows = [dict(r) for r in conn.execute(q, params).fetchall()]
    conn.close()
    return rows

def delete_transaction(tx_id: int):
    conn = get_db()
    conn.execute("DELETE FROM transactions WHERE id=?", (tx_id,))
    conn.commit()
    conn.close()

def clear_all_transactions() -> int:
    """Delete ALL transactions. Returns count deleted. Use before re-import."""
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    conn.execute("DELETE FROM transactions")
    conn.commit()
    conn.close()
    return count

def update_transaction(tx_id: int, **kwargs):
    conn = get_db()
    row = conn.execute("SELECT * FROM transactions WHERE id=?", (tx_id,)).fetchone()
    if not row: conn.close(); return
    row = dict(row)
    for k, v in kwargs.items():
        if k in row: row[k] = v
    fees     = get_fee_settings()
    tx_value = round(row["quantity"] * row["rate"], 2)
    if row["tx_type"] in ("BUY", "SELL"):
        costs = calc_transaction_costs(tx_value, row["tx_type"], bool(row["include_dp"]), fees)
    else:
        costs = {"broker_commission": 0, "vat_on_broker": 0, "sebon_fee": 0, "nepse_fee": 0,
                 "cds_fee": 0, "dp_charge": 0, "total_charges": 0, "net_value": tx_value}
    conn.execute("""
        UPDATE transactions SET symbol=?,company_name=?,tx_type=?,share_type=?,quantity=?,
        rate=?,date=?,tx_value=?,broker_comm=?,vat_broker=?,sebon_fee=?,nepse_fee=?,
        cds_fee=?,dp_charge=?,total_charges=?,net_value=?,include_dp=?,notes=?
        WHERE id=?
    """, (row["symbol"], row["company_name"], row["tx_type"], row["share_type"],
          row["quantity"], row["rate"], row["date"], tx_value,
          costs["broker_commission"], costs["vat_on_broker"], costs["sebon_fee"],
          costs["nepse_fee"], costs["cds_fee"], costs["dp_charge"],
          costs["total_charges"], costs["net_value"], row["include_dp"], row["notes"], tx_id))
    conn.commit()
    conn.close()

def get_holdings() -> List[Dict]:
    """
    FIFO holdings. Investment = buy cost (with buy fees) + sell fees paid on consumed lots.
    Sell fees on partially-consumed lots are proportionally added to remaining cost basis.
    """
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM transactions WHERE tx_type IN ('BUY','BONUS','RIGHT','SELL')"
        " ORDER BY date ASC, id ASC"
    ).fetchall()
    conn.close()

    fifo: Dict[str, list] = {}

    for r in rows:
        r   = dict(r)
        sym = r["symbol"]
        if sym not in fifo:
            fifo[sym] = []

        if r["tx_type"] in ("BUY", "BONUS", "RIGHT"):
            net_cost_per = r["net_value"] / r["quantity"] if r["quantity"] else 0
            fifo[sym].append({
                "qty":          r["quantity"],
                "rate":         r["rate"],
                "net_cost_per": net_cost_per,
                "net_value":    r["net_value"],
                "buy_date":     r["date"],
                "tx_id":        r["id"],
            })

        elif r["tx_type"] == "SELL":
            qty_to_sell  = abs(r["quantity"])
            sell_charges = r["total_charges"]   # fees paid on this sell
            total_sold   = abs(r["quantity"])
            sell_fee_per = sell_charges / total_sold if total_sold else 0

            while qty_to_sell > 0 and fifo.get(sym):
                lot  = fifo[sym][0]
                if lot["qty"] <= qty_to_sell:
                    qty_to_sell -= lot["qty"]
                    fifo[sym].pop(0)
                else:
                    consumed          = qty_to_sell
                    remaining         = lot["qty"] - consumed
                    lot["qty"]        = remaining
                    lot["net_value"]  = remaining * lot["net_cost_per"]
                    qty_to_sell       = 0

    holdings = []
    for sym, lots in fifo.items():
        total_qty = sum(l["qty"] for l in lots)
        if total_qty < 0.01:
            continue
        total_invested = sum(l["net_value"] for l in lots)
        avg_cost       = total_invested / total_qty if total_qty else 0
        holdings.append({
            "symbol":         sym,
            "quantity":       round(total_qty, 2),
            "avg_cost_price": round(avg_cost, 2),
            "total_invested": round(total_invested, 2),
            "lots":           lots,
            "earliest_buy":   lots[0]["buy_date"] if lots else "",
            "latest_buy":     lots[-1]["buy_date"] if lots else "",
        })
    holdings.sort(key=lambda x: x["symbol"])
    return holdings

def get_portfolio_summary(live_prices: dict = None) -> dict:
    holdings = get_holdings()
    fees     = get_fee_settings()
    conn     = get_db()
    all_txs  = conn.execute(
        "SELECT * FROM transactions WHERE tx_type IN ('BUY','BONUS','RIGHT','SELL')"
        " ORDER BY date ASC, id ASC"
    ).fetchall()
    conn.close()

    fifo2: Dict[str, list] = {}
    realized_pl     = 0.0
    cgt_total       = 0.0
    cgt_short_total = 0.0
    cgt_long_total  = 0.0

    for r in [dict(x) for x in all_txs]:
        sym = r["symbol"]
        if sym not in fifo2:
            fifo2[sym] = []
        if r["tx_type"] in ("BUY", "BONUS", "RIGHT"):
            ncp = r["net_value"] / r["quantity"] if r["quantity"] else 0
            fifo2[sym].append({"qty": r["quantity"], "net_cost_per": ncp, "buy_date": r["date"]})
        elif r["tx_type"] == "SELL":
            sell_receive = r["net_value"]
            qty_rem      = abs(r["quantity"])
            sell_date    = r["date"]
            cost_basis   = 0.0
            temp_lots    = []
            while qty_rem > 0 and fifo2.get(sym):
                lot  = fifo2[sym][0]
                take = min(lot["qty"], qty_rem)
                cost_basis  += take * lot["net_cost_per"]
                temp_lots.append({"qty": take, "buy_date": lot["buy_date"]})
                qty_rem     -= take
                lot["qty"]  -= take
                if lot["qty"] < 0.01:
                    fifo2[sym].pop(0)
            profit = sell_receive - cost_basis
            realized_pl += profit
            if profit > 0 and temp_lots:
                total_take = sum(l["qty"] for l in temp_lots)
                for tl in temp_lots:
                    lot_profit = profit * (tl["qty"] / total_take) if total_take else 0
                    cgt_info   = calc_capital_gains_tax(tl["buy_date"], sell_date, lot_profit, fees)
                    cgt_total       += cgt_info["tax"]
                    if "Long" in cgt_info["term"]:
                        cgt_long_total  += cgt_info["tax"]
                    else:
                        cgt_short_total += cgt_info["tax"]

    total_invested   = sum(h["total_invested"] for h in holdings)
    total_market_val = 0.0
    unrealized_pl    = 0.0
    today_pl         = 0.0

    if live_prices:
        for h in holdings:
            sym = h["symbol"]
            raw = live_prices.get(sym, 0)
            if isinstance(raw, dict):
                ltp  = raw.get("ltp", 0) or 0
                prev = raw.get("prev_close", 0) or 0
                if ltp and not prev:
                    chg_pct = raw.get("change_pct", 0) or 0
                    if chg_pct:
                        prev = round(ltp / (1 + chg_pct / 100), 2)
            else:
                ltp  = float(raw or 0)
                prev = 0
            if ltp:
                mval = ltp * h["quantity"]
                h["ltp"]           = ltp
                h["prev_close"]    = prev
                h["market_value"]  = round(mval, 2)
                h["unrealized_pl"] = round(mval - h["total_invested"], 2)
                h["net_pl_pct"]    = round((mval - h["total_invested"]) / h["total_invested"] * 100, 2) if h["total_invested"] else 0
                h["daily_pl"]      = round((ltp - prev) * h["quantity"], 2) if prev else 0
                h["daily_pct"]     = round((ltp - prev) / prev * 100, 2) if prev else 0
                total_market_val  += mval
                unrealized_pl     += mval - h["total_invested"]
                today_pl          += h["daily_pl"]
            else:
                h["ltp"] = 0; h["prev_close"] = 0
                h["market_value"]  = h["total_invested"]
                h["unrealized_pl"] = 0; h["net_pl_pct"] = 0
                h["daily_pl"] = 0; h["daily_pct"] = 0

    net_worth = round(total_market_val or total_invested, 2)
    return {
        "holdings":           holdings,
        "total_invested":     round(total_invested, 2),
        "total_market_value": round(total_market_val, 2),
        "unrealized_pl":      round(unrealized_pl, 2),
        "unrealized_pct":     round(unrealized_pl / total_invested * 100, 2) if total_invested else 0,
        "realized_pl":        round(realized_pl, 2),
        "overall_pl":         round(realized_pl + unrealized_pl, 2),
        "today_pl":           round(today_pl, 2),
        "today_pl_pct":       round(today_pl / (total_market_val - today_pl) * 100, 2) if (total_market_val - today_pl) else 0,
        "cgt_estimate":       round(cgt_total, 2),
        "cgt_short_estimate": round(cgt_short_total, 2),
        "cgt_long_estimate":  round(cgt_long_total, 2),
        "net_worth":          net_worth,
        "total_stocks":       len(holdings),
        "fee_settings":       fees,
    }

def add_watchlist(symbol, target=0, stop_loss=0, notes=""):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO watchlist (symbol,target,stop_loss,notes) VALUES (?,?,?,?)",
                 (symbol.upper(), target, stop_loss, notes))
    conn.commit(); conn.close()

def get_watchlist():
    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM watchlist ORDER BY symbol").fetchall()]
    conn.close(); return rows

def remove_watchlist(symbol):
    conn = get_db()
    conn.execute("DELETE FROM watchlist WHERE symbol=?", (symbol.upper(),))
    conn.commit(); conn.close()

def add_cash(date_str, type_, amount, notes=""):
    conn = get_db()
    conn.execute("INSERT INTO cash_ledger (date,type,amount,notes) VALUES (?,?,?,?)",
                 (date_str, type_.upper(), amount, notes))
    conn.commit(); conn.close()

def get_cash_balance():
    conn = get_db()
    row = conn.execute(
        "SELECT SUM(CASE WHEN type='DEPOSIT' THEN amount WHEN type='WITHDRAW' THEN -amount ELSE amount END) as bal FROM cash_ledger"
    ).fetchone()
    conn.close()
    return round(row["bal"] or 0, 2)

def export_portfolio(path):
    conn = get_db()
    data = {
        "transactions": [dict(r) for r in conn.execute("SELECT * FROM transactions ORDER BY date").fetchall()],
        "watchlist":    [dict(r) for r in conn.execute("SELECT * FROM watchlist").fetchall()],
        "cash_ledger":  [dict(r) for r in conn.execute("SELECT * FROM cash_ledger ORDER BY date").fetchall()],
        "fee_settings": [dict(r) for r in conn.execute("SELECT * FROM fee_settings").fetchall()],
        "exported_at":  datetime.now().isoformat(),
    }
    conn.close()
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    return path

def import_portfolio(path):
    with open(path) as f:
        data = json.load(f)
    conn = get_db()
    imported = {"transactions": 0, "watchlist": 0, "cash_ledger": 0}
    for tx in data.get("transactions", []):
        tx.pop("id", None); tx.pop("created_at", None)
        keys = ",".join(tx.keys()); vals = ",".join(["?"] * len(tx))
        try:
            conn.execute(f"INSERT OR IGNORE INTO transactions ({keys}) VALUES ({vals})", list(tx.values()))
            imported["transactions"] += 1
        except Exception:
            pass
    for wl in data.get("watchlist", []):
        wl.pop("id", None); wl.pop("added_at", None)
        try:
            conn.execute("INSERT OR REPLACE INTO watchlist (symbol,target,stop_loss,notes) VALUES (?,?,?,?)",
                         (wl.get("symbol",""), wl.get("target",0), wl.get("stop_loss",0), wl.get("notes","")))
            imported["watchlist"] += 1
        except Exception:
            pass
    conn.commit(); conn.close()
    return imported

import re as _re

def _profile_db_path(profile_name):
    safe = _re.sub(r"[^\w\-]", "_", profile_name.strip())[:40]
    base = _DATA_DIR
    return os.path.join(base, f"portfolio_{safe}.db")

def list_profiles():
    base = _DATA_DIR
    profiles = ["Default"]
    try:
        for fname in sorted(os.listdir(base)):
            if fname.startswith("portfolio_") and fname.endswith(".db"):
                name = fname[len("portfolio_"):-len(".db")]
                if name and name != "Default":
                    profiles.append(name)
    except Exception:
        pass
    return profiles

def create_profile(profile_name):
    path = _profile_db_path(profile_name)
    old = globals()["DB_PATH"]
    globals()["DB_PATH"] = path
    init_db()
    globals()["DB_PATH"] = old
    return path

def delete_profile(profile_name):
    if profile_name in ("Default", "default", ""):
        return False
    path = _profile_db_path(profile_name)
    try:
        if os.path.exists(path):
            os.remove(path)
        return True
    except Exception:
        return False

def switch_profile(profile_name):
    global DB_PATH
    if profile_name in ("Default", "default", ""):
        DB_PATH = os.path.join(_DATA_DIR, "portfolio.db")
    else:
        DB_PATH = _profile_db_path(profile_name)
    init_db()
    return DB_PATH

init_db()
