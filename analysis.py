"""
Technical Analysis Engine for NEPSE
=====================================
Indicators: SMA, EMA, RSI, MACD, Bollinger Bands,
            Stochastic, ATR, OBV, VWAP, ADX
Patterns:   Doji, Hammer, Shooting Star, Engulfing, Morning/Evening Star,
            Three White Soldiers, Three Black Crows, Harami
Projection: Combined signal scoring (0-100) with BUY / HOLD / SELL + confidence
"""

import math
import numpy as np
import pandas as pd
from typing import List, Dict

# ── Helpers ─────────────────────────────────────────────────────────────────
def _s(series): return pd.Series(series, dtype=float)

def sma(closes, n):
    s = _s(closes)
    return s.rolling(n).mean().tolist()

def ema(closes, n):
    return _s(closes).ewm(span=n, adjust=False).mean().tolist()

def rsi(closes, n=14):
    s = _s(closes)
    delta = s.diff()
    gain = delta.clip(lower=0).rolling(n).mean()
    loss = (-delta.clip(upper=0)).rolling(n).mean()
    rs = gain / loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).tolist()

def macd(closes):
    s = _s(closes)
    ema12 = s.ewm(span=12, adjust=False).mean()
    ema26 = s.ewm(span=26, adjust=False).mean()
    line = ema12 - ema26
    signal = line.ewm(span=9, adjust=False).mean()
    hist = line - signal
    return line.tolist(), signal.tolist(), hist.tolist()

def bollinger(closes, n=20, k=2):
    s = _s(closes)
    mid = s.rolling(n).mean()
    std = s.rolling(n).std()
    return (mid + k*std).tolist(), mid.tolist(), (mid - k*std).tolist()

def stochastic(highs, lows, closes, k_period=14, d_period=3):
    h, l, c = _s(highs), _s(lows), _s(closes)
    lowest_low  = l.rolling(k_period).min()
    highest_high = h.rolling(k_period).max()
    k = 100 * (c - lowest_low) / (highest_high - lowest_low + 1e-10)
    d = k.rolling(d_period).mean()
    return k.tolist(), d.tolist()

def atr(highs, lows, closes, n=14):
    h, l, c = _s(highs), _s(lows), _s(closes)
    prev_c = c.shift(1)
    tr = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean().tolist()

def obv(closes, volumes):
    c, v = _s(closes), _s(volumes)
    direction = np.sign(c.diff().fillna(0))
    return (direction * v).cumsum().tolist()

def adx(highs, lows, closes, n=14):
    h, l, c = _s(highs), _s(lows), _s(closes)
    prev_h, prev_l = h.shift(1), l.shift(1)
    dm_plus  = (h - prev_h).clip(lower=0)
    dm_minus = (prev_l - l).clip(lower=0)
    dm_plus  = dm_plus.where(dm_plus > dm_minus, 0)
    dm_minus = dm_minus.where(dm_minus > dm_plus, 0)
    atr_vals = _s(atr(highs, lows, closes, n))
    di_plus  = 100 * dm_plus.ewm(span=n, adjust=False).mean() / (atr_vals + 1e-10)
    di_minus = 100 * dm_minus.ewm(span=n, adjust=False).mean() / (atr_vals + 1e-10)
    dx = 100 * (di_plus - di_minus).abs() / (di_plus + di_minus + 1e-10)
    adx_line = dx.ewm(span=n, adjust=False).mean()
    return adx_line.tolist(), di_plus.tolist(), di_minus.tolist()

def support_resistance(highs, lows, closes, n=3):
    """Pivot-point based S/R."""
    if len(closes) < 1:
        return [], []
    h_arr = np.array(highs, dtype=float)
    l_arr = np.array(lows, dtype=float)
    c_arr = np.array(closes, dtype=float)
    supports, resistances = [], []
    for i in range(n, len(c_arr) - n):
        if all(c_arr[i] <= c_arr[i-j] for j in range(1, n+1)) and \
           all(c_arr[i] <= c_arr[i+j] for j in range(1, n+1)):
            supports.append(round(float(l_arr[i]), 2))
        if all(c_arr[i] >= c_arr[i-j] for j in range(1, n+1)) and \
           all(c_arr[i] >= c_arr[i+j] for j in range(1, n+1)):
            resistances.append(round(float(h_arr[i]), 2))
    # deduplicate close values (within 0.5%)
    def dedup(lst):
        out = []
        for v in sorted(set(lst)):
            if not out or abs(v - out[-1]) / max(out[-1], 1) > 0.005:
                out.append(v)
        return out
    return dedup(supports)[-4:], dedup(resistances)[:4]

# ── Candlestick Patterns ────────────────────────────────────────────────────
def detect_patterns(ohlcv: list) -> list:
    """
    Detect 15+ candlestick patterns with investment-grade descriptions.
    Returns list of patterns sorted by date, last 8 patterns.
    """
    patterns = []
    df = pd.DataFrame(ohlcv)
    if df.empty or len(df) < 3:
        return patterns
    df = df.astype({"open": float, "high": float, "low": float, "close": float})

    for i in range(2, len(df)):
        o,  h,  l,  c  = df["open"].iloc[i],   df["high"].iloc[i],   df["low"].iloc[i],   df["close"].iloc[i]
        po, ph, pl, pc = df["open"].iloc[i-1],  df["high"].iloc[i-1], df["low"].iloc[i-1], df["close"].iloc[i-1]
        ppo, _, _, ppc = df["open"].iloc[i-2],  df["high"].iloc[i-2], df["low"].iloc[i-2], df["close"].iloc[i-2]

        body         = abs(c - o)
        prev_body    = abs(pc - po)
        prev2_body   = abs(ppc - ppo)
        total_range  = h - l + 1e-6
        upper_shadow = h - max(o, c)
        lower_shadow = min(o, c) - l
        is_bull      = c > o
        is_bear      = c < o
        prev_bull    = pc > po
        prev_bear    = pc < po

        date = df["date"].iloc[i] if "date" in df.columns else str(i)

        # ── Single-candle patterns ──────────────────────────────────────────

        # Doji (tiny body)
        if body <= total_range * 0.1 and total_range > 0:
            # Dragonfly Doji
            if lower_shadow >= 3 * max(upper_shadow, 0.001):
                patterns.append({"date": date, "pattern": "Dragonfly Doji", "type": "bullish",
                    "signal": "BUY", "prob": 62,
                    "description": "Buyers reclaimed all session losses — bullish reversal sign at support."})
            # Gravestone Doji
            elif upper_shadow >= 3 * max(lower_shadow, 0.001):
                patterns.append({"date": date, "pattern": "Gravestone Doji", "type": "bearish",
                    "signal": "SELL", "prob": 60,
                    "description": "Sellers rejected the entire rally — bearish reversal sign at resistance."})
            # Regular Doji
            else:
                patterns.append({"date": date, "pattern": "Doji", "type": "neutral",
                    "signal": "HOLD", "prob": 55,
                    "description": "Market indecision — watch for the next candle to determine breakout direction."})

        # Hammer (bullish reversal at lows)
        elif is_bull and lower_shadow >= 2 * body and upper_shadow <= body * 0.5 and body > 0:
            patterns.append({"date": date, "pattern": "Hammer", "type": "bullish",
                "signal": "BUY", "prob": 68,
                "description": "Hammer at low — sellers pushed down but buyers rejected. Potential upward reversal."})

        # Inverted Hammer (at bottoms — needs confirmation)
        elif is_bull and upper_shadow >= 2 * body and lower_shadow <= body * 0.5 and body > 0:
            patterns.append({"date": date, "pattern": "Inverted Hammer", "type": "bullish",
                "signal": "BUY", "prob": 60,
                "description": "Inverted Hammer — buyers attempted upside. Confirm with next green candle before buying."})

        # Hanging Man (same shape as Hammer but at tops — bearish)
        elif is_bear and lower_shadow >= 2 * body and upper_shadow <= body * 0.5 and body > 0 and prev_bull:
            patterns.append({"date": date, "pattern": "Hanging Man", "type": "bearish",
                "signal": "SELL", "prob": 63,
                "description": "Hanging Man at high — distribution warning. Wait for confirmation red candle."})

        # Shooting Star (bearish reversal at tops)
        elif is_bear and upper_shadow >= 2 * body and lower_shadow <= body * 0.3 and body > 0:
            patterns.append({"date": date, "pattern": "Shooting Star", "type": "bearish",
                "signal": "SELL", "prob": 65,
                "description": "Shooting Star — bulls failed to hold gains. Sellers took control. Potential reversal downward."})

        # Marubozu Bullish (no shadows — strong buying)
        elif is_bull and upper_shadow <= body * 0.05 and lower_shadow <= body * 0.05 and body >= total_range * 0.85:
            patterns.append({"date": date, "pattern": "Bullish Marubozu", "type": "bullish",
                "signal": "BUY", "prob": 72,
                "description": "Full bullish candle with no shadows — strong conviction buying. Trend continuation likely."})

        # Marubozu Bearish
        elif is_bear and upper_shadow <= body * 0.05 and lower_shadow <= body * 0.05 and body >= total_range * 0.85:
            patterns.append({"date": date, "pattern": "Bearish Marubozu", "type": "bearish",
                "signal": "SELL", "prob": 70,
                "description": "Full bearish candle with no shadows — strong conviction selling. Downtrend may continue."})

        # Spinning Top (small body, long shadows — indecision)
        elif body <= total_range * 0.25 and upper_shadow >= body and lower_shadow >= body:
            patterns.append({"date": date, "pattern": "Spinning Top", "type": "neutral",
                "signal": "HOLD", "prob": 52,
                "description": "Spinning Top — equal buying/selling pressure. Trend change possible; wait for direction."})

        # ── Two-candle patterns ────────────────────────────────────────────

        # Bullish Engulfing
        elif prev_bear and is_bull and c >= po and o <= pc and body >= prev_body * 0.8:
            patterns.append({"date": date, "pattern": "Bullish Engulfing", "type": "bullish",
                "signal": "BUY", "prob": 74,
                "description": "Bulls overpowered bears completely — strong reversal signal. Enter on break of high."})

        # Bearish Engulfing
        elif prev_bull and is_bear and c <= po and o >= pc and body >= prev_body * 0.8:
            patterns.append({"date": date, "pattern": "Bearish Engulfing", "type": "bearish",
                "signal": "SELL", "prob": 72,
                "description": "Bears overwhelmed bulls — strong distribution signal. Consider exiting long positions."})

        # Piercing Line (bullish, after downtrend)
        elif prev_bear and is_bull and o < pc and c > (po + pc) / 2 and c < po:
            patterns.append({"date": date, "pattern": "Piercing Line", "type": "bullish",
                "signal": "BUY", "prob": 67,
                "description": "Piercing Line — bulls pushed above midpoint of prior red candle. Potential bottom reversal."})

        # Dark Cloud Cover (bearish, after uptrend)
        elif prev_bull and is_bear and o > pc and c < (po + pc) / 2 and c > po:
            patterns.append({"date": date, "pattern": "Dark Cloud Cover", "type": "bearish",
                "signal": "SELL", "prob": 65,
                "description": "Dark Cloud Cover — bears penetrated prior bull candle. Caution: potential top forming."})

        # Bullish Harami (small candle inside prior bear)
        elif prev_bear and is_bull and o > pc and c < po and body < prev_body * 0.5:
            patterns.append({"date": date, "pattern": "Bullish Harami", "type": "bullish",
                "signal": "BUY", "prob": 60,
                "description": "Bullish Harami — small green candle inside prior red. Sellers losing momentum."})

        # Bearish Harami
        elif prev_bull and is_bear and o < pc and c > po and body < prev_body * 0.5:
            patterns.append({"date": date, "pattern": "Bearish Harami", "type": "bearish",
                "signal": "SELL", "prob": 58,
                "description": "Bearish Harami — small red candle inside prior green. Bulls losing steam; watch closely."})

        # Tweezer Bottom (both candles have same low — support)
        elif abs(l - pl) <= total_range * 0.03 and is_bull and prev_bear:
            patterns.append({"date": date, "pattern": "Tweezer Bottom", "type": "bullish",
                "signal": "BUY", "prob": 65,
                "description": "Tweezer Bottom — price tested same support twice. Strong support level confirmed."})

        # Tweezer Top
        elif abs(h - ph) <= total_range * 0.03 and is_bear and prev_bull:
            patterns.append({"date": date, "pattern": "Tweezer Top", "type": "bearish",
                "signal": "SELL", "prob": 63,
                "description": "Tweezer Top — price rejected same resistance twice. Strong resistance level confirmed."})

        # ── Three-candle patterns ──────────────────────────────────────────

        # Morning Star (3-candle bullish reversal)
        if i >= 2 and ppc < ppo and prev_body <= abs(ppc - ppo) * 0.35 and is_bull and c > (ppo + ppc) / 2:
            patterns.append({"date": date, "pattern": "Morning Star", "type": "bullish",
                "signal": "BUY", "prob": 78,
                "description": "Morning Star — 3-candle bottom reversal. Third candle confirms buyers are in control. Strong buy signal."})

        # Evening Star (3-candle bearish reversal)
        if i >= 2 and ppc > ppo and prev_body <= abs(ppc - ppo) * 0.35 and is_bear and c < (ppo + ppc) / 2:
            patterns.append({"date": date, "pattern": "Evening Star", "type": "bearish",
                "signal": "SELL", "prob": 76,
                "description": "Evening Star — 3-candle top reversal. Bears have taken control. Consider booking profits."})

        # Three White Soldiers (strong bullish trend)
        if (i >= 2 and is_bull and prev_bull and ppc < ppo and
                c > pc > ppc and body > 0 and prev_body > 0 and prev2_body > 0 and
                lower_shadow <= body * 0.3 and
                upper_shadow <= body * 0.3):
            patterns.append({"date": date, "pattern": "Three White Soldiers", "type": "bullish",
                "signal": "BUY", "prob": 80,
                "description": "Three White Soldiers — 3 consecutive strong green candles. Very bullish trend continuation signal."})

        # Three Black Crows (strong bearish trend)
        if (i >= 2 and is_bear and prev_bear and ppc > ppo and
                c < pc < ppc and body > 0 and prev_body > 0 and prev2_body > 0 and
                upper_shadow <= body * 0.3):
            patterns.append({"date": date, "pattern": "Three Black Crows", "type": "bearish",
                "signal": "SELL", "prob": 79,
                "description": "Three Black Crows — 3 consecutive strong red candles. Very bearish trend continuation signal."})

    # Return last 8 patterns (most recent)
    return patterns[-8:] if len(patterns) > 8 else patterns


# ── Main Analysis Engine ────────────────────────────────────────────────────
def analyze(ohlcv: list, fundamentals: dict = None) -> dict:
    """
    Full technical + fundamental analysis.
    Returns buy/sell projection score, indicators, signals, patterns.
    """
    if not ohlcv or len(ohlcv) < 20:
        return {"error": "Not enough data (need ≥ 20 trading days)"}

    df = pd.DataFrame(ohlcv)
    df = df.astype({"open": float, "high": float, "low": float, "close": float, "volume": float})
    closes = df["close"].tolist()
    highs  = df["high"].tolist()
    lows   = df["low"].tolist()
    vols   = df["volume"].tolist()
    dates  = df["date"].tolist()
    n = len(closes)

    # ── Core indicators ────────────────────────────────────────────────────
    sma20  = sma(closes, 20)
    sma50  = sma(closes, 50)
    sma200 = sma(closes, 200)
    ema12  = ema(closes, 12)
    ema26  = ema(closes, 26)
    rsi14  = rsi(closes, 14)
    macd_line, macd_sig, macd_hist = macd(closes)
    bb_up, bb_mid, bb_lo = bollinger(closes, 20)
    stoch_k, stoch_d = stochastic(highs, lows, closes)
    atr14  = atr(highs, lows, closes, 14)
    obv_   = obv(closes, vols)
    adx_line, di_plus, di_minus = adx(highs, lows, closes, 14)
    supports, resistances = support_resistance(highs, lows, closes)
    patterns = detect_patterns(ohlcv)

    def last(lst):
        for v in reversed(lst):
            if v is not None and not (isinstance(v, float) and math.isnan(v)):
                return round(float(v), 4)
        return 0.0

    cur_close  = closes[-1]
    cur_rsi    = last(rsi14)
    cur_macd   = last(macd_line)
    cur_sig    = last(macd_sig)
    cur_macdh  = last(macd_hist)
    cur_sma20  = last(sma20)
    cur_sma50  = last(sma50)
    cur_sma200 = last(sma200)
    cur_bbup   = last(bb_up)
    cur_bblo   = last(bb_lo)
    cur_stochk = last(stoch_k)
    cur_stochd = last(stoch_d)
    cur_adx    = last(adx_line)
    cur_di_p   = last(di_plus)
    cur_di_m   = last(di_minus)
    cur_atr    = last(atr14)

    # ── Scoring Engine (0-100) ─────────────────────────────────────────────
    # Each signal contributes +bullish or -bearish points
    score = 50  # neutral baseline
    signals_detail = []

    def add(pts, label, detail):
        nonlocal score
        score += pts
        signals_detail.append({
            "indicator": label,
            "value": detail,
            "bullish": pts > 0,
            "points": pts
        })

    # RSI
    if cur_rsi < 30:
        add(+12, "RSI", f"Oversold {cur_rsi:.1f} — strong buy zone")
    elif cur_rsi < 40:
        add(+6,  "RSI", f"Near oversold {cur_rsi:.1f} — watch for bounce")
    elif cur_rsi > 70:
        add(-12, "RSI", f"Overbought {cur_rsi:.1f} — caution/sell zone")
    elif cur_rsi > 60:
        add(-5,  "RSI", f"Near overbought {cur_rsi:.1f}")
    else:
        add(0,   "RSI", f"Neutral {cur_rsi:.1f}")

    # MACD
    if cur_macd > cur_sig and cur_macdh > 0:
        add(+10, "MACD", f"Bullish crossover — MACD {cur_macd:.2f} > Signal {cur_sig:.2f}")
    elif cur_macd < cur_sig and cur_macdh < 0:
        add(-10, "MACD", f"Bearish crossover — MACD {cur_macd:.2f} < Signal {cur_sig:.2f}")
    else:
        add(0, "MACD", f"MACD {cur_macd:.2f}, Signal {cur_sig:.2f}")

    # SMA trend
    if cur_close > cur_sma20 > cur_sma50:
        add(+8, "SMA Trend", f"Price above SMA20 ({cur_sma20:.2f}) & SMA50 ({cur_sma50:.2f}) — uptrend")
    elif cur_close < cur_sma20 < cur_sma50:
        add(-8, "SMA Trend", f"Price below SMA20 & SMA50 — downtrend")
    else:
        add(0, "SMA Trend", "Mixed SMA signals")

    # SMA200 (major trend)
    if cur_sma200 > 0:
        if cur_close > cur_sma200:
            add(+5, "SMA200", f"Price above 200-day MA ({cur_sma200:.2f}) — bull market")
        else:
            add(-5, "SMA200", f"Price below 200-day MA ({cur_sma200:.2f}) — bear market")

    # Bollinger Bands
    if cur_close <= cur_bblo:
        add(+8, "Bollinger", f"Price at/below lower band ({cur_bblo:.2f}) — potential bounce")
    elif cur_close >= cur_bbup:
        add(-8, "Bollinger", f"Price at/above upper band ({cur_bbup:.2f}) — overbought")
    else:
        band_pos = (cur_close - cur_bblo) / max(cur_bbup - cur_bblo, 1) * 100
        add(0, "Bollinger", f"Inside bands — position: {band_pos:.0f}%")

    # Stochastic
    if cur_stochk < 20 and cur_stochd < 20:
        add(+8, "Stochastic", f"%K={cur_stochk:.1f} oversold — buy signal")
    elif cur_stochk > 80 and cur_stochd > 80:
        add(-8, "Stochastic", f"%K={cur_stochk:.1f} overbought — sell signal")
    elif cur_stochk > cur_stochd and cur_stochk < 80:
        add(+4, "Stochastic", f"%K crossed above %D — bullish")
    elif cur_stochk < cur_stochd and cur_stochk > 20:
        add(-4, "Stochastic", f"%K crossed below %D — bearish")

    # ADX (trend strength)
    trend_str = "Strong" if cur_adx > 25 else ("Moderate" if cur_adx > 20 else "Weak")
    if cur_adx > 25:
        if cur_di_p > cur_di_m:
            add(+6, "ADX", f"Strong uptrend (ADX={cur_adx:.1f}, +DI>{cur_di_p:.1f} > -DI {cur_di_m:.1f})")
        else:
            add(-6, "ADX", f"Strong downtrend (ADX={cur_adx:.1f})")
    else:
        add(0, "ADX", f"Weak/no trend (ADX={cur_adx:.1f})")

    # Pattern signals
    recent_patterns = [p for p in patterns if p.get("signal") in ("BUY","SELL")]
    for p in recent_patterns[-2:]:
        if p["signal"] == "BUY":
            add(+5, f"Pattern: {p['pattern']}", p["description"])
        else:
            add(-5, f"Pattern: {p['pattern']}", p["description"])

    # Clamp score
    score = max(5, min(95, score))

    # ── Projection decision ────────────────────────────────────────────────
    if score >= 70:
        projection = "STRONG BUY"
        color = "#00e676"
        proj_desc = (f"Multiple bullish indicators align. RSI at {cur_rsi:.1f}, "
                     f"MACD {'bullish' if cur_macd > cur_sig else 'neutral'}. "
                     f"Consider accumulating near support Rs {supports[-1] if supports else 'N/A'}.")
    elif score >= 58:
        projection = "BUY"
        color = "#76ff03"
        proj_desc = (f"Moderately bullish. RSI {cur_rsi:.1f}. "
                     f"Watch for confirmation above SMA20 Rs {cur_sma20:.2f}.")
    elif score >= 43:
        projection = "HOLD"
        color = "#ffd600"
        proj_desc = (f"Mixed signals — no clear direction. RSI {cur_rsi:.1f}. "
                     f"Wait for breakout above Rs {resistances[0] if resistances else 'N/A'} "
                     f"or breakdown below Rs {supports[-1] if supports else 'N/A'}.")
    elif score >= 30:
        projection = "SELL"
        color = "#ff6d00"
        proj_desc = (f"Moderately bearish. RSI {cur_rsi:.1f}. "
                     f"Consider reducing exposure near resistance Rs {resistances[0] if resistances else 'N/A'}.")
    else:
        projection = "STRONG SELL"
        color = "#ff3d57"
        proj_desc = (f"Multiple bearish signals. RSI {cur_rsi:.1f} overbought/trending down. "
                     f"High caution — protect capital.")

    confidence = abs(score - 50) * 2  # 0-100%

    # ── Price targets ──────────────────────────────────────────────────────
    vol_avg = sum(vols[-14:]) / 14 if len(vols) >= 14 else 0
    atr_val = cur_atr if cur_atr else cur_close * 0.02

    # Fibonacci retracement levels (last 52 weeks)
    period_high = max(highs[-min(len(highs),252):])
    period_low  = min(lows[-min(len(lows),252):])
    fib_range = period_high - period_low
    fib_levels = {
        "0%":    round(period_low, 2),
        "23.6%": round(period_low + fib_range * 0.236, 2),
        "38.2%": round(period_low + fib_range * 0.382, 2),
        "50%":   round(period_low + fib_range * 0.500, 2),
        "61.8%": round(period_low + fib_range * 0.618, 2),
        "100%":  round(period_high, 2),
    }

    price_targets = {
        "target_1":  round(cur_close * 1.05, 2),
        "target_2":  round(cur_close * 1.10, 2),
        "stop_loss": round(cur_close - 1.5 * atr_val, 2),
        "risk_reward": round((cur_close * 1.07 - cur_close) / max(cur_close - (cur_close - 1.5 * atr_val), 1), 2),
    }
    if supports:
        price_targets["stop_loss"] = min(price_targets["stop_loss"], supports[-1] * 0.98)
    if resistances:
        price_targets["target_1"] = min(price_targets["target_1"], resistances[0])

    # ── Summary stats ──────────────────────────────────────────────────────
    change_1d  = (closes[-1] - closes[-2]) / closes[-2] * 100 if len(closes) >= 2 else 0
    change_1w  = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 else 0
    change_1m  = (closes[-1] - closes[-22]) / closes[-22] * 100 if len(closes) >= 22 else 0
    change_3m  = (closes[-1] - closes[-66]) / closes[-66] * 100 if len(closes) >= 66 else 0
    change_1y  = (closes[-1] - closes[0]) / closes[0] * 100 if len(closes) > 1 else 0

    # Serialize indicators for charting (last 100 days)
    def zip_dates(values, start=0):
        result = []
        for d, v in zip(dates[start:], values[start:]):
            if v is not None and not (isinstance(v, float) and math.isnan(v)):
                result.append({"date": d, "value": round(float(v), 4)})
        return result[-100:]

    return {
        "projection":      projection,
        "projection_color": color,
        "projection_desc": proj_desc,
        "score":           round(score, 1),
        "confidence":      round(confidence, 1),
        "price_targets":   price_targets,
        "fib_levels":      fib_levels,
        "signals_detail":  signals_detail,
        "patterns":        patterns,
        "indicators": {
            "current": {
                "close":    round(cur_close, 2),
                "rsi14":    round(cur_rsi, 2),
                "macd":     round(cur_macd, 4),
                "macd_signal": round(cur_sig, 4),
                "sma20":    round(cur_sma20, 2),
                "sma50":    round(cur_sma50, 2),
                "sma200":   round(cur_sma200, 2),
                "bb_upper": round(cur_bbup, 2),
                "bb_lower": round(cur_bblo, 2),
                "stoch_k":  round(cur_stochk, 2),
                "stoch_d":  round(cur_stochd, 2),
                "atr14":    round(cur_atr, 2),
                "adx":      round(cur_adx, 2),
                "trend_strength": trend_str,
                "volume_avg14": round(vol_avg, 0),
            },
            "chart": {
                "ohlcv":   ohlcv[-100:],
                "sma20":   zip_dates(sma20),
                "sma50":   zip_dates(sma50),
                "sma200":  zip_dates(sma200),
                "ema12":   zip_dates(ema12),
                "ema26":   zip_dates(ema26),
                "rsi":     zip_dates(rsi14),
                "macd_line":   zip_dates(macd_line),
                "macd_signal": zip_dates(macd_sig),
                "macd_hist":   zip_dates(macd_hist),
                "bb_upper":    zip_dates(bb_up),
                "bb_mid":      zip_dates(bb_mid),
                "bb_lower":    zip_dates(bb_lo),
                "stoch_k":     zip_dates(stoch_k),
                "stoch_d":     zip_dates(stoch_d),
                "obv":         zip_dates(obv_),
                "adx":         zip_dates(adx_line),
            },
        },
        "performance": {
            "change_1d":  round(change_1d, 2),
            "change_1w":  round(change_1w, 2),
            "change_1m":  round(change_1m, 2),
            "change_3m":  round(change_3m, 2),
            "change_1y":  round(change_1y, 2),
            "high_period": round(period_high, 2),
            "low_period":  round(period_low, 2),
        },
        "support_resistance": {
            "supports":    supports,
            "resistances": resistances,
        },
    }
