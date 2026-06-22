# === BOS MODULE START ===

import os
import time
import csv
from datetime import datetime

# ── Module-level state ──
bos_seen = set()
bos_fail_cache = {}
bos_memory = {}   # {symbol: {"last_price": float, "last_time": float}}
_bos_count_cache = {}

# ── BOS Quality constants ──
BOS_MIN_DIST    = 0.0025   # 0.25% — real break threshold
NEAR_BREAK_TOL  = 0.005    # 0.5%  — near-break tolerance
BOS_MOM_MIN     = 0.3      # ADDED: minimal body ratio required for near-break WEAK
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

def log_path(filename):
    return os.path.join(LOG_DIR, filename)


def get_volatility(df15):
    return (df15["high"] - df15["low"]).rolling(14).mean().iloc[-1] / df15["close"].iloc[-1]


def should_log_bos(symbol, cooldown=300):
    now = time.time()

    if symbol in bos_fail_cache:
        if now - bos_fail_cache[symbol] < cooldown:
            return False

    bos_fail_cache[symbol] = now
    return True


def detect_retest_v5(df, level, df15):
    vol = get_volatility(df15)
    tol = max(0.002, vol * 0.8)

    last = df.iloc[-2]

    # ===== WICK TOUCH
    touched = (
        abs(last["low"] - level) / level <= tol or
        abs(last["high"] - level) / level <= tol
    )

    # ===== CLOSE FILTER (anti fake)
    close_ok = abs(last["close"] - level) / level <= tol * 1.5
    dist = abs(last["close"] - level) / level

    if dist < tol * 0.5:
        strength = "STRONG"
    elif dist < tol:
        strength = "NORMAL"
    else:
        strength = "WEAK"
    return touched and close_ok, strength


def log_bos_fail(symbol, side, price, prev_high, prev_low, ema_align="N/A", dist_level=0):
    import csv
    from datetime import datetime
    file = log_path("bos_debug_v4.csv")
    file_exists = os.path.exists(file)
    with open(file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if side == "LONG":
            dist_high = (price - prev_high) / prev_high
            dist_low = (price - prev_low) / prev_low
        else:
            dist_high = (price - prev_high) / prev_high
            dist_low = (price - prev_low) / prev_low
        if not file_exists:
            writer.writerow([
            "time","symbol","side","price","prev_high","prev_low",
            "dist_high_pct","dist_low_pct","ema_align","dist_level"  # thêm 2 cột
            ])

        writer.writerow([
            datetime.now().strftime("%H:%M %d-%m"),
            symbol,
            side,
            round(price,6),
            round(prev_high,6),
            round(prev_low,6),
            round(dist_high,6),   # 🔥 NEW
            round(dist_low,6),     # 🔥 NEW
            ema_align, round(dist_level,4), 
        ])


def valid_bos(df, side):
    prev_high = df["high"].iloc[-20:-2].max()
    prev_low  = df["low"].iloc[-20:-2].min()
    close     = df["close"].iloc[-2]
 
    tol    = 0.003
    candle = df.iloc[-2]
    body   = abs(candle["close"] - candle["open"])
    rng    = candle["high"] - candle["low"]
 
    if rng == 0:
        return False
 
    strength = body / rng
 
    if side == "LONG":
        if close > prev_high * (1 + tol * 0.5) and strength >= 0.3:
            return True
    else:
        if close < prev_low * (1 - tol * 0.5) and strength >= 0.3:
            return True
 
    return False
 
 
def check_retest(df, df15, side):
    prev_high = df15["high"].iloc[-20:-2].max()
    prev_low  = df15["low"].iloc[-20:-2].min()
 
    tol   = 0.005   # relaxed tolerance
    price = df["close"].iloc[-2]
 
    # FIXED: direction validation — retest invalid if price has already broken past level
    if side == "LONG":
        if price > prev_high:
            return False   # ADDED: price above level → no longer retesting, already broke out
        return abs(price - prev_high) / prev_high <= tol
 
    else:
        if price < prev_low:
            return False   # ADDED: price below level → already broke out, not retesting
        return abs(price - prev_low) / prev_low <= tol


def _find_swing_level(df, side, lookback=20, fractal_n=5):
    """
    [BOS V2] Tìm swing high/low bằng fractal logic.
    swing_high[i] = high[i] là max của fractal_n nến xung quanh.
    Trả về level gần nhất hoặc fallback về prev_high/low.
    """
    highs = df["high"].iloc[-lookback:-2].values
    lows  = df["low"].iloc[-lookback:-2].values
    n     = fractal_n // 2  # bán kính fractal

    if side == "LONG":
        # Tìm swing high fractal gần nhất
        for i in range(len(highs)-1, n-1, -1):
            window = highs[max(0,i-n):i+n+1]
            if len(window) >= 3 and highs[i] == max(window):
                return highs[i]   # confirmed swing high
        return df["high"].iloc[-lookback:-2].max()   # fallback

    else:
        # Tìm swing low fractal gần nhất
        for i in range(len(lows)-1, n-1, -1):
            window = lows[max(0,i-n):i+n+1]
            if len(window) >= 3 and lows[i] == min(window):
                return lows[i]   # confirmed swing low
        return df["low"].iloc[-lookback:-2].min()   # fallback


def detect_bos_v45(df, side, symbol=None):
    """
    [BOS V3] Quality-gated BOS: near_break only assigned WEAK when dist < tol
    AND candle has minimal momentum. Blind WEAK fallback removed.
    """
    if len(df) < 22:
        return {"type": None, "level": None, "strength": 0, "near_break": False}
 
    last   = df.iloc[-2]
    close  = last["close"]
    high   = last["high"]
    low    = last["low"]
    open_  = last["open"]
 
    body     = abs(close - open_)
    rng      = high - low if high - low > 0 else 1e-9
    strength = body / rng
 
    vol     = df["volume"].iloc[-2]
    avg_vol = df["volume"].rolling(20).mean().iloc[-2]
    vol_ok  = avg_vol > 0 and vol >= avg_vol * 0.8
 
    # ── Swing level (fractal + fallback) ──
    swing_level  = _find_swing_level(df, side)
    _price_check = df["close"].iloc[-2]
    _dist_check  = abs(swing_level - _price_check) / swing_level if swing_level > 0 else 1.0
    if _dist_check > 0.03:
        if side == "LONG":
            swing_level = df["high"].iloc[-20:-2].max()
        else:
            swing_level = df["low"].iloc[-20:-2].min()
 
    # ── Micro noise filter ──
    range_size = (df["high"].iloc[-20:-2].max() - df["low"].iloc[-20:-2].min()) / close
    if range_size < 0.002:
        return {"type": None, "level": None, "strength": 0, "near_break": False}
 
    if strength < 0.15:
        return {"type": None, "level": None, "strength": 0, "near_break": False}
 
    # ── Fake break filter ──
    if side == "LONG":
        upper_wick = (last["high"] - max(last["open"], last["close"])) / rng
    else:
        upper_wick = (min(last["open"], last["close"]) - last["low"]) / rng
    wick_bad = upper_wick > strength * 3
 
    # ── Range lock (anti-spam per symbol) ──
    now = time.time()
    if symbol:
        mem        = bos_memory.get(symbol, {})
        last_price = mem.get("last_price", 0)
        last_time  = mem.get("last_time", 0)
        if last_price > 0:
            price_diff = abs(close - last_price) / last_price
            if price_diff < 0.003 and (now - last_time) < 900:
                return {"type": None, "level": None, "strength": 0, "near_break": False}
 
    bos_type   = None
    level      = swing_level
    near_break = False
 
    if side == "LONG":
        dist = (close - swing_level) / swing_level if swing_level > 0 else 0
 
        if dist >= BOS_MIN_DIST:
            if vol_ok and strength >= 0.5:
                bos_type = "STRONG"
            else:
                bos_type = "CONFIRM"
            if wick_bad:
                bos_type = "CONFIRM" if bos_type == "STRONG" else "WEAK"
 
        elif close > swing_level:
            # Small positive break below BOS_MIN_DIST
            bos_type   = "TRUE"
            near_break = True
 
        elif abs(close - swing_level) / swing_level <= NEAR_BREAK_TOL:
            # FIXED: only assign WEAK if dist is genuinely close AND candle has momentum
            if strength >= BOS_MOM_MIN:
                bos_type = "WEAK"
            # else: bos_type stays None — not enough momentum to call near-break
            near_break = True
 
        elif high > swing_level:
            bos_type   = "TRAP" if wick_bad else "EARLY"
            near_break = True
 
    elif side == "SHORT":
        dist = (swing_level - close) / swing_level if swing_level > 0 else 0
 
        if dist >= BOS_MIN_DIST:
            if vol_ok and strength >= 0.5:
                bos_type = "STRONG"
            else:
                bos_type = "CONFIRM"
            if wick_bad:
                bos_type = "CONFIRM" if bos_type == "STRONG" else "WEAK"
 
        elif close < swing_level:
            bos_type   = "TRUE"
            near_break = True
 
        elif abs(close - swing_level) / swing_level <= NEAR_BREAK_TOL:
            # FIXED: require minimal momentum for WEAK assignment
            if strength >= BOS_MOM_MIN:
                bos_type = "WEAK"
            near_break = True
 
        elif low < swing_level:
            bos_type   = "TRAP" if wick_bad else "EARLY"
            near_break = True
 
    # FIXED: removed blind fallback "if near_break and bos_type is None: bos_type = WEAK"
    # near_break can be True with bos_type = None (low-momentum near-break → pipeline will reject)
 
    # Update memory on confirmed BOS
    if bos_type in ("CONFIRM", "STRONG", "TRUE") and symbol:
        bos_memory[symbol] = {"last_price": close, "last_time": now}
 
    return {
        "type":       bos_type,
        "level":      level,
        "strength":   strength,
        "wick_bad":   wick_bad,
        "near_break": near_break,
    }
 
 
def bos_count(df, side):
    # [PERF] Cache theo candle time gần nhất + side
    # Mỗi nến mới → key mới → tính lại đúng 1 lần, sau đó dùng cache
    cache_key = f"{side}_{df['time'].iloc[-2]}"

    if cache_key in _bos_count_cache:
        return _bos_count_cache[cache_key]

    count = 0
    for i in range(-20, -2):
        sub = df.iloc[:i]
        bos = detect_bos_v45(sub, side)
        if bos["type"]:
            count += 1

    _bos_count_cache[cache_key] = count

    # Giới hạn cache không phình to
    if len(_bos_count_cache) > 300:
        _bos_count_cache.clear()

    return count


def detect_retest_v45(price, level, df):
    vol = get_volatility(df)
    tol = max(0.002, vol * 0.8)

    return abs(price - level) / level <= tol

# === BOS MODULE END ===