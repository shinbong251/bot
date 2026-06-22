import requests, time, csv, os, certifi, math
import numpy as np
import pandas as pd
from config import config

# =====================================================================
# POOL SELECTION SYSTEM — v8 (extracted module)
# Implements: SCAN → COMPRESSION POOL → PRE-BREAK POOL → TREND POOL
#             → MERGE+DEDUP+SCORE → FINAL CONFIRM POOL (~25-30)
# =====================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR  = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# ----- Pool Constants -----
POOL_SCAN_SIZE_MIN  = 120   # Tier A + B target
POOL_SCAN_SIZE_MAX  = 150
POOL_COMPRESS_MAX   = 80    # [FIX] 60→80: cần nhiều hơn để fill confirm
POOL_PREBREAK_MAX   = 40    # [FIX] 25→40: loosened
POOL_TREND_MAX      = 60    # [FIX] 50→60: loosened
POOL_CONFIRM_MAX    = 50    # [FIX] 30→50: target 40-50 coin sau filter
POOL_CAP_COMPRESS   = 25    # [FIX] 15→25: allow more compression into confirm
POOL_CAP_TREND      = 30    # [FIX] 20→30: allow more trend into confirm

# Window constants (mirrors v8.py)
H1_WINDOW = 20

# ----- Internal log cache (module-scoped, anti-duplicate) -----
_log_cache    = {}
_LOG_COOLDOWN = 300   # 5 phút — same as v8 LOG_COOLDOWN

# ----- Fetch data cache (symbol+tf → (df, timestamp)) -----
_fetch_cache     = {}
_CACHE_MAX_AGE   = 120   # seconds — valid for ~2 scan cycles

# ----- HTF cache (D1: 1h, H4: 15min) — refreshed less often -----
_htf_cache = {}
_HTF_TTL   = {"1d": 3600, "4h": 900}

# ----- Circuit breaker (symbol → consecutive_failures) -----
_cb_failures     = {}   # symbol → int
_cb_skip_until   = {}   # symbol → float (epoch)
_CB_THRESHOLD    = 5    # open circuit after this many consecutive failures
_CB_COOLDOWN     = 180  # seconds to skip symbol (~3 scan cycles)

# ----- Cycle metrics -----
_metrics = {"total": 0, "success": 0, "fail": 0, "fallback": 0}

# ----- Exchange metadata cache for scan-universe symbol classification -----
_exchange_symbol_cache = {"loaded_at": 0, "by_symbol": {}}
_EXCHANGE_SYMBOL_TTL = 3600
_SYMBOL_FILTER_LOG_COOLDOWN = 3600
_SYMBOL_FILTER_LOG_LIMIT = 20
_symbol_filter_last_log = 0


# =====================================================================
# FETCH
# =====================================================================

def fetch(symbol, tf, is_priority=False):
    _RETRYABLE = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.ReadTimeout,
        requests.exceptions.ConnectTimeout,
    )
    max_retries   = 5    if is_priority else 3
    delays        = [0.5, 1.0, 2.0, 3.0, 5.0] if is_priority else [0.5, 1.0, 2.0]
    req_timeout   = (4, 8) if is_priority else (4, 12)
    cache_key     = (symbol, tf)

    _metrics["total"] += 1

    # ----- Circuit breaker check -----
    if not is_priority:
        skip_until = _cb_skip_until.get(symbol, 0)
        if time.time() < skip_until:
            cached = _fetch_cache.get(cache_key)
            if cached is not None:
                cached_df, cached_at = cached
                if time.time() - cached_at <= _CACHE_MAX_AGE:
                    print(f"[FETCH FALLBACK] {symbol} {tf} — circuit open, using cache")
                    _metrics["fallback"] += 1
                    return cached_df
            print(f"[FETCH FAIL] {symbol} {tf} — circuit open, no cache")
            _metrics["fail"] += 1
            return None

    for attempt in range(max_retries):
        try:
            time.sleep(0.05)
            res = requests.get(
                "https://fapi.binance.com/fapi/v1/klines",
                params={"symbol":symbol,"interval":tf,"limit":200},
                verify=certifi.where(),
                timeout=req_timeout
            )

            if res.status_code == 429:
                retry_after = float(res.headers.get("Retry-After", delays[attempt]))
                print(f"⚠️ RATE LIMIT {symbol} — waiting {retry_after}s")
                time.sleep(retry_after)
                continue

            data = res.json()

            # 🔥 FIX CRASH
            if not isinstance(data, list):
                return None

            df = pd.DataFrame(data, columns=[
                "time","open","high","low","close","volume",
                "ct","q","n","tb","tq","ig"
            ])

            if len(df) < 50:
                return None

            for c in ["open","high","low","close","volume"]:
                df[c] = df[c].astype(float)

            _fetch_cache[cache_key] = (df, time.time())
            _cb_failures[symbol]    = 0
            _metrics["success"]    += 1
            #print(f"[FETCH OK] {symbol} {tf}")
            return df

        except _RETRYABLE as e:
            print(f"❌ FETCH ERROR {symbol} (attempt {attempt+1}/{max_retries}):", e)
            if attempt < max_retries - 1:
                time.sleep(delays[attempt])
            continue

        except Exception as e:
            print(f"❌ FETCH ERROR {symbol}:", e)
            break

    # ----- All attempts exhausted -----
    consec = _cb_failures.get(symbol, 0) + 1
    _cb_failures[symbol] = consec
    if not is_priority and consec >= _CB_THRESHOLD:
        _cb_skip_until[symbol] = time.time() + _CB_COOLDOWN
        print(f"[CIRCUIT OPEN] {symbol} — {consec} consecutive failures, skipping for {_CB_COOLDOWN}s")

    cached = _fetch_cache.get(cache_key)
    if cached is not None:
        cached_df, cached_at = cached
        if time.time() - cached_at <= _CACHE_MAX_AGE:
            print(f"[FETCH FALLBACK] {symbol} {tf} — using cached data")
            _metrics["fallback"] += 1
            return cached_df

    print(f"[FETCH FAIL] {symbol} {tf} — no valid cache")
    _metrics["fail"] += 1
    return None


def fetch_log_metrics():
    total    = _metrics["total"]
    success  = _metrics["success"]
    fail     = _metrics["fail"]
    fallback = _metrics["fallback"]
    rate     = round(fail / total * 100, 1) if total > 0 else 0.0
    open_cbs = [s for s, t in _cb_skip_until.items() if time.time() < t]
    print(
        f"[FETCH METRICS] total={total} ok={success} fail={fail} "
        f"fallback={fallback} fail_rate={rate}% open_circuits={len(open_cbs)}"
    )
    if open_cbs:
        print(f"[FETCH METRICS] open circuit symbols: {open_cbs}")
    _metrics["total"]    = 0
    _metrics["success"]  = 0
    _metrics["fail"]     = 0
    _metrics["fallback"] = 0


def fetch_ticker(symbol):
    try:
        res = requests.get(
            "https://fapi.binance.com/fapi/v1/ticker/price",
            params={"symbol": symbol},
            verify=certifi.where(),
            timeout=(3, 5),
        )
        return float(res.json()["price"])
    except Exception:
        return None


def fetch_cached(symbol, tf, max_age=120):
    key = (symbol, tf)
    cached = _fetch_cache.get(key)
    if cached is not None:
        df, ts = cached
        if time.time() - ts <= max_age:
            return df
    return None


def fetch_cached_with_meta(symbol, tf, max_age=120):
    key = (symbol, tf)
    cached = _fetch_cache.get(key)
    if cached is None:
        return None, None, None
    df, ts = cached
    age = time.time() - ts
    if age <= max_age:
        return df, ts, age
    return None, ts, age


def _fetch_htf(symbol, tf):
    key = (symbol, tf)
    cached = _htf_cache.get(key)
    if cached is not None:
        df, ts = cached
        if time.time() - ts < _HTF_TTL.get(tf, 120):
            return df
    df = fetch(symbol, tf)
    if df is not None:
        _htf_cache[key] = (df, time.time())
    return df


# ===== ADD MULTI TIMEFRAME =====
def fetch_multi(symbol):
    df5  = fetch(symbol, "5m")
    df15 = fetch(symbol, "15m")
    df1h = fetch(symbol, "1h")
    df4h = _fetch_htf(symbol, "4h")
    df1d = _fetch_htf(symbol, "1d")

    if df5 is None or df15 is None or df1h is None:
        return None, None, None, None, None

    return df5, df15, df1h, df4h, df1d


# =====================================================================
# LOGGING HELPERS
# =====================================================================

def _log_path(filename):
    return os.path.join(LOG_DIR, filename)


def _format_vn_time(ts):
    from datetime import datetime, timezone, timedelta
    VN_TZ = timezone(timedelta(hours=7))
    return datetime.fromtimestamp(ts, VN_TZ).strftime("%H:%M %d-%m")


def log_pool_stage(symbol, stage, score=None, setup_type=None, reason=None):
    """
    PART 1 — Pool Pipeline Log (with anti-duplicate for SCAN stage, Part 6)
    """
    _cooldown = _LOG_COOLDOWN if stage == "SCAN" else 60
    key = (symbol, "POOL_STAGE", stage)
    now = time.time()
    if now - _log_cache.get(key, 0) < _cooldown:
        return
    _log_cache[key] = now
    file   = _log_path("log_pool_pipeline.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":       _format_vn_time(time.time()),
        "symbol":     symbol,
        "stage":      stage,
        "score":      round(score, 4) if score is not None else "",
        "setup_type": setup_type or "",
        "reason":     reason or "",
    }
    try:
        with open(file, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=row.keys())
            if is_new:
                w.writeheader()
            w.writerow(row)
    except Exception as e:
        print(f"[DBG][LOG_POOL] WRITE FAIL: {file} | {e}")


# =====================================================================
# PART 2 — SCAN UNIVERSE (Tier A + Tier B)
# =====================================================================

def _get_tradfi_symbol_allowlist():
    allowlist = set()
    raw_allowlist = config.get("tradfi_symbol_allowlist", [])
    if not isinstance(raw_allowlist, list):
        raw_allowlist = []
    for item in raw_allowlist:
        if not isinstance(item, str):
            continue
        symbol = item.strip().upper()
        if symbol:
            allowlist.add(symbol)
    return allowlist


def _fetch_exchange_symbol_metadata():
    now = time.time()
    cached = _exchange_symbol_cache.get("by_symbol") or {}
    loaded_at = _exchange_symbol_cache.get("loaded_at", 0)
    if cached and now - loaded_at < _EXCHANGE_SYMBOL_TTL:
        return cached

    try:
        data = requests.get(
            "https://fapi.binance.com/fapi/v1/exchangeInfo",
            verify=certifi.where(),
            timeout=(4, 12),
        ).json()
    except Exception as e:
        print(f"[SYMBOL FILTER] exchangeInfo metadata unavailable: {e}")
        return cached

    symbols = data.get("symbols", []) if isinstance(data, dict) else []
    if not isinstance(symbols, list):
        print("[SYMBOL FILTER] exchangeInfo metadata unavailable: malformed symbols payload")
        return cached

    by_symbol = {}
    for item in symbols:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        if symbol:
            by_symbol[symbol] = item

    _exchange_symbol_cache["loaded_at"] = now
    _exchange_symbol_cache["by_symbol"] = by_symbol
    return by_symbol


def _tradfi_classification(symbol_info):
    if not isinstance(symbol_info, dict):
        return False, "", ""

    underlying_subtype = symbol_info.get("underlyingSubType")
    if isinstance(underlying_subtype, list):
        for item in underlying_subtype:
            if isinstance(item, str) and item.strip().lower() == "tradfi":
                return True, "underlyingSubType", underlying_subtype

    contract_type = symbol_info.get("contractType")
    if isinstance(contract_type, str) and contract_type.strip().upper() == "TRADIFI_PERPETUAL":
        return True, "contractType", contract_type

    return False, "", ""


def is_tradfi_symbol(symbol_info):
    is_tradfi, _, _ = _tradfi_classification(symbol_info)
    return is_tradfi


def get_symbols_pool():
    """
    Mở rộng scan universe lên 120-150 coin.
    Tier A: volume >= 10M, top by liquidity, 100-120 coins.
    Tier B: volume thấp hơn NHƯNG chỉ nếu compression mạnh (opportunistic).
    Returns list of (symbol, vol, tier) tuples sorted by vol desc.
    """
    for i in range(3):
        try:
            data = requests.get(
                "https://fapi.binance.com/fapi/v1/ticker/24hr",
                verify=certifi.where(), timeout=10
            ).json()
            if not isinstance(data, list):
                time.sleep(2)
                continue
            break
        except Exception as e:
            print(f"❌ POOL SCAN ERROR ({i+1}/3):", e)
            time.sleep(3)
    else:
        return []

    global _symbol_filter_last_log

    exclude_tradfi_symbols = bool(config.get("exclude_tradfi_symbols", True))
    tradfi_allowlist = _get_tradfi_symbol_allowlist()
    exchange_symbols = _fetch_exchange_symbol_metadata() if exclude_tradfi_symbols else {}
    should_log_filter = time.time() - _symbol_filter_last_log >= _SYMBOL_FILTER_LOG_COOLDOWN
    if should_log_filter:
        print(
            f"[SYMBOL FILTER] exclude_tradfi_symbols={str(exclude_tradfi_symbols).lower()} "
            f"allowlist_size={len(tradfi_allowlist)}"
        )
        if exclude_tradfi_symbols and not exchange_symbols:
            print("[SYMBOL FILTER] tradfi metadata unavailable; no TradFi exclusions applied")

    tier_a = []
    tier_b = []
    loaded = 0
    tradfi_excluded = []
    tradfi_allowed = []

    for c in data:
        symbol = str(c.get("symbol", "")).strip().upper()
        if not symbol.endswith("USDT"):
            continue
        loaded += 1
        if exclude_tradfi_symbols:
            symbol_info = exchange_symbols.get(symbol)
            is_tradfi, field_name, field_value = _tradfi_classification(symbol_info)
            if is_tradfi:
                if symbol in tradfi_allowlist:
                    tradfi_allowed.append((symbol, field_name, field_value))
                else:
                    tradfi_excluded.append((symbol, field_name, field_value))
                    continue
        vol    = float(c["quoteVolume"])
        change = abs(float(c["priceChangePercent"]))

        if vol >= 10_000_000 and change >= 1.0:
            tier_a.append((symbol, vol, "A"))
        elif vol >= 3_000_000 and change >= 1.5:
            # Tier B: opportunistic — sẽ filter bằng compression_score sau
            tier_b.append((symbol, vol, "B"))

    # Tier A: sort by vol, lấy top 100-120
    tier_a.sort(key=lambda x: x[1], reverse=True)
    tier_a = tier_a[:120]

    # Tier B: sort by vol, lấy đủ để tổng đạt 120-150
    tier_b.sort(key=lambda x: x[1], reverse=True)
    slots_b = max(0, POOL_SCAN_SIZE_MAX - len(tier_a))
    tier_b  = tier_b[:slots_b]

    combined = tier_a + tier_b
    combined.sort(key=lambda x: x[1], reverse=True)

    if should_log_filter:
        for symbol, field_name, field_value in sorted(tradfi_excluded)[:_SYMBOL_FILTER_LOG_LIMIT]:
            print(
                f"[SYMBOL FILTER] excluded {symbol} reason=tradfi_category "
                f"field={field_name} value={field_value}"
            )
        if len(tradfi_excluded) > _SYMBOL_FILTER_LOG_LIMIT:
            remaining = len(tradfi_excluded) - _SYMBOL_FILTER_LOG_LIMIT
            print(f"[SYMBOL FILTER] excluded {remaining} additional TradFi symbol(s)")
        for symbol, _, _ in sorted(tradfi_allowed)[:_SYMBOL_FILTER_LOG_LIMIT]:
            print(f"[SYMBOL FILTER] allowed {symbol} reason=tradfi_allowlist")

    total = len(combined)
    print(
        f"[SYMBOL FILTER] scan universe: loaded={loaded} "
        f"tradfi_excluded={len(tradfi_excluded)} "
        f"tradfi_allowed={len(tradfi_allowed)} final={total}"
    )
    if should_log_filter:
        _symbol_filter_last_log = time.time()
    print(f"🌐 POOL SCAN: {total} symbols (Tier A={len(tier_a)}, Tier B={len(tier_b)})")
    return combined


# =====================================================================
# COMPRESSION HELPERS
# =====================================================================

def _calc_compression_score_fast(df1h):
    """
    Fast compression score cho pool building (không lưu vào watchlist).
    Trả về (score, tightening, candles, near_edge, spread_est).
    """
    if df1h is None or len(df1h) < 22:
        return 0, False, 0, False, 1.0

    high_20 = df1h["high"].iloc[-(H1_WINDOW+2):-2].max()
    low_20  = df1h["low"].iloc[-(H1_WINDOW+2):-2].min()
    close   = df1h["close"].iloc[-2]

    if low_20 <= 0:
        return 0, False, 0, False, 1.0

    range_pct  = (high_20 - low_20) / low_20
    vol        = df1h["volume"].iloc[-2]
    avg_vol    = df1h["volume"].rolling(20).mean().iloc[-2]
    vol_ratio  = vol / avg_vol if avg_vol > 0 else 1.0

    ema34 = df1h["close"].ewm(span=34).mean().iloc[-2]
    ema89 = df1h["close"].ewm(span=89).mean().iloc[-2]
    if math.isnan(ema34) or math.isnan(ema89) or close <= 0:
        return 0, False, 0, False, 1.0
    ema_gap = abs(ema34 - ema89) / close

    touch_thresh  = 0.02
    touch_count   = max(
        sum(1 for i in range(-(H1_WINDOW+2), -2)
            if abs(df1h["close"].iloc[i] - high_20) / high_20 <= touch_thresh),
        sum(1 for i in range(-(H1_WINDOW+2), -2)
            if abs(df1h["close"].iloc[i] - low_20)  / low_20  <= touch_thresh)
    )

    last = df1h.iloc[-2]
    wick_sweep = (
        (last["high"] > high_20 and last["close"] < high_20) or
        (last["low"]  < low_20  and last["close"] > low_20)
    )

    score = 0
    if range_pct  < 0.05: score += 1   # [FIX] 0.02→0.05
    if vol_ratio  < 0.90: score += 1   # [FIX] 0.70→0.90
    if touch_count >= 3:  score += 1
    if ema_gap    < 0.03: score += 1   # [FIX] 0.01→0.03
    if wick_sweep:        score += 1

    # tightening check — [FIX Part 3] allow slight expansion up to 5%
    range_now  = df1h["high"].iloc[-7:-2].max() - df1h["low"].iloc[-7:-2].min()
    range_prev = df1h["high"].iloc[-12:-7].max() - df1h["low"].iloc[-12:-7].min()
    # Cũ: range_now < range_prev (strict)
    # Mới: cho phép tối đa +5% mở rộng → vẫn coi là tightening
    tightening = range_prev > 0 and range_now <= range_prev * 1.15  # [FIX] 1.05→1.15

    # candles in 80% range
    r80_h = low_20 + (high_20 - low_20) * 0.9
    r80_l = low_20 + (high_20 - low_20) * 0.1
    candles = sum(
        1 for i in range(-(H1_WINDOW+2), -2)
        if r80_l <= df1h["close"].iloc[i] <= r80_h
    )

    # near_edge — [FIX Part 2] relaxed: 0.002 → 0.005
    near_edge = (
        abs(close - high_20) / high_20 < 0.005 or
        abs(close - low_20)  / low_20  < 0.005
    )

    # spread estimate (range / close)
    spread_est = range_pct  # approximation

    return score, tightening, candles, near_edge, spread_est


def _detect_compression_v2(df1h, df4h=None):
    """
    [ADD] Compression V2 — phân tích chất lượng compression thực sự.
    Trả về dict hoặc None nếu không đủ điều kiện.
    Không thay thế scan_compression — được gọi từ bên trong để bổ sung context.
    """
    if len(df1h) < 22:
        return None

    high_20 = df1h["high"].iloc[-(H1_WINDOW+2):-2].max()
    low_20  = df1h["low"].iloc[-(H1_WINDOW+2):-2].min()
    close   = df1h["close"].iloc[-2]
    score   = 0

    # ===== PART 1: MINIMUM DURATION =====
    # Đếm số nến close trong range (80% range)
    range_80_high = low_20 + (high_20 - low_20) * 0.9
    range_80_low  = low_20 + (high_20 - low_20) * 0.1
    compression_candles = sum(
        1 for i in range(-(H1_WINDOW+2), -2)
        if range_80_low <= df1h["close"].iloc[i] <= range_80_high
    )
    if compression_candles < 6:
        return None   # không đủ thời gian tích lũy

    # ===== PART 1: VOLATILITY CONTRACTION =====
    range_now  = (df1h["high"].iloc[-7:-2].max()  - df1h["low"].iloc[-7:-2].min())
    range_prev = (df1h["high"].iloc[-12:-7].max() - df1h["low"].iloc[-12:-7].min())
    if range_prev > 0 and range_now >= range_prev:
        score -= 1   # range đang mở rộng, không phải tighten

    # ===== PART 1: NO STRONG TREND STRUCTURE =====
    highs = [df1h["high"].iloc[i] for i in range(-(H1_WINDOW+2), -2)]
    lows  = [df1h["low"].iloc[i]  for i in range(-(H1_WINDOW+2), -2)]
    n = len(highs)

    mid = n // 2
    is_lower_high  = highs[mid:] and highs[:mid] and max(highs[mid:]) < max(highs[:mid])
    is_lower_low   = lows[mid:]  and lows[:mid]  and min(lows[mid:])  < min(lows[:mid])
    is_higher_high = highs[mid:] and highs[:mid] and max(highs[mid:]) > max(highs[:mid])
    is_higher_low  = lows[mid:]  and lows[:mid]  and min(lows[mid:])  > min(lows[:mid])

    # [FIX Part 3] Downtrend: giữ nguyên (chỉ reject khi cả lower_high VÀ lower_low rõ ràng)
    if is_lower_high and is_lower_low:
        return None

    # [FIX Part 3] Uptrend mạnh: nâng ngưỡng 0.02 → 0.04 (ít reject hơn)
    ema34 = df1h["close"].ewm(span=34).mean().iloc[-2]
    ema89 = df1h["close"].ewm(span=89).mean().iloc[-2]
    strong_momentum = abs(ema34 - ema89) / close > 0.04   # [FIX] 0.02 → 0.04
    if is_higher_high and is_higher_low and strong_momentum:
        return None

    # ===== PART 2: CONTEXT CLASSIFICATION =====
    def _ema_side(df):
        if df is None or len(df) < 90:
            return None
        e34 = df["close"].ewm(span=34).mean().iloc[-2]
        e89 = df["close"].ewm(span=89).mean().iloc[-2]
        if e34 > e89: return "UP"
        if e34 < e89: return "DOWN"
        return None

    htf_trend = _ema_side(df4h)

    building_higher_lows = is_higher_low

    rejection_count = sum(
        1 for i in range(-(H1_WINDOW+2), -2)
        if df1h["high"].iloc[i] >= high_20 * 0.998
        and (df1h["high"].iloc[i] - max(df1h["open"].iloc[i], df1h["close"].iloc[i]))
           / (df1h["high"].iloc[i] - df1h["low"].iloc[i] + 1e-9) >= 0.3
    )

    avg_body = (df1h["close"] - df1h["open"]).abs().rolling(20).mean().iloc[-2]
    last_impulse_candles = 0
    for i in range(-2, -(H1_WINDOW+2), -1):
        body = abs(df1h["close"].iloc[i] - df1h["open"].iloc[i])
        if body >= avg_body * 2:
            break
        last_impulse_candles += 1

    if htf_trend == "DOWN":
        compression_type = "TREND_CONTINUATION_FAKE"
        return None   # downtrend pullback → reject

    elif htf_trend == "UP" and building_higher_lows:
        compression_type = "ACCUMULATION"
        score += 2

    elif rejection_count >= 2:
        near_resistance = abs(close - high_20) / high_20 < 0.03
        compression_type = "DISTRIBUTION"
        if near_resistance:
            score -= 1   # [FIX Part 3] cũ: -2; mới: -1
        else:
            score -= 1

    else:
        compression_type = "NEUTRAL"

    # ===== PART 3: IMPULSE FILTER =====
    if last_impulse_candles < 5:
        score -= 1   # [FIX] cũ: < 8 → -2; mới: < 5 → -1

    # ===== PART 4: STRUCTURE QUALITY =====
    range_last_5 = df1h["high"].iloc[-7:-2].max() - df1h["low"].iloc[-7:-2].min()
    range_prev_5 = df1h["high"].iloc[-12:-7].max() - df1h["low"].iloc[-12:-7].min()
    if range_prev_5 > 0 and range_last_5 >= range_prev_5:
        score -= 1   # không tighten

    if not building_higher_lows and compression_type == "ACCUMULATION":
        score -= 1   # claimed accumulation nhưng không có higher lows

    return {
        "type":    compression_type,
        "score":   score,
        "valid":   score >= 0,   # [FIX] 2→0: valid chỉ để classify, không gate pool
        "candles": compression_candles,
        "range":   (low_20, high_20),
    }


def _is_overextended(df):
    # ===== 1. CANDLE SPIKE =====
    open_ = df["open"].iloc[-2]
    close = df["close"].iloc[-2]
    body = abs(close - open_)

    avg_range = (df["high"] - df["low"]).rolling(20).mean().iloc[-2]
    spike = body > avg_range * 1.8

    # ===== 2. MULTI-CANDLE MOVE =====
    close_now  = df["close"].iloc[-2]
    close_prev = df["close"].iloc[-6]

    move = abs(close_now - close_prev) / max(close_prev, 1e-9)
    trend_run = move > 0.04   # >4% dalam 4 nến

    # ===== 3. EMA DISTANCE =====
    ema34 = df["close"].ewm(span=34).mean().iloc[-2]
    ema_dist = abs(close_now - ema34) / max(ema34, 1e-9)

    far_from_ema = ema_dist > 0.03   # >3% xa EMA

    return spike or trend_run or far_from_ema


# =====================================================================
# PART 3+4 — COMPRESSION POOL + PRE-BREAK POOL
# =====================================================================

def build_compression_pool(scan_data):
    """
    PART 3: compression_pool (~40-60) → filter từ scan_data.
    PART 4: pre_break_pool (~15-25) → filter từ compression_pool.

    scan_data: list of (symbol, tier, df1h, df4h, df1d)
    Returns: (compression_pool, pre_break_pool) — list of dicts.

    [FIX v8] Thresholds relaxed để khắc phục PRE-BREAK=0 bottleneck.
    """
    compression_pool = []
    pre_break_pool   = []

    _dbg_rej = {"score": 0, "candles": 0, "tightening": 0, "cv2_none": 0,
                "trend_fake": 0, "pb_score": 0, "pb_edge": 0,
                "pb_spread": 0, "pb_vol_pen": 0, "pb_tier_b": 0}
    _pb_entered        = 0
    _pb_near_edge_pass = 0
    _pb_spread_pass    = 0
    _compress_total    = 0

    for symbol, tier, df1h, df4h, df1d in scan_data:
        score, tightening, candles, near_edge, spread_est = _calc_compression_score_fast(df1h)

        # ── COMPRESSION POOL GATE (PART 3) ──────────────────────────
        # [FIX Part 3] score >= 2 (giữ nguyên, đã đủ thấp)
        if score < 2:
            _dbg_rej["score"] += 1
            print(f"[DBG][COMPRESS] {symbol} REJECT score={score} < 2")
            continue

        # [FIX] candles >= 3 (từ 4 → 3, fill pool nhiều hơn)
        if candles < 3:
            _dbg_rej["candles"] += 1
            print(f"[DBG][COMPRESS] {symbol} REJECT candles={candles} < 4")
            continue

        # [OPT] Tightening không còn là hard gate — chỉ penalty score
        if not tightening:
            score -= 1   # penalty, không reject
            _dbg_rej["tightening"] += 1
            if score < 0:
                print(f"[DBG][COMPRESS] {symbol} REJECT tightening_penalty score={score}")
                continue

        # TREND_FAKE filter
        cv2 = _detect_compression_v2(df1h, df4h)
        if cv2 is None:
            ctype = "NEUTRAL"
            print(f"[DBG][COMPRESS] {symbol} cv2=None → dùng NEUTRAL (không reject)")
        else:
            ctype = cv2.get("type", "NEUTRAL")

        if ctype == "TREND_CONTINUATION_FAKE":
            _dbg_rej["trend_fake"] += 1
            print(f"[DBG][COMPRESS] {symbol} REJECT type=TREND_FAKE")
            continue

        entry = {
            "symbol":              symbol,
            "tier":                tier,
            "compression_score":   score,
            "compression_candles": candles,
            "tightening":          tightening,
            "near_edge":           near_edge,
            "spread_est":          spread_est,
            "compression_type":    ctype,
            "setup_type":          "COMPRESSION",
            "trend_score":         0,
            "score":               float(score),
            "priority":            float(score),
        }
        compression_pool.append(entry)
        _compress_total += 1

        # ── PRE-BREAK POOL GATE (PART 4) ────────────────────────────
        _pb_entered += 1

        # ── compute all conditions up front for full trace ──
        _close_pb   = df1h["close"].iloc[-2]
        _high_pb    = df1h["high"].iloc[-22:-2].max()
        _low_pb     = df1h["low"].iloc[-22:-2].min()
        _d_hi_pb    = abs(_close_pb - _high_pb) / max(_high_pb, 1e-9)
        _d_lo_pb    = abs(_close_pb - _low_pb)  / max(_low_pb,  1e-9)
        _near_pb    = (_d_hi_pb < 0.015) or (_d_lo_pb < 0.015)
        _spread_ok  = spread_est < 0.01
        _score_ok   = score >= 1
        _tierb_ok   = not (tier == "B" and (score < 2.0 or spread_est >= 0.005))

        print(
            f"[PB_TRACE] {symbol} | tier={tier} | score={score} | "
            f"tier_ok={_tierb_ok} | score_ok={_score_ok} | "
            f"near_edge={_near_pb}(d_hi={_d_hi_pb:.4f} d_lo={_d_lo_pb:.4f}) | "
            f"spread_ok={_spread_ok}({spread_est:.4f}) | "
            f"PASS={_tierb_ok and _score_ok and _near_pb and _spread_ok}"
        )

        # Tier B extra filter
        if tier == "B":
            if score < 2.0 or spread_est >= 0.005:
                _dbg_rej["pb_tier_b"] += 1
                print(f"[PB_TRACE] {symbol} | FAIL: tier_B_filter score={score} spread={spread_est:.4f}")
                continue

        # score gate
        if score < 1:
            _dbg_rej["pb_score"] += 1
            print(f"[PB_TRACE] {symbol} | FAIL: score={score} < 1")
            continue

        # ===== REALTIME EDGE DETECT =====
        close   = df1h["close"].iloc[-2]
        high_20 = df1h["high"].iloc[-22:-2].max()
        low_20  = df1h["low"].iloc[-22:-2].min()

        d_hi = abs(close - high_20) / max(high_20, 1e-9)
        d_lo = abs(close - low_20)  / max(low_20, 1e-9)

        near_edge_rt = (d_hi < 0.015) or (d_lo < 0.015)

        if not near_edge_rt:
            _dbg_rej["pb_edge"] += 1
            print(f"[PB_TRACE] {symbol} | FAIL: near_edge=False d_hi={d_hi:.4f} d_lo={d_lo:.4f} (range={d_hi+d_lo:.4f})")
            continue

        _pb_near_edge_pass += 1

        # Volume penalty
        avg_vol = df1h["volume"].rolling(20).mean().iloc[-2]
        vol_pen = 0
        if avg_vol < 3_000_000:
            vol_pen = 1
            _dbg_rej["pb_vol_pen"] += 1
            print(f"[PB_TRACE] {symbol} | vol_pen=1 avg_vol={avg_vol:.0f}")

        _pb_spread_pass += 1

        pb_entry = entry.copy()
        pb_entry["score"]      = float(score - vol_pen)
        pb_entry["priority"]   = float(score - vol_pen)
        pb_entry["pool_stage"] = "PRE_BREAK"
        pre_break_pool.append(pb_entry)
        print(f"[PB_TRACE] {symbol} | PASS -> pre_break_pool")

    # Sort + cap
    compression_pool.sort(key=lambda x: x["compression_score"], reverse=True)
    compression_pool = compression_pool[:POOL_COMPRESS_MAX]

    pre_break_pool.sort(key=lambda x: x["score"], reverse=True)
    pre_break_pool = pre_break_pool[:POOL_PREBREAK_MAX]

    print(f"📦 COMPRESSION POOL: {len(compression_pool)} | PRE-BREAK POOL: {len(pre_break_pool)}")

    _total = max(_pb_entered, 1)
    print(f"\n===== PREBREAK DEBUG SUMMARY =====")
    print(f"  compression_pass : {_compress_total} / {len(scan_data) if hasattr(scan_data, '__len__') else '?'}")
    print(f"  pb_entered       : {_pb_entered} / {_compress_total}  (passed compression → entered PB gate)")
    print(f"  tier_b_fail      : {_dbg_rej['pb_tier_b']} / {_pb_entered}")
    print(f"  score_fail       : {_dbg_rej['pb_score']} / {_pb_entered}")
    print(f"  near_edge_fail   : {_dbg_rej['pb_edge']} / {_pb_entered}  ← MAIN BLOCKER if highest")
    print(f"  near_edge_pass   : {_pb_near_edge_pass} / {_pb_entered}")
    print(f"  spread_fail      : {_dbg_rej['pb_spread']} / {_pb_near_edge_pass}  ← spread_est=range_pct(20H), threshold=0.01(1%)")
    print(f"  vol_pen_applied  : {_dbg_rej['pb_vol_pen']} / {_pb_near_edge_pass}")
    print(f"  [SPREAD NOTE] spread_est is 20H price range (not bid-ask). Compressed coins have range 1-5%. Gate at 1% kills all.")
    print(f"  final_prebreak   : {len(pre_break_pool)} / {_pb_entered}")
    print(f"")
    print(f"  CONFLICT CHECK: compression requires tight range (range_pct<5%)")
    print(f"                  near_edge requires close within 1.5% of that range's edge")
    _range_midpoint_limit = 0.015 * 2   # price must be in top/bottom 30% of range for 5% range
    print(f"                  for range=5%: price must be in outer 30% of range to pass near_edge")
    print(f"                  for range=2%: midpoint passes (d=1% < 1.5%)")
    print(f"                  → conditions CAN coexist only when range_pct < ~3%")
    if _dbg_rej["pb_edge"] == _pb_entered - _dbg_rej["pb_tier_b"] - _dbg_rej["pb_score"]:
        print(f"  VERDICT: near_edge is SOLE BLOCKER — 0 symbols within 1.5% of range edge")
    print(f"===================================\n")
    return compression_pool, pre_break_pool


# =====================================================================
# TREND HELPERS
# =====================================================================

def _is_trend_strong(df1h, df4h=None, symbol=""):
    """
    [FIX Part 4] Relaxed trend detection — trend_ok thay vì trend_strong.
    - len >= 50 (từ 90)
    - ema_gap >= 0.002 (từ 0.005)
    - structure: partial ok (HH hoặc HL cho LONG)
    - H4: chỉ penalty score, không hard reject
    Returns (is_ok, side) | (False, None).
    """
    if df1h is None or len(df1h) < 50:   # [FIX] 90 → 50
        return False, None

    e34 = df1h["close"].ewm(span=34).mean().iloc[-2]
    e89 = df1h["close"].ewm(span=89).mean().iloc[-2]
    close = df1h["close"].iloc[-2]

    if math.isnan(e34) or math.isnan(e89) or close <= 0:
        return False, None
    ema_gap = abs(e34 - e89) / close
    if ema_gap < 0.001:   # [FIX] 0.002 → 0.001 (chỉ reject EMA hoàn toàn flat)
        print(f"[DBG][TREND] ema_gap={ema_gap:.4f} < 0.001 → REJECT")
        return False, None

    side = "LONG" if e34 > e89 else "SHORT"

    # [FIX Part 4] Structure: partial ok — chỉ cần 1 trong 2 điều kiện
    highs = [df1h["high"].iloc[i] for i in range(-12, -2)]
    lows  = [df1h["low"].iloc[i]  for i in range(-12, -2)]
    mid   = len(highs) // 2

    if side == "LONG":
        hh = max(highs[mid:]) > max(highs[:mid])
        hl = min(lows[mid:])  > min(lows[:mid])
        structure_ok = hh or hl or (ema_gap > 0.005)   # [FIX] EMA fallback
    else:
        lh = max(highs[mid:]) < max(highs[:mid])
        ll = min(lows[mid:])  < min(lows[:mid])
        structure_ok = lh or ll or (ema_gap > 0.005)   # [FIX] EMA fallback

    if not structure_ok:
        print(f"[DBG][TREND] {side} structure_ok=False → REJECT")
        return False, None

    # H4 is PRIMARY direction — H1 pullback within H4 trend is valid context
    if df4h is not None and len(df4h) >= 50:   # [FIX] 90 → 50
        h4_e34 = df4h["close"].ewm(span=34).mean().iloc[-2]
        h4_e89 = df4h["close"].ewm(span=89).mean().iloc[-2]
        h4_close_val = df4h["close"].iloc[-2]
        if math.isnan(h4_e34) or math.isnan(h4_e89) or h4_close_val <= 0:
            return True, side
        h4_gap  = abs(h4_e34 - h4_e89) / h4_close_val
        h4_side = "LONG" if h4_e34 > h4_e89 else "SHORT"
        if h4_side != side and h4_gap > 0.003:
            print(f"[DBG][TREND] symbol={symbol} h4={h4_side} h1={side} h4_gap={h4_gap:.4f} mode=pullback → PASS")
            return True, h4_side

    return True, side


def _has_valid_pullback(df1h, side):
    """
    [FIX Part 4] Relaxed pullback check — không yêu cầu giá phải chạm EMA,
    chỉ cần giá không quá xa (within 3% EMA) và structure chưa bẻ gãy.
    Volume: chỉ cần >= 30% avg (từ 50%).
    """
    if df1h is None or len(df1h) < 15:
        return False

    ema34     = df1h["close"].ewm(span=34).mean()
    ema89     = df1h["close"].ewm(span=89).mean()
    close_now = df1h["close"].iloc[-2]
    ema_now   = ema34.iloc[-2]
    avg_vol   = df1h["volume"].rolling(20).mean().iloc[-2]
    cur_vol   = df1h["volume"].iloc[-2]
    e34       = ema34.iloc[-2]
    e89       = ema89.iloc[-2]

    slope = (ema34.iloc[-2] - ema34.iloc[-6]) / max(close_now, 1e-9)

    # [FIX Part 4] Volume: 0.5 → 0.3 (moderate volume OK)
    vol_ok = avg_vol > 0 and cur_vol >= avg_vol * 0.3

    if side == "LONG":
        ema_gap     = abs(df1h["close"].ewm(span=34).mean().iloc[-2] - df1h["close"].ewm(span=89).mean().iloc[-2]) / max(close_now, 1e-9)
        pulled_back = close_now <= ema_now * 1.05   # [FIX] 1.03→1.15
        trend_ok    = (e34 > e89) and (slope > 0)
        not_broken  = df1h["low"].iloc[-5:-2].min() > df1h["low"].iloc[-15:-5].min() * 0.97
        result      = (pulled_back or (trend_ok and not _is_overextended(df1h))) and not_broken and vol_ok
        if not result:
            print(f"[DBG][TREND] LONG pullback fail | pb={pulled_back} nb={not_broken} vol={vol_ok} "
                  f"close={close_now:.4f} ema={ema_now:.4f}")
        return result
    else:
        ema_gap     = abs(df1h["close"].ewm(span=34).mean().iloc[-2] - df1h["close"].ewm(span=89).mean().iloc[-2]) / max(close_now, 1e-9)
        pulled_back = close_now >= ema_now * 0.95   # [FIX] 0.97→0.85
        trend_ok    = (e34 < e89) and (slope < 0)
        not_broken  = df1h["high"].iloc[-5:-2].max() < df1h["high"].iloc[-15:-5].max() * 1.03
        result      = (pulled_back or (trend_ok and not _is_overextended(df1h))) and not_broken and vol_ok
        if not result:
            print(f"[DBG][TREND] SHORT pullback fail | pb={pulled_back} nb={not_broken} vol={vol_ok} "
                  f"close={close_now:.4f} ema={ema_now:.4f}")
        return result


# =====================================================================
# PART 5 — TREND POOL (SEPARATE PATH)
# =====================================================================

def build_trend_pool(scan_data):
    """
    PART 5: trend_pool (~30-50).
    KHÔNG phụ thuộc compression. Path riêng hoàn toàn.
    Conditions: trend_strong + valid pullback + structure intact + volume OK.
    """
    trend_pool = []

    _dbg_trend = {"no_trend": 0, "no_pullback": 0, "pass": 0}

    for symbol, tier, df1h, df4h, df1d in scan_data:
        is_strong, side = _is_trend_strong(df1h, df4h, symbol)
        if not is_strong:
            _dbg_trend["no_trend"] += 1
            continue

        # [FIX] pullback: không reject hoàn toàn — chỉ penalty score nếu không có
        has_pullback = _has_valid_pullback(df1h, side)
        if not has_pullback:
            _dbg_trend["no_pullback"] += 1
            # không continue — giảm score thay vì bỏ qua

        # Volume: 0.3x avg (nới từ 0.4)
        vol     = df1h["volume"].iloc[-2]
        avg_vol = df1h["volume"].rolling(20).mean().iloc[-2]
        vol_ok  = avg_vol > 0 and vol >= avg_vol * 0.3

        # Trend score từ EMA distance
        e34   = df1h["close"].ewm(span=34).mean().iloc[-2]
        e89   = df1h["close"].ewm(span=89).mean().iloc[-2]
        close = df1h["close"].iloc[-2]
        if math.isnan(e34) or math.isnan(e89) or close <= 0:
            continue
        ema_gap     = abs(e34 - e89) / close
        if not math.isfinite(ema_gap):
            continue
        trend_score = min(5.0, ema_gap * 100)   # 0→5

        # penalty nếu không có pullback hoặc vol thấp
        if not has_pullback:
            trend_score = max(0.5, trend_score - 1.0)   # -1 penalty
        if not vol_ok:
            trend_score = max(0.5, trend_score - 0.5)

        _dbg_trend["pass"] += 1
        trend_pool.append({
            "symbol":            symbol,
            "tier":              tier,
            "trend_side":        side,
            "trend_score":       round(trend_score, 2),
            "compression_score": 0,
            "setup_type":        "TREND",
            "score":             round(trend_score, 2),
            "priority":          round(trend_score, 2),
        })

    trend_pool.sort(key=lambda x: x["trend_score"], reverse=True)
    trend_pool = trend_pool[:POOL_TREND_MAX]

    print(f"📈 TREND POOL: {len(trend_pool)} | "
          f"[DBG] no_trend={_dbg_trend['no_trend']} no_pullback={_dbg_trend['no_pullback']} pass={_dbg_trend['pass']}")
    return trend_pool


# =====================================================================
# PART 6 — MERGE + DEDUP + SCORE
# =====================================================================

def merge_dedup_pools(pre_break_pool, trend_pool):
    """
    PART 6: Strict merge + dedup + HYBRID scoring.
    pre_break_pool takes priority for dedup.
    Returns all_candidates dict {symbol: entry}.
    """
    all_candidates = {}

    # Step 1 — pre_break_pool goes first (priority)
    for t in pre_break_pool:
        all_candidates[t["symbol"]] = t.copy()

    # Step 2 — trend_pool: add if not duplicate, merge if duplicate
    for t in trend_pool:
        sym = t["symbol"]
        if sym not in all_candidates:
            all_candidates[sym] = t.copy()
        else:
            # MERGE — symbol in both pools
            existing = all_candidates[sym]
            existing["setup_type"]        = "HYBRID"
            existing["trend_score"]       = t.get("trend_score", 0)
            existing["compression_score"] = existing.get("compression_score", 0)
            # score = product của cả hai
            _cs = existing.get("compression_score", 0)
            _ts = existing.get("trend_score", 0)
            if _cs is None or _ts is None or not math.isfinite(_cs) or not math.isfinite(_ts):
                existing["score"] = 0.0
            else:
                existing["score"] = _cs * _ts

    # Step 3 — Boost HYBRID priority
    for sym, entry in all_candidates.items():
        raw_score = entry.get("score", 0)
        if raw_score is None or not math.isfinite(raw_score):
            raw_score = 0.0
            entry["score"] = 0.0
        if entry.get("setup_type") == "HYBRID":
            entry["priority"] = raw_score + 2.0
        else:
            entry["priority"] = raw_score

    return all_candidates


# =====================================================================
# PART 7 — FINAL RANKING + LIMIT
# =====================================================================

def build_confirm_pool(all_candidates):
    """
    PART 7: Sort by priority DESC, apply hard caps.
    Returns confirm_pool list (25-30 symbols).
    """
    sorted_list = sorted(
        (e for e in all_candidates.values()
         if e.get("priority") is not None and math.isfinite(e["priority"])),
        key=lambda x: x["priority"],
        reverse=True
    )

    compress_count = 0
    trend_count    = 0
    confirm_pool   = []

    for entry in sorted_list:
        if len(confirm_pool) >= POOL_CONFIRM_MAX:
            print(f"[PIPELINE DROP] symbol={entry['symbol']} stage=CONFIRM→POOL reason=POOL_CONFIRM_MAX({POOL_CONFIRM_MAX}) priority={round(entry.get('priority',0),2)}")
            break

        stype = entry.get("setup_type", "COMPRESSION")

        if stype == "COMPRESSION":
            if compress_count >= POOL_CAP_COMPRESS:
                print(f"[PIPELINE DROP] symbol={entry['symbol']} stage=CONFIRM→POOL reason=POOL_CAP_COMPRESS({POOL_CAP_COMPRESS}) priority={round(entry.get('priority',0),2)}")
                continue
            compress_count += 1
        elif stype == "TREND":
            if trend_count >= POOL_CAP_TREND:
                print(f"[PIPELINE DROP] symbol={entry['symbol']} stage=CONFIRM→POOL reason=POOL_CAP_TREND({POOL_CAP_TREND}) priority={round(entry.get('priority',0),2)}")
                continue
            trend_count += 1
        # HYBRID: counts toward both, no double-cap

        confirm_pool.append(entry)

    # [PIPELINE LOG] Log all sorted_list symbols that didn't make confirm_pool
    _confirmed_set = {e["symbol"] for e in confirm_pool}
    for _e in sorted_list:
        if _e["symbol"] not in _confirmed_set:
            print(f"[PIPELINE DROP] symbol={_e['symbol']} stage=CANDIDATES→CONFIRM reason=not_in_confirm_pool type={_e.get('setup_type','?')} priority={round(_e.get('priority',0),2)}")

    print(
        f"✅ CONFIRM POOL: {len(confirm_pool)} "
        f"(COMPRESS={compress_count}, TREND={trend_count}, "
        f"HYBRID={sum(1 for e in confirm_pool if e.get('setup_type')=='HYBRID')})"
    )
    return confirm_pool


# =====================================================================
# ENTRY POINT
# =====================================================================

def build_pool_pipeline(raw_data_pool, stats=None):
    """
    Entry point: chạy toàn bộ pipeline pool selection.
    raw_data_pool: list of (symbol, tier_label, df5, df15, df1h, df4h, df1d)
    stats: optional dict from v8.py to update pool stats keys (passed by reference)
    Returns: confirm_pool list of dicts (symbol + metadata).

    Return format:
    [
        {
            "symbol": str,
            "score": float,
            "priority": float,
            "setup_type": str,
            ...
        }
    ]
    """
    # Chuẩn bị scan_data: chỉ cần df1h + df4h + df1d cho pool
    scan_data = [
        (sym, tier, df1h, df4h, df1d)
        for sym, tier, _df5, _df15, df1h, df4h, df1d in raw_data_pool
        if df1h is not None and len(df1h) >= 22
    ]

    # [PIPELINE LOG] Log symbols dropped by df1h validity filter
    for _sym, _tier, _df5, _df15, _df1h, _df4h, _df1d in raw_data_pool:
        if _df1h is None or len(_df1h) < 22:
            _reason = "df1h=None" if _df1h is None else f"df1h_len={len(_df1h)}<22"
            print(f"[PIPELINE DROP] symbol={_sym} stage=SCAN→POOL reason={_reason}")

    # [LOG] SCAN stage — log trước khi chạy pipeline (1 lần duy nhất)
    for sym, tier, df1h, df4h, df1d in scan_data:
        log_pool_stage(sym, "SCAN", setup_type=tier, reason=f"tier={tier}")

    # Chạy pipeline (1 lần duy nhất — đã fix duplicate)
    compression_pool, pre_break_pool = build_compression_pool(scan_data)
    trend_pool                        = build_trend_pool(scan_data)
    all_candidates                    = merge_dedup_pools(pre_break_pool, trend_pool)
    confirm_pool                      = build_confirm_pool(all_candidates)

    # [LOG] COMPRESSION stage
    for e in compression_pool:
        log_pool_stage(e["symbol"], "COMPRESSION",
                       score=e.get("compression_score"),
                       setup_type="COMPRESSION",
                       reason=e.get("compression_type", ""))

    # [LOG] PRE_BREAK stage
    for e in pre_break_pool:
        log_pool_stage(e["symbol"], "PRE_BREAK",
                       score=e.get("compression_score"),
                       setup_type="COMPRESSION",
                       reason=f"near_edge={e.get('near_edge')}")

    # [LOG] TREND stage
    for e in trend_pool:
        log_pool_stage(e["symbol"], "TREND",
                       score=e.get("trend_score"),
                       setup_type="TREND",
                       reason=f"side={e.get('trend_side')}")

    # [LOG] CONFIRM stage
    for e in confirm_pool:
        log_pool_stage(e["symbol"], "CONFIRM",
                       score=e.get("priority"),
                       setup_type=e.get("setup_type"),
                       reason=f"priority={round(e.get('priority',0),2)}")

    # Track stats (update caller's stats dict if provided)
    if stats is not None:
        stats["pool_compress_size"] = len(compression_pool)
        stats["pool_prebreak_size"] = len(pre_break_pool)
        stats["pool_trend_size"]    = len(trend_pool)
        stats["pool_confirm_size"]  = len(confirm_pool)

    # ===== PART 5: VALIDATION PRINT =====
    print(f"\n{'='*50}")
    print(f"  POOL SCAN          : {len(scan_data)}")
    print(f"  COMPRESSION POOL   : {len(compression_pool)}  (target 40-80)")
    print(f"  PRE-BREAK POOL     : {len(pre_break_pool)}  (target 20-40)")
    print(f"  TREND POOL         : {len(trend_pool)}  (target 20-60)")
    print(f"  CONFIRM POOL       : {len(confirm_pool)}  (target 40-50) ← sau đây EMA/BOS filter còn ~5-15 candidates")
    print(f"{'='*50}\n")

    if len(confirm_pool) < 20:
        print(f"[WARN] CONFIRM POOL={len(confirm_pool)} < 20 → sau EMA/BOS filter sẽ còn rất ít/0 candidates!")
    if len(pre_break_pool) == 0:
        print("[INFO] PRE-BREAK = 0 → market đang trend / breakout, dùng TREND pool")
    if len(trend_pool) <= 5:
        print("[WARN] TREND POOL <= 5 → check _is_trend_strong / _has_valid_pullback debug")

    _pool_log_file = _log_path("log_pool_pipeline.csv")
    try:
        with open(_pool_log_file, "r", encoding="utf-8") as _f:
            _pool_rows = sum(1 for _ in _f)
        print(f"[DBG][LOG_POOL] log_pool_pipeline.csv rows={_pool_rows}")
    except Exception as _e:
        print(f"[DBG][LOG_POOL] cannot read log_pool_pipeline.csv: {_e}")

    return confirm_pool

