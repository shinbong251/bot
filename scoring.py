import requests
import certifi
from trend import trend_h1
from bos import bos_count
# === SCORING MODULE START ===

def candle_metrics(c):
    open_ = c["open"]
    close = c["close"]
    high  = c["high"]
    low   = c["low"]
 
    body = abs(close - open_)
    rng  = high - low if high - low > 0 else 1e-9
 
    br = body / rng
    ur = (high - max(open_, close)) / rng
    lr = (min(open_, close) - low) / rng
    cp = (close - low) / rng
 
    return br, ur, lr, cp


def classify_candle_v3(df, side, retest=False, spring=False, phase=None):
    # ADDED: phase param for exhaustion soft limiter
    c         = df.iloc[-2]
    prev      = df.iloc[-3]
    avg_range = (df["high"] - df["low"]).rolling(10).mean().iloc[-2]
    rng_c     = c["high"] - c["low"]
    br, ur, lr, cp = candle_metrics(c)
    br_prev, _, _, _ = candle_metrics(prev)
 
    core    = 0
    confirm = 0
    reason  = []
 
    # ── RANGE FILTER ──
    if avg_range > 0 and rng_c < avg_range * 0.2:
        return None
 
    # ── CANDLE CLASS ──
    if br >= 0.5:
        candle_class = "STRONG"
    elif br >= 0.3:
        candle_class = "NORMAL"
    else:
        candle_class = "WEAK"
 
    # ── STRONG candle ──
    if br >= 0.5 and (
        (side == "LONG"  and cp > 0.65) or
        (side == "SHORT" and cp < 0.35)
    ):
        core    += 2
        confirm += 1
        reason.append("Strong")
 
    # ── ENGULF ──
    engulf = (
        side == "LONG"
        and c["close"] > c["open"]
        and c["close"] > prev["open"]
        and c["open"] < prev["close"]
        and prev["close"] < prev["open"]
    ) or (
        side == "SHORT"
        and c["close"] < c["open"]
        and c["close"] < prev["open"]
        and c["open"] > prev["close"]
        and prev["close"] > prev["open"]
    )
 
    if engulf and br >= 0.4:
        core    += 2
        confirm += 1
        reason.append("Engulf")
 
    # ── PIN ──
    if side == "LONG" and lr >= 0.55 and br < 0.45:
        if cp >= 0.45:
            confirm += 1
            reason.append("Pin")
 
    if side == "SHORT" and ur >= 0.55 and br < 0.45:
        if cp <= 0.55:
            confirm += 1
            reason.append("Pin")
 
    # ── DOJI (context only, no reject) ──
    if br < 0.15:
        if retest or spring:
            confirm += 1
            reason.append("Doji")
        # else: neutral pass-through
 
    # ── CONTINUATION (2-candle) ──
    if side == "LONG" and br_prev >= 0.4 and c["high"] > prev["high"]:
        if br >= 0.3 and c["close"] > prev["close"]:
            core    += 1
            confirm += 1
            reason.append("Cont")
 
    if side == "SHORT" and br_prev >= 0.4 and c["low"] < prev["low"]:
        if br >= 0.3 and c["close"] < prev["close"]:
            core    += 1
            confirm += 1
            reason.append("Cont")
 
    # ── FAKE BREAK (rejection) ──
    if side == "LONG" and c["low"] < prev["low"] and c["close"] > prev["low"]:
        core    += 1
        confirm += 1
        reason.append("FakeBreak")
 
    if side == "SHORT" and c["high"] > prev["high"] and c["close"] < prev["high"]:
        core    += 1
        confirm += 1
        reason.append("FakeBreak")
 
    # ── RETEST bonus ──
    if retest:
        core += 1
        reason.append("Retest")
 
    # ── WEAK candle soft penalty ──
    if candle_class == "WEAK" and not (retest or spring):
        confirm = max(0, confirm - 1)
 
    # ── ADDED: only-weak-signals balancing ──
    has_strong_signal = any(r in reason for r in ("Strong", "Engulf", "FakeBreak"))
    if not has_strong_signal and confirm > 0:
        confirm = max(0, confirm - 1)
        reason.append("WeakOnly_pen")
 
    # ── ADDED: exhaustion soft limiter ──
    if phase == "EXHAUSTION":
        confirm = max(0, confirm - 1)
        reason.append("Exhaustion_pen")
 
    # ── SCORING V3 ──
    score = core + confirm
 
    if score < 2:
        return None
 
    if core == 1 and confirm == 0:
        return None
 
    if score >= 4:
        entry_type = "CONFIRM"
    else:
        entry_type = "EARLY"
 
    reason.append(f"ET:{entry_type}")
 
    return core, confirm, reason


def calc_rsi(df):
    d = df["close"].diff()
    gain = d.clip(lower=0).rolling(14).mean()
    loss = (-d.clip(upper=0)).rolling(14).mean()
    rs = gain/(loss+1e-9)
    return 100-(100/(1+rs))


def rsi_div(df, side):
    rsi = calc_rsi(df)
    close = df["close"]

    low1 = close.iloc[-6:-3].min()
    low2 = close.iloc[-3:].min()

    r1 = rsi.iloc[-6:-3].min()
    r2 = rsi.iloc[-3:].min()

    return (low2 < low1 and r2 > r1) if side=="LONG" else (low2 > low1 and r2 < r1)


def vol_div(df):
    v1 = df["volume"].iloc[-6:-3].mean()
    v2 = df["volume"].iloc[-3:].mean()
    return v2 < v1 * 0.8


def confidence(df, side):
    score, r = 0, []

    if rsi_div(df, side):
        score+=2; r.append("RSI div")

    if vol_div(df):
        score+=1; r.append("Vol weak")

    rsi = calc_rsi(df).iloc[-2]
    if side=="LONG" and rsi>50:
        score+=1; r.append("RSI>50")
    if side=="SHORT" and rsi<50:
        score+=1; r.append("RSI<50")

    return score, r


def detect_structure(df):
    highs = df["high"]
    lows = df["low"]

    hh = highs.iloc[-2] > highs.iloc[-4] and highs.iloc[-4] > highs.iloc[-6]
    hl = lows.iloc[-2] > lows.iloc[-4]

    ll = lows.iloc[-2] < lows.iloc[-4] and lows.iloc[-4] < lows.iloc[-6]
    lh = highs.iloc[-2] < highs.iloc[-4]

    if hh and hl:
        return "LONG"
    if ll and lh:
        return "SHORT"
    return None


def calc_score_v66(t, df, df15, df1h):
    """
    Spec FINAL v7 scoring:
    score = core
    + wyckoff (split TREND vs REVERSAL)
    + trap
    + exhaustion penalty
    + reversal bonus
    + retest
    + volume
    + RR
    """
    score = t["core"]
    is_reversal = t.get("entry_type", "").startswith("REVERSAL")
    wyckoff = t.get("wyckoff", "NONE")
    trap_valid = t.get("trap_valid", False)
    exhaustion_cls = t.get("exhaustion_cls", "HEALTHY")
    retest_strength = t.get("retest_strength")
    volume_ok = t.get("volume_ok", False)
    volume_spike = t.get("volume_spike", False)

    if not is_reversal:
        if wyckoff == "STRONG":
            score += 2
        elif wyckoff == "MEDIUM":
            score += 1
    else:
        if wyckoff == "STRONG":
            score += 4
        elif wyckoff == "MEDIUM":
            score += 2

    if trap_valid:
        score += 1
    else:
        score -= 1

    if exhaustion_cls == "EXTENDED":
        score -= 1
    elif exhaustion_cls == "EXHAUSTED":
        score -= 2

    if exhaustion_cls == "EXHAUSTED" and wyckoff not in (None, "NONE"):
        score += 2

    if retest_strength == "STRONG":
        score += 2
    elif retest_strength == "NORMAL":
        score += 1
    elif retest_strength == "WEAK":
        score -= 2

    if volume_spike:
        score += 1
    if volume_ok:
        score += 1
    elif not volume_ok:
        score -= 1

    rr = t.get("rr", 0)
    if rr < 1:
        score -= 4
    elif rr < 1.3:
        score -= 2
    elif rr >= 2:
        score += 1

    h1 = trend_h1(df1h)
    if t["side"] == h1:
        score += 1
    else:
        score -= 1

    return round(score, 2)


def calc_score_v68(t, df, df15, df1h):
    # Redirect to v66
    return calc_score_v66(t, df, df15, df1h)


def get_risk_percent(entry_type):
    base = RISK_PER_TRADE

    mapping = {
        "EARLY": base * 0.5,
        "CONFIRM": base * 1.0,
        "RETEST": base * 1.0,
        "RETEST_STRONG": base * 1.2,
        "REVERSAL_EARLY": base * 0.5,
        "REVERSAL_CONFIRM": base * 0.7,
        "TREND_CONFIRM":    base * 1.0,
        "TREND_LIMIT":      base * 1.0,
        "REVERSAL_LIMIT":   base * 0.7,
        "EARLY_TIER0":   base * 0.3,
    }

    return mapping.get(entry_type, base)


def compute_risk_pct(entry_type, final_score):
    if entry_type == "CONFIRM":
        if final_score >= 9:
            return 1.3
        elif final_score >= 8:
            return 1.1
        elif final_score >= 7:
            return 0.9
        elif final_score >= 6:
            return 0.7
        else:
            return 0.5

    elif entry_type.startswith("SWING"):
        if final_score >= 8:
            return 1.2
        elif final_score >= 7:
            return 1.0
        elif final_score >= 6:
            return 0.85
        else:
            return 0.6

    elif entry_type.startswith("REVERSAL"):
        if final_score >= 8:
            return 1.1
        elif final_score >= 7:
            return 0.95
        elif final_score >= 6:
            return 0.8
        else:
            return 0.5

    elif entry_type.startswith("EARLY"):
        if final_score >= 7:
            return 1.0
        elif final_score >= 6:
            return 0.8
        else:
            return 0.5

    else:
        return 0.5


def get_adaptive_tp(entry_real, sl_real, side, entry_type, is_reversal, volatility, atr):
    """
    [V7] TP theo RR target khác nhau cho từng entry_type.
    REVERSAL target RR cao hơn vì rủi ro cao hơn.
    """
    risk = abs(entry_real - sl_real)

    if risk == 0:
        return None, 0

    if is_reversal:
        rr_target = 2.5
    elif entry_type in ("TREND_CONFIRM", "EARLY"):
        rr_target = 1.8
    elif entry_type in ("RETEST_STRONG",):
        rr_target = 2.0
    else:
        rr_target = 1.5

    if side == "LONG":
        tp = entry_real + risk * rr_target
    else:
        tp = entry_real - risk * rr_target

    rr = rr_target
    return tp, rr


def calc_tier_metrics(df15, side_h1):
    """Tính các metrics dùng cho tier classification."""
    prev_high = df15["high"].iloc[-20:-2].max()
    prev_low  = df15["low"].iloc[-20:-2].min()
    price     = df15["close"].iloc[-2]
    rng       = prev_high - prev_low

    price_position = (price - prev_low) / rng if rng > 0 else 0
    range_pct      = rng / price if price > 0 else 0

    if side_h1 == "LONG":
        dist_to_level = abs(price - prev_high) / prev_high
        dist_low      = abs(price - prev_low)  / prev_low
    else:
        dist_to_level = abs(price - prev_low)  / prev_low
        dist_low      = abs(price - prev_high) / prev_high

    return {
        "prev_high":      prev_high,
        "prev_low":       prev_low,
        "price":          price,
        "price_position": round(price_position, 4),
        "dist_to_level":  round(dist_to_level,  4),
        "dist_low":       round(dist_low,        4),
        "range_pct":      round(range_pct,       4),
    }

def count_repeated_tests(df15, side_h1, dist_thresh=0.003, window=20):
    """
    Đếm số lần price test level trong window nến gần nhất.
    Test = close trong khoảng dist_thresh của level.
    """
    prev_high = df15["high"].iloc[-20:-2].max()
    prev_low  = df15["low"].iloc[-20:-2].min()
    level     = prev_high if side_h1 == "LONG" else prev_low

    count = 0
    for i in range(-window, -2):
        p = df15["close"].iloc[i]
        if abs(p - level) / level <= dist_thresh:
            count += 1
    return count


def detect_wick_sweep(df15, side_h1):
    """
    Wick sweep: nến có wick vượt level nhưng close trong range.
    = false break = liquidity sweep = Tier 0 signal.
    """
    prev_high = df15["high"].iloc[-20:-2].max()
    prev_low  = df15["low"].iloc[-20:-2].min()
    last      = df15.iloc[-2]

    if side_h1 == "LONG":
        return last["high"] > prev_high and last["close"] < prev_high
    else:
        return last["low"]  < prev_low  and last["close"] > prev_low

def detect_exhaustion_v66(df15, side, bos_n):
    """
    Spec:
    bos_n >= 3 → +1 | bos_n >= 5 → +2
    dist_from_EMA50 > 2% → +1 | dist > 4% → +2
    range > 1.8x avg → +1
    volume > 2x avg → +1
    CLASS: <=1 HEALTHY, <=3 EXTENDED, >3 EXHAUSTED
    """
    exhaustion = 0

    # BOS count
    if bos_n >= 5:
        exhaustion += 2
    elif bos_n >= 3:
        exhaustion += 1

    # Distance from EMA50
    ema50 = df15["close"].ewm(span=50).mean().iloc[-2]
    price = df15["close"].iloc[-2]
    dist = abs(price - ema50) / ema50

    if dist > 0.04:
        exhaustion += 2
    elif dist > 0.02:
        exhaustion += 1

    # Range vs avg
    recent_range = df15["high"].iloc[-2] - df15["low"].iloc[-2]
    avg_range = (df15["high"] - df15["low"]).rolling(20).mean().iloc[-2]
    if avg_range > 0 and recent_range > avg_range * 1.8:
        exhaustion += 1

    # Volume
    vol = df15["volume"].iloc[-2]
    avg_vol = df15["volume"].rolling(20).mean().iloc[-2]
    if avg_vol > 0 and vol > avg_vol * 2:
        exhaustion += 1

    # Classify
    if exhaustion <= 1:
        cls = "HEALTHY"
    elif exhaustion <= 3:
        cls = "EXTENDED"
    else:
        cls = "EXHAUSTED"

    return cls, exhaustion

def get_funding_rate(symbol):
    """
    Funding rate > 0.1%  → quá nhiều LONG → nguy hiểm khi LONG
    Funding rate < -0.1% → quá nhiều SHORT → nguy hiểm khi SHORT
    """
    try:
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/fundingRate",
            params={"symbol": symbol, "limit": 1},
            verify=certifi.where(),
            timeout=5
        ).json()
        return float(r[0]["fundingRate"]) if r else 0
    except:
        return 0

def detect_early_signal(symbol, df15, side_h1):
    tm           = calc_tier_metrics(df15, side_h1)
    repeat_count = count_repeated_tests(df15, side_h1)
    wick_sweep   = detect_wick_sweep(df15, side_h1)
    exh_cls, _   = detect_exhaustion_v66(df15, side_h1, bos_count(df15, side_h1))
    vol          = df15["volume"].iloc[-2]
    avg_vol      = df15["volume"].rolling(20).mean().iloc[-2]
    vol_ratio    = round(vol / avg_vol, 2) if avg_vol > 0 else 0
    funding      = get_funding_rate(symbol)
    funding_pen  = (1 if side_h1 == "LONG"  and funding >  0.001 else
                    1 if side_h1 == "SHORT" and funding < -0.001 else 0)

    meta = {
        "pp":           tm["price_position"],
        "dist":         tm["dist_to_level"],
        "repeat_count": repeat_count,
        "wick_sweep":   wick_sweep,
        "exhaustion":   exh_cls,
        "vol_ratio":    vol_ratio,
        "funding_pen":  funding_pen,
        "block":        None,
    }

    # DETECTION: có tín hiệu tích lũy không?
    # repeat >= 1 HOẶC wick_sweep — ngưỡng thấp hơn cũ (cũ >= 2)
    # Lý do: detected phải capture được nhiều hơn để funnel có ý nghĩa
    has_signal = repeat_count >= 2 or wick_sweep
    if not has_signal:
        return False, meta   # không detect → không tăng early_detected

    return True, meta

# Vị trí: thêm ngay sau detect_early_signal()
# Trả về (valid: bool, meta: dict đã update block)
# Mỗi hard gate thất bại → ghi block reason → caller tăng counter tương ứng

def classify_tier(tm, side_h1, bos_type, ema_align, repeat_count, wick_sweep):
    """
    Phân loại tier dựa trên metrics.
    Returns: tier (0/1/2/None=reject)
    """
    pp   = tm["price_position"]
    dist = tm["dist_to_level"]
    dl   = tm["dist_low"]

    if (pp >= 0.75
            and dist <= 0.001
            and dl >= 0.02):
        return 2

    if (bos_type in ("CONFIRM", "STRONG")
            and ema_align == "ALIGN"
            and pp >= 0.85
            and dist <= 0.003):
        return 1

    has_accumulation = (
        (pp >= 0.65 and dist <= 0.002 and repeat_count >= 2)
        or (wick_sweep and dist <= 0.003 and pp >= 0.6)
    )
    if has_accumulation:
        return 0

    if bos_type == "NEAR" and dist <= 0.01:
        return 2

    return None


def calc_ema_state(df15, side):
    """EMA state: ALIGN / PARTIAL / MISALIGN + slope direction."""
    ema34  = df15["close"].ewm(span=34).mean()
    ema89  = df15["close"].ewm(span=89).mean()
    e34    = ema34.iloc[-2]
    e89    = ema89.iloc[-2]
    slope = (ema34.iloc[-2] - ema34.iloc[-6]) / max(e34, 1e-9)

    if side == "LONG":
        align = e34 > e89 and slope > 0
        partial = (e34 > e89 and slope >= 0) or (slope > 0.001)
    else:
        align = e34 < e89 and slope < 0
        partial = (e34 < e89 and slope <= 0) or (slope < -0.001)

    ema_align  = "ALIGN"   if align   else \
                 "PARTIAL" if partial else "MISALIGN"
    ema_slope  = "UP"   if slope > 0.001 else \
                 "DOWN" if slope < -0.001 else "FLAT"

    return ema_align, ema_slope


def highlight_tag(t, rr):
    score = t["score"]

    if score >= 12:
        return "🔥"

    reason_str = ",".join(t["reason"])
    if score >= 9 and rr >= 1.5 and "Retest STRONG" in reason_str:
        return "🟢"

    if t["entry_type"].startswith("REVERSAL"):
        return "⚠️"

    return ""


def get_decayed_score(t, elapsed_seconds):
    """
    [U4] Score giảm dần theo thời gian nằm trong buffer.
    Signal cũ = kém tin cậy hơn vì context thị trường đã thay đổi.

    0-60s:   không decay  → signal còn tươi
    60-120s: -0.5         → bắt đầu cũ
    120-180s: -1.0        → gần hết hạn
    >180s:   bị expire bởi filter trước đó (< 180s)
    """
    base = t.get("priority", t["score"])
    decay = (elapsed_seconds // 60) * 0.5
    return base - decay


def apply_signal_scoring(t):
    score = t.get("score", 0)
    max_profit_r = t.get("max_profit_r", 0)
    score -= (max_profit_r * 3)
    return round(score, 2)

# === SCORING MODULE END ===

def get_candle_class(df15, side):
    """
    Simplified candle classification wrapper using classify_candle_v3().
    Returns: "STRONG" | "NORMAL" | "WEAK" | None
    """
    candle_info = classify_candle_v3(df15, side)
    if candle_info is None:
        return None

    c = df15.iloc[-2]
    br, _, _, cp = candle_metrics(c)

    if br < 0.3:
        return "WEAK"

    if side == "LONG":
        close_ok = cp > 0.65
    elif side == "SHORT":
        close_ok = cp < 0.35
    else:
        close_ok = False

    if br >= 0.5 and close_ok:
        return "STRONG"

    return "NORMAL"


def score_signal(bos_type, ema_align, h1_trend, signal_side, candle_class, rr, market_state):
    if rr is None or rr < 1.0:
        return None

    bos_score = {
        "STRONG": 2,
        "CONFIRM": 1,
        "TRUE": 0,
        "WEAK": -1,
        "TRAP": -2,
        None: -1,
    }.get(bos_type, -1)

    ema_score = {
        "ALIGN": 2,
        "PARTIAL": 1,
        "MISALIGN": -2,
        None: -2,
    }.get(ema_align, -2)

    if h1_trend is None or signal_side is None:
        trend_score = 0
    elif h1_trend == signal_side:
        trend_score = 2
    else:
        trend_score = -1

    candle_score = {
        "STRONG": 2,
        "NORMAL": 1,
        "WEAK": -1,
        None: -2,
    }.get(candle_class, -2)

    if rr >= 2.0:
        rr_score = 2
    elif rr >= 1.5:
        rr_score = 1
    else:
        rr_score = 0

    context_score = {
        "TREND": 1,
        "ACCUMULATION": 0,
        "NEUTRAL": 0,
        "EXHAUSTION": -1,
        "DEAD": -1,
    }.get(market_state, 0)

    total = bos_score + ema_score + trend_score + candle_score + rr_score + context_score

    breakdown = {
        "bos": bos_score,
        "ema": ema_score,
        "trend": trend_score,
        "candle": candle_score,
        "rr": rr_score,
        "context": context_score,
        "total": total,
    }

    return total, breakdown


# === EARLY_CONT: CONTINUATION CONFIDENCE MODEL START ===

def detect_pullback_quality(df15, side):
    """
    Measure pullback depth from the recent swing high/low.
    Returns (pullback_depth: float, label: str)
    """
    recent_high = df15["high"].iloc[-10:-2].max()
    recent_low  = df15["low"].iloc[-10:-2].min()
    price       = df15["close"].iloc[-2]
    rng         = recent_high - recent_low

    if rng <= 0:
        return 0.0, "UNKNOWN"

    if side == "LONG":
        pullback_depth = (recent_high - price) / rng
    else:
        pullback_depth = (price - recent_low) / rng

    if pullback_depth <= 0.20:
        label = "SHALLOW"
    elif pullback_depth <= 0.382:
        label = "MODERATE"
    elif pullback_depth <= 0.50:
        label = "DEEP"
    else:
        label = "VERY_DEEP"

    return round(pullback_depth, 4), label


def calc_continuation_confidence(df15, side, bos_n, h1_trend_val):
    """
    Continuation confidence score for EARLY_CONT entries.
    Measures trend continuation quality — NOT accumulation or reversal probability.
    Returns (score: float, factors: list[str])
    """
    score   = 0.0
    factors = []

    # 1. EMA alignment + spread expansion quality
    ema21 = df15["close"].ewm(span=21).mean()
    ema34 = df15["close"].ewm(span=34).mean()
    ema89 = df15["close"].ewm(span=89).mean()

    e21      = ema21.iloc[-2]
    e34      = ema34.iloc[-2]
    e89      = ema89.iloc[-2]
    e21_prev = ema21.iloc[-6]
    e89_prev = ema89.iloc[-6]

    spread_now  = abs(e21 - e89)
    spread_prev = abs(e21_prev - e89_prev)
    spread_expanding = spread_now > spread_prev * 1.02

    if side == "LONG":
        ema_full_align    = e21 > e34 and e34 > e89
        ema_partial_align = e34 > e89
    else:
        ema_full_align    = e21 < e34 and e34 < e89
        ema_partial_align = e34 < e89

    if ema_full_align and spread_expanding:
        score += 2.5
        factors.append("EMA_ALIGN_EXPAND")
    elif ema_full_align:
        score += 1.5
        factors.append("EMA_ALIGN")
    elif ema_partial_align:
        score += 0.5
        factors.append("EMA_PARTIAL")
    else:
        score -= 2.0
        factors.append("EMA_MISALIGN")

    # 2. Directional momentum (5-candle move)
    close_now  = df15["close"].iloc[-2]
    close_prev = df15["close"].iloc[-7]
    move_5 = (close_now - close_prev) / close_prev if close_prev > 0 else 0.0

    if side == "LONG":
        if move_5 > 0.015:
            score += 2.0
            factors.append("STRONG_MOVE")
        elif move_5 > 0.005:
            score += 1.0
            factors.append("MODERATE_MOVE")
        elif move_5 < -0.005:
            score -= 1.5
            factors.append("COUNTER_MOVE")
    else:
        if move_5 < -0.015:
            score += 2.0
            factors.append("STRONG_MOVE")
        elif move_5 < -0.005:
            score += 1.0
            factors.append("MODERATE_MOVE")
        elif move_5 > 0.005:
            score -= 1.5
            factors.append("COUNTER_MOVE")

    # 3. BOS freshness (positive signal for fresh structure breaks)
    if bos_n == 0:
        score -= 1.0
        factors.append("NO_BOS_HIST")
    elif bos_n == 1:
        score += 2.0
        factors.append("FRESH_BOS")
    elif bos_n == 2:
        score += 1.0
        factors.append("MODERATE_BOS")
    # bos_n >= 3: maturity penalty applied below

    # 4. Pullback quality
    pb_depth, pb_label = detect_pullback_quality(df15, side)
    if pb_label == "SHALLOW":
        score += 2.0
        factors.append("SHALLOW_PB")
    elif pb_label == "MODERATE":
        score += 1.0
        factors.append("MODERATE_PB")
    elif pb_label == "DEEP":
        factors.append("DEEP_PB")
    elif pb_label == "VERY_DEEP":
        score -= 2.0
        factors.append("VERY_DEEP_PB")

    # 5. Continuation candle quality
    candle  = df15.iloc[-2]
    prev_c  = df15.iloc[-3]
    c_body  = abs(candle["close"] - candle["open"])
    c_rng   = candle["high"] - candle["low"] if candle["high"] > candle["low"] else 1e-9
    br      = c_body / c_rng
    cp      = (candle["close"] - candle["low"]) / c_rng

    avg_range = (df15["high"] - df15["low"]).rolling(20).mean().iloc[-2]

    if side == "LONG":
        strong_cont = (
            candle["close"] > candle["open"]
            and candle["close"] > prev_c["close"]
            and br >= 0.5
            and cp > 0.60
        )
        reversal_c = candle["close"] < candle["open"] and br >= 0.4
    else:
        strong_cont = (
            candle["close"] < candle["open"]
            and candle["close"] < prev_c["close"]
            and br >= 0.5
            and cp < 0.40
        )
        reversal_c = candle["close"] > candle["open"] and br >= 0.4

    if strong_cont:
        score += 2.0
        factors.append("CONT_CANDLE")
    elif reversal_c:
        score -= 1.5
        factors.append("REVERSAL_CANDLE")

    # Over-extended candle = exhaustion impulse
    if avg_range > 0 and c_rng > avg_range * 2.0:
        score -= 1.5
        factors.append("CANDLE_EXHAUSTION")

    # 6. Volume sustain + counter-volume spike
    vol     = df15["volume"].iloc[-2]
    avg_vol = df15["volume"].rolling(20).mean().iloc[-2]
    if avg_vol > 0:
        vol_ratio = vol / avg_vol
        if vol_ratio >= 1.3:
            score += 1.5
            factors.append("VOL_SUSTAIN")
        elif vol_ratio < 0.7:
            score -= 1.0
            factors.append("VOL_WEAK")

        prev_vol = df15["volume"].iloc[-3]
        prev_c2  = df15.iloc[-3]
        if prev_vol > avg_vol * 1.5:
            if side == "LONG" and prev_c2["close"] < prev_c2["open"]:
                score -= 1.5
                factors.append("COUNTER_VOL_SPIKE")
            elif side == "SHORT" and prev_c2["close"] > prev_c2["open"]:
                score -= 1.5
                factors.append("COUNTER_VOL_SPIKE")

    # 7. H1 trend alignment
    if h1_trend_val and h1_trend_val == side:
        score += 1.5
        factors.append("HTF_ALIGN")
    elif h1_trend_val and h1_trend_val != side:
        score -= 2.0
        factors.append("HTF_CONTRA")

    # 8. BOS maturity penalty (late-trend protection)
    if bos_n >= 5:
        score -= 3.0
        factors.append("BOS_MATURE_5")
    elif bos_n >= 3:
        score -= 1.5
        factors.append("BOS_MATURE_3")

    # 9. RSI divergence
    if rsi_div(df15, side):
        score -= 2.0
        factors.append("RSI_DIV")

    # 10. Compression stall
    range_now  = df15["high"].iloc[-5:-1].max() - df15["low"].iloc[-5:-1].min()
    range_prev = df15["high"].iloc[-10:-5].max() - df15["low"].iloc[-10:-5].min()
    if range_prev > 0 and range_now < range_prev * 0.4:
        score -= 1.5
        factors.append("COMPRESSION")

    return round(score, 2), factors

# === EARLY_CONT: CONTINUATION CONFIDENCE MODEL END ===
