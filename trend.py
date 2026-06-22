# === TREND MODULE START ===
import math

def is_overextended(df):
    # ===== 1. CANDLE SPIKE (giữ lại) =====
    open_ = df["open"].iloc[-2]
    close = df["close"].iloc[-2]
    body = abs(close - open_)

    avg_range = (df["high"] - df["low"]).rolling(20).mean().iloc[-2]
    spike = body > avg_range * 1.8

    # ===== 2. MULTI-CANDLE MOVE (NEW) =====
    close_now = df["close"].iloc[-2]
    close_prev = df["close"].iloc[-6]

    move = abs(close_now - close_prev) / max(close_prev, 1e-9)
    trend_run = move > 0.04   # >4% trong 4 nến

    # ===== 3. EMA DISTANCE (NEW - optional nhưng rất mạnh) =====
    ema34 = df["close"].ewm(span=34).mean().iloc[-2]
    ema_dist = abs(close_now - ema34) / max(ema34, 1e-9)

    far_from_ema = ema_dist > 0.03   # >3% xa EMA

    return spike or trend_run or far_from_ema

def trend_strength(df15):
    move = abs(df15["close"].iloc[-2] - df15["close"].iloc[-10])
    base = df15["close"].iloc[-10]
    return move / base

def get_market_state(df15):
    """
    Classify current market state from M15 data.
    Returns: state string + dict of key metrics.
    """
    if df15 is None or len(df15) < 25:
        return "NEUTRAL", {}

    close  = df15["close"].iloc[-2]
    closes = df15["close"]

    # Impulse: move over last 10 candles
    impulse = abs(close - closes.iloc[-10]) / closes.iloc[-10] if closes.iloc[-10] > 0 else 0

    # Volume comparison
    vol_now  = df15["volume"].iloc[-5:-1].mean()
    vol_prev = df15["volume"].iloc[-20:-5].mean()
    vol_ratio = vol_now / vol_prev if vol_prev > 0 else 1.0

    # Structure
    highs = df15["high"]
    lows  = df15["low"]
    trend_up   = highs.iloc[-2] > highs.iloc[-5] and lows.iloc[-2] > lows.iloc[-5]
    trend_down = highs.iloc[-2] < highs.iloc[-5] and lows.iloc[-2] < lows.iloc[-5]

    # Tightening range
    range_now  = highs.iloc[-5:-1].max() - lows.iloc[-5:-1].min()
    range_prev = highs.iloc[-10:-5].max() - lows.iloc[-10:-5].min()
    tight_range = range_prev > 0 and range_now < range_prev

    metrics = {
        "impulse":   round(impulse, 4),
        "vol_ratio": round(vol_ratio, 2),
        "trend_up":  trend_up,
        "trend_down": trend_down,
        "tight":     tight_range,
    }

    # Classify — order matters
    if trend_up or trend_down:
        return "TREND", metrics

    elif tight_range:
        return "ACCUMULATION", metrics

    elif vol_ratio < 0.5:
        return "DEAD", metrics

    elif impulse > 0.02:
        return "EXHAUSTION", metrics

    else:
        return "NEUTRAL", metrics

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

def trend_h1(df):
    ema34 = df["close"].ewm(span=34).mean()
    ema89 = df["close"].ewm(span=89).mean()

    ema_ok = None
    if ema34.iloc[-2] > ema89.iloc[-2]:
        ema_ok = "LONG"
    elif ema34.iloc[-2] < ema89.iloc[-2]:
        ema_ok = "SHORT"

    # EMA distance filter
    dist = abs(ema34.iloc[-2] - ema89.iloc[-2]) / df["close"].iloc[-2]
    if dist < 0.001:
        return None

    structure = detect_structure(df)

    if structure is None:
        return None   # ❗ reject nếu không rõ structure

    if structure != ema_ok:
        return None   # ❗ EMA vs structure conflict

    return ema_ok

def classify_coin(df):
    change = abs((df["close"].iloc[-1]-df["close"].iloc[-20])/df["close"].iloc[-20])
    range_ = (df["high"].iloc[-20:].max()-df["low"].iloc[-20:].min())/df["low"].iloc[-20:].min()
    ema = df["close"].ewm(span=34).mean()
    slope = ema.iloc[-1] - ema.iloc[-5]

    if change > 0.05 and range_ > 0.05:
        return "MOMENTUM"
    if abs(slope) > 0:
        return "TREND"
    if range_ < 0.02:
        return "SIDEWAY"
    return "NORMAL"

def ema_trend(df):
    ema34 = df["close"].ewm(span=34).mean()
    ema89 = df["close"].ewm(span=89).mean()
    slope = ema34.iloc[-1] - ema34.iloc[-5]

    if ema34.iloc[-1] > ema89.iloc[-1] and slope > 0:
        return "LONG"
    if ema34.iloc[-1] < ema89.iloc[-1] and slope < 0:
        return "SHORT"
    return None

def get_market_mode(df15):
    """
    Simple market mode based on recent move %
    """
    recent_high = df15["high"].iloc[-20:].max()
    recent_low = df15["low"].iloc[-20:].min()

    move = (recent_high - recent_low) / recent_low

    if move < 0.015:
        return "SIDEWAY"
    elif move < 0.03:
        return "NORMAL"
    else:
        return "TREND"

def _is_trend_strong(df1h, df4h=None):
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

    # [FIX Part 4] H4: không hard reject nếu conflict, chỉ return False khi rõ ràng ngược chiều
    if df4h is not None and len(df4h) >= 50:   # [FIX] 90 → 50
        h4_e34 = df4h["close"].ewm(span=34).mean().iloc[-2]
        h4_e89 = df4h["close"].ewm(span=89).mean().iloc[-2]
        h4_close_val = df4h["close"].iloc[-2]
        if math.isnan(h4_e34) or math.isnan(h4_e89) or h4_close_val <= 0:
            return True, side
        h4_gap  = abs(h4_e34 - h4_e89) / h4_close_val
        h4_side = "LONG" if h4_e34 > h4_e89 else "SHORT"
        # [FIX] Chỉ reject khi H4 rõ ràng ngược chiều VÀ ema_gap đủ lớn (> 0.003)
        if h4_side != side and h4_gap > 0.003:
            print(f"[DBG][TREND] H4 conflict: h4={h4_side} h1={side} h4_gap={h4_gap:.4f} → REJECT")
            return False, None

    return True, side

def _has_valid_pullback(df1h, side):
    """
    [FIX Part 4] Relaxed pullback check — không yêu cầu giá phải chạm EMA,
    chỉ cần giá không quá xa (within 3% EMA) và structure chưa bẻ gãy.
    Volume: chỉ cần >= 30% avg (từ 50%).
    """
    if df1h is None or len(df1h) < 15:
        return False

    ema34   = df1h["close"].ewm(span=34).mean()
    ema89 = df1h["close"].ewm(span=89).mean()
    close_now = df1h["close"].iloc[-2]
    ema_now   = ema34.iloc[-2]
    avg_vol   = df1h["volume"].rolling(20).mean().iloc[-2]
    cur_vol   = df1h["volume"].iloc[-2]
    e34 = ema34.iloc[-2]
    e89 = ema89.iloc[-2]
    slope = (ema34.iloc[-2] - ema34.iloc[-6]) / max(close_now, 1e-9)

    # [FIX Part 4] Volume: 0.5 → 0.3 (moderate volume OK)
    vol_ok = avg_vol > 0 and cur_vol >= avg_vol * 0.3

    if side == "LONG":
        ema_gap = abs(df1h["close"].ewm(span=34).mean().iloc[-2] - df1h["close"].ewm(span=89).mean().iloc[-2]) / max(close_now, 1e-9)
        pulled_back = close_now <= ema_now * 1.05   # [FIX] 1.03→1.15
        trend_ok = (e34 > e89) and (slope > 0)
        not_broken  = df1h["low"].iloc[-5:-2].min() > df1h["low"].iloc[-15:-5].min() * 0.97
        result = (pulled_back or (trend_ok and not is_overextended(df1h))) and not_broken and vol_ok
        if not result:
            print(f"[DBG][TREND] LONG pullback fail | pb={pulled_back} nb={not_broken} vol={vol_ok} "
                  f"close={close_now:.4f} ema={ema_now:.4f}")
        return result
    else:
        # [FIX] Pulled back: giá trên EMA*0.97 (từ 0.995 → 0.97)
        ema_gap = abs(df1h["close"].ewm(span=34).mean().iloc[-2] - df1h["close"].ewm(span=89).mean().iloc[-2]) / max(close_now, 1e-9)
        pulled_back = close_now >= ema_now * 0.95   # [FIX] 0.97→0.85
        trend_ok = (e34 < e89) and (slope < 0)
        not_broken  = df1h["high"].iloc[-5:-2].max() < df1h["high"].iloc[-15:-5].max() * 1.03
        result = (pulled_back or (trend_ok and not is_overextended(df1h))) and not_broken and vol_ok
        if not result:
            print(f"[DBG][TREND] SHORT pullback fail | pb={pulled_back} nb={not_broken} vol={vol_ok} "
                  f"close={close_now:.4f} ema={ema_now:.4f}")
        return result

# === TREND MODULE END ===