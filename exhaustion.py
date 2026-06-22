# === EXHAUSTION MODULE START ===

def compute_exhaustion(df15, side, bos_n, symbol=None):
    """
    Centralized market continuation-energy model.
    9-factor composite scoring replacing detect_exhaustion_v66.

    Args:
        df15:   M15 OHLCV dataframe
        side:   "LONG" or "SHORT"
        bos_n:  pre-computed BOS count (from bos_count())
        symbol: optional — enables telemetry print

    Returns:
        state:     "HEALTHY" | "EXTENDED" | "EXHAUSTED" | "COLLAPSING"
        score:     float, 0.0 = freshest energy, higher = more exhausted
        breakdown: list[str] of triggered factors
    """
    score = 0.0
    breakdown = []

    if df15 is None or len(df15) < 25:
        return "HEALTHY", 0.0, []

    # ─── 1. BOS MATURITY ────────────────────────────────────────────────
    # Trend continuation weakens after repeated BOS events
    if bos_n >= 6:
        score += 3.0
        breakdown.append("BOS_MATURE_6+")
    elif bos_n >= 4:
        score += 2.0
        breakdown.append("BOS_MATURE_4")
    elif bos_n >= 3:
        score += 1.0
        breakdown.append("BOS_MATURE_3")

    # ─── 2. EMA50 OVEREXTENSION ─────────────────────────────────────────
    # Price stretched too far from equilibrium
    ema50 = df15["close"].ewm(span=50).mean().iloc[-2]
    price = df15["close"].iloc[-2]
    dist_ema50 = abs(price - ema50) / ema50 if ema50 > 0 else 0

    if dist_ema50 > 0.05:
        score += 3.0
        breakdown.append("EMA50_DIST_5PCT")
    elif dist_ema50 > 0.03:
        score += 2.0
        breakdown.append("EMA50_DIST_3PCT")
    elif dist_ema50 > 0.015:
        score += 1.0
        breakdown.append("EMA50_DIST_1.5PCT")

    # ─── 3. EMA34 SLOPE DECAY ───────────────────────────────────────────
    # EMA momentum flattening = trend losing acceleration
    ema34 = df15["close"].ewm(span=34).mean()
    if len(ema34) >= 12:
        slope_recent = ema34.iloc[-2] - ema34.iloc[-6]
        slope_prior  = ema34.iloc[-6] - ema34.iloc[-10]

        if side == "LONG" and slope_prior > 0:
            decay_ratio = slope_recent / slope_prior
            if decay_ratio < 0:
                score += 2.0
                breakdown.append("EMA_SLOPE_REVERSED")
            elif decay_ratio < 0.4:
                score += 1.5
                breakdown.append("EMA_SLOPE_DECAY")
            elif decay_ratio < 0.7:
                score += 0.5
                breakdown.append("EMA_SLOPE_WEAK")
        elif side == "SHORT" and slope_prior < 0:
            decay_ratio = slope_recent / slope_prior
            if decay_ratio < 0:
                score += 2.0
                breakdown.append("EMA_SLOPE_REVERSED")
            elif decay_ratio < 0.4:
                score += 1.5
                breakdown.append("EMA_SLOPE_DECAY")
            elif decay_ratio < 0.7:
                score += 0.5
                breakdown.append("EMA_SLOPE_WEAK")

    # ─── 4. IMPULSE DECAY ───────────────────────────────────────────────
    # Shrinking body size over recent candles = momentum dying
    avg_range = (df15["high"] - df15["low"]).rolling(20).mean().iloc[-2]
    if len(df15) >= 10 and avg_range > 0:
        bodies = [abs(df15["close"].iloc[i] - df15["open"].iloc[i]) for i in range(-8, -1)]
        early_body  = (bodies[0] + bodies[1]) / 2
        recent_body = (bodies[-2] + bodies[-1]) / 2
        if early_body > 0:
            if recent_body < early_body * 0.4 and recent_body < avg_range * 0.3:
                score += 2.0
                breakdown.append("IMPULSE_DECAY")
            elif recent_body < early_body * 0.6:
                score += 1.0
                breakdown.append("IMPULSE_WEAKENING")

    # ─── 5. VOLUME CLIMAX ───────────────────────────────────────────────
    # Extreme volume spike without continuation = exhaustion signal
    vol_now  = df15["volume"].iloc[-2]
    vol_prev = df15["volume"].iloc[-3]
    avg_vol  = df15["volume"].rolling(20).mean().iloc[-2]
    if avg_vol > 0:
        if vol_now > avg_vol * 2.5:
            score += 2.0
            breakdown.append("VOL_CLIMAX")
        elif vol_now > avg_vol * 1.8:
            score += 1.0
            breakdown.append("VOL_HIGH")
        if vol_prev > avg_vol * 2.0 and vol_now < avg_vol * 0.8:
            score += 1.5
            breakdown.append("VOL_SPIKE_DEAD")

    # ─── 6. FAILED FOLLOWTHROUGH ────────────────────────────────────────
    # Doji/weak body after a breakout = no continuation energy
    recent_range = df15["high"].iloc[-2] - df15["low"].iloc[-2]
    last_body    = abs(df15["close"].iloc[-2] - df15["open"].iloc[-2])
    if avg_range > 0 and recent_range > 0:
        body_ratio = last_body / recent_range
        if body_ratio < 0.25 and recent_range < avg_range * 0.7:
            score += 1.5
            breakdown.append("WEAK_FOLLOWTHROUGH")

    # ─── 7. MULTI-CANDLE OVEREXTENSION ──────────────────────────────────
    # Price ran too far too fast over last 6 candles
    close_now  = df15["close"].iloc[-2]
    close_6ago = df15["close"].iloc[-8]
    move_6 = abs(close_now - close_6ago) / max(close_6ago, 1e-9)
    if move_6 > 0.06:
        score += 2.0
        breakdown.append("PARABOLIC_6C")
    elif move_6 > 0.04:
        score += 1.0
        breakdown.append("OVEREXTENDED_6C")

    # ─── 8. RSI DIVERGENCE ──────────────────────────────────────────────
    # Price making new high/low but RSI declining = structural exhaustion
    try:
        from scoring import rsi_div
        if rsi_div(df15, side):
            score += 2.0
            breakdown.append("RSI_DIV")
    except Exception:
        pass

    # ─── 9. BOS STALL COMPRESSION ───────────────────────────────────────
    # Range compressing after multiple BOS = continuation failing
    if bos_n >= 2:
        range_now  = df15["high"].iloc[-5:-1].max() - df15["low"].iloc[-5:-1].min()
        range_prev = df15["high"].iloc[-10:-5].max() - df15["low"].iloc[-10:-5].min()
        if range_prev > 0 and range_now < range_prev * 0.5:
            score += 1.5
            breakdown.append("BOS_STALL_COMPRESS")

    # ─── CLASSIFY ───────────────────────────────────────────────────────
    if score <= 3.5:
        state = "HEALTHY"
    elif score <= 5.5:
        state = "EXTENDED"
    elif score <= 9.0:
        state = "EXHAUSTED"
    else:
        state = "COLLAPSING"

    if symbol:
        factors_str = ", ".join(breakdown) if breakdown else "clean"
        print(f"[EXHAUSTION] {symbol} | {state} | score={round(score, 1)} | {factors_str}")

    return state, round(score, 1), breakdown

# === EXHAUSTION MODULE END ===
