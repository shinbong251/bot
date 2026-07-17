# =====================================================================
# SMC_PA_SCORE_V3_1_SHADOW (SHADOW / LOG-ONLY)
# ---------------------------------------------------------------------
# Frozen decision-time re-instrumentation of SMC_PA_SCORE_V3 for the
# CONFIRM_SMC_RESEARCH lane. Pure instrumentation: it NEVER gates, NEVER
# changes a paper/live decision, never touches risk/cap/A3, SL/MIN_LOCK/
# trailing or any order path, never fetches network data, and never adds
# fields to any decision payload — it only appends rows to its own
# forward log and maintains its own bounded freeze state.
#
# Design (V3.1 design audit, 2026-07-17; artifacts /tmp/v31_design_audit.*):
#   - identity: v31_signal_id = dedup_key (SYMBOL|SIDE|CONFIRM|signal_created_ts)
#   - freeze policy: FIRST_COMPLETE_OBSERVATION — freeze on the first
#     observation with >= V31_MIN_AVAILABLE_FOR_ELIGIBLE of the
#     V31_TOTAL_COMPONENTS scored components available; if no complete
#     observation arrives within V31_FREEZE_FALLBACK_SECS of first sight,
#     freeze the current snapshot with v31_research_eligible=False.
#     The frozen score/band/candidate NEVER change after freeze; later
#     rescans reuse the frozen result (v31_frozen=True, obs_index rises).
#   - components (8 scored, all weights inherited from V3's declared
#     weights — no outcome-derived tuning): the 7 stable V3 components
#     (market_bias, regime, structure_quality, liquidity_sweep,
#     location_quality, volatility_sl_quality, target_realism; formulas
#     copied exactly from signal_dispatcher._smc_pa_score_v3_eval) plus
#     breakout_acceptance (decision-time signal-candle close/wick vs the
#     break level — NOT future follow-through). V3's proxy breakout,
#     phantom relative_strength and scan-relative execution_risk are not
#     scored; signal age is logged raw.
#   - relative strength (RS_M15) is observational-only in v0.1
#     (rs points always 0, not in the coverage denominator): raw
#     alt-vs-BTC M15 3-bar change difference + category
#     (RS_ALIGNED/RS_OPPOSED/RS_NEUTRAL/RS_MISSING/RS_STALE). Both
#     change windows end at signal_created_ts (closed candles only) so
#     alignment holds by construction; a later v3.1.1 schema may enable
#     scoring after forward validation.
#   - bands (fixed, inherited boundaries; no rolling percentiles):
#     V31_POSITIVE score>=1, V31_NEUTRAL -2<=score<1, V31_NEGATIVE <-2,
#     V31_LOW_COVERAGE when available < V31_MIN_AVAILABLE_FOR_ELIGIBLE.
#     v31_candidate = frozen and research_eligible and V31_POSITIVE —
#     annotation only, consumed by nothing.
#   - state: smc_pa_score_v31_shadow_state.json, atomic temp+fsync+
#     os.replace, schema-versioned, TTL-pruned + hard entry cap (candidate
#     lifetime is minutes, TTL is days, so pruning can never re-freeze a
#     still-active signal). Malformed/oversized state loads empty. Saves
#     are throttled; after a restart inside the throttle window a
#     still-alive signal may emit one duplicate freeze row — offline
#     audits resolve deterministically by taking the EARLIEST frozen row
#     per v31_signal_id. State/log failures never affect dispatch.
#
# Forbidden in rows (enforced by the writer): realized R, first_hit,
# SL outcome, MFE/MAE, trade terminal status, future candle fields.
# =====================================================================

import json
import os
import tempfile
import threading
import time

V31_SCHEMA_VERSION = "smc_pa_score_v3_1"
V31_VERSION = "smc_pa_score_v3_1_shadow_v0.1_log_only"
V31_EVENT_TYPE = "SMC_PA_SCORE_V3_1_SHADOW"
V31_LOG_PATH = os.path.join("logs", "smc_pa_score_v3_1_shadow.jsonl")
V31_STATE_PATH = "smc_pa_score_v31_shadow_state.json"
V31_STATE_SCHEMA_VERSION = 1
V31_STATE_MAX_BYTES = 16 * 1024 * 1024
V31_STATE_SOFT_SAVE_SECS = 60.0

V31_FREEZE_POLICY = "FIRST_COMPLETE_OBSERVATION"
V31_FREEZE_FALLBACK_SECS = 600.0
V31_STATE_TTL_SECS = 7 * 86400.0
V31_STATE_MAX_ENTRIES = 20000

V31_TOTAL_COMPONENTS = 8
V31_MIN_AVAILABLE_FOR_ELIGIBLE = 7

# Inherited V3 band boundaries (declared in production V3; NOT tuned here).
V31_POSITIVE_MIN = 1.0
V31_NEUTRAL_MIN = -2.0

V31_BAND_POSITIVE = "V31_POSITIVE"
V31_BAND_NEUTRAL = "V31_NEUTRAL"
V31_BAND_NEGATIVE = "V31_NEGATIVE"
V31_BAND_LOW_COVERAGE = "V31_LOW_COVERAGE"

# Breakout acceptance categories (decision-time only).
V31_BREAK_ACCEPTED = "BREAK_ACCEPTED"
V31_BREAK_WEAK = "BREAK_WEAK"
V31_BREAK_REJECTED = "BREAK_REJECTED"
V31_BREAK_CONTEXT_MISSING = "BREAK_CONTEXT_MISSING"

# Relative strength categories (observational-only in v0.1).
V31_RS_ALIGNED = "RS_ALIGNED"
V31_RS_OPPOSED = "RS_OPPOSED"
V31_RS_NEUTRAL = "RS_NEUTRAL"
V31_RS_MISSING = "RS_MISSING"
V31_RS_STALE = "RS_STALE"
V31_RS_SCORED = False
# One H1 bar: the BTC context source_ts is a min across M5/M15/H1, so a
# healthy snapshot can trail signal_created_ts by up to one H1 bar.
V31_RS_MAX_BTC_AGE_SECS = 3600.0

V31_COMPONENTS = (
    "market_bias",
    "regime",
    "structure_quality",
    "liquidity_sweep",
    "location_quality",
    "volatility_sl_quality",
    "target_realism",
    "breakout_acceptance",
)

# Same sets/constants as production V3 (copied, not imported, so this
# module never imports signal_dispatcher).
_V31_EXPANSION_REGIMES = {"TRENDING_CONTINUATION", "BREAKOUT_EXPANSION"}
_V31_STRUCTURE_POSITIVE = {
    "STRONG", "CONFIRM", "TRUE", "CONFIRMED", "CLOSE_THROUGH", "DISPLACEMENT",
}

# Keys that must never appear in a V3.1 shadow row (outcome/future data).
_V31_FORBIDDEN_ROW_KEYS = frozenset({
    "realized_r", "realized_rr", "first_hit", "sl_hit", "tp_hit",
    "mfe", "mae", "mfe_r", "mae_r",
    "max_favorable_r", "max_adverse_r",
    "trade_status", "terminal_status", "outcome", "exit_reason",
    "close_reason", "closed_at", "close_ts",
})

_lock = threading.Lock()
_DEFAULT_STORE = None


def _v31_text(value):
    return str(value or "").strip().upper()


def _v31_float(value):
    try:
        if value in (None, ""):
            return None
        out = float(value)
        if out != out:
            return None
        return out
    except (TypeError, ValueError):
        return None


def _v31_source_value(source, *keys):
    for key in keys:
        value = source.get(key)
        if value not in (None, ""):
            return value
    return None


def _v31_first_nonblank(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None


def normalize_v31_inputs(candidate, fields=None, trade=None, btc_ctx=None, now_ts=None):
    """Merge decision-time inputs exactly like the V3 assembler.

    Never mutates candidate/fields/trade/btc_ctx. Returns a dict with the
    merged source, identity, acceptance context and BTC context fields the
    evaluator needs. No I/O, no network.
    """
    candidate = candidate if isinstance(candidate, dict) else {}
    fields = fields if isinstance(fields, dict) else {}
    trade = trade if isinstance(trade, dict) else {}
    btc_ctx = btc_ctx if isinstance(btc_ctx, dict) else {}
    now_ts = time.time() if now_ts is None else now_ts

    entry_type = _v31_text(
        _v31_first_nonblank(
            trade.get("entry_type"),
            candidate.get("entry_type"),
            "CONFIRM_SMC_RESEARCH",
        )
    )

    source = {}
    source.update(candidate)
    source.update({k: v for k, v in fields.items() if v not in (None, "")})
    source.update({k: v for k, v in trade.items() if v not in (None, "")})

    # Log-only fallback: reuse the level/ATR context already carried by the
    # candidate (confirm_entry_acceptance_context) for components whose flat
    # source fields are absent — same rule as V3, never overwrites.
    acceptance_ctx = source.get("confirm_entry_acceptance_context")
    acceptance_ctx = acceptance_ctx if isinstance(acceptance_ctx, dict) else {}
    ctx_fallback_used = []
    for ctx_key in ("atr", "nearest_htf_support", "nearest_htf_resistance"):
        if source.get(ctx_key) in (None, "") and acceptance_ctx.get(ctx_key) not in (None, ""):
            source[ctx_key] = acceptance_ctx.get(ctx_key)
            ctx_fallback_used.append(ctx_key)

    side = _v31_text(_v31_first_nonblank(candidate.get("side"), trade.get("side")))
    symbol = str(_v31_first_nonblank(candidate.get("symbol"), trade.get("symbol")) or "")
    decision_ts = _v31_first_nonblank(
        candidate.get("signal_created_ts"),
        candidate.get("source_timestamp"),
        candidate.get("collector_ts"),
        candidate.get("timestamp"),
    )
    signal_age_secs = None
    decision_ts_f = _v31_float(decision_ts)
    if decision_ts_f is not None:
        signal_age_secs = round(max(0.0, float(now_ts) - decision_ts_f), 3)

    dedup_key = str(candidate.get("dedup_key") or "")
    v31_signal_id = dedup_key or None

    return {
        "now_ts": now_ts,
        "source": source,
        "side": side,
        "symbol": symbol,
        "entry_type": entry_type,
        "decision_ts": decision_ts,
        "signal_age_secs": signal_age_secs,
        "dedup_key": dedup_key,
        "v31_signal_id": v31_signal_id,
        "signal_key": candidate.get("signal_key"),
        "candidate_id": candidate.get("candidate_id"),
        "source_timestamp": candidate.get("source_timestamp"),
        "acceptance_ctx": acceptance_ctx,
        "ctx_fallback_used": ctx_fallback_used,
        "btc_ctx": btc_ctx,
    }


def _v31_breakout_acceptance(norm):
    """Decision-time breakout acceptance from the signal candle vs the
    break level. Same close/wick formulas as _breakout_acceptance_eval;
    NO follow bars, NO future data."""
    side = norm.get("side")
    ctx = norm.get("acceptance_ctx") or {}
    source = norm.get("source") or {}
    level = _v31_float(
        _v31_first_nonblank(ctx.get("break_level"), ctx.get("pre_break_level"))
    )
    c_close = _v31_float(ctx.get("candle_close"))
    c_high = _v31_float(ctx.get("candle_high"))
    c_low = _v31_float(ctx.get("candle_low"))
    atr = _v31_float(_v31_first_nonblank(source.get("atr"), ctx.get("atr")))

    out = {
        "category": V31_BREAK_CONTEXT_MISSING,
        "points": 0,
        "available": False,
        "reason": "level_or_signal_candle_missing",
        "break_level_value": level,
        "signal_candle_close": c_close,
        "signal_candle_high": c_high,
        "signal_candle_low": c_low,
        "close_beyond_level": None,
        "wick_rejection": None,
        "close_distance_from_level_pct": None,
        "close_distance_atr": None,
    }
    if side not in ("LONG", "SHORT") or level is None or level <= 0 or c_close is None:
        return out

    if side == "LONG":
        closed_through = c_close > level
        wick_broke = c_high is not None and c_high > level
    else:
        closed_through = c_close < level
        wick_broke = c_low is not None and c_low < level
    wick_rejection = bool(wick_broke and not closed_through)

    out["close_beyond_level"] = closed_through
    out["wick_rejection"] = wick_rejection
    out["close_distance_from_level_pct"] = (c_close - level) / level
    if atr is not None and atr > 0:
        out["close_distance_atr"] = round(abs(c_close - level) / atr, 4)
    out["available"] = True

    if wick_rejection:
        out["category"] = V31_BREAK_REJECTED
        out["points"] = -2
        out["reason"] = "wick_beyond_level_close_back_inside"
    elif not closed_through:
        out["category"] = V31_BREAK_WEAK
        out["points"] = -1
        out["reason"] = "close_not_beyond_level"
    else:
        out["category"] = V31_BREAK_ACCEPTED
        out["points"] = 1
        out["reason"] = "close_beyond_level"
    return out


def _v31_relative_strength(norm):
    """RS_M15 alt-vs-BTC relative strength. Observational-only in v0.1:
    points are ALWAYS 0 and RS is not in the coverage denominator."""
    side = norm.get("side")
    ctx = norm.get("acceptance_ctx") or {}
    btc_ctx = norm.get("btc_ctx") or {}
    alt_change = _v31_float(ctx.get("alt_m15_change_3bar_pct"))
    alt_source_ts = _v31_float(ctx.get("alt_m15_change_source_ts"))
    btc_change = _v31_float(btc_ctx.get("btc_15m_change_pct"))
    btc_source_ts = _v31_float(btc_ctx.get("btc_context_source_ts"))
    btc_age_sec = _v31_float(btc_ctx.get("btc_context_age_sec"))

    delta = None
    if alt_source_ts is not None and btc_source_ts is not None:
        delta = round(abs(alt_source_ts - btc_source_ts), 3)

    out = {
        "category": V31_RS_MISSING,
        "points": 0,
        "scored": V31_RS_SCORED,
        "rs_m15_raw": None,
        "alt_m15_change_3bar_pct": alt_change,
        "alt_m15_change_source_ts": alt_source_ts,
        "btc_m15_change_pct": btc_change,
        "btc_context_source_ts": btc_source_ts,
        "btc_context_age_sec": btc_age_sec,
        "rs_alignment_delta_secs": delta,
        "reason": "",
    }
    if side not in ("LONG", "SHORT") or alt_change is None or btc_change is None:
        out["reason"] = "alt_or_btc_m15_change_unavailable"
        return out
    if btc_age_sec is not None and btc_age_sec > V31_RS_MAX_BTC_AGE_SECS:
        out["category"] = V31_RS_STALE
        out["reason"] = f"btc_context_age_sec={btc_age_sec}>max={V31_RS_MAX_BTC_AGE_SECS}"
        return out

    if side == "LONG":
        rs = alt_change - btc_change
    else:
        rs = btc_change - alt_change
    out["rs_m15_raw"] = round(rs, 6)
    if rs > 0:
        out["category"] = V31_RS_ALIGNED
    elif rs < 0:
        out["category"] = V31_RS_OPPOSED
    else:
        out["category"] = V31_RS_NEUTRAL
    out["reason"] = f"rs_m15={out['rs_m15_raw']}|side={side}"
    return out


def evaluate_v31_components(norm):
    """Evaluate the 8 scored V3.1 components plus observational RS.

    Components 1-7 are exact copies of the production V3 formulas
    (signal_dispatcher._smc_pa_score_v3_eval); component 8 is the
    decision-time breakout acceptance. No I/O, no network, no mutation.
    """
    norm = norm if isinstance(norm, dict) else {}
    source = norm.get("source") or {}
    btc_ctx = norm.get("btc_ctx") or {}
    side = norm.get("side") or ""

    missing = []
    reasons = {}

    smc_zone = _v31_text(_v31_source_value(source, "smc_zone"))
    market_regime = _v31_text(
        _v31_source_value(source, "market_regime", "market_regime_at_entry", "regime")
    )
    bos_quality = _v31_text(_v31_source_value(source, "bos_quality"))
    liquidity_sweep = _v31_text(_v31_source_value(source, "liquidity_sweep"))
    trend_direction = _v31_text(_v31_source_value(source, "trend_direction"))

    entry = _v31_float(
        _v31_source_value(source, "entry", "entry_real", "entry_price")
    )
    sl = _v31_float(_v31_source_value(source, "sl", "stop_loss"))
    tp = _v31_float(_v31_source_value(source, "tp", "take_profit"))
    planned_rr = _v31_float(_v31_source_value(source, "planned_rr", "rr"))
    atr = _v31_float(
        _v31_source_value(source, "atr", "atr_m15", "atr_15m", "atr14", "atr_value")
    )

    # ── 1. market_bias: INDEPENDENT BTC fields only (exact V3 formula) ───
    market_bias_score = 0
    bias_source = "NONE"
    bias_value = "UNKNOWN"
    independent_bias = _v31_text(btc_ctx.get("btc_bias_independent"))
    independent_quality = _v31_text(btc_ctx.get("btc_context_quality"))
    independent_alignment = _v31_text(btc_ctx.get("btc_alignment_independent"))
    mtf_alignment = _v31_text(btc_ctx.get("btc_mtf_alignment"))
    mtf_data_mode = _v31_text(btc_ctx.get("btc_data_mode"))
    if (
        side in ("LONG", "SHORT")
        and independent_bias in ("BULLISH", "BEARISH", "NEUTRAL_OR_CHOP")
        and independent_quality not in ("", "MISSING")
    ):
        bias_source = "btc_bias_independent"
        bias_value = independent_bias
        if independent_bias == "NEUTRAL_OR_CHOP":
            market_bias_score = 0
            reasons["market_bias"] = "neutral_or_chop"
        elif (side == "LONG" and independent_bias == "BULLISH") or (
            side == "SHORT" and independent_bias == "BEARISH"
        ):
            market_bias_score = 2
            reasons["market_bias"] = f"aligned|side={side}|bias={independent_bias}"
        else:
            market_bias_score = -2
            reasons["market_bias"] = f"counter|side={side}|bias={independent_bias}"
    elif side in ("LONG", "SHORT") and independent_alignment in ("ALIGNED", "COUNTER", "NEUTRAL"):
        bias_source = "btc_alignment_independent"
        bias_value = independent_alignment
        if independent_alignment == "ALIGNED":
            market_bias_score = 2
        elif independent_alignment == "COUNTER":
            market_bias_score = -2
        reasons["market_bias"] = f"alignment_independent={independent_alignment}"
    elif (
        side in ("LONG", "SHORT")
        and mtf_data_mode == "INDEPENDENT_BTC_MTF"
        and mtf_alignment not in ("", "UNKNOWN")
    ):
        bias_source = "btc_mtf_alignment_independent"
        bias_value = mtf_alignment
        if mtf_alignment == "ALL_ALIGNED":
            market_bias_score = 2
        elif mtf_alignment == "COUNTER_HTF":
            market_bias_score = -2
        reasons["market_bias"] = f"mtf_alignment={mtf_alignment}"
    else:
        missing.append("market_bias")
        reasons["market_bias"] = "independent_btc_context_unavailable"

    # ── 2. regime (exact V3 formula) ─────────────────────────────────────
    regime_score = 0
    if market_regime == "RANGE_MEAN_REVERSION":
        regime_score = 1
        reasons["regime"] = "range_mean_reversion"
    elif market_regime in ("CHOP_NO_TRADE", "EXHAUSTION_REVERSAL"):
        regime_score = -2
        reasons["regime"] = f"bad_regime={market_regime}"
    elif market_regime in _V31_EXPANSION_REGIMES:
        if trend_direction in ("LONG", "SHORT") and trend_direction == side:
            regime_score = 1
            reasons["regime"] = f"expansion_side_aligned={market_regime}"
        else:
            regime_score = 0
            reasons["regime"] = f"expansion_not_side_confirmed={market_regime}"
    elif market_regime in ("", "UNKNOWN", "INSUFFICIENT_DATA"):
        missing.append("regime")
        reasons["regime"] = "market_regime_unavailable"
    else:
        reasons["regime"] = f"unmapped_regime={market_regime}"

    # ── 3. structure_quality (exact V3 formula) ──────────────────────────
    structure_quality_score = 0
    if bos_quality == "TRAP":
        structure_quality_score = 2
        reasons["structure_quality"] = "trap_sweep_reclaim_like"
    elif bos_quality in _V31_STRUCTURE_POSITIVE:
        structure_quality_score = 1
        reasons["structure_quality"] = f"bos_quality={bos_quality}"
    elif bos_quality == "NO_FOLLOWTHROUGH":
        structure_quality_score = -1
        reasons["structure_quality"] = "no_followthrough"
    elif bos_quality in ("", "UNKNOWN"):
        missing.append("structure_quality")
        reasons["structure_quality"] = "bos_quality_unavailable"
    else:
        reasons["structure_quality"] = f"neutral_bos_quality={bos_quality}"

    # ── 4. liquidity_sweep (exact V3 formula) ────────────────────────────
    liquidity_sweep_score = 0
    if liquidity_sweep in ("", "UNKNOWN"):
        missing.append("liquidity_sweep")
        reasons["liquidity_sweep"] = "liquidity_sweep_unavailable"
    elif side == "LONG" and liquidity_sweep == "SWEEP_LOW":
        liquidity_sweep_score = 2
        reasons["liquidity_sweep"] = "long_after_sweep_low_reclaim"
    elif side == "SHORT" and liquidity_sweep == "SWEEP_HIGH":
        liquidity_sweep_score = 2
        reasons["liquidity_sweep"] = "short_after_sweep_high_reclaim"
    elif side == "LONG" and liquidity_sweep == "SWEEP_HIGH":
        liquidity_sweep_score = -1
        reasons["liquidity_sweep"] = "long_into_sweep_high"
    elif side == "SHORT" and liquidity_sweep == "SWEEP_LOW":
        liquidity_sweep_score = -1
        reasons["liquidity_sweep"] = "short_into_sweep_low"
    else:
        reasons["liquidity_sweep"] = f"no_sweep={liquidity_sweep or 'NONE'}"

    # ── 5. location_quality (exact V3 formula) ───────────────────────────
    bullish_expansion = (
        market_regime in _V31_EXPANSION_REGIMES and trend_direction == "LONG"
    )
    bearish_expansion = (
        market_regime in _V31_EXPANSION_REGIMES and trend_direction == "SHORT"
    )
    location_quality_score = 0
    if smc_zone in ("", "UNKNOWN"):
        missing.append("location_quality")
        reasons["location_quality"] = "smc_zone_unavailable"
    elif side == "LONG" and smc_zone == "DISCOUNT":
        location_quality_score = 2
        reasons["location_quality"] = "long_in_discount"
    elif side == "LONG" and smc_zone == "PREMIUM":
        if bullish_expansion:
            location_quality_score = 0
            reasons["location_quality"] = "long_in_premium_softened_bullish_expansion"
        else:
            location_quality_score = -2
            reasons["location_quality"] = "long_in_premium"
    elif side == "SHORT" and smc_zone == "PREMIUM":
        location_quality_score = 2
        reasons["location_quality"] = "short_in_premium"
    elif side == "SHORT" and smc_zone == "DISCOUNT":
        if bearish_expansion:
            location_quality_score = 0
            reasons["location_quality"] = "short_in_discount_softened_bearish_expansion"
        else:
            location_quality_score = -2
            reasons["location_quality"] = "short_in_discount"
    else:
        reasons["location_quality"] = f"neutral_zone={smc_zone}"

    # ── 6. volatility_sl_quality (exact V3 formula) ──────────────────────
    volatility_sl_quality_score = 0
    sl_dist = abs(entry - sl) if entry is not None and sl is not None else None
    sl_atr_ratio = None
    if sl_dist in (None, 0) or atr is None or atr <= 0:
        missing.append("volatility_sl_quality")
        if sl_dist in (None, 0) and (atr is None or atr <= 0):
            reasons["volatility_sl_quality"] = "atr_and_sl_distance_unavailable"
        elif sl_dist in (None, 0):
            reasons["volatility_sl_quality"] = "sl_distance_unavailable"
        else:
            reasons["volatility_sl_quality"] = "atr_unavailable"
    else:
        sl_atr_ratio = round(sl_dist / atr, 4)
        if sl_atr_ratio < 1.0:
            volatility_sl_quality_score = -2
            reasons["volatility_sl_quality"] = f"sl_inside_noise_ratio={sl_atr_ratio}"
        elif sl_atr_ratio <= 2.5:
            if market_regime == "RANGE_MEAN_REVERSION":
                volatility_sl_quality_score = 1
            else:
                volatility_sl_quality_score = 0
            reasons["volatility_sl_quality"] = f"sl_ok_ratio={sl_atr_ratio}"
        else:
            volatility_sl_quality_score = -1
            reasons["volatility_sl_quality"] = f"sl_too_wide_ratio={sl_atr_ratio}"

    # ── 7. target_realism (exact V3 formula) ─────────────────────────────
    target_realism_score = 0
    opposing_r = _v31_float(
        _v31_source_value(source, "opposing_barrier_distance_r", "opposing_distance_r")
    )
    if opposing_r is None and entry is not None and sl_dist not in (None, 0):
        resistance = _v31_float(
            _v31_source_value(
                source, "nearest_htf_resistance", "m15_swing_high", "range_high"
            )
        )
        support = _v31_float(
            _v31_source_value(
                source, "nearest_htf_support", "m15_swing_low", "range_low"
            )
        )
        if side == "LONG" and resistance is not None:
            opposing_r = round(abs(resistance - entry) / sl_dist, 4)
        elif side == "SHORT" and support is not None:
            opposing_r = round(abs(entry - support) / sl_dist, 4)
    if opposing_r is None or planned_rr is None:
        missing.append("target_realism")
        if opposing_r is None and planned_rr is None:
            reasons["target_realism"] = "opposing_distance_and_planned_rr_unavailable"
        elif opposing_r is None:
            reasons["target_realism"] = "opposing_distance_unavailable"
        else:
            reasons["target_realism"] = "planned_rr_unavailable"
    elif planned_rr <= opposing_r:
        target_realism_score = 1
        reasons["target_realism"] = (
            f"target_before_opposing_liquidity|planned_rr={planned_rr}|opposing_r={opposing_r}"
        )
    else:
        target_realism_score = -2
        reasons["target_realism"] = (
            f"target_beyond_opposing_liquidity|planned_rr={planned_rr}|opposing_r={opposing_r}"
        )

    # ── 8. breakout_acceptance (decision-time, replaces V3 proxy) ────────
    breakout = _v31_breakout_acceptance(norm)
    if not breakout["available"]:
        missing.append("breakout_acceptance")
    reasons["breakout_acceptance"] = breakout["reason"]

    # ── observational: RS_M15 (unscored in v0.1) ─────────────────────────
    rs = _v31_relative_strength(norm)
    reasons["relative_strength"] = rs["reason"] or rs["category"]

    points = {
        "market_bias": market_bias_score,
        "regime": regime_score,
        "structure_quality": structure_quality_score,
        "liquidity_sweep": liquidity_sweep_score,
        "location_quality": location_quality_score,
        "volatility_sl_quality": volatility_sl_quality_score,
        "target_realism": target_realism_score,
        "breakout_acceptance": breakout["points"],
    }

    return {
        "points": points,
        "missing": list(missing),
        "reasons": dict(reasons),
        "breakout": breakout,
        "relative_strength": rs,
        "bias_source": bias_source,
        "bias_value": bias_value,
        "src": {
            "smc_zone": smc_zone,
            "market_regime": market_regime,
            "bos_quality": bos_quality,
            "liquidity_sweep": liquidity_sweep,
            "trend_direction": trend_direction,
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "planned_rr": planned_rr,
            "atr": atr,
            "sl_atr_ratio": sl_atr_ratio,
            "opposing_barrier_distance_r": opposing_r,
        },
    }


def evaluate_v31_score(components):
    """Sum the scored component points; coverage is tracked separately and
    missing components contribute 0 (never hidden negatives)."""
    components = components if isinstance(components, dict) else {}
    points = components.get("points") or {}
    missing = [name for name in components.get("missing") or [] if name in V31_COMPONENTS]
    score = 0
    for name in V31_COMPONENTS:
        value = points.get(name)
        if isinstance(value, (int, float)) and value == value:
            score += value
    available = V31_TOTAL_COMPONENTS - len(missing)
    return {
        "score": round(float(score), 4),
        "missing_count": len(missing),
        "available_count": available,
        "total_components": V31_TOTAL_COMPONENTS,
        "coverage_ratio": round(available / float(V31_TOTAL_COMPONENTS), 4),
        "missing_components": missing,
        "research_eligible": available >= V31_MIN_AVAILABLE_FOR_ELIGIBLE,
    }


def classify_v31_band(score, available_count):
    """Fixed bands on inherited boundaries. Low coverage is an explicit
    band, never a silently degraded quality score."""
    available = _v31_float(available_count)
    if available is None or available < V31_MIN_AVAILABLE_FOR_ELIGIBLE:
        return V31_BAND_LOW_COVERAGE
    value = _v31_float(score)
    if value is None:
        return V31_BAND_LOW_COVERAGE
    if value >= V31_POSITIVE_MIN:
        return V31_BAND_POSITIVE
    if value >= V31_NEUTRAL_MIN:
        return V31_BAND_NEUTRAL
    return V31_BAND_NEGATIVE


class V31FreezeStore:
    """Bounded frozen-identity registry persisted as atomic JSON.

    One record per v31_signal_id. Records freeze at the first complete
    observation (or the bounded fallback) and are immutable afterwards.
    TTL prune + hard entry cap on save; oversized/malformed state loads
    empty; save failures are swallowed — state can never affect dispatch.
    Saves are throttled to at most one per V31_STATE_SOFT_SAVE_SECS.
    """

    def __init__(self, path=V31_STATE_PATH):
        self.path = path
        self._records = {}
        self._last_save_ts = 0.0
        self._load()

    def _load(self):
        try:
            if not os.path.exists(self.path):
                return
            if os.path.getsize(self.path) > V31_STATE_MAX_BYTES:
                print("[SMC_PA_SCORE_V3_1_SHADOW] state file oversized; starting empty")
                return
            with open(self.path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if (
                isinstance(payload, dict)
                and payload.get("schema_version") == V31_STATE_SCHEMA_VERSION
                and isinstance(payload.get("signals"), dict)
            ):
                self._records = {
                    str(signal_id): record
                    for signal_id, record in payload["signals"].items()
                    if isinstance(record, dict)
                }
        except Exception as exc:
            print(f"[SMC_PA_SCORE_V3_1_SHADOW] state load failed ({exc}); starting empty")
            self._records = {}

    def get(self, signal_id):
        record = self._records.get(str(signal_id))
        return dict(record) if isinstance(record, dict) else None

    def observe(self, signal_id, summary, now_ts=None):
        """Register one observation. Returns the (possibly just-frozen)
        record for the id plus this observation's index.

        summary: dict with score/band/candidate/coverage fields for THIS
        observation (already evaluated). If the id is frozen the stored
        frozen values are returned unchanged; the summary is ignored.
        """
        now_ts = time.time() if now_ts is None else now_ts
        signal_id = str(signal_id)
        record = self._records.get(signal_id)
        if not isinstance(record, dict):
            record = {
                "first_seen_ts": now_ts,
                "last_seen_ts": now_ts,
                "obs_count": 0,
                "frozen": False,
            }
            self._records[signal_id] = record
            # Hard cap enforced at insert time (O(1)): evict the
            # oldest-inserted id. Candidate lifetime is minutes while the
            # cap covers days of volume, so an evicted id can no longer
            # produce a conflicting re-freeze.
            while len(self._records) > V31_STATE_MAX_ENTRIES:
                oldest = next(iter(self._records))
                if oldest == signal_id:
                    break
                self._records.pop(oldest, None)
        record["obs_count"] = int(record.get("obs_count") or 0) + 1
        record["last_seen_ts"] = now_ts

        if not record.get("frozen"):
            complete = bool(summary.get("research_eligible"))
            first_seen = _v31_float(record.get("first_seen_ts"))
            waited = (
                first_seen is not None
                and (now_ts - first_seen) >= V31_FREEZE_FALLBACK_SECS
            )
            if complete or waited:
                record["frozen"] = True
                record["freeze_ts"] = now_ts
                record["freeze_reason"] = (
                    "first_complete_observation" if complete else "fallback_timeout_low_coverage"
                )
                record["score"] = summary.get("score")
                record["band"] = summary.get("band")
                record["missing_count"] = summary.get("missing_count")
                record["available_count"] = summary.get("available_count")
                record["coverage_ratio"] = summary.get("coverage_ratio")
                record["missing_components"] = list(summary.get("missing_components") or [])
                record["research_eligible"] = bool(complete)
                record["candidate"] = bool(
                    complete and summary.get("band") == V31_BAND_POSITIVE
                )
                self.save(now_ts=now_ts)
        return dict(record), int(record["obs_count"])

    def prune(self, now_ts=None):
        now_ts = time.time() if now_ts is None else now_ts
        stale = [
            signal_id for signal_id, record in self._records.items()
            if not isinstance(record, dict)
            or (now_ts - (_v31_float(record.get("last_seen_ts")) or 0)) > V31_STATE_TTL_SECS
        ]
        for signal_id in stale:
            self._records.pop(signal_id, None)
        if len(self._records) > V31_STATE_MAX_ENTRIES:
            # Evict oldest-seen first: candidate lifetime is minutes, so the
            # oldest entries can no longer produce a conflicting re-freeze.
            ordered = sorted(
                self._records.items(),
                key=lambda item: _v31_float(
                    item[1].get("last_seen_ts") if isinstance(item[1], dict) else None
                ) or 0.0,
            )
            for signal_id, _ in ordered[: len(self._records) - V31_STATE_MAX_ENTRIES]:
                self._records.pop(signal_id, None)
        return len(stale)

    def save(self, now_ts=None, force=False):
        try:
            now_ts = time.time() if now_ts is None else now_ts
            if not force and (now_ts - self._last_save_ts) < V31_STATE_SOFT_SAVE_SECS:
                return
            self.prune(now_ts=now_ts)
            payload = {
                "schema_version": V31_STATE_SCHEMA_VERSION,
                "saved_at": now_ts,
                "signals": self._records,
            }
            directory = os.path.dirname(os.path.abspath(self.path)) or "."
            fd, tmp_path = tempfile.mkstemp(
                prefix=".smc_pa_v31_state.", suffix=".tmp", dir=directory
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(payload, handle, default=str)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp_path, self.path)
            finally:
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
            self._last_save_ts = now_ts
        except Exception as exc:
            print(f"[SMC_PA_SCORE_V3_1_SHADOW] state save failed (never affects dispatch): {exc}")

    def __len__(self):
        return len(self._records)


def get_default_store():
    global _DEFAULT_STORE
    with _lock:
        if _DEFAULT_STORE is None:
            _DEFAULT_STORE = V31FreezeStore()
        return _DEFAULT_STORE


def assemble_v31_snapshot(norm, components, score_info, freeze_record, obs_index,
                          execution_mode="", action="", reason="",
                          opened_trade_id=None, v3_summary=None):
    """Build one full V3.1 shadow row. Frozen ids report the FROZEN
    score/band/candidate; the current observation's evaluation is kept in
    obs_* fields for drift audits. No outcome/future fields."""
    freeze_record = freeze_record if isinstance(freeze_record, dict) else {}
    v3_summary = v3_summary if isinstance(v3_summary, dict) else {}
    breakout = components.get("breakout") or {}
    rs = components.get("relative_strength") or {}
    btc_ctx = norm.get("btc_ctx") or {}
    now_ts = norm.get("now_ts")
    frozen = bool(freeze_record.get("frozen"))

    if frozen:
        score = freeze_record.get("score")
        band = freeze_record.get("band")
        candidate_flag = bool(freeze_record.get("candidate"))
        missing_count = freeze_record.get("missing_count")
        available_count = freeze_record.get("available_count")
        coverage_ratio = freeze_record.get("coverage_ratio")
        missing_components = list(freeze_record.get("missing_components") or [])
        research_eligible = bool(freeze_record.get("research_eligible"))
    else:
        score = score_info.get("score")
        band = classify_v31_band(score_info.get("score"), score_info.get("available_count"))
        candidate_flag = False
        missing_count = score_info.get("missing_count")
        available_count = score_info.get("available_count")
        coverage_ratio = score_info.get("coverage_ratio")
        missing_components = list(score_info.get("missing_components") or [])
        research_eligible = bool(score_info.get("research_eligible"))

    row = {
        "schema_version": V31_SCHEMA_VERSION,
        "v31_version": V31_VERSION,
        "event_type": V31_EVENT_TYPE,
        "logged_at": now_ts,
        "decision_ts": norm.get("decision_ts"),
        "action": action,
        "reason": reason,
        "symbol": norm.get("symbol"),
        "side": norm.get("side"),
        "entry_type": norm.get("entry_type"),
        "execution_mode": execution_mode,
        "signal_key": norm.get("signal_key"),
        "candidate_id": norm.get("candidate_id"),
        "dedup_key": norm.get("dedup_key"),
        "source_timestamp": norm.get("source_timestamp"),
        "opened_trade_id": opened_trade_id,
        "v31_signal_id": norm.get("v31_signal_id"),
        "v31_schema_version": V31_SCHEMA_VERSION,
        "v31_score": score,
        "v31_band": band,
        "v31_candidate": candidate_flag,
        "v31_missing_count": missing_count,
        "v31_available_count": available_count,
        "v31_total_components": V31_TOTAL_COMPONENTS,
        "v31_coverage_ratio": coverage_ratio,
        "v31_missing_components": missing_components,
        "v31_research_eligible": research_eligible,
        "v31_frozen": frozen,
        "v31_freeze_ts": freeze_record.get("freeze_ts"),
        "v31_freeze_policy": V31_FREEZE_POLICY,
        "v31_freeze_reason": freeze_record.get("freeze_reason"),
        "v31_obs_index": obs_index,
        "v31_signal_age_secs": norm.get("signal_age_secs"),
        "v31_ctx_fallback_used": list(norm.get("ctx_fallback_used") or []),
        # This observation's own evaluation (drift audit; frozen values above
        # never change after freeze).
        "v31_obs_score": score_info.get("score"),
        "v31_obs_band": classify_v31_band(
            score_info.get("score"), score_info.get("available_count")
        ),
        "v31_obs_missing_count": score_info.get("missing_count"),
        # Component points + reasons.
        "v31_component_points": dict(components.get("points") or {}),
        "v31_component_reasons": dict(components.get("reasons") or {}),
        "v31_bias_source": components.get("bias_source"),
        "v31_bias_value": components.get("bias_value"),
        "v31_src": dict(components.get("src") or {}),
        # Breakout acceptance (decision-time inputs + category).
        "v31_breakout_category": breakout.get("category"),
        "v31_breakout_points": breakout.get("points"),
        "v31_breakout_break_level": breakout.get("break_level_value"),
        "v31_breakout_signal_candle_close": breakout.get("signal_candle_close"),
        "v31_breakout_signal_candle_high": breakout.get("signal_candle_high"),
        "v31_breakout_signal_candle_low": breakout.get("signal_candle_low"),
        "v31_breakout_close_beyond_level": breakout.get("close_beyond_level"),
        "v31_breakout_wick_rejection": breakout.get("wick_rejection"),
        "v31_breakout_close_distance_from_level_pct": breakout.get(
            "close_distance_from_level_pct"
        ),
        "v31_breakout_close_distance_atr": breakout.get("close_distance_atr"),
        # Relative strength (observational-only in v0.1).
        "v31_rs_category": rs.get("category"),
        "v31_rs_scored": rs.get("scored"),
        "v31_rs_points": rs.get("points"),
        "v31_rs_m15_raw": rs.get("rs_m15_raw"),
        "v31_rs_alt_m15_change_3bar_pct": rs.get("alt_m15_change_3bar_pct"),
        "v31_rs_alt_m15_change_source_ts": rs.get("alt_m15_change_source_ts"),
        "v31_rs_btc_m15_change_pct": rs.get("btc_m15_change_pct"),
        "v31_rs_btc_context_source_ts": rs.get("btc_context_source_ts"),
        "v31_rs_btc_context_age_sec": rs.get("btc_context_age_sec"),
        "v31_rs_alignment_delta_secs": rs.get("rs_alignment_delta_secs"),
        # Existing V3 result as baseline context only.
        "v3_total_score": v3_summary.get("smc_pa_v3_total_score"),
        "v3_score_band": v3_summary.get("smc_pa_v3_score_band"),
        # BTC M5/M15 relation as context only (already computed upstream).
        "btc_5m_change_pct": btc_ctx.get("btc_5m_change_pct"),
        "btc_15m_change_pct": btc_ctx.get("btc_15m_change_pct"),
        "btc_bias_independent": btc_ctx.get("btc_bias_independent"),
        "btc_alignment_independent": btc_ctx.get("btc_alignment_independent"),
        "btc_context_quality": btc_ctx.get("btc_context_quality"),
    }
    return row


def append_v31_shadow_row(row, log_path=None):
    """Append one row to the V3.1 forward log. Drops any forbidden
    outcome/future keys defensively. Failures are swallowed."""
    try:
        row = {
            key: value for key, value in dict(row).items()
            if key not in _V31_FORBIDDEN_ROW_KEYS
        }
        path = log_path or V31_LOG_PATH
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n"
            )
        return True
    except Exception as exc:
        print(f"[SMC_PA_SCORE_V3_1_SHADOW] log failed: {exc}")
        return False


def log_v31_shadow(candidate, fields=None, trade=None, execution_mode="",
                   action="", reason="", btc_ctx=None, opened_trade_id=None,
                   v3_summary=None, now_ts=None, store=None, log_path=None):
    """Evaluate + freeze + append one V3.1 shadow row.

    SHADOW / LOG-ONLY. Never mutates its inputs, never raises into the
    caller, never fetches network data, never touches any decision, risk,
    SL/order or gate path. The return value is observational only and MUST
    be ignored by production callers. Returns {} on any failure or when
    the candidate is not on the CONFIRM lane.
    """
    try:
        norm = normalize_v31_inputs(
            candidate, fields=fields, trade=trade, btc_ctx=btc_ctx, now_ts=now_ts
        )
        if norm["entry_type"] not in ("CONFIRM", "CONFIRM_SMC_RESEARCH"):
            return {}

        components = evaluate_v31_components(norm)
        score_info = evaluate_v31_score(components)

        freeze_record = {}
        obs_index = None
        if norm["v31_signal_id"]:
            if store is None:
                store = get_default_store()
            summary = {
                "score": score_info["score"],
                "band": classify_v31_band(
                    score_info["score"], score_info["available_count"]
                ),
                "missing_count": score_info["missing_count"],
                "available_count": score_info["available_count"],
                "coverage_ratio": score_info["coverage_ratio"],
                "missing_components": score_info["missing_components"],
                "research_eligible": score_info["research_eligible"],
            }
            freeze_record, obs_index = store.observe(
                norm["v31_signal_id"], summary, now_ts=norm["now_ts"]
            )

        row = assemble_v31_snapshot(
            norm, components, score_info, freeze_record, obs_index,
            execution_mode=execution_mode, action=action, reason=reason,
            opened_trade_id=opened_trade_id, v3_summary=v3_summary,
        )
        append_v31_shadow_row(row, log_path=log_path)
        return {
            "v31_signal_id": row.get("v31_signal_id"),
            "v31_score": row.get("v31_score"),
            "v31_band": row.get("v31_band"),
            "v31_candidate": row.get("v31_candidate"),
            "v31_frozen": row.get("v31_frozen"),
        }
    except Exception as exc:
        print(f"[SMC_PA_SCORE_V3_1_SHADOW] shadow failed: {exc}")
        return {}
