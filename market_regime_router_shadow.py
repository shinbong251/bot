import os
import json
import time
from datetime import datetime, timezone

from config import config


SCHEMA_VERSION = "v0.1"
EVENT_TYPE = "MARKET_REGIME_ROUTER_SHADOW"
_DEDUP_STATE = {}
_CONFIDENCE_ORDER = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}


def _cfg_bool(key, default=False):
    try:
        return bool(config.get(key, default))
    except Exception:
        return default


def _cfg_int(key, default):
    try:
        return int(config.get(key, default))
    except Exception:
        return default


def _safe_scalar(value):
    try:
        if value is None:
            return None
        if hasattr(value, "item"):
            return _safe_scalar(value.item())
        if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
            return None
    except Exception:
        return None
    return value


def _safe_float(value):
    value = _safe_scalar(value)
    if value is None:
        return None
    try:
        value = float(value)
        if value != value or value in (float("inf"), float("-inf")):
            return None
        return value
    except Exception:
        return None


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, set):
        return sorted(value)
    return [value]


def _to_epoch(value):
    value = _safe_scalar(value)
    if value is None:
        return None
    try:
        if hasattr(value, "timestamp"):
            return float(value.timestamp())
        epoch = float(value)
        if epoch > 10_000_000_000:
            epoch = epoch / 1000.0
        return epoch
    except Exception:
        pass
    try:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return float(parsed.timestamp())
    except Exception:
        return None


def _last_non_null_column_value(df, column):
    try:
        if df is None or len(df) <= 0 or column not in df:
            return None
        series = df[column]
        for value in reversed(series.tolist()):
            value = _safe_scalar(value)
            if value is not None:
                return value
    except Exception:
        return None
    return None


def _last_candle_time_value(df):
    if df is None:
        return None
    try:
        if len(df) <= 0:
            return None
    except Exception:
        return None

    for column in ("timestamp", "time", "open_time", "close_time", "datetime", "date"):
        epoch = _to_epoch(_last_non_null_column_value(df, column))
        if epoch is not None:
            return epoch

    try:
        index = df.index
        last = _safe_scalar(index[-1])
        if hasattr(last, "timestamp"):
            return _to_epoch(last)
        dtype = getattr(index, "dtype", None)
        if getattr(dtype, "kind", None) == "M":
            return _to_epoch(last)
        inferred_type = str(getattr(index, "inferred_type", "") or "").lower()
        if inferred_type in {"datetime64", "datetime", "date"}:
            return _to_epoch(last)
    except Exception:
        return None
    return None


def _freshness_status(candle_ts_15m, df15, observed_at):
    epoch = _to_epoch(candle_ts_15m)
    if epoch is None:
        return "UNKNOWN"
    try:
        return "STALE" if float(observed_at) - epoch > 30 * 60 else "FRESH"
    except Exception:
        return "UNKNOWN"


def _ctx_get(ctx, key, default=None):
    if isinstance(ctx, dict):
        return ctx.get(key, default)
    return default


def _confidence_allowed(confidence):
    wanted = str(config.get("market_regime_router_shadow_min_confidence_to_log", "LOW") or "LOW").upper()
    return _CONFIDENCE_ORDER.get(str(confidence or "LOW").upper(), 1) >= _CONFIDENCE_ORDER.get(wanted, 1)


def _config_snapshot():
    keys = (
        "market_regime_router_shadow_enabled",
        "market_regime_router_shadow_log_path",
        "market_regime_router_shadow_log_unknown",
        "market_regime_router_shadow_log_chop",
        "market_regime_router_shadow_log_every_scan",
        "market_regime_router_shadow_dedup_ttl_secs",
        "market_regime_router_shadow_max_per_scan",
        "market_regime_router_shadow_min_confidence_to_log",
    )
    return {key: config.get(key) for key in keys}


def _empty_row(
    symbol,
    scan_id,
    observed_at,
    had_signal,
    candidate_count,
    accepted_count,
    missing_fields,
    regime="UNKNOWN",
    reasons=None,
):
    candle_ts_15m = None
    candle_ts_5m = None
    return {
        "event_type": EVENT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "observed_at": observed_at,
        "scan_id": scan_id,
        "symbol": symbol,
        "base_timeframe": "15m",
        "candle_ts_15m": candle_ts_15m,
        "candle_ts_5m": candle_ts_5m,
        "collector_ts": observed_at,
        "freshness_status": "UNKNOWN",
        "regime": regime,
        "confidence": "LOW",
        "reasons": reasons or ["unknown_context"],
        "missing_fields": missing_fields,
        "conflict_flags": [],
        "market_state": None,
        "mkt_state": None,
        "phase": None,
        "trend_direction": None,
        "trend_strength": None,
        "range_context": None,
        "bos_confirmation": None,
        "bos_n": None,
        "liquidity_sweep": None,
        "liquidity_context": None,
        "smc_zone": None,
        "smc_bias": None,
        "btc_h4_bias_raw": "NONE",
        "btc_h1_ema_raw": "NONE",
        "btc_h1_structure_ok": "NOT_AVAILABLE_V1",
        "invalid_context": [],
        "exhaustion": None,
        "exhaustion_score": None,
        "exhaustion_breakdown": [],
        "impulse": None,
        "vol_ratio": None,
        "had_signal": bool(had_signal),
        "candidate_count": candidate_count,
        "accepted_count": accepted_count,
        "rejected_summary": {},
        "config_snapshot": _config_snapshot(),
    }


def classify_regime(
    symbol,
    df5,
    df15,
    df1h,
    df4h,
    had_signal=False,
    candidate_count=0,
    accepted_count=0,
    scan_id=None,
    observed_at=None,
):
    observed_at = observed_at or time.time()
    missing_fields = []
    if df15 is None or len(df15) < 25:
        missing_fields.append("df15")
    if df1h is None or len(df1h) < 20:
        missing_fields.append("df1h")
    if missing_fields:
        return _empty_row(
            symbol,
            scan_id,
            observed_at,
            had_signal,
            candidate_count,
            accepted_count,
            missing_fields,
            regime="INSUFFICIENT_DATA",
            reasons=["insufficient_data"],
        )

    candle_ts_15m = _last_candle_time_value(df15)
    candle_ts_5m = _last_candle_time_value(df5)

    try:
        from entry import build_market_context, compute_smc_context
        from trend import trend_h1, trend_strength
        from exhaustion import compute_exhaustion
        from bos import bos_count

        ctx = build_market_context(df15, df1h)
        side = trend_h1(df1h) or "LONG"
        bos_n = bos_count(df15, side)
        smc = compute_smc_context(df15, df1h, df4h, side, None, None, ctx)
        exhaustion, exhaustion_score, exhaustion_breakdown = compute_exhaustion(df15, side, bos_n, symbol)
        try:
            trend_strength_value = trend_strength(df15)
        except Exception:
            trend_strength_value = None
    except Exception:
        row = _empty_row(
            symbol,
            scan_id,
            observed_at,
            had_signal,
            candidate_count,
            accepted_count,
            ["context_build"],
            regime="UNKNOWN",
            reasons=["context_build_failure"],
        )
        row["candle_ts_15m"] = candle_ts_15m
        row["candle_ts_5m"] = candle_ts_5m
        row["collector_ts"] = observed_at
        row["freshness_status"] = _freshness_status(candle_ts_15m, df15, observed_at)
        return row

    market_state = _ctx_get(ctx, "market_state")
    mkt_state = _ctx_get(ctx, "mkt_state")
    phase = _ctx_get(ctx, "phase")
    impulse = bool(_ctx_get(ctx, "impulse", False))
    vol_ratio = _ctx_get(_ctx_get(ctx, "mkt_metrics", {}), "vol_ratio")
    range_context = smc.get("range_context", "UNKNOWN") if isinstance(smc, dict) else "UNKNOWN"
    bos_confirmation = smc.get("bos_confirmation", "UNKNOWN") if isinstance(smc, dict) else "UNKNOWN"
    liquidity_sweep = smc.get("liquidity_sweep", "NONE") if isinstance(smc, dict) else "NONE"
    smc_bias = smc.get("smc_bias", "UNKNOWN") if isinstance(smc, dict) else "UNKNOWN"
    invalid_context = _as_list(smc.get("invalid_context", []) if isinstance(smc, dict) else [])
    btc_h4_bias_raw = smc.get("btc_h4_bias_raw", "NONE") if isinstance(smc, dict) else "NONE"
    btc_h1_ema_raw = smc.get("btc_h1_ema_raw", "NONE") if isinstance(smc, dict) else "NONE"
    btc_h1_structure_ok = smc.get("btc_h1_structure_ok", "NOT_AVAILABLE_V1") if isinstance(smc, dict) else "NOT_AVAILABLE_V1"

    conflict_flags = []
    regime = "CHOP_NO_TRADE"
    confidence = "LOW"
    reasons = ["default_fallback"]

    if not market_state or not mkt_state or not phase or not smc_bias:
        regime = "UNKNOWN"
        confidence = "LOW"
        reasons = ["missing_core_context"]
        missing_fields.extend([key for key, value in (
            ("market_state", market_state),
            ("mkt_state", mkt_state),
            ("phase", phase),
            ("smc_bias", smc_bias),
        ) if not value])
    elif phase == "BREAKOUT_STRONG" and smc_bias == "NEUTRAL" and not impulse:
        regime = "UNKNOWN"
        confidence = "LOW"
        reasons = ["contradictory_context"]
        conflict_flags.append("breakout_strong_neutral_no_impulse")
    elif (
        phase == "BREAKOUT_STRONG"
        and impulse
        and bos_confirmation in {"DISPLACEMENT", "CLOSE_THROUGH"}
        and exhaustion in {"HEALTHY", "EXTENDED"}
    ):
        regime = "BREAKOUT_EXPANSION"
        confidence = "MEDIUM"
        reasons = ["phase_breakout_strong", "impulse", "bos_confirmed"]
    elif exhaustion in {"EXHAUSTED", "COLLAPSING"} or (
        exhaustion == "EXTENDED"
        and (
            "RSI_DIV" in exhaustion_breakdown
            or "WEAK_FOLLOWTHROUGH" in exhaustion_breakdown
            or "COUNTER_HTF_BIAS" in invalid_context
        )
    ):
        regime = "EXHAUSTION_REVERSAL"
        confidence = "MEDIUM" if exhaustion in {"EXHAUSTED", "COLLAPSING"} else "LOW"
        reasons = []
        if exhaustion == "EXHAUSTED":
            reasons.append("exhaustion_exhausted")
        if exhaustion == "COLLAPSING":
            reasons.append("exhaustion_collapsing")
        if "RSI_DIV" in exhaustion_breakdown:
            reasons.append("rsi_div")
        if "WEAK_FOLLOWTHROUGH" in exhaustion_breakdown:
            reasons.append("weak_followthrough")
        if "COUNTER_HTF_BIAS" in invalid_context:
            reasons.append("counter_htf_bias")
    elif (
        market_state == "TREND"
        and smc_bias in {"BULLISH", "BEARISH"}
        and exhaustion == "HEALTHY"
        and bos_confirmation in {"CLOSE_THROUGH", "DISPLACEMENT", "RETESTED"}
        and "BOS_NEAR_NO_SWEEP_NO_DISPLACEMENT" not in invalid_context
    ):
        regime = "TRENDING_CONTINUATION"
        confidence = "MEDIUM"
        reasons = ["trend_state", "bias_present", "healthy_exhaustion", "bos_confirmed"]
    elif (
        range_context in {"RANGE_HIGH", "RANGE_LOW"}
        and phase != "BREAKOUT_STRONG"
        and smc_bias in {"NEUTRAL", "UNKNOWN"}
        and liquidity_sweep in {"SWEEP_HIGH", "SWEEP_LOW", "NONE"}
    ):
        regime = "RANGE_MEAN_REVERSION"
        confidence = "LOW"
        reasons = ["range_edge", "no_strong_breakout"]
    elif (
        market_state in {"NEUTRAL", "DEAD", "ACCUMULATION"}
        or smc_bias in {"NEUTRAL", "UNKNOWN"}
        or (range_context == "MID" and bos_confirmation in {"NEAR", "UNKNOWN"})
    ):
        regime = "CHOP_NO_TRADE"
        confidence = "MEDIUM"
        reasons = []
        if market_state in {"NEUTRAL", "DEAD", "ACCUMULATION"}:
            reasons.append("weak_market_state")
        if smc_bias in {"NEUTRAL", "UNKNOWN"}:
            reasons.append("neutral_bias")
        if range_context == "MID":
            reasons.append("range_mid")
        if bos_confirmation in {"NEAR", "UNKNOWN"}:
            reasons.append("bos_near_or_unknown")

    return {
        "event_type": EVENT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "observed_at": observed_at,
        "scan_id": scan_id,
        "symbol": symbol,
        "base_timeframe": "15m",
        "candle_ts_15m": candle_ts_15m,
        "candle_ts_5m": candle_ts_5m,
        "collector_ts": observed_at,
        "freshness_status": _freshness_status(candle_ts_15m, df15, observed_at),
        "regime": regime,
        "confidence": confidence,
        "reasons": reasons,
        "missing_fields": missing_fields,
        "conflict_flags": conflict_flags,
        "market_state": market_state,
        "mkt_state": mkt_state,
        "phase": phase,
        "trend_direction": side,
        "trend_strength": _safe_float(trend_strength_value),
        "range_context": range_context,
        "bos_confirmation": bos_confirmation,
        "bos_n": bos_n,
        "liquidity_sweep": liquidity_sweep,
        "liquidity_context": liquidity_sweep,
        "smc_zone": smc.get("smc_zone") if isinstance(smc, dict) else None,
        "smc_bias": smc_bias,
        "btc_h4_bias_raw": btc_h4_bias_raw,
        "btc_h1_ema_raw": btc_h1_ema_raw,
        "btc_h1_structure_ok": btc_h1_structure_ok,
        "invalid_context": invalid_context,
        "exhaustion": exhaustion,
        "exhaustion_score": _safe_float(exhaustion_score),
        "exhaustion_breakdown": _as_list(exhaustion_breakdown),
        "impulse": impulse,
        "vol_ratio": _safe_float(vol_ratio),
        "had_signal": bool(had_signal),
        "candidate_count": candidate_count,
        "accepted_count": accepted_count,
        "rejected_summary": {},
        "config_snapshot": _config_snapshot(),
    }


def should_log_market_regime_shadow(row, now_ts=None):
    if not _cfg_bool("market_regime_router_shadow_enabled", False):
        return False
    if not isinstance(row, dict):
        return False
    regime = str(row.get("regime") or "UNKNOWN")
    if regime in {"UNKNOWN", "INSUFFICIENT_DATA"} and not _cfg_bool("market_regime_router_shadow_log_unknown", True):
        return False
    if regime == "CHOP_NO_TRADE" and not _cfg_bool("market_regime_router_shadow_log_chop", True):
        return False
    if not _confidence_allowed(row.get("confidence")):
        return False
    if _cfg_bool("market_regime_router_shadow_log_every_scan", False):
        return True

    now_ts = now_ts or time.time()
    symbol = str(row.get("symbol") or "")
    ttl = max(0, _cfg_int("market_regime_router_shadow_dedup_ttl_secs", 900))
    last = _DEDUP_STATE.get(symbol)
    if not last:
        _DEDUP_STATE[symbol] = {"last_regime": regime, "last_logged_ts": now_ts}
        return True
    if last.get("last_regime") != regime or now_ts - float(last.get("last_logged_ts", 0)) >= ttl:
        _DEDUP_STATE[symbol] = {"last_regime": regime, "last_logged_ts": now_ts}
        return True
    return False


def log_market_regime_shadow(row):
    try:
        path = config.get("market_regime_router_shadow_log_path", "logs/market_regime_router_shadow.jsonl")
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception:
        pass
