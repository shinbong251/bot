"""
Signal dispatch layer.

Signal generation runs ONCE in scan_phase() (execution.py).
This module handles per-executor filtering, policy, and execution dispatch.

Design boundary:
  Signal generation  ->  SHARED   (scan_phase)
  Execution lifecycle ->  ISOLATED (dispatch_to_executor per ctx)
"""
import copy
import html
import json
import os
import re
import time
from collections import Counter

import csv as _csv_mod

from notifier import format_vn_time
from config import config

_paper_observation_last_sent = {}
_paper_smc_research_dedup_keys = set()
_paper_smc_research_qualified_dedup_keys = set()
_paper_smc_research_qualified_first_seen_ts = {}
_paper_smc_main_dedup_keys = set()
_live_smc_research_dedup_keys = set()
_paper_smc_main_gate_shadow_scan_counter = 0
_paper_structural_modifier_research_logged = set()
_paper_boundary_guard_event_dedup = set()
_paper_boundary_guard_event_counts = {}
_paper_boundary_guard_summary_last_ts = time.time()
_live_shadow_smc_decision_dedup = {}
_PAPER_OBSERVATION_TTL_SECS = int(config.get("paper_observation_ttl_secs", 900))
_PAPER_OBSERVATION_OPEN_SYMBOL_TTL_SECS = int(
    config.get("paper_observation_open_symbol_ttl_secs", 1800)
)
_PAPER_OBSERVATION_REASON_TTL_SECS = {
    "symbol_already_open_in_paper": _PAPER_OBSERVATION_OPEN_SYMBOL_TTL_SECS,
    "executor_paused": int(config.get("paper_observation_executor_paused_ttl_secs", 1800)),
    "top_n_cap_not_selected": int(config.get("paper_observation_top_n_ttl_secs", 900)),
    "entry_cooldown": _PAPER_OBSERVATION_TTL_SECS,
    "loss_cooldown": _PAPER_OBSERVATION_TTL_SECS,
    "paper_confirm_pre_break_low_near_filter": _PAPER_OBSERVATION_TTL_SECS,
    "paper_confirm_retest_violated_bos_trap_filter": _PAPER_OBSERVATION_TTL_SECS,
}
_FAILED_CONTINUATION_WATCH_LAST_LOGGED = {}
FAILED_CONTINUATION_REVERSAL_WATCH_TTL_SECS = 900
FAILED_CONTINUATION_REVERSAL_WATCH_MAX_PER_SCAN = 5
FAILED_CONTINUATION_REVERSAL_WATCH_MISSING_CONTEXT = [
    "df15",
    "df1h",
    "df4h",
    "ctx",
    "bos_level",
    "trap_high",
    "trap_low",
    "volume_ratio",
    "displacement",
]
FAILED_CONTINUATION_REVERSAL_WATCH_CONFIRMATION_REQUIRED = [
    "close_above_trap_high",
    "bullish_displacement",
    "volume_follow_through",
    "retest_holds",
    "valid_long_rr_geometry",
    "stale_protection",
    "reversal_dedup",
]

# =====================================================================
# TESTNET EXECUTION POLICY CONSTANTS
# =====================================================================

TESTNET_MIN_SCORE = 8
TESTNET_MAX_CONCURRENT = 5
TESTNET_EXCLUDED_POOL_TIER = "B"
PAPER_ALLOWED_ENTRY_TYPES = {
    "CONFIRM",
    "REVERSAL_CONFIRM",
    "EARLY_V2",
    "EARLY_CONT",
    "SWING_RETEST",
}


# =====================================================================
# LIVE EXECUTION POLICY CONSTANTS
# =====================================================================

LIVE_MIN_SCORE            = 9
LIVE_ALLOWED_ENTRY_TYPES  = {"CONFIRM"}
LIVE_ALLOWED_EXHAUSTION   = {"HEALTHY"}
LIVE_ALLOWED_BOS_TYPES    = {"NEAR"}
LIVE_EXCLUDED_POOL_TIER   = "B"
LIVE_SYMBOL_RE            = re.compile(r"^[A-Z0-9]{2,20}USDT$")

_BTC_MTF_CONTEXT_FIELDS = (
    "btc_context_available",
    "btc_m5_trend",
    "btc_m15_trend",
    "btc_h1_trend",
    "btc_m5_momentum",
    "btc_m15_momentum",
    "btc_h1_momentum",
    "btc_mtf_alignment",
    "btc_alignment_reason",
    "btc_data_mode",
    "btc_context_source_ts",
    "btc_context_age_sec",
    "btc_unknown_reason",
)
_BTC_MTF_TIMEFRAMES = (("m5", "5m"), ("m15", "15m"), ("h1", "1h"))
_BTC_MTF_INTERVAL_SECS = {"5m": 300, "15m": 900, "1h": 3600}
_BTC_MTF_EMA_FAST = 9
_BTC_MTF_EMA_SLOW = 21
_BTC_MTF_SLOPE_BARS = 3
_BTC_MTF_MOMENTUM_EPS = 0.0002
_BTC_MTF_DEFAULT_MAX_AGE_SEC = 3600.0
_BTC_MTF_DEFAULT_REFRESH_INTERVAL_SEC = 60.0
_BTC_MTF_FETCH_CACHE = {"refreshed_at": 0.0, "frames": None}

# TIER4 score threshold for live execution.
# Rationale for 9.5 vs old 10.0:
#   The TIER4 extra gate compounds with HEALTHY-only and NEAR-only filters.
#   When all three align, the probability of zero-throughput ("filter starvation")
#   is high because TIER4 symbols have weaker liquidity AND the BOS confirmation
#   must also be NEAR -- a simultaneously tight constraint.
#   Lowering from 10.0 -> 9.5 admits ~0.5 score-unit headroom for TIER4 symbols
#   that pass every other safety gate, while preserving:
#     - HEALTHY exhaustion hard-gate (unchanged)
#     - NEAR bos_type hard-gate (unchanged)
#     - score >= LIVE_MIN_SCORE (9) already applies before TIER4 check
#   Net effect: TIER4 signals with score in [9.5, 10.0) are now eligible where
#   previously blocked.  Expected lift: 1-2 additional trades per week
#   for liquid TIER4 symbols (SOLUSDT, AVAXUSDT, etc.) in good regimes.
LIVE_TIER4_MIN_SCORE      = 9.5


# =====================================================================
# EXECUTOR-AWARE STRATEGY FILTER
# =====================================================================

def strategy_execution_filter(signal, ctx):
    """
    Apply strategy availability at the executor boundary.

    Generation remains execution-agnostic: scan_phase()/analyze() may emit every
    valid signal they can build.  This filter decides which strategies each
    executor is allowed to consume.
    """
    etype = (signal.get("entry_type") or "").upper()
    mode = ctx.execution_mode

    if mode == "paper":
        if etype not in PAPER_ALLOWED_ENTRY_TYPES:
            return False, f"entry_type {etype!r} not enabled for paper research"
        if etype == "EARLY_V2" and not config.get("paper_enable_early_v2", True):
            return False, "paper_enable_early_v2=False"
        if etype == "EARLY_CONT" and not config.get("paper_enable_early_continuation", True):
            return False, "paper_enable_early_continuation=False"
        if etype == "SWING_RETEST" and not config.get("paper_enable_swing_retest", True):
            return False, "paper_enable_swing_retest=False"
        if etype == "REVERSAL_CONFIRM" and not config.get("paper_enable_reversal_confirm", True):
            return False, "paper_enable_reversal_confirm=False"
        return True, ""

    if mode == "testnet":
        if etype == "EARLY_CONT":
            return False, "EARLY_CONT paper-incubation only -- testnet excluded"
        if etype == "EARLY_V2" and not config.get("enable_early_v2", True):
            return False, "enable_early_v2=False"
        if etype == "SWING_RETEST" and not config.get("enable_swing_retest", True):
            return False, "enable_swing_retest=False"
        return True, ""

    if mode == "live":
        return True, ""

    return True, ""


# =====================================================================
# TESTNET EXECUTION FILTER
# =====================================================================

def testnet_execution_filter(signal, ctx):
    """
    Apply TESTNET execution policy to a single signal.
    Returns (accepted: bool, rejection_reason: str).

    Filtering happens AFTER shared signal generation.
    Strategy/scoring engine is never aware of this filter.
    """
    # Hard gate: EARLY_CONT is paper-incubation only -- never reaches testnet
    if signal.get("entry_type") == "EARLY_CONT":
        return False, "EARLY_CONT paper-incubation only -- testnet excluded"

    # Count only bot-owned trades toward the slot limit.
    # Manual positions on the same account are never in ctx.trades,
    # but any ctx.trades entry lacking owner defaults to "bot" via normalize_trade_schema.
    open_count = sum(
        1 for t in ctx.trades
        if t.get("status", "OPEN") == "OPEN"
        and t.get("owner", "bot") == "bot"
    )
    if open_count >= TESTNET_MAX_CONCURRENT:
        return False, f"max_concurrent={TESTNET_MAX_CONCURRENT} reached (open={open_count})"

    score = signal.get("score", 0)
    if score < TESTNET_MIN_SCORE:
        return False, f"score={round(score, 1)} < threshold={TESTNET_MIN_SCORE}"

    pool_tier = signal.get("_pool_tier", "")
    if pool_tier == TESTNET_EXCLUDED_POOL_TIER:
        return False, f"pool_tier={pool_tier!r} excluded from testnet (Tier A only)"

    return True, ""


# =====================================================================
# LIVE EXECUTION FILTER
# =====================================================================

def _live_symbol_is_well_formed(symbol: str) -> bool:
    return bool(LIVE_SYMBOL_RE.fullmatch(symbol or ""))


def _live_symbol_is_tier4(symbol: str) -> bool:
    try:
        from exchange.execution_policy import get_symbol_tier
        return get_symbol_tier(symbol) == "TIER4"
    except Exception:
        return False


def _get_max_live_trades() -> int:
    raw = config.get("max_live_trades", 3)
    try:
        max_live = int(raw)
    except (TypeError, ValueError):
        print(f"[LIVE FILTER] invalid max_live_trades={raw!r}; using disabled limit 0")
        return 0
    return max(0, max_live)


def _live_open_count_for_ctx(ctx) -> int:
    return sum(
        1 for t in ctx.trades
        if t.get("status", "OPEN") == "OPEN"
        and not t.get("quarantined")
        and t.get("owner", "bot") == "bot"
    )


def _live_pending_count_for_ctx(ctx) -> int:
    return max(0, int(getattr(ctx, "live_pending_slots", 0) or 0))


def _live_slot_snapshot(ctx) -> tuple:
    open_count = _live_open_count_for_ctx(ctx)
    pending = _live_pending_count_for_ctx(ctx)
    return open_count, pending, open_count + pending


def _log_live_slot_decision(signal, ctx, reason: str) -> None:
    max_live = _get_max_live_trades()
    open_count, pending, effective = _live_slot_snapshot(ctx)
    print(
        f"[LIVE SLOT] action=reject symbol={signal.get('symbol', '')} "
        f"side={signal.get('side', '')} strategy={signal.get('strategy') or signal.get('entry_type', '')} "
        f"mode={ctx.execution_mode} max={max_live} open={open_count} "
        f"pending={pending} effective={effective} reason={reason}"
    )


def _normalize_live_bos_type(value) -> str:
    raw = str(value or "").upper().strip()
    if raw.startswith("BOS:"):
        raw = raw.split(":", 1)[1]
    if raw.startswith("BOS_"):
        raw = raw.split("_", 1)[1]
    return raw


def _normalize_dispatch_phase(value) -> str:
    raw = str(value or "").upper().strip()
    if raw.startswith("PHASE:"):
        raw = raw.split(":", 1)[1]
    return raw


def _iter_signal_metadata_strings(signal):
    for field in ("reason", "priority_final", "tags"):
        raw = signal.get(field)
        if raw is None:
            continue
        if isinstance(raw, (list, tuple, set)):
            for item in raw:
                if isinstance(item, str):
                    yield item
        elif isinstance(raw, dict):
            for key, value in raw.items():
                if isinstance(key, str):
                    yield key
                if isinstance(value, str):
                    yield value
        elif isinstance(raw, str):
            yield raw


def _extract_live_bos_type(signal) -> str:
    bos_t = _normalize_live_bos_type(signal.get("bos_type"))
    if bos_t:
        return bos_t
    return next(
        (
            _normalize_live_bos_type(item)
            for item in _iter_signal_metadata_strings(signal)
            if item.upper().startswith("BOS:")
        ),
        "",
    )


def _extract_dispatch_phase(signal) -> str:
    phase = _normalize_dispatch_phase(signal.get("phase"))
    if phase:
        return phase
    return next(
        (
            _normalize_dispatch_phase(item)
            for item in _iter_signal_metadata_strings(signal)
            if item.upper().startswith("PHASE:")
        ),
        "",
    )


def _is_confirm_pre_break_low_near(signal) -> bool:
    etype = (signal.get("entry_type") or "").upper()
    if etype != "CONFIRM":
        return False
    phase = _extract_dispatch_phase(signal)
    bos_t = _extract_live_bos_type(signal)
    return phase == "PRE_BREAK_LOW" and bos_t == "NEAR"


def _is_confirm_short_pre_break_low(signal) -> bool:
    etype = (signal.get("entry_type") or "").upper()
    if etype != "CONFIRM":
        return False
    side = (signal.get("side") or "").upper()
    if side != "SHORT":
        return False
    phase = _extract_dispatch_phase(signal)
    if not phase:
        return False
    return phase == "PRE_BREAK_LOW"


def _log_paper_confirm_pre_break_low_gate(sig, ctx):
    try:
        now_ts = time.time()
        raw_reason = sig.get("reason")
        if isinstance(raw_reason, (list, tuple)):
            reason = list(raw_reason)
        elif raw_reason not in (None, ""):
            reason = [str(raw_reason)]
        else:
            reason = []
        score = sig.get("score")
        try:
            score = float(score) if score not in (None, "") else None
        except (TypeError, ValueError):
            score = None
        planned_rr = sig.get("rr") or sig.get("planned_rr")
        try:
            planned_rr = float(planned_rr) if planned_rr not in (None, "") else None
        except (TypeError, ValueError):
            planned_rr = None
        row = {
            "timestamp": format_vn_time(now_ts),
            "timestamp_unix": now_ts,
            "symbol": sig.get("symbol", ""),
            "side": sig.get("side", ""),
            "entry_type": sig.get("entry_type", ""),
            "phase": _extract_dispatch_phase(sig),
            "reason": reason,
            "decision": "BLOCK_NEW_CONFIRM_PRE_BREAK_LOW_SHORT",
            "gate_version": "v1_paper_only",
        }
        if score is not None:
            row["score"] = score
        if planned_rr is not None:
            row["planned_rr"] = planned_rr
        market_regime = sig.get("market_regime")
        if market_regime not in (None, ""):
            row["market_regime"] = market_regime
        market_state = sig.get("market_state")
        if market_state not in (None, ""):
            row["market_state"] = market_state
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "paper_confirm_pre_break_low_gate.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        print(f"[PAPER GATE] confirm_pre_break_low_gate log failed: {exc}")


def _safe_dict(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        parsed = float(value)
        if parsed != parsed:
            return default
        return parsed
    except (TypeError, ValueError):
        return default


def _first_nonblank(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _btc_mtf_cfg_float(key, default):
    try:
        value = float(config.get(key, default))
        if value != value or value < 0:
            return default
        return value
    except (TypeError, ValueError):
        return default


def _btc_mtf_max_age_sec():
    return _btc_mtf_cfg_float("btc_mtf_max_age_sec", _BTC_MTF_DEFAULT_MAX_AGE_SEC)


def _btc_mtf_refresh_interval_sec():
    return _btc_mtf_cfg_float(
        "btc_mtf_refresh_interval_sec",
        _BTC_MTF_DEFAULT_REFRESH_INTERVAL_SEC,
    )


def _btc_mtf_unknown_context(
    data_mode="NO_INDEPENDENT_BTC_MTF_DATA",
    reason=None,
    unknown_reason=None,
    age_sec=None,
    source_ts=None,
):
    row = {field: None for field in _BTC_MTF_CONTEXT_FIELDS}
    row.update({
        "btc_context_available": False,
        "btc_m5_trend": "UNKNOWN",
        "btc_m15_trend": "UNKNOWN",
        "btc_h1_trend": "UNKNOWN",
        "btc_m5_momentum": "UNKNOWN",
        "btc_m15_momentum": "UNKNOWN",
        "btc_h1_momentum": "UNKNOWN",
        "btc_mtf_alignment": "UNKNOWN",
        "btc_alignment_reason": reason or data_mode,
        "btc_data_mode": data_mode,
        "btc_context_source_ts": source_ts,
        "btc_context_age_sec": round(age_sec, 3) if age_sec is not None else None,
        "btc_unknown_reason": unknown_reason or data_mode,
    })
    return row


def _btc_mtf_signal_ts(source):
    source = source if isinstance(source, dict) else {}
    return _safe_float(
        _first_nonblank(
            source.get("signal_created_ts"),
            source.get("signal_ts"),
            source.get("source_timestamp"),
            source.get("open_ts"),
            source.get("time"),
        )
    )


def _btc_mtf_candle_rows(df, signal_ts, interval):
    if df is None or signal_ts is None:
        return []
    signal_ms = float(signal_ts) * 1000.0
    interval_ms = float(_BTC_MTF_INTERVAL_SECS.get(interval, 0)) * 1000.0
    rows = []
    try:
        records = df.to_dict("records") if hasattr(df, "to_dict") else list(df)
    except Exception:
        return []
    for item in records:
        if not isinstance(item, dict):
            continue
        open_ms = _safe_float(_first_nonblank(item.get("time"), item.get("open_time")))
        close_ms = _safe_float(_first_nonblank(item.get("ct"), item.get("close_time")))
        if close_ms is None and open_ms is not None and interval_ms > 0:
            close_ms = open_ms + interval_ms - 1
        if close_ms is None or close_ms > signal_ms:
            continue
        close = _safe_float(item.get("close"))
        if close is None or close <= 0:
            continue
        rows.append({"close": close, "close_ts": close_ms / 1000.0})
    rows.sort(key=lambda r: r["close_ts"])
    return rows


def _btc_mtf_ema(values, period):
    if not values:
        return []
    alpha = 2.0 / (float(period) + 1.0)
    out = []
    prev = None
    for value in values:
        prev = value if prev is None else (alpha * value) + ((1.0 - alpha) * prev)
        out.append(prev)
    return out


def _btc_mtf_tf_context(df, signal_ts, interval):
    rows = _btc_mtf_candle_rows(df, signal_ts, interval)
    min_rows = _BTC_MTF_EMA_SLOW + _BTC_MTF_SLOPE_BARS + 1
    if len(rows) < min_rows:
        return None
    closes = [r["close"] for r in rows]
    ema_fast = _btc_mtf_ema(closes, _BTC_MTF_EMA_FAST)
    ema_slow = _btc_mtf_ema(closes, _BTC_MTF_EMA_SLOW)
    close = closes[-1]
    fast = ema_fast[-1]
    slow = ema_slow[-1]
    slope = fast - ema_fast[-1 - _BTC_MTF_SLOPE_BARS]
    # Formula is intentionally conservative and log-only:
    # BULLISH when close > EMA9 > EMA21 and EMA9 slope over 3 closed bars is positive;
    # BEARISH when close < EMA9 < EMA21 and that slope is negative; otherwise CHOP.
    if close > fast > slow and slope > 0:
        trend = "BULLISH"
    elif close < fast < slow and slope < 0:
        trend = "BEARISH"
    else:
        trend = "CHOP"

    recent_return = (closes[-1] - closes[-2]) / closes[-2] if closes[-2] else 0.0
    if recent_return > _BTC_MTF_MOMENTUM_EPS:
        momentum = "UP"
    elif recent_return < -_BTC_MTF_MOMENTUM_EPS:
        momentum = "DOWN"
    else:
        momentum = "FLAT"
    return {
        "trend": trend,
        "momentum": momentum,
        "source_ts": rows[-1]["close_ts"],
    }


def _btc_mtf_side_aligned(side, trend, momentum):
    side = str(side or "").upper()
    return (
        (side == "LONG" and trend == "BULLISH" and momentum == "UP")
        or (side == "SHORT" and trend == "BEARISH" and momentum == "DOWN")
    )


def _btc_mtf_side_counter(side, trend, momentum):
    side = str(side or "").upper()
    return (
        (side == "LONG" and (trend == "BEARISH" or momentum == "DOWN"))
        or (side == "SHORT" and (trend == "BULLISH" or momentum == "UP"))
    )


def _btc_mtf_alignment(side, tf_ctx):
    required = ("m5", "m15", "h1")
    if any(tf_ctx.get(k) is None for k in required):
        return "UNKNOWN", "required_btc_timeframe_missing"
    if any(
        tf_ctx[k]["trend"] == "UNKNOWN" or tf_ctx[k]["momentum"] == "UNKNOWN"
        for k in required
    ):
        return "UNKNOWN", "required_btc_context_unknown"
    if any(tf_ctx[k]["trend"] == "CHOP" or tf_ctx[k]["momentum"] == "FLAT" for k in required):
        return "BTC_CHOP", "trend_chop_or_momentum_flat_present"

    aligned = {k: _btc_mtf_side_aligned(side, tf_ctx[k]["trend"], tf_ctx[k]["momentum"]) for k in required}
    counter = {k: _btc_mtf_side_counter(side, tf_ctx[k]["trend"], tf_ctx[k]["momentum"]) for k in required}
    if all(aligned.values()):
        return "ALL_ALIGNED", "m5_m15_h1_align_with_trade_side"
    if aligned["h1"] and counter["m5"]:
        return "HTF_ALIGNED_LTF_COUNTER", "h1_aligns_m5_counters_trade_side"
    if aligned["m5"] and counter["h1"]:
        return "LTF_ALIGNED_HTF_COUNTER", "m5_aligns_h1_counters_trade_side"
    if counter["h1"]:
        return "COUNTER_HTF", "h1_counters_trade_side"
    return "MIXED", "btc_timeframes_mixed_vs_trade_side"


def _btc_mtf_fetch_frames(fetcher=None, now_ts=None):
    now_ts = time.time() if now_ts is None else float(now_ts)
    if fetcher is not None:
        return {
            prefix: fetcher("BTCUSDT", interval, is_priority=True)
            for prefix, interval in _BTC_MTF_TIMEFRAMES
        }

    cached_frames = _BTC_MTF_FETCH_CACHE.get("frames")
    refreshed_at = _safe_float(_BTC_MTF_FETCH_CACHE.get("refreshed_at"), 0.0)
    if (
        isinstance(cached_frames, dict)
        and refreshed_at
        and now_ts - refreshed_at <= _btc_mtf_refresh_interval_sec()
    ):
        return cached_frames

    from pool_pipeline import fetch
    frames = {}
    for prefix, interval in _BTC_MTF_TIMEFRAMES:
        frames[prefix] = fetch("BTCUSDT", interval, is_priority=True)
    _BTC_MTF_FETCH_CACHE["frames"] = frames
    _BTC_MTF_FETCH_CACHE["refreshed_at"] = now_ts
    return frames


def _btc_mtf_context_for_signal(source, side=None, now_ts=None, fetcher=None):
    source = source if isinstance(source, dict) else {}
    side = str(side or source.get("side") or "").upper()
    signal_ts = _btc_mtf_signal_ts(source)
    if signal_ts is None:
        return _btc_mtf_unknown_context(
            reason="entry_or_signal_timestamp_missing",
            unknown_reason="ENTRY_TS_MISSING",
        )
    if side not in {"LONG", "SHORT"}:
        return _btc_mtf_unknown_context(reason="missing_or_invalid_side")
    try:
        frames = _btc_mtf_fetch_frames(fetcher=fetcher, now_ts=now_ts)
        tf_ctx = {}
        for prefix, interval in _BTC_MTF_TIMEFRAMES:
            df = frames.get(prefix) if isinstance(frames, dict) else None
            tf_ctx[prefix] = _btc_mtf_tf_context(df, signal_ts, interval)
        if any(value is None for value in tf_ctx.values()):
            return _btc_mtf_unknown_context(
                data_mode="NO_INDEPENDENT_BTC_MTF_DATA",
                reason="independent_btc_closed_candles_unavailable",
                unknown_reason="NO_INDEPENDENT_BTC_MTF_DATA",
            )
        alignment, reason = _btc_mtf_alignment(side, tf_ctx)
        source_ts = min(tf_ctx[prefix]["source_ts"] for prefix, _ in _BTC_MTF_TIMEFRAMES)
        age = max(0.0, float(signal_ts) - float(source_ts))
        max_age = _btc_mtf_max_age_sec()
        if age > max_age:
            return _btc_mtf_unknown_context(
                data_mode="BTC_CONTEXT_STALE",
                reason=f"btc_context_age_sec={round(age, 3)} > max={max_age}",
                unknown_reason="BTC_SNAPSHOT_TOO_STALE",
                age_sec=age,
                source_ts=source_ts,
            )
        return {
            "btc_context_available": True,
            "btc_m5_trend": tf_ctx["m5"]["trend"],
            "btc_m15_trend": tf_ctx["m15"]["trend"],
            "btc_h1_trend": tf_ctx["h1"]["trend"],
            "btc_m5_momentum": tf_ctx["m5"]["momentum"],
            "btc_m15_momentum": tf_ctx["m15"]["momentum"],
            "btc_h1_momentum": tf_ctx["h1"]["momentum"],
            "btc_mtf_alignment": alignment,
            "btc_alignment_reason": reason,
            "btc_data_mode": "INDEPENDENT_BTC_MTF",
            "btc_context_source_ts": source_ts,
            "btc_context_age_sec": round(age, 3),
            "btc_unknown_reason": "NONE",
        }
    except Exception as exc:
        return _btc_mtf_unknown_context(
            data_mode="BTC_CONTEXT_FETCH_ERROR",
            reason=f"{type(exc).__name__}: {exc}",
            unknown_reason="BTC_CONTEXT_FETCH_ERROR",
        )


# =====================================================================
# BTC_ALIGNMENT_INSTRUMENTATION (SHADOW / LOG-ONLY)
# ---------------------------------------------------------------------
# Independent BTC / market context captured at CONFIRM_SMC_RESEARCH decision
# time. This is pure instrumentation: it NEVER changes a paper/live decision,
# NEVER gates, and NEVER mirrors the trade side (bias is derived from BTC price
# action only). It reuses the existing independent BTC MTF fetch/candle helpers
# and writes one row per decision to
# logs/btc_alignment_instrumentation_shadow.jsonl.
# =====================================================================
_BTC_ALIGN_VERSION = "btc_align_instrument_v1_log_only"
_BTC_ALIGN_CHANGE_BARS = 3
_BTC_ALIGN_STRUCT_BARS = 20
_BTC_ALIGN_VOL_BARS = 20
_BTC_ALIGN_VOL_SPIKE_MULT = 1.8
_BTC_ALIGN_NEAR_PCT = 0.005
_BTC_ALIGN_VOL_HIGH = 0.006
_BTC_ALIGN_VOL_LOW = 0.0015

# The independent BTC context fields the shadow adds. Kept separate so the
# simulator can strip them and assert the underlying decision is unchanged.
_BTC_ALIGN_CONTEXT_FIELDS = (
    "btc_5m_dir",
    "btc_15m_dir",
    "btc_1h_dir",
    "btc_5m_change_pct",
    "btc_15m_change_pct",
    "btc_1h_change_pct",
    "btc_slope_15m",
    "btc_bos_state",
    "btc_structure_state",
    "btc_volatility_state",
    "btc_vol_spike",
    "btc_near_local_high",
    "btc_near_local_low",
    "btc_alignment_independent",
    "btc_bias_independent",
    "btc_bias_votes",
    "btc_context_quality",
    "btc_context_missing_fields",
    "btc_context_version",
    "btc_context_source_ts",
    "btc_context_age_sec",
    "btc_context_signal_ts",
)

def _btc_align_stdev(values):
    if not values or len(values) < 2:
        return None
    n = len(values)
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    return var ** 0.5


def _btc_align_ohlcv_rows(df, signal_ts, interval):
    """Closed BTC candles at/at-or-before signal_ts, keeping OHLCV."""
    if df is None or signal_ts is None:
        return []
    signal_ms = float(signal_ts) * 1000.0
    interval_ms = float(_BTC_MTF_INTERVAL_SECS.get(interval, 0)) * 1000.0
    rows = []
    try:
        records = df.to_dict("records") if hasattr(df, "to_dict") else list(df)
    except Exception:
        return []
    for item in records:
        if not isinstance(item, dict):
            continue
        open_ms = _safe_float(_first_nonblank(item.get("time"), item.get("open_time")))
        close_ms = _safe_float(_first_nonblank(item.get("ct"), item.get("close_time")))
        if close_ms is None and open_ms is not None and interval_ms > 0:
            close_ms = open_ms + interval_ms - 1
        if close_ms is None or close_ms > signal_ms:
            continue
        close = _safe_float(item.get("close"))
        if close is None or close <= 0:
            continue
        rows.append({
            "close": close,
            "high": _safe_float(item.get("high")),
            "low": _safe_float(item.get("low")),
            "volume": _safe_float(item.get("volume")),
            "close_ts": close_ms / 1000.0,
        })
    rows.sort(key=lambda r: r["close_ts"])
    return rows


def _btc_align_tf_metrics(df, signal_ts, interval):
    """Per-TF independent direction + change_pct + slope from BTC OHLCV.

    Direction uses the SAME conservative EMA formula as _btc_mtf_tf_context
    (BULLISH: close>EMA9>EMA21 & positive slope; BEARISH: inverse; else CHOP).
    Returns None when there are not enough closed candles.
    """
    rows = _btc_align_ohlcv_rows(df, signal_ts, interval)
    min_rows = _BTC_MTF_EMA_SLOW + _BTC_MTF_SLOPE_BARS + 1
    if len(rows) < min_rows:
        return None
    closes = [r["close"] for r in rows]
    ema_fast = _btc_mtf_ema(closes, _BTC_MTF_EMA_FAST)
    ema_slow = _btc_mtf_ema(closes, _BTC_MTF_EMA_SLOW)
    close = closes[-1]
    fast = ema_fast[-1]
    slow = ema_slow[-1]
    slope = fast - ema_fast[-1 - _BTC_MTF_SLOPE_BARS]
    if close > fast > slow and slope > 0:
        direction = "BULLISH"
    elif close < fast < slow and slope < 0:
        direction = "BEARISH"
    else:
        direction = "CHOP"
    change_pct = None
    if len(closes) > _BTC_ALIGN_CHANGE_BARS:
        ref = closes[-1 - _BTC_ALIGN_CHANGE_BARS]
        if ref:
            change_pct = round((close - ref) / ref * 100.0, 4)
    return {
        "dir": direction,
        "slope": slope,
        "change_pct": change_pct,
        "closes": closes,
        "rows": rows,
        "source_ts": rows[-1]["close_ts"],
    }


def _btc_align_bias_independent(m5_dir, m15_dir, h1_dir):
    """Derive BTC bias from BTC per-TF directions ONLY (never the trade side).

    Emits BULLISH / BEARISH / NEUTRAL_OR_CHOP / UNKNOWN by majority vote.
    """
    dirs = [m5_dir, m15_dir, h1_dir]
    known = [d for d in dirs if d in ("BULLISH", "BEARISH", "CHOP")]
    bull = sum(1 for d in dirs if d == "BULLISH")
    bear = sum(1 for d in dirs if d == "BEARISH")
    if not known:
        bias = "UNKNOWN"
    elif bull > bear:
        bias = "BULLISH"
    elif bear > bull:
        bias = "BEARISH"
    else:
        bias = "NEUTRAL_OR_CHOP"
    return bias, {"bull": bull, "bear": bear, "known": len(known)}


def _btc_align_alignment(side, bias):
    """Compare trade side vs INDEPENDENT BTC bias.

    LONG+BULLISH / SHORT+BEARISH => ALIGNED
    LONG+BEARISH / SHORT+BULLISH => COUNTER
    NEUTRAL_OR_CHOP => NEUTRAL ; UNKNOWN/unknown side => UNKNOWN
    """
    side = str(side or "").upper()
    if bias == "UNKNOWN" or side not in ("LONG", "SHORT"):
        return "UNKNOWN"
    if bias == "NEUTRAL_OR_CHOP":
        return "NEUTRAL"
    if (side == "LONG" and bias == "BULLISH") or (side == "SHORT" and bias == "BEARISH"):
        return "ALIGNED"
    return "COUNTER"


def _btc_align_unknown_context(signal_ts=None, reason="NO_INDEPENDENT_BTC_DATA"):
    row = {field: None for field in _BTC_ALIGN_CONTEXT_FIELDS}
    row.update({
        "btc_5m_dir": "UNKNOWN",
        "btc_15m_dir": "UNKNOWN",
        "btc_1h_dir": "UNKNOWN",
        "btc_bos_state": "UNKNOWN",
        "btc_structure_state": "UNKNOWN",
        "btc_volatility_state": "UNKNOWN",
        "btc_alignment_independent": "UNKNOWN",
        "btc_bias_independent": "UNKNOWN",
        "btc_context_quality": "MISSING",
        "btc_context_missing_fields": [reason],
        "btc_context_version": _BTC_ALIGN_VERSION,
        "btc_context_signal_ts": signal_ts,
    })
    return row


def _btc_independent_context(source, side=None, now_ts=None, fetcher=None):
    """Build the independent BTC / market context (shadow, never gates).

    Reuses the production BTC MTF fetch cache; adds richer per-TF metrics,
    structure/volatility state, and a side-independent bias + alignment.
    Always returns a dict; on any failure returns an UNKNOWN context.
    """
    source = source if isinstance(source, dict) else {}
    side = str(side or source.get("side") or "").upper()
    signal_ts = _btc_mtf_signal_ts(source)
    if signal_ts is None:
        signal_ts = _safe_float(now_ts) if now_ts is not None else time.time()
    try:
        frames = _btc_mtf_fetch_frames(fetcher=fetcher, now_ts=now_ts)
        tf_metrics = {}
        for prefix, interval in _BTC_MTF_TIMEFRAMES:
            df = frames.get(prefix) if isinstance(frames, dict) else None
            tf_metrics[prefix] = _btc_align_tf_metrics(df, signal_ts, interval)

        missing_fields = []
        m5 = tf_metrics.get("m5")
        m15 = tf_metrics.get("m15")
        h1 = tf_metrics.get("h1")
        m5_dir = m5["dir"] if m5 else "UNKNOWN"
        m15_dir = m15["dir"] if m15 else "UNKNOWN"
        h1_dir = h1["dir"] if h1 else "UNKNOWN"
        for tf_prefix, tf in (("btc_5m_dir", m5), ("btc_15m_dir", m15), ("btc_1h_dir", h1)):
            if tf is None:
                missing_fields.append(tf_prefix)

        bias, votes = _btc_align_bias_independent(m5_dir, m15_dir, h1_dir)
        alignment = _btc_align_alignment(side, bias)

        # --- 15m-based structure / volatility / location metrics ------------
        bos_state = "UNKNOWN"
        structure_state = "UNKNOWN"
        volatility_state = "UNKNOWN"
        vol_spike = None
        near_high = None
        near_low = None
        slope_15m = round(m15["slope"], 6) if m15 else None
        if m15 is not None:
            rows = m15["rows"]
            closes = m15["closes"]
            last_close = closes[-1]
            window = rows[-(_BTC_ALIGN_STRUCT_BARS + 1):-1]
            highs = [r["high"] for r in window if r["high"] is not None]
            lows = [r["low"] for r in window if r["low"] is not None]
            if len(highs) >= 5 and len(lows) >= 5:
                prior_high = max(highs)
                prior_low = min(lows)
                if last_close > prior_high:
                    bos_state = "BOS_UP"
                elif last_close < prior_low:
                    bos_state = "BOS_DOWN"
                else:
                    bos_state = "NONE"
            else:
                missing_fields.append("btc_bos_state")
            # structure via thirds of recent closes
            seg = closes[-_BTC_ALIGN_STRUCT_BARS:]
            if len(seg) >= 9:
                third = len(seg) // 3
                first_mean = sum(seg[:third]) / third
                mid_mean = sum(seg[third:2 * third]) / third
                last_mean = sum(seg[2 * third:]) / (len(seg) - 2 * third)
                if last_mean > mid_mean > first_mean:
                    structure_state = "HH_HL_UPTREND"
                elif last_mean < mid_mean < first_mean:
                    structure_state = "LH_LL_DOWNTREND"
                else:
                    structure_state = "RANGE"
            else:
                missing_fields.append("btc_structure_state")
            # volatility via stdev of recent returns
            rets = [
                (closes[i] - closes[i - 1]) / closes[i - 1]
                for i in range(1, len(closes))
                if closes[i - 1]
            ]
            tail = rets[-_BTC_ALIGN_VOL_BARS:]
            sd = _btc_align_stdev(tail)
            if sd is not None:
                if sd >= _BTC_ALIGN_VOL_HIGH:
                    volatility_state = "HIGH"
                elif sd <= _BTC_ALIGN_VOL_LOW:
                    volatility_state = "LOW"
                else:
                    volatility_state = "NORMAL"
            else:
                missing_fields.append("btc_volatility_state")
            # volume spike on last closed 15m candle
            vols = [r["volume"] for r in rows if r["volume"] is not None]
            if len(vols) >= _BTC_ALIGN_VOL_BARS + 1:
                prev = vols[-(_BTC_ALIGN_VOL_BARS + 1):-1]
                mean_prev = sum(prev) / len(prev) if prev else 0.0
                vol_spike = bool(mean_prev > 0 and vols[-1] > _BTC_ALIGN_VOL_SPIKE_MULT * mean_prev)
            else:
                missing_fields.append("btc_vol_spike")
            # proximity to local high/low
            if len(highs) >= 5 and len(lows) >= 5:
                loc_high = max(highs + [last_close])
                loc_low = min(lows + [last_close])
                near_high = bool(last_close >= loc_high * (1.0 - _BTC_ALIGN_NEAR_PCT))
                near_low = bool(last_close <= loc_low * (1.0 + _BTC_ALIGN_NEAR_PCT))
            else:
                missing_fields.append("btc_near_local_high")
                missing_fields.append("btc_near_local_low")
        else:
            missing_fields.extend([
                "btc_slope_15m", "btc_bos_state", "btc_structure_state",
                "btc_volatility_state", "btc_vol_spike",
                "btc_near_local_high", "btc_near_local_low",
            ])

        resolved_tfs = sum(1 for d in (m5_dir, m15_dir, h1_dir) if d != "UNKNOWN")
        if resolved_tfs == 3:
            quality = "OK"
        elif resolved_tfs >= 1:
            quality = "PARTIAL"
        else:
            quality = "MISSING"

        source_ts = None
        present_ts = [tf["source_ts"] for tf in (m5, m15, h1) if tf is not None]
        if present_ts:
            source_ts = min(present_ts)
        age_sec = None
        if source_ts is not None:
            age_sec = round(max(0.0, float(signal_ts) - float(source_ts)), 3)

        return {
            "btc_5m_dir": m5_dir,
            "btc_15m_dir": m15_dir,
            "btc_1h_dir": h1_dir,
            "btc_5m_change_pct": m5["change_pct"] if m5 else None,
            "btc_15m_change_pct": m15["change_pct"] if m15 else None,
            "btc_1h_change_pct": h1["change_pct"] if h1 else None,
            "btc_slope_15m": slope_15m,
            "btc_bos_state": bos_state,
            "btc_structure_state": structure_state,
            "btc_volatility_state": volatility_state,
            "btc_vol_spike": vol_spike,
            "btc_near_local_high": near_high,
            "btc_near_local_low": near_low,
            "btc_alignment_independent": alignment,
            "btc_bias_independent": bias,
            "btc_bias_votes": votes,
            "btc_context_quality": quality,
            "btc_context_missing_fields": missing_fields,
            "btc_context_version": _BTC_ALIGN_VERSION,
            "btc_context_source_ts": source_ts,
            "btc_context_age_sec": age_sec,
            "btc_context_signal_ts": signal_ts,
        }
    except Exception as exc:
        return _btc_align_unknown_context(
            signal_ts=signal_ts,
            reason=f"BTC_CONTEXT_ERROR:{type(exc).__name__}",
        )


def _btc_alignment_instrumentation_write(row):
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "btc_alignment_instrumentation_shadow.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        print(f"[BTC ALIGNMENT INSTRUMENTATION] log failed: {exc}")


# =====================================================================
# BTC_BIAS_SIDE_ENABLE_SHADOW (SHADOW / LOG-ONLY)
# ---------------------------------------------------------------------
# Log-only side-enable evaluator for the CONFIRM_SMC_RESEARCH lane. Given a
# trade side and the INDEPENDENT BTC context already computed above
# (btc_bias_independent / btc_alignment_independent / btc_context_quality /
# btc_context_missing_fields), it classifies whether the side would be allowed
# under the top-down regime rule surfaced by MARKET_BIAS_AND_TRADING_EDGE_AUDIT:
#   LONG  allowed only when independent BTC bias is BULLISH
#   SHORT allowed only when independent BTC bias is BEARISH
# NEUTRAL_OR_CHOP and UNKNOWN/missing are classified SEPARATELY and are NEVER
# auto-allowed. This NEVER changes any paper/live decision, never gates, and
# reads ONLY the independent BTC fields (the old degenerate BTC MTF shadow
# fields are never consulted). Scoped by call-site: the only callers are the
# paper qualified-research and live SMC-research decision loggers.
# =====================================================================
_BTC_SIDE_ENABLE_VERSION = "btc_bias_side_enable_shadow_v1_log_only"

# The additive shadow fields folded into the instrumentation payload. Kept
# separate so the simulator can strip them and assert the underlying decision
# is byte-for-byte unchanged.
_BTC_SIDE_ENABLE_SHADOW_FIELDS = (
    "btc_side_enable_shadow_label",
    "btc_side_enable_shadow_allow",
    "btc_side_enable_shadow_reason",
    "btc_side_enable_shadow_version",
    "btc_side_enable_bias",
    "btc_side_enable_alignment",
    "btc_side_enable_context_quality",
)


def _btc_bias_side_enable_eval(side, btc_ctx):
    """Pure side-enable classifier. INDEPENDENT BTC fields only; log-only.

    Returns (label, allow, reason) where allow is True / False / None:
      LONG  + BULLISH        -> BTC_SIDE_ENABLE_ALLOW                  (True)
      SHORT + BEARISH        -> BTC_SIDE_ENABLE_ALLOW                  (True)
      LONG  + BEARISH        -> BTC_SIDE_ENABLE_BLOCK_COUNTER_BIAS     (False)
      SHORT + BULLISH        -> BTC_SIDE_ENABLE_BLOCK_COUNTER_BIAS     (False)
      NEUTRAL_OR_CHOP        -> BTC_SIDE_ENABLE_BLOCK_NEUTRAL_CHOP     (False)
      UNKNOWN / missing ctx  -> BTC_SIDE_ENABLE_UNKNOWN_MISSING_CONTEXT (None)
    allow=None marks the unknown/missing class as "not automatically allowed"
    and distinct from an explicit block.
    """
    btc_ctx = btc_ctx if isinstance(btc_ctx, dict) else {}
    side = str(side or "").upper()
    bias = str(btc_ctx.get("btc_bias_independent") or "UNKNOWN").upper()
    quality = str(btc_ctx.get("btc_context_quality") or "MISSING").upper()

    # Unknown / missing context: classified separately, never auto-allowed.
    if (
        bias not in ("BULLISH", "BEARISH", "NEUTRAL_OR_CHOP")
        or quality == "MISSING"
        or side not in ("LONG", "SHORT")
    ):
        return (
            "BTC_SIDE_ENABLE_UNKNOWN_MISSING_CONTEXT",
            None,
            f"unknown_missing_context|side={side or 'NA'}|bias={bias}|quality={quality}",
        )

    if bias == "NEUTRAL_OR_CHOP":
        return (
            "BTC_SIDE_ENABLE_BLOCK_NEUTRAL_CHOP",
            False,
            f"block_neutral_chop|side={side}|bias={bias}",
        )

    if (side == "LONG" and bias == "BULLISH") or (side == "SHORT" and bias == "BEARISH"):
        return (
            "BTC_SIDE_ENABLE_ALLOW",
            True,
            f"allow|side={side}|bias={bias}",
        )

    return (
        "BTC_SIDE_ENABLE_BLOCK_COUNTER_BIAS",
        False,
        f"block_counter_bias|side={side}|bias={bias}",
    )


def _btc_bias_side_enable_shadow_fields(side, btc_ctx):
    """Build the additive side-enable payload fields (log-only)."""
    label, allow, reason = _btc_bias_side_enable_eval(side, btc_ctx)
    btc_ctx = btc_ctx if isinstance(btc_ctx, dict) else {}
    return {
        "btc_side_enable_shadow_label": label,
        "btc_side_enable_shadow_allow": allow,
        "btc_side_enable_shadow_reason": reason,
        "btc_side_enable_shadow_version": _BTC_SIDE_ENABLE_VERSION,
        "btc_side_enable_bias": btc_ctx.get("btc_bias_independent"),
        "btc_side_enable_alignment": btc_ctx.get("btc_alignment_independent"),
        "btc_side_enable_context_quality": btc_ctx.get("btc_context_quality"),
    }


def _btc_bias_side_enable_shadow_write(row):
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "btc_bias_side_enable_shadow.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        print(f"[BTC BIAS SIDE ENABLE SHADOW] log failed: {exc}")


def _btc_alignment_instrumentation_shadow(
    source,
    execution_mode,
    v1_decision,
    v1_reason,
    side=None,
    trade=None,
    gate_fields=None,
    v2b_fields=None,
    now_ts=None,
    fetcher=None,
):
    """Assemble + append one BTC_ALIGNMENT_INSTRUMENTATION shadow row.

    SHADOW / LOG-ONLY. Returns the row (or None on failure). It reads decision
    context that already exists at the call site and never feeds back into any
    decision. Fully guarded so it can never raise into the caller.
    """
    try:
        source = source if isinstance(source, dict) else {}
        trade = trade if isinstance(trade, dict) else None
        gate_fields = gate_fields if isinstance(gate_fields, dict) else {}
        v2b_fields = v2b_fields if isinstance(v2b_fields, dict) else {}
        now_ts = now_ts if now_ts is not None else time.time()
        side = str(side or source.get("side") or "").upper()

        btc_ctx = _btc_independent_context(source, side=side, now_ts=now_ts, fetcher=fetcher)

        entry = _first_nonblank(
            (trade or {}).get("entry"), (trade or {}).get("entry_real"),
            source.get("entry"),
        )
        sl = _first_nonblank((trade or {}).get("sl"), source.get("sl"))
        tp = _first_nonblank((trade or {}).get("tp"), source.get("tp"))
        rr = _first_nonblank(
            (trade or {}).get("rr"), (trade or {}).get("planned_rr"),
            source.get("planned_rr"), source.get("rr"),
        )

        row = {
            "ts": now_ts,
            "timestamp": format_vn_time(now_ts),
            "event_type": "BTC_ALIGNMENT_INSTRUMENTATION_SHADOW",
            "execution_mode": execution_mode,
            "symbol": str(source.get("symbol") or ""),
            "side": side,
            "signal_ts": _btc_mtf_signal_ts(source),
            "dedup_key": str(source.get("dedup_key") or ""),
            "entry_type": str(
                source.get("entry_type") or (trade or {}).get("entry_type")
                or "CONFIRM_SMC_RESEARCH"
            ),
            "v1_decision": v1_decision,
            "v1_reason": v1_reason,
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "rr": rr,
            # paper location gate context (if available at this call site)
            "paper_location_gate_would_block": gate_fields.get(
                "confirm_smc_entry_location_would_block"
            ),
            "trade_location_quality": gate_fields.get("trade_location_quality"),
            "smc_zone": gate_fields.get("smc_zone"),
            "market_regime": gate_fields.get("market_regime"),
            # V2B allowlist context (if available at this call site)
            "v2b_label": v2b_fields.get("v2b_label"),
            "v2b_match": v2b_fields.get("v2b_match"),
            "v2b_reason": v2b_fields.get("v2b_reason"),
            "v2b_market_bias": v2b_fields.get("v2b_market_bias"),
            "v2b_direction_alignment": v2b_fields.get("v2b_direction_alignment"),
        }
        row.update(btc_ctx)

        # --- BTC_BIAS_SIDE_ENABLE_SHADOW (log-only, CONFIRM_SMC_RESEARCH) -----
        # Additive side-enable classification + dedicated forward log. Reads
        # only the independent BTC fields just computed; never changes any
        # paper/live decision. Guarded so a failure here can never prevent the
        # main instrumentation write nor raise into the caller.
        try:
            side_enable = _btc_bias_side_enable_shadow_fields(side, btc_ctx)
            row.update(side_enable)
            _btc_bias_side_enable_shadow_write({
                "ts": row.get("ts"),
                "timestamp": row.get("timestamp"),
                "event_type": "BTC_BIAS_SIDE_ENABLE_SHADOW",
                "execution_mode": execution_mode,
                "symbol": row.get("symbol"),
                "side": side,
                "signal_ts": row.get("signal_ts"),
                "dedup_key": row.get("dedup_key"),
                "entry_type": row.get("entry_type"),
                "v1_decision": v1_decision,
                "v1_reason": v1_reason,
                "entry": row.get("entry"),
                "sl": row.get("sl"),
                "tp": row.get("tp"),
                "rr": row.get("rr"),
                "btc_bias_independent": btc_ctx.get("btc_bias_independent"),
                "btc_alignment_independent": btc_ctx.get("btc_alignment_independent"),
                "btc_context_quality": btc_ctx.get("btc_context_quality"),
                "btc_context_missing_fields": btc_ctx.get("btc_context_missing_fields"),
                "shadow_label": side_enable["btc_side_enable_shadow_label"],
                "shadow_allow": side_enable["btc_side_enable_shadow_allow"],
                "shadow_version": _BTC_SIDE_ENABLE_VERSION,
                # PAPER_LOCATION_GATE context (read-only passthrough, if present)
                "paper_location_gate_would_block": row.get("paper_location_gate_would_block"),
                "trade_location_quality": row.get("trade_location_quality"),
                "smc_zone": row.get("smc_zone"),
                "market_regime": row.get("market_regime"),
                # V2B allowlist context (read-only passthrough, if present)
                "v2b_label": row.get("v2b_label"),
                "v2b_match": row.get("v2b_match"),
                "v2b_reason": row.get("v2b_reason"),
                "v2b_market_bias": row.get("v2b_market_bias"),
                "v2b_direction_alignment": row.get("v2b_direction_alignment"),
                # SMC_PA_SCORE_V3 context (read-only passthrough, if available).
                # Sourced from whichever carrier holds it; None when the V3
                # summary has not been merged at this call site. Never computed
                # or modified here.
                "smc_pa_v3_total_score": _first_nonblank(
                    gate_fields.get("smc_pa_v3_total_score"),
                    v2b_fields.get("smc_pa_v3_total_score"),
                ),
                "smc_pa_v3_score_band": _first_nonblank(
                    gate_fields.get("smc_pa_v3_score_band"),
                    v2b_fields.get("smc_pa_v3_score_band"),
                ),
                "smc_pa_v3_missing_components": _first_nonblank(
                    gate_fields.get("smc_pa_v3_missing_components"),
                    v2b_fields.get("smc_pa_v3_missing_components"),
                ),
                "smc_pa_v3_version": _first_nonblank(
                    gate_fields.get("smc_pa_v3_version"),
                    v2b_fields.get("smc_pa_v3_version"),
                ),
            })
        except Exception as _side_exc:
            print(f"[BTC BIAS SIDE ENABLE SHADOW] shadow failed: {_side_exc}")

        _btc_alignment_instrumentation_write(row)
        return row
    except Exception as exc:
        print(f"[BTC ALIGNMENT INSTRUMENTATION] shadow failed: {exc}")
        return None

def _signal_smc_context(signal):
    score_breakdown = _safe_dict(signal.get("score_breakdown"))
    breakdown = _safe_dict(signal.get("breakdown"))
    breakdown_json = _safe_dict(signal.get("breakdown_json"))

    smc = _safe_dict(signal.get("smc"))
    if not smc:
        smc = _safe_dict(score_breakdown.get("smc"))
    if not smc:
        smc = _safe_dict(breakdown.get("smc"))
    if not smc:
        smc = _safe_dict(breakdown_json.get("smc"))
    return smc


def _signal_smc_value(signal, field):
    value = signal.get(field)
    if value not in (None, ""):
        return value
    smc = _signal_smc_context(signal)
    value = smc.get(field)
    return value if value not in (None, "") else ""


def _paper_smc_confirm_filter_decision(signal, mode="strict_conflict"):
    if (signal.get("entry_type") or "").upper() != "CONFIRM":
        return False, "", ""

    mode = str(mode or "strict_conflict").lower()
    side = str(signal.get("side") or "").upper()
    smc_bias = str(_signal_smc_value(signal, "smc_bias") or "").upper()
    range_context = str(_signal_smc_value(signal, "range_context") or "").upper()
    liquidity_sweep = str(_signal_smc_value(signal, "liquidity_sweep") or "").upper()
    invalid_context = _as_list_copy(signal.get("invalid_context"))
    if not invalid_context:
        invalid_context = _as_list_copy(_signal_smc_context(signal).get("invalid_context"))

    if side not in {"LONG", "SHORT"}:
        return False, "", ""

    if mode != "expanded_conflict":
        if side == "LONG" and smc_bias == "BEARISH" and range_context == "RANGE_HIGH":
            return True, "paper_smc_confirm_conflict_filter", "strict_long_bearish_range_high"
        if side == "SHORT" and smc_bias == "BULLISH" and range_context == "RANGE_LOW":
            return True, "paper_smc_confirm_conflict_filter", "strict_short_bullish_range_low"
        return False, "", ""

    if invalid_context:
        return True, "paper_smc_confirm_phase2_expanded_filter", "invalid_context_nonempty"

    if side == "LONG":
        if smc_bias == "BEARISH" and range_context in {"RANGE_HIGH", "MID"}:
            return True, "paper_smc_confirm_phase2_expanded_filter", "long_bearish_high_or_mid"
        if liquidity_sweep == "SWEEP_HIGH" and smc_bias not in {"", "UNKNOWN", "BULLISH"}:
            return True, "paper_smc_confirm_phase2_expanded_filter", "long_sweep_high_not_bullish"

    if side == "SHORT":
        if smc_bias == "BULLISH" and range_context in {"RANGE_LOW", "MID"}:
            return True, "paper_smc_confirm_phase2_expanded_filter", "short_bullish_low_or_mid"
        if liquidity_sweep == "SWEEP_LOW" and smc_bias not in {"", "UNKNOWN", "BEARISH"}:
            return True, "paper_smc_confirm_phase2_expanded_filter", "short_sweep_low_not_bearish"

    return False, "", ""


def _is_confirm_retest_violated_bos_trap(signal) -> bool:
    etype = (signal.get("entry_type") or "").upper()
    if etype != "CONFIRM":
        return False

    raw_reason = signal.get("reason") or []
    if isinstance(raw_reason, (list, tuple, set)):
        reason_items = raw_reason
    else:
        reason_items = [raw_reason]

    reason_tokens = {str(item or "").upper().strip() for item in reason_items}
    has_retest_violated = "RETEST:VIOLATED" in reason_tokens
    has_bos_trap = "BOS:TRAP" in reason_tokens or "SOFT:BOS_TRAP" in reason_tokens
    return has_retest_violated and has_bos_trap


def _signal_tag_value(signal, prefix: str) -> str:
    prefix_u = str(prefix or "").upper()
    for item in _iter_signal_metadata_strings(signal):
        text = str(item or "")
        if text.upper().startswith(prefix_u):
            return text.split(":", 1)[1] if ":" in text else text
    return ""


def _json_safe_copy(value):
    try:
        return copy.deepcopy(value)
    except Exception:
        return str(value)


def _paper_structural_context(signal):
    score_breakdown = _safe_dict(signal.get("score_breakdown"))
    breakdown = _safe_dict(signal.get("breakdown"))
    breakdown_json = _safe_dict(signal.get("breakdown_json"))
    structural_context = _safe_dict(signal.get("structural_context"))
    if not structural_context:
        structural_context = _safe_dict(score_breakdown.get("structural_context"))
    if not structural_context:
        structural_context = _safe_dict(breakdown.get("structural_context"))
    if not structural_context:
        structural_context = _safe_dict(breakdown_json.get("structural_context"))
    return structural_context


def _paper_structural_context_value(signal, structural_context, field):
    return _first_nonblank(signal.get(field), structural_context.get(field))


def _paper_structural_score_modifier(signal):
    structural_context = _paper_structural_context(signal)
    structural_decision = str(
        _paper_structural_context_value(signal, structural_context, "structural_decision_shadow")
        or ""
    ).upper()
    bos_quality = str(
        _paper_structural_context_value(signal, structural_context, "bos_quality") or ""
    ).upper()
    volume_confirmation = str(
        _paper_structural_context_value(signal, structural_context, "volume_confirmation") or ""
    ).upper()

    modifier = 0.0
    reasons = []
    if volume_confirmation == "DIVERGENCE":
        modifier -= 0.5
        reasons.append("volume_confirmation_divergence:-0.5")
    if bos_quality == "TRAP":
        modifier -= 0.5
        reasons.append("bos_quality_trap:-0.5")
    if bos_quality == "NO_FOLLOWTHROUGH":
        modifier -= 0.25
        reasons.append("bos_quality_no_followthrough:-0.25")
    if structural_decision == "UNKNOWN" and volume_confirmation == "DIVERGENCE":
        modifier -= 0.5
        reasons.append("unknown_structural_decision_with_divergence:-0.5")

    min_mod = _safe_float(config.get("paper_structural_score_modifier_min"), -0.5)
    max_mod = _safe_float(config.get("paper_structural_score_modifier_max"), 0.5)
    if min_mod is None:
        min_mod = -0.5
    if max_mod is None:
        max_mod = 0.5
    if min_mod > max_mod:
        min_mod, max_mod = max_mod, min_mod
    modifier = max(min_mod, min(max_mod, modifier))
    return modifier, reasons, structural_context


def _log_live_shadow_smc_decision(signal, ctx):
    try:
        if getattr(ctx, "execution_mode", None) != "live":
            return None
        if config.get("live_shadow_smc_decision_enabled") is not True:
            return None

        entry_type = str(signal.get("entry_type") or "").upper()
        if entry_type != "CONFIRM":
            return None

        now_ts = time.time()
        ttl = int(config.get("live_shadow_smc_decision_dedup_ttl_secs", 900) or 900)
        ttl = max(1, ttl)
        expired_keys = [
            key for key, last_ts in _live_shadow_smc_decision_dedup.items()
            if now_ts - float(last_ts or 0) > ttl
        ]
        for key in expired_keys:
            _live_shadow_smc_decision_dedup.pop(key, None)

        symbol = signal.get("symbol")
        side = signal.get("side")
        signal_created_ts = _first_nonblank(
            signal.get("signal_created_ts"),
            signal.get("source_timestamp"),
            signal.get("collector_ts"),
            signal.get("timestamp"),
            signal.get("entry"),
        )
        dedup_key = "|".join([
            "LIVE_SHADOW_SMC_DECISION",
            str(symbol or ""),
            str(side or ""),
            str(signal_created_ts or ""),
        ])
        if dedup_key in _live_shadow_smc_decision_dedup:
            return None
        _live_shadow_smc_decision_dedup[dedup_key] = now_ts

        try:
            modifier, reasons, structural_context = _paper_structural_score_modifier(signal)
        except Exception as exc:
            modifier = 0.0
            reasons = [f"modifier_error:{type(exc).__name__}"]
            structural_context = _paper_structural_context(signal)

        score_live = _safe_float(signal.get("score"))
        score_v2_current = _safe_float(
            _first_nonblank(signal.get("score_v2_current"), structural_context.get("score_v2_current")),
            score_live,
        )
        score_v2_structural_shadow = _safe_float(
            _first_nonblank(
                signal.get("score_v2_structural_shadow"),
                structural_context.get("score_v2_structural_shadow"),
            )
        )
        live_smc_effective_score = (
            round(score_live + modifier, 4)
            if score_live is not None and modifier is not None
            else None
        )
        structural_shadow_effective_score = (
            round(score_v2_structural_shadow + modifier, 4)
            if score_v2_structural_shadow is not None and modifier is not None
            else None
        )
        structural_effective_score = live_smc_effective_score
        if modifier < 0:
            smc_decision_divergence = "WOULD_LOWER"
        elif modifier > 0:
            smc_decision_divergence = "WOULD_RAISE"
        else:
            smc_decision_divergence = "NO_CHANGE"

        with ctx.lock:
            live_open_count, live_pending, _effective = _live_slot_snapshot(ctx)

        row = {
            "timestamp": format_vn_time(now_ts),
            "timestamp_unix": now_ts,
            "event_type": "LIVE_SHADOW_SMC_DECISION",
            "action": "OBSERVE_ONLY",
            "symbol": symbol,
            "side": side,
            "entry_type": entry_type,
            "signal_created_ts": signal.get("signal_created_ts"),
            "dedup_key": dedup_key,
            "score_live": score_live,
            "exhaustion_cls": signal.get("exhaustion_cls"),
            "bos_type": _extract_live_bos_type(signal),
            "pool_tier": signal.get("_pool_tier"),
            "execution_tier": signal.get("execution_tier"),
            "passed_live_hard_filters": True,
            "score_v2_current": score_v2_current,
            "score_v2_structural_shadow": score_v2_structural_shadow,
            "structural_modifier": modifier,
            "live_smc_effective_score": live_smc_effective_score,
            "structural_shadow_effective_score": structural_shadow_effective_score,
            "effective_score_base": "score_live",
            "structural_effective_score": structural_effective_score,
            "structural_effective_score_base": "score_live",
            "modifier_reasons": list(reasons or []),
            "structural_decision_shadow": _paper_structural_context_value(
                signal, structural_context, "structural_decision_shadow"
            ),
            "bos_quality": _paper_structural_context_value(signal, structural_context, "bos_quality"),
            "choch_quality": _paper_structural_context_value(signal, structural_context, "choch_quality"),
            "poi_location_quality": _paper_structural_context_value(
                signal, structural_context, "poi_location_quality"
            ),
            "volume_confirmation": _paper_structural_context_value(
                signal, structural_context, "volume_confirmation"
            ),
            "trade_location_quality": _paper_structural_context_value(
                signal, structural_context, "trade_location_quality"
            ),
            "smc_would_downrank": modifier < 0,
            "smc_decision_divergence": smc_decision_divergence,
            "live_open_count": live_open_count,
            "live_pending": live_pending,
            "max_live_trades": _get_max_live_trades(),
            "min_log_only": bool(config.get("live_shadow_smc_decision_min_log_only", True)),
        }
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "live_shadow_smc_decision.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({k: _json_safe_copy(v) for k, v in row.items()}, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[LIVE SHADOW SMC DECISION] observe-only log failed: {exc}")
    return None


def _paper_structural_score_modifier_log(signal, ctx, action, modifier=0.0, effective_score=None,
                                         log_only=True, reasons=None, structural_context=None,
                                         decision_impact="UNKNOWN", original_score=None,
                                         entry_type=None, extra=None):
    try:
        structural_context = structural_context if isinstance(structural_context, dict) else {}
        if original_score is None:
            original_score = _safe_float(
                signal.get("score_v2_original"),
                _safe_float(signal.get("score"), _safe_float(structural_context.get("score_v2_current"))),
            )
        row = {
            "timestamp": format_vn_time(time.time()),
            "symbol": signal.get("symbol"),
            "side": signal.get("side"),
            "entry_type": entry_type or signal.get("entry_type"),
            "original_score_v2": original_score,
            "modifier": modifier,
            "effective_score_v2": effective_score,
            "log_only": bool(log_only),
            "action": action,
            "structural_decision_shadow": _paper_structural_context_value(
                signal, structural_context, "structural_decision_shadow"
            ),
            "bos_quality": _paper_structural_context_value(signal, structural_context, "bos_quality"),
            "choch_quality": _paper_structural_context_value(signal, structural_context, "choch_quality"),
            "poi_location_quality": _paper_structural_context_value(
                signal, structural_context, "poi_location_quality"
            ),
            "volume_confirmation": _paper_structural_context_value(
                signal, structural_context, "volume_confirmation"
            ),
            "trade_location_quality": _paper_structural_context_value(
                signal, structural_context, "trade_location_quality"
            ),
            "original_reason": signal.get("original_reason", signal.get("reason")),
            "research_risk_tier": _paper_structural_context_value(
                signal, structural_context, "research_risk_tier"
            ),
            "research_dedup_key": signal.get("research_dedup_key", signal.get("dedup_key")),
            "modifier_reasons": list(reasons or []),
            "decision_impact": decision_impact,
        }
        if isinstance(extra, dict):
            row.update({str(k): _json_safe_copy(v) for k, v in extra.items()})
        row = {k: _json_safe_copy(v) for k, v in row.items() if v not in (None, "")}
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "paper_structural_score_modifier.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER STRUCTURAL MODIFIER] log failed: {exc}")


def _log_paper_smc_research_structural_modifier(candidate, ctx):
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return None
    if not config.get("paper_enable_structural_score_modifier", False):
        return None
    apply_to = {
        str(item or "").upper()
        for item in config.get("paper_structural_score_modifier_apply_to", ["CONFIRM"])
    }
    if "CONFIRM_SMC_RESEARCH" not in apply_to:
        return None

    structural_context = _paper_structural_context(candidate)
    log_only = bool(config.get("paper_structural_score_modifier_log_only", True))
    try:
        modifier, reasons, structural_context = _paper_structural_score_modifier(candidate)
    except Exception as exc:
        modifier = 0.0
        reasons = [f"modifier_error:{type(exc).__name__}"]
    original_score = _safe_float(structural_context.get("score_v2_structural_shadow"))
    effective_score = round(original_score + modifier, 4) if original_score is not None else None
    _paper_boundary_guard_emit_candidate_seen(
        candidate,
        ctx,
        original_score,
        effective_score,
        modifier,
        reasons,
    )
    boundary_guard_info = _paper_smc_research_boundary_guard_info(
        candidate,
        ctx,
        original_score,
        effective_score,
        modifier,
        reasons,
    )
    research_dedup_key = candidate.get("dedup_key")
    source_slot = _first_nonblank(
        candidate.get("source_timestamp"),
        candidate.get("source_row_time"),
        candidate.get("timestamp"),
        candidate.get("signal_created_ts"),
        candidate.get("collector_ts"),
    )
    log_dedup_key = (
        "LOG_ONLY_RESEARCH" if log_only else "APPLIED_RESEARCH",
        str(research_dedup_key or ""),
        str(source_slot or ""),
        str(candidate.get("symbol") or ""),
        str(candidate.get("side") or ""),
        str(candidate.get("entry") or ""),
        str(candidate.get("sl") or ""),
        str(candidate.get("tp") or ""),
        str(candidate.get("reason") or ""),
        original_score,
        round(modifier, 6),
        tuple(str(reason) for reason in reasons),
        str(_paper_structural_context_value(
            candidate, structural_context, "structural_decision_shadow"
        ) or ""),
        str(_paper_structural_context_value(candidate, structural_context, "bos_quality") or ""),
        str(_paper_structural_context_value(
            candidate, structural_context, "volume_confirmation"
        ) or ""),
    )
    if log_dedup_key in _paper_structural_modifier_research_logged:
        return {
            "original_score": original_score,
            "effective_score": effective_score,
            "modifier": modifier,
            "modifier_reasons": reasons,
            **boundary_guard_info,
        }
    _paper_structural_modifier_research_logged.add(log_dedup_key)
    structural_decision = str(structural_context.get("structural_decision_shadow") or "").upper()
    research_risk_tier = (
        structural_context.get("research_risk_tier")
        or (
            "UNKNOWN_STRUCTURAL_DECISION_RESEARCH"
            if structural_decision == "UNKNOWN"
            else "STRUCTURAL_DECISION_RESEARCH"
        )
    )
    _paper_structural_score_modifier_log(
        candidate,
        ctx,
        "LOG_ONLY_RESEARCH" if log_only else "APPLIED_RESEARCH",
        modifier=modifier,
        effective_score=effective_score,
        log_only=log_only,
        reasons=reasons,
        structural_context=structural_context,
        decision_impact="SCORE_CHANGED" if modifier and not log_only else (
            "WOULD_CHANGE_SCORE" if modifier else "NO_CHANGE"
        ),
        original_score=original_score,
        entry_type="CONFIRM_SMC_RESEARCH",
        extra={
            "trade_location_quality": _paper_structural_context_value(
                candidate, structural_context, "trade_location_quality"
            ),
            "research_risk_tier": research_risk_tier,
            "original_reason": candidate.get("reason"),
            "research_dedup_key": research_dedup_key,
            **boundary_guard_info,
        },
    )
    return {
        "original_score": original_score,
        "effective_score": effective_score,
        "modifier": modifier,
        "modifier_reasons": reasons,
        **boundary_guard_info,
    }


def _apply_paper_structural_score_modifier(signals, ctx):
    if not config.get("paper_enable_structural_score_modifier", False):
        return signals

    log_only = bool(config.get("paper_structural_score_modifier_log_only", True))
    apply_to = {
        str(item or "").upper()
        for item in config.get("paper_structural_score_modifier_apply_to", ["CONFIRM"])
    }
    if not apply_to:
        apply_to = {"CONFIRM"}

    if getattr(ctx, "execution_mode", None) != "paper":
        for signal in signals:
            _paper_structural_score_modifier_log(signal, ctx, "SKIPPED_LIVE_GUARD", log_only=log_only)
        return signals

    adjusted = []
    for signal in signals:
        etype = str(signal.get("entry_type") or "").upper()
        if etype not in apply_to or etype != "CONFIRM":
            _paper_structural_score_modifier_log(signal, ctx, "SKIPPED_NOT_CONFIRM", log_only=log_only)
            adjusted.append(signal)
            continue

        try:
            modifier, reasons, structural_context = _paper_structural_score_modifier(signal)
        except Exception as exc:
            modifier = 0.0
            reasons = [f"modifier_error:{type(exc).__name__}"]
            structural_context = _paper_structural_context(signal)
        original_score = _safe_float(
            signal.get("score_v2_original"),
            _safe_float(signal.get("score"), _safe_float(structural_context.get("score_v2_current"), 0.0)),
        )
        if original_score is None:
            original_score = 0.0
        effective_score = round(original_score + modifier, 4)
        if log_only:
            decision_impact = "WOULD_CHANGE_SCORE" if modifier else "NO_CHANGE"
            _paper_structural_score_modifier_log(
                signal,
                ctx,
                "LOG_ONLY",
                modifier=modifier,
                effective_score=effective_score,
                log_only=True,
                reasons=reasons,
                structural_context=structural_context,
                decision_impact=decision_impact,
            )
            adjusted.append(signal)
            continue

        private_signal = copy.deepcopy(signal)
        private_signal.setdefault("score_v2_original", original_score)
        private_signal.setdefault(
            "score_v2_current",
            _safe_float(structural_context.get("score_v2_current"), original_score),
        )
        private_signal["structural_modifier_applied"] = modifier
        private_signal["paper_score_v2_effective"] = effective_score
        private_signal["score"] = effective_score
        decision_impact = "SCORE_CHANGED" if modifier else "NO_CHANGE"
        _paper_structural_score_modifier_log(
            private_signal,
            ctx,
            "APPLIED",
            modifier=modifier,
            effective_score=effective_score,
            log_only=False,
            reasons=reasons,
            structural_context=structural_context,
            decision_impact=decision_impact,
        )
        adjusted.append(private_signal)
    return adjusted


def _as_list_copy(value):
    if isinstance(value, list):
        return [_json_safe_copy(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe_copy(item) for item in value]
    if isinstance(value, set):
        return [_json_safe_copy(item) for item in sorted(value, key=str)]
    if value in (None, ""):
        return []
    return [_json_safe_copy(value)]


def _first_present(*values):
    for value in values:
        if value not in (None, ""):
            return _json_safe_copy(value)
    return ""


def _opposite_side(side: str) -> str:
    side_u = str(side or "").upper()
    if side_u == "SHORT":
        return "LONG"
    if side_u == "LONG":
        return "SHORT"
    return ""


def _log_failed_continuation_reversal_watch(sig: dict, suppression_reason: str, ctx, scan_state: dict = None) -> None:
    """
    Log-only PAPER research watch for suppressed failed continuation signals.

    This never creates or mutates signals/trades; it only appends a JSONL event.
    """
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return

    try:
        now_ts = time.time()
        source_entry_type = sig.get("entry_type", "")
        suppressed_side = sig.get("side", "")
        watch_side = _opposite_side(suppressed_side)
        phase = _extract_dispatch_phase(sig)
        bos_type = _extract_live_bos_type(sig)
        retest_status = _signal_tag_value(sig, "Retest:")
        key = (
            str(sig.get("symbol", "")),
            str(watch_side),
            str(source_entry_type),
            str(phase),
            str(bos_type),
            str(retest_status),
            str(suppression_reason or "unknown"),
        )

        last_logged = _FAILED_CONTINUATION_WATCH_LAST_LOGGED.get(key, 0)
        if now_ts - last_logged < FAILED_CONTINUATION_REVERSAL_WATCH_TTL_SECS:
            return

        if scan_state is not None:
            if scan_state.get("total", 0) >= FAILED_CONTINUATION_REVERSAL_WATCH_MAX_PER_SCAN:
                return
            symbol_key = str(sig.get("symbol", ""))
            symbol_counts = scan_state.setdefault("symbols", {})
            if symbol_counts.get(symbol_key, 0) >= 1:
                return
            symbol_counts[symbol_key] = symbol_counts.get(symbol_key, 0) + 1
            scan_state["total"] = scan_state.get("total", 0) + 1

        breakdown = sig.get("score_breakdown")
        if not isinstance(breakdown, dict):
            breakdown = sig.get("breakdown")
        if not isinstance(breakdown, dict):
            breakdown = {}
        smc = sig.get("smc")
        if not isinstance(smc, dict):
            smc = breakdown.get("smc")
        if not isinstance(smc, dict):
            smc = {}

        payload = {
            "event_type": "FAILED_CONTINUATION_REVERSAL_WATCH",
            "action": "OBSERVE_ONLY",
            "timestamp": format_vn_time(now_ts),
            "timestamp_unix": now_ts,
            "symbol": sig.get("symbol", ""),
            "watch_side": watch_side,
            "suppressed_side": suppressed_side,
            "source_entry_type": source_entry_type,
            "source_suppress_reason": suppression_reason or "unknown",
            "source_score": sig.get("score", ""),
            "source_entry": sig.get("entry", ""),
            "source_sl": sig.get("sl", ""),
            "source_tp": sig.get("tp", ""),
            "source_rr": sig.get("rr", ""),
            "source_signal_created_ts": sig.get("signal_created_ts", ""),
            "phase": phase,
            "bos_type": bos_type,
            "retest_status": retest_status,
            "candle_strength": _signal_tag_value(sig, "Candle:"),
            "market_state": _first_present(
                _signal_tag_value(sig, "State:"),
                _signal_tag_value(sig, "MKT:"),
            ),
            "exhaustion_cls": _first_present(
                sig.get("exhaustion_cls"),
                sig.get("exhaustion"),
                _signal_tag_value(sig, "Exhaustion:"),
            ),
            "exhaustion_score": sig.get("exhaustion_score", ""),
            "pool_tier": sig.get("_pool_tier", ""),
            "pool_stage": sig.get("_pool_stage", ""),
            "reason": _as_list_copy(sig.get("reason")),
            "smc_zone": _first_present(sig.get("smc_zone"), smc.get("smc_zone")),
            "liquidity_sweep": _first_present(sig.get("liquidity_sweep"), smc.get("liquidity_sweep")),
            "bos_confirmation": _first_present(sig.get("bos_confirmation"), smc.get("bos_confirmation")),
            "smc_bias": _first_present(sig.get("smc_bias"), smc.get("smc_bias")),
            "range_context": _first_present(sig.get("range_context"), smc.get("range_context")),
            "invalid_context": _as_list_copy(
                _first_present(sig.get("invalid_context"), smc.get("invalid_context"))
            ),
            "missing_context": list(FAILED_CONTINUATION_REVERSAL_WATCH_MISSING_CONTEXT),
            "confirmation_required": list(FAILED_CONTINUATION_REVERSAL_WATCH_CONFIRMATION_REQUIRED),
        }

        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "failed_continuation_reversal_watch.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
        _FAILED_CONTINUATION_WATCH_LAST_LOGGED[key] = now_ts
    except Exception as exc:
        print(f"[FAILED CONT WATCH] durable log failed: {exc}")


def live_execution_filter(signal, ctx):
    """
    Apply LIVE execution policy to a single signal.
    Returns (accepted: bool, rejection_reason: str).

    Hard gates enforced for initial live deployment:
      - symbol must be a well-formed USDT futures symbol
      - entry_type must be CONFIRM only
      - exhaustion_cls must be HEALTHY only
      - bos_type must be NEAR only
      - score must be >= LIVE_MIN_SCORE
      - pool_tier must not be "B"
      - execution tier must not be TIER4
      - EARLY_CONT blocked always
      - concurrent live trades must be < max_live_trades from config

    IMPORTANT -- immutability contract:
      This function MUST NOT mutate the shared signal dict.
      bos_type normalization happens on the deepcopy in dispatch_to_executor().
    """
    sym = (signal.get("symbol") or "").upper()
    if not _live_symbol_is_well_formed(sym):
        return False, f"symbol {sym!r} malformed for live execution"

    et = (signal.get("entry_type") or "").upper()
    if et == "EARLY_CONT":
        return False, "EARLY_CONT blocked in live mode"
    if et not in LIVE_ALLOWED_ENTRY_TYPES:
        return False, f"entry_type {et!r} not allowed in live mode (CONFIRM only)"
    if et == "CONFIRM" and not config.get("live_confirm_enabled", False):
        return False, "live_confirm_enabled=False — CONFIRM live disabled"

    if (
        getattr(ctx, "execution_mode", None) == "live"
        and config.get("live_filter_confirm_pre_break_low_near", True)
        and _is_confirm_pre_break_low_near(signal)
    ):
        return False, "live_confirm_pre_break_low_near_guard"

    ex = (signal.get("exhaustion_cls") or "").upper()
    if not ex:
        return False, "missing exhaustion_cls -- rejected in live mode"
    if ex not in LIVE_ALLOWED_EXHAUSTION:
        return False, f"exhaustion {ex!r} not allowed in live mode (HEALTHY only)"

    # FIX: read-only extraction -- do NOT mutate signal["bos_type"] here.
    # Normalization is deferred to dispatch_to_executor() on the deepcopy snapshot.
    bos_t = _extract_live_bos_type(signal)
    if bos_t not in LIVE_ALLOWED_BOS_TYPES:
        return False, f"bos_type {bos_t!r} not allowed in live mode (NEAR only, empty=rejected)"
    # REMOVED: signal["bos_type"] = bos_t  <- was mutating shared signal object

    score = signal.get("score", 0)
    if score < LIVE_MIN_SCORE:
        return False, f"score={round(score, 1)} < live threshold={LIVE_MIN_SCORE}"

    pool_tier = signal.get("_pool_tier", "")
    if pool_tier == LIVE_EXCLUDED_POOL_TIER:
        return False, f"pool_tier={pool_tier!r} excluded from live (Tier A only)"

    if _live_symbol_is_tier4(sym):
        # TIER4 extra gate: score must clear LIVE_TIER4_MIN_SCORE (9.5).
        # Lowered from 10.0 -> 9.5 to reduce filter-starvation on liquid TIER4 symbols
        # while preserving HEALTHY+NEAR hard requirements.  The general LIVE_MIN_SCORE=9
        # gate already runs above; this is an *additional* TIER4-specific margin.
        if score < LIVE_TIER4_MIN_SCORE:
            return False, f"execution_tier='TIER4' score={round(score,1)} < {LIVE_TIER4_MIN_SCORE} -- rejected"
        if ex != "HEALTHY":
            return False, f"execution_tier='TIER4' exhaustion={ex!r} -- rejected (HEALTHY required)"
        if bos_t != "NEAR":
            return False, f"execution_tier='TIER4' bos_type={bos_t!r} -- rejected (NEAR required)"

    max_live = _get_max_live_trades()
    # Count only bot-owned, non-quarantined OPEN trades plus LIVE reservations.
    # Manual positions on the same account never appear in ctx.trades so they cannot
    # consume live slots.  PAPER uses a separate context and is not counted here.
    with ctx.lock:
        open_count, pending, effective = _live_slot_snapshot(ctx)
    if effective >= max_live:
        reason = (
            f"live_max_open_trades_reached max_live_trades={max_live} "
            f"open={open_count} pending={pending} effective={effective}"
        )
        _log_live_slot_decision(signal, ctx, reason)
        return False, reason

    return True, ""


# =====================================================================
# LIVE REJECTION AUDIT LOG
# =====================================================================

def _log_live_rejection(symbol: str, side: str, score: float, reason: str) -> None:
    """
    Append one row to logs/live_rejection_log.csv for every LIVE filter rejection.

    This log is the primary tool for auditing filter starvation.  Columns:
      timestamp   -- wall-clock (VN time) when the signal was rejected
      symbol      -- signal symbol
      side        -- LONG / SHORT
      score       -- signal score at rejection time
      reason      -- the rejection reason string from live_execution_filter()

    The reason string encodes the filter gate that fired, e.g.:
      "entry_type 'EARLY_V2' not allowed in live mode (CONFIRM only)"
      "exhaustion 'EXTENDED' not allowed in live mode (HEALTHY only)"
      "bos_type 'STRONG' not allowed in live mode (NEAR only, empty=rejected)"
      "score=8.9 < live threshold=9"
      "execution_tier='TIER4' score=9.2 < 9.5 -- rejected"
      "max_live_trades=3 reached (open=3)"
      "STALE SIGNAL ..."
      "geometry compressed ..."

    This lets you run offline:
      import pandas as pd
      df = pd.read_csv('logs/live_rejection_log.csv')
      df['reason_category'] = df['reason'].str.split('=').str[0]
      print(df.groupby('reason_category').size().sort_values(ascending=False))

    to identify which filter gates are causing the most rejections.
    """
    import os
    _log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(_log_dir, exist_ok=True)
    _file = os.path.join(_log_dir, "live_rejection_log.csv")
    _is_new = not os.path.exists(_file)
    _fields = ["timestamp", "symbol", "side", "score", "reason"]
    _row = {
        "timestamp": format_vn_time(time.time()),
        "symbol":    symbol,
        "side":      side,
        "score":     round(float(score), 2),
        "reason":    reason,
    }
    try:
        with open(_file, "a", newline="", encoding="utf-8") as _f:
            _w = _csv_mod.DictWriter(_f, fieldnames=_fields)
            if _is_new:
                _w.writeheader()
            _w.writerow(_row)
    except Exception:
        pass


# =====================================================================
# PER-EXECUTOR DISPATCH
# =====================================================================

def _log_market_regime(ratio_pct, regime, extended_count, total):
    import csv as _csv_mod
    from state_manager import log_path
    file = log_path("market_regime.csv")
    is_new = not os.path.exists(file)
    row = {
        "timestamp":                   time.strftime("%Y-%m-%d %H:%M:%S"),
        "market_exhaustion_ratio_pct": ratio_pct,
        "market_regime":               regime,
        "extended_count":              extended_count,
        "total_candidates":            total,
    }
    try:
        with open(file, "a", newline="", encoding="utf-8") as f:
            w = _csv_mod.DictWriter(f, fieldnames=row.keys())
            if is_new:
                w.writeheader()
            w.writerow(row)
    except Exception:
        pass


def _paper_smc_research_log(candidate, action, suppress_reason="", trade=None, extra=None):
    try:
        structural_context = _safe_dict(candidate.get("structural_context"))
        payload = {
            "event_type": "PAPER_SMC_RESEARCH_ENTRY",
            "timestamp": format_vn_time(time.time()),
            "timestamp_unix": time.time(),
            "action": action,
            "dedup_key": candidate.get("dedup_key", ""),
            "symbol": candidate.get("symbol", ""),
            "side": candidate.get("side", ""),
            "entry_type": candidate.get("entry_type", ""),
            "entry": candidate.get("entry"),
            "sl": candidate.get("sl"),
            "tp": candidate.get("tp"),
            "rr": candidate.get("rr"),
            "reason": candidate.get("reason", ""),
            "geometry_status": candidate.get("geometry_status", ""),
            "outcome_trackable": bool(candidate.get("outcome_trackable")),
            "source_timestamp": candidate.get("source_timestamp"),
            "source_row_time": candidate.get("source_row_time", candidate.get("timestamp")),
            "signal_created_ts": candidate.get("signal_created_ts"),
            "collector_ts": candidate.get("collector_ts"),
            "score_v2_current": structural_context.get("score_v2_current"),
            "score_v2_structural_shadow": structural_context.get("score_v2_structural_shadow"),
            "structural_decision_shadow": structural_context.get("structural_decision_shadow"),
            "unknown_structural_decision_allowed": bool(
                structural_context.get("unknown_structural_decision_allowed")
            ),
            "research_risk_tier": structural_context.get("research_risk_tier"),
            "original_structural_decision_shadow": structural_context.get(
                "original_structural_decision_shadow"
            ),
            "score_delta_direction": structural_context.get("score_delta_direction"),
            "bos_quality": structural_context.get("bos_quality"),
            "choch_quality": structural_context.get("choch_quality"),
            "poi_location_quality": structural_context.get("poi_location_quality"),
            "volume_confirmation": structural_context.get("volume_confirmation"),
        }
        if suppress_reason:
            payload["suppress_reason"] = suppress_reason
        elif action == "SUPPRESS":
            payload["suppress_reason"] = "unknown"
        if isinstance(trade, dict):
            payload["opened_trade_id"] = trade.get("id")
            payload["opened_entry_type"] = trade.get("entry_type")
            payload["unknown_structural_decision_allowed"] = bool(
                trade.get("unknown_structural_decision_allowed")
            )
            payload["research_risk_tier"] = trade.get("research_risk_tier")
            payload["original_structural_decision_shadow"] = trade.get(
                "original_structural_decision_shadow"
            )
        if isinstance(extra, dict):
            payload.update({str(k): _json_safe_copy(v) for k, v in extra.items()})
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "paper_smc_research_entries.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH] durable log failed: {exc}")


# ---------------------------------------------------------------------------
# RESEARCH_ENTRY_CONTEXT_SNAPSHOT (paper-only, log-only observability)
#
# Pure side-effect logging. Snapshots already-computed raw entry-context
# fields at the moment CONFIRM_SMC_RESEARCH emits an OPEN decision. Qualified
# callers may attach already-computed shadow labels and would_block context.
# The builder return value is consumed only by logging, never by a decision path.
# ---------------------------------------------------------------------------
_RESEARCH_ENTRY_CONTEXT_SPEC = (
    # field_name, kind, source-key aliases (searched source -> structural_context -> smc)
    ("smc_bias", "text", ("smc_bias", "router_smc_bias")),
    ("trend_direction", "text", ("trend_direction", "router_trend_direction")),
    ("dow_trend_context", "text", ("dow_trend_context",)),
    ("trend_strength", "num", ("trend_strength", "router_trend_strength")),
    ("smc_zone", "text", ("smc_zone", "router_smc_zone")),
    ("premium_discount", "text", ("premium_discount",)),
    ("entry_poi_alignment", "text", ("entry_poi_alignment",)),
    ("poi_location_quality", "text", ("poi_location_quality",)),
    ("poi_type", "text", ("poi_type",)),
    ("trade_location_quality", "text", ("trade_location_quality",)),
    ("market_state", "text", ("market_state", "router_market_state", "mkt_state")),
    ("market_regime", "text", ("market_regime", "market_regime_at_entry", "regime")),
    ("router_regime", "text", ("router_regime",)),
    ("router_regime_confidence", "num", ("router_regime_confidence",)),
    ("range_context", "text", ("range_context", "router_range_context")),
    ("liquidity_context", "text", ("liquidity_context",)),
    ("phase", "text", ("phase", "router_phase")),
    ("dow_phase", "text", ("dow_phase",)),
    ("displacement_quality", "text", ("displacement_quality",)),
    ("impulse_quality", "text", ("impulse_quality",)),
    ("exhaustion_state", "text", ("exhaustion_state", "exhaustion", "exhaustion_cls", "router_exhaustion")),
    ("entry", "num", ("entry", "entry_real")),
    ("sl", "num", ("sl", "sl_init", "sl_real")),
    ("sl_distance_pct", "num", ("sl_distance_pct",)),
    ("planned_rr", "num", ("planned_rr", "rr")),
    ("bos_quality", "text", ("bos_quality",)),
    ("volume_confirmation", "text", ("volume_confirmation",)),
    ("context_source", "text", ("context_source", "regime_context_source", "router_regime_source")),
    ("signal_created_ts", "raw", ("signal_created_ts",)),
    ("decision_ts", "raw", ("decision_ts",)),
)


def _research_entry_context_resolve(source, structural_context, smc, keys):
    for container in (source, structural_context, smc):
        for key in keys:
            value = container.get(key)
            if value not in (None, ""):
                return value
    return None


def _research_entry_context_text_resolved(value):
    if value is None:
        return False
    return str(value).strip().upper() not in ("", "UNKNOWN", "DEFAULT")


def _research_entry_context(source, decision_ts=None):
    source = source if isinstance(source, dict) else {}
    structural_context = _paper_structural_context(source)
    smc = _signal_smc_context(source)
    entry_context = {}
    for field, kind, keys in _RESEARCH_ENTRY_CONTEXT_SPEC:
        if field == "decision_ts":
            value = decision_ts
            resolved = value not in (None, "")
        else:
            raw = _research_entry_context_resolve(source, structural_context, smc, keys)
            if kind == "num":
                value = _safe_float(raw)
                resolved = value is not None
            elif kind == "raw":
                value = raw
                resolved = raw not in (None, "")
            else:
                value = raw
                resolved = _research_entry_context_text_resolved(raw)
        entry_context[field] = _json_safe_copy(value)
        entry_context[f"{field}_resolved"] = bool(resolved)
    return entry_context


def _paper_smc_research_entry_context_write(row):
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "paper_smc_research_entry_context.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH ENTRY CONTEXT] snapshot log failed: {exc}")


def _paper_smc_research_entry_context_snapshot(
    candidate,
    opened,
    decision_ts=None,
    qualified_fields=None,
):
    try:
        candidate = candidate if isinstance(candidate, dict) else {}
        opened = opened if isinstance(opened, dict) else {}
        if decision_ts is None:
            decision_ts = time.time()
        entry_context = _research_entry_context(candidate, decision_ts=decision_ts)
        btc_context = _btc_mtf_context_for_signal(
            {**candidate, **opened, "decision_ts": decision_ts},
            side=_first_nonblank(opened.get("side"), candidate.get("side")),
            now_ts=decision_ts,
        )
        entry_context.update({key: _json_safe_copy(value) for key, value in btc_context.items()})
        entry_fallback_shadow = None
        if isinstance(qualified_fields, dict):
            entry_fallback_shadow = _paper_smc_research_entry_fallback_shadow_safe(
                candidate,
                fields=qualified_fields,
                now_ts=decision_ts,
            )
            entry_context.update({
                "v2_subgroup": _json_safe_copy(qualified_fields.get("v2_subgroup")),
                "v2_subgroup_match": bool(qualified_fields.get("v2_subgroup_match")),
                "v2_reason": _json_safe_copy(qualified_fields.get("v2_reason")),
                "v2b_label": _json_safe_copy(qualified_fields.get("v2b_label")),
                "v2b_class": _json_safe_copy(qualified_fields.get("v2b_class")),
                "v2b_match": bool(qualified_fields.get("v2b_match")),
                "v2b_reason": _json_safe_copy(qualified_fields.get("v2b_reason")),
                "v2b_market_bias": _json_safe_copy(qualified_fields.get("v2b_market_bias")),
                "v2b_direction_alignment": _json_safe_copy(
                    qualified_fields.get("v2b_direction_alignment")
                ),
                "v2b_counter_bias_candidate": bool(
                    qualified_fields.get("v2b_counter_bias_candidate")
                ),
                "v2b_counter_bias_reasons": _json_safe_copy(
                    qualified_fields.get("v2b_counter_bias_reasons") or []
                ),
                "market_regime": _json_safe_copy(qualified_fields.get("market_regime")),
                "side": _json_safe_copy(candidate.get("side")),
                "confirm_smc_entry_location_would_block": _json_safe_copy(
                    qualified_fields.get("confirm_smc_entry_location_would_block")
                ),
                "planned_rr": _json_safe_copy(qualified_fields.get("planned_rr")),
                "bos_quality": _json_safe_copy(qualified_fields.get("bos_quality")),
                "volume_confirmation": _json_safe_copy(
                    qualified_fields.get("volume_confirmation")
                ),
            })
            entry_context.update(_PAPER_SMC_RESEARCH_EXTENSION_METADATA)
            entry_context["research_is_post_50"] = bool(
                opened.get("research_is_post_50")
            )
            entry_context.update({
                key: _json_safe_copy(value)
                for key, value in entry_fallback_shadow.items()
            })
        row = {
            "timestamp": format_vn_time(decision_ts),
            "event_type": "RESEARCH_ENTRY_CONTEXT_SNAPSHOT",
            "dedup_key": candidate.get("dedup_key", ""),
            "research_dedup_key": candidate.get("dedup_key", ""),
            "research_join_key": candidate.get("dedup_key", ""),
            "opened_trade_id": opened.get("id"),
            "symbol": candidate.get("symbol", ""),
            "side": candidate.get("side", ""),
            "entry_type": opened.get("entry_type") or candidate.get("entry_type") or "CONFIRM_SMC_RESEARCH",
            "strategy_family": opened.get("strategy_family") or "confirm_smc_research",
            "signal_created_ts": _first_nonblank(
                opened.get("signal_created_ts"), candidate.get("signal_created_ts")
            ),
            "decision_ts": decision_ts,
            "source_timestamp": candidate.get("source_timestamp"),
            "entry_context": entry_context,
        }
        row.update({key: _json_safe_copy(value) for key, value in btc_context.items()})
        if isinstance(qualified_fields, dict):
            row.update(_PAPER_SMC_RESEARCH_EXTENSION_METADATA)
            row["research_is_post_50"] = bool(opened.get("research_is_post_50"))
            row.update({
                key: _json_safe_copy(value)
                for key, value in entry_fallback_shadow.items()
            })
        _paper_smc_research_entry_context_write(row)
        if (
            isinstance(qualified_fields, dict)
            and bool(opened.get("paper_smc_research_qualified"))
            and opened.get("research_epoch") == "v1_extend_200"
        ):
            _paper_smc_research_entry_fallback_shadow_snapshot(
                candidate,
                opened,
                qualified_fields,
                entry_fallback_shadow,
            )
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH ENTRY CONTEXT] snapshot failed: {exc}")

def _paper_confirm_entry_context_write(row):
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "paper_confirm_entry_context.jsonl")
        with open(file_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER CONFIRM ENTRY CONTEXT] snapshot log failed: {exc}")


def _paper_confirm_entry_acceptance_shadow_write(row):
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        path = os.path.join(log_dir, "paper_confirm_entry_acceptance_shadow.jsonl")
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER CONFIRM ENTRY ACCEPTANCE SHADOW] snapshot log failed: {exc}")


def _paper_confirm_entry_acceptance_shadow_snapshot(opened):
    """Append a post-open, log-only acceptance classification for PAPER CONFIRM."""
    try:
        opened = opened if isinstance(opened, dict) else {}
        if str(opened.get("entry_type") or "").upper() != "CONFIRM":
            return

        structural = _paper_structural_context(opened)
        smc = _signal_smc_context(opened)
        context = _safe_dict(opened.get("confirm_entry_acceptance_context"))

        def value(*keys):
            for source in (context, opened, structural, smc):
                for key in keys:
                    item = source.get(key)
                    if item not in (None, ""):
                        return item
            return None

        side = str(opened.get("side") or "").strip().upper()
        entry_price = _safe_float(_first_nonblank(opened.get("entry_real"), opened.get("entry")))
        stop_loss = _safe_float(_first_nonblank(opened.get("sl_init"), opened.get("sl")))
        take_profit = _safe_float(opened.get("tp"))
        r_unit = (
            abs(entry_price - stop_loss)
            if entry_price is not None and stop_loss is not None
            else None
        )
        candle_open = _safe_float(value("candle_open", "open"))
        candle_high = _safe_float(value("candle_high", "high"))
        candle_low = _safe_float(value("candle_low", "low"))
        candle_close = _safe_float(value("candle_close", "close"))
        atr = _safe_float(value("atr", "atr_m15"))
        break_level = _safe_float(value("break_level", "bos_level", "bos_price"))
        pre_break_level = _safe_float(value("pre_break_level"))
        if pre_break_level is None:
            pre_break_level = break_level

        candle_body = candle_range = candle_body_ratio = candle_close_position = None
        close_beyond_level = break_buffer = wick_rejection = None
        if None not in (candle_open, candle_high, candle_low, candle_close):
            candle_body = abs(candle_close - candle_open)
            candle_range = candle_high - candle_low
            if candle_range > 0:
                candle_body_ratio = candle_body / candle_range
                if side == "LONG":
                    candle_close_position = (candle_close - candle_low) / candle_range
                    wick_rejection = (candle_high - max(candle_open, candle_close)) / candle_range
                elif side == "SHORT":
                    candle_close_position = (candle_high - candle_close) / candle_range
                    wick_rejection = (min(candle_open, candle_close) - candle_low) / candle_range
        if candle_close is not None and break_level is not None:
            if side == "LONG":
                close_beyond_level = candle_close > break_level
            elif side == "SHORT":
                close_beyond_level = candle_close < break_level
        if candle_close is not None and break_level is not None and atr is not None and atr > 0:
            break_buffer = (
                (candle_close - break_level) / atr
                if side == "LONG"
                else (break_level - candle_close) / atr if side == "SHORT" else None
            )

        numeric_complete = all(item is not None for item in (
            candle_open, candle_body_ratio, candle_close_position,
            close_beyond_level, break_buffer, wick_rejection,
        ))
        directional_body = (
            candle_close > candle_open if side == "LONG" and numeric_complete
            else candle_close < candle_open if side == "SHORT" and numeric_complete
            else False
        )
        numeric_accepted = bool(
            numeric_complete and directional_body and candle_body_ratio >= 0.6
            and candle_close_position >= 0.6 and close_beyond_level
            and break_buffer > 0 and wick_rejection <= 0.35
        )
        displacement_quality = str(value("displacement_quality") or "").strip().upper()
        if numeric_complete:
            displacement_class = "ACCEPTED" if numeric_accepted else "NOT_ACCEPTED"
        elif displacement_quality in {"STRONG", "MODERATE", "DISPLACEMENT", "CLOSE_THROUGH"}:
            displacement_class = "ACCEPTED_CATEGORICAL_LOW_CONFIDENCE"
        elif displacement_quality:
            displacement_class = "NOT_ACCEPTED_CATEGORICAL_LOW_CONFIDENCE"
        else:
            displacement_class = None

        raw_retest = str(value("retest_quality", "poi_retest_quality") or "").strip().upper()
        retest_map = {
            "VALID_RETEST": "RETEST_HELD", "CONFIRMED": "RETEST_HELD", "HOLD": "RETEST_HELD",
            "FAILED_RETEST": "RETEST_FAILED", "VIOLATED": "RETEST_FAILED",
            "WAITING_RETEST": "RETEST_IN_PROGRESS", "IN_PROGRESS": "RETEST_IN_PROGRESS",
            "NO_RETEST": "BREAK_NO_RETEST", "NONE": "BREAK_NO_RETEST",
        }
        retest_state = retest_map.get(raw_retest, "UNKNOWN")
        if close_beyond_level is False:
            retest_state = "NO_BREAK"

        htf_bias = value("htf_bias", "m15_bias")
        support = _safe_float(value("nearest_htf_support", "m15_swing_low", "range_low"))
        resistance = _safe_float(value("nearest_htf_resistance", "m15_swing_high", "range_high"))
        distance_to_support_r = distance_to_resistance_r = None
        if entry_price is not None and r_unit is not None and r_unit > 0:
            if support is not None:
                distance_to_support_r = abs(entry_price - support) / r_unit
            if resistance is not None:
                distance_to_resistance_r = abs(resistance - entry_price) / r_unit
        opposing_distance = distance_to_resistance_r if side == "LONG" else distance_to_support_r if side == "SHORT" else None
        barrier_direction = "ABOVE" if side == "LONG" else "BELOW" if side == "SHORT" else None
        barrier_between = (
            resistance is not None and entry_price is not None and take_profit is not None
            and entry_price < resistance < take_profit
            if side == "LONG" else
            support is not None and entry_price is not None and take_profit is not None
            and take_profit < support < entry_price
            if side == "SHORT" else None
        )
        planned_rr_value = _safe_float(_first_nonblank(opened.get("planned_rr"), opened.get("rr")))
        room_to_target = (
            min(opposing_distance, planned_rr_value)
            if opposing_distance is not None and planned_rr_value is not None
            else opposing_distance
        )
        htf_blocks = bool(barrier_between)
        htf_supports = (
            str(htf_bias or "").upper() in {side, "BULLISH" if side == "LONG" else "BEARISH"}
            if side in {"LONG", "SHORT"} and htf_bias not in (None, "") else None
        )

        market_regime = value("market_regime", "market_regime_at_entry", "regime")
        regime_text = str(market_regime or "").upper()
        bad_regime = any(token in regime_text for token in ("NO_TRADE", "DEAD", "CHOP", "EXHAUSTION_REVERSAL"))
        phase = value("phase", "router_phase")
        phase_text = str(phase or "").upper()
        bos_quality = value("bos_quality")
        bos_type = value("bos_type", "bos_confirmation")

        reasons = []
        if bad_regime:
            acceptance_class = "SKIP_BAD_REGIME"
            reasons.append(f"bad_regime:{regime_text}")
        elif htf_blocks:
            acceptance_class = "SKIP_HTF_BARRIER_AGAINST_TRADE"
            reasons.append("opposing_m15_barrier_before_target")
        elif numeric_accepted and retest_state == "RETEST_HELD":
            acceptance_class = "ENTER_NOW_ACCEPTED"
            reasons.extend(("numeric_body_acceptance", "retest_held"))
        elif retest_state in {"RETEST_IN_PROGRESS", "BREAK_NO_RETEST"}:
            acceptance_class = "WAIT_FOR_RETEST"
            reasons.append(f"retest_state:{retest_state}")
        elif close_beyond_level is False or "PRE_BREAK" in phase_text:
            acceptance_class = "WAIT_FOR_LONG_BODY_ACCEPTANCE"
            reasons.append("break_not_accepted")
        elif displacement_class and "NOT_ACCEPTED" in displacement_class:
            acceptance_class = "WAIT_FOR_FOLLOWTHROUGH"
            reasons.append("displacement_not_accepted")
        elif numeric_accepted:
            acceptance_class = "ENTER_NOW_ACCEPTED"
            reasons.append("numeric_body_acceptance")
        else:
            acceptance_class = "UNKNOWN_INSUFFICIENT_CONTEXT"
            reasons.append("insufficient_acceptance_context")

        internal_class = (
            "NUMERIC_HIGH_CONFIDENCE" if numeric_complete
            else "CATEGORICAL_LOW_CONFIDENCE" if displacement_class
            else "INSUFFICIENT_CONTEXT"
        )
        fallback_class = (
            "WAIT_FOR_RETEST" if acceptance_class == "WAIT_FOR_RETEST"
            else "WAIT_FOR_FOLLOWTHROUGH" if acceptance_class in {"WAIT_FOR_LONG_BODY_ACCEPTANCE", "WAIT_FOR_FOLLOWTHROUGH"}
            else "NO_FALLBACK_SUGGESTED" if acceptance_class == "ENTER_NOW_ACCEPTED"
            else acceptance_class
        )
        row = {
            "timestamp": format_vn_time(_safe_float(opened.get("time"), time.time())),
            "event_type": "PAPER_CONFIRM_ENTRY_ACCEPTANCE_SHADOW",
            "shadow_version": "v2",
            "post_open_snapshot": True,
            "symbol": opened.get("symbol"), "side": side or None, "entry_type": "CONFIRM",
            "opened_trade_id": opened.get("id"),
            "candidate_id": _first_nonblank(opened.get("candidate_id"), opened.get("signal_id")),
            "dedup_key": _first_nonblank(opened.get("dedup_key"), opened.get("research_dedup_key")),
            "score": _safe_float(opened.get("score")), "phase": phase,
            "bos_type": bos_type, "bos_quality": bos_quality,
            "market_regime": market_regime, "market_state": value("market_state", "router_market_state"),
            "smc_bias": value("smc_bias", "router_smc_bias"),
            "trend_direction": value("trend_direction", "router_trend_direction"),
            "entry_price": entry_price, "stop_loss": stop_loss, "take_profit": take_profit,
            "planned_rr": planned_rr_value,
            "r_unit": r_unit, "pre_break_level": pre_break_level, "break_level": break_level,
            "candle_body": candle_body, "candle_range": candle_range,
            "candle_body_ratio": candle_body_ratio, "candle_close_position": candle_close_position,
            "close_beyond_level": close_beyond_level, "break_buffer": break_buffer,
            "wick_rejection": wick_rejection, "atr": atr,
            "displacement_class": displacement_class,
            "volume_confirmation": value("volume_confirmation"),
            "retest_state": retest_state, "retest_quality": raw_retest or None,
            "htf_timeframe": "M15", "htf_bias": htf_bias,
            "nearest_htf_support": support, "nearest_htf_resistance": resistance,
            "distance_to_support_r": distance_to_support_r,
            "distance_to_resistance_r": distance_to_resistance_r,
            "opposing_barrier_distance_r": opposing_distance,
            "room_to_target_before_barrier_r": room_to_target,
            "htf_barrier_direction": barrier_direction,
            "htf_zone_supports_trade": htf_supports, "htf_zone_blocks_trade": htf_blocks,
            "entry_acceptance_class": acceptance_class,
            "entry_acceptance_internal_class": internal_class,
            "entry_acceptance_reason": reasons[0], "entry_acceptance_reasons": reasons,
            "fallback_class": fallback_class,
        }
        required_context = (
            "side", "score", "phase", "bos_type", "bos_quality", "market_regime", "market_state",
            "smc_bias", "trend_direction", "entry_price", "stop_loss", "take_profit", "planned_rr",
            "r_unit", "pre_break_level", "break_level", "candle_body", "candle_range",
            "candle_body_ratio", "candle_close_position", "close_beyond_level", "break_buffer",
            "wick_rejection", "atr", "displacement_class", "volume_confirmation", "retest_quality",
            "htf_bias", "nearest_htf_support", "nearest_htf_resistance", "distance_to_support_r",
            "distance_to_resistance_r", "opposing_barrier_distance_r",
            "room_to_target_before_barrier_r", "htf_barrier_direction",
            "htf_zone_supports_trade", "htf_zone_blocks_trade",
        )
        row["missing_fields"] = [name for name in required_context if row.get(name) is None]
        _paper_confirm_entry_acceptance_shadow_write(row)
    except Exception as exc:
        print(f"[PAPER CONFIRM ENTRY ACCEPTANCE SHADOW] snapshot failed: {exc}")


def _paper_smc_research_entry_acceptance_shadow_write(row):
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        path = os.path.join(log_dir, "paper_smc_research_entry_acceptance_shadow.jsonl")
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH ENTRY ACCEPTANCE SHADOW] snapshot log failed: {exc}")


def _paper_smc_research_entry_acceptance_shadow_snapshot(opened, qualified_fields=None):
    """Append a post-open, log-only acceptance classification for PAPER CONFIRM_SMC_RESEARCH.

    Mirrors the PAPER CONFIRM entry-acceptance V2 classifier. Strictly log-only: the
    output is never read by predicate, dispatch, open_trade, sizing, risk, locks, exits,
    trailing, management, DD, or Telegram.
    """
    try:
        opened = opened if isinstance(opened, dict) else {}
        if str(opened.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            return

        structural = _paper_structural_context(opened)
        smc = _signal_smc_context(opened)
        context = _safe_dict(opened.get("confirm_entry_acceptance_context"))

        def value(*keys):
            for source in (context, opened, structural, smc):
                for key in keys:
                    item = source.get(key)
                    if item not in (None, ""):
                        return item
            return None

        side = str(opened.get("side") or "").strip().upper()
        entry_price = _safe_float(_first_nonblank(opened.get("entry_real"), opened.get("entry")))
        stop_loss = _safe_float(_first_nonblank(opened.get("sl_init"), opened.get("sl")))
        take_profit = _safe_float(opened.get("tp"))
        r_unit = (
            abs(entry_price - stop_loss)
            if entry_price is not None and stop_loss is not None
            else None
        )
        candle_open = _safe_float(value("candle_open", "open"))
        candle_high = _safe_float(value("candle_high", "high"))
        candle_low = _safe_float(value("candle_low", "low"))
        candle_close = _safe_float(value("candle_close", "close"))
        atr = _safe_float(value("atr", "atr_m15"))
        break_level = _safe_float(value("break_level", "bos_level", "bos_price"))
        pre_break_level = _safe_float(value("pre_break_level"))
        if pre_break_level is None:
            pre_break_level = break_level

        candle_body = candle_range = candle_body_ratio = candle_close_position = None
        close_beyond_level = break_buffer = wick_rejection = None
        if None not in (candle_open, candle_high, candle_low, candle_close):
            candle_body = abs(candle_close - candle_open)
            candle_range = candle_high - candle_low
            if candle_range > 0:
                candle_body_ratio = candle_body / candle_range
                if side == "LONG":
                    candle_close_position = (candle_close - candle_low) / candle_range
                    wick_rejection = (candle_high - max(candle_open, candle_close)) / candle_range
                elif side == "SHORT":
                    candle_close_position = (candle_high - candle_close) / candle_range
                    wick_rejection = (min(candle_open, candle_close) - candle_low) / candle_range
        if candle_close is not None and break_level is not None:
            if side == "LONG":
                close_beyond_level = candle_close > break_level
            elif side == "SHORT":
                close_beyond_level = candle_close < break_level
        if candle_close is not None and break_level is not None and atr is not None and atr > 0:
            break_buffer = (
                (candle_close - break_level) / atr
                if side == "LONG"
                else (break_level - candle_close) / atr if side == "SHORT" else None
            )

        numeric_complete = all(item is not None for item in (
            candle_open, candle_body_ratio, candle_close_position,
            close_beyond_level, break_buffer, wick_rejection,
        ))
        directional_body = (
            candle_close > candle_open if side == "LONG" and numeric_complete
            else candle_close < candle_open if side == "SHORT" and numeric_complete
            else False
        )
        numeric_accepted = bool(
            numeric_complete and directional_body and candle_body_ratio >= 0.6
            and candle_close_position >= 0.6 and close_beyond_level
            and break_buffer > 0 and wick_rejection <= 0.35
        )
        displacement_quality = str(value("displacement_quality") or "").strip().upper()
        if numeric_complete:
            displacement_class = "ACCEPTED" if numeric_accepted else "NOT_ACCEPTED"
        elif displacement_quality in {"STRONG", "MODERATE", "DISPLACEMENT", "CLOSE_THROUGH"}:
            displacement_class = "ACCEPTED_CATEGORICAL_LOW_CONFIDENCE"
        elif displacement_quality:
            displacement_class = "NOT_ACCEPTED_CATEGORICAL_LOW_CONFIDENCE"
        else:
            displacement_class = None

        raw_retest = str(value("retest_quality", "poi_retest_quality") or "").strip().upper()
        retest_map = {
            "VALID_RETEST": "RETEST_HELD", "CONFIRMED": "RETEST_HELD", "HOLD": "RETEST_HELD",
            "FAILED_RETEST": "RETEST_FAILED", "VIOLATED": "RETEST_FAILED",
            "WAITING_RETEST": "RETEST_IN_PROGRESS", "IN_PROGRESS": "RETEST_IN_PROGRESS",
            "NO_RETEST": "BREAK_NO_RETEST", "NONE": "BREAK_NO_RETEST",
        }
        retest_state = retest_map.get(raw_retest, "UNKNOWN")
        if close_beyond_level is False:
            retest_state = "NO_BREAK"

        if close_beyond_level is True:
            break_happened = True
        elif close_beyond_level is False or retest_state == "NO_BREAK":
            break_happened = False
        else:
            break_happened = None
        if retest_state in {"RETEST_HELD", "RETEST_FAILED", "RETEST_IN_PROGRESS"}:
            price_returned_to_level = True
        elif retest_state in {"BREAK_NO_RETEST", "NO_BREAK"}:
            price_returned_to_level = False
        else:
            price_returned_to_level = None
        if retest_state == "UNKNOWN":
            retest_held = None
            retest_failed = None
        else:
            retest_held = retest_state == "RETEST_HELD"
            retest_failed = retest_state == "RETEST_FAILED"

        htf_bias = value("htf_bias", "m15_bias")
        support = _safe_float(value("nearest_htf_support", "m15_swing_low", "range_low"))
        resistance = _safe_float(value("nearest_htf_resistance", "m15_swing_high", "range_high"))
        distance_to_support_r = distance_to_resistance_r = None
        if entry_price is not None and r_unit is not None and r_unit > 0:
            if support is not None:
                distance_to_support_r = abs(entry_price - support) / r_unit
            if resistance is not None:
                distance_to_resistance_r = abs(resistance - entry_price) / r_unit
        opposing_distance = distance_to_resistance_r if side == "LONG" else distance_to_support_r if side == "SHORT" else None
        barrier_direction = "ABOVE" if side == "LONG" else "BELOW" if side == "SHORT" else None
        barrier_between = (
            resistance is not None and entry_price is not None and take_profit is not None
            and entry_price < resistance < take_profit
            if side == "LONG" else
            support is not None and entry_price is not None and take_profit is not None
            and take_profit < support < entry_price
            if side == "SHORT" else None
        )
        planned_rr_value = _safe_float(_first_nonblank(opened.get("planned_rr"), opened.get("rr")))
        room_to_target = (
            min(opposing_distance, planned_rr_value)
            if opposing_distance is not None and planned_rr_value is not None
            else opposing_distance
        )
        htf_blocks = bool(barrier_between)
        htf_supports = (
            str(htf_bias or "").upper() in {side, "BULLISH" if side == "LONG" else "BEARISH"}
            if side in {"LONG", "SHORT"} and htf_bias not in (None, "") else None
        )

        market_regime = value("market_regime", "market_regime_at_entry", "regime")
        regime_text = str(market_regime or "").upper()
        bad_regime = any(token in regime_text for token in ("NO_TRADE", "DEAD", "CHOP", "EXHAUSTION_REVERSAL"))
        phase = value("phase", "router_phase")
        phase_text = str(phase or "").upper()
        bos_quality = value("bos_quality")
        bos_type = value("bos_type", "bos_confirmation")

        reasons = []
        if bad_regime:
            acceptance_class = "SKIP_BAD_REGIME"
            reasons.append(f"bad_regime:{regime_text}")
        elif htf_blocks:
            acceptance_class = "SKIP_HTF_BARRIER_AGAINST_TRADE"
            reasons.append("opposing_m15_barrier_before_target")
        elif numeric_accepted and retest_state == "RETEST_HELD":
            acceptance_class = "ENTER_NOW_ACCEPTED"
            reasons.extend(("numeric_body_acceptance", "retest_held"))
        elif retest_state in {"RETEST_IN_PROGRESS", "BREAK_NO_RETEST"}:
            acceptance_class = "WAIT_FOR_RETEST"
            reasons.append(f"retest_state:{retest_state}")
        elif close_beyond_level is False or "PRE_BREAK" in phase_text:
            acceptance_class = "WAIT_FOR_LONG_BODY_ACCEPTANCE"
            reasons.append("break_not_accepted")
        elif displacement_class and "NOT_ACCEPTED" in displacement_class:
            acceptance_class = "WAIT_FOR_FOLLOWTHROUGH"
            reasons.append("displacement_not_accepted")
        elif numeric_accepted:
            acceptance_class = "ENTER_NOW_ACCEPTED"
            reasons.append("numeric_body_acceptance")
        else:
            acceptance_class = "UNKNOWN_INSUFFICIENT_CONTEXT"
            reasons.append("insufficient_acceptance_context")

        internal_class = (
            "NUMERIC_HIGH_CONFIDENCE" if numeric_complete
            else "CATEGORICAL_LOW_CONFIDENCE" if displacement_class
            else "INSUFFICIENT_CONTEXT"
        )
        fallback_class = (
            "WAIT_FOR_RETEST" if acceptance_class == "WAIT_FOR_RETEST"
            else "WAIT_FOR_FOLLOWTHROUGH" if acceptance_class in {"WAIT_FOR_LONG_BODY_ACCEPTANCE", "WAIT_FOR_FOLLOWTHROUGH"}
            else "NO_FALLBACK_SUGGESTED" if acceptance_class == "ENTER_NOW_ACCEPTED"
            else acceptance_class
        )

        qf = qualified_fields if isinstance(qualified_fields, dict) else {}

        def _meta(*keys):
            for key in keys:
                item = qf.get(key)
                if item not in (None, ""):
                    return item
            for key in keys:
                item = opened.get(key)
                if item not in (None, ""):
                    return item
            return None

        row = {
            "timestamp": format_vn_time(
                _safe_float(_first_nonblank(opened.get("entry_time"), opened.get("time")), time.time())
            ),
            "event_type": "PAPER_SMC_RESEARCH_ENTRY_ACCEPTANCE_SHADOW",
            "shadow_version": "v2",
            "post_open_snapshot": True,
            "symbol": opened.get("symbol"), "side": side or None,
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "strategy_family": opened.get("strategy_family") or "confirm_smc_research",
            "opened_trade_id": opened.get("id"),
            "candidate_id": _first_nonblank(opened.get("candidate_id"), opened.get("signal_id")),
            "dedup_key": _first_nonblank(opened.get("research_dedup_key"), opened.get("dedup_key")),
            "score": _safe_float(opened.get("score")), "phase": phase,
            "bos_type": bos_type, "bos_quality": bos_quality,
            "market_regime": market_regime, "market_state": value("market_state", "router_market_state"),
            "smc_bias": value("smc_bias", "router_smc_bias"),
            "trend_direction": value("trend_direction", "router_trend_direction"),
            "entry_price": entry_price, "stop_loss": stop_loss, "take_profit": take_profit,
            "planned_rr": planned_rr_value,
            "r_unit": r_unit, "pre_break_level": pre_break_level, "break_level": break_level,
            "candle_body": candle_body, "candle_range": candle_range,
            "candle_body_ratio": candle_body_ratio, "candle_close_position": candle_close_position,
            "close_beyond_level": close_beyond_level, "break_buffer": break_buffer,
            "wick_rejection": wick_rejection, "atr": atr,
            "displacement_class": displacement_class,
            "volume_confirmation": value("volume_confirmation"),
            "retest_state": retest_state, "retest_quality": raw_retest or None,
            "break_happened": break_happened,
            "price_returned_to_level": price_returned_to_level,
            "retest_held": retest_held, "retest_failed": retest_failed,
            "htf_timeframe": "M15", "htf_bias": htf_bias,
            "nearest_htf_support": support, "nearest_htf_resistance": resistance,
            "distance_to_support_r": distance_to_support_r,
            "distance_to_resistance_r": distance_to_resistance_r,
            "opposing_barrier_distance_r": opposing_distance,
            "room_to_target_before_barrier_r": room_to_target,
            "htf_barrier_direction": barrier_direction,
            "htf_zone_supports_trade": htf_supports, "htf_zone_blocks_trade": htf_blocks,
            "entry_acceptance_class": acceptance_class,
            "entry_acceptance_internal_class": internal_class,
            "entry_acceptance_reason": reasons[0], "entry_acceptance_reasons": reasons,
            "fallback_class": fallback_class,
            "research_epoch": _meta("research_epoch"),
            "research_cap_target": _meta("research_cap_target"),
            "research_is_post_50": bool(opened.get("research_is_post_50")),
            "v2_subgroup": _meta("v2_subgroup"),
            "v2_subgroup_match": (
                bool(qf.get("v2_subgroup_match")) if "v2_subgroup_match" in qf else None
            ),
            "v2_reason": _meta("v2_reason"),
            "v2b_class": _meta("v2b_class"),
            "v2b_market_bias": _meta("v2b_market_bias"),
            "v2b_direction_alignment": _meta("v2b_direction_alignment"),
        }
        research_max_open_target = _meta("research_max_open_target")
        if research_max_open_target is not None:
            row["research_max_open_target"] = research_max_open_target
        research_concurrency_epoch = _meta("research_concurrency_epoch")
        if research_concurrency_epoch is not None:
            row["research_concurrency_epoch"] = research_concurrency_epoch

        required_context = (
            "side", "score", "phase", "bos_type", "bos_quality", "market_regime", "market_state",
            "smc_bias", "trend_direction", "entry_price", "stop_loss", "take_profit", "planned_rr",
            "r_unit", "pre_break_level", "break_level", "candle_body", "candle_range",
            "candle_body_ratio", "candle_close_position", "close_beyond_level", "break_buffer",
            "wick_rejection", "atr", "displacement_class", "volume_confirmation", "retest_quality",
            "break_happened", "price_returned_to_level", "retest_held", "retest_failed",
            "htf_bias", "nearest_htf_support", "nearest_htf_resistance", "distance_to_support_r",
            "distance_to_resistance_r", "opposing_barrier_distance_r",
            "room_to_target_before_barrier_r", "htf_barrier_direction",
            "htf_zone_supports_trade", "htf_zone_blocks_trade",
        )
        row["missing_fields"] = [name for name in required_context if row.get(name) is None]
        _paper_smc_research_entry_acceptance_shadow_write(row)
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH ENTRY ACCEPTANCE SHADOW] snapshot failed: {exc}")


def _paper_confirm_score_bucket(score):
    score = _safe_float(score)
    if score is None:
        return "UNKNOWN"
    if score < 10:
        return "<10"
    if score < 12:
        return f"{int(score)}.x"
    return "12+"


def _paper_confirm_context_resolved(value):
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().upper() not in ("", "UNKNOWN", "DEFAULT")
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


_CONFIRM_RESCUE_SHADOW_LABEL = "CONFIRM_PRE_BREAK_LOW_SHADOW_GATE"
_CONFIRM_RESCUE_SHADOW_VERSION = "v1"


def _paper_confirm_pre_break_low_shadow(row, opened):
    """Return observational rescue fields; never consumed by execution."""
    row = row if isinstance(row, dict) else {}
    opened = opened if isinstance(opened, dict) else {}
    side = str(row.get("side") or "").strip().upper()
    phase = str(row.get("phase") or "").strip().upper()
    entry_type = str(row.get("entry_type") or "").strip().upper()
    execution_mode = str(opened.get("execution_mode") or "").strip().lower()
    market_regime = str(row.get("market_regime") or "").strip().upper()
    bos_quality = str(row.get("bos_quality") or "").strip().upper()
    bos_type = str(row.get("bos_type") or "").strip().upper()
    location_would_block = row.get("confirm_smc_entry_location_would_block")
    location_primary_reason = str(row.get("entry_location_primary_reason") or "").strip().upper()
    location_risk_bucket = str(row.get("entry_location_risk_bucket") or "").strip().upper()
    barrier_context = row.get("barrier_context")
    barrier_items = barrier_context if isinstance(barrier_context, list) else []
    barrier_text = " ".join(str(value).upper() for value in barrier_items)
    pre_break_context = row.get("pre_break_context")
    retest_quality = str(row.get("retest_quality") or "").strip().upper()
    displacement_quality = str(row.get("displacement_quality") or "").strip().upper()

    missing_fields = []
    for name, value in (
        ("side", side),
        ("phase", phase),
        ("market_regime", market_regime),
        ("bos_quality", bos_quality),
        ("entry_location_would_block", location_would_block),
        ("entry_location_primary_reason", location_primary_reason),
        ("entry_location_risk_bucket", location_risk_bucket),
        ("retest_quality", retest_quality),
        ("displacement_quality", displacement_quality),
    ):
        if value is None or value in ("", "UNKNOWN", "DEFAULT"):
            missing_fields.append(name)
    if not barrier_items:
        missing_fields.append("barrier_context")
    if pre_break_context is None:
        missing_fields.append("pre_break_context")

    reasons = []
    would_block = False
    reason = "BASE_PATTERN_NOT_MATCHED"
    if side in ("", "UNKNOWN") or phase in ("", "UNKNOWN"):
        would_block = None
        reason = "INSUFFICIENT_CONTEXT"
        reasons.append(reason)
    elif entry_type == "CONFIRM" and execution_mode == "paper" and side == "SHORT" and phase == "PRE_BREAK_LOW":
        would_block = True
        reasons.append("SHORT_PRE_BREAK_LOW_BASE_PATTERN")

        if market_regime in {"CHOP_NO_TRADE", "RANGE_MEAN_REVERSION"}:
            reasons.append(f"BAD_REGIME_{market_regime}")
        if location_would_block is True:
            reasons.append("ENTRY_LOCATION_WOULD_BLOCK")
        if location_risk_bucket == "HIGH_RISK":
            reasons.append("ENTRY_LOCATION_HIGH_RISK")
        if location_primary_reason == "BAD_REGIME":
            reasons.append("ENTRY_LOCATION_BAD_REGIME")

        clean_bos = {"STRONG", "CLOSE_THROUGH", "ACCEPTED", "CONFIRMED"}
        if bos_quality and bos_quality not in clean_bos:
            reasons.append(f"NON_CLEAN_BOS_{bos_quality}")
        elif bos_type and bos_type not in clean_bos:
            reasons.append(f"NON_CLEAN_BOS_TYPE_{bos_type}")

        barrier_risk_tokens = (
            "UNBROKEN_LOW",
            "SUPPORT",
            "NO_BREAKDOWN_ACCEPTANCE",
            "NO_ACCEPTANCE",
            "INTO_BARRIER",
            "NEAR_BARRIER",
        )
        matched_barrier_tokens = [
            token for token in barrier_risk_tokens if token in barrier_text
        ]
        if matched_barrier_tokens:
            reasons.extend(f"BARRIER_{token}" for token in matched_barrier_tokens)
        if pre_break_context is True:
            reasons.append("PRE_BREAK_LOW_NO_ACCEPTANCE_YET")

        reason = reasons[1] if len(reasons) > 1 else reasons[0]

    accepted_break = (
        bos_quality in {"STRONG", "CLOSE_THROUGH", "ACCEPTED", "CONFIRMED"}
        or bos_type in {"STRONG", "CLOSE_THROUGH", "ACCEPTED", "CONFIRMED"}
    )
    retest_confirmed = retest_quality in {"VALID", "STRONG", "CONFIRMED", "HOLD"}
    weak_followthrough = (
        bos_quality in {"WEAK", "CONFIRM", "TRAP", "NO_FOLLOWTHROUGH"}
        or displacement_quality in {"WEAK", "NONE", "NO_FOLLOWTHROUGH"}
    )

    if would_block is None:
        fallback_class = "UNKNOWN_INSUFFICIENT_CONTEXT"
        fallback_reason = "side_or_phase_missing"
    elif not would_block:
        fallback_class = "NO_FALLBACK_SUGGESTED"
        fallback_reason = "shadow_base_pattern_not_matched"
    elif market_regime in {"CHOP_NO_TRADE", "RANGE_MEAN_REVERSION"} and weak_followthrough:
        fallback_class = "SKIP_NO_FALLBACK"
        fallback_reason = "bad_regime_without_followthrough"
    elif not accepted_break:
        fallback_class = "WAIT_FOR_BREAK_ACCEPTANCE"
        fallback_reason = "pre_break_low_without_accepted_break"
    elif not retest_confirmed:
        fallback_class = "WAIT_FOR_RETEST"
        fallback_reason = "accepted_break_without_confirmed_retest"
    elif weak_followthrough:
        fallback_class = "WAIT_FOR_FOLLOWTHROUGH"
        fallback_reason = "weak_bos_or_displacement_followthrough"
    else:
        fallback_class = "NO_FALLBACK_SUGGESTED"
        fallback_reason = "accepted_break_and_retest_context_present"

    return {
        "confirm_rescue_shadow_label": _CONFIRM_RESCUE_SHADOW_LABEL,
        "confirm_rescue_shadow_version": _CONFIRM_RESCUE_SHADOW_VERSION,
        "confirm_rescue_shadow_would_block": would_block,
        "confirm_rescue_shadow_reason": reason,
        "confirm_rescue_shadow_reasons": reasons,
        "confirm_rescue_shadow_components": {
            "execution_mode": execution_mode or None,
            "side": side or None,
            "phase": phase or None,
            "market_regime": market_regime or None,
            "bos_type": bos_type or None,
            "bos_quality": bos_quality or None,
            "entry_location_would_block": location_would_block,
            "entry_location_primary_reason": location_primary_reason or None,
            "entry_location_risk_bucket": location_risk_bucket or None,
            "barrier_context": barrier_items or None,
            "pre_break_context": pre_break_context,
            "retest_quality": retest_quality or None,
            "displacement_quality": displacement_quality or None,
            "accepted_break_detected": accepted_break,
            "retest_confirmed": retest_confirmed,
            "weak_followthrough": weak_followthrough,
        },
        "confirm_rescue_shadow_fallback_class": fallback_class,
        "confirm_rescue_shadow_fallback_reason": fallback_reason,
        "confirm_rescue_shadow_missing_fields": missing_fields,
    }


def _paper_confirm_pre_break_low_shadow_safe(row, opened):
    try:
        return _paper_confirm_pre_break_low_shadow(row, opened)
    except Exception as exc:
        print(f"[PAPER CONFIRM PRE BREAK LOW SHADOW] classify failed: {exc}")
        return {
            "confirm_rescue_shadow_label": _CONFIRM_RESCUE_SHADOW_LABEL,
            "confirm_rescue_shadow_version": _CONFIRM_RESCUE_SHADOW_VERSION,
            "confirm_rescue_shadow_would_block": None,
            "confirm_rescue_shadow_reason": "INSUFFICIENT_CONTEXT",
            "confirm_rescue_shadow_reasons": ["SHADOW_INSTRUMENTATION_ERROR"],
            "confirm_rescue_shadow_components": None,
            "confirm_rescue_shadow_fallback_class": "UNKNOWN_INSUFFICIENT_CONTEXT",
            "confirm_rescue_shadow_fallback_reason": "shadow_instrumentation_error",
            "confirm_rescue_shadow_missing_fields": ["shadow_instrumentation_context"],
        }


def _paper_confirm_pre_break_low_shadow_write(row):
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        path = os.path.join(log_dir, "paper_confirm_pre_break_low_shadow.jsonl")
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER CONFIRM PRE BREAK LOW SHADOW] log failed: {exc}")


def _paper_confirm_pre_break_low_shadow_snapshot(context_row):
    try:
        row = context_row if isinstance(context_row, dict) else {}
        compact = {
            "timestamp": row.get("timestamp"),
            "event_type": "PAPER_CONFIRM_PRE_BREAK_LOW_SHADOW",
            "symbol": row.get("symbol"),
            "side": row.get("side"),
            "opened_trade_id": row.get("opened_trade_id"),
            "entry_type": row.get("entry_type"),
            "score": row.get("score"),
            "score_bucket": row.get("score_bucket"),
            "phase": row.get("phase"),
            "bos_type": row.get("bos_type"),
            "bos_quality": row.get("bos_quality"),
            "market_state": row.get("market_state"),
            "market_regime": row.get("market_regime"),
            "smc_bias": row.get("smc_bias"),
            "trend_direction": row.get("trend_direction"),
            "entry_location_would_block": row.get("confirm_smc_entry_location_would_block"),
            "entry_location_primary_reason": row.get("entry_location_primary_reason"),
            "entry_location_risk_bucket": row.get("entry_location_risk_bucket"),
            "barrier_context": _json_safe_copy(row.get("barrier_context")),
            "pre_break_context": row.get("pre_break_context"),
            "planned_rr": row.get("planned_rr"),
            "entry_price": row.get("entry_price"),
            "stop_loss": row.get("stop_loss"),
            "take_profit": row.get("take_profit"),
        }
        for key in (
            "confirm_rescue_shadow_label",
            "confirm_rescue_shadow_version",
            "confirm_rescue_shadow_would_block",
            "confirm_rescue_shadow_reason",
            "confirm_rescue_shadow_reasons",
            "confirm_rescue_shadow_components",
            "confirm_rescue_shadow_fallback_class",
            "confirm_rescue_shadow_fallback_reason",
            "confirm_rescue_shadow_missing_fields",
        ):
            compact[key] = _json_safe_copy(row.get(key))
        _paper_confirm_pre_break_low_shadow_write(compact)
    except Exception as exc:
        print(f"[PAPER CONFIRM PRE BREAK LOW SHADOW] snapshot failed: {exc}")


def _paper_confirm_entry_context_snapshot(opened):
    """Write post-open PAPER CONFIRM context; never consumed by execution."""
    try:
        opened = opened if isinstance(opened, dict) else {}
        if str(opened.get("entry_type") or "").upper() != "CONFIRM":
            return

        open_ts = _safe_float(opened.get("time"), time.time())
        entry_price = _safe_float(_first_nonblank(opened.get("entry_real"), opened.get("entry")))
        stop_loss = _safe_float(_first_nonblank(opened.get("sl_init"), opened.get("sl")))
        take_profit = _safe_float(opened.get("tp"))
        sl_distance = (
            abs(entry_price - stop_loss)
            if entry_price is not None and stop_loss is not None
            else None
        )
        tp_distance = (
            abs(take_profit - entry_price)
            if entry_price is not None and take_profit is not None
            else None
        )
        planned_rr = _safe_float(_first_nonblank(opened.get("planned_rr"), opened.get("rr")))

        entry_context = _research_entry_context(opened, decision_ts=open_ts)
        btc_context = _btc_mtf_context_for_signal(opened, now_ts=open_ts)
        entry_context.update({key: _json_safe_copy(value) for key, value in btc_context.items()})
        qualified_fields = _paper_smc_research_qualified_fields(opened)
        structural_context = _paper_structural_context(opened)
        smc = _signal_smc_context(opened)
        reason = _json_safe_copy(opened.get("reason") or [])

        phase = _first_nonblank(
            opened.get("phase"),
            qualified_fields.get("phase"),
            entry_context.get("phase"),
            _signal_tag_value(opened, "PHASE:"),
        )
        market_state = _first_nonblank(
            opened.get("market_state"),
            qualified_fields.get("market_state"),
            _signal_tag_value(opened, "STATE:"),
        )
        bos_type = _first_nonblank(
            opened.get("bos_type"),
            opened.get("bos_confirmation"),
            _signal_tag_value(opened, "BOS:"),
        )
        bos_quality = _first_nonblank(
            qualified_fields.get("bos_quality"),
            entry_context.get("bos_quality"),
            bos_type,
        )
        retest_quality = _first_nonblank(
            opened.get("retest_quality"),
            structural_context.get("poi_retest_quality"),
            _signal_tag_value(opened, "RETEST:"),
        )
        candle_quality = _first_nonblank(
            opened.get("candle_quality"),
            opened.get("candle_strength"),
            _signal_tag_value(opened, "CANDLE:"),
            _signal_tag_value(opened, "PA:"),
        )
        exhaustion = _first_nonblank(
            opened.get("exhaustion_cls"),
            qualified_fields.get("exhaustion"),
            entry_context.get("exhaustion_state"),
            _signal_tag_value(opened, "EXHAUSTION:"),
        )
        rr_tag = _signal_tag_value(opened, "RR:")
        score_components = _json_safe_copy(opened.get("score_breakdown") or {})
        invalid_context = _as_list_copy(
            _first_nonblank(opened.get("invalid_context"), smc.get("invalid_context"))
        )
        barrier_context = [
            text for text in _iter_signal_metadata_strings(opened)
            if "BARRIER" in str(text).upper() or "PRE_BREAK" in str(text).upper()
        ]
        phase_text = str(phase or "").upper()
        pre_break_context = bool(
            "PRE_BREAK" in phase_text
            or any("PRE_BREAK" in str(item).upper() for item in barrier_context)
        )

        regime_context = {
            "source": qualified_fields.get("regime_context_source"),
            "freshness": qualified_fields.get("regime_context_freshness"),
            "age_secs": qualified_fields.get("regime_context_age_secs"),
            "router_regime_confidence": entry_context.get("router_regime_confidence"),
        }
        row = {
            "timestamp": format_vn_time(open_ts),
            "event_type": "PAPER_CONFIRM_ENTRY_CONTEXT",
            "symbol": opened.get("symbol", ""),
            "side": opened.get("side", ""),
            "entry_type": "CONFIRM",
            "opened_trade_id": opened.get("id"),
            "signal_created_ts": opened.get("signal_created_ts"),
            "signal_detected_ts": _first_nonblank(
                opened.get("signal_detected_ts"),
                _safe_dict(opened.get("score_breakdown")).get("signal_detected_ts"),
            ),
            "open_ts": open_ts,
            "dedup_key": _first_nonblank(opened.get("dedup_key"), opened.get("research_dedup_key")),
            "reason": reason,
            "original_reason": _json_safe_copy(opened.get("original_reason") or reason),
            "score": _safe_float(opened.get("score")),
            "score_bucket": _paper_confirm_score_bucket(opened.get("score")),
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "planned_rr": planned_rr,
            "sl_distance": sl_distance,
            "tp_distance": tp_distance,
            "risk_percent": _safe_float(opened.get("risk_percent")),
            "phase": phase,
            "market_state": market_state,
            "bos_type": bos_type,
            "bos_quality": bos_quality,
            "retest_quality": retest_quality,
            "candle_quality": candle_quality,
            "exhaustion": exhaustion,
            "rr_tag": rr_tag,
            "score_components": score_components,
            "market_regime": qualified_fields.get("market_regime"),
            "router_regime": entry_context.get("router_regime"),
            "regime_context": regime_context,
            "smc_bias": qualified_fields.get("smc_bias"),
            "trend_direction": qualified_fields.get("trend_direction"),
            "trend_strength": qualified_fields.get("trend_strength"),
            "smc_zone": qualified_fields.get("smc_zone"),
            "premium_discount": entry_context.get("premium_discount"),
            "poi_type": entry_context.get("poi_type"),
            "impulse": qualified_fields.get("impulse"),
            "displacement_quality": entry_context.get("displacement_quality"),
            "range_context": qualified_fields.get("range_context"),
            "liquidity_context": qualified_fields.get("liquidity_context"),
            "liquidity_sweep": qualified_fields.get("liquidity_sweep"),
            "invalid_context": invalid_context,
            "exhaustion_state": exhaustion,
            "volume_confirmation": qualified_fields.get("volume_confirmation"),
            "barrier_context": barrier_context,
            "pre_break_context": pre_break_context,
            "confirm_smc_entry_location_would_block": qualified_fields.get(
                "confirm_smc_entry_location_would_block"
            ),
            "entry_location_primary_reason": qualified_fields.get(
                "confirm_smc_entry_location_primary_reason"
            ),
            "entry_location_risk_bucket": qualified_fields.get(
                "confirm_smc_entry_location_risk_bucket"
            ),
            "entry_location_risk_reasons": _json_safe_copy(
                qualified_fields.get("confirm_smc_entry_location_risk_reasons") or []
            ),
            "v2b_market_bias": qualified_fields.get("v2b_market_bias"),
            "v2b_direction_alignment": qualified_fields.get("v2b_direction_alignment"),
            "v2b_class": qualified_fields.get("v2b_class"),
        }
        row.update({key: _json_safe_copy(value) for key, value in btc_context.items()})
        coverage_fields = (
            "market_regime",
            "router_regime",
            "smc_bias",
            "trend_direction",
            "trend_strength",
            "smc_zone",
            "premium_discount",
            "poi_type",
            "phase",
            "impulse",
            "displacement_quality",
            "range_context",
            "liquidity_context",
            "liquidity_sweep",
            "exhaustion_state",
            "bos_quality",
            "volume_confirmation",
            "retest_quality",
            "candle_quality",
        )
        row["missing_fields"] = [
            field for field in coverage_fields
            if not _paper_confirm_context_resolved(row.get(field))
        ]
        row.update(_paper_confirm_pre_break_low_shadow_safe(row, opened))
        _paper_confirm_entry_context_write(row)
        _paper_confirm_pre_break_low_shadow_snapshot(row)
    except Exception as exc:
        print(f"[PAPER CONFIRM ENTRY CONTEXT] snapshot failed: {exc}")

def _paper_smc_research_float(value):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:
        return None
    return out


def _paper_smc_research_parse_ts(value):
    ts = _paper_smc_research_float(value)
    if ts:
        if ts > 1_000_000_000_000:
            ts = ts / 1000.0
        return ts
    if isinstance(value, str) and value.strip():
        for fmt in ("%Y-%m-%d %H:%M:%S", "%H:%M %d-%m"):
            try:
                parsed = time.strptime(value.strip(), fmt)
                if fmt == "%H:%M %d-%m":
                    current_year = time.localtime().tm_year
                    return time.mktime((
                        current_year,
                        parsed.tm_mon,
                        parsed.tm_mday,
                        parsed.tm_hour,
                        parsed.tm_min,
                        0,
                        -1,
                        -1,
                        -1,
                    ))
                return time.mktime(parsed)
            except Exception:
                pass
    return None


def _paper_smc_research_stale_info(candidate, now_ts):
    max_age = _paper_smc_research_float(config.get("signal_max_age_secs", 180)) or 180
    sources = (
        ("collector_ts", candidate.get("collector_ts")),
        ("source_row_time", candidate.get("source_row_time", candidate.get("timestamp"))),
        ("signal_created_ts", candidate.get("signal_created_ts")),
        ("source_timestamp", candidate.get("source_timestamp")),
    )
    for source_name, raw_value in sources:
        ts = _paper_smc_research_parse_ts(raw_value)
        if ts is None:
            continue
        age = max(0.0, now_ts - ts)
        reason_map = {
            "collector_ts": "stale_collector_ts",
            "source_row_time": "stale_source_row_time",
            "signal_created_ts": "stale_signal_created_ts",
            "source_timestamp": "stale_snapshot_delay",
        }
        return {
            "candidate_age_secs": round(age, 3),
            "candidate_max_age_secs": max_age,
            "candidate_time_source": source_name,
            "source_row_time": candidate.get("source_row_time", candidate.get("timestamp")),
            "signal_created_ts": candidate.get("signal_created_ts"),
            "collector_ts": candidate.get("collector_ts"),
            "stale_reason_detail": reason_map.get(source_name, "stale_candidate"),
            "is_stale": age > max_age,
        }
    return {
        "candidate_age_secs": None,
        "candidate_max_age_secs": max_age,
        "candidate_time_source": "missing",
        "source_row_time": candidate.get("source_row_time", candidate.get("timestamp")),
        "signal_created_ts": candidate.get("signal_created_ts"),
        "collector_ts": candidate.get("collector_ts"),
        "stale_reason_detail": "stale_missing_timestamp",
        "is_stale": True,
    }


def _paper_smc_research_source_ts(candidate):
    for field in ("collector_ts", "source_row_time", "timestamp", "signal_created_ts", "source_timestamp"):
        ts = _paper_smc_research_parse_ts(candidate.get(field))
        if ts is not None:
            return ts
    return None


def _paper_boundary_guard_key(source):
    key = str(source.get("research_dedup_key") or source.get("dedup_key") or "").strip()
    if key:
        return key
    return "|".join(
        str(source.get(field) or "")
        for field in ("id", "symbol", "side", "entry_type", "signal_created_ts", "entry")
    )


def _paper_boundary_guard_base_row(source, event_type, ctx=None, extra=None):
    structural_context = _paper_structural_context(source)
    threshold = _paper_smc_research_float(
        config.get("paper_smc_research_min_score_v2_structural_shadow", 3.5)
    )
    margin = _paper_smc_research_float(
        config.get("paper_structural_score_modifier_boundary_guard_margin", 0.5)
    )
    row = {
        "timestamp": format_vn_time(time.time()),
        "event_type": event_type,
        "symbol": source.get("symbol"),
        "side": source.get("side"),
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "research_dedup_key": _paper_boundary_guard_key(source),
        "original_score_v2": source.get("original_score_v2", source.get("score_v2_structural_shadow")),
        "modifier": source.get("modifier", source.get("structural_modifier_applied")),
        "effective_score_v2": source.get("effective_score_v2", source.get("paper_score_v2_effective")),
        "threshold": threshold,
        "boundary_margin": margin,
        "modifier_reasons": _as_list_copy(source.get("modifier_reasons")),
        "structural_decision_shadow": _paper_structural_context_value(
            source, structural_context, "structural_decision_shadow"
        ),
        "bos_quality": _paper_structural_context_value(source, structural_context, "bos_quality"),
        "volume_confirmation": _paper_structural_context_value(
            source, structural_context, "volume_confirmation"
        ),
        "original_reason": source.get("original_reason", source.get("reason")),
        "geometry_status": source.get("geometry_status"),
        "outcome_trackable": source.get("outcome_trackable"),
    }
    if isinstance(extra, dict):
        row.update({str(k): _json_safe_copy(v) for k, v in extra.items()})
    return {k: _json_safe_copy(v) for k, v in row.items() if v != ""}


def _paper_boundary_guard_write_event(row, ctx=None, notify=False):
    try:
        row = {k: _json_safe_copy(v) for k, v in row.items() if v != ""}
        event_type = str(row.get("event_type") or "")
        key = str(row.get("research_dedup_key") or "")
        dedup_key = (event_type, key)
        if event_type != "BOUNDARY_GUARD_SUMMARY":
            if not key or dedup_key in _paper_boundary_guard_event_dedup:
                return False
            _paper_boundary_guard_event_dedup.add(dedup_key)
            _paper_boundary_guard_event_counts[event_type] = (
                _paper_boundary_guard_event_counts.get(event_type, 0) + 1
            )

        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "paper_boundary_guard_events.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n")

        if notify:
            try:
                from telegram import send_telegram
                if event_type == "BOUNDARY_GUARD_APPLIED":
                    send_telegram(
                        "🧲 PAPER • BOUNDARY-GUARD • CANDIDATE RESCUED\n"
                        "not an entry yet; pending normal gates\n"
                        f"{row.get('symbol')} {row.get('side')}\n"
                        f"score {row.get('original_score_v2')} -> {row.get('effective_score_v2')} "
                        f"threshold={row.get('threshold')}",
                        prefix=getattr(ctx, "mode_prefix", None),
                    )
                elif event_type == "BOUNDARY_GUARD_SUMMARY":
                    send_telegram(
                        "[PAPER BOUNDARY GUARD SUMMARY]\n"
                        f"Seen: {row.get('candidates_seen', 0)} | "
                        f"Applied: {row.get('guard_applied', 0)} | "
                        f"Opened: {row.get('opened', 0)}\n"
                        f"Later suppressed: {row.get('later_suppressed', 0)} | "
                        f"Closed: {row.get('closed', 0)} | "
                        f"Open now: {row.get('current_open_boundary_guarded', 0)}",
                        prefix=getattr(ctx, "mode_prefix", None),
                    )
            except Exception:
                pass
        return True
    except Exception as exc:
        print(f"[PAPER BOUNDARY GUARD WATCHDOG] event log failed: {exc}")
        return False


def _paper_boundary_guard_candidate_seen_info(
    candidate,
    ctx,
    original_score,
    effective_score,
    modifier,
    modifier_reasons=None,
):
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return {}
    if config.get("paper_smc_research_live_enabled", False):
        return {}
    threshold = _paper_smc_research_float(
        config.get("paper_smc_research_min_score_v2_structural_shadow", 3.5)
    )
    margin = _paper_smc_research_float(
        config.get("paper_structural_score_modifier_boundary_guard_margin", 0.5)
    )
    if margin is None:
        margin = 0.5
    if threshold is None or original_score is None or effective_score is None:
        return {}
    if original_score < threshold:
        return {}
    if original_score > threshold + max(0.0, margin):
        return {}
    if modifier is None or modifier >= 0:
        return {}
    if effective_score >= threshold:
        return {}
    return {
        "original_score_v2": original_score,
        "effective_score_v2": effective_score,
        "modifier": modifier,
        "modifier_reasons": list(modifier_reasons or []),
        "threshold": threshold,
        "boundary_margin": max(0.0, margin),
    }


def _paper_boundary_guard_emit_candidate_seen(
    candidate,
    ctx,
    original_score,
    effective_score,
    modifier,
    modifier_reasons=None,
):
    info = _paper_boundary_guard_candidate_seen_info(
        candidate, ctx, original_score, effective_score, modifier, modifier_reasons
    )
    if not info:
        return {}
    row = _paper_boundary_guard_base_row(
        candidate,
        "BOUNDARY_CANDIDATE_SEEN",
        ctx=ctx,
        extra={**info, "final_action": "STILL_PENDING"},
    )
    _paper_boundary_guard_write_event(row, ctx=ctx, notify=False)
    return info


def _paper_boundary_guard_emit_applied(candidate, ctx, boundary_guard_info):
    if not isinstance(boundary_guard_info, dict) or not boundary_guard_info.get("boundary_guard_applied"):
        return
    row = _paper_boundary_guard_base_row(
        candidate,
        "BOUNDARY_GUARD_APPLIED",
        ctx=ctx,
        extra={**boundary_guard_info, "final_action": "STILL_PENDING"},
    )
    _paper_boundary_guard_write_event(
        row,
        ctx=ctx,
        notify=bool(config.get("paper_boundary_guard_notify_applied", True)),
    )


def _paper_boundary_guard_emit_result(candidate, ctx, event_type, final_action, extra=None):
    boundary_info = _paper_smc_research_boundary_guard_log_extra(candidate, ctx)
    if not boundary_info.get("boundary_guard_applied"):
        return
    row = _paper_boundary_guard_base_row(
        candidate,
        event_type,
        ctx=ctx,
        extra={**boundary_info, "final_action": final_action, **(extra or {})},
    )
    _paper_boundary_guard_write_event(row, ctx=ctx, notify=False)


def paper_boundary_guard_observe_close(trade, ctx=None):
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return
    if not isinstance(trade, dict) or not trade.get("boundary_guard_applied"):
        return
    row = _paper_boundary_guard_base_row(
        trade,
        "BOUNDARY_GUARD_CLOSED",
        ctx=ctx,
        extra={
            "final_action": "CLOSED",
            "close_reason": trade.get("close_reason") or trade.get("exit_type"),
            "r_multiple": trade.get("rr_real", trade.get("pnl_r")),
            "mfe_r": trade.get("max_profit_r"),
        },
    )
    _paper_boundary_guard_write_event(row, ctx=ctx, notify=False)


def paper_boundary_guard_maybe_summary(ctx=None):
    global _paper_boundary_guard_summary_last_ts
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return
    if not bool(config.get("paper_boundary_guard_summary_enabled", True)):
        return
    interval = _paper_smc_research_float(
        config.get("paper_boundary_guard_summary_interval_secs", 3600)
    )
    if interval is None:
        interval = 3600
    now_ts = time.time()
    if interval <= 0 or now_ts - _paper_boundary_guard_summary_last_ts < interval:
        return
    _paper_boundary_guard_summary_last_ts = now_ts
    open_boundary = sum(
        1 for t in getattr(ctx, "trades", []) or []
        if t.get("status", "OPEN") == "OPEN" and t.get("boundary_guard_applied")
    )
    row = {
        "timestamp": format_vn_time(now_ts),
        "event_type": "BOUNDARY_GUARD_SUMMARY",
        "candidates_seen": _paper_boundary_guard_event_counts.get("BOUNDARY_CANDIDATE_SEEN", 0),
        "guard_applied": _paper_boundary_guard_event_counts.get("BOUNDARY_GUARD_APPLIED", 0),
        "opened": _paper_boundary_guard_event_counts.get("BOUNDARY_GUARD_OPENED", 0),
        "later_suppressed": _paper_boundary_guard_event_counts.get(
            "BOUNDARY_GUARD_LATER_SUPPRESSED", 0
        ),
        "closed": _paper_boundary_guard_event_counts.get("BOUNDARY_GUARD_CLOSED", 0),
        "current_open_boundary_guarded": open_boundary,
    }
    _paper_boundary_guard_write_event(row, ctx=ctx, notify=True)


def _paper_smc_research_boundary_guard_info(
    candidate,
    ctx,
    original_score,
    effective_score,
    modifier,
    modifier_reasons=None,
):
    if not bool(config.get("paper_structural_score_modifier_boundary_guard_enabled", True)):
        return {}
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return {}
    if config.get("paper_smc_research_live_enabled", False):
        return {}
    if str(candidate.get("entry_type") or "").upper() != "CONFIRM":
        return {}

    reason = str(candidate.get("reason") or "").upper()
    allowed_reasons = {str(x).upper() for x in config.get("paper_smc_research_allow_reasons", [])}
    if reason == "TREND_FAIL" or reason not in allowed_reasons:
        return {}
    if candidate.get("geometry_status") != "VALID_GEOMETRY":
        return {}
    if not candidate.get("outcome_trackable"):
        return {}

    threshold = _paper_smc_research_float(
        config.get("paper_smc_research_min_score_v2_structural_shadow", 3.5)
    )
    margin = _paper_smc_research_float(
        config.get("paper_structural_score_modifier_boundary_guard_margin", 0.5)
    )
    if margin is None:
        margin = 0.5
    if threshold is None or original_score is None or effective_score is None:
        return {}
    if original_score < threshold:
        return {}
    if effective_score >= threshold:
        return {}
    if original_score > threshold + max(0.0, margin):
        return {}

    return {
        "boundary_guard_applied": True,
        "boundary_guard_reason": "modifier_flip_at_threshold",
        "original_score_v2": original_score,
        "effective_score_v2": effective_score,
        "modifier": modifier,
        "modifier_reasons": list(modifier_reasons or []),
    }


def _paper_smc_research_boundary_guard_log_extra(candidate, ctx):
    if not bool(config.get("paper_enable_structural_score_modifier", False)):
        return {}
    if bool(config.get("paper_structural_score_modifier_log_only", True)):
        return {}
    try:
        modifier, reasons, structural_context = _paper_structural_score_modifier(candidate)
    except Exception:
        return {}
    original_score = _paper_smc_research_float(
        structural_context.get("score_v2_structural_shadow")
    )
    effective_score = round(original_score + modifier, 4) if original_score is not None else None
    return _paper_smc_research_boundary_guard_info(
        candidate,
        ctx,
        original_score,
        effective_score,
        modifier,
        reasons,
    )


def _paper_smc_research_open_count(ctx):
    return sum(
        1 for trade in getattr(ctx, "trades", []) or []
        if trade.get("status", "OPEN") == "OPEN"
        and trade.get("owner", "bot") == "bot"
        and trade.get("strategy_family") == "confirm_smc_research"
    )


def _paper_smc_research_symbol_open(ctx, symbol, side=None, research_only=False):
    for trade in getattr(ctx, "trades", []) or []:
        if trade.get("status", "OPEN") != "OPEN":
            continue
        if trade.get("owner", "bot") != "bot":
            continue
        if trade.get("symbol") != symbol:
            continue
        if side and trade.get("side") != side:
            continue
        if research_only and trade.get("strategy_family") != "confirm_smc_research":
            continue
        return True
    return False


def _paper_smc_research_key_open(ctx, dedup_key):
    if not dedup_key:
        return False
    return any(
        trade.get("status", "OPEN") == "OPEN"
        and trade.get("strategy_family") == "confirm_smc_research"
        and trade.get("research_dedup_key") == dedup_key
        for trade in getattr(ctx, "trades", []) or []
    )


def _paper_smc_research_qualified_open_count(ctx):
    return sum(
        1 for trade in getattr(ctx, "trades", []) or []
        if trade.get("status", "OPEN") == "OPEN"
        and trade.get("owner", "bot") == "bot"
        and trade.get("strategy_family") == "confirm_smc_research"
        and bool(trade.get("paper_smc_research_qualified"))
    )


def _paper_smc_research_qualified_mode_allowed(ctx):
    return getattr(ctx, "execution_mode", None) in ("paper", "paper_live")


def _paper_smc_research_qualified_log_path():
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, "paper_smc_research_qualified_decisions.jsonl")


def _qualified_latency_waterfall_log_path():
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, "qualified_latency_waterfall.jsonl")


_QUALIFIED_LATENCY_NULL_FIELDS = {
    "signal_created_ts",
    "signal_detected_ts",
    "collector_ts",
    "qualified_eval_ts",
    "dispatch_ts",
    "open_trade_ts",
    "stage1_candle_to_detect_secs",
    "stage2_detect_to_register_secs",
    "stage3_register_to_eval_secs",
    "stage4_eval_to_dispatch_secs",
    "stage5_dispatch_to_open_secs",
    "total_structural_origin_to_open_secs",
    "total_detect_to_open_secs",
}


def _qualified_latency_ts(value):
    return _paper_smc_research_parse_ts(value)


def _qualified_latency_delta(later, earlier):
    if later is None or earlier is None:
        return None
    return round(later - earlier, 3)


def _paper_smc_research_qualified_latency_fields(
    candidate,
    qualified_eval_ts=None,
    dispatch_ts=None,
    open_trade_ts=None,
):
    signal_created_ts = _qualified_latency_ts(candidate.get("signal_created_ts"))
    signal_detected_ts = _qualified_latency_ts(candidate.get("signal_detected_ts"))
    collector_ts = _qualified_latency_ts(candidate.get("collector_ts"))
    qualified_eval_ts = _qualified_latency_ts(
        qualified_eval_ts if qualified_eval_ts is not None else candidate.get("_qualified_eval_ts")
    )
    dispatch_ts = _qualified_latency_ts(
        dispatch_ts if dispatch_ts is not None else candidate.get("_dispatch_ts")
    )
    open_trade_ts = _qualified_latency_ts(
        open_trade_ts if open_trade_ts is not None else candidate.get("_open_trade_ts")
    )
    return {
        "signal_created_ts": signal_created_ts,
        "signal_detected_ts": signal_detected_ts,
        "collector_ts": collector_ts,
        "qualified_eval_ts": qualified_eval_ts,
        "dispatch_ts": dispatch_ts,
        "open_trade_ts": open_trade_ts,
        "stage1_candle_to_detect_secs": _qualified_latency_delta(signal_detected_ts, signal_created_ts),
        "stage2_detect_to_register_secs": _qualified_latency_delta(collector_ts, signal_detected_ts),
        "stage3_register_to_eval_secs": _qualified_latency_delta(qualified_eval_ts, collector_ts),
        "stage4_eval_to_dispatch_secs": _qualified_latency_delta(dispatch_ts, qualified_eval_ts),
        "stage5_dispatch_to_open_secs": _qualified_latency_delta(open_trade_ts, dispatch_ts),
        "total_structural_origin_to_open_secs": _qualified_latency_delta(open_trade_ts, signal_created_ts),
        "total_detect_to_open_secs": _qualified_latency_delta(open_trade_ts, signal_detected_ts),
    }


def _paper_smc_research_emit_qualified_latency_waterfall(
    candidate,
    opened,
    fields=None,
    qualified_eval_ts=None,
    dispatch_ts=None,
    open_trade_ts=None,
    open_count_at_decision=None,
    max_open=None,
):
    try:
        fields = fields or {}
        latency = _paper_smc_research_qualified_latency_fields(
            candidate,
            qualified_eval_ts=qualified_eval_ts,
            dispatch_ts=dispatch_ts,
            open_trade_ts=open_trade_ts,
        )
        row = {
            "event_type": "QUALIFIED_LATENCY_WATERFALL",
            "timestamp": format_vn_time(time.time()),
            "symbol": candidate.get("symbol"),
            "side": candidate.get("side"),
            "dedup_key": candidate.get("dedup_key"),
            "opened_trade_id": (opened or {}).get("id"),
            **latency,
            "paper_smc_research_qualified_max_open": max_open,
            "open_count_at_decision": open_count_at_decision,
            "decision": "OPEN",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "strategy_family": "confirm_smc_research",
        }
        with open(_qualified_latency_waterfall_log_path(), "a", encoding="utf-8") as handle:
            handle.write(json.dumps(_json_safe_copy(row), ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH QUALIFIED] latency waterfall log failed: {exc}")


_PAPER_SMC_RESEARCH_QUALIFIED_BIAS_FIELDS = (
    "market_state",
    "market_regime",
    "smc_bias",
    "smc_zone",
    "trend_direction",
    "trend_strength",
    "phase",
    "impulse",
    "range_context",
    "liquidity_sweep",
    "invalid_context",
    "exhaustion",
    "regime_context_source",
    "regime_context_freshness",
    "regime_context_age_secs",
)

CONFIRM_SMC_ENTRY_LOCATION_TREND_STRENGTH_MIN = 0.02
CONFIRM_SMC_ENTRY_LOCATION_VERSION = "v0.1_shadow"

_PAPER_SMC_ENTRY_LOCATION_RISK_FIELDS = (
    "confirm_smc_entry_location_risk_score",
    "confirm_smc_entry_location_risk_bucket",
    "confirm_smc_entry_location_risk_reasons",
    "confirm_smc_entry_location_would_block",
    "confirm_smc_entry_location_primary_reason",
    "confirm_smc_entry_location_low_confidence",
    "confirm_smc_entry_location_version",
)

_CONFIRM_SMC_ENTRY_LOCATION_BAD_REGIMES = {
    "CHOP_NO_TRADE",
    "EXHAUSTION_REVERSAL",
}

_CONFIRM_SMC_ENTRY_LOCATION_HEALTHY_EXHAUSTION = {
    "HEALTHY",
    "NONE",
    "NO",
    "FALSE",
}

_CONFIRM_SMC_ENTRY_LOCATION_PHASE_EXACT_RISK = {
    "BREAKOUT_WEAK",
    "DISTRIBUTION",
}


def _paper_smc_research_qualified_unknown(value):
    return value if value not in (None, "") else "UNKNOWN"


def _confirm_smc_entry_location_text(value):
    if isinstance(value, str):
        return value.strip().upper()
    if value is None:
        return ""
    return str(value).strip().upper()


def _confirm_smc_entry_location_known(value, none_is_known=False):
    if value is None:
        return False
    if isinstance(value, str):
        text = value.strip().upper()
        if text in ("", "UNKNOWN"):
            return False
        if text == "NONE" and not none_is_known:
            return False
    return True


def _confirm_smc_entry_location_impulse_true(value):
    if value is True:
        return True
    if isinstance(value, str) and value.strip().upper() == "TRUE":
        return True
    return False


def _confirm_smc_entry_location_phase_risk(phase):
    phase_upper = _confirm_smc_entry_location_text(phase)
    return (
        phase_upper in _CONFIRM_SMC_ENTRY_LOCATION_PHASE_EXACT_RISK
        or "EXHAUST" in phase_upper
        or "LATE" in phase_upper
        or "FAILED" in phase_upper
    )


def _compute_confirm_smc_entry_location_risk(decision_or_candidate_context):
    ctx = decision_or_candidate_context if isinstance(decision_or_candidate_context, dict) else {}
    side = _confirm_smc_entry_location_text(ctx.get("side"))
    market_state = _confirm_smc_entry_location_text(ctx.get("market_state"))
    market_regime = _confirm_smc_entry_location_text(ctx.get("market_regime"))
    smc_bias = _confirm_smc_entry_location_text(ctx.get("smc_bias"))
    smc_zone = _confirm_smc_entry_location_text(ctx.get("smc_zone"))
    trend_direction = _confirm_smc_entry_location_text(ctx.get("trend_direction"))
    phase = _confirm_smc_entry_location_text(ctx.get("phase"))
    exhaustion = _confirm_smc_entry_location_text(ctx.get("exhaustion"))
    trend_strength = _paper_smc_research_float(ctx.get("trend_strength"))

    impulse_known = _confirm_smc_entry_location_known(ctx.get("impulse"))
    impulse_true = _confirm_smc_entry_location_impulse_true(ctx.get("impulse"))
    low_confidence = False
    reasons = []
    risk_reasons = []

    def add_risk(reason):
        if reason not in reasons:
            reasons.append(reason)
        risk_reasons.append(reason)

    def add_uncertain(reason):
        if reason not in reasons:
            reasons.append(reason)

    if _confirm_smc_entry_location_known(ctx.get("market_state")) and market_state != "TREND":
        add_risk("MARKET_NOT_TREND")

    if impulse_known:
        if not impulse_true:
            add_risk("NO_IMPULSE")
    else:
        low_confidence = True
        add_uncertain("IMPULSE_UNKNOWN")

    if trend_strength is not None:
        if trend_strength <= CONFIRM_SMC_ENTRY_LOCATION_TREND_STRENGTH_MIN:
            add_risk("LOW_TREND_STRENGTH")
    else:
        low_confidence = True
        add_uncertain("TREND_STRENGTH_UNKNOWN")

    if _confirm_smc_entry_location_known(ctx.get("smc_zone")):
        if side == "LONG" and smc_zone == "PREMIUM" and not impulse_true:
            add_risk("LONG_PREMIUM_NO_IMPULSE")
        if side == "SHORT" and smc_zone == "DISCOUNT" and not impulse_true:
            add_risk("SHORT_DISCOUNT_NO_IMPULSE")
    else:
        low_confidence = True
        add_uncertain("SMC_ZONE_UNKNOWN")

    if _confirm_smc_entry_location_known(ctx.get("market_regime")):
        if market_regime in _CONFIRM_SMC_ENTRY_LOCATION_BAD_REGIMES:
            add_risk("BAD_REGIME")
    else:
        low_confidence = True
        add_uncertain("MARKET_REGIME_UNKNOWN")

    if (
        market_state == "EXHAUSTION"
        or market_regime == "EXHAUSTION_REVERSAL"
        or (
            _confirm_smc_entry_location_known(ctx.get("exhaustion"), none_is_known=True)
            and exhaustion not in _CONFIRM_SMC_ENTRY_LOCATION_HEALTHY_EXHAUSTION
        )
    ):
        add_risk("EXHAUSTION_RISK")
    if not _confirm_smc_entry_location_known(ctx.get("exhaustion"), none_is_known=True):
        low_confidence = True
        add_uncertain("EXHAUSTION_UNKNOWN")

    trend_known = _confirm_smc_entry_location_known(ctx.get("trend_direction"))
    bias_known = _confirm_smc_entry_location_known(ctx.get("smc_bias"))
    if side == "LONG" and (trend_direction == "SHORT" or smc_bias == "BEARISH"):
        add_risk("DIRECTIONAL_CONFLICT")
    if side == "SHORT" and (trend_direction == "LONG" or smc_bias == "BULLISH"):
        add_risk("DIRECTIONAL_CONFLICT")
    if not trend_known and not bias_known:
        low_confidence = True
        add_uncertain("DIRECTION_CONTEXT_UNKNOWN")

    if _confirm_smc_entry_location_known(ctx.get("phase")):
        if _confirm_smc_entry_location_phase_risk(phase):
            add_risk("LATE_PHASE_RISK")
    else:
        low_confidence = True
        add_uncertain("PHASE_UNKNOWN")

    risk_score = len(risk_reasons)
    if risk_score <= 1:
        bucket = "OK"
    elif risk_score == 2:
        bucket = "CAUTION"
    else:
        bucket = "HIGH_RISK"

    primary_reason = "NONE"
    priority = (
        "EXHAUSTION_RISK",
        "DIRECTIONAL_CONFLICT",
        "BAD_SIDE_ZONE_WITHOUT_IMPULSE",
        "BAD_REGIME",
        "MARKET_NOT_TREND",
        "NO_IMPULSE",
        "LOW_TREND_STRENGTH",
        "LATE_PHASE_RISK",
    )
    primary_aliases = {
        "BAD_SIDE_ZONE_WITHOUT_IMPULSE": {
            "LONG_PREMIUM_NO_IMPULSE",
            "SHORT_DISCOUNT_NO_IMPULSE",
        }
    }
    risk_reason_set = set(risk_reasons)
    for item in priority:
        aliases = primary_aliases.get(item, {item})
        if risk_reason_set.intersection(aliases):
            primary_reason = item
            break

    return {
        "confirm_smc_entry_location_risk_score": risk_score,
        "confirm_smc_entry_location_risk_bucket": bucket,
        "confirm_smc_entry_location_risk_reasons": reasons,
        "confirm_smc_entry_location_would_block": risk_score >= 3,
        "confirm_smc_entry_location_primary_reason": primary_reason,
        "confirm_smc_entry_location_low_confidence": bool(low_confidence),
        "confirm_smc_entry_location_version": CONFIRM_SMC_ENTRY_LOCATION_VERSION,
    }


def _paper_smc_research_qualified_bias_context(candidate, structural_context=None):
    structural_context = _safe_dict(structural_context) or _paper_structural_context(candidate)
    market_regime = _first_nonblank(
        candidate.get("market_regime"),
        candidate.get("market_regime_at_entry"),
        candidate.get("router_regime"),
        candidate.get("regime"),
        structural_context.get("market_regime"),
        structural_context.get("market_regime_at_entry"),
        structural_context.get("router_regime"),
        structural_context.get("regime"),
    )
    router_available = any(
        candidate.get(key) not in (None, "")
        for key in (
            "router_regime",
            "router_market_state",
            "router_phase",
            "router_exhaustion",
            "router_observed_at",
            "router_regime_source",
        )
    )
    candidate_available = any(
        _first_nonblank(candidate.get(key), structural_context.get(key)) not in (None, "")
        for key in (
            "market_state",
            "smc_bias",
            "smc_zone",
            "trend_direction",
            "trend_strength",
            "phase",
            "impulse",
            "range_context",
            "liquidity_sweep",
            "exhaustion",
        )
    )
    invalid_context = _first_nonblank(
        candidate.get("invalid_context"),
        structural_context.get("invalid_context"),
        candidate.get("router_invalid_context"),
    )
    if invalid_context is not None:
        invalid_context = _as_list_copy(invalid_context)

    age_secs = _paper_smc_research_float(
        _first_nonblank(
            candidate.get("regime_context_age_secs"),
            candidate.get("router_regime_age_secs"),
            candidate.get("router_regime_age_sec"),
            structural_context.get("regime_context_age_secs"),
            structural_context.get("router_regime_age_secs"),
            structural_context.get("router_regime_age_sec"),
        )
    )
    if age_secs is not None:
        age_secs = round(age_secs, 3)

    source = _first_nonblank(
        candidate.get("regime_context_source"),
        candidate.get("router_regime_source"),
        structural_context.get("regime_context_source"),
        structural_context.get("router_regime_source"),
    )
    if source in (None, ""):
        if router_available:
            source = "router_context"
        elif candidate_available or market_regime not in (None, "") or invalid_context is not None:
            source = "candidate"

    return {
        "market_state": _paper_smc_research_qualified_unknown(
            _first_nonblank(
                candidate.get("market_state"),
                candidate.get("router_market_state"),
                candidate.get("mkt_state"),
                structural_context.get("market_state"),
                structural_context.get("router_market_state"),
                structural_context.get("mkt_state"),
            )
        ),
        "market_regime": _paper_smc_research_qualified_unknown(market_regime),
        "smc_bias": _paper_smc_research_qualified_unknown(
            _first_nonblank(
                candidate.get("smc_bias"),
                structural_context.get("smc_bias"),
                candidate.get("router_smc_bias"),
                structural_context.get("router_smc_bias"),
            )
        ),
        "smc_zone": _paper_smc_research_qualified_unknown(
            _first_nonblank(
                candidate.get("smc_zone"),
                structural_context.get("smc_zone"),
                candidate.get("router_smc_zone"),
                structural_context.get("router_smc_zone"),
            )
        ),
        "trend_direction": _paper_smc_research_qualified_unknown(
            _first_nonblank(
                candidate.get("trend_direction"),
                candidate.get("router_trend_direction"),
                structural_context.get("trend_direction"),
                structural_context.get("router_trend_direction"),
            )
        ),
        "trend_strength": _paper_smc_research_float(
            _first_nonblank(
                candidate.get("trend_strength"),
                candidate.get("router_trend_strength"),
                structural_context.get("trend_strength"),
                structural_context.get("router_trend_strength"),
            )
        ),
        "phase": _paper_smc_research_qualified_unknown(
            _first_nonblank(
                candidate.get("phase"),
                candidate.get("router_phase"),
                structural_context.get("phase"),
                structural_context.get("router_phase"),
            )
        ),
        "impulse": _first_nonblank(
            candidate.get("impulse"),
            candidate.get("router_impulse"),
            structural_context.get("impulse"),
            structural_context.get("router_impulse"),
        ),
        "range_context": _paper_smc_research_qualified_unknown(
            _first_nonblank(
                candidate.get("range_context"),
                structural_context.get("range_context"),
                candidate.get("router_range_context"),
                structural_context.get("router_range_context"),
            )
        ),
        "liquidity_sweep": _paper_smc_research_qualified_unknown(
            _first_nonblank(
                candidate.get("liquidity_sweep"),
                structural_context.get("liquidity_sweep"),
                candidate.get("router_liquidity_sweep"),
                structural_context.get("router_liquidity_sweep"),
            )
        ),
        "invalid_context": invalid_context,
        "exhaustion": _paper_smc_research_qualified_unknown(
            _first_nonblank(
                candidate.get("exhaustion"),
                candidate.get("exhaustion_cls"),
                candidate.get("router_exhaustion"),
                structural_context.get("exhaustion"),
                structural_context.get("exhaustion_cls"),
                structural_context.get("router_exhaustion"),
            )
        ),
        "regime_context_source": _paper_smc_research_qualified_unknown(source),
        "regime_context_freshness": _paper_smc_research_qualified_unknown(
            _first_nonblank(
                candidate.get("regime_context_freshness"),
                candidate.get("router_freshness_status"),
                structural_context.get("regime_context_freshness"),
                structural_context.get("router_freshness_status"),
            )
        ),
        "regime_context_age_secs": age_secs,
    }


def _paper_smc_research_qualified_apply_bias_context(trade, fields):
    for key in _PAPER_SMC_RESEARCH_QUALIFIED_BIAS_FIELDS:
        trade[key] = _json_safe_copy(fields.get(key))


def _paper_smc_research_qualified_apply_entry_location_risk(trade, fields):
    for key in _PAPER_SMC_ENTRY_LOCATION_RISK_FIELDS:
        trade[key] = _json_safe_copy(fields.get(key))


def _confirm_smc_location_gate_reason(fields):
    fields = fields if isinstance(fields, dict) else {}
    primary = fields.get("confirm_smc_entry_location_primary_reason")
    if primary not in (None, ""):
        return primary
    reasons = fields.get("confirm_smc_entry_location_risk_reasons")
    if isinstance(reasons, (list, tuple)) and reasons:
        return "|".join(str(item) for item in reasons if item not in (None, ""))
    bucket = fields.get("confirm_smc_entry_location_risk_bucket")
    if bucket not in (None, ""):
        return f"risk_bucket={bucket}"
    return "UNKNOWN_LOCATION_BLOCK_REASON"


def _confirm_smc_location_gate_would_block(fields):
    fields = fields if isinstance(fields, dict) else {}
    return fields.get("confirm_smc_entry_location_would_block") is True


def _confirm_smc_location_gate_log_fields(candidate, fields=None, mode="SHADOW_ONLY"):
    candidate = candidate if isinstance(candidate, dict) else {}
    fields = fields if isinstance(fields, dict) else _paper_smc_research_qualified_fields(candidate)
    would_block = _confirm_smc_location_gate_would_block(fields)
    return {
        "gate_source": "EXISTING_LOCATION_BLOCK_SHADOW",
        "location_gate_source": "EXISTING_LOCATION_BLOCK_SHADOW",
        "location_gate_mode": mode,
        "location_gate_would_block": would_block,
        "location_gate_reason": _confirm_smc_location_gate_reason(fields),
        "location_gate_enabled": (
            bool(config.get("paper_smc_research_location_gate_enabled", False))
            if mode == "PAPER_GATE"
            else bool(config.get("live_smc_research_location_gate_enabled", False))
        ),
        "confirm_smc_entry_location_would_block": fields.get(
            "confirm_smc_entry_location_would_block"
        ),
        "confirm_smc_entry_location_primary_reason": fields.get(
            "confirm_smc_entry_location_primary_reason"
        ),
        "confirm_smc_entry_location_risk_bucket": fields.get(
            "confirm_smc_entry_location_risk_bucket"
        ),
        "confirm_smc_entry_location_risk_score": fields.get(
            "confirm_smc_entry_location_risk_score"
        ),
        "confirm_smc_entry_location_risk_reasons": _json_safe_copy(
            fields.get("confirm_smc_entry_location_risk_reasons") or []
        ),
        "would_have_entry": candidate.get("entry"),
        "would_have_sl": candidate.get("sl"),
        "would_have_tp": candidate.get("tp"),
        "would_have_rr": fields.get("planned_rr"),
        "smc_zone": fields.get("smc_zone"),
        "market_regime": fields.get("market_regime"),
        "bos_quality": fields.get("bos_quality"),
        "exhaustion": fields.get("exhaustion"),
        "fallback_reason": fields.get("research_fallback_reason"),
    }


def _paper_smc_research_location_gate_blocks(fields):
    return (
        bool(config.get("paper_smc_research_location_gate_enabled", False))
        and _confirm_smc_location_gate_would_block(fields)
    )


SMC_ENTRY_V2_SHADOW_VERSION = "v0.1_shadow"
SMC_ENTRY_V2_SHADOW_LOG = os.path.join("logs", "smc_entry_v2_shadow.jsonl")
SMC_ENTRY_V2B_ALLOWLIST_VERSION = "v0.1_shadow"
SMC_ENTRY_V2B_ALLOWLIST_LABEL = "SHORT_EXTENDED_SCORE_2_3"
SMC_ENTRY_V2B_ALLOWLIST_V02_LABEL = "PRE_BREAK_LOW_CHOP_RANGE_SCORE_2_3"
SMC_ENTRY_V2B_ALLOWLIST_LOG = os.path.join("logs", "smc_entry_v2b_allowlist_shadow.jsonl")


def _smc_entry_v2_text(value):
    if value is None:
        return ""
    return str(value).strip().upper()


def _smc_entry_v2_float(value):
    return _paper_smc_research_float(value)


def _smc_entry_v2b_score_bucket(value):
    score = _smc_entry_v2_float(value)
    if score is None:
        return "SCORE_UNKNOWN"
    if score < 1:
        return "SCORE_LT_1"
    if score < 2:
        return "SCORE_1_2"
    if score < 4:
        return "SCORE_2_3"
    return "SCORE_GTE_4"


def _smc_entry_v2b_decision_score(fields):
    fields = fields if isinstance(fields, dict) else {}
    source = "fields.score_v2_structural_shadow"
    return source, _smc_entry_v2_float(fields.get("score_v2_structural_shadow"))


def _smc_entry_v2b_v02_regime_match(regime, timing_risk_class):
    regime = _smc_entry_v2_text(regime)
    timing_risk_class = _smc_entry_v2_text(timing_risk_class)
    return (
        timing_risk_class == "CHOP_OR_RANGE_ENTRY"
        or regime in {
            "CHOP_NO_TRADE",
            "RANGE_ENTRY",
            "RANGE_MEAN_REVERSION",
            "CHOP_OR_RANGE_ENTRY",
        }
    )


def _smc_entry_v2b_recompute_mismatch_reason(shadow):
    if not isinstance(shadow, dict):
        return "shadow_not_dict"
    mismatches = []
    if shadow.get("v0.1_match") != shadow.get("v2b_v01_recomputed_match"):
        mismatches.append("v0.1_match")
    if shadow.get("v0.2_match") != shadow.get("v2b_v02_recomputed_match"):
        mismatches.append("v0.2_match")
    return "NONE" if not mismatches else ",".join(mismatches)


def _smc_entry_v2b_feature_quality(candidate, fields):
    candidate = candidate if isinstance(candidate, dict) else {}
    fields = fields if isinstance(fields, dict) else {}
    exact_keys = (
        "premium_discount",
        "dow_trend_context",
        "poi_type",
        "poi_location_quality",
        "entry_poi_alignment",
        "liquidity_context",
    )
    for key in exact_keys:
        value = _smc_entry_v2_text(_first_nonblank(candidate.get(key), fields.get(key)))
        if value not in {"", "UNKNOWN", "NONE"}:
            return "EXACT_FEATURES"
    return "COARSE_PROXY"


def _smc_entry_v2b_allowlist_shadow(candidate, fields=None, trade=None, mode="SHADOW_ONLY"):
    candidate = candidate if isinstance(candidate, dict) else {}
    trade = trade if isinstance(trade, dict) else {}
    fields = fields if isinstance(fields, dict) else _paper_smc_research_qualified_fields(candidate)
    side = _smc_entry_v2_text(_first_nonblank(candidate.get("side"), trade.get("side")))
    score_source, score = _smc_entry_v2b_decision_score(fields)
    score_bucket = _smc_entry_v2b_score_bucket(score)
    exhaustion = _smc_entry_v2_text(
        _first_nonblank(
            fields.get("exhaustion"),
            fields.get("exhaustion_state"),
            candidate.get("exhaustion"),
            candidate.get("exhaustion_state"),
            candidate.get("exhaustion_cls"),
            trade.get("exhaustion"),
        )
    )
    entry_location = _smc_entry_v2_text(fields.get("phase"))
    entry_location_source = "fields.phase"
    regime = _smc_entry_v2_text(fields.get("market_regime"))
    timing_risk_class = _smc_entry_v2_text(fields.get("research_entry_timing_risk_class"))
    if timing_risk_class == "CHOP_OR_RANGE_ENTRY":
        regime_source = "fields.research_entry_timing_risk_class"
    else:
        regime_source = "fields.market_regime"
    match = side == "SHORT" and exhaustion == "EXTENDED" and score_bucket == "SCORE_2_3"
    if match:
        reason = "side=SHORT;exhaustion=EXTENDED;score_bucket=SCORE_2_3"
    else:
        misses = []
        if side != "SHORT":
            misses.append(f"side={side or 'UNKNOWN'}")
        if exhaustion != "EXTENDED":
            misses.append(f"exhaustion={exhaustion or 'UNKNOWN'}")
        if score_bucket != "SCORE_2_3":
            misses.append(f"score_bucket={score_bucket}")
        reason = "not_allowlisted:" + ";".join(misses)
    v02_match = (
        entry_location == "PRE_BREAK_LOW"
        and _smc_entry_v2b_v02_regime_match(regime, timing_risk_class)
        and score_bucket == "SCORE_2_3"
    )
    if v02_match:
        v02_reason = "entry_location=PRE_BREAK_LOW;regime=CHOP_OR_RANGE_ENTRY;score_bucket=SCORE_2_3"
    else:
        v02_misses = []
        if entry_location != "PRE_BREAK_LOW":
            v02_misses.append(f"entry_location={entry_location or 'UNKNOWN'}")
        if not _smc_entry_v2b_v02_regime_match(regime, timing_risk_class):
            v02_misses.append(
                f"regime={regime or 'UNKNOWN'};timing_risk_class={timing_risk_class or 'UNKNOWN'}"
            )
        if score_bucket != "SCORE_2_3":
            v02_misses.append(f"score_bucket={score_bucket}")
        v02_reason = "not_allowlisted:" + ";".join(v02_misses)
    v01_recomputed_match = side == "SHORT" and exhaustion == "EXTENDED" and score_bucket == "SCORE_2_3"
    v02_recomputed_match = (
        entry_location == "PRE_BREAK_LOW"
        and _smc_entry_v2b_v02_regime_match(regime, timing_risk_class)
        and score_bucket == "SCORE_2_3"
    )
    recompute_match = bool(match) == bool(v01_recomputed_match) and bool(v02_match) == bool(v02_recomputed_match)
    recompute_mismatch_reason = "NONE" if recompute_match else _smc_entry_v2b_recompute_mismatch_reason({
        "v0.1_match": bool(match),
        "v0.2_match": bool(v02_match),
        "v2b_v01_recomputed_match": bool(v01_recomputed_match),
        "v2b_v02_recomputed_match": bool(v02_recomputed_match),
    })
    return {
        "smc_entry_v2b_allowlist_label": SMC_ENTRY_V2B_ALLOWLIST_LABEL,
        "smc_entry_v2b_allowlist_match": bool(match),
        "smc_entry_v2b_allowlist_reason": reason,
        "smc_entry_v2b_allowlist_version": SMC_ENTRY_V2B_ALLOWLIST_VERSION,
        "smc_entry_v2b_v02_label": SMC_ENTRY_V2B_ALLOWLIST_V02_LABEL,
        "smc_entry_v2b_v02_match": bool(v02_match),
        "smc_entry_v2b_v02_reason": v02_reason,
        "smc_entry_v2b_v02_version": "v0.2_shadow",
        "v0.1_match": bool(match),
        "v0.2_match": bool(v02_match),
        "smc_entry_v2b_score_bucket": score_bucket,
        "smc_entry_v2b_score": score,
        "v2b_score_source": score_source,
        "v2b_score_value": score,
        "v2b_score_bucket": score_bucket,
        "v2b_entry_location": entry_location,
        "v2b_entry_location_source": entry_location_source,
        "v2b_regime": regime,
        "v2b_timing_risk_class": timing_risk_class,
        "v2b_regime_source": regime_source,
        "v2b_exhaustion": exhaustion,
        "v2b_side": side,
        "v2b_v01_recomputed_match": bool(v01_recomputed_match),
        "v2b_v02_recomputed_match": bool(v02_recomputed_match),
        "v2b_recompute_match": recompute_match,
        "v2b_recompute_mismatch_reason": recompute_mismatch_reason,
        "smc_entry_v2b_feature_quality": _smc_entry_v2b_feature_quality(candidate, fields),
        "smc_entry_v2b_forward_mode": mode,
    }


def _smc_entry_v2b_allowlist_shadow_write(
    candidate,
    fields,
    shadow,
    execution_mode="shadow_context",
    v1_decision="",
    v1_reason="",
    trade=None,
    now_ts=None,
    opened_trade_id=None,
):
    try:
        candidate = candidate if isinstance(candidate, dict) else {}
        fields = fields if isinstance(fields, dict) else {}
        shadow = shadow if isinstance(shadow, dict) else {}
        trade = trade if isinstance(trade, dict) else {}
        now_ts = time.time() if now_ts is None else now_ts
        row = {
            "ts": now_ts,
            "symbol": candidate.get("symbol") or trade.get("symbol"),
            "side": candidate.get("side") or trade.get("side"),
            "lane": _first_nonblank(
                candidate.get("lane"),
                candidate.get("entry_type"),
                trade.get("lane"),
                trade.get("entry_type"),
                "CONFIRM_SMC_RESEARCH",
            ),
            "signal_ts": _first_nonblank(
                candidate.get("signal_created_ts"),
                candidate.get("source_timestamp"),
                candidate.get("collector_ts"),
                candidate.get("timestamp"),
                trade.get("signal_created_ts"),
            ),
            "dedup_key": candidate.get("dedup_key") or trade.get("research_dedup_key"),
            "execution_mode": execution_mode,
            "opened_trade_id": _first_nonblank(
                opened_trade_id,
                candidate.get("opened_trade_id"),
                trade.get("opened_trade_id"),
                trade.get("id"),
            ),
            "v1_decision": v1_decision,
            "v1_reason": v1_reason,
            "score": shadow.get("smc_entry_v2b_score"),
            "score_bucket": shadow.get("smc_entry_v2b_score_bucket"),
            "v2b_score_source": shadow.get("v2b_score_source"),
            "v2b_score_value": shadow.get("v2b_score_value"),
            "v2b_score_bucket": shadow.get("v2b_score_bucket"),
            "exhaustion": _first_nonblank(
                fields.get("exhaustion"),
                fields.get("exhaustion_state"),
                candidate.get("exhaustion"),
                candidate.get("exhaustion_state"),
                candidate.get("exhaustion_cls"),
                trade.get("exhaustion"),
            ),
            "smc_zone": fields.get("smc_zone"),
            "market_regime": fields.get("market_regime"),
            "v2b_regime": shadow.get("v2b_regime"),
            "v2b_timing_risk_class": shadow.get("v2b_timing_risk_class"),
            "v2b_regime_source": shadow.get("v2b_regime_source"),
            "bos_quality": fields.get("bos_quality"),
            "phase": fields.get("phase"),
            "entry_location": shadow.get("v2b_entry_location"),
            "v2b_entry_location": shadow.get("v2b_entry_location"),
            "v2b_entry_location_source": shadow.get("v2b_entry_location_source"),
            "rr": _first_nonblank(fields.get("planned_rr"), candidate.get("rr"), trade.get("rr")),
            "entry": _first_nonblank(candidate.get("entry"), trade.get("entry")),
            "sl": _first_nonblank(candidate.get("sl"), trade.get("sl")),
            "tp": _first_nonblank(candidate.get("tp"), trade.get("tp")),
            "allowlist_match": shadow.get("smc_entry_v2b_allowlist_match"),
            "allowlist_reason": shadow.get("smc_entry_v2b_allowlist_reason"),
            "feature_quality": shadow.get("smc_entry_v2b_feature_quality"),
            "smc_entry_v2b_allowlist_label": shadow.get("smc_entry_v2b_allowlist_label"),
            "smc_entry_v2b_allowlist_version": shadow.get("smc_entry_v2b_allowlist_version"),
            "smc_entry_v2b_v02_label": shadow.get("smc_entry_v2b_v02_label"),
            "smc_entry_v2b_v02_match": shadow.get("smc_entry_v2b_v02_match"),
            "smc_entry_v2b_v02_reason": shadow.get("smc_entry_v2b_v02_reason"),
            "smc_entry_v2b_v02_version": shadow.get("smc_entry_v2b_v02_version"),
            "v0.1_match": shadow.get("v0.1_match"),
            "v0.2_match": shadow.get("v0.2_match"),
            "v2b_recompute_match": shadow.get("v2b_recompute_match"),
            "v2b_recompute_mismatch_reason": shadow.get("v2b_recompute_mismatch_reason"),
            "smc_entry_v2b_forward_mode": shadow.get("smc_entry_v2b_forward_mode"),
        }
        os.makedirs("logs", exist_ok=True)
        with open(SMC_ENTRY_V2B_ALLOWLIST_LOG, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(_json_safe_copy(row), ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[SMC_ENTRY_V2B_ALLOWLIST_SHADOW] log failed: {exc}")


def _smc_entry_v2_expected_rr(side, entry, sl, tp):
    if entry is None or sl is None or tp is None:
        return None
    risk = abs(entry - sl)
    if risk <= 0:
        return None
    if side == "LONG":
        reward = tp - entry
    elif side == "SHORT":
        reward = entry - tp
    else:
        return None
    if reward <= 0:
        return None
    return round(reward / risk, 4)


def _smc_entry_v2_fallback_shadow(candidate, fields, now_ts=None):
    try:
        return _paper_smc_research_entry_fallback_shadow_safe(
            candidate,
            fields=fields,
            now_ts=now_ts,
        )
    except Exception:
        return {}


def _smc_entry_v2_shadow(candidate, fields=None, v1_decision="", v1_reason="", trade=None, now_ts=None):
    candidate = candidate if isinstance(candidate, dict) else {}
    trade = trade if isinstance(trade, dict) else {}
    fields = fields if isinstance(fields, dict) else _paper_smc_research_qualified_fields(candidate)
    now_ts = time.time() if now_ts is None else now_ts
    fallback = _smc_entry_v2_fallback_shadow(candidate, fields, now_ts=now_ts)

    side = _smc_entry_v2_text(_first_nonblank(candidate.get("side"), trade.get("side")))
    entry = _smc_entry_v2_float(_first_nonblank(candidate.get("entry"), trade.get("entry")))
    sl = _smc_entry_v2_float(_first_nonblank(candidate.get("sl"), trade.get("sl")))
    tp = _smc_entry_v2_float(_first_nonblank(candidate.get("tp"), trade.get("tp")))
    rr = _smc_entry_v2_float(
        _first_nonblank(fields.get("planned_rr"), candidate.get("rr"), trade.get("rr"))
    )
    expected_rr = rr if rr is not None else _smc_entry_v2_expected_rr(side, entry, sl, tp)

    smc_zone = _smc_entry_v2_text(fields.get("smc_zone"))
    market_regime = _smc_entry_v2_text(fields.get("market_regime"))
    bos_quality = _smc_entry_v2_text(fields.get("bos_quality"))
    exhaustion = _smc_entry_v2_text(fields.get("exhaustion"))
    phase = _smc_entry_v2_text(fields.get("phase"))
    range_context = _smc_entry_v2_text(fields.get("range_context"))
    liquidity_sweep = _smc_entry_v2_text(fields.get("liquidity_sweep"))
    risk_class = _smc_entry_v2_text(fallback.get("research_entry_timing_risk_class"))
    fallback_reason = fallback.get("research_fallback_reason")
    stale_signal = bool(
        isinstance(fallback.get("research_entry_timing_components"), dict)
        and fallback["research_entry_timing_components"].get("stale_signal")
    )

    exact_keys = (
        "premium_discount",
        "dow_trend_context",
        "poi_type",
        "poi_location_quality",
        "entry_poi_alignment",
        "liquidity_context",
    )
    exact_feature_seen = any(
        _smc_entry_v2_text(_first_nonblank(candidate.get(key), fields.get(key)))
        not in {"", "UNKNOWN", "NONE"}
        for key in exact_keys
    )
    feature_quality = "EXACT_FEATURES" if exact_feature_seen else "COARSE_PROXY"

    missing_core = []
    for key, value in (
        ("side", side),
        ("entry", entry),
        ("sl", sl),
        ("tp", tp),
        ("rr", expected_rr),
    ):
        if value in (None, ""):
            missing_core.append(key)

    location_score = 0
    location_reasons = []
    if side == "LONG":
        if smc_zone == "DISCOUNT" or range_context == "RANGE_LOW":
            location_score += 2
            location_reasons.append("long_favorable_discount_or_low")
        if smc_zone == "PREMIUM":
            location_score -= 3
            location_reasons.append("long_in_premium")
        if liquidity_sweep == "SWEEP_LOW":
            location_score += 1
            location_reasons.append("long_sweep_low_reclaim_proxy")
        if liquidity_sweep == "SWEEP_HIGH":
            location_score -= 1
            location_reasons.append("long_into_sweep_high_proxy")
    elif side == "SHORT":
        if smc_zone == "PREMIUM" or range_context == "RANGE_HIGH":
            location_score += 2
            location_reasons.append("short_favorable_premium_or_high")
        if smc_zone == "DISCOUNT":
            location_score -= 3
            location_reasons.append("short_in_discount")
        if liquidity_sweep == "SWEEP_HIGH":
            location_score += 1
            location_reasons.append("short_sweep_high_rejection_proxy")
        if liquidity_sweep == "SWEEP_LOW":
            location_score -= 1
            location_reasons.append("short_into_sweep_low_proxy")
    else:
        location_reasons.append("missing_side")

    retest_tokens = ("RETEST", "PULLBACK", "PRE_BREAK", "PREBREAK", "ACCEPT", "RECLAIM")
    has_retest_proxy = any(token in phase for token in retest_tokens) or risk_class == "PRE_BREAK_ANTICIPATION"
    if has_retest_proxy:
        location_score += 1
        location_reasons.append("retest_or_location_proxy")

    late_chase_score = 0
    late_reasons = []
    if market_regime == "CHOP_NO_TRADE":
        late_chase_score += 3
        late_reasons.append("chop_no_trade")
    if market_regime == "EXHAUSTION_REVERSAL":
        late_chase_score += 3
        late_reasons.append("exhaustion_reversal")
    if exhaustion not in ("", "UNKNOWN", "HEALTHY", "NONE", "NO", "FALSE"):
        late_chase_score += 2
        late_reasons.append(f"exhaustion={exhaustion}")
    if bos_quality in {"NO_FOLLOWTHROUGH", "TRAP"}:
        late_chase_score += 2
        late_reasons.append(f"bos_quality={bos_quality}")
    if risk_class in {"BAD_REGIME_ENTRY", "CHOP_OR_RANGE_ENTRY", "STALE_SIGNAL_ENTRY", "NO_FOLLOWTHROUGH_RISK"}:
        late_chase_score += 2
        late_reasons.append(f"risk_class={risk_class}")
    if stale_signal:
        late_chase_score += 2
        late_reasons.append("stale_signal")

    structure_score = 0
    structure_reasons = []
    if not missing_core:
        if side == "LONG" and sl < entry < tp:
            structure_score += 1
            structure_reasons.append("long_geometry_valid")
        elif side == "SHORT" and tp < entry < sl:
            structure_score += 1
            structure_reasons.append("short_geometry_valid")
        else:
            structure_score -= 2
            structure_reasons.append("invalid_v1_geometry")
    else:
        structure_reasons.append(f"missing_core={','.join(missing_core)}")
    if expected_rr is not None and expected_rr >= 2.0:
        structure_score += 1
        structure_reasons.append("rr_ge_2")
    elif expected_rr is not None:
        structure_score -= 1
        structure_reasons.append("rr_below_2")

    if missing_core:
        status = "UNKNOWN_MISSING_FEATURES"
        reason = f"missing_core_features={','.join(missing_core)}"
        compared = "NOT_COMPUTABLE"
    elif market_regime == "CHOP_NO_TRADE":
        status = "WOULD_SKIP_CHOP"
        reason = "market_regime=CHOP_NO_TRADE"
        compared = "BETTER_LOCATION"
    elif market_regime == "EXHAUSTION_REVERSAL":
        status = "WOULD_SKIP_EXHAUSTION"
        reason = "market_regime=EXHAUSTION_REVERSAL"
        compared = "BETTER_LOCATION"
    elif side == "LONG" and smc_zone == "PREMIUM" and late_chase_score >= 2:
        status = "WOULD_SKIP_BAD_LOCATION"
        reason = "long_in_premium_with_late_or_weak_context"
        compared = "BETTER_LOCATION"
    elif side == "SHORT" and smc_zone == "DISCOUNT" and late_chase_score >= 2:
        status = "WOULD_SKIP_BAD_LOCATION"
        reason = "short_in_discount_with_late_or_weak_context"
        compared = "BETTER_LOCATION"
    elif late_chase_score >= 4:
        status = "WOULD_SKIP_LATE_CHASE"
        reason = ";".join(late_reasons) or "late_chase_proxy"
        compared = "BETTER_LOCATION"
    elif structure_score < 1:
        status = "WOULD_SKIP_NO_STRUCTURE_SL"
        reason = ";".join(structure_reasons) or "structure_proxy_failed"
        compared = "NOT_COMPUTABLE"
    elif expected_rr is None or expected_rr < 2.0:
        status = "WOULD_SKIP_RR"
        reason = f"expected_rr={expected_rr}"
        compared = "NOT_COMPUTABLE"
    elif location_score >= 2 and has_retest_proxy:
        status = "WOULD_ENTER"
        reason = ";".join(location_reasons + structure_reasons)
        compared = "BETTER_LOCATION" if location_score > 0 else "SAME"
    else:
        status = "WAIT_RETEST"
        reason = "needs_retest_or_location_confirmation"
        compared = "SAME" if location_score >= 0 else "BETTER_LOCATION"

    return {
        "smc_entry_v2_shadow_version": SMC_ENTRY_V2_SHADOW_VERSION,
        "v2_shadow_status": status,
        "v2_shadow_reason": reason,
        "v2_location_score": location_score,
        "v2_late_chase_score": late_chase_score,
        "v2_structure_score": structure_score,
        "v2_expected_entry": entry,
        "v2_structural_sl": sl,
        "v2_expected_rr": expected_rr,
        "v2_compared_to_v1": compared,
        "v2_feature_quality": feature_quality,
        "v2_location_reasons": location_reasons,
        "v2_late_chase_reasons": late_reasons,
        "v2_structure_reasons": structure_reasons,
        "v1_decision": v1_decision,
        "v1_reason": v1_reason,
        "v1_entry": entry,
        "v1_sl": sl,
        "v1_rr": rr,
        "smc_zone": fields.get("smc_zone"),
        "market_regime": fields.get("market_regime"),
        "bos_quality": fields.get("bos_quality"),
        "exhaustion": fields.get("exhaustion"),
        "research_entry_timing_risk_class": fallback.get("research_entry_timing_risk_class"),
        "research_fallback_reason": fallback_reason,
    }


def _smc_entry_v2_shadow_write(candidate, fields, shadow, v1_decision="", v1_reason="", now_ts=None):
    try:
        candidate = candidate if isinstance(candidate, dict) else {}
        fields = fields if isinstance(fields, dict) else {}
        shadow = shadow if isinstance(shadow, dict) else {}
        now_ts = time.time() if now_ts is None else now_ts
        row = {
            "ts": now_ts,
            "symbol": candidate.get("symbol"),
            "side": candidate.get("side"),
            "signal_ts": _first_nonblank(
                candidate.get("signal_created_ts"),
                candidate.get("source_timestamp"),
                candidate.get("collector_ts"),
                candidate.get("timestamp"),
            ),
            "v1_decision": v1_decision,
            "v1_reason": v1_reason,
            "v1_entry": shadow.get("v1_entry"),
            "v1_sl": shadow.get("v1_sl"),
            "v1_rr": shadow.get("v1_rr"),
            "smc_zone": fields.get("smc_zone"),
            "market_regime": fields.get("market_regime"),
            "bos_quality": fields.get("bos_quality"),
            "exhaustion": fields.get("exhaustion"),
            "v2_shadow_status": shadow.get("v2_shadow_status"),
            "v2_shadow_reason": shadow.get("v2_shadow_reason"),
            "v2_location_score": shadow.get("v2_location_score"),
            "v2_late_chase_score": shadow.get("v2_late_chase_score"),
            "v2_structure_score": shadow.get("v2_structure_score"),
            "v2_expected_entry": shadow.get("v2_expected_entry"),
            "v2_structural_sl": shadow.get("v2_structural_sl"),
            "v2_expected_rr": shadow.get("v2_expected_rr"),
            "v2_compared_to_v1": shadow.get("v2_compared_to_v1"),
            "v2_feature_quality": shadow.get("v2_feature_quality"),
        }
        os.makedirs("logs", exist_ok=True)
        with open(SMC_ENTRY_V2_SHADOW_LOG, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(_json_safe_copy(row), ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[SMC_ENTRY_V2_SHADOW] log failed: {exc}")


# =====================================================================
# SMC_PA_SCORE_V3_SHADOW (SHADOW / LOG-ONLY)
# ---------------------------------------------------------------------
# Per-component SMC/PA quality annotator for the CONFIRM_SMC_RESEARCH lane
# (SMC_PA_SCORE_MODEL_AUDIT follow-up). Pure instrumentation: it NEVER gates,
# NEVER changes a paper/live decision, never touches risk/cap/A3, SL/MIN_LOCK/
# trailing or any order path, and never fetches network data — the BTC context
# it reads is the independent context already computed by the caller.
# Components are logged raw and separately so each one's predictiveness can be
# measured on realized outcomes before any weight or gate is proposed
# (the audit script enforces an n>=100 rule before recommendations).
# Old degenerate BTC fields (btc_m5/m15/h1_bias_label unified-router copies)
# are NEVER consulted: market bias uses independent BTC fields only, otherwise
# the component is 0 and marked missing.
# =====================================================================
_SMC_PA_V3_VERSION = "smc_pa_score_v3_shadow_v0.1_log_only"
_SMC_PA_V3_LOG = os.path.join("logs", "smc_pa_score_v3_shadow.jsonl")

# Summary fields folded (additively) into the paper/live decision payloads.
# Kept separate so the simulator can strip them and assert the underlying
# decision rows are unchanged.
_SMC_PA_V3_SUMMARY_FIELDS = (
    "smc_pa_v3_total_score",
    "smc_pa_v3_score_band",
    "smc_pa_v3_missing_components",
    "smc_pa_v3_version",
)

_SMC_PA_V3_COMPONENT_FIELDS = (
    "smc_pa_v3_market_bias_score",
    "smc_pa_v3_regime_score",
    "smc_pa_v3_structure_quality_score",
    "smc_pa_v3_liquidity_sweep_score",
    "smc_pa_v3_location_quality_score",
    "smc_pa_v3_breakout_acceptance_score",
    "smc_pa_v3_relative_strength_score",
    "smc_pa_v3_volatility_sl_quality_score",
    "smc_pa_v3_target_realism_score",
    "smc_pa_v3_execution_risk_score",
)

_SMC_PA_V3_STRONG_MIN = 4.0
_SMC_PA_V3_OK_MIN = 1.0
_SMC_PA_V3_WEAK_MIN = -2.0
# More missing components than this -> banding is not meaningful.
_SMC_PA_V3_MAX_MISSING_FOR_BANDING = 4
_SMC_PA_V3_EXEC_RISK_AGE_FRACTION = 0.7

_SMC_PA_V3_EXPANSION_REGIMES = {"TRENDING_CONTINUATION", "BREAKOUT_EXPANSION"}
_SMC_PA_V3_STRUCTURE_POSITIVE = {
    "STRONG", "CONFIRM", "TRUE", "CONFIRMED", "CLOSE_THROUGH", "DISPLACEMENT",
}
_SMC_PA_V3_PREBREAK_TOKENS = (
    "PRE_BREAK", "PREBREAK", "RETEST", "PULLBACK", "RECLAIM", "ACCEPT",
)
_SMC_PA_V3_LATE_EXHAUSTION = {"EXTENDED", "EXHAUSTED", "COLLAPSING"}


def _smc_pa_v3_text(value):
    return str(value or "").strip().upper()


def _smc_pa_v3_float(value):
    try:
        if value in (None, ""):
            return None
        out = float(value)
        if out != out:
            return None
        return out
    except (TypeError, ValueError):
        return None


def _smc_pa_v3_source_value(source, *keys):
    for key in keys:
        value = source.get(key)
        if value not in (None, ""):
            return value
    return None


def _smc_pa_score_v3_eval(source, side=None, btc_ctx=None, stale_info=None):
    """Pure SMC_PA_SCORE_V3 component evaluator. LOG-ONLY; no I/O, no network.

    Reads only the merged decision-context dict (candidate/fields/trade) and
    the already-computed independent BTC context. Never mutates its inputs.
    Returns all component scores separately plus total/band/missing list.
    """
    source = source if isinstance(source, dict) else {}
    btc_ctx = btc_ctx if isinstance(btc_ctx, dict) else {}
    stale_info = stale_info if isinstance(stale_info, dict) else {}
    side = _smc_pa_v3_text(side or source.get("side"))

    missing = []
    reasons = {}

    smc_zone = _smc_pa_v3_text(_smc_pa_v3_source_value(source, "smc_zone"))
    market_regime = _smc_pa_v3_text(
        _smc_pa_v3_source_value(source, "market_regime", "market_regime_at_entry", "regime")
    )
    bos_quality = _smc_pa_v3_text(_smc_pa_v3_source_value(source, "bos_quality"))
    phase = _smc_pa_v3_text(_smc_pa_v3_source_value(source, "phase"))
    liquidity_sweep = _smc_pa_v3_text(_smc_pa_v3_source_value(source, "liquidity_sweep"))
    exhaustion = _smc_pa_v3_text(
        _smc_pa_v3_source_value(source, "exhaustion", "exhaustion_state")
    )
    volume_confirmation = _smc_pa_v3_text(_smc_pa_v3_source_value(source, "volume_confirmation"))
    trend_direction = _smc_pa_v3_text(_smc_pa_v3_source_value(source, "trend_direction"))

    entry = _smc_pa_v3_float(
        _smc_pa_v3_source_value(source, "entry", "entry_real", "entry_price")
    )
    sl = _smc_pa_v3_float(_smc_pa_v3_source_value(source, "sl", "stop_loss"))
    tp = _smc_pa_v3_float(_smc_pa_v3_source_value(source, "tp", "take_profit"))
    planned_rr = _smc_pa_v3_float(_smc_pa_v3_source_value(source, "planned_rr", "rr"))
    atr = _smc_pa_v3_float(
        _smc_pa_v3_source_value(source, "atr", "atr_m15", "atr_15m", "atr14", "atr_value")
    )

    # ── 1. market_bias_score: INDEPENDENT BTC fields only ────────────────
    # Degenerate unified-router labels (btc_m5/m15/h1_bias_label,
    # btc_mtf_summary_label) are intentionally never read here.
    market_bias_score = 0
    bias_source = "NONE"
    bias_value = "UNKNOWN"
    independent_bias = _smc_pa_v3_text(btc_ctx.get("btc_bias_independent"))
    independent_quality = _smc_pa_v3_text(btc_ctx.get("btc_context_quality"))
    independent_alignment = _smc_pa_v3_text(btc_ctx.get("btc_alignment_independent"))
    mtf_alignment = _smc_pa_v3_text(btc_ctx.get("btc_mtf_alignment"))
    mtf_data_mode = _smc_pa_v3_text(btc_ctx.get("btc_data_mode"))
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

    # ── 2. regime_score ───────────────────────────────────────────────────
    regime_score = 0
    if market_regime == "RANGE_MEAN_REVERSION":
        regime_score = 1
        reasons["regime"] = "range_mean_reversion"
    elif market_regime in ("CHOP_NO_TRADE", "EXHAUSTION_REVERSAL"):
        regime_score = -2
        reasons["regime"] = f"bad_regime={market_regime}"
    elif market_regime in _SMC_PA_V3_EXPANSION_REGIMES:
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

    # ── 3. structure_quality_score ────────────────────────────────────────
    structure_quality_score = 0
    if bos_quality == "TRAP":
        structure_quality_score = 2
        reasons["structure_quality"] = "trap_sweep_reclaim_like"
    elif bos_quality in _SMC_PA_V3_STRUCTURE_POSITIVE:
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

    # ── 4. liquidity_sweep_score ──────────────────────────────────────────
    # The SWEEP_HIGH/SWEEP_LOW annotation (compute_smc_context) already
    # requires the sweep candle to close back inside the level, so the
    # reclaim condition is embedded in the label itself.
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

    # ── 5. location_quality_score ─────────────────────────────────────────
    bullish_expansion = (
        market_regime in _SMC_PA_V3_EXPANSION_REGIMES and trend_direction == "LONG"
    )
    bearish_expansion = (
        market_regime in _SMC_PA_V3_EXPANSION_REGIMES and trend_direction == "SHORT"
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

    # ── 6. breakout_acceptance_score (proxy) ──────────────────────────────
    # Real N-bar follow-through instrumentation does not exist yet; this is a
    # proxy from phase/bos_quality and is always flagged as missing the real
    # follow-through measurement.
    missing.append("breakout_acceptance_followthrough_bars")
    stale_resolved = stale_info.get("candidate_time_source") not in (None, "", "missing")
    is_stale = bool(stale_resolved and stale_info.get("is_stale"))
    late_chase_context = is_stale or exhaustion in _SMC_PA_V3_LATE_EXHAUSTION
    breakout_acceptance_score = 0
    if bos_quality == "NO_FOLLOWTHROUGH":
        breakout_acceptance_score = -2
        reasons["breakout_acceptance"] = "no_followthrough"
    elif phase == "BREAKOUT_STRONG" and late_chase_context:
        breakout_acceptance_score = -2
        reasons["breakout_acceptance"] = "breakout_strong_late_chase"
    elif any(token in phase for token in _SMC_PA_V3_PREBREAK_TOKENS):
        breakout_acceptance_score = 1
        reasons["breakout_acceptance"] = f"pre_break_or_retest_phase={phase}"
    elif phase in ("", "UNKNOWN"):
        reasons["breakout_acceptance"] = "phase_unavailable_proxy_neutral"
    else:
        reasons["breakout_acceptance"] = f"neutral_phase={phase}"

    # ── 7. relative_strength_score ────────────────────────────────────────
    # Never inferred from trade side or degenerate fields.
    relative_strength_score = 0
    rs_value = _smc_pa_v3_float(
        _smc_pa_v3_source_value(
            source,
            "relative_strength_alt_vs_btc",
            "alt_vs_btc_rs",
            "relative_strength_ratio",
        )
    )
    if rs_value is None:
        missing.append("relative_strength")
        reasons["relative_strength"] = "alt_vs_btc_rs_unavailable|NEED_RS_INSTRUMENTATION"
    elif side == "LONG":
        relative_strength_score = 2 if rs_value > 0 else (-2 if rs_value < 0 else 0)
        reasons["relative_strength"] = f"rs={rs_value}"
    elif side == "SHORT":
        relative_strength_score = 2 if rs_value < 0 else (-2 if rs_value > 0 else 0)
        reasons["relative_strength"] = f"rs={rs_value}"
    else:
        missing.append("relative_strength")
        reasons["relative_strength"] = "missing_side"

    # ── 8. volatility_sl_quality_score ────────────────────────────────────
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

    # ── 9. target_realism_score ───────────────────────────────────────────
    target_realism_score = 0
    opposing_r = _smc_pa_v3_float(
        _smc_pa_v3_source_value(source, "opposing_barrier_distance_r", "opposing_distance_r")
    )
    if opposing_r is None and entry is not None and sl_dist not in (None, 0):
        resistance = _smc_pa_v3_float(
            _smc_pa_v3_source_value(
                source, "nearest_htf_resistance", "m15_swing_high", "range_high"
            )
        )
        support = _smc_pa_v3_float(
            _smc_pa_v3_source_value(
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

    # ── 10. execution_risk_score ──────────────────────────────────────────
    execution_risk_score = 0
    age_secs = _smc_pa_v3_float(stale_info.get("candidate_age_secs"))
    max_age_secs = _smc_pa_v3_float(stale_info.get("candidate_max_age_secs"))
    if not stale_resolved:
        missing.append("execution_risk_signal_age")
        reasons["execution_risk"] = "signal_timestamp_unavailable"
    elif is_stale or (
        age_secs is not None
        and max_age_secs not in (None, 0)
        and age_secs >= max_age_secs * _SMC_PA_V3_EXEC_RISK_AGE_FRACTION
    ):
        execution_risk_score = -1
        reasons["execution_risk"] = (
            f"signal_age_near_or_past_stale|age={age_secs}|max_age={max_age_secs}"
        )
    else:
        reasons["execution_risk"] = f"fresh|age={age_secs}"

    total = (
        market_bias_score
        + regime_score
        + structure_quality_score
        + liquidity_sweep_score
        + location_quality_score
        + breakout_acceptance_score
        + relative_strength_score
        + volatility_sl_quality_score
        + target_realism_score
        + execution_risk_score
    )

    if len(missing) > _SMC_PA_V3_MAX_MISSING_FOR_BANDING:
        band = "V3_UNKNOWN_TOO_MISSING"
    elif total >= _SMC_PA_V3_STRONG_MIN:
        band = "V3_STRONG"
    elif total >= _SMC_PA_V3_OK_MIN:
        band = "V3_OK"
    elif total >= _SMC_PA_V3_WEAK_MIN:
        band = "V3_WEAK"
    else:
        band = "V3_REJECT_LIKE"

    return {
        "smc_pa_v3_version": _SMC_PA_V3_VERSION,
        "smc_pa_v3_total_score": round(float(total), 4),
        "smc_pa_v3_score_band": band,
        "smc_pa_v3_market_bias_score": market_bias_score,
        "smc_pa_v3_regime_score": regime_score,
        "smc_pa_v3_structure_quality_score": structure_quality_score,
        "smc_pa_v3_liquidity_sweep_score": liquidity_sweep_score,
        "smc_pa_v3_location_quality_score": location_quality_score,
        "smc_pa_v3_breakout_acceptance_score": breakout_acceptance_score,
        "smc_pa_v3_relative_strength_score": relative_strength_score,
        "smc_pa_v3_volatility_sl_quality_score": volatility_sl_quality_score,
        "smc_pa_v3_target_realism_score": target_realism_score,
        "smc_pa_v3_execution_risk_score": execution_risk_score,
        "smc_pa_v3_missing_components": list(missing),
        "smc_pa_v3_component_reasons": dict(reasons),
        "smc_pa_v3_bias_source": bias_source,
        "smc_pa_v3_bias_value": bias_value,
        # Source snapshot used by the evaluator (for later audits).
        "smc_pa_v3_src_smc_zone": smc_zone,
        "smc_pa_v3_src_market_regime": market_regime,
        "smc_pa_v3_src_bos_quality": bos_quality,
        "smc_pa_v3_src_phase": phase,
        "smc_pa_v3_src_liquidity_sweep": liquidity_sweep,
        "smc_pa_v3_src_exhaustion": exhaustion,
        "smc_pa_v3_src_volume_confirmation": volume_confirmation,
        "smc_pa_v3_src_trend_direction": trend_direction,
        "smc_pa_v3_src_atr": atr,
        "smc_pa_v3_src_sl_atr_ratio": sl_atr_ratio,
        "smc_pa_v3_src_opposing_barrier_distance_r": opposing_r,
        "smc_pa_v3_src_planned_rr": planned_rr,
        "smc_pa_v3_src_candidate_age_secs": age_secs,
        "smc_pa_v3_src_entry": entry,
        "smc_pa_v3_src_sl": sl,
        "smc_pa_v3_src_tp": tp,
    }


def _smc_pa_score_v3_shadow_write(row):
    try:
        os.makedirs("logs", exist_ok=True)
        with open(_SMC_PA_V3_LOG, "a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(_json_safe_copy(row), ensure_ascii=False, default=str, sort_keys=True)
                + "\n"
            )
    except Exception as exc:
        print(f"[SMC_PA_SCORE_V3_SHADOW] log failed: {exc}")


def _smc_pa_score_v3_shadow(
    candidate,
    fields=None,
    trade=None,
    execution_mode="",
    v1_decision="",
    v1_reason="",
    btc_ctx=None,
    now_ts=None,
):
    """Assemble + append one SMC_PA_SCORE_V3 shadow row (forward log) and
    return the additive summary fields for the decision payload.

    SHADOW / LOG-ONLY. Never mutates candidate/fields/trade, never raises into
    the caller, never fetches network data, never touches any decision, risk,
    SL/order or gate path. Returns {} on any failure.
    """
    try:
        candidate = candidate if isinstance(candidate, dict) else {}
        fields = fields if isinstance(fields, dict) else {}
        trade = trade if isinstance(trade, dict) else {}
        btc_ctx = btc_ctx if isinstance(btc_ctx, dict) else {}
        now_ts = time.time() if now_ts is None else now_ts

        entry_type = _smc_pa_v3_text(
            _first_nonblank(
                trade.get("entry_type"),
                candidate.get("entry_type"),
                "CONFIRM_SMC_RESEARCH",
            )
        )
        if entry_type not in ("CONFIRM", "CONFIRM_SMC_RESEARCH"):
            return {}

        source = {}
        source.update(candidate)
        source.update({k: v for k, v in fields.items() if v not in (None, "")})
        source.update({k: v for k, v in trade.items() if v not in (None, "")})

        # Log-only fallback: reuse the level/ATR context already carried by
        # the candidate (confirm_entry_acceptance_context) for components
        # whose flat source fields are absent. Only fills gaps in the local
        # merged copy — never overwrites an existing value and never mutates
        # candidate/fields/trade.
        _v3_acceptance_ctx = source.get("confirm_entry_acceptance_context")
        _v3_acceptance_ctx = _v3_acceptance_ctx if isinstance(_v3_acceptance_ctx, dict) else {}
        ctx_fallback_used = []
        for _ctx_key in ("atr", "nearest_htf_support", "nearest_htf_resistance"):
            if source.get(_ctx_key) in (None, "") and _v3_acceptance_ctx.get(_ctx_key) not in (None, ""):
                source[_ctx_key] = _v3_acceptance_ctx.get(_ctx_key)
                ctx_fallback_used.append(_ctx_key)

        side = _smc_pa_v3_text(_first_nonblank(candidate.get("side"), trade.get("side")))
        stale_info = _paper_smc_research_stale_info(candidate, now_ts)
        shadow = _smc_pa_score_v3_eval(
            source, side=side, btc_ctx=btc_ctx, stale_info=stale_info
        )

        row = {
            "ts": now_ts,
            "timestamp": format_vn_time(now_ts),
            "event_type": "SMC_PA_SCORE_V3_SHADOW",
            "execution_mode": execution_mode,
            "symbol": str(_first_nonblank(candidate.get("symbol"), trade.get("symbol")) or ""),
            "side": side,
            "signal_ts": _first_nonblank(
                candidate.get("signal_created_ts"),
                candidate.get("source_timestamp"),
                candidate.get("collector_ts"),
                candidate.get("timestamp"),
            ),
            "dedup_key": str(candidate.get("dedup_key") or ""),
            "entry_type": entry_type,
            "v1_decision": v1_decision,
            "v1_reason": v1_reason,
            "old_score": _smc_pa_v3_float(
                _first_nonblank(trade.get("score"), candidate.get("score"))
            ),
            "entry": _smc_pa_v3_float(
                _first_nonblank(trade.get("entry"), candidate.get("entry"))
            ),
            "sl": _smc_pa_v3_float(_first_nonblank(trade.get("sl"), candidate.get("sl"))),
            "tp": _smc_pa_v3_float(_first_nonblank(trade.get("tp"), candidate.get("tp"))),
            "rr": _smc_pa_v3_float(
                _first_nonblank(
                    fields.get("planned_rr"), trade.get("rr"), candidate.get("rr")
                )
            ),
            # PAPER_LOCATION_GATE context (read-only passthrough, if available)
            "paper_location_gate_would_block": source.get(
                "confirm_smc_entry_location_would_block"
            ),
            "paper_location_gate_primary_reason": source.get(
                "confirm_smc_entry_location_primary_reason"
            ),
            "paper_location_gate_risk_bucket": source.get(
                "confirm_smc_entry_location_risk_bucket"
            ),
            # V2B allowlist context (read-only passthrough, if available)
            "v2b_label": source.get("v2b_label"),
            "v2b_match": source.get("v2b_match"),
            "v2b_reason": source.get("v2b_reason"),
            "v2b_market_bias": source.get("v2b_market_bias"),
            "v2b_direction_alignment": source.get("v2b_direction_alignment"),
            # BTC bias side-enable context (read-only passthrough, if available)
            "btc_side_enable_shadow_label": btc_ctx.get("btc_side_enable_shadow_label"),
            "btc_side_enable_shadow_allow": btc_ctx.get("btc_side_enable_shadow_allow"),
            "btc_bias_independent": btc_ctx.get("btc_bias_independent"),
            "btc_alignment_independent": btc_ctx.get("btc_alignment_independent"),
            "btc_context_quality": btc_ctx.get("btc_context_quality"),
            "btc_mtf_alignment": btc_ctx.get("btc_mtf_alignment"),
            "btc_data_mode": btc_ctx.get("btc_data_mode"),
            # Which component inputs were filled from the candidate's
            # confirm_entry_acceptance_context (log-only provenance).
            "smc_pa_v3_ctx_fallback_used": list(ctx_fallback_used),
            "smc_pa_v3_ctx_fallback_source": _v3_acceptance_ctx.get("context_source"),
        }
        row.update(shadow)
        _smc_pa_score_v3_shadow_write(row)
        return {key: shadow.get(key) for key in _SMC_PA_V3_SUMMARY_FIELDS}
    except Exception as exc:
        print(f"[SMC_PA_SCORE_V3_SHADOW] shadow failed: {exc}")
        return {}


# =====================================================================
# BREAKOUT_ACCEPTANCE_SHADOW (SHADOW / LOG-ONLY)
# ---------------------------------------------------------------------
# Breakout acceptance instrumentation for the CONFIRM_SMC_RESEARCH lane
# (BOS follow-through research: NO_FOLLOWTHROUGH is the largest losing
# class and current BOS is effectively single-candle/immediate). Pure
# instrumentation: it NEVER gates, NEVER changes a paper/live decision,
# never touches risk/cap/A3, SL/MIN_LOCK/trailing or any order path,
# never fetches network data, and never adds fields to any decision
# payload — it only appends rows to its own forward log so future audits
# can test whether BOS entries should require acceptance / hold / retest
# confirmation.
# Runtime N-bar lifecycle tracking is intentionally NOT wired into the
# candle update loops (too close to decision paths for a shadow); rows
# are written at decision time with lifecycle fields = None and
# lifecycle_tracking = "MISSING_RUNTIME_DEFERRED_TO_AUDIT".
# scripts/debug/audit_breakout_acceptance_shadow.py reconstructs an
# approximate lifecycle from logs/confirm_structural_outcomes.jsonl.
# The pure evaluator below accepts optional follow bars so the simulator
# (and any offline reconstruction) can compute the terminal labels.
# =====================================================================
_BREAKOUT_ACCEPT_VERSION = "breakout_acceptance_shadow_v0.1_log_only"
_BREAKOUT_ACCEPT_LOG = os.path.join("logs", "breakout_acceptance_shadow.jsonl")
# Same retest tolerance family as entry._detect_retest (0.5% of level).
_BREAKOUT_ACCEPT_RETEST_TOL = 0.005
_BREAKOUT_ACCEPT_MAX_BARS = 3

_BREAKOUT_LABEL_ACCEPTED = "BREAKOUT_ACCEPTED"
_BREAKOUT_LABEL_RETEST_HELD = "BREAKOUT_RETEST_HELD"
_BREAKOUT_LABEL_FAILED_BACK_INSIDE = "BREAKOUT_FAILED_BACK_INSIDE"
_BREAKOUT_LABEL_WICK_REJECTED = "BREAKOUT_WICK_REJECTED"
_BREAKOUT_LABEL_NO_FOLLOWTHROUGH = "BREAKOUT_NO_FOLLOWTHROUGH"
_BREAKOUT_LABEL_UNKNOWN_MISSING_LEVEL = "BREAKOUT_UNKNOWN_MISSING_LEVEL"
# Non-terminal label used at decision time when the candle closed through
# the level but no (or not enough) follow bars have been observed yet.
_BREAKOUT_LABEL_PENDING_LIFECYCLE = "BREAKOUT_PENDING_LIFECYCLE"


def _breakout_acceptance_eval(side, breakout_level, signal_candle=None,
                              follow_bars=None, entry=None, sl=None):
    """Pure breakout-acceptance classifier. LOG-ONLY; no I/O, no network.

    signal_candle: OHLC dict of the BOS/breakout signal candle.
    follow_bars: optional list of OHLC dicts AFTER the signal candle (oldest
    first; only the first _BREAKOUT_ACCEPT_MAX_BARS are consumed). Production
    decision-time calls pass no follow bars (runtime lifecycle is deferred to
    the audit reconstruction); the simulator / offline reconstruction pass
    bars to obtain the terminal labels. Never mutates its inputs.
    """
    side = _smc_pa_v3_text(side)
    level = _smc_pa_v3_float(breakout_level)
    candle = signal_candle if isinstance(signal_candle, dict) else {}
    bars = [bar for bar in (follow_bars or []) if isinstance(bar, dict)]
    bars = bars[:_BREAKOUT_ACCEPT_MAX_BARS]

    entry_val = _smc_pa_v3_float(entry)
    sl_val = _smc_pa_v3_float(sl)
    risk = None
    if entry_val is not None and sl_val is not None:
        risk_abs = abs(entry_val - sl_val)
        if risk_abs > 0:
            risk = risk_abs

    out = {
        "breakout_acceptance_version": _BREAKOUT_ACCEPT_VERSION,
        "breakout_level_value": level,
        "signal_candle_open": _smc_pa_v3_float(candle.get("open")),
        "signal_candle_high": _smc_pa_v3_float(candle.get("high")),
        "signal_candle_low": _smc_pa_v3_float(candle.get("low")),
        "signal_candle_close": _smc_pa_v3_float(candle.get("close")),
        "entry_distance_from_level_pct": None,
        "close_distance_from_level_pct": None,
        "close_beyond_level": None,
        "wick_rejection": None,
        "retest_candidate": None,
        "follow_bars_observed": len(bars),
        "acceptance_1bar": None,
        "acceptance_2bar": None,
        "acceptance_3bar": None,
        "held_breakout_level": None,
        "failed_back_inside_level": None,
        "retest_held": None,
        "retest_failed": None,
        "max_favorable_r_after_3bars": None,
        "max_adverse_r_after_3bars": None,
        "time_to_0_25r_bars": None,
        "time_to_0_5r_bars": None,
        "breakout_acceptance_label": _BREAKOUT_LABEL_UNKNOWN_MISSING_LEVEL,
    }

    c_close = out["signal_candle_close"]
    if side not in ("LONG", "SHORT") or level is None or level <= 0 or c_close is None:
        return out

    c_high = out["signal_candle_high"]
    c_low = out["signal_candle_low"]

    if entry_val is not None:
        out["entry_distance_from_level_pct"] = (entry_val - level) / level
    out["close_distance_from_level_pct"] = (c_close - level) / level

    if side == "LONG":
        closed_through = c_close > level
        wick_broke = c_high is not None and c_high > level
    else:
        closed_through = c_close < level
        wick_broke = c_low is not None and c_low < level
    wick_rejection = bool(wick_broke and not closed_through)
    retest_candidate = bool(
        closed_through and abs(c_close - level) / level <= _BREAKOUT_ACCEPT_RETEST_TOL
    )
    out["close_beyond_level"] = closed_through
    out["wick_rejection"] = wick_rejection
    out["retest_candidate"] = retest_candidate

    bar_closes = []
    bar_highs = []
    bar_lows = []
    for bar in bars:
        bar_closes.append(_smc_pa_v3_float(bar.get("close")))
        bar_highs.append(_smc_pa_v3_float(bar.get("high")))
        bar_lows.append(_smc_pa_v3_float(bar.get("low")))

    def _close_beyond(value):
        if value is None:
            return None
        if side == "LONG":
            return value > level
        return value < level

    beyond_flags = [_close_beyond(value) for value in bar_closes]
    for idx, key in enumerate(("acceptance_1bar", "acceptance_2bar", "acceptance_3bar")):
        if idx < len(beyond_flags):
            out[key] = beyond_flags[idx]

    known_flags = [flag for flag in beyond_flags if flag is not None]
    if known_flags:
        held = all(known_flags)
        out["held_breakout_level"] = held
        out["failed_back_inside_level"] = bool(closed_through and not held)

        touched = False
        for bar_high, bar_low in zip(bar_highs, bar_lows):
            if side == "LONG":
                if bar_low is not None and bar_low <= level * (1.0 + _BREAKOUT_ACCEPT_RETEST_TOL):
                    touched = True
            else:
                if bar_high is not None and bar_high >= level * (1.0 - _BREAKOUT_ACCEPT_RETEST_TOL):
                    touched = True
        out["retest_held"] = bool(closed_through and touched and held)
        out["retest_failed"] = bool(closed_through and touched and not held)

    if risk is not None and entry_val is not None and bars:
        fav_extremes = bar_highs if side == "LONG" else bar_lows
        adv_extremes = bar_lows if side == "LONG" else bar_highs
        max_fav = None
        max_adv = None
        for bar_index, (fav, adv) in enumerate(zip(fav_extremes, adv_extremes), start=1):
            if fav is not None:
                if side == "LONG":
                    fav_r = (fav - entry_val) / risk
                else:
                    fav_r = (entry_val - fav) / risk
                if max_fav is None or fav_r > max_fav:
                    max_fav = fav_r
                if out["time_to_0_25r_bars"] is None and fav_r >= 0.25:
                    out["time_to_0_25r_bars"] = bar_index
                if out["time_to_0_5r_bars"] is None and fav_r >= 0.5:
                    out["time_to_0_5r_bars"] = bar_index
            if adv is not None:
                if side == "LONG":
                    adv_r = (entry_val - adv) / risk
                else:
                    adv_r = (adv - entry_val) / risk
                if max_adv is None or adv_r > max_adv:
                    max_adv = adv_r
        out["max_favorable_r_after_3bars"] = max_fav
        out["max_adverse_r_after_3bars"] = max_adv

    if wick_rejection:
        label = _BREAKOUT_LABEL_WICK_REJECTED
    elif not closed_through:
        # BOS claimed a breakout but the signal candle never closed through
        # the level and never even wicked through it: no follow-through
        # evidence at all.
        label = _BREAKOUT_LABEL_NO_FOLLOWTHROUGH
    elif not known_flags:
        label = _BREAKOUT_LABEL_PENDING_LIFECYCLE
    elif out["failed_back_inside_level"]:
        label = _BREAKOUT_LABEL_FAILED_BACK_INSIDE
    elif out["retest_held"]:
        label = _BREAKOUT_LABEL_RETEST_HELD
    elif out["held_breakout_level"] and len(known_flags) >= 2:
        label = _BREAKOUT_LABEL_ACCEPTED
    else:
        label = _BREAKOUT_LABEL_PENDING_LIFECYCLE
    out["breakout_acceptance_label"] = label
    return out


def _breakout_acceptance_shadow_write(row):
    try:
        os.makedirs("logs", exist_ok=True)
        with open(_BREAKOUT_ACCEPT_LOG, "a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(_json_safe_copy(row), ensure_ascii=False, default=str, sort_keys=True)
                + "\n"
            )
    except Exception as exc:
        print(f"[BREAKOUT_ACCEPTANCE_SHADOW] log failed: {exc}")


def _breakout_acceptance_shadow(
    candidate,
    fields=None,
    trade=None,
    execution_mode="",
    v1_decision="",
    v1_reason="",
    btc_ctx=None,
    v3_summary=None,
    now_ts=None,
):
    """Assemble + append one BREAKOUT_ACCEPTANCE_SHADOW forward-log row.

    SHADOW / LOG-ONLY. Never mutates candidate/fields/trade/btc_ctx/
    v3_summary, never raises into the caller, never fetches network data,
    never touches any decision, risk, SL/order or gate path, and never adds
    fields to any decision payload (callers must ignore the return value in
    production). Returns the logged row for the simulator, {} on any failure.
    """
    try:
        candidate = candidate if isinstance(candidate, dict) else {}
        fields = fields if isinstance(fields, dict) else {}
        trade = trade if isinstance(trade, dict) else {}
        btc_ctx = btc_ctx if isinstance(btc_ctx, dict) else {}
        v3_summary = v3_summary if isinstance(v3_summary, dict) else {}
        now_ts = time.time() if now_ts is None else now_ts

        entry_type = _smc_pa_v3_text(
            _first_nonblank(
                trade.get("entry_type"),
                candidate.get("entry_type"),
                "CONFIRM_SMC_RESEARCH",
            )
        )
        if entry_type not in ("CONFIRM", "CONFIRM_SMC_RESEARCH"):
            return {}

        source = {}
        source.update(candidate)
        source.update({k: v for k, v in fields.items() if v not in (None, "")})
        source.update({k: v for k, v in trade.items() if v not in (None, "")})

        side = _smc_pa_v3_text(_first_nonblank(candidate.get("side"), trade.get("side")))
        acceptance_ctx = source.get("confirm_entry_acceptance_context")
        acceptance_ctx = acceptance_ctx if isinstance(acceptance_ctx, dict) else {}

        bos_level = _smc_pa_v3_float(
            _smc_pa_v3_source_value(source, "bos_level", "bos_price")
        )
        # Same precedence as before; additionally records which source the
        # level came from and the exact reason when it is missing (log-only).
        breakout_level = None
        level_source = None
        level_missing_reason = None
        for _level_source_name, _level_raw in (
            ("confirm_entry_acceptance_context.break_level", acceptance_ctx.get("break_level")),
            ("source.breakout_level", source.get("breakout_level")),
            ("source.bos_level", source.get("bos_level")),
            ("confirm_entry_acceptance_context.pre_break_level", acceptance_ctx.get("pre_break_level")),
        ):
            if _level_raw not in (None, ""):
                breakout_level = _smc_pa_v3_float(_level_raw)
                if breakout_level is not None:
                    level_source = _level_source_name
                else:
                    level_missing_reason = f"level_value_unparsable:{_level_source_name}"
                break
        if breakout_level is None and level_missing_reason is None:
            if not acceptance_ctx:
                level_missing_reason = "acceptance_context_absent_and_no_source_level_fields"
            else:
                level_missing_reason = "acceptance_context_present_but_break_level_null"
        level_available = breakout_level is not None

        entry_price = _smc_pa_v3_float(
            _first_nonblank(trade.get("entry"), candidate.get("entry"))
        )
        sl = _smc_pa_v3_float(_first_nonblank(trade.get("sl"), candidate.get("sl")))
        tp = _smc_pa_v3_float(_first_nonblank(trade.get("tp"), candidate.get("tp")))
        rr = _smc_pa_v3_float(
            _first_nonblank(
                fields.get("planned_rr"), trade.get("rr"), candidate.get("rr")
            )
        )

        signal_candle = {
            "open": acceptance_ctx.get("candle_open"),
            "high": acceptance_ctx.get("candle_high"),
            "low": acceptance_ctx.get("candle_low"),
            "close": acceptance_ctx.get("candle_close"),
        }
        eval_fields = _breakout_acceptance_eval(
            side,
            breakout_level,
            signal_candle=signal_candle,
            follow_bars=None,
            entry=entry_price,
            sl=sl,
        )

        row = {
            "ts": now_ts,
            "timestamp": format_vn_time(now_ts),
            "event_type": "BREAKOUT_ACCEPTANCE_SHADOW",
            "execution_mode": execution_mode,
            "symbol": str(_first_nonblank(candidate.get("symbol"), trade.get("symbol")) or ""),
            "side": side,
            "signal_ts": _first_nonblank(
                candidate.get("signal_created_ts"),
                candidate.get("source_timestamp"),
                candidate.get("collector_ts"),
                candidate.get("timestamp"),
            ),
            "dedup_key": str(candidate.get("dedup_key") or ""),
            "entry_type": entry_type,
            "v1_decision": v1_decision,
            "v1_reason": v1_reason,
            "entry": entry_price,
            "entry_price": entry_price,
            "sl": sl,
            "tp": tp,
            "rr": rr,
            "bos_level": bos_level,
            "breakout_level": breakout_level,
            "level_source": level_source,
            "level_available": level_available,
            "level_missing_reason": level_missing_reason,
            "signal_candle_available": _smc_pa_v3_float(acceptance_ctx.get("candle_close")) is not None,
            "level_context_source": acceptance_ctx.get("context_source"),
            "phase": _smc_pa_v3_text(_smc_pa_v3_source_value(source, "phase")),
            "bos_quality": _smc_pa_v3_text(_smc_pa_v3_source_value(source, "bos_quality")),
            "market_regime": _smc_pa_v3_text(
                _smc_pa_v3_source_value(source, "market_regime", "market_regime_at_entry", "regime")
            ),
            "smc_zone": _smc_pa_v3_text(_smc_pa_v3_source_value(source, "smc_zone")),
            "old_score": _smc_pa_v3_float(
                _first_nonblank(trade.get("score"), candidate.get("score"))
            ),
            # BTC context (read-only passthrough, if available)
            "btc_bias_independent": btc_ctx.get("btc_bias_independent"),
            "btc_alignment_independent": btc_ctx.get("btc_alignment_independent"),
            "btc_context_quality": btc_ctx.get("btc_context_quality"),
            "btc_side_enable_shadow_label": btc_ctx.get("btc_side_enable_shadow_label"),
            "btc_side_enable_shadow_allow": btc_ctx.get("btc_side_enable_shadow_allow"),
            # SMC_PA_SCORE_V3 (read-only passthrough, if available)
            "smc_pa_v3_total_score": _first_nonblank(
                v3_summary.get("smc_pa_v3_total_score"), source.get("smc_pa_v3_total_score")
            ),
            "smc_pa_v3_score_band": _first_nonblank(
                v3_summary.get("smc_pa_v3_score_band"), source.get("smc_pa_v3_score_band")
            ),
            "smc_pa_v3_version": _first_nonblank(
                v3_summary.get("smc_pa_v3_version"), source.get("smc_pa_v3_version")
            ),
            # PAPER_LOCATION_GATE context (read-only passthrough, if available)
            "paper_location_gate_would_block": source.get(
                "confirm_smc_entry_location_would_block"
            ),
            "paper_location_gate_primary_reason": source.get(
                "confirm_smc_entry_location_primary_reason"
            ),
            "paper_location_gate_risk_bucket": source.get(
                "confirm_smc_entry_location_risk_bucket"
            ),
            # V2B allowlist context (read-only passthrough, if available)
            "v2b_label": source.get("v2b_label"),
            "v2b_match": source.get("v2b_match"),
            "v2b_reason": source.get("v2b_reason"),
            # Runtime N-bar lifecycle is not tracked in production paths;
            # the audit script reconstructs it offline.
            "lifecycle_tracking": "MISSING_RUNTIME_DEFERRED_TO_AUDIT",
        }
        row.update(eval_fields)
        _breakout_acceptance_shadow_write(row)
        return row
    except Exception as exc:
        print(f"[BREAKOUT_ACCEPTANCE_SHADOW] shadow failed: {exc}")
        return {}


def _paper_smc_research_qualified_opened_total():
    path = _paper_smc_research_qualified_log_path()
    if not os.path.exists(path):
        return 0
    total = 0
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if (
                    row.get("event_type") == "PAPER_SMC_RESEARCH_QUALIFIED_DECISION"
                    and bool(row.get("qualified"))
                    and row.get("decision") == "OPEN"
                    and row.get("opened_trade_id") not in (None, "")
                ):
                    total += 1
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH QUALIFIED] cap log read failed: {exc}")
    return total


_CONFIRM_SMC_RESEARCH_V2_SUBGROUP = "CONFIRM_SMC_RESEARCH__SHORT_TREND_ALIGNED"
_CONFIRM_SMC_RESEARCH_V2_BAD_REGIMES = {
    "CHOP_NO_TRADE",
    "RANGE_MEAN_REVERSION",
    "EXHAUSTION_REVERSAL",
}

_CONFIRM_SMC_RESEARCH_V2B_LABEL = "CONFIRM_SMC_RESEARCH__DIRECTIONAL_BIAS_CONTEXT"
_CONFIRM_SMC_RESEARCH_V2B_BAD_REGIMES = {
    "CHOP_NO_TRADE",
    "RANGE_MEAN_REVERSION",
}
_CONFIRM_SMC_RESEARCH_V2B_BULLISH_TOKENS = (
    "BULLISH",
    "BULL_TREND",
    "UPTREND",
    "LONG_BIAS",
)
_CONFIRM_SMC_RESEARCH_V2B_BEARISH_TOKENS = (
    "BEARISH",
    "BEAR_TREND",
    "DOWNTREND",
    "SHORT_BIAS",
)
_CONFIRM_SMC_RESEARCH_V2B_NEUTRAL_TOKENS = (
    "RANGE",
    "CHOP",
    "MIXED",
)

_PAPER_SMC_RESEARCH_EXTENSION_METADATA = {
    "research_epoch": "v1_extend_200",
    "research_cap_target": 200,
    "research_max_open_target": 3,
    "research_concurrency_epoch": "max_open_3",
    "research_extension_reason": "ENTRY_FALLBACK_TUNING_ANALYSIS",
    "research_original_cap_completed": 100,
}
_PAPER_SMC_RESEARCH_ENTRY_SHADOW_LABEL = "RESEARCH_ENTRY_FALLBACK_SHADOW_V1"
_PAPER_SMC_RESEARCH_ENTRY_SHADOW_VERSION = "v1"


def _paper_smc_research_v2_subgroup_fields(fields, side):
    side = str(side or "").strip().upper()
    market_regime = str(fields.get("market_regime") or "").strip().upper()
    would_block = fields.get("confirm_smc_entry_location_would_block")
    planned_rr = fields.get("planned_rr")
    bos_quality = str(fields.get("bos_quality") or "").strip().upper()
    volume_confirmation = str(fields.get("volume_confirmation") or "").strip().upper()

    reason = None
    if market_regime in ("", "UNKNOWN"):
        reason = "missing_regime"
    elif would_block is None:
        reason = "missing_would_block"
    elif side != "SHORT":
        reason = "not_short"
    elif market_regime in _CONFIRM_SMC_RESEARCH_V2_BAD_REGIMES:
        reason = "bad_regime"
    elif would_block is not False:
        reason = "would_block_true"
    elif planned_rr is None or planned_rr < 2:
        reason = "rr_below_2"
    elif bos_quality == "WEAK":
        reason = "bos_weak"
    elif volume_confirmation == "EXPANSION":
        reason = "volume_expansion"

    match = reason is None
    return {
        "v2_subgroup": _CONFIRM_SMC_RESEARCH_V2_SUBGROUP if match else None,
        "v2_subgroup_match": match,
        "v2_reason": reason,
    }


def _paper_smc_research_v2b_market_bias(fields):
    directional_votes = set()
    neutral_seen = False
    for key in ("smc_bias", "trend_direction", "market_regime", "phase"):
        value = str(fields.get(key) or "").strip().upper()
        if not value or value == "UNKNOWN":
            continue
        if key == "trend_direction" and value == "LONG":
            directional_votes.add("bullish")
        if key == "trend_direction" and value == "SHORT":
            directional_votes.add("bearish")
        if any(token in value for token in _CONFIRM_SMC_RESEARCH_V2B_BULLISH_TOKENS):
            directional_votes.add("bullish")
        if any(token in value for token in _CONFIRM_SMC_RESEARCH_V2B_BEARISH_TOKENS):
            directional_votes.add("bearish")
        if any(token in value for token in _CONFIRM_SMC_RESEARCH_V2B_NEUTRAL_TOKENS):
            neutral_seen = True

    if len(directional_votes) == 1:
        return next(iter(directional_votes))
    if len(directional_votes) > 1:
        return "unknown"
    if neutral_seen:
        return "neutral"
    return "unknown"


def _paper_smc_research_v2b_counter_bias_reasons(fields, side):
    reasons = []
    market_regime = str(fields.get("market_regime") or "").strip().upper()
    exhaustion = str(fields.get("exhaustion") or "").strip().upper()
    bos_quality = str(fields.get("bos_quality") or "").strip().upper()
    liquidity_context = str(fields.get("liquidity_context") or "").strip().upper()
    liquidity_sweep = str(fields.get("liquidity_sweep") or "").strip().upper()
    smc_zone = str(fields.get("smc_zone") or "").strip().upper()
    phase = str(fields.get("phase") or "").strip().upper()

    if market_regime == "EXHAUSTION_REVERSAL":
        reasons.append("market_regime_exhaustion_reversal")
    if "EXHAUSTED" in exhaustion or "REVERSAL" in exhaustion:
        reasons.append("exhaustion_reversal_context")
    if bos_quality == "TRAP":
        reasons.append("bos_trap")
    liquidity_text = f"{liquidity_context} {liquidity_sweep}"
    if "SWEEP" in liquidity_text or "STOP_HUNT" in liquidity_text or "STOPHUNT" in liquidity_text:
        reasons.append("liquidity_sweep_or_stop_hunt")
    if (side == "LONG" and smc_zone == "DISCOUNT") or (
        side == "SHORT" and smc_zone == "PREMIUM"
    ):
        reasons.append("favorable_reversal_zone")
    if any(
        token in phase
        for token in (
            "RANGE_EXTREME",
            "RANGE_LOW",
            "RANGE_HIGH",
            "PRE_BREAK",
            "PREBREAK",
            "RECLAIM",
        )
    ):
        reasons.append("reversal_phase_context")
    return reasons


def _paper_smc_research_v2b_fields(fields, side):
    side = str(side or "").strip().upper()
    planned_rr = fields.get("planned_rr")
    bos_quality = str(fields.get("bos_quality") or "").strip().upper()
    volume_confirmation = str(fields.get("volume_confirmation") or "").strip().upper()
    market_regime = str(fields.get("market_regime") or "").strip().upper()
    would_block = fields.get("confirm_smc_entry_location_would_block")
    market_bias = _paper_smc_research_v2b_market_bias(fields)
    alignment = "unknown"
    v2b_class = "BIAS_UNKNOWN"
    match = False
    reason = "bias_unknown"
    counter_bias_candidate = False
    counter_bias_reasons = []

    v1_pass = (
        planned_rr is not None
        and planned_rr >= 2
        and bool(bos_quality)
        and bos_quality != "WEAK"
        and bool(volume_confirmation)
        and volume_confirmation != "EXPANSION"
    )
    if not v1_pass:
        v2b_class = "V1_FILTER_FAIL"
        reason = "v1_filter_fail"
    elif market_regime in _CONFIRM_SMC_RESEARCH_V2B_BAD_REGIMES:
        v2b_class = "BAD_REGIME"
        alignment = "neutral"
        reason = "bad_regime"
    elif would_block is None:
        v2b_class = "FIELD_MISSING"
        reason = "missing_would_block"
    elif side not in {"LONG", "SHORT"}:
        v2b_class = "FIELD_MISSING"
        reason = "missing_side"
    elif market_bias == "unknown":
        v2b_class = "BIAS_UNKNOWN"
        reason = "bias_unknown"
    elif market_bias == "neutral":
        v2b_class = "BIAS_UNKNOWN"
        alignment = "neutral"
        reason = "neutral_bias"
    else:
        aligned = (
            (market_bias == "bearish" and side == "SHORT")
            or (market_bias == "bullish" and side == "LONG")
        )
        if aligned:
            v2b_class = "BIAS_ALIGNED"
            alignment = "aligned"
            match = would_block is False
            reason = None if match else "would_block_true"
        else:
            alignment = "counter_bias"
            counter_bias_reasons = _paper_smc_research_v2b_counter_bias_reasons(fields, side)
            counter_bias_candidate = bool(counter_bias_reasons) and would_block is False
            if counter_bias_candidate:
                v2b_class = "COUNTER_BIAS_REVERSAL_CANDIDATE"
                match = True
                reason = None
            else:
                v2b_class = "BIAS_MISALIGNED_WEAK"
                reason = "would_block_true" if would_block is not False else "no_reversal_context"

    return {
        "v2b_label": _CONFIRM_SMC_RESEARCH_V2B_LABEL,
        "v2b_class": v2b_class,
        "v2b_match": match,
        "v2b_reason": reason,
        "v2b_market_bias": market_bias,
        "v2b_direction_alignment": alignment,
        "v2b_counter_bias_candidate": counter_bias_candidate,
        "v2b_counter_bias_reasons": counter_bias_reasons,
    }


def _paper_smc_research_qualified_fields(candidate):
    structural_context = _paper_structural_context(candidate)
    fields = {
        "planned_rr": _paper_smc_research_float(
            _first_nonblank(candidate.get("planned_rr"), candidate.get("rr"))
        ),
        "bos_quality": str(
            _paper_structural_context_value(candidate, structural_context, "bos_quality") or ""
        ).upper(),
        "volume_confirmation": str(
            _paper_structural_context_value(candidate, structural_context, "volume_confirmation") or ""
        ).upper(),
        "score_v2_current": _first_nonblank(
            candidate.get("score_v2_current"), structural_context.get("score_v2_current")
        ),
        "score_v2_structural_shadow": _first_nonblank(
            candidate.get("score_v2_structural_shadow"),
            structural_context.get("score_v2_structural_shadow"),
        ),
        "structural_decision_shadow": _paper_structural_context_value(
            candidate, structural_context, "structural_decision_shadow"
        ),
        "candidate_type": _paper_smc_main_candidate_type(candidate),
        "original_research_reason": candidate.get("reason"),
        "liquidity_context": _paper_structural_context_value(
            candidate, structural_context, "liquidity_context"
        ),
    }
    fields.update(_paper_smc_research_qualified_bias_context(candidate, structural_context))
    fields.update(
        _compute_confirm_smc_entry_location_risk(
            {
                **fields,
                "side": candidate.get("side"),
            }
        )
    )
    fields.update(_paper_smc_research_v2_subgroup_fields(fields, candidate.get("side")))
    fields.update(_paper_smc_research_v2b_fields(fields, candidate.get("side")))
    return fields


def _paper_smc_research_entry_fallback_shadow(candidate, fields=None, now_ts=None):
    """Classify entry/fallback context for logging only; never used by a gate."""
    candidate = candidate if isinstance(candidate, dict) else {}
    fields = fields if isinstance(fields, dict) else _paper_smc_research_qualified_fields(candidate)
    now_ts = now_ts or time.time()

    market_regime = str(fields.get("market_regime") or "").strip().upper()
    phase = str(fields.get("phase") or "").strip().upper()
    bos_quality = str(fields.get("bos_quality") or "").strip().upper()
    volume_confirmation = str(fields.get("volume_confirmation") or "").strip().upper()
    displacement_quality = str(
        _first_nonblank(
            candidate.get("displacement_quality"),
            _paper_structural_context(candidate).get("displacement_quality"),
        ) or ""
    ).strip().upper()
    barrier_context = [
        str(value)
        for value in _iter_signal_metadata_strings(candidate)
        if "BARRIER" in str(value).upper() or "PRE_BREAK" in str(value).upper()
    ]
    pre_break_context = bool(
        "PRE_BREAK" in phase
        or "PREBREAK" in phase
        or any(
            "PRE_BREAK" in value.upper() or "PREBREAK" in value.upper()
            for value in barrier_context
        )
    )
    stale_info = _paper_smc_research_stale_info(candidate, now_ts)
    stale_time_resolved = stale_info.get("candidate_time_source") != "missing"
    stale_signal = bool(stale_time_resolved and stale_info.get("is_stale"))
    no_followthrough = (
        bos_quality in {"NO_FOLLOWTHROUGH", "TRAP"}
        or displacement_quality in {"NONE", "WEAK", "NO_FOLLOWTHROUGH"}
    )
    bad_regime = market_regime in {"EXHAUSTION_REVERSAL", "DEAD", "NO_TRADE"}
    chop_or_range = (
        market_regime in {"CHOP_NO_TRADE", "RANGE_MEAN_REVERSION", "RANGE"}
        or "CHOP" in market_regime
        or "RANGE" in market_regime
    )
    acceptance_context = any(
        token in phase
        for token in ("ACCEPT", "RETEST", "BREAKOUT", "CONFIRM")
    ) or bos_quality in {"STRONG", "CONFIRMED", "CLOSE_THROUGH"}

    missing_fields = []
    for name, value in (
        ("market_regime", market_regime),
        ("phase", phase),
        ("bos_quality", bos_quality),
        ("volume_confirmation", volume_confirmation),
    ):
        if value in ("", "UNKNOWN", "DEFAULT"):
            missing_fields.append(name)
    core_context_missing = bool(missing_fields)
    if not stale_time_resolved:
        missing_fields.append("signal_timestamp")
    if not barrier_context:
        missing_fields.append("barrier_context")

    if stale_signal:
        risk_class = "STALE_SIGNAL_ENTRY"
        risk_reason = stale_info.get("stale_reason_detail") or "stale_signal_context"
    elif bad_regime:
        risk_class = "BAD_REGIME_ENTRY"
        risk_reason = f"market_regime={market_regime}"
    elif chop_or_range:
        risk_class = "CHOP_OR_RANGE_ENTRY"
        risk_reason = f"market_regime={market_regime}"
    elif pre_break_context:
        risk_class = "PRE_BREAK_ANTICIPATION"
        risk_reason = "pre_break_or_barrier_context"
    elif no_followthrough:
        risk_class = "NO_FOLLOWTHROUGH_RISK"
        risk_reason = (
            f"bos_quality={bos_quality or 'UNKNOWN'};"
            f"displacement_quality={displacement_quality or 'UNKNOWN'}"
        )
    elif not core_context_missing and acceptance_context:
        risk_class = "CLEAN_BREAK_ACCEPTANCE"
        risk_reason = "acceptance_or_retest_context_present"
    else:
        risk_class = "UNKNOWN_INSUFFICIENT_CONTEXT"
        risk_reason = "missing_required_entry_timing_context"

    if risk_class == "PRE_BREAK_ANTICIPATION":
        fallback_candidate = True
        fallback_reason = "delayed_entry_after_acceptance_or_retest_confirmation"
    elif risk_class == "NO_FOLLOWTHROUGH_RISK":
        fallback_candidate = True
        fallback_reason = "skip_if_no_follow_through"
    elif risk_class in {"BAD_REGIME_ENTRY", "CHOP_OR_RANGE_ENTRY", "STALE_SIGNAL_ENTRY"}:
        fallback_candidate = True
        fallback_reason = "skip_or_delay_until_context_improves"
    elif risk_class == "CLEAN_BREAK_ACCEPTANCE":
        fallback_candidate = False
        fallback_reason = "no_fallback_suggested"
    else:
        fallback_candidate = None
        fallback_reason = "insufficient_context"

    timing_components = {
        "market_regime": fields.get("market_regime"),
        "phase": fields.get("phase"),
        "bos_quality": fields.get("bos_quality"),
        "volume_confirmation": fields.get("volume_confirmation"),
        "displacement_quality": displacement_quality or None,
        "barrier_context": barrier_context or None,
        "pre_break_context": pre_break_context if barrier_context or phase else None,
        "stale_signal": stale_signal,
        "stale_reason": stale_info.get("stale_reason_detail") if stale_signal else None,
    }
    fallback_components = {
        "suggest_delayed_entry_after_acceptance": risk_class == "PRE_BREAK_ANTICIPATION",
        "suggest_retest_confirmation": risk_class == "PRE_BREAK_ANTICIPATION",
        "suggest_skip_no_followthrough": risk_class == "NO_FOLLOWTHROUGH_RISK",
        "suggest_smaller_target": None,
        "no_fallback_suggested": risk_class == "CLEAN_BREAK_ACCEPTANCE",
    }
    return {
        "research_entry_shadow_label": _PAPER_SMC_RESEARCH_ENTRY_SHADOW_LABEL,
        "research_entry_shadow_version": _PAPER_SMC_RESEARCH_ENTRY_SHADOW_VERSION,
        "research_entry_timing_risk_class": risk_class,
        "research_entry_timing_risk_reason": risk_reason,
        "research_entry_timing_components": timing_components,
        "research_fallback_candidate": fallback_candidate,
        "research_fallback_reason": fallback_reason,
        "research_fallback_components": fallback_components,
        "barrier_context": barrier_context or None,
        "pre_break_context": pre_break_context if barrier_context or phase else None,
        "missing_fields": missing_fields,
    }


def _paper_smc_research_entry_fallback_shadow_safe(candidate, fields=None, now_ts=None):
    try:
        return _paper_smc_research_entry_fallback_shadow(
            candidate,
            fields=fields,
            now_ts=now_ts,
        )
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH ENTRY FALLBACK SHADOW] classify failed: {exc}")
        return {
            "research_entry_shadow_label": _PAPER_SMC_RESEARCH_ENTRY_SHADOW_LABEL,
            "research_entry_shadow_version": _PAPER_SMC_RESEARCH_ENTRY_SHADOW_VERSION,
            "research_entry_timing_risk_class": "UNKNOWN_INSUFFICIENT_CONTEXT",
            "research_entry_timing_risk_reason": "shadow_instrumentation_error",
            "research_entry_timing_components": None,
            "research_fallback_candidate": None,
            "research_fallback_reason": "insufficient_context",
            "research_fallback_components": None,
            "barrier_context": None,
            "pre_break_context": None,
            "missing_fields": ["shadow_instrumentation_context"],
        }


def _paper_smc_research_entry_fallback_shadow_write(row):
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        path = os.path.join(log_dir, "paper_smc_research_entry_fallback_shadow.jsonl")
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH ENTRY FALLBACK SHADOW] log failed: {exc}")


def _paper_smc_research_entry_fallback_shadow_snapshot(candidate, opened, fields, shadow):
    try:
        candidate = candidate if isinstance(candidate, dict) else {}
        opened = opened if isinstance(opened, dict) else {}
        fields = fields if isinstance(fields, dict) else {}
        shadow = shadow if isinstance(shadow, dict) else {}
        row = {
            "timestamp": format_vn_time(time.time()),
            "event_type": "PAPER_SMC_RESEARCH_ENTRY_FALLBACK_SHADOW",
            "symbol": opened.get("symbol") or candidate.get("symbol"),
            "side": opened.get("side") or candidate.get("side"),
            "opened_trade_id": opened.get("id"),
            "decision": "OPEN",
            "entry_type": opened.get("entry_type") or "CONFIRM_SMC_RESEARCH",
            "strategy_family": opened.get("strategy_family") or "confirm_smc_research",
            **_PAPER_SMC_RESEARCH_EXTENSION_METADATA,
            "research_is_post_50": bool(opened.get("research_is_post_50")),
            "planned_rr": fields.get("planned_rr"),
            "bos_quality": fields.get("bos_quality"),
            "volume_confirmation": fields.get("volume_confirmation"),
            "market_regime": fields.get("market_regime"),
            "phase": fields.get("phase"),
            "smc_bias": fields.get("smc_bias"),
            "v2b_class": fields.get("v2b_class"),
            "v2b_market_bias": fields.get("v2b_market_bias"),
            "v2b_direction_alignment": fields.get("v2b_direction_alignment"),
            "entry_location_would_block": fields.get("confirm_smc_entry_location_would_block"),
            "entry_location_primary_reason": fields.get("confirm_smc_entry_location_primary_reason"),
            "barrier_context": shadow.get("barrier_context"),
            "pre_break_context": shadow.get("pre_break_context"),
            "research_entry_timing_risk_class": shadow.get("research_entry_timing_risk_class"),
            "research_entry_timing_risk_reason": shadow.get("research_entry_timing_risk_reason"),
            "research_entry_timing_components": _json_safe_copy(
                shadow.get("research_entry_timing_components")
            ),
            "research_fallback_candidate": shadow.get("research_fallback_candidate"),
            "research_fallback_reason": shadow.get("research_fallback_reason"),
            "research_fallback_components": _json_safe_copy(
                shadow.get("research_fallback_components")
            ),
            "missing_fields": _json_safe_copy(shadow.get("missing_fields") or []),
        }
        _paper_smc_research_entry_fallback_shadow_write(row)
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH ENTRY FALLBACK SHADOW] snapshot failed: {exc}")


def _paper_smc_research_qualified_predicate(candidate):
    fields = _paper_smc_research_qualified_fields(candidate)
    min_rr = _paper_smc_research_float(
        config.get("paper_smc_research_qualified_min_rr", 2.0)
    )
    if min_rr is None:
        min_rr = 2.0

    planned_rr = fields.get("planned_rr")
    if planned_rr is None:
        return False, "rr_missing", fields
    if planned_rr < min_rr:
        return False, "rr_below_2", fields

    if not fields.get("bos_quality"):
        return False, "bos_missing", fields
    if fields.get("bos_quality") == "WEAK":
        return False, "bos_weak", fields

    if not fields.get("volume_confirmation"):
        return False, "volume_missing", fields
    if fields.get("volume_confirmation") == "EXPANSION":
        return False, "volume_expansion", fields

    return True, "qualified_open", fields


def _paper_smc_research_qualified_decision_log(
    candidate,
    qualified,
    decision,
    reason,
    fields=None,
    opened_trade_id=None,
    now_ts=None,
    qualified_reject_subreason=None,
    qualified_eval_ts=None,
    dispatch_ts=None,
    open_trade_ts=None,
    open_count_at_decision=None,
    max_open=None,
    cap_enabled=None,
    cap_block_skipped=None,
    max_new=None,
    opened_total=None,
    first_seen_ts=None,
    current_seen_ts=None,
    slot_wait_secs=None,
    decision_extra=None,
):
    try:
        now_ts = now_ts or time.time()
        fields = fields or _paper_smc_research_qualified_fields(candidate)
        latency_fields = _paper_smc_research_qualified_latency_fields(
            candidate,
            qualified_eval_ts=qualified_eval_ts,
            dispatch_ts=dispatch_ts,
            open_trade_ts=open_trade_ts,
        )
        bias_context = {
            key: _json_safe_copy(fields.get(key))
            for key in _PAPER_SMC_RESEARCH_QUALIFIED_BIAS_FIELDS
        }
        entry_location_risk = {
            key: _json_safe_copy(fields.get(key))
            for key in _PAPER_SMC_ENTRY_LOCATION_RISK_FIELDS
        }
        entry_fallback_shadow = _paper_smc_research_entry_fallback_shadow_safe(
            candidate,
            fields=fields,
            now_ts=now_ts,
        )
        v2b_decision_fields = dict(fields)
        v2b_decision_fields["research_entry_timing_risk_class"] = entry_fallback_shadow.get(
            "research_entry_timing_risk_class"
        )
        smc_entry_v2_shadow = _smc_entry_v2_shadow(
            candidate,
            fields=fields,
            v1_decision=decision,
            v1_reason=reason,
            now_ts=now_ts,
        )
        smc_entry_v2b_allowlist_shadow = _smc_entry_v2b_allowlist_shadow(
            candidate,
            fields=v2b_decision_fields,
            mode="PAPER_SHADOW_ONLY",
        )
        row = {
            "event_type": "PAPER_SMC_RESEARCH_QUALIFIED_DECISION",
            "observed_at": format_vn_time(now_ts),
            "observed_at_unix": now_ts,
            "scan_id": candidate.get("scan_id"),
            "batch_id": candidate.get("batch_id"),
            "symbol": candidate.get("symbol"),
            "side": candidate.get("side"),
            "dedup_key": candidate.get("dedup_key"),
            "research_dedup_key": candidate.get("dedup_key"),
            "research_join_key": candidate.get("dedup_key"),
            "source_timestamp": candidate.get("source_timestamp"),
            "planned_rr": fields.get("planned_rr"),
            "bos_quality": fields.get("bos_quality"),
            "volume_confirmation": fields.get("volume_confirmation"),
            "score_v2_current": fields.get("score_v2_current"),
            "score_v2_structural_shadow": fields.get("score_v2_structural_shadow"),
            "structural_decision_shadow": fields.get("structural_decision_shadow"),
            "candidate_type": fields.get("candidate_type"),
            "original_research_reason": fields.get("original_research_reason"),
            "v2_subgroup": fields.get("v2_subgroup"),
            "v2_subgroup_match": bool(fields.get("v2_subgroup_match")),
            "v2_reason": fields.get("v2_reason"),
            "v2b_label": fields.get("v2b_label"),
            "v2b_class": fields.get("v2b_class"),
            "v2b_match": bool(fields.get("v2b_match")),
            "v2b_reason": fields.get("v2b_reason"),
            "v2b_market_bias": fields.get("v2b_market_bias"),
            "v2b_direction_alignment": fields.get("v2b_direction_alignment"),
            "v2b_counter_bias_candidate": bool(fields.get("v2b_counter_bias_candidate")),
            "v2b_counter_bias_reasons": fields.get("v2b_counter_bias_reasons") or [],
            "qualified": bool(qualified),
            "decision": decision,
            "reason": reason,
            "opened_trade_id": opened_trade_id,
            "paper_smc_research_qualified_max_open": max_open,
            "paper_smc_research_cap_enabled": cap_enabled,
            "paper_smc_research_cap_disabled": (
                None if cap_enabled is None else not bool(cap_enabled)
            ),
            "paper_smc_research_cap_block_skipped": cap_block_skipped,
            "cap_block_skipped": cap_block_skipped,
            "paper_smc_research_qualified_max_new_trades": max_new,
            "paper_smc_research_qualified_opened_total": opened_total,
            "qualified_opened_total": opened_total,
            "open_count_at_decision": open_count_at_decision,
            "open_count": open_count_at_decision,
            "max_open": max_open,
            "max_new": max_new,
            "first_seen_ts": first_seen_ts,
            "current_seen_ts": current_seen_ts,
            "slot_wait_secs": slot_wait_secs,
        }
        row.update(latency_fields)
        if reason == "qualified_reject":
            row["qualified_reject_subreason"] = (
                qualified_reject_subreason or "UNKNOWN_BASE_GATE_REJECT"
            )
            row["qualified_reject_subreason_version"] = "v0.1"
        row = {
            key: _json_safe_copy(value)
            for key, value in row.items()
            if value not in ("", None)
            or key in _QUALIFIED_LATENCY_NULL_FIELDS
            or key in {"v2_subgroup", "v2_reason", "v2b_reason"}
        }
        row.update(bias_context)
        row.update(entry_location_risk)
        row.update(_PAPER_SMC_RESEARCH_EXTENSION_METADATA)
        row["research_is_post_50"] = bool(candidate.get("_research_is_post_50"))
        row.update({
            key: _json_safe_copy(value)
            for key, value in entry_fallback_shadow.items()
        })
        row.update({
            key: _json_safe_copy(smc_entry_v2_shadow.get(key))
            for key in (
                "smc_entry_v2_shadow_version",
                "v2_shadow_status",
                "v2_shadow_reason",
                "v2_location_score",
                "v2_late_chase_score",
                "v2_structure_score",
                "v2_expected_entry",
                "v2_structural_sl",
                "v2_expected_rr",
                "v2_compared_to_v1",
                "v2_feature_quality",
            )
        })
        row.update({
            key: _json_safe_copy(smc_entry_v2b_allowlist_shadow.get(key))
            for key in (
                "smc_entry_v2b_allowlist_label",
                "smc_entry_v2b_allowlist_match",
                "smc_entry_v2b_allowlist_reason",
                "smc_entry_v2b_allowlist_version",
                "smc_entry_v2b_v02_label",
                "smc_entry_v2b_v02_match",
                "smc_entry_v2b_v02_reason",
                "smc_entry_v2b_v02_version",
                "v0.1_match",
                "v0.2_match",
                "smc_entry_v2b_score_bucket",
                "v2b_score_source",
                "v2b_score_value",
                "v2b_score_bucket",
                "v2b_entry_location",
                "v2b_entry_location_source",
                "v2b_regime",
                "v2b_timing_risk_class",
                "v2b_regime_source",
                "v2b_recompute_match",
                "v2b_recompute_mismatch_reason",
                "smc_entry_v2b_feature_quality",
                "smc_entry_v2b_forward_mode",
            )
        })
        if isinstance(decision_extra, dict):
            row.update({
                str(key): _json_safe_copy(value)
                for key, value in decision_extra.items()
            })
        _smc_entry_v2_shadow_write(
            candidate,
            fields,
            smc_entry_v2_shadow,
            v1_decision=decision,
            v1_reason=reason,
            now_ts=now_ts,
        )
        _smc_entry_v2b_allowlist_shadow_write(
            candidate,
            v2b_decision_fields,
            smc_entry_v2b_allowlist_shadow,
            execution_mode="paper",
            v1_decision=decision,
            v1_reason=reason,
            now_ts=now_ts,
            opened_trade_id=opened_trade_id,
        )
        btc_instrumentation_row = _btc_alignment_instrumentation_shadow(
            candidate,
            execution_mode="paper",
            v1_decision=decision,
            v1_reason=reason,
            side=candidate.get("side"),
            trade=None,
            gate_fields=fields,
            v2b_fields=fields,
            now_ts=now_ts,
        )
        # SMC_PA_SCORE_V3_SHADOW (log-only): additive annotation, never gates.
        smc_pa_v3_summary = _smc_pa_score_v3_shadow(
            candidate,
            fields=fields,
            trade=None,
            execution_mode="paper",
            v1_decision=decision,
            v1_reason=reason,
            btc_ctx=btc_instrumentation_row,
            now_ts=now_ts,
        )
        if smc_pa_v3_summary:
            row.update({
                key: _json_safe_copy(value)
                for key, value in smc_pa_v3_summary.items()
            })
        # BREAKOUT_ACCEPTANCE_SHADOW (log-only): appends to its own forward
        # log only; never gates and never adds fields to this decision row.
        _breakout_acceptance_shadow(
            candidate,
            fields=fields,
            trade=None,
            execution_mode="paper",
            v1_decision=decision,
            v1_reason=reason,
            btc_ctx=btc_instrumentation_row,
            v3_summary=smc_pa_v3_summary,
            now_ts=now_ts,
        )
        with open(_paper_smc_research_qualified_log_path(), "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH QUALIFIED] decision log failed: {exc}")

def _paper_smc_research_qualified_reject_subreason(candidate, ctx, now_ts):
    # Log-only mirror of the qualified base gate; never use this value for execution.
    try:
        if ctx is None:
            return "MODE_NOT_PAPER"
        mode = getattr(ctx, "execution_mode", None)
        if mode == "live":
            return "LIVE_MODE_BLOCKED"
        if not _paper_smc_research_qualified_mode_allowed(ctx):
            return "MODE_NOT_PAPER"
        if config.get("paper_smc_research_live_enabled", False):
            return "LIVE_MODE_BLOCKED"
        if str(candidate.get("entry_type") or "").upper() != "CONFIRM":
            return "NOT_CONFIRM_ENTRY"
        geometry_status = candidate.get("geometry_status")
        if geometry_status in (None, ""):
            return "GEOMETRY_NOT_COMPUTED"
        if geometry_status != "VALID_GEOMETRY":
            return "GEOMETRY_INVALID"
        if not candidate.get("outcome_trackable"):
            return "OUTCOME_NOT_TRACKABLE"
        if candidate.get("entry") in (None, ""):
            return "MISSING_ENTRY"
        if candidate.get("sl") in (None, ""):
            return "MISSING_SL"
        stale_info = _paper_smc_research_stale_info(candidate, now_ts)
        if stale_info.get("is_stale"):
            return "STALE_SIGNAL"
        return "BASE_GATE_FALSE"
    except Exception:
        return "UNKNOWN_BASE_GATE_REJECT"


def _paper_smc_research_qualified_base_reject(candidate, ctx, now_ts):
    if ctx is None or not _paper_smc_research_qualified_mode_allowed(ctx):
        return "qualified_reject"
    if config.get("paper_smc_research_live_enabled", False):
        return "qualified_reject"
    if str(candidate.get("entry_type") or "").upper() != "CONFIRM":
        return "qualified_reject"
    if candidate.get("geometry_status") != "VALID_GEOMETRY":
        return "qualified_reject"
    if not candidate.get("outcome_trackable"):
        return "qualified_reject"
    if candidate.get("entry") in (None, "") or candidate.get("sl") in (None, ""):
        return "qualified_reject"
    stale_info = _paper_smc_research_stale_info(candidate, now_ts)
    if stale_info.get("is_stale"):
        return "qualified_reject"
    return ""


def _dispatch_paper_smc_research_qualified_lane(ctx):
    if ctx is None or not _paper_smc_research_qualified_mode_allowed(ctx):
        return
    if not bool(config.get("paper_smc_research_qualified_enabled", False)):
        return
    if config.get("paper_smc_research_live_enabled", False):
        return
    from execution import open_trade, update_signal_state
    try:
        from entry import get_confirm_structural_outcome_candidates_snapshot
        candidates = get_confirm_structural_outcome_candidates_snapshot()
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH QUALIFIED] candidate snapshot failed: {exc}")
        return

    now_ts = time.time()
    opened_total = _paper_smc_research_qualified_opened_total()
    try:
        max_new = max(0, int(config.get("paper_smc_research_qualified_max_new_trades", 200)))
    except (TypeError, ValueError):
        max_new = 0
    try:
        max_open = max(0, int(config.get("paper_smc_research_qualified_max_open", 3)))
    except (TypeError, ValueError):
        max_open = 0
    cap_enabled = bool(config.get("paper_smc_research_cap_enabled", True))

    for candidate in candidates:
        candidate = copy.deepcopy(candidate)
        candidate["_research_is_post_50"] = opened_total >= 50
        _paper_smc_main_attach_router_context(candidate, now_ts=now_ts)
        qualified, reason, fields = _paper_smc_research_qualified_predicate(candidate)
        qualified_eval_ts = time.time()
        candidate["_qualified_eval_ts"] = qualified_eval_ts
        dedup_key = str(candidate.get("dedup_key") or "")
        first_seen_ts = None
        current_seen_ts = None
        if not qualified:
            _paper_smc_research_qualified_decision_log(
                candidate,
                False,
                "REJECT",
                reason,
                fields=fields,
                now_ts=now_ts,
                qualified_eval_ts=qualified_eval_ts,
                cap_enabled=cap_enabled,
                max_new=max_new,
                opened_total=opened_total,
            )
            continue

        base_reject = _paper_smc_research_qualified_base_reject(candidate, ctx, now_ts)
        if base_reject:
            _paper_smc_research_qualified_decision_log(
                candidate,
                True,
                "REJECT",
                base_reject,
                fields=fields,
                now_ts=now_ts,
                qualified_reject_subreason=_paper_smc_research_qualified_reject_subreason(
                    candidate, ctx, now_ts
                ),
                qualified_eval_ts=qualified_eval_ts,
                cap_enabled=cap_enabled,
                max_new=max_new,
                opened_total=opened_total,
            )
            continue

        if dedup_key:
            first_seen_ts = _paper_smc_research_qualified_first_seen_ts.setdefault(
                dedup_key,
                qualified_eval_ts,
            )
            current_seen_ts = qualified_eval_ts
        cap_block_skipped = (not cap_enabled) and opened_total >= max_new

        if cap_enabled and opened_total >= max_new:
            _paper_smc_research_qualified_decision_log(
                candidate,
                True,
                "CAP_REACHED",
                "cap_reached",
                fields=fields,
                now_ts=now_ts,
                qualified_eval_ts=qualified_eval_ts,
                cap_enabled=cap_enabled,
                cap_block_skipped=False,
                max_new=max_new,
                opened_total=opened_total,
            )
            continue

        open_count_at_decision = _paper_smc_research_qualified_open_count(ctx)
        if open_count_at_decision >= max_open:
            current_seen_ts = time.time()
            if dedup_key:
                first_seen_ts = _paper_smc_research_qualified_first_seen_ts.setdefault(
                    dedup_key,
                    current_seen_ts,
                )
            _paper_smc_research_qualified_decision_log(
                candidate,
                True,
                "MAX_OPEN_REACHED",
                "max_open_reached",
                fields=fields,
                now_ts=now_ts,
                qualified_eval_ts=qualified_eval_ts,
                open_count_at_decision=open_count_at_decision,
                max_open=max_open,
                cap_enabled=cap_enabled,
                cap_block_skipped=cap_block_skipped,
                max_new=max_new,
                opened_total=opened_total,
                first_seen_ts=first_seen_ts,
                current_seen_ts=current_seen_ts,
                slot_wait_secs=(
                    round(current_seen_ts - first_seen_ts, 3)
                    if first_seen_ts is not None
                    else None
                ),
            )
            continue

        symbol = str(candidate.get("symbol") or "")
        side = str(candidate.get("side") or "").upper()
        duplicate_or_locked = (
            dedup_key in _paper_smc_research_qualified_dedup_keys
            or dedup_key in _paper_smc_research_dedup_keys
            or _paper_smc_research_key_open(ctx, dedup_key)
            or _paper_smc_research_symbol_open(ctx, symbol, side, research_only=True)
            or _paper_smc_research_symbol_open(ctx, symbol)
            or _paper_smc_main_key_open(ctx, dedup_key)
            or _paper_smc_main_symbol_open(ctx, symbol)
        )
        if duplicate_or_locked:
            _paper_smc_research_qualified_decision_log(
                candidate,
                True,
                "DUPLICATE_OR_SYMBOL_LOCKED",
                "duplicate_or_symbol_locked",
                fields=fields,
                now_ts=now_ts,
                qualified_eval_ts=qualified_eval_ts,
                open_count_at_decision=open_count_at_decision,
                max_open=max_open,
                cap_enabled=cap_enabled,
                cap_block_skipped=cap_block_skipped,
                max_new=max_new,
                opened_total=opened_total,
            )
            continue

        trade = _paper_smc_research_trade(candidate)
        entry_fallback_shadow = _paper_smc_research_entry_fallback_shadow_safe(
            candidate,
            fields=fields,
            now_ts=now_ts,
        )
        trade.update(_PAPER_SMC_RESEARCH_EXTENSION_METADATA)
        trade["research_is_post_50"] = bool(candidate.get("_research_is_post_50"))
        trade.update({
            key: _json_safe_copy(value)
            for key, value in entry_fallback_shadow.items()
        })
        dispatch_ts = time.time()
        candidate["_dispatch_ts"] = dispatch_ts
        trade["qualified_eval_ts"] = qualified_eval_ts
        trade["dispatch_ts"] = dispatch_ts
        trade["paper_smc_research_qualified"] = True
        trade["paper_smc_research_qualified_reason"] = "qualified_open"
        trade["paper_smc_research_qualified_min_rr"] = config.get(
            "paper_smc_research_qualified_min_rr", 2.0
        )
        _paper_smc_research_qualified_apply_bias_context(trade, fields)
        _paper_smc_research_qualified_apply_entry_location_risk(trade, fields)
        if not trade.get("symbol") or trade.get("side") not in {"LONG", "SHORT"}:
            _paper_smc_research_qualified_decision_log(
                candidate,
                True,
                "REJECT",
                "qualified_reject",
                fields=fields,
                now_ts=now_ts,
                qualified_reject_subreason="UNKNOWN_BASE_GATE_REJECT",
                qualified_eval_ts=qualified_eval_ts,
                dispatch_ts=dispatch_ts,
                open_count_at_decision=open_count_at_decision,
                max_open=max_open,
                cap_enabled=cap_enabled,
                cap_block_skipped=cap_block_skipped,
                max_new=max_new,
                opened_total=opened_total,
            )
            continue

        if _paper_smc_research_location_gate_blocks(fields):
            location_gate_extra = _confirm_smc_location_gate_log_fields(
                candidate,
                fields=fields,
                mode="PAPER_GATE",
            )
            location_gate_extra.update({
                "decision": "PREFILTER_REJECT",
                "reason": "PAPER_LOCATION_GATE_BLOCK",
                "fallback_reason": entry_fallback_shadow.get("research_fallback_reason"),
                "location_gate_reason": _confirm_smc_location_gate_reason(fields),
            })
            _paper_smc_research_qualified_decision_log(
                candidate,
                True,
                "PREFILTER_REJECT",
                "PAPER_LOCATION_GATE_BLOCK",
                fields=fields,
                now_ts=now_ts,
                qualified_eval_ts=qualified_eval_ts,
                dispatch_ts=dispatch_ts,
                open_count_at_decision=open_count_at_decision,
                max_open=max_open,
                cap_enabled=cap_enabled,
                cap_block_skipped=cap_block_skipped,
                max_new=max_new,
                opened_total=opened_total,
                decision_extra=location_gate_extra,
            )
            continue

        before_open = _paper_smc_research_key_open(ctx, trade.get("research_dedup_key"))
        success = open_trade(copy.deepcopy(trade), ctx)
        if success:
            _paper_smc_research_qualified_dedup_keys.add(trade.get("research_dedup_key"))
            _paper_smc_research_dedup_keys.add(trade.get("research_dedup_key"))
            ctx.entry_cooldown[trade["symbol"]] = time.time()
            update_signal_state(
                trade["symbol"],
                trade["side"],
                trade.get("entry_real") or trade.get("entry", 0),
                executed=True,
                ctx=ctx,
            )
            opened = next(
                (
                    t for t in ctx.trades
                    if t.get("strategy_family") == "confirm_smc_research"
                    and t.get("research_dedup_key") == trade.get("research_dedup_key")
                    and bool(t.get("paper_smc_research_qualified"))
                ),
                trade,
            )
            open_trade_ts = (
                _qualified_latency_ts(opened.get("open_trade_ts"))
                or _qualified_latency_ts(opened.get("time"))
                or time.time()
            )
            candidate["_open_trade_ts"] = open_trade_ts
            opened_total += 1
            _paper_smc_research_qualified_decision_log(
                candidate,
                True,
                "OPEN",
                "qualified_open",
                fields=fields,
                opened_trade_id=opened.get("id"),
                now_ts=now_ts,
                qualified_eval_ts=qualified_eval_ts,
                dispatch_ts=dispatch_ts,
                open_trade_ts=open_trade_ts,
                open_count_at_decision=open_count_at_decision,
                max_open=max_open,
                cap_enabled=cap_enabled,
                cap_block_skipped=cap_block_skipped,
                max_new=max_new,
                opened_total=opened_total,
            )
            _paper_smc_research_emit_qualified_latency_waterfall(
                candidate,
                opened,
                fields=fields,
                qualified_eval_ts=qualified_eval_ts,
                dispatch_ts=dispatch_ts,
                open_trade_ts=open_trade_ts,
                open_count_at_decision=open_count_at_decision,
                max_open=max_open,
            )
            _paper_smc_research_entry_context_snapshot(
                candidate,
                opened,
                qualified_fields=fields,
            )
            _paper_smc_research_entry_acceptance_shadow_snapshot(
                opened,
                qualified_fields=fields,
            )
        elif before_open or _paper_smc_research_key_open(ctx, trade.get("research_dedup_key")):
            _paper_smc_research_qualified_dedup_keys.add(trade.get("research_dedup_key"))
            _paper_smc_research_qualified_decision_log(
                candidate,
                True,
                "DUPLICATE_OR_SYMBOL_LOCKED",
                "duplicate_or_symbol_locked",
                fields=fields,
                now_ts=now_ts,
                qualified_eval_ts=qualified_eval_ts,
                dispatch_ts=dispatch_ts,
                open_count_at_decision=open_count_at_decision,
                max_open=max_open,
                cap_enabled=cap_enabled,
                cap_block_skipped=cap_block_skipped,
                max_new=max_new,
                opened_total=opened_total,
            )
        else:
            _paper_smc_research_qualified_decision_log(
                candidate,
                True,
                "REJECT",
                "qualified_reject",
                fields=fields,
                now_ts=now_ts,
                qualified_reject_subreason="UNKNOWN_BASE_GATE_REJECT",
                qualified_eval_ts=qualified_eval_ts,
                dispatch_ts=dispatch_ts,
                open_count_at_decision=open_count_at_decision,
                max_open=max_open,
                cap_enabled=cap_enabled,
                cap_block_skipped=cap_block_skipped,
                max_new=max_new,
                opened_total=opened_total,
            )


def _paper_smc_research_suppression(candidate, ctx, now_ts):
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return "live_guard_blocked"
    if config.get("paper_smc_research_live_enabled", False):
        return "live_guard_blocked"
    if not config.get("paper_enable_smc_research_lane", False):
        return "disabled"

    structural_context = _safe_dict(candidate.get("structural_context"))
    if str(candidate.get("entry_type") or "").upper() != "CONFIRM":
        return "not_valid_geometry"

    reason = str(candidate.get("reason") or "").upper()
    allowed_reasons = {str(x).upper() for x in config.get("paper_smc_research_allow_reasons", [])}
    if reason not in allowed_reasons:
        return "reason_not_allowed"

    if candidate.get("geometry_status") != "VALID_GEOMETRY":
        return "not_valid_geometry"
    if not candidate.get("outcome_trackable"):
        return "not_valid_geometry"
    if candidate.get("entry") in (None, "") or candidate.get("sl") in (None, ""):
        return "not_valid_geometry"

    decision = str(structural_context.get("structural_decision_shadow") or "").upper()
    allowed_decisions = {
        str(x).upper()
        for x in config.get("paper_smc_research_allow_structural_decisions", [])
    }
    unknown_allowed = bool(config.get("paper_smc_research_allow_unknown_structural_decision", True))
    if decision in ("", "UNKNOWN") and not unknown_allowed:
        return "structural_decision_unknown"
    if decision not in allowed_decisions and not (decision == "UNKNOWN" and unknown_allowed):
        return "structural_decision_not_allowed"

    min_score = _paper_smc_research_float(
        config.get("paper_smc_research_min_score_v2_structural_shadow", 3.5)
    )
    shadow_score = _paper_smc_research_float(structural_context.get("score_v2_structural_shadow"))
    modifier_info = _log_paper_smc_research_structural_modifier(candidate, ctx)
    use_modifier_score = (
        bool(config.get("paper_enable_structural_score_modifier", False))
        and not bool(config.get("paper_structural_score_modifier_log_only", True))
    )
    effective_score = (
        modifier_info.get("effective_score")
        if isinstance(modifier_info, dict) and use_modifier_score
        else shadow_score
    )
    if min_score is not None and (effective_score is None or effective_score < min_score):
        boundary_guard_info = {}
        if use_modifier_score and shadow_score is not None and effective_score is not None:
            boundary_guard_info = _paper_smc_research_boundary_guard_info(
                candidate,
                ctx,
                shadow_score,
                effective_score,
                modifier_info.get("modifier", 0.0) if isinstance(modifier_info, dict) else 0.0,
                modifier_info.get("modifier_reasons", []) if isinstance(modifier_info, dict) else [],
            )
        if boundary_guard_info.get("boundary_guard_applied"):
            _paper_boundary_guard_emit_applied(candidate, ctx, boundary_guard_info)
            pass
        else:
            return "structural_modifier_score_too_low" if use_modifier_score else "score_too_low"

    dedup_key = str(candidate.get("dedup_key") or "")
    if dedup_key in _paper_smc_research_dedup_keys or _paper_smc_research_key_open(ctx, dedup_key):
        _paper_boundary_guard_emit_result(
            candidate,
            ctx,
            "BOUNDARY_GUARD_LATER_SUPPRESSED",
            "SUPPRESS",
            {"suppress_reason": "duplicate_key"},
        )
        return "duplicate_key"

    symbol = str(candidate.get("symbol") or "")
    side = str(candidate.get("side") or "").upper()
    if _paper_smc_research_symbol_open(ctx, symbol, side, research_only=True):
        _paper_boundary_guard_emit_result(
            candidate,
            ctx,
            "BOUNDARY_GUARD_LATER_SUPPRESSED",
            "SUPPRESS",
            {"suppress_reason": "symbol_already_open"},
        )
        return "symbol_already_open"
    if _paper_smc_research_symbol_open(ctx, symbol):
        _paper_boundary_guard_emit_result(
            candidate,
            ctx,
            "BOUNDARY_GUARD_LATER_SUPPRESSED",
            "SUPPRESS",
            {"suppress_reason": "symbol_already_open"},
        )
        return "symbol_already_open"

    try:
        max_open = max(0, int(config.get("paper_smc_research_max_open", 3)))
    except (TypeError, ValueError):
        max_open = 0
    if _paper_smc_research_open_count(ctx) >= max_open:
        _paper_boundary_guard_emit_result(
            candidate,
            ctx,
            "BOUNDARY_GUARD_LATER_SUPPRESSED",
            "SUPPRESS",
            {"suppress_reason": "max_open_reached"},
        )
        return "max_open_reached"

    stale_info = _paper_smc_research_stale_info(candidate, now_ts)
    if stale_info.get("is_stale"):
        _paper_boundary_guard_emit_result(
            candidate,
            ctx,
            "BOUNDARY_GUARD_LATER_SUPPRESSED",
            "SUPPRESS",
            {"suppress_reason": stale_info.get("stale_reason_detail") or "stale_candidate"},
        )
        return stale_info.get("stale_reason_detail") or "stale_candidate"

    return ""


def _paper_smc_research_trade(candidate):
    structural_context = _safe_dict(candidate.get("structural_context"))
    now_ts = time.time()
    entry = _paper_smc_research_float(candidate.get("entry"))
    sl = _paper_smc_research_float(candidate.get("sl"))
    tp = _paper_smc_research_float(candidate.get("tp"))
    rr = _paper_smc_research_float(candidate.get("rr"))
    shadow_score = _paper_smc_research_float(structural_context.get("score_v2_structural_shadow"))
    current_score = _paper_smc_research_float(structural_context.get("score_v2_current"))
    score = shadow_score if shadow_score is not None else (current_score if current_score is not None else 0.0)
    reason = str(candidate.get("reason") or "")
    trade = {
        "symbol": str(candidate.get("symbol") or ""),
        "side": str(candidate.get("side") or "").upper(),
        "entry": entry,
        "entry_real": entry,
        "sl": sl,
        "sl_real": sl,
        "sl_init": sl,
        "tp": tp,
        "rr": rr if rr is not None else 0,
        "score": score,
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "status": "OPEN",
        "reason": [reason, "Research:CONFIRM_STRUCTURAL_OUTCOME_SHADOW"],
        "tp_mode": "SOFT" if (rr or 0) >= 1.8 else "HARD",
        "entry_time": now_ts,
        "signal_created_ts": _paper_smc_research_source_ts(candidate) or now_ts,
        "signal_detected_ts": candidate.get("signal_detected_ts"),
        "max_profit_r": 0,
        "entry_size": 1.0,
        "id": int(now_ts * 1000),
        "strategy_family": "confirm_smc_research",
        "research_source": "confirm_structural_outcome_shadow",
        "research_dedup_key": str(candidate.get("dedup_key") or ""),
        "original_reason": reason,
        "structural_decision_shadow": structural_context.get("structural_decision_shadow"),
        "unknown_structural_decision_allowed": (
            str(structural_context.get("structural_decision_shadow") or "").upper() == "UNKNOWN"
            and bool(config.get("paper_smc_research_allow_unknown_structural_decision", True))
        ),
        "research_risk_tier": (
            "UNKNOWN_STRUCTURAL_DECISION_RESEARCH"
            if str(structural_context.get("structural_decision_shadow") or "").upper() == "UNKNOWN"
            else "STRUCTURAL_DECISION_RESEARCH"
        ),
        "original_structural_decision_shadow": structural_context.get("structural_decision_shadow"),
        "score_v2_current": current_score,
        "score_v2_structural_shadow": shadow_score,
        "score_delta_direction": structural_context.get("score_delta_direction"),
        "bos_quality": structural_context.get("bos_quality"),
        "choch_quality": structural_context.get("choch_quality"),
        "poi_location_quality": structural_context.get("poi_location_quality"),
        "volume_confirmation": structural_context.get("volume_confirmation"),
        "structural_context": _json_safe_copy(structural_context),
        "confirm_entry_acceptance_context": _json_safe_copy(
            candidate.get("confirm_entry_acceptance_context")
        ),
    }
    return trade


def _dispatch_paper_smc_research_lane(ctx):
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return
    if not config.get("paper_enable_smc_research_lane", False):
        return
    from execution import open_trade, update_signal_state
    try:
        from entry import get_confirm_structural_outcome_candidates_snapshot
        candidates = get_confirm_structural_outcome_candidates_snapshot()
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH] candidate snapshot failed: {exc}")
        return

    now_ts = time.time()
    for candidate in candidates:
        suppress_reason = _paper_smc_research_suppression(candidate, ctx, now_ts)
        if suppress_reason:
            _extra = _paper_smc_research_boundary_guard_log_extra(candidate, ctx) or None
            if str(suppress_reason).startswith("stale_"):
                _stale_extra = _paper_smc_research_stale_info(candidate, now_ts)
                if isinstance(_extra, dict):
                    _extra.update(_stale_extra)
                else:
                    _extra = _stale_extra
            _paper_smc_research_log(candidate, "SUPPRESS", suppress_reason, extra=_extra)
            continue
        trade = _paper_smc_research_trade(candidate)
        _boundary_extra = _paper_smc_research_boundary_guard_log_extra(candidate, ctx)
        if _boundary_extra.get("boundary_guard_applied"):
            trade.update(_boundary_extra)
        if not trade.get("symbol") or trade.get("side") not in {"LONG", "SHORT"}:
            _paper_smc_research_log(candidate, "SUPPRESS", "not_valid_geometry")
            continue
        before_open = _paper_smc_research_key_open(ctx, trade.get("research_dedup_key"))
        success = open_trade(copy.deepcopy(trade), ctx)
        if success:
            _paper_smc_research_dedup_keys.add(trade.get("research_dedup_key"))
            ctx.entry_cooldown[trade["symbol"]] = time.time()
            update_signal_state(
                trade["symbol"],
                trade["side"],
                trade.get("entry_real") or trade.get("entry", 0),
                executed=True,
                ctx=ctx,
            )
            opened = next(
                (
                    t for t in ctx.trades
                    if t.get("strategy_family") == "confirm_smc_research"
                    and t.get("research_dedup_key") == trade.get("research_dedup_key")
                ),
                trade,
            )
            _paper_smc_research_log(
                candidate,
                "OPEN",
                trade=opened,
                extra=_boundary_extra,
            )
            _paper_smc_research_entry_context_snapshot(candidate, opened)
            _paper_boundary_guard_emit_result(
                candidate,
                ctx,
                "BOUNDARY_GUARD_OPENED",
                "OPEN",
                _boundary_extra,
            )
        elif before_open or _paper_smc_research_key_open(ctx, trade.get("research_dedup_key")):
            _paper_smc_research_dedup_keys.add(trade.get("research_dedup_key"))
            _paper_smc_research_log(candidate, "SUPPRESS", "duplicate_key")
            _paper_boundary_guard_emit_result(
                candidate,
                ctx,
                "BOUNDARY_GUARD_LATER_SUPPRESSED",
                "SUPPRESS",
                {"suppress_reason": "duplicate_key", **_boundary_extra},
            )
        else:
            _paper_smc_research_log(candidate, "SUPPRESS", "symbol_already_open")
            _paper_boundary_guard_emit_result(
                candidate,
                ctx,
                "BOUNDARY_GUARD_LATER_SUPPRESSED",
                "SUPPRESS",
                {"suppress_reason": "symbol_already_open", **_boundary_extra},
            )


# =====================================================================
# LIVE SMC RESEARCH DISPATCH LANE
# =====================================================================

_LIVE_SMC_RESEARCH_DECISION_LOG = os.path.join("logs", "live_smc_research_decisions.jsonl")
_LIVE_RESEARCH_MICRO_PAUSE_LOG = os.path.join("logs", "live_research_micro_pause.jsonl")
_RESEARCH_ROLLING_HEALTH_LOG = os.path.join("logs", "research_rolling_health.jsonl")
_LIVE_TRADES_CSV = "live_trades.csv"
_LIVE_RESEARCH_HEALTH_ROW_MAX_AGE_SECS = 15 * 60
_LIVE_SMC_RESEARCH_TERMINAL_FAILURE_TTL_SECS = 15 * 60
_live_smc_research_terminal_failures = {}


def _live_smc_research_min_open_score():
    return 7


def _live_smc_research_score_alignment_extra(trade, paper_predicate_aligned=None):
    trade = trade if isinstance(trade, dict) else {}
    min_score = _live_smc_research_min_open_score()
    try:
        score_val = float(trade.get("score"))
    except (TypeError, ValueError):
        score_val = 0.0
    if paper_predicate_aligned is None:
        paper_predicate_aligned = bool(
            trade.get("score_filter_bypassed_for_research")
            and trade.get("paper_predicate_aligned")
        )
    if (
        bool(config.get("live_smc_research_enabled", False))
        and str(trade.get("entry_type") or "").upper() == "CONFIRM_SMC_RESEARCH"
        and score_val < min_score
        and paper_predicate_aligned
    ):
        return {
            "score_filter_bypassed_for_research": True,
            "research_score": score_val,
            "paper_predicate_aligned": True,
            "score_filter_original_threshold": min_score,
            "score_filter_actual_score": score_val,
            "paper_research_population_aligned": True,
        }
    return {}


def _live_smc_research_json_safe(value):
    if isinstance(value, dict):
        return {str(k): _live_smc_research_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_live_smc_research_json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _live_research_micro_float(value, default=None):
    try:
        if value in (None, ""):
            return default
        out = float(value)
        if out != out:
            return default
        return out
    except (TypeError, ValueError):
        return default


def _live_research_micro_read_jsonl(path):
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        return []
    except Exception:
        return []
    return rows


def _live_research_micro_read_csv(path):
    rows = []
    try:
        with open(path, "r", encoding="utf-8", newline="") as handle:
            reader = _csv_mod.DictReader(handle)
            for row in reader:
                rows.append(dict(row))
    except FileNotFoundError:
        return []
    except Exception:
        return []
    return rows


def _live_research_micro_write(row):
    try:
        os.makedirs(os.path.dirname(_LIVE_RESEARCH_MICRO_PAUSE_LOG), exist_ok=True)
        with open(_LIVE_RESEARCH_MICRO_PAUSE_LOG, "a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    _live_smc_research_json_safe(row),
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                )
                + "\n"
            )
    except Exception as exc:
        print(f"[LIVE RESEARCH MICRO PAUSE] log failed: {exc}")


def _live_research_close_dedup_key(row):
    stable = row.get("id") or row.get("trade_id") or row.get("client_order_id") or row.get("clientOrderId")
    if stable not in (None, ""):
        return f"id:{stable}"
    return "|".join(
        str(row.get(field) or "")
        for field in ("symbol", "entry_time", "open_time", "close_time", "side")
    )


def _live_research_csv_close_rows(live_trade_rows=None):
    rows = live_trade_rows if live_trade_rows is not None else _live_research_micro_read_csv(_LIVE_TRADES_CSV)
    out = []
    for row in rows:
        if str(row.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        status = str(row.get("status") or "").upper()
        if status not in ("WIN", "LOSS", "CLOSED", "BREAKEVEN") and not row.get("close_time"):
            continue
        realized = _live_research_micro_float(
            row.get("actual_realized_r", row.get("realized_r", row.get("rr_real", row.get("rr"))))
        )
        if realized is None:
            continue
        item = dict(row)
        item["actual_realized_r"] = realized
        item["_live_close_source"] = "live_trades_csv"
        item["_sort_ts"] = _live_research_micro_float(
            row.get("close_ts", row.get("closed_at_unix", row.get("signal_created_ts"))),
            0.0,
        )
        out.append(item)
    out.sort(key=lambda row: row.get("_sort_ts", 0.0))
    return out


def _live_research_decision_close_rows(decision_rows=None):
    rows = decision_rows if decision_rows is not None else _live_research_micro_read_jsonl(
        _LIVE_SMC_RESEARCH_DECISION_LOG
    )
    out = []
    for row in rows:
        if str(row.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        decision = str(row.get("decision") or "").upper()
        status = str(row.get("status") or "").upper()
        realized = _live_research_micro_float(
            row.get(
                "actual_realized_r",
                row.get("realized_r", row.get("rr_real", row.get("r_multiple"))),
            )
        )
        if realized is None:
            continue
        if "CLOSE" not in decision and "CLOSED" not in decision and status != "CLOSED" and not row.get("close_reason"):
            continue
        item = dict(row)
        item["actual_realized_r"] = realized
        item["_live_close_source"] = "live_smc_research_decisions"
        item["_sort_ts"] = _live_research_micro_float(
            row.get("ts", row.get("observed_at_unix", row.get("time"))),
            0.0,
        )
        out.append(item)
    out.sort(key=lambda row: row.get("_sort_ts", 0.0))
    return out


def _live_research_close_rows(decision_rows=None, live_trade_rows=None):
    decision_closes = _live_research_decision_close_rows(decision_rows)
    csv_closes = _live_research_csv_close_rows(live_trade_rows)
    merged = {}
    for row in decision_closes + csv_closes:
        key = _live_research_close_dedup_key(row)
        if key not in merged:
            merged[key] = row
            continue
        if merged[key].get("_live_close_source") != "live_trades_csv" and row.get("_live_close_source") == "live_trades_csv":
            merged[key] = row
    out = list(merged.values())
    out.sort(key=lambda row: row.get("_sort_ts", 0.0))
    return out


def _live_research_micro_metrics(close_rows=None, decision_rows=None, live_trade_rows=None):
    rows = close_rows if close_rows is not None else _live_research_close_rows(
        decision_rows=decision_rows,
        live_trade_rows=live_trade_rows,
    )
    realized_rows = []
    for row in rows:
        realized = _live_research_micro_float(row.get("actual_realized_r"))
        if realized is None:
            continue
        realized_rows.append((row, realized))
    values = [value for _, value in realized_rows]
    loss_streak = 0
    for value in reversed(values):
        if value < 0:
            loss_streak += 1
        else:
            break
    rolling_values = values[-20:]
    last_live_close_key = ""
    if realized_rows:
        last_live_close_key = _live_research_close_dedup_key(realized_rows[-1][0])
    return {
        "live_loss_streak": loss_streak,
        "live_rolling_net_r": round(sum(rolling_values), 4) if rolling_values else 0.0,
        "live_closed_count": len(values),
        "last_live_close_key": last_live_close_key,
    }


def _live_research_micro_latest_pause(now_ts=None, pause_rows=None):
    now_ts = time.time() if now_ts is None else now_ts
    rows = pause_rows if pause_rows is not None else _live_research_micro_read_jsonl(
        _LIVE_RESEARCH_MICRO_PAUSE_LOG
    )
    latest = None
    for row in rows:
        if str(row.get("event_type") or "") != "LIVE_RESEARCH_MICRO_PAUSE":
            continue
        pause_until = _live_research_micro_float(row.get("pause_until"))
        if pause_until is None:
            continue
        if latest is None or _live_research_micro_float(row.get("ts"), 0.0) >= _live_research_micro_float(latest.get("ts"), 0.0):
            latest = row
    if latest is None:
        return None, None
    pause_until = _live_research_micro_float(latest.get("pause_until"))
    if pause_until and pause_until > now_ts:
        return latest, pause_until
    return latest, None


def _live_research_micro_latest_pause_for_reason(pause_reason, pause_rows=None):
    rows = pause_rows if pause_rows is not None else _live_research_micro_read_jsonl(
        _LIVE_RESEARCH_MICRO_PAUSE_LOG
    )
    latest = None
    for row in rows:
        if str(row.get("event_type") or "") != "LIVE_RESEARCH_MICRO_PAUSE":
            continue
        if str(row.get("pause_reason") or "") != str(pause_reason or ""):
            continue
        if latest is None or _live_research_micro_float(row.get("ts"), 0.0) >= _live_research_micro_float(latest.get("ts"), 0.0):
            latest = row
    return latest


def _live_research_micro_pause_arm_identity(row):
    row = row if isinstance(row, dict) else {}
    live_closed_count = row.get("pause_armed_live_closed_count", row.get("live_closed_count"))
    live_closed_count = _live_research_micro_float(live_closed_count)
    if live_closed_count is not None:
        live_closed_count = int(live_closed_count)
    last_live_close_key = str(
        row.get("pause_armed_last_live_close_key", row.get("last_live_close_key", ""))
        or ""
    )
    return live_closed_count, last_live_close_key


def _live_research_micro_same_pause_arm_identity(current_payload, previous_pause):
    previous_count, previous_key = _live_research_micro_pause_arm_identity(previous_pause)
    current_count, current_key = _live_research_micro_pause_arm_identity(current_payload)
    if previous_count is None:
        return False
    if current_count != previous_count:
        return False
    if previous_key and current_key and previous_key != current_key:
        return False
    return True


def _live_research_micro_latest_paper_health(health_rows=None):
    rows = health_rows if health_rows is not None else _live_research_micro_read_jsonl(
        _RESEARCH_ROLLING_HEALTH_LOG
    )
    for row in reversed(rows):
        value = str(row.get("paper_active_health") or row.get("paper_health") or "").upper()
        if value:
            return value
    return "UNKNOWN"


def _live_research_micro_latest_health_row(health_rows=None):
    rows = health_rows if health_rows is not None else _live_research_micro_read_jsonl(
        _RESEARCH_ROLLING_HEALTH_LOG
    )
    for row in reversed(rows):
        if isinstance(row, dict):
            return row
    return {}


def _live_research_micro_health_row_freshness(health_row, now_ts=None):
    now_ts = time.time() if now_ts is None else now_ts
    threshold = _LIVE_RESEARCH_HEALTH_ROW_MAX_AGE_SECS
    if not isinstance(health_row, dict) or not health_row:
        return {
            "health_row_status": "MISSING",
            "health_row_fresh": False,
            "health_row_stale": True,
            "health_row_ts": None,
            "health_row_age_sec": None,
            "health_row_stale_threshold_sec": threshold,
            "health_row_warning": "LIVE_HEALTH_ROW_STALE_WARN",
        }
    row_ts = _live_research_micro_float(health_row.get("ts"))
    if row_ts is None:
        return {
            "health_row_status": "INVALID_TS",
            "health_row_fresh": False,
            "health_row_stale": True,
            "health_row_ts": None,
            "health_row_age_sec": None,
            "health_row_stale_threshold_sec": threshold,
            "health_row_warning": "LIVE_HEALTH_ROW_STALE_WARN",
        }
    age_sec = max(0.0, now_ts - row_ts)
    stale = age_sec > threshold
    return {
        "health_row_status": "STALE" if stale else "FRESH",
        "health_row_fresh": not stale,
        "health_row_stale": stale,
        "health_row_ts": row_ts,
        "health_row_age_sec": round(age_sec, 3),
        "health_row_stale_threshold_sec": threshold,
        "health_row_warning": "LIVE_HEALTH_ROW_STALE_WARN" if stale else "",
    }


def _live_research_micro_config_status():
    cap_raw = config.get("max_live_research_trades", 1)
    risk_raw = config.get("live_risk_per_trade", 0)
    portfolio_raw = config.get("live_max_portfolio_risk", 0)
    try:
        cap = int(cap_raw)
    except (TypeError, ValueError):
        cap = 1
    risk = _live_research_micro_float(risk_raw)
    portfolio = _live_research_micro_float(portfolio_raw)
    is_micro = (
        cap <= 1
        and risk is not None
        and risk <= 0.005
        and portfolio is not None
        and portfolio <= 0.005
    )
    return {
        "max_live_research_trades": cap,
        "live_risk_per_trade": risk,
        "live_max_portfolio_risk": portfolio,
        "scale_block": not is_micro,
    }


def _live_research_micro_health_status(health_row, metrics, freshness=None):
    health_row = health_row if isinstance(health_row, dict) else {}
    freshness = freshness if isinstance(freshness, dict) else {}
    row_fresh = bool(freshness.get("health_row_fresh"))
    live_metrics = health_row.get("live_metrics")
    if not isinstance(live_metrics, dict):
        live_metrics = {}
    reasons = health_row.get("reasons")
    if not isinstance(reasons, list):
        reasons = [] if reasons in (None, "") else [reasons]
    reason_text = " ".join(str(reason) for reason in reasons).lower()
    live_health = str(health_row.get("live_health") or "UNKNOWN").upper() if row_fresh else "UNKNOWN"
    live_loss_streak = int(_live_research_micro_float(metrics.get("live_loss_streak"), 0) or 0)
    live_rolling_net_r = _live_research_micro_float(
        metrics.get("live_rolling_net_r", 0.0),
    )
    live_unconfirmed_rr_n = int(_live_research_micro_float(
        live_metrics.get("live_unconfirmed_rr_n") if row_fresh else 0,
        0,
    ) or 0)
    current_sl_failure_reasons = {
        "live_sl_sync_failure",
        "sl missing",
        "missing sl",
        "missing exchange sl",
        "exchange sl not confirmed",
    }
    live_sl_sync_failure = any(
        str(reason).strip().lower() in current_sl_failure_reasons
        for reason in reasons
    ) if row_fresh else False
    # NOTE: live_unconfirmed_rr_n counts CLOSED research trades whose exit price
    # was an estimate (rr_unconfirmed on the historical close row). Those are
    # settled positions with a known realized R and confirmed entries; the
    # health producer treats them as benign (excluded from the confirmed
    # sample). They are NOT a current-position safety failure, so they no longer
    # drive entry_unconfirmed. Genuine in-flight unconfirmed entries are detected
    # from live state via _live_research_current_entry_unconfirmed(ctx), and from
    # explicit health-row reason phrases below.
    entry_unconfirmed = (
        "entry_unconfirmed" in reason_text
        or "entry unconfirmed" in reason_text
        or "entry fill unconfirmed" in reason_text
    ) if row_fresh else False
    runtime_error = ("runtime error" in reason_text or "exception" in reason_text) if row_fresh else False
    loss_streak_current = bool(live_metrics.get("loss_streak_current")) if row_fresh else False
    loss_streak_stale_after_new_open = bool(live_metrics.get("loss_streak_stale_after_new_open")) if row_fresh else False
    last_live_open_key = str(live_metrics.get("last_live_open_key") or "") if row_fresh else ""
    last_live_close_key = str(live_metrics.get("last_live_close_key") or "") if row_fresh else ""
    return {
        "live_health": live_health,
        "raw_live_health": str(health_row.get("live_health") or "UNKNOWN").upper(),
        "live_reasons": reasons,
        "live_loss_streak": live_loss_streak,
        "live_rolling_net_r": live_rolling_net_r,
        "live_sl_sync_failure": live_sl_sync_failure,
        "live_unconfirmed_rr_n": live_unconfirmed_rr_n,
        "entry_unconfirmed": entry_unconfirmed,
        "runtime_error": runtime_error,
        "loss_streak_current": loss_streak_current,
        "loss_streak_stale_after_new_open": loss_streak_stale_after_new_open,
        "last_live_open_key": last_live_open_key,
        "last_live_close_key": last_live_close_key,
    }


def _live_research_current_sl_sync_failure(ctx):
    missing = []
    for trade in getattr(ctx, "trades", []) or []:
        if not isinstance(trade, dict):
            continue
        if str(trade.get("status", "OPEN") or "OPEN").upper() != "OPEN":
            continue
        if str(trade.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        owner = str(trade.get("owner") or "bot").lower()
        if owner != "bot":
            continue
        if not trade.get("exchange_sl_id"):
            missing.append(str(trade.get("symbol") or ""))
            continue
        confirmed = trade.get("exchange_sl_price_confirmed")
        if confirmed in (None, "", False):
            missing.append(str(trade.get("symbol") or ""))
    return missing


def _live_research_current_entry_unconfirmed(ctx):
    """Currently-open research positions whose entry/exchange state is unconfirmed.

    Scoped to OPEN positions only. Mirrors the inverse of the producer's
    open_trade_confirmed_healthy() current-safety checks. A CLOSED trade whose
    exit price was an estimate (rr_unconfirmed on the historical close row) is a
    settled position with a known realized R and must NOT appear here.
    """
    missing = []
    for trade in getattr(ctx, "trades", []) or []:
        if not isinstance(trade, dict):
            continue
        if str(trade.get("status", "OPEN") or "OPEN").upper() != "OPEN":
            continue
        if str(trade.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        owner = str(trade.get("owner") or "bot").lower()
        if owner != "bot":
            continue
        if str(trade.get("entry_price_unconfirmed") or "").lower() in ("true", "1", "yes"):
            missing.append(str(trade.get("symbol") or ""))
            continue
        if trade.get("exchange_order_state_unknown") in (True, "true", "1", "yes"):
            missing.append(str(trade.get("symbol") or ""))
            continue
        entry_source = str(trade.get("entry_source") or "").lower()
        entry_state = str(trade.get("entry_state") or "").upper()
        entry_confirmed = entry_source == "actual_exchange_fill" or entry_state == "ENTRY_CONFIRMED"
        if not entry_confirmed:
            missing.append(str(trade.get("symbol") or ""))
    return missing


def _live_research_has_unmanaged_research_position(ctx):
    for trade in getattr(ctx, "trades", []) or []:
        if trade.get("status", "OPEN") != "OPEN":
            continue
        if str(trade.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        owner = str(trade.get("owner") or "bot").lower()
        if owner != "bot":
            return True
    return False


def _live_research_micro_pause_status(
    ctx=None,
    now_ts=None,
    close_rows=None,
    live_trade_rows=None,
    pause_rows=None,
    health_rows=None,
):
    now_ts = time.time() if now_ts is None else now_ts
    metrics = _live_research_micro_metrics(close_rows=close_rows, live_trade_rows=live_trade_rows)
    pause_hours = _live_research_micro_float(
        config.get("live_research_micro_pause_hours", 3),
        3.0,
    )
    pause_secs = max(0.0, pause_hours * 3600.0)
    health_row = _live_research_micro_latest_health_row(health_rows=health_rows)
    config_status = _live_research_micro_config_status()
    health_freshness = _live_research_micro_health_row_freshness(health_row, now_ts=now_ts)
    live_status = _live_research_micro_health_status(health_row, metrics, freshness=health_freshness)
    paper_health = _live_research_micro_latest_paper_health(health_rows=health_rows)
    current_sl_missing_symbols = _live_research_current_sl_sync_failure(ctx)
    current_entry_unconfirmed_symbols = _live_research_current_entry_unconfirmed(ctx)
    health_warnings = []
    if health_freshness.get("health_row_stale"):
        health_warnings.append("LIVE_HEALTH_ROW_STALE_WARN")
    payload = {
        "event_type": "LIVE_RESEARCH_MICRO_PAUSE",
        "ts": now_ts,
        "pause_until": None,
        "pause_remaining_sec": 0,
        "paper_health": paper_health,
        "paper_active_health": paper_health,
        "live_health": live_status.get("live_health"),
        "raw_live_health": live_status.get("raw_live_health"),
        "live_reasons": live_status.get("live_reasons"),
        "live_loss_streak": live_status.get("live_loss_streak"),
        "live_rolling_net_r": live_status.get("live_rolling_net_r"),
        "live_sl_sync_failure": live_status.get("live_sl_sync_failure"),
        "current_sl_missing_symbols": current_sl_missing_symbols,
        "current_entry_unconfirmed_symbols": current_entry_unconfirmed_symbols,
        "live_unconfirmed_rr_n": live_status.get("live_unconfirmed_rr_n"),
        "live_closed_count": metrics.get("live_closed_count", 0),
        "last_live_close_key": metrics.get("last_live_close_key", ""),
        "max_live_research_trades": config_status.get("max_live_research_trades"),
        "live_risk_per_trade": config_status.get("live_risk_per_trade"),
        "live_max_portfolio_risk": config_status.get("live_max_portfolio_risk"),
        "micro_allowed_despite_paper_red": False,
        "scale_block": bool(config_status.get("scale_block")),
        "health_warnings": health_warnings,
        "action": "ALLOW",
    }
    payload.update(health_freshness)

    if not bool(config.get("live_research_micro_pause_enabled", True)):
        return True, "", payload

    if payload.get("scale_block") and payload.get("health_row_stale"):
        payload.update({
            "pause_reason": "LIVE_SCALE_BLOCKED_STALE_HEALTH",
            "action": "BLOCK_SCALE",
        })
        _live_research_micro_write(payload)
        return False, "LIVE_SCALE_BLOCKED_STALE_HEALTH", payload

    if payload.get("paper_health") == "RED" and payload.get("scale_block"):
        # Option A3: paper RED becomes WARN_ONLY for live scale ONLY when the
        # config flag is set AND every live-side safety condition holds. The
        # default mode ("BLOCK") preserves the original hard-block behavior
        # exactly. Cap/risk are never altered by this policy.
        paper_red_mode = str(config.get("live_paper_red_scale_mode", "BLOCK") or "BLOCK")
        paper_red_open_live_count = (
            _live_smc_research_open_count(ctx) if ctx is not None else None
        )
        paper_red_max_live = config_status.get("max_live_research_trades")
        paper_red_rolling_net_r = _live_research_micro_float(
            live_status.get("live_rolling_net_r"), None
        )
        paper_red_conditions = {
            "mode_warn_only": paper_red_mode == "WARN_ONLY_WHEN_LIVE_HEALTH_OK",
            "live_rolling_net_r_positive": (
                paper_red_rolling_net_r is not None and paper_red_rolling_net_r > 0
            ),
            "loss_streak_not_current": not bool(live_status.get("loss_streak_current")),
            "no_entry_unconfirmed": not current_entry_unconfirmed_symbols,
            "no_sl_missing": not current_sl_missing_symbols,
            "no_sl_sync_failure": not bool(live_status.get("live_sl_sync_failure")),
            "health_row_fresh": bool(health_freshness.get("health_row_fresh")),
            "cap_room": (
                isinstance(paper_red_open_live_count, int)
                and isinstance(paper_red_max_live, int)
                and paper_red_open_live_count < paper_red_max_live
            ),
        }
        # Safety extension: even when the A3 conditions pass, never WARN_ALLOW
        # while a live runtime error is flagged or an active micro-pause window
        # is armed. These mirror the downstream runtime_error / active-pause
        # blocks that the early WARN_ALLOW return would otherwise bypass.
        paper_red_runtime_error = bool(live_status.get("runtime_error"))
        _paper_red_latest_pause, _paper_red_active_pause_until = (
            _live_research_micro_latest_pause(now_ts=now_ts, pause_rows=pause_rows)
        )
        paper_red_pause_remaining_sec = (
            round(max(0.0, _paper_red_active_pause_until - now_ts), 3)
            if _paper_red_active_pause_until
            else 0
        )
        paper_red_active_pause = bool(
            _paper_red_active_pause_until and paper_red_pause_remaining_sec > 0
        )
        payload.update({
            "paper_red_ignored": False,
            "paper_red_ignore_reason": "",
            "original_paper_health": payload.get("paper_health"),
            "original_paper_active_health": payload.get("paper_active_health"),
            "live_paper_red_scale_mode": paper_red_mode,
            "live_rolling_net_r": live_status.get("live_rolling_net_r"),
            "loss_streak_current": bool(live_status.get("loss_streak_current")),
            "current_entry_unconfirmed_symbols": current_entry_unconfirmed_symbols,
            "current_sl_missing_symbols": current_sl_missing_symbols,
            "live_sl_sync_failure": bool(live_status.get("live_sl_sync_failure")),
            "health_row_fresh": bool(health_freshness.get("health_row_fresh")),
            "open_live_count": paper_red_open_live_count,
            "max_live_research_trades": paper_red_max_live,
            "paper_red_conditions": paper_red_conditions,
            "runtime_error_blocker": paper_red_runtime_error,
            "active_pause_blocker": paper_red_active_pause,
            "pause_until": _paper_red_active_pause_until,
            "pause_remaining_sec": paper_red_pause_remaining_sec,
        })
        if all(paper_red_conditions.values()):
            if paper_red_runtime_error:
                payload.update({
                    "paper_red_ignored": False,
                    "paper_red_ignore_reason": "RUNTIME_ERROR_BLOCKER",
                    "pause_reason": "LIVE_SCALE_BLOCKED_PAPER_HEALTH",
                    "action": "BLOCK_SCALE",
                })
                _live_research_micro_write(payload)
                return False, "LIVE_SCALE_BLOCKED_PAPER_HEALTH", payload
            if paper_red_active_pause:
                payload.update({
                    "paper_red_ignored": False,
                    "paper_red_ignore_reason": "ACTIVE_MICRO_PAUSE_BLOCKER",
                    "pause_reason": "LIVE_SCALE_BLOCKED_PAPER_HEALTH",
                    "action": "BLOCK_SCALE",
                })
                _live_research_micro_write(payload)
                return False, "LIVE_SCALE_BLOCKED_PAPER_HEALTH", payload
            payload.update({
                "paper_red_ignored": True,
                "paper_red_ignore_reason": "LIVE_HEALTH_OK_AND_ROLLING_POSITIVE",
                "pause_reason": "LIVE_SCALE_WARN_PAPER_HEALTH_RED_ALLOWED",
                "micro_allowed_despite_paper_red": True,
                "action": "WARN_ALLOW_SCALE",
            })
            _live_research_micro_write(payload)
            return True, "", payload
        _paper_red_failed = [k for k, v in paper_red_conditions.items() if not v]
        payload.update({
            "paper_red_ignore_reason": "BLOCKED:" + ",".join(_paper_red_failed),
            "pause_reason": "LIVE_SCALE_BLOCKED_PAPER_HEALTH",
            "action": "BLOCK_SCALE",
        })
        _live_research_micro_write(payload)
        return False, "LIVE_SCALE_BLOCKED_PAPER_HEALTH", payload

    if payload.get("scale_block") and live_status.get("live_health") == "RED":
        # Current-safety failures must always hard-block, regardless of streak staleness.
        _current_safety_block = bool(
            live_status.get("live_sl_sync_failure")
            or live_status.get("entry_unconfirmed")
            or current_sl_missing_symbols
            or current_entry_unconfirmed_symbols
        )
        _rolling_hard_threshold = _live_research_micro_float(
            config.get("live_research_rolling_net_pause_r", -2.0),
            -2.0,
        )
        _rolling_net_block = (
            _live_research_micro_float(live_status.get("live_rolling_net_r"), 0.0) or 0.0
        ) <= _rolling_hard_threshold
        _stale_streak = bool(live_status.get("loss_streak_stale_after_new_open"))
        _current_streak = bool(live_status.get("loss_streak_current"))
        payload.update({
            "loss_streak_current": _current_streak,
            "loss_streak_stale_after_new_open": _stale_streak,
            "last_live_open_key": live_status.get("last_live_open_key", ""),
            "last_live_close_key": live_status.get("last_live_close_key", payload.get("last_live_close_key", "")),
        })
        if _stale_streak and not _current_safety_block and not _rolling_net_block:
            payload.update({
                "pause_reason": "LIVE_SCALE_WARN_STALE_LOSS_STREAK_AFTER_NEW_OPEN",
                "scale_block_cause": "stale_loss_streak_after_new_open",
                "action": "WARN_ALLOW_SCALE",
            })
            _live_research_micro_write(payload)
            return True, "", payload
        if _current_safety_block:
            _scale_block_reason = "LIVE_SCALE_BLOCKED_LIVE_HEALTH"
            _scale_block_cause = "current_safety"
        elif _rolling_net_block:
            _scale_block_reason = "LIVE_SCALE_BLOCKED_LIVE_HEALTH"
            _scale_block_cause = "rolling_net_hard"
        elif _current_streak:
            _scale_block_reason = "LIVE_SCALE_BLOCKED_LIVE_LOSS_STREAK_CURRENT"
            _scale_block_cause = "current_loss_streak"
        else:
            _scale_block_reason = "LIVE_SCALE_BLOCKED_LIVE_HEALTH"
            _scale_block_cause = "live_health"
        payload.update({
            "pause_reason": _scale_block_reason,
            "scale_block_cause": _scale_block_cause,
            "action": "BLOCK_SCALE",
        })
        _live_research_micro_write(payload)
        return False, _scale_block_reason, payload

    if live_status.get("live_sl_sync_failure") or live_status.get("entry_unconfirmed") or current_sl_missing_symbols or current_entry_unconfirmed_symbols:
        payload.update({
            "pause_reason": "LIVE_MICRO_BLOCKED_SL_SYNC",
            "action": "BLOCK",
        })
        _live_research_micro_write(payload)
        return False, "LIVE_MICRO_BLOCKED_SL_SYNC", payload

    if live_status.get("runtime_error"):
        payload.update({
            "pause_reason": "LIVE_MICRO_BLOCKED_LIVE_HEALTH",
            "action": "BLOCK",
        })
        _live_research_micro_write(payload)
        return False, "LIVE_MICRO_BLOCKED_LIVE_HEALTH", payload

    latest_pause, active_pause_until = _live_research_micro_latest_pause(
        now_ts=now_ts,
        pause_rows=pause_rows,
    )
    if active_pause_until:
        active_pause_reason = str(latest_pause.get("pause_reason") or "LIVE_MICRO_PAUSE_ACTIVE")
        active_block_reason = "live_micro_pause"
        if active_pause_reason == "LIVE_MICRO_PAUSE_3_LOSS_STREAK":
            active_block_reason = "LIVE_MICRO_BLOCKED_LOSS_STREAK"
        elif active_pause_reason == "LIVE_MICRO_PAUSE_ROLLING_NET":
            active_block_reason = "LIVE_MICRO_BLOCKED_ROLLING_NET"
        payload.update({
            "pause_reason": active_pause_reason,
            "block_reason": active_block_reason,
            "pause_until": active_pause_until,
            "pause_remaining_sec": round(max(0.0, active_pause_until - now_ts), 3),
            "live_loss_streak": _live_research_micro_float(
                latest_pause.get("live_loss_streak"),
                payload.get("live_loss_streak"),
            ),
            "live_rolling_net_r": _live_research_micro_float(
                latest_pause.get("live_rolling_net_r"),
                payload.get("live_rolling_net_r"),
            ),
            "live_closed_count": int(_live_research_micro_float(
                latest_pause.get("live_closed_count"),
                payload.get("live_closed_count"),
            ) or 0),
            "last_live_close_key": str(
                latest_pause.get("last_live_close_key")
                or payload.get("last_live_close_key")
                or ""
            ),
            "pause_armed_live_closed_count": int(_live_research_micro_float(
                latest_pause.get("pause_armed_live_closed_count", latest_pause.get("live_closed_count")),
                payload.get("live_closed_count"),
            ) or 0),
            "pause_armed_last_live_close_key": str(
                latest_pause.get("pause_armed_last_live_close_key")
                or latest_pause.get("last_live_close_key")
                or payload.get("last_live_close_key")
                or ""
            ),
            "action": "BLOCK",
        })
        _live_research_micro_write(payload)
        return False, active_block_reason, payload

    pause_reason = ""
    streak_threshold = int(config.get("live_research_loss_streak_pause_count", 3) or 3)
    rolling_threshold = _live_research_micro_float(
        config.get("live_research_rolling_net_pause_r", -2.0),
        -2.0,
    )
    if live_status.get("live_loss_streak", 0) >= streak_threshold:
        pause_reason = "LIVE_MICRO_PAUSE_3_LOSS_STREAK"
    elif live_status.get("live_rolling_net_r", 0.0) <= rolling_threshold:
        pause_reason = "LIVE_MICRO_PAUSE_ROLLING_NET"

    if pause_reason:
        block_reason = (
            "LIVE_MICRO_BLOCKED_LOSS_STREAK"
            if pause_reason == "LIVE_MICRO_PAUSE_3_LOSS_STREAK"
            else "LIVE_MICRO_BLOCKED_ROLLING_NET"
        )
        previous_pause = _live_research_micro_latest_pause_for_reason(
            pause_reason,
            pause_rows=pause_rows,
        )
        if (
            previous_pause is not None
            and _live_research_micro_same_pause_arm_identity(payload, previous_pause)
        ):
            previous_pause_until = _live_research_micro_float(previous_pause.get("pause_until"))
            payload.update({
                "pause_reason": pause_reason,
                "previous_pause_until": previous_pause_until,
                "pause_until": None,
                "pause_remaining_sec": 0,
                "reason": "MICRO_PAUSE_NOT_REARMED_STALE_STREAK",
                "action": "ALLOW_AFTER_PAUSE_EXPIRY_NO_NEW_CLOSE",
            })
            _live_research_micro_write(payload)
            if latest_pause is not None and _live_research_has_unmanaged_research_position(ctx):
                payload.update({
                    "pause_reason": "LIVE_MICRO_PAUSE_UNMANAGED_LIVE_POSITION",
                    "pause_until": now_ts + pause_secs,
                    "pause_remaining_sec": round(pause_secs, 3),
                    "action": "EXTEND_AND_BLOCK",
                })
                _live_research_micro_write(payload)
                return False, "live_micro_pause", payload
            if live_status.get("live_health") == "RED":
                payload.update({
                    "pause_reason": "LIVE_MICRO_BLOCKED_LIVE_HEALTH",
                    "action": "BLOCK",
                })
                _live_research_micro_write(payload)
                return False, "LIVE_MICRO_BLOCKED_LIVE_HEALTH", payload
            if payload.get("paper_health") == "RED":
                payload.update({
                    "pause_reason": "LIVE_MICRO_WARN_PAPER_HEALTH_RED_ALLOWED",
                    "micro_allowed_despite_paper_red": True,
                    "action": "ALLOW_AFTER_PAUSE_EXPIRY_NO_NEW_CLOSE",
                })
                _live_research_micro_write(payload)
            return True, "", payload
        pause_until = now_ts + pause_secs
        payload.update({
            "pause_reason": pause_reason,
            "block_reason": block_reason,
            "pause_until": pause_until,
            "pause_remaining_sec": round(pause_secs, 3),
            "pause_armed_live_closed_count": int(metrics.get("live_closed_count", 0) or 0),
            "pause_armed_last_live_close_key": metrics.get("last_live_close_key", ""),
            "action": "SET_AND_BLOCK",
        })
        _live_research_micro_write(payload)
        return False, block_reason, payload

    if latest_pause is not None:
        if _live_research_has_unmanaged_research_position(ctx):
            payload.update({
                "pause_reason": "LIVE_MICRO_PAUSE_UNMANAGED_LIVE_POSITION",
                "pause_until": now_ts + pause_secs,
                "pause_remaining_sec": round(pause_secs, 3),
                "action": "EXTEND_AND_BLOCK",
            })
            _live_research_micro_write(payload)
            return False, "live_micro_pause", payload
        if live_status.get("live_health") == "RED":
            payload.update({
                "pause_reason": "LIVE_MICRO_BLOCKED_LIVE_HEALTH",
                "action": "BLOCK",
            })
            _live_research_micro_write(payload)
            return False, "LIVE_MICRO_BLOCKED_LIVE_HEALTH", payload

    if live_status.get("live_health") == "RED":
        payload.update({
            "pause_reason": "LIVE_MICRO_BLOCKED_LIVE_HEALTH",
            "action": "BLOCK",
        })
        _live_research_micro_write(payload)
        return False, "LIVE_MICRO_BLOCKED_LIVE_HEALTH", payload

    if payload.get("paper_health") == "RED":
        payload.update({
            "pause_reason": "LIVE_MICRO_WARN_PAPER_HEALTH_RED_ALLOWED",
            "micro_allowed_despite_paper_red": True,
            "action": "WARN_ALLOW",
        })
        _live_research_micro_write(payload)
    elif payload.get("health_row_stale"):
        payload.update({
            "pause_reason": "LIVE_HEALTH_ROW_STALE_WARN",
            "action": "WARN_ALLOW",
        })
        _live_research_micro_write(payload)

    return True, "", payload


def _live_smc_research_failure_extra(trade, exc=None):
    trade = trade if isinstance(trade, dict) else {}
    failure = trade.get("_open_failure")
    if not isinstance(failure, dict):
        failure = {}

    extra = {
        "fail_stage": failure.get("fail_stage") or ("open_trade_exception" if exc else "open_trade"),
        "fail_reason": failure.get("fail_reason") or ("open_trade raised exception" if exc else "open_trade returned falsy"),
        "live_mode": bool(config.get("live_mode", False)),
        "live_smc_research_enabled": bool(config.get("live_smc_research_enabled", False)),
        "live_confirm_enabled": bool(config.get("live_confirm_enabled", False)),
        "live_confirm_enabled_blocked": False,
        "entry": trade.get("entry"),
        "sl": trade.get("sl"),
        "tp": trade.get("tp"),
        "rr": trade.get("rr"),
        "symbol": str(trade.get("symbol") or ""),
        "side": str(trade.get("side") or "").upper(),
        "entry_type": trade.get("entry_type"),
        "dedup_key": trade.get("research_dedup_key") or trade.get("dedup_key"),
        "client_order_id": trade.get("client_order_id") or trade.get("exchange_client_id"),
        "exchange_order_id": trade.get("exchange_order_id"),
        "exchange_fill_price": trade.get("exchange_fill_price"),
    }
    if exc is not None:
        extra["exception_type"] = type(exc).__name__
        extra["exception_message"] = str(exc)
    for key in (
        "exception_type",
        "exception_message",
        "qty",
        "tier",
        "raw_qty",
        "rounded_qty",
        "notional",
        "min_notional",
        "rounded_price",
        "sl_distance",
        "sl_distance_pct",
        "leverage",
        "margin",
        "free_balance",
        "required_leverage",
        "final_leverage",
        "target_leverage",
        "allowed_leverage",
        "margin_required",
        "leverage_mode",
        "leverage_source",
        "exchange_max_leverage",
        "safety_gate_reject_reason",
        "exchange_response",
        "open_trades",
        "remaining_secs",
        "current_total_risk",
        "add_risk",
        "max_portfolio_risk",
        "current_symbol_risk",
        "max_symbol_risk",
        "open_live_count",
        "pending_live_count",
        "effective_live_count",
        "max_live_trades",
    ):
        if key in failure:
            extra[key] = failure.get(key)
    return _live_smc_research_json_safe(extra)


def _live_smc_research_prefilter_extra(stage, reason, trade, ctx=None, detail=None):
    trade = trade if isinstance(trade, dict) else {}
    extra = {
        "live_mode": bool(config.get("live_mode", False)),
        "live_smc_research_enabled": bool(config.get("live_smc_research_enabled", False)),
        "live_confirm_enabled": bool(config.get("live_confirm_enabled", False)),
        "prefilter_stage": stage,
        "prefilter_reason": reason,
        "entry": trade.get("entry"),
        "sl": trade.get("sl"),
        "tp": trade.get("tp"),
        "rr": trade.get("rr"),
        "score": trade.get("score"),
        "symbol": str(trade.get("symbol") or ""),
        "side": str(trade.get("side") or "").upper(),
        "entry_type": trade.get("entry_type"),
        "dedup_key": trade.get("research_dedup_key") or trade.get("dedup_key"),
        "execution_mode": getattr(ctx, "execution_mode", None) if ctx is not None else None,
        "client_order_id": None,
        "exchange_order_id": None,
    }
    if isinstance(detail, dict):
        extra.update({
            key: _live_smc_research_json_safe(value)
            for key, value in detail.items()
        })
    elif detail:
        extra["prefilter_detail"] = str(detail)
    extra.update(_live_smc_research_score_alignment_extra(trade))
    return _live_smc_research_json_safe(extra)


def _live_smc_research_terminal_identity(candidate, trade=None):
    trade = trade if isinstance(trade, dict) else {}
    symbol = str(trade.get("symbol") or candidate.get("symbol") or "")
    side = str(trade.get("side") or candidate.get("side") or "").upper()
    entry_type = str(trade.get("entry_type") or candidate.get("entry_type") or "CONFIRM_SMC_RESEARCH")
    dedup_key = str(
        trade.get("research_dedup_key")
        or trade.get("dedup_key")
        or candidate.get("dedup_key")
        or ""
    ).strip()
    signal_ts = _first_nonblank(
        trade.get("signal_created_ts"),
        candidate.get("signal_created_ts"),
        candidate.get("source_timestamp"),
        candidate.get("source_row_time"),
        candidate.get("timestamp"),
        candidate.get("collector_ts"),
    )
    signal_key = dedup_key or str(signal_ts or "")
    return "|".join([symbol, side, entry_type, signal_key])


def _live_smc_research_terminal_failure_key(candidate, trade, stage, reason):
    return "|".join([
        _live_smc_research_terminal_identity(candidate, trade),
        str(stage or ""),
        str(reason or ""),
    ])


def _live_smc_research_terminal_failure_suppressed(candidate, trade, stage, reason, decision, now_ts=None):
    now_ts = time.time() if now_ts is None else now_ts
    expired = [
        key for key, row in _live_smc_research_terminal_failures.items()
        if now_ts - float(row.get("ts", 0)) >= _LIVE_SMC_RESEARCH_TERMINAL_FAILURE_TTL_SECS
    ]
    for key in expired:
        _live_smc_research_terminal_failures.pop(key, None)

    key = _live_smc_research_terminal_failure_key(candidate, trade, stage, reason)
    existing = _live_smc_research_terminal_failures.get(key)
    if existing and now_ts - float(existing.get("ts", 0)) < _LIVE_SMC_RESEARCH_TERMINAL_FAILURE_TTL_SECS:
        return True
    _live_smc_research_terminal_failures[key] = {
        "ts": now_ts,
        "decision": decision,
        "stage": str(stage or ""),
        "reason": str(reason or ""),
        "identity": _live_smc_research_terminal_identity(candidate, trade),
    }
    return False


def _live_smc_research_active_terminal_failure(candidate, trade, now_ts=None):
    now_ts = time.time() if now_ts is None else now_ts
    identity = _live_smc_research_terminal_identity(candidate, trade)
    for row in list(_live_smc_research_terminal_failures.values()):
        if row.get("identity") != identity:
            continue
        if now_ts - float(row.get("ts", 0)) < _LIVE_SMC_RESEARCH_TERMINAL_FAILURE_TTL_SECS:
            return row
    return None


def _live_smc_research_gate_reason_code(reason):
    text = str(reason or "")
    lower = text.lower()
    if "live_mode is not true" in lower:
        return "live_mode_disabled"
    if "live_smc_research_enabled is not true" in lower:
        return "live_smc_research_disabled"
    if "execution_mode=" in lower:
        return "execution_mode_not_live"
    if "entry_type" in lower:
        return "entry_type_not_confirm_smc_research"
    if "symbol" in lower and "malformed" in lower:
        return "invalid_required_field"
    if "tier5" in lower:
        return "research_predicate_fail"
    if "missing or invalid" in lower:
        return "invalid_required_field"
    if "geometry violated" in lower:
        return "invalid_geometry"
    if "planned_rr=" in lower or "cannot compute rr" in lower:
        return "rr_below_min"
    if "bos_quality=weak" in lower or "volume_confirmation=expansion" in lower:
        return "research_predicate_fail"
    if "live_research_open=" in lower:
        return "max_live_research_trades"
    return "live_research_gate_reject"


def _live_smc_research_min_notional_prefilter(trade):
    trade = trade if isinstance(trade, dict) else {}
    symbol = str(trade.get("symbol") or "").upper()
    try:
        entry = float(trade.get("entry"))
        sl = float(trade.get("sl"))
    except (TypeError, ValueError):
        return True, {}

    sl_distance = abs(entry - sl)
    if entry <= 0 or sl <= 0 or sl_distance <= 0:
        return True, {}
    sl_pct = sl_distance / entry

    try:
        from exchange import live_executor as _live_executor
        execution_balance = _live_executor.get_execution_balance()
    except Exception:
        execution_balance = None
    execution_balance = _live_research_micro_float(execution_balance)
    live_risk_per_trade = _live_research_micro_float(config.get("live_risk_per_trade"))
    if execution_balance is None or execution_balance <= 0 or live_risk_per_trade is None or live_risk_per_trade <= 0:
        return False, {
            "min_notional_prefilter_reason": "execution_balance_or_live_risk_unavailable",
            "symbol": symbol,
            "execution_balance": execution_balance,
            "live_risk_per_trade": live_risk_per_trade,
            "entry": entry,
            "sl": sl,
            "sl_distance": sl_distance,
            "sl_pct": sl_pct,
        }

    score = _live_research_micro_float(trade.get("score"), 0.0) or 0.0
    min_score = _live_smc_research_min_open_score()
    effective_risk_pct = live_risk_per_trade * 0.5 if score < min_score else live_risk_per_trade
    risk_amount = execution_balance * effective_risk_pct
    projected_notional = risk_amount / sl_pct if sl_pct > 0 else 0.0

    filters = None
    try:
        from exchange.precision import get_symbol_filters
        filters = get_symbol_filters(symbol)
    except Exception:
        filters = None
    if filters is None:
        return False, {
            "min_notional_prefilter_reason": "min_notional_lookup_failed",
            "symbol": symbol,
            "execution_balance": execution_balance,
            "live_risk_per_trade": live_risk_per_trade,
            "effective_risk_pct": effective_risk_pct,
            "risk_amount": risk_amount,
            "entry": entry,
            "sl": sl,
            "sl_distance": sl_distance,
            "sl_pct": sl_pct,
            "projected_notional": projected_notional,
            "min_notional": None,
            "min_notional_floor_allowed": bool(config.get("min_notional_floor_allowed", False)),
        }

    min_notional = _live_research_micro_float(filters.get("min_notional"), 0.0) or 0.0
    floor_risk_amount = min_notional * sl_pct
    floor_risk_pct_of_balance = (
        floor_risk_amount / execution_balance
        if execution_balance > 0
        else None
    )
    min_notional_floor_allowed = bool(config.get("min_notional_floor_allowed", False))
    would_floor_violate_cap = floor_risk_amount > (execution_balance * live_risk_per_trade)
    detail = {
        "symbol": symbol,
        "execution_balance": execution_balance,
        "live_risk_per_trade": live_risk_per_trade,
        "effective_risk_pct": effective_risk_pct,
        "risk_amount": risk_amount,
        "entry": entry,
        "sl": sl,
        "sl_distance": sl_distance,
        "sl_pct": sl_pct,
        "projected_notional": projected_notional,
        "min_notional": min_notional,
        "floor_risk_amount": floor_risk_amount,
        "floor_risk_pct_of_balance": floor_risk_pct_of_balance,
        "min_notional_floor_allowed": min_notional_floor_allowed,
        "would_floor_violate_cap": would_floor_violate_cap,
    }
    if min_notional <= 0 or projected_notional >= min_notional:
        detail["min_notional_prefilter_passed"] = True
        return True, detail

    detail["min_notional_prefilter_reason"] = "projected_notional_below_min_notional"
    if min_notional_floor_allowed and not would_floor_violate_cap:
        detail["min_notional_floor_unsupported"] = True
        detail["unsupported_floor_reason"] = "execution_validate_and_prepare_does_not_support_floor_sizing"
    return False, detail


def _live_smc_research_prefilter(candidate, trade, ctx):
    if not isinstance(trade, dict):
        return False, "trade_build", "invalid_trade", "trade builder did not return dict"

    if not config.get("live_smc_research_enabled", False):
        return False, "config", "live_smc_research_disabled", "live_smc_research_enabled is not true"

    exec_mode = getattr(ctx, "execution_mode", None) if ctx is not None else None
    if exec_mode not in ("live", "paper_live"):
        return False, "execution_mode", "execution_mode_not_live", f"execution_mode={exec_mode!r}"

    if str(trade.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
        return False, "entry_type", "entry_type_not_confirm_smc_research", trade.get("entry_type")

    symbol = str(trade.get("symbol") or "")
    side = str(trade.get("side") or "").upper()
    if not symbol:
        return False, "required_fields", "missing_required_field", "symbol"
    if side not in {"LONG", "SHORT"}:
        return False, "required_fields", "invalid_required_field", f"side={side!r}"

    values = {}
    for field in ("entry", "sl", "tp"):
        raw = trade.get(field)
        if raw is None:
            return False, "required_fields", "missing_required_field", field
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return False, "required_fields", "invalid_required_field", f"{field}={raw!r}"
        if value <= 0:
            return False, "required_fields", "invalid_required_field", f"{field}={value}"
        values[field] = value

    if side == "LONG" and not (values["sl"] < values["entry"] < values["tp"]):
        return False, "geometry", "invalid_geometry", "LONG requires sl < entry < tp"
    if side == "SHORT" and not (values["tp"] < values["entry"] < values["sl"]):
        return False, "geometry", "invalid_geometry", "SHORT requires tp < entry < sl"

    rr_raw = trade.get("rr")
    if rr_raw is None:
        return False, "rr", "rr_missing", "rr is missing"
    try:
        rr_val = float(rr_raw)
    except (TypeError, ValueError):
        return False, "rr", "rr_missing", f"rr={rr_raw!r}"
    if rr_val < 2.0:
        return False, "rr", "rr_below_min", f"rr={rr_val}"

    score_raw = trade.get("score")
    try:
        score_val = float(score_raw)
    except (TypeError, ValueError):
        score_val = 0.0
    min_score = _live_smc_research_min_open_score()

    bos_quality = str(trade.get("bos_quality") or "").upper()
    if bos_quality == "WEAK":
        return False, "research_predicate", "research_predicate_fail", "bos_quality=WEAK"
    volume_confirmation = str(trade.get("volume_confirmation") or "").upper()
    if volume_confirmation == "EXPANSION":
        return False, "research_predicate", "research_predicate_fail", "volume_confirmation=EXPANSION"

    if score_val < min_score:
        trade.update(_live_smc_research_score_alignment_extra(trade, paper_predicate_aligned=True))

    micro_ok, micro_reason, micro_detail = _live_research_micro_pause_status(ctx=ctx)
    if not micro_ok:
        return False, "live_micro_pause", micro_reason, micro_detail

    notional_ok, notional_detail = _live_smc_research_min_notional_prefilter(trade)
    if not notional_ok:
        return False, "live_min_notional", "LIVE_MIN_NOTIONAL_PREFILTER", notional_detail

    try:
        from exchange import live_executor as _live_executor
        allowed, gate_reason = _live_executor.check_live_research_safety_gate(
            trade,
            ctx=ctx,
            open_trades=getattr(ctx, "trades", None) if ctx is not None else None,
        )
    except Exception as exc:
        return False, "live_research_gate", "live_research_gate_reject", f"{type(exc).__name__}: {exc}"
    if not allowed:
        return False, "live_research_gate", _live_smc_research_gate_reason_code(gate_reason), gate_reason

    return True, "ok", "ok", ""


def _live_smc_research_log(candidate, decision, reason="", trade=None, extra=None):
    try:
        candidate = candidate if isinstance(candidate, dict) else {}
        trade = trade if isinstance(trade, dict) else trade
        row = {
            "ts": time.time(),
            "decision": decision,
            "reason": reason,
            "symbol": str(candidate.get("symbol") or ""),
            "side": str(candidate.get("side") or "").upper(),
            "dedup_key": str(candidate.get("dedup_key") or ""),
        }
        if trade is not None:
            row["entry"] = trade.get("entry")
            row["sl"] = trade.get("sl")
            row["tp"] = trade.get("tp")
            row["rr"] = trade.get("rr")
            row["score"] = trade.get("score")
            row["entry_type"] = trade.get("entry_type")
        if isinstance(extra, dict):
            row.update(extra)
        btc_source = dict(candidate)
        if isinstance(trade, dict):
            btc_source.update(trade)
        btc_source["decision_ts"] = row.get("ts")
        btc_context = _btc_mtf_context_for_signal(
            btc_source,
            side=_first_nonblank(row.get("side"), btc_source.get("side")),
            now_ts=row.get("ts"),
        )
        row.update({key: _live_smc_research_json_safe(value) for key, value in btc_context.items()})
        # SMC_PA_SCORE_V3_SHADOW (log-only): additive annotation, never gates.
        smc_pa_v3_summary = _smc_pa_score_v3_shadow(
            candidate,
            fields=None,
            trade=trade if isinstance(trade, dict) else None,
            execution_mode="live",
            v1_decision=decision,
            v1_reason=reason,
            btc_ctx=btc_context,
            now_ts=row.get("ts"),
        )
        if smc_pa_v3_summary:
            row.update({
                key: _live_smc_research_json_safe(value)
                for key, value in smc_pa_v3_summary.items()
            })
        # BREAKOUT_ACCEPTANCE_SHADOW (log-only): appends to its own forward
        # log only; never gates and never adds fields to this decision row.
        _breakout_acceptance_shadow(
            candidate,
            fields=None,
            trade=trade if isinstance(trade, dict) else None,
            execution_mode="live",
            v1_decision=decision,
            v1_reason=reason,
            btc_ctx=btc_context,
            v3_summary=smc_pa_v3_summary,
            now_ts=row.get("ts"),
        )
        os.makedirs("logs", exist_ok=True)
        with open(_LIVE_SMC_RESEARCH_DECISION_LOG, "a", encoding="utf-8") as _fh:
            _fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        _btc_alignment_instrumentation_shadow(
            btc_source,
            execution_mode="live",
            v1_decision=decision,
            v1_reason=reason,
            side=_first_nonblank(row.get("side"), btc_source.get("side")),
            trade=trade if isinstance(trade, dict) else None,
            gate_fields=btc_source,
            v2b_fields=btc_source,
            now_ts=row.get("ts"),
        )
    except Exception as _log_ex:
        print(f"[WARN] _live_smc_research_log failed: {_log_ex}")

def _live_smc_research_open_count(ctx):
    return sum(
        1 for t in (getattr(ctx, "trades", None) or [])
        if t.get("status") == "OPEN" and (
            t.get("entry_type") == "CONFIRM_SMC_RESEARCH"
            or t.get("strategy_family") == "confirm_smc_research"
        )
    )


def _dispatch_live_smc_research_lane(ctx):
    if ctx is None:
        return
    if getattr(ctx, "execution_mode", None) not in ("live", "paper_live"):
        return
    if not config.get("live_smc_research_enabled", False):
        return

    from execution import open_trade
    try:
        from entry import get_confirm_structural_outcome_candidates_snapshot
        candidates = get_confirm_structural_outcome_candidates_snapshot()
    except Exception as exc:
        print(f"[LIVE SMC RESEARCH] candidate snapshot failed: {exc}")
        return

    now_ts = time.time()

    for candidate in candidates:
        symbol = str(candidate.get("symbol") or "")
        dedup_key = str(candidate.get("dedup_key") or "")

        if not symbol:
            continue

        # Dedup guard (session-level key, separate from paper set)
        if dedup_key and dedup_key in _live_smc_research_dedup_keys:
            continue

        # Per-symbol open lock (any OPEN trade in live ctx blocks same symbol)
        symbol_locked = _paper_smc_research_symbol_open(ctx, symbol)
        if symbol_locked:
            _live_smc_research_log(
                candidate,
                "SYMBOL_LOCKED",
                f"symbol={symbol} already open in live ctx",
            )
            continue

        trade = _paper_smc_research_trade(candidate)
        location_fields = _paper_smc_research_qualified_fields(candidate)
        location_gate_extra = _confirm_smc_location_gate_log_fields(
            candidate,
            fields=location_fields,
            mode="LIVE_SHADOW_ONLY",
        )
        smc_entry_v2_extra = _smc_entry_v2_shadow(
            candidate,
            fields=location_fields,
            v1_decision="LIVE_SHADOW_ONLY",
            v1_reason="live_decision_unchanged",
            trade=trade,
            now_ts=now_ts,
        )
        smc_entry_v2b_allowlist_extra = _smc_entry_v2b_allowlist_shadow(
            candidate,
            fields=location_fields,
            trade=trade,
            mode="LIVE_SHADOW_ONLY",
        )
        _smc_entry_v2_shadow_write(
            candidate,
            location_fields,
            smc_entry_v2_extra,
            v1_decision="LIVE_SHADOW_ONLY",
            v1_reason="live_decision_unchanged",
            now_ts=now_ts,
        )
        _smc_entry_v2b_allowlist_shadow_write(
            candidate,
            location_fields,
            smc_entry_v2b_allowlist_extra,
            execution_mode="live",
            v1_decision="LIVE_SHADOW_ONLY",
            v1_reason="live_decision_unchanged",
            trade=trade,
            now_ts=now_ts,
        )

        prefilter_ok, prefilter_stage, prefilter_reason, prefilter_detail = _live_smc_research_prefilter(
            candidate,
            trade,
            ctx,
        )
        if not prefilter_ok:
            if not _live_smc_research_terminal_failure_suppressed(
                candidate,
                trade,
                prefilter_stage,
                prefilter_reason,
                "PREFILTER_REJECT",
                now_ts=now_ts,
            ):
                _live_smc_research_log(
                    candidate,
                    "PREFILTER_REJECT",
                    prefilter_reason,
                    trade=trade,
                    extra={
                        **_live_smc_research_prefilter_extra(
                            prefilter_stage,
                            prefilter_reason,
                            trade,
                            ctx=ctx,
                            detail=prefilter_detail,
                        ),
                        **location_gate_extra,
                        **smc_entry_v2_extra,
                        **smc_entry_v2b_allowlist_extra,
                    },
                )
            continue

        active_failure = _live_smc_research_active_terminal_failure(candidate, trade, now_ts=now_ts)
        if active_failure:
            continue

        _live_smc_research_log(
            candidate,
            "OPEN_ATTEMPT",
            trade=trade,
            extra={
                **_live_smc_research_score_alignment_extra(trade),
                **location_gate_extra,
                **smc_entry_v2_extra,
                **smc_entry_v2b_allowlist_extra,
            },
        )

        trade_for_open = copy.deepcopy(trade)
        try:
            success = open_trade(trade_for_open, ctx)
        except Exception as exc:
            _failure_extra = _live_smc_research_failure_extra(trade_for_open, exc=exc)
            _fail_stage = _failure_extra.get("fail_stage")
            _fail_reason = _failure_extra.get("fail_reason")
            if not _live_smc_research_terminal_failure_suppressed(
                candidate,
                trade_for_open,
                _fail_stage,
                _fail_reason,
                "OPEN_FAILED",
                now_ts=now_ts,
            ):
                _live_smc_research_log(
                    candidate,
                    "OPEN_FAILED",
                    "open_trade raised exception",
                    trade=trade_for_open,
                    extra={
                        **_failure_extra,
                        **location_gate_extra,
                        **smc_entry_v2_extra,
                        **smc_entry_v2b_allowlist_extra,
                    },
                )
            raise
        if success:
            _live_smc_research_dedup_keys.add(dedup_key)
            _live_smc_research_log(
                candidate,
                "OPEN_ACCEPTED",
                trade=trade_for_open,
                extra={
                    **location_gate_extra,
                    **smc_entry_v2_extra,
                    **smc_entry_v2b_allowlist_extra,
                },
            )
        else:
            _failure_extra = _live_smc_research_failure_extra(trade_for_open)
            _fail_stage = _failure_extra.get("fail_stage")
            _fail_reason = _failure_extra.get("fail_reason")
            if not _live_smc_research_terminal_failure_suppressed(
                candidate,
                trade_for_open,
                _fail_stage,
                _fail_reason,
                "OPEN_FAILED",
                now_ts=now_ts,
            ):
                _live_smc_research_log(
                    candidate,
                    "OPEN_FAILED",
                    "open_trade returned falsy",
                    trade=trade_for_open,
                    extra={
                        **_failure_extra,
                        **location_gate_extra,
                        **smc_entry_v2_extra,
                        **smc_entry_v2b_allowlist_extra,
                    },
                )

def _paper_smc_main_candidate_type(candidate):
    candidate_type = str(candidate.get("candidate_type") or "").upper()
    if candidate_type:
        return candidate_type
    reason = str(candidate.get("reason") or "").upper()
    if reason == "ACCEPT":
        return "ACCEPTED_CONFIRM"
    return reason


def _paper_smc_main_dedup_key(candidate):
    key = str(candidate.get("smc_main_dedup_key") or candidate.get("dedup_key") or "").strip()
    if key:
        return key
    source_ts = _first_nonblank(
        candidate.get("signal_created_ts"),
        candidate.get("source_timestamp"),
        candidate.get("source_row_time"),
        candidate.get("timestamp"),
        candidate.get("collector_ts"),
    )
    return "|".join([
        str(candidate.get("symbol") or ""),
        str(candidate.get("side") or "").upper(),
        "CONFIRM",
        str(source_ts or ""),
    ])


def _paper_smc_main_candidate_priority_info(candidate):
    default_priority = ["ACCEPTED_CONFIRM", "MID_SCORE_WEAK_BOS", "LOW_SCORE", "RR_FAIL"]
    raw_priority = config.get("paper_smc_main_candidate_type_priority", default_priority)
    if not isinstance(raw_priority, (list, tuple)):
        raw_priority = default_priority
    priority_order = {
        str(candidate_type).upper(): idx
        for idx, candidate_type in enumerate(raw_priority)
    }
    candidate_type = _paper_smc_main_candidate_type(candidate)
    fallback_priority = len(priority_order)
    return {
        "candidate_priority": priority_order.get(candidate_type, fallback_priority),
        "candidate_priority_source": (
            "config" if "paper_smc_main_candidate_type_priority" in config else "default"
        ),
    }


def _paper_smc_main_sort_score(candidate):
    structural_context = _paper_structural_context(candidate)
    shadow_score = _paper_smc_research_float(
        _first_nonblank(
            candidate.get("score_v2_structural_shadow"),
            structural_context.get("score_v2_structural_shadow"),
        )
    )
    current_score = _paper_smc_research_float(
        _first_nonblank(
            candidate.get("score_v2_current"),
            structural_context.get("score_v2_current"),
        )
    )
    score = shadow_score if shadow_score is not None else current_score
    if score is None:
        return None
    modifier = 0.0
    if bool(config.get("paper_smc_main_use_structural_modifier", True)):
        try:
            modifier, _, _ = _paper_structural_score_modifier(candidate)
        except Exception:
            modifier = 0.0
    return round(score + modifier, 4)


def _paper_smc_main_ranked_candidates(candidates):
    ranking_enabled = bool(config.get("paper_smc_main_rank_candidates", True))
    candidate_rows = []
    for original_index, raw_candidate in enumerate(list(candidates or [])):
        priority_info = _paper_smc_main_candidate_priority_info(raw_candidate)
        effective_score = _paper_smc_main_sort_score(raw_candidate)
        source_ts = _paper_smc_research_source_ts(raw_candidate)
        sort_key = (
            priority_info.get("candidate_priority", 0),
            -(effective_score if effective_score is not None else float("-inf")),
            -(source_ts if source_ts is not None else float("-inf")),
            original_index,
        )
        candidate_rows.append({
            "candidate": raw_candidate,
            "sort_key": sort_key,
            "original_index": original_index,
            "candidate_priority": priority_info.get("candidate_priority"),
            "candidate_priority_source": priority_info.get("candidate_priority_source"),
        })
    if ranking_enabled:
        candidate_rows.sort(key=lambda row: row["sort_key"])
    for rank_index, row in enumerate(candidate_rows):
        yield {
            "candidate": row["candidate"],
            "rank_index": rank_index if ranking_enabled else None,
            "original_index": row.get("original_index"),
            "candidate_priority": row.get("candidate_priority"),
            "ranking_enabled": ranking_enabled,
            "candidate_priority_source": row.get("candidate_priority_source"),
        }


def _paper_smc_main_open_count(ctx):
    return sum(
        1 for trade in getattr(ctx, "trades", []) or []
        if trade.get("status", "OPEN") == "OPEN"
        and trade.get("owner", "bot") == "bot"
        and (
            trade.get("strategy_family") == "paper_smc_main"
            or trade.get("entry_type") == "PAPER_SMC_MAIN"
        )
    )


def _paper_smc_main_key_open(ctx, dedup_key):
    if not dedup_key:
        return False
    return any(
        trade.get("status", "OPEN") == "OPEN"
        and (
            trade.get("strategy_family") == "paper_smc_main"
            or trade.get("entry_type") == "PAPER_SMC_MAIN"
        )
        and (
            trade.get("smc_main_dedup_key") == dedup_key
            or trade.get("research_dedup_key") == dedup_key
        )
        for trade in getattr(ctx, "trades", []) or []
    )


def _paper_smc_main_symbol_open(ctx, symbol):
    for trade in getattr(ctx, "trades", []) or []:
        if trade.get("status", "OPEN") != "OPEN":
            continue
        if trade.get("owner", "bot") != "bot":
            continue
        if trade.get("symbol") == symbol:
            return True
    return False


def _paper_smc_main_boundary_guard_info(
    candidate,
    ctx,
    score,
    effective_score,
    modifier,
    modifier_reasons=None,
):
    if not bool(config.get("paper_smc_main_use_boundary_guard", True)):
        return {}
    if not bool(config.get("paper_structural_score_modifier_boundary_guard_enabled", True)):
        return {}
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return {}
    if config.get("paper_smc_main_live_enabled", False):
        return {}
    if candidate.get("geometry_status") != "VALID_GEOMETRY":
        return {}
    if not candidate.get("outcome_trackable"):
        return {}

    threshold = _paper_smc_research_float(
        config.get("paper_smc_main_min_score_v2_structural_shadow", 2.5)
    )
    margin = _paper_smc_research_float(
        config.get("paper_structural_score_modifier_boundary_guard_margin", 0.5)
    )
    if margin is None:
        margin = 0.5
    if threshold is None or score is None or effective_score is None:
        return {}
    if score < threshold:
        return {}
    if score > threshold + max(0.0, margin):
        return {}
    if effective_score >= threshold:
        return {}
    if modifier is None or modifier >= 0:
        return {}
    return {
        "boundary_guard_applied": True,
        "boundary_guard_reason": "modifier_flip_at_threshold",
        "original_score_v2": score,
        "effective_score_v2": effective_score,
        "modifier": modifier,
        "modifier_reasons": list(modifier_reasons or []),
    }


def _paper_smc_main_specific_combo_weak_structure_match(candidate, candidate_type, structural_context):
    bos_quality = str(
        _paper_structural_context_value(candidate, structural_context, "bos_quality") or ""
    ).upper()
    volume_confirmation = str(
        _paper_structural_context_value(candidate, structural_context, "volume_confirmation") or ""
    ).upper()
    structural_decision = str(
        _paper_structural_context_value(
            candidate, structural_context, "structural_decision_shadow"
        )
        or ""
    ).upper()
    if (
        candidate_type == "LOW_SCORE"
        and bos_quality == "NO_FOLLOWTHROUGH"
        and volume_confirmation == "DIVERGENCE"
        and structural_decision == "UNKNOWN"
    ):
        return True, "weak_structure_no_followthrough_divergence"
    return False, None


def _paper_smc_main_weak_structure_block(candidate, candidate_type, structural_context):
    if not bool(config.get("paper_smc_main_block_low_score_no_followthrough_divergence", True)):
        return False, None
    mode = str(config.get("paper_smc_main_weak_structure_block_mode", "specific_combo") or "").lower()
    if mode != "specific_combo":
        return False, None
    return _paper_smc_main_specific_combo_weak_structure_match(
        candidate, candidate_type, structural_context
    )


def _paper_smc_main_weak_structure_extended_shadow(candidate, candidate_type, structural_context):
    bos_quality = str(
        _paper_structural_context_value(candidate, structural_context, "bos_quality") or ""
    ).upper()
    structural_decision = str(
        _paper_structural_context_value(
            candidate, structural_context, "structural_decision_shadow"
        )
        or ""
    ).upper()
    if structural_decision == "UNKNOWN" and bos_quality in {"WEAK", "NO_FOLLOWTHROUGH"}:
        return True, f"{candidate_type or 'other'}_unknown_structure_{bos_quality.lower()}"
    return False, None


def _paper_smc_main_attach_router_context(candidate, now_ts=None):
    try:
        if not isinstance(candidate, dict):
            return False
        symbol = candidate.get("symbol")
        if not symbol:
            return False
        from execution import get_paper_quality_router_context_snapshot
        context = get_paper_quality_router_context_snapshot(symbol, now_ts=now_ts)
        if not isinstance(context, dict) or not context:
            return False
        for key, value in context.items():
            if candidate.get(key) in (None, ""):
                candidate[key] = _json_safe_copy(value)
        regime = _first_nonblank(
            candidate.get("market_regime_at_entry"),
            candidate.get("router_regime"),
            candidate.get("regime"),
            candidate.get("market_regime"),
        )
        if regime not in (None, ""):
            if candidate.get("market_regime_at_entry") in (None, ""):
                candidate["market_regime_at_entry"] = regime
            if candidate.get("router_regime") in (None, ""):
                candidate["router_regime"] = regime
        return True
    except Exception as exc:
        print(f"[PAPER SMC MAIN] router context attach failed: {exc}")
        return False


def _paper_regime_context_modifier_shadow(candidate, decision, trade, candidate_type, structural_context, weak_extended):
    current_score = _paper_smc_research_float(decision.get("effective_score"))
    if current_score is None:
        current_score = _paper_smc_research_float(
            _first_nonblank(
                candidate.get("effective_score"),
                candidate.get("score_v2_structural_shadow"),
                structural_context.get("score_v2_structural_shadow"),
                candidate.get("score_v2_current"),
                structural_context.get("score_v2_current"),
            )
        )
    source = trade if isinstance(trade, dict) else candidate
    regime = _first_nonblank(
        source.get("market_regime_at_entry"),
        source.get("router_regime"),
        source.get("regime"),
        source.get("market_regime"),
        candidate.get("market_regime_at_entry"),
        candidate.get("router_regime"),
        candidate.get("regime"),
        candidate.get("market_regime"),
    )
    source_reason = _first_nonblank(
        candidate.get("source_reason"),
        candidate.get("reason"),
        structural_context.get("source_reason"),
    )
    regime_u = str(regime or "").upper()
    no_router = regime_u in ("", "NO_ROUTER", "UNKNOWN_ROUTER")
    data_available = bool(regime and not no_router)
    candidate_type_u = str(candidate_type or "").upper()
    structural_decision = str(
        _paper_structural_context_value(candidate, structural_context, "structural_decision_shadow") or ""
    ).upper()
    bos_quality = str(
        _paper_structural_context_value(candidate, structural_context, "bos_quality") or ""
    ).upper()
    raw_weak_extended = weak_extended
    if raw_weak_extended is None:
        raw_weak_extended = _first_nonblank(
            source.get("weak_structure_extended"),
            candidate.get("weak_structure_extended"),
            structural_context.get("weak_structure_extended"),
        )
    explicit_weak_false = (
        raw_weak_extended is not None
        and str(raw_weak_extended).strip().lower() in {"false", "0", "no", "n"}
    )
    weak_structure_match = not explicit_weak_false
    regime_chop = regime_u == "CHOP_NO_TRADE"
    regime_exhaustion = regime_u == "EXHAUSTION_REVERSAL"
    regime_match = data_available and (regime_chop or regime_exhaustion)
    bos_match = bos_quality in {"WEAK", "NO_FOLLOWTHROUGH"}
    candidate_type_match = candidate_type_u == "LOW_SCORE"
    source_reason_low_score = str(source_reason or "").upper() == "LOW_SCORE"
    grid_rule_match = (
        regime_match
        and bos_match
        and weak_structure_match
        and candidate_type_match
    )
    is_exhaustion_low_score = (
        data_available
        and regime_u == "EXHAUSTION_REVERSAL"
        and candidate_type_u == "LOW_SCORE"
    )
    proposed_delta = -1.0 if grid_rule_match else 0.0
    proposed_after = None if current_score is None else current_score + proposed_delta
    reason = None
    if grid_rule_match:
        reason = "grid_v0_2_chop_or_exhaustion_low_score_weak_bos_weak_structure_not_false"
    elif no_router:
        reason = "no_router_context"
    elif not data_available:
        reason = "regime_context_unavailable"
    return {
        "regime_context_modifier_version": "v0.2_grid_shadow",
        "regime_context_modifier_mode": "shadow_only",
        "current_effective_score": current_score,
        "would_regime_penalize": bool(grid_rule_match),
        "regime_penalty_reason": reason,
        "proposed_score_delta": proposed_delta,
        "proposed_effective_score_after_regime": proposed_after,
        "regime_context_regime": regime if data_available else None,
        "regime_context_candidate_type": candidate_type,
        "bos_quality": bos_quality or None,
        "weak_structure_extended": weak_extended,
        "structural_decision_shadow": structural_decision or None,
        "source_reason": source_reason,
        "regime_context_data_available": bool(data_available),
        "no_router_context": bool(no_router),
        "grid_rule_v0_2_match": bool(grid_rule_match),
        "grid_rule_regime_match": bool(regime_match),
        "grid_rule_bos_match": bool(bos_match),
        "grid_rule_weak_structure_match": bool(weak_structure_match),
        "grid_rule_candidate_type_match": bool(candidate_type_match),
        "grid_rule_source_reason_low_score": bool(source_reason_low_score),
        "grid_rule_explicit_weak_structure_false": bool(explicit_weak_false),
        "grid_rule_no_router_context": bool(no_router),
        "grid_rule_regime_chop": bool(regime_chop),
        "grid_rule_regime_exhaustion": bool(regime_exhaustion),
        "exhaustion_low_score_unknown_structure": bool(
            is_exhaustion_low_score and structural_decision == "UNKNOWN"
        ),
        "exhaustion_low_score_no_followthrough": bool(
            is_exhaustion_low_score and bos_quality == "NO_FOLLOWTHROUGH"
        ),
        "exhaustion_low_score_weak_bos": bool(
            is_exhaustion_low_score and bos_quality in {"WEAK", "NO_FOLLOWTHROUGH"}
        ),
        "exhaustion_low_score_weak_structure_extended": bool(
            is_exhaustion_low_score and bool(weak_extended)
        ),
    }


def _paper_smc_main_decision(candidate, ctx, now_ts):
    decision = {
        "action": "SUPPRESS",
        "suppress_reason": "",
        "dedup_key": _paper_smc_main_dedup_key(candidate),
        "candidate_type": _paper_smc_main_candidate_type(candidate),
        "structural_modifier": 0.0,
        "effective_score": None,
        "modifier_reasons": [],
        "boundary_guard_applied": False,
        "boundary_guard_reason": None,
        "weak_structure_blocked": False,
        "weak_structure_block_reason": None,
    }
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        decision["suppress_reason"] = "live_guard_blocked"
        return decision
    if config.get("paper_smc_main_live_enabled", False):
        decision["suppress_reason"] = "live_guard_blocked"
        return decision
    if not config.get("paper_smc_main_enabled", False):
        decision["suppress_reason"] = "disabled"
        return decision

    candidate_type = decision["candidate_type"]
    allowed_types = {
        str(x).upper()
        for x in config.get(
            "paper_smc_main_candidate_types",
            ["ACCEPTED_CONFIRM", "LOW_SCORE", "MID_SCORE_WEAK_BOS", "RR_FAIL"],
        )
    }
    if candidate_type not in allowed_types:
        decision["suppress_reason"] = "candidate_type_not_allowed"
        return decision
    if bool(config.get("paper_smc_main_exclude_trend_fail", True)) and candidate_type == "TREND_FAIL":
        decision["suppress_reason"] = "trend_fail_excluded"
        return decision

    if str(candidate.get("entry_type") or "").upper() != "CONFIRM":
        decision["suppress_reason"] = "not_confirm_candidate"
        return decision

    require_trackable = bool(config.get("paper_smc_main_require_outcome_trackable", True))
    if require_trackable:
        if candidate.get("geometry_status") != "VALID_GEOMETRY":
            decision["suppress_reason"] = "not_valid_geometry"
            return decision
        if not candidate.get("outcome_trackable"):
            decision["suppress_reason"] = "not_valid_geometry"
            return decision
    if candidate.get("entry") in (None, "") or candidate.get("sl") in (None, ""):
        decision["suppress_reason"] = "not_valid_geometry"
        return decision

    structural_context = _paper_structural_context(candidate)
    structural_decision = str(
        _paper_structural_context_value(
            candidate, structural_context, "structural_decision_shadow"
        )
        or ""
    ).upper()
    unknown_allowed = bool(config.get("paper_smc_main_allow_unknown_structural_decision", True))
    if structural_decision in ("", "UNKNOWN") and not unknown_allowed:
        decision["suppress_reason"] = "structural_decision_unknown"
        return decision
    weak_blocked, weak_block_reason = _paper_smc_main_weak_structure_block(
        candidate, candidate_type, structural_context
    )
    if weak_blocked:
        decision["weak_structure_blocked"] = True
        decision["weak_structure_block_reason"] = weak_block_reason
        decision["suppress_reason"] = weak_block_reason
        return decision

    shadow_score = _paper_smc_research_float(
        _first_nonblank(
            candidate.get("score_v2_structural_shadow"),
            structural_context.get("score_v2_structural_shadow"),
        )
    )
    current_score = _paper_smc_research_float(
        _first_nonblank(
            candidate.get("score_v2_current"),
            structural_context.get("score_v2_current"),
        )
    )
    score = shadow_score if shadow_score is not None else current_score
    decision["score"] = score
    threshold = _paper_smc_research_float(
        config.get("paper_smc_main_min_score_v2_structural_shadow", 2.5)
    )
    decision["threshold"] = threshold

    modifier = 0.0
    modifier_reasons = []
    if bool(config.get("paper_smc_main_use_structural_modifier", True)):
        try:
            modifier, modifier_reasons, structural_context = _paper_structural_score_modifier(candidate)
        except Exception as exc:
            modifier = 0.0
            modifier_reasons = [f"modifier_error:{type(exc).__name__}"]
    effective_score = round(score + modifier, 4) if score is not None else None
    decision["structural_modifier"] = modifier
    decision["effective_score"] = effective_score
    decision["modifier_reasons"] = list(modifier_reasons or [])

    boundary_guard_info = _paper_smc_main_boundary_guard_info(
        candidate,
        ctx,
        score,
        effective_score,
        modifier,
        modifier_reasons,
    )
    if boundary_guard_info.get("boundary_guard_applied"):
        decision.update({
            "boundary_guard_applied": True,
            "boundary_guard_reason": boundary_guard_info.get("boundary_guard_reason"),
        })

    if threshold is not None and (effective_score is None or effective_score < threshold):
        if not decision.get("boundary_guard_applied"):
            decision["suppress_reason"] = (
                "structural_modifier_score_too_low"
                if bool(config.get("paper_smc_main_use_structural_modifier", True))
                else "smc_main_score_too_low"
            )
            return decision

    dedup_key = decision["dedup_key"]
    if dedup_key in _paper_smc_main_dedup_keys or _paper_smc_main_key_open(ctx, dedup_key):
        decision["suppress_reason"] = "duplicate_key"
        return decision

    symbol = str(candidate.get("symbol") or "")
    if _paper_smc_main_symbol_open(ctx, symbol):
        decision["suppress_reason"] = "symbol_already_open"
        return decision

    try:
        max_open = max(0, int(config.get("paper_smc_main_max_open", 5)))
    except (TypeError, ValueError):
        max_open = 0
    if _paper_smc_main_open_count(ctx) >= max_open:
        decision["suppress_reason"] = "max_open_reached"
        return decision

    stale_info = _paper_smc_research_stale_info(candidate, now_ts)
    decision.update(stale_info)
    if stale_info.get("is_stale"):
        decision["suppress_reason"] = stale_info.get("stale_reason_detail") or "stale_candidate"
        return decision

    decision["action"] = "OPEN"
    decision["suppress_reason"] = ""
    return decision


def _paper_smc_main_trade(candidate, decision):
    structural_context = _paper_structural_context(candidate)
    now_ts = time.time()
    entry = _paper_smc_research_float(candidate.get("entry"))
    sl = _paper_smc_research_float(candidate.get("sl"))
    tp = _paper_smc_research_float(candidate.get("tp"))
    rr = _paper_smc_research_float(candidate.get("rr"))
    shadow_score = _paper_smc_research_float(
        _first_nonblank(
            candidate.get("score_v2_structural_shadow"),
            structural_context.get("score_v2_structural_shadow"),
        )
    )
    current_score = _paper_smc_research_float(
        _first_nonblank(candidate.get("score_v2_current"), structural_context.get("score_v2_current"))
    )
    score = decision.get("effective_score")
    if score is None:
        score = shadow_score if shadow_score is not None else (current_score if current_score is not None else 0.0)
    candidate_type = decision.get("candidate_type") or _paper_smc_main_candidate_type(candidate)
    source_reason = str(candidate.get("reason") or "")
    dedup_key = decision.get("dedup_key") or _paper_smc_main_dedup_key(candidate)
    trade = {
        "symbol": str(candidate.get("symbol") or ""),
        "side": str(candidate.get("side") or "").upper(),
        "entry": entry,
        "entry_real": entry,
        "sl": sl,
        "sl_real": sl,
        "sl_init": sl,
        "tp": tp,
        "rr": rr if rr is not None else 0,
        "score": score,
        "entry_type": "PAPER_SMC_MAIN",
        "base_entry_type": "CONFIRM",
        "status": "OPEN",
        "reason": [source_reason, "SMC_MAIN:CONFIRM_STRUCTURAL_OUTCOME_SHADOW"],
        "tp_mode": "SOFT" if (rr or 0) >= 1.8 else "HARD",
        "entry_time": now_ts,
        "signal_created_ts": _paper_smc_research_source_ts(candidate) or now_ts,
        "source_timestamp": candidate.get("source_timestamp"),
        "max_profit_r": 0,
        "entry_size": 1.0,
        "id": int(now_ts * 1000),
        "strategy_family": "paper_smc_main",
        "research_source": "confirm_structural_outcome_shadow",
        "research_dedup_key": dedup_key,
        "smc_main_dedup_key": dedup_key,
        "candidate_type": candidate_type,
        "original_reason": source_reason,
        "source_reason": source_reason,
        "score_v2_original": shadow_score if shadow_score is not None else current_score,
        "score_v2_current": current_score,
        "score_v2_structural_shadow": shadow_score,
        "structural_modifier": decision.get("structural_modifier", 0.0),
        "effective_score": decision.get("effective_score"),
        "modifier_reasons": list(decision.get("modifier_reasons") or []),
        "boundary_guard_applied": bool(decision.get("boundary_guard_applied")),
        "boundary_guard_reason": decision.get("boundary_guard_reason"),
        "structural_decision_shadow": _paper_structural_context_value(
            candidate, structural_context, "structural_decision_shadow"
        ),
        "bos_quality": _paper_structural_context_value(candidate, structural_context, "bos_quality"),
        "choch_quality": _paper_structural_context_value(candidate, structural_context, "choch_quality"),
        "poi_location_quality": _paper_structural_context_value(
            candidate, structural_context, "poi_location_quality"
        ),
        "volume_confirmation": _paper_structural_context_value(
            candidate, structural_context, "volume_confirmation"
        ),
        "trade_location_quality": _paper_structural_context_value(
            candidate, structural_context, "trade_location_quality"
        ),
        "geometry_status": candidate.get("geometry_status"),
        "outcome_trackable": bool(candidate.get("outcome_trackable")),
        "structural_context": _json_safe_copy(structural_context),
    }
    return trade


def _paper_smc_main_log(candidate, decision, trade=None):
    try:
        structural_context = _paper_structural_context(candidate)
        now_ts = time.time()
        candidate_type = decision.get("candidate_type") or _paper_smc_main_candidate_type(candidate)
        weak_extended, weak_extended_reason = _paper_smc_main_weak_structure_extended_shadow(
            candidate, candidate_type, structural_context
        )
        specific_combo_match, specific_combo_reason = (
            _paper_smc_main_specific_combo_weak_structure_match(
                candidate, candidate_type, structural_context
            )
        )
        opened = isinstance(trade, dict)
        regime_context_shadow = _paper_regime_context_modifier_shadow(
            candidate, decision, trade, candidate_type, structural_context, weak_extended
        )
        payload = {
            "event_type": "PAPER_SMC_MAIN_DECISION",
            "weak_structure_extended_schema_version": "v0.1",
            "timestamp": format_vn_time(now_ts),
            "timestamp_unix": now_ts,
            "action": decision.get("action", "SUPPRESS"),
            "engine": "PAPER_SMC_MAIN",
            "engine_version": "v1.1",
            "symbol": candidate.get("symbol"),
            "side": candidate.get("side"),
            "entry_type": "PAPER_SMC_MAIN",
            "candidate_type": candidate_type,
            "rank_index": decision.get("rank_index"),
            "candidate_priority": decision.get("candidate_priority"),
            "ranking_enabled": decision.get("ranking_enabled"),
            "candidate_priority_source": decision.get("candidate_priority_source"),
            "source_reason": candidate.get("reason"),
            "entry": candidate.get("entry"),
            "sl": candidate.get("sl"),
            "tp": candidate.get("tp"),
            "rr": candidate.get("rr"),
            "raw_score": _first_nonblank(candidate.get("score"), candidate.get("raw_score")),
            "geometry_status": candidate.get("geometry_status"),
            "outcome_trackable": bool(candidate.get("outcome_trackable")),
            "score_v2_current": _first_nonblank(
                candidate.get("score_v2_current"), structural_context.get("score_v2_current")
            ),
            "score_v2_structural_shadow": _first_nonblank(
                candidate.get("score_v2_structural_shadow"),
                structural_context.get("score_v2_structural_shadow"),
            ),
            "structural_modifier": decision.get("structural_modifier"),
            "effective_score": decision.get("effective_score"),
            "threshold": decision.get("threshold"),
            "modifier_reasons": list(decision.get("modifier_reasons") or []),
            "structural_decision_shadow": _paper_structural_context_value(
                candidate, structural_context, "structural_decision_shadow"
            ),
            "bos_quality": _paper_structural_context_value(candidate, structural_context, "bos_quality"),
            "displacement_quality": _paper_structural_context_value(
                candidate, structural_context, "displacement_quality"
            ),
            "choch_quality": _paper_structural_context_value(candidate, structural_context, "choch_quality"),
            "poi_location_quality": _paper_structural_context_value(
                candidate, structural_context, "poi_location_quality"
            ),
            "volume_confirmation": _paper_structural_context_value(
                candidate, structural_context, "volume_confirmation"
            ),
            "trade_location_quality": _paper_structural_context_value(
                candidate, structural_context, "trade_location_quality"
            ),
            "range_context": _first_nonblank(
                candidate.get("range_context"), structural_context.get("range_context")
            ),
            "bos_confirmation": _first_nonblank(
                candidate.get("bos_confirmation"), structural_context.get("bos_confirmation")
            ),
            "liquidity_sweep": _first_nonblank(
                candidate.get("liquidity_sweep"), structural_context.get("liquidity_sweep")
            ),
            "smc_bias": _first_nonblank(candidate.get("smc_bias"), structural_context.get("smc_bias")),
            "smc_zone": _first_nonblank(candidate.get("smc_zone"), structural_context.get("smc_zone")),
            "boundary_guard_applied": bool(decision.get("boundary_guard_applied")),
            "boundary_guard_reason": decision.get("boundary_guard_reason"),
            "weak_structure_blocked": bool(decision.get("weak_structure_blocked")),
            "weak_structure_block_reason": decision.get("weak_structure_block_reason"),
            "weak_structure_extended": bool(weak_extended),
            "weak_structure_extended_reason": weak_extended_reason,
            "specific_combo_block_match": bool(specific_combo_match),
            "specific_combo_block_reason": specific_combo_reason,
            "suppress_reason": decision.get("suppress_reason") or None,
            "opened": opened,
            "opened_trade_id": trade.get("id") if opened else None,
            "tp_mode": trade.get("tp_mode") if opened else None,
            "strategy_family": "paper_smc_main",
            "dedup_key": decision.get("dedup_key") or _paper_smc_main_dedup_key(candidate),
            "decision_dedup_key": decision.get("dedup_key") or _paper_smc_main_dedup_key(candidate),
            "signal_created_ts": candidate.get("signal_created_ts"),
            "source_timestamp": candidate.get("source_timestamp"),
        }
        for key in (
            "market_regime_at_entry",
            "router_regime",
            "router_regime_source",
            "router_regime_observed_at",
            "router_regime_age_sec",
            "router_regime_stale",
        ):
            value = _first_nonblank(
                trade.get(key) if opened else None,
                candidate.get(key),
            )
            if value not in (None, ""):
                payload[key] = value
        payload.update(regime_context_shadow)
        for key in (
            "candidate_age_secs",
            "candidate_max_age_secs",
            "candidate_time_source",
            "source_row_time",
            "collector_ts",
            "stale_reason_detail",
        ):
            if key in decision:
                payload[key] = decision.get(key)
        payload = {
            k: _json_safe_copy(v)
            for k, v in payload.items()
            if v not in ("",)
        }
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "paper_smc_main_decisions.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False, default=str, sort_keys=True) + "\n")
        try:
            from entry import register_paper_smc_v0_2_shadow_outcome
            register_paper_smc_v0_2_shadow_outcome(copy.deepcopy(payload))
        except Exception as exc:
            print(f"[PAPER SMC V0.2 SHADOW] outcome register failed: {exc}")
        try:
            if opened and trade is not None:
                from entry import register_paper_smc_main_open_geometry_observer
                observer_payload = copy.deepcopy(payload)
                observer_payload["tp_mode"] = trade.get("tp_mode")
                register_paper_smc_main_open_geometry_observer(observer_payload)
        except Exception as exc:
            print(f"[PAPER SMC MAIN OPEN GEOM] observer register failed: {exc}")
    except Exception as exc:
        print(f"[PAPER SMC MAIN] decision log failed: {exc}")


def _paper_smc_main_gate_shadow_open_snapshot(ctx):
    open_trades = [
        trade for trade in getattr(ctx, "trades", []) or []
        if trade.get("status", "OPEN") == "OPEN"
        and trade.get("owner", "bot") == "bot"
    ]
    symbols = sorted({
        str(trade.get("symbol") or "")
        for trade in open_trades
        if trade.get("symbol")
    })
    main_keys = {
        str(_first_nonblank(trade.get("smc_main_dedup_key"), trade.get("research_dedup_key")) or "")
        for trade in open_trades
        if trade.get("strategy_family") == "paper_smc_main"
        or trade.get("entry_type") == "PAPER_SMC_MAIN"
    }
    research_keys = {
        str(trade.get("research_dedup_key") or "")
        for trade in open_trades
        if trade.get("strategy_family") == "confirm_smc_research"
    }
    main_keys.update(str(key) for key in _paper_smc_main_dedup_keys if key)
    research_keys.update(str(key) for key in _paper_smc_research_dedup_keys if key)
    return {
        "open_count": len(open_trades),
        "open_symbols": symbols,
        "main_open_count": _paper_smc_main_open_count(ctx),
        "research_open_count": _paper_smc_research_open_count(ctx),
        "main_keys": {key for key in main_keys if key},
        "research_keys": {key for key in research_keys if key},
    }


def _paper_smc_main_shadow_score_info(candidate):
    structural_context = _paper_structural_context(candidate)
    shadow_score = _paper_smc_research_float(
        _first_nonblank(
            candidate.get("score_v2_structural_shadow"),
            structural_context.get("score_v2_structural_shadow"),
        )
    )
    current_score = _paper_smc_research_float(
        _first_nonblank(
            candidate.get("score_v2_current"),
            structural_context.get("score_v2_current"),
        )
    )
    modifier = 0.0
    modifier_reasons = []
    if bool(config.get("paper_smc_main_use_structural_modifier", True)):
        try:
            modifier, modifier_reasons, structural_context = _paper_structural_score_modifier(candidate)
        except Exception as exc:
            modifier = 0.0
            modifier_reasons = [f"modifier_error:{type(exc).__name__}"]
    score = shadow_score if shadow_score is not None else current_score
    effective_score = round(score + modifier, 4) if score is not None else None
    return structural_context, shadow_score, current_score, modifier, modifier_reasons, effective_score


def _paper_smc_main_shadow_current_eligibility(candidate, ctx, now_ts):
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return False, "live_guard_blocked", {}
    if config.get("paper_smc_main_live_enabled", False):
        return False, "live_guard_blocked", {}
    if not config.get("paper_smc_main_enabled", False):
        return False, "disabled", {}

    candidate_type = _paper_smc_main_candidate_type(candidate)
    allowed_types = {
        str(x).upper()
        for x in config.get(
            "paper_smc_main_candidate_types",
            ["ACCEPTED_CONFIRM", "LOW_SCORE", "MID_SCORE_WEAK_BOS", "RR_FAIL"],
        )
    }
    if candidate_type not in allowed_types:
        return False, "candidate_type_not_allowed", {"candidate_type": candidate_type}
    if bool(config.get("paper_smc_main_exclude_trend_fail", True)) and candidate_type == "TREND_FAIL":
        return False, "trend_fail_excluded", {"candidate_type": candidate_type}
    if str(candidate.get("entry_type") or "").upper() != "CONFIRM":
        return False, "not_confirm_candidate", {"candidate_type": candidate_type}
    if bool(config.get("paper_smc_main_require_outcome_trackable", True)):
        if candidate.get("geometry_status") != "VALID_GEOMETRY" or not candidate.get("outcome_trackable"):
            return False, "not_valid_geometry", {"candidate_type": candidate_type}
    if candidate.get("entry") in (None, "") or candidate.get("sl") in (None, ""):
        return False, "not_valid_geometry", {"candidate_type": candidate_type}

    structural_context, shadow_score, current_score, modifier, modifier_reasons, effective_score = (
        _paper_smc_main_shadow_score_info(candidate)
    )
    structural_decision = str(
        _paper_structural_context_value(
            candidate, structural_context, "structural_decision_shadow"
        )
        or ""
    ).upper()
    unknown_allowed = bool(config.get("paper_smc_main_allow_unknown_structural_decision", True))
    if structural_decision in ("", "UNKNOWN") and not unknown_allowed:
        return False, "structural_decision_unknown", {"candidate_type": candidate_type}
    weak_blocked, weak_block_reason = _paper_smc_main_weak_structure_block(
        candidate, candidate_type, structural_context
    )
    if weak_blocked:
        return False, weak_block_reason or "weak_structure_blocked", {
            "candidate_type": candidate_type,
            "weak_structure_blocked": True,
            "weak_structure_block_reason": weak_block_reason,
        }

    threshold = _paper_smc_research_float(
        config.get("paper_smc_main_min_score_v2_structural_shadow", 2.5)
    )
    boundary_guard_info = _paper_smc_main_boundary_guard_info(
        candidate,
        ctx,
        shadow_score if shadow_score is not None else current_score,
        effective_score,
        modifier,
        modifier_reasons,
    )
    if threshold is not None and (effective_score is None or effective_score < threshold):
        if not boundary_guard_info.get("boundary_guard_applied"):
            return False, (
                "structural_modifier_score_too_low"
                if bool(config.get("paper_smc_main_use_structural_modifier", True))
                else "smc_main_score_too_low"
            ), {
                "candidate_type": candidate_type,
                "effective_score": effective_score,
                "structural_modifier": modifier,
                "modifier_reasons": list(modifier_reasons or []),
            }

    stale_info = _paper_smc_research_stale_info(candidate, now_ts)
    if stale_info.get("is_stale"):
        return False, stale_info.get("stale_reason_detail") or "stale_candidate", {
            "candidate_type": candidate_type,
            "effective_score": effective_score,
            "structural_modifier": modifier,
            "modifier_reasons": list(modifier_reasons or []),
            **stale_info,
        }
    return True, "eligible", {
        "candidate_type": candidate_type,
        "effective_score": effective_score,
        "structural_modifier": modifier,
        "modifier_reasons": list(modifier_reasons or []),
        "boundary_guard_applied": bool(boundary_guard_info.get("boundary_guard_applied")),
        "boundary_guard_reason": boundary_guard_info.get("boundary_guard_reason"),
        **stale_info,
    }


def _paper_smc_main_shadow_research_eligibility(candidate, ctx, now_ts):
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return False, "live_guard_blocked", {}
    if config.get("paper_smc_research_live_enabled", False):
        return False, "live_guard_blocked", {}
    if not config.get("paper_enable_smc_research_lane", False):
        return False, "disabled", {}
    if str(candidate.get("entry_type") or "").upper() != "CONFIRM":
        return False, "not_valid_geometry", {}

    structural_context = _paper_structural_context(candidate)
    reason = str(candidate.get("reason") or "").upper()
    allowed_reasons = {str(x).upper() for x in config.get("paper_smc_research_allow_reasons", [])}
    if reason not in allowed_reasons:
        return False, "reason_not_allowed", {"candidate_type": reason}
    if candidate.get("geometry_status") != "VALID_GEOMETRY":
        return False, "not_valid_geometry", {"candidate_type": reason}
    if not candidate.get("outcome_trackable"):
        return False, "not_valid_geometry", {"candidate_type": reason}
    if candidate.get("entry") in (None, "") or candidate.get("sl") in (None, ""):
        return False, "not_valid_geometry", {"candidate_type": reason}

    decision = str(structural_context.get("structural_decision_shadow") or "").upper()
    allowed_decisions = {
        str(x).upper()
        for x in config.get("paper_smc_research_allow_structural_decisions", [])
    }
    unknown_allowed = bool(config.get("paper_smc_research_allow_unknown_structural_decision", True))
    if decision in ("", "UNKNOWN") and not unknown_allowed:
        return False, "structural_decision_unknown", {"candidate_type": reason}
    if decision not in allowed_decisions and not (decision == "UNKNOWN" and unknown_allowed):
        return False, "structural_decision_not_allowed", {"candidate_type": reason}

    min_score = _paper_smc_research_float(
        config.get("paper_smc_research_min_score_v2_structural_shadow", 2.5)
    )
    shadow_score = _paper_smc_research_float(structural_context.get("score_v2_structural_shadow"))
    modifier = 0.0
    modifier_reasons = []
    use_modifier_score = (
        bool(config.get("paper_enable_structural_score_modifier", False))
        and not bool(config.get("paper_structural_score_modifier_log_only", True))
    )
    if use_modifier_score:
        try:
            modifier, modifier_reasons, structural_context = _paper_structural_score_modifier(candidate)
        except Exception as exc:
            modifier = 0.0
            modifier_reasons = [f"modifier_error:{type(exc).__name__}"]
    effective_score = (
        round(shadow_score + modifier, 4)
        if use_modifier_score and shadow_score is not None
        else shadow_score
    )
    if min_score is not None and (effective_score is None or effective_score < min_score):
        boundary_guard_info = {}
        if use_modifier_score and shadow_score is not None and effective_score is not None:
            boundary_guard_info = _paper_smc_research_boundary_guard_info(
                candidate,
                ctx,
                shadow_score,
                effective_score,
                modifier,
                modifier_reasons,
            )
        if not boundary_guard_info.get("boundary_guard_applied"):
            return False, (
                "structural_modifier_score_too_low" if use_modifier_score else "score_too_low"
            ), {
                "candidate_type": reason,
                "effective_score": effective_score,
                "structural_modifier": modifier,
                "modifier_reasons": list(modifier_reasons or []),
            }

    stale_info = _paper_smc_research_stale_info(candidate, now_ts)
    if stale_info.get("is_stale"):
        return False, stale_info.get("stale_reason_detail") or "stale_candidate", {
            "candidate_type": reason,
            "effective_score": effective_score,
            "structural_modifier": modifier,
            "modifier_reasons": list(modifier_reasons or []),
            **stale_info,
        }
    return True, "eligible", {
        "candidate_type": reason,
        "effective_score": effective_score,
        "structural_modifier": modifier,
        "modifier_reasons": list(modifier_reasons or []),
        **stale_info,
    }


def _paper_smc_main_gate_shadow_simulate(
    ordered_rows,
    ctx,
    now_ts,
    gate_name,
    snapshot,
):
    max_open = 5
    one_per_symbol = True
    if gate_name == "current":
        initial_open_count = snapshot.get("main_open_count", 0)
        initial_keys = set(snapshot.get("main_keys") or set())
        eligibility_fn = _paper_smc_main_shadow_current_eligibility
    else:
        initial_open_count = snapshot.get("research_open_count", 0)
        initial_keys = set(snapshot.get("research_keys") or set())
        eligibility_fn = _paper_smc_main_shadow_research_eligibility

    selected_keys = set()
    selected_symbols = set()
    open_count = int(initial_open_count or 0)
    open_symbols_before = set(snapshot.get("open_symbols") or [])
    results = {}
    eligible_rank = 0

    for row in ordered_rows:
        candidate = row["candidate"]
        key = row["dedup_key"]
        symbol = str(candidate.get("symbol") or "")
        eligible, reason, info = eligibility_fn(candidate, ctx, now_ts)
        rank = None
        selected = False
        skip_reason = "not_eligible"
        if eligible:
            rank = eligible_rank
            eligible_rank += 1
            if key in initial_keys or key in selected_keys:
                skip_reason = "duplicate_key_sim"
            elif one_per_symbol and symbol and (symbol in open_symbols_before or symbol in selected_symbols):
                skip_reason = "symbol_already_open_sim"
            elif open_count >= max_open:
                skip_reason = "max_open_sim"
            else:
                selected = True
                skip_reason = "selected"
                if key:
                    selected_keys.add(key)
                if symbol:
                    selected_symbols.add(symbol)
                open_count += 1
        results[row["row_id"]] = {
            f"{gate_name}_gate_eligible": bool(eligible),
            f"{gate_name}_gate_rank": rank,
            f"{gate_name}_gate_selected": bool(selected),
            f"{gate_name}_gate_skip_reason": skip_reason,
            f"{gate_name}_gate_eligibility_reason": reason,
            f"{gate_name}_effective_score": info.get("effective_score"),
        }
        if gate_name == "current":
            results[row["row_id"]].update({
                "current_structural_modifier": info.get("structural_modifier"),
                "current_modifier_reasons": info.get("modifier_reasons"),
                "current_boundary_guard_applied": bool(info.get("boundary_guard_applied")),
                "current_boundary_guard_reason": info.get("boundary_guard_reason"),
            })
        for stale_key in (
            "candidate_age_secs",
            "candidate_max_age_secs",
            "candidate_time_source",
            "stale_reason_detail",
        ):
            if stale_key in info:
                results[row["row_id"]][f"{gate_name}_{stale_key}"] = info.get(stale_key)
    return results


def _paper_smc_main_gate_shadow_base_row(row, scan_id, batch_id, now_ts, snapshot, duplicate_counts, truncated, total_count):
    candidate = row["candidate"]
    structural_context = _paper_structural_context(candidate)
    structural_context, shadow_score, current_score, modifier, modifier_reasons, effective_score = (
        _paper_smc_main_shadow_score_info(candidate)
    )
    key = row["dedup_key"]
    symbol = str(candidate.get("symbol") or "")
    return {
        "event_type": "PAPER_SMC_MAIN_GATE_SHADOW",
        "observed_at": now_ts,
        "timestamp": format_vn_time(now_ts),
        "timestamp_unix": now_ts,
        "scan_id": scan_id,
        "batch_id": batch_id,
        "shadow_version": "paper_smc_main_gate_shadow_v1",
        "candidate_order_in_batch": row.get("candidate_order_in_batch"),
        "candidate_count_in_batch": total_count,
        "shadow_truncated_by_max_per_scan": bool(truncated),
        "dedup_key": key,
        "duplicate_key_in_scan": duplicate_counts.get(key, 0) > 1 if key else False,
        "duplicate_key_count_in_scan": duplicate_counts.get(key, 0) if key else 0,
        "symbol_already_open_before_scan": symbol in set(snapshot.get("open_symbols") or []),
        "symbol": symbol,
        "side": str(candidate.get("side") or "").upper(),
        "entry_type": candidate.get("entry_type"),
        "source_entry_type": candidate.get("entry_type"),
        "candidate_type": _paper_smc_main_candidate_type(candidate),
        "reason": candidate.get("reason"),
        "source_reason": candidate.get("reason"),
        "structural_decision_shadow": _paper_structural_context_value(
            candidate, structural_context, "structural_decision_shadow"
        ),
        "effective_score": effective_score,
        "score_v2_current": current_score,
        "score_v2_structural_shadow": shadow_score,
        "structural_score_modifier_shadow": modifier,
        "modifier_reasons": list(modifier_reasons or []),
        "geometry_status": candidate.get("geometry_status"),
        "outcome_trackable": bool(candidate.get("outcome_trackable")),
        "entry": candidate.get("entry"),
        "sl": candidate.get("sl"),
        "tp": candidate.get("tp"),
        "rr": candidate.get("rr"),
        "source_timestamp": candidate.get("source_timestamp"),
        "source_row_time": candidate.get("source_row_time", candidate.get("timestamp")),
        "signal_created_ts": candidate.get("signal_created_ts"),
        "collector_ts": candidate.get("collector_ts"),
        "bos_quality": _paper_structural_context_value(candidate, structural_context, "bos_quality"),
        "bos_confirmation": _first_nonblank(
            candidate.get("bos_confirmation"),
            structural_context.get("bos_confirmation"),
        ),
        "displacement_quality": _paper_structural_context_value(
            candidate, structural_context, "displacement_quality"
        ),
        "choch_quality": _paper_structural_context_value(candidate, structural_context, "choch_quality"),
        "poi_location_quality": _paper_structural_context_value(
            candidate, structural_context, "poi_location_quality"
        ),
        "volume_confirmation": _paper_structural_context_value(
            candidate, structural_context, "volume_confirmation"
        ),
        "trade_location_quality": _paper_structural_context_value(
            candidate, structural_context, "trade_location_quality"
        ),
        "range_context": _first_nonblank(candidate.get("range_context"), structural_context.get("range_context")),
        "smc_bias": _first_nonblank(candidate.get("smc_bias"), structural_context.get("smc_bias")),
        "smc_zone": _first_nonblank(candidate.get("smc_zone"), structural_context.get("smc_zone")),
        "market_regime_at_entry": _first_nonblank(
            candidate.get("market_regime_at_entry"),
            candidate.get("router_regime"),
            candidate.get("regime"),
            structural_context.get("market_regime_at_entry"),
        ),
        "router_regime": candidate.get("router_regime"),
        "router_regime_source": candidate.get("router_regime_source"),
        "router_regime_observed_at": candidate.get("router_regime_observed_at"),
        "router_regime_age_sec": candidate.get("router_regime_age_sec"),
        "router_regime_stale": candidate.get("router_regime_stale"),
        "weak_structure_blocked": False,
        "weak_structure_block_reason": None,
        "weak_structure_extended": _first_nonblank(
            candidate.get("weak_structure_extended"),
            structural_context.get("weak_structure_extended"),
        ),
        "weak_structure_extended_reason": _first_nonblank(
            candidate.get("weak_structure_extended_reason"),
            structural_context.get("weak_structure_extended_reason"),
        ),
        "current_paper_open_count_before": snapshot.get("open_count", 0),
        "current_open_symbols_before": list(snapshot.get("open_symbols") or []),
        "current_smc_main_open_count_before": snapshot.get("main_open_count", 0),
        "current_research_open_count_before": snapshot.get("research_open_count", 0),
        "simulated_max_open_slots": 5,
        "simulated_one_per_symbol": True,
    }


def _shadow_log_paper_smc_main_gates(ctx, candidates, scan_id=None):
    global _paper_smc_main_gate_shadow_scan_counter
    if not bool(config.get("paper_smc_main_gate_shadow_enabled", False)):
        return
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return
    if config.get("paper_smc_main_live_enabled", False) or config.get("paper_smc_research_live_enabled", False):
        return

    try:
        now_ts = time.time()
        if scan_id is None:
            scan_id = now_ts
        _paper_smc_main_gate_shadow_scan_counter += 1
        batch_id = f"{int(float(scan_id))}:{_paper_smc_main_gate_shadow_scan_counter}"
        try:
            max_per_scan = int(config.get("paper_smc_main_gate_shadow_max_per_scan", 200))
        except (TypeError, ValueError):
            max_per_scan = 200
        max_per_scan = max(0, max_per_scan)
        raw_candidates = list(candidates or [])
        total_count = len(raw_candidates)
        if max_per_scan:
            raw_candidates = raw_candidates[:max_per_scan]
        else:
            raw_candidates = []
        truncated = total_count > len(raw_candidates)

        ranked_rows = []
        for ranked in _paper_smc_main_ranked_candidates(raw_candidates):
            candidate = copy.deepcopy(ranked.get("candidate") or {})
            _paper_smc_main_attach_router_context(candidate, now_ts=now_ts)
            original_index = ranked.get("original_index")
            if original_index is None:
                original_index = len(ranked_rows)
            ranked_rows.append({
                "row_id": original_index,
                "candidate": candidate,
                "dedup_key": _paper_smc_main_dedup_key(candidate),
                "candidate_order_in_batch": original_index,
                "rank_index": ranked.get("rank_index"),
                "candidate_priority": ranked.get("candidate_priority"),
                "ranking_enabled": ranked.get("ranking_enabled"),
                "candidate_priority_source": ranked.get("candidate_priority_source"),
            })
        by_order_rows = sorted(ranked_rows, key=lambda item: item["candidate_order_in_batch"])
        duplicate_counts = Counter(row["dedup_key"] for row in by_order_rows if row.get("dedup_key"))
        snapshot = _paper_smc_main_gate_shadow_open_snapshot(ctx)

        current_results = _paper_smc_main_gate_shadow_simulate(
            ranked_rows,
            ctx,
            now_ts,
            "current",
            snapshot,
        )
        research_results = _paper_smc_main_gate_shadow_simulate(
            by_order_rows,
            ctx,
            now_ts,
            "research",
            snapshot,
        )

        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "paper_smc_main_gate_shadow.jsonl")
        with open(file_path, "a", encoding="utf-8") as handle:
            for row in by_order_rows:
                payload = _paper_smc_main_gate_shadow_base_row(
                    row,
                    scan_id,
                    batch_id,
                    now_ts,
                    snapshot,
                    duplicate_counts,
                    truncated,
                    total_count,
                )
                payload.update({
                    "current_runtime_rank_index": row.get("rank_index"),
                    "current_candidate_priority": row.get("candidate_priority"),
                    "current_ranking_enabled": row.get("ranking_enabled"),
                    "current_candidate_priority_source": row.get("candidate_priority_source"),
                })
                payload.update(current_results.get(row["row_id"], {}))
                payload.update(research_results.get(row["row_id"], {}))
                payload = {
                    key: _json_safe_copy(value)
                    for key, value in payload.items()
                    if value not in ("",)
                }
                handle.write(json.dumps(payload, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER SMC MAIN GATE SHADOW] durable log failed: {exc}")


def _dispatch_paper_smc_main(ctx):
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return
    if config.get("paper_smc_main_live_enabled", False):
        return
    main_enabled = bool(config.get("paper_smc_main_enabled", False))
    shadow_enabled = bool(config.get("paper_smc_main_gate_shadow_enabled", False))
    if not main_enabled and not shadow_enabled:
        return
    try:
        from entry import get_confirm_structural_outcome_candidates_snapshot
        candidates = get_confirm_structural_outcome_candidates_snapshot()
    except Exception as exc:
        print(f"[PAPER SMC MAIN] candidate snapshot failed: {exc}")
        return

    now_ts = time.time()
    if shadow_enabled:
        _shadow_log_paper_smc_main_gates(ctx, candidates, scan_id=now_ts)
    if not main_enabled:
        return

    from execution import open_trade, update_signal_state
    for ranked_candidate in _paper_smc_main_ranked_candidates(candidates):
        candidate = copy.deepcopy(ranked_candidate.get("candidate"))
        _paper_smc_main_attach_router_context(candidate, now_ts=now_ts)
        decision = _paper_smc_main_decision(candidate, ctx, now_ts)
        decision.update({
            "rank_index": ranked_candidate.get("rank_index"),
            "candidate_priority": ranked_candidate.get("candidate_priority"),
            "ranking_enabled": ranked_candidate.get("ranking_enabled"),
            "candidate_priority_source": ranked_candidate.get("candidate_priority_source"),
        })
        if decision.get("action") != "OPEN":
            _paper_smc_main_log(candidate, decision)
            continue
        trade = _paper_smc_main_trade(candidate, decision)
        if not trade.get("symbol") or trade.get("side") not in {"LONG", "SHORT"}:
            decision["action"] = "SUPPRESS"
            decision["suppress_reason"] = "not_valid_geometry"
            _paper_smc_main_log(candidate, decision)
            continue
        before_open = _paper_smc_main_key_open(ctx, trade.get("smc_main_dedup_key"))
        success = open_trade(copy.deepcopy(trade), ctx)
        if success:
            _paper_smc_main_dedup_keys.add(trade.get("smc_main_dedup_key"))
            ctx.entry_cooldown[trade["symbol"]] = time.time()
            update_signal_state(
                trade["symbol"],
                trade["side"],
                trade.get("entry_real") or trade.get("entry", 0),
                executed=True,
                ctx=ctx,
            )
            opened = next(
                (
                    t for t in ctx.trades
                    if (
                        t.get("strategy_family") == "paper_smc_main"
                        or t.get("entry_type") == "PAPER_SMC_MAIN"
                    )
                    and t.get("smc_main_dedup_key") == trade.get("smc_main_dedup_key")
                ),
                trade,
            )
            _paper_smc_main_log(candidate, decision, trade=opened)
        elif before_open or _paper_smc_main_key_open(ctx, trade.get("smc_main_dedup_key")):
            _paper_smc_main_dedup_keys.add(trade.get("smc_main_dedup_key"))
            decision["action"] = "SUPPRESS"
            decision["suppress_reason"] = "duplicate_key"
            _paper_smc_main_log(candidate, decision)
        else:
            decision["action"] = "SUPPRESS"
            decision["suppress_reason"] = "symbol_already_open"
            _paper_smc_main_log(candidate, decision)


def _compute_portfolio_exhaustion(candidate_signals):
    _cont_types = {"CONFIRM", "REVERSAL_CONFIRM"}
    cont = [s for s in candidate_signals if s.get("entry_type", "").upper() in _cont_types]
    total = len(cont)
    if total == 0:
        return
    extended_count = sum(1 for s in cont if s.get("exhaustion_cls", "").upper() == "EXTENDED")
    ratio_pct = round(extended_count / total * 100)
    if ratio_pct < 30:
        regime = "HEALTHY"
    elif ratio_pct < 60:
        regime = "CAUTIOUS"
    elif ratio_pct < 75:
        regime = "DEFENSIVE"
    else:
        regime = "COLLAPSING"
    print(f"[MARKET REGIME] ratio={ratio_pct}% regime={regime} extended={extended_count} total={total}")
    _log_market_regime(ratio_pct, regime, extended_count, total)


def _live_prefilter_min_notional(sig, ctx):
    """
    Lightweight pre-dispatch feasibility check for live mode.
    Estimates notional using raw (unrounded) qty before calling open_trade().
    Conservative approximation only -- validate_and_prepare() remains authoritative.
    Returns (feasible: bool, estimated_notional: float, min_notional: float).
    """
    try:
        from exchange.precision import get_symbol_filters
        symbol = sig.get("symbol", "")
        entry = sig.get("entry", 0) or 0
        sl = sig.get("sl", 0) or 0
        sl_distance = abs(entry - sl)
        if entry <= 0 or sl_distance <= 0:
            return True, 0.0, 0.0
        balance = ctx.account_balance
        risk_percent = sig.get("risk_percent") or config["live_risk_per_trade"]
        risk_amount = balance * risk_percent
        raw_qty = risk_amount / sl_distance
        estimated_notional = raw_qty * entry
        filters = get_symbol_filters(symbol)
        if filters is None:
            return True, estimated_notional, 0.0
        min_notional = float(filters.get("min_notional", 0))
        if min_notional > 0 and estimated_notional < min_notional:
            return False, estimated_notional, min_notional
        return True, estimated_notional, min_notional
    except Exception:
        return True, 0.0, 0.0


def _log_paper_signal_observation(sig: dict, suppression_reason: str, ctx, extra_fields: dict = None) -> None:
    """
    Emit a paper-mode signal observation notification.

    Called when a signal passes strategy_execution_filter for paper but is
    suppressed from execution by cooldown, open-symbol, slot, or pause gates.

    This preserves PAPER's role as a broad research/analytics visibility layer:
    even when paper cannot execute a setup, the signal is still visible in
    Telegram and in the paper signal log so analysts can see what LIVE is doing.

    Design contract:
      - NEVER called for live or testnet executors.
      - NEVER mutates the shared signal dict (reads only).
      - Suppression reason is always logged so the cause is auditable.
    """
    if ctx is None or getattr(ctx, "execution_mode", None) != "paper":
        return

    from telegram import send_telegram

    def _json_safe_copy(value):
        try:
            return copy.deepcopy(value)
        except Exception:
            return value

    def _as_list_copy(value):
        if isinstance(value, list):
            return [_json_safe_copy(item) for item in value]
        if isinstance(value, tuple):
            return [_json_safe_copy(item) for item in value]
        if value in (None, ""):
            return []
        return [_json_safe_copy(value)]

    def _first_present(*values):
        for value in values:
            if value not in (None, ""):
                return _json_safe_copy(value)
        return ""

    def _as_dict_copy(value):
        if isinstance(value, dict):
            return _json_safe_copy(value)
        if isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value)
                if isinstance(parsed, dict):
                    return _json_safe_copy(parsed)
            except Exception:
                return {}
        return {}

    def _nested_dict(source, *path):
        cur = source
        for key in path:
            if not isinstance(cur, dict):
                return {}
            cur = cur.get(key)
        return _as_dict_copy(cur)

    def _observation_contexts():
        breakdown = _as_dict_copy(sig.get("score_breakdown"))
        if not breakdown:
            breakdown = _as_dict_copy(sig.get("breakdown"))
        breakdown_json = _as_dict_copy(sig.get("breakdown_json"))
        if not breakdown and breakdown_json:
            breakdown = breakdown_json

        smc = _as_dict_copy(sig.get("smc"))
        if not smc:
            smc = _nested_dict(breakdown, "smc")
        if not smc:
            smc = _nested_dict(breakdown_json, "smc")

        accepted_ctx = _nested_dict(breakdown, "accepted_signal_context")
        if not accepted_ctx:
            accepted_ctx = _nested_dict(breakdown_json, "accepted_signal_context")
        structural_context = _as_dict_copy(sig.get("structural_context"))
        if not structural_context:
            structural_context = _nested_dict(breakdown, "structural_context")
        if not structural_context:
            structural_context = _nested_dict(breakdown_json, "structural_context")
        return breakdown, smc, accepted_ctx, structural_context

    def _smc_value(field, default):
        value = _first_present(sig.get(field), sig.get("smc", {}).get(field) if isinstance(sig.get("smc"), dict) else None)
        if value not in (None, ""):
            return value
        _, smc, _, _ = _observation_contexts()
        value = _first_present(smc.get(field))
        return value if value not in (None, "") else default

    def _invalid_context_value():
        top = sig.get("invalid_context")
        if top not in (None, ""):
            return _as_list_copy(top)
        _, smc, _, _ = _observation_contexts()
        return _as_list_copy(smc.get("invalid_context"))

    def _log_paper_observation_payload(now_ts):
        try:
            _breakdown, _smc, accepted_ctx, structural_context = _observation_contexts()
            reason = _as_list_copy(_first_present(sig.get("reason"), accepted_ctx.get("reason")))
            phase = _first_present(sig.get("phase"), accepted_ctx.get("phase"), _extract_dispatch_phase(sig))
            bos_type = _first_present(sig.get("bos_type"), accepted_ctx.get("bos_type"), _extract_live_bos_type(sig))

            payload = {
                "timestamp": format_vn_time(now_ts),
                "timestamp_unix": now_ts,
                "symbol": sig.get("symbol", ""),
                "side": _first_present(sig.get("side"), accepted_ctx.get("side")),
                "entry_type": sig.get("entry_type", ""),
                "score": sig.get("score", ""),
                "entry": _first_present(sig.get("entry"), accepted_ctx.get("entry")),
                "sl": _first_present(sig.get("sl"), accepted_ctx.get("sl")),
                "tp": _first_present(sig.get("tp"), accepted_ctx.get("tp")),
                "suppress_reason": suppression_reason or "unknown",
                "phase": phase,
                "bos_type": bos_type,
                "exhaustion_cls": _first_present(
                    sig.get("exhaustion_cls"),
                    sig.get("exhaustion"),
                ),
                "signal_created_ts": _first_present(sig.get("signal_created_ts"), accepted_ctx.get("signal_created_ts")),
                "reason": reason,
                "smc_zone": _smc_value("smc_zone", "UNKNOWN"),
                "liquidity_sweep": _smc_value("liquidity_sweep", "NONE"),
                "bos_confirmation": _smc_value("bos_confirmation", "UNKNOWN"),
                "smc_bias": _smc_value("smc_bias", "NEUTRAL"),
                "range_context": _smc_value("range_context", "UNKNOWN"),
                "invalid_context": _invalid_context_value(),
                "structural_context": _json_safe_copy(structural_context),
            }
            if isinstance(extra_fields, dict):
                for key, value in extra_fields.items():
                    if key:
                        payload[str(key)] = _json_safe_copy(value)
            log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
            os.makedirs(log_dir, exist_ok=True)
            file_path = os.path.join(log_dir, "paper_signal_observations.jsonl")
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
        except Exception as exc:
            print(f"[PAPER OBSERVE] durable log failed: {exc}")

    def _ttl_for_reason(reason_value) -> int:
        return int(
            _PAPER_OBSERVATION_REASON_TTL_SECS.get(
                str(reason_value or "unknown"),
                _PAPER_OBSERVATION_TTL_SECS,
            )
        )

    def _coerce_trade_ts(value):
        try:
            ts = float(value)
        except (TypeError, ValueError):
            return None
        if ts <= 0 or ts != ts:
            return None
        if ts > 1_000_000_000_000:
            ts = ts / 1000.0
        return ts

    def _trade_recent_open_ts(trade):
        for field in ("open_time", "entry_time", "created_at", "signal_created_ts", "time"):
            ts = _coerce_trade_ts(trade.get(field))
            if ts is not None:
                return ts
        return None

    def _paper_has_recent_matching_open_trade(raw_symbol, raw_side, raw_etype, now_ts, ttl_secs):
        for trade in getattr(ctx, "trades", []) or []:
            if trade.get("status", "OPEN") != "OPEN":
                continue
            if trade.get("owner", "bot") != "bot":
                continue
            if trade.get("symbol") != raw_symbol or trade.get("side") != raw_side:
                continue

            trade_etype = trade.get("entry_type") or trade.get("strategy")
            if trade_etype and raw_etype and str(trade_etype) != str(raw_etype):
                continue

            opened_ts = _trade_recent_open_ts(trade)
            if opened_ts is None:
                continue
            if now_ts - opened_ts < ttl_secs:
                return opened_ts
        return None

    def _safe_text(value, default="?"):
        text = default if value is None else str(value)
        return html.escape(text, quote=False)

    def _safe_num(value, digits=6):
        try:
            if value is None:
                return "n/a"
            parsed = float(value)
            if parsed != parsed:
                return "n/a"
            return str(round(parsed, digits))
        except (TypeError, ValueError):
            return "n/a"

    raw_symbol = sig.get("symbol", "?")
    raw_side = sig.get("side", "?")
    raw_etype = sig.get("entry_type", "?")
    now = time.time()
    reason_ttl = _ttl_for_reason(suppression_reason)
    _log_paper_observation_payload(now)
    if not bool(config.get("telegram_paper_observe_enabled", False)):
        print(
            f"[PAPER OBSERVE] telegram disabled symbol={raw_symbol} "
            f"side={raw_side} reason={suppression_reason}"
        )
        return

    if suppression_reason == "symbol_already_open_in_paper":
        recent_open_ts = _paper_has_recent_matching_open_trade(
            raw_symbol,
            raw_side,
            raw_etype,
            now,
            _PAPER_OBSERVATION_OPEN_SYMBOL_TTL_SECS,
        )
        if recent_open_ts is not None:
            print(
                f"[PAPER OBSERVE] telegram suppressed anti_spam=open_symbol_recent "
                f"symbol={raw_symbol} side={raw_side} etype={raw_etype} "
                f"age={round(now - recent_open_ts)}s "
                f"ttl={_PAPER_OBSERVATION_OPEN_SYMBOL_TTL_SECS}s"
            )
            return

    symbol = _safe_text(raw_symbol)
    side = _safe_text(raw_side)
    score = _safe_num(sig.get("score"), digits=1)
    etype = _safe_text(raw_etype)
    entry = _safe_num(sig.get("entry"), digits=6)
    sl = _safe_num(sig.get("sl"), digits=6)
    reason = _safe_text(suppression_reason, default="unknown")
    direction_icon = "🟢" if raw_side == "LONG" else "🔴"
    msg = (
        f"👁 PAPER SIGNAL OBSERVED (not executed)\n"
        f"{direction_icon} {symbol} {side} | {etype} | score={score}\n"
        f"E: {entry}  SL: {sl}\n"
        f"suppressed: {reason}"
    )
    print(
        f"[PAPER OBSERVE] {raw_symbol} {raw_side} etype={raw_etype} score={score} "
        f"suppressed={suppression_reason}"
    )
    eviction_age = max(_PAPER_OBSERVATION_REASON_TTL_SECS.values()) * 2
    stale_keys = [
        key
        for key, last_ts in _paper_observation_last_sent.items()
        if now - last_ts > eviction_age
    ]
    for key in stale_keys:
        _paper_observation_last_sent.pop(key, None)

    dedup_key = (
        str(raw_symbol or "?"),
        str(raw_side or "?"),
        str(raw_etype or "?"),
        str(suppression_reason or "unknown"),
    )
    last_sent = _paper_observation_last_sent.get(dedup_key, 0)
    if now - last_sent < reason_ttl:
        print(
            f"[PAPER OBSERVE] telegram throttled symbol={raw_symbol} "
            f"reason={suppression_reason} ttl={reason_ttl}s"
        )
        return
    _paper_observation_last_sent[dedup_key] = now
    try:
        send_telegram(msg, prefix=ctx.mode_prefix)
    except Exception:
        pass


def dispatch_to_executor(signals, ctx):
    """
    Fan-out shared signals to a single executor context.

    Steps:
      1. Check executor pause state
      2. Filter by executor's own open trades
      3. Filter by executor's own cooldowns
      4. Apply executor-specific execution policy (testnet)
      5. Select top-N using same logic as original run()
      6. Call open_trade(t, ctx) for each selected signal

    Paper visibility contract (Issue 2 fix):
      For paper executors, signals that pass strategy_execution_filter but are
      suppressed by execution gates (cooldown, open-symbol, pause, slot) are
      still notified via _log_paper_signal_observation(). This ensures PAPER
      always observes every valid signal that LIVE may execute, preserving its
      role as a broad research/analytics/observability layer.
    """
    from execution import (
        open_trade,
        ENTRY_COOLDOWN,
        LOSS_COOLDOWN,
        update_signal_state,
        paper_dd_rebaseline_pending_blocks_new_paper_entries,
    )
    from config import config

    # ===== PAUSE CHECK =====
    if ctx.pause_until and time.time() < ctx.pause_until:
        print(
            f"[{ctx.name.upper()} PAUSED] skip dispatch - "
            f"resumes at {format_vn_time(ctx.pause_until)}"
        )
        # Paper visibility: even when paused, emit observations for all valid signals
        # so analysts can see what setups are forming during the pause window.
        if ctx.execution_mode == "paper":
            for _sig in signals:
                _accepted, _ = strategy_execution_filter(_sig, ctx)
                if _accepted:
                    _log_paper_signal_observation(_sig, "executor_paused", ctx)
        return

    if ctx.execution_mode == "paper":
        _pending_blocked, _pending_status = paper_dd_rebaseline_pending_blocks_new_paper_entries(
            ctx=ctx,
            reason_context="dispatch_to_executor",
        )
        if _pending_blocked:
            print(
                "[PAPER DD REBASELINE PENDING] drain open paper trades before new entries "
                f"open_paper_trades={_pending_status.get('open_paper_trades', 0)}"
            )
            for _sig in signals:
                _accepted, _ = strategy_execution_filter(_sig, ctx)
                if _accepted:
                    _log_paper_signal_observation(
                        _sig,
                        "PAPER_DD_REBASELINE_PENDING_DRAIN",
                        ctx,
                    )
            return

    # ===== DD RESET CHECK =====
    if ctx.pause_until > 0 and time.time() >= ctx.pause_until:
        print(f"[{ctx.name.upper()}] [DD RESET] Pause expired -> resetting peak and state")
        ctx.equity_peak = ctx.account_balance
        ctx.pause_until = 0
        if ctx.execution_mode == "paper":
            config["pause_until"] = 0
            config["equity_peak"] = ctx.equity_peak
            try:
                with open("config.json", "r", encoding="utf-8") as _f:
                    _disk = json.load(_f)
            except Exception:
                _disk = dict(config)
            _disk["pause_until"] = 0
            _disk["equity_peak"] = ctx.equity_peak
            with open("config.tmp", "w", encoding="utf-8") as f:
                json.dump(_disk, f, indent=2, ensure_ascii=False)
            os.replace("config.tmp", "config.json")
        if ctx.execution_mode in ("testnet", "live"):
            ctx.save_account_state()
        from telegram import send_telegram
        send_telegram("🟢 DD RESET - new cycle started", prefix=ctx.mode_prefix)

    ctx.confirm_count_this_cycle = 0
    now = time.time()

    # ===== FILTER: executor's own open symbols =====
    open_symbols = {t["symbol"] for t in ctx.trades if t.get("status", "OPEN") == "OPEN"}

    # ===== FILTER: executor's own cooldowns + signal freshness =====
    # SIGNAL_MAX_AGE_SECS: continuation signals (CONFIRM, REVERSAL_CONFIRM) must be
    # executed within this window from creation time or rejected as stale.
    # Configurable via config.json "signal_max_age_secs" (default 180s = 3 scan cycles).
    _signal_max_age = float(config.get("signal_max_age_secs", 180))
    # Entry types subject to expiration — early/swing are regenerated fresh each cycle
    _EXPIRY_ENTRY_TYPES = {"CONFIRM", "REVERSAL_CONFIRM", "EARLY_V2", "SWING_RETEST"}

    candidate_signals = []
    for sig in signals:
        symbol = sig["symbol"]

        if symbol in open_symbols:
            # Paper visibility: symbol already open in paper — observe but don't execute.
            if ctx.execution_mode == "paper":
                _accepted, _ = strategy_execution_filter(sig, ctx)
                if _accepted:
                    _log_paper_signal_observation(sig, "symbol_already_open_in_paper", ctx)
            continue

        in_entry_cd = (
            symbol in ctx.entry_cooldown
            and now - ctx.entry_cooldown[symbol] < ENTRY_COOLDOWN
        )
        in_loss_cd = (
            symbol in ctx.cooldown
            and now - ctx.cooldown[symbol] < LOSS_COOLDOWN
        )
        if in_entry_cd or in_loss_cd:
            # Paper visibility: cooldown suppression — observe the signal anyway.
            if ctx.execution_mode == "paper":
                _accepted, _ = strategy_execution_filter(sig, ctx)
                if _accepted:
                    _cd_reason = "entry_cooldown" if in_entry_cd else "loss_cooldown"
                    _log_paper_signal_observation(sig, _cd_reason, ctx)
            continue

        # ── SIGNAL FRESHNESS GATE ──────────────────────────────────────────────
        # Reject continuation setups that survived too long since scan time.
        # Prevents LIVE executing a stale BOS context from a prior scan cycle.
        _sig_etype = sig.get("entry_type", "")
        if _sig_etype in _EXPIRY_ENTRY_TYPES:
            _sig_created = sig.get("signal_created_ts", now)
            _sig_age = now - _sig_created
            if _sig_age > _signal_max_age:
                _stale_reason = f"stale_signal age={round(_sig_age)}s > {int(_signal_max_age)}s etype={_sig_etype}"
                print(
                    f"[STALE SIGNAL] {symbol} {_sig_etype} "
                    f"age={round(_sig_age)}s > {int(_signal_max_age)}s - rejected"
                )
                if ctx.execution_mode == "live":
                    _log_live_rejection(symbol, sig.get("side", ""), sig.get("score", 0), _stale_reason)
                continue

        candidate_signals.append(sig)

    _compute_portfolio_exhaustion(candidate_signals)

    # ===== EXECUTOR-AWARE STRATEGY FILTER =====
    _gated = []
    for _sig in candidate_signals:
        _accepted, _reason = strategy_execution_filter(_sig, ctx)
        if _accepted:
            _gated.append(_sig)
        else:
            print(
                f"[{ctx.name.upper()} STRATEGY FILTER] Rejected: {_sig['symbol']} {_sig.get('side', '')} "
                f"etype={_sig.get('entry_type', '')} score={round(_sig.get('score', 0), 1)} - {_reason}"
            )
            if ctx.execution_mode == "live":
                _log_live_rejection(_sig["symbol"], _sig.get("side", ""), _sig.get("score", 0), _reason)
    candidate_signals = _gated

    # ===== APPLY EXECUTOR-SPECIFIC POLICY =====
    if ctx.execution_mode == "paper" and config.get("paper_filter_confirm_pre_break_low_near", True):
        policy_accepted = []
        for sig in candidate_signals:
            if _is_confirm_pre_break_low_near(sig):
                reason = "paper_confirm_pre_break_low_near_filter"
                print(
                    f"[PAPER FILTER] Suppressed: {sig['symbol']} {sig.get('side', '')} "
                    f"etype={sig.get('entry_type', '')} phase={_extract_dispatch_phase(sig) or 'missing'} "
                    f"bos={_extract_live_bos_type(sig) or 'missing'} score={round(sig.get('score', 0), 1)} - {reason}"
                )
                _log_paper_signal_observation(sig, reason, ctx)
                continue
            policy_accepted.append(sig)
        candidate_signals = policy_accepted

    if ctx.execution_mode == "paper" and config.get("paper_gate_confirm_short_pre_break_low", True):
        policy_accepted = []
        for sig in candidate_signals:
            if _is_confirm_short_pre_break_low(sig):
                reason = "paper_gate_confirm_short_pre_break_low"
                print(
                    f"[PAPER GATE] Blocked: {sig['symbol']} {sig.get('side', '')} "
                    f"etype={sig.get('entry_type', '')} phase={_extract_dispatch_phase(sig) or 'missing'} "
                    f"bos={_extract_live_bos_type(sig) or 'missing'} score={round(sig.get('score', 0), 1)} - {reason}"
                )
                _log_paper_confirm_pre_break_low_gate(sig, ctx)
                continue
            policy_accepted.append(sig)
        candidate_signals = policy_accepted

    if ctx.execution_mode == "paper" and config.get("paper_enable_smc_confirm_filter", False):
        smc_filter_mode = str(config.get("paper_smc_confirm_phase2_mode", "strict_conflict") or "strict_conflict")
        if smc_filter_mode not in {"strict_conflict", "expanded_conflict"}:
            smc_filter_mode = "strict_conflict"
        policy_accepted = []
        for sig in candidate_signals:
            _smc_blocked, reason, smc_filter_rule = _paper_smc_confirm_filter_decision(sig, smc_filter_mode)
            if _smc_blocked:
                print(
                    f"[PAPER FILTER] Suppressed: {sig['symbol']} {sig.get('side', '')} "
                    f"etype={sig.get('entry_type', '')} mode={smc_filter_mode} rule={smc_filter_rule} "
                    f"smc_bias={_signal_smc_value(sig, 'smc_bias') or 'missing'} "
                    f"range={_signal_smc_value(sig, 'range_context') or 'missing'} "
                    f"liquidity={_signal_smc_value(sig, 'liquidity_sweep') or 'missing'} "
                    f"score={round(sig.get('score', 0), 1)} - {reason}"
                )
                _log_paper_signal_observation(
                    sig,
                    reason,
                    ctx,
                    {
                        "smc_filter_mode": smc_filter_mode,
                        "smc_filter_rule": smc_filter_rule,
                    },
                )
                continue
            policy_accepted.append(sig)
        candidate_signals = policy_accepted

    if ctx.execution_mode == "paper" and config.get("paper_filter_confirm_retest_violated_bos_trap", True):
        policy_accepted = []
        _failed_cont_watch_scan_state = {"total": 0, "symbols": {}}
        for sig in candidate_signals:
            if _is_confirm_retest_violated_bos_trap(sig):
                reason = "paper_confirm_retest_violated_bos_trap_filter"
                print(
                    f"[PAPER FILTER] Suppressed: {sig['symbol']} {sig.get('side', '')} "
                    f"etype={sig.get('entry_type', '')} phase={_extract_dispatch_phase(sig) or 'missing'} "
                    f"bos={_extract_live_bos_type(sig) or 'missing'} score={round(sig.get('score', 0), 1)} - {reason}"
                )
                _log_paper_signal_observation(sig, reason, ctx)
                _log_failed_continuation_reversal_watch(sig, reason, ctx, _failed_cont_watch_scan_state)
                continue
            policy_accepted.append(sig)
        candidate_signals = policy_accepted

    if ctx.execution_mode == "testnet":
        policy_accepted = []
        for sig in candidate_signals:
            accepted, reason = testnet_execution_filter(sig, ctx)
            if accepted:
                policy_accepted.append(sig)
            else:
                print(
                    f"[TESTNET FILTER] Rejected: {sig['symbol']} {sig.get('side', '')} "
                    f"score={round(sig.get('score', 0), 1)} - {reason}"
                )
        candidate_signals = policy_accepted

    if ctx.execution_mode == "live":
        policy_accepted = []
        for sig in candidate_signals:
            accepted, reason = live_execution_filter(sig, ctx)
            if accepted:
                _log_live_shadow_smc_decision(sig, ctx)
                policy_accepted.append(sig)
            else:
                print(
                    f"[LIVE FILTER] Rejected: {sig['symbol']} {sig.get('side', '')} "
                    f"score={round(sig.get('score', 0), 1)} - {reason}"
                )
                # ── AUDIT LOG — write every LIVE rejection to CSV ────────────────────
                # Allows offline analysis of rejection distribution / filter starvation.
                # See _log_live_rejection() docstring for column schema and analysis recipe.
                _log_live_rejection(sig["symbol"], sig.get("side", ""), sig.get("score", 0), reason)
        candidate_signals = policy_accepted

    if ctx.execution_mode == "paper":
        candidate_signals = _apply_paper_structural_score_modifier(candidate_signals, ctx)

    if ctx.execution_mode == "paper" and config.get("paper_smc_main_enabled", False):
        candidate_signals = [
            sig for sig in candidate_signals
            if str(sig.get("entry_type") or "").upper() != "CONFIRM"
        ]

    # ===== SELECT TOP-N (same logic as original run()) =====
    _TOP_N = 4
    _confirm_pool = [
        s for s in candidate_signals
        if not s.get("entry_type", "").startswith("REVERSAL")
    ]
    _reversal_pool = [
        s for s in candidate_signals
        if s.get("entry_type", "").startswith("REVERSAL")
    ]

    _selected = _confirm_pool[:2] + _reversal_pool[:1]
    _selected_syms = {s["symbol"] for s in _selected}

    for s in _confirm_pool[2:]:
        if len(_selected) >= _TOP_N:
            break
        if s["symbol"] not in _selected_syms:
            _selected.append(s)
            _selected_syms.add(s["symbol"])

    if ctx.execution_mode == "live":
        max_live = _get_max_live_trades()
        with ctx.lock:
            open_live_count, pending_live_count, effective_live_count = _live_slot_snapshot(ctx)
        available_slots = max(0, max_live - effective_live_count)
        if available_slots <= 0:
            for sig in _selected:
                _slot_reason = (
                    f"live_max_open_trades_reached max_live_trades={max_live} "
                    f"open={open_live_count} pending={pending_live_count} "
                    f"effective={effective_live_count}"
                )
                _log_live_slot_decision(sig, ctx, _slot_reason)
                _log_live_rejection(sig["symbol"], sig.get("side", ""), sig.get("score", 0), _slot_reason)
            _selected = []
        elif len(_selected) > available_slots:
            for sig in _selected[available_slots:]:
                _slot_reason = (
                    f"live_available_slots_cap available={available_slots} "
                    f"max_live_trades={max_live} open={open_live_count} "
                    f"pending={pending_live_count} effective={effective_live_count}"
                )
                _log_live_slot_decision(sig, ctx, _slot_reason)
                _log_live_rejection(sig["symbol"], sig.get("side", ""), sig.get("score", 0), _slot_reason)
            _selected = _selected[:available_slots]

    # Paper visibility: signals that passed all filters but were not selected
    # by the top-N cap should still be observed by paper.
    if ctx.execution_mode == "paper":
        _selected_set = {s["symbol"] for s in _selected}
        for _unselected in candidate_signals:
            if _unselected["symbol"] not in _selected_set:
                _log_paper_signal_observation(_unselected, "top_n_cap_not_selected", ctx)

    # ===== EXECUTE =====
    entry_attempted = set()

    for sig in _selected:
        symbol = sig["symbol"]
        if symbol in entry_attempted:
            print(
                f"[{ctx.name.upper()}] [BLOCK] DUPLICATE_TRADE {symbol} "
                f"reason=duplicate_entry_attempt"
            )
            continue
        entry_attempted.add(symbol)

        if ctx.execution_mode == "live":
            max_live = _get_max_live_trades()
            with ctx.lock:
                open_live_count, pending_live_count, effective_live_count = _live_slot_snapshot(ctx)
            if effective_live_count >= max_live:
                _slot_reason = (
                    f"live_max_open_trades_reached max_live_trades={max_live} "
                    f"pre_open open={open_live_count} pending={pending_live_count} "
                    f"effective={effective_live_count}"
                )
                _log_live_slot_decision(sig, ctx, _slot_reason)
                print(
                    f"[LIVE FILTER] Rejected: {symbol} {sig.get('side', '')} "
                    f"score={round(sig.get('score', 0), 1)} - {_slot_reason}"
                )
                _log_live_rejection(symbol, sig.get("side", ""), sig.get("score", 0), _slot_reason)
                continue
            _feasible, _est_notional, _min_notional = _live_prefilter_min_notional(sig, ctx)
            if not _feasible:
                _notional_reason = (
                    f"insufficient_notional est={round(_est_notional, 4)} "
                    f"min={_min_notional}"
                )
                print(
                    f"[LIVE PREFILTER] {symbol} skipped "
                    f"estimated_notional={round(_est_notional, 4)} "
                    f"min_notional={_min_notional} "
                    f"reason=INSUFFICIENT_POSITION_SIZE"
                )
                _log_live_rejection(symbol, sig.get("side", ""), sig.get("score", 0), _notional_reason)
                continue

            # ── LIVE GEOMETRY SAFETY CHECK ──────────────────────────────────────
            # Reject signals where entry/SL distance has compressed below minimum.
            # Protects against stale re-confirmed setups where price moved against
            # the original BOS context, producing near-zero risk distance.
            # Configurable via config.json "live_min_sl_ratio" (default 0.003 = 0.3%).
            _live_min_sl_ratio = float(config.get("live_min_sl_ratio", 0.003))
            _geo_entry = sig.get("entry", 0) or 0
            _geo_sl    = sig.get("sl", 0) or 0
            if _geo_entry > 0 and _geo_sl > 0:
                _sl_dist_ratio = abs(_geo_entry - _geo_sl) / _geo_entry
                if _sl_dist_ratio < _live_min_sl_ratio:
                    _geo_reason = (
                        f"compressed_geometry sl_dist={_sl_dist_ratio:.4%} "
                        f"< min={_live_min_sl_ratio:.4%}"
                    )
                    print(
                        f"[LIVE STALE SL] {symbol} {sig.get('side','')} "
                        f"sl_dist={_sl_dist_ratio:.4%} < min={_live_min_sl_ratio:.4%} - "
                        f"compressed geometry rejected"
                    )
                    _log_live_rejection(symbol, sig.get("side", ""), sig.get("score", 0), _geo_reason)
                    continue

        # ── IMMUTABLE SNAPSHOT: deepcopy BEFORE any executor-specific mutation ──
        # bos_type normalization is applied here on the private copy,
        # NOT inside live_execution_filter() where it would mutate the shared signal.
        t = copy.deepcopy(sig)
        if ctx.execution_mode == "live":
            t["bos_type"] = _normalize_live_bos_type(t.get("bos_type"))
        success = open_trade(t, ctx)
        if success:
            if ctx.execution_mode == "paper" and str(t.get("entry_type") or "").upper() == "CONFIRM":
                _paper_confirm_entry_context_snapshot(t)
                _paper_confirm_entry_acceptance_shadow_snapshot(t)
            ctx.entry_cooldown[symbol] = time.time()
            update_signal_state(
                symbol,
                t["side"],
                t.get("entry_real") or t.get("entry", 0),
                executed=True,
                ctx=ctx,
            )

    if _paper_smc_research_qualified_mode_allowed(ctx):
        if config.get("paper_smc_main_enabled", False) or config.get("paper_smc_main_gate_shadow_enabled", False):
            _dispatch_paper_smc_main(ctx)
        if config.get("paper_smc_research_qualified_enabled", False):
            _dispatch_paper_smc_research_qualified_lane(ctx)
        if (
            not config.get("paper_smc_main_enabled", False)
            and config.get("paper_enable_smc_research_lane", False)
            and not config.get("paper_smc_research_qualified_enabled", False)
        ):
            _dispatch_paper_smc_research_lane(ctx)

    # Live research lane — isolated from paper research; gates on execution_mode
    # and live_smc_research_enabled internally.  Safe to call for any ctx.
    _dispatch_live_smc_research_lane(ctx)

    if _selected:
        print(
            f"[{ctx.name.upper()} DISPATCH] attempted={len(entry_attempted)} "
            f"candidates={len(candidate_signals)} signals_total={len(signals)}"
        )
