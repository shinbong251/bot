import time, json, math, os, threading
from datetime import datetime
import csv
from config import ACCOUNT_BALANCE, RISK_PER_TRADE, EQUITY_PEAK, config, DEBUG, _strip_secrets_for_save
from execution_mode import EXECUTION_MODE, TRADES_CSV
from state_manager import (
    save_open_trades,
    save_trade,
    save_tier_log,
    log_false_positive,
    log_entry_clean,
    log_wyckoff_outcome,
    load_open_trades,
    normalize_trade_schema,
    log_confirm_reject,
    write_runtime_error,
    log_exhaustion_counterfactual,
)
from notifier import send_entry, send_exit, send_tp_break, send_testnet_entry, send_live_entry, fmt_price, fmt_pnl, format_vn_time, engine_label, paper_engine_name, format_paper_smc_close
from helper import check_correlation, check_signal_cooldown, dynamic_giveback, stats, compression_watchlist, signal_state
from telegram import send_telegram, send_telegram_gated
import telegram_dedup
from bos import get_volatility
from pool_pipeline import fetch, fetch_multi, build_pool_pipeline, get_symbols_pool, fetch_ticker, fetch_cached, fetch_cached_with_meta
from entry import analyze, swing_pipeline, build_trade, reset_scan_filter_summary, print_scan_filter_summary, get_strategy_observability_counters, update_reversal_shadow_outcomes, update_swing_retest_shadow_outcomes, update_early_cont_shadow_outcomes, update_confirm_structural_outcomes, update_paper_smc_v0_2_shadow_outcomes, update_paper_smc_main_open_geometry_observer
from scoring import apply_signal_scoring
pause_until = config.get("pause_until", 0)
early_count = 0
confirm_count_this_cycle = 0
MAX_CONFIRM_PER_CYCLE = 3
_paper_trail_update_last_sent = {}

SIGNAL_TYPE_SUMMARY_ORDER = ("CONFIRM", "SWING_RETEST", "REVERSAL_CONFIRM", "EARLY_CONT")
STRATEGY_OBSERVABILITY_FIELDS = [
    "timestamp",
    "scan_id",
    "total_signals",
    "CONFIRM",
    "SWING_RETEST",
    "REVERSAL_CONFIRM",
    "EARLY_CONT",
    "REVERSAL_SHADOW",
    "reversal_context_fail",
    "reversal_market_state_gate_failure",
    "reversal_missing_extended_exhaustion",
    "reversal_missing_bos_near",
]
_strategy_observability_scan_id = 0
_paper_smc_research_open_seen = set()
_paper_smc_research_closed_seen = set()
_paper_smc_research_summary_last_ts = time.time()
_paper_smc_research_closed_since_summary = []
_paper_smc_research_live_warned = set()
_trade_mgmt_check_log_last = {}
_trade_mgmt_freshness_state = {}
_trade_mgmt_stuck_summary_last_ts = 0
_open_trade_data_refresh_last_ts = 0
_paper_quality_router_context_by_symbol = {}
_paper_quality_router_context_scan_id = None
_btc_m15_shadow_cache = {}  # keyed "BTCUSDT" → latest raw router row
_TRADE_MGMT_FRESHNESS_LOG = os.path.join("logs", "trade_management_freshness.jsonl")
_PAPER_TRADE_QUALITY_LOG = os.path.join("logs", "paper_trade_quality_observations.jsonl")
_PAPER_DD_PAUSE_LOG = os.path.join("logs", "paper_dd_pause_events.jsonl")
_TRADE_MGMT_CHECK_THROTTLE_SECS = 600
_TRADE_MGMT_CACHE_MAX_AGE_SECS = 120
_paper_dd_warn_only_breach_active = False


def _paper_dd_pause_mode():
    mode = str(config.get("paper_dd_pause_mode", "ENFORCE")).strip().upper()
    if mode not in ("ENFORCE", "WARN_ONLY"):
        print(
            f"[PAPER DD CONFIG] invalid paper_dd_pause_mode={mode!r}; "
            "falling back to ENFORCE"
        )
        return "ENFORCE"
    return mode


def _write_paper_dd_pause_event(row):
    try:
        os.makedirs(os.path.dirname(os.path.abspath(_PAPER_DD_PAUSE_LOG)), exist_ok=True)
        with open(_PAPER_DD_PAUSE_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str) + "\n")
    except Exception as exc:
        print(f"[PAPER DD LOG] event write failed: {exc}")


def log_paper_dd_pause_status(ctx):
    if ctx is None or ctx.execution_mode != "paper":
        return
    mode = _paper_dd_pause_mode()
    drawdown = (
        (ctx.equity_peak - ctx.account_balance) / ctx.equity_peak
        if ctx.equity_peak > 0
        else 0
    )
    row = {
        "timestamp": datetime.now().astimezone().isoformat(),
        "event_type": "PAPER_DD_PAUSE_STATUS",
        "paper_dd_pause_mode": mode,
        "max_drawdown_pct": MAX_DRAWDOWN * 100,
        "current_drawdown_pct": drawdown * 100,
        "execution_mode": ctx.execution_mode,
        "enforcement_active": mode == "ENFORCE",
    }
    _write_paper_dd_pause_event(row)
    print(
        f"[PAPER DD STATUS] mode={mode} "
        f"drawdown={drawdown * 100:.3f}% max={MAX_DRAWDOWN * 100:.3f}% "
        f"enforcement_active={mode == 'ENFORCE'}"
    )


def _tmf_safe_scalar(value):
    try:
        if value is None:
            return None
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None
            return value
        if hasattr(value, "item"):
            return _tmf_safe_scalar(value.item())
    except Exception:
        return None
    return value


def _tmf_candle_time(df):
    try:
        if df is None or len(df) <= 0 or "time" not in df:
            return None
        raw = _tmf_safe_scalar(df["time"].iloc[-1])
        if raw is None:
            return None
        ts = float(raw)
        if ts > 10_000_000_000:
            ts = ts / 1000.0
        return ts
    except Exception:
        return None


def _tmf_current_r(t, close_price):
    try:
        entry = t.get("entry_real") or t.get("entry")
        risk = abs(entry - t["sl_init"])
        if risk <= 0:
            return None
        if t.get("side") == "LONG":
            return (close_price - entry) / risk
        return (entry - close_price) / risk
    except Exception:
        return None


def _tmf_cfg_bool(key, default=False):
    try:
        return bool(config.get(key, default))
    except Exception:
        return default


def _tmf_cfg_float(key, default):
    try:
        return float(config.get(key, default))
    except Exception:
        return default


def _tmf_key(t, execution_mode):
    trade_id = t.get("id") or t.get("trade_id")
    symbol = t.get("symbol") or "UNKNOWN"
    if trade_id:
        return (execution_mode, str(trade_id))
    return (execution_mode, symbol)


def _tmf_open_trade_age_secs(t, now_ts):
    try:
        open_ts = t.get("time") or t.get("entry_time") or t.get("signal_created_ts")
        if open_ts:
            return max(0.0, now_ts - float(open_ts))
    except Exception:
        return None
    return None


def _tmf_normalize_skip_reason(reason):
    if reason == "insufficient_rows":
        return "insufficient_rows"
    if reason and "15m" in str(reason):
        return "missing_15m_cache"
    if reason and "5m" in str(reason):
        return "missing_or_stale_5m_cache"
    if reason and "trailing" in str(reason):
        return "missing_15m_cache"
    return "unknown"


def _tmf_stale_feed_status(decision_type, skip_reason, cache_ts, consecutive_skip_count, secs_since_first_skip):
    if decision_type != "SKIP_NO_FRESH_PRICE":
        return "OK"
    enabled = _tmf_cfg_bool("trade_freshness_stuck_feed_enabled", True)
    skip_threshold = _tmf_cfg_float("trade_freshness_stuck_feed_skip_threshold", 30)
    secs_threshold = _tmf_cfg_float("trade_freshness_stuck_feed_secs_threshold", 120)
    if enabled and (
        consecutive_skip_count >= skip_threshold
        or (secs_since_first_skip is not None and secs_since_first_skip >= secs_threshold)
    ):
        return "STUCK_FEED"
    if cache_ts is None:
        return "MISSING_CACHE"
    if skip_reason in ("missing_or_stale_5m_cache", "missing_15m_cache"):
        return "STALE_CACHE"
    return "MISSING_CACHE"


def _tmf_record_skip(t, execution_mode, reason, cache_ts, now_ts):
    key = _tmf_key(t, execution_mode)
    state = _trade_mgmt_freshness_state.setdefault(
        key,
        {
            "symbol": t.get("symbol"),
            "execution_mode": execution_mode,
            "consecutive_skip_count": 0,
            "first_skip_ts": None,
            "last_successful_manage_ts": None,
        },
    )
    state["symbol"] = t.get("symbol")
    state["execution_mode"] = execution_mode
    state["consecutive_skip_count"] = int(state.get("consecutive_skip_count") or 0) + 1
    if not state.get("first_skip_ts"):
        state["first_skip_ts"] = now_ts
    skip_reason = _tmf_normalize_skip_reason(reason)
    first_skip_ts = state.get("first_skip_ts")
    last_success_ts = state.get("last_successful_manage_ts")
    secs_since_first_skip = now_ts - first_skip_ts if first_skip_ts else None
    secs_since_last_success = now_ts - last_success_ts if last_success_ts else None
    stale_status = _tmf_stale_feed_status(
        "SKIP_NO_FRESH_PRICE",
        skip_reason,
        cache_ts,
        state["consecutive_skip_count"],
        secs_since_first_skip,
    )
    state["last_status"] = stale_status
    state["last_skip_reason"] = skip_reason
    state["last_seen_ts"] = now_ts
    return {
        "consecutive_skip_count": state["consecutive_skip_count"],
        "first_skip_ts": first_skip_ts,
        "last_successful_manage_ts": last_success_ts,
        "secs_since_last_successful_manage": secs_since_last_success,
        "secs_since_first_skip": secs_since_first_skip,
        "stale_feed_status": stale_status,
        "skip_reason": skip_reason,
    }


def _tmf_record_success(t, execution_mode, now_ts):
    key = _tmf_key(t, execution_mode)
    state = _trade_mgmt_freshness_state.setdefault(
        key,
        {
            "symbol": t.get("symbol"),
            "execution_mode": execution_mode,
            "consecutive_skip_count": 0,
            "first_skip_ts": None,
            "last_successful_manage_ts": None,
        },
    )
    state["symbol"] = t.get("symbol")
    state["execution_mode"] = execution_mode
    state["consecutive_skip_count"] = 0
    state["first_skip_ts"] = None
    state["last_successful_manage_ts"] = now_ts
    state["last_status"] = "OK"
    state["last_seen_ts"] = now_ts
    return {
        "consecutive_skip_count": 0,
        "first_skip_ts": None,
        "last_successful_manage_ts": now_ts,
        "secs_since_last_successful_manage": 0.0,
        "secs_since_first_skip": None,
        "stale_feed_status": "OK",
        "skip_reason": None,
    }


def _tmf_maybe_notify_stuck_feed():
    global _trade_mgmt_stuck_summary_last_ts
    try:
        if not _tmf_cfg_bool("trade_freshness_notify_stuck_feed", False):
            return
        now_ts = time.time()
        interval = _tmf_cfg_float("trade_freshness_stuck_feed_summary_interval_secs", 900)
        if now_ts - _trade_mgmt_stuck_summary_last_ts < interval:
            return
        stuck = [
            state for state in _trade_mgmt_freshness_state.values()
            if state.get("last_status") == "STUCK_FEED"
        ]
        if not stuck:
            return
        _trade_mgmt_stuck_summary_last_ts = now_ts
        lines = [
            f"{s.get('execution_mode')} {s.get('symbol')} skips={s.get('consecutive_skip_count')} reason={s.get('last_skip_reason')}"
            for s in stuck[:10]
        ]
        send_telegram(
            "[TRADE FRESHNESS] stuck feed summary\n" + "\n".join(lines),
            channel="alerts",
        )
    except Exception as exc:
        if DEBUG:
            print(f"[TMF NOTIFY ERROR] {exc}")


def _tmf_build_row(
    t,
    execution_mode,
    decision_type,
    update_started_ts,
    update_finished_ts=None,
    price_meta=None,
    close_price=None,
    high_price=None,
    low_price=None,
    sl_before=None,
    sl_after=None,
    reason=None,
    exchange_sl_sync_requested_ts=None,
    exchange_sl_sync_confirmed_ts=None,
    exchange_sl_sync_failed=None,
    skip_reason=None,
):
    now_ts = time.time()
    price_meta = price_meta or {}
    cache_ts = price_meta.get("cache_ts")
    reason_value = reason or t.get("close_reason") or t.get("exit_type")
    if decision_type == "SKIP_NO_FRESH_PRICE":
        diag = _tmf_record_skip(t, execution_mode, skip_reason or reason_value, cache_ts, now_ts)
    else:
        diag = _tmf_record_success(t, execution_mode, now_ts)
    row = {
        "timestamp": format_vn_time(now_ts),
        "timestamp_unix": now_ts,
        "execution_mode": execution_mode,
        "symbol": t.get("symbol"),
        "side": t.get("side"),
        "entry_type": t.get("entry_type"),
        "trade_id": t.get("id"),
        "owner": t.get("owner"),
        "status": t.get("status"),
        "update_trades_started_ts": update_started_ts,
        "update_trades_finished_ts": update_finished_ts,
        "management_loop_age_ms": round((now_ts - update_started_ts) * 1000, 3) if update_started_ts else None,
        "price_source": price_meta.get("price_source") or "unknown",
        "candle_time": price_meta.get("candle_time"),
        "cache_ts": cache_ts,
        "price_age_ms": round((now_ts - cache_ts) * 1000, 3) if cache_ts else None,
        "cache_age_secs": price_meta.get("cache_age_secs"),
        "cache_max_age_secs": price_meta.get("cache_max_age_secs"),
        "close_price_used": close_price,
        "high_price_used": high_price,
        "low_price_used": low_price,
        "sl_before": sl_before,
        "sl_after": t.get("sl") if sl_after is None else sl_after,
        "tp": t.get("tp"),
        "rr": t.get("rr") or t.get("rr_real"),
        "max_profit_r": t.get("max_profit_r"),
        "current_r_estimate": _tmf_current_r(t, close_price),
        "consecutive_skip_count": diag.get("consecutive_skip_count"),
        "first_skip_ts": diag.get("first_skip_ts"),
        "last_successful_manage_ts": diag.get("last_successful_manage_ts"),
        "secs_since_last_successful_manage": diag.get("secs_since_last_successful_manage"),
        "secs_since_first_skip": diag.get("secs_since_first_skip"),
        "stale_feed_status": diag.get("stale_feed_status"),
        "skip_reason": diag.get("skip_reason"),
        "open_trade_age_secs": _tmf_open_trade_age_secs(t, now_ts),
        "decision_type": decision_type,
        "decision_ts": now_ts,
        "exchange_sl_sync_requested_ts": exchange_sl_sync_requested_ts,
        "exchange_sl_sync_confirmed_ts": exchange_sl_sync_confirmed_ts,
        "exchange_sl_sync_failed": exchange_sl_sync_failed,
        "reason": reason_value,
    }
    return {k: _tmf_safe_scalar(v) for k, v in row.items() if _tmf_safe_scalar(v) not in ("",)}


def _log_trade_management_freshness(t, execution_mode, decision_type, update_started_ts, **kwargs):
    try:
        row = _tmf_build_row(t, execution_mode, decision_type, update_started_ts, **kwargs)
        os.makedirs(os.path.dirname(_TRADE_MGMT_FRESHNESS_LOG), exist_ok=True)
        with open(_TRADE_MGMT_FRESHNESS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        if row.get("stale_feed_status") == "STUCK_FEED":
            _tmf_maybe_notify_stuck_feed()
    except Exception as exc:
        if DEBUG:
            print(f"[TMF LOG ERROR] {exc}")


def _log_management_refresh(
    symbol,
    tf,
    success,
    reason="open_trade_symbol",
    execution_mode="mixed",
    cache_age_before=None,
    cache_age_after=None,
    source="scan_phase",
):
    try:
        row = {
            "timestamp": format_vn_time(time.time()),
            "timestamp_unix": time.time(),
            "execution_mode": execution_mode,
            "symbol": symbol,
            "decision_type": "MANAGEMENT_REFRESH",
            "management_refresh_symbol": True,
            "refresh_reason": reason,
            "refresh_tf": tf,
            "refresh_success": bool(success),
            "cache_age_before": cache_age_before,
            "cache_age_after": cache_age_after,
            "source": source,
        }
        os.makedirs(os.path.dirname(_TRADE_MGMT_FRESHNESS_LOG), exist_ok=True)
        with open(_TRADE_MGMT_FRESHNESS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    except Exception as exc:
        if DEBUG:
            print(f"[TMF REFRESH LOG ERROR] {exc}")


def _collect_open_trade_symbols(executor_contexts=None):
    open_symbols = set()
    modes = {}

    sources = []
    if executor_contexts:
        for ctx in executor_contexts:
            sources.append((getattr(ctx, "execution_mode", None), getattr(ctx, "trades", [])))
    else:
        sources.append((EXECUTION_MODE, trades))

    for execution_mode, trade_list in sources:
        for t in trade_list or []:
            if not isinstance(t, dict):
                continue
            if t.get("status") != "OPEN":
                continue
            symbol = str(t.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            open_symbols.add(symbol)
            modes.setdefault(symbol, set()).add(execution_mode or "unknown")

    return open_symbols, modes


def _management_refresh_timeframes():
    try:
        raw = config.get("open_trade_data_refresh_timeframes", ["5m", "15m"])
        if isinstance(raw, str):
            raw = [raw]
        tfs = []
        for tf in raw or []:
            tf = str(tf or "").strip()
            if tf and tf not in tfs:
                tfs.append(tf)
        return tfs or ["5m", "15m"]
    except Exception:
        return ["5m", "15m"]


def _refresh_management_symbol_cache(symbols, symbol_modes, source):
    refreshed = set()
    refresh_interval = min(
        _tmf_cfg_float("open_trade_data_refresh_interval_secs", 60),
        _TRADE_MGMT_CACHE_MAX_AGE_SECS - 1,
    )
    for symbol in sorted(symbols or []):
        mode_set = symbol_modes.get(symbol) or set()
        mode_label = ",".join(sorted(str(m) for m in mode_set if m)) or "unknown"
        for tf in _management_refresh_timeframes():
            _df_cached, _cache_ts, cache_age_before = fetch_cached_with_meta(
                symbol,
                tf,
                max_age=_TRADE_MGMT_CACHE_MAX_AGE_SECS,
            )
            if cache_age_before is not None and cache_age_before < refresh_interval:
                continue
            try:
                df = fetch(symbol, tf)
                _df_after, _cache_ts_after, cache_age_after = fetch_cached_with_meta(
                    symbol,
                    tf,
                    max_age=_TRADE_MGMT_CACHE_MAX_AGE_SECS,
                )
                success = df is not None and _df_after is not None
                if success:
                    refreshed.add(symbol)
                else:
                    print(f"[MGMT REFRESH FAIL] {symbol} {tf} reason=fetch_failed")
                _log_management_refresh(
                    symbol,
                    tf,
                    success,
                    execution_mode=mode_label,
                    cache_age_before=cache_age_before,
                    cache_age_after=cache_age_after,
                    source=source,
                )
            except Exception as exc:
                print(f"[MGMT REFRESH FAIL] {symbol} {tf} reason={type(exc).__name__}: {exc}")
                _log_management_refresh(
                    symbol,
                    tf,
                    False,
                    execution_mode=mode_label,
                    cache_age_before=cache_age_before,
                    cache_age_after=None,
                    source=source,
                )
    return refreshed


def _refresh_open_trade_market_data(normal_scan_symbols, executor_contexts=None):
    normal_symbols = {
        str(sym).strip().upper()
        for sym in (normal_scan_symbols or [])
        if str(sym or "").strip()
    }
    open_symbols, symbol_modes = _collect_open_trade_symbols(executor_contexts)
    management_refresh_symbols = sorted(open_symbols - normal_symbols)

    if not management_refresh_symbols:
        return set()

    print(
        "[MGMT REFRESH] open trade symbol(s) outside scan universe: "
        + ", ".join(management_refresh_symbols)
    )

    _refresh_management_symbol_cache(
        management_refresh_symbols,
        symbol_modes,
        source="scan_phase",
    )

    return set(management_refresh_symbols)


def refresh_open_trade_market_data_timer(executor_contexts=None):
    global _open_trade_data_refresh_last_ts

    if not _tmf_cfg_bool("open_trade_data_refresh_enabled", True):
        return set()

    now_ts = time.time()
    interval = min(
        max(1.0, _tmf_cfg_float("open_trade_data_refresh_interval_secs", 60)),
        _TRADE_MGMT_CACHE_MAX_AGE_SECS - 1,
    )
    if now_ts - _open_trade_data_refresh_last_ts < interval:
        return set()
    _open_trade_data_refresh_last_ts = now_ts

    try:
        open_symbols, symbol_modes = _collect_open_trade_symbols(executor_contexts)
        if not open_symbols:
            return set()
        return _refresh_management_symbol_cache(
            open_symbols,
            symbol_modes,
            source="management_timer",
        )
    except Exception as exc:
        print(f"[MGMT REFRESH TIMER FAIL] reason={type(exc).__name__}: {exc}")
        return set()


def _should_log_trade_management_check(symbol, execution_mode, now_ts):
    key = (execution_mode, symbol)
    last_ts = _trade_mgmt_check_log_last.get(key, 0)
    if now_ts - last_ts >= _TRADE_MGMT_CHECK_THROTTLE_SECS:
        _trade_mgmt_check_log_last[key] = now_ts
        return True
    return False


def _trade_identity(t):
    return str(
        t.get("id")
        or t.get("smc_main_dedup_key")
        or t.get("research_dedup_key")
        or "|".join(str(t.get(k) or "") for k in ("symbol", "side", "entry_time", "signal_created_ts"))
    )


def _paper_quality_safe_float(value, default=None):
    try:
        if value in (None, ""):
            return default
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (TypeError, ValueError):
        return default


def _paper_quality_safe_json(value):
    if isinstance(value, dict):
        return {str(k): _paper_quality_safe_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_paper_quality_safe_json(v) for v in value]
    try:
        if hasattr(value, "item"):
            return _paper_quality_safe_json(value.item())
    except Exception:
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    return value


def _paper_quality_risk(t):
    entry = _paper_quality_safe_float(t.get("entry_real", t.get("entry")))
    sl = _paper_quality_safe_float(t.get("sl_init", t.get("sl_real", t.get("sl"))))
    if entry is None or sl is None:
        return None, "missing_entry_or_sl"
    risk = abs(entry - sl)
    if risk <= 0:
        return None, "invalid_zero_risk"
    return risk, None


def _paper_quality_planned_rr(t):
    risk, _reason = _paper_quality_risk(t)
    if risk is None:
        return None
    entry = _paper_quality_safe_float(t.get("entry_real", t.get("entry")))
    tp = _paper_quality_safe_float(t.get("tp"))
    if entry is None or tp is None:
        return None
    return abs(tp - entry) / risk


def _paper_quality_distance_pct(anchor, target):
    anchor = _paper_quality_safe_float(anchor)
    target = _paper_quality_safe_float(target)
    if anchor is None or target is None or anchor == 0:
        return None
    return abs(target - anchor) / abs(anchor) * 100.0


def _paper_quality_is_continuation(t):
    entry_type = str(t.get("entry_type") or "").upper()
    strategy = str(t.get("strategy_family") or "").lower()
    continuation_types = {
        "PAPER_SMC_MAIN",
        "CONFIRM_SMC_RESEARCH",
        "EARLY_CONT",
        "SWING_RETEST",
        "CONFIRM",
    }
    return entry_type in continuation_types or strategy in {"paper_smc_main", "confirm_smc_research"}


def _paper_quality_regime_value(t):
    for field in (
        "market_regime_at_entry",
        "router_regime",
        "regime",
        "market_regime",
    ):
        value = t.get(field)
        if value not in (None, ""):
            return str(value)
    return "NO_ROUTER"


def _paper_quality_regime_flags(t, regime):
    regime_u = str(regime or "").upper()
    is_cont = _paper_quality_is_continuation(t)
    no_router = regime_u in ("", "NO_ROUTER", "UNKNOWN_ROUTER")
    chop = regime_u == "CHOP_NO_TRADE"
    range_mr = regime_u == "RANGE_MEAN_REVERSION"
    exhaustion_rev = regime_u == "EXHAUSTION_REVERSAL"
    unfavorable = is_cont and (chop or range_mr or exhaustion_rev)
    favorable = regime_u in ("TRENDING_CONTINUATION", "BREAKOUT_EXPANSION")
    return {
        "continuation_in_chop": bool(is_cont and chop),
        "continuation_in_range_mean_reversion": bool(is_cont and range_mr),
        "continuation_in_exhaustion_reversal": bool(is_cont and exhaustion_rev),
        "continuation_in_unfavorable_regime": bool(unfavorable),
        "trend_or_breakout_regime_at_entry": bool(favorable),
        "no_router_context": bool(no_router),
    }


def _paper_quality_router_scalar(value):
    value = _paper_quality_safe_json(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_paper_quality_router_scalar(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _paper_quality_router_scalar(v) for k, v in value.items()}
    return str(value)


def _paper_quality_router_context_from_row(row):
    if not isinstance(row, dict):
        return None
    regime = row.get("regime")
    if regime in (None, "", "NO_ROUTER"):
        return None
    context = {
        "market_regime_at_entry": regime,
        "market_regime_confidence": row.get("confidence"),
        "router_market_state": row.get("market_state"),
        "router_phase": row.get("phase"),
        "router_exhaustion": row.get("exhaustion"),
        "router_bos_confirmation": row.get("bos_confirmation"),
        "router_smc_bias": row.get("smc_bias"),
        "router_smc_zone": row.get("smc_zone"),
        "router_range_context": row.get("range_context"),
        "router_liquidity_sweep": row.get("liquidity_sweep"),
        "router_invalid_context": row.get("invalid_context"),
        "router_freshness_status": row.get("freshness_status"),
        "router_observed_at": row.get("observed_at") or row.get("collector_ts"),
        "router_scan_id": row.get("scan_id"),
        "router_trend_direction": row.get("trend_direction"),
        "router_trend_strength": row.get("trend_strength"),
        "router_impulse": row.get("impulse"),
        "router_liquidity_context": row.get("liquidity_context"),
        "router_reasons": row.get("reasons"),
        "router_conflict_flags": row.get("conflict_flags"),
    }
    return {k: _paper_quality_router_scalar(v) for k, v in context.items()}


def _paper_quality_capture_router_context(row):
    try:
        context = _paper_quality_router_context_from_row(row)
        if not context:
            return
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            return
        _paper_quality_router_context_by_symbol[symbol] = dict(context)
        if symbol == "BTCUSDT":
            _btc_m15_shadow_cache["BTCUSDT"] = dict(row)
    except Exception as exc:
        print(f"[PAPER QUALITY] router context capture failed: {exc}")


_BTC_M15_SHADOW_MAX_AGE_SECS = 3600  # accept up to 60-minute-old snapshot


def _btc_m15_bias_label(smc_bias):
    b = str(smc_bias or "").upper()
    if b == "BEARISH":
        return "BEARISH"
    if b == "BULLISH":
        return "BULLISH"
    if b in ("NEUTRAL", "MIXED"):
        return "NEUTRAL_OR_CHOP"
    return "UNKNOWN"


def _btc_m15_alignment_label(side, bias_label):
    s = str(side or "").upper()
    if bias_label == "UNKNOWN":
        return "BTC_BIAS_UNKNOWN"
    if bias_label == "NEUTRAL_OR_CHOP":
        return "BTC_BIAS_NEUTRAL"
    if (s == "LONG" and bias_label == "BULLISH") or (s == "SHORT" and bias_label == "BEARISH"):
        return "BTC_BIAS_ALIGNED"
    if (s == "LONG" and bias_label == "BEARISH") or (s == "SHORT" and bias_label == "BULLISH"):
        return "BTC_BIAS_COUNTER"
    return "BTC_BIAS_UNKNOWN"


def _btc_m15_bias_shadow_snapshot(side, now_ts=None):
    now_ts = now_ts or time.time()
    unknown = {
        "btc_m15_bias_shadow_version": "v1_log_only",
        "btc_m15_candle_ts": None,
        "btc_m15_smc_bias": None,
        "btc_m15_trend_direction": None,
        "btc_m15_trend_strength": None,
        "btc_m15_market_state": None,
        "btc_m15_regime": None,
        "btc_m15_phase": None,
        "btc_m15_source": "NONE",
        "btc_m15_snapshot_age_secs": None,
        "btc_m15_bias_label": "UNKNOWN",
        "btc_m15_alignment_label": "BTC_BIAS_UNKNOWN",
        "btc_h4_bias_raw": "NONE",
        "btc_h1_ema_raw": "NONE",
        "btc_h1_structure_ok": "NOT_AVAILABLE_V1",
        "btc_bias_metadata_version": "v2_raw_h1_h4_log_only",
    }
    try:
        row = _btc_m15_shadow_cache.get("BTCUSDT")
        if not row:
            return unknown
        observed_at = _paper_quality_safe_float(row.get("observed_at") or row.get("collector_ts"))
        if observed_at is None:
            return unknown
        age = now_ts - observed_at
        if age < 0 or age > _BTC_M15_SHADOW_MAX_AGE_SECS:
            return {**unknown, "btc_m15_snapshot_age_secs": round(age, 1), "btc_m15_source": "STALE"}
        smc_bias = row.get("smc_bias")
        bias_label = _btc_m15_bias_label(smc_bias)
        return {
            "btc_m15_bias_shadow_version": "v1_log_only",
            "btc_m15_candle_ts": row.get("candle_ts_15m"),
            "btc_m15_smc_bias": smc_bias,
            "btc_m15_trend_direction": row.get("trend_direction"),
            "btc_m15_trend_strength": row.get("trend_strength"),
            "btc_m15_market_state": row.get("market_state"),
            "btc_m15_regime": row.get("regime"),
            "btc_m15_phase": row.get("phase"),
            "btc_m15_source": "router_shadow_cache",
            "btc_m15_snapshot_age_secs": round(age, 1),
            "btc_m15_bias_label": bias_label,
            "btc_m15_alignment_label": _btc_m15_alignment_label(side, bias_label),
            "btc_h4_bias_raw": row.get("btc_h4_bias_raw", "NONE"),
            "btc_h1_ema_raw": row.get("btc_h1_ema_raw", "NONE"),
            "btc_h1_structure_ok": row.get("btc_h1_structure_ok", "NOT_AVAILABLE_V1"),
            "btc_bias_metadata_version": "v2_raw_h1_h4_log_only",
        }
    except Exception:
        return unknown


def _btc_m15_bias_shadow_write(dedicated_row):
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "paper_smc_research_btc_m15_bias_shadow.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(dedicated_row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[BTC M15 BIAS SHADOW] log failed: {exc}")


def _btc_mtf_combined_labels(m5_align, m15_align, h1_align):
    labels = [m5_align, m15_align, h1_align]
    known = [l for l in labels if l != "BTC_BIAS_UNKNOWN"]
    aligned = [l for l in labels if l == "BTC_BIAS_ALIGNED"]
    counter = [l for l in labels if l == "BTC_BIAS_COUNTER"]
    neutral = [l for l in labels if l == "BTC_BIAS_NEUTRAL"]
    if not known:
        summary = "UNKNOWN"
    elif all(l == "BTC_BIAS_ALIGNED" for l in labels):
        summary = "ALL_ALIGNED"
    elif len(neutral) == len(known):
        summary = "ALL_NEUTRAL"
    elif counter:
        summary = "COUNTER_PRESENT"
    elif m5_align == "BTC_BIAS_ALIGNED" and m15_align == "BTC_BIAS_ALIGNED":
        summary = "M5_M15_ALIGNED"
    elif m15_align == "BTC_BIAS_ALIGNED" and h1_align == "BTC_BIAS_ALIGNED":
        summary = "M15_H1_ALIGNED"
    else:
        summary = "MIXED"
    return {
        "btc_mtf_all_aligned": m5_align == "BTC_BIAS_ALIGNED" and m15_align == "BTC_BIAS_ALIGNED" and h1_align == "BTC_BIAS_ALIGNED",
        "btc_mtf_m5_m15_aligned": m5_align == "BTC_BIAS_ALIGNED" and m15_align == "BTC_BIAS_ALIGNED",
        "btc_mtf_m15_h1_aligned": m15_align == "BTC_BIAS_ALIGNED" and h1_align == "BTC_BIAS_ALIGNED",
        "btc_mtf_any_counter": bool(counter),
        "btc_mtf_known_count": len(known),
        "btc_mtf_aligned_count": len(aligned),
        "btc_mtf_counter_count": len(counter),
        "btc_mtf_neutral_count": len(neutral),
        "btc_mtf_summary_label": summary,
    }


def _btc_mtf_bias_shadow_snapshot(side, now_ts=None):
    now_ts = now_ts or time.time()
    _U = "BTC_BIAS_UNKNOWN"

    def _tf_unknown(prefix):
        return {
            f"btc_{prefix}_bias_shadow_version": "v1_log_only",
            f"btc_{prefix}_candle_ts": None,
            f"btc_{prefix}_smc_bias": None,
            f"btc_{prefix}_trend_direction": None,
            f"btc_{prefix}_trend_strength": None,
            f"btc_{prefix}_market_state": None,
            f"btc_{prefix}_regime": None,
            f"btc_{prefix}_phase": None,
            f"btc_{prefix}_source": "NONE",
            f"btc_{prefix}_snapshot_age_secs": None,
            f"btc_{prefix}_bias_label": "UNKNOWN",
            f"btc_{prefix}_alignment_label": _U,
        }

    unknown_all = {
        **_tf_unknown("m5"),
        **_tf_unknown("m15"),
        **_tf_unknown("h1"),
        **_btc_mtf_combined_labels(_U, _U, _U),
        "btc_h4_bias_raw": "NONE",
        "btc_h1_ema_raw": "NONE",
        "btc_h1_structure_ok": "NOT_AVAILABLE_V1",
        "btc_bias_metadata_version": "v2_raw_h1_h4_log_only",
    }
    try:
        row = _btc_m15_shadow_cache.get("BTCUSDT")
        if not row:
            return unknown_all
        observed_at = _paper_quality_safe_float(row.get("observed_at") or row.get("collector_ts"))
        if observed_at is None:
            return unknown_all
        row_age = now_ts - observed_at
        if row_age < 0 or row_age > _BTC_M15_SHADOW_MAX_AGE_SECS:
            return {
                **_tf_unknown("m5"), **_tf_unknown("m15"), **_tf_unknown("h1"),
                "btc_m5_source": "STALE", "btc_m15_source": "STALE", "btc_h1_source": "STALE",
                **_btc_mtf_combined_labels(_U, _U, _U),
                "btc_h4_bias_raw": "NONE",
                "btc_h1_ema_raw": "NONE",
                "btc_h1_structure_ok": "NOT_AVAILABLE_V1",
                "btc_bias_metadata_version": "v2_raw_h1_h4_log_only",
            }
        smc_bias = row.get("smc_bias")
        trend_direction = row.get("trend_direction")
        trend_strength = row.get("trend_strength")
        market_state = row.get("market_state")
        regime = row.get("regime")
        phase = row.get("phase")
        candle_ts_5m = _paper_quality_safe_float(row.get("candle_ts_5m"))
        candle_ts_15m = _paper_quality_safe_float(row.get("candle_ts_15m"))
        h1_candle_ts = int(candle_ts_15m // 3600) * 3600 if candle_ts_15m else None
        bias_label = _btc_m15_bias_label(smc_bias)
        alignment = _btc_m15_alignment_label(side, bias_label)
        m5_age = round(now_ts - candle_ts_5m, 1) if candle_ts_5m else None
        m15_age = round(now_ts - candle_ts_15m, 1) if candle_ts_15m else None
        h1_age = round(now_ts - h1_candle_ts, 1) if h1_candle_ts else None
        shared = {
            "smc_bias": smc_bias, "trend_direction": trend_direction,
            "trend_strength": trend_strength, "market_state": market_state,
            "regime": regime, "phase": phase,
        }
        m5 = {
            "btc_m5_bias_shadow_version": "v1_log_only",
            "btc_m5_candle_ts": candle_ts_5m,
            "btc_m5_smc_bias": shared["smc_bias"],
            "btc_m5_trend_direction": shared["trend_direction"],
            "btc_m5_trend_strength": shared["trend_strength"],
            "btc_m5_market_state": shared["market_state"],
            "btc_m5_regime": shared["regime"],
            "btc_m5_phase": shared["phase"],
            "btc_m5_source": "router_shadow_cache",
            "btc_m5_snapshot_age_secs": m5_age,
            "btc_m5_bias_label": bias_label,
            "btc_m5_alignment_label": alignment,
        }
        m15 = {
            "btc_m15_bias_shadow_version": "v1_log_only",
            "btc_m15_candle_ts": candle_ts_15m,
            "btc_m15_smc_bias": shared["smc_bias"],
            "btc_m15_trend_direction": shared["trend_direction"],
            "btc_m15_trend_strength": shared["trend_strength"],
            "btc_m15_market_state": shared["market_state"],
            "btc_m15_regime": shared["regime"],
            "btc_m15_phase": shared["phase"],
            "btc_m15_source": "router_shadow_cache",
            "btc_m15_snapshot_age_secs": m15_age,
            "btc_m15_bias_label": bias_label,
            "btc_m15_alignment_label": alignment,
        }
        h1 = {
            "btc_h1_bias_shadow_version": "v1_log_only",
            "btc_h1_candle_ts": h1_candle_ts,
            "btc_h1_smc_bias": shared["smc_bias"],
            "btc_h1_trend_direction": shared["trend_direction"],
            "btc_h1_trend_strength": shared["trend_strength"],
            "btc_h1_market_state": shared["market_state"],
            "btc_h1_regime": shared["regime"],
            "btc_h1_phase": shared["phase"],
            "btc_h1_source": "router_shadow_cache_h1_derived",
            "btc_h1_snapshot_age_secs": h1_age,
            "btc_h1_bias_label": bias_label,
            "btc_h1_alignment_label": alignment,
        }
        return {
            **m5, **m15, **h1,
            **_btc_mtf_combined_labels(alignment, alignment, alignment),
            "btc_h4_bias_raw": row.get("btc_h4_bias_raw", "NONE"),
            "btc_h1_ema_raw": row.get("btc_h1_ema_raw", "NONE"),
            "btc_h1_structure_ok": row.get("btc_h1_structure_ok", "NOT_AVAILABLE_V1"),
            "btc_bias_metadata_version": "v2_raw_h1_h4_log_only",
        }
    except Exception:
        return unknown_all


def _btc_mtf_bias_shadow_write(dedicated_row):
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "paper_smc_research_btc_mtf_bias_shadow.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(dedicated_row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[BTC MTF BIAS SHADOW] log failed: {exc}")


def get_paper_quality_router_context_snapshot(symbol, now_ts=None):
    try:
        symbol = str(symbol or "").strip().upper()
        if not symbol:
            return None
        context = _paper_quality_router_context_by_symbol.get(symbol)
        if not context:
            return None
        now_ts = now_ts or time.time()
        observed_at = _paper_quality_safe_float(context.get("router_observed_at"))
        if observed_at is None:
            return None
        if observed_at > now_ts + 1.0:
            return None
        max_age = max(1.0, _paper_quality_safe_float(config.get("signal_max_age_secs"), 180.0) or 180.0)
        age_sec = now_ts - observed_at
        if age_sec > max_age:
            return None
        snapshot = dict(context)
        regime = _paper_quality_regime_value(snapshot)
        snapshot.setdefault("router_regime", regime)
        snapshot.setdefault("router_regime_source", "market_regime_router_shadow_scan")
        snapshot.setdefault("router_regime_observed_at", observed_at)
        snapshot.setdefault("router_regime_age_sec", age_sec)
        snapshot.setdefault("router_regime_stale", False)
        return {k: _paper_quality_router_scalar(v) for k, v in snapshot.items()}
    except Exception as exc:
        print(f"[PAPER QUALITY] router context snapshot failed: {exc}")
        return None


def _paper_quality_attach_router_context(t, now_ts=None):
    try:
        symbol = str(t.get("symbol") or "").strip().upper()
        if not symbol:
            return False
        context = _paper_quality_router_context_by_symbol.get(symbol)
        if not context:
            return False
        now_ts = now_ts or time.time()
        observed_at = _paper_quality_safe_float(context.get("router_observed_at"))
        if observed_at is None:
            return False
        if observed_at > now_ts + 1.0:
            return False
        max_age = max(1.0, _paper_quality_safe_float(config.get("signal_max_age_secs"), 180.0) or 180.0)
        if now_ts - observed_at > max_age:
            return False
        trend_direction = str(context.get("router_trend_direction") or "").upper()
        side = str(t.get("side") or "").upper()
        t["router_side_match"] = bool(not trend_direction or not side or trend_direction == side)
        for key, value in context.items():
            t[key] = _paper_quality_router_scalar(value)
        regime = _paper_quality_regime_value(t)
        for key, value in _paper_quality_regime_flags(t, regime).items():
            t[key] = value
        return True
    except Exception as exc:
        print(f"[PAPER QUALITY] router context attach failed: {exc}")
        return False


def _paper_quality_init_trade(t):
    try:
        entry = _paper_quality_safe_float(t.get("entry_real", t.get("entry")))
        sl = _paper_quality_safe_float(t.get("sl_init", t.get("sl_real", t.get("sl"))))
        tp = _paper_quality_safe_float(t.get("tp"))
        risk, risk_reason = _paper_quality_risk(t)
        regime = _paper_quality_regime_value(t)
        t.setdefault("paper_quality_schema_version", "v0.1")
        t.setdefault("planned_rr", _paper_quality_planned_rr(t))
        t.setdefault("mae_r", None if risk is None else 0.0)
        t.setdefault("mae_r_unavailable_reason", risk_reason)
        t.setdefault("max_favorable_price", entry)
        t.setdefault("max_adverse_price", entry)
        t.setdefault("time_to_max_mfe_secs", None)
        t.setdefault("time_to_max_mae_secs", None)
        t.setdefault("bars_to_max_mfe", None)
        t.setdefault("bars_to_max_mae", None)
        t.setdefault("paper_quality_bars_observed", 0)
        t.setdefault("reached_0_3r", False)
        t.setdefault("reached_0_5r", False)
        t.setdefault("reached_0_75r", False)
        t.setdefault("reached_1r", False)
        t.setdefault("reached_1_5r", False)
        t.setdefault("reached_2r", False)
        t.setdefault("sl_before_0_5r", False)
        t.setdefault("sl_before_1r", False)
        t.setdefault("entry_sl_distance_pct", _paper_quality_distance_pct(entry, sl))
        t.setdefault("entry_tp_distance_pct", _paper_quality_distance_pct(entry, tp))
        t.setdefault("risk_distance_pct", t.get("entry_sl_distance_pct"))
        t.setdefault("tp_distance_pct", t.get("entry_tp_distance_pct"))
        t.setdefault("rr_artificial_flag", None)
        t.setdefault("geometry_status", t.get("geometry_status") or t.get("compressed_geometry_status"))
        t.setdefault("market_regime_at_entry", regime)
        t.setdefault("market_regime_confidence", t.get("market_regime_confidence") or t.get("router_confidence"))
        for key, value in _paper_quality_regime_flags(t, regime).items():
            t.setdefault(key, value)
    except Exception as exc:
        print(f"[PAPER QUALITY] init failed: {exc}")


def _paper_quality_update_excursion(t, high, low, now_ts=None):
    try:
        risk, risk_reason = _paper_quality_risk(t)
        if risk is None:
            t["mae_r"] = None
            t["mae_r_unavailable_reason"] = risk_reason
            return
        now_ts = now_ts or time.time()
        entry = _paper_quality_safe_float(t.get("entry_real", t.get("entry")))
        if entry is None:
            t["mae_r"] = None
            t["mae_r_unavailable_reason"] = "missing_entry"
            return
        high = _paper_quality_safe_float(high)
        low = _paper_quality_safe_float(low)
        if high is None or low is None:
            return
        t["paper_quality_bars_observed"] = int(t.get("paper_quality_bars_observed") or 0) + 1
        if t.get("side") == "LONG":
            favorable_price = high
            adverse_price = low
            favorable_r = max(0.0, (high - entry) / risk)
            adverse_r = max(0.0, (entry - low) / risk)
            better_favorable = (
                t.get("max_favorable_price") in (None, "")
                or favorable_price > _paper_quality_safe_float(t.get("max_favorable_price"), entry)
            )
            worse_adverse = (
                t.get("max_adverse_price") in (None, "")
                or adverse_price < _paper_quality_safe_float(t.get("max_adverse_price"), entry)
            )
        else:
            favorable_price = low
            adverse_price = high
            favorable_r = max(0.0, (entry - low) / risk)
            adverse_r = max(0.0, (high - entry) / risk)
            better_favorable = (
                t.get("max_favorable_price") in (None, "")
                or favorable_price < _paper_quality_safe_float(t.get("max_favorable_price"), entry)
            )
            worse_adverse = (
                t.get("max_adverse_price") in (None, "")
                or adverse_price > _paper_quality_safe_float(t.get("max_adverse_price"), entry)
            )
        if better_favorable:
            t["max_favorable_price"] = favorable_price
            open_ts = _paper_quality_safe_float(t.get("time") or t.get("entry_time") or t.get("signal_created_ts"))
            t["time_to_max_mfe_secs"] = round(now_ts - open_ts, 1) if open_ts and now_ts >= open_ts else None
            t["bars_to_max_mfe"] = t.get("paper_quality_bars_observed")
        if worse_adverse:
            t["max_adverse_price"] = adverse_price
            open_ts = _paper_quality_safe_float(t.get("time") or t.get("entry_time") or t.get("signal_created_ts"))
            t["time_to_max_mae_secs"] = round(now_ts - open_ts, 1) if open_ts and now_ts >= open_ts else None
            t["bars_to_max_mae"] = t.get("paper_quality_bars_observed")
        t["mae_r"] = max(_paper_quality_safe_float(t.get("mae_r"), 0.0) or 0.0, adverse_r)
        for threshold, field in (
            (0.3, "reached_0_3r"),
            (0.5, "reached_0_5r"),
            (0.75, "reached_0_75r"),
            (1.0, "reached_1r"),
            (1.5, "reached_1_5r"),
            (2.0, "reached_2r"),
        ):
            if favorable_r >= threshold:
                t[field] = True
    except Exception as exc:
        print(f"[PAPER QUALITY] excursion update failed: {exc}")


def _paper_quality_observation_row(t, event_type):
    _paper_quality_init_trade(t)
    regime = _paper_quality_regime_value(t)
    flags = _paper_quality_regime_flags(t, regime)
    entry = t.get("entry_real", t.get("entry"))
    sl = t.get("sl_init", t.get("sl_real", t.get("sl")))
    tp = t.get("tp")
    row = {
        "schema_version": "v0.1",
        "event_type": event_type,
        "observed_at": time.time(),
        "trade_id": t.get("id") or t.get("trade_id"),
        "symbol": t.get("symbol"),
        "side": t.get("side"),
        "entry_type": t.get("entry_type"),
        "strategy": t.get("strategy_family") or t.get("strategy"),
        "phase": t.get("phase"),
        "opened_at": t.get("time") or t.get("entry_time"),
        "signal_created_ts": t.get("signal_created_ts"),
        "closed_at": t.get("close_time") if event_type == "CLOSED" else None,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "planned_rr": t.get("planned_rr", _paper_quality_planned_rr(t)),
        "realized_r": t.get("rr_real") if event_type == "CLOSED" else None,
        "exit_type": t.get("exit_type") if event_type == "CLOSED" else None,
        "mfe_r": t.get("max_profit_r"),
        "mae_r": t.get("mae_r"),
        "mae_r_unavailable_reason": t.get("mae_r_unavailable_reason"),
        "max_favorable_price": t.get("max_favorable_price"),
        "max_adverse_price": t.get("max_adverse_price"),
        "reached_0_3r": bool(t.get("reached_0_3r")),
        "reached_0_5r": bool(t.get("reached_0_5r")),
        "reached_0_75r": bool(t.get("reached_0_75r")),
        "reached_1r": bool(t.get("reached_1r")),
        "reached_1_5r": bool(t.get("reached_1_5r")),
        "reached_2r": bool(t.get("reached_2r")),
        "sl_before_0_5r": bool(t.get("sl_before_0_5r")),
        "sl_before_1r": bool(t.get("sl_before_1r")),
        "time_to_max_mfe_secs": t.get("time_to_max_mfe_secs"),
        "time_to_max_mae_secs": t.get("time_to_max_mae_secs"),
        "bars_to_max_mfe": t.get("bars_to_max_mfe"),
        "bars_to_max_mae": t.get("bars_to_max_mae"),
        "entry_sl_distance_pct": t.get("entry_sl_distance_pct", _paper_quality_distance_pct(entry, sl)),
        "entry_tp_distance_pct": t.get("entry_tp_distance_pct", _paper_quality_distance_pct(entry, tp)),
        "risk_distance_pct": t.get("risk_distance_pct"),
        "tp_distance_pct": t.get("tp_distance_pct"),
        "rr_artificial_flag": t.get("rr_artificial_flag"),
        "geometry_status": t.get("geometry_status"),
        "market_regime_at_entry": regime,
        "market_regime_confidence": t.get("market_regime_confidence"),
        "router_market_state": t.get("router_market_state") or t.get("market_state"),
        "router_phase": t.get("router_phase") or t.get("phase"),
        "router_exhaustion": t.get("router_exhaustion") or t.get("exhaustion_cls"),
        "router_bos_confirmation": t.get("router_bos_confirmation") or t.get("bos_type"),
        "router_smc_bias": t.get("router_smc_bias") or t.get("smc_bias"),
        "router_range_context": t.get("router_range_context") or t.get("range_context"),
        "router_liquidity_sweep": t.get("router_liquidity_sweep") or t.get("liquidity_sweep"),
        "router_invalid_context": t.get("router_invalid_context") or t.get("invalid_context"),
        "router_freshness_status": t.get("router_freshness_status"),
        "router_observed_at": t.get("router_observed_at"),
        "router_scan_id": t.get("router_scan_id"),
        "candidate_type": t.get("candidate_type"),
        "source_reason": t.get("source_reason") or t.get("original_reason"),
        "structural_decision_shadow": t.get("structural_decision_shadow"),
        "bos_quality": t.get("bos_quality"),
        "weak_structure_extended": t.get("weak_structure_extended"),
        "weak_structure_extended_reason": t.get("weak_structure_extended_reason"),
        "specific_combo_block_match": t.get("specific_combo_block_match"),
        "displacement_quality": t.get("displacement_quality"),
        "volume_confirmation": t.get("volume_confirmation"),
        "dow_phase": t.get("dow_phase"),
        "dow_trend_context": t.get("dow_trend_context"),
        "smc_zone": t.get("smc_zone"),
        "smc_bias": t.get("smc_bias"),
        "range_context": t.get("range_context"),
        "liquidity_sweep": t.get("liquidity_sweep"),
        "liquidity_context": t.get("liquidity_context"),
        "invalid_context": t.get("invalid_context"),
    }
    row.update(flags)
    return _paper_quality_safe_json(row)


def _paper_quality_write_observation(t, event_type, exec_mode):
    if exec_mode != "paper":
        return
    try:
        log_dir = os.path.dirname(os.path.abspath(_PAPER_TRADE_QUALITY_LOG))
        os.makedirs(log_dir, exist_ok=True)
        row = _paper_quality_observation_row(t, event_type)
        with open(_PAPER_TRADE_QUALITY_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str) + "\n")
    except Exception as exc:
        print(f"[PAPER QUALITY] observation log failed: {exc}")


def _sl_r_from_trade(t):
    try:
        entry = float(t.get("entry_real") or t.get("entry"))
        sl = float(t.get("sl"))
        sl_init = float(t.get("sl_init") or t.get("sl_real"))
        initial_risk = abs(entry - sl_init)
        if initial_risk <= 0:
            return None
        return abs(sl - entry) / initial_risk
    except (TypeError, ValueError):
        return None


def _send_trail_update_telegram(t, exec_mode, mode_prefix, sl_r, management_context=None):
    label = engine_label(t, live_mode=(exec_mode == "live"))
    sl_r_text = "n/a" if sl_r is None else round(sl_r, 2)
    _trail_dedup_key = telegram_dedup.build_key(
        "trail", t.get("id"), t.get("symbol"), t.get("side"), round(t.get("sl", 0), 8)
    )
    if exec_mode == "paper":
        msg = (
            f"🔁 {t['symbol']} {t.get('side', '')} | {paper_engine_name(t)} TRAIL\n"
            f"SL → {round(t['sl'], 6)} | {sl_r_text}R"
        )
    else:
        msg = (
            f"🔁 {label} • TRAIL UPDATE\n"
            f"{t['symbol']} {t.get('side', '')}\n"
            f"SL → {round(t['sl'], 6)} ({sl_r_text}R)"
        )
    if exec_mode != "paper":
        _send_management_telegram(
            t,
            msg,
            "trail_sl_update",
            mode_prefix,
            exec_mode,
            category="trailing",
            management_context=management_context,
        )
        return

    now = time.time()
    throttle_secs = max(0, int(config.get("telegram_paper_trail_update_throttle_secs", 300)))
    min_r_step = max(0.0, float(config.get("telegram_paper_trail_update_min_r_step", 0.2)))
    key = _trade_identity(t)
    last = _paper_trail_update_last_sent.get(key)
    should_send = last is None
    if last is not None:
        last_ts = float(last.get("ts", 0))
        last_r = last.get("sl_r")
        r_step = abs(sl_r - last_r) if last_r is not None and sl_r is not None else None
        should_send = (now - last_ts >= throttle_secs) or (r_step is not None and r_step >= min_r_step)
    if not should_send:
        print(
            f"[PAPER TRAIL TELEGRAM THROTTLED] {t.get('symbol')} "
            f"throttle={throttle_secs}s min_r_step={min_r_step}"
        )
        _send_management_telegram(
            t,
            msg,
            "trail_sl_update",
            mode_prefix,
            exec_mode,
            category="trailing",
            management_context=management_context,
            attempt_send=False,
            suppressed=True,
            suppress_reason="paper_trail_throttle",
            throttle_rule=f"{throttle_secs}s_or_{min_r_step}R",
        )
        return
    _paper_trail_update_last_sent[key] = {"ts": now, "sl_r": sl_r}
    _send_management_telegram(
        t,
        msg,
        "trail_sl_update",
        mode_prefix,
        exec_mode,
        category="trailing",
        management_context=management_context,
        throttle_rule=f"{throttle_secs}s_or_{min_r_step}R",
        dedup_key=_trail_dedup_key,
    )


def _paper_smc_research_trade_matches(t):
    if not isinstance(t, dict):
        return False
    return (
        t.get("entry_type") == "CONFIRM_SMC_RESEARCH"
        or t.get("strategy_family") == "confirm_smc_research"
        or t.get("research_source") == "confirm_structural_outcome_shadow"
    )


def _paper_smc_research_key(t):
    key = str(t.get("research_dedup_key") or "").strip()
    if key:
        return key
    return "|".join(
        str(t.get(field) or "")
        for field in ("id", "symbol", "side", "entry_type", "signal_created_ts", "entry")
    )


def _paper_smc_research_duration_secs(t, now_ts=None):
    now_ts = now_ts or time.time()
    open_ts = _safe_float_value(t.get("time") or t.get("entry_time") or t.get("signal_created_ts"), 0.0)
    close_ts = _safe_float_value(t.get("close_time"), 0.0)
    end_ts = close_ts if close_ts > 0 else now_ts
    if open_ts > 0 and end_ts >= open_ts:
        return round(end_ts - open_ts, 1)
    return None


def _paper_smc_research_current_r(t):
    for field in ("current_r", "unrealized_r", "r_now"):
        if field in t and t.get(field) not in (None, ""):
            return _safe_float_value(t.get(field), None)
    return None


def _paper_smc_research_event_row(t, status, now_ts=None):
    now_ts = now_ts or time.time()
    row = {
        "timestamp": format_vn_time(now_ts),
        "event_type": status,
        "symbol": t.get("symbol"),
        "side": t.get("side"),
        "entry_type": t.get("entry_type"),
        "entry": t.get("entry_real", t.get("entry")),
        "sl": t.get("sl_init", t.get("sl")),
        "research_dedup_key": _paper_smc_research_key(t),
        "research_join_key": _paper_smc_research_key(t),
        "trade_id": t.get("id") or t.get("trade_id"),
        "original_reason": t.get("original_reason"),
        "score_v2_structural_shadow": t.get("score_v2_structural_shadow"),
        "structural_decision_shadow": t.get("structural_decision_shadow"),
        "research_risk_tier": t.get("research_risk_tier"),
        "bos_quality": t.get("bos_quality"),
        "choch_quality": t.get("choch_quality"),
        "poi_location_quality": t.get("poi_location_quality"),
        "volume_confirmation": t.get("volume_confirmation"),
        "research_epoch": t.get("research_epoch"),
        "research_cap_target": t.get("research_cap_target"),
        "research_max_open_target": t.get("research_max_open_target"),
        "research_concurrency_epoch": t.get("research_concurrency_epoch"),
        "research_extension_reason": t.get("research_extension_reason"),
        "research_original_cap_completed": t.get("research_original_cap_completed"),
        "research_is_post_50": t.get("research_is_post_50"),
        "research_entry_shadow_label": t.get("research_entry_shadow_label"),
        "research_entry_shadow_version": t.get("research_entry_shadow_version"),
        "research_entry_timing_risk_class": t.get("research_entry_timing_risk_class"),
        "research_entry_timing_risk_reason": t.get("research_entry_timing_risk_reason"),
        "research_entry_timing_components": t.get("research_entry_timing_components"),
        "research_fallback_candidate": t.get("research_fallback_candidate"),
        "research_fallback_reason": t.get("research_fallback_reason"),
        "research_fallback_components": t.get("research_fallback_components"),
        "missing_fields": t.get("missing_fields") or [],
        "status": status,
    }
    duration_secs = _paper_smc_research_duration_secs(t, now_ts)
    if duration_secs is not None:
        row["duration_secs"] = duration_secs
    if status in ("RESEARCH_CLOSED", "RESEARCH_CLOSE_MISSING_CONTEXT"):
        row["close_price"] = t.get("exit_price")
        row["close_reason"] = t.get("close_reason") or t.get("exit_type")
        row["pnl"] = t.get("pnl")
        row["pnl_r"] = t.get("pnl_r")
        row["r_multiple"] = t.get("rr_real")
        row["mfe_r"] = t.get("max_profit_r")
        row["mae_r"] = t.get("mae_r")
    current_r = _paper_smc_research_current_r(t)
    if current_r is not None:
        row["current_r"] = current_r
    return {k: v for k, v in row.items() if v not in (None, "")}


def _paper_smc_research_min_lock_shadow_row(t, now_ts=None):
    if (
        not isinstance(t, dict)
        or str(t.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH"
        or t.get("research_epoch") != "v1_extend_200"
    ):
        return None

    actual_r = _safe_float_value(t.get("rr_real"), None)
    mfe_r = _safe_float_value(t.get("max_profit_r"), None)
    if actual_r is None or mfe_r is None:
        return None

    entry_price = _safe_float_value(t.get("entry_real") or t.get("entry"), None)
    initial_sl = _safe_float_value(
        t.get("sl_init") or t.get("sl_real") or t.get("sl"), None
    )
    actual_close_price = _safe_float_value(t.get("exit_price"), None)
    triggered_075 = mfe_r >= 0.75
    triggered_100 = mfe_r >= 1.0
    cf_075 = max(actual_r, 0.75) if triggered_075 else actual_r
    cf_100 = max(actual_r, 1.0) if triggered_100 else actual_r

    return {
        "ts": now_ts or t.get("close_time") or time.time(),
        "event": "PAPER_SMC_RESEARCH_MIN_LOCK_SHADOW",
        "symbol": t.get("symbol"),
        "side": t.get("side"),
        "trade_id": t.get("id") or t.get("trade_id"),
        "research_dedup_key": _paper_smc_research_key(t),
        "research_join_key": _paper_smc_research_key(t),
        "research_epoch": t.get("research_epoch"),
        "entry_type": t.get("entry_type"),
        "close_reason": t.get("close_reason") or t.get("exit_type"),
        "actual_realized_r": actual_r,
        "mfe_r": mfe_r,
        "mae_r": _safe_float_value(t.get("mae_r"), None),
        "entry_price": entry_price,
        "initial_sl": initial_sl,
        "final_sl": _safe_float_value(t.get("sl"), None),
        "tp": _safe_float_value(t.get("tp"), None),
        "actual_close_price": actual_close_price,
        "min_lock_075_triggered": triggered_075,
        "min_lock_100_triggered": triggered_100,
        "cf_realized_r_min_lock_075": cf_075,
        "cf_realized_r_min_lock_100": cf_100,
        "delta_r_min_lock_075_vs_actual": cf_075 - actual_r,
        "delta_r_min_lock_100_vs_actual": cf_100 - actual_r,
        "assumption": "STATIC_MFE_BOUND_NOT_FORWARD_REPLAY",
    }


def _paper_smc_research_min_lock_shadow_write(t, exec_mode):
    if exec_mode != "paper":
        return
    try:
        row = _paper_smc_research_min_lock_shadow_row(t)
        if row is None:
            return
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "paper_smc_research_min_lock_shadow.jsonl")
        with open(file_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH MIN LOCK SHADOW] log failed: {exc}")


def _paper_smc_research_sl_gap_calibration_shadow_row(t, now_ts=None):
    if (
        not isinstance(t, dict)
        or str(t.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH"
        or t.get("research_epoch") != "v1_extend_200"
        or str(t.get("close_reason") or t.get("exit_type") or "").upper() != "SL"
    ):
        return None

    actual_r = _safe_float_value(t.get("rr_real"), None)
    mae_r = _safe_float_value(t.get("mae_r"), None)
    configured_gap_r = _safe_float_value(t.get("sl_gap"), None)
    entry_price = _safe_float_value(t.get("entry_real") or t.get("entry"), None)
    initial_sl = _safe_float_value(
        t.get("sl_init") or t.get("sl_real") or t.get("sl"), None
    )
    actual_close_price = _safe_float_value(t.get("exit_price"), None)
    planned_risk_distance = None
    actual_loss_distance = None
    price_r = None
    if entry_price is not None and initial_sl is not None:
        planned_risk_distance = abs(entry_price - initial_sl)
        if planned_risk_distance <= 0:
            planned_risk_distance = None
    if entry_price is not None and actual_close_price is not None:
        actual_loss_distance = abs(actual_close_price - entry_price)
    side = str(t.get("side") or "").upper()
    if planned_risk_distance is not None and actual_close_price is not None:
        if side == "LONG":
            price_r = (actual_close_price - entry_price) / planned_risk_distance
        elif side == "SHORT":
            price_r = (entry_price - actual_close_price) / planned_risk_distance

    expected_sl_r = None if configured_gap_r is None else -(1.0 + configured_gap_r)
    gap_minus_mae_r = None
    if configured_gap_r is not None and mae_r is not None:
        gap_minus_mae_r = configured_gap_r - mae_r
    material_epsilon_r = 0.05
    possible_overcharge = False
    if (
        expected_sl_r is not None
        and mae_r is not None
        and actual_r is not None
    ):
        possible_overcharge = (
            abs(expected_sl_r) - mae_r > material_epsilon_r
            and abs(min(actual_r, 0.0)) - mae_r > material_epsilon_r
        )

    return {
        "ts": now_ts or t.get("close_time") or time.time(),
        "event": "PAPER_SMC_RESEARCH_SL_GAP_CALIBRATION_SHADOW",
        "symbol": t.get("symbol"),
        "side": t.get("side"),
        "trade_id": t.get("id") or t.get("trade_id"),
        "research_dedup_key": _paper_smc_research_key(t),
        "research_join_key": _paper_smc_research_key(t),
        "entry_type": t.get("entry_type"),
        "research_epoch": t.get("research_epoch"),
        "execution_tier": t.get("sl_gap_tier") or t.get("execution_tier"),
        "configured_sl_gap_r": configured_gap_r,
        "expected_sl_r_with_gap": expected_sl_r,
        "actual_realized_r": actual_r,
        "mae_r": mae_r,
        "entry_price": entry_price,
        "initial_sl": initial_sl,
        "actual_close_price": actual_close_price,
        "planned_risk_distance": planned_risk_distance,
        "actual_loss_distance": actual_loss_distance,
        "price_r": price_r,
        "is_be_stop": None if actual_r is None else abs(actual_r) < 1e-6,
        "gap_minus_mae_r": gap_minus_mae_r,
        "possible_overcharge": possible_overcharge,
        "assumption": "PAPER_SL_GAP_CALIBRATION_ONLY",
    }


def _paper_smc_research_sl_gap_calibration_shadow_write(t, exec_mode):
    if exec_mode != "paper":
        return
    try:
        row = _paper_smc_research_sl_gap_calibration_shadow_row(t)
        if row is None:
            return
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(
            log_dir, "paper_smc_research_sl_gap_calibration_shadow.jsonl"
        )
        with open(file_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH SL GAP CALIBRATION SHADOW] log failed: {exc}")


def _paper_smc_research_write(row):
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "paper_smc_research_lifecycle.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[PAPER SMC RESEARCH MONITOR] lifecycle log failed: {exc}")


def _telegram_latency_waterfall_write(row):
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "telegram_latency_waterfall.jsonl")
        with open(file_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[TELEGRAM LATENCY] waterfall log failed: {exc}")


def _telegram_management_latency_write(row):
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "telegram_management_latency_waterfall.jsonl")
        with open(file_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception as exc:
        print(f"[TELEGRAM MANAGEMENT LATENCY] waterfall log failed: {exc}")


def _send_management_telegram(
    t,
    msg,
    alert_type,
    prefix,
    execution_mode,
    category=None,
    management_context=None,
    attempt_send=True,
    suppressed=False,
    suppress_reason=None,
    throttle_rule=None,
    dedup_key=None,
):
    """Preserve the existing send behavior while appending timing metadata."""
    context = management_context if isinstance(management_context, dict) else {}
    event_detected_ts = _safe_float_value(context.get("event_detected_ts"), time.time())
    telegram_build_ts = time.time()
    telegram_send_start_ts = None
    telegram_send_done_ts = None
    metadata = {}
    send_exception = None

    if attempt_send:
        telegram_send_start_ts = time.time()
        try:
            if category is not None:
                metadata = send_telegram_gated(
                    msg,
                    prefix=prefix,
                    category=category,
                    return_metadata=True,
                    dedup_key=dedup_key,
                ) or {}
            else:
                metadata = send_telegram(
                    msg,
                    prefix=prefix,
                    return_metadata=True,
                    dedup_key=dedup_key,
                ) or {}
        except Exception as exc:
            send_exception = exc
        telegram_send_done_ts = time.time()

    try:
        price_meta = context.get("price_meta") if isinstance(context.get("price_meta"), dict) else {}
        update_started_ts = _safe_float_value(context.get("update_trades_started_ts"), None)
        candle_ts = _safe_float_value(price_meta.get("candle_time"), None)
        cache_age_secs = _safe_float_value(price_meta.get("cache_age_secs"), None)
        cache_max_age_secs = _safe_float_value(price_meta.get("cache_max_age_secs"), None)
        price_age_ms = (
            round(max(0.0, event_detected_ts - candle_ts) * 1000, 3)
            if candle_ts is not None
            else None
        )
        stale_feed_status = context.get("stale_feed_status")
        if stale_feed_status in (None, "") and cache_age_secs is not None:
            stale_feed_status = (
                "STALE"
                if cache_max_age_secs is not None and cache_age_secs > cache_max_age_secs
                else "FRESH"
            )

        effective_suppressed = bool(suppressed or metadata.get("suppressed"))
        effective_reason = suppress_reason or metadata.get("suppress_reason")
        telegram_error = (
            f"{type(send_exception).__name__}: {send_exception}"
            if send_exception is not None
            else metadata.get("error")
        )
        row = {
            "timestamp": format_vn_time(telegram_send_done_ts or telegram_build_ts),
            "event_type": "TELEGRAM_MANAGEMENT_ALERT_LATENCY",
            "alert_type": alert_type,
            "category": category,
            "entry_type": t.get("entry_type"),
            "strategy_family": t.get("strategy_family"),
            "symbol": t.get("symbol"),
            "side": t.get("side"),
            "trade_id": t.get("id") or t.get("trade_id"),
            "opened_trade_id": t.get("id") or t.get("trade_id"),
            "execution_mode": execution_mode,
            "event_detected_ts": event_detected_ts,
            "update_trades_started_ts": update_started_ts,
            "trade_loop_id": context.get("trade_loop_id"),
            "symbol_loop_index": context.get("symbol_loop_index"),
            "management_loop_age_ms": (
                round(max(0.0, event_detected_ts - update_started_ts) * 1000, 3)
                if update_started_ts is not None
                else None
            ),
            "price_age_ms": price_age_ms,
            "cache_age_secs": cache_age_secs,
            "stale_feed_status": stale_feed_status,
            "telegram_build_ts": telegram_build_ts,
            "telegram_send_start_ts": telegram_send_start_ts,
            "telegram_send_done_ts": telegram_send_done_ts,
            "telegram_send_ok": (
                False if send_exception is not None else metadata.get("ok")
            ),
            "telegram_error": telegram_error,
            "telegram_message_id": metadata.get("message_id"),
            "telegram_send_duration_sec": (
                round(telegram_send_done_ts - telegram_send_start_ts, 3)
                if telegram_send_start_ts is not None and telegram_send_done_ts is not None
                else None
            ),
            "event_to_telegram_send_done_sec": (
                round(telegram_send_done_ts - event_detected_ts, 3)
                if telegram_send_done_ts is not None
                else None
            ),
            "gated": metadata.get("gated", True) if category is not None else False,
            "suppressed": effective_suppressed,
            "suppress_reason": effective_reason,
            "throttle_rule": throttle_rule,
            "old_sl": context.get("old_sl"),
            "new_sl": context.get("new_sl"),
            "entry_price": t.get("entry_real") or t.get("entry"),
            "current_price_used_in_decision": context.get("current_price_used_in_decision"),
            "close_price": context.get("close_price"),
            "exit_reason": context.get("exit_reason") or t.get("close_reason") or t.get("exit_type"),
            "r_multiple": context.get("r_multiple", t.get("rr_real")),
            "message_price_source": context.get("message_price_source") or "unknown",
        }
        nullable_fields = (
            "update_trades_started_ts",
            "trade_loop_id",
            "symbol_loop_index",
            "management_loop_age_ms",
            "price_age_ms",
            "cache_age_secs",
            "stale_feed_status",
            "telegram_send_start_ts",
            "telegram_send_done_ts",
            "telegram_send_ok",
            "telegram_message_id",
            "telegram_send_duration_sec",
            "event_to_telegram_send_done_sec",
            "old_sl",
            "new_sl",
            "current_price_used_in_decision",
            "close_price",
            "r_multiple",
        )
        row["missing_fields"] = [field for field in nullable_fields if row.get(field) is None]
        _telegram_management_latency_write(row)
    except Exception as exc:
        print(f"[TELEGRAM MANAGEMENT LATENCY] instrumentation failed: {exc}")

    if send_exception is not None:
        raise send_exception
    return metadata if attempt_send else None


def _telegram_latency_explicit_mark(t, field):
    value = t.get(field)
    if value in (None, ""):
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if value == value and value > 0 else None


def _paper_smc_research_log_telegram_latency(
    t,
    telegram_build_ts,
    telegram_send_start_ts,
    telegram_send_done_ts,
    send_metadata=None,
    send_error=None,
):
    try:
        send_metadata = send_metadata if isinstance(send_metadata, dict) else {}
        open_trade_ts = _safe_float_value(
            t.get("open_trade_ts") or t.get("time") or t.get("entry_time"),
            None,
        )
        signal_created_ts = _safe_float_value(t.get("signal_created_ts"), None)
        signal_detected_ts = _safe_float_value(t.get("signal_detected_ts"), None)
        qualified_eval_ts = _safe_float_value(t.get("qualified_eval_ts"), None)
        dispatch_ts = _safe_float_value(t.get("dispatch_ts"), None)
        entry_price = _safe_float_value(t.get("entry_real") or t.get("entry"), None)
        sl_price = _safe_float_value(t.get("sl_init") or t.get("sl"), None)
        tp_price = _safe_float_value(t.get("tp"), None)
        sl_distance = (
            abs(entry_price - sl_price)
            if entry_price is not None and sl_price is not None
            else None
        )

        # Only explicit mark snapshots are accepted. Planned/candle entry is not a mark proxy.
        mark_price_at_open = _telegram_latency_explicit_mark(t, "mark_price_at_open")
        mark_price_at_send = _telegram_latency_explicit_mark(t, "mark_price_at_send")
        drift_abs = None
        drift_pct = None
        drift_r = None
        if mark_price_at_open is not None and mark_price_at_send is not None:
            drift_abs = abs(mark_price_at_send - mark_price_at_open)
            drift_pct = (drift_abs / mark_price_at_open) * 100
            if sl_distance is not None and sl_distance > 0:
                drift_r = drift_abs / sl_distance

        telegram_error = send_error or send_metadata.get("error")
        row = {
            "timestamp": format_vn_time(telegram_send_done_ts),
            "event_type": "TELEGRAM_OPEN_ALERT_LATENCY",
            "entry_type": t.get("entry_type"),
            "strategy_family": t.get("strategy_family"),
            "symbol": t.get("symbol"),
            "side": t.get("side"),
            "opened_trade_id": t.get("id"),
            "open_trade_ts": open_trade_ts,
            "telegram_build_ts": telegram_build_ts,
            "telegram_send_start_ts": telegram_send_start_ts,
            "telegram_send_done_ts": telegram_send_done_ts,
            "telegram_send_ok": False if send_error else send_metadata.get("ok"),
            "telegram_error": telegram_error,
            "telegram_message_id": send_metadata.get("message_id"),
            "entry_price_planned": entry_price,
            "sl_price": sl_price,
            "tp_price": tp_price,
            "sl_distance": sl_distance,
            "mark_price_at_open": mark_price_at_open,
            "mark_price_at_send": mark_price_at_send,
            "drift_open_to_send_abs": drift_abs,
            "drift_open_to_send_pct": drift_pct,
            "drift_open_to_send_r": drift_r,
            "signal_created_ts": signal_created_ts,
            "signal_detected_ts": signal_detected_ts,
            "qualified_eval_ts": qualified_eval_ts,
            "dispatch_ts": dispatch_ts,
            "candle_close_to_open_sec": (
                round(open_trade_ts - signal_created_ts, 3)
                if open_trade_ts is not None and signal_created_ts is not None
                else None
            ),
            "open_to_telegram_send_done_sec": (
                round(telegram_send_done_ts - open_trade_ts, 3)
                if open_trade_ts is not None
                else None
            ),
            "telegram_send_duration_sec": round(
                telegram_send_done_ts - telegram_send_start_ts,
                3,
            ),
        }
        nullable_fields = (
            "open_trade_ts",
            "telegram_build_ts",
            "telegram_send_start_ts",
            "telegram_send_done_ts",
            "telegram_send_ok",
            "telegram_message_id",
            "entry_price_planned",
            "sl_price",
            "tp_price",
            "sl_distance",
            "mark_price_at_open",
            "mark_price_at_send",
            "drift_open_to_send_abs",
            "drift_open_to_send_pct",
            "drift_open_to_send_r",
            "signal_created_ts",
            "signal_detected_ts",
            "qualified_eval_ts",
            "dispatch_ts",
            "candle_close_to_open_sec",
            "open_to_telegram_send_done_sec",
            "telegram_send_duration_sec",
        )
        row["missing_fields"] = [field for field in nullable_fields if row.get(field) is None]
        _telegram_latency_waterfall_write(row)
    except Exception as exc:
        print(f"[TELEGRAM LATENCY] instrumentation failed: {exc}")


def _paper_smc_research_notify_close(t, row, prefix=None, management_context=None):
    if not bool(config.get("paper_smc_research_notify_close", True)):
        _send_management_telegram(
            t,
            None,
            "confirm_smc_research_close",
            prefix,
            "paper",
            management_context=management_context,
            attempt_send=False,
            suppressed=True,
            suppress_reason="paper_smc_research_notify_close=False",
        )
        return
    msg = format_paper_smc_close(t, engine="SMC-RESEARCH", row=row)
    try:
        _send_management_telegram(
            t,
            msg,
            "confirm_smc_research_close",
            prefix,
            "paper",
            management_context=management_context,
            dedup_key=telegram_dedup.build_key(
                "research_close",
                _paper_smc_research_key(t),
                t.get("close_reason") or t.get("exit_type"),
            ),
        )
    except Exception:
        pass


def paper_smc_research_observe_open(t, ctx=None, monitor_only=False):
    exec_mode = getattr(ctx, "execution_mode", EXECUTION_MODE)
    if exec_mode != "paper" or not _paper_smc_research_trade_matches(t):
        return
    key = _paper_smc_research_key(t)
    if key in _paper_smc_research_open_seen:
        return
    _paper_smc_research_open_seen.add(key)
    row = _paper_smc_research_event_row(t, "RESEARCH_OPEN_OBSERVED")
    try:
        from signal_dispatcher import _research_entry_context
        row["entry_context"] = _research_entry_context(t, decision_ts=t.get("entry_time"))
    except Exception:
        pass
    _paper_smc_research_write(row)
    try:
        _now_ts = time.time()
        _btc_snap = _btc_m15_bias_shadow_snapshot(t.get("side"), now_ts=_now_ts)
        _btc_dedicated = {
            "timestamp": format_vn_time(_now_ts),
            "timestamp_unix": round(_now_ts, 3),
            "symbol": t.get("symbol"),
            "side": t.get("side"),
            "entry_type": t.get("entry_type"),
            "research_epoch": t.get("research_epoch"),
            "research_dedup_key": key,
            "research_join_key": key,
            "trade_id": t.get("id"),
            "signal_created_ts": t.get("signal_created_ts"),
            "open_ts": t.get("entry_time") or t.get("time"),
            "planned_rr": t.get("planned_rr") or t.get("rr"),
            "bos_quality": t.get("bos_quality"),
            "market_regime": t.get("market_regime"),
            "market_state": t.get("market_state"),
            "phase": t.get("phase"),
            "decision": "BTC_M15_BIAS_SHADOW_LOG_ONLY",
        }
        _btc_dedicated.update(_btc_snap)
        _btc_m15_bias_shadow_write({k: v for k, v in _btc_dedicated.items() if v not in (None, "")})
    except Exception as _btc_exc:
        print(f"[BTC M15 BIAS SHADOW] open capture failed: {_btc_exc}")
    try:
        _mtf_now_ts = time.time()
        _mtf_snap = _btc_mtf_bias_shadow_snapshot(t.get("side"), now_ts=_mtf_now_ts)
        _mtf_dedicated = {
            "timestamp": format_vn_time(_mtf_now_ts),
            "timestamp_unix": round(_mtf_now_ts, 3),
            "symbol": t.get("symbol"),
            "side": t.get("side"),
            "entry_type": t.get("entry_type"),
            "research_epoch": t.get("research_epoch"),
            "research_dedup_key": key,
            "research_join_key": key,
            "trade_id": t.get("id"),
            "signal_created_ts": t.get("signal_created_ts"),
            "open_ts": t.get("entry_time") or t.get("time"),
            "planned_rr": t.get("planned_rr") or t.get("rr"),
            "bos_quality": t.get("bos_quality"),
            "market_regime": t.get("market_regime"),
            "market_state": t.get("market_state"),
            "phase": t.get("phase"),
            "decision": "BTC_MTF_BIAS_SHADOW_LOG_ONLY",
            "btc_mtf_data_mode": "UNIFIED_ROUTER_BIAS_NOT_INDEPENDENT_TF",
        }
        _mtf_dedicated.update(_mtf_snap)
        _btc_mtf_bias_shadow_write({k: v for k, v in _mtf_dedicated.items() if v not in (None, "")})
    except Exception as _mtf_exc:
        print(f"[BTC MTF BIAS SHADOW] open capture failed: {_mtf_exc}")
    if monitor_only and bool(config.get("paper_smc_main_enabled", False)):
        print(
            f"[PAPER SMC RESEARCH LEGACY STILL-OPEN SUPPRESSED] "
            f"{row.get('symbol')} {row.get('side')} monitor-only jsonl_written"
        )
        return
    if bool(config.get("paper_smc_research_notify_open", False)):
        telegram_build_ts = time.time()
        side = str(row.get("side") or "").upper()
        side_icon = "🟢" if side == "LONG" else "🔴" if side == "SHORT" else "⚪"

        def _research_open_value(value):
            return "null" if value in (None, "") else str(value)

        tp = t.get("tp")
        planned_rr = t.get("planned_rr")
        if planned_rr in (None, ""):
            planned_rr = t.get("rr")
        msg_lines = [
            f"{side_icon} PAPER RESEARCH OPEN | {side or 'UNKNOWN'}",
            str(row.get("symbol") or "UNKNOWN"),
            (
                f"E {_research_open_value(row.get('entry'))} · "
                f"SL {_research_open_value(row.get('sl'))} · "
                f"TP {_research_open_value(tp)} · "
                f"RR {_research_open_value(planned_rr)}"
            ),
        ]
        epoch = t.get("research_epoch")
        cap = t.get("research_cap_target")
        if epoch not in (None, "") or cap not in (None, ""):
            msg_lines.append(
                f"Epoch {_research_open_value(epoch)} · cap {_research_open_value(cap)}"
            )
        msg = "\n".join(msg_lines)
        telegram_send_start_ts = time.time()
        send_metadata = None
        send_error = None
        try:
            send_metadata = send_telegram(
                msg,
                prefix=getattr(ctx, "mode_prefix", None),
                return_metadata=True,
                dedup_key=telegram_dedup.build_key(
                    "research_open", _paper_smc_research_key(t)
                ),
            )
        except Exception as exc:
            send_error = f"{type(exc).__name__}: {exc}"
        telegram_send_done_ts = time.time()
        _paper_smc_research_log_telegram_latency(
            t,
            telegram_build_ts,
            telegram_send_start_ts,
            telegram_send_done_ts,
            send_metadata=send_metadata,
            send_error=send_error,
        )


def paper_smc_research_observe_close(t, ctx=None, management_context=None):
    exec_mode = getattr(ctx, "execution_mode", EXECUTION_MODE)
    if exec_mode != "paper" or not _paper_smc_research_trade_matches(t):
        return
    key = _paper_smc_research_key(t)
    if key in _paper_smc_research_closed_seen:
        return
    _paper_smc_research_closed_seen.add(key)
    missing = []
    for field in ("symbol", "side", "entry_type", "entry", "sl", "exit_price", "rr_real"):
        has_fallback = (
            (field == "entry" and t.get("entry_real") not in (None, ""))
            or (field == "sl" and t.get("sl_init") not in (None, ""))
        )
        if t.get(field) in (None, "") and not has_fallback:
            missing.append(field)
    status = "RESEARCH_CLOSE_MISSING_CONTEXT" if missing else "RESEARCH_CLOSED"
    row = _paper_smc_research_event_row(t, status)
    if missing:
        row["missing_fields"] = missing
    _paper_smc_research_min_lock_shadow_write(t, exec_mode)
    _paper_smc_research_sl_gap_calibration_shadow_write(t, exec_mode)
    _paper_smc_research_write(row)
    _paper_smc_research_closed_since_summary.append(row)
    try:
        from signal_dispatcher import paper_boundary_guard_observe_close
        paper_boundary_guard_observe_close(t, ctx=ctx)
    except Exception:
        pass
    _paper_smc_research_notify_close(
        t,
        row,
        prefix=getattr(ctx, "mode_prefix", None),
        management_context=management_context,
    )


def paper_smc_research_monitor_cycle(ctx=None):
    global _paper_smc_research_summary_last_ts
    exec_mode = getattr(ctx, "execution_mode", EXECUTION_MODE)
    active_trades = getattr(ctx, "trades", trades)
    if exec_mode == "live":
        for t in active_trades:
            if not _paper_smc_research_trade_matches(t):
                continue
            key = _paper_smc_research_key(t)
            if key not in _paper_smc_research_live_warned:
                _paper_smc_research_live_warned.add(key)
                print(
                    "[PAPER SMC RESEARCH MONITOR WARNING] research-tagged trade "
                    f"found outside PAPER state symbol={t.get('symbol')} side={t.get('side')} "
                    "action=observe_only_no_mutation"
                )
        return
    if exec_mode != "paper":
        return

    now_ts = time.time()
    open_research = [
        t for t in active_trades
        if t.get("status", "OPEN") == "OPEN" and _paper_smc_research_trade_matches(t)
    ]
    for t in open_research:
        paper_smc_research_observe_open(t, ctx=ctx, monitor_only=True)

    try:
        from signal_dispatcher import paper_boundary_guard_maybe_summary
        paper_boundary_guard_maybe_summary(ctx=ctx)
    except Exception:
        pass

    if not bool(config.get("paper_smc_research_summary_enabled", True)):
        return
    interval = _safe_float_value(config.get("paper_smc_research_summary_interval_secs", 3600), 3600)
    if interval <= 0 or now_ts - _paper_smc_research_summary_last_ts < interval:
        return
    _paper_smc_research_summary_last_ts = now_ts

    max_open = config.get("paper_smc_research_max_open", 3)
    open_items = []
    for t in open_research:
        item = {
            "symbol": t.get("symbol"),
            "side": t.get("side"),
            "duration_secs": _paper_smc_research_duration_secs(t, now_ts),
        }
        current_r = _paper_smc_research_current_r(t)
        if current_r is not None:
            item["current_r"] = current_r
        open_items.append({k: v for k, v in item.items() if v not in (None, "")})

    row = {
        "timestamp": format_vn_time(now_ts),
        "event_type": "RESEARCH_STILL_OPEN_SUMMARY",
        "status": "RESEARCH_STILL_OPEN_SUMMARY",
        "open_count": len(open_research),
        "max_open_usage": f"{len(open_research)}/{max_open}",
        "open_trades": open_items,
        "closed_since_last_summary": list(_paper_smc_research_closed_since_summary),
    }
    _paper_smc_research_write(row)

    if bool(config.get("paper_smc_research_summary_enabled", True)):
        lines = [
            "[PAPER SMC RESEARCH SUMMARY]",
            f"Open: {len(open_research)}/{max_open}",
        ]
        for item in open_items:
            age_m = round((item.get("duration_secs") or 0) / 60, 1)
            r_text = f" | R: {item['current_r']}" if "current_r" in item else ""
            lines.append(f"{item.get('symbol')} {item.get('side')} age={age_m}m{r_text}")
        if _paper_smc_research_closed_since_summary:
            lines.append(f"Closed since last summary: {len(_paper_smc_research_closed_since_summary)}")
        try:
            send_telegram("\n".join(lines), prefix=getattr(ctx, "mode_prefix", None))
        except Exception:
            pass
    _paper_smc_research_closed_since_summary.clear()


def _print_signal_type_summary(signals):
    counts = {}
    for signal in signals:
        entry_type = signal.get("entry_type") or "UNKNOWN"
        counts[entry_type] = counts.get(entry_type, 0) + 1

    ordered = [f"{entry_type}={counts.pop(entry_type, 0)}" for entry_type in SIGNAL_TYPE_SUMMARY_ORDER]
    ordered.extend(f"{entry_type}={counts[entry_type]}" for entry_type in sorted(counts))
    print(f"[SIGNAL TYPES] {' '.join(ordered)}")


def _signal_type_counts(signals):
    counts = {entry_type: 0 for entry_type in SIGNAL_TYPE_SUMMARY_ORDER}
    for signal in signals or []:
        entry_type = signal.get("entry_type") or "UNKNOWN"
        counts[entry_type] = counts.get(entry_type, 0) + 1
    return counts


def _write_strategy_observability(signals):
    global _strategy_observability_scan_id

    _strategy_observability_scan_id += 1
    counts = _signal_type_counts(signals)
    counters = get_strategy_observability_counters() or {}
    row = {
        "timestamp": format_vn_time(time.time()),
        "scan_id": _strategy_observability_scan_id,
        "total_signals": len(signals or []),
        "CONFIRM": counts.get("CONFIRM", 0),
        "SWING_RETEST": counts.get("SWING_RETEST", 0),
        "REVERSAL_CONFIRM": counts.get("REVERSAL_CONFIRM", 0),
        "EARLY_CONT": counts.get("EARLY_CONT", 0),
        "REVERSAL_SHADOW": counters.get("REVERSAL_SHADOW", 0),
        "reversal_context_fail": counters.get("reversal_context_fail", 0),
        "reversal_market_state_gate_failure": counters.get("reversal_market_state_gate_failure", 0),
        "reversal_missing_extended_exhaustion": counters.get("reversal_missing_extended_exhaustion", 0),
        "reversal_missing_bos_near": counters.get("reversal_missing_bos_near", 0),
    }

    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    file = os.path.join(log_dir, "strategy_observability.csv")
    is_new = not os.path.exists(file)
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=STRATEGY_OBSERVABILITY_FIELDS)
        if is_new:
            w.writeheader()
        w.writerow(row)


def _scan_snapshot_safe_scalar(value):
    try:
        if value is None:
            return None
        if hasattr(value, "item"):
            return _scan_snapshot_safe_scalar(value.item())
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
    except Exception:
        return None
    return value


def _scan_snapshot_safe_float(value):
    value = _scan_snapshot_safe_scalar(value)
    if value is None:
        return None
    try:
        value = float(value)
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    except Exception:
        return None


def _scan_snapshot_epoch(value):
    value = _scan_snapshot_safe_scalar(value)
    if value is None:
        return None
    try:
        if hasattr(value, "timestamp"):
            return float(value.timestamp())
        ts = float(value)
        if ts > 10_000_000_000:
            ts = ts / 1000.0
        return ts
    except Exception:
        return None


def _scan_snapshot_last_value(df, column):
    try:
        if df is None or len(df) <= 0 or column not in df:
            return None
        return _scan_snapshot_safe_scalar(df[column].iloc[-1])
    except Exception:
        return None


def _scan_snapshot_last_ts(df):
    try:
        if df is None or len(df) <= 0:
            return None
        for column in ("time", "timestamp", "open_time", "close_time", "datetime", "date"):
            ts = _scan_snapshot_epoch(_scan_snapshot_last_value(df, column))
            if ts is not None:
                return ts
        idx_value = _scan_snapshot_safe_scalar(df.index[-1])
        return _scan_snapshot_epoch(idx_value)
    except Exception:
        return None


def _scan_snapshot_compact_m5(df5, window):
    try:
        if df5 is None or len(df5) <= 0:
            return None
        n = max(1, min(int(window or 10), 20))
        tail = df5.tail(n)
        highs, lows, closes, timestamps = [], [], [], []
        for idx, row in tail.iterrows():
            highs.append(_scan_snapshot_safe_float(row.get("high")))
            lows.append(_scan_snapshot_safe_float(row.get("low")))
            closes.append(_scan_snapshot_safe_float(row.get("close")))
            ts = None
            for column in ("time", "timestamp", "open_time", "close_time", "datetime", "date"):
                if column in tail.columns:
                    ts = _scan_snapshot_epoch(row.get(column))
                    if ts is not None:
                        break
            if ts is None:
                ts = _scan_snapshot_epoch(idx)
            timestamps.append(ts)
        return {
            "timestamps": timestamps,
            "highs": highs,
            "lows": lows,
            "closes": closes,
        }
    except Exception:
        return None


def _scan_snapshot_first_present(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _scan_snapshot_by_symbol(signals):
    grouped = {}
    for signal in signals or []:
        if not isinstance(signal, dict):
            continue
        symbol = signal.get("symbol")
        if not symbol:
            continue
        bucket = grouped.setdefault(symbol, {"candidate_count": 0, "top": None})
        bucket["candidate_count"] += 1
        top = bucket.get("top")
        signal_score = _scan_snapshot_safe_float(signal.get("score"))
        top_score = _scan_snapshot_safe_float(top.get("score")) if isinstance(top, dict) else None
        if top is None or (signal_score if signal_score is not None else float("-inf")) > (top_score if top_score is not None else float("-inf")):
            bucket["top"] = signal
    return grouped


def _append_scan_feature_snapshots(raw_data_map, signals, scan_id=None, observed_at=None):
    if not config.get("scan_feature_snapshot_enabled", True):
        return
    try:
        signal_groups = _scan_snapshot_by_symbol(signals)
        if not signal_groups:
            return
        max_per_scan = max(0, int(config.get("scan_feature_snapshot_max_per_scan", 100) or 100))
        if max_per_scan <= 0:
            return
        window = config.get("scan_feature_snapshot_candle_window", 10)
        path = config.get("scan_feature_snapshot_log_path", "logs/scan_feature_snapshots.jsonl")
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        written = 0
        observed = observed_at or time.time()
        with open(path, "a", encoding="utf-8") as f:
            for symbol, group in signal_groups.items():
                if written >= max_per_scan:
                    break
                frames = raw_data_map.get(symbol) if isinstance(raw_data_map, dict) else None
                if not frames:
                    continue
                df5 = frames[0] if len(frames) > 0 else None
                df15 = frames[1] if len(frames) > 1 else None
                if df5 is None or df15 is None:
                    continue
                top = group.get("top") or {}
                router = _paper_quality_router_context_by_symbol.get(symbol, {})
                m5_window = _scan_snapshot_compact_m5(df5, window)
                if not m5_window:
                    continue

                row = {
                    "event_type": "SCAN_FEATURE_SNAPSHOT",
                    "schema_version": "v0.1",
                    "observed_at": observed,
                    "scan_id": scan_id,
                    "symbol": symbol,
                    "timeframe_context": top.get("timeframe_context") or top.get("base_timeframe") or router.get("base_timeframe"),
                    "close": _scan_snapshot_safe_float(_scan_snapshot_last_value(df5, "close")),
                    "m5_last_close": _scan_snapshot_safe_float(_scan_snapshot_last_value(df5, "close")),
                    "m15_last_close": _scan_snapshot_safe_float(_scan_snapshot_last_value(df15, "close")),
                    "m5_last_ts": _scan_snapshot_last_ts(df5),
                    "m15_last_ts": _scan_snapshot_last_ts(df15),
                    "m5_recent": m5_window,
                    "market_regime": _scan_snapshot_first_present(top.get("market_regime_at_entry"), top.get("market_regime"), router.get("market_regime_at_entry")),
                    "router_regime": _scan_snapshot_first_present(top.get("router_regime"), router.get("router_regime"), router.get("market_regime_at_entry")),
                    "router_regime_source": _scan_snapshot_first_present(top.get("router_regime_source"), router.get("router_regime_source"), "market_regime_router_shadow_scan"),
                    "router_regime_confidence": _scan_snapshot_first_present(top.get("router_confidence"), top.get("market_regime_confidence"), router.get("market_regime_confidence")),
                    "router_reasons": router.get("router_reasons"),
                    "trend_direction": _scan_snapshot_first_present(top.get("trend_direction"), router.get("router_trend_direction")),
                    "range_context": _scan_snapshot_first_present(top.get("range_context"), router.get("router_range_context")),
                    "bos_quality": top.get("bos_quality"),
                    "bos_confirmation": _scan_snapshot_first_present(top.get("bos_confirmation"), router.get("router_bos_confirmation")),
                    "structural_decision_shadow": top.get("structural_decision_shadow"),
                    "weak_structure_extended": top.get("weak_structure_extended"),
                    "premium_discount": top.get("premium_discount"),
                    "displacement_quality": top.get("displacement_quality"),
                    "liquidity_context": _scan_snapshot_first_present(top.get("liquidity_context"), router.get("router_liquidity_context")),
                    "candidate_count": group.get("candidate_count", 0),
                    "top_candidate_entry_type": top.get("entry_type"),
                    "top_candidate_score": _scan_snapshot_safe_float(top.get("score")),
                    "data_quality": {
                        "has_df5": df5 is not None,
                        "has_df15": df15 is not None,
                        "m5_window_count": len(m5_window.get("closes") or []),
                        "has_router_context": bool(router),
                    },
                }
                f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str) + "\n")
                written += 1
    except Exception:
        try:
            import traceback as _scan_snapshot_traceback
            write_runtime_error("SCAN_FEATURE_SNAPSHOT/write", _scan_snapshot_traceback.format_exc())
        except Exception:
            pass

try:
    trades = load_open_trades()
except RuntimeError as _hydration_err:
    import sys
    print(str(_hydration_err))
    print("[CRITICAL] Cannot start bot safely — trade state corrupted. Exiting.")
    sys.exit(1)
print(f"[INIT] Loaded {len(trades)} trades")
entry_cooldown = {}
cooldown = {}
MAX_TOTAL_RISK = 0.05
MAX_RISK_PER_SYMBOL = 0.02
MAX_TRADES_PER_SYMBOL = 2
MAX_DRAWDOWN = 0.1
ENTRY_COOLDOWN = 300
LOSS_COOLDOWN = 600
SPREAD = 0.0005     # 0.05%
SLIPPAGE = 0.001    # 0.1%
SL_GAP_BY_TIER = {
    "TIER1": 0.002,
    "TIER2": 0.005,
    "TIER3": 0.015,
    "TIER4": 0.03,
}
SL_GAP_R_BY_TIER = {
    "TIER1": 0.10,
    "TIER2": 0.20,
    "TIER3": 0.30,
    "TIER4": 0.50,
}
SL_GAP_MAX_R = 0.50
MIN_HOLD_TIME = 90
history = []
_csv_last_check = 0
ACCOUNT_BALANCE = config["account_balance"]
trades_lock = threading.Lock()
live_pending_slots = 0
_sl_repair_cooldown: dict = {}
_SL_REPAIR_COOLDOWN_SECS = 600


def _resolve_exchange_executor(exec_mode: str):
    """Return the correct executor module for the given execution_mode."""
    if exec_mode == "live":
        from exchange import live_executor
        return live_executor
    from exchange import testnet_executor
    return testnet_executor


def _normalize_runtime_bos_type(value) -> str:
    raw = str(value or "").upper().strip()
    if raw.startswith("BOS:"):
        raw = raw.split(":", 1)[1]
    if raw.startswith("BOS_"):
        raw = raw.split("_", 1)[1]
    return raw


def _extract_runtime_bos_type(t: dict) -> str:
    bos_t = _normalize_runtime_bos_type(t.get("bos_type"))
    if bos_t:
        return bos_t
    return next(
        (
            _normalize_runtime_bos_type(r)
            for r in t.get("reason", [])
            if isinstance(r, str) and r.upper().startswith("BOS:")
        ),
        "",
    )


def _live_open_count(open_trades: list, exclude_trade: dict = None) -> int:
    # Count only bot-owned trades toward the live slot limit.
    # Manual positions on the same account have no entry in ctx.trades and
    # are never counted here.  Any ctx.trades entry that somehow lacks an
    # owner field (e.g. loaded from an older state file) defaults to "bot"
    # via normalize_trade_schema so the behaviour is backward-compatible.
    return sum(
        1 for x in open_trades
        if x is not exclude_trade
        and x.get("status", "OPEN") == "OPEN"
        and not x.get("quarantined")
        and x.get("owner", "bot") == "bot"
    )


def _get_max_live_trades() -> int:
    raw = config.get("max_live_trades", 3)
    try:
        max_live = int(raw)
    except (TypeError, ValueError):
        return 0
    return max(0, max_live)


def _live_pending_count(ctx=None) -> int:
    if ctx is not None:
        return max(0, int(getattr(ctx, "live_pending_slots", 0) or 0))
    return max(0, int(globals().get("live_pending_slots", 0) or 0))


def _live_slot_snapshot(open_trades: list, ctx=None, exclude_trade: dict = None) -> tuple:
    open_count = _live_open_count(open_trades, exclude_trade=exclude_trade)
    pending = _live_pending_count(ctx)
    return open_count, pending, open_count + pending


def _live_signal_fields(t: dict) -> tuple:
    return (
        t.get("symbol", ""),
        t.get("side", ""),
        t.get("strategy") or t.get("entry_type", ""),
    )


def _log_live_slot_decision(
    action: str,
    t: dict,
    max_live: int,
    open_count: int,
    pending: int,
    effective: int,
    exec_mode: str,
    reason: str = "",
) -> None:
    symbol, side, strategy = _live_signal_fields(t)
    print(
        f"[LIVE SLOT] action={action} symbol={symbol} side={side} "
        f"strategy={strategy} mode={exec_mode} max={max_live} "
        f"open={open_count} pending={pending} effective={effective} "
        f"reason={reason}"
    )


def _create_live_slot_reservation(ctx, open_trades: list, t: dict, exec_mode: str) -> bool:
    max_live = _get_max_live_trades()
    open_count, pending, effective = _live_slot_snapshot(open_trades, ctx)
    if effective >= max_live:
        _log_live_slot_decision(
            "reject",
            t,
            max_live,
            open_count,
            pending,
            effective,
            exec_mode,
            "live_max_open_trades_reached",
        )
        return False
    if ctx is not None:
        ctx.live_pending_slots = pending + 1
        new_pending = ctx.live_pending_slots
    else:
        globals()["live_pending_slots"] = pending + 1
        new_pending = globals()["live_pending_slots"]
    _log_live_slot_decision(
        "reservation_created",
        t,
        max_live,
        open_count,
        new_pending,
        open_count + new_pending,
        exec_mode,
        "live_slot_reserved",
    )
    return True


def _release_live_slot_reservation(ctx, open_trades: list, t: dict, exec_mode: str, reason: str) -> None:
    if ctx is not None:
        current = _live_pending_count(ctx)
        ctx.live_pending_slots = max(0, current - 1)
        pending = ctx.live_pending_slots
    else:
        current = _live_pending_count(None)
        globals()["live_pending_slots"] = max(0, current - 1)
        pending = globals()["live_pending_slots"]
    max_live = _get_max_live_trades()
    open_count = _live_open_count(open_trades)
    _log_live_slot_decision(
        "reservation_released",
        t,
        max_live,
        open_count,
        pending,
        open_count + pending,
        exec_mode,
        reason,
    )


def _consume_live_slot_reservation(ctx, open_trades: list, t: dict, exec_mode: str) -> None:
    if ctx is not None:
        current = _live_pending_count(ctx)
        ctx.live_pending_slots = max(0, current - 1)
        pending = ctx.live_pending_slots
    else:
        current = _live_pending_count(None)
        globals()["live_pending_slots"] = max(0, current - 1)
        pending = globals()["live_pending_slots"]
    max_live = _get_max_live_trades()
    open_count = _live_open_count(open_trades)
    _log_live_slot_decision(
        "reservation_converted",
        t,
        max_live,
        open_count,
        pending,
        open_count + pending,
        exec_mode,
        "consumed_by_open_trade",
    )


def _check_live_runtime_safety_gate(
    executor,
    t: dict,
    open_trades: list,
    risk_percent: float,
    current_portfolio_risk: float,
    leverage: int = None,
    exclude_trade: dict = None,
    ctx=None,
    post_insert: bool = False,
) -> tuple:
    # CONFIRM_SMC_RESEARCH trades bypass the CONFIRM safety gate entirely.
    # They route to a dedicated research gate that omits exhaustion_cls /
    # bos_type checks (which research trades do not carry).
    if t.get("entry_type") == "CONFIRM_SMC_RESEARCH":
        return executor.check_live_research_safety_gate(
            t, ctx=ctx, open_trades=open_trades,
        )

    open_count, pending, effective = _live_slot_snapshot(open_trades, ctx, exclude_trade=exclude_trade)
    max_live = _get_max_live_trades()
    if post_insert and effective > max_live:
        _log_live_slot_decision(
            "reject",
            t,
            max_live,
            open_count,
            pending,
            effective,
            t.get("execution_mode", ""),
            "live_runtime_safety_gate_reject",
        )
        return False, (
            f"live_runtime_safety_gate_reject max_live_trades={max_live} "
            f"open={open_count} pending={pending} effective={effective}"
        )
    concurrent_for_executor = max(0, effective - 1) if post_insert else effective
    return executor.check_live_safety_gate(
        symbol=t.get("symbol", ""),
        entry_type=t.get("entry_type", ""),
        exhaustion=t.get("exhaustion_cls", ""),
        bos_type=_extract_runtime_bos_type(t),
        concurrent_trades=concurrent_for_executor,
        risk_percent=risk_percent,
        current_portfolio_risk=current_portfolio_risk,
        leverage=leverage,
    )


def _get_live_config_float(key: str) -> float:
    raw = config.get(key)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{key} malformed in config.json: {raw!r}")
    if value <= 0:
        raise ValueError(f"{key} must be > 0, got {value}")
    return value


def apply_entry_spread(entry: float, side: str) -> float:
    if entry is None or entry <= 0:
        return entry
    if side == "LONG":
        return entry * (1 + SPREAD)
    if side == "SHORT":
        return entry * (1 - SPREAD)
    return entry


def get_sl_gap_for_tier(tier) -> float:
    tier_key = str(tier or "TIER3").upper()
    return SL_GAP_BY_TIER.get(tier_key, SL_GAP_BY_TIER["TIER3"])


def get_sl_gap_r_for_tier(tier) -> float:
    tier_key = str(tier or "TIER3").upper()
    return SL_GAP_R_BY_TIER.get(tier_key, SL_GAP_R_BY_TIER["TIER3"])


def _get_execution_tier(symbol: str) -> str:
    try:
        from exchange.execution_policy import get_symbol_tier
        return get_symbol_tier(symbol)
    except Exception:
        return "TIER3"


def apply_sl_gap_to_stop_fill(sl_price: float, side: str, gap: float, entry: float = None) -> float:
    if sl_price is None or sl_price <= 0 or gap <= 0:
        return sl_price
    if entry is not None and entry > 0:
        risk_distance = abs(entry - sl_price)
        if risk_distance > 0:
            gap_points = min(risk_distance * gap, risk_distance * SL_GAP_MAX_R)
            if side == "LONG":
                return sl_price - gap_points
            if side == "SHORT":
                return sl_price + gap_points
        return sl_price
    if side == "LONG":
        return sl_price * (1 - gap)
    if side == "SHORT":
        return sl_price * (1 + gap)
    return sl_price


def _quarantine_trade(t: dict, reason: str) -> bool:
    """
    Mark a trade as permanently quarantined with a standardized reason.

    Idempotent — safe to call multiple times.
    Returns True if this call newly quarantined the trade (caller should alert + save).
    Returns False if the trade was already quarantined (no action needed).

    Valid reasons:
      invalid_symbol                 — exchange does not recognise the symbol
      orphan_local_trade             — local trade has no matching exchange position
      unrecoverable_stop_failure     — SL repair failed; manual intervention required
      exchange_desync_unrecoverable  — exchange state cannot be reconciled
    """
    if t.get("quarantined") and t.get("repair_disabled"):
        return False
    t["quarantined"] = True
    t["repair_disabled"] = True
    t["quarantine_reason"] = reason
    t["quarantine_timestamp"] = time.time()
    return True


_LIVE_RECONCILE_PRESERVE_ALERT_TTL_SECS = {
    "exchange_position_open": 15 * 60,
    "exchange_state_uncertain": 15 * 60,
    "fresh_entry_under_300s": 5 * 60,
    "manual_or_uncertain_position": 15 * 60,
    "bot_ownership_recovered": 15 * 60,
}
_live_reconcile_preserve_last_sent = {}


def _send_live_reconcile_preserve_alert(
    symbol: str,
    side: str,
    reason: str,
    age_secs,
    exchange_position_status: str,
    prefix=None,
    issue_type: str = "missing_exchange_qty",
) -> bool:
    ttl_secs = _LIVE_RECONCILE_PRESERVE_ALERT_TTL_SECS.get(reason, 0)
    now = time.time()
    key = f"live_reconcile_preserve:{symbol}:{side}:{reason}:{issue_type}"
    if ttl_secs > 0:
        last_sent = _live_reconcile_preserve_last_sent.get(key, 0)
        if now - last_sent < ttl_secs:
            print(
                f"[LIVE RECONCILE] telegram suppressed key={key} "
                f"age={round(now - last_sent, 1)}s ttl={ttl_secs}s"
            )
            return False
        _live_reconcile_preserve_last_sent[key] = now

    age_line = ""
    if age_secs is not None:
        age_line = f"\nage_secs={round(age_secs, 1)}"
    send_telegram(
        f"[LIVE RECONCILE] {symbol} {side} {issue_type}\n"
        f"reason={reason}{age_line}\n"
        f"exchange_position_status={exchange_position_status}\n"
        f"action=local_state_preserved",
        prefix=prefix,
        channel="alerts",
    )
    return True


def _has_bot_client_id(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    return any(
        isinstance(row.get(field), str) and row.get(field).startswith("BOT_")
        for field in ("clientOrderId", "origClientOrderId", "clientAlgoId", "newClientOrderId")
    )


def _get_order_client_id(row: dict) -> str:
    if not isinstance(row, dict):
        return ""
    for field in ("clientOrderId", "origClientOrderId", "clientAlgoId", "newClientOrderId"):
        value = row.get(field)
        if isinstance(value, str) and value:
            return value
    return ""


def _local_bot_client_ids(t: dict) -> list:
    ids = []
    for field in ("client_order_id", "exchange_client_id"):
        value = t.get(field)
        if isinstance(value, str) and value.startswith("BOT_") and value not in ids:
            ids.append(value)
    return ids


def _missing_exchange_qty(t: dict) -> bool:
    qty = t.get("exchange_qty")
    if qty is None:
        return True
    try:
        return float(qty) <= 0
    except (TypeError, ValueError):
        return True


def _missing_live_exchange_proof(t: dict) -> bool:
    return (
        _missing_exchange_qty(t)
        and not t.get("exchange_order_id")
        and not t.get("exchange_sl_id")
        and not t.get("exchange_position_owner_confirmed")
    )


def _live_entry_result_is_uncertain(result: dict) -> bool:
    if not isinstance(result, dict):
        return False
    return (
        result.get("entry_state") == "ENTRY_UNCERTAIN"
        or result.get("error_code") == "order_state_unknown"
    )


def _mark_live_entry_uncertain(t: dict, client_order_id: str, reason: str = "order_state_unknown") -> None:
    t["entry_state"] = "ENTRY_UNCERTAIN"
    t["exchange_order_state_unknown"] = True
    t["exchange_client_id"] = client_order_id
    t["client_order_id"] = client_order_id
    t["exchange_order_id"] = None
    t["exchange_fill_price"] = None
    t["exchange_qty"] = None
    t["exchange_position_owner_confirmed"] = False
    t["entry_uncertain_ts"] = time.time()
    t["entry_uncertain_reason"] = reason


def _live_order_status_is_filled(order: dict) -> bool:
    status = str(order.get("status", "")).upper()
    if status not in ("FILLED", "PARTIALLY_FILLED"):
        return False
    return _safe_float_value(order.get("executedQty"), 0.0) > 0


def _entry_result_from_live_query(order: dict, client_order_id: str) -> dict:
    return {
        "success": True,
        "order_id": order.get("orderId"),
        "client_order_id": _get_order_client_id(order) or client_order_id,
        "status": order.get("status", "?"),
        "fill_price": _safe_float_value(order.get("avgPrice"), None),
        "fill_qty": _safe_float_value(order.get("executedQty"), None),
        "raw": order,
        "error": None,
        "entry_state": "ENTRY_CONFIRMED",
    }


def _resolve_live_uncertain_entry(t: dict, live_executor, prefix=None) -> dict:
    symbol = t.get("symbol", "UNKNOWN")
    client_order_id = t.get("client_order_id") or t.get("exchange_client_id")
    if not client_order_id:
        return {"state": "ambiguous", "reason": "missing_client_order_id"}
    query_order = getattr(live_executor, "query_order", None)
    if query_order is None:
        return {"state": "ambiguous", "reason": "query_order_unavailable"}

    print(
        f"[LIVE ENTRY UNCERTAIN] {symbol} querying market order by "
        f"clientOrderId={client_order_id}"
    )
    try:
        order = query_order(
            symbol,
            client_order_id=client_order_id,
            return_not_found=True,
        )
    except TypeError:
        order = query_order(symbol, client_order_id=client_order_id)
    except Exception as exc:
        return {"state": "ambiguous", "reason": f"query_exception:{type(exc).__name__}"}

    if isinstance(order, dict) and order.get("_query_not_found"):
        t["entry_state"] = "ENTRY_NOT_FOUND"
        t["exchange_order_state_unknown"] = False
        t["entry_not_found_ts"] = time.time()
        print(
            f"[LIVE ENTRY NOT FOUND] {symbol} clientOrderId={client_order_id} "
            "authoritatively absent on exchange"
        )
        return {"state": "not_found", "reason": "query_order_not_found"}

    if order is None:
        _mark_live_entry_uncertain(t, client_order_id, "query_order_ambiguous")
        print(
            f"[LIVE ENTRY UNCERTAIN] {symbol} query unavailable/ambiguous - "
            "local state preserved; market order will NOT be retried blindly"
        )
        send_telegram(
            f"[LIVE ENTRY UNCERTAIN] {symbol}\n"
            f"clientOrderId={client_order_id}\n"
            "query_order ambiguous; local state preserved; no retry",
            prefix=prefix,
            channel="alerts",
        )
        return {"state": "ambiguous", "reason": "query_order_ambiguous"}

    if _live_order_status_is_filled(order):
        result = _entry_result_from_live_query(order, client_order_id)
        print(
            f"[LIVE ENTRY CONFIRMED] {symbol} clientOrderId={client_order_id} "
            f"status={result.get('status')} qty={result.get('fill_qty')}"
        )
        return {"state": "confirmed", "reason": "query_order_filled", "entry_result": result}

    _mark_live_entry_uncertain(t, client_order_id, f"query_status_{order.get('status', '?')}")
    print(
        f"[LIVE ENTRY UNCERTAIN] {symbol} query found status={order.get('status', '?')} "
        "without fill; local state preserved; no market retry"
    )
    send_telegram(
        f"[LIVE ENTRY UNCERTAIN] {symbol}\n"
        f"clientOrderId={client_order_id}\n"
        f"query_order status={order.get('status', '?')}; no fill confirmation",
        prefix=prefix,
        channel="alerts",
    )
    return {"state": "ambiguous", "reason": f"query_status_{order.get('status', '?')}"}


def _order_side_matches_trade(order: dict, t: dict) -> bool:
    expected = "BUY" if t.get("side") == "LONG" else "SELL"
    return str(order.get("side", "")).upper() == expected


def _truthy_exchange_flag(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return bool(value)


def _regular_order_is_entry(order: dict, t: dict) -> bool:
    if not isinstance(order, dict):
        return False
    if not _has_bot_client_id(order):
        return False
    if not _order_side_matches_trade(order, t):
        return False
    if _truthy_exchange_flag(order.get("reduceOnly")) or _truthy_exchange_flag(order.get("closePosition")):
        return False
    status = str(order.get("status", "")).upper()
    executed_qty = _safe_float_value(order.get("executedQty"), 0.0)
    return status in ("FILLED", "PARTIALLY_FILLED") and executed_qty > 0


def _bot_algo_order_for_trade(order: dict, t: dict) -> bool:
    if not isinstance(order, dict):
        return False
    if not _has_bot_client_id(order):
        return False
    expected_stop_side = "SELL" if t.get("side") == "LONG" else "BUY"
    return str(order.get("side", "")).upper() == expected_stop_side


def _apply_live_bot_ownership_recovery(t: dict, evidence: dict) -> bool:
    changed = False
    entry_order = evidence.get("entry_order")
    if isinstance(entry_order, dict):
        client_id = _get_order_client_id(entry_order)
        if client_id and t.get("client_order_id") != client_id:
            t["client_order_id"] = client_id
            changed = True
        if client_id and t.get("exchange_client_id") != client_id:
            t["exchange_client_id"] = client_id
            changed = True
        order_id = entry_order.get("orderId")
        if order_id and t.get("exchange_order_id") != order_id:
            t["exchange_order_id"] = order_id
            changed = True
        fill_qty = _safe_float_value(entry_order.get("executedQty"), 0.0)
        if fill_qty > 0 and t.get("exchange_qty") != fill_qty:
            t["exchange_qty"] = fill_qty
            t["qty"] = fill_qty
            changed = True
        fill_price = _safe_float_value(entry_order.get("avgPrice"), 0.0)
        if fill_price > 0 and t.get("exchange_fill_price") != fill_price:
            t["exchange_fill_price"] = fill_price
            changed = True
        if client_id.startswith("BOT_") and not t.get("exchange_position_owner_confirmed"):
            t["exchange_position_owner_confirmed"] = True
            changed = True

    algo_order = evidence.get("algo_order")
    if isinstance(algo_order, dict):
        algo_id = algo_order.get("algoId")
        if algo_id and t.get("exchange_sl_id") != algo_id:
            t["exchange_sl_id"] = algo_id
            changed = True
        stop_price = _safe_float_value(algo_order.get("triggerPrice"), 0.0)
        if stop_price > 0 and t.get("exchange_sl_price_confirmed") != stop_price:
            t["exchange_sl_price_confirmed"] = stop_price
            changed = True

    return changed


def _inspect_live_missing_proof_ownership(t: dict, live_executor) -> dict:
    symbol = t.get("symbol", "UNKNOWN")
    local_bot_ids = _local_bot_client_ids(t)
    result = {
        "status": "no_bot_evidence",
        "reason": "",
        "entry_order": None,
        "algo_order": None,
        "local_bot_ids": local_bot_ids,
    }

    for client_id in local_bot_ids:
        get_signed = getattr(live_executor, "_get_signed", None)
        if not get_signed:
            result["status"] = "uncertain"
            result["reason"] = "raw_order_query_unavailable"
            return result
        order = get_signed("/fapi/v1/order", {"symbol": symbol, "origClientOrderId": client_id})
        if order is None:
            result["status"] = "uncertain"
            result["reason"] = "local_bot_client_id_query_failed"
            return result
        if isinstance(order, dict) and isinstance(order.get("code"), int) and order["code"] < 0:
            if order.get("code") == -2013:
                continue
            result["status"] = "uncertain"
            result["reason"] = f"local_bot_client_id_query_error_{order.get('code')}"
            return result
        if order is not None:
            if _regular_order_is_entry(order, t):
                result["status"] = "bot_evidence"
                result["reason"] = "local_bot_client_id_found"
                result["entry_order"] = order
                return result
            if _has_bot_client_id(order):
                result["status"] = "uncertain"
                result["reason"] = "local_bot_client_id_found_not_filled"
                return result

    get_recent_orders = getattr(live_executor, "get_recent_orders", None)
    get_open_orders = getattr(live_executor, "get_open_orders", None)
    get_open_algo_orders = getattr(live_executor, "get_open_algo_orders", None)
    if not get_recent_orders or not get_open_orders or not get_open_algo_orders:
        result["status"] = "uncertain"
        result["reason"] = "ownership_query_unavailable"
        return result

    recent_orders = get_recent_orders(symbol, limit=50)
    open_orders = get_open_orders(symbol)
    open_algos = get_open_algo_orders(symbol)
    if recent_orders is None or open_orders is None or open_algos is None:
        result["status"] = "uncertain"
        result["reason"] = "ownership_query_failed"
        return result

    entry_candidates = [o for o in recent_orders if _regular_order_is_entry(o, t)]
    algo_candidates = [o for o in open_algos if _bot_algo_order_for_trade(o, t)]

    if len(entry_candidates) == 1:
        result["status"] = "bot_evidence"
        result["reason"] = "single_recent_bot_entry"
        result["entry_order"] = entry_candidates[0]
        if len(algo_candidates) == 1:
            result["algo_order"] = algo_candidates[0]
        return result

    if len(entry_candidates) > 1:
        result["status"] = "uncertain"
        result["reason"] = "multiple_recent_bot_entries"
        return result

    if len(algo_candidates) == 1:
        result["status"] = "bot_evidence"
        result["reason"] = "single_open_bot_algo"
        result["algo_order"] = algo_candidates[0]
        return result

    if len(algo_candidates) > 1:
        result["status"] = "uncertain"
        result["reason"] = "multiple_open_bot_algos"
        return result

    manual_order_count = sum(1 for o in list(recent_orders) + list(open_orders) + list(open_algos) if not _has_bot_client_id(o))
    result["reason"] = f"no_bot_order_evidence manual_or_unknown_orders={manual_order_count}"
    return result


def _inspect_exchange_only_live_bot_evidence(exchange_position: dict, live_executor) -> dict:
    symbol = exchange_position.get("symbol", "UNKNOWN")
    try:
        amt = float(exchange_position.get("positionAmt", 0.0))
    except (TypeError, ValueError):
        return {"status": "uncertain", "reason": "malformed_position_amt", "entry_order": None}
    if amt == 0.0:
        return {"status": "no_bot_evidence", "reason": "flat_position", "entry_order": None}

    probe_trade = {
        "symbol": symbol,
        "side": "LONG" if amt > 0 else "SHORT",
    }
    get_recent_orders = getattr(live_executor, "get_recent_orders", None)
    get_open_orders = getattr(live_executor, "get_open_orders", None)
    get_open_algo_orders = getattr(live_executor, "get_open_algo_orders", None)
    if not get_recent_orders or not get_open_orders or not get_open_algo_orders:
        return {"status": "uncertain", "reason": "ownership_query_unavailable", "entry_order": None}

    recent_orders = get_recent_orders(symbol, limit=50)
    open_orders = get_open_orders(symbol)
    open_algos = get_open_algo_orders(symbol)
    if recent_orders is None or open_orders is None or open_algos is None:
        return {"status": "uncertain", "reason": "ownership_query_failed", "entry_order": None}

    entry_candidates = [
        o for o in list(recent_orders) + list(open_orders)
        if _regular_order_is_entry(o, probe_trade)
    ]
    if len(entry_candidates) == 1:
        return {
            "status": "bot_orphan_needs_reconstruction",
            "reason": "single_filled_bot_entry",
            "entry_order": entry_candidates[0],
        }
    if len(entry_candidates) > 1:
        return {"status": "uncertain", "reason": "multiple_filled_bot_entries", "entry_order": None}

    bot_order_count = sum(
        1 for o in list(recent_orders) + list(open_orders) + list(open_algos)
        if _has_bot_client_id(o)
    )
    if bot_order_count:
        return {
            "status": "uncertain",
            "reason": f"bot_order_evidence_without_single_entry count={bot_order_count}",
            "entry_order": None,
        }
    return {"status": "no_bot_evidence", "reason": "no_bot_order_evidence", "entry_order": None}


def _backup_live_state_files_for_manual_uncertain(state_file: str) -> list:
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    created = []
    for path in (state_file, _state_backup_path_for_file(state_file)):
        if not path or not os.path.exists(path):
            continue
        backup_path = f"{path}.pre_manual_uncertain_cleanup.{timestamp}.bak"
        with open(path, "rb") as src, open(backup_path, "wb") as dst:
            dst.write(src.read())
            dst.flush()
            os.fsync(dst.fileno())
        created.append(backup_path)
    return created


def _state_backup_path_for_file(state_file: str) -> str:
    return f"{state_file}.bak"


def _remove_manual_or_uncertain_live_trade(trades_list: list, trade: dict, state_file: str) -> list:
    backups = _backup_live_state_files_for_manual_uncertain(state_file)
    with _lock:
        if trade in trades_list:
            trades_list.remove(trade)
    save_open_trades(trades_list, state_file)
    if os.path.basename(state_file).lower() == "live_state.json":
        save_open_trades(trades_list, _state_backup_path_for_file(state_file))
    return backups


def _safe_float_value(value, default=0.0) -> float:
    try:
        if value is None:
            return default
        parsed = float(value)
        if math.isnan(parsed):
            return default
        return parsed
    except (TypeError, ValueError):
        return default


def _safe_numeric_value(value):
    try:
        if value is None:
            return None
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return None
        return parsed
    except (TypeError, ValueError):
        return None


def _warn_missing_tp_reconstructed_orphan(t: dict):
    if not t.get("reconstructed_from_orphan") or t.get("_missing_tp_reconstructed_orphan_warned"):
        return
    print(
        f"[TRADE MGMT] {t.get('symbol', 'UNKNOWN')} missing_tp_reconstructed_orphan "
        "tp comparison disabled; SL/trailing management continues"
    )
    t["_missing_tp_reconstructed_orphan_warned"] = True


def _finalize_audit_exchange_sl_close(t: dict, ctx, source: str = "audit_exchange_sl") -> bool:
    """
    Finalize a bot-owned trade when the exchange confirms its position is already
    closed and the missing SL algo was consumed by a normal stop fill.

    This is deliberately idempotent: the audit can run more than once after the
    exchange close, but the CSV/log and balance effects should happen once.
    """
    if ctx is None or t.get("owner", "bot") != "bot":
        return False

    symbol = t.get("symbol", "UNKNOWN")

    if t.get("sl_audit_finalized"):
        save_open_trades(ctx.trades, ctx.state_file)
        return True

    now = time.time()
    entry_real = _safe_float_value(t.get("entry_real") or t.get("entry"), 0.0)
    sl_init = _safe_float_value(t.get("sl_init"), _safe_float_value(t.get("sl"), 0.0))
    exit_price_source = "exchange_fill"
    exit_price = _safe_float_value(
        t.get("exchange_exit_price")
        or t.get("exchange_close_price")
        or t.get("exit_price"),
        0.0,
    )
    if exit_price <= 0:
        exit_price_source = "confirmed_sl"
        exit_price = _safe_float_value(t.get("exchange_sl_price_confirmed"), 0.0)
    if exit_price <= 0:
        exit_price_source = "stored_sl"
        exit_price = _safe_float_value(t.get("sl"), 0.0)
    if exit_price <= 0:
        exit_price_source = "entry_fallback"
        exit_price = entry_real
        print(
            f"[SL AUDIT WARNING] {symbol} using entry fallback for exchange SL close "
            f"source={source}"
        )

    t["exit_type"] = "SL"
    t["close_reason"] = "exchange_sl_filled"
    t["close_time"] = now
    t["exit_price"] = exit_price
    t["exit_price_source"] = exit_price_source
    t["sl_audit_closed"] = True
    t["sl_audit_close_source"] = source

    risk_init = abs(entry_real - sl_init)
    if entry_real > 0 and risk_init > 0:
        if t.get("side") == "SHORT":
            rr_real = (entry_real - exit_price) / risk_init
        else:
            rr_real = (exit_price - entry_real) / risk_init
        t["rr_real"] = round(max(min(rr_real, 10), -10), 2)
    else:
        t["rr_real"] = 0

    if abs(t["rr_real"]) < 0.1:
        t["status"] = "BE"
        t["rr_real"] = 0
    elif t["rr_real"] > 0:
        t["status"] = "WIN"
    else:
        t["status"] = "LOSE"

    open_ts = _safe_float_value(t.get("time"), 0.0)
    t["trade_age_minutes"] = round((now - open_ts) / 60, 1) if open_ts > 0 and now > open_ts else 0
    t["giveback_r"] = round(max(0.0, t.get("max_profit_r", 0) - t.get("rr_real", 0)), 2)
    t["trailing_phase_at_exit"] = t.get("trail_phase", 0)
    if t.get("_above_1r_since"):
        t["time_spent_above_1r"] = round(
            t.get("time_spent_above_1r", 0) + (now - t["_above_1r_since"]) / 60,
            1,
        )
        t["_above_1r_since"] = None

    if t["status"] == "LOSE" and not t.get("cooldown_set"):
        ctx.cooldown[symbol] = now
        t["cooldown_set"] = True

    if not t.get("balance_updated"):
        bal_at_entry = _safe_float_value(t.get("balance_at_entry"), ctx.account_balance)
        pnl = bal_at_entry * t.get("risk_percent", RISK_PER_TRADE) * t["rr_real"]
        ctx.account_balance += pnl
        ctx.equity_peak = max(ctx.equity_peak, ctx.account_balance)
        ctx.session_pnl_r += t["rr_real"]
        t["balance_updated"] = True
        if hasattr(ctx, "save_account_state"):
            ctx.save_account_state()

    if not t.get("stat_counted"):
        if t["status"] == "WIN":
            stats["win"] += 1
            ctx.stats["win"] = ctx.stats.get("win", 0) + 1
        elif t["status"] == "LOSE":
            stats["loss"] += 1
            ctx.stats["loss"] = ctx.stats.get("loss", 0) + 1
        elif t["status"] == "BE":
            ctx.stats["be"] = ctx.stats.get("be", 0) + 1
        t["stat_counted"] = True

    t["exchange_sl_id"] = None
    t["exchange_sl_sync_pending"] = None
    t["exchange_sl_price_confirmed"] = None
    t["orphan_stop_ids"] = []

    if not t.get("sl_audit_trade_saved"):
        save_trade(t, ctx.trades_csv)
        save_tier_log(t)
        log_false_positive(t)
        log_wyckoff_outcome(t)
        history.append(t)
        t["sl_audit_trade_saved"] = True

    if not t.get("sl_audit_close_alert_sent"):
        msg = (
            f"[SL AUDIT CLOSE] {symbol}\n"
            f"Exit: {fmt_price(exit_price, symbol)}\n"
            f"RR: {t.get('rr_real', 0)}R\n"
            f"Source: {exit_price_source} | Reason: {source}"
        )
        try:
            send_telegram(msg, prefix=ctx.mode_prefix)
        except Exception:
            pass
        t["sl_audit_close_alert_sent"] = True

    t["sl_audit_finalized"] = True
    save_open_trades(ctx.trades, ctx.state_file)
    print(
        f"[SL AUDIT] {symbol} finalized exchange SL close "
        f"exit={round(exit_price, 6)} rr={t.get('rr_real', 0)} "
        f"source={exit_price_source} reason={source}"
    )
    return True



_QUARANTINE_TTL_SECS = 24 * 3600
_quarantine_alerted: set = set()


def check_quarantine_ttl(ctx=None):
    _trades = ctx.trades if ctx is not None else trades
    _mode_prefix = ctx.mode_prefix if ctx is not None else None
    now = time.time()
    for t in _trades:
        if not t.get("quarantined"):
            continue
        if t.get("stale_quarantine"):
            continue
        trade_id = t.get("id", t.get("symbol", ""))
        if trade_id in _quarantine_alerted:
            continue
        ts = t.get("quarantine_timestamp", 0)
        if ts > 0 and (now - ts) > _QUARANTINE_TTL_SECS:
            t["stale_quarantine"] = True
            symbol = t.get("symbol", "UNKNOWN")
            reason = t.get("quarantine_reason", "unknown")
            age_h = int((now - ts) / 3600)
            msg = (
                f"[QUARANTINE ALERT]\n"
                f"{symbol} quarantined >{age_h}h\n"
                f"reason={reason}\n"
                f"Manual review required"
            )
            print(f"\n{'='*40}\n⚠ STALE QUARANTINE: {symbol} age={age_h}h reason={reason}\n{'='*40}")
            try:
                send_telegram(msg, prefix=_mode_prefix, channel="alerts")
            except Exception:
                pass
            _quarantine_alerted.add(trade_id)


def calc_current_total_risk(trades):
    # Portfolio risk is calculated from bot-owned positions only.
    # Manual exchange positions are not in ctx.trades and are never counted.
    total = 0
    for t in trades:
        if (t["status"] == "OPEN"
                and not t.get("quarantined")
                and t.get("owner", "bot") == "bot"):
            total += t.get("risk_percent", RISK_PER_TRADE)
    return total

def calc_symbol_risk(trades, symbol):
    # Per-symbol risk counts bot-owned positions only.
    total = 0
    for t in trades:
        if (t["status"] == "OPEN"
                and not t.get("quarantined")
                and t["symbol"] == symbol
                and t.get("owner", "bot") == "bot"):
            total += t.get("risk_percent", RISK_PER_TRADE)
    return total

def update_signal_state(symbol, direction, price=0, executed=False, ctx=None):
    _state = ctx.signal_state if ctx is not None else signal_state
    _state[symbol] = {"time": time.time(), "direction": direction, "price": price, "executed": executed}

def dynamic_phase_trigger(vol):
    return {
        "phase2": 1 + vol * 1.2,
        "phase3": 2 + vol * 1.5
    }

def is_momentum_weakening(df15, side):
    """
    Body thu nh? + volume gi?m + n?n ng??c chi?u = momentum y?u.
    D?ng ?? exit s?m thay v? ch? giveback ratio c?ng.
    """
    if len(df15) < 4:
        return False

    c1 = df15.iloc[-3]
    c2 = df15.iloc[-2]

    body1 = abs(c1["close"] - c1["open"])
    body2 = abs(c2["close"] - c2["open"])
    vol1  = df15["volume"].iloc[-3]
    vol2  = df15["volume"].iloc[-2]

    body_shrink = body2 < body1 * 0.5
    vol_shrink  = vol2  < vol1  * 0.6

    if side == "LONG":
        wrong_color = c2["close"] < c2["open"]
    else:
        wrong_color = c2["close"] > c2["open"]

    return body_shrink and vol_shrink and wrong_color

def exit_optimization(t, price, df15, prefix=None, management_context=None):
    entry = t.get("entry_real") or t.get("entry")
    sl_init = t["sl_init"]

    if t["side"] == "LONG":
        price_real = price * (1 - SLIPPAGE)
    else:
        price_real = price * (1 + SLIPPAGE)

    if t["side"] == "LONG":
        profit = price_real - entry
    else:
        profit = entry - price_real

    risk = abs(entry - t["sl_init"])

    profit_r = profit / risk if risk > 0 else 0

    t["max_profit_r"] = max(t.get("max_profit_r", 0), profit_r)   # 🔥 NEW

    # ===== PHASE =====
    old_phase = t["trail_phase"]
    volatility = get_volatility(df15) 
    phase_trigger = dynamic_phase_trigger(volatility)
    margin = 0.2

    if t["trail_phase"] == 3:
        pass  # lock phase 3

    elif profit_r >= phase_trigger["phase3"] + margin or t.get("tp_hit", False):
        t["trail_phase"] = 3

    elif profit_r >= phase_trigger["phase2"] + margin:
        t["trail_phase"] = max(t["trail_phase"], 2)

     # ========================
    # [U4] MOMENTUM WEAKENING EXIT
    # ========================
    if t.get("partial_done") and t.get("max_profit_r", 0) >= 1.5:
        if is_momentum_weakening(df15, t["side"]):
            if not t.get("momentum_notified"):
                t["momentum_notified"] = True
            return "EARLY_MOMENTUM_WEAK"
        
    # ===== TELE PHASE =====
    if t["trail_phase"] == 2 and old_phase == 1 and not t.get("phase2_sent"):
        _send_management_telegram(
            t,
            f"📊 {t['symbol']} PHASE 2 — +{round(profit_r, 2)}R\n"
            f"SL → {fmt_price(t['sl'], t['symbol'])}",
            "trail_phase_2",
            prefix,
            t.get("execution_mode") or EXECUTION_MODE,
            category="phase",
            management_context=management_context,
        )
        t["phase2_sent"] = True

    if t["trail_phase"] == 3 and old_phase == 2 and not t.get("phase3_sent"):
        _send_management_telegram(
            t,
            f"🔒 {t['symbol']} PHASE 3 — LOCK\n"
            f"Max: {round(t.get('max_profit_r', 0), 2)}R | SL → {fmt_price(t['sl'], t['symbol'])}",
            "trail_phase_3",
            prefix,
            t.get("execution_mode") or EXECUTION_MODE,
            category="phase",
            management_context=management_context,
        )
        t["phase3_sent"] = True

    # ===== GIVEBACK =====
    if risk == 0:
        return None
    max_r = t.get("max_profit_r", 0)
    cur_r = profit_r
    
    if max_r >= 3 and not t.get("lock_done"):
        if t["side"] == "LONG":
            t["sl"] = max(t["sl"], entry + risk)
        else:
            t["sl"] = min(t["sl"], entry - risk)
        t["lock_done"] = True

    _trend_mode = t.get("market_mode", "") == "TREND"
    _eg_threshold = 2.5 if _trend_mode else 2.0

    if max_r >= _eg_threshold:
        volatility = get_volatility(df15)
        if len(df15) >= 10:
            trend_pow = abs(df15["close"].iloc[-2] - df15["close"].iloc[-10]) / df15["close"].iloc[-10]
        else:
            trend_pow = 0

        giveback_ratio = min(0.9, dynamic_giveback(volatility) + trend_pow * 0.3)

        if _trend_mode:
            giveback_ratio = min(giveback_ratio, 0.6)

        exhaustion_cls_trail = t.get("exhaustion_cls", "HEALTHY")
        if exhaustion_cls_trail == "COLLAPSING":
            giveback_ratio = min(giveback_ratio, 0.35)
        elif exhaustion_cls_trail == "EXHAUSTED":
            giveback_ratio = min(giveback_ratio, 0.45)
        elif exhaustion_cls_trail == "EXTENDED":
            giveback_ratio = min(giveback_ratio, 0.55)

        # ===== TIME-DECAY TRAILING =====
        if t.get("be_07_done") or max_r >= 1.0:
            _trade_age_h = (time.time() - t.get("time", time.time())) / 3600
            if _trade_age_h >= 6:
                _td_mode = "AGGRESSIVE_HARVEST"
                giveback_ratio = min(giveback_ratio, 0.40)
            elif _trade_age_h >= 2:
                _td_mode = "MODERATE_RETENTION"
                giveback_ratio = min(giveback_ratio, 0.55)
            else:
                _td_mode = None
            if _td_mode and t.get("_time_decay_mode") != _td_mode:
                print(f"[TIME DECAY] {t['symbol']} age={round(_trade_age_h, 1)}h mode={_td_mode}")
                t["_time_decay_mode"] = _td_mode

        if cur_r < max_r * giveback_ratio:
            if not t.get("giveback_notified"):
                t["giveback_notified"] = True
            return "EARLY_GIVEBACK"
        

    # ===== STRUCT FAIL =====
    _struct_high_score = t.get("score", 0) >= 10

    if t["side"]=="LONG":
        _struct_level_long = df15["low"].iloc[-5:-1].min()
        if df15["close"].iloc[-1] < _struct_level_long:
            _body_broke_long = min(df15["close"].iloc[-1], df15["open"].iloc[-1]) < _struct_level_long
            if not _struct_high_score or _body_broke_long:
                if not (t.get("max_profit_r", 0) >= 1.5 and not is_momentum_weakening(df15, t["side"])):
                    if not t["struct_notified"]:
                        _send_management_telegram(
                            t,
                            f"?? {t['symbol']} G?y c?u tr?c\n"
                            f"Max ??t: {round(t.get('max_profit_r', 0), 2)}R ? C?n nh?c ??ng",
                            "structure_warning",
                            prefix,
                            t.get("execution_mode") or EXECUTION_MODE,
                            category="struct_warn",
                            management_context=management_context,
                        )
                        t["struct_notified"] = True
                    return "EARLY_STRUCT"

    else:
        _struct_level_short = df15["high"].iloc[-5:-1].max()
        if df15["close"].iloc[-1] > _struct_level_short:
            _body_broke_short = max(df15["close"].iloc[-1], df15["open"].iloc[-1]) > _struct_level_short
            if not _struct_high_score or _body_broke_short:
                if not (t.get("max_profit_r", 0) >= 1.5 and not is_momentum_weakening(df15, t["side"])):
                    if not t["struct_notified"]:
                        _send_management_telegram(
                            t,
                            f"?? {t['symbol']} G?y c?u tr?c\n"
                            f"Max ??t: {round(t.get('max_profit_r', 0), 2)}R ? C?n nh?c ??ng",
                            "structure_warning",
                            prefix,
                            t.get("execution_mode") or EXECUTION_MODE,
                            category="struct_warn",
                            management_context=management_context,
                        )
                        t["struct_notified"] = True
                    return "EARLY_STRUCT"

    return None

def open_trade(t, ctx=None):
    global ACCOUNT_BALANCE, EQUITY_PEAK, early_count, confirm_count_this_cycle

    # ===== RESOLVE EXECUTOR STATE =====
    if ctx is not None:
        _trades = ctx.trades
        _entry_cooldown = ctx.entry_cooldown
        _cooldown = ctx.cooldown
        _lock = ctx.lock
        _exec_mode = ctx.execution_mode
        _mode_prefix = ctx.mode_prefix
        _state_file = ctx.state_file
    else:
        _trades = trades
        _entry_cooldown = entry_cooldown
        _cooldown = cooldown
        _lock = trades_lock
        _exec_mode = EXECUTION_MODE
        _mode_prefix = None
        _state_file = None

    symbol = t["symbol"]
    t["time"] = time.time()
    t["execution_mode"] = _exec_mode
    t["execution_tier"] = _get_execution_tier(symbol)
    # Stamp ownership unconditionally on every bot-originated trade so that
    # reconcile, SL audit, risk accounting, and slot counting can distinguish
    # bot entries from any manually-injected state file entries.
    t["owner"] = "bot"
    if _exec_mode == "live":
        t["bos_type"] = _extract_runtime_bos_type(t)

    if _exec_mode == "paper":
        _signal_entry = t.get("entry", 0)
        t["entry_real"] = apply_entry_spread(_signal_entry, t["side"])
        t["entry_spread"] = SPREAD

    # ===== ANTI DUPLICATE =====
    existing = [x for x in _trades if x["symbol"] == symbol and x["status"] == "OPEN"]
    if existing:
        print(f"[BLOCK] DUPLICATE_TRADE {symbol} open_trades={len(existing)}")
        return

    # ===== [U3] CORRELATION FILTER =====
    if not check_correlation(_trades, t["side"]):
        print(f"[BLOCK] PORTFOLIO_EXPOSURE_LIMIT {symbol} side={t['side']} reason=correlation_limit")
        return

    # ===== SCORE-BASED POSITION SIZING =====
    if _exec_mode == "live":
        try:
            base_risk = _get_live_config_float("live_risk_per_trade")
        except ValueError as _risk_cfg_err:
            print(f"[LIVE SAFETY BLOCK] {symbol} reason={_risk_cfg_err}")
            return False
    else:
        base_risk = t.get("base_risk_percent", RISK_PER_TRADE)
    score = t.get("score", 0)
    _is_paper_smc_research = (
        _exec_mode == "paper"
        and t.get("entry_type") == "CONFIRM_SMC_RESEARCH"
        and t.get("strategy_family") == "confirm_smc_research"
    )

    if score < 7:
        if _is_paper_smc_research:
            t["risk_percent"] = base_risk * 0.5
        else:
            print(f"[BLOCK] LOW_SCORE {symbol} score={round(score, 2)}")
            return

    elif 7 <= score < 8:
        t["risk_percent"] = base_risk * 0.5
        if DEBUG:
            print(f"[SIZE REDUCED] symbol={symbol} score={round(score, 2)} → 50%")

    else:
        t["risk_percent"] = base_risk

    now = time.time()

    # ===== COOLDOWN =====
    if symbol in _entry_cooldown and now - _entry_cooldown[symbol] < ENTRY_COOLDOWN:
        if DEBUG:
            print(f"[PIPELINE DROP] symbol={symbol} stage=ENTRY→OPEN reason=entry_cooldown remaining={round(ENTRY_COOLDOWN-(now-_entry_cooldown[symbol]))}s")
        return
    if symbol in _cooldown and now - _cooldown[symbol] < LOSS_COOLDOWN:
        if DEBUG:
            print(f"[PIPELINE DROP] symbol={symbol} stage=ENTRY→OPEN reason=loss_cooldown remaining={round(LOSS_COOLDOWN-(now-_cooldown[symbol]))}s")
        return

    # ===== MINIMUM RISK GUARD =====
    _entry_price = t.get("entry_real") or t.get("entry", 0)
    _sl_price = t.get("sl", 0)
    _risk_dist = abs(_entry_price - _sl_price)
    if _entry_price > 0 and _risk_dist / _entry_price < 0.001:
        print(f"[BLOCK] RISK_LIMIT_EXCEEDED {symbol} entry={_entry_price} sl={_sl_price} risk_ratio={round(_risk_dist/_entry_price,6)}")
        return

    # ===== INVALID SL PLACEMENT GUARD =====
    if t["side"] == "LONG" and _sl_price >= _entry_price:
        print(f"[BLOCK] INVALID_STATE {symbol} side=LONG entry={_entry_price} sl={_sl_price}")
        return
    if t["side"] == "SHORT" and _sl_price <= _entry_price:
        print(f"[BLOCK] INVALID_STATE {symbol} side=SHORT entry={_entry_price} sl={_sl_price}")
        return

    # ===== RISK =====
    risk_percent = t["risk_percent"]
    current_total_risk = calc_current_total_risk(_trades)
    symbol_risk = calc_symbol_risk(_trades, symbol)

    if _exec_mode == "live":
        try:
            _portfolio_risk_cap = _get_live_config_float("live_max_portfolio_risk")
        except ValueError as _risk_cfg_err:
            print(f"[LIVE SAFETY BLOCK] {symbol} reason={_risk_cfg_err}")
            return False
    else:
        _portfolio_risk_cap = MAX_TOTAL_RISK

    # soft throttle when portfolio already loaded
    if current_total_risk > _portfolio_risk_cap * 0.7:
        risk_percent *= 0.7

    if current_total_risk > _portfolio_risk_cap * 0.9:
        if t["score"] < 8:
            return

    _confirm_count = ctx.confirm_count_this_cycle if ctx is not None else confirm_count_this_cycle
    if t.get("entry_type", "") == "CONFIRM":
        if _confirm_count >= MAX_CONFIRM_PER_CYCLE:
            risk_percent *= 0.7

    if current_total_risk + risk_percent > _portfolio_risk_cap:
        if t["score"] >= 12:
            excess = (current_total_risk + risk_percent) - _portfolio_risk_cap
            scale = max(0.3, 1 - excess / max(_portfolio_risk_cap, 1e-9))
            risk_percent *= scale
            t["risk_percent"] = risk_percent
            if DEBUG:
                print(f"[RISK SOFT CAP] {symbol} scaled risk → {risk_percent:.4f}")
        else:
            print(f"[BLOCK] RISK_LIMIT_EXCEEDED {symbol} current={round(current_total_risk,4)} add={round(risk_percent,4)} max={_portfolio_risk_cap}")
            return

    t["risk_percent"] = risk_percent

    if DEBUG:
        print(
            f"[PORTFOLIO] risk={current_total_risk:.3f} "
            f"new={risk_percent:.3f} score={t['score']}"
        )

    if symbol_risk + risk_percent > MAX_RISK_PER_SYMBOL:
        print(f"[BLOCK] RISK_LIMIT_EXCEEDED {symbol} current_symbol_risk={round(symbol_risk,4)} add={round(risk_percent,4)} max={MAX_RISK_PER_SYMBOL}")
        return

    if len([x for x in _trades if x["symbol"] == symbol and x["status"] == "OPEN"]) >= MAX_TRADES_PER_SYMBOL:
        print(f"[BLOCK] DUPLICATE_TRADE {symbol} max_trades_per_symbol={MAX_TRADES_PER_SYMBOL}")
        return

    if _exec_mode == "live":
        _live_exec = _resolve_exchange_executor(_exec_mode)
        _allowed, _reason = _check_live_runtime_safety_gate(
            _live_exec,
            t,
            _trades,
            risk_percent,
            current_total_risk,
            ctx=ctx,
        )
        if not _allowed:
            print(f"[LIVE SAFETY BLOCK] {symbol} reason={_reason}")
            return False

    stats["entry"] += 1
    if ctx is not None:
        if t.get("entry_type") == "EARLY_TIER0":
            ctx.early_count += 1
        if t.get("entry_type") == "CONFIRM":
            ctx.confirm_count_this_cycle += 1
    else:
        if t.get("entry_type") == "EARLY_TIER0":
            early_count += 1
        if t.get("entry_type") == "CONFIRM":
            confirm_count_this_cycle += 1

    # ===== [ADD] SWING TRACKING ASSIGNMENT =====
    _sym = t["symbol"]
    cw   = compression_watchlist.get(_sym, {})

    t["compression_score"] = cw.get("score")
    t["phase"]             = cw.get("phase")

    if t.get("entry_type", "").startswith("SWING"):
        t["priority_final"] = cw.get("priority_final", cw.get("priority"))
    else:
        t["priority_final"] = None

    if "bias_type" not in t:
        t["bias_type"] = None

    existing_open   = [x for x in _trades if x["symbol"] == _sym and x["status"] == "OPEN"]
    t["is_scale_in"] = len(existing_open) > 0
    # ===== END SWING TRACKING =====

    # [Part 5] Signal anti-spam: executor-local cooldown per ctx
    _signal_state_dict = ctx.signal_state if ctx is not None else signal_state
    if not check_signal_cooldown(t["symbol"], t["side"], signal_state_dict=_signal_state_dict):
        if DEBUG:
            print(f"[SIGNAL_SPAM] {t['symbol']} {t['side']} → skip (cooldown 15min)")
            print(f"[PIPELINE DROP] symbol={symbol} stage=ENTRY→OPEN reason=signal_cooldown_15min side={t['side']}")
        return

    _balance = ctx.account_balance if ctx is not None else ACCOUNT_BALANCE
    t["balance_at_entry"] = _balance

    _live_slot_reserved = False

    # ===== FOR LIVE: reserve a slot before exchange validation/order path =====
    if _exec_mode == "live":
        with _lock:
            if not _create_live_slot_reservation(ctx, _trades, t, _exec_mode):
                return False
            _live_slot_reserved = True

    # ===== PAPER MODE: Insert trade now (no exchange validation needed) =====
    if _exec_mode not in ("testnet", "live"):
        _paper_quality_attach_router_context(t, now_ts=t.get("time"))
        _paper_quality_init_trade(t)
        with _lock:
            _trades.append(t)
        _paper_quality_write_observation(t, "OPENED", _exec_mode)
        if ctx is not None:
            ctx.stats["opened"] = ctx.stats.get("opened", 0) + 1
        print(f"[ENTRY CREATED] {symbol} {t['side']} entry={round(t.get('entry', 0), 6)} sl={round(t.get('sl', 0), 6)} entry_time={t.get('entry_time')}")
        log_entry_clean(
            symbol=t["symbol"], direction=t["side"],
            score=t.get("score", 0), state=t.get("market_mode", "UNKNOWN"),
            structure="BOS" if t.get("bos_type") in ("CONFIRM","STRONG") else ("Near" if t.get("bos_type") in ("EARLY","NEAR") else "None"),
            volume_status=t.get("volume_state", "OK"),
            position_status="Near" if t.get("dist_to_level", 1) <= 0.005 else "Mid",
            entry_type=t.get("entry_type","")
        )
        if not _is_paper_smc_research:
            send_entry(t, prefix=_mode_prefix)
            stats["sent"] += 1
        else:
            print(
                f"[PAPER SMC RESEARCH ENTRY] {symbol} {t['side']} "
                f"score_shadow={t.get('score_v2_structural_shadow')} "
                f"reason={t.get('original_reason')}"
            )
            paper_smc_research_observe_open(t, ctx=ctx)

    # ===== EXCHANGE EXECUTION GATE (testnet + live) =====
    # Trade is NOT yet in _trades. Insertion happens only after validate_and_prepare() passes.
    if _exec_mode in ("testnet", "live"):
        _tn = _resolve_exchange_executor(_exec_mode)
        try:
            _eb = _tn.get_execution_balance()
        except Exception:
            if _exec_mode == "live" and _live_slot_reserved:
                with _lock:
                    _release_live_slot_reservation(
                        ctx, _trades, t, _exec_mode, "execution_balance_exception"
                    )
                _live_slot_reserved = False
            raise
        if _eb is None and _exec_mode == "live":
            print(
                f"[LIVE SAFETY BLOCK] {symbol} execution_balance unavailable — "
                f"skipped before local state insertion. exchange=unreachable"
            )
            send_telegram(
                f"[LIVE SAFETY BLOCK] {symbol}\nreason=execution_balance_unavailable",
                prefix=_mode_prefix,
                channel="alerts",
            )
            if _live_slot_reserved:
                with _lock:
                    _release_live_slot_reservation(
                        ctx, _trades, t, _exec_mode, "execution_balance_unavailable"
                    )
                _live_slot_reserved = False
            return False
        if _eb is not None:
            _side_str = "BUY" if t["side"] == "LONG" else "SELL"
            if _exec_mode == "live":
                _allowed, _reason = _check_live_runtime_safety_gate(
                    _tn,
                    t,
                    _trades,
                    t["risk_percent"],
                    current_total_risk,
                    ctx=ctx,
                    post_insert=True,
                )
                if not _allowed:
                    print(f"[LIVE SAFETY BLOCK] {symbol} before validate reason={_reason}")
                    send_telegram(
                        f"[LIVE SAFETY BLOCK] {symbol}\nreason={_reason}",
                        prefix=_mode_prefix,
                        channel="alerts",
                    )
                    if _live_slot_reserved:
                        with _lock:
                            _release_live_slot_reservation(
                                ctx, _trades, t, _exec_mode, "live_runtime_safety_gate_reject"
                            )
                        _live_slot_reserved = False
                    return False
            try:
                _prep = _tn.validate_and_prepare(
                    symbol=symbol,
                    side=_side_str,
                    entry=t.get("entry_real") or t.get("entry"),
                    sl=t.get("sl"),
                    tp=t.get("tp"),
                    balance=_eb,
                    risk_percent=t["risk_percent"],
                )
            except Exception:
                if _exec_mode == "live" and _live_slot_reserved:
                    with _lock:
                        _release_live_slot_reservation(
                            ctx, _trades, t, _exec_mode, "validate_and_prepare_exception"
                        )
                    _live_slot_reserved = False
                raise
            if not _prep["valid"]:
                if _exec_mode == "live":
                    print(
                        f"[LIVE SAFETY BLOCK] {symbol} validate_and_prepare failed — "
                        f"trade not inserted into local state. reason={_prep['reason']}"
                    )
                    send_telegram(
                        f"[LIVE SAFETY BLOCK] {symbol}\nvalidate failed: {_prep['reason']}",
                        prefix=_mode_prefix,
                        channel="alerts",
                    )
                    if _live_slot_reserved:
                        with _lock:
                            _release_live_slot_reservation(
                                ctx, _trades, t, _exec_mode, "validate_and_prepare_failed"
                            )
                        _live_slot_reserved = False
                return False
            if _prep["valid"]:
                # â”€â”€ INSERT TRADE (only after validation confirms executable) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                with _lock:
                    if ctx is not None:
                        _trades = ctx.trades
                    if _exec_mode == "live":
                        max_live = _get_max_live_trades()
                        open_live_count, pending_live_count, effective_live_count = _live_slot_snapshot(_trades, ctx)
                        if effective_live_count > max_live or not _live_slot_reserved:
                            _log_live_slot_decision(
                                "reject",
                                t,
                                max_live,
                                open_live_count,
                                pending_live_count,
                                effective_live_count,
                                _exec_mode,
                                "live_runtime_safety_gate_reject",
                            )
                            if _live_slot_reserved:
                                _release_live_slot_reservation(
                                    ctx, _trades, t, _exec_mode, "live_runtime_safety_gate_reject"
                                )
                                _live_slot_reserved = False
                            return False
                    _trades.append(t)
                    if _exec_mode == "live" and _live_slot_reserved:
                        _consume_live_slot_reservation(ctx, _trades, t, _exec_mode)
                        _live_slot_reserved = False
                if ctx is not None:
                    ctx.stats["opened"] = ctx.stats.get("opened", 0) + 1
                print(f"[ENTRY CREATED] {symbol} {t['side']} entry={round(t.get('entry', 0), 6)} sl={round(t.get('sl', 0), 6)} entry_time={t.get('entry_time')}")
                log_entry_clean(
                    symbol=t["symbol"], direction=t["side"],
                    score=t.get("score", 0), state=t.get("market_mode", "UNKNOWN"),
                    structure="BOS" if t.get("bos_type") in ("CONFIRM","STRONG") else ("Near" if t.get("bos_type") in ("EARLY","NEAR") else "None"),
                    volume_status=t.get("volume_state", "OK"),
                    position_status="Near" if t.get("dist_to_level", 1) <= 0.005 else "Mid",
                    entry_type=t.get("entry_type","")
                )
                stats["sent"] += 1
                # â”€â”€ STEP 1: MARKET ENTRY â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                if _exec_mode == "live":
                    _allowed, _reason = _check_live_runtime_safety_gate(
                        _tn,
                        t,
                        _trades,
                        t["risk_percent"],
                        current_total_risk,
                        leverage=_prep["leverage"],
                        ctx=ctx,
                        post_insert=True,
                    )
                    if not _allowed:
                        print(f"[LIVE SAFETY BLOCK] {symbol} before market order reason={_reason}")
                        send_telegram(
                            f"[LIVE SAFETY BLOCK] {symbol}\nreason={_reason}",
                            prefix=_mode_prefix,
                            channel="alerts",
                        )
                        with _lock:
                            if t in _trades:
                                _trades.remove(t)
                            if _live_slot_reserved:
                                _release_live_slot_reservation(
                                    ctx, _trades, t, _exec_mode, "live_runtime_safety_gate_reject"
                                )
                                _live_slot_reserved = False
                        save_open_trades(_trades, _state_file)
                        return False
                _entry_client_order_id = None
                if _exec_mode == "live" and hasattr(_tn, "_bot_client_order_id"):
                    _entry_client_order_id = _tn._bot_client_order_id(symbol, "E")
                    t["exchange_client_id"] = _entry_client_order_id
                    t["client_order_id"] = _entry_client_order_id
                try:
                    _entry_result = _tn.place_market_order(
                        symbol=symbol,
                        side=_side_str,
                        qty=_prep["qty"],
                        leverage=_prep["leverage"],
                        client_order_id=_entry_client_order_id,
                    )
                except Exception as _entry_exc:
                    if _exec_mode == "live" and _entry_client_order_id:
                        _mark_live_entry_uncertain(
                            t,
                            _entry_client_order_id,
                            f"market_order_exception:{type(_entry_exc).__name__}",
                        )
                        save_open_trades(_trades, _state_file)
                        _resolution = _resolve_live_uncertain_entry(t, _tn, prefix=_mode_prefix)
                        if _resolution.get("state") == "confirmed":
                            _entry_result = _resolution["entry_result"]
                        elif _resolution.get("state") == "not_found":
                            print(
                                f"[LIVE ENTRY NOT FOUND] {symbol} removing local state "
                                "after authoritative query"
                            )
                            with _lock:
                                if t in _trades:
                                    _trades.remove(t)
                            save_open_trades(_trades, _state_file)
                            return False
                        else:
                            save_open_trades(_trades, _state_file)
                            return True
                    else:
                        print(
                            f"[{_exec_mode.upper()} CRITICAL] {symbol} MARKET entry EXCEPTION — "
                            f"removing from local state before re-raise"
                        )
                        with _lock:
                            if t in _trades:
                                _trades.remove(t)
                            if _live_slot_reserved:
                                _release_live_slot_reservation(
                                    ctx, _trades, t, _exec_mode, "market_order_exception"
                                )
                                _live_slot_reserved = False
                        save_open_trades(_trades, _state_file)
                        raise
                t["exchange_order_id"]   = _entry_result.get("order_id")
                t["exchange_fill_price"] = _entry_result.get("fill_price")
                t["exchange_client_id"]  = _entry_result.get("client_order_id")

                if _exec_mode == "live" and _live_entry_result_is_uncertain(_entry_result):
                    _uncertain_cid = _entry_result.get("client_order_id") or _entry_client_order_id
                    _mark_live_entry_uncertain(t, _uncertain_cid, _entry_result.get("error_code", "order_state_unknown"))
                    save_open_trades(_trades, _state_file)
                    send_telegram(
                        f"[LIVE ENTRY UNCERTAIN] {symbol}\n"
                        f"clientOrderId={_uncertain_cid}\n"
                        "Market POST state unknown; querying exchange before any retry",
                        prefix=_mode_prefix,
                        channel="alerts",
                    )
                    _resolution = _resolve_live_uncertain_entry(t, _tn, prefix=_mode_prefix)
                    if _resolution.get("state") == "confirmed":
                        _entry_result = _resolution["entry_result"]
                    elif _resolution.get("state") == "not_found":
                        print(
                            f"[LIVE ENTRY NOT FOUND] {symbol} removing local state "
                            "after authoritative query"
                        )
                        with _lock:
                            if t in _trades:
                                _trades.remove(t)
                        save_open_trades(_trades, _state_file)
                        return False
                    else:
                        save_open_trades(_trades, _state_file)
                        return True

                t["exchange_order_id"]   = _entry_result.get("order_id")
                t["exchange_fill_price"] = _entry_result.get("fill_price")
                t["exchange_client_id"]  = _entry_result.get("client_order_id")

                if not _entry_result.get("success"):
                    print(
                        f"[{_exec_mode.upper()} CRITICAL] {symbol} MARKET entry FAILED — "
                        f"removing from local state. error={_entry_result.get('error')}"
                    )
                    send_telegram(
                        f"💀 {symbol} entry FAILED\nNo position opened",
                        prefix=_mode_prefix,
                    )
                    with _lock:
                        if t in _trades:
                            _trades.remove(t)
                        if _live_slot_reserved:
                            _release_live_slot_reservation(
                                ctx, _trades, t, _exec_mode, "market_order_failed"
                            )
                            _live_slot_reserved = False
                    save_open_trades(_trades, _state_file)
                    return False

                t["exchange_qty"] = _entry_result.get("fill_qty") or _prep["qty"]
                if _exec_mode == "live":
                    t["entry_state"] = "ENTRY_CONFIRMED"
                    t["exchange_order_state_unknown"] = False

                # â”€â”€ BOT OWNERSHIP CONFIRMATION (exchange-level proof) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                # Persist the BOT_-prefixed clientOrderId returned by the exchange fill
                # as canonical, exchange-verifiable ownership proof.  This allows:
                #   - reconcile_exchange_positions()  to skip non-bot entries
                #   - audit_exchange_sl()             to skip non-bot entries
                #   - update_trades()                 to skip non-bot entries
                #   - slot / risk accounting          to exclude non-bot entries
                #
                # client_order_id stored here is the authoritative field; the legacy
                # t["exchange_client_id"] is preserved for backward-compat.
                #
                # exchange_position_owner_confirmed = True iff the confirmed exchange fill
                # clientOrderId carries the BOT_ prefix generated by _bot_client_order_id().
                # False means the clientOrderId was absent or non-BOT-prefixed — rare edge
                # case (e.g. testnet quirk).  owner = "bot" is set unconditionally either
                # way; this flag is additional exchange-level evidence.
                _entry_cid = _entry_result.get("client_order_id") or t.get("exchange_client_id") or ""
                t["client_order_id"] = _entry_cid
                t["exchange_position_owner_confirmed"] = _entry_cid.startswith("BOT_")
                if not t["exchange_position_owner_confirmed"]:
                    print(
                        f"[OWNERSHIP] {symbol} entry clientOrderId={_entry_cid!r} "
                        f"missing BOT_ prefix — exchange-level ownership NOT confirmed. "
                        f"owner='bot' is set but exchange proof absent."
                    )
                else:
                    print(
                        f"[OWNERSHIP] {symbol} clientOrderId={_entry_cid!r} "
                        f"confirmed BOT-owned — ownership verified on exchange."
                    )

                # â”€â”€ STEP 2: STOP PLACEMENT (retry + emergency close) â”€â”€â”€â”€â”€
                if t.get("sl"):
                    _sl_result = _tn.place_stop_loss(
                        symbol=symbol,
                        entry_side=_side_str,
                        qty=_prep["qty"],
                        stop_price=t["sl"],
                    )

                    if not _sl_result.get("success"):
                        print(
                            f"[{_exec_mode.upper()} CRITICAL] {symbol} STOP placement FAILED — "
                            f"querying exchange before retry. error={_sl_result.get('error')}"
                        )
                        send_telegram(
                            f"💀 {symbol} STOP failed — querying exchange before retry...",
                            prefix=_mode_prefix,
                        )

                        time.sleep(0.5)

                        _open_algos = _tn.get_open_algo_orders(symbol) if hasattr(_tn, "get_open_algo_orders") else []

                        if _open_algos is None:
                            t["sl_verification_uncertain"] = True
                            send_telegram(
                                f"⚠️ {symbol} SL state UNCERTAIN — algo query failed.\n"
                                f"Stop may exist on exchange. No emergency close.\n"
                                f"Manual check required. Periodic audit will attempt repair.",
                                prefix=_mode_prefix,
                                channel="alerts",
                            )
                            print(
                                f"[{_exec_mode.upper()} CRITICAL] {symbol} algo query FAILED — "
                                f"SL state uncertain, no emergency close"
                            )
                            t["exchange_sl_id"] = None
                            for _nt in _trades:
                                normalize_trade_schema(_nt)
                            save_open_trades(_trades, _state_file)
                            _sl_synced = False
                            if _exec_mode == "live":
                                send_live_entry(t, prefix=_mode_prefix, sl_synced=_sl_synced)
                            else:
                                send_testnet_entry(t, prefix=_mode_prefix, sl_synced=_sl_synced)
                            return True

                        _existing_sl = next((o for o in _open_algos if o.get("symbol") == symbol), None)

                        if _existing_sl:
                            t["exchange_sl_id"] = _existing_sl.get("algoId")
                            print(
                                f"[{_exec_mode.upper()}] {symbol} STOP already on exchange "
                                f"algoId={t['exchange_sl_id']} — rebound, no retry needed"
                            )
                            send_telegram(
                                f"⚠️ {symbol} STOP rebound from exchange algoId={t['exchange_sl_id']}",
                                prefix=_mode_prefix,
                                channel="alerts",
                            )
                            _sl_result = {"success": True, "order_id": t["exchange_sl_id"]}
                        else:
                            _sl_result = _tn.place_stop_loss(
                                symbol=symbol,
                                entry_side=_side_str,
                                qty=_prep["qty"],
                                stop_price=t["sl"],
                            )

                        if not _sl_result.get("success"):
                            print(
                                f"[{_exec_mode.upper()} CRITICAL] {symbol} STOP retry FAILED — "
                                f"emergency close position"
                            )
                            send_telegram(
                                f"💀 STOP sync failed\n{symbol} emergency close executed",
                                prefix=_mode_prefix,
                            )
                            _tn.emergency_close_position(
                                symbol=symbol,
                                entry_side=_side_str,
                                qty=_prep["qty"],
                            )
                            if ctx is not None:
                                ctx.emergency_close_count += 1
                            with _lock:
                                if t in _trades:
                                    _trades.remove(t)
                            save_open_trades(_trades, _state_file)
                            return False

                        print(
                            f"[{_exec_mode.upper()} CRITICAL] {symbol} STOP retry SUCCEEDED — "
                            f"position protected"
                        )
                        send_telegram(
                            f"[CRITICAL] {symbol} STOP retry OK — position protected",
                            prefix=_mode_prefix,
                            channel="alerts",
                        )

                    t["exchange_sl_id"] = _sl_result.get("order_id")

                _sl_synced = t.get("exchange_sl_id") is not None
                if _exec_mode == "live" and _sl_synced:
                    with _lock:
                        if ctx is not None:
                            _trades = ctx.trades
                        for _nt in _trades:
                            normalize_trade_schema(_nt)
                        save_open_trades(_trades, _state_file)
                if _exec_mode == "live":
                    send_live_entry(t, prefix=_mode_prefix, sl_synced=_sl_synced)
                else:
                    send_testnet_entry(t, prefix=_mode_prefix, sl_synced=_sl_synced)
    # ===== END EXCHANGE EXECUTION GATE =====

    with _lock:
        if ctx is not None:
            _trades = ctx.trades
        for t in _trades:
            normalize_trade_schema(t)
        save_open_trades(_trades, _state_file)
    return True

def get_trade_risk(t):
    return t.get("risk_percent", RISK_PER_TRADE)

def apply_adaptive_trailing_floor(t):
    entry = t.get("entry_real") or t.get("entry")
    risk = abs(entry - t.get("sl_init", entry))
    max_r = t.get("max_profit_r", 0)

    if risk <= 0 or max_r < 1.0:
        return False

    if max_r >= 4.0:
        floor_r = 3.0
    elif max_r >= 3.0:
        floor_r = 2.0
    elif max_r >= 2.0:
        floor_r = 1.0
    else:
        floor_r = 0.5

    if t["side"] == "LONG":
        floor_sl = entry + risk * floor_r
        if floor_sl > t["sl"]:
            t["sl"] = floor_sl
            print(f"[ADAPTIVE TRAIL FLOOR] {t['symbol']} SL -> {round(t['sl'], 6)} (+{floor_r}R)")
            return True
    else:
        floor_sl = entry - risk * floor_r
        if floor_sl < t["sl"]:
            t["sl"] = floor_sl
            print(f"[ADAPTIVE TRAIL FLOOR] {t['symbol']} SL -> {round(t['sl'], 6)} (+{floor_r}R)")
            return True

    return False

def _sync_testnet_trailing_sl(t, ctx, old_sl=None):
    # Returns: None (paper/no-op), True (sync confirmed), False (sync failed)
    if ctx is None or ctx.execution_mode not in ("testnet", "live"):
        return None
    old_sl_id = t.get("exchange_sl_id")
    if not old_sl_id:
        return None
    qty = t.get("exchange_qty")
    if not qty:
        return None
    _tn = _resolve_exchange_executor(ctx.execution_mode)
    _side_str = "BUY" if t["side"] == "LONG" else "SELL"
    new_sl = t["sl"]
    _old_sl_str = round(old_sl, 6) if old_sl is not None else "?"
    print(
        f"[TESTNET TRAIL] {t['symbol']} Updating stop: "
        f"{_old_sl_str} -> {round(new_sl, 6)}"
    )
    result = _tn.update_trailing_stop(
        symbol=t["symbol"],
        entry_side=_side_str,
        qty=qty,
        new_stop_price=new_sl,
        old_order_id=old_sl_id,
    )
    if result.get("success"):
        t["exchange_sl_id"] = result["new_order_id"]
        t["exchange_sl_price_confirmed"] = new_sl
        t.pop("exchange_sl_sync_pending", None)
        # FIX 5: if old cancel failed, track the orphaned ID for periodic cleanup
        if not result.get("cancel_ok") and old_sl_id:
            _orphans = t.setdefault("orphan_stop_ids", [])
            if old_sl_id not in _orphans:
                _orphans.append(old_sl_id)
        print(
            f"[TESTNET TRAIL] {t['symbol']} New stop confirmed "
            f"orderId={result['new_order_id']} stopPrice={round(new_sl, 6)}"
        )
        return True
    else:
        # FIX 2b: record intended SL for audit resync
        t["exchange_sl_sync_pending"] = new_sl
        print(
            f"[TESTNET CRITICAL] {t['symbol']} Stop update failed — "
            f"Old protection retained at {_old_sl_str}. error={result.get('error')}"
        )
        send_telegram(
            f"[TESTNET CRITICAL] {t['symbol']} Stop update failed — "
            f"Old protection retained at {_old_sl_str}",
            prefix=ctx.mode_prefix,
            channel="alerts",
        )
        return False


def _cancel_live_remaining_stop(t: dict, live_executor, prefix=None) -> bool:
    symbol = t.get("symbol", "UNKNOWN")
    stop_ids = []
    if t.get("exchange_sl_id"):
        stop_ids.append(t.get("exchange_sl_id"))
    elif hasattr(live_executor, "get_open_algo_orders"):
        open_algos = live_executor.get_open_algo_orders(symbol)
        if open_algos is None:
            t["exchange_stop_cancel_error"] = "open algo query failed"
            send_telegram(
                f"[LIVE CLOSE BLOCK] {symbol} close confirmed but stop query failed",
                prefix=prefix,
                channel="alerts",
            )
            return False
        stop_ids.extend(
            o.get("algoId") for o in open_algos
            if isinstance(o, dict) and o.get("algoId")
        )

    if not stop_ids:
        t["exchange_stop_cancelled"] = True
        return True

    for stop_id in stop_ids:
        result = live_executor.cancel_stop_loss(symbol, stop_id)
        if not result.get("success"):
            t["exchange_stop_cancel_error"] = result.get("error", "unknown")
            send_telegram(
                f"[LIVE CLOSE BLOCK] {symbol} close confirmed but stop cancel failed\n"
                f"algoId={stop_id}\nerror={t['exchange_stop_cancel_error']}",
                prefix=prefix,
                channel="alerts",
            )
            return False

    t["exchange_stop_cancelled"] = True
    t["exchange_sl_id"] = None
    t.pop("exchange_stop_cancel_error", None)
    return True


def _close_live_exchange_position_for_local_exit(t: dict, ctx, exit_type: str) -> bool:
    if ctx is None or ctx.execution_mode != "live":
        return True

    symbol = t.get("symbol", "UNKNOWN")
    live_executor = _resolve_exchange_executor("live")

    if not t.get("exchange_close_confirmed"):
        qty = t.get("exchange_qty")
        if not qty:
            t["exchange_close_error"] = "missing exchange_qty"
            send_telegram(
                f"[LIVE CLOSE BLOCK] {symbol} local {exit_type} but exchange_qty missing",
                prefix=ctx.mode_prefix,
                channel="alerts",
            )
            return False

        side_str = "BUY" if t.get("side") == "LONG" else "SELL"
        print(f"[LIVE CLOSE] {symbol} local {exit_type} -> reduceOnly MARKET close")
        result = live_executor.emergency_close_position(
            symbol=symbol,
            entry_side=side_str,
            qty=qty,
        )
        if not result.get("success"):
            verifier = getattr(live_executor, "is_position_closed", None)
            already_closed = verifier(symbol) if verifier else None
            if already_closed is True:
                t["exchange_close_confirmed"] = True
                t["exchange_close_order_id"] = None
                t["exchange_close_client_id"] = None
                t.pop("exchange_close_error", None)
                print(
                    f"[LIVE CLOSE] {symbol} reduceOnly failed but position confirmed closed "
                    f"on exchange (SL already triggered) — proceeding to stop cancel"
                )
            else:
                t["exchange_close_error"] = result.get("error", "unknown")
                send_telegram(
                    f"[LIVE CLOSE BLOCK] {symbol} reduceOnly close failed\n"
                    f"exit={exit_type}\nerror={t['exchange_close_error']}",
                    prefix=ctx.mode_prefix,
                    channel="alerts",
                )
                return False

        verifier = getattr(live_executor, "is_position_closed", None)
        verified = verifier(symbol) if verifier else None
        if not t.get("exchange_close_confirmed") and verified is not True:
            t["exchange_close_error"] = "position close verification failed"
            send_telegram(
                f"[LIVE CLOSE BLOCK] {symbol} reduceOnly close sent but verification failed",
                prefix=ctx.mode_prefix,
                channel="alerts",
            )
            return False

        t["exchange_close_confirmed"] = True
        t["exchange_close_order_id"] = result.get("order_id")
        t["exchange_close_client_id"] = result.get("client_order_id")
        t.pop("exchange_close_error", None)

    if not _cancel_live_remaining_stop(t, live_executor, prefix=ctx.mode_prefix):
        return False

    return True


def audit_exchange_sl(ctx):
    """
    P3 — Exchange SL Guarantee System.

    Invariant: every OPEN testnet trade with an exchange position MUST have
    a valid exchange_sl_id.  Run once at startup and can be called any time.

    For each OPEN testnet trade:
      - If exchange_sl_id is None AND exchange_qty is set → attempt repair.
      - On failure → CRITICAL alert.
    """
    if ctx is None or ctx.execution_mode not in ("testnet", "live"):
        return

    _tn = _resolve_exchange_executor(ctx.execution_mode)

    # SL audit operates on bot-owned positions only.  Manual positions on the
    # same account have no entry in ctx.trades.  If a non-bot entry somehow
    # exists in ctx.trades it must not be subject to SL repair or quarantine.
    open_trades = [
        t for t in ctx.trades
        if t.get("status") == "OPEN"
        and not t.get("quarantined")
        and not t.get("repair_disabled")
        and t.get("owner", "bot") == "bot"
    ]
    _skipped = sum(
        1 for t in ctx.trades
        if t.get("status") == "OPEN" and (t.get("quarantined") or t.get("repair_disabled"))
    )
    _skipped_manual = sum(
        1 for t in ctx.trades
        if t.get("status") == "OPEN"
        and not t.get("quarantined")
        and not t.get("repair_disabled")
        and t.get("owner", "bot") != "bot"
    )
    print(
        f"[SL AUDIT] Checking {len(open_trades)} open {ctx.execution_mode} trade(s) "
        f"({_skipped} quarantined/repair_disabled skipped"
        f"{f', {_skipped_manual} non-bot skipped' if _skipped_manual else ''})..."
    )

    for t in open_trades:
        symbol = t.get("symbol", "UNKNOWN")
        sl_id  = t.get("exchange_sl_id")
        qty    = t.get("exchange_qty")

        # FIX 2c: resync if a previous sync attempt failed before checking anything else
        if t.get("exchange_sl_sync_pending") and sl_id and qty:
            _pending_sl = t["exchange_sl_sync_pending"]
            print(f"[SL AUDIT] {symbol} exchange_sl_sync_pending={round(_pending_sl, 6)} — forcing resync")
            _pending_sync_ok = _sync_testnet_trailing_sl(t, ctx, old_sl=t.get("exchange_sl_price_confirmed"))
            if _pending_sync_ok:
                save_open_trades(ctx.trades, ctx.state_file)
                print(f"[SL AUDIT] {symbol} pending sync resolved → {round(_pending_sl, 6)}")
                sl_id = t.get("exchange_sl_id")  # refresh after successful sync
            else:
                print(f"[SL AUDIT] {symbol} pending sync retry FAILED — will retry next audit")

        if sl_id:
            if hasattr(_tn, "query_algo_order"):
                _order_data = _tn.query_algo_order(symbol, sl_id)
                if _order_data is not None:
                    print(f"[SL AUDIT] {symbol} algoId={sl_id} verified active on exchange")
                    # FIX 2c: verify exchange stop price matches local t["sl"]
                    _exch_trigger = _order_data.get("triggerPrice")
                    if _exch_trigger is not None:
                        _exch_price = float(_exch_trigger)
                        _local_sl   = t.get("sl", 0)
                        _confirmed  = t.get("exchange_sl_price_confirmed")
                        if abs(_exch_price - _local_sl) > 1e-9 and _confirmed != _local_sl:
                            print(
                                f"[SL AUDIT] {symbol} PRICE MISMATCH "
                                f"exchange={round(_exch_price, 6)} local={round(_local_sl, 6)} "
                                f"— scheduling resync"
                            )
                            t["exchange_sl_sync_pending"] = _local_sl
                    continue
                _all_algos = _tn.get_open_algo_orders(symbol) if hasattr(_tn, "get_open_algo_orders") else None
                if _all_algos is None:
                    print(f"[SL AUDIT] {symbol} algo query FAILED — cannot verify, skipping repair")
                    send_telegram(
                        f"[SL AUDIT] ⚠️ {symbol} stop verification UNCERTAIN\n"
                        f"Exchange unreachable — manual check required",
                        prefix=ctx.mode_prefix,
                        channel="alerts",
                    )
                    continue
                _found_any = next((o for o in _all_algos if o.get("symbol") == symbol), None)
                if _found_any:
                    t["exchange_sl_id"] = _found_any.get("algoId")
                    save_open_trades(ctx.trades, ctx.state_file)
                    print(f"[SL AUDIT] {symbol} stop rebound to algoId={t['exchange_sl_id']}")
                    send_telegram(
                        f"[SL AUDIT] {symbol} stop rebound to algoId={t['exchange_sl_id']}",
                        prefix=ctx.mode_prefix,
                        channel="alerts",
                    )
                    continue
                print(
                    f"[SL AUDIT] {symbol} algoId={sl_id} not open on exchange "
                    f"-- checking position before any repair"
                )
                # Preserve the last known SL id until the position check decides
                # whether this was a normal SL fill, an uncertain exchange state,
                # or a true naked open position.
            else:
                print(f"[SL AUDIT] {symbol} exchange_sl_id={sl_id} OK (query unavailable)")
                continue

        if not qty:
            print(f"[SL AUDIT] {symbol} no exchange_qty — cannot repair (may not be exchange-filled)")
            continue

        if t.get("invalid_exchange_symbol"):
            print(f"[SL AUDIT] {symbol} invalid_exchange_symbol flag set — skipping (legacy guard)")
            continue

        # â”€â”€ POSITION EXISTENCE CHECK â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # Before attempting any SL repair, verify the exchange position still exists.
        # A missing SL algo order after a normal SL hit means the position was already
        # closed by the exchange — NOT that it is naked/unprotected.
        # Attempting to place a stop on a closed position produces Binance -4509
        # "TIF GTE can only be used with open positions", which previously caused
        # a false permanent quarantine.
        #
        # Race condition this guards against:
        #   T+0  Exchange SL triggers → position closed
        #   T+1  audit_exchange_sl() runs
        #   T+2  query_algo_order() → None (SL order consumed by fill)
        #   T+3  get_open_algo_orders() → [] (no open algos)
        #   T+4  [OLD] code falls through to repair → -4509 → quarantine  ← BUG
        #   T+4  [NEW] is_position_closed() → True → close trade normally  ← FIX
        if hasattr(_tn, "is_position_closed"):
            _pos_closed = _tn.is_position_closed(symbol)
            if _pos_closed is True:
                # Position is confirmed closed on exchange — SL was hit normally.
                # Close the local trade record cleanly; do NOT quarantine, do NOT repair.
                print(
                    f"[SL AUDIT] {symbol} position confirmed CLOSED on exchange "
                    f"— SL was hit normally. Closing local trade record."
                )
                send_telegram(
                    f"[SL AUDIT] {symbol} position closed on exchange\n"
                    f"SL hit detected — closing local record (no quarantine)",
                    prefix=ctx.mode_prefix,
                    channel="alerts",
                )
                _finalize_audit_exchange_sl_close(t, ctx, source="missing_algo_position_closed")
                continue
            elif _pos_closed is None:
                # Exchange query failed — cannot determine position state.
                # Skip repair this cycle; audit will retry on next interval.
                print(
                    f"[SL AUDIT] {symbol} position existence check FAILED "
                    f"— skipping repair this cycle (will retry)"
                )
                send_telegram(
                    f"[SL AUDIT] ⚠️ {symbol} position check uncertain\n"
                    f"Exchange unreachable — repair deferred to next audit",
                    prefix=ctx.mode_prefix,
                    channel="alerts",
                )
                continue
            # _pos_closed is False → position still open but SL is missing → proceed to repair

        if sl_id:
            t["exchange_sl_id"] = None
            sl_id = None

        from exchange.precision import get_symbol_filters as _get_sym_filters
        _sym_filters = _get_sym_filters(symbol)
        if _sym_filters is None:
            print(f"[SL AUDIT] {symbol} Invalid exchange symbol — permanently quarantining")
            _newly = _quarantine_trade(t, "invalid_symbol")
            t["invalid_exchange_symbol"] = True
            if _newly:
                send_telegram(
                    f"[RECON] {symbol} permanently quarantined\nreason=invalid_symbol",
                    prefix=ctx.mode_prefix,
                    channel="alerts",
                )
                save_open_trades(ctx.trades, ctx.state_file)
            continue

        _repair_now = time.time()
        _last_repair = _sl_repair_cooldown.get(symbol, 0)
        _effective_cooldown = 120 if ctx.execution_mode == "live" else _SL_REPAIR_COOLDOWN_SECS
        if _repair_now - _last_repair < _effective_cooldown and _last_repair > 0:
            _mins_left = int((_effective_cooldown - (_repair_now - _last_repair)) / 60)
            print(f"[SL AUDIT] {symbol} Repair cooldown active — skipping (retry in ~{_mins_left}m)")
            continue
        _sl_repair_cooldown[symbol] = _repair_now

        print(f"[SL AUDIT] {symbol} stop confirmed MISSING and position is OPEN -- triggering repair")
        send_telegram(
            f"[SL AUDIT] {symbol} stop MISSING on exchange\n"
            f"position still open -- triggering repair now",
            prefix=ctx.mode_prefix,
            channel="alerts",
        )

        print(f"[SL AUDIT] {symbol} Missing exchange SL — position confirmed open — Repairing...")
        send_telegram(
            f"[SL AUDIT] {symbol} Missing exchange SL — Repairing...",
            prefix=ctx.mode_prefix,
            channel="alerts",
        )

        _side_str = "BUY" if t.get("side") == "LONG" else "SELL"
        _stop_price = t.get("sl", 0)

        result = _tn.place_stop_loss(
            symbol=symbol,
            entry_side=_side_str,
            qty=qty,
            stop_price=_stop_price,
        )

        if result.get("success"):
            t["exchange_sl_id"] = result.get("order_id")
            save_open_trades(ctx.trades, ctx.state_file)
            print(f"[SL AUDIT] {symbol} Repair successful algoId={t['exchange_sl_id']}")
            send_telegram(
                f"[SL AUDIT] {symbol} Repair successful algoId={t['exchange_sl_id']}",
                prefix=ctx.mode_prefix,
                channel="alerts",
            )
        else:
            _err = result.get("error", "unknown")
            print(f"[SL AUDIT] CRITICAL {symbol} Stop placement failed — error={_err}")
            # Final safety check: if the repair itself fails with a position-closed error,
            # do not quarantine — the position may have closed between the existence check
            # and the repair attempt (second race window). Check once more before quarantining.
            _recheck_closed = False
            if hasattr(_tn, "is_position_closed"):
                _recheck = _tn.is_position_closed(symbol)
                if _recheck is True:
                    _recheck_closed = True
                    print(
                        f"[SL AUDIT] {symbol} repair failed but position now confirmed CLOSED "
                        f"— SL hit during repair window. Closing local record (no quarantine)."
                    )
                    send_telegram(
                        f"[SL AUDIT] {symbol} repair failed — position closed during repair\n"
                        f"Closing local record (no quarantine)",
                        prefix=ctx.mode_prefix,
                        channel="alerts",
                    )
                    _finalize_audit_exchange_sl_close(t, ctx, source="repair_failed_position_closed")
            if not _recheck_closed:
                _newly = _quarantine_trade(t, "unrecoverable_stop_failure")
                if _newly:
                    send_telegram(
                        f"[SL AUDIT] CRITICAL {symbol} permanently quarantined\n"
                        f"reason=unrecoverable_stop_failure\nerror={_err}\n"
                        f"Manual intervention required!",
                        prefix=ctx.mode_prefix,
                        channel="alerts",
                    )
                    save_open_trades(ctx.trades, ctx.state_file)

    # ===== FIX 5: ORPHAN STOP CLEANUP =====
    # Cancel old stop IDs where the new stop was placed successfully but the
    # old stop cancel failed (cancel_ok=False).  These are safe to cancel because
    # the active stop (exchange_sl_id) is always excluded from this pass.
    for t in open_trades:
        _orphans = t.get("orphan_stop_ids")
        if not _orphans:
            continue
        _symbol_o = t.get("symbol", "UNKNOWN")
        _current_sl_id = t.get("exchange_sl_id")
        _cleaned = []
        _changed = False
        for _oid in list(_orphans):
            if _oid == _current_sl_id:
                # Never cancel the currently active stop
                _cleaned.append(_oid)
                continue
            # Verify order still exists before cancelling
            _still_open = (
                _tn.query_algo_order(_symbol_o, _oid)
                if hasattr(_tn, "query_algo_order")
                else None
            )
            if _still_open is None:
                # Already gone (triggered or previously cancelled) — drop from list
                print(f"[ORPHAN CLEANUP] {_symbol_o} algoId={_oid} already gone — removing")
                _changed = True
                continue
            # Order still open on exchange — attempt cancel
            _cxl = (
                _tn.cancel_stop_loss(symbol=_symbol_o, order_id=_oid)
                if hasattr(_tn, "cancel_stop_loss")
                else {"success": False}
            )
            if _cxl.get("success"):
                print(f"[ORPHAN CLEANUP] {_symbol_o} algoId={_oid} cancelled successfully")
                _changed = True
            else:
                print(f"[ORPHAN CLEANUP] {_symbol_o} algoId={_oid} cancel failed - retaining for next audit")
                _cleaned.append(_oid)
        t["orphan_stop_ids"] = _cleaned
        if _changed:
            save_open_trades(ctx.trades, ctx.state_file)


def reconcile_exchange_positions(ctx):
    """
    P4 — Phantom Position Reconciliation.

    Exchange state is authoritative.  On startup:
      - Query live exchange positions.
      - Compare against local OPEN trades.
      - For each local trade with no exchange position → quarantine it.
      - Log all results.
    """
    if ctx is None or ctx.execution_mode not in ("testnet", "live"):
        return

    _tn = _resolve_exchange_executor(ctx.execution_mode)

    print(f"[RECON] Starting exchange reconciliation for {ctx.name}...")

    exchange_positions = _tn.get_exchange_positions()
    if exchange_positions is None:
        print(
            f"[RECON] {ctx.name} exchange position query failed or was ambiguous - "
            "skipping reconciliation; local LIVE state preserved"
        )
        send_telegram(
            "[RECON] Exchange position query uncertain\n"
            "Local state preserved; retry reconciliation after exchange recovers",
            prefix=ctx.mode_prefix,
            channel="alerts",
        )
        return

    # Reconcile only bot-owned positions against the exchange.
    # Manual positions opened directly on the account are expected to appear
    # as exchange_only entries — they are logged but NEVER quarantined or
    # managed by the bot.  The local_positions list must not include them.
    open_trades = [
        t for t in ctx.trades
        if t.get("status") == "OPEN"
        and not t.get("quarantined")
        and not t.get("repair_disabled")
        and t.get("owner", "bot") == "bot"
    ]
    _skipped_q = sum(
        1 for t in ctx.trades
        if t.get("status") == "OPEN" and (t.get("quarantined") or t.get("repair_disabled"))
    )
    _skipped_manual = sum(
        1 for t in ctx.trades
        if t.get("status") == "OPEN"
        and not t.get("quarantined")
        and not t.get("repair_disabled")
        and t.get("owner", "bot") != "bot"
    )
    if _skipped_q:
        print(f"[RECON] {_skipped_q} quarantined/repair_disabled trade(s) excluded from comparison")
    if _skipped_manual:
        print(f"[RECON] {_skipped_manual} non-bot trade(s) excluded from comparison (ownership guard)")

    local_positions = [
        {
            "symbol":    t.get("symbol"),
            "direction": t.get("side"),
            "qty":       t.get("exchange_qty") or 0,
            "entry":     t.get("entry_real") or t.get("entry", 0),
        }
        for t in open_trades
    ]

    result = _tn.compare_local_vs_exchange(local_positions, exchange_positions)

    for item in result.get("matched", []):
        sym = item["local"]["symbol"]
        print(f"[RECON] {sym} matched - OK")

    _state_dirty = False
    _alert_exchange_only = []
    _alert_bot_orphans = []
    _alert_exchange_only_uncertain = []
    _alert_local_only = []
    _alert_discrepancies = []

    for item in result.get("local_only", []):
        sym = item["local"]["symbol"]
        print(f"[RECON] {sym} Local position found Exchange position absent - quarantining")
        for t in ctx.trades:
            if t.get("symbol") == sym and t.get("status") == "OPEN":
                _newly = _quarantine_trade(t, "orphan_local_trade")
                if _newly:
                    _alert_local_only.append(sym)
                    _state_dirty = True

    for item in result.get("exchange_only", []):
        sym = item["exchange"]["symbol"]
        amt = item["exchange"]["positionAmt"]
        if ctx.execution_mode == "live":
            _bot_evidence = _inspect_exchange_only_live_bot_evidence(item["exchange"], _tn)
            _bot_status = _bot_evidence.get("status")
            _bot_reason = _bot_evidence.get("reason", "")
            if _bot_status == "bot_orphan_needs_reconstruction":
                _entry_order = _bot_evidence.get("entry_order") or {}
                _client_id = _get_order_client_id(_entry_order)
                print(
                    f"[RECON] {sym} Exchange-only position amt={amt} has BOT_ entry evidence "
                    f"clientOrderId={_client_id} - CRITICAL reconstruction required; "
                    "exchange untouched"
                )
                _alert_bot_orphans.append(
                    f"{sym} amt={amt} [BOT_ORPHAN_NEEDS_RECONSTRUCTION] "
                    f"clientOrderId={_client_id or '?'}"
                )
                continue
            if _bot_status == "uncertain":
                print(
                    f"[RECON] {sym} Exchange-only position amt={amt} ownership evidence "
                    f"UNCERTAIN reason={_bot_reason} - exchange untouched"
                )
                _alert_exchange_only_uncertain.append(
                    f"{sym} amt={amt} [OWNERSHIP_UNCERTAIN] reason={_bot_reason}"
                )
                continue
        # Exchange-only positions are NEVER quarantined, managed, trailed, or closed by
        # the bot.  They are presumed to be manual trades opened directly on the account
        # (which is explicitly supported — manual + bot trades may coexist on the same
        # Binance Futures account).
        # The bot can never touch these positions because:
        #   1. They have no entry in ctx.trades → no bot code path reaches them.
        #   2. compare_local_vs_exchange() puts them in exchange_only, not local_only.
        #   3. Only local_only entries (bot-owned orphans) are quarantined below.
        # This block only logs + alerts so the operator can see the manual positions.
        print(
            f"[RECON] {sym} Exchange-only position amt={amt} - "
            f"presumed MANUAL trade, NOT managed by bot. Review if unexpected."
        )
        _alert_exchange_only.append(f"{sym} amt={amt} [MANUAL/UNTRACKED]")

    for item in result.get("discrepancies", []):
        sym = item["symbol"]
        exp = item["expected_signed_qty"]
        act = item["actual_signed_qty"]
        print(f"[RECON] {sym} qty mismatch local={exp} exchange={act}")
        _alert_discrepancies.append(f"{sym} local={exp} exchange={act}")

    if _state_dirty:
        save_open_trades(ctx.trades, ctx.state_file)

    _has_alerts = (
        _alert_exchange_only
        or _alert_bot_orphans
        or _alert_exchange_only_uncertain
        or _alert_local_only
        or _alert_discrepancies
    )
    if _has_alerts:
        _lines = ["[RECON SUMMARY]"]
        if _alert_bot_orphans:
            _lines.append("\nExchange-only BOT orphans (critical):")
            for _s in _alert_bot_orphans:
                _lines.append(f"  - {_s}")
            _lines.append("  action=manual_reconstruction_required exchange_untouched")
        if _alert_exchange_only_uncertain:
            _lines.append("\nExchange-only ownership uncertain:")
            for _s in _alert_exchange_only_uncertain:
                _lines.append(f"  - {_s}")
            _lines.append("  action=manual_review_required exchange_untouched")
        if _alert_exchange_only:
            _lines.append("\nExchange-only (orphans):")
            for _s in _alert_exchange_only:
                _lines.append(f"  - {_s}")
        if _alert_local_only:
            _lines.append("\nLocal-only (quarantined):")
            for _s in _alert_local_only:
                _lines.append(f"  - {_s}")
        if _alert_discrepancies:
            _lines.append("\nQty mismatches:")
            for _s in _alert_discrepancies:
                _lines.append(f"  - {_s}")
        _lines.append(f"\nMatched: {len(result.get('matched', []))}")
        send_telegram(
            "\n".join(_lines),
            prefix=ctx.mode_prefix,
            channel="alerts",
        )

    ctx.recon_orphan_count = len(result.get("exchange_only", []))
    print(
        f"[RECON] Done - matched={len(result.get('matched', []))} "
        f"quarantined={len(result.get('local_only', []))} "
        f"orphans={ctx.recon_orphan_count} "
        f"mismatches={len(result.get('discrepancies', []))}"
    )


def get_swing_low(df):
    """Phase 1 trailing - M5, 4 n?n g?n nh?t"""
    return df["low"].iloc[-5:-1].min()    # [FIX] 4 n?n, ??i x?ng v?i SHORT

def get_swing_high(df):
    """Phase 1 trailing - M5, 4 n?n g?n nh?t"""
    return df["high"].iloc[-5:-1].max()   # [FIX] 4 n?n, index nh?t qu?n

def get_swing_low_m15(df):
    """Phase 2 trailing - M15, 5 n?n"""
    return df["low"].iloc[-6:-1].min()    # [FIX] 5 n?n th?c s?

def get_swing_high_m15(df):
    """Phase 2 trailing - M15, 5 n?n"""
    return df["high"].iloc[-6:-1].max()   # [FIX] nh?t qu?n v?i LOW

def calc_volatility(df):
    return (df["high"] - df["low"]).rolling(10).mean().iloc[-1] / df["close"].iloc[-1]

def update_stat_dict(d, key, result):
    if key not in d:
        d[key] = {"win": 0, "loss": 0, "be": 0}

    if result == "WIN":
        d[key]["win"] += 1
    elif result == "LOSE":
        d[key]["loss"] += 1
    elif result == "BE":
        d[key]["be"] += 1

def update_trades(fast_mode=False, ctx=None):
    global trades, history, ACCOUNT_BALANCE, EQUITY_PEAK, pause_until
    global _paper_dd_warn_only_breach_active

    # ===== RESOLVE EXECUTOR STATE =====
    _update_started_ts = time.time()
    if ctx is not None:
        _trades = ctx.trades
        _cooldown = ctx.cooldown
        _entry_cooldown = ctx.entry_cooldown
        _state_file = ctx.state_file
        _trades_csv = ctx.trades_csv
        _mode_prefix = ctx.mode_prefix
        _account_balance = ctx.account_balance
        _equity_peak = ctx.equity_peak
        _pause_until = ctx.pause_until
        _exec_mode = ctx.execution_mode
    else:
        _trades = trades
        _cooldown = cooldown
        _entry_cooldown = entry_cooldown
        _state_file = None
        _trades_csv = None
        _mode_prefix = None
        _account_balance = ACCOUNT_BALANCE
        _equity_peak = EQUITY_PEAK
        _pause_until = pause_until
        _exec_mode = EXECUTION_MODE

    updated = False
    _now = time.time()
    if _exec_mode == "paper" and _pause_until > 0 and _now >= _pause_until:
        _equity_peak = _account_balance
        _pause_until = 0
        if ctx is not None:
            ctx.equity_peak = _equity_peak
            ctx.pause_until = _pause_until
        else:
            EQUITY_PEAK = _equity_peak
            pause_until = _pause_until
        config["pause_until"] = 0
        config["equity_peak"] = _equity_peak
        with open("config.tmp", "w", encoding="utf-8") as f:
            json.dump(_strip_secrets_for_save(config), f, indent=2, ensure_ascii=False)
        os.replace("config.tmp", "config.json")
        print(
            "[PAPER DD RESET] pause expired - "
            f"reset equity_peak to account_balance={round(_account_balance, 2)}"
        )

    if _equity_peak > 0:
        drawdown = (_equity_peak - _account_balance) / _equity_peak
    else:
        drawdown = 0
    if _exec_mode == "paper" and drawdown < MAX_DRAWDOWN:
        _paper_dd_warn_only_breach_active = False
    _trade_loop_id = f"{_exec_mode}:{int(_update_started_ts * 1000)}"
    for _symbol_loop_index, t in enumerate(_trades):
        try:
            risk = get_trade_risk(t)
            t.setdefault("tp_hit", False)
            t.setdefault("risk_percent", RISK_PER_TRADE)
            t.setdefault("exit_type", "")
            t.setdefault("trail_phase", 1)
            t.setdefault("be_early_done", False)
            t.setdefault("swing_lock_done", False)
            if drawdown >= MAX_DRAWDOWN:
                _dd_pause_mode = (
                    _paper_dd_pause_mode() if _exec_mode == "paper" else "ENFORCE"
                )
                if _exec_mode == "paper" and _dd_pause_mode == "WARN_ONLY":
                    if not _paper_dd_warn_only_breach_active:
                        _pause_until_before = _pause_until
                        row = {
                            "timestamp": datetime.now().astimezone().isoformat(),
                            "event_type": "PAPER_DD_WARN_ONLY_BREACH",
                            "account_balance": _account_balance,
                            "equity_peak": _equity_peak,
                            "current_drawdown_pct": drawdown * 100,
                            "max_drawdown_pct": MAX_DRAWDOWN * 100,
                            "paper_dd_pause_mode": _dd_pause_mode,
                            "execution_mode": _exec_mode,
                            "pause_until_before": _pause_until_before,
                            "pause_until_after": _pause_until,
                            "action_taken": "WARN_ONLY_NO_PAUSE",
                            "reason": "TEMP_RESEARCH_OVERRIDE",
                        }
                        _write_paper_dd_pause_event(row)
                        print(
                            f"[PAPER DD WARN ONLY] drawdown={drawdown * 100:.3f}% "
                            f">= {MAX_DRAWDOWN * 100:.3f}% - no pause applied"
                        )
                        _paper_dd_warn_only_breach_active = True
                        send_telegram(
                            "PAPER DD WARN_ONLY BREACH\n"
                            f"Drawdown: {drawdown * 100:.3f}%\n"
                            f"Threshold: {MAX_DRAWDOWN * 100:.3f}%\n"
                            "Action: WARN_ONLY_NO_PAUSE\n"
                            "Reason: TEMP_RESEARCH_OVERRIDE",
                            prefix=_mode_prefix,
                            channel="alerts",
                            dedup_key=telegram_dedup.build_key(
                                "dd_warn",
                                "PAPER_DD_WARN_ONLY_BREACH",
                                round(_equity_peak, 2),
                            ),
                        )
                elif time.time() > _pause_until:
                    _pause_until = time.time() + 3600 * 24
                    if ctx is not None:
                        ctx.pause_until = _pause_until
                    else:
                        pause_until = _pause_until
                    if ctx is None or ctx.execution_mode == "paper":
                        config["pause_until"] = _pause_until
                        with open("config.tmp", "w", encoding="utf-8") as f:
                            json.dump(_strip_secrets_for_save(config), f, indent=2, ensure_ascii=False)
                        os.replace("config.tmp", "config.json")
                    if ctx is not None and ctx.execution_mode in ("testnet", "live"):
                        ctx.save_account_state()
                    print(f"[DD TRIGGER] drawdown={round(drawdown*100,1)}% ≥ {MAX_DRAWDOWN*100}% — bot paused 24h")
                    send_telegram("💀 MAX DD HIT - PAUSE BOT 24H", prefix=_mode_prefix, channel="alerts")
    
            # [FIX] Guard: trade thi?u status ? coi nh? OPEN (compat JSON c?)
            if t.get("status", "OPEN") != "OPEN":
                continue

            if t.get("quarantined"):
                continue

            # ===== [LIVE ZOMBIE GUARD] Exchange-authoritative: live trade with no exchange_qty = orphan =====
            # Only applies to bot-owned trades.  Any non-bot entry in ctx.trades
            # (e.g. manually injected recovery record) is silently skipped so the
            # bot never manages, trails, exits, or quarantines manual positions.
            if _exec_mode == "live" and t.get("owner", "bot") != "bot":
                continue

            if _exec_mode == "live":
                if _missing_exchange_qty(t) and not t.get("exchange_close_confirmed"):
                    _sym_q = t.get("symbol", "UNKNOWN")
                    _side_q = t.get("side", "")
                    _entry_age = time.time() - float(t.get("entry_time") or 0)
                    if _entry_age < 300:
                        print(
                            f"[LIVE RECONCILE] {_sym_q} {_side_q} missing exchange_qty "
                            f"but entry is fresh ({round(_entry_age, 1)}s) - preserving local state"
                        )
                        _send_live_reconcile_preserve_alert(
                            _sym_q,
                            _side_q,
                            "fresh_entry_under_300s",
                            _entry_age,
                            "not_checked_fresh_entry",
                            prefix=_mode_prefix,
                        )
                        continue
                    _pos_closed = None
                    _verifier = getattr(_resolve_exchange_executor("live"), "is_position_closed", None)
                    if _verifier:
                        _pos_closed = _verifier(_sym_q)
                    if _pos_closed is not True:
                        reason = "exchange_position_open" if _pos_closed is False else "exchange_state_uncertain"
                        if _pos_closed is False and _missing_live_exchange_proof(t):
                            _live_exec = _resolve_exchange_executor("live")
                            _ownership = _inspect_live_missing_proof_ownership(t, _live_exec)
                            _ownership_status = _ownership.get("status")
                            _ownership_reason = _ownership.get("reason", "")
                            if _ownership_status == "uncertain":
                                reason = "exchange_state_uncertain"
                                print(
                                    f"[LIVE RECONCILE] {_sym_q} {_side_q} missing exchange proof "
                                    f"ownership uncertain - preserving local state reason={_ownership_reason}"
                                )
                                _send_live_reconcile_preserve_alert(
                                    _sym_q,
                                    _side_q,
                                    reason,
                                    _entry_age,
                                    "open_but_ownership_uncertain",
                                    prefix=_mode_prefix,
                                )
                                continue
                            if _ownership_status == "bot_evidence":
                                _changed = _apply_live_bot_ownership_recovery(t, _ownership)
                                print(
                                    f"[LIVE RECONCILE] {_sym_q} {_side_q} BOT ownership evidence found "
                                    f"reason={_ownership_reason} changed={_changed} - preserving local state"
                                )
                                if _changed:
                                    save_open_trades(_trades, _state_file)
                                _send_live_reconcile_preserve_alert(
                                    _sym_q,
                                    _side_q,
                                    "bot_ownership_recovered",
                                    _entry_age,
                                    "open_bot_evidence",
                                    prefix=_mode_prefix,
                                )
                                continue

                            print(
                                f"[LIVE RECONCILE] {_sym_q} {_side_q} missing exchange proof "
                                f"but exchange position is open without BOT evidence - "
                                f"classifying manual_or_uncertain_position reason={_ownership_reason}"
                            )
                            _backups = _remove_manual_or_uncertain_live_trade(_trades, t, _state_file)
                            send_telegram(
                                f"[LIVE RECONCILE] {_sym_q} {_side_q} removed false local bot state\n"
                                f"reason=manual_or_uncertain_position\n"
                                f"ownership_evidence={_ownership_reason or 'no_bot_order_evidence'}\n"
                                f"action=local_state_removed_exchange_untouched",
                                prefix=_mode_prefix,
                                channel="alerts",
                            )
                            print(
                                f"[LIVE RECONCILE] {_sym_q} {_side_q} manual_or_uncertain cleanup "
                                f"backups={_backups}"
                            )
                            continue
                        print(
                            f"[LIVE RECONCILE] {_sym_q} {_side_q} missing exchange_qty "
                            f"but {reason} - preserving local state"
                        )
                        _send_live_reconcile_preserve_alert(
                            _sym_q,
                            _side_q,
                            reason,
                            _entry_age,
                            "open" if _pos_closed is False else "uncertain",
                            prefix=_mode_prefix,
                        )
                        continue
                    _newly_q = _quarantine_trade(t, "missing_exchange_qty")
                    if _newly_q:
                        print(
                            f"[LIVE RECONCILE] Removed orphan trade: {_sym_q} {_side_q}\n"
                            f"reason=no_exchange_position"
                        )
                        send_telegram(
                            f"[LIVE RECONCILE] Removed orphan trade: {_sym_q} {_side_q}\n"
                            f"reason=no_exchange_position",
                            prefix=_mode_prefix,
                            channel="alerts",
                        )
                        save_open_trades(_trades, _state_file)
                    continue

            _now = time.time()
            if _now - t.get("entry_time", 0) < MIN_HOLD_TIME:
                if DEBUG and not t.get("skip_logged"):
                    print(f"[SKIP FIRST CYCLE] {t['symbol']}")
                    t["skip_logged"] = True
                continue
    
            symbol = t.get("symbol", "UNKNOWN")
            if symbol == "UNKNOWN":
                continue
    
            if fast_mode:
                df, _cache_ts_5m, _cache_age_5m = fetch_cached_with_meta(symbol, "5m", max_age=120)
                _price_meta_5m = {
                    "price_source": "cached_5m",
                    "cache_ts": _cache_ts_5m,
                    "cache_age_secs": _cache_age_5m,
                    "cache_max_age_secs": 120,
                }
            else:
                df = fetch(symbol, "5m")
                _price_meta_5m = {
                    "price_source": "unknown",
                    "cache_ts": None,
                    "cache_age_secs": None,
                    "cache_max_age_secs": None,
                }
    
            if df is None or len(df) < 5:
                _skip_reason_5m = "insufficient_rows" if df is not None else "missing_or_stale_5m_cache"
                _log_trade_management_freshness(
                    t,
                    _exec_mode,
                    "SKIP_NO_FRESH_PRICE",
                    _update_started_ts,
                    price_meta=_price_meta_5m,
                    reason=_skip_reason_5m if fast_mode else "missing_5m_price",
                    skip_reason=_skip_reason_5m,
                )
                continue

            _price_meta_5m["candle_time"] = _tmf_candle_time(df)
    
            high = df["high"].iloc[-1]
            low = df["low"].iloc[-1]
            p = df["close"].iloc[-1]

            _management_context_base = {
                "update_trades_started_ts": _update_started_ts,
                "trade_loop_id": _trade_loop_id,
                "symbol_loop_index": _symbol_loop_index,
                "price_meta": _price_meta_5m,
                "current_price_used_in_decision": p,
                "message_price_source": _price_meta_5m.get("price_source") or "unknown",
            }

            def _management_send(msg, alert_type, category=None, **extra):
                management_context = dict(_management_context_base)
                management_context.update(extra)
                management_context.setdefault("event_detected_ts", time.time())
                _dedup_key = None
                if _exec_mode == "paper":
                    if alert_type in ("trade_close", "paper_smc_main_close"):
                        _dedup_key = telegram_dedup.build_key(
                            "close",
                            alert_type,
                            t.get("id"),
                            t.get("symbol"),
                            t.get("side"),
                            extra.get("exit_reason") or t.get("close_reason") or t.get("exit_type"),
                        )
                    elif category in ("be_move", "profit_lock"):
                        _dedup_key = telegram_dedup.build_key(
                            "mgmt",
                            alert_type,
                            t.get("id"),
                            t.get("symbol"),
                            t.get("side"),
                            extra.get("new_sl"),
                        )
                return _send_management_telegram(
                    t,
                    msg,
                    alert_type,
                    _mode_prefix,
                    _exec_mode,
                    category=category,
                    management_context=management_context,
                    dedup_key=_dedup_key,
                )
    
            entry_real = t.get("entry_real") or t.get("entry")
            entry = entry_real
            risk = abs(entry_real - t["sl_init"])
    
            # ===== UPDATE MAX PROFIT R (REAL) =====
            if t["side"] == "LONG":
                r_now = (high - entry_real) / abs(entry_real - t["sl_init"])
            else:
                r_now = (entry_real - low) / abs(entry_real - t["sl_init"])
    
            t["max_profit_r"] = max(t.get("max_profit_r", 0), r_now)
            if _exec_mode == "paper":
                _paper_quality_update_excursion(t, high, low)

            # ===== RETENTION METRICS TRACKING =====
            if r_now > t.get("_prev_max_r", -1):
                t["peak_profit_timestamp"] = time.time()
            t["_prev_max_r"] = t["max_profit_r"]
            if not t.get("time_to_1r") and r_now >= 1.0:
                t["time_to_1r"] = round((time.time() - t["time"]) / 60, 1)
            if r_now >= 1.0:
                if not t.get("_above_1r_since"):
                    t["_above_1r_since"] = time.time()
            else:
                if t.get("_above_1r_since"):
                    t["time_spent_above_1r"] = round(
                        t.get("time_spent_above_1r", 0) + (time.time() - t["_above_1r_since"]) / 60, 1
                    )
                    t["_above_1r_since"] = None
            if t.get("partial_done"):
                t["max_r_after_partial"] = max(t.get("max_r_after_partial", 0), r_now)

            _sl_before_updates = t["sl"]
    
            # ===== PRE-1R MOMENTUM LOCK (P6) =====
            if not t.get("pre1r_lock_done") and not t.get("partial_done") and not t.get("be_07_done"):
                _r_now = t.get("max_profit_r", 0)
                if 0.7 <= _r_now < 1.0 and is_momentum_weakening(df, t["side"]):
                    t["pre1r_lock_done"] = True
                    t["be_07_done"] = True
                    save_open_trades(_trades, _state_file)
                    _lock_r = 0.3
                    if t["side"] == "LONG":
                        _new_sl = entry + risk * _lock_r
                        if _new_sl > t["sl"]:
                            _decision_sl_before = t["sl"]
                            t["sl"] = _new_sl
                            print(f"[PRE1R LOCK] {symbol} weakening at {round(_r_now,2)}R SL → +0.3R {round(t['sl'],6)}")
                            _log_trade_management_freshness(
                                t, _exec_mode, "MOMENTUM_LOCK", _update_started_ts,
                                price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                                sl_before=_decision_sl_before, reason="pre1r_momentum_lock",
                            )
                            _management_send(
                                f"⚡ {t['symbol']} MOMENTUM LOCK ({round(_r_now,2)}R)\n"
                                f"SL → +0.3R: {fmt_price(t['sl'], t['symbol'])}",
                                "momentum_lock",
                                category="be_move",
                                old_sl=_decision_sl_before,
                                new_sl=t["sl"],
                            )
                    else:
                        _new_sl = entry - risk * _lock_r
                        if _new_sl < t["sl"]:
                            _decision_sl_before = t["sl"]
                            t["sl"] = _new_sl
                            print(f"[PRE1R LOCK] {symbol} weakening at {round(_r_now,2)}R SL → +0.3R {round(t['sl'],6)}")
                            _log_trade_management_freshness(
                                t, _exec_mode, "MOMENTUM_LOCK", _update_started_ts,
                                price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                                sl_before=_decision_sl_before, reason="pre1r_momentum_lock",
                            )
                            _management_send(
                                f"⚡ {t['symbol']} MOMENTUM LOCK ({round(_r_now,2)}R)\n"
                                f"SL → +0.3R: {fmt_price(t['sl'], t['symbol'])}",
                                "momentum_lock",
                                category="be_move",
                                old_sl=_decision_sl_before,
                                new_sl=t["sl"],
                            )

            # ===== 0.7R BREAKEVEN =====
            if not t.get("be_07_done") and not t.get("partial_done"):
                if t.get("max_profit_r", 0) >= 0.7:
                    t["be_07_done"] = True
                    save_open_trades(_trades, _state_file)
                    if t["side"] == "LONG":
                        new_sl = entry
                        if new_sl > t["sl"]:
                            _decision_sl_before = t["sl"]
                            t["sl"] = new_sl
                            print(f"[BE] moved to breakeven at 0.7R {t['symbol']}")
                            _log_trade_management_freshness(
                                t, _exec_mode, "PROFIT_LOCK", _update_started_ts,
                                price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                                sl_before=_decision_sl_before, reason="breakeven_07r",
                            )
                            _management_send(
                                f"🛡️ {t['symbol']} SL → BE (0.7R)\n"
                                f"SL → {fmt_price(entry, t['symbol'])}",
                                "breakeven_move",
                                category="be_move",
                                old_sl=_decision_sl_before,
                                new_sl=t["sl"],
                            )
                    else:
                        new_sl = entry
                        if new_sl < t["sl"]:
                            _decision_sl_before = t["sl"]
                            t["sl"] = new_sl
                            print(f"[BE] moved to breakeven at 0.7R {t['symbol']}")
                            _log_trade_management_freshness(
                                t, _exec_mode, "PROFIT_LOCK", _update_started_ts,
                                price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                                sl_before=_decision_sl_before, reason="breakeven_07r",
                            )
                            _management_send(
                                f"🛡️ {t['symbol']} SL → BE (0.7R)\n"
                                f"SL → {fmt_price(entry, t['symbol'])}",
                                "breakeven_move",
                                category="be_move",
                                old_sl=_decision_sl_before,
                                new_sl=t["sl"],
                            )
    
            # ===== 0.5R EARLY MOMENTUM PROTECT =====
            if not t.get("be_early_done") and not t.get("partial_done") and not t.get("be_07_done"):
                if t.get("max_profit_r", 0) >= 0.5:
                    _r_close = (p - entry) / risk if t["side"] == "LONG" else (entry - p) / risk
                    _giving_back = t["max_profit_r"] - _r_close >= 0.35
                    if _giving_back and is_momentum_weakening(df, t["side"]):
                        t["be_early_done"] = True
                        save_open_trades(_trades, _state_file)
                        if t["side"] == "LONG":
                            if entry > t["sl"]:
                                _decision_sl_before = t["sl"]
                                t["sl"] = entry
                                print(f"[BE PROTECT] {symbol} {round(t['max_profit_r'], 2)}R reached Momentum weakening SL → BE")
                                _log_trade_management_freshness(
                                    t, _exec_mode, "MOMENTUM_LOCK", _update_started_ts,
                                    price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                                    sl_before=_decision_sl_before, reason="be_protect_momentum_weakening",
                                )
                                _management_send(
                                    f"🛡️ {symbol} BE PROTECT\n"
                                    f"Max: {round(t['max_profit_r'], 2)}R | Momentum weak\n"
                                    f"SL → {fmt_price(entry, symbol)}",
                                    "breakeven_protect",
                                    category="be_move",
                                    old_sl=_decision_sl_before,
                                    new_sl=t["sl"],
                                )
                        else:
                            if entry < t["sl"]:
                                _decision_sl_before = t["sl"]
                                t["sl"] = entry
                                print(f"[BE PROTECT] {symbol} {round(t['max_profit_r'], 2)}R reached Momentum weakening SL → BE")
                                _log_trade_management_freshness(
                                    t, _exec_mode, "MOMENTUM_LOCK", _update_started_ts,
                                    price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                                    sl_before=_decision_sl_before, reason="be_protect_momentum_weakening",
                                )
                                _management_send(
                                    f"🛡️ {symbol} BE PROTECT\n"
                                    f"Max: {round(t['max_profit_r'], 2)}R | Momentum weak\n"
                                    f"SL → {fmt_price(entry, symbol)}",
                                    "breakeven_protect",
                                    category="be_move",
                                    old_sl=_decision_sl_before,
                                    new_sl=t["sl"],
                                )

            # ===== PAPER SMC RESEARCH MIN-LOCK 0.75R =====
            try:
                if (
                    _exec_mode == "paper"
                    and _paper_smc_research_trade_matches(t)
                    and not t.get("min_lock_075_done")
                    and float(t.get("max_profit_r", 0)) >= 0.75
                ):
                    _ml075_entry_real = t.get("entry_real") or t.get("entry")
                    _ml075_sl_init = t.get("sl_init")
                    if _ml075_entry_real is None or _ml075_sl_init is None:
                        pass
                    else:
                        _ml075_initial_risk = abs(float(_ml075_entry_real) - float(_ml075_sl_init))
                        if _ml075_initial_risk > 0:
                            _ml075_current_sl = t["sl"]
                            if t["side"] == "LONG":
                                _ml075_floor = float(_ml075_entry_real) + _ml075_initial_risk * 0.75
                                _ml075_should_move = _ml075_floor > _ml075_current_sl
                            else:
                                _ml075_floor = float(_ml075_entry_real) - _ml075_initial_risk * 0.75
                                _ml075_should_move = _ml075_floor < _ml075_current_sl
                            if _ml075_should_move:
                                t["sl"] = _ml075_floor
                            t["min_lock_075_done"] = True
                            save_open_trades(_trades, _state_file)
                            try:
                                _ml075_r_now = (
                                    (p - float(_ml075_entry_real)) / _ml075_initial_risk
                                    if t["side"] == "LONG"
                                    else (float(_ml075_entry_real) - p) / _ml075_initial_risk
                                )
                                _ml075_log_row = {
                                    "ts": time.time(),
                                    "timestamp": datetime.now().astimezone().isoformat(),
                                    "event": "PAPER_SMC_RESEARCH_MIN_LOCK_075",
                                    "version": "v1_paper_research_only",
                                    "reason": "MIN_LOCK_075_TRIGGERED",
                                    "symbol": t.get("symbol"),
                                    "side": t.get("side"),
                                    "entry_type": t.get("entry_type"),
                                    "research_join_key": _paper_smc_research_key(t),
                                    "research_dedup_key": t.get("research_dedup_key"),
                                    "entry": float(_ml075_entry_real),
                                    "initial_risk": _ml075_initial_risk,
                                    "old_sl": _ml075_current_sl,
                                    "new_sl": t["sl"],
                                    "sl_moved": _ml075_should_move,
                                    "current_r": round(_ml075_r_now, 4),
                                    "mfe_r": t.get("max_profit_r"),
                                }
                                _ml075_log_path = os.path.join("logs", "paper_smc_research_min_lock_075_events.jsonl")
                                os.makedirs("logs", exist_ok=True)
                                with open(_ml075_log_path, "a", encoding="utf-8") as _ml075_fh:
                                    _ml075_fh.write(json.dumps(_ml075_log_row, ensure_ascii=False) + "\n")
                            except Exception as _ml075_log_ex:
                                print(f"[WARN] MIN_LOCK_075 log failed: {_ml075_log_ex}")
                            if _ml075_should_move:
                                print(f"[MIN_LOCK_075] {t.get('symbol')} {t.get('side')} SL → {_ml075_floor} (0.75R floor)")
            except Exception as _ml075_ex:
                print(f"[WARN] MIN_LOCK_075 block failed: {_ml075_ex}")

            # ===== LIVE SMC RESEARCH MIN-LOCK 0.75R =====
            # Applies only to CONFIRM_SMC_RESEARCH trades in live mode when
            # live_smc_research_enabled is true.  Exchange SL sync is attempted
            # via _sync_testnet_trailing_sl; min_lock_075_done is only marked
            # when the exchange confirms the update (sync_result=True) to prevent
            # silent non-protection.  A failed sync logs SYNC_FAILED and retries
            # next scan without moving the done flag.
            try:
                if (
                    _exec_mode == "live"
                    and config.get("live_smc_research_enabled", False)
                    and _paper_smc_research_trade_matches(t)
                    and not t.get("min_lock_075_done")
                    and float(t.get("max_profit_r", 0)) >= 0.75
                ):
                    _lml_entry_real = t.get("entry_real") or t.get("entry")
                    _lml_sl_init = t.get("sl_init")
                    if _lml_entry_real is not None and _lml_sl_init is not None:
                        _lml_initial_risk = abs(
                            float(_lml_entry_real) - float(_lml_sl_init)
                        )
                        if _lml_initial_risk > 0:
                            _lml_current_sl = t["sl"]
                            if t["side"] == "LONG":
                                _lml_floor = (
                                    float(_lml_entry_real) + _lml_initial_risk * 0.75
                                )
                                _lml_should_move = _lml_floor > _lml_current_sl
                            else:
                                _lml_floor = (
                                    float(_lml_entry_real) - _lml_initial_risk * 0.75
                                )
                                _lml_should_move = _lml_floor < _lml_current_sl

                            if _lml_should_move:
                                t["sl"] = _lml_floor

                            _lml_r_now = (
                                (p - float(_lml_entry_real)) / _lml_initial_risk
                                if t["side"] == "LONG"
                                else (float(_lml_entry_real) - p) / _lml_initial_risk
                            )

                            # Attempt exchange SL sync — same mechanism as trailing stop.
                            # Returns True=synced, False=failed, None=no exchange (live
                            # should never return None; guard is defensive).
                            _lml_sync_result = _sync_testnet_trailing_sl(
                                t, ctx, old_sl=_lml_current_sl
                            )

                            if _lml_sync_result is True:
                                _lml_log_event = "MIN_LOCK_075_LIVE_SYNC_OK"
                                t["min_lock_075_done"] = True
                            elif _lml_sync_result is False:
                                # Do NOT mark done — exchange still has old SL.
                                # Will retry next scan; exchange_sl_sync_pending is set.
                                _lml_log_event = "MIN_LOCK_075_LIVE_SYNC_FAILED"
                            else:
                                # Unexpected None in live mode — mark done defensively
                                # so we don't loop forever, but flag it clearly.
                                _lml_log_event = "MIN_LOCK_075_LIVE_NO_SYNC_CTX"
                                t["min_lock_075_done"] = True

                            save_open_trades(_trades, _state_file)

                            try:
                                _lml_log_row = {
                                    "ts": time.time(),
                                    "timestamp": datetime.now().astimezone().isoformat(),
                                    "event": "LIVE_SMC_RESEARCH_MIN_LOCK_075",
                                    "version": "v1_live_research",
                                    "reason": _lml_log_event,
                                    "symbol": t.get("symbol"),
                                    "side": t.get("side"),
                                    "entry_type": t.get("entry_type"),
                                    "research_join_key": _paper_smc_research_key(t),
                                    "research_dedup_key": t.get("research_dedup_key"),
                                    "entry": float(_lml_entry_real),
                                    "initial_risk": _lml_initial_risk,
                                    "old_sl": _lml_current_sl,
                                    "new_sl": t["sl"],
                                    "sl_moved": _lml_should_move,
                                    "current_r": round(_lml_r_now, 4),
                                    "mfe_r": t.get("max_profit_r"),
                                    "sync_result": str(_lml_sync_result),
                                    "min_lock_075_done": t.get("min_lock_075_done"),
                                }
                                _lml_log_path = os.path.join(
                                    "logs",
                                    "live_smc_research_min_lock_075_events.jsonl",
                                )
                                os.makedirs("logs", exist_ok=True)
                                with open(_lml_log_path, "a", encoding="utf-8") as _lml_fh:
                                    _lml_fh.write(
                                        json.dumps(_lml_log_row, ensure_ascii=False) + "\n"
                                    )
                            except Exception as _lml_log_ex:
                                print(f"[WARN] LIVE_MIN_LOCK_075 log failed: {_lml_log_ex}")

                            if _lml_sync_result is True and _lml_should_move:
                                print(
                                    f"[LIVE_MIN_LOCK_075] {t.get('symbol')} "
                                    f"{t.get('side')} SL → {_lml_floor} (0.75R floor, sync OK)"
                                )
                            elif _lml_sync_result is False:
                                print(
                                    f"[LIVE_MIN_LOCK_075] {t.get('symbol')} "
                                    f"{t.get('side')} SL sync FAILED — will retry next scan"
                                )
            except Exception as _lml_ex:
                print(f"[WARN] LIVE_MIN_LOCK_075 block failed: {_lml_ex}")

            # ===== PARTIAL + BE =====
            _be_just_set = False
            if t["side"]=="LONG":
                oneR = entry + risk
    
                if not t.get("partial_done") and high >= oneR:
                    _decision_sl_before = t["sl"]
                    t["partial_done"] = True
                    t["trail_started"] = True
                    t["sl"] = max(t["sl"], entry + 0.1 * risk)
                    _be_just_set = True
                    save_open_trades(_trades, _state_file)
                    print(f"[BE TRIGGER] {t['symbol']} LONG SL → {round(t['sl'], 6)} (entry+0.1R buffer)")
                    _log_trade_management_freshness(
                        t, _exec_mode, "BREAK_TP", _update_started_ts,
                        price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                        sl_before=_decision_sl_before, reason="partial_be_trigger_1r",
                    )
                    _management_send(
                        f"💰 {t['symbol']} ĐẠT 1R\n"
                        f"SL → Hòa vốn: {fmt_price(entry, t['symbol'])}\n"
                        f"🔁 Trailing bắt đầu",
                        "partial_be_trailing_start",
                        category="be_move",
                        old_sl=_decision_sl_before,
                        new_sl=t["sl"],
                    )
    
            else:
                oneR = entry - risk
    
                if not t.get("partial_done") and low <= oneR:
                    _decision_sl_before = t["sl"]
                    t["partial_done"] = True
                    t["trail_started"] = True
                    t["sl"] = min(t["sl"], entry - 0.1 * risk)
                    _be_just_set = True
                    save_open_trades(_trades, _state_file)
                    print(f"[BE TRIGGER] {t['symbol']} SHORT SL → {round(t['sl'], 6)} (entry-0.1R buffer)")
                    _log_trade_management_freshness(
                        t, _exec_mode, "BREAK_TP", _update_started_ts,
                        price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                        sl_before=_decision_sl_before, reason="partial_be_trigger_1r",
                    )
                    _management_send(
                        f"💰 {t['symbol']} ĐẠT 1R\n"
                        f"SL → Hòa vốn: {fmt_price(entry, t['symbol'])}\n"
                        f"🔁 Trailing bắt đầu",
                        "partial_be_trailing_start",
                        category="be_move",
                        old_sl=_decision_sl_before,
                        new_sl=t["sl"],
                    )
    
            if apply_adaptive_trailing_floor(t):
                save_open_trades(_trades, _state_file)
    
            # ===== SWING TRAIL: at +1R → exact BE; at +1.5R → lock 0.5R =====
            if t.get("entry_type", "").startswith("SWING"):
                if _be_just_set:
                    if t["side"] == "LONG":
                        t["sl"] = max(t["sl"], entry)
                    else:
                        t["sl"] = min(t["sl"], entry)
    
                if t.get("partial_done") and not t.get("swing_lock_done"):
                    if t["side"] == "LONG":
                        if high >= entry + risk * 1.5:
                            t["sl"] = max(t["sl"], entry + risk * 0.5)
                            t["swing_lock_done"] = True
                    else:
                        if low <= entry - risk * 1.5:
                            t["sl"] = min(t["sl"], entry - risk * 0.5)
                            t["swing_lock_done"] = True
    
            # ===== PROFIT LOCK =====
            _lock_max_r = t.get("max_profit_r", 0)
            _lock_risk = abs(entry_real - t["sl_init"])
            if _lock_risk > 0 and t.get("partial_done") and t.get("entry_type") != "REVERSAL_CONFIRM":
                if _lock_max_r >= 1.5 and not t.get("profit_lock_15"):
                    _lock_sl_long = entry_real + _lock_risk * 0.6
                    _lock_sl_short = entry_real - _lock_risk * 0.6
                    if t["side"] == "LONG" and _lock_sl_long > t["sl"]:
                        _decision_sl_before = t["sl"]
                        t["sl"] = _lock_sl_long
                        print(f"[PROFIT LOCK 1.5R] {t['symbol']} SL → {round(t['sl'], 6)} (+0.6R)")
                        _log_trade_management_freshness(
                            t, _exec_mode, "PROFIT_LOCK", _update_started_ts,
                            price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                            sl_before=_decision_sl_before, reason="profit_lock_15r",
                        )
                        _management_send(
                            f"🔒 {t['symbol']} PROFIT LOCK +1.5R → SL {fmt_price(t['sl'], t['symbol'])} (+0.6R)",
                            "profit_lock",
                            category="profit_lock",
                            old_sl=_decision_sl_before,
                            new_sl=t["sl"],
                        )
                    elif t["side"] == "SHORT" and _lock_sl_short < t["sl"]:
                        _decision_sl_before = t["sl"]
                        t["sl"] = _lock_sl_short
                        print(f"[PROFIT LOCK 1.5R] {t['symbol']} SL → {round(t['sl'], 6)} (+0.6R)")
                        _log_trade_management_freshness(
                            t, _exec_mode, "PROFIT_LOCK", _update_started_ts,
                            price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                            sl_before=_decision_sl_before, reason="profit_lock_15r",
                        )
                        _management_send(
                            f"🔒 {t['symbol']} PROFIT LOCK +1.5R → SL {fmt_price(t['sl'], t['symbol'])} (+0.6R)",
                            "profit_lock",
                            category="profit_lock",
                            old_sl=_decision_sl_before,
                            new_sl=t["sl"],
                        )
                    t["profit_lock_15"] = True
                    t["profit_lock_12"] = True
                    save_open_trades(_trades, _state_file)
                elif _lock_max_r >= 1.2 and not t.get("profit_lock_12"):
                    _lock_sl_long = entry_real + _lock_risk * 0.3
                    _lock_sl_short = entry_real - _lock_risk * 0.3
                    if t["side"] == "LONG" and _lock_sl_long > t["sl"]:
                        _decision_sl_before = t["sl"]
                        t["sl"] = _lock_sl_long
                        print(f"[PROFIT LOCK 1.2R] {t['symbol']} SL → {round(t['sl'], 6)} (+0.3R)")
                        _log_trade_management_freshness(
                            t, _exec_mode, "PROFIT_LOCK", _update_started_ts,
                            price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                            sl_before=_decision_sl_before, reason="profit_lock_12r",
                        )
                    elif t["side"] == "SHORT" and _lock_sl_short < t["sl"]:
                        _decision_sl_before = t["sl"]
                        t["sl"] = _lock_sl_short
                        print(f"[PROFIT LOCK 1.2R] {t['symbol']} SL → {round(t['sl'], 6)} (+0.3R)")
                        _log_trade_management_freshness(
                            t, _exec_mode, "PROFIT_LOCK", _update_started_ts,
                            price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                            sl_before=_decision_sl_before, reason="profit_lock_12r",
                        )
                    t["profit_lock_12"] = True
                    save_open_trades(_trades, _state_file)

            if t["sl"] != _sl_before_updates:
                _sync_requested_ts = time.time() if _exec_mode == "live" else None
                _sync_result = _sync_testnet_trailing_sl(t, ctx, old_sl=_sl_before_updates)
                _sync_finished_ts = time.time() if _sync_requested_ts is not None else None
                _log_trade_management_freshness(
                    t, _exec_mode, "TRAIL_UPDATE", _update_started_ts,
                    price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                    sl_before=_sl_before_updates, reason="sl_changed_sync_attempt",
                    exchange_sl_sync_requested_ts=_sync_requested_ts,
                    exchange_sl_sync_confirmed_ts=_sync_finished_ts if _sync_result is True else None,
                    exchange_sl_sync_failed=(_sync_result is False) if _sync_requested_ts is not None else None,
                )
    
            # ===== EXIT OPTIMIZATION (CH? SAU 1R) =====
            exit_signal = None
    
            if t.get("partial_done"):   # 🔥 KEY FIX
                if fast_mode:
                    df15, _cache_ts_15m, _cache_age_15m = fetch_cached_with_meta(symbol, "15m", max_age=120)
                    _price_meta_15m = {
                        "price_source": "cached_15m",
                        "cache_ts": _cache_ts_15m,
                        "cache_age_secs": _cache_age_15m,
                        "cache_max_age_secs": 120,
                    }
                else:
                    df15 = fetch(symbol, "15m")
                    _price_meta_15m = {
                        "price_source": "unknown",
                        "cache_ts": None,
                        "cache_age_secs": None,
                        "cache_max_age_secs": None,
                    }
    
                if df15 is None:
                    _log_trade_management_freshness(
                        t,
                        _exec_mode,
                        "SKIP_NO_FRESH_PRICE",
                        _update_started_ts,
                        price_meta=_price_meta_15m,
                        close_price=p,
                        high_price=high,
                        low_price=low,
                        reason="missing_or_stale_15m_cache_exit_optimization" if fast_mode else "missing_15m_price_exit_optimization",
                        skip_reason="missing_15m_cache",
                    )
                    continue
                _price_meta_15m["candle_time"] = _tmf_candle_time(df15)
    
                _sl_before_exit_opt = t["sl"]
                _phase_before_exit_opt = t.get("trail_phase")
                _exit_opt_management_context = dict(_management_context_base)
                _exit_opt_management_context.update({
                    "event_detected_ts": time.time(),
                    "price_meta": _price_meta_15m,
                    "current_price_used_in_decision": p,
                })
                exit_signal = exit_optimization(
                    t,
                    p,
                    df15,
                    prefix=_mode_prefix,
                    management_context=_exit_opt_management_context,
                )
                if t.get("trail_phase") != _phase_before_exit_opt:
                    _log_trade_management_freshness(
                        t, _exec_mode, "PHASE_LOCK", _update_started_ts,
                        price_meta=_price_meta_15m, close_price=p, high_price=high, low_price=low,
                        sl_before=_sl_before_exit_opt, reason="exit_optimization_phase_change",
                    )
                if exit_signal == "EARLY_MOMENTUM_WEAK":
                    _log_trade_management_freshness(
                        t, _exec_mode, "MOMENTUM_LOCK", _update_started_ts,
                        price_meta=_price_meta_15m, close_price=p, high_price=high, low_price=low,
                        sl_before=_sl_before_exit_opt, reason=exit_signal,
                    )
                # FIX 1: lock_done SL sync — exit_optimization() may advance t["sl"] to +1R
                # floor (lock_done block).  Sync immediately so exchange stop never lags.
                if t["sl"] != _sl_before_exit_opt:
                    _sync_requested_ts = time.time() if _exec_mode == "live" else None
                    _sync_result = _sync_testnet_trailing_sl(t, ctx, old_sl=_sl_before_exit_opt)
                    _sync_finished_ts = time.time() if _sync_requested_ts is not None else None
                    _log_trade_management_freshness(
                        t, _exec_mode, "TRAIL_UPDATE", _update_started_ts,
                        price_meta=_price_meta_15m, close_price=p, high_price=high, low_price=low,
                        sl_before=_sl_before_exit_opt, reason="exit_optimization_sl_changed_sync_attempt",
                        exchange_sl_sync_requested_ts=_sync_requested_ts,
                        exchange_sl_sync_confirmed_ts=_sync_finished_ts if _sync_result is True else None,
                        exchange_sl_sync_failed=(_sync_result is False) if _sync_requested_ts is not None else None,
                    )
            _tp_val = _safe_numeric_value(t.get("tp"))
            if _tp_val is None:
                _warn_missing_tp_reconstructed_orphan(t)
                hit_tp_now = False
            else:
                hit_tp_now = high >= _tp_val if t["side"]=="LONG" else low <= _tp_val
            # ===== APPLY EXIT OPTIMIZATION =====
            if exit_signal:
                if hit_tp_now:
                    pass  # TP ?? x?y ra ? b? qua early exit
    
                else:
                    t["exit_type"] = exit_signal
                    t["early_exit"] = True
                    t["status"] = "CLOSING"
                    _log_trade_management_freshness(
                        t, _exec_mode, "CLOSE", _update_started_ts,
                        price_meta=_price_meta_15m, close_price=p, high_price=high, low_price=low,
                        sl_before=t.get("sl"), reason=exit_signal,
                    )
    
            # ===== TRAILING STRUCTURE =====
            if t.get("trail_started") and not t.get("early_exit") and not _be_just_set:
                if DEBUG and not t.get("trail_log_sent"):
                    print(f"[TRAIL START] {t['symbol']} phase={t['trail_phase']}")
                if not t.get("trail_log_sent"):
                    t["trail_log_sent"] = True
                if fast_mode:
                    df5, _trail_cache_ts_5m, _trail_cache_age_5m = fetch_cached_with_meta(symbol, "5m", max_age=120)
                    df15, _trail_cache_ts_15m, _trail_cache_age_15m = fetch_cached_with_meta(symbol, "15m", max_age=120)
                    _trail_price_meta_5m = {
                        "price_source": "cached_5m",
                        "cache_ts": _trail_cache_ts_5m,
                        "cache_age_secs": _trail_cache_age_5m,
                        "cache_max_age_secs": 120,
                    }
                    _trail_price_meta_15m = {
                        "price_source": "cached_15m",
                        "cache_ts": _trail_cache_ts_15m,
                        "cache_age_secs": _trail_cache_age_15m,
                        "cache_max_age_secs": 120,
                    }
                else:
                    df5  = fetch(symbol, "5m")
                    df15 = fetch(symbol, "15m")
                    _trail_price_meta_5m = {"price_source": "unknown", "cache_ts": None, "cache_age_secs": None, "cache_max_age_secs": None}
                    _trail_price_meta_15m = {"price_source": "unknown", "cache_ts": None, "cache_age_secs": None, "cache_max_age_secs": None}
    
                if df5 is None or df15 is None:
                    _missing_meta = _trail_price_meta_5m if df5 is None else _trail_price_meta_15m
                    _log_trade_management_freshness(
                        t,
                        _exec_mode,
                        "SKIP_NO_FRESH_PRICE",
                        _update_started_ts,
                        price_meta=_missing_meta,
                        close_price=p,
                        high_price=high,
                        low_price=low,
                        reason="missing_or_stale_trailing_cache" if fast_mode else "missing_trailing_price",
                        skip_reason="missing_15m_cache" if df15 is None else "missing_or_stale_5m_cache",
                    )
                    continue
                _trail_price_meta_5m["candle_time"] = _tmf_candle_time(df5)
                _trail_price_meta_15m["candle_time"] = _tmf_candle_time(df15)

                _strong_cont = (t.get("score", 0) >= 11 or t.get("market_mode", "") == "TREND")

                if t["side"]=="LONG":

                    if t["trail_phase"] == 1:
                        swing = get_swing_low_m15(df15) if _strong_cont else get_swing_low(df5)
                        _trail_decision_meta = _trail_price_meta_15m if _strong_cont else _trail_price_meta_5m
                    elif t["trail_phase"] == 2:
                        swing = get_swing_low_m15(df15)
                        _trail_decision_meta = _trail_price_meta_15m
                    else:
                        swing = df5["low"].iloc[-3:-1].min()
                        _trail_decision_meta = _trail_price_meta_5m
    
                    if swing > t["sl"]:
                        _old_trail_sl = t["sl"]
                        t["sl"] = swing
                        # FIX 3: sync first — persist only after confirmed, eliminating crash window
                        _sync_requested_ts = time.time() if _exec_mode == "live" else None
                        _sync_ok = _sync_testnet_trailing_sl(t, ctx, old_sl=_old_trail_sl)
                        _sync_finished_ts = time.time() if _sync_requested_ts is not None else None
                        _log_trade_management_freshness(
                            t, _exec_mode, "TRAIL_UPDATE", _update_started_ts,
                            price_meta=_trail_decision_meta, close_price=p, high_price=high, low_price=low,
                            sl_before=_old_trail_sl, reason="structure_trailing_update",
                            exchange_sl_sync_requested_ts=_sync_requested_ts,
                            exchange_sl_sync_confirmed_ts=_sync_finished_ts if _sync_ok is True else None,
                            exchange_sl_sync_failed=(_sync_ok is False) if _sync_requested_ts is not None else None,
                        )
                        if _sync_ok is not False:
                            save_open_trades(_trades, _state_file)

                        sl_r = abs(t["sl"] - t["entry_real"]) / abs(t["entry_real"] - t["sl_init"])

                        if ctx is not None and ctx.execution_mode == "testnet":
                            _management_send(
                                f"🔁 {t['symbol']} SL updated",
                                "sl_sync_update",
                                old_sl=_old_trail_sl,
                                new_sl=t["sl"],
                                price_meta=_trail_decision_meta,
                            )
                        else:
                            _trail_management_context = dict(_management_context_base)
                            _trail_management_context.update({
                                "event_detected_ts": time.time(),
                                "old_sl": _old_trail_sl,
                                "new_sl": t["sl"],
                                "price_meta": _trail_decision_meta,
                            })
                            _send_trail_update_telegram(
                                t, _exec_mode, _mode_prefix, sl_r,
                                management_context=_trail_management_context,
                            )

                else:
    
                    if t["trail_phase"] == 1:
                        swing = get_swing_high_m15(df15) if _strong_cont else get_swing_high(df5)
                        _trail_decision_meta = _trail_price_meta_15m if _strong_cont else _trail_price_meta_5m
                    elif t["trail_phase"] == 2:
                        swing = get_swing_high_m15(df15)
                        _trail_decision_meta = _trail_price_meta_15m
                    else:
                        swing = df5["high"].iloc[-3:-1].max()
                        _trail_decision_meta = _trail_price_meta_5m
    
                    if swing < t["sl"]:
                        _old_trail_sl = t["sl"]
                        t["sl"] = swing
                        # FIX 3: sync first — persist only after confirmed, eliminating crash window
                        _sync_requested_ts = time.time() if _exec_mode == "live" else None
                        _sync_ok = _sync_testnet_trailing_sl(t, ctx, old_sl=_old_trail_sl)
                        _sync_finished_ts = time.time() if _sync_requested_ts is not None else None
                        _log_trade_management_freshness(
                            t, _exec_mode, "TRAIL_UPDATE", _update_started_ts,
                            price_meta=_trail_decision_meta, close_price=p, high_price=high, low_price=low,
                            sl_before=_old_trail_sl, reason="structure_trailing_update",
                            exchange_sl_sync_requested_ts=_sync_requested_ts,
                            exchange_sl_sync_confirmed_ts=_sync_finished_ts if _sync_ok is True else None,
                            exchange_sl_sync_failed=(_sync_ok is False) if _sync_requested_ts is not None else None,
                        )
                        if _sync_ok is not False:
                            save_open_trades(_trades, _state_file)

                        sl_r = abs(t["sl"] - t["entry_real"]) / abs(t["entry_real"] - t["sl_init"])

                        if ctx is not None and ctx.execution_mode == "testnet":
                            _management_send(
                                f"🔁 {t['symbol']} SL updated",
                                "sl_sync_update",
                                old_sl=_old_trail_sl,
                                new_sl=t["sl"],
                                price_meta=_trail_decision_meta,
                            )
                        else:
                            _trail_management_context = dict(_management_context_base)
                            _trail_management_context.update({
                                "event_detected_ts": time.time(),
                                "old_sl": _old_trail_sl,
                                "new_sl": t["sl"],
                                "price_meta": _trail_decision_meta,
                            })
                            _send_trail_update_telegram(
                                t, _exec_mode, _mode_prefix, sl_r,
                                management_context=_trail_management_context,
                            )
    
            # ===== TP / SL (PRODUCTION FIX v6.9+) =====
            if not t.get("early_exit"):
                _tp_val = _safe_numeric_value(t.get("tp"))
                if _tp_val is None:
                    _warn_missing_tp_reconstructed_orphan(t)

                if t["side"] == "LONG":
                    hit_tp = False if _tp_val is None else high >= _tp_val
                    hit_sl = p    <= t["sl"]
                else:
                    hit_tp = False if _tp_val is None else low  <= _tp_val
                    hit_sl = p    >= t["sl"]
    
                # ===== BOTH HIT =====
                if hit_tp and hit_sl:
                    t["both_hit"] = True
    
                    if t["tp_mode"] == "HARD":
                        hit_sl = False   # TP lu?n th?ng
                    else:
                        hit_tp = False   # trailing ? SL th?ng
    
                # ===== HANDLE TP =====
                if hit_tp:
                    t["tp_hit"] = True   # ?? FIX QUAN TR?NG
    
                    if t["tp_mode"] == "HARD":
                        t["status"] = "WIN"
                        t["exit_type"] = "TP"
                        _log_trade_management_freshness(
                            t, _exec_mode, "TP_HIT", _update_started_ts,
                            price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                            sl_before=t.get("sl"), reason="hard_tp_hit",
                        )
    
                    else:
                        # SOFT TP ? kh?ng ??ng, ch? k?ch ho?t trailing
                        if not t.get("tp_break_sent"):
                            t["tp_break_sent"] = True
                            save_open_trades(_trades, _state_file)
                            _log_trade_management_freshness(
                                t, _exec_mode, "BREAK_TP", _update_started_ts,
                                price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                                sl_before=t.get("sl"), reason="soft_tp_break",
                            )
                            _management_send(
                                f"🚀 {t['symbol']} BREAK TP → trailing",
                                "break_tp_trailing",
                                category="tp_break",
                                old_sl=t.get("sl"),
                                new_sl=t.get("sl"),
                            )
    
                # ===== HANDLE SL =====
                elif hit_sl:
    
                    # ?? hit TP tr??c ?? ? trailing win
                    if t.get("tp_hit"):
                        t["status"] = "WIN"
                        t["exit_type"] = "TRAIL"
                        _log_trade_management_freshness(
                            t, _exec_mode, "SL_HIT", _update_started_ts,
                            price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                            sl_before=t.get("sl"), reason="trailing_sl_after_tp",
                        )
    
                    # ?? partial ? TRAIL n?u SL ?? trailing qua entry, ng??c l?i BE
                    elif t.get("partial_done"):
                        _entry_val = t.get("entry_real") or t.get("entry")
                        _sl_val = t.get("sl", 0)
                        if t["side"] == "LONG" and _sl_val > _entry_val:
                            t["status"] = "WIN"
                            t["exit_type"] = "TRAIL"
                            _log_trade_management_freshness(
                                t, _exec_mode, "SL_HIT", _update_started_ts,
                                price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                                sl_before=t.get("sl"), reason="partial_trailing_sl",
                            )
                        elif t["side"] == "SHORT" and _sl_val < _entry_val:
                            t["status"] = "WIN"
                            t["exit_type"] = "TRAIL"
                            _log_trade_management_freshness(
                                t, _exec_mode, "SL_HIT", _update_started_ts,
                                price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                                sl_before=t.get("sl"), reason="partial_trailing_sl",
                            )
                        else:
                            t["status"] = "BE"
                            t["exit_type"] = "BE"
                            _log_trade_management_freshness(
                                t, _exec_mode, "SL_HIT", _update_started_ts,
                                price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                                sl_before=t.get("sl"), reason="partial_be_sl",
                            )
    
                    else:
                        t["status"] = "LOSE"
                        t["exit_type"] = "SL"
                        _log_trade_management_freshness(
                            t, _exec_mode, "SL_HIT", _update_started_ts,
                            price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                            sl_before=t.get("sl"), reason="stop_loss_hit",
                        )
                        # SWING SL hit → reset phase to compress so BOS can re-confirm
                        if (t.get("entry_type", "").startswith("SWING")
                                and t["symbol"] in compression_watchlist):
                            compression_watchlist[t["symbol"]]["phase"] = "compress"
    
                elif hit_tp:
                    t["tp_hit"] = True   # ?? ADD D?NG N?Y
    
                    if t["tp_mode"] == "HARD":
                        t["status"] = "WIN"
                        t["exit_type"] = "TP"
                        _log_trade_management_freshness(
                            t, _exec_mode, "TP_HIT", _update_started_ts,
                            price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                            sl_before=t.get("sl"), reason="hard_tp_hit_duplicate_branch",
                        )
                    else:
                        if not t.get("tp_break_sent"):
                            t["tp_break_sent"] = True
                            save_open_trades(_trades, _state_file)
                            _log_trade_management_freshness(
                                t, _exec_mode, "BREAK_TP", _update_started_ts,
                                price_meta=_price_meta_5m, close_price=p, high_price=high, low_price=low,
                                sl_before=t.get("sl"), reason="soft_tp_break_duplicate_branch",
                            )
                            _management_send(
                                f"🚀 {t['symbol']} BREAK TP → trailing",
                                "break_tp_trailing",
                                category="tp_break",
                                old_sl=t.get("sl"),
                                new_sl=t.get("sl"),
                            )
                            
            # ===== CALC SLIPPAGE (SAFE + ??NG FLOW) =====
            if fast_mode:
                df15, _slip_cache_ts_15m, _slip_cache_age_15m = fetch_cached_with_meta(symbol, "15m", max_age=120)
                _slip_price_meta_15m = {
                    "price_source": "cached_15m",
                    "cache_ts": _slip_cache_ts_15m,
                    "cache_age_secs": _slip_cache_age_15m,
                    "cache_max_age_secs": 120,
                }
            else:
                df15 = fetch(symbol, "15m")
                _slip_price_meta_15m = {
                    "price_source": "unknown",
                    "cache_ts": None,
                    "cache_age_secs": None,
                    "cache_max_age_secs": None,
                }
    
            if df15 is None:
                _log_trade_management_freshness(
                    t,
                    _exec_mode,
                    "SKIP_NO_FRESH_PRICE",
                    _update_started_ts,
                    price_meta=_slip_price_meta_15m,
                    close_price=p,
                    high_price=high,
                    low_price=low,
                    reason="missing_or_stale_15m_cache_slippage" if fast_mode else "missing_15m_price_slippage",
                    skip_reason="missing_15m_cache",
                )
                continue
            _slip_price_meta_15m["candle_time"] = _tmf_candle_time(df15)
    
            vol = calc_volatility(df15)
            slippage_real = SLIPPAGE * (1 + vol * 10)
            if slippage_real is None or math.isnan(slippage_real):
                slippage_real = 0
            # ===== EXIT PRICE REAL (FIX CHU?N) =====
    
            entry_real = t.get("entry_real") or t.get("entry")
            paper_sl_gap_fill = False

            if t["exit_type"] == "TP":
                exit_raw = t["tp"]
    
            elif t["exit_type"] == "SL":
                exit_raw = t["sl"]
                if _exec_mode == "paper":
                    _sl_gap_tier = t.get("execution_tier") or _get_execution_tier(symbol)
                    _sl_gap = get_sl_gap_r_for_tier(_sl_gap_tier)
                    exit_raw = apply_sl_gap_to_stop_fill(exit_raw, t["side"], _sl_gap, entry=entry_real)
                    t["sl_gap_tier"] = _sl_gap_tier
                    t["sl_gap"] = _sl_gap
                    paper_sl_gap_fill = True
    
            elif t["exit_type"] == "BE":
                exit_raw = entry_real   # ?? FIX QUAN TR?NG
    
            elif t["exit_type"] == "TRAIL":
                exit_raw = t["sl"]
    
            elif "EARLY" in t.get("exit_type", ""):
                if t["side"] == "LONG":
                    exit_raw = max(p, t["sl"])
                else:
                    exit_raw = min(p, t["sl"])
    
            else:
                exit_raw = p
    
            # ===== APPLY SLIPPAGE =====
            if paper_sl_gap_fill:
                exit_real = exit_raw
            elif t["side"] == "LONG":
                exit_real = exit_raw * (1 - slippage_real)
            else:
                exit_real = exit_raw * (1 + slippage_real)
    
            if t["exit_type"] == "BE":
                _sl_at_be = t.get("sl", 0)
                if t["side"] == "LONG" and _sl_at_be <= entry_real:
                    exit_real = entry_real
                elif t["side"] == "SHORT" and _sl_at_be >= entry_real:
                    exit_real = entry_real
    
            t["exit_price"] = exit_real

            _tmf_record_success(t, _exec_mode, time.time())
            if t.get("status") == "OPEN" and _should_log_trade_management_check(symbol, _exec_mode, time.time()):
                _log_trade_management_freshness(
                    t,
                    _exec_mode,
                    "CHECK_ONLY",
                    _update_started_ts,
                    update_finished_ts=time.time(),
                    price_meta=_price_meta_5m,
                    close_price=p,
                    high_price=high,
                    low_price=low,
                    sl_before=_sl_before_updates,
                    reason="throttled_open_trade_check",
                )
    
            # ===== CLOSE TRADE =====
            
            if t["status"]!="OPEN":
                if _exec_mode == "live":
                    _local_status = t["status"]
                    _local_exit_type = t.get("exit_type", "")
                    t["pending_exit_status"] = _local_status
                    t["pending_exit_type"] = _local_exit_type
                    t["status"] = "OPEN"
                    save_open_trades(_trades, _state_file)
                    _exchange_closed = _close_live_exchange_position_for_local_exit(
                        t,
                        ctx,
                        _local_exit_type,
                    )
                    if not _exchange_closed:
                        t["status"] = "OPEN"
                        t["exit_type"] = ""
                        save_open_trades(_trades, _state_file)
                        continue
                    save_open_trades(_trades, _state_file)
                    t["status"] = _local_status
                    t["exit_type"] = _local_exit_type
                    t.pop("pending_exit_status", None)
                    t.pop("pending_exit_type", None)
                t["close_time"] = time.time()
                if _exec_mode == "paper" and t.get("exit_type") == "SL":
                    t["sl_before_0_5r"] = not bool(t.get("reached_0_5r"))
                    t["sl_before_1r"] = not bool(t.get("reached_1r"))
                _log_trade_management_freshness(
                    t,
                    _exec_mode,
                    "CLOSE",
                    _update_started_ts,
                    update_finished_ts=t.get("close_time"),
                    price_meta=_price_meta_5m,
                    close_price=p,
                    high_price=high,
                    low_price=low,
                    sl_before=_sl_before_updates,
                    reason=t.get("close_reason") or t.get("exit_type"),
                )
    
                # ===== RR =====
                entry = t.get("entry_real") or t.get("entry")
                exit_real = t["exit_price"]
                sl_init_val = t["sl_init"]
                if sl_init_val is None or math.isnan(sl_init_val):
                    t["rr_real"] = 0
                    t["status"]  = "BE"
                    continue
                risk_init = abs(entry_real - sl_init_val)   # lu?n d?ng sl_init
    
                if risk_init <= 0:
                    t["rr_real"] = 0
                    t["status"]  = "BE"
                    continue
    
                if t["side"] == "LONG":
                    rr_real = (exit_real - entry_real) / risk_init
                else:
                    rr_real = (entry_real - exit_real) / risk_init
                rr_clamped = max(min(rr_real, 10), -10)
                if rr_clamped != rr_real:
                    if DEBUG:
                        print(f"[RR CLAMPED] {t['symbol']} rr_real={round(rr_real,4)} → {rr_clamped}")
                t["rr_real"] = round(rr_clamped, 2)

                # ===== RETENTION METRICS AT CLOSE =====
                _open_ts = t.get("time", 0)
                _close_ts = t.get("close_time", time.time())
                t["trade_age_minutes"] = round((_close_ts - _open_ts) / 60, 1) if _close_ts > _open_ts else 0
                t["giveback_r"] = round(max(0.0, t.get("max_profit_r", 0) - t["rr_real"]), 2)
                t["trailing_phase_at_exit"] = t.get("trail_phase", 0)
                if t.get("_above_1r_since"):
                    t["time_spent_above_1r"] = round(
                        t.get("time_spent_above_1r", 0) + (_close_ts - t["_above_1r_since"]) / 60, 1
                    )
                    t["_above_1r_since"] = None

                # ===== NORMALIZE BE =====
                if abs(t["rr_real"]) < 0.1:
                    t["status"] = "BE"
                    t["rr_real"] = 0
                elif t["rr_real"] > 0:
                    t["status"] = "WIN"
                else:
                    t["status"] = "LOSE"
                
                if t.get("partial_done") and t["status"] == "OPEN":
                    continue
    
                stats.setdefault("entry_type_stats", {})
                stats.setdefault("bos_type_stats", {})
                stats.setdefault("market_mode_stats", {})
                stats.setdefault("exhaustion_stats", {})
                stats.setdefault("wyckoff_stats", {})
                if not t.get("stat_counted"):
                    # ===== UPDATE GLOBAL STATS =====
                    if t["status"] == "WIN":
                        stats["win"] += 1
                        if ctx is not None:
                            ctx.stats["win"] = ctx.stats.get("win", 0) + 1
                    elif t["status"] == "LOSE":
                        stats["loss"] += 1
                        if ctx is not None:
                            ctx.stats["loss"] = ctx.stats.get("loss", 0) + 1
                    elif t["status"] == "BE":
                        if ctx is not None:
                            ctx.stats["be"] = ctx.stats.get("be", 0) + 1
    
                    # ===== UPDATE DETAIL STATS =====
                    result = t["status"]
                    key = t.get("entry_type") or "UNKNOWN"
                    update_stat_dict(stats["entry_type_stats"], t.get("entry_type"), result)
                    update_stat_dict(stats["bos_type_stats"], t.get("bos_type"), result)
                    update_stat_dict(stats["market_mode_stats"], t.get("market_mode"), result)
    
                    # ===== EXHAUSTION STATS V6.6 =====
                    update_stat_dict(
                        stats["exhaustion_stats"],
                        t.get("exhaustion_cls", "UNKNOWN"),
                        result
                    )
    
                    # ===== WYCKOFF PARSE =====
                    update_stat_dict(
                        stats["wyckoff_stats"],
                        t.get("wyckoff", "NONE"),
                        result
                    )
                    t["stat_counted"] = True
    
                # ===== DEBUG SL REASON =====
                if t["rr_real"] <= 0:
    
                    debug_reason = []
                    if "EARLY" in t.get("exit_type",""):
                        debug_reason.append("EARLY_EXIT")
    
                    if t.get("entry_type","").startswith("REVERSAL"):
                        debug_reason.append("REVERSAL_FAIL")
    
                    if t.get("bos_n", 0) >= 3:
                        debug_reason.append("LATE_TREND")
    
                    if t.get("market_mode") == "SIDEWAY":
                        debug_reason.append("RANGE")
    
                    if t.get("score",0) < 8:
                        debug_reason.append("LOW_SCORE")
    
                    if "Overextended" in ",".join(t.get("reason",[])):
                        debug_reason.append("OVEREXTENDED")
    
                    t["sl_reason"] = "|".join(debug_reason)
                
    
                # ===== SET LOSS COOLDOWN (??NG TH?I ?I?M) =====
                if t["status"] == "LOSE" and not t.get("cooldown_set"):
                    _cooldown[t["symbol"]] = time.time()
                    t["cooldown_set"] = True
    
                # ===== RESET LOSS COOLDOWN IF WIN =====
                if t["status"] == "WIN":
                    if t["symbol"] in _cooldown:
                        del _cooldown[t["symbol"]]
    
                # ===== RESET ENTRY COOLDOWN (KH?NG ?P D?NG CHO LOSE) =====
                if t["status"] != "OPEN" and not t.get("entry_cd_set"):
                    if t["status"] != "LOSE":
                        _entry_cooldown[t["symbol"]] = time.time()
                    t["entry_cd_set"] = True
                # ===== UPDATE BALANCE TR??C TELE (FIX BUG) =====
                if not t.get("balance_updated"):
                    _pre_trade_balance = _account_balance
                    _bal_at_entry = t.get("balance_at_entry", _account_balance)
                    pnl = _bal_at_entry * t.get("risk_percent", RISK_PER_TRADE) * t["rr_real"]
                    _account_balance += pnl
                    t["balance_updated"] = True
                    _equity_peak = max(_equity_peak, _account_balance)
                    if ctx is not None:
                        ctx.account_balance = _account_balance
                        ctx.equity_peak = _equity_peak
                        ctx.session_pnl_r += t["rr_real"]
                    else:
                        ACCOUNT_BALANCE = _account_balance
                        EQUITY_PEAK = _equity_peak
                    if ctx is None or ctx.execution_mode == "paper":
                        config["account_balance"] = _account_balance
                        config["equity_peak"] = _equity_peak
                        with open("config.tmp", "w", encoding="utf-8") as f:
                            json.dump(_strip_secrets_for_save(config), f, indent=2, ensure_ascii=False)
                        os.replace("config.tmp", "config.json")
                    if ctx is not None and ctx.execution_mode in ("testnet", "live"):
                        ctx.save_account_state()
                else:
                    _pre_trade_balance = _account_balance
                        
                # ===== TELE =====
                status_icon = {
                    "WIN": "🎯",
                    "LOSE": "❌",
                    "BE": "⚖️"
                }.get(t["status"], "❓")
                exit_map = {
                    "TP":                   "Chốt lời",
                    "SL":                   "Cắt lỗ",
                    "TRAIL":                "Trailing",
                    "BE":                   "Hòa vốn",
                    "EARLY_GIVEBACK":       "Mất lợi nhuận",
                    "EARLY_STRUCT":         "Gãy cấu trúc",
                    "EARLY_MOMENTUM_WEAK":  "Momentum yếu",
                }
    
                exit_text = exit_map.get(t.get("exit_type"), t.get("exit_type"))
    
                exit_real = t["exit_price"]
                max_r = round(t.get("max_profit_r", 0), 2)
                entry = t.get("entry_real") or t.get("entry")
                sl_real = t.get("sl_real", t["sl_init"])
    
                risk_init = abs(entry_real - sl_real)
    
                if t["side"] == "LONG":
                    oneR = entry_real + risk_init
                else:
                    oneR = entry_real - risk_init
    
                # ===== TELE EXIT V6.4 =====
    
                balance_entry = t.get("balance_at_entry", _account_balance)
                risk_amt = balance_entry * t.get("risk_percent", RISK_PER_TRADE)
                pnl_str = fmt_pnl(t["rr_real"], risk_amt)
                max_r = round(t.get("max_profit_r", 0), 2)
    
                if ctx is not None and ctx.execution_mode == "testnet":
                    _tn_icon = "✅" if t["rr_real"] >= 0 else "❌"
                    _rr_sign = "+" if t["rr_real"] >= 0 else ""
                    msg = (
                        f"{_tn_icon} {t['symbol']} {_rr_sign}{round(t['rr_real'], 1)}R\n"
                        f"Bal: {round(_account_balance, 1)}"
                    )
                else:
                    balance_line = f"Balance: {round(_account_balance, 2)}$"
                    _gb_r = t.get("giveback_r", 0)
                    _age_m = t.get("trade_age_minutes", 0)
                    _t1r = t.get("time_to_1r")
                    _age_str = f"{round(_age_m / 60, 1)}h" if _age_m >= 60 else f"{round(_age_m):.0f}m"
                    _ana_line = f"Max: {max_r}R | GB: {round(_gb_r, 2)}R | Age: {_age_str}"
                    if _t1r:
                        _ana_line += f" | T→1R: {_t1r}m"
                    if _exec_mode == "paper" and (
                        t.get("entry_type") == "PAPER_SMC_MAIN"
                        or t.get("strategy_family") == "paper_smc_main"
                    ):
                        msg = format_paper_smc_close(t, engine="SMC-MAIN", close_reason=exit_text)
                    elif _exec_mode == "paper":
                        msg = (
                            f"{status_icon} {engine_label(t)} • CLOSED\n"
                            f"{t['symbol']} {t.get('side', '')} | {exit_text}\n"
                            f"E: {fmt_price(entry_real, t['symbol'])} → X: {fmt_price(exit_real, t['symbol'])}\n"
                            f"PnL: {pnl_str}\n"
                            f"{_ana_line}\n"
                            f"{balance_line} | {format_vn_time(time.time())}"
                        )
                    else:
                        msg = (
                            f"{status_icon} {t['symbol']} | {exit_text}\n"
                            f"E: {fmt_price(entry_real, t['symbol'])} → X: {fmt_price(exit_real, t['symbol'])}\n"
                            f"PnL: {pnl_str}\n"
                            f"{_ana_line}\n"
                            f"{balance_line} | {format_vn_time(time.time())}"
                        )

                _is_research_close = _paper_smc_research_trade_matches(t) and _exec_mode == "paper"
                _is_paper_smc_main_close = (
                    _exec_mode == "paper"
                    and (
                        t.get("entry_type") == "PAPER_SMC_MAIN"
                        or t.get("strategy_family") == "paper_smc_main"
                    )
                )
                if _is_paper_smc_main_close:
                    if bool(config.get("paper_smc_main_notify_close", True)):
                        _management_send(
                            msg,
                            "paper_smc_main_close",
                            close_price=exit_real,
                            exit_reason=t.get("close_reason") or t.get("exit_type"),
                            r_multiple=t.get("rr_real"),
                            message_price_source="close_price",
                        )
                    else:
                        print(f"[PAPER SMC MAIN CLOSE TELEGRAM SUPPRESSED] {t.get('symbol')} {t.get('side')}")
                        _paper_main_suppressed_context = dict(_management_context_base)
                        _paper_main_suppressed_context.update({
                            "event_detected_ts": t.get("close_time") or time.time(),
                            "close_price": exit_real,
                            "exit_reason": t.get("close_reason") or t.get("exit_type"),
                            "r_multiple": t.get("rr_real"),
                            "message_price_source": "close_price",
                        })
                        _send_management_telegram(
                            t,
                            msg,
                            "paper_smc_main_close",
                            _mode_prefix,
                            _exec_mode,
                            management_context=_paper_main_suppressed_context,
                            attempt_send=False,
                            suppressed=True,
                            suppress_reason="paper_smc_main_notify_close=False",
                        )
                elif _is_research_close:
                    _research_close_context = dict(_management_context_base)
                    _research_close_context.update({
                        "event_detected_ts": t.get("close_time") or time.time(),
                        "close_price": exit_real,
                        "exit_reason": t.get("close_reason") or t.get("exit_type"),
                        "r_multiple": t.get("rr_real"),
                        "message_price_source": "close_price",
                    })
                    paper_smc_research_observe_close(
                        t,
                        ctx=ctx,
                        management_context=_research_close_context,
                    )
                else:
                    _management_send(
                        msg,
                        "trade_close",
                        close_price=exit_real,
                        exit_reason=t.get("close_reason") or t.get("exit_type"),
                        r_multiple=t.get("rr_real"),
                        message_price_source="close_price",
                    )
                print(f"[EXIT] {t['symbol']} {t['status']} {round(t['rr_real'],2)}R")
                #print(f"[EXIT DEBUG] {t['symbol']} entry={round(entry_real, 6)} sl_used={round(t.get('sl', 0), 6)} exit_price={round(t.get('exit_price', 0), 6)}")
                #print(f"[EXIT REASON + PRICE] {t['symbol']} {t['side']} exit_type={t.get('exit_type')} exit_price={round(t.get('exit_price', 0), 6)} rr={t.get('rr_real')} entry={round(entry_real, 6)}")
                _paper_quality_write_observation(t, "CLOSED", _exec_mode)
                save_trade(t, _trades_csv)
                save_tier_log(t)    
                log_false_positive(t) 
                log_wyckoff_outcome(t)   
                history.append(t)
    
                # ===== CLEAN FLAGS =====
                t.pop("entry_cd_set", None)
                t.pop("cooldown_set", None)
            # print("CHECK:", t["symbol"], t["status"])
            # ===== ANTI DUPLICATE TRADE =====
        except Exception as _trade_exc:
            _sym = t.get("symbol", "UNKNOWN") if isinstance(t, dict) else "UNKNOWN"
            print(f"[TRADE ERROR] symbol={_sym} trade quarantined — continuing remaining trades. error={_trade_exc}")
    # print("RUN UPDATE")
    # print("TRADES:", trades)
    _active_lock = ctx.lock if ctx is not None else trades_lock
    with _active_lock:
        if ctx is not None and ctx.execution_mode == "live":
            _filtered = [t for t in _trades if t.get("status", "OPEN") == "OPEN" and not t.get("quarantined")]
        else:
            _filtered = [t for t in _trades if t.get("status", "OPEN") == "OPEN"]
        if ctx is not None:
            _trades[:] = _filtered
        else:
            trades[:] = _filtered
        save_open_trades(_trades, _state_file)
    paper_smc_research_monitor_cycle(ctx=ctx)

    if updated:
        if ctx is None or ctx.execution_mode == "paper":
            with open("config.tmp", "w", encoding="utf-8") as f:
                json.dump(_strip_secrets_for_save(config), f, indent=2, ensure_ascii=False)
            os.replace("config.tmp", "config.json")

def scan_phase(executor_contexts=None):
    global _paper_quality_router_context_by_symbol, _paper_quality_router_context_scan_id
    """
    Shared signal generation — execution-agnostic.

    Fetches market data ONCE, builds pool, runs analyze() on all candidates
    WITHOUT filtering by any executor's open trades or cooldowns.

    Returns:
        (raw_data_pool, raw_data_map, all_signals)
        all_signals is sorted by score descending, READ-ONLY after return.
    """
    print(f"\n🔄 Scan at {format_vn_time(time.time())}")
    reset_scan_filter_summary()

    symbols_data = get_symbols_pool()
    normal_scan_symbols = {sym for sym, _vol, _tier in symbols_data}

    t0 = time.time()
    _paper_quality_router_context_by_symbol = {}
    _paper_quality_router_context_scan_id = t0
    raw_data_pool = []
    for sym, vol, tier in symbols_data:
        df5, df15, df1h, df4h, df1d = fetch_multi(sym)
        if df5 is None:
            if DEBUG:
                print(f"[PIPELINE DROP] symbol={sym} stage=SCAN→FETCH reason=df5=None")
            continue
        raw_data_pool.append((sym, tier, df5, df15, df1h, df4h, df1d))
    _refresh_open_trade_market_data(normal_scan_symbols, executor_contexts=executor_contexts)
    t_fetch = round(time.time() - t0, 1)

    raw_data_map = {sym: (df5, df15, df1h, df4h, df1d)
                    for sym, tier, df5, df15, df1h, df4h, df1d in raw_data_pool}

    t1 = time.time()
    confirm_pool = build_pool_pipeline(raw_data_pool, stats)
    t_pool = round(time.time() - t1, 1)
    print(f"🎯 CONFIRM POOL SET: {len(confirm_pool)} symbols")

    seen = set()
    dedup_pool = []
    for entry in confirm_pool:
        if entry["symbol"] not in seen:
            seen.add(entry["symbol"])
            dedup_pool.append(entry)

    t2 = time.time()
    all_signals = []
    _analyze_called = 0
    _skip_no_map = 0
    _skip_no_df5 = 0

    if DEBUG:
        print(f"[ANALYZE_TRACE] CONFIRM_POOL SIZE: {len(dedup_pool)}")

    for entry in dedup_pool:
        symbol = entry["symbol"]

        if symbol not in raw_data_map:
            _skip_no_map += 1
            if DEBUG:
                print(f"[ANALYZE_TRACE] {symbol} | SKIP: not in raw_data_map")
            continue

        df5, df15, df1h, df4h, df1d = raw_data_map[symbol]
        if df5 is None:
            _skip_no_df5 += 1
            if DEBUG:
                print(f"[ANALYZE_TRACE] {symbol} | SKIP: df5=None")
            continue

        _analyze_called += 1
        df_map = {"5m": df5, "15m": df15, "1h": df1h, "4h": df4h, "1d": df1d}

        try:
            t = analyze(symbol, df_map)
            if t:
                t["_pool_tier"] = entry.get("tier", "")
                t["_pool_stage"] = entry.get("pool_stage", "")
                # Belt-and-suspenders: build_trade() already sets signal_created_ts.
                # setdefault ensures we never overwrite the original scan timestamp.
                t.setdefault("signal_created_ts", time.time())
                print(f"[PRE-CHECK PASS] {t['symbol']} {t['side']} score={t['score']} type={entry.get('setup_type','?')}")
                all_signals.append(t)
            else:
                if DEBUG:
                    print(f"[ANALYZE_TRACE] {symbol} | analyze() -> no signal")

            if entry.get("pool_stage") == "PRE_BREAK":
                sw_signal = swing_pipeline(symbol, df5, df15, df15, df1h, df4h, df1d)
                if sw_signal:
                    sw_t = build_trade(symbol, sw_signal, sw_signal["_ctx"], early_size_mult=sw_signal["_size_mult"])
                    if sw_t:
                        sw_t["_pool_tier"] = entry.get("tier", "")
                        sw_t["_pool_stage"] = entry.get("pool_stage", "")
                        sw_t.setdefault("signal_created_ts", time.time())
                        all_signals.append(sw_t)
                        if DEBUG:
                            print(f"[ANALYZE CANDIDATE] {symbol} | pipeline=SWING(PRE_BREAK) score={sw_t.get('score')}")
        except Exception as _sym_err:
            import traceback as _tb_mod
            _sym_tb = _tb_mod.format_exc()
            print(f"[SCAN WARNING] {symbol} malformed signal skipped: {type(_sym_err).__name__}: {_sym_err}")
            print(_sym_tb)
            write_runtime_error(f"SYMBOL/scan_phase/{symbol}", _sym_tb)

    # ===== REGIME TELEMETRY =====
    _exh_counts = {"HEALTHY": 0, "EXTENDED": 0, "EXHAUSTED": 0, "COLLAPSING": 0}
    for _s in all_signals:
        _k = _s.get("exhaustion_cls", "")
        if _k in _exh_counts:
            _exh_counts[_k] += 1
    _total_exh = sum(_exh_counts.values())
    if _total_exh >= 3:
        _extended_pct = _exh_counts["EXTENDED"] / _total_exh
        if _extended_pct >= 0.6:
            print(
                f"[MARKET REGIME] Late-trend EXTENDED continuation | "
                f"{_exh_counts['EXTENDED']}/{_total_exh} signals EXTENDED | "
                f"Defensive mode active"
            )

    for _s in all_signals:
        _s["score"] = apply_signal_scoring(_s)

    all_signals.sort(key=lambda x: x["score"], reverse=True)

    t_analyze = round(time.time() - t2, 1)

    if DEBUG:
        print(f"[ANALYZE_TRACE] EXPECTED: {len(dedup_pool)} | ACTUAL analyze_called: {_analyze_called} | skip_no_map: {_skip_no_map} | skip_df5: {_skip_no_df5}")
    print(f"â± fetch: {t_fetch}s | pool: {t_pool}s | analyze: {t_analyze}s")
    print(f"Signals found: {len(all_signals)}")
    _print_signal_type_summary(all_signals)
    if all_signals:
        print(f"🔥 BEST: {all_signals[0]['symbol']} {all_signals[0]['side']} ({all_signals[0]['score']:.1f})")
    print_scan_filter_summary()
    update_reversal_shadow_outcomes(raw_data_map)
    update_swing_retest_shadow_outcomes(raw_data_map)
    update_early_cont_shadow_outcomes(raw_data_map)
    update_confirm_structural_outcomes(raw_data_map)
    update_paper_smc_v0_2_shadow_outcomes(raw_data_map)
    update_paper_smc_main_open_geometry_observer(raw_data_map)
    _write_strategy_observability(all_signals)
    if config.get("market_regime_router_shadow_enabled", False):
        try:
            import traceback as _mr_traceback
            from market_regime_router_shadow import (
                classify_regime as _mr_classify_regime,
                log_market_regime_shadow as _mr_log_market_regime_shadow,
                should_log_market_regime_shadow as _mr_should_log_market_regime_shadow,
            )

            _mr_scan_id = t0
            _mr_now = time.time()
            _mr_max = int(config.get("market_regime_router_shadow_max_per_scan", 100) or 100)
            _mr_logged = 0
            for _mr_entry in dedup_pool:
                if _mr_logged >= _mr_max:
                    break
                try:
                    _mr_symbol = _mr_entry.get("symbol") if isinstance(_mr_entry, dict) else None
                    if not _mr_symbol or _mr_symbol not in raw_data_map:
                        continue
                    _mr_frames = raw_data_map.get(_mr_symbol) or ()
                    _mr_df5 = _mr_frames[0] if len(_mr_frames) > 0 else None
                    _mr_df15 = _mr_frames[1] if len(_mr_frames) > 1 else None
                    _mr_df1h = _mr_frames[2] if len(_mr_frames) > 2 else None
                    _mr_df4h = _mr_frames[3] if len(_mr_frames) > 3 else None
                    _mr_symbol_signals = [
                        _mr_signal for _mr_signal in all_signals
                        if isinstance(_mr_signal, dict) and _mr_signal.get("symbol") == _mr_symbol
                    ]
                    _mr_row = _mr_classify_regime(
                        _mr_symbol,
                        _mr_df5,
                        _mr_df15,
                        _mr_df1h,
                        _mr_df4h,
                        had_signal=bool(_mr_symbol_signals),
                        candidate_count=len(_mr_symbol_signals),
                        accepted_count=len(_mr_symbol_signals),
                        scan_id=_mr_scan_id,
                        observed_at=_mr_now,
                    )
                    _paper_quality_capture_router_context(_mr_row)
                    if _mr_should_log_market_regime_shadow(_mr_row, now_ts=_mr_now):
                        _mr_log_market_regime_shadow(_mr_row)
                        _mr_logged += 1
                except Exception:
                    write_runtime_error(
                        f"MARKET_REGIME_ROUTER_SHADOW/{_mr_entry.get('symbol', 'UNKNOWN') if isinstance(_mr_entry, dict) else 'UNKNOWN'}",
                        _mr_traceback.format_exc(),
                    )
        except Exception:
            import traceback as _mr_traceback
            write_runtime_error("MARKET_REGIME_ROUTER_SHADOW", _mr_traceback.format_exc())
    _append_scan_feature_snapshots(raw_data_map, all_signals, scan_id=t0, observed_at=time.time())

    return raw_data_pool, raw_data_map, all_signals


def run():
    global pause_until, EQUITY_PEAK, confirm_count_this_cycle
    confirm_count_this_cycle = 0
    if time.time() < pause_until:
        print(f"[PAUSED ACTIVE] skip cycle — resumes at {format_vn_time(pause_until)}")
        return

    now = time.time()
    if pause_until > 0 and now >= pause_until:
        print("[DD RESET] Pause expired → resetting peak and state")
        EQUITY_PEAK = ACCOUNT_BALANCE
        pause_until = 0
        config["pause_until"] = 0
        config["equity_peak"] = EQUITY_PEAK
        try:
            with open("config.json", "r", encoding="utf-8") as _f:
                _disk = json.load(_f)
        except Exception:
            _disk = dict(config)
        _disk["pause_until"] = 0
        _disk["equity_peak"] = EQUITY_PEAK
        with open("config.tmp", "w", encoding="utf-8") as f:
            json.dump(_strip_secrets_for_save(_disk), f, indent=2, ensure_ascii=False)
        os.replace("config.tmp", "config.json")
        send_telegram("🟢 DD RESET — new cycle started")

    print(f"\n🔄 Scan at {format_vn_time(time.time())}")
    reset_scan_filter_summary()

    symbols_data = get_symbols_pool()
    normal_scan_symbols = {sym for sym, _vol, _tier in symbols_data}

    t0 = time.time()
    raw_data_pool = []
    for sym, vol, tier in symbols_data:
        df5, df15, df1h, df4h, df1d = fetch_multi(sym)
        if df5 is None:
            if DEBUG:
                print(f"[PIPELINE DROP] symbol={sym} stage=SCAN→FETCH reason=df5=None")
            continue
        raw_data_pool.append((sym, tier, df5, df15, df1h, df4h, df1d))
    _refresh_open_trade_market_data(normal_scan_symbols)
    t_fetch = round(time.time() - t0, 1)

    # build lookup so confirm_pool loop can reuse already-fetched data
    raw_data_map = {sym: (df5, df15, df1h, df4h, df1d)
                    for sym, tier, df5, df15, df1h, df4h, df1d in raw_data_pool}

    t1 = time.time()
    confirm_pool = build_pool_pipeline(raw_data_pool, stats)
    t_pool = round(time.time() - t1, 1)
    print(f"🎯 CONFIRM POOL SET: {len(confirm_pool)} symbols")

    seen = set()
    dedup_pool = []
    for entry in confirm_pool:
        if entry["symbol"] not in seen:
            seen.add(entry["symbol"])
            dedup_pool.append(entry)

    t2 = time.time()
    _candidate_count = 0
    _best = None
    _analyze_called   = 0
    _skip_no_map      = 0
    _skip_no_df5      = 0
    entry_attempted = set()
    _all_signals = []
    if DEBUG:
        print(f"[ANALYZE_TRACE] CONFIRM_POOL SIZE: {len(dedup_pool)}")
    open_symbols = {t["symbol"] for t in trades if t.get("status", "OPEN") == "OPEN"}
    for entry in dedup_pool:
        symbol = entry["symbol"]

        if symbol in open_symbols:
            if DEBUG:
                print(f"[SKIP] {symbol} (already in trade)")
            continue

        if symbol not in raw_data_map:
            _skip_no_map += 1
            if DEBUG:
                print(f"[ANALYZE_TRACE] {symbol} | SKIP: not in raw_data_map")
                print(f"[PIPELINE TRACE] symbol={symbol} stage=SCAN→CONFIRM→DROP reason=not_in_raw_data_map final=DROP")
            continue

        df5, df15, df1h, df4h, df1d = raw_data_map[symbol]

        if df5 is None:
            _skip_no_df5 += 1
            if DEBUG:
                print(f"[ANALYZE_TRACE] {symbol} | SKIP: df5=None")
                print(f"[PIPELINE TRACE] symbol={symbol} stage=SCAN→CONFIRM→DROP reason=df5=None final=DROP")
            continue

        now = time.time()
        in_entry_cd = symbol in entry_cooldown and now - entry_cooldown[symbol] < ENTRY_COOLDOWN
        in_loss_cd = symbol in cooldown and now - cooldown[symbol] < LOSS_COOLDOWN
        if DEBUG:
            print(f"[COOLDOWN CHECK] {symbol} in_cooldown={in_entry_cd or in_loss_cd}")
        if in_entry_cd:
            if DEBUG:
                print(f"[SKIP COOLDOWN] {symbol}")
            continue
        if in_loss_cd:
            if DEBUG:
                print(f"[SKIP COOLDOWN] {symbol}")
            continue

        if DEBUG:
            print(f"[ANALYZE ONCE] {symbol}")
        _analyze_called += 1
        if DEBUG:
            print(f"[ANALYZE_TRACE] {symbol} | analyze() called ({_analyze_called}/{len(dedup_pool)})")

        df_map = {
            "5m":  df5,
            "15m": df15,
            "1h":  df1h,
            "4h":  df4h,
            "1d":  df1d,
        }

        try:
            t = analyze(symbol, df_map)
            if t:
                print(f"[PRE-CHECK PASS] {t['symbol']} {t['side']} score={t['score']} type={entry.get('setup_type','?')}")
                _candidate_count += 1
                if DEBUG:
                    print(f"[PIPELINE TRACE] symbol={symbol} stage=SCAN->CONFIRM->ANALYZE->ENTRY_ATTEMPT side={t['side']} score={t['score']} pipeline={t.get('entry_type','?')} final=ENTRY_ATTEMPT")
                _all_signals.append(t)
            else:
                if DEBUG:
                    print(f"[ANALYZE_TRACE] {symbol} | analyze() → no signal")
                    print(f"[PIPELINE TRACE] symbol={symbol} stage=SCAN→CONFIRM→ANALYZE→REJECT reason=no_signal_all_pipelines final=REJECT")

            stage = entry.get("pool_stage", "")
            if stage == "PRE_BREAK":
                sw_signal = swing_pipeline(symbol, df5, df15, df15, df1h, df4h, df1d)
                if sw_signal:
                    sw_t = build_trade(symbol, sw_signal, sw_signal["_ctx"], early_size_mult=sw_signal["_size_mult"])
                    if sw_t:
                        _all_signals.append(sw_t)
                        if DEBUG:
                            print(f"[ANALYZE CANDIDATE] {symbol} | pipeline=SWING(PRE_BREAK) score={sw_t.get('score')}")
        except Exception as _sym_err:
            import traceback as _tb_mod
            _sym_tb = _tb_mod.format_exc()
            print(f"[SCAN WARNING] {symbol} malformed signal skipped: {type(_sym_err).__name__}: {_sym_err}")
            print(_sym_tb)
            write_runtime_error(f"SYMBOL/run/{symbol}", _sym_tb)

    # ===== REGIME TELEMETRY =====
    _exh_counts_r = {"HEALTHY": 0, "EXTENDED": 0, "EXHAUSTED": 0, "COLLAPSING": 0}
    for _s in _all_signals:
        _k = _s.get("exhaustion_cls", "")
        if _k in _exh_counts_r:
            _exh_counts_r[_k] += 1
    _total_exh_r = sum(_exh_counts_r.values())
    if _total_exh_r >= 3:
        _extended_pct_r = _exh_counts_r["EXTENDED"] / _total_exh_r
        if _extended_pct_r >= 0.6:
            print(
                f"[MARKET REGIME] Late-trend EXTENDED continuation | "
                f"{_exh_counts_r['EXTENDED']}/{_total_exh_r} signals EXTENDED | "
                f"Defensive mode active"
            )

    for _s in _all_signals:
        _s["score"] = apply_signal_scoring(_s)

    _all_signals.sort(key=lambda x: x["score"], reverse=True)

    _TOP_N = 4
    _confirm_pool  = [_s for _s in _all_signals if not _s.get("entry_type", "").startswith("REVERSAL")]
    _reversal_pool = [_s for _s in _all_signals if _s.get("entry_type", "").startswith("REVERSAL")]

    _selected = _confirm_pool[:2] + _reversal_pool[:1]
    _selected_syms = {_s["symbol"] for _s in _selected}

    for _s in _confirm_pool[2:]:
        if len(_selected) >= _TOP_N:
            break
        if _s["symbol"] not in _selected_syms:
            _selected.append(_s)
            _selected_syms.add(_s["symbol"])

    for _s in _selected:
        if _best is None or _s["score"] > _best["score"]:
            _best = _s

    for t in _selected:
        symbol = t["symbol"]
        if symbol in entry_attempted:
            print(f"[BLOCK] DUPLICATE_TRADE {symbol} reason=duplicate_entry_attempt")
        else:
            entry_attempted.add(symbol)
            success = open_trade(t)
            if success:
                entry_cooldown[symbol] = time.time()
                if DEBUG:
                    print(f"[COOLDOWN SET] {symbol}")
                update_signal_state(symbol, t["side"], t.get("entry_real") or t.get("entry", 0), executed=True)

    t_analyze = round(time.time() - t2, 1)

    if DEBUG:
        print(f"[ANALYZE_TRACE] EXPECTED: {len(dedup_pool)} | ACTUAL analyze_called: {_analyze_called} | skip_no_map: {_skip_no_map} | skip_df5: {_skip_no_df5}")
    print(f"â± fetch: {t_fetch}s | pool: {t_pool}s | analyze: {t_analyze}s")
    print(f"Candidates found: {_candidate_count}")
    _print_signal_type_summary(_all_signals)
    if _best:
        print(f"🔥 BEST: {_best['symbol']} {_best['side']} ({_best['score']:.1f})")
    print_scan_filter_summary()
    update_reversal_shadow_outcomes(raw_data_map)
    update_swing_retest_shadow_outcomes(raw_data_map)
    update_early_cont_shadow_outcomes(raw_data_map)
    update_confirm_structural_outcomes(raw_data_map)
    update_paper_smc_v0_2_shadow_outcomes(raw_data_map)
    update_paper_smc_main_open_geometry_observer(raw_data_map)
    _write_strategy_observability(_all_signals)

    global _csv_last_check
    _now = time.time()
    if _now - _csv_last_check >= 300:
        _csv_last_check = _now
        _monitored_csvs = [
            (TRADES_CSV,               TRADES_CSV),
            ("log_pool_pipeline.csv",  os.path.join("logs", "log_pool_pipeline.csv")),
            ("bos_debug_v4.csv",       os.path.join("logs", "bos_debug_v4.csv")),
        ]
        for _label, _csv in _monitored_csvs:
            if not os.path.exists(_csv):
                print(f"⚠️ CSV MISSING: {_label}")
            else:
                _size = os.path.getsize(_csv)
                if _size == 0:
                    print(f"❌ CSV EMPTY (no writes): {_label}")
                    if _label == "bos_debug_v4.csv":
                        print("💣 BOS LOG NOT WRITING — check bos.py logging calls")
                else:
                    try:
                        with open(_csv, "r") as _f:
                            _rows = sum(1 for _ in _f)
                    except Exception:
                        _rows = 0
                    if _rows < 2:
                        print(f"❌ CSV NO DATA: {_label}")
                        if _label == "bos_debug_v4.csv":
                            print("💣 BOS LOG NOT WRITING — check bos.py logging calls")
                    else:
                        print(f"? CSV OK: {_label} ({_size} bytes)")
