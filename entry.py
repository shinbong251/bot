# ENTRY LAYER v6.9+ — Refactored
import time, math, os, json, csv
from config import DEBUG, DEBUG_FILTERS, config
from trend import get_market_state, trend_h1
from scoring import detect_early_signal, detect_exhaustion_v66, get_candle_class, score_signal, calc_ema_state, compute_risk_pct, calc_continuation_confidence
from exhaustion import compute_exhaustion
from state_manager import log_state, log_scan_early, log_confirm_reject, log_scan_ema, log_bos_fail, log_exhaustion_counterfactual, write_runtime_error
from bos import should_log_bos, bos_seen, bos_count
from helper import is_duplicate_zone, validate_early
from helper import signal_state

stats = {
    "scanned": 0,
    "early_detected": 0,
    "early_block_state": 0,
    "early_block_dist": 0,
    "early_block_pp": 0,
    "early_block_exhaustion": 0,
    "early_pass": 0,
    "ema_fail": 0,
    "bos_fail": 0,
    "candle_fail": 0,
    "core_fail": 0,
    "pass": 0,
}

_base_log_confirm_reject = log_confirm_reject
_shadow_score_ctx = {}
_reversal_shadow_seen = {}
_reversal_shadow_logged_this_scan = 0
_reversal_shadow_outcome_pending = {}
_reversal_shadow_outcome_terminal_seen = set()
_reversal_qualified_shadow_pending = set()
_swing_retest_shadow_outcome_pending = {}
_swing_retest_shadow_outcome_seen = set()
_swing_retest_shadow_outcome_terminal_seen = set()
_early_cont_shadow_outcome_pending = {}
_early_cont_shadow_outcome_seen = set()
_early_cont_shadow_outcome_terminal_seen = set()
_confirm_structural_outcome_pending = {}
_confirm_structural_outcome_seen = set()
_confirm_structural_outcome_terminal_seen = set()
_paper_smc_v0_2_shadow_outcome_pending = {}
_paper_smc_v0_2_shadow_outcome_seen = set()
_paper_smc_v0_2_shadow_outcome_terminal_seen = set()
_paper_smc_main_open_geom_pending = {}
_paper_smc_main_open_geom_seen = set()
_paper_smc_main_open_geom_terminal_seen = set()
_early_shadow_seen = {}
_early_shadow_logged_this_scan = 0
_swing_shadow_seen = {}
_swing_shadow_logged_this_scan = 0
EARLY_CONT_ENTRY_TYPE = "EARLY_CONT"
EARLY_SHADOW_ENTRY_TYPE = "EARLY_CONT_SHADOW"
EARLY_LEGACY_SCORE_TYPE = "EARLY_V2"
REVERSAL_SHADOW_ENTRY_TYPE = "REVERSAL_CONFIRM_SHADOW"
REVERSAL_QUALIFIED_SHADOW_SCHEMA_VERSION = 1
SWING_SHADOW_ENTRY_TYPE = "SWING_RETEST_SHADOW"
STRUCTURAL_CONTEXT_COLLECTOR_SCHEMA_VERSION = "1.3"
STRUCTURAL_CONTEXT_COLLECTOR_SCHEMA_ACTIVATED_TS = time.time()
REVERSAL_SHADOW_TTL_SECS = 900
REVERSAL_SHADOW_OUTCOME_TTL_SECS = 86400
REVERSAL_SHADOW_OUTCOME_MISSING_LIMIT = 3
SWING_RETEST_SHADOW_OUTCOME_MISSING_LIMIT = 3
EARLY_CONT_SHADOW_OUTCOME_MISSING_LIMIT = 3
CONFIRM_STRUCTURAL_OUTCOME_TTL_SECS = 86400
CONFIRM_STRUCTURAL_OUTCOME_MISSING_LIMIT = 3
PAPER_SMC_V0_2_SHADOW_OUTCOME_TTL_SECS = 86400
PAPER_SMC_V0_2_SHADOW_OUTCOME_MISSING_LIMIT = 3
PAPER_SMC_V0_2_SHADOW_TRACKER_VERSION = "paper_smc_v0_2_shadow_v1"
PAPER_SMC_MAIN_OPEN_GEOM_TTL_SECS = 86400
PAPER_SMC_MAIN_OPEN_GEOM_MISSING_LIMIT = 3
PAPER_SMC_MAIN_OPEN_GEOM_VERSION = "paper_smc_main_open_geom_v1"
PAPER_SMC_MAIN_OPEN_GEOM_THEORETICAL_SL_R = -1.0
EARLY_SHADOW_TTL_SECS = 900
SWING_SHADOW_TTL_SECS = 900
SHADOW_OUTCOME_LOG_SWEEP_INTERVAL_SECS = 3600
SHADOW_OUTCOME_LOG_SWEEP_MAX_EXPIRED = 5000
SHADOW_OUTCOME_TERMINAL_STATUSES = {"RESOLVED", "CLOSED", "EXPIRED", "DATA_MISSING"}
_shadow_outcome_log_sweep_last_at = {}

FILTER_BLOCK_KEYS = {
    "LOW_SCORE": "blocked_low_score",
    "TREND_FAIL": "blocked_trend_fail",
    "REVERSAL_CONTEXT_FAIL": "blocked_context",
    "WRONG_SETUP_FOR_MARKET_STATE": "blocked_market_state",
    "MID_SCORE_WEAK_BOS": "blocked_weak_bos",
    "HIGH_SCORE_WEAK_BOS": "blocked_high_score_weak_bos",
}

CONFIRM_FULL_FUNNEL_REJECT_REASONS = {
    "LOW_SCORE",
    "MID_SCORE_WEAK_BOS",
    "RR_FAIL",
    "TREND_FAIL",
}


def get_confirm_structural_outcome_candidates_snapshot():
    try:
        import copy as _copy_mod
        return _copy_mod.deepcopy(list(_confirm_structural_outcome_pending.values()))
    except Exception:
        return []

_scan_filter_summary = {
    "scanned": 0,
    "passed": 0,
    **{key: 0 for key in FILTER_BLOCK_KEYS.values()},
}

REVERSAL_GATE_KEYS = (
    "context_fail",
    "market_state_gate_failure",
    "missing_extended_exhaustion",
    "missing_bos_near",
)

_reversal_gate_summary = {key: 0 for key in REVERSAL_GATE_KEYS}


def reset_scan_filter_summary():
    global _reversal_shadow_logged_this_scan, _early_shadow_logged_this_scan, _swing_shadow_logged_this_scan
    for key in _scan_filter_summary:
        _scan_filter_summary[key] = 0
    for key in _reversal_gate_summary:
        _reversal_gate_summary[key] = 0
    _reversal_shadow_logged_this_scan = 0
    _early_shadow_logged_this_scan = 0
    _swing_shadow_logged_this_scan = 0


def print_scan_filter_summary():
    print("[SCAN SUMMARY]")
    print(f"scanned={_scan_filter_summary['scanned']}")
    print(f"passed={_scan_filter_summary['passed']}")

    block_items = [
        (key, count)
        for key, count in _scan_filter_summary.items()
        if key.startswith("blocked_")
    ]
    block_items.sort(key=lambda item: item[1], reverse=True)
    for key, count in block_items:
        print(f"{key}={count}")

    reversal_items = [(key, count) for key, count in _reversal_gate_summary.items() if count]
    if reversal_items:
        reversal_items.sort(key=lambda item: item[1], reverse=True)
        print("[REVERSAL GATES]")
        for key, count in reversal_items:
            print(f"{key}={count}")


def get_strategy_observability_counters():
    return {
        "REVERSAL_SHADOW": _reversal_shadow_logged_this_scan,
        "EARLY_SHADOW": _early_shadow_logged_this_scan,
        "SWING_SHADOW": _swing_shadow_logged_this_scan,
        "reversal_context_fail": _reversal_gate_summary.get("context_fail", 0),
        "reversal_market_state_gate_failure": _reversal_gate_summary.get("market_state_gate_failure", 0),
        "reversal_missing_extended_exhaustion": _reversal_gate_summary.get("missing_extended_exhaustion", 0),
        "reversal_missing_bos_near": _reversal_gate_summary.get("missing_bos_near", 0),
    }


def _dict_copy(value):
    return dict(value) if isinstance(value, dict) else {}


def _first_structural_value(*values, default=""):
    for value in values:
        if value not in (None, ""):
            return value
    return default


def _json_optional_scalar(value):
    if value in (None, ""):
        return None
    return value


def _collector_key_part(value):
    value = _json_optional_scalar(value)
    return str(value) if value is not None else "null"


def _collector_optional_float(value):
    value = _json_optional_scalar(value)
    if value is None:
        return None
    try:
        value = float(value)
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    except (TypeError, ValueError):
        return None


def _shadow_outcome_log_path(filename):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, "logs", filename)


def _shadow_outcome_key_from_row(row, key_fields):
    if not isinstance(row, dict):
        return None
    for field in key_fields:
        value = _json_optional_scalar(row.get(field))
        if value is not None:
            return str(value)
    return None


def _shadow_outcome_status(row):
    value = _json_optional_scalar(row.get("status") or row.get("event_status"))
    return str(value or "").upper()


def _shadow_outcome_ts_value(value):
    value = _json_optional_scalar(value)
    if value is None:
        return None
    try:
        ts = float(value)
        if ts > 100000000000:
            ts = ts / 1000.0
        if math.isfinite(ts):
            return ts
    except Exception:
        pass
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return time.mktime(time.strptime(text[:26], fmt))
        except Exception:
            continue
    return None


def _shadow_outcome_opened_ts(row):
    for field in (
        "source_timestamp",
        "signal_created_ts",
        "source_signal_created_ts",
        "timestamp_unix",
        "source_row_time",
        "first_seen",
        "opened_at",
        "observed_at",
        "timestamp",
    ):
        ts = _shadow_outcome_ts_value(row.get(field))
        if ts is not None:
            return ts
    return None


def _shadow_outcome_ttl_secs(row, default_ttl_secs):
    ttl = _collector_optional_float(row.get("ttl_secs"))
    if ttl is not None and ttl > 0:
        return ttl
    try:
        ttl = float(default_ttl_secs)
        if ttl > 0 and math.isfinite(ttl):
            return ttl
    except Exception:
        pass
    return None


def _shadow_outcome_expired_row(row, now_ts, opened_ts, ttl_secs, reason_fields):
    expired = dict(row)
    original_event_ts = row.get("observed_at") if "observed_at" in row else row.get("timestamp")
    expired["status"] = "EXPIRED"
    if "observed_at" in expired:
        expired["observed_at"] = now_ts
    else:
        expired["timestamp"] = now_ts
    if "opened_at" not in expired and "first_seen" not in expired:
        expired["opened_at"] = original_event_ts if original_event_ts not in (None, "") else opened_ts
    if opened_ts is not None:
        expired["age_secs"] = max(0.0, now_ts - opened_ts)
    expired["ttl_secs"] = ttl_secs
    for field in reason_fields:
        expired[field] = "ttl_expired"
    if "terminal" in expired:
        expired["terminal"] = True
    return expired


def _append_shadow_outcome_expired_row(filename, row, error_tag):
    try:
        path = _shadow_outcome_log_path(filename)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(_json_safe_value(row), ensure_ascii=False, default=str, sort_keys=True) + "\n")
        return True
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error(error_tag, traceback_mod.format_exc())
        return False


def _maybe_sweep_shadow_outcome_expiries(
    tracker_name,
    filename,
    key_fields,
    default_ttl_secs,
    terminal_seen,
    reason_fields,
    pending=None,
):
    now_ts = time.time()
    last_at = _shadow_outcome_log_sweep_last_at.get(tracker_name, 0)
    if now_ts - last_at < SHADOW_OUTCOME_LOG_SWEEP_INTERVAL_SECS:
        return
    _shadow_outcome_log_sweep_last_at[tracker_name] = now_ts
    try:
        path = _shadow_outcome_log_path(filename)
        if not os.path.exists(path):
            return
        latest_open = {}
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                key = _shadow_outcome_key_from_row(row, key_fields)
                if not key:
                    continue
                status = _shadow_outcome_status(row)
                if status in SHADOW_OUTCOME_TERMINAL_STATUSES:
                    terminal_seen.add(key)
                    latest_open.pop(key, None)
                elif status == "OPEN" and key not in terminal_seen:
                    latest_open[key] = row

        expired_count = 0
        for key, row in latest_open.items():
            if key in terminal_seen:
                continue
            ttl_secs = _shadow_outcome_ttl_secs(row, default_ttl_secs)
            opened_ts = _shadow_outcome_opened_ts(row)
            if ttl_secs is None or opened_ts is None:
                continue
            if now_ts - opened_ts < ttl_secs:
                continue
            expired_row = _shadow_outcome_expired_row(row, now_ts, opened_ts, ttl_secs, reason_fields)
            if _append_shadow_outcome_expired_row(
                filename,
                expired_row,
                f"{tracker_name}_TTL_EXPIRE_SWEEP",
            ):
                terminal_seen.add(key)
                if isinstance(pending, dict):
                    pending.pop(key, None)
                expired_count += 1
                if expired_count >= SHADOW_OUTCOME_LOG_SWEEP_MAX_EXPIRED:
                    break
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error(f"{tracker_name}_TTL_EXPIRE_SWEEP", traceback_mod.format_exc())


def _struct_score_delta_direction(score_delta_value):
    if score_delta_value is None:
        return "UNKNOWN"
    if score_delta_value > 0:
        return "RAISE"
    if score_delta_value < 0:
        return "LOWER"
    return "NO_CHANGE"


def _struct_score_decision_delta(score_current, score_shadow, pass_threshold):
    if score_current is None or score_shadow is None or pass_threshold is None:
        return "UNKNOWN"
    current_pass = score_current >= pass_threshold
    shadow_pass = score_shadow >= pass_threshold
    if current_pass and not shadow_pass:
        return "PASS_TO_FAIL_SHADOW"
    if not current_pass and shadow_pass:
        return "FAIL_TO_PASS_SHADOW"
    return "UNCHANGED"


def _struct_score_simulation(
    score_v2_current,
    structural_score_modifier_shadow,
    threshold_reference=None,
    pass_threshold=None,
    shadow_pass_threshold=None,
):
    score_current = _collector_optional_float(score_v2_current)
    modifier = _collector_optional_float(structural_score_modifier_shadow)
    score_shadow = None
    score_delta_value = None
    if score_current is not None and modifier is not None:
        score_shadow = score_current + modifier
        score_delta_value = score_shadow - score_current
    current_threshold = _collector_optional_float(pass_threshold)
    shadow_threshold = _collector_optional_float(shadow_pass_threshold)
    if shadow_threshold is None:
        shadow_threshold = current_threshold
    threshold_reference = _json_optional_scalar(threshold_reference)
    structural_decision_delta = "UNKNOWN"
    structural_delta_reason = "threshold_unavailable_in_logging_context"
    if score_current is None:
        structural_delta_reason = "score_v2_current_unavailable"
    elif score_shadow is None:
        structural_delta_reason = "score_v2_structural_shadow_unavailable"
    elif current_threshold is not None and shadow_threshold == current_threshold:
        structural_decision_delta = _struct_score_decision_delta(
            score_current,
            score_shadow,
            current_threshold,
        )
        structural_delta_reason = None
    elif current_threshold is not None and shadow_threshold is not None:
        current_pass = score_current >= current_threshold
        shadow_pass = score_shadow >= shadow_threshold
        if current_pass and not shadow_pass:
            structural_decision_delta = "PASS_TO_FAIL_SHADOW"
        elif not current_pass and shadow_pass:
            structural_decision_delta = "FAIL_TO_PASS_SHADOW"
        else:
            structural_decision_delta = "UNCHANGED"
        structural_delta_reason = None
    return {
        "score_v2_current": score_current,
        "score_v2_structural_shadow": score_shadow,
        "score_delta_value": score_delta_value,
        "score_delta_direction": _struct_score_delta_direction(score_delta_value),
        "current_threshold_reference": threshold_reference,
        "current_pass_threshold": current_threshold,
        "structural_shadow_pass_threshold": shadow_threshold,
        "structural_decision_delta": structural_decision_delta,
        "structural_delta_reason": structural_delta_reason,
    }


def _collector_signal_key(symbol, side, entry_type, signal_created_ts, source_row_time):
    key_ts = _json_optional_scalar(signal_created_ts)
    if key_ts is None:
        key_ts = _json_optional_scalar(source_row_time)
    return "|".join([
        _collector_key_part(symbol),
        _collector_key_part(side),
        _collector_key_part(entry_type),
        _collector_key_part(key_ts),
    ])


def _collector_trade_join_fields(symbol, side, entry_type, signal_created_ts, source_row_time, collector_ts, enabled):
    fields = {
        "trade_join_key": None,
        "trade_join_method": "unavailable",
        "trade_join_confidence": None,
        "trade_join_candidates": None,
        "source_signal_ts": _json_optional_scalar(signal_created_ts),
        "source_entry_time": _json_optional_scalar(source_row_time),
    }
    if not enabled:
        return fields

    join_ts = _json_optional_scalar(signal_created_ts)
    method = "signal_created_ts_exact"
    confidence = "high"
    if join_ts is None:
        join_ts = _json_optional_scalar(source_row_time)
        method = "source_row_time_fallback"
        confidence = "medium"
    if join_ts is None:
        join_ts = _json_optional_scalar(collector_ts)
        method = "collector_ts_fallback"
        confidence = "low"

    if join_ts is None:
        return fields

    key = "|".join([
        _collector_key_part(symbol),
        _collector_key_part(side),
        _collector_key_part(entry_type),
        _collector_key_part(join_ts),
    ])
    fields.update({
        "trade_join_key": key,
        "trade_join_method": method,
        "trade_join_confidence": confidence,
        "trade_join_candidates": [key],
    })
    return fields


def _collector_strategy_family(entry_type):
    entry_type = str(entry_type or "").upper()
    if entry_type in ("CONFIRM",):
        return "confirm"
    if entry_type in ("REVERSAL_CONFIRM", REVERSAL_SHADOW_ENTRY_TYPE):
        return "reversal"
    if entry_type in (EARLY_CONT_ENTRY_TYPE, EARLY_SHADOW_ENTRY_TYPE, EARLY_LEGACY_SCORE_TYPE):
        return "early_cont"
    if entry_type == SWING_SHADOW_ENTRY_TYPE or entry_type.startswith("SWING"):
        return "swing_retest"
    return None


def _collector_shadow_outcome_key(entry_type, breakdown):
    if entry_type != REVERSAL_SHADOW_ENTRY_TYPE or not isinstance(breakdown, dict):
        return None
    parts = [
        REVERSAL_SHADOW_ENTRY_TYPE,
        breakdown.get("symbol"),
        breakdown.get("side"),
        breakdown.get("timestamp"),
        breakdown.get("entry"),
        breakdown.get("sl"),
        breakdown.get("tp"),
        breakdown.get("original_reject_reason"),
    ]
    if any(_json_optional_scalar(part) is None for part in parts):
        return None
    return "|".join(str(part) for part in parts)


def _confirm_structural_candidate_type(status, reason, structural_context):
    reason = str(reason or "")
    if status == "SIGNAL" and reason == "ACCEPT":
        return "ACCEPTED_CONFIRM"
    if reason in CONFIRM_FULL_FUNNEL_REJECT_REASONS:
        return reason
    structural_context = _dict_copy(structural_context)
    if (
        structural_context.get("structural_decision_shadow") in ("QUALIFIED", "NEUTRAL", "WOULD_DOWNRANK")
        or structural_context.get("score_delta_direction") in ("RAISE", "LOWER", "NO_CHANGE")
        or _json_optional_scalar(structural_context.get("score_v2_current")) is not None
        or _json_optional_scalar(structural_context.get("score_v2_structural_shadow")) is not None
    ):
        return "STRUCTURAL_INTEREST"
    return None


def _confirm_structural_candidate_selected(status, reason, structural_context):
    structural_context = _dict_copy(structural_context)
    if not structural_context:
        return False
    if status == "SIGNAL" and reason == "ACCEPT":
        return True
    if str(reason or "") in CONFIRM_FULL_FUNNEL_REJECT_REASONS:
        return True
    if structural_context.get("structural_decision_shadow") in ("QUALIFIED", "NEUTRAL", "WOULD_DOWNRANK"):
        return True
    if structural_context.get("score_delta_direction") in ("RAISE", "LOWER", "NO_CHANGE"):
        return True
    if _json_optional_scalar(structural_context.get("score_v2_current")) is not None:
        return True
    if _json_optional_scalar(structural_context.get("score_v2_structural_shadow")) is not None:
        return True
    return False


def _confirm_structural_dedup_key(symbol, side, entry_type, source_row_time, signal_created_ts):
    key_ts = _json_optional_scalar(signal_created_ts)
    if key_ts is None:
        key_ts = _json_optional_scalar(source_row_time)
    return "|".join([
        _collector_key_part(symbol),
        _collector_key_part(side),
        _collector_key_part(entry_type),
        _collector_key_part(key_ts),
    ])


def _confirm_structural_source_ts(signal_created_ts, source_row_time):
    value = _json_optional_scalar(signal_created_ts)
    if value is not None:
        try:
            value = float(value)
            if value > 100000000000:
                value = value / 1000.0
            if math.isfinite(value):
                return value
        except Exception:
            pass
    try:
        parsed = time.strptime(str(source_row_time), "%Y-%m-%d %H:%M:%S")
        return time.mktime(parsed)
    except Exception:
        return time.time()


def _confirm_structural_snapshot(structural_context):
    structural_context = _dict_copy(structural_context)
    list_fields = {
        "bos_evidence",
        "choch_evidence",
        "poi_evidence",
        "volume_evidence",
        "structural_reasons",
    }
    snapshot = {}
    for field in (
        "bos_quality",
        "bos_evidence",
        "choch_quality",
        "choch_evidence",
        "poi_type",
        "poi_source",
        "poi_retest_quality",
        "entry_poi_alignment",
        "poi_location_quality",
        "poi_evidence",
        "volume_confirmation",
        "volume_source_active",
        "volume_data_usable",
        "volume_evidence",
        "trade_location_quality",
        "structural_decision_shadow",
        "structural_score_modifier_shadow",
        "score_v2_current",
        "score_v2_structural_shadow",
        "score_delta_value",
        "score_delta_direction",
        "structural_reasons",
    ):
        value = structural_context.get(field)
        if field in list_fields:
            snapshot[field] = list(value) if isinstance(value, list) else []
        else:
            snapshot[field] = _json_optional_scalar(value)
    return snapshot


def _confirm_structural_geometry_state(entry, sl, tp):
    entry_present = entry is not None
    sl_present = sl is not None
    tp_present = tp is not None
    if not entry_present and not sl_present and not tp_present:
        return {
            "geometry_status": "NO_GEOMETRY",
            "outcome_trackable": False,
            "data_missing_reason": "no_geometry",
            "risk": None,
            "rr": None,
        }
    if not entry_present and not sl_present:
        return {
            "geometry_status": "MISSING_ENTRY_OR_SL",
            "outcome_trackable": False,
            "data_missing_reason": "no_geometry",
            "risk": None,
            "rr": None,
        }
    if not entry_present:
        return {
            "geometry_status": "MISSING_ENTRY",
            "outcome_trackable": False,
            "data_missing_reason": "missing_entry",
            "risk": None,
            "rr": None,
        }
    if not sl_present:
        return {
            "geometry_status": "MISSING_SL",
            "outcome_trackable": False,
            "data_missing_reason": "missing_sl",
            "risk": None,
            "rr": None,
        }
    if not tp_present:
        return {
            "geometry_status": "MISSING_TP",
            "outcome_trackable": False,
            "data_missing_reason": "missing_tp",
            "risk": None,
            "rr": None,
        }
    risk = abs(entry - sl)
    if risk <= 0 or not math.isfinite(risk):
        return {
            "geometry_status": "INVALID_RR",
            "outcome_trackable": False,
            "data_missing_reason": "invalid_rr",
            "risk": None,
            "rr": None,
        }
    rr = abs(tp - entry) / risk
    if rr <= 0 or not math.isfinite(rr):
        return {
            "geometry_status": "INVALID_RR",
            "outcome_trackable": False,
            "data_missing_reason": "invalid_rr",
            "risk": None,
            "rr": None,
        }
    return {
        "geometry_status": "VALID_GEOMETRY",
        "outcome_trackable": True,
        "data_missing_reason": None,
        "risk": risk,
        "rr": rr,
    }


def _append_confirm_structural_outcome_event(candidate, status, **fields):
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(base_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        structural_snapshot = _confirm_structural_snapshot(candidate.get("structural_context"))
        row = {
            "event_type": "CONFIRM_STRUCTURAL_OUTCOME",
            "schema_version": 1,
            "dedup_key": _json_optional_scalar(candidate.get("dedup_key")),
            "timestamp": candidate.get("timestamp"),
            "observed_at": time.time(),
            "status": status,
            "symbol": _json_optional_scalar(candidate.get("symbol")),
            "side": _json_optional_scalar(candidate.get("side")),
            "entry_type": _json_optional_scalar(candidate.get("entry_type")),
            "reason": _json_optional_scalar(candidate.get("reason")),
            "candidate_type": _json_optional_scalar(candidate.get("candidate_type")),
            "entry": _json_optional_scalar(candidate.get("entry")),
            "sl": _json_optional_scalar(candidate.get("sl")),
            "tp": _json_optional_scalar(candidate.get("tp")),
            "rr": _json_optional_scalar(candidate.get("rr")),
            "geometry_status": _json_optional_scalar(candidate.get("geometry_status")) or "UNKNOWN",
            "outcome_trackable": bool(candidate.get("outcome_trackable")),
            "data_missing_reason": _json_optional_scalar(candidate.get("data_missing_reason")),
            "ttl_secs": CONFIRM_STRUCTURAL_OUTCOME_TTL_SECS,
            "time_filter": "candles_with_open_time_gt_source_timestamp",
        }
        row.update(structural_snapshot)
        row.update({k: _json_safe_value(v) for k, v in fields.items()})
        path = os.path.join(log_dir, "confirm_structural_outcomes.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(_json_safe_value(row), ensure_ascii=False, default=str, sort_keys=True) + "\n")
        return True
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("CONFIRM_STRUCTURAL_OUTCOME", traceback_mod.format_exc())
        return False


def _confirm_structural_outcome_fields(candidate):
    return {
        "signal_created_ts": candidate.get("signal_created_ts"),
        "signal_detected_ts": candidate.get("signal_detected_ts"),
        "collector_ts": candidate.get("collector_ts"),
        "detect_minus_created": candidate.get("detect_minus_created"),
        "registration_minus_detected": candidate.get("registration_minus_detected"),
        "detected_ts_present": candidate.get("detected_ts_present", False),
        "bars_back_estimate": candidate.get("bars_back_estimate"),
        "mfe_r": candidate.get("mfe_r"),
        "mae_r": candidate.get("mae_r"),
        "hit_1r": candidate.get("hit_1r", False),
        "hit_1_5r": candidate.get("hit_1_5r", False),
        "hit_2r": candidate.get("hit_2r", False),
        "sl_hit": candidate.get("sl_hit", False),
        "tp_hit": candidate.get("tp_hit", False),
        "first_hit": candidate.get("first_hit", "OPEN"),
        "ambiguous_same_bar": candidate.get("ambiguous_same_bar", False),
        "time_to_1r_secs": candidate.get("time_to_1r_secs"),
        "time_to_1_5r_secs": candidate.get("time_to_1_5r_secs"),
        "time_to_2r_secs": candidate.get("time_to_2r_secs"),
        "time_to_sl_secs": candidate.get("time_to_sl_secs"),
        "time_to_tp_secs": candidate.get("time_to_tp_secs"),
        "bars_elapsed_m5": candidate.get("bars_elapsed_m5", 0),
        "bars_elapsed_m15": candidate.get("bars_elapsed_m15", 0),
        "geometry_status": candidate.get("geometry_status", "UNKNOWN"),
        "outcome_trackable": candidate.get("outcome_trackable", False),
        "data_missing_reason": candidate.get("data_missing_reason"),
    }


def _confirm_structural_outcome_merged_fields(candidate, extra=None):
    fields = _confirm_structural_outcome_fields(candidate)
    if isinstance(extra, dict):
        fields.update(extra)
    return fields


def _register_confirm_structural_outcome(symbol, status, reason, entry_type, breakdown=None, source_row_time=None):
    try:
        if entry_type != "CONFIRM" or not isinstance(breakdown, dict):
            return
        structural_context = _dict_copy(breakdown.get("structural_context"))
        if not _confirm_structural_candidate_selected(status, reason, structural_context):
            return

        accepted_ctx = _dict_copy(breakdown.get("accepted_signal_context"))
        side = _json_optional_scalar(_first_structural_value(breakdown.get("side"), accepted_ctx.get("side")))
        signal_created_ts = _json_optional_scalar(_first_structural_value(
            breakdown.get("signal_created_ts"),
            accepted_ctx.get("signal_created_ts"),
        ))
        signal_detected_ts = _json_optional_scalar(_first_structural_value(
            breakdown.get("signal_detected_ts"),
            accepted_ctx.get("signal_detected_ts"),
        ))
        source_row_time = source_row_time or time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        dedup_key = _confirm_structural_dedup_key(symbol, side, entry_type, source_row_time, signal_created_ts)
        if dedup_key in _confirm_structural_outcome_seen or dedup_key in _confirm_structural_outcome_pending:
            return
        _confirm_structural_outcome_seen.add(dedup_key)

        entry = _collector_optional_float(_first_structural_value(breakdown.get("entry"), accepted_ctx.get("entry")))
        sl = _collector_optional_float(_first_structural_value(breakdown.get("sl"), accepted_ctx.get("sl")))
        tp = _collector_optional_float(_first_structural_value(breakdown.get("tp"), accepted_ctx.get("tp")))
        rr = _collector_optional_float(_first_structural_value(breakdown.get("rr"), accepted_ctx.get("rr")))
        geometry_state = _confirm_structural_geometry_state(entry, sl, tp)
        if geometry_state.get("rr") is not None:
            rr = geometry_state.get("rr")
        source_ts = _confirm_structural_source_ts(signal_created_ts, source_row_time)
        collector_ts = time.time()
        signal_created_ts_float = _collector_optional_float(signal_created_ts)
        signal_detected_ts_float = _collector_optional_float(signal_detected_ts)
        detect_minus_created = (
            collector_ts - signal_created_ts_float
            if signal_created_ts_float is not None
            else None
        )
        registration_minus_detected = (
            collector_ts - signal_detected_ts_float
            if signal_detected_ts_float is not None
            else None
        )
        candidate = {
            "dedup_key": dedup_key,
            "timestamp": source_row_time,
            "source_row_time": source_row_time,
            "signal_created_ts": signal_created_ts,
            "signal_detected_ts": signal_detected_ts,
            "collector_ts": collector_ts,
            "detect_minus_created": detect_minus_created,
            "registration_minus_detected": registration_minus_detected,
            "detected_ts_present": signal_detected_ts_float is not None,
            "bars_back_estimate": (
                round(detect_minus_created / 900.0, 2)
                if detect_minus_created is not None
                else None
            ),
            "source_timestamp": source_ts,
            "symbol": str(symbol or ""),
            "side": str(side or "").upper(),
            "entry_type": "CONFIRM",
            "status": status,
            "reason": str(reason or ""),
            "candidate_type": _confirm_structural_candidate_type(status, reason, structural_context),
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "rr": rr,
            "geometry_status": geometry_state.get("geometry_status", "UNKNOWN"),
            "outcome_trackable": geometry_state.get("outcome_trackable", False),
            "data_missing_reason": geometry_state.get("data_missing_reason"),
            "structural_context": _json_safe_value(structural_context),
            "confirm_entry_acceptance_context": _dict_copy(
                _first_structural_value(
                    breakdown.get("confirm_entry_acceptance_context"),
                    accepted_ctx.get("confirm_entry_acceptance_context"),
                    default=None,
                )
            ),
            "mfe_r": 0.0,
            "mae_r": 0.0,
            "hit_1r": False,
            "hit_1_5r": False,
            "hit_2r": False,
            "sl_hit": False,
            "tp_hit": False,
            "first_hit": "OPEN",
            "ambiguous_same_bar": False,
            "time_to_1r_secs": None,
            "time_to_1_5r_secs": None,
            "time_to_2r_secs": None,
            "time_to_sl_secs": None,
            "time_to_tp_secs": None,
            "bars_elapsed_m5": 0,
            "bars_elapsed_m15": 0,
            "missing_updates": 0,
        }

        if candidate["side"] not in ("LONG", "SHORT"):
            candidate["geometry_status"] = "UNKNOWN"
            candidate["outcome_trackable"] = False
            candidate["data_missing_reason"] = "unknown"
            if _append_confirm_structural_outcome_event(
                candidate,
                "DATA_MISSING",
                **_confirm_structural_outcome_merged_fields(candidate),
            ):
                _confirm_structural_outcome_terminal_seen.add(dedup_key)
            return
        if not candidate.get("outcome_trackable"):
            if _append_confirm_structural_outcome_event(
                candidate,
                "DATA_MISSING",
                **_confirm_structural_outcome_merged_fields(candidate),
            ):
                _confirm_structural_outcome_terminal_seen.add(dedup_key)
            return
        candidate["risk"] = geometry_state.get("risk")
        _confirm_structural_outcome_pending[dedup_key] = candidate
        _append_confirm_structural_outcome_event(
            candidate,
            "OPEN",
            **_confirm_structural_outcome_fields(candidate),
        )
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("CONFIRM_STRUCTURAL_OUTCOME_REGISTER", traceback_mod.format_exc())


def _swing_retest_shadow_dedup_key(payload):
    existing = _json_optional_scalar(payload.get("dedup_key") or payload.get("shadow_outcome_key"))
    if existing:
        return str(existing)
    signal_created_ts = _json_optional_scalar(payload.get("signal_created_ts") or payload.get("timestamp"))
    return "|".join([
        _collector_key_part(payload.get("symbol")),
        _collector_key_part(payload.get("side")),
        _collector_key_part(signal_created_ts),
    ])


def _swing_retest_shadow_source_ts(payload):
    value = _json_optional_scalar(payload.get("signal_created_ts") or payload.get("timestamp"))
    if value is not None:
        try:
            value = float(value)
            if value > 100000000000:
                value = value / 1000.0
            if math.isfinite(value):
                return value
        except Exception:
            pass
    return time.time()


def _swing_retest_shadow_geometry_state(entry, sl, tp, rr=None):
    if entry is None or sl is None:
        return {
            "geometry_status": "NO_GEOMETRY",
            "outcome_trackable": False,
            "data_missing_reason": "no_geometry",
            "risk": None,
            "rr": rr,
        }
    risk = abs(entry - sl)
    if risk <= 0 or not math.isfinite(risk):
        return {
            "geometry_status": "INVALID_RR",
            "outcome_trackable": False,
            "data_missing_reason": "invalid_rr",
            "risk": None,
            "rr": rr,
        }
    if rr is None and tp is not None:
        rr = abs(tp - entry) / risk
    if rr is not None and (rr <= 0 or not math.isfinite(rr)):
        return {
            "geometry_status": "INVALID_RR",
            "outcome_trackable": False,
            "data_missing_reason": "invalid_rr",
            "risk": None,
            "rr": rr,
        }
    return {
        "geometry_status": "VALID_GEOMETRY",
        "outcome_trackable": True,
        "data_missing_reason": None,
        "risk": risk,
        "rr": rr,
    }


def _swing_retest_shadow_structural_snapshot(structural_context):
    structural_context = _dict_copy(structural_context)
    return {
        "bos_quality": _json_optional_scalar(structural_context.get("bos_quality")),
        "choch_quality": _json_optional_scalar(structural_context.get("choch_quality")),
        "volume_confirmation": _json_optional_scalar(structural_context.get("volume_confirmation")),
        "structural_decision_shadow": _json_optional_scalar(structural_context.get("structural_decision_shadow")),
        "trade_location_quality": _json_optional_scalar(structural_context.get("trade_location_quality")),
        "entry_poi_alignment": _json_optional_scalar(structural_context.get("entry_poi_alignment")),
        "premium_discount": _json_optional_scalar(structural_context.get("premium_discount")),
        "retest_quality": _json_optional_scalar(
            structural_context.get("retest_quality")
            or structural_context.get("poi_retest_quality")
        ),
    }


def _swing_retest_shadow_outcome_fields(candidate):
    return {
        "mfe_r": candidate.get("mfe_r", 0.0),
        "mae_r": candidate.get("mae_r", 0.0),
        "hit_1r": candidate.get("hit_1r", False),
        "hit_1_5r": candidate.get("hit_1_5r", False),
        "hit_2r": candidate.get("hit_2r", False),
        "sl_hit": candidate.get("sl_hit", False),
        "tp_hit": candidate.get("tp_hit", False),
        "first_hit": candidate.get("first_hit", "OPEN"),
        "ambiguous_same_bar": candidate.get("ambiguous_same_bar", False),
        "time_to_1r_secs": candidate.get("time_to_1r_secs"),
        "time_to_1_5r_secs": candidate.get("time_to_1_5r_secs"),
        "time_to_2r_secs": candidate.get("time_to_2r_secs"),
        "time_to_sl_secs": candidate.get("time_to_sl_secs"),
        "time_to_tp_secs": candidate.get("time_to_tp_secs"),
        "bars_elapsed_m5": candidate.get("bars_elapsed_m5", 0),
        "bars_elapsed_m15": candidate.get("bars_elapsed_m15", 0),
        "expiry_reason": candidate.get("expiry_reason"),
        "data_missing_reason": candidate.get("data_missing_reason"),
    }


def _append_swing_retest_shadow_outcome_event(candidate, status, **fields):
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(base_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        structural_context = _dict_copy(candidate.get("structural_context"))
        structural_snapshot = _swing_retest_shadow_structural_snapshot(structural_context)
        score_breakdown = _dict_copy(candidate.get("score_breakdown"))
        row = {
            "event_type": "SWING_RETEST_SHADOW_OUTCOME",
            "schema_version": 1,
            "timestamp": time.time(),
            "source_timestamp": candidate.get("source_timestamp"),
            "status": status,
            "symbol": _json_optional_scalar(candidate.get("symbol")),
            "side": _json_optional_scalar(candidate.get("side")),
            "dedup_key": _json_optional_scalar(candidate.get("dedup_key")),
            "entry_type": _json_optional_scalar(candidate.get("entry_type")),
            "entry": _json_optional_scalar(candidate.get("entry")),
            "sl": _json_optional_scalar(candidate.get("sl")),
            "tp": _json_optional_scalar(candidate.get("tp")),
            "rr": _json_optional_scalar(candidate.get("rr")),
            "score": _json_optional_scalar(candidate.get("score")),
            "score_v2": _json_optional_scalar(candidate.get("score_v2")),
            "bos_quality": structural_snapshot.get("bos_quality"),
            "choch_quality": structural_snapshot.get("choch_quality"),
            "volume_confirmation": structural_snapshot.get("volume_confirmation"),
            "structural_decision_shadow": structural_snapshot.get("structural_decision_shadow"),
            "trade_location_quality": structural_snapshot.get("trade_location_quality"),
            "smc_zone": _smc_annotation_value(candidate, "smc_zone", "UNKNOWN"),
            "liquidity_sweep": _smc_annotation_value(candidate, "liquidity_sweep", "NONE"),
            "premium_discount": structural_snapshot.get("premium_discount"),
            "entry_poi_alignment": structural_snapshot.get("entry_poi_alignment"),
            "retest_quality": structural_snapshot.get("retest_quality"),
            "geometry_status": _json_optional_scalar(candidate.get("geometry_status")) or "UNKNOWN",
            "outcome_trackable": bool(candidate.get("outcome_trackable")),
            "source_signal_created_ts": candidate.get("signal_created_ts"),
            "score_breakdown": _json_safe_value(score_breakdown),
            "structural_context": _json_safe_value(structural_context),
            "ttl_secs": _swing_retest_shadow_outcome_ttl_secs(),
            "time_filter": "candles_with_open_time_gt_source_timestamp",
        }
        row.update({k: _json_safe_value(v) for k, v in fields.items()})
        path = os.path.join(log_dir, "swing_retest_shadow_outcomes.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(_json_safe_value(row), ensure_ascii=False, default=str, sort_keys=True) + "\n")
        return True
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("SWING_RETEST_SHADOW_OUTCOME", traceback_mod.format_exc())
        return False


def _register_swing_retest_shadow_outcome(payload):
    try:
        if not _swing_retest_shadow_outcome_enabled() or not isinstance(payload, dict):
            return
        entry_type = str(payload.get("entry_type") or "")
        if entry_type not in ("SWING_RETEST", SWING_SHADOW_ENTRY_TYPE):
            return
        if len(_swing_retest_shadow_outcome_pending) >= _swing_retest_shadow_outcome_max_pending():
            return

        side = str(payload.get("side") or "").upper()
        if side not in ("LONG", "SHORT"):
            return

        entry = _collector_optional_float(payload.get("entry"))
        sl = _collector_optional_float(payload.get("sl"))
        tp = _collector_optional_float(payload.get("tp"))
        rr = _collector_optional_float(payload.get("rr"))
        geometry_state = _swing_retest_shadow_geometry_state(entry, sl, tp, rr=rr)
        if not geometry_state.get("outcome_trackable"):
            return

        dedup_key = _swing_retest_shadow_dedup_key(payload)
        if (
            dedup_key in _swing_retest_shadow_outcome_seen
            or dedup_key in _swing_retest_shadow_outcome_pending
            or dedup_key in _swing_retest_shadow_outcome_terminal_seen
        ):
            return
        _swing_retest_shadow_outcome_seen.add(dedup_key)

        structural_context = _dict_copy(payload.get("structural_context"))
        if not structural_context:
            structural_context = _safe_structural_context(
                signal=payload,
                ctx=payload.get("_ctx") if isinstance(payload.get("_ctx"), dict) else None,
                smc_ctx={
                    "smc_zone": payload.get("smc_zone"),
                    "liquidity_sweep": payload.get("liquidity_sweep"),
                    "bos_confirmation": payload.get("bos_confirmation"),
                    "smc_bias": payload.get("smc_bias"),
                    "range_context": payload.get("range_context"),
                    "invalid_context": payload.get("invalid_context", []),
                },
                reason=payload.get("reason", []),
            )

        source_ts = _swing_retest_shadow_source_ts(payload)
        candidate = {
            "dedup_key": dedup_key,
            "source_timestamp": source_ts,
            "signal_created_ts": payload.get("signal_created_ts") or payload.get("timestamp") or source_ts,
            "symbol": str(payload.get("symbol") or ""),
            "side": side,
            "entry_type": entry_type,
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "rr": geometry_state.get("rr") if geometry_state.get("rr") is not None else rr,
            "risk": geometry_state.get("risk"),
            "score": payload.get("score") if payload.get("score") not in (None, "") else payload.get("_score", ""),
            "score_v2": payload.get("score_v2", ""),
            "score_breakdown": _json_safe_value(payload.get("score_breakdown", {})),
            "smc_zone": _smc_annotation_value(payload, "smc_zone", "UNKNOWN"),
            "liquidity_sweep": _smc_annotation_value(payload, "liquidity_sweep", "NONE"),
            "geometry_status": geometry_state.get("geometry_status"),
            "outcome_trackable": True,
            "structural_context": _json_safe_value(structural_context),
            "mfe_r": 0.0,
            "mae_r": 0.0,
            "hit_1r": False,
            "hit_1_5r": False,
            "hit_2r": False,
            "sl_hit": False,
            "tp_hit": False,
            "first_hit": "OPEN",
            "ambiguous_same_bar": False,
            "time_to_1r_secs": None,
            "time_to_1_5r_secs": None,
            "time_to_2r_secs": None,
            "time_to_sl_secs": None,
            "time_to_tp_secs": None,
            "bars_elapsed_m5": 0,
            "bars_elapsed_m15": 0,
            "missing_updates": 0,
        }
        _swing_retest_shadow_outcome_pending[dedup_key] = candidate
        if _swing_retest_shadow_outcome_log_open():
            _append_swing_retest_shadow_outcome_event(
                candidate,
                "OPEN",
                **_swing_retest_shadow_outcome_fields(candidate),
            )
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("SWING_RETEST_SHADOW_OUTCOME_REGISTER", traceback_mod.format_exc())


def _early_cont_shadow_dedup_key(payload):
    existing = _json_optional_scalar(payload.get("dedup_key") or payload.get("shadow_outcome_key"))
    if existing:
        return str(existing)
    signal_created_ts = _json_optional_scalar(payload.get("signal_created_ts") or payload.get("timestamp"))
    return "|".join([
        _collector_key_part(payload.get("symbol")),
        _collector_key_part(payload.get("side")),
        _collector_key_part(signal_created_ts),
    ])


def _early_cont_shadow_source_ts(payload):
    value = _json_optional_scalar(payload.get("signal_created_ts") or payload.get("timestamp"))
    if value is not None:
        try:
            value = float(value)
            if value > 100000000000:
                value = value / 1000.0
            if math.isfinite(value):
                return value
        except Exception:
            pass
    return time.time()


def _early_cont_shadow_geometry_state(entry, sl, tp, rr=None):
    if entry is None or sl is None:
        return {
            "geometry_status": "NO_GEOMETRY",
            "outcome_trackable": False,
            "data_missing_reason": "no_geometry",
            "risk": None,
            "rr": rr,
        }
    risk = abs(entry - sl)
    if risk <= 0 or not math.isfinite(risk):
        return {
            "geometry_status": "INVALID_RR",
            "outcome_trackable": False,
            "data_missing_reason": "invalid_rr",
            "risk": None,
            "rr": rr,
        }
    if rr is None and tp is not None:
        rr = abs(tp - entry) / risk
    if rr is not None and (rr <= 0 or not math.isfinite(rr)):
        return {
            "geometry_status": "INVALID_RR",
            "outcome_trackable": False,
            "data_missing_reason": "invalid_rr",
            "risk": None,
            "rr": rr,
        }
    return {
        "geometry_status": "VALID_GEOMETRY",
        "outcome_trackable": True,
        "data_missing_reason": None,
        "risk": risk,
        "rr": rr,
    }


def _early_cont_shadow_structural_snapshot(structural_context):
    structural_context = _dict_copy(structural_context)
    return {
        "bos_quality": _json_optional_scalar(structural_context.get("bos_quality")),
        "choch_quality": _json_optional_scalar(structural_context.get("choch_quality")),
        "volume_confirmation": _json_optional_scalar(structural_context.get("volume_confirmation")),
        "structural_decision_shadow": _json_optional_scalar(structural_context.get("structural_decision_shadow")),
        "trade_location_quality": _json_optional_scalar(structural_context.get("trade_location_quality")),
        "entry_poi_alignment": _json_optional_scalar(structural_context.get("entry_poi_alignment")),
        "premium_discount": _json_optional_scalar(structural_context.get("premium_discount")),
    }


def _early_cont_shadow_outcome_fields(candidate):
    return {
        "mfe_r": candidate.get("mfe_r", 0.0),
        "mae_r": candidate.get("mae_r", 0.0),
        "hit_1r": candidate.get("hit_1r", False),
        "hit_1_5r": candidate.get("hit_1_5r", False),
        "hit_2r": candidate.get("hit_2r", False),
        "sl_hit": candidate.get("sl_hit", False),
        "tp_hit": candidate.get("tp_hit", False),
        "first_hit": candidate.get("first_hit", "OPEN"),
        "ambiguous_same_bar": candidate.get("ambiguous_same_bar", False),
        "time_to_1r_secs": candidate.get("time_to_1r_secs"),
        "time_to_1_5r_secs": candidate.get("time_to_1_5r_secs"),
        "time_to_2r_secs": candidate.get("time_to_2r_secs"),
        "time_to_sl_secs": candidate.get("time_to_sl_secs"),
        "time_to_tp_secs": candidate.get("time_to_tp_secs"),
        "bars_elapsed_m5": candidate.get("bars_elapsed_m5", 0),
        "bars_elapsed_m15": candidate.get("bars_elapsed_m15", 0),
        "expiry_reason": candidate.get("expiry_reason"),
        "data_missing_reason": candidate.get("data_missing_reason"),
    }


def _append_early_cont_shadow_outcome_event(candidate, status, **fields):
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(base_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        structural_context = _dict_copy(candidate.get("structural_context"))
        structural_snapshot = _early_cont_shadow_structural_snapshot(structural_context)
        score_breakdown = _dict_copy(candidate.get("score_breakdown"))
        row = {
            "event_type": "EARLY_CONT_SHADOW_OUTCOME",
            "schema_version": 1,
            "timestamp": time.time(),
            "source_timestamp": candidate.get("source_timestamp"),
            "status": status,
            "symbol": _json_optional_scalar(candidate.get("symbol")),
            "side": _json_optional_scalar(candidate.get("side")),
            "dedup_key": _json_optional_scalar(candidate.get("dedup_key")),
            "entry_type": _json_optional_scalar(candidate.get("entry_type")),
            "entry": _json_optional_scalar(candidate.get("entry")),
            "sl": _json_optional_scalar(candidate.get("sl")),
            "tp": _json_optional_scalar(candidate.get("tp")),
            "rr": _json_optional_scalar(candidate.get("rr")),
            "score": _json_optional_scalar(candidate.get("score")),
            "score_v2": _json_optional_scalar(candidate.get("score_v2")),
            "bos_quality": structural_snapshot.get("bos_quality"),
            "choch_quality": structural_snapshot.get("choch_quality"),
            "volume_confirmation": structural_snapshot.get("volume_confirmation"),
            "structural_decision_shadow": structural_snapshot.get("structural_decision_shadow"),
            "trade_location_quality": structural_snapshot.get("trade_location_quality"),
            "smc_zone": _smc_annotation_value(candidate, "smc_zone", "UNKNOWN"),
            "liquidity_sweep": _smc_annotation_value(candidate, "liquidity_sweep", "NONE"),
            "premium_discount": structural_snapshot.get("premium_discount"),
            "entry_poi_alignment": structural_snapshot.get("entry_poi_alignment"),
            "continuation_reason": _json_optional_scalar(candidate.get("continuation_reason")),
            "reject_reason": _json_optional_scalar(candidate.get("reject_reason")),
            "geometry_status": _json_optional_scalar(candidate.get("geometry_status")) or "UNKNOWN",
            "outcome_trackable": bool(candidate.get("outcome_trackable")),
            "source_signal_created_ts": candidate.get("signal_created_ts"),
            "score_breakdown": _json_safe_value(score_breakdown),
            "structural_context": _json_safe_value(structural_context),
            "ttl_secs": _early_cont_shadow_outcome_ttl_secs(),
            "time_filter": "candles_with_open_time_gt_source_timestamp",
        }
        row.update({k: _json_safe_value(v) for k, v in fields.items()})
        path = os.path.join(log_dir, "early_cont_shadow_outcomes.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(_json_safe_value(row), ensure_ascii=False, default=str, sort_keys=True) + "\n")
        return True
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("EARLY_CONT_SHADOW_OUTCOME", traceback_mod.format_exc())
        return False


def _register_early_cont_shadow_outcome(payload):
    try:
        if not _early_cont_shadow_outcome_enabled() or not isinstance(payload, dict):
            return
        entry_type = str(payload.get("entry_type") or "")
        if entry_type not in (EARLY_CONT_ENTRY_TYPE, EARLY_SHADOW_ENTRY_TYPE):
            return
        if len(_early_cont_shadow_outcome_pending) >= _early_cont_shadow_outcome_max_pending():
            return

        side = str(payload.get("side") or "").upper()
        if side not in ("LONG", "SHORT"):
            return

        entry = _collector_optional_float(payload.get("entry"))
        sl = _collector_optional_float(payload.get("sl"))
        tp = _collector_optional_float(payload.get("tp"))
        rr = _collector_optional_float(payload.get("rr"))
        geometry_state = _early_cont_shadow_geometry_state(entry, sl, tp, rr=rr)
        if not geometry_state.get("outcome_trackable"):
            return

        dedup_key = _early_cont_shadow_dedup_key(payload)
        if (
            dedup_key in _early_cont_shadow_outcome_seen
            or dedup_key in _early_cont_shadow_outcome_pending
            or dedup_key in _early_cont_shadow_outcome_terminal_seen
        ):
            return
        _early_cont_shadow_outcome_seen.add(dedup_key)

        structural_context = _dict_copy(payload.get("structural_context"))
        if not structural_context:
            structural_context = _safe_structural_context(
                signal=payload,
                ctx=payload.get("_ctx") if isinstance(payload.get("_ctx"), dict) else None,
                smc_ctx={
                    "smc_zone": payload.get("smc_zone"),
                    "liquidity_sweep": payload.get("liquidity_sweep"),
                    "bos_confirmation": payload.get("bos_confirmation"),
                    "smc_bias": payload.get("smc_bias"),
                    "range_context": payload.get("range_context"),
                    "invalid_context": payload.get("invalid_context", []),
                },
                reason=payload.get("reason", []),
            )

        source_ts = _early_cont_shadow_source_ts(payload)
        candidate = {
            "dedup_key": dedup_key,
            "source_timestamp": source_ts,
            "signal_created_ts": payload.get("signal_created_ts") or payload.get("timestamp") or source_ts,
            "symbol": str(payload.get("symbol") or ""),
            "side": side,
            "entry_type": entry_type,
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "rr": geometry_state.get("rr") if geometry_state.get("rr") is not None else rr,
            "risk": geometry_state.get("risk"),
            "score": payload.get("score") if payload.get("score") not in (None, "") else payload.get("_score", ""),
            "score_v2": payload.get("score_v2", ""),
            "score_breakdown": _json_safe_value(payload.get("score_breakdown", {})),
            "smc_zone": _smc_annotation_value(payload, "smc_zone", "UNKNOWN"),
            "liquidity_sweep": _smc_annotation_value(payload, "liquidity_sweep", "NONE"),
            "continuation_reason": payload.get("continuation_reason") if payload.get("continuation_reason") not in (None, "") else payload.get("cont_factors", ""),
            "reject_reason": payload.get("reject_reason", ""),
            "geometry_status": geometry_state.get("geometry_status"),
            "outcome_trackable": True,
            "structural_context": _json_safe_value(structural_context),
            "mfe_r": 0.0,
            "mae_r": 0.0,
            "hit_1r": False,
            "hit_1_5r": False,
            "hit_2r": False,
            "sl_hit": False,
            "tp_hit": False,
            "first_hit": "OPEN",
            "ambiguous_same_bar": False,
            "time_to_1r_secs": None,
            "time_to_1_5r_secs": None,
            "time_to_2r_secs": None,
            "time_to_sl_secs": None,
            "time_to_tp_secs": None,
            "bars_elapsed_m5": 0,
            "bars_elapsed_m15": 0,
            "missing_updates": 0,
        }
        _early_cont_shadow_outcome_pending[dedup_key] = candidate
        if _early_cont_shadow_outcome_log_open():
            _append_early_cont_shadow_outcome_event(
                candidate,
                "OPEN",
                **_early_cont_shadow_outcome_fields(candidate),
            )
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("EARLY_CONT_SHADOW_OUTCOME_REGISTER", traceback_mod.format_exc())


def _append_structural_context_sample(symbol, status, reason, entry_type, score_old=None, score_v2=None, breakdown=None, source_row_time=None):
    try:
        if not isinstance(breakdown, dict):
            return
        structural_context = _dict_copy(breakdown.get("structural_context"))
        if not structural_context:
            return
        _register_confirm_structural_outcome(
            symbol,
            str(status or ""),
            str(reason or ""),
            str(entry_type or breakdown.get("entry_type") or ""),
            breakdown=breakdown,
            source_row_time=source_row_time,
        )

        entry_type = str(entry_type or breakdown.get("entry_type") or "")
        status = str(status or "")
        reason = str(reason or "")
        shadow_types = {REVERSAL_SHADOW_ENTRY_TYPE, SWING_SHADOW_ENTRY_TYPE, EARLY_SHADOW_ENTRY_TYPE}
        accepted_types = {"CONFIRM", "REVERSAL_CONFIRM", EARLY_CONT_ENTRY_TYPE}
        is_shadow_sample = entry_type in shadow_types
        is_accepted_sample = entry_type in accepted_types and status == "SIGNAL" and reason == "ACCEPT"
        is_confirm_reject_sample = (
            entry_type == "CONFIRM"
            and status == "REJECT"
            and reason in CONFIRM_FULL_FUNNEL_REJECT_REASONS
        )
        if not (is_shadow_sample or is_accepted_sample or is_confirm_reject_sample):
            return

        accepted_ctx = _dict_copy(breakdown.get("accepted_signal_context"))
        smc = _dict_copy(breakdown.get("smc"))
        collector_ts = time.time()
        source_row_time = source_row_time or time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(collector_ts))
        side = _json_optional_scalar(_first_structural_value(breakdown.get("side"), accepted_ctx.get("side")))
        phase = _json_optional_scalar(_first_structural_value(breakdown.get("phase"), accepted_ctx.get("phase")))
        entry = _json_optional_scalar(_first_structural_value(breakdown.get("entry"), accepted_ctx.get("entry")))
        sl = _json_optional_scalar(_first_structural_value(breakdown.get("sl"), accepted_ctx.get("sl")))
        tp = _json_optional_scalar(_first_structural_value(breakdown.get("tp"), accepted_ctx.get("tp")))
        signal_created_ts = _json_optional_scalar(_first_structural_value(
            breakdown.get("signal_created_ts"),
            accepted_ctx.get("signal_created_ts"),
        ))
        rr = _json_optional_scalar(_first_structural_value(breakdown.get("rr"), accepted_ctx.get("rr")))
        risk_distance_pct = _json_optional_scalar(_first_structural_value(
            breakdown.get("risk_distance_pct"),
            breakdown.get("sl_distance_pct"),
            accepted_ctx.get("risk_distance_pct"),
            accepted_ctx.get("sl_distance_pct"),
        ))
        if risk_distance_pct is None:
            entry_float = _collector_optional_float(entry)
            sl_float = _collector_optional_float(sl)
            if entry_float and sl_float is not None:
                risk_distance_pct = abs(entry_float - sl_float) / entry_float
        signal_key = _collector_signal_key(symbol, side, entry_type, signal_created_ts, source_row_time)
        is_shadow = is_shadow_sample
        executor_context = _json_optional_scalar(_first_structural_value(
            breakdown.get("executor_context"),
            breakdown.get("execution_mode"),
            accepted_ctx.get("executor_context"),
            accepted_ctx.get("execution_mode"),
        ))
        if executor_context is None and is_shadow:
            executor_context = "shadow"
        opened_trade = False if is_shadow else None
        trade_join = _collector_trade_join_fields(
            symbol,
            side,
            entry_type,
            signal_created_ts,
            source_row_time,
            collector_ts,
            enabled=(is_accepted_sample and not is_shadow),
        )

        shadow_outcome_key = _collector_shadow_outcome_key(entry_type, breakdown)

        row = {
            "timestamp": source_row_time,
            "collector_schema_version": STRUCTURAL_CONTEXT_COLLECTOR_SCHEMA_VERSION,
            "collector_schema_activated_ts": STRUCTURAL_CONTEXT_COLLECTOR_SCHEMA_ACTIVATED_TS,
            "collector_ts": collector_ts,
            "source_log": "score_shadow_log",
            "source_row_time": _json_optional_scalar(source_row_time),
            "sample_id": "|".join([
                _collector_key_part(source_row_time),
                _collector_key_part(symbol),
                _collector_key_part(entry_type),
                _collector_key_part(side),
                _collector_key_part(status),
                _collector_key_part(signal_created_ts),
            ]),
            "symbol": symbol,
            "entry_type": entry_type,
            "status": status,
            "reason": reason,
            "side": side,
            "phase": phase,
            "executor_context": executor_context,
            "opened_trade": opened_trade,
            "is_shadow": is_shadow,
            "strategy_family": _collector_strategy_family(entry_type),
            "score_old": _json_optional_scalar(score_old),
            "score_v2": _json_optional_scalar(score_v2),
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "rr": rr,
            "risk_distance_pct": risk_distance_pct,
            "bos_type": _json_optional_scalar(_first_structural_value(breakdown.get("bos_type"), accepted_ctx.get("bos_type"))),
            "signal_created_ts": signal_created_ts,
            "smc_zone": _json_optional_scalar(_first_structural_value(breakdown.get("smc_zone"), smc.get("smc_zone"))),
            "liquidity_sweep": _json_optional_scalar(_first_structural_value(breakdown.get("liquidity_sweep"), smc.get("liquidity_sweep"))),
            "bos_confirmation": _json_optional_scalar(_first_structural_value(breakdown.get("bos_confirmation"), smc.get("bos_confirmation"))),
            "smc_bias": _json_optional_scalar(_first_structural_value(breakdown.get("smc_bias"), smc.get("smc_bias"))),
            "range_context": _json_optional_scalar(_first_structural_value(breakdown.get("range_context"), smc.get("range_context"))),
            "invalid_context": _first_structural_value(breakdown.get("invalid_context"), smc.get("invalid_context"), default=[]),
            "signal_key": signal_key,
            "shadow_outcome_key": shadow_outcome_key,
            "dedup_key": shadow_outcome_key,
            "trade_join_key": trade_join["trade_join_key"],
            "trade_join_method": trade_join["trade_join_method"],
            "trade_join_confidence": trade_join["trade_join_confidence"],
            "trade_join_candidates": trade_join["trade_join_candidates"],
            "source_signal_ts": trade_join["source_signal_ts"],
            "source_entry_time": trade_join["source_entry_time"],
        }

        for field in (
            "dow_trend_context",
            "dow_phase",
            "external_structure",
            "internal_structure",
            "bos_quality",
            "bos_evidence",
            "choch_quality",
            "choch_evidence",
            "displacement_quality",
            "liquidity_context",
            "poi_type",
            "poi_source",
            "poi_retest_quality",
            "entry_poi_alignment",
            "poi_location_quality",
            "poi_evidence",
            "premium_discount",
            "pa_confirmation",
            "volume_confirmation",
            "volume_source_active",
            "volume_data_usable",
            "volume_evidence",
            "trade_location_quality",
            "structural_decision_shadow",
            "structural_score_modifier_shadow",
            "score_v2_current",
            "score_v2_structural_shadow",
            "score_delta_value",
            "score_delta_direction",
            "current_threshold_reference",
            "current_pass_threshold",
            "structural_shadow_pass_threshold",
            "structural_decision_delta",
            "structural_delta_reason",
            "full_funnel_source",
            "original_reject_reason",
            "structural_reasons",
        ):
            row[field] = structural_context.get(field, [] if field.endswith("_evidence") or field == "structural_reasons" else None)

        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(base_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        file_path = os.path.join(log_dir, "structural_context_samples.jsonl")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(_json_safe_value(row), ensure_ascii=False, sort_keys=True, default=str) + "\n")
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("STRUCTURAL_CONTEXT_SAMPLE", traceback_mod.format_exc())


def _log_score_shadow(symbol, status, reason, entry_type, score_old=None, score_v2=None, breakdown=None):
    os_mod = __import__("os")
    csv_mod = __import__("csv")
    json_mod = __import__("json")

    base_dir = os_mod.path.dirname(os_mod.path.abspath(__file__))
    log_dir = os_mod.path.join(base_dir, "logs")
    os_mod.makedirs(log_dir, exist_ok=True)
    file = os_mod.path.join(log_dir, "score_shadow_log.csv")
    is_new = not os_mod.path.exists(file)

    row = {
        "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "symbol": symbol,
        "status": status,
        "reason": reason or "",
        "entry_type": entry_type or "",
        "score_old": score_old if score_old is not None else "",
        "score_v2": score_v2 if score_v2 is not None else "",
        "breakdown_json": json_mod.dumps(breakdown, ensure_ascii=False, sort_keys=True) if breakdown is not None else "",
    }

    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv_mod.DictWriter(f, fieldnames=row.keys())
        if is_new:
            w.writeheader()
        w.writerow(row)
    _append_structural_context_sample(
        symbol,
        status,
        reason,
        entry_type,
        score_old=score_old,
        score_v2=score_v2,
        breakdown=breakdown,
        source_row_time=row["time"],
    )


def _reversal_shadow_outcome_key(payload):
    return "|".join(
        [
            REVERSAL_SHADOW_ENTRY_TYPE,
            str(payload.get("symbol", "")),
            str(payload.get("side", "")),
            str(payload.get("timestamp", "")),
            str(payload.get("entry", "")),
            str(payload.get("sl", "")),
            str(payload.get("tp", "")),
            str(payload.get("original_reject_reason", "")),
        ]
    )


def _json_safe_value(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    return str(value)


def _smc_annotation_value(payload, key, default):
    value = payload.get(key)
    return value if value not in (None, "") else default


def _smc_invalid_context_value(payload):
    value = payload.get("invalid_context")
    return value if isinstance(value, list) else []


def _reversal_qualified_shadow_enabled():
    return bool(config.get("reversal_qualified_shadow_enabled", True))


def _reversal_qualified_shadow_log_open_enabled():
    return bool(config.get("reversal_qualified_shadow_log_open", True))


def _reversal_qualified_cfg_list(key, default):
    raw = config.get(key, default)
    if isinstance(raw, (list, tuple, set)):
        return {str(item).upper() for item in raw}
    return {str(raw).upper()} if raw not in (None, "") else set()


def _reversal_qualified_cfg_bool(key, default):
    return bool(config.get(key, default))


def _reversal_qualified_cfg_float(key, default):
    try:
        return float(config.get(key, default))
    except Exception:
        return float(default)


def _reversal_qualified_scalar(candidate, key):
    value = candidate.get(key)
    if value not in (None, ""):
        return value
    structural_context = candidate.get("structural_context")
    if isinstance(structural_context, dict):
        value = structural_context.get(key)
        if value not in (None, ""):
            return value
    return value


def _reversal_qualified_string(candidate, key):
    value = _reversal_qualified_scalar(candidate, key)
    if value in (None, ""):
        return ""
    return str(value).upper()


def _reversal_qualified_eval(candidate):
    reject_flags = []
    missing_fields = []

    entry_type = str(candidate.get("entry_type") or REVERSAL_SHADOW_ENTRY_TYPE)
    if entry_type != REVERSAL_SHADOW_ENTRY_TYPE:
        reject_flags.append("entry_type_not_reversal_shadow")

    try:
        score = float(candidate.get("score"))
    except Exception:
        score = None
        missing_fields.append("score")

    min_score = _reversal_qualified_cfg_float("reversal_qualified_shadow_min_score", -999)
    max_score = _reversal_qualified_cfg_float("reversal_qualified_shadow_max_score", -10)
    if score is not None and (score < min_score or score > max_score):
        reject_flags.append("score_outside_range")

    range_context = _reversal_qualified_string(candidate, "range_context")
    if not range_context:
        missing_fields.append("range_context")
    allowed_range_context = _reversal_qualified_cfg_list(
        "reversal_qualified_shadow_allowed_range_context",
        ["RANGE_LOW", "RANGE_HIGH"],
    )
    if range_context and range_context not in allowed_range_context:
        reject_flags.append("range_context_not_allowed")
    if (
        _reversal_qualified_cfg_bool("reversal_qualified_shadow_reject_range_mid", True)
        and range_context == "RANGE_MID"
    ):
        reject_flags.append("range_mid")

    bos_confirmation = _reversal_qualified_string(candidate, "bos_confirmation")
    if not bos_confirmation:
        missing_fields.append("bos_confirmation")
    allowed_bos_confirmation = _reversal_qualified_cfg_list(
        "reversal_qualified_shadow_allowed_bos_confirmation",
        ["NEAR", "CLOSE_THROUGH"],
    )
    if bos_confirmation and bos_confirmation not in allowed_bos_confirmation:
        reject_flags.append("bos_confirmation_not_allowed")

    phase = _reversal_qualified_string(candidate, "phase")
    if not phase:
        missing_fields.append("phase")
    if (
        _reversal_qualified_cfg_bool("reversal_qualified_shadow_reject_breakout_strong", True)
        and phase == "BREAKOUT_STRONG"
    ):
        reject_flags.append("breakout_strong")

    liquidity_context = _reversal_qualified_string(candidate, "liquidity_context")
    liquidity_sweep = _reversal_qualified_string(candidate, "liquidity_sweep")
    if _reversal_qualified_cfg_bool("reversal_qualified_shadow_reject_sweep_high", True):
        if liquidity_context == "SWEEP_HIGH":
            reject_flags.append("liquidity_context_sweep_high")
        if liquidity_sweep == "SWEEP_HIGH":
            reject_flags.append("liquidity_sweep_high")

    if missing_fields:
        reject_flags.append("insufficient_fields")

    return {
        "qualified": not reject_flags,
        "reject_flags": reject_flags,
        "missing_fields": missing_fields,
        "score": score,
        "range_context": range_context,
        "bos_confirmation": bos_confirmation,
        "phase": phase,
        "liquidity_context": liquidity_context,
        "liquidity_sweep": liquidity_sweep,
        "extended_preferred": (
            bool(config.get("reversal_qualified_shadow_soft_prefer_extended", True))
            and _reversal_qualified_string(candidate, "exhaustion") == "EXTENDED"
        ),
    }


def _append_reversal_qualified_shadow_event(candidate, event_type, status, **fields):
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(base_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)

        eval_result = _reversal_qualified_eval(candidate)
        structural_context = candidate.get("structural_context")
        if not isinstance(structural_context, dict):
            structural_context = {}

        row = {
            "timestamp": time.time(),
            "event_type": event_type,
            "schema_version": REVERSAL_QUALIFIED_SHADOW_SCHEMA_VERSION,
            "dedup_key": _json_optional_scalar(candidate.get("dedup_key")),
            "symbol": candidate.get("symbol", ""),
            "side": candidate.get("side", ""),
            "score": eval_result.get("score"),
            "range_context": eval_result.get("range_context", ""),
            "bos_confirmation": eval_result.get("bos_confirmation", ""),
            "phase": eval_result.get("phase", ""),
            "liquidity_context": eval_result.get("liquidity_context", ""),
            "liquidity_sweep": eval_result.get("liquidity_sweep", ""),
            "exhaustion": candidate.get("exhaustion", ""),
            "extended_preferred": eval_result.get("extended_preferred", False),
            "qualified_reason": "v0.1_core_mean_reversion",
            "reject_flags": _json_safe_value(eval_result.get("reject_flags", [])),
            "entry": candidate.get("entry", ""),
            "sl": candidate.get("sl", ""),
            "tp": candidate.get("tp", ""),
            "rr": candidate.get("rr", ""),
            "status": status,
            "first_hit": candidate.get("first_hit", "OPEN"),
            "mfe_r": candidate.get("mfe_r", 0.0),
            "mae_r": candidate.get("mae_r", 0.0),
            "hit_1r": candidate.get("hit_1r", False),
            "hit_1_5r": candidate.get("hit_1_5r", False),
            "hit_2r": candidate.get("hit_2r", False),
            "sl_hit": candidate.get("sl_hit", False),
            "tp_hit": candidate.get("tp_hit", False),
            "ambiguous_same_bar": candidate.get("ambiguous_same_bar", False),
            "time_to_1r_secs": candidate.get("time_to_1r_secs"),
            "time_to_1_5r_secs": candidate.get("time_to_1_5r_secs"),
            "time_to_2r_secs": candidate.get("time_to_2r_secs"),
            "bars_elapsed_m5": candidate.get("bars_elapsed_m5", 0),
            "bars_elapsed_m15": candidate.get("bars_elapsed_m15", 0),
        }
        row.update({k: _json_safe_value(v) for k, v in fields.items()})
        path = os.path.join(log_dir, "reversal_qualified_shadow.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("REVERSAL_QUALIFIED_SHADOW", traceback_mod.format_exc())


def _maybe_open_reversal_qualified_shadow(candidate):
    try:
        if not _reversal_qualified_shadow_enabled():
            return
        key = candidate.get("dedup_key")
        if not key or key in _reversal_qualified_shadow_pending:
            return
        eval_result = _reversal_qualified_eval(candidate)
        if not eval_result.get("qualified"):
            return
        _reversal_qualified_shadow_pending.add(key)
        if _reversal_qualified_shadow_log_open_enabled():
            _append_reversal_qualified_shadow_event(
                candidate,
                "REVERSAL_QUALIFIED_SHADOW_OPEN",
                "OPEN",
                reject_flags=[],
            )
    except Exception:
        pass


def _maybe_close_reversal_qualified_shadow(candidate, status, **fields):
    try:
        key = candidate.get("dedup_key")
        if key not in _reversal_qualified_shadow_pending:
            return
        event_map = {
            "RESOLVED": "REVERSAL_QUALIFIED_SHADOW_RESOLVED",
            "EXPIRED": "REVERSAL_QUALIFIED_SHADOW_EXPIRED",
            "DATA_MISSING": "REVERSAL_QUALIFIED_SHADOW_DATA_MISSING",
        }
        event_type = event_map.get(status)
        if not event_type:
            return
        _append_reversal_qualified_shadow_event(candidate, event_type, status, **fields)
        _reversal_qualified_shadow_pending.discard(key)
    except Exception:
        pass


def _reversal_structural_snapshot(structural_context):
    structural_context = _dict_copy(structural_context)
    list_defaults = {
        "bos_evidence",
        "choch_evidence",
        "poi_evidence",
        "volume_evidence",
        "structural_reasons",
    }
    snapshot = {}
    for field in (
        "bos_quality",
        "bos_evidence",
        "choch_quality",
        "choch_evidence",
        "poi_type",
        "poi_source",
        "poi_retest_quality",
        "entry_poi_alignment",
        "poi_location_quality",
        "poi_evidence",
        "volume_confirmation",
        "volume_source_active",
        "volume_data_usable",
        "volume_evidence",
        "trade_location_quality",
        "structural_decision_shadow",
        "structural_score_modifier_shadow",
        "score_v2_current",
        "score_v2_structural_shadow",
        "score_delta_value",
        "score_delta_direction",
        "current_threshold_reference",
        "current_pass_threshold",
        "structural_shadow_pass_threshold",
        "structural_decision_delta",
        "structural_delta_reason",
        "structural_reasons",
    ):
        value = structural_context.get(field)
        if field in list_defaults:
            snapshot[field] = list(value) if isinstance(value, list) else []
        else:
            snapshot[field] = _json_optional_scalar(value)
    return snapshot


def _append_reversal_shadow_outcome_event(candidate, status, **fields):
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(base_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        dedup_key = _json_optional_scalar(candidate.get("dedup_key"))
        structural_context = _dict_copy(candidate.get("structural_context"))
        if not structural_context:
            structural_context = _safe_structural_context(signal=candidate, reason=candidate.get("reason", []))
        structural_snapshot = _reversal_structural_snapshot(structural_context)
        row = {
            "event_type": "REVERSAL_SHADOW_OUTCOME",
            "schema_version": 1,
            "status": status,
            "dedup_key": dedup_key,
            "shadow_outcome_key": dedup_key,
            "source_timestamp": candidate.get("source_timestamp", ""),
            "source_row_time": candidate.get("source_row_time", ""),
            "observed_at": time.time(),
            "symbol": candidate.get("symbol", ""),
            "side": candidate.get("side", ""),
            "entry": candidate.get("entry", ""),
            "sl": candidate.get("sl", ""),
            "tp": candidate.get("tp", ""),
            "rr": candidate.get("rr", ""),
            "risk": candidate.get("risk", ""),
            "score": candidate.get("score", ""),
            "phase": candidate.get("phase", ""),
            "bos_type": candidate.get("bos_type", ""),
            "exhaustion": candidate.get("exhaustion", ""),
            "market_state": candidate.get("market_state", ""),
            "original_reject_reason": candidate.get("original_reject_reason", ""),
            "shadow_candidate_class": candidate.get("shadow_candidate_class", ""),
            "geometry_flags": _json_safe_value(candidate.get("geometry_flags", [])),
            "reason": _json_safe_value(candidate.get("reason", [])),
            "tags": _json_safe_value(candidate.get("tags", [])),
            "smc_zone": _smc_annotation_value(candidate, "smc_zone", "UNKNOWN"),
            "liquidity_sweep": _smc_annotation_value(candidate, "liquidity_sweep", "NONE"),
            "bos_confirmation": _smc_annotation_value(candidate, "bos_confirmation", "UNKNOWN"),
            "smc_bias": _smc_annotation_value(candidate, "smc_bias", "NEUTRAL"),
            "range_context": _smc_annotation_value(candidate, "range_context", "UNKNOWN"),
            "invalid_context": _json_safe_value(_smc_invalid_context_value(candidate)),
            "structural_context": _json_safe_value(structural_context),
            "ttl_secs": REVERSAL_SHADOW_OUTCOME_TTL_SECS,
            "time_filter": "candles_with_open_time_gt_source_timestamp",
        }
        row.update(_json_safe_value(structural_snapshot))
        row.update({k: _json_safe_value(v) for k, v in fields.items()})
        path = os.path.join(log_dir, "reversal_shadow_outcomes.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, default=str, sort_keys=True) + "\n")
        return True
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("REVERSAL_SHADOW_OUTCOME", traceback_mod.format_exc())
        return False


def _register_reversal_shadow_outcome(payload):
    try:
        if payload.get("entry_type") != REVERSAL_SHADOW_ENTRY_TYPE:
            return
        if payload.get("geometry_status") != "computed" or payload.get("valid_geometry") is not True:
            return

        entry = float(payload.get("entry"))
        sl = float(payload.get("sl"))
        tp = float(payload.get("tp"))
        risk = abs(entry - sl)
        if risk <= 0:
            candidate = {
                "dedup_key": _reversal_shadow_outcome_key(payload),
                "source_timestamp": payload.get("timestamp", time.time()),
                "source_row_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "symbol": payload.get("symbol", ""),
                "side": payload.get("side", ""),
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "rr": payload.get("rr", ""),
                "risk": risk,
            }
            _append_reversal_shadow_outcome_event(
                candidate,
                "DATA_MISSING",
                data_missing_reason="risk_invalid",
                mfe_r=0.0,
                mae_r=0.0,
                hit_1r=False,
                hit_1_5r=False,
                hit_2r=False,
                sl_hit=False,
                tp_hit=False,
                first_hit="OPEN",
                ambiguous_same_bar=False,
                time_to_1r_secs=None,
                time_to_1_5r_secs=None,
                time_to_2r_secs=None,
                time_to_sl_secs=None,
                time_to_tp_secs=None,
                bars_elapsed_m5=0,
                bars_elapsed_m15=0,
            )
            return

        source_ts = float(payload.get("timestamp") or time.time())
        candidate = {
            "dedup_key": _reversal_shadow_outcome_key(payload),
            "entry_type": payload.get("entry_type", REVERSAL_SHADOW_ENTRY_TYPE),
            "source_timestamp": source_ts,
            "source_row_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(source_ts)),
            "symbol": str(payload.get("symbol", "")),
            "side": str(payload.get("side", "")).upper(),
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "rr": payload.get("rr", ""),
            "risk": risk,
            "score": payload.get("score", ""),
            "phase": payload.get("phase", ""),
            "bos_type": payload.get("bos_type", ""),
            "exhaustion": payload.get("exhaustion", ""),
            "market_state": payload.get("market_state", ""),
            "original_reject_reason": payload.get("original_reject_reason", ""),
            "shadow_candidate_class": payload.get("shadow_candidate_class", ""),
            "geometry_flags": _json_safe_value(payload.get("geometry_flags", [])),
            "reason": _json_safe_value(payload.get("reason", [])),
            "tags": _json_safe_value(payload.get("tags", [])),
            "smc_zone": _smc_annotation_value(payload, "smc_zone", "UNKNOWN"),
            "liquidity_sweep": _smc_annotation_value(payload, "liquidity_sweep", "NONE"),
            "bos_confirmation": _smc_annotation_value(payload, "bos_confirmation", "UNKNOWN"),
            "smc_bias": _smc_annotation_value(payload, "smc_bias", "NEUTRAL"),
            "range_context": _smc_annotation_value(payload, "range_context", "UNKNOWN"),
            "invalid_context": _json_safe_value(_smc_invalid_context_value(payload)),
            "structural_context": _json_safe_value(
                payload.get("structural_context")
                or _safe_structural_context(signal=payload, reason=payload.get("reason", []))
            ),
            "mfe_r": 0.0,
            "mae_r": 0.0,
            "hit_1r": False,
            "hit_1_5r": False,
            "hit_2r": False,
            "sl_hit": False,
            "tp_hit": False,
            "first_hit": "OPEN",
            "ambiguous_same_bar": False,
            "time_to_1r_secs": None,
            "time_to_1_5r_secs": None,
            "time_to_2r_secs": None,
            "time_to_sl_secs": None,
            "time_to_tp_secs": None,
            "bars_elapsed_m5": 0,
            "bars_elapsed_m15": 0,
            "missing_updates": 0,
        }
        if candidate["side"] not in ("LONG", "SHORT"):
            _append_reversal_shadow_outcome_event(
                candidate,
                "DATA_MISSING",
                data_missing_reason="side_invalid",
                mfe_r=candidate.get("mfe_r", 0.0),
                mae_r=candidate.get("mae_r", 0.0),
                hit_1r=candidate.get("hit_1r", False),
                hit_1_5r=candidate.get("hit_1_5r", False),
                hit_2r=candidate.get("hit_2r", False),
                sl_hit=candidate.get("sl_hit", False),
                tp_hit=candidate.get("tp_hit", False),
                first_hit=candidate.get("first_hit", "OPEN"),
                ambiguous_same_bar=candidate.get("ambiguous_same_bar", False),
                time_to_1r_secs=candidate.get("time_to_1r_secs"),
                time_to_1_5r_secs=candidate.get("time_to_1_5r_secs"),
                time_to_2r_secs=candidate.get("time_to_2r_secs"),
                time_to_sl_secs=candidate.get("time_to_sl_secs"),
                time_to_tp_secs=candidate.get("time_to_tp_secs"),
                bars_elapsed_m5=candidate.get("bars_elapsed_m5", 0),
                bars_elapsed_m15=candidate.get("bars_elapsed_m15", 0),
            )
            return

        key = candidate["dedup_key"]
        if key in _reversal_shadow_outcome_pending or key in _reversal_shadow_outcome_terminal_seen:
            return
        _reversal_shadow_outcome_pending[key] = candidate
        _append_reversal_shadow_outcome_event(
            candidate,
            "OPEN",
            mfe_r=candidate.get("mfe_r", 0.0),
            mae_r=candidate.get("mae_r", 0.0),
            hit_1r=candidate.get("hit_1r", False),
            hit_1_5r=candidate.get("hit_1_5r", False),
            hit_2r=candidate.get("hit_2r", False),
            sl_hit=candidate.get("sl_hit", False),
            tp_hit=candidate.get("tp_hit", False),
            first_hit=candidate.get("first_hit", "OPEN"),
            ambiguous_same_bar=candidate.get("ambiguous_same_bar", False),
            time_to_1r_secs=candidate.get("time_to_1r_secs"),
            time_to_1_5r_secs=candidate.get("time_to_1_5r_secs"),
            time_to_2r_secs=candidate.get("time_to_2r_secs"),
            time_to_sl_secs=candidate.get("time_to_sl_secs"),
            time_to_tp_secs=candidate.get("time_to_tp_secs"),
            bars_elapsed_m5=candidate.get("bars_elapsed_m5", 0),
            bars_elapsed_m15=candidate.get("bars_elapsed_m15", 0),
        )
        _maybe_open_reversal_qualified_shadow(candidate)
    except Exception:
        pass


def _candle_open_ts(candle):
    try:
        value = float(candle.get("time"))
        if value > 100000000000:
            value = value / 1000.0
        return value
    except Exception:
        return None


def _count_bars_after(df, source_ts):
    try:
        if df is None:
            return 0
        count = 0
        for _, candle in df.iterrows():
            candle_ts = _candle_open_ts(candle)
            if candle_ts is not None and candle_ts > source_ts:
                count += 1
        return count
    except Exception:
        return 0


def _evaluate_reversal_shadow_candidate(candidate, df5, df15):
    source_ts = float(candidate.get("source_timestamp") or 0)
    now_ts = time.time()
    candidate["bars_elapsed_m5"] = _count_bars_after(df5, source_ts)
    candidate["bars_elapsed_m15"] = _count_bars_after(df15, source_ts)

    df_eval = df5 if df5 is not None else df15
    if df_eval is None:
        candidate["missing_updates"] = candidate.get("missing_updates", 0) + 1
        if candidate["missing_updates"] >= REVERSAL_SHADOW_OUTCOME_MISSING_LIMIT:
            return "DATA_MISSING", {"data_missing_reason": "candles_unavailable"}
        return None, {}
    candidate["missing_updates"] = 0

    side = candidate.get("side")
    entry = float(candidate.get("entry"))
    sl = float(candidate.get("sl"))
    tp = float(candidate.get("tp"))
    risk = float(candidate.get("risk"))
    first_hit = candidate.get("first_hit") or "OPEN"

    for _, candle in df_eval.iterrows():
        candle_ts = _candle_open_ts(candle)
        if candle_ts is None or candle_ts <= source_ts:
            continue
        try:
            high = float(candle.get("high"))
            low = float(candle.get("low"))
        except Exception:
            continue

        if side == "LONG":
            favorable = max(0.0, high - entry)
            adverse = max(0.0, entry - low)
            sl_hit_now = low <= sl
            tp_hit_now = high >= tp
        elif side == "SHORT":
            favorable = max(0.0, entry - low)
            adverse = max(0.0, high - entry)
            sl_hit_now = high >= sl
            tp_hit_now = low <= tp
        else:
            return "DATA_MISSING", {"data_missing_reason": "side_invalid"}

        mfe_r_now = favorable / risk
        mae_r_now = adverse / risk
        candidate["mfe_r"] = max(float(candidate.get("mfe_r") or 0), mfe_r_now)
        candidate["mae_r"] = max(float(candidate.get("mae_r") or 0), mae_r_now)

        hit_1r_now = mfe_r_now >= 1.0
        hit_1_5r_now = mfe_r_now >= 1.5
        hit_2r_now = mfe_r_now >= 2.0
        favorable_hit_now = tp_hit_now or hit_2r_now or hit_1_5r_now or hit_1r_now
        if sl_hit_now and favorable_hit_now:
            candidate["ambiguous_same_bar"] = True

        if hit_1r_now and not candidate.get("hit_1r"):
            candidate["hit_1r"] = True
            candidate["time_to_1r_secs"] = candle_ts - source_ts
        if hit_1_5r_now:
            if not candidate.get("hit_1_5r"):
                candidate["time_to_1_5r_secs"] = candle_ts - source_ts
            candidate["hit_1_5r"] = True
        if hit_2r_now:
            if not candidate.get("hit_2r"):
                candidate["time_to_2r_secs"] = candle_ts - source_ts
            candidate["hit_2r"] = True
        if sl_hit_now and not candidate.get("sl_hit"):
            candidate["sl_hit"] = True
            candidate["time_to_sl_secs"] = candle_ts - source_ts
        if tp_hit_now and not candidate.get("tp_hit"):
            candidate["tp_hit"] = True
            candidate["time_to_tp_secs"] = candle_ts - source_ts

        if first_hit == "OPEN" and (sl_hit_now or favorable_hit_now):
            if sl_hit_now and favorable_hit_now:
                first_hit = "AMBIGUOUS"
                candidate["ambiguous_same_bar"] = True
            elif sl_hit_now:
                first_hit = "SL"
            elif tp_hit_now:
                first_hit = "TP"
            elif hit_2r_now:
                first_hit = "2R"
            elif hit_1_5r_now:
                first_hit = "1.5R"
            else:
                first_hit = "1R"
            candidate["first_hit"] = first_hit

    if now_ts - source_ts >= REVERSAL_SHADOW_OUTCOME_TTL_SECS:
        return "EXPIRED", {}
    if candidate.get("sl_hit") or candidate.get("tp_hit") or candidate.get("hit_2r"):
        return "RESOLVED", {}
    return None, {}


def _evaluate_confirm_structural_candidate(candidate, df5, df15):
    source_ts = float(candidate.get("source_timestamp") or 0)
    now_ts = time.time()
    candidate["bars_elapsed_m5"] = _count_bars_after(df5, source_ts)
    candidate["bars_elapsed_m15"] = _count_bars_after(df15, source_ts)

    df_eval = df5 if df5 is not None else df15
    if df_eval is None:
        candidate["missing_updates"] = candidate.get("missing_updates", 0) + 1
        if candidate["missing_updates"] >= CONFIRM_STRUCTURAL_OUTCOME_MISSING_LIMIT:
            candidate["data_missing_reason"] = "candle_data_unavailable"
            return "DATA_MISSING", {"data_missing_reason": "candle_data_unavailable"}
        return None, {}

    side = candidate.get("side")
    entry = float(candidate.get("entry"))
    sl = float(candidate.get("sl"))
    tp = float(candidate.get("tp"))
    risk = float(candidate.get("risk"))
    first_hit = candidate.get("first_hit") or "OPEN"

    for _, candle in df_eval.iterrows():
        candle_ts = _candle_open_ts(candle)
        if candle_ts is None or candle_ts <= source_ts:
            continue
        try:
            high = float(candle.get("high"))
            low = float(candle.get("low"))
        except Exception:
            continue

        if side == "LONG":
            favorable = max(0.0, high - entry)
            adverse = max(0.0, entry - low)
            sl_hit_now = low <= sl
            tp_hit_now = high >= tp
        elif side == "SHORT":
            favorable = max(0.0, entry - low)
            adverse = max(0.0, high - entry)
            sl_hit_now = high >= sl
            tp_hit_now = low <= tp
        else:
            candidate["geometry_status"] = "UNKNOWN"
            candidate["outcome_trackable"] = False
            candidate["data_missing_reason"] = "unknown"
            return "DATA_MISSING", {"data_missing_reason": "unknown"}

        mfe_r_now = favorable / risk
        mae_r_now = adverse / risk
        candidate["mfe_r"] = max(float(candidate.get("mfe_r") or 0), mfe_r_now)
        candidate["mae_r"] = max(float(candidate.get("mae_r") or 0), mae_r_now)

        hit_1r_now = mfe_r_now >= 1.0
        hit_1_5r_now = mfe_r_now >= 1.5
        hit_2r_now = mfe_r_now >= 2.0
        favorable_hit_now = tp_hit_now or hit_2r_now or hit_1_5r_now or hit_1r_now
        if sl_hit_now and favorable_hit_now:
            candidate["ambiguous_same_bar"] = True

        if hit_1r_now and not candidate.get("hit_1r"):
            candidate["hit_1r"] = True
            candidate["time_to_1r_secs"] = candle_ts - source_ts
        if hit_1_5r_now:
            if not candidate.get("hit_1_5r"):
                candidate["time_to_1_5r_secs"] = candle_ts - source_ts
            candidate["hit_1_5r"] = True
        if hit_2r_now:
            if not candidate.get("hit_2r"):
                candidate["time_to_2r_secs"] = candle_ts - source_ts
            candidate["hit_2r"] = True
        if sl_hit_now and not candidate.get("sl_hit"):
            candidate["sl_hit"] = True
            candidate["time_to_sl_secs"] = candle_ts - source_ts
        if tp_hit_now and not candidate.get("tp_hit"):
            candidate["tp_hit"] = True
            candidate["time_to_tp_secs"] = candle_ts - source_ts

        if first_hit == "OPEN" and (sl_hit_now or favorable_hit_now):
            if sl_hit_now and favorable_hit_now:
                first_hit = "AMBIGUOUS"
                candidate["ambiguous_same_bar"] = True
            elif sl_hit_now:
                first_hit = "SL"
            elif tp_hit_now:
                first_hit = "TP"
            elif hit_2r_now:
                first_hit = "2R"
            elif hit_1_5r_now:
                first_hit = "1.5R"
            else:
                first_hit = "1R"
            candidate["first_hit"] = first_hit

    if now_ts - source_ts >= CONFIRM_STRUCTURAL_OUTCOME_TTL_SECS:
        return "EXPIRED", {}
    if candidate.get("sl_hit") or candidate.get("tp_hit") or candidate.get("hit_2r"):
        return "RESOLVED", {}
    return None, {}


def update_reversal_shadow_outcomes(raw_data_map):
    try:
        if not isinstance(raw_data_map, dict):
            raw_data_map = {}
        if _reversal_shadow_outcome_pending:
            finished = []
            for key, candidate in list(_reversal_shadow_outcome_pending.items()):
                if key in _reversal_shadow_outcome_terminal_seen:
                    finished.append(key)
                    continue
                symbol = candidate.get("symbol", "")
                frames = raw_data_map.get(symbol)
                df5 = frames[0] if isinstance(frames, tuple) and len(frames) > 0 else None
                df15 = frames[1] if isinstance(frames, tuple) and len(frames) > 1 else None
                status, extra = _evaluate_reversal_shadow_candidate(candidate, df5, df15)
                if status:
                    terminal_fields = {
                        "mfe_r": candidate.get("mfe_r", 0.0),
                        "mae_r": candidate.get("mae_r", 0.0),
                        "hit_1r": candidate.get("hit_1r", False),
                        "hit_1_5r": candidate.get("hit_1_5r", False),
                        "hit_2r": candidate.get("hit_2r", False),
                        "sl_hit": candidate.get("sl_hit", False),
                        "tp_hit": candidate.get("tp_hit", False),
                        "first_hit": candidate.get("first_hit", "OPEN"),
                        "ambiguous_same_bar": candidate.get("ambiguous_same_bar", False),
                        "time_to_1r_secs": candidate.get("time_to_1r_secs"),
                        "time_to_1_5r_secs": candidate.get("time_to_1_5r_secs"),
                        "time_to_2r_secs": candidate.get("time_to_2r_secs"),
                        "time_to_sl_secs": candidate.get("time_to_sl_secs"),
                        "time_to_tp_secs": candidate.get("time_to_tp_secs"),
                        "bars_elapsed_m5": candidate.get("bars_elapsed_m5", 0),
                        "bars_elapsed_m15": candidate.get("bars_elapsed_m15", 0),
                        **extra,
                    }
                    wrote = _append_reversal_shadow_outcome_event(
                        candidate,
                        status,
                        **terminal_fields,
                    )
                    if wrote:
                        _reversal_shadow_outcome_terminal_seen.add(key)
                        _maybe_close_reversal_qualified_shadow(candidate, status, **terminal_fields)
                    else:
                        write_runtime_error(
                            f"REVERSAL_SHADOW_OUTCOME_UPDATE/{key}",
                            f"terminal_write_failed status={status} dedup_key={key}",
                        )
                    finished.append(key)
            for key in finished:
                _reversal_shadow_outcome_pending.pop(key, None)
    except Exception:
        pass
    _maybe_sweep_shadow_outcome_expiries(
        "REVERSAL_SHADOW_OUTCOME",
        "reversal_shadow_outcomes.jsonl",
        ("shadow_outcome_key", "dedup_key"),
        REVERSAL_SHADOW_OUTCOME_TTL_SECS,
        _reversal_shadow_outcome_terminal_seen,
        ("expired_reason",),
        pending=_reversal_shadow_outcome_pending,
    )


def _evaluate_swing_retest_shadow_candidate(candidate, df5, df15):
    source_ts = float(candidate.get("source_timestamp") or 0)
    now_ts = time.time()
    candidate["bars_elapsed_m5"] = _count_bars_after(df5, source_ts)
    candidate["bars_elapsed_m15"] = _count_bars_after(df15, source_ts)

    df_eval = df5 if df5 is not None else df15
    if df_eval is None:
        candidate["missing_updates"] = candidate.get("missing_updates", 0) + 1
        if candidate["missing_updates"] >= SWING_RETEST_SHADOW_OUTCOME_MISSING_LIMIT:
            candidate["data_missing_reason"] = "candles_unavailable"
            return "DATA_MISSING", {"data_missing_reason": "candles_unavailable"}
        return None, {}
    candidate["missing_updates"] = 0

    side = candidate.get("side")
    entry = float(candidate.get("entry"))
    sl = float(candidate.get("sl"))
    tp = candidate.get("tp")
    tp = float(tp) if tp not in (None, "") else None
    risk = float(candidate.get("risk"))
    first_hit = candidate.get("first_hit") or "OPEN"

    for _, candle in df_eval.iterrows():
        candle_ts = _candle_open_ts(candle)
        if candle_ts is None or candle_ts <= source_ts:
            continue
        try:
            high = float(candle.get("high"))
            low = float(candle.get("low"))
        except Exception:
            continue

        if side == "LONG":
            favorable = max(0.0, high - entry)
            adverse = max(0.0, entry - low)
            sl_hit_now = low <= sl
            tp_hit_now = tp is not None and high >= tp
        elif side == "SHORT":
            favorable = max(0.0, entry - low)
            adverse = max(0.0, high - entry)
            sl_hit_now = high >= sl
            tp_hit_now = tp is not None and low <= tp
        else:
            candidate["data_missing_reason"] = "side_invalid"
            return "DATA_MISSING", {"data_missing_reason": "side_invalid"}

        mfe_r_now = favorable / risk
        mae_r_now = adverse / risk
        candidate["mfe_r"] = max(float(candidate.get("mfe_r") or 0), mfe_r_now)
        candidate["mae_r"] = max(float(candidate.get("mae_r") or 0), mae_r_now)

        hit_1r_now = mfe_r_now >= 1.0
        hit_1_5r_now = mfe_r_now >= 1.5
        hit_2r_now = mfe_r_now >= 2.0
        favorable_hit_now = tp_hit_now or hit_2r_now or hit_1_5r_now or hit_1r_now
        if sl_hit_now and favorable_hit_now:
            candidate["ambiguous_same_bar"] = True

        if hit_1r_now and not candidate.get("hit_1r"):
            candidate["hit_1r"] = True
            candidate["time_to_1r_secs"] = candle_ts - source_ts
        if hit_1_5r_now and not candidate.get("hit_1_5r"):
            candidate["hit_1_5r"] = True
            candidate["time_to_1_5r_secs"] = candle_ts - source_ts
        if hit_2r_now and not candidate.get("hit_2r"):
            candidate["hit_2r"] = True
            candidate["time_to_2r_secs"] = candle_ts - source_ts
        if sl_hit_now and not candidate.get("sl_hit"):
            candidate["sl_hit"] = True
            candidate["time_to_sl_secs"] = candle_ts - source_ts
        if tp_hit_now and not candidate.get("tp_hit"):
            candidate["tp_hit"] = True
            candidate["time_to_tp_secs"] = candle_ts - source_ts

        if first_hit == "OPEN" and (sl_hit_now or favorable_hit_now):
            if sl_hit_now and favorable_hit_now:
                first_hit = "UNKNOWN"
                candidate["ambiguous_same_bar"] = True
            elif sl_hit_now:
                first_hit = "SL"
            elif tp_hit_now:
                first_hit = "TP"
            elif hit_2r_now:
                first_hit = "2R"
            elif hit_1_5r_now:
                first_hit = "1.5R"
            else:
                first_hit = "1R"
            candidate["first_hit"] = first_hit
            return "RESOLVED", {}

    if now_ts - source_ts >= _swing_retest_shadow_outcome_ttl_secs():
        candidate["expiry_reason"] = "ttl_elapsed"
        return "EXPIRED", {"expiry_reason": "ttl_elapsed"}
    return None, {}


def update_swing_retest_shadow_outcomes(raw_data_map):
    try:
        if not isinstance(raw_data_map, dict):
            raw_data_map = {}
        if _swing_retest_shadow_outcome_pending:
            finished = []
            for key, candidate in list(_swing_retest_shadow_outcome_pending.items()):
                try:
                    if key in _swing_retest_shadow_outcome_terminal_seen:
                        finished.append(key)
                        continue
                    symbol = candidate.get("symbol", "")
                    frames = raw_data_map.get(symbol)
                    df5 = frames[0] if isinstance(frames, tuple) and len(frames) > 0 else None
                    df15 = frames[1] if isinstance(frames, tuple) and len(frames) > 1 else None
                    status, extra = _evaluate_swing_retest_shadow_candidate(candidate, df5, df15)
                    if status:
                        fields = _swing_retest_shadow_outcome_fields(candidate)
                        if isinstance(extra, dict):
                            fields.update(extra)
                        wrote = _append_swing_retest_shadow_outcome_event(
                            candidate,
                            status,
                            **fields,
                        )
                        if wrote:
                            _swing_retest_shadow_outcome_terminal_seen.add(key)
                        else:
                            write_runtime_error(
                                f"SWING_RETEST_SHADOW_OUTCOME_UPDATE/{key}",
                                f"terminal_write_failed status={status} dedup_key={key}",
                            )
                        finished.append(key)
                except Exception:
                    traceback_mod = __import__("traceback")
                    write_runtime_error(
                        f"SWING_RETEST_SHADOW_OUTCOME_UPDATE/{key}",
                        traceback_mod.format_exc(),
                    )
            for key in finished:
                _swing_retest_shadow_outcome_pending.pop(key, None)
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("SWING_RETEST_SHADOW_OUTCOME_UPDATE", traceback_mod.format_exc())
    _maybe_sweep_shadow_outcome_expiries(
        "SWING_RETEST_SHADOW_OUTCOME",
        "swing_retest_shadow_outcomes.jsonl",
        ("dedup_key", "shadow_outcome_key"),
        _swing_retest_shadow_outcome_ttl_secs(),
        _swing_retest_shadow_outcome_terminal_seen,
        ("expiry_reason", "data_missing_reason"),
        pending=_swing_retest_shadow_outcome_pending,
    )


def _evaluate_early_cont_shadow_candidate(candidate, df5, df15):
    source_ts = float(candidate.get("source_timestamp") or 0)
    now_ts = time.time()
    candidate["bars_elapsed_m5"] = _count_bars_after(df5, source_ts)
    candidate["bars_elapsed_m15"] = _count_bars_after(df15, source_ts)

    df_eval = df5 if df5 is not None else df15
    if df_eval is None:
        candidate["missing_updates"] = candidate.get("missing_updates", 0) + 1
        if candidate["missing_updates"] >= EARLY_CONT_SHADOW_OUTCOME_MISSING_LIMIT:
            candidate["data_missing_reason"] = "candles_unavailable"
            return "DATA_MISSING", {"data_missing_reason": "candles_unavailable"}
        return None, {}
    candidate["missing_updates"] = 0

    side = candidate.get("side")
    entry = float(candidate.get("entry"))
    sl = float(candidate.get("sl"))
    tp = candidate.get("tp")
    tp = float(tp) if tp not in (None, "") else None
    risk = float(candidate.get("risk"))
    first_hit = candidate.get("first_hit") or "OPEN"

    for _, candle in df_eval.iterrows():
        candle_ts = _candle_open_ts(candle)
        if candle_ts is None or candle_ts <= source_ts:
            continue
        try:
            high = float(candle.get("high"))
            low = float(candle.get("low"))
        except Exception:
            continue

        if side == "LONG":
            favorable = max(0.0, high - entry)
            adverse = max(0.0, entry - low)
            sl_hit_now = low <= sl
            tp_hit_now = tp is not None and high >= tp
        elif side == "SHORT":
            favorable = max(0.0, entry - low)
            adverse = max(0.0, high - entry)
            sl_hit_now = high >= sl
            tp_hit_now = tp is not None and low <= tp
        else:
            candidate["data_missing_reason"] = "side_invalid"
            return "DATA_MISSING", {"data_missing_reason": "side_invalid"}

        mfe_r_now = favorable / risk
        mae_r_now = adverse / risk
        candidate["mfe_r"] = max(float(candidate.get("mfe_r") or 0), mfe_r_now)
        candidate["mae_r"] = max(float(candidate.get("mae_r") or 0), mae_r_now)

        hit_1r_now = mfe_r_now >= 1.0
        hit_1_5r_now = mfe_r_now >= 1.5
        hit_2r_now = mfe_r_now >= 2.0
        favorable_hit_now = tp_hit_now or hit_2r_now or hit_1_5r_now or hit_1r_now
        if sl_hit_now and favorable_hit_now:
            candidate["ambiguous_same_bar"] = True

        if hit_1r_now and not candidate.get("hit_1r"):
            candidate["hit_1r"] = True
            candidate["time_to_1r_secs"] = candle_ts - source_ts
        if hit_1_5r_now and not candidate.get("hit_1_5r"):
            candidate["hit_1_5r"] = True
            candidate["time_to_1_5r_secs"] = candle_ts - source_ts
        if hit_2r_now and not candidate.get("hit_2r"):
            candidate["hit_2r"] = True
            candidate["time_to_2r_secs"] = candle_ts - source_ts
        if sl_hit_now and not candidate.get("sl_hit"):
            candidate["sl_hit"] = True
            candidate["time_to_sl_secs"] = candle_ts - source_ts
        if tp_hit_now and not candidate.get("tp_hit"):
            candidate["tp_hit"] = True
            candidate["time_to_tp_secs"] = candle_ts - source_ts

        if first_hit == "OPEN" and (sl_hit_now or favorable_hit_now):
            if sl_hit_now and favorable_hit_now:
                first_hit = "UNKNOWN"
                candidate["ambiguous_same_bar"] = True
            elif sl_hit_now:
                first_hit = "SL"
            elif tp_hit_now:
                first_hit = "TP"
            elif hit_2r_now:
                first_hit = "2R"
            elif hit_1_5r_now:
                first_hit = "1.5R"
            else:
                first_hit = "1R"
            candidate["first_hit"] = first_hit
            return "RESOLVED", {}

    if now_ts - source_ts >= _early_cont_shadow_outcome_ttl_secs():
        candidate["expiry_reason"] = "ttl_elapsed"
        return "EXPIRED", {"expiry_reason": "ttl_elapsed"}
    return None, {}


def update_early_cont_shadow_outcomes(raw_data_map):
    try:
        if not isinstance(raw_data_map, dict):
            raw_data_map = {}
        if _early_cont_shadow_outcome_pending:
            finished = []
            for key, candidate in list(_early_cont_shadow_outcome_pending.items()):
                try:
                    if key in _early_cont_shadow_outcome_terminal_seen:
                        finished.append(key)
                        continue
                    symbol = candidate.get("symbol", "")
                    frames = raw_data_map.get(symbol)
                    df5 = frames[0] if isinstance(frames, tuple) and len(frames) > 0 else None
                    df15 = frames[1] if isinstance(frames, tuple) and len(frames) > 1 else None
                    status, extra = _evaluate_early_cont_shadow_candidate(candidate, df5, df15)
                    if status:
                        fields = _early_cont_shadow_outcome_fields(candidate)
                        if isinstance(extra, dict):
                            fields.update(extra)
                        wrote = _append_early_cont_shadow_outcome_event(
                            candidate,
                            status,
                            **fields,
                        )
                        if wrote:
                            _early_cont_shadow_outcome_terminal_seen.add(key)
                        else:
                            write_runtime_error(
                                f"EARLY_CONT_SHADOW_OUTCOME_UPDATE/{key}",
                                f"terminal_write_failed status={status} dedup_key={key}",
                            )
                        finished.append(key)
                except Exception:
                    traceback_mod = __import__("traceback")
                    write_runtime_error(
                        f"EARLY_CONT_SHADOW_OUTCOME_UPDATE/{key}",
                        traceback_mod.format_exc(),
                    )
            for key in finished:
                _early_cont_shadow_outcome_pending.pop(key, None)
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("EARLY_CONT_SHADOW_OUTCOME_UPDATE", traceback_mod.format_exc())
    _maybe_sweep_shadow_outcome_expiries(
        "EARLY_CONT_SHADOW_OUTCOME",
        "early_cont_shadow_outcomes.jsonl",
        ("dedup_key", "shadow_outcome_key"),
        _early_cont_shadow_outcome_ttl_secs(),
        _early_cont_shadow_outcome_terminal_seen,
        ("expiry_reason", "data_missing_reason"),
        pending=_early_cont_shadow_outcome_pending,
    )


def update_confirm_structural_outcomes(raw_data_map):
    try:
        if not isinstance(raw_data_map, dict):
            raw_data_map = {}
        if _confirm_structural_outcome_pending:
            finished = []
            for key, candidate in list(_confirm_structural_outcome_pending.items()):
                try:
                    if key in _confirm_structural_outcome_terminal_seen:
                        finished.append(key)
                        continue
                    symbol = candidate.get("symbol", "")
                    frames = raw_data_map.get(symbol)
                    df5 = frames[0] if isinstance(frames, tuple) and len(frames) > 0 else None
                    df15 = frames[1] if isinstance(frames, tuple) and len(frames) > 1 else None
                    status, extra = _evaluate_confirm_structural_candidate(candidate, df5, df15)
                    if status:
                        if key in _confirm_structural_outcome_terminal_seen:
                            finished.append(key)
                            continue
                        wrote = _append_confirm_structural_outcome_event(
                            candidate,
                            status,
                            **_confirm_structural_outcome_merged_fields(candidate, extra),
                        )
                        if wrote:
                            _confirm_structural_outcome_terminal_seen.add(key)
                        else:
                            write_runtime_error(
                                f"CONFIRM_STRUCTURAL_OUTCOME_UPDATE/{key}",
                                f"terminal_write_failed status={status} dedup_key={key}",
                            )
                        finished.append(key)
                except Exception:
                    traceback_mod = __import__("traceback")
                    write_runtime_error(
                        f"CONFIRM_STRUCTURAL_OUTCOME_UPDATE/{key}",
                        traceback_mod.format_exc(),
                    )
            for key in finished:
                _confirm_structural_outcome_pending.pop(key, None)
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("CONFIRM_STRUCTURAL_OUTCOME_UPDATE", traceback_mod.format_exc())
    _maybe_sweep_shadow_outcome_expiries(
        "CONFIRM_STRUCTURAL_OUTCOME",
        "confirm_structural_outcomes.jsonl",
        ("dedup_key",),
        CONFIRM_STRUCTURAL_OUTCOME_TTL_SECS,
        _confirm_structural_outcome_terminal_seen,
        ("data_missing_reason",),
        pending=_confirm_structural_outcome_pending,
    )


def _paper_smc_v0_2_shadow_dedup_key(payload):
    existing = _json_optional_scalar(payload.get("dedup_key"))
    source_ts = _json_optional_scalar(payload.get("source_timestamp") or payload.get("signal_created_ts"))
    parts = [
        "PAPER_SMC_MAIN_V0_2_SHADOW",
        _collector_key_part(payload.get("symbol")),
        _collector_key_part(payload.get("side")),
        _collector_key_part(source_ts),
        _collector_key_part(payload.get("entry")),
        _collector_key_part(payload.get("sl")),
        _collector_key_part(payload.get("regime_context_modifier_version") or "v0.2_grid_shadow"),
    ]
    if existing:
        parts.append(_collector_key_part(existing))
    return "|".join(parts)


def _paper_smc_v0_2_shadow_source_ts(payload):
    value = _json_optional_scalar(payload.get("source_timestamp") or payload.get("signal_created_ts"))
    if value is not None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return None


def _paper_smc_v0_2_shadow_slice(payload):
    suppress_reason = str(payload.get("suppress_reason") or "").strip().upper().replace("-", "_").replace(" ", "_")
    regime = str(payload.get("regime_context_regime") or "").strip().upper()
    if (
        payload.get("grid_rule_v0_2_match") is True
        and regime == "EXHAUSTION_REVERSAL"
        and suppress_reason == "SYMBOL_ALREADY_OPEN"
    ):
        return "EXH_REV_SYMBOL_ALREADY_OPEN"
    return None


def _paper_smc_v0_2_shadow_base_fields(candidate):
    return {
        "smc_v0_2_slice": _paper_smc_v0_2_shadow_slice(candidate),
        "MFE_R": candidate.get("MFE_R", 0.0),
        "MAE_R": candidate.get("MAE_R", 0.0),
        "first_hit": candidate.get("first_hit", "OPEN"),
        "hit_0_5r": candidate.get("hit_0_5r", False),
        "hit_1r": candidate.get("hit_1r", False),
        "hit_1_5r": candidate.get("hit_1_5r", False),
        "hit_2r": candidate.get("hit_2r", False),
        "sl_hit": candidate.get("sl_hit", False),
        "tp_hit": candidate.get("tp_hit", False),
        "ambiguous_same_bar": candidate.get("ambiguous_same_bar", False),
        "time_to_0_5r_secs": candidate.get("time_to_0_5r_secs"),
        "time_to_1r_secs": candidate.get("time_to_1r_secs"),
        "time_to_1_5r_secs": candidate.get("time_to_1_5r_secs"),
        "time_to_2r_secs": candidate.get("time_to_2r_secs"),
        "time_to_sl_secs": candidate.get("time_to_sl_secs"),
        "time_to_tp_secs": candidate.get("time_to_tp_secs"),
        "bars_elapsed": candidate.get("bars_elapsed", 0),
        "bars_elapsed_m5": candidate.get("bars_elapsed_m5", 0),
        "bars_elapsed_m15": candidate.get("bars_elapsed_m15", 0),
        "data_missing_reason": candidate.get("data_missing_reason"),
        "expired_reason": candidate.get("expired_reason"),
    }


def _append_paper_smc_v0_2_shadow_outcome_event(candidate, status, **fields):
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(base_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        row = {
            "event_type": "PAPER_SMC_V0_2_SHADOW_OUTCOME",
            "status": status,
            "tracker_version": PAPER_SMC_V0_2_SHADOW_TRACKER_VERSION,
            "dedup_key": _json_optional_scalar(candidate.get("dedup_key")),
            "observed_at": time.time(),
            "symbol": _json_optional_scalar(candidate.get("symbol")),
            "side": _json_optional_scalar(candidate.get("side")),
            "source_timestamp": _json_optional_scalar(candidate.get("source_timestamp")),
            "signal_created_ts": _json_optional_scalar(candidate.get("signal_created_ts")),
            "entry": _json_optional_scalar(candidate.get("entry")),
            "sl": _json_optional_scalar(candidate.get("sl")),
            "tp": _json_optional_scalar(candidate.get("tp")),
            "risk": _json_optional_scalar(candidate.get("risk")),
            "planned_rr": _json_optional_scalar(candidate.get("planned_rr")),
            "grid_rule_v0_2_match": bool(candidate.get("grid_rule_v0_2_match")),
            "would_regime_penalize": bool(candidate.get("would_regime_penalize")),
            "proposed_score_delta": _json_optional_scalar(candidate.get("proposed_score_delta")),
            "current_effective_score": _json_optional_scalar(candidate.get("current_effective_score")),
            "proposed_effective_score_after_regime": _json_optional_scalar(
                candidate.get("proposed_effective_score_after_regime")
            ),
            "suppress_reason": _json_optional_scalar(candidate.get("suppress_reason")),
            "action": _json_optional_scalar(candidate.get("action")),
            "opened": bool(candidate.get("opened")),
            "regime_context_regime": _json_optional_scalar(candidate.get("regime_context_regime")),
            "router_regime_source": _json_optional_scalar(candidate.get("router_regime_source")),
            "router_regime_age_sec": _json_optional_scalar(candidate.get("router_regime_age_sec")),
            "bos_quality": _json_optional_scalar(candidate.get("bos_quality")),
            "weak_structure_extended": candidate.get("weak_structure_extended"),
            "structural_decision_shadow": _json_optional_scalar(candidate.get("structural_decision_shadow")),
            "candidate_type": _json_optional_scalar(candidate.get("candidate_type")),
            "source_reason": _json_optional_scalar(candidate.get("source_reason")),
            "ttl_secs": PAPER_SMC_V0_2_SHADOW_OUTCOME_TTL_SECS,
            "time_filter": "candles_with_open_time_gt_source_timestamp",
        }
        row.update(_paper_smc_v0_2_shadow_base_fields(candidate))
        row.update({k: _json_safe_value(v) for k, v in fields.items()})
        path = os.path.join(log_dir, "paper_smc_v0_2_shadow_outcomes.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(_json_safe_value(row), ensure_ascii=False, default=str, sort_keys=True) + "\n")
        return True
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("PAPER_SMC_V0_2_SHADOW_OUTCOME", traceback_mod.format_exc())
        return False


def register_paper_smc_v0_2_shadow_outcome(payload):
    try:
        if not isinstance(payload, dict):
            return
        if payload.get("entry_type") != "PAPER_SMC_MAIN":
            return
        if payload.get("grid_rule_v0_2_match") is not True:
            return
        if payload.get("would_regime_penalize") is not True:
            return
        try:
            proposed_delta = float(payload.get("proposed_score_delta"))
        except (TypeError, ValueError):
            proposed_delta = None
        if proposed_delta != -1.0:
            return

        dedup_key = _paper_smc_v0_2_shadow_dedup_key(payload)
        if (
            dedup_key in _paper_smc_v0_2_shadow_outcome_seen
            or dedup_key in _paper_smc_v0_2_shadow_outcome_pending
            or dedup_key in _paper_smc_v0_2_shadow_outcome_terminal_seen
        ):
            return
        _paper_smc_v0_2_shadow_outcome_seen.add(dedup_key)

        source_ts = _paper_smc_v0_2_shadow_source_ts(payload)
        entry = _collector_optional_float(payload.get("entry"))
        sl = _collector_optional_float(payload.get("sl"))
        tp = _collector_optional_float(payload.get("tp"))
        side = str(payload.get("side") or "").upper()
        symbol = str(payload.get("symbol") or "")
        missing = []
        if not symbol:
            missing.append("symbol")
        if side not in ("LONG", "SHORT"):
            missing.append("side")
        if entry is None:
            missing.append("entry")
        if sl is None:
            missing.append("sl")
        if source_ts is None:
            missing.append("source_timestamp")

        candidate = {
            "dedup_key": dedup_key,
            "symbol": symbol,
            "side": side,
            "source_timestamp": source_ts,
            "signal_created_ts": payload.get("signal_created_ts"),
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "planned_rr": payload.get("planned_rr", payload.get("rr")),
            "grid_rule_v0_2_match": bool(payload.get("grid_rule_v0_2_match")),
            "would_regime_penalize": bool(payload.get("would_regime_penalize")),
            "proposed_score_delta": proposed_delta,
            "current_effective_score": payload.get("current_effective_score"),
            "proposed_effective_score_after_regime": payload.get("proposed_effective_score_after_regime"),
            "suppress_reason": payload.get("suppress_reason"),
            "action": payload.get("action"),
            "opened": bool(payload.get("opened")),
            "regime_context_regime": payload.get("regime_context_regime"),
            "router_regime_source": payload.get("router_regime_source"),
            "router_regime_age_sec": payload.get("router_regime_age_sec"),
            "bos_quality": payload.get("bos_quality"),
            "weak_structure_extended": payload.get("weak_structure_extended"),
            "structural_decision_shadow": payload.get("structural_decision_shadow"),
            "candidate_type": payload.get("candidate_type"),
            "source_reason": payload.get("source_reason"),
            "MFE_R": 0.0,
            "MAE_R": 0.0,
            "first_hit": "OPEN",
            "hit_0_5r": False,
            "hit_1r": False,
            "hit_1_5r": False,
            "hit_2r": False,
            "sl_hit": False,
            "tp_hit": False,
            "ambiguous_same_bar": False,
            "time_to_0_5r_secs": None,
            "time_to_1r_secs": None,
            "time_to_1_5r_secs": None,
            "time_to_2r_secs": None,
            "time_to_sl_secs": None,
            "time_to_tp_secs": None,
            "bars_elapsed": 0,
            "bars_elapsed_m5": 0,
            "bars_elapsed_m15": 0,
            "missing_updates": 0,
            "data_missing_reason": None,
            "expired_reason": None,
        }
        if missing:
            candidate["data_missing_reason"] = "missing_geometry:" + ",".join(missing)
            if _append_paper_smc_v0_2_shadow_outcome_event(candidate, "DATA_MISSING"):
                _paper_smc_v0_2_shadow_outcome_terminal_seen.add(dedup_key)
            return

        if side == "LONG":
            risk = entry - sl
        else:
            risk = sl - entry
        candidate["risk"] = risk
        if risk is None or risk <= 0:
            candidate["data_missing_reason"] = "invalid_risk_geometry"
            if _append_paper_smc_v0_2_shadow_outcome_event(candidate, "DATA_MISSING"):
                _paper_smc_v0_2_shadow_outcome_terminal_seen.add(dedup_key)
            return

        _paper_smc_v0_2_shadow_outcome_pending[dedup_key] = candidate
        _append_paper_smc_v0_2_shadow_outcome_event(candidate, "OPEN")
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("PAPER_SMC_V0_2_SHADOW_OUTCOME_REGISTER", traceback_mod.format_exc())


def _evaluate_paper_smc_v0_2_shadow_candidate(candidate, df5, df15):
    source_ts = float(candidate.get("source_timestamp") or 0)
    now_ts = time.time()
    candidate["bars_elapsed_m5"] = _count_bars_after(df5, source_ts)
    candidate["bars_elapsed_m15"] = _count_bars_after(df15, source_ts)
    candidate["bars_elapsed"] = candidate.get("bars_elapsed_m5", 0)

    df_eval = df5 if df5 is not None else df15
    if df_eval is None:
        candidate["missing_updates"] = candidate.get("missing_updates", 0) + 1
        if candidate["missing_updates"] >= PAPER_SMC_V0_2_SHADOW_OUTCOME_MISSING_LIMIT:
            candidate["data_missing_reason"] = "candles_unavailable"
            return "DATA_MISSING", {"data_missing_reason": "candles_unavailable"}
        return None, {}
    candidate["missing_updates"] = 0

    side = candidate.get("side")
    entry = float(candidate.get("entry"))
    sl = float(candidate.get("sl"))
    tp = candidate.get("tp")
    tp = float(tp) if tp not in (None, "") else None
    risk = float(candidate.get("risk"))
    first_hit = candidate.get("first_hit") or "OPEN"

    for _, candle in df_eval.iterrows():
        candle_ts = _candle_open_ts(candle)
        if candle_ts is None or candle_ts <= source_ts:
            continue
        try:
            high = float(candle.get("high"))
            low = float(candle.get("low"))
        except Exception:
            continue

        if side == "LONG":
            favorable = max(0.0, high - entry)
            adverse = max(0.0, entry - low)
            sl_hit_now = low <= sl
            tp_hit_now = tp is not None and high >= tp
        elif side == "SHORT":
            favorable = max(0.0, entry - low)
            adverse = max(0.0, high - entry)
            sl_hit_now = high >= sl
            tp_hit_now = tp is not None and low <= tp
        else:
            candidate["data_missing_reason"] = "missing_geometry:side"
            return "DATA_MISSING", {"data_missing_reason": "missing_geometry:side"}

        mfe_r_now = favorable / risk
        mae_r_now = adverse / risk
        candidate["MFE_R"] = max(float(candidate.get("MFE_R") or 0), mfe_r_now)
        candidate["MAE_R"] = max(float(candidate.get("MAE_R") or 0), mae_r_now)

        hit_0_5r_now = mfe_r_now >= 0.5
        hit_1r_now = mfe_r_now >= 1.0
        hit_1_5r_now = mfe_r_now >= 1.5
        hit_2r_now = mfe_r_now >= 2.0
        terminal_favorable_hit_now = tp_hit_now or hit_2r_now or hit_1_5r_now or hit_1r_now
        if sl_hit_now and terminal_favorable_hit_now:
            candidate["ambiguous_same_bar"] = True

        if hit_0_5r_now and not candidate.get("hit_0_5r"):
            candidate["hit_0_5r"] = True
            candidate["time_to_0_5r_secs"] = candle_ts - source_ts
        if hit_1r_now and not candidate.get("hit_1r"):
            candidate["hit_1r"] = True
            candidate["time_to_1r_secs"] = candle_ts - source_ts
        if hit_1_5r_now and not candidate.get("hit_1_5r"):
            candidate["hit_1_5r"] = True
            candidate["time_to_1_5r_secs"] = candle_ts - source_ts
        if hit_2r_now and not candidate.get("hit_2r"):
            candidate["hit_2r"] = True
            candidate["time_to_2r_secs"] = candle_ts - source_ts
        if sl_hit_now and not candidate.get("sl_hit"):
            candidate["sl_hit"] = True
            candidate["time_to_sl_secs"] = candle_ts - source_ts
        if tp_hit_now and not candidate.get("tp_hit"):
            candidate["tp_hit"] = True
            candidate["time_to_tp_secs"] = candle_ts - source_ts

        if first_hit == "OPEN" and (sl_hit_now or terminal_favorable_hit_now):
            if sl_hit_now and terminal_favorable_hit_now:
                first_hit = "AMBIGUOUS_SAME_BAR"
                candidate["ambiguous_same_bar"] = True
            elif sl_hit_now:
                first_hit = "SL"
            elif tp_hit_now:
                first_hit = "TP"
            elif hit_2r_now:
                first_hit = "2R"
            elif hit_1_5r_now:
                first_hit = "1.5R"
            elif hit_1r_now:
                first_hit = "1R"
            candidate["first_hit"] = first_hit
            return "RESOLVED", {}

    if now_ts - source_ts >= PAPER_SMC_V0_2_SHADOW_OUTCOME_TTL_SECS:
        candidate["expired_reason"] = "ttl_secs_elapsed"
        return "EXPIRED", {"expired_reason": "ttl_secs_elapsed"}
    return None, {}


def update_paper_smc_v0_2_shadow_outcomes(raw_data_map):
    try:
        if not isinstance(raw_data_map, dict):
            raw_data_map = {}
        if _paper_smc_v0_2_shadow_outcome_pending:
            finished = []
            for key, candidate in list(_paper_smc_v0_2_shadow_outcome_pending.items()):
                try:
                    if key in _paper_smc_v0_2_shadow_outcome_terminal_seen:
                        finished.append(key)
                        continue
                    symbol = candidate.get("symbol", "")
                    frames = raw_data_map.get(symbol)
                    df5 = frames[0] if isinstance(frames, tuple) and len(frames) > 0 else None
                    df15 = frames[1] if isinstance(frames, tuple) and len(frames) > 1 else None
                    status, extra = _evaluate_paper_smc_v0_2_shadow_candidate(candidate, df5, df15)
                    if status:
                        fields = _paper_smc_v0_2_shadow_base_fields(candidate)
                        if isinstance(extra, dict):
                            fields.update(extra)
                        wrote = _append_paper_smc_v0_2_shadow_outcome_event(candidate, status, **fields)
                        if wrote:
                            _paper_smc_v0_2_shadow_outcome_terminal_seen.add(key)
                        else:
                            write_runtime_error(
                                f"PAPER_SMC_V0_2_SHADOW_OUTCOME_UPDATE/{key}",
                                f"terminal_write_failed status={status} dedup_key={key}",
                            )
                        finished.append(key)
                except Exception:
                    traceback_mod = __import__("traceback")
                    write_runtime_error(
                        f"PAPER_SMC_V0_2_SHADOW_OUTCOME_UPDATE/{key}",
                        traceback_mod.format_exc(),
                    )
            for key in finished:
                _paper_smc_v0_2_shadow_outcome_pending.pop(key, None)
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("PAPER_SMC_V0_2_SHADOW_OUTCOME_UPDATE", traceback_mod.format_exc())
    _maybe_sweep_shadow_outcome_expiries(
        "PAPER_SMC_V0_2_SHADOW_OUTCOME",
        "paper_smc_v0_2_shadow_outcomes.jsonl",
        ("dedup_key",),
        PAPER_SMC_V0_2_SHADOW_OUTCOME_TTL_SECS,
        _paper_smc_v0_2_shadow_outcome_terminal_seen,
        ("expired_reason", "data_missing_reason"),
        pending=_paper_smc_v0_2_shadow_outcome_pending,
    )


def _paper_smc_main_open_geom_source_ts(payload):
    value = _json_optional_scalar(payload.get("source_timestamp") or payload.get("signal_created_ts"))
    if value is not None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return None


def _paper_smc_main_open_geom_dedup_key(payload):
    source_ts = _json_optional_scalar(payload.get("source_timestamp") or payload.get("signal_created_ts"))
    opened_trade_id = _json_optional_scalar(payload.get("opened_trade_id"))
    decision_key = _json_optional_scalar(payload.get("decision_dedup_key") or payload.get("dedup_key"))
    parts = [
        "PAPER_SMC_MAIN_OPEN_GEOM",
        _collector_key_part(payload.get("symbol")),
        _collector_key_part(payload.get("side")),
        _collector_key_part(source_ts),
        _collector_key_part(payload.get("entry")),
        _collector_key_part(payload.get("sl")),
        _collector_key_part(opened_trade_id or decision_key),
    ]
    return "|".join(parts)


def _paper_smc_main_open_geom_score_bucket(score):
    score = _collector_optional_float(score)
    if score is None:
        return None
    if score < 0:
        return "s < 0"
    if score < 1:
        return "0 <= s < 1"
    if score < 2:
        return "1 <= s < 2"
    if score < 3:
        return "2 <= s < 3"
    return "s >= 3"


def _paper_smc_main_open_geom_raw_r(first_hit, planned_rr):
    if first_hit == "SL":
        return -1.0
    if first_hit == "1R":
        return 1.0
    if first_hit == "1.5R":
        return 1.5
    if first_hit == "2R":
        return 2.0
    if first_hit == "TP":
        return _collector_optional_float(planned_rr)
    if first_hit == "AMBIGUOUS_SAME_BAR":
        return None
    return None


def _paper_smc_main_open_geom_sl_gap_info(symbol=None):
    try:
        from execution import SL_GAP_MAX_R, get_sl_gap_r_for_tier, _get_execution_tier

        tier = _get_execution_tier(symbol) if symbol else None
        tier_gap_r = get_sl_gap_r_for_tier(tier)
        tier_gap_r = _collector_optional_float(tier_gap_r)
        sl_gap_max_r = _collector_optional_float(SL_GAP_MAX_R)
        if tier_gap_r is not None and sl_gap_max_r is not None:
            tier_gap_r = min(tier_gap_r, sl_gap_max_r)
        return tier_gap_r, sl_gap_max_r, tier, "EXECUTION_TIER"
    except Exception:
        try:
            from execution import SL_GAP_R_BY_TIER, SL_GAP_MAX_R

            tier_gap_r = SL_GAP_R_BY_TIER.get("TIER3", SL_GAP_MAX_R)
            tier_gap_r = _collector_optional_float(tier_gap_r)
            sl_gap_max_r = _collector_optional_float(SL_GAP_MAX_R)
            if tier_gap_r is not None and sl_gap_max_r is not None:
                tier_gap_r = min(tier_gap_r, sl_gap_max_r)
            return tier_gap_r, sl_gap_max_r, "TIER3", "TIER3_PROXY"
        except Exception:
            return None, None, None, None
    except Exception:
        return None, None


def _paper_smc_main_open_geom_actual_fields(candidate, raw_no_gap_r):
    fields = {
        "observed_actual_close_r": None,
        "actual_exit_type": None,
        "actual_status": None,
        "actual_fill_gap_r": None,
        "actual_vs_raw_delta_r": None,
        "actual_source": "paper_trades.csv",
    }
    try:
        opened_trade_id = _json_optional_scalar(candidate.get("opened_trade_id"))
        if opened_trade_id is None:
            return fields
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "paper_trades.csv")
        if not os.path.exists(path):
            return fields
        matched = None
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if str(row.get("id") or "") == str(opened_trade_id):
                    matched = row
        if not matched:
            return fields
        actual_r = _collector_optional_float(matched.get("rr"))
        fields["observed_actual_close_r"] = actual_r
        fields["actual_exit_type"] = _json_optional_scalar(matched.get("exit_type"))
        fields["actual_status"] = _json_optional_scalar(matched.get("status"))
        if actual_r is not None and fields["actual_exit_type"] == "SL":
            fields["actual_fill_gap_r"] = actual_r - PAPER_SMC_MAIN_OPEN_GEOM_THEORETICAL_SL_R
        if actual_r is not None and raw_no_gap_r is not None:
            fields["actual_vs_raw_delta_r"] = actual_r - raw_no_gap_r
    except Exception:
        pass
    return fields


def _paper_smc_main_open_geom_base_fields(candidate):
    first_hit = candidate.get("first_hit_raw_no_gap", "OPEN")
    raw_no_gap_r = _paper_smc_main_open_geom_raw_r(first_hit, candidate.get("planned_rr"))
    tier_gap_r, sl_gap_max_r, tier_gap_tier, tier_gap_r_source = _paper_smc_main_open_geom_sl_gap_info(
        candidate.get("symbol")
    )
    if first_hit == "SL" and tier_gap_r is not None:
        sl_gap_adjusted_theoretical_r = -(1.0 + tier_gap_r)
        sl_gap_adjusted_theoretical_r_source = (
            "symbol_tier_estimate" if tier_gap_r_source == "EXECUTION_TIER" else "proxy_estimate"
        )
    else:
        sl_gap_adjusted_theoretical_r = raw_no_gap_r
        sl_gap_adjusted_theoretical_r_source = None if raw_no_gap_r is None else "raw_no_gap"
    fields = {
        "measurement_frame": "source_timestamp_raw_candles",
        "MFE_R": candidate.get("MFE_R", 0.0),
        "MAE_R": candidate.get("MAE_R", 0.0),
        "first_hit_raw_no_gap": first_hit,
        "hit_0_5r": candidate.get("hit_0_5r", False),
        "hit_1r": candidate.get("hit_1r", False),
        "hit_1_5r": candidate.get("hit_1_5r", False),
        "hit_2r": candidate.get("hit_2r", False),
        "tp_hit": candidate.get("tp_hit", False),
        "sl_hit": candidate.get("sl_hit", False),
        "ambiguous_same_bar": candidate.get("ambiguous_same_bar", False),
        "time_to_0_5r_secs": candidate.get("time_to_0_5r_secs"),
        "time_to_1r_secs": candidate.get("time_to_1r_secs"),
        "time_to_1_5r_secs": candidate.get("time_to_1_5r_secs"),
        "time_to_2r_secs": candidate.get("time_to_2r_secs"),
        "time_to_sl_secs": candidate.get("time_to_sl_secs"),
        "time_to_tp_secs": candidate.get("time_to_tp_secs"),
        "bars_elapsed": candidate.get("bars_elapsed", 0),
        "bars_elapsed_m5": candidate.get("bars_elapsed_m5", 0),
        "bars_elapsed_m15": candidate.get("bars_elapsed_m15", 0),
        "raw_no_gap_r": raw_no_gap_r,
        "theoretical_sl_r": PAPER_SMC_MAIN_OPEN_GEOM_THEORETICAL_SL_R,
        "tier_gap_r": tier_gap_r,
        "tier_gap_tier": tier_gap_tier,
        "tier_gap_r_source": tier_gap_r_source,
        "sl_gap_max_r": sl_gap_max_r,
        "sl_gap_adjusted_theoretical_r": sl_gap_adjusted_theoretical_r,
        "sl_gap_adjusted_theoretical_r_source": sl_gap_adjusted_theoretical_r_source,
        "data_missing_reason": candidate.get("data_missing_reason"),
        "expired_reason": candidate.get("expired_reason"),
    }
    fields.update(_paper_smc_main_open_geom_actual_fields(candidate, raw_no_gap_r))
    return fields


def _append_paper_smc_main_open_geom_event(candidate, status, **fields):
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(base_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        row = {
            "event_type": "PAPER_SMC_MAIN_OPEN_GEOMETRY_OBSERVER",
            "status": status,
            "tracker_version": PAPER_SMC_MAIN_OPEN_GEOM_VERSION,
            "dedup_key": _json_optional_scalar(candidate.get("dedup_key")),
            "observer_key": _json_optional_scalar(candidate.get("dedup_key")),
            "observed_at": time.time(),
            "symbol": _json_optional_scalar(candidate.get("symbol")),
            "side": _json_optional_scalar(candidate.get("side")),
            "entry": _json_optional_scalar(candidate.get("entry")),
            "sl": _json_optional_scalar(candidate.get("sl")),
            "tp": _json_optional_scalar(candidate.get("tp")),
            "risk": _json_optional_scalar(candidate.get("risk")),
            "rr": _json_optional_scalar(candidate.get("planned_rr")),
            "planned_rr": _json_optional_scalar(candidate.get("planned_rr")),
            "source_timestamp": _json_optional_scalar(candidate.get("source_timestamp")),
            "signal_created_ts": _json_optional_scalar(candidate.get("signal_created_ts")),
            "opened_trade_id": _json_optional_scalar(candidate.get("opened_trade_id")),
            "decision_dedup_key": _json_optional_scalar(candidate.get("decision_dedup_key")),
            "effective_score": _json_optional_scalar(candidate.get("effective_score")),
            "current_effective_score": _json_optional_scalar(candidate.get("current_effective_score")),
            "current_score_bucket": _json_optional_scalar(candidate.get("current_score_bucket")),
            "candidate_type": _json_optional_scalar(candidate.get("candidate_type")),
            "source_reason": _json_optional_scalar(candidate.get("source_reason")),
            "bos_quality": _json_optional_scalar(candidate.get("bos_quality")),
            "bos_confirmation": _json_optional_scalar(candidate.get("bos_confirmation")),
            "structural_decision_shadow": _json_optional_scalar(candidate.get("structural_decision_shadow")),
            "weak_structure_extended": candidate.get("weak_structure_extended"),
            "regime_context_regime": _json_optional_scalar(candidate.get("regime_context_regime")),
            "market_regime_at_entry": _json_optional_scalar(candidate.get("market_regime_at_entry")),
            "router_regime": _json_optional_scalar(candidate.get("router_regime")),
            "range_context": _json_optional_scalar(candidate.get("range_context")),
            "suppress_reason": _json_optional_scalar(candidate.get("suppress_reason")),
            "action": _json_optional_scalar(candidate.get("action")),
            "opened": bool(candidate.get("opened")),
            "tp_mode": _json_optional_scalar(candidate.get("tp_mode")),
            "structural_modifier_delta": _json_optional_scalar(candidate.get("structural_modifier_delta")),
            "proposed_score_delta": _json_optional_scalar(candidate.get("proposed_score_delta")),
            "regime_proposed_score_delta": _json_optional_scalar(candidate.get("proposed_score_delta")),
            "entry_type": _json_optional_scalar(candidate.get("entry_type")),
            "strategy_family": _json_optional_scalar(candidate.get("strategy_family")),
            "ttl_secs": PAPER_SMC_MAIN_OPEN_GEOM_TTL_SECS,
            "time_filter": "candles_with_open_time_gt_source_timestamp",
        }
        row.update(_paper_smc_main_open_geom_base_fields(candidate))
        row.update({k: _json_safe_value(v) for k, v in fields.items()})
        path = os.path.join(log_dir, "paper_smc_main_open_geometry_observer.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(_json_safe_value(row), ensure_ascii=False, default=str, sort_keys=True) + "\n")
        return True
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("PAPER_SMC_MAIN_OPEN_GEOM_OBSERVER", traceback_mod.format_exc())
        return False


def register_paper_smc_main_open_geometry_observer(payload):
    try:
        if not isinstance(payload, dict):
            return
        if payload.get("entry_type") != "PAPER_SMC_MAIN":
            return
        if payload.get("opened") is not True or payload.get("opened_trade_id") in (None, ""):
            return
        dedup_key = _paper_smc_main_open_geom_dedup_key(payload)
        if (
            dedup_key in _paper_smc_main_open_geom_seen
            or dedup_key in _paper_smc_main_open_geom_pending
            or dedup_key in _paper_smc_main_open_geom_terminal_seen
        ):
            return
        _paper_smc_main_open_geom_seen.add(dedup_key)

        source_ts = _paper_smc_main_open_geom_source_ts(payload)
        entry = _collector_optional_float(payload.get("entry"))
        sl = _collector_optional_float(payload.get("sl"))
        tp = _collector_optional_float(payload.get("tp"))
        side = str(payload.get("side") or "").upper()
        symbol = str(payload.get("symbol") or "")
        current_score = _collector_optional_float(
            payload.get("current_effective_score") or payload.get("effective_score")
        )
        candidate = {
            "dedup_key": dedup_key,
            "symbol": symbol,
            "side": side,
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "planned_rr": payload.get("planned_rr", payload.get("rr")),
            "source_timestamp": source_ts,
            "signal_created_ts": payload.get("signal_created_ts"),
            "opened_trade_id": payload.get("opened_trade_id"),
            "decision_dedup_key": payload.get("decision_dedup_key") or payload.get("dedup_key"),
            "effective_score": payload.get("effective_score"),
            "current_effective_score": payload.get("current_effective_score"),
            "current_score_bucket": _paper_smc_main_open_geom_score_bucket(current_score),
            "candidate_type": payload.get("candidate_type"),
            "source_reason": payload.get("source_reason"),
            "bos_quality": payload.get("bos_quality"),
            "bos_confirmation": payload.get("bos_confirmation"),
            "structural_decision_shadow": payload.get("structural_decision_shadow"),
            "weak_structure_extended": payload.get("weak_structure_extended"),
            "regime_context_regime": payload.get("regime_context_regime"),
            "market_regime_at_entry": payload.get("market_regime_at_entry"),
            "router_regime": payload.get("router_regime"),
            "range_context": payload.get("range_context"),
            "suppress_reason": payload.get("suppress_reason"),
            "action": payload.get("action"),
            "opened": bool(payload.get("opened")),
            "tp_mode": payload.get("tp_mode"),
            "structural_modifier_delta": payload.get("structural_modifier", payload.get("structural_modifier_delta")),
            "proposed_score_delta": payload.get("proposed_score_delta"),
            "entry_type": payload.get("entry_type"),
            "strategy_family": payload.get("strategy_family"),
            "MFE_R": 0.0,
            "MAE_R": 0.0,
            "first_hit_raw_no_gap": "OPEN",
            "hit_0_5r": False,
            "hit_1r": False,
            "hit_1_5r": False,
            "hit_2r": False,
            "tp_hit": False,
            "sl_hit": False,
            "ambiguous_same_bar": False,
            "time_to_0_5r_secs": None,
            "time_to_1r_secs": None,
            "time_to_1_5r_secs": None,
            "time_to_2r_secs": None,
            "time_to_sl_secs": None,
            "time_to_tp_secs": None,
            "bars_elapsed": 0,
            "bars_elapsed_m5": 0,
            "bars_elapsed_m15": 0,
            "missing_updates": 0,
            "data_missing_reason": None,
            "expired_reason": None,
        }
        missing = []
        if not symbol:
            missing.append("symbol")
        if side not in ("LONG", "SHORT"):
            missing.append("side")
        if entry is None:
            missing.append("entry")
        if sl is None:
            missing.append("sl")
        if source_ts is None:
            missing.append("source_timestamp")
        if missing:
            candidate["data_missing_reason"] = "missing_geometry:" + ",".join(missing)
            if _append_paper_smc_main_open_geom_event(candidate, "DATA_MISSING"):
                _paper_smc_main_open_geom_terminal_seen.add(dedup_key)
            return

        risk = entry - sl if side == "LONG" else sl - entry
        candidate["risk"] = risk
        if risk <= 0:
            candidate["data_missing_reason"] = "invalid_risk_geometry"
            if _append_paper_smc_main_open_geom_event(candidate, "DATA_MISSING"):
                _paper_smc_main_open_geom_terminal_seen.add(dedup_key)
            return

        _paper_smc_main_open_geom_pending[dedup_key] = candidate
        _append_paper_smc_main_open_geom_event(candidate, "OPEN")
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("PAPER_SMC_MAIN_OPEN_GEOM_OBSERVER_REGISTER", traceback_mod.format_exc())


def _evaluate_paper_smc_main_open_geom_candidate(candidate, df5, df15):
    source_ts = float(candidate.get("source_timestamp") or 0)
    now_ts = time.time()
    candidate["bars_elapsed_m5"] = _count_bars_after(df5, source_ts)
    candidate["bars_elapsed_m15"] = _count_bars_after(df15, source_ts)
    candidate["bars_elapsed"] = candidate.get("bars_elapsed_m5", 0)

    df_eval = df5 if df5 is not None else df15
    if df_eval is None:
        candidate["missing_updates"] = candidate.get("missing_updates", 0) + 1
        if candidate["missing_updates"] >= PAPER_SMC_MAIN_OPEN_GEOM_MISSING_LIMIT:
            candidate["data_missing_reason"] = "candles_unavailable"
            candidate["first_hit_raw_no_gap"] = "DATA_MISSING"
            return "DATA_MISSING", {"data_missing_reason": "candles_unavailable"}
        return None, {}
    candidate["missing_updates"] = 0

    side = candidate.get("side")
    entry = float(candidate.get("entry"))
    sl = float(candidate.get("sl"))
    tp = candidate.get("tp")
    tp = float(tp) if tp not in (None, "") else None
    risk = float(candidate.get("risk"))

    for _, candle in df_eval.iterrows():
        candle_ts = _candle_open_ts(candle)
        if candle_ts is None or candle_ts <= source_ts:
            continue
        try:
            high = float(candle.get("high"))
            low = float(candle.get("low"))
        except Exception:
            continue
        if side == "LONG":
            favorable = max(0.0, high - entry)
            adverse = max(0.0, entry - low)
            sl_hit_now = low <= sl
            tp_hit_now = tp is not None and high >= tp
        elif side == "SHORT":
            favorable = max(0.0, entry - low)
            adverse = max(0.0, high - entry)
            sl_hit_now = high >= sl
            tp_hit_now = tp is not None and low <= tp
        else:
            candidate["data_missing_reason"] = "missing_geometry:side"
            candidate["first_hit_raw_no_gap"] = "DATA_MISSING"
            return "DATA_MISSING", {"data_missing_reason": "missing_geometry:side"}

        mfe_r_now = favorable / risk
        mae_r_now = adverse / risk
        candidate["MFE_R"] = max(float(candidate.get("MFE_R") or 0), mfe_r_now)
        candidate["MAE_R"] = max(float(candidate.get("MAE_R") or 0), mae_r_now)

        hit_0_5r_now = mfe_r_now >= 0.5
        hit_1r_now = mfe_r_now >= 1.0
        hit_1_5r_now = mfe_r_now >= 1.5
        hit_2r_now = mfe_r_now >= 2.0
        terminal_favorable_hit_now = tp_hit_now or hit_2r_now or hit_1_5r_now or hit_1r_now
        if sl_hit_now and terminal_favorable_hit_now:
            candidate["ambiguous_same_bar"] = True

        if hit_0_5r_now and not candidate.get("hit_0_5r"):
            candidate["hit_0_5r"] = True
            candidate["time_to_0_5r_secs"] = candle_ts - source_ts
        if hit_1r_now and not candidate.get("hit_1r"):
            candidate["hit_1r"] = True
            candidate["time_to_1r_secs"] = candle_ts - source_ts
        if hit_1_5r_now and not candidate.get("hit_1_5r"):
            candidate["hit_1_5r"] = True
            candidate["time_to_1_5r_secs"] = candle_ts - source_ts
        if hit_2r_now and not candidate.get("hit_2r"):
            candidate["hit_2r"] = True
            candidate["time_to_2r_secs"] = candle_ts - source_ts
        if sl_hit_now and not candidate.get("sl_hit"):
            candidate["sl_hit"] = True
            candidate["time_to_sl_secs"] = candle_ts - source_ts
        if tp_hit_now and not candidate.get("tp_hit"):
            candidate["tp_hit"] = True
            candidate["time_to_tp_secs"] = candle_ts - source_ts

        if sl_hit_now or terminal_favorable_hit_now:
            if sl_hit_now and terminal_favorable_hit_now:
                candidate["first_hit_raw_no_gap"] = "AMBIGUOUS_SAME_BAR"
            elif sl_hit_now:
                candidate["first_hit_raw_no_gap"] = "SL"
            elif tp_hit_now:
                candidate["first_hit_raw_no_gap"] = "TP"
            elif hit_2r_now:
                candidate["first_hit_raw_no_gap"] = "2R"
            elif hit_1_5r_now:
                candidate["first_hit_raw_no_gap"] = "1.5R"
            elif hit_1r_now:
                candidate["first_hit_raw_no_gap"] = "1R"
            return "RESOLVED", {}

    if now_ts - source_ts >= PAPER_SMC_MAIN_OPEN_GEOM_TTL_SECS:
        candidate["expired_reason"] = "ttl_secs_elapsed"
        candidate["first_hit_raw_no_gap"] = "EXPIRED"
        return "EXPIRED", {"expired_reason": "ttl_secs_elapsed"}
    return None, {}


def update_paper_smc_main_open_geometry_observer(raw_data_map):
    try:
        if not _paper_smc_main_open_geom_pending:
            return
        if not isinstance(raw_data_map, dict):
            raw_data_map = {}
        finished = []
        for key, candidate in list(_paper_smc_main_open_geom_pending.items()):
            try:
                if key in _paper_smc_main_open_geom_terminal_seen:
                    finished.append(key)
                    continue
                symbol = candidate.get("symbol", "")
                frames = raw_data_map.get(symbol)
                df5 = frames[0] if isinstance(frames, tuple) and len(frames) > 0 else None
                df15 = frames[1] if isinstance(frames, tuple) and len(frames) > 1 else None
                status, extra = _evaluate_paper_smc_main_open_geom_candidate(candidate, df5, df15)
                if status:
                    fields = _paper_smc_main_open_geom_base_fields(candidate)
                    if isinstance(extra, dict):
                        fields.update(extra)
                    wrote = _append_paper_smc_main_open_geom_event(candidate, status, **fields)
                    if wrote:
                        _paper_smc_main_open_geom_terminal_seen.add(key)
                    else:
                        write_runtime_error(
                            f"PAPER_SMC_MAIN_OPEN_GEOM_OBSERVER_UPDATE/{key}",
                            f"terminal_write_failed status={status} dedup_key={key}",
                        )
                    finished.append(key)
            except Exception:
                traceback_mod = __import__("traceback")
                write_runtime_error(
                    f"PAPER_SMC_MAIN_OPEN_GEOM_OBSERVER_UPDATE/{key}",
                    traceback_mod.format_exc(),
                )
        for key in finished:
            _paper_smc_main_open_geom_pending.pop(key, None)
    except Exception:
        traceback_mod = __import__("traceback")
        write_runtime_error("PAPER_SMC_MAIN_OPEN_GEOM_OBSERVER_UPDATE", traceback_mod.format_exc())


def _set_shadow_score_context(symbol, entry_type, score_old=None, score_v2=None, breakdown=None):
    _shadow_score_ctx[symbol] = {
        "entry_type": entry_type,
        "score_old": score_old,
        "score_v2": score_v2,
        "breakdown": breakdown,
    }


def _compute_shadow_score(symbol, entry_type, df_candle, df_ema, bos_type, h1_trend, signal_side, rr, market_state, score_old=None):
    score_v2 = None
    breakdown = None

    if rr is not None and rr >= 1.0 and signal_side:
        candle_class = get_candle_class(df_candle, signal_side)
        ema_align, _ = calc_ema_state(df_ema, signal_side)
        score_result = score_signal(
            bos_type,
            ema_align,
            h1_trend,
            signal_side,
            candle_class,
            rr,
            market_state,
        )
        if score_result is not None:
            score_v2, breakdown = score_result

    _set_shadow_score_context(symbol, entry_type, score_old=score_old, score_v2=score_v2, breakdown=breakdown)
    if DEBUG:
        print(f"[SCORE V2] {symbol} score={score_v2} breakdown={breakdown}")
    return score_v2, breakdown


def _confirm_reject_structural_breakdown(
    symbol,
    reject_reason,
    entry_type=None,
    score=None,
    score_v2=None,
    reason_tags=None,
    ctx=None,
    smc_ctx=None,
    breakdown=None,
    **fields,
):
    entry_type = entry_type or "CONFIRM"
    reject_reason = str(reject_reason or "")
    if entry_type != "CONFIRM" or reject_reason not in CONFIRM_FULL_FUNNEL_REJECT_REASONS:
        return breakdown

    reason_copy = list(reason_tags or [])
    ctx_copy = dict(ctx or {})
    smc_copy = dict(smc_ctx or {})
    logged = dict(breakdown) if isinstance(breakdown, dict) else {}

    signal = {
        "symbol": symbol,
        "entry_type": entry_type,
        "reject_reason": reject_reason,
        "original_reject_reason": reject_reason,
        "reason": reason_copy,
        "_ctx": ctx_copy,
        "score": score,
        "score_v2": score_v2,
    }
    for key, value in fields.items():
        signal[key] = value
    score_breakdown = signal.get("score_breakdown")
    if isinstance(score_breakdown, dict):
        signal["score_breakdown"] = dict(score_breakdown)

    structural_context = dict(_safe_structural_context(
        signal=signal,
        ctx=ctx_copy,
        smc_ctx=smc_copy,
        reason=reason_copy,
    ))
    structural_context["full_funnel_source"] = "confirm_reject"
    structural_context["original_reject_reason"] = reject_reason

    logged.update({
        "symbol": symbol,
        "status": "REJECT",
        "reason": reason_copy,
        "original_reject_reason": reject_reason,
        "entry_type": entry_type,
        "score": score,
        "side": fields.get("side"),
        "entry": fields.get("entry"),
        "sl": fields.get("sl"),
        "tp": fields.get("tp"),
        "rr": fields.get("rr"),
        "phase": fields.get("phase") or ctx_copy.get("phase"),
        "bos_type": fields.get("bos_type"),
        "signal_created_ts": fields.get("signal_created_ts"),
        "risk_distance_pct": fields.get("risk_distance_pct"),
        "score_breakdown": signal.get("score_breakdown", {}),
        "accepted_signal_context": {},
        "smc": smc_copy,
        "structural_context": structural_context,
    })
    for key in (
        "smc_zone",
        "liquidity_sweep",
        "bos_confirmation",
        "smc_bias",
        "range_context",
        "invalid_context",
    ):
        if key in smc_copy:
            logged[key] = smc_copy.get(key)
    return logged


def log_confirm_reject(symbol, reason, score=None, entry_type=None, structural_payload=None):
    ctx = _shadow_score_ctx.get(symbol, {})
    entry_type = entry_type or ctx.get("entry_type")
    breakdown = ctx.get("breakdown")
    if isinstance(structural_payload, dict):
        breakdown = _confirm_reject_structural_breakdown(
            symbol,
            reason,
            entry_type=entry_type,
            score=score if score is not None else ctx.get("score_old"),
            score_v2=ctx.get("score_v2"),
            breakdown=breakdown,
            **structural_payload,
        )
    _base_log_confirm_reject(
        symbol, reason,
        score_old=score if score is not None else ctx.get("score_old"),
        score_v2=ctx.get("score_v2"),
        breakdown=breakdown,
        entry_type=entry_type,
    )
    _log_score_shadow(
        symbol,
        "REJECT",
        reason,
        entry_type,
        score_old=score if score is not None else ctx.get("score_old"),
        score_v2=ctx.get("score_v2"),
        breakdown=breakdown,
    )

def _reason_tags(reason):
    tags = set()
    for item in reason or []:
        text = str(item)
        tags.add(text)
        if text.startswith("Soft:"):
            tags.add(text.split(":", 1)[1].upper())
    return tags


def _tag_value(reason, prefix):
    for item in reason or []:
        text = str(item)
        if text.startswith(prefix):
            return text.split(":", 1)[1]
    return ""


def _paper_reversal_shadow_enabled():
    if not config.get("paper_reversal_shadow_enabled", True):
        return False
    mode = str(config.get("execution_mode", "paper")).lower()
    return mode in ("paper", "both", "paper_live")


def _reversal_shadow_max_per_scan():
    try:
        return max(0, int(config.get("paper_reversal_shadow_max_per_scan", 20)))
    except Exception:
        return 20


def _paper_early_shadow_enabled():
    if not config.get("paper_early_shadow_enabled", True):
        return False
    mode = str(config.get("execution_mode", "paper")).lower()
    return mode in ("paper", "both", "paper_live")


def _early_shadow_max_per_scan():
    try:
        return max(0, int(config.get("paper_early_shadow_max_per_scan", 20)))
    except Exception:
        return 20


def _paper_swing_shadow_enabled():
    if not config.get("paper_swing_shadow_enabled", True):
        return False
    mode = str(config.get("execution_mode", "paper")).lower()
    return mode in ("paper", "both", "paper_live")


def _swing_shadow_max_per_scan():
    try:
        return max(0, int(config.get("paper_swing_shadow_max_per_scan", 20)))
    except Exception:
        return 20


def _swing_retest_shadow_outcome_enabled():
    if not config.get("swing_retest_shadow_outcome_enabled", True):
        return False
    mode = str(config.get("execution_mode", "paper")).lower()
    return mode in ("paper", "both", "paper_live")


def _swing_retest_shadow_outcome_ttl_secs():
    try:
        return max(1, int(config.get("swing_retest_shadow_outcome_ttl_secs", 86400)))
    except Exception:
        return 86400


def _swing_retest_shadow_outcome_max_pending():
    try:
        return max(0, int(config.get("swing_retest_shadow_outcome_max_pending", 5000)))
    except Exception:
        return 5000


def _swing_retest_shadow_outcome_log_open():
    return bool(config.get("swing_retest_shadow_outcome_log_open", True))


def _early_cont_shadow_outcome_enabled():
    if not config.get("early_cont_shadow_outcome_enabled", True):
        return False
    mode = str(config.get("execution_mode", "paper")).lower()
    return mode in ("paper", "both", "paper_live")


def _early_cont_shadow_outcome_ttl_secs():
    try:
        return max(1, int(config.get("early_cont_shadow_outcome_ttl_secs", 86400)))
    except Exception:
        return 86400


def _early_cont_shadow_outcome_max_pending():
    try:
        return max(0, int(config.get("early_cont_shadow_outcome_max_pending", 5000)))
    except Exception:
        return 5000


def _early_cont_shadow_outcome_log_open():
    return bool(config.get("early_cont_shadow_outcome_log_open", True))


def _json_safe_shadow_value(value):
    if isinstance(value, dict):
        return {str(k): _json_safe_shadow_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_shadow_value(v) for v in value]
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    try:
        if hasattr(value, "item"):
            return _json_safe_shadow_value(value.item())
    except Exception:
        pass
    return str(value)


def _early_geometry_status(entry=None, sl=None, tp=None, rr=None):
    if entry is None:
        return "unavailable"
    if sl is not None and tp is not None and rr is not None:
        return "computed"
    return "partial"


def _swing_geometry_status(entry=None, sl=None, tp=None, rr=None):
    if entry is None:
        return "unavailable"
    if sl is not None and tp is not None and rr is not None:
        return "computed"
    return "partial"


def _swing_shadow_candidate_class(reject_reason=None, phase=None, entry=None, sl=None, tp=None, rr=None, score=None, in_retest_zone=None, near_ema=None):
    if score is not None:
        return "SWING_RETEST_SCORE_READY"
    if entry is not None and sl is not None and tp is not None and rr is not None:
        return "SWING_RETEST_GEOMETRY_READY"
    if reject_reason in ("RETEST_ZONE_FAIL", "RETEST_TRIGGER_FAIL") or in_retest_zone or near_ema:
        return "SWING_RETEST_NEAR"
    if phase == "bos_confirmed" or reject_reason in ("STALE_AFTER_BOS",):
        return "SWING_BOS_CONFIRMED"
    if phase == "compress" or reject_reason == "BOS_WAITING":
        return "SWING_COMPRESSION_READY"
    return "SWING_UNKNOWN"


def _round_shadow_level(value):
    try:
        if value is None or value == "":
            return ""
        return round(float(value), 6)
    except Exception:
        return ""


def _snapshot_swing_watchlist(w):
    if not isinstance(w, dict):
        return {}
    keys = (
        "phase", "market_state", "mkt_state", "regime", "bias", "bias_type",
        "breakout_dir", "compression_type", "range_high", "range_low",
        "live_range_high", "live_range_low", "setup_range_high",
        "setup_range_low", "bos_level", "bos_price", "priority",
        "priority_final", "score", "compression_score_v2", "timestamp",
    )
    return {key: _json_safe_shadow_value(w.get(key)) for key in keys if key in w}


def _log_swing_shadow_candidate(
    symbol,
    reject_reason,
    side=None,
    ctx=None,
    watchlist=None,
    reason=None,
    phase=None,
    market_state=None,
    mkt_state=None,
    regime=None,
    bias=None,
    bias_type=None,
    breakout_dir=None,
    compression_phase=None,
    range_high=None,
    range_low=None,
    live_range_high=None,
    live_range_low=None,
    setup_range_high=None,
    setup_range_low=None,
    bos_level=None,
    bos_price=None,
    close=None,
    ema34=None,
    retest_tol=None,
    in_retest_zone=None,
    near_ema=None,
    valid_long_trigger=None,
    valid_short_trigger=None,
    body_ratio=None,
    upper_wick=None,
    lower_wick=None,
    priority=None,
    priority_final=None,
    compression_score=None,
    entry=None,
    signal_entry=None,
    sl=None,
    tp=None,
    rr=None,
    score=None,
    score_breakdown=None,
    signal_created_ts=None,
    smc_ctx=None,
):
    global _swing_shadow_logged_this_scan

    try:
        if not _paper_swing_shadow_enabled():
            return False
        max_per_scan = _swing_shadow_max_per_scan()
        if _swing_shadow_logged_this_scan >= max_per_scan:
            return False

        snap = _snapshot_swing_watchlist(watchlist)
        phase = phase or snap.get("phase") or ""
        breakout_dir = breakout_dir or snap.get("breakout_dir") or ""
        bos_level = bos_level if bos_level is not None else snap.get("bos_level")
        geometry_status = _swing_geometry_status(entry=entry, sl=sl, tp=tp, rr=rr)
        retest_zone_flag = bool(in_retest_zone) or bool(near_ema)
        trigger_flag = bool(valid_long_trigger) or bool(valid_short_trigger)
        key = (
            symbol,
            reject_reason,
            phase or "",
            breakout_dir or "",
            _round_shadow_level(bos_level),
            side or "",
            retest_zone_flag,
            trigger_flag,
            geometry_status,
        )
        now = time.time()
        last = _swing_shadow_seen.get(key, 0)
        if now - last < SWING_SHADOW_TTL_SECS:
            return False
        _swing_shadow_seen[key] = now
        _swing_shadow_logged_this_scan += 1

        ctx = dict(ctx or {})
        reason_copy = list(reason or [])
        smc_ctx = dict(smc_ctx or {})
        compression_phase = compression_phase or snap.get("compression_type") or ""
        market_state = market_state or ctx.get("market_state") or ""
        mkt_state = mkt_state or ctx.get("mkt_state") or ""
        regime = regime or ctx.get("regime") or ""
        shadow_candidate_class = _swing_shadow_candidate_class(
            reject_reason=reject_reason,
            phase=phase,
            entry=entry,
            sl=sl,
            tp=tp,
            rr=rr,
            score=score,
            in_retest_zone=in_retest_zone,
            near_ema=near_ema,
        )

        payload = {
            "status": "SWING_SHADOW",
            "entry_type": SWING_SHADOW_ENTRY_TYPE,
            "timestamp": now,
            "symbol": symbol,
            "side": side or "",
            "reject_reason": reject_reason,
            "shadow_candidate_class": shadow_candidate_class,
            "phase": phase or "",
            "market_state": market_state or "",
            "mkt_state": mkt_state or "",
            "regime": regime or "",
            "bias": bias or snap.get("bias") or "",
            "bias_type": bias_type or snap.get("bias_type") or "",
            "breakout_dir": breakout_dir or "",
            "compression_phase": compression_phase or "",
            "range_high": range_high if range_high is not None else snap.get("range_high", ""),
            "range_low": range_low if range_low is not None else snap.get("range_low", ""),
            "live_range_high": live_range_high if live_range_high is not None else snap.get("live_range_high", ""),
            "live_range_low": live_range_low if live_range_low is not None else snap.get("live_range_low", ""),
            "setup_range_high": setup_range_high if setup_range_high is not None else snap.get("setup_range_high", ""),
            "setup_range_low": setup_range_low if setup_range_low is not None else snap.get("setup_range_low", ""),
            "bos_level": bos_level if bos_level is not None else "",
            "bos_price": bos_price if bos_price is not None else snap.get("bos_price", ""),
            "close": close if close is not None else "",
            "ema34": ema34 if ema34 is not None else "",
            "retest_tol": retest_tol if retest_tol is not None else "",
            "in_retest_zone": in_retest_zone if in_retest_zone is not None else "",
            "near_ema": near_ema if near_ema is not None else "",
            "valid_long_trigger": valid_long_trigger if valid_long_trigger is not None else "",
            "valid_short_trigger": valid_short_trigger if valid_short_trigger is not None else "",
            "body_ratio": body_ratio if body_ratio is not None else "",
            "upper_wick": upper_wick if upper_wick is not None else "",
            "lower_wick": lower_wick if lower_wick is not None else "",
            "priority": priority if priority is not None else snap.get("priority", ""),
            "priority_final": priority_final if priority_final is not None else snap.get("priority_final", ""),
            "compression_score": compression_score if compression_score is not None else snap.get("score", ""),
            "entry": entry if entry is not None else "",
            "signal_entry": signal_entry if signal_entry is not None else "",
            "sl": sl if sl is not None else "",
            "tp": tp if tp is not None else "",
            "rr": rr if rr is not None else "",
            "geometry_status": geometry_status,
            "score": score if score is not None else "",
            "score_breakdown": _json_safe_shadow_value(score_breakdown or {}),
            "reason": reason_copy,
            "signal_created_ts": signal_created_ts if signal_created_ts is not None else now,
            "smc_zone": smc_ctx.get("smc_zone", ""),
            "liquidity_sweep": smc_ctx.get("liquidity_sweep", ""),
            "bos_confirmation": smc_ctx.get("bos_confirmation", ""),
            "smc_bias": smc_ctx.get("smc_bias", ""),
            "range_context": smc_ctx.get("range_context", ""),
            "invalid_context": _json_safe_shadow_value(smc_ctx.get("invalid_context", [])),
        }
        payload["structural_context"] = _safe_structural_context(
            signal=payload,
            ctx=ctx,
            smc_ctx=smc_ctx,
            reason=reason_copy,
        )
        payload = _json_safe_shadow_value(payload)
        _log_score_shadow(
            symbol,
            "SWING_SHADOW",
            reject_reason,
            SWING_SHADOW_ENTRY_TYPE,
            score_old=score,
            score_v2=None,
            breakdown=payload,
        )
        _register_swing_retest_shadow_outcome(dict(payload))
        return True
    except Exception:
        return False


def _early_shadow_candidate_class(
    side=None,
    ctx=None,
    bos_type=None,
    bos_n=None,
    cont_score=None,
    entry=None,
    sl=None,
    tp=None,
    rr=None,
    score=None,
    score_breakdown=None,
):
    if score is not None or score_breakdown is not None:
        return "EARLY_CONT_SCORE_READY"
    if entry is not None and sl is not None and tp is not None and rr is not None:
        return "EARLY_CONT_GEOMETRY_READY"
    if side and ctx and bos_type is not None and bos_n is not None and cont_score is not None:
        return "EARLY_CONT_STRUCTURE_READY"
    return "EARLY_CONT_UNKNOWN"


def _early_bos_n_bucket(bos_n):
    try:
        bos_n_int = int(bos_n)
        if bos_n_int <= 10:
            return bos_n_int
        return "10+"
    except Exception:
        return str(bos_n) if bos_n is not None else ""


def _log_early_shadow_candidate(
    symbol,
    reject_reason,
    side=None,
    ctx=None,
    reason=None,
    h1_trend=None,
    bos_type=None,
    bos_level=None,
    bos_n=None,
    cont_score=None,
    cont_threshold=None,
    cont_factors=None,
    entry=None,
    signal_entry=None,
    sl=None,
    tp=None,
    rr=None,
    exhaustion_cls=None,
    exhaustion_score=None,
    score=None,
    score_breakdown=None,
    signal_created_ts=None,
    smc_ctx=None,
):
    global _early_shadow_logged_this_scan

    try:
        if not _paper_early_shadow_enabled():
            return False
        max_per_scan = _early_shadow_max_per_scan()
        if _early_shadow_logged_this_scan >= max_per_scan:
            return False

        ctx = dict(ctx or {})
        reason_copy = list(reason or [])
        mkt_state = ctx.get("mkt_state") or _tag_value(reason_copy, "MKT:")
        phase = ctx.get("phase") or _tag_value(reason_copy, "PHASE:")
        geometry_status = _early_geometry_status(entry=entry, sl=sl, tp=tp, rr=rr)
        key = (
            symbol,
            reject_reason,
            side or "",
            mkt_state or "",
            phase or "",
            bos_type or "",
            _early_bos_n_bucket(bos_n),
            geometry_status,
        )
        now = time.time()
        last = _early_shadow_seen.get(key, 0)
        if now - last < EARLY_SHADOW_TTL_SECS:
            return False
        _early_shadow_seen[key] = now
        _early_shadow_logged_this_scan += 1

        shadow_candidate_class = _early_shadow_candidate_class(
            side=side,
            ctx=ctx,
            bos_type=bos_type,
            bos_n=bos_n,
            cont_score=cont_score,
            entry=entry,
            sl=sl,
            tp=tp,
            rr=rr,
            score=score,
            score_breakdown=score_breakdown,
        )
        smc_ctx = dict(smc_ctx or {})
        payload = {
            "status": "EARLY_SHADOW",
            "entry_type": EARLY_SHADOW_ENTRY_TYPE,
            "timestamp": now,
            "symbol": symbol,
            "side": side or "",
            "reject_reason": reject_reason,
            "shadow_candidate_class": shadow_candidate_class,
            "market_state": ctx.get("market_state") or mkt_state or "",
            "mkt_state": mkt_state or "",
            "phase": phase or "",
            "regime": ctx.get("regime") or _tag_value(reason_copy, "regime:") or "",
            "h1_trend": h1_trend or "",
            "bos_type": bos_type or "",
            "bos_level": bos_level if bos_level is not None else "",
            "bos_n": bos_n if bos_n is not None else "",
            "cont_score": cont_score if cont_score is not None else "",
            "cont_threshold": cont_threshold if cont_threshold is not None else "",
            "cont_factors": _json_safe_shadow_value(list(cont_factors or [])),
            "entry": entry if entry is not None else "",
            "signal_entry": signal_entry if signal_entry is not None else "",
            "sl": sl if sl is not None else "",
            "tp": tp if tp is not None else "",
            "rr": rr if rr is not None else "",
            "geometry_status": geometry_status,
            "exhaustion_cls": exhaustion_cls if exhaustion_cls is not None else "",
            "exhaustion_score": exhaustion_score if exhaustion_score is not None else "",
            "score": score if score is not None else "",
            "score_breakdown": _json_safe_shadow_value(score_breakdown or {}),
            "reason": reason_copy,
            "signal_created_ts": signal_created_ts if signal_created_ts is not None else now,
            "smc_zone": smc_ctx.get("smc_zone", ""),
            "liquidity_sweep": smc_ctx.get("liquidity_sweep", ""),
            "bos_confirmation": smc_ctx.get("bos_confirmation", ""),
            "smc_bias": smc_ctx.get("smc_bias", ""),
            "range_context": smc_ctx.get("range_context", ""),
            "invalid_context": _json_safe_shadow_value(smc_ctx.get("invalid_context", [])),
        }
        payload["structural_context"] = _safe_structural_context(
            signal=payload,
            ctx=ctx,
            smc_ctx=smc_ctx,
            reason=reason_copy,
        )
        payload = _json_safe_shadow_value(payload)
        _log_score_shadow(
            symbol,
            "EARLY_SHADOW",
            reject_reason,
            EARLY_SHADOW_ENTRY_TYPE,
            score_old=score,
            score_v2=None,
            breakdown=payload,
        )
        _register_early_cont_shadow_outcome(dict(payload))
        return True
    except Exception:
        return False


def _should_shadow_reversal_candidate(reject_reason, mkt_state, reason, score=None):
    tags = _reason_tags(reason)
    exhaustion = _tag_value(reason, "Exhaustion:")
    bos_type = _tag_value(reason, "BOS:")

    if reject_reason in ("REVERSAL_CONTEXT_FAIL", "WRONG_SETUP_FOR_MARKET_STATE"):
        if "BOS:NEAR" in tags:
            return True
        if mkt_state == "TREND" and exhaustion in ("EXTENDED", "EXHAUSTED"):
            return True
        if mkt_state == "ACCUMULATION" and exhaustion == "EXTENDED":
            return True
        if exhaustion in ("EXTENDED", "EXHAUSTED") and score is None:
            return True

    if exhaustion in ("EXTENDED", "EXHAUSTED") and score is not None and score >= 2.0:
        return True
    if bos_type == "NEAR" and score is not None and score >= 0:
        return True

    return False


def _reversal_shadow_candidate_class(ctx, mkt_state, exhaustion_cls, bos_type):
    market_state = (ctx or {}).get("market_state") or ""
    is_exhaustion_context = market_state == "EXHAUSTION" or mkt_state == "EXHAUSTION"
    if is_exhaustion_context and exhaustion_cls in ("EXTENDED", "EXHAUSTED") and bos_type == "WEAK":
        return f"EXHAUSTION_{exhaustion_cls}_BOS_WEAK"
    return ""


def _compute_reversal_shadow_geometry(symbol, shadow, reason):
    def _unavailable(reason_code):
        return {
            "geometry_status": "unavailable",
            "geometry_reason": reason_code,
            "valid_geometry": False,
            "geometry_flags": ["geometry_unavailable"],
            "sl_distance_pct": "",
            "tp_distance_pct": "",
        }

    try:
        ctx = shadow.get("ctx") or {}
        df15 = shadow.get("df15")
        df1h = shadow.get("df1h")
        rev_side = shadow.get("side")
        bos_type = shadow.get("bos_type")
        bos_level = shadow.get("bos_level")
        retest_ok = shadow.get("retest_ok")
        retest_str = shadow.get("retest_str")
        exhaustion_cls = shadow.get("exhaustion_cls")

        if df15 is None or df1h is None:
            return _unavailable("missing_dataframe")
        if rev_side not in ("LONG", "SHORT"):
            return _unavailable("missing_side")
        if bos_level is None or bos_level <= 0:
            return _unavailable("missing_bos_level")

        signal_entry = df15["close"].iloc[-2]
        entry = df15["close"].iloc[-1]
        atr = (df15["high"] - df15["low"]).rolling(14).mean().iloc[-2]
        if math.isnan(atr) or atr <= 0:
            return _unavailable("atr_invalid")

        if rev_side == "LONG":
            sl = df15["low"].iloc[-10:-2].min() - atr * 0.5
            tp = entry + atr * 4.0
        else:
            sl = df15["high"].iloc[-10:-2].max() + atr * 0.5
            tp = entry - atr * 4.0

        risk = abs(entry - sl)
        rr = abs(tp - entry) / risk if risk > 0 else 0
        if risk <= 0:
            return _unavailable("risk_invalid")

        wy_type, wy_name, _ = _detect_wyckoff_v3(df15, rev_side)
        if not wy_type:
            wy_type, wy_name = "WEAK", "NONE"

        shadow_reason = list(reason or [])
        shadow_reason.append(f"Wyckoff:{wy_type}_{wy_name}")
        if rr < 1.5:
            shadow_reason.append("Soft:RR_FAIL")
        shadow_reason.append(f"RR:{round(rr, 2)}")

        candle_strength = _classify_candle_confirm(df15)
        score, score_breakdown = _compute_unified_score(
            ctx, bos_type, bos_level, retest_ok, retest_str, candle_strength, rr,
            entry, signal_entry, rev_side, "REVERSAL_CONFIRM",
            exhaustion_cls, wy_type, wy_name, df15, df1h, shadow_reason
        )
        smc_ctx = compute_smc_context(
            df15,
            df1h,
            shadow.get("df4h"),
            rev_side,
            bos_type,
            bos_level,
            ctx,
        )
        if isinstance(score_breakdown, dict):
            score_breakdown["smc"] = smc_ctx
        geometry_flags = []
        sl_distance_pct = float(abs(entry - sl) / entry) if entry > 0 else ""
        tp_distance_pct = float(abs(tp - entry) / entry) if entry > 0 else ""

        if rev_side == "LONG":
            if sl >= entry:
                geometry_flags.append("invalid_sl_side")
            if tp <= entry:
                geometry_flags.append("invalid_tp_side")
        elif rev_side == "SHORT":
            if sl <= entry:
                geometry_flags.append("invalid_sl_side")
            if tp >= entry:
                geometry_flags.append("invalid_tp_side")

        if sl_distance_pct != "":
            if sl_distance_pct < 0.0005:
                geometry_flags.append("too_tight_sl")
            if sl_distance_pct > 0.20:
                geometry_flags.append("extreme_sl_distance")
        if tp_distance_pct != "" and tp_distance_pct > 0.50:
            geometry_flags.append("extreme_tp_distance")
        try:
            rr_for_flags = float(rr)
        except (TypeError, ValueError):
            rr_for_flags = None
        if rr_for_flags is not None:
            if rr_for_flags >= 10:
                geometry_flags.append("extreme_rr")
            if rr_for_flags <= 0:
                geometry_flags.append("invalid_rr")

        return {
            "geometry_status": "computed",
            "geometry_reason": "",
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "rr": rr,
            "score": score,
            "score_breakdown": score_breakdown,
            "valid_geometry": len(geometry_flags) == 0,
            "geometry_flags": geometry_flags,
            "sl_distance_pct": sl_distance_pct,
            "tp_distance_pct": tp_distance_pct,
            "smc_zone": smc_ctx.get("smc_zone", ""),
            "liquidity_sweep": smc_ctx.get("liquidity_sweep", ""),
            "bos_confirmation": smc_ctx.get("bos_confirmation", ""),
            "smc_bias": smc_ctx.get("smc_bias", ""),
            "range_context": smc_ctx.get("range_context", ""),
            "invalid_context": _json_safe_shadow_value(smc_ctx.get("invalid_context", [])),
        }
    except Exception as exc:
        return _unavailable(type(exc).__name__)


def _log_reversal_shadow_candidate(
    symbol,
    reject_reason,
    side=None,
    ctx=None,
    reason=None,
    bos_type=None,
    exhaustion_cls=None,
    score=None,
    entry=None,
    sl=None,
    tp=None,
    signal_created_ts=None,
    shadow=None,
):
    global _reversal_shadow_logged_this_scan

    if not _paper_reversal_shadow_enabled():
        return False
    max_per_scan = _reversal_shadow_max_per_scan()
    if _reversal_shadow_logged_this_scan >= max_per_scan:
        return False

    ctx = ctx or {}
    reason = list(reason or [])
    mkt_state = ctx.get("mkt_state") or _tag_value(reason, "MKT:")
    exhaustion_cls = exhaustion_cls or _tag_value(reason, "Exhaustion:")
    bos_type = bos_type or _tag_value(reason, "BOS:")

    if not _should_shadow_reversal_candidate(reject_reason, mkt_state, reason, score=score):
        return False

    now = time.time()
    key = (symbol, reject_reason, mkt_state, exhaustion_cls, bos_type)
    last = _reversal_shadow_seen.get(key, 0)
    if now - last < REVERSAL_SHADOW_TTL_SECS:
        return False
    _reversal_shadow_seen[key] = now
    _reversal_shadow_logged_this_scan += 1

    shadow_candidate_class = _reversal_shadow_candidate_class(ctx, mkt_state, exhaustion_cls, bos_type)
    geometry = {}
    if shadow_candidate_class and entry is None and sl is None and tp is None:
        geometry = _compute_reversal_shadow_geometry(symbol, shadow or {}, reason)
        if geometry.get("geometry_status") == "computed":
            entry = geometry.get("entry")
            sl = geometry.get("sl")
            tp = geometry.get("tp")
            score = geometry.get("score") if score is None else score

    rr_value = geometry.get("rr", "")
    if rr_value == "" and entry is not None and sl is not None and tp is not None:
        risk = abs(entry - sl)
        rr_value = abs(tp - entry) / risk if risk > 0 else ""

    geometry_status = geometry.get("geometry_status", "not_attempted")
    geometry_reason = geometry.get("geometry_reason", "")
    if geometry_status == "not_attempted" and not geometry_reason:
        geometry_reason = "not_shadow_geometry_class"

    payload = {
        "timestamp": now,
        "symbol": symbol,
        "side": side or "",
        "entry_type": REVERSAL_SHADOW_ENTRY_TYPE,
        "original_reject_reason": reject_reason,
        "market_state": ctx.get("market_state") or mkt_state or "",
        "mkt_state": mkt_state or "",
        "exhaustion": exhaustion_cls or "",
        "bos_type": bos_type or "",
        "phase": ctx.get("phase") or _tag_value(reason, "PHASE:") or "",
        "score": score if score is not None else "",
        "entry": entry if entry is not None else "",
        "sl": sl if sl is not None else "",
        "tp": tp if tp is not None else "",
        "rr": rr_value,
        "geometry_status": geometry_status,
        "geometry_reason": geometry_reason,
        "shadow_candidate_class": shadow_candidate_class,
        "valid_geometry": geometry.get("valid_geometry", False),
        "geometry_flags": geometry.get("geometry_flags", ["not_attempted"]),
        "sl_distance_pct": geometry.get("sl_distance_pct", ""),
        "tp_distance_pct": geometry.get("tp_distance_pct", ""),
        "smc_zone": geometry.get("smc_zone", ""),
        "liquidity_sweep": geometry.get("liquidity_sweep", ""),
        "bos_confirmation": geometry.get("bos_confirmation", ""),
        "smc_bias": geometry.get("smc_bias", ""),
        "range_context": geometry.get("range_context", ""),
        "invalid_context": _json_safe_shadow_value(geometry.get("invalid_context", [])),
        "reason": reason,
        "tags": sorted(_reason_tags(reason)),
        "signal_created_ts": signal_created_ts if signal_created_ts is not None else "",
    }
    payload["structural_context"] = _safe_structural_context(
        signal=payload,
        ctx=ctx,
        smc_ctx={
            "smc_zone": payload.get("smc_zone", ""),
            "liquidity_sweep": payload.get("liquidity_sweep", ""),
            "bos_confirmation": payload.get("bos_confirmation", ""),
            "smc_bias": payload.get("smc_bias", ""),
            "range_context": payload.get("range_context", ""),
            "invalid_context": payload.get("invalid_context", []),
        },
        reason=reason,
    )
    _log_score_shadow(
        symbol,
        "REVERSAL_SHADOW",
        reject_reason,
        REVERSAL_SHADOW_ENTRY_TYPE,
        score_old=score,
        score_v2=None,
        breakdown=payload,
    )
    _register_reversal_shadow_outcome(dict(payload))
    print(
        f"[REVERSAL SHADOW] {symbol} side={side or '?'} reject={reject_reason} "
        f"state={mkt_state or '?'} exhaustion={exhaustion_cls or '?'} bos={bos_type or '?'}"
    )
    return True


def _block_signal(symbol, reason_code, entry_type, score=None, structural_payload=None):
    summary_key = FILTER_BLOCK_KEYS.get(reason_code)
    if summary_key:
        _scan_filter_summary[summary_key] += 1
    if DEBUG_FILTERS:
        print(f"[BLOCK] {reason_code} {symbol} entry_type={entry_type}")
    log_confirm_reject(
        symbol,
        reason_code,
        score=score,
        entry_type=entry_type,
        structural_payload=structural_payload,
    )


def _record_reversal_gate_reject(reason_code, mkt_state, tags):
    if reason_code == "WRONG_SETUP_FOR_MARKET_STATE":
        _reversal_gate_summary["market_state_gate_failure"] += 1
        if mkt_state == "TREND" and "Exhaustion:EXTENDED" not in tags:
            _reversal_gate_summary["missing_extended_exhaustion"] += 1
    elif reason_code == "REVERSAL_CONTEXT_FAIL":
        _reversal_gate_summary["context_fail"] += 1
        if "Exhaustion:EXTENDED" not in tags:
            _reversal_gate_summary["missing_extended_exhaustion"] += 1
        if "BOS:NEAR" not in tags:
            _reversal_gate_summary["missing_bos_near"] += 1


def _passes_market_state_gate(symbol, entry_type, mkt_state, reason, shadow=None):
    tags = _reason_tags(reason)

    if mkt_state == "ACCUMULATION" and entry_type != "REVERSAL_CONFIRM":
        _block_signal(symbol, "WRONG_SETUP_FOR_MARKET_STATE", entry_type)
        return False

    if mkt_state == "TREND" and entry_type == "REVERSAL_CONFIRM" and "Exhaustion:EXTENDED" not in tags:
        _record_reversal_gate_reject("WRONG_SETUP_FOR_MARKET_STATE", mkt_state, tags)
        if shadow is not None:
            _log_reversal_shadow_candidate(
                symbol,
                "WRONG_SETUP_FOR_MARKET_STATE",
                side=shadow.get("side"),
                ctx=shadow.get("ctx"),
                reason=reason,
                bos_type=shadow.get("bos_type"),
                exhaustion_cls=shadow.get("exhaustion_cls"),
                signal_created_ts=shadow.get("signal_created_ts"),
                shadow=shadow,
            )
        _block_signal(symbol, "WRONG_SETUP_FOR_MARKET_STATE", entry_type)
        return False

    return True


def _passes_reversal_context(symbol, entry_type, mkt_state, reason, shadow=None):
    if entry_type != "REVERSAL_CONFIRM":
        return True

    tags = _reason_tags(reason)
    if (
        mkt_state == "ACCUMULATION"
        and "Exhaustion:EXTENDED" in tags
        and "BOS:NEAR" in tags
    ):
        return True

    _record_reversal_gate_reject("REVERSAL_CONTEXT_FAIL", mkt_state, tags)
    if shadow is not None:
        _log_reversal_shadow_candidate(
            symbol,
            "REVERSAL_CONTEXT_FAIL",
            side=shadow.get("side"),
            ctx=shadow.get("ctx"),
            reason=reason,
            bos_type=shadow.get("bos_type"),
            exhaustion_cls=shadow.get("exhaustion_cls"),
            signal_created_ts=shadow.get("signal_created_ts"),
            shadow=shadow,
        )
    _block_signal(symbol, "REVERSAL_CONTEXT_FAIL", entry_type)
    return False


def _passes_pre_score_tag_gate(symbol, entry_type, reason, structural_payload=None):
    tags = _reason_tags(reason)
    if entry_type in ("CONFIRM", EARLY_LEGACY_SCORE_TYPE, EARLY_CONT_ENTRY_TYPE) and "TREND_FAIL" in tags:
        _block_signal(symbol, "TREND_FAIL", entry_type, structural_payload=structural_payload)
        return False
    return True


def _passes_entry_quality_gate(symbol, entry_type, score, reason, structural_payload=None):
    tags = _reason_tags(reason)

    if score is None:
        return False

    if score < 7:
        tags.add("LOW_SCORE")

    if "LOW_SCORE" in tags:
        _block_signal(symbol, "LOW_SCORE", entry_type, score=score, structural_payload=structural_payload)
        return False

    if entry_type in ("CONFIRM", EARLY_LEGACY_SCORE_TYPE, EARLY_CONT_ENTRY_TYPE) and "TREND_FAIL" in tags:
        _block_signal(symbol, "TREND_FAIL", entry_type, score=score, structural_payload=structural_payload)
        return False

    if "BOS:WEAK" in tags and score >= 9:
        has_retest = any(
            t.startswith("Retest:") and t not in ("Retest:NONE", "Retest:VIOLATED")
            for t in tags
        )
        has_strong_confirm = (
            has_retest
            or "Candle:STRONG" in tags
            or "Exhaustion:EXHAUSTED" in tags
        )
        if not has_strong_confirm:
            print(f"[BLOCK] HIGH_SCORE_WEAK_BOS {symbol}")
            _block_signal(symbol, "HIGH_SCORE_WEAK_BOS", entry_type, score=score)
            return False

    if score >= 8:
        return True

    if 7 <= score < 8:
        if "BOS:WEAK" in tags:
            _block_signal(symbol, "MID_SCORE_WEAK_BOS", entry_type, score=score, structural_payload=structural_payload)
            return False
        if "BOS:TRAP" in tags:
            print(f"[PASS] MID_SCORE_BOS_TRAP {symbol} entry_type={entry_type} score={round(score, 2)}")
            return True
        _block_signal(symbol, "MID_SCORE_WEAK_BOS", entry_type, score=score, structural_payload=structural_payload)
        return False

    return True

SWING_TOP_N        = 7
SWING_RR_MIN       = 3.0
SWING_RISK_MULT    = 0.5
MIN_SL_ATR         = 0.25
MAX_SL_ATR         = 3.0
H1_WINDOW          = 20
H4_WINDOW          = 30
COMPRESS_MIN_SCORE = 3

compression_watchlist  = {}
compression_alert_sent = {}
_swing_log_cache       = {}
_LOG_COOLDOWN          = 300

# =====================================================================
# MARKET CONTEXT
# =====================================================================

def build_market_context(df15, df1h):
    mkt_state, mkt_metrics = get_market_state(df15)
    market_state = mkt_state

    ema34_m15 = df15["close"].ewm(span=34).mean().iloc[-2]
    close_m15 = df15["close"].iloc[-2]
    dist_ema  = abs(close_m15 - ema34_m15) / ema34_m15

    if dist_ema > 0.01 and mkt_state == "TREND":
        market_state = "EXHAUSTION"

    phase   = "NEUTRAL"
    impulse = False

    candle    = df15.iloc[-2]
    body      = abs(candle["close"] - candle["open"])
    avg_range = (df15["high"] - df15["low"]).rolling(20).mean().iloc[-2]
    move      = abs(df15["close"].iloc[-2] - df15["close"].iloc[-6]) / df15["close"].iloc[-6]

    if body > avg_range * 1.5 or move > 0.03:
        impulse = True

    recent     = df1h.iloc[-20:-2]
    range_high = recent["high"].max()
    range_low  = recent["low"].min()
    price      = df1h["close"].iloc[-2]

    dist_high = abs(price - range_high) / max(range_high, 1e-9)
    dist_low  = abs(price - range_low)  / max(range_low,  1e-9)

    phase = "NEUTRAL"
    if dist_high < 0.015:
        phase = "PRE_BREAK_HIGH"
    elif dist_low < 0.015:
        phase = "PRE_BREAK_LOW"
    elif mkt_state == "TREND":
        phase = "BREAKOUT_STRONG" if impulse else "BREAKOUT_WEAK"

    early_allowed  = True
    confirm_boost  = 1.0
    momentum_allow = False

    if mkt_state == "DEAD":
        early_allowed = False
    elif mkt_state == "EXHAUSTION":
        # KHÔNG block nữa
        # chỉ siết nhẹ
        if phase not in ("PRE_BREAK", "BREAKOUT_STRONG"):
            early_allowed = True   # vẫn cho phép

    if phase == "PRE_BREAK_HIGH":
        confirm_boost = 0.7
    elif phase == "PRE_BREAK_LOW":
        confirm_boost = 1.0
    elif phase == "BREAKOUT_STRONG":
        confirm_boost  = 1.2
        momentum_allow = True
    elif phase == "BREAKOUT_WEAK":
        confirm_boost = 1.0

    _state_bias = 0
    if mkt_state == "ACCUMULATION": _state_bias = +1
    elif mkt_state == "TREND":      _state_bias = +1
    elif mkt_state == "EXHAUSTION": _state_bias = -1
    elif mkt_state == "DEAD":       _state_bias = -2

    return {
        "mkt_state":      mkt_state,
        "mkt_metrics":    mkt_metrics,
        "market_state":   market_state,
        "ema34_m15":      ema34_m15,
        "close_m15":      close_m15,
        "dist_ema":       dist_ema,
        "phase":          phase,
        "impulse":        impulse,
        "candle":         candle,
        "body":           body,
        "avg_range":      avg_range,
        "move":           move,
        "recent":         recent,
        "range_high":     range_high,
        "range_low":      range_low,
        "price":          price,
        "dist_high":      dist_high,
        "dist_low":       dist_low,
        "confirm_boost":  confirm_boost,
        "momentum_allow": momentum_allow,
        "_state_bias":    _state_bias,
        "early_allowed":  early_allowed,
    }


def compute_smc_context(df15, df1h, df4h, side, bos_type, bos_level, ctx):
    defaults = {
        "smc_zone": "UNKNOWN",
        "liquidity_sweep": "NONE",
        "bos_confirmation": "UNKNOWN",
        "smc_bias": "UNKNOWN",
        "range_context": "UNKNOWN",
        "invalid_context": [],
    }

    try:
        smc_zone = defaults["smc_zone"]
        liquidity_sweep = defaults["liquidity_sweep"]
        bos_confirmation = defaults["bos_confirmation"]
        smc_bias = defaults["smc_bias"]
        range_context = defaults["range_context"]
        flags = []

        def _finite(value):
            try:
                value = float(value)
                return value if math.isfinite(value) else None
            except Exception:
                return None

        if df4h is not None and len(df4h) >= 50:
            htf_high = _finite(df4h["high"].iloc[-50:-2].max())
            htf_low = _finite(df4h["low"].iloc[-50:-2].min())
            price = _finite(df1h["close"].iloc[-2])
            if htf_high is not None and htf_low is not None and price is not None and htf_high > htf_low:
                position = (price - htf_low) / (htf_high - htf_low)
                if position >= 0.65:
                    smc_zone = "PREMIUM"
                elif position <= 0.35:
                    smc_zone = "DISCOUNT"
                else:
                    smc_zone = "EQUILIBRIUM"

        if df15 is not None and len(df15) >= 24:
            swing_high = _finite(df15["high"].iloc[-20:-4].max())
            swing_low = _finite(df15["low"].iloc[-20:-4].min())
            if swing_high is not None and swing_low is not None:
                for i in (-4, -3, -2):
                    candle = df15.iloc[i]
                    high = _finite(candle["high"])
                    low = _finite(candle["low"])
                    close = _finite(candle["close"])
                    if high is None or low is None or close is None:
                        continue
                    if high > swing_high and close < swing_high and (high - swing_high) / swing_high >= 0.001:
                        liquidity_sweep = "SWEEP_HIGH"
                        break
                    if low < swing_low and close > swing_low and (swing_low - low) / swing_low >= 0.001:
                        liquidity_sweep = "SWEEP_LOW"
                        break

        bos_level_value = _finite(bos_level)
        if df15 is not None and len(df15) >= 16 and bos_level_value is not None and bos_level_value > 0:
            bos_candle = df15.iloc[-3]
            open_ = _finite(bos_candle["open"])
            high = _finite(bos_candle["high"])
            low = _finite(bos_candle["low"])
            close = _finite(bos_candle["close"])
            atr = _finite((df15["high"] - df15["low"]).rolling(14).mean().iloc[-2])
            last_close = _finite(df15["close"].iloc[-2])
            if None not in (open_, high, low, close, atr, last_close):
                body = abs(close - open_)
                rng = high - low
                body_ratio = body / rng if rng > 0 else 0
                move_pct = abs(close - bos_level_value) / bos_level_value
                if atr >= 0 and body_ratio >= 0.6 and move_pct >= atr / bos_level_value:
                    bos_confirmation = "DISPLACEMENT"
                elif body_ratio >= 0.4 and move_pct >= 0.003:
                    bos_confirmation = "CLOSE_THROUGH"
                elif abs(last_close - bos_level_value) / bos_level_value <= 0.005:
                    bos_confirmation = "RETESTED"
                else:
                    bos_confirmation = "NEAR"

        h1_bias = trend_h1(df1h)
        h4_bias = None
        if df4h is not None and len(df4h) >= 50:
            ema34 = _finite(df4h["close"].ewm(span=34).mean().iloc[-2])
            ema89 = _finite(df4h["close"].ewm(span=89).mean().iloc[-2])
            if ema34 is not None and ema89 is not None:
                h4_bias = "LONG" if ema34 > ema89 else "SHORT"
        if h1_bias == "LONG" and (h4_bias is None or h4_bias == "LONG"):
            smc_bias = "BULLISH"
        elif h1_bias == "SHORT" and (h4_bias is None or h4_bias == "SHORT"):
            smc_bias = "BEARISH"
        elif h1_bias is None:
            smc_bias = "NEUTRAL"
        else:
            smc_bias = "NEUTRAL"

        ctx = ctx or {}
        dist_high = _finite(ctx.get("dist_high"))
        dist_low = _finite(ctx.get("dist_low"))
        if dist_high is not None and dist_low is not None:
            if dist_high < 0.015 and dist_low < 0.015:
                range_context = "MID"
            elif dist_high < 0.015:
                range_context = "RANGE_HIGH"
            elif dist_low < 0.015:
                range_context = "RANGE_LOW"
            else:
                range_context = "MID"

        range_high = _finite(ctx.get("range_high"))
        range_low = _finite(ctx.get("range_low"))
        bos_close = None
        if df15 is not None and len(df15) >= 3:
            bos_close = _finite(df15["close"].iloc[-3])

        if side == "SHORT" and smc_zone == "DISCOUNT" and range_low is not None and bos_close is not None:
            breakdown_confirmed = bos_close < range_low * (1 - 0.005)
            if not breakdown_confirmed:
                flags.append("SHORT_IN_DISCOUNT_WITHOUT_BREAKDOWN")
        if side == "LONG" and smc_zone == "PREMIUM" and range_high is not None and bos_close is not None:
            breakout_confirmed = bos_close > range_high * (1 + 0.005)
            if not breakout_confirmed:
                flags.append("LONG_IN_PREMIUM_WITHOUT_BREAKOUT")
        if bos_confirmation == "NEAR" and liquidity_sweep == "NONE":
            flags.append("BOS_NEAR_NO_SWEEP_NO_DISPLACEMENT")
        if side == "LONG" and smc_bias == "BEARISH":
            flags.append("COUNTER_HTF_BIAS")
        if side == "SHORT" and smc_bias == "BULLISH":
            flags.append("COUNTER_HTF_BIAS")

        return {
            "smc_zone": smc_zone,
            "liquidity_sweep": liquidity_sweep,
            "bos_confirmation": bos_confirmation,
            "smc_bias": smc_bias,
            "range_context": range_context,
            "invalid_context": flags,
        }
    except Exception:
        return dict(defaults)


def _with_smc_breakdown(breakdown, smc_ctx):
    if isinstance(breakdown, dict):
        logged = dict(breakdown)
    else:
        logged = {}
    logged["smc"] = smc_ctx
    return logged


def _struct_reason_tags(reason):
    return {str(item or "").upper() for item in reason or []}


def _struct_reason_value(reason, prefix, default=""):
    value = _reason_tag_value_ci(reason, prefix)
    return value if value not in (None, "") else default


def _struct_first(*values, default="UNKNOWN"):
    for value in values:
        if value not in (None, ""):
            return value
    return default


def _struct_float(value):
    try:
        if value in (None, ""):
            return None
        value = float(value)
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    except (TypeError, ValueError):
        return None


def _struct_direction(value):
    value = str(value or "").upper()
    if value in ("LONG", "UP", "BULL", "BULLISH", "TREND_UP", "UPTREND"):
        return "BULLISH"
    if value in ("SHORT", "DOWN", "BEAR", "BEARISH", "TREND_DOWN", "DOWNTREND"):
        return "BEARISH"
    return ""


def _struct_range_context(signal):
    explicit = str(signal.get("range_context") or "").upper()
    if explicit not in ("", "UNKNOWN"):
        return explicit

    price = _struct_float(
        _struct_first(
            signal.get("close"),
            signal.get("entry"),
            signal.get("signal_entry"),
            default="",
        )
    )
    high = _struct_float(
        _struct_first(
            signal.get("range_high"),
            signal.get("live_range_high"),
            signal.get("setup_range_high"),
            default="",
        )
    )
    low = _struct_float(
        _struct_first(
            signal.get("range_low"),
            signal.get("live_range_low"),
            signal.get("setup_range_low"),
            default="",
        )
    )
    if price is None or high is None or low is None or high <= low:
        return "UNKNOWN"

    pos = (price - low) / (high - low)
    if pos >= 0.66:
        return "RANGE_HIGH"
    if pos <= 0.34:
        return "RANGE_LOW"
    return "MID"


def _struct_truthy(value):
    if value is True:
        return True
    if isinstance(value, str):
        return value.strip().upper() in ("1", "TRUE", "YES", "Y", "OK", "VALID", "PRESENT")
    return False


def _struct_add_unique(items, value):
    if value and value not in items:
        items.append(value)


def _safe_structural_context(signal=None, ctx=None, smc_ctx=None, reason=None):
    signal = dict(signal or {})
    ctx = dict(ctx or signal.get("_ctx") or {})
    smc_ctx = dict(smc_ctx or signal.get("smc") or {})
    reason = list(reason if reason is not None else signal.get("reason") or [])
    tags = _struct_reason_tags(reason)

    side = str(signal.get("side") or "").upper()
    entry_type = str(signal.get("entry_type") or "").upper()
    mkt_state = str(
        _struct_first(
            ctx.get("mkt_state"),
            ctx.get("market_state"),
            signal.get("mkt_state"),
            signal.get("market_state"),
            _struct_reason_value(reason, "MKT:"),
            default="UNKNOWN",
        )
    ).upper()
    phase = str(
        _struct_first(
            ctx.get("phase"),
            signal.get("phase"),
            _struct_reason_value(reason, "PHASE:"),
            default="UNKNOWN",
        )
    ).upper()
    bos_type = str(
        _struct_first(
            signal.get("bos_type"),
            _struct_reason_value(reason, "BOS:"),
            default="UNKNOWN",
        )
    ).upper()
    retest = str(
        _struct_first(
            signal.get("retest_status"),
            _struct_reason_value(reason, "Retest:", ""),
            default="",
        )
    ).upper()
    if not retest:
        if signal.get("in_retest_zone") is True or signal.get("near_ema") is True:
            retest = "RETEST"
        elif signal.get("in_retest_zone") is False and signal.get("near_ema") is False:
            retest = "NONE"
    reject_reason = str(signal.get("reject_reason") or signal.get("original_reject_reason") or "").upper()
    if not reject_reason:
        for known_reject in ("BOS_WAITING", "RETEST_ZONE_FAIL", "RETEST_TRIGGER_FAIL", "STALE_AFTER_BOS"):
            if known_reject in tags:
                reject_reason = known_reject
                break
    if entry_type == SWING_SHADOW_ENTRY_TYPE:
        retest_zone_reached = signal.get("in_retest_zone") is True or signal.get("near_ema") is True
        if reject_reason == "BOS_WAITING":
            retest = "NONE"
        elif reject_reason == "RETEST_ZONE_FAIL":
            retest = "VIOLATED"
        elif reject_reason == "RETEST_TRIGGER_FAIL":
            retest = "RETEST" if retest_zone_reached else "UNKNOWN"
        elif reject_reason == "STALE_AFTER_BOS":
            retest = "VIOLATED" if retest_zone_reached else "NONE"
    candle = str(_struct_reason_value(reason, "Candle:", "")).upper()
    exhaustion = str(
        _struct_first(
            signal.get("exhaustion_cls"),
            signal.get("exhaustion"),
            _struct_reason_value(reason, "Exhaustion:"),
            default="UNKNOWN",
        )
    ).upper()

    explicit_direction = _struct_direction(
        _struct_first(
            signal.get("h1_trend"),
            signal.get("bias_type"),
            signal.get("bias"),
            signal.get("breakout_dir"),
            default="",
        )
    )
    score_breakdown = signal.get("score_breakdown")
    if not isinstance(score_breakdown, dict):
        score_breakdown = {}

    smc_zone = str(_struct_first(signal.get("smc_zone"), smc_ctx.get("smc_zone"), default="UNKNOWN")).upper()
    liquidity_sweep = str(_struct_first(signal.get("liquidity_sweep"), smc_ctx.get("liquidity_sweep"), default="UNKNOWN")).upper()
    bos_confirmation = str(_struct_first(signal.get("bos_confirmation"), smc_ctx.get("bos_confirmation"), default="UNKNOWN")).upper()
    smc_bias = str(_struct_first(signal.get("smc_bias"), smc_ctx.get("smc_bias"), explicit_direction, default="UNKNOWN")).upper()
    range_context = str(_struct_first(signal.get("range_context"), smc_ctx.get("range_context"), _struct_range_context(signal), default="UNKNOWN")).upper()
    invalid_context = signal.get("invalid_context", smc_ctx.get("invalid_context", []))
    if not isinstance(invalid_context, list):
        invalid_context = []
    invalid_tags = {str(item or "").upper() for item in invalid_context}

    if smc_bias == "BULLISH":
        dow_trend_context = "BULL"
    elif smc_bias == "BEARISH":
        dow_trend_context = "BEAR"
    elif mkt_state in ("ACCUMULATION", "RANGE", "NEUTRAL"):
        dow_trend_context = "RANGE"
    elif mkt_state == "EXHAUSTION":
        dow_trend_context = "TRANSITION"
    else:
        dow_trend_context = "UNKNOWN"

    if phase in ("BREAKOUT_STRONG", "BREAKOUT_WEAK"):
        dow_phase = "EXPANSION"
    elif phase in ("PRE_BREAK_HIGH", "PRE_BREAK_LOW", "PRE_BREAK"):
        dow_phase = "PULLBACK"
    elif exhaustion in ("EXTENDED", "EXHAUSTED") or mkt_state == "EXHAUSTION":
        dow_phase = "LATE_TREND"
    elif mkt_state == "ACCUMULATION":
        dow_phase = "ACCUMULATION"
    elif mkt_state == "DISTRIBUTION":
        dow_phase = "DISTRIBUTION"
    elif mkt_state in ("RANGE", "NEUTRAL"):
        dow_phase = "CHOP"
    else:
        dow_phase = "UNKNOWN"

    if smc_bias in ("BULLISH", "BEARISH") and side in ("LONG", "SHORT"):
        external_structure = "ALIGNED" if ((side == "LONG" and smc_bias == "BULLISH") or (side == "SHORT" and smc_bias == "BEARISH")) else "CONFLICTING"
    elif mkt_state in ("RANGE", "NEUTRAL", "ACCUMULATION"):
        external_structure = "NOISY"
    else:
        external_structure = "UNKNOWN"

    body_ratio = signal.get("body_ratio")
    try:
        body_ratio = float(body_ratio)
    except (TypeError, ValueError):
        body_ratio = None
    impulse = str(signal.get("impulse") or "").upper()
    trigger_confirmed = signal.get("valid_long_trigger") is True or signal.get("valid_short_trigger") is True
    geometry_status = str(signal.get("geometry_status") or "").upper()
    if bos_confirmation == "DISPLACEMENT" or candle == "STRONG" or impulse == "STRONG" or (body_ratio is not None and body_ratio >= 0.6):
        displacement_quality = "STRONG"
    elif bos_confirmation in ("CLOSE_THROUGH", "RETESTED") or impulse == "MODERATE" or (body_ratio is not None and body_ratio >= 0.4):
        displacement_quality = "MODERATE"
    elif bos_confirmation == "NEAR" or candle == "WEAK" or (body_ratio is not None and body_ratio > 0):
        displacement_quality = "WEAK"
    elif bos_confirmation in ("NONE", ""):
        displacement_quality = "NONE"
    else:
        displacement_quality = "UNKNOWN"

    has_close_through = bos_confirmation in ("DISPLACEMENT", "CLOSE_THROUGH", "RETESTED")
    has_retest_followthrough = retest in ("RETEST", "OK", "VALID") or trigger_confirmed
    has_followthrough = has_close_through or has_retest_followthrough
    has_bos_evidence = bos_type not in ("", "UNKNOWN", "NONE") or has_close_through or liquidity_sweep in ("SWEEP_LOW", "SWEEP_HIGH")
    explicit_no_followthrough = (
        retest == "VIOLATED"
        or "SOFT:RETEST_FAIL" in tags
        or reject_reason in ("RETEST_ZONE_FAIL", "RETEST_TRIGGER_FAIL")
    )

    if "SOFT:BOS_TRAP" in tags or "BOS_TRAP" in tags or "BOS:TRAP" in tags or bos_type == "TRAP":
        bos_quality = "TRAP"
    elif liquidity_sweep in ("SWEEP_LOW", "SWEEP_HIGH") and not has_followthrough:
        bos_quality = "SWEEP_ONLY"
    elif bos_confirmation in ("DISPLACEMENT", "CLOSE_THROUGH") and displacement_quality in ("STRONG", "MODERATE"):
        bos_quality = "STRONG"
    elif explicit_no_followthrough:
        bos_quality = "NO_FOLLOWTHROUGH"
    elif has_bos_evidence and not has_followthrough and displacement_quality in ("NONE", "UNKNOWN"):
        bos_quality = "NO_FOLLOWTHROUGH"
    elif has_bos_evidence and (displacement_quality in ("NONE", "WEAK") or bos_type in ("WEAK", "NEAR", "EARLY", "SWING")):
        bos_quality = "WEAK"
    elif bos_type in ("STRONG", "CONFIRM") and displacement_quality in ("STRONG", "MODERATE"):
        bos_quality = "STRONG"
    else:
        bos_quality = "UNKNOWN"

    bos_evidence = []
    if bos_type == "TRAP" or "BOS:TRAP" in tags or "SOFT:BOS_TRAP" in tags or "BOS_TRAP" in tags:
        _struct_add_unique(bos_evidence, "bos_trap")
        _struct_add_unique(bos_evidence, "bos_type_trap")
    if bos_type == "NEAR" or bos_confirmation == "NEAR":
        _struct_add_unique(bos_evidence, "bos_near")
        _struct_add_unique(bos_evidence, "bos_type_near")
    if bos_confirmation == "RETESTED":
        _struct_add_unique(bos_evidence, "bos_retested")
        _struct_add_unique(bos_evidence, "close_through")
    if bos_confirmation == "CLOSE_THROUGH":
        _struct_add_unique(bos_evidence, "close_through")
    if bos_confirmation == "DISPLACEMENT" or displacement_quality in ("STRONG", "MODERATE"):
        _struct_add_unique(bos_evidence, "displacement")
    if displacement_quality == "WEAK":
        _struct_add_unique(bos_evidence, "weak_displacement")
    elif displacement_quality == "NONE":
        _struct_add_unique(bos_evidence, "no_displacement")
    if has_retest_followthrough:
        _struct_add_unique(bos_evidence, "valid_retest")
    if explicit_no_followthrough or bos_quality == "NO_FOLLOWTHROUGH":
        _struct_add_unique(bos_evidence, "no_followthrough")
        _struct_add_unique(bos_evidence, "downgraded_by_no_followthrough")
    if retest == "VIOLATED" or "SOFT:RETEST_FAIL" in tags or reject_reason in ("RETEST_ZONE_FAIL", "RETEST_TRIGGER_FAIL"):
        _struct_add_unique(bos_evidence, "failed_retest")
        _struct_add_unique(bos_evidence, "downgraded_by_failed_retest")
    if liquidity_sweep in ("SWEEP_LOW", "SWEEP_HIGH"):
        _struct_add_unique(bos_evidence, "liquidity_sweep")
        if bos_quality == "SWEEP_ONLY":
            _struct_add_unique(bos_evidence, "sweep_without_followthrough")
    if bos_quality == "STRONG" and not any(item in bos_evidence for item in ("close_through", "displacement")):
        _struct_add_unique(bos_evidence, "displacement")
    if bos_quality == "TRAP" and "bos_trap" not in bos_evidence:
        _struct_add_unique(bos_evidence, "bos_trap")
    if bos_quality == "SWEEP_ONLY" and "sweep_without_followthrough" not in bos_evidence:
        _struct_add_unique(bos_evidence, "sweep_without_followthrough")
    if bos_quality == "NO_FOLLOWTHROUGH" and not any(item in bos_evidence for item in ("no_followthrough", "failed_retest")):
        _struct_add_unique(bos_evidence, "no_followthrough")
    if bos_quality == "WEAK" and not any(item in bos_evidence for item in ("bos_near", "weak_displacement", "bos_type_near", "downgraded_by_no_followthrough")):
        _struct_add_unique(bos_evidence, "weak_displacement")
    if bos_quality == "UNKNOWN" and not bos_evidence:
        _struct_add_unique(bos_evidence, "insufficient_bos_evidence")

    if bos_quality in ("STRONG", "WEAK"):
        internal_structure = "INTACT"
    elif bos_quality in ("TRAP", "NO_FOLLOWTHROUGH"):
        internal_structure = "BROKEN"
    elif bos_quality == "SWEEP_ONLY":
        internal_structure = "NOISY"
    else:
        internal_structure = "UNKNOWN"

    has_liquidity_sweep = liquidity_sweep in ("SWEEP_LOW", "SWEEP_HIGH")
    has_reversal_context = (
        entry_type.startswith("REVERSAL")
        or "REVERSAL" in reject_reason
        or any(("CHOCH" in tag or "CHANGE_OF_CHARACTER" in tag or "STRUCTURE_SHIFT" in tag or "REVERSAL" in tag) for tag in tags)
    )
    has_displacement_or_close = (
        bos_confirmation in ("DISPLACEMENT", "CLOSE_THROUGH", "RETESTED")
        or displacement_quality in ("STRONG", "MODERATE")
    )
    has_retest_or_confirmation = (
        retest in ("RETEST", "OK", "VALID")
        or bos_confirmation == "RETESTED"
        or trigger_confirmed
    )
    has_choch_like_evidence = has_reversal_context or (
        has_liquidity_sweep and (has_bos_evidence or explicit_no_followthrough)
    )
    choch_invalidated = (
        bos_quality in ("TRAP", "NO_FOLLOWTHROUGH")
        or explicit_no_followthrough
        or retest == "VIOLATED"
        or "SOFT:RETEST_FAIL" in tags
    )
    choch_evidence = []
    if has_liquidity_sweep:
        choch_evidence.append("sweep")
    if has_reversal_context:
        choch_evidence.append("reversal_context")
    if has_choch_like_evidence and has_displacement_or_close:
        choch_evidence.append("displacement_or_close")
    if has_retest_or_confirmation:
        choch_evidence.append("retest_or_confirmation")
    if retest == "VIOLATED" or "SOFT:RETEST_FAIL" in tags:
        choch_evidence.append("failed_retest")
    if explicit_no_followthrough or bos_quality == "NO_FOLLOWTHROUGH":
        choch_evidence.append("no_followthrough")
    if bos_quality == "TRAP" or bos_type == "TRAP" or "BOS:TRAP" in tags or "SOFT:BOS_TRAP" in tags or "BOS_TRAP" in tags:
        choch_evidence.append("trap_context")
    if choch_invalidated:
        choch_evidence.append("invalidated")
    if not choch_evidence:
        choch_evidence.append("insufficient_evidence")
    choch_reasons = []
    if has_liquidity_sweep and has_reversal_context:
        choch_reasons.append("choch_after_sweep")
    if has_choch_like_evidence and has_displacement_or_close:
        choch_reasons.append("choch_with_displacement")
    if choch_invalidated:
        choch_reasons.append("choch_fake_failed_retest")

    if has_choch_like_evidence and choch_invalidated:
        choch_quality = "FAKE"
    elif has_liquidity_sweep and has_reversal_context and has_displacement_or_close and has_retest_or_confirmation:
        choch_quality = "VALID"
    elif has_choch_like_evidence and (displacement_quality in ("WEAK", "NONE") or not has_retest_or_confirmation or not has_displacement_or_close):
        choch_quality = "WEAK"
        if "choch_fake_failed_retest" not in choch_reasons:
            choch_reasons.append("choch_weak_no_followthrough")
    elif has_choch_like_evidence:
        choch_quality = "UNKNOWN"
    else:
        choch_quality = "NONE"
        choch_reasons.append("no_choch_evidence")

    if liquidity_sweep in ("SWEEP_LOW", "SWEEP_HIGH", "NONE"):
        liquidity_context = liquidity_sweep
    elif "EQUAL_HIGH_LOW" in tags:
        liquidity_context = "EQUAL_HIGH_LOW"
    else:
        liquidity_context = "UNKNOWN"

    if smc_zone in ("PREMIUM", "DISCOUNT", "EQUILIBRIUM"):
        premium_discount = smc_zone
    elif range_context == "RANGE_HIGH":
        premium_discount = "PREMIUM"
    elif range_context == "RANGE_LOW":
        premium_discount = "DISCOUNT"
    elif range_context == "MID":
        premium_discount = "EQUILIBRIUM"
    else:
        premium_discount = "UNKNOWN"

    poi_evidence = []
    signal_sources = (signal, ctx, smc_ctx, score_breakdown)

    def _source_truthy(*keys):
        for source in signal_sources:
            if not isinstance(source, dict):
                continue
            for key in keys:
                if _struct_truthy(source.get(key)):
                    return True
        return False

    def _source_text(*keys):
        for source in signal_sources:
            if not isinstance(source, dict):
                continue
            for key in keys:
                value = source.get(key)
                if value not in (None, ""):
                    return str(value).upper()
        return ""

    has_explicit_ob = (
        _source_truthy("explicit_ob", "has_ob", "has_order_block", "order_block", "ob_present")
        or any(tag.endswith(":OB") or tag in ("OB", "POI:OB", "ORDER_BLOCK") or "ORDER_BLOCK" in tag for tag in tags)
    )
    has_explicit_fvg = (
        _source_truthy("explicit_fvg", "has_fvg", "fvg_present")
        or any(tag == "FVG" or tag.endswith(":FVG") or tag.startswith("FVG:") or "FVG" in tag for tag in tags)
    )
    has_explicit_breaker = (
        _source_truthy("explicit_breaker", "has_breaker", "breaker_present")
        or any(tag == "BREAKER" or tag.endswith(":BREAKER") or tag.startswith("BREAKER:") or "BREAKER" in tag for tag in tags)
    )
    explicit_poi_text = _source_text("poi_type", "poi", "poi_source", "smc_poi", "zone_type")
    if not has_explicit_ob and explicit_poi_text in ("OB", "ORDER_BLOCK", "EXPLICIT_OB"):
        has_explicit_ob = True
    if not has_explicit_fvg and explicit_poi_text in ("FVG", "EXPLICIT_FVG"):
        has_explicit_fvg = True
    if not has_explicit_breaker and explicit_poi_text in ("BREAKER", "EXPLICIT_BREAKER"):
        has_explicit_breaker = True

    if has_explicit_ob:
        poi_type = "OB"
        poi_source = "explicit_ob"
        _struct_add_unique(poi_evidence, "explicit_ob")
    elif has_explicit_fvg:
        poi_type = "FVG"
        poi_source = "explicit_fvg"
        _struct_add_unique(poi_evidence, "explicit_fvg")
    elif has_explicit_breaker:
        poi_type = "BREAKER"
        poi_source = "explicit_breaker"
        _struct_add_unique(poi_evidence, "explicit_breaker")
    elif signal.get("in_retest_zone") is True or retest in ("RETEST", "OK", "VALID"):
        poi_type = "RETEST_ZONE"
        poi_source = "retest_zone"
        if signal.get("in_retest_zone") is True:
            _struct_add_unique(poi_evidence, "in_retest_zone")
    elif signal.get("near_ema") is True:
        poi_type = "NONE"
        poi_source = "ema_proxy"
        _struct_add_unique(poi_evidence, "near_ema")
    elif premium_discount != "UNKNOWN" or range_context not in ("", "UNKNOWN") or smc_zone not in ("", "UNKNOWN"):
        poi_type = "NONE"
        poi_source = "range_proxy"
    elif retest == "NONE":
        poi_type = "NONE"
        poi_source = "none"
    else:
        poi_type = "UNKNOWN"
        poi_source = "unknown"

    if retest in ("RETEST", "OK", "VALID"):
        poi_retest_quality = "VALID_RETEST"
        _struct_add_unique(poi_evidence, "retest_ok")
    elif retest == "VIOLATED" or "SOFT:RETEST_FAIL" in tags or reject_reason == "RETEST_ZONE_FAIL":
        poi_retest_quality = "FAILED_RETEST"
        if retest == "VIOLATED":
            _struct_add_unique(poi_evidence, "retest_violated")
        if "SOFT:RETEST_FAIL" in tags:
            _struct_add_unique(poi_evidence, "retest_violated")
        if reject_reason == "RETEST_ZONE_FAIL":
            _struct_add_unique(poi_evidence, "retest_zone_fail")
    elif reject_reason == "BOS_WAITING":
        poi_retest_quality = "WAITING_RETEST"
        _struct_add_unique(poi_evidence, "bos_waiting")
    elif reject_reason == "RETEST_TRIGGER_FAIL":
        poi_retest_quality = "FAILED_RETEST" if signal.get("in_retest_zone") is True or signal.get("near_ema") is True else "UNKNOWN"
        _struct_add_unique(poi_evidence, "retest_trigger_fail")
    elif reject_reason == "STALE_AFTER_BOS":
        poi_retest_quality = "FAILED_RETEST" if signal.get("in_retest_zone") is True or signal.get("near_ema") is True else "NO_RETEST"
        _struct_add_unique(poi_evidence, "stale_after_bos")
    elif retest == "NONE":
        poi_retest_quality = "NO_RETEST"
        _struct_add_unique(poi_evidence, "retest_none")
    else:
        poi_retest_quality = "UNKNOWN"

    if side == "LONG" and premium_discount == "DISCOUNT":
        entry_poi_alignment = "ALIGNED"
        poi_location_quality = "GOOD"
        _struct_add_unique(poi_evidence, "range_low_for_long")
    elif side == "LONG" and premium_discount == "PREMIUM":
        entry_poi_alignment = "CONFLICTING"
        poi_location_quality = "POOR"
        _struct_add_unique(poi_evidence, "range_high_for_long_bad")
    elif side == "SHORT" and premium_discount == "PREMIUM":
        entry_poi_alignment = "ALIGNED"
        poi_location_quality = "GOOD"
        _struct_add_unique(poi_evidence, "range_high_for_short")
    elif side == "SHORT" and premium_discount == "DISCOUNT":
        entry_poi_alignment = "CONFLICTING"
        poi_location_quality = "POOR"
        _struct_add_unique(poi_evidence, "range_low_for_short_bad")
    elif premium_discount == "EQUILIBRIUM":
        entry_poi_alignment = "NEUTRAL"
        poi_location_quality = "ACCEPTABLE"
    elif side in ("LONG", "SHORT"):
        entry_poi_alignment = "UNKNOWN"
        poi_location_quality = "UNKNOWN"
    else:
        entry_poi_alignment = "UNKNOWN"
        poi_location_quality = "UNKNOWN"

    if not has_explicit_ob and not has_explicit_fvg and not has_explicit_breaker:
        _struct_add_unique(poi_evidence, "no_explicit_poi")
    if not poi_evidence:
        _struct_add_unique(poi_evidence, "insufficient_poi_data")

    trigger_confirmed = signal.get("valid_long_trigger") is True or signal.get("valid_short_trigger") is True
    geometry_status = str(signal.get("geometry_status") or "").upper()
    if candle == "STRONG" or poi_retest_quality == "VALID_RETEST" or trigger_confirmed:
        pa_confirmation = "CONFIRMED"
    elif candle == "WEAK" or bos_quality == "WEAK":
        pa_confirmation = "WEAK"
    elif poi_retest_quality == "FAILED_RETEST" or bos_quality in ("TRAP", "NO_FOLLOWTHROUGH") or geometry_status in ("INVALID", "FAILED"):
        pa_confirmation = "FAILED"
    else:
        pa_confirmation = "UNKNOWN"
    if entry_type == SWING_SHADOW_ENTRY_TYPE:
        if reject_reason in ("RETEST_ZONE_FAIL", "RETEST_TRIGGER_FAIL"):
            pa_confirmation = "FAILED"
        elif reject_reason == "STALE_AFTER_BOS":
            pa_confirmation = "WEAK"
        elif reject_reason == "BOS_WAITING":
            pa_confirmation = "UNKNOWN"

    def _source_value(*keys):
        for source in signal_sources:
            if not isinstance(source, dict):
                continue
            for key in keys:
                value = source.get(key)
                if value not in (None, ""):
                    return value
        return None

    def _source_float(*keys):
        value = _source_value(*keys)
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    volume_evidence = []
    volume_state = str(_struct_first(
        _source_value("volume_state", "volume_confirmation"),
        default="",
    )).upper()
    volume_ok = _source_value("volume_ok")
    volume_confirm = _source_value("volume_confirm", "volume_confirmed")
    volume_spike = _source_value("volume_spike", "vol_spike")
    volume_follow_through = _source_value("volume_follow_through", "volume_followthrough")
    volume_ratio = _source_float("volume_ratio", "vol_ratio")
    volume_score = _source_float("volume_score", "volume")

    volume_tag_text = " ".join(tags)
    has_volume_tag = any(("VOL" in tag or "VOLUME" in tag) for tag in tags)
    if not volume_state:
        if any(tag in tags for tag in ("VOL_SUSTAIN", "VOLUME_SUSTAIN", "VOLUME:FOLLOW_THROUGH", "VOLUME:FOLLOWTHROUGH")):
            volume_state = "FOLLOW_THROUGH"
        elif any(tag in tags for tag in ("VOL_CLIMAX", "VOL_HIGH", "VOLUME:ABSORPTION", "ABSORPTION")):
            volume_state = "ABSORPTION"
        elif any(tag in tags for tag in ("VOL_WEAK", "VOLUME_WEAK", "VOLUME:WEAK")):
            volume_state = "WEAK"
        elif any(tag in tags for tag in ("COUNTER_VOL_SPIKE", "VOLUME:DIVERGENCE", "VOL_DIVERGENCE")):
            volume_state = "DIVERGENCE"
        elif any(tag in tags for tag in ("VOL_SPIKE", "VOLUME_SPIKE", "VOLUME:SPIKE", "VOLUME:EXPANSION")):
            volume_state = "SPIKE"

    has_volume_source = (
        volume_state not in ("", "UNKNOWN", "NONE")
        or volume_ok is not None
        or volume_confirm is not None
        or volume_spike is not None
        or volume_follow_through is not None
        or volume_ratio is not None
        or volume_score is not None
        or has_volume_tag
    )
    volume_source_active = True if has_volume_source else False
    has_high_volume = (
        _struct_truthy(volume_spike)
        or volume_state in ("EXPANSION", "SPIKE", "HIGH", "CLIMAX")
        or (volume_score is not None and volume_score > 0)
    )
    has_weak_volume = (
        volume_ok is False
        or volume_confirm is False
        or volume_state in ("WEAK", "LOW")
        or (volume_score is not None and volume_score < 0)
    )
    has_followthrough_volume = (
        _struct_truthy(volume_confirm)
        or _struct_truthy(volume_follow_through)
        or volume_state in ("FOLLOW_THROUGH", "FOLLOWTHROUGH", "SUSTAIN", "SUSTAINED")
    )
    has_absorption_volume = volume_state in ("ABSORPTION", "CLIMAX") or "ABSORPTION" in volume_tag_text
    has_divergence_volume = volume_state == "DIVERGENCE" or "DIVERGENCE" in volume_tag_text or "COUNTER_VOL_SPIKE" in tags
    has_price_push = has_close_through or displacement_quality in ("STRONG", "MODERATE")
    has_failed_followthrough = (
        pa_confirmation == "FAILED"
        or bos_quality in ("TRAP", "NO_FOLLOWTHROUGH")
        or explicit_no_followthrough
    )

    if not has_volume_source:
        volume_confirmation = "UNKNOWN"
        _struct_add_unique(volume_evidence, "no_volume_source")
    elif has_absorption_volume and has_failed_followthrough:
        volume_confirmation = "ABSORPTION"
        _struct_add_unique(volume_evidence, "volume_absorption")
    elif has_divergence_volume or (has_price_push and has_weak_volume):
        volume_confirmation = "DIVERGENCE"
        _struct_add_unique(volume_evidence, "volume_divergence")
    elif has_followthrough_volume and has_followthrough:
        volume_confirmation = "FOLLOW_THROUGH"
        _struct_add_unique(volume_evidence, "volume_followthrough")
    elif has_high_volume and has_price_push:
        volume_confirmation = "EXPANSION"
        _struct_add_unique(volume_evidence, "volume_expansion")
    elif has_weak_volume or volume_ok is False:
        volume_confirmation = "WEAK"
        _struct_add_unique(volume_evidence, "volume_weak")
    else:
        volume_confirmation = "UNKNOWN"
        _struct_add_unique(volume_evidence, "insufficient_volume_data")
    usable_volume_evidence = {
        "volume_absorption",
        "volume_divergence",
        "volume_followthrough",
        "volume_expansion",
        "volume_weak",
    }
    volume_data_usable = any(item in usable_volume_evidence for item in volume_evidence)
    if volume_source_active and not volume_data_usable:
        _struct_add_unique(volume_evidence, "volume_source_present_but_uninformative")

    poor_reasons = []
    good_reasons = []
    if external_structure == "CONFLICTING":
        poor_reasons.append("external_structure_conflict")
    if premium_discount == "PREMIUM" and side == "LONG":
        poor_reasons.append("long_in_premium")
    if premium_discount == "DISCOUNT" and side == "SHORT":
        poor_reasons.append("short_in_discount")
    if bos_quality in ("TRAP", "NO_FOLLOWTHROUGH", "SWEEP_ONLY"):
        poor_reasons.append(f"bos_{bos_quality.lower()}")
    if displacement_quality in ("NONE", "WEAK"):
        poor_reasons.append(f"displacement_{displacement_quality.lower()}")
    if poi_retest_quality == "FAILED_RETEST":
        poor_reasons.append("failed_retest")
    if entry_type == SWING_SHADOW_ENTRY_TYPE:
        if reject_reason == "RETEST_ZONE_FAIL":
            poor_reasons.append("retest_zone_not_reached")
        elif reject_reason == "RETEST_TRIGGER_FAIL":
            poor_reasons.append("retest_trigger_failed")
        elif reject_reason == "STALE_AFTER_BOS":
            poor_reasons.append("stale_after_bos")
    if premium_discount == "DISCOUNT" and side == "LONG":
        good_reasons.append("long_from_discount")
    if premium_discount == "PREMIUM" and side == "SHORT":
        good_reasons.append("short_from_premium")
    if external_structure == "ALIGNED":
        good_reasons.append("external_structure_aligned")
    if bos_quality == "STRONG":
        good_reasons.append("strong_bos")
    if poi_retest_quality == "VALID_RETEST":
        good_reasons.append("valid_retest")

    if not side or dow_trend_context == "UNKNOWN":
        trade_location_quality = "UNKNOWN"
    elif poor_reasons:
        trade_location_quality = "POOR"
    elif len(good_reasons) >= 2:
        trade_location_quality = "GOOD"
    else:
        trade_location_quality = "ACCEPTABLE"

    if trade_location_quality == "GOOD" and pa_confirmation == "CONFIRMED":
        structural_decision_shadow = "QUALIFIED"
        structural_score_modifier_shadow = 0.5
    elif trade_location_quality == "POOR":
        structural_decision_shadow = "WOULD_DOWNRANK"
        structural_score_modifier_shadow = -0.5
    elif trade_location_quality in ("ACCEPTABLE",):
        structural_decision_shadow = "NEUTRAL"
        structural_score_modifier_shadow = 0.0
    else:
        structural_decision_shadow = "UNKNOWN"
        structural_score_modifier_shadow = 0.0

    structural_score_simulation = _struct_score_simulation(
        signal.get("score_v2"),
        structural_score_modifier_shadow,
        threshold_reference=signal.get("structural_threshold_reference"),
        pass_threshold=signal.get("structural_pass_threshold"),
        shadow_pass_threshold=signal.get("structural_shadow_pass_threshold"),
    )

    structural_reasons = good_reasons + poor_reasons
    for choch_reason in choch_reasons:
        if choch_reason not in structural_reasons:
            structural_reasons.append(choch_reason)
    if not structural_reasons:
        structural_reasons = ["insufficient_structural_evidence"]

    return _json_safe_shadow_value({
        "dow_trend_context": dow_trend_context,
        "dow_phase": dow_phase,
        "external_structure": external_structure,
        "internal_structure": internal_structure,
        "bos_quality": bos_quality,
        "bos_evidence": bos_evidence,
        "choch_quality": choch_quality,
        "choch_evidence": choch_evidence,
        "displacement_quality": displacement_quality,
        "liquidity_context": liquidity_context,
        "poi_type": poi_type,
        "poi_source": poi_source,
        "poi_retest_quality": poi_retest_quality,
        "entry_poi_alignment": entry_poi_alignment,
        "poi_location_quality": poi_location_quality,
        "poi_evidence": poi_evidence,
        "premium_discount": premium_discount,
        "pa_confirmation": pa_confirmation,
        "volume_confirmation": volume_confirmation,
        "volume_source_active": volume_source_active,
        "volume_data_usable": volume_data_usable,
        "volume_evidence": volume_evidence,
        "trade_location_quality": trade_location_quality,
        "structural_decision_shadow": structural_decision_shadow,
        "structural_score_modifier_shadow": structural_score_modifier_shadow,
        "score_v2_current": structural_score_simulation["score_v2_current"],
        "score_v2_structural_shadow": structural_score_simulation["score_v2_structural_shadow"],
        "score_delta_value": structural_score_simulation["score_delta_value"],
        "score_delta_direction": structural_score_simulation["score_delta_direction"],
        "current_threshold_reference": structural_score_simulation["current_threshold_reference"],
        "current_pass_threshold": structural_score_simulation["current_pass_threshold"],
        "structural_shadow_pass_threshold": structural_score_simulation["structural_shadow_pass_threshold"],
        "structural_decision_delta": structural_score_simulation["structural_decision_delta"],
        "structural_delta_reason": structural_score_simulation["structural_delta_reason"],
        "structural_reasons": structural_reasons,
    })


def _attach_structural_context(signal, breakdown=None, ctx=None, smc_ctx=None):
    structural_context = _safe_structural_context(
        signal=signal,
        ctx=ctx,
        smc_ctx=smc_ctx,
        reason=(signal or {}).get("reason"),
    )
    if isinstance(signal, dict):
        signal["structural_context"] = structural_context
    if isinstance(breakdown, dict):
        logged = dict(breakdown)
    else:
        logged = {}
    logged["structural_context"] = structural_context
    return logged


def _reason_tag_value_ci(reason, prefix):
    prefix_u = str(prefix or "").upper()
    for item in reason or []:
        text = str(item or "")
        if text.upper().startswith(prefix_u):
            return text.split(":", 1)[1] if ":" in text else text
    return ""


def _with_accepted_signal_context(breakdown, symbol, signal):
    if isinstance(breakdown, dict):
        logged = dict(breakdown)
    else:
        logged = {}
    reason = list(signal.get("reason") or [])
    logged["accepted_signal_context"] = _json_safe_shadow_value({
        "symbol": symbol,
        "side": signal.get("side", ""),
        "entry_type": signal.get("entry_type", ""),
        "entry": signal.get("entry", ""),
        "sl": signal.get("sl", ""),
        "tp": signal.get("tp", ""),
        "reason": reason,
        "phase": signal.get("phase") or _reason_tag_value_ci(reason, "PHASE:"),
        "bos_type": _reason_tag_value_ci(reason, "BOS:") or signal.get("bos_type", ""),
        "signal_created_ts": signal.get("signal_created_ts", ""),
    })
    return logged


# =====================================================================
# SCORING ENGINE
# =====================================================================

def compute_entry_score(ctx, rr, side_h1):
    """
    Tính score từ context + RR.
    Trả về score: int
    """
    score = 0

    # Market state
    if ctx["mkt_state"] in ("TREND", "ACCUMULATION"):
        score += 2
    elif ctx["mkt_state"] == "EXHAUSTION":
        score -= 1
    elif ctx["mkt_state"] == "DEAD":
        score -= 2

    # Phase
    if ctx["phase"] == "PRE_BREAK":
        score += 2
    elif ctx["phase"] == "BREAKOUT_STRONG":
        score += 2
    elif ctx["phase"] == "BREAKOUT_WEAK":
        score += 1

    # Impulse
    if ctx["impulse"]:
        score += 2

    # EMA alignment (không quá xa, không quá gần)
    if ctx["dist_ema"] < 0.01:
        score += 2

    # RR
    if rr >= 1.5:
        score += 2
    elif rr >= 1.2:
        score += 1

    return score


# =====================================================================
# TRADE BUILDER
# =====================================================================

def build_trade(symbol, signal, ctx, early_size_mult=1.0):
    """
    Nhận signal dict, trả về trade dict đầy đủ.
    signal keys: side, entry, sl, tp, reason, entry_type
    """
    side  = signal["side"]
    entry = signal["entry"]
    sl    = signal["sl"]
    tp    = signal["tp"]

    if sl is None or tp is None or math.isnan(sl) or math.isnan(tp):
        print(f"[REJECT NaN] {symbol} sl={sl} tp={tp}")
        return None

    risk = abs(entry - sl)

    if entry > 0 and risk / entry < 0.001:
        rr_display = round(abs(tp - entry) / risk, 2) if risk > 0 else float("inf")
        print(
            f"[INVALID TRADE] symbol={symbol}"
            f" entry={entry}"
            f" sl={sl}"
            f" risk%={round(risk / entry * 100, 4)}"
            f" rr={rr_display}"
        )
        return None

    rr   = abs(tp - entry) / risk if risk > 0 else 0

    score = signal.get("_score")

    if score is None:
        print(f"[REJECT SCORE] {symbol} score={score} type={signal['entry_type']}")
        return None

    size_mult    = early_size_mult
    risk_pct     = compute_risk_pct(signal["entry_type"], score)
    risk_percent = (risk_pct / 100) * size_mult
    if DEBUG:
        print(f"[RISK] {symbol} score={round(score, 2)} entry_type={signal['entry_type']} risk={risk_pct}")

    reason = signal["reason"].copy()
    reason.append(f"Score:{score}")
    reason.append(f"State:{ctx['mkt_state']}")

    _now_ts = time.time()
    t = {
        "symbol":       symbol,
        "side":         side,
        "entry":        entry,
        "entry_real":   entry,
        "sl":           sl,
        "sl_real":      sl,
        "sl_init":      sl,
        "tp":           tp,
        "rr":           rr,
        "risk_percent": risk_percent,
        "score":        score,
        "entry_type":   signal["entry_type"],
        "status":       "OPEN",
        "reason":       reason,
        "tp_mode":      "SOFT" if rr >= 1.8 else "HARD",
        "entry_time":   _now_ts,
        "signal_created_ts": _now_ts,  # immutable — set once at build_trade, never overwritten
        "max_profit_r": 0,
        "entry_size":   size_mult,
        "id":           int(_now_ts * 1000),
    }
    if signal.get("cont_score") is not None:
        t["cont_score"] = signal["cont_score"]
    if signal.get("exhaustion_cls") is not None:
        t["exhaustion_cls"]   = signal["exhaustion_cls"]
    if signal.get("exhaustion_score") is not None:
        t["exhaustion_score"] = signal["exhaustion_score"]
    for key in (
        "smc_zone",
        "liquidity_sweep",
        "bos_confirmation",
        "smc_bias",
        "range_context",
        "invalid_context",
        "score_breakdown",
        "breakdown",
        "phase",
        "confirm_entry_acceptance_context",
    ):
        if key in signal and signal.get(key) is not None:
            t[key] = _json_safe_shadow_value(signal.get(key))
    return t


# =====================================================================
# EARLY_CONT: DYNAMIC THRESHOLD
# =====================================================================

def _get_early_continuation_threshold(regime, bos_n):
    """
    Dynamic continuation_confidence threshold for EARLY_CONT.
    Returns None to hard-reject; float to require minimum cont_score.
    """
    base = {
        "STRONG_TREND": 7.8,
        "TREND":        8.5,
        "NEUTRAL":      9.2,
        "ACCUMULATION": None,
        "EXHAUSTION":   None,
        "DEAD":         None,
    }.get(regime)

    if base is None:
        return None

    if bos_n >= 5:
        base += 1.0
    elif bos_n >= 3:
        base += 0.5

    return base


# =====================================================================
# EARLY PIPELINE
# =====================================================================

def early_pipeline(symbol, df, cls, df15, df1h, df4h=None, df1d=None):
    reason = []
    _set_shadow_score_context(symbol, EARLY_CONT_ENTRY_TYPE)

    # ── H1 trend: hard gate for continuation (no trend = no basis)
    h1_trend = trend_h1(df1h)
    side_h1  = _fallback_side(df15, h1_trend)
    if not h1_trend:
        return None

    # ── Market context
    ctx         = build_market_context(df15, df1h)
    mkt_state   = ctx["mkt_state"]
    mkt_metrics = ctx["mkt_metrics"]

    log_state(symbol, mkt_state, mkt_metrics.get("impulse", 0), mkt_metrics.get("vol_ratio", 1))

    # ── Regime gate: continuation invalid in non-trending markets
    if mkt_state in ("ACCUMULATION", "DEAD", "EXHAUSTION"):
        stats["early_block_state"] += 1
        return None

    # ── BOS required: no structural break = no continuation basis
    bos_type_e, bos_level_e = _detect_bos_m15(df15, side_h1)
    if bos_type_e is None:
        return None

    bos_n = bos_count(df15, side_h1)
    reason.append(f"BOS:{bos_type_e}")

    # ── Continuation confidence scoring
    cont_score, cont_factors = calc_continuation_confidence(df15, side_h1, bos_n, h1_trend)
    for f in cont_factors:
        reason.append(f"CC:{f}")

    if DEBUG:
        print(f"[EARLY_CONT] {symbol} cont_score={cont_score} bos={bos_type_e} bos_n={bos_n}")

    stats["early_detected"] += 1

    # ── Determine regime (STRONG_TREND = TREND + impulse + BREAKOUT_STRONG phase)
    is_strong_trend = (mkt_state == "TREND" and ctx["impulse"] and ctx["phase"] == "BREAKOUT_STRONG")
    regime = "STRONG_TREND" if is_strong_trend else mkt_state

    # ── Dynamic continuation threshold (regime-aware + BOS maturity)
    cont_threshold = _get_early_continuation_threshold(regime, bos_n)
    if cont_threshold is None:
        return None

    if cont_score < cont_threshold:
        print(f"[EARLY_CONT REJECT] {symbol} cont_score={cont_score} threshold={cont_threshold} regime={regime}")
        _log_early_shadow_candidate(
            symbol,
            "CONT_SCORE_FAIL",
            side=side_h1,
            ctx=dict(ctx, regime=regime),
            reason=reason,
            h1_trend=h1_trend,
            bos_type=bos_type_e,
            bos_level=bos_level_e,
            bos_n=bos_n,
            cont_score=cont_score,
            cont_threshold=cont_threshold,
            cont_factors=cont_factors,
        )
        log_confirm_reject(symbol, "CONT_SCORE_FAIL", entry_type=EARLY_CONT_ENTRY_TYPE)
        return None

    # ── Entry / SL / TP
    signal_entry  = df["close"].iloc[-2]
    current_price = df15["close"].iloc[-1]
    drift = abs(current_price - signal_entry) / signal_entry
    if DEBUG:
        print(f"[ENTRY FIX] {symbol} signal_entry={round(signal_entry, 6)} actual_entry={round(current_price, 6)} drift={round(drift * 100, 4)}%")
    entry = current_price

    if is_duplicate_zone(symbol, side_h1, entry):
        _log_early_shadow_candidate(
            symbol,
            "DUPLICATE_ZONE",
            side=side_h1,
            ctx=dict(ctx, regime=regime),
            reason=reason,
            h1_trend=h1_trend,
            bos_type=bos_type_e,
            bos_level=bos_level_e,
            bos_n=bos_n,
            cont_score=cont_score,
            cont_threshold=cont_threshold,
            cont_factors=cont_factors,
            entry=entry,
            signal_entry=signal_entry,
        )
        return None

    atr = (df15["high"] - df15["low"]).rolling(14).mean().iloc[-2]
    if math.isnan(atr) or atr <= 0:
        _log_early_shadow_candidate(
            symbol,
            "ATR_INVALID",
            side=side_h1,
            ctx=dict(ctx, regime=regime),
            reason=reason,
            h1_trend=h1_trend,
            bos_type=bos_type_e,
            bos_level=bos_level_e,
            bos_n=bos_n,
            cont_score=cont_score,
            cont_threshold=cont_threshold,
            cont_factors=cont_factors,
            entry=entry,
            signal_entry=signal_entry,
        )
        return None

    if side_h1 == "LONG":
        sl = df15["low"].iloc[-10:-2].min() - atr * 0.5
        tp = entry + atr * 4.0
    else:
        sl = df15["high"].iloc[-10:-2].max() + atr * 0.5
        tp = entry - atr * 4.0

    risk = abs(entry - sl)
    rr   = abs(tp - entry) / risk if risk > 0 else 0

    if rr < 1.0:
        print(f"[EARLY_CONT REJECT] {symbol} | RR={rr:.2f} < 1.0")
        _log_early_shadow_candidate(
            symbol,
            "RR_FAIL",
            side=side_h1,
            ctx=dict(ctx, regime=regime),
            reason=reason,
            h1_trend=h1_trend,
            bos_type=bos_type_e,
            bos_level=bos_level_e,
            bos_n=bos_n,
            cont_score=cont_score,
            cont_threshold=cont_threshold,
            cont_factors=cont_factors,
            entry=entry,
            signal_entry=signal_entry,
            sl=sl,
            tp=tp,
            rr=rr,
        )
        return None

    if rr < 1.5:
        reason.append("Soft:RR_FAIL")

    # ── Size multiplier
    early_size_mult = 1.0
    if mkt_state == "NEUTRAL":
        early_size_mult = 0.7
    elif mkt_state == "TREND":
        early_size_mult = 1.2
        if not (mkt_metrics.get("trend_up") or mkt_metrics.get("trend_down")):
            early_size_mult *= 0.7
            reason.append("FakeTrend")

    reason.append(f"PHASE:{ctx['phase']}")
    reason.append(f"MKT:{mkt_state}")
    reason.append(f"RR:{round(rr, 2)}")
    reason.append(f"CC:{cont_score}")
    reason.append(f"regime:{regime}")

    if not _passes_pre_score_tag_gate(symbol, EARLY_CONT_ENTRY_TYPE, reason):
        return None
    if not _passes_market_state_gate(symbol, EARLY_CONT_ENTRY_TYPE, mkt_state, reason):
        return None

    soft_flag_count = sum(1 for r in reason if str(r).startswith("Soft:"))
    if soft_flag_count >= 2:
        print(f"[BLOCK] EARLY_CONT_SOFT_CONFLICT {symbol} soft_flags={soft_flag_count}")
        _log_early_shadow_candidate(
            symbol,
            "SOFT_CONFLICT",
            side=side_h1,
            ctx=dict(ctx, regime=regime),
            reason=reason,
            h1_trend=h1_trend,
            bos_type=bos_type_e,
            bos_level=bos_level_e,
            bos_n=bos_n,
            cont_score=cont_score,
            cont_threshold=cont_threshold,
            cont_factors=cont_factors,
            entry=entry,
            signal_entry=signal_entry,
            sl=sl,
            tp=tp,
            rr=rr,
        )
        log_confirm_reject(symbol, "SOFT_CONFLICT", entry_type=EARLY_CONT_ENTRY_TYPE)
        return None

    candle_strength      = _classify_candle_confirm(df15)
    exhaustion_cls_early, exhaustion_score_early, _ = compute_exhaustion(df15, side_h1, bos_n, symbol)
    _dist_ev = abs(entry - bos_level_e) / max(bos_level_e, 1e-9)
    _early_ok, _meta_ev = validate_early({"dist": _dist_ev, "pp": 0.0, "exhaustion": exhaustion_cls_early})
    if not _early_ok:
        _ev_block = _meta_ev.get("block", "validate_early")
        _early_reject_reason = f"EARLY_VALIDATE_{_ev_block.upper()}"
        _log_early_shadow_candidate(
            symbol,
            _early_reject_reason,
            side=side_h1,
            ctx=dict(ctx, regime=regime),
            reason=reason,
            h1_trend=h1_trend,
            bos_type=bos_type_e,
            bos_level=bos_level_e,
            bos_n=bos_n,
            cont_score=cont_score,
            cont_threshold=cont_threshold,
            cont_factors=cont_factors,
            entry=entry,
            signal_entry=signal_entry,
            sl=sl,
            tp=tp,
            rr=rr,
            exhaustion_cls=exhaustion_cls_early,
            exhaustion_score=exhaustion_score_early,
        )
        log_confirm_reject(symbol, _early_reject_reason, entry_type=EARLY_CONT_ENTRY_TYPE)
        if DEBUG:
            print(f"[EARLY REJECT] {symbol} | validate_early={_ev_block} exhaustion={exhaustion_cls_early}")
        return None
    e_score, score_breakdown = _compute_unified_score(
        ctx, bos_type_e, bos_level_e, False, "NONE", candle_strength, rr,
        entry, signal_entry, side_h1, EARLY_LEGACY_SCORE_TYPE,
        exhaustion_cls_early, "NONE", "NONE", df15, df1h, reason
    )
    score_v2, breakdown = _compute_shadow_score(
        symbol, EARLY_CONT_ENTRY_TYPE, df15, df15, bos_type_e, h1_trend, side_h1, rr, ctx["market_state"], score_old=e_score
    )

    if not _passes_entry_quality_gate(symbol, EARLY_CONT_ENTRY_TYPE, e_score, reason):
        _log_early_shadow_candidate(
            symbol,
            "ENTRY_QUALITY_FAIL",
            side=side_h1,
            ctx=dict(ctx, regime=regime),
            reason=reason,
            h1_trend=h1_trend,
            bos_type=bos_type_e,
            bos_level=bos_level_e,
            bos_n=bos_n,
            cont_score=cont_score,
            cont_threshold=cont_threshold,
            cont_factors=cont_factors,
            entry=entry,
            signal_entry=signal_entry,
            sl=sl,
            tp=tp,
            rr=rr,
            exhaustion_cls=exhaustion_cls_early,
            exhaustion_score=exhaustion_score_early,
            score=e_score,
            score_breakdown=score_breakdown,
        )
        return None

    cont_meta = {
        "cont_score":    cont_score,
        "cont_factors":  cont_factors,
        "regime":        regime,
        "bos_type":      bos_type_e,
        "bos_n":         bos_n,
    }
    log_scan_early(symbol, side_h1, cont_meta)

    smc_ctx = compute_smc_context(df15, df1h, df4h, side_h1, bos_type_e, bos_level_e, ctx)
    if isinstance(score_breakdown, dict):
        score_breakdown["smc"] = smc_ctx
    breakdown = _with_smc_breakdown(breakdown, smc_ctx)

    signal = {
        "side":            side_h1,
        "entry":           entry,
        "sl":              sl,
        "tp":              tp,
        "reason":          reason,
        "entry_type":      EARLY_CONT_ENTRY_TYPE,
        "_size_mult":      early_size_mult,
        "_ctx":            ctx,
        "_score":          e_score,
        "score_v2":        score_v2,
        "score_breakdown":  score_breakdown,
        "cont_score":       cont_score,
        "exhaustion_cls":   exhaustion_cls_early,
        "exhaustion_score": exhaustion_score_early,
        "smc_zone":         smc_ctx["smc_zone"],
        "liquidity_sweep":  smc_ctx["liquidity_sweep"],
        "bos_confirmation": smc_ctx["bos_confirmation"],
        "smc_bias":         smc_ctx["smc_bias"],
        "range_context":    smc_ctx["range_context"],
        "invalid_context":  smc_ctx["invalid_context"],
    }
    breakdown = _attach_structural_context(signal, breakdown, ctx=ctx, smc_ctx=smc_ctx)
    breakdown = _with_accepted_signal_context(breakdown, symbol, signal)
    _log_score_shadow(symbol, "SIGNAL", "ACCEPT", EARLY_CONT_ENTRY_TYPE, score_old=e_score, score_v2=score_v2, breakdown=breakdown)
    early_cont_outcome_source_ts = _candle_open_ts(df15.iloc[-2]) or time.time()
    early_cont_outcome_payload = dict(signal)
    early_cont_outcome_payload.update({
        "symbol": symbol,
        "entry_type": EARLY_CONT_ENTRY_TYPE,
        "score": e_score,
        "score_v2": score_v2,
        "signal_created_ts": early_cont_outcome_source_ts,
        "structural_context": _dict_copy(breakdown.get("structural_context")),
    })
    _register_early_cont_shadow_outcome(early_cont_outcome_payload)
    return signal


def _soft_filter(symbol, reason):
    if DEBUG:
        print(f"[SOFT FILTER] {symbol} reason={reason}")


def _fallback_side(df15, preferred=None):
    if preferred in ("LONG", "SHORT"):
        return preferred
    m15_side = _ema_trend_m15(df15)
    if m15_side in ("LONG", "SHORT"):
        return m15_side
    candle = df15.iloc[-2]
    return "LONG" if candle["close"] >= candle["open"] else "SHORT"


# =====================================================================
# CONFIRM PIPELINE
# =====================================================================

def _detect_bos_m15(df15, side):
    """
    BOS detection đơn giản cho confirm pipeline.
    Trả về (bos_type: str | None, bos_level: float)
    """
    prev_high = df15["high"].iloc[-20:-3].max()
    prev_low  = df15["low"].iloc[-20:-3].min()
    last      = df15.iloc[-3]
    close     = last["close"]
    high      = last["high"]
    low       = last["low"]
    open_     = last["open"]

    body_ratio = abs(close - open_) / (high - low + 1e-9)
    avg_vol    = df15["volume"].rolling(20).mean().iloc[-3]
    vol_ok     = avg_vol > 0 and df15["volume"].iloc[-3] >= avg_vol * 0.8

    if side == "LONG":
        level    = prev_high
        dist     = abs(close - level) / max(level, 1e-9)
        move_pct = (close - level) / max(level, 1e-9)
        if close > level:
            if move_pct > 0.005 and body_ratio >= 0.5 and vol_ok:
                bos_type = "STRONG"
            elif move_pct > 0.002 and body_ratio >= 0.3:
                bos_type = "CONFIRM"
            else:
                bos_type = "WEAK"
        elif high > level and close < level:
            bos_type = "TRAP"
        elif dist <= 0.005:
            bos_type = "NEAR"
        else:
            bos_type = None
        if bos_type == "CONFIRM" and move_pct < 0.003:
            bos_type = "NEAR"
    else:
        level    = prev_low
        dist     = abs(close - level) / max(level, 1e-9)
        move_pct = (level - close) / max(level, 1e-9)
        if close < level:
            if move_pct > 0.005 and body_ratio >= 0.5 and vol_ok:
                bos_type = "STRONG"
            elif move_pct > 0.002 and body_ratio >= 0.3:
                bos_type = "CONFIRM"
            else:
                bos_type = "WEAK"
        elif low < level and close > level:
            bos_type = "TRAP"
        elif dist <= 0.005:
            bos_type = "NEAR"
        else:
            bos_type = None
        if bos_type == "CONFIRM" and move_pct < 0.003:
            bos_type = "NEAR"

    return bos_type, level


def _detect_retest(df15, bos_level, side, tol=0.005):
    """Kiểm tra retest sau BOS."""
    price = df15["close"].iloc[-2]
    prev_high = df15["high"].iloc[-20:-2].max()
    prev_low  = df15["low"].iloc[-20:-2].min()

    if abs(price - bos_level) / max(bos_level, 1e-9) > tol:
        return False, "NONE"

    if side == "LONG" and price > prev_high:
        return False, "VIOLATED"
    if side == "SHORT" and price < prev_low:
        return False, "VIOLATED"

    return True, "RETEST"


def _classify_candle_confirm(df15):
    """
    Phân loại nến xác nhận.
    Trả về: "STRONG" | "NORMAL" | "WEAK"
    """
    candle    = df15.iloc[-2]
    body      = abs(candle["close"] - candle["open"])
    full_range = candle["high"] - candle["low"]

    if full_range < 1e-9:
        return "WEAK"

    body_ratio = body / full_range

    if body_ratio >= 0.5:
        return "STRONG"
    elif body_ratio >= 0.3:
        return "NORMAL"
    return "WEAK"


def _compute_confirm_score(ctx, bos_type, retest_ok, candle_strength, rr):
    """
    Score riêng cho confirm pipeline — ngưỡng cao hơn EARLY.
    Trả về score: int
    """
    score = 0

    # Market context (collapsed from mkt_state + phase + impulse triple-count)
    mkt_context_score = 0
    if ctx["mkt_state"] == "TREND":
        if ctx["impulse"]:
            mkt_context_score = 3
        else:
            mkt_context_score = 2
    elif ctx["mkt_state"] == "ACCUMULATION":
        mkt_context_score = 1
    elif ctx["mkt_state"] in ("DEAD", "EXHAUSTION"):
        mkt_context_score = -2
    score += mkt_context_score

    # EMA distance
    if ctx["dist_ema"] < 0.01:
        score += 2

    # BOS
    if bos_type == "STRONG":
        score += 2
    elif bos_type == "CONFIRM":
        score += 1
    elif bos_type == "NEAR":
        score += 0
    elif bos_type == "TRAP":
        score -= 1
    elif bos_type == "WEAK":
        score -= 1

    # Retest
    if retest_ok:
        score += 1.5
    else:
        score -= 1

    # Candle
    if candle_strength == "STRONG":
        score += 1.5
    elif candle_strength == "NORMAL":
        score += 1
    elif candle_strength == "WEAK":
        score -= 1

    # RR
    if rr >= 1.5:
        score += 1.5
    elif rr >= 1.2:
        score += 1
    else:
        score -= 1

    return score


def _compute_structure_score(entry, bos_level):
    if bos_level <= 0:
        return 0
    dist = abs(entry - bos_level) / bos_level
    if dist <= 0.0015:
        return 2
    elif dist <= 0.003:
        return 1
    elif dist <= 0.005:
        return -2
    else:
        return -3


def _compute_drift_score(current_price, signal_price):
    if signal_price <= 0:
        return 0
    drift = abs(current_price - signal_price) / signal_price
    if drift <= 0.001:     # ≤ 0.1%
        return +2
    elif drift <= 0.004:   # ≤ 0.4%
        return +1
    elif drift <= 0.008:   # ≤ 0.8%
        return -1
    elif drift <= 0.013:   # ≤ 1.3%
        return -2
    else:
        return -3


def _compute_unified_score(ctx, bos_type, bos_level, retest_ok, retest_str, candle_strength, rr,
                            entry, signal_entry, side, entry_type,
                            exhaustion_cls, wyckoff, wyckoff_name, df_vol, df1h, reason_list):
    base_score = _compute_confirm_score(ctx, bos_type, retest_ok, candle_strength, rr)

    structure_score = _compute_structure_score(entry, bos_level)

    if entry_type and entry_type.startswith("REVERSAL"):
        _raw_drift = abs(entry - signal_entry) / signal_entry if signal_entry > 0 else 0
        if _raw_drift <= 0.0015:
            drift_score = +2
        elif _raw_drift <= 0.006:
            drift_score = +1
        elif _raw_drift <= 0.012:
            drift_score = -1
        elif _raw_drift <= 0.02:
            drift_score = -2
        else:
            drift_score = -3
    else:
        drift_score = _compute_drift_score(entry, signal_entry)

    exhaustion_score = 0
    if entry_type == "REVERSAL_CONFIRM":
        if exhaustion_cls in ("EXHAUSTED", "COLLAPSING"):
            exhaustion_score = 2.0
        elif exhaustion_cls == "HEALTHY":
            exhaustion_score = -1.5
    else:
        if exhaustion_cls == "COLLAPSING":
            exhaustion_score = -5
        elif exhaustion_cls == "EXHAUSTED":
            exhaustion_score = -3
        elif exhaustion_cls == "EXTENDED":
            exhaustion_score = -1

    volume_score = 0
    avg_vol = df_vol["volume"].rolling(20).mean().iloc[-2]
    if avg_vol > 0:
        vol_ratio = df_vol["volume"].iloc[-2] / avg_vol
        if vol_ratio >= 1.5:
            volume_score = 1
        elif vol_ratio < 0.8:
            volume_score = -1

    wyckoff_score = 0
    if wyckoff == "STRONG":
        wyckoff_score = 2
    elif wyckoff == "WEAK":
        wyckoff_score = -1

    h1_align_score = 0
    h1_trend_val = trend_h1(df1h)
    if h1_trend_val and side:
        if h1_trend_val == side:
            h1_align_score = 1
        else:
            h1_align_score = -1

    reversal_score = 0
    if entry_type and entry_type.startswith("REVERSAL"):
        reason_str = ",".join(reason_list) if isinstance(reason_list, list) else str(reason_list)
        reversal_score = 0
        if wyckoff == "WEAK":
            reversal_score -= 2
        if retest_str in ("NONE", "VIOLATED") or not retest_ok:
            reversal_score -= 2
        if bos_type == "WEAK":
            reversal_score -= 2
        if "MKT:TREND" in reason_str:
            reversal_score -= 1
        if wyckoff_name in ("SPRING", "UPTHRUST") or bos_type == "TRAP":
            reversal_score += 2

    soft_score = 0
    _soft_reason_str = ",".join(reason_list) if isinstance(reason_list, list) else str(reason_list)
    if "Soft:DEAD_MARKET" in _soft_reason_str:
        soft_score -= 2
    if "Soft:TREND_FAIL" in _soft_reason_str or "Soft:TrendFail" in _soft_reason_str:
        soft_score -= 1
    if "Soft:EMA_TOO_FAR" in _soft_reason_str:
        soft_score -= 1
    if "Soft:EMA_TOO_CLOSE" in _soft_reason_str:
        soft_score -= 0.5
    if "Soft:BOS_TRAP" in _soft_reason_str:
        soft_score -= 1
    if "Soft:NO_REVERSAL" in _soft_reason_str:
        soft_score -= 1
    if "Soft:RETEST_WEAK" in _soft_reason_str:
        soft_score -= 0.5

    combo_penalty = 0

    if (entry_type in ("CONFIRM",) or (entry_type and entry_type.startswith("SWING"))) and (bos_type in (None, "WEAK")) and (not retest_ok):
        combo_penalty -= 2
    elif (bos_type in (None, "WEAK")) and (candle_strength == "WEAK") and (rr < 1.5):
        combo_penalty -= 2
    elif (exhaustion_cls in ("EXTENDED", "EXHAUSTED", "COLLAPSING")) and (bos_type in (None, "WEAK")):
        combo_penalty -= 2
    elif (rr < 1.5) and (not retest_ok) and (candle_strength == "WEAK"):
        combo_penalty -= 2
    elif entry_type and entry_type.startswith("REVERSAL") and (wyckoff in (None, "WEAK")) and (bos_type in (None, "WEAK")) and (not retest_ok):
        combo_penalty -= 2
    elif (not retest_ok) and (candle_strength == "WEAK"):
        combo_penalty -= 1
    elif (exhaustion_cls in ("EXTENDED", "EXHAUSTED", "COLLAPSING")) and (candle_strength == "WEAK"):
        combo_penalty -= 1
    elif (bos_type == "TRAP") and (not retest_ok):
        combo_penalty -= 1

    combo_penalty = max(combo_penalty, -4)

    final_score = (base_score + structure_score + drift_score +
                   exhaustion_score + volume_score + wyckoff_score +
                   h1_align_score + reversal_score + soft_score + combo_penalty)

    recent_high = df_vol["high"].iloc[-20:-2].max()
    recent_low = df_vol["low"].iloc[-20:-2].min()
    if side == "SHORT":
        distance = (recent_high - entry) / max(recent_high, 1e-9)
        if distance < 0.01:
            loc_score = +1
        elif distance > 0.03:
            loc_score = -1
        else:
            loc_score = 0
    elif side == "LONG":
        distance = (entry - recent_low) / max(recent_low, 1e-9)
        if distance < 0.01:
            loc_score = +1
        elif distance > 0.03:
            loc_score = -1
        else:
            loc_score = 0
    else:
        loc_score = 0

    move = abs(df_vol["close"].iloc[-2] - df_vol["close"].iloc[-6]) / max(df_vol["close"].iloc[-6], 1e-9)
    if move > 0.03:
        timing_score = -1
    else:
        timing_score = 0

    if ctx["mkt_state"] == "TREND":
        ctx_score = +0.5
    elif ctx["mkt_state"] == "EXHAUSTION":
        ctx_score = -0.5
    else:
        ctx_score = 0

    position_score = loc_score + timing_score + ctx_score
    position_score = max(min(position_score, 1), -1)

    if position_score > 0:
        final_score *= 1.05
    elif position_score < 0:
        final_score *= 0.85

    if side == "LONG" and h1_trend_val == "SHORT":
        final_score *= 0.85
    elif side == "SHORT" and h1_trend_val == "LONG":
        final_score *= 0.85

    if entry_type and entry_type.startswith("REVERSAL"):
        final_score += 1.0
    if entry_type == "EARLY_V2":
        final_score += 0.5

    score_breakdown = {
        "base":       base_score,
        "structure":  structure_score,
        "drift":      drift_score,
        "exhaustion": exhaustion_score,
        "volume":     volume_score,
        "wyckoff":    wyckoff_score,
        "h1_align":   h1_align_score,
        "reversal":   reversal_score,
        "soft":       soft_score,
        "combo":      combo_penalty,
    }

    return final_score, score_breakdown


def confirm_pipeline(symbol, df, cls, df15, df1h, df4h=None, df1d=None):
    """
    Confirm pipeline với BOS + retest + candle confirmation.
    Trả về signal dict hoặc None.
    """
    reason = []
    _set_shadow_score_context(symbol, "CONFIRM")

    ctx = build_market_context(df15, df1h)
    mkt_state    = ctx["mkt_state"]
    mkt_metrics  = ctx["mkt_metrics"]
    market_state = ctx["market_state"]
    dist_ema     = ctx["dist_ema"]
    phase        = ctx["phase"]
    impulse      = ctx["impulse"]
    price        = ctx["price"]

    reason.append(f"PHASE:{phase}")
    _confirm_signal_created_ts = _candle_open_ts(df15.iloc[-2])
    _confirm_signal_detected_ts = time.time()

    # ── HARD GATE: state rõ ràng xấu
    if mkt_state == "DEAD":
        reason.append("Soft:DEAD_MARKET")

    # ── H1 trend
    h1_trend = trend_h1(df1h)
    side_h1 = _fallback_side(df15, h1_trend)
    if not h1_trend:
        stats["ema_fail"] += 1
        reason.append("Soft:TREND_FAIL")

    reason.append(f"MKT:{mkt_state}")
    log_state(symbol, mkt_state, mkt_metrics.get("impulse", 0), mkt_metrics.get("vol_ratio", 1))

    if not _passes_pre_score_tag_gate(
        symbol,
        "CONFIRM",
        reason,
        structural_payload={
            "reason_tags": reason,
            "ctx": ctx,
            "side": side_h1,
            "phase": phase,
            "signal_created_ts": _confirm_signal_created_ts,
            "signal_detected_ts": _confirm_signal_detected_ts,
        },
    ):
        return None
    if not _passes_market_state_gate(symbol, "CONFIRM", mkt_state, reason):
        return None

    # ── BOS detection
    if DEBUG:
        print(f"[BOS CALL] {symbol}")
    if side_h1:
        bos_type, bos_level = _detect_bos_m15(df15, side_h1)
    else:
        bos_long, bos_level_long = _detect_bos_m15(df15, "LONG")
        bos_short, bos_level_short = _detect_bos_m15(df15, "SHORT")
        if bos_long is not None:
            bos_type, bos_level = bos_long, bos_level_long
        else:
            bos_type, bos_level = bos_short, bos_level_short
    if DEBUG:
        print(f"[BOS RESULT] {symbol} bos={bos_type}")

    bos_was_fallback = False
    if bos_type is None:
        stats["bos_fail"] += 1
        reason.append("Soft:BOS_FAIL")
        bos_level = df15["high"].iloc[-20:-3].max() if side_h1 == "LONG" else df15["low"].iloc[-20:-3].min()
        bos_type = "WEAK"
        bos_was_fallback = True

    # Re-evaluate BOS on the confirmed H1 side after the BOS-first gate.
    _bos_type_side, _bos_level_side = _detect_bos_m15(df15, side_h1)
    if _bos_type_side is not None:
        bos_type, bos_level = _bos_type_side, _bos_level_side
        bos_was_fallback = False

    # ── EMA distance (confirm dùng ngưỡng rộng hơn một chút)
    if dist_ema < 0.002:
        reason.append("Soft:EMA_TOO_CLOSE")
    if dist_ema > 0.02:
        stats["ema_fail"] += 1
        reason.append("Soft:EMA_TOO_FAR")

    if DEBUG:
        print(f"[CONFIRM DBG] {symbol} | bos={bos_type} level={round(bos_level,4)} price={round(price,4)}")

    _ema34_log = df15["close"].ewm(span=34).mean().iloc[-2]
    _ema89_log = df15["close"].ewm(span=89).mean().iloc[-2]
    _slope_log = _ema34_log - df15["close"].ewm(span=34).mean().iloc[-5]
    _side_m15_log = _ema_trend_m15(df15)
    _ema_align_log = "ALIGN" if _side_m15_log == side_h1 else "MISALIGN"
    _ema_slope_log = "UP" if _slope_log > 0 else "DOWN"
    _price_log = df15["close"].iloc[-2]
    _ph_log    = df15["high"].iloc[-20:-2].max()
    _pl_log    = df15["low"].iloc[-20:-2].min()
    _rng_log   = _ph_log - _pl_log if _ph_log > _pl_log else 1e-9
    _pp_log    = (_price_log - _pl_log) / _rng_log
    _level_log = _ph_log if side_h1 == "LONG" else _pl_log
    _dtl_log   = abs(_price_log - _level_log) / max(_level_log, 1e-9)
    _dl_log    = abs(_price_log - _pl_log) / max(_pl_log, 1e-9)
    _tm_log    = {"price_position": round(_pp_log, 4), "dist_to_level": round(_dtl_log, 4), "dist_low": round(_dl_log, 4)}
    if _dtl_log <= 0.005:
        log_scan_ema(symbol, side_h1, _side_m15_log, _ema_align_log, _ema_slope_log, _tm_log, None, passed=(bos_type is not None))

    if bos_type is None:
        reason.append("Soft:BOS_FAIL")
        bos_level = _ph_log if side_h1 == "LONG" else _pl_log
        bos_type = "WEAK"

    if bos_type == "TRAP":
        reason.append("Soft:BOS_TRAP")

    reason.append(f"BOS:{bos_type}")

    # ── Retest
    retest_ok, retest_str = _detect_retest(df15, bos_level, side_h1)
    reason.append(f"Retest:{retest_str}")
    if not retest_ok:
        reason.append("Soft:RETEST_FAIL")

    # ── Candle confirmation
    candle_strength = _classify_candle_confirm(df15)
    reason.append(f"Candle:{candle_strength}")

    if candle_strength == "WEAK":
        stats["candle_fail"] += 1
        reason.append("Soft:CANDLE_WEAK")

    # old logic (disabled):     
    #if False and candle_strength == "WEAK":
        #log_confirm_reject(symbol, "CANDLE_WEAK")
        #print(f"[CONFIRM REJECT] {symbol} | Candle WEAK — confirm cần ít nhất NORMAL")
        #return None

    # ── CONFIRM STALENESS CHECK: distance-from-BOS validation
    # Equivalent protection to SWING pipeline (entry.py:2241-2249).
    # If price has traveled > 1.5 ATR from the BOS level without a pullback,
    # the continuation geometry is degraded — reject to prevent stale re-confirmation.
    if bos_level and bos_level > 0:
        _stale_atr = (df15["high"] - df15["low"]).rolling(14).mean().iloc[-2]
        _current_close = df15["close"].iloc[-2]
        if _stale_atr > 0 and not math.isnan(_stale_atr):
            if side_h1 == "LONG":
                _bos_dist = _current_close - bos_level
            else:
                _bos_dist = bos_level - _current_close
            if _bos_dist > _stale_atr * 1.5:
                log_confirm_reject(symbol, "STALE_BOS_DISTANCE", entry_type="CONFIRM")
                if DEBUG:
                    print(
                        f"[CONFIRM STALE] {symbol} | price traveled {round(_bos_dist/_stale_atr, 2)}x ATR "
                        f"from BOS level={round(bos_level, 4)} — stale continuation rejected"
                    )
                return None

    # ── Entry / SL / TP từ BOS level
    signal_entry = df15["close"].iloc[-2]
    current_price = df15["close"].iloc[-1]
    drift = abs(current_price - signal_entry) / signal_entry
    if DEBUG:
        print(f"[ENTRY FIX] {symbol} signal_entry={round(signal_entry, 6)} actual_entry={round(current_price, 6)} drift={round(drift * 100, 4)}%")
    entry = current_price

    atr = (df15["high"] - df15["low"]).rolling(14).mean().iloc[-2]
    if math.isnan(atr) or atr <= 0:
        log_confirm_reject(symbol, "ATR_INVALID")
        if DEBUG:
            print(f"[CONFIRM REJECT] {symbol} | ATR invalid atr={atr}")
        return None

    if side_h1 == "LONG":
        sl = df15["low"].iloc[-10:-2].min() - atr * 0.5
        tp = entry + atr * 4.0
    else:
        sl = df15["high"].iloc[-10:-2].max() + atr * 0.5
        tp = entry - atr * 4.0

    risk = abs(entry - sl)
    rr   = abs(tp - entry) / risk if risk > 0 else 0

    # ── RR hard gate (confirm ngặt hơn)
    if rr < 1.0:
        log_confirm_reject(
            symbol,
            "RR_FAIL",
            entry_type="CONFIRM",
            structural_payload={
                "reason_tags": reason,
                "ctx": ctx,
                "side": side_h1,
                "phase": phase,
                "bos_type": bos_type,
                "retest_status": retest_str,
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "rr": rr,
                "signal_created_ts": _confirm_signal_created_ts,
                "signal_detected_ts": _confirm_signal_detected_ts,
            },
        )
        print(f"[CONFIRM REJECT] {symbol} | RR tệ rr={rr:.2f} < 1.5")
        return None

    # ── Scoring
    if rr < 1.5:
        reason.append("Soft:RR_FAIL")

    exhaustion_cls, exhaustion_score, _ = compute_exhaustion(df15, side_h1, bos_count(df15, side_h1), symbol)
    reason.append(f"Exhaustion:{exhaustion_cls}")
    score, score_breakdown = _compute_unified_score(
        ctx, bos_type, bos_level, retest_ok, retest_str, candle_strength, rr,
        entry, signal_entry, side_h1, "CONFIRM",
        exhaustion_cls, "NONE", "NONE", df15, df1h, reason
    )
    score_v2, breakdown = _compute_shadow_score(
        symbol, "CONFIRM", df15, df15, bos_type, h1_trend, side_h1, rr, ctx["market_state"], score_old=score
    )

    if bos_was_fallback:
        score *= 0.8
        if DEBUG:
            print(f"[CONFIRM BOS_FALLBACK] {symbol} | no real BOS — score penalized to {round(score, 2)}")

    if bos_type == "NEAR":
        if exhaustion_cls in ("EXHAUSTED", "COLLAPSING"):
            log_confirm_reject(symbol, "BOS_NEAR_EXHAUSTED")
            log_exhaustion_counterfactual(
                symbol=symbol, side=side_h1, entry=entry, sl=sl, tp=tp,
                exhaustion_cls=exhaustion_cls, bos_type=bos_type, pool_stage="",
                entry_type="CONFIRM", score=score, reject_reason="BOS_NEAR_EXHAUSTED",
            )
            if DEBUG:
                print(f"[CONFIRM REJECT] {symbol} | BOS:NEAR + {exhaustion_cls} — hard gate")
            return None
        elif exhaustion_cls == "HEALTHY":
            score -= 1.0
            if DEBUG:
                print(f"[CONFIRM NEAR_BOS] {symbol} | proximity only, HEALTHY — minor penalty to {round(score, 2)}")
        elif DEBUG:
            print(f"[CONFIRM NEAR_BOS] {symbol} | BOS:NEAR + {exhaustion_cls} — no hard gate")

    if bos_type == "STRONG" and exhaustion_cls == "EXTENDED":
        log_confirm_reject(symbol, "EXHAUSTION_GATE_EXTENDED+BOS_STRONG")
        log_exhaustion_counterfactual(
            symbol=symbol, side=side_h1, entry=entry, sl=sl, tp=tp,
            exhaustion_cls=exhaustion_cls, bos_type=bos_type, pool_stage="",
            entry_type="CONFIRM", score=score, reject_reason="EXHAUSTION_GATE_EXTENDED+BOS_STRONG",
        )
        print(
            f"[EXHAUSTION GATE] {symbol} {side_h1} score={round(score,1)} "
            f"— EXTENDED+BOS_STRONG late-trend rejected"
        )
        return None

    entry_position = "MID" if 0.35 <= _pp_log <= 0.65 else "EDGE"
    if entry_position == "MID":
        score -= 2.5
        if DEBUG:
            print(f"[CONFIRM MID_RANGE] {symbol} | mid-range entry pp={round(_pp_log, 2)} — score penalized to {round(score, 2)}")

    if DEBUG:
        print(f"[CONFIRM CHECK] {symbol} | bos={bos_type} score={round(score,2)}")

    if not _passes_entry_quality_gate(
        symbol,
        "CONFIRM",
        score,
        reason,
        structural_payload={
            "reason_tags": reason,
            "ctx": ctx,
            "side": side_h1,
            "phase": phase,
            "bos_type": bos_type,
            "retest_status": retest_str,
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "rr": rr,
            "score_breakdown": score_breakdown,
            "exhaustion_cls": exhaustion_cls,
            "signal_created_ts": _confirm_signal_created_ts,
            "signal_detected_ts": _confirm_signal_detected_ts,
        },
    ):
        return None

    bos_n = bos_count(df15, side_h1)
    if bos_n >= 3:
        score_threshold = 3.5
    else:
        score_threshold = 5.5

    if score < score_threshold:
        stats["core_fail"] += 1
        log_confirm_reject(symbol, f"SCORE_FAIL ({score_threshold})")
        if DEBUG:
            print(f"[CONFIRM REJECT] {symbol} | Score thấp score={score} threshold={score_threshold}")
        return None

    reason.append(f"RR:{round(rr, 2)}")
    reason.append(f"Score:{score}")

    # === CONFIRM V2 — PHASE DETECTION + CONFIDENCE ===

    # Step 1: phase_type
    _v2_impulse = ctx["impulse"]
    _v2_move    = ctx["move"]
    _v2_candle  = candle_strength

    if _v2_impulse and _v2_move > 0.02:
        phase_type = "IMPULSE"
    elif retest_ok and _v2_candle == "STRONG":
        phase_type = "RETEST_STRONG"
    elif retest_ok:
        phase_type = "RETEST"
    elif _v2_candle == "STRONG":
        phase_type = "CONFIRM_STRONG"
    else:
        phase_type = "NEUTRAL"

    # Step 2: multipliers
    phase_mult = 1.0
    entry_mult = 1.0
    htf_mult   = 1.0

    if phase_type == "IMPULSE":
        phase_mult = 1.1
    elif phase_type == "RETEST_STRONG":
        phase_mult = 1.15
    elif phase_type == "RETEST":
        phase_mult = 1.05
    elif phase_type == "CONFIRM_STRONG":
        phase_mult = 1.1

    _v2_recent_high = df15["high"].iloc[-20:-2].max()
    _v2_recent_low  = df15["low"].iloc[-20:-2].min()
    if side_h1 == "SHORT":
        _v2_dist = (_v2_recent_high - entry) / max(_v2_recent_high, 1e-9)
        _v2_loc  = +1 if _v2_dist < 0.01 else (-1 if _v2_dist > 0.03 else 0)
    elif side_h1 == "LONG":
        _v2_dist = (entry - _v2_recent_low) / max(_v2_recent_low, 1e-9)
        _v2_loc  = +1 if _v2_dist < 0.01 else (-1 if _v2_dist > 0.03 else 0)
    else:
        _v2_loc = 0
    _v2_timing   = -1 if _v2_move > 0.03 else 0
    _v2_ctx_bias = +0.5 if ctx["mkt_state"] == "TREND" else (-0.5 if ctx["mkt_state"] == "EXHAUSTION" else 0)
    _v2_pos      = max(min(_v2_loc + _v2_timing + _v2_ctx_bias, 1), -1)

    if _v2_pos > 0:
        entry_mult = 1.05
    elif _v2_pos < 0:
        entry_mult = 0.95

    if h1_trend == side_h1:
        htf_mult = 1.1
    else:
        htf_mult = 0.9

    # Step 2.5: position_mult
    entry_type = "CONFIRM"
    position_mult = 1.0
    if df4h is not None:
        recent_h4 = df4h.iloc[-50:-2]
        recent_h1 = df1h.iloc[-60:-2]

        high_h4 = recent_h4["high"].quantile(0.9)
        low_h4  = recent_h4["low"].quantile(0.1)

        high_h1 = recent_h1["high"].quantile(0.9)
        low_h1  = recent_h1["low"].quantile(0.1)

        price_pos = df1h["close"].iloc[-2]

        pos_h4 = (price_pos - low_h4) / (high_h4 - low_h4 + 1e-9)
        pos_h1 = (price_pos - low_h1) / (high_h1 - low_h1 + 1e-9)

        if entry_type == "CONFIRM":
            if side_h1 == "LONG" and pos_h4 > 0.8:
                position_mult *= 0.8
            if side_h1 == "SHORT" and pos_h4 < 0.2:
                position_mult *= 0.8

            if side_h1 == "LONG" and pos_h1 > 0.8:
                position_mult *= 0.9
            if side_h1 == "SHORT" and pos_h1 < 0.2:
                position_mult *= 0.9

        if entry_type.startswith("SWING"):
            if side_h1 == "LONG" and pos_h4 < 0.2:
                position_mult *= 1.1
            if side_h1 == "SHORT" and pos_h4 > 0.8:
                position_mult *= 1.1

        if entry_type.startswith("REVERSAL"):
            if side_h1 == "LONG" and pos_h4 < 0.2:
                position_mult *= 1.1
            if side_h1 == "SHORT" and pos_h4 > 0.8:
                position_mult *= 1.1

        if entry_type.startswith("EARLY"):
            if side_h1 == "LONG" and pos_h1 > 0.8:
                position_mult *= 0.9
            if side_h1 == "SHORT" and pos_h1 < 0.2:
                position_mult *= 0.9

    # Step 3: final_score (non-destructive — base_score unchanged)
    base_score  = score
    final_score = base_score * phase_mult * entry_mult * htf_mult * position_mult

    # Step 4: logging
    if DEBUG:
        print(
            f"[CONFIRM V2] {symbol}"
            f" base_score={round(base_score, 2)}"
            f" phase_type={phase_type}"
            f" phase_mult={phase_mult}"
            f" entry_mult={entry_mult}"
            f" htf_mult={htf_mult}"
            f" position_mult={round(position_mult, 4)}"
            f" final_score={round(final_score, 2)}"
        )

    # === END CONFIRM V2 ===

    # ── Duplicate check
    if is_duplicate_zone(symbol, side_h1, entry):
        return None

    smc_ctx = compute_smc_context(df15, df1h, df4h, side_h1, bos_type, bos_level, ctx)
    # Log-only M15 snapshot for post-open PAPER CONFIRM acceptance research.
    # This metadata is not consumed by scoring or execution.
    _acceptance_candle = df15.iloc[-3]
    _acceptance_range = df15.iloc[-20:-4]
    _acceptance_ema34 = df15["close"].ewm(span=34).mean().iloc[-2]
    _acceptance_ema89 = df15["close"].ewm(span=89).mean().iloc[-2]
    _acceptance_ema_slope = (
        df15["close"].ewm(span=34).mean().iloc[-2]
        - df15["close"].ewm(span=34).mean().iloc[-5]
    )
    _acceptance_htf_bias = None
    if _acceptance_ema34 > _acceptance_ema89 and _acceptance_ema_slope > 0:
        _acceptance_htf_bias = "LONG"
    elif _acceptance_ema34 < _acceptance_ema89 and _acceptance_ema_slope < 0:
        _acceptance_htf_bias = "SHORT"
    confirm_entry_acceptance_context = {
        "candle_open": _acceptance_candle["open"],
        "candle_high": _acceptance_candle["high"],
        "candle_low": _acceptance_candle["low"],
        "candle_close": _acceptance_candle["close"],
        "atr": atr,
        "break_level": bos_level,
        "pre_break_level": bos_level,
        "htf_bias": _acceptance_htf_bias,
        "m15_ema34": _acceptance_ema34,
        "m15_close": df15["close"].iloc[-2],
        "nearest_htf_support": _acceptance_range["low"].min(),
        "nearest_htf_resistance": _acceptance_range["high"].max(),
    }
    if isinstance(score_breakdown, dict):
        score_breakdown["smc"] = smc_ctx
    breakdown = _with_smc_breakdown(breakdown, smc_ctx)

    signal = {
        "side":       side_h1,
        "entry":      entry,
        "sl":         sl,
        "tp":         tp,
        "reason":     reason,
        "entry_type": "CONFIRM",
        "_size_mult": 1.0,
        "_ctx":       ctx,
        "_score":          score,
        "score_v2":        score_v2,
        "score_breakdown": score_breakdown,
        "exhaustion_cls":   exhaustion_cls,
        "exhaustion_score": exhaustion_score,
        "smc_zone":         smc_ctx["smc_zone"],
        "liquidity_sweep":  smc_ctx["liquidity_sweep"],
        "bos_confirmation": smc_ctx["bos_confirmation"],
        "smc_bias":         smc_ctx["smc_bias"],
        "range_context":    smc_ctx["range_context"],
        "invalid_context":  smc_ctx["invalid_context"],
        "confirm_entry_acceptance_context": confirm_entry_acceptance_context,
    }
    breakdown = _attach_structural_context(signal, breakdown, ctx=ctx, smc_ctx=smc_ctx)
    if isinstance(breakdown, dict):
        breakdown["signal_created_ts"] = _confirm_signal_created_ts
        breakdown["signal_detected_ts"] = _confirm_signal_detected_ts
        breakdown["confirm_entry_acceptance_context"] = confirm_entry_acceptance_context
    breakdown = _with_accepted_signal_context(breakdown, symbol, signal)
    _log_score_shadow(symbol, "SIGNAL", "ACCEPT", "CONFIRM", score_old=score, score_v2=score_v2, breakdown=breakdown)
    return signal


# =====================================================================
# REVERSAL HELPERS
# =====================================================================

def _ema_trend_m15(df):
    ema34 = df["close"].ewm(span=34).mean()
    ema89 = df["close"].ewm(span=89).mean()
    slope = ema34.iloc[-1] - ema34.iloc[-5]
    if ema34.iloc[-1] > ema89.iloc[-1] and slope > 0:
        return "LONG"
    if ema34.iloc[-1] < ema89.iloc[-1] and slope < 0:
        return "SHORT"
    return None


def _bos_count_m15(df15, side):
    count = 0
    for i in range(-20, -2):
        sub = df15.iloc[:i]
        if len(sub) < 5:
            continue
        bos_type, _ = _detect_bos_m15(sub, side)
        if bos_type:
            count += 1
    return count


def _detect_wyckoff_v3(df15, side):
    if len(df15) < 55:
        return None, None, 0

    last = df15.iloc[-2]
    body = abs(last["close"] - last["open"])
    high = last["high"]
    low  = last["low"]

    prev_high = df15["high"].iloc[-50:-2].max()
    prev_low  = df15["low"].iloc[-50:-2].min()

    base_high  = df15["high"].iloc[-17:-2].max()
    base_low   = df15["low"].iloc[-17:-2].min()
    base_range = (base_high - base_low) / (base_low + 1e-9)
    has_base   = base_range < 0.03

    wyckoff_score = 0
    wyckoff_type  = None

    if side == "LONG" and low < prev_low and last["close"] > prev_low:

        if not has_base:
            wyckoff_score -= 1

        wick       = prev_low - low
        wick_ratio = wick / (body + 1e-9)

        if wick_ratio >= 2:   wyckoff_score += 2
        elif wick_ratio >= 1: wyckoff_score += 1

        prev_closed = df15.iloc[-3]
        if last["close"] > prev_closed["close"]:
            wyckoff_score += 1

        vol = df15["volume"].iloc[-2]
        avg = df15["volume"].rolling(20).mean().iloc[-2]
        if vol > avg * 1.5:
            wyckoff_score += 1

        if last["close"] > base_low:
            wyckoff_score += 1

        wyckoff_type = "SPRING"

    elif side == "SHORT" and high > prev_high and last["close"] < prev_high:

        if not has_base:
            wyckoff_score -= 1

        wick       = high - prev_high
        wick_ratio = wick / (body + 1e-9)

        if wick_ratio >= 2:   wyckoff_score += 2
        elif wick_ratio >= 1: wyckoff_score += 1

        prev_closed = df15.iloc[-3]
        if last["close"] < prev_closed["close"]:
            wyckoff_score += 1

        vol = df15["volume"].iloc[-2]
        avg = df15["volume"].rolling(20).mean().iloc[-2]
        if vol > avg * 1.5:
            wyckoff_score += 1

        if last["close"] < base_high:
            wyckoff_score += 1

        wyckoff_type = "UPTHRUST"

    if wyckoff_type is None:
        return None, None, 0

    if wyckoff_score >= 4:   return "STRONG", wyckoff_type, wyckoff_score
    elif wyckoff_score >= 2: return "MEDIUM", wyckoff_type, wyckoff_score
    elif wyckoff_score >= 1: return "WEAK",   wyckoff_type, wyckoff_score

    return None, None, 0


# =====================================================================
# REVERSAL PIPELINE
# =====================================================================

def reversal_pipeline(symbol, df, cls, df15, df1h, df4h=None, df1d=None):
    reason = []
    _set_shadow_score_context(symbol, "REVERSAL_CONFIRM")

    ctx = build_market_context(df15, df1h)
    mkt_state    = ctx["mkt_state"]
    mkt_metrics  = ctx["mkt_metrics"]
    market_state = ctx["market_state"]
    phase        = ctx["phase"]
    dist_ema     = ctx["dist_ema"]

    reason.append(f"PHASE:{phase}")

    h1_trend = trend_h1(df1h)
    side_m15 = _ema_trend_m15(df15)
    if not side_m15:
        side_m15 = h1_trend

    if not h1_trend:
        reason.append("Soft:TREND_FAIL")

    rev_side = _fallback_side(df15, side_m15 or h1_trend)

    if DEBUG:
        print(f"[BOS CALL] {symbol}")
    if rev_side:
        bos_type, bos_level = _detect_bos_m15(df15, rev_side)
    else:
        bos_long, bos_level_long = _detect_bos_m15(df15, "LONG")
        bos_short, bos_level_short = _detect_bos_m15(df15, "SHORT")
        if bos_long is not None:
            bos_type, bos_level = bos_long, bos_level_long
        else:
            bos_type, bos_level = bos_short, bos_level_short
    if DEBUG:
        print(f"[BOS RESULT] {symbol} bos={bos_type}")
    if not bos_type:
        stats["bos_fail"] += 1
        reason.append("Soft:BOS_FAIL")
        bos_level = df15["high"].iloc[-20:-3].max() if rev_side == "LONG" else df15["low"].iloc[-20:-3].min()
        bos_type = "WEAK"

    if side_m15 == h1_trend and h1_trend:
        reason.append("Soft:NO_REVERSAL")

    _bos_type_side, _bos_level_side = _detect_bos_m15(df15, rev_side)
    if _bos_type_side is not None:
        bos_type, bos_level = _bos_type_side, _bos_level_side
    if bos_type == "TRAP":
        reason.append("Soft:BOS_TRAP")

    reason.append(f"BOS:{bos_type}")
    reason.append(f"MKT:{mkt_state}")
    log_state(symbol, mkt_state, mkt_metrics.get("impulse", 0), mkt_metrics.get("vol_ratio", 1))

    retest_ok, retest_str = _detect_retest(df15, bos_level, rev_side)
    reason.append(f"Retest:{retest_str}")
    if not retest_ok:
        reason.append("Soft:RETEST_FAIL")

    if retest_str == "WEAK":
        reason.append("Soft:RETEST_WEAK")

    bos_n = _bos_count_m15(df15, rev_side)
    exhaustion_cls, exhaustion_score, _ = compute_exhaustion(df15, rev_side, bos_n, symbol)
    reason.append(f"Exhaustion:{exhaustion_cls}")
    if DEBUG:
        print(f"[REVERSAL CHECK] {symbol} | state={mkt_state} exhaustion={exhaustion_cls}")

    shadow_base = {
        "side": rev_side,
        "ctx": ctx,
        "df15": df15,
        "df1h": df1h,
        "df4h": df4h,
        "bos_type": bos_type,
        "bos_level": bos_level,
        "retest_ok": retest_ok,
        "retest_str": retest_str,
        "exhaustion_cls": exhaustion_cls,
        "h1_trend": h1_trend,
        "signal_created_ts": time.time(),
    }

    if not _passes_market_state_gate(symbol, "REVERSAL_CONFIRM", mkt_state, reason, shadow=shadow_base):
        return None
    if not _passes_reversal_context(symbol, "REVERSAL_CONFIRM", mkt_state, reason, shadow=shadow_base):
        return None

    wy_type, wy_name, wy_score = _detect_wyckoff_v3(df15, rev_side)
    if not wy_type:
        reason.append("Soft:WYCKOFF_FAIL")
        wy_type, wy_name = "WEAK", "NONE"

    reason.append(f"Wyckoff:{wy_type}_{wy_name}")

    signal_entry = df15["close"].iloc[-2]
    current_price = df15["close"].iloc[-1]
    drift = abs(current_price - signal_entry) / signal_entry
    if DEBUG:
        print(f"[ENTRY FIX] {symbol} signal_entry={round(signal_entry, 6)} actual_entry={round(current_price, 6)} drift={round(drift * 100, 4)}%")
    entry = current_price

    atr = (df15["high"] - df15["low"]).rolling(14).mean().iloc[-2]
    if math.isnan(atr) or atr <= 0:
        log_confirm_reject(symbol, "ATR_INVALID")
        if DEBUG:
            print(f"[REVERSAL REJECT] {symbol} | ATR invalid atr={atr}")
        return None

    if rev_side == "LONG":
        sl = df15["low"].iloc[-10:-2].min() - atr * 0.5
        tp = entry + atr * 4.0
    else:
        sl = df15["high"].iloc[-10:-2].max() + atr * 0.5
        tp = entry - atr * 4.0

    risk = abs(entry - sl)
    rr   = abs(tp - entry) / risk if risk > 0 else 0

    if rr < 1.0:
        log_confirm_reject(symbol, "RR_FAIL")
        print(f"[REVERSAL REJECT] {symbol} | RR tệ rr={rr:.2f} < 1.0")
        return None

    if rr < 1.5:
        reason.append("Soft:RR_FAIL")

    reason.append(f"RR:{round(rr, 2)}")

    if is_duplicate_zone(symbol, rev_side, entry):
        return None

    entry_size = 0.7

    candle_strength = _classify_candle_confirm(df15)
    r_score, score_breakdown = _compute_unified_score(
        ctx, bos_type, bos_level, retest_ok, retest_str, candle_strength, rr,
        entry, signal_entry, rev_side, "REVERSAL_CONFIRM",
        exhaustion_cls, wy_type or "NONE", wy_name or "NONE", df15, df1h, reason
    )
    score_v2, breakdown = _compute_shadow_score(
        symbol, "REVERSAL_CONFIRM", df15, df15, bos_type, h1_trend, rev_side, rr, ctx["market_state"], score_old=r_score
    )

    if not _passes_entry_quality_gate(symbol, "REVERSAL_CONFIRM", r_score, reason):
        _log_reversal_shadow_candidate(
            symbol,
            "ENTRY_QUALITY_FAIL",
            side=rev_side,
            ctx=ctx,
            reason=reason,
            bos_type=bos_type,
            exhaustion_cls=exhaustion_cls,
            score=r_score,
            entry=entry,
            sl=sl,
            tp=tp,
            signal_created_ts=shadow_base["signal_created_ts"],
        )
        return None

    if r_score < 3.5:
        log_confirm_reject(symbol, "SCORE_FAIL")
        _log_reversal_shadow_candidate(
            symbol,
            "SCORE_FAIL",
            side=rev_side,
            ctx=ctx,
            reason=reason,
            bos_type=bos_type,
            exhaustion_cls=exhaustion_cls,
            score=r_score,
            entry=entry,
            sl=sl,
            tp=tp,
            signal_created_ts=shadow_base["signal_created_ts"],
        )
        print(f"[REVERSAL REJECT] {symbol} | Score low score={r_score}")
        return None

    smc_ctx = compute_smc_context(df15, df1h, df4h, rev_side, bos_type, bos_level, ctx)
    if isinstance(score_breakdown, dict):
        score_breakdown["smc"] = smc_ctx
    breakdown = _with_smc_breakdown(breakdown, smc_ctx)

    signal = {
        "side":           rev_side,
        "entry":          entry,
        "sl":             sl,
        "tp":             tp,
        "reason":         reason,
        "entry_type":     "REVERSAL_CONFIRM",
        "_size_mult":     entry_size,
        "_ctx":           ctx,
        "exhaustion_cls":   exhaustion_cls,
        "exhaustion_score": exhaustion_score,
        "wyckoff":          wy_type,
        "wyckoff_name":   wy_name,
        "_score":         r_score,
        "score_v2":       score_v2,
        "score_breakdown": score_breakdown,
        "smc_zone":         smc_ctx["smc_zone"],
        "liquidity_sweep":  smc_ctx["liquidity_sweep"],
        "bos_confirmation": smc_ctx["bos_confirmation"],
        "smc_bias":         smc_ctx["smc_bias"],
        "range_context":    smc_ctx["range_context"],
        "invalid_context":  smc_ctx["invalid_context"],
    }
    breakdown = _attach_structural_context(signal, breakdown, ctx=ctx, smc_ctx=smc_ctx)
    breakdown = _with_accepted_signal_context(breakdown, symbol, signal)
    _log_score_shadow(symbol, "SIGNAL", "ACCEPT", "REVERSAL_CONFIRM", score_old=r_score, score_v2=score_v2, breakdown=breakdown)
    return signal


# =====================================================================
# SWING HELPERS
# =====================================================================

def _should_log_swing(symbol, log_type, reason=""):
    key = (symbol, log_type, str(reason))
    now = time.time()
    last = _swing_log_cache.get(key, 0)
    if now - last < _LOG_COOLDOWN:
        return False
    _swing_log_cache[key] = now
    return True


def _detect_early_break(df1h, compression, bias_dir):
    if not compression or not compression.get("valid"):
        return None, 0

    low_20, high_20 = compression["range"]
    close = df1h["close"].iloc[-2]
    last  = df1h.iloc[-2]
    prev  = df1h.iloc[-3]
    vol   = df1h["volume"].iloc[-2]
    avg_vol = df1h["volume"].rolling(20).mean().iloc[-2]
    vol_ratio = vol / avg_vol if avg_vol > 0 else 1.0

    score = 0

    broke_up   = close > high_20
    broke_down = close < low_20
    if not broke_up and not broke_down:
        return None, 0

    break_dir   = "UP" if broke_up else "DOWN"
    level       = high_20 if broke_up else low_20

    break_strength = abs(close - level) / level
    if break_strength < 0.0005 and vol_ratio < 1.2:
        return None, 0

    body      = abs(last["close"] - last["open"])
    rng       = last["high"] - last["low"] if last["high"] - last["low"] > 0 else 1e-9
    br        = body / rng
    upper_wick = (last["high"] - max(last["open"], last["close"])) / rng
    lower_wick = (min(last["open"], last["close"]) - last["low"]) / rng

    strong_body = br >= 0.6

    if strong_body:
        score += 1

    if broke_up   and upper_wick >= 0.4: return None, 0
    if broke_down and lower_wick >= 0.4: return None, 0

    volume_spike = vol_ratio >= 1.5
    if volume_spike:
        score += 1
    elif vol_ratio < 1.0:
        score -= 1

    ctype = compression.get("type", "NEUTRAL")
    aligned = (ctype == "ACCUMULATION" and break_dir == "UP") or \
              (ctype == "DISTRIBUTION" and break_dir == "DOWN")
    if aligned:
        score += 2
    else:
        score -= 2

    bias_map = {"LONG": "UP", "SHORT": "DOWN"}
    bias_norm = bias_map.get(bias_dir, bias_dir) if bias_dir else None
    if bias_norm and break_dir == bias_norm:
        score += 1

    prev_close_inside = low_20 <= prev["close"] <= high_20
    wick_break = (
        (broke_up   and last["high"] > high_20 and last["close"] <= high_20) or
        (broke_down and last["low"]  < low_20  and last["close"] >= low_20)
    )
    close_inside = low_20 <= close <= high_20

    prev_body = abs(prev["close"] - prev["open"])
    prev_rng  = prev["high"] - prev["low"] if prev["high"] - prev["low"] > 0 else 1e-9
    prev_br   = prev_body / prev_rng

    prev_broke_up   = prev["close"] > high_20
    prev_broke_down = prev["close"] < low_20
    current_reversal = (
        (prev_broke_up   and last["close"] < last["open"] and br >= 0.6) or
        (prev_broke_down and last["close"] > last["open"] and br >= 0.6)
    )

    reverse_strong = prev_br >= 0.6 and (
        (broke_up   and prev["close"] < prev["open"]) or
        (broke_down and prev["close"] > prev["open"])
    )

    trap_signal = current_reversal or (reverse_strong and abs(prev["close"] - level) / level > 0.002)

    rejection_count_local = sum(
        1 for i in range(-(H1_WINDOW+2), -2)
        if df1h["high"].iloc[i] >= (high_20 if broke_up else low_20) * 0.998
        and (df1h["high"].iloc[i] - max(df1h["open"].iloc[i], df1h["close"].iloc[i]))
           / (df1h["high"].iloc[i] - df1h["low"].iloc[i] + 1e-9) >= 0.3
    )

    if ctype == "DISTRIBUTION" and break_dir == "UP":
        return None, 0

    if rejection_count_local >= 2 and break_dir == "UP":
        return None, 0

    if strong_body and volume_spike and aligned and score >= 3:
        action = "ENTER"
    elif wick_break and close_inside:
        action = "REJECT"
    elif trap_signal:
        action = "COUNTER"
    else:
        action = None

    return action, score


def _send_compression_alert(symbol, ctype, high_20, low_20, rejection_count, pre_break, score, now, tightening):
    RANGE_EPS = 0.002
    prev = compression_alert_sent.get(symbol, {})
    prev_high = prev.get("range_high")
    already_sent = (
        prev_high is not None and
        abs(high_20 - prev_high) / high_20 < RANGE_EPS and
        abs(low_20 - prev.get("range_low", 0)) / low_20 < RANGE_EPS and
        now - prev.get("time", 0) < 7200
    )
    if already_sent:
        return
    pre_str   = pre_break if pre_break else "NONE"
    tight_str = "YES" if tightening else "NO"
    print(
        f"[COMPRESSION] {symbol} {ctype} | "
        f"Range: {str(round(low_20, 4))} – {str(round(high_20, 4))} | "
        f"Rejection: {rejection_count} | Tightening: {tight_str} | Pre-break: {pre_str}"
    )
    compression_alert_sent[symbol] = {
        "range_high": high_20, "range_low": low_20, "score": score, "time": now
    }


def _scan_compression(symbol, df1h, df4h=None, df1d=None):
    if len(df1h) < 22:
        return

    high_20 = df1h["high"].iloc[-(H1_WINDOW+2):-2].max()
    low_20  = df1h["low"].iloc[-(H1_WINDOW+2):-2].min()
    close   = df1h["close"].iloc[-2]
    if low_20 <= 0 or high_20 <= 0:
        return

    impulse_move = abs(close - df1h["close"].iloc[-8]) / df1h["close"].iloc[-8] if df1h["close"].iloc[-8] > 0 else 0
    if impulse_move > 0.08:
        return

    highs_arr = [df1h["high"].iloc[i] for i in range(-(H1_WINDOW+2), -2)]
    lows_arr  = [df1h["low"].iloc[i]  for i in range(-(H1_WINDOW+2), -2)]
    mid = len(highs_arr) // 2
    is_lower_high  = max(highs_arr[mid:]) < max(highs_arr[:mid])
    is_lower_low   = min(lows_arr[mid:])  < min(lows_arr[:mid])
    is_higher_high = max(highs_arr[mid:]) > max(highs_arr[:mid])
    is_higher_low  = min(lows_arr[mid:])  > min(lows_arr[:mid])

    if is_lower_high and is_lower_low:
        return

    ema34 = df1h["close"].ewm(span=34).mean().iloc[-2]
    ema89 = df1h["close"].ewm(span=89).mean().iloc[-2]
    strong_momentum = abs(ema34 - ema89) / max(close, 1e-9) > 0.04
    if is_higher_high and is_higher_low and strong_momentum:
        return

    ema_slope = ema34 - df1h["close"].ewm(span=34).mean().iloc[-5]
    ema_bearish = ema34 < ema89 and ema_slope < 0
    ema_bullish = ema34 > ema89 and ema_slope > 0

    range_last5 = df1h["high"].iloc[-7:-2].max() - df1h["low"].iloc[-7:-2].min()
    range_prev5 = df1h["high"].iloc[-12:-7].max() - df1h["low"].iloc[-12:-7].min()
    tightening  = range_prev5 > 0 and range_last5 < range_prev5

    r80_h = low_20 + (high_20 - low_20) * 0.9
    r80_l = low_20 + (high_20 - low_20) * 0.1
    compression_candles = sum(
        1 for i in range(-(H1_WINDOW+2), -2)
        if r80_l <= df1h["close"].iloc[i] <= r80_h
    )
    if compression_candles < 6:
        return

    recent_lows = [df1h["low"].iloc[i] for i in range(-7, -2)]
    higher_lows = all(recent_lows[i] >= recent_lows[i-1] * 0.998 for i in range(1, len(recent_lows)))
    equal_lows  = all(abs(recent_lows[i] - recent_lows[0]) / max(recent_lows[0], 1e-9) < 0.005
                      for i in range(1, len(recent_lows)))
    flat_base   = (high_20 - low_20) / max(low_20, 1e-9) < 0.03

    if not (higher_lows or equal_lows or flat_base):
        return

    trend_h1_side = trend_h1(df1h)
    near_resistance = abs(close - high_20) / high_20 < 0.003
    near_support    = abs(close - low_20)  / low_20  < 0.003

    rejection_count = sum(
        1 for i in range(-(H1_WINDOW+2), -2)
        if df1h["high"].iloc[i] >= high_20 * 0.998
        and (df1h["high"].iloc[i] - max(df1h["open"].iloc[i], df1h["close"].iloc[i]))
           / (df1h["high"].iloc[i] - df1h["low"].iloc[i] + 1e-9) >= 0.3
    )

    if trend_h1_side == "LONG" and higher_lows:
        ctype = "ACCUMULATION"
    elif near_resistance and rejection_count >= 2:
        ctype = "DISTRIBUTION"
    elif near_support and rejection_count >= 2:
        ctype = "ACCUMULATION_WEAK"
    else:
        ctype = "NEUTRAL"

    score = 0
    if tightening:             score += 2
    if higher_lows:            score += 1
    if rejection_count >= 2:   score += 1
    vol     = df1h["volume"].iloc[-2]
    avg_vol = df1h["volume"].rolling(20).mean().iloc[-2]
    vol_ratio = vol / avg_vol if avg_vol > 0 else 1.0
    if vol_ratio < 0.8:        score += 1

    if score < 3:
        return

    dist_high = abs(close - high_20) / high_20
    dist_low  = abs(close - low_20)  / low_20
    if dist_high < 0.001:
        pre_break = "UP"
    elif dist_low < 0.001:
        pre_break = "DOWN"
    else:
        pre_break = None

    def _ema_side(df):
        if df is None or len(df) < 90: return None
        e34 = df["close"].ewm(span=34).mean().iloc[-2]
        e89 = df["close"].ewm(span=89).mean().iloc[-2]
        if e34 > e89: return "LONG"
        if e34 < e89: return "SHORT"
        return None

    h4_side = _ema_side(df4h)
    d1_side = _ema_side(df1d)
    long_score = short_score = 0
    if h4_side == "LONG"  and d1_side == "LONG":  long_score  += 2
    elif h4_side == "LONG":                        long_score  += 1
    if h4_side == "SHORT" and d1_side == "SHORT":  short_score += 2
    elif h4_side == "SHORT":                       short_score += 1
    total_bias = long_score + short_score
    bias_long  = round(long_score  / total_bias * 100, 1) if total_bias else 50.0
    bias_short = round(short_score / total_bias * 100, 1) if total_bias else 50.0

    now = time.time()

    if symbol in compression_watchlist:
        w = compression_watchlist[symbol]
        w["range_high"]       = high_20
        w["range_low"]        = low_20
        w["live_range_high"]  = high_20
        w["live_range_low"]   = low_20
        w["score"]            = score
        w["compression_type"] = ctype
        w["rejection_count"]  = rejection_count
        w["pre_break"]        = pre_break
        w["bias_long"]        = bias_long
        w["bias_short"]       = bias_short
        w["tightening"]       = tightening
        w["compression_v2_ok"]    = True
        w["compression_score_v2"] = score
        w["pre_break_score"]      = (3 if pre_break else 0)

        if pre_break and _should_log_swing(symbol, "PRE_BREAK_ALERT", pre_break):
            print(
                f"[PRE-BREAKOUT] {symbol} {pre_break} | "
                f"Range: {str(round(low_20, 4))} – {str(round(high_20, 4))} | "
                f"{ctype} | Rejection: {rejection_count}"
            )

        return

    if score < COMPRESS_MIN_SCORE:
        return

    _send_compression_alert(symbol, ctype, high_20, low_20, rejection_count, pre_break, score, now, tightening)

    compression_watchlist[symbol] = {
        "range_high":       high_20,
        "range_low":        low_20,
        "setup_range_high": high_20,
        "setup_range_low":  low_20,
        "live_range_high":  high_20,
        "live_range_low":   low_20,
        "score":            score,
        "priority":         0.0,
        "pre_break_score":  (3 if pre_break else 0),
        "timestamp":        now,
        "phase":            "compress",
        "breakout_dir":     None,
        "compression_type":     ctype,
        "rejection_count":      rejection_count,
        "pre_break":            pre_break,
        "tightening":           tightening,
        "bias_long":            bias_long,
        "bias_short":           bias_short,
        "compression_v2_ok":    True,
        "compression_score_v2": score,
        "last_sent_range_high": 0,
        "last_sent_range_low":  0,
        "last_sent_score":      None,
        "last_sent_time":       0,
        "last_score":           score,
        "last_alert_time":      0,
        "last_range":           high_20 - low_20,
        "last_pre_break_score": 0,
    }


def _cleanup_compression(symbol, df1h):
    if symbol not in compression_watchlist:
        return
    w     = compression_watchlist[symbol]
    if w.get("phase") == "bos_confirmed":
        return
    close = df1h["close"].iloc[-2]
    live_high = w.get("live_range_high", w["range_high"])
    live_low  = w.get("live_range_low",  w["range_low"])
    broke_high = close > live_high * 1.002
    broke_low  = close < live_low  * 0.998
    moved_away = abs(close - live_high) / live_high > 0.015
    if broke_high or broke_low or moved_away:
        del compression_watchlist[symbol]


def _update_swing_priority(symbol, df1h, df4h=None):
    if symbol not in compression_watchlist:
        return

    w     = compression_watchlist[symbol]
    close = df1h["close"].iloc[-2]
    vol   = df1h["volume"].iloc[-2]
    avg_vol = df1h["volume"].rolling(20).mean().iloc[-2]
    vol_ratio = vol / avg_vol if avg_vol > 0 else 1.0

    range_high = w.get("live_range_high", w["range_high"])
    range_low  = w.get("live_range_low",  w["range_low"])
    range_pct  = (range_high - range_low) / range_low

    dist_high    = (range_high - close) / close
    dist_low     = (close - range_low)  / close
    edge_prox    = min(dist_high, dist_low)

    tightness    = max(0.0, 1.0 - range_pct)
    dry_score    = max(0.0, 1.0 - vol_ratio)

    in_range = sum(
        1 for i in range(-H1_WINDOW, -2)
        if range_low <= df1h["close"].iloc[i] <= range_high
    )
    time_score = min(1.0, in_range / H1_WINDOW)

    false_break = 0
    last = df1h.iloc[-2]
    if last["high"] > range_high and last["close"] < range_high:
        false_break += 1
    if last["low"]  < range_low  and last["close"] > range_low:
        false_break += 1

    priority_raw  = 0.0
    priority_raw += (1.0 - edge_prox)  * 2.0
    priority_raw += tightness          * 1.5
    priority_raw += dry_score          * 1.5
    priority_raw += time_score         * 1.0
    priority_raw += false_break        * 1.0

    age_hours    = (time.time() - w["timestamp"]) / 3600
    time_penalty = min(1.0, age_hours / 12)
    priority_final = priority_raw * (1 - time_penalty * 0.3)

    bias_long_val  = w.get("bias_long",  50)
    bias_strength  = abs(bias_long_val - 50) / 50
    priority_raw  += bias_strength * 0.5

    htf_context = "unknown"
    if df4h is not None and len(df4h) >= 30:
        h4_high    = df4h["high"].iloc[-H4_WINDOW:-2].max()
        h4_low     = df4h["low"].iloc[-H4_WINDOW:-2].min()
        h4_close   = df4h["close"].iloc[-2]
        h4_range   = (h4_high - h4_low) / h4_close if h4_close > 0 else 1

        h4_ema34   = df4h["close"].ewm(span=34).mean().iloc[-2]
        h4_ema89   = df4h["close"].ewm(span=89).mean().iloc[-2]
        ema_spread = abs(h4_ema34 - h4_ema89) / h4_close if h4_close > 0 else 1

        tight_range    = h4_range   < 0.03
        ema_compressed = ema_spread < 0.01

        h4_highs = df4h["high"].iloc[-10:-2].values
        h4_lows  = df4h["low"].iloc[-10:-2].values
        hh_hl    = all(h4_highs[i] > h4_highs[i-1] and h4_lows[i] > h4_lows[i-1]
                       for i in range(1, len(h4_highs)))
        ll_lh    = all(h4_highs[i] < h4_highs[i-1] and h4_lows[i] < h4_lows[i-1]
                       for i in range(1, len(h4_highs)))
        no_strong_trend = not hh_hl and not ll_lh

        if tight_range and ema_compressed and no_strong_trend:
            htf_context = "compress"
        else:
            htf_context = "trend"

        w["htf_context"] = htf_context

        if htf_context == "compress":
            priority_raw += 1.0
            if w.get("pre_break_score", 0) >= 2:
                priority_raw += 0.5
        elif htf_context == "trend":
            priority_raw -= 1.0

    priority_final = priority_raw * (1 - time_penalty * 0.3)

    w["priority"]       = round(priority_raw, 2)
    w["priority_final"] = round(priority_final, 2)


def _get_swing_bias(df4h, df1d):
    def ema_side(df):
        if df is None or len(df) < 90:
            return None
        e34 = df["close"].ewm(span=34).mean().iloc[-2]
        e89 = df["close"].ewm(span=89).mean().iloc[-2]
        if e34 > e89:   return "LONG"
        if e34 < e89:   return "SHORT"
        return None

    h4_side = ema_side(df4h)
    d1_side = ema_side(df1d)

    if h4_side and d1_side and h4_side != d1_side:
        return None, 0

    if h4_side and d1_side and h4_side == d1_side:
        return h4_side, 1.0

    if h4_side and not d1_side:
        return h4_side, 0.5

    return None, 0


def _detect_swing_entry(symbol, df1h, bias):
    if symbol not in compression_watchlist:
        return None, None

    w      = compression_watchlist[symbol]
    close  = df1h["close"].iloc[-2]
    last   = df1h.iloc[-2]

    range_high = w.get("setup_range_high", w["range_high"])
    range_low  = w.get("setup_range_low",  w["range_low"])

    body       = abs(last["close"] - last["open"])
    rng        = last["high"] - last["low"]
    br         = body / rng if rng > 0 else 0
    upper_wick = (last["high"] - max(last["open"], last["close"])) / rng if rng > 0 else 0
    lower_wick = (min(last["open"], last["close"]) - last["low"]) / rng if rng > 0 else 0
    is_bull    = last["close"] > last["open"]
    is_bear    = last["close"] < last["open"]

    # TASK 2: momentum confirmation — strong candle OR wick rejection >= 2x body
    strong_bull         = br >= 0.6 and is_bull
    strong_bear         = br >= 0.6 and is_bear
    wick_rejection_bull = br > 0 and lower_wick >= br * 2 and is_bull
    wick_rejection_bear = br > 0 and upper_wick >= br * 2 and is_bear

    valid_long_trigger  = strong_bull or wick_rejection_bull
    valid_short_trigger = strong_bear or wick_rejection_bear

    # TASK 1: detect confirmed BOS — candle must close beyond level by >= 0.25% with strong body
    if w["phase"] == "compress":
        if bias == "LONG" and close > range_high:
            bos_dist = (close - range_high) / range_high if range_high > 0 else 0
            if bos_dist >= 0.0025 and br >= 0.6 and is_bull:
                w["phase"]        = "bos_confirmed"
                w["breakout_dir"] = "LONG"
                w["bos_level"]    = range_high
                w["bos_price"]    = close
                print(f"[BOS CONFIRMED] {symbol} LONG level={round(range_high,4)} bos_dist={round(bos_dist,4)}")
                _log_swing_shadow_candidate(
                    symbol, "BOS_WAITING", side="LONG", watchlist=w,
                    phase="bos_confirmed", bias=bias, breakout_dir="LONG",
                    range_high=range_high, range_low=range_low,
                    bos_level=range_high, bos_price=close, close=close,
                    body_ratio=br, upper_wick=upper_wick, lower_wick=lower_wick,
                    valid_long_trigger=valid_long_trigger,
                    valid_short_trigger=valid_short_trigger,
                )

        if bias == "SHORT" and close < range_low:
            bos_dist = (range_low - close) / range_low if range_low > 0 else 0
            if bos_dist >= 0.0025 and br >= 0.6 and is_bear:
                w["phase"]        = "bos_confirmed"
                w["breakout_dir"] = "SHORT"
                w["bos_level"]    = range_low
                w["bos_price"]    = close
                print(f"[BOS CONFIRMED] {symbol} SHORT level={round(range_low,4)} bos_dist={round(bos_dist,4)}")
                _log_swing_shadow_candidate(
                    symbol, "BOS_WAITING", side="SHORT", watchlist=w,
                    phase="bos_confirmed", bias=bias, breakout_dir="SHORT",
                    range_high=range_high, range_low=range_low,
                    bos_level=range_low, bos_price=close, close=close,
                    body_ratio=br, upper_wick=upper_wick, lower_wick=lower_wick,
                    valid_long_trigger=valid_long_trigger,
                    valid_short_trigger=valid_short_trigger,
                )

        if w["phase"] == "compress":
            _log_swing_shadow_candidate(
                symbol, "BOS_WAITING", side=bias, watchlist=w,
                phase="compress", bias=bias, breakout_dir=w.get("breakout_dir"),
                range_high=range_high, range_low=range_low, close=close,
                body_ratio=br, upper_wick=upper_wick, lower_wick=lower_wick,
                valid_long_trigger=valid_long_trigger,
                valid_short_trigger=valid_short_trigger,
            )

        return None, None

    # TASK 1: pullback entry — wait for retest of BOS level AFTER confirmed BOS
    if w["phase"] == "bos_confirmed":
        dir_      = w["breakout_dir"]
        bos_level = w.get("bos_level", range_high if dir_ == "LONG" else range_low)

        # TASK 3: distance filter — if price ran > 1.5 ATR from BOS without pullback, setup stale
        atr = (df1h["high"] - df1h["low"]).rolling(14).mean().iloc[-2]
        if atr > 0 and not math.isnan(atr):
            if dir_ == "LONG":
                if close - bos_level > atr * 1.5:
                    _log_swing_shadow_candidate(
                        symbol, "STALE_AFTER_BOS", side="LONG", watchlist=w,
                        phase="bos_confirmed", bias=bias, breakout_dir=dir_,
                        range_high=range_high, range_low=range_low,
                        bos_level=bos_level, bos_price=w.get("bos_price"),
                        close=close, body_ratio=br, upper_wick=upper_wick,
                        lower_wick=lower_wick,
                        valid_long_trigger=valid_long_trigger,
                        valid_short_trigger=valid_short_trigger,
                    )
                    del compression_watchlist[symbol]
                    return None, None
            else:
                if bos_level - close > atr * 1.5:
                    _log_swing_shadow_candidate(
                        symbol, "STALE_AFTER_BOS", side="SHORT", watchlist=w,
                        phase="bos_confirmed", bias=bias, breakout_dir=dir_,
                        range_high=range_high, range_low=range_low,
                        bos_level=bos_level, bos_price=w.get("bos_price"),
                        close=close, body_ratio=br, upper_wick=upper_wick,
                        lower_wick=lower_wick,
                        valid_long_trigger=valid_long_trigger,
                        valid_short_trigger=valid_short_trigger,
                    )
                    del compression_watchlist[symbol]
                    return None, None

        ema34      = df1h["close"].ewm(span=34).mean().iloc[-2]
        retest_tol = 0.005

        if dir_ == "LONG":
            in_retest_zone = (
                close >= bos_level * (1 - retest_tol) and
                close <= bos_level * (1 + retest_tol)
            )
            near_ema = abs(close - ema34) / ema34 <= 0.005
            if (in_retest_zone or near_ema) and valid_long_trigger:
                return "SWING_RETEST", "LONG"
            _log_swing_shadow_candidate(
                symbol,
                "RETEST_TRIGGER_FAIL" if (in_retest_zone or near_ema) else "RETEST_ZONE_FAIL",
                side="LONG", watchlist=w, phase="bos_confirmed", bias=bias,
                breakout_dir=dir_, range_high=range_high, range_low=range_low,
                bos_level=bos_level, bos_price=w.get("bos_price"), close=close,
                ema34=ema34, retest_tol=retest_tol, in_retest_zone=in_retest_zone,
                near_ema=near_ema, valid_long_trigger=valid_long_trigger,
                valid_short_trigger=valid_short_trigger, body_ratio=br,
                upper_wick=upper_wick, lower_wick=lower_wick,
            )

        if dir_ == "SHORT":
            in_retest_zone = (
                close <= bos_level * (1 + retest_tol) and
                close >= bos_level * (1 - retest_tol)
            )
            near_ema = abs(close - ema34) / ema34 <= 0.005
            if (in_retest_zone or near_ema) and valid_short_trigger:
                return "SWING_RETEST", "SHORT"
            _log_swing_shadow_candidate(
                symbol,
                "RETEST_TRIGGER_FAIL" if (in_retest_zone or near_ema) else "RETEST_ZONE_FAIL",
                side="SHORT", watchlist=w, phase="bos_confirmed", bias=bias,
                breakout_dir=dir_, range_high=range_high, range_low=range_low,
                bos_level=bos_level, bos_price=w.get("bos_price"), close=close,
                ema34=ema34, retest_tol=retest_tol, in_retest_zone=in_retest_zone,
                near_ema=near_ema, valid_long_trigger=valid_long_trigger,
                valid_short_trigger=valid_short_trigger, body_ratio=br,
                upper_wick=upper_wick, lower_wick=lower_wick,
            )

    return None, None


# =====================================================================
# SWING PIPELINE
# =====================================================================

def swing_pipeline(symbol, df, cls, df15, df1h, df4h=None, df1d=None):
    reason = []
    _set_shadow_score_context(symbol, "SWING")

    ctx = build_market_context(df15, df1h)

    _scan_compression(symbol, df1h, df4h, df1d)
    _cleanup_compression(symbol, df1h)
    _update_swing_priority(symbol, df1h, df4h)

    # allow scan to seed watchlist first — top-N gate below still applies
    top_symbols = sorted(
        compression_watchlist,
        key=lambda s: compression_watchlist[s].get("priority_final",
                      compression_watchlist[s].get("priority", 0)),
        reverse=True
    )[:SWING_TOP_N]

    # allow swing evaluation, let scoring decide
    pass

    ema34 = df1h["close"].ewm(span=34).mean().iloc[-2]
    ema89 = df1h["close"].ewm(span=89).mean().iloc[-2]
    ema_slope = ema34 - df1h["close"].ewm(span=34).mean().iloc[-5]
    ema_bearish = ema34 < ema89 and ema_slope < 0
    ema_bullish = ema34 > ema89 and ema_slope > 0

    _w_fallback = {
        "range_high":     df1h["high"].iloc[-20:-2].max(),
        "range_low":      df1h["low"].iloc[-20:-2].min(),
        "score":          0,
        "priority":       0,
        "priority_final": 0,
        "breakout_dir":   None,
    }
    _w = compression_watchlist.get(symbol, _w_fallback)
    _h1_side = _w.get("breakout_dir") or "LONG"
    _exh_cls_h1, _exh_score_h1 = detect_exhaustion_v66(df1h, _h1_side, 0)
    if _exh_cls_h1 == "EXHAUSTED" and _exh_score_h1 > 3:
        _log_swing_shadow_candidate(
            symbol,
            "H1_EXHAUSTION_FAIL",
            side=_h1_side,
            ctx=ctx,
            watchlist=_w,
            phase=_w.get("phase"),
            breakout_dir=_w.get("breakout_dir"),
            range_high=_w.get("range_high"),
            range_low=_w.get("range_low"),
            priority=_w.get("priority"),
            priority_final=_w.get("priority_final"),
            compression_score=_w.get("score"),
        )
        return None

    bias, size_mult = _get_swing_bias(df4h, df1d)

    mkt_state, _ = get_market_state(df1h)

    phase   = "NEUTRAL"
    impulse = False

    candle    = df1h.iloc[-2]
    body      = abs(candle["close"] - candle["open"])
    avg_range = (df1h["high"] - df1h["low"]).rolling(20).mean().iloc[-2]
    move      = abs(df1h["close"].iloc[-2] - df1h["close"].iloc[-6]) / df1h["close"].iloc[-6]

    if body > avg_range * 1.5 or move > 0.03:
        impulse = True

    w = _w

    range_high = w["range_high"]
    range_low  = w["range_low"]
    price      = df1h["close"].iloc[-2]

    dist_high = abs(price - range_high) / max(range_high, 1e-9)
    dist_low  = abs(price - range_low)  / max(range_low, 1e-9)

    phase = "NEUTRAL"

    if dist_high < 0.01:
        phase = "PRE_BREAK"

    elif dist_low < 0.01:
        phase = "PRE_BREAK"

    elif mkt_state == "TREND":
        phase = "BREAKOUT_STRONG" if impulse else "BREAKOUT_WEAK"

    if DEBUG:
        print(f"[PHASE DBG] {symbol} | price={round(price,4)} | high={round(range_high,4)} | low={round(range_low,4)} | distH={round(dist_high,4)} | distL={round(dist_low,4)} | phase={phase}")

    if not bias:
        bias = trend_h1(df1h)

    if not bias:
        bias = "LONG"
    if DEBUG:
        print(f"[SWING CHECK] {symbol} | bias={bias} zone_ok={phase}")

    bias_type = "H4+D1" if size_mult == 1.0 else "H4_only"

    if symbol not in compression_watchlist:
        compression_watchlist[symbol] = {
            "range_high":     w["range_high"],
            "range_low":      w["range_low"],
            "score":          w.get("score", 0),
            "priority":       w.get("priority", 0),
            "priority_final": w.get("priority_final", 0),
            "breakout_dir":   bias,
            "phase":          "compress",
            "timestamp":      time.time(),
        }

    swing_type, swing_side = _detect_swing_entry(symbol, df1h, bias)
    if not swing_type:
        return None

    if phase == "BREAKOUT_STRONG":
        if swing_side != bias:
            size_mult *= 0.5

    if phase == "BREAKOUT_STRONG":
        size_mult *= 1.2

    elif phase == "BREAKOUT_WEAK":
        size_mult *= 1.0

    elif phase == "NEUTRAL":
        size_mult *= 0.7

    is_confirm = False

    if phase == "BREAKOUT_STRONG" and swing_side == bias:
        is_confirm = True

    elif phase == "BREAKOUT_WEAK" and swing_side == bias and impulse:
        is_confirm = True

    if is_confirm:

        if swing_side == "LONG" and ema_bullish:
            size_mult *= 1.2

        elif swing_side == "SHORT" and ema_bearish:
            size_mult *= 1.2

        else:
            size_mult *= 0.7

        size_mult *= 1.3
        swing_type = "CONFIRM"

    if swing_side == "LONG" and ema_bearish:
        size_mult *= 0.6

    if swing_side == "SHORT" and ema_bullish:
        size_mult *= 0.6

    atr_h1 = (df1h["high"] - df1h["low"]).rolling(14).mean().iloc[-2]
    if math.isnan(atr_h1) or atr_h1 <= 0:
        _log_swing_shadow_candidate(
            symbol,
            "ATR_INVALID",
            side=swing_side,
            ctx=ctx,
            watchlist=w,
            reason=reason,
            phase=phase,
            market_state=ctx.get("market_state"),
            mkt_state=mkt_state,
            bias=bias,
            bias_type=bias_type,
            breakout_dir=w.get("breakout_dir"),
            range_high=range_high,
            range_low=range_low,
            close=df1h["close"].iloc[-2],
        )
        if DEBUG:
            print(f"[SWING REJECT] {symbol} | ATR invalid atr_h1={atr_h1}")
        return None
    signal_entry = df1h["close"].iloc[-2]
    current_price = df1h["close"].iloc[-1]
    drift = abs(current_price - signal_entry) / signal_entry
    if DEBUG:
        print(f"[ENTRY FIX] {symbol} signal_entry={round(signal_entry, 6)} actual_entry={round(current_price, 6)} drift={round(drift * 100, 4)}%")
    entry = current_price

    if swing_side == "LONG":
        swing_lvl = df1h["low"].iloc[-10:-2].min()
        sl        = swing_lvl - atr_h1 * 0.25
    else:
        swing_lvl = df1h["high"].iloc[-10:-2].max()
        sl        = swing_lvl + atr_h1 * 0.25

    sl_dist = abs(entry - sl)

    if sl_dist < atr_h1 * 0.2 or sl_dist > atr_h1 * 3.0:
        _log_swing_shadow_candidate(
            symbol,
            "SL_DISTANCE_FAIL",
            side=swing_side,
            ctx=ctx,
            watchlist=w,
            reason=reason,
            phase=phase,
            market_state=ctx.get("market_state"),
            mkt_state=mkt_state,
            bias=bias,
            bias_type=bias_type,
            breakout_dir=w.get("breakout_dir"),
            range_high=range_high,
            range_low=range_low,
            close=signal_entry,
            priority=w.get("priority"),
            priority_final=w.get("priority_final"),
            compression_score=w.get("score"),
            entry=entry,
            signal_entry=signal_entry,
            sl=sl,
        )
        return None

    # TASK 4: TP reference 1.8R – 3R; TASK 5: reject if trade cannot reach 1R
    swing_tp_mult = 2.5
    tp = (entry + sl_dist * swing_tp_mult if swing_side == "LONG"
          else entry - sl_dist * swing_tp_mult)

    rr = abs(tp - entry) / sl_dist
    if rr < 1.8:
        _log_swing_shadow_candidate(
            symbol,
            "RR_FAIL",
            side=swing_side,
            ctx=ctx,
            watchlist=w,
            reason=reason,
            phase=phase,
            market_state=ctx.get("market_state"),
            mkt_state=mkt_state,
            bias=bias,
            bias_type=bias_type,
            breakout_dir=w.get("breakout_dir"),
            range_high=range_high,
            range_low=range_low,
            close=signal_entry,
            priority=w.get("priority"),
            priority_final=w.get("priority_final"),
            compression_score=w.get("score"),
            entry=entry,
            signal_entry=signal_entry,
            sl=sl,
            tp=tp,
            rr=rr,
        )
        return None

    entry_size = 0.5 * size_mult

    if swing_type == "CONFIRM":
        entry_type_str = "SWING_BREAK"
    else:
        entry_type_str = swing_type

    reason.append(f"Compress_{w['score']}")
    reason.append(f"Pri_{w.get('priority_final', w.get('priority', 0))}")
    reason.append(f"Phase:{phase}")
    reason.append(f"Bias:{bias_type}")

    if DEBUG:
        print(f"[SWING] {symbol} | {entry_type_str} | phase={phase}")

    candle_strength = _classify_candle_confirm(df1h)
    swing_bos_level = range_high if swing_side == "LONG" else range_low
    s_score, score_breakdown = _compute_unified_score(
        ctx, "SWING", swing_bos_level, True, "SWING", candle_strength, rr,
        entry, signal_entry, swing_side, entry_type_str,
        "HEALTHY", "NONE", "NONE", df1h, df1h, reason
    )
    _s_score_pre_boost = s_score
    if swing_type == "SWING_RETEST":
        s_score += 0.5
    if DEBUG:
        print(f"[SWING SCORE FIX] symbol={symbol} old_score={round(_s_score_pre_boost, 2)} new_score={round(s_score, 2)}")
    score_v2, breakdown = _compute_shadow_score(
        symbol, entry_type_str, df15, df15, None, trend_h1(df1h), swing_side, rr, ctx["market_state"], score_old=s_score
    )
    if s_score < 5.0:
        _log_swing_shadow_candidate(
            symbol,
            "SCORE_FAIL",
            side=swing_side,
            ctx=ctx,
            watchlist=w,
            reason=reason,
            phase=phase,
            market_state=ctx.get("market_state"),
            mkt_state=mkt_state,
            bias=bias,
            bias_type=bias_type,
            breakout_dir=w.get("breakout_dir"),
            range_high=range_high,
            range_low=range_low,
            close=signal_entry,
            priority=w.get("priority"),
            priority_final=w.get("priority_final"),
            compression_score=w.get("score"),
            entry=entry,
            signal_entry=signal_entry,
            sl=sl,
            tp=tp,
            rr=rr,
            score=s_score,
            score_breakdown=score_breakdown,
        )
        _log_score_shadow(symbol, "REJECT", "SCORE_FAIL", entry_type_str, score_old=s_score, score_v2=score_v2, breakdown=breakdown)
        if DEBUG:
            print(f"[SWING REJECT] {symbol} | Score low score={s_score}")
        return None

    smc_ctx = compute_smc_context(df15, df1h, df4h, swing_side, None, None, ctx)
    if entry_type_str == "SWING_RETEST" and isinstance(score_breakdown, dict):
        score_breakdown["smc"] = smc_ctx
    breakdown = _with_smc_breakdown(breakdown, smc_ctx)

    signal = {
        "side":             swing_side,
        "entry":            entry,
        "sl":               sl,
        "tp":               tp,
        "reason":           reason,
        "entry_type":       entry_type_str,
        "_size_mult":       entry_size,
        "_ctx":             ctx,
        "priority_final":   w.get("priority_final", w.get("priority", 0)),
        "compression_score": w["score"],
        "range_high":       w["range_high"],
        "range_low":        w["range_low"],
        "_score":           s_score,
        "score_v2":         score_v2,
        "score_breakdown":  score_breakdown,
    }
    if entry_type_str == "SWING_RETEST":
        signal.update({
            "smc_zone":         smc_ctx["smc_zone"],
            "liquidity_sweep":  smc_ctx["liquidity_sweep"],
            "bos_confirmation": smc_ctx["bos_confirmation"],
            "smc_bias":         smc_ctx["smc_bias"],
            "range_context":    smc_ctx["range_context"],
            "invalid_context":  smc_ctx["invalid_context"],
        })
    breakdown = _attach_structural_context(signal, breakdown, ctx=ctx, smc_ctx=smc_ctx)
    breakdown = _with_accepted_signal_context(breakdown, symbol, signal)
    _log_score_shadow(symbol, "SIGNAL", "ACCEPT", entry_type_str, score_old=s_score, score_v2=score_v2, breakdown=breakdown)
    if entry_type_str == "SWING_RETEST":
        swing_outcome_source_ts = _candle_open_ts(df15.iloc[-2]) or time.time()
        swing_outcome_payload = dict(signal)
        swing_outcome_payload.update({
            "symbol": symbol,
            "entry_type": "SWING_RETEST",
            "score": s_score,
            "score_v2": score_v2,
            "signal_created_ts": swing_outcome_source_ts,
            "structural_context": _dict_copy(breakdown.get("structural_context")),
        })
        _register_swing_retest_shadow_outcome(swing_outcome_payload)
    return signal


# =====================================================================
# MODE SELECTOR
# =====================================================================

def select_mode(state):
    if state == "TREND":
        return ["confirm", "swing", "early", "reversal"]

    elif state == "ACCUMULATION":
        return ["reversal"]

    elif state == "EXHAUSTION":
        return ["reversal", "early", "confirm"]

    else:
        return ["early", "reversal", "confirm"]


# =====================================================================
# SIGNAL SELECTION
# =====================================================================

def select_best_signal(candidates):
    if not candidates:
        return None

    priority = {
        "REVERSAL": 4,
        "SWING":    3,
        "CONFIRM":  2,
        "EARLY":    1,
    }

    def _priority(t):
        et = t.get("entry_type", "")
        if et.startswith("REVERSAL"):
            return priority["REVERSAL"]
        if et.startswith("SWING"):
            return priority["SWING"]
        if et == "CONFIRM":
            return priority["CONFIRM"]
        if et.startswith("EARLY"):
            return priority["EARLY"]
        return 0

    candidates.sort(
        key=lambda t: (t.get("score", 0), _priority(t)),
        reverse=True,
    )

    best = candidates[0]

    if best.get("entry_type") == "CONFIRM":
        swing = next((c for c in candidates if c.get("entry_type", "").startswith("SWING")), None)
        if swing and swing.get("score", 0) >= best.get("score", 0) - 1.5:
            best = swing

    if DEBUG:
        print(
            f"[SELECT] candidates={[(c.get('entry_type'), round(c.get('score', 0), 2)) for c in candidates]}"
            f" → chosen={best.get('entry_type')}"
        )
    return best


# =====================================================================
# ANALYZE — orchestrator
# =====================================================================

def analyze(symbol, df_map):
    if DEBUG:
        print(f"[ANALYZE START] {symbol}")
    stats["scanned"] += 1
    _scan_filter_summary["scanned"] += 1
    if DEBUG:
        print(f"[STATS DEBUG] scanned={stats['scanned']}")
    df5  = df_map["5m"]
    df15 = df_map["15m"]
    df1h = df_map["1h"]
    df4h = df_map.get("4h")
    df1d = df_map.get("1d")

    ctx = build_market_context(df15, df1h)
    allowed = select_mode(ctx["market_state"])
    if DEBUG:
        print(f"[ANALYZE MODE] {symbol} | state={ctx['market_state']} | allowed={allowed}")

    candidates = []

    # EARLY
    if "early" in allowed:
        signal = early_pipeline(symbol, df5, None, df15, df1h)
        if signal:
            t = build_trade(symbol, signal, signal["_ctx"], early_size_mult=signal["_size_mult"])
            if t:
                candidates.append(t)
                if DEBUG:
                    print(f"[ANALYZE CANDIDATE] {symbol} | pipeline={t.get('entry_type', EARLY_CONT_ENTRY_TYPE)} score={t.get('score')}")
            else:
                if DEBUG:
                    print(f"[ANALYZE REJECT] {symbol} | {EARLY_CONT_ENTRY_TYPE} pipeline → no signal")
        else:
            if DEBUG:
                print(f"[ANALYZE REJECT] {symbol} | {EARLY_CONT_ENTRY_TYPE} pipeline → no signal")
    else:
        if DEBUG:
            print(f"[ANALYZE REJECT] {symbol} | {EARLY_CONT_ENTRY_TYPE} not in allowed modes")

    # CONFIRM
    if "confirm" in allowed:
        signal = confirm_pipeline(symbol, df5, None, df15, df1h)
        if signal:
            t = build_trade(symbol, signal, signal["_ctx"], early_size_mult=signal["_size_mult"])
            if t:
                candidates.append(t)
                if DEBUG:
                    print(f"[ANALYZE CANDIDATE] {symbol} | pipeline=CONFIRM score={t.get('score')}")
            else:
                if DEBUG:
                    print(f"[ANALYZE REJECT] {symbol} | CONFIRM pipeline → no signal")
        else:
            if DEBUG:
                print(f"[ANALYZE REJECT] {symbol} | CONFIRM pipeline → no signal")
    else:
        if DEBUG:
            print(f"[ANALYZE REJECT] {symbol} | CONFIRM not in allowed modes")

    # SWING
    if "swing" in allowed:
        signal = swing_pipeline(symbol, df5, None, df15, df1h, df4h, df1d)
        if signal:
            t = build_trade(symbol, signal, signal["_ctx"], early_size_mult=signal["_size_mult"])
            if t:
                candidates.append(t)
                if DEBUG:
                    print(f"[ANALYZE CANDIDATE] {symbol} | pipeline=SWING score={t.get('score')}")
            else:
                if DEBUG:
                    print(f"[ANALYZE REJECT] {symbol} | SWING pipeline → no signal")
        else:
            if DEBUG:
                print(f"[ANALYZE REJECT] {symbol} | SWING pipeline → no signal")
    else:
        if DEBUG:
            print(f"[ANALYZE REJECT] {symbol} | SWING not in allowed modes")

    # REVERSAL
    if "reversal" in allowed:
        signal = reversal_pipeline(symbol, df5, None, df15, df1h)
        if signal:
            t = build_trade(symbol, signal, signal["_ctx"], early_size_mult=signal["_size_mult"])
            if t:
                candidates.append(t)
                if DEBUG:
                    print(f"[ANALYZE CANDIDATE] {symbol} | pipeline=REVERSAL score={t.get('score')}")
            else:
                if DEBUG:
                    print(f"[ANALYZE REJECT] {symbol} | REVERSAL pipeline → no signal")
        else:
            if DEBUG:
                print(f"[ANALYZE REJECT] {symbol} | REVERSAL pipeline → no signal")
    else:
        if DEBUG:
            print(f"[ANALYZE REJECT] {symbol} | REVERSAL not in allowed modes")

    if DEBUG:
        print(f"[CANDIDATES] {symbol} | count={len(candidates)} types={[c['entry_type'] for c in candidates]}")
    best = select_best_signal(candidates)
    if DEBUG:
        print(f"[SELECTED] {symbol} | entry={best['entry_type'] if best else None}")
    if best:
        signal_state[symbol] = {
            "time":      time.time(),
            "direction": best["side"],
            "price":     best["entry"],
        }
        if best["entry_type"].startswith("EARLY"):
            stats["early_pass"] += 1
        stats["pass"] += 1
        _scan_filter_summary["passed"] += 1
        if DEBUG:
            print(f"[ANALYZE PASS] {symbol} | pipeline={best['entry_type']}")
        return best

    if DEBUG:
        print(f"[ANALYZE REJECT] {symbol} | all pipelines exhausted → None")
    return None
