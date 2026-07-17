import json
import math
import os
import time
from datetime import datetime, timezone


SCHEMA_VERSION = "giveback_management_shadow_v1"
CLOSE_SCHEMA_VERSION = "giveback_management_close_shadow_v1"
STATE_SCHEMA_VERSION = 1
STATE_FILE = "giveback_management_shadow_state.json"
OBS_LOG = os.path.join("logs", "giveback_management_shadow_v1.jsonl")
CLOSE_LOG = os.path.join("logs", "giveback_management_close_shadow_v1.jsonl")
MAX_STATE_BYTES = 1024 * 1024
MAX_ACTIVE_RECORDS = 100
MAX_TERMINAL_IDS = 500
TERMINAL_TTL_SECS = 45 * 24 * 3600
HEARTBEAT_SECS = 30 * 60
V5_SLIPPAGE_ASSUMPTION_R = 0.05
PEAK_EMIT_STEP_R = 0.10


def _now():
    return time.time()


def _iso(ts=None):
    return datetime.fromtimestamp(ts or _now(), tz=timezone.utc).isoformat()


def _safe_float(value, default=None):
    try:
        if value in ("", None):
            return default
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _round(value, places=6):
    value = _safe_float(value)
    return None if value is None else round(value, places)


def _bool(value):
    return bool(value) if value is not None else False


def _trade_identity(trade):
    for key in ("client_order_id", "exchange_client_id", "opened_trade_id"):
        value = str((trade or {}).get(key) or "").strip()
        if key in ("client_order_id", "exchange_client_id") and value.startswith("BOT_"):
            return value
        if key == "opened_trade_id" and value:
            return value
    dedup = str((trade or {}).get("dedup_key") or "").strip()
    opened = str((trade or {}).get("time") or (trade or {}).get("entry_time") or "").strip()
    if dedup and opened:
        return f"{dedup}:{opened}"
    return ""


def normalize_giveback_trade(trade):
    if not isinstance(trade, dict):
        return None, ["trade_not_dict"]
    flags = []
    if str(trade.get("execution_mode") or "").lower() != "live":
        flags.append("not_live")
    if trade.get("owner", "bot") != "bot":
        flags.append("not_bot_owned")
    if trade.get("canary_enabled_at_open") is not True:
        flags.append("not_canary")
    trade_id = _trade_identity(trade)
    if not trade_id:
        flags.append("missing_authoritative_identity")
    side = str(trade.get("side") or "").upper()
    if side not in ("LONG", "SHORT"):
        flags.append("invalid_side")
    entry = _safe_float(trade.get("entry_real") or trade.get("exchange_fill_price") or trade.get("entry"))
    if entry is None or entry <= 0:
        flags.append("missing_actual_entry")
    initial_sl = _safe_float(
        trade.get("giveback_initial_confirmed_sl")
        or trade.get("initial_confirmed_sl")
        or trade.get("exchange_initial_sl_price_confirmed")
        or trade.get("exchange_sl_price_confirmed")
        or trade.get("sl_init")
    )
    if initial_sl is None or initial_sl <= 0:
        flags.append("missing_initial_confirmed_sl")
    risk = abs(entry - initial_sl) if entry is not None and initial_sl is not None else None
    if risk is None or risk <= 0:
        flags.append("invalid_initial_risk_distance")
    if flags:
        blocking = {
            "not_live", "not_bot_owned", "not_canary", "missing_authoritative_identity",
            "invalid_side", "missing_actual_entry", "missing_initial_confirmed_sl",
            "invalid_initial_risk_distance",
        }
        if any(flag in blocking for flag in flags):
            return None, flags
    return {
        "giveback_trade_id": trade_id,
        "symbol": trade.get("symbol"),
        "side": side,
        "entry_type": trade.get("entry_type"),
        "execution_mode": "live",
        "canary_epoch": trade.get("canary_epoch"),
        "canary_candidate": trade.get("canary_candidate_id"),
        "opened_trade_id": trade.get("opened_trade_id") or trade.get("id") or trade.get("trade_id"),
        "client_order_id": trade.get("client_order_id"),
        "dedup_key": trade.get("dedup_key"),
        "opened_ts": _safe_float(trade.get("time") or trade.get("entry_time")),
        "actual_entry": entry,
        "initial_confirmed_sl": initial_sl,
        "initial_risk_distance": risk,
    }, flags


def compute_current_r(side, actual_entry, initial_risk_distance, current_price):
    entry = _safe_float(actual_entry)
    risk = _safe_float(initial_risk_distance)
    price = _safe_float(current_price)
    side = str(side or "").upper()
    if entry is None or risk is None or risk <= 0 or price is None:
        return None
    if side == "LONG":
        return (price - entry) / risk
    if side == "SHORT":
        return (entry - price) / risk
    return None


def _locked_r(side, entry, risk, sl_price):
    sl = _safe_float(sl_price)
    if sl is None:
        return None
    return compute_current_r(side, entry, risk, sl)


def _hypothetical_stop_price(record, lock_r):
    if lock_r is None:
        return None
    if record["side"] == "LONG":
        return record["actual_entry"] + record["initial_risk_distance"] * lock_r
    return record["actual_entry"] - record["initial_risk_distance"] * lock_r


def _new_record(norm, now_ts):
    return {
        **norm,
        "peak_r": None,
        "peak_r_ts": None,
        "first_hit_0_5r_ts": None,
        "first_hit_1_0r_ts": None,
        "first_hit_1_5r_ts": None,
        "first_hit_2_0r_ts": None,
        "max_locked_r_actual": None,
        "max_locked_r_ts": None,
        "management_action_count": 0,
        "last_management_action": None,
        "last_management_action_ts": None,
        "v5_armed": False,
        "v5_armed_ts": None,
        "v5_peak_r": None,
        "v5_lock_r": None,
        "v5_hypothetical_stop_price": None,
        "v5_lock_update_count": 0,
        "v5_would_trigger_observed": False,
        "v5_trigger_observed_ts": None,
        "v5_trigger_observed_price": None,
        "v5_counterfactual_r_conservative": None,
        "v5_trigger_certainty": "NOT_TRIGGERED",
        "observation_count": 0,
        "last_update_ts": now_ts,
        "last_observation_emit_ts": None,
        "last_emitted_peak_r": None,
        "last_emitted_v5_lock_r": None,
    }


def update_giveback_peak(record, current_r, observation_ts):
    changed = False
    if current_r is None:
        return changed
    if record.get("peak_r") is None or current_r > float(record.get("peak_r")):
        record["peak_r"] = current_r
        record["peak_r_ts"] = observation_ts
        changed = True
    for level, key in (
        (0.5, "first_hit_0_5r_ts"),
        (1.0, "first_hit_1_0r_ts"),
        (1.5, "first_hit_1_5r_ts"),
        (2.0, "first_hit_2_0r_ts"),
    ):
        if current_r >= level and not record.get(key):
            record[key] = observation_ts
            changed = True
    return changed


def evaluate_v5_counterfactual(record, current_r, current_price, observation_ts, previous_peak=None, previous_lock=None):
    peak = _safe_float(record.get("peak_r"))
    if peak is None:
        record["v5_trigger_certainty"] = "DATA_MISSING"
        return False
    changed = False
    if peak >= 1.5 and not record.get("v5_armed"):
        record["v5_armed"] = True
        record["v5_armed_ts"] = observation_ts
        changed = True
    if record.get("v5_armed"):
        record["v5_peak_r"] = max(_safe_float(record.get("v5_peak_r"), peak), peak)
        proposed_lock = record["v5_peak_r"] - 1.0
        old_lock = _safe_float(record.get("v5_lock_r"))
        if old_lock is None or proposed_lock > old_lock:
            record["v5_lock_r"] = proposed_lock
            record["v5_hypothetical_stop_price"] = _hypothetical_stop_price(record, proposed_lock)
            record["v5_lock_update_count"] = int(record.get("v5_lock_update_count") or 0) + 1
            changed = True
    lock = _safe_float(record.get("v5_lock_r"))
    if not record.get("v5_armed") or lock is None:
        record["v5_trigger_certainty"] = "NOT_TRIGGERED"
        return changed
    if record.get("v5_would_trigger_observed"):
        return changed
    if current_r is None:
        record["v5_trigger_certainty"] = "DATA_MISSING"
        return changed
    crossed = current_r <= lock
    if not crossed:
        record["v5_trigger_certainty"] = "NOT_TRIGGERED"
        return changed
    if previous_lock is not None and current_r <= previous_lock:
        certainty = "CONSERVATIVE_CROSSING"
    elif previous_peak is not None and peak > previous_peak:
        certainty = "AMBIGUOUS_SAME_OBSERVATION"
    else:
        certainty = "CONSERVATIVE_CROSSING"
    record["v5_would_trigger_observed"] = True
    record["v5_trigger_observed_ts"] = observation_ts
    record["v5_trigger_observed_price"] = current_price
    record["v5_trigger_certainty"] = certainty
    if certainty in ("CONSERVATIVE_CROSSING", "CONFIRMED_BY_TICK_OR_MARK_SEQUENCE"):
        record["v5_counterfactual_r_conservative"] = lock - V5_SLIPPAGE_ASSUMPTION_R
    changed = True
    return changed


class GivebackShadowStore:
    def __init__(self, path=STATE_FILE, max_records=MAX_ACTIVE_RECORDS, max_bytes=MAX_STATE_BYTES, save_every=1):
        self.path = path
        self.max_records = max_records
        self.max_bytes = max_bytes
        self.save_every = max(1, int(save_every or 1))
        self._save_counter = 0
        self.state = {"schema_version": STATE_SCHEMA_VERSION, "active": {}, "terminal_ids": {}}
        self._loaded = False

    def load(self):
        if self._loaded:
            return self.state
        try:
            if not os.path.exists(self.path):
                self._loaded = True
                return self.state
            if os.path.getsize(self.path) > self.max_bytes:
                self.state = {"schema_version": STATE_SCHEMA_VERSION, "active": {}, "terminal_ids": {}}
                self._loaded = True
                return self.state
            with open(self.path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if not isinstance(data, dict):
                raise ValueError("state_not_object")
            if data.get("schema_version") != STATE_SCHEMA_VERSION:
                raise ValueError("state_schema_version_mismatch")
            active = data.get("active") if isinstance(data.get("active"), dict) else {}
            terminal = data.get("terminal_ids") if isinstance(data.get("terminal_ids"), dict) else {}
            self.state = {
                "schema_version": STATE_SCHEMA_VERSION,
                "active": dict(list(active.items())[-self.max_records:]),
                "terminal_ids": self._prune_terminal(terminal),
            }
        except Exception:
            self.state = {"schema_version": STATE_SCHEMA_VERSION, "active": {}, "terminal_ids": {}}
        self._loaded = True
        return self.state

    def _prune_terminal(self, terminal):
        now_ts = _now()
        items = [
            (str(k), _safe_float(v, now_ts))
            for k, v in (terminal or {}).items()
            if now_ts - _safe_float(v, now_ts) <= TERMINAL_TTL_SECS
        ]
        return dict(items[-MAX_TERMINAL_IDS:])

    def save(self, force=False):
        try:
            self._save_counter += 1
            if not force and self.save_every > 1 and self._save_counter % self.save_every != 0:
                return True
            active = self.state.get("active") or {}
            if len(active) > self.max_records:
                ordered = sorted(active.items(), key=lambda item: _safe_float(item[1].get("last_update_ts"), 0))
                self.state["active"] = dict(ordered[-self.max_records:])
            self.state["terminal_ids"] = self._prune_terminal(self.state.get("terminal_ids") or {})
            directory = os.path.dirname(os.path.abspath(self.path))
            os.makedirs(directory, exist_ok=True)
            tmp = f"{self.path}.tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(self.state, fh, separators=(",", ":"))
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except Exception:
            return False
        return True


def append_giveback_shadow_row(row, path=OBS_LOG):
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str) + "\n")


def _base_observation_row(record, action, current_price, current_r, observation_ts, actual_action, actual_sl_price, actual_locked_r, flags, source):
    peak = _safe_float(record.get("peak_r"))
    return {
        "schema_version": SCHEMA_VERSION,
        "logged_at": _iso(),
        "observation_ts": observation_ts,
        "action": action,
        "symbol": record.get("symbol"),
        "side": record.get("side"),
        "entry_type": record.get("entry_type"),
        "execution_mode": record.get("execution_mode"),
        "canary_epoch": record.get("canary_epoch"),
        "canary_candidate": record.get("canary_candidate"),
        "giveback_trade_id": record.get("giveback_trade_id"),
        "opened_trade_id": record.get("opened_trade_id"),
        "client_order_id": record.get("client_order_id"),
        "dedup_key": record.get("dedup_key"),
        "opened_ts": record.get("opened_ts"),
        "actual_entry": _round(record.get("actual_entry")),
        "initial_confirmed_sl": _round(record.get("initial_confirmed_sl")),
        "initial_risk_distance": _round(record.get("initial_risk_distance")),
        "current_price": _round(current_price),
        "current_r": _round(current_r, 4),
        "peak_r": _round(peak, 4),
        "peak_r_ts": record.get("peak_r_ts"),
        "provisional_giveback_r": None if peak is None or current_r is None else round(max(0.0, peak - current_r), 4),
        "max_locked_r_actual": _round(record.get("max_locked_r_actual"), 4),
        "max_locked_r_ts": record.get("max_locked_r_ts"),
        "first_hit_0_5r_ts": record.get("first_hit_0_5r_ts"),
        "first_hit_1_0r_ts": record.get("first_hit_1_0r_ts"),
        "first_hit_1_5r_ts": record.get("first_hit_1_5r_ts"),
        "first_hit_2_0r_ts": record.get("first_hit_2_0r_ts"),
        "observation_count": record.get("observation_count"),
        "actual_management_action": actual_action,
        "actual_sl_price": _round(actual_sl_price),
        "actual_locked_r": _round(actual_locked_r, 4),
        "v5_armed": _bool(record.get("v5_armed")),
        "v5_armed_ts": record.get("v5_armed_ts"),
        "v5_peak_r": _round(record.get("v5_peak_r"), 4),
        "v5_lock_r": _round(record.get("v5_lock_r"), 4),
        "v5_hypothetical_stop_price": _round(record.get("v5_hypothetical_stop_price")),
        "v5_lock_update_count": record.get("v5_lock_update_count"),
        "v5_would_trigger_observed": _bool(record.get("v5_would_trigger_observed")),
        "v5_trigger_observed_ts": record.get("v5_trigger_observed_ts"),
        "v5_trigger_certainty": record.get("v5_trigger_certainty") or "NOT_TRIGGERED",
        "v5_slippage_assumption_r": V5_SLIPPAGE_ASSUMPTION_R,
        "source": source,
        "data_quality_flags": list(flags or []),
    }


def assemble_giveback_observation(*args, **kwargs):
    return _base_observation_row(*args, **kwargs)


def _should_emit(record, action, previous):
    if action in ("initialization", "terminal_close", "v5_trigger_observed"):
        return True
    peak = _safe_float(record.get("peak_r"))
    last_peak = _safe_float(record.get("last_emitted_peak_r"))
    if peak is not None and (last_peak is None or peak - last_peak >= PEAK_EMIT_STEP_R):
        return True
    lock = _safe_float(record.get("v5_lock_r"))
    last_lock = _safe_float(record.get("last_emitted_v5_lock_r"))
    if lock is not None and (last_lock is None or lock - last_lock >= PEAK_EMIT_STEP_R):
        return True
    for key in ("first_hit_0_5r_ts", "first_hit_1_0r_ts", "first_hit_1_5r_ts", "first_hit_2_0r_ts", "v5_armed_ts"):
        if record.get(key) and not previous.get(key):
            return True
    old_locked = _safe_float(previous.get("max_locked_r_actual"))
    new_locked = _safe_float(record.get("max_locked_r_actual"))
    if new_locked is not None and (old_locked is None or new_locked > old_locked):
        return True
    last_emit = _safe_float(record.get("last_observation_emit_ts"), 0)
    return _now() - last_emit >= HEARTBEAT_SECS


def observe_giveback_trade(trade, current_price=None, observation_ts=None, actual_management_action=None,
                           actual_sl_price=None, source="live_management", store=None, log_path=OBS_LOG):
    store = store or GivebackShadowStore()
    state = store.load()
    norm, flags = normalize_giveback_trade(trade)
    if norm is None:
        return {"tracked": False, "reason": ",".join(flags)}
    observation_ts = observation_ts or _now()
    active = state.setdefault("active", {})
    trade_id = norm["giveback_trade_id"]
    initialized = trade_id not in active
    record = active.get(trade_id) or _new_record(norm, observation_ts)
    previous = dict(record)
    current_r = compute_current_r(record["side"], record["actual_entry"], record["initial_risk_distance"], current_price)
    observed_peak_r = _safe_float(trade.get("max_profit_r") or trade.get("max_r") or trade.get("mfe_r"))
    peak_input_r = current_r
    if observed_peak_r is not None and (peak_input_r is None or observed_peak_r > peak_input_r):
        peak_input_r = observed_peak_r
    record["observation_count"] = int(record.get("observation_count") or 0) + 1
    update_giveback_peak(record, peak_input_r, observation_ts)
    actual_locked = _locked_r(record["side"], record["actual_entry"], record["initial_risk_distance"], actual_sl_price)
    if actual_locked is not None:
        old_locked = _safe_float(record.get("max_locked_r_actual"))
        if old_locked is None or actual_locked > old_locked:
            record["max_locked_r_actual"] = actual_locked
            record["max_locked_r_ts"] = observation_ts
    if actual_management_action:
        record["management_action_count"] = int(record.get("management_action_count") or 0) + 1
        record["last_management_action"] = actual_management_action
        record["last_management_action_ts"] = observation_ts
    previous_peak = _safe_float(previous.get("peak_r"))
    previous_lock = _safe_float(previous.get("v5_lock_r"))
    evaluate_v5_counterfactual(record, current_r, current_price, observation_ts, previous_peak, previous_lock)
    record["last_update_ts"] = observation_ts
    active[trade_id] = record
    action = "initialization" if initialized else "observation"
    if record.get("v5_would_trigger_observed") and not previous.get("v5_would_trigger_observed"):
        action = "v5_trigger_observed"
    if _should_emit(record, action, previous):
        row = assemble_giveback_observation(
            record, action, current_price, current_r, observation_ts,
            actual_management_action, actual_sl_price, actual_locked, flags, source,
        )
        try:
            append_giveback_shadow_row(row, log_path)
            record["last_observation_emit_ts"] = _now()
            record["last_emitted_peak_r"] = record.get("peak_r")
            record["last_emitted_v5_lock_r"] = record.get("v5_lock_r")
        except Exception:
            flags.append("observation_log_write_failed")
    saved = store.save()
    return {"tracked": True, "trade_id": trade_id, "saved": saved}


def assemble_giveback_close_row(record, trade, flags, close_ts):
    realized_r = _safe_float(trade.get("rr_real") or trade.get("realized_r"))
    peak = _safe_float(record.get("peak_r"))
    actual_lock = _safe_float(record.get("max_locked_r_actual"))
    v5_cf = _safe_float(record.get("v5_counterfactual_r_conservative"))
    certainty = record.get("v5_trigger_certainty") or "NOT_TRIGGERED"
    if v5_cf is None or certainty == "AMBIGUOUS_SAME_OBSERVATION":
        v5_cf = realized_r
    capture = None if peak is None or peak <= 0 or realized_r is None else realized_r / peak
    return {
        "schema_version": CLOSE_SCHEMA_VERSION,
        "logged_at": _iso(),
        "close_ts": close_ts,
        "symbol": record.get("symbol"),
        "side": record.get("side"),
        "entry_type": record.get("entry_type"),
        "canary_epoch": record.get("canary_epoch"),
        "canary_candidate": record.get("canary_candidate"),
        "giveback_trade_id": record.get("giveback_trade_id"),
        "opened_trade_id": record.get("opened_trade_id"),
        "client_order_id": record.get("client_order_id"),
        "dedup_key": record.get("dedup_key"),
        "opened_ts": record.get("opened_ts"),
        "actual_entry": _round(record.get("actual_entry")),
        "actual_exit_price": _round(trade.get("exit_price") or trade.get("exchange_exit_price")),
        "initial_confirmed_sl": _round(record.get("initial_confirmed_sl")),
        "initial_risk_distance": _round(record.get("initial_risk_distance")),
        "realized_r": _round(realized_r, 4),
        "realized_pnl": _round(trade.get("realized_pnl") or trade.get("pnl")),
        "fees": _round(trade.get("fees") or trade.get("fee")),
        "close_reason": trade.get("close_reason") or trade.get("exit_type"),
        "peak_r": _round(peak, 4),
        "peak_r_ts": record.get("peak_r_ts"),
        "giveback_r": None if peak is None or realized_r is None else round(max(0.0, peak - realized_r), 4),
        "capture_ratio": _round(capture, 4),
        "max_locked_r_actual": _round(actual_lock, 4),
        "giveback_below_actual_lock_r": None if actual_lock is None or realized_r is None else round(max(0.0, actual_lock - realized_r), 4),
        "management_action_count": record.get("management_action_count"),
        "first_hit_0_5r_ts": record.get("first_hit_0_5r_ts"),
        "first_hit_1_0r_ts": record.get("first_hit_1_0r_ts"),
        "first_hit_1_5r_ts": record.get("first_hit_1_5r_ts"),
        "first_hit_2_0r_ts": record.get("first_hit_2_0r_ts"),
        "v5_armed": _bool(record.get("v5_armed")),
        "v5_armed_ts": record.get("v5_armed_ts"),
        "v5_peak_r": _round(record.get("v5_peak_r"), 4),
        "v5_final_lock_r": _round(record.get("v5_lock_r"), 4),
        "v5_would_trigger_observed": _bool(record.get("v5_would_trigger_observed")),
        "v5_trigger_observed_ts": record.get("v5_trigger_observed_ts"),
        "v5_trigger_certainty": certainty,
        "v5_counterfactual_r_conservative": _round(v5_cf, 4),
        "v5_delta_vs_actual_r": None if v5_cf is None or realized_r is None else round(v5_cf - realized_r, 4),
        "v5_slippage_assumption_r": V5_SLIPPAGE_ASSUMPTION_R,
        "data_quality_flags": list(flags or []),
    }


def finalize_giveback_trade(trade, close_ts=None, source="live_close", store=None, close_log_path=CLOSE_LOG, observation_log_path=OBS_LOG):
    store = store or GivebackShadowStore()
    state = store.load()
    norm, flags = normalize_giveback_trade(trade)
    if norm is None and "missing_authoritative_identity" in flags:
        return {"finalized": False, "reason": ",".join(flags)}
    trade_id = (norm or {}).get("giveback_trade_id") or _trade_identity(trade)
    if not trade_id:
        return {"finalized": False, "reason": "missing_authoritative_identity"}
    terminal = state.setdefault("terminal_ids", {})
    if trade_id in terminal:
        return {"finalized": False, "idempotent": True, "reason": "terminal_already_logged"}
    active = state.setdefault("active", {})
    record = active.get(trade_id)
    if record is None:
        if norm is None:
            return {"finalized": False, "reason": ",".join(flags)}
        flags = list(flags) + ["missing_active_state_terminal_reconstructed"]
        record = _new_record(norm, close_ts or _now())
    close_ts = close_ts or _safe_float(trade.get("close_ts") or trade.get("close_time")) or _now()
    row = assemble_giveback_close_row(record, trade, flags, close_ts)
    try:
        append_giveback_shadow_row(row, close_log_path)
    except Exception:
        return {
            "finalized": False,
            "trade_id": trade_id,
            "reason": "terminal_log_write_failed",
            "saved": False,
        }

    terminal[trade_id] = close_ts
    active.pop(trade_id, None)
    saved = store.save(force=True)
    warnings = []
    if not saved:
        warnings.append("terminal_marker_save_failed_crash_window_at_least_once")

    obs_row = _base_observation_row(
        record, "terminal_close", trade.get("exit_price") or trade.get("exchange_exit_price"),
        row.get("realized_r"), close_ts, record.get("last_management_action"),
        trade.get("sl"), record.get("max_locked_r_actual"), flags, source,
    )
    try:
        append_giveback_shadow_row(obs_row, observation_log_path)
    except Exception:
        flags.append("terminal_observation_log_write_failed")
    out = {"finalized": True, "trade_id": trade_id, "saved": saved}
    if warnings:
        out["warnings"] = warnings
        out["crash_window_residual_risk"] = "close_row_appended_but_terminal_marker_not_durably_saved"
    return out
