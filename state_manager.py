import os
import time
import csv
import json
import numpy as np
import traceback
import threading
import copy
from datetime import datetime
from notifier import format_vn_time
from helper import _should_log, ensure_columns
from execution_mode import TRADES_CSV, STATE_FILE
from config import RISK_PER_TRADE, config
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
_STATE_WRITE_LOCK = threading.Lock()
_STATE_SAVE_SUCCESS_THROTTLE = {}
_STATE_SAVE_SUCCESS_THROTTLE_SECS = 60.0

TRADE_CSV_HEADERS = [
    "id","open_time","close_time","symbol","type","side",
    "entry","sl","tp","exit_price",
    "rr","max_r","status","exit_type",
    "entry_type","bos_type","retest_strength","market_mode",
    "wyckoff_name","wyckoff_strength","trap_score","trap_valid",
    "core","confirm","score",
    "volume_ok","volume_spike","exhaustion","exhaustion_score",
    "sl_reason","reason",
    "priority_final","compression_score",
    "phase","market_state","impulse",
    "bias_type","is_scale_in",
    "cont_score",
    "giveback_r","trade_age_minutes","time_to_1r",
    "time_spent_above_1r","trailing_phase_at_exit","max_r_after_partial",
    "signal_created_ts",
    "exchange_fill_price","entry_source","entry_price_unconfirmed","rr_unconfirmed",
    "canary_epoch","canary_candidate_id","canary_open_sequence","canary_enabled_at_open",
    "close_ts","closed_at_unix",
    "terminal_key",
]

CANARY_STATE_FILE = os.path.join(BASE_DIR, "canary_state.json")
CANARY_SUPPORTED_CANDIDATES = {"INCUMBENT_LIVE_CONFIRM"}
CANARY_DEFAULT_CONFIG = {
    "canary_enabled": False,
    "canary_epoch": "",
    "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM",
    "canary_max_open": 2,
    "canary_max_total_trades": 50,
    "canary_max_cum_loss_r": -8.0,
    "canary_max_consecutive_losses": 5,
}
CANARY_DEFAULT_STATE = {
    "schema_version": 1,
    "canary_epoch": "",
    "candidate_id": "INCUMBENT_LIVE_CONFIRM",
    "opened_total": 0,
    "closed_total": 0,
    "cum_realized_r": 0.0,
    "consecutive_losses": 0,
    "abort_latched": False,
    "abort_reason": "",
    "abort_ts": None,
    "opened_ids": [],
    "counted_close_ids": [],
}

CANARY_ACTUAL_LANE_ENTRY_TYPE = "CONFIRM_SMC_RESEARCH"
_CANARY_RUNTIME_ABORT_REASON = ""

def log_path(filename):
    return os.path.join(LOG_DIR, filename)

def write_runtime_error(context, tb_str):
    try:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(log_path("runtime_errors.log"), "a", encoding="utf-8") as f:
            f.write(f"\n[{ts}] [{context}]\n{tb_str}\n")
    except Exception:
        pass

def _state_backup_path(path):
    return path + ".bak"

def _state_temp_path(path, suffix="tmp"):
    return f"{path}.{suffix}.{os.getpid()}.{time.time_ns()}"

def _replace_with_permission_retry(src, dst):
    delays = [0.1, 0.2, 0.4, 0.8]
    for attempt in range(5):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if attempt == 4:
                raise
            time.sleep(delays[attempt])

def _state_save_summary(data):
    if isinstance(data, list):
        symbols = sorted(
            str(t.get("symbol"))
            for t in data
            if isinstance(t, dict) and t.get("symbol")
        )
        return len(data), symbols
    return None, []

def _state_save_debug_enabled():
    return bool(config.get("state_save_telemetry_debug", False))

def _should_print_state_save_success(basename, trade_count, symbols):
    now = time.time()
    snapshot = (trade_count, tuple(symbols))
    previous = _STATE_SAVE_SUCCESS_THROTTLE.get(basename)
    if (
        previous is None
        or previous.get("snapshot") != snapshot
        or now - previous.get("printed_at", 0.0) >= _STATE_SAVE_SUCCESS_THROTTLE_SECS
    ):
        _STATE_SAVE_SUCCESS_THROTTLE[basename] = {
            "snapshot": snapshot,
            "printed_at": now,
        }
        return True
    return False

def _load_json_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _load_json_with_permission_retry(path):
    """
    Read JSON with bounded retry on Windows transient PermissionError.

    Windows raises PermissionError (Errno 13) when a second process or an
    AV/sync/editor briefly holds the file open during another process's
    os.replace.  We retry the *read* only on PermissionError; any other error
    propagates immediately.  Behavior on a readable file is identical to
    _load_json_file (same parse, same return value).
    """
    delays = [0.05, 0.1, 0.2, 0.4, 0.8]
    for attempt in range(5):
        try:
            return _load_json_file(path)
        except PermissionError:
            if attempt == 4:
                raise
            print(
                f"[STATE] read PermissionError on {os.path.basename(path)} "
                f"(attempt {attempt + 1}/5), retrying in {delays[attempt]}s"
            )
            time.sleep(delays[attempt])

def _load_trade_state_file(path):
    data = _load_json_file(path)
    if not isinstance(data, list):
        raise ValueError(f"{path} top-level JSON must be a list, got {type(data).__name__}")
    return data

def _notify_state_warning(message):
    print(message)
    try:
        from telegram import send_telegram
        send_telegram(message)
    except Exception:
        pass

def _convert_numpy(o):
    """
    [FIX] json.dump fallback converter cho numpy types.
    Dùng trong default= của json.dump.
    Covers: bool_ / int_ / float_ / ndarray / generic
    """
    if isinstance(o, np.bool_):
        return bool(o)
    if isinstance(o, (np.integer,)):       # int8, int16, int32, int64, uint*
        return int(o)
    if isinstance(o, (np.floating,)):      # float16, float32, float64
        return float(o)
    if isinstance(o, np.ndarray):          # array lọt vào → list
        return o.tolist()
    raise TypeError(f"[JSON] Object of type {type(o).__name__} is not JSON serializable")

def atomic_save_json(data, state_file):
    """
    Crash-hardened JSON persistence for state files.
    """
    path = state_file
    directory = os.path.dirname(os.path.abspath(path))
    basename = os.path.basename(path)
    trade_count, symbols = _state_save_summary(data)
    os.makedirs(directory, exist_ok=True)
    temp_file = None
    bak_file = _state_backup_path(path)
    bak_temp = None

    with _STATE_WRITE_LOCK:
        try:
            if _state_save_debug_enabled():
                print(f"[STATE] state_save_start file={basename} count={trade_count} symbols={symbols}")
            temp_file = _state_temp_path(path)
            serialized = json.dumps(data, indent=2, ensure_ascii=False, default=_convert_numpy)
            json.loads(serialized)

            with open(temp_file, "w", encoding="utf-8") as f:
                f.write(serialized)
                f.flush()
                os.fsync(f.fileno())

            _load_json_with_permission_retry(temp_file)

            if os.path.exists(path):
                try:
                    current = _load_json_with_permission_retry(path)
                    bak_temp = _state_temp_path(bak_file)
                    with open(bak_temp, "w", encoding="utf-8") as f:
                        json.dump(current, f, indent=2, ensure_ascii=False, default=_convert_numpy)
                        f.flush()
                        os.fsync(f.fileno())
                    _load_json_with_permission_retry(bak_temp)
                    _replace_with_permission_retry(bak_temp, bak_file)
                    bak_temp = None
                except PermissionError:
                    raise
                except Exception as e:
                    print(f"[STATE] Skipping backup refresh for {path}: current file is not valid JSON ({e})")

            _replace_with_permission_retry(temp_file, path)
            temp_file = None
            _load_json_with_permission_retry(path)
            if _should_print_state_save_success(basename, trade_count, symbols):
                print(f"[STATE] state_save_success file={basename} count={trade_count} symbols={symbols}")
        except Exception:
            print(f"[STATE] state_save_fail file={basename} count={trade_count} symbols={symbols}")
            write_runtime_error("STATE/atomic_save_json", traceback.format_exc())
            for candidate in (temp_file, bak_temp):
                if candidate:
                    try:
                        if os.path.exists(candidate):
                            os.remove(candidate)
                    except Exception:
                        pass
            raise

def _load_hot_canary_config(config_path="config.json"):
    cfg = dict(CANARY_DEFAULT_CONFIG)
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            disk = json.load(f)
        if isinstance(disk, dict):
            cfg.update(disk)
            for key, value in CANARY_DEFAULT_CONFIG.items():
                cfg.setdefault(key, value)
    except FileNotFoundError:
        pass
    return cfg

def _canary_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _canary_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def canary_fresh_state():
    return copy.deepcopy(CANARY_DEFAULT_STATE)

def canary_config_status(config_path="config.json"):
    cfg = _load_hot_canary_config(config_path)
    enabled = bool(cfg.get("canary_enabled", False))
    candidate_id = str(cfg.get("canary_candidate_id") or "").strip()
    epoch = str(cfg.get("canary_epoch") or "").strip()
    max_open = _canary_int(cfg.get("canary_max_open"), -1)
    max_total = _canary_int(cfg.get("canary_max_total_trades"), -1)
    errors = []
    if enabled:
        if not epoch:
            errors.append("canary_epoch_empty")
        if candidate_id != "INCUMBENT_LIVE_CONFIRM":
            errors.append(f"canary_candidate_id_not_actual_lane:{candidate_id}")
        elif candidate_id not in CANARY_SUPPORTED_CANDIDATES:
            errors.append(f"unsupported_candidate_id:{candidate_id}")
        if max_open != 2:
            errors.append(f"canary_max_open_must_be_2:{max_open}")
        if max_total not in (50, 75) or max_total <= 0:
            errors.append(f"canary_max_total_trades_must_be_50_or_75:{max_total}")
        if not bool(cfg.get("live_smc_research_enabled", False)):
            errors.append("live_smc_research_enabled_false")
        if not bool(cfg.get("live_mode", False)):
            errors.append("live_mode_false")
        exec_mode = str(cfg.get("execution_mode") or "").strip().lower()
        if exec_mode not in {"paper_live", "live"}:
            errors.append(f"execution_mode_not_live:{exec_mode or 'missing'}")
    return {
        "enabled": enabled,
        "epoch": epoch,
        "candidate_id": candidate_id,
        "max_open": max_open,
        "max_total": max_total,
        "max_cum_loss_r": _canary_float(cfg.get("canary_max_cum_loss_r"), -8.0),
        "max_consecutive_losses": _canary_int(cfg.get("canary_max_consecutive_losses"), 5),
        "errors": errors,
    }

def canary_candidate_id_for_trade(t):
    if not isinstance(t, dict):
        return ""
    if str(t.get("entry_type") or "").upper() == CANARY_ACTUAL_LANE_ENTRY_TYPE:
        return "INCUMBENT_LIVE_CONFIRM"
    return ""

def is_canary_incumbent_candidate(t):
    return canary_candidate_id_for_trade(t) == "INCUMBENT_LIVE_CONFIRM"

def canary_load_state(state_file=None):
    path = state_file or CANARY_STATE_FILE
    data = _load_json_with_permission_retry(path)
    if not isinstance(data, dict):
        raise ValueError("canary_state top-level JSON must be an object")
    return data

def _canary_validate_state_shape(state):
    if not isinstance(state, dict):
        return ["state_not_object"]
    errors = []
    expected_types = {
        "schema_version": int,
        "canary_epoch": str,
        "candidate_id": str,
        "opened_total": int,
        "closed_total": int,
        "cum_realized_r": (int, float),
        "consecutive_losses": int,
        "abort_latched": bool,
        "abort_reason": str,
        "opened_ids": list,
        "counted_close_ids": list,
    }
    for key, expected in expected_types.items():
        if key not in state:
            errors.append(f"missing:{key}")
        elif not isinstance(state.get(key), expected):
            errors.append(f"invalid_type:{key}")
    if "abort_ts" not in state:
        errors.append("missing:abort_ts")
    if state.get("schema_version") != 1:
        errors.append("schema_version_not_1")
    opened_ids = state.get("opened_ids") if isinstance(state.get("opened_ids"), list) else []
    close_ids = state.get("counted_close_ids") if isinstance(state.get("counted_close_ids"), list) else []
    if len(opened_ids) != len(set(map(str, opened_ids))):
        errors.append("duplicate_opened_ids")
    if len(close_ids) != len(set(map(str, close_ids))):
        errors.append("duplicate_counted_close_ids")
    opened_total = state.get("opened_total")
    closed_total = state.get("closed_total")
    if isinstance(opened_total, int) and isinstance(opened_ids, list) and opened_total != len(opened_ids):
        errors.append("opened_total_opened_ids_mismatch")
    if isinstance(closed_total, int) and isinstance(close_ids, list) and closed_total != len(close_ids):
        errors.append("closed_total_counted_close_ids_mismatch")
    if isinstance(opened_total, int) and isinstance(closed_total, int):
        if opened_total < 0 or closed_total < 0 or closed_total > opened_total:
            errors.append("invalid_open_close_totals")
    return errors

def _canary_trade_open_count(open_trades, status, exclude_trade=None):
    epoch = status.get("epoch")
    candidate_id = status.get("candidate_id")
    count = 0
    for trade in open_trades or []:
        if trade is exclude_trade:
            continue
        if not isinstance(trade, dict):
            continue
        if str(trade.get("status", "OPEN")).upper() != "OPEN":
            continue
        if trade.get("owner", "bot") != "bot":
            continue
        if (
            trade.get("canary_enabled_at_open") is True
            and str(trade.get("canary_epoch") or "") == epoch
            and str(trade.get("canary_candidate_id") or "") == candidate_id
        ):
            count += 1
    return count

def _canary_open_epoch_conflict(open_trades, status):
    expected_epoch = status.get("epoch")
    for trade in open_trades or []:
        if not isinstance(trade, dict):
            continue
        if str(trade.get("status", "OPEN")).upper() != "OPEN":
            continue
        if trade.get("canary_enabled_at_open") is not True:
            continue
        if str(trade.get("canary_epoch") or "") != expected_epoch:
            return True
    return False

def canary_latch(reason, state_file=None, config_path="config.json"):
    global _CANARY_RUNTIME_ABORT_REASON
    status = canary_config_status(config_path)
    try:
        state = canary_load_state(state_file)
    except Exception as exc:
        state = canary_fresh_state()
        state["canary_epoch"] = status.get("epoch", "")
        state["candidate_id"] = status.get("candidate_id", "INCUMBENT_LIVE_CONFIRM")
        state["abort_latched"] = True
        state["abort_reason"] = f"canary_state_unreadable_latch_not_persisted:{type(exc).__name__}:{reason or 'canary_abort'}"
        state["abort_ts"] = time.time()
        _CANARY_RUNTIME_ABORT_REASON = state["abort_reason"]
        write_runtime_error("CANARY/canary_latch_state_unreadable", traceback.format_exc())
        return state
    state["abort_latched"] = True
    state["abort_reason"] = str(reason or "canary_abort")
    state["abort_ts"] = time.time()
    try:
        atomic_save_json(state, state_file or CANARY_STATE_FILE)
    except Exception:
        _CANARY_RUNTIME_ABORT_REASON = state["abort_reason"]
        write_runtime_error("CANARY/canary_latch_persist_failed", traceback.format_exc())
        return state
    return state

def canary_preflight_open(t, open_trades=None, state_file=None, config_path="config.json", exclude_trade=None):
    status = canary_config_status(config_path)
    if not status["enabled"]:
        return {"ok": False, "enabled": False, "reason": "canary_disabled", "status": status, "state": None}
    if status["errors"]:
        return {"ok": False, "enabled": True, "reason": "canary_config_invalid:" + ",".join(status["errors"]), "status": status, "state": None}
    if _CANARY_RUNTIME_ABORT_REASON:
        return {"ok": False, "enabled": True, "reason": f"canary_runtime_abort_latched:{_CANARY_RUNTIME_ABORT_REASON}", "status": status, "state": None}
    if canary_candidate_id_for_trade(t) != status["candidate_id"]:
        return {"ok": False, "enabled": True, "reason": "canary_candidate_not_supported", "status": status, "state": None}
    try:
        state = canary_load_state(state_file)
    except Exception as exc:
        state = canary_fresh_state()
        state["abort_latched"] = True
        state["abort_reason"] = f"canary_state_unavailable:{type(exc).__name__}"
        state["abort_ts"] = time.time()
        return {"ok": False, "enabled": True, "reason": f"canary_state_unavailable:{type(exc).__name__}", "status": status, "state": state}
    state_errors = _canary_validate_state_shape(state)
    if state_errors:
        if isinstance(state, dict):
            state["abort_latched"] = True
            state["abort_reason"] = "canary_state_invalid:" + ",".join(state_errors)
            state["abort_ts"] = time.time()
        return {"ok": False, "enabled": True, "reason": "canary_state_invalid:" + ",".join(state_errors), "status": status, "state": state}
    if state.get("canary_epoch") != status["epoch"] or state.get("candidate_id") != status["candidate_id"]:
        return {"ok": False, "enabled": True, "reason": "canary_state_epoch_or_candidate_mismatch", "status": status, "state": state}
    if state.get("abort_latched"):
        return {"ok": False, "enabled": True, "reason": f"canary_abort_latched:{state.get('abort_reason')}", "status": status, "state": state}
    if _canary_open_epoch_conflict(open_trades, status):
        canary_latch("canary_epoch_inconsistency_with_open_trade", state_file=state_file, config_path=config_path)
        return {"ok": False, "enabled": True, "reason": "canary_epoch_inconsistency_latched", "status": status, "state": state}
    if state.get("opened_total", 0) >= status["max_total"]:
        return {"ok": False, "enabled": True, "reason": "canary_total_cap_reached", "status": status, "state": state}
    open_count = _canary_trade_open_count(open_trades, status, exclude_trade=exclude_trade)
    if open_count >= status["max_open"]:
        return {"ok": False, "enabled": True, "reason": "canary_max_open_reached", "status": status, "state": state}
    return {"ok": True, "enabled": True, "reason": "", "status": status, "state": state, "open_count": open_count}

def canary_attach_open_attribution(t, preflight):
    if not (isinstance(t, dict) and isinstance(preflight, dict) and preflight.get("ok")):
        return t
    status = preflight["status"]
    state = preflight["state"]
    t["canary_epoch"] = status["epoch"]
    t["canary_candidate_id"] = status["candidate_id"]
    t["canary_open_sequence"] = int(state.get("opened_total", 0)) + 1
    t["canary_enabled_at_open"] = True
    return t

def _canary_trade_open_id(t):
    for key in ("client_order_id", "exchange_client_id"):
        value = str(t.get(key) or "").strip()
        if value.startswith("BOT_"):
            return value
    return ""

def canary_record_confirmed_open(t, open_trades=None, state_file=None, config_path="config.json"):
    status = canary_config_status(config_path)
    if not status["enabled"]:
        return {"recorded": False, "reason": "canary_disabled"}
    open_id = _canary_trade_open_id(t)
    if open_id:
        try:
            existing_state = canary_load_state(state_file)
            existing_errors = _canary_validate_state_shape(existing_state)
            if not existing_errors and open_id in set(map(str, existing_state.get("opened_ids", []))):
                try:
                    t["canary_open_sequence"] = list(map(str, existing_state.get("opened_ids", []))).index(open_id) + 1
                except ValueError:
                    pass
                return {
                    "recorded": False,
                    "idempotent": True,
                    "reason": "canary_open_already_counted",
                    "state": existing_state,
                }
        except Exception:
            pass
    preflight = canary_preflight_open(
        t,
        open_trades=open_trades,
        state_file=state_file,
        config_path=config_path,
        exclude_trade=t,
    )
    if not preflight.get("ok"):
        return {"recorded": False, "reason": preflight.get("reason")}
    if not open_id:
        canary_latch("canary_open_missing_bot_client_order_id", state_file=state_file, config_path=config_path)
        return {"recorded": False, "reason": "canary_open_missing_bot_client_order_id"}
    state = preflight["state"]
    if state.get("opened_total", 0) >= preflight["status"]["max_total"]:
        return {"recorded": False, "reason": "canary_total_cap_reached"}
    state["opened_ids"].append(open_id)
    state["opened_total"] = int(state.get("opened_total", 0)) + 1
    t["canary_open_sequence"] = state["opened_total"]
    t["canary_epoch"] = preflight["status"]["epoch"]
    t["canary_candidate_id"] = preflight["status"]["candidate_id"]
    t["canary_enabled_at_open"] = True
    atomic_save_json(state, state_file or CANARY_STATE_FILE)
    return {"recorded": True, "reason": "", "state": state}

def canary_record_close(t, state_file=None, config_path="config.json"):
    if not isinstance(t, dict) or t.get("canary_enabled_at_open") is not True:
        return {"recorded": False, "reason": "not_canary_trade"}
    status = canary_config_status(config_path)
    if str(t.get("canary_epoch") or "") != status.get("epoch") or str(t.get("canary_candidate_id") or "") != status.get("candidate_id"):
        return {"recorded": False, "reason": "canary_close_epoch_or_candidate_mismatch"}
    try:
        state = canary_load_state(state_file)
    except Exception as exc:
        return {"recorded": False, "reason": f"canary_state_unavailable:{type(exc).__name__}"}
    state_errors = _canary_validate_state_shape(state)
    if state_errors:
        return {"recorded": False, "reason": "canary_state_invalid:" + ",".join(state_errors)}
    close_id = _canary_trade_open_id(t)
    if not close_id:
        return {"recorded": False, "reason": "canary_close_missing_bot_client_order_id"}
    if close_id in set(map(str, state.get("counted_close_ids", []))):
        return {"recorded": False, "idempotent": True, "reason": "canary_close_already_counted", "state": state}
    if close_id not in set(map(str, state.get("opened_ids", []))):
        return {"recorded": False, "reason": "canary_close_without_counted_open"}
    rr = _canary_float(t.get("rr_real"), 0.0)
    state["counted_close_ids"].append(close_id)
    state["closed_total"] = int(state.get("closed_total", 0)) + 1
    state["cum_realized_r"] = round(float(state.get("cum_realized_r", 0.0)) + rr, 10)
    state["consecutive_losses"] = int(state.get("consecutive_losses", 0)) + 1 if rr < 0 else 0
    if state["cum_realized_r"] <= status.get("max_cum_loss_r", -8.0):
        state["abort_latched"] = True
        state["abort_reason"] = "canary_max_cum_loss_r_reached"
        state["abort_ts"] = time.time()
    elif state["consecutive_losses"] >= status.get("max_consecutive_losses", 5):
        state["abort_latched"] = True
        state["abort_reason"] = "canary_max_consecutive_losses_reached"
        state["abort_ts"] = time.time()
    atomic_save_json(state, state_file or CANARY_STATE_FILE)
    return {"recorded": True, "reason": "", "state": state}

def _state_kind(path):
    name = os.path.basename(path).lower()
    if name == "paper_state.json":
        return "paper"
    if name == "live_state.json":
        return "live"
    if name == "testnet_state.json":
        return "testnet"
    return "unknown"

def _normalize_trade_list(data):
    for t in data:
        normalize_trade_schema(t)
    return data

def _paper_empty_recovery_allowed():
    csv_path = "paper_trades.csv"
    if not os.path.exists(csv_path):
        return False, "paper_trades.csv missing"
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        open_like = [
            r for r in rows
            if r.get("status") == "OPEN" or not (r.get("close_time") or "").strip()
        ]
        if open_like:
            return False, f"paper_trades.csv has {len(open_like)} open/blank-close row(s)"
        return True, "paper CSV has no open/blank-close rows"
    except Exception as e:
        return False, f"paper_trades.csv read failed: {e}"

def _is_bot_client_id(value):
    return isinstance(value, str) and value.startswith("BOT_")

def _row_has_bot_client_id(row):
    if not isinstance(row, dict):
        return False
    return any(
        _is_bot_client_id(row.get(k))
        for k in ("clientOrderId", "origClientOrderId", "clientAlgoId", "newClientOrderId")
    )

def _load_live_exchange_rows():
    from exchange import live_executor

    positions_raw = live_executor._get_signed("/fapi/v2/positionRisk", {})
    if positions_raw is None or not isinstance(positions_raw, list):
        raise RuntimeError("LIVE positionRisk query failed or returned unexpected payload")

    open_orders = live_executor._get_signed("/fapi/v1/openOrders", {})
    if open_orders is None or not isinstance(open_orders, list):
        raise RuntimeError("LIVE openOrders query failed or returned unexpected payload")

    open_algo = live_executor._get_signed("/fapi/v1/openAlgoOrders", {})
    if open_algo is None or not isinstance(open_algo, list):
        raise RuntimeError("LIVE openAlgoOrders query failed or returned unexpected payload")

    positions = []
    for pos in positions_raw:
        try:
            amt = float(pos.get("positionAmt", 0))
        except (TypeError, ValueError):
            continue
        if amt != 0.0:
            positions.append(pos)

    order_history = {}
    for pos in positions:
        symbol = pos.get("symbol")
        if not symbol:
            continue
        rows = live_executor._get_signed("/fapi/v1/allOrders", {"symbol": symbol, "limit": 50})
        if rows is None or not isinstance(rows, list):
            raise RuntimeError(f"LIVE allOrders query failed for {symbol}")
        order_history[symbol] = rows

    return positions, open_orders, open_algo, order_history

def _live_csv_open_rows():
    csv_path = "live_trades.csv"
    if not os.path.exists(csv_path):
        return []
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return [
        r for r in rows
        if r.get("status") == "OPEN" or not (r.get("close_time") or "").strip()
    ]

def _recover_live_state_from_exchange(state_file):
    positions, open_orders, open_algo, order_history = _load_live_exchange_rows()
    live_csv_open = _live_csv_open_rows()

    bot_open_orders = [o for o in open_orders if _row_has_bot_client_id(o)]
    bot_algo_orders = [o for o in open_algo if _row_has_bot_client_id(o)]
    bot_history = [
        row
        for rows in order_history.values()
        for row in rows
        if _row_has_bot_client_id(row)
    ]

    if bot_open_orders or bot_algo_orders or bot_history:
        summary = (
            f"bot_open_orders={len(bot_open_orders)} "
            f"bot_algo_orders={len(bot_algo_orders)} "
            f"bot_recent_history={len(bot_history)}"
        )
        raise RuntimeError(
            "[CRITICAL] LIVE state primary and backup are invalid, and BOT_ "
            f"exchange ownership evidence exists ({summary}). Manual reconstruction is required."
        )

    if live_csv_open and (positions or open_orders or open_algo):
        raise RuntimeError(
            "[CRITICAL] LIVE state primary and backup are invalid, exchange has exposure, "
            f"and live_trades.csv has {len(live_csv_open)} open/blank-close row(s). "
            "Manual reconciliation is required."
        )

    if positions or open_orders or open_algo:
        pos_symbols = ",".join(sorted({p.get("symbol", "?") for p in positions}))
        _notify_state_warning(
            "[STATE RECOVERY] LIVE exchange has manual/uncertain exposure but no BOT_ "
            f"ownership evidence. Restoring live_state.json as empty so bot will not manage it. "
            f"positions={len(positions)} symbols={pos_symbols or '-'} "
            f"open_orders={len(open_orders)} open_algo_orders={len(open_algo)}"
        )
    else:
        _notify_state_warning(
            "[STATE RECOVERY] LIVE exchange has no open positions/orders with BOT_ evidence. "
            "Restoring live_state.json as empty."
        )

    atomic_save_json([], state_file)
    return []

def load_open_trades(state_file=None):
    _file = state_file if state_file is not None else STATE_FILE
    _kind = _state_kind(_file)
    _bak = _state_backup_path(_file)
    primary_error = None

    if os.path.exists(_file):
        try:
            return _normalize_trade_list(_load_trade_state_file(_file))
        except Exception as e:
            primary_error = e
            _notify_state_warning(
                f"[CRITICAL] Trade restoration failed - State preservation risk detected. "
                f"file={_file} error={e}"
            )
    else:
        primary_error = FileNotFoundError(_file)

    if os.path.exists(_bak):
        try:
            data = _load_trade_state_file(_bak)
            atomic_save_json(data, _file)
            _notify_state_warning(f"[STATE RECOVERY] Restored {_file} from valid backup {_bak}")
            return _normalize_trade_list(data)
        except Exception as e:
            _notify_state_warning(
                f"[CRITICAL] Backup trade restoration failed. file={_bak} error={e}"
            )

    if _kind == "paper":
        ok, reason = _paper_empty_recovery_allowed()
        if ok:
            _notify_state_warning(
                f"[STATE RECOVERY] PAPER state primary/backup invalid; "
                f"restoring empty paper state. reason={reason}"
            )
            atomic_save_json([], _file)
            return []
        raise RuntimeError(
            f"[CRITICAL] Trade hydration failed: {_file} corrupted and no valid backup. "
            f"PAPER empty recovery refused: {reason}. State preservation risk detected."
        )

    if _kind == "live":
        try:
            return _recover_live_state_from_exchange(_file)
        except Exception as e:
            _notify_state_warning(
                f"[CRITICAL] LIVE state recovery failed. file={_file} error={e}\n"
                "Bot cannot continue safely. Manual intervention required."
            )
            raise RuntimeError(
                f"[CRITICAL] Trade hydration failed: {_file} corrupted. "
                f"State preservation risk detected. error={primary_error}; "
                f"live_recovery_error={e}"
            )

    if isinstance(primary_error, FileNotFoundError):
        return []

    raise RuntimeError(
        f"[CRITICAL] Trade hydration failed: {_file} corrupted. "
        f"State preservation risk detected. error={primary_error}"
    )

def sanitize_trade(trade: dict) -> dict:
    """
    [FIX] Deep-sanitize toàn bộ trade dict trước khi lưu JSON.
    Convert mọi numpy type → Python native, đệ quy qua list/dict.
    Đây là tầng bảo vệ chính — không phụ thuộc vào fallback converter.
    """
    def _sanitize(v):
        if isinstance(v, np.bool_):
            return bool(v)
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.floating):
            return float(v)
        if isinstance(v, np.ndarray):
            return v.tolist()
        if isinstance(v, dict):
            return {k: _sanitize(val) for k, val in v.items()}
        if isinstance(v, list):
            return [_sanitize(i) for i in v]
        return v  # str, int, float, bool, None → giữ nguyên

    return {k: _sanitize(v) for k, v in trade.items()}

def normalize_trade_schema(t):
    t.setdefault("status", "OPEN")
    t.setdefault("trail_phase", 1)
    t.setdefault("tp_hit", False)
    t.setdefault("partial_done", False)
    t.setdefault("trail_started", False)
    t.setdefault("exit_type", "")
    t.setdefault("close_reason", "")
    t.setdefault("exit_price", 0)
    t.setdefault("rr_real", 0)
    is_live_trade = str(t.get("execution_mode", "")).lower() == "live"
    exchange_fill_price = None
    try:
        raw_exchange_fill = t.get("exchange_fill_price") or t.get("exchange_entry_price")
        if raw_exchange_fill is not None:
            exchange_fill_price = float(raw_exchange_fill)
            if np.isnan(exchange_fill_price) or exchange_fill_price <= 0:
                exchange_fill_price = None
    except (TypeError, ValueError):
        exchange_fill_price = None

    if "entry_real" not in t:
        if is_live_trade and exchange_fill_price is None:
            t["entry_real"] = None
            t["entry_price_unconfirmed"] = True
            t.setdefault("entry_source", "unconfirmed_exchange_fill")
        elif is_live_trade and exchange_fill_price is not None:
            t["entry_real"] = exchange_fill_price
            t["entry_price_unconfirmed"] = False
            t.setdefault("entry_source", "actual_exchange_fill")
        else:
            t["entry_real"] = t.get("entry", 0)
    if "entry" not in t:
        t["entry"] = t.get("entry_real", 0)
    if is_live_trade:
        if exchange_fill_price is None:
            t["entry_price_unconfirmed"] = True
            t["entry_source"] = "unconfirmed_exchange_fill"
        elif exchange_fill_price is not None:
            t["entry_real"] = exchange_fill_price
            t["entry_price_unconfirmed"] = False
            t["entry_source"] = "actual_exchange_fill"
    t.setdefault("phase2_sent", False)
    t.setdefault("phase3_sent", False)
    t.setdefault("max_profit_r", 0)
    t.setdefault("sl_init", t.get("sl", 0))
    t.setdefault("risk_percent", RISK_PER_TRADE)
    t.setdefault("side", "LONG")
    t.setdefault("symbol", "UNKNOWN")
    t.setdefault("entry_type", "CONFIRM")
    t.setdefault("tp_mode", "HARD")
    t.setdefault("giveback_notified", False)
    t.setdefault("momentum_notified", False)
    t.setdefault("struct_notified", False)
    t.setdefault("lock_done", False)
    t.setdefault("tp_break_sent", False)
    t.setdefault("profit_lock_12", False)
    t.setdefault("profit_lock_15", False)
    t.setdefault("be_07_done", False)
    t.setdefault("balance_at_entry", 0)
    t.setdefault("exchange_sl_id", None)
    t.setdefault("exchange_qty", None)
    t.setdefault("be_early_done", False)
    t.setdefault("swing_lock_done", False)
    t.setdefault("trail_log_sent", False)
    t.setdefault("pre1r_lock_done", False)
    t.setdefault("quarantined", False)
    t.setdefault("quarantine_reason", "")
    t.setdefault("repair_disabled", False)
    t.setdefault("quarantine_timestamp", 0)
    t.setdefault("stale_quarantine", False)
    t.setdefault("execution_mode", "unknown")
    # Ownership field — all bot-originated trades are stamped "bot" at open_trade().
    # The setdefault here ensures backward-compatibility: trades loaded from a state
    # file created before this field existed default to "bot" (correct assumption —
    # all pre-existing ctx.trades entries were bot-created).
    # Any entry that should NOT be bot-managed must explicitly carry owner != "bot".
    t.setdefault("owner", "bot")
    # Trailing integrity fields (FIX 2a — exchange/local SL mismatch detection)
    t.setdefault("exchange_sl_price_confirmed", None)  # last SL price confirmed on exchange
    t.setdefault("exchange_sl_sync_pending", None)     # intended SL price when last sync failed
    t.setdefault("orphan_stop_ids", [])                # old stop IDs whose cancel failed
    # ── Bot ownership identity fields ─────────────────────────────────────────
    # client_order_id: the BOT_<SYM>_E_<hex12> clientOrderId sent to exchange for the market
    #   entry order.  Populated by open_trade() after exchange fill confirmation.
    #   Allows at-a-glance identification of bot vs manual orders in exchange history.
    #   Defense-in-depth alongside t["owner"] = "bot".
    # exchange_position_owner_confirmed: True once the exchange confirms the market entry fill
    #   AND the client_order_id was a BOT_-prefixed identifier.  Proves this local trade record
    #   corresponds to a position opened by this bot process — not a manually injected entry.
    t.setdefault("client_order_id", None)
    t.setdefault("exchange_position_owner_confirmed", False)
    t.setdefault("entry_state", "")
    t.setdefault("exchange_order_state_unknown", False)
    t.setdefault("entry_uncertain_ts", 0)
    t.setdefault("entry_uncertain_reason", "")
    t.setdefault("entry_not_found_ts", 0)
    t.setdefault("canary_epoch", "")
    t.setdefault("canary_candidate_id", "")
    t.setdefault("canary_open_sequence", "")
    t.setdefault("canary_enabled_at_open", False)

def save_open_trades(trades, state_file=None):
    _file = state_file if state_file is not None else STATE_FILE
    atomic_save_json([sanitize_trade(t) for t in trades], _file)
        # [FIX] sanitize từng trade trước khi dump
        # + thêm default=_convert_numpy làm lưới bắt thứ 2

def log_scan_ema(symbol, side_h1, side_m15, ema_align, ema_slope, tm, tier, passed):
    """
    Log EMA state mỗi lần scan — không phụ thuộc vào trade outcome.
    Dùng để phân tích: EMA align vs misalign phân bố thế nào trong actionable zone.
    """
    file = log_path("scan_ema_log.csv")
    is_new = not os.path.exists(file)

    row = {
        "time":           format_vn_time(time.time()),
        "symbol":         symbol,
        "side_h1":        side_h1,
        "side_m15":       side_m15 or "NONE",
        "ema_align":      ema_align,
        "ema_slope":      ema_slope,
        "price_position": tm["price_position"],
        "dist_to_level":  tm["dist_to_level"],
        "dist_low":       tm["dist_low"],
        "tier":           tier if tier is not None else -1,
        "passed":         passed,   # True = đi tiếp vào scoring, False = bị reject
    }

    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new:
            w.writeheader()
        w.writerow(row)

def log_scan_early(symbol, side_h1, meta):
    """Log mỗi lần early entry được check — kể cả bị block."""
    file = log_path("scan_early_log.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":           format_vn_time(time.time()),
        "symbol":         symbol,
        "side_h1":        side_h1,
        "pp":           meta.get("pp", ""),
        "dist":         meta.get("dist", ""),
        "repeat_count": meta.get("repeat_count", ""),
        "wick_sweep":   meta.get("wick_sweep", ""),
        "exhaustion":   meta.get("exhaustion", ""),
        "vol_ratio":    meta.get("vol_ratio", ""),
        "funding_pen":  meta.get("funding_pen", ""),
        "block_reason": meta.get("block", ""),
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new: w.writeheader()
        w.writerow(row)

def log_exhaustion_counterfactual(symbol, side, entry, sl, tp, exhaustion_cls, bos_type, pool_stage, entry_type, score, reject_reason):
    file = log_path("exhaustion_counterfactual.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":                 format_vn_time(time.time()),
        "symbol":               symbol,
        "side":                 side,
        "entry":                round(float(entry), 6) if entry else "",
        "sl":                   round(float(sl), 6) if sl else "",
        "tp":                   round(float(tp), 6) if tp else "",
        "exhaustion_cls":       exhaustion_cls,
        "bos_type":             bos_type,
        "pool_stage":           pool_stage,
        "entry_type":           entry_type,
        "score":                round(float(score), 2) if score else "",
        "reject_reason":        reject_reason,
        "hypothetical_max_r":   "",
        "hypothetical_outcome": "",
        "evaluated_at":         "",
    }
    try:
        with open(file, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=row.keys())
            if is_new:
                w.writeheader()
            w.writerow(row)
    except Exception:
        pass

def _ensure_trade_csv_header(trades_csv):
    """
    Keep trade CSV headers aligned with save_trade() rows.
    Existing data rows are preserved as-is; only the header is created/upgraded.
    """
    if not os.path.exists(trades_csv) or os.path.getsize(trades_csv) == 0:
        with open(trades_csv, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(TRADE_CSV_HEADERS)
        return

    with open(trades_csv, "r", newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))

    if rows and rows[0] == TRADE_CSV_HEADERS:
        return

    temp_file = trades_csv + ".tmp"
    with open(temp_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(TRADE_CSV_HEADERS)
        for row in rows[1:]:
            writer.writerow(row)
    os.replace(temp_file, trades_csv)

def save_trade(t, trades_csv=None):
    _csv = trades_csv if trades_csv is not None else TRADES_CSV
    headers = TRADE_CSV_HEADERS
    _ensure_trade_csv_header(_csv)

    with open(_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)

        row = {
            "id": t.get("id"),
            "open_time": format_vn_time(t.get("time")),
            "close_time": format_vn_time(t.get("close_time")) if t.get("close_time") else "",
            "symbol": t.get("symbol"),
            "type": t.get("type"),
            "side": t.get("side"),

            # PRICE
            "entry": t.get("entry_real", t.get("entry")),
            "sl": t.get("sl_init"),
            "tp": t.get("tp"),
            "exit_price": t.get("exit_price", 0),

            # PERFORMANCE
            "rr": t.get("rr_real", 0),
            "max_r": t.get("max_profit_r", 0),
            "status": t.get("status"),
            "exit_type": t.get("exit_type", "UNKNOWN"),

            # CORE LOGIC
            "entry_type": t.get("entry_type"),
            "bos_type": t.get("bos_type"),
            "retest_strength": t.get("retest_strength"),
            "market_mode": t.get("market_mode"),

            # WYCKOFF
            "wyckoff_name": t.get("wyckoff_name"),
            "wyckoff_strength": t.get("wyckoff"),
            "trap_score": t.get("trap_score"),
            "trap_valid": t.get("trap_valid"),

            # SCORE
            "core": t.get("core"),
            "confirm": t.get("conf"),
            "score": t.get("score"),

            # CONTEXT
            "volume_ok": t.get("volume_ok"),
            "volume_spike": t.get("volume_spike"),
            "exhaustion":       t.get("exhaustion_cls"),
            "exhaustion_score": t.get("exhaustion_score", ""),

            # DEBUG
            "sl_reason": t.get("sl_reason"),
            "reason": json.dumps(t.get("reason", []), ensure_ascii=False),

            # SWING
            "priority_final": t.get("priority_final"),
            "compression_score": t.get("compression_score"),

            # 🔥 NEW
            "phase": t.get("phase"),
            "market_state": t.get("market_state"),
            "impulse": t.get("impulse"),

            "bias_type": t.get("bias_type"),
            "is_scale_in": t.get("layer"),
            "cont_score": t.get("cont_score", ""),
            "giveback_r": t.get("giveback_r", ""),
            "trade_age_minutes": t.get("trade_age_minutes", ""),
            "time_to_1r": t.get("time_to_1r", ""),
            "time_spent_above_1r": t.get("time_spent_above_1r", ""),
            "trailing_phase_at_exit": t.get("trailing_phase_at_exit", ""),
            "max_r_after_partial": t.get("max_r_after_partial", ""),
            "signal_created_ts": t.get("signal_created_ts", ""),
            "close_ts": t.get("close_ts", t.get("close_time", "")),
            "closed_at_unix": t.get("closed_at_unix", t.get("close_ts", t.get("close_time", ""))),
            "terminal_key": t.get("terminal_key") or t.get("startup_backfill_terminal_key", ""),
            "exchange_fill_price": t.get("exchange_fill_price", ""),
            "entry_source": t.get("entry_source", ""),
            "entry_price_unconfirmed": t.get("entry_price_unconfirmed", ""),
            "rr_unconfirmed": t.get("rr_unconfirmed", ""),
            "canary_epoch": t.get("canary_epoch", ""),
            "canary_candidate_id": t.get("canary_candidate_id", ""),
            "canary_open_sequence": t.get("canary_open_sequence", ""),
            "canary_enabled_at_open": t.get("canary_enabled_at_open", ""),
        }

        row = ensure_columns(row, headers)   # 👈 QUAN TRỌNG
        writer.writerow(row)
def save_tier_log(t):
    file   = log_path("tier_trades.csv")
    is_new = not os.path.exists(file)
    row = {
        "id":            t["id"],
        "time_open":     format_vn_time(t["time"]),
        "time_close":    format_vn_time(t.get("close_time", t["time"])),
        "symbol":        t["symbol"],
        "side":          t["side"],
        "tier":          t.get("tier", 1),
        "entry_type":    t["entry_type"],
        "h1_direction":  t["side"],
        "price_position":t.get("price_position", 0),
        "dist_to_level": t.get("dist_to_level",  0),
        "dist_low":      t.get("dist_low",        0),
        "range_pct":     t.get("range_pct",       0),
        "ema_align":     t.get("ema_align",     "UNKNOWN"),
        "ema_slope":     t.get("ema_slope",     "UNKNOWN"),
        "ema_bypassed":  t.get("ema_bypassed",   False),
        "repeat_count":  t.get("repeat_count",   0),
        "wick_sweep":    t.get("wick_sweep",      False),
        "volume_state":  t.get("volume_state",   "UNKNOWN"),
        "wyckoff":       t.get("wyckoff",        "NONE"),
        "exhaustion":    t.get("exhaustion_cls", ""),
        "score":         t.get("score",           0),
        "rr_at_entry":   t.get("rr_at_entry",     0),
        "entry_size":    t.get("entry_size",      1.0),
        "status":        t.get("status",         ""),
        "exit_type":     t.get("exit_type",      ""),
        "rr_real":       t.get("rr_real",         0),
        "max_profit_r":  t.get("max_profit_r",    0),
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new:
            w.writeheader()
        w.writerow(row)

def log_false_positive(t):
    """
    Log chi tiết khi trade thua hoặc sweep — tìm nguồn gốc false positive.
    Chỉ ghi khi rr_real < 0 hoặc exit_type là SWEEP.
    """
    if t.get("rr_real", 0) >= 0:
        return   # chỉ log trade thua

    file = log_path("false_positive.csv")
    is_new = not os.path.exists(file)
    row = {
        "time_open":      format_vn_time(t["time"]),
        "time_close":     format_vn_time(t.get("close_time", t["time"])),
        "symbol":         t["symbol"],
        "side":           t["side"],
        "tier":           t.get("tier", -1),
        "ema_align":      t.get("ema_align", "UNKNOWN"),
        "ema_bypassed":   t.get("ema_bypassed", False),
        "price_position": t.get("price_position", 0),
        "dist_to_level":  t.get("dist_to_level", 0),
        "dist_low":       t.get("dist_low", 0),
        "range_pct":      t.get("range_pct", 0),
        "candle_pattern": "|".join([r for r in t.get("reason", [])
                                    if r in ("Strong","Engulf","Pin","FakeBreak","Doji","Cont")]),
        "wyckoff":        t.get("wyckoff", "NONE"),
        "exhaustion_cls": t.get("exhaustion_cls", ""),
        "volume_state":   t.get("volume_state", ""),
        "bos_type":       t.get("bos_type", ""),
        "market_mode":    t.get("market_mode", ""),
        "entry_type":     t.get("entry_type", ""),
        "score":          t.get("score", 0),
        "rr_at_entry":    t.get("rr_at_entry", 0),
        "rr_real":        t.get("rr_real", 0),
        "max_profit_r":   t.get("max_profit_r", 0),
        "exit_type":      t.get("exit_type", ""),
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new: w.writeheader()
        w.writerow(row)

def log_candle_quality(symbol, side, tier, patterns, br, ur, lr, cp, passed):
    """
    Log candle quality (with anti-duplicate, Part 6 — only log fails)
    """
    if passed:
        return  # chỉ log fail để tránh spam
    pat_str = "|".join(patterns) if patterns else "NONE"
    if not _should_log(symbol, "CANDLE", pat_str):
        return
    file = log_path("candle_quality.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":     format_vn_time(time.time()),
        "symbol":   symbol,
        "side":     side,
        "tier":     tier,
        "patterns": "|".join(patterns) if patterns else "NONE",
        "br":       round(br, 3),
        "ur":       round(ur, 3),
        "lr":       round(lr, 3),
        "cp":       round(cp, 3),
        "passed":   passed,
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new: w.writeheader()
        w.writerow(row)

def log_wyckoff_outcome(t):
    """
    Log wyckoff type + trade outcome để validate V2 vs V3.
    Ghi riêng để không làm phức tạp wyckoff_compare.csv.
    """
    wy = t.get("wyckoff", "NONE")
    if wy == "NONE":
        return   # chỉ log khi có wyckoff

    file = log_path("wyckoff_outcome.csv")
    is_new = not os.path.exists(file)
    row = {
        "time_open":    format_vn_time(t["time"]),
        "time_close":   format_vn_time(t.get("close_time", t["time"])),
        "symbol":       t["symbol"],
        "side":         t["side"],
        "tier":         t.get("tier", -1),
        "wyckoff":      wy,                              # STRONG/MEDIUM/WEAK
        "entry_type":   t.get("entry_type", ""),
        "trap_valid":   t.get("trap_valid", False),
        "is_reversal":  t.get("entry_type","").startswith("REVERSAL"),
        "exhaustion":   t.get("exhaustion_cls", ""),
        "score":        t.get("score", 0),
        "rr_at_entry":  t.get("rr_at_entry", 0),
        "rr_real":      t.get("rr_real", 0),
        "max_profit_r": t.get("max_profit_r", 0),
        "status":       t.get("status", ""),
        "exit_type":    t.get("exit_type", ""),
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new: w.writeheader()
        w.writerow(row)

def log_wyckoff_compare(symbol, side, wy_type_v2, wy_name_v2, wy_score_v2, wy_type_v3, wy_name_v3, wy_score_v3):
    """Lưu so sánh V2 vs V3 ra CSV để analyze sau"""
    file = log_path("wyckoff_compare.csv")
    is_new = not os.path.exists(file)

    row = {
        "time":       format_vn_time(time.time()),
        "symbol":     symbol,
        "side":       side,
        "v2_type":    wy_type_v2 or "NONE",
        "v2_name":    wy_name_v2 or "",
        "v2_score":   wy_score_v2,
        "v3_type":    wy_type_v3 or "NONE",
        "v3_name":    wy_name_v3 or "",
        "v3_score":   wy_score_v3,
        "verdict":    (
            "V3_FILTER" if wy_type_v2 and not wy_type_v3 else
            "V3_NEW"    if not wy_type_v2 and wy_type_v3 else
            "BOTH"      if wy_type_v2 and wy_type_v3 else
            "NONE"
        )
    }

    with open(file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if is_new:
            writer.writeheader()
        writer.writerow(row)

def log_exhaustion(symbol, side, cls, score, tier, bos_n):
    """
    Log exhaustion (with anti-duplicate, Part 6)
    """
    if cls == "HEALTHY":
        return
    if not _should_log(symbol, "EXHAUSTION", cls):
        return

    file = log_path("exhaustion_log.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":    format_vn_time(time.time()),
        "symbol":  symbol,
        "side":    side,
        "cls":     cls,
        "score":   score,
        "tier":    tier,
        "bos_n":   bos_n,
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new: w.writeheader()
        w.writerow(row)

def log_pool_stage(symbol, stage, score=None, setup_type=None, reason=None):
    """
    PART 1 — Pool Pipeline Log (with anti-duplicate for SCAN stage, Part 6)
    """
    # SCAN logged every cycle → cooldown 300s; other stages cooldown 60s
    _cooldown = LOG_COOLDOWN if stage == "SCAN" else 60
    key = (symbol, "POOL_STAGE", stage)
    now = time.time()
    if now - log_cache.get(key, 0) < _cooldown:
        return
    log_cache[key] = now
    file   = log_path("log_pool_pipeline.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":       format_vn_time(time.time()),
        "symbol":     symbol,
        "stage":      stage,
        "score":      round(score, 4) if score is not None else "",
        "setup_type": setup_type or "",
        "reason":     reason or "",
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new:
            w.writeheader()
        w.writerow(row)
 
 
def log_compression(symbol, data):
    """
    PART 2 — Compression Detection Log (with anti-duplicate, Part 6)
    """
    _score = data.get("score", 0)
    if not _should_log(symbol, "COMPRESSION", f"score{_score}"):
        return   # cùng score trong 5 phút → skip
    file   = log_path("compression_log.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":            format_vn_time(time.time()),
        "symbol":          symbol,
        "range_high":      round(data.get("range_high", 0), 6),
        "range_low":       round(data.get("range_low",  0), 6),
        "score":           data.get("score", 0),
        "type":            data.get("type", "UNKNOWN"),
        "tightening":      data.get("tightening", ""),
        "rejection_count": data.get("rejection_count", 0),
        "candles":         data.get("candles", 0),
        "pre_break_score": data.get("pre_break_score", 0),
        "bias_long":       round(data.get("bias_long", 50), 1),
        "htf_context":     data.get("htf_context", ""),
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new:
            w.writeheader()
        w.writerow(row)
 
 
def log_breakout(symbol, breakout_type, direction, strength, volume_ratio, score):
    """
    PART 3 — Breakout Classification Log (with anti-duplicate, Part 6)
    """
    if not _should_log(symbol, "BREAKOUT", breakout_type):
        return
    file   = log_path("breakout_log.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":           format_vn_time(time.time()),
        "symbol":         symbol,
        "type":           breakout_type,
        "direction":      direction,
        "break_strength": round(strength, 6),
        "volume_ratio":   round(volume_ratio, 4),
        "score":          score,
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new:
            w.writeheader()
        w.writerow(row)
 
 
_CONFIRM_REJECT_FIELDS = ["time", "symbol", "reason", "entry_type", "score_old", "score_v2", "breakdown"]

def log_confirm_reject(symbol, reason, score_old=None, score_v2=None, breakdown=None, entry_type=None):
    """
    PART 4 — Confirm Rejection Log (with anti-duplicate, Part 6)
    """
    if not _should_log(symbol, "CONFIRM_REJECT", reason):
        return   # same symbol+reason trong 5 phút → skip
    file   = log_path("confirm_reject_log.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":       format_vn_time(time.time()),
        "symbol":     symbol,
        "reason":     reason,
        "entry_type": entry_type or "",
        "score_old":  score_old if score_old is not None else "",
        "score_v2":   score_v2 if score_v2 is not None else "",
        "breakdown":  breakdown if breakdown is not None else "",
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_CONFIRM_REJECT_FIELDS)
        if is_new:
            w.writeheader()
        w.writerow(row)

def log_pipeline(symbol, state, score, structure, volume_status, position_status, decision):
    """pipeline_log.csv — full scan→decision trace."""
    if not _should_log(symbol, "PIPELINE", decision):
        return
    file   = log_path("pipeline_log.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":            format_vn_time(time.time()),
        "symbol":          symbol,
        "state":           state,
        "score":           score,
        "structure":       structure,
        "volume_status":   volume_status,
        "position_status": position_status,
        "decision":        decision,
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new: w.writeheader()
        w.writerow(row)


def log_reject_clean(symbol, reason, key_var=""):
    """reject_log.csv — why a symbol was rejected with key value."""
    if not _should_log(symbol, "REJECT", reason):
        return
    file   = log_path("reject_log.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":     format_vn_time(time.time()),
        "symbol":   symbol,
        "reason":   reason,
        "key_var":  str(key_var),
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new: w.writeheader()
        w.writerow(row)


def log_entry_clean(symbol, direction, score, state, structure, volume_status, position_status, entry_type):
    """entry_log.csv — full context when entry is triggered."""
    file   = log_path("entry_log.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":            format_vn_time(time.time()),
        "symbol":          symbol,
        "direction":       direction,
        "score":           score,
        "state":           state,
        "structure":       structure,
        "volume_status":   volume_status,
        "position_status": position_status,
        "entry_type":      entry_type,
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new: w.writeheader()
        w.writerow(row)


def log_state(symbol, state, impulse, vol_ratio):
    """state_log.csv — market state per symbol per scan."""
    if not _should_log(symbol, "STATE", state):
        return
    file   = log_path("state_log.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":      format_vn_time(time.time()),
        "symbol":    symbol,
        "state":     state,
        "impulse":   impulse,
        "vol_ratio": vol_ratio,
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if is_new: w.writeheader()
        w.writerow(row)

def log_bos_fail(symbol, side, price, prev_high, prev_low, ema_align="N/A", dist_level=0):
    import csv
    from datetime import datetime
    file = log_path("bos_debug_v4.csv")
    file_exists = os.path.exists(file)
    with open(file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if side == "LONG":
            dist_high = (price - prev_high) / prev_high
            dist_low = (price - prev_low) / prev_low
        else:
            dist_high = (price - prev_high) / prev_high
            dist_low = (price - prev_low) / prev_low
        if not file_exists:
            writer.writerow([
            "time","symbol","side","price","prev_high","prev_low",
            "dist_high_pct","dist_low_pct","ema_align","dist_level"  # thêm 2 cột
            ])

        writer.writerow([
            datetime.now().strftime("%H:%M %d-%m"),
            symbol,
            side,
            round(price,6),
            round(prev_high,6),
            round(prev_low,6),
            round(dist_high,6),   # 🔥 NEW
            round(dist_low,6),     # 🔥 NEW
            ema_align, round(dist_level,4), 
        ])

def _log_swing_watchlist(symbol, w):
    file   = log_path("swing_watchlist_log.csv")
    is_new = not os.path.exists(file)
    row = {
        "time":               format_vn_time(time.time()),
        "symbol":             symbol,
        "phase":              w.get("phase"),
        "score":              w.get("score"),
        "pre_break_score":    w.get("pre_break_score"),
        "priority":           w.get("priority"),
        "priority_final":     w.get("priority_final"),
        "bias_long":          w.get("bias_long"),
        "bias_short":         w.get("bias_short"),
        "range_high":         w.get("range_high"),
        "range_low":          w.get("range_low"),
        "breakout_dir":       w.get("breakout_dir"),
        "htf_context":        w.get("htf_context"),   # [ADD]
        "compression_type":   w.get("compression_type"),  # [ADD]
        "compression_v2_ok":  w.get("compression_v2_ok"), # [ADD]
    }
    with open(file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if is_new:
            writer.writeheader()
        writer.writerow(row)

def init_csv(trades_csv=None):
    _csv = trades_csv if trades_csv is not None else TRADES_CSV
    _ensure_trade_csv_header(_csv)
        
    if not os.path.exists(log_path("bos_debug_v4.csv")):
        with open(log_path("bos_debug_v4.csv"), "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "time", "symbol", "side",
                "price", "prev_high", "prev_low",
                "dist_high_pct", "dist_low_pct",
                "ema_align", "dist_level"    # ← thêm 2 cột này
            ])
    # [LOG] Pool pipeline log
    if not os.path.exists(log_path("log_pool_pipeline.csv")):
        with open(log_path("log_pool_pipeline.csv"), "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=["time","symbol","stage","score","setup_type","reason"]).writeheader()
 
    # [LOG] Compression log
    if not os.path.exists(log_path("compression_log.csv")):
        with open(log_path("compression_log.csv"), "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=["time","symbol","range_high","range_low",
                                          "score","type","tightening","rejection_count",
                                          "candles","pre_break_score","bias_long","htf_context"]).writeheader()
 
    # [LOG] Breakout log
    if not os.path.exists(log_path("breakout_log.csv")):
        with open(log_path("breakout_log.csv"), "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=["time","symbol","type","direction",
                                          "break_strength","volume_ratio","score"]).writeheader()
 
    # [LOG] Confirm reject log
    _confirm_log = log_path("confirm_reject_log.csv")
    if os.path.exists(_confirm_log):
        with open(_confirm_log, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
        if "score_v2" not in first_line:
            os.remove(_confirm_log)
            print("[LOG INIT] Reset old log file due to schema mismatch")
    if not os.path.exists(_confirm_log):
        with open(_confirm_log, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=_CONFIRM_REJECT_FIELDS).writeheader()
