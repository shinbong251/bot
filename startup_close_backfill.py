import copy
import json
import os
import time
from datetime import datetime, timezone


SCHEMA_VERSION = "startup_authoritative_close_backfill_v1"
MARKER_FILE = "startup_authoritative_close_backfill_markers.json"
AUDIT_LOG = os.path.join("logs", "startup_authoritative_close_backfill_v1.jsonl")
REVIEW_LOG = os.path.join("logs", "startup_authoritative_close_review_v1.jsonl")
MAX_MARKERS = 2000
MARKER_TTL_SECS = 90 * 24 * 3600
MAX_MARKER_FILE_BYTES = 512 * 1024
TX_STEPS = ("canary_done", "ledger_done", "stats_done", "csv_done", "giveback_done", "local_state_done")
LOOKBACK_SECS = 6 * 3600
PRE_BUFFER_SECS = 5 * 60
POST_BUFFER_SECS = 5 * 60


def _now_ts():
    return time.time()


def _iso(ts=None):
    try:
        return datetime.fromtimestamp(float(ts if ts is not None else _now_ts()), timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=None):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _client_id(row):
    if not isinstance(row, dict):
        return ""
    for key in ("clientOrderId", "origClientOrderId", "clientAlgoId", "newClientOrderId"):
        val = row.get(key)
        if isinstance(val, str) and val:
            return val
    return ""


def _is_bot_id(value):
    return isinstance(value, str) and value.startswith("BOT_")


def _bot_ids(t):
    ids = []
    for key in ("client_order_id", "exchange_client_id", "bot_client_order_id"):
        val = t.get(key)
        if _is_bot_id(val) and val not in ids:
            ids.append(val)
    return ids


def normalize_startup_backfill_trade(t):
    if not isinstance(t, dict):
        return {"eligible": False, "reason": "trade_not_dict", "identity": "", "symbol": ""}
    symbol = str(t.get("symbol") or "").upper()
    identity = next(iter(_bot_ids(t)), "")
    if t.get("status") != "OPEN":
        return {"eligible": False, "reason": "not_open", "identity": identity, "symbol": symbol}
    if t.get("owner", "bot") != "bot":
        return {"eligible": False, "reason": "not_bot_owned", "identity": identity, "symbol": symbol}
    if t.get("quarantined") or t.get("repair_disabled"):
        return {"eligible": False, "reason": "quarantined_or_repair_disabled", "identity": identity, "symbol": symbol}
    if not identity:
        return {"eligible": False, "reason": "missing_bot_identity", "identity": "", "symbol": symbol}
    if t.get("exchange_position_owner_confirmed") is not True and t.get("entry_state") != "ENTRY_CONFIRMED":
        return {"eligible": False, "reason": "bot_ownership_not_confirmed", "identity": identity, "symbol": symbol}
    qty = _safe_float(t.get("exchange_qty"), 0.0)
    if qty <= 0:
        return {"eligible": False, "reason": "missing_exchange_qty", "identity": identity, "symbol": symbol}
    return {"eligible": True, "reason": "", "identity": identity, "symbol": symbol}


def build_exchange_lookup_window(t, startup_ts=None):
    now = float(startup_ts if startup_ts is not None else _now_ts())
    anchors = []
    for key in (
        "last_management_ts",
        "last_trade_management_ts",
        "exchange_sl_price_confirmed_ts",
        "exchange_entry_price_ts",
        "entry_time",
        "time",
    ):
        val = _safe_float(t.get(key), None)
        if val and val > 0:
            anchors.append(val)
    anchor = max(anchors) if anchors else now
    start = max(anchor - PRE_BUFFER_SECS, now - LOOKBACK_SECS)
    end = now + POST_BUFFER_SECS
    return {"start_ms": int(start * 1000), "end_ms": int(end * 1000), "anchor_ts": anchor, "startup_ts": now}


def _bounded_result(ok, data=None, error="", source=""):
    return {"ok": bool(ok), "data": data if data is not None else [], "error": error, "source": source}


def fetch_bounded_terminal_evidence(t, exchange, window):
    symbol = str(t.get("symbol") or "").upper()
    calls = []
    evidence = {
        "symbol": symbol,
        "window": dict(window),
        "position": None,
        "recent_orders": [],
        "user_trades": [],
        "income": [],
        "calls": calls,
        "errors": [],
    }

    def call(name, func, *args, **kwargs):
        calls.append(name)
        try:
            result = func(*args, **kwargs)
        except Exception as exc:
            evidence["errors"].append(f"{name}:{type(exc).__name__}")
            return _bounded_result(False, error=type(exc).__name__, source=name)
        if isinstance(result, dict) and "ok" in result:
            if not result.get("ok"):
                evidence["errors"].append(f"{name}:{result.get('error') or 'not_ok'}")
            return result
        if result is None:
            evidence["errors"].append(f"{name}:none")
            return _bounded_result(False, error="none", source=name)
        return _bounded_result(True, result, source=name)

    pos = call("position", getattr(exchange, "is_position_closed"), symbol)
    evidence["position"] = pos

    orders_func = getattr(exchange, "get_recent_orders", None)
    if orders_func is None:
        orders_func = lambda sym, limit=50: exchange._get_signed("/fapi/v1/allOrders", {"symbol": sym, "limit": limit})
    orders = call("recent_orders", orders_func, symbol, limit=50)
    evidence["recent_orders"] = orders.get("data") or []

    trades_func = getattr(exchange, "get_user_trades", None)
    if trades_func is not None:
        trades = call(
            "user_trades",
            trades_func,
            symbol,
            start_time=window["start_ms"],
            end_time=window["end_ms"],
            limit=100,
        )
        evidence["user_trades"] = trades.get("data") or []
    else:
        evidence["errors"].append("user_trades:helper_unavailable")

    income_func = getattr(exchange, "get_income_history", None)
    if income_func is not None:
        income = call(
            "income",
            income_func,
            symbol,
            income_type="REALIZED_PNL",
            start_time=window["start_ms"],
            end_time=window["end_ms"],
            limit=100,
        )
        evidence["income"] = income.get("data") or []
    else:
        evidence["errors"].append("income:helper_unavailable")

    return evidence


def _order_time(row):
    return _safe_int(row.get("updateTime"), _safe_int(row.get("time"), 0)) or 0


def _is_filled(row):
    return str(row.get("status") or "").upper() == "FILLED" and _safe_float(row.get("executedQty"), 0.0) > 0


def _opposite_close_side(side):
    return "BUY" if str(side).upper() == "SHORT" else "SELL"


def _find_entry_order(t, orders):
    ids = set(_bot_ids(t))
    expected_order_id = str(t.get("exchange_order_id") or t.get("exchange_entry_order_id") or "")
    for row in orders:
        if not _is_filled(row):
            continue
        if expected_order_id and str(row.get("orderId")) == expected_order_id:
            return row
        if _client_id(row) in ids:
            return row
    return None


def _is_true(value):
    return value is True or str(value).lower() == "true"


def _is_protective_stop_order(t, row):
    cid = _client_id(row)
    typ = str(row.get("type") or row.get("origType") or row.get("orderType") or "").upper()
    local_stop_ids = {
        str(t.get(key))
        for key in ("exchange_sl_id", "exchange_stop_order_id", "exchange_trailing_sl_id")
        if t.get(key) not in (None, "")
    }
    if local_stop_ids and str(row.get("orderId") or row.get("algoId") or "") in local_stop_ids:
        return True
    if typ in ("STOP", "STOP_MARKET", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"):
        return True
    if _is_true(row.get("closePosition")) and _is_true(row.get("reduceOnly")):
        return True
    return _is_bot_id(cid) and (cid.endswith("_S") or cid.endswith("_TS") or "_S_" in cid or "_TS_" in cid)


def _is_generic_bot_close(row):
    return _is_bot_id(_client_id(row))


def _terminal_candidates(t, orders, entry_order):
    side = str(t.get("side") or "").upper()
    close_side = _opposite_close_side(side)
    entry_time = _order_time(entry_order or {})
    entry_order_id = str((entry_order or {}).get("orderId") or "")
    candidates = []
    for row in orders:
        if not _is_filled(row):
            continue
        if str(row.get("orderId") or "") == entry_order_id:
            continue
        if str(row.get("side") or "").upper() != close_side:
            continue
        if _order_time(row) and entry_time and _order_time(row) < entry_time:
            continue
        reduce_only = _is_true(row.get("reduceOnly"))
        close_position = _is_true(row.get("closePosition"))
        if reduce_only or close_position:
            candidates.append(row)
    candidates.sort(key=_order_time)
    return candidates


def _aggregate_terminal_orders(t, candidates, orders):
    target_qty = _safe_float(t.get("exchange_qty"), 0.0) or 0.0
    if target_qty <= 0:
        return None, "missing_local_exchange_qty"
    side = str(t.get("side") or "").upper()
    entry_side = "SELL" if side == "SHORT" else "BUY"
    total = 0.0
    selected = []
    first_close_time = None
    for row in candidates:
        row_time = _order_time(row)
        if first_close_time is None:
            first_close_time = row_time
        # Same-symbol re-entry before the terminal quantity is complete makes
        # attribution ambiguous; fail closed.
        for other in orders:
            if other is row or not _is_filled(other):
                continue
            ot = _order_time(other)
            if first_close_time and ot > first_close_time and ot < row_time:
                if str(other.get("side") or "").upper() == entry_side and not _is_true(other.get("reduceOnly")):
                    return None, "same_symbol_reentry_contamination"
        selected.append(row)
        total += abs(_safe_float(row.get("executedQty"), 0.0) or 0.0)
        if total >= target_qty - 1e-8:
            if abs(total - target_qty) > max(1e-8, target_qty * 1e-6):
                return None, "terminal_qty_exceeds_local_qty"
            return selected, ""
    return None, "terminal_qty_incomplete"


def classify_terminal_evidence(t, evidence):
    flags = []
    if len(evidence.get("calls", [])) > 4:
        flags.append("call_bound_exceeded")
    pos = evidence.get("position") or {}
    if pos.get("ok") is not True or pos.get("data") is not True:
        return {"classification": "INCOMPLETE", "reason": "position_not_authoritatively_closed", "data_quality_flags": flags + evidence.get("errors", [])}
    orders = evidence.get("recent_orders") or []
    entry_order = _find_entry_order(t, orders)
    if not entry_order:
        return {"classification": "INCOMPLETE", "reason": "entry_order_not_found", "data_quality_flags": flags + evidence.get("errors", [])}
    candidates = _terminal_candidates(t, orders, entry_order)
    if not candidates:
        return {"classification": "INCOMPLETE", "reason": "terminal_close_order_not_found", "entry_order": entry_order, "data_quality_flags": flags + evidence.get("errors", [])}
    selected, aggregate_error = _aggregate_terminal_orders(t, candidates, orders)
    if not selected:
        return {"classification": "INCOMPLETE", "reason": aggregate_error, "entry_order": entry_order, "data_quality_flags": flags + evidence.get("errors", [])}
    close_order = selected[-1]
    reduce_only = _is_true(close_order.get("reduceOnly"))
    typ = str(close_order.get("type") or close_order.get("origType") or "").upper()
    if any(_is_protective_stop_order(t, row) for row in selected):
        classification = "EXCHANGE_SL_FILLED"
        terminal_cause = "EXCHANGE_SL_FILLED"
    elif any(_is_generic_bot_close(row) for row in selected):
        classification = "BOT_MARKET_CLOSE"
        terminal_cause = "BOT_MARKET_CLOSE"
    elif reduce_only:
        classification = "MANUAL_CLOSE"
        terminal_cause = "MANUAL_CLOSE"
    elif typ in ("LIQUIDATION", "ADL"):
        classification = "REVIEW_REQUIRED"
        terminal_cause = typ
    else:
        classification = "REVIEW_REQUIRED"
        terminal_cause = "UNSUPPORTED_TERMINAL_ORDER"
    return {
        "classification": classification,
        "terminal_cause": terminal_cause,
        "entry_order": entry_order,
        "close_order": close_order,
        "terminal_orders": selected,
        "data_quality_flags": flags + evidence.get("errors", []),
        "reason": "",
    }


def _weighted_avg_trade_price(rows):
    total_qty = 0.0
    total_quote = 0.0
    for row in rows:
        qty = abs(_safe_float(row.get("qty"), 0.0) or 0.0)
        price = _safe_float(row.get("price"), None)
        quote = abs(_safe_float(row.get("quoteQty"), 0.0) or 0.0)
        if qty <= 0:
            continue
        total_qty += qty
        total_quote += quote if quote > 0 else qty * (price or 0.0)
    if total_qty <= 0:
        return None
    return total_quote / total_qty


def reconstruct_terminal_close(t, classified, evidence):
    if classified.get("classification") in ("INCOMPLETE", "REVIEW_REQUIRED"):
        return {"complete": False, "reason": classified.get("reason") or classified.get("classification")}
    if classified.get("classification") == "MANUAL_CLOSE":
        return {"complete": False, "review_only": True, "reason": "manual_close_review_only"}
    entry_order = classified.get("entry_order") or {}
    close_order = classified.get("close_order") or {}
    terminal_orders = classified.get("terminal_orders") or ([close_order] if close_order else [])
    entry_order_id = _safe_int(entry_order.get("orderId"))
    close_order_id = _safe_int(close_order.get("orderId"))
    close_order_ids = [str(row.get("orderId")) for row in terminal_orders if row.get("orderId")]
    terminal_trades = [
        row for row in evidence.get("user_trades", [])
        if str(row.get("orderId") or "") in set(close_order_ids)
    ]
    entry = _safe_float(entry_order.get("avgPrice"), None) or _safe_float(t.get("entry_real") or t.get("entry"), None)
    exit_price = _weighted_avg_trade_price(terminal_trades) or _weighted_avg_trade_price([
        {"qty": row.get("executedQty"), "price": row.get("avgPrice")}
        for row in terminal_orders
    ])
    close_ts_ms = _order_time(close_order)
    sl_init = _safe_float(t.get("sl_init"), None)
    if sl_init is None:
        sl_init = _safe_float(t.get("sl"), None)
        risk_source = "LOCAL_SL_FALLBACK" if sl_init else "UNAVAILABLE"
    else:
        risk_source = "EXACT_LOCAL_INITIAL_RISK"
    risk = abs(entry - sl_init) if entry and sl_init else None
    if not entry or not exit_price or not risk or risk <= 0:
        rr = None
        rr_source = "UNAVAILABLE"
    else:
        if str(t.get("side") or "").upper() == "SHORT":
            rr = (entry - exit_price) / risk
        else:
            rr = (exit_price - entry) / risk
        rr_source = risk_source
    realized_pnl = None
    matched_income = [
        row for row in evidence.get("income", [])
        if str(row.get("incomeType") or "").upper() == "REALIZED_PNL"
    ]
    if terminal_trades:
        ids = {str(row.get("id") or row.get("tradeId") or "") for row in terminal_trades}
        scoped = [
            row for row in matched_income
            if str(row.get("tradeId") or row.get("info") or "") in ids
        ]
    else:
        scoped = []
    if scoped:
        realized_pnl = round(sum(_safe_float(row.get("income"), 0.0) or 0.0 for row in scoped), 8)
    fees = None
    if terminal_trades:
        fees = round(sum(_safe_float(row.get("commission"), 0.0) or 0.0 for row in terminal_trades), 8)
    terminal_ids = [str(row.get("id") or row.get("tradeId")) for row in terminal_trades if row.get("id") or row.get("tradeId")]
    usd_complete = realized_pnl is not None
    r_complete = rr is not None
    if not r_complete and classified.get("classification") in ("EXCHANGE_SL_FILLED", "BOT_MARKET_CLOSE") and t.get("canary_enabled_at_open") is True:
        return {
            "complete": False,
            "review_only": True,
            "reason": "canary_requires_realized_r",
            "realized_pnl": realized_pnl,
            "fees": fees,
            "rr_real": None,
            "rr_source": "UNAVAILABLE",
            "data_quality_flags": classified.get("data_quality_flags", []) + ["usd_only_canary_review_required"],
        }
    return {
        "complete": bool(entry and exit_price and (r_complete or usd_complete)),
        "classification": classified.get("classification"),
        "terminal_cause": classified.get("terminal_cause"),
        "entry": entry,
        "exit_price": exit_price,
        "entry_order_id": entry_order_id,
        "close_order_id": close_order_id,
        "close_order_ids": close_order_ids,
        "close_ts": close_ts_ms / 1000.0 if close_ts_ms else _now_ts(),
        "initial_risk": risk,
        "initial_risk_source": risk_source,
        "rr_real": rr,
        "rr_source": rr_source,
        "realized_pnl": realized_pnl,
        "fees": fees,
        "terminal_fill_ids": terminal_ids,
        "data_quality_flags": classified.get("data_quality_flags", []),
    }


def _marker_key(identity, terminal):
    if terminal.get("close_order_id"):
        return f"close_order:{terminal['close_order_id']}"
    ids = terminal.get("terminal_fill_ids") or []
    if ids:
        return "terminal_fills:" + ",".join(sorted(map(str, ids)))
    return f"identity_ts:{identity}:{round(float(terminal.get('close_ts') or 0), 3)}"


def load_marker_store(path=MARKER_FILE, now_ts=None):
    now = now_ts if now_ts is not None else _now_ts()
    default = {"schema_version": 2, "transactions": []}
    if not os.path.exists(path):
        return default
    try:
        if os.path.getsize(path) > MAX_MARKER_FILE_BYTES:
            return default
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict) and isinstance(data.get("markers"), list):
            data = {
                "schema_version": 2,
                "transactions": [
                    {
                        "terminal_key": row.get("terminal_key"),
                        "status": "COMPLETE",
                        "marked_at": row.get("marked_at", now),
                        "completed_at": row.get("marked_at", now),
                        "steps": {step: True for step in TX_STEPS},
                    }
                    for row in data.get("markers", [])
                    if isinstance(row, dict)
                ],
            }
        if not isinstance(data, dict) or not isinstance(data.get("transactions"), list):
            return default
    except Exception:
        return default
    return prune_marker_store(data, now)


def prune_marker_store(store, now_ts=None):
    now = now_ts if now_ts is not None else _now_ts()
    transactions = []
    seen = set()
    for row in store.get("transactions", []):
        if not isinstance(row, dict):
            continue
        key = str(row.get("terminal_key") or "")
        ts = _safe_float(row.get("completed_at") or row.get("marked_at"), now) or now
        status = str(row.get("status") or "PREPARED").upper()
        if not key or key in seen:
            continue
        if status == "COMPLETE" and now - ts > MARKER_TTL_SECS:
            continue
        seen.add(key)
        row.setdefault("steps", {})
        transactions.append(row)
    incomplete = [row for row in transactions if row.get("status") != "COMPLETE"]
    complete = [row for row in transactions if row.get("status") == "COMPLETE"]
    transactions = incomplete + complete[-max(0, MAX_MARKERS - len(incomplete)):]
    return {"schema_version": 2, "transactions": transactions}


def save_marker_store(store, path=MARKER_FILE):
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}.{time.time_ns()}"
    data = prune_marker_store(store)
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, sort_keys=True)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def get_transaction(store, terminal_key):
    for row in store.get("transactions", []):
        if row.get("terminal_key") == terminal_key:
            row.setdefault("steps", {})
            return row
    return None


def prepare_transaction(store, terminal_key, t, terminal):
    tx = get_transaction(store, terminal_key)
    if tx:
        return tx
    tx = {
        "terminal_key": terminal_key,
        "status": "PREPARED",
        "trade_id": t.get("id"),
        "local_identity": next(iter(_bot_ids(t)), ""),
        "symbol": t.get("symbol"),
        "close_order_id": terminal.get("close_order_id"),
        "close_order_ids": terminal.get("close_order_ids", []),
        "expected_steps": list(TX_STEPS),
        "steps": {},
        "prepared_at": _now_ts(),
        "marked_at": _now_ts(),
    }
    store.setdefault("transactions", []).append(tx)
    return tx


def mark_transaction_step(path, terminal_key, step, **fields):
    store = load_marker_store(path)
    tx = get_transaction(store, terminal_key)
    if tx is None:
        tx = {"terminal_key": terminal_key, "status": "PREPARED", "steps": {}, "prepared_at": _now_ts(), "marked_at": _now_ts()}
        store.setdefault("transactions", []).append(tx)
    tx.setdefault("steps", {})[step] = True
    tx.update(fields)
    tx["updated_at"] = _now_ts()
    save_marker_store(store, path)
    return tx


def mark_transaction_complete(path, terminal_key):
    store = load_marker_store(path)
    tx = get_transaction(store, terminal_key)
    if tx is None:
        return None
    if all(tx.get("steps", {}).get(step) is True for step in TX_STEPS):
        tx["status"] = "COMPLETE"
        tx["completed_at"] = _now_ts()
    tx["updated_at"] = _now_ts()
    save_marker_store(store, path)
    return tx


def _append_jsonl(path, row):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def _audit_row(t, classification, window, evidence, terminal=None, error=""):
    terminal = terminal or {}
    return {
        "schema_version": SCHEMA_VERSION,
        "logged_at": _iso(),
        "startup_pid": os.getpid(),
        "symbol": t.get("symbol"),
        "side": t.get("side"),
        "local_identity": next(iter(_bot_ids(t)), ""),
        "exchange_entry_order_id": terminal.get("entry_order_id") or t.get("exchange_order_id"),
        "exchange_close_order_id": terminal.get("close_order_id"),
        "lookup_window": window,
        "evidence_sources": evidence.get("calls", []),
        "classification": classification.get("classification"),
        "evidence_completeness": bool(terminal.get("complete")),
        "entry": terminal.get("entry"),
        "exit": terminal.get("exit_price"),
        "initial_risk": terminal.get("initial_risk"),
        "realized_r": None if terminal.get("rr_real") is None else round(float(terminal.get("rr_real")), 6),
        "realized_r_source": terminal.get("rr_source"),
        "realized_pnl": terminal.get("realized_pnl"),
        "fees": terminal.get("fees"),
        "close_ts": terminal.get("close_ts"),
        "csv_result": terminal.get("csv_result"),
        "canary_result": terminal.get("canary_result"),
        "ledger_result": terminal.get("ledger_result"),
        "giveback_result": terminal.get("giveback_result"),
        "local_state_result": terminal.get("local_state_result"),
        "quarantine_latch_result": terminal.get("quarantine_latch_result"),
        "idempotent_noop": terminal.get("idempotent_noop", False),
        "data_quality_flags": terminal.get("data_quality_flags") or classification.get("data_quality_flags", []),
        "error": error,
    }


_INELIGIBLE_AUDIT_KEYS = set()


def _audit_ineligible_once(t, reason, window=None):
    if not isinstance(t, dict):
        t = {}
    key = f"{t.get('symbol')}:{next(iter(_bot_ids(t)), t.get('id', ''))}:{reason}"
    if key in _INELIGIBLE_AUDIT_KEYS:
        return
    _INELIGIBLE_AUDIT_KEYS.add(key)
    _append_jsonl(AUDIT_LOG, _audit_row(
        t,
        {"classification": "INELIGIBLE", "data_quality_flags": []},
        window or {},
        {"calls": []},
        {"quarantine_latch_result": "existing_quarantine_path", "complete": False},
        error=reason,
    ))


def apply_startup_close_backfill(t, ctx, evidence, finalizer, marker_path=MARKER_FILE):
    eligibility = normalize_startup_backfill_trade(t)
    if not eligibility.get("eligible"):
        _audit_ineligible_once(t, eligibility.get("reason"), evidence.get("window") if isinstance(evidence, dict) else None)
        return {"status": "INELIGIBLE", "reason": eligibility.get("reason")}
    window = evidence.get("window") or build_exchange_lookup_window(t)
    classified = classify_terminal_evidence(t, evidence)
    terminal = reconstruct_terminal_close(t, classified, evidence)
    if terminal.get("review_only") or classified.get("classification") == "MANUAL_CLOSE":
        _append_jsonl(REVIEW_LOG, _audit_row(t, classified, window, evidence, terminal))
        return {"status": "REVIEW_REQUIRED", "reason": terminal.get("reason") or "manual_close_review_only", "classification": classified}
    if not terminal.get("complete"):
        _append_jsonl(AUDIT_LOG, _audit_row(t, classified, window, evidence, terminal))
        return {"status": "INCOMPLETE", "reason": terminal.get("reason") or classified.get("reason"), "classification": classified}
    marker_store = load_marker_store(marker_path)
    terminal_key = _marker_key(eligibility.get("identity"), terminal)
    tx = get_transaction(marker_store, terminal_key)
    if tx and tx.get("status") == "COMPLETE":
        terminal["idempotent_noop"] = True
        _append_jsonl(AUDIT_LOG, _audit_row(t, classified, window, evidence, terminal))
        return {"status": "ALREADY_FINALIZED", "terminal_key": terminal_key, "terminal": terminal}
    tx = prepare_transaction(marker_store, terminal_key, t, terminal)
    save_marker_store(marker_store, marker_path)
    local_t = t
    local_t["exchange_exit_price"] = terminal["exit_price"]
    local_t["exchange_close_price"] = terminal["exit_price"]
    local_t["exchange_close_order_id"] = terminal.get("close_order_id")
    local_t["exchange_entry_order_id"] = terminal.get("entry_order_id")
    local_t["startup_backfill_terminal_key"] = terminal_key
    local_t["startup_backfill_source"] = classified.get("classification")
    if classified.get("classification") == "BOT_MARKET_CLOSE":
        terminal["exit_type"] = "MARKET"
    terminal["transaction_path"] = marker_path
    terminal["terminal_key"] = terminal_key
    terminal["transaction_steps"] = dict(tx.get("steps", {}))
    terminal["transaction_meta"] = dict(tx)
    ok = finalizer(
        local_t,
        ctx,
        source="startup_authoritative_close_backfill",
        terminal_close=terminal,
        exit_type="SL" if classified.get("classification") == "EXCHANGE_SL_FILLED" else terminal.get("exit_type"),
        close_reason="exchange_sl_filled" if classified.get("classification") == "EXCHANGE_SL_FILLED" else str(classified.get("terminal_cause") or classified.get("classification") or "").lower(),
    )
    if not ok:
        _append_jsonl(AUDIT_LOG, _audit_row(t, classified, window, evidence, terminal, error="finalizer_failed"))
        return {"status": "ERROR", "reason": "finalizer_failed", "terminal_key": terminal_key}
    mark_transaction_complete(marker_path, terminal_key)
    terminal.update({
        "csv_result": "shared_finalizer",
        "canary_result": "shared_finalizer",
        "ledger_result": "shared_finalizer",
        "giveback_result": "shared_finalizer",
        "local_state_result": "shared_finalizer",
        "quarantine_latch_result": "skipped_quarantine",
    })
    _append_jsonl(AUDIT_LOG, _audit_row(t, classified, window, evidence, terminal))
    return {"status": "FINALIZED", "terminal_key": terminal_key, "terminal": terminal}


def startup_close_backfill_once(t, ctx, exchange, finalizer, startup_ts=None, marker_path=MARKER_FILE):
    eligibility = normalize_startup_backfill_trade(t)
    if not eligibility.get("eligible"):
        _audit_ineligible_once(t, eligibility.get("reason"), {"startup_ts": startup_ts})
        return {"status": "INELIGIBLE", "reason": eligibility.get("reason")}
    window = build_exchange_lookup_window(t, startup_ts=startup_ts)
    evidence = fetch_bounded_terminal_evidence(t, exchange, window)
    try:
        return apply_startup_close_backfill(t, ctx, evidence, finalizer, marker_path=marker_path)
    except Exception as exc:
        row = _audit_row(t, {"classification": "ERROR", "data_quality_flags": []}, window, evidence, error=type(exc).__name__)
        _append_jsonl(AUDIT_LOG, row)
        return {"status": "ERROR", "reason": type(exc).__name__}


def dry_run_with_evidence(t, evidence):
    evidence = copy.deepcopy(evidence)
    classified = classify_terminal_evidence(t, evidence)
    terminal = reconstruct_terminal_close(t, classified, evidence)
    return {"classification": classified, "terminal": terminal}
