"""Durable manual-position ownership exclusions.

This module is intentionally pure at import time: it performs no exchange
queries and no filesystem mutation unless a caller explicitly loads/saves or
writes an audit row.
"""

from __future__ import annotations

import json
import os
import time
import hashlib
from datetime import datetime, timezone
from typing import Iterable, Optional


SCHEMA_VERSION = 1
DEFAULT_REGISTRY_PATH = "manual_position_exclusions.json"
AUDIT_LOG = os.path.join("logs", "manual_position_exclusion_v1.jsonl")
MAX_REGISTRY_BYTES = 64 * 1024
MAX_POSITIONS = 100
QTY_TOLERANCE = 1e-9
ENTRY_REL_TOLERANCE = 0.01
ENTRY_ABS_TOLERANCE = 1e-12

MANUAL_CONFIRMED = "MANUAL_CONFIRMED"
MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
EXCLUSION_STALE = "EXCLUSION_STALE"
NO_EXCLUSION = "NO_EXCLUSION"
REGISTRY_INVALID = "REGISTRY_INVALID"

_AUDIT_DEDUP_KEYS = set()


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _norm_symbol(value) -> str:
    return str(value or "").upper().strip()


def _side_from_qty(qty) -> str:
    value = _safe_float(qty, 0.0) or 0.0
    if value > 0:
        return "LONG"
    if value < 0:
        return "SHORT"
    return "FLAT"


def _norm_side(position_side=None, exchange_qty=None) -> str:
    raw = str(position_side or "").upper().strip()
    if raw in ("LONG", "SHORT"):
        return raw
    return _side_from_qty(exchange_qty)


def _order_id_set(rows: Optional[Iterable[dict]]) -> set[str]:
    ids = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        for field in ("orderId", "algoId", "actualOrderId"):
            value = row.get(field)
            if value not in (None, ""):
                ids.add(str(value))
    return ids


def _registry_order_id_set(record: dict, *fields: str) -> set[str]:
    ids = set()
    for field in fields:
        value = record.get(field)
        if isinstance(value, list):
            ids.update(str(item) for item in value if item not in (None, ""))
        elif value not in (None, ""):
            ids.add(str(value))
    return ids


def _epoch_order_ids(record: dict) -> set[str]:
    ids = _registry_order_id_set(
        record,
        "entry_order_ids",
        "entry_fill_ids",
        "protective_order_ids",
        "position_epoch_order_ids",
    )
    epoch = record.get("position_epoch")
    if isinstance(epoch, dict):
        for field in ("entry_order_ids", "entry_fill_ids", "protective_order_ids", "order_ids"):
            value = epoch.get(field)
            if isinstance(value, list):
                ids.update(str(item) for item in value if item not in (None, ""))
            elif value not in (None, ""):
                ids.add(str(value))
    return ids


def _build_epoch_fingerprint(symbol, side, entry_order_ids=None, entry_fill_ids=None, opened_at=None) -> str:
    payload = {
        "symbol": _norm_symbol(symbol),
        "side": _norm_side(side),
        "entry_order_ids": sorted(str(x) for x in (entry_order_ids or []) if x not in (None, "")),
        "entry_fill_ids": sorted(str(x) for x in (entry_fill_ids or []) if x not in (None, "")),
        "opened_at": str(opened_at or ""),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _entry_consistent(expected, actual) -> bool:
    exp = _safe_float(expected, None)
    act = _safe_float(actual, None)
    if exp is None or act is None or exp <= 0 or act <= 0:
        return False
    return abs(exp - act) <= max(ENTRY_ABS_TOLERANCE, abs(exp) * ENTRY_REL_TOLERANCE)


def _qty_matches(expected, actual) -> bool:
    exp = abs(_safe_float(expected, 0.0) or 0.0)
    act = abs(_safe_float(actual, 0.0) or 0.0)
    return abs(exp - act) <= max(QTY_TOLERANCE, exp * 1e-9)


def _atomic_save_json(data: dict, path: str = DEFAULT_REGISTRY_PATH) -> None:
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    payload = json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True)
    json.loads(payload)
    tmp = f"{path}.tmp.{os.getpid()}.{int(time.time() * 1_000_000)}"
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    with open(tmp, encoding="utf-8") as handle:
        json.load(handle)
    os.replace(tmp, path)
    try:
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def validate_manual_registry(registry) -> tuple[bool, str]:
    if not isinstance(registry, dict):
        return False, "registry_not_dict"
    if registry.get("schema_version") != SCHEMA_VERSION:
        return False, "schema_version_mismatch"
    positions = registry.get("positions")
    if not isinstance(positions, list):
        return False, "positions_not_list"
    if len(positions) > MAX_POSITIONS:
        return False, "positions_too_many"
    seen_active = set()
    for idx, row in enumerate(positions):
        if not isinstance(row, dict):
            return False, f"position_{idx}_not_dict"
        symbol = _norm_symbol(row.get("symbol"))
        side = _norm_side(row.get("position_side"))
        if not symbol or side not in ("LONG", "SHORT"):
            return False, f"position_{idx}_missing_identity"
        key = (symbol, side)
        active = not (row.get("inactive") is True or str(row.get("status") or "").upper() == "STALE")
        if active and key in seen_active:
            return False, f"duplicate_active_position:{symbol}:{side}"
        if active:
            seen_active.add(key)
        if row.get("ownership") != MANUAL_CONFIRMED:
            return False, f"position_{idx}_unsupported_ownership"
    return True, ""


def load_manual_registry(path: str = DEFAULT_REGISTRY_PATH) -> dict:
    if not os.path.exists(path):
        return {"schema_version": SCHEMA_VERSION, "positions": []}
    try:
        if os.path.getsize(path) > MAX_REGISTRY_BYTES:
            return {"_invalid": True, "reason": "registry_oversized"}
        with open(path, encoding="utf-8") as handle:
            registry = json.load(handle)
    except Exception as exc:
        return {"_invalid": True, "reason": f"registry_load_failed:{type(exc).__name__}"}
    ok, reason = validate_manual_registry(registry)
    if not ok:
        return {"_invalid": True, "reason": reason}
    return registry


def save_manual_registry(registry: dict, path: str = DEFAULT_REGISTRY_PATH) -> None:
    ok, reason = validate_manual_registry(registry)
    if not ok:
        raise ValueError(reason)
    if len(json.dumps(registry, ensure_ascii=False)) > MAX_REGISTRY_BYTES:
        raise ValueError("registry_oversized")
    _atomic_save_json(registry, path)


def build_manual_position_record(
    *,
    symbol: str,
    position_side: str,
    expected_qty: float,
    entry_price: float,
    entry_order_ids=None,
    entry_fill_ids=None,
    protective_order_ids=None,
    position_opened_at: str | None = None,
    source: str = "USER_CONFIRMED",
    notes: str = "",
    confirmed_at: str | None = None,
) -> dict:
    confirmed = confirmed_at or _iso_now()
    opened_at = position_opened_at or confirmed
    entry_ids = [str(x) for x in (entry_order_ids or [])]
    fill_ids = [str(x) for x in (entry_fill_ids or [])]
    protective_ids = [str(x) for x in (protective_order_ids or [])]
    epoch_order_ids = sorted(set(entry_ids + fill_ids + protective_ids))
    return {
        "symbol": _norm_symbol(symbol),
        "position_side": _norm_side(position_side),
        "ownership": MANUAL_CONFIRMED,
        "source": source,
        "confirmed_at": confirmed,
        "expected_qty": float(expected_qty),
        "entry_price": float(entry_price),
        "entry_order_ids": entry_ids,
        "entry_fill_ids": fill_ids,
        "protective_order_ids": protective_ids,
        "position_opened_at": opened_at,
        "position_epoch_order_ids": epoch_order_ids,
        "position_epoch_fingerprint": _build_epoch_fingerprint(
            symbol,
            position_side,
            entry_order_ids=entry_ids,
            entry_fill_ids=fill_ids,
            opened_at=opened_at,
        ),
        "position_epoch": {
            "entry_order_ids": entry_ids,
            "entry_fill_ids": fill_ids,
            "protective_order_ids": protective_ids,
            "opened_at": opened_at,
            "fingerprint": _build_epoch_fingerprint(
                symbol,
                position_side,
                entry_order_ids=entry_ids,
                entry_fill_ids=fill_ids,
                opened_at=opened_at,
            ),
        },
        "notes": notes,
    }


def resolve_manual_position_exclusion(
    symbol,
    position_side=None,
    exchange_qty=None,
    entry_price=None,
    open_orders=None,
    registry=None,
) -> dict:
    if not isinstance(registry, dict):
        return _decision(REGISTRY_INVALID, "registry_not_dict", symbol, position_side, exchange_qty, entry_price)
    if registry.get("_invalid"):
        return _decision(REGISTRY_INVALID, registry.get("reason", "registry_invalid"), symbol, position_side, exchange_qty, entry_price)
    ok, reason = validate_manual_registry(registry)
    if not ok:
        return _decision(REGISTRY_INVALID, reason, symbol, position_side, exchange_qty, entry_price)

    sym = _norm_symbol(symbol)
    side = _norm_side(position_side, exchange_qty)
    qty = _safe_float(exchange_qty, 0.0) or 0.0
    if side == "FLAT" or qty == 0.0:
        matches = [row for row in registry.get("positions", []) if _norm_symbol(row.get("symbol")) == sym]
        if matches:
            return _decision(EXCLUSION_STALE, "position_flat", sym, side, qty, entry_price, record=matches[0])
        return _decision(NO_EXCLUSION, "no_matching_symbol", sym, side, qty, entry_price)

    same_symbol = [row for row in registry.get("positions", []) if _norm_symbol(row.get("symbol")) == sym]
    if not same_symbol:
        return _decision(NO_EXCLUSION, "no_matching_symbol", sym, side, qty, entry_price)

    active_exact = [
        row for row in same_symbol
        if _norm_side(row.get("position_side")) == side
        and not (row.get("inactive") is True or str(row.get("status") or "").upper() == "STALE")
    ]
    stale_exact = [
        row for row in same_symbol
        if _norm_side(row.get("position_side")) == side
        and (row.get("inactive") is True or str(row.get("status") or "").upper() == "STALE")
    ]
    if not active_exact:
        if stale_exact:
            return _decision(EXCLUSION_STALE, "registry_record_stale", sym, side, qty, entry_price, record=stale_exact[0])
        return _decision(EXCLUSION_STALE, "side_changed", sym, side, qty, entry_price, record=same_symbol[0])
    if len(active_exact) > 1:
        return _decision(REGISTRY_INVALID, "duplicate_active_matching_entries", sym, side, qty, entry_price)

    record = active_exact[0]
    if not _qty_matches(record.get("expected_qty"), qty):
        return _decision(MANUAL_REVIEW_REQUIRED, "quantity_changed", sym, side, qty, entry_price, record=record)
    if not _entry_consistent(record.get("entry_price"), entry_price):
        return _decision(MANUAL_REVIEW_REQUIRED, "entry_price_mismatch", sym, side, qty, entry_price, record=record)

    flags = []
    current_ids = _order_id_set(open_orders)
    epoch_ids = _epoch_order_ids(record)
    if not epoch_ids:
        return _decision(MANUAL_REVIEW_REQUIRED, "position_epoch_identity_missing", sym, side, qty, entry_price, record=record)
    if not current_ids:
        return _decision(MANUAL_REVIEW_REQUIRED, "position_epoch_evidence_unavailable", sym, side, qty, entry_price, record=record)
    if not (epoch_ids & current_ids):
        return _decision(MANUAL_REVIEW_REQUIRED, "position_epoch_mismatch", sym, side, qty, entry_price, record=record)
    entry_ids = _registry_order_id_set(record, "entry_order_ids", "entry_fill_ids")
    if entry_ids and not (entry_ids & current_ids):
        flags.append("entry_epoch_history_absent_but_other_epoch_id_matched")

    return _decision(MANUAL_CONFIRMED, "explicit_user_confirmed", sym, side, qty, entry_price, record=record, flags=flags)


def mark_manual_exclusion_stale(registry: dict, symbol: str, position_side: str | None = None, reason: str = "position_flat") -> bool:
    if not isinstance(registry, dict) or registry.get("_invalid"):
        return False
    positions = registry.get("positions")
    if not isinstance(positions, list):
        return False
    sym = _norm_symbol(symbol)
    side = _norm_side(position_side) if position_side else None
    changed = False
    for row in positions:
        if not isinstance(row, dict) or _norm_symbol(row.get("symbol")) != sym:
            continue
        if side and _norm_side(row.get("position_side")) != side:
            continue
        if row.get("inactive") is True and str(row.get("status") or "").upper() == "STALE":
            continue
        row["inactive"] = True
        row["status"] = "STALE"
        row["stale_at"] = _iso_now()
        row["stale_reason"] = reason
        changed = True
    return changed


def _decision(classification, reason, symbol, side, qty, entry_price, record=None, flags=None):
    allowed = classification == MANUAL_CONFIRMED
    review = classification in (MANUAL_REVIEW_REQUIRED, EXCLUSION_STALE, REGISTRY_INVALID)
    return {
        "classification": classification,
        "reason": reason,
        "symbol": _norm_symbol(symbol),
        "position_side": _norm_side(side, qty),
        "exchange_qty": _safe_float(qty, 0.0) or 0.0,
        "entry_price": _safe_float(entry_price, None),
        "registry_record": dict(record or {}),
        "registry_status": "valid" if classification != REGISTRY_INVALID else "invalid",
        "data_quality_flags": list(flags or []),
        "reconstruct": False if (allowed or review) else None,
        "manage": False if (allowed or review) else None,
        "account": False if (allowed or review) else None,
        "canary": False if (allowed or review) else None,
        "startup_backfill": False if (allowed or review) else None,
        "modify_orders": False if (allowed or review) else None,
    }


def append_manual_exclusion_audit(result: dict, path: str = AUDIT_LOG) -> bool:
    if not isinstance(result, dict):
        return False
    classification = result.get("classification")
    if classification not in (MANUAL_CONFIRMED, MANUAL_REVIEW_REQUIRED, EXCLUSION_STALE, REGISTRY_INVALID):
        return False
    symbol = result.get("symbol")
    side = result.get("position_side")
    reason = result.get("reason")
    key = f"{symbol}:{side}:{classification}:{reason}"
    if key in _AUDIT_DEDUP_KEYS:
        return False
    _AUDIT_DEDUP_KEYS.add(key)
    record = result.get("registry_record") if isinstance(result.get("registry_record"), dict) else {}
    row = {
        "schema_version": SCHEMA_VERSION,
        "logged_at": _iso_now(),
        "symbol": symbol,
        "position_side": side,
        "exchange_qty": result.get("exchange_qty"),
        "entry_price": result.get("entry_price"),
        "registry_ownership": record.get("ownership"),
        "registry_source": record.get("source"),
        "classification": classification,
        "reason": reason,
        "reconstruct": result.get("reconstruct"),
        "manage": result.get("manage"),
        "account": result.get("account"),
        "canary": result.get("canary"),
        "modify_orders": result.get("modify_orders"),
        "registry_status": result.get("registry_status"),
        "data_quality_flags": result.get("data_quality_flags") or [],
    }
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return True


def reset_audit_dedup_for_tests() -> None:
    _AUDIT_DEDUP_KEYS.clear()
