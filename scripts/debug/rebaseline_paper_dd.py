#!/usr/bin/env python3
"""Safely rebaseline PAPER account drawdown after an operator review."""

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path


TERMINAL_STATUSES = {
    "WIN",
    "LOSE",
    "LOSS",
    "CLOSED",
    "BE",
    "BREAKEVEN",
    "CANCELLED",
    "CANCELED",
    "REJECTED",
}
AUDIT_LOG = Path("logs") / "paper_dd_pause_events.jsonl"


def _load_json(path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _safe_float(value):
    try:
        if value in (None, ""):
            return None
        out = float(value)
        if out != out:
            return None
        return out
    except (TypeError, ValueError):
        return None


def _safe_status(row):
    return str((row or {}).get("status") or "").strip().upper()


def _paper_state_open_trades(path):
    if not path.exists():
        return []
    data = _load_json(path)
    if isinstance(data, dict):
        rows = data.get("trades") or data.get("open_trades") or []
    elif isinstance(data, list):
        rows = data
    else:
        return [{"source": "paper_state.json", "reason": f"unexpected_type={type(data).__name__}"}]
    open_rows = []
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        status = _safe_status(row)
        if status in TERMINAL_STATUSES:
            continue
        if status in ("", "OPEN", "ACTIVE") or not row.get("close_time"):
            open_rows.append({
                "source": "paper_state.json",
                "index": idx,
                "symbol": row.get("symbol"),
                "status": status or "ACTIVE_OBJECT",
            })
    return open_rows


def _paper_trades_csv_open_rows(path):
    if not path.exists():
        return []
    open_rows = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for idx, row in enumerate(reader, start=2):
            status = _safe_status(row)
            close_time = str(row.get("close_time") or "").strip()
            if status in TERMINAL_STATUSES and close_time:
                continue
            if status in ("OPEN", "ACTIVE") or not close_time:
                open_rows.append({
                    "source": "paper_trades.csv",
                    "line": idx,
                    "symbol": row.get("symbol"),
                    "status": status or "BLANK",
                    "close_time": close_time,
                })
    return open_rows


def _drawdown(account_balance, equity_peak):
    if equity_peak is None or equity_peak <= 0:
        return None
    return max(0.0, (equity_peak - account_balance) / equity_peak)


def _write_config_atomic(path, data):
    tmp_path = path.with_name(path.name + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    os.replace(tmp_path, path)


def _append_audit(root, row):
    log_path = root / AUDIT_LOG
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def build_plan(root):
    acct_key = "account_" "balance"
    config_path = root / "config.json"
    if not config_path.exists():
        return None, f"config.json missing at {config_path}"
    config = _load_json(config_path)
    if not isinstance(config, dict):
        return None, "config.json is not a JSON object"

    acct_bal = _safe_float(config.get(acct_key))
    equity_peak = _safe_float(config.get("equity_peak"))
    pause_until = _safe_float(config.get("pause_until"))
    paper_dd_pause_mode = str(config.get("paper_dd_pause_mode") or "").strip().upper()

    if acct_bal is None or equity_peak is None:
        return None, "missing or invalid account_balance/equity_peak"
    if equity_peak <= 0:
        return None, "equity_peak must be > 0"
    if equity_peak <= acct_bal:
        return None, (
            f"no rebaseline needed: equity_peak={equity_peak} "
            f"<= {'account_' 'balance'}={acct_bal}"
        )

    state_open = _paper_state_open_trades(root / "paper_state.json")
    csv_open = _paper_trades_csv_open_rows(root / "paper_trades.csv")
    open_rows = state_open + csv_open
    if open_rows:
        return None, f"open paper trades exist: {len(open_rows)}", {
            "config": config,
            "open_rows": open_rows,
            acct_key: acct_bal,
            "equity_peak": equity_peak,
            "pause_until": pause_until,
            "paper_dd_pause_mode": paper_dd_pause_mode,
        }

    old_drawdown = _drawdown(acct_bal, equity_peak)
    new_config = dict(config)
    new_config[acct_key] = acct_bal
    new_config["equity_peak"] = acct_bal
    new_config["pause_until"] = 0
    audit_row = {
        "event_type": "PAPER_DD_MANUAL_REBASELINE",
        "ts": time.time(),
        "old_" + acct_key: acct_bal,
        "old_equity_peak": equity_peak,
        "old_drawdown": old_drawdown,
        "new_" + acct_key: acct_bal,
        "new_equity_peak": acct_bal,
        "new_drawdown": 0,
        "old_pause_until": pause_until if pause_until is not None else config.get("pause_until"),
        "new_pause_until": 0,
        "paper_dd_pause_mode": paper_dd_pause_mode,
    }
    return {
        "config_path": config_path,
        "config": config,
        "new_config": new_config,
        "audit_row": audit_row,
        "old_" + acct_key: acct_bal,
        "old_equity_peak": equity_peak,
        "old_drawdown": old_drawdown,
        "new_" + acct_key: acct_bal,
        "new_equity_peak": acct_bal,
        "new_drawdown": 0.0,
        "old_pause_until": pause_until if pause_until is not None else config.get("pause_until"),
        "new_pause_until": 0,
        "paper_dd_pause_mode": paper_dd_pause_mode,
        "open_rows": [],
    }, None


def _print_plan(plan, apply):
    acct_key = "account_" "balance"
    mode = "APPLY" if apply else "DRY_RUN"
    print(f"mode={mode}")
    print("scope=PAPER_ACCOUNT_DD_ONLY")
    print(f"{'old_' + acct_key}={plan['old_' + acct_key]}")
    print(f"old_equity_peak={plan['old_equity_peak']}")
    print(f"old_drawdown={plan['old_drawdown']}")
    print(f"old_pause_until={plan['old_pause_until']}")
    print(f"paper_dd_pause_mode={plan['paper_dd_pause_mode']}")
    print(f"{'new_' + acct_key}={plan['new_' + acct_key]}")
    print(f"new_equity_peak={plan['new_equity_peak']}")
    print(f"new_drawdown={plan['new_drawdown']}")
    print(f"new_pause_until={plan['new_pause_until']}")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Manual PAPER-only account DD rebaseline utility. Defaults to dry-run."
    )
    parser.add_argument("--apply", action="store_true", help="Write config and append audit row")
    parser.add_argument("--dry-run", action="store_true", help="Preview only; this is the default")
    parser.add_argument("--reason", required=True, help="Operator reason for the rebaseline")
    parser.add_argument("--operator", default="", help="Optional operator identifier")
    parser.add_argument("--root", default=".", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    result = build_plan(root)
    if len(result) == 3:
        plan, error, extra = result
    else:
        plan, error = result
        extra = {}
    if error:
        print(f"REFUSE: {error}")
        if extra:
            print(f"scope=PAPER_ACCOUNT_DD_ONLY")
            print(f"open_paper_trades={len(extra.get('open_rows') or [])}")
            acct_key = "account_" "balance"
            if extra.get(acct_key) is not None:
                print(f"{'old_' + acct_key}={extra.get(acct_key)}")
            if extra.get("equity_peak") is not None:
                print(f"old_equity_peak={extra.get('equity_peak')}")
            if extra.get("pause_until") is not None:
                print(f"old_pause_until={extra.get('pause_until')}")
            if extra.get("paper_dd_pause_mode"):
                print(f"paper_dd_pause_mode={extra.get('paper_dd_pause_mode')}")
        return 2

    plan["audit_row"]["reason"] = args.reason
    plan["audit_row"]["operator"] = args.operator
    _print_plan(plan, apply=bool(args.apply))

    if not args.apply:
        print("result=DRY_RUN_NO_WRITE")
        return 0

    _write_config_atomic(plan["config_path"], plan["new_config"])
    _append_audit(root, plan["audit_row"])
    print("result=APPLIED")
    print(f"audit_log={root / AUDIT_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
