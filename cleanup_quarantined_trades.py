"""
cleanup_quarantined_trades.py — Manual operator utility.

Lists all permanently quarantined trades from the local state file and
optionally removes them from the JSON permanently.

NEVER runs automatically — must be invoked manually by the operator.

Usage:
    python cleanup_quarantined_trades.py

Quarantined trades are identified by:
    quarantined=True  OR  repair_disabled=True

They remain in the state file until this utility removes them.
"""

import json
import os
import sys
import time

_BOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _BOT_DIR)


def _load_config() -> dict:
    path = os.path.join(_BOT_DIR, "config.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[CLEANUP] config.json read error: {e}")
        sys.exit(1)


def _resolve_state_file(cfg: dict) -> str:
    from execution_mode import STATE_FILE
    return STATE_FILE


def _fmt_ts(ts) -> str:
    if not ts:
        return "unknown"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(ts)))
    except Exception:
        return str(ts)


def main():
    cfg = _load_config()

    state_file = _resolve_state_file(cfg)

    if not os.path.exists(state_file):
        print(f"[CLEANUP] State file not found: {state_file}")
        return

    try:
        with open(state_file, "r", encoding="utf-8") as f:
            trades = json.load(f)
    except Exception as e:
        print(f"[CLEANUP] State file read error: {e}")
        sys.exit(1)

    quarantined = [
        t for t in trades
        if t.get("quarantined") or t.get("repair_disabled")
    ]
    active = [
        t for t in trades
        if not t.get("quarantined") and not t.get("repair_disabled")
    ]

    if not quarantined:
        print(f"[CLEANUP] No quarantined trades found in {state_file}")
        print(f"[CLEANUP] Total trades: {len(trades)}  Active: {len(active)}")
        return

    print(f"\n[CLEANUP] Found {len(quarantined)} quarantined trade(s) in {state_file}:\n")
    print(f"  {'#':<4} {'Symbol':<14} {'Reason':<34} {'Quarantine Time':<22} {'Status'}")
    print(f"  {'-'*4} {'-'*14} {'-'*34} {'-'*22} {'-'*8}")

    for i, t in enumerate(quarantined, 1):
        sym    = t.get("symbol", "UNKNOWN")
        reason = t.get("quarantine_reason") or ("invalid_symbol" if t.get("invalid_exchange_symbol") else "legacy")
        ts     = _fmt_ts(t.get("quarantine_timestamp"))
        status = t.get("status", "?")
        print(f"  {i:<4} {sym:<14} {reason:<34} {ts:<22} {status}")

    print(f"\n  Active trades: {len(active)}")
    print(f"  Total in file: {len(trades)}\n")

    confirm = input(
        "Remove ALL quarantined trades from state file? (yes/no): "
    ).strip().lower()

    if confirm != "yes":
        print("[CLEANUP] Aborted — no trades removed.")
        return

    try:
        from state_manager import save_open_trades
        save_open_trades(active, state_file)
    except Exception as e:
        print(f"[CLEANUP] Write error: {e}")
        sys.exit(1)

    print(
        f"[CLEANUP] Done — removed {len(quarantined)} quarantined trade(s). "
        f"{len(active)} active trade(s) remain."
    )
    for t in quarantined:
        sym    = t.get("symbol", "UNKNOWN")
        reason = t.get("quarantine_reason") or "legacy"
        print(f"  removed: {sym}  reason={reason}")


if __name__ == "__main__":
    main()
