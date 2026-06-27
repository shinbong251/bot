#!/usr/bin/env python3
"""Simulator for SL-audit close source labels and emergency-close fill capture."""

import ast
import copy
import math
import os
import time
from types import SimpleNamespace


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
EXECUTION_PATH = os.path.join(REPO_ROOT, "execution.py")

results = []
issues = []


def check(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((label, status, detail))
    if not condition:
        issues.append(f"[FAIL] {label}: {detail}")
    return condition


class FakeLiveExecutor:
    def __init__(self, close_result):
        self.close_result = close_result
        self.close_calls = []

    def emergency_close_position(self, **kwargs):
        self.close_calls.append(kwargs)
        return copy.deepcopy(self.close_result)

    def is_position_closed(self, symbol):
        return True


def load_scope(fake_executor=None):
    with open(EXECUTION_PATH, "r", encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=EXECUTION_PATH)
    wanted = {
        "_safe_float_value",
        "_safe_numeric_value",
        "_finalize_audit_exchange_sl_close",
        "_close_live_exchange_position_for_local_exit",
    }
    nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    found = {node.name for node in nodes}
    missing = wanted - found
    if missing:
        raise RuntimeError(f"missing execution functions: {sorted(missing)}")
    module = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    scope = {
        "math": math,
        "time": time,
        "RISK_PER_TRADE": 0.01,
        "stats": {"win": 0, "loss": 0},
        "history": [],
        "save_trade": lambda *args, **kwargs: None,
        "save_tier_log": lambda *args, **kwargs: None,
        "log_false_positive": lambda *args, **kwargs: None,
        "log_wyckoff_outcome": lambda *args, **kwargs: None,
        "save_open_trades": lambda *args, **kwargs: None,
        "fmt_price": lambda value, symbol: str(value),
        "send_telegram": lambda *args, **kwargs: None,
        "_resolve_exchange_executor": lambda _mode: fake_executor,
        "_cancel_live_remaining_stop": lambda *args, **kwargs: True,
    }
    exec(compile(module, EXECUTION_PATH, "exec"), scope)
    return scope


def base_trade(**overrides):
    trade = {
        "id": "sim-sl-audit",
        "owner": "bot",
        "symbol": "SIMUSDT",
        "side": "LONG",
        "entry": 100.0,
        "entry_real": 100.0,
        "sl_init": 95.0,
        "sl": 101.0,
        "status": "OPEN",
        "risk_percent": 0.01,
        "balance_at_entry": 1000.0,
        "time": 1000.0,
        "trail_phase": 3,
        "max_profit_r": 1.5,
        "exchange_sl_id": "SL_ID",
        "exchange_qty": 2.0,
    }
    trade.update(overrides)
    return trade


def ctx_for(trade):
    return SimpleNamespace(
        execution_mode="live",
        trades=[trade],
        state_file="sim_state.json",
        trades_csv="sim_trades.csv",
        account_balance=1000.0,
        equity_peak=1000.0,
        session_pnl_r=0.0,
        cooldown={},
        stats={},
        mode_prefix="[LIVE]",
        save_account_state=lambda: None,
    )


def finalize_case(trade):
    scope = load_scope()
    ctx = ctx_for(trade)
    ok = scope["_finalize_audit_exchange_sl_close"](trade, ctx)
    return ok, trade


def main():
    ok, trade = finalize_case(base_trade(exchange_exit_price=102.0, exit_price=101.0))
    check("SL audit exchange_exit_price => exchange_fill", ok and trade.get("exit_price_source") == "exchange_fill", str(trade))
    check("SL audit exchange_exit_price used as exit", trade.get("exit_price") == 102.0, str(trade))

    ok, trade = finalize_case(base_trade(exit_price=101.0))
    check("SL audit local exit_price only => sl_plus_slippage_estimate", ok and trade.get("exit_price_source") == "sl_plus_slippage_estimate", str(trade))
    check("SL audit local exit_price used", trade.get("exit_price") == 101.0, str(trade))

    ok, trade = finalize_case(base_trade(exchange_sl_price_confirmed=100.5))
    check("SL audit confirmed SL fallback => confirmed_sl", ok and trade.get("exit_price_source") == "confirmed_sl", str(trade))
    check("SL audit confirmed SL used", trade.get("exit_price") == 100.5, str(trade))

    fake = FakeLiveExecutor({"success": True, "order_id": "EC1", "client_order_id": "BOT_SIM_EC", "fill_price": 99.25})
    scope = load_scope(fake)
    trade = base_trade()
    ctx = ctx_for(trade)
    close_ok = scope["_close_live_exchange_position_for_local_exit"](trade, ctx, "SL")
    check("emergency_close_position success closes", close_ok is True, str(trade))
    check("emergency_close_position fill_price stores exchange_exit_price", trade.get("exchange_exit_price") == 99.25, str(trade))
    check("emergency_close_position stores fill source", trade.get("exchange_exit_source") == "emergency_close_position", str(trade))
    check("emergency_close_position stores fill timestamp", isinstance(trade.get("exchange_exit_price_ts"), float), str(trade))

    fake = FakeLiveExecutor({"success": True, "order_id": "EC2", "client_order_id": "BOT_SIM_EC", "raw": {"avgPrice": "98.75"}})
    scope = load_scope(fake)
    trade = base_trade()
    ctx = ctx_for(trade)
    close_ok = scope["_close_live_exchange_position_for_local_exit"](trade, ctx, "SL")
    check("emergency_close_position raw avgPrice closes", close_ok is True, str(trade))
    check("emergency_close_position raw avgPrice stores exchange_exit_price", trade.get("exchange_exit_price") == 98.75, str(trade))

    print("\n=== SL Audit Fill Source Simulator ===")
    for label, status, detail in results:
        suffix = f" — {detail}" if status == "FAIL" and detail else ""
        print(f"[{status}] {label}{suffix}")
    if issues:
        print("\nRESULT: FAIL")
        raise SystemExit(1)
    print("\nRESULT: PASS")


if __name__ == "__main__":
    main()
