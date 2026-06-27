#!/usr/bin/env python3
"""
Simulator for LIVE/testnet trailing stop wrapper labels and persisted failure state.

No exchange calls are made. execution._resolve_exchange_executor and
execution.send_telegram are replaced with local fakes before invoking the wrapper.
"""

import ast
import copy
import math
import os
import time

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
EXECUTION_PATH = os.path.join(REPO_ROOT, "execution.py")
NOTIFIER_PATH = os.path.join(REPO_ROOT, "notifier.py")


PASS = "PASS"
FAIL = "FAIL"

results = []
issues = []


def check(label, condition, detail=""):
    status = PASS if condition else FAIL
    results.append((label, status, detail))
    if not condition:
        issues.append(f"[FAIL] {label}: {detail}")
    return condition


class Ctx:
    def __init__(self, execution_mode):
        self.execution_mode = execution_mode
        self.mode_prefix = f"[{execution_mode.upper()}]"


class FakeExecutor:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def update_trailing_stop(self, **kwargs):
        self.calls.append(kwargs)
        return copy.deepcopy(self.result)


def load_execution_functions(fake, messages):
    with open(EXECUTION_PATH, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=EXECUTION_PATH)

    wanted = {
        "_safe_numeric_value",
        "_sync_testnet_trailing_sl",
        "_exit_text_for_telegram",
    }
    fn_nodes = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            fn_nodes.append(node)
    found = {node.name for node in fn_nodes}
    missing = wanted - found
    if missing:
        raise RuntimeError(f"missing execution functions: {sorted(missing)}")

    module = ast.Module(body=fn_nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    scope = {
        "math": math,
        "time": time,
        "_resolve_exchange_executor": lambda _mode: fake,
        "send_telegram": lambda msg, **kwargs: messages.append((msg, kwargs)),
    }
    exec(compile(module, EXECUTION_PATH, "exec"), scope)
    return scope


def base_trade():
    return {
        "symbol": "SIMUSDT",
        "side": "LONG",
        "sl": 105.0,
        "exchange_sl_id": "OLD_STOP_ID",
        "exchange_qty": 12.5,
        "exchange_sl_price_confirmed": 100.0,
    }


def run_case(mode, result, trade=None, current_price=None):
    fake = FakeExecutor(result)
    messages = []
    scope = load_execution_functions(fake, messages)
    sync_fn = scope["_sync_testnet_trailing_sl"]
    t = copy.deepcopy(trade or base_trade())
    sync_result = sync_fn(t, Ctx(mode), old_sl=100.0, current_price=current_price)
    return t, sync_result, messages, fake.calls


def label_case(exit_type, rr):
    scope = load_execution_functions(FakeExecutor({}), [])
    return scope["_exit_text_for_telegram"](exit_type, rr)


def paper_label_case(exit_type, rr):
    with open(NOTIFIER_PATH, "r", encoding="utf-8-sig") as f:
        tree = ast.parse(f.read(), filename=NOTIFIER_PATH)
    wanted = {"_safe_float", "_paper_close_reason_text"}
    fn_nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    found = {node.name for node in fn_nodes}
    missing = wanted - found
    if missing:
        raise RuntimeError(f"missing notifier functions: {sorted(missing)}")
    module = ast.Module(body=fn_nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    scope = {}
    exec(compile(module, NOTIFIER_PATH, "exec"), scope)
    return scope["_paper_close_reason_text"](exit_type, rr)


def main():
    fail_result = {"success": False, "new_order_id": None, "cancel_ok": False, "error": "simulated stop reject"}
    success_result = {"success": True, "new_order_id": "NEW_STOP_ID", "cancel_ok": True, "error": None}

    live_t, live_ok, live_messages, live_calls = run_case("live", fail_result)
    live_text = "\n".join(msg for msg, _ in live_messages)
    check("live failure returns False", live_ok is False, f"sync_result={live_ok}")
    check("live failure uses LIVE label", "[LIVE CRITICAL]" in live_text, live_text)
    check("live failure does not use TESTNET label", "[TESTNET CRITICAL]" not in live_text, live_text)
    check("live failure persists error", live_t.get("exchange_sl_sync_error") == "simulated stop reject", str(live_t))
    check("live failure persists error ts", isinstance(live_t.get("exchange_sl_sync_error_ts"), float), str(live_t))
    check("live failure keeps pending behavior", live_t.get("exchange_sl_sync_pending") == 105.0, str(live_t))
    check("live failure increments count", live_t.get("sl_sync_fail_count") == 1, str(live_t))
    check("live failure keeps old protection text", "Old protection retained" in live_text, live_text)
    check("live failure uses one exchange wrapper call", len(live_calls) == 1, str(live_calls))

    testnet_t, testnet_ok, testnet_messages, _ = run_case("testnet", fail_result)
    testnet_text = "\n".join(msg for msg, _ in testnet_messages)
    check("testnet failure returns False", testnet_ok is False, f"sync_result={testnet_ok}")
    check("testnet failure keeps TESTNET label", "[TESTNET CRITICAL]" in testnet_text, testnet_text)
    check("testnet failure persists error", testnet_t.get("exchange_sl_sync_error") == "simulated stop reject", str(testnet_t))

    prefailed = base_trade()
    prefailed["exchange_sl_sync_pending"] = 104.0
    prefailed["exchange_sl_sync_error"] = "previous failure"
    prefailed["exchange_sl_sync_error_ts"] = 1.0
    prefailed["sl_sync_fail_count"] = 2
    success_t, success_ok, success_messages, _ = run_case("live", success_result, prefailed)
    success_text = "\n".join(msg for msg, _ in success_messages)
    check("success returns True", success_ok is True, f"sync_result={success_ok}")
    check("success clears pending", "exchange_sl_sync_pending" not in success_t, str(success_t))
    check("success clears error", "exchange_sl_sync_error" not in success_t, str(success_t))
    check("success clears error ts", "exchange_sl_sync_error_ts" not in success_t, str(success_t))
    check("success resets fail count", success_t.get("sl_sync_fail_count") == 0, str(success_t))
    check("success does not send alert", not success_text, success_text)

    repeated = base_trade()
    repeated["sl_sync_fail_count"] = 2
    repeated_t, _, repeated_messages, _ = run_case("live", fail_result, repeated)
    repeated_text = "\n".join(msg for msg, _ in repeated_messages)
    check("third failure increments count", repeated_t.get("sl_sync_fail_count") == 3, str(repeated_t))
    check("third failure sends high severity", "HIGH-SEVERITY" in repeated_text, repeated_text)
    check("high severity says protection retained", "old protection still retained" in repeated_text, repeated_text)
    check("high severity includes current confirmed SL", "current_confirmed_sl=100.0" in repeated_text, repeated_text)
    check("high severity includes pending target SL", "pending_target_sl=105.0" in repeated_text, repeated_text)

    short_triggerable = base_trade()
    short_triggerable["side"] = "SHORT"
    short_triggerable["sl"] = 95.0
    short_t, short_ok, short_messages, short_calls = run_case(
        "live", success_result, short_triggerable, current_price=95.0
    )
    short_text = "\n".join(msg for msg, _ in short_messages)
    check("SHORT current >= stop skips update", short_ok is False and not short_calls, str(short_calls))
    check("SHORT skip persists reason", short_t.get("exchange_sl_sync_skipped_reason") == "immediately_triggerable", str(short_t))
    check("SHORT skip persists pending stop", short_t.get("exchange_sl_sync_pending") == 95.0, str(short_t))
    check("SHORT skip sends TRAIL not CRITICAL", "[LIVE TRAIL]" in short_text and "CRITICAL" not in short_text, short_text)
    check("SHORT local hit_sl remains true", 95.0 >= short_t["sl"], str(short_t))

    short_normal = base_trade()
    short_normal["side"] = "SHORT"
    short_normal["sl"] = 95.0
    short_normal_t, short_normal_ok, _, short_normal_calls = run_case(
        "live", success_result, short_normal, current_price=94.9
    )
    check("SHORT current < stop uses normal update", short_normal_ok is True and len(short_normal_calls) == 1, str(short_normal_calls))
    check("SHORT normal clears skip reason", "exchange_sl_sync_skipped_reason" not in short_normal_t, str(short_normal_t))

    long_triggerable = base_trade()
    long_triggerable["side"] = "LONG"
    long_triggerable["sl"] = 105.0
    long_t, long_ok, long_messages, long_calls = run_case(
        "testnet", success_result, long_triggerable, current_price=105.0
    )
    long_text = "\n".join(msg for msg, _ in long_messages)
    check("LONG current <= stop skips update", long_ok is False and not long_calls, str(long_calls))
    check("LONG skip uses TESTNET TRAIL", "[TESTNET TRAIL]" in long_text and "CRITICAL" not in long_text, long_text)
    check("LONG local hit_sl remains true", 105.0 <= long_t["sl"], str(long_t))

    long_normal = base_trade()
    long_normal["side"] = "LONG"
    long_normal["sl"] = 105.0
    _, long_normal_ok, _, long_normal_calls = run_case(
        "live", success_result, long_normal, current_price=105.1
    )
    check("LONG current > stop uses normal update", long_normal_ok is True and len(long_normal_calls) == 1, str(long_normal_calls))

    profitable_sl_label = label_case("SL", 0.68)
    losing_sl_label = label_case("SL", -0.2)
    check("profitable SL label is profit-lock", profitable_sl_label == "Chốt lời bằng SL", profitable_sl_label)
    check("profitable SL label is not cut loss", profitable_sl_label != "Cắt lỗ", profitable_sl_label)
    check("losing SL label stays cut loss", losing_sl_label == "Cắt lỗ", losing_sl_label)
    check("paper profitable SL label is profit-lock", paper_label_case("SL", 0.68) == "Chốt lời bằng SL")
    check("paper losing SL label stays raw SL", paper_label_case("SL", -0.2) == "SL")

    print("\n=== Trailing Stop Label Simulator ===")
    for label, status, detail in results:
        suffix = f" — {detail}" if status == FAIL and detail else ""
        print(f"[{status}] {label}{suffix}")

    if issues:
        print("\nRESULT: FAIL")
        raise SystemExit(1)
    print("\nRESULT: PASS")


if __name__ == "__main__":
    main()
