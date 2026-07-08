#!/usr/bin/env python3
"""
Simulator for LIVE stop-update Telegram throttling.

No exchange, Telegram, .env, or production log writes are performed. The
execution wrapper is AST-loaded with a fake executor, fake clock, and temporary
JSONL sink.
"""

import ast
import copy
import json
import math
import os
import tempfile

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
EXECUTION_PATH = os.path.join(REPO_ROOT, "execution.py")

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


class FakeTime:
    def __init__(self, start=1000.0):
        self.now = float(start)

    def time(self):
        return self.now

    def advance(self, seconds):
        self.now += float(seconds)


class Ctx:
    execution_mode = "live"
    mode_prefix = "[LIVE]"


class FakeExecutor:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def update_trailing_stop(self, **kwargs):
        self.calls.append(kwargs)
        return copy.deepcopy(self.result)


class DummyDedup:
    @staticmethod
    def build_key(*parts):
        return "|".join("null" if part in (None, "") else str(part) for part in parts)


def load_scope(fake, messages, log_path, fake_time):
    with open(EXECUTION_PATH, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=EXECUTION_PATH)

    wanted = {
        "_safe_numeric_value",
        "_live_stop_update_failure_key",
        "_should_send_live_stop_update_failure_telegram",
        "_write_live_stop_update_failure_event",
        "_sync_testnet_trailing_sl",
    }
    fn_nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    found = {node.name for node in fn_nodes}
    missing = wanted - found
    if missing:
        raise RuntimeError(f"missing execution functions: {sorted(missing)}")

    module = ast.Module(body=fn_nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    scope = {
        "math": math,
        "os": os,
        "json": json,
        "time": fake_time,
        "telegram_dedup": DummyDedup,
        "_resolve_exchange_executor": lambda _mode: fake,
        "send_telegram": lambda msg, **kwargs: messages.append((msg, kwargs)),
        "_immediately_triggerable_alert_last_sent": {},
        "_IMMEDIATELY_TRIGGERABLE_ALERT_TTL_SECS": 300.0,
        "_LIVE_STOP_UPDATE_ALERT_THROTTLE_SECS": 300.0,
        "_LIVE_STOP_UPDATE_ALERT_MILESTONES": {3, 5, 10, 20, 50},
        "_live_stop_update_failed_telegram_state": {},
        "_LIVE_STOP_UPDATE_FAILURE_LOG": log_path,
    }
    exec(compile(module, EXECUTION_PATH, "exec"), scope)
    return scope


def base_trade():
    return {
        "symbol": "ASTERUSDT",
        "side": "LONG",
        "sl": 0.6336116071428572,
        "exchange_sl_id": "SL-123",
        "exchange_qty": 42.0,
        "exchange_sl_price_confirmed": 0.6316,
    }


def failure_result(old_protection_retained=True):
    return {
        "success": False,
        "new_order_id": None,
        "cancel_ok": False,
        "error": "simulated stop reject",
        "old_protection_retained": old_protection_retained,
    }


def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = os.path.join(tmpdir, "live_stop_update_failures.jsonl")

        fake_time = FakeTime()
        messages = []
        fake = FakeExecutor(failure_result())
        scope = load_scope(fake, messages, log_path, fake_time)
        sync = scope["_sync_testnet_trailing_sl"]

        t = base_trade()
        sync(t, Ctx(), old_sl=t["exchange_sl_price_confirmed"])
        check("A first failure sends Telegram", len(messages) == 1, str(messages))
        check("A first failure is critical", "Stop update failed" in messages[-1][0], messages[-1][0])

        for _ in range(9):
            fake_time.advance(10)
            sync(t, Ctx(), old_sl=t["exchange_sl_price_confirmed"])
        alert_counts = [
            msg for msg, _ in messages
            if "Stop update failed" in msg or "HIGH-SEVERITY stop update failure" in msg
        ]
        check(
            "B same failure 10 times under 5 minutes sends first plus milestones only",
            len(alert_counts) == 4,
            str(alert_counts),
        )
        check(
            "B milestone alerts are 3, 5, and 10",
            all(f"consecutive_failures={n}" in "\n".join(alert_counts) for n in (3, 5, 10)),
            "\n".join(alert_counts),
        )

        t["sl"] = 0.6342
        fake_time.advance(10)
        before = len(messages)
        sync(t, Ctx(), old_sl=t["exchange_sl_price_confirmed"])
        check("C target SL changes sends again", len(messages) == before + 1, str(messages[before:]))

        t["exchange_sl_price_confirmed"] = 0.6320
        fake_time.advance(10)
        before = len(messages)
        sync(t, Ctx(), old_sl=t["exchange_sl_price_confirmed"])
        check("D current confirmed SL changes sends again", len(messages) == before + 1, str(messages[before:]))

        unprotected_messages = []
        unprotected_fake_time = FakeTime()
        unprotected_fake = FakeExecutor(failure_result(old_protection_retained=False))
        unprotected_log = os.path.join(tmpdir, "unprotected_failures.jsonl")
        unprotected_scope = load_scope(
            unprotected_fake,
            unprotected_messages,
            unprotected_log,
            unprotected_fake_time,
        )
        unprotected_sync = unprotected_scope["_sync_testnet_trailing_sl"]
        u = base_trade()
        for _ in range(3):
            unprotected_sync(u, Ctx(), old_sl=u["exchange_sl_price_confirmed"])
            unprotected_fake_time.advance(10)
        check("E unprotected failures bypass throttle", len(unprotected_messages) == 3, str(unprotected_messages))
        check(
            "E unprotected text does not claim retained protection",
            all("NOT confirmed retained" in msg for msg, _ in unprotected_messages),
            str(unprotected_messages),
        )

        rows = read_jsonl(log_path)
        check("F JSONL log row written for every protected failure", len(rows) == 12, str(rows))
        check(
            "F throttled rows are still logged",
            any(row.get("telegram_alert_sent") is False for row in rows),
            str(rows),
        )
        check(
            "F dedup key includes requested fields",
            all(
                "live_stop_update_failed|ASTERUSDT|LONG|" in row.get("telegram_dedup_key", "")
                and str(row.get("current_confirmed_sl")) in row.get("telegram_dedup_key", "")
                and str(row.get("pending_target_sl")) in row.get("telegram_dedup_key", "")
                and str(row.get("exchange_sl_id")) in row.get("telegram_dedup_key", "")
                for row in rows
            ),
            str(rows),
        )

    print("\n=== LIVE Stop Update Alert Throttle Simulator ===")
    for label, status, detail in results:
        suffix = f" - {detail}" if status == FAIL and detail else ""
        print(f"[{status}] {label}{suffix}")

    if issues:
        print("\nRESULT: FAIL")
        raise SystemExit(1)
    print("\nRESULT: PASS")


if __name__ == "__main__":
    main()
