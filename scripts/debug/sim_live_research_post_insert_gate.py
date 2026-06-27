"""Read-only simulator for LIVE CONFIRM_SMC_RESEARCH post-insert gate."""

import copy
import os
import sys
import types
from dataclasses import dataclass, field

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


@dataclass
class DummyCtx:
    execution_mode: str = "live"
    trades: list = field(default_factory=list)
    live_pending_slots: int = 0


def _research_trade(symbol="BTCUSDT"):
    return {
        "symbol": symbol,
        "side": "LONG",
        "status": "OPEN",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "strategy_family": "confirm_smc_research",
        "entry": 100.0,
        "sl": 90.0,
        "tp": 125.0,
        "rr": 2.5,
        "risk_percent": 0.001,
    }


def _confirm_trade(symbol="ETHUSDT"):
    return {
        "symbol": symbol,
        "side": "LONG",
        "status": "OPEN",
        "entry_type": "CONFIRM",
        "entry": 100.0,
        "sl": 90.0,
        "tp": 125.0,
        "risk_percent": 0.001,
        "exhaustion_cls": "HEALTHY",
        "bos_type": "NEAR",
        "execution_mode": "live",
    }


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<78} {status} {detail}")
    return bool(condition)


class FakeExecutor:
    def __init__(self, live_executor):
        self.live_executor = live_executor
        self.confirm_calls = []
        self.research_calls = []

    def check_live_research_safety_gate(self, *args, **kwargs):
        self.research_calls.append((args, kwargs))
        return self.live_executor.check_live_research_safety_gate(*args, **kwargs)

    def check_live_safety_gate(self, **kwargs):
        self.confirm_calls.append(kwargs)
        return True, "OK"


def main():
    sys.modules.setdefault("numpy", types.ModuleType("numpy"))
    sys.modules.setdefault("pandas", types.ModuleType("pandas"))

    import execution
    from exchange import live_executor

    original_load_config = live_executor._load_config
    original_tier5 = live_executor._live_symbol_is_tier5
    original_exec_slot_snapshot = execution._live_slot_snapshot
    original_exec_max_live = execution._get_max_live_trades
    original_send_telegram = execution.send_telegram
    original_time = execution.time.time
    original_plan = live_executor.calculate_execution_plan

    sent = []
    now = [1000.0]

    def fake_load_config():
        return {
            "live_mode": True,
            "live_smc_research_enabled": True,
            "max_live_research_trades": 1,
            "max_live_trades": 1,
            "live_risk_per_trade": 0.001,
            "live_max_portfolio_risk": 0.003,
            "use_exchange_max_leverage": False,
        }

    def fake_send_telegram(msg, prefix=None, channel=None):
        sent.append({"msg": msg, "prefix": prefix, "channel": channel})
        return True

    def fake_qty_floor_plan(symbol, balance, risk_percent, entry, sl):
        return {
            "valid": False,
            "reason": "qty floors to 0.0 - raw_qty 0.001 below stepSize 1.0",
        }

    try:
        live_executor._load_config = fake_load_config
        live_executor._live_symbol_is_tier5 = lambda symbol: False
        execution._get_max_live_trades = lambda: 1
        execution._live_slot_snapshot = lambda open_trades, ctx=None, exclude_trade=None: (
            len([t for t in open_trades if t is not exclude_trade and t.get("status") == "OPEN"]),
            0,
            len([t for t in open_trades if t is not exclude_trade and t.get("status") == "OPEN"]),
        )
        execution.send_telegram = fake_send_telegram
        execution.time.time = lambda: now[0]

        results = []

        current = _research_trade("BTCUSDT")
        existing = _research_trade("ETHUSDT")
        ctx = DummyCtx(trades=[existing])
        allowed, reason = live_executor.check_live_research_safety_gate(
            current,
            ctx=ctx,
            open_trades=ctx.trades,
        )
        results.append(_assert(
            "pre-insert blocks when one different research trade is already open",
            allowed is False and "live_research_open=1" in reason,
            reason,
        ))

        current = _research_trade("BTCUSDT")
        fake = FakeExecutor(live_executor)
        allowed, reason = execution._check_live_runtime_safety_gate(
            fake,
            current,
            [current],
            current["risk_percent"],
            0.0,
            ctx=DummyCtx(trades=[current]),
            post_insert=True,
        )
        results.append(_assert(
            "post-insert does not self-block on only the current research trade",
            allowed is True,
            reason,
        ))

        current = _research_trade("BTCUSDT")
        existing = _research_trade("ETHUSDT")
        fake = FakeExecutor(live_executor)
        allowed, reason = execution._check_live_runtime_safety_gate(
            fake,
            current,
            [existing, current],
            current["risk_percent"],
            0.0,
            ctx=DummyCtx(trades=[existing, current]),
            post_insert=True,
        )
        results.append(_assert(
            "post-insert blocks with current plus one different research trade",
            allowed is False and "live_research_open=1" in reason,
            reason,
        ))

        confirm = _confirm_trade()
        fake = FakeExecutor(live_executor)
        allowed, reason = execution._check_live_runtime_safety_gate(
            fake,
            confirm,
            [confirm],
            confirm["risk_percent"],
            0.0,
            ctx=DummyCtx(trades=[confirm]),
            post_insert=True,
        )
        results.append(_assert(
            "normal CONFIRM live path still routes to standard live safety gate",
            allowed is True and len(fake.confirm_calls) == 1 and len(fake.research_calls) == 0,
            f"confirm_calls={len(fake.confirm_calls)} research_calls={len(fake.research_calls)} reason={reason}",
        ))

        paper_ctx = DummyCtx(execution_mode="paper", trades=[])
        allowed, reason = live_executor.check_live_research_safety_gate(
            _research_trade("BTCUSDT"),
            ctx=paper_ctx,
            open_trades=paper_ctx.trades,
        )
        results.append(_assert(
            "paper mode remains outside live research gate",
            allowed is False and "execution_mode='paper'" in reason,
            reason,
        ))

        live_executor.calculate_execution_plan = fake_qty_floor_plan
        floor_result = live_executor.validate_and_prepare(
            "LABUSDT",
            "BUY",
            1.0,
            0.9,
            1.25,
            50.0,
            0.001,
        )
        results.append(_assert(
            "LABUSDT-like qty floor validation remains fail-closed",
            floor_result.get("valid") is False and "qty floors to 0.0" in floor_result.get("reason", ""),
            str(floor_result),
        ))

        sent.clear()
        execution._live_safety_block_telegram_last_sent.clear()
        first = execution._send_live_safety_block_telegram(
            "BTCUSDT",
            "live_research_open=1 >= max_live_research_trades=1",
            prefix="[LIVE]",
        )
        repeat = execution._send_live_safety_block_telegram(
            "BTCUSDT",
            "live_research_open=1 >= max_live_research_trades=1",
            prefix="[LIVE]",
        )
        different_reason = execution._send_live_safety_block_telegram(
            "BTCUSDT",
            "validate failed: qty floors to 0.0 - raw_qty 0.001 below stepSize 1.0",
            prefix="[LIVE]",
        )
        different_symbol = execution._send_live_safety_block_telegram(
            "ETHUSDT",
            "live_research_open=1 >= max_live_research_trades=1",
            prefix="[LIVE]",
        )
        results.append(_assert(
            "LIVE SAFETY BLOCK cooldown sends first and suppresses identical repeat",
            first is True and repeat is False and len(sent) == 3,
            f"sent={len(sent)} first={first} repeat={repeat}",
        ))
        results.append(_assert(
            "LIVE SAFETY BLOCK cooldown allows different reason or symbol",
            different_reason is True and different_symbol is True,
            f"different_reason={different_reason} different_symbol={different_symbol}",
        ))

        now[0] += execution._LIVE_SAFETY_BLOCK_TELEGRAM_TTL_SECS + 1
        after_ttl = execution._send_live_safety_block_telegram(
            "BTCUSDT",
            "live_research_open=1 >= max_live_research_trades=1",
            prefix="[LIVE]",
        )
        results.append(_assert(
            "LIVE SAFETY BLOCK cooldown expires after TTL",
            after_ttl is True,
            f"sent={len(sent)}",
        ))

        total = len(results)
        passed = sum(1 for item in results if item)
        print("=" * 80)
        print(f"SUMMARY: {passed}/{total} PASS")
        return 0 if passed == total else 1

    finally:
        live_executor._load_config = original_load_config
        live_executor._live_symbol_is_tier5 = original_tier5
        live_executor.calculate_execution_plan = original_plan
        execution._live_slot_snapshot = original_exec_slot_snapshot
        execution._get_max_live_trades = original_exec_max_live
        execution.send_telegram = original_send_telegram
        execution.time.time = original_time
        execution._live_safety_block_telegram_last_sent.clear()


if __name__ == "__main__":
    raise SystemExit(main())
