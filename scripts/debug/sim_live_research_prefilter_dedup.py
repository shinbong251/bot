"""Read-only simulator for LIVE CONFIRM_SMC_RESEARCH prefilter/dedup."""

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


def _candidate(key, rr=2.25, score=7.0, symbol="BTCUSDT", side="LONG"):
    entry = 100.0
    sl = 90.0 if side == "LONG" else 110.0
    tp = 125.0 if side == "LONG" else 75.0
    return {
        "symbol": symbol,
        "side": side,
        "dedup_key": key,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "reason": "ACCEPTED_CONFIRM",
        "source_timestamp": key,
        "structural_context": {
            "score_v2_structural_shadow": score,
            "bos_quality": "STRONG",
            "volume_confirmation": "NORMAL",
        },
    }


def _install_fake_modules(candidates, open_calls):
    entry_mod = types.ModuleType("entry")
    entry_mod.get_confirm_structural_outcome_candidates_snapshot = lambda: copy.deepcopy(candidates)
    sys.modules["entry"] = entry_mod

    execution_mod = types.ModuleType("execution")

    def fake_open_trade(trade, ctx):
        open_calls.append(copy.deepcopy(trade))
        return True

    execution_mod.open_trade = fake_open_trade
    sys.modules["execution"] = execution_mod


def _run_dispatch(sd, candidates, logs, open_calls, ctx=None):
    ctx = ctx or DummyCtx()
    _install_fake_modules(candidates, open_calls)
    sd._dispatch_live_smc_research_lane(ctx)


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<62} {status} {detail}")
    return bool(condition)


def main():
    import signal_dispatcher as sd
    from exchange import live_executor

    original_config = dict(sd.config)
    original_log = sd._live_smc_research_log
    original_gate = live_executor.check_live_research_safety_gate

    logs = []
    open_calls = []

    def fake_log(candidate, decision, reason="", trade=None, extra=None):
        row = {
            "decision": decision,
            "reason": reason,
            "dedup_key": candidate.get("dedup_key"),
            "entry_type": trade.get("entry_type") if isinstance(trade, dict) else None,
        }
        if isinstance(extra, dict):
            row.update(extra)
        logs.append(row)

    def fake_gate(trade, ctx=None, open_trades=None):
        return True, "OK"

    try:
        sd.config["live_smc_research_enabled"] = True
        sd.config["live_mode"] = True
        sd.config["live_confirm_enabled"] = False
        sd._live_smc_research_log = fake_log
        sd._live_smc_research_terminal_failures.clear()
        sd._live_smc_research_dedup_keys.clear()
        live_executor.check_live_research_safety_gate = fake_gate

        results = []

        logs.clear()
        open_calls.clear()
        _run_dispatch(sd, [_candidate("rr-low", rr=1.5, score=7.0)], logs, open_calls)
        results.append(_assert(
            "rr < 2 logs PREFILTER_REJECT",
            len(logs) == 1 and logs[0]["decision"] == "PREFILTER_REJECT" and logs[0]["reason"] == "rr_below_min",
            str(logs),
        ))
        results.append(_assert("rr < 2 does not call open_trade", len(open_calls) == 0, f"calls={len(open_calls)}"))

        logs.clear()
        open_calls.clear()
        sd._live_smc_research_terminal_failures.clear()
        _run_dispatch(sd, [_candidate("low-score", rr=2.2, score=2.0)], logs, open_calls)
        results.append(_assert(
            "low score logs PREFILTER_REJECT",
            len(logs) == 1 and logs[0]["decision"] == "PREFILTER_REJECT" and logs[0]["reason"] == "low_score",
            str(logs),
        ))
        results.append(_assert("low score does not call open_trade", len(open_calls) == 0, f"calls={len(open_calls)}"))

        logs.clear()
        open_calls.clear()
        _run_dispatch(sd, [_candidate("low-score", rr=2.2, score=2.0)], logs, open_calls)
        results.append(_assert(
            "same dedup_key + same reason suppressed",
            len(logs) == 0 and len(open_calls) == 0,
            f"logs={len(logs)} calls={len(open_calls)}",
        ))

        logs.clear()
        open_calls.clear()
        _run_dispatch(sd, [_candidate("low-score", rr=1.5, score=7.0)], logs, open_calls)
        results.append(_assert(
            "same dedup_key + different reason logs once",
            len(logs) == 1 and logs[0]["reason"] == "rr_below_min",
            str(logs),
        ))

        logs.clear()
        open_calls.clear()
        _run_dispatch(sd, [_candidate("new-key", rr=2.2, score=2.0)], logs, open_calls)
        results.append(_assert(
            "new dedup_key logs normally",
            len(logs) == 1 and logs[0]["reason"] == "low_score",
            str(logs),
        ))

        logs.clear()
        open_calls.clear()
        sd._live_smc_research_terminal_failures.clear()
        _run_dispatch(sd, [_candidate("valid-key", rr=2.2, score=7.0)], logs, open_calls)
        results.append(_assert(
            "valid candidate proceeds to OPEN_ATTEMPT",
            any(row["decision"] == "OPEN_ATTEMPT" for row in logs) and len(open_calls) == 1,
            f"logs={logs} calls={len(open_calls)}",
        ))

        logs.clear()
        open_calls.clear()
        sd.config["live_smc_research_enabled"] = False
        _run_dispatch(sd, [_candidate("disabled", rr=2.2, score=7.0)], logs, open_calls)
        results.append(_assert(
            "paper research path unaffected by live dispatcher disabled guard",
            len(logs) == 0 and len(open_calls) == 0,
            f"logs={len(logs)} calls={len(open_calls)}",
        ))
        sd.config["live_smc_research_enabled"] = True

        trade = sd._paper_smc_research_trade(_candidate("normal-confirm", rr=2.2, score=7.0))
        trade["entry_type"] = "CONFIRM"
        ok, _, reason, _ = sd._live_smc_research_prefilter(_candidate("normal-confirm"), trade, DummyCtx())
        results.append(_assert(
            "normal CONFIRM live path not routed through research prefilter",
            ok is False and reason == "entry_type_not_confirm_smc_research",
            f"research_prefilter_reason={reason}",
        ))

        total = len(results)
        passed = sum(1 for item in results if item)
        print("=" * 80)
        print(f"SUMMARY: {passed}/{total} PASS")
        return 0 if passed == total else 1

    finally:
        sd.config.clear()
        sd.config.update(original_config)
        sd._live_smc_research_log = original_log
        live_executor.check_live_research_safety_gate = original_gate
        sd._live_smc_research_terminal_failures.clear()


if __name__ == "__main__":
    raise SystemExit(main())
