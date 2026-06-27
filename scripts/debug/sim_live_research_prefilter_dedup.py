"""Read-only simulator for LIVE CONFIRM_SMC_RESEARCH prefilter/dedup."""

import copy
import os
import sys
import types
from dataclasses import dataclass, field

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _install_execution_mode_stub():
    mode_mod = types.ModuleType("execution_mode")
    mode_mod.EXECUTION_MODE = "paper"
    mode_mod.TRADES_CSV = "paper_trades.csv"
    mode_mod.STATE_FILE = "paper_state.json"
    mode_mod.MODE_PREFIX = "[SIM]"
    mode_mod.validate_startup = lambda: None
    sys.modules["execution_mode"] = mode_mod


@dataclass
class DummyCtx:
    execution_mode: str = "live"
    trades: list = field(default_factory=list)


def _candidate(
    key,
    rr=2.25,
    score=7.0,
    symbol="BTCUSDT",
    side="LONG",
    bos_quality="STRONG",
    volume_confirmation="NORMAL",
):
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
            "bos_quality": bos_quality,
            "volume_confirmation": volume_confirmation,
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
    _install_execution_mode_stub()
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
            "low score research passes to OPEN_ATTEMPT",
            any(row["decision"] == "OPEN_ATTEMPT" for row in logs) and len(open_calls) == 1,
            str(logs),
        ))
        results.append(_assert(
            "low score research logs score bypass metadata",
            any(
                row.get("decision") == "OPEN_ATTEMPT"
                and row.get("score_filter_bypassed_for_research") is True
                and row.get("score_filter_original_threshold") == 7
                and row.get("score_filter_actual_score") == 2.0
                and row.get("paper_research_population_aligned") is True
                for row in logs
            ),
            str(logs),
        ))

        logs.clear()
        open_calls.clear()
        _run_dispatch(sd, [_candidate("low-score", rr=2.2, score=2.0)], logs, open_calls)
        results.append(_assert(
            "same accepted dedup_key is skipped after open",
            len(logs) == 0 and len(open_calls) == 0,
            f"logs={len(logs)} calls={len(open_calls)}",
        ))

        logs.clear()
        open_calls.clear()
        _run_dispatch(sd, [_candidate("rr-low", rr=2.2, score=7.0, bos_quality="WEAK")], logs, open_calls)
        results.append(_assert(
            "same rejected dedup_key + different reason logs once",
            len(logs) == 1 and logs[0]["reason"] == "research_predicate_fail",
            str(logs),
        ))

        logs.clear()
        open_calls.clear()
        _run_dispatch(sd, [_candidate("new-key", rr=2.2, score=2.0)], logs, open_calls)
        results.append(_assert(
            "new low score research key opens normally",
            any(row["decision"] == "OPEN_ATTEMPT" for row in logs) and len(open_calls) == 1,
            str(logs),
        ))

        logs.clear()
        open_calls.clear()
        sd._live_smc_research_terminal_failures.clear()
        _run_dispatch(sd, [_candidate("weak-bos", rr=2.2, score=2.0, bos_quality="WEAK")], logs, open_calls)
        results.append(_assert(
            "weak BOS still rejects research predicate",
            len(logs) == 1 and logs[0]["decision"] == "PREFILTER_REJECT" and logs[0]["reason"] == "research_predicate_fail",
            str(logs),
        ))
        results.append(_assert("weak BOS does not call open_trade", len(open_calls) == 0, f"calls={len(open_calls)}"))

        logs.clear()
        open_calls.clear()
        sd._live_smc_research_terminal_failures.clear()
        _run_dispatch(
            sd,
            [_candidate("expansion-volume", rr=2.2, score=2.0, volume_confirmation="EXPANSION")],
            logs,
            open_calls,
        )
        results.append(_assert(
            "volume EXPANSION still rejects research predicate",
            len(logs) == 1 and logs[0]["decision"] == "PREFILTER_REJECT" and logs[0]["reason"] == "research_predicate_fail",
            str(logs),
        ))
        results.append(_assert("volume EXPANSION does not call open_trade", len(open_calls) == 0, f"calls={len(open_calls)}"))

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
