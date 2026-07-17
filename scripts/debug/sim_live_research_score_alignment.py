"""Read-only simulator for LIVE CONFIRM_SMC_RESEARCH score alignment."""

import copy
import os
import sys
import threading
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


def _install_optional_module_stubs():
    sys.modules.setdefault("numpy", types.ModuleType("numpy"))
    sys.modules.setdefault("pandas", types.ModuleType("pandas"))


@dataclass
class DispatchCtx:
    execution_mode: str = "live"
    trades: list = field(default_factory=list)


@dataclass
class ExecutionCtx:
    execution_mode: str = "live"
    trades: list = field(default_factory=list)
    entry_cooldown: dict = field(default_factory=dict)
    cooldown: dict = field(default_factory=dict)
    signal_state: dict = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    mode_prefix: str = "[SIM]"
    state_file: str = ""
    account_balance: float = 1000.0
    stats: dict = field(default_factory=dict)
    confirm_count_this_cycle: int = 0
    early_count: int = 0
    live_pending_slots: int = 0


def _candidate(
    key,
    rr=2.25,
    score=2.0,
    entry_type="CONFIRM_SMC_RESEARCH",
    bos_quality="STRONG",
    volume_confirmation="NORMAL",
):
    return {
        "symbol": f"{key.upper().replace('-', '')}USDT",
        "side": "SHORT",
        "dedup_key": key,
        "entry": 100.0,
        "sl": 110.0,
        "tp": 75.0,
        "rr": rr,
        "reason": "ACCEPTED_CONFIRM",
        "source_timestamp": key,
        "entry_type": entry_type,
        "structural_context": {
            "score_v2_structural_shadow": score,
            "bos_quality": bos_quality,
            "volume_confirmation": volume_confirmation,
        },
    }


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<78} {status} {detail}")
    return bool(condition)


def _install_dispatch_modules(candidates, open_calls):
    entry_mod = types.ModuleType("entry")
    entry_mod.get_confirm_structural_outcome_candidates_snapshot = lambda: copy.deepcopy(candidates)
    sys.modules["entry"] = entry_mod

    execution_mod = types.ModuleType("execution")

    def fake_open_trade(trade, ctx):
        open_calls.append(copy.deepcopy(trade))
        return True

    execution_mod.LIVE_RESEARCH_LOW_SCORE_RISK_MULTIPLIER = 0.6
    execution_mod.open_trade = fake_open_trade
    sys.modules["execution"] = execution_mod


def _run_dispatch(sd, candidates, logs, open_calls, gate_result=(True, "OK")):
    from exchange import live_executor
    from exchange import precision

    original_gate = live_executor.check_live_research_safety_gate
    original_get_symbol_filters = precision.get_symbol_filters
    original_log = sd._live_smc_research_log

    def fake_gate(trade, ctx=None, open_trades=None):
        return gate_result

    def fake_filters(symbol):
        return {"min_notional": 1.0}

    def fake_log(candidate, decision, reason="", trade=None, extra=None):
        row = {
            "decision": decision,
            "reason": reason,
            "dedup_key": candidate.get("dedup_key"),
            "score": trade.get("score") if isinstance(trade, dict) else None,
            "entry_type": trade.get("entry_type") if isinstance(trade, dict) else None,
        }
        if isinstance(extra, dict):
            row.update(extra)
        logs.append(row)

    try:
        live_executor.check_live_research_safety_gate = fake_gate
        precision.get_symbol_filters = fake_filters
        sd._live_smc_research_log = fake_log
        _install_dispatch_modules(candidates, open_calls)
        sd._dispatch_live_smc_research_lane(DispatchCtx())
    finally:
        live_executor.check_live_research_safety_gate = original_gate
        precision.get_symbol_filters = original_get_symbol_filters
        sd._live_smc_research_log = original_log


def _trade(score=2.0, entry_type="CONFIRM_SMC_RESEARCH", strategy_family="confirm_smc_research"):
    trade = {
        "symbol": "ALIGNUSDT",
        "side": "SHORT",
        "entry": 100.0,
        "sl": 110.0,
        "tp": 75.0,
        "rr": 2.5,
        "score": score,
        "entry_type": entry_type,
        "strategy_family": strategy_family,
        "research_source": "confirm_structural_outcome_shadow",
        "status": "OPEN",
    }
    if strategy_family is None:
        trade.pop("strategy_family", None)
    return trade


def _run_execution_until_cooldown(execution, trade, mode="live", disable_canary=False):
    original_canary_preflight_open = execution.canary_preflight_open
    try:
        if disable_canary:
            execution.canary_preflight_open = lambda *args, **kwargs: {"enabled": False, "ok": True}
        ctx = ExecutionCtx(execution_mode=mode)
        ctx.entry_cooldown[trade["symbol"]] = execution.time.time()
        execution.open_trade(trade, ctx=ctx)
        return trade
    finally:
        execution.canary_preflight_open = original_canary_preflight_open


def _run_execution_until_validate(execution, trade, mode="live"):
    calls = []

    class FakeExecutor:
        def get_execution_balance(self):
            return 50.0

        def validate_and_prepare(self, **kwargs):
            calls.append(dict(kwargs))
            return {"valid": False, "reason": "sim_stop_after_validate", "plan": {}}

    original_resolve_executor = execution._resolve_exchange_executor
    original_check_signal_cooldown = execution.check_signal_cooldown
    original_create_live_slot_reservation = execution._create_live_slot_reservation
    original_release_live_slot_reservation = execution._release_live_slot_reservation
    original_check_live_runtime_safety_gate = execution._check_live_runtime_safety_gate
    original_canary_preflight_open = execution.canary_preflight_open
    original_send_live_safety_block_telegram = execution._send_live_safety_block_telegram

    try:
        execution._resolve_exchange_executor = lambda exec_mode: FakeExecutor()
        execution.check_signal_cooldown = lambda *args, **kwargs: True
        execution._create_live_slot_reservation = lambda *args, **kwargs: True
        execution._release_live_slot_reservation = lambda *args, **kwargs: None
        execution._check_live_runtime_safety_gate = lambda *args, **kwargs: (True, "OK")
        execution.canary_preflight_open = lambda *args, **kwargs: {"enabled": False, "ok": True}
        execution._send_live_safety_block_telegram = lambda *args, **kwargs: None
        ctx = ExecutionCtx(execution_mode=mode)
        execution.open_trade(trade, ctx=ctx)
        return trade, calls
    finally:
        execution._resolve_exchange_executor = original_resolve_executor
        execution.check_signal_cooldown = original_check_signal_cooldown
        execution._create_live_slot_reservation = original_create_live_slot_reservation
        execution._release_live_slot_reservation = original_release_live_slot_reservation
        execution._check_live_runtime_safety_gate = original_check_live_runtime_safety_gate
        execution.canary_preflight_open = original_canary_preflight_open
        execution._send_live_safety_block_telegram = original_send_live_safety_block_telegram


def main():
    _install_execution_mode_stub()
    _install_optional_module_stubs()
    import signal_dispatcher as sd
    import execution

    original_sd_config = dict(sd.config)
    original_execution_config = dict(execution.config)
    original_entry_cooldown = execution.ENTRY_COOLDOWN
    original_check_correlation = execution.check_correlation

    try:
        sd.config["live_smc_research_enabled"] = True
        sd.config["live_mode"] = True
        execution.config["live_smc_research_enabled"] = True
        execution.config["live_risk_per_trade"] = 0.01
        execution.config["live_max_portfolio_risk"] = 0.02
        execution.ENTRY_COOLDOWN = 900
        execution.check_correlation = lambda trades, side: True

        results = []

        sd._live_smc_research_terminal_failures.clear()
        sd._live_smc_research_dedup_keys.clear()
        logs = []
        open_calls = []
        _run_dispatch(sd, [_candidate("score-pass", rr=2.2, score=2.0)], logs, open_calls)
        results.append(_assert(
            "live research rr>=2 score=2 reaches OPEN_ATTEMPT",
            any(row.get("decision") == "OPEN_ATTEMPT" for row in logs) and len(open_calls) == 1,
            f"logs={logs} calls={len(open_calls)}",
        ))
        results.append(_assert(
            "OPEN_ATTEMPT logs paper-aligned score bypass fields",
            any(
                row.get("score_filter_bypassed_for_research") is True
                and row.get("score_filter_original_threshold") == 7
                and row.get("score_filter_actual_score") == 2.0
                and row.get("paper_research_population_aligned") is True
                for row in logs
            ),
            str(logs),
        ))

        for label, candidate, expected_reason in (
            ("live research rr<2 still rejects", _candidate("rr-fail", rr=1.5, score=2.0), "rr_below_min"),
            (
                "live research WEAK BOS still rejects",
                _candidate("weak-fail", rr=2.2, score=2.0, bos_quality="WEAK"),
                "research_predicate_fail",
            ),
            (
                "live research EXPANSION volume still rejects",
                _candidate("volume-fail", rr=2.2, score=2.0, volume_confirmation="EXPANSION"),
                "research_predicate_fail",
            ),
        ):
            sd._live_smc_research_terminal_failures.clear()
            logs = []
            open_calls = []
            _run_dispatch(sd, [candidate], logs, open_calls)
            results.append(_assert(
                label,
                len(logs) == 1
                and logs[0].get("decision") == "PREFILTER_REJECT"
                and logs[0].get("reason") == expected_reason
                and len(open_calls) == 0,
                f"logs={logs} calls={len(open_calls)}",
            ))

        sd._live_smc_research_terminal_failures.clear()
        logs = []
        open_calls = []
        _run_dispatch(
            sd,
            [_candidate("max-research", rr=2.2, score=2.0)],
            logs,
            open_calls,
            gate_result=(False, "live_research_open=1 >= max_live_research_trades=1"),
        )
        results.append(_assert(
            "max_live_research_trades gate still rejects after score bypass",
            len(logs) == 1
            and logs[0].get("decision") == "PREFILTER_REJECT"
            and logs[0].get("reason") == "max_live_research_trades"
            and len(open_calls) == 0,
            f"logs={logs} calls={len(open_calls)}",
        ))

        live_research = _run_execution_until_cooldown(execution, _trade(score=2.0), mode="live")
        results.append(_assert(
            "execution.py live research score=2 uses 0.6x risk multiplier",
            live_research.get("risk_percent") == 0.006
            and live_research.get("score_filter_bypassed_for_research") is True
            and live_research.get("_open_failure", {}).get("fail_stage") == "entry_cooldown",
            str(live_research.get("_open_failure")),
        ))
        results.append(_assert(
            "execution.py live research score=2 no longer uses old 0.5x risk",
            live_research.get("risk_percent") != 0.005,
            f"risk_percent={live_research.get('risk_percent')}",
        ))

        validate_trade, validate_calls = _run_execution_until_validate(
            execution,
            _trade(score=2.0, entry_type="CONFIRM_SMC_RESEARCH", strategy_family="confirm_smc_research"),
            mode="live",
        )
        results.append(_assert(
            "validate_and_prepare receives final live research risk_percent=0.006 exactly once",
            len(validate_calls) == 1
            and validate_calls[0].get("risk_percent") == 0.006
            and validate_trade.get("risk_percent") == 0.006
            and validate_trade.get("_open_failure", {}).get("fail_stage") == "validate_and_prepare",
            f"calls={validate_calls} failure={validate_trade.get('_open_failure')}",
        ))

        high_score_live = _run_execution_until_cooldown(execution, _trade(score=8.0), mode="live")
        results.append(_assert(
            "high-score live research remains base risk",
            high_score_live.get("risk_percent") == 0.01
            and high_score_live.get("score_filter_bypassed_for_research") is not True,
            f"risk_percent={high_score_live.get('risk_percent')}",
        ))

        normal_confirm = _run_execution_until_cooldown(
            execution,
            _trade(score=7.5, entry_type="CONFIRM", strategy_family=None),
            mode="live",
            disable_canary=True,
        )
        results.append(_assert(
            "normal CONFIRM live 7<=score<8 remains half base risk",
            normal_confirm.get("risk_percent") == 0.005
            and normal_confirm.get("score_filter_bypassed_for_research") is not True,
            f"risk_percent={normal_confirm.get('risk_percent')}",
        ))

        low_score_normal_confirm = _run_execution_until_cooldown(
            execution,
            _trade(score=2.0, entry_type="CONFIRM", strategy_family=None),
            mode="live",
            disable_canary=True,
        )
        results.append(_assert(
            "normal CONFIRM live score<7 remains blocked before cooldown",
            "risk_percent" not in low_score_normal_confirm
            and low_score_normal_confirm.get("score_filter_bypassed_for_research") is not True,
            str(low_score_normal_confirm),
        ))

        paper_research = _run_execution_until_cooldown(execution, _trade(score=2.0), mode="paper")
        results.append(_assert(
            "paper research low-score behavior remains half risk",
            paper_research.get("risk_percent") == paper_research.get("base_risk_percent", execution.RISK_PER_TRADE) * 0.5
            and paper_research.get("score_filter_bypassed_for_research") is not True,
            f"risk_percent={paper_research.get('risk_percent')}",
        ))

        execution.config["live_smc_research_enabled"] = False
        disabled_live_research = _run_execution_until_cooldown(execution, _trade(score=2.0), mode="live")
        results.append(_assert(
            "live research score bypass fails closed when disabled",
            "risk_percent" not in disabled_live_research
            and disabled_live_research.get("score_filter_bypassed_for_research") is not True,
            str(disabled_live_research),
        ))

        total = len(results)
        passed = sum(1 for item in results if item)
        print("=" * 80)
        print(f"SUMMARY: {passed}/{total} PASS")
        return 0 if passed == total else 1

    finally:
        sd.config.clear()
        sd.config.update(original_sd_config)
        execution.config.clear()
        execution.config.update(original_execution_config)
        execution.ENTRY_COOLDOWN = original_entry_cooldown
        execution.check_correlation = original_check_correlation
        sd._live_smc_research_terminal_failures.clear()
        sd._live_smc_research_dedup_keys.clear()


if __name__ == "__main__":
    raise SystemExit(main())
