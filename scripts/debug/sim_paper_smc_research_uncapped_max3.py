"""Simulator for PAPER CONFIRM_SMC_RESEARCH uncapped collection with max-open=3."""

import copy
import os
import sys
import time
import types
from dataclasses import dataclass, field

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


@dataclass
class DummyCtx:
    execution_mode: str = "paper"
    trades: list = field(default_factory=list)
    entry_cooldown: dict = field(default_factory=dict)


@dataclass
class LiveCtx:
    execution_mode: str = "live"
    trades: list = field(default_factory=list)


def _candidate(
    key,
    rr=2.25,
    symbol=None,
    side="LONG",
    bos_quality="STRONG",
    volume_confirmation="NORMAL",
    entry_type="CONFIRM",
):
    symbol = symbol or f"{key.upper().replace('-', '')}USDT"
    entry = 100.0
    sl = 90.0 if side == "LONG" else 110.0
    tp = 125.0 if side == "LONG" else 75.0
    now_ts = time.time()
    return {
        "symbol": symbol,
        "side": side,
        "dedup_key": key,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "planned_rr": rr,
        "reason": "ACCEPTED_CONFIRM",
        "source_timestamp": now_ts,
        "signal_created_ts": now_ts,
        "entry_type": entry_type,
        "geometry_status": "VALID_GEOMETRY",
        "outcome_trackable": True,
        "structural_context": {
            "score_v2_current": 7.0,
            "score_v2_structural_shadow": 7.0,
            "structural_decision_shadow": "QUALIFIED",
            "bos_quality": bos_quality,
            "volume_confirmation": volume_confirmation,
        },
    }


def _open_research_trade(idx):
    return {
        "symbol": f"OPEN{idx}USDT",
        "side": "LONG",
        "status": "OPEN",
        "owner": "bot",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "strategy_family": "confirm_smc_research",
        "research_dedup_key": f"open-{idx}",
        "paper_smc_research_qualified": True,
    }


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<76} {status} {detail}")
    return bool(condition)


def _install_fake_modules(candidates, open_calls):
    entry_mod = types.ModuleType("entry")
    entry_mod.get_confirm_structural_outcome_candidates_snapshot = lambda: copy.deepcopy(candidates)
    sys.modules["entry"] = entry_mod

    execution_mod = types.ModuleType("execution")

    def fake_open_trade(trade, ctx):
        opened = copy.deepcopy(trade)
        opened["open_trade_ts"] = opened.get("entry_time")
        open_calls.append(opened)
        ctx.trades.append(opened)
        return True

    execution_mod.open_trade = fake_open_trade
    execution_mod.update_signal_state = lambda *args, **kwargs: None
    sys.modules["execution"] = execution_mod


def _run_case(sd, candidates, *, cap_enabled, opened_total, existing_open=0):
    logs = []
    open_calls = []
    ctx = DummyCtx(trades=[_open_research_trade(i) for i in range(existing_open)])
    _install_fake_modules(candidates, open_calls)

    def fake_log(candidate, qualified, decision, reason, **kwargs):
        row = {
            "dedup_key": candidate.get("dedup_key"),
            "qualified": qualified,
            "decision": decision,
            "reason": reason,
        }
        row.update(kwargs)
        logs.append(row)

    original_log = sd._paper_smc_research_qualified_decision_log
    original_opened_total = sd._paper_smc_research_qualified_opened_total
    original_attach = sd._paper_smc_main_attach_router_context
    original_shadow = sd._paper_smc_research_entry_fallback_shadow_safe
    original_waterfall = sd._paper_smc_research_emit_qualified_latency_waterfall
    original_context = sd._paper_smc_research_entry_context_snapshot
    original_acceptance = sd._paper_smc_research_entry_acceptance_shadow_snapshot

    try:
        sd._paper_smc_research_qualified_decision_log = fake_log
        sd._paper_smc_research_qualified_opened_total = lambda: opened_total
        sd._paper_smc_main_attach_router_context = lambda candidate, now_ts=None: candidate
        sd._paper_smc_research_entry_fallback_shadow_safe = lambda *args, **kwargs: {}
        sd._paper_smc_research_emit_qualified_latency_waterfall = lambda *args, **kwargs: None
        sd._paper_smc_research_entry_context_snapshot = lambda *args, **kwargs: None
        sd._paper_smc_research_entry_acceptance_shadow_snapshot = lambda *args, **kwargs: None
        sd._paper_smc_research_dedup_keys.clear()
        sd._paper_smc_research_qualified_dedup_keys.clear()
        sd._paper_smc_research_qualified_first_seen_ts.clear()
        sd.config["paper_smc_research_qualified_enabled"] = True
        sd.config["paper_smc_research_live_enabled"] = False
        sd.config["paper_smc_research_cap_enabled"] = cap_enabled
        sd.config["paper_smc_research_qualified_max_new_trades"] = 1
        sd.config["paper_smc_research_qualified_max_open"] = 3
        sd.config["paper_smc_research_qualified_min_rr"] = 2.0
        sd._dispatch_paper_smc_research_qualified_lane(ctx)
    finally:
        sd._paper_smc_research_qualified_decision_log = original_log
        sd._paper_smc_research_qualified_opened_total = original_opened_total
        sd._paper_smc_main_attach_router_context = original_attach
        sd._paper_smc_research_entry_fallback_shadow_safe = original_shadow
        sd._paper_smc_research_emit_qualified_latency_waterfall = original_waterfall
        sd._paper_smc_research_entry_context_snapshot = original_context
        sd._paper_smc_research_entry_acceptance_shadow_snapshot = original_acceptance

    return logs, open_calls, ctx


def main():
    import signal_dispatcher as sd

    original_config = dict(sd.config)
    results = []

    try:
        logs, open_calls, _ = _run_case(
            sd,
            [_candidate("uncapped-open-0")],
            cap_enabled=False,
            opened_total=1,
            existing_open=0,
        )
        results.append(_assert(
            "cap_enabled=false, qualified candidate, open_count=0 opens",
            len(open_calls) == 1
            and logs[-1]["decision"] == "OPEN"
            and logs[-1]["reason"] == "qualified_open"
            and logs[-1].get("cap_enabled") is False
            and logs[-1].get("cap_block_skipped") is True,
            f"logs={logs} calls={len(open_calls)}",
        ))

        logs, open_calls, _ = _run_case(
            sd,
            [_candidate("uncapped-open-2")],
            cap_enabled=False,
            opened_total=1,
            existing_open=2,
        )
        results.append(_assert(
            "cap_enabled=false, qualified candidate, open_count=2 opens",
            len(open_calls) == 1
            and logs[-1]["decision"] == "OPEN"
            and logs[-1].get("open_count_at_decision") == 2
            and logs[-1].get("cap_block_skipped") is True,
            f"logs={logs} calls={len(open_calls)}",
        ))

        logs, open_calls, _ = _run_case(
            sd,
            [_candidate("uncapped-block-3")],
            cap_enabled=False,
            opened_total=1,
            existing_open=3,
        )
        results.append(_assert(
            "cap_enabled=false, qualified candidate, open_count=3 blocks MAX_OPEN_REACHED",
            len(open_calls) == 0
            and len(logs) == 1
            and logs[0]["decision"] == "MAX_OPEN_REACHED"
            and logs[0]["reason"] == "max_open_reached"
            and logs[0].get("max_open") == 3
            and logs[0].get("cap_block_skipped") is True,
            f"logs={logs} calls={len(open_calls)}",
        ))

        logs, open_calls, _ = _run_case(
            sd,
            [_candidate("capped-block")],
            cap_enabled=True,
            opened_total=1,
            existing_open=0,
        )
        results.append(_assert(
            "cap_enabled=true, cap reached keeps CAP_REACHED behavior",
            len(open_calls) == 0
            and len(logs) == 1
            and logs[0]["decision"] == "CAP_REACHED"
            and logs[0]["reason"] == "cap_reached"
            and logs[0].get("cap_block_skipped") is False,
            f"logs={logs} calls={len(open_calls)}",
        ))

        for label, candidate, expected_reason in (
            ("rr < 2 still rejects", _candidate("rr-low", rr=1.5), "rr_below_2"),
            (
                "bos_quality=WEAK still rejects",
                _candidate("weak-bos", bos_quality="WEAK"),
                "bos_weak",
            ),
            (
                "volume_confirmation=EXPANSION still rejects",
                _candidate("volume-expansion", volume_confirmation="EXPANSION"),
                "volume_expansion",
            ),
        ):
            logs, open_calls, _ = _run_case(
                sd,
                [candidate],
                cap_enabled=False,
                opened_total=1,
                existing_open=0,
            )
            results.append(_assert(
                label,
                len(open_calls) == 0
                and len(logs) == 1
                and logs[0]["decision"] == "REJECT"
                and logs[0]["reason"] == expected_reason,
                f"logs={logs} calls={len(open_calls)}",
            ))

        live_trade = sd._paper_smc_research_trade(_candidate("live-unchanged"))
        sd.config["live_smc_research_enabled"] = True
        live_ok, live_stage, live_reason, _ = sd._live_smc_research_prefilter(
            _candidate("live-unchanged"),
            live_trade,
            LiveCtx(),
        )
        results.append(_assert(
            "live CONFIRM_SMC_RESEARCH prefilter is unchanged by paper cap flag",
            live_ok is True and live_stage == "ok" and live_reason == "ok",
            f"ok={live_ok} stage={live_stage} reason={live_reason}",
        ))

        normal_confirm = _candidate("normal-confirm", entry_type="CONFIRM")
        normal_trade = sd._paper_smc_research_trade(normal_confirm)
        normal_trade["entry_type"] = "CONFIRM"
        normal_ok, normal_stage, normal_reason, _ = sd._live_smc_research_prefilter(
            normal_confirm,
            normal_trade,
            LiveCtx(),
        )
        results.append(_assert(
            "normal CONFIRM remains outside live research prefilter",
            normal_ok is False
            and normal_stage == "entry_type"
            and normal_reason == "entry_type_not_confirm_smc_research",
            f"ok={normal_ok} stage={normal_stage} reason={normal_reason}",
        ))

        results.append(_assert(
            "paper gap-calibrated accounting keys are not written by cap dispatcher",
            all(
                "calibrated_r" not in call
                and "sl_gap_calibration" not in call
                and "gap_calibration" not in call
                for call in open_calls
            ),
            f"last_open_calls={open_calls}",
        ))
    finally:
        sd.config.clear()
        sd.config.update(original_config)

    print("\nRESULT:", "PASS" if all(results) else "FAIL")
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
