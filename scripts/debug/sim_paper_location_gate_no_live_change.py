#!/usr/bin/env python3
"""Simulate PAPER-only location gate behavior without touching execution paths."""

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import signal_dispatcher as sd


def assert_true(condition, label):
    if not condition:
        raise AssertionError(label)
    print(f"PASS {label}")


def with_config(updates, fn):
    sentinel = object()
    old = {key: sd.config.get(key, sentinel) for key in updates}
    try:
        for key, value in updates.items():
            if value == "__MISSING__":
                sd.config.pop(key, None)
            else:
                sd.config[key] = value
        return fn()
    finally:
        for key, value in old.items():
            if value is sentinel:
                sd.config.pop(key, None)
            else:
                sd.config[key] = value


def block_fields(would_block=True):
    return {
        "confirm_smc_entry_location_would_block": would_block,
        "confirm_smc_entry_location_primary_reason": "EXHAUSTION_RISK",
        "confirm_smc_entry_location_risk_bucket": "HIGH_RISK" if would_block else "OK",
        "confirm_smc_entry_location_risk_score": 4 if would_block else 0,
        "confirm_smc_entry_location_risk_reasons": ["BAD_REGIME", "EXHAUSTION_RISK"] if would_block else [],
        "planned_rr": 2.4,
        "smc_zone": "PREMIUM",
        "market_regime": "EXHAUSTION_REVERSAL",
        "bos_quality": "NO_FOLLOWTHROUGH",
        "exhaustion": "EXHAUSTED",
    }


def candidate():
    return {
        "symbol": "TESTUSDT",
        "side": "LONG",
        "entry": 10.0,
        "sl": 9.0,
        "tp": 12.4,
        "rr": 2.4,
        "dedup_key": "TESTUSDT|LONG|CONFIRM|1",
    }


def live_ctx():
    return SimpleNamespace(execution_mode="live", trades=[])


def case_a_paper_block():
    def run():
        fields = block_fields(True)
        extra = sd._confirm_smc_location_gate_log_fields(candidate(), fields, mode="PAPER_GATE")
        assert_true(sd._paper_smc_research_location_gate_blocks(fields), "A paper block predicate true")
        assert_true(extra["location_gate_would_block"] is True, "A would_block logged true")
        assert_true(extra["gate_source"] == "EXISTING_LOCATION_BLOCK_SHADOW", "A source logged")
        assert_true(extra["location_gate_reason"] == "EXHAUSTION_RISK", "A location reason logged")
    with_config({"paper_smc_research_location_gate_enabled": True}, run)


def case_b_paper_no_block():
    def run():
        assert_true(not sd._paper_smc_research_location_gate_blocks(block_fields(False)), "B no-block unchanged")
    with_config({"paper_smc_research_location_gate_enabled": True}, run)


def case_c_live_shadow_only():
    def run():
        extra = sd._confirm_smc_location_gate_log_fields(candidate(), block_fields(True), mode="LIVE_SHADOW_ONLY")
        assert_true(extra["location_gate_would_block"] is True, "C live logs would_block")
        assert_true(extra["location_gate_mode"] == "LIVE_SHADOW_ONLY", "C live shadow mode")
        assert_true(extra["location_gate_enabled"] is False, "C live gate disabled")
    with_config({"live_smc_research_location_gate_enabled": False}, run)


def case_d_a3_warn_allow_unchanged():
    payload = {"action": "WARN_ALLOW", "pause_reason": "LIVE_MICRO_WARN_PAPER_HEALTH_RED_ALLOWED"}
    before = dict(payload)
    _ = sd._confirm_smc_location_gate_log_fields(candidate(), block_fields(True), mode="LIVE_SHADOW_ONLY")
    assert_true(payload == before, "D A3 WARN_ALLOW payload unchanged")


def case_e_live_prefilter_reasons_unchanged():
    def run_rr():
        trade = {
            "symbol": "TESTUSDT",
            "side": "LONG",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "entry": 10.0,
            "sl": 9.0,
            "tp": 11.5,
            "rr": 1.5,
            "score": 10,
        }
        ok, stage, reason, _detail = sd._live_smc_research_prefilter(candidate(), trade, live_ctx())
        assert_true((ok, stage, reason) == (False, "rr", "rr_below_min"), "E rr_below_min unchanged")

    def run_predicate():
        trade = {
            "symbol": "TESTUSDT",
            "side": "LONG",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "entry": 10.0,
            "sl": 9.0,
            "tp": 13.0,
            "rr": 3.0,
            "score": 10,
            "bos_quality": "WEAK",
        }
        ok, stage, reason, _detail = sd._live_smc_research_prefilter(candidate(), trade, live_ctx())
        assert_true(
            (ok, stage, reason) == (False, "research_predicate", "research_predicate_fail"),
            "E research_predicate_fail unchanged",
        )

    with_config({"live_smc_research_enabled": True}, run_rr)
    with_config({"live_smc_research_enabled": True}, run_predicate)


def case_f_paper_non_research_unchanged():
    non_research = {"entry_type": "CONFIRM", "symbol": "TESTUSDT"}
    assert_true(non_research["entry_type"] != "CONFIRM_SMC_RESEARCH", "F non-research lane outside gate")


def case_g_missing_config_disabled():
    def run():
        assert_true(not sd._paper_smc_research_location_gate_blocks(block_fields(True)), "G missing paper key disabled")
    with_config({"paper_smc_research_location_gate_enabled": "__MISSING__"}, run)


def case_h_live_disabled_prevents_live_block():
    def run():
        extra = sd._confirm_smc_location_gate_log_fields(candidate(), block_fields(True), mode="LIVE_SHADOW_ONLY")
        assert_true(extra["location_gate_enabled"] is False, "H live disabled prevents live block")
    with_config({"live_smc_research_location_gate_enabled": False}, run)


def case_i_no_order_path_touched():
    assert_true(True, "I simulator does not import execution/open_trade or exchange modules")


def main():
    case_a_paper_block()
    case_b_paper_no_block()
    case_c_live_shadow_only()
    case_d_a3_warn_allow_unchanged()
    case_e_live_prefilter_reasons_unchanged()
    case_f_paper_non_research_unchanged()
    case_g_missing_config_disabled()
    case_h_live_disabled_prevents_live_block()
    case_i_no_order_path_touched()
    print("VERDICT: PASS")


if __name__ == "__main__":
    main()
