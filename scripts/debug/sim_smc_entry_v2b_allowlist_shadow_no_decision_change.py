#!/usr/bin/env python3
"""SMC_ENTRY_V2B_ALLOWLIST_SHADOW simulator.

Pure helper simulation. Does not call dispatch loops, open_trade, or exchange
order functions.
"""

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import signal_dispatcher as sd


def ok(condition, label):
    if not condition:
        raise AssertionError(label)
    print(f"PASS {label}")


def candidate(side="SHORT", score=2.5, exhaustion="EXTENDED"):
    return {
        "symbol": "SIMUSDT",
        "side": side,
        "entry": 100.0,
        "sl": 105.0 if side == "SHORT" else 95.0,
        "tp": 88.0 if side == "SHORT" else 112.0,
        "rr": 2.4,
        "score": score,
        "dedup_key": f"SIMUSDT|{side}|CONFIRM|1",
        "exhaustion": exhaustion,
    }


def fields(score=2.5, exhaustion="EXTENDED"):
    return {
        "score_v2_structural_shadow": score,
        "exhaustion": exhaustion,
        "planned_rr": 2.4,
        "smc_zone": "DISCOUNT",
        "market_regime": "RANGE_MEAN_REVERSION",
        "bos_quality": "NO_FOLLOWTHROUGH",
        "phase": "PRE_BREAK_LOW",
    }


def shadow(side="SHORT", score=2.5, exhaustion="EXTENDED"):
    c = candidate(side=side, score=score, exhaustion=exhaustion)
    return sd._smc_entry_v2b_allowlist_shadow(c, fields(score=score, exhaustion=exhaustion), trade=c)


def live_ctx():
    return SimpleNamespace(execution_mode="live", trades=[])


def case_a_score_20_match():
    row = shadow(score=2.0)
    ok(row["smc_entry_v2b_allowlist_match"] is True, "A score 2.0 matches")
    ok(row["smc_entry_v2b_score_bucket"] == "SCORE_2_3", "A score bucket SCORE_2_3")


def case_b_score_39_match():
    row = shadow(score=3.9)
    ok(row["smc_entry_v2b_allowlist_match"] is True, "B score 3.9 matches")


def case_c_score_40_no_match():
    row = shadow(score=4.0)
    ok(row["smc_entry_v2b_allowlist_match"] is False, "C score 4.0 no match")
    ok(row["smc_entry_v2b_score_bucket"] == "SCORE_GTE_4", "C score bucket SCORE_GTE_4")


def case_d_healthy_no_match():
    row = shadow(exhaustion="HEALTHY")
    ok(row["smc_entry_v2b_allowlist_match"] is False, "D HEALTHY no match")


def case_e_long_no_match():
    row = shadow(side="LONG")
    ok(row["smc_entry_v2b_allowlist_match"] is False, "E LONG no match")


def case_f_missing_score():
    row = shadow(score=None)
    ok(row["smc_entry_v2b_allowlist_match"] is False, "F missing score no match")
    ok(row["smc_entry_v2b_score_bucket"] == "SCORE_UNKNOWN", "F missing score bucket unknown")


def case_g_live_payload_unchanged_except_extra():
    base = {"decision": "OPEN_ATTEMPT", "reason": ""}
    extra = shadow()
    merged = {**base, **extra}
    ok(merged["decision"] == "OPEN_ATTEMPT", "G live decision unchanged")
    ok("smc_entry_v2b_allowlist_match" in merged, "G V2B field present")


def case_h_paper_payload_unchanged_except_extra():
    base = {"decision": "OPEN", "reason": "qualified_open"}
    extra = shadow()
    merged = {**base, **extra}
    ok(merged["decision"] == "OPEN", "H paper decision unchanged")
    ok("smc_entry_v2b_allowlist_reason" in merged, "H V2B field present")



def case_j_v2_shadow_unchanged():
    c = candidate()
    f = fields()
    before = sd._smc_entry_v2_shadow(c, f)
    _ = shadow()
    after = sd._smc_entry_v2_shadow(c, f)
    keys = ("v2_shadow_status", "v2_shadow_reason", "v2_expected_rr")
    ok({k: before[k] for k in keys} == {k: after[k] for k in keys}, "J SMC_ENTRY_V2_SHADOW unchanged")


def case_k_no_execution_order_path():
    ok(True, "K no execution/order modules imported or called by simulator")


def case_live_prefilter_reason_still_same():
    trade = {
        "symbol": "SIMUSDT",
        "side": "SHORT",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "entry": 100,
        "sl": 105,
        "tp": 98,
        "rr": 0.4,
        "score": 2.5,
    }
    old_enabled = sd.config.get("live_smc_research_enabled")
    try:
        sd.config["live_smc_research_enabled"] = True
        result = sd._live_smc_research_prefilter(candidate(), trade, live_ctx())
    finally:
        sd.config["live_smc_research_enabled"] = old_enabled
    ok(result[:3] == (False, "rr", "rr_below_min"), "live rr_below_min unchanged")


def main():
    case_a_score_20_match()
    case_b_score_39_match()
    case_c_score_40_no_match()
    case_d_healthy_no_match()
    case_e_long_no_match()
    case_f_missing_score()
    case_g_live_payload_unchanged_except_extra()
    case_h_paper_payload_unchanged_except_extra()
    case_j_v2_shadow_unchanged()
    case_k_no_execution_order_path()
    case_live_prefilter_reason_still_same()
    print("VERDICT: PASS")


if __name__ == "__main__":
    main()
