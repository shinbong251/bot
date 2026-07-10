#!/usr/bin/env python3
"""SMC_ENTRY_V2B v0.2 shadow-only simulator.

Pure helper simulation. Does not call dispatch loops, open_trade, exchange
order functions, restart hooks, or config writers.
"""

import copy
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import signal_dispatcher as sd


def ok(condition, label, detail=None):
    if not condition:
        raise AssertionError(f"{label}: {detail}")
    print(f"PASS {label}")


def candidate(side="SHORT", score=9.0):
    return {
        "symbol": "SIMUSDT",
        "side": side,
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "entry": 100.0,
        "sl": 105.0 if side == "SHORT" else 95.0,
        "tp": 88.0 if side == "SHORT" else 112.0,
        "rr": 2.4,
        "score": score,
        "dedup_key": f"SIMUSDT|{side}|CONFIRM_SMC_RESEARCH|1",
        "exhaustion": "EXTENDED",
    }


def fields(score=2.5, phase="PRE_BREAK_LOW", regime="RANGE_MEAN_REVERSION", risk_class=""):
    return {
        "score_v2_structural_shadow": score,
        "exhaustion": "EXTENDED",
        "planned_rr": 2.4,
        "smc_zone": "DISCOUNT",
        "market_regime": regime,
        "bos_quality": "NO_FOLLOWTHROUGH",
        "phase": phase,
        "research_entry_timing_risk_class": risk_class,
    }


def live_ctx():
    return SimpleNamespace(execution_mode="live", trades=[])


def shadow(c=None, f=None):
    c = candidate() if c is None else c
    f = fields() if f is None else f
    return sd._smc_entry_v2b_allowlist_shadow(c, f, trade=c, mode="SIM_SHADOW_ONLY")


def case_a_v01_identity_preserved():
    row = shadow()
    ok(row["smc_entry_v2b_allowlist_label"] == "SHORT_EXTENDED_SCORE_2_3", "A v0.1 label unchanged")
    ok(row["smc_entry_v2b_allowlist_version"] == "v0.1_shadow", "A v0.1 version unchanged")


def case_b_v02_predicate_matches_range_equivalent():
    row = shadow(f=fields(score=2.5, phase="PRE_BREAK_LOW", regime="RANGE_MEAN_REVERSION"))
    ok(row["smc_entry_v2b_v02_label"] == "PRE_BREAK_LOW_CHOP_RANGE_SCORE_2_3", "B v0.2 label present")
    ok(row["v0.2_match"] is True, "B v0.2 matches RANGE_MEAN_REVERSION")
    ok(row["v2b_recompute_match"] is True, "B recompute exact")


def case_c_v02_predicate_matches_risk_class():
    row = shadow(f=fields(score=3.9, phase="PRE_BREAK_LOW", regime="TREND", risk_class="CHOP_OR_RANGE_ENTRY"))
    ok(row["v0.2_match"] is True, "C v0.2 matches CHOP_OR_RANGE_ENTRY risk class")
    ok(row["v2b_regime_source"] == "fields.research_entry_timing_risk_class", "C regime provenance")


def case_d_v02_nonmatches():
    ok(shadow(f=fields(score=4.0))["v0.2_match"] is False, "D score outside bucket does not match")
    ok(shadow(f=fields(phase="BREAKOUT_STRONG"))["v0.2_match"] is False, "D phase outside predicate does not match")
    ok(shadow(f=fields(regime="TREND"))["v0.2_match"] is False, "D regime outside predicate does not match")


def case_e_canonical_score_source_no_fallback():
    c = candidate(score=2.5)
    f = fields(score=None)
    row = shadow(c=c, f=f)
    ok(row["v2b_score_source"] == "fields.score_v2_structural_shadow", "E canonical score source logged")
    ok(row["v2b_score_value"] is None, "E candidate score fallback not used")
    ok(row["v0.1_match"] is False and row["v0.2_match"] is False, "E missing canonical score does not match")


def case_f_shadow_inputs_unmutated():
    c = candidate()
    f = fields(risk_class="CHOP_OR_RANGE_ENTRY")
    before_c = copy.deepcopy(c)
    before_f = copy.deepcopy(f)
    _ = shadow(c=c, f=f)
    ok(c == before_c and f == before_f, "F candidate and fields unmutated")


def case_g_paper_open_decision_unchanged():
    base = {"decision": "OPEN", "reason": "qualified_open", "rr_threshold": 2.0}
    merged = {**base, **shadow()}
    ok(merged["decision"] == "OPEN", "G paper open decision unchanged")
    ok(merged["reason"] == "qualified_open", "G paper reason unchanged")
    ok(merged["rr_threshold"] == 2.0, "G RR threshold value unchanged")


def case_h_live_testnet_decisions_unchanged():
    base_live = {"decision": "OPEN_ATTEMPT", "execution_mode": "live"}
    base_testnet = {"decision": "OPEN_ATTEMPT", "execution_mode": "testnet"}
    ok({**base_live, **shadow()}["decision"] == "OPEN_ATTEMPT", "H live decision payload unchanged")
    ok({**base_testnet, **shadow()}["decision"] == "OPEN_ATTEMPT", "H testnet decision payload unchanged")


def case_i_live_rr_prefilter_unchanged():
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
        before = sd._live_smc_research_prefilter(candidate(), trade, live_ctx())
        _ = shadow()
        after = sd._live_smc_research_prefilter(candidate(), trade, live_ctx())
    finally:
        sd.config["live_smc_research_enabled"] = old_enabled
    ok(before[:3] == (False, "rr", "rr_below_min"), "I live RR reject baseline")
    ok(after[:3] == before[:3], "I live RR reject unchanged")


def case_j_research_cap_config_unchanged():
    keys = (
        "paper_smc_research_qualified_max_open",
        "paper_smc_research_qualified_max_new_trades",
        "paper_smc_research_cap_enabled",
    )
    before = {key: copy.deepcopy(sd.config.get(key)) for key in keys}
    _ = shadow()
    after = {key: copy.deepcopy(sd.config.get(key)) for key in keys}
    ok(before == after, "J research cap config unchanged")


def main():
    case_a_v01_identity_preserved()
    case_b_v02_predicate_matches_range_equivalent()
    case_c_v02_predicate_matches_risk_class()
    case_d_v02_nonmatches()
    case_e_canonical_score_source_no_fallback()
    case_f_shadow_inputs_unmutated()
    case_g_paper_open_decision_unchanged()
    case_h_live_testnet_decisions_unchanged()
    case_i_live_rr_prefilter_unchanged()
    case_j_research_cap_config_unchanged()
    print("NO_DECISION_CHANGE: PASS")
    print("VERDICT: PASS")


if __name__ == "__main__":
    main()
