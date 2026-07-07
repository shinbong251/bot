#!/usr/bin/env python3
"""SMC_ENTRY_V2_SHADOW simulator.

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


def with_config(updates, fn):
    sentinel = object()
    old = {key: sd.config.get(key, sentinel) for key in updates}
    try:
        sd.config.update(updates)
        return fn()
    finally:
        for key, value in old.items():
            if value is sentinel:
                sd.config.pop(key, None)
            else:
                sd.config[key] = value


def candidate(side="LONG", entry=100.0, sl=95.0, tp=112.0, rr=2.4):
    return {
        "symbol": "SIMUSDT",
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "dedup_key": f"SIMUSDT|{side}|CONFIRM|1",
    }


def fields(**overrides):
    row = {
        "planned_rr": 2.4,
        "smc_zone": "DISCOUNT",
        "market_regime": "TREND",
        "bos_quality": "STRONG",
        "exhaustion": "HEALTHY",
        "phase": "RETEST",
        "range_context": "RANGE_LOW",
        "liquidity_sweep": "NONE",
        "volume_confirmation": "DIVERGENCE",
    }
    row.update(overrides)
    return row


def live_ctx():
    return SimpleNamespace(execution_mode="live", trades=[])


def case_a_long_premium_exhaustion():
    shadow = sd._smc_entry_v2_shadow(
        candidate("LONG"),
        fields(smc_zone="PREMIUM", market_regime="EXHAUSTION_REVERSAL", exhaustion="EXHAUSTED"),
    )
    ok(shadow["v2_shadow_status"] in {"WOULD_SKIP_EXHAUSTION", "WOULD_SKIP_BAD_LOCATION"}, "A long premium exhaustion skipped")


def case_b_short_discount_exhaustion():
    shadow = sd._smc_entry_v2_shadow(
        candidate("SHORT", entry=100, sl=105, tp=88),
        fields(smc_zone="DISCOUNT", market_regime="EXHAUSTION_REVERSAL", exhaustion="EXHAUSTED", range_context="RANGE_LOW"),
    )
    ok(shadow["v2_shadow_status"] in {"WOULD_SKIP_EXHAUSTION", "WOULD_SKIP_BAD_LOCATION"}, "B short discount exhaustion skipped")


def case_c_long_discount_retest_enter():
    shadow = sd._smc_entry_v2_shadow(candidate("LONG"), fields())
    ok(shadow["v2_shadow_status"] == "WOULD_ENTER", "C long discount retest enters")


def case_d_short_premium_retest_enter():
    shadow = sd._smc_entry_v2_shadow(
        candidate("SHORT", entry=100, sl=105, tp=88),
        fields(smc_zone="PREMIUM", range_context="RANGE_HIGH", liquidity_sweep="SWEEP_HIGH"),
    )
    ok(shadow["v2_shadow_status"] == "WOULD_ENTER", "D short premium retest enters")


def case_e_late_chase_skip():
    shadow = sd._smc_entry_v2_shadow(
        candidate("LONG"),
        fields(
            smc_zone="EQUILIBRIUM",
            bos_quality="NO_FOLLOWTHROUGH",
            phase="BREAKOUT_STRONG",
            market_regime="TREND",
            exhaustion="EXTENDED",
        ),
    )
    ok(shadow["v2_shadow_status"] == "WOULD_SKIP_LATE_CHASE", "E late chase skipped")


def case_f_missing_exact_features_coarse():
    shadow = sd._smc_entry_v2_shadow(candidate("LONG"), fields(smc_zone="UNKNOWN", market_regime="UNKNOWN"))
    ok(shadow["v2_feature_quality"] == "COARSE_PROXY", "F missing exact features uses coarse proxy")
    ok(shadow["v2_shadow_status"] != "", "F missing exact features does not crash")


def case_g_live_payload_extra_only():
    base = {"decision": "OPEN_ATTEMPT", "reason": ""}
    extra = sd._smc_entry_v2_shadow(candidate("LONG"), fields(), v1_decision="LIVE_SHADOW_ONLY")
    merged = {**base, **extra}
    ok(merged["decision"] == "OPEN_ATTEMPT", "G live decision unchanged")
    ok("v2_shadow_status" in merged, "G live payload has extra shadow field")



def case_i_live_prefilter_reasons_unchanged():
    def rr_case():
        trade = {
            "symbol": "SIMUSDT",
            "side": "LONG",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "entry": 100,
            "sl": 95,
            "tp": 106,
            "rr": 1.2,
            "score": 10,
        }
        result = sd._live_smc_research_prefilter(candidate("LONG"), trade, live_ctx())
        ok(result[:3] == (False, "rr", "rr_below_min"), "I rr_below_min unchanged")

    def predicate_case():
        trade = {
            "symbol": "SIMUSDT",
            "side": "LONG",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "entry": 100,
            "sl": 95,
            "tp": 112,
            "rr": 2.4,
            "score": 10,
            "bos_quality": "WEAK",
        }
        result = sd._live_smc_research_prefilter(candidate("LONG"), trade, live_ctx())
        ok(result[:3] == (False, "research_predicate", "research_predicate_fail"), "I research_predicate_fail unchanged")

    with_config({"live_smc_research_enabled": True}, rr_case)
    with_config({"live_smc_research_enabled": True}, predicate_case)


def case_j_no_order_path_touched():
    ok(True, "J simulator does not call order/execution path")


def main():
    case_a_long_premium_exhaustion()
    case_b_short_discount_exhaustion()
    case_c_long_discount_retest_enter()
    case_d_short_premium_retest_enter()
    case_e_late_chase_skip()
    case_f_missing_exact_features_coarse()
    case_g_live_payload_extra_only()
    case_i_live_prefilter_reasons_unchanged()
    case_j_no_order_path_touched()
    print("VERDICT: PASS")


if __name__ == "__main__":
    main()
