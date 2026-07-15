#!/usr/bin/env python3
"""Deterministic simulator for BTC M5/M15 decomposition shadow V2.

No exchange calls and no real log writes. The simulator monkeypatches the V2
writer and feeds fixed BTC context directly into the pure evaluator/wrapper.
"""

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import signal_dispatcher as sd

NOW_TS = 1_700_000_000.0
CAPTURED = []


def _capture(row):
    CAPTURED.append(row)


def _check(name, cond, detail=""):
    assert cond, f"FAIL {name}: {detail}"
    print(f"PASS {name}")


def _ctx(m5, m15, h1="BEARISH", age=60, m5_change=0.4, m15_change=-0.2, slope=-1.5):
    return {
        "btc_5m_dir": m5,
        "btc_15m_dir": m15,
        "btc_1h_dir": h1,
        "btc_5m_change_pct": m5_change,
        "btc_15m_change_pct": m15_change,
        "btc_1h_change_pct": -0.9,
        "btc_slope_15m": slope,
        "btc_context_source_ts": NOW_TS - age,
        "btc_context_age_sec": age,
        "btc_context_quality": "OK",
        "btc_context_missing_fields": [],
        "btc_alignment_independent": "AUX_ONLY",
        "btc_bias_independent": "AUX_BIAS_ONLY",
    }


def _source(side="LONG", entry_type="CONFIRM_SMC_RESEARCH"):
    return {
        "symbol": "ETHUSDT",
        "side": side,
        "entry_type": entry_type,
        "dedup_key": f"ETHUSDT|{side}|{entry_type}|{NOW_TS}",
        "source_timestamp": NOW_TS - 10,
        "entry": 100.0,
        "sl": 98.0,
        "planned_rr": 2.0,
        "score": 7.5,
        "score_v2": 7.1,
    }


def _run(side, m5, m15, **kwargs):
    src = _source(side, kwargs.pop("entry_type", "CONFIRM_SMC_RESEARCH"))
    before = dict(src)
    row = sd._btc_m5_m15_decomposition_shadow(
        src,
        execution_mode="paper",
        action=kwargs.pop("action", "OPEN"),
        reject_reason=kwargs.pop("reject_reason", ""),
        side=side,
        btc_ctx=kwargs.pop("btc_ctx", _ctx(m5, m15, **kwargs)),
        v3_summary={"smc_pa_v3_total_score": 3, "smc_pa_v3_score_band": "SIM"},
        v2b_fields={"v2b_label": "SIM_V2B", "v2b_match": True, "v2b_reason": "sim"},
        now_ts=NOW_TS,
    )
    _check(f"source unmutated {side}/{m5}/{m15}", src == before, (src, before))
    assert row is not None
    return row


def main():
    sd._btc_m5_m15_decomposition_write = _capture

    cases = [
        ("LONG both aligned", "LONG", "LONG", "LONG", "M5_M15_BOTH_ALIGNED"),
        ("LONG m5 aligned m15 opposed", "LONG", "LONG", "SHORT", "M5_ALIGNED_M15_OPPOSED"),
        ("LONG m5 opposed m15 aligned", "LONG", "SHORT", "LONG", "M5_OPPOSED_M15_ALIGNED"),
        ("LONG both opposed", "LONG", "SHORT", "SHORT", "M5_M15_BOTH_OPPOSED"),
        ("SHORT both aligned", "SHORT", "SHORT", "SHORT", "M5_M15_BOTH_ALIGNED"),
        ("SHORT m5 aligned m15 opposed", "SHORT", "SHORT", "LONG", "M5_ALIGNED_M15_OPPOSED"),
        ("SHORT m5 opposed m15 aligned", "SHORT", "LONG", "SHORT", "M5_OPPOSED_M15_ALIGNED"),
        ("SHORT both opposed", "SHORT", "LONG", "LONG", "M5_M15_BOTH_OPPOSED"),
        ("neutral m5", "LONG", "NEUTRAL", "LONG", "M5_NEUTRAL_M15_ALIGNED"),
        ("neutral m15", "LONG", "LONG", "NEUTRAL", "M5_ALIGNED_M15_NEUTRAL"),
        ("direction aliases", "LONG", "UP", "BULLISH", "M5_M15_BOTH_ALIGNED"),
    ]
    for name, side, m5, m15, expected in cases:
        row = _run(side, m5, m15)
        _check(name, row["btc_m5_m15_label"] == expected, row)

    missing = _run("LONG", "UNKNOWN", "LONG")
    _check("missing context", missing["btc_m5_m15_label"] == "CONTEXT_MISSING", missing)

    stale = _run("LONG", "LONG", "LONG", age=999999)
    _check("stale context", stale["btc_m5_m15_label"] == "CONTEXT_STALE", stale)

    malformed = _run(
        "LONG",
        "LONG",
        "LONG",
        m5_change="not-a-number",
        m15_change="bad",
        slope="nan-ish",
    )
    _check(
        "malformed numeric fields become null strength values",
        malformed["btc_m5_abs_change_pct"] is None
        and malformed["btc_m15_abs_change_pct"] is None
        and malformed["btc_m15_slope_abs"] is None,
        malformed,
    )

    h1_disagree = _run("LONG", "LONG", "LONG", h1="BEARISH")
    _check(
        "BTC 1H disagreement does not change primary label",
        h1_disagree["btc_m5_m15_label"] == "M5_M15_BOTH_ALIGNED"
        and h1_disagree["btc_1h_relation"] == "OPPOSED",
        h1_disagree,
    )

    action = {"candidate_action": "OPEN"}
    ret = sd._btc_m5_m15_decomposition_shadow(
        _source("LONG"),
        execution_mode="paper",
        action=action["candidate_action"],
        side="LONG",
        btc_ctx=_ctx("LONG", "LONG"),
        now_ts=NOW_TS,
    )
    _check("evaluator return ignored by caller simulation", action == {"candidate_action": "OPEN"} and ret, ret)

    def _failing_writer(row):
        raise RuntimeError("simulated writer failure")

    sd._btc_m5_m15_decomposition_write = _failing_writer
    src = _source("LONG")
    before = dict(src)
    failed = sd._btc_m5_m15_decomposition_shadow(
        src,
        execution_mode="paper",
        action="REJECT",
        reject_reason="simulated",
        side="LONG",
        btc_ctx=_ctx("LONG", "SHORT"),
        now_ts=NOW_TS,
    )
    _check("writer failure does not change candidate/action", src == before and failed is not None, failed)

    forbidden = {"realized_outcome", "mfe", "mae", "first_hit", "close_status", "future_candle"}
    _check(
        "no future/outcome fields appear",
        all(not (forbidden & set(row.keys())) for row in CAPTURED),
        CAPTURED[-1] if CAPTURED else {},
    )

    print("PASS btc_m5_m15_decomposition_shadow_v2 simulator")


if __name__ == "__main__":
    main()
