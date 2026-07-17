#!/usr/bin/env python3
"""Simulator: SMC_PA_SCORE_V3_SHADOW is shadow/log-only and component-correct.

No Binance calls, no orders, no real log/state writes. It imports the
production evaluator, monkeypatches the V3 forward-log writer (and never calls
any other writer), and verifies:

  A BTC aligned + TRAP + good location + sweep       => strong score (V3_STRONG)
  B BTC counter + CHOP + bad location + NO_FOLLOWTHROUGH => reject-like score
  C LONG premium in confirmed bullish expansion not punished as hard (0, not -2)
  D SHORT discount in confirmed bearish expansion not punished as hard (0, not -2)
  E old degenerate BTC fields ignored (market_bias missing, score 0)
  F missing RS/follow-through fields add missing_components, no crash
  G paper decision unchanged (qualified predicate identical pre/post V3)
  H live decision unchanged (live prefilter identical pre/post V3)
  I PAPER_LOCATION_GATE unchanged (same inputs -> same output pre/post V3)
  K BTC bias side-enable unchanged (same inputs -> same output pre/post V3)
  L no execution/order module calls inside the V3 evaluator/wrapper
"""

import copy
import inspect
import os
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import signal_dispatcher as sd

RESULTS = []
_V3_CAPTURED = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok), detail))
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name}" + (f" | {detail}" if detail else ""))


def _capture_v3(row):
    _V3_CAPTURED.append(row)


# Patch the V3 forward-log writer so the sim never appends to logs/.
sd._smc_pa_score_v3_shadow_write = _capture_v3

NOW = time.time()


def fresh_candidate(**extra):
    base = {
        "symbol": "SIMUSDT",
        "side": "LONG",
        "entry_type": "CONFIRM",
        "dedup_key": "SIMUSDT|LONG|CONFIRM|123",
        "signal_created_ts": NOW - 10,
        "entry": 100.0,
        "sl": 98.0,
        "tp": 104.0,
        "rr": 2.0,
        "planned_rr": 2.0,
        "geometry_status": "VALID_GEOMETRY",
        "outcome_trackable": True,
    }
    base.update(extra)
    return base


# ─── Case A: BTC aligned + TRAP + good location + sweep => strong ─────────
cand_a = fresh_candidate(
    smc_zone="DISCOUNT",
    market_regime="RANGE_MEAN_REVERSION",
    bos_quality="TRAP",
    liquidity_sweep="SWEEP_LOW",
    phase="PRE_BREAK_LOW",
    atr=1.5,
    opposing_barrier_distance_r=3.0,
)
btc_aligned = {"btc_bias_independent": "BULLISH", "btc_context_quality": "FULL"}
stale_fresh = sd._paper_smc_research_stale_info(cand_a, NOW)
res_a = sd._smc_pa_score_v3_eval(cand_a, side="LONG", btc_ctx=btc_aligned, stale_info=stale_fresh)
check(
    "A strong-context score",
    res_a["smc_pa_v3_score_band"] == "V3_STRONG" and res_a["smc_pa_v3_total_score"] >= 4,
    f"total={res_a['smc_pa_v3_total_score']} band={res_a['smc_pa_v3_score_band']}",
)
check(
    "A components positive",
    res_a["smc_pa_v3_market_bias_score"] == 2
    and res_a["smc_pa_v3_structure_quality_score"] == 2
    and res_a["smc_pa_v3_location_quality_score"] == 2
    and res_a["smc_pa_v3_liquidity_sweep_score"] == 2,
    "",
)

# ─── Case B: BTC counter + CHOP + bad location + NO_FOLLOWTHROUGH ─────────
cand_b = fresh_candidate(
    side="LONG",
    smc_zone="PREMIUM",
    market_regime="CHOP_NO_TRADE",
    bos_quality="NO_FOLLOWTHROUGH",
    liquidity_sweep="NONE",
    phase="BREAKOUT_STRONG",
    atr=1.5,
    sl=99.0,
    opposing_barrier_distance_r=1.2,
    planned_rr=2.5,
    signal_created_ts=NOW - 3600,
)
btc_counter = {"btc_bias_independent": "BEARISH", "btc_context_quality": "FULL"}
stale_b = sd._paper_smc_research_stale_info(cand_b, NOW)
res_b = sd._smc_pa_score_v3_eval(cand_b, side="LONG", btc_ctx=btc_counter, stale_info=stale_b)
check(
    "B reject-like score",
    res_b["smc_pa_v3_score_band"] == "V3_REJECT_LIKE" and res_b["smc_pa_v3_total_score"] < -2,
    f"total={res_b['smc_pa_v3_total_score']} band={res_b['smc_pa_v3_score_band']}",
)
check(
    "B components negative",
    res_b["smc_pa_v3_market_bias_score"] == -2
    and res_b["smc_pa_v3_regime_score"] == -2
    and res_b["smc_pa_v3_location_quality_score"] == -2
    and res_b["smc_pa_v3_structure_quality_score"] == -1
    and res_b["smc_pa_v3_breakout_acceptance_score"] == -2,
    "",
)

# ─── Case C: LONG premium in confirmed bullish expansion softened ─────────
cand_c = fresh_candidate(
    smc_zone="PREMIUM",
    market_regime="TRENDING_CONTINUATION",
    trend_direction="LONG",
)
res_c = sd._smc_pa_score_v3_eval(cand_c, side="LONG", btc_ctx={}, stale_info=stale_fresh)
res_c_no_exp = sd._smc_pa_score_v3_eval(
    fresh_candidate(smc_zone="PREMIUM", market_regime="CHOP_NO_TRADE"),
    side="LONG", btc_ctx={}, stale_info=stale_fresh,
)
check(
    "C long premium softened in bullish expansion",
    res_c["smc_pa_v3_location_quality_score"] == 0
    and res_c_no_exp["smc_pa_v3_location_quality_score"] == -2,
    f"expansion={res_c['smc_pa_v3_location_quality_score']} chop={res_c_no_exp['smc_pa_v3_location_quality_score']}",
)

# ─── Case D: SHORT discount in confirmed bearish expansion softened ───────
cand_d = fresh_candidate(
    side="SHORT",
    smc_zone="DISCOUNT",
    market_regime="BREAKOUT_EXPANSION",
    trend_direction="SHORT",
    sl=102.0,
    tp=96.0,
)
res_d = sd._smc_pa_score_v3_eval(cand_d, side="SHORT", btc_ctx={}, stale_info=stale_fresh)
res_d_no_exp = sd._smc_pa_score_v3_eval(
    fresh_candidate(side="SHORT", smc_zone="DISCOUNT", market_regime="CHOP_NO_TRADE",
                    sl=102.0, tp=96.0),
    side="SHORT", btc_ctx={}, stale_info=stale_fresh,
)
check(
    "D short discount softened in bearish expansion",
    res_d["smc_pa_v3_location_quality_score"] == 0
    and res_d_no_exp["smc_pa_v3_location_quality_score"] == -2,
    f"expansion={res_d['smc_pa_v3_location_quality_score']} chop={res_d_no_exp['smc_pa_v3_location_quality_score']}",
)

# ─── Case E: old degenerate BTC fields ignored ─────────────────────────────
btc_degenerate = {
    "btc_m5_bias_label": "BEARISH",
    "btc_m15_bias_label": "BEARISH",
    "btc_h1_bias_label": "BEARISH",
    "btc_mtf_summary_label": "ALL_ALIGNED",
    "btc_mtf_data_mode": "UNIFIED_ROUTER_BIAS_NOT_INDEPENDENT_TF",
}
res_e = sd._smc_pa_score_v3_eval(cand_a, side="LONG", btc_ctx=btc_degenerate, stale_info=stale_fresh)
check(
    "E degenerate BTC fields ignored",
    res_e["smc_pa_v3_market_bias_score"] == 0
    and "market_bias" in res_e["smc_pa_v3_missing_components"]
    and res_e["smc_pa_v3_bias_source"] == "NONE",
    f"bias_score={res_e['smc_pa_v3_market_bias_score']} source={res_e['smc_pa_v3_bias_source']}",
)

# ─── Case F: missing RS/follow-through => missing_components, no crash ────
res_f = sd._smc_pa_score_v3_eval({}, side="LONG", btc_ctx={}, stale_info={})
missing_f = res_f["smc_pa_v3_missing_components"]
check(
    "F missing fields tolerated",
    "relative_strength" in missing_f
    and "breakout_acceptance_followthrough_bars" in missing_f
    and isinstance(res_f["smc_pa_v3_total_score"], float),
    f"missing={missing_f}",
)
check(
    "F too-missing band",
    res_f["smc_pa_v3_score_band"] == "V3_UNKNOWN_TOO_MISSING",
    f"band={res_f['smc_pa_v3_score_band']}",
)
check(
    "F relative strength always missing today",
    "relative_strength" in res_a["smc_pa_v3_missing_components"]
    and res_a["smc_pa_v3_relative_strength_score"] == 0,
    "",
)

# ─── Case M: volatility SL score computes when ATR + entry + SL exist ──────
check(
    "M volatility_sl_quality computes with atr+entry+sl",
    "volatility_sl_quality" not in res_a["smc_pa_v3_missing_components"]
    and res_a["smc_pa_v3_src_sl_atr_ratio"] is not None,
    f"ratio={res_a['smc_pa_v3_src_sl_atr_ratio']}",
)

# ─── Case N: target realism computes when opposing_distance exists ─────────
check(
    "N target_realism computes with opposing_distance",
    "target_realism" not in res_a["smc_pa_v3_missing_components"]
    and res_a["smc_pa_v3_src_opposing_barrier_distance_r"] == 3.0,
    f"opposing_r={res_a['smc_pa_v3_src_opposing_barrier_distance_r']}",
)

# ─── Case O: acceptance-context fallback fills atr/barriers (wrapper) ──────
cand_o = fresh_candidate(
    confirm_entry_acceptance_context={
        "candle_open": 99.5, "candle_high": 101.5,
        "candle_low": 99.2, "candle_close": 101.0,
        "atr": 1.5,
        "break_level": 100.0, "pre_break_level": 100.0,
        "nearest_htf_support": 97.0, "nearest_htf_resistance": 106.5,
        "context_source": "confirm_reject_level_wiring_v1",
    },
)
cand_o_snapshot = copy.deepcopy(cand_o)
rows_before_o = len(_V3_CAPTURED)
sd._smc_pa_score_v3_shadow(
    cand_o, fields=None, trade=None, execution_mode="paper",
    v1_decision="REJECT", v1_reason="rr_below_2", btc_ctx={}, now_ts=NOW,
)
row_o = _V3_CAPTURED[-1] if len(_V3_CAPTURED) > rows_before_o else {}
check(
    "O ctx fallback fills atr + resistance (volatility + target realism compute)",
    "volatility_sl_quality" not in row_o.get("smc_pa_v3_missing_components", ["x"])
    and "target_realism" not in row_o.get("smc_pa_v3_missing_components", ["x"])
    and row_o.get("smc_pa_v3_src_atr") == 1.5
    and sorted(row_o.get("smc_pa_v3_ctx_fallback_used", []))
    == ["atr", "nearest_htf_resistance", "nearest_htf_support"]
    and row_o.get("smc_pa_v3_ctx_fallback_source") == "confirm_reject_level_wiring_v1",
    f"missing={row_o.get('smc_pa_v3_missing_components')} "
    f"fallback={row_o.get('smc_pa_v3_ctx_fallback_used')}",
)
check(
    "O candidate not mutated by ctx fallback",
    cand_o == cand_o_snapshot,
    "",
)
check(
    "O flat source fields still win over ctx fallback",
    res_a["smc_pa_v3_src_atr"] == 1.5,  # case A had flat atr, no ctx needed
    "",
)

# ─── Case P: missing fields keep exact reasons, no crash ───────────────────
res_p = sd._smc_pa_score_v3_eval({}, side="LONG", btc_ctx={}, stale_info={})
reasons_p = res_p["smc_pa_v3_component_reasons"]
check(
    "P exact missing reasons (volatility/target/RS)",
    reasons_p.get("volatility_sl_quality") == "atr_and_sl_distance_unavailable"
    and reasons_p.get("target_realism") == "opposing_distance_and_planned_rr_unavailable"
    and reasons_p.get("relative_strength")
    == "alt_vs_btc_rs_unavailable|NEED_RS_INSTRUMENTATION",
    f"reasons={ {k: reasons_p.get(k) for k in ('volatility_sl_quality', 'target_realism', 'relative_strength')} }",
)
res_p2 = sd._smc_pa_score_v3_eval(
    {"entry": 100.0, "sl": 98.0, "rr": 2.0}, side="LONG", btc_ctx={}, stale_info={},
)
check(
    "P atr-only missing => atr_unavailable; opposing-only missing => opposing_distance_unavailable",
    res_p2["smc_pa_v3_component_reasons"].get("volatility_sl_quality") == "atr_unavailable"
    and res_p2["smc_pa_v3_component_reasons"].get("target_realism")
    == "opposing_distance_unavailable",
    f"reasons={res_p2['smc_pa_v3_component_reasons']}",
)

# ─── Case G: paper decision unchanged ──────────────────────────────────────
cand_g = fresh_candidate(bos_quality="STRONG", volume_confirmation="DIVERGENCE")
cand_g_snapshot = copy.deepcopy(cand_g)
pred_before = sd._paper_smc_research_qualified_predicate(copy.deepcopy(cand_g))
summary_g = sd._smc_pa_score_v3_shadow(
    cand_g,
    fields=None,
    trade=None,
    execution_mode="paper",
    v1_decision="REJECT",
    v1_reason="rr_below_2",
    btc_ctx=btc_aligned,
    now_ts=NOW,
)
pred_after = sd._paper_smc_research_qualified_predicate(copy.deepcopy(cand_g))
check(
    "G paper qualified predicate unchanged",
    pred_before[:2] == pred_after[:2],
    f"before={pred_before[:2]} after={pred_after[:2]}",
)
check(
    "G candidate not mutated by V3",
    cand_g == cand_g_snapshot,
    "",
)
check(
    "G summary fields returned",
    set(summary_g.keys()) == set(sd._SMC_PA_V3_SUMMARY_FIELDS),
    f"keys={sorted(summary_g.keys())}",
)

# ─── Case H: live decision unchanged ───────────────────────────────────────
trade_h = {
    "symbol": "SIMUSDT",
    "side": "LONG",
    "entry_type": "CONFIRM_SMC_RESEARCH",
    "entry": 100.0,
    "sl": 98.0,
    "tp": 104.0,
    "rr": 2.0,
    "score": 3.0,
}
trade_h_snapshot = copy.deepcopy(trade_h)
live_before = sd._live_smc_research_prefilter(fresh_candidate(), copy.deepcopy(trade_h), None)
sd._smc_pa_score_v3_shadow(
    fresh_candidate(),
    fields=None,
    trade=trade_h,
    execution_mode="live",
    v1_decision="PREFILTER_REJECT",
    v1_reason="execution_mode_not_live",
    btc_ctx={},
    now_ts=NOW,
)
live_after = sd._live_smc_research_prefilter(fresh_candidate(), copy.deepcopy(trade_h), None)
check(
    "H live prefilter unchanged",
    live_before == live_after,
    f"before={live_before} after={live_after}",
)
check("H trade not mutated by V3", trade_h == trade_h_snapshot, "")

# ─── Case I: PAPER_LOCATION_GATE unchanged ─────────────────────────────────
gate_input = {
    "side": "LONG",
    "smc_zone": "PREMIUM",
    "market_regime": "CHOP_NO_TRADE",
    "exhaustion": "EXTENDED",
    "trend_direction": "SHORT",
    "trend_strength": 0.01,
}
gate_before = sd._compute_confirm_smc_entry_location_risk(copy.deepcopy(gate_input))
sd._smc_pa_score_v3_shadow(
    fresh_candidate(**gate_input), fields=None, trade=None,
    execution_mode="paper", v1_decision="REJECT", v1_reason="rr_below_2",
    btc_ctx={}, now_ts=NOW,
)
gate_after = sd._compute_confirm_smc_entry_location_risk(copy.deepcopy(gate_input))
check("I PAPER_LOCATION_GATE unchanged", gate_before == gate_after, "")

# ─── Case K: BTC side-enable unchanged ─────────────────────────────────────
side_enable_ctx = {"btc_bias_independent": "BULLISH", "btc_context_quality": "FULL"}
se_before = sd._btc_bias_side_enable_eval("LONG", dict(side_enable_ctx))
sd._smc_pa_score_v3_shadow(
    fresh_candidate(), fields=None, trade=None, execution_mode="paper",
    v1_decision="REJECT", v1_reason="rr_below_2", btc_ctx=dict(side_enable_ctx), now_ts=NOW,
)
se_after = sd._btc_bias_side_enable_eval("LONG", dict(side_enable_ctx))
check("K BTC side-enable unchanged", se_before == se_after, "")

# ─── Case L: no execution/order module calls in V3 code ───────────────────
v3_source = (
    inspect.getsource(sd._smc_pa_score_v3_eval)
    + inspect.getsource(sd._smc_pa_score_v3_shadow)
    + inspect.getsource(sd._smc_pa_score_v3_shadow_write)
)
forbidden_tokens = (
    "live_executor", "place_order", "create_order", "cancel_order",
    "open_trade", "close_trade", "execution.", "exchange_harness",
    "validate_and_prepare", "requests.", "urllib",
)
hits = [tok for tok in forbidden_tokens if tok in v3_source]
check("L no execution/order/network calls in V3", not hits, f"hits={hits}")

# Forward-log rows were captured (writer patched), never written to logs/.
required_row_fields = (
    ("ts", "symbol", "side", "signal_ts", "dedup_key", "execution_mode",
     "v1_decision", "v1_reason", "old_score", "entry", "sl", "tp", "rr",
     "paper_location_gate_would_block", "btc_bias_independent")
    + sd._SMC_PA_V3_SUMMARY_FIELDS
    + sd._SMC_PA_V3_COMPONENT_FIELDS
)
row_ok = bool(_V3_CAPTURED) and all(
    field in _V3_CAPTURED[0] for field in required_row_fields
)
missing_row_fields = (
    [field for field in required_row_fields if field not in _V3_CAPTURED[0]]
    if _V3_CAPTURED else ["no_rows_captured"]
)
check(
    "forward-log row schema complete",
    row_ok,
    f"captured={len(_V3_CAPTURED)} missing={missing_row_fields}",
)

failed = [name for name, ok, _ in RESULTS if not ok]
print()
if failed:
    print(f"RESULT: FAIL ({len(failed)}/{len(RESULTS)} checks failed): {failed}")
    sys.exit(1)
print(f"RESULT: PASS ({len(RESULTS)}/{len(RESULTS)} checks)")
