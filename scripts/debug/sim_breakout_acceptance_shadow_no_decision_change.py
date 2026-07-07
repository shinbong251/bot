#!/usr/bin/env python3
"""Simulator: BREAKOUT_ACCEPTANCE_SHADOW is shadow/log-only with no decision change.

No Binance calls, no orders, no real log/state writes. It imports the
production helpers, monkeypatches the shadow writer to memory, and booby-traps
the network fetcher plus the old-score / V3 / BTC-side-enable / V2B /
location-gate evaluators so any hidden call fails loudly. It verifies:

  A LONG close beyond level + holds 2 bars           => BREAKOUT_ACCEPTED
  B SHORT close beyond level + holds 2 bars          => BREAKOUT_ACCEPTED
  C LONG breaks then closes back below level         => BREAKOUT_FAILED_BACK_INSIDE
  D SHORT breaks then closes back above level        => BREAKOUT_FAILED_BACK_INSIDE
  E wick break without close-through                 => BREAKOUT_WICK_REJECTED
  F retest holds level                               => BREAKOUT_RETEST_HELD
  G missing bos/breakout level                       => BREAKOUT_UNKNOWN_MISSING_LEVEL
  H paper decision unchanged (inputs unmutated, fields not folded back)
  I live decision unchanged (inputs unmutated, fields not folded back)
  J SMC_PA_SCORE_V3 unchanged (never invoked; read-only passthrough only)
  K PAPER_LOCATION_GATE unchanged (never invoked; read-only passthrough only)
  L BTC side-enable unchanged (never invoked; read-only passthrough only)
  M V2B unchanged (never invoked; read-only passthrough only)
  N no execution/order module calls, no network fetch, no disk writes
"""

import copy
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import signal_dispatcher as sd
import pool_pipeline

SIGNAL_TS = 1_750_000_000.0

_CAPTURED = []


def _capture(row):
    _CAPTURED.append(row)


def _boom_factory(name):
    def _boom(*args, **kwargs):
        raise AssertionError(f"{name} was invoked by BREAKOUT_ACCEPTANCE_SHADOW")
    return _boom


def _bar(open_, high, low, close):
    return {"open": open_, "high": high, "low": low, "close": close}


def _check(name, cond, detail=""):
    assert cond, f"FAIL {name}: {detail}"
    print(f"PASS {name}")


def _candidate(side, with_acceptance_ctx=True, level=100.0):
    cand = {
        "symbol": "ETHUSDT",
        "side": side,
        "dedup_key": f"ETHUSDT|{side}|CONFIRM|{SIGNAL_TS}",
        "signal_created_ts": SIGNAL_TS,
        "source_timestamp": SIGNAL_TS,
        "entry_type": "CONFIRM",
        "entry": 101.0 if side == "LONG" else 99.0,
        "sl": 99.0 if side == "LONG" else 101.0,
        "tp": 105.0 if side == "LONG" else 95.0,
        "rr": 2.0,
        "score": 6.5,
        "smc_zone": "EQUILIBRIUM",
        "market_regime": "TRENDING_CONTINUATION",
        "phase": "BREAKOUT",
        "bos_quality": "CLOSE_THROUGH",
    }
    if with_acceptance_ctx:
        if side == "LONG":
            candle = {"candle_open": 99.5, "candle_high": 101.5,
                      "candle_low": 99.2, "candle_close": 101.0}
        else:
            candle = {"candle_open": 100.5, "candle_high": 100.8,
                      "candle_low": 98.5, "candle_close": 99.0}
        cand["confirm_entry_acceptance_context"] = dict(candle, break_level=level)
    return cand


def main():
    # Redirect the shadow writer to memory: no real logs/ writes.
    sd._breakout_acceptance_shadow_write = _capture

    # Booby-trap everything the shadow must never touch.
    pool_pipeline.fetch = _boom_factory("pool_pipeline.fetch (network)")
    if hasattr(sd, "fetch"):
        sd.fetch = _boom_factory("signal_dispatcher.fetch (network)")
    sd._smc_pa_score_v3_eval = _boom_factory("SMC_PA_SCORE_V3 evaluator")
    sd._smc_pa_score_v3_shadow = _boom_factory("SMC_PA_SCORE_V3 shadow")
    if hasattr(sd, "_btc_bias_side_enable_eval"):
        sd._btc_bias_side_enable_eval = _boom_factory("BTC side-enable evaluator")
    if hasattr(sd, "_compute_confirm_smc_entry_location_risk"):
        sd._compute_confirm_smc_entry_location_risk = _boom_factory("PAPER_LOCATION_GATE risk computer")
    if hasattr(sd, "_smc_entry_v2b_allowlist_shadow"):
        sd._smc_entry_v2b_allowlist_shadow = _boom_factory("V2B allowlist shadow")

    log_path = os.path.join(REPO_ROOT, "logs", "breakout_acceptance_shadow.jsonl")
    log_stat_before = os.stat(log_path) if os.path.exists(log_path) else None

    ev = sd._breakout_acceptance_eval
    LEVEL = 100.0

    # A. LONG close beyond level + holds 2 bars => ACCEPTED
    a = ev(
        "LONG", LEVEL,
        signal_candle=_bar(99.5, 101.5, 99.2, 101.0),
        follow_bars=[_bar(101.0, 102.5, 100.8, 102.0), _bar(102.0, 103.5, 101.8, 103.0)],
        entry=101.0, sl=99.0,
    )
    _check("A LONG hold-2-bars => ACCEPTED",
           a["breakout_acceptance_label"] == "BREAKOUT_ACCEPTED"
           and a["close_beyond_level"] is True
           and a["acceptance_1bar"] is True and a["acceptance_2bar"] is True
           and a["held_breakout_level"] is True
           and a["failed_back_inside_level"] is False
           and a["max_favorable_r_after_3bars"] == 1.25
           and a["time_to_0_25r_bars"] == 1, a)

    # B. SHORT close beyond level + holds 2 bars => ACCEPTED
    b = ev(
        "SHORT", LEVEL,
        signal_candle=_bar(100.5, 100.8, 98.5, 99.0),
        follow_bars=[_bar(99.0, 99.2, 97.5, 98.0), _bar(98.0, 98.2, 96.5, 97.0)],
        entry=99.0, sl=101.0,
    )
    _check("B SHORT hold-2-bars => ACCEPTED",
           b["breakout_acceptance_label"] == "BREAKOUT_ACCEPTED"
           and b["close_beyond_level"] is True
           and b["held_breakout_level"] is True
           and b["max_favorable_r_after_3bars"] == 1.25, b)

    # C. LONG breaks then closes back below level => FAILED_BACK_INSIDE
    c = ev(
        "LONG", LEVEL,
        signal_candle=_bar(99.5, 101.5, 99.2, 101.0),
        follow_bars=[_bar(101.0, 101.2, 99.0, 99.5)],
        entry=101.0, sl=99.0,
    )
    _check("C LONG close back below => FAILED_BACK_INSIDE",
           c["breakout_acceptance_label"] == "BREAKOUT_FAILED_BACK_INSIDE"
           and c["failed_back_inside_level"] is True
           and c["acceptance_1bar"] is False, c)

    # D. SHORT breaks then closes back above level => FAILED_BACK_INSIDE
    d = ev(
        "SHORT", LEVEL,
        signal_candle=_bar(100.5, 100.8, 98.5, 99.0),
        follow_bars=[_bar(99.0, 101.0, 98.8, 100.5)],
        entry=99.0, sl=101.0,
    )
    _check("D SHORT close back above => FAILED_BACK_INSIDE",
           d["breakout_acceptance_label"] == "BREAKOUT_FAILED_BACK_INSIDE"
           and d["failed_back_inside_level"] is True, d)

    # E. Wick break without close-through => WICK_REJECTED
    e = ev(
        "LONG", LEVEL,
        signal_candle=_bar(99.0, 101.0, 98.8, 99.5),
        follow_bars=None,
        entry=99.5, sl=98.0,
    )
    _check("E wick break, no close-through => WICK_REJECTED",
           e["breakout_acceptance_label"] == "BREAKOUT_WICK_REJECTED"
           and e["close_beyond_level"] is False
           and e["wick_rejection"] is True, e)

    # F. Retest holds level => RETEST_HELD (bar dips into level zone, closes beyond, holds)
    f = ev(
        "LONG", LEVEL,
        signal_candle=_bar(99.5, 101.5, 99.2, 101.0),
        follow_bars=[_bar(101.0, 101.2, 100.2, 100.8), _bar(100.8, 102.0, 100.7, 101.8)],
        entry=101.0, sl=99.0,
    )
    _check("F retest holds level => RETEST_HELD",
           f["breakout_acceptance_label"] == "BREAKOUT_RETEST_HELD"
           and f["retest_held"] is True and f["retest_failed"] is False
           and f["held_breakout_level"] is True, f)

    # G. Missing bos/breakout level => UNKNOWN_MISSING_LEVEL
    g = ev("LONG", None, signal_candle=_bar(99.5, 101.5, 99.2, 101.0))
    _check("G missing level => UNKNOWN_MISSING_LEVEL",
           g["breakout_acceptance_label"] == "BREAKOUT_UNKNOWN_MISSING_LEVEL"
           and g["close_beyond_level"] is None, g)

    # Decision-time (no follow bars) close-through stays PENDING, not terminal.
    p = ev("LONG", LEVEL, signal_candle=_bar(99.5, 101.5, 99.2, 101.0),
           entry=101.0, sl=99.0)
    _check("decision-time close-through => PENDING_LIFECYCLE (non-terminal)",
           p["breakout_acceptance_label"] == "BREAKOUT_PENDING_LIFECYCLE"
           and p["follow_bars_observed"] == 0, p)

    # ---- End-to-end assembler: paper (H) and live (I) decisions unchanged ----
    gate_fields = {
        "planned_rr": 2.0,
        "confirm_smc_entry_location_would_block": True,
        "confirm_smc_entry_location_primary_reason": "premium_long",
        "confirm_smc_entry_location_risk_bucket": "HIGH",
        "v2b_label": "CONFIRM_SMC_RESEARCH__DIRECTIONAL_BIAS_CONTEXT",
        "v2b_match": True,
        "v2b_reason": "shadow_reason",
    }
    btc_ctx = {
        "btc_bias_independent": "BULLISH",
        "btc_alignment_independent": "ALIGNED",
        "btc_context_quality": "OK",
        "btc_side_enable_shadow_label": "BTC_SIDE_ENABLE_ALLOW",
        "btc_side_enable_shadow_allow": True,
    }
    v3_summary = {
        "smc_pa_v3_total_score": 5.0,
        "smc_pa_v3_score_band": "STRONG",
        "smc_pa_v3_version": sd._SMC_PA_V3_VERSION,
    }

    for tag, mode, decision, reason, trade in (
        ("H", "paper", "OPEN", "qualified_open", None),
        ("I", "live", "OPEN", "confirmed", {
            "entry_type": "CONFIRM_SMC_RESEARCH", "side": "LONG",
            "entry": 101.0, "sl": 99.0, "tp": 105.0, "rr": 2.0, "score": 7.0,
            "bos_quality": "CLOSE_THROUGH",
        }),
    ):
        cand = _candidate("LONG")
        cand_before = copy.deepcopy(cand)
        fields_before = copy.deepcopy(gate_fields)
        trade_before = copy.deepcopy(trade)
        btc_before = copy.deepcopy(btc_ctx)
        v3_before = copy.deepcopy(v3_summary)
        row = sd._breakout_acceptance_shadow(
            cand,
            fields=dict(gate_fields),
            trade=trade,
            execution_mode=mode,
            v1_decision=decision,
            v1_reason=reason,
            btc_ctx=btc_ctx,
            v3_summary=v3_summary,
            now_ts=SIGNAL_TS,
        )
        _check(f"{tag} {mode} candidate dict unmutated", cand == cand_before, cand)
        _check(f"{tag} {mode} trade dict unmutated", trade == trade_before, trade)
        _check(f"{tag} {mode} btc_ctx/v3 dicts unmutated",
               btc_ctx == btc_before and v3_summary == v3_before, (btc_ctx, v3_summary))
        _check(f"{tag} {mode} gate fields template unmutated",
               gate_fields == fields_before, gate_fields)
        _check(f"{tag} {mode} v1 decision/reason preserved in row (additive log only)",
               row.get("v1_decision") == decision and row.get("v1_reason") == reason
               and row.get("execution_mode") == mode, row)
        _check(f"{tag} {mode} no shadow fields folded into decision inputs",
               all("breakout_acceptance" not in key for key in list(cand) + list(gate_fields))
               and (trade is None or all("breakout_acceptance" not in key for key in trade)),
               (list(cand), trade))
        _check(f"{tag} {mode} row logged with expected event/label fields",
               _CAPTURED and _CAPTURED[-1]["event_type"] == "BREAKOUT_ACCEPTANCE_SHADOW"
               and _CAPTURED[-1]["breakout_acceptance_label"] in (
                   "BREAKOUT_PENDING_LIFECYCLE", "BREAKOUT_WICK_REJECTED",
                   "BREAKOUT_NO_FOLLOWTHROUGH", "BREAKOUT_UNKNOWN_MISSING_LEVEL")
               and _CAPTURED[-1]["lifecycle_tracking"] == "MISSING_RUNTIME_DEFERRED_TO_AUDIT"
               and set(("ts", "symbol", "side", "dedup_key", "signal_ts", "execution_mode",
                        "entry", "sl", "tp", "rr", "entry_price", "bos_level",
                        "breakout_level", "phase", "bos_quality", "market_regime",
                        "smc_zone", "old_score", "btc_bias_independent",
                        "close_beyond_level", "wick_rejection", "retest_candidate"))
               <= set(_CAPTURED[-1].keys()),
               _CAPTURED[-1] if _CAPTURED else "no rows captured")

    # J. SMC_PA_SCORE_V3 unchanged: evaluator/shadow are booby-trapped above and
    #    were never invoked; V3 fields appear read-only in the logged row.
    _check("J SMC_PA_SCORE_V3 never invoked; passthrough read-only",
           _CAPTURED[-1]["smc_pa_v3_total_score"] == 5.0
           and _CAPTURED[-1]["smc_pa_v3_score_band"] == "STRONG"
           and _CAPTURED[-1]["smc_pa_v3_version"] == sd._SMC_PA_V3_VERSION,
           _CAPTURED[-1])

    # K. PAPER_LOCATION_GATE unchanged: risk computer booby-trapped and never
    #    invoked; gate fields appear read-only in the logged row.
    _check("K PAPER_LOCATION_GATE never invoked; passthrough read-only",
           _CAPTURED[-1]["paper_location_gate_would_block"] is True
           and _CAPTURED[-1]["paper_location_gate_primary_reason"] == "premium_long"
           and _CAPTURED[-1]["paper_location_gate_risk_bucket"] == "HIGH",
           _CAPTURED[-1])

    # L. BTC side-enable unchanged: evaluator booby-trapped and never invoked;
    #    side-enable fields appear read-only in the logged row.
    _check("L BTC side-enable never invoked; passthrough read-only",
           _CAPTURED[-1]["btc_side_enable_shadow_label"] == "BTC_SIDE_ENABLE_ALLOW"
           and _CAPTURED[-1]["btc_side_enable_shadow_allow"] is True,
           _CAPTURED[-1])

    # M. V2B unchanged: allowlist shadow booby-trapped and never invoked;
    #    V2B fields appear read-only in the logged row.
    _check("M V2B never invoked; passthrough read-only",
           _CAPTURED[-1]["v2b_label"] == "CONFIRM_SMC_RESEARCH__DIRECTIONAL_BIAS_CONTEXT"
           and _CAPTURED[-1]["v2b_match"] is True,
           _CAPTURED[-1])

    # ---- Level wiring provenance (log-only fields, no decision change) ----
    # O. Level present via acceptance context => label NOT UNKNOWN_MISSING_LEVEL
    #    and provenance fields populated.
    o_row = sd._breakout_acceptance_shadow(
        _candidate("LONG"), execution_mode="paper",
        v1_decision="REJECT", v1_reason="rr_below_2", now_ts=SIGNAL_TS,
    )
    _check("O level present => label not UNKNOWN_MISSING_LEVEL",
           o_row.get("breakout_acceptance_label") != "BREAKOUT_UNKNOWN_MISSING_LEVEL"
           and o_row.get("breakout_level") == 100.0
           and o_row.get("level_available") is True
           and o_row.get("level_source") == "confirm_entry_acceptance_context.break_level"
           and o_row.get("level_missing_reason") is None
           and o_row.get("signal_candle_available") is True, o_row)

    # P. Level missing everywhere => UNKNOWN with exact missing reason.
    p_cand = _candidate("LONG", with_acceptance_ctx=False)
    p_row = sd._breakout_acceptance_shadow(
        p_cand, execution_mode="paper",
        v1_decision="REJECT", v1_reason="rr_below_2", now_ts=SIGNAL_TS,
    )
    _check("P level missing => UNKNOWN + exact reason",
           p_row.get("breakout_acceptance_label") == "BREAKOUT_UNKNOWN_MISSING_LEVEL"
           and p_row.get("level_available") is False
           and p_row.get("level_source") is None
           and p_row.get("level_missing_reason")
           == "acceptance_context_absent_and_no_source_level_fields", p_row)

    # Q. Level from flat source field (no acceptance context) => provenance
    #    says source.bos_level; label resolves from level + no candle => still
    #    UNKNOWN (candle unavailable) but level fields are populated.
    q_cand = _candidate("LONG", with_acceptance_ctx=False)
    q_cand["bos_level"] = 100.0
    q_row = sd._breakout_acceptance_shadow(
        q_cand, execution_mode="paper",
        v1_decision="REJECT", v1_reason="rr_below_2", now_ts=SIGNAL_TS,
    )
    _check("Q level from source.bos_level provenance",
           q_row.get("level_available") is True
           and q_row.get("level_source") == "source.bos_level"
           and q_row.get("breakout_level") == 100.0
           and q_row.get("signal_candle_available") is False, q_row)

    # R. Acceptance context present but break_level null => exact reason.
    r_cand = _candidate("LONG", with_acceptance_ctx=True)
    r_cand["confirm_entry_acceptance_context"] = dict(
        r_cand["confirm_entry_acceptance_context"], break_level=None, pre_break_level=None,
    )
    r_row = sd._breakout_acceptance_shadow(
        r_cand, execution_mode="paper",
        v1_decision="REJECT", v1_reason="rr_below_2", now_ts=SIGNAL_TS,
    )
    _check("R context present but level null => exact reason",
           r_row.get("level_available") is False
           and r_row.get("level_missing_reason")
           == "acceptance_context_present_but_break_level_null"
           and r_row.get("breakout_acceptance_label") == "BREAKOUT_UNKNOWN_MISSING_LEVEL",
           r_row)

    # Non-CONFIRM entry types are ignored entirely (CONFIRM_SMC_RESEARCH only).
    rows_before = len(_CAPTURED)
    skipped = sd._breakout_acceptance_shadow(
        {"symbol": "ETHUSDT", "side": "LONG", "entry_type": "EARLY_CONT"},
        execution_mode="paper", v1_decision="OPEN", v1_reason="x", now_ts=SIGNAL_TS,
    )
    _check("non-CONFIRM entry type skipped (no row, empty return)",
           skipped == {} and len(_CAPTURED) == rows_before, skipped)

    # N. No execution/order module calls, no network fetch, no disk writes.
    _check("N no execution/order call names in dispatcher globals",
           not any(name in sd.__dict__ for name in (
               "place_order", "cancel_order", "create_order", "submit_order")),
           [n for n in ("place_order", "cancel_order", "create_order", "submit_order")
            if n in sd.__dict__])
    log_stat_after = os.stat(log_path) if os.path.exists(log_path) else None
    unchanged = (
        (log_stat_before is None and log_stat_after is None)
        or (log_stat_before is not None and log_stat_after is not None
            and log_stat_before.st_size == log_stat_after.st_size)
    )
    _check("N writer redirected to memory (forward log untouched on disk)",
           unchanged and len(_CAPTURED) > 0,
           (log_stat_before, log_stat_after, len(_CAPTURED)))
    _check("N no network fetch (fetchers booby-trapped, never triggered)", True)

    print(f"\nPASS all BREAKOUT_ACCEPTANCE_SHADOW cases "
          f"({len(_CAPTURED)} rows captured in-memory, 0 written to disk)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
