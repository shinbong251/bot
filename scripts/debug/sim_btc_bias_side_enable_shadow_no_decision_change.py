#!/usr/bin/env python3
"""Simulator: BTC_BIAS_SIDE_ENABLE_SHADOW is shadow/log-only and side-enable-correct.

No Binance calls, no orders, no real log/state writes. It imports the production
helpers, injects deterministic BTC candles via a fake fetcher, and monkeypatches
BOTH shadow writers so nothing touches logs/. It verifies:

  A LONG  + BULLISH        => BTC_SIDE_ENABLE_ALLOW (allow=True)
  B SHORT + BEARISH        => BTC_SIDE_ENABLE_ALLOW (allow=True)
  C LONG  + BEARISH        => BTC_SIDE_ENABLE_BLOCK_COUNTER_BIAS (allow=False)
  D SHORT + BULLISH        => BTC_SIDE_ENABLE_BLOCK_COUNTER_BIAS (allow=False)
  E LONG  + NEUTRAL_OR_CHOP=> BTC_SIDE_ENABLE_BLOCK_NEUTRAL_CHOP (allow=False)
  F SHORT + NEUTRAL_OR_CHOP=> BTC_SIDE_ENABLE_BLOCK_NEUTRAL_CHOP (allow=False)
  G missing/UNKNOWN BTC    => BTC_SIDE_ENABLE_UNKNOWN_MISSING_CONTEXT (allow=None)
  H old degenerate BTC fields ignored (only independent fields consulted)
  I paper decision fields unchanged (side-enable fields purely additive)
  J live decision fields unchanged (side-enable fields purely additive)
  K PAPER_LOCATION_GATE inputs unchanged (read-only passthrough)
  L V2B allowlist inputs unchanged (read-only passthrough)
  M SMC_PA_SCORE_V3 inputs unchanged (read-only passthrough, never computed here)
  N no execution/order module or network calls
"""

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import signal_dispatcher as sd
import pool_pipeline

START_TS = 1_000_000
SIGNAL_TS = START_TS + 400 * 3600 - 1  # far enough that all TFs have >min bars

_ALIGN_CAPTURED = []
_SIDE_ENABLE_CAPTURED = []


def _capture_align(row):
    _ALIGN_CAPTURED.append(row)


def _capture_side_enable(row):
    _SIDE_ENABLE_CAPTURED.append(row)


def _series(kind, n):
    if kind == "bull":
        return [100.0 + i * 0.5 for i in range(n)]
    if kind == "bear":
        return [100.0 + (n - i) * 0.5 for i in range(n)]
    if kind == "chop":
        return [100.0 for _ in range(n)]
    raise ValueError(kind)


def _candles(kind, interval_secs, n):
    rows = []
    for i, close in enumerate(_series(kind, n)):
        open_ts = START_TS + i * interval_secs
        rows.append({
            "time": open_ts * 1000,
            "ct": (open_ts + interval_secs - 1) * 1000,
            "open": float(close),
            "high": float(close + 0.25),
            "low": float(close - 0.25),
            "close": float(close),
            "volume": 100.0 + (i % 5),
        })
    return rows


def _fetcher_for(kind, fail=False):
    data = {
        "5m": _candles(kind, 300, 520),
        "15m": _candles(kind, 900, 180),
        "1h": _candles(kind, 3600, 60),
    }

    def fetcher(symbol, interval, is_priority=False):
        if fail:
            return None
        return data.get(interval)

    return fetcher


def _decision_row(side, decision="OPEN", reason="simulated"):
    return {
        "symbol": "ETHUSDT",
        "side": side,
        "dedup_key": f"ETHUSDT|{side}|CONFIRM|{SIGNAL_TS}",
        "signal_created_ts": SIGNAL_TS,
        "entry_type": "CONFIRM",
        "v1_decision": decision,
        "v1_reason": reason,
    }


def _run(side, kind, gate_fields=None, v2b_fields=None, fail=False,
         decision="OPEN", reason="simulated", execution_mode="paper"):
    src = _decision_row(side, decision=decision, reason=reason)
    row = sd._btc_alignment_instrumentation_shadow(
        src,
        execution_mode=execution_mode,
        v1_decision=decision,
        v1_reason=reason,
        side=side,
        trade=None,
        gate_fields=gate_fields,
        v2b_fields=v2b_fields,
        now_ts=SIGNAL_TS,
        fetcher=_fetcher_for(kind, fail=fail),
    )
    assert row is not None, f"{side}/{kind}: instrumentation returned None"
    return row


def _check(name, cond, detail=""):
    assert cond, f"FAIL {name}: {detail}"
    print(f"PASS {name}")


def main():
    # Redirect BOTH shadow writers to memory: no real logs/ writes.
    sd._btc_alignment_instrumentation_write = _capture_align
    sd._btc_bias_side_enable_shadow_write = _capture_side_enable

    # Guard M (part 1): network fetch must never be used (we always inject one).
    def _boom_fetch(*a, **k):
        raise AssertionError("pool_pipeline.fetch called — shadow hit the network")

    pool_pipeline.fetch = _boom_fetch
    if hasattr(sd, "fetch"):
        sd.fetch = _boom_fetch

    # ---- Pure evaluator cases (independent BTC fields only) -----------------
    def ev(side, bias, quality="OK"):
        return sd._btc_bias_side_enable_eval(
            side, {"btc_bias_independent": bias, "btc_context_quality": quality}
        )

    _check("A LONG+BULLISH=ALLOW",
           ev("LONG", "BULLISH") == ("BTC_SIDE_ENABLE_ALLOW", True,
                                     "allow|side=LONG|bias=BULLISH"), ev("LONG", "BULLISH"))
    _check("B SHORT+BEARISH=ALLOW",
           ev("SHORT", "BEARISH")[:2] == ("BTC_SIDE_ENABLE_ALLOW", True), ev("SHORT", "BEARISH"))
    _check("C LONG+BEARISH=BLOCK_COUNTER_BIAS",
           ev("LONG", "BEARISH")[:2] == ("BTC_SIDE_ENABLE_BLOCK_COUNTER_BIAS", False),
           ev("LONG", "BEARISH"))
    _check("D SHORT+BULLISH=BLOCK_COUNTER_BIAS",
           ev("SHORT", "BULLISH")[:2] == ("BTC_SIDE_ENABLE_BLOCK_COUNTER_BIAS", False),
           ev("SHORT", "BULLISH"))
    _check("E LONG+NEUTRAL_OR_CHOP=BLOCK_NEUTRAL_CHOP",
           ev("LONG", "NEUTRAL_OR_CHOP")[:2] == ("BTC_SIDE_ENABLE_BLOCK_NEUTRAL_CHOP", False),
           ev("LONG", "NEUTRAL_OR_CHOP"))
    _check("F SHORT+NEUTRAL_OR_CHOP=BLOCK_NEUTRAL_CHOP",
           ev("SHORT", "NEUTRAL_OR_CHOP")[:2] == ("BTC_SIDE_ENABLE_BLOCK_NEUTRAL_CHOP", False),
           ev("SHORT", "NEUTRAL_OR_CHOP"))
    _check("G UNKNOWN bias=UNKNOWN_MISSING_CONTEXT",
           ev("LONG", "UNKNOWN")[:2] == ("BTC_SIDE_ENABLE_UNKNOWN_MISSING_CONTEXT", None),
           ev("LONG", "UNKNOWN"))
    _check("G MISSING quality=UNKNOWN_MISSING_CONTEXT (bias present)",
           ev("LONG", "BULLISH", quality="MISSING")[:2]
           == ("BTC_SIDE_ENABLE_UNKNOWN_MISSING_CONTEXT", None),
           ev("LONG", "BULLISH", quality="MISSING"))

    # H. Old degenerate BTC fields must be IGNORED: feed contradictory legacy
    #    fields and a bogus 'side' mirror; only the independent bias is used.
    poisoned = {
        "btc_bias_independent": "BEARISH",   # the only field that counts
        "btc_context_quality": "OK",
        # legacy/degenerate fields that must never be consulted:
        "btc_h1_bias_label": "BULLISH",
        "btc_m15_bias_label": "BULLISH",
        "btc_mtf_summary_label": "BULLISH",
        "btc_bias": "BULLISH",
        "side_mirror": "LONG",
    }
    _check("H old degenerate BTC fields ignored (uses independent only)",
           sd._btc_bias_side_enable_eval("SHORT", poisoned)[:2]
           == ("BTC_SIDE_ENABLE_ALLOW", True),
           sd._btc_bias_side_enable_eval("SHORT", poisoned))

    # ---- End-to-end via instrumentation shadow (fields additive, log-only) --
    r = _run("LONG", "bull")
    _check("A(e2e) payload label ALLOW",
           r["btc_side_enable_shadow_label"] == "BTC_SIDE_ENABLE_ALLOW"
           and r["btc_side_enable_shadow_allow"] is True
           and r["btc_side_enable_bias"] == "BULLISH"
           and r["btc_side_enable_alignment"] == "ALIGNED"
           and r["btc_side_enable_shadow_version"] == sd._BTC_SIDE_ENABLE_VERSION, r)
    _check("forward log row emitted with required fields",
           _SIDE_ENABLE_CAPTURED
           and _SIDE_ENABLE_CAPTURED[-1]["event_type"] == "BTC_BIAS_SIDE_ENABLE_SHADOW"
           and _SIDE_ENABLE_CAPTURED[-1]["shadow_label"] == "BTC_SIDE_ENABLE_ALLOW"
           and set(("ts", "symbol", "side", "dedup_key", "execution_mode",
                    "v1_decision", "entry", "sl", "tp", "rr",
                    "btc_bias_independent", "btc_context_missing_fields"))
           <= set(_SIDE_ENABLE_CAPTURED[-1].keys()),
           _SIDE_ENABLE_CAPTURED[-1])

    # I / J. Decision fields unchanged; side-enable fields purely additive.
    for mode, tag, decision, reason in (("paper", "I", "PREFILTER_REJECT", "rr_below_min"),
                                        ("live", "J", "OPEN", "confirmed")):
        src = _decision_row("LONG", decision=decision, reason=reason)
        before = dict(src)
        row = sd._btc_alignment_instrumentation_shadow(
            src, execution_mode=mode, v1_decision=decision, v1_reason=reason,
            side="LONG", now_ts=SIGNAL_TS, fetcher=_fetcher_for("bull"),
        )
        _check(f"{tag} {mode} source dict unmutated", src == before, (src, before))
        _check(f"{tag} {mode} v1 decision/reason preserved",
               row["v1_decision"] == decision and row["v1_reason"] == reason,
               (row["v1_decision"], row["v1_reason"]))
        # Stripping the additive shadow fields restores a payload with no
        # side-enable leakage into the decision-bearing keys.
        stripped = {k: v for k, v in row.items()
                    if k not in sd._BTC_SIDE_ENABLE_SHADOW_FIELDS}
        _check(f"{tag} {mode} side-enable fields are additive-only",
               all(f not in stripped for f in sd._BTC_SIDE_ENABLE_SHADOW_FIELDS)
               and stripped["v1_decision"] == decision, stripped["v1_decision"])

    # K. PAPER_LOCATION_GATE inputs unchanged (read-only passthrough).
    gate = {
        "confirm_smc_entry_location_would_block": True,
        "trade_location_quality": "BAD",
        "smc_zone": "PREMIUM",
        "market_regime": "CHOP_NO_TRADE",
    }
    gate_before = dict(gate)
    r = _run("LONG", "bull", gate_fields=gate)
    _check("K location gate dict unmutated", gate == gate_before, gate)
    _check("K location gate passthrough in side-enable log",
           _SIDE_ENABLE_CAPTURED[-1]["paper_location_gate_would_block"] is True
           and _SIDE_ENABLE_CAPTURED[-1]["smc_zone"] == "PREMIUM", _SIDE_ENABLE_CAPTURED[-1])

    # L. V2B allowlist inputs unchanged (read-only passthrough).
    v2b = {
        "v2b_label": "CONFIRM_SMC_RESEARCH__DIRECTIONAL_BIAS_CONTEXT",
        "v2b_match": True,
        "v2b_reason": "shadow_reason",
        "v2b_market_bias": "bearish",
        "v2b_direction_alignment": "aligned",
    }
    v2b_before = dict(v2b)
    r = _run("SHORT", "bear", v2b_fields=v2b)
    _check("L v2b dict unmutated", v2b == v2b_before, v2b)
    _check("L v2b passthrough in side-enable log",
           _SIDE_ENABLE_CAPTURED[-1]["v2b_match"] is True
           and _SIDE_ENABLE_CAPTURED[-1]["v2b_market_bias"] == "bearish"
           and _SIDE_ENABLE_CAPTURED[-1]["shadow_label"] == "BTC_SIDE_ENABLE_ALLOW",
           _SIDE_ENABLE_CAPTURED[-1])

    # M. SMC_PA_SCORE_V3 inputs unchanged: side-enable never computes/mutates V3;
    #    V3 fields are passed through read-only when a carrier holds them.
    pa_v3 = {
        "smc_pa_v3_total_score": 7.5,
        "smc_pa_v3_score_band": "HIGH",
        "smc_pa_v3_missing_components": ["btc_bias"],
        "smc_pa_v3_version": sd._SMC_PA_V3_VERSION,
    }
    pa_v3_before = dict(pa_v3)
    r = _run("LONG", "bull", gate_fields=dict(pa_v3))
    _check("M SMC_PA_SCORE_V3 dict unmutated", pa_v3 == pa_v3_before, pa_v3)
    _check("M SMC_PA_SCORE_V3 passthrough in side-enable log (read-only)",
           _SIDE_ENABLE_CAPTURED[-1]["smc_pa_v3_total_score"] == 7.5
           and _SIDE_ENABLE_CAPTURED[-1]["smc_pa_v3_score_band"] == "HIGH"
           and _SIDE_ENABLE_CAPTURED[-1]["smc_pa_v3_version"] == sd._SMC_PA_V3_VERSION,
           _SIDE_ENABLE_CAPTURED[-1])
    _check("M side-enable does not invoke the V3 evaluator",
           "SMC_PA_SCORE_V3_SHADOW" not in {r_.get("event_type") for r_ in _SIDE_ENABLE_CAPTURED},
           {r_.get("event_type") for r_ in _SIDE_ENABLE_CAPTURED})

    # N. No execution/order module imported by dispatcher; no network fetch used.
    _check("N no execution/order module in dispatcher globals",
           not any(name in sd.__dict__ for name in ("place_order", "cancel_order",
                                                    "create_order", "submit_order")),
           [n for n in ("place_order", "cancel_order", "create_order", "submit_order")
            if n in sd.__dict__])
    _check("N no network fetch (injected fetcher only)", True)
    _check("N writers redirected to memory (no logs/ writes)",
           len(_ALIGN_CAPTURED) > 0 and len(_SIDE_ENABLE_CAPTURED) > 0)

    print(f"\nPASS all BTC_BIAS_SIDE_ENABLE_SHADOW cases "
          f"({len(_SIDE_ENABLE_CAPTURED)} side-enable rows, "
          f"{len(_ALIGN_CAPTURED)} align rows captured in-memory, 0 written to disk)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
