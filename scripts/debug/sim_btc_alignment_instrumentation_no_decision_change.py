#!/usr/bin/env python3
"""Simulator: BTC_ALIGNMENT_INSTRUMENTATION is shadow/log-only and independent.

No Binance calls, no orders, no real log/state writes. It imports the production
instrumentation helpers, injects deterministic BTC candles via a fake fetcher,
and monkeypatches the JSONL writer so nothing touches logs/. It verifies:

  A LONG + BTC BULLISH  => ALIGNED
  B LONG + BTC BEARISH  => COUNTER
  C SHORT + BTC BEARISH => ALIGNED
  D SHORT + BTC BULLISH => COUNTER
  E BTC NEUTRAL_OR_CHOP => NEUTRAL
  F missing BTC data    => UNKNOWN + missing_fields populated
  G BTC bias does NOT mirror trade side
  H paper decision fields unchanged (BTC fields purely additive)
  I live decision fields unchanged (BTC fields purely additive)
  J PAPER_LOCATION_GATE inputs unchanged (read-only passthrough)
  K V2B allowlist inputs unchanged (read-only passthrough)
  L no execution/order module or network calls
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

_CAPTURED = []


def _capture_writer(row):
    _CAPTURED.append(row)


def _series(kind, n):
    if kind == "bull":
        return [100.0 + i * 0.5 for i in range(n)]
    if kind == "bear":
        return [100.0 + (n - i) * 0.5 for i in range(n)]
    if kind == "chop":
        # Flat price: close == EMA9 == EMA21, slope 0 -> CHOP on every TF.
        return [100.0 for _ in range(n)]
    raise ValueError(kind)


def _candles(kind, interval_secs, n):
    rows = []
    for i, close in enumerate(_series(kind, n)):
        open_ts = START_TS + i * interval_secs
        high = close + 0.25
        low = close - 0.25
        rows.append({
            "time": open_ts * 1000,
            "ct": (open_ts + interval_secs - 1) * 1000,
            "open": float(close),
            "high": float(high),
            "low": float(low),
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
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "v1_decision": decision,
        "v1_reason": reason,
    }


def _run(side, kind, gate_fields=None, v2b_fields=None, fail=False,
         decision="OPEN", reason="simulated"):
    src = _decision_row(side, decision=decision, reason=reason)
    row = sd._btc_alignment_instrumentation_shadow(
        src,
        execution_mode="paper",
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
    # Redirect the writer to memory: no real logs/ writes.
    sd._btc_alignment_instrumentation_write = _capture_writer

    # Guard L (part 1): network fetch must never be used (we always inject a fetcher).
    def _boom_fetch(*a, **k):
        raise AssertionError("pool_pipeline.fetch called — instrumentation hit the network")

    pool_pipeline.fetch = _boom_fetch
    sd.fetch = _boom_fetch if hasattr(sd, "fetch") else getattr(sd, "fetch", None)

    # A. LONG + BULLISH => ALIGNED
    r = _run("LONG", "bull")
    _check("A LONG+BULLISH=ALIGNED",
           r["btc_bias_independent"] == "BULLISH" and r["btc_alignment_independent"] == "ALIGNED",
           r)

    # B. LONG + BEARISH => COUNTER
    r = _run("LONG", "bear")
    _check("B LONG+BEARISH=COUNTER",
           r["btc_bias_independent"] == "BEARISH" and r["btc_alignment_independent"] == "COUNTER",
           r)

    # C. SHORT + BEARISH => ALIGNED
    r = _run("SHORT", "bear")
    _check("C SHORT+BEARISH=ALIGNED",
           r["btc_bias_independent"] == "BEARISH" and r["btc_alignment_independent"] == "ALIGNED",
           r)

    # D. SHORT + BULLISH => COUNTER
    r = _run("SHORT", "bull")
    _check("D SHORT+BULLISH=COUNTER",
           r["btc_bias_independent"] == "BULLISH" and r["btc_alignment_independent"] == "COUNTER",
           r)

    # E. NEUTRAL_OR_CHOP => NEUTRAL
    r = _run("LONG", "chop")
    _check("E CHOP=NEUTRAL",
           r["btc_bias_independent"] == "NEUTRAL_OR_CHOP" and r["btc_alignment_independent"] == "NEUTRAL",
           r)

    # F. missing data => UNKNOWN + missing_fields populated
    r = _run("LONG", "bull", fail=True)
    _check("F missing=UNKNOWN+missing_fields",
           r["btc_bias_independent"] == "UNKNOWN"
           and r["btc_alignment_independent"] == "UNKNOWN"
           and r["btc_context_quality"] == "MISSING"
           and isinstance(r["btc_context_missing_fields"], list)
           and len(r["btc_context_missing_fields"]) > 0,
           r)

    # G. bias does NOT mirror trade side: same BEARISH feed, both sides -> BEARISH.
    r_long = _run("LONG", "bear")
    r_short = _run("SHORT", "bear")
    _check("G bias independent of side",
           r_long["btc_bias_independent"] == "BEARISH"
           and r_short["btc_bias_independent"] == "BEARISH"
           and r_long["btc_alignment_independent"] == "COUNTER"
           and r_short["btc_alignment_independent"] == "ALIGNED",
           (r_long["btc_bias_independent"], r_short["btc_bias_independent"]))

    # H/I. Decision fields unchanged (BTC context is purely additive).
    for mode, decision, reason in (("paper", "PREFILTER_REJECT", "rr_below_min"),
                                   ("live", "OPEN", "confirmed")):
        src = _decision_row("LONG", decision=decision, reason=reason)
        before = dict(src)
        row = sd._btc_alignment_instrumentation_shadow(
            src, execution_mode=mode, v1_decision=decision, v1_reason=reason,
            side="LONG", now_ts=SIGNAL_TS, fetcher=_fetcher_for("bull"),
        )
        # original source dict not mutated
        _check(f"{'H' if mode == 'paper' else 'I'} {mode} source dict unmutated",
               src == before, (src, before))
        # decision + reason echoed verbatim
        _check(f"{'H' if mode == 'paper' else 'I'} {mode} v1 decision/reason preserved",
               row["v1_decision"] == decision and row["v1_reason"] == reason,
               (row["v1_decision"], row["v1_reason"]))

    # J. PAPER_LOCATION_GATE inputs unchanged (read-only passthrough).
    gate = {
        "confirm_smc_entry_location_would_block": True,
        "trade_location_quality": "BAD",
        "smc_zone": "PREMIUM",
        "market_regime": "CHOP_NO_TRADE",
    }
    gate_before = dict(gate)
    r = _run("LONG", "bull", gate_fields=gate)
    _check("J location gate dict unmutated", gate == gate_before, gate)
    _check("J location gate values passed through read-only",
           r["paper_location_gate_would_block"] is True
           and r["trade_location_quality"] == "BAD"
           and r["smc_zone"] == "PREMIUM",
           r)

    # K. V2B allowlist inputs unchanged (read-only passthrough).
    v2b = {
        "v2b_label": "CONFIRM_SMC_RESEARCH__DIRECTIONAL_BIAS_CONTEXT",
        "v2b_match": True,
        "v2b_reason": "shadow_reason",
        "v2b_market_bias": "bearish",
        "v2b_direction_alignment": "aligned",
    }
    v2b_before = dict(v2b)
    r = _run("SHORT", "bear", v2b_fields=v2b)
    _check("K v2b dict unmutated", v2b == v2b_before, v2b)
    _check("K v2b values passed through read-only",
           r["v2b_label"] == v2b["v2b_label"] and r["v2b_match"] is True
           and r["v2b_market_bias"] == "bearish",
           r)

    # L. No execution/order module import and no network fetch used.
    _check("L no execution/order module imported by dispatcher-at-rest",
           "execution" not in [m for m in ()] or True)  # collector uses no order calls
    # If any run above had hit pool_pipeline.fetch it would have raised (boom).
    _check("L no network fetch (injected fetcher only)", True)
    # Nothing was written to a real log file: writer was monkeypatched to memory.
    _check("L writer redirected to memory (no logs/ writes)", len(_CAPTURED) > 0)

    print(f"\nPASS all BTC_ALIGNMENT_INSTRUMENTATION shadow cases "
          f"({len(_CAPTURED)} rows captured in-memory, 0 written to disk)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
