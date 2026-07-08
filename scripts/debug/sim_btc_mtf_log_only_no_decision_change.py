#!/usr/bin/env python3
"""Simulator for BTC MTF LOG_ONLY freshness/wiring.

No Binance calls, no orders, no log/state writes. The simulator imports the
production helper and injects deterministic BTC candles with a fake fetcher.
"""

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from signal_dispatcher import _BTC_MTF_CONTEXT_FIELDS, _btc_mtf_context_for_signal

START_TS = 1_000_000
SIGNAL_TS = START_TS + 40 * 3600 - 1


def _series(kind, n):
    if kind == "bull":
        return [100.0 + i * 0.35 for i in range(n)]
    if kind == "bear":
        return [120.0 - i * 0.35 for i in range(n)]
    if kind == "chop":
        return [100.0 + (0.005 if i % 2 else -0.005) for i in range(n)]
    raise ValueError(kind)


def _candles(kind, interval_secs, n):
    rows = []
    for i, close in enumerate(_series(kind, n)):
        open_ts = START_TS + i * interval_secs
        rows.append({
            "time": open_ts * 1000,
            "ct": (open_ts + interval_secs - 1) * 1000,
            "close": float(close),
        })
    return rows


def _fetcher_for(kinds=None, fail=False):
    kinds = kinds or {"5m": "bull", "15m": "bull", "1h": "bull"}
    data = {
        "5m": _candles(kinds["5m"], 300, 520),
        "15m": _candles(kinds["15m"], 900, 180),
        "1h": _candles(kinds["1h"], 3600, 41),
    }

    def fetcher(symbol, interval, is_priority=False):
        if fail:
            raise RuntimeError("simulated fetch failure")
        return data.get(interval)

    return fetcher, data


def _decision(decision="OPEN_ATTEMPT", reason="simulated", signal_ts=SIGNAL_TS):
    row = {
        "decision": decision,
        "reason": reason,
        "symbol": "ETHUSDT",
        "side": "LONG",
        "entry_type": "CONFIRM_SMC_RESEARCH",
    }
    if signal_ts is not None:
        row["signal_created_ts"] = signal_ts
    return row


def _with_btc(row, ctx):
    out = dict(row)
    out.update(ctx)
    return out


def _assert_decision_unchanged(before, after):
    stripped = {k: v for k, v in after.items() if k not in _BTC_MTF_CONTEXT_FIELDS}
    assert stripped == before, f"decision changed: before={before} after_stripped={stripped}"


def _case(name, row, expected_mode, expected_unknown, fetcher):
    ctx = _btc_mtf_context_for_signal(row, fetcher=fetcher, now_ts=row.get("signal_created_ts"))
    _assert_decision_unchanged(row, _with_btc(row, ctx))
    assert ctx["btc_data_mode"] == expected_mode, f"{name}: mode {ctx}"
    assert ctx["btc_unknown_reason"] == expected_unknown, f"{name}: unknown {ctx}"
    print(f"PASS {name}: mode={expected_mode} unknown={expected_unknown} decision unchanged")
    return ctx


def main():
    fresh_fetcher, fresh_data = _fetcher_for()

    # A. Fresh BTC context => INDEPENDENT_BTC_MTF, decision unchanged.
    ctx = _case(
        "A fresh BTC context",
        _decision(),
        "INDEPENDENT_BTC_MTF",
        "NONE",
        fresh_fetcher,
    )
    assert ctx["btc_context_available"] is True
    assert ctx["btc_mtf_alignment"] == "ALL_ALIGNED"

    # B. Stale BTC context > max age => BTC_CONTEXT_STALE, decision unchanged.
    stale_row = _decision(signal_ts=SIGNAL_TS + 7201)
    _case(
        "B stale BTC context",
        stale_row,
        "BTC_CONTEXT_STALE",
        "BTC_SNAPSHOT_TOO_STALE",
        fresh_fetcher,
    )

    # C. Fetch error => BTC_CONTEXT_FETCH_ERROR, decision unchanged.
    failing_fetcher, _ = _fetcher_for(fail=True)
    _case(
        "C fetch error",
        _decision(),
        "BTC_CONTEXT_FETCH_ERROR",
        "BTC_CONTEXT_FETCH_ERROR",
        failing_fetcher,
    )

    # D. Missing entry ts => ENTRY_TS_MISSING, decision unchanged.
    _case(
        "D missing entry ts",
        _decision(signal_ts=None),
        "NO_INDEPENDENT_BTC_MTF_DATA",
        "ENTRY_TS_MISSING",
        fresh_fetcher,
    )

    # E/F. Decision labels remain unchanged except extra btc fields.
    _case(
        "E A3 paper-red WARN_ALLOW",
        _decision(decision="WARN_ALLOW", reason="paper_red_warn_only"),
        "INDEPENDENT_BTC_MTF",
        "NONE",
        fresh_fetcher,
    )
    _case(
        "F rr_below_min",
        _decision(decision="PREFILTER_REJECT", reason="rr_below_min"),
        "INDEPENDENT_BTC_MTF",
        "NONE",
        fresh_fetcher,
    )
    _case(
        "F research_predicate_fail",
        _decision(decision="PREFILTER_REJECT", reason="research_predicate_fail"),
        "INDEPENDENT_BTC_MTF",
        "NONE",
        fresh_fetcher,
    )

    # G. No future BTC candle is used.
    future_open = SIGNAL_TS + 1
    fresh_data["1h"].append({
        "time": future_open * 1000,
        "ct": (future_open + 3599) * 1000,
        "close": 50.0,
    })
    ctx = _case(
        "G no future BTC candle",
        _decision(),
        "INDEPENDENT_BTC_MTF",
        "NONE",
        fresh_fetcher,
    )
    assert ctx["btc_context_source_ts"] <= SIGNAL_TS
    assert ctx["btc_h1_trend"] == "BULLISH"

    print("PASS all BTC MTF LOG_ONLY freshness/wiring cases")
    return 0


if __name__ == "__main__":
    sys.exit(main())
