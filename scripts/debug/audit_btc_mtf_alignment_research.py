#!/usr/bin/env python3
"""Read-only audit for BTC M5/M15/H1 LOG_ONLY research instrumentation."""

import csv
import json
import os
import sys
from collections import Counter, defaultdict

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_DIR = os.path.join(REPO_ROOT, "logs")

PAPER_ENTRY_CONTEXT = os.path.join(LOG_DIR, "paper_smc_research_entry_context.jsonl")
PAPER_CONFIRM_CONTEXT = os.path.join(LOG_DIR, "paper_confirm_entry_context.jsonl")
LIVE_DECISIONS = os.path.join(LOG_DIR, "live_smc_research_decisions.jsonl")
LIFECYCLE = os.path.join(LOG_DIR, "paper_smc_research_lifecycle.jsonl")
MIN_LOCK_075 = os.path.join(LOG_DIR, "paper_smc_research_min_lock_075_events.jsonl")
LIVE_TRADES = os.path.join(REPO_ROOT, "live_trades.csv")

BTC_FIELDS = (
    "btc_context_available",
    "btc_m5_trend",
    "btc_m15_trend",
    "btc_h1_trend",
    "btc_m5_momentum",
    "btc_m15_momentum",
    "btc_h1_momentum",
    "btc_mtf_alignment",
    "btc_alignment_reason",
    "btc_data_mode",
    "btc_context_source_ts",
    "btc_context_age_sec",
    "btc_unknown_reason",
)
ALIGNMENT_BUCKETS = (
    "ALL_ALIGNED",
    "HTF_ALIGNED_LTF_COUNTER",
    "LTF_ALIGNED_HTF_COUNTER",
    "COUNTER_HTF",
    "BTC_CHOP",
    "MIXED",
    "UNKNOWN",
)


def _read_jsonl(path):
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        return []
    return rows


def _safe_float(value):
    try:
        if value in (None, ""):
            return None
        out = float(value)
        if out != out:
            return None
        return out
    except (TypeError, ValueError):
        return None


def _context_from_row(row):
    nested = row.get("entry_context") if isinstance(row.get("entry_context"), dict) else {}
    ctx = {}
    for field in BTC_FIELDS:
        value = row.get(field)
        if value in (None, ""):
            value = nested.get(field)
        ctx[field] = value
    if not ctx.get("btc_mtf_alignment"):
        ctx["btc_mtf_alignment"] = "UNKNOWN"
    if not ctx.get("btc_data_mode"):
        ctx["btc_data_mode"] = "NO_INDEPENDENT_BTC_MTF_DATA"
    if not ctx.get("btc_unknown_reason"):
        if ctx.get("btc_data_mode") == "INDEPENDENT_BTC_MTF":
            ctx["btc_unknown_reason"] = "NONE"
        elif ctx.get("btc_data_mode") == "BTC_CONTEXT_STALE":
            ctx["btc_unknown_reason"] = "BTC_SNAPSHOT_TOO_STALE"
        elif ctx.get("btc_data_mode") == "BTC_CONTEXT_FETCH_ERROR":
            ctx["btc_unknown_reason"] = "BTC_CONTEXT_FETCH_ERROR"
        else:
            ctx["btc_unknown_reason"] = "NO_INDEPENDENT_BTC_MTF_DATA"
    return ctx


def _key(row):
    for field in ("research_dedup_key", "research_join_key", "dedup_key"):
        value = row.get(field)
        if value not in (None, ""):
            return str(value)
    return None


def _load_contexts():
    by_key = {}
    all_rows = []
    for path in (PAPER_ENTRY_CONTEXT, PAPER_CONFIRM_CONTEXT, LIVE_DECISIONS):
        for row in _read_jsonl(path):
            ctx = _context_from_row(row)
            record = {"row": row, "ctx": ctx, "source": os.path.basename(path)}
            all_rows.append(record)
            key = _key(row)
            if key:
                by_key[key] = ctx
    return by_key, all_rows


def _load_paper_closed(contexts):
    out = []
    for row in _read_jsonl(LIFECYCLE):
        if str(row.get("event_type") or "") != "RESEARCH_CLOSED":
            continue
        key = row.get("research_dedup_key")
        r_mult = _safe_float(row.get("r_multiple"))
        if not key or r_mult is None:
            continue
        out.append({
            "key": str(key),
            "r": r_mult,
            "side": str(row.get("side") or "").upper(),
            "entry_ts": _safe_float(row.get("entry_ts") or row.get("open_ts")),
            "ctx": contexts.get(str(key), _context_from_row({})),
        })
    return out


def _live_r_from_row(row):
    for field in ("r_multiple", "net_r", "realized_r", "rr"):
        value = _safe_float(row.get(field))
        if value is not None:
            status = str(row.get("status") or "").upper()
            if status in {"LOSE", "LOSS", "SL"}:
                return -abs(value)
            if status in {"BE", "BREAKEVEN"}:
                return 0.0
            return value
    return None


def _load_live_confirmed(contexts):
    out = []
    try:
        with open(LIVE_TRADES, "r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                if str(row.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
                    continue
                r_mult = _live_r_from_row(row)
                if r_mult is None:
                    continue
                key = _key(row)
                out.append({
                    "key": key,
                    "r": r_mult,
                    "side": str(row.get("side") or "").upper(),
                    "entry_ts": _safe_float(row.get("signal_created_ts") or row.get("time")),
                    "ctx": contexts.get(str(key), _context_from_row({})) if key else _context_from_row({}),
                })
    except FileNotFoundError:
        return []
    return out


def _post_min_lock_boundary():
    vals = []
    for row in _read_jsonl(MIN_LOCK_075):
        ts = _safe_float(row.get("ts") or row.get("timestamp_unix"))
        if ts is not None:
            vals.append(ts)
    return min(vals) if vals else None


def _stats(rows):
    n = len(rows)
    if n == 0:
        return {"n": 0, "net": 0.0, "avg": None, "wr": None, "pf": None}
    values = [r["r"] for r in rows]
    wins = [v for v in values if v > 0]
    losses = [v for v in values if v < 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    pf = gross_win / gross_loss if gross_loss > 0 else (float("inf") if gross_win > 0 else None)
    return {
        "n": n,
        "net": sum(values),
        "avg": sum(values) / n,
        "wr": len(wins) / n,
        "pf": pf,
    }


def _fmt(value, digits=2):
    if value is None:
        return "n/a"
    if value == float("inf"):
        return "inf"
    return f"{value:.{digits}f}"


def _print_bucket_table(title, rows):
    print(f"\n{title}")
    print("bucket                         n     net R    avg R   win rate      PF")
    for bucket in ALIGNMENT_BUCKETS:
        subset = [r for r in rows if str(r["ctx"].get("btc_mtf_alignment") or "UNKNOWN") == bucket]
        st = _stats(subset)
        sample_note = " diagnostic" if 0 < st["n"] < 20 else (" no-hard-filter" if st["n"] < 50 and st["n"] > 0 else "")
        print(
            f"{bucket:<28} {st['n']:>3}  {_fmt(st['net']):>8}  "
            f"{_fmt(st['avg']):>7}  {_fmt(st['wr']):>9}  {_fmt(st['pf']):>6}{sample_note}"
        )


def _coverage(rows):
    if not rows:
        return 0.0
    return sum(1 for r in rows if r["ctx"].get("btc_context_available") is True) / len(rows)


def _recommendation(paper_rows):
    known = [r for r in paper_rows if r["ctx"].get("btc_mtf_alignment") not in (None, "UNKNOWN")]
    if len(known) < 20:
        return "NO_EDGE"
    if len(known) < 50:
        return "LOG_ONLY_MTF_BTC_CONTEXT"
    all_rows = _stats(known)
    aligned = _stats([r for r in known if r["ctx"].get("btc_mtf_alignment") == "ALL_ALIGNED"])
    counter = _stats([r for r in known if r["ctx"].get("btc_mtf_alignment") == "COUNTER_HTF"])
    if aligned["n"] >= 50 and all_rows["pf"] and aligned["pf"] and aligned["pf"] > all_rows["pf"] * 1.2 and aligned["net"] > all_rows["net"]:
        return "HARD_FILTER"
    if counter["n"] >= 20 and counter["net"] < 0:
        return "WARN_ONLY_BTC_AGAINST_HTF"
    return "NO_EDGE"


def main():
    contexts, context_rows = _load_contexts()
    paper = _load_paper_closed(contexts)
    live = _load_live_confirmed(contexts)
    boundary = _post_min_lock_boundary()
    last50 = sorted(paper, key=lambda r: (r["entry_ts"] is None, r["entry_ts"] or 0.0))[-50:]
    post_lock = [
        r for r in paper
        if boundary is not None and r["entry_ts"] is not None and r["entry_ts"] >= boundary
    ]

    mode_counts = Counter(r["ctx"].get("btc_data_mode") or "NO_INDEPENDENT_BTC_MTF_DATA" for r in context_rows)
    trend_counts = {tf: Counter(r["ctx"].get(f"btc_{tf}_trend") or "UNKNOWN" for r in context_rows) for tf in ("m5", "m15", "h1")}
    momentum_counts = {tf: Counter(r["ctx"].get(f"btc_{tf}_momentum") or "UNKNOWN" for r in context_rows) for tf in ("m5", "m15", "h1")}
    alignment_counts = Counter(r["ctx"].get("btc_mtf_alignment") or "UNKNOWN" for r in context_rows)
    unknown_counts = Counter(r["ctx"].get("btc_unknown_reason") or "NO_INDEPENDENT_BTC_MTF_DATA" for r in context_rows)
    independent_rows = mode_counts.get("INDEPENDENT_BTC_MTF", 0)

    print("PASS/WARN/FAIL: " + ("PASS" if independent_rows else "WARN"))
    print("BTC M5/M15/H1 source: pool_pipeline.fetch('BTCUSDT', '5m'/'15m'/'1h') production log fields")
    print("Independent per-TF data, not unified router bias: " + ("YES" if independent_rows else "NO HISTORICAL COVERAGE YET"))
    print("Trend formula: BULLISH close > EMA9 > EMA21 and EMA9 3-bar slope > 0; BEARISH inverse; else CHOP.")
    print("Momentum formula: latest closed-candle return > 0.02% UP, < -0.02% DOWN, otherwise FLAT.")
    print(f"Context rows read: {len(context_rows)}")
    print(f"btc_data_mode counts: {dict(mode_counts)}")
    print(f"btc_unknown_reason counts: {dict(unknown_counts)}")
    print(f"Independent BTC MTF rows: {independent_rows}")
    print(f"Coverage all active paper research: {_coverage(paper):.1%}")
    print(f"Coverage active last50 paper: {_coverage(last50):.1%}")
    print(f"Coverage post-min-lock era: {_coverage(post_lock):.1%}")
    print(f"Coverage live confirmed trades: {_coverage(live):.1%}")

    print("\nTrend counts")
    for tf in ("m5", "m15", "h1"):
        print(f"btc_{tf}_trend: {dict(trend_counts[tf])}")
    print("\nMomentum counts")
    for tf in ("m5", "m15", "h1"):
        print(f"btc_{tf}_momentum: {dict(momentum_counts[tf])}")
    print(f"\nAlignment bucket counts: {dict(alignment_counts)}")

    _print_bucket_table("all active paper research", paper)
    _print_bucket_table("active last50 paper", last50)
    _print_bucket_table("post-min-lock era", post_lock)
    _print_bucket_table("live confirmed trades", live)

    rec = _recommendation(paper)
    print(f"\nRecommendation: {rec}")
    print("Minimum sample rule: n < 20 diagnostic only; n < 50 no hard-filter recommendation.")
    print("Integrity: read-only audit, no live/testnet orders touched, no logs/state/config written.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
