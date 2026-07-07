#!/usr/bin/env python3
"""Audit BTC trend alignment vs research-trade outcomes (READ-ONLY).

Joins the existing log-only BTC bias shadow snapshots
(logs/paper_smc_research_btc_m15_bias_shadow.jsonl and
 logs/paper_smc_research_btc_mtf_bias_shadow.jsonl) against realized
RESEARCH_CLOSED outcomes (logs/paper_smc_research_lifecycle.jsonl) on
research_dedup_key, and reports win-rate / PF / net-R / avg-R by BTC
alignment bucket.

This script does NOT change any trade decision, does NOT write to any
production log, and uses ONLY entry-time BTC context (the shadow snapshot
is captured at open time). It is a pure offline audit.

Buckets: ALIGNED / COUNTER / NEUTRAL / UNKNOWN (from btc_m15_alignment_label).
"""

import json
import os
import sys

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))), "logs")

M15_SHADOW = os.path.join(LOG_DIR, "paper_smc_research_btc_m15_bias_shadow.jsonl")
MTF_SHADOW = os.path.join(LOG_DIR, "paper_smc_research_btc_mtf_bias_shadow.jsonl")
LIFECYCLE = os.path.join(LOG_DIR, "paper_smc_research_lifecycle.jsonl")
LIVE_TRADES = os.path.join(os.path.dirname(LOG_DIR), "live_trades.csv")

ALIGN_FROM_LABEL = {
    "BTC_BIAS_ALIGNED": "ALIGNED",
    "BTC_BIAS_COUNTER": "COUNTER",
    "BTC_BIAS_NEUTRAL": "NEUTRAL",
    "BTC_BIAS_UNKNOWN": "UNKNOWN",
}


def _read_jsonl(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_btc_alignment():
    """research_dedup_key -> dict(alignment, bias_label, source, mtf_summary)."""
    out = {}
    for row in _read_jsonl(M15_SHADOW):
        key = row.get("research_dedup_key") or row.get("research_join_key")
        if not key:
            continue
        out[key] = {
            "alignment": ALIGN_FROM_LABEL.get(
                row.get("btc_m15_alignment_label", "BTC_BIAS_UNKNOWN"), "UNKNOWN"
            ),
            "bias_label": row.get("btc_m15_bias_label"),
            "source": row.get("btc_m15_source"),
            "side": row.get("side"),
            "mtf_summary": None,
        }
    for row in _read_jsonl(MTF_SHADOW):
        key = row.get("research_dedup_key") or row.get("research_join_key")
        if not key or key not in out:
            continue
        out[key]["mtf_summary"] = row.get("btc_mtf_summary_label")
    return out


def _load_closed():
    """List of closed research trades with realized R and join key (chronological)."""
    closed = []
    for row in _read_jsonl(LIFECYCLE):
        if str(row.get("event_type")) != "RESEARCH_CLOSED":
            continue
        key = row.get("research_dedup_key")
        r_mult = _safe_float(row.get("r_multiple"))
        if not key or r_mult is None:
            continue
        closed.append({
            "key": key,
            "r": r_mult,
            "side": str(row.get("side") or "").upper(),
            "epoch": row.get("research_epoch"),
            "is_post_50": bool(row.get("research_is_post_50")),
        })
    return closed


def _bucket_stats(trades):
    """Aggregate stats for a list of {r, side} trades."""
    n = len(trades)
    if n == 0:
        return {"n": 0, "net_r": 0.0, "avg_r": None, "wr": None, "pf": None,
                "wins": 0, "losses": 0, "be": 0}
    net_r = sum(t["r"] for t in trades)
    wins = [t["r"] for t in trades if t["r"] > 1e-9]
    losses = [t["r"] for t in trades if t["r"] < -1e-9]
    be = [t for t in trades if abs(t["r"]) <= 1e-9]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    pf = (gross_win / gross_loss) if gross_loss > 1e-9 else (
        float("inf") if gross_win > 0 else None)
    return {
        "n": n,
        "net_r": net_r,
        "avg_r": net_r / n,
        "wr": len(wins) / n,
        "pf": pf,
        "wins": len(wins),
        "losses": len(losses),
        "be": len(be),
    }


def _fmt(stats):
    def f(v, p=2):
        if v is None:
            return "n/a"
        if v == float("inf"):
            return "inf"
        return f"{v:.{p}f}"
    return (f"n={stats['n']:>3}  net_r={f(stats['net_r']):>7}  "
            f"avg_r={f(stats['avg_r']):>6}  wr={f(stats['wr'])}  "
            f"pf={f(stats['pf'])}  (W{stats['wins']}/L{stats['losses']}/BE{stats['be']})")


def _report_buckets(title, joined):
    print(f"\n=== {title} (n={len(joined)}) ===")
    order = ["ALIGNED", "NEUTRAL", "COUNTER", "UNKNOWN"]
    by_bucket = {b: [] for b in order}
    for t in joined:
        by_bucket.setdefault(t["alignment"], []).append(t)
    for b in order:
        print(f"  {b:<8} {_fmt(_bucket_stats(by_bucket[b]))}")
    # side split inside known-direction buckets
    print("  -- side split (ALIGNED/COUNTER only) --")
    for b in ("ALIGNED", "COUNTER"):
        for side in ("LONG", "SHORT"):
            sub = [t for t in by_bucket[b] if t["side"] == side]
            if sub:
                print(f"    {b:<8} {side:<5} {_fmt(_bucket_stats(sub))}")
    return by_bucket


def main():
    btc = _load_btc_alignment()
    closed = _load_closed()

    joined = []
    for t in closed:
        ctx = btc.get(t["key"])
        if ctx is None:
            joined.append({**t, "alignment": "UNKNOWN", "btc_source": "NO_SHADOW"})
        else:
            joined.append({**t, "alignment": ctx["alignment"],
                           "btc_source": ctx["source"]})

    print("=" * 72)
    print("BTC ALIGNMENT vs RESEARCH OUTCOME AUDIT (READ-ONLY, entry-time context)")
    print("=" * 72)
    print(f"closed research trades w/ realized R : {len(closed)}")
    print(f"closed trades joined to BTC shadow   : "
          f"{sum(1 for t in joined if t['btc_source'] not in ('NO_SHADOW', 'NONE', 'STALE'))}")
    print(f"closed trades w/ NO usable BTC ctx   : "
          f"{sum(1 for t in joined if t['btc_source'] in ('NO_SHADOW', 'NONE', 'STALE'))}")

    # all paper research active window
    _report_buckets("ALL PAPER RESEARCH (all joined)", joined)

    # last50 active paper (chronological tail)
    _report_buckets("LAST 50 PAPER RESEARCH (chronological tail)", joined[-50:])

    # live: BTC context availability
    print("\n=== LIVE CONFIRMED RESEARCH TRADES ===")
    live_n = 0
    if os.path.exists(LIVE_TRADES):
        import csv
        with open(LIVE_TRADES, newline="") as fh:
            live_rows = [r for r in csv.DictReader(fh)
                         if str(r.get("entry_type")) == "CONFIRM_SMC_RESEARCH"]
        live_n = len(live_rows)
        live_trades = []
        for r in live_rows:
            rr = _safe_float(r.get("rr"))
            st = str(r.get("status") or "").upper()
            # realized R sign from status when rr magnitude present
            if rr is None:
                continue
            if st == "LOSE":
                rr = -abs(rr) if rr > 0 else rr
            elif st == "BE":
                rr = 0.0
            live_trades.append({"r": rr, "side": str(r.get("side") or "").upper()})
        print(f"  live closed research trades         : {live_n}")
        print(f"  overall                             : {_fmt(_bucket_stats(live_trades))}")
    print("  BTC alignment buckets               : NOT AVAILABLE "
          "(BTC bias shadow is captured for PAPER opens only; live_trades.csv "
          "and live_smc_research_decisions.jsonl carry NO btc_* fields)")

    # Recommendation logic
    print("\n" + "=" * 72)
    print("RECOMMENDATION")
    print("=" * 72)
    known = [t for t in joined if t["alignment"] in ("ALIGNED", "COUNTER")]
    counter = [t for t in joined if t["alignment"] == "COUNTER"]
    aligned = [t for t in joined if t["alignment"] == "ALIGNED"]
    unknown_share = (sum(1 for t in joined if t["alignment"] == "UNKNOWN")
                     / max(1, len(joined)))
    rec = "LOG_ONLY"
    reasons = []
    reasons.append(f"directional(ALIGNED+COUNTER) sample = {len(known)} "
                   f"(ALIGNED={len(aligned)}, COUNTER={len(counter)})")
    reasons.append(f"UNKNOWN share = {unknown_share:.0%}")
    reasons.append("live side has ZERO BTC context -> cannot validate on live")
    if len(counter) < 20 or len(aligned) < 20:
        rec = "NO_EDGE / LOG_ONLY"
        reasons.append("COUNTER and/or ALIGNED bucket < 20 -> statistically "
                       "insufficient for WARN/HARD_FILTER")
    print(f"  recommended BTC action: {rec}")
    for r in reasons:
        print(f"   - {r}")
    print("\nNOTE: M15 and MTF shadows use the SAME unified router bias "
          "(btc_mtf_data_mode=UNIFIED_ROUTER_BIAS_NOT_INDEPENDENT_TF); MTF "
          "adds no independent timeframe signal over M15.")


if __name__ == "__main__":
    sys.exit(main())
