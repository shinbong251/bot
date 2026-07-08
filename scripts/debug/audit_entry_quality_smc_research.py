#!/usr/bin/env python3
"""ENTRY_QUALITY research audit for executed CONFIRM_SMC_RESEARCH trades.

LOG-ONLY / RESEARCH-ONLY. Reads existing logs + trade CSVs, joins entry-time
context to realized outcomes, classifies each executed trade into entry-quality
buckets, and reports performance by bucket. Writes derived research rows to:

    logs/paper_smc_research_entry_quality.jsonl
    logs/live_smc_research_entry_quality.jsonl

This script does NOT change any live decision, predicate, risk, or order. It only
reads history and emits a derived research log + a console report.
"""

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"

PAPER_ENTRY_CONTEXT = LOG_DIR / "paper_smc_research_entry_context.jsonl"
PAPER_TRADES = ROOT / "paper_trades.csv"
LIVE_TRADES = ROOT / "live_trades.csv"

PAPER_OUT = LOG_DIR / "paper_smc_research_entry_quality.jsonl"
LIVE_OUT = LOG_DIR / "live_smc_research_entry_quality.jsonl"

ENTRY_TYPE = "CONFIRM_SMC_RESEARCH"
IMMEDIATE_FAIL_MFE = 0.5  # MFE_R below this before SL == immediate failure


def _f(val):
    try:
        if val in (None, "", "None"):
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


def read_jsonl(path):
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def read_trades(path, entry_type):
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for r in csv.DictReader(handle):
            if r.get("entry_type") == entry_type:
                rows.append(r)
    return rows


def load_paper_context():
    """Map opened_trade_id (str) -> flattened entry_context dict."""
    ctx = {}
    for d in read_jsonl(PAPER_ENTRY_CONTEXT):
        tid = d.get("opened_trade_id")
        if tid is None:
            continue
        ec = d.get("entry_context") or {}
        ctx[str(tid)] = ec
    return ctx


def classify_bucket(ec, side):
    """Assign one entry-quality bucket using ONLY entry-time context fields.

    Priority order is deliberate: location/structure faults first, then timing,
    then acceptable/good. Fields that resolve to UNKNOWN cannot drive a verdict
    and fall through (reported as instrumentation gaps elsewhere).
    """
    smc_zone = (ec.get("smc_zone") or "UNKNOWN").upper()
    regime = (ec.get("market_regime") or "UNKNOWN").upper()
    exhaustion = (ec.get("exhaustion_state") or "UNKNOWN").upper()
    bos_q = (ec.get("bos_quality") or "UNKNOWN").upper()
    dow = (ec.get("dow_trend_context") or "UNKNOWN").upper()
    risk_class = (ec.get("research_entry_timing_risk_class") or "UNKNOWN").upper()
    liq = (ec.get("liquidity_context") or "UNKNOWN").upper()
    side = (side or "").upper()

    # 1. Premium/discount location faults (smc_zone is populated; reliable).
    if side == "LONG" and smc_zone == "PREMIUM":
        return "PREMIUM_LONG_BAD"
    if side == "SHORT" and smc_zone == "DISCOUNT":
        return "DISCOUNT_SHORT_BAD"

    # 2. Against Dow (only when Dow trend is actually resolved, not UNKNOWN).
    if dow in ("UP", "DOWN"):
        with_dow = (side == "LONG" and dow == "UP") or (side == "SHORT" and dow == "DOWN")
        if not with_dow:
            return "AGAINST_DOW"

    # 3. Into opposing liquidity (only when liquidity context is resolved).
    if liq in ("OPPOSING", "INTO_POOL", "OPPOSING_POOL"):
        return "INTO_OPPOSING_LIQUIDITY"

    # 4. Late chase / exhaustion entry.
    if regime == "EXHAUSTION_REVERSAL" or exhaustion in ("EXHAUSTED", "COLLAPSING") \
            or risk_class == "BAD_REGIME_ENTRY":
        return "LATE_CHASE"

    # 5. Chop / range entry.
    if regime in ("CHOP_NO_TRADE", "RANGE_MEAN_REVERSION") or risk_class == "CHOP_OR_RANGE_ENTRY":
        return "MID_TREND_ACCEPTABLE" if bos_q == "STRONG" else "LATE_CHASE"

    # 6. No-followthrough breakout chase.
    if bos_q in ("NO_FOLLOWTHROUGH", "TRAP"):
        return "LATE_CHASE"

    # 7. Clean trend continuation with good structure.
    if bos_q == "STRONG" and exhaustion == "HEALTHY":
        return "EARLY_GOOD_LOCATION"
    if bos_q == "STRONG":
        return "MID_TREND_ACCEPTABLE"

    return "UNKNOWN"


def perf(rows):
    """Aggregate performance stats for a list of joined trade dicts."""
    n = len(rows)
    rr = [r["rr"] for r in rows if r["rr"] is not None]
    mfe = [r["mfe_r"] for r in rows if r["mfe_r"] is not None]
    wins = [r for r in rows if r["status"] == "WIN"]
    gross_win = sum(x for x in rr if x > 0)
    gross_loss = -sum(x for x in rr if x < 0)
    pf = (gross_win / gross_loss) if gross_loss > 0 else None
    imm = [r for r in rows if r["immediate_fail"]]
    side = Counter(r["side"] for r in rows)
    return {
        "n": n,
        "net_r": round(sum(rr), 2) if rr else 0.0,
        "avg_r": round(sum(rr) / len(rr), 3) if rr else None,
        "win_rate": round(len(wins) / n, 3) if n else None,
        "pf": round(pf, 2) if pf is not None else None,
        "mfe_avg": round(sum(mfe) / len(mfe), 3) if mfe else None,
        "immediate_fail_rate": round(len(imm) / n, 3) if n else None,
        "long": side.get("LONG", 0),
        "short": side.get("SHORT", 0),
    }


def join_paper():
    ctx = load_paper_context()
    trades = read_trades(PAPER_TRADES, ENTRY_TYPE)
    joined = []
    missing_ctx = 0
    for t in trades:
        tid = str(t.get("id"))
        ec = ctx.get(tid)
        if ec is None:
            missing_ctx += 1
            ec = {}
        rr = _f(t.get("rr"))
        mfe = _f(t.get("max_r"))
        immediate_fail = (mfe is not None and mfe < IMMEDIATE_FAIL_MFE)
        row = {
            "trade_id": tid,
            "venue": "paper",
            "symbol": t.get("symbol"),
            "side": (t.get("side") or "").upper(),
            "status": t.get("status"),
            "exit_type": t.get("exit_type"),
            "rr": rr,
            "mfe_r": mfe,
            "time_to_1r": _f(t.get("time_to_1r")),
            "trade_age_minutes": _f(t.get("trade_age_minutes")),
            "giveback_r": _f(t.get("giveback_r")),
            "immediate_fail": immediate_fail,
            "smc_zone": ec.get("smc_zone"),
            "market_regime": ec.get("market_regime"),
            "exhaustion_state": ec.get("exhaustion_state"),
            "bos_quality": ec.get("bos_quality"),
            "displacement_quality": ec.get("displacement_quality"),
            "dow_trend_context": ec.get("dow_trend_context"),
            "premium_discount": ec.get("premium_discount"),
            "liquidity_context": ec.get("liquidity_context"),
            "phase": ec.get("phase"),
            "volume_confirmation": ec.get("volume_confirmation"),
            "planned_rr": ec.get("planned_rr"),
            "risk_class": ec.get("research_entry_timing_risk_class"),
            "fallback_reason": ec.get("research_fallback_reason"),
            "entry_location_would_block": ec.get("confirm_smc_entry_location_would_block"),
            "research_is_post_50": ec.get("research_is_post_50"),
            "bucket": classify_bucket(ec, t.get("side")),
        }
        joined.append(row)
    return joined, missing_ctx


def join_live():
    """Live has no rich entry-context log; use CSV fields only (gap noted)."""
    trades = read_trades(LIVE_TRADES, ENTRY_TYPE)
    joined = []
    for t in trades:
        rr = _f(t.get("rr"))
        mfe = _f(t.get("max_r"))
        immediate_fail = (mfe is not None and mfe < IMMEDIATE_FAIL_MFE)
        row = {
            "trade_id": str(t.get("id")),
            "venue": "live",
            "symbol": t.get("symbol"),
            "side": (t.get("side") or "").upper(),
            "status": t.get("status"),
            "exit_type": t.get("exit_type"),
            "rr": rr,
            "mfe_r": mfe,
            "time_to_1r": _f(t.get("time_to_1r")),
            "immediate_fail": immediate_fail,
            "bos_type": t.get("bos_type"),
            "market_state": t.get("market_state"),
            "phase": t.get("phase"),
            "exhaustion": t.get("exhaustion"),
            "impulse": t.get("impulse"),
            # rich SMC entry-context fields are NOT logged for live trades:
            "smc_zone": None,
            "market_regime": None,
            "bucket": "UNKNOWN",  # cannot bucket reliably without entry context
        }
        joined.append(row)
    return joined


def print_table(title, groups):
    print(f"\n=== {title} ===")
    hdr = ("bucket", "n", "net_r", "avg_r", "win", "pf", "mfe", "imm_fail", "L/S")
    print("{:<26}{:>5}{:>9}{:>8}{:>7}{:>7}{:>7}{:>10}{:>9}".format(*hdr))
    for name, p in groups:
        print("{:<26}{:>5}{:>9}{:>8}{:>7}{:>7}{:>7}{:>10}{:>9}".format(
            name, p["n"],
            f"{p['net_r']:.2f}",
            "-" if p["avg_r"] is None else f"{p['avg_r']:.3f}",
            "-" if p["win_rate"] is None else f"{p['win_rate']:.2f}",
            "-" if p["pf"] is None else f"{p['pf']:.2f}",
            "-" if p["mfe_avg"] is None else f"{p['mfe_avg']:.2f}",
            "-" if p["immediate_fail_rate"] is None else f"{p['immediate_fail_rate']:.2f}",
            f"{p['long']}/{p['short']}",
        ))


def grouped(rows, key):
    g = defaultdict(list)
    for r in rows:
        g[r.get(key) or "UNKNOWN"].append(r)
    out = [(k, perf(v)) for k, v in g.items()]
    out.sort(key=lambda kv: kv[1]["net_r"])
    return out


def main():
    paper, missing_ctx = join_paper()
    live = join_live()

    with PAPER_OUT.open("w", encoding="utf-8") as h:
        for r in paper:
            h.write(json.dumps(r) + "\n")
    with LIVE_OUT.open("w", encoding="utf-8") as h:
        for r in live:
            h.write(json.dumps(r) + "\n")

    print("ENTRY_QUALITY AUDIT — CONFIRM_SMC_RESEARCH")
    print(f"paper trades joined: {len(paper)} (missing entry_context: {missing_ctx})")
    print(f"live trades joined:  {len(live)} (rich entry-context NOT logged for live)")

    overall = perf(paper)
    print(f"\nPAPER OVERALL: n={overall['n']} net_r={overall['net_r']} "
          f"avg_r={overall['avg_r']} win_rate={overall['win_rate']} "
          f"pf={overall['pf']} mfe_avg={overall['mfe_avg']} "
          f"immediate_fail_rate={overall['immediate_fail_rate']}")

    print_table("PAPER by entry-quality bucket", grouped(paper, "bucket"))
    print_table("PAPER by native risk_class", grouped(paper, "risk_class"))
    print_table("PAPER by market_regime", grouped(paper, "market_regime"))
    print_table("PAPER by smc_zone", grouped(paper, "smc_zone"))
    print_table("PAPER by bos_quality", grouped(paper, "bos_quality"))

    # last-50 active paper + post-50 era split
    last50 = paper[-50:]
    print_table("PAPER last-50 by bucket", grouped(last50, "bucket"))
    post50 = [r for r in paper if r.get("research_is_post_50")]
    if post50:
        print_table("PAPER post-50 era by bucket", grouped(post50, "bucket"))

    if live:
        lo = perf(live)
        print(f"\nLIVE OVERALL: n={lo['n']} net_r={lo['net_r']} avg_r={lo['avg_r']} "
              f"win_rate={lo['win_rate']} pf={lo['pf']} mfe_avg={lo['mfe_avg']} "
              f"immediate_fail_rate={lo['immediate_fail_rate']}")

    # PASS/WARN/FAIL verdict
    verdict = "PASS"
    if overall["net_r"] < 0 or (overall["pf"] is not None and overall["pf"] < 1.0):
        verdict = "FAIL"
    elif overall["immediate_fail_rate"] and overall["immediate_fail_rate"] > 0.4:
        verdict = "WARN"
    print(f"\nVERDICT: {verdict}")
    print(f"Outputs: {PAPER_OUT.name}, {LIVE_OUT.name}")


if __name__ == "__main__":
    main()
