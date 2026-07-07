#!/usr/bin/env python3
"""Audit: SMC_PA_SCORE_V3_SHADOW forward log vs realized outcomes.

Read-only. Consumes logs/smc_pa_score_v3_shadow.jsonl when present, joins
OPEN/opened rows to realized outcomes in paper_trades.csv / live_trades.csv
(symbol + side + nearest signal timestamp), then reports:

  - bucket stats by total score band and by each component value
    (n, net_R, avg_R, PF, WR, immediate-fail rate via max_r < 0.3)
  - monotonicity of avg_R across total-score buckets
  - splits: paper/live, LONG/SHORT, BTC bias (bullish/bearish/neutral/unknown)

Recommendation policy: requires n >= 100 joined outcomes before recommending
any weight or gate. Below that it only reports observations.
"""

import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FORWARD_LOG = os.path.join(REPO_ROOT, "logs", "smc_pa_score_v3_shadow.jsonl")
PAPER_TRADES = os.path.join(REPO_ROOT, "paper_trades.csv")
LIVE_TRADES = os.path.join(REPO_ROOT, "live_trades.csv")

MIN_N_FOR_RECOMMENDATION = 100
JOIN_TOLERANCE_SECS = 1800
IMMEDIATE_FAIL_MFE = 0.3

COMPONENT_FIELDS = (
    "smc_pa_v3_market_bias_score",
    "smc_pa_v3_regime_score",
    "smc_pa_v3_structure_quality_score",
    "smc_pa_v3_liquidity_sweep_score",
    "smc_pa_v3_location_quality_score",
    "smc_pa_v3_breakout_acceptance_score",
    "smc_pa_v3_relative_strength_score",
    "smc_pa_v3_volatility_sl_quality_score",
    "smc_pa_v3_target_realism_score",
    "smc_pa_v3_execution_risk_score",
)

CLOSED_STATUSES = {"WIN", "LOSE", "LOSS", "BE", "BREAKEVEN"}


def _f(value):
    try:
        if value in (None, ""):
            return None
        out = float(value)
        if out != out:
            return None
        return out
    except (TypeError, ValueError):
        return None


def _parse_open_time(value, ref_year):
    # paper/live trades open_time format: "HH:MM DD-MM" (no year)
    try:
        dt = datetime.strptime(f"{value}-{ref_year}", "%H:%M %d-%m-%Y")
        return dt.timestamp()
    except Exception:
        return None


def load_forward_rows():
    rows = []
    if not os.path.exists(FORWARD_LOG):
        return rows
    with open(FORWARD_LOG, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def load_trades(path, venue):
    out = []
    if not os.path.exists(path):
        return out
    ref_year = datetime.now().year
    with open(path, "r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("status") or "").upper() not in CLOSED_STATUSES:
                continue
            rr = _f(row.get("rr"))
            if rr is None:
                continue
            ts = _f(row.get("signal_created_ts"))
            if ts is None:
                ts = _parse_open_time(row.get("open_time"), ref_year)
            out.append({
                "venue": venue,
                "symbol": row.get("symbol"),
                "side": str(row.get("side") or "").upper(),
                "rr": rr,
                "max_r": _f(row.get("max_r")),
                "ts": ts,
                "trade_id": row.get("id"),
            })
    return out


def join_outcomes(shadow_rows, trades):
    by_symbol_side = defaultdict(list)
    for t in trades:
        by_symbol_side[(t["symbol"], t["side"])].append(t)

    joined = []
    used_trade_ids = set()
    for row in shadow_rows:
        decision = str(row.get("v1_decision") or "").upper()
        if decision not in ("OPEN", "OPENED", "QUALIFIED_OPEN"):
            continue
        key = (row.get("symbol"), str(row.get("side") or "").upper())
        cands = by_symbol_side.get(key) or []
        sig_ts = _f(row.get("signal_ts")) or _f(row.get("ts"))
        best = None
        best_dt = None
        for t in cands:
            if t["trade_id"] in used_trade_ids or t["ts"] is None or sig_ts is None:
                continue
            dt = abs(t["ts"] - sig_ts)
            if dt <= JOIN_TOLERANCE_SECS and (best_dt is None or dt < best_dt):
                best, best_dt = t, dt
        if best is not None:
            used_trade_ids.add(best["trade_id"])
            merged = dict(row)
            merged["_rr"] = best["rr"]
            merged["_max_r"] = best["max_r"]
            merged["_venue"] = best["venue"]
            joined.append(merged)
    return joined


def bucket_stats(rows, label):
    n = len(rows)
    if n == 0:
        print(f"  {label:52s} n=0")
        return None
    rrs = [r["_rr"] for r in rows]
    wins = sum(v for v in rrs if v > 0)
    losses = -sum(v for v in rrs if v < 0)
    pf = (wins / losses) if losses > 0 else float("inf")
    wr = sum(1 for v in rrs if v > 0) / n
    mfes = [r["_max_r"] for r in rows if r["_max_r"] is not None]
    imm = (
        sum(1 for v in mfes if v < IMMEDIATE_FAIL_MFE) / len(mfes)
        if mfes else float("nan")
    )
    avg = sum(rrs) / n
    print(
        f"  {label:52s} n={n:4d} netR={sum(rrs):8.2f} avgR={avg:7.3f} "
        f"PF={pf:5.2f} WR={wr:5.1%} immFail={imm:5.1%}"
    )
    return avg


def main():
    shadow_rows = load_forward_rows()
    print(f"forward log rows: {len(shadow_rows)} ({FORWARD_LOG})")
    if not shadow_rows:
        print("No forward log yet - nothing to audit. "
              "Run the bot with the V3 shadow deployed and re-run this audit.")
        print("RESULT: NO_DATA")
        return 0

    # distribution snapshot (all rows, no outcomes needed)
    band_counts = defaultdict(int)
    missing_counts = defaultdict(int)
    for row in shadow_rows:
        band_counts[str(row.get("smc_pa_v3_score_band"))] += 1
        for m in row.get("smc_pa_v3_missing_components") or []:
            missing_counts[str(m)] += 1
    print("\nband distribution (all shadow rows):")
    for band, cnt in sorted(band_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {band:28s} {cnt}")
    print("missing-component frequency (all shadow rows):")
    for name, cnt in sorted(missing_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {name:44s} {cnt} ({cnt / len(shadow_rows):.0%})")

    trades = load_trades(PAPER_TRADES, "paper") + load_trades(LIVE_TRADES, "live")
    joined = join_outcomes(shadow_rows, trades)
    print(f"\njoined OPEN rows to realized outcomes: {len(joined)}")
    if not joined:
        print("No joined outcomes yet - component predictiveness not measurable.")
        print("RESULT: NO_OUTCOMES")
        return 0

    print("\n===== total score buckets =====")
    def total_bucket(row):
        t = _f(row.get("smc_pa_v3_total_score"))
        if t is None:
            return "NA"
        if t >= 4:
            return "4_STRONG(>=4)"
        if t >= 1:
            return "3_OK(1..4)"
        if t >= -2:
            return "2_WEAK(-2..1)"
        return "1_REJECT_LIKE(<-2)"

    buckets = defaultdict(list)
    for row in joined:
        buckets[total_bucket(row)].append(row)
    ordered = sorted(buckets.keys())
    avgs = []
    for key in ordered:
        avg = bucket_stats(buckets[key], f"total {key}")
        if avg is not None and key != "NA":
            avgs.append((key, avg, len(buckets[key])))

    if len(avgs) >= 2:
        monotone = all(avgs[i][1] <= avgs[i + 1][1] for i in range(len(avgs) - 1))
        print(f"\nmonotonicity (higher bucket -> higher avgR): "
              f"{'YES' if monotone else 'NO'} across {len(avgs)} buckets")

    print("\n===== band buckets =====")
    band_groups = defaultdict(list)
    for row in joined:
        band_groups[str(row.get("smc_pa_v3_score_band"))].append(row)
    for band, rows in sorted(band_groups.items()):
        bucket_stats(rows, f"band {band}")

    print("\n===== per-component buckets =====")
    for comp in COMPONENT_FIELDS:
        groups = defaultdict(list)
        for row in joined:
            groups[str(row.get(comp))].append(row)
        if len(groups) <= 1:
            print(f"\n-- {comp}: degenerate (single value "
                  f"{next(iter(groups)) if groups else 'NA'}) --")
            continue
        print(f"\n-- {comp} --")
        comp_avgs = []
        for value, rows in sorted(groups.items(), key=lambda kv: _f(kv[0]) if _f(kv[0]) is not None else -99):
            avg = bucket_stats(rows, f"{comp.split('smc_pa_v3_')[-1]}={value}")
            if avg is not None and _f(value) is not None:
                comp_avgs.append((_f(value), avg))
        if len(comp_avgs) >= 2:
            monotone = all(
                comp_avgs[i][1] <= comp_avgs[i + 1][1]
                for i in range(len(comp_avgs) - 1)
            )
            print(f"  monotone vs avgR: {'YES' if monotone else 'NO'}")

    print("\n===== splits =====")
    for split_name, key_fn in (
        ("venue", lambda r: r.get("_venue")),
        ("side", lambda r: str(r.get("side") or "").upper()),
        ("btc_bias", lambda r: str(
            r.get("btc_bias_independent") or r.get("smc_pa_v3_bias_value") or "UNKNOWN"
        ).upper()),
    ):
        groups = defaultdict(list)
        for row in joined:
            groups[str(key_fn(row))].append(row)
        print(f"\n-- by {split_name} --")
        for value, rows in sorted(groups.items()):
            bucket_stats(rows, f"{split_name}={value}")
            # nested: total bucket inside split
            sub = defaultdict(list)
            for r in rows:
                sub[total_bucket(r)].append(r)
            if len(sub) > 1:
                for sk in sorted(sub.keys()):
                    bucket_stats(sub[sk], f"    {split_name}={value} total {sk}")

    print("\n===== recommendation policy =====")
    if len(joined) < MIN_N_FOR_RECOMMENDATION:
        print(f"n={len(joined)} < {MIN_N_FOR_RECOMMENDATION}: observations only. "
              "NO weight/gate recommendation until n >= "
              f"{MIN_N_FOR_RECOMMENDATION} joined outcomes.")
        print("RESULT: INSUFFICIENT_N")
    else:
        print(f"n={len(joined)} >= {MIN_N_FOR_RECOMMENDATION}: component stats above are "
              "eligible for weight/gate discussion (still shadow-only; any gate "
              "needs its own sim + separate approval).")
        print("RESULT: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
