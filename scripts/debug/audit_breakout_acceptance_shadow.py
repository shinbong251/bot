#!/usr/bin/env python3
"""Audit: BREAKOUT_ACCEPTANCE_SHADOW forward log vs realized outcomes.

Read-only. Consumes logs/breakout_acceptance_shadow.jsonl when present:

  1. Label distribution across all shadow rows.
  2. Lifecycle reconstruction: runtime N-bar lifecycle is MISSING by design
     (lifecycle_tracking = MISSING_RUNTIME_DEFERRED_TO_AUDIT); this script
     reconstructs an APPROXIMATE lifecycle by joining rows to
     logs/confirm_structural_outcomes.jsonl on dedup_key (mfe_r/mae_r/first_hit
     are intrabar excursions over the tracked horizon, not close-basis N-bar
     acceptance — treated as approximation and labelled as such).
     back-inside approximation: mae_r exceeded the entry->level distance in R,
     i.e. price traded back through the breakout level (intrabar).
  3. Realized-outcome join: OPEN/opened rows joined to closed trades in
     paper_trades.csv / live_trades.csv (symbol + side + nearest signal ts).
  4. Buckets by acceptance label (n, net_R, PF, WR, avg_R, immediate_SL,
     MFE/MAE) with accepted vs failed/no-followthrough comparison and
     LONG/SHORT + BTC bias splits.

Promotion policy (all must hold before any gate is even discussed):
  - >= 100 shadow rows OR >= 30 realized closed joined trades
  - accepted bucket PF materially above baseline PF (>= 1.5x and >= 1.3 abs)
  - failed/no-followthrough bucket clearly negative (net_R < 0 and PF < 0.8)
  - not outlier-only (accepted bucket still positive with best trade removed)
  - does not block everything (accepted share >= 20% of labelled rows)
Below sample thresholds the script reports LOW_SAMPLE and makes no
recommendation. Any actual gate needs its own sim + separate approval.
"""

import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FORWARD_LOG = os.path.join(REPO_ROOT, "logs", "breakout_acceptance_shadow.jsonl")
STRUCTURAL_OUTCOMES = os.path.join(REPO_ROOT, "logs", "confirm_structural_outcomes.jsonl")
PAPER_TRADES = os.path.join(REPO_ROOT, "paper_trades.csv")
LIVE_TRADES = os.path.join(REPO_ROOT, "live_trades.csv")

MIN_SHADOW_ROWS = 100
MIN_REALIZED_JOINS = 30
JOIN_TOLERANCE_SECS = 1800
IMMEDIATE_FAIL_MFE = 0.3
ACCEPTED_PF_RATIO_MIN = 1.5
ACCEPTED_PF_ABS_MIN = 1.3
FAILED_PF_MAX = 0.8
ACCEPTED_SHARE_MIN = 0.20

CLOSED_STATUSES = {"WIN", "LOSE", "LOSS", "BE", "BREAKEVEN"}
ACCEPT_LABELS = {"BREAKOUT_ACCEPTED", "BREAKOUT_RETEST_HELD"}
FAIL_LABELS = {"BREAKOUT_FAILED_BACK_INSIDE", "BREAKOUT_NO_FOLLOWTHROUGH",
               "BREAKOUT_WICK_REJECTED"}


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


def load_jsonl(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as handle:
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


def latest_structural_by_dedup(rows):
    latest = {}
    for row in rows:
        key = str(row.get("dedup_key") or "")
        if not key:
            continue
        observed = _f(row.get("observed_at")) or 0.0
        prev = latest.get(key)
        if prev is None or observed >= (_f(prev.get("observed_at")) or 0.0):
            latest[key] = row
    return latest


def reconstruct_lifecycle(shadow_rows, structural_by_dedup):
    """Approximate lifecycle from structural-outcome excursions (intrabar,
    not close-basis N-bar acceptance). Returns count of enriched rows."""
    enriched = 0
    for row in shadow_rows:
        outcome = structural_by_dedup.get(str(row.get("dedup_key") or ""))
        if outcome is None:
            continue
        row["_lc_mfe_r"] = _f(outcome.get("mfe_r"))
        row["_lc_mae_r"] = _f(outcome.get("mae_r"))
        row["_lc_first_hit"] = outcome.get("first_hit")
        row["_lc_sl_hit"] = bool(outcome.get("sl_hit"))
        row["_lc_status"] = outcome.get("status")
        entry = _f(row.get("entry"))
        sl = _f(row.get("sl"))
        level = _f(row.get("breakout_level"))
        side = str(row.get("side") or "").upper()
        back_inside = None
        if (entry is not None and sl is not None and level is not None
                and abs(entry - sl) > 0 and row["_lc_mae_r"] is not None):
            level_dist_r = None
            if side == "LONG" and entry > level:
                level_dist_r = (entry - level) / abs(entry - sl)
            elif side == "SHORT" and entry < level:
                level_dist_r = (level - entry) / abs(entry - sl)
            if level_dist_r is not None:
                back_inside = row["_lc_mae_r"] > level_dist_r
        row["_lc_back_inside_approx"] = back_inside
        enriched += 1
    return enriched


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
        print(f"  {label:46s} n=0")
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
    lc_mfes = [r["_lc_mfe_r"] for r in rows if r.get("_lc_mfe_r") is not None]
    lc_maes = [r["_lc_mae_r"] for r in rows if r.get("_lc_mae_r") is not None]
    avg_mfe = sum(lc_mfes) / len(lc_mfes) if lc_mfes else float("nan")
    avg_mae = sum(lc_maes) / len(lc_maes) if lc_maes else float("nan")
    avg = sum(rrs) / n
    print(
        f"  {label:46s} n={n:4d} netR={sum(rrs):8.2f} avgR={avg:7.3f} "
        f"PF={pf:5.2f} WR={wr:5.1%} immSL={imm:5.1%} "
        f"MFE={avg_mfe:6.2f} MAE={avg_mae:6.2f}"
    )
    return {"n": n, "net_r": sum(rrs), "avg_r": avg, "pf": pf, "wr": wr,
            "rrs": rrs}


def main():
    shadow_rows = load_jsonl(FORWARD_LOG)
    print(f"forward log rows: {len(shadow_rows)} ({FORWARD_LOG})")
    print("runtime N-bar lifecycle: MISSING by design "
          "(lifecycle_tracking=MISSING_RUNTIME_DEFERRED_TO_AUDIT); "
          "this audit reconstructs an approximation from "
          "confirm_structural_outcomes.jsonl (intrabar excursions, "
          "not close-basis N-bar acceptance).")
    if not shadow_rows:
        print("No forward log yet - nothing to audit. Run the bot with the "
              "breakout acceptance shadow deployed and re-run this audit.")
        print("RESULT: LOW_SAMPLE (no shadow rows)")
        return 0

    label_counts = defaultdict(int)
    for row in shadow_rows:
        label_counts[str(row.get("breakout_acceptance_label"))] += 1
    print("\nlabel distribution (all shadow rows):")
    for label, cnt in sorted(label_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {label:36s} {cnt} ({cnt / len(shadow_rows):.0%})")

    structural_by_dedup = latest_structural_by_dedup(load_jsonl(STRUCTURAL_OUTCOMES))
    enriched = reconstruct_lifecycle(shadow_rows, structural_by_dedup)
    print(f"\nlifecycle reconstruction: {enriched}/{len(shadow_rows)} rows joined "
          f"to structural outcomes (dedup_key)")
    back_counts = defaultdict(int)
    for row in shadow_rows:
        back_counts[str(row.get("_lc_back_inside_approx"))] += 1
    print("back-inside-level approximation (MAE-based, intrabar):")
    for value, cnt in sorted(back_counts.items()):
        print(f"  back_inside_approx={value:8s} {cnt}")

    trades = load_trades(PAPER_TRADES, "paper") + load_trades(LIVE_TRADES, "live")
    joined = join_outcomes(shadow_rows, trades)
    print(f"\njoined OPEN rows to realized closed outcomes: {len(joined)}")

    verdicts = {}
    if joined:
        print("\n===== acceptance label buckets (realized outcomes) =====")
        groups = defaultdict(list)
        for row in joined:
            groups[str(row.get("breakout_acceptance_label"))].append(row)
        for label, rows in sorted(groups.items(), key=lambda kv: -len(kv[1])):
            stats = bucket_stats(rows, label)
            if stats is not None:
                verdicts[label] = stats

        accepted_rows = [r for r in joined
                         if str(r.get("breakout_acceptance_label")) in ACCEPT_LABELS]
        failed_rows = [r for r in joined
                       if str(r.get("breakout_acceptance_label")) in FAIL_LABELS]
        print("\n===== accepted vs failed/no-followthrough =====")
        accepted_stats = bucket_stats(accepted_rows, "ACCEPTED+RETEST_HELD")
        failed_stats = bucket_stats(failed_rows, "FAILED+NO_FOLLOWTHROUGH+WICK")
        baseline_stats = bucket_stats(joined, "BASELINE (all joined)")

        print("\n===== splits =====")
        for split_name, key_fn in (
            ("side", lambda r: str(r.get("side") or "").upper()),
            ("btc_bias", lambda r: str(r.get("btc_bias_independent") or "UNKNOWN").upper()),
            ("venue", lambda r: r.get("_venue")),
        ):
            split_groups = defaultdict(list)
            for row in joined:
                split_groups[str(key_fn(row))].append(row)
            print(f"\n-- by {split_name} --")
            for value, rows in sorted(split_groups.items()):
                bucket_stats(rows, f"{split_name}={value}")
                sub = defaultdict(list)
                for r in rows:
                    sub[str(r.get("breakout_acceptance_label"))].append(r)
                if len(sub) > 1:
                    for sk in sorted(sub.keys()):
                        bucket_stats(sub[sk], f"    {split_name}={value} {sk}")
    else:
        accepted_stats = failed_stats = baseline_stats = None
        print("No realized joined outcomes yet - label predictiveness not "
              "measurable against closed trades.")

    print("\n===== promotion policy =====")
    sample_ok = len(shadow_rows) >= MIN_SHADOW_ROWS or len(joined) >= MIN_REALIZED_JOINS
    print(f"sample: shadow_rows={len(shadow_rows)} (need >= {MIN_SHADOW_ROWS}) "
          f"OR realized_joins={len(joined)} (need >= {MIN_REALIZED_JOINS}) "
          f"-> {'OK' if sample_ok else 'LOW_SAMPLE'}")
    if not sample_ok:
        print("RESULT: LOW_SAMPLE (observations only; no recommendation)")
        return 0

    checks = []
    if accepted_stats and baseline_stats and accepted_stats["n"] > 0:
        pf_ok = (
            baseline_stats["pf"] > 0
            and accepted_stats["pf"] >= max(
                baseline_stats["pf"] * ACCEPTED_PF_RATIO_MIN, ACCEPTED_PF_ABS_MIN
            )
        )
        checks.append(("accepted PF materially > baseline "
                       f"(>= {ACCEPTED_PF_RATIO_MIN}x and >= {ACCEPTED_PF_ABS_MIN})", pf_ok))
        without_best = sorted(accepted_stats["rrs"])[:-1]
        checks.append(("not outlier-only (accepted netR > 0 without best trade)",
                       bool(without_best) and sum(without_best) > 0))
        labelled = [r for r in joined
                    if str(r.get("breakout_acceptance_label")) in (ACCEPT_LABELS | FAIL_LABELS)]
        share = (len([r for r in labelled
                      if str(r.get("breakout_acceptance_label")) in ACCEPT_LABELS])
                 / len(labelled)) if labelled else 0.0
        checks.append((f"does not block everything (accepted share >= {ACCEPTED_SHARE_MIN:.0%}; "
                       f"actual {share:.0%})", share >= ACCEPTED_SHARE_MIN))
    else:
        checks.append(("accepted bucket has realized outcomes", False))
    if failed_stats and failed_stats["n"] > 0:
        checks.append((f"failed/no-followthrough clearly negative "
                       f"(netR < 0 and PF < {FAILED_PF_MAX})",
                       failed_stats["net_r"] < 0 and failed_stats["pf"] < FAILED_PF_MAX))
    else:
        checks.append(("failed bucket has realized outcomes", False))

    all_ok = all(ok for _, ok in checks)
    for name, ok in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    if all_ok:
        print("RESULT: PROMOTION_CANDIDATE (still shadow-only; any gate needs "
              "its own sim + separate approval)")
    else:
        print("RESULT: NOT_READY (keep shadow logging)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
