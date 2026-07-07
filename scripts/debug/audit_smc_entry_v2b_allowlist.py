#!/usr/bin/env python3
"""SMC_ENTRY_V2B_ALLOWLIST_AUDIT for CONFIRM_SMC_RESEARCH.

Read-only historical audit. Searches executed paper trades for positive
allowlist subsets across 1D/2D/3D buckets and compares them with the current
paper location gate. Does not import runtime execution modules or write logs.
"""

import csv
import itertools
import json
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"
PAPER_TRADES = ROOT / "paper_trades.csv"
LIVE_TRADES = ROOT / "live_trades.csv"
PAPER_ENTRY_CONTEXT = LOG_DIR / "paper_smc_research_entry_context.jsonl"
ENTRY_TYPE = "CONFIRM_SMC_RESEARCH"
IMMEDIATE_MFE_R = 0.5
MIN_N = 20
MIN_VOLUME = 0.15
MIN_PF_EDGE = 0.20


def _f(value):
    try:
        if value in (None, "", "None"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _s(value):
    text = str(value or "").strip().upper()
    return text or "UNKNOWN"


def read_jsonl(path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def load_contexts():
    contexts = {}
    for row in read_jsonl(PAPER_ENTRY_CONTEXT) or []:
        trade_id = row.get("opened_trade_id")
        if trade_id is None:
            continue
        ctx = row.get("entry_context") if isinstance(row.get("entry_context"), dict) else {}
        merged = dict(row)
        merged.update(ctx)
        contexts[str(trade_id)] = merged
    return contexts


def read_trades(path):
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if _s(row.get("entry_type")) == ENTRY_TYPE:
                rows.append(row)
    return rows


def trend_strength_bucket(value):
    val = _f(value)
    if val is None:
        return "TREND_STRENGTH_UNKNOWN"
    if val < 0.01:
        return "TREND_STRENGTH_LT_001"
    if val < 0.02:
        return "TREND_STRENGTH_001_002"
    if val < 0.04:
        return "TREND_STRENGTH_002_004"
    return "TREND_STRENGTH_GTE_004"


def score_bucket(value):
    val = _f(value)
    if val is None:
        return "SCORE_UNKNOWN"
    if val < 1:
        return "SCORE_LT_1"
    if val < 2:
        return "SCORE_1_2"
    if val < 4:
        return "SCORE_2_3"
    return "SCORE_GTE_4"


def rr_bucket(value):
    val = _f(value)
    if val is None:
        return "RR_UNKNOWN"
    if val < 2:
        return "RR_LT_2"
    if val < 3:
        return "RR_2_3"
    if val < 4:
        return "RR_3_4"
    return "RR_GTE_4"


def stale_flag(ctx):
    components = ctx.get("research_entry_timing_components")
    if isinstance(components, dict) and components.get("stale_signal") is not None:
        return "STALE_TRUE" if components.get("stale_signal") else "STALE_FALSE"
    risk_class = _s(ctx.get("research_entry_timing_risk_class"))
    if risk_class == "STALE_SIGNAL_ENTRY":
        return "STALE_TRUE"
    return "STALE_UNKNOWN"


def correct_side_zone(row):
    side = row.get("side")
    zone = row.get("smc_zone")
    if side == "LONG":
        return "CORRECT_SIDE_ZONE" if zone != "PREMIUM" else "BAD_SIDE_ZONE"
    if side == "SHORT":
        return "CORRECT_SIDE_ZONE" if zone != "DISCOUNT" else "BAD_SIDE_ZONE"
    return "SIDE_ZONE_UNKNOWN"


def paper_location_gate_keeps(row):
    return row.get("entry_location_would_block") is not True


def build_rows():
    contexts = load_contexts()
    rows = []
    for trade in read_trades(PAPER_TRADES):
        trade_id = str(trade.get("id"))
        ctx = contexts.get(trade_id, {})
        rr = _f(trade.get("rr"))
        mfe = _f(trade.get("max_r"))
        planned_rr = _f(ctx.get("planned_rr"))
        score = _f(ctx.get("score_v2_structural_shadow"))
        if score is None:
            score = _f(trade.get("score"))
        row = {
            "venue": "paper",
            "trade_id": trade_id,
            "symbol": trade.get("symbol"),
            "side": _s(trade.get("side")),
            "rr": rr,
            "mfe_r": mfe,
            "status": _s(trade.get("status")),
            "exit_type": _s(trade.get("exit_type")),
            "immediate_sl": bool(
                (mfe is not None and mfe < IMMEDIATE_MFE_R)
                or (_s(trade.get("status")) == "LOSE" and _s(trade.get("exit_type")) == "SL")
            ),
            "market_regime": _s(ctx.get("market_regime") or ctx.get("router_regime")),
            "smc_zone": _s(ctx.get("smc_zone")),
            "phase": _s(ctx.get("phase")),
            "range_context": _s(ctx.get("range_context")),
            "bos_quality": _s(ctx.get("bos_quality")),
            "volume_confirmation": _s(ctx.get("volume_confirmation")),
            "exhaustion": _s(ctx.get("exhaustion_state") or ctx.get("exhaustion")),
            "impulse": _s(ctx.get("impulse")),
            "trend_strength_bucket": trend_strength_bucket(ctx.get("trend_strength")),
            "research_entry_timing_risk_class": _s(ctx.get("research_entry_timing_risk_class")),
            "stale_signal": stale_flag(ctx),
            "liquidity_sweep": _s(ctx.get("liquidity_sweep")),
            "candidate_type": _s(ctx.get("candidate_type") or trade.get("priority_final")),
            "score_bucket": score_bucket(score),
            "rr_bucket": rr_bucket(planned_rr if planned_rr is not None else trade.get("rr")),
            "correct_side_zone": correct_side_zone({
                "side": _s(trade.get("side")),
                "smc_zone": _s(ctx.get("smc_zone")),
            }),
            "entry_location_would_block": ctx.get("confirm_smc_entry_location_would_block"),
        }
        rows.append(row)
    return rows


def build_live_rows():
    rows = []
    for trade in read_trades(LIVE_TRADES):
        rr = _f(trade.get("rr"))
        mfe = _f(trade.get("max_r"))
        rows.append({
            "venue": "live",
            "side": _s(trade.get("side")),
            "rr": rr,
            "immediate_sl": bool(
                (mfe is not None and mfe < IMMEDIATE_MFE_R)
                or (_s(trade.get("status")) == "LOSE" and _s(trade.get("exit_type")) == "SL")
            ),
            "score_bucket": score_bucket(trade.get("score")),
            "rr_bucket": rr_bucket(trade.get("rr")),
        })
    return rows


def pf(rows):
    gross_win = sum(row["rr"] for row in rows if row.get("rr") is not None and row["rr"] > 0)
    gross_loss = -sum(row["rr"] for row in rows if row.get("rr") is not None and row["rr"] < 0)
    if gross_loss > 0:
        return gross_win / gross_loss
    if gross_win > 0:
        return float("inf")
    return None


def perf(rows):
    valid = [row for row in rows if row.get("rr") is not None]
    n = len(valid)
    net = sum(row["rr"] for row in valid)
    wins = sum(1 for row in valid if row["rr"] > 0)
    immediate = sum(1 for row in valid if row.get("immediate_sl"))
    return {
        "n": n,
        "net_R": net,
        "avg_R": net / n if n else None,
        "WR": wins / n if n else None,
        "PF": pf(valid),
        "immediate_SL": immediate / n if n else None,
    }


def fmt(value, digits=2, signed=False):
    if value is None:
        return "NA"
    if value == float("inf"):
        return "INF"
    spec = f"+.{digits}f" if signed else f".{digits}f"
    return format(value, spec)


def pct(value):
    return "NA" if value is None else f"{value * 100:.1f}%"


def bucket_key(row, dims):
    return tuple((dim, row.get(dim, "UNKNOWN")) for dim in dims)


def label_key(key):
    return " + ".join(f"{dim}={value}" for dim, value in key)


def outlier_dependency(rows):
    values = sorted([row["rr"] for row in rows if row.get("rr") is not None], reverse=True)
    if not values:
        return "NO_R"
    net = sum(values)
    if len(values) <= 2:
        return "TOO_FEW"
    top1 = values[0]
    top2 = values[0] + values[1]
    if net > 0 and top1 / net > 0.60:
        return "TOP1_DEPENDENT"
    if net > 0 and top2 / net > 0.80:
        return "TOP2_DEPENDENT"
    return "OK"


def evaluate_subset(name, subset, all_rows, base_perf):
    p = perf(subset)
    selected_ids = {id(row) for row in subset}
    not_kept = [row for row in all_rows if id(row) not in selected_ids]
    avoided_losses = -sum(row["rr"] for row in not_kept if row.get("rr") is not None and row["rr"] < 0)
    missed_winners = sum(row["rr"] for row in not_kept if row.get("rr") is not None and row["rr"] > 0)
    volume = p["n"] / base_perf["n"] if base_perf["n"] else None
    pf_edge = None if p["PF"] is None or base_perf["PF"] is None else p["PF"] - base_perf["PF"]
    avg_edge = None if p["avg_R"] is None or base_perf["avg_R"] is None else p["avg_R"] - base_perf["avg_R"]
    qualifies = (
        p["n"] >= MIN_N or (volume is not None and volume >= MIN_VOLUME)
    ) and (
        pf_edge is not None and pf_edge >= MIN_PF_EDGE
    ) and (
        avg_edge is not None and avg_edge > 0
    ) and outlier_dependency(subset) == "OK"
    return {
        "name": name,
        **p,
        "volume_retained": volume,
        "missed_winners": missed_winners,
        "avoided_losses": avoided_losses,
        "pf_edge": pf_edge,
        "avg_edge": avg_edge,
        "outlier_check": outlier_dependency(subset),
        "qualifies": qualifies,
    }


def generate_bucket_results(rows, base_perf):
    dims = [
        "market_regime",
        "smc_zone",
        "side",
        "phase",
        "range_context",
        "bos_quality",
        "volume_confirmation",
        "exhaustion",
        "impulse",
        "trend_strength_bucket",
        "research_entry_timing_risk_class",
        "stale_signal",
        "liquidity_sweep",
        "candidate_type",
        "score_bucket",
        "rr_bucket",
        "correct_side_zone",
    ]
    results = []
    for depth in (1, 2, 3):
        for combo in itertools.combinations(dims, depth):
            groups = defaultdict(list)
            for row in rows:
                groups[bucket_key(row, combo)].append(row)
            for key, subset in groups.items():
                if len(subset) < 3:
                    continue
                results.append(evaluate_subset(label_key(key), subset, rows, base_perf))
    return results


def specific_tests(rows, base_perf):
    tests = {
        "A_RANGE_MEAN_REVERSION_ONLY": lambda r: r["market_regime"] == "RANGE_MEAN_REVERSION",
        "B_RANGE_MEAN_REVERSION_CORRECT_SIDE_ZONE": lambda r: r["market_regime"] == "RANGE_MEAN_REVERSION" and r["correct_side_zone"] == "CORRECT_SIDE_ZONE",
        "C_FRESH_SIGNAL_ONLY": lambda r: r["stale_signal"] == "STALE_FALSE",
        "D_FRESH_SIGNAL_NON_CHOP": lambda r: r["stale_signal"] == "STALE_FALSE" and r["market_regime"] != "CHOP_NO_TRADE",
        "E_NOT_STALE_RR_2_4": lambda r: r["stale_signal"] != "STALE_TRUE" and r["rr_bucket"] in {"RR_2_3", "RR_3_4"},
        "F_EXCLUDE_NO_FOLLOWTHROUGH_ONLY": lambda r: r["bos_quality"] != "NO_FOLLOWTHROUGH",
        "G_CORRECT_SIDE_ZONE_ONLY": lambda r: r["correct_side_zone"] == "CORRECT_SIDE_ZONE",
    }
    out = [
        evaluate_subset(name, [row for row in rows if pred(row)], rows, base_perf)
        for name, pred in tests.items()
    ]
    for bucket in sorted({row["score_bucket"] for row in rows}):
        out.append(evaluate_subset(
            f"H_SCORE_BUCKET_{bucket}",
            [row for row in rows if row["score_bucket"] == bucket],
            rows,
            base_perf,
        ))
    return out


def live_positive_paper_negative(rows, live_rows):
    common_dims = ("side", "score_bucket", "rr_bucket")
    paper_groups = defaultdict(list)
    live_groups = defaultdict(list)
    for row in rows:
        paper_groups[bucket_key(row, common_dims)].append(row)
    for row in live_rows:
        live_groups[bucket_key(row, common_dims)].append(row)

    out = []
    for key, live_subset in live_groups.items():
        paper_subset = paper_groups.get(key, [])
        live_perf = perf(live_subset)
        paper_perf = perf(paper_subset)
        if live_perf["n"] < 3 or paper_perf["n"] < 3:
            continue
        if (live_perf["net_R"] or 0) > 0 and (paper_perf["net_R"] or 0) < 0:
            out.append({
                "name": label_key(key),
                "live_n": live_perf["n"],
                "live_net_R": live_perf["net_R"],
                "live_PF": live_perf["PF"],
                "paper_n": paper_perf["n"],
                "paper_net_R": paper_perf["net_R"],
                "paper_PF": paper_perf["PF"],
            })
    out.sort(key=lambda row: (row["live_net_R"], -row["paper_net_R"]), reverse=True)
    return out


def recommendation(best, base_perf, paper_gate_perf):
    if best and best["qualifies"]:
        return "PROMOTE_ALLOWLIST_SHADOW"
    if not best or best["n"] < MIN_N:
        return "KILL_CONFIRM_SMC_RESEARCH_MODEL"
    if paper_gate_perf["n"] > 0 and (paper_gate_perf["PF"] or 0) > (base_perf["PF"] or 0):
        return "PAPER_ONLY_CONTINUE_LOCATION_GATE"
    return "NEED_FEATURES"


def print_table(title, rows, limit=10):
    print(f"\n{title}")
    print("bucket                                                           n    net_R   avg_R      WR      PF  imm_SL  volume  missed_W  avoided_L  outlier")
    for row in rows[:limit]:
        print(
            f"{row['name'][:62]:<62}"
            f"{row['n']:>5}"
            f"{fmt(row['net_R']):>9}"
            f"{fmt(row['avg_R'], 3):>8}"
            f"{pct(row['WR']):>8}"
            f"{fmt(row['PF']):>8}"
            f"{pct(row['immediate_SL']):>8}"
            f"{pct(row['volume_retained']):>8}"
            f"{fmt(row['missed_winners']):>10}"
            f"{fmt(row['avoided_losses']):>11}"
            f"  {row['outlier_check']}"
        )


def print_live_paper_table(rows, limit=10):
    print("\nI. Live-positive buckets vs paper-negative buckets")
    if not rows:
        print("NONE with available shared live/paper fields: side, score_bucket, rr_bucket.")
        return
    print("bucket                                                           live_n live_R live_PF paper_n paper_R paper_PF")
    for row in rows[:limit]:
        print(
            f"{row['name'][:62]:<62}"
            f"{row['live_n']:>7}"
            f"{fmt(row['live_net_R']):>7}"
            f"{fmt(row['live_PF']):>8}"
            f"{row['paper_n']:>8}"
            f"{fmt(row['paper_net_R']):>8}"
            f"{fmt(row['paper_PF']):>9}"
        )


def main():
    rows = build_rows()
    live_rows = build_live_rows()
    base = perf(rows)
    live_base = perf(live_rows)

    all_results = generate_bucket_results(rows, base)
    specific = specific_tests(rows, base)
    candidates = [
        row for row in all_results
        if row["n"] >= MIN_N or (row["volume_retained"] is not None and row["volume_retained"] >= MIN_VOLUME)
    ]
    ranked = sorted(
        candidates,
        key=lambda r: (
            r["qualifies"],
            r["PF"] if r["PF"] is not None else -1,
            r["avg_R"] if r["avg_R"] is not None else -999,
            r["n"],
        ),
        reverse=True,
    )
    qualifying = [row for row in ranked if row["qualifies"]]
    best = qualifying[0] if qualifying else (ranked[0] if ranked else None)

    paper_gate_kept = [row for row in rows if paper_location_gate_keeps(row)]
    paper_gate = evaluate_subset("CURRENT_PAPER_LOCATION_GATE_KEEP", paper_gate_kept, rows, base)
    v2b_kept = [row for row in rows if best and row_matches_name(row, best["name"])]
    overlap = len({id(row) for row in paper_gate_kept}.intersection(id(row) for row in v2b_kept))
    live_paper = live_positive_paper_negative(rows, live_rows)
    rec = recommendation(best, base, paper_gate)
    verdict = "PASS" if rec == "PROMOTE_ALLOWLIST_SHADOW" else ("WARN" if rec in {"NEED_FEATURES", "PAPER_ONLY_CONTINUE_LOCATION_GATE"} else "FAIL")

    print(f"PASS/WARN/FAIL: {verdict}")
    print("\nV1 baseline")
    print(
        f"paper n={base['n']} net_R={fmt(base['net_R'])} avg_R={fmt(base['avg_R'], 3)} "
        f"WR={pct(base['WR'])} PF={fmt(base['PF'])} immediate_SL={pct(base['immediate_SL'])}"
    )
    print(
        f"live reference n={live_base['n']} net_R={fmt(live_base['net_R'])} "
        f"WR={pct(live_base['WR'])} PF={fmt(live_base['PF'])}"
    )

    print_table("Top 10 allowlist buckets", ranked, limit=10)
    print_table("Specific requested tests", specific, limit=len(specific))
    print_live_paper_table(live_paper)

    print("\nCurrent PAPER_LOCATION_GATE comparison")
    print(
        f"paper gate keep n={paper_gate['n']} net_R={fmt(paper_gate['net_R'])} "
        f"PF={fmt(paper_gate['PF'])} volume={pct(paper_gate['volume_retained'])}"
    )
    if best:
        print(
            f"best V2B keep n={best['n']} net_R={fmt(best['net_R'])} PF={fmt(best['PF'])} "
            f"volume={pct(best['volume_retained'])} overlap_with_paper_gate={overlap}"
        )
    else:
        print("best V2B keep n=0 overlap_with_paper_gate=0")

    print("\nBest candidate")
    if best:
        print(
            f"{best['name']} | n={best['n']} net_R={fmt(best['net_R'])} "
            f"avg_R={fmt(best['avg_R'], 3)} WR={pct(best['WR'])} PF={fmt(best['PF'])} "
            f"volume={pct(best['volume_retained'])} outlier={best['outlier_check']} "
            f"qualifies={best['qualifies']}"
        )
        print(
            f"PF improvement={fmt(best['pf_edge'])} avg_R improvement={fmt(best['avg_edge'], 3)} "
            f"missed_winners={fmt(best['missed_winners'])} avoided_losses={fmt(best['avoided_losses'])}"
        )
    else:
        print("NONE")

    print(f"\nRecommendation: {rec}")
    print("Confirmed no live/testnet orders touched; audit reads CSV/JSONL only.")
    print("Do not commit. Do not push.")


def row_matches_name(row, name):
    if not name:
        return False
    parts = [part.strip() for part in name.split("+")]
    for part in parts:
        if "=" not in part:
            return False
        dim, value = [piece.strip() for piece in part.split("=", 1)]
        if str(row.get(dim, "UNKNOWN")) != value:
            return False
    return True


if __name__ == "__main__":
    main()
