#!/usr/bin/env python3
"""Historical evaluator for SMC_ENTRY_V2_SHADOW.

Read-only. Compares executed CONFIRM_SMC_RESEARCH paper trades against the V2
shadow entry model using existing CSV/JSONL context fields.
"""

import csv
import json
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"
PAPER_TRADES = ROOT / "paper_trades.csv"
PAPER_ENTRY_CONTEXT = LOG_DIR / "paper_smc_research_entry_context.jsonl"
ENTRY_TYPE = "CONFIRM_SMC_RESEARCH"
IMMEDIATE_MFE_R = 0.5


def _f(value):
    try:
        if value in (None, "", "None"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _s(value):
    return str(value or "").strip().upper()


def first(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None


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


def read_trades():
    if not PAPER_TRADES.exists():
        return []
    rows = []
    with PAPER_TRADES.open("r", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if _s(row.get("entry_type")) == ENTRY_TYPE:
                rows.append(row)
    return rows


def expected_rr(side, entry, sl, tp):
    if entry is None or sl is None or tp is None:
        return None
    risk = abs(entry - sl)
    if risk <= 0:
        return None
    reward = tp - entry if side == "LONG" else entry - tp if side == "SHORT" else None
    if reward is None or reward <= 0:
        return None
    return reward / risk


def v2_shadow_for_trade(trade, ctx):
    side = _s(first(ctx.get("side"), trade.get("side")))
    entry = _f(first(ctx.get("entry"), trade.get("entry")))
    sl = _f(first(ctx.get("sl"), trade.get("sl")))
    tp = _f(first(ctx.get("tp"), trade.get("tp")))
    rr = _f(first(ctx.get("planned_rr"), trade.get("rr")))
    if rr is None:
        rr = expected_rr(side, entry, sl, tp)

    smc_zone = _s(ctx.get("smc_zone"))
    market_regime = _s(first(ctx.get("market_regime"), ctx.get("router_regime")))
    bos_quality = _s(ctx.get("bos_quality"))
    exhaustion = _s(first(ctx.get("exhaustion_state"), ctx.get("exhaustion")))
    phase = _s(ctx.get("phase"))
    range_context = _s(ctx.get("range_context"))
    liquidity_sweep = _s(ctx.get("liquidity_sweep"))
    risk_class = _s(ctx.get("research_entry_timing_risk_class"))

    feature_quality = "EXACT_FEATURES" if any(
        _s(ctx.get(key)) not in {"", "UNKNOWN", "NONE"}
        for key in (
            "premium_discount",
            "dow_trend_context",
            "poi_type",
            "poi_location_quality",
            "entry_poi_alignment",
            "liquidity_context",
        )
    ) else "COARSE_PROXY"

    missing = [
        name for name, value in (
            ("side", side),
            ("entry", entry),
            ("sl", sl),
            ("tp", tp),
            ("rr", rr),
        )
        if value in (None, "")
    ]

    location_score = 0
    if side == "LONG":
        if smc_zone == "DISCOUNT" or range_context == "RANGE_LOW":
            location_score += 2
        if smc_zone == "PREMIUM":
            location_score -= 3
        if liquidity_sweep == "SWEEP_LOW":
            location_score += 1
        if liquidity_sweep == "SWEEP_HIGH":
            location_score -= 1
    elif side == "SHORT":
        if smc_zone == "PREMIUM" or range_context == "RANGE_HIGH":
            location_score += 2
        if smc_zone == "DISCOUNT":
            location_score -= 3
        if liquidity_sweep == "SWEEP_HIGH":
            location_score += 1
        if liquidity_sweep == "SWEEP_LOW":
            location_score -= 1

    has_retest_proxy = (
        any(token in phase for token in ("RETEST", "PULLBACK", "PRE_BREAK", "PREBREAK", "ACCEPT", "RECLAIM"))
        or risk_class == "PRE_BREAK_ANTICIPATION"
    )
    if has_retest_proxy:
        location_score += 1

    late_score = 0
    late_reasons = []
    if market_regime == "CHOP_NO_TRADE":
        late_score += 3
        late_reasons.append("CHOP_NO_TRADE")
    if market_regime == "EXHAUSTION_REVERSAL":
        late_score += 3
        late_reasons.append("EXHAUSTION_REVERSAL")
    if exhaustion not in ("", "UNKNOWN", "HEALTHY", "NONE", "NO", "FALSE"):
        late_score += 2
        late_reasons.append(f"exhaustion={exhaustion}")
    if bos_quality in {"NO_FOLLOWTHROUGH", "TRAP"}:
        late_score += 2
        late_reasons.append(f"bos_quality={bos_quality}")
    if risk_class in {"BAD_REGIME_ENTRY", "CHOP_OR_RANGE_ENTRY", "STALE_SIGNAL_ENTRY", "NO_FOLLOWTHROUGH_RISK"}:
        late_score += 2
        late_reasons.append(f"risk_class={risk_class}")

    structure_score = 0
    if not missing:
        if side == "LONG" and sl < entry < tp:
            structure_score += 1
        elif side == "SHORT" and tp < entry < sl:
            structure_score += 1
        else:
            structure_score -= 2
        if rr is not None and rr >= 2.0:
            structure_score += 1
        elif rr is not None:
            structure_score -= 1

    if missing:
        status = "UNKNOWN_MISSING_FEATURES"
        reason = "missing_core_features=" + ",".join(missing)
    elif market_regime == "CHOP_NO_TRADE":
        status = "WOULD_SKIP_CHOP"
        reason = "market_regime=CHOP_NO_TRADE"
    elif market_regime == "EXHAUSTION_REVERSAL":
        status = "WOULD_SKIP_EXHAUSTION"
        reason = "market_regime=EXHAUSTION_REVERSAL"
    elif side == "LONG" and smc_zone == "PREMIUM" and late_score >= 2:
        status = "WOULD_SKIP_BAD_LOCATION"
        reason = "long_in_premium_with_late_or_weak_context"
    elif side == "SHORT" and smc_zone == "DISCOUNT" and late_score >= 2:
        status = "WOULD_SKIP_BAD_LOCATION"
        reason = "short_in_discount_with_late_or_weak_context"
    elif late_score >= 4:
        status = "WOULD_SKIP_LATE_CHASE"
        reason = ";".join(late_reasons) or "late_chase_proxy"
    elif structure_score < 1:
        status = "WOULD_SKIP_NO_STRUCTURE_SL"
        reason = "structure_proxy_failed"
    elif rr is None or rr < 2.0:
        status = "WOULD_SKIP_RR"
        reason = f"expected_rr={rr}"
    elif location_score >= 2 and has_retest_proxy:
        status = "WOULD_ENTER"
        reason = "location_and_retest_proxy_confirmed"
    else:
        status = "WAIT_RETEST"
        reason = "needs_retest_or_location_confirmation"

    return {
        "v2_shadow_status": status,
        "v2_shadow_reason": reason,
        "v2_location_score": location_score,
        "v2_late_chase_score": late_score,
        "v2_structure_score": structure_score,
        "v2_expected_rr": rr,
        "v2_feature_quality": feature_quality,
        "smc_zone": smc_zone,
        "market_regime": market_regime,
        "bos_quality": bos_quality,
    }


def perf(rows):
    values = [row["rr"] for row in rows if row.get("rr") is not None]
    wins = [rr for rr in values if rr > 0]
    losses = [rr for rr in values if rr < 0]
    gross_win = sum(wins)
    gross_loss = -sum(losses)
    immediate = sum(1 for row in rows if row.get("immediate_sl"))
    return {
        "n": len(values),
        "net_r": sum(values),
        "avg_r": sum(values) / len(values) if values else None,
        "win_rate": len(wins) / len(values) if values else None,
        "pf": gross_win / gross_loss if gross_loss > 0 else None,
        "immediate_sl_rate": immediate / len(values) if values else None,
    }


def fmt(value, digits=2, signed=False):
    if value is None:
        return "NA"
    spec = f"+.{digits}f" if signed else f".{digits}f"
    return format(value, spec)


def pct(value):
    return "NA" if value is None else f"{value * 100:.1f}%"


def recommendation(base, kept, skipped, delta, volume_retained):
    affected_n = len(skipped)
    pf_improves = kept["pf"] is not None and base["pf"] is not None and kept["pf"] > base["pf"]
    immediate_drops = (
        kept["immediate_sl_rate"] is not None
        and base["immediate_sl_rate"] is not None
        and kept["immediate_sl_rate"] < base["immediate_sl_rate"]
    )
    if volume_retained is not None and volume_retained < 0.15 and kept["net_r"] <= 0:
        return "KILL_RULE"
    if volume_retained is not None and volume_retained < 0.20:
        return "NEED_EXACT_FEATURES"
    if affected_n >= 30 and delta >= 5.0 and pf_improves and immediate_drops:
        return "PROMOTE_TO_PAPER_GATE"
    if affected_n < 30:
        return "KEEP_SHADOW_UNTIL_50"
    if delta <= 0:
        return "KILL_RULE"
    return "NEED_EXACT_FEATURES"


def main():
    contexts = load_contexts()
    rows = []
    for trade in read_trades():
        trade_id = str(trade.get("id"))
        rr = _f(trade.get("rr"))
        mfe = _f(trade.get("max_r"))
        shadow = v2_shadow_for_trade(trade, contexts.get(trade_id, {}))
        rows.append({
            "trade_id": trade_id,
            "symbol": trade.get("symbol"),
            "side": _s(trade.get("side")),
            "rr": rr,
            "status": _s(trade.get("status")),
            "exit_type": _s(trade.get("exit_type")),
            "immediate_sl": bool(
                (mfe is not None and mfe < IMMEDIATE_MFE_R)
                or (_s(trade.get("status")) == "LOSE" and _s(trade.get("exit_type")) == "SL")
            ),
            **shadow,
        })

    allowed = [row for row in rows if row.get("v2_shadow_status") == "WOULD_ENTER"]
    skipped = [row for row in rows if row.get("v2_shadow_status") != "WOULD_ENTER"]
    base_perf = perf(rows)
    allowed_perf = perf(allowed)
    skipped_perf = perf(skipped)
    saved_losses = -sum(row["rr"] for row in skipped if row.get("rr") is not None and row["rr"] < 0)
    missed_winners = sum(row["rr"] for row in skipped if row.get("rr") is not None and row["rr"] > 0)
    delta = allowed_perf["net_r"] - base_perf["net_r"]
    volume_retained = allowed_perf["n"] / base_perf["n"] if base_perf["n"] else None
    rec = recommendation(base_perf, allowed_perf, skipped, delta, volume_retained)

    status_counts = Counter(row["v2_shadow_status"] for row in rows)
    reason_counts = Counter(row["v2_shadow_reason"] for row in skipped)

    verdict = "PASS" if rows else "FAIL"
    if rec in {"NEED_EXACT_FEATURES", "KEEP_SHADOW_UNTIL_50"}:
        verdict = "WARN"
    if rec == "KILL_RULE":
        verdict = "FAIL"

    print(f"1. {verdict}")
    print("\nHistorical V1 vs V2 table")
    print("bucket                     n     net_R   avg_R     WR      PF  immediate_SL")
    for name, data in (
        ("V1_ALL", base_perf),
        ("V2_WOULD_SKIP", skipped_perf),
        ("V2_WOULD_ENTER", allowed_perf),
    ):
        print(
            f"{name:<22}{data['n']:>5}{fmt(data['net_r']):>10}{fmt(data['avg_r'], 3):>8}"
            f"{pct(data['win_rate']):>8}{fmt(data['pf']):>8}{pct(data['immediate_sl_rate']):>14}"
        )

    print("\nTop V2 skip reasons")
    for reason, count in reason_counts.most_common(10):
        print(f"{reason}: {count}")

    print("\nV2 status counts")
    for status, count in status_counts.most_common():
        print(f"{status}: {count}")

    print("\nEconomics")
    print(f"saved losses: {fmt(saved_losses)}")
    print(f"missed winners: {fmt(missed_winners)}")
    print(f"estimated net R delta: {fmt(delta, signed=True)}")
    print(f"PF before/after estimate: {fmt(base_perf['pf'])}/{fmt(allowed_perf['pf'])}")
    print(f"immediate SL before/after: {pct(base_perf['immediate_sl_rate'])}/{pct(allowed_perf['immediate_sl_rate'])}")
    print(f"volume retained %: {pct(volume_retained)}")
    print(f"Recommendation: {rec}")
    print("No live/testnet orders touched. No decision behavior changed by this audit.")


if __name__ == "__main__":
    main()
