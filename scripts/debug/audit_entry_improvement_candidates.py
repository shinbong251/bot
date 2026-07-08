#!/usr/bin/env python3
"""ENTRY_IMPROVEMENT_AUDIT for CONFIRM_SMC_RESEARCH.

Read-only research audit. Uses existing trade CSVs and JSONL context logs to
classify executed CONFIRM_SMC_RESEARCH trades into actionable bad-entry buckets
and estimate filter economics. It does not import execution code, touch config
or state files, or place/cancel/modify orders.
"""

import bisect
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"

PAPER_TRADES = ROOT / "paper_trades.csv"
LIVE_TRADES = ROOT / "live_trades.csv"
PAPER_ENTRY_CONTEXT = LOG_DIR / "paper_smc_research_entry_context.jsonl"
QUALIFIED_DECISIONS = LOG_DIR / "paper_smc_research_qualified_decisions.jsonl"
SCAN_FEATURES = LOG_DIR / "scan_feature_snapshots.jsonl"

ENTRY_TYPE = "CONFIRM_SMC_RESEARCH"
IMMEDIATE_MFE_R = 0.50
MIN_SAMPLE_FOR_PROMOTE = 8

TREND_REGIMES = {
    "TREND",
    "TREND_CONTINUATION",
    "BREAKOUT_TREND",
    "PULLBACK_CONTINUATION",
    "TREND_PULLBACK",
}
RANGE_REGIMES = {"CHOP_NO_TRADE", "RANGE_MEAN_REVERSION", "RANGE", "CHOP"}
BAD_BOS = {"NO_FOLLOWTHROUGH", "TRAP"}


def _f(value):
    try:
        if value in (None, "", "None"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _s(value):
    return str(value or "").strip().upper()


def _dt_from_id_ms(value):
    ts = _f(value)
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        return None


def _fmt_date(dt):
    return dt.strftime("%Y-%m-%d %H:%M UTC") if dt else "UNKNOWN"


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


def read_trades(path):
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if _s(row.get("entry_type")) == ENTRY_TYPE:
                rows.append(row)
    return rows


def load_entry_contexts():
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


def load_scan_features(symbols):
    by_symbol = defaultdict(list)
    if not SCAN_FEATURES.exists():
        return by_symbol
    symbols = set(symbols)
    for row in read_jsonl(SCAN_FEATURES) or []:
        symbol = row.get("symbol")
        if symbol not in symbols:
            continue
        ts = _f(row.get("m5_last_ts") or row.get("scan_id") or row.get("observed_at"))
        if ts is None:
            continue
        by_symbol[symbol].append((ts, row))
    for symbol in list(by_symbol):
        by_symbol[symbol].sort(key=lambda item: item[0])
    return by_symbol


def nearest_snapshot(scan_by_symbol, symbol, ts):
    rows = scan_by_symbol.get(symbol) or []
    if not rows or ts is None:
        return None
    keys = [item[0] for item in rows]
    idx = bisect.bisect_right(keys, ts) - 1
    if idx < 0:
        return None
    snap_ts, row = rows[idx]
    if ts - snap_ts > 1800:
        return None
    return row


def avg_range(m5_recent):
    highs = m5_recent.get("highs") if isinstance(m5_recent, dict) else None
    lows = m5_recent.get("lows") if isinstance(m5_recent, dict) else None
    if not highs or not lows or len(highs) != len(lows):
        return None
    ranges = []
    for high, low in zip(highs, lows):
        high_f = _f(high)
        low_f = _f(low)
        if high_f is not None and low_f is not None and high_f >= low_f:
            ranges.append(high_f - low_f)
    return sum(ranges) / len(ranges) if ranges else None


def structure_sl_flags(trade, ctx, snapshot):
    entry = _f(ctx.get("entry")) or _f(trade.get("entry"))
    sl = _f(ctx.get("sl")) or _f(trade.get("sl"))
    side = _s(trade.get("side"))
    if entry is None or sl is None or not snapshot:
        return None, None

    m5 = snapshot.get("m5_recent")
    highs = m5.get("highs") if isinstance(m5, dict) else None
    lows = m5.get("lows") if isinstance(m5, dict) else None
    if not highs or not lows:
        return None, None

    highs_f = [_f(x) for x in highs]
    lows_f = [_f(x) for x in lows]
    highs_f = [x for x in highs_f if x is not None]
    lows_f = [x for x in lows_f if x is not None]
    atr = avg_range(m5)
    risk = abs(entry - sl)
    if not highs_f or not lows_f or risk <= 0:
        return None, None

    buffer = (atr or 0) * 0.25
    too_tight = bool(atr is not None and risk < atr * 0.80)
    if side == "LONG":
        not_behind = sl > (min(lows_f) - buffer)
    elif side == "SHORT":
        not_behind = sl < (max(highs_f) + buffer)
    else:
        not_behind = None
    return not_behind, too_tight


def join_executed_trades():
    paper_raw = read_trades(PAPER_TRADES)
    live_raw = read_trades(LIVE_TRADES)
    contexts = load_entry_contexts()
    symbols = {row.get("symbol") for row in paper_raw if row.get("symbol")}
    snapshots = load_scan_features(symbols)

    paper = []
    missing_context = 0
    missing_snapshot = 0
    for trade in paper_raw:
        trade_id = str(trade.get("id"))
        ctx = contexts.get(trade_id) or {}
        if not ctx:
            missing_context += 1
        source_ts = _f(ctx.get("source_timestamp") or ctx.get("signal_created_ts") or trade.get("signal_created_ts"))
        snap = nearest_snapshot(snapshots, trade.get("symbol"), source_ts)
        if snap is None:
            missing_snapshot += 1
        sl_not_behind, sl_too_tight = structure_sl_flags(trade, ctx, snap)
        mfe = _f(trade.get("max_r"))
        rr = _f(trade.get("rr"))
        row = {
            "venue": "paper",
            "trade_id": trade_id,
            "symbol": trade.get("symbol"),
            "side": _s(trade.get("side")),
            "rr": rr,
            "mfe_r": mfe,
            "status": _s(trade.get("status")),
            "exit_type": _s(trade.get("exit_type")),
            "open_dt": _dt_from_id_ms(trade_id),
            "entry": _f(trade.get("entry")),
            "sl": _f(trade.get("sl")),
            "risk_class": _s(ctx.get("research_entry_timing_risk_class")),
            "risk_reason": ctx.get("research_entry_timing_risk_reason"),
            "market_regime": _s(ctx.get("market_regime") or ctx.get("router_regime")),
            "market_state": _s(ctx.get("market_state")),
            "exhaustion_state": _s(ctx.get("exhaustion_state") or ctx.get("exhaustion")),
            "bos_quality": _s(ctx.get("bos_quality")),
            "displacement_quality": _s(ctx.get("displacement_quality")),
            "volume_confirmation": _s(ctx.get("volume_confirmation")),
            "phase": _s(ctx.get("phase")),
            "dow_trend_context": _s(ctx.get("dow_trend_context")),
            "dow_phase": _s(ctx.get("dow_phase")),
            "smc_zone": _s(ctx.get("smc_zone")),
            "premium_discount": _s(ctx.get("premium_discount")),
            "range_context": _s(ctx.get("range_context")),
            "trend_direction": _s(ctx.get("trend_direction")),
            "smc_bias": _s(ctx.get("smc_bias")),
            "liquidity_context": _s(ctx.get("liquidity_context")),
            "liquidity_sweep": _s(ctx.get("liquidity_sweep") or (snap or {}).get("liquidity_context")),
            "planned_rr": _f(ctx.get("planned_rr")),
            "score_v2_structural_shadow": _f(ctx.get("score_v2_structural_shadow")),
            "entry_location_would_block": ctx.get("confirm_smc_entry_location_would_block"),
            "entry_location_risk_bucket": _s(ctx.get("confirm_smc_entry_location_risk_bucket")),
            "entry_location_primary_reason": _s(ctx.get("confirm_smc_entry_location_primary_reason")),
            "entry_location_risk_score": _f(ctx.get("confirm_smc_entry_location_risk_score")),
            "pre_break_context": ctx.get("pre_break_context"),
            "signal_age_secs": _f(ctx.get("stage3_register_to_eval_secs") or ctx.get("total_structural_origin_to_open_secs")),
            "total_structural_origin_to_open_secs": _f(ctx.get("total_structural_origin_to_open_secs")),
            "source_timestamp": source_ts,
            "sl_not_behind_recent_m5": sl_not_behind,
            "sl_too_tight_vs_m5_noise": sl_too_tight,
            "immediate_sl": bool((mfe is not None and mfe < IMMEDIATE_MFE_R) or (_s(trade.get("status")) == "LOSE" and _s(trade.get("exit_type")) == "SL")),
            "has_entry_context": bool(ctx),
            "has_snapshot": snap is not None,
        }
        paper.append(row)

    live = []
    for trade in live_raw:
        mfe = _f(trade.get("max_r"))
        live.append({
            "venue": "live",
            "trade_id": str(trade.get("id")),
            "symbol": trade.get("symbol"),
            "side": _s(trade.get("side")),
            "rr": _f(trade.get("rr")),
            "mfe_r": mfe,
            "status": _s(trade.get("status")),
            "exit_type": _s(trade.get("exit_type")),
            "open_dt": _dt_from_id_ms(trade.get("id")),
            "immediate_sl": bool((mfe is not None and mfe < IMMEDIATE_MFE_R) or (_s(trade.get("status")) == "LOSE" and _s(trade.get("exit_type")) == "SL")),
        })
    return paper, live, missing_context, missing_snapshot


def pf(rows):
    gross_win = sum((row.get("rr") or 0.0) for row in rows if row.get("rr") is not None and row["rr"] > 0)
    gross_loss = -sum((row.get("rr") or 0.0) for row in rows if row.get("rr") is not None and row["rr"] < 0)
    if gross_loss > 0:
        return gross_win / gross_loss
    if gross_win > 0:
        return float("inf")
    return None


def perf(rows):
    rows = [row for row in rows if row.get("rr") is not None]
    n = len(rows)
    net = sum(row["rr"] for row in rows)
    wins = sum(1 for row in rows if row["rr"] > 0)
    immediate = sum(1 for row in rows if row.get("immediate_sl"))
    return {
        "n": n,
        "net_r": net,
        "avg_r": net / n if n else None,
        "win_rate": wins / n if n else None,
        "pf": pf(rows),
        "immediate_sl_rate": immediate / n if n else None,
    }


def fmt_num(value, digits=2, signed=False):
    if value is None:
        return "NA"
    if value == float("inf"):
        return "INF"
    spec = f"+.{digits}f" if signed else f".{digits}f"
    return format(value, spec)


def fmt_pct(value):
    return "NA" if value is None else f"{value * 100:.1f}%"


def with_dow(row):
    side = row.get("side")
    dow = row.get("dow_trend_context")
    trend = row.get("trend_direction")
    bias = row.get("smc_bias")
    if dow in {"UP", "BULLISH", "LONG"}:
        return side == "LONG"
    if dow in {"DOWN", "BEARISH", "SHORT"}:
        return side == "SHORT"
    if trend in {"LONG", "SHORT"}:
        return side == trend
    if bias in {"BULLISH", "BEARISH"}:
        return (side == "LONG" and bias == "BULLISH") or (side == "SHORT" and bias == "BEARISH")
    return None


def favorable_pd(row):
    side = row.get("side")
    zone = row.get("smc_zone")
    pd = row.get("premium_discount")
    range_context = row.get("range_context")
    if side == "LONG":
        return zone == "DISCOUNT" or pd == "DISCOUNT" or range_context == "RANGE_LOW"
    if side == "SHORT":
        return zone == "PREMIUM" or pd == "PREMIUM" or range_context == "RANGE_HIGH"
    return False


def bad_pd(row):
    side = row.get("side")
    zone = row.get("smc_zone")
    pd = row.get("premium_discount")
    if side == "LONG":
        return zone == "PREMIUM" or pd == "PREMIUM"
    if side == "SHORT":
        return zone == "DISCOUNT" or pd == "DISCOUNT"
    return False


def opposing_liquidity(row):
    side = row.get("side")
    liq = row.get("liquidity_context")
    sweep = row.get("liquidity_sweep")
    if liq in {"OPPOSING", "INTO_POOL", "OPPOSING_POOL"}:
        return True
    if side == "LONG" and sweep in {"SWEEP_HIGH", "HIGH", "SELL_SIDE_ABOVE"}:
        return True
    if side == "SHORT" and sweep in {"SWEEP_LOW", "LOW", "BUY_SIDE_BELOW"}:
        return True
    return False


def late_chase(row):
    age = row.get("signal_age_secs")
    return (
        row.get("risk_class") in {"BAD_REGIME_ENTRY", "STALE_SIGNAL_ENTRY", "NO_FOLLOWTHROUGH_RISK"}
        or row.get("market_regime") == "EXHAUSTION_REVERSAL"
        or row.get("exhaustion_state") in {"EXHAUSTED", "COLLAPSING"}
        or (row.get("bos_quality") in BAD_BOS and row.get("phase", "").startswith("BREAKOUT"))
        or (age is not None and age > 900)
    )


def against_dow_or_range(row):
    dow_ok = with_dow(row)
    return (
        row.get("market_regime") in RANGE_REGIMES
        or row.get("market_state") in {"RANGE", "CHOP"}
        or row.get("dow_phase") in {"RANGE", "CHOP"}
        or dow_ok is False
    )


def no_retest_or_late_retest_proxy(row):
    age = row.get("signal_age_secs")
    return (
        row.get("pre_break_context") is False
        and row.get("phase", "").startswith("BREAKOUT")
        and (
            row.get("bos_quality") in BAD_BOS
            or row.get("risk_class") == "STALE_SIGNAL_ENTRY"
            or (age is not None and age > 600)
        )
    )


RULES = [
    (
        "SKIP_LATE_CHASE_PROXY",
        late_chase,
        "Late/end-of-wave proxy: stale signal, exhaustion regime/state, no-followthrough/TRAP breakout, or signal age > 900s.",
    ),
    (
        "SKIP_BAD_PREMIUM_DISCOUNT",
        bad_pd,
        "Location proxy: LONG in premium or SHORT in discount from entry-context premium/discount fields.",
    ),
    (
        "SKIP_RANGE_OR_AGAINST_DOW",
        against_dow_or_range,
        "Structure proxy: RANGE/CHOP regime/state or direction against resolved Dow/trend/bias context.",
    ),
    (
        "SKIP_NO_OR_LATE_RETEST_PROXY",
        no_retest_or_late_retest_proxy,
        "Retest proxy: post-breakout entry with no pre-break context plus stale/no-followthrough/TRAP evidence.",
    ),
    (
        "SKIP_SL_NOT_BEHIND_M5_STRUCTURE",
        lambda row: row.get("sl_not_behind_recent_m5") is True or row.get("sl_too_tight_vs_m5_noise") is True,
        "SL geometry proxy: SL not beyond recent M5 swing extreme with ATR buffer, or risk < 0.8x recent M5 average range.",
    ),
    (
        "SKIP_INTO_OPPOSING_LIQUIDITY",
        opposing_liquidity,
        "Liquidity proxy: LONG into sweep-high/opposing pool or SHORT into sweep-low/opposing pool.",
    ),
    (
        "SKIP_EXISTING_LOCATION_BLOCK_SHADOW",
        lambda row: row.get("entry_location_would_block") is True,
        "Existing shadow location gate says this entry would block; uses logged shadow-only decision.",
    ),
]


def evaluate_rule(rows, name, pred, reason, base_perf):
    affected = [row for row in rows if row.get("rr") is not None and pred(row)]
    kept = [row for row in rows if row.get("rr") is not None and not pred(row)]
    affected_perf = perf(affected)
    kept_perf = perf(kept)
    affected_net = affected_perf["net_r"]
    saved_losses = -sum(row["rr"] for row in affected if row["rr"] < 0)
    missed_winners = sum(row["rr"] for row in affected if row["rr"] > 0)
    delta = kept_perf["net_r"] - base_perf["net_r"]
    if affected_perf["n"] == 0:
        rec = "NEED_DATA"
        rec_reason = "No affected executed trades in current dataset."
    elif affected_perf["n"] < MIN_SAMPLE_FOR_PROMOTE:
        rec = "NEED_DATA"
        rec_reason = f"Only {affected_perf['n']} affected trades; below sample floor {MIN_SAMPLE_FOR_PROMOTE}."
    elif delta > 3.0 and saved_losses > missed_winners:
        rec = "PROMOTE_SHADOW"
        rec_reason = "Filtering would have improved realized net R and saved-loss R exceeds missed-winner R."
    elif delta > 0:
        rec = "NEED_DATA"
        rec_reason = "Positive realized filter delta, but edge is not strong enough for promotion."
    else:
        rec = "REJECT"
        rec_reason = "Filtering would not have improved realized net R."
    return {
        "rule_name": name,
        "affected_n": affected_perf["n"],
        "affected_net_R": affected_net,
        "avg_R": affected_perf["avg_r"],
        "win_rate": affected_perf["win_rate"],
        "PF": affected_perf["pf"],
        "immediate_SL_rate": affected_perf["immediate_sl_rate"],
        "kept_net_R": kept_perf["net_r"],
        "estimated_delta_R": delta,
        "saved_losses": saved_losses,
        "missed_winners": missed_winners,
        "PF_before": base_perf["pf"],
        "PF_after": kept_perf["pf"],
        "recommendation": rec,
        "reason": reason,
        "exact_reason": rec_reason,
    }


def skip_reason(row):
    decision = row.get("decision")
    reason = row.get("reason")
    sub = row.get("qualified_reject_subreason")
    text = " ".join(str(x or "") for x in (reason, sub, row.get("v2_reason"), row.get("original_research_reason"))).lower()
    if decision in {"MAX_OPEN_REACHED", "CAP_REACHED"}:
        return "cap_full"
    if "rr_below" in text or reason == "rr_below_2":
        return "rr_below_min"
    if "bos_quality=weak" in text or reason == "bos_weak" or _s(row.get("bos_quality")) == "WEAK":
        return "bos_quality=WEAK"
    if "volume_confirmation=expansion" in text or _s(row.get("volume_confirmation")) == "EXPANSION":
        return "volume_confirmation=EXPANSION"
    if sub == "STALE_SIGNAL" or "stale" in text:
        return "late_signal_generated"
    if "health" in text or "pause" in text or "runtime" in text:
        return "health_block"
    if "min_notional" in text:
        return "min_notional"
    if "missing" in text or row.get("confirm_smc_entry_location_low_confidence") is True:
        return "missing_geometry"
    if "score" in text or "low_score" in text:
        return "score/filter"
    if reason == "qualified_reject":
        return "research_predicate_fail"
    return reason or sub or "other"


def missed_good_setup(row):
    if row.get("decision") == "OPEN":
        return False
    rr = _f(row.get("planned_rr"))
    side = _s(row.get("side"))
    if rr is None or rr < 2.0 or side not in {"LONG", "SHORT"}:
        return False
    normalized = {
        "side": side,
        "trend_direction": _s(row.get("trend_direction")),
        "smc_bias": _s(row.get("smc_bias")),
        "dow_trend_context": _s(row.get("dow_trend_context")),
        "smc_zone": _s(row.get("smc_zone")),
        "premium_discount": _s(row.get("premium_discount")),
        "range_context": _s(row.get("range_context")),
    }
    if with_dow(normalized) is not True:
        return False
    if not favorable_pd(normalized):
        return False
    bos = _s(row.get("bos_quality"))
    if bos in {"WEAK", "TRAP", "NO_FOLLOWTHROUGH"}:
        return False
    if row.get("confirm_smc_entry_location_would_block") is True:
        return False
    regime = _s(row.get("market_regime"))
    if regime and regime not in TREND_REGIMES and regime != "UNKNOWN":
        return False
    near_retest_or_liq = (
        row.get("pre_break_context") is True
        or _s(row.get("phase")) in {"PRE_BREAK_HIGH", "PRE_BREAK_LOW", "PULLBACK", "RETEST"}
        or _s(row.get("liquidity_sweep")) in {"SWEEP_LOW", "SWEEP_HIGH"}
        or _s(row.get("bos_confirmation")) == "RETESTED"
    )
    return bool(near_retest_or_liq)


def audit_missed_good_setups():
    total = 0
    skipped = 0
    good = 0
    reasons = Counter()
    if not QUALIFIED_DECISIONS.exists():
        return total, skipped, good, reasons
    for row in read_jsonl(QUALIFIED_DECISIONS) or []:
        total += 1
        if row.get("decision") == "OPEN":
            continue
        skipped += 1
        if missed_good_setup(row):
            good += 1
            reasons[skip_reason(row)] += 1
    return total, skipped, good, reasons


def missing_fields(rows):
    gaps = []
    if not any(row.get("signal_age_secs") is not None for row in rows):
        gaps.append("LATE_CHASE: bars_since_bos / structural_origin_age unavailable")
    else:
        gaps.append("LATE_CHASE: bars_since_bos and impulse-origin ATR move unavailable; using logged stale age/regime/BOS proxies")
    if not any(row.get("premium_discount") for row in rows):
        gaps.append("BAD_PREMIUM_DISCOUNT_LOCATION: premium_discount mostly unavailable; using smc_zone/range_context where present")
    if not any(row.get("dow_trend_context") for row in rows):
        gaps.append("AGAINST_DOW_OR_RANGE: dow_trend_context unavailable; using trend_direction/smc_bias and range regime proxies")
    gaps.append("NO_RETEST_OR_LATE_RETEST: exact first-retest counterfactual unavailable; using phase/pre_break/stale/no-followthrough proxy")
    if not any(row.get("has_snapshot") for row in rows):
        gaps.append("SL_NOT_BEHIND_STRUCTURE: scan_feature_snapshots unavailable; cannot test recent M5 structure")
    else:
        gaps.append("SL_NOT_BEHIND_STRUCTURE: valid swing/sweep/OB anchor unavailable; using recent M5 extreme + ATR buffer proxy")
    gaps.append("INTO_OPPOSING_LIQUIDITY: opposing liquidity distance in R unavailable; using sweep/opposing-context proxy")
    return gaps


def print_rule_table(results):
    header = (
        "rule_name",
        "affected_n",
        "affected_net_R",
        "avg_R",
        "win_rate",
        "PF",
        "immediate_SL",
        "kept_net_R",
        "estimated_delta_R",
        "saved_losses",
        "missed_winners",
        "PF_before/after",
        "recommendation",
    )
    print("\n5. Rule candidate table")
    print("{:<36}{:>11}{:>16}{:>9}{:>11}{:>8}{:>14}{:>13}{:>19}{:>14}{:>16}{:>18}  {}".format(*header))
    for row in results:
        pf_pair = f"{fmt_num(row['PF_before'])}/{fmt_num(row['PF_after'])}"
        print("{:<36}{:>11}{:>16}{:>9}{:>11}{:>8}{:>14}{:>13}{:>19}{:>14}{:>16}{:>18}  {}".format(
            row["rule_name"],
            row["affected_n"],
            fmt_num(row["affected_net_R"]),
            fmt_num(row["avg_R"], 3),
            fmt_pct(row["win_rate"]),
            fmt_num(row["PF"]),
            fmt_pct(row["immediate_SL_rate"]),
            fmt_num(row["kept_net_R"]),
            fmt_num(row["estimated_delta_R"], signed=True),
            fmt_num(row["saved_losses"]),
            fmt_num(row["missed_winners"]),
            pf_pair,
            row["recommendation"],
        ))
        print(f"  reason: {row['reason']} {row['exact_reason']}")


def main():
    paper, live, missing_context, missing_snapshot = join_executed_trades()
    all_dts = [row.get("open_dt") for row in paper + live if row.get("open_dt")]
    date_range = f"{_fmt_date(min(all_dts))} -> {_fmt_date(max(all_dts))}" if all_dts else "UNKNOWN"
    paper_perf = perf(paper)
    live_perf = perf(live)

    results = [evaluate_rule(paper, name, pred, reason, paper_perf) for name, pred, reason in RULES]
    results.sort(key=lambda row: (row["estimated_delta_R"], row["affected_n"]), reverse=True)
    damaging = sorted(
        [row for row in results if row["affected_n"] > 0],
        key=lambda row: row["affected_net_R"],
    )
    q_total, q_skipped, missed_good, missed_reasons = audit_missed_good_setups()

    best = None
    promote = [row for row in results if row["recommendation"] == "PROMOTE_SHADOW"]
    if promote:
        best = promote[0]
    else:
        positive = [row for row in results if row["estimated_delta_R"] > 0 and row["affected_n"] > 0]
        best = positive[0] if positive else (results[0] if results else None)

    verdict = "PASS"
    if any(row["recommendation"] == "PROMOTE_SHADOW" for row in results):
        verdict = "WARN"
    if not paper:
        verdict = "FAIL"

    print(f"1. {verdict}")
    if verdict == "FAIL":
        print("No paper CONFIRM_SMC_RESEARCH executed trades were found; cannot compute entry-improvement economics.")
    elif verdict == "WARN":
        print("At least one read-only rule candidate has positive enough realized filter economics for shadow promotion review.")
    else:
        print("No blocking issue proven; current data does not justify immediate filter promotion.")

    print("\n2. Dataset summary")
    print(f"paper trades n: {len(paper)} (with R: {paper_perf['n']}, missing entry_context: {missing_context}, missing M5 snapshot: {missing_snapshot})")
    print(f"live trades n: {len(live)} (with R: {live_perf['n']}; rich SMC context not present in live CSV)")
    print(f"date range: {date_range}")
    print(f"paper baseline: net_R={fmt_num(paper_perf['net_r'])} avg_R={fmt_num(paper_perf['avg_r'], 3)} win_rate={fmt_pct(paper_perf['win_rate'])} PF={fmt_num(paper_perf['pf'])} immediate_SL={fmt_pct(paper_perf['immediate_sl_rate'])}")
    print(f"live baseline: net_R={fmt_num(live_perf['net_r'])} avg_R={fmt_num(live_perf['avg_r'], 3)} win_rate={fmt_pct(live_perf['win_rate'])} PF={fmt_num(live_perf['pf'])} immediate_SL={fmt_pct(live_perf['immediate_sl_rate'])}")

    print("\n3. Top 5 bad-entry buckets by net R damage")
    if not damaging:
        print("none")
    for row in damaging[:5]:
        print(
            f"{row['rule_name']}: affected_n={row['affected_n']} "
            f"affected_net_R={fmt_num(row['affected_net_R'])} "
            f"avg_R={fmt_num(row['avg_R'], 3)} WR={fmt_pct(row['win_rate'])} "
            f"PF={fmt_num(row['PF'])} immediate_SL={fmt_pct(row['immediate_SL_rate'])} "
            f"filter_delta={fmt_num(row['estimated_delta_R'], signed=True)}"
        )

    print("\n4. Top 5 missed-good-setup reasons")
    print(f"qualified rows scanned: {q_total}, skipped rows: {q_skipped}, structurally-good skipped rows: {missed_good}")
    if not missed_reasons:
        print("none under strict no-lookahead definition")
    for reason, count in missed_reasons.most_common(5):
        print(f"{reason}: {count}")

    print_rule_table(results)

    print("\n6. Pick ONE best next shadow/filter candidate")
    if best:
        print(
            f"{best['rule_name']} -> {best['recommendation']}; "
            f"affected_n={best['affected_n']} delta_R={fmt_num(best['estimated_delta_R'], signed=True)}. "
            f"Exact reason: {best['exact_reason']}"
        )
    else:
        print("NEED_DATA: no candidate could be evaluated.")

    print("\n7. Missing fields / proxy limitations")
    for gap in missing_fields(paper):
        print(f"- {gap}")

    print("\n8. Live/testnet orders touched")
    print("Confirmed no live/testnet orders touched: script is read-only and imports no exchange/execution modules.")

    print("\n9. Commit/push")
    print("Not committed or pushed.")


if __name__ == "__main__":
    main()
