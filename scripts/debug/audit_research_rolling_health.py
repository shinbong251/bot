#!/usr/bin/env python3
"""Rolling health audit for CONFIRM_SMC_RESEARCH paper and live research."""

import json
import math
import time
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"
PAPER_LIFECYCLE = LOG_DIR / "paper_smc_research_lifecycle.jsonl"
PAPER_GAP_SHADOW = LOG_DIR / "paper_smc_research_sl_gap_calibration_shadow.jsonl"
PAPER_MIN_LOCK = LOG_DIR / "paper_smc_research_min_lock_shadow.jsonl"
LIVE_DECISIONS = LOG_DIR / "live_smc_research_decisions.jsonl"
LIVE_MIN_LOCK = LOG_DIR / "live_smc_research_min_lock_075_events.jsonl"
LIVE_STATE = ROOT / "live_state.json"
SUMMARY_LOG = LOG_DIR / "research_rolling_health.jsonl"
CONFIG_JSON = ROOT / "config.json"


def read_jsonl(path):
    if not path.exists():
        return []
    rows = []
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


def read_json(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def write_jsonl(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str) + "\n")


def fnum(value, default=None):
    try:
        if value in (None, ""):
            return default
        out = float(value)
        if math.isnan(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def fmt(value):
    if value is None:
        return "n/a"
    if value == float("inf"):
        return "inf"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def row_key(row):
    for field in ("research_join_key", "research_dedup_key", "dedup_key"):
        value = str(row.get(field) or "").strip()
        if value:
            return value
    if row.get("trade_id") not in (None, ""):
        return str(row.get("trade_id"))
    parts = [row.get("symbol"), row.get("side"), row.get("source_timestamp"), row.get("signal_ts")]
    return "|".join(str(part or "") for part in parts)


def row_ts(row):
    for field in ("close_ts", "closed_at_unix", "ts", "observed_at_unix", "time", "entry_time"):
        value = fnum(row.get(field))
        if value is not None:
            return value
    return 0.0


def pf(values):
    gains = sum(v for v in values if v > 0)
    losses = -sum(v for v in values if v < 0)
    if losses <= 0:
        return None if gains <= 0 else float("inf")
    return gains / losses


def win_rate(values):
    return None if not values else sum(1 for v in values if v > 0) / len(values)


def max_loss_streak(values):
    best = 0
    cur = 0
    for value in values:
        if value < 0:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def max_drawdown(values):
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return abs(worst)


def compact_metrics(rows):
    values = [fnum(row.get("calibrated_r_cap_1_0")) for row in rows]
    values = [v for v in values if v is not None]
    return {
        "n": len(values),
        "net_r": round(sum(values), 4),
        "pf": pf(values),
        "win_rate": win_rate(values),
    }


def split_metrics(rows, field):
    buckets = defaultdict(list)
    for row in rows:
        raw = row.get(field)
        if raw in (None, "") and field in ("phase", "market_regime"):
            raw = (row.get("entry_context") or {}).get(field)
        label = str(raw or "UNKNOWN").upper()
        buckets[label].append(row)
    return {
        label: compact_metrics(bucket)
        for label, bucket in sorted(buckets.items(), key=lambda item: (-len(item[1]), item[0]))
    }


def metrics(rows):
    rows = [row for row in rows if fnum(row.get("raw_realized_r")) is not None]
    raw = [fnum(row.get("raw_realized_r")) for row in rows]
    cap_1_0 = [fnum(row.get("calibrated_r_cap_1_0")) for row in rows]
    cap_1_2 = [fnum(row.get("calibrated_r_cap_1_2")) for row in rows]
    raw = [value for value in raw if value is not None]
    cap_1_0 = [value for value in cap_1_0 if value is not None]
    cap_1_2 = [value for value in cap_1_2 if value is not None]
    return {
        "n": len(rows),
        "raw_net_r": round(sum(raw), 4),
        "raw_pf": pf(raw),
        "calibrated_net_r_cap_1_0": round(sum(cap_1_0), 4),
        "calibrated_pf_cap_1_0": pf(cap_1_0),
        "calibrated_net_r_cap_1_2": round(sum(cap_1_2), 4),
        "calibrated_pf_cap_1_2": pf(cap_1_2),
        "win_rate": win_rate(raw),
        "max_loss_streak": max_loss_streak(cap_1_0),
        "max_drawdown_r": round(max_drawdown(cap_1_0), 4),
        "gap_loss_count": sum(1 for row in rows if row.get("is_gap_loss")),
        "possible_overcharge_count": sum(1 for row in rows if row.get("possible_overcharge")),
        "side_split": split_metrics(rows, "side"),
        "phase_split": split_metrics(rows, "phase"),
        "regime_split": split_metrics(rows, "market_regime"),
    }


def paper_close_rows():
    seen = set()
    out = []
    for row in read_jsonl(PAPER_LIFECYCLE):
        candidates = []
        if row.get("event_type") in ("RESEARCH_CLOSED", "RESEARCH_CLOSE_MISSING_CONTEXT"):
            candidates.append(row)
        for child in row.get("closed_since_last_summary") or []:
            if isinstance(child, dict) and child.get("event_type") in (
                "RESEARCH_CLOSED",
                "RESEARCH_CLOSE_MISSING_CONTEXT",
            ):
                candidates.append(child)
        for candidate in candidates:
            if str(candidate.get("entry_type") or "").upper() not in ("", "CONFIRM_SMC_RESEARCH"):
                continue
            ident = row_key(candidate)
            if ident in seen:
                continue
            seen.add(ident)
            out.append(dict(candidate))
    out.sort(key=row_ts)
    return out


def shadow_index(path):
    indexed = {}
    for row in read_jsonl(path):
        indexed[row_key(row)] = row
        if row.get("trade_id") not in (None, ""):
            indexed[str(row.get("trade_id"))] = row
    return indexed


def enrich_paper_rows(rows, gap_shadow=None, min_lock_shadow=None):
    gap_shadow = gap_shadow if gap_shadow is not None else shadow_index(PAPER_GAP_SHADOW)
    min_lock_shadow = min_lock_shadow if min_lock_shadow is not None else shadow_index(PAPER_MIN_LOCK)
    min_lock_keys = {
        row_key(row)
        for row in min_lock_shadow.values()
        if isinstance(row, dict) and row_key(row)
    }
    out = []
    for source in rows:
        row = dict(source)
        joined = gap_shadow.get(row_key(row)) or gap_shadow.get(str(row.get("trade_id") or ""))
        if joined:
            row["_gap_shadow_joined"] = True
            for field in (
                "possible_overcharge",
                "configured_sl_gap_r",
                "execution_tier",
                "gap_minus_mae_r",
                "price_r",
                "expected_sl_r_with_gap",
            ):
                if field in joined and row.get(field) in (None, ""):
                    row[field] = joined[field]
        else:
            row["_gap_shadow_joined"] = False

        raw_r = fnum(row.get("raw_realized_r", row.get("r_multiple")))
        row["raw_realized_r"] = raw_r
        close_reason = str(row.get("close_reason") or row.get("exit_type") or "").upper()
        gap_r = fnum(row.get("configured_sl_gap_r", row.get("raw_sl_gap_r")))
        is_gap_loss = bool(row.get("is_gap_loss"))
        if not is_gap_loss:
            is_gap_loss = (
                close_reason == "SL"
                and gap_r is not None
                and gap_r > 0
                and raw_r is not None
                and raw_r < -1.0
            )
        if not is_gap_loss and close_reason == "SL" and raw_r is not None:
            is_gap_loss = abs(raw_r + 1.5) < 1e-9 or abs(raw_r + 1.3) < 1e-9
        row["is_gap_loss"] = is_gap_loss
        row["configured_sl_gap_r"] = gap_r
        row["calibrated_r_cap_1_0"] = (
            max(raw_r, -1.0)
            if is_gap_loss and raw_r is not None and raw_r <= -1.2
            else raw_r
        )
        row["calibrated_r_cap_1_2"] = (
            max(raw_r, -1.2)
            if is_gap_loss and raw_r is not None and raw_r <= -1.2
            else raw_r
        )
        entry_context = row.get("entry_context") if isinstance(row.get("entry_context"), dict) else {}
        row["phase"] = row.get("phase") or entry_context.get("phase")
        row["market_regime"] = row.get("market_regime") or entry_context.get("market_regime")
        row["since_min_lock_active"] = row_key(row) in min_lock_keys
        out.append(row)
    out.sort(key=row_ts)
    return out


def classify_paper(last20, last50):
    reasons = []
    if last50["n"] < 50:
        reasons.append(f"paper_last50_insufficient_n={last50['n']}")
    if last20["n"] < 20:
        reasons.append(f"paper_last20_insufficient_n={last20['n']}")

    last50_net = last50["calibrated_net_r_cap_1_0"]
    last50_pf = last50["calibrated_pf_cap_1_0"]
    last20_net = last20["calibrated_net_r_cap_1_0"]
    loss_streak = last50["max_loss_streak"]

    if last50["n"] == 0 or last20["n"] == 0:
        return "RED", reasons + ["paper_no_closed_data"]
    if last50_net <= 0:
        return "RED", reasons + [f"last50_calibrated_net_r_cap_1_0={last50_net}<=0"]
    if last50_pf is None or last50_pf < 1.0:
        return "RED", reasons + [f"last50_calibrated_pf_cap_1_0={fmt(last50_pf)}<1.00"]
    if last20_net <= -4.0:
        return "RED", reasons + [f"last20_calibrated_net_r_cap_1_0={last20_net}<=-4R"]
    if loss_streak >= 5:
        return "RED", reasons + [f"last50_max_loss_streak={loss_streak}>=5"]

    if last50_net > 0 and (last50_pf is None or last50_pf < 1.10):
        return "YELLOW", reasons + [f"last50_positive_but_pf={fmt(last50_pf)}<1.10"]
    if -4.0 < last20_net <= -2.0:
        return "YELLOW", reasons + [f"last20_calibrated_net_r_cap_1_0={last20_net}_between_-2R_and_-4R"]

    if last50_net > 0 and last50_pf >= 1.10 and last20_net > -2.0:
        return "GREEN", reasons + ["paper_last50_positive_pf_ge_1_10_last20_gt_-2R"]
    return "YELLOW", reasons + ["paper_health_default_yellow"]


def classify_active_paper(active_rows, min_active_closed):
    last20 = metrics(active_rows[-20:])
    last50 = metrics(active_rows[-50:])
    if len(active_rows) < min_active_closed:
        return (
            "INSUFFICIENT_DATA",
            [
                f"active_closed_n={len(active_rows)}<{min_active_closed}",
                "legacy_health_warning_only",
            ],
            last20,
            last50,
        )
    health, reasons = classify_paper(last20, last50)
    return health, reasons, last20, last50


def live_close_rows(decision_rows=None):
    rows = decision_rows if decision_rows is not None else read_jsonl(LIVE_DECISIONS)
    out = []
    for row in rows:
        if str(row.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        decision = str(row.get("decision") or "").upper()
        status = str(row.get("status") or "").upper()
        realized = fnum(
            row.get(
                "actual_realized_r",
                row.get("realized_r", row.get("rr_real", row.get("r_multiple"))),
            )
        )
        if realized is None:
            continue
        if "CLOSE" in decision or "CLOSED" in decision or status == "CLOSED" or row.get("close_reason"):
            item = dict(row)
            item["actual_realized_r"] = realized
            out.append(item)
    out.sort(key=row_ts)
    return out


def live_state_has_confirmed_research_sl(state=None):
    if state is None:
        state = read_json(LIVE_STATE)
    trades = state.get("trades") if isinstance(state, dict) else state
    if not isinstance(trades, list):
        return False
    for trade in trades:
        if str(trade.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        if str(trade.get("status") or "OPEN").upper() != "OPEN":
            continue
        if trade.get("exchange_sl_id") not in (None, "") and fnum(trade.get("exchange_sl_price_confirmed")) is not None:
            return True
    return False


def live_safety_issues(decision_rows=None, min_lock_rows=None, live_state=None):
    decision_rows = decision_rows if decision_rows is not None else read_jsonl(LIVE_DECISIONS)
    min_lock_rows = min_lock_rows if min_lock_rows is not None else read_jsonl(LIVE_MIN_LOCK)
    warnings = []
    critical = []
    sl_sync_ok = live_state_has_confirmed_research_sl(live_state)
    for row in decision_rows:
        if str(row.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        text = " ".join(str(row.get(field) or "") for field in ("decision", "reason", "prefilter_reason", "prefilter_detail", "fail_reason"))
        lower = text.lower()
        if "reconcile" in lower:
            warnings.append("live_reconcile_warning")
        if "unmanaged" in lower or "missing exchange" in lower or "local state" in lower:
            critical.append("live_exchange_local_state_consistency_issue")
        if ("sl sync" in lower or "sl_sync" in lower) and not sl_sync_ok:
            critical.append("live_sl_sync_failure")
    for row in min_lock_rows:
        if str(row.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        reason = str(row.get("reason") or "")
        sync_result = str(row.get("sync_result") or "")
        if ("FAILED" in reason.upper() or sync_result.lower() == "false") and not sl_sync_ok:
            critical.append("live_sl_sync_failure")
    if sl_sync_ok:
        warnings.append("SL_SYNC_OK")
    return sorted(set(warnings)), sorted(set(critical))


def classify_live(close_rows=None, decision_rows=None, min_lock_rows=None, live_state=None):
    closes = close_rows if close_rows is not None else live_close_rows(decision_rows)
    warnings, critical = live_safety_issues(decision_rows, min_lock_rows, live_state=live_state)
    values = [fnum(row.get("actual_realized_r")) for row in closes]
    values = [value for value in values if value is not None]
    reasons = []
    if critical:
        return "RED", critical, {"n": len(values), "net_r": round(sum(values), 4), "consecutive_losses": max_loss_streak(values)}
    if len(values) == 0:
        return "UNKNOWN", ["live_no_closed_research_trades"] + warnings, {"n": 0, "net_r": 0.0, "consecutive_losses": 0}
    live_net = round(sum(values), 4)
    consecutive_losses = 0
    for value in reversed(values):
        if value < 0:
            consecutive_losses += 1
        else:
            break
    if consecutive_losses >= 3:
        return "RED", [f"live_consecutive_losses={consecutive_losses}>=3"], {"n": len(values), "net_r": live_net, "consecutive_losses": consecutive_losses}
    if live_net <= -2.0:
        return "RED", [f"live_net_r={live_net}<=-2R"], {"n": len(values), "net_r": live_net, "consecutive_losses": consecutive_losses}
    if consecutive_losses in (1, 2):
        reasons.append(f"live_consecutive_losses={consecutive_losses}")
    if warnings:
        reasons.extend(warnings)
    if reasons:
        return "YELLOW", reasons, {"n": len(values), "net_r": live_net, "consecutive_losses": consecutive_losses}
    return "GREEN", ["live_closed_small_sample_no_critical_failure"], {"n": len(values), "net_r": live_net, "consecutive_losses": consecutive_losses}


def promotion_status(paper_health, live_health, last50, active_n=None, min_active_closed=20):
    if paper_health == "INSUFFICIENT_DATA" or (active_n is not None and active_n < min_active_closed):
        return "LIVE_MICRO_ONLY"
    if last50.get("n", 0) < min_active_closed or live_health == "UNKNOWN":
        return "LIVE_MICRO_ONLY"
    if paper_health != "GREEN":
        return "LIVE_SCALE_BLOCKED_PAPER_HEALTH"
    if live_health == "RED":
        return "LIVE_SCALE_BLOCKED_LIVE_HEALTH"
    return "PROMOTION_ALLOWED_MICRO_ONLY"


def build_summary(source="audit_script", write_summary=True):
    cfg = read_json(CONFIG_JSON)
    baseline_ts = fnum(cfg.get("research_health_baseline_ts"), 0.0) or 0.0
    min_active_closed = int(cfg.get("research_health_min_active_closed", 20) or 20)
    use_active_only = bool(cfg.get("research_health_use_active_only_for_live_scale", True))
    paper = enrich_paper_rows(paper_close_rows())
    active_paper = [row for row in paper if row_ts(row) >= baseline_ts]
    last20_rows = paper[-20:]
    last50_rows = paper[-50:]
    last100_rows = paper[-100:]
    since_min_lock_rows = [row for row in paper if row.get("since_min_lock_active")]
    last20 = metrics(last20_rows)
    last50 = metrics(last50_rows)
    last100 = metrics(last100_rows)
    since_min_lock = metrics(since_min_lock_rows)
    legacy_health, legacy_reasons = classify_paper(last20, last50)
    active_health, active_reasons, active_last20, active_last50 = classify_active_paper(
        active_paper,
        min_active_closed,
    )
    paper_health_for_scale = active_health if use_active_only else legacy_health
    live_health, live_reasons, live_metrics = classify_live()
    status = promotion_status(
        paper_health_for_scale,
        live_health,
        active_last50 if use_active_only else last50,
        active_n=len(active_paper) if use_active_only else last50.get("n"),
        min_active_closed=min_active_closed,
    )
    reasons = active_reasons + live_reasons
    if legacy_health == "RED":
        reasons.append("legacy_health_RED_warning_only")
    if status != "PROMOTION_ALLOWED_MICRO_ONLY":
        reasons.append(status)
    row = {
        "ts": time.time(),
        "source": source,
        "paper_health": paper_health_for_scale,
        "legacy_health": legacy_health,
        "paper_legacy_health": legacy_health,
        "paper_active_health": active_health,
        "live_health": live_health,
        "promotion_status": status,
        "research_health_baseline_ts": baseline_ts,
        "research_health_min_active_closed": min_active_closed,
        "research_health_use_active_only_for_live_scale": use_active_only,
        "last20": last20,
        "last50": last50,
        "last100": last100,
        "active_last20": active_last20,
        "active_last50": active_last50,
        "active_closed_count": len(active_paper),
        "since_min_lock_active": since_min_lock,
        "live_metrics": live_metrics,
        "max_live_research_trades": cfg.get("max_live_research_trades"),
        "reasons": reasons,
    }
    if write_summary:
        write_jsonl(SUMMARY_LOG, row)
    return row


def print_metrics(label, data):
    print(f"\n[{label}]")
    print(f"n={data['n']}")
    print(f"raw_net_r={fmt(data['raw_net_r'])} raw_pf={fmt(data['raw_pf'])} win_rate={fmt(data['win_rate'])}")
    print(
        "calibrated_cap_1_0 net_r={net} pf={pf}".format(
            net=fmt(data["calibrated_net_r_cap_1_0"]),
            pf=fmt(data["calibrated_pf_cap_1_0"]),
        )
    )
    print(
        "calibrated_cap_1_2 net_r={net} pf={pf}".format(
            net=fmt(data["calibrated_net_r_cap_1_2"]),
            pf=fmt(data["calibrated_pf_cap_1_2"]),
        )
    )
    print(f"max_loss_streak={data['max_loss_streak']} max_drawdown_r={fmt(data['max_drawdown_r'])}")
    print(f"gap_loss_count={data['gap_loss_count']} possible_overcharge_count={data['possible_overcharge_count']}")
    print(f"side_split={json.dumps(data['side_split'], sort_keys=True, default=str)}")
    print(f"phase_split={json.dumps(data['phase_split'], sort_keys=True, default=str)}")
    print(f"regime_split={json.dumps(data['regime_split'], sort_keys=True, default=str)}")


def main():
    row = build_summary()
    print("PASS rolling health audit completed")
    print(f"legacy_health={row['legacy_health']}")
    print(f"paper_active_health={row['paper_active_health']}")
    print(f"paper_health={row['paper_health']}")
    print(f"live_health={row['live_health']}")
    print(f"promotion_status={row['promotion_status']}")
    print(f"research_health_baseline_ts={row['research_health_baseline_ts']}")
    print(f"active_closed_count={row['active_closed_count']}")
    print(f"max_live_research_trades={row.get('max_live_research_trades')}")
    print("reasons=" + json.dumps(row["reasons"], sort_keys=True))
    print_metrics("ACTIVE LAST 20 CLOSED", row["active_last20"])
    print_metrics("ACTIVE LAST 50 CLOSED", row["active_last50"])
    print_metrics("LAST 20 CLOSED", row["last20"])
    print_metrics("LAST 50 CLOSED", row["last50"])
    print_metrics("LAST 100 CLOSED", row["last100"])
    print_metrics("SINCE MIN-LOCK ACTIVE", row["since_min_lock_active"])
    print(f"\nsummary_log={SUMMARY_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
