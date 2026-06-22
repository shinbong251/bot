#!/usr/bin/env python3
"""
Offline counterfactual reader for CONFIRM_SMC_RESEARCH_QUALIFIED decisions.

Read-only by default: loads the qualified decision JSONL, paper_trades.csv, and
optionally paper_state.json. It does not import runtime bot modules.
"""

import argparse
import csv
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path


RULE_NAMES = [
    "confirm_smc_entry_location_would_block",
    "entry_location_shadow_any",
]

ENTRY_LOCATION_SHADOW_FIELDS = [
    "confirm_smc_entry_location_would_block",
    "confirm_smc_entry_location_risk_score",
    "confirm_smc_entry_location_risk_bucket",
    "confirm_smc_entry_location_primary_reason",
    "confirm_smc_entry_location_risk_reasons",
    "confirm_smc_entry_location_low_confidence",
    "confirm_smc_entry_location_version",
]

BIAS_FIELDS = [
    "market_state",
    "market_regime",
    "smc_bias",
    "smc_zone",
    "trend_direction",
    "trend_strength",
    "phase",
    "impulse",
    "range_context",
    "liquidity_sweep",
    "invalid_context",
    "exhaustion",
    "regime_context_source",
    "regime_context_freshness",
    "regime_context_age_secs",
]

UNKNOWN_STRINGS = {"", "UNKNOWN", "NONE", "NULL", "N/A", "NA"}


def norm(value, default="UNKNOWN"):
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    return text.upper()


def is_known(value):
    return norm(value) not in UNKNOWN_STRINGS


def as_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().upper()
    if text in {"TRUE", "T", "YES", "Y", "1"}:
        return True
    if text in {"FALSE", "F", "NO", "N", "0"}:
        return False
    return None


def as_list(value):
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
        return [part.strip() for part in text.split("|") if part.strip()]
    return [value]


def ts_key(value):
    parsed = as_float(value)
    if parsed is None:
        return ""
    return f"{parsed:.6f}"


def parse_dedup_ts(dedup_key):
    text = str(dedup_key or "").strip()
    if not text:
        return ""
    parts = text.split("|")
    if len(parts) < 4:
        return ""
    return ts_key(parts[-1])


def row_sort_ts(row):
    for key in ("observed_at_unix", "source_timestamp", "signal_created_ts"):
        value = as_float(row.get(key))
        if value is not None:
            return value
    return -1.0


def stable_open_key(row):
    opened_trade_id = str(row.get("opened_trade_id") or "").strip()
    if opened_trade_id:
        return "id:" + opened_trade_id
    dedup_key = str(row.get("dedup_key") or "").strip()
    if dedup_key:
        return "dedup:" + dedup_key
    return "|".join(
        [
            "fallback",
            norm(row.get("symbol")),
            norm(row.get("side")),
            ts_key(row.get("source_timestamp") or row.get("signal_created_ts")),
        ]
    )


def format_num(value, digits=2):
    if value is None:
        return "NA"
    return f"{value:.{digits}f}"


def format_pct(numerator, denominator):
    if not denominator:
        return "NA"
    return f"{(100.0 * numerator / denominator):.1f}%"


def load_decisions(path):
    stats = Counter()
    rows = []
    if not path.exists():
        stats["missing"] = 1
        return rows, stats
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_no, line in enumerate(handle, start=1):
            stats["lines"] += 1
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                stats["malformed_jsonl_rows"] += 1
                continue
            if row.get("event_type") != "PAPER_SMC_RESEARCH_QUALIFIED_DECISION":
                stats["non_matching_event_type"] += 1
                continue
            row["_line_no"] = line_no
            rows.append(row)
    stats["decision_rows"] = len(rows)
    return rows, stats


def load_paper_trades(path):
    stats = Counter()
    trades = []
    by_id = {}
    by_signal = defaultdict(list)
    if not path.exists():
        stats["missing"] = 1
        return trades, by_id, by_signal, stats
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            stats["rows"] += 1
            trade_id = str(row.get("id") or "").strip()
            if trade_id:
                by_id[trade_id] = row
            symbol = norm(row.get("symbol"))
            side = norm(row.get("side"))
            signal_ts = ts_key(row.get("signal_created_ts"))
            if symbol and side and signal_ts:
                by_signal[(symbol, side, signal_ts)].append(row)
            trades.append(row)
    return trades, by_id, by_signal, stats


def load_open_state(path):
    stats = Counter()
    open_ids = set()
    open_dedup_keys = set()
    if not path or not path.exists():
        stats["missing"] = 1
        return open_ids, open_dedup_keys, stats
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        stats["read_errors"] += 1
        return open_ids, open_dedup_keys, stats
    if isinstance(data, dict):
        rows = data.get("open_trades") or data.get("trades") or data.get("positions") or []
    elif isinstance(data, list):
        rows = data
    else:
        rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        stats["rows"] += 1
        trade_id = str(row.get("id") or "").strip()
        if trade_id:
            open_ids.add(trade_id)
        for key in ("research_dedup_key", "dedup_key", "smc_main_dedup_key"):
            value = str(row.get(key) or "").strip()
            if value:
                open_dedup_keys.add(value)
    return open_ids, open_dedup_keys, stats


def is_closed_trade(row):
    status = norm(row.get("status"))
    if status == "OPEN":
        return False
    return as_float(row.get("rr")) is not None


def join_trade(open_row, by_id, by_signal):
    opened_trade_id = str(open_row.get("opened_trade_id") or "").strip()
    if opened_trade_id and opened_trade_id in by_id:
        return by_id[opened_trade_id], "opened_trade_id"

    symbol = norm(open_row.get("symbol"))
    side = norm(open_row.get("side"))
    candidate_ts = [
        ts_key(open_row.get("signal_created_ts")),
        ts_key(open_row.get("source_timestamp")),
        parse_dedup_ts(open_row.get("dedup_key")),
    ]
    seen_ts = set()
    for key_ts in candidate_ts:
        if not key_ts or key_ts in seen_ts:
            continue
        seen_ts.add(key_ts)
        matches = by_signal.get((symbol, side, key_ts), [])
        if matches:
            return matches[-1], "symbol_side_signal_ts"
    return None, "none"


def decision_open_rows(decisions):
    rows = [
        row
        for row in decisions
        if norm(row.get("decision")) == "OPEN" and norm(row.get("reason")) == "QUALIFIED_OPEN"
    ]
    latest = {}
    duplicates = 0
    for row in rows:
        key = stable_open_key(row)
        current = latest.get(key)
        if current is not None:
            duplicates += 1
        if current is None or row_sort_ts(row) >= row_sort_ts(current):
            latest[key] = row
    ordered = sorted(latest.values(), key=lambda item: (row_sort_ts(item), item.get("_line_no", 0)))
    return rows, ordered, duplicates


def read_entry_location_shadow(row):
    has_shadow = any(key in row for key in ENTRY_LOCATION_SHADOW_FIELDS)
    would_block = as_bool(row.get("confirm_smc_entry_location_would_block"))
    low_confidence = as_bool(row.get("confirm_smc_entry_location_low_confidence"))
    risk_score = as_float(row.get("confirm_smc_entry_location_risk_score"))
    risk_reasons = as_list(row.get("confirm_smc_entry_location_risk_reasons"))
    primary_reason = norm(row.get("confirm_smc_entry_location_primary_reason"))
    risk_bucket = norm(row.get("confirm_smc_entry_location_risk_bucket"))
    version = str(row.get("confirm_smc_entry_location_version") or "").strip()

    if not has_shadow:
        primary_reason = "NOT_EVALUATED"
        risk_bucket = "NOT_EVALUATED"
        version = "MISSING"
        would_block = False
        low_confidence = False
    else:
        if would_block is None:
            would_block = False
        if low_confidence is None:
            low_confidence = False
        if not primary_reason or primary_reason == "UNKNOWN":
            primary_reason = "NONE"
        if not risk_bucket or risk_bucket == "UNKNOWN":
            risk_bucket = "UNKNOWN"
        if not version:
            version = "UNKNOWN"

    return {
        "confirm_smc_entry_location_shadow_present": bool(has_shadow),
        "confirm_smc_entry_location_would_block": bool(would_block),
        "confirm_smc_entry_location_risk_score": risk_score,
        "confirm_smc_entry_location_risk_bucket": risk_bucket,
        "confirm_smc_entry_location_primary_reason": primary_reason,
        "confirm_smc_entry_location_risk_reasons": risk_reasons,
        "confirm_smc_entry_location_low_confidence": bool(low_confidence),
        "confirm_smc_entry_location_version": version,
        "entry_location_shadow_any": bool(would_block),
        "primary_entry_location_reason": primary_reason,
        "low_confidence": bool(low_confidence),
    }


def annotate_rows(open_rows, by_id, by_signal, open_ids, open_dedup_keys):
    annotated = []
    stats = Counter()
    for row in open_rows:
        item = dict(row)
        flags = read_entry_location_shadow(item)
        item.update(flags)
        trade, source = join_trade(item, by_id, by_signal)
        item["_join_source"] = source
        item["_trade_id"] = str(item.get("opened_trade_id") or "")
        item["_is_currently_open"] = False
        if item["_trade_id"] and item["_trade_id"] in open_ids:
            item["_is_currently_open"] = True
        dedup_key = str(item.get("dedup_key") or "").strip()
        if dedup_key and dedup_key in open_dedup_keys:
            item["_is_currently_open"] = True
        if trade and is_closed_trade(trade):
            item["_closed"] = True
            item["_trade"] = trade
            item["_trade_id"] = str(trade.get("id") or item["_trade_id"])
            item["_realized_R"] = as_float(trade.get("rr"))
            item["_result"] = norm(trade.get("status"))
            stats[f"join_{source}"] += 1
        else:
            item["_closed"] = False
            item["_trade"] = trade
            item["_realized_R"] = None
            item["_result"] = "OPEN" if item["_is_currently_open"] else "UNJOINED"
            if trade:
                stats["joined_not_closed"] += 1
            else:
                stats["unjoined"] += 1
        annotated.append(item)
    stats["open_or_unclosed_rows"] = sum(1 for row in annotated if not row.get("_closed"))
    stats["closed_joined_rows"] = sum(1 for row in annotated if row.get("_closed"))
    return annotated, stats


def summarize_performance(rows):
    closed = [row for row in rows if row.get("_closed")]
    r_values = [row["_realized_R"] for row in closed if row.get("_realized_R") is not None]
    wins = sum(1 for value in r_values if value > 0)
    losses = sum(1 for value in r_values if value < 0)
    be = sum(1 for value in r_values if value == 0)
    net_r = sum(r_values)
    return {
        "closed_count": len(r_values),
        "wins": wins,
        "losses": losses,
        "be": be,
        "net_R": net_r,
        "WR": wins / len(r_values) if r_values else None,
        "avg_R": net_r / len(r_values) if r_values else None,
    }


def rule_metrics(rows, rule_name):
    closed = [row for row in rows if row.get("_closed") and row.get("_realized_R") is not None]
    blocked = [row for row in rows if row.get(rule_name)]
    blocked_closed = [row for row in closed if row.get(rule_name)]
    passed_closed = [row for row in closed if not row.get(rule_name)]
    losses = [row for row in blocked_closed if row["_realized_R"] < 0]
    winners = [row for row in blocked_closed if row["_realized_R"] > 0]
    avoided_loss_r = sum(abs(row["_realized_R"]) for row in losses)
    missed_win_r = sum(row["_realized_R"] for row in winners)
    return {
        "opened_trade_count": len(rows),
        "closed_trade_count": len(closed),
        "blocked_closed_count": len(blocked_closed),
        "avoided_loss_R": avoided_loss_r,
        "missed_win_R": missed_win_r,
        "net_counterfactual_R": avoided_loss_r - missed_win_r,
        "loss_capture_count": len(losses),
        "missed_winner_count": len(winners),
        "loss_capture_rate": len(losses) / sum(1 for row in closed if row["_realized_R"] < 0)
        if any(row["_realized_R"] < 0 for row in closed)
        else None,
        "winner_false_block_rate": len(winners) / sum(1 for row in closed if row["_realized_R"] > 0)
        if any(row["_realized_R"] > 0 for row in closed)
        else None,
        "blocked_net_actual_R": sum(row["_realized_R"] for row in blocked_closed),
        "passed_net_actual_R": sum(row["_realized_R"] for row in passed_closed),
        "passed_count": len(rows) - len(blocked),
        "blocked_count": len(blocked),
    }


def coverage_metrics(rows):
    total = len(rows)
    ages = [as_float(row.get("regime_context_age_secs")) for row in rows]
    ages = [value for value in ages if value is not None]
    trend_present = sum(1 for row in rows if as_float(row.get("trend_strength")) is not None)
    impulse_known = sum(1 for row in rows if as_bool(row.get("impulse")) is not None)
    shadow_present = sum(
        1 for row in rows
        if any(key in row for key in ENTRY_LOCATION_SHADOW_FIELDS)
    )
    return {
        "market_state_known": sum(1 for row in rows if is_known(row.get("market_state"))),
        "market_regime_known": sum(1 for row in rows if is_known(row.get("market_regime"))),
        "regime_context_source_known": sum(1 for row in rows if is_known(row.get("regime_context_source"))),
        "entry_location_shadow_present": shadow_present,
        "entry_location_shadow_missing": total - shadow_present,
        "total": total,
        "regime_age_count": len(ages),
        "regime_age_median": statistics.median(ages) if ages else None,
        "regime_age_max": max(ages) if ages else None,
        "regime_age_gt_180": sum(1 for value in ages if value > 180),
        "impulse_known": impulse_known,
        "trend_strength_present": trend_present,
    }


def segment_rows(rows, key_fields):
    groups = defaultdict(list)
    for row in rows:
        key = tuple(norm(row.get(field)) for field in key_fields)
        groups[key].append(row)
    return groups


def confidence_groups(rows):
    groups = defaultdict(list)
    for row in rows:
        if not row.get("confirm_smc_entry_location_shadow_present"):
            key = "NOT_EVALUATED"
        else:
            key = "LOW_CONFIDENCE" if row.get("low_confidence") else "NORMAL_CONFIDENCE"
        groups[(key,)].append(row)
    return groups


def primary_reason_groups(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[(row.get("primary_entry_location_reason") or "NONE",)].append(row)
    return groups


def shadow_bucket_groups(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[(row.get("confirm_smc_entry_location_risk_bucket") or "UNKNOWN",)].append(row)
    return groups


def shadow_version_groups(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[(row.get("confirm_smc_entry_location_version") or "UNKNOWN",)].append(row)
    return groups


def table_line(values, widths):
    return "  ".join(str(value).ljust(width) for value, width in zip(values, widths)).rstrip()


def append_rule_table(lines, title, metrics_by_name):
    headers = [
        "rule",
        "blocked",
        "blocked_closed",
        "avoid_loss_R",
        "miss_win_R",
        "net_cf_R",
        "loss_cap",
        "win_false",
        "blocked_actual_R",
        "passed_actual_R",
    ]
    widths = [38, 8, 14, 12, 10, 9, 9, 10, 16, 15]
    lines.append("")
    lines.append(title)
    lines.append(table_line(headers, widths))
    for name, metrics in metrics_by_name:
        lines.append(
            table_line(
                [
                    name,
                    metrics["blocked_count"],
                    metrics["blocked_closed_count"],
                    format_num(metrics["avoided_loss_R"]),
                    format_num(metrics["missed_win_R"]),
                    format_num(metrics["net_counterfactual_R"]),
                    format_pct(metrics["loss_capture_count"], sum(1 for row in metrics.get("_closed_rows", []) if row["_realized_R"] < 0))
                    if "_closed_rows" in metrics
                    else format_num(metrics["loss_capture_rate"] * 100.0, 1) + "%"
                    if metrics["loss_capture_rate"] is not None
                    else "NA",
                    format_num(metrics["winner_false_block_rate"] * 100.0, 1) + "%"
                    if metrics["winner_false_block_rate"] is not None
                    else "NA",
                    format_num(metrics["blocked_net_actual_R"]),
                    format_num(metrics["passed_net_actual_R"]),
                ],
                widths,
            )
        )


def append_segment_table(lines, title, groups, key_headers):
    headers = list(key_headers) + ["rows", "closed", "blocked", "net_actual_R", "net_cf_R"]
    widths = [max(12, len(header) + 2) for header in key_headers] + [6, 7, 8, 13, 9]
    lines.append("")
    lines.append(title)
    lines.append(table_line(headers, widths))
    for key, group_rows in sorted(groups.items(), key=lambda item: (item[0], len(item[1]))):
        metrics = rule_metrics(group_rows, "entry_location_shadow_any")
        closed_r = [row["_realized_R"] for row in group_rows if row.get("_closed") and row.get("_realized_R") is not None]
        lines.append(
            table_line(
                list(key)
                + [
                    len(group_rows),
                    len(closed_r),
                    metrics["blocked_count"],
                    format_num(sum(closed_r)),
                    format_num(metrics["net_counterfactual_R"]),
                ],
                widths,
            )
        )


def recommendation(rows):
    perf = summarize_performance(rows)
    any_metrics = rule_metrics(rows, "entry_location_shadow_any")
    closed_count = perf["closed_count"]
    blocked_closed = any_metrics["blocked_closed_count"]
    net_cf = any_metrics["net_counterfactual_R"]
    false_block = any_metrics["winner_false_block_rate"]
    loss_capture = any_metrics["loss_capture_rate"]
    if closed_count == 0 or blocked_closed == 0:
        return "WATCH_ONLY", "No closed blocked sample yet."
    if net_cf <= 0:
        return "NO_GUARD", "Blocked winners cost as much or more than avoided losses."
    if closed_count < 10 or blocked_closed < 5:
        return "WATCH_ONLY", "Positive counterfactual, but closed blocked sample is still small."
    if closed_count >= 20 and net_cf >= 2.0 and (false_block is None or false_block <= 0.25):
        return "PAPER_GUARD_CANDIDATE", "Positive net counterfactual with enough closed sample for PAPER-only guard review."
    if loss_capture is not None and loss_capture >= 0.4 and (false_block is None or false_block <= 0.4):
        return "SHADOW_GUARD_CANDIDATE", "Positive net counterfactual; validate further as emitted shadow evidence first."
    return "WATCH_ONLY", "Positive net counterfactual, but capture/false-block mix needs more observation."


def build_report(decisions, decision_stats, raw_open_rows, unique_open_rows, duplicate_open_rows, annotated, join_stats, trade_stats, state_stats):
    lines = []
    perf = summarize_performance(annotated)
    coverage = coverage_metrics(unique_open_rows)
    closed_rows = [row for row in annotated if row.get("_closed")]

    rule_names = list(RULE_NAMES)
    metrics_by_name = []
    for name in rule_names:
        metrics = rule_metrics(annotated, name)
        metrics["_closed_rows"] = closed_rows
        metrics_by_name.append((name, metrics))

    lines.append("CONFIRM_SMC_RESEARCH_QUALIFIED entry-location shadow counterfactual")
    lines.append("")
    lines.append("Data reconciliation")
    lines.append(f"- total decision rows: {decision_stats.get('decision_rows', 0)}")
    lines.append(f"- malformed JSONL rows skipped: {decision_stats.get('malformed_jsonl_rows', 0)}")
    lines.append(f"- qualified OPEN rows raw: {len(raw_open_rows)}")
    lines.append(f"- qualified OPEN rows unique: {len(unique_open_rows)}")
    lines.append(f"- duplicate OPEN rows de-duplicated: {duplicate_open_rows}")
    lines.append(f"- paper_trades.csv rows loaded: {trade_stats.get('rows', 0)}")
    lines.append(f"- closed joined rows: {join_stats.get('closed_joined_rows', 0)}")
    lines.append(f"- open/unclosed rows: {join_stats.get('open_or_unclosed_rows', 0)}")
    lines.append(f"- join opened_trade_id: {join_stats.get('join_opened_trade_id', 0)}")
    lines.append(f"- join fallback symbol+side+timestamp: {join_stats.get('join_symbol_side_signal_ts', 0)}")
    lines.append(f"- unjoined rows: {join_stats.get('unjoined', 0)}")
    lines.append(f"- paper_state rows loaded for open-status hint: {state_stats.get('rows', 0)}")

    lines.append("")
    lines.append("Entry-location shadow coverage on unique OPEN rows")
    total = coverage["total"]
    lines.append(
        f"- emitted shadow present: {coverage['entry_location_shadow_present']}/{total} "
        f"({format_pct(coverage['entry_location_shadow_present'], total)})"
    )
    lines.append(
        f"- emitted shadow missing / not evaluated: {coverage['entry_location_shadow_missing']}/{total} "
        f"({format_pct(coverage['entry_location_shadow_missing'], total)})"
    )

    lines.append("")
    lines.append("Current qualified performance")
    lines.append(f"- closed count: {perf['closed_count']}")
    lines.append(f"- wins/losses/BE: {perf['wins']}/{perf['losses']}/{perf['be']}")
    lines.append(f"- net_R: {format_num(perf['net_R'])}")
    lines.append(f"- WR: {format_num(perf['WR'] * 100.0, 1) + '%' if perf['WR'] is not None else 'NA'}")
    lines.append(f"- avg_R: {format_num(perf['avg_R'])}")

    lines.append("")
    lines.append("Bias field coverage on unique OPEN rows")
    lines.append(f"- market_state known: {coverage['market_state_known']}/{total} ({format_pct(coverage['market_state_known'], total)})")
    lines.append(f"- market_regime known: {coverage['market_regime_known']}/{total} ({format_pct(coverage['market_regime_known'], total)})")
    lines.append(
        f"- regime_context_source known: {coverage['regime_context_source_known']}/{total} "
        f"({format_pct(coverage['regime_context_source_known'], total)})"
    )
    lines.append(
        "- regime_context_age_secs: "
        f"count={coverage['regime_age_count']} median={format_num(coverage['regime_age_median'], 3)} "
        f"max={format_num(coverage['regime_age_max'], 3)} count_gt_180={coverage['regime_age_gt_180']}"
    )
    lines.append(f"- impulse known: {coverage['impulse_known']}/{total} ({format_pct(coverage['impulse_known'], total)})")
    lines.append(
        f"- trend_strength present: {coverage['trend_strength_present']}/{total} "
        f"({format_pct(coverage['trend_strength_present'], total)})"
    )

    append_rule_table(lines, "Emitted shadow counterfactual table", metrics_by_name)
    append_segment_table(lines, "Side + market_state table", segment_rows(annotated, ("side", "market_state")), ("side", "market_state"))
    append_segment_table(lines, "Side + market_regime table", segment_rows(annotated, ("side", "market_regime")), ("side", "market_regime"))
    append_segment_table(lines, "Entry-location primary reason table", primary_reason_groups(annotated), ("primary_reason",))
    append_segment_table(lines, "Entry-location bucket table", shadow_bucket_groups(annotated), ("risk_bucket",))
    append_segment_table(lines, "Entry-location confidence table", confidence_groups(annotated), ("confidence",))
    append_segment_table(lines, "Entry-location version table", shadow_version_groups(annotated), ("version",))

    lines.append("")
    lines.append("Qualified opened trades")
    trade_headers = [
        "trade_id",
        "symbol",
        "side",
        "result",
        "R",
        "market_state",
        "market_regime",
        "smc_bias",
        "trend_direction",
        "smc_zone",
        "impulse",
        "shadow",
        "bucket",
        "score",
        "primary_reason",
        "would_block",
        "low_conf",
        "version",
    ]
    widths = [15, 16, 6, 9, 7, 14, 22, 10, 16, 10, 8, 7, 11, 7, 22, 12, 8, 12]
    lines.append(table_line(trade_headers, widths))
    for row in annotated:
        lines.append(
            table_line(
                [
                    row.get("_trade_id") or row.get("opened_trade_id") or "",
                    row.get("symbol") or "",
                    norm(row.get("side")),
                    row.get("_result") or "",
                    format_num(row.get("_realized_R")),
                    norm(row.get("market_state")),
                    norm(row.get("market_regime")),
                    norm(row.get("smc_bias")),
                    norm(row.get("trend_direction")),
                    norm(row.get("smc_zone")),
                    str(row.get("impulse")),
                    "T" if row.get("confirm_smc_entry_location_shadow_present") else "F",
                    row.get("confirm_smc_entry_location_risk_bucket") or "",
                    format_num(row.get("confirm_smc_entry_location_risk_score"), 0),
                    row.get("confirm_smc_entry_location_primary_reason") or "",
                    "T" if row.get("confirm_smc_entry_location_would_block") else "F",
                    "T" if row.get("low_confidence") else "F",
                    row.get("confirm_smc_entry_location_version") or "",
                ],
                widths,
            )
        )

    rec, reason = recommendation(annotated)
    lines.append("")
    lines.append("Recommendation")
    lines.append(f"- {rec}: {reason}")
    return "\n".join(lines)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Offline entry-location shadow counterfactual reader for CONFIRM_SMC_RESEARCH_QUALIFIED."
    )
    parser.add_argument("--decisions", default="logs/paper_smc_research_qualified_decisions.jsonl")
    parser.add_argument("--trades", default="paper_trades.csv")
    parser.add_argument("--state", default="paper_state.json")
    parser.add_argument("--no-state", action="store_true", help="Do not read paper_state.json for open-status hints.")
    parser.add_argument(
        "--trend-strength-threshold",
        type=float,
        default=None,
        help="Legacy option retained for CLI compatibility; emitted shadow fields are authoritative.",
    )
    parser.add_argument("--out", default="", help="Optional report output path. Stdout is always printed.")
    return parser.parse_args()


def main():
    args = parse_args()
    decisions_path = Path(args.decisions)
    trades_path = Path(args.trades)
    state_path = None if args.no_state else Path(args.state)

    decisions, decision_stats = load_decisions(decisions_path)
    trades, by_id, by_signal, trade_stats = load_paper_trades(trades_path)
    del trades
    open_ids, open_dedup_keys, state_stats = load_open_state(state_path) if state_path else (set(), set(), Counter())
    raw_open_rows, unique_open_rows, duplicate_open_rows = decision_open_rows(decisions)
    annotated, join_stats = annotate_rows(
        unique_open_rows,
        by_id,
        by_signal,
        open_ids,
        open_dedup_keys,
    )
    report = build_report(
        decisions,
        decision_stats,
        raw_open_rows,
        unique_open_rows,
        duplicate_open_rows,
        annotated,
        join_stats,
        trade_stats,
        state_stats,
    )
    print(report)
    if args.out:
        Path(args.out).write_text(report + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
