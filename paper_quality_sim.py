#!/usr/bin/env python3
"""
Read-only PAPER quality simulator v0.1.

Decision-row counterfactual / avoided-loss replay for PAPER_SMC_MAIN rows.
This is not a portfolio simulator and does not import bot execution modules.
"""

import argparse
import bisect
import csv
import datetime
import json
import math
import statistics
import sys
from collections import Counter, defaultdict, namedtuple
from pathlib import Path


BAD_REGIMES = {"CHOP_NO_TRADE", "RANGE_MEAN_REVERSION", "EXHAUSTION_REVERSAL"}
WEAK_BOS = {"WEAK", "NO_FOLLOWTHROUGH"}
STRONG_BOS_CONFIRMATIONS = {"CLOSE_THROUGH", "DISPLACEMENT", "RETESTED"}
HIGH_COLLAPSE_PCT = 80.0
NEAR_FULL_COLLAPSE_PCT = 99.0
LOW_SAMPLE_N = 30
SMALL_HOLDOUT_RESOLVED_N = 200
GridRule = namedtuple("GridRule", "name fn fields diagnostic")
DECISION_FIELDS = [
    "dedup_key",
    "symbol",
    "side",
    "source_timestamp",
    "signal_created_ts",
    "timestamp_unix",
    "entry_type",
    "candidate_type",
    "strategy_family",
    "opened",
    "opened_trade_id",
    "suppress_reason",
    "effective_score",
    "raw_score",
    "rr",
    "planned_rr",
    "entry",
    "sl",
    "tp",
    "rank_index",
    "candidate_priority",
    "ranking_enabled",
    "structural_decision_shadow",
    "bos_quality",
    "bos_confirmation",
    "weak_structure_extended",
    "weak_structure_extended_reason",
    "specific_combo_block_match",
    "source_reason",
    "range_context",
    "smc_bias",
    "smc_zone",
    "volume_confirmation",
    "displacement_quality",
    "geometry_status",
    "modifier_reasons",
    "structural_modifier",
]
OUTCOME_FIELDS = [
    "status",
    "mfe_r",
    "mae_r",
    "first_hit",
    "sl_hit",
    "tp_hit",
    "hit_1r",
    "hit_1_5r",
    "hit_2r",
    "time_to_1r_secs",
    "time_to_1_5r_secs",
    "time_to_2r_secs",
    "time_to_sl_secs",
    "time_to_tp_secs",
    "data_missing_reason",
    "observed_at",
    "rr",
    "structural_context",
]
REVERSAL_OUTCOME_FILE = "reversal_shadow_outcomes.jsonl"
SMC_V0_2_SHADOW_OUTCOME_FILE = "paper_smc_v0_2_shadow_outcomes.jsonl"
SMC_MAIN_GATE_SWAP_OUTCOME_FILE = "confirm_structural_outcomes.jsonl"
SMC_MAIN_GATE_SWAP_MAIN_DECISIONS_FILE = "paper_smc_main_decisions.jsonl"
SMC_MAIN_GATE_SWAP_RESEARCH_ENTRIES_FILE = "paper_smc_research_entries.jsonl"
SMC_MAIN_GATE_SWAP_TRADE_FILE = "paper_trades.csv"
SMC_V0_2_TERMINAL_STATUSES = {"RESOLVED", "DATA_MISSING", "EXPIRED"}
SMC_V0_2_SLICE_EXH_REV_SYMBOL_ALREADY_OPEN = "EXH_REV_SYMBOL_ALREADY_OPEN"
SMC_V0_2_BREAKDOWN_FIELDS = [
    "smc_v0_2_slice",
    "regime_context_regime",
    "bos_quality",
    "weak_structure_extended",
    "structural_decision_shadow",
    "candidate_type",
    "source_reason",
    "suppress_reason",
    "side",
    "symbol",
    "router_regime_age_bucket",
    "first_hit",
]
REVERSAL_BREAKDOWN_FIELDS = [
    "phase",
    "market_state",
    "exhaustion",
    "shadow_candidate_class",
    "bos_confirmation",
    "bos_type",
    "structural_decision_shadow",
    "geometry_status",
    "valid_geometry",
    "symbol",
    "side",
    "data_missing_reason",
    "expired_reason",
]
REVERSAL_POSITIVE_SLICES = [
    ("EXTENDED", lambda row: norm(row.get("exhaustion")) == "EXTENDED"),
    ("PRE_BREAK_LOW", lambda row: norm(row.get("phase")) == "PRE_BREAK_LOW"),
    ("NEAR", lambda row: norm(row.get("bos_confirmation")) == "NEAR"),
    ("SHORT", lambda row: norm(row.get("side")) == "SHORT"),
    ("EXTENDED + PRE_BREAK_LOW", lambda row: norm(row.get("exhaustion")) == "EXTENDED" and norm(row.get("phase")) == "PRE_BREAK_LOW"),
    ("EXTENDED + NEAR", lambda row: norm(row.get("exhaustion")) == "EXTENDED" and norm(row.get("bos_confirmation")) == "NEAR"),
    ("PRE_BREAK_LOW + NEAR", lambda row: norm(row.get("phase")) == "PRE_BREAK_LOW" and norm(row.get("bos_confirmation")) == "NEAR"),
    ("EXTENDED + PRE_BREAK_LOW + NEAR", lambda row: norm(row.get("exhaustion")) == "EXTENDED" and norm(row.get("phase")) == "PRE_BREAK_LOW" and norm(row.get("bos_confirmation")) == "NEAR"),
    ("EXTENDED + PRE_BREAK_LOW + NEAR + SHORT", lambda row: norm(row.get("exhaustion")) == "EXTENDED" and norm(row.get("phase")) == "PRE_BREAK_LOW" and norm(row.get("bos_confirmation")) == "NEAR" and norm(row.get("side")) == "SHORT"),
    ("EXTENDED + NEAR + SHORT", lambda row: norm(row.get("exhaustion")) == "EXTENDED" and norm(row.get("bos_confirmation")) == "NEAR" and norm(row.get("side")) == "SHORT"),
    ("PRE_BREAK_LOW + NEAR + SHORT", lambda row: norm(row.get("phase")) == "PRE_BREAK_LOW" and norm(row.get("bos_confirmation")) == "NEAR" and norm(row.get("side")) == "SHORT"),
]
REVERSAL_NEGATIVE_SLICES = [
    ("EXHAUSTED", lambda row: norm(row.get("exhaustion")) == "EXHAUSTED"),
    ("BREAKOUT_STRONG", lambda row: norm(row.get("phase")) == "BREAKOUT_STRONG"),
    ("LONG", lambda row: norm(row.get("side")) == "LONG"),
    ("EXHAUSTED + LONG", lambda row: norm(row.get("exhaustion")) == "EXHAUSTED" and norm(row.get("side")) == "LONG"),
    ("BREAKOUT_STRONG + LONG", lambda row: norm(row.get("phase")) == "BREAKOUT_STRONG" and norm(row.get("side")) == "LONG"),
    ("CLOSE_THROUGH", lambda row: norm(row.get("bos_confirmation")) == "CLOSE_THROUGH"),
    ("DISPLACEMENT", lambda row: norm(row.get("bos_confirmation")) == "DISPLACEMENT"),
    ("EXHAUSTED + CLOSE_THROUGH", lambda row: norm(row.get("exhaustion")) == "EXHAUSTED" and norm(row.get("bos_confirmation")) == "CLOSE_THROUGH"),
    ("BREAKOUT_STRONG + DISPLACEMENT", lambda row: norm(row.get("phase")) == "BREAKOUT_STRONG" and norm(row.get("bos_confirmation")) == "DISPLACEMENT"),
]
REVERSAL_SYMBOL_NORMALIZED_SLICES = [
    ("EXTENDED", "candidate", lambda row: norm(row.get("exhaustion")) == "EXTENDED"),
    ("EXTENDED + PRE_BREAK_LOW", "candidate", lambda row: norm(row.get("exhaustion")) == "EXTENDED" and norm(row.get("phase")) == "PRE_BREAK_LOW"),
    ("NEAR", "candidate", lambda row: norm(row.get("bos_confirmation")) == "NEAR"),
    ("PRE_BREAK_LOW", "candidate", lambda row: norm(row.get("phase")) == "PRE_BREAK_LOW"),
    ("EXTENDED + NEAR", "candidate", lambda row: norm(row.get("exhaustion")) == "EXTENDED" and norm(row.get("bos_confirmation")) == "NEAR"),
    ("EXTENDED + PRE_BREAK_LOW + NEAR", "candidate", lambda row: norm(row.get("exhaustion")) == "EXTENDED" and norm(row.get("phase")) == "PRE_BREAK_LOW" and norm(row.get("bos_confirmation")) == "NEAR"),
    ("EXTENDED + NEAR + SHORT", "candidate", lambda row: norm(row.get("exhaustion")) == "EXTENDED" and norm(row.get("bos_confirmation")) == "NEAR" and norm(row.get("side")) == "SHORT"),
    ("EXTENDED + PRE_BREAK_LOW + NEAR + SHORT", "candidate", lambda row: norm(row.get("exhaustion")) == "EXTENDED" and norm(row.get("phase")) == "PRE_BREAK_LOW" and norm(row.get("bos_confirmation")) == "NEAR" and norm(row.get("side")) == "SHORT"),
    ("SHORT", "diagnostic", lambda row: norm(row.get("side")) == "SHORT"),
    ("LONG", "weak diagnostic", lambda row: norm(row.get("side")) == "LONG"),
]
REGIME_FIELDS = [
    "observed_at",
    "scan_id",
    "symbol",
    "regime",
    "market_state",
    "exhaustion",
    "phase",
    "range_context",
    "smc_bias",
    "smc_zone",
    "bos_confirmation",
    "invalid_context",
]
SMC_MAIN_GATE_SWAP_MAIN_TYPES = {"ACCEPTED_CONFIRM", "LOW_SCORE", "MID_SCORE_WEAK_BOS", "RR_FAIL"}
SMC_MAIN_GATE_SWAP_RESEARCH_TYPES = {"LOW_SCORE", "MID_SCORE_WEAK_BOS", "RR_FAIL"}
SMC_MAIN_GATE_SWAP_RESEARCH_DECISIONS = {"QUALIFIED", "NEUTRAL", "WOULD_DOWNRANK", "UNKNOWN"}
SMC_MAIN_GATE_SWAP_PRIORITY = {
    "ACCEPTED_CONFIRM": 0,
    "MID_SCORE_WEAK_BOS": 1,
    "LOW_SCORE": 2,
    "RR_FAIL": 3,
}
SMC_MAIN_GATE_SWAP_REPLAY_MAX_OPEN = 5
SMC_MAIN_GATE_SWAP_REPLAY_ONE_PER_SYMBOL = True
SMC_MAIN_GATE_SWAP_REPLAY_FALLBACK_TTL_SECS = 86400
SMC_MAIN_GATE_SWAP_SOURCE_TIME_REPLAY = "CONCURRENCY_REPLAY_SOURCE_TIME_BUCKET"
SMC_MAIN_GATE_SWAP_RANK_INDEX_REPLAY = "CONCURRENCY_REPLAY_RANK_INDEX_BATCH_APPROX"
SMC_MAIN_GATE_SWAP_APPROX_REPLAY = "APPROX_CONCURRENCY_REPLAY"
SMC_MAIN_GATE_SWAP_RANK_BATCH_SOURCE = "rank_index_segmentation"
SMC_MAIN_GATE_SWAP_SOURCE_TIME_BATCH_SOURCE = "source_time_bucket"
SMC_MAIN_GATE_SWAP_FALLBACK_BATCH_SOURCE = "source_time_bucket_fallback"
SMC_MAIN_GATE_SWAP_LOW_RANK_COVERAGE_PCT = 60.0
SMC_MAIN_GATE_SWAP_SUPPRESS_DIAGNOSTICS = [
    "stale_collector_ts",
    "symbol_already_open",
    "max_open_reached",
    "duplicate_key",
    "structural_modifier_score_too_low",
    "weak_structure_no_followthrough_divergence",
]
SMC_MAIN_GATE_SWAP_SCORE_BUCKETS = [
    ("s < 0", lambda value: value is not None and value < 0),
    ("0 <= s < 1", lambda value: value is not None and 0 <= value < 1),
    ("1 <= s < 2", lambda value: value is not None and 1 <= value < 2),
    ("2 <= s < 3", lambda value: value is not None and 2 <= value < 3),
    ("s >= 3", lambda value: value is not None and value >= 3),
    ("missing", lambda value: value is None),
]


def as_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        value = float(value)
        if math.isnan(value):
            return default
        return value
    except (TypeError, ValueError):
        return default


def as_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def norm(value, default="UNKNOWN"):
    if value is None or value == "":
        return default
    return str(value).strip().upper()


def load_jsonl(path):
    rows = []
    stats = {"missing": 0, "parse_errors": 0, "lines": 0}
    if not path.exists():
        stats["missing"] = 1
        return rows, stats
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            stats["lines"] += 1
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                stats["parse_errors"] += 1
    return rows, stats


def is_decision_row(row):
    event_type = norm(row.get("event_type"), "")
    if event_type == "PAPER_SMC_MAIN_DECISION":
        return True
    if norm(row.get("engine"), "") == "PAPER_SMC_MAIN":
        return True
    required = {"dedup_key", "symbol", "side"}
    has_time = any(row.get(key) is not None for key in ("source_timestamp", "signal_created_ts", "timestamp_unix"))
    return required.issubset(row) and has_time and (
        row.get("entry_type") == "PAPER_SMC_MAIN" or row.get("strategy_family") == "paper_smc_main"
    )


def compact(row, fields):
    return {field: row.get(field) for field in fields if field in row}


def row_timestamp(row):
    for key in ("source_timestamp", "signal_created_ts", "timestamp_unix"):
        value = as_float(row.get(key))
        if value is not None:
            return value
    return None


def load_decisions(path):
    raw, stats = load_jsonl(path)
    decisions = []
    seen = set()
    deduped = 0
    for row in raw:
        if not is_decision_row(row):
            continue
        item = compact(row, DECISION_FIELDS)
        item["_source_ts"] = row_timestamp(row)
        item["_fallback_key"] = fallback_key(item)
        key = item.get("dedup_key") or item["_fallback_key"] or id(item)
        if key in seen:
            deduped += 1
            continue
        seen.add(key)
        decisions.append(item)
    stats["loaded"] = len(decisions)
    stats["deduped"] = deduped
    return decisions, stats


def fallback_key(row):
    symbol = row.get("symbol")
    side = row.get("side")
    ts = row_timestamp(row)
    if not symbol or not side or ts is None:
        return None
    return f"{symbol}|{side}|{round(ts)}"


def outcome_sort_key(row):
    return as_float(row.get("observed_at"), -1.0)


def load_outcomes(path):
    raw, stats = load_jsonl(path)
    by_dedup = {}
    by_fallback = {}
    loaded = 0
    for row in raw:
        if norm(row.get("event_type"), "") not in {"", "CONFIRM_STRUCTURAL_OUTCOME"}:
            continue
        item = compact(row, OUTCOME_FIELDS + ["dedup_key", "symbol", "side", "source_timestamp", "signal_created_ts", "timestamp_unix"])
        key = item.get("dedup_key")
        if key and outcome_sort_key(item) >= outcome_sort_key(by_dedup.get(key, {})):
            by_dedup[key] = item
        fb = fallback_key(item)
        if fb and outcome_sort_key(item) >= outcome_sort_key(by_fallback.get(fb, {})):
            by_fallback[fb] = item
        loaded += 1
    stats["loaded"] = loaded
    stats["dedup_keys"] = len(by_dedup)
    stats["fallback_keys"] = len(by_fallback)
    return by_dedup, by_fallback, stats


def load_reversal_outcomes(path):
    raw, stats = load_jsonl(path)
    rows = []
    for row in raw:
        if norm(row.get("event_type"), "") not in {"", "REVERSAL_SHADOW_OUTCOME"}:
            continue
        item = dict(row)
        item["_source_ts"] = row_timestamp(item) or as_float(item.get("observed_at"))
        item["_outcome"] = item
        rows.append(item)
    stats["loaded"] = len(rows)
    stats["deduped"] = 0
    stats["dedup_keys"] = len({row.get("dedup_key") for row in rows if row.get("dedup_key")})
    return rows, stats


def load_regimes(path):
    raw, stats = load_jsonl(path)
    per_symbol = defaultdict(list)
    for row in raw:
        if norm(row.get("event_type"), "") not in {"", "MARKET_REGIME_ROUTER_SHADOW"}:
            continue
        symbol = row.get("symbol")
        observed_at = as_float(row.get("observed_at"))
        if not symbol or observed_at is None:
            continue
        item = compact(row, REGIME_FIELDS)
        item["observed_at"] = observed_at
        per_symbol[symbol].append(item)
    indexed = {}
    for symbol, rows in per_symbol.items():
        rows.sort(key=lambda item: item["observed_at"])
        indexed[symbol] = ([item["observed_at"] for item in rows], rows)
    stats["loaded"] = sum(len(rows) for _, rows in per_symbol.items())
    stats["symbols"] = len(indexed)
    return indexed, stats


def nearest_prior_regime(index, symbol, ts):
    if ts is None:
        return None, "UNKNOWN_TS"
    if symbol not in index:
        return None, "NO_ROUTER"
    times, rows = index[symbol]
    pos = bisect.bisect_right(times, ts) - 1
    if pos < 0:
        return None, "NO_PRIOR_ROUTER"
    return rows[pos], "JOINED"


def join_rows(decisions, outcomes_by_dedup, outcomes_by_fallback, regime_index):
    joined = []
    counts = Counter()
    for decision in decisions:
        key = decision.get("dedup_key")
        outcome = outcomes_by_dedup.get(key) if key else None
        if outcome is not None:
            counts["outcome_dedup_join"] += 1
        elif decision.get("_fallback_key") and decision["_fallback_key"] in outcomes_by_fallback:
            outcome = outcomes_by_fallback[decision["_fallback_key"]]
            counts["outcome_fallback_join"] += 1
        else:
            counts["outcome_missing"] += 1
        regime_row, regime_join_status = nearest_prior_regime(regime_index, decision.get("symbol"), decision.get("_source_ts"))
        counts[f"regime_{regime_join_status.lower()}"] += 1
        merged = dict(decision)
        merged["_outcome"] = outcome or {}
        merged["_regime"] = regime_row or {}
        merged["regime"] = norm((regime_row or {}).get("regime"), "NO_ROUTER")
        merged["router_join_status"] = regime_join_status
        if not merged.get("bos_confirmation"):
            merged["bos_confirmation"] = (regime_row or {}).get("bos_confirmation")
        joined.append(merged)
    return joined, counts


def candidate_is_continuation(row):
    text = " ".join(
        str(row.get(key, ""))
        for key in ("entry_type", "candidate_type", "strategy_family", "source_reason")
    ).upper()
    return "CONT" in text or "CONFIRM" in text or row.get("entry_type") == "PAPER_SMC_MAIN"


def gate_bad_regime(row):
    return candidate_is_continuation(row) and norm(row.get("regime")) in BAD_REGIMES


def gate_worst_combo(row):
    return (
        as_bool(row.get("weak_structure_extended"))
        and norm(row.get("structural_decision_shadow")) == "UNKNOWN"
        and norm(row.get("bos_quality")) in WEAK_BOS
        and norm(row.get("regime")) in BAD_REGIMES
    )


def gate_weak_bos(row):
    return norm(row.get("bos_quality")) in WEAK_BOS


def gate_bos_confirmation(row):
    return norm(row.get("bos_confirmation")) not in STRONG_BOS_CONFIRMATIONS


GATES = [
    ("BASELINE", lambda row: False),
    ("BAD_REGIME_LABEL", gate_bad_regime),
    ("WORST_COMBO", gate_worst_combo),
    ("BLOCK_WEAK_BOS", gate_weak_bos),
    ("KEEP_STRONG_BOS_CONFIRMATION", gate_bos_confirmation),
]


def planned_rr(row):
    value = as_float(row.get("planned_rr"))
    if value is not None:
        return value
    value = as_float(row.get("rr"))
    if value is not None:
        return value
    return as_float(row.get("_outcome", {}).get("rr"), 0.0)


def realized_r(row):
    outcome = row.get("_outcome") or {}
    first_hit = norm(outcome.get("first_hit"), "")
    rr = as_float(outcome.get("rr"), planned_rr(row) or 0.0) or 0.0
    mfe = as_float(outcome.get("mfe_r"), 0.0) or 0.0
    if first_hit == "SL":
        return -1.0
    if first_hit == "TP":
        return rr
    if first_hit == "2R":
        return 2.0
    if first_hit in {"1.5R", "1_5R"}:
        return 1.5
    if first_hit == "1R":
        return 1.0
    if first_hit == "0.5R":
        return 0.5
    if first_hit == "AMBIGUOUS":
        return min(mfe, 2.0)
    return 0.0


def status_of(row):
    status = norm((row.get("_outcome") or {}).get("status"), "NO_OUTCOME")
    if status == "NO_OUTCOME":
        return "NO_OUTCOME"
    return status


def max_drawdown(rows):
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for row in sorted(rows, key=lambda item: (item.get("_source_ts") is None, item.get("_source_ts") or 0.0)):
        equity += realized_r(row)
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def mean(values):
    return statistics.fmean(values) if values else 0.0


def median(values):
    return statistics.median(values) if values else 0.0


def pct(num, den):
    return (100.0 * num / den) if den else 0.0


def metric_block(rows, gate):
    blocked = [row for row in rows if gate(row)]
    kept = [row for row in rows if not gate(row)]
    resolved = [row for row in rows if status_of(row) == "RESOLVED"]
    kept_resolved = [row for row in kept if status_of(row) == "RESOLVED"]
    blocked_resolved = [row for row in blocked if status_of(row) == "RESOLVED"]
    open_count = sum(1 for row in rows if status_of(row) == "OPEN")
    missing_rows = [row for row in rows if status_of(row) == "DATA_MISSING"]
    no_outcome = sum(1 for row in rows if status_of(row) == "NO_OUTCOME")
    net_base = sum(realized_r(row) for row in resolved)
    net_kept = sum(realized_r(row) for row in kept_resolved)
    net_blocked = sum(realized_r(row) for row in blocked_resolved)
    mfe_values = [as_float(row.get("_outcome", {}).get("mfe_r")) for row in kept_resolved]
    mae_values = [as_float(row.get("_outcome", {}).get("mae_r")) for row in kept_resolved]
    mfe_values = [value for value in mfe_values if value is not None]
    mae_values = [value for value in mae_values if value is not None]
    r_values = [realized_r(row) for row in kept_resolved]
    wins = sum(1 for value in r_values if value > 0)
    losses = sum(1 for value in r_values if value < 0)
    be = sum(1 for value in r_values if value == 0)
    full_sl_before_half = sum(
        1
        for row in kept_resolved
        if norm(row.get("_outcome", {}).get("first_hit"), "") == "SL"
        and (as_float(row.get("_outcome", {}).get("mfe_r"), 0.0) or 0.0) < 0.5
    )
    return {
        "total": len(rows),
        "resolved": len(resolved),
        "open": open_count,
        "data_missing": len(missing_rows),
        "no_outcome": no_outcome,
        "blocked": len(blocked),
        "blocked_resolved": len(blocked_resolved),
        "kept": len(kept),
        "collapse_pct": pct(len(blocked), len(rows)),
        "resolved_collapse_pct": pct(len(blocked_resolved), len(resolved)),
        "net_R_baseline": net_base,
        "net_R_kept": net_kept,
        "net_R_blocked": net_blocked,
        "avoided_R": -net_blocked,
        "mean_mfe_R": mean(mfe_values),
        "median_mfe_R": median(mfe_values),
        "mean_mae_R": mean(mae_values),
        "median_mae_R": median(mae_values),
        "wins": wins,
        "losses": losses,
        "be": be,
        "wr_pct": pct(wins, len(kept_resolved)),
        "hit_0_5r_pct": pct(sum(1 for row in kept_resolved if (as_float(row.get("_outcome", {}).get("mfe_r"), 0.0) or 0.0) >= 0.5), len(kept_resolved)),
        "hit_1r_pct": pct(sum(1 for row in kept_resolved if as_bool(row.get("_outcome", {}).get("hit_1r"))), len(kept_resolved)),
        "hit_1_5r_pct": pct(sum(1 for row in kept_resolved if as_bool(row.get("_outcome", {}).get("hit_1_5r"))), len(kept_resolved)),
        "hit_2r_pct": pct(sum(1 for row in kept_resolved if as_bool(row.get("_outcome", {}).get("hit_2r"))), len(kept_resolved)),
        "full_sl_before_0_5r_pct": pct(full_sl_before_half, len(kept_resolved)),
        "max_dd_R": max_drawdown(kept_resolved),
        "data_missing_reasons": Counter((row.get("_outcome") or {}).get("data_missing_reason") or "UNKNOWN" for row in missing_rows),
    }


def split_train_holdout(rows):
    resolved = [row for row in rows if status_of(row) == "RESOLVED"]
    resolved.sort(key=lambda item: (item.get("_source_ts") is None, item.get("_source_ts") or 0.0))
    cutoff = int(len(resolved) * 0.70)
    train_ids = {id(row) for row in resolved[:cutoff]}
    holdout_ids = {id(row) for row in resolved[cutoff:]}
    return {
        "ALL": rows,
        "TRAIN": [row for row in rows if id(row) in train_ids],
        "HOLDOUT": [row for row in rows if id(row) in holdout_ids],
    }


def print_table(title, headers, rows):
    print()
    print(title)
    widths = [len(header) for header in headers]
    rendered = []
    for row in rows:
        cells = [format_cell(row.get(header, "")) for header in headers]
        widths = [max(width, len(cell)) for width, cell in zip(widths, cells)]
        rendered.append(cells)
    print(" | ".join(header.ljust(width) for header, width in zip(headers, widths)))
    print("-+-".join("-" * width for width in widths))
    for cells in rendered:
        print(" | ".join(cell.ljust(width) for cell, width in zip(cells, widths)))


def format_cell(value):
    if isinstance(value, float):
        return f"{value:.2f}"
    text = str(value)
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    return text.encode(encoding, errors="replace").decode(encoding, errors="replace")


def metric_rows(rows):
    groups = split_train_holdout(rows)
    table = []
    all_metrics = {}
    for split_name, split_rows in groups.items():
        for gate_name, gate in GATES:
            metrics = metric_block(split_rows, gate)
            all_metrics[(split_name, gate_name)] = metrics
            table.append(
                {
                    "split": split_name,
                    "gate": gate_name,
                    "total": metrics["total"],
                    "resolved": metrics["resolved"],
                    "open": metrics["open"],
                    "missing": metrics["data_missing"],
                    "blocked": metrics["blocked"],
                    "blocked_res": metrics["blocked_resolved"],
                    "collapse%": metrics["collapse_pct"],
                    "label": gate_action_label(metrics),
                    "base_R": metrics["net_R_baseline"],
                    "kept_R": metrics["net_R_kept"],
                    "blocked_R": metrics["net_R_blocked"],
                    "avoided_R": metrics["avoided_R"],
                    "WR%": metrics["wr_pct"],
                    "maxDD_R": metrics["max_dd_R"],
                }
            )
    return table, all_metrics


def gate_action_label(metrics):
    if metrics["resolved"] and metrics["resolved_collapse_pct"] >= NEAR_FULL_COLLAPSE_PCT:
        return "DEGENERATE"
    if metrics["collapse_pct"] >= HIGH_COLLAPSE_PCT:
        return "HIGH_COLLAPSE"
    if 0 < metrics["blocked_resolved"] < LOW_SAMPLE_N:
        return "LOW_SAMPLE"
    return "RESEARCH"


def bucket_rr(value):
    if value is None:
        return "UNKNOWN"
    if value < 1.0:
        return "<1"
    if value < 1.5:
        return "1-1.5"
    if value < 2.0:
        return "1.5-2"
    if value < 3.0:
        return "2-3"
    return "3+"


def print_rr_distribution(rows):
    resolved = [row for row in rows if status_of(row) == "RESOLVED"]
    buckets = defaultdict(list)
    artificial = 0
    for row in resolved:
        rr = planned_rr(row)
        mfe = as_float(row.get("_outcome", {}).get("mfe_r"), 0.0) or 0.0
        buckets[bucket_rr(rr)].append(row)
        if rr is not None and rr >= 2.0 and mfe < 0.75:
            artificial += 1
    table = []
    for bucket in ["<1", "1-1.5", "1.5-2", "2-3", "3+", "UNKNOWN"]:
        bucket_rows = buckets.get(bucket, [])
        if not bucket_rows:
            continue
        mfe_values = [as_float(row.get("_outcome", {}).get("mfe_r"), 0.0) or 0.0 for row in bucket_rows]
        mae_values = [as_float(row.get("_outcome", {}).get("mae_r"), 0.0) or 0.0 for row in bucket_rows]
        table.append(
            {
                "rr_bucket": bucket,
                "n": len(bucket_rows),
                "median_mfe": median(mfe_values),
                "median_mae": median(mae_values),
                "hit_0.5R%": pct(sum(1 for value in mfe_values if value >= 0.5), len(bucket_rows)),
                "hit_1R%": pct(sum(1 for row in bucket_rows if as_bool(row.get("_outcome", {}).get("hit_1r"))), len(bucket_rows)),
                "hit_1.5R%": pct(sum(1 for row in bucket_rows if as_bool(row.get("_outcome", {}).get("hit_1_5r"))), len(bucket_rows)),
                "hit_2R%": pct(sum(1 for row in bucket_rows if as_bool(row.get("_outcome", {}).get("hit_2r"))), len(bucket_rows)),
            }
        )
    print_table("MFE probability / volatility-fit log-only distribution", ["rr_bucket", "n", "median_mfe", "median_mae", "hit_0.5R%", "hit_1R%", "hit_1.5R%", "hit_2R%"], table)
    print(f"RR_ARTIFICIAL_PROXY labels: {artificial} resolved rows where planned_rr >= 2.0 and mfe_r < 0.75")


def field_non_null_pct(rows, field):
    if not rows:
        return 0.0
    present = sum(1 for row in rows if row.get(field) is not None and row.get(field) != "")
    return pct(present, len(rows))


def combine_conditions(parts):
    def combined(row):
        return all(part.fn(row) for part in parts)

    name = " AND ".join(part.name for part in parts)
    fields = []
    for part in parts:
        for field in part.fields:
            if field not in fields:
                fields.append(field)
    return GridRule(name, combined, fields, False)


def make_value_rule(field, op_name, values, diagnostic=False):
    values = set(values)

    def check(row):
        return norm(row.get(field)) in values

    value_text = "|".join(sorted(values))
    return GridRule(f"{field} {op_name} {value_text}", check, [field], diagnostic)


def generate_grid_rules():
    regime_rules = [
        make_value_rule("regime", "==", {"CHOP_NO_TRADE"}),
        make_value_rule("regime", "==", {"RANGE_MEAN_REVERSION"}),
        make_value_rule("regime", "==", {"EXHAUSTION_REVERSAL"}),
        make_value_rule("regime", "in", {"CHOP_NO_TRADE", "RANGE_MEAN_REVERSION"}),
        make_value_rule("regime", "in", {"CHOP_NO_TRADE", "EXHAUSTION_REVERSAL"}),
        make_value_rule("regime", "in", {"RANGE_MEAN_REVERSION", "EXHAUSTION_REVERSAL"}),
        make_value_rule("regime", "in", BAD_REGIMES),
        make_value_rule("regime", "==", {"NO_ROUTER"}, diagnostic=True),
    ]
    bos_rules = [
        make_value_rule("bos_quality", "==", {"WEAK"}),
        make_value_rule("bos_quality", "==", {"NO_FOLLOWTHROUGH"}),
        make_value_rule("bos_quality", "in", {"WEAK", "NO_FOLLOWTHROUGH"}),
        GridRule("bos_quality != STRONG", lambda row: norm(row.get("bos_quality")) != "STRONG", ["bos_quality"], False),
        make_value_rule("bos_quality", "in", {"WEAK", "NO_FOLLOWTHROUGH", "TRAP"}),
    ]
    structural_rules = [
        make_value_rule("structural_decision_shadow", "==", {"UNKNOWN"}),
        GridRule("structural_decision_shadow != QUALIFIED", lambda row: norm(row.get("structural_decision_shadow")) != "QUALIFIED", ["structural_decision_shadow"], False),
        GridRule("weak_structure_extended == true", lambda row: as_bool(row.get("weak_structure_extended")), ["weak_structure_extended"], False),
        GridRule("weak_structure_extended != false", lambda row: row.get("weak_structure_extended") is None or as_bool(row.get("weak_structure_extended")), ["weak_structure_extended"], False),
        GridRule("specific_combo_block_match == false", lambda row: row.get("specific_combo_block_match") is not None and not as_bool(row.get("specific_combo_block_match")), ["specific_combo_block_match"], False),
    ]
    candidate_rules = [
        make_value_rule("candidate_type", "==", {"LOW_SCORE"}),
        make_value_rule("candidate_type", "==", {"MID_SCORE_WEAK_BOS"}),
        make_value_rule("candidate_type", "in", {"LOW_SCORE", "MID_SCORE_WEAK_BOS"}),
        make_value_rule("source_reason", "==", {"LOW_SCORE"}),
    ]
    diagnostic_rr_rules = [
        GridRule("planned_rr >= 2.0", lambda row: planned_rr(row) is not None and planned_rr(row) >= 2.0, ["planned_rr", "rr"], True),
        GridRule("planned_rr >= 3.0", lambda row: planned_rr(row) is not None and planned_rr(row) >= 3.0, ["planned_rr", "rr"], True),
    ]

    rules = []
    seen = set()

    def add(rule):
        if rule.name not in seen:
            seen.add(rule.name)
            rules.append(rule)

    for regime in regime_rules:
        add(regime)
        for group in (bos_rules, structural_rules, candidate_rules):
            for rule in group:
                add(combine_conditions([regime, rule]))
        for bos in bos_rules:
            for structural in structural_rules:
                add(combine_conditions([regime, bos, structural]))
        for structural in structural_rules:
            for candidate in candidate_rules:
                add(combine_conditions([regime, structural, candidate]))
        for bos in bos_rules:
            for structural in structural_rules:
                for candidate in candidate_rules:
                    add(combine_conditions([regime, bos, structural, candidate]))

    add(GridRule("BASELINE_BLOCK_WEAK_BOS", gate_weak_bos, ["bos_quality"], False))
    add(GridRule("BASELINE_BAD_REGIME_LABEL", gate_bad_regime, ["regime"], False))
    add(GridRule("BASELINE_WORST_COMBO", gate_worst_combo, ["weak_structure_extended", "structural_decision_shadow", "bos_quality", "regime"], False))
    for rule in diagnostic_rr_rules:
        add(rule)
    return rules


def classify_grid_result(all_metrics, train_metrics, holdout_metrics, args, diagnostic=False):
    max_collapse_pct = args.max_collapse * 100.0
    max_holdout_collapse_pct = args.max_holdout_collapse * 100.0
    if diagnostic:
        return "DIAGNOSTIC"
    if all_metrics["collapse_pct"] >= 95.0 or all_metrics["kept"] == 0:
        return "DEGENERATE"
    if all_metrics["blocked_resolved"] < args.min_blocked_resolved:
        return "LOW_SAMPLE"
    holdout_limit_applies = holdout_metrics["resolved"] >= args.min_holdout_resolved
    if all_metrics["collapse_pct"] > max_collapse_pct or (
        holdout_limit_applies and holdout_metrics["collapse_pct"] > max_holdout_collapse_pct
    ):
        return "HIGH_COLLAPSE"
    if all_metrics["avoided_R"] <= 0:
        return "NEGATIVE"
    if train_metrics["avoided_R"] > 0 and holdout_metrics["avoided_R"] < 0:
        return "TRAIN_ONLY"
    if holdout_metrics["avoided_R"] > 0 and train_metrics["avoided_R"] <= 0:
        return "HOLDOUT_ONLY"
    if train_metrics["avoided_R"] > 0 and holdout_metrics["avoided_R"] >= 0:
        return "PRACTICAL_CANDIDATE"
    return "NEGATIVE"


def grid_score(all_metrics, holdout_metrics):
    return (
        all_metrics["avoided_R"]
        + 0.5 * holdout_metrics["avoided_R"]
        - 0.25 * all_metrics["collapse_pct"]
        + 0.02 * all_metrics["blocked_resolved"]
        - 0.05 * all_metrics["max_dd_R"]
    )


def evaluate_grid(rows, args):
    groups = split_train_holdout(rows)
    results = []
    for rule in generate_grid_rules():
        all_metrics = metric_block(groups["ALL"], rule.fn)
        train_metrics = metric_block(groups["TRAIN"], rule.fn)
        holdout_metrics = metric_block(groups["HOLDOUT"], rule.fn)
        label = classify_grid_result(all_metrics, train_metrics, holdout_metrics, args, rule.diagnostic)
        coverage_bits = [f"{field}:{field_non_null_pct(rows, field):.0f}%" for field in rule.fields]
        notes = "diagnostic only" if rule.diagnostic else ""
        if coverage_bits:
            notes = (notes + "; " if notes else "") + "coverage " + ",".join(coverage_bits)
        results.append(
            {
                "rule": rule,
                "label": label,
                "score": grid_score(all_metrics, holdout_metrics),
                "ALL": all_metrics,
                "TRAIN": train_metrics,
                "HOLDOUT": holdout_metrics,
                "notes": notes,
            }
        )
    return results


def grid_row(rank, item):
    all_metrics = item["ALL"]
    train_metrics = item["TRAIN"]
    holdout_metrics = item["HOLDOUT"]
    return {
        "rank": rank,
        "rule_name": item["rule"].name,
        "label": item["label"],
        "ALL_avoided_R": all_metrics["avoided_R"],
        "TRAIN_avoided_R": train_metrics["avoided_R"],
        "HOLDOUT_avoided_R": holdout_metrics["avoided_R"],
        "ALL_collapse%": all_metrics["collapse_pct"],
        "HOLDOUT_collapse%": holdout_metrics["collapse_pct"],
        "blocked_resolved": all_metrics["blocked_resolved"],
        "kept_R": all_metrics["net_R_kept"],
        "maxDD_R": all_metrics["max_dd_R"],
        "notes": item["notes"],
    }


def print_grid_output(rows, args, paths=None, stats=None, join_counts=None):
    print("PAPER quality simulator v0.1 grid experiment mode")
    print_method_disclaimers()
    groups = split_train_holdout(rows)
    status_counts = Counter(status_of(row) for row in rows)
    if paths and stats:
        decisions_path, outcomes_path, router_path = paths
        decision_stats, outcome_stats, regime_stats = stats
        print()
        print("Inputs read")
        print(f"- decisions: {decisions_path} loaded={decision_stats.get('loaded', 0)} parse_errors={decision_stats.get('parse_errors', 0)} deduped={decision_stats.get('deduped', 0)}")
        print(f"- outcomes:  {outcomes_path} loaded={outcome_stats.get('loaded', 0)} parse_errors={outcome_stats.get('parse_errors', 0)} dedup_keys={outcome_stats.get('dedup_keys', 0)}")
        print(f"- router:    {router_path} loaded={regime_stats.get('loaded', 0)} parse_errors={regime_stats.get('parse_errors', 0)} symbols={regime_stats.get('symbols', 0)}")
    if join_counts is not None:
        print(f"Join counts: {dict(join_counts)}")
    print()
    print("Dataset summary")
    print(f"- total={len(rows)} resolved={status_counts.get('RESOLVED', 0)} open={status_counts.get('OPEN', 0)} data_missing={status_counts.get('DATA_MISSING', 0)} no_outcome={status_counts.get('NO_OUTCOME', 0)}")
    print(f"- train_resolved={sum(1 for row in groups['TRAIN'] if status_of(row) == 'RESOLVED')} holdout_resolved={sum(1 for row in groups['HOLDOUT'] if status_of(row) == 'RESOLVED')}")
    print_table("Field coverage summary", ["field", "total", "non_null", "non_null%", "UNKNOWN", "UNKNOWN%"], field_coverage(rows))

    results = evaluate_grid(rows, args)
    practical = [item for item in results if item["label"] == "PRACTICAL_CANDIDATE"]
    if practical:
        ranked = sorted(practical, key=lambda item: item["score"], reverse=True)[: args.top]
    else:
        ranked = sorted(
            [item for item in results if item["label"] not in {"DIAGNOSTIC", "DEGENERATE"}],
            key=lambda item: item["score"],
            reverse=True,
        )[: args.top]
    print_table(
        "Top practical candidates" if practical else "Top non-diagnostic candidates (no PRACTICAL_CANDIDATE met filters)",
        ["rank", "rule_name", "label", "ALL_avoided_R", "TRAIN_avoided_R", "HOLDOUT_avoided_R", "ALL_collapse%", "HOLDOUT_collapse%", "blocked_resolved", "kept_R", "maxDD_R", "notes"],
        [grid_row(index + 1, item) for index, item in enumerate(ranked)],
    )

    label_counts = Counter(item["label"] for item in results)
    print_table("Grid candidate label counts", ["label", "count"], [{"label": key, "count": value} for key, value in label_counts.most_common()])

    high_collapse = sorted(
        [item for item in results if item["label"] == "HIGH_COLLAPSE"],
        key=lambda item: item["ALL"]["avoided_R"],
        reverse=True,
    )[:5]
    print_table(
        "Top high avoided_R but high-collapse examples",
        ["rank", "rule_name", "label", "ALL_avoided_R", "TRAIN_avoided_R", "HOLDOUT_avoided_R", "ALL_collapse%", "HOLDOUT_collapse%", "blocked_resolved", "kept_R", "maxDD_R", "notes"],
        [grid_row(index + 1, item) for index, item in enumerate(high_collapse)],
    )

    diagnostic = sorted(
        [item for item in results if item["label"] == "DIAGNOSTIC"],
        key=lambda item: item["ALL"]["avoided_R"],
        reverse=True,
    )[:10]
    print_table(
        "Diagnostic-only candidates",
        ["rank", "rule_name", "label", "ALL_avoided_R", "TRAIN_avoided_R", "HOLDOUT_avoided_R", "ALL_collapse%", "HOLDOUT_collapse%", "blocked_resolved", "kept_R", "maxDD_R", "notes"],
        [grid_row(index + 1, item) for index, item in enumerate(diagnostic)],
    )

    print()
    print("Grid interpretation")
    if practical:
        print(f"- {len(practical)} PRACTICAL_CANDIDATE rules met the configured filters; treat them as PAPER shadow-first soft-modifier research only.")
        print(f"- Highest ranked practical rule: {ranked[0]['rule'].name}.")
    else:
        print("- No PRACTICAL_CANDIDATE rule met the configured filters; use the ranked non-diagnostic list only for research triage.")
    if high_collapse:
        print("- High-collapse rules may show attractive avoided_R but are too broad for actionability in current data.")
    print("- Diagnostic RR and NO_ROUTER rules are excluded from practical gate interpretation.")
    print("- No LIVE or production change should be inferred from this grid.")


def print_breakdowns(rows, gate_name, gate):
    kept_resolved = [row for row in rows if not gate(row) and status_of(row) == "RESOLVED"]
    for field in ("regime", "bos_quality", "structural_decision_shadow", "weak_structure_extended", "candidate_type", "symbol"):
        grouped = defaultdict(list)
        for row in kept_resolved:
            grouped[str(row.get(field, "UNKNOWN"))].append(row)
        table = []
        for key, group_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))[:20]:
            r_values = [realized_r(row) for row in group_rows]
            table.append(
                {
                    field: key,
                    "n": len(group_rows),
                    "net_R": sum(r_values),
                    "WR%": pct(sum(1 for value in r_values if value > 0), len(r_values)),
                    "median_mfe": median([as_float(row.get("_outcome", {}).get("mfe_r"), 0.0) or 0.0 for row in group_rows]),
                }
            )
        print_table(f"Breakdown for {gate_name} kept resolved by {field}", [field, "n", "net_R", "WR%", "median_mfe"], table)


def resolved_values(rows, field):
    values = []
    for row in rows:
        value = as_float((row.get("_outcome") or {}).get(field))
        if value is not None:
            values.append(value)
    return values


def sl_before_half_count(rows):
    return sum(
        1
        for row in rows
        if norm((row.get("_outcome") or {}).get("first_hit"), "") == "SL"
        and (as_float((row.get("_outcome") or {}).get("mfe_r"), 0.0) or 0.0) < 0.5
    )


def blocked_summary(rows, gate):
    blocked = [row for row in rows if gate(row)]
    resolved = [row for row in blocked if status_of(row) == "RESOLVED"]
    r_values = [realized_r(row) for row in resolved]
    mfe_values = resolved_values(resolved, "mfe_r")
    mae_values = resolved_values(resolved, "mae_r")
    net_r = sum(r_values)
    return {
        "blocked_total": len(blocked),
        "blocked_resolved": len(resolved),
        "blocked_open": sum(1 for row in blocked if status_of(row) == "OPEN"),
        "blocked_data_missing": sum(1 for row in blocked if status_of(row) == "DATA_MISSING"),
        "blocked_R": net_r,
        "avoided_R": -net_r,
        "WR%": pct(sum(1 for value in r_values if value > 0), len(resolved)),
        "median_mfe": median(mfe_values),
        "mean_mfe": mean(mfe_values),
        "median_mae": median(mae_values),
        "hit_0.5R%": pct(sum(1 for value in mfe_values if value >= 0.5), len(resolved)),
        "hit_1R%": pct(sum(1 for row in resolved if as_bool((row.get("_outcome") or {}).get("hit_1r"))), len(resolved)),
        "hit_1.5R%": pct(sum(1 for row in resolved if as_bool((row.get("_outcome") or {}).get("hit_1_5r"))), len(resolved)),
        "hit_2R%": pct(sum(1 for row in resolved if as_bool((row.get("_outcome") or {}).get("hit_2r"))), len(resolved)),
        "SL_before_0.5R%": pct(sl_before_half_count(resolved), len(resolved)),
    }


def blocked_breakdown_table(rows, field_getter, field_name, top=None, include_sl_before_half=False):
    grouped = defaultdict(list)
    for row in rows:
        grouped[field_getter(row)].append(row)
    table = []
    for key, group_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), str(item[0]))):
        r_values = [realized_r(row) for row in group_rows]
        mfe_values = resolved_values(group_rows, "mfe_r")
        row = {
            field_name: key,
            "n": len(group_rows),
            "net_R": sum(r_values),
            "avoided_R": -sum(r_values),
            "WR%": pct(sum(1 for value in r_values if value > 0), len(group_rows)),
            "median_mfe": median(mfe_values),
        }
        if include_sl_before_half:
            row["SL_before_0.5R%"] = pct(sl_before_half_count(group_rows), len(group_rows))
        table.append(row)
    return table[:top] if top else table


def gate_by_name(name):
    requested = norm(name, "")
    for gate_name, gate in GATES:
        if norm(gate_name, "") == requested:
            return gate_name, gate
    return None, None


def print_blocked_breakdowns(rows, requested_gate):
    gate_name, gate = gate_by_name(requested_gate)
    if gate is None:
        print(f"Unknown blocked breakdown gate: {requested_gate}")
        print(f"Available gates: {', '.join(gate_name for gate_name, _ in GATES)}")
        return
    blocked = [row for row in rows if gate(row)]
    blocked_resolved = [row for row in blocked if status_of(row) == "RESOLVED"]

    print()
    print(f"Blocked-row breakdown for {gate_name}")
    print_method_disclaimers()
    print_table("Overall blocked summary", ["blocked_total", "blocked_resolved", "blocked_open", "blocked_data_missing", "blocked_R", "avoided_R", "WR%", "median_mfe", "mean_mfe", "median_mae", "hit_0.5R%", "hit_1R%", "hit_1.5R%", "hit_2R%", "SL_before_0.5R%"], [blocked_summary(rows, gate)])
    print_table("Blocked resolved by exact regime", ["regime", "n", "net_R", "avoided_R", "WR%", "median_mfe", "SL_before_0.5R%"], blocked_breakdown_table(blocked_resolved, lambda row: row.get("regime", "UNKNOWN"), "regime", include_sl_before_half=True))
    print_table("Blocked resolved by symbol top 20", ["symbol", "n", "net_R", "avoided_R", "WR%", "median_mfe"], blocked_breakdown_table(blocked_resolved, lambda row: row.get("symbol", "UNKNOWN"), "symbol", top=20))
    print_table("Blocked resolved by bos_quality", ["bos_quality", "n", "net_R", "avoided_R", "WR%", "median_mfe"], blocked_breakdown_table(blocked_resolved, lambda row: row.get("bos_quality", "UNKNOWN"), "bos_quality"))
    print_table("Blocked resolved by structural_decision_shadow", ["structural_decision_shadow", "n", "net_R", "avoided_R", "WR%", "median_mfe"], blocked_breakdown_table(blocked_resolved, lambda row: row.get("structural_decision_shadow", "UNKNOWN"), "structural_decision_shadow"))
    print_table("Blocked resolved by weak_structure_extended", ["weak_structure_extended", "n", "net_R", "avoided_R", "WR%", "median_mfe"], blocked_breakdown_table(blocked_resolved, lambda row: str(row.get("weak_structure_extended", "UNKNOWN")), "weak_structure_extended"))
    print_table("Blocked resolved by candidate_type", ["candidate_type", "n", "net_R", "avoided_R", "WR%", "median_mfe"], blocked_breakdown_table(blocked_resolved, lambda row: row.get("candidate_type", "UNKNOWN"), "candidate_type"))
    print_table("Blocked resolved by regime + bos_quality", ["regime+bos_quality", "n", "net_R", "avoided_R", "WR%", "median_mfe"], blocked_breakdown_table(blocked_resolved, lambda row: f"{row.get('regime', 'UNKNOWN')} + {row.get('bos_quality', 'UNKNOWN')}", "regime+bos_quality"))
    print_table("Blocked resolved by regime + structural_decision_shadow", ["regime+structural_decision_shadow", "n", "net_R", "avoided_R", "WR%", "median_mfe"], blocked_breakdown_table(blocked_resolved, lambda row: f"{row.get('regime', 'UNKNOWN')} + {row.get('structural_decision_shadow', 'UNKNOWN')}", "regime+structural_decision_shadow"))
    print_table("Blocked resolved by regime + candidate_type", ["regime+candidate_type", "n", "net_R", "avoided_R", "WR%", "median_mfe"], blocked_breakdown_table(blocked_resolved, lambda row: f"{row.get('regime', 'UNKNOWN')} + {row.get('candidate_type', 'UNKNOWN')}", "regime+candidate_type"))


def known_unknown_enums(rows):
    bos = Counter(norm(row.get("bos_quality")) for row in rows)
    confirm = Counter(norm(row.get("bos_confirmation")) for row in rows)
    return bos, confirm


def field_coverage(rows):
    fields = [
        "bos_confirmation",
        "weak_structure_extended",
        "structural_decision_shadow",
        "bos_quality",
        "regime",
    ]
    table = []
    total = len(rows)
    for field in fields:
        present = [row for row in rows if row.get(field) is not None and row.get(field) != ""]
        unknown = [row for row in present if norm(row.get(field)) == "UNKNOWN"]
        table.append(
            {
                "field": field,
                "total": total,
                "non_null": len(present),
                "non_null%": pct(len(present), total),
                "UNKNOWN": len(unknown),
                "UNKNOWN%": pct(len(unknown), len(present)),
            }
        )
    return table


def print_method_disclaimers():
    print("This is an avoided-loss simulation, not a full portfolio simulation.")
    print("Replacement trade selection is unknown.")
    print("Full scan replay is not performed.")
    print("Use results for log-only / soft-modifier design only; not for LIVE or production changes.")
    print("WR convention: wins / kept resolved rows; BE rows remain in the denominator and are not wins.")
    print("Unknown resolved first_hit values map to 0R for net_R evaluation.")


def reversal_realized_r(row):
    first_hit = norm(row.get("first_hit"), "")
    mfe = as_float(row.get("mfe_r"), 0.0) or 0.0
    if first_hit == "SL":
        return -1.0
    if first_hit == "TP":
        return (
            as_float(row.get("tp_r"))
            or as_float(row.get("target_r"))
            or as_float(row.get("planned_rr"))
            or as_float(row.get("rr"))
            or 0.0
        )
    if first_hit == "2R":
        return 2.0
    if first_hit in {"1.5R", "1_5R"}:
        return 1.5
    if first_hit == "1R":
        return 1.0
    if first_hit == "0.5R":
        return 0.5
    if first_hit == "AMBIGUOUS":
        return min(mfe, 2.0)
    return 0.0


def reversal_status(row):
    status = norm(row.get("status"), "UNKNOWN")
    if status in {"RESOLVED", "OPEN", "DATA_MISSING", "EXPIRED"}:
        return status
    return status


def reversal_hit(row, threshold):
    key = {
        1.0: "hit_1r",
        1.5: "hit_1_5r",
        2.0: "hit_2r",
    }.get(threshold)
    if key and key in row:
        return as_bool(row.get(key))
    first_hit = norm(row.get("first_hit"), "")
    first_hit_r = {
        "0.5R": 0.5,
        "1R": 1.0,
        "1.5R": 1.5,
        "1_5R": 1.5,
        "2R": 2.0,
    }.get(first_hit)
    if first_hit == "TP":
        first_hit_r = reversal_realized_r(row)
    if first_hit_r is not None and first_hit_r >= threshold:
        return True
    return (as_float(row.get("mfe_r"), 0.0) or 0.0) >= threshold


def reversal_max_drawdown(rows):
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for row in sorted(rows, key=lambda item: (item.get("_source_ts") is None, item.get("_source_ts") or 0.0)):
        equity += reversal_realized_r(row)
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def reversal_overall_metrics(rows):
    status_counts = Counter(reversal_status(row) for row in rows)
    resolved = [row for row in rows if reversal_status(row) == "RESOLVED"]
    r_values = [reversal_realized_r(row) for row in resolved]
    mfe_values = [as_float(row.get("mfe_r")) for row in resolved]
    mae_values = [as_float(row.get("mae_r")) for row in resolved]
    mfe_values = [value for value in mfe_values if value is not None]
    mae_values = [value for value in mae_values if value is not None]
    time_to_1r = [as_float(row.get("time_to_1r_secs")) for row in resolved]
    time_to_1r = [value for value in time_to_1r if value is not None]
    first_hits = Counter(norm(row.get("first_hit"), "UNKNOWN") for row in resolved)
    return {
        "total": len(rows),
        "resolved": len(resolved),
        "open": status_counts.get("OPEN", 0),
        "data_missing": status_counts.get("DATA_MISSING", 0),
        "expired": status_counts.get("EXPIRED", 0),
        "net_R": sum(r_values),
        "WR%": pct(sum(1 for value in r_values if value > 0), len(resolved)),
        "BE_count": first_hits.get("BE", 0),
        "AMBIGUOUS_count": first_hits.get("AMBIGUOUS", 0),
        "SL_first%": pct(first_hits.get("SL", 0), len(resolved)),
        "mean_mfe_R": mean(mfe_values),
        "median_mfe_R": median(mfe_values),
        "mean_mae_R": mean(mae_values),
        "median_mae_R": median(mae_values),
        "hit_0.5R%": pct(sum(1 for row in resolved if reversal_hit(row, 0.5)), len(resolved)),
        "hit_1R%": pct(sum(1 for row in resolved if reversal_hit(row, 1.0)), len(resolved)),
        "hit_1.5R%": pct(sum(1 for row in resolved if reversal_hit(row, 1.5)), len(resolved)),
        "hit_2R%": pct(sum(1 for row in resolved if reversal_hit(row, 2.0)), len(resolved)),
        "SL_before_0.5R%": pct(
            sum(
                1
                for row in resolved
                if norm(row.get("first_hit"), "") == "SL"
                and (as_float(row.get("mfe_r"), 0.0) or 0.0) < 0.5
            ),
            len(resolved),
        ),
        "time_to_1R_median_secs": median(time_to_1r),
        "maxDD_R": reversal_max_drawdown(resolved),
    }


def smc_v0_2_parse_ts(value, default=None):
    parsed = as_float(value)
    if parsed is not None:
        return parsed
    if not value:
        return default
    text = str(value).strip()
    if not text:
        return default
    try:
        return datetime.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return default


def smc_v0_2_status(row):
    status = norm(row.get("status"), "UNKNOWN")
    if status in {"OPEN", "RESOLVED", "DATA_MISSING", "EXPIRED"}:
        return status
    return status


def smc_v0_2_normalize_token(value):
    return norm(value, "").replace("-", "_").replace(" ", "_")


def smc_v0_2_computed_slice(row):
    tracker_or_phase = (
        row.get("tracker_version") == "paper_smc_v0_2_shadow_v1"
        or smc_v0_2_normalize_token(row.get("phase")) in {"SMC_V0_2_SHADOW", "PAPER_SMC_V0_2_SHADOW"}
    )
    if (
        tracker_or_phase
        and as_bool(row.get("grid_rule_v0_2_match"))
        and smc_v0_2_normalize_token(row.get("regime_context_regime")) == "EXHAUSTION_REVERSAL"
        and smc_v0_2_normalize_token(row.get("suppress_reason")) == "SYMBOL_ALREADY_OPEN"
    ):
        return SMC_V0_2_SLICE_EXH_REV_SYMBOL_ALREADY_OPEN
    return None


def smc_v0_2_slice(row):
    existing = row.get("smc_v0_2_slice")
    if existing not in (None, ""):
        return str(existing)
    return smc_v0_2_computed_slice(row)


def smc_v0_2_sort_ts(row):
    for key in ("observed_at", "resolved_at", "source_timestamp", "signal_created_ts", "timestamp_unix"):
        value = smc_v0_2_parse_ts(row.get(key))
        if value is not None:
            return value
    return -1.0


def smc_v0_2_keep_candidate(current, candidate):
    if current is None:
        return True
    current_terminal = smc_v0_2_status(current) in SMC_V0_2_TERMINAL_STATUSES
    candidate_terminal = smc_v0_2_status(candidate) in SMC_V0_2_TERMINAL_STATUSES
    if candidate_terminal and not current_terminal:
        return True
    if current_terminal and not candidate_terminal:
        return False
    return smc_v0_2_sort_ts(candidate) >= smc_v0_2_sort_ts(current)


def load_smc_v0_2_shadow_outcomes(path):
    raw, stats = load_jsonl(path)
    by_key = {}
    missing_key = 0
    for index, row in enumerate(raw, start=1):
        if norm(row.get("event_type"), "") not in {"", "PAPER_SMC_V0_2_SHADOW_OUTCOME"}:
            continue
        item = dict(row)
        item["_source_ts"] = smc_v0_2_parse_ts(
            item.get("source_timestamp"),
            smc_v0_2_parse_ts(item.get("signal_created_ts"), smc_v0_2_parse_ts(item.get("observed_at"))),
        )
        item["_sort_ts"] = smc_v0_2_sort_ts(item)
        item["smc_v0_2_slice"] = smc_v0_2_slice(item)
        key = item.get("dedup_key")
        if not key:
            missing_key += 1
            key = f"__missing_dedup_key_line_{index}"
        item["_dedup_key"] = key
        if smc_v0_2_keep_candidate(by_key.get(key), item):
            by_key[key] = item
    rows = list(by_key.values())
    rows.sort(key=lambda item: (item.get("_sort_ts") is None, item.get("_sort_ts") or -1.0))
    stats["loaded"] = len(raw)
    stats["deduped"] = len(rows)
    stats["missing_dedup_key"] = missing_key
    return rows, stats


def smc_v0_2_planned_r(row):
    return (
        as_float(row.get("planned_rr"))
        or as_float(row.get("tp_r"))
        or as_float(row.get("target_r"))
        or as_float(row.get("rr"))
        or 0.0
    )


def smc_v0_2_realized_r(row):
    first_hit = norm(row.get("first_hit"), "")
    if first_hit == "SL":
        return -1.0
    if first_hit == "TP":
        return smc_v0_2_planned_r(row)
    if first_hit == "2R":
        return 2.0
    if first_hit in {"1.5R", "1_5R"}:
        return 1.5
    if first_hit == "1R":
        return 1.0
    if first_hit == "0.5R":
        return 0.5
    if first_hit in {"AMBIGUOUS", "AMBIGUOUS_SAME_BAR"}:
        return 0.0
    return 0.0


def smc_v0_2_mfe_r(row, default=None):
    return as_float(row.get("MFE_R", row.get("mfe_r")), default)


def smc_v0_2_mae_r(row, default=None):
    return as_float(row.get("MAE_R", row.get("mae_r")), default)


def smc_v0_2_hit(row, threshold):
    key_map = {
        0.5: ("hit_0_5r", "hit_0.5r", "hit_half_r"),
        1.0: ("hit_1r",),
        1.5: ("hit_1_5r", "hit_1.5r"),
        2.0: ("hit_2r",),
    }
    for key in key_map.get(threshold, ()):
        if key in row:
            return as_bool(row.get(key))
    first_hit = norm(row.get("first_hit"), "")
    first_hit_r = {
        "0.5R": 0.5,
        "1R": 1.0,
        "1.5R": 1.5,
        "1_5R": 1.5,
        "2R": 2.0,
    }.get(first_hit)
    if first_hit == "TP":
        first_hit_r = smc_v0_2_realized_r(row)
    if first_hit_r is not None and first_hit_r >= threshold:
        return True
    return (smc_v0_2_mfe_r(row, 0.0) or 0.0) >= threshold


def smc_v0_2_max_drawdown(rows):
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for row in sorted(rows, key=lambda item: (item.get("_sort_ts") is None, item.get("_sort_ts") or 0.0)):
        equity += smc_v0_2_realized_r(row)
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def smc_v0_2_resolved_rows(rows):
    return sorted(
        [row for row in rows if smc_v0_2_status(row) == "RESOLVED"],
        key=lambda item: (item.get("_sort_ts") is None, item.get("_sort_ts") or 0.0),
    )


def smc_v0_2_metrics(rows):
    status_counts = Counter(smc_v0_2_status(row) for row in rows)
    resolved = smc_v0_2_resolved_rows(rows)
    r_values = [smc_v0_2_realized_r(row) for row in resolved]
    mfe_values = [smc_v0_2_mfe_r(row) for row in resolved]
    mae_values = [smc_v0_2_mae_r(row) for row in resolved]
    mfe_values = [value for value in mfe_values if value is not None]
    mae_values = [value for value in mae_values if value is not None]
    time_to_1r = [as_float(row.get("time_to_1r_secs")) for row in resolved]
    time_to_sl = [as_float(row.get("time_to_sl_secs")) for row in resolved]
    time_to_1r = [value for value in time_to_1r if value is not None]
    time_to_sl = [value for value in time_to_sl if value is not None]
    first_hits = Counter(norm(row.get("first_hit"), "UNKNOWN") for row in resolved)
    return {
        "total": len(rows),
        "resolved": len(resolved),
        "open": status_counts.get("OPEN", 0),
        "data_missing": status_counts.get("DATA_MISSING", 0),
        "expired": status_counts.get("EXPIRED", 0),
        "net_R": sum(r_values),
        "WR%": pct(sum(1 for value in r_values if value > 0), len(resolved)),
        "SL_first%": pct(first_hits.get("SL", 0), len(resolved)),
        "AMBIGUOUS_count": first_hits.get("AMBIGUOUS", 0) + first_hits.get("AMBIGUOUS_SAME_BAR", 0),
        "AMBIGUOUS%": pct(first_hits.get("AMBIGUOUS", 0) + first_hits.get("AMBIGUOUS_SAME_BAR", 0), len(resolved)),
        "mean_mfe_R": mean(mfe_values),
        "median_mfe_R": median(mfe_values),
        "mean_mae_R": mean(mae_values),
        "median_mae_R": median(mae_values),
        "hit_0.5R%": pct(sum(1 for row in resolved if smc_v0_2_hit(row, 0.5)), len(resolved)),
        "hit_1R%": pct(sum(1 for row in resolved if smc_v0_2_hit(row, 1.0)), len(resolved)),
        "hit_1.5R%": pct(sum(1 for row in resolved if smc_v0_2_hit(row, 1.5)), len(resolved)),
        "hit_2R%": pct(sum(1 for row in resolved if smc_v0_2_hit(row, 2.0)), len(resolved)),
        "SL_before_0.5R%": pct(
            sum(
                1
                for row in resolved
                if norm(row.get("first_hit"), "") == "SL"
                and (smc_v0_2_mfe_r(row, 0.0) or 0.0) < 0.5
            ),
            len(resolved),
        ),
        "time_to_1R_median_secs": median(time_to_1r),
        "time_to_SL_median_secs": median(time_to_sl),
        "maxDD_R": smc_v0_2_max_drawdown(resolved),
    }


def smc_v0_2_router_age_bucket(row):
    value = None
    for key in ("router_regime_age_secs", "router_regime_age", "regime_context_age_secs"):
        value = as_float(row.get(key))
        if value is not None:
            break
    if value is None or value < 0:
        return "stale/unknown"
    if value <= 1.0:
        return "0-1s"
    if value <= 5.0:
        return "1-5s"
    if value <= 30.0:
        return "5-30s"
    return "stale/unknown"


def smc_v0_2_breakdown_value(row, field):
    if field == "router_regime_age_bucket":
        return smc_v0_2_router_age_bucket(row)
    value = row.get(field)
    if isinstance(value, list):
        return ",".join(str(item) for item in value) if value else "NONE"
    if value in (None, ""):
        return "MISSING"
    return str(value)


def smc_v0_2_breakdown_rows(rows, field, top=None):
    grouped = defaultdict(list)
    for row in rows:
        grouped[smc_v0_2_breakdown_value(row, field)].append(row)
    table = []
    for key, group_rows in grouped.items():
        metrics = smc_v0_2_metrics(group_rows)
        table.append(
            {
                field: key,
                "total": metrics["total"],
                "resolved": metrics["resolved"],
                "open": metrics["open"],
                "data_missing": metrics["data_missing"],
                "expired": metrics["expired"],
                "net_R": metrics["net_R"],
                "WR%": metrics["WR%"],
                "SL_first%": metrics["SL_first%"],
                "median_mfe_R": metrics["median_mfe_R"],
                "maxDD_R": metrics["maxDD_R"],
            }
        )
    table.sort(key=lambda item: (-item["resolved"], -abs(item["net_R"]), str(item[field])))
    return table[:top] if top else table


def smc_v0_2_split_resolved(rows):
    resolved = smc_v0_2_resolved_rows(rows)
    cutoff = int(len(resolved) * 0.70)
    return [("TRAIN", resolved[:cutoff]), ("HOLDOUT", resolved[cutoff:])]


def smc_v0_2_thirds(rows):
    resolved = smc_v0_2_resolved_rows(rows)
    if len(resolved) < 3:
        return []
    result = []
    for index, label in enumerate(("EARLY", "MIDDLE", "LATE")):
        start = int(len(resolved) * index / 3)
        end = int(len(resolved) * (index + 1) / 3)
        result.append((label, resolved[start:end]))
    return result


def smc_v0_2_stability_classification(rows):
    resolved = smc_v0_2_resolved_rows(rows)
    if len(resolved) < 30:
        return "TOO_EARLY"
    if len(resolved) < 100:
        return "EARLY_SIGNAL"
    split = dict(smc_v0_2_split_resolved(rows))
    train_net = smc_v0_2_metrics(split.get("TRAIN", []))["net_R"]
    holdout_net = smc_v0_2_metrics(split.get("HOLDOUT", []))["net_R"]
    thirds_positive = sum(1 for _, third_rows in smc_v0_2_thirds(rows) if smc_v0_2_metrics(third_rows)["net_R"] > 0)
    if train_net > 0 and holdout_net > 0 and thirds_positive >= 2:
        return "STABLE_SHADOW_CONFIRMATION"
    if train_net < 0 and holdout_net < 0:
        return "REJECTED"
    return "MIXED"


def smc_v0_2_stability_table(rows):
    table = []
    for split_name, split_rows in smc_v0_2_split_resolved(rows):
        metrics = smc_v0_2_metrics(split_rows)
        table.append(
            {
                "split": split_name,
                "resolved": metrics["resolved"],
                "net_R": metrics["net_R"],
                "WR%": metrics["WR%"],
                "maxDD_R": metrics["maxDD_R"],
            }
        )
    for split_name, split_rows in smc_v0_2_thirds(rows):
        metrics = smc_v0_2_metrics(split_rows)
        table.append(
            {
                "split": split_name,
                "resolved": metrics["resolved"],
                "net_R": metrics["net_R"],
                "WR%": metrics["WR%"],
                "maxDD_R": metrics["maxDD_R"],
            }
        )
    return table


def smc_v0_2_top3_symbol_abs_net_concentration(rows):
    resolved = smc_v0_2_resolved_rows(rows)
    grouped = defaultdict(list)
    for row in resolved:
        grouped[row.get("symbol") or "UNKNOWN"].append(row)
    symbol_abs = sorted(
        (abs(sum(smc_v0_2_realized_r(row) for row in group_rows)) for group_rows in grouped.values()),
        reverse=True,
    )
    total_abs = sum(symbol_abs)
    return pct(sum(symbol_abs[:3]), total_abs)


def smc_v0_2_first_hit_summary(rows):
    counts = Counter(norm(row.get("first_hit"), "UNKNOWN") for row in smc_v0_2_resolved_rows(rows))
    if not counts:
        return ""
    return ", ".join(f"{key}:{value}" for key, value in counts.most_common())


def smc_v0_2_thirds_summary(rows):
    parts = []
    for split_name, split_rows in smc_v0_2_thirds(rows):
        metrics = smc_v0_2_metrics(split_rows)
        parts.append(f"{split_name}:{metrics['net_R']:.2f}R/{metrics['resolved']}n")
    return "; ".join(parts)


def smc_v0_2_slice_classification(rows):
    metrics = smc_v0_2_metrics(rows)
    split = dict(smc_v0_2_split_resolved(rows))
    train_metrics = smc_v0_2_metrics(split.get("TRAIN", []))
    holdout_metrics = smc_v0_2_metrics(split.get("HOLDOUT", []))
    third_metrics = [smc_v0_2_metrics(third_rows) for _, third_rows in smc_v0_2_thirds(rows)]
    non_positive_thirds = sum(1 for item in third_metrics if item["net_R"] <= 0)
    top3_concentration = smc_v0_2_top3_symbol_abs_net_concentration(rows)
    if metrics["resolved"] < 50:
        return "TOO_SMALL"
    if (
        metrics["resolved"] >= 100
        and holdout_metrics["resolved"] >= 50
        and metrics["net_R"] < 0
        and holdout_metrics["net_R"] < 0
        and non_positive_thirds >= 2
        and (metrics["median_mfe_R"] < 0.8 or metrics["SL_before_0.5R%"] >= 50.0)
        and top3_concentration <= 50.0
    ):
        return "BAD_STABLE_SHADOW_CANDIDATE"
    train_holdout_mixed = train_metrics["net_R"] * holdout_metrics["net_R"] < 0
    if metrics["resolved"] >= 50 and metrics["net_R"] < 0 and train_holdout_mixed:
        return "BORDERLINE_SHADOW_REDESIGN"
    return "SHADOW_MONITOR_ONLY"


def smc_v0_2_slice_diagnostic_rows(rows):
    grouped = defaultdict(list)
    for row in rows:
        slice_name = smc_v0_2_slice(row)
        if slice_name:
            grouped[slice_name].append(row)
    table = []
    for slice_name, slice_rows in grouped.items():
        metrics = smc_v0_2_metrics(slice_rows)
        split = dict(smc_v0_2_split_resolved(slice_rows))
        train_metrics = smc_v0_2_metrics(split.get("TRAIN", []))
        holdout_metrics = smc_v0_2_metrics(split.get("HOLDOUT", []))
        table.append(
            {
                "smc_v0_2_slice": slice_name,
                "total": metrics["total"],
                "resolved": metrics["resolved"],
                "net_R": metrics["net_R"],
                "WR%": metrics["WR%"],
                "first_hit_distribution": smc_v0_2_first_hit_summary(slice_rows),
                "hit_1R%": metrics["hit_1R%"],
                "hit_1.5R%": metrics["hit_1.5R%"],
                "hit_2R%": metrics["hit_2R%"],
                "SL_first%": metrics["SL_first%"],
                "median_mfe_R": metrics["median_mfe_R"],
                "median_mae_R": metrics["median_mae_R"],
                "train_R": train_metrics["net_R"],
                "train_resolved": train_metrics["resolved"],
                "holdout_R": holdout_metrics["net_R"],
                "holdout_resolved": holdout_metrics["resolved"],
                "chronological_thirds": smc_v0_2_thirds_summary(slice_rows),
                "maxDD_R": metrics["maxDD_R"],
                "top3_symbol_concentration%": smc_v0_2_top3_symbol_abs_net_concentration(slice_rows),
                "classification": smc_v0_2_slice_classification(slice_rows),
            }
        )
    table.sort(key=lambda item: (-item["resolved"], item["smc_v0_2_slice"]))
    return table


def smc_v0_2_warnings(rows, stats, metrics):
    warnings = []
    total = metrics["total"]
    if stats.get("missing"):
        warnings.append("LOG_MISSING")
    if stats.get("parse_errors", 0):
        warnings.append("PARSE_ERRORS")
    if metrics["resolved"] < 30:
        warnings.append("VERY_LOW_SAMPLE")
    elif metrics["resolved"] < 100:
        warnings.append("LOW_SAMPLE")
    if pct(metrics["data_missing"], total) > 30.0:
        warnings.append("HIGH_DATA_MISSING")
    if metrics["open"] > metrics["resolved"] * 3:
        warnings.append("HIGH_OPEN_BACKLOG")
    if metrics["AMBIGUOUS%"] > 10.0:
        warnings.append("HIGH_AMBIGUOUS")
    if smc_v0_2_top3_symbol_abs_net_concentration(rows) > 50.0:
        warnings.append("SYMBOL_CONCENTRATION_WARN")
    if stats.get("missing_dedup_key", 0):
        warnings.append("MISSING_DEDUP_KEY")
    return warnings


def print_smc_v0_2_shadow_output(rows, stats, path, args):
    metrics = smc_v0_2_metrics(rows)
    warnings = smc_v0_2_warnings(rows, stats, metrics)
    status = "WARN" if warnings or stats.get("missing") else "PASS"
    first_hits = Counter(norm(row.get("first_hit"), "UNKNOWN") for row in smc_v0_2_resolved_rows(rows))
    status_counts = Counter(smc_v0_2_status(row) for row in rows)

    print(f"{status} summary")
    print("PAPER quality simulator v0.1 --phase smc_v0_2_shadow")
    print("PAPER_SMC_MAIN v0.2 matched shadow outcome tracker analysis is read-only and forward-only.")
    print()
    print("Files changed")
    print("- paper_quality_sim.py")
    print()
    print("New CLI behavior")
    print("- python paper_quality_sim.py --phase smc_v0_2_shadow")
    print("- Optional: --breakdowns and --stability")
    print("- This phase ignores --write and does not load default decision/outcome/router logs.")
    print()
    print("Dataset summary")
    print(f"- input: {path}")
    print(f"- total raw rows={stats.get('lines', 0)}")
    print(f"- deduped trackers={metrics['total']}")
    print(f"- OPEN={metrics['open']} RESOLVED={metrics['resolved']} DATA_MISSING={metrics['data_missing']} EXPIRED={metrics['expired']}")
    print(f"- parse_errors={stats.get('parse_errors', 0)} missing_dedup_key={stats.get('missing_dedup_key', 0)}")
    print_table("Deduped status counts", ["status", "count"], [{"status": key, "count": value} for key, value in status_counts.most_common()])
    print_table("Resolved first_hit distribution", ["first_hit", "count"], [{"first_hit": key, "count": value} for key, value in first_hits.most_common()])
    print_table(
        "Overall tracker metrics",
        [
            "resolved",
            "net_R",
            "WR%",
            "SL_first%",
            "AMBIGUOUS_count",
            "AMBIGUOUS%",
            "mean_mfe_R",
            "median_mfe_R",
            "mean_mae_R",
            "median_mae_R",
            "hit_0.5R%",
            "hit_1R%",
            "hit_1.5R%",
            "hit_2R%",
            "SL_before_0.5R%",
            "time_to_1R_median_secs",
            "time_to_SL_median_secs",
            "maxDD_R",
        ],
        [metrics],
    )
    if args.breakdowns:
        print()
        print("Breakdowns")
        for field in SMC_V0_2_BREAKDOWN_FIELDS:
            top = 20 if field == "symbol" else None
            print_table(
                f"SMC v0.2 shadow breakdown by {field}" + (" top 20" if top else ""),
                [field, "total", "resolved", "open", "data_missing", "expired", "net_R", "WR%", "SL_first%", "median_mfe_R", "maxDD_R"],
                smc_v0_2_breakdown_rows(rows, field, top=top),
            )
        print_table(
            "SMC v0.2 shadow monitored slice diagnostics",
            [
                "smc_v0_2_slice",
                "total",
                "resolved",
                "net_R",
                "WR%",
                "first_hit_distribution",
                "hit_1R%",
                "hit_1.5R%",
                "hit_2R%",
                "SL_first%",
                "median_mfe_R",
                "median_mae_R",
                "train_R",
                "train_resolved",
                "holdout_R",
                "holdout_resolved",
                "chronological_thirds",
                "maxDD_R",
                "top3_symbol_concentration%",
                "classification",
            ],
            smc_v0_2_slice_diagnostic_rows(rows),
        )
    else:
        print()
        print("Breakdowns")
        print("- Not requested. Pass --breakdowns to print v0.2 shadow slices.")
    if args.stability:
        classification = smc_v0_2_stability_classification(rows)
        print()
        print("Stability")
        print_table("Chronological stability splits", ["split", "resolved", "net_R", "WR%", "maxDD_R"], smc_v0_2_stability_table(rows))
        print(f"- classification={classification}")
    print()
    print("Warnings")
    if warnings:
        for warning in warnings:
            print(f"- {warning}")
    else:
        print("- None.")
    print()
    print("Interpretation")
    if metrics["resolved"] < 30:
        print("- Too early: resolved sample is below 30, so the v0.2 shadow tracker cannot confirm the simulator hypothesis yet.")
    elif metrics["resolved"] < 100:
        print("- Early signal only: the v0.2 shadow tracker has enough resolved rows to inspect direction, but not enough for material confidence.")
    elif metrics["net_R"] > 0:
        print("- The tracker directionally supports the simulator hypothesis on current resolved rows, subject to the warnings above.")
    else:
        print("- The tracker does not currently support the simulator hypothesis on net_R.")
    print("- Do not enable a real penalty yet unless a materially sufficient forward sample remains stable.")
    print("- smc_v0_2_slice is a shadow-only monitoring label and is not a scoring, ranking, dispatch, or execution gate.")
    print()
    print("Safety confirmation")
    print("- Read-only standard-library analysis; no bot imports, no Binance/API calls, no config/state/log mutation, no strategy or LIVE execution changes.")
    print()
    print("Validation results")
    print("- Run commands listed by the user after this patch; py_compile only proves syntax/import safety.")
    print()
    print("Recommended next action only")
    print("- Continue forward-only collection until resolved n is materially sufficient, then rerun with --breakdowns --stability.")


def reversal_breakdown_rows(rows, field, top=None):
    grouped = defaultdict(list)
    for row in rows:
        if field == "expired_reason":
            if reversal_status(row) != "EXPIRED":
                continue
            value = row.get("expired_reason") or row.get("expire_reason") or row.get("reason") or "UNKNOWN"
        elif field == "data_missing_reason":
            if reversal_status(row) != "DATA_MISSING":
                continue
            value = row.get("data_missing_reason") or "UNKNOWN"
        else:
            value = row.get(field)
        if isinstance(value, list):
            value = ",".join(str(item) for item in value) if value else "NONE"
        grouped[str(value if value not in (None, "") else "MISSING")].append(row)
    table = []
    for key, group_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        resolved = [row for row in group_rows if reversal_status(row) == "RESOLVED"]
        r_values = [reversal_realized_r(row) for row in resolved]
        row = {
            field: key,
            "total": len(group_rows),
            "resolved": len(resolved),
            "data_missing%": pct(sum(1 for item in group_rows if reversal_status(item) == "DATA_MISSING"), len(group_rows)),
            "net_R": sum(r_values),
            "WR%": pct(sum(1 for value in r_values if value > 0), len(resolved)),
            "SL_first%": pct(sum(1 for item in resolved if norm(item.get("first_hit"), "") == "SL"), len(resolved)),
            "median_mfe_R": median([as_float(item.get("mfe_r"), 0.0) or 0.0 for item in resolved]),
            "warnings": reversal_bucket_warnings(group_rows, resolved),
        }
        table.append(row)
    return table[:top] if top else table


def reversal_bucket_warnings(group_rows, resolved):
    warnings = []
    if 0 < len(resolved) < LOW_SAMPLE_N:
        warnings.append("LOW_SAMPLE")
    if pct(sum(1 for row in group_rows if reversal_status(row) == "DATA_MISSING"), len(group_rows)) > 40.0:
        warnings.append("DM_DEGRADED")
    return ",".join(warnings)


def reversal_field_coverage(rows, fields):
    table = []
    total = len(rows)
    for field in fields:
        present = [row for row in rows if row.get(field) not in (None, "")]
        missing_pct = pct(total - len(present), total)
        warning = "FIELD_COVERAGE_WARN" if missing_pct > 40.0 else ""
        table.append(
            {
                "field": field,
                "total": total,
                "non_null": len(present),
                "non_null%": pct(len(present), total),
                "missing%": missing_pct,
                "warning": warning,
            }
        )
    return table


def print_reversal_phase_output(rows, stats, path):
    metrics = reversal_overall_metrics(rows)
    status_counts = Counter(reversal_status(row) for row in rows)
    first_hits = Counter(norm(row.get("first_hit"), "UNKNOWN") for row in rows if reversal_status(row) == "RESOLVED")

    print("PAPER quality simulator v0.1 --phase reversal")
    print("REVERSAL phase simulator is outcome-replay only.")
    print("It is not a portfolio simulator.")
    print("It does not model replacement trade selection.")
    print("It does not imply enabling reversal paper/live.")
    print("It does not recommend LIVE changes.")
    print("Safety: standard-library only; no bot imports; no API/network; no log/state/config mutation; no writes in reversal mode.")
    print()
    print("Inputs read")
    print(f"- reversal_outcomes: {path} loaded={stats.get('loaded', 0)} parse_errors={stats.get('parse_errors', 0)} deduped={stats.get('deduped', 0)}")
    print("- SMC_MAIN decision joins/grid skipped for reversal outcome replay.")
    print()
    print("Dataset summary")
    print(f"- total={metrics['total']} resolved={metrics['resolved']} open={metrics['open']} data_missing={metrics['data_missing']} expired={metrics['expired']}")
    print_table("Terminal status counts", ["status", "count"], [{"status": key, "count": value} for key, value in status_counts.most_common()])
    print_table("Resolved first_hit distribution", ["first_hit", "count"], [{"first_hit": key, "count": value} for key, value in first_hits.most_common()])
    print_table(
        "Overall reversal metrics",
        [
            "total",
            "resolved",
            "open",
            "data_missing",
            "expired",
            "net_R",
            "WR%",
            "BE_count",
            "AMBIGUOUS_count",
            "SL_first%",
            "mean_mfe_R",
            "median_mfe_R",
            "mean_mae_R",
            "median_mae_R",
            "hit_0.5R%",
            "hit_1R%",
            "hit_1.5R%",
            "hit_2R%",
            "SL_before_0.5R%",
            "time_to_1R_median_secs",
            "maxDD_R",
        ],
        [metrics],
    )
    print_table("Field coverage summary", ["field", "total", "non_null", "non_null%", "missing%", "warning"], reversal_field_coverage(rows, REVERSAL_BREAKDOWN_FIELDS))
    for field in REVERSAL_BREAKDOWN_FIELDS:
        headers = [field, "total", "resolved", "data_missing%", "net_R", "WR%", "SL_first%", "median_mfe_R", "warnings"]
        top = 20 if field == "symbol" else None
        print_table(f"Reversal breakdown by {field}" + (" top 20" if top else ""), headers, reversal_breakdown_rows(rows, field, top=top))
    print()
    print("Warnings")
    if metrics["resolved"] < LOW_SAMPLE_N:
        print(f"- LOW_SAMPLE overall resolved n={metrics['resolved']}.")
    if pct(metrics["data_missing"], metrics["total"]) > 40.0:
        print(f"- DM_DEGRADED overall DATA_MISSING={pct(metrics['data_missing'], metrics['total']):.1f}%.")
    holdout_resolved = int(metrics["resolved"] * 0.30)
    if holdout_resolved < SMALL_HOLDOUT_RESOLVED_N:
        print(f"- SMALL_HOLDOUT chronological holdout proxy resolved n={holdout_resolved}.")
    missing_warns = [item["field"] for item in reversal_field_coverage(rows, REVERSAL_BREAKDOWN_FIELDS) if item["warning"]]
    if missing_warns:
        print(f"- FIELD_COVERAGE_WARN: {', '.join(missing_warns)} missing on many rows.")
    print("- No rule predicates, gates, threshold tuning, strategy toggles, or LIVE recommendations were produced.")


def reversal_sorted_resolved(rows):
    resolved = [row for row in rows if reversal_status(row) == "RESOLVED"]
    return sorted(resolved, key=lambda item: (item.get("_source_ts") is None, item.get("_source_ts") or 0.0))


def reversal_slice_split_rows(rows, predicate):
    slice_rows = [row for row in rows if predicate(row)]
    resolved = reversal_sorted_resolved(slice_rows)
    cutoff = int(len(resolved) * 0.70)
    train_ids = {id(row) for row in resolved[:cutoff]}
    holdout_ids = {id(row) for row in resolved[cutoff:]}
    return {
        "ALL": slice_rows,
        "TRAIN": [row for row in slice_rows if id(row) in train_ids],
        "HOLDOUT": [row for row in slice_rows if id(row) in holdout_ids],
    }


def reversal_slice_metrics(rows):
    resolved = reversal_sorted_resolved(rows)
    r_values = [reversal_realized_r(row) for row in resolved]
    mfe_values = [as_float(row.get("mfe_r")) for row in resolved]
    mae_values = [as_float(row.get("mae_r")) for row in resolved]
    mfe_values = [value for value in mfe_values if value is not None]
    mae_values = [value for value in mae_values if value is not None]
    return {
        "total": len(rows),
        "resolved": len(resolved),
        "open": sum(1 for row in rows if reversal_status(row) == "OPEN"),
        "data_missing": sum(1 for row in rows if reversal_status(row) == "DATA_MISSING"),
        "expired": sum(1 for row in rows if reversal_status(row) == "EXPIRED"),
        "net_R": sum(r_values),
        "WR%": pct(sum(1 for value in r_values if value > 0), len(resolved)),
        "median_MFE": median(mfe_values),
        "median_MAE": median(mae_values),
        "maxDD_R": reversal_max_drawdown(resolved),
        "hit_1R%": pct(sum(1 for row in resolved if reversal_hit(row, 1.0)), len(resolved)),
        "SL_before_0.5R%": pct(
            sum(
                1
                for row in resolved
                if norm(row.get("first_hit"), "") == "SL"
                and (as_float(row.get("mfe_r"), 0.0) or 0.0) < 0.5
            ),
            len(resolved),
        ),
    }


def reversal_chronological_thirds(rows):
    resolved = reversal_sorted_resolved(rows)
    if len(resolved) < 3:
        return []
    thirds = []
    for index, label in enumerate(("early", "middle", "late")):
        start = int(len(resolved) * index / 3)
        end = int(len(resolved) * (index + 1) / 3)
        thirds.append((label, resolved[start:end]))
    return thirds


def reversal_thirds_label(rows):
    thirds = reversal_chronological_thirds(rows)
    if not thirds:
        return "NO_THIRDS"
    parts = []
    positive = 0
    for label, third_rows in thirds:
        metrics = reversal_slice_metrics(third_rows)
        if metrics["net_R"] > 0:
            positive += 1
        parts.append(f"{label}:{metrics['net_R']:.2f}R/{metrics['WR%']:.1f}%/{metrics['maxDD_R']:.2f}DD")
    if positive == 1:
        parts.append("UNSTABLE")
    return "; ".join(parts)


def reversal_top3_symbol_concentration(rows):
    resolved = reversal_sorted_resolved(rows)
    net_r = sum(reversal_realized_r(row) for row in resolved)
    if net_r <= 0:
        return 0.0
    grouped = defaultdict(list)
    for row in resolved:
        grouped[row.get("symbol") or "UNKNOWN"].append(row)
    symbol_nets = sorted(
        (sum(reversal_realized_r(row) for row in group_rows) for group_rows in grouped.values()),
        reverse=True,
    )
    return pct(sum(value for value in symbol_nets[:3] if value > 0), net_r)


def reversal_symbol_table(rows):
    grouped = defaultdict(list)
    for row in reversal_sorted_resolved(rows):
        grouped[row.get("symbol") or "UNKNOWN"].append(row)
    table = []
    for symbol, group_rows in grouped.items():
        r_values = [reversal_realized_r(row) for row in group_rows]
        table.append(
            {
                "symbol": symbol,
                "resolved": len(group_rows),
                "net_R": sum(r_values),
                "WR%": pct(sum(1 for value in r_values if value > 0), len(group_rows)),
                "median_MFE": median([as_float(row.get("mfe_r"), 0.0) or 0.0 for row in group_rows]),
            }
        )
    return table


def reversal_slice_notes(all_metrics, train_metrics, holdout_metrics, thirds_text, concentration):
    notes = []
    resolved = all_metrics["resolved"]
    if resolved < 30:
        notes.append("VERY_LOW_SAMPLE")
    elif resolved < 100:
        notes.append("LOW_SAMPLE")
    if all_metrics["net_R"] > 0 and (
        all_metrics["maxDD_R"] > abs(all_metrics["net_R"])
        or all_metrics["maxDD_R"] > 2.0 * all_metrics["net_R"]
    ):
        notes.append("HIGH_DD")
    if concentration > 50.0:
        notes.append("SYMBOL_CONCENTRATION_WARN")
    if train_metrics["net_R"] > 0 and holdout_metrics["net_R"] < 0:
        notes.append("TRAIN_ONLY")
    elif holdout_metrics["net_R"] > 0 and train_metrics["net_R"] < 0:
        notes.append("HOLDOUT_ONLY")
    thirds_positive = 0
    thirds = [part for part in thirds_text.split("; ") if ":" in part]
    for part in thirds:
        try:
            value = float(part.split(":", 1)[1].split("R", 1)[0])
        except (IndexError, ValueError):
            value = 0.0
        if value > 0:
            thirds_positive += 1
    if "UNSTABLE" in thirds_text:
        notes.append("UNSTABLE")
    if train_metrics["net_R"] > 0 and holdout_metrics["net_R"] > 0 and thirds_positive >= 2:
        notes.append("STABLE_CANDIDATE")
    return ",".join(notes) if notes else "RESEARCH"


def reversal_required_field_warn(rows):
    required = ("phase", "exhaustion", "bos_confirmation", "side", "symbol")
    if not rows:
        return False
    for field in required:
        missing_pct = pct(sum(1 for row in rows if row.get(field) in (None, "")), len(rows))
        if missing_pct > 40.0:
            return True
    return False


def reversal_stability_rows(rows, slices):
    table = []
    detail = {}
    for slice_name, predicate in slices:
        split_rows = reversal_slice_split_rows(rows, predicate)
        all_metrics = reversal_slice_metrics(split_rows["ALL"])
        train_metrics = reversal_slice_metrics(split_rows["TRAIN"])
        holdout_metrics = reversal_slice_metrics(split_rows["HOLDOUT"])
        thirds_text = reversal_thirds_label(split_rows["ALL"])
        concentration = reversal_top3_symbol_concentration(split_rows["ALL"])
        notes = reversal_slice_notes(all_metrics, train_metrics, holdout_metrics, thirds_text, concentration)
        if reversal_required_field_warn(split_rows["ALL"]):
            notes = f"{notes},FIELD_COVERAGE_WARN" if notes else "FIELD_COVERAGE_WARN"
        row = {
            "slice_name": slice_name,
            "label": "STABLE_CANDIDATE" if "STABLE_CANDIDATE" in notes else "RESEARCH",
            "ALL net_R": all_metrics["net_R"],
            "TRAIN net_R": train_metrics["net_R"],
            "HOLDOUT net_R": holdout_metrics["net_R"],
            "resolved n": all_metrics["resolved"],
            "WR%": all_metrics["WR%"],
            "median MFE": all_metrics["median_MFE"],
            "median MAE": all_metrics["median_MAE"],
            "maxDD_R": all_metrics["maxDD_R"],
            "hit_1R%": all_metrics["hit_1R%"],
            "SL_before_0.5R%": all_metrics["SL_before_0.5R%"],
            "open": all_metrics["open"],
            "data_missing": all_metrics["data_missing"],
            "expired": all_metrics["expired"],
            "thirds result": thirds_text,
            "top3 symbol concentration %": concentration,
            "notes": notes,
        }
        table.append(row)
        detail[slice_name] = {"rows": split_rows["ALL"], "metrics": all_metrics, "predicate": predicate}
    return table, detail


def reversal_side_interaction_rows(rows, slices):
    table = []
    for slice_name, predicate in slices:
        base_rows = [row for row in rows if predicate(row)]
        long_metrics = reversal_slice_metrics([row for row in base_rows if norm(row.get("side")) == "LONG"])
        short_metrics = reversal_slice_metrics([row for row in base_rows if norm(row.get("side")) == "SHORT"])
        if short_metrics["net_R"] > 0 and long_metrics["net_R"] <= 0:
            interpretation = "SHORT_SUPPORT"
        elif long_metrics["net_R"] > 0 and short_metrics["net_R"] <= 0:
            interpretation = "LONG_SUPPORT"
        elif short_metrics["net_R"] > long_metrics["net_R"]:
            interpretation = "SHORT_STRONGER"
        elif long_metrics["net_R"] > short_metrics["net_R"]:
            interpretation = "LONG_STRONGER"
        else:
            interpretation = "MIXED"
        table.append(
            {
                "slice_name": slice_name,
                "LONG net_R / n / WR": f"{long_metrics['net_R']:.2f} / {long_metrics['resolved']} / {long_metrics['WR%']:.2f}",
                "SHORT net_R / n / WR": f"{short_metrics['net_R']:.2f} / {short_metrics['resolved']} / {short_metrics['WR%']:.2f}",
                "interpretation": interpretation,
            }
        )
    return table


def reversal_group_by_symbol(rows):
    grouped = defaultdict(list)
    for row in reversal_sorted_resolved(rows):
        grouped[row.get("symbol") or "UNKNOWN"].append(row)
    return grouped


def reversal_remove_symbols(rows, symbols):
    symbols = set(symbols)
    return [row for row in rows if (row.get("symbol") or "UNKNOWN") not in symbols]


def reversal_symbols_by_net_r(rows, reverse=True):
    grouped = reversal_group_by_symbol(rows)
    table = []
    for symbol, symbol_rows in grouped.items():
        r_values = [reversal_realized_r(row) for row in symbol_rows]
        table.append(
            {
                "symbol": symbol,
                "resolved": len(symbol_rows),
                "net_R": sum(r_values),
                "WR%": pct(sum(1 for value in r_values if value > 0), len(symbol_rows)),
            }
        )
    return sorted(table, key=lambda row: (row["net_R"], row["resolved"], row["symbol"]), reverse=reverse)


def reversal_symbol_normalized_metrics(rows):
    resolved = reversal_sorted_resolved(rows)
    raw = reversal_slice_metrics(resolved)
    split = reversal_slice_split_rows(resolved, lambda row: True)
    train = reversal_slice_metrics(split["TRAIN"])
    holdout = reversal_slice_metrics(split["HOLDOUT"])
    top_symbols = reversal_symbols_by_net_r(resolved, reverse=True)
    bottom_symbols = reversal_symbols_by_net_r(resolved, reverse=False)

    def removed_metrics(symbol_rows, count):
        symbols = [row["symbol"] for row in symbol_rows[:count]]
        return reversal_slice_metrics(reversal_remove_symbols(resolved, symbols))

    top1_removed = removed_metrics(top_symbols, 1)
    top3_removed = removed_metrics(top_symbols, 3)
    top5_removed = removed_metrics(top_symbols, 5)
    bottom1_removed = removed_metrics(bottom_symbols, 1)
    bottom3_removed = removed_metrics(bottom_symbols, 3)

    symbol_nets = [row["net_R"] for row in top_symbols]
    positive_symbols = sum(1 for value in symbol_nets if value > 0)
    negative_symbols = sum(1 for value in symbol_nets if value < 0)
    positive_symbol_rate = pct(positive_symbols, len(symbol_nets))
    top3_concentration = reversal_top3_symbol_concentration(resolved)

    def capped_metrics(limit):
        counts = Counter()
        capped = []
        for row in resolved:
            symbol = row.get("symbol") or "UNKNOWN"
            if counts[symbol] >= limit:
                continue
            counts[symbol] += 1
            capped.append(row)
        return reversal_slice_metrics(capped)

    cap3 = capped_metrics(3)
    cap5 = capped_metrics(5)
    cap10 = capped_metrics(10)
    notes = []
    if raw["resolved"] < 100:
        notes.append("LOW_SAMPLE")
    if top3_concentration > 50.0:
        notes.append("SYMBOL_CONCENTRATION_WARN")
    if top3_removed["net_R"] < 0:
        notes.append("FRAGILE_EDGE")
    median_symbol_r = median(symbol_nets)
    if median_symbol_r <= 0:
        notes.append("MEDIAN_SYMBOL_NEGATIVE")
    if cap5["net_R"] < 0 or cap10["net_R"] < 0:
        notes.append("CAP_FRAGILE")
    if raw["net_R"] > 0 and raw["maxDD_R"] > raw["net_R"]:
        notes.append("HIGH_DD")
    if positive_symbol_rate >= 55.0 and median_symbol_r > 0:
        notes.append("BROAD_EDGE")
    if raw["net_R"] > 0 and (median_symbol_r <= 0 or top3_concentration > 50.0):
        notes.append("CONCENTRATED_EDGE")
    if (
        raw["net_R"] > 0
        and train["net_R"] > 0
        and holdout["net_R"] > 0
        and top3_removed["net_R"] > 0
        and median_symbol_r > 0
        and positive_symbol_rate >= 55.0
    ):
        notes.append("ROBUST_CANDIDATE")

    return {
        "raw": raw,
        "train": train,
        "holdout": holdout,
        "top1_removed": top1_removed,
        "top3_removed": top3_removed,
        "top5_removed": top5_removed,
        "bottom1_removed": bottom1_removed,
        "bottom3_removed": bottom3_removed,
        "top_symbols": top_symbols,
        "bottom_symbols": bottom_symbols,
        "mean_symbol_R": mean(symbol_nets),
        "median_symbol_R": median_symbol_r,
        "positive_symbols": positive_symbols,
        "negative_symbols": negative_symbols,
        "positive_symbol_rate": positive_symbol_rate,
        "avg_symbol_trade_count": mean([row["resolved"] for row in top_symbols]),
        "top3_concentration": top3_concentration,
        "cap3": cap3,
        "cap5": cap5,
        "cap10": cap10,
        "notes": ",".join(notes) if notes else "RESEARCH",
    }


def reversal_symbol_normalized_rows(rows):
    robust_rows = []
    diagnostic_rows = []
    symbol_rows = []
    detail = {}
    for slice_name, label, predicate in REVERSAL_SYMBOL_NORMALIZED_SLICES:
        slice_rows = [row for row in rows if predicate(row)]
        metrics = reversal_symbol_normalized_metrics(slice_rows)
        detail[slice_name] = metrics
        robust_rows.append(
            {
                "slice": slice_name,
                "label": label,
                "raw_R": metrics["raw"]["net_R"],
                "train_R": metrics["train"]["net_R"],
                "holdout_R": metrics["holdout"]["net_R"],
                "top1_removed_R": metrics["top1_removed"]["net_R"],
                "top3_removed_R": metrics["top3_removed"]["net_R"],
                "top5_removed_R": metrics["top5_removed"]["net_R"],
                "median_symbol_R": metrics["median_symbol_R"],
                "positive_symbol_rate": metrics["positive_symbol_rate"],
                "top3_concentration": metrics["top3_concentration"],
                "cap5_R": metrics["cap5"]["net_R"],
                "cap10_R": metrics["cap10"]["net_R"],
                "notes": metrics["notes"],
            }
        )
        if slice_name in {"SHORT", "LONG"}:
            diagnostic_rows.append(
                {
                    "slice": slice_name,
                    "raw_R": metrics["raw"]["net_R"],
                    "top3_removed_R": metrics["top3_removed"]["net_R"],
                    "median_symbol_R": metrics["median_symbol_R"],
                    "positive_symbol_rate": metrics["positive_symbol_rate"],
                    "cap5_R": metrics["cap5"]["net_R"],
                    "notes": metrics["notes"],
                }
            )
        if label == "candidate":
            for rank, symbol_row in enumerate(metrics["top_symbols"][:5], start=1):
                row = dict(symbol_row)
                row.update({"slice": slice_name, "rank_type": "top", "rank": rank})
                symbol_rows.append(row)
            for rank, symbol_row in enumerate(metrics["bottom_symbols"][:5], start=1):
                row = dict(symbol_row)
                row.update({"slice": slice_name, "rank_type": "bottom", "rank": rank})
                symbol_rows.append(row)
    return robust_rows, symbol_rows, diagnostic_rows, detail


def print_reversal_symbol_normalized_output(rows):
    robust_rows, symbol_rows, diagnostic_rows, detail = reversal_symbol_normalized_rows(rows)
    print()
    print("Symbol-normalized reversal robustness")
    print("Research-only concentration stress test. This is not a gate and does not imply PAPER/LIVE enablement.")
    print_table(
        "Symbol-normalized robustness table",
        ["slice", "label", "raw_R", "train_R", "holdout_R", "top1_removed_R", "top3_removed_R", "top5_removed_R", "median_symbol_R", "positive_symbol_rate", "top3_concentration", "cap5_R", "cap10_R", "notes"],
        robust_rows,
    )
    print_table(
        "Top/bottom symbol table",
        ["slice", "rank_type", "rank", "symbol", "resolved", "net_R", "WR%"],
        symbol_rows,
    )
    print_table(
        "Diagnostic side table",
        ["slice", "raw_R", "top3_removed_R", "median_symbol_R", "positive_symbol_rate", "cap5_R", "notes"],
        diagnostic_rows,
    )
    warnings = sorted(
        {
            note
            for metrics in detail.values()
            for note in metrics["notes"].split(",")
            if note and note != "RESEARCH" and note in {
                "LOW_SAMPLE",
                "SYMBOL_CONCENTRATION_WARN",
                "FRAGILE_EDGE",
                "MEDIAN_SYMBOL_NEGATIVE",
                "CAP_FRAGILE",
                "HIGH_DD",
            }
        }
    )
    print()
    print("Symbol-normalized warnings")
    if warnings:
        for warning in warnings:
            affected = [name for name, metrics in detail.items() if warning in metrics["notes"].split(",")]
            print(f"- {warning}: {', '.join(affected)}")
    else:
        print("- None.")
    robust = [name for name, metrics in detail.items() if "ROBUST_CANDIDATE" in metrics["notes"].split(",")]
    broad = [name for name, metrics in detail.items() if "BROAD_EDGE" in metrics["notes"].split(",")]
    fragile = [name for name, metrics in detail.items() if "FRAGILE_EDGE" in metrics["notes"].split(",")]
    concentrated = [name for name, metrics in detail.items() if "CONCENTRATED_EDGE" in metrics["notes"].split(",")]
    print()
    print("Symbol-normalized interpretation")
    if robust:
        print(f"- Slices surviving this robustness screen: {', '.join(robust)}.")
    elif broad:
        print(f"- Broad but not fully robust slices: {', '.join(broad)}.")
    else:
        print("- No slice qualifies as ROBUST_CANDIDATE under symbol-normalized checks.")
    if concentrated:
        print(f"- Concentration-sensitive slices: {', '.join(concentrated)}.")
    if fragile:
        print(f"- Top3-removal fragile slices: {', '.join(fragile)}.")
    if "EXTENDED + PRE_BREAK_LOW" in detail:
        metrics = detail["EXTENDED + PRE_BREAK_LOW"]
        print(f"- EXTENDED + PRE_BREAK_LOW: raw={metrics['raw']['net_R']:.2f}R top3_removed={metrics['top3_removed']['net_R']:.2f}R median_symbol={metrics['median_symbol_R']:.2f}R notes={metrics['notes']}.")
    if "SHORT" in detail:
        print(f"- SHORT remains diagnostic/interaction-only here; no execution enablement is implied.")
    print("- Any future PAPER-small proposal needs a separate audit; this report does not recommend reversal execution or LIVE changes.")


def print_reversal_stability_output(rows):
    positive_rows, positive_detail = reversal_stability_rows(rows, REVERSAL_POSITIVE_SLICES)
    negative_rows, _ = reversal_stability_rows(rows, REVERSAL_NEGATIVE_SLICES)
    positive_sorted = sorted(positive_rows, key=lambda row: row["ALL net_R"], reverse=True)
    negative_sorted = sorted(negative_rows, key=lambda row: row["ALL net_R"])

    print()
    print("Second-pass reversal stability / holdout slicing")
    print("Research-only outcome replay. These slices are not gates and do not imply PAPER/LIVE enablement.")
    print_table(
        "Top positive stability candidates",
        ["slice_name", "label", "ALL net_R", "TRAIN net_R", "HOLDOUT net_R", "resolved n", "WR%", "median MFE", "median MAE", "maxDD_R", "thirds result", "top3 symbol concentration %", "notes"],
        positive_sorted,
    )
    print_table(
        "Weak / avoid candidates",
        ["slice_name", "ALL net_R", "TRAIN net_R", "HOLDOUT net_R", "resolved n", "WR%", "maxDD_R", "notes"],
        negative_sorted,
    )
    print_table(
        "Side interaction table",
        ["slice_name", "LONG net_R / n / WR", "SHORT net_R / n / WR", "interpretation"],
        reversal_side_interaction_rows(rows, REVERSAL_POSITIVE_SLICES + REVERSAL_NEGATIVE_SLICES),
    )
    symbol_rows = []
    for item in positive_sorted[:3]:
        slice_name = item["slice_name"]
        symbols = reversal_symbol_table(positive_detail[slice_name]["rows"])
        for rank, symbol_row in enumerate(sorted(symbols, key=lambda row: row["net_R"], reverse=True)[:10], start=1):
            row = dict(symbol_row)
            row["slice_name"] = slice_name
            row["rank_type"] = "top"
            row["rank"] = rank
            symbol_rows.append(row)
        for rank, symbol_row in enumerate(sorted(symbols, key=lambda row: row["net_R"])[:10], start=1):
            row = dict(symbol_row)
            row["slice_name"] = slice_name
            row["rank_type"] = "bottom"
            row["rank"] = rank
            symbol_rows.append(row)
    print_table(
        "Symbol concentration for top 3 positive slices",
        ["slice_name", "rank_type", "rank", "symbol", "resolved", "net_R", "WR%", "median_MFE"],
        symbol_rows,
    )
    print()
    print("Stability interpretation")
    stable = [row["slice_name"] for row in positive_sorted if "STABLE_CANDIDATE" in row["notes"]]
    weak = [row["slice_name"] for row in negative_sorted if row["ALL net_R"] < 0]
    if stable:
        print(f"- Continued shadow research candidates: {', '.join(stable)}.")
    else:
        print("- No slice qualifies as a stable candidate under this report's train/holdout + thirds check.")
    if weak:
        print(f"- Structurally weak current slices: {', '.join(weak[:6])}.")
    print("- Any future PAPER-small proposal should remain separate from this simulator and require additional review.")
    print("- No reversal execution, LIVE change, threshold tuning, or gate addition is recommended by this report.")


def smc_main_gate_swap_parse_ts(row):
    for key in ("source_timestamp", "signal_created_ts", "timestamp_unix"):
        value = smc_v0_2_parse_ts(row.get(key))
        if value is not None:
            return value
    key = str(row.get("dedup_key") or "")
    parts = key.split("|")
    if len(parts) >= 4:
        value = smc_v0_2_parse_ts(parts[-1])
        if value is not None:
            return value
    return smc_v0_2_parse_ts(row.get("observed_at"))


def smc_main_gate_swap_stable_key(row):
    dedup_key = str(row.get("dedup_key") or "").strip()
    if dedup_key and "|CONFIRM|" in dedup_key.upper():
        return dedup_key
    symbol = str(row.get("symbol") or "").strip().upper()
    side = str(row.get("side") or "").strip().upper()
    source_ts = smc_main_gate_swap_parse_ts(row)
    if symbol and side and source_ts is not None:
        return f"{symbol}|{side}|CONFIRM|{source_ts}"
    return dedup_key or None


def smc_main_gate_swap_sort_ts(row):
    value = smc_v0_2_parse_ts(row.get("observed_at"))
    if value is not None:
        return value
    value = smc_main_gate_swap_parse_ts(row)
    return value if value is not None else -1.0


def smc_main_gate_swap_keep_candidate(current, candidate):
    if current is None:
        return True
    current_terminal = norm(current.get("status")) == "RESOLVED"
    candidate_terminal = norm(candidate.get("status")) == "RESOLVED"
    if candidate_terminal and not current_terminal:
        return True
    if current_terminal and not candidate_terminal:
        return False
    return smc_main_gate_swap_sort_ts(candidate) >= smc_main_gate_swap_sort_ts(current)


def smc_main_gate_swap_effective_score(row, stats=None):
    base = as_float(row.get("score_v2_structural_shadow"))
    if base is None:
        if stats is not None:
            stats["missing_score_v2_structural_shadow"] += 1
        base = as_float(row.get("score_v2_current"))
    if base is None:
        base = as_float(row.get("score_v2"))
    if base is None:
        if stats is not None:
            stats["missing_score_all"] += 1
        return None
    modifier = as_float(row.get("structural_score_modifier_shadow"))
    if modifier is None:
        modifier = as_float(row.get("structural_modifier"))
    if modifier is None:
        if stats is not None:
            stats["missing_structural_modifier_shadow"] += 1
        modifier = 0.0
    return round(base + modifier, 4)


def smc_main_gate_swap_int(value):
    parsed = as_float(value)
    if parsed is None:
        return None
    if int(parsed) != parsed:
        return None
    return int(parsed)


def smc_main_gate_swap_observed_ts(row):
    value = smc_v0_2_parse_ts(row.get("observed_at"))
    if value is not None:
        return value
    value = smc_v0_2_parse_ts(row.get("timestamp_unix"))
    if value is not None:
        return value
    return smc_main_gate_swap_sort_ts(row)


def smc_main_gate_swap_priority_value(row):
    value = as_float(row.get("candidate_priority"))
    if value is not None:
        return value
    return SMC_MAIN_GATE_SWAP_PRIORITY.get(smc_main_gate_swap_candidate_type(row), 99)


def smc_main_gate_swap_valid_universe_row(row):
    if norm(row.get("status")) != "RESOLVED":
        return False
    if norm(row.get("geometry_status"), "VALID_GEOMETRY") != "VALID_GEOMETRY":
        return False
    if "outcome_trackable" in row and not as_bool(row.get("outcome_trackable")):
        return False
    if "entry_type" in row and norm(row.get("entry_type")) != "CONFIRM":
        return False
    if not row.get("symbol") or norm(row.get("side"), "") not in {"LONG", "SHORT"}:
        return False
    entry = as_float(row.get("entry"))
    sl = as_float(row.get("sl"))
    if entry is None or sl is None or entry == sl:
        return False
    return abs(entry - sl) > 0


def load_smc_main_gate_swap_universe(path):
    stats = Counter()
    by_key = {}
    if not path.exists():
        stats["missing"] = 1
        return [], stats
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_no, line in enumerate(handle, start=1):
            stats["lines"] += 1
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                stats["parse_errors"] += 1
                continue
            if norm(row.get("event_type"), "") not in {"", "CONFIRM_STRUCTURAL_OUTCOME"}:
                continue
            stats["candidate_rows"] += 1
            if not smc_main_gate_swap_valid_universe_row(row):
                stats[f"filtered_{norm(row.get('status'), 'UNKNOWN').lower()}"] += 1
                continue
            key = smc_main_gate_swap_stable_key(row)
            if not key:
                stats["missing_key"] += 1
                key = f"__missing_key_line_{line_no}"
            item = dict(row)
            item["_dedup_key"] = key
            item["_source_ts"] = smc_main_gate_swap_parse_ts(item)
            item["_sort_ts"] = smc_main_gate_swap_sort_ts(item)
            item["_log_order"] = stats["kept_before_dedupe"]
            item["_effective_score"] = smc_main_gate_swap_effective_score(item, stats)
            if smc_main_gate_swap_keep_candidate(by_key.get(key), item):
                if key in by_key:
                    stats["dedupe_replaced"] += 1
                by_key[key] = item
            else:
                stats["dedupe_dropped"] += 1
            stats["kept_before_dedupe"] += 1
    rows = list(by_key.values())
    rows.sort(key=lambda item: (item.get("_source_ts") is None, item.get("_source_ts") or 0.0, item.get("_log_order", 0)))
    stats["deduped"] = len(rows)
    return rows, stats


def smc_main_gate_swap_stream_log_summary(path, event_type):
    stats = Counter()
    opened_by_key = {}
    if not path.exists():
        stats["missing"] = 1
        return opened_by_key, stats
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            stats["lines"] += 1
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                stats["parse_errors"] += 1
                continue
            if norm(row.get("event_type"), "") != event_type:
                continue
            stats["rows"] += 1
            action = norm(row.get("action"), "UNKNOWN")
            stats[f"action_{action.lower()}"] += 1
            key = smc_main_gate_swap_stable_key(row)
            opened_trade_id = row.get("opened_trade_id")
            is_open = action == "OPEN" or as_bool(row.get("opened"))
            if is_open:
                stats["open_or_opened_true"] += 1
            if is_open and key and opened_trade_id not in (None, ""):
                current = opened_by_key.get(key)
                item = {
                    "dedup_key": key,
                    "opened_trade_id": str(opened_trade_id),
                    "observed_at": row.get("observed_at"),
                    "timestamp_unix": row.get("timestamp_unix"),
                    "entry_type": row.get("entry_type"),
                    "opened_entry_type": row.get("opened_entry_type"),
                    "symbol": row.get("symbol"),
                    "side": row.get("side"),
                    "_sort_ts": smc_main_gate_swap_sort_ts(row),
                }
                if current is not None:
                    stats["duplicate_open_key"] += 1
                if current is None or item["_sort_ts"] >= current.get("_sort_ts", -1.0):
                    opened_by_key[key] = item
                    if current is not None:
                        stats["duplicate_open_key_replaced"] += 1
                else:
                    stats["duplicate_open_key_kept_existing"] += 1
            elif is_open and key:
                stats["open_missing_trade_id"] += 1
            elif is_open:
                stats["open_missing_key"] += 1
    return opened_by_key, stats


def load_smc_main_gate_swap_decision_batches(path):
    stats = Counter()
    key_to_meta = defaultdict(list)
    batches = []
    current = []
    current_prev_rank = None

    def flush_current():
        nonlocal current, current_prev_rank
        if not current:
            current_prev_rank = None
            return
        batch_index = len(batches)
        batch_id = f"rank_batch_{batch_index + 1:06d}"
        starts = [
            item["_decision_observed_ts"]
            for item in current
            if item.get("_decision_observed_ts") is not None and item.get("_decision_observed_ts") >= 0
        ]
        if not starts:
            starts = [
                item["_decision_source_ts"]
                for item in current
                if item.get("_decision_source_ts") is not None and item.get("_decision_source_ts") >= 0
            ]
        batch_start = min(starts) if starts else float(current[0]["_decision_log_order"])
        batch_size = len(current)
        batch_items = []
        for order, item in enumerate(current):
            meta = dict(item)
            meta["batch_id_approx"] = batch_id
            meta["candidate_order_in_batch"] = order
            meta["batch_observed_at_start"] = batch_start
            meta["batch_size"] = batch_size
            batch_items.append(meta)
            key = meta.get("dedup_key")
            if key:
                key_to_meta[key].append(meta)
        batches.append({
            "batch_id_approx": batch_id,
            "arrival": batch_start,
            "items": batch_items,
            "batch_size": batch_size,
        })
        current = []
        current_prev_rank = None

    if not path.exists():
        stats["missing"] = 1
        return {}, batches, stats

    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_no, line in enumerate(handle, start=1):
            stats["lines"] += 1
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                stats["parse_errors"] += 1
                continue
            if not is_decision_row(row):
                continue
            stats["decision_rows"] += 1
            suppress_reason = str(row.get("suppress_reason") or "").strip()
            stale_reason = str(row.get("stale_reason_detail") or "").strip()
            for reason in SMC_MAIN_GATE_SWAP_SUPPRESS_DIAGNOSTICS:
                if suppress_reason == reason or stale_reason == reason:
                    stats[f"suppress_diag_{reason}"] += 1
            rank_index = smc_main_gate_swap_int(row.get("rank_index"))
            if rank_index is None:
                stats["missing_rank_index"] += 1
                if current:
                    stats["rank_missing_broke_batch"] += 1
                    flush_current()
                continue

            key = smc_main_gate_swap_stable_key(row)
            if not key:
                stats["missing_key"] += 1
                continue
            observed_ts = smc_main_gate_swap_observed_ts(row)
            source_ts = smc_main_gate_swap_parse_ts(row)
            item = {
                "dedup_key": key,
                "rank_index": rank_index,
                "candidate_priority": row.get("candidate_priority"),
                "ranking_enabled": row.get("ranking_enabled"),
                "observed_at": row.get("observed_at"),
                "source_timestamp": row.get("source_timestamp"),
                "action": row.get("action"),
                "opened": row.get("opened"),
                "suppress_reason": row.get("suppress_reason"),
                "_decision_log_order": line_no,
                "_decision_observed_ts": observed_ts,
                "_decision_source_ts": source_ts,
            }

            if rank_index == 0:
                flush_current()
                current.append(item)
                current_prev_rank = rank_index
                stats["rank_zero_starts"] += 1
                continue
            if not current:
                stats["rank_nonzero_without_batch"] += 1
                continue
            if current_prev_rank is None or rank_index >= current_prev_rank:
                current.append(item)
                current_prev_rank = rank_index
                continue

            stats["rank_sequence_break"] += 1
            flush_current()
            stats["rank_nonzero_without_batch"] += 1

    flush_current()
    stats["rank_index_batches"] = len(batches)
    stats["rank_index_metadata_rows"] = sum(len(items) for items in key_to_meta.values())
    stats["rank_index_unique_keys"] = len(key_to_meta)
    stats["duplicate_dedup_key_metadata"] = sum(max(0, len(items) - 1) for items in key_to_meta.values())
    return key_to_meta, batches, stats


def smc_main_gate_swap_pick_batch_meta(row, metas):
    if not metas:
        return None
    row_source_ts = row.get("_source_ts")
    row_observed_ts = smc_v0_2_parse_ts(row.get("observed_at"))

    def sort_key(meta):
        meta_source_ts = meta.get("_decision_source_ts")
        meta_observed_ts = meta.get("_decision_observed_ts")
        source_delta = abs((meta_source_ts or 0.0) - row_source_ts) if row_source_ts is not None and meta_source_ts is not None else float("inf")
        observed_delta = abs((meta_observed_ts or 0.0) - row_observed_ts) if row_observed_ts is not None and meta_observed_ts is not None else float("inf")
        latest_observed = -(meta_observed_ts if meta_observed_ts is not None else -1.0)
        latest_order = -int(meta.get("_decision_log_order", 0))
        return (source_delta, observed_delta, latest_observed, latest_order)

    return sorted(metas, key=sort_key)[0]


def smc_main_gate_swap_attach_batch_metadata(universe, key_to_meta):
    stats = Counter()
    for row in universe:
        stats["universe_rows"] += 1
        key = row.get("_dedup_key")
        metas = key_to_meta.get(key, []) if key else []
        if not metas:
            row["batch_source"] = SMC_MAIN_GATE_SWAP_FALLBACK_BATCH_SOURCE
            stats["fallback_source_time_bucket"] += 1
            continue
        if len(metas) > 1:
            stats["duplicate_key_candidates_seen"] += 1
        meta = smc_main_gate_swap_pick_batch_meta(row, metas)
        if not meta:
            row["batch_source"] = SMC_MAIN_GATE_SWAP_FALLBACK_BATCH_SOURCE
            stats["fallback_source_time_bucket"] += 1
            continue
        row["batch_source"] = SMC_MAIN_GATE_SWAP_RANK_BATCH_SOURCE
        row["batch_id_approx"] = meta.get("batch_id_approx")
        row["candidate_order_in_batch"] = meta.get("candidate_order_in_batch")
        row["rank_index"] = meta.get("rank_index")
        row["candidate_priority"] = meta.get("candidate_priority")
        row["ranking_enabled"] = meta.get("ranking_enabled")
        row["batch_observed_at_start"] = meta.get("batch_observed_at_start")
        row["batch_size"] = meta.get("batch_size")
        stats["rank_index_batch_metadata"] += 1
    stats["coverage_rank_index"] = pct(stats.get("rank_index_batch_metadata", 0), stats.get("universe_rows", 0))
    return stats


def smc_main_gate_swap_batch_coverage_rows(rows_by_gate, universe_stats):
    rows = [{
        "scope": "universe",
        "rows": universe_stats.get("universe_rows", 0),
        "rank_index rows": universe_stats.get("rank_index_batch_metadata", 0),
        "coverage_rank_index": universe_stats.get("coverage_rank_index", 0.0),
        "fallback_source": SMC_MAIN_GATE_SWAP_SOURCE_TIME_BATCH_SOURCE,
        "fallback rows": universe_stats.get("fallback_source_time_bucket", 0),
    }]
    for gate, selected in rows_by_gate.items():
        rank_rows = sum(1 for row in selected if row.get("batch_source") == SMC_MAIN_GATE_SWAP_RANK_BATCH_SOURCE)
        rows.append({
            "scope": gate,
            "rows": len(selected),
            "rank_index rows": rank_rows,
            "coverage_rank_index": pct(rank_rows, len(selected)),
            "fallback_source": SMC_MAIN_GATE_SWAP_SOURCE_TIME_BATCH_SOURCE,
            "fallback rows": max(0, len(selected) - rank_rows),
        })
    return rows


def smc_main_gate_swap_suppress_diagnostic_rows(stats):
    return [
        {
            "suppress_reason": reason,
            "decision rows": stats.get(f"suppress_diag_{reason}", 0),
        }
        for reason in SMC_MAIN_GATE_SWAP_SUPPRESS_DIAGNOSTICS
    ]


def smc_main_gate_swap_candidate_type(row):
    return norm(row.get("candidate_type") or row.get("reason") or row.get("source_reason"), "")


def smc_main_gate_swap_has_geometry(row):
    return (
        norm(row.get("geometry_status"), "VALID_GEOMETRY") == "VALID_GEOMETRY"
        and as_float(row.get("entry")) is not None
        and as_float(row.get("sl")) is not None
    )


def smc_main_gate_swap_main_eligible(row):
    candidate_type = smc_main_gate_swap_candidate_type(row)
    if candidate_type not in SMC_MAIN_GATE_SWAP_MAIN_TYPES:
        return False
    if "entry_type" in row and norm(row.get("entry_type")) != "CONFIRM":
        return False
    if not smc_main_gate_swap_has_geometry(row):
        return False
    if as_float(row.get("_effective_score")) is None or row["_effective_score"] < 2.5:
        return False
    if as_bool(row.get("weak_structure_blocked")):
        return False
    return True


def smc_main_gate_swap_research_eligible(row):
    candidate_type = smc_main_gate_swap_candidate_type(row)
    if candidate_type not in SMC_MAIN_GATE_SWAP_RESEARCH_TYPES:
        return False
    if "entry_type" in row and norm(row.get("entry_type")) != "CONFIRM":
        return False
    if not smc_main_gate_swap_has_geometry(row):
        return False
    if norm(row.get("structural_decision_shadow")) not in SMC_MAIN_GATE_SWAP_RESEARCH_DECISIONS:
        return False
    if as_float(row.get("_effective_score")) is None or row["_effective_score"] < 2.5:
        return False
    return True


def smc_main_gate_swap_selector_rows(rows, gate_name):
    if gate_name == "RESEARCH_SELECTOR_GATE":
        selected = [row for row in rows if smc_main_gate_swap_research_eligible(row)]
        return sorted(selected, key=lambda row: (row.get("_source_ts") is None, row.get("_source_ts") or 0.0, row.get("_log_order", 0)))
    selected = [row for row in rows if smc_main_gate_swap_main_eligible(row)]
    if gate_name == "CURRENT_MAIN_GATE":
        return sorted(
            selected,
            key=lambda row: (
                smc_main_gate_swap_priority_value(row),
                -(row.get("_effective_score") if row.get("_effective_score") is not None else float("-inf")),
                -(row.get("_source_ts") if row.get("_source_ts") is not None else float("-inf")),
                row.get("_log_order", 0),
            ),
        )
    if gate_name == "INVERTED_SCORE_GATE":
        return sorted(
            selected,
            key=lambda row: (
                smc_main_gate_swap_priority_value(row),
                row.get("_effective_score") if row.get("_effective_score") is not None else float("inf"),
                -(row.get("_source_ts") if row.get("_source_ts") is not None else float("-inf")),
                row.get("_log_order", 0),
            ),
        )
    if gate_name == "SCORE_IGNORED_GATE":
        return sorted(selected, key=lambda row: (row.get("_source_ts") is None, row.get("_source_ts") or 0.0, row.get("_log_order", 0)))
    if gate_name == "STRUCTURAL_ONLY_D2":
        filtered = [row for row in selected if norm(row.get("bos_quality")) not in WEAK_BOS]
        return sorted(filtered, key=lambda row: (row.get("_source_ts") is None, row.get("_source_ts") or 0.0, row.get("_log_order", 0)))
    return []


def smc_main_gate_swap_topk(rows, gate_name, k=5):
    eligible = smc_main_gate_swap_selector_rows(rows, gate_name)
    buckets = defaultdict(list)
    for row in eligible:
        source_ts = row.get("_source_ts")
        bucket = round(source_ts) if source_ts is not None else f"missing_{row.get('_log_order', 0)}"
        buckets[bucket].append(row)
    selected = []
    for bucket in sorted(buckets, key=lambda value: (isinstance(value, str), value)):
        bucket_rows = buckets[bucket]
        if gate_name in {"CURRENT_MAIN_GATE", "INVERTED_SCORE_GATE"}:
            ordered = smc_main_gate_swap_selector_rows(bucket_rows, gate_name)
        elif gate_name == "STRUCTURAL_ONLY_D2":
            ordered = smc_main_gate_swap_selector_rows(bucket_rows, gate_name)
        else:
            ordered = sorted(bucket_rows, key=lambda row: row.get("_log_order", 0))
        selected.extend(ordered[:k])
    return selected


def smc_main_gate_swap_arrival_ts(row):
    source_ts = row.get("_source_ts")
    if source_ts is not None:
        return source_ts, "_source_ts"
    fallback = smc_main_gate_swap_sort_ts(row)
    if fallback is not None and fallback >= 0:
        return fallback, "log_order_timestamp"
    return float(row.get("_log_order", 0)), "log_order_index"


def smc_main_gate_swap_source_time_replay_batches(rows):
    annotated = []
    source_ts_fallbacks = 0
    log_index_fallbacks = 0
    rows_with_scan_id = 0
    for row in rows:
        arrival, arrival_source = smc_main_gate_swap_arrival_ts(row)
        if arrival_source != "_source_ts":
            source_ts_fallbacks += 1
        if arrival_source == "log_order_index":
            log_index_fallbacks += 1
        scan_id = row.get("scan_id")
        if scan_id not in (None, ""):
            rows_with_scan_id += 1
        annotated.append((arrival, row.get("_log_order", 0), row, arrival_source))

    annotated.sort(key=lambda item: (item[0], item[1]))
    use_scan_id = rows and rows_with_scan_id == len(rows)
    grouped = defaultdict(list)
    group_order = {}
    if use_scan_id:
        for arrival, log_order, row, arrival_source in annotated:
            group_key = ("scan_id", str(row.get("scan_id")))
            grouped[group_key].append((arrival, log_order, row, arrival_source))
            group_order[group_key] = min(group_order.get(group_key, (arrival, log_order)), (arrival, log_order))
    else:
        for arrival, log_order, row, arrival_source in annotated:
            group_key = ("arrival_ts", arrival) if arrival_source != "log_order_index" else ("log_order", log_order)
            grouped[group_key].append((arrival, log_order, row, arrival_source))
            group_order[group_key] = min(group_order.get(group_key, (arrival, log_order)), (arrival, log_order))

    batches = []
    for group_key in sorted(grouped, key=lambda key: group_order[key]):
        items = sorted(grouped[group_key], key=lambda item: (item[0], item[1]))
        batch_arrival = min(item[0] for item in items)
        batches.append({
            "group_key": group_key,
            "arrival": batch_arrival,
            "rows": [item[2] for item in items],
        })
    meta = {
        "replay_mode": SMC_MAIN_GATE_SWAP_APPROX_REPLAY,
        "replay_variant": SMC_MAIN_GATE_SWAP_SOURCE_TIME_REPLAY,
        "batch_source": SMC_MAIN_GATE_SWAP_SOURCE_TIME_BATCH_SOURCE,
        "exact_scan_id_available": False,
        "source_time_scan_id_complete": use_scan_id,
        "rows_with_scan_id": rows_with_scan_id,
        "rows_without_scan_id": max(0, len(rows) - rows_with_scan_id),
        "source_ts_fallback_count": source_ts_fallbacks,
        "log_order_index_fallback_count": log_index_fallbacks,
        "batch_count": len(batches),
    }
    return batches, meta


def smc_main_gate_swap_rank_index_replay_batches(rows):
    covered = [row for row in rows if row.get("batch_source") == SMC_MAIN_GATE_SWAP_RANK_BATCH_SOURCE and row.get("batch_id_approx")]
    grouped = defaultdict(list)
    for row in covered:
        grouped[row.get("batch_id_approx")].append(row)
    batches = []
    for batch_id, batch_rows in grouped.items():
        ordered = sorted(
            batch_rows,
            key=lambda row: (
                row.get("candidate_order_in_batch") is None,
                row.get("candidate_order_in_batch") if row.get("candidate_order_in_batch") is not None else row.get("_log_order", 0),
                row.get("_log_order", 0),
            ),
        )
        batch_start = ordered[0].get("batch_observed_at_start")
        if batch_start is None:
            batch_start = min(smc_main_gate_swap_arrival_ts(row)[0] for row in ordered)
        batches.append({
            "group_key": ("rank_index_batch", batch_id),
            "arrival": batch_start,
            "rows": ordered,
        })
    batches.sort(key=lambda item: (item["arrival"], str(item["group_key"])))
    meta = {
        "replay_mode": SMC_MAIN_GATE_SWAP_APPROX_REPLAY,
        "replay_variant": SMC_MAIN_GATE_SWAP_RANK_INDEX_REPLAY,
        "batch_source": SMC_MAIN_GATE_SWAP_RANK_BATCH_SOURCE,
        "fallback_source": SMC_MAIN_GATE_SWAP_SOURCE_TIME_BATCH_SOURCE,
        "exact_scan_id_available": False,
        "batch_count": len(batches),
        "rank_index_covered_rows": len(covered),
        "rank_index_excluded_fallback_rows": max(0, len(rows) - len(covered)),
        "coverage_rank_index": pct(len(covered), len(rows)),
    }
    return batches, meta


def smc_main_gate_swap_gate_selected_rows(rows, gate_name, replay_variant=None):
    if replay_variant != SMC_MAIN_GATE_SWAP_RANK_INDEX_REPLAY:
        return smc_main_gate_swap_selector_rows(rows, gate_name)

    if gate_name == "RESEARCH_SELECTOR_GATE":
        selected = [row for row in rows if smc_main_gate_swap_research_eligible(row)]
        return sorted(
            selected,
            key=lambda row: (
                row.get("candidate_order_in_batch") is None,
                row.get("candidate_order_in_batch") if row.get("candidate_order_in_batch") is not None else row.get("_log_order", 0),
                row.get("rank_index") if row.get("rank_index") is not None else 999999,
                row.get("_log_order", 0),
            ),
        )

    selected = [row for row in rows if smc_main_gate_swap_main_eligible(row)]
    if gate_name == "CURRENT_MAIN_GATE":
        return sorted(
            selected,
            key=lambda row: (
                smc_main_gate_swap_priority_value(row),
                -(row.get("_effective_score") if row.get("_effective_score") is not None else float("-inf")),
                -(row.get("_source_ts") if row.get("_source_ts") is not None else float("-inf")),
                row.get("candidate_order_in_batch") if row.get("candidate_order_in_batch") is not None else row.get("_log_order", 0),
            ),
        )
    if gate_name == "INVERTED_SCORE_GATE":
        return sorted(
            selected,
            key=lambda row: (
                row.get("_effective_score") if row.get("_effective_score") is not None else float("inf"),
                row.get("candidate_order_in_batch") if row.get("candidate_order_in_batch") is not None else row.get("_log_order", 0),
                row.get("_log_order", 0),
            ),
        )
    if gate_name == "SCORE_IGNORED_GATE":
        return sorted(
            selected,
            key=lambda row: (
                row.get("candidate_order_in_batch") is None,
                row.get("candidate_order_in_batch") if row.get("candidate_order_in_batch") is not None else row.get("_log_order", 0),
                row.get("_log_order", 0),
            ),
        )
    if gate_name == "STRUCTURAL_ONLY_D2":
        filtered = [row for row in selected if norm(row.get("bos_quality")) not in WEAK_BOS]
        return sorted(
            filtered,
            key=lambda row: (
                row.get("candidate_order_in_batch") is None,
                row.get("candidate_order_in_batch") if row.get("candidate_order_in_batch") is not None else row.get("_log_order", 0),
                row.get("_log_order", 0),
            ),
        )
    return []


def smc_main_gate_swap_actual_release_ts(actual, selected_arrival):
    close_ts = smc_v0_2_parse_ts(actual.get("close_time"))
    if close_ts is not None and close_ts >= selected_arrival:
        return close_ts
    open_ts = smc_v0_2_parse_ts(actual.get("open_time"))
    signal_ts = smc_v0_2_parse_ts(actual.get("signal_created_ts"))
    duration_minutes = as_float(actual.get("trade_age_minutes"))
    if duration_minutes is not None:
        base_ts = open_ts if open_ts is not None else signal_ts
        if base_ts is None or base_ts < selected_arrival:
            base_ts = selected_arrival
        return base_ts + max(0.0, duration_minutes) * 60.0
    return None


def smc_main_gate_swap_raw_release_delta(row):
    def first_available(keys):
        values = [as_float(row.get(key)) for key in keys]
        values = [value for value in values if value is not None and value >= 0]
        return min(values) if values else None

    first_hit = norm(row.get("first_hit"), "")
    if first_hit == "SL":
        return first_available(["time_to_sl_secs"])
    if first_hit == "1R":
        return first_available(["time_to_1r_secs"])
    if first_hit in {"1.5R", "1_5R"}:
        return first_available(["time_to_1_5r_secs"])
    if first_hit == "2R":
        return first_available(["time_to_2r_secs"])
    if first_hit == "TP":
        return first_available(["time_to_tp_secs", "time_to_2r_secs", "time_to_1_5r_secs", "time_to_1r_secs"])
    if first_hit in {"AMBIGUOUS", "AMBIGUOUS_SAME_BAR"}:
        return first_available(["time_to_sl_secs", "time_to_tp_secs", "time_to_1r_secs", "time_to_1_5r_secs", "time_to_2r_secs"])
    return first_available(["time_to_sl_secs", "time_to_tp_secs", "time_to_1r_secs", "time_to_1_5r_secs", "time_to_2r_secs"])


def smc_main_gate_swap_raw_r(row):
    first_hit = norm(row.get("first_hit"), "")
    if first_hit == "SL":
        return -1.0
    if first_hit == "TP":
        return as_float(row.get("planned_rr")) or as_float(row.get("rr")) or 0.0
    if first_hit == "2R":
        return 2.0
    if first_hit in {"1.5R", "1_5R"}:
        return 1.5
    if first_hit == "1R":
        return 1.0
    if first_hit in {"AMBIGUOUS", "AMBIGUOUS_SAME_BAR"}:
        return None
    return None


def smc_main_gate_swap_hit(row, threshold):
    key = {1.0: "hit_1r", 1.5: "hit_1_5r", 2.0: "hit_2r"}.get(threshold)
    if key and key in row:
        return as_bool(row.get(key))
    r_value = smc_main_gate_swap_raw_r(row)
    if r_value is not None and r_value >= threshold:
        return True
    return (as_float(row.get("mfe_r"), 0.0) or 0.0) >= threshold


def smc_main_gate_swap_maxdd(rows):
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for row in sorted(rows, key=lambda item: (item.get("_source_ts") is None, item.get("_source_ts") or 0.0, item.get("_log_order", 0))):
        r_value = smc_main_gate_swap_raw_r(row)
        if r_value is None:
            continue
        equity += r_value
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def smc_main_gate_swap_metrics(rows):
    evaluable = [row for row in rows if smc_main_gate_swap_raw_r(row) is not None]
    r_values = [smc_main_gate_swap_raw_r(row) for row in evaluable]
    mfe_values = [as_float(row.get("mfe_r")) for row in evaluable]
    mae_values = [as_float(row.get("mae_r")) for row in evaluable]
    mfe_values = [value for value in mfe_values if value is not None]
    mae_values = [value for value in mae_values if value is not None]
    first_hits = Counter(norm(row.get("first_hit"), "UNKNOWN") for row in rows)
    return {
        "n": len(rows),
        "evaluable": len(evaluable),
        "net_R": sum(r_values),
        "WR%": pct(sum(1 for value in r_values if value > 0), len(r_values)),
        "avg_R": mean(r_values),
        "median_R": median(r_values),
        "maxDD_R": smc_main_gate_swap_maxdd(evaluable),
        "first_hit_distribution": ", ".join(f"{key}:{value}" for key, value in first_hits.most_common(8)),
        "hit_0.5R%": pct(sum(1 for row in evaluable if (as_float(row.get("mfe_r"), 0.0) or 0.0) >= 0.5), len(evaluable)),
        "hit_1R%": pct(sum(1 for row in evaluable if smc_main_gate_swap_hit(row, 1.0)), len(evaluable)),
        "hit_1.5R%": pct(sum(1 for row in evaluable if smc_main_gate_swap_hit(row, 1.5)), len(evaluable)),
        "hit_2R%": pct(sum(1 for row in evaluable if smc_main_gate_swap_hit(row, 2.0)), len(evaluable)),
        "median_MFE_R": median(mfe_values),
        "median_MAE_R": median(mae_values),
        "SL_first%": pct(first_hits.get("SL", 0), len(rows)),
        "ambiguous_excluded": sum(1 for row in rows if norm(row.get("first_hit"), "") in {"AMBIGUOUS", "AMBIGUOUS_SAME_BAR"}),
    }


def smc_main_gate_swap_split(rows):
    ordered = sorted(rows, key=lambda item: (item.get("_source_ts") is None, item.get("_source_ts") or 0.0, item.get("_log_order", 0)))
    cutoff = int(len(ordered) * 0.70)
    return ordered[:cutoff], ordered[cutoff:]


def smc_main_gate_swap_thirds(rows):
    ordered = sorted(rows, key=lambda item: (item.get("_source_ts") is None, item.get("_source_ts") or 0.0, item.get("_log_order", 0)))
    if not ordered:
        return [[], [], []]
    size = math.ceil(len(ordered) / 3.0)
    return [ordered[:size], ordered[size:size * 2], ordered[size * 2:]]


def smc_main_gate_swap_comparison_rows(rows_by_gate, mode):
    table = []
    for gate_name, rows in rows_by_gate.items():
        metrics = smc_main_gate_swap_metrics(rows)
        train, holdout = smc_main_gate_swap_split(rows)
        train_metrics = smc_main_gate_swap_metrics(train)
        holdout_metrics = smc_main_gate_swap_metrics(holdout)
        thirds = smc_main_gate_swap_thirds(rows)
        third_bits = []
        for idx, third_rows in enumerate(thirds, start=1):
            third_metrics = smc_main_gate_swap_metrics(third_rows)
            third_bits.append(f"T{idx}:{third_metrics['net_R']:.1f}R/{third_metrics['WR%']:.1f}%")
        table.append({
            "mode": mode,
            "gate": gate_name,
            "n": metrics["n"],
            "evaluable": metrics["evaluable"],
            "net_R raw": metrics["net_R"],
            "WR raw": metrics["WR%"],
            "avg_R": metrics["avg_R"],
            "median_R": metrics["median_R"],
            "maxDD_R": metrics["maxDD_R"],
            "SL_first%": metrics["SL_first%"],
            "hit_1R%": metrics["hit_1R%"],
            "hit_1.5R%": metrics["hit_1.5R%"],
            "hit_2R%": metrics["hit_2R%"],
            "median MFE_R": metrics["median_MFE_R"],
            "median MAE_R": metrics["median_MAE_R"],
            "train_R": train_metrics["net_R"],
            "holdout_R": holdout_metrics["net_R"],
            "thirds": "; ".join(third_bits),
        })
    return table


def smc_main_gate_swap_bucket_label(score):
    for label, predicate in SMC_MAIN_GATE_SWAP_SCORE_BUCKETS:
        if predicate(score):
            return label
    return "missing"


def smc_main_gate_swap_breakdown_rows(rows, field, top=None):
    grouped = defaultdict(list)
    for row in rows:
        if field == "score_bucket":
            value = smc_main_gate_swap_bucket_label(row.get("_effective_score"))
        elif field == "candidate_type/reason":
            value = smc_main_gate_swap_candidate_type(row)
        else:
            value = norm(row.get(field), "UNKNOWN")
        grouped[value].append(row)
    table = []
    for value, group_rows in grouped.items():
        metrics = smc_main_gate_swap_metrics(group_rows)
        table.append({
            field: value,
            "n": metrics["n"],
            "net_R": metrics["net_R"],
            "WR%": metrics["WR%"],
            "SL_first%": metrics["SL_first%"],
            "median_MFE_R": metrics["median_MFE_R"],
        })
    table.sort(key=lambda item: (-item["n"], str(item[field])))
    return table[:top] if top else table


def smc_main_gate_swap_load_actual_trades(path):
    stats = Counter()
    anchors = defaultdict(list)
    geometry_anchors = defaultdict(list)
    summary = defaultdict(list)
    trade_by_id = {}
    if not path.exists():
        stats["missing"] = 1
        return anchors, geometry_anchors, summary, trade_by_id, stats
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            stats["rows"] += 1
            entry_type = str(row.get("entry_type") or "").upper()
            if entry_type not in {"PAPER_SMC_MAIN", "CONFIRM_SMC_RESEARCH", "CONFIRM"}:
                continue
            actual_r = as_float(row.get("rr"))
            item = dict(row)
            item["_actual_r"] = actual_r
            trade_id = str(row.get("id") or "").strip()
            if trade_id:
                if trade_id in trade_by_id:
                    stats["duplicate_trade_id"] += 1
                trade_by_id[trade_id] = item
                stats[f"{entry_type.lower()}_with_id"] += 1
            else:
                stats[f"{entry_type.lower()}_missing_id"] += 1
            stats[f"{entry_type.lower()}_rows"] += 1
            summary[entry_type].append(item)
            symbol = str(row.get("symbol") or "").upper()
            side = str(row.get("side") or "").upper()
            ts = smc_v0_2_parse_ts(row.get("signal_created_ts"))
            if symbol and side and ts is not None:
                anchors[f"{symbol}|{side}|CONFIRM|{ts}"].append(item)
            entry = as_float(row.get("entry"))
            sl = as_float(row.get("sl"))
            if symbol and side and entry is not None and sl is not None:
                geometry_anchors[f"{symbol}|{side}|{entry:.12g}|{sl:.12g}"].append(item)
    stats["trade_by_id"] = len(trade_by_id)
    return anchors, geometry_anchors, summary, trade_by_id, stats


def smc_main_gate_swap_actual_metrics(rows):
    actual = [row for row in rows if row.get("_actual_r") is not None]
    values = [row["_actual_r"] for row in actual]
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    exits = Counter(str(row.get("exit_type") or row.get("status") or "UNKNOWN").upper() for row in actual)
    statuses = Counter(str(row.get("status") or "UNKNOWN").upper() for row in actual)
    sources = Counter(str(row.get("_actual_source") or "unlabeled").lower() for row in actual)
    entry_types = Counter(str(row.get("actual_entry_type") or row.get("entry_type") or "UNKNOWN").upper() for row in actual)
    return {
        "count": len(actual),
        "net actual_R": sum(values),
        "WR actual": pct(len(wins), len(values)),
        "avg actual_R": mean(values),
        "avg win": mean(wins),
        "avg loss": mean(losses),
        "status distribution": ", ".join(f"{key}:{value}" for key, value in statuses.most_common(8)),
        "exit_type distribution": ", ".join(f"{key}:{value}" for key, value in exits.most_common(8)),
        "actual_entry_type distribution": ", ".join(f"{key}:{value}" for key, value in entry_types.most_common(8)),
        "actual source distribution": ", ".join(f"{key}:{value}" for key, value in sources.most_common(8)),
    }


def smc_main_gate_swap_actual_anchor_rows(summary):
    rows = []
    for entry_type in ("PAPER_SMC_MAIN", "CONFIRM_SMC_RESEARCH", "CONFIRM"):
        metrics = smc_main_gate_swap_actual_metrics(summary.get(entry_type, []))
        item = {"entry_type": entry_type}
        item.update(metrics)
        rows.append(item)
    return rows


def smc_main_gate_swap_geometry_key(row):
    symbol = str(row.get("symbol") or "").upper()
    side = str(row.get("side") or "").upper()
    entry = as_float(row.get("entry"))
    sl = as_float(row.get("sl"))
    if not symbol or not side or entry is None or sl is None:
        return None
    return f"{symbol}|{side}|{entry:.12g}|{sl:.12g}"


def smc_main_gate_swap_copy_actual(trade, source, selected_key=None, bridge=None):
    item = dict(trade)
    item["_actual_source"] = source
    item["actual_source"] = source
    item["actual_trade_id"] = item.get("id")
    item["actual_entry_type"] = item.get("entry_type")
    item["actual_status"] = item.get("status")
    item["actual_exit_type"] = item.get("exit_type")
    item["actual_r"] = item.get("_actual_r")
    item["actual_close_R"] = item.get("_actual_r")
    if selected_key is not None:
        item["_selected_dedup_key"] = selected_key
    if bridge:
        item["_bridge_entry_type"] = bridge.get("entry_type")
        item["_bridge_opened_entry_type"] = bridge.get("opened_entry_type")
    return item


def smc_main_gate_swap_selected_actual_rows(selected, anchors, geometry_anchors, main_opened, research_opened, trade_by_id):
    actual = []
    seen = set()
    stats = Counter()
    for row in selected:
        row_joined = False
        selected_key = row.get("_dedup_key") or smc_main_gate_swap_stable_key(row)
        for bridge_source, bridge_map in (("decision_bridge", main_opened), ("research_bridge", research_opened)):
            bridge = bridge_map.get(selected_key) if selected_key else None
            if not bridge:
                continue
            opened_trade_id = str(bridge.get("opened_trade_id") or "").strip()
            trade = trade_by_id.get(opened_trade_id)
            if not trade:
                stats[f"{bridge_source}_missing_trade"] += 1
                continue
            key = str(trade.get("id") or opened_trade_id or id(trade))
            if key in seen:
                stats[f"{bridge_source}_duplicate_trade_skipped"] += 1
                row_joined = True
                continue
            seen.add(key)
            actual.append(smc_main_gate_swap_copy_actual(trade, bridge_source, selected_key, bridge))
            stats[bridge_source] += 1
            row_joined = True
            break
        if row_joined:
            continue

        candidate_trades = []
        for trade in anchors.get(selected_key, []):
            candidate_trades.append(("signal_created_ts", trade))
        geometry_key = smc_main_gate_swap_geometry_key(row)
        if geometry_key:
            for trade in geometry_anchors.get(geometry_key, []):
                candidate_trades.append(("geometry", trade))
        for source, trade in candidate_trades:
            key = str(trade.get("id") or id(trade))
            if key in seen:
                continue
            seen.add(key)
            actual.append(smc_main_gate_swap_copy_actual(trade, source, selected_key))
            stats[source] += 1
            row_joined = True
            break
        if not row_joined:
            stats["none"] += 1
    return actual, stats


def smc_main_gate_swap_join_actual_for_row(row, anchors, geometry_anchors, main_opened, research_opened, trade_by_id):
    selected_key = row.get("_dedup_key") or smc_main_gate_swap_stable_key(row)
    for bridge_source, bridge_map in (("decision_bridge", main_opened), ("research_bridge", research_opened)):
        bridge = bridge_map.get(selected_key) if selected_key else None
        if not bridge:
            continue
        opened_trade_id = str(bridge.get("opened_trade_id") or "").strip()
        trade = trade_by_id.get(opened_trade_id)
        if trade:
            return smc_main_gate_swap_copy_actual(trade, bridge_source, selected_key, bridge), bridge_source

    for trade in anchors.get(selected_key, []):
        return smc_main_gate_swap_copy_actual(trade, "signal_created_ts", selected_key), "signal_created_ts"
    geometry_key = smc_main_gate_swap_geometry_key(row)
    if geometry_key:
        for trade in geometry_anchors.get(geometry_key, []):
            return smc_main_gate_swap_copy_actual(trade, "geometry", selected_key), "geometry"
    return None, "none"


def smc_main_gate_swap_estimated_release(row, arrival, anchors, geometry_anchors, main_opened, research_opened, trade_by_id):
    actual, actual_source = smc_main_gate_swap_join_actual_for_row(
        row,
        anchors,
        geometry_anchors,
        main_opened,
        research_opened,
        trade_by_id,
    )
    if actual:
        actual_release = smc_main_gate_swap_actual_release_ts(actual, arrival)
        if actual_release is not None:
            return actual_release, "actual_close_time", actual_source

    raw_delta = smc_main_gate_swap_raw_release_delta(row)
    if raw_delta is not None:
        return arrival + raw_delta, "raw_first_hit_time", actual_source

    return arrival + SMC_MAIN_GATE_SWAP_REPLAY_FALLBACK_TTL_SECS, "fallback_ttl", actual_source


def smc_main_gate_swap_concurrency_replay(rows, gate_names, anchors, geometry_anchors, main_opened, research_opened, trade_by_id, replay_variant=SMC_MAIN_GATE_SWAP_SOURCE_TIME_REPLAY):
    if replay_variant == SMC_MAIN_GATE_SWAP_RANK_INDEX_REPLAY:
        batches, batch_meta = smc_main_gate_swap_rank_index_replay_batches(rows)
        replay_rows = [row for batch in batches for row in batch["rows"]]
    else:
        batches, batch_meta = smc_main_gate_swap_source_time_replay_batches(rows)
        replay_rows = rows
    selected_by_gate = {}
    stats_by_gate = {}

    for gate_name in gate_names:
        open_slots = []
        selected = []
        stats = Counter()
        stats["candidates_seen"] = len(replay_rows)
        stats["excluded_fallback_rows"] = batch_meta.get("rank_index_excluded_fallback_rows", 0)
        stats["max_open_slots"] = SMC_MAIN_GATE_SWAP_REPLAY_MAX_OPEN
        stats["one_per_symbol"] = 1 if SMC_MAIN_GATE_SWAP_REPLAY_ONE_PER_SYMBOL else 0

        for batch in batches:
            arrival = batch["arrival"]
            still_open = []
            for slot in open_slots:
                if slot["release_time"] <= arrival:
                    stats["released_slots"] += 1
                else:
                    still_open.append(slot)
            open_slots = still_open

            batch_rows = batch["rows"]
            eligible = smc_main_gate_swap_gate_selected_rows(batch_rows, gate_name, replay_variant)
            eligible_ids = {id(row) for row in eligible}
            stats["eligible_count"] += len(eligible)
            stats["skipped_not_eligible"] += len(batch_rows) - len(eligible)

            for row in eligible:
                symbol = str(row.get("symbol") or "").upper()
                open_symbols = {slot["symbol"] for slot in open_slots}
                if SMC_MAIN_GATE_SWAP_REPLAY_ONE_PER_SYMBOL and symbol and symbol in open_symbols:
                    stats["skipped_symbol_already_open"] += 1
                    continue
                if len(open_slots) >= SMC_MAIN_GATE_SWAP_REPLAY_MAX_OPEN:
                    stats["skipped_max_open"] += 1
                    continue
                release_time, release_source, actual_source = smc_main_gate_swap_estimated_release(
                    row,
                    arrival,
                    anchors,
                    geometry_anchors,
                    main_opened,
                    research_opened,
                    trade_by_id,
                )
                selected.append(row)
                stats["selected_count"] += 1
                stats[f"release_{release_source}"] += 1
                stats[f"release_actual_source_{actual_source}"] += 1
                open_slots.append({
                    "symbol": symbol,
                    "release_time": release_time,
                    "row": row,
                })
                stats["max_concurrent_observed"] = max(stats["max_concurrent_observed"], len(open_slots))

            stats["batch_count"] += 1
            stats["batch_candidates_max"] = max(stats["batch_candidates_max"], len(batch_rows))
            stats["batch_eligible_max"] = max(stats["batch_eligible_max"], len(eligible_ids))

        selected_by_gate[gate_name] = selected
        stats["unique_symbols_selected"] = len({str(row.get("symbol") or "").upper() for row in selected if row.get("symbol")})
        stats_by_gate[gate_name] = stats

    replay_meta = dict(batch_meta)
    replay_meta["max_open_slots"] = SMC_MAIN_GATE_SWAP_REPLAY_MAX_OPEN
    replay_meta["one_per_symbol"] = SMC_MAIN_GATE_SWAP_REPLAY_ONE_PER_SYMBOL
    replay_meta["fallback_ttl_secs"] = SMC_MAIN_GATE_SWAP_REPLAY_FALLBACK_TTL_SECS
    return selected_by_gate, stats_by_gate, replay_meta


def smc_main_gate_swap_selected_actual_panel(rows_by_gate, mode, anchors, geometry_anchors, main_opened, research_opened, trade_by_id):
    panel = []
    join_stats_by_gate = {}
    for gate, selected in rows_by_gate.items():
        joined, join_stats = smc_main_gate_swap_selected_actual_rows(
            selected,
            anchors,
            geometry_anchors,
            main_opened,
            research_opened,
            trade_by_id,
        )
        join_stats_by_gate[gate] = join_stats
        fallback = join_stats.get("signal_created_ts", 0) + join_stats.get("geometry", 0)
        item = {
            "mode": mode,
            "gate": gate,
            "selected rows": len(selected),
            "decision_bridge rows": join_stats.get("decision_bridge", 0),
            "research_bridge rows": join_stats.get("research_bridge", 0),
            "fallback rows": fallback,
            "none rows": join_stats.get("none", 0),
        }
        item.update(smc_main_gate_swap_actual_metrics(joined))
        panel.append(item)
    return panel, join_stats_by_gate


def smc_main_gate_swap_selected_actual_lane_panel(rows_by_gate, mode, anchors, geometry_anchors, main_opened, research_opened, trade_by_id):
    panel = []
    join_stats_by_gate = {}
    for gate, selected in rows_by_gate.items():
        joined, join_stats = smc_main_gate_swap_selected_actual_rows(
            selected,
            anchors,
            geometry_anchors,
            main_opened,
            research_opened,
            trade_by_id,
        )
        join_stats_by_gate[gate] = join_stats
        lane_rows = [
            ("actual_all_joined", joined),
            ("actual_opened_by_main / decision_bridge", [row for row in joined if row.get("_actual_source") == "decision_bridge"]),
            ("actual_opened_by_research / research_bridge", [row for row in joined if row.get("_actual_source") == "research_bridge"]),
            ("actual_opened_by_other / fallback", [row for row in joined if row.get("_actual_source") in {"signal_created_ts", "geometry"}]),
            ("actual_none", []),
        ]
        for lane, lane_joined in lane_rows:
            item = {
                "mode": mode,
                "gate": gate,
                "lane": lane,
                "selected rows": len(selected),
                "joined rows": len(lane_joined),
                "none rows": join_stats.get("none", 0) if lane == "actual_none" else 0,
                "note": "",
            }
            if gate == "CURRENT_MAIN_GATE" and lane == "actual_opened_by_research / research_bridge" and lane_joined:
                item["note"] = "selected by CURRENT_MAIN_GATE in replay, but actually opened historically by research lane"
            item.update(smc_main_gate_swap_actual_metrics(lane_joined))
            panel.append(item)
    return panel, join_stats_by_gate


def smc_main_gate_swap_actual_open_coverage(universe, actual_summary, main_opened, trade_by_id):
    universe_keys = {row.get("_dedup_key") for row in universe if row.get("_dedup_key")}
    bridge_trade_ids = {
        str(item.get("opened_trade_id"))
        for item in main_opened.values()
        if item.get("opened_trade_id") not in (None, "")
    }
    actual_main = actual_summary.get("PAPER_SMC_MAIN", [])
    actual_main_ids = {str(row.get("id")) for row in actual_main if row.get("id") not in (None, "")}
    matched_keys = [
        key
        for key, item in main_opened.items()
        if key in universe_keys and str(item.get("opened_trade_id") or "") in actual_main_ids
    ]
    return {
        "PAPER_SMC_MAIN actual opens total": len(actual_main),
        "actual opens with decision bridge id match": len(actual_main_ids & bridge_trade_ids),
        "actual opens with universe resolved key match": len(matched_keys),
        "actual opens outside RESOLVED universe": max(0, len(actual_main) - len(matched_keys)),
        "decision bridge ids missing in paper_trades": sum(1 for trade_id in bridge_trade_ids if trade_id not in trade_by_id),
        "outside DATA_MISSING": "not separable in this pass",
        "outside OPEN": "not separable in this pass",
        "outside no outcome row": "not separable in this pass",
        "outside no key": "not separable in this pass",
        "missing reasons": "outside RESOLVED universe: DATA_MISSING/OPEN/no outcome row/no key are not separable from selected RESOLVED set in this analyzer pass",
    }


def smc_main_gate_swap_classification(rows_by_gate):
    metrics = {gate: smc_main_gate_swap_metrics(rows) for gate, rows in rows_by_gate.items()}
    current = metrics.get("CURRENT_MAIN_GATE", {})
    if not current or current.get("evaluable", 0) < 30:
        return "NEED_MORE_DATA"

    def stable_beats(gate):
        challenger_rows = rows_by_gate.get(gate, [])
        current_rows = rows_by_gate.get("CURRENT_MAIN_GATE", [])
        challenger = smc_main_gate_swap_metrics(challenger_rows)
        if challenger.get("evaluable", 0) < 30 or challenger["net_R"] <= current["net_R"]:
            return False
        _, challenger_holdout = smc_main_gate_swap_split(challenger_rows)
        _, current_holdout = smc_main_gate_swap_split(current_rows)
        if smc_main_gate_swap_metrics(challenger_holdout)["net_R"] <= smc_main_gate_swap_metrics(current_holdout)["net_R"]:
            return False
        better_thirds = 0
        for challenger_third, current_third in zip(smc_main_gate_swap_thirds(challenger_rows), smc_main_gate_swap_thirds(current_rows)):
            if smc_main_gate_swap_metrics(challenger_third)["net_R"] > smc_main_gate_swap_metrics(current_third)["net_R"]:
                better_thirds += 1
        return better_thirds >= 2

    labels = []
    if stable_beats("RESEARCH_SELECTOR_GATE"):
        labels.append("RESEARCH_SELECTOR_CANDIDATE")
    if stable_beats("INVERTED_SCORE_GATE"):
        labels.append("INVERTED_SCORE_CANDIDATE")
    if stable_beats("SCORE_IGNORED_GATE"):
        labels.append("SCORE_IGNORE_CANDIDATE")
    if labels:
        if len(labels) >= 2:
            labels.insert(0, "MAIN_SCORE_DESC_BAD")
        return " + ".join(labels)
    return "NO_STABLE_EDGE"


def smc_main_gate_swap_replay_comparison_rows(rows_by_gate, stats_by_gate, mode):
    table = smc_main_gate_swap_comparison_rows(rows_by_gate, mode)
    for item in table:
        stats = stats_by_gate.get(item["gate"], {})
        item["candidates_seen"] = stats.get("candidates_seen", 0)
        item["eligible_count"] = stats.get("eligible_count", 0)
        item["selected_count"] = stats.get("selected_count", item.get("n", 0))
        item["max_concurrent_observed"] = stats.get("max_concurrent_observed", 0)
        item["unique_symbols_selected"] = stats.get("unique_symbols_selected", 0)
    return table


def smc_main_gate_swap_replay_slot_rows(stats_by_gate):
    rows = []
    for gate, stats in stats_by_gate.items():
        rows.append({
            "gate": gate,
            "candidates_seen": stats.get("candidates_seen", 0),
            "eligible_count": stats.get("eligible_count", 0),
            "selected_count": stats.get("selected_count", 0),
            "excluded_fallback_rows": stats.get("excluded_fallback_rows", 0),
            "skipped_not_eligible": stats.get("skipped_not_eligible", 0),
            "skipped_max_open": stats.get("skipped_max_open", 0),
            "skipped_symbol_already_open": stats.get("skipped_symbol_already_open", 0),
            "max_concurrent_observed": stats.get("max_concurrent_observed", 0),
            "unique_symbols_selected": stats.get("unique_symbols_selected", 0),
            "release_source distribution": (
                f"actual_close_time:{stats.get('release_actual_close_time', 0)}, "
                f"raw_first_hit_time:{stats.get('release_raw_first_hit_time', 0)}, "
                f"fallback_ttl:{stats.get('release_fallback_ttl', 0)}, "
                "missing:0"
            ),
            "batch_count": stats.get("batch_count", 0),
            "batch_candidates_max": stats.get("batch_candidates_max", 0),
            "batch_eligible_max": stats.get("batch_eligible_max", 0),
        })
    return rows


def smc_main_gate_swap_stability_rows(rows_by_gate):
    stability_rows = []
    current = smc_main_gate_swap_metrics(rows_by_gate.get("CURRENT_MAIN_GATE", []))
    for gate, selected in rows_by_gate.items():
        train, holdout = smc_main_gate_swap_split(selected)
        third_metrics = [smc_main_gate_swap_metrics(third) for third in smc_main_gate_swap_thirds(selected)]
        metrics = smc_main_gate_swap_metrics(selected)
        stability_rows.append({
            "gate": gate,
            "all_R": metrics["net_R"],
            "delta_vs_current": metrics["net_R"] - current.get("net_R", 0.0),
            "train_R": smc_main_gate_swap_metrics(train)["net_R"],
            "holdout_R": smc_main_gate_swap_metrics(holdout)["net_R"],
            "positive_thirds": sum(1 for item in third_metrics if item["net_R"] > 0),
            "thirds": "; ".join(f"T{idx}:{item['net_R']:.1f}R/{item['WR%']:.1f}%" for idx, item in enumerate(third_metrics, start=1)),
        })
    return stability_rows


def smc_main_gate_swap_replay_classification(rows_by_gate, stats_by_gate, join_stats_by_gate, replay_meta):
    labels = []
    current_rows = rows_by_gate.get("CURRENT_MAIN_GATE", [])
    current_metrics = smc_main_gate_swap_metrics(current_rows)
    current_joined = sum(
        join_stats_by_gate.get("CURRENT_MAIN_GATE", Counter()).get(key, 0)
        for key in ("decision_bridge", "research_bridge", "signal_created_ts", "geometry")
    )
    if stats_by_gate.get("CURRENT_MAIN_GATE", {}).get("selected_count", 0) < 30 or current_joined < 10:
        labels.append("NEED_MORE_COVERAGE")

    def stable_beats(gate, require_all_net):
        challenger_rows = rows_by_gate.get(gate, [])
        challenger_metrics = smc_main_gate_swap_metrics(challenger_rows)
        if not challenger_rows:
            return False
        if require_all_net and challenger_metrics["net_R"] <= current_metrics["net_R"]:
            return False
        _, challenger_holdout = smc_main_gate_swap_split(challenger_rows)
        _, current_holdout = smc_main_gate_swap_split(current_rows)
        if smc_main_gate_swap_metrics(challenger_holdout)["net_R"] <= smc_main_gate_swap_metrics(current_holdout)["net_R"]:
            return False
        better_thirds = 0
        for challenger_third, current_third in zip(smc_main_gate_swap_thirds(challenger_rows), smc_main_gate_swap_thirds(current_rows)):
            if smc_main_gate_swap_metrics(challenger_third)["net_R"] > smc_main_gate_swap_metrics(current_third)["net_R"]:
                better_thirds += 1
        return better_thirds >= 2

    stable_all_net_challengers = [
        gate
        for gate in ("RESEARCH_SELECTOR_GATE", "INVERTED_SCORE_GATE", "SCORE_IGNORED_GATE")
        if stable_beats(gate, require_all_net=True)
    ]
    if stable_all_net_challengers:
        labels.append("CURRENT_GATE_BAD_SUBSET")
    if stable_beats("RESEARCH_SELECTOR_GATE", require_all_net=False):
        labels.append("RESEARCH_SELECTOR_CANDIDATE")
    if stable_beats("INVERTED_SCORE_GATE", require_all_net=False):
        labels.append("INVERTED_SCORE_CANDIDATE")
    if stable_beats("SCORE_IGNORED_GATE", require_all_net=False):
        labels.append("SCORE_IGNORE_CANDIDATE")
    if not any(label.endswith("_CANDIDATE") or label == "CURRENT_GATE_BAD_SUBSET" for label in labels):
        labels.append("NO_STABLE_REPLAY_EDGE")
    if replay_meta.get("batch_source") == SMC_MAIN_GATE_SWAP_RANK_BATCH_SOURCE and replay_meta.get("coverage_rank_index", 100.0) < SMC_MAIN_GATE_SWAP_LOW_RANK_COVERAGE_PCT:
        labels.append("LOW_BATCH_COVERAGE")
    if replay_meta.get("replay_mode") == SMC_MAIN_GATE_SWAP_APPROX_REPLAY or any(stats.get("release_fallback_ttl", 0) for stats in stats_by_gate.values()):
        labels.append("APPROX_REPLAY_LIMITATION")
    return " + ".join(labels)


def print_smc_main_gate_swap_output(args):
    logs_dir = Path(args.logs_dir)
    outcomes_path = resolve_path(logs_dir, SMC_MAIN_GATE_SWAP_OUTCOME_FILE)
    main_path = resolve_path(logs_dir, SMC_MAIN_GATE_SWAP_MAIN_DECISIONS_FILE)
    research_path = resolve_path(logs_dir, SMC_MAIN_GATE_SWAP_RESEARCH_ENTRIES_FILE)
    trades_path = Path(SMC_MAIN_GATE_SWAP_TRADE_FILE)

    universe, universe_stats = load_smc_main_gate_swap_universe(outcomes_path)
    decision_batch_map, decision_batches, decision_batch_stats = load_smc_main_gate_swap_decision_batches(main_path)
    batch_attach_stats = smc_main_gate_swap_attach_batch_metadata(universe, decision_batch_map)
    main_opened, main_stats = smc_main_gate_swap_stream_log_summary(main_path, "PAPER_SMC_MAIN_DECISION")
    research_opened, research_stats = smc_main_gate_swap_stream_log_summary(research_path, "PAPER_SMC_RESEARCH_ENTRY")
    actual_anchors, geometry_anchors, actual_summary, trade_by_id, actual_stats = smc_main_gate_swap_load_actual_trades(trades_path)

    gate_names = [
        "CURRENT_MAIN_GATE",
        "RESEARCH_SELECTOR_GATE",
        "INVERTED_SCORE_GATE",
        "SCORE_IGNORED_GATE",
        "STRUCTURAL_ONLY_D2",
    ]
    all_rows = {gate: smc_main_gate_swap_selector_rows(universe, gate) for gate in gate_names}
    topk_rows = {gate: smc_main_gate_swap_topk(universe, gate, k=5) for gate in gate_names}
    source_replay_rows, source_replay_stats, source_replay_meta = smc_main_gate_swap_concurrency_replay(
        universe,
        gate_names,
        actual_anchors,
        geometry_anchors,
        main_opened,
        research_opened,
        trade_by_id,
        replay_variant=SMC_MAIN_GATE_SWAP_SOURCE_TIME_REPLAY,
    )
    rank_replay_rows, rank_replay_stats, rank_replay_meta = smc_main_gate_swap_concurrency_replay(
        universe,
        gate_names,
        actual_anchors,
        geometry_anchors,
        main_opened,
        research_opened,
        trade_by_id,
        replay_variant=SMC_MAIN_GATE_SWAP_RANK_INDEX_REPLAY,
    )
    old_classification = smc_main_gate_swap_classification(all_rows)
    limited_classification = f"CLASSIFICATION_LIMITED_BY_ORDER_INVARIANT_ALL_ELIGIBLE: {old_classification}"
    coverage = smc_main_gate_swap_actual_open_coverage(universe, actual_summary, main_opened, trade_by_id)
    all_actual_panel, all_join_stats = smc_main_gate_swap_selected_actual_panel(
        all_rows,
        "ALL_ELIGIBLE",
        actual_anchors,
        geometry_anchors,
        main_opened,
        research_opened,
        trade_by_id,
    )
    source_replay_lane_panel, source_replay_join_stats = smc_main_gate_swap_selected_actual_lane_panel(
        source_replay_rows,
        source_replay_meta["replay_variant"],
        actual_anchors,
        geometry_anchors,
        main_opened,
        research_opened,
        trade_by_id,
    )
    rank_replay_lane_panel, rank_replay_join_stats = smc_main_gate_swap_selected_actual_lane_panel(
        rank_replay_rows,
        rank_replay_meta["replay_variant"],
        actual_anchors,
        geometry_anchors,
        main_opened,
        research_opened,
        trade_by_id,
    )
    source_replay_classification = smc_main_gate_swap_replay_classification(source_replay_rows, source_replay_stats, source_replay_join_stats, source_replay_meta)
    rank_replay_classification = smc_main_gate_swap_replay_classification(rank_replay_rows, rank_replay_stats, rank_replay_join_stats, rank_replay_meta)
    topk_actual_panel, topk_join_stats = smc_main_gate_swap_selected_actual_panel(
        topk_rows,
        "GREEDY_TOPK_APPROX",
        actual_anchors,
        geometry_anchors,
        main_opened,
        research_opened,
        trade_by_id,
    )
    status = "WARN" if universe_stats.get("parse_errors") or universe_stats.get("missing") or source_replay_meta.get("replay_mode") == SMC_MAIN_GATE_SWAP_APPROX_REPLAY else "PASS"

    print(f"{status} summary")
    print("PAPER quality simulator v0.1 --phase smc_main_gate_swap")
    print("Analyzer-only PAPER_SMC_MAIN gate-swap counterfactual over CONFIRM structural outcomes.")
    print()
    print("Files changed")
    print("- paper_quality_sim.py")
    print()
    print("Data sources loaded")
    print(f"- outcomes: {outcomes_path} lines={universe_stats.get('lines', 0)} parse_errors={universe_stats.get('parse_errors', 0)} deduped_universe={universe_stats.get('deduped', 0)}")
    print(f"- main decisions: {main_path} rows={main_stats.get('rows', 0)} opens={main_stats.get('action_open', 0)} bridged_keys={len(main_opened)} duplicate_open_key={main_stats.get('duplicate_open_key', 0)} parse_errors={main_stats.get('parse_errors', 0)}")
    print(f"- decision rank batches: rows={decision_batch_stats.get('decision_rows', 0)} rank_rows={decision_batch_stats.get('rank_index_metadata_rows', 0)} batches={len(decision_batches)} unique_keys={decision_batch_stats.get('rank_index_unique_keys', 0)} duplicate_dedup_key_metadata={decision_batch_stats.get('duplicate_dedup_key_metadata', 0)} missing_rank_index={decision_batch_stats.get('missing_rank_index', 0)} parse_errors={decision_batch_stats.get('parse_errors', 0)}")
    print(f"- research entries: {research_path} rows={research_stats.get('rows', 0)} opens={research_stats.get('action_open', 0)} bridged_keys={len(research_opened)} duplicate_open_key={research_stats.get('duplicate_open_key', 0)} parse_errors={research_stats.get('parse_errors', 0)}")
    print(f"- paper trades: {trades_path} rows={actual_stats.get('rows', 0)} trade_by_id={actual_stats.get('trade_by_id', 0)} PAPER_SMC_MAIN={actual_stats.get('paper_smc_main_rows', 0)} CONFIRM_SMC_RESEARCH={actual_stats.get('confirm_smc_research_rows', 0)} CONFIRM={actual_stats.get('confirm_rows', 0)}")
    print()
    print("Bridge implementation details")
    print("- selected_row._dedup_key -> decision/research OPEN dedup_key -> opened_trade_id -> paper_trades.csv id.")
    print("- Join priority: decision_bridge, research_bridge, signal_created_ts fallback, geometry fallback.")
    print("- Duplicate OPEN keys keep the latest sortable observed/source timestamp row with non-null opened_trade_id.")
    print()
    print("paper_trades id index result")
    print(f"- trade_by_id={actual_stats.get('trade_by_id', 0)} duplicate_trade_id={actual_stats.get('duplicate_trade_id', 0)}")
    print(f"- PAPER_SMC_MAIN with id={actual_stats.get('paper_smc_main_with_id', 0)} missing_id={actual_stats.get('paper_smc_main_missing_id', 0)}")
    print(f"- CONFIRM_SMC_RESEARCH with id={actual_stats.get('confirm_smc_research_with_id', 0)} missing_id={actual_stats.get('confirm_smc_research_missing_id', 0)}")
    print()
    print("Decision OPEN bridge result")
    print(f"- main bridge keys={len(main_opened)} action_OPEN={main_stats.get('action_open', 0)} open_or_opened_true={main_stats.get('open_or_opened_true', 0)} duplicate_open_key={main_stats.get('duplicate_open_key', 0)} open_missing_trade_id={main_stats.get('open_missing_trade_id', 0)} open_missing_key={main_stats.get('open_missing_key', 0)}")
    print()
    print("Research bridge result")
    if research_opened:
        print(f"- research bridge keys={len(research_opened)} action_OPEN={research_stats.get('action_open', 0)} open_or_opened_true={research_stats.get('open_or_opened_true', 0)} duplicate_open_key={research_stats.get('duplicate_open_key', 0)} open_missing_trade_id={research_stats.get('open_missing_trade_id', 0)} open_missing_key={research_stats.get('open_missing_key', 0)}")
    else:
        print("- research bridge unavailable or empty; no research OPEN rows with both dedup_key/source key and opened_trade_id were found.")
    print()
    print("Universe counts and dedupe counts")
    print(f"- terminal policy: status=RESOLVED, geometry_status=VALID_GEOMETRY, outcome_trackable=true when present, entry_type=CONFIRM when present")
    print(f"- candidate_rows={universe_stats.get('candidate_rows', 0)} kept_before_dedupe={universe_stats.get('kept_before_dedupe', 0)} deduped={universe_stats.get('deduped', 0)} dedupe_replaced={universe_stats.get('dedupe_replaced', 0)} dedupe_dropped={universe_stats.get('dedupe_dropped', 0)} missing_key={universe_stats.get('missing_key', 0)}")
    print(f"- score fallback counts: missing_score_v2_structural_shadow={universe_stats.get('missing_score_v2_structural_shadow', 0)} missing_structural_modifier_shadow={universe_stats.get('missing_structural_modifier_shadow', 0)} missing_score_all={universe_stats.get('missing_score_all', 0)}")
    print()
    print("Selector definitions actually implemented")
    print("- CURRENT_MAIN_GATE: main eligible types, valid geometry, effective_score >= 2.5, weak_structure_blocked excluded if present; ranked by candidate priority, effective_score DESC, source_timestamp DESC.")
    print("- RESEARCH_SELECTOR_GATE: LOW_SCORE/MID_SCORE_WEAK_BOS/RR_FAIL only, ACCEPTED_CONFIRM excluded, allowed structural decisions, effective_score >= 2.5, FIFO by source timestamp/log order.")
    print("- INVERTED_SCORE_GATE: same eligibility as current main; ranked by candidate priority, effective_score ASC, source_timestamp DESC.")
    print("- SCORE_IGNORED_GATE: same eligibility as current main; FIFO by source timestamp/log order after admission floor.")
    print("- STRUCTURAL_ONLY_D2: same as score ignored but excludes bos_quality WEAK/NO_FOLLOWTHROUGH when field is available.")
    print()
    print("Replay model assumptions")
    print(f"- analyzer-only {source_replay_meta['replay_mode']}; no runtime path reads this output.")
    print(f"- exact_scan_id_available={str(source_replay_meta.get('exact_scan_id_available', False)).lower()}.")
    print(f"- max_open_slots={source_replay_meta['max_open_slots']} one_per_symbol={str(source_replay_meta['one_per_symbol']).lower()} fallback_ttl_secs={source_replay_meta['fallback_ttl_secs']}.")
    print(f"- {SMC_MAIN_GATE_SWAP_SOURCE_TIME_REPLAY}: batch_source={source_replay_meta.get('batch_source')} scan_id rows={source_replay_meta.get('rows_with_scan_id', 0)} missing_scan_id_rows={source_replay_meta.get('rows_without_scan_id', 0)} batches={source_replay_meta.get('batch_count', 0)}.")
    print(f"- source timestamp fallback count={source_replay_meta.get('source_ts_fallback_count', 0)} log-order-index fallback count={source_replay_meta.get('log_order_index_fallback_count', 0)}.")
    print(f"- {SMC_MAIN_GATE_SWAP_RANK_INDEX_REPLAY}: batch_source={rank_replay_meta.get('batch_source')} coverage_rank_index={rank_replay_meta.get('coverage_rank_index', 0.0):.2f}% batches={rank_replay_meta.get('batch_count', 0)} fallback_source={rank_replay_meta.get('fallback_source')} excluded_fallback_rows={rank_replay_meta.get('rank_index_excluded_fallback_rows', 0)}.")
    print("- Rank-index replay is pure covered rows only; rows without rank_index metadata remain in the source-time replay comparison.")
    print("- APPROX_CONCURRENCY_REPLAY: scan_id is unavailable/incomplete, so exact runtime scan reconstruction is not claimed.")
    print()
    print("Outcome model policies")
    print("- raw_first_hit_R: SL=-1, 1R=+1, 1.5R=+1.5, 2R=+2, TP=rr/planned_rr. AMBIGUOUS/AMBIGUOUS_SAME_BAR excluded.")
    print("- sl_gap_adjusted_theoretical_R: not calculated; analyzer has no safe tier/gap source without importing runtime execution helpers.")
    print("- actual_close_R: separate sparse anchor from paper_trades.csv. Decision/research opened_trade_id bridge is primary; signal_created_ts and geometry anchors are fallback only; never mixed with raw metrics.")
    print("- GREEDY_TOPK_APPROX: K=5 per source_timestamp bucket, matching paper_smc_main_max_open default/current config value observed in config files; not an exact slot replay.")
    print()
    print("Release-time model")
    print("- actual joined row: close_time when parseable; otherwise signal/open timestamp plus trade_age_minutes when available.")
    print("- raw outcome row: first_hit time_to_sl/1R/1.5R/2R/TP seconds; TP falls back to first favorable terminal time; AMBIGUOUS uses earliest SL/favorable time.")
    print(f"- missing release evidence: fallback TTL {SMC_MAIN_GATE_SWAP_REPLAY_FALLBACK_TTL_SECS} seconds.")

    headers = ["mode", "gate", "n", "evaluable", "net_R raw", "WR raw", "avg_R", "median_R", "maxDD_R", "SL_first%", "hit_1R%", "hit_1.5R%", "hit_2R%", "median MFE_R", "median MAE_R", "train_R", "holdout_R", "thirds"]
    replay_headers = ["mode", "gate", "candidates_seen", "eligible_count", "selected_count", "n", "evaluable", "net_R raw", "WR raw", "avg_R", "median_R", "maxDD_R", "SL_first%", "hit_1R%", "hit_1.5R%", "hit_2R%", "median MFE_R", "median MAE_R", "train_R", "holdout_R", "thirds", "max_concurrent_observed", "unique_symbols_selected"]
    print_table("Gate comparison table - ALL_ELIGIBLE", headers, smc_main_gate_swap_comparison_rows(all_rows, "ALL_ELIGIBLE"))
    print_table(
        f"CONCURRENCY_REPLAY gate comparison table - {source_replay_meta['replay_variant']}",
        replay_headers,
        smc_main_gate_swap_replay_comparison_rows(source_replay_rows, source_replay_stats, source_replay_meta["replay_variant"]),
    )
    print_table(
        f"CONCURRENCY_REPLAY skip reasons / slot stats - {source_replay_meta['replay_variant']}",
        ["gate", "candidates_seen", "eligible_count", "selected_count", "excluded_fallback_rows", "skipped_not_eligible", "skipped_max_open", "skipped_symbol_already_open", "max_concurrent_observed", "unique_symbols_selected", "release_source distribution", "batch_count", "batch_candidates_max", "batch_eligible_max"],
        smc_main_gate_swap_replay_slot_rows(source_replay_stats),
    )
    print_table(
        f"CONCURRENCY_REPLAY gate comparison table - {rank_replay_meta['replay_variant']}",
        replay_headers,
        smc_main_gate_swap_replay_comparison_rows(rank_replay_rows, rank_replay_stats, rank_replay_meta["replay_variant"]),
    )
    print_table(
        f"CONCURRENCY_REPLAY skip reasons / slot stats - {rank_replay_meta['replay_variant']}",
        ["gate", "candidates_seen", "eligible_count", "selected_count", "excluded_fallback_rows", "skipped_not_eligible", "skipped_max_open", "skipped_symbol_already_open", "max_concurrent_observed", "unique_symbols_selected", "release_source distribution", "batch_count", "batch_candidates_max", "batch_eligible_max"],
        smc_main_gate_swap_replay_slot_rows(rank_replay_stats),
    )
    print_table("Gate comparison table - GREEDY_TOPK_APPROX", headers, smc_main_gate_swap_comparison_rows(topk_rows, "GREEDY_TOPK_APPROX"))
    print_table(
        "Rank-index batch metadata coverage",
        ["scope", "rows", "rank_index rows", "coverage_rank_index", "fallback_source", "fallback rows"],
        smc_main_gate_swap_batch_coverage_rows(all_rows, batch_attach_stats),
    )
    print_table(
        "Decision suppress diagnostics - not applied as replay filters",
        ["suppress_reason", "decision rows"],
        smc_main_gate_swap_suppress_diagnostic_rows(decision_batch_stats),
    )
    print_table(
        "Actual paper opens anchor",
        ["entry_type", "count", "net actual_R", "WR actual", "avg actual_R", "avg win", "avg loss", "status distribution", "exit_type distribution", "actual_entry_type distribution"],
        smc_main_gate_swap_actual_anchor_rows(actual_summary),
    )

    print_table(
        "Selected-row actual_close_R join panel - ALL_ELIGIBLE",
        ["mode", "gate", "selected rows", "decision_bridge rows", "research_bridge rows", "fallback rows", "none rows", "count", "net actual_R", "WR actual", "avg actual_R", "status distribution", "exit_type distribution", "actual_entry_type distribution", "actual source distribution"],
        all_actual_panel,
    )
    print_table(
        f"Actual join panel by source lane - {source_replay_meta['replay_variant']}",
        ["mode", "gate", "lane", "selected rows", "joined rows", "none rows", "count", "net actual_R", "WR actual", "avg actual_R", "status distribution", "exit_type distribution", "actual_entry_type distribution", "actual source distribution", "note"],
        source_replay_lane_panel,
    )
    print_table(
        f"Actual join panel by source lane - {rank_replay_meta['replay_variant']}",
        ["mode", "gate", "lane", "selected rows", "joined rows", "none rows", "count", "net actual_R", "WR actual", "avg actual_R", "status distribution", "exit_type distribution", "actual_entry_type distribution", "actual source distribution", "note"],
        rank_replay_lane_panel,
    )
    print_table(
        "Selected-row actual_close_R join panel - GREEDY_TOPK_APPROX",
        ["mode", "gate", "selected rows", "decision_bridge rows", "research_bridge rows", "fallback rows", "none rows", "count", "net actual_R", "WR actual", "avg actual_R", "status distribution", "exit_type distribution", "actual_entry_type distribution", "actual source distribution"],
        topk_actual_panel,
    )

    print_table(
        "Resolved coverage of PAPER_SMC_MAIN actual opens",
        ["PAPER_SMC_MAIN actual opens total", "actual opens with decision bridge id match", "actual opens with universe resolved key match", "actual opens outside RESOLVED universe", "decision bridge ids missing in paper_trades", "outside DATA_MISSING", "outside OPEN", "outside no outcome row", "outside no key", "missing reasons"],
        [coverage],
    )

    print_table(
        "Score bucket inversion table - CURRENT_MAIN_GATE ALL_ELIGIBLE",
        ["score_bucket", "n", "net_R", "WR%", "SL_first%", "median_MFE_R"],
        smc_main_gate_swap_breakdown_rows(all_rows["CURRENT_MAIN_GATE"], "score_bucket"),
    )

    if args.breakdowns:
        for field in ("side", "regime", "bos_quality", "candidate_type/reason"):
            print_table(
                f"Breakdown by {field} - CURRENT_MAIN_GATE ALL_ELIGIBLE",
                [field, "n", "net_R", "WR%", "SL_first%", "median_MFE_R"],
                smc_main_gate_swap_breakdown_rows(all_rows["CURRENT_MAIN_GATE"], field, top=30 if field == "regime" else None),
            )
        for replay_label, rows_by_gate in (
            (source_replay_meta["replay_variant"], source_replay_rows),
            (rank_replay_meta["replay_variant"], rank_replay_rows),
        ):
            for gate, selected in rows_by_gate.items():
                for field in ("side", "regime", "bos_quality", "score_bucket", "candidate_type/reason"):
                    print_table(
                        f"Breakdown by {field} - {gate} {replay_label}",
                        [field, "n", "net_R", "WR%", "SL_first%", "median_MFE_R"],
                        smc_main_gate_swap_breakdown_rows(selected, field, top=30 if field == "regime" else None),
                    )
    if args.stability:
        print_table(
            "Stability - train/holdout/thirds - ALL_ELIGIBLE order-invariant",
            ["gate", "all_R", "delta_vs_current", "train_R", "holdout_R", "positive_thirds", "thirds"],
            smc_main_gate_swap_stability_rows(all_rows),
        )
        print_table(
            f"Stability - train/holdout/thirds - {source_replay_meta['replay_variant']}",
            ["gate", "all_R", "delta_vs_current", "train_R", "holdout_R", "positive_thirds", "thirds"],
            smc_main_gate_swap_stability_rows(source_replay_rows),
        )
        print_table(
            f"Stability - train/holdout/thirds - {rank_replay_meta['replay_variant']}",
            ["gate", "all_R", "delta_vs_current", "train_R", "holdout_R", "positive_thirds", "thirds"],
            smc_main_gate_swap_stability_rows(rank_replay_rows),
        )

    print()
    print("Classification")
    print(f"- SOURCE_TIME_REPLAY_CLASSIFICATION: {source_replay_classification}")
    print(f"- RANK_INDEX_REPLAY_CLASSIFICATION: {rank_replay_classification}")
    print(f"- {limited_classification}")
    print("- Warning: ALL_ELIGIBLE aggregate metrics are order-invariant, so CURRENT_MAIN_GATE, INVERTED_SCORE_GATE, and SCORE_IGNORED_GATE can classify identically even when ranking differs.")
    print()
    print("Safety confirmation")
    print("- Analyzer/reporting only; no execution, exchange, config, state, CSV, tracker, observer, dispatch, scoring, SL/TP/trailing, risk/DD, PAPER runtime, or LIVE code paths are modified.")
    print("- This phase reads on-disk logs/CSV and prints to stdout only.")
    print("- No config/state/LIVE changes, no CSV/log rewrite, and no behavior path reads this output.")
    print()
    print("Limitations")
    print("- APPROX_CONCURRENCY_REPLAY: scan_id is unavailable/incomplete, so exact runtime scan reconstruction is not claimed.")
    print("- Rank-index segmentation is approximate and duplicate dedup_key metadata is resolved by closest source timestamp with latest decision-observed tie-break.")
    print("- GREEDY_TOPK_APPROX is source_timestamp bucket top-K, not exact open-slot lifecycle replay.")
    print("- Weak-structure block is mirrored only when logged fields exist; this phase does not import runtime config or signal_dispatcher.")
    print("- sl_gap_adjusted_theoretical_R is intentionally unavailable without runtime tier/gap helpers.")
    print("- Decision/research logs are summarized for anchors; selector universe is the structural outcome log as requested.")
    print()
    print("Recommended next action only")
    print("- Audit any replay candidate with exact scan IDs before considering a separate runtime gate change.")


def interpret(metrics):
    all_gate_metrics = {gate: metrics[("ALL", gate)] for gate, _ in GATES if gate != "BASELINE"}
    practical = {
        gate: item
        for gate, item in all_gate_metrics.items()
        if item["collapse_pct"] < HIGH_COLLAPSE_PCT
        and item["resolved_collapse_pct"] < NEAR_FULL_COLLAPSE_PCT
        and item["blocked_resolved"] >= LOW_SAMPLE_N
    }
    best_gate = best = None
    if practical:
        best_gate, best = max(practical.items(), key=lambda item: item[1]["avoided_R"])
    collapsed = [gate for gate, item in all_gate_metrics.items() if item["collapse_pct"] >= 60.0]
    high_collapse = [gate for gate, item in all_gate_metrics.items() if item["collapse_pct"] >= HIGH_COLLAPSE_PCT]
    degenerate = [
        gate
        for gate, item in all_gate_metrics.items()
        if item["resolved"] and item["resolved_collapse_pct"] >= NEAR_FULL_COLLAPSE_PCT
    ]
    low_sample = [
        gate
        for gate, item in all_gate_metrics.items()
        if 0 < item["blocked_resolved"] < LOW_SAMPLE_N
    ]
    consistency = []
    for gate in all_gate_metrics:
        train = metrics.get(("TRAIN", gate), {}).get("avoided_R", 0.0)
        holdout = metrics.get(("HOLDOUT", gate), {}).get("avoided_R", 0.0)
        label = gate_action_label(all_gate_metrics[gate])
        if train > 0 and holdout > 0:
            consistency.append(f"{gate}: consistent avoided loss ({label})")
        elif train > 0 >= holdout:
            consistency.append(f"{gate}: train-only edge, keep log-only ({label})")
        elif train <= 0 < holdout:
            consistency.append(f"{gate}: holdout-only edge, not enough data ({label})")
        else:
            consistency.append(f"{gate}: no avoided-loss support ({label})")
    print()
    print("Interpretation")
    if best_gate:
        print(f"- Best practical avoided-loss gate on ALL: {best_gate} ({best['avoided_R']:.2f}R avoided, {best['collapse_pct']:.1f}% sample collapse).")
    else:
        print("- Best practical avoided-loss gate on ALL: none; all positive candidates are high-collapse, degenerate, or low-sample.")
    raw_gate, raw_best = max(all_gate_metrics.items(), key=lambda item: item[1]["avoided_R"])
    if raw_gate != best_gate:
        print(f"- Raw avoided_R leader is {raw_gate} ({raw_best['avoided_R']:.2f}R), but it is {gate_action_label(raw_best)} and not actionable on current data.")
    if collapsed:
        print(f"- Sample collapse warning: {', '.join(collapsed)} blocked at least 60% of rows.")
    else:
        print("- No gate crossed the 60% sample-collapse warning threshold.")
    if high_collapse:
        print(f"- HIGH_COLLAPSE gates (>= {HIGH_COLLAPSE_PCT:.0f}% total blocked): {', '.join(high_collapse)}.")
    if degenerate:
        print(f"- DEGENERATE gates on current data (>= {NEAR_FULL_COLLAPSE_PCT:.0f}% resolved blocked): {', '.join(degenerate)}.")
    if low_sample:
        print(f"- LOW_SAMPLE gates (< {LOW_SAMPLE_N} blocked resolved rows): {', '.join(low_sample)}.")
    holdout_resolved = metrics.get(("HOLDOUT", "BASELINE"), {}).get("resolved", 0)
    if holdout_resolved < SMALL_HOLDOUT_RESOLVED_N:
        print(f"- Small-holdout warning: HOLDOUT resolved n={holdout_resolved}, below {SMALL_HOLDOUT_RESOLVED_N}.")
    for line in consistency:
        print(f"- {line}.")
    print("- RR_ARTIFICIAL_PROXY remains log-only because it depends on outcome evaluation and no ATR snapshot exists.")
    print("- No LIVE or production change should be inferred from this report.")
    print_method_disclaimers()


def write_csv(path, rows, overwrite):
    if path.exists() and not overwrite:
        raise FileExistsError(f"{path} exists; pass --overwrite to replace it")
    fields = [
        "dedup_key",
        "symbol",
        "side",
        "source_timestamp",
        "regime",
        "router_join_status",
        "status",
        "first_hit",
        "realized_r",
        "mfe_r",
        "mae_r",
        "bad_regime_block",
        "worst_combo_block",
        "weak_bos_block",
        "strong_bos_confirmation_block",
        "rr_artificial_proxy",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            outcome = row.get("_outcome") or {}
            rr = planned_rr(row)
            mfe = as_float(outcome.get("mfe_r"), 0.0) or 0.0
            writer.writerow(
                {
                    "dedup_key": row.get("dedup_key"),
                    "symbol": row.get("symbol"),
                    "side": row.get("side"),
                    "source_timestamp": row.get("_source_ts"),
                    "regime": row.get("regime"),
                    "router_join_status": row.get("router_join_status"),
                    "status": status_of(row),
                    "first_hit": outcome.get("first_hit"),
                    "realized_r": realized_r(row) if status_of(row) == "RESOLVED" else "",
                    "mfe_r": outcome.get("mfe_r"),
                    "mae_r": outcome.get("mae_r"),
                    "bad_regime_block": gate_bad_regime(row),
                    "worst_combo_block": gate_worst_combo(row),
                    "weak_bos_block": gate_weak_bos(row),
                    "strong_bos_confirmation_block": gate_bos_confirmation(row),
                    "rr_artificial_proxy": bool(rr is not None and rr >= 2.0 and mfe < 0.75),
                }
            )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read-only PAPER decision-row counterfactual avoided-loss simulator."
    )
    parser.add_argument("--logs-dir", default="logs", help="Directory containing JSONL logs. Default: logs")
    parser.add_argument("--phase", choices=["reversal", "smc_v0_2_shadow", "smc_main_gate_swap"], default="", help="Run a phase-specific simulator extension. Currently supported: reversal, smc_v0_2_shadow, smc_main_gate_swap")
    parser.add_argument("--stability", action="store_true", help="With --phase reversal, smc_v0_2_shadow, or smc_main_gate_swap, print stability / holdout slicing")
    parser.add_argument("--symbol-normalized", action="store_true", help="With --phase reversal --stability, print symbol-normalized robustness stress tests")
    parser.add_argument("--decisions", default="paper_smc_main_decisions.jsonl", help="Decision JSONL filename or path")
    parser.add_argument("--outcomes", default="confirm_structural_outcomes.jsonl", help="Outcome JSONL filename or path")
    parser.add_argument("--router", default="market_regime_router_shadow.jsonl", help="Router JSONL filename or path")
    parser.add_argument("--write", action="store_true", help="Write a new result CSV. Default is print-only")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting the result CSV when --write is passed")
    parser.add_argument("--output", default="", help="Output CSV path for --write. Default: logs/paper_quality_sim_result.csv")
    parser.add_argument("--breakdowns", action="store_true", help="Print top breakdown tables for each gate")
    parser.add_argument("--blocked", default="", help="With --breakdowns, print blocked-row breakdowns for a named gate")
    parser.add_argument("--grid", action="store_true", help="Run fast batch/grid avoided-loss research experiments")
    parser.add_argument("--top", type=int, default=20, help="Number of grid candidates to print. Default: 20")
    parser.add_argument("--max-collapse", type=float, default=0.60, help="Max ALL collapse ratio for practical grid candidates. Default: 0.60")
    parser.add_argument("--max-holdout-collapse", type=float, default=0.80, help="Max HOLDOUT collapse ratio when holdout has enough resolved rows. Default: 0.80")
    parser.add_argument("--min-blocked-resolved", type=int, default=30, help="Minimum ALL blocked resolved rows for practical grid candidates. Default: 30")
    parser.add_argument("--min-holdout-resolved", type=int, default=50, help="Minimum HOLDOUT resolved rows before applying holdout collapse limit. Default: 50")
    return parser.parse_args()


def resolve_path(logs_dir, value):
    path = Path(value)
    if path.is_absolute() or path.parent != Path("."):
        return path
    return Path(logs_dir) / path


def main():
    args = parse_args()
    logs_dir = Path(args.logs_dir)
    if args.phase == "smc_main_gate_swap":
        print_smc_main_gate_swap_output(args)
        return

    if args.phase == "smc_v0_2_shadow":
        shadow_path = resolve_path(logs_dir, SMC_V0_2_SHADOW_OUTCOME_FILE)
        rows, stats = load_smc_v0_2_shadow_outcomes(shadow_path)
        print_smc_v0_2_shadow_output(rows, stats, shadow_path, args)
        return

    if args.phase == "reversal":
        reversal_path = resolve_path(logs_dir, REVERSAL_OUTCOME_FILE)
        rows, stats = load_reversal_outcomes(reversal_path)
        print_reversal_phase_output(rows, stats, reversal_path)
        if args.stability:
            print_reversal_stability_output(rows)
            if args.symbol_normalized:
                print_reversal_symbol_normalized_output(rows)
        return

    decisions_path = resolve_path(logs_dir, args.decisions)
    outcomes_path = resolve_path(logs_dir, args.outcomes)
    router_path = resolve_path(logs_dir, args.router)

    decisions, decision_stats = load_decisions(decisions_path)
    outcomes_by_dedup, outcomes_by_fallback, outcome_stats = load_outcomes(outcomes_path)
    regime_index, regime_stats = load_regimes(router_path)
    rows, join_counts = join_rows(decisions, outcomes_by_dedup, outcomes_by_fallback, regime_index)

    if args.grid:
        print_grid_output(
            rows,
            args,
            paths=(decisions_path, outcomes_path, router_path),
            stats=(decision_stats, outcome_stats, regime_stats),
            join_counts=join_counts,
        )
        return

    print("PAPER quality simulator v0.1")
    print_method_disclaimers()
    print()
    print(f"Inputs read:")
    print(f"- decisions: {decisions_path} loaded={decision_stats.get('loaded', 0)} parse_errors={decision_stats.get('parse_errors', 0)} deduped={decision_stats.get('deduped', 0)}")
    print(f"- outcomes:  {outcomes_path} loaded={outcome_stats.get('loaded', 0)} parse_errors={outcome_stats.get('parse_errors', 0)} dedup_keys={outcome_stats.get('dedup_keys', 0)}")
    print(f"- router:    {router_path} loaded={regime_stats.get('loaded', 0)} parse_errors={regime_stats.get('parse_errors', 0)} symbols={regime_stats.get('symbols', 0)}")
    print("Safety: standard-library only; no execution/exchange/state/pool imports; no writes unless --write is passed.")
    print(f"Join counts: {dict(join_counts)}")
    if join_counts.get("outcome_fallback_join", 0):
        print("WARNING: fallback outcome joins were used. Review rows before relying on those joins.")
    if join_counts.get("regime_unknown_ts", 0):
        print("WARNING: some rows lacked usable timestamps and were excluded from regime-join claims.")

    table, metrics = metric_rows(rows)
    print_table(
        "Gate metrics",
        ["split", "gate", "total", "resolved", "open", "missing", "blocked", "blocked_res", "collapse%", "label", "base_R", "kept_R", "blocked_R", "avoided_R", "WR%", "maxDD_R"],
        table,
    )

    missing_reasons = Counter()
    for row in rows:
        if status_of(row) == "DATA_MISSING":
            missing_reasons[(row.get("_outcome") or {}).get("data_missing_reason") or "UNKNOWN"] += 1
    print_table("DATA_MISSING reasons", ["reason", "count"], [{"reason": key, "count": value} for key, value in missing_reasons.most_common()])

    bos_enums, confirm_enums = known_unknown_enums(rows)
    print_table("Observed bos_quality enums", ["enum", "count"], [{"enum": key, "count": value} for key, value in bos_enums.most_common()])
    print_table("Observed bos_confirmation enums", ["enum", "count"], [{"enum": key, "count": value} for key, value in confirm_enums.most_common()])
    print_table("Field coverage summary", ["field", "total", "non_null", "non_null%", "UNKNOWN", "UNKNOWN%"], field_coverage(rows))

    print_rr_distribution(rows)
    if args.breakdowns:
        if args.blocked:
            print_blocked_breakdowns(rows, args.blocked)
        else:
            for gate_name, gate in GATES:
                print_breakdowns(rows, gate_name, gate)
    interpret(metrics)

    if args.write:
        out_path = Path(args.output) if args.output else logs_dir / "paper_quality_sim_result.csv"
        write_csv(out_path, rows, args.overwrite)
        print(f"CSV written: {out_path}")


if __name__ == "__main__":
    main()
