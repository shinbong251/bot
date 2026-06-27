#!/usr/bin/env python3
"""Audit shadow-only partial TP outcomes for CONFIRM_SMC_RESEARCH closes."""

import json
import math
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOG_PATH = ROOT / "logs" / "research_partial_tp_shadow.jsonl"
CONFIG_JSON = ROOT / "config.json"
MODEL_FIELDS = (
    "model_30_70_r",
    "model_40_60_r",
    "model_50_50_r",
    "model_60_40_r",
)


def read_json(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


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


def fnum(value, default=None):
    try:
        if value in (None, ""):
            return default
        out = float(value)
        if math.isnan(out) or math.isinf(out):
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


def pf(values):
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if losses <= 0:
        return None if gains <= 0 else float("inf")
    return gains / losses


def win_rate(values):
    return None if not values else sum(1 for value in values if value > 0) / len(values)


def max_drawdown(values):
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return abs(worst)


def loss_streak(values):
    best = 0
    cur = 0
    for value in values:
        if value < 0:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def row_ts(row):
    return fnum(row.get("close_time"), fnum(row.get("ts"), 0.0)) or 0.0


def values_for(rows, field):
    return [value for value in (fnum(row.get(field)) for row in rows) if value is not None]


def metrics_for_values(values):
    return {
        "n": len(values),
        "net_R": round(sum(values), 4),
        "PF": pf(values),
        "WR": win_rate(values),
        "maxDD": round(max_drawdown(values), 4),
        "streak": loss_streak(values),
    }


def metrics(rows, field="realized_r_current"):
    return metrics_for_values(values_for(rows, field))


def split(rows, field):
    buckets = defaultdict(list)
    for row in rows:
        value = row.get(field)
        if field == "trail_phase":
            value = f"PHASE_{value}" if value not in (None, "") else "UNKNOWN"
        label = str(value or "UNKNOWN").upper()
        buckets[label].append(row)
    return {
        label: metrics(bucket)
        for label, bucket in sorted(buckets.items(), key=lambda item: (-len(item[1]), item[0]))
    }


def model_summary(rows, field):
    current_values = values_for(rows, "realized_r_current")
    model_values = values_for(rows, field)
    summary = metrics_for_values(model_values)
    summary["delta_net_R_vs_current"] = round(sum(model_values) - sum(current_values), 4)
    return summary


def build_summary(rows=None):
    rows = sorted(rows if rows is not None else read_jsonl(LOG_PATH), key=row_ts)
    cfg = read_json(CONFIG_JSON, {})
    baseline_ts = fnum(cfg.get("research_health_baseline_ts"), 0.0) or 0.0
    active_rows = [row for row in rows if baseline_ts > 0 and row_ts(row) >= baseline_ts]
    return {
        "n": len(rows),
        "eligible_1r_n": sum(1 for row in rows if row.get("reached_1r") is True),
        "missing_mfe_n": sum(1 for row in rows if fnum(row.get("max_profit_r")) is None),
        "current": metrics(rows),
        "models": {field: model_summary(rows, field) for field in MODEL_FIELDS},
        "side_split": split(rows, "side"),
        "phase_split": split(rows, "trail_phase"),
        "active_baseline_ts": baseline_ts,
        "active_baseline": metrics(active_rows) if baseline_ts > 0 else None,
        "active_baseline_models": (
            {field: model_summary(active_rows, field) for field in MODEL_FIELDS}
            if baseline_ts > 0
            else None
        ),
        "live_only": metrics([row for row in rows if row.get("source") == "live"]),
    }


def print_metrics(label, data):
    print(
        f"{label}: n={data['n']} net_R={fmt(data['net_R'])} PF={fmt(data['PF'])} "
        f"WR={fmt(data['WR'])} maxDD={fmt(data['maxDD'])} streak={data['streak']}"
    )


def main():
    summary = build_summary()
    status = "PASS" if summary["n"] > 0 else "WARN"
    print(f"{status} partial TP shadow audit completed")
    print(f"log={LOG_PATH}")
    print(f"n={summary['n']}")
    print(f"eligible_1r_n={summary['eligible_1r_n']}")
    print(f"missing_mfe_n={summary['missing_mfe_n']}")
    print_metrics("current", summary["current"])
    for field in MODEL_FIELDS:
        data = summary["models"][field]
        print_metrics(field, data)
        print(f"{field}_delta_vs_current={fmt(data['delta_net_R_vs_current'])}")
    print("side_split=" + json.dumps(summary["side_split"], sort_keys=True, default=str))
    print("phase_split=" + json.dumps(summary["phase_split"], sort_keys=True, default=str))
    if summary["active_baseline"] is not None:
        print(f"active_baseline_ts={summary['active_baseline_ts']}")
        print_metrics("active_baseline_current", summary["active_baseline"])
        print("active_baseline_models=" + json.dumps(summary["active_baseline_models"], sort_keys=True, default=str))
    else:
        print("active_baseline_split=n/a")
    print_metrics("live_only", summary["live_only"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
