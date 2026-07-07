#!/usr/bin/env python3
"""Read-only taxonomy audit for BTC MTF LOG_ONLY unknown reasons."""

import json
import os
import sys
from collections import Counter, defaultdict

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_DIR = os.path.join(REPO_ROOT, "logs")

LOG_PATHS = (
    os.path.join(LOG_DIR, "live_smc_research_decisions.jsonl"),
    os.path.join(LOG_DIR, "paper_smc_research_entry_context.jsonl"),
    os.path.join(LOG_DIR, "paper_confirm_entry_context.jsonl"),
)

UNKNOWN_REASONS = (
    "NONE",
    "BTC_SNAPSHOT_TOO_STALE",
    "ENTRY_TS_MISSING",
    "BTC_CONTEXT_FETCH_ERROR",
    "NO_INDEPENDENT_BTC_MTF_DATA",
)


def _read_jsonl(path):
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        return []
    return rows


def _safe_float(value):
    try:
        if value in (None, ""):
            return None
        out = float(value)
        if out != out:
            return None
        return out
    except (TypeError, ValueError):
        return None


def _ctx(row):
    nested = row.get("entry_context") if isinstance(row.get("entry_context"), dict) else {}
    out = {}
    for field in (
        "btc_context_available",
        "btc_data_mode",
        "btc_unknown_reason",
        "btc_context_age_sec",
        "btc_mtf_alignment",
        "btc_m5_trend",
        "btc_m15_trend",
        "btc_h1_trend",
        "btc_m5_momentum",
        "btc_m15_momentum",
        "btc_h1_momentum",
    ):
        value = row.get(field)
        if value in (None, ""):
            value = nested.get(field)
        out[field] = value
    if not out.get("btc_unknown_reason"):
        mode = out.get("btc_data_mode")
        if mode == "INDEPENDENT_BTC_MTF":
            out["btc_unknown_reason"] = "NONE"
        elif mode == "BTC_CONTEXT_STALE":
            out["btc_unknown_reason"] = "BTC_SNAPSHOT_TOO_STALE"
        elif mode == "BTC_CONTEXT_FETCH_ERROR":
            out["btc_unknown_reason"] = "BTC_CONTEXT_FETCH_ERROR"
        else:
            out["btc_unknown_reason"] = "NO_INDEPENDENT_BTC_MTF_DATA"
    return out


def _pct(num, den):
    return 100.0 * num / den if den else 0.0


def _median(values):
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    mid = len(vals) // 2
    if len(vals) % 2:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2.0


def main():
    rows = []
    by_source = defaultdict(list)
    for path in LOG_PATHS:
        source = os.path.basename(path)
        for row in _read_jsonl(path):
            item = {"source": source, "ctx": _ctx(row)}
            rows.append(item)
            by_source[source].append(item)

    reason_counts = Counter(item["ctx"].get("btc_unknown_reason") for item in rows)
    mode_counts = Counter(item["ctx"].get("btc_data_mode") or "MISSING" for item in rows)
    alignment_counts = Counter(item["ctx"].get("btc_mtf_alignment") or "UNKNOWN" for item in rows)
    ages = [_safe_float(item["ctx"].get("btc_context_age_sec")) for item in rows]
    available = sum(1 for item in rows if item["ctx"].get("btc_context_available") is True)
    total = len(rows)

    print("PASS/WARN/FAIL: " + ("PASS" if available else "WARN"))
    print("BTC UNKNOWN TAXONOMY AUDIT (read-only)")
    print(f"Rows read: {total}")
    print(f"Coverage: {available}/{total} = {_pct(available, total):.1f}%")
    med = _median(ages)
    print(f"Median btc_context_age_sec: {'n/a' if med is None else round(med, 1)}")
    print(f"Rows age > 3600 sec: {sum(1 for age in ages if age is not None and age > 3600)}")
    print(f"btc_data_mode counts: {dict(mode_counts)}")
    print(f"btc_mtf_alignment counts: {dict(alignment_counts)}")

    print("\nUnknown reason counts")
    print("reason                         count      pct")
    for reason in UNKNOWN_REASONS:
        count = reason_counts.get(reason, 0)
        print(f"{reason:<30} {count:>5}  {_pct(count, total):>6.1f}%")
    for reason, count in sorted(reason_counts.items()):
        if reason not in UNKNOWN_REASONS:
            print(f"{str(reason):<30} {count:>5}  {_pct(count, total):>6.1f}%")

    print("\nBy source")
    for source, source_rows in sorted(by_source.items()):
        src_total = len(source_rows)
        src_available = sum(1 for item in source_rows if item["ctx"].get("btc_context_available") is True)
        src_reasons = Counter(item["ctx"].get("btc_unknown_reason") for item in source_rows)
        print(
            f"{source}: rows={src_total} coverage={_pct(src_available, src_total):.1f}% "
            f"reasons={dict(src_reasons)}"
        )

    print("\nIntegrity: read-only audit, no orders, no config/state/log writes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
