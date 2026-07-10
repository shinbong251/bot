#!/usr/bin/env python3
"""Reconcile SMC_ENTRY_V2B forward shadow labels from logged inputs.

Read-only audit. Reconstructs v0.1 and v0.2 matches from the JSONL predicate
inputs emitted by runtime shadow logging and exits nonzero on any mismatch.
"""

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOG = ROOT / "logs" / "smc_entry_v2b_allowlist_shadow.jsonl"


def text(value):
    return str(value or "").strip().upper()


def as_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def read_jsonl(path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for lineno, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"WARN invalid_json line={lineno} error={exc}")
                continue
            row["_lineno"] = lineno
            yield row


def regime_match(regime, timing_risk_class):
    regime = text(regime)
    timing_risk_class = text(timing_risk_class)
    return (
        timing_risk_class == "CHOP_OR_RANGE_ENTRY"
        or regime in {
            "CHOP_NO_TRADE",
            "RANGE_ENTRY",
            "RANGE_MEAN_REVERSION",
            "CHOP_OR_RANGE_ENTRY",
        }
    )


def reconstruct(row):
    side = text(row.get("v2b_side") or row.get("side"))
    exhaustion = text(row.get("v2b_exhaustion") or row.get("exhaustion"))
    score_bucket = text(row.get("v2b_score_bucket") or row.get("score_bucket"))
    entry_location = text(row.get("v2b_entry_location") or row.get("entry_location") or row.get("phase"))
    regime = text(row.get("v2b_regime") or row.get("market_regime"))
    timing_risk_class = text(row.get("v2b_timing_risk_class"))
    v01 = side == "SHORT" and exhaustion == "EXTENDED" and score_bucket == "SCORE_2_3"
    v02 = (
        entry_location == "PRE_BREAK_LOW"
        and regime_match(regime, timing_risk_class)
        and score_bucket == "SCORE_2_3"
    )
    return {
        "v0.1_match": v01,
        "v0.2_match": v02,
        "side": side,
        "exhaustion": exhaustion,
        "score_bucket": score_bucket,
        "entry_location": entry_location,
        "regime": regime,
        "timing_risk_class": timing_risk_class,
        "score_source": row.get("v2b_score_source"),
        "entry_location_source": row.get("v2b_entry_location_source"),
        "regime_source": row.get("v2b_regime_source"),
    }


def mismatch_reason(row, recomputed):
    reasons = []
    if as_bool(row.get("v0.1_match")) != recomputed["v0.1_match"]:
        reasons.append("v0.1_match")
    if as_bool(row.get("v0.2_match")) != recomputed["v0.2_match"]:
        reasons.append("v0.2_match")
    if row.get("v2b_recompute_match") is not None and as_bool(row.get("v2b_recompute_match")) is False:
        reasons.append("runtime_recompute_match_false")
    return ",".join(reasons) or "NONE"


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_LOG
    total = 0
    audited = 0
    skipped_legacy = 0
    mismatches = []
    for row in read_jsonl(path) or []:
        total += 1
        if "v0.1_match" not in row or "v0.2_match" not in row:
            skipped_legacy += 1
            continue
        audited += 1
        recomputed = reconstruct(row)
        reason = mismatch_reason(row, recomputed)
        if reason != "NONE":
            mismatches.append({
                "line": row.get("_lineno"),
                "symbol": row.get("symbol"),
                "dedup_key": row.get("dedup_key"),
                "runtime_v0.1_match": row.get("v0.1_match"),
                "runtime_v0.2_match": row.get("v0.2_match"),
                "recomputed_v0.1_match": recomputed["v0.1_match"],
                "recomputed_v0.2_match": recomputed["v0.2_match"],
                "mismatch_reason": reason,
                "provenance": {
                    "v2b_score_source": recomputed["score_source"],
                    "v2b_entry_location_source": recomputed["entry_location_source"],
                    "v2b_regime_source": recomputed["regime_source"],
                    "score_bucket": recomputed["score_bucket"],
                    "entry_location": recomputed["entry_location"],
                    "regime": recomputed["regime"],
                    "timing_risk_class": recomputed["timing_risk_class"],
                    "side": recomputed["side"],
                    "exhaustion": recomputed["exhaustion"],
                },
            })

    for item in mismatches[:25]:
        print("MISMATCH " + json.dumps(item, sort_keys=True, default=str))
    print(
        "SUMMARY "
        f"path={path} total_rows={total} audited_rows={audited} "
        f"skipped_legacy_rows={skipped_legacy} mismatches={len(mismatches)}"
    )
    if mismatches:
        print("VERDICT: FAIL")
        return 1
    print("VERDICT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
