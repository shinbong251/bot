#!/usr/bin/env python3
"""Read-only audit for PAPER CONFIRM_SMC_RESEARCH calibrated gap R."""

import json
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"
LIFECYCLE = LOG_DIR / "paper_smc_research_lifecycle.jsonl"
SHADOW = LOG_DIR / "paper_smc_research_sl_gap_calibration_shadow.jsonl"
MIN_LOCK = LOG_DIR / "paper_smc_research_min_lock_shadow.jsonl"
LIVE_DECISIONS = LOG_DIR / "live_smc_research_decisions.jsonl"

PF_THRESHOLD = 1.10
MIN_SAMPLE = 100


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
        if math.isnan(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def key(row):
    for field in ("research_join_key", "research_dedup_key", "dedup_key"):
        value = str(row.get(field) or "").strip()
        if value:
            return value
    parts = [row.get("symbol"), row.get("side"), row.get("signal_ts"), row.get("trade_id")]
    return "|".join(str(part or "") for part in parts)


def close_rows():
    seen = set()
    out = []
    for row in read_jsonl(LIFECYCLE):
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
            ident = key(candidate) or str(candidate.get("trade_id") or "")
            if ident in seen:
                continue
            seen.add(ident)
            out.append(dict(candidate))
    return out


def shadow_index(path):
    indexed = {}
    for row in read_jsonl(path):
        indexed[key(row)] = row
        if row.get("trade_id") not in (None, ""):
            indexed[str(row.get("trade_id"))] = row
    return indexed


def enrich(rows, gap_shadow, min_lock_shadow):
    enriched = []
    min_lock_keys = {key(row) for row in min_lock_shadow.values() if isinstance(row, dict)}
    for row in rows:
        row = dict(row)
        joined = gap_shadow.get(key(row)) or gap_shadow.get(str(row.get("trade_id") or ""))
        if joined:
            row["_shadow_joined"] = True
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
            row["_shadow_joined"] = False

        raw_r = fnum(row.get("raw_realized_r", row.get("r_multiple")))
        row["raw_realized_r"] = raw_r
        close_reason = str(row.get("close_reason") or "").upper()
        gap_r = fnum(row.get("configured_sl_gap_r", row.get("raw_sl_gap_r")))
        is_gap_loss = bool(row.get("is_gap_loss"))
        if not is_gap_loss:
            is_gap_loss = close_reason == "SL" and gap_r is not None and gap_r > 0 and raw_r is not None and raw_r < -1.0
        if not is_gap_loss and close_reason == "SL" and raw_r is not None:
            is_gap_loss = abs(raw_r + 1.5) < 1e-9 or abs(raw_r + 1.3) < 1e-9
        row["is_gap_loss"] = is_gap_loss
        row["configured_sl_gap_r"] = gap_r
        row["gap_loss_tier"] = row.get("gap_loss_tier") or row.get("execution_tier")
        row["calibrated_r_cap_1_0"] = max(raw_r, -1.0) if is_gap_loss and raw_r is not None and raw_r <= -1.2 else raw_r
        row["calibrated_r_cap_1_2"] = max(raw_r, -1.2) if is_gap_loss and raw_r is not None and raw_r <= -1.2 else raw_r
        row["gap_overcharge_r"] = (
            round(row["calibrated_r_cap_1_0"] - raw_r, 6)
            if raw_r is not None and row["calibrated_r_cap_1_0"] is not None
            else 0.0
        )
        row["segment"] = "post_extension" if row.get("research_is_post_50") is True else "pre_extension"
        if key(row) in min_lock_keys:
            row["since_minlock"] = True
        enriched.append(row)
    return enriched


def pf(values):
    gains = sum(v for v in values if v > 0)
    losses = -sum(v for v in values if v < 0)
    if losses <= 0:
        return None if gains <= 0 else float("inf")
    return gains / losses


def wr(values):
    return None if not values else sum(1 for v in values if v > 0) / len(values)


def metrics(rows):
    rows = [r for r in rows if fnum(r.get("raw_realized_r")) is not None]
    raw = [fnum(r.get("raw_realized_r")) for r in rows]
    cap_1_0 = [fnum(r.get("calibrated_r_cap_1_0")) for r in rows]
    cap_1_2 = [fnum(r.get("calibrated_r_cap_1_2")) for r in rows]
    return {
        "n": len(rows),
        "raw_net_r": round(sum(raw), 4),
        "raw_pf": pf(raw),
        "raw_wr": wr(raw),
        "cap_1_0_net_r": round(sum(cap_1_0), 4),
        "cap_1_0_pf": pf(cap_1_0),
        "cap_1_0_wr": wr(cap_1_0),
        "cap_1_2_net_r": round(sum(cap_1_2), 4),
        "cap_1_2_pf": pf(cap_1_2),
        "cap_1_2_wr": wr(cap_1_2),
        "gap_loss_count": sum(1 for r in rows if r.get("is_gap_loss")),
        "possible_overcharge_count": sum(1 for r in rows if r.get("possible_overcharge")),
        "shadow_join_count": sum(1 for r in rows if r.get("_shadow_joined")),
    }


def fmt(value):
    if value is None:
        return "n/a"
    if value == float("inf"):
        return "inf"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def print_metrics(label, m):
    print(f"\n[{label}]")
    print(f"n={m['n']}")
    print(f"raw net R={fmt(m['raw_net_r'])} PF={fmt(m['raw_pf'])} WR={fmt(m['raw_wr'])}")
    print(f"cap -1.0 net R={fmt(m['cap_1_0_net_r'])} PF={fmt(m['cap_1_0_pf'])} WR={fmt(m['cap_1_0_wr'])}")
    print(f"cap -1.2 net R={fmt(m['cap_1_2_net_r'])} PF={fmt(m['cap_1_2_pf'])} WR={fmt(m['cap_1_2_wr'])}")
    print(
        "gap_loss_count={gap_loss_count} possible_overcharge_count={possible_overcharge_count} "
        "shadow_join_count={shadow_join_count}".format(**m)
    )


def live_micro_opened():
    for row in read_jsonl(LIVE_DECISIONS):
        if (
            str(row.get("entry_type") or "").upper() == "CONFIRM_SMC_RESEARCH"
            and str(row.get("decision") or "").upper() == "OPEN_ACCEPTED"
        ):
            return True
    return False


def promotion_view(m):
    raw_weak = (m["raw_pf"] or 0) < PF_THRESHOLD or m["raw_net_r"] <= 0
    calibrated_ok = (m["cap_1_0_pf"] or 0) >= PF_THRESHOLD and m["cap_1_0_net_r"] > 0
    sample_ok = m["n"] >= MIN_SAMPLE
    live_ok = live_micro_opened()
    if raw_weak and not calibrated_ok:
        verdict = "PROMOTION_BLOCKED"
    elif calibrated_ok and not live_ok:
        verdict = "PROMOTION_CANDIDATE_SHADOW"
    elif calibrated_ok and live_ok:
        verdict = "PROMOTION_CANDIDATE_LIVE_MICRO_ONLY"
    else:
        verdict = "PROMOTION_BLOCKED"
    return verdict, calibrated_ok, sample_ok, live_ok


def main():
    rows = enrich(close_rows(), shadow_index(SHADOW), shadow_index(MIN_LOCK))
    full = metrics(rows)
    print("PASS audit completed read-only")
    print_metrics("FULL DATASET", full)
    print_metrics("PRE-EXTENSION", metrics([r for r in rows if r.get("segment") == "pre_extension"]))
    print_metrics("POST-EXTENSION", metrics([r for r in rows if r.get("segment") == "post_extension"]))
    print_metrics("SINCE MIN-LOCK ACTIVE", metrics([r for r in rows if r.get("since_minlock")]))

    exact_15 = sum(1 for r in rows if fnum(r.get("raw_realized_r")) == -1.5)
    exact_13 = sum(1 for r in rows if fnum(r.get("raw_realized_r")) == -1.3)
    extra_vs_10 = sum(max(0.0, -1.0 - fnum(r.get("raw_realized_r"), 0.0)) for r in rows)
    extra_vs_12 = sum(max(0.0, -1.2 - fnum(r.get("raw_realized_r"), 0.0)) for r in rows)
    print("\n[TOP LOSS DRIVERS]")
    print(f"count exact -1.5R={exact_15}")
    print(f"count exact -1.3R={exact_13}")
    print(f"extra R lost vs -1.0={extra_vs_10:.4f}")
    print(f"extra R lost vs -1.2={extra_vs_12:.4f}")

    verdict, calibrated_ok, sample_ok, live_ok = promotion_view(full)
    print("\n[PROMOTION VIEW]")
    print(f"raw verdict={'WEAK' if (full['raw_pf'] or 0) < PF_THRESHOLD or full['raw_net_r'] <= 0 else 'OK'}")
    print(f"calibrated verdict={'OK' if calibrated_ok else 'WEAK'}")
    print(f"calibrated PF >= {PF_THRESHOLD}={calibrated_ok}")
    print(f"calibrated net R > 0={full['cap_1_0_net_r'] > 0}")
    print(f"sample size sufficient n>={MIN_SAMPLE}={sample_ok}")
    print(f"live micro order path verified separately={live_ok}")
    print(f"promotion_gate={verdict}")
    print("caveat=calibrated is promotion audit layer, not live proof")


if __name__ == "__main__":
    main()
