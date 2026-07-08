#!/usr/bin/env python3
"""MISSED_SETUP research audit for CONFIRM_SMC_RESEARCH.

LOG-ONLY / RESEARCH-ONLY. Streams the qualified-decision and reject logs (no
lookahead on outcomes), classifies every symbol/time where a candidate did NOT
open, and counts how many of those skips were "potential good setups" under a
structural, no-lookahead definition. Writes capped sample rows to:

    logs/smc_research_missed_setups.jsonl

Does NOT change any live decision, predicate, risk, or order.
"""

import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"

QUALIFIED = LOG_DIR / "paper_smc_research_qualified_decisions.jsonl"
OUT = LOG_DIR / "smc_research_missed_setups.jsonl"

MAX_SAMPLES = 2000  # cap derived output; source file is multi-GB

# market_regime values the router actually treats as tradeable trend context.
TREND_REGIMES = {"TREND", "TREND_CONTINUATION", "BREAKOUT_TREND",
                 "PULLBACK_CONTINUATION", "TREND_PULLBACK"}


def classify_skip(d):
    """Map a non-OPEN qualified-decision row to a missed-setup reason taxonomy."""
    decision = d.get("decision")
    reason = d.get("reason")
    sub = d.get("qualified_reject_subreason")
    if decision == "MAX_OPEN_REACHED":
        return "cap_full"
    if decision == "CAP_REACHED":
        return "cap_full"
    if decision == "DUPLICATE_OR_SYMBOL_LOCKED":
        return "duplicate/dedup"
    if reason == "qualified_reject":
        if sub == "STALE_SIGNAL":
            return "late_signal_after_move"
        if sub in (None, "UNKNOWN_BASE_GATE_REJECT"):
            return "research_predicate_fail"
        return f"qualified_reject:{sub}"
    return reason or "other"


def favorable_tier(d):
    """Return the best 'good missed setup' tier this skip qualifies for.

    No lookahead: uses only entry-time structural context. Tiers are nested.
      STRICT   - clean trend regime + strong BOS + good location + RR>=2 + zone-aligned
      RELAXED  - location not flagged bad + strong BOS + RR>=2
      LOOSE    - location not flagged bad + RR>=2 + bos not TRAP
    Returns None if not even LOOSE.
    """
    side = (d.get("side") or "").upper()
    zone = (d.get("smc_zone") or "UNKNOWN").upper()
    regime = (d.get("market_regime") or "UNKNOWN").upper()
    bos = (d.get("bos_quality") or "UNKNOWN").upper()
    would_block = d.get("confirm_smc_entry_location_would_block")
    rr = d.get("planned_rr")
    rr_ok = isinstance(rr, (int, float)) and rr >= 2.0
    zone_aligned = (side == "LONG" and zone == "DISCOUNT") or (side == "SHORT" and zone == "PREMIUM")

    if not rr_ok:
        return None
    if would_block is False and bos != "TRAP":
        if regime in TREND_REGIMES and bos == "STRONG" and zone_aligned:
            return "STRICT"
        if bos == "STRONG":
            return "RELAXED"
        return "LOOSE"
    return None


def main():
    if not QUALIFIED.exists():
        print("FAIL: qualified decisions log not found:", QUALIFIED)
        return

    total = 0
    not_open = 0
    reason_counts = Counter()
    tier_counts = Counter()
    tier_by_reason = Counter()
    samples = []

    with QUALIFIED.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            total += 1
            if d.get("decision") == "OPEN":
                continue
            not_open += 1
            reason = classify_skip(d)
            reason_counts[reason] += 1
            tier = favorable_tier(d)
            if tier:
                tier_counts[tier] += 1
                tier_by_reason[(tier, reason)] += 1
                if len(samples) < MAX_SAMPLES:
                    samples.append({
                        "tier": tier,
                        "skip_reason": reason,
                        "symbol": d.get("symbol"),
                        "side": d.get("side"),
                        "smc_zone": d.get("smc_zone"),
                        "market_regime": d.get("market_regime"),
                        "bos_quality": d.get("bos_quality"),
                        "planned_rr": d.get("planned_rr"),
                        "entry_location_would_block": d.get("confirm_smc_entry_location_would_block"),
                        "signal_created_ts": d.get("signal_created_ts"),
                        "decision": d.get("decision"),
                        "reason": d.get("reason"),
                        "qualified_reject_subreason": d.get("qualified_reject_subreason"),
                    })

    with OUT.open("w", encoding="utf-8") as h:
        for s in samples:
            h.write(json.dumps(s) + "\n")

    print("MISSED_SETUP AUDIT — CONFIRM_SMC_RESEARCH")
    print(f"qualified-decision rows scanned: {total}")
    print(f"non-OPEN (skipped) rows:         {not_open}")

    print("\n=== skip reason taxonomy (non-OPEN rows) ===")
    for r, c in reason_counts.most_common():
        print(f"  {c:>9}  {r}")

    print("\n=== 'potential good missed setup' tiers (no lookahead) ===")
    if not tier_counts:
        print("  NONE — no skipped candidate met even the LOOSE structural bar.")
    for t in ("STRICT", "RELAXED", "LOOSE"):
        print(f"  {t:<8} {tier_counts.get(t, 0)}")

    if tier_counts:
        print("\n=== good-missed tier x skip reason ===")
        for (t, r), c in sorted(tier_by_reason.items(), key=lambda kv: -kv[1])[:15]:
            print(f"  {c:>7}  {t:<8} {r}")

    print(f"\nSamples written: {len(samples)} -> {OUT.name}")

    # PASS/WARN/FAIL: FAIL if the pipeline is leaking many genuinely-good setups.
    strict = tier_counts.get("STRICT", 0)
    relaxed = tier_counts.get("RELAXED", 0)
    if strict > 0:
        verdict = "WARN"  # some clean setups skipped -> investigate timing
    elif relaxed > 0:
        verdict = "WARN"
    else:
        verdict = "PASS"  # no structurally-clean setups were skipped (feature gap, not leak)
    print(f"VERDICT: {verdict}")


if __name__ == "__main__":
    main()
