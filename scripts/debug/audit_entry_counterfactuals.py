#!/usr/bin/env python3
"""ENTRY counterfactual research audit for CONFIRM_SMC_RESEARCH.

LOG-ONLY / RESEARCH-ONLY. Re-uses the joined entry-quality rows produced by
audit_entry_quality_smc_research.py and estimates, for each candidate SHADOW
gate, the realized net-R delta if that gate had filtered executed trades.

IMPORTANT — scope/honesty:
  * These are FILTER counterfactuals: "what if we had NOT taken trades matching
    gate X". They use the trade's own realized R, so they are exact for the
    filter question.
  * Re-entry counterfactuals (enter at first retest / OB-FVG midpoint / after
    sweep) require per-bar candle replay that is NOT available in these logs.
    Those are reported as an instrumentation gap, not fabricated.

Does NOT change any live decision, predicate, risk, or order.
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"
PAPER_EQ = LOG_DIR / "paper_smc_research_entry_quality.jsonl"


def read_rows(path):
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as h:
        for line in h:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def net_r(rows):
    return sum(r["rr"] for r in rows if r.get("rr") is not None)


# Each gate: name -> predicate(row) True == trade WOULD BE FILTERED (skipped).
GATES = {
    "SKIP_LATE_CHASE_EXHAUSTION":
        lambda r: (r.get("market_regime") == "EXHAUSTION_REVERSAL"
                   or r.get("exhaustion_state") in ("EXHAUSTED", "COLLAPSING")),
    "SKIP_CHOP_REGIME":
        lambda r: r.get("market_regime") in ("CHOP_NO_TRADE", "RANGE_MEAN_REVERSION"),
    "SKIP_NO_FOLLOWTHROUGH_BOS":
        lambda r: r.get("bos_quality") in ("NO_FOLLOWTHROUGH", "TRAP"),
    "SKIP_PREMIUM_LONG_DISCOUNT_SHORT":
        lambda r: (r.get("side") == "LONG" and r.get("smc_zone") == "PREMIUM")
                  or (r.get("side") == "SHORT" and r.get("smc_zone") == "DISCOUNT"),
    "SKIP_ENTRY_LOCATION_WOULD_BLOCK":
        lambda r: r.get("entry_location_would_block") is True,
    "SKIP_BAD_RISK_CLASS":
        lambda r: r.get("risk_class") in ("BAD_REGIME_ENTRY", "STALE_SIGNAL_ENTRY",
                                          "NO_FOLLOWTHROUGH_RISK"),
}


def evaluate_gate(rows, predicate):
    skipped = [r for r in rows if predicate(r) and r.get("rr") is not None]
    kept = [r for r in rows if not predicate(r) and r.get("rr") is not None]
    saved_losses = [r for r in skipped if r["rr"] < 0]
    missed_winners = [r for r in skipped if r["rr"] > 0]
    base_net = net_r(rows)
    kept_net = net_r(kept)
    return {
        "affected": len(skipped),
        "net_r_delta": round(kept_net - base_net, 2),  # filtered net minus baseline
        "saved_losses_n": len(saved_losses),
        "saved_losses_r": round(-net_r(saved_losses), 2),
        "missed_winners_n": len(missed_winners),
        "missed_winners_r": round(net_r(missed_winners), 2),
        "kept_n": len(kept),
        "kept_net_r": round(kept_net, 2),
        "kept_win_rate": round(
            sum(1 for r in kept if r["status"] == "WIN") / len(kept), 3) if kept else None,
    }


def recommend(g):
    """Heuristic recommendation per gate from its filter economics."""
    if g["affected"] == 0:
        return "NO_EFFECT"
    delta = g["net_r_delta"]
    # net_r_delta > 0 means filtering improved realized net R.
    if delta > 5 and g["saved_losses_r"] > g["missed_winners_r"]:
        return "SHADOW_GATE_CANDIDATE"
    if delta > 0:
        return "WEAK_POSITIVE_SHADOW"
    return "DO_NOT_FILTER"


def main():
    rows = read_rows(PAPER_EQ)
    if not rows:
        print("FAIL: entry-quality rows not found. Run "
              "audit_entry_quality_smc_research.py first.")
        return

    base_net = round(net_r(rows), 2)
    n = sum(1 for r in rows if r.get("rr") is not None)
    print("ENTRY COUNTERFACTUAL AUDIT — CONFIRM_SMC_RESEARCH (paper)")
    print(f"baseline executed: n={n} net_r={base_net}\n")

    hdr = ("gate", "affected", "net_dR", "saved_L($R)", "missed_W($R)",
           "kept_n", "kept_net", "kept_win", "recommend")
    print("{:<34}{:>9}{:>8}{:>13}{:>14}{:>8}{:>9}{:>9}  {}".format(*hdr))

    results = {}
    for name, pred in GATES.items():
        g = evaluate_gate(rows, pred)
        results[name] = g
        rec = recommend(g)
        print("{:<34}{:>9}{:>8}{:>13}{:>14}{:>8}{:>9}{:>9}  {}".format(
            name, g["affected"], f"{g['net_r_delta']:+.1f}",
            f"{g['saved_losses_n']}({g['saved_losses_r']:.1f})",
            f"{g['missed_winners_n']}({g['missed_winners_r']:.1f})",
            g["kept_n"], f"{g['kept_net_r']:.1f}",
            "-" if g["kept_win_rate"] is None else f"{g['kept_win_rate']:.2f}",
            rec,
        ))

    # Combined (apply the strongest non-overlapping structural filters together).
    combo_pred = lambda r: (GATES["SKIP_NO_FOLLOWTHROUGH_BOS"](r)
                            or GATES["SKIP_LATE_CHASE_EXHAUSTION"](r))
    combo = evaluate_gate(rows, combo_pred)
    print(f"\nCOMBO (no-followthrough OR exhaustion): affected={combo['affected']} "
          f"kept_n={combo['kept_n']} kept_net_r={combo['kept_net_r']} "
          f"net_dR={combo['net_r_delta']:+.1f} kept_win={combo['kept_win_rate']}")

    print("\nRE-ENTRY COUNTERFACTUALS (first retest after BOS / OB-FVG midpoint / "
          "after sweep):")
    print("  NOT COMPUTABLE — requires per-bar candle replay (open/high/low/close "
          "per symbol/timeframe) which is not present in current logs. See "
          "instrumentation gap report.")

    verdict = "PASS"
    if any(recommend(g) == "SHADOW_GATE_CANDIDATE" for g in results.values()):
        verdict = "WARN"  # an executable shadow gate looks beneficial -> investigate
    print(f"\nVERDICT: {verdict}")


if __name__ == "__main__":
    main()
