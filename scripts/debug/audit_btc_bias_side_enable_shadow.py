#!/usr/bin/env python3
"""Read-only audit for BTC_BIAS_SIDE_ENABLE_SHADOW.

AUDIT ONLY. No writes, no orders, no decision changes.

Consumes the forward log logs/btc_bias_side_enable_shadow.jsonl when present.
Because that log only starts populating after the running bot picks up the new
code (a restart we deliberately do NOT perform here), this also falls back to
logs/btc_alignment_instrumentation_shadow.jsonl and recomputes the side-enable
label on the fly with the SAME production evaluator (_btc_bias_side_enable_eval),
so a retrospective read is available immediately.

It:
  - dedups candidate decisions by dedup_key (keep last)
  - joins to realized outcomes (paper_trades.csv + live_trades.csv) by
    symbol+side+entry price within tolerance
  - compares actual vs would_allow / would_block
  - separates BULLISH / BEARISH / NEUTRAL / UNKNOWN
  - reports whether the promotion sample threshold is met (future decision only)
"""

import csv
import json
import os
import sys
from collections import defaultdict

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

FORWARD_LOG = os.path.join(REPO_ROOT, "logs", "btc_bias_side_enable_shadow.jsonl")
FALLBACK_LOG = os.path.join(REPO_ROOT, "logs", "btc_alignment_instrumentation_shadow.jsonl")
ENTRY_TOL = 0.003  # 0.3% relative entry-price match

# Promotion threshold (FUTURE decision only; this script never promotes).
MIN_CLOSED_WITH_CTX = 30
MIN_SHADOW_DECISIONS = 100
MIN_REALIZED_CLOSES = 20


def _fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _load_outcomes():
    """symbol -> list of {side, entry, rr} from realized closed trades."""
    idx = defaultdict(list)
    for fn in ("paper_trades.csv", "live_trades.csv"):
        path = os.path.join(REPO_ROOT, fn)
        if not os.path.exists(path):
            continue
        with open(path) as f:
            for r in csv.DictReader(f):
                if r.get("status") not in ("WIN", "LOSE", "BE"):
                    continue
                e = _fnum(r.get("entry"))
                rr = _fnum(r.get("rr"))
                if e is None or rr is None:
                    continue
                idx[(r.get("symbol"), str(r.get("side") or "").upper())].append(
                    {"entry": e, "rr": rr}
                )
    return idx


def _match_rr(outcomes, symbol, side, entry):
    entry = _fnum(entry)
    if entry is None:
        return None
    best, bd = None, 1e9
    for o in outcomes.get((symbol, str(side or "").upper()), []):
        rel = abs(o["entry"] - entry) / entry if entry else 1e9
        if rel < bd:
            bd, best = rel, o
    if best and bd < ENTRY_TOL:
        return best["rr"]
    return None


def _load_forward():
    rows = {}
    with open(FORWARD_LOG) as f:
        for line in f:
            try:
                d = json.loads(line)
            except Exception:
                continue
            rows[d.get("dedup_key") or id(d)] = d
    return list(rows.values()), "forward"


def _load_fallback():
    """Recompute the side-enable label from the instrumentation log."""
    import signal_dispatcher as sd
    rows = {}
    with open(FALLBACK_LOG) as f:
        for line in f:
            try:
                d = json.loads(line)
            except Exception:
                continue
            label, allow, reason = sd._btc_bias_side_enable_eval(d.get("side"), d)
            d = dict(d)
            d["shadow_label"] = label
            d["shadow_allow"] = allow
            rows[d.get("dedup_key") or id(d)] = d
    return list(rows.values()), "fallback(recomputed)"


def _bias_class(bias):
    b = str(bias or "UNKNOWN").upper()
    if b in ("BULLISH", "BEARISH", "NEUTRAL_OR_CHOP"):
        return b
    return "UNKNOWN"


def _agg(name, rrs):
    n = len(rrs)
    net = sum(rrs)
    g = sum(x for x in rrs if x > 0)
    l = abs(sum(x for x in rrs if x < 0))
    pf = (g / l) if l else (float("inf") if g > 0 else 0.0)
    wr = (len([x for x in rrs if x > 0]) / n) if n else 0.0
    return f"{name:34s} n={n:4d} netR={net:8.2f} PF={pf:5.2f} WR={wr:.0%}"


def main():
    if os.path.exists(FORWARD_LOG) and os.path.getsize(FORWARD_LOG) > 0:
        rows, source = _load_forward()
    elif os.path.exists(FALLBACK_LOG) and os.path.getsize(FALLBACK_LOG) > 0:
        rows, source = _load_fallback()
    else:
        print("VERDICT: LOW_SAMPLE — no forward log and no fallback instrumentation log.")
        print("No side-enable shadow sample available yet. (Forward log populates "
              "after the bot restarts onto the new code; not performed here.)")
        return 0

    print(f"[BTC_BIAS_SIDE_ENABLE_SHADOW AUDIT] source={source} distinct_decisions={len(rows)}")

    # Label + bias-class distribution
    by_label = defaultdict(int)
    by_class = defaultdict(int)
    for d in rows:
        by_label[d.get("shadow_label")] += 1
        by_class[_bias_class(d.get("btc_bias_independent"))] += 1
    print("\n-- shadow_label distribution --")
    for k, v in sorted(by_label.items(), key=lambda x: -x[1]):
        print(f"   {k}: {v} ({v/len(rows):.0%})")
    print("-- bias class distribution --")
    for k, v in sorted(by_class.items(), key=lambda x: -x[1]):
        print(f"   {k}: {v} ({v/len(rows):.0%})")

    # Join to realized outcomes
    outcomes = _load_outcomes()
    kept, blocked_counter, blocked_neutral, unknown = [], [], [], []
    by_side_class = defaultdict(list)
    joined = 0
    for d in rows:
        rr = _match_rr(outcomes, d.get("symbol"), d.get("side"), d.get("entry"))
        if rr is None:
            continue
        joined += 1
        lab = d.get("shadow_label")
        if lab == "BTC_SIDE_ENABLE_ALLOW":
            kept.append(rr)
        elif lab == "BTC_SIDE_ENABLE_BLOCK_COUNTER_BIAS":
            blocked_counter.append(rr)
        elif lab == "BTC_SIDE_ENABLE_BLOCK_NEUTRAL_CHOP":
            blocked_neutral.append(rr)
        else:
            unknown.append(rr)
        by_side_class[(str(d.get("side") or "").upper(),
                       _bias_class(d.get("btc_bias_independent")))].append(rr)

    print(f"\n-- realized-outcome join (n={joined} of {len(rows)}) --")
    print(_agg("KEPT (would-allow)", kept))
    print(_agg("BLOCKED counter-bias", blocked_counter))
    print(_agg("BLOCKED neutral/chop", blocked_neutral))
    print(_agg("UNKNOWN/missing ctx", unknown))
    all_blocked = blocked_counter + blocked_neutral
    print(_agg("BLOCKED (all)", all_blocked))
    print("\n-- side x bias_class (realized) --")
    for k, v in sorted(by_side_class.items()):
        print("   " + _agg(f"{k[0]}/{k[1]}", v))

    # Promotion-readiness (reporting only; never acts)
    print("\n-- promotion readiness (FUTURE decision only; this script never promotes) --")
    closed_with_ctx = len(kept) + len(all_blocked)
    checks = []

    def _pf(rrs):
        g = sum(x for x in rrs if x > 0)
        l = abs(sum(x for x in rrs if x < 0))
        return (g / l) if l else (float("inf") if g > 0 else 0.0)

    sample_ok = (closed_with_ctx >= MIN_CLOSED_WITH_CTX) or (
        len(rows) >= MIN_SHADOW_DECISIONS and joined >= MIN_REALIZED_CLOSES
    )
    checks.append(("sample_threshold", sample_ok,
                   f"closed_with_ctx={closed_with_ctx} (need {MIN_CLOSED_WITH_CTX}) OR "
                   f"decisions={len(rows)}>= {MIN_SHADOW_DECISIONS} & joined={joined}>= {MIN_REALIZED_CLOSES}"))
    kept_pf = _pf(kept)
    checks.append(("kept_pf_improves", len(kept) >= 10 and kept_pf > 1.0,
                   f"kept PF={kept_pf:.2f} (n={len(kept)})"))
    blocked_neg = (sum(all_blocked) < 0) if all_blocked else False
    checks.append(("blocked_set_negative", blocked_neg,
                   f"blocked netR={sum(all_blocked):.2f} (n={len(all_blocked)})"))
    not_block_all = (len(kept) > 0 and len(all_blocked) > 0)
    checks.append(("does_not_block_everything", not_block_all,
                   f"kept={len(kept)} blocked={len(all_blocked)}"))
    # outlier guard: kept edge not driven by a single trade
    outlier_ok = True
    if kept:
        mx = max(kept, key=abs)
        outlier_ok = abs(mx) < 0.6 * abs(sum(kept)) if sum(kept) != 0 else False
    checks.append(("not_outlier_only", outlier_ok,
                   "largest kept trade < 60% of kept netR" if kept else "no kept trades"))

    for name, ok, detail in checks:
        print(f"   [{'PASS' if ok else 'WAIT'}] {name}: {detail}")

    ready = all(ok for _, ok, _ in checks)
    print(f"\nVERDICT: {'READY_FOR_PROMOTION_REVIEW' if ready else 'LOW_SAMPLE (NOT_READY / WAIT_FOR_DATA)'}")
    if source.startswith("fallback"):
        print("NOTE: using recomputed fallback (btc_alignment_instrumentation_shadow) — "
              "forward log not yet populated. Treat as retrospective, not forward-validated.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
