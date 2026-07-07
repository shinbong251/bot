#!/usr/bin/env python3
"""BTC_ALIGNMENT_ENTRY_FILTER audit for CONFIRM_SMC_RESEARCH (READ-ONLY).

Determines whether BTC / market-context alignment can separate good vs bad
CONFIRM_SMC_RESEARCH entries. This is a pure offline audit:

  * It reads existing JSONL/CSV history only.
  * It does NOT change any live/paper decision, risk/cap/A3, SL/MIN_LOCK/
    trailing/order formula.
  * It does NOT place/cancel/modify any live or testnet order.
  * It does NOT write to .env / state / logs / config, and does not commit.

Data sources (entry-time context, captured at open):
  logs/paper_smc_research_btc_m15_bias_shadow.jsonl  (independent-ish BTC bias)
  logs/paper_smc_research_btc_mtf_bias_shadow.jsonl  (BTC MTF summary)
  logs/paper_smc_research_entry_context.jsonl        (market_regime, location gate,
                                                      v2b match, market bias proxy)
  logs/paper_smc_research_lifecycle.jsonl            (RESEARCH_CLOSED realized R)
  live_trades.csv                                    (live CONFIRM_SMC_RESEARCH ref)

Join keys: research_dedup_key (lifecycle <-> btc shadow), dedup_key
(entry_context top-level == research_dedup_key).
"""

import csv
import json
import os
import sys
from collections import Counter, defaultdict

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_DIR = os.path.join(REPO_ROOT, "logs")

BTC_M15_SHADOW = os.path.join(LOG_DIR, "paper_smc_research_btc_m15_bias_shadow.jsonl")
BTC_MTF_SHADOW = os.path.join(LOG_DIR, "paper_smc_research_btc_mtf_bias_shadow.jsonl")
ENTRY_CONTEXT = os.path.join(LOG_DIR, "paper_smc_research_entry_context.jsonl")
LIFECYCLE = os.path.join(LOG_DIR, "paper_smc_research_lifecycle.jsonl")
LIVE_TRADES = os.path.join(REPO_ROOT, "live_trades.csv")
# NEW independent instrumentation (BTC_ALIGNMENT_INSTRUMENTATION shadow, log-only).
BTC_INSTRUMENTATION = os.path.join(LOG_DIR, "btc_alignment_instrumentation_shadow.jsonl")

ENTRY_TYPE = "CONFIRM_SMC_RESEARCH"
IMMEDIATE_MFE_R = 0.5

# New independent BTC fields emitted by _btc_alignment_instrumentation_shadow.
NEW_INDEPENDENT_BTC_FIELDS = [
    "btc_5m_dir", "btc_15m_dir", "btc_1h_dir",
    "btc_5m_change_pct", "btc_15m_change_pct", "btc_1h_change_pct",
    "btc_slope_15m", "btc_bos_state", "btc_structure_state",
    "btc_volatility_state", "btc_vol_spike",
    "btc_near_local_high", "btc_near_local_low",
    "btc_alignment_independent", "btc_bias_independent",
    "btc_context_quality", "btc_context_missing_fields", "btc_context_version",
]
# Old degenerate / sparse fields kept only for contrast.
OLD_DEGENERATE_BTC_FIELDS = [
    "btc_m15_bias_label", "btc_m15_alignment_label", "btc_mtf_summary_label",
    "v2b_market_bias", "v2b_direction_alignment", "trend_direction",
]

# Requested BTC / market-context fields and whether they exist in history.
REQUESTED_BTC_FIELDS = [
    "btc_m15_bias_label",
    "btc_m15_alignment_label",
    "btc_mtf_summary_label",
    "btc_1m_trend",
    "btc_5m_trend",
    "btc_15m_trend",
    "btc_change_pct",
    "btc_slope",
    "btc_bos",
    "btc_structure",
    "btc_near_high",
    "btc_near_low",
    "btc_volatility",
    "btc_vol_spike",
    "btc_impulse",
    "market_regime",
    "market_state",
    "market_bias",
    "trend_state",
    "trend_direction",
    "v2b_market_bias",
    "v2b_direction_alignment",
]

# Promotion standard (task item 8).
MIN_N = 30
MIN_VOLUME = 0.15
MIN_PF_EDGE = 0.20


# --------------------------------------------------------------------------- io
def _read_jsonl(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def _f(value):
    try:
        if value in (None, "", "None"):
            return None
        out = float(value)
        if out != out:
            return None
        return out
    except (TypeError, ValueError):
        return None


def _s(value):
    return str(value).strip().upper() if value not in (None, "") else "UNKNOWN"


# ---------------------------------------------------------------------- loaders
def _load_btc_context():
    """research_dedup_key -> BTC entry-time context."""
    ctx = {}
    for row in _read_jsonl(BTC_M15_SHADOW):
        key = row.get("research_dedup_key") or row.get("research_join_key")
        if not key:
            continue
        ctx[key] = {
            "btc_m15_bias_label": row.get("btc_m15_bias_label") or "UNKNOWN",
            "btc_m15_alignment_label": row.get("btc_m15_alignment_label")
            or "BTC_BIAS_UNKNOWN",
            "btc_m15_source": row.get("btc_m15_source") or "NONE",
            "btc_mtf_summary_label": None,
        }
    for row in _read_jsonl(BTC_MTF_SHADOW):
        key = row.get("research_dedup_key") or row.get("research_join_key")
        if not key or key not in ctx:
            continue
        ctx[key]["btc_mtf_summary_label"] = row.get("btc_mtf_summary_label")
    return ctx


def _load_entry_context():
    """dedup_key (== research_dedup_key) -> flattened entry context."""
    ctx = {}
    for row in _read_jsonl(ENTRY_CONTEXT):
        key = row.get("dedup_key") or row.get("research_dedup_key")
        if not key:
            continue
        nested = row.get("entry_context") if isinstance(row.get("entry_context"), dict) else {}
        merged = {}
        merged.update(row)
        merged.update(nested)
        ctx[key] = merged
    return ctx


def _immediate_sl(mfe_r, status):
    """Immediate-SL marker consistent with existing audits.

    mfe below IMMEDIATE_MFE_R, or an outright SL loss with no follow-through.
    Uses lifecycle mfe_r/status only (no order/formula touched).
    """
    if mfe_r is not None and mfe_r < IMMEDIATE_MFE_R:
        return True
    return _s(status) in {"LOSE", "LOSS", "SL"}


def _correct_side_zone(side, zone):
    if side == "LONG":
        return "CORRECT_SIDE_ZONE" if zone != "PREMIUM" else "BAD_SIDE_ZONE"
    if side == "SHORT":
        return "CORRECT_SIDE_ZONE" if zone != "DISCOUNT" else "BAD_SIDE_ZONE"
    return "SIDE_ZONE_UNKNOWN"


def _btc_dir_from_bias(bias_label):
    """Map BTC bias label to a directional side, or None if not directional."""
    label = _s(bias_label)
    if label == "BULLISH":
        return "LONG"
    if label == "BEARISH":
        return "SHORT"
    return None


def _load_paper_closed(btc_ctx, entry_ctx):
    """Chronological (file/close order) list of closed research trades."""
    out = []
    for row in _read_jsonl(LIFECYCLE):
        if str(row.get("event_type") or "") != "RESEARCH_CLOSED":
            continue
        key = row.get("research_dedup_key")
        r_mult = _f(row.get("r_multiple"))
        if not key or r_mult is None:
            continue
        side = _s(row.get("side"))
        mfe_r = _f(row.get("mfe_r"))
        status = row.get("status")
        bc = btc_ctx.get(key, {})
        ec = entry_ctx.get(key, {})
        bias_label = bc.get("btc_m15_bias_label") or "UNKNOWN"
        align_label = bc.get("btc_m15_alignment_label") or "BTC_BIAS_UNKNOWN"
        btc_dir = _btc_dir_from_bias(bias_label)
        # Recompute alignment ourselves so LONG/SHORT symmetry is explicit.
        if btc_dir is None:
            derived_align = "NEUTRAL" if _s(bias_label) == "NEUTRAL_OR_CHOP" else "UNKNOWN"
        elif side in ("LONG", "SHORT"):
            derived_align = "ALIGNED" if side == btc_dir else "COUNTER"
        else:
            derived_align = "UNKNOWN"
        market_regime = _s(ec.get("market_regime") or ec.get("router_regime"))
        smc_zone = _s(ec.get("smc_zone"))
        out.append({
            "key": key,
            "r": r_mult,
            "side": side,
            "mfe_r": mfe_r,
            "immediate_sl": _immediate_sl(mfe_r, status),
            "btc_shadow_present": key in btc_ctx,
            "btc_source": bc.get("btc_m15_source") or "NO_SHADOW",
            "btc_bias_label": _s(bias_label),
            "btc_align_label": align_label,
            "btc_derived_align": derived_align,
            "btc_mtf_summary": _s(bc.get("btc_mtf_summary_label")),
            "market_regime": market_regime,
            "market_state": _s(ec.get("market_state")),
            "bos_quality": _s(ec.get("bos_quality")),
            "smc_zone": smc_zone,
            "correct_side_zone": _correct_side_zone(side, smc_zone),
            "location_would_block": ec.get("confirm_smc_entry_location_would_block"),
            "v2b_match": ec.get("v2b_match"),
            "v2b_market_bias": _s(ec.get("v2b_market_bias") or ec.get("trend_direction")),
        })
    return out


def _load_live_closed():
    out = []
    if not os.path.exists(LIVE_TRADES):
        return out
    with open(LIVE_TRADES, "r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if _s(row.get("entry_type")) != ENTRY_TYPE:
                continue
            rr = _f(row.get("rr"))
            if rr is None:
                continue
            status = _s(row.get("status"))
            if status == "LOSE":
                rr = -abs(rr)
            elif status == "BE":
                rr = 0.0
            mfe = _f(row.get("max_r"))
            out.append({
                "r": rr,
                "side": _s(row.get("side")),
                "immediate_sl": bool(
                    (mfe is not None and mfe < IMMEDIATE_MFE_R)
                    or (status == "LOSE" and _s(row.get("exit_type")) == "SL")
                ),
            })
    return out


# ------------------------------------------------------------------- statistics
def _stats(rows):
    n = len(rows)
    if n == 0:
        return {"n": 0, "net": 0.0, "avg": None, "wr": None, "pf": None, "imm": None}
    values = [r["r"] for r in rows]
    wins = [v for v in values if v > 1e-9]
    losses = [v for v in values if v < -1e-9]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    pf = gross_win / gross_loss if gross_loss > 1e-9 else (
        float("inf") if gross_win > 0 else None)
    imm = sum(1 for r in rows if r.get("immediate_sl")) / n
    return {
        "n": n,
        "net": sum(values),
        "avg": sum(values) / n,
        "wr": len(wins) / n,
        "pf": pf,
        "imm": imm,
    }


def _fmt(value, digits=2):
    if value is None:
        return "NA"
    if value == float("inf"):
        return "INF"
    return f"{value:.{digits}f}"


def _pct(value):
    return "NA" if value is None else f"{value * 100:.0f}%"


def _line(label, st, width=34):
    return (
        f"{label:<{width}} n={st['n']:>4}  net={_fmt(st['net']):>8}  "
        f"avg={_fmt(st['avg'], 3):>7}  wr={_pct(st['wr']):>4}  "
        f"pf={_fmt(st['pf']):>5}  immSL={_pct(st['imm']):>4}"
    )


def _baseline_block(title, rows):
    print(f"\n[{title}] baseline")
    print("  " + _line("ALL", _stats(rows)))
    for side in ("LONG", "SHORT"):
        print("  " + _line(side, _stats([r for r in rows if r["side"] == side])))


# ---------------------------------------------------------------- filter search
def _evaluate_filter(name, keep_pred, rows, base):
    kept = [r for r in rows if keep_pred(r)]
    blocked = [r for r in rows if not keep_pred(r)]
    ks = _stats(kept)
    bs = _stats(blocked)
    volume = ks["n"] / base["n"] if base["n"] else None
    pf_edge = None if ks["pf"] in (None, float("inf")) or base["pf"] in (None, float("inf")) \
        else ks["pf"] - base["pf"]
    avg_edge = None if ks["avg"] is None or base["avg"] is None else ks["avg"] - base["avg"]
    imm_delta = None if ks["imm"] is None or base["imm"] is None else ks["imm"] - base["imm"]
    # outlier dependence on kept side
    vals = sorted((r["r"] for r in kept if r["r"] > 0), reverse=True)
    outlier = "OK"
    if ks["net"] and ks["net"] > 0 and vals:
        if vals[0] / ks["net"] > 0.60:
            outlier = "TOP1_DEPENDENT"
        elif len(vals) >= 2 and (vals[0] + vals[1]) / ks["net"] > 0.80:
            outlier = "TOP2_DEPENDENT"
    qualifies = (
        (ks["n"] >= MIN_N or (volume is not None and volume >= MIN_VOLUME))
        and pf_edge is not None and pf_edge >= MIN_PF_EDGE
        and avg_edge is not None and avg_edge > 0
        and imm_delta is not None and imm_delta < 0
        and ks["n"] > 0 and bs["n"] > 0
        and outlier == "OK"
    )
    return {
        "name": name,
        "kept_n": ks["n"], "blocked_n": bs["n"],
        "kept_net": ks["net"], "blocked_net": bs["net"],
        "delta_r": ks["net"] - base["net"],
        "kept_pf": ks["pf"], "kept_wr": ks["wr"],
        "kept_imm": ks["imm"], "imm_delta": imm_delta,
        "volume": volume, "pf_edge": pf_edge, "avg_edge": avg_edge,
        "outlier": outlier, "qualifies": qualifies,
    }


def _print_filter_table(title, results):
    print(f"\n{title}")
    print("  filter                              keptN blkN  keptNet blkNet  deltaR  keptPF keptWR immSLΔ  vol   outlier  qual")
    for r in results:
        print(
            f"  {r['name']:<34}{r['kept_n']:>5}{r['blocked_n']:>5} "
            f"{_fmt(r['kept_net']):>8}{_fmt(r['blocked_net']):>7} "
            f"{_fmt(r['delta_r']):>7} {_fmt(r['kept_pf']):>6} "
            f"{_pct(r['kept_wr']):>5} {_pct(r['imm_delta']) if r['imm_delta'] is not None else 'NA':>6} "
            f"{_pct(r['volume']):>5} {r['outlier']:<8} {'YES' if r['qualifies'] else 'no'}"
        )


def _load_btc_instrumentation():
    """dedup_key -> newest independent BTC instrumentation row (per key)."""
    by_key = {}
    rows = _read_jsonl(BTC_INSTRUMENTATION)
    for row in rows:
        key = row.get("dedup_key")
        if not key:
            continue
        prev = by_key.get(key)
        if prev is None or _f(row.get("ts")) is None or _f(row.get("ts")) >= _f(prev.get("ts") or 0):
            by_key[key] = row
    return by_key, rows


def _report_independent_instrumentation(paper):
    print("\n" + "=" * 92)
    print("9) NEW INDEPENDENT BTC INSTRUMENTATION  (btc_alignment_instrumentation_shadow.jsonl)")
    print("=" * 92)
    instr_by_key, instr_rows = _load_btc_instrumentation()
    if not instr_rows:
        print("  STATUS: NEED_DATA — new shadow log absent/empty. It is populated forward")
        print("  by signal_dispatcher._btc_alignment_instrumentation_shadow at each")
        print("  CONFIRM_SMC_RESEARCH decision (paper + live). Re-run after the bot has")
        print("  logged decisions. Fields are LOG-ONLY and never gate.")
        print("  OLD degenerate fields (do NOT use for alignment): "
              f"{OLD_DEGENERATE_BTC_FIELDS}")
        print(f"  NEW independent fields (use once populated): {NEW_INDEPENDENT_BTC_FIELDS}")
        return None

    bias_dist = Counter(_s(r.get("btc_bias_independent")) for r in instr_rows)
    align_dist = Counter(_s(r.get("btc_alignment_independent")) for r in instr_rows)
    quality_dist = Counter(_s(r.get("btc_context_quality")) for r in instr_rows)
    mode_dist = Counter(_s(r.get("execution_mode")) for r in instr_rows)
    print(f"  rows: {len(instr_rows)}  execution_mode: {dict(mode_dist)}")
    print(f"  btc_bias_independent: {dict(bias_dist)}")
    print(f"  btc_alignment_independent: {dict(align_dist)}")
    print(f"  btc_context_quality: {dict(quality_dist)}")
    can_bull = "YES" if bias_dist.get("BULLISH", 0) > 0 else "NOT YET (no BULLISH row observed)"
    print(f"  independent bias can emit BULLISH: {can_bull}")

    # Join to paper outcomes on dedup_key and show independent alignment buckets.
    joined = []
    for r in paper:
        instr = instr_by_key.get(r["key"])
        if not instr:
            continue
        joined.append({
            "r": r["r"], "side": r["side"], "immediate_sl": r["immediate_sl"],
            "align": _s(instr.get("btc_alignment_independent")),
            "bias": _s(instr.get("btc_bias_independent")),
        })
    print(f"\n  paper closed trades joinable to NEW instrumentation: {len(joined)}")
    if joined:
        for b in ("ALIGNED", "COUNTER", "NEUTRAL", "UNKNOWN"):
            print("  " + _line(b, _stats([r for r in joined if r["align"] == b])))
    else:
        print("  (no closed-trade overlap yet — instrumentation is forward-only)")
    return instr_rows


# ------------------------------------------------------------------------- main
def main():
    btc_ctx = _load_btc_context()
    entry_ctx = _load_entry_context()
    paper = _load_paper_closed(btc_ctx, entry_ctx)
    live = _load_live_closed()

    # --- Task 1: field availability -------------------------------------------
    present = set()
    for path in (BTC_M15_SHADOW, BTC_MTF_SHADOW, ENTRY_CONTEXT, BTC_INSTRUMENTATION):
        for row in _read_jsonl(path)[:500]:
            present |= set(row.keys())
            nested = row.get("entry_context")
            if isinstance(nested, dict):
                present |= set(nested.keys())
    field_status = {f: (f in present) for f in REQUESTED_BTC_FIELDS}
    new_field_status = {f: (f in present) for f in NEW_INDEPENDENT_BTC_FIELDS}
    missing = [f for f, ok in field_status.items() if not ok]

    print("=" * 92)
    print("BTC_ALIGNMENT_ENTRY_FILTER AUDIT  (READ-ONLY, entry-time context)")
    print("=" * 92)
    print("\n1) BTC / MARKET CONTEXT FIELD AVAILABILITY")
    for f in REQUESTED_BTC_FIELDS:
        print(f"   {'[y]' if field_status[f] else '[ ]'} {f}")
    print(f"   MISSING FIELDS (not logged anywhere): {missing}")
    src_counts = Counter(
        (btc_ctx.get(k, {}) or {}).get("btc_m15_source") or "NO_SHADOW"
        for k in {r['key'] for r in paper}
    )
    print(f"   btc_m15_source over closed trades: {dict(src_counts)}")
    print("   NOTE: btc_m15_bias_label is sourced from the unified router-shadow cache "
          "(data_mode=UNIFIED_ROUTER_BIAS_NOT_INDEPENDENT_TF); NOT independent per-TF BTC.")
    bias_dist = Counter(r["btc_bias_label"] for r in paper)
    print(f"   btc_m15_bias_label over closed trades: {dict(bias_dist)}")
    if "BULLISH" not in bias_dist:
        print("   *** DEFECT: BTC bias NEVER emits BULLISH -> LONG-vs-BTC alignment "
              "(the main bleeding side) is UNMEASURABLE with current instrumentation.")
    vb = Counter(r["v2b_market_bias"] for r in paper)
    print(f"   v2b_market_bias/trend_direction over closed trades: {dict(vb)} "
          "(DEGENERATE: mirrors trade side -> not an external market direction)")
    print("\n   NEW independent instrumentation field availability "
          "(btc_alignment_instrumentation_shadow.jsonl):")
    for f in NEW_INDEPENDENT_BTC_FIELDS:
        print(f"     {'[y]' if new_field_status[f] else '[ ]'} {f}")

    # --- Tasks 3+4: baselines -------------------------------------------------
    print("\n" + "=" * 92)
    print("3-4) BASELINES  (immSL = mfe_r<0.5 or SL loss)")
    print("=" * 92)
    _baseline_block("PAPER active epoch (all closed)", paper)
    _baseline_block("PAPER last50", paper[-50:])
    _baseline_block("PAPER last100", paper[-100:])
    _baseline_block("LIVE reference (no BTC fields)", live)

    base = _stats(paper)

    # --- Task 5: BTC alignment buckets ---------------------------------------
    print("\n" + "=" * 92)
    print("5) BTC ALIGNMENT / MARKET-CONTEXT BUCKETS")
    print("=" * 92)
    print("\n BTC m15 derived alignment (recomputed side vs btc bias)")
    for b in ("ALIGNED", "COUNTER", "NEUTRAL", "UNKNOWN"):
        print("  " + _line(b, _stats([r for r in paper if r["btc_derived_align"] == b])))
    print("\n BTC m15 bias_label")
    for b in ("BULLISH", "BEARISH", "NEUTRAL_OR_CHOP", "UNKNOWN"):
        print("  " + _line(b, _stats([r for r in paper if r["btc_bias_label"] == b])))
    print("\n BTC MTF summary")
    for b in sorted({r["btc_mtf_summary"] for r in paper}):
        print("  " + _line(b, _stats([r for r in paper if r["btc_mtf_summary"] == b])))
    print("\n market_regime (symbol-local, not BTC)")
    for b in sorted({r["market_regime"] for r in paper}):
        print("  " + _line(b, _stats([r for r in paper if r["market_regime"] == b])))
    print("\n NOT AVAILABLE (no field logged): BTC strong-vs-weak trend, BTC impulse "
          "with/against, BTC near local high/low, BTC volatility spike.")

    # --- Task 6: cross buckets -----------------------------------------------
    print("\n" + "=" * 92)
    print("6) CROSS BUCKETS")
    print("=" * 92)
    cross = [
        ("LONG + BTC bearish/counter", lambda r: r["side"] == "LONG" and r["btc_bias_label"] == "BEARISH"),
        ("LONG + BTC neutral/chop", lambda r: r["side"] == "LONG" and r["btc_bias_label"] == "NEUTRAL_OR_CHOP"),
        ("LONG + BTC bullish/aligned", lambda r: r["side"] == "LONG" and r["btc_bias_label"] == "BULLISH"),
        ("SHORT + BTC bearish/aligned", lambda r: r["side"] == "SHORT" and r["btc_bias_label"] == "BEARISH"),
        ("SHORT + BTC bullish/counter", lambda r: r["side"] == "SHORT" and r["btc_bias_label"] == "BULLISH"),
        ("locationGate KEEP + BTC aligned", lambda r: r["location_would_block"] is not True and r["btc_derived_align"] == "ALIGNED"),
        ("locationGate KEEP + BTC counter", lambda r: r["location_would_block"] is not True and r["btc_derived_align"] == "COUNTER"),
        ("V2B allowlist match + BTC aligned", lambda r: r["v2b_match"] is True and r["btc_derived_align"] == "ALIGNED"),
        ("V2B allowlist match + BTC counter", lambda r: r["v2b_match"] is True and r["btc_derived_align"] == "COUNTER"),
        ("immediate_SL cluster + BTC bearish", lambda r: r["immediate_sl"] and r["btc_bias_label"] == "BEARISH"),
        ("immediate_SL cluster + BTC neutral", lambda r: r["immediate_sl"] and r["btc_bias_label"] == "NEUTRAL_OR_CHOP"),
        ("immediate_SL cluster + BTC unknown", lambda r: r["immediate_sl"] and r["btc_bias_label"] == "UNKNOWN"),
    ]
    for label, pred in cross:
        print("  " + _line(label, _stats([r for r in paper if pred(r)]), width=36))

    # --- Task 7+8: candidate filter evaluation -------------------------------
    print("\n" + "=" * 92)
    print("7-8) CANDIDATE FILTER EVALUATION  (keep = pass, rest = blocked)")
    print(f"   promotion standard: keptN>={MIN_N} OR vol>={MIN_VOLUME:.0%}; pf_edge>={MIN_PF_EDGE}; "
          "avg improves; immSL drops; not delete-all; not outlier-only.")
    candidates = [
        ("keep BTC not-counter", lambda r: r["btc_derived_align"] != "COUNTER"),
        ("keep BTC aligned-only", lambda r: r["btc_derived_align"] == "ALIGNED"),
        ("block BTC counter-only", lambda r: r["btc_derived_align"] != "COUNTER"),
        ("keep BTC known-only", lambda r: r["btc_bias_label"] not in ("UNKNOWN",)),
        ("block LONG+BTC bearish", lambda r: not (r["side"] == "LONG" and r["btc_bias_label"] == "BEARISH")),
        ("keep regime RANGE_MEAN_REV", lambda r: r["market_regime"] == "RANGE_MEAN_REVERSION"),
        ("block regime CHOP_NO_TRADE", lambda r: r["market_regime"] != "CHOP_NO_TRADE"),
    ]
    results = [_evaluate_filter(name, pred, paper, base) for name, pred in candidates]
    _print_filter_table("candidate filters (paper active epoch)", results)

    # --- Task 9: NEW independent instrumentation ------------------------------
    _report_independent_instrumentation(paper)

    # --- Tasks 10: ranking + recommendation ----------------------------------
    print("\n" + "=" * 92)
    print("10) SUMMARY")
    print("=" * 92)

    def _bucket_rows():
        out = []
        dims = [
            ("btc_derived_align", "btcAlign"),
            ("btc_bias_label", "btcBias"),
            ("market_regime", "regime"),
        ]
        for field, tag in dims:
            for value in {r[field] for r in paper}:
                sub = [r for r in paper if r[field] == value]
                st = _stats(sub)
                if st["n"] >= 10:
                    out.append((f"{tag}={value}", st))
        return out

    ranked = sorted(_bucket_rows(), key=lambda kv: kv[1]["avg"])
    print("\nTop 5 WORST BTC/context buckets (n>=10, by avg R):")
    for label, st in ranked[:5]:
        print("  " + _line(label, st, width=26))
    print("\nTop 5 BEST BTC/context buckets (n>=10, by avg R):")
    for label, st in list(reversed(ranked))[:5]:
        print("  " + _line(label, st, width=26))

    qualifying = [r for r in results if r["qualifies"]]
    best = None
    scored = [r for r in results if r["kept_n"] > 0 and r["blocked_n"] > 0]
    if qualifying:
        best = sorted(qualifying, key=lambda r: (r["pf_edge"] or -9, r["kept_n"]), reverse=True)[0]
    elif scored:
        best = sorted(scored, key=lambda r: (r["kept_pf"] or -9), reverse=True)[0]

    known_directional = [r for r in paper if r["btc_derived_align"] in ("ALIGNED", "COUNTER")]
    counter_n = sum(1 for r in paper if r["btc_derived_align"] == "COUNTER")
    aligned_n = sum(1 for r in paper if r["btc_derived_align"] == "ALIGNED")
    unknown_share = sum(1 for r in paper if r["btc_derived_align"] == "UNKNOWN") / max(1, len(paper))
    btc_broken = "BULLISH" not in bias_dist

    if best and best["qualifies"]:
        verdict, rec = "PASS", "PROMOTE_BTC_ALIGNMENT_SHADOW"
        edge = "YES"
    elif btc_broken or len(known_directional) < MIN_N or counter_n < 20:
        verdict, rec = "WARN", "NEED_BTC_ALIGNMENT_INSTRUMENTATION"
        edge = "UNDETERMINED (BTC feed too sparse/degenerate to test)"
    else:
        verdict, rec = "FAIL", "KILL_BTC_ALIGNMENT_IDEA"
        edge = "NO"

    print(f"\nPASS/WARN/FAIL: {verdict}")
    print(f"BTC alignment has edge: {edge}")
    print(f"directional sample: ALIGNED={aligned_n} COUNTER={counter_n} "
          f"(need COUNTER>=20 and directional>={MIN_N}); UNKNOWN share={unknown_share:.0%}")
    print(f"BTC feed emits BULLISH: {'NO (broken -> LONG side untestable)' if btc_broken else 'yes'}")
    if best:
        print(f"best candidate filter: {best['name']} | keptN={best['kept_n']} "
              f"keptNet={_fmt(best['kept_net'])} keptPF={_fmt(best['kept_pf'])} "
              f"vol={_pct(best['volume'])} pf_edge={_fmt(best['pf_edge'])} "
              f"outlier={best['outlier']} qualifies={best['qualifies']}")
    else:
        print("best candidate filter: NONE")
    instr_rows = _read_jsonl(BTC_INSTRUMENTATION)
    instr_deployed = any(f in present for f in NEW_INDEPENDENT_BTC_FIELDS)
    print(f"\nRECOMMENDATION: {rec}")
    if rec == "NEED_BTC_ALIGNMENT_INSTRUMENTATION":
        if instr_deployed and instr_rows:
            print("  STATUS: independent instrumentation is DEPLOYED and populating forward")
            print(f"    ({len(instr_rows)} rows in btc_alignment_instrumentation_shadow.jsonl).")
            print("    It is LOG-ONLY and never gates. Next: accumulate closed-trade sample,")
            print("    then re-audit section 9 for a real (non-degenerate) alignment edge.")
        else:
            print("  STATUS: independent instrumentation is WIRED (shadow/log-only) but the")
            print("    forward log is still empty — it fills as CONFIRM_SMC_RESEARCH decisions")
            print("    are made (paper + live). No gate. Re-audit once rows accumulate.")
        print("  Independent fields now emitted: btc_5m_dir/btc_15m_dir/btc_1h_dir,")
        print("    btc_*_change_pct, btc_slope_15m, btc_bos_state, btc_structure_state,")
        print("    btc_volatility_state, btc_vol_spike, btc_near_local_high/low,")
        print("    btc_bias_independent (CAN emit BULLISH), btc_alignment_independent.")

    print("\nINTEGRITY: read-only audit; no live/testnet orders placed/cancelled/modified; "
          "no .env/state/logs/config written; no commit/push.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
