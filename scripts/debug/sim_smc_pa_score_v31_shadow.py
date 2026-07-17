#!/usr/bin/env python3
"""Simulator: SMC_PA_SCORE_V3_1_SHADOW is shadow/log-only, freeze-correct
and behavior-isolated.

No Binance calls, no orders, no production log/state writes: every store
and log path used here lives in a temp directory. Verifies:

  01 stable positive score (complete inputs -> V31_POSITIVE, frozen 1st obs)
  02 stable negative score -> V31_NEGATIVE
  03 neutral score -> V31_NEUTRAL
  04 low coverage -> V31_LOW_COVERAGE, research_eligible False
  05 first incomplete row does not freeze
  06 first complete row freezes (FIRST_COMPLETE_OBSERVATION)
  07 later contradictory rescan cannot change frozen score/band/candidate
  08 fallback timeout freezes a low-coverage snapshot as ineligible
  09 restart reload preserves the frozen label
  10 duplicate lifecycle actions share v31_signal_id + rising obs_index
  11 identity collisions: distinct ids stay distinct; blank id never freezes
  12 missing breakout context -> BREAK_CONTEXT_MISSING, counted missing
  13 breakout categories: ACCEPTED/+1, WEAK/-1, REJECTED(wick)/-2
  14 RS aligned/opposed/neutral/missing/stale categories
  15 LONG/SHORT RS direction handling (sign flips with side)
  16 RS/BTC-M15 disagreement never alters v31_score (observational only)
  17 malformed numerics never raise and degrade to missing
  18 writer failure is swallowed; log_v31_shadow still returns a dict
  19 state corruption loads empty without raising
  20 pruning/cap behavior (TTL prune + hard entry cap)
  21 no outcome/future fields in rows (writer also strips forbidden keys)
  22 old V3 evaluator unchanged (version + fixed-input score + no v31 refs)
  23 dispatcher wiring: return ignored, try/except-pass wrapped, after
     decision write paths; no gate consumes v31 fields
  24 rotation target present exactly once in log_rotation.ROTATION_TARGETS

--stress adds the memory benchmark:
  100,000 evaluator calls, 100,000 freeze-store lookups/updates,
  10,000 temp log writes; reports baseline/peak/final RSS, elapsed,
  state cardinality; asserts bounded growth and no production artifacts.
"""

import copy
import json
import os
import re
import sys
import tempfile
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import log_rotation
import signal_dispatcher as sd
import smc_pa_score_v31_shadow as v31

PROD_LOG = os.path.join(REPO_ROOT, v31.V31_LOG_PATH)
PROD_STATE = os.path.join(REPO_ROOT, v31.V31_STATE_PATH)
_PROD_LOG_STAT = os.stat(PROD_LOG) if os.path.exists(PROD_LOG) else None
_PROD_STATE_STAT = os.stat(PROD_STATE) if os.path.exists(PROD_STATE) else None

TMP = tempfile.mkdtemp(prefix="smc_pa_v31_sim_")
os.chdir(TMP)  # any accidental default-path write lands here, not in the repo

RESULTS = []
NOW = 1780000000.0


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok), detail))
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name}" + (f" | {detail}" if detail else ""))


def tmp_paths(tag):
    return (
        os.path.join(TMP, f"state_{tag}.json"),
        os.path.join(TMP, f"log_{tag}.jsonl"),
    )


def full_btc_ctx(**extra):
    base = {
        "btc_bias_independent": "BULLISH",
        "btc_context_quality": "OK",
        "btc_alignment_independent": "ALIGNED",
        "btc_mtf_alignment": "ALL_ALIGNED",
        "btc_data_mode": "INDEPENDENT_BTC_MTF",
        "btc_5m_change_pct": 0.10,
        "btc_15m_change_pct": 0.20,
        "btc_context_source_ts": NOW - 30.0,
        "btc_context_age_sec": 30.0,
    }
    base.update(extra)
    return base


def full_candidate(**extra):
    base = {
        "symbol": "SIMUSDT",
        "side": "LONG",
        "entry_type": "CONFIRM",
        "dedup_key": "SIMUSDT|LONG|CONFIRM|1780000000",
        "signal_created_ts": NOW - 10.0,
        "source_timestamp": NOW - 10.0,
        "entry": 100.0,
        "sl": 98.0,
        "tp": 104.0,
        "rr": 2.0,
        "planned_rr": 2.0,
        "smc_zone": "DISCOUNT",
        "market_regime": "RANGE_MEAN_REVERSION",
        "bos_quality": "CONFIRMED",
        "liquidity_sweep": "SWEEP_LOW",
        "trend_direction": "LONG",
        "atr": 1.5,
        "nearest_htf_resistance": 106.0,
        "nearest_htf_support": 95.0,
        "confirm_entry_acceptance_context": {
            "candle_open": 99.0,
            "candle_high": 100.6,
            "candle_low": 98.8,
            "candle_close": 100.4,
            "break_level": 100.0,
            "atr": 1.5,
            "alt_m15_change_3bar_pct": 0.50,
            "alt_m15_change_source_ts": NOW - 10.0,
        },
    }
    base.update(extra)
    return base


def run_shadow(candidate, tag, btc_ctx=None, store=None, log_path=None,
               now_ts=NOW, action="OPEN_ACCEPTED", **kw):
    state_path, default_log = tmp_paths(tag)
    if store is None:
        store = v31.V31FreezeStore(path=state_path)
    if log_path is None:
        log_path = default_log
    out = v31.log_v31_shadow(
        candidate,
        execution_mode="paper",
        action=action,
        reason="SIM",
        btc_ctx=full_btc_ctx() if btc_ctx is None else btc_ctx,
        now_ts=now_ts,
        store=store,
        log_path=log_path,
        **kw,
    )
    rows = []
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as handle:
            rows = [json.loads(line) for line in handle if line.strip()]
    return out, rows, store, log_path


def jsonl_rows(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


# ── 01 stable positive ────────────────────────────────────────────────────
out, rows, store01, _ = run_shadow(full_candidate(), "01")
row = rows[-1]
check(
    "01 stable positive score",
    row["v31_score"] == 11
    and row["v31_band"] == "V31_POSITIVE"
    and row["v31_frozen"] is True
    and row["v31_candidate"] is True
    and row["v31_research_eligible"] is True
    and row["v31_available_count"] == 8,
    f"score={row['v31_score']} band={row['v31_band']}",
)

# ── 02 stable negative ────────────────────────────────────────────────────
neg = full_candidate(
    dedup_key="SIMUSDT|LONG|CONFIRM|neg",
    smc_zone="PREMIUM",
    market_regime="CHOP_NO_TRADE",
    bos_quality="NO_FOLLOWTHROUGH",
    liquidity_sweep="SWEEP_HIGH",
    sl=99.5,          # sl_dist 0.5 < atr -> -2
    planned_rr=15.0,  # beyond opposing (opposing_r=12.0) -> -2
)
neg["confirm_entry_acceptance_context"] = dict(
    neg["confirm_entry_acceptance_context"],
    candle_close=99.9, candle_high=100.6,  # wick beyond, close back inside -> -2
)
out, rows, _, _ = run_shadow(neg, "02", btc_ctx=full_btc_ctx(btc_bias_independent="BEARISH"))
row = rows[-1]
check(
    "02 stable negative score",
    row["v31_score"] == -14 and row["v31_band"] == "V31_NEGATIVE"
    and row["v31_candidate"] is False,
    f"score={row['v31_score']} band={row['v31_band']}",
)

# ── 03 neutral ────────────────────────────────────────────────────────────
neu = full_candidate(
    dedup_key="SIMUSDT|LONG|CONFIRM|neu",
    smc_zone="EQ",
    market_regime="SOME_REGIME",
    bos_quality="WEAK",
    liquidity_sweep="NONE",
    planned_rr=9.0,   # -2
)
out, rows, _, _ = run_shadow(neu, "03", btc_ctx=full_btc_ctx(btc_bias_independent="NEUTRAL_OR_CHOP"))
row = rows[-1]
check(
    "03 neutral score",
    row["v31_score"] == -1 and row["v31_band"] == "V31_NEUTRAL",
    f"score={row['v31_score']} band={row['v31_band']}",
)

# ── 04 low coverage ───────────────────────────────────────────────────────
low = {
    "symbol": "SIMUSDT",
    "side": "LONG",
    "entry_type": "CONFIRM",
    "dedup_key": "SIMUSDT|LONG|CONFIRM|low",
    "signal_created_ts": NOW - 10.0,
}
out, rows, store04, log04 = run_shadow(low, "04", btc_ctx={})
row = rows[-1]
check(
    "04 low coverage explicit band",
    row["v31_band"] == "V31_LOW_COVERAGE"
    and row["v31_research_eligible"] is False
    and row["v31_candidate"] is False
    and row["v31_available_count"] < v31.V31_MIN_AVAILABLE_FOR_ELIGIBLE,
    f"available={row['v31_available_count']} band={row['v31_band']}",
)

# ── 05 first incomplete row does not freeze ───────────────────────────────
check(
    "05 first incomplete row does not freeze",
    row["v31_frozen"] is False and row["v31_freeze_ts"] is None,
    f"frozen={row['v31_frozen']}",
)

# ── 06 first complete row freezes ─────────────────────────────────────────
rec = store01.get("SIMUSDT|LONG|CONFIRM|1780000000")
check(
    "06 first complete row freezes",
    rec["frozen"] is True and rec["freeze_reason"] == "first_complete_observation"
    and rec["freeze_ts"] == NOW,
    f"reason={rec['freeze_reason']}",
)

# ── 07 later contradictory rescan cannot change frozen label ──────────────
contra = full_candidate(
    smc_zone="PREMIUM",
    market_regime="CHOP_NO_TRADE",
    bos_quality="NO_FOLLOWTHROUGH",
    liquidity_sweep="SWEEP_HIGH",
)
out, rows, _, _ = run_shadow(
    contra, "01", store=store01, log_path=tmp_paths("01")[1],
    now_ts=NOW + 60, action="REJECT",
)
row = rows[-1]
check(
    "07 contradictory rescan cannot change frozen score/band",
    row["v31_frozen"] is True
    and row["v31_score"] == 11
    and row["v31_band"] == "V31_POSITIVE"
    and row["v31_candidate"] is True
    and row["v31_obs_score"] < 11,
    f"frozen_score={row['v31_score']} obs_score={row['v31_obs_score']}",
)

# ── 08 fallback timeout freezes low-coverage snapshot as ineligible ──────
out, rows, store04, _ = run_shadow(
    low, "04", btc_ctx={}, store=store04, log_path=log04,
    now_ts=NOW + v31.V31_FREEZE_FALLBACK_SECS + 1,
)
row = rows[-1]
check(
    "08 fallback timeout freeze (ineligible)",
    row["v31_frozen"] is True
    and row["v31_research_eligible"] is False
    and row["v31_freeze_reason"] == "fallback_timeout_low_coverage"
    and row["v31_candidate"] is False,
    f"reason={row['v31_freeze_reason']}",
)

# ── 09 restart reload preserves frozen label ──────────────────────────────
store01.save(now_ts=NOW + 120, force=True)
reloaded = v31.V31FreezeStore(path=store01.path)
rec2 = reloaded.get("SIMUSDT|LONG|CONFIRM|1780000000")
check(
    "09 restart reload preserves frozen label",
    rec2 is not None and rec2["frozen"] is True and rec2["score"] == 11
    and rec2["band"] == "V31_POSITIVE" and rec2["candidate"] is True,
    f"reloaded={rec2 and rec2['band']}",
)

# ── 10 duplicate lifecycle actions share id + rising obs_index ────────────
state10, log10 = tmp_paths("10")
store10 = v31.V31FreezeStore(path=state10)
ids, indices = [], []
for i, action in enumerate(("REJECT", "OPEN_ATTEMPT", "OPEN_ACCEPTED")):
    out, rows, _, _ = run_shadow(
        full_candidate(), "10", store=store10, log_path=log10,
        now_ts=NOW + i, action=action,
    )
    ids.append(rows[-1]["v31_signal_id"])
    indices.append(rows[-1]["v31_obs_index"])
check(
    "10 duplicate lifecycle rows share v31_signal_id",
    len(set(ids)) == 1 and indices == [1, 2, 3],
    f"ids={set(ids)} obs={indices}",
)

# ── 11 identity collision checks ──────────────────────────────────────────
state11, log11 = tmp_paths("11")
store11 = v31.V31FreezeStore(path=state11)
run_shadow(full_candidate(dedup_key="A|LONG|CONFIRM|1"), "11", store=store11, log_path=log11)
run_shadow(full_candidate(dedup_key="B|SHORT|CONFIRM|2", symbol="B", side="SHORT",
                          smc_zone="PREMIUM", liquidity_sweep="SWEEP_HIGH",
                          trend_direction="SHORT"),
           "11", store=store11, log_path=log11)
blank = full_candidate(dedup_key="")
out_blank, rows11, _, _ = run_shadow(blank, "11", store=store11, log_path=log11)
row_blank = rows11[-1]
check(
    "11 identity: distinct ids distinct records; blank id never freezes",
    len(store11) == 2
    and store11.get("A|LONG|CONFIRM|1")["frozen"]
    and store11.get("B|SHORT|CONFIRM|2")["frozen"]
    and row_blank["v31_signal_id"] is None
    and row_blank["v31_frozen"] is False,
    f"store_len={len(store11)}",
)

# ── 12 missing breakout context ───────────────────────────────────────────
nb = full_candidate(dedup_key="SIMUSDT|LONG|CONFIRM|nb")
nb["confirm_entry_acceptance_context"] = {}
out, rows, _, _ = run_shadow(nb, "12")
row = rows[-1]
check(
    "12 missing breakout context",
    row["v31_breakout_category"] == "BREAK_CONTEXT_MISSING"
    and row["v31_breakout_points"] == 0
    and "breakout_acceptance" in row["v31_missing_components"],
    f"cat={row['v31_breakout_category']}",
)

# ── 13 breakout categories ────────────────────────────────────────────────
def breakout_of(close, high, low):
    cand = full_candidate(dedup_key=f"SIMUSDT|LONG|CONFIRM|bk{close}")
    cand["confirm_entry_acceptance_context"] = dict(
        cand["confirm_entry_acceptance_context"],
        candle_close=close, candle_high=high, candle_low=low,
    )
    _, rows_, _, _ = run_shadow(cand, f"13_{close}")
    return rows_[-1]

r_acc = breakout_of(100.4, 100.6, 98.8)   # close beyond 100
r_weak = breakout_of(99.5, 99.9, 98.8)    # no wick break, close not beyond
r_rej = breakout_of(99.9, 100.6, 98.8)    # wick beyond, close back inside
check(
    "13 breakout ACCEPTED/WEAK/REJECTED categories + points",
    r_acc["v31_breakout_category"] == "BREAK_ACCEPTED" and r_acc["v31_breakout_points"] == 1
    and r_weak["v31_breakout_category"] == "BREAK_WEAK" and r_weak["v31_breakout_points"] == -1
    and r_rej["v31_breakout_category"] == "BREAK_REJECTED" and r_rej["v31_breakout_points"] == -2,
    f"{r_acc['v31_breakout_category']}/{r_weak['v31_breakout_category']}/{r_rej['v31_breakout_category']}",
)

# ── 14 RS categories ──────────────────────────────────────────────────────
def rs_of(tag, alt=0.5, btc=0.2, age=30.0, side="LONG", drop_alt=False, drop_btc=False):
    cand = full_candidate(dedup_key=f"SIM|{side}|CONFIRM|rs{tag}", side=side)
    if side == "SHORT":
        cand.update(smc_zone="PREMIUM", liquidity_sweep="SWEEP_HIGH", trend_direction="SHORT")
    ctx = dict(cand["confirm_entry_acceptance_context"], alt_m15_change_3bar_pct=alt)
    if drop_alt:
        ctx["alt_m15_change_3bar_pct"] = None
    cand["confirm_entry_acceptance_context"] = ctx
    bctx = full_btc_ctx(btc_15m_change_pct=None if drop_btc else btc, btc_context_age_sec=age)
    _, rows_, _, _ = run_shadow(cand, f"14_{tag}", btc_ctx=bctx)
    return rows_[-1]

r_al = rs_of("al", alt=0.5, btc=0.2)
r_op = rs_of("op", alt=0.1, btc=0.2)
r_ne = rs_of("ne", alt=0.2, btc=0.2)
r_mi = rs_of("mi", drop_alt=True)
r_st = rs_of("st", age=v31.V31_RS_MAX_BTC_AGE_SECS + 1)
check(
    "14 RS aligned/opposed/neutral/missing/stale",
    r_al["v31_rs_category"] == "RS_ALIGNED"
    and r_op["v31_rs_category"] == "RS_OPPOSED"
    and r_ne["v31_rs_category"] == "RS_NEUTRAL"
    and r_mi["v31_rs_category"] == "RS_MISSING"
    and r_st["v31_rs_category"] == "RS_STALE",
    "/".join(r["v31_rs_category"] for r in (r_al, r_op, r_ne, r_mi, r_st)),
)

# ── 15 LONG/SHORT RS direction ────────────────────────────────────────────
r_long = rs_of("dirL", alt=0.5, btc=0.2, side="LONG")
r_short = rs_of("dirS", alt=0.5, btc=0.2, side="SHORT")
check(
    "15 LONG/SHORT RS direction handling",
    r_long["v31_rs_m15_raw"] == 0.3 and r_long["v31_rs_category"] == "RS_ALIGNED"
    and r_short["v31_rs_m15_raw"] == -0.3 and r_short["v31_rs_category"] == "RS_OPPOSED",
    f"long={r_long['v31_rs_m15_raw']} short={r_short['v31_rs_m15_raw']}",
)

# ── 16 RS/BTC-M15 disagreement never alters score ─────────────────────────
check(
    "16 RS observational: category flip never alters v31_score",
    r_al["v31_score"] == r_op["v31_score"] == r_mi["v31_score"] == r_st["v31_score"]
    and r_al["v31_rs_scored"] is False and r_al["v31_rs_points"] == 0,
    f"scores={[r['v31_score'] for r in (r_al, r_op, r_mi, r_st)]}",
)

# ── 17 malformed numerics ─────────────────────────────────────────────────
bad = full_candidate(
    dedup_key="SIMUSDT|LONG|CONFIRM|bad",
    entry="abc", sl=float("nan"), tp=None, planned_rr="", atr="x",
)
bad["confirm_entry_acceptance_context"] = {
    "candle_close": "oops", "break_level": float("nan"),
    "alt_m15_change_3bar_pct": "z", "alt_m15_change_source_ts": {},
}
out, rows, _, _ = run_shadow(bad, "17")
row = rows[-1]
check(
    "17 malformed numerics degrade to missing, no crash",
    isinstance(out, dict)
    and row["v31_band"] == "V31_LOW_COVERAGE"
    and "volatility_sl_quality" in row["v31_missing_components"]
    and row["v31_breakout_category"] == "BREAK_CONTEXT_MISSING"
    and row["v31_rs_category"] == "RS_MISSING",
    f"missing={row['v31_missing_components']}",
)

# ── 18 writer failure swallowed ───────────────────────────────────────────
state18, _ = tmp_paths("18")
out, _, _, _ = run_shadow(
    full_candidate(), "18",
    store=v31.V31FreezeStore(path=state18),
    log_path=os.path.join(TMP, "no_such_dir_file", "x", "log.jsonl"),
)
bad_dir = os.path.join(TMP, "no_such_dir_file")
os.path.isdir(bad_dir)  # makedirs may create it; failure path is open() on dir-as-file
out2 = v31.append_v31_shadow_row({"a": 1}, log_path=TMP)  # path IS a directory -> open fails
check(
    "18 writer failure swallowed, dict still returned",
    isinstance(out, dict) and out2 is False,
    f"append_to_dir={out2}",
)

# ── 19 state corruption loads empty ───────────────────────────────────────
state19, _ = tmp_paths("19")
with open(state19, "w", encoding="utf-8") as handle:
    handle.write("{not json !!!")
s19 = v31.V31FreezeStore(path=state19)
with open(state19, "w", encoding="utf-8") as handle:
    handle.write(json.dumps({"schema_version": 999, "signals": {"x": {"frozen": True}}}))
s19b = v31.V31FreezeStore(path=state19)
check(
    "19 malformed/wrong-schema state loads empty without raising",
    len(s19) == 0 and len(s19b) == 0,
)

# ── 20 pruning/cap behavior ───────────────────────────────────────────────
state20, log20 = tmp_paths("20")
s20 = v31.V31FreezeStore(path=state20)
s20.observe("old_id", {"research_eligible": True, "score": 1, "band": "V31_POSITIVE",
                       "missing_count": 0, "available_count": 8,
                       "coverage_ratio": 1.0, "missing_components": []},
            now_ts=NOW - v31.V31_STATE_TTL_SECS - 100)
pruned = s20.prune(now_ts=NOW)
cap_probe = v31.V31_STATE_MAX_ENTRIES
saved_cap, v31.V31_STATE_MAX_ENTRIES = v31.V31_STATE_MAX_ENTRIES, 50
try:
    for i in range(120):
        s20.observe(f"id_{i}", {"research_eligible": True, "score": 0,
                                "band": "V31_NEUTRAL", "missing_count": 0,
                                "available_count": 8, "coverage_ratio": 1.0,
                                "missing_components": []}, now_ts=NOW + i)
    cap_ok = len(s20) <= 50 and s20.get("id_119") is not None and s20.get("id_0") is None
finally:
    v31.V31_STATE_MAX_ENTRIES = saved_cap
check(
    "20 TTL prune + hard cap (oldest evicted, newest kept)",
    pruned == 1 and cap_ok,
    f"pruned={pruned} len={len(s20)}",
)

# ── 21 no outcome fields ──────────────────────────────────────────────────
all_rows = []
for tag in ("01", "02", "03", "04", "10", "11", "12", "17"):
    path = tmp_paths(tag)[1]
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as handle:
            all_rows.extend(json.loads(line) for line in handle if line.strip())
forbidden_hits = sorted({
    key for row_ in all_rows for key in row_ if key in v31._V31_FORBIDDEN_ROW_KEYS
})
poison = {"realized_r": 1.5, "first_hit": "TP", "mfe": 2.0, "sl_hit": True}
stripped_path = os.path.join(TMP, "log_strip.jsonl")
v31.append_v31_shadow_row(dict(poison, keep="yes"), log_path=stripped_path)
with open(stripped_path, "r", encoding="utf-8") as handle:
    stripped = json.loads(handle.readline())
check(
    "21 no outcome/future fields; writer strips forbidden keys",
    not forbidden_hits and stripped == {"keep": "yes"},
    f"hits={forbidden_hits} stripped={stripped}",
)

# ── 22 old V3 unchanged ───────────────────────────────────────────────────
import inspect
v3_src = inspect.getsource(sd._smc_pa_score_v3_eval) + inspect.getsource(sd._smc_pa_score_v3_shadow)
cand22 = full_candidate()
v3_out = sd._smc_pa_score_v3_eval(cand22, side="LONG", btc_ctx=full_btc_ctx(), stale_info={})
check(
    "22 old V3 evaluator unchanged",
    sd._SMC_PA_V3_VERSION == "smc_pa_score_v3_shadow_v0.1_log_only"
    and sd._SMC_PA_V3_LOG.endswith("smc_pa_score_v3_shadow.jsonl")
    and "v31" not in v3_src
    and v3_out["smc_pa_v3_total_score"] == 10.0  # core7=10 + proxy BA 0 + RS missing + exec 0
    and v3_out["smc_pa_v3_score_band"] == "V3_STRONG",
    f"v3_total={v3_out['smc_pa_v3_total_score']} band={v3_out['smc_pa_v3_score_band']}",
)

# ── 23 dispatcher wiring: return ignored, isolated ────────────────────────
sd_path = os.path.join(REPO_ROOT, "signal_dispatcher.py")
with open(sd_path, "r", encoding="utf-8") as handle:
    sd_src = handle.read()
wiring_blocks = re.findall(
    r"try:\s*\n\s*from smc_pa_score_v31_shadow import log_v31_shadow\n(?:.*\n)*?\s*except Exception:\s*\n\s*pass",
    sd_src,
)
assigned = re.search(r"=\s*log_v31_shadow\(", sd_src)
v31_consumed = re.search(r"row\[.v31_|row\.update\(.*v31", sd_src)
check(
    "23 wiring: 2 try/except-pass blocks, return ignored, no row consumption",
    len(wiring_blocks) == 2 and assigned is None and v31_consumed is None,
    f"blocks={len(wiring_blocks)} assigned={bool(assigned)}",
)

# ── 24 rotation target present exactly once ───────────────────────────────
check(
    "24 rotation target registered once",
    log_rotation.ROTATION_TARGETS.count("smc_pa_score_v3_1_shadow.jsonl") == 1,
    f"count={log_rotation.ROTATION_TARGETS.count('smc_pa_score_v3_1_shadow.jsonl')}",
)


# ── 25 live dispatcher opened_trade_id lifecycle attribution ───────────────
live_v31_log = os.path.join(TMP, "live_v31_lifecycle.jsonl")
v31.V31_LOG_PATH = live_v31_log
v31._DEFAULT_STORE = v31.V31FreezeStore(path=os.path.join(TMP, "live_v31_state.json"))
sd._LIVE_SMC_RESEARCH_DECISION_LOG = os.path.join(TMP, "live_smc_research_decisions.jsonl")
sd._btc_mtf_context_for_signal = lambda *args, **kwargs: full_btc_ctx()
sd._btc_alignment_instrumentation_shadow = lambda *args, **kwargs: full_btc_ctx()
sd._breakout_acceptance_shadow = lambda *args, **kwargs: None
sd._btc_m5_m15_decomposition_shadow = lambda *args, **kwargs: None

live_candidate = full_candidate(
    symbol="V31LIVEUSDT",
    entry_type="CONFIRM_SMC_RESEARCH",
    dedup_key="V31LIVEUSDT|LONG|CONFIRM|1780000000",
    signal_key="V31LIVEUSDT|LONG|CONFIRM|1780000000",
    candidate_id="v31-live-candidate",
)
candidate_trade = copy.deepcopy(live_candidate)
candidate_trade.update({
    "id": 1780000000123,
    "trade_id": "synthetic-candidate-trade-id",
    "client_order_id": "synthetic-candidate-client-id",
    "score": 90,
    "status": "OPEN",
})
opened_trade = copy.deepcopy(candidate_trade)
opened_trade.update({
    "id": "opened-authoritative-id",
    "trade_id": "opened-trade-id",
    "client_order_id": "BOT-E-V31LIVEUSDT-1",
})
for action, trade in (
    ("SYMBOL_LOCKED", candidate_trade),
    ("PREFILTER_REJECT", candidate_trade),
    ("OPEN_ATTEMPT", candidate_trade),
    ("OPEN_FAILED", candidate_trade),
    ("OPEN_ACCEPTED", opened_trade),
):
    sd._live_smc_research_log(live_candidate, action, reason="simulated", trade=trade)
live_rows = [
    row for row in jsonl_rows(live_v31_log)
    if row.get("execution_mode") == "live"
]
live_by_action = {row.get("action"): row for row in live_rows}
check(
    "25 live SYMBOL_LOCKED opened_trade_id null",
    live_by_action["SYMBOL_LOCKED"].get("opened_trade_id") is None,
    live_by_action["SYMBOL_LOCKED"].get("opened_trade_id"),
)
check(
    "25 live PREFILTER_REJECT opened_trade_id null",
    live_by_action["PREFILTER_REJECT"].get("opened_trade_id") is None,
    live_by_action["PREFILTER_REJECT"].get("opened_trade_id"),
)
check(
    "25 live OPEN_ATTEMPT opened_trade_id null",
    live_by_action["OPEN_ATTEMPT"].get("opened_trade_id") is None,
    live_by_action["OPEN_ATTEMPT"].get("opened_trade_id"),
)
check(
    "25 live OPEN_FAILED opened_trade_id null",
    live_by_action["OPEN_FAILED"].get("opened_trade_id") is None,
    live_by_action["OPEN_FAILED"].get("opened_trade_id"),
)
check(
    "25 live OPEN_ACCEPTED opened_trade_id present",
    live_by_action["OPEN_ACCEPTED"].get("opened_trade_id") == "opened-authoritative-id",
    live_by_action["OPEN_ACCEPTED"].get("opened_trade_id"),
)
check(
    "25 lifecycle rows share dedup_key and v31_signal_id",
    len({row.get("dedup_key") for row in live_rows}) == 1
    and len({row.get("v31_signal_id") for row in live_rows}) == 1,
    [(row.get("dedup_key"), row.get("v31_signal_id")) for row in live_rows],
)
check(
    "25 only accepted row carries opened_trade_id",
    [row.get("opened_trade_id") for row in live_rows]
    == [None, None, None, None, "opened-authoritative-id"],
    [row.get("opened_trade_id") for row in live_rows],
)
check(
    "25 freeze score and band unchanged across lifecycle",
    len({(row.get("v31_score"), row.get("v31_band")) for row in live_rows}) == 1,
    [(row.get("v31_score"), row.get("v31_band")) for row in live_rows],
)
check(
    "25 live lifecycle no outcome fields",
    all(not (set(row) & v31._V31_FORBIDDEN_ROW_KEYS) for row in live_rows),
    [sorted(set(row) & v31._V31_FORBIDDEN_ROW_KEYS) for row in live_rows],
)

paper_lifecycle_store = v31.V31FreezeStore(path=os.path.join(TMP, "paper_v31_lifecycle_state.json"))
paper_lifecycle_log = os.path.join(TMP, "paper_v31_lifecycle.jsonl")
for action, opened_id in (
    ("SYMBOL_LOCKED", None),
    ("PREFILTER_REJECT", None),
    ("CAP", None),
    ("OPEN", "paper-authoritative-id"),
):
    v31.log_v31_shadow(
        live_candidate,
        execution_mode="paper",
        action=action,
        reason="simulated",
        btc_ctx=full_btc_ctx(),
        opened_trade_id=opened_id,
        now_ts=NOW,
        store=paper_lifecycle_store,
        log_path=paper_lifecycle_log,
    )
paper_rows = jsonl_rows(paper_lifecycle_log)
paper_by_action = {row.get("action"): row for row in paper_rows}
check(
    "25 paper rejected cap lock null and open preserved",
    paper_by_action["SYMBOL_LOCKED"].get("opened_trade_id") is None
    and paper_by_action["PREFILTER_REJECT"].get("opened_trade_id") is None
    and paper_by_action["CAP"].get("opened_trade_id") is None
    and paper_by_action["OPEN"].get("opened_trade_id") == "paper-authoritative-id",
    [row.get("opened_trade_id") for row in paper_rows],
)
check(
    "25 four-phase attribution fix remains present",
    "_four_phase_opened_trade_id = None" in sd_src
    and 'if decision == "OPEN_ACCEPTED":' in sd_src
    and "opened_trade_id=_four_phase_opened_trade_id" in sd_src,
)
check(
    "25 v31 wiring return ignored",
    re.search(r"=\s*log_v31_shadow\(", sd_src) is None,
)


# ── stress / memory benchmark ─────────────────────────────────────────────
def rss_kb():
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except Exception:
        pass
    return None


def peak_kb():
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
    except Exception:
        pass
    return None


def stress():
    print("\n=== STRESS / MEMORY BENCHMARK ===")
    baseline = rss_kb()
    t0 = time.time()

    norm = v31.normalize_v31_inputs(full_candidate(), btc_ctx=full_btc_ctx(), now_ts=NOW)
    for i in range(100000):
        comps = v31.evaluate_v31_components(norm)
        v31.evaluate_v31_score(comps)
    t_eval = time.time()
    rss_eval = rss_kb()

    state_path = os.path.join(TMP, "state_stress.json")
    store = v31.V31FreezeStore(path=state_path)
    summary = {"research_eligible": True, "score": 2, "band": "V31_POSITIVE",
               "missing_count": 0, "available_count": 8, "coverage_ratio": 1.0,
               "missing_components": []}
    for i in range(100000):
        store.observe(f"stress_{i}", summary, now_ts=NOW + i * 0.001)
        store.get(f"stress_{i}")
    store.save(now_ts=NOW + 200, force=True)
    t_store = time.time()
    rss_store = rss_kb()

    log_path = os.path.join(TMP, "log_stress.jsonl")
    base_row = v31.assemble_v31_snapshot(
        norm, v31.evaluate_v31_components(norm),
        v31.evaluate_v31_score(v31.evaluate_v31_components(norm)),
        {"frozen": True, "score": 2, "band": "V31_POSITIVE", "candidate": True,
         "research_eligible": True, "freeze_ts": NOW}, 1,
        execution_mode="paper", action="SIM_STRESS",
    )
    for i in range(10000):
        v31.append_v31_shadow_row(base_row, log_path=log_path)
    t_log = time.time()
    final = rss_kb()
    peak = peak_kb()

    print(f"baseline_rss_kb={baseline} after_eval={rss_eval} after_store={rss_store} "
          f"final={final} peak={peak}")
    print(f"eval_100k={t_eval - t0:.2f}s store_100k={t_store - t_eval:.2f}s "
          f"log_10k={t_log - t_store:.2f}s total={t_log - t0:.2f}s")
    print(f"state_cardinality={len(store)} (cap={v31.V31_STATE_MAX_ENTRIES})")
    growth = (final or 0) - (baseline or 0)
    check("S1 store bounded by cap", len(store) <= v31.V31_STATE_MAX_ENTRIES,
          f"len={len(store)}")
    check("S2 no retained upward slope (final-baseline < 200MB)",
          growth < 200 * 1024, f"growth_kb={growth}")
    check("S3 log writes completed", sum(1 for _ in open(log_path)) == 10000)


if "--stress" in sys.argv:
    stress()

# ── production artifact isolation ─────────────────────────────────────────
prod_log_after = os.stat(PROD_LOG) if os.path.exists(PROD_LOG) else None
prod_state_after = os.stat(PROD_STATE) if os.path.exists(PROD_STATE) else None


def same_stat(before, after):
    if before is None and after is None:
        return True
    if before is None or after is None:
        return False
    return before.st_size == after.st_size and before.st_mtime == after.st_mtime


check(
    "00 no production log/state artifacts from simulator",
    same_stat(_PROD_LOG_STAT, prod_log_after)
    and same_stat(_PROD_STATE_STAT, prod_state_after),
    f"log={prod_log_after and prod_log_after.st_size} state={prod_state_after and prod_state_after.st_size}",
)

failed = [name for name, ok, _ in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} checks passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
print("ALL PASS")
