#!/usr/bin/env python3
"""Deterministic simulator for FOUR_PHASE_BREAKOUT_CONTEXT_SHADOW_V1.

Pure fixtures, no network, no production logs/state touched: all state
and log writes go to a temp directory. Run from repo root:

    python scripts/debug/sim_four_phase_breakout_context_shadow_v1.py
    python scripts/debug/sim_four_phase_breakout_context_shadow_v1.py --stress
"""
import json
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import four_phase_breakout_shadow as fp

BASE = 1_700_000_000  # aligned hour
H1 = 3600
M5 = 300

PASS = []
FAIL = []


def check(name, condition, detail=""):
    if condition:
        PASS.append(name)
        print("[PASS] %s" % name)
    else:
        FAIL.append(name)
        print("[FAIL] %s %s" % (name, detail))


def bar(t, o, h, l, c):
    return {"time": float(t), "open": float(o), "high": float(h), "low": float(l), "close": float(c)}


def wave_bars(t0, start_close, end_close, n=20):
    bars = []
    for i in range(n):
        c = start_close + (end_close - start_close) * (i + 1) / float(n)
        o = start_close + (end_close - start_close) * i / float(n)
        h = max(o, c) + 0.5
        l = min(o, c) - 0.5
        bars.append(bar(t0 + i * H1, o, h, l, c))
    return bars


def range_bars(t0, low=100.0, high=104.0, n=18, touches=True):
    """Alternating boundary-touch bars; ATR_H1 == 1.0; width_atr == 4."""
    mid = (low + high) / 2.0
    bars = []
    for i in range(n):
        if touches and i % 2 == 0:
            bars.append(bar(t0 + i * H1, high - 0.8, high, high - 1.0, high - 0.6))
        elif touches:
            bars.append(bar(t0 + i * H1, low + 0.8, low + 1.0, low, low + 0.6))
        else:
            bars.append(bar(t0 + i * H1, mid - 0.2, mid + 0.5, mid - 0.5, mid + 0.2))
    return bars


def m5_inside(t0, n=17, level=102.0):
    return [bar(t0 + i * M5, level, level + 0.25, level - 0.25, level + 0.1) for i in range(n)]


def now_after(bars):
    return bars[-1]["time"] + M5 + 60


def fresh_store(tmp, name):
    return fp.FourPhaseStateStore(path=os.path.join(tmp, name + "_state.json"))


def establish(tmp, name, wave="DOWN", sym="TESTUSDT", wave_n=20):
    """Build store + established cycle; returns (store, h1, m5, now)."""
    store = fresh_store(tmp, name)
    t0 = BASE
    if wave == "DOWN":
        w = wave_bars(t0, 130.0, 104.0, wave_n)
    elif wave == "UP":
        w = wave_bars(t0, 80.0, 100.0, wave_n)
    elif wave == "NEUTRAL":
        w = wave_bars(t0, 103.0, 104.0, wave_n)
    else:
        w = []
    r0 = t0 + len(w) * H1
    h1 = w + range_bars(r0)
    m5 = m5_inside(h1[-1]["time"] + H1)
    now = now_after(m5)
    rec = fp.update_market_cycle(sym, h1, m5, store, now_ts=now)
    return store, h1, m5, now, rec, sym


def mixed_wave_bars(t0):
    """Displacement UP (+6%) but split-half structure clearly DOWN."""
    bars = []
    # first half: flat closes 100, one big spike high 120, low floor 99
    for i in range(10):
        h = 120.0 if i == 4 else 101.0
        bars.append(bar(t0 + i * H1, 100.0, h, 99.0, 100.0))
    # second half: closes rise to 106, highs capped 107 (< 120), one low 98 (< 99)
    for i in range(10):
        c = 100.0 + 6.0 * (i + 1) / 10.0
        o = 100.0 + 6.0 * i / 10.0
        l = 98.0 if i == 0 else min(o, c) - 0.3
        bars.append(bar(t0 + (10 + i) * H1, o, 107.0 if c > 105 else max(o, c) + 0.3, l, c))
    return bars


def breakout_bar(t, kind):
    if kind == "UP_CONFIRMED":
        return bar(t, 103.5, 104.6, 103.45, 104.5)   # dist 0.5 >= 0.25, body 0.91
    if kind == "DOWN_CONFIRMED":
        return bar(t, 100.5, 100.55, 99.4, 99.5)      # dist 0.5, body strong
    if kind == "WICK_UP":
        return bar(t, 103.5, 104.6, 103.4, 103.8)     # wick beyond, close inside
    if kind == "SUB_THRESHOLD_UP":
        return bar(t, 103.8, 104.15, 103.75, 104.1)   # close beyond by 0.1 < 0.25
    if kind == "DOJI_UP":
        return bar(t, 104.35, 104.45, 103.9, 104.4)   # dist ok, body_ratio 0.09
    raise ValueError(kind)


def _production_paths_snapshot():
    repo = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    paths = (
        os.path.join(repo, "four_phase_breakout_shadow_state.json"),
        os.path.join(repo, "logs", "four_phase_breakout_context_shadow_v1.jsonl"),
    )
    return {
        p: (os.path.getmtime(p), os.path.getsize(p)) if os.path.exists(p) else None
        for p in paths
    }


def main():
    pre = _production_paths_snapshot()
    tmp = tempfile.mkdtemp(prefix="four_phase_sim_")
    try:
        run_cases(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    check("no_production_state_or_log_writes", _production_paths_snapshot() == pre)
    print("\n%d passed, %d failed" % (len(PASS), len(FAIL)))
    if FAIL:
        sys.exit(1)


def _redirect_module_defaults(tmp):
    """Point the module's default store/log at the temp dir so no test can
    ever touch production logs/ or the production state file."""
    fp._DEFAULT_STORE = fp.FourPhaseStateStore(path=os.path.join(tmp, "default_state.json"))
    fp.FOUR_PHASE_LOG_PATH = os.path.join(tmp, "default_rows.jsonl")
    fp.FOUR_PHASE_STATE_PATH = os.path.join(tmp, "default_state.json")


def run_cases(tmp):
    _redirect_module_defaults(tmp)
    # ---- 1-4: four phase mappings ----
    for wave, kind, expect in (
        ("DOWN", "UP_CONFIRMED", "ACCUMULATION_CONFIRMED"),
        ("UP", "UP_CONFIRMED", "REACCUMULATION_CONFIRMED"),
        ("UP", "DOWN_CONFIRMED", "DISTRIBUTION_CONFIRMED"),
        ("DOWN", "DOWN_CONFIRMED", "REDISTRIBUTION_CONFIRMED"),
    ):
        store, h1, m5, now, rec, sym = establish(tmp, "phase_%s_%s" % (wave, kind), wave=wave)
        check("establish_%s" % expect, rec["cycle_state"] == "RANGE_ESTABLISHED", rec["cycle_state"])
        m5b = m5 + [breakout_bar(m5[-1]["time"] + M5, kind)]
        rec = fp.update_market_cycle(sym, h1, m5b, store, now_ts=now_after(m5b))
        check("map_%s" % expect,
              rec["cycle_state"] == "PHASE_CONFIRMED" and rec["confirmed_phase"] == expect,
              "%s/%s" % (rec["cycle_state"], rec.get("confirmed_phase")))
        if expect == "ACCUMULATION_CONFIRMED":
            row = fp.assemble_four_phase_snapshot(
                {"symbol": sym, "side": "LONG", "entry_type": "CONFIRM_SMC_RESEARCH",
                 "dedup_key": "%s|LONG|CONFIRM|1" % sym, "source_timestamp": 1},
                store.get(sym), execution_mode="paper", action="OPEN", now_ts=now_after(m5b))
            check("relation_aligned_long_acc", row["entry_side_relation"] == "PHASE_ALIGNED",
                  row["entry_side_relation"])
            row_s = fp.assemble_four_phase_snapshot(
                {"symbol": sym, "side": "SHORT"}, store.get(sym), now_ts=now_after(m5b))
            check("relation_opposed_short_acc", row_s["entry_side_relation"] == "PHASE_OPPOSED",
                  row_s["entry_side_relation"])
            check("confidence_clean", row["phase_confidence"] == "CLEAN", row["phase_confidence"])
            required = [
                "schema_version", "logged_at", "decision_ts", "symbol", "side", "entry_type",
                "signal_key", "candidate_id", "dedup_key", "source_timestamp", "opened_trade_id",
                "market_cycle_id", "previous_wave_direction", "previous_wave_source_tf",
                "previous_wave_strength", "previous_wave_freshness_sec", "range_state",
                "range_high", "range_low", "range_width", "range_width_atr", "range_age_bars",
                "breakout_state", "breakout_direction", "break_close_beyond_atr",
                "body_acceptance", "wick_rejection", "retest_status", "market_phase_candidate",
                "market_phase_confirmed", "phase_freeze_ts", "phase_confidence",
                "entry_side_relation", "missing_fields", "context_quality",
                "false_break_count", "cycle_state", "cycle_age_bars",
            ]
            check("schema_fields_all_present", all(k in row for k in required),
                  [k for k in required if k not in row])
            banned = {"realized_r", "first_hit", "mfe", "mae", "max_favorable_r",
                      "max_adverse_r", "terminal_status", "final_status", "outcome",
                      "sl_hit", "tp_hit", "net_r"}
            check("no_outcome_fields", not (set(row) & banned), sorted(set(row) & banned))
            check("schema_version_value", row["schema_version"] == "four_phase_breakout_v1")

    # ---- no range (trending H1) ----
    store = fresh_store(tmp, "trend")
    h1 = wave_bars(BASE, 100.0, 150.0, 38)
    rec = fp.update_market_cycle("TRENDUSDT", h1, m5_inside(BASE + 38 * H1), store,
                                 now_ts=BASE + 39 * H1)
    check("no_range_trending", rec["cycle_state"] == "NO_CONTEXT"
          and rec["last_range_eval_state"] == "NO_RANGE",
          "%s/%s" % (rec["cycle_state"], rec["last_range_eval_state"]))

    # ---- forming range / insufficient touches ----
    store = fresh_store(tmp, "forming")
    w = wave_bars(BASE, 130.0, 104.0, 20)
    r0 = BASE + 20 * H1
    mid = range_bars(r0, touches=False)
    mid[2] = bar(r0 + 2 * H1, 103.2, 104.0, 103.0, 103.4)   # one high touch
    mid[3] = bar(r0 + 3 * H1, 100.8, 101.0, 100.0, 100.6)   # one low touch
    rec = fp.update_market_cycle("FORMUSDT", w + mid, m5_inside(r0 + 18 * H1), store,
                                 now_ts=r0 + 19 * H1)
    check("forming_insufficient_touches", rec["cycle_state"] == "RANGE_FORMING",
          rec["cycle_state"])

    # ---- excessive width ----
    store = fresh_store(tmp, "wide")
    wide = [bar(BASE + i * H1, 106, 112 if i % 2 == 0 else 107, 100 if i % 2 else 105, 106)
            for i in range(18)]
    rec = fp.update_market_cycle("WIDEUSDT", wave_bars(BASE - 20 * H1, 130, 106, 20) + wide,
                                 m5_inside(BASE + 18 * H1), store, now_ts=BASE + 19 * H1)
    check("excessive_width_no_range", rec["last_range_eval_state"] in ("NO_RANGE", "RANGE_INVALID"),
          rec["last_range_eval_state"])

    # ---- insufficient pre-range bars -> PREV_WAVE_MISSING -> PHASE_UNKNOWN ----
    store = fresh_store(tmp, "nopre")
    short_wave = wave_bars(BASE, 120.0, 104.0, 10)
    h1 = short_wave + range_bars(BASE + 10 * H1)
    m5 = m5_inside(h1[-1]["time"] + H1)
    rec = fp.update_market_cycle("NOPREUSDT", h1, m5, store, now_ts=now_after(m5))
    check("prev_wave_missing", (rec.get("previous_wave") or {}).get("previous_wave_direction")
          == "PREV_WAVE_MISSING", rec.get("previous_wave"))
    m5b = m5 + [breakout_bar(m5[-1]["time"] + M5, "UP_CONFIRMED")]
    rec = fp.update_market_cycle("NOPREUSDT", h1, m5b, store, now_ts=now_after(m5b))
    check("missing_wave_confirms_unknown", rec["confirmed_phase"] == "PHASE_UNKNOWN",
          rec.get("confirmed_phase"))

    # ---- neutral previous wave -> PHASE_UNKNOWN ----
    store, h1, m5, now, rec, sym = establish(tmp, "neutral", wave="NEUTRAL", sym="NEUTUSDT")
    check("prev_wave_neutral", (rec.get("previous_wave") or {}).get("previous_wave_direction")
          == "PREV_WAVE_NEUTRAL", rec.get("previous_wave"))
    m5b = m5 + [breakout_bar(m5[-1]["time"] + M5, "UP_CONFIRMED")]
    rec = fp.update_market_cycle(sym, h1, m5b, store, now_ts=now_after(m5b))
    check("neutral_wave_confirms_unknown", rec["confirmed_phase"] == "PHASE_UNKNOWN",
          rec.get("confirmed_phase"))

    # ---- mixed previous wave (M15/H1-style conflict analogue) ----
    store = fresh_store(tmp, "mixed")
    h1 = mixed_wave_bars(BASE) + range_bars(BASE + 20 * H1)
    m5 = m5_inside(h1[-1]["time"] + H1)
    rec = fp.update_market_cycle("MIXUSDT", h1, m5, store, now_ts=now_after(m5))
    wave_out = (rec.get("previous_wave") or {})
    check("prev_wave_mixed", wave_out.get("previous_wave_direction") == "PREV_WAVE_MIXED",
          wave_out)

    # ---- wick-only / sub-threshold / weak body -> PENDING ----
    for kind, name in (("WICK_UP", "wick_only"), ("SUB_THRESHOLD_UP", "sub_threshold"),
                       ("DOJI_UP", "body_below_04")):
        store, h1, m5, now, rec, sym = establish(tmp, "pend_" + name, sym="P%sUSDT" % name[:3].upper())
        m5b = m5 + [breakout_bar(m5[-1]["time"] + M5, kind)]
        rec = fp.update_market_cycle(sym, h1, m5b, store, now_ts=now_after(m5b))
        check("pending_%s" % name, rec["cycle_state"] == "BREAK_PENDING"
              and rec["pending_direction"] == "UP",
              "%s/%s" % (rec["cycle_state"], rec.get("pending_direction")))
        if kind == "WICK_UP":
            check("wick_rejection_flag", rec["last_breakout"]["wick_rejection"] is True,
                  rec["last_breakout"])

    # ---- false break: pending then 3 closes back inside ----
    store, h1, m5, now, rec, sym = establish(tmp, "falsebreak", sym="FBUSDT")
    wick_t = m5[-1]["time"] + M5
    m5b = m5 + [breakout_bar(wick_t, "WICK_UP")]
    rec = fp.update_market_cycle(sym, h1, m5b, store, now_ts=now_after(m5b))
    check("false_break_precondition", rec["cycle_state"] == "BREAK_PENDING", rec["cycle_state"])
    m5c = m5b + [bar(wick_t + i * M5, 103.0, 103.3, 102.8, 103.1) for i in (1, 2, 3)]
    rec = fp.update_market_cycle(sym, h1, m5c, store, now_ts=now_after(m5c))
    check("false_break_resolution", rec["cycle_state"] == "RANGE_ESTABLISHED"
          and rec["false_break_count"] == 1 and rec["pending_direction"] is None,
          "%s/fb=%s" % (rec["cycle_state"], rec["false_break_count"]))
    check("false_break_phase_unresolved", rec["confirmed_phase"] is None)

    # ---- confirmed persistence + later contradiction cannot relabel ----
    store, h1, m5, now, rec, sym = establish(tmp, "persist", sym="PERSUSDT")
    m5b = m5 + [breakout_bar(m5[-1]["time"] + M5, "UP_CONFIRMED")]
    rec = fp.update_market_cycle(sym, h1, m5b, store, now_ts=now_after(m5b))
    frozen_phase = rec["confirmed_phase"]
    frozen_ts = rec["phase_freeze_ts"]
    frozen_id = rec["market_cycle_id"]
    m5c = m5b + [breakout_bar(m5b[-1]["time"] + M5, "DOWN_CONFIRMED")]
    rec = fp.update_market_cycle(sym, h1, m5c, store, now_ts=now_after(m5c))
    check("confirmed_immutable", rec["cycle_state"] == "PHASE_CONFIRMED"
          and rec["confirmed_phase"] == frozen_phase and rec["phase_freeze_ts"] == frozen_ts
          and rec["market_cycle_id"] == frozen_id,
          "%s/%s" % (rec["cycle_state"], rec.get("confirmed_phase")))

    # ---- cooldown then new cycle with new identity ----
    later = frozen_ts + 12 * H1 + 60
    shift = 30.0
    h1_new = wave_bars(BASE + 40 * H1, 130.0 + shift, 104.0 + shift, 20) + \
        range_bars(BASE + 60 * H1, low=100.0 + shift, high=104.0 + shift)
    m5_new = m5_inside(h1_new[-1]["time"] + H1, level=102.0 + shift)
    rec = fp.update_market_cycle(sym, h1_new, m5_new, store,
                                 now_ts=max(later, now_after(m5_new)))
    check("cooldown_new_cycle", rec["cycle_state"] == "RANGE_ESTABLISHED"
          and rec["market_cycle_id"] != frozen_id,
          "%s/%s" % (rec["cycle_state"], rec.get("market_cycle_id")))

    # ---- TTL invalidation ----
    store, h1, m5, now, rec, sym = establish(tmp, "ttl", sym="TTLUSDT")
    rec = fp.update_market_cycle(sym, h1, m5, store, now_ts=now + 96 * H1 + 60)
    check("ttl_invalidation", rec["cycle_state"] == "PHASE_INVALIDATED"
          and rec["invalidated_reason"] == "TTL_EXPIRED",
          "%s/%s" % (rec["cycle_state"], rec.get("invalidated_reason")))
    rec = fp.update_market_cycle(sym, [], [], store, now_ts=now + 96 * H1 + 120)
    check("invalidated_collapses_no_context", rec["cycle_state"] == "NO_CONTEXT",
          rec["cycle_state"])

    # ---- H1 unconfirmed departure -> invalidation ----
    store, h1, m5, now, rec, sym = establish(tmp, "depart", sym="DEPUSDT")
    drift = h1 + [bar(h1[-1]["time"] + (i + 1) * H1, 105.0, 106.5, 104.9, 106.0 + i * 0.2)
                  for i in range(6)]
    rec = fp.update_market_cycle(sym, drift, m5_inside(drift[-1]["time"] + H1), store,
                                 now_ts=drift[-1]["time"] + 2 * H1)
    check("departure_invalidation", rec["cycle_state"] == "PHASE_INVALIDATED"
          and str(rec["invalidated_reason"]).startswith("UNCONFIRMED_DEPARTURE"),
          "%s/%s" % (rec["cycle_state"], rec.get("invalidated_reason")))

    # ---- state corruption fails safe ----
    corrupt_path = os.path.join(tmp, "corrupt_state.json")
    with open(corrupt_path, "w") as fh:
        fh.write("{not json!!!")
    store = fp.FourPhaseStateStore(path=corrupt_path)
    check("corrupt_state_loads_empty", len(store) == 0)

    # ---- restart reconstruction ----
    store, h1, m5, now, rec, sym = establish(tmp, "restart", sym="RESUSDT")
    cid = rec["market_cycle_id"]
    store2 = fp.FourPhaseStateStore(path=store.path)
    check("restart_reload_same_cycle", (store2.get(sym) or {}).get("market_cycle_id") == cid)
    os.remove(store.path)
    store3 = fp.FourPhaseStateStore(path=store.path)
    rec3 = fp.update_market_cycle(sym, h1, m5, store3, now_ts=now + 60)
    check("restart_rederive_same_identity", rec3["market_cycle_id"] == cid,
          "%s vs %s" % (rec3.get("market_cycle_id"), cid))

    # ---- identity determinism / collisions ----
    a = fp.build_market_cycle_id("AAAUSDT", 1700000000, 104.0, 100.0)
    b = fp.build_market_cycle_id("AAAUSDT", 1700000000, 104.0, 100.0)
    c = fp.build_market_cycle_id("BBBUSDT", 1700000000, 104.0, 100.0)
    d = fp.build_market_cycle_id("AAAUSDT", 1700003600, 104.0, 100.0)
    e = fp.build_market_cycle_id("AAAUSDT", 1700000000, 104.00001, 100.0)
    check("identity_deterministic", a == b)
    check("identity_symbol_distinct", a != c)
    check("identity_start_distinct", a != d)
    check("identity_price_distinct", a != e, "%s vs %s" % (a, e))

    # ---- pruning ----
    store = fresh_store(tmp, "prune")
    old = fp._fresh_record(BASE)
    old["last_seen_ts"] = BASE - 8 * 86400
    store.put("OLDUSDT", old, hard=False, now_ts=BASE)
    store.put("NEWUSDT", fp._fresh_record(BASE), hard=False, now_ts=BASE)
    store.save(now_ts=BASE)
    check("prune_old_symbols", store.get("OLDUSDT") is None and store.get("NEWUSDT") is not None)

    # ---- writer failure isolation ----
    blocker = os.path.join(tmp, "blocker_file")
    with open(blocker, "w") as fh:
        fh.write("x")
    ok = fp.append_four_phase_shadow_row({"schema_version": "x"},
                                         log_path=os.path.join(blocker, "sub", "x.jsonl"))
    check("writer_failure_isolated", ok is False)

    # ---- evaluator return ignored / garbage inputs never raise ----
    try:
        fp.log_four_phase_snapshot(None, execution_mode="paper", action="OPEN")
        fp.log_four_phase_snapshot({"symbol": None, "side": object()}, action="X")
        fp.update_market_cycle_from_frames("GARBAGE", None, None)
        check("garbage_inputs_never_raise", True)
    except Exception as exc:
        check("garbage_inputs_never_raise", False, repr(exc))

    # ---- writer emits valid json rows to temp log ----
    log_path = os.path.join(tmp, "rows.jsonl")
    row = fp.assemble_four_phase_snapshot({"symbol": "AAAUSDT", "side": "LONG"}, None,
                                          execution_mode="paper", action="OPEN", now_ts=BASE)
    fp.append_four_phase_shadow_row(row, log_path=log_path)
    with open(log_path) as fh:
        parsed = json.loads(fh.readline())
    check("row_json_roundtrip", parsed["schema_version"] == "four_phase_breakout_v1"
          and parsed["cycle_state"] == "NO_CONTEXT"
          and parsed["context_quality"] == "MISSING")

    # ---- BTC context irrelevant / no current-trend inputs (static) ----
    src = open(fp.__file__.replace(".pyc", ".py")).read()
    check("no_trend_direction_input", "trend_direction" not in src)
    check("no_smc_bias_input", "smc_bias" not in src)
    check("no_btc_input", "btc_" not in src.lower())
    check("no_outcome_tokens_in_module", all(tok not in src for tok in
          ("realized_r", "first_hit", '"mfe"', '"mae"')))

    # ---- direct mapper truth table ----
    check("mapper_truth_table",
          fp.map_four_phase("PREV_WAVE_DOWN", "BREAK_UP_CONFIRMED") == "ACCUMULATION_CONFIRMED"
          and fp.map_four_phase("PREV_WAVE_UP", "BREAK_UP_CONFIRMED") == "REACCUMULATION_CONFIRMED"
          and fp.map_four_phase("PREV_WAVE_UP", "BREAK_DOWN_CONFIRMED") == "DISTRIBUTION_CONFIRMED"
          and fp.map_four_phase("PREV_WAVE_DOWN", "BREAK_DOWN_CONFIRMED") == "REDISTRIBUTION_CONFIRMED"
          and fp.map_four_phase("PREV_WAVE_MIXED", "BREAK_UP_CONFIRMED") == "PHASE_UNKNOWN"
          and fp.map_four_phase("PREV_WAVE_UP", "BREAK_NONE") == "PHASE_PENDING"
          and fp.map_four_phase("PREV_WAVE_UP", "BREAK_NONE", range_established=False)
          == "RANGE_UNRESOLVED")


def _rss_kb():
    with open("/proc/self/status") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return -1


def stress():
    import resource
    tmp = tempfile.mkdtemp(prefix="four_phase_stress_")
    try:
        _redirect_module_defaults(tmp)
        store = fp.FourPhaseStateStore(path=os.path.join(tmp, "stress_state.json"))
        log_path = os.path.join(tmp, "stress_rows.jsonl")
        symbols = ["SYM%03dUSDT" % i for i in range(500)]
        h1_cache = {}
        for idx, sym in enumerate(symbols):
            t0 = BASE + (idx % 7) * H1
            h1_cache[sym] = wave_bars(t0, 130.0, 104.0, 20) + range_bars(t0 + 20 * H1)

        n_calls = 100_000
        rows = 0
        baseline = _rss_kb()
        samples = []
        t_start = time.time()
        now = BASE + 40 * H1
        for i in range(n_calls):
            sym = symbols[i % len(symbols)]
            now += 1.0
            m5 = m5_inside(now - 18 * M5, n=17)
            rec = fp.update_market_cycle(sym, h1_cache[sym], m5, store, now_ts=now)
            if i % 10 == 0:
                row = fp.assemble_four_phase_snapshot(
                    {"symbol": sym, "side": "LONG", "dedup_key": "%s|LONG|CONFIRM|%d" % (sym, i)},
                    rec, execution_mode="paper", action="STRESS", now_ts=now)
                if fp.append_four_phase_shadow_row(row, log_path=log_path):
                    rows += 1
            if i % 10_000 == 0:
                samples.append((i, _rss_kb()))
        elapsed = time.time() - t_start
        final = _rss_kb()
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        mid = samples[len(samples) // 2][1]
        print(json.dumps({
            "calls": n_calls,
            "elapsed_secs": round(elapsed, 2),
            "calls_per_sec": round(n_calls / elapsed, 0),
            "baseline_rss_kb": baseline,
            "mid_rss_kb": mid,
            "final_rss_kb": final,
            "peak_rss_kb": peak,
            "rss_samples": samples,
            "state_symbols": len(store),
            "log_rows_written": rows,
            "retained_slope_kb_mid_to_final": final - mid,
        }, indent=2))
        check("stress_state_bounded", len(store) <= len(symbols))
        check("stress_no_retained_growth", final - mid < 20_000,
              "final-mid=%dkB" % (final - mid))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("\n%d passed, %d failed" % (len(PASS), len(FAIL)))
    if FAIL:
        sys.exit(1)


if __name__ == "__main__":
    if "--stress" in sys.argv:
        stress()
    else:
        main()
