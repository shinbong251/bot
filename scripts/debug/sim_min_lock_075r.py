#!/usr/bin/env python3
"""
Standalone read-only simulator for CONFIRM_SMC_RESEARCH min-lock 0.75R SL movement.

Reproduces ONLY the formula block from execution.py lines 5648-5708.
No imports from production code. No file writes. No state mutation.
"""

import sys

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"

results = []


# ---------------------------------------------------------------------------
# Replicated helpers (verbatim logic from execution.py)
# ---------------------------------------------------------------------------

def _sim_trade_matches(t):
    """Mirrors _paper_smc_research_trade_matches (execution.py:1377-1384)."""
    if not isinstance(t, dict):
        return False
    return (
        t.get("entry_type") == "CONFIRM_SMC_RESEARCH"
        or t.get("strategy_family") == "confirm_smc_research"
        or t.get("research_source") == "confirm_structural_outcome_shadow"
    )


def _sim_min_lock_075(t, exec_mode="paper"):
    """
    Mirrors the PAPER SMC RESEARCH MIN-LOCK 0.75R block (execution.py:5648-5708).

    Returns a dict with simulation outputs; never mutates t directly —
    works on a shallow copy.
    """
    t = dict(t)  # shallow copy — do not mutate caller's dict
    out = {
        "gate_passed": False,
        "sl_moved": False,
        "new_sl": t.get("sl"),
        "floor": None,
        "initial_risk": None,
        "min_lock_075_done": t.get("min_lock_075_done", False),
        "skip_reason": None,
    }

    # Gate 1: exec_mode
    if exec_mode != "paper":
        out["skip_reason"] = "exec_mode != paper"
        return out

    # Gate 2: trade matches
    if not _sim_trade_matches(t):
        out["skip_reason"] = "trade_not_matched (entry_type/strategy_family/research_source)"
        return out

    # Gate 3: not already done
    if t.get("min_lock_075_done"):
        out["skip_reason"] = "min_lock_075_done already True"
        return out

    # Gate 4: max_profit_r >= 0.75
    if float(t.get("max_profit_r", 0)) < 0.75:
        out["skip_reason"] = f"max_profit_r={t.get('max_profit_r')} < 0.75"
        return out

    # All gates passed
    out["gate_passed"] = True

    _entry_real = t.get("entry_real") or t.get("entry")
    _sl_init = t.get("sl_init")

    if _entry_real is None or _sl_init is None:
        out["skip_reason"] = "entry_real or sl_init missing"
        return out

    _initial_risk = abs(float(_entry_real) - float(_sl_init))
    out["initial_risk"] = _initial_risk

    if _initial_risk <= 0:
        out["skip_reason"] = "initial_risk <= 0"
        return out

    _current_sl = t["sl"]

    if t["side"] == "LONG":
        _floor = float(_entry_real) + _initial_risk * 0.75
        _should_move = _floor > _current_sl
    else:
        _floor = float(_entry_real) - _initial_risk * 0.75
        _should_move = _floor < _current_sl

    out["floor"] = _floor

    if _should_move:
        out["new_sl"] = _floor
    out["sl_moved"] = _should_move
    out["min_lock_075_done"] = True  # always set after logic runs

    return out


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

def check(label, condition, detail=""):
    status = PASS if condition else FAIL
    results.append((label, status, detail))
    return condition


def run_case(case_name, t, exec_mode="paper"):
    return _sim_min_lock_075(t, exec_mode=exec_mode)


# ---------------------------------------------------------------------------
# Case 1: LONG sl_moved=True
# ---------------------------------------------------------------------------
print("\n=== CASE 1: LONG, old_sl=90, expect sl_moved=True ===")
t1 = {
    "side": "LONG",
    "entry_type": "CONFIRM_SMC_RESEARCH",
    "entry": 100.0,
    "entry_real": 100.0,
    "sl_init": 90.0,
    "sl": 90.0,
    "max_profit_r": 0.80,
    "min_lock_075_done": False,
}
r1 = run_case("CASE1_LONG_sl90", t1)
expected_floor_1 = 107.5
expected_realized_r_at_sl_1 = 0.75

check("C1 gate_passed", r1["gate_passed"])
check("C1 sl_moved=True", r1["sl_moved"] is True, f"sl_moved={r1['sl_moved']}")
check("C1 floor=107.5", abs(r1["floor"] - expected_floor_1) < 1e-9, f"floor={r1['floor']}")
check("C1 new_sl=107.5", abs(r1["new_sl"] - 107.5) < 1e-9, f"new_sl={r1['new_sl']}")
check("C1 realized_R=0.75", abs((r1["new_sl"] - 100.0) / 10.0 - 0.75) < 1e-9,
      f"realized_R={(r1['new_sl']-100.0)/10.0}")
check("C1 min_lock_075_done=True", r1["min_lock_075_done"] is True)

print(f"  floor={r1['floor']}  new_sl={r1['new_sl']}  sl_moved={r1['sl_moved']}")

# LONG with old_sl=100 (at breakeven — floor still 107.5 > 100)
print("\n=== CASE 1b: LONG, old_sl=100 (BE), expect sl_moved=True ===")
t1b = dict(t1, sl=100.0)
r1b = run_case("CASE1b_LONG_sl100", t1b)
check("C1b sl_moved=True (old_sl=BE)", r1b["sl_moved"] is True, f"sl_moved={r1b['sl_moved']}")
check("C1b new_sl=107.5", abs(r1b["new_sl"] - 107.5) < 1e-9, f"new_sl={r1b['new_sl']}")
print(f"  floor={r1b['floor']}  new_sl={r1b['new_sl']}  sl_moved={r1b['sl_moved']}")

# ---------------------------------------------------------------------------
# Case 2: SHORT sl_moved=True
# ---------------------------------------------------------------------------
print("\n=== CASE 2: SHORT, old_sl=110, expect sl_moved=True ===")
t2 = {
    "side": "SHORT",
    "entry_type": "CONFIRM_SMC_RESEARCH",
    "entry": 100.0,
    "entry_real": 100.0,
    "sl_init": 110.0,
    "sl": 110.0,
    "max_profit_r": 0.80,
    "min_lock_075_done": False,
}
r2 = run_case("CASE2_SHORT_sl110", t2)
expected_floor_2 = 92.5

check("C2 gate_passed", r2["gate_passed"])
check("C2 sl_moved=True", r2["sl_moved"] is True, f"sl_moved={r2['sl_moved']}")
check("C2 floor=92.5", abs(r2["floor"] - expected_floor_2) < 1e-9, f"floor={r2['floor']}")
check("C2 new_sl=92.5", abs(r2["new_sl"] - 92.5) < 1e-9, f"new_sl={r2['new_sl']}")
check("C2 realized_R=0.75", abs((100.0 - r2["new_sl"]) / 10.0 - 0.75) < 1e-9,
      f"realized_R={(100.0-r2['new_sl'])/10.0}")
check("C2 min_lock_075_done=True", r2["min_lock_075_done"] is True)
print(f"  floor={r2['floor']}  new_sl={r2['new_sl']}  sl_moved={r2['sl_moved']}")

# SHORT with old_sl=100
print("\n=== CASE 2b: SHORT, old_sl=100 (BE), expect sl_moved=True ===")
t2b = dict(t2, sl=100.0)
r2b = run_case("CASE2b_SHORT_sl100", t2b)
check("C2b sl_moved=True (old_sl=BE)", r2b["sl_moved"] is True, f"sl_moved={r2b['sl_moved']}")
check("C2b new_sl=92.5", abs(r2b["new_sl"] - 92.5) < 1e-9, f"new_sl={r2b['new_sl']}")
print(f"  floor={r2b['floor']}  new_sl={r2b['new_sl']}  sl_moved={r2b['sl_moved']}")

# ---------------------------------------------------------------------------
# Case 3: No-regression — SHORT, old_sl already better than floor
# ---------------------------------------------------------------------------
print("\n=== CASE 3: SHORT no-regression, old_sl=85 < floor=92.5, expect sl_moved=False ===")
t3 = {
    "side": "SHORT",
    "entry_type": "CONFIRM_SMC_RESEARCH",
    "entry": 100.0,
    "entry_real": 100.0,
    "sl_init": 110.0,
    "sl": 85.0,   # already better (lower) than 92.5 floor
    "max_profit_r": 1.20,
    "min_lock_075_done": False,
}
r3 = run_case("CASE3_SHORT_noregress", t3)
check("C3 gate_passed", r3["gate_passed"])
check("C3 sl_moved=False", r3["sl_moved"] is False, f"sl_moved={r3['sl_moved']}")
check("C3 new_sl=85 (unchanged)", abs(r3["new_sl"] - 85.0) < 1e-9, f"new_sl={r3['new_sl']}")
check("C3 min_lock_075_done=True", r3["min_lock_075_done"] is True)
print(f"  floor={r3['floor']}  new_sl={r3['new_sl']}  sl_moved={r3['sl_moved']}")

# ---------------------------------------------------------------------------
# Case 4: CONFIRM (non-CONFIRM_SMC_RESEARCH) entry_type — gate should block
# ---------------------------------------------------------------------------
print("\n=== CASE 4: entry_type=CONFIRM (not CONFIRM_SMC_RESEARCH), expect gate skipped ===")
t4 = {
    "side": "LONG",
    "entry_type": "CONFIRM",
    "entry": 100.0,
    "entry_real": 100.0,
    "sl_init": 90.0,
    "sl": 90.0,
    "max_profit_r": 1.0,
    "min_lock_075_done": False,
}
r4 = run_case("CASE4_CONFIRM_nonresearch", t4)
check("C4 gate_passed=False", r4["gate_passed"] is False, f"gate_passed={r4['gate_passed']}")
check("C4 sl_moved=False", r4["sl_moved"] is False)
check("C4 new_sl unchanged=90", abs(r4["new_sl"] - 90.0) < 1e-9, f"new_sl={r4['new_sl']}")
check("C4 min_lock_075_done unchanged", r4["min_lock_075_done"] is False)
print(f"  skip_reason={r4['skip_reason']}  sl_moved={r4['sl_moved']}  min_lock_075_done={r4['min_lock_075_done']}")

# ---------------------------------------------------------------------------
# Case 5: Missing/invalid risk — no crash
# ---------------------------------------------------------------------------
print("\n=== CASE 5a: sl_init missing — no crash ===")
t5a = {
    "side": "LONG",
    "entry_type": "CONFIRM_SMC_RESEARCH",
    "entry": 100.0,
    "entry_real": 100.0,
    # sl_init intentionally omitted
    "sl": 90.0,
    "max_profit_r": 0.80,
    "min_lock_075_done": False,
}
try:
    r5a = run_case("CASE5a_missing_sl_init", t5a)
    check("C5a no crash", True)
    check("C5a sl_moved=False", r5a["sl_moved"] is False)
    check("C5a new_sl unchanged=90", abs(r5a["new_sl"] - 90.0) < 1e-9, f"new_sl={r5a['new_sl']}")
    print(f"  skip_reason={r5a['skip_reason']}  sl_moved={r5a['sl_moved']}")
except Exception as ex:
    check("C5a no crash", False, str(ex))

print("\n=== CASE 5b: initial_risk=0 (entry==sl_init) — no crash ===")
t5b = {
    "side": "LONG",
    "entry_type": "CONFIRM_SMC_RESEARCH",
    "entry": 100.0,
    "entry_real": 100.0,
    "sl_init": 100.0,   # risk = 0
    "sl": 100.0,
    "max_profit_r": 0.80,
    "min_lock_075_done": False,
}
try:
    r5b = run_case("CASE5b_zero_risk", t5b)
    check("C5b no crash", True)
    check("C5b sl_moved=False", r5b["sl_moved"] is False)
    print(f"  skip_reason={r5b['skip_reason']}  sl_moved={r5b['sl_moved']}")
except Exception as ex:
    check("C5b no crash", False, str(ex))

print("\n=== CASE 5c: max_profit_r=0.74 — gate blocks at 0.75 threshold ===")
t5c = dict(t1, max_profit_r=0.74)
r5c = run_case("CASE5c_maxR_below_075", t5c)
check("C5c gate_passed=False", r5c["gate_passed"] is False)
check("C5c sl_moved=False", r5c["sl_moved"] is False)
print(f"  skip_reason={r5c['skip_reason']}  sl_moved={r5c['sl_moved']}")

# ---------------------------------------------------------------------------
# Case 6: exec_mode=live — gate blocks
# ---------------------------------------------------------------------------
print("\n=== CASE 6: exec_mode=live — paper gate blocks ===")
t6 = dict(t1)
r6 = run_case("CASE6_live_mode", t6, exec_mode="live")
check("C6 gate_passed=False (live)", r6["gate_passed"] is False)
check("C6 sl_moved=False", r6["sl_moved"] is False)
print(f"  skip_reason={r6['skip_reason']}")

# ---------------------------------------------------------------------------
# Results table
# ---------------------------------------------------------------------------
print("\n" + "=" * 72)
print(f"{'Label':<45} {'Status':<8} {'Detail'}")
print("-" * 72)
all_pass = True
for label, status, detail in results:
    marker = "" if status == PASS else " <---"
    print(f"  {label:<43} {status:<8} {detail}{marker}")
    if status != PASS:
        all_pass = False

print("=" * 72)
total = len(results)
passed = sum(1 for _, s, _ in results if s == PASS)
failed = total - passed
print(f"\nSUMMARY: {passed}/{total} PASS  |  {failed} FAIL")

# ---------------------------------------------------------------------------
# Audit summary
# ---------------------------------------------------------------------------
print("""
PRODUCTION CODE AUDIT (execution.py)
=====================================
Block location  : lines 5648–5708  (PAPER SMC RESEARCH MIN-LOCK 0.75R)
                  lines 5710–5812  (LIVE SMC RESEARCH MIN-LOCK 0.75R — separate block)

Formula match   :
  LONG  : entry_real + abs(entry_real - sl_init) * 0.75   [line 5665]  ✓
  SHORT : entry_real - abs(entry_real - sl_init) * 0.75   [line 5668]  ✓

Never-regress guards:
  LONG  : floor > current_sl  → _ml075_should_move        [line 5666]  ✓
  SHORT : floor < current_sl  → _ml075_should_move        [line 5669]  ✓
  (SL only updated if _ml075_should_move is True)          [line 5670]  ✓

Gate conditions (all must pass):
  _exec_mode == "paper"                                    [line 5651]  ✓
  _paper_smc_research_trade_matches(t)                     [line 5652]  ✓
  not t.get("min_lock_075_done")                           [line 5653]  ✓
  float(t.get("max_profit_r", 0)) >= 0.75                 [line 5654]  ✓

min_lock_075_done set: ALWAYS after risk > 0, regardless of sl_moved  [line 5672]  ✓
save_open_trades() called: YES in production               [line 5673]
  (NOT called in this simulator — read-only by design)

LIVE path (lines 5710–5812):
  Gated by _exec_mode == "live" AND live_smc_research_enabled=True
  Requires exchange SL sync via _sync_testnet_trailing_sl()
  min_lock_075_done only marked after confirmed sync (line 5762/5771)
""")

sys.exit(0 if all_pass else 1)
