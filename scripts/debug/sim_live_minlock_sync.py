#!/usr/bin/env python3
"""
Mock contract test for live min-lock 0.75R SL sync behavior.

Mirrors execution.py lines 5710-5820 (LIVE SMC RESEARCH MIN-LOCK 0.75R block).
Replaces _sync_testnet_trailing_sl() with a controllable mock.

No production imports. No exchange calls. No state mutation. Read-only.
"""

import sys
import traceback
from copy import deepcopy

# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"

results = []
issues = []


def check(label, condition, detail="", severity=FAIL):
    status = PASS if condition else severity
    results.append((label, status, detail))
    if not condition and severity == FAIL:
        issues.append(f"[FAIL] {label}: {detail}")
    elif not condition and severity == WARN:
        issues.append(f"[WARN] {label}: {detail}")
    return condition


# ---------------------------------------------------------------------------
# Replicated gate helpers (exact logic from execution.py)
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


# ---------------------------------------------------------------------------
# Mock _sync_testnet_trailing_sl contract
# ---------------------------------------------------------------------------

class SyncCall:
    """Records one call to the mock sync."""
    def __init__(self, t_sl, old_sl):
        self.t_sl = t_sl        # t["sl"] at call time (what would be sent to exchange)
        self.old_sl = old_sl    # old_sl argument passed


def make_sync_mock(return_value, raise_exc=None):
    """
    Returns (mock_fn, calls_list).
    mock_fn signature mirrors _sync_testnet_trailing_sl(t, ctx, old_sl=None).
    """
    calls = []

    def mock_sync(t, ctx, old_sl=None):
        calls.append(SyncCall(t["sl"], old_sl))
        if raise_exc is not None:
            raise raise_exc
        # Simulate side-effects of the real function on success path
        if return_value is True:
            t["exchange_sl_id"] = "NEW_ORDER_ID_MOCK"
            t["exchange_sl_price_confirmed"] = t["sl"]
            t.pop("exchange_sl_sync_pending", None)
        elif return_value is False:
            t["exchange_sl_sync_pending"] = t["sl"]
        return return_value

    return mock_sync, calls


# ---------------------------------------------------------------------------
# Core simulator — mirrors the live min-lock block verbatim
# (execution.py:5717-5820, with _sync replaced by mock)
# ---------------------------------------------------------------------------

def run_live_minlock_block(t_input, sync_mock, exec_mode="live", live_research_enabled=True,
                           current_price=None):
    """
    Runs the live min-lock 0.75R block on a copy of t_input.
    Returns (t_after, out_dict).
    """
    t = deepcopy(t_input)
    p = current_price if current_price is not None else float(t.get("entry_real") or t.get("entry") or 100)

    out = {
        "gate_passed": False,
        "lml_log_event": None,
        "sl_moved": None,
        "new_sl": t["sl"],
        "floor": None,
        "initial_risk": None,
        "min_lock_075_done": t.get("min_lock_075_done", False),
        "sync_called": False,
        "sync_result": None,
        "exception_caught": None,
    }

    # Outer try mirrors production (execution.py:5717)
    try:
        # Gate block (lines 5718-5724)
        if not (
            exec_mode == "live"
            and live_research_enabled
            and _sim_trade_matches(t)
            and not t.get("min_lock_075_done")
            and float(t.get("max_profit_r", 0)) >= 0.75
        ):
            return t, out

        out["gate_passed"] = True

        _lml_entry_real = t.get("entry_real") or t.get("entry")
        _lml_sl_init = t.get("sl_init")

        if _lml_entry_real is None or _lml_sl_init is None:
            return t, out

        _lml_initial_risk = abs(float(_lml_entry_real) - float(_lml_sl_init))
        out["initial_risk"] = _lml_initial_risk

        if _lml_initial_risk <= 0:
            return t, out

        _lml_current_sl = t["sl"]

        # Formula (lines 5733-5742)
        if t["side"] == "LONG":
            _lml_floor = float(_lml_entry_real) + _lml_initial_risk * 0.75
            _lml_should_move = _lml_floor > _lml_current_sl
        else:
            _lml_floor = float(_lml_entry_real) - _lml_initial_risk * 0.75
            _lml_should_move = _lml_floor < _lml_current_sl

        out["floor"] = _lml_floor
        out["sl_moved"] = _lml_should_move

        # Local SL mutation (line 5744-5745)
        if _lml_should_move:
            t["sl"] = _lml_floor

        out["new_sl"] = t["sl"]

        # Exchange sync call (line 5756-5758)
        out["sync_called"] = True
        _lml_sync_result = sync_mock(t, ctx=None, old_sl=_lml_current_sl)
        out["sync_result"] = _lml_sync_result

        # Branch on sync result (lines 5760-5771 — PATCHED)
        if _lml_sync_result is True:
            _lml_log_event = "MIN_LOCK_075_LIVE_SYNC_OK"
            t["min_lock_075_done"] = True           # line 5762 — unchanged
        elif _lml_sync_result is False:
            # Do NOT mark done (line 5764-5766) — unchanged
            _lml_log_event = "MIN_LOCK_075_LIVE_SYNC_FAILED"
        else:
            # None path — PATCHED: fail-closed, retry next scan
            # (was: t["min_lock_075_done"] = True — removed by patch)
            _lml_log_event = "MIN_LOCK_075_LIVE_NO_SYNC_CTX"
            t["exchange_sl_sync_pending"] = t.get("sl")

        out["lml_log_event"] = _lml_log_event
        out["min_lock_075_done"] = t.get("min_lock_075_done", False)

    except Exception as exc:
        # Outer except (line 5819)
        out["exception_caught"] = str(exc)
        out["min_lock_075_done"] = t.get("min_lock_075_done", False)

    return t, out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def base_trade(**overrides):
    t = {
        "side": "LONG",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "entry": 100.0,
        "entry_real": 100.0,
        "sl_init": 90.0,
        "sl": 90.0,
        "max_profit_r": 0.85,
        "min_lock_075_done": False,
        "symbol": "BTCUSDT",
        "exchange_sl_id": "OLD_ORDER_123",
        "exchange_qty": 0.01,
        "exchange_sl_price_confirmed": 90.0,
    }
    t.update(overrides)
    return t


def section(title):
    print(f"\n{'='*68}")
    print(f"  {title}")
    print('='*68)


# ---------------------------------------------------------------------------
# CASE 1: sync_result=True — SL moves, min_lock_075_done set
# ---------------------------------------------------------------------------
section("CASE 1: sync_result=True  →  sl_moved=True, done=True")

sync_mock, calls = make_sync_mock(True)
t_in = base_trade(side="LONG", sl=90.0)
t_out, out = run_live_minlock_block(t_in, sync_mock)

print(f"  gate_passed={out['gate_passed']}  floor={out['floor']}  sl_moved={out['sl_moved']}")
print(f"  sync_result={out['sync_result']}  log_event={out['lml_log_event']}")
print(f"  new_sl={out['new_sl']}  min_lock_075_done={out['min_lock_075_done']}")
print(f"  exchange_sl_id={t_out.get('exchange_sl_id')}  exchange_sl_price_confirmed={t_out.get('exchange_sl_price_confirmed')}")

check("C1 gate_passed",            out["gate_passed"])
check("C1 sl_moved=True",          out["sl_moved"] is True,          f"sl_moved={out['sl_moved']}")
check("C1 new_sl=107.5",           abs(out["new_sl"] - 107.5) < 1e-9, f"new_sl={out['new_sl']}")
check("C1 min_lock_075_done=True", out["min_lock_075_done"] is True,  f"done={out['min_lock_075_done']}")
check("C1 log_event=SYNC_OK",      out["lml_log_event"] == "MIN_LOCK_075_LIVE_SYNC_OK",
      f"event={out['lml_log_event']}")
check("C1 exchange_sl_id updated", t_out.get("exchange_sl_id") == "NEW_ORDER_ID_MOCK",
      f"id={t_out.get('exchange_sl_id')}")
check("C1 exchange_sl_price_confirmed updated",
      abs(t_out.get("exchange_sl_price_confirmed", 0) - 107.5) < 1e-9,
      f"confirmed={t_out.get('exchange_sl_price_confirmed')}")
check("C1 sync_pending cleared",   "exchange_sl_sync_pending" not in t_out,
      f"pending key present: {'exchange_sl_sync_pending' in t_out}")
check("C1 sync called once",       len(calls) == 1,                   f"calls={len(calls)}")

# ---------------------------------------------------------------------------
# CASE 2: sync_result=False — min_lock_075_done NOT set, retry guaranteed
# ---------------------------------------------------------------------------
section("CASE 2: sync_result=False  →  done=False, sync_pending set, retry safe")

sync_mock, calls = make_sync_mock(False)
t_in = base_trade(side="LONG", sl=90.0)
t_out, out = run_live_minlock_block(t_in, sync_mock)

print(f"  sync_result={out['sync_result']}  log_event={out['lml_log_event']}")
print(f"  min_lock_075_done={out['min_lock_075_done']}  new_sl={out['new_sl']}")
print(f"  exchange_sl_sync_pending={t_out.get('exchange_sl_sync_pending')}")

check("C2 gate_passed",             out["gate_passed"])
check("C2 sl_moved=True (local)",   out["sl_moved"] is True,           f"sl_moved={out['sl_moved']}")
check("C2 min_lock_075_done=False", out["min_lock_075_done"] is False,  f"done={out['min_lock_075_done']}")
check("C2 log_event=SYNC_FAILED",   out["lml_log_event"] == "MIN_LOCK_075_LIVE_SYNC_FAILED",
      f"event={out['lml_log_event']}")
check("C2 sync_pending set",        t_out.get("exchange_sl_sync_pending") == 107.5,
      f"pending={t_out.get('exchange_sl_sync_pending')}")

# Simulate retry: second scan — gate still fires because done=False
sync_mock2, calls2 = make_sync_mock(True)
t_in2 = deepcopy(t_out)
t_out2, out2 = run_live_minlock_block(t_in2, sync_mock2)
check("C2 retry: gate fires again (done=False allows retry)",
      out2["gate_passed"],           f"gate_passed={out2['gate_passed']}")
check("C2 retry: min_lock_075_done=True after success",
      out2["min_lock_075_done"] is True, f"done={out2['min_lock_075_done']}")
print(f"  [RETRY] gate_passed={out2['gate_passed']}  done={out2['min_lock_075_done']}")

# ---------------------------------------------------------------------------
# CASE 3: sync_result=None — PATCHED: fail-closed, done=False, retry safe
# ---------------------------------------------------------------------------
section("CASE 3: sync_result=None  →  PATCHED: done=False, sync_pending set, retry safe")

sync_mock, calls = make_sync_mock(None)
t_in = base_trade(side="LONG", sl=90.0)
t_out, out = run_live_minlock_block(t_in, sync_mock)

print(f"  sync_result={out['sync_result']}  log_event={out['lml_log_event']}")
print(f"  min_lock_075_done={out['min_lock_075_done']}")
print(f"  exchange_sl_sync_pending={t_out.get('exchange_sl_sync_pending')}")
print(f"  [PATCH] None now treated as SYNC_FAILED: no done flag, retry next scan")

check("C3 log_event=NO_SYNC_CTX",
      out["lml_log_event"] == "MIN_LOCK_075_LIVE_NO_SYNC_CTX",
      f"event={out['lml_log_event']}")
check("C3 done=False (PATCHED — fail-closed)",
      out["min_lock_075_done"] is False,
      f"done={out['min_lock_075_done']}")
check("C3 sync_pending set (PATCHED)",
      t_out.get("exchange_sl_sync_pending") == 107.5,
      f"pending={t_out.get('exchange_sl_sync_pending')}")

# Retry: gate fires again because done=False
sync_mock_c3r, calls_c3r = make_sync_mock(True)
t_in_c3r = deepcopy(t_out)
t_out_c3r, out_c3r = run_live_minlock_block(t_in_c3r, sync_mock_c3r)
check("C3 retry: gate fires again after None",
      out_c3r["gate_passed"],           f"gate_passed={out_c3r['gate_passed']}")
check("C3 retry: done=True after sync succeeds",
      out_c3r["min_lock_075_done"] is True, f"done={out_c3r['min_lock_075_done']}")
print(f"  [RETRY] gate_passed={out_c3r['gate_passed']}  done={out_c3r['min_lock_075_done']}")

# ---------------------------------------------------------------------------
# CASE 4: Exception in sync — no crash, done not set, retry safe
# ---------------------------------------------------------------------------
section("CASE 4: Exception raised inside sync  →  caught, done=False, retry safe")

sync_mock, calls = make_sync_mock(True, raise_exc=RuntimeError("exchange timeout"))
t_in = base_trade(side="LONG", sl=90.0)
t_out, out = run_live_minlock_block(t_in, sync_mock)

print(f"  exception_caught={out['exception_caught']}")
print(f"  min_lock_075_done={out['min_lock_075_done']}  gate_passed={out['gate_passed']}")

check("C4 no crash",               out["exception_caught"] is not None,
      f"caught={out['exception_caught']}")
check("C4 min_lock_075_done=False", out["min_lock_075_done"] is False,
      f"done={out['min_lock_075_done']}")

# Retry after exception: gate fires again
sync_mock3, calls3 = make_sync_mock(True)
t_in3 = deepcopy(t_out)
t_out3, out3 = run_live_minlock_block(t_in3, sync_mock3)
check("C4 retry gate fires after exception",
      out3["gate_passed"],           f"gate_passed={out3['gate_passed']}")
check("C4 retry done=True after recovery",
      out3["min_lock_075_done"] is True, f"done={out3['min_lock_075_done']}")
print(f"  [RETRY] gate_passed={out3['gate_passed']}  done={out3['min_lock_075_done']}")

# ---------------------------------------------------------------------------
# CASE 5a: No-regression — LONG, old_sl already better than floor
# ---------------------------------------------------------------------------
section("CASE 5a: No-regression LONG — old_sl=115 already above 107.5 floor")

sync_mock, calls = make_sync_mock(True)
t_in = base_trade(side="LONG", sl=115.0, max_profit_r=1.5)  # SL already above floor
t_out, out = run_live_minlock_block(t_in, sync_mock)

print(f"  floor={out['floor']}  sl_moved={out['sl_moved']}  new_sl={out['new_sl']}")
print(f"  sync_called={out['sync_called']}  sync_result={out['sync_result']}")

check("C5a gate_passed",           out["gate_passed"])
check("C5a sl_moved=False",        out["sl_moved"] is False,          f"sl_moved={out['sl_moved']}")
check("C5a new_sl unchanged=115",  abs(out["new_sl"] - 115.0) < 1e-9, f"new_sl={out['new_sl']}")
# Sync is STILL called even when sl_moved=False — this is correct: done flag must be set
# once triggered, regardless of whether SL moved. The sync confirms exchange state.
check("C5a sync still called",     out["sync_called"] is True,
      f"sync_called={out['sync_called']}")
check("C5a done=True after sync",  out["min_lock_075_done"] is True,
      f"done={out['min_lock_075_done']}")
print(f"  [Note: sync is called even when sl_moved=False — idempotent done flag path]")

# ---------------------------------------------------------------------------
# CASE 5b: No-regression — SHORT, old_sl already better
# ---------------------------------------------------------------------------
section("CASE 5b: No-regression SHORT — old_sl=80 already below 92.5 floor")

sync_mock, calls = make_sync_mock(True)
t_in = base_trade(side="SHORT", entry=100, entry_real=100, sl_init=110, sl=80.0, max_profit_r=1.5)
t_out, out = run_live_minlock_block(t_in, sync_mock)

print(f"  floor={out['floor']}  sl_moved={out['sl_moved']}  new_sl={out['new_sl']}")

check("C5b sl_moved=False",        out["sl_moved"] is False,          f"sl_moved={out['sl_moved']}")
check("C5b new_sl unchanged=80",   abs(out["new_sl"] - 80.0) < 1e-9,  f"new_sl={out['new_sl']}")
check("C5b done=True",             out["min_lock_075_done"] is True)

# ---------------------------------------------------------------------------
# CASE 6: exec_mode guard — paper/testnet should not enter live block
# ---------------------------------------------------------------------------
section("CASE 6: exec_mode=paper  →  live gate blocks entirely")

sync_mock, calls = make_sync_mock(True)
t_in = base_trade(side="LONG")
_, out = run_live_minlock_block(t_in, sync_mock, exec_mode="paper")

check("C6 gate_passed=False for paper", out["gate_passed"] is False)
check("C6 sync not called for paper",   len(calls) == 0, f"calls={len(calls)}")
check("C6 done unchanged for paper",    out["min_lock_075_done"] is False)

# ---------------------------------------------------------------------------
# CASE 7: live_smc_research_enabled=False gate
# ---------------------------------------------------------------------------
section("CASE 7: live_smc_research_enabled=False  →  gate blocks")

sync_mock, calls = make_sync_mock(True)
t_in = base_trade(side="LONG")
_, out = run_live_minlock_block(t_in, sync_mock, live_research_enabled=False)

check("C7 gate_passed=False",      out["gate_passed"] is False)
check("C7 sync not called",        len(calls) == 0, f"calls={len(calls)}")

# ---------------------------------------------------------------------------
# CASE 8: Missing exchange fields — sync returns None (PATCHED: fail-closed)
# ---------------------------------------------------------------------------
section("CASE 8: exchange_sl_id missing  →  PATCHED: done=False, sync_pending set")

# In real _sync_testnet_trailing_sl, missing exchange_sl_id returns None immediately
def sync_missing_anchor(t, ctx, old_sl=None):
    # Simulates execution.py:4447-4448
    if not t.get("exchange_sl_id"):
        return None
    return True

t_in = base_trade(side="LONG", sl=90.0)
del t_in["exchange_sl_id"]   # simulate missing anchor
t_out, out = run_live_minlock_block(t_in, sync_missing_anchor)

print(f"  sync_result={out['sync_result']}  log_event={out['lml_log_event']}")
print(f"  min_lock_075_done={out['min_lock_075_done']}")
print(f"  exchange_sl_sync_pending={t_out.get('exchange_sl_sync_pending')}")
print(f"  [PATCH] None with missing anchor: no done flag, sync_pending set, retry safe")

check("C8 gate_passed",            out["gate_passed"])
check("C8 sync_result=None",       out["sync_result"] is None, f"sync_result={out['sync_result']}")
check("C8 done=False (PATCHED — no exchange confirmation = no done)",
      out["min_lock_075_done"] is False, f"done={out['min_lock_075_done']}")
check("C8 sync_pending set (PATCHED)",
      t_out.get("exchange_sl_sync_pending") == 107.5,
      f"pending={t_out.get('exchange_sl_sync_pending')}")

# ---------------------------------------------------------------------------
# Results table
# ---------------------------------------------------------------------------

print(f"\n{'='*72}")
print(f"{'Label':<52} {'Status':<8} {'Detail'}")
print(f"{'-'*72}")
all_pass = True
warn_count = 0
for label, status, detail in results:
    marker = "" if status == PASS else " <---"
    print(f"  {label:<50} {status:<8} {detail}{marker}")
    if status == FAIL:
        all_pass = False
    if status == WARN:
        warn_count += 1

print("=" * 72)
total = len(results)
passed = sum(1 for _, s, _ in results if s == PASS)
failed = sum(1 for _, s, _ in results if s == FAIL)
warned = sum(1 for _, s, _ in results if s == WARN)
print(f"\nSUMMARY: {passed}/{total} PASS  |  {warned} WARN  |  {failed} FAIL")

# ---------------------------------------------------------------------------
# Audit summary
# ---------------------------------------------------------------------------
print("""
PRODUCTION CODE AUDIT
=====================

Q: Does _sync_testnet_trailing_sl name imply testnet-only?
   Name is historical — it handles BOTH testnet and live.
   execution.py:4444: ctx.execution_mode not in ("testnet", "live") → return None
   The live_executor.update_trailing_stop() docstring confirms live use.  ✓

Q: For live mode, does it use place-new-stop-before-cancel-old?
   YES. live_executor.py:1910-1918 documents the safety guarantee explicitly:
     1. Place new STOP_MARKET at new_stop_price
     2. If placement fails → old stop preserved unchanged → return failure
     3. If placement succeeds → cancel old stop
     4. If cancel fails → both stops exist (new protects position) → return success, cancel_ok=False
   This means: a naked position from a trailing update is IMPOSSIBLE by design.  ✓

Q: What fields must exist on trade t for sync to succeed?
   - t["exchange_sl_id"]           (line 4446: old order to cancel)
   - t["exchange_qty"]             (line 4449: qty for new stop order)
   - t["symbol"]                   (line 4461: exchange symbol)
   - t["sl"]                       (line 4454: new_stop_price)
   - t["side"]                     (line 4453: BUY/SELL direction)
   - ctx.execution_mode in ("testnet","live")  (line 4444)
   If any are absent → sync returns None (early return, not False)

Q: Is there a scenario where local SL changes but exchange SL does not?
   YES — two paths, both now handled safely:
   Path A: sync_result=False (exchange API failure)
     → local t["sl"] was mutated to floor
     → exchange SL unchanged (old stop still live)
     → exchange_sl_sync_pending set for audit resync
     → min_lock_075_done=False → retry next scan  ← SAFE
   Path B: sync_result=None (missing exchange anchor) — PATCHED
     → local t["sl"] was mutated to floor
     → exchange SL was NEVER called
     → exchange_sl_sync_pending set (same as False path)
     → min_lock_075_done=False → retry next scan  ← NOW SAFE (was: done=True)

PATCH APPLIED (execution.py:5767-5771)
=======================================
Before:
    else:
        _lml_log_event = "MIN_LOCK_075_LIVE_NO_SYNC_CTX"
        t["min_lock_075_done"] = True   ← marked done without exchange confirmation

After:
    else:
        _lml_log_event = "MIN_LOCK_075_LIVE_NO_SYNC_CTX"
        t["exchange_sl_sync_pending"] = t.get("sl")
        # min_lock_075_done NOT set — retry next scan until True confirmed

None path is now consistent with False path: fail-closed, retry-safe.
""")

sys.exit(0 if (failed == 0) else 1)
