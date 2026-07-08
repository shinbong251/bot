#!/usr/bin/env python3
"""
Simulator: SL-AUDIT forced-resync immediate-trigger guard.

Verifies the safety hardening added to execution.audit_exchange_sl's forced
"exchange_sl_sync_pending" resync path. That path places t["sl"] on the exchange
via _sync_testnet_trailing_sl(). Previously it was called WITHOUT current_price,
which silently bypassed that function's inner immediate-trigger guard. The patch:

  - reads the best already-available current price from the trade
    (_sl_audit_resync_current_price); no ticker/mark is fetched here,
  - blocks the forced resync (no exchange order touched) when the stop is
    immediately triggerable, logging SL_AUDIT_RESYNC_IMMEDIATE_TRIGGER_GUARD,
  - when no price is available, preserves prior behavior and logs
    SL_AUDIT_RESYNC_PRICE_UNAVAILABLE_GUARD_BYPASS.

NO real exchange orders are placed: a fake executor records update_trailing_stop
calls so we can assert nothing was sent in the blocked cases.

Run:
  PYTHONIOENCODING=utf-8 python3 scripts/debug/sim_sl_audit_resync_immediate_trigger_guard.py
"""

import io
import os
import sys
import contextlib

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import execution  # noqa: E402


# ---------------------------------------------------------------------------
# Fakes (no real network / exchange / disk)
# ---------------------------------------------------------------------------
class FakeExecutor:
    """Records update_trailing_stop calls; reports stops as active on exchange."""

    def __init__(self):
        self.update_calls = []

    def update_trailing_stop(self, symbol, entry_side, qty, new_stop_price, old_order_id):
        self.update_calls.append({
            "symbol": symbol,
            "entry_side": entry_side,
            "qty": qty,
            "new_stop_price": new_stop_price,
            "old_order_id": old_order_id,
        })
        return {"success": True, "new_order_id": "NEW_ALGO_1", "cancel_ok": True}

    def query_algo_order(self, symbol, sl_id):
        # Return an active order whose trigger matches local sl so the audit's
        # post-resync verify path does not schedule a follow-up mismatch resync.
        return {"algoId": sl_id, "triggerPrice": _CURRENT_LOCAL_SL.get(symbol)}

    def get_open_algo_orders(self, symbol):
        return [{"symbol": symbol, "algoId": "EXISTING_ALGO"}]


class Ctx:
    def __init__(self, trades):
        self.execution_mode = "live"
        self.trades = trades
        self.state_file = os.path.join(
            "/tmp/claude-0/-opt-bot",
            "sim_sl_audit_resync_state.json",
        )
        self.mode_prefix = "[SIM]"


# Per-symbol local sl mirror so FakeExecutor.query_algo_order can match.
_CURRENT_LOCAL_SL = {}

_FAKE = FakeExecutor()
_orig_resolve = execution._resolve_exchange_executor
_orig_save = execution.save_open_trades
_orig_send = execution.send_telegram


def _install_stubs():
    execution._resolve_exchange_executor = lambda exec_mode: _FAKE
    execution.save_open_trades = lambda *a, **k: None
    execution.send_telegram = lambda *a, **k: None


def _restore_stubs():
    execution._resolve_exchange_executor = _orig_resolve
    execution.save_open_trades = _orig_save
    execution.send_telegram = _orig_send


def make_trade(symbol, side, sl, current_price=None, sl_init=None, entry=None):
    _CURRENT_LOCAL_SL[symbol] = sl
    t = {
        "symbol": symbol,
        "side": side,
        "status": "OPEN",
        "owner": "bot",
        "sl": sl,
        "sl_init": sl_init if sl_init is not None else sl,
        "entry": entry if entry is not None else sl,
        "entry_real": entry if entry is not None else sl,
        "exchange_sl_id": "OLD_ALGO_1",
        "exchange_qty": 100.0,
        "exchange_sl_price_confirmed": sl,
        "exchange_sl_sync_pending": sl,
    }
    if current_price is not None:
        t["current_price_used_in_decision"] = current_price
    return t


def run_audit(trade):
    """Run the real audit_exchange_sl over one trade, capturing stdout."""
    _FAKE.update_calls.clear()
    ctx = Ctx([trade])
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        execution.audit_exchange_sl(ctx)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------
results = []

_install_stubs()


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))


# ===== A. SHORT, target SL above current price => safe resync allowed =====
print("Case A: SHORT, stop ABOVE price (safe) -> resync allowed")
out = run_audit(make_trade("AAAUSDT", "SHORT", sl=105.0, current_price=100.0))
check("A order placed", len(_FAKE.update_calls) == 1, f"update_calls={len(_FAKE.update_calls)}")
check("A no block log", "SL_AUDIT_RESYNC_IMMEDIATE_TRIGGER_GUARD" not in out)

# ===== B. SHORT, target SL below/equal current price => blocked =====
print("Case B: SHORT, stop BELOW price (triggerable) -> blocked")
out = run_audit(make_trade("BBBUSDT", "SHORT", sl=100.0, current_price=105.0))
check("B no order placed", len(_FAKE.update_calls) == 0, f"update_calls={len(_FAKE.update_calls)}")
check("B block log present", "SL_AUDIT_RESYNC_IMMEDIATE_TRIGGER_GUARD" in out)
out_eq = run_audit(make_trade("BBEUSDT", "SHORT", sl=100.0, current_price=100.0))
check("B(equal) no order placed", len(_FAKE.update_calls) == 0, f"update_calls={len(_FAKE.update_calls)}")
check("B(equal) block log present", "SL_AUDIT_RESYNC_IMMEDIATE_TRIGGER_GUARD" in out_eq)

# ===== C. LONG, target SL below current price => safe resync allowed =====
print("Case C: LONG, stop BELOW price (safe) -> resync allowed")
out = run_audit(make_trade("CCCUSDT", "LONG", sl=100.0, current_price=105.0))
check("C order placed", len(_FAKE.update_calls) == 1, f"update_calls={len(_FAKE.update_calls)}")
check("C no block log", "SL_AUDIT_RESYNC_IMMEDIATE_TRIGGER_GUARD" not in out)

# ===== D. LONG, target SL above/equal current price => blocked =====
print("Case D: LONG, stop ABOVE price (triggerable) -> blocked")
out = run_audit(make_trade("DDDUSDT", "LONG", sl=105.0, current_price=100.0))
check("D no order placed", len(_FAKE.update_calls) == 0, f"update_calls={len(_FAKE.update_calls)}")
check("D block log present", "SL_AUDIT_RESYNC_IMMEDIATE_TRIGGER_GUARD" in out)
out_eq = run_audit(make_trade("DDEUSDT", "LONG", sl=105.0, current_price=105.0))
check("D(equal) no order placed", len(_FAKE.update_calls) == 0, f"update_calls={len(_FAKE.update_calls)}")
check("D(equal) block log present", "SL_AUDIT_RESYNC_IMMEDIATE_TRIGGER_GUARD" in out_eq)

# ===== E. current_price unavailable => existing behavior preserved + warn =====
print("Case E: no price available -> BYPASS warn, prior behavior (resync proceeds)")
out = run_audit(make_trade("EEEUSDT", "SHORT", sl=105.0, current_price=None))
check("E order placed (existing behavior)", len(_FAKE.update_calls) == 1, f"update_calls={len(_FAKE.update_calls)}")
check("E bypass warn logged", "SL_AUDIT_RESYNC_PRICE_UNAVAILABLE_GUARD_BYPASS" in out)
check("E no block log", "SL_AUDIT_RESYNC_IMMEDIATE_TRIGGER_GUARD" not in out)

# ===== F. Normal trailing path unchanged =====
# Directly exercise _sync_testnet_trailing_sl (the trailing call site already
# passes current_price). Confirms the patch did not alter that function.
print("Case F: normal trailing _sync path unchanged")
_FAKE.update_calls.clear()


class _DirectCtx:
    execution_mode = "live"
    mode_prefix = "[SIM]"


# F1: safe price -> sync succeeds, order placed
t_f_safe = {"symbol": "FFFUSDT", "side": "SHORT", "sl": 105.0,
            "exchange_sl_id": "OLD", "exchange_qty": 10.0}
res_safe = execution._sync_testnet_trailing_sl(t_f_safe, _DirectCtx(), old_sl=110.0, current_price=100.0)
check("F1 trailing safe -> True", res_safe is True, f"res={res_safe}")
check("F1 trailing safe order placed", len(_FAKE.update_calls) == 1, f"update_calls={len(_FAKE.update_calls)}")

# F2: triggerable price -> inner guard still blocks (no order), returns False
_FAKE.update_calls.clear()
t_f_trig = {"symbol": "FFGUSDT", "side": "SHORT", "sl": 100.0,
            "exchange_sl_id": "OLD", "exchange_qty": 10.0}
res_trig = execution._sync_testnet_trailing_sl(t_f_trig, _DirectCtx(), old_sl=110.0, current_price=105.0)
check("F2 trailing triggerable -> False", res_trig is False, f"res={res_trig}")
check("F2 trailing triggerable no order", len(_FAKE.update_calls) == 0, f"update_calls={len(_FAKE.update_calls)}")

# ===== G. NFP-like BE resync remains allowed =====
# SHORT, local SL at breakeven (above current price) -> not triggerable.
print("Case G: NFP-like BE resync (SHORT, BE above price) -> allowed")
out = run_audit(make_trade(
    "NFPUSDT", "SHORT",
    sl=0.005586, current_price=0.005514,
    sl_init=0.005816, entry=0.005586,
))
check("G order placed", len(_FAKE.update_calls) == 1, f"update_calls={len(_FAKE.update_calls)}")
check("G no block log", "SL_AUDIT_RESYNC_IMMEDIATE_TRIGGER_GUARD" not in out)
# G2: NFP BE resync with no price -> BYPASS, still resyncs (prior behavior)
out2 = run_audit(make_trade(
    "NFPUSDT", "SHORT",
    sl=0.005586, current_price=None,
    sl_init=0.005816, entry=0.005586,
))
check("G2 order placed (no price)", len(_FAKE.update_calls) == 1, f"update_calls={len(_FAKE.update_calls)}")
check("G2 bypass warn logged", "SL_AUDIT_RESYNC_PRICE_UNAVAILABLE_GUARD_BYPASS" in out2)


_restore_stubs()

# ---------------------------------------------------------------------------
print()
_failed = [n for n, ok, _ in results if not ok]
if _failed:
    print(f"RESULT: FAIL ({len(_failed)}/{len(results)} checks failed)")
    for n in _failed:
        print(f"  - {n}")
    sys.exit(1)
print(f"RESULT: PASS ({len(results)}/{len(results)} checks passed)")
print("No real exchange orders touched (FakeExecutor only).")
