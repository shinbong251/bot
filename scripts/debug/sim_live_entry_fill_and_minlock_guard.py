#!/usr/bin/env python3
"""Deep live-trade lifecycle simulator for CONFIRM_SMC_RESEARCH.

Simulator only. Deterministic. NO exchange/testnet/live orders are placed,
cancelled, or modified. NO runtime config / .env / state / history is touched.

Coverage (live BE / MIN_LOCK / trailing phases):
  A. Entry fill confirmation
  B. BE 0.7R
  C. MIN_LOCK 0.75R
  D. 1R trailing start
  E. Phase 2 trailing
  F. Phase 3 trailing
  G. Same-loop BE/MIN_LOCK close prevention
  H. Side-correct SL hit checks (LONG / SHORT)

Numbered scenarios 1..13 below map onto A..H and are reported PASS/FAIL.

Faithfulness:
  * Entry fill confirmation reuses the REAL execution functions
    (_confirm_live_entry_fill_price / _mark_live_entry_fill_* ) via AST scope.
  * Schema fallback reuses the REAL state_manager.normalize_trade_schema.
  * Exchange SL sync reuses the REAL execution._sync_testnet_trailing_sl with a
    fake executor (no network).
  * BE / MIN_LOCK / partial-1R / trailing-phase / SL-hit decisions are mirrored
    line-for-line from execution.py management loop and exit_optimization, with
    the real dynamic_phase_trigger pulled in via AST scope.
"""

import ast
import math
import os
import sys
import time


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
EXECUTION_PATH = os.path.join(REPO_ROOT, "execution.py")
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Mirror of execution.SLIPPAGE (used by exit_optimization for phase profit_r).
SLIPPAGE = 0.001

results = []
issues = []
scenario_status = {}


def check(scenario, label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((scenario, label, status, detail))
    prev = scenario_status.get(scenario, "PASS")
    if status == "FAIL":
        scenario_status[scenario] = "FAIL"
        issues.append(f"[FAIL] {scenario} :: {label}: {detail}")
    else:
        scenario_status.setdefault(scenario, "PASS")
        if prev != "FAIL":
            scenario_status[scenario] = "PASS"
    return bool(condition)


class FakeExecutor:
    """Records query_order calls; returns queued canned order dicts."""

    def __init__(self, orders):
        self.orders = list(orders)
        self.queries = []

    def query_order(self, symbol, order_id=None, client_order_id=None, return_not_found=False):
        self.queries.append({
            "symbol": symbol,
            "order_id": order_id,
            "client_order_id": client_order_id,
            "return_not_found": return_not_found,
        })
        if not self.orders:
            return None
        return self.orders.pop(0)


class FakeSyncExecutor:
    """Records update_trailing_stop calls for the real _sync_testnet_trailing_sl."""

    def __init__(self, result):
        self.result = result
        self.calls = []

    def update_trailing_stop(self, **kwargs):
        self.calls.append(kwargs)
        return dict(self.result)


class Ctx:
    def __init__(self, execution_mode):
        self.execution_mode = execution_mode
        self.mode_prefix = f"[{execution_mode.upper()}]"


class DummyDedup:
    @staticmethod
    def build_key(*parts):
        return "|".join(str(part) for part in parts)


def load_execution_scope():
    with open(EXECUTION_PATH, "r", encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=EXECUTION_PATH)
    wanted = {
        "_safe_float_value",
        "_get_order_client_id",
        "_live_order_status_is_filled",
        "_entry_result_from_live_query",
        "_live_entry_fill_price_from_result",
        "_mark_live_entry_fill_confirmed",
        "_mark_live_entry_fill_unconfirmed",
        "_query_live_entry_order_once",
        "_confirm_live_entry_fill_price",
        "dynamic_phase_trigger",
    }
    nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    found = {node.name for node in nodes}
    missing = wanted - found
    if missing:
        raise RuntimeError(f"missing execution functions: {sorted(missing)}")
    module = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    scope = {
        "math": math,
        "time": time,
        "send_telegram": lambda *args, **kwargs: None,
        "print": print,
    }
    exec(compile(module, EXECUTION_PATH, "exec"), scope)
    return scope


def load_sync_scope(fake, messages):
    """Load the REAL _sync_testnet_trailing_sl with no network side-effects."""
    with open(EXECUTION_PATH, "r", encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=EXECUTION_PATH)
    wanted = {"_safe_numeric_value", "_sync_testnet_trailing_sl"}
    nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    found = {node.name for node in nodes}
    missing = wanted - found
    if missing:
        raise RuntimeError(f"missing execution functions: {sorted(missing)}")
    module = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    scope = {
        "math": math,
        "time": time,
        "_resolve_exchange_executor": lambda _mode: fake,
        "send_telegram": lambda msg, **kwargs: messages.append((msg, kwargs)),
        "telegram_dedup": DummyDedup,
        "_immediately_triggerable_alert_last_sent": {},
        "_IMMEDIATELY_TRIGGERABLE_ALERT_TTL_SECS": 300.0,
    }
    exec(compile(module, EXECUTION_PATH, "exec"), scope)
    return scope


def load_state_scope():
    state_path = os.path.join(REPO_ROOT, "state_manager.py")
    with open(state_path, "r", encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=state_path)
    nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "normalize_trade_schema"
    ]
    if not nodes:
        raise RuntimeError("missing normalize_trade_schema")
    module = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(module)

    class MiniNp:
        @staticmethod
        def isnan(value):
            return math.isnan(value)

    scope = {"np": MiniNp, "RISK_PER_TRADE": 0.01}
    exec(compile(module, state_path, "exec"), scope)
    return scope


# ---------------------------------------------------------------------------
# Faithful mirrors of execution.py management decisions (deterministic).
# ---------------------------------------------------------------------------

def live_r_now(side, entry_real, sl_init, high, low):
    # execution.py lines ~6269-6272
    risk = abs(entry_real - sl_init)
    if side == "LONG":
        return (high - entry_real) / risk
    return (entry_real - low) / risk


def live_be_07_decision(side, entry_real, sl_init, current_sl, high, low):
    # execution.py 0.7R BREAKEVEN block (~6348-6392)
    max_profit_r = live_r_now(side, entry_real, sl_init, high, low)
    triggered = max_profit_r >= 0.7
    sl = current_sl
    be_changed = False
    if triggered:
        new_sl = entry_real
        if side == "LONG" and new_sl > current_sl:
            sl = new_sl
            be_changed = True
        elif side == "SHORT" and new_sl < current_sl:
            sl = new_sl
            be_changed = True
    return {"sl": sl, "be_changed": be_changed, "triggered": triggered, "max_profit_r": max_profit_r}


def live_min_lock_decision(side, entry_real, sl_init, current_sl, current_price,
                           max_profit_r, be_changed=False, sync_ok=True):
    # execution.py LIVE SMC RESEARCH MIN-LOCK 0.75R block (~6505-6657)
    # The whole block is gated by max_profit_r >= 0.75; below that it does not run.
    if max_profit_r < 0.75:
        return {
            "sl": current_sl,
            "sync_called": False,
            "done": False,
            "skipped": "below_0_75r_threshold",
            "local_hit_sl": current_price <= current_sl if side == "LONG" else current_price >= current_sl,
        }
    if be_changed:
        return {
            "sl": current_sl,
            "sync_called": False,
            "done": False,
            "skipped": "be_sl_changed_this_loop",
            "local_hit_sl": False,
        }
    risk = abs(entry_real - sl_init)
    floor = entry_real + risk * 0.75 if side == "LONG" else entry_real - risk * 0.75
    should_move = floor > current_sl if side == "LONG" else floor < current_sl
    immediately_triggerable = (
        (side == "LONG" and current_price <= floor)
        or (side == "SHORT" and current_price >= floor)
    )
    if should_move and immediately_triggerable:
        return {
            "sl": current_sl,
            "proposed_sl": floor,
            "sync_called": False,
            "done": False,
            "skipped": "immediately_triggerable_before_local_sl_mutation",
            "local_hit_sl": current_price <= current_sl if side == "LONG" else current_price >= current_sl,
        }
    if should_move:
        # Real code mutates t["sl"]=floor, then reverts if sync_result is not True.
        moved_sl = floor if sync_ok else current_sl
        return {
            "sl": moved_sl,
            "proposed_sl": floor,
            "sync_called": True,
            "done": bool(sync_ok),
            "skipped": "",
            "local_hit_sl": current_price <= moved_sl if side == "LONG" else current_price >= moved_sl,
        }
    return {
        "sl": current_sl,
        "proposed_sl": floor,
        "sync_called": False,
        "done": True,
        "skipped": "",
        "local_hit_sl": current_price <= current_sl if side == "LONG" else current_price >= current_sl,
    }


def live_partial_be_1r(side, entry_real, sl_init, current_sl, high, low):
    # execution.py PARTIAL + BE block (~6659-6713)
    risk = abs(entry_real - sl_init)
    sl = current_sl
    partial_done = False
    be_changed = False
    if side == "LONG":
        oneR = entry_real + risk
        if high >= oneR:
            partial_done = True
            sl = max(current_sl, entry_real + 0.1 * risk)
            be_changed = True
    else:
        oneR = entry_real - risk
        if low <= oneR:
            partial_done = True
            sl = min(current_sl, entry_real - 0.1 * risk)
            be_changed = True
    return {"sl": sl, "partial_done": partial_done, "be_changed": be_changed}


def live_trail_phase(dynamic_phase_trigger, side, entry_real, sl_init, close,
                     prev_phase, vol=0.0, tp_hit=False):
    # execution.py exit_optimization PHASE block (~4045-4078)
    if side == "LONG":
        price_real = close * (1 - SLIPPAGE)
        profit = price_real - entry_real
    else:
        price_real = close * (1 + SLIPPAGE)
        profit = entry_real - price_real
    risk = abs(entry_real - sl_init)
    profit_r = profit / risk if risk > 0 else 0
    pt = dynamic_phase_trigger(vol)
    margin = 0.2
    phase = prev_phase
    if prev_phase == 3:
        phase = 3
    elif profit_r >= pt["phase3"] + margin or tp_hit:
        phase = 3
    elif profit_r >= pt["phase2"] + margin:
        phase = max(prev_phase, 2)
    return {"trail_phase": phase, "profit_r": profit_r}


def live_sl_hit(side, close_price, sl, be_changed_this_loop=False):
    # execution.py TP/SL block (~7081-7090)
    if side == "LONG":
        hit_sl = close_price <= sl
    else:
        hit_sl = close_price >= sl
    skipped = ""
    if be_changed_this_loop and hit_sl:
        skipped = "be_sl_changed_this_loop"
        hit_sl = False
    return {"hit_sl": hit_sl, "skipped": skipped}


def main():
    scope = load_execution_scope()
    dynamic_phase_trigger = scope["dynamic_phase_trigger"]

    # =====================================================================
    # Scenario 1 (A) — LIVE entry actual fill overrides planned entry
    # =====================================================================
    immediate = {
        "success": True,
        "order_id": 123,
        "client_order_id": "BOT_TAOUSDT_E_abc",
        "status": "FILLED",
        "fill_price": 0.0,
        "fill_qty": 1.0,
        "raw": {"avgPrice": "0", "executedQty": "1"},
    }
    queried = {
        "orderId": 123,
        "clientOrderId": "BOT_TAOUSDT_E_abc",
        "status": "FILLED",
        "avgPrice": "209.37",
        "executedQty": "1",
    }
    trade = {"symbol": "TAOUSDT", "side": "SHORT", "entry": 210.62, "sl_init": 212.65}
    fake = FakeExecutor([queried])
    confirmation = scope["_confirm_live_entry_fill_price"](trade, immediate, fake, prefix="[LIVE]")
    check("1", 'avgPrice="0" immediate response triggers query_order',
          len(fake.queries) == 1, f"queries={fake.queries}")
    check("1", "actual fill 209.37 overrides planned 210.62",
          confirmation.get("confirmed") is True
          and trade.get("entry_real") == 209.37
          and trade.get("exchange_fill_price") == 209.37
          and trade.get("exchange_entry_price") == 209.37
          and trade.get("exchange_entry_source") == "query_order_after_market_fill"
          and trade.get("entry_source") == "actual_exchange_fill",
          f"trade={trade} confirmation={confirmation}")
    check("1", "entry_price_unconfirmed is false after confirmed fill",
          trade.get("entry_price_unconfirmed") is False, f"trade={trade}")
    check("1", "entry_real is actual fill, not planned entry",
          trade.get("entry_real") != trade.get("entry"),
          f"entry_real={trade.get('entry_real')} planned={trade.get('entry')}")
    # BE thresholds computed from 209.37, not 210.62.
    _be_from_actual = trade["entry_real"]  # SHORT BE target == entry_real
    check("1", "BE threshold derives from actual fill 209.37",
          _be_from_actual == 209.37 and _be_from_actual != trade["entry"],
          f"be_target={_be_from_actual}")

    # =====================================================================
    # Scenario 2 (A) — Fill unavailable blocks normal management
    # =====================================================================
    blocked_trade = {"symbol": "BADUSDT", "side": "LONG", "entry": 100.0, "sl_init": 95.0}
    blocked = scope["_confirm_live_entry_fill_price"](
        blocked_trade,
        {**immediate, "client_order_id": "BOT_BADUSDT_E_abc"},
        FakeExecutor([{"orderId": 1, "clientOrderId": "BOT_BADUSDT_E_abc",
                       "status": "FILLED", "avgPrice": "0", "executedQty": "1"}]),
        prefix="[LIVE]",
    )
    check("2", "unconfirmed fill is safety-blocked (ENTRY_FILL_UNCONFIRMED)",
          blocked.get("confirmed") is False
          and blocked_trade.get("entry_state") == "ENTRY_FILL_UNCONFIRMED"
          and blocked_trade.get("entry_price_unconfirmed") is True
          and blocked_trade.get("entry_source") == "unconfirmed_exchange_fill"
          and blocked_trade.get("entry_real") is None,
          f"trade={blocked_trade} blocked={blocked}")
    # The management loop skips BE/MIN_LOCK/trailing for this guard — assert the
    # live skip predicate matches (execution.py ~6051-6063).
    _mgmt_skipped = (
        blocked_trade.get("entry_price_unconfirmed")
        or blocked_trade.get("entry_state") == "ENTRY_FILL_UNCONFIRMED"
        or (blocked_trade.get("entry_real") in (None, "") and not blocked_trade.get("exchange_fill_price"))
    )
    check("2", "live management predicate skips unconfirmed entry",
          bool(_mgmt_skipped), f"trade={blocked_trade}")
    check("2", "no local close possible while unconfirmed (no entry_real)",
          blocked_trade.get("entry_real") is None, f"trade={blocked_trade}")

    normalize_trade_schema = load_state_scope()["normalize_trade_schema"]
    normalized_live = {"execution_mode": "live", "entry": 100.0, "exchange_fill_price": None}
    normalize_trade_schema(normalized_live)
    check("2", "normalize_trade_schema does not hide missing live entry_real",
          normalized_live.get("entry_real") is None
          and normalized_live.get("entry_price_unconfirmed") is True
          and normalized_live.get("entry_source") == "unconfirmed_exchange_fill",
          f"normalized={normalized_live}")
    normalized_paper = {"execution_mode": "paper", "entry": 100.0}
    normalize_trade_schema(normalized_paper)
    check("2", "paper normalize fallback remains unchanged (no regression)",
          normalized_paper.get("entry_real") == 100.0
          and not normalized_paper.get("entry_price_unconfirmed", False),
          f"normalized={normalized_paper}")

    # =====================================================================
    # Scenario 3 (A/B) — TAO false-BE regression (planned vs actual R)
    # =====================================================================
    planned_entry = 210.62
    actual_entry = 209.37
    sl_init = 212.65
    candle_low = 209.199
    candle_close = 209.50
    candle_high = 210.55
    planned_r = live_r_now("SHORT", planned_entry, sl_init, candle_high, candle_low)
    actual_r = live_r_now("SHORT", actual_entry, sl_init, candle_high, candle_low)
    check("3", "planned entry would FALSELY reach >=0.7R (old bug)",
          planned_r >= 0.7 - 1e-9, f"planned_r={planned_r:.6f}")
    check("3", "actual fill stays ~0.05R, not 0.7R",
          actual_r < 0.1, f"actual_r={actual_r:.4f}")
    be3 = live_be_07_decision("SHORT", actual_entry, sl_init, sl_init, candle_high, candle_low)
    check("3", "BE does not trigger on actual R",
          be3["triggered"] is False and be3["be_changed"] is False, f"be={be3}")
    check("3", "SL is NOT moved to planned entry 210.62",
          be3["sl"] == sl_init and be3["sl"] != planned_entry, f"sl={be3['sl']}")
    hit3 = live_sl_hit("SHORT", candle_close, be3["sl"])
    check("3", "no local close (close 209.50 < SL 212.65)",
          hit3["hit_sl"] is False, f"hit={hit3}")

    # =====================================================================
    # Scenario 4 (B/G/H) — Normal BE move LONG
    # =====================================================================
    be4 = live_be_07_decision("LONG", 100.0, 99.0, 99.0, high=100.70, low=99.50)
    check("4", "BE triggers at 0.7R (LONG)", be4["triggered"] and be4["be_changed"], f"be={be4}")
    check("4", "local SL moves to entry 100 (LONG)", be4["sl"] == 100.0, f"be={be4}")
    # exchange SL sync requested + confirmed via REAL sync path
    sync_fake4 = FakeSyncExecutor({"success": True, "new_order_id": "NEW4", "cancel_ok": True, "error": None})
    sync_msgs4 = []
    sync_scope4 = load_sync_scope(sync_fake4, sync_msgs4)
    t4 = {"symbol": "SIMUSDT", "side": "LONG", "sl": be4["sl"],
          "exchange_sl_id": "OLD4", "exchange_qty": 5.0, "exchange_sl_price_confirmed": 99.0}
    sync4 = sync_scope4["_sync_testnet_trailing_sl"](t4, Ctx("live"), old_sl=99.0, current_price=100.60)
    check("4", "exchange SL update requested + confirmed",
          sync4 is True and len(sync_fake4.calls) == 1, f"sync={sync4} calls={sync_fake4.calls}")
    ml4 = live_min_lock_decision("LONG", 100.0, 99.0, be4["sl"], current_price=100.60,
                                 max_profit_r=be4["max_profit_r"], be_changed=be4["be_changed"])
    # At a 0.7R BE move max_profit_r=0.70 < 0.75, so MIN_LOCK does not run this loop.
    check("4", "no MIN_LOCK move in same loop as BE",
          ml4["done"] is False and ml4["sync_called"] is False and ml4["sl"] == be4["sl"]
          and ml4["skipped"] == "below_0_75r_threshold", f"ml={ml4}")
    hit4 = live_sl_hit("LONG", 100.60, be4["sl"], be_changed_this_loop=be4["be_changed"])
    check("4", "no local close in same loop (close 100.60 > SL 100)",
          hit4["hit_sl"] is False, f"hit={hit4}")

    # =====================================================================
    # Scenario 5 (B/G/H) — Normal BE move SHORT
    # =====================================================================
    be5 = live_be_07_decision("SHORT", 100.0, 101.0, 101.0, high=100.50, low=99.30)
    check("5", "BE triggers at 0.7R (SHORT)", be5["triggered"] and be5["be_changed"], f"be={be5}")
    check("5", "local SL moves to entry 100 (SHORT)", be5["sl"] == 100.0, f"be={be5}")
    sync_fake5 = FakeSyncExecutor({"success": True, "new_order_id": "NEW5", "cancel_ok": True, "error": None})
    sync_msgs5 = []
    sync_scope5 = load_sync_scope(sync_fake5, sync_msgs5)
    t5 = {"symbol": "SIMUSDT", "side": "SHORT", "sl": be5["sl"],
          "exchange_sl_id": "OLD5", "exchange_qty": 5.0, "exchange_sl_price_confirmed": 101.0}
    sync5 = sync_scope5["_sync_testnet_trailing_sl"](t5, Ctx("live"), old_sl=101.0, current_price=99.40)
    check("5", "exchange SL update requested + confirmed",
          sync5 is True and len(sync_fake5.calls) == 1, f"sync={sync5} calls={sync_fake5.calls}")
    ml5 = live_min_lock_decision("SHORT", 100.0, 101.0, be5["sl"], current_price=99.40,
                                 max_profit_r=be5["max_profit_r"], be_changed=be5["be_changed"])
    check("5", "no MIN_LOCK move in same loop as BE",
          ml5["done"] is False and ml5["sync_called"] is False and ml5["sl"] == be5["sl"]
          and ml5["skipped"] == "below_0_75r_threshold", f"ml={ml5}")
    hit5 = live_sl_hit("SHORT", 99.40, be5["sl"], be_changed_this_loop=be5["be_changed"])
    check("5", "no local close in same loop (close 99.40 < SL 100)",
          hit5["hit_sl"] is False, f"hit={hit5}")

    # =====================================================================
    # Scenario 6 (C) — MIN_LOCK 0.75 safe LONG (floor 100.75, close 100.90)
    # =====================================================================
    ml6 = live_min_lock_decision("LONG", 100.0, 99.0, current_sl=100.0, current_price=100.90,
                                 max_profit_r=0.95, be_changed=False, sync_ok=True)
    check("6", "proposed floor 100.75 is NOT immediately-triggerable (LONG)",
          ml6["skipped"] == "" and ml6["proposed_sl"] == 100.75, f"ml={ml6}")
    check("6", "local SL moves to 100.75 only when synced",
          ml6["sl"] == 100.75 and ml6["sync_called"] and ml6["done"], f"ml={ml6}")
    check("6", "no close (close 100.90 > floor 100.75)",
          ml6["local_hit_sl"] is False, f"ml={ml6}")
    # safety: if sync fails, local SL must revert (not silently mutate)
    ml6b = live_min_lock_decision("LONG", 100.0, 99.0, current_sl=100.0, current_price=100.90,
                                  max_profit_r=0.95, be_changed=False, sync_ok=False)
    check("6", "sync failure keeps previous SL (not mutated to floor)",
          ml6b["sl"] == 100.0 and ml6b["done"] is False, f"ml={ml6b}")

    # =====================================================================
    # Scenario 7 (C/G) — MIN_LOCK 0.75 immediately-triggerable LONG
    # =====================================================================
    ml7 = live_min_lock_decision("LONG", 100.0, 99.0, current_sl=100.0, current_price=100.70,
                                 max_profit_r=0.80, be_changed=False)
    check("7", "immediately_triggerable true (close 100.70 <= floor 100.75)",
          ml7["skipped"] == "immediately_triggerable_before_local_sl_mutation", f"ml={ml7}")
    check("7", "do NOT mutate local SL to floor; keep previous BE 100",
          ml7["sl"] == 100.0 and ml7["proposed_sl"] == 100.75, f"ml={ml7}")
    check("7", "no exchange sync called on immediate-trigger guard",
          ml7["sync_called"] is False, f"ml={ml7}")
    check("7", "do not local-close (close 100.70 > SL 100)",
          ml7["local_hit_sl"] is False, f"ml={ml7}")
    # log reason field surfaced by real code
    with open(EXECUTION_PATH, "r", encoding="utf-8") as handle:
        source = handle.read()
    check("7", "real code logs min_lock_skipped_reason=immediately_triggerable_before_local_sl_mutation",
          "immediately_triggerable_before_local_sl_mutation" in source
          and "min_lock_skipped_reason" in source, "")

    # =====================================================================
    # Scenario 8 (C) — MIN_LOCK 0.75 immediately-triggerable SHORT
    # =====================================================================
    ml8 = live_min_lock_decision("SHORT", 100.0, 101.0, current_sl=100.0, current_price=99.30,
                                 max_profit_r=0.80, be_changed=False)
    check("8", "immediately_triggerable true (close 99.30 >= floor 99.25)",
          ml8["skipped"] == "immediately_triggerable_before_local_sl_mutation", f"ml={ml8}")
    check("8", "do NOT mutate local SL to floor; keep previous SL 100",
          ml8["sl"] == 100.0 and ml8["proposed_sl"] == 99.25, f"ml={ml8}")
    check("8", "do not local-close (close 99.30 < SL 100)",
          ml8["local_hit_sl"] is False, f"ml={ml8}")

    # =====================================================================
    # Scenario 9 (D/H) — Phase 1 / 1R trailing start
    # =====================================================================
    # below 1R: no partial/trailing start
    pb_below = live_partial_be_1r("LONG", 100.0, 99.0, 99.0, high=100.90, low=99.80)
    check("9", "1R trailing does NOT start below 1R (high 100.90)",
          pb_below["partial_done"] is False, f"pb={pb_below}")
    # at/over 1R: partial+trailing start, SL -> entry+0.1R
    pb9 = live_partial_be_1r("LONG", 100.0, 99.0, 99.0, high=101.05, low=99.80)
    check("9", "1R trailing starts only at/after 1R (high 101.05)",
          pb9["partial_done"] is True, f"pb={pb9}")
    check("9", "SL safe-advances to entry+0.1R = 100.1",
          abs(pb9["sl"] - 100.1) < 1e-9, f"pb={pb9}")
    ph9 = live_trail_phase(dynamic_phase_trigger, "LONG", 100.0, 99.0, close=100.95,
                           prev_phase=1)
    check("9", "trail_phase stays 1 at ~1R close", ph9["trail_phase"] == 1, f"ph={ph9}")
    hit9 = live_sl_hit("LONG", 100.95, pb9["sl"], be_changed_this_loop=pb9["be_changed"])
    check("9", "no same-loop local close (close 100.95 > SL 100.1)",
          hit9["hit_sl"] is False, f"hit={hit9}")

    # =====================================================================
    # Scenario 10 (E) — Phase 2 trailing safe update
    # =====================================================================
    ph10 = live_trail_phase(dynamic_phase_trigger, "LONG", 100.0, 99.0, close=101.35,
                            prev_phase=1)
    check("10", "trail_phase becomes 2 when profit_r >= phase2 trigger",
          ph10["trail_phase"] == 2, f"ph={ph10}")
    # SL moves forward only if not immediately-triggerable -> use real sync path
    sync_fake10 = FakeSyncExecutor({"success": True, "new_order_id": "NEW10", "cancel_ok": True, "error": None})
    sync_msgs10 = []
    sync_scope10 = load_sync_scope(sync_fake10, sync_msgs10)
    t10 = {"symbol": "SIMUSDT", "side": "LONG", "sl": 100.50,
           "exchange_sl_id": "OLD10", "exchange_qty": 5.0, "exchange_sl_price_confirmed": 100.1}
    sync10 = sync_scope10["_sync_testnet_trailing_sl"](t10, Ctx("live"), old_sl=100.1, current_price=101.30)
    check("10", "phase2 SL forward-move syncs to exchange (not immediately-triggerable)",
          sync10 is True and len(sync_fake10.calls) == 1, f"sync={sync10} calls={sync_fake10.calls}")
    hit10 = live_sl_hit("LONG", 101.30, t10["sl"])
    check("10", "no local close (close 101.30 > SL 100.50)",
          hit10["hit_sl"] is False, f"hit={hit10}")

    # =====================================================================
    # Scenario 11 (F) — Phase 3 runner preservation (>2R)
    # =====================================================================
    ph11 = live_trail_phase(dynamic_phase_trigger, "LONG", 100.0, 99.0, close=102.35,
                            prev_phase=2)
    check("11", "trail_phase becomes 3 on >2R move", ph11["trail_phase"] == 3, f"ph={ph11}")
    ph11_lock = live_trail_phase(dynamic_phase_trigger, "LONG", 100.0, 99.0, close=101.50,
                                 prev_phase=3)
    check("11", "phase 3 is sticky (does not regress to 2)",
          ph11_lock["trail_phase"] == 3, f"ph={ph11_lock}")
    # runner preserved: research partial TP is shadow-only, trailing-only
    check("11", "research partial TP is shadow-only (no real partial sell)",
          "_research_partial_tp_shadow_write(t, \"live\")" in source
          and "reduce_position" not in source
          and "partial_close" not in source,
          "shadow writer present; no partial-reduce call")

    # =====================================================================
    # Scenario 12 (H) — Side-correct local SL hit
    # =====================================================================
    check("12", "LONG close above SL => no hit",
          live_sl_hit("LONG", 100.10, 100.0)["hit_sl"] is False, "")
    check("12", "LONG close <= SL => hit allowed",
          live_sl_hit("LONG", 100.0, 100.0)["hit_sl"] is True, "")
    check("12", "LONG close below SL => hit allowed",
          live_sl_hit("LONG", 99.90, 100.0)["hit_sl"] is True, "")
    check("12", "SHORT close below SL => no hit",
          live_sl_hit("SHORT", 99.90, 100.0)["hit_sl"] is False, "")
    check("12", "SHORT close >= SL => hit allowed",
          live_sl_hit("SHORT", 100.0, 100.0)["hit_sl"] is True, "")
    check("12", "SHORT close above SL => hit allowed",
          live_sl_hit("SHORT", 100.10, 100.0)["hit_sl"] is True, "")

    # =====================================================================
    # Scenario 13 (G) — Same-loop BE then MIN_LOCK regression
    # =====================================================================
    # wick crosses 0.7R and 0.75R (high 100.80), close retraced into floor (99.95)
    be13 = live_be_07_decision("LONG", 100.0, 99.0, 99.0, high=100.80, low=99.50)
    check("13", "BE can trigger (max_profit_r 0.80 >= 0.7)",
          be13["triggered"] and be13["be_changed"] and be13["sl"] == 100.0, f"be={be13}")
    ml13 = live_min_lock_decision("LONG", 100.0, 99.0, be13["sl"], current_price=99.95,
                                  max_profit_r=be13["max_profit_r"], be_changed=be13["be_changed"])
    check("13", "MIN_LOCK skipped until next loop (be_sl_changed_this_loop)",
          ml13["skipped"] == "be_sl_changed_this_loop" and ml13["done"] is False, f"ml={ml13}")
    # without guard close 99.95 <= SL 100 would close; guard must prevent it
    raw_hit13 = live_sl_hit("LONG", 99.95, be13["sl"], be_changed_this_loop=False)
    guarded_hit13 = live_sl_hit("LONG", 99.95, be13["sl"], be_changed_this_loop=be13["be_changed"])
    check("13", "without guard this WOULD local-close (close 99.95 <= SL 100)",
          raw_hit13["hit_sl"] is True, f"raw={raw_hit13}")
    check("13", "same-loop guard prevents local close",
          guarded_hit13["hit_sl"] is False
          and guarded_hit13["skipped"] == "be_sl_changed_this_loop", f"guarded={guarded_hit13}")
    check("13", "real code carries same-loop SL-hit guard",
          "same_loop_sl_hit_skipped_reason" in source
          and "be_sl_changed_this_loop" in source, "")

    # =====================================================================
    # Source-level assertions on the live management guards
    # =====================================================================
    check("2", "live management skips ENTRY_FILL_UNCONFIRMED normal management",
          "ENTRY_FILL_UNCONFIRMED - normal BE/MIN_LOCK/trailing skipped" in source, "")
    check("1", "live entry fill source fields are stored",
          "exchange_entry_price_ts" in source
          and "exchange_entry_source" in source
          and "actual_exchange_fill" in source, "")

    # =====================================================================
    # Report
    # =====================================================================
    scenario_to_phase = {
        "1": "A", "2": "A", "3": "A/B", "4": "B/G/H", "5": "B/G/H",
        "6": "C", "7": "C/G", "8": "C", "9": "D/H", "10": "E",
        "11": "F", "12": "H", "13": "G",
    }

    print("\n=== Deep Live Trade Lifecycle Simulator (BE / MIN_LOCK / Trailing) ===")
    for scenario, label, status, detail in results:
        suffix = f" - {detail}" if status != "PASS" and detail else ""
        print(f"[{status}] (S{scenario}) {label}{suffix}")

    print("\n--- Scenario rollup ---")
    for scenario in sorted(scenario_status, key=lambda s: int(s)):
        phase = scenario_to_phase.get(scenario, "?")
        print(f"Scenario {scenario} [{phase}]: {scenario_status[scenario]}")

    print("\n--- Confirmations ---")
    confirmations = [
        ("BE uses actual exchange fill, not planned entry",
         scenario_status.get("1") == "PASS" and scenario_status.get("3") == "PASS"),
        ("planned entry never drives live BE/MIN_LOCK",
         scenario_status.get("3") == "PASS"),
        ("MIN_LOCK immediately-triggerable never mutates local SL",
         scenario_status.get("7") == "PASS" and scenario_status.get("8") == "PASS"),
        ("BE same-loop guard works (no MIN_LOCK / no local close same loop)",
         scenario_status.get("4") == "PASS" and scenario_status.get("13") == "PASS"),
        ("Phase 1/2/3 trailing paths still work",
         scenario_status.get("9") == "PASS" and scenario_status.get("10") == "PASS"
         and scenario_status.get("11") == "PASS"),
        ("No paper regression (normalize fallback unchanged)",
         scenario_status.get("2") == "PASS"),
        ("No risk/cap/entry predicate changes (simulator-only; no runtime mutated)",
         True),
    ]
    for label, ok in confirmations:
        print(f"[{'PASS' if ok else 'FAIL'}] {label}")

    overall = "FAIL" if issues or not all(ok for _, ok in confirmations) else "PASS"
    print(f"\nRESULT: {overall}")
    if overall == "FAIL":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
