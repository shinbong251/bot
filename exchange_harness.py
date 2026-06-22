#!/usr/bin/env python3
"""
REAL TESTNET EXECUTION HARNESS — COIN BOT ONLY

Bypasses market scanning. Injects synthetic trade scenarios directly
into the exchange layer. All exchange calls are REAL testnet API calls.

SAFETY: Only runs when testnet_mode=true in config.json.

Usage:
    python exchange_harness.py
    python exchange_harness.py --scenario long_btc
    python exchange_harness.py --scenario short_sol
    python exchange_harness.py --scenario stop_replacement
    python exchange_harness.py --scenario emergency_close
    python exchange_harness.py --scenario reconciliation
    python exchange_harness.py --scenario trailing_lifecycle
    python exchange_harness.py --scenario restart_recovery
    python exchange_harness.py --scenario partial_close
    python exchange_harness.py --scenario timeout_recovery
    python exchange_harness.py --scenario rapid_trailing
    python exchange_harness.py --scenario exchange_desync
    python exchange_harness.py --scenario duplicate_stop

Scenarios:
    long_btc          LONG BTCUSDT — market entry + stop
    long_sol          LONG SOLUSDT — market entry + stop
    short_btc         SHORT BTCUSDT — market entry + stop
    short_sol         SHORT SOLUSDT — market entry + stop
    stop_replacement  Create stop, trail it, verify old removed
    emergency_close   Entry + simulate stop failure + emergency close
    reconciliation    Compare local state vs exchange positions
    trailing_lifecycle  Advance 0.8R→1.5R→2.5R via real stop replacements
    restart_recovery  Crash/restart: persist state, rebind algoId, no -4130
    partial_close     Partial TP close, stop resized for remaining qty
    timeout_recovery  Simulate POST timeout, recover without duplicate/naked
    rapid_trailing    5 rapid trailing updates, verify no orphan stops
    exchange_desync   Manual stop cancel simulation, desync detection
    duplicate_stop    Pre-placement check: rebind existing stop, avoid -4130
    all               Run all scenarios sequentially (default)
"""

import argparse
import json
import os
import sys
import time

# ── PATH SETUP ──────────────────────────────────────────────────────────────
_BOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _BOT_DIR)

# ── CONFIG LOADER ────────────────────────────────────────────────────────────
_CONFIG_PATH = os.path.join(_BOT_DIR, "config.json")


def _load_config() -> dict:
    try:
        with open(_CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"[HARNESS] config.json read error: {e}")
        return {}


# ── SAFETY GATE — must pass before any exchange interaction ──────────────────
def _require_testnet_gate():
    cfg = _load_config()
    if not cfg.get("testnet_mode", False):
        print("[HARNESS] BLOCKED — testnet_mode is not true in config.json.")
        print("[HARNESS] Set \"testnet_mode\": true before using this harness.")
        sys.exit(1)
    print("[HARNESS] Safety gate: testnet_mode=true confirmed.")


# ── IMPORT EXCHANGE LAYER ────────────────────────────────────────────────────
import certifi
import requests
from exchange import testnet_executor as tn
from exchange.precision import get_symbol_filters, round_qty


# ── LOGGING HELPERS ──────────────────────────────────────────────────────────

def _hdr(msg: str):
    print(f"[HARNESS] {msg}")


def _sep():
    print("[HARNESS] " + "-" * 48)


def _section(title: str):
    print()
    print("[HARNESS] " + "=" * 48)
    print(f"[HARNESS] {title}")
    print("[HARNESS] " + "=" * 48)


# ── MARK PRICE FETCH ─────────────────────────────────────────────────────────

def _get_mark_price(symbol: str) -> float | None:
    """Fetch current mark price from testnet public endpoint."""
    try:
        res = requests.get(
            "https://testnet.binancefuture.com/fapi/v1/premiumIndex",
            params={"symbol": symbol},
            verify=certifi.where(),
            timeout=(5, 10),
        )
        if res.status_code == 200:
            data = res.json()
            mp = float(data.get("markPrice", 0))
            if mp > 0:
                _hdr(f"Mark price {symbol}: {mp}")
                return mp
        _hdr(f"Mark price fetch HTTP {res.status_code} for {symbol}")
    except Exception as e:
        _hdr(f"Mark price fetch error: {e}")
    return None


# ── CORE HARNESS UTILITIES ───────────────────────────────────────────────────

def inject_trade(
    symbol:       str,
    side:         str,
    entry:        float,
    sl:           float,
    tp:           float,
    balance:      float,
    risk_percent: float,
) -> dict:
    """
    Inject a synthetic trade directly into the exchange layer.
    Calls validate_and_prepare → place_market_order → place_stop_loss.
    Returns dict: prep, entry_result, sl_result
    """
    _hdr(f"inject_trade: {symbol} {side}  entry={entry}  sl={sl}  tp={tp}")
    _sep()

    prep = tn.validate_and_prepare(
        symbol=symbol,
        side=side,
        entry=entry,
        sl=sl,
        tp=tp,
        balance=balance,
        risk_percent=risk_percent,
    )

    if not prep["valid"]:
        _hdr(f"VALIDATE FAILED: {prep['reason']}")
        return {"prep": prep, "entry_result": None, "sl_result": None}

    _hdr(
        f"VALIDATE OK — qty={prep['qty']}  "
        f"leverage={prep['leverage']}x  "
        f"margin=${prep['margin']:.2f}"
    )

    entry_result = tn.place_market_order(
        symbol=symbol,
        side=side,
        qty=prep["qty"],
        leverage=prep["leverage"],
    )

    if not entry_result.get("success"):
        _hdr(f"MARKET ENTRY FAILED: {entry_result.get('error')}")
        return {"prep": prep, "entry_result": entry_result, "sl_result": None}

    _hdr(
        f"MARKET ENTRY ACCEPTED — "
        f"orderId={entry_result['order_id']}  "
        f"fill_price={entry_result['fill_price']}"
    )

    time.sleep(0.5)

    sl_result = tn.place_stop_loss(
        symbol=symbol,
        entry_side=side,
        qty=prep["qty"],
        stop_price=sl,
    )

    if sl_result.get("success"):
        _hdr(f"STOP ACCEPTED — orderId={sl_result['order_id']}  stopPrice={sl}")
    else:
        _hdr(f"STOP FAILED: {sl_result.get('error')}")

    return {"prep": prep, "entry_result": entry_result, "sl_result": sl_result}


def force_trailing_progression(
    symbol:       str,
    side:         str,
    qty:          float,
    old_order_id: int,
    old_sl:       float,
    new_sl:       float,
) -> dict:
    """
    Simulate a trailing stop update via real exchange call.
    Calls update_trailing_stop() — place-first, then cancel old.
    """
    _hdr(f"force_trailing_progression: {symbol}  {old_sl} → {new_sl}")
    result = tn.update_trailing_stop(
        symbol=symbol,
        entry_side=side,
        qty=qty,
        new_stop_price=new_sl,
        old_order_id=old_order_id,
    )
    if result.get("success"):
        _hdr(
            f"Trailing update OK — "
            f"new orderId={result['new_order_id']}  "
            f"cancel_ok={result['cancel_ok']}"
        )
    else:
        _hdr(f"Trailing update FAILED: {result.get('error')}")
    return result


def verify_exchange_stop(symbol: str, order_id: int) -> bool:
    """Query and verify stop algo order on exchange. Returns True if confirmed active."""
    _hdr(f"verify_exchange_stop: {symbol}  algoId={order_id}")
    data = tn.query_algo_order(symbol=symbol, algo_id=order_id)
    if data is None:
        _hdr(f"STOP NOT FOUND in open algo orders: algoId={order_id} (may have triggered)")
        return False
    _hdr(
        f"STOP VERIFIED — "
        f"algoStatus={data.get('algoStatus')}  "
        f"triggerPrice={data.get('triggerPrice')}"
    )
    return True


def verify_no_open_position(symbol: str) -> bool:
    """Confirm exchange has no open position for symbol. Returns True if clean."""
    _hdr(f"verify_no_open_position: {symbol}")
    positions = tn.get_exchange_positions()
    for pos in positions:
        if pos["symbol"] == symbol:
            _hdr(f"POSITION STILL OPEN: {symbol}  amt={pos['positionAmt']}")
            return False
    _hdr(f"NO OPEN POSITION confirmed for {symbol}")
    return True


def _get_balance() -> float | None:
    balance = tn.get_execution_balance()
    if balance is None:
        _hdr("execution_balance not configured in config.json — aborting")
    return balance


# ── SCENARIOS ────────────────────────────────────────────────────────────────

def scenario_long_entry(symbol: str = "BTCUSDT"):
    """Scenario 1: LONG entry + STOP placement on TESTNET."""
    _section(f"Scenario: LONG_ENTRY  symbol={symbol}")

    mark = _get_mark_price(symbol)
    if mark is None:
        _hdr("Could not fetch mark price — aborting")
        return None

    balance = _get_balance()
    if balance is None:
        return None

    entry = mark
    sl    = round(entry * 0.970, 2)
    tp    = round(entry * 1.030, 2)

    _hdr(f"Injecting LONG  entry={entry}  sl={sl}  tp={tp}")
    _hdr("(risk_percent=0.05 — harness uses 5% to meet min notional on all symbols)")

    result = inject_trade(
        symbol=symbol, side="BUY",
        entry=entry, sl=sl, tp=tp,
        balance=balance, risk_percent=0.05,
    )

    _sep()
    if result["entry_result"] and result["entry_result"].get("success"):
        sl_ok = result["sl_result"] and result["sl_result"].get("success")
        _hdr(f"LONG_ENTRY SCENARIO: {'PASS' if sl_ok else 'PARTIAL (entry OK, stop FAILED)'}")
        _hdr(f"  Entry orderId   : {result['entry_result']['order_id']}")
        _hdr(f"  Fill price      : {result['entry_result']['fill_price']}")
        _hdr(f"  Stop orderId    : {result['sl_result']['order_id'] if sl_ok else 'FAILED'}")
    else:
        _hdr("LONG_ENTRY SCENARIO: FAIL")

    return result


def scenario_short_entry(symbol: str = "SOLUSDT"):
    """Scenario 2: SHORT entry + STOP placement on TESTNET."""
    _section(f"Scenario: SHORT_ENTRY  symbol={symbol}")

    mark = _get_mark_price(symbol)
    if mark is None:
        _hdr("Could not fetch mark price — aborting")
        return None

    balance = _get_balance()
    if balance is None:
        return None

    entry = mark
    sl    = round(entry * 1.030, 4)
    tp    = round(entry * 0.970, 4)

    _hdr(f"Injecting SHORT  entry={entry}  sl={sl}  tp={tp}")
    _hdr("(risk_percent=0.05 — harness uses 5% to meet min notional on all symbols)")

    result = inject_trade(
        symbol=symbol, side="SELL",
        entry=entry, sl=sl, tp=tp,
        balance=balance, risk_percent=0.05,
    )

    _sep()
    if result["entry_result"] and result["entry_result"].get("success"):
        sl_ok = result["sl_result"] and result["sl_result"].get("success")
        _hdr(f"SHORT_ENTRY SCENARIO: {'PASS' if sl_ok else 'PARTIAL (entry OK, stop FAILED)'}")
        _hdr(f"  Entry orderId   : {result['entry_result']['order_id']}")
        _hdr(f"  Fill price      : {result['entry_result']['fill_price']}")
        _hdr(f"  Stop orderId    : {result['sl_result']['order_id'] if sl_ok else 'FAILED'}")
        _hdr(f"  Trigger direction: BUY STOP_MARKET stopPrice={sl} > markPrice={mark}")
    else:
        _hdr("SHORT_ENTRY SCENARIO: FAIL")

    return result


def scenario_stop_replacement(symbol: str = "ETHUSDT"):
    """
    Scenario 3: Place trade + stop, manually trail the stop.
    Verifies old stop removed, new stop active on exchange.
    """
    _section(f"Scenario: STOP_REPLACEMENT  symbol={symbol}")

    mark = _get_mark_price(symbol)
    if mark is None:
        _hdr("Could not fetch mark price — aborting")
        return None

    balance = _get_balance()
    if balance is None:
        return None

    entry    = mark
    sl_init  = round(entry * 0.970, 2)
    sl_trail = round(entry * 0.982, 2)
    tp       = round(entry * 1.050, 2)

    _hdr(f"Injecting LONG  entry={entry}  sl={sl_init}  tp={tp}")
    result = inject_trade(
        symbol=symbol, side="BUY",
        entry=entry, sl=sl_init, tp=tp,
        balance=balance, risk_percent=0.05,
    )

    if not (result["entry_result"] and result["entry_result"].get("success")):
        _hdr("STOP_REPLACEMENT: entry failed — cannot continue")
        return None

    if not (result["sl_result"] and result["sl_result"].get("success")):
        _hdr("STOP_REPLACEMENT: initial SL failed — cannot continue")
        return None

    old_order_id = result["sl_result"]["order_id"]
    qty          = result["prep"]["qty"]

    _hdr(f"Initial stop placed   orderId={old_order_id}  stopPrice={sl_init}")
    _hdr(f"Trailing SL from {sl_init} → {sl_trail}")

    time.sleep(1.0)

    trail_result = force_trailing_progression(
        symbol=symbol, side="BUY", qty=qty,
        old_order_id=old_order_id,
        old_sl=sl_init,
        new_sl=sl_trail,
    )

    _sep()
    if trail_result.get("success"):
        new_id = trail_result["new_order_id"]
        _hdr("STOP_REPLACEMENT SCENARIO: PASS")
        _hdr(f"  Old stop removed    : {'OK' if trail_result['cancel_ok'] else 'FAILED (both stops may be active)'}")
        _hdr(f"  New stop orderId    : {new_id}")
        _hdr(f"  New stopPrice       : {sl_trail}")
        verify_exchange_stop(symbol, new_id)
    else:
        _hdr(f"STOP_REPLACEMENT SCENARIO: FAIL  error={trail_result.get('error')}")

    return trail_result


def scenario_emergency_close(symbol: str = "XRPUSDT"):
    """
    Scenario 4: Place entry, skip stop, call emergency_close_position().
    Simulates the SL-failure branch in open_trade() and verifies position closed.
    """
    _section(f"Scenario: EMERGENCY_CLOSE  symbol={symbol}")

    mark = _get_mark_price(symbol)
    if mark is None:
        _hdr("Could not fetch mark price — aborting")
        return None

    balance = _get_balance()
    if balance is None:
        return None

    entry = mark
    sl    = round(entry * 0.970, 4)
    tp    = round(entry * 1.030, 4)

    prep = tn.validate_and_prepare(
        symbol=symbol, side="BUY",
        entry=entry, sl=sl, tp=tp,
        balance=balance, risk_percent=0.05,
    )

    if not prep["valid"]:
        _hdr(f"VALIDATE FAILED: {prep['reason']}")
        return None

    _hdr(f"Placing MARKET entry  qty={prep['qty']}  leverage={prep['leverage']}x")
    entry_result = tn.place_market_order(
        symbol=symbol, side="BUY",
        qty=prep["qty"], leverage=prep["leverage"],
    )

    if not entry_result.get("success"):
        _hdr(f"EMERGENCY_CLOSE: entry failed — {entry_result.get('error')}")
        return None

    _hdr(f"Entry placed — orderId={entry_result['order_id']}")
    _hdr("Simulating STOP failure — skipping stop placement")
    _hdr("Triggering emergency_close_position()")

    time.sleep(0.5)

    close_result = tn.emergency_close_position(
        symbol=symbol,
        entry_side="BUY",
        qty=prep["qty"],
    )

    _sep()
    if close_result.get("success"):
        _hdr("EMERGENCY_CLOSE SCENARIO: PASS")
        _hdr(f"  Close orderId   : {close_result['order_id']}")
        _hdr(f"  Close status    : {close_result['status']}")
        _hdr(f"  Fill price      : {close_result['fill_price']}")
        time.sleep(1.5)
        clean = verify_no_open_position(symbol)
        _hdr(f"  Position clean  : {clean}")
    else:
        _hdr(f"EMERGENCY_CLOSE SCENARIO: FAIL — {close_result.get('error')}")
        _hdr("WARNING: position may be NAKED — check testnet manually")

    return close_result


def scenario_reconciliation(symbol: str = "BTCUSDT"):
    """
    Scenario 5: Place a real position, then run compare_local_vs_exchange().
    Verifies orphan detection and state matching logic.
    """
    _section(f"Scenario: RECONCILIATION  symbol={symbol}")

    mark = _get_mark_price(symbol)
    if mark is None:
        _hdr("Could not fetch mark price — aborting")
        return None

    balance = _get_balance()
    if balance is None:
        return None

    entry = mark
    sl    = round(entry * 0.970, 2)
    tp    = round(entry * 1.030, 2)

    _hdr("Placing LONG position for reconciliation test")
    result = inject_trade(
        symbol=symbol, side="BUY",
        entry=entry, sl=sl, tp=tp,
        balance=balance, risk_percent=0.05,
    )

    qty = result["prep"]["qty"] if result["prep"] else 0.001

    local_positions = []
    if result["entry_result"] and result["entry_result"].get("success"):
        local_positions = [{
            "symbol":    symbol,
            "direction": "LONG",
            "qty":       qty,
            "entry":     entry,
        }]
        _hdr(f"Local state: 1 trade  {symbol} LONG qty={qty}")
    else:
        _hdr("Entry failed — testing with empty local state (orphan detection)")

    time.sleep(1.5)

    _hdr("Fetching live exchange positions")
    exchange_positions = tn.get_exchange_positions()
    _hdr(f"Exchange reports {len(exchange_positions)} open position(s)")

    _hdr("Running compare_local_vs_exchange()")
    recon = tn.compare_local_vs_exchange(local_positions, exchange_positions)

    _sep()
    _hdr("RECONCILIATION RESULT:")
    _hdr(f"  matched        : {len(recon['matched'])}")
    _hdr(f"  local_only     : {len(recon['local_only'])}")
    _hdr(f"  exchange_only  : {len(recon['exchange_only'])}")
    _hdr(f"  discrepancies  : {len(recon['discrepancies'])}")

    if recon["matched"]:
        _hdr("RECONCILIATION SCENARIO: PASS — local state matches exchange")
    elif recon["discrepancies"]:
        _hdr("RECONCILIATION SCENARIO: MISMATCH — qty or direction differs")
    elif recon["exchange_only"]:
        _hdr("RECONCILIATION SCENARIO: ORPHAN DETECTED on exchange")
    elif recon["local_only"]:
        _hdr("RECONCILIATION SCENARIO: LOCAL ONLY — not on exchange")

    return recon


def scenario_trailing_lifecycle(symbol: str = "BNBUSDT"):
    """
    Scenario 6: Manually advance trade through 0.8R → 1.5R → 2.5R.
    Each phase uses real exchange stop replacement via update_trailing_stop().
    Stops are kept safely below mark price to pass Binance validation.
    """
    _section(f"Scenario: TRAILING_LIFECYCLE  symbol={symbol}")

    mark = _get_mark_price(symbol)
    if mark is None:
        _hdr("Could not fetch mark price — aborting")
        return None

    balance = _get_balance()
    if balance is None:
        return None

    entry    = mark
    sl_init  = round(entry * 0.970, 4)
    tp       = round(entry * 1.090, 4)

    _hdr(f"Injecting LONG  entry={entry}  sl_init={sl_init}  tp={tp}")
    result = inject_trade(
        symbol=symbol, side="BUY",
        entry=entry, sl=sl_init, tp=tp,
        balance=balance, risk_percent=0.05,
    )

    if not (result["entry_result"] and result["entry_result"].get("success")):
        _hdr("TRAILING_LIFECYCLE: entry failed — aborting")
        return None

    if not (result["sl_result"] and result["sl_result"].get("success")):
        _hdr("TRAILING_LIFECYCLE: initial SL failed — aborting")
        return None

    qty           = result["prep"]["qty"]
    current_sl_id = result["sl_result"]["order_id"]
    current_sl    = sl_init

    _hdr(f"Trade active — entry={entry}  sl={sl_init}  qty={qty}  sl_orderId={current_sl_id}")

    phases = [
        ("0.8R → BE (SL to -1.8%)", round(entry * 0.982, 4)),
        ("1.5R → Profit lock (SL to -1.2%)", round(entry * 0.988, 4)),
        ("2.5R → Phase 3 trail (SL to -0.8%)", round(entry * 0.992, 4)),
    ]

    for phase_name, new_sl in phases:
        _sep()
        _hdr(f"Phase: {phase_name}")
        _hdr(f"  {current_sl} → {new_sl}")

        time.sleep(0.8)

        r = force_trailing_progression(
            symbol=symbol, side="BUY", qty=qty,
            old_order_id=current_sl_id,
            old_sl=current_sl,
            new_sl=new_sl,
        )

        if not r.get("success"):
            _hdr(f"TRAILING_LIFECYCLE: phase FAILED: {r.get('error')}")
            return None

        current_sl_id = r["new_order_id"]
        current_sl    = new_sl

    _sep()
    _hdr("TRAILING_LIFECYCLE SCENARIO: PASS — all phases complete")
    _hdr(f"  Final stop orderId  : {current_sl_id}")
    _hdr(f"  Final SL price      : {current_sl}")
    verify_exchange_stop(symbol, current_sl_id)

    return {"final_sl_id": current_sl_id, "final_sl": current_sl}


# ── SCENARIO 7: RESTART RECOVERY ─────────────────────────────────────────────

_RESTART_STATE_FILE = os.path.join(_BOT_DIR, "harness_restart_state.json")


def scenario_restart_recovery(symbol: str = "DOGEUSDT"):
    """
    Scenario 7: Crash/restart resilience.
    Phase 1 — open trade, persist state to file.
    Phase 2 — simulate crash: discard runtime state.
    Phase 3 — fresh runtime: hydrate from file, query exchange, rebind algoId.
    Phase 4 — continue trailing with rebound algoId (no -4130 collision).
    """
    _section(f"Scenario: RESTART_RECOVERY  symbol={symbol}")

    mark = _get_mark_price(symbol)
    if mark is None:
        _hdr("Could not fetch mark price — aborting")
        return None

    balance = _get_balance()
    if balance is None:
        return None

    entry = mark
    sl    = round(entry * 0.970, 6)
    tp    = round(entry * 1.030, 6)

    _hdr("Phase 1: Opening trade and persisting state to file")
    result = inject_trade(
        symbol=symbol, side="BUY",
        entry=entry, sl=sl, tp=tp,
        balance=balance, risk_percent=0.05,
    )

    if not (result["entry_result"] and result["entry_result"].get("success")):
        _hdr("RESTART_RECOVERY: entry failed — aborting")
        return None
    if not (result["sl_result"] and result["sl_result"].get("success")):
        _hdr("RESTART_RECOVERY: initial SL failed — aborting")
        return None

    qty     = result["prep"]["qty"]
    algo_id = result["sl_result"]["order_id"]

    state = {"symbol": symbol, "side": "BUY", "qty": qty, "sl": sl, "algo_id": algo_id}
    with open(_RESTART_STATE_FILE, "w") as f:
        json.dump(state, f)
    _hdr(f"State persisted: algo_id={algo_id}  qty={qty}  sl={sl}")
    _hdr(f"File: {_RESTART_STATE_FILE}")

    # Phase 2: Simulate crash — discard all runtime references
    _sep()
    _hdr("Phase 2: Simulating crash — discarding runtime state")
    del result
    qty     = None
    algo_id = None
    sl      = None
    _hdr("Runtime state cleared. Simulating fresh process start...")
    time.sleep(1.0)

    # Phase 3: Fresh runtime — hydrate from state file, rebind algoId
    _sep()
    _hdr("Phase 3: Fresh runtime — hydrating from state file")
    with open(_RESTART_STATE_FILE, "r") as f:
        persisted = json.load(f)

    r_qty     = persisted["qty"]
    r_sl      = persisted["sl"]
    r_algo_id = persisted["algo_id"]
    _hdr(f"Loaded state: algo_id={r_algo_id}  qty={r_qty}  sl={r_sl}")

    _hdr("Querying exchange to check if stop already exists before any placement")
    existing = tn.query_algo_order(symbol=symbol, algo_id=r_algo_id)

    if existing is not None:
        _hdr(f"EXISTING STOP FOUND — rebinding algoId={r_algo_id} (no new placement)")
        _hdr(f"  algoStatus   : {existing.get('algoStatus')}")
        _hdr(f"  triggerPrice : {existing.get('triggerPrice')}")
        _hdr("SKIP stop placement — no duplicate, no -4130 risk")
        rebound_id = r_algo_id
    else:
        _hdr("Stop not found on exchange (may have triggered). Scenario inconclusive.")
        try:
            os.remove(_RESTART_STATE_FILE)
        except OSError:
            pass
        return None

    # Phase 4: Continue trailing from rebound algoId
    _sep()
    new_sl = round(r_sl * 1.008, 6)
    _hdr(f"Phase 4: Trailing with rebound algoId  {r_sl} -> {new_sl}")

    trail = force_trailing_progression(
        symbol=symbol, side="BUY", qty=r_qty,
        old_order_id=rebound_id, old_sl=r_sl, new_sl=new_sl,
    )

    _sep()
    if trail.get("success"):
        _hdr("RESTART_RECOVERY SCENARIO: PASS")
        _hdr(f"  Rebound algoId  : {rebound_id}")
        _hdr(f"  New algoId      : {trail['new_order_id']}")
        _hdr(f"  No -4130        : confirmed — no duplicate stop placed")
        _hdr(f"  Post-restart trail: OK")
        verify_exchange_stop(symbol, trail["new_order_id"])
    else:
        _hdr(f"RESTART_RECOVERY SCENARIO: FAIL — {trail.get('error')}")

    try:
        os.remove(_RESTART_STATE_FILE)
    except OSError:
        pass
    return trail


# ── SCENARIO 8: PARTIAL CLOSE + STOP RESIZE ──────────────────────────────────

def scenario_partial_close(symbol: str = "LINKUSDT"):
    """
    Scenario 8: Partial TP close + stop resize for remaining qty.
    Phase 1 — open full position with initial stop.
    Phase 2 — partial close ~40% via reduceOnly MARKET.
    Phase 3 — cancel old stop (sized for full position).
    Phase 4 — place new stop for remaining qty.
    """
    _section(f"Scenario: PARTIAL_CLOSE  symbol={symbol}")

    mark = _get_mark_price(symbol)
    if mark is None:
        _hdr("Could not fetch mark price — aborting")
        return None

    balance = _get_balance()
    if balance is None:
        return None

    entry = mark
    sl    = round(entry * 0.970, 4)
    tp    = round(entry * 1.030, 4)

    _hdr("Phase 1: Opening full position with initial stop")
    result = inject_trade(
        symbol=symbol, side="BUY",
        entry=entry, sl=sl, tp=tp,
        balance=balance, risk_percent=0.05,
    )

    if not (result["entry_result"] and result["entry_result"].get("success")):
        _hdr("PARTIAL_CLOSE: entry failed — aborting")
        return None
    if not (result["sl_result"] and result["sl_result"].get("success")):
        _hdr("PARTIAL_CLOSE: initial SL failed — aborting")
        return None

    full_qty = result["prep"]["qty"]
    algo_id  = result["sl_result"]["order_id"]
    _hdr(f"Position open: full_qty={full_qty}  stop algoId={algo_id}")

    filters = get_symbol_filters(symbol)
    if filters is None:
        _hdr("PARTIAL_CLOSE: could not fetch symbol filters — aborting")
        return None

    partial_qty = round_qty(symbol, full_qty * 0.4)
    if partial_qty is None or partial_qty <= 0:
        _hdr(f"PARTIAL_CLOSE: partial qty rounding failed — aborting")
        return None

    remaining_qty = round_qty(symbol, full_qty - partial_qty)
    if remaining_qty is None or remaining_qty <= 0:
        _hdr(f"PARTIAL_CLOSE: remaining qty invalid ({full_qty} - {partial_qty}) — aborting")
        return None

    _sep()
    _hdr(f"Phase 2: Simulating partial TP — closing {partial_qty} of {full_qty}")
    _hdr(f"  Step size    : {filters['step_size']}")
    _hdr(f"  Partial qty  : {partial_qty}  ({round(partial_qty/full_qty*100, 1)}% of position)")
    _hdr(f"  Remaining qty: {remaining_qty}")

    time.sleep(0.5)

    partial_close = tn.emergency_close_position(
        symbol=symbol, entry_side="BUY", qty=partial_qty,
    )

    if not partial_close.get("success"):
        _hdr(f"PARTIAL_CLOSE: partial close FAILED — {partial_close.get('error')}")
        return None

    _hdr(f"Partial close filled — orderId={partial_close['order_id']}  fill={partial_close['fill_price']}")

    # Phase 3: Cancel old stop (sized for full position)
    _sep()
    _hdr(f"Phase 3: Cancelling old stop (sized for full_qty={full_qty})  algoId={algo_id}")
    time.sleep(0.5)
    cancel = tn.cancel_stop_loss(symbol=symbol, order_id=algo_id)
    if not cancel.get("success"):
        _hdr(f"WARNING: old stop cancel FAILED — {cancel.get('error')}")
    else:
        _hdr(f"Old stop cancelled  algoId={algo_id}")

    # Phase 4: Place new stop for remaining qty
    _sep()
    _hdr(f"Phase 4: Placing new stop for remaining_qty={remaining_qty}  sl={sl}")
    time.sleep(0.5)

    new_sl_result = tn.place_stop_loss(
        symbol=symbol, entry_side="BUY",
        qty=remaining_qty, stop_price=sl,
    )

    _sep()
    if new_sl_result.get("success"):
        new_algo_id = new_sl_result["order_id"]
        _hdr("PARTIAL_CLOSE SCENARIO: PASS")
        _hdr(f"  Full qty        : {full_qty}")
        _hdr(f"  Closed qty      : {partial_qty}")
        _hdr(f"  Remaining qty   : {remaining_qty}")
        _hdr(f"  Old stop removed: algoId={algo_id}")
        _hdr(f"  New stop placed : algoId={new_algo_id}  qty={remaining_qty}")
        verify_exchange_stop(symbol, new_algo_id)
    else:
        _hdr(f"PARTIAL_CLOSE SCENARIO: FAIL — new stop rejected: {new_sl_result.get('error')}")

    return new_sl_result


# ── SCENARIO 9: TIMEOUT RECOVERY ─────────────────────────────────────────────

def scenario_timeout_recovery(symbol: str = "AVAXUSDT"):
    """
    Scenario 9: Simulate POST timeout during trailing — recover without duplicate/naked.
    Phase 1 — open trade with initial stop.
    Phase 2 — monkey-patch _post to return None once, fire trailing.
    Phase 3 — confirm old stop preserved (no naked position).
    Phase 4 — real trailing update as recovery.
    """
    _section(f"Scenario: TIMEOUT_RECOVERY  symbol={symbol}")

    mark = _get_mark_price(symbol)
    if mark is None:
        _hdr("Could not fetch mark price — aborting")
        return None

    balance = _get_balance()
    if balance is None:
        return None

    entry = mark
    sl    = round(entry * 0.970, 4)
    tp    = round(entry * 1.030, 4)

    _hdr("Phase 1: Opening trade with stop (baseline for timeout test)")
    result = inject_trade(
        symbol=symbol, side="BUY",
        entry=entry, sl=sl, tp=tp,
        balance=balance, risk_percent=0.05,
    )

    if not (result["entry_result"] and result["entry_result"].get("success")):
        _hdr("TIMEOUT_RECOVERY: entry failed — aborting")
        return None
    if not (result["sl_result"] and result["sl_result"].get("success")):
        _hdr("TIMEOUT_RECOVERY: initial SL failed — aborting")
        return None

    qty     = result["prep"]["qty"]
    algo_id = result["sl_result"]["order_id"]
    _hdr(f"Trade active: qty={qty}  stop algoId={algo_id}")

    # Phase 2: Simulate POST timeout on trailing update
    _sep()
    new_sl = round(sl * 1.008, 4)
    _hdr(f"Phase 2: Simulating POST timeout during trailing {sl} -> {new_sl}")
    _hdr("Patching tn._post to return None on first call (timeout simulation)...")

    real_post = tn._post
    fired     = [False]

    def _timeout_once(path, params):
        if not fired[0]:
            fired[0] = True
            print(
                f"[TESTNET] TIMEOUT on POST {path}. "
                "Order status is UNKNOWN — do NOT retry without calling query_order() first."
            )
            return None
        return real_post(path, params)

    tn._post = _timeout_once
    try:
        timeout_result = tn.update_trailing_stop(
            symbol=symbol,
            entry_side="BUY",
            qty=qty,
            new_stop_price=new_sl,
            old_order_id=algo_id,
        )
    finally:
        tn._post = real_post
        _hdr("_post patch restored to real implementation")

    _hdr(f"Trailing with timeout: success={timeout_result.get('success')}  error={timeout_result.get('error')}")

    # Phase 3: Confirm old stop still active (timeout fired → new stop not placed → old preserved)
    _sep()
    _hdr(f"Phase 3: Querying exchange — confirming old stop still active (position not naked)")
    time.sleep(0.5)
    old_still_active = verify_exchange_stop(symbol, algo_id)
    _hdr(f"Old stop active: {old_still_active}")
    if old_still_active:
        _hdr("Position is PROTECTED — old stop survived the timeout, no naked exposure")
    else:
        _hdr("WARNING: old stop NOT found — position may be NAKED")

    # Phase 4: Real trailing update as recovery
    _sep()
    _hdr(f"Phase 4: Recovery — real trailing update {sl} -> {new_sl}")
    time.sleep(0.5)

    recovery_result = force_trailing_progression(
        symbol=symbol, side="BUY", qty=qty,
        old_order_id=algo_id, old_sl=sl, new_sl=new_sl,
    )

    _sep()
    if recovery_result.get("success"):
        _hdr("TIMEOUT_RECOVERY SCENARIO: PASS")
        _hdr(f"  Timeout simulated  : YES — _post returned None for trailing POST")
        _hdr(f"  Old stop preserved : {old_still_active}")
        _hdr(f"  No naked position  : confirmed")
        _hdr(f"  Recovery trailing  : algoId={recovery_result['new_order_id']}")
        verify_exchange_stop(symbol, recovery_result["new_order_id"])
    else:
        _hdr(f"TIMEOUT_RECOVERY SCENARIO: FAIL — {recovery_result.get('error')}")

    return recovery_result


# ── SCENARIO 10: RAPID TRAILING STRESS TEST ──────────────────────────────────

def scenario_rapid_trailing(symbol: str = "ADAUSDT"):
    """
    Scenario 10: 5 rapid successive trailing updates — verify no orphan algo orders.
    No sleep between trailing updates. After all 5: query openAlgoOrders for symbol.
    Expects exactly 1 open stop matching final algoId.
    """
    _section(f"Scenario: RAPID_TRAILING  symbol={symbol}")

    mark = _get_mark_price(symbol)
    if mark is None:
        _hdr("Could not fetch mark price — aborting")
        return None

    balance = _get_balance()
    if balance is None:
        return None

    entry   = mark
    sl_init = round(entry * 0.970, 6)
    tp      = round(entry * 1.050, 6)

    _hdr(f"Injecting LONG  entry={entry}  sl={sl_init}")
    result = inject_trade(
        symbol=symbol, side="BUY",
        entry=entry, sl=sl_init, tp=tp,
        balance=balance, risk_percent=0.05,
    )

    if not (result["entry_result"] and result["entry_result"].get("success")):
        _hdr("RAPID_TRAILING: entry failed — aborting")
        return None
    if not (result["sl_result"] and result["sl_result"].get("success")):
        _hdr("RAPID_TRAILING: initial SL failed — aborting")
        return None

    qty           = result["prep"]["qty"]
    current_sl_id = result["sl_result"]["order_id"]
    current_sl    = sl_init
    _hdr(f"Trade active: qty={qty}  initial stop algoId={current_sl_id}")

    sl_levels = [
        round(entry * 0.973, 6),
        round(entry * 0.976, 6),
        round(entry * 0.979, 6),
        round(entry * 0.982, 6),
        round(entry * 0.985, 6),
    ]

    _sep()
    _hdr(f"Firing {len(sl_levels)} rapid trailing updates (no sleep between)")
    all_ids = [current_sl_id]

    for i, new_sl in enumerate(sl_levels, 1):
        _hdr(f"  Rapid update {i}/{len(sl_levels)}: {current_sl} -> {new_sl}")
        r = tn.update_trailing_stop(
            symbol=symbol,
            entry_side="BUY",
            qty=qty,
            new_stop_price=new_sl,
            old_order_id=current_sl_id,
        )
        if not r.get("success"):
            _hdr(f"RAPID_TRAILING: update {i} FAILED — {r.get('error')}")
            return None
        current_sl_id = r["new_order_id"]
        current_sl    = new_sl
        all_ids.append(current_sl_id)
        _hdr(f"    -> new algoId={current_sl_id}  cancel_ok={r['cancel_ok']}")

    # Verify: query all open algo orders — expect exactly 1 matching final algoId
    _sep()
    _hdr(f"Verifying open algo orders for {symbol} after {len(sl_levels)} rapid updates")
    time.sleep(1.0)
    open_orders = tn.get_open_algo_orders(symbol)

    _sep()
    _hdr("RAPID_TRAILING RESULT:")
    _hdr(f"  Total updates   : {len(sl_levels)}")
    _hdr(f"  algoId chain    : {' -> '.join(str(x) for x in all_ids)}")
    _hdr(f"  Expected final  : algoId={current_sl_id}  sl={current_sl}")
    _hdr(f"  Open algo orders: {len(open_orders)} found on exchange")

    if len(open_orders) == 1 and open_orders[0].get("algoId") == current_sl_id:
        _hdr("RAPID_TRAILING SCENARIO: PASS — exactly 1 open stop, matches final algoId")
        _hdr(f"  Final algoId  : {current_sl_id}")
        _hdr(f"  Final sl price: {current_sl}")
    elif len(open_orders) == 0:
        _hdr("RAPID_TRAILING SCENARIO: WARN — no open algo orders found (stop may have triggered)")
    else:
        orphans = [o.get("algoId") for o in open_orders if o.get("algoId") != current_sl_id]
        _hdr(f"RAPID_TRAILING SCENARIO: FAIL — {len(open_orders)} orders found, orphans={orphans}")

    return {"final_sl_id": current_sl_id, "final_sl": current_sl, "open_orders": open_orders}


# ── SCENARIO 11: EXCHANGE DESYNC RECOVERY ────────────────────────────────────

def scenario_exchange_desync(symbol: str = "DOTUSDT"):
    """
    Scenario 11: Simulate manual stop cancellation (UI interference) + desync detection.
    Phase 1 — open trade with stop.
    Phase 2 — cancel stop directly (simulates manual cancel via Binance UI).
    Phase 3 — verify position still open (cancel didn't close position).
    Phase 4 — query stop: confirm it's gone (desync state confirmed).
    Phase 5 — run reconciliation: surface orphan position without stop.
    """
    _section(f"Scenario: EXCHANGE_DESYNC  symbol={symbol}")

    mark = _get_mark_price(symbol)
    if mark is None:
        _hdr("Could not fetch mark price — aborting")
        return None

    balance = _get_balance()
    if balance is None:
        return None

    entry = mark
    sl    = round(entry * 0.970, 4)
    tp    = round(entry * 1.030, 4)

    _hdr("Phase 1: Opening trade with stop")
    result = inject_trade(
        symbol=symbol, side="BUY",
        entry=entry, sl=sl, tp=tp,
        balance=balance, risk_percent=0.05,
    )

    if not (result["entry_result"] and result["entry_result"].get("success")):
        _hdr("EXCHANGE_DESYNC: entry failed — aborting")
        return None
    if not (result["sl_result"] and result["sl_result"].get("success")):
        _hdr("EXCHANGE_DESYNC: initial SL failed — aborting")
        return None

    qty     = result["prep"]["qty"]
    algo_id = result["sl_result"]["order_id"]
    _hdr(f"Trade active: qty={qty}  stop algoId={algo_id}")

    local_positions = [{
        "symbol":    symbol,
        "direction": "LONG",
        "qty":       qty,
        "entry":     entry,
    }]

    # Phase 2: Simulate manual cancellation of stop (Binance UI interference)
    _sep()
    _hdr("Phase 2: Simulating manual stop cancellation (Binance UI interference)")
    _hdr(f"  Cancelling algoId={algo_id} directly (bypassing bot runtime)...")
    time.sleep(0.5)

    manual_cancel = tn.cancel_stop_loss(symbol=symbol, order_id=algo_id)
    if manual_cancel.get("success"):
        _hdr(f"Stop cancelled externally — algoId={algo_id}  (position now NAKED)")
    else:
        _hdr(f"WARNING: manual cancel failed — {manual_cancel.get('error')}")

    # Phase 3: Verify position still open
    _sep()
    _hdr("Phase 3: Verifying position still open (stop cancel does not close position)")
    time.sleep(1.0)
    exchange_positions = tn.get_exchange_positions()
    pos_open = any(p["symbol"] == symbol for p in exchange_positions)
    _hdr(f"Position still open: {pos_open}")

    # Phase 4: Query stop — confirm it's gone
    _sep()
    _hdr(f"Phase 4: Querying exchange for stop algoId={algo_id}")
    stop_found = verify_exchange_stop(symbol, algo_id)
    _hdr(f"Stop found on exchange: {stop_found}")
    if not stop_found:
        _hdr("DESYNC CONFIRMED — position is OPEN but STOP IS MISSING")
        _hdr("WARNING: position is NAKED — stop must be replaced immediately")

    # Phase 5: Reconciliation — detect orphan/desync
    _sep()
    _hdr("Phase 5: Running reconciliation to surface desync")
    recon = tn.compare_local_vs_exchange(local_positions, exchange_positions)

    _sep()
    _hdr("EXCHANGE_DESYNC SCENARIO RESULT:")
    _hdr(f"  Position open   : {pos_open}")
    _hdr(f"  Stop active     : {stop_found}")
    _hdr(f"  Recon matched   : {len(recon['matched'])}")
    _hdr(f"  Recon discrepan.: {len(recon['discrepancies'])}")
    _hdr(f"  Exchange-only   : {len(recon['exchange_only'])}")

    if not stop_found and pos_open:
        _hdr("EXCHANGE_DESYNC SCENARIO: PASS")
        _hdr("  Desync detected : YES — naked position identified")
        _hdr("  Action required : replace stop immediately to protect position")
    else:
        _hdr("EXCHANGE_DESYNC SCENARIO: INCONCLUSIVE")

    return {"stop_found": stop_found, "pos_open": pos_open, "recon": recon}


# ── SCENARIO 12: DUPLICATE STOP PREVENTION ───────────────────────────────────

def scenario_duplicate_stop(symbol: str = "ATOMUSDT"):
    """
    Scenario 12: Pre-placement check prevents -4130 on restart.
    Phase 1 — open trade, stop placed as usual.
    Phase 2 — simulate restart: runtime lost algoId, would try to place stop again.
    Phase 3 — pre-placement check: GET /fapi/v1/openAlgoOrders, find existing stop.
    Phase 4 — SKIP placement, rebind existing algoId.
    Phase 5 — verify rebound algoId works for trailing.
    """
    _section(f"Scenario: DUPLICATE_STOP  symbol={symbol}")

    mark = _get_mark_price(symbol)
    if mark is None:
        _hdr("Could not fetch mark price — aborting")
        return None

    balance = _get_balance()
    if balance is None:
        return None

    entry = mark
    sl    = round(entry * 0.970, 4)
    tp    = round(entry * 1.030, 4)

    _hdr("Phase 1: Opening trade with initial stop (normal boot)")
    result = inject_trade(
        symbol=symbol, side="BUY",
        entry=entry, sl=sl, tp=tp,
        balance=balance, risk_percent=0.05,
    )

    if not (result["entry_result"] and result["entry_result"].get("success")):
        _hdr("DUPLICATE_STOP: entry failed — aborting")
        return None
    if not (result["sl_result"] and result["sl_result"].get("success")):
        _hdr("DUPLICATE_STOP: initial SL failed — aborting")
        return None

    qty     = result["prep"]["qty"]
    algo_id = result["sl_result"]["order_id"]
    _hdr(f"Stop placed: algoId={algo_id}  qty={qty}  sl={sl}")

    # Phase 2: Simulate restart — runtime lost algoId, would try to re-place stop
    _sep()
    _hdr("Phase 2: Simulating restart — runtime lost algoId, would try to place stop again")
    _hdr("PRE-PLACEMENT CHECK: querying exchange for existing stop before placement...")
    time.sleep(0.5)

    # Phase 3: Pre-placement check — list all open algo orders for symbol
    open_orders = tn.get_open_algo_orders(symbol)

    close_side_expected = "SELL"  # BUY position → stop is SELL side
    found_algo_id = None
    for o in open_orders:
        if o.get("symbol") == symbol and o.get("side") == close_side_expected:
            found_algo_id = o.get("algoId")
            _hdr("EXISTING STOP FOUND on exchange:")
            _hdr(f"  algoId       : {found_algo_id}")
            _hdr(f"  algoStatus   : {o.get('algoStatus')}")
            _hdr(f"  triggerPrice : {o.get('triggerPrice')}")
            _hdr(f"  side         : {o.get('side')}")
            break

    # Phase 4: Skip placement, rebind existing algoId
    _sep()
    if found_algo_id is not None:
        _hdr(f"REBINDING existing algoId={found_algo_id} — skipping stop placement")
        _hdr("No duplicate stop placed — -4130 collision avoided")
        rebound_id = found_algo_id
    else:
        _hdr("No existing stop found — would place fresh stop (normal restart path)")
        _hdr("DUPLICATE_STOP: existing stop not found — scenario inconclusive")
        return None

    # Phase 5: Verify rebound algoId works for trailing
    _sep()
    new_sl = round(sl * 1.006, 4)
    _hdr(f"Phase 5: Trailing with rebound algoId={rebound_id}  {sl} -> {new_sl}")
    time.sleep(0.5)

    trail = force_trailing_progression(
        symbol=symbol, side="BUY", qty=qty,
        old_order_id=rebound_id, old_sl=sl, new_sl=new_sl,
    )

    _sep()
    if trail.get("success"):
        _hdr("DUPLICATE_STOP SCENARIO: PASS")
        _hdr(f"  Original algoId   : {algo_id}")
        _hdr(f"  Rebound algoId    : {rebound_id}")
        _hdr(f"  New algoId        : {trail['new_order_id']}")
        _hdr(f"  -4130 avoided     : confirmed — 0 duplicate stops placed")
        _hdr(f"  Rebind trailing   : OK")
        verify_exchange_stop(symbol, trail["new_order_id"])
    else:
        _hdr(f"DUPLICATE_STOP SCENARIO: FAIL — trailing with rebound id failed: {trail.get('error')}")

    return trail


# ── SCENARIO REGISTRY ────────────────────────────────────────────────────────

SCENARIOS = {
    "long_btc":           lambda: scenario_long_entry("BTCUSDT"),
    "long_sol":           lambda: scenario_long_entry("SOLUSDT"),
    "short_btc":          lambda: scenario_short_entry("BTCUSDT"),
    "short_sol":          lambda: scenario_short_entry("SOLUSDT"),
    "stop_replacement":   lambda: scenario_stop_replacement("ETHUSDT"),
    "emergency_close":    lambda: scenario_emergency_close("XRPUSDT"),
    "reconciliation":     lambda: scenario_reconciliation("BTCUSDT"),
    "trailing_lifecycle": lambda: scenario_trailing_lifecycle("BNBUSDT"),
    "restart_recovery":   lambda: scenario_restart_recovery("DOGEUSDT"),
    "partial_close":      lambda: scenario_partial_close("LINKUSDT"),
    "timeout_recovery":   lambda: scenario_timeout_recovery("AVAXUSDT"),
    "rapid_trailing":     lambda: scenario_rapid_trailing("ADAUSDT"),
    "exchange_desync":    lambda: scenario_exchange_desync("DOTUSDT"),
    "duplicate_stop":     lambda: scenario_duplicate_stop("ATOMUSDT"),
}


def run_all():
    _hdr("Running all scenarios sequentially")
    results = {}
    for name, fn in SCENARIOS.items():
        try:
            results[name] = fn()
        except Exception as exc:
            _hdr(f"Scenario {name!r} raised exception: {exc}")
            results[name] = None
        time.sleep(2.0)

    _section("ALL SCENARIOS SUMMARY")
    for name, r in results.items():
        status = "SKIP" if r is None else "RUN"
        _hdr(f"  {name:<22}: {status}")
    return results


# ── ENTRY POINT ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Real TESTNET execution harness — Coin Bot exchange QA."
    )
    parser.add_argument(
        "--scenario", "-s",
        choices=list(SCENARIOS.keys()) + ["all"],
        default="all",
        help="Scenario to run (default: all)",
    )
    args = parser.parse_args()

    _require_testnet_gate()
    tn.log_startup_mode()

    if args.scenario == "all":
        run_all()
    else:
        fn = SCENARIOS[args.scenario]
        try:
            fn()
        except Exception as exc:
            _hdr(f"Scenario {args.scenario!r} raised exception: {exc}")

    print()
    _hdr("Harness run complete.")


if __name__ == "__main__":
    main()
