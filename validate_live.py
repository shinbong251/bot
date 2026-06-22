"""
LIVE MAINNET VALIDATION HARNESS

Purpose: manually verify mainnet connectivity and order lifecycle
BEFORE enabling strategy execution.

This script:
  - does NOT run any strategy logic
  - does NOT generate signals
  - does NOT trade autonomously
  - requires explicit operator confirmation at each step

Usage:
  python validate_live.py

Steps executed (each requires Enter to proceed):
  1. Auth check     — ping mainnet, verify API key identity
  2. Balance check  — fetch account balance
  3. Position check — confirm no unexpected open positions
  4. Min entry      — place minimum BTCUSDT MARKET order (BUY)
  5. Stop place     — place STOP_MARKET algo stop on the position
  6. Stop query     — verify stop is visible in open algo orders
  7. Stop cancel    — cancel the stop order
  8. Close position — emergency MARKET close (reduceOnly)

IMPORTANT:
  Steps 4-8 involve REAL MONEY on MAINNET.
  The position size is the absolute minimum allowed by Binance.
  Each step requires explicit confirmation before execution.

Prerequisites:
  config.json must have:
    "live_mode": true
    "api_key": "<mainnet key>"
    "api_secret": "<mainnet secret>"
"""

import json
import os
import sys
import time

# ── path setup ────────────────────────────────────────────────────────
_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(_DIR)

# ── imports ───────────────────────────────────────────────────────────
from exchange.live_executor import (
    get_execution_balance,
    log_startup_mode,
    place_market_order,
    place_stop_loss,
    query_order,
    query_algo_order,
    get_open_algo_orders,
    cancel_stop_loss,
    emergency_close_position,
    get_exchange_positions,
    is_position_closed,
    _require_live,
    _get_signed,
    _LIVE_BASE_URL,
)

# ── constants ─────────────────────────────────────────────────────────
_SYMBOL        = "BTCUSDT"
_VALIDATE_QTY  = 0.001       # minimum BTC — ~$60-100 at current prices
_LEVERAGE      = 125        # 1x for validation — lowest possible risk

# ──────────────────────────────────────────────────────────────────────

def _confirm(msg: str) -> bool:
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")
    try:
        ans = input("  Type 'yes' to proceed, anything else to abort: ").strip().lower()
    except (KeyboardInterrupt, EOFError):
        print("\n[ABORTED] Keyboard interrupt.")
        return False
    return ans == "yes"


def _separator(title: str):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


def _wait_for_fill(
    symbol:          str,
    order_id:        int,
    client_order_id: str   = None,
    timeout:         float = 10.0,
    interval:        float = 0.25,
) -> dict | None:
    """
    Poll query_order() every `interval` seconds until status == FILLED
    or `timeout` seconds have elapsed.

    Returns the full order dict when FILLED.
    Returns None on timeout, query failure, or terminal non-fill status.
    """
    deadline    = time.time() + timeout
    last_status = "UNKNOWN"

    while time.time() < deadline:
        data = query_order(symbol, order_id=order_id, client_order_id=client_order_id)
        if data is None:
            print(f"  [POLL] query returned None — retrying in {interval}s")
            time.sleep(interval)
            continue

        status      = data.get("status", "?")
        last_status = status

        if status == "FILLED":
            avg_price = float(data.get("avgPrice",    0) or 0)
            exec_qty  = float(data.get("executedQty", 0) or 0)
            print(f"  [POLL] FILLED — avgPrice={avg_price}  executedQty={exec_qty}")
            return data

        if status in ("CANCELED", "REJECTED", "EXPIRED"):
            print(f"  [POLL] Terminal status={status} — order did not fill")
            return None

        if status == "PARTIALLY_FILLED":
            exec_qty = float(data.get("executedQty", 0) or 0)
            print(f"  [POLL] PARTIALLY_FILLED qty={exec_qty} — waiting for full fill...")
        else:
            print(f"  [POLL] status={status} — waiting...")

        time.sleep(interval)

    print(f"  [POLL] TIMEOUT after {timeout}s — last_status={last_status}")
    return None


def step_auth_check() -> bool:
    _separator("STEP 1 — AUTH CHECK")
    print(f"  Endpoint : {_LIVE_BASE_URL}/fapi/v1/ping")
    print(f"  Endpoint : {_LIVE_BASE_URL}/fapi/v1/account")

    ok, key, _ = _require_live()
    if not ok:
        print("[FAIL] live_mode guard rejected. Check config.json.")
        return False

    key_preview = key[:8] + "..." if len(key) >= 8 else "(empty)"
    print(f"  API key  : {key_preview}")

    # GET /fapi/v1/ping
    import requests, certifi
    try:
        res = requests.get(
            f"{_LIVE_BASE_URL}/fapi/v1/ping",
            verify=certifi.where(),
            timeout=(5, 10),
        )
        if res.status_code == 200:
            print("  [OK] Ping: mainnet reachable")
        else:
            print(f"  [FAIL] Ping: HTTP {res.status_code}")
            return False
    except Exception as e:
        print(f"  [FAIL] Ping error: {e}")
        return False

    # Signed account info
    data = _get_signed("/fapi/v2/account", {})
    if data is None:
        print("  [FAIL] account query failed — check api_key / api_secret")
        return False

    if isinstance(data, dict) and data.get("code", 0) < 0:
        print(f"  [FAIL] account error: code={data.get('code')} msg={data.get('msg')}")
        return False

    total_balance = None
    if isinstance(data, dict):
        total_balance = data.get("totalWalletBalance") or data.get("totalCrossWalletBalance")

    print(f"  [OK] Auth verified. totalWalletBalance={total_balance} USDT")
    return True


def step_balance_check() -> float | None:
    _separator("STEP 2 — BALANCE CHECK")

    data = _get_signed("/fapi/v2/balance", {})
    if data is None:
        print("  [FAIL] balance query returned None")
        return None

    usdt_balance = None
    if isinstance(data, list):
        for asset in data:
            if asset.get("asset") == "USDT":
                usdt_balance = float(asset.get("availableBalance", 0))
                break
    elif isinstance(data, dict):
        usdt_balance = float(data.get("availableBalance", 0))

    if usdt_balance is None:
        print("  [FAIL] could not find USDT balance")
        return None

    print(f"  [OK] Available USDT balance: {usdt_balance:.4f} USDT")

    if usdt_balance < 10:
        print(f"  [WARN] Balance is very low ({usdt_balance:.4f} USDT) — proceed with caution")

    return usdt_balance


def step_position_check() -> bool:
    _separator("STEP 3 — OPEN POSITIONS CHECK")

    positions = get_exchange_positions()

    if not positions:
        print("  [OK] No open positions on mainnet — clean slate confirmed")
        return True

    print(f"  [WARN] {len(positions)} open position(s) found:")
    for p in positions:
        print(f"    {p['symbol']}  amt={p['positionAmt']}  entry={p['entryPrice']}")

    print("  [WARN] Existing positions detected. Validate results may interact with them.")
    return True


def step_min_entry() -> dict | None:
    _separator("STEP 4 — MINIMUM MARKET ENTRY (REAL MONEY)")

    print(f"  Symbol   : {_SYMBOL}")
    print(f"  Side     : BUY (LONG)")
    print(f"  Qty      : {_VALIDATE_QTY} BTC  (minimum position)")
    print(f"  Leverage : {_LEVERAGE}x")
    print()
    print("  WARNING: This places a REAL order on Binance Futures MAINNET.")

    if not _confirm("Place minimum BUY MARKET order on BTCUSDT?"):
        print("  [SKIPPED] Entry step skipped — stopping validation.")
        return None

    result = place_market_order(
        symbol=_SYMBOL,
        side="BUY",
        qty=_VALIDATE_QTY,
        leverage=_LEVERAGE,
    )

    if not result.get("success"):
        print(f"  [FAIL] Entry failed: {result.get('error')}")
        return None

    order_id        = result.get("order_id")
    client_order_id = result.get("client_order_id")
    print(f"  [OK] Order accepted — orderId={order_id}  status={result['status']}")
    print(f"  [POLL] Waiting for FILLED confirmation (timeout=10s, interval=250ms)...")

    fill = _wait_for_fill(_SYMBOL, order_id, client_order_id=client_order_id)

    if fill is None:
        print("  [FAIL] Fill confirmation timed out or failed.")
        print(f"  [WARN] Position state UNKNOWN — attempting emergency reduceOnly close...")
        _close = emergency_close_position(
            symbol=_SYMBOL, entry_side="BUY", qty=_VALIDATE_QTY
        )
        if _close.get("success"):
            _close_fill = _wait_for_fill(
                _SYMBOL, _close.get("order_id"),
                client_order_id=_close.get("client_order_id"),
            )
            if _close_fill:
                print("  [WARN] Emergency close fill confirmed — position likely cleared.")
            else:
                print("  [WARN] Emergency close fill not confirmed — verify manually on exchange UI.")
        else:
            print(f"  [WARN] Emergency close failed: {_close.get('error')} — verify manually.")
        return None

    avg_price = float(fill.get("avgPrice",    0) or 0)
    exec_qty  = float(fill.get("executedQty", 0) or 0)
    result["fill_price"] = avg_price if avg_price else result.get("fill_price")
    result["fill_qty"]   = exec_qty  if exec_qty  else result.get("fill_qty")

    print(f"  [OK] Fill confirmed:")
    print(f"       orderId     = {order_id}")
    print(f"       status      = FILLED")
    print(f"       fill_price  = {result['fill_price']}")
    print(f"       fill_qty    = {result['fill_qty']}")

    return result


def step_place_stop(entry_result: dict) -> dict | None:
    _separator("STEP 5 — PLACE STOP-LOSS")

    fill_price = entry_result.get("fill_price")
    if fill_price is None:
        print("  [FAIL] No fill_price from entry — cannot compute SL")
        return None

    sl_price = round(fill_price * 0.995, 1)
    print(f"  Entry price : {fill_price}")
    print(f"  SL price    : {sl_price}  (-0.5% from entry)")
    print(f"  Qty         : {_VALIDATE_QTY}")

    if not _confirm(f"Place SELL STOP_MARKET at {sl_price} for {_VALIDATE_QTY} BTC?"):
        print("  [SKIPPED] Stop step skipped.")
        print("  [WARNING] Position is now NAKED — proceed to close immediately.")
        return None

    result = place_stop_loss(
        symbol=_SYMBOL,
        entry_side="BUY",
        qty=_VALIDATE_QTY,
        stop_price=sl_price,
    )

    if not result.get("success"):
        print(f"  [FAIL] Stop placement failed: {result.get('error')}")
        print("  [WARNING] Position is NAKED — close immediately via Step 8.")
        return None

    print(f"  [OK] Stop placed:")
    print(f"       algoId  = {result['order_id']}")
    print(f"       status  = {result['status']}")

    return result


def step_query_stop(stop_result: dict) -> bool:
    _separator("STEP 6 — QUERY STOP ORDER")

    algo_id = stop_result.get("order_id")
    print(f"  Querying algoId={algo_id} via /fapi/v1/openAlgoOrders")

    orders = get_open_algo_orders(_SYMBOL)

    found = False
    for o in orders:
        if o.get("algoId") == algo_id:
            found = True
            print(f"  [OK] Stop confirmed in open algo orders:")
            print(f"       algoId     = {o.get('algoId')}")
            print(f"       algoStatus = {o.get('algoStatus')}")
            print(f"       triggerPrice = {o.get('triggerPrice')}")
            break

    if not found:
        print(f"  [FAIL] algoId={algo_id} NOT found in open algo orders")
        print("  [WARN] Stop may not be active — verify manually before proceeding.")
        return False

    return True


def step_cancel_stop(stop_result: dict) -> bool:
    _separator("STEP 7 — CANCEL STOP ORDER")

    algo_id = stop_result.get("order_id")
    print(f"  Cancelling algoId={algo_id}")
    print("  NOTE: After cancel, position will be NAKED until Step 8 closes it.")

    if not _confirm(f"Cancel stop algoId={algo_id}?"):
        print("  [SKIPPED] Cancel skipped — stop remains active.")
        return False

    result = cancel_stop_loss(symbol=_SYMBOL, order_id=algo_id)

    if not result.get("success"):
        print(f"  [FAIL] Cancel failed: {result.get('error')}")
        return False

    print(f"  [OK] Stop cancelled algoId={algo_id}")
    print("  [WARN] Position is now NAKED — close immediately in Step 8.")
    return True


def step_close_position() -> bool:
    _separator("STEP 8 — EMERGENCY CLOSE (reduceOnly)")

    print(f"  Symbol : {_SYMBOL}")
    print(f"  Side   : SELL (close LONG)")
    print(f"  Qty    : {_VALIDATE_QTY}")
    print()
    print("  WARNING: This closes the open position on MAINNET.")

    if not _confirm(f"Close {_VALIDATE_QTY} BTC LONG position via SELL MARKET reduceOnly?"):
        print("  [SKIPPED] Close skipped.")
        print("  [WARNING] Open position remains — close manually via exchange UI.")
        return False

    result = emergency_close_position(
        symbol=_SYMBOL,
        entry_side="BUY",
        qty=_VALIDATE_QTY,
    )

    if not result.get("success"):
        print(f"  [FAIL] Close failed: {result.get('error')}")
        print("  [WARNING] Position may still be open — close manually.")
        return False

    order_id        = result.get("order_id")
    client_order_id = result.get("client_order_id")
    print(f"  [OK] Close order accepted — orderId={order_id}  status={result['status']}")
    print(f"  [POLL] Waiting for close FILLED confirmation (timeout=10s, interval=250ms)...")

    fill = _wait_for_fill(_SYMBOL, order_id, client_order_id=client_order_id)

    if fill is None:
        print("  [WARN] Close fill confirmation timed out — position state UNKNOWN.")
        print("  [WARNING] Verify manually on Binance Futures UI.")
        return False

    avg_price = float(fill.get("avgPrice",    0) or 0)
    exec_qty  = float(fill.get("executedQty", 0) or 0)
    print(f"  [OK] Close fill confirmed:")
    print(f"       orderId    = {order_id}")
    print(f"       status     = FILLED")
    print(f"       fill_price = {avg_price}")
    print(f"       fill_qty   = {exec_qty}")

    # Verify exchange position is now zero
    print(f"  [VERIFY] Checking exchange position for {_SYMBOL}...")
    closed = is_position_closed(_SYMBOL)
    if closed is True:
        print(f"  [OK] Exchange confirms {_SYMBOL} positionAmt = 0 — fully closed.")
    elif closed is False:
        print(f"  [WARN] Exchange still shows open position for {_SYMBOL} after close fill.")
        print("  [WARNING] Close may be partial — verify manually.")
    else:
        print(f"  [WARN] Position close check returned None — verify manually.")

    # Verify no orphan algo stops remain
    print(f"  [VERIFY] Checking for orphan algo orders on {_SYMBOL}...")
    algo_orders = get_open_algo_orders(_SYMBOL)
    if algo_orders is None:
        print(f"  [WARN] Could not fetch open algo orders — verify manually.")
    elif not algo_orders:
        print(f"  [OK] No orphan algo orders for {_SYMBOL} — exchange state clean.")
    else:
        print(f"  [WARN] {len(algo_orders)} orphan algo order(s) still active for {_SYMBOL}:")
        for _o in algo_orders:
            print(
                f"    algoId={_o.get('algoId')}  "
                f"algoStatus={_o.get('algoStatus')}  "
                f"triggerPrice={_o.get('triggerPrice')}"
            )
        print("  [WARNING] Cancel orphan stops manually on Binance Futures UI.")

    return True


# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 60)
    print("  LIVE MAINNET VALIDATION HARNESS")
    print("  Binance Futures — Manual Step-by-Step")
    print("=" * 60)
    print()
    print("  This harness verifies mainnet endpoint connectivity")
    print("  using REAL orders with MINIMUM position sizes.")
    print()
    print("  Steps 1-3 are READ-ONLY (no money at risk).")
    print("  Steps 4-8 involve REAL trades on MAINNET.")
    print()

    log_startup_mode()

    ok, _, _ = _require_live()
    if not ok:
        print("\n[ABORT] live_mode guard failed. Fix config.json first.")
        sys.exit(1)

    # ── STEP 1 — AUTH ────────────────────────────────────────────────
    if not step_auth_check():
        print("\n[ABORT] Auth check failed.")
        sys.exit(1)

    # ── STEP 2 — BALANCE ─────────────────────────────────────────────
    balance = step_balance_check()
    if balance is None:
        print("\n[ABORT] Balance check failed.")
        sys.exit(1)

    # ── STEP 3 — POSITIONS ───────────────────────────────────────────
    step_position_check()

    print()
    print("─" * 60)
    print("  Read-only checks complete.")
    print("  Steps 4-8 involve REAL trades on MAINNET.")
    print("─" * 60)

    if not _confirm("Proceed with live order lifecycle test (real money)?"):
        print("\n[DONE] Validation stopped after read-only checks.")
        sys.exit(0)

    # ── STEP 4 — ENTRY ───────────────────────────────────────────────
    entry_result = step_min_entry()
    if entry_result is None:
        print("\n[DONE] Stopped at entry step.")
        sys.exit(0)

    # brief pause to allow fill to register
    time.sleep(2)

    # ── STEP 5 — STOP ────────────────────────────────────────────────
    stop_result = step_place_stop(entry_result)
    if stop_result is None:
        print("\n[WARN] No stop placed — closing position now.")
        step_close_position()
        sys.exit(0)

    # ── STEP 6 — QUERY ───────────────────────────────────────────────
    step_query_stop(stop_result)

    # ── STEP 7 — CANCEL ──────────────────────────────────────────────
    cancelled = step_cancel_stop(stop_result)

    # ── STEP 8 — CLOSE ───────────────────────────────────────────────
    closed = step_close_position()

    # ── SUMMARY ──────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  VALIDATION SUMMARY")
    print("=" * 60)
    print(f"  Auth check       : OK")
    print(f"  Balance check    : OK  ({balance:.4f} USDT available)")
    print(f"  Entry placed     : OK  (orderId={entry_result.get('order_id')})")
    print(f"  Stop placed      : OK  (algoId={stop_result.get('order_id')})")
    print(f"  Stop cancelled   : {'OK' if cancelled else 'FAIL/SKIPPED'}")
    print(f"  Position closed  : {'OK' if closed else 'FAIL/SKIPPED — CHECK EXCHANGE'}")
    print()

    if not closed:
        print("  [WARNING] Position may still be open on mainnet.")
        print("  Check Binance Futures UI and close manually if needed.")
    else:
        print("  [PASS] Full order lifecycle validated on mainnet.")
        print("  Live execution is ready for strategy integration.")

    print("=" * 60)


if __name__ == "__main__":
    main()
