"""
cleanup_orphan_positions.py — Manual operator utility.

Lists all exchange-only orphan positions (positions on the exchange with no
matching local trade record) and closes them one by one via reduceOnly MARKET
orders after operator confirmation.

NEVER runs automatically — must be invoked manually by the operator.

Usage:
    python cleanup_orphan_positions.py

Config required (config.json):
    testnet_mode       bool   — must be true
    testnet_api_key    str
    testnet_api_secret str
"""

import json
import sys
import os

_BOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _BOT_DIR)


def _load_config() -> dict:
    path = os.path.join(_BOT_DIR, "config.json")
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"[CLEANUP] config.json read error: {e}")
        sys.exit(1)


def _load_local_trades() -> list:
    from state_manager import load_open_trades
    try:
        return load_open_trades()
    except Exception as e:
        print(f"[CLEANUP] local trade load error: {e}")
        return []


def main():
    cfg = _load_config()

    if not cfg.get("testnet_mode"):
        print("[CLEANUP] testnet_mode is not enabled in config.json — aborting")
        sys.exit(1)

    from exchange import testnet_executor as _tn
    from exchange.binance_client import get_exchange_info

    print("[CLEANUP] Fetching exchange positions...")
    exchange_positions = _tn.get_exchange_positions()

    if not exchange_positions:
        print("[CLEANUP] No open exchange positions found.")
        return

    local_trades = _load_local_trades()
    local_symbols = {
        t.get("symbol")
        for t in local_trades
        if t.get("status") == "OPEN" and t.get("symbol")
    }

    orphans = [
        p for p in exchange_positions
        if p.get("symbol") not in local_symbols
    ]

    if not orphans:
        print("[CLEANUP] No orphan exchange positions found — all matched to local trades.")
        return

    print(f"\n[CLEANUP] Found {len(orphans)} orphan exchange position(s):\n")
    for p in orphans:
        sym = p.get("symbol")
        amt = p.get("positionAmt", 0)
        entry = p.get("entryPrice", "?")
        pnl = p.get("unrealizedProfit", "?")
        print(f"  {sym}  amt={amt}  entry={entry}  unrealizedPnl={pnl}")

    print()
    confirm = input("Close ALL orphan positions via reduceOnly MARKET? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("[CLEANUP] Aborted — no positions closed.")
        return

    for p in orphans:
        sym = p.get("symbol")
        amt = float(p.get("positionAmt", 0))

        if amt == 0:
            print(f"[CLEANUP] {sym} amt=0 — skipping")
            continue

        side = "SELL" if amt > 0 else "BUY"
        abs_qty = abs(amt)

        print(f"[CLEANUP] Closing {sym}: {side} {abs_qty} (reduceOnly MARKET)...")

        result = _close_orphan(sym, side, abs_qty)

        if result.get("success"):
            print(f"[CLEANUP] {sym} CLOSED — orderId={result.get('order_id')}")
        else:
            print(f"[CLEANUP] {sym} CLOSE FAILED — error={result.get('error')}")

    print("\n[CLEANUP] Done.")


def _close_orphan(symbol: str, side: str, qty: float) -> dict:
    from exchange.testnet_executor import _post, _require_testnet, _new_client_order_id

    ok, _, _ = _require_testnet()
    if not ok:
        return {"success": False, "order_id": None, "error": "testnet_mode not enabled"}

    from exchange.precision import round_qty
    rounded_qty = round_qty(symbol, qty)
    if rounded_qty is None or rounded_qty <= 0:
        return {"success": False, "order_id": None, "error": f"qty rounding failed for {symbol}"}

    client_order_id = _new_client_order_id("CB-CLN")
    params = {
        "symbol":       symbol,
        "side":         side,
        "type":         "MARKET",
        "quantity":     rounded_qty,
        "reduceOnly":   "true",
        "newClientOrderId": client_order_id,
    }

    print(f"[CLEANUP] POST /fapi/v1/order {params}")
    raw = _post("/fapi/v1/order", params)

    if raw is None:
        return {"success": False, "order_id": None, "error": "POST timeout — state unknown"}

    if isinstance(raw, dict) and isinstance(raw.get("code"), int) and raw["code"] < 0:
        return {"success": False, "order_id": None, "error": f"code={raw['code']} msg={raw.get('msg','?')}"}

    order_id = raw.get("orderId") if isinstance(raw, dict) else None
    return {"success": True, "order_id": order_id, "error": None}


if __name__ == "__main__":
    main()
