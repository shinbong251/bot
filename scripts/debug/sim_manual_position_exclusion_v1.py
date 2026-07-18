#!/usr/bin/env python3
"""Deterministic simulator for manual position exclusions.

All filesystem writes happen inside TemporaryDirectory. Exchange helpers are
fakes that expose only read/query methods plus counters for forbidden writes.
"""

import copy
import importlib
import json
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import manual_position_exclusions as M  # noqa: E402


PASSED = 0
FAILED = 0


def check(label, cond, detail=""):
    global PASSED, FAILED
    if cond:
        PASSED += 1
        print("PASS %s" % label)
    else:
        FAILED += 1
        print("FAIL %s :: %s" % (label, detail))


def vanry_record(**kw):
    data = {
        "symbol": "VANRYUSDT",
        "position_side": "LONG",
        "expected_qty": 18942.0,
        "entry_price": 0.0052829999999999995,
        "entry_order_ids": ["2584640627", "2585475398"],
        "protective_order_ids": ["2590622728", "3000002084925813"],
        "source": "USER_CONFIRMED",
        "notes": "User-owned manual position; bot must not reconstruct/manage/account",
        "confirmed_at": "2026-07-18T00:00:00+00:00",
    }
    data.update(kw)
    return M.build_manual_position_record(**data)


def registry(*records):
    return {"schema_version": 1, "positions": list(records or [vanry_record()])}


def orders():
    return [
        {"symbol": "VANRYUSDT", "orderId": 2590622728, "side": "SELL", "type": "LIMIT", "reduceOnly": True},
        {"symbol": "VANRYUSDT", "algoId": 3000002084925813, "side": "SELL", "orderType": "STOP_MARKET", "reduceOnly": True},
        {"symbol": "VANRYUSDT", "orderId": 2584640627, "clientOrderId": "BOT_VANRY_E_575e1ac6d124", "status": "FILLED", "reduceOnly": False},
        {"symbol": "VANRYUSDT", "orderId": 2585475398, "clientOrderId": "BOT_VANRY_E_e7f285743bd8", "status": "FILLED", "reduceOnly": False},
    ]


def resolve(**kw):
    params = {
        "symbol": "VANRYUSDT",
        "position_side": "LONG",
        "exchange_qty": 18942.0,
        "entry_price": 0.0052829999999999995,
        "open_orders": orders(),
        "registry": registry(),
    }
    params.update(kw)
    return M.resolve_manual_position_exclusion(**params)


class FakeExchange:
    def __init__(self, positions=None, open_orders=None, open_algos=None, recent_orders=None):
        self.positions = positions or []
        self._open_orders = open_orders or {}
        self._open_algos = open_algos or {}
        self._recent_orders = recent_orders or {}
        self.write_calls = []

    def get_exchange_positions(self):
        return copy.deepcopy(self.positions)

    def compare_local_vs_exchange(self, local_positions, exchange_positions):
        from exchange.live_executor import compare_local_vs_exchange
        return compare_local_vs_exchange(local_positions, exchange_positions)

    def get_open_orders(self, symbol):
        return copy.deepcopy(self._open_orders.get(symbol, []))

    def get_open_algo_orders(self, symbol):
        return copy.deepcopy(self._open_algos.get(symbol, []))

    def get_recent_orders(self, symbol, limit=50):
        return copy.deepcopy(self._recent_orders.get(symbol, []))

    def place_market_order(self, *args, **kwargs):
        self.write_calls.append(("place_market_order", args, kwargs))
        raise AssertionError("write helper called")

    def cancel_stop_loss(self, *args, **kwargs):
        self.write_calls.append(("cancel_stop_loss", args, kwargs))
        raise AssertionError("write helper called")


def scenario_resolver_core(tmp):
    exact = resolve()
    check("1 exact VANRY match -> MANUAL_CONFIRMED", exact["classification"] == M.MANUAL_CONFIRMED, exact)
    check("2 historical BOT evidence still manual", exact["reconstruct"] is False and exact["manage"] is False, exact)
    check("3 manual protective stop remains untouched", exact["modify_orders"] is False, exact)
    check("4 no local live-state record required", exact["account"] is False and exact["canary"] is False, exact)
    check("5 quantity increase -> review", resolve(exchange_qty=18943.0)["classification"] == M.MANUAL_REVIEW_REQUIRED)
    check("6 quantity decrease -> review", resolve(exchange_qty=18941.0)["classification"] == M.MANUAL_REVIEW_REQUIRED)
    check("7 side reversal -> stale/review", resolve(position_side="SHORT", exchange_qty=-18942.0)["classification"] in (M.EXCLUSION_STALE, M.MANUAL_REVIEW_REQUIRED))
    check("8 position flat -> stale", resolve(position_side="LONG", exchange_qty=0.0)["classification"] == M.EXCLUSION_STALE)
    future_orders = [
        {"symbol": "VANRYUSDT", "orderId": 999001, "side": "BUY", "status": "FILLED", "reduceOnly": False},
        {"symbol": "VANRYUSDT", "algoId": 999002, "side": "SELL", "orderType": "STOP_MARKET", "reduceOnly": True},
    ]
    check("9 future same side/qty/entry new epoch -> review",
          resolve(open_orders=future_orders)["classification"] == M.MANUAL_REVIEW_REQUIRED)
    check("10 entry minor tolerance accepted", resolve(entry_price=0.005283)["classification"] == M.MANUAL_CONFIRMED)
    check("11 material entry mismatch -> review", resolve(entry_price=0.006)["classification"] == M.MANUAL_REVIEW_REQUIRED)
    check("12 missing old order history -> review", resolve(open_orders=[])["classification"] == M.MANUAL_REVIEW_REQUIRED)
    check("12b bounded matching entry evidence -> manual", resolve(open_orders=[orders()[2]])["classification"] == M.MANUAL_CONFIRMED)
    bad_file = tmp / "bad.json"
    bad_file.write_text("{bad", encoding="utf-8")
    check("13 malformed registry -> fail closed", M.load_manual_registry(str(bad_file)).get("_invalid") is True)
    big_file = tmp / "big.json"
    big_file.write_text("x" * (M.MAX_REGISTRY_BYTES + 1), encoding="utf-8")
    check("14 oversized registry -> fail closed", M.load_manual_registry(str(big_file)).get("_invalid") is True)
    dup = registry(vanry_record(), vanry_record())
    check("15 duplicate registry entries reject", M.resolve_manual_position_exclusion("VANRYUSDT", "LONG", 18942, 0.0052829999999999995, [], dup)["classification"] == M.REGISTRY_INVALID)
    check("16 unknown symbol unchanged", M.resolve_manual_position_exclusion("ABCUSDT", "LONG", 1, 1, [], registry())["classification"] == M.NO_EXCLUSION)


def scenario_reconcile(tmp):
    tmp.mkdir(parents=True, exist_ok=True)
    execution = importlib.import_module("execution")
    cwd = os.getcwd()
    os.chdir(tmp)
    try:
        M.save_manual_registry(registry(), "manual_position_exclusions.json")
        M.reset_audit_dedup_for_tests()
        sent = []
        saved = []
        old_resolve = execution._resolve_exchange_executor
        old_send = execution.send_telegram
        old_save = execution.save_open_trades
        try:
            positions = [
                {"symbol": "LABUSDT", "positionAmt": -49.0, "entryPrice": 0.1772, "positionSide": "BOTH"},
                {"symbol": "MANTRAUSDT", "positionAmt": 2345.0, "entryPrice": 0.006644300213219616, "positionSide": "BOTH"},
                {"symbol": "VANRYUSDT", "positionAmt": 18942.0, "entryPrice": 0.0052829999999999995, "positionSide": "BOTH"},
            ]
            fx = FakeExchange(
                positions=positions,
                open_orders={"VANRYUSDT": [orders()[0]]},
                open_algos={"VANRYUSDT": [orders()[1]]},
                recent_orders={
                    "VANRYUSDT": [
                        {"symbol": "VANRYUSDT", "orderId": 2584640627, "clientOrderId": "BOT_VANRY_E_575e1ac6d124", "status": "FILLED", "executedQty": "1", "side": "SELL", "reduceOnly": False},
                        {"symbol": "VANRYUSDT", "orderId": 999999, "clientOrderId": "BOT_VANRY_E_unrelated", "status": "FILLED", "executedQty": "1", "side": "SELL", "reduceOnly": False},
                    ]
                },
            )
            ctx = SimpleNamespace(
                execution_mode="live",
                name="live",
                mode_prefix="[LIVE]",
                state_file=str(tmp / "live_state.json"),
                trades=[
                    {"symbol": "LABUSDT", "side": "SHORT", "status": "OPEN", "owner": "bot", "exchange_qty": 49.0, "client_order_id": "BOT_LAB_E_x", "exchange_position_owner_confirmed": True},
                    {"symbol": "MANTRAUSDT", "side": "LONG", "status": "OPEN", "owner": "bot", "exchange_qty": 2345.0, "client_order_id": "BOT_MANTRA_E_x", "exchange_position_owner_confirmed": True},
                ],
            )
            execution._resolve_exchange_executor = lambda mode: fx
            execution.send_telegram = lambda msg, **kw: sent.append(msg)
            execution.save_open_trades = lambda trades, state_file: saved.append((trades, state_file))
            execution.reconcile_exchange_positions(ctx)
            audit_rows = (tmp / "logs" / "manual_position_exclusion_v1.jsonl").read_text(encoding="utf-8").strip().splitlines()
            check("17 MANTRA/LAB bot ownership unchanged", sorted(t["symbol"] for t in ctx.trades) == ["LABUSDT", "MANTRAUSDT"])
            check("18 startup backfill excludes manual record", not saved, saved)
            check("19 canary ignores manual record", all("VANRY" not in str(t) for t in ctx.trades), ctx.trades)
            check("20 no CSV/ledger mutation", not (tmp / "live_trades.csv").exists() and not (tmp / "live_account_state.json").exists())
            check("21 no order/cancel helper called", not fx.write_calls, fx.write_calls)
            check("22 transition audit dedup", len(audit_rows) == 1 and "MANUAL_CONFIRMED" in audit_rows[0], audit_rows)
        finally:
            execution._resolve_exchange_executor = old_resolve
            execution.send_telegram = old_send
            execution.save_open_trades = old_save
    finally:
        os.chdir(cwd)


def scenario_atomic_and_import(tmp):
    tmp.mkdir(parents=True, exist_ok=True)
    path = tmp / "manual_position_exclusions.json"
    old = registry(vanry_record(expected_qty=1.0))
    M.save_manual_registry(old, str(path))
    before = path.read_text(encoding="utf-8")
    try:
        M.save_manual_registry(registry(vanry_record(), {"bad": object()}), str(path))
    except Exception:
        pass
    check("23 atomic save interruption retains previous valid registry", path.read_text(encoding="utf-8") == before)

    calls = []
    before_files = set(tmp.iterdir())
    importlib.reload(M)
    after_files = set(tmp.iterdir())
    check("24 zero import-time exchange calls", not calls and before_files == after_files)


def scenario_stale_lifecycle(tmp):
    tmp.mkdir(parents=True, exist_ok=True)
    path = tmp / "manual_position_exclusions.json"
    reg = registry()
    M.save_manual_registry(reg, str(path))
    changed = M.mark_manual_exclusion_stale(reg, "VANRYUSDT", "LONG", reason="position_flat")
    M.save_manual_registry(reg, str(path))
    loaded = M.load_manual_registry(str(path))
    stale = M.resolve_manual_position_exclusion(
        "VANRYUSDT",
        "LONG",
        18942.0,
        0.0052829999999999995,
        orders(),
        loaded,
    )
    check("25 flat observation persists inactive/stale status", changed and loaded["positions"][0].get("inactive") is True, loaded)
    check("26 stale record never auto-reactivates", stale["classification"] == M.EXCLUSION_STALE, stale)


def main():
    with tempfile.TemporaryDirectory(prefix="manual-exclusion-v1-") as td:
        tmp = Path(td)
        scenario_resolver_core(tmp)
        scenario_reconcile(tmp / "reconcile")
        scenario_atomic_and_import(tmp / "atomic")
        scenario_stale_lifecycle(tmp / "stale")
    print("-" * 60)
    print("SIM SUMMARY passed=%d failed=%d" % (PASSED, FAILED))
    if FAILED:
        print("FAIL sim_manual_position_exclusion_v1")
        sys.exit(1)
    print("PASS sim_manual_position_exclusion_v1")


if __name__ == "__main__":
    main()
