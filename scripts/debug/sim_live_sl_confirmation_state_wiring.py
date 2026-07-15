#!/usr/bin/env python3
import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import execution
from scripts.debug import audit_research_rolling_health as health


def check(name, condition, detail=""):
    if not condition:
        raise AssertionError(f"{name}: {detail}")
    print(f"PASS {name}")


def base_trade(**overrides):
    trade = {
        "symbol": "RAVEUSDT",
        "side": "SHORT",
        "status": "OPEN",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "owner": "bot",
        "sl": 0.251925,
        "qty": 12.0,
        "exchange_qty": 12.0,
        "exchange_sl_id": None,
        "exchange_sl_price_confirmed": None,
        "sl_sync_fail_count": 0,
        "entry_state": "ENTRY_CONFIRMED",
        "entry_source": "actual_exchange_fill",
    }
    trade.update(overrides)
    return trade


def valid_order(**overrides):
    order = {
        "symbol": "RAVEUSDT",
        "side": "BUY",
        "algoId": 3000002076786300,
        "clientAlgoId": "BOT_RAVE_S_abcdef123456",
        "algoStatus": "NEW",
        "triggerPrice": "0.251925",
        "reduceOnly": True,
        "quantity": "12.0",
        "closePosition": False,
    }
    order.update(overrides)
    return order


def valid_result(**overrides):
    raw = valid_order(closePosition=True, reduceOnly=False)
    raw.update(overrides.pop("raw", {}))
    result = {
        "success": True,
        "order_id": raw.get("algoId"),
        "raw": raw,
    }
    result.update(overrides)
    return result


def test_initial_placement_confirmed():
    trade = base_trade()
    changed = execution._confirm_exchange_sl_from_result(
        trade,
        valid_result(),
        source="initial_stop_placement",
    )
    check("initial placement confirmed changes state", changed)
    check("initial placement stores id", trade["exchange_sl_id"] == 3000002076786300, trade)
    check("initial placement stores trigger", trade["exchange_sl_price_confirmed"] == 0.251925, trade)


def test_initial_placement_no_trigger_fail_safe():
    trade = base_trade()
    result = valid_result(raw={"triggerPrice": None})
    changed = execution._confirm_exchange_sl_from_result(
        trade,
        result,
        source="initial_stop_placement",
    )
    check("initial placement without trigger not confirmed", not changed, trade)
    check("initial placement without trigger leaves confirmation unset", trade["exchange_sl_price_confirmed"] is None, trade)


def test_rebound_valid_and_invalid():
    trade = base_trade(exchange_sl_sync_pending=0.251925, exchange_sl_sync_error="old", sl_sync_fail_count=2)
    changed = execution._confirm_exchange_sl_from_order(
        trade,
        valid_order(),
        source="stop_rebound",
        require_bot_id=True,
    )
    check("rebound valid stop confirmed", changed, trade)
    check("rebound clears stale pending", "exchange_sl_sync_pending" not in trade, trade)
    check("rebound resets fail count", trade.get("sl_sync_fail_count") == 0, trade)

    bad_cases = {
        "wrong side": valid_order(side="SELL"),
        "wrong quantity": valid_order(quantity="11.999"),
        "wrong trigger": valid_order(triggerPrice="0.252000"),
        "missing bot id": valid_order(clientAlgoId="manual_stop"),
    }
    for label, order in bad_cases.items():
        bad = base_trade()
        changed = execution._confirm_exchange_sl_from_order(
            bad,
            order,
            source="stop_rebound",
            require_bot_id=True,
        )
        check(f"rebound {label} not confirmed", not changed and bad["exchange_sl_price_confirmed"] is None, bad)


def test_audit_repair_success_confirmed():
    trade = base_trade()
    changed = execution._confirm_exchange_sl_from_result(
        trade,
        valid_result(),
        source="sl_audit_repair",
    )
    check("audit repair success stores confirmed trigger", changed and trade["exchange_sl_price_confirmed"] == 0.251925, trade)


class FakeExecutor:
    def __init__(self, order=None, open_algos=None):
        self.order = order
        self.open_algos = open_algos if open_algos is not None else []
        self.placed = 0
        self.cancelled = 0

    def query_algo_order(self, symbol, algo_id):
        return self.order

    def get_open_algo_orders(self, symbol):
        return self.open_algos

    def update_trailing_stop(self, **kwargs):
        return {
            "success": True,
            "new_order_id": "NEW_TRAIL",
            "cancel_ok": True,
        }

    def place_stop_loss(self, **kwargs):
        self.placed += 1
        return valid_result()

    def cancel_stop_loss(self, *args, **kwargs):
        self.cancelled += 1
        return {"success": True}


def with_patched_runtime(fake_executor, func):
    originals = {
        "resolver": execution._resolve_exchange_executor,
        "save": execution.save_open_trades,
        "telegram": execution.send_telegram,
    }
    saves = []
    try:
        execution._resolve_exchange_executor = lambda mode: fake_executor
        execution.save_open_trades = lambda trades, state_file: saves.append((copy.deepcopy(trades), state_file))
        execution.send_telegram = lambda *args, **kwargs: None
        return func(saves)
    finally:
        execution._resolve_exchange_executor = originals["resolver"]
        execution.save_open_trades = originals["save"]
        execution.send_telegram = originals["telegram"]


def test_periodic_audit_refresh():
    trade = base_trade(exchange_sl_id=3000002076786300)
    fake = FakeExecutor(order=valid_order())
    ctx = SimpleNamespace(execution_mode="live", trades=[trade], state_file="/tmp/no_write_live_state.json", mode_prefix="[LIVE]")

    def run(saves):
        execution.audit_exchange_sl(ctx)
        check("periodic audit refreshes stale None", trade["exchange_sl_price_confirmed"] == 0.251925, trade)
        check("periodic audit did not place orders", fake.placed == 0)
        check("periodic audit did not cancel orders", fake.cancelled == 0)

    with_patched_runtime(fake, run)


def test_missing_stop_remains_failure():
    missing = base_trade(exchange_sl_id=None, exchange_sl_price_confirmed=None)
    no_price = base_trade(exchange_sl_id="SL1", exchange_sl_price_confirmed=None)
    check("missing stop remains health failure", health.open_trade_has_unresolved_sl_sync_risk(missing, []) is True)
    check("missing confirmed price remains health failure", health.open_trade_has_unresolved_sl_sync_risk(no_price, []) is True)


def test_trailing_sync_unchanged():
    trade = base_trade(exchange_sl_id="OLD_TRAIL", exchange_sl_price_confirmed=0.251925, sl=0.250000)
    fake = FakeExecutor()
    ctx = SimpleNamespace(execution_mode="live", mode_prefix="[LIVE]")

    def run(_saves):
        ok = execution._sync_testnet_trailing_sl(trade, ctx, old_sl=0.251925, current_price=0.246)
        check("trailing sync still succeeds", ok is True, trade)
        check("trailing sync still sets new id", trade["exchange_sl_id"] == "NEW_TRAIL", trade)
        check("trailing sync still sets local new sl", trade["exchange_sl_price_confirmed"] == 0.250000, trade)

    with_patched_runtime(fake, run)


def test_duplicate_verification_idempotent():
    trade = base_trade()
    order = valid_order()
    first = execution._confirm_exchange_sl_from_order(trade, order, source="audit", require_bot_id=True)
    snapshot = json.dumps(trade, sort_keys=True, default=str)
    second = execution._confirm_exchange_sl_from_order(trade, order, source="audit", require_bot_id=True)
    check("duplicate first verification changed", first is True)
    check("duplicate second verification idempotent", second is False, trade)
    check("duplicate state unchanged", json.dumps(trade, sort_keys=True, default=str) == snapshot, trade)


def test_existing_valid_state_and_health_transition():
    ena_like = base_trade(
        symbol="ENAUSDT",
        side="LONG",
        sl=0.0835,
        exchange_sl_id=3000002079961850,
        exchange_sl_price_confirmed=0.0835,
        exchange_qty=203.0,
        qty=203.0,
    )
    false_red = copy.deepcopy(ena_like)
    false_red["exchange_sl_price_confirmed"] = None
    check("ENA style valid state remains healthy", health.open_trade_has_unresolved_sl_sync_risk(ena_like, []) is False, ena_like)
    check("false RED before exchange proof", health.open_trade_has_unresolved_sl_sync_risk(false_red, []) is True, false_red)
    order = valid_order(
        symbol="ENAUSDT",
        side="SELL",
        triggerPrice="0.0835",
        quantity="203.0",
        clientAlgoId="exchange_generated_algo_id",
        algoId=3000002079961850,
    )
    false_red["exchange_sl_id"] = 3000002079961850
    false_red["exchange_position_owner_confirmed"] = True
    execution._confirm_exchange_sl_from_order(false_red, order, source="audit", require_bot_id=True)
    check("health changes to SL_SYNC_OK only with proof", health.open_trade_has_unresolved_sl_sync_risk(false_red, []) is False, false_red)


def main():
    test_initial_placement_confirmed()
    test_initial_placement_no_trigger_fail_safe()
    test_rebound_valid_and_invalid()
    test_audit_repair_success_confirmed()
    test_periodic_audit_refresh()
    test_missing_stop_remains_failure()
    test_trailing_sync_unchanged()
    test_duplicate_verification_idempotent()
    test_existing_valid_state_and_health_transition()
    print("PASS sim_live_sl_confirmation_state_wiring")


if __name__ == "__main__":
    main()
