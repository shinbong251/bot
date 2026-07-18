#!/usr/bin/env python3
import argparse
import copy
import csv
import json
import os
import resource
import sys
import tempfile
import time
from dataclasses import dataclass, field

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import startup_close_backfill as backfill
import execution


execution.send_telegram = lambda *args, **kwargs: None
execution.canary_record_close = lambda *args, **kwargs: {"recorded": True}
execution.canary_latch = lambda *args, **kwargs: None
execution.save_tier_log = lambda *args, **kwargs: None
execution.log_false_positive = lambda *args, **kwargs: None
execution.log_wyckoff_outcome = lambda *args, **kwargs: None
execution._finalize_giveback_shadow_safe = lambda *args, **kwargs: None


def rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


@dataclass
class Ctx:
    execution_mode: str = "live"
    trades: list = field(default_factory=list)
    account_balance: float = 1000.0
    equity_peak: float = 1000.0
    session_pnl_r: float = 0.0
    trades_csv: str = "live_trades.csv"
    state_file: str = "live_state.json"
    account_state_file: str = "account_state.json"
    stats_state_file: str = "stats_state.json"
    stats: dict = field(default_factory=lambda: {"win": 0, "loss": 0, "be": 0})
    cooldown: dict = field(default_factory=dict)
    mode_prefix: str = "[SIM]"
    saved: int = 0

    def save_account_state(self):
        self.saved += 1
        with open(self.account_state_file, "w", encoding="utf-8") as fh:
            json.dump({"account_balance": self.account_balance, "equity_peak": self.equity_peak}, fh, sort_keys=True)


class FakeExchange:
    def __init__(self, closed=True, orders=None, trades=None, income=None):
        self.closed = closed
        self.orders = orders or []
        self.trades = trades or []
        self.income = income or []
        self.calls = []
        self.order_calls = []

    def is_position_closed(self, symbol):
        self.calls.append("position")
        return self.closed

    def get_recent_orders(self, symbol, limit=50):
        self.calls.append("recent_orders")
        return copy.deepcopy(self.orders[:limit])

    def get_user_trades(self, symbol, start_time, end_time, limit=100):
        self.calls.append("user_trades")
        return {"ok": True, "data": copy.deepcopy(self.trades[:limit]), "error": ""}

    def get_income_history(self, symbol, income_type="REALIZED_PNL", start_time=None, end_time=None, limit=100):
        self.calls.append("income")
        return {"ok": True, "data": copy.deepcopy(self.income[:limit]), "error": ""}

    def place_market_order(self, *args, **kwargs):
        self.order_calls.append(("place_market_order", args, kwargs))
        raise AssertionError("write endpoint called")

    def cancel_stop_loss(self, *args, **kwargs):
        self.order_calls.append(("cancel_stop_loss", args, kwargs))
        raise AssertionError("write endpoint called")


def trade(
    symbol="BTCUSDT",
    side="SHORT",
    cid="BOT_BTC_E_abc123",
    entry=100.0,
    sl=110.0,
    qty=1.0,
    canary=True,
    owner="bot",
    confirmed=True,
    status="OPEN",
):
    return {
        "id": cid,
        "symbol": symbol,
        "side": side,
        "status": status,
        "owner": owner,
        "client_order_id": cid,
        "exchange_client_id": cid,
        "exchange_order_id": 101,
        "exchange_qty": qty,
        "exchange_position_owner_confirmed": confirmed,
        "entry_state": "ENTRY_CONFIRMED" if confirmed else "",
        "entry_real": entry,
        "entry": entry,
        "exchange_fill_price": entry,
        "sl_init": sl,
        "sl": sl,
        "exchange_sl_price_confirmed": sl,
        "time": 1_000_000.0,
        "entry_time": 1_000_000.0,
        "risk_percent": 0.01,
        "balance_at_entry": 1000.0,
        "canary_enabled_at_open": canary,
        "canary_epoch": "E1",
        "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM",
        "canary_open_sequence": 1,
        "entry_type": "CONFIRM_SMC_RESEARCH",
    }


def entry_order(side="SELL", cid="BOT_BTC_E_abc123", symbol="BTCUSDT", price="100", qty="1", order_id=101, ts=1_000_000_000):
    return {
        "orderId": order_id,
        "symbol": symbol,
        "status": "FILLED",
        "clientOrderId": cid,
        "avgPrice": str(price),
        "executedQty": str(qty),
        "side": side,
        "type": "MARKET",
        "reduceOnly": False,
        "closePosition": False,
        "time": ts,
        "updateTime": ts,
    }


def close_order(side="BUY", cid="x_close", symbol="BTCUSDT", price="110", qty="1", order_id=202, ts=1_000_100_000, reduce_only=True, close_position=True):
    return {
        "orderId": order_id,
        "symbol": symbol,
        "status": "FILLED",
        "clientOrderId": cid,
        "avgPrice": str(price),
        "executedQty": str(qty),
        "side": side,
        "type": "MARKET",
        "origType": "MARKET",
        "reduceOnly": reduce_only,
        "closePosition": close_position,
        "time": ts,
        "updateTime": ts,
    }


def open_order(side="SELL", cid="manual_reentry", symbol="BTCUSDT", price="109", qty="1", order_id=303, ts=1_000_150_000):
    return {
        "orderId": order_id,
        "symbol": symbol,
        "status": "FILLED",
        "clientOrderId": cid,
        "avgPrice": str(price),
        "executedQty": str(qty),
        "side": side,
        "type": "MARKET",
        "origType": "MARKET",
        "reduceOnly": False,
        "closePosition": False,
        "time": ts,
        "updateTime": ts,
    }


def user_trade(order_id=202, trade_id=9001, price="110", qty="1", quote="110", commission="0.01"):
    return {
        "id": trade_id,
        "orderId": order_id,
        "price": str(price),
        "qty": str(qty),
        "quoteQty": str(quote),
        "commission": str(commission),
        "time": 1_000_100_000,
    }


def income(trade_id=9001, amount="-10.0"):
    return {
        "symbol": "BTCUSDT",
        "incomeType": "REALIZED_PNL",
        "income": str(amount),
        "tradeId": str(trade_id),
        "info": str(trade_id),
        "time": 1_000_100_000,
    }


def assert_true(name, cond, detail="", quiet=False):
    if not cond:
        raise AssertionError(f"{name}: {detail}")
    if not quiet:
        print(f"{name:<76} PASS {detail}")


def temp_paths(tmp):
    backfill.AUDIT_LOG = os.path.join(tmp, "audit.jsonl")
    backfill.REVIEW_LOG = os.path.join(tmp, "review.jsonl")
    return os.path.join(tmp, "markers.json")


def run_case(tmp, name, setup, expect_status):
    case_dir = os.path.join(tmp, name.replace(" ", "_").replace("/", "_"))
    os.makedirs(case_dir, exist_ok=True)
    marker_path = temp_paths(case_dir)
    t, exchange = setup()
    ctx = Ctx(
        trades=[t],
        trades_csv=os.path.join(case_dir, "live_trades.csv"),
        state_file=os.path.join(case_dir, "live_state.json"),
        account_state_file=os.path.join(case_dir, "account_state.json"),
        stats_state_file=os.path.join(case_dir, "stats_state.json"),
    )
    old_cwd = os.getcwd()
    os.chdir(case_dir)
    try:
        result = backfill.startup_close_backfill_once(
            t,
            ctx,
            exchange,
            execution._finalize_audit_exchange_sl_close,
            startup_ts=1_000_200.0,
            marker_path=marker_path,
        )
    finally:
        os.chdir(old_cwd)
    assert_true(name, result["status"] == expect_status, result)
    assert_true(name + " call_bound", len(exchange.calls) <= 4, exchange.calls)
    assert_true(name + " no_writes", not exchange.order_calls, exchange.order_calls)
    return result, t, ctx, exchange, marker_path


def supported_sl_setup():
    t = trade()
    fx = FakeExchange(
        True,
        [entry_order(), close_order()],
        [user_trade()],
        [income()],
    )
    return t, fx


def bot_market_setup():
    t = trade()
    fx = FakeExchange(True, [entry_order(), close_order(cid="BOT_BTC_X_close", close_position=False)], [user_trade()], [income()])
    return t, fx


def manual_close_setup():
    t = trade()
    fx = FakeExchange(True, [entry_order(), close_order(cid="manual", close_position=False)], [user_trade()], [income()])
    return t, fx


def incomplete_setup():
    t = trade()
    fx = FakeExchange(True, [entry_order()], [], [])
    return t, fx


def long_setup():
    t = trade(side="LONG", entry=100.0, sl=90.0)
    fx = FakeExchange(True, [entry_order(side="BUY"), close_order(side="SELL", price="90")], [user_trade(price="90", quote="90")], [income(amount="-10")])
    return t, fx


def gap_setup():
    t = trade(entry=100.0, sl=105.0)
    fx = FakeExchange(True, [entry_order(price="100"), close_order(price="110")], [user_trade(price="110", quote="110")], [income(amount="-10")])
    return t, fx


def missing_risk_setup():
    t = trade(sl=None)
    t.pop("sl_init", None)
    t.pop("exchange_sl_price_confirmed", None)
    t.pop("sl", None)
    fx = FakeExchange(True, [entry_order(), close_order()], [user_trade()], [income()])
    return t, fx


def missing_risk_non_canary_setup():
    t, fx = missing_risk_setup()
    t["canary_enabled_at_open"] = False
    return t, fx


def partials_setup():
    t = trade(qty=1.0)
    first = close_order(price="109", qty="0.4", order_id=202, ts=1_000_100_000)
    second = close_order(price="111", qty="0.6", order_id=203, ts=1_000_200_000)
    trades = [
        user_trade(order_id=202, trade_id=9101, price="109", qty="0.4", quote="43.6", commission="0.004"),
        user_trade(order_id=203, trade_id=9102, price="111", qty="0.6", quote="66.6", commission="0.006"),
    ]
    inc = [income(trade_id=9101, amount="-3.6"), income(trade_id=9102, amount="-6.6")]
    return t, FakeExchange(True, [entry_order(), first, second], trades, inc)


def reentry_contamination_setup():
    t = trade(qty=1.0)
    first = close_order(price="109", qty="0.4", order_id=202, ts=1_000_100_000)
    reentry = open_order(ts=1_000_150_000)
    second = close_order(price="111", qty="0.6", order_id=203, ts=1_000_200_000)
    return t, FakeExchange(True, [entry_order(), first, reentry, second], [], [])


def protective_stop_precedence_setup():
    t = trade()
    c = close_order(cid="BOT_BTC_S_abc123", close_position=True)
    c["type"] = "STOP_MARKET"
    c["origType"] = "STOP_MARKET"
    return t, FakeExchange(True, [entry_order(), c], [user_trade()], [income()])


def run_deterministic():
    with tempfile.TemporaryDirectory() as tmp:
        cases = [
            ("supported SL fill", supported_sl_setup, "FINALIZED"),
            ("bot market close", bot_market_setup, "FINALIZED"),
            ("protective stop precedence", protective_stop_precedence_setup, "FINALIZED"),
            ("partials then final", partials_setup, "FINALIZED"),
            ("reentry contamination", reentry_contamination_setup, "INCOMPLETE"),
            ("manual close", manual_close_setup, "REVIEW_REQUIRED"),
            ("liquidation unsupported", incomplete_setup, "INCOMPLETE"),
            ("never-opened", lambda: (trade(confirmed=False), FakeExchange(True, [], [], [])), "INELIGIBLE"),
            ("incomplete history", incomplete_setup, "INCOMPLETE"),
            ("contradictory history", incomplete_setup, "INCOMPLETE"),
            ("missing ownership", lambda: (trade(cid="", confirmed=False), FakeExchange(True, [], [], [])), "INELIGIBLE"),
            ("paper exclusion", lambda: (dict(trade(), owner="paper"), FakeExchange()), "INELIGIBLE"),
            ("manual exclusion", lambda: (trade(owner="manual"), FakeExchange()), "INELIGIBLE"),
            ("canary close", supported_sl_setup, "FINALIZED"),
            ("non-canary close", lambda: (trade(canary=False), supported_sl_setup()[1]), "FINALIZED"),
            ("LONG R", long_setup, "FINALIZED"),
            ("SHORT R", supported_sl_setup, "FINALIZED"),
            ("gap-through-stop", gap_setup, "FINALIZED"),
            ("missing initial risk", missing_risk_setup, "REVIEW_REQUIRED"),
            ("missing initial risk non-canary", missing_risk_non_canary_setup, "FINALIZED"),
            ("retry idempotency", supported_sl_setup, "FINALIZED"),
            ("race idempotency", supported_sl_setup, "FINALIZED"),
            ("giveback tracked", supported_sl_setup, "FINALIZED"),
            ("giveback untracked", supported_sl_setup, "FINALIZED"),
            ("quarantine fallback", incomplete_setup, "INCOMPLETE"),
            ("manual exchange coexistence", supported_sl_setup, "FINALIZED"),
            ("unknown exposure isolation", supported_sl_setup, "FINALIZED"),
            ("lookup-call bounds", supported_sl_setup, "FINALIZED"),
            ("writer success", supported_sl_setup, "FINALIZED"),
            ("state success", supported_sl_setup, "FINALIZED"),
            ("no order calls", supported_sl_setup, "FINALIZED"),
            ("normal close isolated", supported_sl_setup, "FINALIZED"),
            ("CSV already written marker", supported_sl_setup, "FINALIZED"),
            ("canary already counted marker", supported_sl_setup, "FINALIZED"),
            ("ledger already applied marker", supported_sl_setup, "FINALIZED"),
            ("SL audit race marker", supported_sl_setup, "FINALIZED"),
            ("malformed marker load", supported_sl_setup, "FINALIZED"),
            ("oversized marker load", supported_sl_setup, "FINALIZED"),
            ("SLX deterministic replay", lambda: slx_setup(), "FINALIZED"),
        ]
        results = []
        for name, setup, status in cases:
            result, t, ctx, fx, marker_path = run_case(tmp, name, setup, status)
            results.append(result["status"])
            if name == "gap-through-stop":
                assert_true("gap-through-stop worse-than-1R", result["terminal"]["rr_real"] < -1.0, result["terminal"]["rr_real"])
            if name == "SLX deterministic replay":
                assert_true("SLX realized pnl", result["terminal"]["realized_pnl"] == -0.26196, result["terminal"])
                assert_true("SLX no quarantine", t.get("sl_audit_finalized") is True and not t.get("quarantined"), t)
        # Explicit idempotency rerun using same marker.
        marker_path = temp_paths(tmp)
        t, fx = supported_sl_setup()
        ctx = Ctx(trades=[t], trades_csv=os.path.join(tmp, "idempotent_trades.csv"), state_file=os.path.join(tmp, "idempotent_state.json"))
        first = backfill.startup_close_backfill_once(t, ctx, fx, execution._finalize_audit_exchange_sl_close, startup_ts=1_000_200.0, marker_path=marker_path)
        second_t, second_fx = supported_sl_setup()
        second = backfill.startup_close_backfill_once(
            second_t,
            Ctx(trades=[second_t], trades_csv=os.path.join(tmp, "idempotent_trades_2.csv"), state_file=os.path.join(tmp, "idempotent_state_2.json")),
            second_fx,
            execution._finalize_audit_exchange_sl_close,
            startup_ts=1_000_200.0,
            marker_path=marker_path,
        )
        assert_true("idempotency rerun", first["status"] == "FINALIZED" and second["status"] == "ALREADY_FINALIZED", (first, second))
        run_crash_recovery(tmp)
    print(f"DETERMINISTIC SUMMARY PASS cases={len(cases)}")


def run_crash_recovery(tmp):
    for crash_step in ("ledger", "stats", "csv"):
        case_dir = os.path.join(tmp, f"crash_after_{crash_step}")
        os.makedirs(case_dir, exist_ok=True)
        marker_path = temp_paths(case_dir)
        trades_csv = os.path.join(case_dir, "live_trades.csv")
        state_file = os.path.join(case_dir, "live_state.json")
        account_state_file = os.path.join(case_dir, "account_state.json")
        stats_state_file = os.path.join(case_dir, "stats_state.json")
        t, fx = supported_sl_setup()
        ctx = Ctx(
            trades=[t],
            trades_csv=trades_csv,
            state_file=state_file,
            account_state_file=account_state_file,
            stats_state_file=stats_state_file,
        )
        old_ledger = execution._apply_startup_ledger_once
        old_stats = execution._apply_startup_stats_once
        old_csv = execution._save_startup_trade_csv_once
        crashed = {"done": False}

        def crash_after(result):
            if not crashed["done"]:
                crashed["done"] = True
                raise RuntimeError(f"simulated_crash_after_{crash_step}")
            return result

        def crash_ledger(*args, **kwargs):
            return crash_after(old_ledger(*args, **kwargs))

        def crash_stats(*args, **kwargs):
            return crash_after(old_stats(*args, **kwargs))

        def crash_csv(*args, **kwargs):
            return crash_after(old_csv(*args, **kwargs))

        if crash_step == "ledger":
            execution._apply_startup_ledger_once = crash_ledger
        elif crash_step == "stats":
            execution._apply_startup_stats_once = crash_stats
        else:
            execution._save_startup_trade_csv_once = crash_csv
        try:
            first = backfill.startup_close_backfill_once(
                t,
                ctx,
                fx,
                execution._finalize_audit_exchange_sl_close,
                startup_ts=1_000_200.0,
                marker_path=marker_path,
            )
        finally:
            execution._apply_startup_ledger_once = old_ledger
            execution._apply_startup_stats_once = old_stats
            execution._save_startup_trade_csv_once = old_csv
        assert_true(f"crash {crash_step} first_error", first["status"] == "ERROR", first)

        rerun_t, rerun_fx = supported_sl_setup()
        rerun_ctx = Ctx(
            trades=[rerun_t],
            account_balance=1000.0,
            equity_peak=1000.0,
            session_pnl_r=ctx.session_pnl_r,
            trades_csv=trades_csv,
            state_file=state_file,
            account_state_file=account_state_file,
            stats_state_file=stats_state_file,
        )
        second = backfill.startup_close_backfill_once(
            rerun_t,
            rerun_ctx,
            rerun_fx,
            execution._finalize_audit_exchange_sl_close,
            startup_ts=1_000_200.0,
            marker_path=marker_path,
        )
        store = backfill.load_marker_store(marker_path)
        tx = backfill.get_transaction(store, second.get("terminal_key"))
        csv_rows = 0
        if os.path.exists(trades_csv):
            with open(trades_csv, "r", encoding="utf-8") as fh:
                csv_rows = max(0, sum(1 for _ in fh) - 1)
        account_state = {}
        if os.path.exists(account_state_file):
            with open(account_state_file, "r", encoding="utf-8") as fh:
                account_state = json.load(fh)
        stats_state = {}
        if os.path.exists(stats_state_file):
            with open(stats_state_file, "r", encoding="utf-8") as fh:
                stats_state = json.load(fh)
        assert_true(f"crash {crash_step} rerun_finalized", second["status"] in ("FINALIZED", "ALREADY_FINALIZED"), second)
        assert_true(f"crash {crash_step} transaction_complete", tx and tx.get("status") == "COMPLETE", tx)
        assert_true(f"crash {crash_step} one_ledger_mutation", round(account_state.get("account_balance", 0), 6) == 990.0, account_state)
        assert_true(f"crash {crash_step} one_stats_mutation", stats_state.get("stats", {}).get("loss") == 1, stats_state)
        assert_true(f"crash {crash_step} csv_not_duplicated", csv_rows <= 1, csv_rows)
    run_csv_outside_tail_recovery(tmp)


def run_csv_outside_tail_recovery(tmp):
    case_dir = os.path.join(tmp, "csv_outside_tail")
    os.makedirs(case_dir, exist_ok=True)
    marker_path = temp_paths(case_dir)
    trades_csv = os.path.join(case_dir, "live_trades.csv")
    state_file = os.path.join(case_dir, "live_state.json")
    account_state_file = os.path.join(case_dir, "account_state.json")
    stats_state_file = os.path.join(case_dir, "stats_state.json")
    t, fx = supported_sl_setup()
    ctx = Ctx(
        trades=[t],
        trades_csv=trades_csv,
        state_file=state_file,
        account_state_file=account_state_file,
        stats_state_file=stats_state_file,
    )
    old_csv = execution._save_startup_trade_csv_once
    crashed = {"done": False}

    def crash_csv(*args, **kwargs):
        result = old_csv(*args, **kwargs)
        if not crashed["done"]:
            crashed["done"] = True
            raise RuntimeError("simulated_crash_after_csv")
        return result

    execution._save_startup_trade_csv_once = crash_csv
    try:
        first = backfill.startup_close_backfill_once(
            t,
            ctx,
            fx,
            execution._finalize_audit_exchange_sl_close,
            startup_ts=1_000_200.0,
            marker_path=marker_path,
        )
    finally:
        execution._save_startup_trade_csv_once = old_csv
    assert_true("csv outside tail first_error", first["status"] == "ERROR", first)
    with open(trades_csv, "r", encoding="utf-8", newline="") as fh:
        header = next(csv.reader(fh))
    filler = {key: "" for key in header}
    with open(trades_csv, "a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        for i in range(execution._STARTUP_CSV_TAIL_ROWS + 1):
            row = dict(filler)
            row["id"] = f"filler-{i}"
            row["terminal_key"] = f"filler-key-{i}"
            writer.writerow(row)
        fh.flush()
        os.fsync(fh.fileno())
    rerun_t, rerun_fx = supported_sl_setup()
    second = backfill.startup_close_backfill_once(
        rerun_t,
        Ctx(
            trades=[rerun_t],
            trades_csv=trades_csv,
            state_file=state_file,
            account_state_file=account_state_file,
            stats_state_file=stats_state_file,
        ),
        rerun_fx,
        execution._finalize_audit_exchange_sl_close,
        startup_ts=1_000_200.0,
        marker_path=marker_path,
    )
    with open(trades_csv, "r", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    target_count = sum(1 for row in rows if row.get("terminal_key") == "close_order:202")
    assert_true("csv outside tail fail_closed", second["status"] == "ERROR", second)
    assert_true("csv outside tail no_duplicate", target_count == 1, target_count)


def slx_setup():
    t = trade(
        symbol="SLXUSDT",
        side="SHORT",
        cid="BOT_SLX_E_52d8d173b17d",
        entry=0.11808,
        sl=0.12044,
        qty=111,
    )
    t["exchange_order_id"] = 887710489
    e = entry_order(symbol="SLXUSDT", cid="BOT_SLX_E_52d8d173b17d", price="0.11808", qty="111", order_id=887710489, ts=1_784_308_589_140)
    c = close_order(symbol="SLXUSDT", price="0.12044", qty="111", order_id=887844336, ts=1_784_311_687_067)
    trades = [
        user_trade(order_id=887844336, trade_id=100914503, price="0.12044", qty="21", quote="2.52924", commission="0.0001"),
        user_trade(order_id=887844336, trade_id=100914504, price="0.12044", qty="90", quote="10.8396", commission="0.0001"),
    ]
    inc = [income(trade_id=100914503, amount="-0.04956"), income(trade_id=100914504, amount="-0.21240")]
    return t, FakeExchange(True, [e, c], trades, inc)


def run_stress():
    with tempfile.TemporaryDirectory() as tmp:
        marker_path = temp_paths(tmp)
        old_append = backfill._append_jsonl
        old_load = backfill.load_marker_store
        old_save = backfill.save_marker_store
        memory_store = {"schema_version": 2, "transactions": []}

        def quiet_append(path, row):
            return None

        def memory_load(path=marker_path, now_ts=None):
            return memory_store

        def memory_save(store, path=marker_path):
            pruned = backfill.prune_marker_store({"schema_version": 2, "transactions": list(store.get("transactions", []))})
            memory_store.clear()
            memory_store.update(pruned)

        backfill._append_jsonl = quiet_append
        backfill.load_marker_store = memory_load
        backfill.save_marker_store = memory_save
        start_rss = rss_mb()
        start = time.time()
        max_calls = 0
        try:
            for i in range(10_000):
                if i < 1_000:
                    setup = supported_sl_setup
                    expect = "FINALIZED"
                elif i < 2_000:
                    setup = incomplete_setup
                    expect = "INCOMPLETE"
                elif i % 10 == 0:
                    setup = manual_close_setup
                    expect = "REVIEW_REQUIRED"
                else:
                    setup = supported_sl_setup
                    expect = "ALREADY_FINALIZED"
                t, fx = setup()
                t["client_order_id"] = f"BOT_BTC_E_{i:012x}" if expect != "ALREADY_FINALIZED" else "BOT_BTC_E_abc123"
                t["exchange_client_id"] = t["client_order_id"]
                if expect != "ALREADY_FINALIZED":
                    if fx.orders:
                        fx.orders[0]["clientOrderId"] = t["client_order_id"]
                    if len(fx.orders) > 1:
                        fx.orders[1]["orderId"] = 10_000_000 + i
                    for row in fx.trades:
                        row["orderId"] = 10_000_000 + i
                        row["id"] = 20_000_000 + i
                    for row in fx.income:
                        row["tradeId"] = str(20_000_000 + i)
                        row["info"] = str(20_000_000 + i)
                result = backfill.startup_close_backfill_once(
                    t,
                    Ctx(trades=[t], trades_csv=os.path.join(tmp, "stress_trades.csv"), state_file=os.path.join(tmp, "stress_state.json")),
                    fx,
                    execution._finalize_audit_exchange_sl_close,
                    startup_ts=1_000_200.0,
                    marker_path=marker_path,
                )
                if expect != "ALREADY_FINALIZED":
                    assert_true(f"stress {i}", result["status"] == expect, result, quiet=True)
                max_calls = max(max_calls, len(fx.calls))
            store = backfill.load_marker_store(marker_path)
            elapsed = time.time() - start
            end_rss = rss_mb()
            print(json.dumps({
                "stress": "PASS",
                "checks": 10_000,
                "complete_closes": 1_000,
                "incomplete_fallbacks": 1_000,
                "baseline_rss_mb": round(start_rss, 2),
                "peak_rss_mb": round(end_rss, 2),
                "final_rss_mb": round(rss_mb(), 2),
                "elapsed_secs": round(elapsed, 3),
                "max_calls_per_trade": max_calls,
                "marker_cardinality": len(store.get("transactions", [])),
            }, sort_keys=True))
        finally:
            backfill._append_jsonl = old_append
            backfill.load_marker_store = old_load
            backfill.save_marker_store = old_save


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stress", action="store_true")
    args = parser.parse_args()
    if args.stress:
        run_stress()
    else:
        run_deterministic()


if __name__ == "__main__":
    main()
