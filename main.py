import sys
import time
import traceback
import threading
from execution_mode import EXECUTION_MODE, validate_startup
from execution import refresh_open_trade_market_data_timer, update_trades
from state_manager import init_csv, write_runtime_error
from telegram import send_telegram
from heartbeat import build_paper_heartbeat, build_testnet_heartbeat, build_live_heartbeat, PAPER_HB_INTERVAL, TESTNET_HB_INTERVAL, LIVE_HB_INTERVAL
from log_rotation import rotate_logs, maybe_rotate_logs
from evaluate_counterfactuals import maybe_evaluate_counterfactuals

RUNNING = True
try:
    sys.stdout.reconfigure(encoding="utf-8")
except:
    pass

SCAN_INTERVAL  = 60
TRADE_INTERVAL = 2

_ALERT_FAIL_THRESHOLD = 3
_BOT_START_TIME       = time.time()


def _paper_qualified_dispatcher_enabled():
    if EXECUTION_MODE != "paper":
        return False
    try:
        from config import config
        return bool(config.get("paper_smc_research_qualified_enabled", False))
    except Exception:
        return False


# =====================================================================
# EXECUTOR SETUP
# =====================================================================

def _build_executors():
    """
    Build executor contexts based on execution_mode in config.json.

    Returns list of ExecutorContext objects.
    In 'paper' or 'testnet' mode: single executor (backward compat).
    In 'both' mode: paper executor + testnet executor.
    In 'paper_live' mode: paper executor + live executor.
    """
    import json
    from executor_context import ExecutorContext
    from config import config

    executors = []

    if EXECUTION_MODE in ("paper", "both", "paper_live"):
        paper_ctx = ExecutorContext(
            name="paper",
            account_balance=config.get("account_balance", 1000.0),
            trades_csv="paper_trades.csv",
            state_file="paper_state.json",
            mode_prefix="[PAPER]",
            execution_mode="paper",
            pause_until=config.get("pause_until", 0),
            equity_peak=config.get("equity_peak", config.get("account_balance", 1000.0)),
        )
        paper_ctx.load_trades()
        executors.append(paper_ctx)

    if EXECUTION_MODE in ("testnet", "both"):
        tn_balance = config.get("execution_balance", 50.0)
        testnet_ctx = ExecutorContext(
            name="testnet",
            account_balance=tn_balance,
            trades_csv="testnet_trades.csv",
            state_file="testnet_state.json",
            mode_prefix="[TESTNET]",
            execution_mode="testnet",
            pause_until=0,
            equity_peak=tn_balance,
        )
        testnet_ctx.load_trades()
        testnet_ctx.load_account_state()
        executors.append(testnet_ctx)

    if EXECUTION_MODE in ("live", "paper_live"):
        live_balance = config.get("execution_balance", 50.0)
        live_ctx = ExecutorContext(
            name="live",
            account_balance=live_balance,
            trades_csv="live_trades.csv",
            state_file="live_state.json",
            mode_prefix="[LIVE]",
            execution_mode="live",
            pause_until=0,
            equity_peak=live_balance,
        )
        live_ctx.load_trades()
        live_ctx.load_account_state()
        executors.append(live_ctx)

    return executors


# =====================================================================
# LOOPS
# =====================================================================

_LIVE_SL_AUDIT_INTERVAL = 180  # 3 minutes (FIX 4 — reduce mismatch detection delay)

def scan_loop(executors):
    global RUNNING
    _fail_count       = 0
    _last_heartbeat   = {ctx.name: time.time() for ctx in executors}
    _last_sl_audit    = {ctx.name: 0 for ctx in executors}

    from signal_dispatcher import dispatch_to_executor
    from execution import check_quarantine_ttl

    if EXECUTION_MODE in ("paper", "testnet") and not _paper_qualified_dispatcher_enabled():
        # Single-executor backward-compat: use original run()
        from execution import run as _run
        _use_shared_scan = False
    else:
        from execution import scan_phase as _scan_phase
        _use_shared_scan = True

    while RUNNING:
        start = time.time()
        try:
            print("\n===== SCAN CYCLE =====")

            if _use_shared_scan:
                # Shared scan — runs ONCE, dispatches to all executors
                _, _, all_signals = _scan_phase(executor_contexts=executors)
                for ctx in executors:
                    dispatch_to_executor(all_signals, ctx)
            else:
                # Single-executor mode — original run() path
                _run()

            _fail_count = 0
        except Exception as e:
            _fail_count += 1
            _tb = traceback.format_exc()
            print("💥 SCAN ERROR:", e)
            print(_tb)
            write_runtime_error(f"SCAN/{EXECUTION_MODE}", _tb)
            if _fail_count >= _ALERT_FAIL_THRESHOLD:
                try:
                    send_telegram(
                        f"💀 SCAN LOOP ERROR x{_fail_count}: {type(e).__name__}: {e}\nSee runtime_errors.log",
                        channel="alerts"
                    )
                except Exception:
                    pass
                _fail_count = 0
            time.sleep(5)

        now = time.time()

        # Quarantine TTL check — runs every scan cycle
        for ctx in executors:
            try:
                check_quarantine_ttl(ctx)
            except Exception:
                pass

        # Periodic live SL audit — every 10 minutes for live executors
        from execution import audit_exchange_sl as _audit_sl
        for ctx in executors:
            if ctx.execution_mode == "live":
                if now - _last_sl_audit.get(ctx.name, 0) >= _LIVE_SL_AUDIT_INTERVAL:
                    try:
                        _audit_sl(ctx)
                    except Exception:
                        pass
                    _last_sl_audit[ctx.name] = now

        # Log rotation — runs every 6 hours (throttled inside maybe_rotate_logs)
        try:
            maybe_rotate_logs()
        except Exception:
            pass

        # Counterfactual evaluator — runs every 6 hours (throttled inside)
        try:
            maybe_evaluate_counterfactuals()
        except Exception:
            pass

        for ctx in executors:
            if ctx.execution_mode == "paper":
                interval = PAPER_HB_INTERVAL
            elif ctx.execution_mode == "live":
                interval = LIVE_HB_INTERVAL
            else:
                interval = TESTNET_HB_INTERVAL
            if now - _last_heartbeat.get(ctx.name, 0) >= interval:
                try:
                    if ctx.execution_mode == "paper":
                        msg = build_paper_heartbeat(ctx, _BOT_START_TIME)
                    elif ctx.execution_mode == "live":
                        msg = build_live_heartbeat(ctx, _BOT_START_TIME)
                    else:
                        msg = build_testnet_heartbeat(ctx, _BOT_START_TIME)
                    send_telegram(msg, prefix=ctx.mode_prefix)
                except Exception:
                    pass
                _last_heartbeat[ctx.name] = now

        elapsed = time.time() - start
        sleep_time = max(0, SCAN_INTERVAL - elapsed)
        print(f"⏱ scan: {round(elapsed,2)}s | sleep: {round(sleep_time,2)}s")
        time.sleep(sleep_time)


def trade_loop(executors):
    global RUNNING
    _fail_count = 0
    _last_alert_time = 0
    _MIN_ALERT_INTERVAL = 60

    if EXECUTION_MODE in ("paper", "testnet") and not _paper_qualified_dispatcher_enabled():
        # Single-executor backward-compat: use original update_trades()
        _use_ctx = False
    else:
        _use_ctx = True

    while RUNNING:
        start = time.time()
        try:
            refresh_open_trade_market_data_timer(executors if _use_ctx else None)
            if _use_ctx:
                for ctx in executors:
                    update_trades(fast_mode=True, ctx=ctx)
            else:
                update_trades(fast_mode=True)
            _fail_count = 0
        except Exception as e:
            _fail_count += 1
            _tb = traceback.format_exc()
            print("💥 TRADE ERROR:", e)
            print(_tb)
            write_runtime_error(f"TRADE/{EXECUTION_MODE}", _tb)
            if _fail_count >= _ALERT_FAIL_THRESHOLD:
                _now = time.time()
                if _now - _last_alert_time >= _MIN_ALERT_INTERVAL:
                    try:
                        send_telegram(
                            f"💀 TRADE LOOP ERROR x{_fail_count}: {type(e).__name__}: {e}\nSee runtime_errors.log",
                            channel="alerts"
                        )
                    except Exception:
                        pass
                    _last_alert_time = _now
                _fail_count = 0
            time.sleep(1)
        elapsed = time.time() - start
        sleep_time = max(0, TRADE_INTERVAL - elapsed)
        time.sleep(sleep_time)


# =====================================================================
# ENTRYPOINT
# =====================================================================

if __name__ == "__main__":
    # Single-instance lock: prevent a second bot process in this working
    # directory from co-owning paper_state.json (file race) or re-sending
    # duplicate Telegram alerts. Acquired before any trading work begins.
    from instance_lock import acquire as _acquire_instance_lock
    if not _acquire_instance_lock():
        print("🛑 Duplicate bot instance detected — exiting without starting trade/scan loops.")
        sys.exit(1)

    validate_startup()

    # Rotate oversized logs at startup
    try:
        rotate_logs()
    except Exception:
        pass

    executors = _build_executors()

    from execution import log_paper_dd_pause_status
    for ctx in executors:
        log_paper_dd_pause_status(ctx)

    # Init CSVs for all executors
    for ctx in executors:
        init_csv(ctx.trades_csv)

    # P3 + P4: Startup audit and reconciliation for testnet + live executors
    from execution import audit_exchange_sl, reconcile_exchange_positions
    for ctx in executors:
        if ctx.execution_mode in ("testnet", "live"):
            reconcile_exchange_positions(ctx)
            audit_exchange_sl(ctx)

    t_trade = threading.Thread(
        target=trade_loop, args=(executors,), daemon=True, name="TradeLoop"
    )
    t_scan  = threading.Thread(
        target=scan_loop,  args=(executors,), daemon=True, name="ScanLoop"
    )

    t_trade.start()
    t_scan.start()

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 Stopping bot...")
        RUNNING = False

        time.sleep(2)

        # Save state for all executors
        from state_manager import save_open_trades
        for ctx in executors:
            if ctx.trades:
                save_open_trades(ctx.trades, ctx.state_file)
                print(f"✅ State saved: {ctx.name} ({len(ctx.trades)} trades)")

        print("✅ Bot stopped")
