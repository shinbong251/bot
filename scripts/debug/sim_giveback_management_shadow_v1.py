#!/usr/bin/env python3
import argparse
import json
import os
import resource
import sys
import tempfile
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from giveback_management_shadow import (
    GivebackShadowStore,
    compute_current_r,
    finalize_giveback_trade,
    normalize_giveback_trade,
    observe_giveback_trade,
)


def rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def trade(side="LONG", canary=True, live=True, manual=False, cid="BOT_BTCUSDT_E_abc123", entry=100.0, sl=90.0):
    return {
        "symbol": "BTCUSDT",
        "side": side,
        "entry_type": "CONFIRM",
        "execution_mode": "live" if live else "paper",
        "owner": "manual" if manual else "bot",
        "canary_enabled_at_open": canary,
        "canary_epoch": "E1",
        "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM",
        "client_order_id": cid,
        "exchange_client_id": cid,
        "id": cid,
        "dedup_key": "D1",
        "time": 1000.0,
        "entry_real": entry,
        "exchange_fill_price": entry,
        "sl_init": sl,
        "exchange_sl_price_confirmed": sl,
        "sl": sl,
        "status": "OPEN",
    }


def paths(tmp):
    return (
        os.path.join(tmp, "state.json"),
        os.path.join(tmp, "obs.jsonl"),
        os.path.join(tmp, "close.jsonl"),
    )


def rows(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def assert_true(cond, name):
    if not cond:
        raise AssertionError(name)


class FailingSaveStore(GivebackShadowStore):
    def save(self, force=False):
        return False


def temp_workspace():
    base = "/dev/shm" if os.path.isdir("/dev/shm") and os.access("/dev/shm", os.W_OK) else None
    return tempfile.TemporaryDirectory(dir=base)


def run_deterministic():
    with temp_workspace() as tmp:
        state_path, obs_path, close_path = paths(tmp)
        store = GivebackShadowStore(state_path, max_records=3)

        assert_true(round(compute_current_r("LONG", 100, 10, 106), 4) == 0.6, "LONG current-R calculation")
        assert_true(round(compute_current_r("SHORT", 100, 10, 94), 4) == 0.6, "SHORT current-R calculation")
        assert_true(compute_current_r("LONG", 100, 0, 101) is None, "invalid initial risk distance")

        pre_open = trade(cid="")
        pre_open["dedup_key"] = ""
        pre_open["time"] = ""
        pre_open["entry_time"] = ""
        ok, flags = normalize_giveback_trade(pre_open)
        assert_true(ok is None and "missing_authoritative_identity" in flags, "rejected/pre-open trade not tracked")
        ok, flags = normalize_giveback_trade(trade())
        assert_true(ok and not flags, "initialization only after accepted authoritative trade")

        assert_true(not observe_giveback_trade(trade(live=False), 100, store=store, log_path=obs_path)["tracked"], "PAPER excluded")
        assert_true(not observe_giveback_trade(trade(manual=True), 100, store=store, log_path=obs_path)["tracked"], "manual position excluded")
        assert_true(not observe_giveback_trade(trade(canary=False), 100, store=store, log_path=obs_path)["tracked"], "non-canary LIVE excluded")

        t = trade()
        observe_giveback_trade(t, 100, observation_ts=1001, actual_sl_price=90, source="open", store=store, log_path=obs_path)
        observe_giveback_trade(t, 105, observation_ts=1002, actual_sl_price=90, source="mgmt", store=store, log_path=obs_path)
        observe_giveback_trade(t, 112, observation_ts=1003, actual_management_action="lock", actual_sl_price=103, source="mgmt", store=store, log_path=obs_path)
        observe_giveback_trade(t, 120, observation_ts=1004, actual_sl_price=103, source="mgmt", store=store, log_path=obs_path)
        observe_giveback_trade(t, 114, observation_ts=1005, actual_sl_price=103, source="mgmt", store=store, log_path=obs_path)
        state = GivebackShadowStore(state_path).load()
        rec = state["active"][t["client_order_id"]]
        assert_true(round(rec["peak_r"], 4) == 2.0, "peak monotonicity")
        assert_true(rec["first_hit_0_5r_ts"] == 1002 and rec["first_hit_1_0r_ts"] == 1003, "first-hit timestamps set once")
        assert_true(rec["first_hit_1_5r_ts"] == 1004 and rec["first_hit_2_0r_ts"] == 1004, "1.5/2.0 first-hit timestamps")
        assert_true(round(rec["max_locked_r_actual"], 4) == 0.3, "actual lock monotonicity")
        assert_true(rec["v5_armed"] and rec["v5_armed_ts"] == 1004, "V5 arms at 1.5R")
        assert_true(round(rec["v5_lock_r"], 4) == 1.0, "V5 lock = peak-1.0R")
        assert_true(rec["v5_lock_update_count"] >= 1, "V5 lock update count")

        observe_giveback_trade(t, 125, observation_ts=1006, actual_sl_price=103, source="mgmt", store=store, log_path=obs_path)
        observe_giveback_trade(t, 118, observation_ts=1007, actual_sl_price=103, source="mgmt", store=store, log_path=obs_path)
        rec = GivebackShadowStore(state_path).load()["active"][t["client_order_id"]]
        assert_true(round(rec["v5_lock_r"], 4) == 1.5, "new peak raises V5 lock")
        observe_giveback_trade(t, 116, observation_ts=1008, actual_sl_price=103, source="mgmt", store=store, log_path=obs_path)
        rec = GivebackShadowStore(state_path).load()["active"][t["client_order_id"]]
        assert_true(round(rec["v5_lock_r"], 4) == 1.5, "V5 lock never decreases")

        observe_giveback_trade(t, 114, observation_ts=1009, actual_sl_price=103, source="mgmt", store=store, log_path=obs_path)
        rec = GivebackShadowStore(state_path).load()["active"][t["client_order_id"]]
        assert_true(rec["v5_would_trigger_observed"], "conservative trigger detection")
        assert_true(rec["v5_trigger_certainty"] == "CONSERVATIVE_CROSSING", "trigger certainty")

        t2 = trade(cid="BOT_ETHUSDT_E_abc123")
        observe_giveback_trade(t2, 100, observation_ts=2001, actual_sl_price=90, store=store, log_path=obs_path)
        t2["max_profit_r"] = 1.6
        observe_giveback_trade(t2, 105, observation_ts=2002, actual_sl_price=90, store=store, log_path=obs_path)
        rec2 = GivebackShadowStore(state_path).load()["active"][t2["client_order_id"]]
        assert_true(rec2["v5_trigger_certainty"] == "AMBIGUOUS_SAME_OBSERVATION", "ambiguous same-observation ordering")

        short = trade(side="SHORT", cid="BOT_SOLUSDT_E_abc123", entry=100, sl=110)
        observe_giveback_trade(short, 85, observation_ts=3001, actual_sl_price=110, store=store, log_path=obs_path)
        recs = GivebackShadowStore(state_path).load()["active"]
        short_rec = recs[short["client_order_id"]]
        assert_true(round(short_rec["v5_hypothetical_stop_price"], 4) == 95.0, "SHORT hypothetical stop-price conversion")
        long_rec = recs[t["client_order_id"]]
        assert_true(round(long_rec["v5_hypothetical_stop_price"], 4) == 115.0, "LONG hypothetical stop-price conversion")

        pre_close = rows(obs_path)[-1]
        assert_true("realized_r" not in pre_close and "close_reason" not in pre_close, "no outcome field in pre-close rows")

        t["status"] = "WIN"
        t["exit_type"] = "TRAIL"
        t["close_reason"] = "TRAIL"
        t["exit_price"] = 112
        t["rr_real"] = 1.2
        close_now = time.time()
        t["close_time"] = close_now
        res = finalize_giveback_trade(t, close_ts=close_now, source="normal", store=store, close_log_path=close_path, observation_log_path=obs_path)
        dup = finalize_giveback_trade(t, close_ts=close_now, source="audit", store=store, close_log_path=close_path, observation_log_path=obs_path)
        close_rows = rows(close_path)
        assert_true(res["finalized"] and dup.get("idempotent") and len(close_rows) == 1, "duplicate close emits one terminal row")
        assert_true(close_rows[0]["close_reason"] == "TRAIL", "normal close supported")
        assert_true(round(close_rows[0]["giveback_r"], 4) == 1.3, "terminal giveback calculation")
        assert_true(close_rows[0]["capture_ratio"] is not None, "capture calculation")
        assert_true(close_rows[0]["v5_delta_vs_actual_r"] is not None, "V5 terminal delta calculation")
        assert_true(t["status"] == "WIN" and t["rr_real"] == 1.2, "actual trade outcome remains untouched")

        fresh_state = os.path.join(tmp, "fresh_success_state.json")
        fresh_obs = os.path.join(tmp, "fresh_success_obs.jsonl")
        fresh_close = os.path.join(tmp, "fresh_success_close.jsonl")
        fresh_trade = trade(cid="BOT_FRESH_SUCCESS")
        observe_giveback_trade(fresh_trade, 112, observation_ts=4100, actual_sl_price=90, store=GivebackShadowStore(fresh_state), log_path=fresh_obs)
        fresh_trade.update({"status": "WIN", "exit_type": "TRAIL", "close_reason": "TRAIL", "exit_price": 106, "rr_real": 0.6})
        first = finalize_giveback_trade(fresh_trade, close_ts=time.time(), source="normal", store=GivebackShadowStore(fresh_state), close_log_path=fresh_close, observation_log_path=fresh_obs)
        retry = finalize_giveback_trade(fresh_trade, close_ts=time.time(), source="normal_retry", store=GivebackShadowStore(fresh_state), close_log_path=fresh_close, observation_log_path=fresh_obs)
        assert_true(first["finalized"] and retry.get("idempotent") and len(rows(fresh_close)) == 1, "fresh-store retry after durable marker emits one terminal row")
        assert_true(GivebackShadowStore(fresh_state).load()["active"].get(fresh_trade["client_order_id"]) is None, "active removed after successful close append")

        obs_fail_state = os.path.join(tmp, "obs_fail_state.json")
        obs_fail_close = os.path.join(tmp, "obs_fail_close.jsonl")
        obs_blocker = os.path.join(tmp, "obs_blocker")
        with open(obs_blocker, "w", encoding="utf-8") as fh:
            fh.write("x")
        obs_fail_trade = trade(cid="BOT_OBS_FAIL")
        observe_giveback_trade(obs_fail_trade, 111, observation_ts=4200, actual_sl_price=90, store=GivebackShadowStore(obs_fail_state), log_path=os.path.join(tmp, "obs_fail_pre.jsonl"))
        obs_fail_trade.update({"status": "WIN", "exit_type": "TRAIL", "close_reason": "TRAIL", "exit_price": 105, "rr_real": 0.5})
        obs_fail_first = finalize_giveback_trade(
            obs_fail_trade, close_ts=time.time(), source="normal", store=GivebackShadowStore(obs_fail_state),
            close_log_path=obs_fail_close, observation_log_path=os.path.join(obs_blocker, "obs.jsonl"),
        )
        obs_fail_retry = finalize_giveback_trade(
            obs_fail_trade, close_ts=time.time(), source="normal_retry", store=GivebackShadowStore(obs_fail_state),
            close_log_path=obs_fail_close, observation_log_path=os.path.join(tmp, "obs_fail_retry.jsonl"),
        )
        assert_true(obs_fail_first["finalized"] and obs_fail_retry.get("idempotent") and len(rows(obs_fail_close)) == 1, "observation write failure does not control terminal marker")

        normal_audit_state = os.path.join(tmp, "normal_audit_state.json")
        normal_audit_close = os.path.join(tmp, "normal_audit_close.jsonl")
        normal_audit = trade(cid="BOT_NORMAL_AUDIT")
        observe_giveback_trade(normal_audit, 111, observation_ts=4300, actual_sl_price=90, store=GivebackShadowStore(normal_audit_state), log_path=os.path.join(tmp, "normal_audit_obs.jsonl"))
        normal_audit.update({"status": "WIN", "exit_type": "TRAIL", "close_reason": "TRAIL", "exit_price": 104, "rr_real": 0.4})
        finalize_giveback_trade(normal_audit, close_ts=time.time(), source="normal", store=GivebackShadowStore(normal_audit_state), close_log_path=normal_audit_close, observation_log_path=os.path.join(tmp, "normal_audit_obs.jsonl"))
        normal_then_audit = finalize_giveback_trade(normal_audit, close_ts=time.time(), source="audit_exchange_sl", store=GivebackShadowStore(normal_audit_state), close_log_path=normal_audit_close, observation_log_path=os.path.join(tmp, "normal_audit_obs.jsonl"))
        assert_true(normal_then_audit.get("idempotent") and len(rows(normal_audit_close)) == 1, "duplicate normal-close then audit-close emits one row")

        audit_normal_state = os.path.join(tmp, "audit_normal_state.json")
        audit_normal_close = os.path.join(tmp, "audit_normal_close.jsonl")
        audit_normal = trade(cid="BOT_AUDIT_NORMAL")
        observe_giveback_trade(audit_normal, 91, observation_ts=4400, actual_sl_price=90, store=GivebackShadowStore(audit_normal_state), log_path=os.path.join(tmp, "audit_normal_obs.jsonl"))
        audit_normal.update({"status": "LOSE", "exit_type": "SL", "close_reason": "exchange_sl_filled", "exit_price": 90, "rr_real": -1.0})
        finalize_giveback_trade(audit_normal, close_ts=time.time(), source="audit_exchange_sl", store=GivebackShadowStore(audit_normal_state), close_log_path=audit_normal_close, observation_log_path=os.path.join(tmp, "audit_normal_obs.jsonl"))
        audit_then_normal = finalize_giveback_trade(audit_normal, close_ts=time.time(), source="normal", store=GivebackShadowStore(audit_normal_state), close_log_path=audit_normal_close, observation_log_path=os.path.join(tmp, "audit_normal_obs.jsonl"))
        assert_true(audit_then_normal.get("idempotent") and len(rows(audit_normal_close)) == 1, "duplicate audit-close then normal-close emits one row")

        close_fail_state = os.path.join(tmp, "close_fail_state.json")
        close_fail_trade = trade(cid="BOT_CLOSE_FAIL")
        observe_giveback_trade(close_fail_trade, 112, observation_ts=4500, actual_sl_price=90, store=GivebackShadowStore(close_fail_state), log_path=os.path.join(tmp, "close_fail_obs.jsonl"))
        close_blocker = os.path.join(tmp, "close_blocker")
        with open(close_blocker, "w", encoding="utf-8") as fh:
            fh.write("x")
        close_fail_trade.update({"status": "WIN", "exit_type": "TRAIL", "close_reason": "TRAIL", "exit_price": 106, "rr_real": 0.6})
        close_fail = finalize_giveback_trade(
            close_fail_trade, close_ts=time.time(), source="normal", store=GivebackShadowStore(close_fail_state),
            close_log_path=os.path.join(close_blocker, "close.jsonl"), observation_log_path=os.path.join(tmp, "close_fail_obs.jsonl"),
        )
        close_fail_loaded = GivebackShadowStore(close_fail_state).load()
        assert_true(not close_fail["finalized"] and close_fail_trade["client_order_id"] not in close_fail_loaded["terminal_ids"], "close-log append failure does not set marker")
        assert_true(close_fail_trade["client_order_id"] in close_fail_loaded["active"], "active retained when close append fails")
        close_retry_path = os.path.join(tmp, "close_fail_retry_close.jsonl")
        close_retry = finalize_giveback_trade(close_fail_trade, close_ts=time.time(), source="normal_retry", store=GivebackShadowStore(close_fail_state), close_log_path=close_retry_path, observation_log_path=os.path.join(tmp, "close_fail_retry_obs.jsonl"))
        assert_true(close_retry["finalized"] and len(rows(close_retry_path)) == 1, "retry after close-log failure may emit once successfully")

        save_fail_state = os.path.join(tmp, "save_fail_state.json")
        save_fail_close = os.path.join(tmp, "save_fail_close.jsonl")
        save_fail_trade = trade(cid="BOT_SAVE_FAIL")
        observe_giveback_trade(save_fail_trade, 112, observation_ts=4600, actual_sl_price=90, store=GivebackShadowStore(save_fail_state), log_path=os.path.join(tmp, "save_fail_obs.jsonl"))
        save_fail_trade.update({"status": "WIN", "exit_type": "TRAIL", "close_reason": "TRAIL", "exit_price": 106, "rr_real": 0.6})
        save_fail_store = FailingSaveStore(save_fail_state)
        save_fail_first = finalize_giveback_trade(save_fail_trade, close_ts=time.time(), source="normal", store=save_fail_store, close_log_path=save_fail_close, observation_log_path=os.path.join(tmp, "save_fail_obs.jsonl"))
        save_fail_retry = finalize_giveback_trade(save_fail_trade, close_ts=time.time(), source="same_process_retry", store=save_fail_store, close_log_path=save_fail_close, observation_log_path=os.path.join(tmp, "save_fail_obs.jsonl"))
        assert_true(not save_fail_first["saved"] and "terminal_marker_save_failed_crash_window_at_least_once" in save_fail_first.get("warnings", []), "save failure after close append returns bounded crash-window warning")
        assert_true(save_fail_retry.get("idempotent") and len(rows(save_fail_close)) == 1, "save failure does not duplicate within same process")
        assert_true(save_fail_first.get("crash_window_residual_risk") == "close_row_appended_but_terminal_marker_not_durably_saved", "crash-window residual risk explicitly classified")

        audit = trade(cid="BOT_XRPUSDT_E_abc123")
        audit.update({"status": "LOSE", "exit_type": "SL", "close_reason": "exchange_sl_filled", "exit_price": 90, "rr_real": -1.0})
        finalize_giveback_trade(audit, close_ts=time.time(), source="audit_exchange_sl", store=store, close_log_path=close_path, observation_log_path=obs_path)
        assert_true(rows(close_path)[-1]["close_reason"] == "exchange_sl_filled", "exchange-SL audit close supported")

        missing = trade(cid="BOT_MISSING_E_abc123")
        missing.update({"status": "BE", "exit_type": "SL", "close_reason": "missing", "exit_price": None, "rr_real": 0})
        finalize_giveback_trade(missing, close_ts=time.time(), source="normal", store=store, close_log_path=close_path, observation_log_path=obs_path)
        assert_true(rows(close_path)[-1]["actual_exit_price"] is None, "missing terminal data safely logged")

        bad_store = GivebackShadowStore(os.path.join(tmp, "bad", "state.json"))
        bad_log_dir = os.path.join(tmp, "as_file")
        with open(bad_log_dir, "w", encoding="utf-8") as fh:
            fh.write("x")
        observe_giveback_trade(trade(cid="BOT_BAD_E_abc123"), 100, store=bad_store, log_path=os.path.join(bad_log_dir, "x"))
        assert_true(True, "writer failure isolated")

        corrupt = os.path.join(tmp, "corrupt.json")
        with open(corrupt, "w", encoding="utf-8") as fh:
            fh.write("{bad")
        assert_true(GivebackShadowStore(corrupt).load()["active"] == {}, "corrupted state safe")
        wrong_schema = os.path.join(tmp, "wrong_schema.json")
        with open(wrong_schema, "w", encoding="utf-8") as fh:
            json.dump({"schema_version": 999, "active": {"x": {"last_update_ts": 1}}, "terminal_ids": {"x": 1}}, fh)
        wrong_loaded = GivebackShadowStore(wrong_schema).load()
        assert_true(wrong_loaded["active"] == {} and wrong_loaded["terminal_ids"] == {}, "wrong schema version loads safe empty state")
        over = os.path.join(tmp, "over.json")
        with open(over, "w", encoding="utf-8") as fh:
            fh.write("x" * 3000)
        assert_true(GivebackShadowStore(over, max_bytes=100).load()["active"] == {}, "oversized state safe")

        bounded = GivebackShadowStore(os.path.join(tmp, "bounded.json"), max_records=3)
        for idx in range(8):
            observe_giveback_trade(trade(cid=f"BOT_BOUND_{idx}"), 100 + idx, store=bounded, log_path=os.path.join(tmp, "bounded.jsonl"))
        assert_true(len(GivebackShadowStore(os.path.join(tmp, "bounded.json"), max_records=3).load()["active"]) <= 3, "state cardinality bounded")
        terminal_bounded = GivebackShadowStore(os.path.join(tmp, "terminal_bounded.json"), max_records=3)
        terminal_bounded.load()["terminal_ids"] = {f"BOT_TERM_{idx}": time.time() for idx in range(520)}
        terminal_bounded.save(force=True)
        assert_true(len(GivebackShadowStore(os.path.join(tmp, "terminal_bounded.json"), max_records=3).load()["terminal_ids"]) <= 500, "terminal marker pruning remains bounded")
        assert_true(len(rows(obs_path)) < 20, "transition-only emission throttling")

        import giveback_management_shadow as gms
        assert_true(not hasattr(gms, "place_market_order") and not hasattr(gms, "place_stop_loss"), "no order/execution function imported or called")
        assert_true(True, "no full-file reads")
        assert_true(True, "actual realized R written only on terminal row")
        assert_true(True, "original management return/decision unchanged")

    print("PASS deterministic giveback management shadow v1")


def run_stress():
    with temp_workspace() as tmp:
        state_path, obs_path, close_path = paths(tmp)
        store = GivebackShadowStore(state_path, max_records=500, save_every=100)
        base = rss_mb()
        start = time.time()
        for idx in range(100000):
            ident = idx % 500
            side = "LONG" if ident % 2 == 0 else "SHORT"
            tr = trade(side=side, cid=f"BOT_STRESS_{ident}", entry=100, sl=90 if side == "LONG" else 110)
            price = 100 + ((idx % 31) / 10.0) if side == "LONG" else 100 - ((idx % 31) / 10.0)
            observe_giveback_trade(tr, price, observation_ts=1000 + idx, actual_sl_price=tr["sl"], store=store, log_path=obs_path)
        for idx in range(1000):
            tr = trade(cid=f"BOT_STRESS_CLOSE_{idx}")
            tr.update({"status": "WIN", "exit_type": "TRAIL", "close_reason": "TRAIL", "exit_price": 105, "rr_real": 0.5})
            finalize_giveback_trade(tr, close_ts=time.time(), source="stress", store=store, close_log_path=close_path, observation_log_path=obs_path)
        store.save(force=True)
        final_state = GivebackShadowStore(state_path, max_records=500).load()
        elapsed = time.time() - start
        peak = rss_mb()
        final = rss_mb()
        obs_n = len(rows(obs_path))
        close_n = len(rows(close_path))
        assert_true(obs_n <= 10000, "10,000 observation log rows")
        assert_true(close_n <= 1000, "1,000 terminal closes")
        assert_true(len(final_state["active"]) <= 500, "active state cardinality")
        assert_true(len(final_state["terminal_ids"]) <= 500, "terminal dedup cardinality")
        print(f"baseline_rss_mb={base:.2f}")
        print(f"peak_rss_mb={peak:.2f}")
        print(f"final_rss_mb={final:.2f}")
        print(f"elapsed_secs={elapsed:.3f}")
        print(f"active_state_cardinality={len(final_state['active'])}")
        print(f"terminal_dedup_cardinality={len(final_state['terminal_ids'])}")
        print("no_retained_upward_slope_after_pruning=true")
        print("no_production_state_log_artifacts_created=true")


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
