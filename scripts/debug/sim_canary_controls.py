import json
import os
import sys
import tempfile

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from state_manager import (
    CANARY_DEFAULT_STATE,
    atomic_save_json,
    canary_attach_open_attribution,
    canary_candidate_id_for_trade,
    canary_fresh_state,
    canary_latch,
    canary_load_state,
    canary_preflight_open,
    canary_record_close,
    canary_record_confirmed_open,
    save_trade,
)


def _write_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _cfg(path, enabled=True, epoch="epoch-a"):
    _write_json(
        path,
        {
            "canary_enabled": enabled,
            "canary_epoch": epoch,
            "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM",
            "canary_max_open": 2,
            "canary_max_total_trades": 50,
            "canary_max_cum_loss_r": -8.0,
            "canary_max_consecutive_losses": 5,
            "live_smc_research_enabled": True,
            "live_mode": True,
            "execution_mode": "paper_live",
        },
    )


def _state(path, epoch="epoch-a", opened=0, closed=0, abort=False):
    state = canary_fresh_state()
    state["canary_epoch"] = epoch
    state["candidate_id"] = "INCUMBENT_LIVE_CONFIRM"
    state["opened_ids"] = [f"BOT_SYM{i}_E_x" for i in range(opened)]
    state["counted_close_ids"] = state["opened_ids"][:closed]
    state["opened_total"] = opened
    state["closed_total"] = closed
    state["abort_latched"] = abort
    if abort:
        state["abort_reason"] = "test_abort"
        state["abort_ts"] = 1
    atomic_save_json(state, path)
    return state


def _trade(symbol="ADAUSDT", entry_type="CONFIRM_SMC_RESEARCH", cid="BOT_ADAUSDT_E_1", rr=0.0):
    return {
        "symbol": symbol,
        "side": "LONG",
        "entry_type": entry_type,
        "status": "OPEN",
        "owner": "bot",
        "client_order_id": cid,
        "exchange_client_id": cid,
        "rr_real": rr,
    }


def _assert(condition, label):
    if not condition:
        raise AssertionError(label)


def main():
    with tempfile.TemporaryDirectory() as td:
        cfg = os.path.join(td, "config.json")
        state = os.path.join(td, "canary_state.json")

        _cfg(cfg, enabled=False, epoch="")
        t = _trade()
        pre = canary_preflight_open(t, [], state_file=state, config_path=cfg)
        canary_attach_open_attribution(t, pre)
        _assert(pre["enabled"] is False and not t.get("canary_enabled_at_open"), "disabled default is inert")
        _assert(canary_preflight_open(_trade(entry_type="CONFIRM"), [], state_file=state, config_path=cfg)["enabled"] is False, "canary disabled does not gate plain confirm")

        _cfg(cfg)
        _state(state)
        _assert(canary_candidate_id_for_trade(_trade(entry_type="CONFIRM_SMC_RESEARCH")) == "INCUMBENT_LIVE_CONFIRM", "CONFIRM_SMC_RESEARCH maps to incumbent")
        _assert(canary_candidate_id_for_trade(_trade(entry_type="CONFIRM")) == "", "plain CONFIRM is not promoted")
        pre = canary_preflight_open(_trade(), [], state_file=state, config_path=cfg)
        _assert(pre["ok"], "valid epoch arms incumbent research lane")
        _assert(
            not canary_preflight_open(_trade(entry_type="CONFIRM"), [], state_file=state, config_path=cfg)["ok"],
            "plain CONFIRM rejected",
        )
        disk = json.load(open(cfg, encoding="utf-8"))
        disk["live_smc_research_enabled"] = False
        _write_json(cfg, disk)
        disabled_lane = canary_preflight_open(_trade(), [], state_file=state, config_path=cfg)
        _assert(not disabled_lane["ok"] and "live_smc_research_enabled_false" in disabled_lane["reason"], "disabled live research lane fails closed")
        disk["live_smc_research_enabled"] = True
        disk["live_mode"] = False
        _write_json(cfg, disk)
        disabled_live = canary_preflight_open(_trade(), [], state_file=state, config_path=cfg)
        _assert(not disabled_live["ok"] and "live_mode_false" in disabled_live["reason"], "disabled live mode fails closed")
        disk["live_mode"] = True
        disk["execution_mode"] = "paper"
        _write_json(cfg, disk)
        wrong_mode = canary_preflight_open(_trade(), [], state_file=state, config_path=cfg)
        _assert(not wrong_mode["ok"] and "execution_mode_not_live:paper" in wrong_mode["reason"], "wrong execution mode fails closed")
        disk["execution_mode"] = "paper_live"
        _write_json(cfg, disk)

        open_trades = [
            {"status": "OPEN", "owner": "bot", "canary_enabled_at_open": True, "canary_epoch": "epoch-a", "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM"},
            {"status": "OPEN", "owner": "bot", "canary_enabled_at_open": True, "canary_epoch": "epoch-a", "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM"},
            {"status": "OPEN", "owner": "manual", "symbol": "ETHUSDT"},
        ]
        _assert(not canary_preflight_open(_trade(), open_trades, state_file=state, config_path=cfg)["ok"], "third concurrent open rejected")
        _assert(canary_preflight_open(_trade(), open_trades[:1], state_file=state, config_path=cfg)["ok"], "first/second concurrent open allowed")

        _state(state, opened=49)
        for idx in range(49, 50):
            trade = _trade(symbol=f"SYM{idx}USDT", cid=f"BOT_SYM{idx}USDT_E_x")
            canary_attach_open_attribution(trade, canary_preflight_open(trade, [], state_file=state, config_path=cfg))
            result = canary_record_confirmed_open(trade, [], state_file=state, config_path=cfg)
            _assert(result["recorded"], "open 50 counted")
            dup = canary_record_confirmed_open(trade, [], state_file=state, config_path=cfg)
            _assert(dup.get("idempotent"), "duplicate confirmed open replay idempotent")
        _assert(canary_load_state(state)["opened_total"] == 50, "opened_total exactly 50")
        _assert(not canary_preflight_open(_trade(cid="BOT_51_E_x"), [], state_file=state, config_path=cfg)["ok"], "attempt 51 rejected")

        before = canary_load_state(state)["opened_total"]
        _assert(before == 50, "open failed does not increment without record call")

        _state(state, opened=1)
        closed = _trade(cid="BOT_SYM0_E_x", rr=1.25)
        closed.update({"canary_enabled_at_open": True, "canary_epoch": "epoch-a", "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM", "status": "WIN"})
        _assert(canary_record_close(closed, state_file=state, config_path=cfg)["recorded"], "close counted")
        _assert(canary_record_close(closed, state_file=state, config_path=cfg).get("idempotent"), "close replay idempotent")
        _assert(canary_load_state(state)["cum_realized_r"] == 1.25, "cumulative R updates once")

        _state(state, opened=5)
        for i in range(5):
            loss = _trade(cid=f"BOT_SYM{i}_E_x", rr=-1.0)
            loss.update({"canary_enabled_at_open": True, "canary_epoch": "epoch-a", "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM", "status": "LOSE"})
            canary_record_close(loss, state_file=state, config_path=cfg)
        _assert(canary_load_state(state)["abort_latched"], "five consecutive losses latch")

        _state(state, opened=4)
        for i in range(4):
            loss = _trade(cid=f"BOT_SYM{i}_E_x", rr=-2.0)
            loss.update({"canary_enabled_at_open": True, "canary_epoch": "epoch-a", "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM", "status": "LOSE"})
            canary_record_close(loss, state_file=state, config_path=cfg)
        _assert(canary_load_state(state)["abort_latched"], "cumulative -8R latch")
        _assert(not canary_preflight_open(_trade(cid="BOT_NEW_E_x"), [], state_file=state, config_path=cfg)["ok"], "latched blocks new entries")

        preserved = canary_load_state(state)
        _assert(preserved["abort_latched"], "restart preserves latch")

        _state(state, epoch="other-epoch")
        _assert(not canary_preflight_open(_trade(), [], state_file=state, config_path=cfg)["ok"], "epoch mismatch fails closed")
        _write_json(state, {"bad": True})
        corrupt = canary_preflight_open(_trade(), [], state_file=state, config_path=cfg)
        _assert(not corrupt["ok"] and corrupt["state"].get("abort_latched"), "corrupt state fails closed latched")
        os.remove(state)
        missing = canary_preflight_open(_trade(), [], state_file=state, config_path=cfg)
        _assert(not missing["ok"] and missing["state"].get("abort_latched"), "missing state fails closed latched")

        _state(state)
        historical = [_trade(symbol=f"HIST{i}USDT", cid=f"BOT_HIST{i}_E_x") for i in range(51)]
        manual_eth = {"symbol": "ETHUSDT", "status": "OPEN", "owner": "manual"}
        _assert(canary_preflight_open(_trade(cid="BOT_FRESH_E_x"), historical + [manual_eth], state_file=state, config_path=cfg)["ok"], "historical/manual trades ignored")

        _state(state, opened=50, closed=49)
        existing = _trade(cid="BOT_SYM49_E_x", rr=0.5)
        existing.update({"canary_enabled_at_open": True, "canary_epoch": "epoch-a", "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM", "status": "WIN"})
        _assert(canary_record_close(existing, state_file=state, config_path=cfg)["recorded"], "cap reached still permits close accounting")

        import execution

        class DummyCtx:
            def __init__(self, trade):
                self.trades = [trade]
                self.state_file = os.path.join(td, "live_state.json")
                self.trades_csv = os.path.join(td, "live_trades.csv")
                self.account_balance = 100.0
                self.equity_peak = 100.0
                self.session_pnl_r = 0.0
                self.cooldown = {}
                self.stats = {"win": 0, "loss": 0, "be": 0}
                self.mode_prefix = None
                self.execution_mode = "live"

            def save_account_state(self):
                return None

        original_record_close = execution.canary_record_close
        original_latch = execution.canary_latch
        original_config_status = execution.canary_config_status
        original_save_trade = execution.save_trade
        original_save_tier_log = execution.save_tier_log
        original_log_false_positive = execution.log_false_positive
        original_log_wyckoff_outcome = execution.log_wyckoff_outcome
        original_send_telegram = execution.send_telegram
        try:
            execution.canary_record_close = lambda trade: canary_record_close(trade, state_file=state, config_path=cfg)
            execution.canary_latch = lambda reason: canary_latch(reason, state_file=state, config_path=cfg)
            execution.canary_config_status = lambda: __import__("state_manager").canary_config_status(config_path=cfg)
            execution.save_trade = lambda *args, **kwargs: None
            execution.save_tier_log = lambda *args, **kwargs: None
            execution.log_false_positive = lambda *args, **kwargs: None
            execution.log_wyckoff_outcome = lambda *args, **kwargs: None
            execution.send_telegram = lambda *args, **kwargs: None

            _state(state, opened=1)
            for quarantine_reason in (
                "invalid_symbol",
                "unrecoverable_stop_failure",
                "orphan_local_trade",
                "missing_exchange_qty",
            ):
                _state(state, opened=1)
                q_trade = _trade(cid="BOT_SYM0_E_x")
                q_trade.update({
                    "canary_enabled_at_open": True,
                    "canary_epoch": "epoch-a",
                    "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM",
                    "status": "OPEN",
                })
                _assert(execution._quarantine_trade(q_trade, quarantine_reason), f"{quarantine_reason} quarantines")
                q_state = canary_load_state(state)
                _assert(q_state["abort_latched"], f"{quarantine_reason} latch set")
                _assert(q_state["abort_reason"] == f"canary_trade_quarantined:{quarantine_reason}", f"{quarantine_reason} exact latch reason")
                _assert(not execution._quarantine_trade(q_trade, quarantine_reason), f"{quarantine_reason} repeat idempotent")
                q_state_repeat = canary_load_state(state)
                _assert(q_state_repeat["abort_reason"] == f"canary_trade_quarantined:{quarantine_reason}", f"{quarantine_reason} repeat keeps reason")
                _assert(q_state_repeat["opened_total"] == 1 and q_state_repeat["closed_total"] == 0, f"{quarantine_reason} repeat does not reset ledger")

            _state(state, opened=0)
            manual_q = {"symbol": "ETHUSDT", "status": "OPEN", "owner": "manual", "canary_enabled_at_open": False}
            _assert(execution._quarantine_trade(manual_q, "orphan_local_trade"), "manual quarantine can mark local record")
            _assert(not canary_load_state(state)["abort_latched"], "manual/non-canary quarantine ignored by canary")

            _state(state, opened=0)
            one_q = {
                "status": "OPEN",
                "owner": "bot",
                "quarantined": True,
                "canary_enabled_at_open": True,
                "canary_epoch": "epoch-a",
                "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM",
            }
            one_open = {
                "status": "OPEN",
                "owner": "bot",
                "canary_enabled_at_open": True,
                "canary_epoch": "epoch-a",
                "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM",
            }
            _assert(canary_preflight_open(_trade(cid="BOT_SLOT1_E_x"), [one_q], state_file=state, config_path=cfg)["ok"], "one quarantined open consumes one slot but second allowed")
            _assert(not canary_preflight_open(_trade(cid="BOT_SLOT2_E_x"), [one_q, one_open], state_file=state, config_path=cfg)["ok"], "one quarantined plus one normal rejects third")
            _assert(not canary_preflight_open(_trade(cid="BOT_SLOT3_E_x"), [one_q, dict(one_q)], state_file=state, config_path=cfg)["ok"], "two quarantined opens reject all new opens")
            closed_q = dict(one_q)
            closed_q["status"] = "LOSE"
            _assert(canary_preflight_open(_trade(cid="BOT_SLOT4_E_x"), [closed_q], state_file=state, config_path=cfg)["ok"], "closed quarantined trade frees slot")
            hist_q = dict(one_q)
            hist_q["canary_enabled_at_open"] = False
            _assert(canary_preflight_open(_trade(cid="BOT_SLOT5_E_x"), [hist_q], state_file=state, config_path=cfg)["ok"], "historical/non-canary quarantined excluded")

            _state(state, opened=1)
            startup_orphan = _trade(cid="BOT_SYM0_E_x")
            startup_orphan.update({
                "canary_enabled_at_open": True,
                "canary_epoch": "epoch-a",
                "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM",
                "status": "OPEN",
            })
            execution._quarantine_trade(startup_orphan, "orphan_local_trade")
            _assert(canary_load_state(state)["abort_reason"] == "canary_trade_quarantined:orphan_local_trade", "startup orphan_local_trade latches")
            _assert(not canary_preflight_open(_trade(cid="BOT_AFTER_RESTART_E_x"), [startup_orphan], state_file=state, config_path=cfg)["ok"], "restart with quarantined canary blocks new open")
            _assert(startup_orphan.get("quarantined") and startup_orphan.get("repair_disabled"), "orphan trade preserved for review")

            _state(state, opened=1)
            stop_failure = _trade(cid="BOT_SYM0_E_x")
            stop_failure.update({
                "canary_enabled_at_open": True,
                "canary_epoch": "epoch-a",
                "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM",
                "status": "OPEN",
            })
            execution._quarantine_trade(stop_failure, "unrecoverable_stop_failure")
            _assert(canary_load_state(state)["abort_reason"] == "canary_trade_quarantined:unrecoverable_stop_failure", "unrecoverable stop failure latches")
            _assert(not canary_preflight_open(_trade(cid="BOT_REPLACE_E_x"), [stop_failure], state_file=state, config_path=cfg)["ok"], "unrecoverable stop failure prevents replacement")
            _assert(canary_load_state(state)["abort_latched"], "quarantine latch survives reload simulation")

            _state(state, opened=1)
            audit_loss = _trade(cid="BOT_SYM0_E_x")
            audit_loss.update({
                "canary_enabled_at_open": True,
                "canary_epoch": "epoch-a",
                "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM",
                "entry": 100.0,
                "entry_real": 100.0,
                "sl": 90.0,
                "sl_init": 90.0,
                "exit_price": 90.0,
                "time": 1.0,
                "risk_percent": 0.01,
            })
            ctx = DummyCtx(audit_loss)
            _assert(execution._finalize_audit_exchange_sl_close(audit_loss, ctx), "audit close finalized")
            _assert(audit_loss.get("close_ts") == audit_loss.get("close_time"), "audit close persists close_ts")
            _assert(audit_loss.get("closed_at_unix") == audit_loss.get("close_time"), "audit close persists closed_at_unix")
            audit_state = canary_load_state(state)
            _assert(audit_state["closed_total"] == 1 and audit_state["cum_realized_r"] == -1.0, "audit loss increments once")
            _assert(execution._finalize_audit_exchange_sl_close(audit_loss, ctx), "duplicate audit close replay safe")
            _assert(canary_load_state(state)["closed_total"] == 1, "duplicate audit close does not double count")
            _assert(canary_record_close(audit_loss, state_file=state, config_path=cfg).get("idempotent"), "normal close after audit is idempotent")

            _state(state, opened=5)
            for i in range(5):
                audit = _trade(cid=f"BOT_SYM{i}_E_x")
                audit.update({
                    "canary_enabled_at_open": True,
                    "canary_epoch": "epoch-a",
                    "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM",
                    "entry": 100.0,
                    "entry_real": 100.0,
                    "sl": 90.0,
                    "sl_init": 90.0,
                    "exit_price": 90.0,
                    "time": 1.0,
                    "risk_percent": 0.01,
                })
                execution._finalize_audit_exchange_sl_close(audit, DummyCtx(audit))
            _assert(canary_load_state(state)["abort_latched"], "five audit losses latch")

            _state(state, opened=8)
            for i in range(8):
                audit = _trade(cid=f"BOT_SYM{i}_E_x")
                audit.update({
                    "canary_enabled_at_open": True,
                    "canary_epoch": "epoch-a",
                    "canary_candidate_id": "INCUMBENT_LIVE_CONFIRM",
                    "entry": 100.0,
                    "entry_real": 100.0,
                    "sl": 90.0,
                    "sl_init": 90.0,
                    "exit_price": 90.0,
                    "time": 1.0,
                    "risk_percent": 0.01,
                })
                execution._finalize_audit_exchange_sl_close(audit, DummyCtx(audit))
            _assert(canary_load_state(state)["abort_reason"] == "canary_max_cum_loss_r_reached", "audit cumulative -8R latches")

            _state(state, opened=1)
            manual = _trade(cid="BOT_SYM0_E_x")
            manual.update({"owner": "manual", "canary_enabled_at_open": False})
            _assert(not execution._finalize_audit_exchange_sl_close(manual, DummyCtx(manual)), "manual audit trade ignored")
            _assert(canary_load_state(state)["closed_total"] == 0, "manual/non-canary not counted")

            normal_csv = os.path.join(td, "normal_close_trades.csv")
            normal_closed = _trade(cid="BOT_NORMAL_E_x", rr=0.4)
            normal_closed.update({
                "id": "normal-close",
                "time": 1000.0,
                "close_time": 1200.0,
                "close_ts": 1200.0,
                "closed_at_unix": 1200.0,
                "status": "WIN",
                "exit_type": "TP",
            })
            save_trade(normal_closed, normal_csv)
            with open(normal_csv, "r", encoding="utf-8") as handle:
                header = handle.readline().strip().split(",")
                row = handle.readline().strip().split(",")
            saved = dict(zip(header, row))
            _assert(saved.get("close_ts") == "1200.0", "normal close CSV persists close_ts")
            _assert(saved.get("closed_at_unix") == "1200.0", "normal close CSV persists closed_at_unix")
        finally:
            execution.canary_record_close = original_record_close
            execution.canary_latch = original_latch
            execution.canary_config_status = original_config_status
            execution.save_trade = original_save_trade
            execution.save_tier_log = original_save_tier_log
            execution.log_false_positive = original_log_false_positive
            execution.log_wyckoff_outcome = original_log_wyckoff_outcome
            execution.send_telegram = original_send_telegram

    print("CANARY CONTROL SIM PASS")


if __name__ == "__main__":
    main()
