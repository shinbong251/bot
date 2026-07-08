#!/usr/bin/env python3
"""Simulator for LIVE_RESEARCH_MICRO_PAUSE stale-streak rearm protection."""

import os
import sys
from dataclasses import dataclass, field


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import signal_dispatcher as sd


@dataclass
class DummyCtx:
    execution_mode: str = "live"
    trades: list = field(default_factory=list)


def _close(trade_id, realized_r, ts):
    return {
        "id": trade_id,
        "symbol": "TESTUSDT",
        "side": "LONG",
        "actual_realized_r": realized_r,
        "_sort_ts": ts,
    }


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<76} {status} {detail}")
    return bool(condition)


def _health(paper="GREEN", live="GREEN", reasons=None, metrics=None, ts=1000.0):
    return {
        "ts": ts,
        "paper_health": paper,
        "paper_active_health": paper,
        "live_health": live,
        "reasons": list(reasons or []),
        "live_metrics": dict(metrics or {}),
    }


def _call(now_ts, close_rows, pause_rows, health="GREEN", health_rows=None):
    return sd._live_research_micro_pause_status(
        ctx=DummyCtx(),
        now_ts=now_ts,
        close_rows=close_rows,
        pause_rows=pause_rows,
        health_rows=health_rows if health_rows is not None else [_health(paper=health)],
    )


def _set_rows_with_same_closed_count(rows):
    sets = [
        row for row in rows
        if row.get("action") == "SET_AND_BLOCK"
        and row.get("pause_reason") in (
            "LIVE_MICRO_PAUSE_3_LOSS_STREAK",
            "LIVE_MICRO_PAUSE_ROLLING_NET",
        )
    ]
    for previous, current in zip(sets, sets[1:]):
        if previous.get("live_closed_count") == current.get("live_closed_count"):
            return previous, current
    return None, None


def main():
    results = []
    original_config = dict(sd.config)
    original_pause_write = sd._live_research_micro_write
    pause_logs = []

    def fake_pause_write(row):
        pause_logs.append(dict(row))

    sd._live_research_micro_write = fake_pause_write
    sd.config["live_research_micro_pause_enabled"] = True
    sd.config["live_research_loss_streak_pause_count"] = 3
    sd.config["live_research_micro_pause_hours"] = 3
    sd.config["live_research_rolling_net_pause_r"] = -2.0
    sd.config["max_live_research_trades"] = 1
    sd.config["live_risk_per_trade"] = 0.005
    sd.config["live_max_portfolio_risk"] = 0.005

    try:
        now = 1000.0
        duration = 3 * 3600
        three_losses = [
            _close("loss-1", -0.4, 1),
            _close("loss-2", -0.5, 2),
            _close("loss-3", -0.6, 3),
        ]

        ok, reason, detail = _call(now, three_losses, pause_logs)
        first_pause_until = detail.get("pause_until")
        results.append(_assert(
            "A. 3 losses first signal => SET_AND_BLOCK with close identity",
            ok is False
            and reason == "LIVE_MICRO_BLOCKED_LOSS_STREAK"
            and detail.get("action") == "SET_AND_BLOCK"
            and detail.get("pause_reason") == "LIVE_MICRO_PAUSE_3_LOSS_STREAK"
            and round(first_pause_until - now) == duration
            and detail.get("live_closed_count") == 3
            and detail.get("pause_armed_live_closed_count") == 3
            and detail.get("last_live_close_key") == "id:loss-3",
            f"detail={detail}",
        ))

        active_ok = True
        active_details = []
        for step in range(1, 6):
            ok, reason, active_detail = _call(now + step * 60, three_losses, pause_logs)
            active_details.append(active_detail)
            active_ok = (
                active_ok
                and ok is False
                and reason == "LIVE_MICRO_BLOCKED_LOSS_STREAK"
                and active_detail.get("action") == "BLOCK"
                and active_detail.get("pause_until") == first_pause_until
            )
        results.append(_assert(
            "B. active pause many signals => BLOCK and pause_until unchanged",
            active_ok,
            f"last_detail={active_details[-1] if active_details else None}",
        ))

        ok, reason, detail = _call(first_pause_until + 1, three_losses, pause_logs)
        results.append(_assert(
            "C. expired pause, no new close => allow without rearm",
            ok is True
            and reason == ""
            and detail.get("action") == "ALLOW_AFTER_PAUSE_EXPIRY_NO_NEW_CLOSE"
            and detail.get("reason") == "MICRO_PAUSE_NOT_REARMED_STALE_STREAK"
            and detail.get("previous_pause_until") == first_pause_until
            and detail.get("pause_until") is None,
            f"detail={detail}",
        ))

        four_losses = three_losses + [_close("loss-4", -0.3, 4)]
        ok, reason, detail = _call(first_pause_until + 2, four_losses, pause_logs)
        second_pause_until = detail.get("pause_until")
        results.append(_assert(
            "D. expired pause, new losing close => new SET_AND_BLOCK",
            ok is False
            and reason == "LIVE_MICRO_BLOCKED_LOSS_STREAK"
            and detail.get("action") == "SET_AND_BLOCK"
            and detail.get("live_closed_count") == 4
            and detail.get("last_live_close_key") == "id:loss-4"
            and round(second_pause_until - (first_pause_until + 2)) == duration,
            f"detail={detail}",
        ))

        new_non_loss = four_losses + [_close("win-1", 0.2, 5)]
        ok, reason, detail = _call(second_pause_until + 1, new_non_loss, pause_logs)
        results.append(_assert(
            "E. expired pause, new non-losing close + stale health row => WARN_ALLOW",
            ok is True
            and reason == ""
            and detail.get("action") == "WARN_ALLOW"
            and detail.get("pause_reason") == "LIVE_HEALTH_ROW_STALE_WARN"
            and detail.get("live_loss_streak") == 0,
            f"detail={detail}",
        ))

        duplicate_previous, duplicate_current = _set_rows_with_same_closed_count(pause_logs)
        results.append(_assert(
            "F. deadlock detector => no consecutive SET_AND_BLOCK with same live_closed_count",
            duplicate_previous is None and duplicate_current is None,
            f"previous={duplicate_previous} current={duplicate_current}",
        ))

        pause_logs.clear()
        rolling_rows = [
            _close("r-loss-1", -1.0, 1),
            _close("r-win-1", 0.2, 2),
            _close("r-loss-2", -1.3, 3),
        ]
        ok, reason, detail = _call(now, rolling_rows, pause_logs)
        rolling_pause_until = detail.get("pause_until")
        ok2, reason2, detail2 = _call(rolling_pause_until + 1, rolling_rows, pause_logs)
        rolling_rows_new_loss = rolling_rows + [_close("r-loss-3", -0.1, 4)]
        ok3, reason3, detail3 = _call(rolling_pause_until + 2, rolling_rows_new_loss, pause_logs)
        results.append(_assert(
            "G. rolling-net pause => no stale rearm, fresh close can rearm",
            ok is False
            and reason == "LIVE_MICRO_BLOCKED_ROLLING_NET"
            and detail.get("action") == "SET_AND_BLOCK"
            and detail.get("pause_reason") == "LIVE_MICRO_PAUSE_ROLLING_NET"
            and ok2 is True
            and reason2 == ""
            and detail2.get("action") == "ALLOW_AFTER_PAUSE_EXPIRY_NO_NEW_CLOSE"
            and ok3 is False
            and reason3 == "LIVE_MICRO_BLOCKED_ROLLING_NET"
            and detail3.get("action") == "SET_AND_BLOCK"
            and detail3.get("live_closed_count") == 4,
            f"first={detail} stale={detail2} fresh={detail3}",
        ))

        duplicate_previous, duplicate_current = _set_rows_with_same_closed_count(pause_logs)
        results.append(_assert(
            "G. rolling-net deadlock detector => no duplicate SET_AND_BLOCK close count",
            duplicate_previous is None and duplicate_current is None,
            f"previous={duplicate_previous} current={duplicate_current}",
        ))

        pause_logs.clear()
        ok, reason, detail = _call(now, [], [], health_rows=[_health(paper="RED", live="GREEN")])
        results.append(_assert(
            "H1. paper RED + live GREEN + cap=1/risk micro/no pause => ALLOW warning",
            ok is True
            and reason == ""
            and detail.get("pause_reason") == "LIVE_MICRO_WARN_PAPER_HEALTH_RED_ALLOWED"
            and detail.get("micro_allowed_despite_paper_red") is True
            and detail.get("scale_block") is False,
            f"detail={detail}",
        ))

        expired_stale = [{
            "event_type": "LIVE_RESEARCH_MICRO_PAUSE",
            "ts": now,
            "pause_reason": "LIVE_MICRO_PAUSE_3_LOSS_STREAK",
            "pause_until": now - 1,
            "live_closed_count": 3,
            "pause_armed_live_closed_count": 3,
            "last_live_close_key": "id:loss-3",
            "pause_armed_last_live_close_key": "id:loss-3",
        }]
        ok, reason, detail = _call(
            now,
            three_losses,
            expired_stale,
            health_rows=[_health(paper="RED", live="GREEN")],
        )
        results.append(_assert(
            "H2. paper RED + expired stale pause/no new close => ALLOW_AFTER_PAUSE_EXPIRY_NO_NEW_CLOSE",
            ok is True
            and reason == ""
            and detail.get("action") == "ALLOW_AFTER_PAUSE_EXPIRY_NO_NEW_CLOSE"
            and detail.get("pause_reason") == "LIVE_MICRO_WARN_PAPER_HEALTH_RED_ALLOWED"
            and detail.get("micro_allowed_despite_paper_red") is True,
            f"detail={detail}",
        ))

        ok, reason, detail = _call(now, [], [], health_rows=[_health(paper="RED", live="RED")])
        results.append(_assert(
            "H3. paper RED + live RED => BLOCK live-specific reason",
            ok is False
            and reason == "LIVE_MICRO_BLOCKED_LIVE_HEALTH"
            and detail.get("pause_reason") == "LIVE_MICRO_BLOCKED_LIVE_HEALTH",
            f"detail={detail}",
        ))

        stale_red = _health(
            paper="GREEN",
            live="RED",
            reasons=["live_consecutive_losses=3>=3"],
            metrics={"live_loss_streak": 3, "live_rolling_net_r": -3.0},
            ts=now - sd._LIVE_RESEARCH_HEALTH_ROW_MAX_AGE_SECS - 1,
        )
        ok, reason, detail = _call(now, [], [], health_rows=[stale_red])
        results.append(_assert(
            "H3a. stale live_health RED + cap=1/no current issue => WARN_ALLOW",
            ok is True
            and reason == ""
            and detail.get("pause_reason") == "LIVE_HEALTH_ROW_STALE_WARN"
            and detail.get("health_row_age_sec") > sd._LIVE_RESEARCH_HEALTH_ROW_MAX_AGE_SECS
            and detail.get("raw_live_health") == "RED"
            and detail.get("live_health") == "UNKNOWN",
            f"detail={detail}",
        ))

        ok, reason, detail = _call(now, [], [], health_rows=[])
        results.append(_assert(
            "H3b. missing health file + cap=1/no current issue => WARN_ALLOW",
            ok is True
            and reason == ""
            and detail.get("pause_reason") == "LIVE_HEALTH_ROW_STALE_WARN"
            and detail.get("health_row_status") == "MISSING",
            f"detail={detail}",
        ))

        ok, reason, detail = _call(
            now,
            three_losses,
            [],
            health_rows=[_health(
                paper="GREEN",
                live="RED",
                reasons=["live_consecutive_losses=3>=3"],
                metrics={"live_loss_streak": 3},
            )],
        )
        results.append(_assert(
            "H3c. fresh live RED + current trailing loss streak >=3 => BLOCK loss-streak",
            ok is False
            and reason == "LIVE_MICRO_BLOCKED_LOSS_STREAK"
            and detail.get("pause_reason") == "LIVE_MICRO_PAUSE_3_LOSS_STREAK",
            f"detail={detail}",
        ))

        sd.config["max_live_research_trades"] = 2
        ok, reason, detail = _call(now, [], [], health_rows=[stale_red])
        results.append(_assert(
            "H3d. stale live_health RED + cap>1 => BLOCK_SCALE stale health",
            ok is False
            and reason == "LIVE_SCALE_BLOCKED_STALE_HEALTH"
            and detail.get("action") == "BLOCK_SCALE",
            f"detail={detail}",
        ))
        sd.config["max_live_research_trades"] = 1
        sd.config["live_risk_per_trade"] = 0.006
        ok, reason, detail = _call(now, [], [], health_rows=[stale_red])
        results.append(_assert(
            "H3e. stale live_health RED + risk>0.005 => BLOCK_SCALE stale health",
            ok is False
            and reason == "LIVE_SCALE_BLOCKED_STALE_HEALTH"
            and detail.get("action") == "BLOCK_SCALE",
            f"detail={detail}",
        ))
        sd.config["live_risk_per_trade"] = 0.005

        ok, reason, detail = sd._live_research_micro_pause_status(
            ctx=DummyCtx(trades=[{
                "status": "OPEN",
                "owner": "bot",
                "entry_type": "CONFIRM_SMC_RESEARCH",
                "symbol": "SLMISSUSDT",
            }]),
            now_ts=now,
            close_rows=[],
            pause_rows=[],
            health_rows=[stale_red],
        )
        results.append(_assert(
            "H3f. stale health row + open bot research missing SL => BLOCK current SL reason",
            ok is False
            and reason == "LIVE_MICRO_BLOCKED_SL_SYNC"
            and detail.get("pause_reason") == "LIVE_MICRO_BLOCKED_SL_SYNC"
            and detail.get("current_sl_missing_symbols") == ["SLMISSUSDT"],
            f"detail={detail}",
        ))

        pause_logs.clear()
        ok, reason, detail = _call(now, three_losses, [], health_rows=[_health(paper="RED", live="GREEN")])
        results.append(_assert(
            "H4. paper RED + active fresh loss streak => BLOCK loss-streak",
            ok is False
            and reason == "LIVE_MICRO_BLOCKED_LOSS_STREAK"
            and detail.get("pause_reason") == "LIVE_MICRO_PAUSE_3_LOSS_STREAK",
            f"detail={detail}",
        ))

        ok, reason, detail = _call(
            now,
            [
                _close("roll-1", -1.2, 1),
                _close("roll-2", 0.1, 2),
                _close("roll-3", -1.0, 3),
            ],
            [],
            health_rows=[_health(paper="RED", live="GREEN")],
        )
        results.append(_assert(
            "H5. paper RED + live rolling net <= -2R => BLOCK rolling-net",
            ok is False
            and reason == "LIVE_MICRO_BLOCKED_ROLLING_NET"
            and detail.get("pause_reason") == "LIVE_MICRO_PAUSE_ROLLING_NET",
            f"detail={detail}",
        ))

        sd.config["max_live_research_trades"] = 2
        ok, reason, detail = _call(now, [], [], health_rows=[_health(paper="RED", live="GREEN")])
        results.append(_assert(
            "H6. paper RED + cap>1 => LIVE_SCALE_BLOCKED_PAPER_HEALTH",
            ok is False
            and reason == "LIVE_SCALE_BLOCKED_PAPER_HEALTH"
            and detail.get("scale_block") is True,
            f"detail={detail}",
        ))

        sd.config["max_live_research_trades"] = 1
        sd.config["live_risk_per_trade"] = 0.006
        ok, reason, detail = _call(now, [], [], health_rows=[_health(paper="RED", live="GREEN")])
        results.append(_assert(
            "H7. paper RED + risk>0.005 => LIVE_SCALE_BLOCKED_PAPER_HEALTH",
            ok is False
            and reason == "LIVE_SCALE_BLOCKED_PAPER_HEALTH"
            and detail.get("scale_block") is True,
            f"detail={detail}",
        ))

        sd.config["live_risk_per_trade"] = 0.005
        ok, reason, detail = _call(
            now,
            [],
            [],
            health_rows=[_health(
                paper="RED",
                live="RED",
                reasons=["entry_fill_unconfirmed", "missing exchange SL"],
                metrics={"live_unconfirmed_rr_n": 1},
            )],
        )
        results.append(_assert(
            "H8. entry unconfirmed / SL missing => micro safety BLOCK",
            ok is False
            and reason == "LIVE_MICRO_BLOCKED_SL_SYNC"
            and detail.get("live_sl_sync_failure") is True,
            f"detail={detail}",
        ))

        ok, reason, detail = _call(now, [], [], health_rows=[_health(paper="RED", live="RED")])
        results.append(_assert(
            "H9. cap=1 live-side block must not emit scale paper reason",
            ok is False
            and reason != "LIVE_SCALE_BLOCKED_PAPER_HEALTH"
            and detail.get("pause_reason") != "LIVE_SCALE_BLOCKED_PAPER_HEALTH",
            f"detail={detail}",
        ))

        from exchange import live_executor
        original_load_config = live_executor._load_config
        original_tier5 = live_executor._live_symbol_is_tier5
        try:
            live_executor._load_config = lambda: {
                "live_mode": True,
                "live_smc_research_enabled": True,
                "max_live_research_trades": 1,
            }
            live_executor._live_symbol_is_tier5 = lambda symbol: False
            existing = {
                "status": "OPEN",
                "owner": "bot",
                "entry_type": "CONFIRM_SMC_RESEARCH",
                "symbol": "OPENUSDT",
                "side": "LONG",
                "entry_source": "actual_exchange_fill",
                "entry_state": "ENTRY_CONFIRMED",
                "exchange_sl_id": "sl-open",
                "exchange_sl_price_confirmed": 90.0,
            }
            candidate = {
                "entry_type": "CONFIRM_SMC_RESEARCH",
                "symbol": "NEWUSDT",
                "side": "LONG",
                "entry": 100.0,
                "sl": 90.0,
                "tp": 125.0,
                "rr": 2.5,
            }
            micro_ok, micro_reason, micro_detail = sd._live_research_micro_pause_status(
                ctx=DummyCtx(trades=[existing]),
                now_ts=now,
                close_rows=[],
                pause_rows=[],
                health_rows=[stale_red],
            )
            allowed, gate_reason = live_executor.check_live_research_safety_gate(
                candidate,
                ctx=DummyCtx(trades=[existing]),
                open_trades=[existing],
            )
            results.append(_assert(
                "H10. stale health row but cap occupied => micro allows then cap gate BLOCKS",
                micro_ok is True
                and micro_reason == ""
                and micro_detail.get("pause_reason") == "LIVE_HEALTH_ROW_STALE_WARN"
                and allowed is False
                and "live_research_open=1" in gate_reason,
                f"micro={micro_detail} gate_reason={gate_reason}",
            ))
        finally:
            live_executor._load_config = original_load_config
            live_executor._live_symbol_is_tier5 = original_tier5
    finally:
        sd._live_research_micro_write = original_pause_write
        sd.config.clear()
        sd.config.update(original_config)

    print("\nRESULT:", "PASS" if all(results) else "FAIL")
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
