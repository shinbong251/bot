#!/usr/bin/env python3
"""Simulator for CONFIRM_SMC_RESEARCH rolling health classification."""

import json
import os
import sys
from dataclasses import dataclass, field


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from scripts.debug import audit_research_rolling_health as rh
import signal_dispatcher as sd


@dataclass
class DummyCtx:
    execution_mode: str = "live"
    trades: list = field(default_factory=list)


def _row(value, idx=0, side="LONG", phase="NEUTRAL", regime="TREND"):
    return {
        "ts": float(idx),
        "raw_realized_r": value,
        "calibrated_r_cap_1_0": value,
        "calibrated_r_cap_1_2": value,
        "side": side,
        "phase": phase,
        "market_regime": regime,
        "entry_type": "CONFIRM_SMC_RESEARCH",
    }


def _rows(values):
    return [_row(value, idx=i, side=("LONG" if i % 2 == 0 else "SHORT")) for i, value in enumerate(values)]


def _interleave(wins, losses):
    out = []
    for idx in range(max(len(wins), len(losses))):
        if idx < len(wins):
            out.append(wins[idx])
        if idx < len(losses):
            out.append(losses[idx])
    return out


def _paper_health(values):
    rows = _rows(values)
    last20 = rh.metrics(rows[-20:])
    last50 = rh.metrics(rows[-50:])
    return rh.classify_paper(last20, last50), last20, last50


def _active_health(values, min_active=20):
    rows = _rows(values)
    health, reasons, last20, last50 = rh.classify_active_paper(rows, min_active)
    return (health, reasons), last20, last50


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<74} {status} {detail}")
    return bool(condition)


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

    try:
        values = _interleave([1.0] * 25, [-0.8333333333] * 25)
        (health, reasons), last20, last50 = _paper_health(values)
        results.append(_assert(
            "last50 positive PF 1.2 => GREEN",
            health == "GREEN"
            and last50["calibrated_net_r_cap_1_0"] > 0
            and last50["calibrated_pf_cap_1_0"] >= 1.10,
            f"health={health} reasons={reasons} last50={last50}",
        ))

        values = _interleave([1.0] * 25, [-1.1] * 25)
        (health, reasons), _, last50 = _paper_health(values)
        results.append(_assert(
            "last50 net <= 0 => RED",
            health == "RED" and last50["calibrated_net_r_cap_1_0"] <= 0,
            f"health={health} reasons={reasons} last50={last50}",
        ))

        values = [1.0] * 30 + [-0.21] * 20
        (health, reasons), last20, _ = _paper_health(values)
        results.append(_assert(
            "last20 <= -4R => RED",
            health == "RED" and last20["calibrated_net_r_cap_1_0"] <= -4.0,
            f"health={health} reasons={reasons} last20={last20}",
        ))

        values = _interleave([0.5] * 45, [-0.1] * 0) + [-0.1] * 5
        (health, reasons), _, last50 = _paper_health(values)
        results.append(_assert(
            "max_loss_streak >= 5 => RED",
            health == "RED" and last50["max_loss_streak"] >= 5,
            f"health={health} reasons={reasons} last50={last50}",
        ))

        values = _interleave([1.0] * 25, [-0.9524] * 25)
        (health, reasons), _, last50 = _paper_health(values)
        results.append(_assert(
            "last50 positive but PF 1.05 => YELLOW",
            health == "YELLOW"
            and last50["calibrated_net_r_cap_1_0"] > 0
            and 1.0 <= last50["calibrated_pf_cap_1_0"] < 1.10,
            f"health={health} reasons={reasons} last50={last50}",
        ))

        live_rows = [
            {"ts": 1, "actual_realized_r": -0.4},
            {"ts": 2, "actual_realized_r": -0.6},
            {"ts": 3, "actual_realized_r": -0.5},
        ]
        live_health, live_reasons, live_metrics = rh.classify_live(close_rows=live_rows, decision_rows=[], min_lock_rows=[])
        results.append(_assert(
            "live 3 losses => RED",
            live_health == "RED" and live_metrics["consecutive_losses"] == 3,
            f"health={live_health} reasons={live_reasons} metrics={live_metrics}",
        ))

        live_health, live_reasons, live_metrics = rh.classify_live(close_rows=[], decision_rows=[], min_lock_rows=[])
        status = rh.promotion_status("GREEN", live_health, {"n": 50})
        results.append(_assert(
            "no live data => UNKNOWN / micro only",
            live_health == "UNKNOWN" and status == "LIVE_MICRO_ONLY",
            f"health={live_health} status={status} reasons={live_reasons} metrics={live_metrics}",
        ))

        one_win_csv = [{
            "id": "live-win-1",
            "symbol": "0GUSDT",
            "open_time": "10:01 27-06",
            "close_time": "10:29 27-06",
            "side": "SHORT",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "status": "WIN",
            "rr": "0.59",
            "signal_created_ts": "1782529242.5687084",
        }]
        csv_closes = rh.live_close_rows(decision_rows=[], live_trade_rows=one_win_csv)
        live_health, live_reasons, live_metrics = rh.classify_live(
            decision_rows=[],
            min_lock_rows=[],
            live_state=[],
            live_trade_rows=one_win_csv,
        )
        results.append(_assert(
            "live_trades.csv one WIN => live_health not UNKNOWN, loss_streak=0",
            len(csv_closes) == 1
            and csv_closes[0]["symbol"] == "0GUSDT"
            and live_health in ("GREEN", "SMALL_SAMPLE_OK")
            and live_metrics["live_closed_n"] == 1
            and live_metrics["live_loss_streak"] == 0
            and live_metrics["live_rolling_net_r"] > 0,
            f"health={live_health} reasons={live_reasons} metrics={live_metrics} closes={csv_closes}",
        ))

        duplicate_decision = [{
            "id": "live-win-1",
            "symbol": "0GUSDT",
            "side": "SHORT",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "status": "CLOSED",
            "decision": "CLOSED",
            "actual_realized_r": 0.59,
            "ts": 1782529242.5687084,
        }]
        deduped = rh.live_close_rows(decision_rows=duplicate_decision, live_trade_rows=one_win_csv)
        results.append(_assert(
            "decision log close + live_trades same trade => no double count",
            len(deduped) == 1 and deduped[0].get("_live_close_source") == "live_trades_csv",
            f"deduped={deduped}",
        ))

        status = rh.promotion_status("YELLOW", "GREEN", {"n": 50})
        results.append(_assert(
            "paper collection unaffected while YELLOW blocks only live scale",
            status == "LIVE_SCALE_BLOCKED_PAPER_HEALTH",
            f"promotion_status={status}",
        ))

        results.append(_assert(
            "normal CONFIRM unchanged by rolling audit module",
            not hasattr(rh, "_dispatch_live_smc_research_lane")
            and not hasattr(rh, "_dispatch_paper_smc_research_qualified_lane"),
            "audit module has no dispatcher hooks",
        ))

        status = rh.promotion_status("GREEN", "GREEN", {"n": 50})
        results.append(_assert(
            "live scale allowed only when paper GREEN and live not RED",
            status == "PROMOTION_ALLOWED_MICRO_ONLY",
            f"promotion_status={status}",
        ))

        status = rh.promotion_status("GREEN", "RED", {"n": 50})
        results.append(_assert(
            "live scale blocked when live health RED",
            status == "LIVE_SCALE_BLOCKED_LIVE_HEALTH",
            f"promotion_status={status}",
        ))

        legacy_values = _interleave([1.0] * 25, [-1.1] * 25)
        (legacy_health, _), _, _ = _paper_health(legacy_values)
        (active_health, active_reasons), _, _ = _active_health([])
        status = rh.promotion_status(
            active_health,
            "GREEN",
            {"n": 0},
            active_n=0,
            min_active_closed=20,
        )
        results.append(_assert(
            "old bad history but baseline after it => active INSUFFICIENT_DATA, legacy RED",
            legacy_health == "RED"
            and active_health == "INSUFFICIENT_DATA"
            and status == "LIVE_MICRO_ONLY",
            f"legacy={legacy_health} active={active_health} status={status} reasons={active_reasons}",
        ))

        (active_health, _), _, _ = _active_health(_interleave([1.0] * 10, [-0.5] * 10))
        results.append(_assert(
            "active 20 good trades => active GREEN",
            active_health == "GREEN",
            f"active={active_health}",
        ))

        (active_health, _), _, _ = _active_health([-0.4] * 20)
        results.append(_assert(
            "active 20 bad trades => active RED",
            active_health == "RED",
            f"active={active_health}",
        ))

        live_health, live_reasons, _ = rh.classify_live(
            close_rows=[],
            decision_rows=[],
            min_lock_rows=[{
                "entry_type": "CONFIRM_SMC_RESEARCH",
                "reason": "MIN_LOCK_075_LIVE_SYNC_FAILED",
                "sync_result": "False",
            }],
            live_state=[{
                "status": "OPEN",
                "entry_type": "CONFIRM_SMC_RESEARCH",
                "exchange_sl_id": "abc",
                "exchange_sl_price_confirmed": 1.23,
            }],
        )
        results.append(_assert(
            "live SL id confirmed => live_health not RED for sl sync",
            live_health != "RED" and "SL_SYNC_OK" in live_reasons,
            f"health={live_health} reasons={live_reasons}",
        ))

        now = 1000.0
        ok, reason, detail = sd._live_research_micro_pause_status(
            ctx=DummyCtx(),
            now_ts=now,
            close_rows=[],
            pause_rows=[],
            health_rows=[{"paper_health": "GREEN"}],
        )
        results.append(_assert(
            "0 losses => no pause",
            ok is True and reason == "",
            f"ok={ok} reason={reason} detail={detail}",
        ))

        ok, reason, detail = sd._live_research_micro_pause_status(
            ctx=DummyCtx(),
            now_ts=now,
            close_rows=[{"actual_realized_r": -0.5}, {"actual_realized_r": -0.4}],
            pause_rows=[],
            health_rows=[{"paper_health": "GREEN"}],
        )
        results.append(_assert(
            "2 consecutive losses => no pause",
            ok is True and detail.get("live_loss_streak") == 2,
            f"ok={ok} reason={reason} detail={detail}",
        ))

        pause_logs.clear()
        ok, reason, detail = sd._live_research_micro_pause_status(
            ctx=DummyCtx(),
            now_ts=now,
            live_trade_rows=[
                {"id": "loss-1", "symbol": "AAAUSDT", "open_time": "1", "close_time": "2", "side": "LONG", "entry_type": "CONFIRM_SMC_RESEARCH", "status": "LOSS", "rr": "-0.5"},
                {"id": "loss-2", "symbol": "BBBUSDT", "open_time": "3", "close_time": "4", "side": "LONG", "entry_type": "CONFIRM_SMC_RESEARCH", "status": "LOSS", "rr": "-0.4"},
                {"id": "loss-3", "symbol": "CCCUSDT", "open_time": "5", "close_time": "6", "side": "LONG", "entry_type": "CONFIRM_SMC_RESEARCH", "status": "LOSS", "rr": "-0.3"},
            ],
            pause_rows=[],
            health_rows=[{"paper_health": "GREEN"}],
        )
        results.append(_assert(
            "3 consecutive losses => pause 3h",
            ok is False
            and reason == "live_micro_pause"
            and detail.get("pause_reason") == "LIVE_MICRO_PAUSE_3_LOSS_STREAK"
            and round(detail.get("pause_until") - now) == 10800,
            f"ok={ok} reason={reason} detail={detail}",
        ))

        active_pause = [{
            "event_type": "LIVE_RESEARCH_MICRO_PAUSE",
            "ts": now,
            "pause_reason": "LIVE_MICRO_PAUSE_3_LOSS_STREAK",
            "pause_until": now + 3600,
        }]
        ok, reason, detail = sd._live_research_micro_pause_status(
            ctx=DummyCtx(),
            now_ts=now + 60,
            close_rows=[],
            pause_rows=active_pause,
            health_rows=[{"paper_health": "GREEN"}],
        )
        results.append(_assert(
            "during pause => PREFILTER_REJECT live_micro_pause",
            ok is False
            and reason == "live_micro_pause"
            and detail.get("pause_remaining_sec") == 3540,
            f"ok={ok} reason={reason} detail={detail}",
        ))

        expired_pause = [dict(active_pause[0], pause_until=now - 1)]
        ok, reason, detail = sd._live_research_micro_pause_status(
            ctx=DummyCtx(),
            now_ts=now,
            close_rows=[],
            pause_rows=expired_pause,
            health_rows=[{"paper_health": "GREEN"}],
        )
        results.append(_assert(
            "after 3h and paper health GREEN/YELLOW => live allowed",
            ok is True and reason == "",
            f"ok={ok} reason={reason} detail={detail}",
        ))

        ok, reason, detail = sd._live_research_micro_pause_status(
            ctx=DummyCtx(),
            now_ts=now,
            close_rows=[],
            pause_rows=expired_pause,
            health_rows=[{"paper_health": "RED"}],
        )
        results.append(_assert(
            "after 3h and paper health RED => still blocked by paper health",
            ok is False and reason == "LIVE_SCALE_BLOCKED_PAPER_HEALTH",
            f"ok={ok} reason={reason} detail={detail}",
        ))

        ok, reason, detail = sd._live_research_micro_pause_status(
            ctx=DummyCtx(),
            now_ts=now,
            close_rows=[
                {"actual_realized_r": -1.0},
                {"actual_realized_r": 0.2},
                {"actual_realized_r": -1.3},
            ],
            pause_rows=[],
            health_rows=[{"paper_health": "GREEN"}],
        )
        results.append(_assert(
            "live rolling net <= -2R => pause 3h",
            ok is False
            and reason == "live_micro_pause"
            and detail.get("pause_reason") == "LIVE_MICRO_PAUSE_ROLLING_NET"
            and round(detail.get("pause_until") - now) == 10800,
            f"ok={ok} reason={reason} detail={detail}",
        ))

        ctx = DummyCtx(trades=[{"status": "OPEN", "entry_type": "CONFIRM_SMC_RESEARCH", "owner": "bot"}])
        before = json.dumps(ctx.trades, sort_keys=True)
        sd._live_research_micro_pause_status(
            ctx=ctx,
            now_ts=now,
            close_rows=[],
            pause_rows=[],
            health_rows=[{"paper_health": "GREEN"}],
        )
        after = json.dumps(ctx.trades, sort_keys=True)
        results.append(_assert(
            "existing live position management state unaffected",
            before == after,
            f"before={before} after={after}",
        ))
    finally:
        sd._live_research_micro_write = original_pause_write
        sd.config.clear()
        sd.config.update(original_config)

    print("\nRESULT:", "PASS" if all(results) else "FAIL")
    print("sample_last50=" + json.dumps(last50, sort_keys=True, default=str))
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
