#!/usr/bin/env python3
"""Simulate OFF-by-default stale live loss-streak dormancy demotion."""

import json
import os
import sys
import tempfile
import types
from dataclasses import dataclass, field
from pathlib import Path


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


np_stub = types.ModuleType("numpy")
np_stub.bool_ = bool
np_stub.integer = int
np_stub.floating = float
np_stub.ndarray = type("ndarray", (), {})
sys.modules.setdefault("numpy", np_stub)
sys.modules.setdefault("pandas", types.ModuleType("pandas"))

import signal_dispatcher as sd
from scripts.debug import audit_research_rolling_health as rh


NOW = 2_000_000.0


@dataclass
class DummyCtx:
    execution_mode: str = "live"
    trades: list = field(default_factory=list)


def _key(*parts):
    return "".join(parts)


def _close(realized_r, ts, *, unconfirmed=False, trade_id=None):
    row = {
        "id": trade_id or f"c{int(ts)}",
        "symbol": "TESTUSDT",
        "side": "LONG",
        "actual_realized_r": realized_r,
        "_sort_ts": ts,
    }
    if unconfirmed:
        row["rr_unconfirmed"] = True
    return row


def _stale_tail():
    age = 7 * 24 * 3600
    return [
        _close(3.0, NOW - age - 300.0, trade_id="w1"),
        _close(3.0, NOW - age - 200.0, trade_id="w2"),
        _close(-0.5, NOW - age - 30.0, trade_id="l1"),
        _close(-0.4, NOW - age - 20.0, trade_id="l2"),
        _close(-0.3, NOW - age - 10.0, trade_id="l3"),
        _close(0.1, NOW - 3600.0, unconfirmed=True, trade_id="newer_be"),
    ]


def _recent_tail():
    return [
        _close(3.0, NOW - 5000.0, trade_id="w1"),
        _close(3.0, NOW - 4000.0, trade_id="w2"),
        _close(-0.5, NOW - 3600.0, trade_id="l1"),
        _close(-0.4, NOW - 1800.0, trade_id="l2"),
        _close(-0.3, NOW - 900.0, trade_id="l3"),
        _close(0.1, NOW - 300.0, unconfirmed=True, trade_id="newer_be"),
    ]


def _current_stale_tail():
    age = 7 * 24 * 3600
    return [
        _close(3.0, NOW - age - 300.0, trade_id="w1"),
        _close(3.0, NOW - age - 200.0, trade_id="w2"),
        _close(-0.5, NOW - age - 30.0, trade_id="l1"),
        _close(-0.4, NOW - age - 20.0, trade_id="l2"),
        _close(-0.3, NOW - age - 10.0, trade_id="l3"),
    ]


def _classify(rows, enabled):
    with tempfile.TemporaryDirectory() as tmp:
        cfg_path = Path(tmp) / "config.json"
        cfg_path.write_text(json.dumps({
            "live_health_stale_streak_demote_enabled": enabled,
            "live_health_stale_streak_dormancy_hours": 48,
        }), encoding="utf-8")
        old_config_json = rh.CONFIG_JSON
        old_time = rh.time.time
        rh.CONFIG_JSON = cfg_path
        rh.time.time = lambda: NOW
        try:
            return rh.classify_live(
                close_rows=rows,
                decision_rows=[],
                min_lock_rows=[],
                live_state={"trades": []},
                live_trade_rows=[],
            )
        finally:
            rh.CONFIG_JSON = old_config_json
            rh.time.time = old_time


def _health_row(health, reasons, metrics, paper="GREEN", ts=NOW):
    return {
        "ts": ts,
        "source": "sim",
        "paper_health": paper,
        "paper_active_health": paper,
        "live_health": health,
        "reasons": list(reasons),
        "live_metrics": dict(metrics),
        "promotion_status": "PROMOTION_ALLOWED_MICRO_ONLY",
    }


def _gate(health_row, *, cap, risk, port, close_rows=None, pause_rows=None):
    old_config = dict(sd.config)
    old_write = sd._live_research_micro_write
    sd._live_research_micro_write = lambda payload: None
    sd.config["live_research_micro_pause_enabled"] = True
    sd.config["live_research_loss_streak_pause_count"] = 3
    sd.config["live_research_micro_pause_hours"] = 3
    sd.config[_key("max_live_", "research_trades")] = cap
    sd.config[_key("live_", "risk_per_trade")] = risk
    sd.config[_key("live_max_", "portfolio_risk")] = port
    try:
        return sd._live_research_micro_pause_status(
            ctx=DummyCtx(),
            now_ts=NOW,
            close_rows=close_rows if close_rows is not None else [],
            pause_rows=pause_rows or [],
            health_rows=[health_row],
        )
    finally:
        sd.config.clear()
        sd.config.update(old_config)
        sd._live_research_micro_write = old_write


def _same_metrics(left, right):
    return left == right


def _check(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<82} {status} {detail}")
    return bool(condition)


def run():
    results = []

    off_health, off_reasons, off_metrics = _classify(_stale_tail(), False)
    results.append(_check(
        "1. flag OFF + dormant stale 3-loss tail stays RED",
        off_health == "RED"
        and off_reasons == ["live_consecutive_losses=3>=3"]
        and off_metrics.get("loss_streak_current") is False,
        f"health={off_health} reasons={off_reasons}",
    ))

    on_health, on_reasons, on_metrics = _classify(_stale_tail(), True)
    results.append(_check(
        "2. flag ON + dormancy exceeded demotes to YELLOW",
        on_health == "YELLOW"
        and any("stale_streak_demoted_dormancy_hours" in r for r in on_reasons)
        and _same_metrics(off_metrics, on_metrics),
        f"health={on_health} reasons={on_reasons}",
    ))

    recent_health, recent_reasons, recent_metrics = _classify(_recent_tail(), True)
    results.append(_check(
        "3. flag ON + recent stale 3-loss tail stays RED",
        recent_health == "RED"
        and recent_reasons == ["live_consecutive_losses=3>=3"]
        and recent_metrics.get("loss_streak_current") is False,
        f"health={recent_health} reasons={recent_reasons}",
    ))

    current_health, current_reasons, current_metrics = _classify(_current_stale_tail(), True)
    results.append(_check(
        "4. flag ON but loss_streak_current True stays RED",
        current_health == "RED"
        and current_reasons == ["live_consecutive_losses=3>=3"]
        and current_metrics.get("loss_streak_current") is True,
        f"health={current_health} current={current_metrics.get('loss_streak_current')}",
    ))

    current_scale_ok, current_scale_reason, current_scale_detail = _gate(
        _health_row(on_health, on_reasons, on_metrics, paper="GREEN"),
        cap=2,
        risk=0.01,
        port=0.02,
        close_rows=_current_stale_tail(),
    )
    prior_pause = [{
        "event_type": "LIVE_RESEARCH_MICRO_PAUSE",
        "ts": NOW - 10.0,
        "pause_reason": "LIVE_MICRO_PAUSE_3_LOSS_STREAK",
        "pause_until": NOW - 1.0,
        "live_loss_streak": current_scale_detail.get("live_loss_streak"),
        "live_rolling_net_r": current_scale_detail.get("live_rolling_net_r"),
        "live_closed_count": current_scale_detail.get("live_closed_count"),
        "last_live_close_key": current_scale_detail.get("last_live_close_key"),
        "pause_armed_live_closed_count": current_scale_detail.get("live_closed_count"),
        "pause_armed_last_live_close_key": current_scale_detail.get("last_live_close_key"),
    }]
    probe_ok, probe_reason, probe_detail = _gate(
        _health_row(on_health, on_reasons, on_metrics, paper="GREEN"),
        cap=1,
        risk=0.005,
        port=0.005,
        close_rows=_current_stale_tail(),
        pause_rows=prior_pause,
    )
    results.append(_check(
        "5. gate interaction documented: scale still blocks; micro probe can allow after old pause",
        current_scale_ok is False
        and current_scale_reason == "LIVE_MICRO_BLOCKED_LOSS_STREAK"
        and current_scale_detail.get("action") == "SET_AND_BLOCK"
        and probe_ok is True
        and probe_reason == ""
        and probe_detail.get("action") == "ALLOW_AFTER_PAUSE_EXPIRY_NO_NEW_CLOSE",
        (
            f"scale=({current_scale_ok},{current_scale_reason},{current_scale_detail.get('action')}) "
            f"probe=({probe_ok},{probe_reason},{probe_detail.get('action')})"
        ),
    ))

    passed = all(results)
    print("PASS live stale streak dormancy demotion sim" if passed else "FAIL live stale streak dormancy demotion sim")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(run())
