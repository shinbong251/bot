#!/usr/bin/env python3
"""Simulator for stale LIVE SL sync health blocking."""

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


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<86} {status} {detail}")
    return bool(condition)


def _trade(**overrides):
    base = {
        "status": "OPEN",
        "owner": "bot",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "symbol": "ADAUSDT",
        "side": "LONG",
        "trade_id": "t-ada",
        "research_join_key": "ada-key",
    }
    base.update(overrides)
    return base


def _min_lock(reason, sync_result=None, idx=1, **overrides):
    row = {
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "symbol": "ADAUSDT",
        "side": "LONG",
        "trade_id": "t-ada",
        "research_join_key": "ada-key",
        "reason": reason,
        "ts": float(idx),
    }
    if sync_result is not None:
        row["sync_result"] = sync_result
    row.update(overrides)
    return row


def _health_for(state, min_lock_rows=None, decision_rows=None):
    live_health, live_reasons, live_metrics = rh.classify_live(
        close_rows=[],
        decision_rows=decision_rows or [],
        min_lock_rows=min_lock_rows or [],
        live_state=state,
    )
    return {
        "ts": 1000.0,
        "paper_health": "GREEN",
        "live_health": live_health,
        "reasons": live_reasons,
        "live_metrics": live_metrics,
    }


def _micro_status(health_row):
    return sd._live_research_micro_pause_status(
        ctx=DummyCtx(),
        now_ts=1000.0,
        close_rows=[],
        pause_rows=[],
        health_rows=[health_row],
    )


def main():
    results = []
    original_config = dict(sd.config)
    original_pause_write = sd._live_research_micro_write
    pause_logs = []

    def fake_pause_write(row):
        pause_logs.append(dict(row))

    sd._live_research_micro_write = fake_pause_write
    sd.config["live_research_micro_pause_enabled"] = True
    sd.config["max_live_research_trades"] = 1
    sd.config["live_risk_per_trade"] = 0.005
    sd.config["live_max_portfolio_risk"] = 0.005

    try:
        failed = _min_lock("MIN_LOCK_075_LIVE_SYNC_FAILED", "False", idx=1)
        ok = _min_lock("MIN_LOCK_075_LIVE_SYNC_OK", "True", idx=2)
        health = _health_for([], [failed, ok])
        allow, reason, detail = _micro_status(health)
        results.append(_assert(
            "A. flat + historical FAILED followed by SYNC_OK => WARN/ALLOW",
            health["live_health"] != "RED"
            and "live_sl_sync_failure" not in health["reasons"]
            and "historical_sl_sync_failure_resolved_or_flat_warning" in health["reasons"]
            and allow is True
            and reason == "",
            f"health={health} detail={detail}",
        ))

        health = _health_for([], [failed])
        allow, reason, detail = _micro_status(health)
        results.append(_assert(
            "B. flat + historical FAILED only => WARN/ALLOW, no hard block",
            health["live_health"] != "RED"
            and "live_sl_sync_failure" not in health["reasons"]
            and "historical_sl_sync_failure_resolved_or_flat_warning" in health["reasons"]
            and allow is True
            and reason == "",
            f"health={health} detail={detail}",
        ))

        health = _health_for([_trade()])
        allow, reason, detail = _micro_status(health)
        results.append(_assert(
            "C. open position missing exchange_sl_id => critical + micro BLOCK",
            health["live_health"] == "RED"
            and "live_sl_sync_failure" in health["reasons"]
            and allow is False
            and reason == "LIVE_MICRO_BLOCKED_SL_SYNC",
            f"health={health} detail={detail}",
        ))

        health = _health_for([_trade(exchange_sl_id="sl-1", exchange_sl_price_confirmed=0.62)])
        allow, reason, detail = _micro_status(health)
        results.append(_assert(
            "D. open position with confirmed exchange SL => no critical SL failure",
            health["live_health"] != "RED"
            and "live_sl_sync_failure" not in health["reasons"]
            and "SL_SYNC_OK" in health["reasons"]
            and allow is True
            and reason == "",
            f"health={health} detail={detail}",
        ))

        health = _health_for(
            [_trade(exchange_sl_id="sl-1", exchange_sl_price_confirmed=0.62)],
            [failed],
        )
        allow, reason, detail = _micro_status(health)
        results.append(_assert(
            "E. confirmed SL + MIN_LOCK tighten FAILED => WARN/ALLOW",
            health["live_health"] != "RED"
            and "historical_min_lock_sync_failure_unresolved_warning" in health["reasons"]
            and "live_sl_sync_failure" not in health["reasons"]
            and allow is True
            and reason == "",
            f"health={health} detail={detail}",
        ))

        self_ref = [{
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "decision": "PREFILTER_REJECT",
            "reason": "LIVE_MICRO_BLOCKED_SL_SYNC",
        }]
        health = _health_for([], [], self_ref)
        allow, reason, detail = _micro_status(health)
        results.append(_assert(
            "F. decision self-reference LIVE_MICRO_BLOCKED_SL_SYNC => no critical feedback",
            health["live_health"] != "RED"
            and "live_sl_sync_failure" not in health["reasons"]
            and "historical_sl_sync_failure_resolved_or_flat_warning" not in health["reasons"]
            and allow is True
            and reason == "",
            f"health={health} detail={detail}",
        ))

        ada_failed = _min_lock("MIN_LOCK_075_LIVE_SYNC_FAILED", "False", idx=10)
        ada_ok = _min_lock("MIN_LOCK_075_LIVE_SYNC_OK", "True", idx=11)
        health = _health_for([], [ada_failed, ada_ok])
        allow, reason, detail = _micro_status(health)
        results.append(_assert(
            "G. latest ADA SYNC_FAILED then SYNC_OK same trade => resolved WARN/ALLOW",
            health["live_health"] != "RED"
            and "historical_min_lock_sync_failure_resolved" in health["reasons"]
            and "live_sl_sync_failure" not in health["reasons"]
            and allow is True
            and reason == "",
            f"health={health} detail={detail}",
        ))
    finally:
        sd._live_research_micro_write = original_pause_write
        sd.config.clear()
        sd.config.update(original_config)

    print("\nRESULT:", "PASS" if all(results) else "FAIL")
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
