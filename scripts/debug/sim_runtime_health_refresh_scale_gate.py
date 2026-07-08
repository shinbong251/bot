#!/usr/bin/env python3
"""Simulator for runtime rolling-health refresh and live scale gate behavior."""

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

import main as bot_main
import signal_dispatcher as sd
from scripts.debug import audit_research_rolling_health as rh


@dataclass
class DummyCtx:
    execution_mode: str = "live"
    trades: list = field(default_factory=list)


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<82} {status} {detail}")
    return bool(condition)


def _health(ts, paper="GREEN", live="GREEN", reasons=None, metrics=None, source="sim"):
    return {
        "ts": ts,
        "source": source,
        "paper_health": paper,
        "paper_active_health": paper,
        "live_health": live,
        "live_metrics": dict(metrics or {
            "live_loss_streak": 0,
            "live_rolling_net_r": 0.0,
            "live_unconfirmed_rr_n": 0,
        }),
        "reasons": list(reasons or []),
        "promotion_status": "PROMOTION_ALLOWED_MICRO_ONLY",
    }


def _call_gate(now_ts, health_rows):
    return sd._live_research_micro_pause_status(
        ctx=DummyCtx(),
        now_ts=now_ts,
        close_rows=[],
        pause_rows=[],
        health_rows=health_rows,
    )


def _read_jsonl(path):
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def main():
    results = []
    original_sd_config = dict(sd.config)
    original_summary_log = rh.SUMMARY_LOG
    original_build_summary = rh.build_summary
    original_write_runtime_error = bot_main.write_runtime_error
    original_refresh = bot_main._refresh_runtime_research_health
    runtime_errors = []

    bot_main.write_runtime_error = lambda where, detail: runtime_errors.append((where, detail))
    sd.config["live_research_micro_pause_enabled"] = True
    sd.config["live_research_loss_streak_pause_count"] = 3
    sd.config["live_research_rolling_net_pause_r"] = -2.0

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary_log = Path(tmpdir) / "research_rolling_health.jsonl"
            rh.SUMMARY_LOG = summary_log

            sd.config["max_live_research_trades"] = 2
            sd.config["live_risk_per_trade"] = 0.01
            sd.config["live_max_portfolio_risk"] = 0.02

            ok, reason, detail = _call_gate(
                2000.0,
                [_health(1000.0, paper="GREEN", live="GREEN")],
            )
            results.append(_assert(
                "A. cap=2 + stale health row + no refresh => stale scale block",
                ok is False
                and reason == "LIVE_SCALE_BLOCKED_STALE_HEALTH"
                and detail.get("action") == "BLOCK_SCALE",
                f"reason={reason} detail={detail}",
            ))

            def fake_green_build_summary(source="audit_script", write_summary=True):
                row = _health(2000.0, paper="GREEN", live="GREEN", source=source)
                if write_summary:
                    rh.write_jsonl(rh.SUMMARY_LOG, row)
                return row

            rh.build_summary = fake_green_build_summary
            refresh_ok, refresh_row = bot_main._refresh_runtime_research_health(now=2000.0)
            rows = _read_jsonl(summary_log)
            ok, reason, detail = _call_gate(2001.0, rows)
            results.append(_assert(
                "B. cap=2 + runtime refresh fresh acceptable row => no stale-health block",
                refresh_ok is True
                and refresh_row.get("source") == "runtime_health_refresh"
                and len(rows) == 1
                and ok is True
                and reason == "",
                f"refresh={refresh_row} gate_reason={reason} detail={detail}",
            ))

            ok, reason, detail = _call_gate(
                2002.0,
                [_health(2002.0, paper="GREEN", live="RED")],
            )
            results.append(_assert(
                "C. cap=2 + fresh RED live health => scale live-health block",
                ok is False
                and reason == "LIVE_SCALE_BLOCKED_LIVE_HEALTH"
                and detail.get("action") == "BLOCK_SCALE",
                f"reason={reason} detail={detail}",
            ))

            sd.config["max_live_research_trades"] = 1
            sd.config["live_risk_per_trade"] = 0.005
            sd.config["live_max_portfolio_risk"] = 0.005
            ok, reason, detail = _call_gate(
                3000.0,
                [_health(1000.0, paper="GREEN", live="RED")],
            )
            results.append(_assert(
                "D. cap=1 + stale health row => warn/allow if no current safety issue",
                ok is True
                and reason == ""
                and detail.get("action") == "WARN_ALLOW"
                and detail.get("pause_reason") == "LIVE_HEALTH_ROW_STALE_WARN",
                f"reason={reason} detail={detail}",
            ))

            sd.config["max_live_research_trades"] = 2
            sd.config["live_risk_per_trade"] = 0.01
            sd.config["live_max_portfolio_risk"] = 0.02

            def raising_build_summary(source="audit_script", write_summary=True):
                raise RuntimeError("simulated health refresh failure")

            rh.build_summary = raising_build_summary
            last_ts, did_refresh, row = bot_main._maybe_refresh_runtime_research_health(
                [DummyCtx()],
                0,
                now=4000.0,
            )
            ok, reason, detail = _call_gate(
                4001.0,
                [_health(1000.0, paper="GREEN", live="GREEN")],
            )
            results.append(_assert(
                "E. refresh exception => no crash, error logged, stale scale blocks safely",
                last_ts == 4000.0
                and did_refresh is False
                and row.get("source") == "runtime_health_refresh_error"
                and runtime_errors
                and ok is False
                and reason == "LIVE_SCALE_BLOCKED_STALE_HEALTH",
                f"last={last_ts} refreshed={did_refresh} errors={runtime_errors} reason={reason} detail={detail}",
            ))

            append_rows = []

            def fake_refresh(now=None):
                row = _health(now or 0, source="runtime_health_refresh")
                append_rows.append(row)
                return True, row

            bot_main._refresh_runtime_research_health = fake_refresh
            last_ts, did_refresh, _ = bot_main._maybe_refresh_runtime_research_health(
                [DummyCtx()],
                0,
                now=5000.0,
            )
            last_ts, did_refresh_early, _ = bot_main._maybe_refresh_runtime_research_health(
                [DummyCtx()],
                last_ts,
                now=5000.0 + bot_main._ROLLING_HEALTH_REFRESH_INTERVAL - 1,
            )
            last_ts, did_refresh_due, _ = bot_main._maybe_refresh_runtime_research_health(
                [DummyCtx()],
                last_ts,
                now=5000.0 + bot_main._ROLLING_HEALTH_REFRESH_INTERVAL,
            )
            results.append(_assert(
                "F. refresh cadence => no append before 5 minutes, append when due",
                len(append_rows) == 2
                and did_refresh_early is False
                and did_refresh_due is True,
                f"append_rows={len(append_rows)} early={did_refresh_early} due={did_refresh_due}",
            ))

    finally:
        sd.config.clear()
        sd.config.update(original_sd_config)
        rh.SUMMARY_LOG = original_summary_log
        rh.build_summary = original_build_summary
        bot_main.write_runtime_error = original_write_runtime_error
        bot_main._refresh_runtime_research_health = original_refresh

    overall = all(results)
    print("\nRESULT:", "PASS" if overall else "FAIL")
    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
