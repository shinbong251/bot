#!/usr/bin/env python3
"""Simulator for LIVE CONFIRM_SMC_RESEARCH min-notional prefilter."""

import os
import sys
import types
from dataclasses import dataclass, field
from decimal import Decimal


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
from exchange import live_executor
from exchange import precision
import entry
import execution


@dataclass
class DummyCtx:
    execution_mode: str = "live"
    trades: list = field(default_factory=list)


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<88} {status} {detail}")
    return bool(condition)


def _trade(symbol="ACTUSDT", entry=100.0, sl_pct=0.0726, score=6.0):
    sl = entry * (1.0 - sl_pct)
    return {
        "symbol": symbol,
        "side": "LONG",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "strategy_family": "confirm_smc_research",
        "entry": entry,
        "sl": sl,
        "tp": entry * 1.2,
        "rr": 2.75,
        "score": score,
        "bos_quality": "GOOD",
        "volume_confirmation": "NORMAL",
        "research_dedup_key": f"{symbol}-key",
    }


def _candidate(symbol="ACTUSDT", entry=100.0, sl_pct=0.0726, score=6.0):
    t = _trade(symbol=symbol, entry=entry, sl_pct=sl_pct, score=score)
    return {
        "symbol": t["symbol"],
        "side": t["side"],
        "entry": t["entry"],
        "sl": t["sl"],
        "tp": t["tp"],
        "rr": t["rr"],
        "score": t["score"],
        "dedup_key": t["research_dedup_key"],
        "structural_context": {
            "score_v2_structural_shadow": t["score"],
            "score_v2_current": t["score"],
            "bos_quality": "GOOD",
            "volume_confirmation": "NORMAL",
        },
    }


def _filters(min_notional=5.0):
    return {
        "symbol": "SIMUSDT",
        "min_notional": Decimal(str(min_notional)),
        "step_size": Decimal("0.001"),
        "tick_size": Decimal("0.0001"),
    }


def main():
    results = []
    original_config = dict(sd.config)
    original_get_balance = live_executor.get_execution_balance
    original_get_filters = precision.get_symbol_filters
    original_micro = sd._live_research_micro_pause_status
    original_gate = live_executor.check_live_research_safety_gate
    original_candidates = entry.get_confirm_structural_outcome_candidates_snapshot
    original_open_trade = execution.open_trade
    original_log = sd._live_smc_research_log
    original_terminal = dict(sd._live_smc_research_terminal_failures)
    logs = []
    opened = []

    def fake_get_filters(symbol):
        return _filters(5.0)

    def fake_micro(*args, **kwargs):
        return True, "", {"action": "ALLOW"}

    def fake_gate(*args, **kwargs):
        return True, "OK"

    def fake_open_trade(trade, ctx=None):
        opened.append(dict(trade))
        return True

    def fake_log(candidate, decision, reason="", trade=None, extra=None):
        logs.append({
            "candidate": dict(candidate or {}),
            "decision": decision,
            "reason": reason,
            "trade": dict(trade or {}),
            "extra": dict(extra or {}) if isinstance(extra, dict) else extra,
        })

    sd.config["live_smc_research_enabled"] = True
    sd.config["live_risk_per_trade"] = 0.005
    sd.config["min_notional_floor_allowed"] = False
    live_executor.get_execution_balance = lambda *args, **kwargs: 50.0
    precision.get_symbol_filters = fake_get_filters
    sd._live_research_micro_pause_status = fake_micro
    live_executor.check_live_research_safety_gate = fake_gate
    execution.open_trade = fake_open_trade
    sd._live_smc_research_log = fake_log
    sd._live_smc_research_terminal_failures.clear()

    try:
        ok, stage, reason, detail = sd._live_smc_research_prefilter(
            _candidate("ACTUSDT", sl_pct=0.0726),
            _trade("ACTUSDT", sl_pct=0.0726),
            DummyCtx(),
        )
        results.append(_assert(
            "A. strict mode + sl_pct=7.26%, projected_notional<$5 => PREFILTER_REJECT",
            ok is False
            and stage == "live_min_notional"
            and reason == "LIVE_MIN_NOTIONAL_PREFILTER"
            and detail.get("projected_notional") < detail.get("min_notional")
            and round(detail.get("effective_risk_pct"), 6) == 0.0025
            and detail.get("min_notional_floor_allowed") is False,
            f"stage={stage} reason={reason} detail={detail}",
        ))

        ok, stage, reason, detail = sd._live_smc_research_prefilter(
            _candidate("PASSUSDT", sl_pct=0.02),
            _trade("PASSUSDT", sl_pct=0.02),
            DummyCtx(),
        )
        results.append(_assert(
            "B. strict mode + sl_pct=2.0%, projected_notional>=$5 => passes prefilter",
            ok is True and stage == "ok" and reason == "ok",
            f"ok={ok} stage={stage} reason={reason} detail={detail}",
        ))

        sd.config["min_notional_floor_allowed"] = True
        ok, stage, reason, detail = sd._live_smc_research_prefilter(
            _candidate("FLOORUSDT", sl_pct=0.0337),
            _trade("FLOORUSDT", sl_pct=0.0337),
            DummyCtx(),
        )
        results.append(_assert(
            "C. floor allowed but execution lacks floor sizing => explicit unsupported reject",
            ok is False
            and reason == "LIVE_MIN_NOTIONAL_PREFILTER"
            and detail.get("min_notional_floor_allowed") is True
            and detail.get("would_floor_violate_cap") is False
            and detail.get("min_notional_floor_unsupported") is True,
            f"detail={detail}",
        ))

        ok, stage, reason, detail = sd._live_smc_research_prefilter(
            _candidate("RAVEUSDT", sl_pct=0.0726),
            _trade("RAVEUSDT", sl_pct=0.0726),
            DummyCtx(),
        )
        results.append(_assert(
            "D. floor mode + sl_pct=7.26%, floor risk >0.5% => reject",
            ok is False
            and reason == "LIVE_MIN_NOTIONAL_PREFILTER"
            and detail.get("would_floor_violate_cap") is True,
            f"detail={detail}",
        ))

        bad = _trade("BADUSDT", sl_pct=0.0)
        bad["sl"] = bad["entry"]
        ok, stage, reason, detail = sd._live_smc_research_prefilter(
            _candidate("BADUSDT", sl_pct=0.0),
            bad,
            DummyCtx(),
        )
        results.append(_assert(
            "E. malformed zero SL distance => existing geometry reject",
            ok is False and stage == "geometry" and reason == "invalid_geometry",
            f"stage={stage} reason={reason} detail={detail}",
        ))

        precision.get_symbol_filters = lambda symbol: None
        ok, stage, reason, detail = sd._live_smc_research_prefilter(
            _candidate("MISSUSDT", sl_pct=0.02),
            _trade("MISSUSDT", sl_pct=0.02),
            DummyCtx(),
        )
        results.append(_assert(
            "F. missing min_notional filters => fail safe before OPEN_ATTEMPT",
            ok is False
            and stage == "live_min_notional"
            and reason == "LIVE_MIN_NOTIONAL_PREFILTER"
            and detail.get("min_notional_prefilter_reason") == "min_notional_lookup_failed",
            f"detail={detail}",
        ))

        precision.get_symbol_filters = fake_get_filters
        sd.config["min_notional_floor_allowed"] = False
        logs.clear()
        opened.clear()
        sd._live_smc_research_terminal_failures.clear()
        entry.get_confirm_structural_outcome_candidates_snapshot = lambda: [
            _candidate("ACTUSDT", sl_pct=0.0726),
            _candidate("PASSUSDT", sl_pct=0.02),
        ]
        sd._dispatch_live_smc_research_lane(DummyCtx())
        open_attempts = [row for row in logs if row.get("decision") == "OPEN_ATTEMPT"]
        rejects = [
            row for row in logs
            if row.get("decision") == "PREFILTER_REJECT"
            and row.get("reason") == "LIVE_MIN_NOTIONAL_PREFILTER"
        ]
        results.append(_assert(
            "G. invalid candidate rejected before OPEN_ATTEMPT; valid candidate still attempts open",
            len(open_attempts) == 1
            and open_attempts[0]["candidate"].get("symbol") == "PASSUSDT"
            and len(rejects) == 1
            and rejects[0]["candidate"].get("symbol") == "ACTUSDT"
            and len(opened) == 1
            and opened[0].get("symbol") == "PASSUSDT",
            f"logs={logs} opened={opened}",
        ))
    finally:
        sd.config.clear()
        sd.config.update(original_config)
        live_executor.get_execution_balance = original_get_balance
        precision.get_symbol_filters = original_get_filters
        sd._live_research_micro_pause_status = original_micro
        live_executor.check_live_research_safety_gate = original_gate
        entry.get_confirm_structural_outcome_candidates_snapshot = original_candidates
        execution.open_trade = original_open_trade
        sd._live_smc_research_log = original_log
        sd._live_smc_research_terminal_failures.clear()
        sd._live_smc_research_terminal_failures.update(original_terminal)

    print("\nRESULT:", "PASS" if all(results) else "FAIL")
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
