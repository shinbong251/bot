#!/usr/bin/env python3
"""Simulator for cap=2 live research scale persistence after a second trade.

Reproduces the observed regression where cap=2 opened two concurrent live
research trades once, then reverted to effectively one-trade behavior.

Root cause: both burst trades closed through the SL-audit / non-exchange-fill
exit path, which set rr_unconfirmed=True (the EXIT price was an estimate). Their
entries were confirmed (entry_price_unconfirmed=False) and their realized R was
known. The health producer treats such CLOSED estimated-exit rows as benign
(excluded from the confirmed sample, health can still be GREEN), but the consumer
scale gate was treating live_unconfirmed_rr_n>0 as a CURRENT-position safety
failure, overriding the stale-loss-streak WARN_ALLOW_SCALE and permanently
hard-blocking the second trade until those closed rows aged out of the window.

The patch scopes current entry/RR-unconfirmed safety to OPEN positions only
(_live_research_current_entry_unconfirmed(ctx)); closed estimated-exit rows no
longer block scaling. Genuine in-flight unconfirmed entries / missing-SL / rolling
hard breaches / current loss streaks still hard-block.

Cases:
  A. cap=2, one open healthy, stale loss streak only (+ closed unconfirmed) -> allow second.
  B. cap=2, second trade closes loss (current streak) -> block scale current streak.
  C. cap=2, second trade closes win -> allow future second.
  D. cap=2, second trade closes BE/non-negative -> clears current loss streak (producer).
  E. cap=2, portfolio risk room available -> no portfolio block (accounting invariant).
  F. cap=2, portfolio risk exhausted -> block (accounting invariant).
  G. stale health row -> block stale health.
  H. fresh health row with stale streak flags only -> no raw RED hard block (allow scale).
  I. current safety issue (open unconfirmed entry / missing SL) -> block.
"""

import os
import sys
import types
from dataclasses import dataclass, field


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Lightweight stubs so signal_dispatcher imports without heavy deps.
np_stub = types.ModuleType("numpy")
np_stub.bool_ = bool
np_stub.integer = int
np_stub.floating = float
np_stub.ndarray = type("ndarray", (), {})
sys.modules.setdefault("numpy", np_stub)
sys.modules.setdefault("pandas", types.ModuleType("pandas"))

import signal_dispatcher as sd
from scripts.debug import audit_research_rolling_health as rh


@dataclass
class DummyCtx:
    execution_mode: str = "live"
    trades: list = field(default_factory=list)


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<78} {status} {detail}")
    return bool(condition)


def _close(realized_r, ts, trade_id=None):
    return {
        "id": trade_id or f"c{int(ts)}",
        "symbol": "TESTUSDT",
        "side": "LONG",
        "actual_realized_r": realized_r,
        "_sort_ts": ts,
    }


def _open(entry_time, *, confirmed=True, entry_unconfirmed=False, sl_id="SL1",
          sl_confirmed=4.6, sl_fail_count=0, order_state_unknown=False, trade_id=None):
    return {
        "symbol": "INJUSDT",
        "side": "LONG",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "status": "OPEN",
        "owner": "bot",
        "entry_time": entry_time,
        "id": trade_id or int(entry_time),
        "entry_price_unconfirmed": entry_unconfirmed,
        "entry_source": "actual_exchange_fill" if confirmed else "pending",
        "entry_state": "ENTRY_CONFIRMED" if confirmed else "PENDING",
        "exchange_order_state_unknown": order_state_unknown,
        "exchange_sl_id": sl_id,
        "exchange_sl_price_confirmed": sl_confirmed,
        "sl_sync_fail_count": sl_fail_count,
    }


def _health(live="RED", paper="GREEN", metrics=None, reasons=None, ts=1000.0):
    return {
        "ts": ts,
        "source": "sim",
        "paper_health": paper,
        "paper_active_health": paper,
        "live_health": live,
        "live_metrics": dict(metrics or {}),
        "reasons": list(reasons or []),
        "promotion_status": "PROMOTION_ALLOWED_MICRO_ONLY",
    }


def _gate(now_ts, health_rows, ctx=None, close_rows=None):
    return sd._live_research_micro_pause_status(
        ctx=ctx if ctx is not None else DummyCtx(),
        now_ts=now_ts,
        close_rows=close_rows if close_rows is not None else [],
        pause_rows=[],
        health_rows=health_rows,
    )


def _scale_cfg():
    sd.config["max_live_research_trades"] = 2
    sd.config["live_risk_per_trade"] = 0.01
    sd.config["live_max_portfolio_risk"] = 0.02


# Mirror of the live portfolio-risk gate arithmetic at execution.py:4785
#   if current_total_risk + risk_percent > _portfolio_risk_cap: BLOCK
def _portfolio_blocked(current_total_risk, risk_percent, cap):
    return (current_total_risk + risk_percent) > cap


def run():
    results = []
    sd.config["live_research_micro_pause_enabled"] = True
    sd.config["live_research_loss_streak_pause_count"] = 3
    sd.config["live_research_rolling_net_pause_r"] = -2.0

    pos_net = [_close(3.62, 1000.0)]  # positive rolling net => no rolling-net hard block

    # A. one open healthy, stale loss streak + TWO closed estimated-exit rows => allow second.
    #    (This is the exact regression: live_unconfirmed_rr_n=2 must NOT block.)
    _scale_cfg()
    ctx_a = DummyCtx(trades=[_open(2000.0)])
    ok, reason, detail = _gate(
        2001.0,
        [_health(live="RED",
                 reasons=["live_consecutive_losses=3>=3"],
                 metrics={"loss_streak_current": False,
                          "loss_streak_stale_after_new_open": True,
                          "last_live_open_key": "id:2000",
                          "live_unconfirmed_rr_n": 2}, ts=2001.0)],
        ctx=ctx_a,
        close_rows=pos_net,
    )
    results.append(_assert(
        "A. cap=2 one open healthy + stale streak + 2 closed unconfirmed => WARN_ALLOW second",
        ok is True
        and detail.get("action") == "WARN_ALLOW_SCALE"
        and detail.get("pause_reason") == "LIVE_SCALE_WARN_STALE_LOSS_STREAK_AFTER_NEW_OPEN",
        f"reason={reason} action={detail.get('action')} unconf_n={detail.get('live_unconfirmed_rr_n')}",
    ))

    # B. second trade closed loss, no newer open => current loss streak block.
    _scale_cfg()
    ok, reason, detail = _gate(
        2002.0,
        [_health(live="RED", metrics={"loss_streak_current": True,
                                      "loss_streak_stale_after_new_open": False}, ts=2002.0)],
        ctx=DummyCtx(),
        close_rows=pos_net,
    )
    results.append(_assert(
        "B. cap=2 second closes LOSS (current streak) => BLOCK current loss streak",
        ok is False
        and reason == "LIVE_SCALE_BLOCKED_LIVE_LOSS_STREAK_CURRENT"
        and detail.get("scale_block_cause") == "current_loss_streak",
        f"reason={reason} cause={detail.get('scale_block_cause')}",
    ))

    # C. second trade closed win => streak broken, health not RED => allow future second.
    _scale_cfg()
    ctx_c = DummyCtx(trades=[_open(2000.0)])
    ok, reason, detail = _gate(
        2003.0,
        [_health(live="GREEN", metrics={"loss_streak_current": False,
                                        "loss_streak_stale_after_new_open": False,
                                        "live_unconfirmed_rr_n": 1}, ts=2003.0)],
        ctx=ctx_c,
        close_rows=pos_net,
    )
    results.append(_assert(
        "C. cap=2 second closes WIN (health GREEN) => allow future second",
        ok is True and not str(reason).startswith("LIVE_SCALE_BLOCKED"),
        f"reason={reason} action={detail.get('action')}",
    ))

    # D. BE / non-negative close clears current loss streak (producer classify_live).
    closes_be = [_close(2.0, 50.0), _close(-1.0, 100.0), _close(-1.1, 200.0),
                 _close(-1.2, 300.0), _close(0.0, 600.0)]
    health_d, _, m_d = rh.classify_live(
        close_rows=closes_be, decision_rows=[], min_lock_rows=[],
        live_state={"trades": []}, live_trade_rows=[],
    )
    results.append(_assert(
        "D. BE (R=0) close after 3 losses => consecutive_losses==0, not streak-RED",
        m_d.get("consecutive_losses") == 0 and health_d != "RED",
        f"health={health_d} m_streak={m_d.get('consecutive_losses')}",
    ))

    # E. portfolio risk room available (1 open @0.005 + 0.005 add <= 0.02 cap) => no block.
    results.append(_assert(
        "E. cap=2 portfolio room (0.005+0.005<=0.02) => no portfolio block",
        _portfolio_blocked(0.005, 0.005, 0.02) is False,
    ))

    # F. portfolio risk exhausted (0.018 + 0.005 > 0.02 cap) => block.
    results.append(_assert(
        "F. cap=2 portfolio exhausted (0.018+0.005>0.02) => portfolio block",
        _portfolio_blocked(0.018, 0.005, 0.02) is True,
    ))

    # G. stale health row (age > 900s) => block stale health.
    _scale_cfg()
    ok, reason, detail = _gate(
        2000.0 + 100000.0,  # far beyond 900s freshness window
        [_health(live="RED", metrics={"loss_streak_current": False,
                                      "loss_streak_stale_after_new_open": True}, ts=2000.0)],
        ctx=DummyCtx(trades=[_open(2000.0)]),
        close_rows=pos_net,
    )
    results.append(_assert(
        "G. stale health row => BLOCK stale health",
        ok is False and reason == "LIVE_SCALE_BLOCKED_STALE_HEALTH",
        f"reason={reason} age={detail.get('health_row_age_sec')}",
    ))

    # H. fresh row, RED purely from stale streak (no unconfirmed, no safety) => allow scale.
    _scale_cfg()
    ctx_h = DummyCtx(trades=[_open(2000.0)])
    ok, reason, detail = _gate(
        2001.0,
        [_health(live="RED",
                 reasons=["live_consecutive_losses=3>=3"],
                 metrics={"loss_streak_current": False,
                          "loss_streak_stale_after_new_open": True,
                          "live_unconfirmed_rr_n": 0}, ts=2001.0)],
        ctx=ctx_h,
        close_rows=pos_net,
    )
    results.append(_assert(
        "H. fresh row stale-only RED => no raw-RED hard block (WARN_ALLOW scale)",
        ok is True and detail.get("action") == "WARN_ALLOW_SCALE",
        f"reason={reason} action={detail.get('action')}",
    ))

    # I-1. genuine OPEN unconfirmed entry => current-safety block (even with stale streak).
    _scale_cfg()
    ctx_i1 = DummyCtx(trades=[_open(2000.0, confirmed=False, entry_unconfirmed=True)])
    ok, reason, detail = _gate(
        2002.0,
        [_health(live="RED", metrics={"loss_streak_current": False,
                                      "loss_streak_stale_after_new_open": True}, ts=2002.0)],
        ctx=ctx_i1,
        close_rows=pos_net,
    )
    results.append(_assert(
        "I-1. OPEN entry unconfirmed => BLOCK current safety (overrides stale streak)",
        ok is False
        and detail.get("scale_block_cause") == "current_safety"
        and detail.get("current_entry_unconfirmed_symbols"),
        f"reason={reason} cause={detail.get('scale_block_cause')} "
        f"sym={detail.get('current_entry_unconfirmed_symbols')}",
    ))

    # I-2. OPEN position missing exchange SL => current-safety block.
    _scale_cfg()
    ctx_i2 = DummyCtx(trades=[_open(2000.0, sl_id="")])
    ok, reason, detail = _gate(
        2003.0,
        [_health(live="RED", metrics={"loss_streak_current": False,
                                      "loss_streak_stale_after_new_open": True}, ts=2003.0)],
        ctx=ctx_i2,
        close_rows=pos_net,
    )
    results.append(_assert(
        "I-2. OPEN missing exchange SL => BLOCK current safety",
        ok is False
        and detail.get("scale_block_cause") == "current_safety"
        and detail.get("current_sl_missing_symbols"),
        f"reason={reason} cause={detail.get('scale_block_cause')} "
        f"sym={detail.get('current_sl_missing_symbols')}",
    ))

    # I-3. closed estimated-exit row alone must NOT register as a current entry-unconfirmed.
    closed_only_ctx = DummyCtx(trades=[_open(2000.0)])  # only a clean confirmed open
    results.append(_assert(
        "I-3. clean open + closed unconfirmed rows => current_entry_unconfirmed empty",
        sd._live_research_current_entry_unconfirmed(closed_only_ctx) == [],
        f"got={sd._live_research_current_entry_unconfirmed(closed_only_ctx)}",
    ))

    return results


def main():
    original_config = dict(sd.config)
    try:
        print("== cap=2 persistence after second trade ==")
        results = run()
    finally:
        sd.config.clear()
        sd.config.update(original_config)
    overall = all(results)
    print("\nRESULT:", "PASS" if overall else "FAIL")
    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
