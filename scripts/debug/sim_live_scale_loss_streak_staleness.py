#!/usr/bin/env python3
"""Simulator for live cap=2 scale gating vs current/stale live loss streak.

Validates that a historical (stale) 3-loss streak does not permanently block
cap=2 scale once a newer, confirmed, healthy live research open exists, while
all current-safety gates (SL sync, entry/RR confirmation, rolling-net hard
breach) and a genuinely current trailing loss streak still hard-block.

Two layers are covered:
  * producer  (scripts/debug/audit_research_rolling_health.py): streak staleness
    classification + emitted health-row flags.
  * consumer  (signal_dispatcher._live_research_micro_pause_status): the cap=2
    scale-gate decision driven by those flags.
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
    print(f"{label:<74} {status} {detail}")
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
          sl_confirmed=4.6, sl_fail_count=0, trade_id=None):
    trade = {
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
        "exchange_sl_id": sl_id,
        "exchange_sl_price_confirmed": sl_confirmed,
        "sl_sync_fail_count": sl_fail_count,
    }
    return trade


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


# ----------------------------------------------------------------------------
# Producer-level staleness classification
# ----------------------------------------------------------------------------
def producer_tests():
    results = []

    results.append(_assert(
        "P1. open_trade_confirmed_healthy: clean confirmed open => True",
        rh.open_trade_confirmed_healthy(_open(2000.0)) is True,
    ))
    results.append(_assert(
        "P2. open_trade_confirmed_healthy: entry unconfirmed => False",
        rh.open_trade_confirmed_healthy(_open(2000.0, entry_unconfirmed=True)) is False,
    ))
    results.append(_assert(
        "P3. open_trade_confirmed_healthy: missing exchange SL => False",
        rh.open_trade_confirmed_healthy(_open(2000.0, sl_id="")) is False,
    ))
    results.append(_assert(
        "P4. open_trade_confirmed_healthy: sl_sync_fail_count>0 => False",
        rh.open_trade_confirmed_healthy(_open(2000.0, sl_fail_count=2)) is False,
    ))

    closes3 = [_close(-1.0, 100.0), _close(-1.1, 200.0), _close(-1.2, 300.0)]

    meta_stale = rh.live_loss_streak_meta(closes3, 3, 0.5, [], {"trades": [_open(500.0)]})
    results.append(_assert(
        "P5. 3-loss streak + newer confirmed open => stale, not current",
        meta_stale["loss_streak_stale_after_new_open"] is True
        and meta_stale["loss_streak_current"] is False
        and meta_stale["last_live_open_key"].startswith("id:"),
        f"meta={meta_stale}",
    ))

    meta_current = rh.live_loss_streak_meta(closes3, 3, 0.5, [], {"trades": []})
    results.append(_assert(
        "P6. 3-loss streak + no open => current, not stale",
        meta_current["loss_streak_current"] is True
        and meta_current["loss_streak_stale_after_new_open"] is False,
        f"meta={meta_current}",
    ))

    meta_older = rh.live_loss_streak_meta(closes3, 3, 0.5, [], {"trades": [_open(250.0)]})
    results.append(_assert(
        "P7. 3-loss streak + open OLDER than last loss => current, not stale",
        meta_older["loss_streak_current"] is True
        and meta_older["loss_streak_stale_after_new_open"] is False,
        f"meta={meta_older}",
    ))

    meta_net = rh.live_loss_streak_meta(closes3, 3, -2.5, [], {"trades": [_open(500.0)]})
    results.append(_assert(
        "P8. 3-loss streak + newer open BUT net<=-2 => not stale (hard net guard)",
        meta_net["loss_streak_stale_after_new_open"] is False
        and meta_net["loss_streak_current"] is True,
        f"meta={meta_net}",
    ))

    # End-to-end classify_live with newer confirmed open => RED but flagged stale.
    closes_pos = [_close(2.0, 50.0), _close(-1.0, 100.0), _close(-1.1, 200.0), _close(-1.2, 300.0)]
    health, reasons, m = rh.classify_live(
        close_rows=closes_pos, decision_rows=[], min_lock_rows=[],
        live_state={"trades": [_open(500.0)]}, live_trade_rows=[],
    )
    results.append(_assert(
        "P9. classify_live: trailing 3 losses + newer open => RED + stale flag",
        health == "RED"
        and m.get("loss_streak_stale_after_new_open") is True
        and m.get("consecutive_losses") == 3,
        f"health={health} m={m}",
    ))

    # E. newer open later closes loss => streak current again (open gone, loss newest).
    closes_e = closes_pos + [_close(-1.0, 600.0)]
    health_e, _, m_e = rh.classify_live(
        close_rows=closes_e, decision_rows=[], min_lock_rows=[],
        live_state={"trades": []}, live_trade_rows=[],
    )
    results.append(_assert(
        "P10/E. newer open closes LOSS => RED, current streak (not stale)",
        health_e == "RED"
        and m_e.get("loss_streak_current") is True
        and m_e.get("loss_streak_stale_after_new_open") is False,
        f"health={health_e} m={m_e}",
    ))

    # F. newer open later closes win => streak broken, not RED-for-streak.
    closes_f = closes_pos + [_close(1.5, 600.0)]
    health_f, _, m_f = rh.classify_live(
        close_rows=closes_f, decision_rows=[], min_lock_rows=[],
        live_state={"trades": []}, live_trade_rows=[],
    )
    results.append(_assert(
        "P11/F. newer open closes WIN => no streak RED (consecutive_losses==0)",
        m_f.get("consecutive_losses") == 0 and health_f != "RED",
        f"health={health_f} m={m_f}",
    ))

    return results


# ----------------------------------------------------------------------------
# Consumer-level cap=2 scale-gate decision
# ----------------------------------------------------------------------------
def consumer_tests():
    results = []
    sd.config["live_research_micro_pause_enabled"] = True
    sd.config["live_research_loss_streak_pause_count"] = 3
    sd.config["live_research_rolling_net_pause_r"] = -2.0

    def scale_cfg():
        sd.config["max_live_research_trades"] = 2
        sd.config["live_risk_per_trade"] = 0.01
        sd.config["live_max_portfolio_risk"] = 0.02

    pos_net = [_close(3.62, 1000.0)]  # rolling net positive => no rolling-net hard block

    # A. cap=2 + fresh health + trailing 3 losses + no newer open => current-streak block.
    scale_cfg()
    ok, reason, detail = _gate(
        2000.0,
        [_health(live="RED", metrics={"loss_streak_current": True,
                                      "loss_streak_stale_after_new_open": False}, ts=2000.0)],
        ctx=DummyCtx(),
        close_rows=pos_net,
    )
    results.append(_assert(
        "A. cap=2 + current 3-loss streak, no newer open => BLOCK current loss streak",
        ok is False
        and reason == "LIVE_SCALE_BLOCKED_LIVE_LOSS_STREAK_CURRENT"
        and detail.get("action") == "BLOCK_SCALE"
        and detail.get("scale_block_cause") == "current_loss_streak",
        f"reason={reason} cause={detail.get('scale_block_cause')}",
    ))

    # B. cap=2 + trailing 3 losses + newer confirmed healthy open => WARN/ALLOW.
    scale_cfg()
    ctx_b = DummyCtx(trades=[_open(2000.0)])
    ok, reason, detail = _gate(
        2001.0,
        [_health(live="RED", metrics={"loss_streak_current": False,
                                      "loss_streak_stale_after_new_open": True,
                                      "last_live_open_key": "id:2000"}, ts=2001.0)],
        ctx=ctx_b,
        close_rows=pos_net,
    )
    results.append(_assert(
        "B. cap=2 + stale streak + newer confirmed open => WARN_ALLOW scale",
        ok is True
        and reason == ""
        and detail.get("action") == "WARN_ALLOW_SCALE"
        and detail.get("pause_reason") == "LIVE_SCALE_WARN_STALE_LOSS_STREAK_AFTER_NEW_OPEN",
        f"reason={reason} action={detail.get('action')}",
    ))

    # C. newer open exists but entry unconfirmed => current entry/fill safety hard-block.
    scale_cfg()
    ctx_c = DummyCtx(trades=[_open(2000.0, confirmed=False, entry_unconfirmed=True)])
    ok, reason, detail = _gate(
        2002.0,
        [_health(live="RED", reasons=["entry_unconfirmed"],
                 metrics={"loss_streak_current": True,
                          "loss_streak_stale_after_new_open": False,
                          "live_unconfirmed_rr_n": 1}, ts=2002.0)],
        ctx=ctx_c,
        close_rows=pos_net,
    )
    results.append(_assert(
        "C. newer open entry unconfirmed => BLOCK current safety (not allow)",
        ok is False
        and detail.get("action") == "BLOCK_SCALE"
        and detail.get("scale_block_cause") == "current_safety",
        f"reason={reason} cause={detail.get('scale_block_cause')}",
    ))

    # D. newer open missing exchange SL => current SL safety hard-block.
    scale_cfg()
    ctx_d = DummyCtx(trades=[_open(2000.0, sl_id="")])
    ok, reason, detail = _gate(
        2003.0,
        [_health(live="RED", metrics={"loss_streak_current": True,
                                      "loss_streak_stale_after_new_open": False}, ts=2003.0)],
        ctx=ctx_d,
        close_rows=pos_net,
    )
    results.append(_assert(
        "D. newer open missing exchange SL => BLOCK current safety",
        ok is False
        and detail.get("action") == "BLOCK_SCALE"
        and detail.get("scale_block_cause") == "current_safety",
        f"reason={reason} cause={detail.get('scale_block_cause')} missing={detail.get('current_sl_missing_symbols')}",
    ))

    # G. rolling net <= hard threshold => BLOCK regardless of stale streak.
    scale_cfg()
    ctx_g = DummyCtx(trades=[_open(2000.0)])
    ok, reason, detail = _gate(
        2004.0,
        [_health(live="RED", metrics={"loss_streak_current": False,
                                      "loss_streak_stale_after_new_open": True}, ts=2004.0)],
        ctx=ctx_g,
        close_rows=[_close(-1.3, 900.0), _close(-1.3, 1000.0)],  # net -2.6 <= -2
    )
    results.append(_assert(
        "G. rolling net hard breach => BLOCK even with stale streak",
        ok is False
        and detail.get("action") == "BLOCK_SCALE"
        and detail.get("scale_block_cause") == "rolling_net_hard",
        f"reason={reason} cause={detail.get('scale_block_cause')}",
    ))

    # H. cap=1 behavior unchanged (no scale branch; micro live-health block).
    sd.config["max_live_research_trades"] = 1
    sd.config["live_risk_per_trade"] = 0.005
    sd.config["live_max_portfolio_risk"] = 0.005
    ok, reason, detail = _gate(
        2005.0,
        [_health(live="RED", metrics={"loss_streak_current": True,
                                      "loss_streak_stale_after_new_open": False}, ts=2005.0)],
        ctx=DummyCtx(),
        close_rows=[],
    )
    results.append(_assert(
        "H. cap=1 unchanged => micro (not scale) live-health block",
        ok is False
        and reason == "LIVE_MICRO_BLOCKED_LIVE_HEALTH"
        and not str(reason).startswith("LIVE_SCALE_"),
        f"reason={reason}",
    ))

    return results


def main():
    original_config = dict(sd.config)
    try:
        results = []
        print("== producer staleness classification ==")
        results += producer_tests()
        print("\n== consumer cap=2 scale gate ==")
        results += consumer_tests()
    finally:
        sd.config.clear()
        sd.config.update(original_config)

    overall = all(results)
    print("\nRESULT:", "PASS" if overall else "FAIL")
    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
