#!/usr/bin/env python3
"""Simulator: live CONFIRM_SMC_RESEARCH must not deadlock after NFP-style close.

Reproduces the observed "live tịt after NFPUSDT closed" deadlock and validates
the fix. The historical 3-loss confirmed streak (OPG/TAO/PUNDIX) was frozen as
*current* because the three newer probes opened after it (INJ loss, ASTER win,
NFP breakeven) had already CLOSED and were excluded from the confirmed sample
(rr_unconfirmed estimated exits). With live_state empty there was no *open*
position to mark the streak stale, so loss_streak_current stayed True forever.

The fix (audit_research_rolling_health.live_loss_streak_meta) treats the streak
as stale when the MOST RECENT live close after the streak is non-negative
(win/BE), even if that close is no longer an open position. A newer closing
LOSS keeps the streak current. Current-safety failures on genuinely OPEN
positions, hard rolling-net breaches, and stale health rows still hard-block.

Two layers are covered:
  * producer (scripts/debug/audit_research_rolling_health.py): streak staleness.
  * consumer (signal_dispatcher._live_research_micro_pause_status): cap=2 gate.
  * risk accounting (execution.calc_current_total_risk): OPEN positions only.

Required cases:
  A. NFP closed estimated-exit, entry confirmed, R known -> no current-safety block.
  B. NFP closed loss and is latest current loss -> current loss-streak block applies.
  C. NFP closed win/BE -> no loss-streak block (stale).
  D. Closed NFP SL-sync issue does not block (closed row not current safety).
  E. Open trade missing SL still blocks.
  F. Open trade entry unconfirmed still blocks.
  G. Portfolio risk counts only OPEN trades (closed NFP excluded).
  H. cap=2 risk=0.01 portfolio=0.02, one open 0.005 effective risk leaves room.
  I. Fresh health stale-loss-only -> WARN_ALLOW_SCALE.
  J. Stale health row -> BLOCK (stale-health policy).
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

try:
    from execution import calc_current_total_risk as _calc_current_total_risk
except Exception:  # pragma: no cover - fall back to the documented invariant
    def _calc_current_total_risk(trades):
        total = 0
        for t in trades:
            if (t["status"] == "OPEN"
                    and not t.get("quarantined")
                    and t.get("owner", "bot") == "bot"):
                total += t.get("risk_percent", 0.0)
        return total


@dataclass
class DummyCtx:
    execution_mode: str = "live"
    trades: list = field(default_factory=list)


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<78} {status} {detail}")
    return bool(condition)


def _close(realized_r, ts, *, unconfirmed=False, symbol="TESTUSDT", trade_id=None):
    row = {
        "id": trade_id or f"c{int(ts)}",
        "symbol": symbol,
        "side": "SHORT",
        "actual_realized_r": realized_r,
        "_sort_ts": ts,
    }
    if unconfirmed:
        row["rr_unconfirmed"] = True
    return row


def _open(entry_time, *, confirmed=True, entry_unconfirmed=False, sl_id="SL1",
          sl_confirmed=4.6, sl_fail_count=0, trade_id=None, symbol="INJUSDT",
          status="OPEN", risk_percent=0.005):
    return {
        "symbol": symbol,
        "side": "LONG",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "status": status,
        "owner": "bot",
        "entry_time": entry_time,
        "id": trade_id or int(entry_time),
        "entry_price_unconfirmed": entry_unconfirmed,
        "entry_source": "actual_exchange_fill" if confirmed else "pending",
        "entry_state": "ENTRY_CONFIRMED" if confirmed else "PENDING",
        "exchange_sl_id": sl_id,
        "exchange_sl_price_confirmed": sl_confirmed,
        "sl_sync_fail_count": sl_fail_count,
        "risk_percent": risk_percent,
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


# Realistic NFP scenario builder: positive overall confirmed net (wins early),
# a trailing 3-loss confirmed streak, then newer rr_unconfirmed probe closes.
def _nfp_closes(nfp_r, nfp_unconfirmed=True):
    return [
        _close(2.0, 10.0, symbol="WIN1USDT"),
        _close(2.0, 20.0, symbol="WIN2USDT"),
        _close(-1.07, 100.0, symbol="OPGUSDT"),   # confirmed streak loss 1
        _close(-1.12, 200.0, symbol="TAOUSDT"),   # confirmed streak loss 2
        _close(-1.12, 300.0, symbol="PUNDIXUSDT"),  # confirmed streak loss 3
        _close(-1.08, 400.0, unconfirmed=True, symbol="INJUSDT"),   # newer loss (excluded)
        _close(0.61, 500.0, unconfirmed=True, symbol="ASTERUSDT"),  # newer win (excluded)
        _close(nfp_r, 600.0, unconfirmed=nfp_unconfirmed, symbol="NFPUSDT"),  # latest
    ]


# ----------------------------------------------------------------------------
# Producer-level staleness classification
# ----------------------------------------------------------------------------
def producer_tests():
    results = []

    # A / C. NFP closed BE (R=0.0, entry confirmed, R known) -> stale, not current.
    health_c, _, m_c = rh.classify_live(
        close_rows=_nfp_closes(0.0), decision_rows=[], min_lock_rows=[],
        live_state={"trades": []}, live_trade_rows=[],
    )
    results.append(_assert(
        "A/C. NFP closed BE (latest non-negative) => RED streak but STALE, not current",
        health_c == "RED"
        and m_c.get("consecutive_losses") == 3
        and m_c.get("loss_streak_stale_after_new_open") is True
        and m_c.get("loss_streak_current") is False,
        f"m={ {k: m_c.get(k) for k in ('consecutive_losses','loss_streak_current','loss_streak_stale_after_new_open','last_live_open_key')} }",
    ))

    # C2. NFP closed WIN (R=+0.61) latest -> stale, not current.
    _, _, m_w = rh.classify_live(
        close_rows=_nfp_closes(0.61), decision_rows=[], min_lock_rows=[],
        live_state={"trades": []}, live_trade_rows=[],
    )
    results.append(_assert(
        "C2. NFP closed WIN latest => STALE, not current",
        m_w.get("loss_streak_stale_after_new_open") is True
        and m_w.get("loss_streak_current") is False,
        f"current={m_w.get('loss_streak_current')} stale={m_w.get('loss_streak_stale_after_new_open')}",
    ))

    # B. NFP closed LOSS and is the latest close -> current streak stays, not stale.
    health_b, _, m_b = rh.classify_live(
        close_rows=_nfp_closes(-0.9), decision_rows=[], min_lock_rows=[],
        live_state={"trades": []}, live_trade_rows=[],
    )
    results.append(_assert(
        "B. NFP closed LOSS latest => current streak block applies (not stale)",
        health_b == "RED"
        and m_b.get("loss_streak_current") is True
        and m_b.get("loss_streak_stale_after_new_open") is False,
        f"current={m_b.get('loss_streak_current')} stale={m_b.get('loss_streak_stale_after_new_open')}",
    ))

    # Regression: confirmed-newer paths unchanged (existing sim P10/P11 invariants).
    confirmed_streak = [_close(2.0, 50.0), _close(-1.0, 100.0), _close(-1.1, 200.0), _close(-1.2, 300.0)]
    _, _, m_cl = rh.classify_live(
        close_rows=confirmed_streak + [_close(-1.0, 600.0)], decision_rows=[], min_lock_rows=[],
        live_state={"trades": []}, live_trade_rows=[],
    )
    results.append(_assert(
        "R1. confirmed newer LOSS still extends current streak (no false stale)",
        m_cl.get("loss_streak_current") is True
        and m_cl.get("loss_streak_stale_after_new_open") is False,
        f"current={m_cl.get('loss_streak_current')} stale={m_cl.get('loss_streak_stale_after_new_open')}",
    ))

    # Regression: net<=-2 hard guard keeps streak current even with non-neg latest close.
    meta_net = rh.live_loss_streak_meta(
        [_close(-1.0, 100.0), _close(-1.1, 200.0), _close(-1.2, 300.0)],
        3, -2.5, [], {"trades": []},
        all_closes=[_close(-1.0, 100.0), _close(-1.1, 200.0), _close(-1.2, 300.0), _close(0.5, 600.0, unconfirmed=True)],
    )
    results.append(_assert(
        "R2. net<=-2 hard guard => not stale even with newer non-neg close",
        meta_net.get("loss_streak_stale_after_new_open") is False
        and meta_net.get("loss_streak_current") is True,
        f"meta={meta_net}",
    ))

    # Regression: no all_closes arg (default) preserves legacy open-only behavior.
    meta_legacy = rh.live_loss_streak_meta(
        [_close(-1.0, 100.0), _close(-1.1, 200.0), _close(-1.2, 300.0)],
        3, 0.5, [], {"trades": []},
    )
    results.append(_assert(
        "R3. legacy call (no all_closes) => current, not stale (no open)",
        meta_legacy.get("loss_streak_current") is True
        and meta_legacy.get("loss_streak_stale_after_new_open") is False,
        f"meta={meta_legacy}",
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

    # I. Fresh health, stale-loss-only, no open position => WARN_ALLOW_SCALE.
    scale_cfg()
    ok, reason, detail = _gate(
        2001.0,
        [_health(live="RED", paper="GREEN",
                 metrics={"loss_streak_current": False,
                          "loss_streak_stale_after_new_open": True,
                          "last_live_open_key": "id:c600"}, ts=2001.0)],
        ctx=DummyCtx(),
        close_rows=pos_net,
    )
    results.append(_assert(
        "I. fresh health stale-loss-only, no open => WARN_ALLOW_SCALE",
        ok is True
        and reason == ""
        and detail.get("action") == "WARN_ALLOW_SCALE"
        and detail.get("pause_reason") == "LIVE_SCALE_WARN_STALE_LOSS_STREAK_AFTER_NEW_OPEN",
        f"reason={reason} action={detail.get('action')}",
    ))

    # B(consumer). current loss streak => BLOCK current loss streak.
    scale_cfg()
    ok, reason, detail = _gate(
        2002.0,
        [_health(live="RED", paper="GREEN",
                 metrics={"loss_streak_current": True,
                          "loss_streak_stale_after_new_open": False}, ts=2002.0)],
        ctx=DummyCtx(),
        close_rows=pos_net,
    )
    results.append(_assert(
        "B2. current loss streak => BLOCK LIVE_LOSS_STREAK_CURRENT",
        ok is False
        and reason == "LIVE_SCALE_BLOCKED_LIVE_LOSS_STREAK_CURRENT"
        and detail.get("scale_block_cause") == "current_loss_streak",
        f"reason={reason} cause={detail.get('scale_block_cause')}",
    ))

    # D. Closed NFP SL-sync issue: NFP is CLOSED so it is not in ctx (open) trades.
    #    Stale streak + no current-safety => allow (closed row not current safety).
    scale_cfg()
    ctx_d = DummyCtx(trades=[_open(600.0, symbol="NFPUSDT", status="CLOSED", sl_id="")])
    ok, reason, detail = _gate(
        2003.0,
        [_health(live="RED", paper="GREEN",
                 metrics={"loss_streak_current": False,
                          "loss_streak_stale_after_new_open": True}, ts=2003.0)],
        ctx=ctx_d,
        close_rows=pos_net,
    )
    results.append(_assert(
        "D. closed NFP SL-sync issue => not current safety, WARN_ALLOW_SCALE",
        ok is True
        and detail.get("action") == "WARN_ALLOW_SCALE"
        and not detail.get("current_sl_missing_symbols"),
        f"reason={reason} missing={detail.get('current_sl_missing_symbols')}",
    ))

    # E. OPEN trade missing exchange SL => current SL safety hard-block.
    scale_cfg()
    ctx_e = DummyCtx(trades=[_open(600.0, symbol="LIVEUSDT", status="OPEN", sl_id="")])
    ok, reason, detail = _gate(
        2004.0,
        [_health(live="RED", paper="GREEN",
                 metrics={"loss_streak_current": False,
                          "loss_streak_stale_after_new_open": True}, ts=2004.0)],
        ctx=ctx_e,
        close_rows=pos_net,
    )
    results.append(_assert(
        "E. OPEN trade missing SL => BLOCK current safety",
        ok is False
        and detail.get("action") == "BLOCK_SCALE"
        and detail.get("scale_block_cause") == "current_safety",
        f"reason={reason} cause={detail.get('scale_block_cause')} missing={detail.get('current_sl_missing_symbols')}",
    ))

    # F. OPEN trade entry unconfirmed => current entry safety hard-block.
    scale_cfg()
    ctx_f = DummyCtx(trades=[_open(600.0, symbol="LIVEUSDT", status="OPEN",
                                   confirmed=False, entry_unconfirmed=True)])
    ok, reason, detail = _gate(
        2005.0,
        [_health(live="RED", paper="GREEN", reasons=["entry_unconfirmed"],
                 metrics={"loss_streak_current": False,
                          "loss_streak_stale_after_new_open": True}, ts=2005.0)],
        ctx=ctx_f,
        close_rows=pos_net,
    )
    results.append(_assert(
        "F. OPEN trade entry unconfirmed => BLOCK current safety",
        ok is False
        and detail.get("action") == "BLOCK_SCALE"
        and detail.get("scale_block_cause") == "current_safety",
        f"reason={reason} cause={detail.get('scale_block_cause')} unconf={detail.get('current_entry_unconfirmed_symbols')}",
    ))

    # J. Stale health row => BLOCK_SCALE with stale-health policy.
    scale_cfg()
    ok, reason, detail = _gate(
        2006.0 + 10_000.0,  # health row ts far in the past => stale
        [_health(live="RED", paper="GREEN",
                 metrics={"loss_streak_current": False,
                          "loss_streak_stale_after_new_open": True}, ts=2006.0)],
        ctx=DummyCtx(),
        close_rows=pos_net,
    )
    results.append(_assert(
        "J. stale health row => BLOCK_SCALE LIVE_SCALE_BLOCKED_STALE_HEALTH",
        ok is False
        and reason == "LIVE_SCALE_BLOCKED_STALE_HEALTH"
        and detail.get("health_row_stale") is True,
        f"reason={reason} stale={detail.get('health_row_stale')}",
    ))

    # A(consumer). Stale streak + paper GREEN + confirmed entry/R known => allow.
    scale_cfg()
    ok, reason, detail = _gate(
        2007.0,
        [_health(live="RED", paper="GREEN",
                 metrics={"loss_streak_current": False,
                          "loss_streak_stale_after_new_open": True,
                          "live_unconfirmed_rr_n": 3}, ts=2007.0)],
        ctx=DummyCtx(),
        close_rows=pos_net,
    )
    results.append(_assert(
        "A2. NFP estimated-exit (rr_unconfirmed) closed => no current-safety block",
        ok is True and detail.get("action") == "WARN_ALLOW_SCALE",
        f"reason={reason} action={detail.get('action')}",
    ))

    return results


# ----------------------------------------------------------------------------
# Portfolio risk accounting (OPEN positions only)
# ----------------------------------------------------------------------------
def risk_tests():
    results = []

    closed_nfp = _open(600.0, symbol="NFPUSDT", status="CLOSED", risk_percent=0.01)
    open_one = _open(700.0, symbol="LIVEUSDT", status="OPEN", risk_percent=0.005)

    # G. Portfolio risk counts only OPEN trades (closed NFP excluded).
    total_with_closed = _calc_current_total_risk([closed_nfp, open_one])
    results.append(_assert(
        "G. portfolio risk excludes CLOSED NFP, counts only OPEN (0.005)",
        abs(total_with_closed - 0.005) < 1e-9,
        f"current_total_risk={total_with_closed}",
    ))

    # H. cap=2 risk=0.01 portfolio=0.02, one open 0.005 => room for new 0.01.
    cap = 2
    portfolio_cap = 0.02
    new_risk = 0.01
    current_total = _calc_current_total_risk([open_one])
    open_count = sum(1 for t in [open_one] if t["status"] == "OPEN")
    results.append(_assert(
        "H. one open 0.005 + new 0.01 <= 0.02 and open_count(1) < cap(2) => room",
        abs(current_total - 0.005) < 1e-9
        and (current_total + new_risk) <= portfolio_cap + 1e-9
        and open_count < cap,
        f"current={current_total} +new={current_total + new_risk} cap={portfolio_cap} open={open_count}/{cap}",
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
        print("\n== portfolio risk accounting ==")
        results += risk_tests()
    finally:
        sd.config.clear()
        sd.config.update(original_config)

    overall = all(results)
    print("\nRESULT:", "PASS" if overall else "FAIL")
    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
