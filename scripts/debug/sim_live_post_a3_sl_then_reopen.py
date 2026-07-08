"""Simulator: live opens 1 trade after A3, hits SL, then must not deadlock.

Reproduces the 2026-06-30 RAVEUSDT case and locks the fix in
``live_loss_streak_meta`` (audit_research_rolling_health.py): a stale historical
3-loss streak must NOT be pinned "current" by a single fresh loss that follows
an intervening win/BE close. It also guards the safety cases that must STILL
block (genuine ongoing 3-loss run, critical SL-sync failure).

This drives the same health-row producer (``classify_live``) the live runtime
consumes via main.py -> build_summary -> research_rolling_health.jsonl, whose
``loss_streak_current`` feeds the A3 WARN-mode gate in signal_dispatcher.py
(``loss_streak_not_current`` condition). It does NOT touch live/testnet orders,
config, or state files.
"""

import importlib.util
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
AUDIT_PATH = os.path.join(ROOT, "scripts", "debug", "audit_research_rolling_health.py")

_spec = importlib.util.spec_from_file_location("audit_research_rolling_health", AUDIT_PATH)
arh = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(arh)


def close_row(ts, realized_r, *, rr_unconfirmed=False, ident=None, symbol="TESTUSDT", side="LONG"):
    return {
        "id": ident if ident is not None else f"id-{ts}",
        "symbol": symbol,
        "side": side,
        "actual_realized_r": realized_r,
        "rr_unconfirmed": "true" if rr_unconfirmed else None,
        "entry_unconfirmed": None,
        "exit_unconfirmed": None,
        "_sort_ts": float(ts),
    }


def open_trade(ts, *, healthy=True, ident="open-1"):
    trade = {
        "id": ident,
        "symbol": "OPENUSDT",
        "side": "LONG",
        "status": "OPEN",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "entry_time": float(ts),
        "entry_source": "actual_exchange_fill",
        "entry_state": "ENTRY_CONFIRMED",
        "exchange_sl_id": "sl-1",
        "exchange_sl_price_confirmed": 100.0,
        "sl_sync_fail_count": 0,
    }
    if not healthy:
        trade["entry_price_unconfirmed"] = "true"
    return trade


def classify(closes, live_state):
    # Isolate from on-disk logs: empty decision/min-lock rows.
    return arh.classify_live(
        close_rows=closes,
        decision_rows=[],
        min_lock_rows=[],
        live_state=live_state,
    )


RESULTS = []

# Confirmed winning closes that keep the confirmed rolling net above the -2R
# hard pause threshold, so streak cases isolate the loss-streak rule from the
# separate rolling-net rule (live_research_rolling_net_pause_r = -2.0). Mirrors
# the real 2026-06-30 sample whose confirmed net was +3.62.
NET_LIFT = [
    close_row(100, 2.5, ident="LIFT1"),
    close_row(200, 2.5, ident="LIFT2"),
]


def not_current(metrics):
    # The dispatcher gate evaluates bool(live_metrics.get("loss_streak_current")),
    # so both False and missing/None mean "not current" (A3 allowed).
    return not bool(metrics.get("loss_streak_current"))


def check(name, condition, detail=""):
    RESULTS.append((name, bool(condition), detail))
    flag = "PASS" if condition else "FAIL"
    print(f"[{flag}] {name}" + (f" :: {detail}" if detail else ""))


# --- Headline regression: the exact RAVEUSDT 2026-06-30 shape -----------------
# 3 confirmed losses (06-27/28), then rr_unconfirmed: INJ loss, ASTER win,
# NFP BE, RAVE loss (latest). The stale 3-streak must be downgraded.
rave = NET_LIFT + [
    close_row(1000, -1.07, ident="OPG"),
    close_row(2000, -1.12, ident="TAO"),
    close_row(3000, -1.12, ident="PUNDIX"),
    close_row(4000, -1.08, rr_unconfirmed=True, ident="INJ"),
    close_row(5000, 0.61, rr_unconfirmed=True, ident="ASTER"),
    close_row(6000, 0.0, rr_unconfirmed=True, ident="NFP"),
    close_row(7000, -1.03, rr_unconfirmed=True, ident="RAVE"),
]
color, reasons, metrics = classify(rave, [])
check(
    "REGRESSION RAVE: stale 3-streak downgraded after intervening win/BE then 1 fresh loss",
    not_current(metrics) and metrics.get("consecutive_losses") == 3,
    f"loss_streak_current={metrics.get('loss_streak_current')} consecutive={metrics.get('consecutive_losses')} health={color}",
)

# --- Case A: A3 allows paper RED when live clean and 0 open -------------------
# No 3-streak at all -> loss_streak_current False, no critical.
clean = [
    close_row(1000, 1.0, ident="W1"),
    close_row(2000, -1.0, ident="L1"),
    close_row(3000, 0.8, ident="W2"),
]
color, reasons, metrics = classify(clean, [])
check(
    "A: live clean (no streak), 0 open -> loss_streak_current False",
    not_current(metrics),
    f"loss_streak_current={metrics.get('loss_streak_current')} health={color}",
)

# --- Case B: A3 allows second trade when 1 open and live clean ---------------
# 3 old confirmed losses but a newer CONFIRMED-HEALTHY open exists -> stale.
open_state = [open_trade(9000, healthy=True)]
streak_with_open = NET_LIFT + [
    close_row(1000, -1.1, ident="L1"),
    close_row(2000, -1.1, ident="L2"),
    close_row(3000, -1.1, ident="L3"),
]
color, reasons, metrics = classify(streak_with_open, open_state)
check(
    "B: 1 newer healthy open after streak -> loss_streak_current False (2nd trade allowed)",
    not_current(metrics),
    f"loss_streak_current={metrics.get('loss_streak_current')} stale={metrics.get('loss_streak_stale_after_new_open')}",
)

# --- Case C: genuine ongoing 3-loss run still blocks -------------------------
genuine = NET_LIFT + [
    close_row(2000, -1.1, ident="L1"),
    close_row(3000, -1.1, ident="L2"),
    close_row(4000, -1.1, ident="L3"),
]
color, reasons, metrics = classify(genuine, [])
check(
    "C: genuine fresh 3-loss run, no newer non-loss/open -> loss_streak_current True (blocks)",
    metrics.get("loss_streak_current") is True and color == "RED",
    f"loss_streak_current={metrics.get('loss_streak_current')} health={color}",
)

# --- Case C-guard: win then 3 NEW losses must STAY current (no over-relax) ----
new_run = NET_LIFT + [
    close_row(1000, -1.1, ident="L1"),
    close_row(2000, -1.1, ident="L2"),
    close_row(3000, -1.1, ident="L3"),
    close_row(4000, 0.7, rr_unconfirmed=True, ident="WIN"),
    close_row(5000, -1.1, rr_unconfirmed=True, ident="N1"),
    close_row(6000, -1.1, rr_unconfirmed=True, ident="N2"),
    close_row(7000, -1.1, rr_unconfirmed=True, ident="N3"),
]
color, reasons, metrics = classify(new_run, [])
check(
    "C-guard: win then 3 NEW losses -> loss_streak_current True (still blocks)",
    metrics.get("loss_streak_current") is True,
    f"loss_streak_current={metrics.get('loss_streak_current')}",
)

# --- Case D: BE/non-negative latest close does not create current block -------
be_latest = NET_LIFT + [
    close_row(1000, -1.1, ident="L1"),
    close_row(2000, -1.1, ident="L2"),
    close_row(3000, -1.1, ident="L3"),
    close_row(4000, 0.0, rr_unconfirmed=True, ident="BE"),
]
color, reasons, metrics = classify(be_latest, [])
check(
    "D: BE latest close after 3-streak -> loss_streak_current False",
    not_current(metrics),
    f"loss_streak_current={metrics.get('loss_streak_current')}",
)

# --- Case E: closed rr_unconfirmed rows do not current-safety block -----------
# Only rr_unconfirmed closes -> excluded from confirmed sample, no critical,
# health UNKNOWN (no confirmed rr), loss_streak_current False.
unconf_only = [
    close_row(1000, -1.1, rr_unconfirmed=True, ident="U1"),
    close_row(2000, -1.1, rr_unconfirmed=True, ident="U2"),
    close_row(3000, -1.1, rr_unconfirmed=True, ident="U3"),
]
color, reasons, metrics = classify(unconf_only, [])
check(
    "E: rr_unconfirmed-only closes -> no confirmed streak, loss_streak_current False",
    not_current(metrics) and metrics.get("live_unconfirmed_rr_n") == 3,
    f"loss_streak_current={metrics.get('loss_streak_current')} unconf_n={metrics.get('live_unconfirmed_rr_n')} health={color}",
)

# --- Case F: CLOSED trade does not count as open portfolio/open risk ----------
closed_state = [
    {
        "id": "closed-1",
        "symbol": "OPENUSDT",
        "side": "LONG",
        "status": "CLOSED",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "entry_time": 9000.0,
    }
]
open_positions = arh.live_research_open_positions(closed_state)
check(
    "F: CLOSED research row excluded from open positions (no portfolio/open risk)",
    open_positions == [],
    f"open_positions={len(open_positions)}",
)

# --- Case G: A3 config WARN mode + cap/risk preserved in config.json ----------
with open(os.path.join(ROOT, "config.json")) as handle:
    cfg = json.load(handle)
check(
    "G: live_paper_red_scale_mode == WARN_ONLY_WHEN_LIVE_HEALTH_OK",
    cfg.get("live_paper_red_scale_mode") == "WARN_ONLY_WHEN_LIVE_HEALTH_OK",
    f"mode={cfg.get('live_paper_red_scale_mode')}",
)
check(
    "G: cap=2, live_risk=0.01, portfolio_risk=0.02 preserved",
    cfg.get("max_live_research_trades") == 2
    and cfg.get("live_risk_per_trade") == 0.01
    and cfg.get("live_max_portfolio_risk") == 0.02,
    f"cap={cfg.get('max_live_research_trades')} risk={cfg.get('live_risk_per_trade')} pf={cfg.get('live_max_portfolio_risk')}",
)

# --- Case H: critical safety failure (SL-sync) keeps streak current ----------
# An open research trade with an unresolved SL-sync risk is critical; the
# staleness downgrade must never fire while critical. (Mirrors the dispatcher
# AND-gate that runtime_error / active-pause still block even when A3 passes.)
critical_state = [
    {
        "id": "open-crit",
        "symbol": "CRITUSDT",
        "side": "LONG",
        "status": "OPEN",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "entry_time": 9000.0,
        "entry_source": "actual_exchange_fill",
        "entry_state": "ENTRY_CONFIRMED",
        "exchange_sl_id": None,
        "exchange_sl_price_confirmed": None,
        "sl_sync_fail_count": 3,
    }
]
crit_closes = [
    close_row(1000, -1.1, ident="L1"),
    close_row(2000, -1.1, ident="L2"),
    close_row(3000, -1.1, ident="L3"),
    close_row(4000, 0.0, rr_unconfirmed=True, ident="BE"),
]
color, reasons, metrics = classify(crit_closes, critical_state)
check(
    "H: critical SL-sync failure -> health RED with sl-sync reason (A3 no_sl_sync_failure blocks)",
    color == "RED" and "live_sl_sync_failure" in reasons,
    f"health={color} reasons={reasons} loss_streak_current={metrics.get('loss_streak_current')}",
)

print()
failed = [name for name, ok, _ in RESULTS if not ok]
if failed:
    print(f"FAIL: {len(failed)}/{len(RESULTS)} cases failed: {failed}")
    raise SystemExit(1)
print(f"PASS: all {len(RESULTS)} cases passed")
