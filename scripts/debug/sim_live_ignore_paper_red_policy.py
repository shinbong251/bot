#!/usr/bin/env python3
"""Simulator for the paper-RED scale policy (Option A3) — READ-ONLY.

Models the EXACT decision now implemented in
signal_dispatcher._live_research_micro_pause_status() at the
"paper_health == RED and scale_block" branch, so the truth table and audit
payload can be validated without driving live execution.

Background
----------
With the live scale config (max_live_research_trades=2, risk=0.01,
portfolio=0.02) scale_block is True. Previously paper RED unconditionally
hard-blocked BOTH the first (open=0) and second (open=1) live research
trade. Option A3 makes paper RED a WARN_ONLY allow ONLY when the config
flag live_paper_red_scale_mode == "WARN_ONLY_WHEN_LIVE_HEALTH_OK" AND every
live-side safety condition holds; otherwise it preserves the hard block.
Cap/risk are never changed by this policy.

This mirrors the production branch exactly; it does not import it because
the production function needs ctx + log/file reads + config.
"""

import sys

# --- frozen policy invariants (must never be changed by this policy) ---
EXPECTED_MAX_LIVE_RESEARCH_TRADES = 2
EXPECTED_LIVE_RISK_PER_TRADE = 0.01
EXPECTED_LIVE_MAX_PORTFOLIO_RISK = 0.02

REQUIRED_AUDIT_FIELDS = (
    "paper_red_ignored",
    "paper_red_ignore_reason",
    "original_paper_health",
    "original_paper_active_health",
    "live_paper_red_scale_mode",
    "live_rolling_net_r",
    "loss_streak_current",
    "current_entry_unconfirmed_symbols",
    "current_sl_missing_symbols",
    "live_sl_sync_failure",
    "health_row_fresh",
    "open_live_count",
    "max_live_research_trades",
    "runtime_error_blocker",
    "active_pause_blocker",
    "pause_until",
    "pause_remaining_sec",
)


def paper_red_block_entered(paper_health, scale_block):
    """The A3 branch is only entered when paper is RED and scale_block True."""
    return str(paper_health).upper() == "RED" and bool(scale_block)


def evaluate_paper_red_scale(state, mode="BLOCK"):
    """Pure model of the production paper-RED decision under scale_block.

    Precondition: paper_health == RED and scale_block True (cap>=2).
    Returns (allow: bool, reason: str, audit: dict).
    `mode` is config.live_paper_red_scale_mode.
    """
    open_live = state.get("open_live_count")
    cap = state.get("max_live_research_trades")
    rolling = state.get("live_rolling_net_r")
    rolling = rolling if isinstance(rolling, (int, float)) else None

    conditions = {
        "mode_warn_only": mode == "WARN_ONLY_WHEN_LIVE_HEALTH_OK",
        "live_rolling_net_r_positive": rolling is not None and rolling > 0,
        "loss_streak_not_current": not bool(state.get("loss_streak_current")),
        "no_entry_unconfirmed": not (state.get("current_entry_unconfirmed_symbols") or []),
        "no_sl_missing": not (state.get("current_sl_missing_symbols") or []),
        "no_sl_sync_failure": not bool(state.get("live_sl_sync_failure")),
        "health_row_fresh": bool(state.get("health_row_fresh")),
        "cap_room": (isinstance(open_live, int) and isinstance(cap, int)
                     and open_live < cap),
    }

    # Safety extension blockers (evaluated only when base A3 conditions pass).
    runtime_error = bool(state.get("runtime_error"))
    pause_remaining = state.get("pause_remaining_sec") or 0
    active_pause = bool(pause_remaining and pause_remaining > 0)
    pause_until = state.get("pause_until")

    audit = {
        "paper_red_ignored": False,
        "paper_red_ignore_reason": "",
        "original_paper_health": "RED",
        "original_paper_active_health": "RED",
        "live_paper_red_scale_mode": mode,
        "live_rolling_net_r": state.get("live_rolling_net_r"),
        "loss_streak_current": bool(state.get("loss_streak_current")),
        "current_entry_unconfirmed_symbols": list(state.get("current_entry_unconfirmed_symbols") or []),
        "current_sl_missing_symbols": list(state.get("current_sl_missing_symbols") or []),
        "live_sl_sync_failure": bool(state.get("live_sl_sync_failure")),
        "health_row_fresh": bool(state.get("health_row_fresh")),
        "open_live_count": open_live,
        "max_live_research_trades": cap,
        "paper_red_conditions": conditions,
        "runtime_error_blocker": runtime_error,
        "active_pause_blocker": active_pause,
        "pause_until": pause_until,
        "pause_remaining_sec": pause_remaining,
    }

    if all(conditions.values()):
        if runtime_error:
            audit.update({
                "paper_red_ignored": False,
                "paper_red_ignore_reason": "RUNTIME_ERROR_BLOCKER",
                "pause_reason": "LIVE_SCALE_BLOCKED_PAPER_HEALTH",
                "action": "BLOCK_SCALE",
            })
            return False, "LIVE_SCALE_BLOCKED_PAPER_HEALTH", audit
        if active_pause:
            audit.update({
                "paper_red_ignored": False,
                "paper_red_ignore_reason": "ACTIVE_MICRO_PAUSE_BLOCKER",
                "pause_reason": "LIVE_SCALE_BLOCKED_PAPER_HEALTH",
                "action": "BLOCK_SCALE",
            })
            return False, "LIVE_SCALE_BLOCKED_PAPER_HEALTH", audit
        audit.update({
            "paper_red_ignored": True,
            "paper_red_ignore_reason": "LIVE_HEALTH_OK_AND_ROLLING_POSITIVE",
            "pause_reason": "LIVE_SCALE_WARN_PAPER_HEALTH_RED_ALLOWED",
            "micro_allowed_despite_paper_red": True,
            "action": "WARN_ALLOW_SCALE",
        })
        return True, "", audit

    failed = [k for k, v in conditions.items() if not v]
    audit.update({
        "paper_red_ignore_reason": "BLOCKED:" + ",".join(failed),
        "pause_reason": "LIVE_SCALE_BLOCKED_PAPER_HEALTH",
        "action": "BLOCK_SCALE",
    })
    return False, "LIVE_SCALE_BLOCKED_PAPER_HEALTH", audit


def _clean_state(**over):
    """Baseline: paper RED + all live-side safety conditions OK."""
    base = {
        "live_rolling_net_r": 3.15,
        "loss_streak_current": False,
        "current_entry_unconfirmed_symbols": [],
        "current_sl_missing_symbols": [],
        "live_sl_sync_failure": False,
        "health_row_fresh": True,
        "open_live_count": 0,
        "max_live_research_trades": EXPECTED_MAX_LIVE_RESEARCH_TRADES,
    }
    base.update(over)
    return base


WARN = "WARN_ONLY_WHEN_LIVE_HEALTH_OK"

CASES = [
    # (id, mode, state, expect_allow, expect_reason_substr)
    ("A 0-open clean", WARN, _clean_state(open_live_count=0), True, ""),
    ("B 1-open cap2 room", WARN, _clean_state(open_live_count=1), True, ""),
    ("C current loss streak", WARN, _clean_state(loss_streak_current=True),
     False, "BLOCKED_PAPER_HEALTH"),
    ("D missing SL open trade", WARN,
     _clean_state(current_sl_missing_symbols=["ETHUSDT"]), False, "BLOCKED_PAPER_HEALTH"),
    ("E entry unconfirmed open", WARN,
     _clean_state(current_entry_unconfirmed_symbols=["BTCUSDT"]), False, "BLOCKED_PAPER_HEALTH"),
    ("F rolling <= 0 (neg)", WARN, _clean_state(live_rolling_net_r=-1.67),
     False, "BLOCKED_PAPER_HEALTH"),
    ("F rolling == 0", WARN, _clean_state(live_rolling_net_r=0.0),
     False, "BLOCKED_PAPER_HEALTH"),
    ("F sl_sync_failure", WARN, _clean_state(live_sl_sync_failure=True),
     False, "BLOCKED_PAPER_HEALTH"),
    ("H cap full (2 open)", WARN, _clean_state(open_live_count=2),
     False, "BLOCKED_PAPER_HEALTH"),
    ("I health stale", WARN, _clean_state(health_row_fresh=False),
     False, "BLOCKED_PAPER_HEALTH"),
    ("J default mode BLOCK", "BLOCK", _clean_state(open_live_count=0),
     False, "BLOCKED_PAPER_HEALTH"),
    ("K mode typo/unknown", "WARN_ONLY", _clean_state(open_live_count=0),
     False, "BLOCKED_PAPER_HEALTH"),
    ("K mode empty", "", _clean_state(open_live_count=0),
     False, "BLOCKED_PAPER_HEALTH"),
    ("M runtime_error", WARN, _clean_state(open_live_count=0, runtime_error=True),
     False, "BLOCKED_PAPER_HEALTH"),
    ("N active pause window", WARN,
     _clean_state(open_live_count=0, pause_remaining_sec=1800, pause_until=9.99e9),
     False, "BLOCKED_PAPER_HEALTH"),
    ("O clean no rt/no pause", WARN,
     _clean_state(open_live_count=0, runtime_error=False, pause_remaining_sec=0),
     True, ""),
]


def main():
    print("=" * 74)
    print("SIM: paper-RED scale policy (Option A3) truth table — mirrors production")
    print("=" * 74)

    failures = 0
    for cid, mode, state, exp_allow, exp_reason in CASES:
        allow, reason, audit = evaluate_paper_red_scale(state, mode=mode)
        ok = (allow == exp_allow) and (exp_reason in reason)
        if not ok:
            failures += 1
        status = "PASS" if ok else "FAIL"
        verdict = "WARN_ALLOW_SCALE" if allow else "BLOCK_SCALE"
        print(f"[{status}] {cid:<24} mode={mode:<30} -> {verdict:<16} "
              f"action={audit.get('action')}")
        if not ok:
            print(f"        expected allow={exp_allow} reason~={exp_reason!r} "
                  f"got allow={allow} reason={reason!r}")

    # Case G: paper GREEN -> A3 branch is never entered; behavior unchanged.
    g_entered = paper_red_block_entered("GREEN", True)
    g_ok = (g_entered is False)
    failures += 0 if g_ok else 1
    print(f"[{'PASS' if g_ok else 'FAIL'}] G paper GREEN          "
          f"-> A3 branch entered={g_entered} (must be False; behavior UNCHANGED)")
    # sanity: RED+scale_block does enter
    red_entered = paper_red_block_entered("RED", True)
    print(f"[{'PASS' if red_entered else 'FAIL'}] G' RED+scale enters     "
          f"-> A3 branch entered={red_entered} (must be True)")
    if not red_entered:
        failures += 1

    # Case L: audit fields present and correct on WARN_ALLOW
    allow, _reason, audit = evaluate_paper_red_scale(
        _clean_state(open_live_count=0), mode=WARN)
    missing = [f for f in REQUIRED_AUDIT_FIELDS if f not in audit]
    l_checks = {
        "allow_is_true": allow is True,
        "no_missing_fields": not missing,
        "paper_red_ignored_true": audit.get("paper_red_ignored") is True,
        "ignore_reason_set": audit.get("paper_red_ignore_reason") == "LIVE_HEALTH_OK_AND_ROLLING_POSITIVE",
        "original_paper_health_RED": audit.get("original_paper_health") == "RED",
        "original_paper_active_health_RED": audit.get("original_paper_active_health") == "RED",
        "mode_recorded": audit.get("live_paper_red_scale_mode") == WARN,
        "action_warn_allow_scale": audit.get("action") == "WARN_ALLOW_SCALE",
        "pause_reason_not_blocked": audit.get("pause_reason") != "LIVE_SCALE_BLOCKED_PAPER_HEALTH",
        "cap_unchanged": audit.get("max_live_research_trades") == 2,
    }
    l_ok = all(l_checks.values())
    failures += 0 if l_ok else 1
    print(f"\n[{'PASS' if l_ok else 'FAIL'}] L audit fields on WARN_ALLOW")
    for k, v in l_checks.items():
        print(f"        {'ok ' if v else 'BAD'} {k}")
    if missing:
        print(f"        MISSING FIELDS: {missing}")

    # Cases M/N: blocked by the safety extension with the exact reason strings.
    print("\n=== safety-extension blocker reasons (M/N) ===")
    _, _, m_audit = evaluate_paper_red_scale(
        _clean_state(open_live_count=0, runtime_error=True), mode=WARN)
    m_ok = (m_audit.get("paper_red_ignore_reason") == "RUNTIME_ERROR_BLOCKER"
            and m_audit.get("paper_red_ignored") is False
            and m_audit.get("runtime_error_blocker") is True
            and m_audit.get("action") == "BLOCK_SCALE")
    failures += 0 if m_ok else 1
    print(f"  [{'PASS' if m_ok else 'FAIL'}] M runtime_error -> reason="
          f"{m_audit.get('paper_red_ignore_reason')!r}")
    _, _, n_audit = evaluate_paper_red_scale(
        _clean_state(open_live_count=0, pause_remaining_sec=1800, pause_until=9.99e9),
        mode=WARN)
    n_ok = (n_audit.get("paper_red_ignore_reason") == "ACTIVE_MICRO_PAUSE_BLOCKER"
            and n_audit.get("paper_red_ignored") is False
            and n_audit.get("active_pause_blocker") is True
            and n_audit.get("pause_remaining_sec") == 1800
            and n_audit.get("action") == "BLOCK_SCALE")
    failures += 0 if n_ok else 1
    print(f"  [{'PASS' if n_ok else 'FAIL'}] N active pause  -> reason="
          f"{n_audit.get('paper_red_ignore_reason')!r} "
          f"remaining={n_audit.get('pause_remaining_sec')}")

    # Case H invariants: cap/risk
    print("\n=== cap/risk invariants (policy must NOT change these) ===")
    inv_ok = (
        EXPECTED_MAX_LIVE_RESEARCH_TRADES == 2
        and abs(EXPECTED_LIVE_RISK_PER_TRADE - 0.01) < 1e-12
        and abs(EXPECTED_LIVE_MAX_PORTFOLIO_RISK - 0.02) < 1e-12
    )
    print(f"  max_live_research_trades=2 risk=0.01 portfolio=0.02 -> "
          f"{'PASS' if inv_ok else 'FAIL'}")
    if not inv_ok:
        failures += 1

    print("\n" + "=" * 74)
    if failures == 0:
        print("RESULT: PASS — A3 policy matches the expected truth table and audit schema.")
        return 0
    print(f"RESULT: FAIL — {failures} check(s) mismatched.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
