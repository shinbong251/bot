#!/usr/bin/env python3
"""
Simulator: live SL confirmation semantics (WLDUSDT WARN audit).

Reproduces the WLDUSDT-like state where:
  - local t["sl"] has been moved to BE (entry) and Telegram announced "SL -> BE",
  - the exchange stop was never price-confirmed (exchange_sl_price_confirmed is
    None / unconfirmed, or exchange_sl_id absent),
  - the position is then only protected by the LOCAL close path.

It validates, WITHOUT changing production logic, the three inconsistencies the
audit found and checks that a proposed *unified* "exchange-protected" predicate
resolves them:

  1. HEALTH predicate (signal_dispatcher `_live_research_current_sl_sync_failure`)
     treats confirmed in (None, "", False) as SL-missing.
  2. MIN-LOCK predicate (execution `_lml_unprotected`) treats confirmed in
     (None, "") as unprotected -- it does NOT include False, and is a different
     condition set living in a different module (no shared definition).
  3. MESSAGING: the BE/trailing Telegram send is emitted at the local-mutation
     site, ahead of (and independent of) the deferred exchange sync attempt, so
     "SL -> BE" can be announced while the exchange stop is unconfirmed.

Source-presence assertions guard against drift: if someone unifies the two
predicates or gates the BE Telegram on confirmation, the corresponding
"documents current behavior" check flips and this sim must be updated.

Run:
  PYTHONIOENCODING=utf-8 python3 scripts/debug/sim_live_sl_confirmation_semantics.py
"""

import os
import re
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _ROOT)

_RESULTS = []


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    _RESULTS.append((name, status, detail))
    print(f"[{status}] {name}" + (f" :: {detail}" if detail else ""))


def _read(path):
    with open(os.path.join(_ROOT, path), "r", encoding="utf-8") as fh:
        return fh.read()


# ---------------------------------------------------------------------------
# Mirrors of the two production predicates (do NOT change production logic).
# Kept byte-faithful to the source lines referenced in the audit so the cases
# below exercise exactly what production evaluates.
# ---------------------------------------------------------------------------

def health_sl_missing(trade):
    """Mirror of signal_dispatcher._live_research_current_sl_sync_failure inner
    test (per-open CONFIRM_SMC_RESEARCH bot trade). Returns True == SL-missing."""
    if not trade.get("exchange_sl_id"):
        return True
    confirmed = trade.get("exchange_sl_price_confirmed")
    if confirmed in (None, "", False):
        return True
    return False


def min_lock_unprotected(trade):
    """Mirror of execution.py `_lml_unprotected` (min-lock immediate-trigger
    guard). Returns True == treated as unprotected -> emergency alert bypass."""
    return (
        not trade.get("exchange_sl_id")
        or trade.get("exchange_sl_price_confirmed") in (None, "")
        or int(trade.get("sl_sync_fail_count") or 0) >= 3
        or bool(trade.get("entry_price_unconfirmed"))
        or trade.get("entry_state") != "ENTRY_CONFIRMED"
        or bool(trade.get("exchange_order_state_unknown"))
    )


def proposed_exchange_unprotected(trade):
    """PROPOSED unified predicate (Option B, safety-only). Single definition of
    'exchange stop is not confirmed in force' shared by health + min-lock + audit.
    Does NOT change SL values, orders, entries, risk, cap, or trailing targets --
    only makes the two views agree and treats a non-numeric confirmation as
    unprotected consistently."""
    if not trade.get("exchange_sl_id"):
        return True
    confirmed = trade.get("exchange_sl_price_confirmed")
    # only a real numeric confirmed price counts as protected
    if not isinstance(confirmed, (int, float)) or isinstance(confirmed, bool):
        return True
    if int(trade.get("sl_sync_fail_count") or 0) >= 3:
        return True
    return False


def _base_trade(**over):
    t = {
        "symbol": "WLDUSDT",
        "side": "SHORT",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "status": "OPEN",
        "owner": "bot",
        "entry": 0.4048,
        "entry_real": 0.4048,
        "sl_init": 0.417875,
        "sl": 0.4048,                      # local SL moved to BE
        "exchange_sl_id": "ALGO123",
        "exchange_sl_price_confirmed": None,   # never price-confirmed
        "sl_sync_fail_count": 0,
        "entry_price_unconfirmed": False,
        "entry_state": "ENTRY_CONFIRMED",
        "exchange_order_state_unknown": False,
    }
    t.update(over)
    return t


# ===========================================================================
# CASE 1 -- WLDUSDT actual: confirmed=None, exchange_sl_id present.
# ===========================================================================
wld = _base_trade(exchange_sl_price_confirmed=None)
h = health_sl_missing(wld)
m = min_lock_unprotected(wld)
check(
    "case1_health_flags_wld_sl_missing",
    h is True,
    "confirmed=None -> health marks SL unconfirmed (conservative)",
)
check(
    "case1_minlock_agrees_when_confirmed_None",
    m is True,
    "confirmed=None IS in (None,'') so min-lock also flags unprotected here",
)

# ===========================================================================
# CASE 2 -- Latent divergence: confirmed=False (type asymmetry).
#   health tuple  = (None, "", False)  -> missing
#   min-lock tuple= (None, "")         -> NOT unprotected
# This is the exact inconsistency called out in the audit: the two predicates
# use different membership tuples, so a False confirmation is 'missing' to
# health but 'protected' to min-lock.
# ===========================================================================
wld_false = _base_trade(exchange_sl_price_confirmed=False)
h2 = health_sl_missing(wld_false)
m2 = min_lock_unprotected(wld_false)
check(
    "case2_health_flags_false_as_missing",
    h2 is True,
    "confirmed=False -> health: SL-missing",
)
check(
    "case2_minlock_treats_false_as_protected_DIVERGENCE",
    m2 is False,
    "confirmed=False -> min-lock: protected  (PREDICATES DISAGREE)",
)
check(
    "case2_predicates_diverge_today",
    h2 != m2,
    "documents the current inconsistency; flips if predicates are unified",
)

# ===========================================================================
# CASE 3 -- No exchange stop at all (exchange_sl_id absent).
# ===========================================================================
wld_noid = _base_trade(exchange_sl_id=None, exchange_sl_price_confirmed=None)
check("case3_health_missing_no_id", health_sl_missing(wld_noid) is True)
check("case3_minlock_unprotected_no_id", min_lock_unprotected(wld_noid) is True)

# ===========================================================================
# CASE 4 -- Proposed unified predicate resolves the divergence.
# ===========================================================================
for label, tr in (
    ("confirmed_None", wld),
    ("confirmed_False", wld_false),
    ("no_exchange_id", wld_noid),
):
    check(
        f"case4_proposed_marks_unprotected_{label}",
        proposed_exchange_unprotected(tr) is True,
        "unified predicate treats every unconfirmed state as unprotected",
    )
# a genuinely confirmed numeric stop is protected under all three
confirmed_ok = _base_trade(exchange_sl_price_confirmed=0.4048)
check(
    "case4_confirmed_number_is_protected",
    health_sl_missing(confirmed_ok) is False
    and min_lock_unprotected(confirmed_ok) is False
    and proposed_exchange_unprotected(confirmed_ok) is False,
    "numeric confirmed=0.4048 -> protected in all three predicates",
)

# ===========================================================================
# CASE 5 -- Messaging must not claim exchange-confirmed BE.
# Proposed helper: choose message text from confirmation state.
# ===========================================================================
def be_message(trade, entry, symbol):
    if proposed_exchange_unprotected(trade):
        return f"{symbol} SL -> BE (local; exchange sync pending) {entry}"
    return f"{symbol} SL -> BE {entry}"


msg_unconf = be_message(wld, 0.4048, "WLDUSDT")
msg_conf = be_message(confirmed_ok, 0.4048, "WLDUSDT")
check(
    "case5_unconfirmed_message_flags_pending",
    "exchange sync pending" in msg_unconf,
    msg_unconf,
)
check(
    "case5_confirmed_message_plain_be",
    "exchange sync pending" not in msg_conf,
    msg_conf,
)

# ===========================================================================
# CASE 6 -- Fee-aware close messaging (Option D).
# WLDUSDT: SHORT entry 0.4048, exit 0.40524823903519563, 1R=0.013075.
# Raw R rounds to BE (0.0) but price + fees make it net-negative.
# ===========================================================================
entry, exitp, risk = 0.4048, 0.40524823903519563, 0.013075
raw_r = (entry - exitp) / risk          # short
price_negative = exitp > entry
check(
    "case6_raw_r_near_be_but_price_negative",
    abs(raw_r) < 0.05 and price_negative,
    f"raw_r={raw_r:.4f} exit>entry={price_negative} -> BE label hides small loss",
)

# ===========================================================================
# SOURCE-PRESENCE assertions (drift guards against the real files).
# ===========================================================================
sd = _read("signal_dispatcher.py")
ex = _read("execution.py")

check(
    "src_health_uses_none_empty_false_tuple",
    'confirmed in (None, "", False)' in sd,
    "signal_dispatcher health predicate still includes False",
)
check(
    "src_minlock_uses_none_empty_tuple_only",
    'exchange_sl_price_confirmed") in (None, "")' in ex,
    "execution _lml_unprotected still excludes False (divergence source)",
)
check(
    "src_be_telegram_sends_breakeven_move",
    '"breakeven_move"' in ex and "SL → BE (0.7R)" in ex,
    "BE Telegram send still present at local-mutation site",
)
# ordering: BE send (breakeven_move) occurs before the deferred sync gate
_be_idx = ex.find('"breakeven_move"')
_sync_gate_idx = ex.find('if t["sl"] != _sl_before_updates:')
check(
    "src_be_message_precedes_deferred_sync_gate",
    0 < _be_idx < _sync_gate_idx,
    "confirms Telegram BE is emitted ahead of the exchange sync attempt",
)

# ===========================================================================
# CASE 7 -- Applied A+D helpers (real functions from execution.py).
# Imports the module and calls ONLY the pure messaging helpers. No order,
# sync, or execution path is invoked.
# ===========================================================================
import execution  # noqa: E402  (import after path setup; pure-helper use only)

# A) exchange-confirmation wording
check(
    "A_live_unconfirmed_says_pending",
    execution._sl_exchange_confirmation_note(
        {"exchange_sl_price_confirmed": None}, "live"
    ) == "local SL updated; exchange confirmation pending",
)
check(
    "A_live_numeric_says_confirmed",
    execution._sl_exchange_confirmation_note(
        {"exchange_sl_price_confirmed": 0.4048}, "live"
    ) == "exchange SL confirmed",
)
check(
    "A_live_false_says_pending",
    execution._sl_exchange_confirmation_note(
        {"exchange_sl_price_confirmed": False}, "live"
    ) == "local SL updated; exchange confirmation pending",
    "bool False is not a numeric confirmation",
)
check(
    "A_paper_note_empty_unchanged",
    execution._sl_exchange_confirmation_note(
        {"exchange_sl_price_confirmed": None}, "paper"
    ) == "",
    "paper messages must be byte-identical (no exchange)",
)
check(
    "A_suffix_wraps_pending",
    execution._sl_confirmation_suffix(
        {"exchange_sl_price_confirmed": None}, "live"
    ) == "\n(local SL updated; exchange confirmation pending)",
)
check(
    "A_suffix_empty_for_paper",
    execution._sl_confirmation_suffix(
        {"exchange_sl_price_confirmed": None}, "paper"
    ) == "",
)

# WLD-like BE message reconstruction (mirrors the 0.7R BE send site)
_wld_be_msg = (
    "🛡️ WLDUSDT SL → BE (0.7R)\n"
    "SL → 0.4048"
    + execution._sl_confirmation_suffix({"exchange_sl_price_confirmed": None}, "live")
)
check(
    "A_wld_be_message_flags_pending",
    "exchange confirmation pending" in _wld_be_msg
    and "exchange SL confirmed" not in _wld_be_msg,
    _wld_be_msg.replace("\n", " | "),
)

# D) fee/slippage close wording
check(
    "D_live_be_close_has_fee_note",
    execution._close_fee_slippage_note({"status": "BE", "rr_real": 0}, "live")
    == "raw BE; net may be slightly negative after fees/slippage",
)
check(
    "D_live_near_zero_rr_has_fee_note",
    bool(execution._close_fee_slippage_note({"status": "LOSE", "rr_real": -0.03}, "live")),
    "|rr|<0.05 also triggers the caveat",
)
check(
    "D_live_winner_no_fee_note",
    execution._close_fee_slippage_note({"status": "WIN", "rr_real": 1.9}, "live") == "",
)
check(
    "D_paper_be_close_unchanged",
    execution._close_fee_slippage_note({"status": "BE", "rr_real": 0}, "paper") == "",
    "paper close messages unchanged (no real fills)",
)

# ===========================================================================
# CASE 8 -- Wiring + dedup-stability source-presence assertions.
# ===========================================================================
ex2 = _read("execution.py")
check(
    "wire_management_sites_use_suffix",
    ex2.count("+ _sl_confirmation_suffix(t, _exec_mode)") >= 3,
    "BE-0.7R + BE-protect + momentum-lock sends append confirmation suffix",
)
check(
    "wire_trail_uses_suffix",
    "+ _sl_confirmation_suffix(t, exec_mode)" in ex2,
    "trail-update live message appends confirmation suffix",
)
check(
    "wire_close_uses_fee_note",
    "{_ana_line}{_rr_note}{_fee_note}" in ex2,
    "live close message appends fee/slippage note",
)
# dedup/throttle keys are built from structured fields, NOT message text
check(
    "dedup_keys_not_message_derived",
    '"mgmt",' in ex2 and '"trail",' in ex2 and 'extra.get("new_sl")' in ex2,
    "mgmt/trail dedup keys use id/symbol/side/new_sl — message edits do not move them",
)
# safety: A+D touch messaging only — no B (shared predicate) / C (exchange-first)
check(
    "no_B_shared_predicate_in_minlock",
    'exchange_sl_price_confirmed") in (None, "")' in ex2,
    "min-lock _lml_unprotected tuple unchanged (B not implemented)",
)
check(
    "no_C_be_mutation_still_precedes_sync",
    ex2.find('"breakeven_move"') < ex2.find('if t["sl"] != _sl_before_updates:'),
    "BE local mutation still precedes deferred sync (C not implemented)",
)

# ---------------------------------------------------------------------------
_fails = [r for r in _RESULTS if r[1] == "FAIL"]
print("\n" + "=" * 60)
print(f"TOTAL {len(_RESULTS)}  PASS {len(_RESULTS) - len(_fails)}  FAIL {len(_fails)}")
if _fails:
    for n, _s, d in _fails:
        print(f"  FAIL {n} :: {d}")
    sys.exit(1)
print("ALL CHECKS PASSED (documents current semantics + validates proposal)")
