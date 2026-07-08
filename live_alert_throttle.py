"""
In-process throttle/dedup for repetitive *informational* live-management alerts.

Purpose
-------
Some live-management skip alerts (e.g. LIVE_MIN_LOCK_075 immediate-trigger
guard) are emitted every management scan while the guard condition persists.
The underlying safety decision is correct; only the Telegram flood is a defect.

Policy
------
- The FIRST occurrence of a key always sends.
- Identical repeats are suppressed for `cooldown_sec` (default 300s = 5 min).
- A repeat sends again when its repeat count crosses a milestone
  (default 3, 5, 10, 20, 50) or once the cooldown window elapses.
- Any change in the key (symbol/side/reason/confirmed SL/local SL/target SL/
  trade id/severity) produces a brand-new key, which sends as a first
  occurrence. That is how "target SL changed", "confirmed SL changed",
  "reason changed", "severity changed" all force a re-send.

Design notes
------------
- State is intentionally IN-PROCESS only. A fresh process starts empty, so a
  restart can never silently swallow the first alert for a key. This matches
  the requirement "send first occurrence immediately".
- State is module-level but guarded by a lock and partitioned by key, so it is
  not a fragile shared-mutable global leaking across unrelated callers.
- This helper makes NO send/IO decisions itself and never touches orders. It
  only returns a decision dict; the caller performs the send and logging.
"""

import threading
import time

import telegram_dedup

_LOCK = threading.Lock()
_STATE = {}  # key -> {"first_ts": float, "last_sent_ts": float, "count": int}

DEFAULT_COOLDOWN_SEC = 300
DEFAULT_MILESTONES = (3, 5, 10, 20, 50)
DEFAULT_MIN_SEND_SPACING_SEC = 60


def should_send(key, now=None, cooldown_sec=DEFAULT_COOLDOWN_SEC, milestones=DEFAULT_MILESTONES,
                min_send_spacing_sec=DEFAULT_MIN_SEND_SPACING_SEC):
    """Record one occurrence of `key` and decide whether Telegram should send.

    Returns a dict:
      send (bool)              -- True if this occurrence should be Telegram'd
      repeat_count (int)       -- 1-based count including this occurrence
      throttle_reason (str)    -- why we did/didn't send
      last_sent_age_sec (float|None) -- age of previous send (None on first)
      milestone (int|None)     -- milestone hit by this occurrence, else None
      min_send_spacing_sec (int) -- minimum wall-clock gap enforced on milestones

    Minimum spacing: a milestone send is gated by `min_send_spacing_sec` so a
    high-frequency loop cannot turn the milestone schedule (3/5/10/20/50) into a
    rapid burst. A milestone reached sooner than that gap is deferred (NOT sent),
    while first_occurrence and cooldown_elapsed are unaffected. Emergency/
    unprotected bypass is decided by the caller and ignores this entirely.
    """
    if now is None:
        now = time.time()
    if not key:
        # No stable key -> never throttle (fail-open).
        return {
            "send": True,
            "repeat_count": 1,
            "throttle_reason": "no_key_fail_open",
            "last_sent_age_sec": None,
            "milestone": None,
            "min_send_spacing_sec": min_send_spacing_sec,
        }
    with _LOCK:
        st = _STATE.get(key)
        if st is None:
            _STATE[key] = {"first_ts": now, "last_sent_ts": now, "count": 1}
            return {
                "send": True,
                "repeat_count": 1,
                "throttle_reason": "first_occurrence",
                "last_sent_age_sec": None,
                "milestone": None,
                "min_send_spacing_sec": min_send_spacing_sec,
            }
        st["count"] += 1
        count = st["count"]
        last_sent_age = now - st["last_sent_ts"]
        if count in milestones:
            if last_sent_age >= min_send_spacing_sec:
                st["last_sent_ts"] = now
                return {
                    "send": True,
                    "repeat_count": count,
                    "throttle_reason": f"milestone_{count}",
                    "last_sent_age_sec": round(last_sent_age, 3),
                    "milestone": count,
                    "min_send_spacing_sec": min_send_spacing_sec,
                }
            # Milestone reached before the minimum spacing -> defer (do not send).
            # last_sent_ts is intentionally left unchanged.
            return {
                "send": False,
                "repeat_count": count,
                "throttle_reason": "milestone_deferred_by_min_spacing",
                "last_sent_age_sec": round(last_sent_age, 3),
                "milestone": count,
                "min_send_spacing_sec": min_send_spacing_sec,
            }
        if last_sent_age >= cooldown_sec:
            st["last_sent_ts"] = now
            return {
                "send": True,
                "repeat_count": count,
                "throttle_reason": "cooldown_elapsed",
                "last_sent_age_sec": round(last_sent_age, 3),
                "milestone": None,
                "min_send_spacing_sec": min_send_spacing_sec,
            }
        return {
            "send": False,
            "repeat_count": count,
            "throttle_reason": "within_cooldown",
            "last_sent_age_sec": round(last_sent_age, 3),
            "milestone": None,
            "min_send_spacing_sec": min_send_spacing_sec,
        }


def round_key_value(value, ndigits=10):
    """Round a float used inside a throttle key to a safe fixed precision.

    Throttle keys must be byte-stable across loops so the same persisting skip
    maps to one key (so repeat_count increments and the cooldown applies).
    Rounding to a fixed precision absorbs any tiny float-formatting jitter while
    staying far finer than any real SL move, so a genuine SL change still yields
    a new key (and therefore a fresh send). Non-numeric values pass through.
    """
    try:
        return round(float(value), ndigits)
    except (TypeError, ValueError):
        return value


def build_min_lock_075_throttle_key(trade, target_floor, ndigits=10):
    """Stable throttle/dedup key for the LIVE_MIN_LOCK_075 immediate-trigger skip.

    Key parts: event_type, symbol, side, reason, confirmed exchange SL, local
    SL, target/min-lock SL, trade id. Float fields are rounded so identical
    spam loops collapse to one key; any salient change (reason / confirmed SL /
    local SL / target floor) produces a new key and forces a re-send.
    """
    return telegram_dedup.build_key(
        "LIVE_MIN_LOCK_075",
        trade.get("symbol"),
        trade.get("side"),
        trade.get("min_lock_skipped_reason"),
        round_key_value(trade.get("exchange_sl_price_confirmed"), ndigits),
        round_key_value(trade.get("sl"), ndigits),
        round_key_value(target_floor, ndigits),
        trade.get("id"),
    )


def is_symbol_open(symbol, open_trades):
    """Return True if `symbol` is present as an OPEN trade in `open_trades`.

    Used by the stale-symbol guard: a live-management alert about a symbol that
    is not currently OPEN must not be Telegram-spammed.
    """
    if not symbol or not open_trades:
        return False
    for tr in open_trades:
        if not isinstance(tr, dict):
            continue
        if tr.get("symbol") != symbol:
            continue
        if tr.get("status", "OPEN") == "OPEN" and not tr.get("quarantined"):
            return True
    return False


def reset(key=None):
    """Clear throttle state (test helper)."""
    with _LOCK:
        if key is None:
            _STATE.clear()
        else:
            _STATE.pop(key, None)
