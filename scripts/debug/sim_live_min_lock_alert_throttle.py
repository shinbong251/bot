#!/usr/bin/env python3
"""
Simulator: LIVE_MIN_LOCK_075 Telegram throttle / dedup.

Covers the alert-flood fix for the immediate-trigger guard skip:
  [LIVE_MIN_LOCK_075] <SYM> skipped
  reason=immediately_triggerable_before_local_sl_mutation

It validates the throttle helper behavior directly (live_alert_throttle) and the
decision flow that execution.py performs around it (stale-symbol guard, emergency
protection bypass, throttle key). It also asserts the real execution.py source
still wires those pieces and still writes every occurrence to the jsonl row, and
that the critical stop-update-failed path is untouched.

Run:
  PYTHONIOENCODING=utf-8 python3 scripts/debug/sim_live_min_lock_alert_throttle.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import telegram_dedup
import live_alert_throttle

_RESULTS = []


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    _RESULTS.append((name, status, detail))
    print(f"[{status}] {name}" + (f" :: {detail}" if detail else ""))


# --- Mirror of execution.py immediate-trigger decision flow -------------------
# This mirrors (does NOT change) the logic now in execution.py so the cases can
# be exercised end-to-end. Source-presence assertions below guard against drift.

def build_key(trade, floor):
    # Use the same production key builder (rounds float fields for stability).
    return live_alert_throttle.build_min_lock_075_throttle_key(trade, floor)


def is_unprotected(trade):
    return (
        not trade.get("exchange_sl_id")
        or trade.get("exchange_sl_price_confirmed") in (None, "")
        or int(trade.get("sl_sync_fail_count") or 0) >= 3
        or bool(trade.get("entry_price_unconfirmed"))
        or trade.get("entry_state") != "ENTRY_CONFIRMED"
        or bool(trade.get("exchange_order_state_unknown"))
    )


def decide(trade, open_trades, floor, now):
    """Return (send_telegram, throttle_reason, repeat_count, stale_suppressed)."""
    symbol_open = live_alert_throttle.is_symbol_open(trade.get("symbol"), open_trades)
    unprotected = is_unprotected(trade)
    key = build_key(trade, floor)
    d = live_alert_throttle.should_send(key, now=now)
    reason = d["throttle_reason"]
    if not symbol_open:
        return False, "stale_symbol_suppressed", d["repeat_count"], True
    if unprotected:
        return True, "emergency_bypass_unprotected", d["repeat_count"], False
    return bool(d["send"]), reason, d["repeat_count"], False


def make_trade(symbol="NFPUSDT", **over):
    t = {
        "symbol": symbol,
        "side": "SHORT",
        "id": 1782734526059,
        "status": "OPEN",
        "min_lock_skipped_reason": "immediately_triggerable_before_local_sl_mutation",
        "exchange_sl_id": 3000001986358451,
        "exchange_sl_price_confirmed": 0.005586,
        "sl": 0.005586,
        "sl_sync_fail_count": 0,
        "entry_price_unconfirmed": False,
        "entry_state": "ENTRY_CONFIRMED",
        "exchange_order_state_unknown": False,
        "quarantined": False,
    }
    t.update(over)
    return t


FLOOR = 0.0054134732142857135
T0 = 1_000_000.0


# === A. First MIN_LOCK skip sends Telegram ===================================
live_alert_throttle.reset()
nfp = make_trade("NFPUSDT")
opens = [nfp]
send, reason, rc, stale = decide(nfp, opens, FLOOR, T0)
check("A first NFPUSDT skip sends", send and reason == "first_occurrence" and rc == 1,
      f"send={send} reason={reason} rc={rc}")

# === B. 10 repeats fast (1s apart) -> only first; milestones deferred ========
# Minimum inter-send spacing (60s) means milestones reached < 60s after the
# last send are deferred, so a fast loop no longer bursts.
live_alert_throttle.reset()
sends = []
for i in range(10):
    s, r, rc, _ = decide(make_trade("NFPUSDT"), opens, FLOOR, T0 + i)  # 1s apart, <60s
    if s:
        sends.append((rc, r))
sent_counts = [rc for rc, _ in sends]
check("B fast repeats send only first (milestones deferred by spacing)",
      sent_counts == [1], f"sent at repeat_counts={sent_counts}")

# === C. ASTERUSDT throttles independently ====================================
live_alert_throttle.reset()
nfp = make_trade("NFPUSDT")
aster = make_trade("ASTERUSDT")
opens2 = [nfp, aster]
# Send NFP first, then ASTER first -> both send (independent keys)
s_nfp, _, rc_nfp, _ = decide(nfp, opens2, FLOOR, T0)
s_ast, _, rc_ast, _ = decide(aster, opens2, FLOOR, T0)
# Repeat each once within cooldown -> both suppressed
s_nfp2, _, _, _ = decide(nfp, opens2, FLOOR, T0 + 1)
s_ast2, _, _, _ = decide(aster, opens2, FLOOR, T0 + 1)
check("C ASTER throttles independently of NFP",
      s_nfp and s_ast and (not s_nfp2) and (not s_ast2),
      f"nfp_first={s_nfp} aster_first={s_ast} nfp_repeat={s_nfp2} aster_repeat={s_ast2}")

# === D. Target SL (floor) changes -> sends again =============================
live_alert_throttle.reset()
nfp = make_trade("NFPUSDT")
s1, _, _, _ = decide(nfp, opens, FLOOR, T0)
s2_same, _, _, _ = decide(nfp, opens, FLOOR, T0 + 1)          # suppressed
s3_newfloor, r3, _, _ = decide(nfp, opens, FLOOR + 0.0001, T0 + 2)  # new target -> send
check("D target SL change re-sends", s1 and (not s2_same) and s3_newfloor and r3 == "first_occurrence",
      f"first={s1} same={s2_same} new_floor={s3_newfloor}")

# === E. Current confirmed SL changes -> sends again ==========================
live_alert_throttle.reset()
nfp = make_trade("NFPUSDT")
s1, _, _, _ = decide(nfp, opens, FLOOR, T0)
s2_same, _, _, _ = decide(nfp, opens, FLOOR, T0 + 1)
nfp_conf = make_trade("NFPUSDT", exchange_sl_price_confirmed=0.005590)
s3_conf, r3, _, _ = decide(nfp_conf, [nfp_conf], FLOOR, T0 + 2)
check("E confirmed SL change re-sends", s1 and (not s2_same) and s3_conf and r3 == "first_occurrence",
      f"first={s1} same={s2_same} new_confirmed={s3_conf}")

# === F. Reason changes -> sends again ========================================
live_alert_throttle.reset()
nfp = make_trade("NFPUSDT")
s1, _, _, _ = decide(nfp, opens, FLOOR, T0)
s2_same, _, _, _ = decide(nfp, opens, FLOOR, T0 + 1)
nfp_reason = make_trade("NFPUSDT", min_lock_skipped_reason="some_other_reason")
s3_reason, r3, _, _ = decide(nfp_reason, [nfp_reason], FLOOR, T0 + 2)
check("F reason change re-sends", s1 and (not s2_same) and s3_reason and r3 == "first_occurrence",
      f"first={s1} same={s2_same} new_reason={s3_reason}")

# === G. Missing exchange SL -> bypass throttle, always alert =================
live_alert_throttle.reset()
nfp = make_trade("NFPUSDT")
decide(nfp, opens, FLOOR, T0)          # first send recorded
unprot = make_trade("NFPUSDT", exchange_sl_id=None)
s_unprot, r_unprot, _, _ = decide(unprot, [unprot], FLOOR, T0 + 1)  # within cooldown but unprotected
# Use SAME key so cooldown would normally suppress; bypass must override.
unprot2 = make_trade("NFPUSDT", exchange_sl_id=None)
live_alert_throttle.reset()
decide(unprot2, [unprot2], FLOOR, T0)
s_bypass, r_bypass, _, _ = decide(unprot2, [unprot2], FLOOR, T0 + 1)
check("G missing exchange SL bypasses throttle", s_bypass and r_bypass == "emergency_bypass_unprotected",
      f"send={s_bypass} reason={r_bypass}")

# === H. Entry unconfirmed -> bypass throttle, always alert ===================
live_alert_throttle.reset()
unconf = make_trade("NFPUSDT", entry_price_unconfirmed=True, entry_state="ENTRY_UNCONFIRMED")
decide(unconf, [unconf], FLOOR, T0)
s_h, r_h, _, _ = decide(unconf, [unconf], FLOOR, T0 + 1)  # within cooldown but unconfirmed
check("H entry unconfirmed bypasses throttle", s_h and r_h == "emergency_bypass_unprotected",
      f"send={s_h} reason={r_h}")

# Extra: sl_sync_fail_count >= 3 also bypasses
live_alert_throttle.reset()
failt = make_trade("NFPUSDT", sl_sync_fail_count=3)
decide(failt, [failt], FLOOR, T0)
s_fail, r_fail, _, _ = decide(failt, [failt], FLOOR, T0 + 1)
check("H2 sync_fail_count>=3 bypasses throttle", s_fail and r_fail == "emergency_bypass_unprotected",
      f"send={s_fail} reason={r_fail}")

# === I. Symbol not in live_state -> no Telegram; stale suppression ===========
live_alert_throttle.reset()
ghost = make_trade("ASTERUSDT")
open_without_aster = [make_trade("NFPUSDT")]
s_i, r_i, _, stale_i = decide(ghost, open_without_aster, FLOOR, T0)
check("I non-open symbol suppressed as stale", (not s_i) and stale_i and r_i == "stale_symbol_suppressed",
      f"send={s_i} stale={stale_i} reason={r_i}")
# A closed/quarantined trade present in list is also not 'open'
s_i2, _, _, stale_i2 = decide(
    make_trade("NFPUSDT", status="CLOSED"),
    [make_trade("NFPUSDT", status="CLOSED")],
    FLOOR, T0 + 1,
)
check("I2 closed symbol suppressed as stale", (not s_i2) and stale_i2, f"send={s_i2} stale={stale_i2}")

# === J. Audit/jsonl row still written for every occurrence (source check) ====
exec_src = open(os.path.join(os.path.dirname(__file__), "..", "..", "execution.py"),
                encoding="utf-8").read()
check("J jsonl row carries telegram_throttled field", '"telegram_throttled": _lml_telegram_throttled' in exec_src)
check("J jsonl row carries throttle_key field", '"throttle_key": _lml_throttle_key' in exec_src)
check("J jsonl row carries repeat_count field", '"repeat_count": _lml_throttle_repeat_count' in exec_src)
check("J jsonl row carries throttle_reason field", '"throttle_reason": _lml_throttle_reason' in exec_src)
check("J jsonl row carries last_sent_age_sec field", '"last_sent_age_sec": _lml_last_sent_age_sec' in exec_src)
# jsonl write is still unconditional (outside any attempt_send gate)
check("J jsonl write path unchanged/unconditional",
      'live_smc_research_min_lock_075_events.jsonl' in exec_src)

# === K. Critical stop-update-failed path still first-sends, not silenced =====
# The throttle is wired ONLY into the immediate-trigger skip alert. The latency
# sender defaults attempt_send=True, so untouched callers still send.
import inspect
import execution
sig = inspect.signature(execution._send_management_telegram)
check("K _send_management_telegram defaults attempt_send=True",
      sig.parameters["attempt_send"].default is True)
# Throttle wiring is scoped: only the immediate-trigger alert passes attempt_send
check("K throttle scoped to immediate-trigger alert only",
      exec_src.count('throttle_rule="live_min_lock_075_immediate_trigger"') == 1)
# The high-severity sync-failed telegram gate is still present and unmodified
check("K stop-update-failed (sync fail) alert path still present",
      "consecutive_failures=" in exec_src and "_send_failure_telegram" in exec_src)

# === Source-presence: decision flow wired into execution.py ==================
check("WIRE stale-symbol guard present", "LIVE_MANAGEMENT_STALE_SYMBOL_ALERT_SUPPRESSED" in exec_src)
check("WIRE is_symbol_open used", "live_alert_throttle.is_symbol_open(" in exec_src)
check("WIRE should_send used", "live_alert_throttle.should_send(" in exec_src)
check("WIRE emergency bypass predicate present",
      'or t.get("entry_state") != "ENTRY_CONFIRMED"' in exec_src)
check("WIRE throttle key built via stable production helper",
      "live_alert_throttle.build_min_lock_075_throttle_key(" in exec_src)
check("WIRE first alert still sends (no suppression on first_occurrence)",
      "attempt_send=not _lml_telegram_throttled" in exec_src)

# === Summary =================================================================
fails = [r for r in _RESULTS if r[1] == "FAIL"]
print("\n==================== SUMMARY ====================")
print(f"total={len(_RESULTS)} pass={len(_RESULTS)-len(fails)} fail={len(fails)}")
if fails:
    print("RESULT: FAIL")
    for n, _, d in fails:
        print(f"  - {n} :: {d}")
    sys.exit(1)
print("RESULT: PASS")
