#!/usr/bin/env python3
"""
Simulator: LIVE_MIN_LOCK_075 throttle *runtime wiring*.

Proves that the throttle decision is actually honored on the real send path:
  A. attempt_send=False does NOT call the Telegram sender.
  B. The latency/audit row is still written when suppressed (with
     attempted_send=false, telegram_suppressed=true, suppress_reason, throttle_key).
  C. The same NFP MIN_LOCK skip repeated 20x produces first + milestones only.
  D. throttle_key is stable despite tiny float-formatting differences.
  E. Protected NFP (exchange_sl_id + confirmed SL) does NOT emergency-bypass.
  F. Unprotected (missing SL) still bypasses throttle.
  G. Stale ASTER not in live_state -> stale suppression, no Telegram.

This exercises the REAL execution._send_management_telegram with the Telegram
sender + latency writer monkeypatched to capture calls (no network, no orders).

Run:
  PYTHONIOENCODING=utf-8 python3 scripts/debug/sim_live_min_lock_alert_throttle_runtime_wiring.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import live_alert_throttle
import execution

_RESULTS = []


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    _RESULTS.append((name, status, detail))
    print(f"[{status}] {name}" + (f" :: {detail}" if detail else ""))


def make_trade(symbol="NFPUSDT", **over):
    t = {
        "symbol": symbol, "side": "SHORT", "id": 1782734526059, "status": "OPEN",
        "min_lock_skipped_reason": "immediately_triggerable_before_local_sl_mutation",
        "exchange_sl_id": 3000001986358451, "exchange_sl_price_confirmed": 0.005586,
        "sl": 0.005586, "sl_init": 0.0058160357142857146, "entry_real": 0.005586,
        "sl_sync_fail_count": 0, "entry_price_unconfirmed": False,
        "entry_state": "ENTRY_CONFIRMED", "exchange_order_state_unknown": False,
        "quarantined": False,
    }
    t.update(over)
    return t


FLOOR = 0.0054134732142857135


# --- Mirror of execution.py emergency-bypass predicate (drift-guarded below) --
def is_unprotected(trade):
    return (
        not trade.get("exchange_sl_id")
        or trade.get("exchange_sl_price_confirmed") in (None, "")
        or int(trade.get("sl_sync_fail_count") or 0) >= 3
        or bool(trade.get("entry_price_unconfirmed"))
        or trade.get("entry_state") != "ENTRY_CONFIRMED"
        or bool(trade.get("exchange_order_state_unknown"))
    )


# --- Capture harness: replace real Telegram senders + latency writer ----------
_sender_calls = []
_latency_rows = []


def _fake_send_telegram(*a, **k):
    _sender_calls.append(("send_telegram", a, k))
    return {"ok": True, "message_id": 1, "error": None}


def _fake_send_telegram_gated(*a, **k):
    _sender_calls.append(("send_telegram_gated", a, k))
    return {"ok": True, "message_id": 1, "error": None, "gated": True, "suppressed": False}


def _fake_latency_write(row):
    _latency_rows.append(row)


execution.send_telegram = _fake_send_telegram
execution.send_telegram_gated = _fake_send_telegram_gated
execution._telegram_management_latency_write = _fake_latency_write


def call_send(t, attempt_send, suppressed, suppress_reason, throttle_key):
    ctx = {
        "update_trades_started_ts": 1.0, "trade_loop_id": "L", "symbol_loop_index": 0,
        "price_meta": {}, "current_price_used_in_decision": 0.0055,
        "message_price_source": "test", "old_sl": t["sl"], "new_sl": t["sl"],
        "proposed_sl": FLOOR, "event_detected_ts": 123.0,
    }
    return execution._send_management_telegram(
        t,
        f"[LIVE_MIN_LOCK_075] {t['symbol']} skipped\n"
        "reason=immediately_triggerable_before_local_sl_mutation",
        "min_lock_075_skipped", "[LIVE]", "live", category="profit_lock",
        management_context=ctx, attempt_send=attempt_send, suppressed=suppressed,
        suppress_reason=suppress_reason,
        throttle_rule="live_min_lock_075_immediate_trigger", throttle_key=throttle_key,
    )


# === A. attempt_send=False does not call Telegram sender =====================
_sender_calls.clear(); _latency_rows.clear()
key = live_alert_throttle.build_min_lock_075_throttle_key(make_trade(), FLOOR)
call_send(make_trade(), attempt_send=False, suppressed=True,
          suppress_reason="within_cooldown", throttle_key=key)
check("A attempt_send=False -> Telegram sender NOT called", len(_sender_calls) == 0,
      f"sender_calls={len(_sender_calls)}")

# === B. latency/audit row still written when suppressed ======================
row = _latency_rows[-1] if _latency_rows else {}
check("B latency row written when suppressed", len(_latency_rows) == 1)
check("B row attempted_send=false", row.get("attempted_send") is False, f"attempted_send={row.get('attempted_send')}")
check("B row telegram_suppressed=true", row.get("telegram_suppressed") is True, f"telegram_suppressed={row.get('telegram_suppressed')}")
check("B row suppress_reason carried", row.get("suppress_reason") == "within_cooldown", f"suppress_reason={row.get('suppress_reason')}")
check("B row throttle_key carried", row.get("throttle_key") == key, f"throttle_key={row.get('throttle_key')}")
check("B row no send timing (no API)", row.get("telegram_send_start_ts") is None and row.get("telegram_send_done_ts") is None)

# Sanity: attempt_send=True DOES call sender and writes a row.
_sender_calls.clear(); _latency_rows.clear()
call_send(make_trade(), attempt_send=True, suppressed=False, suppress_reason=None, throttle_key=key)
check("B2 attempt_send=True -> sender called + row written",
      len(_sender_calls) == 1 and len(_latency_rows) == 1, f"calls={len(_sender_calls)}")

# === C. every 2s for 100s -> NO 6-message burst (min spacing) ================
# 50 occurrences, 2s apart. Milestones 3/5/10/20 land < 60s after the first send
# and are deferred; milestone_50 lands at ~98s (>= 60s spacing) and sends.
live_alert_throttle.reset()
T0 = 1_000_000.0
sent = []
deferred = []
for i in range(50):
    d = live_alert_throttle.should_send(key, now=T0 + 2 * i)
    if d["send"]:
        sent.append((d["repeat_count"], d["throttle_reason"]))
    elif d["throttle_reason"] == "milestone_deferred_by_min_spacing":
        deferred.append(d["repeat_count"])
check("C 2s/100s does NOT send 6 messages", len(sent) < 6, f"sent={sent}")
check("C 2s/100s sends only first + spacing-eligible milestone_50",
      sent == [(1, "first_occurrence"), (50, "milestone_50")], f"sent={sent}")
check("C early milestones deferred by spacing", deferred == [3, 5, 10, 20],
      f"deferred={deferred}")

# === C2. milestone reached < 60s -> deferred with audit fields ===============
live_alert_throttle.reset()
live_alert_throttle.should_send(key, now=T0)            # first send
live_alert_throttle.should_send(key, now=T0 + 2)        # count 2
d_def = live_alert_throttle.should_send(key, now=T0 + 4)  # count 3 (milestone) at +4s
check("C2 milestone <60s deferred (not sent)", d_def["send"] is False, f"send={d_def['send']}")
check("C2 deferred reason", d_def["throttle_reason"] == "milestone_deferred_by_min_spacing",
      f"reason={d_def['throttle_reason']}")
check("C2 deferred audit: milestone field", d_def["milestone"] == 3, f"milestone={d_def['milestone']}")
check("C2 deferred audit: min_send_spacing_sec=60", d_def["min_send_spacing_sec"] == 60,
      f"spacing={d_def['min_send_spacing_sec']}")
check("C2 deferred audit: last_sent_age_sec", d_def["last_sent_age_sec"] == 4.0,
      f"age={d_def['last_sent_age_sec']}")

# === C3. milestone reached >= 60s -> sends =====================================
live_alert_throttle.reset()
live_alert_throttle.should_send(key, now=T0)            # first send at T0
# advance occurrences cheaply to reach count 3 at >= 60s after last send
live_alert_throttle.should_send(key, now=T0 + 10)       # count 2
d_send = live_alert_throttle.should_send(key, now=T0 + 70)  # count 3 (milestone) at +70s
check("C3 milestone >=60s sends", d_send["send"] is True and d_send["throttle_reason"] == "milestone_3",
      f"send={d_send['send']} reason={d_send['throttle_reason']}")

# === C4. first_occurrence + cooldown unaffected by spacing ====================
live_alert_throttle.reset()
d_first = live_alert_throttle.should_send(key, now=T0)
d_cool = None
# many fast repeats (all deferred/within-cooldown), then > 5 min later
for i in range(1, 6):
    live_alert_throttle.should_send(key, now=T0 + i)
d_cool = live_alert_throttle.should_send(key, now=T0 + 400)  # > cooldown
check("C4 first sends immediately", d_first["send"] is True and d_first["throttle_reason"] == "first_occurrence")
check("C4 cooldown_elapsed still sends (>=5min)",
      d_cool["send"] is True and d_cool["throttle_reason"] == "cooldown_elapsed",
      f"reason={d_cool['throttle_reason']}")

# === C5. emergency/unprotected bypass ignores spacing ========================
# Caller sends immediately when unprotected, regardless of should_send deferral.
live_alert_throttle.reset()
unprot = make_trade("NFPUSDT", exchange_sl_id=None)
ukey = live_alert_throttle.build_min_lock_075_throttle_key(unprot, FLOOR)
live_alert_throttle.should_send(ukey, now=T0)           # first
d_bypass = live_alert_throttle.should_send(ukey, now=T0 + 4)  # would be within_cooldown
# mirror caller: unprotected -> send True even though decision said False
caller_send = is_unprotected(unprot) or d_bypass["send"]
check("C5 unprotected bypasses spacing (sends)", caller_send is True and is_unprotected(unprot),
      f"decision_send={d_bypass['send']} unprotected={is_unprotected(unprot)}")

# === D. stable throttle_key despite tiny float-formatting differences ========
live_alert_throttle.reset()
t1 = make_trade("NFPUSDT", sl=0.005586, exchange_sl_price_confirmed=0.005586)
# Same values reconstructed through arithmetic that yields float jitter.
jitter_sl = (0.005586 * 3) / 3            # mathematically 0.005586, may differ in last bits
jitter_floor = FLOOR + 1e-15              # sub-precision jitter
t2 = make_trade("NFPUSDT", sl=jitter_sl, exchange_sl_price_confirmed=0.005586)
k1 = live_alert_throttle.build_min_lock_075_throttle_key(t1, FLOOR)
k2 = live_alert_throttle.build_min_lock_075_throttle_key(t2, jitter_floor)
check("D key stable under float jitter", k1 == k2, f"k1=={k1!r}\n        k2=={k2!r}")
# And a REAL SL change still produces a different key (so re-send happens).
k3 = live_alert_throttle.build_min_lock_075_throttle_key(
    make_trade("NFPUSDT", sl=0.005586), FLOOR + 0.0001)
check("D real floor change still changes key", k1 != k3)
# repeat_count increments for the identical (jittered) spam loop.
live_alert_throttle.reset()
d_a = live_alert_throttle.should_send(k1, now=T0)
d_b = live_alert_throttle.should_send(k2, now=T0 + 1)
check("D identical-jitter loop increments repeat_count",
      d_a["repeat_count"] == 1 and d_b["repeat_count"] == 2 and not d_b["send"],
      f"rc1={d_a['repeat_count']} rc2={d_b['repeat_count']} send2={d_b['send']}")


# === E. protected NFP does NOT emergency-bypass ==============================
nfp = make_trade("NFPUSDT", exchange_sl_sync_pending=FLOOR)  # pending set by skip itself
check("E protected NFP -> emergency_bypass FALSE", is_unprotected(nfp) is False,
      f"unprotected={is_unprotected(nfp)} (sync_pending set but SL confirmed)")

# === F. unprotected (missing SL) still bypasses ==============================
check("F missing exchange_sl_id -> unprotected TRUE",
      is_unprotected(make_trade("NFPUSDT", exchange_sl_id=None)) is True)
check("F missing confirmed SL -> unprotected TRUE",
      is_unprotected(make_trade("NFPUSDT", exchange_sl_price_confirmed=None)) is True)

# === G. stale ASTER not in live_state -> stale suppression, no Telegram ======
opens_no_aster = [make_trade("NFPUSDT")]
check("G ASTER not open -> is_symbol_open FALSE",
      live_alert_throttle.is_symbol_open("ASTERUSDT", opens_no_aster) is False)
# And the wired path logs stale suppression + sends no telegram (source check).
exec_src = open(os.path.join(os.path.dirname(__file__), "..", "..", "execution.py"),
                encoding="utf-8").read()
check("G stale-symbol suppression wired (no telegram on stale)",
      "LIVE_MANAGEMENT_STALE_SYMBOL_ALERT_SUPPRESSED" in exec_src
      and "stale_symbol_suppressed" in exec_src)

# --- Drift guards: production wiring still present ----------------------------
check("WIRE bypass predicate matches mirror",
      'or t.get("entry_state") != "ENTRY_CONFIRMED"' in exec_src
      and 'int(t.get("sl_sync_fail_count") or 0) >= 3' in exec_src)
check("WIRE exchange_sl_sync_pending NOT a bypass trigger",
      "exchange_sl_sync_pending" not in exec_src.split("_lml_unprotected = (")[1].split(")")[0])
check("WIRE production uses stable key helper",
      "live_alert_throttle.build_min_lock_075_throttle_key(" in exec_src)
check("WIRE attempt_send forwarded to sender",
      "attempt_send=not _lml_telegram_throttled" in exec_src)
check("WIRE latency row gains attempted_send/throttle_key",
      '"attempted_send": bool(attempt_send)' in exec_src and '"throttle_key": throttle_key' in exec_src)
check("WIRE audit row carries milestone + min_send_spacing_sec",
      '"milestone": _lml_throttle_milestone' in exec_src
      and '"min_send_spacing_sec": _lml_min_send_spacing_sec' in exec_src)
check("WIRE execution captures milestone/spacing from decision",
      '_lml_throttle_decision.get("milestone")' in exec_src
      and '_lml_throttle_decision.get("min_send_spacing_sec")' in exec_src)

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
