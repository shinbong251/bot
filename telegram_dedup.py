"""
Persistent Telegram event dedup store.

Stops the same trade-alert event from being re-sent across process restarts or
duplicate attempts. Each successfully sent event is recorded as one line in
logs/telegram_sent_events.jsonl keyed by a stable, event-specific string.

This is a defense-in-depth layer on top of the single-instance lock and the
existing in-memory dedup. It is FAIL-OPEN: any IO/parse error makes
already_sent() return False so a real alert is never wrongly suppressed by a
dedup malfunction.

Dedup keys MUST NOT include telegram_message_id or volatile timestamps.
"""

import os
import json
import time
import threading

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR = os.path.join(_BASE_DIR, "logs")
_DEDUP_FILE = os.path.join(_LOG_DIR, "telegram_sent_events.jsonl")

_LOCK = threading.Lock()
_SENT_KEYS = None  # lazy-loaded set of keys


def build_key(*parts):
    """Build a stable dedup key from event-specific parts (None/'' -> 'null')."""
    return "|".join("null" if p in (None, "") else str(p) for p in parts)


def _ensure_loaded():
    global _SENT_KEYS
    if _SENT_KEYS is not None:
        return
    keys = set()
    try:
        if os.path.exists(_DEDUP_FILE):
            with open(_DEDUP_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    k = row.get("key")
                    if k:
                        keys.add(k)
    except Exception:
        # Fail-open: never block sends because the store could not be read.
        pass
    _SENT_KEYS = keys


def already_sent(key):
    if not key:
        return False
    with _LOCK:
        _ensure_loaded()
        return key in _SENT_KEYS


def mark_sent(key, meta=None):
    """Persist a key after a successful send. No-op if already recorded."""
    if not key:
        return
    with _LOCK:
        _ensure_loaded()
        if key in _SENT_KEYS:
            return
        _SENT_KEYS.add(key)
        try:
            os.makedirs(_LOG_DIR, exist_ok=True)
            row = {"key": key, "ts": time.time(), "ts_human": time.strftime("%Y-%m-%d %H:%M:%S")}
            if isinstance(meta, dict):
                for mk in ("category", "symbol", "side", "event_type", "alert_type"):
                    if meta.get(mk) not in (None, ""):
                        row[mk] = meta[mk]
            with open(_DEDUP_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            # Fail-open: recorded in memory for this process even if disk write fails.
            pass
