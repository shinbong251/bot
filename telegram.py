import builtins
import sys

import requests
from config import config
from execution_mode import MODE_PREFIX

TOKEN = config["tele_token"]

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

PAPER_CHAT_ID   = config.get("paper_chat_id", "")
TESTNET_CHAT_ID = config.get("testnet_chat_id") or PAPER_CHAT_ID
ALERTS_CHAT_ID  = config.get("alerts_chat_id")  or PAPER_CHAT_ID
LIVE_CHAT_ID    = config.get("live_chat_id")    or ALERTS_CHAT_ID

_MOJIBAKE_MARKERS = ("\u00c3", "\u00c2", "\u00e2", "\u00f0\u0178", "\u00ef\u00b8", "\u00e1\u00ba", "\u00e1\u00bb", "\u00c4", "\u00c6")
_STRUCTURAL_TEXT_REPLACEMENTS = {
    "—": "-",
    "–": "-",
    "•": "-",
    "“": '"',
    "”": '"',
    "‘": "'",
    "’": "'",
    "…": "...",
}


def _encode_mojibake_bytes(text):
    raw = bytearray()
    for char in text:
        try:
            raw.extend(char.encode("cp1252"))
        except UnicodeEncodeError:
            codepoint = ord(char)
            if codepoint <= 255:
                raw.append(codepoint)
            else:
                return None
    return bytes(raw)


def normalize_message_text(value):
    """Return safe Unicode text for console/Telegram without touching valid strings."""
    if value is None:
        return ""

    text = str(value)
    if any(marker in text for marker in _MOJIBAKE_MARKERS):
        raw = _encode_mojibake_bytes(text)
        if raw is not None:
            try:
                repaired = raw.decode("utf-8")
                if not any(marker in repaired for marker in _MOJIBAKE_MARKERS):
                    text = repaired
            except UnicodeDecodeError:
                pass

    for bad, replacement in _STRUCTURAL_TEXT_REPLACEMENTS.items():
        text = text.replace(bad, replacement)
    return text


if not hasattr(builtins, "_bot_original_print"):
    builtins._bot_original_print = builtins.print


def _normalized_print(*args, **kwargs):
    normalized_args = tuple(normalize_message_text(arg) for arg in args)
    builtins._bot_original_print(*normalized_args, **kwargs)


builtins.print = _normalized_print


if not config.get("testnet_chat_id"):
    print("[TELE WARNING] testnet_chat_id not set in config.json - TESTNET messages will route to PAPER channel")
if not config.get("alerts_chat_id"):
    print("[TELE WARNING] alerts_chat_id not set in config.json - alerts channel will route to PAPER channel")
if not config.get("live_chat_id"):
    print("[TELE WARNING] live_chat_id not set in config.json - LIVE messages will route to ALERTS channel")
if LIVE_CHAT_ID and LIVE_CHAT_ID == TESTNET_CHAT_ID:
    print("[TELE WARNING] live_chat_id == testnet_chat_id - LIVE alerts will appear in TESTNET channel. Set a separate live_chat_id in config.json")
if LIVE_CHAT_ID and LIVE_CHAT_ID == PAPER_CHAT_ID:
    print("[TELE WARNING] live_chat_id == paper_chat_id - LIVE alerts will appear in PAPER channel. Set a separate live_chat_id in config.json")

# Future verbosity levels: "minimal", "normal", "detailed", "critical_only"
VERBOSITY = config.get("tele_verbosity", "normal")

_TESTNET_SUPPRESSED = frozenset({
    "trailing",
    "be_move",
    "phase",
    "tp_break",
    "struct_warn",
    "profit_lock",
})

_CHANNEL_CHAT_IDS = {
    "paper":   PAPER_CHAT_ID,
    "testnet": TESTNET_CHAT_ID,
    "alerts":  ALERTS_CHAT_ID,
    "live":    LIVE_CHAT_ID,
}

print(
    "[TELE CONFIG]", TOKEN[:5] if TOKEN else "EMPTY",
    f"| paper={PAPER_CHAT_ID} testnet={TESTNET_CHAT_ID} alerts={ALERTS_CHAT_ID} live={LIVE_CHAT_ID}"
)


def _resolve_channel(prefix, channel):
    if channel is not None:
        return channel
    if prefix is None:
        return None
    p = prefix.upper()
    if "[PAPER]" in p:
        return "paper"
    if "[TESTNET]" in p:
        return "testnet"
    if "[LIVE]" in p:
        return "live"
    return None


def _resolve_chat_id(channel):
    if channel is None:
        return PAPER_CHAT_ID
    return _CHANNEL_CHAT_IDS.get(channel) or PAPER_CHAT_ID


def send_telegram(msg, prefix=None, channel=None, return_metadata=False, dedup_key=None):
    metadata = {
        "ok": None,
        "error": None,
        "message_id": None,
    }
    active_prefix = normalize_message_text(prefix if prefix is not None else MODE_PREFIX)
    msg = normalize_message_text(msg)
    resolved_channel = _resolve_channel(active_prefix, channel)
    chat_id = _resolve_chat_id(resolved_channel)

    if not chat_id:
        print("[TELE SKIP] No chat_id configured")
        metadata["ok"] = False
        metadata["error"] = "no_chat_id_configured"
        return metadata if return_metadata else None

    if dedup_key is not None:
        try:
            from telegram_dedup import already_sent
            if already_sent(dedup_key):
                print(f"[TELEGRAM_DEDUP_SUPPRESSED] {dedup_key}")
                metadata["ok"] = None
                metadata["error"] = None
                metadata["suppressed"] = True
                metadata["suppress_reason"] = "telegram_dedup"
                return metadata if return_metadata else None
        except Exception:
            # Fail-open: dedup failure must never block a real alert.
            pass

    prefixed = active_prefix + "\n" + msg
    print("[TELEGRAM]:", prefixed)
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": prefixed,
        "parse_mode": "HTML",
    }
    try:
        r = requests.post(url, json=payload, timeout=(4, 10))
        print(f"[TELE DEBUG] sent OK ch={resolved_channel or 'default'}", r.status_code, r.text)
        metadata["ok"] = bool(r.ok)
        if not r.ok:
            metadata["error"] = f"http_status_{r.status_code}"
        try:
            response_payload = r.json()
            if isinstance(response_payload, dict):
                if response_payload.get("ok") is False:
                    metadata["ok"] = False
                    metadata["error"] = str(
                        response_payload.get("description") or "telegram_api_error"
                    )
                result = response_payload.get("result")
                if isinstance(result, dict):
                    metadata["message_id"] = result.get("message_id")
        except Exception:
            pass
    except Exception as e:
        print("[TELE ERROR]", e)
        metadata["ok"] = False
        metadata["error"] = f"{type(e).__name__}: {e}"

    if dedup_key is not None and metadata.get("ok"):
        try:
            from telegram_dedup import mark_sent
            mark_sent(dedup_key, {"category": resolved_channel})
        except Exception:
            pass

    return metadata if return_metadata else None


def send_telegram_gated(msg, prefix=None, channel=None, category=None, return_metadata=False, dedup_key=None):
    msg = normalize_message_text(msg)
    if category is not None and channel is None:
        active_prefix = normalize_message_text(prefix if prefix is not None else MODE_PREFIX)
        resolved = _resolve_channel(active_prefix, channel)
        if resolved == "testnet" and category in _TESTNET_SUPPRESSED:
            print(f"[TELE QUIET testnet/{category}]", msg[:80])
            if return_metadata:
                return {
                    "ok": None,
                    "error": None,
                    "message_id": None,
                    "gated": True,
                    "suppressed": True,
                    "suppress_reason": f"testnet_category_suppressed:{category}",
                }
            return None
    if not return_metadata:
        send_telegram(msg, prefix=prefix, channel=channel, dedup_key=dedup_key)
        return None
    metadata = send_telegram(
        msg,
        prefix=prefix,
        channel=channel,
        return_metadata=True,
        dedup_key=dedup_key,
    )
    if return_metadata and isinstance(metadata, dict):
        metadata.update({
            "gated": True,
            "suppressed": False,
            "suppress_reason": None,
        })
    return metadata


def test_telegram():
    message = "[TEST] BOT CONNECTED"
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    try:
        resp = requests.post(url, data={"chat_id": PAPER_CHAT_ID, "text": message})
        if resp.status_code == 200:
            print("TELEGRAM SUCCESS -- message sent")
        else:
            print(f"TELEGRAM FAIL -- status: {resp.status_code}")
            print(f"TELEGRAM FAIL -- response: {resp.text}")
    except Exception as e:
        print(f"TELEGRAM FAIL -- exception: {e}")
