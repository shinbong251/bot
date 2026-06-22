import json
import os
import sys

_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
_VALID_MODES = {"paper", "testnet", "both", "live", "paper_live"}
_MODE_LIST = "'paper', 'testnet', 'live', 'both', or 'paper_live'"


def _load_execution_mode() -> str:
    try:
        from config import load_config
        cfg = load_config()
    except Exception as e:
        print(f"[MODE] FATAL: config.json unreadable: {e}")
        sys.exit(1)

    mode = cfg.get("execution_mode", "").lower().strip()
    if mode not in _VALID_MODES:
        print(
            f"[MODE] FATAL: execution_mode={mode!r} is invalid. "
            f"Must be {_MODE_LIST}. Add to config.json and restart."
        )
        sys.exit(1)

    return mode


EXECUTION_MODE = _load_execution_mode()

if EXECUTION_MODE == "paper":
    TRADES_CSV  = "paper_trades.csv"
    STATE_FILE  = "paper_state.json"
    MODE_PREFIX = "[PAPER]"
elif EXECUTION_MODE == "live":
    TRADES_CSV  = "live_trades.csv"
    STATE_FILE  = "live_state.json"
    MODE_PREFIX = "[LIVE]"
elif EXECUTION_MODE == "paper_live":
    TRADES_CSV  = "paper_trades.csv"
    STATE_FILE  = "paper_state.json"
    MODE_PREFIX = "[PAPER+LIVE]"
else:
    # testnet / both — defaults point to paper; each executor uses its own ctx paths
    TRADES_CSV  = "paper_trades.csv"
    STATE_FILE  = "paper_state.json"
    MODE_PREFIX = "[TESTNET]"


def validate_startup() -> None:
    try:
        from config import load_config
        cfg = load_config()
    except Exception as e:
        print(f"[MODE] FATAL: config.json unreadable: {e}")
        sys.exit(1)

    mode = cfg.get("execution_mode", "").lower().strip()

    if mode not in _VALID_MODES:
        print(
            f"[MODE] FATAL: execution_mode={mode!r} is invalid. "
            f"Must be {_MODE_LIST}."
        )
        sys.exit(1)

    if mode == "testnet":
        if not cfg.get("testnet_mode", False):
            print(
                "[MODE] FATAL: execution_mode=testnet "
                "but testnet_mode is not true in config.json."
            )
            sys.exit(1)
        key    = cfg.get("testnet_api_key", "")
        secret = cfg.get("testnet_api_secret", "")
        if not key or not secret:
            print(
                "[MODE] FATAL: execution_mode=testnet "
                "but testnet_api_key or testnet_api_secret missing in config.json."
            )
            sys.exit(1)

    if mode == "both":
        key    = cfg.get("testnet_api_key", "")
        secret = cfg.get("testnet_api_secret", "")
        if not key or not secret:
            print(
                "[MODE] FATAL: execution_mode=both "
                "but testnet_api_key or testnet_api_secret missing in config.json."
            )
            sys.exit(1)

    if mode in ("live", "paper_live"):
        if not cfg.get("live_mode", False):
            print(
                f"[MODE] FATAL: execution_mode={mode} "
                "but live_mode is not true in config.json."
            )
            sys.exit(1)
        key    = cfg.get("api_key", "")
        secret = cfg.get("api_secret", "")
        if not key or not secret:
            print(
                f"[MODE] FATAL: execution_mode={mode} "
                "but api_key or api_secret missing in config.json."
            )
            sys.exit(1)
        _live_cid    = cfg.get("live_chat_id", "")
        _testnet_cid = cfg.get("testnet_chat_id", "")
        _paper_cid   = cfg.get("paper_chat_id", "")
        if _live_cid and (_live_cid == _testnet_cid or _live_cid == _paper_cid):
            print(
                "[MODE] FATAL: live_chat_id must be a dedicated Telegram channel. "
                "live_chat_id must not equal testnet_chat_id or paper_chat_id."
            )
            sys.exit(1)

    print("=" * 48)
    print(f"[MODE] Execution mode : {mode.upper()}")
    if mode == "both":
        print(f"[MODE] Paper CSV      : paper_trades.csv")
        print(f"[MODE] Paper state    : paper_state.json")
        print(f"[MODE] Testnet CSV    : testnet_trades.csv")
        print(f"[MODE] Testnet state  : testnet_state.json")
    elif mode == "paper_live":
        print(f"[MODE] Paper CSV      : paper_trades.csv")
        print(f"[MODE] Paper state    : paper_state.json")
        print(f"[MODE] Live CSV       : live_trades.csv")
        print(f"[MODE] Live state     : live_state.json")
        print(f"[MODE] *** REAL MONEY AT RISK ***")
    elif mode == "live":
        print(f"[MODE] Live CSV       : live_trades.csv")
        print(f"[MODE] Live state     : live_state.json")
        print(f"[MODE] *** REAL MONEY AT RISK ***")
    else:
        print(f"[MODE] Trades CSV     : {TRADES_CSV}")
        print(f"[MODE] State file     : {STATE_FILE}")
    print("=" * 48)
