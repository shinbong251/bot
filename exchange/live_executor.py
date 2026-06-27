"""
LIVE (MAINNET) order execution module for COIN BOT.

SAFE BOUNDARIES — this module MUST NOT:
  - Call any testnet endpoint (https://testnet.binancefuture.com)
  - Execute when live_mode != true in config.json
  - Execute without api_key and api_secret set in config.json
  - Import from execution.py, entry.py, main.py, or state_manager.py

Role:
  Execute validated MARKET + STOP_MARKET orders against
  Binance Futures MAINNET with tiny-risk deployment parameters.

Mainnet URL : https://fapi.binance.com  ← ONLY endpoint used by this module
Testnet URL : https://testnet.binancefuture.com  ← NEVER CALLED FROM THIS MODULE

Config keys required:
  live_mode          bool   — must be true to allow any write operation
  api_key            str    — MAINNET API key
  api_secret         str    — MAINNET API secret
  execution_balance  float  — authoritative capital for ALL sizing / risk / margin
                              (must be set explicitly; no default assumed)

Public API:
  get_execution_balance(wallet_balance=None)  → float | None
  log_startup_mode(wallet_balance=None)
  validate_and_prepare(symbol, side, entry, sl, tp, balance, risk_percent)
  place_market_order(symbol, side, qty, leverage, client_order_id=None)
  place_stop_loss(symbol, entry_side, qty, stop_price, client_order_id=None)
  query_order(symbol, order_id=None, client_order_id=None)
  cancel_stop_loss(symbol, order_id)
  update_trailing_stop(symbol, entry_side, qty, new_stop_price, old_order_id)
  get_exchange_positions()
  is_position_closed(symbol)
  compare_local_vs_exchange(local_positions, exchange_positions)
  get_open_algo_orders(symbol)
  check_live_safety_gate(symbol, entry_type, exhaustion, bos_type, concurrent_trades, risk_percent)

Balance flow:
  get_execution_balance()
      ↓  (return value passed by caller)
  validate_and_prepare(..., balance=execution_balance, ...)
      ↓
  calculate_execution_plan(..., balance, ...)
      ↓
  qty = risk_amount / sl_distance
  risk_amount = execution_balance × risk_percent
  margin = notional / leverage

  The actual loss if SL hits = qty × sl_distance ≈ execution_balance × risk_percent
  NOT exchange_wallet_balance × risk_percent.

LIVE SAFETY POSTURE:
  Allowed entry types  : CONFIRM only
  Allowed exhaustion   : HEALTHY only
  Allowed BOS types    : NEAR only
  Max concurrent trades: config.max_live_trades
  Max risk percent     : config.live_risk_per_trade
  Symbol policy        : well-formed USDT futures symbols only, no TIER5
"""

import hashlib
import hmac
import json
import os
import re
import time
import urllib.parse
import uuid

import certifi
import requests
from decimal import Decimal, ROUND_DOWN, InvalidOperation

from .execution_policy import (
    calculate_execution_plan,
    get_max_leverage,
    get_min_acceptable_leverage,
    get_symbol_tier,
)
from .precision import round_price, get_symbol_filters

# =====================================================================
# CONSTANTS
# =====================================================================

_LIVE_BASE_URL = "https://fapi.binance.com"
_TIMEOUT       = (5, 15)   # (connect, read) — slightly wider for write ops
_MAX_RETRY_GET = 3         # GET queries may retry safely
_RETRY_DELAYS  = [0.5, 1.0, 2.0]

_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "config.json"
)
_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs")
_RUNTIME_LOG_PATH = os.path.join(_LOG_DIR, "runtime_errors.log")
_LEVERAGE_BRACKET_CACHE: dict = {}

# =====================================================================
# LIVE SAFETY GATE CONSTANTS
# =====================================================================

_LIVE_ALLOWED_ENTRY_TYPES  = {"CONFIRM"}
_LIVE_ALLOWED_EXHAUSTION   = {"HEALTHY"}
_LIVE_ALLOWED_BOS_TYPES    = {"NEAR"}
_LIVE_SYMBOL_RE            = re.compile(r"^[A-Z0-9]{2,20}USDT$")

# Separate allowlist for the live research gate — does NOT overlap with CONFIRM gate.
_LIVE_RESEARCH_ALLOWED_TYPES = {"CONFIRM_SMC_RESEARCH"}

# =====================================================================
# CONFIG LOADER
# =====================================================================

def _load_config() -> dict:
    try:
        from config import load_config
        return load_config()
    except Exception as e:
        print(f"[LIVE] config load error: {e}")
        return {}


def _get_live_max_concurrent() -> int:
    cfg = _load_config()
    raw = cfg.get("max_live_trades", 3)
    try:
        max_live = int(raw)
    except (TypeError, ValueError):
        print(f"[LIVE] invalid max_live_trades={raw!r}; using disabled limit 0")
        return 0
    return max(0, max_live)


def _get_live_max_research_trades() -> int:
    cfg = _load_config()
    raw = cfg.get("max_live_research_trades", 1)
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 1


def _count_live_research_open(open_trades, exclude_trade=None) -> int:
    if not open_trades:
        return 0
    count = 0
    for t in open_trades:
        if exclude_trade is not None and t is exclude_trade:
            continue
        if t.get("status") == "OPEN" and (
            t.get("entry_type") == "CONFIRM_SMC_RESEARCH"
            or t.get("strategy_family") == "confirm_smc_research"
        ):
            count += 1
    return count


def _log_leverage_diagnostic(symbol: str, stage: str, reason: str, **fields) -> None:
    """
    Durable diagnostics for LIVE exchange-max leverage lookup failures.

    Kept local to avoid importing state_manager from the live executor.
    """
    row = {
        "ts": int(time.time()),
        "scope": "live_leverage",
        "symbol": (symbol or "").upper(),
        "action": "leverageBracket",
        "stage": stage,
        "reason": reason,
    }
    row.update(fields)
    try:
        os.makedirs(_LOG_DIR, exist_ok=True)
        with open(_RUNTIME_LOG_PATH, "a", encoding="utf-8") as f:
            f.write("[LIVE_LEVERAGE_DIAG] " + json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception:
        pass


def _get_live_risk_per_trade() -> tuple[float | None, str | None]:
    cfg = _load_config()
    raw = cfg.get("live_risk_per_trade")
    try:
        risk = float(raw)
    except (TypeError, ValueError):
        return None, "live_risk_per_trade malformed in config.json"
    if risk <= 0:
        return None, f"live_risk_per_trade={risk} must be > 0"
    return risk, None


def _get_live_max_portfolio_risk() -> tuple[float | None, str | None]:
    cfg = _load_config()
    raw = cfg.get("live_max_portfolio_risk")
    try:
        risk = float(raw)
    except (TypeError, ValueError):
        return None, "live_max_portfolio_risk malformed in config.json"
    if risk <= 0:
        return None, f"live_max_portfolio_risk={risk} must be > 0"
    return risk, None


def _use_exchange_max_leverage() -> bool:
    cfg = _load_config()
    return bool(cfg.get("use_exchange_max_leverage", True))


def _leverage_cache_ttl_secs() -> int:
    cfg = _load_config()
    raw = cfg.get("leverage_cache_ttl_secs", 21600)
    try:
        ttl = int(raw)
    except (TypeError, ValueError):
        return 21600
    return max(0, ttl)


def _live_symbol_is_well_formed(symbol: str) -> bool:
    return bool(_LIVE_SYMBOL_RE.fullmatch(symbol or ""))


def _live_symbol_is_tier5(symbol: str) -> bool:
    try:
        return get_symbol_tier(symbol) == "TIER5"
    except Exception:
        return False


def _normalize_live_bos_type(value) -> str:
    raw = str(value or "").upper().strip()
    if raw.startswith("BOS:"):
        raw = raw.split(":", 1)[1]
    if raw.startswith("BOS_"):
        raw = raw.split("_", 1)[1]
    return raw


def get_execution_balance(wallet_balance: float = None) -> float | None:
    """
    Return the authoritative execution balance from config.json.

    This is the ONLY sanctioned source of balance for all execution logic.
    All callers (validate_and_prepare, build_execution_payload, etc.) must
    receive balance from this function, not from the raw exchange wallet.

    Why a separate config key instead of the exchange wallet:
      The live execution capital is intentionally small (e.g. $50) for
      initial deployment. Using the wallet balance directly would produce
      unrealistic position sizes relative to the intended risk.

    Args:
      wallet_balance: optional — if provided, a warning is logged when
                      execution_balance exceeds available wallet balance.
                      Pass the result of get_account_balance() here.

    Returns:
      float  — the configured execution_balance
      None   — if execution_balance is missing or invalid in config.json

    Invariant guaranteed by this function:
      risk_amount = execution_balance × risk_percent
      qty × sl_distance ≈ execution_balance × risk_percent
      NOT wallet_balance × risk_percent
    """
    cfg = _load_config()
    raw = cfg.get("execution_balance")

    if raw is None:
        print(
            "[EXECUTION] execution_balance not set in config.json. "
            "Add \"execution_balance\": <float> to config.json before trading."
        )
        return None

    try:
        eb = float(raw)
    except (TypeError, ValueError):
        print(f"[EXECUTION] execution_balance is not a valid number: {raw!r}")
        return None

    if eb <= 0:
        print(f"[EXECUTION] execution_balance must be > 0, got {eb}")
        return None

    if wallet_balance is not None:
        try:
            wb = float(wallet_balance)
            if eb > wb:
                print(
                    f"[EXECUTION] WARNING: execution_balance ${eb:.2f} exceeds "
                    f"exchange wallet balance ${wb:.2f}. "
                    "Position sizing will be correct but margin may be unavailable."
                )
        except (TypeError, ValueError):
            pass

    return eb


def _require_live() -> tuple:
    """
    Guard called at the start of every write operation.

    Returns (enabled: bool, api_key: str, api_secret: str).
    Returns (False, "", "") on any guard failure — caller must abort.

    This is the primary safety gate. It is impossible to reach any
    POST endpoint in this module without passing this check.

    Reads:
      live_mode   (bool)  — must be true
      api_key     (str)   — mainnet API key
      api_secret  (str)   — mainnet API secret
    """
    cfg = _load_config()

    if not cfg.get("live_mode", False):
        print(
            "[LIVE] BLOCKED — live_mode is not true in config.json. "
            "Set \"live_mode\": true before using live execution."
        )
        return False, "", ""

    key    = cfg.get("api_key", "")
    secret = cfg.get("api_secret", "")

    if not key or not secret:
        print(
            "[LIVE] BLOCKED — api_key or api_secret "
            "missing in config.json."
        )
        return False, "", ""

    return True, key, secret


# =====================================================================
# LIVE SAFETY GATE
# =====================================================================

def check_live_safety_gate(
    symbol:           str,
    entry_type:       str,
    exhaustion:       str,
    bos_type:         str,
    concurrent_trades: int,
    risk_percent:     float,
    current_portfolio_risk: float = 0.0,
    leverage:         int | None = None,
) -> tuple:
    """
    Hard safety gate enforcing initial live deployment restrictions.

    All parameters must pass ALL checks before live execution is allowed.

    Returns (allowed: bool, reason: str).

    Hard limits enforced:
      symbol         must be a well-formed USDT futures symbol
      entry_type     must be CONFIRM
      exhaustion     must be HEALTHY
      bos_type       must be NEAR
      execution tier must not be TIER5
      concurrent     must be < config.max_live_trades
      risk_percent   must be <= config.live_risk_per_trade
      portfolio risk must remain <= config.live_max_portfolio_risk
      leverage       when provided must be positive; in fallback tier mode it
                     must also fit the symbol tier cap
    """
    sym = (symbol or "").upper()
    if not _live_symbol_is_well_formed(sym):
        return False, f"symbol {sym!r} malformed for live execution"

    et = (entry_type or "").upper()
    if et not in _LIVE_ALLOWED_ENTRY_TYPES:
        return False, f"entry_type {et!r} not in live allowed set {_LIVE_ALLOWED_ENTRY_TYPES}"

    ex = (exhaustion or "").upper()
    if not ex:
        return False, "missing exhaustion — rejected in live safety gate"
    if ex not in _LIVE_ALLOWED_EXHAUSTION:
        return False, f"exhaustion {ex!r} not in live allowed set {_LIVE_ALLOWED_EXHAUSTION}"

    bt = _normalize_live_bos_type(bos_type)
    if bt not in _LIVE_ALLOWED_BOS_TYPES:
        return False, f"bos_type {bt!r} not in live allowed set {_LIVE_ALLOWED_BOS_TYPES}"

    if _live_symbol_is_tier5(sym):
        return False, f"execution_tier='TIER5' excluded from live"

    max_live = _get_live_max_concurrent()
    try:
        concurrent = int(concurrent_trades)
    except (TypeError, ValueError):
        return False, f"concurrent_trades malformed: {concurrent_trades!r}"
    if concurrent < 0:
        return False, f"concurrent_trades malformed: {concurrent_trades!r}"
    if concurrent >= max_live:
        return False, f"concurrent_trades={concurrent} >= live max {max_live}"

    try:
        rp = float(risk_percent)
    except (TypeError, ValueError):
        return False, f"risk_percent malformed: {risk_percent!r}"
    if rp <= 0:
        return False, f"risk_percent={rp} must be > 0"
    live_risk_cap, live_risk_error = _get_live_risk_per_trade()
    if live_risk_error:
        return False, live_risk_error
    if rp > live_risk_cap:
        return False, f"risk_percent={rp} > live_risk_per_trade {live_risk_cap}"

    try:
        cur_risk = float(current_portfolio_risk or 0.0)
    except (TypeError, ValueError):
        return False, f"current_portfolio_risk malformed: {current_portfolio_risk!r}"
    if cur_risk < 0:
        return False, f"current_portfolio_risk malformed: {cur_risk}"
    portfolio_cap, portfolio_error = _get_live_max_portfolio_risk()
    if portfolio_error:
        return False, portfolio_error
    if cur_risk + rp > portfolio_cap:
        return False, f"portfolio_risk={cur_risk + rp} > live cap {portfolio_cap}"

    if leverage is not None:
        try:
            lev = int(leverage)
        except (TypeError, ValueError):
            return False, f"leverage malformed: {leverage!r}"
        if lev <= 0:
            return False, f"leverage={lev} must be > 0"
        if _use_exchange_max_leverage():
            return True, "OK"
        cap = get_max_leverage(sym)
        if lev > cap:
            return False, f"leverage {lev}x exceeds tier cap {cap}x for {sym}"

    return True, "OK"


# =====================================================================
# LIVE RESEARCH SAFETY GATE (CONFIRM_SMC_RESEARCH only)
# =====================================================================

def check_live_research_safety_gate(
    trade: dict,
    ctx=None,
    open_trades: list = None,
    exclude_trade: dict = None,
) -> tuple:
    """
    Research-specific live safety gate for CONFIRM_SMC_RESEARCH trades only.

    Separate from check_live_safety_gate() — does NOT require exhaustion_cls
    or bos_type (research trades do not carry those fields).

    Returns (allowed: bool, reason: str).

    Hard limits enforced:
      live_mode              must be true in config
      live_smc_research_enabled must be true in config
      execution_mode         must be live or paper_live
      entry_type             must be CONFIRM_SMC_RESEARCH
      symbol                 must be well-formed USDT futures, not TIER5
      side                   must be LONG or SHORT
      entry, sl, tp          must be present and numeric and positive
      SL geometry            LONG: sl < entry < tp; SHORT: tp < entry < sl
      planned_rr             must be >= 2.0 (from rr field or computed)
      bos_quality            if present, must not be WEAK
      volume_confirmation    if present, must not be EXPANSION
      open live research     must be < max_live_research_trades
    """
    cfg = _load_config()

    if not cfg.get("live_mode", False):
        return False, "live_mode is not true — live research blocked"
    if not cfg.get("live_smc_research_enabled", False):
        return False, "live_smc_research_enabled is not true — live research blocked"

    exec_mode = getattr(ctx, "execution_mode", None) if ctx is not None else None
    if exec_mode not in ("live", "paper_live"):
        return False, f"execution_mode={exec_mode!r} not in (live, paper_live)"

    et = (trade.get("entry_type") or "").upper()
    if et not in _LIVE_RESEARCH_ALLOWED_TYPES:
        return False, (
            f"entry_type {et!r} not in live research allowed set "
            f"{_LIVE_RESEARCH_ALLOWED_TYPES}"
        )

    sym = (trade.get("symbol") or "").upper()
    if not _live_symbol_is_well_formed(sym):
        return False, f"symbol {sym!r} malformed for live execution"
    if _live_symbol_is_tier5(sym):
        return False, "execution_tier='TIER5' excluded from live"

    side = (trade.get("side") or "").upper()
    if side not in ("LONG", "SHORT"):
        return False, f"side {side!r} must be LONG or SHORT"

    try:
        entry = float(trade.get("entry_real") or trade.get("entry"))
        if entry <= 0:
            raise ValueError(f"entry={entry} non-positive")
    except (TypeError, ValueError) as _e:
        return False, f"entry missing or invalid: {_e}"

    try:
        sl = float(trade.get("sl"))
        if sl <= 0:
            raise ValueError(f"sl={sl} non-positive")
    except (TypeError, ValueError) as _e:
        return False, f"sl missing or invalid: {_e}"

    try:
        tp = float(trade.get("tp"))
        if tp <= 0:
            raise ValueError(f"tp={tp} non-positive")
    except (TypeError, ValueError) as _e:
        return False, f"tp missing or invalid: {_e}"

    if side == "LONG":
        if not (sl < entry < tp):
            return False, (
                f"LONG geometry violated: required sl < entry < tp, "
                f"got sl={sl} entry={entry} tp={tp}"
            )
    else:
        if not (tp < entry < sl):
            return False, (
                f"SHORT geometry violated: required tp < entry < sl, "
                f"got tp={tp} entry={entry} sl={sl}"
            )

    rr_raw = trade.get("rr")
    try:
        rr_val = float(rr_raw) if rr_raw is not None else None
    except (TypeError, ValueError):
        rr_val = None
    if rr_val is None:
        sl_dist = abs(entry - sl)
        tp_dist = abs(tp - entry)
        if sl_dist <= 0:
            return False, "cannot compute RR — sl_dist=0"
        rr_val = tp_dist / sl_dist
    if rr_val < 2.0:
        return False, f"planned_rr={rr_val:.2f} < 2.0 required for live research"

    bos_quality = (trade.get("bos_quality") or "").upper()
    if bos_quality == "WEAK":
        return False, "bos_quality=WEAK rejected in live research gate"

    vol_conf = (trade.get("volume_confirmation") or "").upper()
    if vol_conf == "EXPANSION":
        return False, "volume_confirmation=EXPANSION rejected in live research gate"

    _ot = open_trades if open_trades is not None else (
        getattr(ctx, "trades", None) or []
    )
    max_research = _get_live_max_research_trades()
    open_research = _count_live_research_open(_ot, exclude_trade=exclude_trade)
    if open_research >= max_research:
        return False, (
            f"live_research_open={open_research} >= "
            f"max_live_research_trades={max_research}"
        )

    return True, "OK"


# =====================================================================
# HMAC-SHA256 SIGNING
# =====================================================================

def _sign(params: dict, api_secret: str) -> str:
    """
    Return HMAC-SHA256 hex-digest of the URL-encoded param string.

    Kept separate from binance_client._sign intentionally:
    the read-only client and the write client must never share
    a code path that could accidentally mix mainnet/testnet state.
    """
    query = urllib.parse.urlencode(params)
    return hmac.new(
        api_secret.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


# =====================================================================
# REQUEST WRAPPERS — LIVE MAINNET ONLY
# =====================================================================

def _post(path: str, params: dict) -> dict | None:
    """
    Signed POST to Binance Futures MAINNET only.

    CRITICAL — RETRY POLICY:
      This function does NOT retry on failure.
      Reason: a POST timeout does NOT mean the order was rejected.
      Binance may have accepted the order after the TCP connection
      dropped on our side. Blindly retrying would create duplicate
      positions. The caller must call query_order() before any retry.

    Returns:
      dict — parsed JSON response (may contain error code)
      None — network failure or timeout (order state UNKNOWN)
    """
    ok, api_key, api_secret = _require_live()
    if not ok:
        return None

    params = dict(params)
    params["timestamp"]  = int(time.time() * 1000)
    params["recvWindow"] = 5000
    params["signature"]  = _sign(params, api_secret)

    headers = {"X-MBX-APIKEY": api_key}
    url     = _LIVE_BASE_URL + path

    try:
        res = requests.post(
            url,
            params=params,
            headers=headers,
            verify=certifi.where(),
            timeout=_TIMEOUT,
        )

        if res.status_code == 429:
            wait = float(res.headers.get("Retry-After", 5.0))
            print(f"[LIVE] Rate limit on POST {path} — back off {wait}s before any retry")
            return None

        if res.status_code == 418:
            print("[LIVE] IP banned (418) on LIVE — halt all requests")
            return None

        if res.status_code == 401:
            print(f"[LIVE] Auth error (401) — check api_key / api_secret")
            return None

        data = res.json()

        if res.status_code != 200:
            code = data.get("code", "?") if isinstance(data, dict) else "?"
            msg  = data.get("msg",  res.text[:200]) if isinstance(data, dict) else res.text[:200]
            print(f"[LIVE] POST {path} HTTP {res.status_code} — code={code} msg={msg}")
            return data

        return data

    except requests.exceptions.Timeout:
        # A POST timeout is NOT a confirmed rejection.
        # The order request may have reached Binance before the connection timed out.
        # Treat as UNKNOWN state — caller must query before any retry.
        print(
            f"[LIVE] TIMEOUT on POST {path}. "
            "Order status is UNKNOWN — do NOT retry without calling query_order() first."
        )
        return None

    except Exception as e:
        print(f"[LIVE] Unexpected error on POST {path}: {e}")
        return None


def _get_signed(path: str, params: dict) -> dict | list | None:
    """
    Signed GET to Binance Futures MAINNET.

    Used for order queries and position reads.
    Retries are safe for GET — idempotent reads only.
    """
    ok, api_key, api_secret = _require_live()
    if not ok:
        return None

    base_params = dict(params)
    diag_symbol = base_params.get("symbol", "")
    is_leverage_bracket = path == "/fapi/v1/leverageBracket"

    headers = {"X-MBX-APIKEY": api_key}
    url     = _LIVE_BASE_URL + path

    _RETRYABLE = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.ReadTimeout,
        requests.exceptions.ConnectTimeout,
    )

    for attempt in range(_MAX_RETRY_GET):
        signed_params = dict(base_params)
        signed_params["timestamp"]  = int(time.time() * 1000)
        signed_params["recvWindow"] = 5000
        signed_params["signature"]  = _sign(signed_params, api_secret)
        try:
            res = requests.get(
                url,
                params=signed_params,
                headers=headers,
                verify=certifi.where(),
                timeout=_TIMEOUT,
            )

            if res.status_code == 429:
                wait = float(res.headers.get("Retry-After", _RETRY_DELAYS[attempt]))
                print(f"[LIVE] Rate limit on GET {path} — waiting {wait}s")
                if is_leverage_bracket:
                    _log_leverage_diagnostic(
                        diag_symbol,
                        "bracket_refresh",
                        "rate_limited",
                        attempt=attempt + 1,
                        http_status=res.status_code,
                        cache_hit="unknown",
                    )
                time.sleep(wait)
                continue

            if res.status_code != 200:
                print(f"[LIVE] GET {path} HTTP {res.status_code}: {res.text[:200]}")
                if is_leverage_bracket:
                    code = ""
                    msg = res.text[:200]
                    try:
                        err_data = res.json()
                        if isinstance(err_data, dict):
                            code = err_data.get("code", "")
                            msg = err_data.get("msg", msg)
                    except Exception:
                        pass
                    _log_leverage_diagnostic(
                        diag_symbol,
                        "bracket_refresh",
                        "http_error",
                        attempt=attempt + 1,
                        http_status=res.status_code,
                        binance_code=code,
                        binance_msg=msg,
                        cache_hit="unknown",
                    )
                return None

            return res.json()

        except _RETRYABLE as e:
            print(
                f"[LIVE] Network error on GET {path} "
                f"attempt {attempt+1}/{_MAX_RETRY_GET}: {type(e).__name__}"
            )
            if is_leverage_bracket:
                _log_leverage_diagnostic(
                    diag_symbol,
                    "bracket_refresh",
                    "network_error",
                    attempt=attempt + 1,
                    exception_type=type(e).__name__,
                    exception_msg=str(e),
                    cache_hit="unknown",
                )
            if attempt < _MAX_RETRY_GET - 1:
                time.sleep(_RETRY_DELAYS[attempt])

        except Exception as e:
            print(f"[LIVE] Unexpected error on GET {path}: {e}")
            if is_leverage_bracket:
                _log_leverage_diagnostic(
                    diag_symbol,
                    "bracket_refresh",
                    "unexpected_exception",
                    attempt=attempt + 1,
                    exception_type=type(e).__name__,
                    exception_msg=str(e),
                    cache_hit="unknown",
                )
            return None

    print(f"[LIVE] All {_MAX_RETRY_GET} GET attempts failed for {path}")
    if is_leverage_bracket:
        _log_leverage_diagnostic(
            diag_symbol,
            "bracket_refresh",
            "all_attempts_failed",
            attempt=_MAX_RETRY_GET,
            cache_hit="unknown",
        )
    return None


def _delete(path: str, params: dict) -> dict | None:
    """
    Signed DELETE to Binance Futures MAINNET.

    Used exclusively for cancelling orders.
    Not retried on failure — cancel state must be confirmed by caller
    via query_order() if needed.

    Returns:
      dict — parsed JSON response (may contain error code)
      None — network failure or timeout (cancel state UNKNOWN)
    """
    ok, api_key, api_secret = _require_live()
    if not ok:
        return None

    params = dict(params)
    params["timestamp"]  = int(time.time() * 1000)
    params["recvWindow"] = 5000
    params["signature"]  = _sign(params, api_secret)

    headers = {"X-MBX-APIKEY": api_key}
    url     = _LIVE_BASE_URL + path

    try:
        res = requests.delete(
            url,
            params=params,
            headers=headers,
            verify=certifi.where(),
            timeout=_TIMEOUT,
        )

        if res.status_code == 429:
            wait = float(res.headers.get("Retry-After", 5.0))
            print(f"[LIVE] Rate limit on DELETE {path} — back off {wait}s")
            return None

        if res.status_code == 418:
            print("[LIVE] IP banned (418) on LIVE — halt all requests")
            return None

        data = res.json()

        if res.status_code != 200:
            code = data.get("code", "?") if isinstance(data, dict) else "?"
            msg  = data.get("msg",  res.text[:200]) if isinstance(data, dict) else res.text[:200]
            print(f"[LIVE] DELETE {path} HTTP {res.status_code} — code={code} msg={msg}")
            return data

        return data

    except requests.exceptions.Timeout:
        print(
            f"[LIVE] TIMEOUT on DELETE {path}. "
            "Cancel state UNKNOWN — query order to confirm."
        )
        return None

    except Exception as e:
        print(f"[LIVE] Unexpected error on DELETE {path}: {e}")
        return None


# =====================================================================
# LEVERAGE & MARGIN TYPE SETUP
# =====================================================================

def _get_leverage_brackets(symbol: str, force_refresh: bool = False) -> tuple:
    """
    Return Binance leverage brackets for a LIVE symbol.

    Returns (brackets: list | None, source: str, reason: str | None).
    Source is one of: bracket_cache, bracket_refresh, cache_fallback, lookup_failed.
    """
    sym = (symbol or "").upper()
    ttl = _leverage_cache_ttl_secs()
    now = time.time()
    cached = _LEVERAGE_BRACKET_CACHE.get(sym)
    cache_hit = bool(cached and cached.get("brackets"))

    if cached and not force_refresh and ttl > 0 and now - cached.get("ts", 0) <= ttl:
        return cached.get("brackets"), "bracket_cache", None

    data = _get_signed("/fapi/v1/leverageBracket", {"symbol": sym})
    if isinstance(data, list) and data:
        entry = data[0]
        brackets = entry.get("brackets") if isinstance(entry, dict) else None
        if isinstance(brackets, list) and brackets:
            _LEVERAGE_BRACKET_CACHE[sym] = {"brackets": brackets, "ts": now}
            return brackets, "bracket_refresh", None
        _log_leverage_diagnostic(
            sym,
            "bracket_refresh",
            "malformed_bracket_entry",
            cache_hit=cache_hit,
            force_refresh=force_refresh,
            response_type=type(entry).__name__,
            response_symbol=entry.get("symbol", "") if isinstance(entry, dict) else "",
        )
    else:
        _log_leverage_diagnostic(
            sym,
            "bracket_refresh",
            "empty_or_unusable_response",
            cache_hit=cache_hit,
            force_refresh=force_refresh,
            response_type=type(data).__name__,
        )

    if cached and cached.get("brackets"):
        _log_leverage_diagnostic(
            sym,
            "cache_fallback",
            "bracket_refresh_failed",
            cache_hit=True,
            force_refresh=force_refresh,
        )
        return cached.get("brackets"), "cache_fallback", "bracket_refresh_failed"

    _log_leverage_diagnostic(
        sym,
        "lookup_failed",
        "leverage_max_unknown",
        cache_hit=False,
        force_refresh=force_refresh,
    )
    return None, "lookup_failed", "leverage_max_unknown"


def _max_leverage_from_brackets(brackets: list, notional: float | None = None) -> int | None:
    if not isinstance(brackets, list) or not brackets:
        return None

    selected = None
    if notional is not None:
        try:
            n = float(notional)
        except (TypeError, ValueError):
            n = None
        if n is not None:
            for bracket in brackets:
                try:
                    floor = float(bracket.get("notionalFloor", 0))
                    cap = float(bracket.get("notionalCap", float("inf")))
                except (TypeError, ValueError):
                    continue
                if floor <= n <= cap:
                    selected = bracket
                    break
            if selected is None:
                selected = brackets[-1]

    if selected is not None:
        try:
            return int(selected.get("initialLeverage"))
        except (TypeError, ValueError):
            return None

    values = []
    for bracket in brackets:
        try:
            values.append(int(bracket.get("initialLeverage")))
        except (TypeError, ValueError):
            continue
    return max(values) if values else None


def _get_exchange_max_leverage(symbol: str, notional: float | None = None, force_refresh: bool = False) -> tuple:
    """
    Return (exchange_max: int | None, source: str, reason: str | None).
    When notional is available, the max is selected from the matching bracket.
    """
    brackets, source, reason = _get_leverage_brackets(symbol, force_refresh=force_refresh)
    if not brackets:
        return None, source, reason or "leverage_max_unknown"
    max_lev = _max_leverage_from_brackets(brackets, notional=notional)
    if max_lev is None or max_lev <= 0:
        _log_leverage_diagnostic(
            symbol,
            source,
            "leverage_max_unknown",
            cache_hit=source in ("bracket_cache", "cache_fallback"),
            notional=notional,
            bracket_count=len(brackets) if isinstance(brackets, list) else "",
            detail="missing_or_invalid_initialLeverage",
        )
        return None, source, "leverage_max_unknown"
    return max_lev, source, reason


def _apply_exchange_max_leverage_to_plan(symbol: str, plan: dict) -> tuple:
    """
    Replace tier-target LIVE leverage with Binance exchange max leverage.
    Returns (ok: bool, reason: str | None, plan: dict).
    """
    notional = plan.get("notional")
    exchange_max, source, reason = _get_exchange_max_leverage(symbol, notional=notional)
    if exchange_max is None:
        print(
            f"[LEVERAGE] {symbol} max leverage lookup failed - "
            f"rejecting entry reason=leverage_max_unknown"
        )
        _log_leverage_diagnostic(
            symbol,
            source,
            "leverage_max_unknown",
            cache_hit=source in ("bracket_cache", "cache_fallback"),
            notional=notional,
            required_leverage=plan.get("required_leverage"),
            final_reason="validate_and_prepare_reject",
        )
        plan["leverage_mode"] = "exchange_max"
        plan["leverage_source"] = source
        plan["exchange_max_leverage"] = None
        return False, "leverage_max_unknown", plan

    required = plan.get("required_leverage")
    try:
        required_lev = int(required)
    except (TypeError, ValueError):
        required_lev = 1
    if required_lev > exchange_max:
        reason = f"leverage infeasible: need {required_lev}x but exchange max is {exchange_max}x"
        print(f"[LEVERAGE] {symbol} mode=exchange_max exchange_max={exchange_max} applied=None source={source} reason=required_gt_exchange_max")
        plan["leverage_mode"] = "exchange_max"
        plan["leverage_source"] = source
        plan["exchange_max_leverage"] = exchange_max
        return False, reason, plan

    applied = exchange_max
    margin = float(notional) / applied if notional is not None else plan.get("margin_required")
    plan["target_leverage"] = applied
    plan["allowed_leverage"] = applied
    plan["final_leverage"] = applied
    plan["margin_required"] = round(margin, 4) if margin is not None else margin
    plan["leverage_clamp_reason"] = None
    plan["leverage_mode"] = "exchange_max"
    plan["leverage_source"] = source
    plan["exchange_max_leverage"] = exchange_max
    print(
        f"[LEVERAGE] {symbol} mode=exchange_max exchange_max={exchange_max} "
        f"applied={applied} source={source}"
    )
    return True, None, plan


def _set_leverage(symbol: str, leverage: int, detailed: bool = False) -> tuple:
    """
    Set leverage for symbol on LIVE account via POST /fapi/v1/leverage.

    Called before every entry order. If leverage is already at the
    requested level, Binance returns success — safe to call repeatedly.

    Returns (success: bool, confirmed_leverage: int | None).
      confirmed_leverage is the value Binance actually applied —
      may differ from requested when the subaccount or symbol has
      a lower cap enforced by the exchange.

    Error code -4421: subaccount capped below requested leverage.
      → returns (False, None), Telegram warning sent, trade aborted.
    """
    print(f"[LIVE] Setting leverage {leverage}x for {symbol}")

    data = _post("/fapi/v1/leverage", {
        "symbol":   symbol,
        "leverage": leverage,
    })

    if data is None:
        print(f"[LIVE] _set_leverage: POST returned None for {symbol}")
        return (False, None, "post_returned_none") if detailed else (False, None)

    if isinstance(data, dict) and isinstance(data.get("code"), int) and data["code"] < 0:
        code = data["code"]
        msg  = data.get("msg", "")
        if code == -4421:
            warn = (
                f"[LIVE] LEVERAGE REJECTED (code=-4421): {symbol} — "
                f"subaccount may be capped below {leverage}x. "
                f"msg={msg}. Trade skipped safely."
            )
            print(warn)
            try:
                from telegram import send_telegram
                send_telegram(warn, channel="live")
            except Exception:
                pass
        else:
            print(f"[LIVE] _set_leverage error: code={code} msg={msg}")
        err = f"code={code} msg={msg}"
        return (False, None, err) if detailed else (False, None)

    confirmed_raw = data.get("leverage") if isinstance(data, dict) else None
    confirmed     = int(confirmed_raw) if isinstance(confirmed_raw, (int, float)) else None

    print(f"[LIVE] Leverage confirmed: {symbol} = {confirmed}x")
    return (True, confirmed, None) if detailed else (True, confirmed)


def _set_leverage_exchange_max_with_retry(symbol: str, leverage: int) -> tuple:
    lev_ok, confirmed_lev, err = _set_leverage(symbol, leverage, detailed=True)
    if lev_ok:
        return True, confirmed_lev, leverage

    print(
        f"[LEVERAGE] {symbol} set leverage failed requested={leverage} "
        f"error={err} retrying after bracket refresh"
    )
    refreshed_max, source, reason = _get_exchange_max_leverage(symbol, force_refresh=True)
    if refreshed_max is None:
        print(
            f"[LEVERAGE] {symbol} bracket refresh failed after set leverage failure "
            f"reason={reason}"
        )
        return False, None, leverage

    retry_lev = int(refreshed_max)
    if retry_lev != int(leverage):
        print(
            f"[LEVERAGE] {symbol} refreshed max changed after set failure "
            f"requested={leverage} refreshed={retry_lev} reason=unsafe_retry_rejected"
        )
        return False, None, leverage

    print(
        f"[LEVERAGE] {symbol} mode=exchange_max exchange_max={retry_lev} "
        f"applied={retry_lev} source={source} reason=retry_after_set_failure"
    )
    retry_ok, retry_confirmed = _set_leverage(symbol, retry_lev)
    return retry_ok, retry_confirmed, retry_lev


def _set_margin_type(symbol: str) -> bool:
    """
    Set margin type to CROSSED for symbol on LIVE account via POST /fapi/v1/marginType.

    Binance returns error code -4046 when margin type is already CROSSED.
    This is NOT a failure — treat -4046 as success (already correct state).
    """
    print(f"[LIVE] Setting margin type CROSSED for {symbol}")

    data = _post("/fapi/v1/marginType", {
        "symbol":     symbol,
        "marginType": "CROSSED",
    })

    if data is None:
        print(f"[LIVE] _set_margin_type: POST returned None for {symbol}")
        return False

    if isinstance(data, dict) and data.get("code") in (-4046, -4067):
        print(f"[LIVE] Margin type already CROSSED for {symbol} — OK (code={data.get('code')})")
        return True

    if isinstance(data, dict) and isinstance(data.get("code"), int) and data["code"] < 0:
        print(f"[LIVE] _set_margin_type error: code={data['code']} msg={data.get('msg')}")
        return False

    print(f"[LIVE] Margin type set: {symbol} = CROSSED")
    return True


# =====================================================================
# CLIENT ORDER ID
# =====================================================================

def _new_client_order_id(prefix: str = "CB") -> str:
    """
    Generate a unique clientOrderId for Binance order tracking.

    Format: <prefix>-<28-char uuid4 hex>  (total ≤ 36 chars)
    Prefix  CB-E = CoinBot Entry
            CB-S = CoinBot Stop-loss

    UUID4 guarantees uniqueness. The clientOrderId is the primary key
    for retry-safe deduplication: if we send the same clientOrderId
    twice, Binance rejects the second with code -2013 (or -1099 on
    testnet). This prevents duplicate positions on retry.
    """
    uid = uuid.uuid4().hex[:28]
    return f"{prefix}-{uid}"


def _bot_client_order_id(symbol: str, kind: str) -> str:
    """
    Generate a bot-tagged clientOrderId for ownership identification.

    Format: BOT_<BASE>_<KIND>_<hex12>  (total ≤ 36 chars; Binance limit is 36)
      BASE — symbol with USDT suffix stripped, truncated to 8 chars
             BTCUSDT  → BTC
             XRPUSDT  → XRP
             1000SHIBUSDT → 1000SHIB
      KIND — order type tag (single or two-letter mnemonic):
             E  = market entry order
             S  = initial stop-loss (STOP_MARKET)
             TS = trailing stop update (STOP_MARKET replace)
             EC = emergency close (reduceOnly MARKET)
      hex12 — 12 hex chars from uuid4 for global uniqueness within each run

    Examples:
      BOT_XRP_E_a3f1c9e8b204    (entry)
      BOT_BTC_S_7d09e4f3a100    (initial stop)
      BOT_SOL_TS_9a1c2d88f501   (trailing stop replacement)
      BOT_ETH_EC_bb3a01fc7de2   (emergency close)

    Binance will reject with -2013 if the same clientOrderId is submitted
    twice, providing implicit deduplication on retry.

    The BOT_ prefix is the exchange-level ownership proof used by:
      - open_trade()   → stores as t["client_order_id"] + confirms ownership
      - reconcile_exchange_positions() → skips exchange-only non-BOT orders
      - audit_exchange_sl()            → skips non-bot state entries
      - update_trades()                → skips non-bot live entries

    Length check:
      "BOT_" (4) + base_8 (≤8) + "_" (1) + kind_2 (≤2) + "_" (1) + hex12 (12) = ≤28
      Well within the Binance 36-char clientOrderId limit.
    """
    base = (symbol or "").upper()
    if base.endswith("USDT"):
        base = base[:-4]
    base = base[:8]
    uid = uuid.uuid4().hex[:12]
    cid = f"BOT_{base}_{kind}_{uid}"
    # Defensive truncation — should never trigger given length analysis above
    return cid[:36]


# =====================================================================
# PRE-EXECUTION VALIDATION
# =====================================================================

def validate_and_prepare(
    symbol:       str,
    side:         str,
    entry:        float,
    sl:           float,
    tp:           float,
    balance:      float,
    risk_percent: float,
) -> dict:
    """
    Run full pre-execution validation using execution_policy + precision layers.

    Returns dict:
      valid          bool
      reason         str
      plan           dict   (from calculate_execution_plan)
      qty            float  (rounded_qty)
      leverage       int    (final_leverage)
      margin         float  (margin_required)
      free_balance   float  (balance - margin_required)
    """
    side_upper = (side or "").upper()
    if side_upper not in ("BUY", "SELL"):
        return {"valid": False, "reason": f"invalid side: {side!r}", "plan": {}}

    plan = calculate_execution_plan(symbol, balance, risk_percent, entry, sl)

    if not plan.get("valid"):
        if not (_use_exchange_max_leverage() and str(plan.get("reason", "")).startswith("leverage infeasible:")):
            print(f"[LIVE VALIDATE] {symbol} rejected: {plan['reason']}")
            return {"valid": False, "reason": plan["reason"], "plan": plan}

    if _use_exchange_max_leverage():
        ok, reason, plan = _apply_exchange_max_leverage_to_plan(symbol, plan)
        if not ok:
            return {"valid": False, "reason": reason, "plan": plan}
        plan["valid"] = True
        plan["reason"] = "OK"

    qty      = plan["rounded_qty"]
    leverage = plan["final_leverage"]
    margin   = plan["margin_required"]

    if not _use_exchange_max_leverage():
        tier     = plan.get("tier") or get_symbol_tier(symbol)
        target   = plan.get("target_leverage")
        max_lev  = plan.get("allowed_leverage")
        clamp_reason = plan.get("leverage_clamp_reason")

        cap = get_max_leverage(symbol)
        if leverage > cap:
            reason = f"leverage {leverage}x exceeds tier cap {cap}x for {symbol}"
            print(f"[LIVE VALIDATE] {reason}")
            return {"valid": False, "reason": reason, "plan": plan}

        clamp_suffix = f" reason={clamp_reason}" if clamp_reason else ""
        print(
            f"[LEVERAGE] {symbol} tier={tier} target={target} "
            f"max={max_lev} applied={leverage}{clamp_suffix}"
        )

    free = balance - margin
    if free < 0:
        reason = f"insufficient free margin: balance={balance} margin={margin}"
        print(f"[LIVE VALIDATE] {reason}")
        return {"valid": False, "reason": reason, "plan": plan}

    print(
        f"[LIVE VALIDATE] {symbol} {side_upper} OK — "
        f"qty={qty}  leverage={leverage}x  margin=${margin:.2f}  free=${free:.2f}"
    )

    return {
        "valid":        True,
        "reason":       "OK",
        "plan":         plan,
        "qty":          qty,
        "leverage":     leverage,
        "margin":       margin,
        "free_balance": round(free, 4),
    }


# =====================================================================
# MARKET ENTRY ORDER
# =====================================================================

def place_market_order(
    symbol:          str,
    side:            str,
    qty:             float,
    leverage:        int,
    client_order_id: str = None,
) -> dict:
    """
    Place a MARKET entry order on Binance Futures MAINNET.

    Execution steps (always in this order):
      1. _require_live() guard
      2. _set_leverage(symbol, leverage)
      3. _set_margin_type(symbol)  → CROSSED
      4. POST /fapi/v1/order  type=MARKET

    Args:
      symbol:          e.g. "BTCUSDT"
      side:            "BUY" (long) or "SELL" (short)
      qty:             rounded quantity from execution plan
      leverage:        final_leverage from execution plan
      client_order_id: auto-generated if None (CB-E prefix)

    Returns dict:
      success          bool
      order_id         int | None
      client_order_id  str
      status           str     e.g. "FILLED"
      fill_price       float | None
      fill_qty         float | None
      raw              dict    full exchange response
      error            str | None

    ── RETRY SAFETY ──────────────────────────────────────────────────
    POST timeout ≠ failed order.

    When _post() returns None (timeout / network drop), the order
    may have been accepted by Binance before the connection failed.
    Sending the same request again would create a second position.

    Safe retry protocol:
      1. After timeout, call query_order(symbol, client_order_id=...)
      2. If found (any status) → order exists, do NOT re-send
      3. If not found (code -2013) → safe to retry ONCE with same id
      4. If second attempt also times out → escalate to operator
    ──────────────────────────────────────────────────────────────────
    """
    ok, _, _ = _require_live()
    if not ok:
        return _failed_result("live_mode not enabled", client_order_id)

    side_upper = (side or "").upper()
    if side_upper not in ("BUY", "SELL"):
        return _failed_result(f"invalid side: {side!r}", client_order_id)

    if qty is None or qty <= 0:
        return _failed_result(f"invalid qty: {qty}", client_order_id)

    if client_order_id is None:
        client_order_id = _bot_client_order_id(symbol, "E")

    if _use_exchange_max_leverage():
        exchange_max, source, reason = _get_exchange_max_leverage(symbol)
        if exchange_max is None:
            print(
                f"[LEVERAGE] {symbol} max leverage lookup failed - "
                f"rejecting entry reason=leverage_max_unknown"
            )
            _log_leverage_diagnostic(
                symbol,
                source,
                "leverage_max_unknown",
                cache_hit=source in ("bracket_cache", "cache_fallback"),
                final_reason="place_market_order_reject",
            )
            return _failed_result("leverage_max_unknown", client_order_id)
        leverage = int(exchange_max)
        print(
            f"[LEVERAGE] {symbol} mode=exchange_max exchange_max={exchange_max} "
            f"applied={leverage} source={source}"
        )

    direction = "LONG" if side_upper == "BUY" else "SHORT"
    print(f"[LIVE ORDER] {symbol} {direction}  qty={qty}  leverage={leverage}x")
    print(f"[LIVE ORDER] clientOrderId={client_order_id}")

    if _use_exchange_max_leverage():
        lev_ok, confirmed_lev, leverage = _set_leverage_exchange_max_with_retry(symbol, leverage)
    else:
        lev_ok, confirmed_lev = _set_leverage(symbol, leverage)
    if not lev_ok:
        return _failed_result(f"leverage setup failed for {symbol}", client_order_id)

    # ── Confirmed leverage validation ─────────────────────────────────
    if confirmed_lev is not None:
        if confirmed_lev != leverage:
            if _use_exchange_max_leverage():
                reject_msg = (
                    f"[LIVE] TRADE REJECTED: {symbol} leverage confirmation mismatch — "
                    f"requested={leverage}x confirmed={confirmed_lev}x"
                )
                print(reject_msg)
                try:
                    from telegram import send_telegram
                    send_telegram(reject_msg, channel="live")
                except Exception:
                    pass
                return _failed_result(
                    f"leverage confirmation mismatch: requested={leverage}x confirmed={confirmed_lev}x",
                    client_order_id,
                )
            adj_msg = (
                f"[LIVE] LEVERAGE ADJUSTED: {symbol} "
                f"requested={leverage}x confirmed={confirmed_lev}x"
            )
            print(adj_msg)
            try:
                from telegram import send_telegram
                send_telegram(adj_msg, channel="live")
            except Exception:
                pass

        if not _use_exchange_max_leverage():
            min_lev = get_min_acceptable_leverage(symbol)
            if confirmed_lev < min_lev:
                tier = get_symbol_tier(symbol)
                reject_msg = (
                f"[LIVE] TRADE REJECTED: {symbol} leverage confirmation too low — "
                    f"requested={leverage}x confirmed={confirmed_lev}x "
                    f"minimum_required={min_lev}x ({tier})"
                )
                print(reject_msg)
                try:
                    from telegram import send_telegram
                    send_telegram(reject_msg, channel="live")
                except Exception:
                    pass
                return _failed_result(
                    f"leverage too low: confirmed={confirmed_lev}x < minimum={min_lev}x",
                    client_order_id,
                )

    if not _set_margin_type(symbol):
        return _failed_result(f"margin type setup failed for {symbol}", client_order_id)

    raw = _post("/fapi/v1/order", {
        "symbol":           symbol,
        "side":             side_upper,
        "type":             "MARKET",
        "quantity":         qty,
        "newClientOrderId": client_order_id,
    })

    if raw is None:
        print(
            f"[LIVE ORDER] POST returned None — clientOrderId={client_order_id}. "
            "Order state UNKNOWN. Query before any retry."
        )
        result = _failed_result(
            "POST timeout — order state unknown, call query_order before retry",
            client_order_id,
        )
        result["entry_state"] = "ENTRY_UNCERTAIN"
        result["error_code"] = "order_state_unknown"
        return result

    if isinstance(raw, dict) and isinstance(raw.get("code"), int) and raw["code"] < 0:
        err = f"code={raw['code']} msg={raw.get('msg', '?')}"
        print(f"[LIVE ORDER] Rejected: {err}")
        return _failed_result(err, client_order_id, raw=raw)

    order_id   = raw.get("orderId")
    status     = raw.get("status", "?")
    fill_price = _safe_float(raw.get("avgPrice"))
    fill_qty   = _safe_float(raw.get("executedQty"))

    effective_lev = confirmed_lev if confirmed_lev is not None else leverage
    print(
        f"[LIVE ORDER] ACCEPTED — "
        f"orderId={order_id}  status={status}  "
        f"fill_price={fill_price}  fill_qty={fill_qty}  "
        f"confirmed_leverage={effective_lev}x"
    )

    return {
        "success":            True,
        "order_id":           order_id,
        "client_order_id":    client_order_id,
        "status":             status,
        "fill_price":         fill_price,
        "fill_qty":           fill_qty,
        "raw":                raw,
        "error":              None,
        "confirmed_leverage": effective_lev,
    }


# =====================================================================
# STOP-LOSS ORDER
# =====================================================================

def place_stop_loss(
    symbol:          str,
    entry_side:      str,
    qty:             float,
    stop_price:      float,
    client_order_id: str = None,
) -> dict:
    """
    Place a STOP_MARKET stop-loss order on Binance Futures MAINNET.

    The close side is derived from entry_side:
      entry_side=BUY  → stop side=SELL
      entry_side=SELL → stop side=BUY

    Args:
      symbol:          e.g. "BTCUSDT"
      entry_side:      the entry direction ("BUY" or "SELL")
      qty:             rounded quantity matching entry
      stop_price:      SL trigger price (rounded to tickSize)
      client_order_id: auto-generated if None (CB-S prefix)

    Returns same dict shape as place_market_order.

    ── reduceOnly=true ───────────────────────────────────────────────
    reduceOnly ensures this order can ONLY close an existing position.
    Binance rejects reduceOnly orders when no matching open position
    exists on that side. Always confirm entry fill before placing SL.

    ── workingType ───────────────────────────────────────────────────
    MARK_PRICE     = trigger on smoothed index/mark price (safer, avoids wick-triggering).
    CONTRACT_PRICE = trigger on last traded price (more sensitive to wicks).
    MARK_PRICE is used here to avoid premature SL triggers from
    temporary wicks that don't reflect true market price.
    ──────────────────────────────────────────────────────────────────
    """
    ok, _, _ = _require_live()
    if not ok:
        return _failed_result("live_mode not enabled", client_order_id)

    entry_upper = (entry_side or "").upper()
    if entry_upper not in ("BUY", "SELL"):
        return _failed_result(f"invalid entry_side: {entry_side!r}", client_order_id)

    if qty is None or qty <= 0:
        return _failed_result(f"invalid qty: {qty}", client_order_id)

    if stop_price is None or stop_price <= 0:
        return _failed_result(f"invalid stop_price: {stop_price}", client_order_id)

    close_side = "SELL" if entry_upper == "BUY" else "BUY"

    if client_order_id is None:
        client_order_id = _bot_client_order_id(symbol, "S")

    _filters_info = get_symbol_filters(symbol)
    if _filters_info is None:
        return _failed_result(
            f"stopPrice rounding failed for {symbol} — precision filters unavailable",
            client_order_id,
        )

    try:
        _tick = _filters_info["tick_size"]
        _stop_decimal = Decimal(str(stop_price)).quantize(_tick, rounding=ROUND_DOWN)
    except InvalidOperation as _e:
        return _failed_result(
            f"triggerPrice precision error for {symbol}: {_e}", client_order_id
        )

    if _stop_decimal <= 0:
        return _failed_result(
            f"triggerPrice underflow: raw={stop_price} tick={_tick} rounded={_stop_decimal}",
            client_order_id,
        )

    _stop_str = format(_stop_decimal, 'f')
    rounded_stop = float(_stop_decimal)
    print(
        f"[PRECISION] {symbol} triggerPrice: "
        f"raw_input={stop_price} tick={_tick} rounded={_stop_decimal} serialized={_stop_str!r}"
    )

    _stop_params = {
        "symbol":        symbol,
        "side":          close_side,
        "type":          "STOP_MARKET",
        "algoType":      "CONDITIONAL",
        "triggerPrice":  _stop_str,
        "closePosition": "true",
        "workingType":   "MARK_PRICE",
        "newClientOrderId": client_order_id,
    }

    print(
        f"[LIVE STOP]  {symbol}  STOP_MARKET(algo)  "
        f"side={close_side}  triggerPrice={_stop_str}  closePosition=true  workingType=MARK_PRICE"
    )
    print(f"[LIVE STOP]  clientOrderId={client_order_id}")

    if close_side == "BUY":
        print(
            f"[LIVE DEBUG] SHORT stop direction: "
            f"BUY STOP_MARKET triggerPrice={_stop_str} (raw_input={stop_price}) "
            f"— must be > current markPrice at placement"
        )
    else:
        print(
            f"[LIVE DEBUG] LONG stop direction: "
            f"SELL STOP_MARKET triggerPrice={_stop_str} (raw_input={stop_price}) "
            f"— must be < current markPrice at placement"
        )

    print(f"[LIVE DEBUG] STOP payload:\n{json.dumps(_stop_params, indent=2)}")

    raw = _post("/fapi/v1/algoOrder", _stop_params)

    print(
        f"[LIVE DEBUG] STOP response:\n"
        f"{json.dumps(raw, indent=2) if isinstance(raw, dict) else raw}"
    )

    if raw is None:
        print(
            f"[LIVE STOP] POST returned None — clientOrderId={client_order_id}. "
            "SL state UNKNOWN. Query before any retry."
        )
        return _failed_result(
            "POST timeout — SL state unknown, call query_order before retry",
            client_order_id,
        )

    if isinstance(raw, dict) and isinstance(raw.get("code"), int) and raw["code"] < 0:
        err = f"code={raw['code']} msg={raw.get('msg', '?')}"
        print(f"[LIVE STOP] Rejected: {err}")
        return _failed_result(err, client_order_id, raw=raw)

    order_id = raw.get("algoId")
    status   = raw.get("algoStatus", "?")

    print(
        f"[LIVE STOP] ACCEPTED — "
        f"orderId={order_id}  status={status}  "
        f"stopPrice={rounded_stop}  closePosition=true"
    )

    return {
        "success":         True,
        "order_id":        order_id,
        "client_order_id": client_order_id,
        "status":          status,
        "fill_price":      None,
        "fill_qty":        None,
        "raw":             raw,
        "error":           None,
    }


# =====================================================================
# ORDER STATUS QUERY
# =====================================================================

def query_order(
    symbol:          str,
    order_id:        int = None,
    client_order_id: str = None,
    return_not_found: bool = False,
) -> dict | None:
    """
    Query a single order by orderId or clientOrderId on MAINNET.

    At least one of order_id or client_order_id must be provided.

    Primary use case — retry safety:
      After a POST timeout, call this before deciding to retry.
      Possible outcomes:
        status=FILLED → entry succeeded, position is open, do NOT retry
        status=NEW    → order queued, do NOT retry
        code=-2013    → order does not exist, safe to retry once
        None returned → query itself failed, treat conservatively

    Returns:
      dict — full Binance order response (has "status", "orderId", etc.)
      None — order not found or query failed
    """
    if order_id is None and client_order_id is None:
        print("[LIVE QUERY] must provide order_id or client_order_id")
        return None

    params = {"symbol": symbol}
    if order_id is not None:
        params["orderId"] = order_id
    if client_order_id is not None:
        params["origClientOrderId"] = client_order_id

    label = f"orderId={order_id}" if order_id else f"clientOrderId={client_order_id}"
    print(f"[LIVE QUERY] {symbol}  {label}")

    data = _get_signed("/fapi/v1/order", params)

    if data is None:
        print(f"[LIVE QUERY] No response for {symbol} — treat as unknown")
        return None

    if isinstance(data, dict) and isinstance(data.get("code"), int) and data["code"] < 0:
        code = data["code"]
        msg  = data.get("msg", "?")
        if code == -2013 and return_not_found:
            print(f"[LIVE QUERY] Order does not exist ({code}) - authoritative not-found")
            return {
                "_query_not_found": True,
                "code": code,
                "msg": msg,
                "symbol": symbol,
                "orderId": order_id,
                "clientOrderId": client_order_id,
            }
        if code == -2013:
            print(f"[LIVE QUERY] Order does not exist ({code}) — safe to retry")
        else:
            print(f"[LIVE QUERY] Error: code={code} msg={msg}")
        return None

    status = data.get("status", "?")
    oid    = data.get("orderId")
    print(f"[LIVE QUERY] Found: orderId={oid}  status={status}")
    return data


def query_algo_order(symbol: str, algo_id: int) -> dict | None:
    """
    Query an algo stop order by algoId via GET /fapi/v1/openAlgoOrders.

    Algo orders (placed via /fapi/v1/algoOrder) cannot be queried via
    /fapi/v1/order — they require the algo orders list endpoint.

    Returns the matching algo order dict if found and still open,
    or None if not found (may have been triggered or cancelled).
    """
    print(f"[LIVE QUERY] {symbol}  algoId={algo_id}")
    data = _get_signed("/fapi/v1/openAlgoOrders", {"symbol": symbol})

    if data is None:
        print(f"[LIVE QUERY] No response for {symbol}")
        return None

    if not isinstance(data, list):
        print(f"[LIVE QUERY] Unexpected response type: {type(data)}")
        return None

    for order in data:
        if order.get("algoId") == algo_id:
            status = order.get("algoStatus", "?")
            print(f"[LIVE QUERY] Found: algoId={algo_id}  algoStatus={status}")
            return order

    print(f"[LIVE QUERY] algoId={algo_id} not in open algo orders for {symbol}")
    return None


def get_open_algo_orders(symbol: str) -> list | None:
    """
    Return all open algo orders for symbol via GET /fapi/v1/openAlgoOrders.

    Used for pre-placement duplicate checks and post-update orphan verification.

    Returns:
      list  — algo order dicts (may be empty [])
      None  — query failed or unexpected response (caller must treat as uncertain,
              NOT as confirmed absence of stops)
    """
    print(f"[LIVE QUERY] list open algo orders for {symbol}")
    data = _get_signed("/fapi/v1/openAlgoOrders", {"symbol": symbol})

    if data is None:
        print(f"[LIVE QUERY] no response for {symbol}")
        return None

    if not isinstance(data, list):
        print(f"[LIVE QUERY] unexpected response type for {symbol}: {type(data)}")
        return None

    print(f"[LIVE QUERY] {len(data)} open algo order(s) for {symbol}")
    return data


def get_open_orders(symbol: str) -> list | None:
    """
    Return all open regular orders for symbol via GET /fapi/v1/openOrders.

    Read-only ownership/reconcile helper. Returns None on query failure so callers
    can preserve local state instead of treating absence as authoritative.
    """
    print(f"[LIVE QUERY] list open orders for {symbol}")
    data = _get_signed("/fapi/v1/openOrders", {"symbol": symbol})

    if data is None:
        print(f"[LIVE QUERY] no openOrders response for {symbol}")
        return None

    if not isinstance(data, list):
        print(f"[LIVE QUERY] unexpected openOrders response type for {symbol}: {type(data)}")
        return None

    print(f"[LIVE QUERY] {len(data)} open regular order(s) for {symbol}")
    return data


def get_recent_orders(symbol: str, limit: int = 50) -> list | None:
    """
    Return recent regular orders for symbol via GET /fapi/v1/allOrders.

    Read-only ownership/reconcile helper. Returns None on query failure so callers
    can preserve local state instead of treating absence as authoritative.
    """
    print(f"[LIVE QUERY] list recent orders for {symbol} limit={limit}")
    data = _get_signed("/fapi/v1/allOrders", {"symbol": symbol, "limit": limit})

    if data is None:
        print(f"[LIVE QUERY] no allOrders response for {symbol}")
        return None

    if not isinstance(data, list):
        print(f"[LIVE QUERY] unexpected allOrders response type for {symbol}: {type(data)}")
        return None

    print(f"[LIVE QUERY] {len(data)} recent regular order(s) for {symbol}")
    return data


# =====================================================================
# CANCEL STOP ORDER
# =====================================================================

def cancel_stop_loss(symbol: str, order_id: int) -> dict:
    """
    Cancel an existing STOP_MARKET order by orderId on MAINNET.

    Called during trailing stop synchronization: after a new stop is
    successfully placed, the old stop is cancelled via this function.

    A -2011 response (Unknown Order) is treated as success — the order
    was already filled or cancelled before this call arrived.

    Returns dict:
      success   bool
      order_id  int | None
      error     str | None
    """
    ok, _, _ = _require_live()
    if not ok:
        return {"success": False, "order_id": None, "error": "live_mode not enabled"}

    if not order_id:
        return {"success": False, "order_id": None, "error": "no order_id provided"}

    print(f"[LIVE STOP] {symbol} cancel stop algoId={order_id}")

    # Algo orders are cancelled via DELETE /fapi/v1/algoOrder with algoId.
    # The response on success: {"algoId": ..., "code": "200", "msg": "success"}
    raw = _delete("/fapi/v1/algoOrder", {"algoId": order_id})

    if raw is None:
        print(
            f"[LIVE STOP] {symbol} DELETE returned None — cancel state UNKNOWN algoId={order_id}"
        )
        return {"success": False, "order_id": order_id, "error": "DELETE timeout — cancel state unknown"}

    if isinstance(raw, dict) and isinstance(raw.get("code"), int) and raw["code"] < 0:
        code = raw["code"]
        msg  = raw.get("msg", "?")
        if code == -2011:
            print(f"[LIVE STOP] {symbol} algoId={order_id} already cancelled or filled — OK")
            return {"success": True, "order_id": order_id, "error": None}
        print(f"[LIVE STOP] {symbol} REJECTED: code={code} msg={msg}")
        return {"success": False, "order_id": order_id, "error": f"code={code} msg={msg}"}

    # Success response has code="200" (string) — treat any non-error response as success
    msg = raw.get("msg", "?") if isinstance(raw, dict) else "?"
    print(f"[LIVE STOP] {symbol} cancelled algoId={order_id}  msg={msg}")
    return {"success": True, "order_id": order_id, "error": None}


# =====================================================================
# TRAILING STOP SYNCHRONIZATION
# =====================================================================

def update_trailing_stop(
    symbol:         str,
    entry_side:     str,
    qty:            float,
    new_stop_price: float,
    old_order_id:   int,
) -> dict:
    """
    Synchronize a trailing stop update to the exchange.

    Safety guarantee — place-first strategy:
      1. Place new STOP_MARKET at new_stop_price.
      2. If placement fails → old stop is preserved unchanged. Return failure.
      3. If placement succeeds → cancel old stop.
      4. If cancel fails → both stops exist. Position is protected by the new
         stop. Log warning. Return success with cancel_ok=False.

    This ensures a naked position (no stop) can never result from a
    trailing update operation.

    Args:
      symbol:         e.g. "BTCUSDT"
      entry_side:     "BUY" (long) or "SELL" (short)
      qty:            position quantity — must match the open position
      new_stop_price: updated SL trigger price
      old_order_id:   orderId of the current active STOP_MARKET to replace

    Returns dict:
      success:      True if new stop is active on exchange
      new_order_id: orderId of the new stop (None on failure)
      cancel_ok:    True if old stop was successfully cancelled
      error:        error message if success is False
    """
    ok, _, _ = _require_live()
    if not ok:
        return {
            "success": False, "new_order_id": None,
            "cancel_ok": False, "error": "live_mode not enabled",
        }

    print(
        f"[LIVE TRAIL] {symbol} update stop: "
        f"old orderId={old_order_id} → new stopPrice={new_stop_price}"
    )

    # Algo API constraint: closePosition=true allows only ONE stop per symbol/side.
    # Trailing uses quantity+reduceOnly for the new stop so it can be placed BEFORE
    # cancelling the old one, preserving the place-first safety guarantee.
    entry_upper = (entry_side or "").upper()
    close_side  = "SELL" if entry_upper == "BUY" else "BUY"

    _filters_info = get_symbol_filters(symbol)
    if _filters_info is None:
        return {
            "success": False, "new_order_id": None,
            "cancel_ok": False,
            "error": f"stopPrice rounding failed for {symbol}",
        }

    try:
        _tick = _filters_info["tick_size"]
        _stop_decimal = Decimal(str(new_stop_price)).quantize(_tick, rounding=ROUND_DOWN)
    except InvalidOperation as _e:
        return {
            "success": False, "new_order_id": None,
            "cancel_ok": False,
            "error": f"triggerPrice precision error for {symbol}: {_e}",
        }

    if _stop_decimal <= 0:
        return {
            "success": False, "new_order_id": None,
            "cancel_ok": False,
            "error": f"triggerPrice underflow: raw={new_stop_price} tick={_tick} rounded={_stop_decimal}",
        }

    _stop_str = format(_stop_decimal, 'f')
    rounded_stop = float(_stop_decimal)
    print(
        f"[PRECISION] {symbol} triggerPrice: "
        f"raw_input={new_stop_price} tick={_tick} rounded={_stop_decimal} serialized={_stop_str!r}"
    )

    _trail_client_id = _bot_client_order_id(symbol, "TS")
    _trail_params = {
        "symbol":           symbol,
        "side":             close_side,
        "type":             "STOP_MARKET",
        "algoType":         "CONDITIONAL",
        "triggerPrice":     _stop_str,
        "quantity":         qty,
        "reduceOnly":       "true",
        "workingType":      "MARK_PRICE",
        "newClientOrderId": _trail_client_id,
    }

    print(f"[LIVE TRAIL] {symbol} placing new stop (qty+reduceOnly) triggerPrice={_stop_str} clientOrderId={_trail_client_id}")
    raw_new = _post("/fapi/v1/algoOrder", _trail_params)

    if raw_new is None or (
        isinstance(raw_new, dict)
        and isinstance(raw_new.get("code"), int)
        and raw_new["code"] < 0
    ):
        err = (
            f"code={raw_new['code']} msg={raw_new.get('msg','?')}"
            if isinstance(raw_new, dict)
            else "POST timeout"
        )
        print(
            f"[LIVE TRAIL] {symbol} new stop FAILED — old stop PRESERVED. error={err}"
        )
        return {
            "success": False, "new_order_id": None,
            "cancel_ok": False, "error": err,
        }

    new_order_id = raw_new.get("algoId")
    print(
        f"[LIVE TRAIL] {symbol} new stop placed — "
        f"orderId={new_order_id}  stopPrice={new_stop_price}"
    )

    cancel_result = cancel_stop_loss(symbol=symbol, order_id=old_order_id)

    if cancel_result.get("success"):
        print(f"[LIVE TRAIL] {symbol} old stop cancelled orderId={old_order_id}")
        return {
            "success": True, "new_order_id": new_order_id,
            "cancel_ok": True, "error": None,
        }

    print(
        f"[LIVE TRAIL] {symbol} old stop cancel FAILED orderId={old_order_id} — "
        f"BOTH stops active. Position protected by new stop orderId={new_order_id}."
    )
    return {
        "success": True, "new_order_id": new_order_id,
        "cancel_ok": False, "error": None,
    }


# =====================================================================
# RECONCILIATION HELPERS
# =====================================================================

def get_exchange_positions() -> list | None:
    """
    Return all open positions from LIVE MAINNET account.

    Fetches from GET /fapi/v2/positionRisk on mainnet.
    Filters to non-zero positionAmt only.

    Returns list of dicts:
      symbol, positionAmt (float, signed: + long / - short),
      entryPrice (float), unrealizedProfit (float),
      leverage (int), marginType (str)

    Returns:
      list  - confirmed open positions, possibly empty
      None  - query failed or response was unusable; callers must not treat
              this as confirmed absence of exchange positions
    """
    data = _get_signed("/fapi/v2/positionRisk", {})

    if data is None:
        print("[LIVE ERROR] get_exchange_positions failed")
        return None

    if not isinstance(data, list):
        print(f"[LIVE ERROR] unexpected type: {type(data)}")
        return None

    positions = []
    for pos in data:
        try:
            amt = float(pos.get("positionAmt", 0))
        except (ValueError, TypeError):
            continue

        if amt == 0.0:
            continue

        positions.append({
            "symbol":           pos.get("symbol", ""),
            "positionAmt":      amt,
            "entryPrice":       _safe_float(pos.get("entryPrice")),
            "unrealizedProfit": _safe_float(pos.get("unRealizedProfit")),
            "leverage":         int(pos.get("leverage", 0)),
            "marginType":       pos.get("marginType", ""),
            "positionSide":     pos.get("positionSide", ""),
        })

    print(f"[LIVE] {len(positions)} open position(s) found on exchange")
    return positions


def is_position_closed(symbol: str) -> bool | None:
    """
    Verify whether a symbol has no open LIVE futures position.

    Returns:
      True  — exchange confirms positionAmt is zero or absent
      False — exchange confirms a non-zero position remains
      None  — exchange query failed; caller must treat close state as unknown
    """
    sym = (symbol or "").upper()
    data = _get_signed("/fapi/v2/positionRisk", {"symbol": sym})

    if data is None:
        print(f"[LIVE VERIFY] {sym} position close check failed")
        return None

    rows = data if isinstance(data, list) else [data]
    for pos in rows:
        if not isinstance(pos, dict) or pos.get("symbol") != sym:
            continue
        try:
            amt = float(pos.get("positionAmt", 0))
        except (TypeError, ValueError):
            print(f"[LIVE VERIFY] {sym} malformed positionAmt={pos.get('positionAmt')!r}")
            return None
        if abs(amt) > 0.0:
            print(f"[LIVE VERIFY] {sym} position still open amt={amt}")
            return False

    print(f"[LIVE VERIFY] {sym} position confirmed closed")
    return True


def compare_local_vs_exchange(
    local_positions:    list,
    exchange_positions: list,
) -> dict:
    """
    Compare local bot state against live exchange positions.

    Args:
      local_positions:    list of dicts from open_trades_v4.json:
                            symbol (str), direction (str: LONG/SHORT),
                            qty (float), entry (float)
      exchange_positions: list returned by get_exchange_positions()

    Returns dict:
      matched       list — same symbol present and qty agrees
      local_only    list — in local state but not on exchange (lost order?)
      exchange_only list — on exchange but not in local state (orphan?)
      discrepancies list — present both sides but qty or direction mismatch

    positionAmt sign convention:
      positive = long position
      negative = short position
    """
    matched       = []
    local_only    = []
    exchange_only = []
    discrepancies = []
    used_exchange_indexes = set()

    def _matches_side(lc: dict, ex: dict) -> bool:
        lc_dir = (lc.get("direction") or lc.get("side") or "LONG").upper()
        ex_side = (ex.get("positionSide") or "BOTH").upper()
        ex_amt = float(ex.get("positionAmt", 0.0))

        if ex_side in ("", "BOTH"):
            if lc_dir == "LONG":
                return ex_amt > 0
            if lc_dir == "SHORT":
                return ex_amt < 0
            return False

        return ex_side == lc_dir and abs(ex_amt) > 0.0

    for lc in local_positions:
        sym = lc.get("symbol")
        match_idx = None
        match_ex = None

        for idx, ex in enumerate(exchange_positions or []):
            if idx in used_exchange_indexes:
                continue
            if ex.get("symbol") != sym:
                continue
            if not _matches_side(lc, ex):
                continue
            match_idx = idx
            match_ex = ex
            break

        if match_ex is None:
            local_only.append({"symbol": sym, "local": lc, "exchange": None})
            print(f"[LIVE] LOCAL ONLY:    {sym} — not found on exchange")
            continue

        used_exchange_indexes.add(match_idx)
        ex = match_ex
        ex_amt = round(float(ex["positionAmt"]), 8)
        ex_side = (ex.get("positionSide") or "BOTH").upper()

        lc_qty  = float(lc.get("qty", 0.0))
        lc_dir  = (lc.get("direction") or lc.get("side") or "LONG").upper()
        expected = round(lc_qty if lc_dir == "LONG" else -lc_qty, 8)
        actual_for_compare = round(abs(ex_amt), 8) if ex_side not in ("", "BOTH") else ex_amt
        expected_for_compare = round(abs(expected), 8) if ex_side not in ("", "BOTH") else expected

        if abs(expected_for_compare - actual_for_compare) > 0.0001:
            discrepancies.append({
                "symbol":   sym,
                "local":    lc,
                "exchange": ex,
                "expected_signed_qty": expected,
                "actual_signed_qty":   ex_amt,
            })
            print(
                f"[LIVE] MISMATCH:      {sym} — "
                f"local={expected}  exchange={ex_amt} positionSide={ex_side}"
            )
        else:
            matched.append({"symbol": sym, "local": lc, "exchange": ex})
            print(f"[LIVE] MATCHED:       {sym}  qty={ex_amt} positionSide={ex_side}")

    for idx, ex in enumerate(exchange_positions or []):
        if idx not in used_exchange_indexes:
            sym = ex.get("symbol")
            exchange_only.append({"symbol": sym, "local": None, "exchange": ex})
            print(
                f"[LIVE] EXCHANGE ONLY: {sym}  "
                f"amt={ex['positionAmt']} — orphan position"
            )

    return {
        "matched":       matched,
        "local_only":    local_only,
        "exchange_only": exchange_only,
        "discrepancies": discrepancies,
    }


# =====================================================================
# EMERGENCY CLOSE
# =====================================================================

def emergency_close_position(
    symbol:          str,
    entry_side:      str,
    qty:             float,
    client_order_id: str = None,
) -> dict:
    """
    Emergency MARKET close — reduceOnly=true.

    Called ONLY when stop-loss placement fails after a successful market entry.
    Closes the live exchange position immediately to prevent naked exposure.

    Args:
      symbol:     e.g. "BTCUSDT"
      entry_side: the entry direction ("BUY" or "SELL") — close side is derived
      qty:        quantity matching the open position

    Returns same dict shape as place_market_order.
    """
    ok, _, _ = _require_live()
    if not ok:
        return _failed_result("live_mode not enabled", client_order_id)

    entry_upper = (entry_side or "").upper()
    if entry_upper not in ("BUY", "SELL"):
        return _failed_result(f"invalid entry_side: {entry_side!r}", client_order_id)

    if qty is None or qty <= 0:
        return _failed_result(f"invalid qty: {qty}", client_order_id)

    close_side = "SELL" if entry_upper == "BUY" else "BUY"

    if client_order_id is None:
        client_order_id = _bot_client_order_id(symbol, "EC")

    print(
        f"[LIVE ERROR] EMERGENCY CLOSE {symbol}  {close_side}  qty={qty}  reduceOnly=true"
    )
    print(f"[LIVE ERROR] EMERGENCY CLOSE clientOrderId={client_order_id}")

    raw = _post("/fapi/v1/order", {
        "symbol":           symbol,
        "side":             close_side,
        "type":             "MARKET",
        "quantity":         qty,
        "reduceOnly":       "true",
        "newClientOrderId": client_order_id,
    })

    if raw is None:
        print(
            f"[LIVE ERROR] EMERGENCY CLOSE POST returned None — clientOrderId={client_order_id}. "
            "Close state UNKNOWN — position may still be open."
        )
        return _failed_result(
            "emergency close POST timeout — position state unknown",
            client_order_id,
        )

    if isinstance(raw, dict) and isinstance(raw.get("code"), int) and raw["code"] < 0:
        err = f"code={raw['code']} msg={raw.get('msg', '?')}"
        print(f"[LIVE ERROR] EMERGENCY CLOSE REJECTED: {err}")
        return _failed_result(err, client_order_id, raw=raw)

    order_id   = raw.get("orderId")
    status     = raw.get("status", "?")
    fill_price = _safe_float(raw.get("avgPrice"))
    fill_qty   = _safe_float(raw.get("executedQty"))

    print(
        f"[LIVE ERROR] EMERGENCY CLOSE ACCEPTED — "
        f"orderId={order_id}  status={status}  "
        f"fill_price={fill_price}  fill_qty={fill_qty}"
    )

    return {
        "success":         True,
        "order_id":        order_id,
        "client_order_id": client_order_id,
        "status":          status,
        "fill_price":      fill_price,
        "fill_qty":        fill_qty,
        "raw":             raw,
        "error":           None,
    }


# =====================================================================
# STARTUP LOG
# =====================================================================

def log_startup_mode(wallet_balance: float = None) -> None:
    """
    Print explicit environment and balance banner at bot startup.

    Must be called once before any execution flow begins.
    Pass wallet_balance (from get_account_balance()) to enable the
    over-configured execution balance warning.

    If live_mode is False, prints a single inactive notice.
    """
    cfg  = _load_config()
    mode = cfg.get("live_mode", False)

    if not mode:
        print("[EXCHANGE] live_mode=false — live executor inactive")
        return

    key = cfg.get("api_key", "")
    key_preview = key[:8] + "..." if len(key) >= 8 else "(empty)"

    eb = get_execution_balance(wallet_balance)
    eb_str = f"${eb:.2f} USDT" if eb is not None else "NOT SET — config missing"

    print("=" * 56)
    print("[EXCHANGE] *** LIVE MODE ACTIVE — REAL MONEY AT RISK ***")
    print(f"[EXCHANGE] URL              : {_LIVE_BASE_URL}")
    print(f"[EXCHANGE] API key          : {key_preview}")
    print(f"[EXECUTION] Effective execution balance : {eb_str}")
    if wallet_balance is not None:
        print(f"[EXECUTION] Exchange wallet balance     : ${float(wallet_balance):.2f} USDT")
    print("[EXCHANGE] LIVE SAFETY POSTURE:")
    print("[EXCHANGE]   Symbol policy        : well-formed USDT futures symbols; TIER5 blocked")
    print(f"[EXCHANGE]   Allowed entry types  : {_LIVE_ALLOWED_ENTRY_TYPES}")
    print(f"[EXCHANGE]   Allowed exhaustion   : {_LIVE_ALLOWED_EXHAUSTION}")
    print(f"[EXCHANGE]   Allowed BOS types    : {_LIVE_ALLOWED_BOS_TYPES}")
    print(f"[EXCHANGE]   Max concurrent       : {_get_live_max_concurrent()}")
    live_risk_cap, live_risk_error = _get_live_risk_per_trade()
    live_risk_text = live_risk_cap if live_risk_error is None else live_risk_error
    live_portfolio_cap, live_portfolio_error = _get_live_max_portfolio_risk()
    live_portfolio_text = live_portfolio_cap if live_portfolio_error is None else live_portfolio_error
    print(f"[EXCHANGE]   Max risk percent     : {live_risk_text}")
    print(f"[EXCHANGE]   Max portfolio risk   : {live_portfolio_text}")
    print("[EXCHANGE] All orders → Binance Futures MAINNET.")
    print("=" * 56)


# =====================================================================
# INTERNAL HELPERS
# =====================================================================

def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)
        return f if f != 0.0 else None
    except (ValueError, TypeError):
        return None


def _failed_result(
    reason:          str,
    client_order_id: str = None,
    raw:             dict = None,
) -> dict:
    return {
        "success":         False,
        "order_id":        None,
        "client_order_id": client_order_id,
        "status":          "FAILED",
        "fill_price":      None,
        "fill_qty":        None,
        "raw":             raw or {},
        "error":           reason,
    }
