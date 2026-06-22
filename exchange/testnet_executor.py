"""
TESTNET-ONLY order execution module for COIN BOT.

SAFE BOUNDARIES — this module MUST NOT:
  - Call any mainnet endpoint (https://fapi.binance.com)
  - Execute when testnet_mode != true in config.json
  - Import from execution.py, entry.py, main.py, or state_manager.py

Role:
  Execute validated MARKET + STOP_MARKET orders against
  Binance Futures TESTNET for full lifecycle validation
  before any mainnet promotion.

Testnet URL : https://testnet.binancefuture.com
Mainnet URL : https://fapi.binance.com  ← NEVER CALLED FROM THIS MODULE

Config keys required:
  testnet_mode       bool   — must be true to allow any write operation
  testnet_api_key    str    — TESTNET API key (different from mainnet key)
  testnet_api_secret str    — TESTNET API secret
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
  compare_local_vs_exchange(local_positions, exchange_positions)
  get_open_algo_orders(symbol)

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
"""

import hashlib
import hmac
import json
import os
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

_TESTNET_BASE_URL = "https://testnet.binancefuture.com"
_TIMEOUT          = (5, 15)   # (connect, read) — slightly wider for write ops
_MAX_RETRY_GET    = 3         # GET queries may retry safely
_RETRY_DELAYS     = [0.5, 1.0, 2.0]

_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "config.json"
)

# =====================================================================
# CONFIG LOADER
# =====================================================================

def _load_config() -> dict:
    try:
        with open(_CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"[TESTNET] config.json read error: {e}")
        return {}


def get_execution_balance(wallet_balance: float = None) -> float | None:
    """
    Return the authoritative execution balance from config.json.

    This is the ONLY sanctioned source of balance for all execution logic.
    All callers (validate_and_prepare, build_execution_payload, etc.) must
    receive balance from this function, not from the raw exchange wallet.

    Why a separate config key instead of the exchange wallet:
      The TESTNET wallet may hold 5000+ USDT for testing purposes.
      Execution-testing capital is intentionally smaller (e.g. $50) to
      replicate real small-account behavior. Using the wallet balance
      directly would produce unrealistic position sizes.

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


def _require_testnet() -> tuple:
    """
    Guard called at the start of every write operation.

    Returns (enabled: bool, api_key: str, api_secret: str).
    Returns (False, "", "") on any guard failure — caller must abort.

    This is the primary safety gate. It is impossible to reach any
    POST endpoint in this module without passing this check.
    """
    cfg = _load_config()

    if not cfg.get("testnet_mode", False):
        print(
            "[TESTNET] BLOCKED — testnet_mode is not true in config.json. "
            "Set \"testnet_mode\": true before using testnet execution."
        )
        return False, "", ""

    key    = cfg.get("testnet_api_key", "")
    secret = cfg.get("testnet_api_secret", "")

    if not key or not secret:
        print(
            "[TESTNET] BLOCKED — testnet_api_key or testnet_api_secret "
            "missing in config.json."
        )
        return False, "", ""

    return True, key, secret


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
# REQUEST WRAPPERS — TESTNET ONLY
# =====================================================================

def _post(path: str, params: dict) -> dict | None:
    """
    Signed POST to Binance Futures TESTNET only.

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
    ok, api_key, api_secret = _require_testnet()
    if not ok:
        return None

    params = dict(params)
    params["timestamp"]  = int(time.time() * 1000)
    params["recvWindow"] = 5000
    params["signature"]  = _sign(params, api_secret)

    headers = {"X-MBX-APIKEY": api_key}
    url     = _TESTNET_BASE_URL + path

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
            print(f"[TESTNET] Rate limit on POST {path} — back off {wait}s before any retry")
            return None

        if res.status_code == 418:
            print("[TESTNET] IP banned (418) on TESTNET — halt all requests")
            return None

        if res.status_code == 401:
            print(f"[TESTNET] Auth error (401) — check testnet_api_key / testnet_api_secret")
            return None

        data = res.json()

        if res.status_code != 200:
            code = data.get("code", "?") if isinstance(data, dict) else "?"
            msg  = data.get("msg",  res.text[:200]) if isinstance(data, dict) else res.text[:200]
            print(f"[TESTNET] POST {path} HTTP {res.status_code} — code={code} msg={msg}")
            return data

        return data

    except requests.exceptions.Timeout:
        # A POST timeout is NOT a confirmed rejection.
        # The order request may have reached Binance before the connection timed out.
        # Treat as UNKNOWN state — caller must query before any retry.
        print(
            f"[TESTNET] TIMEOUT on POST {path}. "
            "Order status is UNKNOWN — do NOT retry without calling query_order() first."
        )
        return None

    except Exception as e:
        print(f"[TESTNET] Unexpected error on POST {path}: {e}")
        return None


def _get_signed(path: str, params: dict) -> dict | list | None:
    """
    Signed GET to Binance Futures TESTNET.

    Used for order queries and position reads.
    Retries are safe for GET — idempotent reads only.
    """
    ok, api_key, api_secret = _require_testnet()
    if not ok:
        return None

    params = dict(params)
    params["timestamp"]  = int(time.time() * 1000)
    params["recvWindow"] = 5000
    params["signature"]  = _sign(params, api_secret)

    headers = {"X-MBX-APIKEY": api_key}
    url     = _TESTNET_BASE_URL + path

    _RETRYABLE = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.ReadTimeout,
        requests.exceptions.ConnectTimeout,
    )

    for attempt in range(_MAX_RETRY_GET):
        try:
            res = requests.get(
                url,
                params=params,
                headers=headers,
                verify=certifi.where(),
                timeout=_TIMEOUT,
            )

            if res.status_code == 429:
                wait = float(res.headers.get("Retry-After", _RETRY_DELAYS[attempt]))
                print(f"[TESTNET] Rate limit on GET {path} — waiting {wait}s")
                time.sleep(wait)
                continue

            if res.status_code != 200:
                print(f"[TESTNET] GET {path} HTTP {res.status_code}: {res.text[:200]}")
                return None

            return res.json()

        except _RETRYABLE as e:
            print(
                f"[TESTNET] Network error on GET {path} "
                f"attempt {attempt+1}/{_MAX_RETRY_GET}: {type(e).__name__}"
            )
            if attempt < _MAX_RETRY_GET - 1:
                time.sleep(_RETRY_DELAYS[attempt])

        except Exception as e:
            print(f"[TESTNET] Unexpected error on GET {path}: {e}")
            return None

    print(f"[TESTNET] All {_MAX_RETRY_GET} GET attempts failed for {path}")
    return None


def _delete(path: str, params: dict) -> dict | None:
    """
    Signed DELETE to Binance Futures TESTNET.

    Used exclusively for cancelling orders.
    Not retried on failure — cancel state must be confirmed by caller
    via query_order() if needed.

    Returns:
      dict — parsed JSON response (may contain error code)
      None — network failure or timeout (cancel state UNKNOWN)
    """
    ok, api_key, api_secret = _require_testnet()
    if not ok:
        return None

    params = dict(params)
    params["timestamp"]  = int(time.time() * 1000)
    params["recvWindow"] = 5000
    params["signature"]  = _sign(params, api_secret)

    headers = {"X-MBX-APIKEY": api_key}
    url     = _TESTNET_BASE_URL + path

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
            print(f"[TESTNET] Rate limit on DELETE {path} — back off {wait}s")
            return None

        if res.status_code == 418:
            print("[TESTNET] IP banned (418) on TESTNET — halt all requests")
            return None

        data = res.json()

        if res.status_code != 200:
            code = data.get("code", "?") if isinstance(data, dict) else "?"
            msg  = data.get("msg",  res.text[:200]) if isinstance(data, dict) else res.text[:200]
            print(f"[TESTNET] DELETE {path} HTTP {res.status_code} — code={code} msg={msg}")
            return data

        return data

    except requests.exceptions.Timeout:
        print(
            f"[TESTNET] TIMEOUT on DELETE {path}. "
            "Cancel state UNKNOWN — query order to confirm."
        )
        return None

    except Exception as e:
        print(f"[TESTNET] Unexpected error on DELETE {path}: {e}")
        return None


# =====================================================================
# LEVERAGE & MARGIN TYPE SETUP
# =====================================================================

def _set_leverage(symbol: str, leverage: int) -> tuple:
    """
    Set leverage for symbol on TESTNET account via POST /fapi/v1/leverage.

    Called before every entry order. If leverage is already at the
    requested level, Binance returns success — safe to call repeatedly.

    Returns (success: bool, confirmed_leverage: int | None).
      confirmed_leverage is the value Binance actually applied —
      may differ from requested when the subaccount or symbol has
      a lower cap enforced by the exchange.

    Error code -4421: subaccount capped below requested leverage.
      → returns (False, None), warning logged, trade aborted.
    """
    print(f"[TESTNET] Setting leverage {leverage}x for {symbol}")

    data = _post("/fapi/v1/leverage", {
        "symbol":   symbol,
        "leverage": leverage,
    })

    if data is None:
        print(f"[TESTNET] _set_leverage: POST returned None for {symbol}")
        return False, None

    if isinstance(data, dict) and isinstance(data.get("code"), int) and data["code"] < 0:
        code = data["code"]
        msg  = data.get("msg", "")
        if code == -4421:
            print(
                f"[TESTNET] LEVERAGE REJECTED (code=-4421): {symbol} — "
                f"subaccount may be capped below {leverage}x. "
                f"msg={msg}. Trade skipped safely."
            )
        else:
            print(f"[TESTNET] _set_leverage error: code={code} msg={msg}")
        return False, None

    confirmed_raw = data.get("leverage") if isinstance(data, dict) else None
    confirmed     = int(confirmed_raw) if isinstance(confirmed_raw, (int, float)) else None

    print(f"[TESTNET] Leverage confirmed: {symbol} = {confirmed}x")
    return True, confirmed


def _set_margin_type(symbol: str) -> bool:
    """
    Set margin type to CROSSED for symbol on TESTNET via POST /fapi/v1/marginType.

    Binance returns error code -4046 when margin type is already CROSSED.
    This is NOT a failure — treat -4046 as success (already correct state).
    """
    print(f"[TESTNET] Setting margin type CROSSED for {symbol}")

    data = _post("/fapi/v1/marginType", {
        "symbol":     symbol,
        "marginType": "CROSSED",
    })

    if data is None:
        print(f"[TESTNET] _set_margin_type: POST returned None for {symbol}")
        return False

    if isinstance(data, dict) and data.get("code") in (-4046, -4067):
        print(f"[TESTNET] Margin type already CROSSED for {symbol} — OK (code={data.get('code')})")
        return True

    if isinstance(data, dict) and isinstance(data.get("code"), int) and data["code"] < 0:
        print(f"[TESTNET] _set_margin_type error: code={data['code']} msg={data.get('msg')}")
        return False

    print(f"[TESTNET] Margin type set: {symbol} = CROSSED")
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

    NOTE: This function is retained for backward-compat with any
    callers that pass an explicit prefix (e.g. cleanup utilities).
    New order placements should use _bot_client_order_id() instead
    so that the BOT_ prefix is present for ownership identification.
    """
    uid = uuid.uuid4().hex[:28]
    return f"{prefix}-{uid}"


def _bot_client_order_id(symbol: str, kind: str) -> str:
    """
    Generate a bot-tagged clientOrderId carrying the BOT_ ownership prefix.

    Mirrors live_executor._bot_client_order_id exactly so the ownership
    identification logic in execution.py works identically on testnet and live.

    Format: BOT_<BASE>_<KIND>_<hex12>  (total ≤ 28 chars, well within 36-char limit)
      BASE — symbol with USDT stripped, truncated to 8 chars
      KIND — E (entry) | S (stop) | TS (trailing stop) | EC (emergency close)
      hex12 — 12 hex chars from uuid4 for uniqueness

    Examples:
      BOT_XRP_E_a3f1c9e8b204   (testnet entry)
      BOT_BTC_S_7d09e4f3a100   (testnet stop)
      BOT_ETH_TS_9a1c2d88f501  (testnet trailing stop replacement)
      BOT_SOL_EC_bb3a01fc7de2  (testnet emergency close)

    The BOT_ prefix is checked by execution.py open_trade() to set
    t["exchange_position_owner_confirmed"] = True, providing
    exchange-level ownership proof on testnet exactly as on live.
    """
    base = (symbol or "").upper()
    if base.endswith("USDT"):
        base = base[:-4]
    base = base[:8]
    uid = uuid.uuid4().hex[:12]
    cid = f"BOT_{base}_{kind}_{uid}"
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
        print(f"[TESTNET VALIDATE] {symbol} rejected: {plan['reason']}")
        return {"valid": False, "reason": plan["reason"], "plan": plan}

    qty      = plan["rounded_qty"]
    leverage = plan["final_leverage"]
    margin   = plan["margin_required"]
    tier     = plan.get("tier") or get_symbol_tier(symbol)
    target   = plan.get("target_leverage")
    max_lev  = plan.get("allowed_leverage")
    clamp_reason = plan.get("leverage_clamp_reason")

    cap = get_max_leverage(symbol)
    if leverage > cap:
        reason = f"leverage {leverage}x exceeds tier cap {cap}x for {symbol}"
        print(f"[TESTNET VALIDATE] {reason}")
        return {"valid": False, "reason": reason, "plan": plan}

    clamp_suffix = f" reason={clamp_reason}" if clamp_reason else ""
    print(
        f"[LEVERAGE] {symbol} tier={tier} target={target} "
        f"max={max_lev} applied={leverage}{clamp_suffix}"
    )

    free = balance - margin
    if free < 0:
        reason = f"insufficient free margin: balance={balance} margin={margin}"
        print(f"[TESTNET VALIDATE] {reason}")
        return {"valid": False, "reason": reason, "plan": plan}

    print(
        f"[TESTNET VALIDATE] {symbol} {side_upper} OK — "
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
    Place a MARKET entry order on Binance Futures TESTNET.

    Execution steps (always in this order):
      1. _require_testnet() guard
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
    ok, _, _ = _require_testnet()
    if not ok:
        return _failed_result("testnet_mode not enabled", client_order_id)

    side_upper = (side or "").upper()
    if side_upper not in ("BUY", "SELL"):
        return _failed_result(f"invalid side: {side!r}", client_order_id)

    if qty is None or qty <= 0:
        return _failed_result(f"invalid qty: {qty}", client_order_id)

    if client_order_id is None:
        # Use BOT_-prefixed id so execution.py can set exchange_position_owner_confirmed=True
        # and ownership integrity is verified on testnet identically to live.
        client_order_id = _bot_client_order_id(symbol, "E")

    direction = "LONG" if side_upper == "BUY" else "SHORT"
    print(f"[TESTNET ENTRY] {symbol} {direction}  qty={qty}  leverage={leverage}x")
    print(f"[TESTNET ENTRY] clientOrderId={client_order_id}")

    lev_ok, confirmed_lev = _set_leverage(symbol, leverage)
    if not lev_ok:
        return _failed_result(f"leverage setup failed for {symbol}", client_order_id)

    # ── Confirmed leverage validation ─────────────────────────────────
    if confirmed_lev is not None:
        if confirmed_lev != leverage:
            print(
                f"[TESTNET] LEVERAGE ADJUSTED: {symbol} "
                f"requested={leverage}x confirmed={confirmed_lev}x"
            )

        min_lev = get_min_acceptable_leverage(symbol)
        if confirmed_lev < min_lev:
            tier = get_symbol_tier(symbol)
            print(
                f"[TESTNET] TRADE REJECTED: {symbol} leverage confirmation too low — "
                f"requested={leverage}x confirmed={confirmed_lev}x "
                f"minimum_required={min_lev}x ({tier})"
            )
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
            f"[TESTNET ENTRY] POST returned None — clientOrderId={client_order_id}. "
            "Order state UNKNOWN. Query before any retry."
        )
        return _failed_result(
            "POST timeout — order state unknown, call query_order before retry",
            client_order_id,
        )

    if isinstance(raw, dict) and isinstance(raw.get("code"), int) and raw["code"] < 0:
        err = f"code={raw['code']} msg={raw.get('msg', '?')}"
        print(f"[TESTNET ENTRY] Rejected: {err}")
        return _failed_result(err, client_order_id, raw=raw)

    order_id   = raw.get("orderId")
    status     = raw.get("status", "?")
    fill_price = _safe_float(raw.get("avgPrice"))
    fill_qty   = _safe_float(raw.get("executedQty"))

    effective_lev = confirmed_lev if confirmed_lev is not None else leverage
    print(
        f"[TESTNET ENTRY] ACCEPTED — "
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
    Place a STOP_MARKET stop-loss order on Binance Futures TESTNET.

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
    ok, _, _ = _require_testnet()
    if not ok:
        return _failed_result("testnet_mode not enabled", client_order_id)

    entry_upper = (entry_side or "").upper()
    if entry_upper not in ("BUY", "SELL"):
        return _failed_result(f"invalid entry_side: {entry_side!r}", client_order_id)

    if qty is None or qty <= 0:
        return _failed_result(f"invalid qty: {qty}", client_order_id)

    if stop_price is None or stop_price <= 0:
        return _failed_result(f"invalid stop_price: {stop_price}", client_order_id)

    close_side = "SELL" if entry_upper == "BUY" else "BUY"

    if client_order_id is None:
        # BOT_-prefix ensures this stop order is identifiable as bot-originated
        # in exchange order history (defense-in-depth alongside t["owner"]="bot").
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

    # Binance Futures testnet requires conditional stop orders to go through
    # /fapi/v1/algoOrder with algoType=CONDITIONAL and triggerPrice.
    # The old /fapi/v1/order endpoint with type=STOP_MARKET returns -4120 on testnet.
    _stop_params = {
        "symbol":        symbol,
        "side":          close_side,
        "type":          "STOP_MARKET",
        "algoType":      "CONDITIONAL",
        "triggerPrice":  _stop_str,
        "closePosition": "true",
        "workingType":   "MARK_PRICE",
    }

    print(
        f"[TESTNET STOP]  {symbol}  STOP_MARKET(algo)  "
        f"side={close_side}  triggerPrice={_stop_str}  closePosition=true  workingType=MARK_PRICE"
    )
    print(f"[TESTNET STOP]  clientOrderId={client_order_id}")

    if close_side == "BUY":
        print(
            f"[TESTNET DEBUG] SHORT stop direction: "
            f"BUY STOP_MARKET triggerPrice={_stop_str} (raw_input={stop_price}) "
            f"— must be > current markPrice at placement"
        )
    else:
        print(
            f"[TESTNET DEBUG] LONG stop direction: "
            f"SELL STOP_MARKET triggerPrice={_stop_str} (raw_input={stop_price}) "
            f"— must be < current markPrice at placement"
        )

    print(f"[TESTNET DEBUG] STOP payload:\n{json.dumps(_stop_params, indent=2)}")

    raw = _post("/fapi/v1/algoOrder", _stop_params)

    print(
        f"[TESTNET DEBUG] STOP response:\n"
        f"{json.dumps(raw, indent=2) if isinstance(raw, dict) else raw}"
    )

    if raw is None:
        print(
            f"[TESTNET STOP] POST returned None — clientOrderId={client_order_id}. "
            "SL state UNKNOWN. Query before any retry."
        )
        return _failed_result(
            "POST timeout — SL state unknown, call query_order before retry",
            client_order_id,
        )

    if isinstance(raw, dict) and isinstance(raw.get("code"), int) and raw["code"] < 0:
        err = f"code={raw['code']} msg={raw.get('msg', '?')}"
        print(f"[TESTNET STOP] Rejected: {err}")
        return _failed_result(err, client_order_id, raw=raw)

    order_id = raw.get("algoId")
    status   = raw.get("algoStatus", "?")

    print(
        f"[TESTNET STOP] ACCEPTED — "
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
) -> dict | None:
    """
    Query a single order by orderId or clientOrderId on TESTNET.

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
        print("[TESTNET QUERY] must provide order_id or client_order_id")
        return None

    params = {"symbol": symbol}
    if order_id is not None:
        params["orderId"] = order_id
    if client_order_id is not None:
        params["origClientOrderId"] = client_order_id

    label = f"orderId={order_id}" if order_id else f"clientOrderId={client_order_id}"
    print(f"[TESTNET QUERY] {symbol}  {label}")

    data = _get_signed("/fapi/v1/order", params)

    if data is None:
        print(f"[TESTNET QUERY] No response for {symbol} — treat as unknown")
        return None

    if isinstance(data, dict) and isinstance(data.get("code"), int) and data["code"] < 0:
        code = data["code"]
        msg  = data.get("msg", "?")
        if code == -2013:
            print(f"[TESTNET QUERY] Order does not exist ({code}) — safe to retry")
        else:
            print(f"[TESTNET QUERY] Error: code={code} msg={msg}")
        return None

    status = data.get("status", "?")
    oid    = data.get("orderId")
    print(f"[TESTNET QUERY] Found: orderId={oid}  status={status}")
    return data


def query_algo_order(symbol: str, algo_id: int) -> dict | None:
    """
    Query an algo stop order by algoId via GET /fapi/v1/openAlgoOrders.

    Algo orders (placed via /fapi/v1/algoOrder) cannot be queried via
    /fapi/v1/order — they require the algo orders list endpoint.

    Returns the matching algo order dict if found and still open,
    or None if not found (may have been triggered or cancelled).
    """
    print(f"[TESTNET QUERY ALGO] {symbol}  algoId={algo_id}")
    data = _get_signed("/fapi/v1/openAlgoOrders", {"symbol": symbol})

    if data is None:
        print(f"[TESTNET QUERY ALGO] No response for {symbol}")
        return None

    if not isinstance(data, list):
        print(f"[TESTNET QUERY ALGO] Unexpected response type: {type(data)}")
        return None

    for order in data:
        if order.get("algoId") == algo_id:
            status = order.get("algoStatus", "?")
            print(f"[TESTNET QUERY ALGO] Found: algoId={algo_id}  algoStatus={status}")
            return order

    print(f"[TESTNET QUERY ALGO] algoId={algo_id} not in open algo orders for {symbol}")
    return None


def get_open_algo_orders(symbol: str) -> list | None:
    """
    Return all open algo orders for symbol via GET /fapi/v1/openAlgoOrders.

    Used for pre-placement duplicate checks (Scenario 12) and
    post-update orphan verification (Scenario 10).

    Returns:
      list  — algo order dicts (may be empty [])
      None  — query failed or unexpected response (caller must treat as uncertain,
              NOT as confirmed absence of stops)
    """
    print(f"[TESTNET QUERY ALGO] list open algo orders for {symbol}")
    data = _get_signed("/fapi/v1/openAlgoOrders", {"symbol": symbol})

    if data is None:
        print(f"[TESTNET QUERY ALGO] no response for {symbol}")
        return None

    if not isinstance(data, list):
        print(f"[TESTNET QUERY ALGO] unexpected response type for {symbol}: {type(data)}")
        return None

    print(f"[TESTNET QUERY ALGO] {len(data)} open algo order(s) for {symbol}")
    return data


# =====================================================================
# CANCEL STOP ORDER
# =====================================================================

def cancel_stop_loss(symbol: str, order_id: int) -> dict:
    """
    Cancel an existing STOP_MARKET order by orderId on TESTNET.

    Called during trailing stop synchronization: after a new stop is
    successfully placed, the old stop is cancelled via this function.

    A -2011 response (Unknown Order) is treated as success — the order
    was already filled or cancelled before this call arrived.

    Returns dict:
      success   bool
      order_id  int | None
      error     str | None
    """
    ok, _, _ = _require_testnet()
    if not ok:
        return {"success": False, "order_id": None, "error": "testnet_mode not enabled"}

    if not order_id:
        return {"success": False, "order_id": None, "error": "no order_id provided"}

    print(f"[TESTNET CANCEL] {symbol} cancel stop algoId={order_id}")

    # Algo orders are cancelled via DELETE /fapi/v1/algoOrder with algoId.
    # The response on success: {"algoId": ..., "code": "200", "msg": "success"}
    raw = _delete("/fapi/v1/algoOrder", {"algoId": order_id})

    if raw is None:
        print(
            f"[TESTNET CANCEL] {symbol} DELETE returned None — cancel state UNKNOWN algoId={order_id}"
        )
        return {"success": False, "order_id": order_id, "error": "DELETE timeout — cancel state unknown"}

    if isinstance(raw, dict) and isinstance(raw.get("code"), int) and raw["code"] < 0:
        code = raw["code"]
        msg  = raw.get("msg", "?")
        if code == -2011:
            print(f"[TESTNET CANCEL] {symbol} algoId={order_id} already cancelled or filled — OK")
            return {"success": True, "order_id": order_id, "error": None}
        print(f"[TESTNET CANCEL] {symbol} REJECTED: code={code} msg={msg}")
        return {"success": False, "order_id": order_id, "error": f"code={code} msg={msg}"}

    # Success response has code="200" (string) — treat any non-error response as success
    msg = raw.get("msg", "?") if isinstance(raw, dict) else "?"
    print(f"[TESTNET CANCEL] {symbol} cancelled algoId={order_id}  msg={msg}")
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
    ok, _, _ = _require_testnet()
    if not ok:
        return {
            "success": False, "new_order_id": None,
            "cancel_ok": False, "error": "testnet_mode not enabled",
        }

    print(
        f"[TESTNET TRAIL] {symbol} update stop: "
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

    _trail_params = {
        "symbol":       symbol,
        "side":         close_side,
        "type":         "STOP_MARKET",
        "algoType":     "CONDITIONAL",
        "triggerPrice": _stop_str,
        "quantity":     qty,
        "reduceOnly":   "true",
        "workingType":  "MARK_PRICE",
    }

    print(f"[TESTNET TRAIL] {symbol} placing new stop (qty+reduceOnly) triggerPrice={_stop_str}")
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
            f"[TESTNET TRAIL] {symbol} new stop FAILED — old stop PRESERVED. error={err}"
        )
        return {
            "success": False, "new_order_id": None,
            "cancel_ok": False, "error": err,
        }

    new_order_id = raw_new.get("algoId")
    print(
        f"[TESTNET TRAIL] {symbol} new stop placed — "
        f"orderId={new_order_id}  stopPrice={new_stop_price}"
    )

    cancel_result = cancel_stop_loss(symbol=symbol, order_id=old_order_id)

    if cancel_result.get("success"):
        print(f"[TESTNET TRAIL] {symbol} old stop cancelled orderId={old_order_id}")
        return {
            "success": True, "new_order_id": new_order_id,
            "cancel_ok": True, "error": None,
        }

    print(
        f"[TESTNET TRAIL] {symbol} old stop cancel FAILED orderId={old_order_id} — "
        f"BOTH stops active. Position protected by new stop orderId={new_order_id}."
    )
    return {
        "success": True, "new_order_id": new_order_id,
        "cancel_ok": False, "error": None,
    }


# =====================================================================
# RECONCILIATION HELPERS
# =====================================================================

def get_exchange_positions() -> list:
    """
    Return all open positions from TESTNET account.

    Fetches from GET /fapi/v2/positionRisk on testnet.
    Filters to non-zero positionAmt only.

    Returns list of dicts:
      symbol, positionAmt (float, signed: + long / - short),
      entryPrice (float), unrealizedProfit (float),
      leverage (int), marginType (str)

    Returns [] on any failure.
    """
    data = _get_signed("/fapi/v2/positionRisk", {})

    if data is None:
        print("[TESTNET RECON] get_exchange_positions failed")
        return []

    if not isinstance(data, list):
        print(f"[TESTNET RECON] unexpected type: {type(data)}")
        return []

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
        })

    print(f"[TESTNET RECON] {len(positions)} open position(s) found on exchange")
    return positions


def compare_local_vs_exchange(
    local_positions:    list,
    exchange_positions: list,
) -> dict:
    """
    Compare local bot state against live exchange positions.

    Surfaces discrepancies before any mainnet promotion.

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
    ex_by_symbol = {p["symbol"]: p for p in exchange_positions}
    lc_by_symbol = {p["symbol"]: p for p in local_positions}

    matched       = []
    local_only    = []
    exchange_only = []
    discrepancies = []

    for sym, lc in lc_by_symbol.items():
        if sym not in ex_by_symbol:
            local_only.append({"symbol": sym, "local": lc, "exchange": None})
            print(f"[TESTNET RECON] LOCAL ONLY:    {sym} — not found on exchange")
            continue

        ex     = ex_by_symbol[sym]
        ex_amt = round(ex["positionAmt"], 8)

        lc_qty  = float(lc.get("qty", 0.0))
        lc_dir  = (lc.get("direction") or lc.get("side") or "LONG").upper()
        expected = round(lc_qty if lc_dir == "LONG" else -lc_qty, 8)

        if abs(expected - ex_amt) > 0.0001:
            discrepancies.append({
                "symbol":   sym,
                "local":    lc,
                "exchange": ex,
                "expected_signed_qty": expected,
                "actual_signed_qty":   ex_amt,
            })
            print(
                f"[TESTNET RECON] MISMATCH:      {sym} — "
                f"local={expected}  exchange={ex_amt}"
            )
        else:
            matched.append({"symbol": sym, "local": lc, "exchange": ex})
            print(f"[TESTNET RECON] MATCHED:       {sym}  qty={ex_amt}")

    for sym, ex in ex_by_symbol.items():
        if sym not in lc_by_symbol:
            exchange_only.append({"symbol": sym, "local": None, "exchange": ex})
            print(
                f"[TESTNET RECON] EXCHANGE ONLY: {sym}  "
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
      symbol:     e.g. "SOLUSDT"
      entry_side: the entry direction ("BUY" or "SELL") — close side is derived
      qty:        quantity matching the open position

    Returns same dict shape as place_market_order.
    """
    ok, _, _ = _require_testnet()
    if not ok:
        return _failed_result("testnet_mode not enabled", client_order_id)

    entry_upper = (entry_side or "").upper()
    if entry_upper not in ("BUY", "SELL"):
        return _failed_result(f"invalid entry_side: {entry_side!r}", client_order_id)

    if qty is None or qty <= 0:
        return _failed_result(f"invalid qty: {qty}", client_order_id)

    close_side = "SELL" if entry_upper == "BUY" else "BUY"

    if client_order_id is None:
        # BOT_-prefix for emergency close — consistent ownership tagging across all order types.
        client_order_id = _bot_client_order_id(symbol, "EC")

    print(
        f"[TESTNET EMERGENCY CLOSE] {symbol}  {close_side}  qty={qty}  reduceOnly=true"
    )
    print(f"[TESTNET EMERGENCY CLOSE] clientOrderId={client_order_id}")

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
            f"[TESTNET EMERGENCY CLOSE] POST returned None — clientOrderId={client_order_id}. "
            "Close state UNKNOWN — position may still be open."
        )
        return _failed_result(
            "emergency close POST timeout — position state unknown",
            client_order_id,
        )

    if isinstance(raw, dict) and isinstance(raw.get("code"), int) and raw["code"] < 0:
        err = f"code={raw['code']} msg={raw.get('msg', '?')}"
        print(f"[TESTNET EMERGENCY CLOSE] REJECTED: {err}")
        return _failed_result(err, client_order_id, raw=raw)

    order_id   = raw.get("orderId")
    status     = raw.get("status", "?")
    fill_price = _safe_float(raw.get("avgPrice"))
    fill_qty   = _safe_float(raw.get("executedQty"))

    print(
        f"[TESTNET EMERGENCY CLOSE] ACCEPTED — "
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

    If testnet_mode is False, prints a single inactive notice.
    """
    cfg  = _load_config()
    mode = cfg.get("testnet_mode", False)

    if not mode:
        print("[EXCHANGE] testnet_mode=false — testnet executor inactive")
        return

    key = cfg.get("testnet_api_key", "")
    key_preview = key[:8] + "..." if len(key) >= 8 else "(empty)"

    eb = get_execution_balance(wallet_balance)
    eb_str = f"${eb:.2f} USDT" if eb is not None else "NOT SET — config missing"

    print("=" * 56)
    print("[EXCHANGE] *** TESTNET MODE ACTIVE ***")
    print(f"[EXCHANGE] URL              : {_TESTNET_BASE_URL}")
    print(f"[EXCHANGE] API key          : {key_preview}")
    print(f"[EXECUTION] Effective execution balance : {eb_str}")
    if wallet_balance is not None:
        print(f"[EXECUTION] Exchange wallet balance     : ${float(wallet_balance):.2f} USDT")
    print("[EXCHANGE] No real money at risk.")
    print("[EXCHANGE] All orders → Binance Futures TESTNET.")
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
