"""
Read-only Binance Futures REST client.

SAFE BOUNDARIES — this module MUST NOT:
  - Place, modify, or cancel orders
  - Change leverage or margin type
  - Transfer funds
  - Touch any /order, /batchOrders, /leverage, /transfer endpoint

Authenticated endpoints use HMAC-SHA256 (timestamp + signature).
API key and secret are read from config.json: "api_key" / "api_secret".
"""

import hashlib
import hmac
import json
import os
import time
import urllib.parse

import certifi
import requests

# =====================================================================
# CONSTANTS
# =====================================================================

_BASE_URL   = "https://fapi.binance.com"
_TIMEOUT    = (4, 12)   # (connect, read) — matches pool_pipeline.py
_MAX_RETRY  = 3
_RETRY_DELAYS = [0.5, 1.0, 2.0]

# =====================================================================
# CONFIG LOADER (isolated — does NOT import from config.py)
# =====================================================================

_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config.json")

def _load_keys():
    """Return (api_key, api_secret) from config.json. Empty strings if absent."""
    try:
        with open(_CONFIG_PATH, "r") as f:
            cfg = json.load(f)
        return cfg.get("api_key", ""), cfg.get("api_secret", "")
    except Exception as e:
        print(f"[EXCHANGE] config.json read error: {e}")
        return "", ""

# =====================================================================
# HMAC-SHA256 SIGNING
# =====================================================================

def _sign(params: dict, api_secret: str) -> str:
    """
    Return HMAC-SHA256 hex-digest of the URL-encoded param string.
    Binance requires the signature appended as &signature=<hex>.
    """
    query = urllib.parse.urlencode(params)
    return hmac.new(
        api_secret.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

# =====================================================================
# REQUEST WRAPPER
# =====================================================================

def _get(path: str, params: dict = None, signed: bool = False):
    """
    GET request against Binance Futures REST API.

    signed=True  → injects timestamp + signature; requires api_key/api_secret
    signed=False → public endpoint, no credentials needed

    Returns:
        dict | list — parsed JSON on success
        None        — on any failure (network, auth, HTTP error, parse error)
    """
    params = dict(params) if params else {}

    api_key, api_secret = ("", "") if not signed else _load_keys()

    if signed:
        if not api_key or not api_secret:
            print(f"[EXCHANGE] Signed request to {path} skipped — api_key/api_secret missing in config.json")
            return None

        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 5000
        params["signature"] = _sign(params, api_secret)

    headers = {"X-MBX-APIKEY": api_key} if signed else {}
    url     = _BASE_URL + path

    _RETRYABLE = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.ReadTimeout,
        requests.exceptions.ConnectTimeout,
    )

    for attempt in range(_MAX_RETRY):
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
                print(f"[EXCHANGE] Rate limit hit {path} — waiting {wait}s")
                time.sleep(wait)
                continue

            if res.status_code == 418:
                print(f"[EXCHANGE] IP banned (418) — backing off 60s")
                time.sleep(60)
                return None

            if res.status_code == 401:
                print(f"[EXCHANGE] Auth error (401) on {path} — check api_key/api_secret")
                return None

            if res.status_code != 200:
                print(f"[EXCHANGE] HTTP {res.status_code} on {path}: {res.text[:200]}")
                return None

            return res.json()

        except _RETRYABLE as e:
            print(f"[EXCHANGE] Network error on {path} attempt {attempt+1}/{_MAX_RETRY}: {type(e).__name__}")
            if attempt < _MAX_RETRY - 1:
                time.sleep(_RETRY_DELAYS[attempt])

        except Exception as e:
            print(f"[EXCHANGE] Unexpected error on {path}: {e}")
            return None

    print(f"[EXCHANGE] All {_MAX_RETRY} attempts failed for {path}")
    return None

# =====================================================================
# PUBLIC API — READ-ONLY FUNCTIONS
# =====================================================================

def ping() -> bool:
    """
    Test connectivity to Binance Futures.
    Returns True if reachable, False otherwise.
    No authentication required.
    """
    result = _get("/fapi/v1/ping")
    if result is not None:
        print("[EXCHANGE] ping OK — Binance Futures reachable")
        return True
    print("[EXCHANGE] ping FAILED — Binance Futures not reachable")
    return False


def get_account_balance() -> float | None:
    """
    Return available USDT balance in the Futures wallet.
    Returns:
        float — available USDT balance
        None  — on any error (auth failure, network error, etc.)
    Requires: api_key + api_secret in config.json
    Endpoint: GET /fapi/v2/balance (signed)
    """
    data = _get("/fapi/v2/balance", signed=True)

    if data is None:
        return None

    if not isinstance(data, list):
        print(f"[EXCHANGE] get_account_balance: unexpected response type {type(data)}")
        return None

    for asset in data:
        if asset.get("asset") == "USDT":
            try:
                balance = float(asset["availableBalance"])
                print(f"[EXCHANGE] USDT available balance: {balance}")
                return balance
            except (KeyError, ValueError) as e:
                print(f"[EXCHANGE] get_account_balance: parse error: {e}")
                return None

    print("[EXCHANGE] get_account_balance: USDT asset not found in response")
    return None


def get_open_positions() -> list:
    """
    Return all futures positions with non-zero position size.
    Returns:
        list of dicts — each dict has: symbol, positionSide, positionAmt,
                        entryPrice, unrealizedProfit, leverage, marginType
        []            — on any error or no open positions
    Requires: api_key + api_secret in config.json
    Endpoint: GET /fapi/v2/positionRisk (signed)
    """
    data = _get("/fapi/v2/positionRisk", signed=True)

    if data is None:
        return []

    if not isinstance(data, list):
        print(f"[EXCHANGE] get_open_positions: unexpected response type {type(data)}")
        return []

    open_positions = []
    for pos in data:
        try:
            amt = float(pos.get("positionAmt", 0))
        except (ValueError, TypeError):
            continue

        if amt == 0.0:
            continue

        open_positions.append({
            "symbol":           pos.get("symbol", ""),
            "positionSide":     pos.get("positionSide", "BOTH"),
            "positionAmt":      amt,
            "entryPrice":       float(pos.get("entryPrice", 0)),
            "unrealizedProfit": float(pos.get("unRealizedProfit", 0)),
            "leverage":         int(pos.get("leverage", 0)),
            "marginType":       pos.get("marginType", ""),
        })

    print(f"[EXCHANGE] get_open_positions: {len(open_positions)} open position(s)")
    return open_positions


def get_exchange_info(symbol: str = None) -> dict:
    """
    Fetch symbol precision and filter info from Binance Futures.

    Args:
        symbol: Optional. If provided, returns info for that symbol only.
                If None, returns the full exchangeInfo dict.
    Returns:
        dict — full exchangeInfo, or single symbol info dict if symbol given
        {}   — on any error or symbol not found
    No authentication required.
    Endpoint: GET /fapi/v1/exchangeInfo (public)

    Key fields per symbol:
        pricePrecision    — decimal places for price
        quantityPrecision — decimal places for quantity
        filters:
          PRICE_FILTER    → tickSize
          LOT_SIZE        → stepSize, minQty, maxQty
          MIN_NOTIONAL    → notional (minimum order value in USDT)
    """
    params = {}
    if symbol:
        params["symbol"] = symbol

    data = _get("/fapi/v1/exchangeInfo", params=params)

    if data is None:
        return {}

    if not isinstance(data, dict):
        print(f"[EXCHANGE] get_exchange_info: unexpected response type {type(data)}")
        return {}

    if symbol:
        symbols_list = data.get("symbols", [])
        for s in symbols_list:
            if s.get("symbol") == symbol:
                print(f"[EXCHANGE] get_exchange_info: found {symbol}")
                return s
        print(f"[EXCHANGE] get_exchange_info: symbol {symbol} not found")
        return {}

    symbol_count = len(data.get("symbols", []))
    print(f"[EXCHANGE] get_exchange_info: {symbol_count} symbol(s) loaded")
    return data
