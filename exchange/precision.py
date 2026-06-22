"""
Binance Futures precision and order-filter validation layer.

SAFE BOUNDARIES — this module MUST NOT:
  - Place, modify, or cancel orders
  - Call any signed endpoint
  - Import from execution.py or entry.py

Public API:
  get_symbol_filters(symbol)        → dict   | None
  round_qty(symbol, qty)            → float  | None
  round_price(symbol, price)        → float  | None
  validate_min_qty(symbol, qty)     → (bool, str)
  validate_min_notional(symbol, qty, price) → (bool, float, float)
  validate_order(symbol, qty, price)        → dict
"""

import time
from decimal import Decimal, ROUND_DOWN, InvalidOperation

from .binance_client import get_exchange_info

# =====================================================================
# FILTER CACHE
# =====================================================================

_filter_cache: dict = {}   # symbol → {"filters": dict, "ts": float}
_CACHE_TTL = 3600          # 1 hour — exchangeInfo changes rarely

# =====================================================================
# INTERNAL — PARSE RAW exchangeInfo SYMBOL ENTRY
# =====================================================================

def _parse_filters(info: dict) -> dict | None:
    """
    Extract the fields we care about from a single symbol's exchangeInfo entry.

    Binance Futures filter keys used here:
      PRICE_FILTER  → tickSize, minPrice, maxPrice
      LOT_SIZE      → stepSize, minQty, maxQty
      MIN_NOTIONAL  → notional  (futures uses "notional", not "minNotional")

    Returns None if any critical filter is missing (LOT_SIZE or PRICE_FILTER).
    MIN_NOTIONAL absence is treated as 0 (no minimum) — some pairs omit it.
    """
    symbol = info.get("symbol", "UNKNOWN")

    # Index filters by type for O(1) lookup
    filter_map: dict = {}
    for f in info.get("filters", []):
        filter_map[f.get("filterType", "")] = f

    # ----- LOT_SIZE (required) -----
    lot = filter_map.get("LOT_SIZE")
    if not lot:
        print(f"[PRECISION] {symbol}: LOT_SIZE filter missing — cannot validate")
        return None

    try:
        step_size = Decimal(lot["stepSize"])
        min_qty   = Decimal(lot["minQty"])
        max_qty   = Decimal(lot["maxQty"])
    except (KeyError, InvalidOperation) as e:
        print(f"[PRECISION] {symbol}: LOT_SIZE parse error: {e}")
        return None

    # ----- PRICE_FILTER (required) -----
    pf = filter_map.get("PRICE_FILTER")
    if not pf:
        print(f"[PRECISION] {symbol}: PRICE_FILTER filter missing — cannot validate")
        return None

    try:
        tick_size = Decimal(pf["tickSize"])
        min_price = Decimal(pf.get("minPrice", "0"))
        max_price = Decimal(pf.get("maxPrice", "0"))
    except (KeyError, InvalidOperation) as e:
        print(f"[PRECISION] {symbol}: PRICE_FILTER parse error: {e}")
        return None

    # ----- MIN_NOTIONAL (optional — futures key is "notional") -----
    mn = filter_map.get("MIN_NOTIONAL", {})
    try:
        # Futures uses "notional"; spot uses "minNotional" — handle both
        raw_notional = mn.get("notional") or mn.get("minNotional") or "0"
        min_notional = Decimal(str(raw_notional))
    except InvalidOperation:
        min_notional = Decimal("0")

    return {
        "symbol":             symbol,
        "price_precision":    int(info.get("pricePrecision",    8)),
        "quantity_precision": int(info.get("quantityPrecision", 8)),
        "tick_size":          tick_size,
        "min_price":          min_price,
        "max_price":          max_price,
        "step_size":          step_size,
        "min_qty":            min_qty,
        "max_qty":            max_qty,
        "min_notional":       min_notional,
    }

# =====================================================================
# INTERNAL — CACHE-AWARE FILTER LOOKUP
# =====================================================================

def _get_filters_cached(symbol: str) -> dict | None:
    """
    Return parsed filters for symbol, using in-memory cache.
    Refreshes from API when TTL expires or entry is absent.
    Returns None on any fetch or parse failure.
    """
    now = time.time()
    cached = _filter_cache.get(symbol)

    if cached and (now - cached["ts"]) < _CACHE_TTL:
        return cached["filters"]

    info = get_exchange_info(symbol)
    if not info:
        print(f"[PRECISION] {symbol}: get_exchange_info returned empty — using stale cache")
        # Return stale data rather than None if we have something
        if cached:
            return cached["filters"]
        return None

    filters = _parse_filters(info)
    if filters is None:
        # Parse failed — keep stale if available
        if cached:
            print(f"[PRECISION] {symbol}: using stale cache after parse failure")
            return cached["filters"]
        return None

    _filter_cache[symbol] = {"filters": filters, "ts": now}
    return filters

# =====================================================================
# INTERNAL — DECIMAL FLOOR TO STEP
# =====================================================================

def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    """
    Floor `value` to the nearest multiple of `step` (always rounds DOWN).

    Uses integer division: (value // step) * step
    This correctly handles non-power-of-10 steps (e.g. 0.0025, 2.5, 500).

    Examples:
      _floor_to_step(Decimal("0.001782"), Decimal("0.001"))  → Decimal("0.001")
      _floor_to_step(Decimal("47391.17"), Decimal("0.10"))   → Decimal("47391.10")
      _floor_to_step(Decimal("0.00178"),  Decimal("0.0025")) → Decimal("0.0000")
    """
    if step <= 0:
        return value
    return (value // step) * step

# =====================================================================
# PUBLIC API
# =====================================================================

def get_symbol_filters(symbol: str) -> dict | None:
    """
    Return all precision/filter data for a symbol.

    Return dict keys:
      symbol             str
      price_precision    int   — decimal places for price (from exchangeInfo)
      quantity_precision int   — decimal places for qty   (from exchangeInfo)
      tick_size          Decimal — smallest price increment
      min_price          Decimal — minimum allowed price (0 = no minimum)
      max_price          Decimal — maximum allowed price (0 = no maximum)
      step_size          Decimal — smallest quantity increment
      min_qty            Decimal — minimum order quantity
      max_qty            Decimal — maximum order quantity
      min_notional       Decimal — minimum order value in USDT (0 = no minimum)

    Returns None if filters cannot be fetched or parsed.

    Example — BTCUSDT:
      step_size=0.001, tick_size=0.10, min_notional=5 (USDT)

    Example — low-cap SNDKUSDT:
      step_size varies, min_notional may be 1–5 USDT, low liquidity
    """
    return _get_filters_cached(symbol)


def round_qty(symbol: str, qty: float) -> float | None:
    """
    Round quantity DOWN to the nearest valid stepSize for symbol.

    ALWAYS floors — never rounds up — to avoid LOT_SIZE rejection.

    Args:
      symbol: e.g. "BTCUSDT"
      qty:    raw float quantity before rounding

    Returns:
      float — floored quantity, or None if filters unavailable

    Examples (BTCUSDT stepSize=0.001):
      round_qty("BTCUSDT", 0.001782) → 0.001
      round_qty("BTCUSDT", 0.009999) → 0.009
      round_qty("BTCUSDT", 0.001000) → 0.001

    Examples (ETHUSDT stepSize=0.001):
      round_qty("ETHUSDT", 1.2349)   → 1.234
    """
    filters = _get_filters_cached(symbol)
    if filters is None:
        print(f"[PRECISION] round_qty: no filters for {symbol} — returning None")
        return None

    try:
        qty_d    = Decimal(str(qty))
        step     = filters["step_size"]
        floored  = _floor_to_step(qty_d, step)
        result   = float(floored)
        if result != qty:
            print(f"[PRECISION] round_qty {symbol}: {qty} → {result} (step={step})")
        return result
    except (InvalidOperation, Exception) as e:
        print(f"[PRECISION] round_qty {symbol} error: {e}")
        return None


def round_price(symbol: str, price: float) -> float | None:
    """
    Round price DOWN to the nearest valid tickSize for symbol.

    Args:
      symbol: e.g. "BTCUSDT"
      price:  raw float price

    Returns:
      float — floored price, or None if filters unavailable

    Examples (BTCUSDT tickSize=0.10):
      round_price("BTCUSDT", 47391.17) → 47391.1
      round_price("BTCUSDT", 47391.99) → 47391.9

    Examples (XAUUSDT tickSize=0.01):
      round_price("XAUUSDT", 4739.647) → 4739.64
    """
    filters = _get_filters_cached(symbol)
    if filters is None:
        print(f"[PRECISION] round_price: no filters for {symbol} — returning None")
        return None

    try:
        price_d  = Decimal(str(price))
        tick     = filters["tick_size"]
        floored  = _floor_to_step(price_d, tick)
        result   = float(floored)
        if result != price:
            print(f"[PRECISION] round_price {symbol}: {price} → {result} (tick={tick})")
        return result
    except (InvalidOperation, Exception) as e:
        print(f"[PRECISION] round_price {symbol} error: {e}")
        return None


def validate_min_qty(symbol: str, qty: float) -> tuple[bool, str]:
    """
    Check whether qty meets the LOT_SIZE minQty requirement.

    Args:
      symbol: e.g. "BTCUSDT"
      qty:    quantity AFTER rounding (use round_qty first)

    Returns:
      (True,  "OK")
      (False, reason_string)

    Examples:
      validate_min_qty("BTCUSDT", 0.001)   → (True,  "OK")
      validate_min_qty("BTCUSDT", 0.0001)  → (False, "qty 0.0001 < minQty 0.001")
      validate_min_qty("BTCUSDT", 0.0)     → (False, "qty 0.0 < minQty 0.001")
    """
    filters = _get_filters_cached(symbol)
    if filters is None:
        return False, f"filters unavailable for {symbol}"

    try:
        qty_d   = Decimal(str(qty))
        min_qty = filters["min_qty"]
        max_qty = filters["max_qty"]

        if qty_d <= Decimal("0"):
            return False, f"qty {qty} <= 0"

        if qty_d < min_qty:
            return False, f"qty {qty} < minQty {min_qty}"

        if max_qty > Decimal("0") and qty_d > max_qty:
            return False, f"qty {qty} > maxQty {max_qty}"

        return True, "OK"

    except (InvalidOperation, Exception) as e:
        return False, f"validate_min_qty error: {e}"


def validate_min_notional(
    symbol: str,
    qty: float,
    price: float,
) -> tuple[bool, float, float]:
    """
    Check whether qty * price meets the MIN_NOTIONAL requirement.

    Args:
      symbol: e.g. "BTCUSDT"
      qty:    quantity AFTER rounding
      price:  price AFTER rounding

    Returns:
      (valid: bool, actual_notional: float, required_min: float)

    Binance Futures MIN_NOTIONAL note:
      - The filter uses key "notional" (not "minNotional" — that is spot).
      - If the filter is absent, min_notional = 0 → always passes.
      - Common values: $5 for majors (BTC, ETH), $1–$5 for alts.
      - At $10 account with 1% risk ($0.10 risk), even with 10x leverage
        the notional may fall below the minimum for expensive symbols.

    Examples:
      validate_min_notional("BTCUSDT", 0.001, 60000)
        → (True, 60.0, 5.0)     ✓ $60 notional ≥ $5 minimum

      validate_min_notional("BTCUSDT", 0.001, 4.9)
        → (False, 0.0049, 5.0)  ✗ under minimum (unrealistic price but shows logic)
    """
    filters = _get_filters_cached(symbol)
    if filters is None:
        return False, 0.0, 0.0

    try:
        qty_d      = Decimal(str(qty))
        price_d    = Decimal(str(price))
        notional   = qty_d * price_d
        min_notl   = filters["min_notional"]

        actual  = float(notional)
        minimum = float(min_notl)

        if min_notl > Decimal("0") and notional < min_notl:
            print(
                f"[PRECISION] {symbol}: notional ${actual:.4f} < "
                f"minNotional ${minimum:.2f} — order would be rejected"
            )
            return False, actual, minimum

        return True, actual, minimum

    except (InvalidOperation, Exception) as e:
        print(f"[PRECISION] validate_min_notional {symbol} error: {e}")
        return False, 0.0, 0.0


def validate_order(symbol: str, qty: float, price: float) -> dict:
    """
    Combined order validator. Rounds both values then runs all checks.

    Args:
      symbol: e.g. "BTCUSDT"
      qty:    raw (unrounded) quantity
      price:  raw (unrounded) price

    Returns dict:
      {
        "valid":         bool,
        "reasons":       list[str],   # empty when valid
        "rounded_qty":   float | None,
        "rounded_price": float | None,
        "notional":      float,       # actual notional after rounding
        "min_notional":  float,       # required minimum (0 = no minimum)
        "step_size":     str,         # for logging
        "tick_size":     str,         # for logging
      }

    Checks performed (in order):
      1. Filters available
      2. round_qty (floors to stepSize)
      3. round_price (floors to tickSize)
      4. validate_min_qty on rounded qty
      5. validate_min_notional on rounded qty * rounded price

    A qty that floors to 0.0 is rejected immediately (would be 0 notional).
    """
    result = {
        "valid":         False,
        "reasons":       [],
        "rounded_qty":   None,
        "rounded_price": None,
        "notional":      0.0,
        "min_notional":  0.0,
        "step_size":     "",
        "tick_size":     "",
    }

    # Step 1 — filters
    filters = _get_filters_cached(symbol)
    if filters is None:
        result["reasons"].append(f"filters unavailable for {symbol}")
        return result

    result["step_size"] = str(filters["step_size"])
    result["tick_size"] = str(filters["tick_size"])
    result["min_notional"] = float(filters["min_notional"])

    # Step 2 — round qty down
    r_qty = round_qty(symbol, qty)
    if r_qty is None:
        result["reasons"].append("qty rounding failed")
        return result

    result["rounded_qty"] = r_qty

    if r_qty == 0.0:
        result["reasons"].append(
            f"qty {qty} floors to 0.0 with stepSize {filters['step_size']} "
            f"— increase position size or use higher leverage"
        )
        return result

    # Step 3 — round price down
    r_price = round_price(symbol, price)
    if r_price is None:
        result["reasons"].append("price rounding failed")
        return result

    result["rounded_price"] = r_price

    # Step 4 — min qty
    qty_ok, qty_reason = validate_min_qty(symbol, r_qty)
    if not qty_ok:
        result["reasons"].append(f"LOT_SIZE: {qty_reason}")

    # Step 5 — min notional
    notl_ok, actual_notl, min_notl = validate_min_notional(symbol, r_qty, r_price)
    result["notional"]     = actual_notl
    result["min_notional"] = min_notl
    if not notl_ok:
        result["reasons"].append(
            f"MIN_NOTIONAL: ${actual_notl:.4f} < ${min_notl:.2f} required"
        )

    result["valid"] = len(result["reasons"]) == 0

    if result["valid"]:
        print(
            f"[PRECISION] {symbol} validate_order OK — "
            f"qty={r_qty} price={r_price} notional=${actual_notl:.2f}"
        )
    else:
        print(
            f"[PRECISION] {symbol} validate_order FAIL — "
            + " | ".join(result["reasons"])
        )

    return result


# =====================================================================
# CACHE UTILITIES
# =====================================================================

def clear_cache(symbol: str = None) -> None:
    """
    Clear filter cache.
    If symbol given, clear only that symbol.
    If None, clear everything.
    """
    if symbol:
        _filter_cache.pop(symbol, None)
        print(f"[PRECISION] cache cleared for {symbol}")
    else:
        _filter_cache.clear()
        print("[PRECISION] full filter cache cleared")


def cache_info() -> dict:
    """
    Return summary of current cache state for debugging.
    Returns dict: {symbol: age_seconds}
    """
    now = time.time()
    return {
        sym: round(now - entry["ts"], 1)
        for sym, entry in _filter_cache.items()
    }
