"""
Dry-run execution orchestrator for COIN BOT.

SAFE BOUNDARIES — this module MUST NOT:
  - Place, modify, or cancel orders
  - Call any POST endpoint on Binance
  - Change leverage or margin settings on the exchange
  - Import from execution.py, entry.py, or main.py

Role of this module:
  Simulate the full order lifecycle for a proposed trade WITHOUT
  sending any real orders. Given (symbol, side, entry, sl, tp,
  balance, risk_percent) → produce a complete simulated execution
  payload, realistic order objects, risk warnings, and a
  human-readable dry-run report.

Public API:
  build_execution_payload(symbol, side, entry, sl, tp, balance, risk_percent)
      → dict   (full simulated execution result)

  print_dry_run_report(payload)
      → None   (prints formatted console report)

  format_dry_run_report(payload)
      → str    (returns the same report as a string — for Telegram etc.)
"""

import math
import uuid

from .execution_policy import calculate_execution_plan, get_max_leverage, get_symbol_tier

# =====================================================================
# CONSTANTS
# =====================================================================

# Rough maintenance margin rate used for liquidation price approximation.
# Binance COIN-M / USD-M futures typical value for BTC/ETH; higher tiers
# use the same value here since this is for operator visibility only.
_MAINTENANCE_MARGIN_RATE = 0.004  # 0.4%

# Warning thresholds
_WARN_HIGH_LEVERAGE          = 100     # leverage above this triggers a warning
_WARN_MARGIN_USAGE_HIGH      = 0.50   # margin / balance > 50%
_WARN_MARGIN_USAGE_CRITICAL  = 0.80   # margin / balance > 80%
_WARN_FREE_BALANCE_LOW       = 50.0   # remaining free balance < $50
_WARN_QTY_ROUND_THRESHOLD    = 0.05   # (raw - rounded) / raw > 5%
_WARN_LEVERAGE_NEAR_CAP      = 0.80   # final_leverage >= allowed * 80%

# =====================================================================
# INTERNAL — LIQUIDATION PRICE ESTIMATION
# =====================================================================

def _estimate_liquidation_price(
    side: str,
    entry: float,
    leverage: int,
) -> float | None:
    """
    Rough liquidation price for CROSS margin.

    Formula (operator visibility only — not exact Binance calculation):
      LONG:  liq ≈ entry × (1 − 1/leverage + maintenance_margin_rate)
      SHORT: liq ≈ entry × (1 + 1/leverage − maintenance_margin_rate)

    Returns None on invalid input.
    """
    if entry is None or entry <= 0:
        return None
    if leverage is None or leverage <= 0:
        return None

    if side == "BUY":
        liq = entry * (1 - (1 / leverage) + _MAINTENANCE_MARGIN_RATE)
    elif side == "SELL":
        liq = entry * (1 + (1 / leverage) - _MAINTENANCE_MARGIN_RATE)
    else:
        return None

    if liq <= 0:
        return None

    return round(liq, 6)


# =====================================================================
# INTERNAL — RISK WARNINGS
# =====================================================================

def _generate_warnings(plan: dict, side: str, tp: float) -> list[str]:
    """
    Generate human-readable risk warnings from a valid execution plan.

    Checks:
      - leverage above HIGH threshold
      - margin usage above 50% of balance (high)
      - margin usage above 80% of balance (critical)
      - remaining free balance below $50
      - qty rounded more than 5% from raw
      - leverage at or near the tier cap
      - TP not set (or on wrong side of entry)

    Returns a list of warning strings. Empty list = no warnings.
    Only meaningful on a valid plan.
    """
    warnings = []

    if not plan.get("valid"):
        return warnings

    leverage       = plan.get("final_leverage", 0)
    allowed        = plan.get("allowed_leverage", 0)
    margin         = plan.get("margin_required", 0.0)
    balance        = plan.get("balance", 0.0)
    raw_qty        = plan.get("raw_qty", 0.0)
    rounded_qty    = plan.get("rounded_qty", 0.0)
    entry          = plan.get("entry", 0.0)

    # High leverage
    if leverage > _WARN_HIGH_LEVERAGE:
        warnings.append(
            f"HIGH LEVERAGE: {leverage}x — elevated liquidation risk"
        )

    # Leverage near cap
    if allowed > 0 and leverage >= math.ceil(allowed * _WARN_LEVERAGE_NEAR_CAP):
        warnings.append(
            f"LEVERAGE NEAR CAP: {leverage}x / {allowed}x allowed"
        )

    # Margin usage
    if balance > 0:
        margin_pct = margin / balance
        if margin_pct > _WARN_MARGIN_USAGE_CRITICAL:
            warnings.append(
                f"CRITICAL MARGIN USAGE: {margin_pct*100:.1f}% of balance "
                f"(${margin:.2f} of ${balance:.2f})"
            )
        elif margin_pct > _WARN_MARGIN_USAGE_HIGH:
            warnings.append(
                f"HIGH MARGIN USAGE: {margin_pct*100:.1f}% of balance "
                f"(${margin:.2f} of ${balance:.2f})"
            )

    # Low remaining free balance
    free_balance = balance - margin
    if free_balance < _WARN_FREE_BALANCE_LOW:
        warnings.append(
            f"LOW FREE BALANCE: ${free_balance:.2f} remaining after margin"
        )

    # Qty rounded heavily
    if raw_qty > 0:
        round_slip = (raw_qty - rounded_qty) / raw_qty
        if round_slip > _WARN_QTY_ROUND_THRESHOLD:
            warnings.append(
                f"QTY ROUNDED HEAVILY: raw={raw_qty:.8f} → {rounded_qty} "
                f"({round_slip*100:.1f}% slip)"
            )

    # TP sanity check
    if tp is None or tp <= 0:
        warnings.append("NO TP SET: take-profit price missing or zero")
    elif side == "BUY" and entry > 0 and tp <= entry:
        warnings.append(
            f"TP BELOW ENTRY on LONG: tp={tp} entry={entry}"
        )
    elif side == "SELL" and entry > 0 and tp >= entry:
        warnings.append(
            f"TP ABOVE ENTRY on SHORT: tp={tp} entry={entry}"
        )

    return warnings


# =====================================================================
# INTERNAL — SIMULATED ORDER OBJECTS
# =====================================================================

def _build_entry_order(plan: dict, side: str) -> dict:
    return {
        "symbol":      plan["symbol"],
        "side":        side,
        "type":        "MARKET",
        "quantity":    plan["rounded_qty"],
        "leverage":    plan["final_leverage"],
        "margin_mode": "CROSS",
    }


def _build_stop_order(plan: dict, side: str) -> dict:
    close_side = "SELL" if side == "BUY" else "BUY"
    return {
        "symbol":     plan["symbol"],
        "side":       close_side,
        "type":       "STOP_MARKET",
        "stopPrice":  plan["sl"],
        "quantity":   plan["rounded_qty"],
        "reduceOnly": True,
    }


def _build_tp_order(plan: dict, side: str, tp: float) -> dict:
    close_side = "SELL" if side == "BUY" else "BUY"
    return {
        "symbol":     plan["symbol"],
        "side":       close_side,
        "type":       "TAKE_PROFIT_MARKET",
        "stopPrice":  tp,
        "quantity":   plan["rounded_qty"],
        "reduceOnly": True,
    }


# =====================================================================
# INTERNAL — SIMULATED EXCHANGE RESPONSE
# =====================================================================

def _build_exchange_response(
    plan:     dict,
    side:     str,
    tp:       float,
    warnings: list[str],
) -> dict:
    """
    Build a realistic simulated exchange response object.

    Fields mirror what a real Binance response would contain,
    plus dry-run specific fields for operator visibility.
    """
    if not plan.get("valid"):
        return {
            "accepted":           False,
            "reason":             plan.get("reason", "unknown"),
            "simulated_order_id": None,
            "required_margin":    plan.get("margin_required"),
            "estimated_liq_price": None,
            "warnings":           warnings,
        }

    liq_price = _estimate_liquidation_price(
        side, plan["entry"], plan["final_leverage"]
    )

    return {
        "accepted":            True,
        "reason":              "OK",
        "simulated_order_id":  str(uuid.uuid4()),
        "required_margin":     plan["margin_required"],
        "estimated_liq_price": liq_price,
        "warnings":            warnings,
    }


# =====================================================================
# PUBLIC API — BUILD EXECUTION PAYLOAD
# =====================================================================

def build_execution_payload(
    symbol:       str,
    side:         str,
    entry:        float,
    sl:           float,
    tp:           float,
    balance:      float,
    risk_percent: float,
) -> dict:
    """
    Build a complete simulated execution payload for a proposed trade.

    This is the primary entry point for dry-run simulation.
    Does NOT call any exchange write endpoint. Read-only and safe.

    Args:
      symbol:       Futures symbol, e.g. "BTCUSDT"
      side:         "BUY" (long) or "SELL" (short)
      entry:        Intended entry price
      sl:           Stop-loss price
      tp:           Take-profit price (used for R:R calculation and TP order)
      balance:      Available USDT in the Futures wallet
      risk_percent: Fraction of balance to risk (e.g. 0.01 = 1%)

    Returns a dict with:
      valid            bool   — True = trade is simulatable / executable
      plan             dict   — raw output from calculate_execution_plan
      entry_order      dict   — simulated MARKET entry order object
      stop_order       dict   — simulated STOP_MARKET SL order object
      tp_order         dict   — simulated TAKE_PROFIT_MARKET TP order object
      exchange_response dict  — simulated exchange acceptance/rejection
      risk_reward      float  — R:R ratio (tp_distance / sl_distance), or None
      warnings         list   — risk warning strings
    """
    # Validate side before calling the plan
    side_upper = (side or "").upper()
    if side_upper not in ("BUY", "SELL"):
        return {
            "valid":             False,
            "plan":              {"valid": False, "reason": f"invalid side: {side!r}"},
            "entry_order":       None,
            "stop_order":        None,
            "tp_order":          None,
            "exchange_response": {
                "accepted":           False,
                "reason":             f"invalid side: {side!r}",
                "simulated_order_id": None,
                "required_margin":    None,
                "estimated_liq_price": None,
                "warnings":           [],
            },
            "risk_reward":       None,
            "warnings":          [],
        }

    plan = calculate_execution_plan(symbol, balance, risk_percent, entry, sl)

    warnings = _generate_warnings(plan, side_upper, tp)

    if not plan.get("valid"):
        response = _build_exchange_response(plan, side_upper, tp, warnings)
        return {
            "valid":             False,
            "plan":              plan,
            "entry_order":       None,
            "stop_order":        None,
            "tp_order":          None,
            "exchange_response": response,
            "risk_reward":       None,
            "warnings":          warnings,
        }

    entry_order = _build_entry_order(plan, side_upper)
    stop_order  = _build_stop_order(plan, side_upper)
    tp_order    = _build_tp_order(plan, side_upper, tp) if (tp and tp > 0) else None

    # R:R ratio
    sl_distance = plan.get("sl_distance", 0.0)
    risk_reward = None
    if sl_distance and sl_distance > 0 and tp and tp > 0 and entry and entry > 0:
        tp_distance = abs(tp - entry)
        risk_reward = round(tp_distance / sl_distance, 2)

    response = _build_exchange_response(plan, side_upper, tp, warnings)

    return {
        "valid":             True,
        "plan":              plan,
        "entry_order":       entry_order,
        "stop_order":        stop_order,
        "tp_order":          tp_order,
        "exchange_response": response,
        "risk_reward":       risk_reward,
        "warnings":          warnings,
    }


# =====================================================================
# REPORT FORMATTING
# =====================================================================

def format_dry_run_report(payload: dict) -> str:
    """
    Format a human-readable dry-run execution report.

    Returns the report as a string (suitable for Telegram or logging).
    Does not print — call print_dry_run_report to also print it.
    """
    plan     = payload.get("plan", {})
    response = payload.get("exchange_response", {})
    warnings = payload.get("warnings", [])
    valid    = payload.get("valid", False)

    symbol   = plan.get("symbol", "?")
    side     = (payload.get("entry_order") or {}).get("side", "?")
    entry    = plan.get("entry")
    sl       = plan.get("sl")
    tp       = None
    tp_order = payload.get("tp_order")
    if tp_order:
        tp = tp_order.get("stopPrice")

    qty      = plan.get("rounded_qty")
    leverage = plan.get("final_leverage")
    margin   = plan.get("margin_required")
    balance  = plan.get("balance")
    rr       = payload.get("risk_reward")

    risk_amount   = plan.get("risk_amount")
    notional      = plan.get("notional")
    liq_price     = response.get("estimated_liq_price")
    order_id      = response.get("simulated_order_id")

    direction = "LONG" if side == "BUY" else "SHORT" if side == "SELL" else side

    lines = ["[DRY-RUN]"]
    lines.append(f"{symbol}  {direction}")
    lines.append("")

    if entry is not None:
        lines.append(f"  Entry         : {entry}")
    if sl is not None:
        sl_pct = plan.get("sl_distance_pct")
        sl_pct_str = f"  ({sl_pct:.4f}%)" if sl_pct is not None else ""
        lines.append(f"  SL            : {sl}{sl_pct_str}")
    if tp is not None:
        lines.append(f"  TP            : {tp}")
    if rr is not None:
        lines.append(f"  R:R           : {rr}R")

    lines.append("")

    if qty is not None:
        lines.append(f"  Qty           : {qty}")
    if notional is not None:
        lines.append(f"  Notional      : ${notional:.2f}")
    if leverage is not None:
        allowed = plan.get("allowed_leverage")
        target = plan.get("target_leverage")
        clamp_reason = plan.get("leverage_clamp_reason")
        reason = f"  reason: {clamp_reason}" if clamp_reason else ""
        lines.append(
            f"  Leverage      : {leverage}x  (target: {target}x  cap: {allowed}x){reason}"
        )
    if margin is not None:
        lines.append(f"  Margin        : ${margin:.2f}")
    if balance is not None and margin is not None:
        free = balance - margin
        lines.append(f"  Free balance  : ${free:.2f}")

    lines.append("")

    if risk_amount is not None:
        lines.append(f"  Risk amount   : ${risk_amount:.4f}")
    if liq_price is not None:
        lines.append(f"  Est. liq price: {liq_price}")

    lines.append("")

    if warnings:
        lines.append("  WARNINGS:")
        for w in warnings:
            lines.append(f"    ⚠  {w}")
        lines.append("")

    if valid:
        lines.append(f"  Executable    : YES")
        if order_id:
            lines.append(f"  Sim order ID  : {order_id}")
    else:
        reason = plan.get("reason") or response.get("reason", "unknown")
        lines.append(f"  Executable    : NO")
        lines.append(f"  Reason        : {reason}")

    return "\n".join(lines)


def print_dry_run_report(payload: dict) -> None:
    """
    Print the dry-run execution report to console.
    """
    print(format_dry_run_report(payload))
