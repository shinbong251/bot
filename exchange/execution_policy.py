"""
Execution policy layer — CROSS margin, adaptive leverage, dry-run feasibility.

SAFE BOUNDARIES — this module MUST NOT:
  - Place, modify, or cancel orders
  - Make any API calls (all exchange contact goes through binance_client)
  - Import from execution.py, entry.py, or main.py
  - Change any strategy risk parameters

Role of this module:
  Given (symbol, balance, risk_percent, entry, sl) → answer the question:
  "Can this trade be executed on Binance Futures right now, and at what leverage?"
  LIVE mode with use_exchange_max_leverage=true uses this module for risk,
  precision, notional, and required-leverage math, then live_executor replaces
  the tier leverage with Binance leverageBracket max before setting leverage.

Leverage in this system:
  - Used ONLY to satisfy Binance margin requirements (notional / leverage ≤ balance)
  - Does NOT change actual USD risk (risk = qty × sl_distance = balance × risk_percent)
  - Does NOT change position qty or SL
  - Higher leverage = less margin posted per dollar of notional

Public API:
  get_max_leverage(symbol)              → int
  get_target_leverage(symbol)           → int
  get_min_acceptable_leverage(symbol)   → int
  get_symbol_tier(symbol)               → str
  calculate_execution_plan(...)         → dict
  dry_run_report(...)                   → None   (prints to console)
"""

import math
from decimal import Decimal, InvalidOperation

from .precision import (
    get_symbol_filters,
    round_qty,
    round_price,
    validate_min_qty,
    validate_min_notional,
)

# =====================================================================
# SYMBOL TIER CLASSIFICATION
# =====================================================================

# Tier 1 — BTC and ETH only. Deepest liquidity, tightest spreads.
# Binance Futures supports up to 125x for BTC/ETH.
_TIER1: set = {
    "BTCUSDT",
    "ETHUSDT",
}

# Tier 2 — Highest-volume alts with institutional futures depth.
# Binance supports up to 100x+ for SOL and BNB.
_TIER2: set = {
    "SOLUSDT",
    "BNBUSDT",
}

# Tier 3 — Major liquid alts. Strong Binance futures depth, tight spreads.
# Binance supports up to 75x for most of these.
_TIER3: set = {
    "XRPUSDT",   "DOGEUSDT",  "AVAXUSDT",  "LINKUSDT",
    "LTCUSDT",   "ADAUSDT",   "DOTUSDT",   "MATICUSDT",
    "ATOMUSDT",  "UNIUSDT",   "AAVEUSDT",  "NEARUSDT",
    "APTUSDT",   "ARBUSDT",   "OPUSDT",    "SEIUSDT",
    "SUIUSDT",   "INJUSDT",   "TIAUSDT",   "WLDUSDT",
    "PYTHUSDT",
}

# Tier 5 — Explicitly ultra-volatile / illiquid. Hard cap at 5x.
# Add symbols as experience grows. Default for unknowns is Tier 4.
_TIER5: set = {
    "SNDKUSDT",  "BZUSDT",
}

# Leverage caps per tier for non-LIVE/fallback execution policy.
# LIVE exchange-max mode bypasses these caps and uses Binance leverageBracket.
# They do not represent strategy risk — they are margin-efficiency limits.
_LEVERAGE_CAP: dict = {
    "TIER1": 125,  # BTC, ETH — exchange allows up to 125x
    "TIER2": 100,  # SOL, BNB — exchange allows up to 100x+
    "TIER3": 30,   # Major liquid alts
    "TIER4": 20,   # Unknown / new / low-confidence (default bucket)
    "TIER5": 5,    # Dangerous / restricted symbols
}

# Target leverage per tier for non-LIVE/fallback CROSS-margin portfolios.
# LIVE exchange-max mode bypasses these targets and uses Binance leverageBracket.
# When required_leverage ≤ target, the bot uses target to free up margin headroom.
# When required_leverage > target, the bot uses required_leverage (minimum needed).
# Actual SL risk (balance × risk_percent) is NOT changed by this setting.
_LEVERAGE_TARGET: dict = {
    "TIER1": 125,  # BTC, ETH
    "TIER2": 100,  # SOL, BNB
    "TIER3": 25,   # Major liquid alts
    "TIER4": 20,   # Unknown / new / low-confidence (default bucket)
    "TIER5": 5,    # Dangerous / restricted symbols
}

# Minimum confirmed leverage as a fraction of target per tier in fallback mode.
# LIVE exchange-max mode does not use this tier min-ratio check.
# If Binance confirms leverage below this threshold, the trade is rejected.
# Protects against subaccount caps silently making margin assumptions invalid.
_LEVERAGE_MIN_RATIO: dict = {
    "TIER1": 0.70,  # BTC/ETH:  min = ceil(125 × 0.70) = 88
    "TIER2": 0.70,  # SOL/BNB:  min = ceil(100 × 0.70) = 70
    "TIER3": 0.50,  # Alts:     min = ceil(25  × 0.50) = 13
    "TIER4": 0.40,  # Unknown:  min = ceil(20  × 0.40) = 8
    "TIER5": 1.00,  # Blocked:  min = 5 (blocked at gate anyway)
}

# =====================================================================
# TIER & LEVERAGE ACCESSORS
# =====================================================================

def get_symbol_tier(symbol: str) -> str:
    """
    Return the leverage tier for a symbol.

    Returns one of: "TIER1", "TIER2", "TIER3", "TIER4", "TIER5"

    Classification rules (evaluated in order):
      1. TIER1 → BTCUSDT, ETHUSDT
      2. TIER5 → explicit ultra-volatile override list
      3. TIER2 → SOL, BNB
      4. TIER3 → known major liquid alts
      5. TIER4 → everything else (default)

    TIER5 is checked before TIER2/TIER3 so explicit overrides always win.
    TIER4 is the safe default — unknown symbols get the conservative cap.
    """
    if symbol in _TIER1:
        return "TIER1"
    if symbol in _TIER5:
        return "TIER5"
    if symbol in _TIER2:
        return "TIER2"
    if symbol in _TIER3:
        return "TIER3"
    return "TIER4"


def get_max_leverage(symbol: str) -> int:
    """
    Return the maximum allowed leverage for a symbol.

    Examples:
      get_max_leverage("BTCUSDT")   → 125  (TIER1)
      get_max_leverage("SOLUSDT")   → 100  (TIER2)
      get_max_leverage("XRPUSDT")   → 30   (TIER3)
      get_max_leverage("PEPEUSDT")  → 20   (TIER4 default)
      get_max_leverage("SNDKUSDT")  → 5    (TIER5 explicit)
    """
    tier = get_symbol_tier(symbol)
    return _LEVERAGE_CAP[tier]


def get_target_leverage(symbol: str) -> int:
    """
    Return the portfolio-efficiency target leverage for a symbol.

    Used to free up CROSS-margin headroom for concurrent positions.
    When the required leverage is below this target, the bot uses the
    target so that margin posted = notional / target instead of
    notional / 1 (or notional / required_min).

    Actual USD risk at SL is unaffected — only margin efficiency changes.

    Examples:
      get_target_leverage("BTCUSDT")   → 125  (TIER1)
      get_target_leverage("SOLUSDT")   → 100  (TIER2)
      get_target_leverage("XRPUSDT")   → 25   (TIER3)
      get_target_leverage("PEPEUSDT")  → 20   (TIER4 default)
      get_target_leverage("SNDKUSDT")  → 5    (TIER5 explicit)
    """
    tier = get_symbol_tier(symbol)
    return _LEVERAGE_TARGET[tier]


def get_min_acceptable_leverage(symbol: str) -> int:
    """
    Return the minimum confirmed leverage below which a trade is rejected.

    After calling Binance /fapi/v1/leverage, the exchange returns the
    actually-confirmed leverage. If confirmed < this minimum, the trade
    is aborted — using lower leverage than expected invalidates margin
    calculations and can exhaust free balance on concurrent positions.

    Computed as: ceil(target_leverage × _LEVERAGE_MIN_RATIO[tier])

    Examples:
      get_min_acceptable_leverage("BTCUSDT")   → 88   (TIER1: ceil(125×0.70))
      get_min_acceptable_leverage("SOLUSDT")   → 70   (TIER2: ceil(100×0.70))
      get_min_acceptable_leverage("XRPUSDT")   → 13   (TIER3: ceil(25×0.50))
      get_min_acceptable_leverage("PEPEUSDT")  → 8    (TIER4: ceil(20×0.40))
      get_min_acceptable_leverage("SNDKUSDT")  → 5    (TIER5: ceil(5×1.00))
    """
    tier = get_symbol_tier(symbol)
    target = _LEVERAGE_TARGET[tier]
    ratio  = _LEVERAGE_MIN_RATIO[tier]
    return math.ceil(target * ratio)

# =====================================================================
# INTERNAL — BUILD INVALID PLAN RESPONSE
# =====================================================================

def _invalid_plan(reason: str, **overrides) -> dict:
    """
    Return a fully-populated invalid execution plan dict.
    `overrides` allow partial data to be preserved even on failure.
    """
    base = {
        "valid":              False,
        "reason":             reason,
        "symbol":             None,
        "margin_mode":        "CROSS",
        "tier":               None,
        # Risk math
        "balance":            None,
        "risk_percent":       None,
        "risk_amount":        None,
        "sl_distance":        None,
        "sl_distance_pct":    None,
        # Qty
        "raw_qty":            None,
        "rounded_qty":        None,
        # Price
        "entry":              None,
        "sl":                 None,
        "rounded_price":      None,
        # Notional
        "notional":           None,
        "min_notional":       None,
        # Leverage
        "required_leverage":  None,
        "target_leverage":    None,
        "allowed_leverage":   None,
        "final_leverage":     None,
        "leverage_clamp_reason": None,
        # Margin
        "margin_required":    None,
    }
    base.update(overrides)
    return base

# =====================================================================
# CORE — EXECUTION FEASIBILITY CALCULATOR
# =====================================================================

def calculate_execution_plan(
    symbol:       str,
    balance:      float,
    risk_percent: float,
    entry:        float,
    sl:           float,
) -> dict:
    """
    Calculate a complete dry-run execution plan for a proposed trade.

    This function answers: "Is this trade executable on Binance Futures,
    and at what leverage must we set the account to open the position?"

    Args:
      symbol:       Futures symbol, e.g. "BTCUSDT"
      balance:      Available USDT in the Futures wallet
      risk_percent: Fraction of balance to risk (e.g. 0.01 = 1%)
      entry:        Intended entry price
      sl:           Stop-loss price

    Returns a dict (see below). Always returns a dict — never raises.

    HOW LEVERAGE IS CALCULATED:
      1. Qty is fixed by risk math: qty = (balance × risk_percent) / sl_distance
         This qty guarantees exactly risk_percent of balance is lost if SL hits.
      2. Notional = qty × entry
      3. In CROSS margin: margin_required = notional / leverage
         To open the position, margin_required must be ≤ balance.
         Therefore: leverage ≥ ceil(notional / balance)
      4. required_leverage = ceil(notional / balance)
      5. target_leverage   = get_target_leverage(symbol)  (efficiency target per tier)
      6. If required_leverage ≤ target_leverage → final_leverage = target_leverage
         If required_leverage > target_leverage → final_leverage = required_leverage
         Then clamp final_leverage to allowed_leverage when the target exceeds
         the symbol/tier cap.
      7. If required_leverage > allowed_leverage → INFEASIBLE

    WHY LEVERAGE ≠ ACTUAL RISK:
      Leverage only determines how much margin is posted.
      The actual USD loss if SL fires is always: qty × sl_distance = risk_amount.
      Using a higher target_leverage reduces margin posted but does NOT change
      position size, SL distance, or the USD amount lost if SL hits.

    Return dict fields:
      valid              bool   — True = trade can be executed
      reason             str    — "OK" or human-readable rejection reason
      symbol             str
      margin_mode        str    — always "CROSS"
      tier               str    — "TIER1" / "TIER2" / "TIER3" / "TIER4"
      balance            float
      risk_percent       float
      risk_amount        float  — balance × risk_percent  (USD at risk)
      sl_distance        float  — abs(entry - sl)
      sl_distance_pct    float  — sl_distance / entry  (e.g. 0.003 = 0.3%)
      raw_qty            float  — risk_amount / sl_distance  (before rounding)
      rounded_qty        float  — after floor to stepSize
      entry              float
      sl                 float
      rounded_price      float  — after floor to tickSize
      notional           float  — rounded_qty × rounded_price
      min_notional       float  — Binance minimum (from exchangeInfo)
      required_leverage  int    — minimum leverage to fit margin in balance
      target_leverage    int    — efficiency target for this tier
      allowed_leverage   int    — hard cap from tier policy
      final_leverage     int    — what will be set (max of required and target, capped)
      margin_required    float  — notional / final_leverage
    """

    # ----- Shared context for _invalid_plan calls -----
    ctx = {
        "symbol":       symbol,
        "balance":      balance,
        "risk_percent": risk_percent,
        "entry":        entry,
        "sl":           sl,
        "tier":         get_symbol_tier(symbol),
        "allowed_leverage": get_max_leverage(symbol),
        "margin_mode":  "CROSS",
    }

    # ── Guard: balance ────────────────────────────────────────────────
    if balance is None or balance <= 0:
        return _invalid_plan("balance is zero or negative", **ctx)

    if risk_percent is None or risk_percent <= 0 or risk_percent > 1:
        return _invalid_plan(
            f"risk_percent {risk_percent} out of range (must be 0 < x ≤ 1)", **ctx
        )

    # ── Guard: SL placement ───────────────────────────────────────────
    if entry is None or sl is None or entry <= 0 or sl <= 0:
        return _invalid_plan("entry or sl is zero / None", **ctx)

    sl_distance = abs(entry - sl)
    sl_distance_pct = sl_distance / entry
    ctx["sl_distance"]     = sl_distance
    ctx["sl_distance_pct"] = sl_distance_pct

    if sl_distance <= 0:
        return _invalid_plan("SL distance is zero — entry equals sl", **ctx)

    if sl_distance_pct < 0.001:
        return _invalid_plan(
            f"SL too close to entry: {sl_distance_pct*100:.3f}% "
            f"(minimum 0.1% required to avoid precision loss)", **ctx
        )

    # ── Risk math ─────────────────────────────────────────────────────
    risk_amount = balance * risk_percent
    raw_qty     = risk_amount / sl_distance
    ctx["risk_amount"] = risk_amount
    ctx["raw_qty"]     = raw_qty

    # ── Precision filters ─────────────────────────────────────────────
    filters = get_symbol_filters(symbol)
    if filters is None:
        return _invalid_plan(
            f"precision filters unavailable for {symbol} — "
            f"check connectivity or symbol name", **ctx
        )

    ctx["min_notional"] = float(filters["min_notional"])

    # ── Round qty (floor) ─────────────────────────────────────────────
    rounded_qty = round_qty(symbol, raw_qty)
    if rounded_qty is None:
        return _invalid_plan("qty rounding failed — filter error", **ctx)

    ctx["rounded_qty"] = rounded_qty

    if rounded_qty == 0.0:
        return _invalid_plan(
            f"qty floors to 0.0 — raw_qty {raw_qty:.8f} is below stepSize "
            f"{filters['step_size']} — increase balance or use higher leverage tier",
            **ctx
        )

    # ── Min qty check ─────────────────────────────────────────────────
    qty_ok, qty_reason = validate_min_qty(symbol, rounded_qty)
    if not qty_ok:
        return _invalid_plan(f"LOT_SIZE rejected: {qty_reason}", **ctx)

    # ── Round price (floor) ───────────────────────────────────────────
    rounded_price = round_price(symbol, entry)
    if rounded_price is None:
        return _invalid_plan("price rounding failed — filter error", **ctx)

    ctx["rounded_price"] = rounded_price

    # ── Notional ──────────────────────────────────────────────────────
    notional = rounded_qty * rounded_price
    ctx["notional"] = notional

    # ── Min notional check ────────────────────────────────────────────
    notl_ok, actual_notl, min_notl = validate_min_notional(
        symbol, rounded_qty, rounded_price
    )
    ctx["notional"]     = actual_notl
    ctx["min_notional"] = min_notl

    if not notl_ok:
        return _invalid_plan(
            f"MIN_NOTIONAL: position ${actual_notl:.4f} < "
            f"Binance minimum ${min_notl:.2f} — "
            f"increase balance, use more leverage, or choose a lower-priced symbol",
            **ctx
        )

    # ── Required leverage (margin feasibility) ────────────────────────
    #
    # In CROSS margin: margin_posted = notional / leverage
    # For the trade to open: margin_posted ≤ balance
    # → leverage ≥ notional / balance
    # → required_leverage = ceil(notional / balance)
    #
    # Edge case: if notional ≤ balance, required_leverage = 1 (no leverage needed).
    required_leverage = max(1, math.ceil(actual_notl / balance))
    ctx["required_leverage"] = required_leverage

    target_leverage  = get_target_leverage(symbol)
    ctx["target_leverage"] = target_leverage

    allowed_leverage = get_max_leverage(symbol)
    ctx["allowed_leverage"] = allowed_leverage

    if required_leverage > allowed_leverage:
        return _invalid_plan(
            f"leverage infeasible: need {required_leverage}x but "
            f"{symbol} ({get_symbol_tier(symbol)}) is capped at {allowed_leverage}x — "
            f"increase balance or reduce position size",
            **ctx
        )

    # ── Final leverage — target-based for portfolio margin efficiency ──
    #
    # If the trade only needs e.g. 2x to fit in balance but the tier
    # target is 20x, using 20x frees up 90% more margin for concurrent
    # positions without changing qty, SL, or actual USD risk at SL.
    #
    # If required_leverage > target_leverage (large notional trade),
    # we use required_leverage — the minimum that makes the trade feasible.
    if required_leverage <= target_leverage:
        final_leverage = target_leverage
    else:
        final_leverage = required_leverage

    leverage_clamp_reason = None
    if final_leverage > allowed_leverage:
        final_leverage = allowed_leverage
        leverage_clamp_reason = "exchange_max_clamp"

    margin_required  = actual_notl / final_leverage

    return {
        "valid":             True,
        "reason":            "OK",
        "symbol":            symbol,
        "margin_mode":       "CROSS",
        "tier":              get_symbol_tier(symbol),
        # Account
        "balance":           balance,
        "risk_percent":      risk_percent,
        "risk_amount":       round(risk_amount, 6),
        # SL
        "sl_distance":       round(sl_distance, 8),
        "sl_distance_pct":   round(sl_distance_pct * 100, 4),   # % for readability
        # Qty
        "raw_qty":           raw_qty,
        "rounded_qty":       rounded_qty,
        # Price
        "entry":             entry,
        "sl":                sl,
        "rounded_price":     rounded_price,
        # Notional
        "notional":          round(actual_notl, 4),
        "min_notional":      round(min_notl, 4),
        # Leverage
        "required_leverage": required_leverage,
        "target_leverage":   target_leverage,
        "allowed_leverage":  allowed_leverage,
        "final_leverage":    final_leverage,
        "leverage_clamp_reason": leverage_clamp_reason,
        # Margin
        "margin_required":   round(margin_required, 4),
    }

# =====================================================================
# DRY-RUN REPORT — HUMAN-READABLE CONSOLE OUTPUT
# =====================================================================

def dry_run_report(
    symbol:       str,
    balance:      float,
    risk_percent: float,
    entry:        float,
    sl:           float,
) -> dict:
    """
    Calculate execution plan and print a formatted dry-run summary.

    Returns the same dict as calculate_execution_plan.
    Safe to call in any context — never raises, never places orders.

    Example output (valid):
      ╔══════════════════════════════════════╗
      ║ DRY-RUN: BTCUSDT  [TIER1]           ║
      ╠══════════════════════════════════════╣
      ║ Balance       $200.00               ║
      ║ Risk          1.00%  →  $2.00       ║
      ║ Entry         67000.00              ║
      ║ SL            66500.00  (-0.7463%)  ║
      ║ SL Distance   $500.00               ║
      ╠══════════════════════════════════════╣
      ║ Raw qty       0.004000              ║
      ║ Rounded qty   0.004  (step 0.001)   ║
      ║ Notional      $268.00               ║
      ║ Min notional  $5.00          ✓      ║
      ╠══════════════════════════════════════╣
      ║ Leverage      2x  (cap: 50x)        ║
      ║ Margin posted $134.00               ║
      ║ Margin mode   CROSS                 ║
      ╠══════════════════════════════════════╣
      ║ RESULT        ✓ EXECUTABLE          ║
      ╚══════════════════════════════════════╝
    """
    plan = calculate_execution_plan(symbol, balance, risk_percent, entry, sl)
    _print_plan(plan)
    return plan


def _print_plan(p: dict) -> None:
    symbol      = p.get("symbol", "?")
    tier        = p.get("tier", "?")
    valid       = p.get("valid", False)
    W           = 42   # box width

    def row(label: str, value: str, flag: str = "") -> str:
        content = f"{label:<16}{value}"
        if flag:
            content = f"{content:<{W-4}}{flag}"
        return f"║ {content:<{W-2}} ║"

    def divider() -> str:
        return "╠" + "═" * W + "╣"

    lines = [
        "╔" + "═" * W + "╗",
        f"║ {'DRY-RUN: ' + symbol + '  [' + tier + ']':<{W-2}} ║",
        divider(),
    ]

    if not valid:
        reason = p.get("reason", "unknown")
        lines.append(row("REJECTED:", reason[:W-18]))
        if len(reason) > W - 18:
            # Wrap long reason
            for chunk in [reason[i:i+W-2] for i in range(W-18, len(reason), W-2)]:
                lines.append(f"║ {chunk:<{W-2}} ║")
        lines.append(divider())

    # Account
    if p.get("balance") is not None:
        lines.append(row("Balance", f"${p['balance']:.2f}"))
    if p.get("risk_percent") is not None and p.get("risk_amount") is not None:
        lines.append(row("Risk", f"{p['risk_percent']*100:.2f}%  →  ${p['risk_amount']:.4f}"))

    # Prices
    if p.get("entry") is not None:
        lines.append(row("Entry", f"{p['entry']}"))
    if p.get("sl") is not None:
        sl_pct = f"  ({p['sl_distance_pct']:.4f}%)" if p.get("sl_distance_pct") else ""
        lines.append(row("SL", f"{p['sl']}{sl_pct}"))
    if p.get("sl_distance") is not None:
        lines.append(row("SL Distance", f"${p['sl_distance']:.4f}"))

    if any(p.get(k) is not None for k in ("raw_qty", "rounded_qty", "notional")):
        lines.append(divider())

    # Qty & Notional
    if p.get("raw_qty") is not None:
        lines.append(row("Raw qty", f"{p['raw_qty']:.8f}"))
    if p.get("rounded_qty") is not None:
        filters = None
        try:
            from .precision import get_symbol_filters
            filters = get_symbol_filters(symbol)
        except Exception:
            pass
        step_str = f"  (step {filters['step_size']})" if filters else ""
        lines.append(row("Rounded qty", f"{p['rounded_qty']}{step_str}"))
    if p.get("notional") is not None:
        min_notl = p.get("min_notional", 0)
        notl_flag = "✓" if p["notional"] >= min_notl else "✗"
        lines.append(row("Notional", f"${p['notional']:.4f}", notl_flag))
    if p.get("min_notional") is not None:
        lines.append(row("Min notional", f"${p['min_notional']:.2f}"))

    if any(p.get(k) is not None for k in ("final_leverage", "margin_required")):
        lines.append(divider())

    # Leverage & Margin
    if p.get("final_leverage") is not None:
        lev_str = (
            f"{p['final_leverage']}x  "
            f"(need: {p['required_leverage']}x  "
            f"target: {p['target_leverage']}x  "
            f"cap: {p['allowed_leverage']}x)"
        )
        if p.get("leverage_clamp_reason"):
            lev_str += f"  reason: {p['leverage_clamp_reason']}"
        lines.append(row("Leverage", lev_str))
    if p.get("margin_required") is not None:
        lines.append(row("Margin posted", f"${p['margin_required']:.4f}"))
    lines.append(row("Margin mode", p.get("margin_mode", "CROSS")))

    lines.append(divider())

    # Result
    if valid:
        lines.append(row("RESULT", "✓ EXECUTABLE"))
    else:
        lines.append(row("RESULT", "✗ NOT EXECUTABLE"))
        if p.get("reason"):
            lines.append(row("Reason", p["reason"][:W-18]))

    lines.append("╚" + "═" * W + "╝")
    print("\n".join(lines))
