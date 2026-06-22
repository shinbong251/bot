# -*- coding: utf-8 -*-
"""
Central Telegram formatter / style guard.

Single source of truth for the visual building blocks of trade alerts so that
future Telegram edits stop re-breaking the font/format around Entry, SL, TP,
RR and trailing fields.

Canonical rules (do not diverge per-file):
  * side icon:     LONG = 🟢   SHORT = 🔴   UNKNOWN = ⚪
  * outcome icon:  win/TP = ✅  loss/SL = ❌  BE = ⚪
  * null value:    "null"
  * separator:     " · "  (one style, used everywhere here)
  * prices go through the canonical price formatter (notifier.fmt_price)

This module is additive: existing renderers are left untouched. New/updated
Telegram code should build fields with these helpers. All strings returned are
valid Unicode; rendering does not depend on the terminal codepage.
"""

# One canonical separator. Do not introduce other Unicode separators elsewhere.
SEP = " · "
NULL = "null"

_SIDE_ICONS = {"LONG": "🟢", "SHORT": "🔴"}
_SIDE_ICON_UNKNOWN = "⚪"

_OUTCOME_WIN = "✅"
_OUTCOME_LOSS = "❌"
_OUTCOME_BE = "⚪"


def side_icon(side):
    """LONG = 🟢, SHORT = 🔴, anything else = ⚪."""
    return _SIDE_ICONS.get(str(side or "").upper(), _SIDE_ICON_UNKNOWN)


def outcome_icon(status=None, rr=None):
    """win/TP = ✅, loss/SL = ❌, breakeven = ⚪. rr takes precedence over status."""
    if rr is not None:
        try:
            rr_value = float(rr)
        except (TypeError, ValueError):
            rr_value = None
        if rr_value is not None:
            if abs(rr_value) <= 0.01:
                return _OUTCOME_BE
            return _OUTCOME_WIN if rr_value > 0 else _OUTCOME_LOSS
    s = str(status or "").upper()
    if s in ("WIN", "TP"):
        return _OUTCOME_WIN
    if s in ("BE", "BREAKEVEN"):
        return _OUTCOME_BE
    if s in ("LOSS", "LOSE", "SL"):
        return _OUTCOME_LOSS
    return _OUTCOME_BE


def fmt_null(value):
    """Canonical null rendering for any scalar field."""
    return NULL if value in (None, "") else str(value)


def fmt_price_field(value, symbol):
    """Format a price field via the canonical price formatter; null-safe."""
    if value in (None, ""):
        return NULL
    try:
        from notifier import fmt_price
        return fmt_price(float(value), symbol)
    except Exception:
        return NULL


def fmt_rr(value):
    """Canonical RR rendering; null-safe."""
    if value in (None, ""):
        return NULL
    try:
        return str(round(float(value), 2))
    except (TypeError, ValueError):
        return NULL


def price_block(entry, sl, tp, symbol, rr=None):
    """E · SL · TP [· RR] line built from canonical fields."""
    parts = [
        f"E {fmt_price_field(entry, symbol)}",
        f"SL {fmt_price_field(sl, symbol)}",
        f"TP {fmt_price_field(tp, symbol)}",
    ]
    if rr is not None:
        parts.append(f"RR {fmt_rr(rr)}")
    return SEP.join(parts)


def trail_block(old_sl, new_sl, symbol, sl_r=None):
    """Old SL → New SL [· R] line built from canonical fields."""
    line = (
        f"Old SL {fmt_price_field(old_sl, symbol)}{SEP}"
        f"New SL {fmt_price_field(new_sl, symbol)}"
    )
    if sl_r is not None:
        line += f"{SEP}{fmt_rr(sl_r)}R"
    return line
