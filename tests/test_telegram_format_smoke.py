# -*- coding: utf-8 -*-
"""
Smoke checks for the central Telegram formatter (telegram_format).

Builds sample messages for the documented alert shapes and asserts the key
fields are present. Does NOT send any Telegram message — pure string building.

Run:  python tests/test_telegram_format_smoke.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import telegram_format as tf


def build_samples():
    samples = {}

    # LONG research open
    samples["long_research_open"] = "\n".join([
        f"{tf.side_icon('LONG')} PAPER RESEARCH OPEN | LONG",
        "BTCUSDT",
        tf.price_block(43250.1, 42800.0, 44500.0, "BTCUSDT", rr=1.8),
    ])

    # SHORT research open
    samples["short_research_open"] = "\n".join([
        f"{tf.side_icon('SHORT')} PAPER RESEARCH OPEN | SHORT",
        "ETHUSDT",
        tf.price_block(2850.12, 2900.0, 2750.0, "ETHUSDT", rr=2.0),
    ])

    # research close (SL / loss)
    samples["research_close_sl"] = "\n".join([
        f"{tf.outcome_icon('LOSE', rr=-1.0)} BTCUSDT SHORT -1.0R | SMC-RESEARCH",
        f"Exit: SL @ {tf.fmt_price_field(43680.0, 'BTCUSDT')}",
    ])

    # generic CONFIRM close (win)
    samples["confirm_close"] = "\n".join([
        f"{tf.outcome_icon('WIN', rr=1.5)} PAPER • CONFIRM • CLOSED",
        f"SOLUSDT {tf.side_icon('LONG')} LONG",
        tf.price_block(125.430, 123.000, 132.000, "SOLUSDT", rr=1.5),
    ])

    # trail SL update
    samples["trail_update"] = "\n".join([
        f"🔁 DOGEUSDT {tf.side_icon('LONG')} LONG | TRAIL",
        tf.trail_block(0.1200, 0.1234, "DOGEUSDT", sl_r=0.5),
    ])

    # BE / profit-lock update (null TP shows canonical "null")
    samples["profit_lock"] = "\n".join([
        f"🔒 BTCUSDT {tf.side_icon('SHORT')} SHORT PROFIT LOCK",
        tf.price_block(43250.1, 43000.0, None, "BTCUSDT", rr=None),
    ])

    return samples


def main():
    samples = build_samples()

    for name, msg in samples.items():
        print(f"===== {name} =====")
        print(msg)
        print()

    # ---- assertions ----
    s = samples
    assert "🟢" in s["long_research_open"], "LONG icon missing"
    assert "🔴" in s["short_research_open"], "SHORT icon missing"
    assert "E " in s["long_research_open"] and "SL " in s["long_research_open"], "Entry/SL missing"
    assert "TP " in s["long_research_open"] and "RR " in s["long_research_open"], "TP/RR missing"
    assert "❌" in s["research_close_sl"], "loss outcome icon missing"
    assert "✅" in s["confirm_close"], "win outcome icon missing"
    assert "Old SL" in s["trail_update"] and "New SL" in s["trail_update"], "old/new SL missing"
    assert tf.SEP in s["long_research_open"], "canonical separator missing"
    assert tf.NULL in s["profit_lock"], "canonical null missing for empty TP"
    # icon canon
    assert tf.side_icon("???") == "⚪", "unknown side icon must be ⚪"
    assert tf.outcome_icon("BE", rr=0.0) == "⚪", "BE outcome icon must be ⚪"

    print("SMOKE OK: all telegram_format checks passed.")


if __name__ == "__main__":
    main()
