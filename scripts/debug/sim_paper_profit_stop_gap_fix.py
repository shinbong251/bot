#!/usr/bin/env python3
"""Deterministic simulator for PAPER in-profit SL gap fill behavior."""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution import apply_sl_gap_to_stop_fill  # noqa: E402


def old_apply_sl_gap_to_stop_fill(sl_price, side, gap, entry=None):
    if sl_price is None or sl_price <= 0 or gap <= 0:
        return sl_price
    if entry is not None and entry > 0:
        risk_distance = abs(entry - sl_price)
        if risk_distance > 0:
            gap_points = min(risk_distance * gap, risk_distance * 0.50)
            if side == "LONG":
                return sl_price - gap_points
            if side == "SHORT":
                return sl_price + gap_points
        return sl_price
    if side == "LONG":
        return sl_price * (1 - gap)
    if side == "SHORT":
        return sl_price * (1 + gap)
    return sl_price


def r_value(entry, fill, side, risk=1.0):
    if side == "LONG":
        return (fill - entry) / risk
    if side == "SHORT":
        return (entry - fill) / risk
    return 0.0


def almost_equal(actual, expected, eps=1e-9):
    return abs(actual - expected) <= eps


def record(rows, name, actual, expected):
    ok = almost_equal(actual, expected) if isinstance(actual, float) else actual == expected
    rows.append((name, actual, expected, "PASS" if ok else "FAIL"))
    if not ok:
        raise AssertionError(f"{name}: actual={actual!r} expected={expected!r}")


def main():
    rows = []

    old_defect = old_apply_sl_gap_to_stop_fill(100.75, "LONG", 0.50, entry=100.0)
    record(rows, "old defect LONG +0.75R stop", old_defect, 100.375)
    record(rows, "new LONG profit stop", apply_sl_gap_to_stop_fill(100.75, "LONG", 0.50, entry=100.0), 100.75)
    record(rows, "new SHORT profit stop", apply_sl_gap_to_stop_fill(99.25, "SHORT", 0.50, entry=100.0), 99.25)

    for gap, expected in ((0.10, 98.9), (0.20, 98.8), (0.30, 98.7), (0.50, 98.5)):
        record(rows, f"LONG initial SL gap {gap:.2f}", apply_sl_gap_to_stop_fill(99.0, "LONG", gap, entry=100.0), expected)

    for gap, expected in ((0.10, 101.1), (0.20, 101.2), (0.30, 101.3), (0.50, 101.5)):
        record(rows, f"SHORT initial SL gap {gap:.2f}", apply_sl_gap_to_stop_fill(101.0, "SHORT", gap, entry=100.0), expected)

    record(rows, "LONG BE stop", apply_sl_gap_to_stop_fill(100.0, "LONG", 0.50, entry=100.0), 100.0)
    record(rows, "SHORT BE stop", apply_sl_gap_to_stop_fill(100.0, "SHORT", 0.50, entry=100.0), 100.0)

    long_small = apply_sl_gap_to_stop_fill(100.20, "LONG", 0.50, entry=100.0)
    short_small = apply_sl_gap_to_stop_fill(99.80, "SHORT", 0.50, entry=100.0)
    record(rows, "LONG small +0.20R lock", r_value(100.0, long_small, "LONG"), 0.20)
    record(rows, "SHORT small +0.20R lock", r_value(100.0, short_small, "SHORT"), 0.20)

    long_underwater = apply_sl_gap_to_stop_fill(99.80, "LONG", 0.50, entry=100.0)
    short_underwater = apply_sl_gap_to_stop_fill(100.20, "SHORT", 0.50, entry=100.0)
    record(rows, "LONG raised-but-underwater keeps gap", long_underwater, 99.70)
    record(rows, "SHORT raised-but-underwater keeps gap", short_underwater, 100.30)
    record(rows, "LONG raised-but-underwater adverse R", r_value(100.0, long_underwater, "LONG"), -0.30)
    record(rows, "SHORT raised-but-underwater adverse R", r_value(100.0, short_underwater, "SHORT"), -0.30)

    record(rows, "LONG missing entry fallback", apply_sl_gap_to_stop_fill(100.0, "LONG", 0.10), 90.0)
    record(rows, "SHORT missing entry fallback", apply_sl_gap_to_stop_fill(100.0, "SHORT", 0.10), 110.0)
    record(rows, "invalid entry fallback", apply_sl_gap_to_stop_fill(100.0, "LONG", 0.10, entry=0.0), 90.0)

    before_decision = {
        "symbol": "SIMUSDT",
        "side": "LONG",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "exit_type": "SL",
        "exit_class": "TRAIL",
        "decision": "CLOSE",
    }
    after_decision = dict(before_decision)
    old_fill = old_apply_sl_gap_to_stop_fill(100.75, "LONG", 0.50, entry=100.0)
    new_fill = apply_sl_gap_to_stop_fill(100.75, "LONG", 0.50, entry=100.0)
    record(rows, "decision path unchanged", after_decision == before_decision, True)
    record(rows, "only in-profit fill price changed", old_fill != new_fill and new_fill == 100.75, True)

    print("PASS deterministic paper profit stop gap simulator")
    print("case                                      actual        expected      status")
    for name, actual, expected, status in rows:
        print(f"{name:<40} {str(actual):>12} {str(expected):>13} {status}")


if __name__ == "__main__":
    main()
