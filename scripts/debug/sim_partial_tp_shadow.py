#!/usr/bin/env python3
"""Simulator tests for partial TP shadow accounting."""

import math


def _safe_float(value, default=None):
    try:
        if value in (None, ""):
            return default
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _model_values(current_r, max_profit_r):
    if current_r is None:
        return {}
    if max_profit_r is None:
        return {
            "model_30_70_r": None,
            "model_40_60_r": None,
            "model_50_50_r": None,
            "model_60_40_r": None,
        }
    if max_profit_r < 1.0:
        return {
            "model_30_70_r": current_r,
            "model_40_60_r": current_r,
            "model_50_50_r": current_r,
            "model_60_40_r": current_r,
        }
    return {
        "model_30_70_r": 0.3 + 0.7 * current_r,
        "model_40_60_r": 0.4 + 0.6 * current_r,
        "model_50_50_r": 0.5 + 0.5 * current_r,
        "model_60_40_r": 0.6 + 0.4 * current_r,
    }


def _shadow_row(t, source, calibrated_fields=None):
    calibrated_fields = calibrated_fields if isinstance(calibrated_fields, dict) else {}
    raw_r = _safe_float(t.get("rr_real"), None)
    calibrated_r = _safe_float(calibrated_fields.get("calibrated_r_cap_1_0"), None)
    current_r = calibrated_r if source == "paper" and calibrated_r is not None else raw_r
    max_profit_r = _safe_float(t.get("max_profit_r"), None)
    models = _model_values(current_r, max_profit_r)
    row = {
        "realized_r_current": current_r,
        "calibrated_realized_r_current": calibrated_r,
        "max_profit_r": max_profit_r,
        "reason": "max_profit_r_missing" if max_profit_r is None else None,
    }
    row.update(models)
    row["improved_by_partial"] = {
        "30_70": models.get("model_30_70_r") is not None and models["model_30_70_r"] > current_r,
        "40_60": models.get("model_40_60_r") is not None and models["model_40_60_r"] > current_r,
        "50_50": models.get("model_50_50_r") is not None and models["model_50_50_r"] > current_r,
        "60_40": models.get("model_60_40_r") is not None and models["model_60_40_r"] > current_r,
    }
    return row


def _trade(**overrides):
    row = {
        "id": "sim-1",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "time": 1000.0,
        "close_time": 1300.0,
        "entry": 100.0,
        "exit_price": 101.0,
        "exit_type": "TRAIL",
        "rr_real": 0.5,
        "max_profit_r": 1.0,
        "trail_phase": 3,
        "tp_hit": False,
        "time_to_1r": 12.0,
        "time_spent_above_1r": 6.0,
    }
    row.update(overrides)
    return row


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<72} {status} {detail}")
    return bool(condition)


def main():
    results = []

    row = _shadow_row(
        _trade(rr_real=-0.4, max_profit_r=0.99),
        "paper",
    )
    results.append(_assert(
        "max_profit_r < 1 => all models equal current",
        row["model_30_70_r"] == row["realized_r_current"]
        and row["model_40_60_r"] == row["realized_r_current"]
        and row["model_50_50_r"] == row["realized_r_current"]
        and row["model_60_40_r"] == row["realized_r_current"],
        str(row),
    ))

    row = _shadow_row(
        _trade(rr_real=-0.2, max_profit_r=1.2),
        "paper",
    )
    results.append(_assert(
        "max_profit_r >= 1 and current < 1 => partial improves",
        row["model_50_50_r"] > row["realized_r_current"]
        and row["improved_by_partial"]["50_50"] is True,
        str(row),
    ))

    row = _shadow_row(
        _trade(rr_real=2.0, max_profit_r=2.4),
        "paper",
    )
    results.append(_assert(
        "max_profit_r >= 1 and current > 1 => partial worsens",
        row["model_50_50_r"] < row["realized_r_current"]
        and row["improved_by_partial"]["50_50"] is False,
        str(row),
    ))

    row = _shadow_row(
        _trade(rr_real=2.0, max_profit_r=2.4),
        "paper",
    )
    results.append(_assert(
        "30/70 preserves runner more than 50/50",
        row["model_30_70_r"] > row["model_50_50_r"],
        str(row),
    ))

    row = _shadow_row(
        _trade(rr_real=0.25, max_profit_r=None),
        "paper",
    )
    results.append(_assert(
        "missing max_profit_r is logged as unknown, not crashing",
        row is not None
        and row["reason"] == "max_profit_r_missing"
        and row["model_50_50_r"] is None,
        str(row),
    ))

    row = _shadow_row(
        _trade(rr_real=-1.4, max_profit_r=1.1),
        "paper",
        calibrated_fields={"calibrated_r_cap_1_0": -1.0},
    )
    results.append(_assert(
        "paper uses calibrated R when available",
        row["realized_r_current"] == -1.0
        and row["calibrated_realized_r_current"] == -1.0,
        str(row),
    ))

    row = _shadow_row(
        _trade(rr_real=0.37, max_profit_r=1.1),
        "live",
        calibrated_fields={"calibrated_r_cap_1_0": -1.0},
    )
    results.append(_assert(
        "live uses actual R",
        row["realized_r_current"] == 0.37
        and row["calibrated_realized_r_current"] == -1.0,
        str(row),
    ))

    if all(results):
        print("PASS partial TP shadow simulator completed")
        return 0
    print("FAIL partial TP shadow simulator failed")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
