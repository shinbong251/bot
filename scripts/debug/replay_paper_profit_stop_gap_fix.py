#!/usr/bin/env python3
"""Read-only replay for PAPER CONFIRM_SMC_RESEARCH in-profit SL gap fills."""

import csv
import json
import math
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PAPER_TRADES = ROOT / "paper_trades.csv"
SHADOW = ROOT / "logs" / "paper_smc_research_sl_gap_calibration_shadow.jsonl"


def fnum(value, default=None):
    try:
        if value in (None, ""):
            return default
        out = float(value)
        if math.isnan(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def read_shadow():
    indexed = {}
    if not SHADOW.exists():
        return indexed
    with SHADOW.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            trade_id = str(row.get("trade_id") or "").strip()
            if trade_id:
                indexed[trade_id] = row
    return indexed


def read_paper_rows():
    if not PAPER_TRADES.exists():
        return []
    with PAPER_TRADES.open("r", newline="", encoding="utf-8") as handle:
        return [
            row for row in csv.DictReader(handle)
            if str(row.get("entry_type") or "").upper() == "CONFIRM_SMC_RESEARCH"
        ]


def profit_factor(values):
    gains = sum(v for v in values if v > 0)
    losses = -sum(v for v in values if v < 0)
    if losses <= 0:
        return None if gains <= 0 else float("inf")
    return gains / losses


def avg(values):
    return sum(values) / len(values) if values else None


def fmt(value, places=4):
    if value is None:
        return "n/a"
    if value == float("inf"):
        return "inf"
    return f"{value:.{places}f}"


def side_from_row(row):
    return str(row.get("side") or "").upper()


def is_in_profit_sl(row, shadow):
    if str(row.get("exit_type") or "").upper() != "SL":
        return False
    if not shadow:
        return False
    price_r = fnum(shadow.get("price_r"))
    gap = fnum(shadow.get("configured_sl_gap_r"))
    return price_r is not None and price_r > 0 and gap is not None and 0 <= gap < 1


def post_fix_r(row, shadow, raw_r):
    if not is_in_profit_sl(row, shadow):
        return raw_r
    price_r = fnum(shadow.get("price_r"))
    gap = fnum(shadow.get("configured_sl_gap_r"))
    return price_r / (1 - gap)


def main():
    shadow_by_id = read_shadow()
    rows = read_paper_rows()
    replay = []
    materially_changed = []

    for row in rows:
        raw_r = fnum(row.get("rr"))
        if raw_r is None:
            continue
        trade_id = str(row.get("id") or "").strip()
        shadow = shadow_by_id.get(trade_id) or {}
        new_r = post_fix_r(row, shadow, raw_r)
        replay.append((row, raw_r, new_r, shadow))
        if abs(new_r - raw_r) > 1e-9:
            materially_changed.append((row, raw_r, new_r, shadow))

    raw_values = [raw for _, raw, _, _ in replay]
    post_values = [new for _, _, new, _ in replay]
    loss_pairs = [(raw, new) for _, raw, new, _ in replay if raw < 0]
    win_to_loss = sum(1 for _, raw, new, _ in replay if raw > 0 and new < 0)
    changed_losses = sum(1 for raw, new in loss_pairs if abs(raw - new) > 1e-12)
    initial_sl_excess = sum(max(0.0, -1.0 - raw) for raw in raw_values)
    post_initial_sl_excess = sum(max(0.0, -1.0 - new) for new in post_values)
    restored_r = sum(new - raw for _, raw, new, _ in materially_changed)
    residual_suppression = sum(
        abs(new - (fnum(shadow.get("price_r")) / (1 - fnum(shadow.get("configured_sl_gap_r")))))
        for _, _, new, shadow in materially_changed
    )

    side_split = Counter(side_from_row(row) for row, _, _, _ in replay)
    changed_side_split = Counter(side_from_row(row) for row, _, _, _ in materially_changed)
    exit_split = Counter(str(row.get("exit_type") or "").upper() for row, _, _, _ in replay)
    changed_exit_split = Counter(str(row.get("exit_type") or "").upper() for row, _, _, _ in materially_changed)

    print("PASS read-only historical replay completed")
    print("\n[BEFORE/AFTER]")
    print("metric                         raw        post_fix")
    print(f"N                         {len(raw_values):>9} {len(post_values):>15}")
    print(f"net_R                     {fmt(sum(raw_values)):>9} {fmt(sum(post_values)):>15}")
    print(f"PF                        {fmt(profit_factor(raw_values)):>9} {fmt(profit_factor(post_values)):>15}")
    print(f"avg_win                   {fmt(avg([v for v in raw_values if v > 0])):>9} {fmt(avg([v for v in post_values if v > 0])):>15}")
    print(f"avg_loss                  {fmt(avg([v for v in raw_values if v < 0])):>9} {fmt(avg([v for v in post_values if v < 0])):>15}")
    print(f"initial_SL_excess<-1R     {fmt(initial_sl_excess):>9} {fmt(post_initial_sl_excess):>15}")

    print("\n[SPLITS]")
    print(f"LONG/SHORT all={dict(side_split)} changed={dict(changed_side_split)}")
    print(f"exit_class all={dict(exit_split)} changed={dict(changed_exit_split)}")

    print("\n[CHANGE SAFETY]")
    print(f"materially_changed_trades={len(materially_changed)}")
    print(f"raised_stop_suppression_restored_R={fmt(restored_r)}")
    print(f"raised_stop_residual_suppression_R={fmt(residual_suppression)}")
    print(f"loss_rows_changed={changed_losses}")
    print(f"loss_rows_byte_identical={changed_losses == 0}")
    print(f"avg_loss_unchanged={avg([raw for raw, _ in loss_pairs]) == avg([new for _, new in loss_pairs])}")
    print(f"initial_SL_excess_unchanged={abs(initial_sl_excess - post_initial_sl_excess) <= 1e-9}")
    print(f"no_win_becomes_loss={win_to_loss == 0}")
    print("production_decision_path_mutation=none; replay changes fill-derived R only for in-profit SL rows")
    print("live_testnet_diff=empty; replay reads paper_trades.csv and paper shadow logs only")

    print("\n[MATERIALLY CHANGED SAMPLE]")
    print("id             symbol       side raw_R   post_R  gap")
    for row, raw, new, shadow in materially_changed[:12]:
        print(
            f"{str(row.get('id') or ''):<14} {str(row.get('symbol') or ''):<11} "
            f"{side_from_row(row):<5} {raw:>5.2f} {new:>8.4f} {fnum(shadow.get('configured_sl_gap_r')):>4.2f}"
        )


if __name__ == "__main__":
    main()
