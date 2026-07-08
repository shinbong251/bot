#!/usr/bin/env python3
"""Forward audit for the PAPER-only CONFIRM_SMC_RESEARCH location gate.

Read-only. Reports new PAPER_LOCATION_GATE_BLOCK rows and allowed paper outcomes
from existing CSV/JSONL history. It does not write logs, state, or config.
"""

import csv
import json
import os
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"
QUALIFIED_DECISIONS = LOG_DIR / "paper_smc_research_qualified_decisions.jsonl"
PAPER_TRADES = ROOT / "paper_trades.csv"
ENTRY_TYPE = "CONFIRM_SMC_RESEARCH"

# Decisions that occur BEFORE the location gate in _dispatch_paper_smc_research_qualified_lane.
PRE_GATE_DECISIONS = {
    "REJECT",
    "CAP_REACHED",
    "MAX_OPEN_REACHED",
    "DUPLICATE_OR_SYMBOL_LOCKED",
}


def _gate_effective_ts():
    """Best-effort unix ts from which the running bot has the gate code + flag.

    The paper gate code is loaded at process start, so its effective boundary is
    the latest restart of main.py (not the source file mtime). Override with the
    GATE_EFFECTIVE_TS env var (unix seconds). Returns (ts_or_None, source_str).
    Read-only; no subprocess, no new dependencies.
    """
    override = os.environ.get("GATE_EFFECTIVE_TS")
    if override:
        try:
            return float(override), "env:GATE_EFFECTIVE_TS"
        except (TypeError, ValueError):
            pass
    try:
        btime = None
        with open("/proc/stat", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("btime"):
                    btime = int(line.split()[1])
                    break
        if btime is None:
            return None, "unavailable"
        clk = os.sysconf("SC_CLK_TCK")
        best = None
        for pid in os.listdir("/proc"):
            if not pid.isdigit():
                continue
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as handle:
                    cmd = handle.read().replace(b"\0", b" ").decode("utf-8", "ignore")
                if "main.py" not in cmd:
                    continue
                with open(f"/proc/{pid}/stat", encoding="utf-8") as handle:
                    starttime = int(handle.read().split()[21])
                ts = btime + starttime / clk
                if best is None or ts > best:
                    best = ts
            except (OSError, ValueError, IndexError):
                continue
        return (best, "proc:main.py_start") if best is not None else (None, "unavailable")
    except OSError:
        return None, "unavailable"


def _f(value):
    try:
        if value in (None, "", "None"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def iter_jsonl(path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def read_paper_trades():
    if not PAPER_TRADES.exists():
        return {}
    rows = {}
    with PAPER_TRADES.open("r", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("entry_type") or "").upper() != ENTRY_TYPE:
                continue
            rows[str(row.get("id"))] = row
    return rows


def perf(trades):
    values = []
    immediate = 0
    for trade in trades:
        rr = _f(trade.get("rr"))
        if rr is None:
            continue
        values.append(rr)
        mfe = _f(trade.get("max_r"))
        if (mfe is not None and mfe < 0.5) or (
            str(trade.get("status") or "").upper() == "LOSE"
            and str(trade.get("exit_type") or "").upper() == "SL"
        ):
            immediate += 1
    wins = sum(1 for rr in values if rr > 0)
    gross_win = sum(rr for rr in values if rr > 0)
    gross_loss = -sum(rr for rr in values if rr < 0)
    return {
        "n": len(values),
        "net_r": sum(values),
        "win_rate": wins / len(values) if values else None,
        "immediate_sl_rate": immediate / len(values) if values else None,
        "pf": gross_win / gross_loss if gross_loss > 0 else None,
    }


def fmt(value, digits=2):
    if value is None:
        return "NA"
    return f"{value:.{digits}f}"


def pct(value):
    return "NA" if value is None else f"{value * 100:.1f}%"


def main():
    trades_by_id = read_paper_trades()
    blocked_count = 0
    blocked_keys = set()
    opened_keys = set()
    allowed_open_count = 0
    allowed_trades = []
    blocked_reasons = Counter()

    effective_ts, effective_src = _gate_effective_ts()
    pre_gate_reject_count_by_reason = Counter()
    otherwise_open_eligible_would_block_count = 0
    would_block_opened_bug_count = 0
    open_would_block_all_history = 0

    for row in iter_jsonl(QUALIFIED_DECISIONS) or []:
        key = row.get("dedup_key") or row.get("research_dedup_key")
        decision = row.get("decision")
        reason = row.get("reason")
        would_block = row.get("confirm_smc_entry_location_would_block") is True
        row_ts = row.get("observed_at_unix")
        post_effective = (
            effective_ts is None
            or (isinstance(row_ts, (int, float)) and row_ts >= effective_ts)
        )
        if decision == "OPEN" and would_block:
            open_would_block_all_history += 1
        if post_effective:
            if (
                decision == "PREFILTER_REJECT"
                and reason == "PAPER_LOCATION_GATE_BLOCK"
            ):
                otherwise_open_eligible_would_block_count += 1
            elif decision in PRE_GATE_DECISIONS and would_block:
                bucket = reason or "UNKNOWN"
                if reason == "qualified_reject":
                    bucket = f"qualified_reject:{row.get('qualified_reject_subreason')}"
                pre_gate_reject_count_by_reason[bucket] += 1
            elif decision == "OPEN" and would_block:
                otherwise_open_eligible_would_block_count += 1
                would_block_opened_bug_count += 1

        if (
            row.get("decision") == "PREFILTER_REJECT"
            and row.get("reason") == "PAPER_LOCATION_GATE_BLOCK"
            and row.get("gate_source") == "EXISTING_LOCATION_BLOCK_SHADOW"
        ):
            blocked_count += 1
            if key:
                blocked_keys.add(key)
            blocked_reasons[row.get("location_gate_reason") or "UNKNOWN"] += 1
            continue
        if (
            row.get("decision") == "OPEN"
            and str(row.get("entry_type") or ENTRY_TYPE).upper() == ENTRY_TYPE
        ):
            allowed_open_count += 1
            if key:
                opened_keys.add(key)
            trade = trades_by_id.get(str(row.get("opened_trade_id")))
            if trade:
                allowed_trades.append(trade)

    allowed_perf = perf(allowed_trades)
    same_key_opened_after_block = len(blocked_keys.intersection(opened_keys))
    retained_total = blocked_count + allowed_open_count
    retained_pct = (allowed_open_count / retained_total) if retained_total else None
    too_broad = bool(retained_total and retained_pct is not None and retained_pct < 0.25)

    print("PAPER LOCATION GATE FORWARD AUDIT")
    print(f"number of paper candidates blocked: {blocked_count}")
    print(
        "blocked bucket simulated R from later paper outcome if available: "
        f"joinable_same_key_opens={same_key_opened_after_block}"
    )
    print(f"paper trades allowed: {allowed_open_count}")
    print(f"allowed net_R: {fmt(allowed_perf['net_r'])}")
    print(f"allowed PF: {fmt(allowed_perf['pf'])}")
    print(f"immediate_SL rate after gate on joinable allowed trades: {pct(allowed_perf['immediate_sl_rate'])}")
    print(f"volume retained: {pct(retained_pct)}")
    print(f"whether gate is too broad: {'YES' if too_broad else 'NO' if retained_total else 'NEED_DATA'}")

    print("\nTop block reasons")
    if not blocked_reasons:
        print("NEED_DATA: no PAPER_LOCATION_GATE_BLOCK rows yet.")
    for reason, count in blocked_reasons.most_common(10):
        print(f"{reason}: {count}")

    print("\nGATE WIRING FORWARD METRICS")
    if effective_ts is None:
        print(f"gate_effective_ts: NONE (source={effective_src}) -> scope=ALL_HISTORY")
    else:
        print(
            f"gate_effective_ts: {effective_ts:.0f} (source={effective_src}) "
            "-> scope=rows at/after this ts"
        )
    print(f"open_would_block_all_history (pre-enable shadow + bug): {open_would_block_all_history}")
    print("pre_gate_reject_count_by_reason:")
    if not pre_gate_reject_count_by_reason:
        print("  (none)")
    for bucket, count in pre_gate_reject_count_by_reason.most_common():
        print(f"  {bucket}: {count}")
    print(
        "otherwise_open_eligible_would_block_count: "
        f"{otherwise_open_eligible_would_block_count}"
    )
    print(f"would_block_opened_bug_count: {would_block_opened_bug_count}")

    if would_block_opened_bug_count > 0:
        gate_verdict = "FAIL_WIRING_BUG"
    elif otherwise_open_eligible_would_block_count > 0:
        gate_verdict = "PASS_GATE_ACTIVE"
    else:
        gate_verdict = "EXPECTED_NEED_DATA"
    print(f"gate_wiring_verdict: {gate_verdict}")

    print("\nVERDICT:", "NEED_DATA" if not blocked_count else "PASS")


if __name__ == "__main__":
    main()
