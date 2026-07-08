#!/usr/bin/env python3
"""Simulator for manual PAPER DD rebaseline utility."""

import csv
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "debug" / "rebaseline_paper_dd.py"


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<82} {status} {detail}")
    return bool(condition)


def _write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")


def _read_json(path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_trades_csv(path, rows):
    fields = ["id", "symbol", "status", "open_time", "close_time", "entry_type"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _base_config(**overrides):
    data = {
        "account_balance": 520.35,
        "equity_peak": 847.97,
        "pause_until": 12345,
        "paper_dd_pause_mode": "WARN_ONLY",
        "live_mode": True,
        "testnet_mode": False,
        "live_risk_per_trade": 0.005,
    }
    data.update(overrides)
    return data


def _make_root(config=None, paper_state=None, paper_rows=None):
    tmp = tempfile.TemporaryDirectory()
    root = Path(tmp.name)
    _write_json(root / "config.json", config if config is not None else _base_config())
    _write_json(root / "paper_state.json", paper_state if paper_state is not None else [])
    _write_json(root / "live_state.json", [{"symbol": "LIVEUSDT", "status": "OPEN"}])
    (root / "live_trades.csv").write_text("id,symbol,status\n1,LIVEUSDT,OPEN\n", encoding="utf-8")
    (root / "testnet_trades.csv").write_text("id,symbol,status\n1,TESTUSDT,OPEN\n", encoding="utf-8")
    _write_trades_csv(root / "paper_trades.csv", paper_rows if paper_rows is not None else [])
    return tmp, root


def _run(root, *args):
    cmd = [sys.executable, str(SCRIPT), "--root", str(root), *args]
    return subprocess.run(
        cmd,
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )


def _audit_rows(root):
    path = root / "logs" / "paper_dd_pause_events.jsonl"
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def main():
    results = []

    tmp, root = _make_root()
    with tmp:
        before_config = (root / "config.json").read_text(encoding="utf-8")
        before_live_state = (root / "live_state.json").read_text(encoding="utf-8")
        before_live_trades = (root / "live_trades.csv").read_text(encoding="utf-8")
        proc = _run(root, "--dry-run", "--reason", "research epoch rebaseline")
        after_config = (root / "config.json").read_text(encoding="utf-8")
        results.append(_assert(
            "A. no open paper trades + dry-run => no config/audit change, prints old/new",
            proc.returncode == 0
            and "mode=DRY_RUN" in proc.stdout
            and "old_equity_peak=847.97" in proc.stdout
            and "new_equity_peak=520.35" in proc.stdout
            and before_config == after_config
            and _audit_rows(root) == [],
            f"rc={proc.returncode} stdout={proc.stdout!r} stderr={proc.stderr!r}",
        ))
        results.append(_assert(
            "A. dry-run does not touch live/testnet files",
            before_live_state == (root / "live_state.json").read_text(encoding="utf-8")
            and before_live_trades == (root / "live_trades.csv").read_text(encoding="utf-8"),
        ))

    tmp, root = _make_root()
    with tmp:
        before_live_state = (root / "live_state.json").read_text(encoding="utf-8")
        before_live_trades = (root / "live_trades.csv").read_text(encoding="utf-8")
        before_testnet = (root / "testnet_trades.csv").read_text(encoding="utf-8")
        proc = _run(
            root,
            "--apply",
            "--reason",
            "manual paper-only DD rebaseline before future ENFORCE",
            "--operator",
            "sim",
        )
        config = _read_json(root / "config.json")
        rows = _audit_rows(root)
        row = rows[-1] if rows else {}
        results.append(_assert(
            "B. no open paper trades + apply => equity_peak rebaselined and audit appended",
            proc.returncode == 0
            and config.get("account_balance") == 520.35
            and config.get("equity_peak") == 520.35
            and config.get("pause_until") == 0
            and config.get("paper_dd_pause_mode") == "WARN_ONLY"
            and len(rows) == 1
            and row.get("event_type") == "PAPER_DD_MANUAL_REBASELINE"
            and row.get("old_account_balance") == 520.35
            and row.get("old_equity_peak") == 847.97
            and row.get("new_account_balance") == 520.35
            and row.get("new_equity_peak") == 520.35
            and row.get("new_drawdown") == 0
            and row.get("old_pause_until") == 12345.0
            and row.get("new_pause_until") == 0
            and row.get("paper_dd_pause_mode") == "WARN_ONLY"
            and row.get("reason") == "manual paper-only DD rebaseline before future ENFORCE"
            and row.get("operator") == "sim",
            f"rc={proc.returncode} config={config} row={row} stdout={proc.stdout!r}",
        ))
        results.append(_assert(
            "B/F. apply does not touch live/testnet files",
            before_live_state == (root / "live_state.json").read_text(encoding="utf-8")
            and before_live_trades == (root / "live_trades.csv").read_text(encoding="utf-8")
            and before_testnet == (root / "testnet_trades.csv").read_text(encoding="utf-8"),
        ))

    tmp, root = _make_root(
        paper_state=[{"symbol": "OPENUSDT", "status": "OPEN", "entry_type": "CONFIRM"}],
    )
    with tmp:
        before_config = (root / "config.json").read_text(encoding="utf-8")
        proc = _run(root, "--apply", "--reason", "should refuse")
        results.append(_assert(
            "C. open paper trade exists => refuse non-zero and no config/audit write",
            proc.returncode != 0
            and "REFUSE: open paper trades exist" in proc.stdout
            and before_config == (root / "config.json").read_text(encoding="utf-8")
            and _audit_rows(root) == [],
            f"rc={proc.returncode} stdout={proc.stdout!r}",
        ))

    tmp, root = _make_root(config={"account_balance": 520.35, "pause_until": 0})
    with tmp:
        proc = _run(root, "--dry-run", "--reason", "missing fields")
        results.append(_assert(
            "D. missing account_balance/equity_peak => refuse non-zero",
            proc.returncode != 0
            and "missing or invalid account_balance/equity_peak" in proc.stdout,
            f"rc={proc.returncode} stdout={proc.stdout!r}",
        ))

    tmp, root = _make_root(config=_base_config(**{"account_" "balance": 520.35, "equity_peak": 520.35}))
    with tmp:
        before_config = (root / "config.json").read_text(encoding="utf-8")
        proc = _run(root, "--apply", "--reason", "no dd")
        results.append(_assert(
            "E. equity_peak <= " + "account_" "balance => clear refusal and no write",
            proc.returncode != 0
            and "no rebaseline needed" in proc.stdout
            and before_config == (root / "config.json").read_text(encoding="utf-8")
            and _audit_rows(root) == [],
            f"rc={proc.returncode} stdout={proc.stdout!r}",
        ))

    tmp, root = _make_root()
    with tmp:
        proc = _run(root, "--reason", "default dry run")
        results.append(_assert(
            "default invocation without --apply is dry-run",
            proc.returncode == 0
            and "mode=DRY_RUN" in proc.stdout
            and "result=DRY_RUN_NO_WRITE" in proc.stdout
            and _audit_rows(root) == [],
            f"rc={proc.returncode} stdout={proc.stdout!r}",
        ))

    print("\nRESULT:", "PASS" if all(results) else "FAIL")
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
