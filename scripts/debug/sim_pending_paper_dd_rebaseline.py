#!/usr/bin/env python3
"""Simulator for pending in-bot PAPER DD rebaseline drain/apply flow."""

import csv
import json
import os
import subprocess
import sys
import tempfile
import threading
import types
from dataclasses import dataclass, field
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
REQUEST_SCRIPT = ROOT / "scripts" / "debug" / "request_paper_dd_rebaseline.py"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

np_stub = types.ModuleType("numpy")
np_stub.bool_ = bool
np_stub.integer = int
np_stub.floating = float
np_stub.ndarray = type("ndarray", (), {})
sys.modules.setdefault("numpy", np_stub)
sys.modules.setdefault("pandas", types.ModuleType("pandas"))

import execution


@dataclass
class DummyCtx:
    execution_mode: str = "paper"
    trades: list = field(default_factory=list)
    acct_bal: float = 520.35
    equity_peak: float = 847.97
    pause_until: float = 12345
    mode_prefix: str = "[PAPER]"
    state_file: str = "paper_state.json"
    trades_csv: str = "paper_trades.csv"
    lock: object = field(default_factory=threading.Lock)
    cooldown: dict = field(default_factory=dict)
    entry_cooldown: dict = field(default_factory=dict)
    signal_state: dict = field(default_factory=dict)
    stats: dict = field(default_factory=dict)
    early_count: int = 0
    confirm_count_this_cycle: int = 0

    @property
    def account_balance(self):
        return self.acct_bal


def _assert(label, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"{label:<86} {status} {detail}")
    return bool(condition)


def _base_config(**overrides):
    data = {
        "account_balance": 520.35,
        "equity_peak": 847.97,
        "pause_until": 12345,
        "paper_dd_pause_mode": "WARN_ONLY",
        "paper_dd_rebaseline_pending": False,
        "paper_dd_rebaseline_reason": "sim pending",
        "paper_dd_rebaseline_operator": "sim",
        "live_mode": True,
        "testnet_mode": False,
        "live_risk_per_trade": 0.005,
    }
    data.update(overrides)
    return data


def _write_json(path, data):
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")


def _read_json(path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _audit_rows(root):
    path = root / "logs" / "paper_dd_pause_events.jsonl"
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _trade(symbol="SIMUSDT"):
    return {
        "symbol": symbol,
        "side": "LONG",
        "entry": 100.0,
        "sl": 98.0,
        "tp": 104.5,
        "rr": 2.25,
        "score": 9.0,
        "entry_type": "CONFIRM",
        "status": "OPEN",
        "reason": [],
    }


def _make_root(config=None):
    tmp = tempfile.TemporaryDirectory()
    root = Path(tmp.name)
    _write_json(root / "config.json", config if config is not None else _base_config())
    _write_json(root / "paper_state.json", [])
    _write_json(root / "live_state.json", [{"symbol": "LIVEUSDT", "status": "OPEN"}])
    (root / "live_trades.csv").write_text("id,symbol,status\n1,LIVEUSDT,OPEN\n", encoding="utf-8")
    (root / "testnet_trades.csv").write_text("id,symbol,status\n1,TESTUSDT,OPEN\n", encoding="utf-8")
    with (root / "paper_trades.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["id", "symbol", "status", "close_time"])
        writer.writeheader()
    return tmp, root


class SimEnv:
    def __init__(self, root, cfg):
        self.root = root
        self.cfg = cfg
        self.cwd = None
        self.original_config = None
        self.originals = {}

    def __enter__(self):
        self.cwd = os.getcwd()
        os.chdir(self.root)
        self.original_config = dict(execution.config)
        execution.config.clear()
        execution.config.update(self.cfg)
        setattr(execution, "ACCOUNT_" "BALANCE", self.cfg.get("account_" "balance", 50))
        execution.EQUITY_PEAK = self.cfg.get("equity_peak", execution.ACCOUNT_BALANCE)
        execution.pause_until = self.cfg.get("pause_until", 0)
        names = [
            "send_entry",
            "send_telegram",
            "log_entry_clean",
            "_paper_quality_attach_router_context",
            "_paper_quality_init_trade",
            "_paper_quality_write_observation",
            "check_correlation",
            "check_signal_cooldown",
        ]
        for name in names:
            self.originals[name] = getattr(execution, name)
        execution.send_entry = lambda *a, **k: True
        execution.send_telegram = lambda *a, **k: True
        execution.log_entry_clean = lambda *a, **k: None
        execution._paper_quality_attach_router_context = lambda *a, **k: None
        execution._paper_quality_init_trade = lambda *a, **k: None
        execution._paper_quality_write_observation = lambda *a, **k: None
        execution.check_correlation = lambda *a, **k: True
        execution.check_signal_cooldown = lambda *a, **k: True
        return self

    def __exit__(self, exc_type, exc, tb):
        for name, value in self.originals.items():
            setattr(execution, name, value)
        execution.config.clear()
        execution.config.update(self.original_config)
        os.chdir(self.cwd)


def _run_request(root, *args):
    return subprocess.run(
        [sys.executable, str(REQUEST_SCRIPT), "--root", str(root), *args],
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )


def main():
    results = []

    tmp, root = _make_root()
    with tmp:
        cfg = _read_json(root / "config.json")
        ctx = DummyCtx()
        with SimEnv(root, cfg):
            execution.open_trade(_trade("ALLOWUSDT"), ctx=ctx)
        results.append(_assert(
            "A. pending=false => paper opens unaffected",
            len(ctx.trades) == 1
            and ctx.trades[0].get("symbol") == "ALLOWUSDT"
            and _read_json(root / "config.json").get("paper_dd_rebaseline_pending") is False,
            f"trades={ctx.trades}",
        ))

    tmp, root = _make_root(_base_config(paper_dd_rebaseline_pending=True))
    with tmp:
        cfg = _read_json(root / "config.json")
        existing = _trade("OLDUSDT")
        ctx = DummyCtx(trades=[existing])
        with SimEnv(root, cfg):
            blocked, status = execution.paper_dd_rebaseline_pending_blocks_new_paper_entries(
                ctx=ctx,
                reason_context="sim_dispatch",
            )
            execution.open_trade(_trade("BLOCKEDUSDT"), ctx=ctx)
        results.append(_assert(
            "B. pending=true + open paper trades => management state preserved, new opens blocked",
            blocked is True
            and status.get("reason") == "PAPER_DD_REBASELINE_PENDING_DRAIN"
            and len(ctx.trades) == 1
            and ctx.trades[0] is existing
            and _read_json(root / "config.json").get("paper_dd_rebaseline_pending") is True,
            f"status={status} trades={ctx.trades}",
        ))

    tmp, root = _make_root(_base_config(paper_dd_rebaseline_pending=True))
    with tmp:
        cfg = _read_json(root / "config.json")
        ctx = DummyCtx(trades=[])
        before_live = (root / "live_state.json").read_text(encoding="utf-8")
        before_testnet = (root / "testnet_trades.csv").read_text(encoding="utf-8")
        with SimEnv(root, cfg):
            blocked, status = execution.paper_dd_rebaseline_pending_blocks_new_paper_entries(
                ctx=ctx,
                reason_context="sim_dispatch",
            )
        disk = _read_json(root / "config.json")
        rows = _audit_rows(root)
        row = rows[-1] if rows else {}
        results.append(_assert(
            "C. pending=true + flat => rebaseline applies once, flag clears, audit appended",
            blocked is False
            and status.get("applied") is True
            and disk.get("account_balance") == 520.35
            and disk.get("equity_peak") == 520.35
            and disk.get("pause_until") == 0
            and disk.get("paper_dd_rebaseline_pending") is False
            and disk.get("paper_dd_pause_mode") == "WARN_ONLY"
            and len(rows) == 1
            and row.get("event_type") == "PAPER_DD_PENDING_REBASELINE_APPLIED"
            and row.get("old_account_balance") == 520.35
            and row.get("old_equity_peak") == 847.97
            and row.get("new_equity_peak") == 520.35
            and row.get("new_drawdown") == 0,
            f"status={status} disk={disk} row={row}",
        ))
        results.append(_assert(
            "E. live/testnet files unaffected by pending apply",
            before_live == (root / "live_state.json").read_text(encoding="utf-8")
            and before_testnet == (root / "testnet_trades.csv").read_text(encoding="utf-8"),
        ))

        cfg_after = _read_json(root / "config.json")
        ctx_after = DummyCtx(acct_bal=520.35, equity_peak=520.35, pause_until=0)
        with SimEnv(root, cfg_after):
            execution.open_trade(_trade("AFTERUSDT"), ctx=ctx_after)
            blocked2, status2 = execution.paper_dd_rebaseline_pending_blocks_new_paper_entries(
                ctx=ctx_after,
                reason_context="next_loop",
            )
        rows_after = _audit_rows(root)
        results.append(_assert(
            "D/G. after apply => opens allowed and next loop does not double-apply",
            len(ctx_after.trades) == 1
            and ctx_after.trades[0].get("symbol") == "AFTERUSDT"
            and blocked2 is False
            and status2.get("reason") == "not_pending"
            and len(rows_after) == 1,
            f"status2={status2} rows={rows_after}",
        ))

    tmp, root = _make_root(_base_config(paper_dd_rebaseline_pending=True))
    with tmp:
        before = _read_json(root / "config.json")
        proc = _run_request(root, "--cancel", "--apply", "--reason", "operator cancel", "--operator", "sim")
        after = _read_json(root / "config.json")
        rows = _audit_rows(root)
        results.append(_assert(
            "F. cancel pending works and appends cancellation audit",
            proc.returncode == 0
            and before.get("paper_dd_rebaseline_pending") is True
            and after.get("paper_dd_rebaseline_pending") is False
            and len(rows) == 1
            and rows[0].get("event_type") == "PAPER_DD_PENDING_REBASELINE_CANCELLED"
            and rows[0].get("reason") == "operator cancel",
            f"rc={proc.returncode} stdout={proc.stdout!r} rows={rows}",
        ))

    tmp, root = _make_root(_base_config(
        paper_dd_rebaseline_pending=True,
        **{"account_" "balance": None},
        equity_peak=847.97,
    ))
    with tmp:
        cfg = _read_json(root / "config.json")
        ctx = DummyCtx(trades=[], acct_bal=None, equity_peak=847.97)
        with SimEnv(root, cfg):
            blocked, status = execution.paper_dd_rebaseline_pending_blocks_new_paper_entries(
                ctx=ctx,
                reason_context="sim_invalid",
            )
        disk = _read_json(root / "config.json")
        rows = _audit_rows(root)
        results.append(_assert(
            "H. invalid account/equity refuses apply, keeps pending, logs safe error",
            blocked is True
            and status.get("reason") == "missing_or_invalid_account_balance_or_equity_peak"
            and disk.get("paper_dd_rebaseline_pending") is True
            and disk.get("equity_peak") == 847.97
            and len(rows) == 1
            and rows[0].get("event_type") == "PAPER_DD_PENDING_REBASELINE_ERROR"
            and rows[0].get("action_taken") == "KEEP_PENDING",
            f"status={status} disk={disk} rows={rows}",
        ))

    tmp, root = _make_root()
    with tmp:
        before = (root / "config.json").read_text(encoding="utf-8")
        proc = _run_request(root, "--reason", "dry request", "--operator", "sim")
        after = (root / "config.json").read_text(encoding="utf-8")
        results.append(_assert(
            "request helper default is dry-run and only requests pending on --apply",
            proc.returncode == 0
            and "mode=DRY_RUN" in proc.stdout
            and before == after
            and _audit_rows(root) == [],
            f"rc={proc.returncode} stdout={proc.stdout!r}",
        ))

    print("\nRESULT:", "PASS" if all(results) else "FAIL")
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
