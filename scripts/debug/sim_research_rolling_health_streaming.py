#!/usr/bin/env python3
"""Parity and memory checks for bounded rolling-health summary."""

import argparse
import csv
import json
import resource
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.debug import audit_research_rolling_health as rh


def _write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _rss_kb():
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        return 0
    return 0


class _PatchPaths:
    def __init__(self, root):
        self.root = Path(root)
        self.old = {}

    def __enter__(self):
        logs = self.root / "logs"
        paths = {
            "LOG_DIR": logs,
            "PAPER_LIFECYCLE": logs / "paper_smc_research_lifecycle.jsonl",
            "PAPER_GAP_SHADOW": logs / "paper_smc_research_sl_gap_calibration_shadow.jsonl",
            "PAPER_MIN_LOCK": logs / "paper_smc_research_min_lock_shadow.jsonl",
            "LIVE_DECISIONS": logs / "live_smc_research_decisions.jsonl",
            "LIVE_MIN_LOCK": logs / "live_smc_research_min_lock_075_events.jsonl",
            "LIVE_STATE": self.root / "live_state.json",
            "LIVE_TRADES": self.root / "live_trades.csv",
            "SUMMARY_LOG": logs / "research_rolling_health.jsonl",
            "CONFIG_JSON": self.root / "config.json",
        }
        for name, value in paths.items():
            self.old[name] = getattr(rh, name)
            setattr(rh, name, value)
        return paths

    def __exit__(self, exc_type, exc, tb):
        for name, value in self.old.items():
            setattr(rh, name, value)


def _fixture(root):
    root = Path(root)
    logs = root / "logs"
    cfg = {
        "research_health_baseline_ts": 1000,
        "research_health_min_active_closed": 3,
        "research_health_use_active_only_for_live_scale": True,
        "max_live_research_trades": 2,
        "live_health_stale_streak_demote_enabled": True,
        "live_health_stale_streak_dormancy_hours": 48,
    }
    (root / "config.json").write_text(json.dumps(cfg), encoding="utf-8")
    (root / "live_state.json").write_text(json.dumps([
        {
            "id": "live_open_1",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "status": "OPEN",
            "entry_time": 2400,
            "entry_source": "actual_exchange_fill",
            "exchange_sl_id": "sl-ok",
            "exchange_sl_price_confirmed": 1.2,
            "sl_sync_fail_count": 0,
        }
    ]), encoding="utf-8")
    paper_rows = []
    for idx, value in enumerate([0.8, -1.4, 0.3, -0.6, 1.1, -1.3]):
        ts = 900 + idx * 100
        paper_rows.append({
            "event_type": "RESEARCH_CLOSED",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "trade_id": f"p{idx}",
            "research_join_key": f"pkey{idx}",
            "raw_realized_r": value,
            "close_reason": "SL" if value < -1.0 else "TP",
            "close_ts": ts,
            "side": "LONG" if idx % 2 else "SHORT",
            "phase": "BREAKOUT",
            "market_regime": "TREND",
        })
    _write_jsonl(logs / "paper_smc_research_lifecycle.jsonl", paper_rows)
    _write_jsonl(logs / "paper_smc_research_sl_gap_calibration_shadow.jsonl", [
        {"research_join_key": "pkey1", "configured_sl_gap_r": 0.5},
        {"research_join_key": "pkey5", "configured_sl_gap_r": 0.3},
    ])
    _write_jsonl(logs / "paper_smc_research_min_lock_shadow.jsonl", [
        {"research_join_key": "pkey2"},
        {"research_join_key": "pkey3"},
    ])
    live_decisions = [
        {
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "decision": "CLOSED",
            "id": "l1",
            "symbol": "AAAUSDT",
            "side": "LONG",
            "actual_realized_r": 0.4,
            "close_ts": 1100,
        },
        {
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "decision": "CLOSED",
            "id": "l2",
            "symbol": "BBBUSDT",
            "side": "SHORT",
            "actual_realized_r": -1.0,
            "close_ts": 1200,
        },
        {
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "decision": "PREFILTER_REJECT",
            "reason": "live_micro_blocked_sl_sync missing exchange sl",
            "symbol": "CCCUSDT",
            "side": "LONG",
        },
    ]
    _write_jsonl(logs / "live_smc_research_decisions.jsonl", live_decisions)
    _write_jsonl(logs / "live_smc_research_min_lock_075_events.jsonl", [
        {"entry_type": "CONFIRM_SMC_RESEARCH", "event_type": "SYNC_FAILED", "sync_result": "false", "trade_id": "l2", "symbol": "BBBUSDT", "side": "SHORT"},
        {"entry_type": "CONFIRM_SMC_RESEARCH", "event_type": "SYNC_OK", "sync_result": "true", "trade_id": "l2", "symbol": "BBBUSDT", "side": "SHORT"},
    ])
    _write_csv(root / "live_trades.csv", [
        {
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "status": "LOSS",
            "id": "l3",
            "symbol": "CCCUSDT",
            "side": "LONG",
            "actual_realized_r": -0.5,
            "close_ts": 1300,
        }
    ])


def _selected(row):
    live = row.get("live_metrics") or {}
    return {
        "paper_health": row.get("paper_health"),
        "legacy_health": row.get("legacy_health"),
        "paper_active_health": row.get("paper_active_health"),
        "live_health": row.get("live_health"),
        "promotion_status": row.get("promotion_status"),
        "active_closed_count": row.get("active_closed_count"),
        "last50": row.get("last50"),
        "active_last50": row.get("active_last50"),
        "since_min_lock_active": row.get("since_min_lock_active"),
        "live_closed_n": live.get("live_closed_n", live.get("n")),
        "live_rolling_net_r": live.get("live_rolling_net_r", live.get("net_r")),
        "live_loss_streak": live.get("live_loss_streak", live.get("consecutive_losses")),
        "loss_streak_current": live.get("loss_streak_current"),
        "loss_streak_stale_after_new_open": live.get("loss_streak_stale_after_new_open"),
        "reasons": row.get("reasons"),
    }


def run_parity():
    with tempfile.TemporaryDirectory(prefix="rh_stream_fixture_") as tmpdir:
        _fixture(tmpdir)
        with _PatchPaths(tmpdir):
            old = rh.build_summary_full_load_reference(source="fixture_old", write_summary=False)
            new = rh.build_summary(source="fixture_new", write_summary=False)
        old_selected = _selected(old)
        new_selected = _selected(new)
        if old_selected != new_selected:
            print(json.dumps({"old": old_selected, "new": new_selected}, indent=2, sort_keys=True, default=str))
            raise SystemExit("FAIL streaming parity drift")
    print("PASS streaming rolling-health fixture parity")
    run_min_lock_semantic_parity()


def _confirm_row(**fields):
    row = {"entry_type": "CONFIRM_SMC_RESEARCH"}
    row.update(fields)
    return row


def _open_state(enabled):
    if not enabled:
        return []
    return [
        {
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "status": "OPEN",
            "symbol": "AAAUSDT",
            "side": "LONG",
            "entry_source": "actual_exchange_fill",
            "exchange_sl_id": "sl-ok",
            "exchange_sl_price_confirmed": 1.0,
            "sl_sync_fail_count": 0,
        }
    ]


def _min_lock_reference(rows, has_open):
    return rh.live_safety_issues([], rows, live_state=_open_state(has_open))


def _min_lock_stream(rows, has_open):
    return rh.live_safety_issues_stream([], [], rows, live_state=_open_state(has_open))


def _assert_min_lock_case(name, rows):
    for has_open in (False, True):
        expected = _min_lock_reference(rows, has_open)
        actual = _min_lock_stream(rows, has_open)
        if expected != actual:
            print(json.dumps({
                "case": name,
                "has_open_research": has_open,
                "expected": expected,
                "actual": actual,
                "rows": rows,
            }, indent=2, sort_keys=True, default=str))
            raise SystemExit("FAIL min-lock semantic mismatch")


def run_min_lock_semantic_parity():
    cases = {
        "failed_only": [
            _confirm_row(event_type="SYNC_FAILED", sync_result="false", trade_id="A", symbol="AAAUSDT", side="LONG"),
        ],
        "succeeded_only": [
            _confirm_row(event_type="SYNC_OK", sync_result="true", trade_id="A", symbol="AAAUSDT", side="LONG"),
        ],
        "both_failed_and_succeeded_same_row": [
            _confirm_row(event_type="SYNC_FAILED_SYNC_OK", sync_result="false", done="true", trade_id="A", symbol="AAAUSDT", side="LONG"),
        ],
        "later_success_resolves_earlier_failure": [
            _confirm_row(event_type="SYNC_FAILED", sync_result="false", trade_id="A", symbol="AAAUSDT", side="LONG"),
            _confirm_row(event_type="SYNC_OK", sync_result="true", trade_id="A", symbol="AAAUSDT", side="LONG"),
        ],
        "same_row_success_does_not_resolve_itself": [
            _confirm_row(event_type="SYNC_FAILED_SYNC_OK", sync_result="false", done="true", trade_id="A", symbol="AAAUSDT", side="LONG"),
        ],
        "success_before_failure_does_not_resolve": [
            _confirm_row(event_type="SYNC_OK", sync_result="true", trade_id="A", symbol="AAAUSDT", side="LONG"),
            _confirm_row(event_type="SYNC_FAILED", sync_result="false", trade_id="A", symbol="AAAUSDT", side="LONG"),
        ],
        "duplicate_keys": [
            _confirm_row(event_type="SYNC_FAILED", sync_result="false", trade_id="A", symbol="AAAUSDT", side="LONG"),
            _confirm_row(event_type="SYNC_FAILED", sync_result="false", trade_id="A", symbol="AAAUSDT", side="LONG"),
            _confirm_row(event_type="SYNC_OK", sync_result="true", trade_id="A", symbol="AAAUSDT", side="LONG"),
        ],
        "missing_timestamps": [
            _confirm_row(event_type="SYNC_FAILED", sync_result="false", research_join_key="J1", symbol="AAAUSDT", side="LONG"),
            _confirm_row(event_type="SYNC_OK", sync_result="true", research_join_key="J1", symbol="AAAUSDT", side="LONG"),
        ],
        "no_success_case": [
            _confirm_row(event_type="SYNC_FAILED", sync_result="false", trade_id="A", symbol="AAAUSDT", side="LONG"),
            _confirm_row(event_type="SYNC_FAILED", sync_result="false", trade_id="B", symbol="BBBUSDT", side="SHORT"),
        ],
        "symbol_side_cross_match_isolation": [
            _confirm_row(event_type="SYNC_FAILED", sync_result="false", symbol="AAAUSDT", side="LONG"),
            _confirm_row(event_type="SYNC_OK", sync_result="true", symbol="AAAUSDT", side="SHORT"),
            _confirm_row(event_type="SYNC_OK", sync_result="true", symbol="BBBUSDT", side="LONG"),
        ],
        "non_confirm_success_does_not_resolve_confirm_failure": [
            _confirm_row(event_type="SYNC_FAILED", sync_result="false", trade_id="A", symbol="AAAUSDT", side="LONG"),
            {"entry_type": "OTHER", "event_type": "SYNC_OK", "sync_result": "true", "trade_id": "A", "symbol": "AAAUSDT", "side": "LONG"},
        ],
        "malformed_like_rows_ignored_by_identity": [
            {},
            {"entry_type": "CONFIRM_SMC_RESEARCH", "event_type": "SYNC_FAILED", "sync_result": "false"},
            _confirm_row(event_type="SYNC_OK", sync_result="true", trade_id="A", symbol="AAAUSDT", side="LONG"),
        ],
        "both_row_resolves_earlier_but_not_self": [
            _confirm_row(event_type="SYNC_FAILED", sync_result="false", trade_id="A", symbol="AAAUSDT", side="LONG"),
            _confirm_row(event_type="SYNC_FAILED_SYNC_OK", sync_result="false", done="true", trade_id="A", symbol="AAAUSDT", side="LONG"),
        ],
    }
    for name, rows in cases.items():
        _assert_min_lock_case(name, rows)

    with tempfile.TemporaryDirectory(prefix="rh_min_lock_malformed_") as tmpdir:
        with _PatchPaths(tmpdir) as paths:
            paths["LIVE_MIN_LOCK"].parent.mkdir(parents=True, exist_ok=True)
            paths["LIVE_MIN_LOCK"].write_text(
                "\n".join([
                    "{bad-json",
                    json.dumps(_confirm_row(event_type="SYNC_FAILED", sync_result="false", trade_id="A")),
                    json.dumps({"entry_type": "OTHER", "event_type": "SYNC_OK", "sync_result": "true", "trade_id": "A"}),
                    "",
                ]),
                encoding="utf-8",
            )
            streamed = rh.min_lock_rows_stream()
            if len(streamed) != 1 or streamed[0].get("trade_id") != "A":
                raise SystemExit("FAIL malformed min-lock rows were not skipped safely")

    print("PASS min-lock semantic mismatches=0")


def run_current_once():
    row = rh.build_summary(source="stream_current_probe", write_summary=False)
    print(json.dumps(_selected(row), indent=2, sort_keys=True, default=str))


def run_benchmark(iterations):
    baseline = _rss_kb()
    peak = baseline
    started = time.time()
    rows = []
    for idx in range(iterations):
        t0 = time.time()
        row = rh.build_summary(source="stream_memory_benchmark", write_summary=False)
        rss = _rss_kb()
        peak = max(peak, rss)
        rows.append({
            "i": idx + 1,
            "elapsed_sec": round(time.time() - t0, 3),
            "rss_kb": rss,
            "live_closed_n": row.get("live_metrics", {}).get("live_closed_n"),
            "live_rolling_net_r": row.get("live_metrics", {}).get("live_rolling_net_r"),
            "live_loss_streak": row.get("live_metrics", {}).get("live_loss_streak"),
        })
    final = _rss_kb()
    result = {
        "iterations": iterations,
        "total_elapsed_sec": round(time.time() - started, 3),
        "baseline_rss_kb": baseline,
        "peak_rss_kb": peak,
        "final_rss_kb": final,
        "maxrss_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "incremental_peak_kb": peak - baseline,
        "rows": rows,
    }
    print(json.dumps(result, indent=2, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--parity", action="store_true")
    parser.add_argument("--current-once", action="store_true")
    parser.add_argument("--benchmark", type=int, default=0)
    args = parser.parse_args()
    if args.parity:
        run_parity()
    if args.current_once:
        run_current_once()
    if args.benchmark:
        run_benchmark(args.benchmark)
    if not (args.parity or args.current_once or args.benchmark):
        run_parity()


if __name__ == "__main__":
    main()
