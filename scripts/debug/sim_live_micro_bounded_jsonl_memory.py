#!/usr/bin/env python3
import gc
import json
import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import log_rotation
import signal_dispatcher as sd


def rss_kb():
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except Exception:
        return 0
    return 0


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def old_full_jsonl(path):
    rows = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def assert_equal(name, actual, expected):
    if actual != expected:
        raise AssertionError(f"{name}: expected {expected!r}, got {actual!r}")
    print(f"PASS {name}")


def assert_true(name, value):
    if not value:
        raise AssertionError(name)
    print(f"PASS {name}")


def representative_close_row(idx, realized):
    return {
        "id": f"close-{idx}",
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "decision": "CLOSE",
        "actual_realized_r": realized,
        "ts": float(idx),
    }


def run_tail_equivalence(tmp):
    path = tmp / "representative.jsonl"
    rows = [{"id": f"noise-{i}", "decision": "OPEN", "ts": i} for i in range(30)]
    rows.extend(representative_close_row(i, (-1.0 if i % 3 else 1.5)) for i in range(30, 75))
    write_jsonl(path, rows)

    expected = old_full_jsonl(path)[-40:]
    actual = sd._live_research_micro_read_jsonl(str(path), max_rows=40)
    assert_equal("bounded_reader_matches_old_tail_subset", actual, expected)

    full_metrics = sd._live_research_micro_metrics(decision_rows=old_full_jsonl(path)[-40:], live_trade_rows=[])
    bounded_metrics = sd._live_research_micro_metrics(decision_rows=actual, live_trade_rows=[])
    assert_equal("rolling_metrics_unchanged_for_same_relevant_window", bounded_metrics, full_metrics)


def run_malformed_partial_test(tmp):
    path = tmp / "malformed.jsonl"
    with open(path, "wb") as handle:
        handle.write(b'{"id": 1, "ok": true}\n')
        handle.write(b'{"broken": \n')
        handle.write(b"\xff\xfe\xfa\n")
        handle.write(b'{"id": 2, "ok": true}\n')
        handle.write(b'{"partial": ')
    rows = sd._live_research_micro_read_jsonl(str(path), max_rows=20)
    assert_equal("malformed_partial_utf8_rows_skipped", [row.get("id") for row in rows], [1, 2])


def run_pause_status_read_once_test(tmp):
    now = time.time()
    decisions = tmp / "decisions.jsonl"
    pauses = tmp / "pause.jsonl"
    health = tmp / "health.jsonl"
    csv_path = tmp / "live_trades.csv"
    write_jsonl(decisions, [representative_close_row(i, 0.25) for i in range(1, 25)])
    write_jsonl(pauses, [
        {
            "event_type": "LIVE_RESEARCH_MICRO_PAUSE",
            "pause_reason": "OLD",
            "pause_until": now - 3600,
            "ts": now - 7200,
        }
    ])
    write_jsonl(health, [
        {
            "ts": now,
            "paper_active_health": "GREEN",
            "live_health": "GREEN",
            "live_metrics": {},
            "reasons": [],
        }
    ])
    csv_path.write_text(
        "entry_type,status,actual_realized_r,close_ts,id\n",
        encoding="utf-8",
    )

    originals = {
        "decision_log": sd._LIVE_SMC_RESEARCH_DECISION_LOG,
        "pause_log": sd._LIVE_RESEARCH_MICRO_PAUSE_LOG,
        "health_log": sd._RESEARCH_ROLLING_HEALTH_LOG,
        "csv": sd._LIVE_TRADES_CSV,
        "reader": sd._live_research_micro_read_jsonl,
        "writer": sd._live_research_micro_write,
    }
    counts = {}

    def counting_reader(path, max_rows=None):
        counts[path] = counts.get(path, 0) + 1
        return originals["reader"](path, max_rows=max_rows)

    try:
        sd._LIVE_SMC_RESEARCH_DECISION_LOG = str(decisions)
        sd._LIVE_RESEARCH_MICRO_PAUSE_LOG = str(pauses)
        sd._RESEARCH_ROLLING_HEALTH_LOG = str(health)
        sd._LIVE_TRADES_CSV = str(csv_path)
        sd._live_research_micro_read_jsonl = counting_reader
        sd._live_research_micro_write = lambda row: None

        ok, reason, payload = sd._live_research_micro_pause_status(ctx=None, now_ts=now)
        assert_equal("pause_status_allows_green_representative_case", (ok, reason), (True, ""))
        assert_equal("decision_log_read_once", counts.get(str(decisions), 0), 1)
        assert_equal("pause_log_read_once", counts.get(str(pauses), 0), 1)
        assert_equal("health_log_read_once", counts.get(str(health), 0), 1)
        assert_equal("live_closed_count_preserved_in_payload", payload.get("live_closed_count"), 24)
    finally:
        sd._LIVE_SMC_RESEARCH_DECISION_LOG = originals["decision_log"]
        sd._LIVE_RESEARCH_MICRO_PAUSE_LOG = originals["pause_log"]
        sd._RESEARCH_ROLLING_HEALTH_LOG = originals["health_log"]
        sd._LIVE_TRADES_CSV = originals["csv"]
        sd._live_research_micro_read_jsonl = originals["reader"]
        sd._live_research_micro_write = originals["writer"]


def run_200_pause_status_rss_test(tmp):
    now = time.time()
    decisions = tmp / "decisions_200.jsonl"
    pauses = tmp / "pause_200.jsonl"
    health = tmp / "health_200.jsonl"
    csv_path = tmp / "live_trades_200.csv"
    write_jsonl(decisions, [representative_close_row(i, 0.1) for i in range(1, 60)])
    write_jsonl(pauses, [])
    write_jsonl(health, [{"ts": now, "paper_health": "GREEN", "live_health": "GREEN", "live_metrics": {}, "reasons": []}])
    csv_path.write_text("entry_type,status,actual_realized_r,close_ts,id\n", encoding="utf-8")

    originals = {
        "decision_log": sd._LIVE_SMC_RESEARCH_DECISION_LOG,
        "pause_log": sd._LIVE_RESEARCH_MICRO_PAUSE_LOG,
        "health_log": sd._RESEARCH_ROLLING_HEALTH_LOG,
        "csv": sd._LIVE_TRADES_CSV,
        "writer": sd._live_research_micro_write,
    }
    try:
        sd._LIVE_SMC_RESEARCH_DECISION_LOG = str(decisions)
        sd._LIVE_RESEARCH_MICRO_PAUSE_LOG = str(pauses)
        sd._RESEARCH_ROLLING_HEALTH_LOG = str(health)
        sd._LIVE_TRADES_CSV = str(csv_path)
        sd._live_research_micro_write = lambda row: None
        before = rss_kb()
        peak = before
        for _ in range(200):
            ok, reason, _payload = sd._live_research_micro_pause_status(ctx=None, now_ts=now)
            if not ok or reason:
                raise AssertionError(f"unexpected pause status: {ok=} {reason=}")
            peak = max(peak, rss_kb())
        gc.collect()
        after = rss_kb()
        print(f"RSS_200_PAUSE_STATUS before_kb={before} peak_kb={peak} after_kb={after} delta_after_kb={after - before}")
        assert_true("rss_not_growing_across_200_pause_status_evaluations", after - before < 8192)
    finally:
        sd._LIVE_SMC_RESEARCH_DECISION_LOG = originals["decision_log"]
        sd._LIVE_RESEARCH_MICRO_PAUSE_LOG = originals["pause_log"]
        sd._RESEARCH_ROLLING_HEALTH_LOG = originals["health_log"]
        sd._LIVE_TRADES_CSV = originals["csv"]
        sd._live_research_micro_write = originals["writer"]


def run_current_log_bounded_rss_test():
    path = ROOT / "logs" / "live_smc_research_decisions.jsonl"
    if not path.exists():
        print("WARN current_decisions_log_missing")
        return
    before = rss_kb()
    peak = before
    counts = []
    for _ in range(8):
        rows = sd._live_research_micro_read_jsonl(
            str(path),
            max_rows=sd._LIVE_RESEARCH_MICRO_DECISION_TAIL_ROWS,
        )
        counts.append(len(rows))
        peak = max(peak, rss_kb())
        del rows
        gc.collect()
    after = rss_kb()
    print(
        "RSS_CURRENT_DECISIONS_BOUNDED "
        f"file_bytes={path.stat().st_size} rows_each={counts} "
        f"before_kb={before} peak_kb={peak} after_kb={after} "
        f"delta_peak_kb={peak - before} delta_after_kb={after - before}"
    )
    assert_true("current_416mb_log_repeated_bounded_reads_stable", after - before < 65536)


def run_rotation_target_test():
    runtime_read = {
        "live_smc_research_decisions.jsonl",
        "live_research_micro_pause.jsonl",
        "research_rolling_health.jsonl",
        "paper_smc_research_qualified_decisions.jsonl",
    }
    observational = {
        "paper_smc_main_gate_shadow.jsonl",
        "structural_context_samples.jsonl",
    }
    retained_runtime = sorted(runtime_read & set(log_rotation.ROTATION_TARGETS))
    missing_observational = sorted(observational - set(log_rotation.ROTATION_TARGETS))
    assert_equal("rotation_targets_exclude_runtime_read_jsonl_logs", retained_runtime, [])
    assert_equal("rotation_targets_include_observational_jsonl_logs", missing_observational, [])
    assert_equal("rotation_threshold_mb_unchanged", log_rotation.DEFAULT_MAX_SIZE_MB, 50)


def main():
    with tempfile.TemporaryDirectory(prefix="live_micro_bounded_") as tmp_name:
        tmp = Path(tmp_name)
        run_tail_equivalence(tmp)
        run_malformed_partial_test(tmp)
        run_pause_status_read_once_test(tmp)
        run_200_pause_status_rss_test(tmp)
    run_current_log_bounded_rss_test()
    run_rotation_target_test()
    print("PASS sim_live_micro_bounded_jsonl_memory")


if __name__ == "__main__":
    main()
