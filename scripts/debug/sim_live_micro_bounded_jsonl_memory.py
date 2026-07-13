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


def run_close_ordering_resolver_test():
    from scripts.debug import audit_research_rolling_health as rh

    csv_rows = [
        {
            "id": "a",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "status": "WIN",
            "actual_realized_r": "1.0",
            "signal_created_ts": "300",
            "close_ts": "1000",
        },
        {
            "id": "b",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "status": "LOSE",
            "actual_realized_r": "-0.5",
            "signal_created_ts": "100",
            "closed_at_unix": "1010",
        },
        {
            "id": "c",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "status": "WIN",
            "actual_realized_r": "0.2",
            "signal_created_ts": "50",
            "close_time": "00:30 01-07",
        },
        {
            "id": "d",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "status": "LOSE",
            "actual_realized_r": "-1.0",
            "signal_created_ts": "10",
        },
    ]
    decision_rows = [
        {
            "id": "d",
            "entry_type": "CONFIRM_SMC_RESEARCH",
            "decision": "CLOSE",
            "actual_realized_r": "-1.0",
            "ts": "1020",
        }
    ]
    rows = sd._live_research_close_rows(decision_rows=decision_rows, live_trade_rows=csv_rows)
    assert_equal(
        "dispatcher_close_order_sources",
        [row.get("_sort_ts_source") for row in rows],
        ["close_ts", "closed_at_unix", "decision_log", "close_time"],
    )
    wrong = sorted(csv_rows, key=lambda row: float(row.get("signal_created_ts") or 0))
    assert_equal("creation_time_order_differs_from_close_order", [r["id"] for r in wrong], ["d", "c", "b", "a"])
    assert_equal("close_time_order_uses_terminal_evidence", [r["id"] for r in rows], ["a", "b", "d", "c"])

    fixed_metrics = sd._live_research_micro_metrics(close_rows=rows)
    wrong_metrics = sd._live_research_micro_metrics(close_rows=[
        {**row, "actual_realized_r": float(row["actual_realized_r"])}
        for row in wrong
    ])
    assert_equal("latest_loss_streak_uses_latest_closes", fixed_metrics["live_loss_streak"], 0)
    assert_true(
        "rolling_net_changes_when_creation_order_was_wrong",
        fixed_metrics["live_rolling_net_r"] != wrong_metrics["live_rolling_net_r"]
        or [r["id"] for r in rows] != [r["id"] for r in wrong],
    )

    audit_rows = rh.live_close_rows(decision_rows=decision_rows, live_trade_rows=csv_rows)
    assert_equal("audit_resolver_matches_dispatcher_order", [r["id"] for r in audit_rows], ["a", "b", "d", "c"])


def run_current_live_close_metrics_test():
    from scripts.debug import audit_research_rolling_health as rh

    rows = rh.live_csv_close_rows()
    values = [rh.fnum(row.get("actual_realized_r")) for row in rows]
    values = [value for value in values if value is not None]
    if len(values) != 51:
        print(f"WARN current_live_closed_count_expected_51 got={len(values)}")
        return
    loss_streak = 0
    for value in reversed(values):
        if value < 0:
            loss_streak += 1
        else:
            break
    old_rows = []
    for row in rh.read_csv(rh.LIVE_TRADES):
        if str(row.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        realized = rh.fnum(row.get("actual_realized_r", row.get("realized_r", row.get("rr_real", row.get("rr")))))
        if realized is None:
            continue
        item = dict(row)
        item["actual_realized_r"] = realized
        item["_old_sort"] = rh.fnum(row.get("signal_created_ts"), 0.0) or 0.0
        old_rows.append(item)
    old_rows.sort(key=lambda row: row["_old_sort"])
    old_values = [row["actual_realized_r"] for row in old_rows]
    assert_equal("current_live_closed_count", len(values), 51)
    assert_equal("current_close_ordered_rolling20_exact", round(sum(values[-20:]), 4), -0.34)
    assert_equal("current_creation_ordered_rolling20_old_bug", round(sum(old_values[-20:]), 4), -2.05)
    assert_equal("current_latest_loss_streak_close_ordered", loss_streak, 2)
    assert_equal(
        "current_close_ts_resolution_coverage",
        sum(1 for row in rows if row.get("_sort_ts_source") == "legacy_signal_created_ts"),
        0,
    )


def _health_row(now, paper="RED", live="GREEN", rolling=-0.34, current_streak=False):
    return {
        "ts": now,
        "paper_health": paper,
        "paper_active_health": paper,
        "live_health": live,
        "live_metrics": {
            "live_rolling_net_r": rolling,
            "loss_streak_current": current_streak,
            "loss_streak_stale_after_new_open": False,
        },
        "reasons": [],
    }


def run_bounded_canary_posture_test():
    now = time.time()
    old_config = dict(sd.config)
    old_write = sd._live_research_micro_write
    old_status = sd.canary_config_status
    old_preflight = sd.canary_preflight_open
    sd._live_research_micro_write = lambda row: None
    sd.config["live_research_micro_pause_enabled"] = True
    sd.config["live_paper_red_scale_mode"] = "WARN_ONLY_WHEN_LIVE_HEALTH_OK"
    sd.config["max_live_research_trades"] = 2
    sd.config["live_risk_per_trade"] = old_config.get("live_risk_per_trade", 0.003)
    sd.config["live_max_portfolio_risk"] = old_config.get("live_max_portfolio_risk", 0.009)

    class Ctx:
        trades = []

    valid_status = {
        "enabled": True,
        "epoch": "epoch-a",
        "candidate_id": "INCUMBENT_LIVE_CONFIRM",
        "max_open": 2,
        "max_total": 50,
        "max_cum_loss_r": -8.0,
        "max_consecutive_losses": 5,
        "errors": [],
    }
    try:
        sd.canary_config_status = lambda: dict(valid_status)
        sd.canary_preflight_open = lambda trade, open_trades=None: {
            "ok": True,
            "enabled": True,
            "reason": "",
            "status": dict(valid_status),
            "state": {"abort_latched": False},
        }
        ok, reason, payload = sd._live_research_micro_pause_status(
            ctx=Ctx(),
            now_ts=now,
            close_rows=[],
            pause_rows=[],
            health_rows=[_health_row(now)],
        )
        assert_equal("bounded_canary_clears_rolling_positive_bootstrap", (ok, reason), (True, ""))
        assert_equal("bounded_canary_warn_allow_action", payload.get("action"), "WARN_ALLOW_SCALE")
        assert_true("bounded_canary_posture_recorded", payload.get("bounded_canary_posture", {}).get("bounded_canary_valid"))

        bad_status = dict(valid_status)
        bad_status["max_open"] = 1
        sd.canary_config_status = lambda: dict(bad_status)
        ok_bad, reason_bad, _payload_bad = sd._live_research_micro_pause_status(
            ctx=Ctx(),
            now_ts=now,
            close_rows=[],
            pause_rows=[],
            health_rows=[_health_row(now)],
        )
        assert_equal("bounded_canary_invalid_max_open_fails_closed", (ok_bad, reason_bad), (False, "LIVE_SCALE_BLOCKED_PAPER_HEALTH"))

        sd.canary_config_status = lambda: dict(valid_status)
        sd.canary_preflight_open = lambda trade, open_trades=None: {
            "ok": False,
            "enabled": True,
            "reason": "canary_abort_latched:test",
        }
        ok_latch, reason_latch, _payload_latch = sd._live_research_micro_pause_status(
            ctx=Ctx(),
            now_ts=now,
            close_rows=[],
            pause_rows=[],
            health_rows=[_health_row(now)],
        )
        assert_equal("bounded_canary_latch_still_blocks", (ok_latch, reason_latch), (False, "LIVE_SCALE_BLOCKED_PAPER_HEALTH"))

        sd.canary_preflight_open = lambda trade, open_trades=None: {
            "ok": True,
            "enabled": True,
            "reason": "",
            "status": dict(valid_status),
            "state": {"abort_latched": False},
        }
        ok_current, reason_current, _payload_current = sd._live_research_micro_pause_status(
            ctx=Ctx(),
            now_ts=now,
            close_rows=[],
            pause_rows=[],
            health_rows=[_health_row(now, current_streak=True)],
        )
        assert_equal("bounded_canary_current_loss_streak_blocks", (ok_current, reason_current), (False, "LIVE_SCALE_BLOCKED_PAPER_HEALTH"))
    finally:
        sd.config.clear()
        sd.config.update(old_config)
        sd._live_research_micro_write = old_write
        sd.canary_config_status = old_status
        sd.canary_preflight_open = old_preflight


def main():
    with tempfile.TemporaryDirectory(prefix="live_micro_bounded_") as tmp_name:
        tmp = Path(tmp_name)
        run_tail_equivalence(tmp)
        run_malformed_partial_test(tmp)
        run_pause_status_read_once_test(tmp)
        run_200_pause_status_rss_test(tmp)
        run_close_ordering_resolver_test()
        run_bounded_canary_posture_test()
    run_current_log_bounded_rss_test()
    run_rotation_target_test()
    run_current_live_close_metrics_test()
    print("PASS sim_live_micro_bounded_jsonl_memory")


if __name__ == "__main__":
    main()
