#!/usr/bin/env python3
"""Rolling health audit for CONFIRM_SMC_RESEARCH paper and live research."""

import json
import math
import time
import csv
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"
PAPER_LIFECYCLE = LOG_DIR / "paper_smc_research_lifecycle.jsonl"
PAPER_GAP_SHADOW = LOG_DIR / "paper_smc_research_sl_gap_calibration_shadow.jsonl"
PAPER_MIN_LOCK = LOG_DIR / "paper_smc_research_min_lock_shadow.jsonl"
LIVE_DECISIONS = LOG_DIR / "live_smc_research_decisions.jsonl"
LIVE_MIN_LOCK = LOG_DIR / "live_smc_research_min_lock_075_events.jsonl"
LIVE_STATE = ROOT / "live_state.json"
LIVE_TRADES = ROOT / "live_trades.csv"
SUMMARY_LOG = LOG_DIR / "research_rolling_health.jsonl"
CONFIG_JSON = ROOT / "config.json"


def read_jsonl(path):
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def read_json(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def read_csv(path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_jsonl(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str) + "\n")


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


def fmt(value):
    if value is None:
        return "n/a"
    if value == float("inf"):
        return "inf"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def row_key(row):
    for field in ("research_join_key", "research_dedup_key", "dedup_key"):
        value = str(row.get(field) or "").strip()
        if value:
            return value
    if row.get("trade_id") not in (None, ""):
        return str(row.get("trade_id"))
    parts = [row.get("symbol"), row.get("side"), row.get("source_timestamp"), row.get("signal_ts")]
    return "|".join(str(part or "") for part in parts)


def row_ts(row):
    for field in ("close_ts", "closed_at_unix", "ts", "observed_at_unix", "time", "entry_time"):
        value = fnum(row.get(field))
        if value is not None:
            return value
    return 0.0


def pf(values):
    gains = sum(v for v in values if v > 0)
    losses = -sum(v for v in values if v < 0)
    if losses <= 0:
        return None if gains <= 0 else float("inf")
    return gains / losses


def win_rate(values):
    return None if not values else sum(1 for v in values if v > 0) / len(values)


def max_loss_streak(values):
    best = 0
    cur = 0
    for value in values:
        if value < 0:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def max_drawdown(values):
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return abs(worst)


def compact_metrics(rows):
    values = [fnum(row.get("calibrated_r_cap_1_0")) for row in rows]
    values = [v for v in values if v is not None]
    return {
        "n": len(values),
        "net_r": round(sum(values), 4),
        "pf": pf(values),
        "win_rate": win_rate(values),
    }


def split_metrics(rows, field):
    buckets = defaultdict(list)
    for row in rows:
        raw = row.get(field)
        if raw in (None, "") and field in ("phase", "market_regime"):
            raw = (row.get("entry_context") or {}).get(field)
        label = str(raw or "UNKNOWN").upper()
        buckets[label].append(row)
    return {
        label: compact_metrics(bucket)
        for label, bucket in sorted(buckets.items(), key=lambda item: (-len(item[1]), item[0]))
    }


def metrics(rows):
    rows = [row for row in rows if fnum(row.get("raw_realized_r")) is not None]
    raw = [fnum(row.get("raw_realized_r")) for row in rows]
    cap_1_0 = [fnum(row.get("calibrated_r_cap_1_0")) for row in rows]
    cap_1_2 = [fnum(row.get("calibrated_r_cap_1_2")) for row in rows]
    raw = [value for value in raw if value is not None]
    cap_1_0 = [value for value in cap_1_0 if value is not None]
    cap_1_2 = [value for value in cap_1_2 if value is not None]
    return {
        "n": len(rows),
        "raw_net_r": round(sum(raw), 4),
        "raw_pf": pf(raw),
        "calibrated_net_r_cap_1_0": round(sum(cap_1_0), 4),
        "calibrated_pf_cap_1_0": pf(cap_1_0),
        "calibrated_net_r_cap_1_2": round(sum(cap_1_2), 4),
        "calibrated_pf_cap_1_2": pf(cap_1_2),
        "win_rate": win_rate(raw),
        "max_loss_streak": max_loss_streak(cap_1_0),
        "max_drawdown_r": round(max_drawdown(cap_1_0), 4),
        "gap_loss_count": sum(1 for row in rows if row.get("is_gap_loss")),
        "possible_overcharge_count": sum(1 for row in rows if row.get("possible_overcharge")),
        "side_split": split_metrics(rows, "side"),
        "phase_split": split_metrics(rows, "phase"),
        "regime_split": split_metrics(rows, "market_regime"),
    }


def paper_close_rows():
    seen = set()
    out = []
    for row in read_jsonl(PAPER_LIFECYCLE):
        candidates = []
        if row.get("event_type") in ("RESEARCH_CLOSED", "RESEARCH_CLOSE_MISSING_CONTEXT"):
            candidates.append(row)
        for child in row.get("closed_since_last_summary") or []:
            if isinstance(child, dict) and child.get("event_type") in (
                "RESEARCH_CLOSED",
                "RESEARCH_CLOSE_MISSING_CONTEXT",
            ):
                candidates.append(child)
        for candidate in candidates:
            if str(candidate.get("entry_type") or "").upper() not in ("", "CONFIRM_SMC_RESEARCH"):
                continue
            ident = row_key(candidate)
            if ident in seen:
                continue
            seen.add(ident)
            out.append(dict(candidate))
    out.sort(key=row_ts)
    return out


def shadow_index(path):
    indexed = {}
    for row in read_jsonl(path):
        indexed[row_key(row)] = row
        if row.get("trade_id") not in (None, ""):
            indexed[str(row.get("trade_id"))] = row
    return indexed


def enrich_paper_rows(rows, gap_shadow=None, min_lock_shadow=None):
    gap_shadow = gap_shadow if gap_shadow is not None else shadow_index(PAPER_GAP_SHADOW)
    min_lock_shadow = min_lock_shadow if min_lock_shadow is not None else shadow_index(PAPER_MIN_LOCK)
    min_lock_keys = {
        row_key(row)
        for row in min_lock_shadow.values()
        if isinstance(row, dict) and row_key(row)
    }
    out = []
    for source in rows:
        row = dict(source)
        joined = gap_shadow.get(row_key(row)) or gap_shadow.get(str(row.get("trade_id") or ""))
        if joined:
            row["_gap_shadow_joined"] = True
            for field in (
                "possible_overcharge",
                "configured_sl_gap_r",
                "execution_tier",
                "gap_minus_mae_r",
                "price_r",
                "expected_sl_r_with_gap",
            ):
                if field in joined and row.get(field) in (None, ""):
                    row[field] = joined[field]
        else:
            row["_gap_shadow_joined"] = False

        raw_r = fnum(row.get("raw_realized_r", row.get("r_multiple")))
        row["raw_realized_r"] = raw_r
        close_reason = str(row.get("close_reason") or row.get("exit_type") or "").upper()
        gap_r = fnum(row.get("configured_sl_gap_r", row.get("raw_sl_gap_r")))
        is_gap_loss = bool(row.get("is_gap_loss"))
        if not is_gap_loss:
            is_gap_loss = (
                close_reason == "SL"
                and gap_r is not None
                and gap_r > 0
                and raw_r is not None
                and raw_r < -1.0
            )
        if not is_gap_loss and close_reason == "SL" and raw_r is not None:
            is_gap_loss = abs(raw_r + 1.5) < 1e-9 or abs(raw_r + 1.3) < 1e-9
        row["is_gap_loss"] = is_gap_loss
        row["configured_sl_gap_r"] = gap_r
        row["calibrated_r_cap_1_0"] = (
            max(raw_r, -1.0)
            if is_gap_loss and raw_r is not None and raw_r <= -1.2
            else raw_r
        )
        row["calibrated_r_cap_1_2"] = (
            max(raw_r, -1.2)
            if is_gap_loss and raw_r is not None and raw_r <= -1.2
            else raw_r
        )
        entry_context = row.get("entry_context") if isinstance(row.get("entry_context"), dict) else {}
        row["phase"] = row.get("phase") or entry_context.get("phase")
        row["market_regime"] = row.get("market_regime") or entry_context.get("market_regime")
        row["since_min_lock_active"] = row_key(row) in min_lock_keys
        out.append(row)
    out.sort(key=row_ts)
    return out


def classify_paper(last20, last50):
    reasons = []
    if last50["n"] < 50:
        reasons.append(f"paper_last50_insufficient_n={last50['n']}")
    if last20["n"] < 20:
        reasons.append(f"paper_last20_insufficient_n={last20['n']}")

    last50_net = last50["calibrated_net_r_cap_1_0"]
    last50_pf = last50["calibrated_pf_cap_1_0"]
    last20_net = last20["calibrated_net_r_cap_1_0"]
    loss_streak = last50["max_loss_streak"]

    if last50["n"] == 0 or last20["n"] == 0:
        return "RED", reasons + ["paper_no_closed_data"]
    if last50_net <= 0:
        return "RED", reasons + [f"last50_calibrated_net_r_cap_1_0={last50_net}<=0"]
    if last50_pf is None or last50_pf < 1.0:
        return "RED", reasons + [f"last50_calibrated_pf_cap_1_0={fmt(last50_pf)}<1.00"]
    if last20_net <= -4.0:
        return "RED", reasons + [f"last20_calibrated_net_r_cap_1_0={last20_net}<=-4R"]
    if loss_streak >= 5:
        return "RED", reasons + [f"last50_max_loss_streak={loss_streak}>=5"]

    if last50_net > 0 and (last50_pf is None or last50_pf < 1.10):
        return "YELLOW", reasons + [f"last50_positive_but_pf={fmt(last50_pf)}<1.10"]
    if -4.0 < last20_net <= -2.0:
        return "YELLOW", reasons + [f"last20_calibrated_net_r_cap_1_0={last20_net}_between_-2R_and_-4R"]

    if last50_net > 0 and last50_pf >= 1.10 and last20_net > -2.0:
        return "GREEN", reasons + ["paper_last50_positive_pf_ge_1_10_last20_gt_-2R"]
    return "YELLOW", reasons + ["paper_health_default_yellow"]


def classify_active_paper(active_rows, min_active_closed):
    last20 = metrics(active_rows[-20:])
    last50 = metrics(active_rows[-50:])
    if len(active_rows) < min_active_closed:
        return (
            "INSUFFICIENT_DATA",
            [
                f"active_closed_n={len(active_rows)}<{min_active_closed}",
                "legacy_health_warning_only",
            ],
            last20,
            last50,
        )
    health, reasons = classify_paper(last20, last50)
    return health, reasons, last20, last50


def live_close_dedup_key(row):
    stable = row.get("id") or row.get("trade_id") or row.get("client_order_id") or row.get("clientOrderId")
    if stable not in (None, ""):
        return f"id:{stable}"
    return "|".join(
        str(row.get(field) or "")
        for field in ("symbol", "entry_time", "open_time", "close_time", "side")
    )


def live_csv_close_rows(live_trade_rows=None):
    rows = live_trade_rows if live_trade_rows is not None else read_csv(LIVE_TRADES)
    out = []
    for row in rows:
        if str(row.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        status = str(row.get("status") or "").upper()
        if status not in ("WIN", "LOSS", "CLOSED", "BREAKEVEN") and not row.get("close_time"):
            continue
        realized = fnum(row.get("actual_realized_r", row.get("realized_r", row.get("rr_real", row.get("rr")))))
        if realized is None:
            continue
        item = dict(row)
        item["actual_realized_r"] = realized
        item["_live_close_source"] = "live_trades_csv"
        item["_sort_ts"] = fnum(row.get("close_ts", row.get("closed_at_unix", row.get("signal_created_ts"))), 0.0) or 0.0
        out.append(item)
    out.sort(key=lambda item: item.get("_sort_ts", 0.0))
    return out


def live_decision_close_rows(decision_rows=None):
    rows = decision_rows if decision_rows is not None else read_jsonl(LIVE_DECISIONS)
    out = []
    for row in rows:
        if str(row.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        decision = str(row.get("decision") or "").upper()
        status = str(row.get("status") or "").upper()
        realized = fnum(
            row.get(
                "actual_realized_r",
                row.get("realized_r", row.get("rr_real", row.get("r_multiple"))),
            )
        )
        if realized is None:
            continue
        if "CLOSE" in decision or "CLOSED" in decision or status == "CLOSED" or row.get("close_reason"):
            item = dict(row)
            item["actual_realized_r"] = realized
            item["_live_close_source"] = "live_smc_research_decisions"
            item["_sort_ts"] = row_ts(row)
            out.append(item)
    out.sort(key=lambda item: item.get("_sort_ts", 0.0))
    return out


def live_close_rows(decision_rows=None, live_trade_rows=None):
    decision_closes = live_decision_close_rows(decision_rows)
    csv_closes = live_csv_close_rows(live_trade_rows)
    merged = {}
    for row in decision_closes + csv_closes:
        key = live_close_dedup_key(row)
        if key not in merged:
            merged[key] = row
            continue
        if merged[key].get("_live_close_source") != "live_trades_csv" and row.get("_live_close_source") == "live_trades_csv":
            merged[key] = row
    out = list(merged.values())
    out.sort(key=lambda item: item.get("_sort_ts", row_ts(item)))
    return out


def live_research_open_positions(state=None):
    if state is None:
        state = read_json(LIVE_STATE)
    trades = state.get("trades") if isinstance(state, dict) else state
    if not isinstance(trades, list):
        return []
    out = []
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        if str(trade.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        if str(trade.get("status") or "OPEN").upper() != "OPEN":
            continue
        out.append(trade)
    return out


def live_state_has_confirmed_research_sl(state=None):
    for trade in live_research_open_positions(state):
        if trade.get("exchange_sl_id") not in (None, "") and fnum(trade.get("exchange_sl_price_confirmed")) is not None:
            return True
    return False


def sl_sync_identity(row):
    keys = []
    for field in ("trade_id", "research_join_key", "symbol", "side"):
        value = str(row.get(field) or "").strip().upper()
        if value:
            keys.append((field, value))
    return tuple(keys)


def sl_sync_rows_match(left, right):
    left_keys = dict(sl_sync_identity(left))
    right_keys = dict(sl_sync_identity(right))
    for field in ("trade_id", "research_join_key"):
        if left_keys.get(field) and left_keys.get(field) == right_keys.get(field):
            return True
    return (
        left_keys.get("symbol")
        and left_keys.get("side")
        and left_keys.get("symbol") == right_keys.get("symbol")
        and left_keys.get("side") == right_keys.get("side")
    )


def min_lock_row_failed(row):
    reason = str(row.get("reason") or row.get("event_type") or "")
    sync_result = str(row.get("sync_result") or "")
    return "FAILED" in reason.upper() or sync_result.lower() == "false"


def min_lock_row_succeeded(row):
    reason = str(row.get("reason") or row.get("event_type") or "")
    sync_result = str(row.get("sync_result") or "")
    done = str(row.get("done", row.get("min_lock_075_done", "")) or "")
    return (
        "SYNC_OK" in reason.upper()
        or sync_result.lower() == "true"
        or done.lower() == "true"
    )


def min_lock_failure_resolved(failed_row, min_lock_rows, failed_idx):
    for later in min_lock_rows[failed_idx + 1:]:
        if str(later.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        if sl_sync_rows_match(failed_row, later) and min_lock_row_succeeded(later):
            return True
    return False


def open_trade_has_unresolved_sl_sync_risk(trade, min_lock_rows):
    if trade.get("exchange_sl_id") in (None, ""):
        return True
    if fnum(trade.get("exchange_sl_price_confirmed")) is None:
        return True
    fail_count = int(fnum(trade.get("sl_sync_fail_count"), 0) or 0)
    has_error = trade.get("exchange_sl_sync_error") not in (None, "")
    if not fail_count and not has_error:
        return False
    for row in reversed(min_lock_rows):
        if str(row.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        if sl_sync_rows_match(trade, row) and min_lock_row_succeeded(row):
            return False
    return True


def decision_sl_sync_text(row):
    text = " ".join(str(row.get(field) or "") for field in ("decision", "reason", "prefilter_reason", "prefilter_detail", "fail_reason"))
    lower = text.lower()
    if not ("sl sync" in lower or "sl_sync" in lower or "sl missing" in lower or "missing sl" in lower):
        return ""
    if (
        "live_micro_blocked_sl_sync" in lower
        or "live_micro_blocked_sl_sync".upper() in text
    ) and not any(token in lower for token in ("missing exchange", "exchange sl not confirmed", "sl missing", "missing sl")):
        return ""
    return lower


def live_safety_issues(decision_rows=None, min_lock_rows=None, live_state=None):
    decision_rows = decision_rows if decision_rows is not None else read_jsonl(LIVE_DECISIONS)
    min_lock_rows = min_lock_rows if min_lock_rows is not None else read_jsonl(LIVE_MIN_LOCK)
    warnings = []
    critical = []
    open_positions = live_research_open_positions(live_state)
    has_open_research = bool(open_positions)
    sl_sync_ok = any(
        trade.get("exchange_sl_id") not in (None, "")
        and fnum(trade.get("exchange_sl_price_confirmed")) is not None
        for trade in open_positions
    )
    if has_open_research:
        for trade in open_positions:
            if open_trade_has_unresolved_sl_sync_risk(trade, min_lock_rows):
                critical.append("live_sl_sync_failure")
    for row in decision_rows:
        if str(row.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        lower = " ".join(str(row.get(field) or "") for field in ("decision", "reason", "prefilter_reason", "prefilter_detail", "fail_reason")).lower()
        sl_text = decision_sl_sync_text(row)
        if "reconcile" in lower:
            warnings.append("live_reconcile_warning")
        if (
            "unmanaged" in lower
            or "local state" in lower
            or ("missing exchange" in lower and not sl_text)
        ):
            critical.append("live_exchange_local_state_consistency_issue")
        if sl_text:
            warnings.append("historical_sl_sync_failure_resolved_or_flat_warning")
    for idx, row in enumerate(min_lock_rows):
        if str(row.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
            continue
        if min_lock_row_failed(row):
            warnings.append("historical_sl_sync_failure_resolved_or_flat_warning")
            if min_lock_failure_resolved(row, min_lock_rows, idx):
                warnings.append("historical_min_lock_sync_failure_resolved")
            elif has_open_research:
                warnings.append("historical_min_lock_sync_failure_unresolved_warning")
    if has_open_research and sl_sync_ok:
        warnings.append("SL_SYNC_OK")
    return sorted(set(warnings)), sorted(set(critical))


def _close_sort_ts(row):
    value = fnum(row.get("_sort_ts"))
    if value is not None:
        return value
    return row_ts(row)


def live_open_identity_key(trade):
    stable = trade.get("id") or trade.get("trade_id") or trade.get("client_order_id") or trade.get("clientOrderId")
    if stable not in (None, ""):
        return f"id:{stable}"
    join = trade.get("research_join_key") or trade.get("research_dedup_key") or trade.get("dedup_key")
    if join not in (None, ""):
        return f"key:{join}"
    return "|".join(
        str(trade.get(field) or "")
        for field in ("symbol", "side", "entry_time")
    )


def open_trade_confirmed_healthy(trade):
    """A currently-open research trade that is safe to treat as a fresh probe.

    Requires: entry actually filled and confirmed, exchange SL present and
    confirmed, and no pending SL-sync failure. This intentionally mirrors the
    current-safety gates so a stale historical loss streak is only downgraded
    when the newer live open is genuinely clean.
    """
    if not isinstance(trade, dict):
        return False
    entry_unconfirmed = str(trade.get("entry_price_unconfirmed") or "").lower() in ("true", "1", "yes")
    if entry_unconfirmed:
        return False
    entry_source = str(trade.get("entry_source") or "").lower()
    entry_state = str(trade.get("entry_state") or "").upper()
    entry_confirmed = entry_source == "actual_exchange_fill" or entry_state == "ENTRY_CONFIRMED"
    if not entry_confirmed:
        return False
    if trade.get("exchange_sl_id") in (None, ""):
        return False
    if fnum(trade.get("exchange_sl_price_confirmed")) is None:
        return False
    if int(fnum(trade.get("sl_sync_fail_count"), 0) or 0) != 0:
        return False
    return True


def live_loss_streak_meta(confirmed_closes, consecutive_losses, live_net, critical, live_state, all_closes=None):
    """Distinguish a current trailing loss streak from a stale historical one.

    A streak is considered stale (no longer current) when newer live research
    activity exists AFTER the most recent loss close that produced the streak,
    and that newer activity is not itself a continuing loss. Newer activity is
    either:
      * a newer, confirmed, healthy live research OPEN still present in
        live_state (opened after the streak's last loss), or
      * a newer live CLOSE whose MOST RECENT realized R is non-negative
        (win/BE). This covers probes that were opened after the streak but have
        already CLOSED (so they are no longer in live_state) and were excluded
        from the confirmed sample because their exit price was an estimate
        (rr_unconfirmed). A newer closing LOSS keeps the streak current so an
        ongoing losing run still blocks.
    The streak is never downgraded while there is a current safety failure
    (critical) or a hard rolling-net breach.
    """
    meta = {
        "last_live_close_key": "",
        "last_live_open_key": "",
        "loss_streak_current": False,
        "loss_streak_stale_after_new_open": False,
    }
    if confirmed_closes:
        meta["last_live_close_key"] = live_close_dedup_key(confirmed_closes[-1])
    if consecutive_losses < 3:
        return meta
    streak_rows = confirmed_closes[-consecutive_losses:] if consecutive_losses else []
    last_loss_ts = max((_close_sort_ts(row) for row in streak_rows), default=0.0)
    newer_open = None
    newer_open_ts = None
    for trade in live_research_open_positions(live_state):
        open_ts = fnum(trade.get("entry_time"))
        if open_ts is None or open_ts <= last_loss_ts:
            continue
        if not open_trade_confirmed_healthy(trade):
            continue
        if newer_open_ts is None or open_ts > newer_open_ts:
            newer_open = trade
            newer_open_ts = open_ts
    # A newer live CLOSE after the streak also makes the historical streak stale
    # when a non-negative (win/BE) close has interrupted it. A newer closing loss
    # must keep the streak current so an ongoing losing run still blocks, BUT a
    # single fresh loss that follows an intervening win/BE must NOT pin the stale
    # historical streak as current: that win/BE already broke the run, and the
    # fresh loss is a separate, short run. We therefore locate the MOST RECENT
    # non-negative post-streak close and only keep the streak current when the
    # loss run that resumed after it has itself reached the pause threshold
    # (>=3). This recognises closed probes (e.g. rr_unconfirmed estimated exits)
    # that are no longer present in live_state open positions and were excluded
    # from the confirmed sample.
    newer_nonloss_close = False
    newer_close_key = ""
    if all_closes:
        post_streak_closes = [row for row in all_closes if _close_sort_ts(row) > last_loss_ts]
        if post_streak_closes:
            post_streak_sorted = sorted(post_streak_closes, key=_close_sort_ts)
            last_nonneg_idx = None
            for idx in range(len(post_streak_sorted) - 1, -1, -1):
                close_r = fnum(post_streak_sorted[idx].get("actual_realized_r"))
                if close_r is not None and close_r >= 0:
                    last_nonneg_idx = idx
                    break
            if last_nonneg_idx is not None:
                trailing_losses_after_nonneg = 0
                for row in post_streak_sorted[last_nonneg_idx + 1:]:
                    close_r = fnum(row.get("actual_realized_r"))
                    if close_r is not None and close_r < 0:
                        trailing_losses_after_nonneg += 1
                if trailing_losses_after_nonneg < 3:
                    newer_nonloss_close = True
                    newer_close_key = live_close_dedup_key(post_streak_sorted[last_nonneg_idx])
    stale = (bool(newer_open) or newer_nonloss_close) and not critical and live_net > -2.0
    if newer_open is not None:
        meta["last_live_open_key"] = live_open_identity_key(newer_open)
    elif newer_nonloss_close:
        meta["last_live_open_key"] = newer_close_key
    meta["loss_streak_stale_after_new_open"] = stale
    meta["loss_streak_current"] = (consecutive_losses >= 3) and not stale
    return meta


def classify_live(close_rows=None, decision_rows=None, min_lock_rows=None, live_state=None, live_trade_rows=None):
    closes = close_rows if close_rows is not None else live_close_rows(decision_rows, live_trade_rows)
    warnings, critical = live_safety_issues(decision_rows, min_lock_rows, live_state=live_state)
    unconfirmed_rows = [
        row for row in closes
        if str(row.get("rr_unconfirmed") or "").lower() in ("true", "1", "yes")
        or str(row.get("entry_unconfirmed") or "").lower() in ("true", "1", "yes")
        or str(row.get("exit_unconfirmed") or "").lower() in ("true", "1", "yes")
    ]
    confirmed_closes = [row for row in closes if row not in unconfirmed_rows]
    values = [fnum(row.get("actual_realized_r")) for row in confirmed_closes]
    values = [value for value in values if value is not None]
    reasons = []
    unconfirmed_n = len(unconfirmed_rows)
    if critical:
        live_net = round(sum(values), 4)
        streak = max_loss_streak(values)
        return "RED", critical, {"n": len(values), "net_r": live_net, "consecutive_losses": streak, "live_closed_n": len(values), "live_rolling_net_r": live_net, "live_loss_streak": streak, "live_unconfirmed_rr_n": unconfirmed_n}
    if len(values) == 0:
        reason = "live_no_confirmed_rr_trades" if unconfirmed_n else "live_no_closed_research_trades"
        return "UNKNOWN", [reason] + warnings, {"n": 0, "net_r": 0.0, "consecutive_losses": 0, "live_closed_n": 0, "live_rolling_net_r": 0.0, "live_loss_streak": 0, "live_unconfirmed_rr_n": unconfirmed_n}
    live_net = round(sum(values), 4)
    consecutive_losses = 0
    for value in reversed(values):
        if value < 0:
            consecutive_losses += 1
        else:
            break
    if consecutive_losses >= 3:
        _streak_metrics = {"n": len(values), "net_r": live_net, "consecutive_losses": consecutive_losses, "live_closed_n": len(values), "live_rolling_net_r": live_net, "live_loss_streak": consecutive_losses, "live_unconfirmed_rr_n": unconfirmed_n}
        _streak_metrics.update(live_loss_streak_meta(confirmed_closes, consecutive_losses, live_net, critical, live_state, all_closes=closes))
        return "RED", [f"live_consecutive_losses={consecutive_losses}>=3"], _streak_metrics
    if live_net <= -2.0:
        return "RED", [f"live_net_r={live_net}<=-2R"], {"n": len(values), "net_r": live_net, "consecutive_losses": consecutive_losses, "live_closed_n": len(values), "live_rolling_net_r": live_net, "live_loss_streak": consecutive_losses, "live_unconfirmed_rr_n": unconfirmed_n}
    if consecutive_losses in (1, 2):
        reasons.append(f"live_consecutive_losses={consecutive_losses}")
    blocking_warnings = [warning for warning in warnings if warning != "SL_SYNC_OK"]
    if blocking_warnings:
        reasons.extend(blocking_warnings)
    if reasons:
        return "YELLOW", reasons + [warning for warning in warnings if warning == "SL_SYNC_OK"], {"n": len(values), "net_r": live_net, "consecutive_losses": consecutive_losses, "live_closed_n": len(values), "live_rolling_net_r": live_net, "live_loss_streak": consecutive_losses, "live_unconfirmed_rr_n": unconfirmed_n}
    green_reasons = ["live_closed_small_sample_no_critical_failure"] + [warning for warning in warnings if warning == "SL_SYNC_OK"]
    if unconfirmed_n:
        green_reasons.append(f"live_unconfirmed_rr_excluded={unconfirmed_n}")
    return "GREEN", green_reasons, {"n": len(values), "net_r": live_net, "consecutive_losses": consecutive_losses, "live_closed_n": len(values), "live_rolling_net_r": live_net, "live_loss_streak": consecutive_losses, "live_unconfirmed_rr_n": unconfirmed_n}


def promotion_status(paper_health, live_health, last50, active_n=None, min_active_closed=20):
    if paper_health == "INSUFFICIENT_DATA" or (active_n is not None and active_n < min_active_closed):
        return "LIVE_MICRO_ONLY"
    if last50.get("n", 0) < min_active_closed or live_health == "UNKNOWN":
        return "LIVE_MICRO_ONLY"
    if paper_health != "GREEN":
        return "LIVE_SCALE_BLOCKED_PAPER_HEALTH"
    if live_health == "RED":
        return "LIVE_SCALE_BLOCKED_LIVE_HEALTH"
    return "PROMOTION_ALLOWED_MICRO_ONLY"


def build_summary(source="audit_script", write_summary=True):
    cfg = read_json(CONFIG_JSON)
    baseline_ts = fnum(cfg.get("research_health_baseline_ts"), 0.0) or 0.0
    min_active_closed = int(cfg.get("research_health_min_active_closed", 20) or 20)
    use_active_only = bool(cfg.get("research_health_use_active_only_for_live_scale", True))
    paper = enrich_paper_rows(paper_close_rows())
    active_paper = [row for row in paper if row_ts(row) >= baseline_ts]
    last20_rows = paper[-20:]
    last50_rows = paper[-50:]
    last100_rows = paper[-100:]
    since_min_lock_rows = [row for row in paper if row.get("since_min_lock_active")]
    last20 = metrics(last20_rows)
    last50 = metrics(last50_rows)
    last100 = metrics(last100_rows)
    since_min_lock = metrics(since_min_lock_rows)
    legacy_health, legacy_reasons = classify_paper(last20, last50)
    active_health, active_reasons, active_last20, active_last50 = classify_active_paper(
        active_paper,
        min_active_closed,
    )
    paper_health_for_scale = active_health if use_active_only else legacy_health
    live_health, live_reasons, live_metrics = classify_live()
    status = promotion_status(
        paper_health_for_scale,
        live_health,
        active_last50 if use_active_only else last50,
        active_n=len(active_paper) if use_active_only else last50.get("n"),
        min_active_closed=min_active_closed,
    )
    reasons = active_reasons + live_reasons
    if legacy_health == "RED":
        reasons.append("legacy_health_RED_warning_only")
    if status != "PROMOTION_ALLOWED_MICRO_ONLY":
        reasons.append(status)
    row = {
        "ts": time.time(),
        "source": source,
        "paper_health": paper_health_for_scale,
        "legacy_health": legacy_health,
        "paper_legacy_health": legacy_health,
        "paper_active_health": active_health,
        "live_health": live_health,
        "promotion_status": status,
        "research_health_baseline_ts": baseline_ts,
        "research_health_min_active_closed": min_active_closed,
        "research_health_use_active_only_for_live_scale": use_active_only,
        "last20": last20,
        "last50": last50,
        "last100": last100,
        "active_last20": active_last20,
        "active_last50": active_last50,
        "active_closed_count": len(active_paper),
        "since_min_lock_active": since_min_lock,
        "live_metrics": live_metrics,
        "max_live_research_trades": cfg.get("max_live_research_trades"),
        "reasons": reasons,
    }
    if write_summary:
        write_jsonl(SUMMARY_LOG, row)
    return row


def print_metrics(label, data):
    print(f"\n[{label}]")
    print(f"n={data['n']}")
    print(f"raw_net_r={fmt(data['raw_net_r'])} raw_pf={fmt(data['raw_pf'])} win_rate={fmt(data['win_rate'])}")
    print(
        "calibrated_cap_1_0 net_r={net} pf={pf}".format(
            net=fmt(data["calibrated_net_r_cap_1_0"]),
            pf=fmt(data["calibrated_pf_cap_1_0"]),
        )
    )
    print(
        "calibrated_cap_1_2 net_r={net} pf={pf}".format(
            net=fmt(data["calibrated_net_r_cap_1_2"]),
            pf=fmt(data["calibrated_pf_cap_1_2"]),
        )
    )
    print(f"max_loss_streak={data['max_loss_streak']} max_drawdown_r={fmt(data['max_drawdown_r'])}")
    print(f"gap_loss_count={data['gap_loss_count']} possible_overcharge_count={data['possible_overcharge_count']}")
    print(f"side_split={json.dumps(data['side_split'], sort_keys=True, default=str)}")
    print(f"phase_split={json.dumps(data['phase_split'], sort_keys=True, default=str)}")
    print(f"regime_split={json.dumps(data['regime_split'], sort_keys=True, default=str)}")


def main():
    row = build_summary()
    print("PASS rolling health audit completed")
    print(f"legacy_health={row['legacy_health']}")
    print(f"paper_active_health={row['paper_active_health']}")
    print(f"paper_health={row['paper_health']}")
    print(f"live_health={row['live_health']}")
    print(f"promotion_status={row['promotion_status']}")
    print(f"live_closed_n={row['live_metrics'].get('live_closed_n', row['live_metrics'].get('n'))}")
    print(f"live_loss_streak={row['live_metrics'].get('live_loss_streak', row['live_metrics'].get('consecutive_losses'))}")
    print(f"live_rolling_net_r={row['live_metrics'].get('live_rolling_net_r', row['live_metrics'].get('net_r'))}")
    print(f"research_health_baseline_ts={row['research_health_baseline_ts']}")
    print(f"active_closed_count={row['active_closed_count']}")
    print(f"max_live_research_trades={row.get('max_live_research_trades')}")
    print("reasons=" + json.dumps(row["reasons"], sort_keys=True))
    print_metrics("ACTIVE LAST 20 CLOSED", row["active_last20"])
    print_metrics("ACTIVE LAST 50 CLOSED", row["active_last50"])
    print_metrics("LAST 20 CLOSED", row["last20"])
    print_metrics("LAST 50 CLOSED", row["last50"])
    print_metrics("LAST 100 CLOSED", row["last100"])
    print_metrics("SINCE MIN-LOCK ACTIVE", row["since_min_lock_active"])
    print(f"\nsummary_log={SUMMARY_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
