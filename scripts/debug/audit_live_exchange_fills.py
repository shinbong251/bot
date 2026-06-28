#!/usr/bin/env python3
"""Read-only audit of Binance fills for recent LIVE CONFIRM_SMC_RESEARCH closes."""

import argparse
import csv
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python <3.9 fallback
    ZoneInfo = None


ROOT = Path(__file__).resolve().parents[2]
LIVE_TRADES = ROOT / "live_trades.csv"
ENTRY_TYPE = "CONFIRM_SMC_RESEARCH"


def _read_dotenv(path):
    values = {}
    if not path.exists():
        return values
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip().strip('"').strip("'")
    except Exception as exc:
        print(f"[AUDIT] API_UNAVAILABLE: .env read failed: {exc}")
    return values


def _load_read_only_keys():
    """Mirror the repo credential lookup without importing config.py or writing config.json."""
    cfg = {}
    cfg_path = ROOT / "config.json"
    try:
        with cfg_path.open("r", encoding="utf-8") as handle:
            cfg = json.load(handle)
    except Exception as exc:
        print(f"[AUDIT] API_UNAVAILABLE: config.json read failed: {exc}")

    dotenv = _read_dotenv(ROOT / ".env")
    api_key = os.environ.get("API_KEY") or dotenv.get("API_KEY") or cfg.get("api_key", "")
    api_secret = os.environ.get("API_SECRET") or dotenv.get("API_SECRET") or cfg.get("api_secret", "")
    return api_key, api_secret


def _import_binance_client():
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    try:
        from exchange import binance_client
    except Exception as exc:
        print(f"[AUDIT] API_UNAVAILABLE: cannot import exchange.binance_client: {exc}")
        return None
    if not hasattr(binance_client, "_get"):
        print("[AUDIT] API_UNAVAILABLE: exchange.binance_client._get is unavailable")
        return None
    api_key, api_secret = _load_read_only_keys()
    if not api_key or not api_secret:
        print("[AUDIT] API_UNAVAILABLE: API_KEY/API_SECRET missing from env/.env/config.json")
        return None
    if hasattr(binance_client, "_load_keys"):
        binance_client._load_keys = lambda: (api_key, api_secret)
    return binance_client


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


def clean(value):
    return str(value or "").strip()


def local_tz(name):
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    if name in ("Asia/Bangkok", "ICT"):
        return timezone(timedelta(hours=7), name="ICT")
    return timezone.utc


def parse_csv_time(value, year, tzinfo):
    raw = clean(value)
    if not raw:
        return None
    for fmt in ("%H:%M %d-%m", "%H:%M:%S %d-%m"):
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.replace(year=year, tzinfo=tzinfo).astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def row_year(row, now_utc):
    for field in ("signal_created_ts", "source_timestamp", "ts", "closed_at_unix"):
        value = fnum(row.get(field))
        if value:
            try:
                return datetime.fromtimestamp(value, tz=timezone.utc).year
            except (OverflowError, OSError, ValueError):
                pass
    return now_utc.year


def read_live_rows(path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


@dataclass
class Window:
    start_ms: int
    end_ms: int


def ms(dt):
    return int(dt.timestamp() * 1000)


def query_user_trades(client, symbol, window, order_id=None):
    params = {
        "symbol": symbol,
        "startTime": window.start_ms,
        "endTime": window.end_ms,
        "limit": 1000,
    }
    if order_id not in (None, ""):
        params["orderId"] = order_id
    return client._get("/fapi/v1/userTrades", params=params, signed=True)


def weighted_avg(fills):
    qty = 0.0
    notional = 0.0
    order_ids = []
    for fill in fills:
        q = fnum(fill.get("qty"), 0.0)
        p = fnum(fill.get("price"), 0.0)
        if q <= 0 or p <= 0:
            continue
        qty += q
        notional += p * q
        oid = fill.get("orderId")
        if oid not in (None, "") and oid not in order_ids:
            order_ids.append(oid)
    if qty <= 0:
        return None, 0.0, []
    return notional / qty, qty, order_ids


def fill_time_ms(fill):
    value = fnum(fill.get("time"))
    return None if value is None else int(value)


def select_side_fills(fills, side):
    expected = side.upper()
    return [fill for fill in fills if clean(fill.get("side")).upper() == expected]


def group_fills_by_order(fills):
    groups = {}
    for fill in fills:
        oid = clean(fill.get("orderId")) or "UNKNOWN"
        groups.setdefault(oid, []).append(fill)
    return list(groups.values())


def select_best_order_fills(fills, expected_side, expected_price, target_dt):
    side_fills = select_side_fills(fills, expected_side)
    if not side_fills:
        return []
    groups = group_fills_by_order(side_fills)
    if len(groups) == 1:
        return groups[0]

    target_ms = ms(target_dt) if target_dt is not None else None

    def score_group(group):
        avg, qty, _ = weighted_avg(group)
        if avg is None or qty <= 0:
            return (float("inf"), float("inf"), 0.0)
        pricescore = 0.0
        if expected_price and expected_price > 0:
            pricescore = abs(avg - expected_price) / expected_price
        times = [fill_time_ms(fill) for fill in group]
        times = [t for t in times if t is not None]
        timescore = 0.0
        if target_ms is not None and times:
            center = sum(times) / len(times)
            timescore = abs(center - target_ms) / 60000.0
        return (timescore, pricescore, -qty)

    return min(groups, key=score_group)


def trade_windows(row, args, now_utc):
    year = row_year(row, now_utc)
    tzinfo = local_tz(args.timezone)
    open_dt = parse_csv_time(row.get("open_time"), year, tzinfo)
    close_dt = parse_csv_time(row.get("close_time"), year, tzinfo)
    signal_ts = fnum(row.get("signal_created_ts"))
    if open_dt is None and signal_ts:
        open_dt = datetime.fromtimestamp(signal_ts, tz=timezone.utc)
    if close_dt is not None and open_dt is not None and close_dt < open_dt - timedelta(hours=12):
        close_dt += timedelta(days=1)
    if close_dt is None and open_dt is not None:
        age_min = fnum(row.get("trade_age_minutes"))
        if age_min is not None:
            close_dt = open_dt + timedelta(minutes=age_min)
    pad = timedelta(minutes=args.window_minutes)
    open_window = None if open_dt is None else Window(ms(open_dt - pad), ms(open_dt + pad))
    close_window = None if close_dt is None else Window(ms(close_dt - pad), ms(close_dt + pad))
    return open_dt, close_dt, open_window, close_window


def is_recent_close(row, args, now_utc):
    year = row_year(row, now_utc)
    tzinfo = local_tz(args.timezone)
    close_dt = parse_csv_time(row.get("close_time"), year, tzinfo)
    open_dt = parse_csv_time(row.get("open_time"), year, tzinfo)
    if close_dt is not None and open_dt is not None and close_dt < open_dt - timedelta(hours=12):
        close_dt += timedelta(days=1)
    if close_dt is None:
        return False
    return close_dt >= now_utc - timedelta(hours=args.hours)


def risk_per_unit(row, side):
    entry = fnum(row.get("entry"))
    initial_sl = fnum(row.get("initial_sl"))
    if initial_sl is not None and entry is not None and entry != initial_sl:
        return abs(entry - initial_sl), "LOGGED_ENTRY_INITIAL_SL"

    logged_rr = fnum(row.get("rr"))
    logged_exit = fnum(row.get("exit_price"))
    if entry is not None and logged_exit is not None and logged_rr not in (None, 0.0):
        pnl = logged_exit - entry if side == "LONG" else entry - logged_exit
        inferred = abs(pnl / logged_rr)
        if inferred > 0:
            return inferred, "INFERRED_FROM_LOGGED_RR_UNCERTAIN"
    return None, "MISSING"


def compute_r(side, entry_fill, exit_fill, risk_unit):
    if entry_fill is None or exit_fill is None or risk_unit in (None, 0.0):
        return None
    if side == "LONG":
        return (exit_fill - entry_fill) / risk_unit
    return (entry_fill - exit_fill) / risk_unit


def classify(logged_rr, exchange_rr, entry_fill, exit_fill, api_ok):
    if not api_ok:
        return "API_UNAVAILABLE"
    if entry_fill is None or exit_fill is None:
        return "FILL_NOT_FOUND"
    if logged_rr is None or exchange_rr is None:
        return "FILL_NOT_FOUND"
    delta = exchange_rr - logged_rr
    if abs(delta) <= 0.02:
        return "MATCH"
    if abs(delta) <= 0.10:
        return "SMALL_DELTA"
    if delta < 0:
        return "LOGGED_RR_OVERSTATED"
    return "LOGGED_RR_UNDERSTATED"


def fmt(value, digits=6):
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def audit_trade(client, row, args, now_utc):
    symbol = clean(row.get("symbol")).upper()
    side = clean(row.get("side")).upper()
    open_dt, close_dt, open_window, close_window = trade_windows(row, args, now_utc)
    entry_order_id = clean(row.get("exchange_order_id"))
    exit_order_id = clean(row.get("exchange_exit_order_id") or row.get("exit_order_id"))
    entry_logged = fnum(row.get("entry"))
    exit_logged = fnum(row.get("exit_price"))

    api_ok = client is not None and open_window is not None and close_window is not None
    entry_fills = []
    exit_fills = []
    if api_ok:
        entry_raw = query_user_trades(client, symbol, open_window, entry_order_id or None)
        exit_raw = query_user_trades(client, symbol, close_window, exit_order_id or None)
        if entry_raw is None or exit_raw is None:
            api_ok = False
        elif not isinstance(entry_raw, list) or not isinstance(exit_raw, list):
            api_ok = False
        else:
            entry_side = "BUY" if side == "LONG" else "SELL"
            exit_side = "SELL" if side == "LONG" else "BUY"
            if entry_order_id:
                entry_fills = select_side_fills(entry_raw, entry_side)
            else:
                entry_fills = select_best_order_fills(entry_raw, entry_side, entry_logged, open_dt)
            if exit_order_id:
                exit_fills = select_side_fills(exit_raw, exit_side)
            else:
                exit_fills = select_best_order_fills(exit_raw, exit_side, exit_logged, close_dt)

    entry_fill, entry_qty, entry_order_ids = weighted_avg(entry_fills)
    exit_fill, exit_qty, exit_order_ids = weighted_avg(exit_fills)
    risk_unit, risk_source = risk_per_unit(row, side)
    exchange_rr = compute_r(side, entry_fill, exit_fill, risk_unit)
    logged_rr = fnum(row.get("rr"))
    delta_r = None if exchange_rr is None or logged_rr is None else exchange_rr - logged_rr
    status = classify(logged_rr, exchange_rr, entry_fill, exit_fill, api_ok)

    return {
        "status": status,
        "symbol": symbol,
        "side": side,
        "trade_id": clean(row.get("id") or row.get("trade_id")),
        "open_time": clean(row.get("open_time")),
        "close_time": clean(row.get("close_time")),
        "open_dt_utc": open_dt,
        "close_dt_utc": close_dt,
        "entry_logged": fnum(row.get("entry")),
        "exit_logged": fnum(row.get("exit_price")),
        "rr_logged": logged_rr,
        "exit_type": clean(row.get("exit_type")),
        "exit_price_source": clean(row.get("exit_price_source")),
        "exchange_order_id": entry_order_id,
        "exchange_sl_id": clean(row.get("exchange_sl_id")),
        "api_window_available": open_window is not None and close_window is not None,
        "entry_fill": entry_fill,
        "entry_qty": entry_qty,
        "entry_order_ids": entry_order_ids,
        "exit_fill": exit_fill,
        "exit_qty": exit_qty,
        "exit_order_ids": exit_order_ids,
        "exchange_realized_r": exchange_rr,
        "delta_r": delta_r,
        "risk_per_unit": risk_unit,
        "risk_source": risk_source,
    }


def print_table(results):
    headers = [
        "status", "symbol", "side", "trade_id", "open", "close",
        "entry_log", "entry_fill", "exit_log", "exit_fill",
        "rr_log", "rr_exch", "delta_r", "exit_type", "exit_src",
        "ex_order", "ex_sl", "fill_entry_orders", "fill_exit_orders", "risk_src",
    ]
    rows = []
    for r in results:
        rows.append([
            r["status"],
            r["symbol"],
            r["side"],
            r["trade_id"],
            r["open_time"],
            r["close_time"],
            fmt(r["entry_logged"], 8),
            fmt(r["entry_fill"], 8),
            fmt(r["exit_logged"], 8),
            fmt(r["exit_fill"], 8),
            fmt(r["rr_logged"], 4),
            fmt(r["exchange_realized_r"], 4),
            fmt(r["delta_r"], 4),
            r["exit_type"] or "n/a",
            r["exit_price_source"] or "n/a",
            r["exchange_order_id"] or "n/a",
            r["exchange_sl_id"] or "n/a",
            ",".join(str(x) for x in r["entry_order_ids"]) or "n/a",
            ",".join(str(x) for x in r["exit_order_ids"]) or "n/a",
            r["risk_source"],
        ])
    widths = [len(h) for h in headers]
    for row in rows:
        widths = [max(width, len(str(cell))) for width, cell in zip(widths, row)]
    print(" | ".join(h.ljust(widths[i]) for i, h in enumerate(headers)))
    print("-+-".join("-" * width for width in widths))
    for row in rows:
        print(" | ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))


def print_ondo_answer(results):
    ondo = [r for r in results if r["symbol"] == "ONDOUSDT"]
    if not ondo:
        return
    r = ondo[-1]
    verdict = "unknown"
    if r["status"] == "FILL_NOT_FOUND":
        verdict = "fill not found"
    elif r["status"] == "API_UNAVAILABLE":
        verdict = "API unavailable"
    elif r["exchange_realized_r"] is not None:
        if abs(r["exchange_realized_r"] - 0.61) <= 0.10:
            verdict = "logged +0.61R was broadly real"
        elif abs(r["exchange_realized_r"]) <= 0.10:
            verdict = "actual close was near BE, logged +0.61R was false/overstated"
        elif r["exchange_realized_r"] < 0:
            verdict = "actual close was below BE, logged +0.61R was false/overstated"
        else:
            verdict = "actual close was profitable but materially different from logged +0.61R"
    print("\nONDOUSDT answer:")
    print(f"  actual entry fill : {fmt(r['entry_fill'], 8)}")
    print(f"  actual exit fill  : {fmt(r['exit_fill'], 8)}")
    print(f"  exchange RR       : {fmt(r['exchange_realized_r'], 4)}")
    print(f"  logged RR         : {fmt(r['rr_logged'], 4)}")
    print(f"  delta R           : {fmt(r['delta_r'], 4)}")
    print(f"  classification    : {r['status']}")
    print(f"  verdict           : {verdict}")
    print(f"  risk source       : {r['risk_source']} risk_per_unit={fmt(r['risk_per_unit'], 8)}")
    print(f"  entry order ids   : {r['entry_order_ids'] or 'n/a'}")
    print(f"  exit order ids    : {r['exit_order_ids'] or 'n/a'}")


def overall_status(results):
    if not results:
        return "WARN"
    fail_statuses = {"LOGGED_RR_OVERSTATED"}
    warn_statuses = {"SMALL_DELTA", "LOGGED_RR_UNDERSTATED", "FILL_NOT_FOUND", "API_UNAVAILABLE"}
    if any(r["status"] in fail_statuses for r in results):
        return "FAIL"
    if any(r["status"] in warn_statuses for r in results):
        return "WARN"
    return "PASS"


def main():
    parser = argparse.ArgumentParser(
        description="Read-only Binance fill audit for recent LIVE CONFIRM_SMC_RESEARCH closes."
    )
    parser.add_argument("--symbol", help="Optional symbol filter, e.g. ONDOUSDT")
    parser.add_argument("--hours", type=float, default=24.0, help="Recent close window in hours (default: 24)")
    parser.add_argument("--window-minutes", type=float, default=10.0, help="Open/close fill lookup padding (default: 10)")
    parser.add_argument("--timezone", default="Asia/Bangkok", help="Timezone for CSV display times (default: Asia/Bangkok)")
    args = parser.parse_args()

    now_utc = datetime.now(timezone.utc)
    rows = read_live_rows(LIVE_TRADES)
    symbol_filter = clean(args.symbol).upper()
    candidates = []
    for row in rows:
        if clean(row.get("entry_type")).upper() != ENTRY_TYPE:
            continue
        if clean(row.get("status")).upper() in ("", "OPEN"):
            continue
        if symbol_filter and clean(row.get("symbol")).upper() != symbol_filter:
            continue
        if not is_recent_close(row, args, now_utc):
            continue
        candidates.append(row)

    print(f"[AUDIT] read_only=true endpoint=GET /fapi/v1/userTrades csv={LIVE_TRADES}")
    print(f"[AUDIT] candidates={len(candidates)} hours={args.hours:g} symbol={symbol_filter or 'ALL'} timezone={args.timezone}")

    client = _import_binance_client()
    results = [audit_trade(client, row, args, now_utc) for row in candidates]
    status = overall_status(results)
    print(f"\n{status}")
    if results:
        print_table(results)
    else:
        print("No recent LIVE CONFIRM_SMC_RESEARCH closed trades matched the filters.")

    print_ondo_answer(results)

    matched_n = sum(1 for r in results if r["status"] == "MATCH")
    overstated_n = sum(1 for r in results if r["status"] == "LOGGED_RR_OVERSTATED")
    fill_not_found_n = sum(1 for r in results if r["status"] == "FILL_NOT_FOUND")
    deltas = [abs(r["delta_r"]) for r in results if r["delta_r"] is not None]
    max_abs_delta_r = max(deltas) if deltas else None
    print("\nSummary:")
    print(f"  n_audited       : {len(results)}")
    print(f"  matched_n       : {matched_n}")
    print(f"  overstated_n    : {overstated_n}")
    print(f"  fill_not_found_n: {fill_not_found_n}")
    print(f"  max_abs_delta_r : {fmt(max_abs_delta_r, 4)}")


if __name__ == "__main__":
    main()
