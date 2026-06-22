import os
import csv
import time
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR  = os.path.join(BASE_DIR, "logs")
CF_CSV   = os.path.join(LOG_DIR, "exhaustion_counterfactual.csv")
SUMMARY  = os.path.join(LOG_DIR, "counterfactual_summary.txt")

LOOKAHEAD_HOURS  = 48
MIN_FUTURE_BARS  = 8

VN_TZ = timezone(timedelta(hours=7))

_last_run = 0
_RUN_INTERVAL = 6 * 3600

OUTCOME_WIN     = "WIN"
OUTCOME_LOSS    = "LOSS"
OUTCOME_RUNNER  = "RUNNER"
OUTCOME_PARTIAL = "PARTIAL"
OUTCOME_EXPIRED = "EXPIRED"


def maybe_evaluate_counterfactuals():
    global _last_run
    now = time.time()
    if now - _last_run < _RUN_INTERVAL:
        return
    _last_run = now
    try:
        evaluate_counterfactuals()
    except Exception as e:
        print(f"[COUNTERFACTUAL] Evaluator error (trading unaffected): {e}")


def evaluate_counterfactuals():
    if not os.path.exists(CF_CSV):
        return

    rows = _read_csv(CF_CSV)
    if not rows:
        return

    pending = [r for r in rows if not r.get("evaluated_at")]
    if not pending:
        return

    symbols = list({r["symbol"] for r in pending if r.get("symbol")})

    evaluated = 0
    for symbol in symbols:
        df15 = _fetch_m15(symbol)
        if df15 is None:
            continue

        candle_times = [float(t) / 1000.0 for t in df15["time"].tolist()]
        highs  = df15["high"].astype(float).tolist()
        lows   = df15["low"].astype(float).tolist()

        for row in pending:
            if row.get("symbol") != symbol:
                continue
            try:
                result = _simulate(row, candle_times, highs, lows)
                if result is None:
                    continue
                row["hypothetical_max_r"]   = result["max_r"]
                row["hypothetical_outcome"]  = result["outcome"]
                row["evaluated_at"]          = datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")
                evaluated += 1
            except Exception:
                pass

    if evaluated > 0:
        _write_csv(CF_CSV, rows)
        _write_summary(rows)
        all_evaluated = [r for r in rows if r.get("evaluated_at")]
        total_ev = len(all_evaluated)
        loss_n   = sum(1 for r in all_evaluated if r.get("hypothetical_outcome") == OUTCOME_LOSS)
        loss_pct = round(loss_n / total_ev * 100, 1) if total_ev > 0 else 0.0
        print(
            f"[COUNTERFACTUAL] Evaluated {evaluated} rejected trades | "
            f"cumulative={total_ev} | {loss_pct}% hypothetical losers"
        )


def _simulate(row, candle_times, highs, lows):
    try:
        entry = float(row.get("entry") or 0)
        sl    = float(row.get("sl")    or 0)
        tp    = float(row.get("tp")    or 0)
        side  = (row.get("side") or "LONG").upper()
    except (ValueError, TypeError):
        return None

    if entry == 0 or sl == 0 or tp == 0:
        return None

    r_unit = abs(entry - sl)
    if r_unit < 1e-9:
        return None

    tp_r = abs(tp - entry) / r_unit

    reject_ts = _parse_time(row.get("time", ""))
    if reject_ts is None:
        return None

    lookahead_limit = reject_ts + LOOKAHEAD_HOURS * 3600

    start_idx = None
    for i, ct in enumerate(candle_times):
        if ct >= reject_ts:
            start_idx = i
            break

    if start_idx is None:
        return None

    future_count = len(candle_times) - start_idx
    if future_count < MIN_FUTURE_BARS:
        return None

    max_r = 0.0

    for i in range(start_idx, len(candle_times)):
        ct = candle_times[i]
        if ct > lookahead_limit:
            break

        hi = highs[i]
        lo = lows[i]

        if side == "LONG":
            favorable_move = hi - entry
            sl_breach      = lo <= sl
            tp_breach      = hi >= tp
        else:
            favorable_move = entry - lo
            sl_breach      = hi >= sl
            tp_breach      = lo <= tp

        candle_max_r = favorable_move / r_unit if favorable_move > 0 else 0.0
        if candle_max_r > max_r:
            max_r = candle_max_r

        if sl_breach:
            return {"max_r": round(max_r, 3), "outcome": OUTCOME_LOSS}

        if tp_breach:
            return {"max_r": round(max_r, 3), "outcome": OUTCOME_WIN}

    return {"max_r": round(max_r, 3), "outcome": _classify_expired(max_r, tp_r)}


def _classify_expired(max_r, tp_r):
    if max_r >= tp_r * 0.85:
        return OUTCOME_RUNNER
    if max_r >= 1.0:
        return OUTCOME_PARTIAL
    return OUTCOME_EXPIRED


def _parse_time(time_str):
    if not time_str:
        return None
    try:
        now = datetime.now(VN_TZ)
        dt = datetime.strptime(time_str, "%H:%M %d-%m")
        dt = dt.replace(year=now.year, tzinfo=VN_TZ)
        if dt > now + timedelta(hours=1):
            dt = dt.replace(year=now.year - 1)
        return dt.timestamp()
    except Exception:
        return None


def _fetch_m15(symbol):
    try:
        from pool_pipeline import fetch_cached, fetch
        df = fetch_cached(symbol, "15m", max_age=300)
        if df is None:
            df = fetch(symbol, "15m")
        return df
    except Exception:
        return None


def _read_csv(path):
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _write_csv(path, rows):
    if not rows:
        return
    temp = path + ".tmp"
    fieldnames = list(rows[0].keys())
    try:
        with open(temp, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for row in rows:
                w.writerow(row)
        os.replace(temp, path)
    except Exception as e:
        print(f"[COUNTERFACTUAL] CSV write error: {e}")
        try:
            os.remove(temp)
        except Exception:
            pass


def _write_summary(rows):
    try:
        evaluated = [r for r in rows if r.get("evaluated_at")]
        if not evaluated:
            return

        by_reason = {}
        for r in evaluated:
            reason  = r.get("reject_reason") or "UNKNOWN"
            outcome = r.get("hypothetical_outcome") or ""
            try:
                max_r = float(r.get("hypothetical_max_r") or 0)
            except (ValueError, TypeError):
                max_r = 0.0

            if reason not in by_reason:
                by_reason[reason] = {"outcomes": [], "max_rs": []}
            by_reason[reason]["outcomes"].append(outcome)
            by_reason[reason]["max_rs"].append(max_r)

        total_ev  = len(evaluated)
        total_all = len(rows)

        lines = [
            f"[COUNTERFACTUAL SUMMARY] {datetime.now(VN_TZ).strftime('%Y-%m-%d %H:%M:%S')}",
            f"Evaluated: {total_ev} / {total_all} total rejected",
            "",
        ]

        for reason in sorted(by_reason):
            data     = by_reason[reason]
            outcomes = data["outcomes"]
            max_rs   = data["max_rs"]
            n        = len(outcomes)
            loss_n   = sum(1 for o in outcomes if o == OUTCOME_LOSS)
            win_n    = sum(1 for o in outcomes if o == OUTCOME_WIN)
            runner_n = sum(1 for o in outcomes if o == OUTCOME_RUNNER)
            partial_n = sum(1 for o in outcomes if o == OUTCOME_PARTIAL)
            avg_max_r = round(sum(max_rs) / n, 3) if n > 0 else 0.0
            loss_pct  = round(loss_n  / n * 100, 1) if n > 0 else 0.0
            win_pct   = round(win_n   / n * 100, 1) if n > 0 else 0.0

            lines.append(f"{reason}: n={n}")
            lines.append(f"  LOSS={loss_pct}%  WIN={win_pct}%  RUNNER={runner_n}  PARTIAL={partial_n}")
            lines.append(f"  avg_hypothetical_max_r={avg_max_r}")
            lines.append("")

        with open(SUMMARY, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

    except Exception:
        pass


if __name__ == "__main__":
    print("[COUNTERFACTUAL] Running standalone evaluation...")
    evaluate_counterfactuals()
    print("[COUNTERFACTUAL] Done.")
