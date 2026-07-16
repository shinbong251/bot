# =====================================================================
# FOUR_PHASE_BREAKOUT_CONTEXT_SHADOW_V1 (SHADOW / LOG-ONLY)
# =====================================================================
# Log-only market-context classifier:
#   previous wave (fixed pre-range H1 displacement, frozen at range
#   establishment) + frozen H1 range + first accepted closed-M5 breakout
#   -> ACCUMULATION / REACCUMULATION / DISTRIBUTION / REDISTRIBUTION.
#
# Pure instrumentation: it NEVER gates, NEVER changes a paper/live
# decision, never touches risk/cap/RR/SL/TP/exits or any order path,
# never fetches network data, and never adds fields to any decision
# payload — callers must ignore every return value in production.
# Previous wave is computed ONLY from the 20 closed H1 bars preceding
# the frozen range start; current trend/bias fields are intentionally
# not inputs (see /tmp/four_phase_design_audit.md: a current-trend
# previous wave makes the reversal phases unreachable).
# No outcome data (realized R, first-hit, MFE/MAE, terminal status,
# future candles) is used as a classifier input or logged in rows.
# =====================================================================

import json
import math
import os
import tempfile
import threading
import time

FOUR_PHASE_SCHEMA_VERSION = "four_phase_breakout_v1"
FOUR_PHASE_EVENT_TYPE = "FOUR_PHASE_BREAKOUT_CONTEXT_SHADOW"
FOUR_PHASE_LOG_PATH = os.path.join("logs", "four_phase_breakout_context_shadow_v1.jsonl")
# *_state.json is already gitignored (see .gitignore "runtime / local state").
FOUR_PHASE_STATE_PATH = "four_phase_breakout_shadow_state.json"
FOUR_PHASE_STATE_SCHEMA_VERSION = 1

# ---- range rule (closed H1 bars) ----
RANGE_WINDOW_BARS = 18
RANGE_MIN_CONTAINMENT_BARS = 12
RANGE_CLOSE_TOL = 0.0025
RANGE_TOUCH_TOL = 0.005
RANGE_MIN_TOUCHES_PER_SIDE = 2
RANGE_WIDTH_ATR_MIN = 2.0
RANGE_WIDTH_ATR_MAX = 12.0
RANGE_WIDTH_PCT_MAX = 0.08
ATR_BARS = 14
RANGE_TTL_H1_BARS = 96
H1_DEPARTURE_INVALIDATE_BARS = 6
H1_BAR_SECS = 3600

# ---- previous-wave rule (closed H1 bars strictly before range start) ----
PREV_WAVE_BARS = 20
PREV_WAVE_MIN_DISP_FLOOR = 0.02
PREV_WAVE_SOURCE_TF = "1h"

# ---- M5 breakout rule (closed M5 bars vs frozen boundaries) ----
BREAK_MIN_ATR_MULT = 0.5
BREAK_MIN_PCT = 0.001
BREAK_MIN_BODY_RATIO = 0.4
FALSE_BREAK_M5_INSIDE_BARS = 3
M5_BAR_SECS = 300
M5_STALE_SECS = 900

# ---- cycle lifecycle ----
CYCLE_COOLDOWN_H1_BARS = 12
STATE_PRUNE_SECS = 7 * 86400
STATE_MAX_BYTES = 10 * 1024 * 1024
STATE_SOFT_SAVE_SECS = 600

_FOUR_PHASES = (
    "ACCUMULATION_CONFIRMED",
    "REACCUMULATION_CONFIRMED",
    "DISTRIBUTION_CONFIRMED",
    "REDISTRIBUTION_CONFIRMED",
)
_PHASE_MAP = {
    ("DOWN", "UP"): "ACCUMULATION_CONFIRMED",
    ("UP", "UP"): "REACCUMULATION_CONFIRMED",
    ("UP", "DOWN"): "DISTRIBUTION_CONFIRMED",
    ("DOWN", "DOWN"): "REDISTRIBUTION_CONFIRMED",
}

_lock = threading.Lock()
_DEFAULT_STORE = None


def _fp_float(value):
    try:
        if value is None or value == "":
            return None
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except (TypeError, ValueError):
        return None


def _fp_norm_price(value):
    value = _fp_float(value)
    if value is None:
        return "NA"
    return format(value, ".10g")


def bars_tail_from_df(df, n):
    """Extract the last n CLOSED bars from a klines DataFrame as plain dicts.

    The last DataFrame row is the in-progress candle (codebase convention:
    closed candle = iloc[-2]) and is dropped. Binance kline "time" is ms;
    converted to seconds. Returns [] on any failure. Never mutates df.
    """
    try:
        if df is None or len(df) < 2:
            return []
        tail = df.iloc[-(n + 1):-1]
        bars = []
        for t, o, h, l, c in zip(
            tail["time"], tail["open"], tail["high"], tail["low"], tail["close"]
        ):
            ts = _fp_float(t)
            if ts is not None and ts > 1e12:
                ts = ts / 1000.0
            bars.append({
                "time": ts,
                "open": _fp_float(o),
                "high": _fp_float(h),
                "low": _fp_float(l),
                "close": _fp_float(c),
            })
        return bars
    except Exception:
        return []


def _fp_atr(bars, n=ATR_BARS):
    if not bars or len(bars) < n:
        return None
    total = 0.0
    count = 0
    for bar in bars[-n:]:
        high = _fp_float(bar.get("high"))
        low = _fp_float(bar.get("low"))
        if high is None or low is None or high < low:
            return None
        total += high - low
        count += 1
    if count == 0:
        return None
    return total / count


def build_market_cycle_id(symbol, range_start_ts, range_high, range_low):
    """Deterministic restart-safe cycle identity (NOT a signal key)."""
    try:
        start = int(float(range_start_ts))
    except (TypeError, ValueError):
        start = -1
    return "{}|{}|{}|{}".format(
        str(symbol or ""), start, _fp_norm_price(range_high), _fp_norm_price(range_low)
    )


# =====================================================================
# RANGE EVALUATOR (candidate; boundaries freeze only at establishment)
# =====================================================================

def evaluate_range_state(h1_bars):
    """Evaluate a CANDIDATE range from closed H1 bars. Pure; no I/O.

    Returns the candidate's range_state plus boundary/width/touch fields.
    Boundaries here are candidate values: the caller may replace them
    wholesale every scan until establishment, and must freeze them (stop
    calling this for the cycle) once established.
    """
    out = {
        "range_state": "RANGE_CONTEXT_MISSING",
        "range_high": None,
        "range_low": None,
        "range_width": None,
        "range_width_pct": None,
        "range_width_atr": None,
        "atr_h1": None,
        "containment_bars": 0,
        "touches_high": 0,
        "touches_low": 0,
        "range_start_ts": None,
        "reasons": [],
    }
    if not h1_bars or len(h1_bars) < RANGE_WINDOW_BARS:
        out["reasons"].append("insufficient_h1_bars")
        return out

    window = h1_bars[-RANGE_WINDOW_BARS:]
    highs = [_fp_float(bar.get("high")) for bar in window]
    lows = [_fp_float(bar.get("low")) for bar in window]
    closes = [_fp_float(bar.get("close")) for bar in window]
    if any(v is None for v in highs) or any(v is None for v in lows) or any(v is None for v in closes):
        out["reasons"].append("non_finite_bar_values")
        return out

    range_high = max(highs)
    range_low = min(lows)
    if range_high <= range_low or range_low <= 0:
        out["range_state"] = "RANGE_INVALID"
        out["reasons"].append("degenerate_boundaries")
        return out

    mid = (range_high + range_low) / 2.0
    width = range_high - range_low
    width_pct = width / mid
    atr_h1 = _fp_atr(h1_bars, ATR_BARS)
    width_atr = (width / atr_h1) if atr_h1 else None

    out.update({
        "range_high": range_high,
        "range_low": range_low,
        "range_width": width,
        "range_width_pct": width_pct,
        "range_width_atr": width_atr,
        "atr_h1": atr_h1,
        "range_start_ts": window[0].get("time"),
    })

    if atr_h1 is None or width_atr is None:
        out["reasons"].append("atr_unavailable")
        return out
    if width_pct > RANGE_WIDTH_PCT_MAX or width_atr > RANGE_WIDTH_ATR_MAX:
        out["range_state"] = "NO_RANGE"
        out["reasons"].append("width_bounds_exceeded")
        return out
    if width_atr < RANGE_WIDTH_ATR_MIN:
        out["range_state"] = "RANGE_INVALID"
        out["reasons"].append("width_below_noise_floor")
        return out

    close_low_bound = range_low * (1.0 - RANGE_CLOSE_TOL)
    close_high_bound = range_high * (1.0 + RANGE_CLOSE_TOL)
    containment = 0
    for close in reversed(closes):
        if close_low_bound <= close <= close_high_bound:
            containment += 1
        else:
            break
    touches_high = sum(1 for high in highs if high >= range_high * (1.0 - RANGE_TOUCH_TOL))
    touches_low = sum(1 for low in lows if low <= range_low * (1.0 + RANGE_TOUCH_TOL))
    containment_start_index = len(window) - containment
    if 0 <= containment_start_index < len(window):
        out["range_start_ts"] = window[containment_start_index].get("time")

    out["containment_bars"] = containment
    out["touches_high"] = touches_high
    out["touches_low"] = touches_low

    if (
        containment >= RANGE_MIN_CONTAINMENT_BARS
        and touches_high >= RANGE_MIN_TOUCHES_PER_SIDE
        and touches_low >= RANGE_MIN_TOUCHES_PER_SIDE
    ):
        out["range_state"] = "RANGE_ESTABLISHED"
        out["reasons"].append("establishment_criteria_met")
    else:
        out["range_state"] = "RANGE_FORMING"
        if containment < RANGE_MIN_CONTAINMENT_BARS:
            out["reasons"].append("containment_insufficient")
        if touches_high < RANGE_MIN_TOUCHES_PER_SIDE or touches_low < RANGE_MIN_TOUCHES_PER_SIDE:
            out["reasons"].append("touches_insufficient")
    return out


# =====================================================================
# PREVIOUS-WAVE EVALUATOR (frozen once at range establishment)
# =====================================================================

def evaluate_previous_wave(pre_range_bars, range_width_pct, range_start_ts=None, now_ts=None):
    """Fixed pre-range H1 displacement. Pure; no I/O; no outcome data.

    pre_range_bars: the PREV_WAVE_BARS closed H1 bars strictly BEFORE the
    frozen range_start_ts (caller slices by bar time). Deliberately does
    NOT accept current-trend or HTF-bias style inputs.
    """
    out = {
        "previous_wave_direction": "PREV_WAVE_MISSING",
        "previous_wave_source_tf": PREV_WAVE_SOURCE_TF,
        "wave_disp_pct": None,
        "previous_wave_strength": None,
        "structural_agreement": None,
        "wave_threshold_pct": None,
    }
    start_ts = _fp_float(range_start_ts)
    if start_ts is not None and now_ts is not None:
        if (now_ts - start_ts) > RANGE_TTL_H1_BARS * H1_BAR_SECS:
            out["previous_wave_direction"] = "PREV_WAVE_STALE"
            return out
    if not pre_range_bars or len(pre_range_bars) < PREV_WAVE_BARS:
        return out

    bars = pre_range_bars[-PREV_WAVE_BARS:]
    start_close = _fp_float(bars[0].get("close"))
    end_close = _fp_float(bars[-1].get("close"))
    width_pct = _fp_float(range_width_pct)
    if start_close is None or end_close is None or start_close <= 0 or width_pct is None:
        return out

    disp = (end_close - start_close) / start_close
    threshold = max(1.0 * width_pct, PREV_WAVE_MIN_DISP_FLOOR)
    out["wave_disp_pct"] = disp
    out["wave_threshold_pct"] = threshold
    if width_pct > 0:
        out["previous_wave_strength"] = abs(disp) / width_pct

    if abs(disp) < threshold:
        out["previous_wave_direction"] = "PREV_WAVE_NEUTRAL"
        return out

    direction = "PREV_WAVE_UP" if disp > 0 else "PREV_WAVE_DOWN"

    half = PREV_WAVE_BARS // 2
    first_half = bars[:half]
    second_half = bars[half:]
    first_high = max(_fp_float(bar.get("high")) or float("-inf") for bar in first_half)
    second_high = max(_fp_float(bar.get("high")) or float("-inf") for bar in second_half)
    first_low = min(_fp_float(bar.get("low")) or float("inf") for bar in first_half)
    second_low = min(_fp_float(bar.get("low")) or float("inf") for bar in second_half)
    up_structure = (second_high > first_high) or (second_low > first_low)
    down_structure = (second_high < first_high) or (second_low < first_low)

    if direction == "PREV_WAVE_UP" and down_structure and not up_structure:
        out["previous_wave_direction"] = "PREV_WAVE_MIXED"
        out["structural_agreement"] = False
        return out
    if direction == "PREV_WAVE_DOWN" and up_structure and not down_structure:
        out["previous_wave_direction"] = "PREV_WAVE_MIXED"
        out["structural_agreement"] = False
        return out

    out["previous_wave_direction"] = direction
    out["structural_agreement"] = True
    return out


# =====================================================================
# M5 BREAKOUT EVALUATOR (vs FROZEN boundaries)
# =====================================================================

def evaluate_breakout_state(m5_bars, range_high, range_low,
                            pending_direction=None, pending_bar_ts=None,
                            now_ts=None):
    """Classify the last fully CLOSED M5 candle against frozen boundaries.

    Pure; no I/O; never looks at the open candle or any future bar.
    pending_direction/pending_bar_ts carry an unresolved breach from the
    cycle state so the 3-closes-back-inside false-break rule can resolve.
    """
    out = {
        "breakout_state": "BREAK_CONTEXT_MISSING",
        "breakout_direction": None,
        "break_close_beyond_atr": None,
        "body_acceptance": None,
        "body_ratio": None,
        "wick_rejection": None,
        "atr_m5": None,
        "break_bar_ts": None,
        "reasons": [],
    }
    range_high = _fp_float(range_high)
    range_low = _fp_float(range_low)
    if range_high is None or range_low is None or range_high <= range_low:
        out["reasons"].append("frozen_boundaries_missing")
        return out
    if not m5_bars or len(m5_bars) < ATR_BARS + 1:
        out["reasons"].append("insufficient_m5_bars")
        return out

    last = m5_bars[-1]
    last_ts = _fp_float(last.get("time"))
    if now_ts is not None and last_ts is not None:
        if now_ts - (last_ts + M5_BAR_SECS) > M5_STALE_SECS:
            out["reasons"].append("m5_stale")
            return out

    open_ = _fp_float(last.get("open"))
    high = _fp_float(last.get("high"))
    low = _fp_float(last.get("low"))
    close = _fp_float(last.get("close"))
    if None in (open_, high, low, close):
        out["reasons"].append("non_finite_last_bar")
        return out

    atr_m5 = _fp_atr(m5_bars, ATR_BARS)
    if atr_m5 is None or atr_m5 <= 0:
        out["reasons"].append("atr_m5_unavailable")
        return out

    bar_range = high - low
    body_ratio = (abs(close - open_) / bar_range) if bar_range > 0 else 0.0
    out["atr_m5"] = atr_m5
    out["body_ratio"] = body_ratio
    out["break_bar_ts"] = last_ts

    up_close_beyond = close > range_high
    down_close_beyond = close < range_low
    up_wick = (high > range_high) and not up_close_beyond
    down_wick = (low < range_low) and not down_close_beyond
    out["wick_rejection"] = bool(up_wick or down_wick)

    up_thresh = max(BREAK_MIN_ATR_MULT * atr_m5, BREAK_MIN_PCT * range_high)
    down_thresh = max(BREAK_MIN_ATR_MULT * atr_m5, BREAK_MIN_PCT * range_low)
    up_confirmed = (
        up_close_beyond
        and (close - range_high) >= up_thresh
        and body_ratio >= BREAK_MIN_BODY_RATIO
    )
    down_confirmed = (
        down_close_beyond
        and (range_low - close) >= down_thresh
        and body_ratio >= BREAK_MIN_BODY_RATIO
    )

    if up_confirmed or down_confirmed:
        direction = "UP" if up_confirmed else "DOWN"
        beyond = (close - range_high) if up_confirmed else (range_low - close)
        out["breakout_state"] = "BREAK_%s_CONFIRMED" % direction
        out["breakout_direction"] = direction
        out["break_close_beyond_atr"] = beyond / atr_m5
        out["body_acceptance"] = True
        out["reasons"].append("first_accepted_close")
        return out

    if pending_direction in ("UP", "DOWN") and pending_bar_ts is not None:
        pending_ts = _fp_float(pending_bar_ts)
        bars_after = [
            bar for bar in m5_bars
            if _fp_float(bar.get("time")) is not None
            and _fp_float(bar.get("time")) > pending_ts
        ]
        inside_trailing = 0
        for bar in reversed(bars_after):
            bar_close = _fp_float(bar.get("close"))
            if bar_close is not None and range_low <= bar_close <= range_high:
                inside_trailing += 1
            else:
                break
        if inside_trailing >= FALSE_BREAK_M5_INSIDE_BARS:
            out["breakout_state"] = "FALSE_BREAK_%s" % pending_direction
            out["breakout_direction"] = pending_direction
            out["reasons"].append("returned_inside_%d_closes" % inside_trailing)
            return out

    if up_close_beyond or up_wick:
        out["breakout_state"] = "BREAK_UP_PENDING"
        out["breakout_direction"] = "UP"
        out["break_close_beyond_atr"] = (close - range_high) / atr_m5 if up_close_beyond else None
        out["body_acceptance"] = bool(up_close_beyond and body_ratio >= BREAK_MIN_BODY_RATIO)
        out["reasons"].append("wick_breach" if up_wick else "sub_threshold_close")
        return out
    if down_close_beyond or down_wick:
        out["breakout_state"] = "BREAK_DOWN_PENDING"
        out["breakout_direction"] = "DOWN"
        out["break_close_beyond_atr"] = (range_low - close) / atr_m5 if down_close_beyond else None
        out["body_acceptance"] = bool(down_close_beyond and body_ratio >= BREAK_MIN_BODY_RATIO)
        out["reasons"].append("wick_breach" if down_wick else "sub_threshold_close")
        return out

    if pending_direction in ("UP", "DOWN"):
        out["breakout_state"] = "BREAK_%s_PENDING" % pending_direction
        out["breakout_direction"] = pending_direction
        out["reasons"].append("pending_unresolved")
        return out

    out["breakout_state"] = "BREAK_NONE"
    return out


# =====================================================================
# PHASE MAPPER
# =====================================================================

def map_four_phase(previous_wave_direction, breakout_state, range_established=True):
    """Map (frozen previous wave, breakout state) to a phase label.

    MIXED/NEUTRAL/MISSING/STALE waves are NEVER forced into one of the
    four phases: a confirmed break under those waves is PHASE_UNKNOWN.
    """
    if not range_established:
        return "RANGE_UNRESOLVED"
    if breakout_state == "BREAK_UP_CONFIRMED":
        break_dir = "UP"
    elif breakout_state == "BREAK_DOWN_CONFIRMED":
        break_dir = "DOWN"
    else:
        return "PHASE_PENDING"
    wave = str(previous_wave_direction or "")
    if wave == "PREV_WAVE_UP":
        wave_dir = "UP"
    elif wave == "PREV_WAVE_DOWN":
        wave_dir = "DOWN"
    else:
        return "PHASE_UNKNOWN"
    return _PHASE_MAP[(wave_dir, break_dir)]


# =====================================================================
# STATE STORE (bounded, atomic, restart-safe)
# =====================================================================

class FourPhaseStateStore:
    """One active cycle per symbol, persisted as bounded JSON.

    Atomic writes (temp + fsync + os.replace). Malformed/oversized state
    loads fail safely to empty. Save/prune failures are swallowed —
    state persistence can never affect dispatch.
    """

    def __init__(self, path=FOUR_PHASE_STATE_PATH):
        self.path = path
        self._records = {}
        self._last_save_ts = 0.0
        self._soft_dirty = False
        self._load()

    def _load(self):
        try:
            if not os.path.exists(self.path):
                return
            if os.path.getsize(self.path) > STATE_MAX_BYTES:
                print("[FOUR_PHASE_SHADOW] state file oversized; starting empty")
                return
            with open(self.path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if (
                isinstance(payload, dict)
                and payload.get("schema_version") == FOUR_PHASE_STATE_SCHEMA_VERSION
                and isinstance(payload.get("symbols"), dict)
            ):
                self._records = {
                    str(symbol): record
                    for symbol, record in payload["symbols"].items()
                    if isinstance(record, dict)
                }
        except Exception as exc:
            print("[FOUR_PHASE_SHADOW] state load failed (%s); starting empty" % exc)
            self._records = {}

    def get(self, symbol):
        record = self._records.get(symbol)
        return dict(record) if isinstance(record, dict) else None

    def put(self, symbol, record, hard=False, now_ts=None):
        try:
            self._records[str(symbol)] = record
            if hard:
                self.save(now_ts=now_ts)
            else:
                self._soft_dirty = True
                now_ts = now_ts if now_ts is not None else time.time()
                if now_ts - self._last_save_ts >= STATE_SOFT_SAVE_SECS:
                    self.save(now_ts=now_ts)
        except Exception:
            pass

    def prune(self, now_ts=None):
        now_ts = now_ts if now_ts is not None else time.time()
        stale = [
            symbol for symbol, record in self._records.items()
            if not isinstance(record, dict)
            or (now_ts - (_fp_float(record.get("last_seen_ts")) or 0)) > STATE_PRUNE_SECS
        ]
        for symbol in stale:
            self._records.pop(symbol, None)
        return len(stale)

    def save(self, now_ts=None):
        try:
            now_ts = now_ts if now_ts is not None else time.time()
            self.prune(now_ts=now_ts)
            payload = {
                "schema_version": FOUR_PHASE_STATE_SCHEMA_VERSION,
                "saved_at": now_ts,
                "symbols": self._records,
            }
            directory = os.path.dirname(os.path.abspath(self.path)) or "."
            fd, tmp_path = tempfile.mkstemp(
                prefix=".four_phase_state.", suffix=".tmp", dir=directory
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(payload, handle, default=str)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp_path, self.path)
            finally:
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
            self._last_save_ts = now_ts
            self._soft_dirty = False
        except Exception as exc:
            print("[FOUR_PHASE_SHADOW] state save failed (never affects dispatch): %s" % exc)

    def __len__(self):
        return len(self._records)


def get_default_store():
    global _DEFAULT_STORE
    with _lock:
        if _DEFAULT_STORE is None:
            _DEFAULT_STORE = FourPhaseStateStore()
        return _DEFAULT_STORE


# =====================================================================
# CYCLE STATE MACHINE
# =====================================================================

def _fresh_record(now_ts):
    return {
        "cycle_state": "NO_CONTEXT",
        "market_cycle_id": None,
        "range_high": None,
        "range_low": None,
        "range_width": None,
        "range_width_pct": None,
        "range_width_atr": None,
        "range_start_ts": None,
        "range_established_ts": None,
        "previous_wave": None,
        "false_break_count": 0,
        "pending_direction": None,
        "pending_bar_ts": None,
        "h1_departure_direction": None,
        "confirmed_phase": None,
        "phase_freeze_ts": None,
        "confirmed_breakout": None,
        "invalidated_reason": None,
        "last_breakout": None,
        "last_range_eval_state": None,
        "last_seen_ts": now_ts,
    }


def _h1_trailing_outside(h1_bars, range_high, range_low):
    """Trailing consecutive closed H1 closes beyond frozen bounds (+tol)."""
    up_bound = range_high * (1.0 + RANGE_CLOSE_TOL)
    down_bound = range_low * (1.0 - RANGE_CLOSE_TOL)
    up_run = 0
    down_run = 0
    for bar in reversed(h1_bars):
        close = _fp_float(bar.get("close"))
        if close is None:
            break
        if close > up_bound and down_run == 0:
            up_run += 1
        elif close < down_bound and up_run == 0:
            down_run += 1
        else:
            break
    if up_run:
        return "UP", up_run
    if down_run:
        return "DOWN", down_run
    return None, 0


def update_market_cycle(symbol, h1_bars, m5_bars, store, now_ts=None):
    """Advance one symbol's cycle state machine. LOG/STATE-ONLY.

    Callers in production must ignore the returned record.
    """
    now_ts = now_ts if now_ts is not None else time.time()
    record = store.get(symbol) or _fresh_record(now_ts)
    record["last_seen_ts"] = now_ts
    hard = False

    state = record.get("cycle_state") or "NO_CONTEXT"

    # PHASE_INVALIDATED collapses to NO_CONTEXT on the next scan; a new
    # cycle starts only after a fresh range establishment below.
    if state == "PHASE_INVALIDATED":
        record = _fresh_record(now_ts)
        state = "NO_CONTEXT"
        hard = True

    # Confirmed phase is immutable; hold through cooldown, then close the
    # cycle. Later scans can never relabel it.
    if state == "PHASE_CONFIRMED":
        freeze_ts = _fp_float(record.get("phase_freeze_ts")) or now_ts
        if now_ts - freeze_ts >= CYCLE_COOLDOWN_H1_BARS * H1_BAR_SECS:
            record = _fresh_record(now_ts)
            state = "NO_CONTEXT"
            hard = True
        else:
            store.put(symbol, record, hard=False, now_ts=now_ts)
            return record

    if state in ("RANGE_ESTABLISHED", "BREAK_PENDING"):
        established_ts = _fp_float(record.get("range_established_ts")) or now_ts
        range_high = _fp_float(record.get("range_high"))
        range_low = _fp_float(record.get("range_low"))

        if now_ts - established_ts >= RANGE_TTL_H1_BARS * H1_BAR_SECS:
            record["cycle_state"] = "PHASE_INVALIDATED"
            record["invalidated_reason"] = "TTL_EXPIRED"
            store.put(symbol, record, hard=True, now_ts=now_ts)
            return record

        if range_high is None or range_low is None:
            record["cycle_state"] = "PHASE_INVALIDATED"
            record["invalidated_reason"] = "CONTEXT_GAP_FROZEN_BOUNDS_LOST"
            store.put(symbol, record, hard=True, now_ts=now_ts)
            return record

        # H1 departure tracking against FROZEN bounds (never recomputed).
        if h1_bars:
            departure_dir, run = _h1_trailing_outside(h1_bars, range_high, range_low)
            prev_departure = record.get("h1_departure_direction")
            if departure_dir is None:
                if prev_departure in ("UP", "DOWN"):
                    record["false_break_count"] = int(record.get("false_break_count") or 0) + 1
                    record["h1_departure_direction"] = None
                    hard = True
            else:
                if run >= H1_DEPARTURE_INVALIDATE_BARS:
                    record["cycle_state"] = "PHASE_INVALIDATED"
                    record["invalidated_reason"] = "UNCONFIRMED_DEPARTURE_%s" % departure_dir
                    store.put(symbol, record, hard=True, now_ts=now_ts)
                    return record
                if prev_departure != departure_dir:
                    record["h1_departure_direction"] = departure_dir
                    hard = True

        breakout = evaluate_breakout_state(
            m5_bars,
            range_high,
            range_low,
            pending_direction=record.get("pending_direction"),
            pending_bar_ts=record.get("pending_bar_ts"),
            now_ts=now_ts,
        )
        record["last_breakout"] = {
            "breakout_state": breakout.get("breakout_state"),
            "breakout_direction": breakout.get("breakout_direction"),
            "break_close_beyond_atr": breakout.get("break_close_beyond_atr"),
            "body_acceptance": breakout.get("body_acceptance"),
            "wick_rejection": breakout.get("wick_rejection"),
            "break_bar_ts": breakout.get("break_bar_ts"),
        }

        b_state = breakout.get("breakout_state")
        if b_state in ("BREAK_UP_CONFIRMED", "BREAK_DOWN_CONFIRMED"):
            wave = (record.get("previous_wave") or {}).get("previous_wave_direction")
            record["cycle_state"] = "PHASE_CONFIRMED"
            record["confirmed_phase"] = map_four_phase(wave, b_state, True)
            record["phase_freeze_ts"] = now_ts
            record["confirmed_breakout"] = dict(record["last_breakout"])
            record["pending_direction"] = None
            record["pending_bar_ts"] = None
            hard = True
        elif b_state in ("FALSE_BREAK_UP", "FALSE_BREAK_DOWN"):
            record["cycle_state"] = "RANGE_ESTABLISHED"
            record["false_break_count"] = int(record.get("false_break_count") or 0) + 1
            record["pending_direction"] = None
            record["pending_bar_ts"] = None
            hard = True
        elif b_state in ("BREAK_UP_PENDING", "BREAK_DOWN_PENDING"):
            new_dir = breakout.get("breakout_direction")
            new_ts = breakout.get("break_bar_ts")
            if (
                record.get("cycle_state") != "BREAK_PENDING"
                or record.get("pending_direction") != new_dir
            ):
                record["cycle_state"] = "BREAK_PENDING"
                record["pending_direction"] = new_dir
                record["pending_bar_ts"] = new_ts
                hard = True
        # BREAK_NONE / BREAK_CONTEXT_MISSING: no transition.

        store.put(symbol, record, hard=hard, now_ts=now_ts)
        return record

    # NO_CONTEXT / RANGE_FORMING: candidate range replaced wholesale each
    # scan; nothing frozen yet.
    range_eval = evaluate_range_state(h1_bars)
    record["last_range_eval_state"] = range_eval.get("range_state")

    if range_eval.get("range_state") == "RANGE_ESTABLISHED":
        # Anti-pre-broken guard: closed M5 bars after the last closed H1
        # bar may already sit beyond the candidate boundaries; do not
        # establish a range that is already broken.
        cand_high = range_eval["range_high"]
        cand_low = range_eval["range_low"]
        last_m5_close = None
        if m5_bars:
            last_m5_close = _fp_float(m5_bars[-1].get("close"))
        already_broken = (
            last_m5_close is not None
            and not (
                cand_low * (1.0 - RANGE_CLOSE_TOL)
                <= last_m5_close
                <= cand_high * (1.0 + RANGE_CLOSE_TOL)
            )
        )
        if already_broken:
            record["cycle_state"] = "RANGE_FORMING"
        else:
            start_ts = range_eval.get("range_start_ts")
            pre_bars = []
            if h1_bars and start_ts is not None:
                pre_bars = [
                    bar for bar in h1_bars
                    if _fp_float(bar.get("time")) is not None
                    and _fp_float(bar.get("time")) < _fp_float(start_ts)
                ][-PREV_WAVE_BARS:]
            wave = evaluate_previous_wave(
                pre_bars,
                range_eval.get("range_width_pct"),
                range_start_ts=start_ts,
                now_ts=now_ts,
            )
            record.update({
                "cycle_state": "RANGE_ESTABLISHED",
                "market_cycle_id": build_market_cycle_id(
                    symbol, start_ts, cand_high, cand_low
                ),
                "range_high": cand_high,
                "range_low": cand_low,
                "range_width": range_eval.get("range_width"),
                "range_width_pct": range_eval.get("range_width_pct"),
                "range_width_atr": range_eval.get("range_width_atr"),
                "range_start_ts": start_ts,
                "range_established_ts": now_ts,
                "previous_wave": wave,
                "false_break_count": 0,
                "pending_direction": None,
                "pending_bar_ts": None,
                "h1_departure_direction": None,
                "confirmed_phase": None,
                "phase_freeze_ts": None,
                "confirmed_breakout": None,
                "invalidated_reason": None,
            })
            hard = True
    elif range_eval.get("range_state") == "RANGE_FORMING":
        record["cycle_state"] = "RANGE_FORMING"
    else:
        record["cycle_state"] = "NO_CONTEXT"

    store.put(symbol, record, hard=hard, now_ts=now_ts)
    return record


# =====================================================================
# SNAPSHOT ASSEMBLER + WRITER
# =====================================================================

def assemble_four_phase_snapshot(candidate, cycle, execution_mode="",
                                 action="", opened_trade_id=None, now_ts=None):
    """Build one decision-time context row. Pure; no I/O.

    Contains NO outcome data: no realized R, no first-hit, no favorable/
    adverse excursions, no terminal status, no future-candle fields — by
    construction and by simulator assertion.
    """
    candidate = candidate if isinstance(candidate, dict) else {}
    cycle = cycle if isinstance(cycle, dict) else {}
    now_ts = now_ts if now_ts is not None else time.time()

    state = cycle.get("cycle_state") or "NO_CONTEXT"
    established = state in ("RANGE_ESTABLISHED", "BREAK_PENDING", "PHASE_CONFIRMED")
    wave = cycle.get("previous_wave") or {}
    wave_dir = wave.get("previous_wave_direction")

    if state == "PHASE_CONFIRMED":
        breakout = cycle.get("confirmed_breakout") or {}
    else:
        breakout = cycle.get("last_breakout") or {}
    breakout_state = breakout.get("breakout_state") or (
        "BREAK_NONE" if established else "BREAK_CONTEXT_MISSING"
    )

    confirmed = cycle.get("confirmed_phase") if state == "PHASE_CONFIRMED" else None
    candidate_phase = (
        confirmed
        if confirmed is not None
        else map_four_phase(wave_dir, breakout_state, established)
    )

    side = str(candidate.get("side") or "").upper()
    if confirmed in ("ACCUMULATION_CONFIRMED", "REACCUMULATION_CONFIRMED"):
        entry_side_relation = "PHASE_ALIGNED" if side == "LONG" else "PHASE_OPPOSED"
    elif confirmed in ("DISTRIBUTION_CONFIRMED", "REDISTRIBUTION_CONFIRMED"):
        entry_side_relation = "PHASE_ALIGNED" if side == "SHORT" else "PHASE_OPPOSED"
    elif confirmed == "PHASE_UNKNOWN":
        entry_side_relation = "PHASE_UNKNOWN"
    elif established:
        entry_side_relation = "PHASE_PENDING"
    else:
        entry_side_relation = "PHASE_UNKNOWN"

    phase_confidence = None
    if confirmed in _FOUR_PHASES:
        clean = (
            wave.get("structural_agreement") is True
            and int(cycle.get("false_break_count") or 0) == 0
        )
        phase_confidence = "CLEAN" if clean else "PARTIAL"

    range_start_ts = _fp_float(cycle.get("range_start_ts"))
    established_ts = _fp_float(cycle.get("range_established_ts"))
    range_age_bars = (
        int((now_ts - range_start_ts) // H1_BAR_SECS)
        if range_start_ts is not None else None
    )
    cycle_age_bars = (
        int((now_ts - established_ts) // H1_BAR_SECS)
        if established_ts is not None else None
    )

    missing_fields = ["retest_status"]
    if not established:
        missing_fields.append("range_context")
    if breakout_state == "BREAK_CONTEXT_MISSING":
        missing_fields.append("breakout_context")
    if wave_dir in (None, "PREV_WAVE_MISSING"):
        missing_fields.append("previous_wave")

    if not cycle or cycle.get("last_range_eval_state") is None and not established:
        context_quality = "MISSING"
    elif (
        established
        and breakout_state != "BREAK_CONTEXT_MISSING"
        and wave_dir in ("PREV_WAVE_UP", "PREV_WAVE_DOWN")
    ):
        context_quality = "FULL"
    else:
        context_quality = "PARTIAL"

    return {
        "schema_version": FOUR_PHASE_SCHEMA_VERSION,
        "event_type": FOUR_PHASE_EVENT_TYPE,
        "logged_at": now_ts,
        "decision_ts": now_ts,
        "execution_mode": execution_mode,
        "action": action,
        "symbol": candidate.get("symbol"),
        "side": side or None,
        "entry_type": candidate.get("entry_type"),
        "signal_key": candidate.get("dedup_key"),
        "candidate_id": candidate.get("candidate_id"),
        "dedup_key": candidate.get("dedup_key"),
        "source_timestamp": candidate.get("source_timestamp"),
        "opened_trade_id": opened_trade_id,
        "market_cycle_id": cycle.get("market_cycle_id"),
        "previous_wave_direction": wave_dir,
        "previous_wave_source_tf": wave.get("previous_wave_source_tf"),
        "previous_wave_strength": wave.get("previous_wave_strength"),
        "previous_wave_freshness_sec": (
            (now_ts - range_start_ts) if range_start_ts is not None else None
        ),
        "range_state": (
            state if state in ("RANGE_ESTABLISHED", "BREAK_PENDING") else
            "RANGE_ESTABLISHED" if state == "PHASE_CONFIRMED" else
            cycle.get("last_range_eval_state") or "RANGE_CONTEXT_MISSING"
        ),
        "range_high": cycle.get("range_high"),
        "range_low": cycle.get("range_low"),
        "range_width": cycle.get("range_width"),
        "range_width_atr": cycle.get("range_width_atr"),
        "range_age_bars": range_age_bars,
        "breakout_state": breakout_state,
        "breakout_direction": breakout.get("breakout_direction"),
        "break_close_beyond_atr": breakout.get("break_close_beyond_atr"),
        "body_acceptance": breakout.get("body_acceptance"),
        "wick_rejection": breakout.get("wick_rejection"),
        "retest_status": None,
        "market_phase_candidate": candidate_phase,
        "market_phase_confirmed": confirmed,
        "phase_freeze_ts": cycle.get("phase_freeze_ts"),
        "phase_confidence": phase_confidence,
        "entry_side_relation": entry_side_relation,
        "missing_fields": missing_fields,
        "context_quality": context_quality,
        "false_break_count": int(cycle.get("false_break_count") or 0),
        "cycle_state": state,
        "cycle_age_bars": cycle_age_bars,
    }


def append_four_phase_shadow_row(row, log_path=None):
    """Append one row to the shadow forward log. Failure is swallowed."""
    try:
        path = log_path or FOUR_PHASE_LOG_PATH
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        return True
    except Exception as exc:
        print("[FOUR_PHASE_SHADOW] log append failed (never affects dispatch): %s" % exc)
        return False


# =====================================================================
# PRODUCTION ENTRY POINTS (fully failure-isolated; returns ignored)
# =====================================================================

def update_market_cycle_from_frames(symbol, df1h, df5, now_ts=None):
    """Per-symbol cycle state update from scan DataFrames. LOG-ONLY.

    Swallows every exception; production callers must ignore the return.
    """
    try:
        h1_bars = bars_tail_from_df(df1h, RANGE_WINDOW_BARS + PREV_WAVE_BARS + 2)
        m5_bars = bars_tail_from_df(df5, ATR_BARS + FALSE_BREAK_M5_INSIDE_BARS + 4)
        store = get_default_store()
        return update_market_cycle(symbol, h1_bars, m5_bars, store, now_ts=now_ts)
    except Exception as exc:
        try:
            print("[FOUR_PHASE_SHADOW] cycle update failed (log-only): %s" % exc)
        except Exception:
            pass
        return None


def log_four_phase_snapshot(candidate, execution_mode="", action="",
                            opened_trade_id=None, now_ts=None):
    """Assemble + append one decision snapshot row. LOG-ONLY.

    Reads the symbol's cycle record without mutating it. Swallows every
    exception; production callers must ignore the return value.
    """
    try:
        candidate = candidate if isinstance(candidate, dict) else {}
        symbol = candidate.get("symbol")
        store = get_default_store()
        cycle = store.get(symbol) if symbol else None
        row = assemble_four_phase_snapshot(
            candidate,
            cycle,
            execution_mode=execution_mode,
            action=action,
            opened_trade_id=opened_trade_id,
            now_ts=now_ts,
        )
        append_four_phase_shadow_row(row)
        return row
    except Exception as exc:
        try:
            print("[FOUR_PHASE_SHADOW] snapshot failed (log-only): %s" % exc)
        except Exception:
            pass
        return None
