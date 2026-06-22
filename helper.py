import time
signal_state = {} 

SWING_TOP_N        = 7
MAX_EARLY_PER_CYCLE = 2   # tránh flood EARLY cùng lúc
SWING_RR_MIN       = 3.0
SWING_RISK_MULT    = 0.5      # 0.5x normal risk
COMPRESS_MIN_SCORE = 3
COMPRESS_EXPIRE_H  = 24
H1_WINDOW = 20
H4_WINDOW = 30

def check_correlation(trades, new_side, max_same_side=7):
    """
    [UPGRADE] Không mở quá N lệnh cùng chiều cùng lúc.
    Tránh rủi ro tập trung khi market đảo chiều đột ngột.
    """
    same_side = [t for t in trades if t["status"] == "OPEN" and t["side"] == new_side]
    return len(same_side) < max_same_side

def check_signal_cooldown(symbol, direction, cooldown_sec=900, signal_state_dict=None):
    """
    Returns True if signal is allowed (not spam).
    False if same symbol+direction within cooldown AND last signal was executed.

    signal_state_dict: executor-local dict (ctx.signal_state) when called from
                       executor-aware path; falls back to global signal_state
                       for single-executor backward compatibility.
    """
    _state = signal_state_dict if signal_state_dict is not None else signal_state
    now = time.time()
    prev = _state.get(symbol, {})
    last_exists = bool(prev)
    executed = prev.get("executed", False)
    print(f"[SPAM CHECK] {symbol} last_signal={last_exists} executed={executed}")
    if (prev.get("direction") == direction and
            now - prev.get("time", 0) < cooldown_sec and
            executed):
        return False
    return True

def _should_log(symbol, log_type, reason=""):
    """
    Returns True nếu nên log (chưa log hoặc đã qua cooldown).
    Nếu same symbol+type+reason trong LOG_COOLDOWN → skip.
    Nếu same symbol+type nhưng KHÁC reason → cho phép (Part 7).
    """
    key = (symbol, log_type, str(reason))
    now = time.time()
    last = log_cache.get(key, 0)
    if now - last < LOG_COOLDOWN:
        return False
    log_cache[key] = now
    return True

def cleanup_watchlist():
    now    = time.time()
    remove = [sym for sym, w in compression_watchlist.items()
              if now - w["timestamp"] > COMPRESS_EXPIRE_H * 3600]
    for sym in remove:
        del compression_watchlist[sym]

def dynamic_tp_multiplier(vol):
    return 1.5 + vol * 2

def dynamic_giveback(vol):
    return 0.6 + vol * 0.2

def dynamic_phase_trigger(vol):
    return {
        "phase2": 1 + vol * 1.2,
        "phase3": 2 + vol * 1.5
    }

def dynamic_tp_mode(vol):
    return "SOFT" if vol > 0.015 else "HARD"

# =====================================================================
# POOL SELECTION SYSTEM — v8
# Implements: SCAN → COMPRESSION POOL → PRE-BREAK POOL → TREND POOL
#             → MERGE+DEDUP+SCORE → FINAL CONFIRM POOL (~25-30)
# DO NOT redesign: chỉ add structured pool processing on top.
# =====================================================================

# ----- Constants (không đổi CONFIG gốc) -----
POOL_SCAN_SIZE_MIN  = 120   # Tier A + B target
POOL_SCAN_SIZE_MAX  = 150
POOL_COMPRESS_MAX   = 80    # [FIX] 60→80: cần nhiều hơn để fill confirm
POOL_PREBREAK_MAX   = 40    # [FIX] 25→40: loosened
POOL_TREND_MAX      = 60    # [FIX] 50→60: loosened
POOL_CONFIRM_MAX    = 50    # [FIX] 30→50: target 40-50 coin sau filter
POOL_CAP_COMPRESS   = 25    # [FIX] 15→25: allow more compression into confirm
POOL_CAP_TREND      = 30    # [FIX] 20→30: allow more trend into confirm

compression_watchlist = {}
# [ADD] Track compression detected alerts — tách riêng khỏi watchlist
# Tồn tại dù symbol bị xóa khỏi watchlist, tránh re-alert khi tạo lại
# key = symbol, value = {"range_high", "range_low", "score", "time"}
compression_alert_sent = {}
# schema per symbol:
# {

# [ADD] Persistent ranking state — anti-spam
ranking_state = {
    "last_top_symbols": [],
    "last_scores":      {},   # {symbol: rank_score}
    "last_alert_time":  0,
}
#   "range_high": float,
#   "range_low":  float,
#   "score":      int,
#   "priority":   float,
#   "timestamp":  float,      # time.time()
#   "phase":      "compress" | "breakout" | "retest",
#   "breakout_dir": "LONG" | "SHORT" | None,
# }
log_cache = {}
LOG_COOLDOWN = 300  # 5 phút — cùng symbol+type+reason không log lại

# ===== STATS =====
stats = {
    "total": 0,
    "ema_fail": 0,
    "bos_fail": 0,
    "retest_fail": 0,
    "candle_fail": 0,
    "core_fail": 0,
    "entry_type_stats": {},
    "bos_type_stats": {},
    "wyckoff_stats": {},
    "market_mode_stats": {},

    "pass": 0,

    # 🔥 ADD MỚI
    "sent": 0,
    "entry": 0,
    "win": 0,
    "loss": 0,

    # debug filter
    "reject_price": 0,
    "reject_score": 0,
    "reject_hard_filter": 0,
    

    # v6.6
    "exhaustion_stats": {},

    # [DEBUG] silent reject
    "fail_vwap":       0,
    "fail_adx":        0,
    "fail_funding":    0,
    "fail_tier":       0,
    "fail_exhaustion": 0,
    "fail_wyckoff":    0,
    "fail_sl":            0,
    "fail_rr":            0,
    "fail_reversal_mode": 0,
    "fail_overextended":  0,
    "fail_confirm":       0,
    "fail_volume":        0,
    "fail_conflict":      0,

    # [EARLY]
    "early_detected":           0,
    "early_pass":               0,
    "early_block_no_repeat":    0,
    "early_block_volume":       0,
    "early_block_exhaustion":   0,
    "early_block_dist":         0,
    "early_block_pp": 0,   # pp < 0.70  
    "early_block_sl_rr": 0,    

    "swing_pass":        0,
    "swing_compress_new":0,   # số symbol mới vào watchlist

    # [v8] Breakout classification stats (Prompt 2 Part 6)
    "break_true":  0,
    "break_fake":  0,
    "break_trap":  0,

    # [v8] Pool pipeline stats
    "pool_compress_size": 0,
    "pool_prebreak_size": 0,
    "pool_trend_size":    0,
    "pool_confirm_size":  0,
    "early_block_state": 0,
    "early_block_breakout": 0,
}

def is_duplicate_zone(symbol, side, entry, threshold=0.003, cooldown=900):
    """
    Check duplicate entry cùng vùng giá + cùng hướng trong 15 phút
    threshold: 0.003 = 0.3%
    cooldown: 900s = 15 phút
    """
    last = signal_state.get(symbol)

    if not last:
        return False

    # check direction
    if last["direction"] != side:
        return False

    # check time
    if time.time() - last["time"] > cooldown:
        return False

    # check price zone
    price_diff = abs(entry - last["price"]) / last["price"]

    if price_diff < threshold:
        print(f"[SKIP DUPLICATE] {symbol} diff={price_diff:.4f}")
        return True

    return False

  
def validate_early(meta):
    # HARD GATE 1: gần level
    if meta["dist"] > 0.02:   # [FIX] 0.005→0.02: 85% bị block vì dist quá chặt
        return False, {**meta, "block": "dist"}

    # HARD GATE 2: không vào cực đoan của range
    if meta["pp"] >= 0.5:
        return False, {**meta, "block": "pp_low"}

    # HARD GATE 3: không exhaustion cực đoan
    # EXTENDED vẫn cho qua — chỉ block EXHAUSTED và COLLAPSING
    if meta["exhaustion"] in ("EXHAUSTED", "COLLAPSING"):
        return False, {**meta, "block": "exhausted"}

    # SOFT: volume thấp → penalty vào meta, KHÔNG reject
    # SOFT: funding bất lợi → penalty vào meta, KHÔNG reject
    # Không có gate nào khác

    return True, meta  

def ensure_columns(row, headers):
    for h in headers:
        if h not in row:
            row[h] = ""
    return row