import os
import time
import csv
from datetime import datetime, timezone, timedelta
from config import ACCOUNT_BALANCE, RISK_PER_TRADE, config
MAX_TOTAL_RISK = 0.05
MAX_RISK_PER_SYMBOL = 0.02
MAX_TRADES_PER_SYMBOL = 2

from telegram import normalize_message_text, send_telegram
VN_TZ = timezone(timedelta(hours=7))
def format_vn_time(ts):
    return datetime.fromtimestamp(ts, VN_TZ).strftime("%H:%M %d-%m")

def _safe_print_message(msg):
    msg = normalize_message_text(msg)
    print(msg)

def fmt_price(price, symbol):
    """
    Format giá adaptive theo giá trị thực tế.
    Không cứng theo tên coin vì cùng 1 coin giá có thể thay đổi lớn.
    """
    if price >= 10000:
        return f"{price:,.1f}"      # BTC: 43,250.1
    elif price >= 100:
        return f"{price:,.2f}"      # ETH: 2,850.12
    elif price >= 1:
        return f"{price:,.3f}"      # SOL: 125.430
    elif price >= 0.01:
        return f"{price:,.4f}"      # DOGE: 0.1234
    elif price >= 0.0001:
        return f"{price:,.6f}"      # SHIB: 0.000024
    else:
        return f"{price:.8f}"       # PEPE và coin siêu nhỏ

def fmt_pnl(rr_real, risk_amt):
    """Format PnL với dấu +/- rõ ràng"""
    dollar = risk_amt * rr_real
    sign = "+" if rr_real >= 0 else ""
    if abs(dollar) < 0.01:
        dollar_str = f"{dollar:.4f}"
    else:
        dollar_str = str(round(dollar, 2))
    return f"{sign}{round(rr_real, 2)}R ({sign}{dollar_str}$)"

def _safe_float(value, default=None):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

def paper_side_icon(side):
    side = str(side or "").upper()
    if side == "LONG":
        return "🟢"
    if side == "SHORT":
        return "🔴"
    return ""

def paper_outcome_icon(status=None, rr=None):
    status = str(status or "").upper()
    rr_value = _safe_float(rr, None)
    if rr_value is not None:
        if abs(rr_value) <= 0.01:
            return "⚖️"
        if rr_value > 0:
            return "✅"
        if rr_value < 0:
            return "❌"
    if status in ("WIN", "TP"):
        return "✅"
    if status in ("BE", "BREAKEVEN"):
        return "⚖️"
    if status in ("LOSS", "LOSE", "SL"):
        return "❌"
    return "🎯"

def paper_engine_name(t):
    entry_type = str(t.get("entry_type") or "")
    strategy_family = str(t.get("strategy_family") or "")
    if entry_type == "PAPER_SMC_MAIN" or strategy_family == "paper_smc_main":
        return "SMC-MAIN"
    if entry_type == "CONFIRM_SMC_RESEARCH" or strategy_family == "confirm_smc_research":
        return "SMC-RESEARCH"
    return engine_label(t).replace("PAPER • ", "")

def _paper_first_value(t, *keys, default=None):
    for key in keys:
        value = t.get(key)
        if value not in (None, ""):
            return value
    return default

def _paper_close_reason_text(reason, rr_value):
    if str(reason or "").upper() == "SL" and _safe_float(rr_value, 0.0) > 0:
        return "Chốt lời bằng SL"
    return reason

def _paper_score_text(value):
    score = _safe_float(value, None)
    if score is None:
        return "?"
    return str(round(score, 1))

def _paper_reason_text(t, *keys, default="UNKNOWN"):
    for key in keys:
        value = t.get(key)
        if isinstance(value, (list, tuple)):
            text = format_reason(value)
            if text:
                return text
        elif value not in (None, ""):
            return str(value)
    return default

def _format_paper_smc_entry_legacy(t, engine):
    score_key = "effective_score" if engine == "SMC-MAIN" else "score"
    score = _paper_score_text(_paper_first_value(
        t,
        score_key,
        "score_v2_structural_shadow",
        "score_v2",
        "score",
        default=0,
    ))
    risk_amt = t.get("balance_at_entry", ACCOUNT_BALANCE) * t.get("risk_percent", RISK_PER_TRADE)
    reason = (
        _paper_reason_text(t, "candidate_type", "source_reason", "original_reason")
        if engine == "SMC-MAIN"
        else _paper_reason_text(t, "original_reason", "source_reason", "candidate_type", "reason")
    )
    lines = [
        f"{paper_side_icon(t.get('side'))} {t.get('symbol')} ({score}) {t.get('side')} | {engine}",
        (
            f"E {fmt_price(t.get('entry_real', t.get('entry', 0)), t['symbol'])}  "
            f"SL {fmt_price(t.get('sl_real', t.get('sl', 0)), t['symbol'])}  "
            f"TP {fmt_price(t.get('tp', 0), t['symbol'])}"
        ),
        f"📊 RR {round(t.get('rr', 0), 2)} | Risk {round(t.get('risk_percent', RISK_PER_TRADE) * 100, 1)}% (~{round(risk_amt, 2)}$)",
    ]
    if engine == "SMC-MAIN":
        modifier = _paper_first_value(t, "structural_modifier", default=0)
        effective = _paper_score_text(_paper_first_value(
            t,
            "effective_score",
            "score_v2_structural_shadow",
            "score_v2",
            "score",
            default=0,
        ))
        lines.append(f"⭐ {reason} | mod {modifier} | eff {effective}")
        if t.get("boundary_guard_applied"):
            lines.append("🧲 Boundary rescued")
    else:
        lines.append(f"⭐ {reason}")
    return "\n".join(lines)

def format_paper_smc_entry(t, engine):
    score_key = "effective_score" if engine == "SMC-MAIN" else "score"
    score = _paper_score_text(_paper_first_value(
        t,
        score_key,
        "score_v2_structural_shadow",
        "score_v2",
        "score",
        default=0,
    ))
    risk_percent = round(t.get("risk_percent", RISK_PER_TRADE) * 100, 1)
    risk_amt = t.get("balance_at_entry", ACCOUNT_BALANCE) * t.get("risk_percent", RISK_PER_TRADE)
    reason = (
        _paper_reason_text(t, "candidate_type", "source_reason", "original_reason")
        if engine == "SMC-MAIN"
        else _paper_reason_text(t, "original_reason", "source_reason", "candidate_type", "reason")
    )
    lines = [
        f"{paper_side_icon(t.get('side'))} {t.get('symbol')} ({score}) {t.get('side')} | {engine}",
        (
            f"🚀 {fmt_price(t.get('entry_real', t.get('entry', 0)), t['symbol'])}   "
            f"💀 {fmt_price(t.get('sl_real', t.get('sl', 0)), t['symbol'])}   "
            f"💰 {fmt_price(t.get('tp', 0), t['symbol'])}"
        ),
    ]
    if engine == "SMC-MAIN":
        modifier = _paper_first_value(t, "structural_modifier", default=0)
        effective = _paper_score_text(_paper_first_value(
            t,
            "effective_score",
            "score_v2_structural_shadow",
            "score_v2",
            "score",
            default=0,
        ))
        lines.append(f"📊 RR {round(t.get('rr', 0), 2)} | Risk {risk_percent}% (~{round(risk_amt, 2)}$)")
        reason_line = f"⭐ {reason} | mod {modifier} | eff {effective}"
        if t.get("boundary_guard_applied"):
            reason_line += " | 🧲 Boundary rescued"
        lines.append(reason_line)
    else:
        lines.append(f"📊 RR {round(t.get('rr', 0), 2)} | Risk {risk_percent}%")
        lines.append(f"⭐ {reason}")
    return normalize_message_text("\n".join(lines))

def _paper_duration_text(t, row=None):
    row = row or {}
    minutes = _safe_float(t.get("trade_age_minutes"), None)
    if minutes is None:
        duration_secs = _safe_float(row.get("duration_secs"), None)
        if duration_secs is not None:
            minutes = duration_secs / 60.0
    if minutes is None:
        return "UNKNOWN"
    return f"{round(minutes / 60, 1)}h" if minutes >= 60 else f"{round(minutes):.0f}m"

def format_paper_smc_close(t, engine=None, row=None, close_reason=None):
    row = row or {}
    rr = _paper_first_value(t, "rr_real", "pnl_r", default=row.get("r_multiple", 0))
    rr_value = _safe_float(rr, 0.0)
    mfe = _safe_float(_paper_first_value(t, "max_profit_r", default=row.get("mfe_r")), 0.0)
    giveback = _paper_first_value(t, "giveback_r", default=row.get("giveback_r"))
    reason = close_reason or row.get("close_reason") or t.get("close_reason") or t.get("exit_type") or "UNKNOWN"
    reason = normalize_message_text(_paper_close_reason_text(reason, rr_value))
    close_price = _paper_first_value(t, "exit_price", default=row.get("close_price", "UNKNOWN"))
    symbol = t.get("symbol") or row.get("symbol")
    side = t.get("side") or row.get("side") or ""
    engine = engine or paper_engine_name(t)
    rr_text = f"+{round(rr_value, 2)}R" if rr_value > 0 else f"{round(rr_value, 2)}R"
    lines = [
        f"{paper_outcome_icon(t.get('status'), rr_value)} {symbol} {side} {rr_text} | {engine}",
        f"Exit: {reason} @ {close_price if close_price == 'UNKNOWN' else fmt_price(_safe_float(close_price, 0), symbol)}",
    ]
    age = _paper_duration_text(t, row)
    if giveback not in (None, ""):
        lines.append(f"Max: {round(mfe, 2)}R | GB: {round(_safe_float(giveback, 0.0), 2)}R | Age: {age}")
    else:
        lines.append(f"Max: {round(mfe, 2)}R | Age: {age}")
    return normalize_message_text("\n".join(lines))

def highlight_tag(t, rr):
    score = t["score"]

    # 🔥 SUPER
    if score >= 12:
        return "🔥"

    # 🟢 KÈO NGON (ưu tiên trade)
    reason_str = ",".join(t["reason"])
    if score >= 9 and rr >= 1.5 and "Retest STRONG" in reason_str:
        return "🟢"

    # ⚠️ REVERSAL (cảnh báo)
    if t["entry_type"].startswith("REVERSAL"):
        return "⚠️"

    return ""

def vi_entry_type(entry_type):
    mapping = {
        "EARLY": "Early",
        "CONFIRM": "Confirm",
        "RETEST": "Retest",
        "RETEST_STRONG": "Retest_strong",
        "REVERSAL_EARLY": "Reversal_early",
        "REVERSAL_CONFIRM": "Reversal_confirm",
        "TREND_CONFIRM":    "Trend_confirm",
        "TREND_LIMIT":      "Trend_limit",
        "REVERSAL_LIMIT":   "Reversal_limit",
        "EARLY_TIER0":  "Tier_0",
        "SWING_BREAK":  "Swing Breakout",
        "SWING_RETEST": "Swing Retest",
        "EARLY_CONT":   "Early_Continuation",
    }
    return mapping.get(entry_type, entry_type)

def engine_label(t, live_mode=False):
    entry_type = str(t.get("entry_type") or "")
    strategy_family = str(t.get("strategy_family") or "")
    if entry_type == "PAPER_SMC_MAIN" or strategy_family == "paper_smc_main":
        return "PAPER • SMC-MAIN"
    if entry_type == "CONFIRM_SMC_RESEARCH" or strategy_family == "confirm_smc_research":
        return "PAPER • SMC-RESEARCH"
    if live_mode:
        return "LIVE • CONFIRM"
    if entry_type == "CONFIRM":
        return "PAPER • CONFIRM"
    return vi_entry_type(entry_type)

def format_reason(reason):
    """
    [v8] REASON BUG FIX — never return empty string khi có reason content.
    Always include context: retest, volume, exhaustion, wyckoff, etc.
    If no reason → return "" (caller decides to hide ⭐ line entirely).
    """
    if not reason:
        return ""

    parts = [r.strip() for r in reason if r and r.strip()]
    if not parts:
        return ""

    core    = []
    confirm = []
    special = []
    context = []

    for p in parts:
        # ===== CORE =====
        if any(k in p for k in ("Retest", "BOS STRONG", "Spring", "SPRING", "UPTHRUST")):
            core.append(p)
        # ===== CONFIRM =====
        elif p in ("Strong", "Engulf", "Pin", "Volume OK", "Cont"):
            confirm.append(p)
        # ===== SPECIAL / RISK =====
        elif any(k in p for k in ("Volume Spike", "FakeBreak", "Late", "Overextended", "Exhausted")):
            special.append(p)
        # ===== CONTEXT (wyckoff, exhaustion, tier tags, etc.) =====
        elif any(k in p for k in ("H1", "M15", "BOS", "Tier", "Trend", "Reversal",
                                   "Wyckoff", "SPRING", "UPTHRUST", "ACCUMULATION",
                                   "retest", "volume", "exhaustion", "Momentum")):
            context.append(p)

    # Build result: core → confirm → special → context (each category limited)
    result = core[:2] + confirm[:2] + special[:1] + context[:1]

    # Safety: nếu categories lọc không còn gì nhưng parts có nội dung
    # → lấy trực tiếp các phần tử đầu
    if not result and parts:
        result = parts[:3]

    final = " | ".join(result)
    return final if final.strip() else ""

def send_entry(t, prefix=None):
    if (
        (t.get("entry_type") == "PAPER_SMC_MAIN" or t.get("strategy_family") == "paper_smc_main")
        and not bool(config.get("paper_smc_main_notify_open", True))
    ):
        print(f"[PAPER SMC MAIN ENTRY TELEGRAM SUPPRESSED] {t.get('symbol')} {t.get('side')}")
        return

    if t.get("entry_type") == "PAPER_SMC_MAIN" or t.get("strategy_family") == "paper_smc_main":
        msg = format_paper_smc_entry(t, "SMC-MAIN")
        _safe_print_message(msg)
        send_telegram(msg, prefix=prefix)
        return

    if t.get("entry_type") == "CONFIRM_SMC_RESEARCH" or t.get("strategy_family") == "confirm_smc_research":
        msg = format_paper_smc_entry(t, "SMC-RESEARCH")
        _safe_print_message(msg)
        send_telegram(msg, prefix=prefix)
        return

    rr = t["rr"]
    risk_amt = t.get("balance_at_entry", ACCOUNT_BALANCE) * t.get("risk_percent", RISK_PER_TRADE)
    icon = highlight_tag(t, rr) or ""
    entry_vi = engine_label(t)
    score = round(t.get("score_v2", t.get("score", 0)), 1)

    if t["tp_mode"] == "HARD":
        tp_mode_str = "HARD"
    elif t["tp_mode"] == "SOFT":
        tp_mode_str = "SOFT → trailing"
    else:
        tp_mode_str = "MIX"

    reason_str = format_reason(t.get("reason", []))
    if reason_str and len(reason_str.strip()) > 0:
        reason_line = f"⭐ {reason_str}"
    else:
        reason_line = ""

    direction_icon = "🟢" if t["side"] == "LONG" else "🔴"

    if t.get("entry_type") == "PAPER_SMC_MAIN" or t.get("strategy_family") == "paper_smc_main":
        details = [
            f"🟢 {entry_vi} • ENTRY",
            f"{t.get('symbol')} {t.get('side')}",
            f"Score: {round(t.get('score_v2', t.get('score', 0)), 1)}",
        ]
        if t.get("structural_modifier") not in (None, ""):
            details.append(f"Structural modifier: {t.get('structural_modifier')}")
        if t.get("candidate_type"):
            details.append(f"Candidate: {t.get('candidate_type')}")
        if t.get("boundary_guard_applied"):
            details.append("Boundary Guard: applied")
        details.extend([
            f"E: {fmt_price(t.get('entry_real', t.get('entry', 0)), t['symbol'])}  "
            f"SL: {fmt_price(t.get('sl_real', t.get('sl', 0)), t['symbol'])}  "
            f"TP: {fmt_price(t.get('tp', 0), t['symbol'])}",
            f"RR: {round(rr, 2)}",
        ])
        msg = "\n".join(details)
        _safe_print_message(msg)
        send_telegram(msg, prefix=prefix)
        return

    if t.get("entry_type") == "EARLY_CONT":
        cont_score = t.get("cont_score", "?")
        incubation_line = f"🟡 EARLY_CONTINUATION [INCUBATING] | cont_conf={cont_score}"
        price_line = (
            f"🚀 {fmt_price(t['entry_real'], t['symbol'])}  "
            f"☠️ {fmt_price(t['sl_real'], t['symbol'])}  "
            f"💰 {fmt_price(t['tp'], t['symbol'])}"
        )
        rr_line = (
            f"📊 RR {round(t['rr'], 2)} | Risk {round(t['risk_percent']*100, 1)}% "
            f"(~{round(risk_amt, 2)}$)"
        )
        direction_icon = "🟢" if t["side"] == "LONG" else "🔴"
        msg = (
            f"{direction_icon} {t['symbol']} ({score}) {t['side']}\n"
            f"{incubation_line}\n"
            f"{price_line}\n"
            f"{rr_line}"
        )
        _safe_print_message(msg)
        send_telegram(msg, prefix=prefix)
        return

    if t.get("entry_mode") == "SWING":
        early_tag = " ✦Early" if "EarlyConfirm" in t.get("reason", []) else ""
        phase_vi  = "Breakout" if t["entry_type"] == "SWING_BREAK" else "Retest"
        priority  = t.get("swing_meta", {}).get("priority", 0)
        swing_reason = format_reason(t.get("reason", []))
        swing_reason_line = f"📦 Priority {round(priority,1)} | {swing_reason}" if swing_reason else f"📦 Priority {round(priority,1)}"
        msg = (
            f"{direction_icon} 🧠 SWING {t['symbol']} ({t['score']}) {t['side']} | {phase_vi}{early_tag}\n"
            f"🚀 {fmt_price(t['entry_real'], t['symbol'])}  "
            f"☠️ {fmt_price(t['sl_real'], t['symbol'])}  "
            f"💰 {fmt_price(t['tp'], t['symbol'])} ({tp_mode_str})\n"
            f"📊 RR {round(rr, 2)} | Risk {round(t['risk_percent']*100, 1)}% (~{round(risk_amt, 2)}$)\n"
            f"{swing_reason_line}"
        )
    else:
        price_line = (
            f"🚀 {fmt_price(t['entry_real'], t['symbol'])}  "
            f"☠️ {fmt_price(t['sl_real'], t['symbol'])}  "
            f"💰 {fmt_price(t['tp'], t['symbol'])} ({tp_mode_str})"
        )
        rr_line = (
            f"📊 RR {round(rr, 2)} | Risk {round(t['risk_percent']*100, 1)}% "
            f"(~{round(risk_amt, 2)}$)"
        )
        if reason_line:
            msg = (
                f"{direction_icon} {icon} {t['symbol']} ({score}) {t['side']} | {entry_vi}\n"
                f"{price_line}\n"
                f"{rr_line}\n"
                f"{reason_line}"
            )
        else:
            msg = (
                f"{direction_icon} {icon} {t['symbol']} ({score}) {t['side']} | {entry_vi}\n"
                f"{price_line}\n"
                f"{rr_line}"
            )
    _safe_print_message(msg)
    send_telegram(msg, prefix=prefix)


def send_exit(t, prefix=None):
    rr_real  = t.get("rr_real", 0)
    risk_amt = ACCOUNT_BALANCE * t["risk_percent"]
    direction_icon = "🟢" if t["side"] == "LONG" else "🔴"
    exit_icon = "✅" if rr_real >= 0 else "❌"
    pnl_str = fmt_pnl(rr_real, risk_amt)
    msg = (
        f"{exit_icon} {direction_icon} {t['symbol']} {t['side']} | CLOSED\n"
        f"🏁 {fmt_price(t.get('exit_price', 0), t['symbol'])}  "
        f"☠️ {fmt_price(t.get('sl_real', t.get('sl',0)), t['symbol'])}\n"
        f"📊 {pnl_str} | Exit: {t.get('exit_type','')}"
    )
    _safe_print_message(msg)
    send_telegram(msg, prefix=prefix)


def send_tp_break(t, prefix=None):
    rr_real  = t.get("rr_real", 0)
    risk_amt = ACCOUNT_BALANCE * t["risk_percent"]
    direction_icon = "🟢" if t["side"] == "LONG" else "🔴"
    pnl_str = fmt_pnl(rr_real, risk_amt)
    msg = (
        f"💰 {direction_icon} {t['symbol']} {t['side']} | TP HIT\n"
        f"🏁 {fmt_price(t.get('exit_price', 0), t['symbol'])}  "
        f"☠️ {fmt_price(t.get('sl_real', t.get('sl',0)), t['symbol'])}\n"
        f"📊 {pnl_str} | Exit: {t.get('exit_type','')}"
    )
    _safe_print_message(msg)
    send_telegram(msg, prefix=prefix)


def format_testnet_entry(t, sl_synced):
    direction_icon = "🟢" if t["side"] == "LONG" else "🔴"
    entry_price = fmt_price(
        t.get("exchange_fill_price") or t.get("entry_real") or t.get("entry", 0),
        t["symbol"],
    )
    sl_line = "SL synced ✅" if sl_synced else "SL pending ⚠"
    return (
        f"{direction_icon} {t['symbol']} {t['side']}\n"
        f"E: {entry_price}\n"
        f"{sl_line}"
    )


def send_testnet_entry(t, prefix=None, sl_synced=True):
    msg = format_testnet_entry(t, sl_synced)
    _safe_print_message(msg)
    send_telegram(msg, prefix=prefix)


def format_live_entry(t, sl_synced):
    direction_icon = "🟢" if t["side"] == "LONG" else "🔴"
    entry_price = fmt_price(
        t.get("exchange_fill_price") or t.get("entry_real") or t.get("entry", 0),
        t["symbol"],
    )
    sl_price = fmt_price(t.get("sl", 0), t["symbol"])
    sl_line = "SL synced ✅" if sl_synced else "SL MISSING ⚠️ — CHECK EXCHANGE"
    return (
        f"🚨 LIVE ENTRY\n"
        f"{direction_icon} {t['symbol']} {t['side']}\n"
        f"E: {entry_price}  SL: {sl_price}\n"
        f"Risk: {round(t.get('risk_percent', 0) * 100, 2)}%  Score: {round(t.get('score', 0), 1)}\n"
        f"{sl_line}"
    )


def send_live_entry(t, prefix=None, sl_synced=True):
    msg = format_live_entry(t, sl_synced)
    _safe_print_message(msg)
    send_telegram(msg, prefix=prefix)
