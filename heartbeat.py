import time
from datetime import datetime, timezone, timedelta

_VN_TZ = timezone(timedelta(hours=7))

PAPER_HB_INTERVAL   = 6 * 3600
TESTNET_HB_INTERVAL = 2 * 3600
LIVE_HB_INTERVAL    = 6 * 3600


def _vn_now_str():
    return datetime.now(_VN_TZ).strftime("%Y-%m-%d %H:%M (VN)")


def _format_uptime(seconds):
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    return f"{h}h {m:02d}m"


def _health_state(ctx):
    if ctx.pause_until and time.time() < ctx.pause_until:
        return "DEGRADED\n• Drawdown pause active"
    return "HEALTHY"


def build_paper_heartbeat(ctx, bot_start_time):
    uptime_secs = time.time() - bot_start_time
    open_count  = sum(1 for t in ctx.trades if t.get("status") == "OPEN")
    opened = ctx.stats.get("opened", 0)
    wins   = ctx.stats.get("win", 0)
    losses = ctx.stats.get("loss", 0)
    be     = ctx.stats.get("be", 0)
    closed = wins + losses + be
    total  = wins + losses
    winrate = f"{round(wins / total * 100)}%" if total > 0 else "N/A"
    pnl_r   = ctx.session_pnl_r
    pnl_str = f"+{round(pnl_r, 2)}R" if pnl_r >= 0 else f"{round(pnl_r, 2)}R"
    health  = _health_state(ctx)

    return (
        "====================\n"
        "[PAPER HEARTBEAT]\n"
        "====================\n"
        "\n"
        f"🕒 Uptime: {_format_uptime(uptime_secs)}\n"
        "\n"
        "📊 Runtime\n"
        f"• Open trades: {open_count}\n"
        f"• Session: {opened} opened / {closed} closed\n"
        f"• Runtime status: {health}\n"
        "\n"
        "💰 Performance\n"
        f"• Session PnL: {pnl_str}\n"
        f"• Winrate: {winrate}\n"
        "\n"
        "⚙️ System\n"
        "• Scan loop: OK\n"
        "• Trade loop: OK\n"
        "\n"
        f"🕒 Updated: {_vn_now_str()}"
    )


def build_live_heartbeat(ctx, bot_start_time):
    active_trades = [
        t for t in ctx.trades
        if t.get("status") == "OPEN"
        and not t.get("quarantined")
        and not t.get("repair_disabled")
    ]
    open_count   = len(active_trades)
    active_stops = sum(1 for t in active_trades if t.get("exchange_sl_id") is not None)

    opened  = ctx.stats.get("opened", 0)
    wins    = ctx.stats.get("win", 0)
    losses  = ctx.stats.get("loss", 0)
    be      = ctx.stats.get("be", 0)
    closed  = wins + losses + be
    total   = wins + losses
    winrate = f"{round(wins / total * 100)}%" if total > 0 else "N/A"

    bal_start = round(ctx.initial_balance, 1)
    bal_now   = round(ctx.account_balance, 1)
    pnl_delta = round(ctx.account_balance - ctx.initial_balance, 2)
    pnl_str   = f"+{pnl_delta}" if pnl_delta >= 0 else str(pnl_delta)
    health    = _health_state(ctx)

    quarantined_count = sum(
        1 for t in ctx.trades
        if t.get("quarantined") or t.get("repair_disabled")
    )
    orphan_count  = getattr(ctx, "recon_orphan_count", 0)
    emergency     = ctx.emergency_close_count

    recon_parts = []
    if quarantined_count:
        recon_parts.append(f"• {quarantined_count} quarantined")
    if orphan_count:
        recon_parts.append(f"• {orphan_count} orphan exchange position(s)")
    if emergency:
        recon_parts.append(f"• {emergency} emergency close(s)")
    recon_str = "\n".join(recon_parts) if recon_parts else "• None"

    return (
        "====================\n"
        "🔴 [LIVE SUMMARY]\n"
        "====================\n"
        "\n"
        f"💰 Balance:\n{bal_start} → {bal_now} USDT ({pnl_str})\n"
        "\n"
        f"📊 Open Trades:\n{open_count}\n"
        "\n"
        f"🛡 Protection:\n{active_stops}/{open_count} SL synced\n"
        "\n"
        f"📈 Session:\n{opened} opened / {closed} closed | WR: {winrate}\n"
        "\n"
        f"⚠ Runtime:\n{health}\n"
        "\n"
        f"⚠ Reconciliation:\n{recon_str}\n"
        "\n"
        f"🕒 Updated:\n{_vn_now_str()}"
    )


def build_testnet_heartbeat(ctx, bot_start_time):
    active_trades = [
        t for t in ctx.trades
        if t.get("status") == "OPEN"
        and not t.get("quarantined")
        and not t.get("repair_disabled")
    ]
    open_count   = len(active_trades)
    active_stops = sum(1 for t in active_trades if t.get("exchange_sl_id") is not None)

    opened  = ctx.stats.get("opened", 0)
    wins    = ctx.stats.get("win", 0)
    losses  = ctx.stats.get("loss", 0)
    be      = ctx.stats.get("be", 0)
    closed  = wins + losses + be
    total   = wins + losses
    winrate = f"{round(wins / total * 100)}%" if total > 0 else "N/A"

    bal_start = round(ctx.initial_balance, 1)
    bal_now   = round(ctx.account_balance, 1)
    health    = _health_state(ctx)

    quarantined_count = sum(
        1 for t in ctx.trades
        if t.get("quarantined") or t.get("repair_disabled")
    )
    orphan_count  = getattr(ctx, "recon_orphan_count", 0)
    emergency     = ctx.emergency_close_count

    recon_parts = []
    if quarantined_count:
        recon_parts.append(f"• {quarantined_count} quarantined")
    if orphan_count:
        recon_parts.append(f"• {orphan_count} orphan exchange position(s)")
    if emergency:
        recon_parts.append(f"• {emergency} emergency close(s)")
    recon_str = "\n".join(recon_parts) if recon_parts else "• None"

    return (
        "====================\n"
        "[TESTNET SUMMARY]\n"
        "=================\n"
        "\n"
        f"💰 Balance:\n{bal_start} → {bal_now}\n"
        "\n"
        f"📊 Open Trades:\n{open_count}\n"
        "\n"
        f"🛡 Protection:\n{active_stops}/{open_count} synced\n"
        "\n"
        f"📈 Session:\n{opened} opened / {closed} closed | WR: {winrate}\n"
        "\n"
        f"⚠ Runtime:\n{health}\n"
        "\n"
        f"⚠ Reconciliation:\n{recon_str}\n"
        "\n"
        f"🕒 Updated:\n{_vn_now_str()}"
    )
