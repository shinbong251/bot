import os
import json


_SECRET_ENV_MAP = {
    "API_KEY":            "api_key",
    "API_SECRET":         "api_secret",
    "TESTNET_API_KEY":    "testnet_api_key",
    "TESTNET_API_SECRET": "testnet_api_secret",
    "TELE_TOKEN":         "tele_token",
}

_PERSISTENT_KEYS_EXCLUDE = set(_SECRET_ENV_MAP.values())


def _strip_secrets_for_save(cfg: dict) -> dict:
    """Return a copy of cfg with all .env-injected secret keys removed.

    .env secrets must never be persisted back into config.json.
    They are runtime-injected at load time from the environment only.
    """
    return {k: v for k, v in cfg.items() if k not in _PERSISTENT_KEYS_EXCLUDE}


def _load_dotenv(path=".env") -> dict:
    if not os.path.exists(path):
        return {}
    result = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            result[key.strip()] = val.strip().strip('"').strip("'")
    return result


def load_config():
    default = {
        "account_balance": 50,
        "risk_per_trade": 0.01,
        "equity_peak": 50,
        "paper_dd_pause_mode": "ENFORCE",
        "tele_token": "",
        "paper_chat_id": "",
        "testnet_chat_id": "",
        "alerts_chat_id": "",
        "tele_verbosity": "normal",
        "state_save_telemetry_debug": False,
        "api_key": "",
        "api_secret": "",
        "live_mode": False,
        "live_chat_id": "",
        "max_live_trades": 3,
        "live_risk_per_trade": 0.003,
        "live_max_portfolio_risk": 0.009,
        "live_smc_research_enabled": False,
        "max_live_research_trades": 1,
        "live_confirm_enabled": False,
        "live_paper_red_scale_mode": "BLOCK",
        "live_research_micro_pause_enabled": True,
        "live_research_loss_streak_pause_count": 3,
        "live_research_micro_pause_hours": 3,
        "live_research_rolling_net_pause_r": -2.0,
        "min_notional_floor_allowed": False,
        "research_health_baseline_ts": 0,
        "research_health_min_active_closed": 20,
        "research_health_use_active_only_for_live_scale": True,
        "use_exchange_max_leverage": True,
        "leverage_cache_ttl_secs": 21600,
        "paper_dd_rebaseline_pending": False,
        "paper_dd_rebaseline_reason": "",
        "paper_dd_rebaseline_operator": "",
        "enable_early_v2": True,
        "enable_swing_retest": True,
        "enable_early_continuation": False,
        "paper_enable_early_v2": True,
        "paper_enable_swing_retest": True,
        "paper_enable_early_continuation": True,
        "paper_reversal_shadow_enabled": True,
        "paper_reversal_shadow_max_per_scan": 20,
        "reversal_qualified_shadow_enabled": True,
        "reversal_qualified_shadow_live_enabled": False,
        "reversal_qualified_shadow_log_open": True,
        "reversal_qualified_shadow_min_score": -999,
        "reversal_qualified_shadow_max_score": -10,
        "reversal_qualified_shadow_allowed_range_context": ["RANGE_LOW", "RANGE_HIGH"],
        "reversal_qualified_shadow_allowed_bos_confirmation": ["NEAR", "CLOSE_THROUGH"],
        "reversal_qualified_shadow_reject_breakout_strong": True,
        "reversal_qualified_shadow_reject_sweep_high": True,
        "reversal_qualified_shadow_reject_range_mid": True,
        "reversal_qualified_shadow_soft_prefer_extended": True,
        "swing_retest_shadow_outcome_enabled": True,
        "swing_retest_shadow_outcome_ttl_secs": 86400,
        "swing_retest_shadow_outcome_max_pending": 5000,
        "swing_retest_shadow_outcome_log_open": True,
        "early_cont_shadow_outcome_enabled": True,
        "early_cont_shadow_outcome_ttl_secs": 86400,
        "early_cont_shadow_outcome_max_pending": 5000,
        "early_cont_shadow_outcome_log_open": True,
        "paper_filter_confirm_pre_break_low_near": True,
        "paper_gate_confirm_short_pre_break_low": True,
        "paper_enable_smc_confirm_filter": False,
        "paper_smc_confirm_phase2_mode": "strict_conflict",
        "paper_enable_smc_research_lane": True,
        "paper_smc_research_max_open": 5,
        "paper_smc_research_min_score_v2_structural_shadow": 2.5,
        "paper_smc_research_allow_reasons": [
            "LOW_SCORE",
            "MID_SCORE_WEAK_BOS",
            "RR_FAIL",
        ],
        "paper_smc_research_allow_structural_decisions": [
            "QUALIFIED",
            "NEUTRAL",
            "WOULD_DOWNRANK",
        ],
        "paper_smc_research_exclude_trend_fail_no_geometry": True,
        "paper_smc_research_allow_unknown_structural_decision": True,
        "paper_smc_research_live_enabled": False,
        "paper_smc_research_notify_open": False,
        "paper_smc_research_notify_close": True,
        "paper_smc_research_summary_enabled": True,
        "paper_smc_research_summary_interval_secs": 3600,
        "paper_smc_research_qualified_enabled": False,
        "paper_smc_research_cap_enabled": False,
        "paper_smc_research_qualified_max_open": 3,
        "paper_smc_research_qualified_max_new_trades": 131,
        "paper_smc_research_qualified_min_rr": 2.0,
        "paper_smc_research_location_gate_enabled": False,
        "live_smc_research_location_gate_enabled": False,
        "paper_smc_main_enabled": False,
        "paper_smc_main_candidate_types": [
            "ACCEPTED_CONFIRM",
            "LOW_SCORE",
            "MID_SCORE_WEAK_BOS",
            "RR_FAIL",
        ],
        "paper_smc_main_exclude_trend_fail": True,
        "paper_smc_main_min_score_v2_structural_shadow": 2.5,
        "paper_smc_main_max_open": 5,
        "paper_smc_main_rank_candidates": True,
        "paper_smc_main_candidate_type_priority": [
            "ACCEPTED_CONFIRM",
            "MID_SCORE_WEAK_BOS",
            "LOW_SCORE",
            "RR_FAIL",
        ],
        "paper_smc_main_block_low_score_no_followthrough_divergence": True,
        "paper_smc_main_block_unknown_structural_low_score": False,
        "paper_smc_main_weak_structure_block_mode": "specific_combo",
        "paper_smc_main_use_structural_modifier": True,
        "paper_smc_main_use_boundary_guard": True,
        "paper_smc_main_require_outcome_trackable": True,
        "paper_smc_main_allow_unknown_structural_decision": True,
        "paper_smc_main_notify_open": True,
        "paper_smc_main_notify_close": True,
        "paper_smc_main_live_enabled": False,
        "paper_smc_main_gate_shadow_enabled": False,
        "paper_smc_main_gate_shadow_max_per_scan": 200,
        "telegram_paper_observe_enabled": False,
        "telegram_paper_trail_update_throttle_secs": 300,
        "telegram_paper_trail_update_min_r_step": 0.2,
        "paper_enable_structural_score_modifier": True,
        "paper_structural_score_modifier_apply_to": ["CONFIRM", "CONFIRM_SMC_RESEARCH"],
        "paper_structural_score_modifier_live_enabled": False,
        "paper_structural_score_modifier_log_only": False,
        "paper_structural_score_modifier_min": -0.5,
        "paper_structural_score_modifier_max": 0.5,
        "paper_structural_score_modifier_boundary_guard_enabled": True,
        "paper_structural_score_modifier_boundary_guard_margin": 0.5,
        "paper_boundary_guard_notify_applied": True,
        "paper_boundary_guard_summary_enabled": True,
        "paper_boundary_guard_summary_interval_secs": 3600,
        "live_shadow_smc_decision_enabled": False,
        "live_shadow_smc_decision_dedup_ttl_secs": 900,
        "live_shadow_smc_decision_min_log_only": True,
        "market_regime_router_shadow_enabled": False,
        "market_regime_router_shadow_log_path": "logs/market_regime_router_shadow.jsonl",
        "market_regime_router_shadow_log_unknown": True,
        "market_regime_router_shadow_log_chop": True,
        "market_regime_router_shadow_log_every_scan": False,
        "market_regime_router_shadow_dedup_ttl_secs": 900,
        "market_regime_router_shadow_max_per_scan": 100,
        "market_regime_router_shadow_min_confidence_to_log": "LOW",
        "scan_feature_snapshot_enabled": True,
        "scan_feature_snapshot_log_path": "logs/scan_feature_snapshots.jsonl",
        "scan_feature_snapshot_max_per_scan": 100,
        "scan_feature_snapshot_candle_window": 10,
        "live_filter_confirm_pre_break_low_near": True,
        "exclude_tradfi_symbols": True,
        "tradfi_symbol_allowlist": [
            "NATGASUSDT",
            "XAUUSDT",
            "PAXGUSDT",
        ],
        # Signal timing integrity — added to prevent stale continuation execution
        "open_trade_data_refresh_enabled": True,
        "open_trade_data_refresh_interval_secs": 60,
        "open_trade_data_refresh_timeframes": ["5m", "15m"],
        "trade_freshness_stuck_feed_enabled": True,
        "trade_freshness_stuck_feed_skip_threshold": 30,
        "trade_freshness_stuck_feed_secs_threshold": 120,
        "trade_freshness_stuck_feed_summary_interval_secs": 900,
        "trade_freshness_notify_stuck_feed": False,
        "signal_max_age_secs": 180,   # reject continuation signals older than this (seconds)
        "live_min_sl_ratio": 0.003,   # reject LIVE signals with entry/SL distance < 0.3%
    }

    if not os.path.exists("config.json"):
        with open("config.json", "w", encoding="utf-8") as f:
            json.dump(default, f, indent=2, ensure_ascii=False)
        return default

    try:
        with open("config.json", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as e:
        print(f"[CONFIG ERROR] config.json corrupted or unreadable: {e} — using defaults")
        return default

    # ===== ONE-TIME SANITIZATION PASS =====
    # Detect leaked secrets (non-empty value persisted for any secret key)
    _leaked = [k for k in _PERSISTENT_KEYS_EXCLUDE if cfg.get(k)]
    # Detect escaped Unicode in raw file bytes (ensure_ascii=True artifact)
    _has_escaped_unicode = False
    try:
        with open("config.json", "rb") as _rb:
            _has_escaped_unicode = b"\\u" in _rb.read()
    except Exception:
        pass

    if _leaked or _has_escaped_unicode:
        for _sk in _PERSISTENT_KEYS_EXCLUDE:
            cfg.pop(_sk, None)
        _tmp = "config.json.tmp"
        with open(_tmp, "w", encoding="utf-8") as _f:
            json.dump(cfg, _f, indent=2, ensure_ascii=False)
        os.replace(_tmp, "config.json")
        if _leaked:
            print(f"[CONFIG SANITIZE] Removed leaked secrets from config.json: {_leaked}")
        if _has_escaped_unicode:
            print("[CONFIG SANITIZE] Rewrote config.json as clean UTF-8 (Unicode normalized)")
    # ===== END SANITIZATION PASS =====

    updated = False

    legacy_tele_chat_id = cfg.get("tele_chat_id", "")
    if legacy_tele_chat_id and not cfg.get("paper_chat_id", ""):
        cfg["paper_chat_id"] = legacy_tele_chat_id
        print(f"[CONFIG] Migrated legacy tele_chat_id → paper_chat_id")
        updated = True
    if "tele_chat_id" in cfg:
        del cfg["tele_chat_id"]
        updated = True

    if "EQUITY_PEAK" in cfg:
        del cfg["EQUITY_PEAK"]
        print("[CONFIG] Removed duplicate EQUITY_PEAK (uppercase) — canonical field is equity_peak")
        updated = True

    for k, v in default.items():
        if k not in cfg:
            cfg[k] = v
            updated = True

    if updated:
        with open("config.json", "w", encoding="utf-8") as f:
            json.dump(_strip_secrets_for_save(cfg), f, indent=2, ensure_ascii=False)

    _env_file = _load_dotenv()
    for env_key, cfg_key in _SECRET_ENV_MAP.items():
        val = os.environ.get(env_key) or _env_file.get(env_key)
        if val:
            cfg[cfg_key] = val

    return cfg


# ===== LOAD ONCE =====
config = load_config()

ACCOUNT_BALANCE = config["account_balance"]
RISK_PER_TRADE = config["risk_per_trade"]
EQUITY_PEAK = max(ACCOUNT_BALANCE, config.get("equity_peak", ACCOUNT_BALANCE))

DEBUG = False
DEBUG_FILTERS = False
