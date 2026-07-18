import os
import time
from datetime import datetime, timezone

from log_retention_14d import (
    DEFAULT_RETENTION_DAYS,
    build_retention_plan,
    execute_retention_plan,
)


ROTATION_TARGETS = [
    "confirm_reject_log.csv",
    "score_shadow_log.csv",
    "log_pool_pipeline.csv",
    "state_log.csv",
    "scan_early_log.csv",
    "scan_ema_log.csv",
    "scan_feature_snapshots.jsonl",
    "qualified_latency_waterfall.jsonl",
    "paper_smc_main_gate_shadow.jsonl",
    "structural_context_samples.jsonl",
    "btc_m5_m15_decomposition_shadow_v2.jsonl",
    "four_phase_breakout_context_shadow_v1.jsonl",
    "smc_pa_score_v3_1_shadow.jsonl",
    "giveback_management_shadow_v1.jsonl",
    "giveback_management_close_shadow_v1.jsonl",
    "runtime_errors.log",
]

PROMOTION_PROTECTED_LOGS = {
    "smc_pa_score_v3_shadow.jsonl",
    "smc_pa_score_v3_1_shadow.jsonl",
    "btc_m5_m15_decomposition_shadow_v2.jsonl",
    "confirm_structural_outcomes.jsonl",
    "structural_context_samples.jsonl",
    "paper_smc_research_qualified_decisions.jsonl",
    "qualified_latency_waterfall.jsonl",
}

P0_PROTECTED_LOGS = {
    "startup_authoritative_close_backfill_v1.jsonl",
    "startup_authoritative_close_review_v1.jsonl",
}

STATE_OR_RUNTIME_BASENAMES = {
    "bot_state.json",
    "paper_state.json",
    "live_state.json",
    "testnet_state.json",
    "canary_state.json",
    "live_account_state.json",
    "testnet_account_state.json",
    "startup_authoritative_close_backfill_markers.json",
}

KNOWN_ROTATED_BASENAMES = tuple(
    name for name in ROTATION_TARGETS if name not in PROMOTION_PROTECTED_LOGS
)

DEFAULT_MAX_SIZE_MB = 50
DEFAULT_RETENTION_DRY_RUN = True
DEFAULT_RETENTION_MAX_FILES = 100
DEFAULT_RETENTION_MAX_BYTES = 2 * 1024 * 1024 * 1024

_last_rotation_check = 0
_ROTATION_CHECK_INTERVAL = 6 * 3600


def maybe_rotate_logs(log_dir=None, max_size_mb=DEFAULT_MAX_SIZE_MB, retention_days=DEFAULT_RETENTION_DAYS):
    global _last_rotation_check
    now = time.time()
    if now - _last_rotation_check < _ROTATION_CHECK_INTERVAL:
        return
    _last_rotation_check = now
    rotate_logs(log_dir=log_dir, max_size_mb=max_size_mb, retention_days=retention_days)


def rotate_logs(
    log_dir=None,
    max_size_mb=DEFAULT_MAX_SIZE_MB,
    retention_days=DEFAULT_RETENTION_DAYS,
    gzip_rotated=False,
    retention_dry_run=DEFAULT_RETENTION_DRY_RUN,
):
    try:
        if log_dir is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            log_dir = os.path.join(base_dir, "logs")

        archive_base = os.path.join(log_dir, "archive")
        now_utc = datetime.now(timezone.utc)
        today_archive = os.path.join(archive_base, now_utc.strftime("%Y-%m-%d"))

        rotated = []
        for filename in ROTATION_TARGETS:
            filepath = os.path.join(log_dir, filename)
            if not os.path.exists(filepath):
                continue
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            if size_mb >= max_size_mb:
                os.makedirs(today_archive, exist_ok=True)
                base, ext = os.path.splitext(filename)
                archive_path = _unique_archive_path(today_archive, base, ext, now_utc)
                os.replace(filepath, archive_path)
                if gzip_rotated:
                    import log_retention_14d as retention_core
                    gzip_result = retention_core.gzip_archive_streaming(
                        archive_path,
                        log_root=os.path.realpath(log_dir),
                        dry_run=False,
                    )
                    if gzip_result.status in ("compressed", "idempotent") and gzip_result.dst:
                        archive_path = gzip_result.dst
                rotated.append(f"{filename} ({round(size_mb, 1)}MB)")

        if rotated:
            print(f"[LOG ROTATION] Rotated {len(rotated)} file(s): {', '.join(rotated)}")

        cleanup_rotated_log_archives(
            log_dir=log_dir,
            retention_days=retention_days,
            dry_run=retention_dry_run,
        )

    except Exception as e:
        print(f"[LOG ROTATION ERROR] {e} — rotation skipped, trading unaffected")


def _unique_archive_path(archive_dir, base, ext, now_utc=None):
    now_utc = now_utc or datetime.now(timezone.utc)
    stamp = now_utc.strftime("%Y%m%dT%H%M%SZ")
    candidate = os.path.join(archive_dir, f"{base}_{stamp}{ext}")
    if not os.path.exists(candidate):
        return candidate
    for suffix in range(1, 1000):
        candidate = os.path.join(archive_dir, f"{base}_{stamp}_{suffix}{ext}")
        if not os.path.exists(candidate):
            return candidate
    raise RuntimeError(f"archive name collision for {base}{ext}")


def cleanup_rotated_log_archives(
    log_dir=None,
    retention_days=DEFAULT_RETENTION_DAYS,
    dry_run=True,
    max_files=DEFAULT_RETENTION_MAX_FILES,
    max_bytes=DEFAULT_RETENTION_MAX_BYTES,
    now_ts=None,
):
    started = time.time()
    try:
        if log_dir is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            log_dir = os.path.join(base_dir, "logs")

        sets = build_retention_basename_sets()
        if not sets["known_rotated_basenames"]:
            summary = _empty_retention_summary(
                dry_run=dry_run,
                started=started,
                error="known_rotated_basenames_empty",
            )
            print(_format_retention_summary(summary))
            return summary

        now = None
        if now_ts is not None:
            now = datetime.fromtimestamp(float(now_ts), tz=timezone.utc)

        plan = build_retention_plan(
            log_dir,
            now=now,
            retention_days=retention_days,
            active_basenames=sets["active_basenames"],
            protected_basenames=sets["protected_basenames"],
            state_runtime_basenames=sets["state_or_runtime_basenames"],
            known_rotated_basenames=sets["known_rotated_basenames"],
            gzip_uncompressed=True,
        )
        result = execute_retention_plan(
            plan,
            dry_run=bool(dry_run),
            max_delete_files=max(0, int(max_files)),
            max_delete_bytes=max(0, int(max_bytes)),
        )
        summary = _retention_summary(result, started=started)
        print(_format_retention_summary(summary))
        return summary
    except Exception as exc:
        summary = _empty_retention_summary(
            dry_run=dry_run,
            started=started,
            error=f"cleanup_failed:{type(exc).__name__}:{exc}",
        )
        print(_format_retention_summary(summary))
        return summary


def build_retention_basename_sets():
    active = set(ROTATION_TARGETS)
    protected = set(PROMOTION_PROTECTED_LOGS) | set(P0_PROTECTED_LOGS)
    state_or_runtime = set(STATE_OR_RUNTIME_BASENAMES)
    known = set(KNOWN_ROTATED_BASENAMES)
    return {
        "active_basenames": frozenset(active),
        "protected_basenames": frozenset(protected),
        "known_rotated_basenames": frozenset(known),
        "state_or_runtime_basenames": frozenset(state_or_runtime),
    }


def _empty_retention_summary(dry_run, started, error=None):
    errors = 1 if error else 0
    return {
        "dry_run": bool(dry_run),
        "scanned": 0,
        "within_retention": 0,
        "would_compress": 0,
        "compressed": 0,
        "would_delete": 0,
        "deleted": 0,
        "skipped_active": 0,
        "skipped_protected": 0,
        "skipped_unknown": 0,
        "skipped_nonregular": 0,
        "conflicts": 0,
        "errors": errors,
        "bytes_reclaimable": 0,
        "bytes_deleted": 0,
        "elapsed_ms": int((time.time() - started) * 1000),
        "error": error or "",
    }


def _retention_summary(result, started):
    plan = result.plan
    compressed = sum(
        1 for item in result.compressed
        if item.status in ("compressed", "idempotent") and not result.dry_run
    )
    deleted = sum(1 for item in result.deleted if item.status == "deleted")
    bytes_deleted = sum(item.size for item in result.deleted if item.status == "deleted")
    return {
        "dry_run": bool(result.dry_run),
        "scanned": plan.scanned,
        "within_retention": len(plan.within_retention),
        "would_compress": len(plan.would_compress),
        "compressed": compressed,
        "would_delete": len(plan.would_delete),
        "deleted": deleted,
        "skipped_active": len(plan.would_skip_active),
        "skipped_protected": len(plan.would_skip_protected),
        "skipped_unknown": len(plan.would_skip_unknown),
        "skipped_nonregular": len(plan.would_skip_symlink),
        "conflicts": len(result.conflicts),
        "errors": len(result.errors),
        "bytes_reclaimable": plan.bytes_reclaimable,
        "bytes_deleted": bytes_deleted,
        "elapsed_ms": int((time.time() - started) * 1000),
    }


def _format_retention_summary(summary):
    keys = (
        "dry_run",
        "scanned",
        "within_retention",
        "would_compress",
        "compressed",
        "would_delete",
        "deleted",
        "skipped_active",
        "skipped_protected",
        "skipped_unknown",
        "skipped_nonregular",
        "conflicts",
        "errors",
        "bytes_reclaimable",
        "bytes_deleted",
        "elapsed_ms",
    )
    body = " ".join(f"{key}={summary.get(key, 0)}" for key in keys)
    error = summary.get("error")
    if error:
        body = f"{body} error={error}"
    return f"[LOG RETENTION] {body}"


def _cleanup_old_archives(archive_base, retention_days):
    log_dir = os.path.dirname(os.path.realpath(archive_base))
    return cleanup_rotated_log_archives(
        log_dir=log_dir,
        retention_days=retention_days,
        dry_run=True,
    )
