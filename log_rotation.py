import os
import shutil
import time
from datetime import datetime, timedelta

ROTATION_TARGETS = [
    "confirm_reject_log.csv",
    "score_shadow_log.csv",
    "log_pool_pipeline.csv",
    "state_log.csv",
    "scan_early_log.csv",
    "scan_ema_log.csv",
    "scan_feature_snapshots.jsonl",
    "qualified_latency_waterfall.jsonl",
    "runtime_errors.log",
]

DEFAULT_MAX_SIZE_MB = 50
DEFAULT_RETENTION_DAYS = 7

_last_rotation_check = 0
_ROTATION_CHECK_INTERVAL = 6 * 3600


def maybe_rotate_logs(log_dir=None, max_size_mb=DEFAULT_MAX_SIZE_MB, retention_days=DEFAULT_RETENTION_DAYS):
    global _last_rotation_check
    now = time.time()
    if now - _last_rotation_check < _ROTATION_CHECK_INTERVAL:
        return
    _last_rotation_check = now
    rotate_logs(log_dir=log_dir, max_size_mb=max_size_mb, retention_days=retention_days)


def rotate_logs(log_dir=None, max_size_mb=DEFAULT_MAX_SIZE_MB, retention_days=DEFAULT_RETENTION_DAYS):
    try:
        if log_dir is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            log_dir = os.path.join(base_dir, "logs")

        archive_base = os.path.join(log_dir, "archive")
        today_str = datetime.now().strftime("%Y-%m-%d")
        today_archive = os.path.join(archive_base, today_str)

        rotated = []
        for filename in ROTATION_TARGETS:
            filepath = os.path.join(log_dir, filename)
            if not os.path.exists(filepath):
                continue
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            if size_mb >= max_size_mb:
                os.makedirs(today_archive, exist_ok=True)
                timestamp = datetime.now().strftime("%H%M%S")
                base, ext = os.path.splitext(filename)
                archive_name = f"{base}_{timestamp}{ext}"
                archive_path = os.path.join(today_archive, archive_name)
                shutil.move(filepath, archive_path)
                rotated.append(f"{filename} ({round(size_mb, 1)}MB)")

        if rotated:
            print(f"[LOG ROTATION] Rotated {len(rotated)} file(s): {', '.join(rotated)}")

        _cleanup_old_archives(archive_base, retention_days)

    except Exception as e:
        print(f"[LOG ROTATION ERROR] {e} — rotation skipped, trading unaffected")


def _cleanup_old_archives(archive_base, retention_days):
    try:
        if not os.path.exists(archive_base):
            return
        cutoff = datetime.now() - timedelta(days=retention_days)
        for entry in os.scandir(archive_base):
            if not entry.is_dir():
                continue
            try:
                dir_date = datetime.strptime(entry.name, "%Y-%m-%d")
                if dir_date < cutoff:
                    shutil.rmtree(entry.path)
                    print(f"[LOG ROTATION] Deleted archive older than {retention_days}d: {entry.name}")
            except ValueError:
                pass
    except Exception as e:
        print(f"[LOG ROTATION CLEANUP ERROR] {e}")
