#!/usr/bin/env python3
"""Deterministic simulator for the isolated 14-day retention core.

Exercises log_retention_14d.py entirely inside a temporary directory. It
creates NO production artifacts, imports NO runtime/strategy module, runs no
shell command, and performs no recursive/wildcard delete. Every mutation is
scoped to a tempfile.TemporaryDirectory that is removed on exit.

Usage:
    python scripts/debug/sim_log_retention_14d.py            # correctness
    python scripts/debug/sim_log_retention_14d.py --stress   # + stress/memory
"""

import gzip
import os
import resource
import stat as stat_mod
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import log_retention_14d as R  # noqa: E402


NOW = datetime(2026, 7, 17, 0, 0, 0, tzinfo=timezone.utc)
NOW_TS = NOW.timestamp()
DAY = 24 * 3600

# Basenames used to exercise category routing.
ACTIVE = ["score_shadow_log.csv", "confirm_reject_log.csv"]
PROTECTED = [
    "btc_m5_m15_decomposition_shadow_v2.jsonl",
    "smc_pa_score_v3_1_shadow.jsonl",
    "structural_context_samples.jsonl",
    "four_phase_breakout_context_shadow_v1.jsonl",
    "live_trades.csv",
]
STATE = ["bot_state.json"]
KNOWN_ROTATED = ACTIVE + [
    "log_pool_pipeline.csv",
    "scan_ema_log.csv",
    "paper_smc_main_gate_shadow.jsonl",
]

_PASSED = 0
_FAILED = 0


def _check(label, cond, detail=""):
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
        print("PASS %s" % label)
    else:
        _FAILED += 1
        print("FAIL %s :: %s" % (label, detail))


def _rss_kb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def _ts_name(base, dt, compressed=False, ext=None):
    root, real_ext = os.path.splitext(base)
    if ext is not None:
        real_ext = ext
    stamp = dt.strftime("%Y%m%dT%H%M%SZ")
    name = "%s_%s%s" % (root, stamp, real_ext)
    if compressed:
        name += ".gz"
    return name


def _write(path, data=b"x", mtime=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(data)
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


def _write_gz(path, data, mtime=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as fh:
        fh.write(data)
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


def _plan(logs, **kw):
    return R.build_retention_plan(
        str(logs),
        now=NOW,
        retention_days=14,
        active_basenames=ACTIVE,
        protected_basenames=PROTECTED,
        state_runtime_basenames=STATE,
        known_rotated_basenames=KNOWN_ROTATED,
        **kw,
    )


# --------------------------------------------------------------------------
# Correctness scenarios
# --------------------------------------------------------------------------

def scenario_classification(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-10"
    arc.mkdir(parents=True)

    active = _write(arc / "score_shadow_log.csv")                       # 1 active
    rotated_unc = _write(arc / _ts_name("score_shadow_log.csv", NOW - timedelta(days=20)))
    within = _write_gz(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=13), compressed=True), b"recent")
    expired_gz = _write_gz(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=20), compressed=True), b"old")
    protected = _write(arc / _ts_name("btc_m5_m15_decomposition_shadow_v2.jsonl", NOW - timedelta(days=99)))
    v2_hist = _write(arc / _ts_name("smc_pa_score_v2_shadow.jsonl", NOW - timedelta(days=99)))  # not in known list
    unknown = _write(arc / "random_temp_output.jsonl")                  # 9 no timestamp
    malformed = _write(arc / "scan_ema_log_20261301T000000Z.csv")       # 10 bad month
    hhmmss = _write(arc / "score_shadow_log_034621.csv")                # legacy time-only

    plan = _plan(logs)

    _check("1 active log preserved", str(active) in plan.would_skip_active, plan.would_skip_active)
    _check("2 rotated uncompressed slated to compress", str(rotated_unc) in plan.would_compress, plan.would_compress)
    _check("4 compressed archive within 14d retained", str(within) in plan.within_retention, plan.within_retention)
    _check("5 compressed archive >14d deletable", str(expired_gz) in plan.would_delete, plan.would_delete)
    _check("7 protected file never deleted", str(protected) in plan.would_skip_protected, plan.would_skip_protected)
    _check("8 historical V2/V2B preserved (not whitelisted)",
           str(v2_hist) in plan.would_skip_unknown and str(v2_hist) not in plan.would_delete, plan.would_skip_unknown)
    _check("9 unknown filename skipped", str(unknown) in plan.would_skip_unknown, plan.would_skip_unknown)
    _check("10 malformed timestamp skipped", str(malformed) in plan.would_skip_unknown, plan.would_skip_unknown)
    _check("25b legacy time-only name skipped (no date)", str(hhmmss) in plan.would_skip_unknown, plan.would_skip_unknown)


def scenario_symlink_dir_fifo(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)

    real_old = _write(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=30)))

    symlink = arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=40))
    sym_ok = True
    try:
        symlink.symlink_to(real_old)
    except OSError:
        sym_ok = False

    directory = arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=50), ext=".d")
    directory = Path(str(directory))
    directory.mkdir()

    fifo = arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=60), ext=".fifo")
    fifo_ok = True
    try:
        os.mkfifo(fifo)
    except (AttributeError, OSError):
        fifo_ok = False

    plan = _plan(logs)

    if sym_ok:
        _check("11 symlink skipped", str(symlink) in plan.would_skip_symlink, plan.would_skip_symlink)
    else:
        _check("11 symlink skipped (unsupported, vacuous)", True)
    # Directory is never yielded as a file by the scanner; assert it is not in any delete/compress list.
    _check("12 directory not treated as candidate",
           str(directory) not in plan.would_delete and str(directory) not in plan.would_compress, directory)
    if fifo_ok:
        _check("13 FIFO/nonregular skipped", str(fifo) in plan.would_skip_symlink, plan.would_skip_symlink)
        os.unlink(fifo)
    else:
        _check("13 FIFO skipped (unsupported, vacuous)", True)


def scenario_traversal(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)
    outside = tmp / "outside_secret.jsonl"
    _write(outside, b"do-not-touch")

    # A candidate path that lexically escapes the root must be rejected.
    escaping = os.path.join(str(arc), "..", "..", "..", "outside_secret.jsonl")
    log_root = R.resolve_logs_root(str(logs))
    cand = R.classify_retention_candidate(
        escaping, log_root=log_root, now=NOW, retention_days=14,
    )
    _check("14 path traversal rejected",
           cand.category == R.UNKNOWN_OR_UNPARSEABLE and cand.reason == "outside_log_root", cand)
    _check("14b escaped file untouched", outside.exists(), outside)


def scenario_compress_lifecycle(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)
    log_root = R.resolve_logs_root(str(logs))

    src = _write(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=20)),
                 data=b"payload-" * 4096)
    src_bytes = src.read_bytes()

    # 19 dry-run creates/deletes nothing.
    dry = R.gzip_archive_streaming(str(src), log_root=log_root, dry_run=True)
    _check("19 dry-run compress mutates nothing",
           src.exists() and not (arc / (src.name + ".gz")).exists() and dry.status == "compressed", dry)

    # 3 original removed only after verified gzip.
    res = R.gzip_archive_streaming(str(src), log_root=log_root, dry_run=False)
    gz = Path(res.dst)
    _check("3 original removed only after verified gzip",
           res.status == "compressed" and res.removed_source and gz.exists() and not src.exists(), res)
    _check("34b gzip content round-trips",
           gzip.open(str(gz), "rb").read() == src_bytes, gz)

    # 20 rerun after compression is a no-op (source already gone).
    rerun = R.gzip_archive_streaming(str(src), log_root=log_root, dry_run=False)
    _check("20 rerun after compression no-op",
           rerun.status == "error" and rerun.error.startswith("lstat_failed"), rerun)

    # 17 existing equivalent gzip handled idempotently.
    src2 = _write(arc / _ts_name("paper_smc_main_gate_shadow.jsonl", NOW - timedelta(days=20)),
                  data=b"same-content")
    _write_gz(Path(str(src2) + ".gz"), b"same-content")
    idem = R.gzip_archive_streaming(str(src2), log_root=log_root, dry_run=False)
    _check("17 equivalent existing gzip is idempotent",
           idem.status == "idempotent" and idem.removed_source and not src2.exists(), idem)

    # 18 conflicting gzip preserves both and reports conflict.
    src3 = _write(arc / _ts_name("log_pool_pipeline.csv", NOW - timedelta(days=20)),
                  data=b"content-A")
    _write_gz(Path(str(src3) + ".gz"), b"DIFFERENT-content-B")
    conflict = R.gzip_archive_streaming(str(src3), log_root=log_root, dry_run=False)
    _check("18 conflicting gzip preserves both",
           conflict.status == "conflict" and src3.exists() and Path(str(src3) + ".gz").exists(), conflict)


def scenario_verify_and_corruption(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)

    good = _write_gz(arc / "x.gz", b"hello-world")
    _check("34 valid gzip verified", R.verify_gzip_archive(str(good), expected_size=11), good)

    corrupt = _write(arc / "y.gz", b"not-a-gzip-stream")
    _check("34c corrupt gzip detected", not R.verify_gzip_archive(str(corrupt)), corrupt)

    empty = _write(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=20)), data=b"")
    log_root = R.resolve_logs_root(str(logs))
    res = R.gzip_archive_streaming(str(empty), log_root=log_root, dry_run=False)
    _check("33 empty file supported",
           res.status == "compressed" and Path(res.dst).exists() and not empty.exists(), res)


def scenario_verification_failure_retains(tmp):
    """16 verification failure retains original; 15 temp gzip failure retains."""
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)
    log_root = R.resolve_logs_root(str(logs))
    src = _write(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=20)), data=b"keepme")

    orig_verify = R.verify_gzip_archive
    try:
        R.verify_gzip_archive = lambda *a, **k: False
        res = R.gzip_archive_streaming(str(src), log_root=log_root, dry_run=False)
    finally:
        R.verify_gzip_archive = orig_verify
    _check("16 verification failure retains original",
           res.status == "error" and res.error == "verify_failed" and src.exists()
           and not Path(str(src) + ".gz").exists(), res)
    # No leftover temp files.
    leftovers = [p for p in arc.iterdir() if ".gz.tmp." in p.name]
    _check("31 interrupted .gz.tmp cleaned up", not leftovers, leftovers)

    orig_open = gzip.open
    try:
        def boom(*a, **k):
            raise OSError("disk full")
        gzip.open = boom
        res2 = R.gzip_archive_streaming(str(src), log_root=log_root, dry_run=False)
    finally:
        gzip.open = orig_open
    _check("15 temp gzip write failure retains original",
           res2.status == "error" and src.exists(), res2)


def scenario_delete_lifecycle(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)

    expired = _write_gz(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=20), compressed=True), b"old")
    fresh = _write_gz(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=2), compressed=True), b"new")

    plan = _plan(logs)
    _check("6a within-retention gz retained", str(fresh) in plan.within_retention, plan.within_retention)

    # 19 dry-run deletes nothing.
    dres = R.execute_retention_plan(plan, dry_run=True)
    _check("19b dry-run delete removes nothing",
           expired.exists() and all(d.status == "would_delete" for d in dres.deleted), dres.deleted)

    # Real delete.
    plan2 = _plan(logs)
    real = R.execute_retention_plan(plan2, dry_run=False)
    _check("5b expired gz deleted one-by-one",
           not expired.exists() and any(d.status == "deleted" for d in real.deleted), real.deleted)
    # 21 rerun after deletion is a no-op.
    plan3 = _plan(logs)
    rerun = R.execute_retention_plan(plan3, dry_run=False)
    _check("21 rerun after deletion no-op",
           plan3.would_delete == [] and all(d.status != "deleted" for d in rerun.deleted), rerun.deleted)


def scenario_cutoff_boundary(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)

    exactly = _write_gz(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=14), compressed=True), b"e")
    just_inside = _write_gz(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=14) + timedelta(seconds=1), compressed=True), b"i")

    plan = _plan(logs)
    _check("6 exact 14d boundary is expired (deleted)", str(exactly) in plan.would_delete, plan.would_delete)
    _check("6b just-inside boundary retained", str(just_inside) in plan.within_retention, plan.within_retention)


def scenario_mtime_ignored(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)
    # Filename says 20 days old (expired); mtime says brand-new.
    p = _write_gz(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=20), compressed=True), b"x",
                  mtime=NOW_TS)
    plan = _plan(logs)
    _check("25 mtime does not override valid filename timestamp",
           str(p) in plan.would_delete, plan.would_delete)


def scenario_dryrun_determinism(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)
    names = []
    for i in range(20):
        age = 20 if i % 2 == 0 else 3
        p = _write_gz(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=age, seconds=i), compressed=True), b"x")
        names.append(p)
    a = _plan(logs)
    b = _plan(logs)
    _check("24/35 UTC-cutoff deterministic + sorted stable",
           a.would_delete == b.would_delete == sorted(a.would_delete), (a.would_delete, b.would_delete))


def scenario_p0_future_protected(tmp):
    """36 an unknown future P0 log can be protected purely via input list."""
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)
    future = _write(arc / _ts_name("startup_close_backfill.log", NOW - timedelta(days=99)))
    plan = R.build_retention_plan(
        str(logs), now=NOW, retention_days=14,
        protected_basenames=["startup_close_backfill.log"],
    )
    _check("36 future P0 log protected via input (no code edit)",
           str(future) in plan.would_skip_protected, plan.would_skip_protected)


def scenario_permission_error(tmp):
    """30 permission error isolated to one file."""
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)
    victim = _write_gz(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=20), compressed=True), b"x")
    other = _write_gz(arc / _ts_name("log_pool_pipeline.csv", NOW - timedelta(days=20), compressed=True), b"y")

    plan = _plan(logs)
    orig_unlink = os.unlink

    def selective_unlink(path, *a, **k):
        if os.path.basename(str(path)) == victim.name:
            raise PermissionError("EACCES")
        return orig_unlink(path, *a, **k)

    try:
        os.unlink = selective_unlink
        res = R.execute_retention_plan(plan, dry_run=False)
    finally:
        os.unlink = orig_unlink

    errored = [d for d in res.deleted if d.status == "error"]
    deleted = [d for d in res.deleted if d.status == "deleted"]
    _check("30 permission error isolated",
           victim.exists() and not other.exists() and len(errored) == 1 and len(deleted) == 1, res.deleted)


def scenario_bounded_chunk(tmp):
    """22 bounded chunked reads / 23 large synthetic file compression."""
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)
    log_root = R.resolve_logs_root(str(logs))
    big = arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=20))
    big.parent.mkdir(parents=True, exist_ok=True)
    payload = b"L" * (1024 * 1024)  # 1 MiB
    with open(big, "wb") as fh:
        for _ in range(8):
            fh.write(payload)  # 8 MiB
    before = _rss_kb()
    res = R.gzip_archive_streaming(str(big), log_root=log_root, chunk_size=64 * 1024, dry_run=False)
    after = _rss_kb()
    _check("22/23 large file streamed with bounded RSS",
           res.status == "compressed" and (after - before) < 16384, (before, after, res.status))


def scenario_byte_accounting(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)
    sizes = [111, 222, 333]
    total = 0
    for i, s in enumerate(sizes):
        _write_gz(arc / _ts_name("scan_ema_log.csv", NOW - timedelta(days=20, seconds=i), compressed=True),
                  b"z" * s)
    plan = _plan(logs)
    # bytes_reclaimable is the on-disk size of the gz files, not payload size.
    on_disk = sum(os.path.getsize(p) for p in plan.would_delete)
    _check("29 byte accounting matches on-disk sizes",
           plan.bytes_reclaimable == on_disk and len(plan.would_delete) == 3, (plan.bytes_reclaimable, on_disk))


def run_correctness():
    with tempfile.TemporaryDirectory(prefix="log-retention-14d-") as td:
        base = Path(td)
        scenario_classification(base / "c1")
        scenario_symlink_dir_fifo(base / "c2")
        scenario_traversal(base / "c3")
        scenario_compress_lifecycle(base / "c4")
        scenario_verify_and_corruption(base / "c5")
        scenario_verification_failure_retains(base / "c6")
        scenario_delete_lifecycle(base / "c7")
        scenario_cutoff_boundary(base / "c8")
        scenario_mtime_ignored(base / "c9")
        scenario_dryrun_determinism(base / "c10")
        scenario_p0_future_protected(base / "c11")
        scenario_permission_error(base / "c12")
        scenario_bounded_chunk(base / "c13")
        scenario_byte_accounting(base / "c14")


# --------------------------------------------------------------------------
# Stress / memory
# --------------------------------------------------------------------------

def run_stress():
    started = time.time()
    baseline = _rss_kb()
    peak = baseline
    with tempfile.TemporaryDirectory(prefix="log-retention-stress-") as td:
        logs = Path(td) / "logs"
        arc = logs / "archive" / "2026-06-01"
        arc.mkdir(parents=True)

        # 20,000 directory entries: 5,000 rotated candidates + noise.
        n_rotated = 5000
        n_expired = 5000  # expired compressed archives among candidates
        # Create expired compressed archives (candidates for deletion).
        for i in range(n_expired):
            name = _ts_name("scan_ema_log.csv", NOW - timedelta(days=30, seconds=i), compressed=True)
            fd = os.open(arc / name, os.O_CREAT | os.O_WRONLY, 0o644)
            os.close(fd)
        # Non-candidate noise entries (unparseable) up to ~20k total.
        for i in range(20000 - n_expired):
            fd = os.open(arc / ("noise_%d.tmp" % i), os.O_CREAT | os.O_WRONLY, 0o644)
            os.close(fd)

        before_plan = _rss_kb()
        plan = R.build_retention_plan(
            str(logs), now=NOW, retention_days=14,
            known_rotated_basenames=KNOWN_ROTATED,
        )
        after_plan = _rss_kb()
        peak = max(peak, after_plan)
        assert plan.scanned >= 20000, plan.scanned
        assert len(plan.would_delete) == n_expired, len(plan.would_delete)

        # Repeated dry-run passes should not leak.
        for _ in range(5):
            R.build_retention_plan(str(logs), now=NOW, retention_days=14,
                                   known_rotated_basenames=KNOWN_ROTATED)
        after_repeat = _rss_kb()
        peak = max(peak, after_repeat)

        # 1,000 real gzip operations in temp storage.
        gz_dir = logs / "archive" / "2026-06-02"
        gz_dir.mkdir(parents=True)
        log_root = R.resolve_logs_root(str(logs))
        before_gzip = _rss_kb()
        for i in range(1000):
            src = gz_dir / _ts_name("log_pool_pipeline.csv", NOW - timedelta(days=30, seconds=i))
            with open(src, "wb") as fh:
                fh.write(b"row\n" * 64)
            res = R.gzip_archive_streaming(str(src), log_root=log_root, chunk_size=64 * 1024, dry_run=False)
            assert res.status == "compressed", res
        after_gzip = _rss_kb()
        peak = max(peak, after_gzip)

        # Real deletion pass over every expired archive in the tree: the
        # 5,000 seeded plus the 1,000 gzip outputs created above (also >14d).
        expected_deleted = n_expired + 1000
        plan_del = R.build_retention_plan(str(logs), now=NOW, retention_days=14,
                                         known_rotated_basenames=KNOWN_ROTATED)
        res_del = R.execute_retention_plan(plan_del, dry_run=False)
        deleted = sum(1 for d in res_del.deleted if d.status == "deleted")
        assert deleted == expected_deleted, deleted
        peak = max(peak, _rss_kb())

        final = _rss_kb()
        elapsed = time.time() - started

    slope_ok = (final - baseline) < 262144  # < 256 MiB retained growth
    _check("stress scanned >= 20000", plan.scanned >= 20000, plan.scanned)
    _check("stress deleted == 6000 expired (5000 seeded + 1000 gz)", deleted == expected_deleted, deleted)
    _check("stress no unbounded RSS slope", slope_ok, (baseline, peak, final))
    print(
        "MEMORY_BENCHMARK "
        "baseline_rss_kb=%d peak_rss_kb=%d final_rss_kb=%d elapsed_sec=%.3f "
        "scanned=%d candidates=%d gzip_ops=%d deleted=%d"
        % (baseline, peak, final, elapsed, plan.scanned, len(plan_del.would_delete),
           1000, deleted)
    )


def main():
    stress = "--stress" in sys.argv[1:]
    run_correctness()
    if stress:
        run_stress()
    print("-" * 60)
    print("SIM SUMMARY passed=%d failed=%d" % (_PASSED, _FAILED))
    if _FAILED:
        print("FAIL sim_log_retention_14d")
        sys.exit(1)
    print("PASS sim_log_retention_14d")


if __name__ == "__main__":
    main()
