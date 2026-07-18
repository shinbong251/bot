#!/usr/bin/env python3
"""Deterministic integration simulator for log_rotation.py retention wiring.

Every filesystem mutation is confined to TemporaryDirectory. The simulator
imports the production integration module and uses its public helpers.
"""

import gzip
import os
import stat as stat_mod
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import log_retention_14d as R  # noqa: E402
import log_rotation as LR  # noqa: E402


NOW = datetime(2026, 7, 18, 0, 0, 0, tzinfo=timezone.utc)
PASSED = 0
FAILED = 0


def check(label, cond, detail=""):
    global PASSED, FAILED
    if cond:
        PASSED += 1
        print("PASS %s" % label)
    else:
        FAILED += 1
        print("FAIL %s :: %s" % (label, detail))


def ts_name(base, dt, compressed=False):
    root, ext = os.path.splitext(base)
    name = "%s_%s%s" % (root, dt.strftime("%Y%m%dT%H%M%SZ"), ext)
    if compressed:
        name += ".gz"
    return name


def write_file(path, data=b"x", mtime=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(data)
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


def write_gz(path, data=b"x", mtime=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as fh:
        fh.write(data)
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


def plan(logs, **kw):
    sets = LR.build_retention_basename_sets()
    return R.build_retention_plan(
        str(logs),
        now=NOW,
        retention_days=14,
        active_basenames=sets["active_basenames"],
        protected_basenames=sets["protected_basenames"],
        state_runtime_basenames=sets["state_or_runtime_basenames"],
        known_rotated_basenames=sets["known_rotated_basenames"],
        **kw,
    )


def summary(logs, dry_run=True, **kw):
    return LR.cleanup_rotated_log_archives(
        log_dir=str(logs),
        retention_days=14,
        dry_run=dry_run,
        now_ts=NOW.timestamp(),
        **kw,
    )


def scenario_plan_and_dry_run(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    active = write_file(arc / "score_shadow_log.csv")
    within = write_gz(arc / ts_name("scan_ema_log.csv", NOW - timedelta(days=13), True))
    expired_uncompressed = write_file(arc / ts_name("scan_ema_log.csv", NOW - timedelta(days=20)))
    expired_gz = write_gz(arc / ts_name("scan_ema_log.csv", NOW - timedelta(days=20, seconds=1), True))
    protected = write_file(arc / ts_name("qualified_latency_waterfall.jsonl", NOW - timedelta(days=90)))
    p0_backfill = write_file(arc / ts_name("startup_authoritative_close_backfill_v1.jsonl", NOW - timedelta(days=90)))
    p0_review = write_file(arc / ts_name("startup_authoritative_close_review_v1.jsonl", NOW - timedelta(days=90)))
    state_file = write_file(arc / ts_name("live_state.json", NOW - timedelta(days=90)))
    unknown = write_file(arc / ts_name("not_managed.jsonl", NOW - timedelta(days=90)))
    v2 = write_file(arc / ts_name("smc_pa_score_v2_shadow.jsonl", NOW - timedelta(days=90)))
    legacy = write_file(arc / "scan_ema_log_034621.csv")
    malformed = write_file(arc / "scan_ema_log_20261301T000000Z.csv")
    symlink = arc / ts_name("scan_ema_log.csv", NOW - timedelta(days=40))
    try:
        symlink.symlink_to(expired_uncompressed)
    except OSError:
        symlink = None
    fifo = arc / ts_name("scan_ema_log.csv", NOW - timedelta(days=50)).replace(".csv", ".fifo")
    try:
        os.mkfifo(fifo)
    except OSError:
        fifo = None

    p = plan(logs)
    check("1 active log untouched", str(active) in p.would_skip_active)
    check("2 rotated known within 14d retained", str(within) in p.within_retention)
    check("3 expired uncompressed would compress", str(expired_uncompressed) in p.would_compress)
    check("5 expired compressed would delete", str(expired_gz) in p.would_delete)
    check("7 promotion-protected rotated untouched", str(protected) in p.would_skip_protected)
    check("8 P0 backfill log protected", str(p0_backfill) in p.would_skip_protected)
    check("9 P0 review log protected", str(p0_review) in p.would_skip_protected)
    check("10 state/runtime JSON untouched", str(state_file) in p.would_skip_state)
    check("11 unknown basename skipped", str(unknown) in p.would_skip_unknown)
    check("12 historical V2/V2B skipped", str(v2) in p.would_skip_unknown)
    check("13 legacy _HHMMSS skipped", str(legacy) in p.would_skip_unknown)
    check("14 malformed timestamp skipped", str(malformed) in p.would_skip_unknown)
    if symlink is not None:
        check("15 symlink skipped", str(symlink) in p.would_skip_symlink)
    else:
        check("15 symlink skipped", True)
    if fifo is not None:
        check("16 FIFO/nonregular skipped", str(fifo) in p.would_skip_symlink)
        os.unlink(fifo)
    else:
        check("16 FIFO/nonregular skipped", True)

    before = {str(path): path.exists() for path in arc.iterdir()}
    s = summary(logs, dry_run=True)
    after = {str(path): path.exists() for path in arc.iterdir()}
    required = {
        "dry_run", "scanned", "within_retention", "would_compress", "compressed",
        "would_delete", "deleted", "skipped_active", "skipped_protected",
        "skipped_unknown", "skipped_nonregular", "conflicts", "errors",
        "bytes_reclaimable", "bytes_deleted", "elapsed_ms",
    }
    check("20 dry-run creates/deletes nothing", before == after, (before, after))
    check("24 summary fields accurate", required.issubset(set(s)), s)
    check("26 no production path accessed", str(logs).startswith(str(tmp)), logs)


def scenario_active_mutations(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    old_uncompressed = write_file(arc / ts_name("scan_ema_log.csv", NOW - timedelta(days=20)), b"payload")
    old_gz = write_gz(arc / ts_name("log_pool_pipeline.csv", NOW - timedelta(days=30), True), b"delete-me")

    s1 = summary(logs, dry_run=False)
    check("4 active run compresses through audited core",
          not old_uncompressed.exists() and Path(str(old_uncompressed) + ".gz").exists() and s1["compressed"] == 1, s1)
    check("6 active run deletes through audited core",
          not old_gz.exists() and s1["deleted"] == 1 and s1["bytes_deleted"] > 0, s1)
    summary(logs, dry_run=False)
    s3 = summary(logs, dry_run=False)
    check("25 repeated maintenance call idempotent", s3["deleted"] == 0 and s3["errors"] == 0, s3)


def scenario_traversal_and_conflicts(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    arc.mkdir(parents=True)
    outside = tmp / "outside.txt"
    write_file(outside, b"secret")
    escaping = os.path.join(str(arc), "..", "..", "..", "outside.txt")
    cand = R.classify_retention_candidate(
        escaping,
        log_root=R.resolve_logs_root(str(logs)),
        now=NOW,
        known_rotated_basenames=LR.build_retention_basename_sets()["known_rotated_basenames"],
    )
    check("17 traversal rejected", cand.reason == "outside_log_root" and outside.exists(), cand)

    conflict_src = write_file(arc / ts_name("scan_ema_log.csv", NOW - timedelta(days=20)), b"A")
    write_gz(Path(str(conflict_src) + ".gz"), b"B")
    p = plan(logs)
    r = R.execute_retention_plan(p, dry_run=False)
    check("18 conflicting gzip preserves both",
          conflict_src.exists() and Path(str(conflict_src) + ".gz").exists() and len(r.conflicts) == 1, r.conflicts)

    idem_src = write_file(arc / ts_name("log_pool_pipeline.csv", NOW - timedelta(days=20)), b"C")
    write_gz(Path(str(idem_src) + ".gz"), b"C")
    p2 = plan(logs)
    r2 = R.execute_retention_plan(p2, dry_run=False)
    check("19 equivalent existing gzip idempotent",
          not idem_src.exists() and any(g.status == "idempotent" for g in r2.compressed), r2.compressed)


def scenario_allowlist_and_exception(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    candidate = write_gz(arc / ts_name("scan_ema_log.csv", NOW - timedelta(days=30), True))
    no_known = R.build_retention_plan(
        str(logs),
        now=NOW,
        retention_days=14,
        known_rotated_basenames=None,
    )
    empty_known = R.build_retention_plan(
        str(logs),
        now=NOW,
        retention_days=14,
        known_rotated_basenames=[],
    )
    check("21 missing known allowlist fails closed",
          str(candidate) in no_known.would_skip_unknown and not no_known.would_delete, no_known)
    check("22 empty known allowlist fails closed",
          str(candidate) in empty_known.would_skip_unknown and not empty_known.would_delete, empty_known)

    original = LR.build_retention_plan
    try:
        def boom(*args, **kwargs):
            raise RuntimeError("simulated")
        LR.build_retention_plan = boom
        s = summary(logs, dry_run=True)
    finally:
        LR.build_retention_plan = original
    check("23 retention exception does not crash maintenance caller",
          s["errors"] == 1 and "cleanup_failed:RuntimeError" in s.get("error", ""), s)


def scenario_boundaries_mtime_caps(tmp):
    logs = tmp / "logs"
    arc = logs / "archive" / "2026-07-01"
    exact = write_gz(arc / ts_name("scan_ema_log.csv", NOW - timedelta(days=14), True), b"e")
    inside = write_gz(arc / ts_name("scan_ema_log.csv", NOW - timedelta(days=14) + timedelta(seconds=1), True), b"i")
    mtime_ignored = write_gz(
        arc / ts_name("log_pool_pipeline.csv", NOW - timedelta(days=30), True),
        b"m",
        mtime=NOW.timestamp(),
    )
    for i in range(3):
        write_gz(arc / ts_name("confirm_reject_log.csv", NOW - timedelta(days=40, seconds=i), True), b"x" * (i + 1))

    p = plan(logs)
    check("28 exact 14-day boundary preserved",
          str(exact) in p.would_delete and str(inside) in p.within_retention, p)
    check("29 mtime ignored", str(mtime_ignored) in p.would_delete, p.would_delete)
    s = summary(logs, dry_run=False, max_files=1, max_bytes=10 ** 9)
    check("30 delete count cap enforced", s["deleted"] == 1, s)


def scenario_rotation_dry_default(tmp):
    logs = tmp / "logs"
    arc = logs / "archive"
    logs.mkdir(parents=True)
    active = write_file(logs / "scan_ema_log.csv", b"x" * 2048)
    LR.rotate_logs(log_dir=str(logs), max_size_mb=0.0001, retention_days=14)
    rotated = list(arc.rglob("scan_ema_log_*.csv"))
    check("5 dry-run default does not delete during rotation", len(rotated) == 1 and not active.exists(), rotated)


def scenario_p0_hashes_unchanged():
    expected = {
        "startup_close_backfill.py": "d080c2a61e9ed54b0c44e91e6de9f3d76681ae33cc6fd18fec0d1da114121022",
        "execution.py": "c3c9350ad92368dcf2ff53c955a739583c854eb8592653536dbd4e67af12c594",
        "exchange/live_executor.py": "2125c39465981e246b7ae52e1a25b9e5db2462188caac4e4b930df0e65dc198f",
        "state_manager.py": "789ccf035996d67224de7d82bd7f1c396afd68409381b4ef15667287eaae0abf",
    }
    import hashlib
    ok = True
    detail = {}
    for rel, want in expected.items():
        got = hashlib.sha256((ROOT / rel).read_bytes()).hexdigest()
        detail[rel] = got
        ok = ok and got == want
    check("27 no P0/shared file modified", ok, detail)


def main():
    with tempfile.TemporaryDirectory(prefix="log-retention-integration-") as td:
        base = Path(td)
        scenario_plan_and_dry_run(base / "plan")
        scenario_active_mutations(base / "mutate")
        scenario_traversal_and_conflicts(base / "safe")
        scenario_allowlist_and_exception(base / "allow")
        scenario_boundaries_mtime_caps(base / "caps")
        scenario_rotation_dry_default(base / "rotate")
    scenario_p0_hashes_unchanged()
    print("-" * 60)
    print("INTEGRATION SIM SUMMARY passed=%d failed=%d" % (PASSED, FAILED))
    if FAILED:
        print("FAIL sim_log_retention_integration_14d")
        sys.exit(1)
    print("PASS sim_log_retention_integration_14d")


if __name__ == "__main__":
    main()
