"""Isolated 14-day rotated-log retention core.

This module is intentionally self-contained: it imports only the Python
standard library and MUST NOT import execution.py, state_manager.py,
log_rotation.py or any strategy/runtime module.

It performs NO work at import time. Nothing here mutates the filesystem
unless a caller explicitly invokes an execute/prune function with
``dry_run=False``. The default posture everywhere is dry-run.

Design (per P1 spec):

  * Age is derived from the rotated *filename timestamp*, never from
    ``mtime`` when a valid filename timestamp exists. Files whose name
    carries no parseable timestamp are classified UNKNOWN and skipped.
  * All paths are confined to a single resolved logs root.
  * Symlinks and non-regular files are refused.
  * Compression is streaming, verified (decompress + size/CRC compare),
    fsynced and atomically renamed before the source is removed.
  * Deletion is one file at a time, only for expired compressed/rotated
    archives that pass every safety predicate.
"""

from __future__ import annotations

import fnmatch
import gzip
import os
import re
import stat as stat_mod
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Iterator, Optional

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

DEFAULT_RETENTION_DAYS = 14
DEFAULT_CHUNK_SIZE = 1024 * 1024  # bounded streaming read size (1 MiB)
ARCHIVE_SUBDIR = "archive"

# Retention candidate categories.
ACTIVE_LOG = "ACTIVE_LOG"
ROTATED_UNCOMPRESSED = "ROTATED_UNCOMPRESSED"
ROTATED_COMPRESSED = "ROTATED_COMPRESSED"
PROMOTION_PROTECTED = "PROMOTION_PROTECTED"
STATE_OR_RUNTIME_FILE = "STATE_OR_RUNTIME_FILE"
UNKNOWN_OR_UNPARSEABLE = "UNKNOWN_OR_UNPARSEABLE"
SYMLINK_OR_NONREGULAR = "SYMLINK_OR_NONREGULAR"

# Known rotated timestamp patterns, tried in order of specificity. Every
# pattern is evaluated against the filename AFTER a single trailing ".gz"
# has been stripped. A date component of 8 digits (or YYYY-MM-DD) is
# required; a rotation stamp that carries only a time-of-day (e.g. the
# legacy ``_HHMMSS`` convention) is deliberately NOT parseable here and is
# reported UNKNOWN rather than deleted.
_TS_PATTERNS = (
    # base_YYYYMMDDTHHMMSSZ.ext   (current live convention)
    (re.compile(r"_(\d{8})T(\d{6})Z(?=\.[^.]*$|$)"), "%Y%m%d%H%M%S"),
    # name.jsonl.YYYYMMDD_HHMMSS
    (re.compile(r"[._](\d{8})_(\d{6})(?=\.|$)"), "%Y%m%d%H%M%S"),
    # name.jsonl.YYYYMMDD
    (re.compile(r"[._](\d{8})(?=\.|$)"), "%Y%m%d"),
    # name.YYYY-MM-DD.log
    (re.compile(r"[._](\d{4}-\d{2}-\d{2})(?=\.|$)"), "%Y-%m-%d"),
)


# --------------------------------------------------------------------------
# Timestamp parsing
# --------------------------------------------------------------------------

def _strip_gz(name: str) -> str:
    if name.endswith(".gz"):
        return name[:-3]
    return name


def parse_rotated_log_timestamp(name: str) -> Optional[datetime]:
    """Return a UTC-aware datetime for a rotated log filename, else None.

    Only explicitly known rotated formats are accepted. mtime is never
    consulted here. An unparseable name yields None (caller skips it).
    """
    if not name:
        return None
    stem = _strip_gz(name)
    for pattern, fmt in _TS_PATTERNS:
        match = pattern.search(stem)
        if not match:
            continue
        raw = "".join(match.groups())
        try:
            parsed = datetime.strptime(raw, fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc)
    return None


def _is_compressed_name(name: str) -> bool:
    return name.endswith(".gz")


def _stem_matches_any(name: str, basenames: Optional[Iterable[str]]) -> bool:
    """True iff ``name`` is a rotated copy (``{root}_*{ext}`` optionally +
    ``.gz``) of one of ``basenames``. Empty/None ``basenames`` -> False
    (i.e. nothing matches when no list is supplied).
    """
    if not basenames:
        return False
    for base in basenames:
        root, ext = os.path.splitext(str(base))
        if fnmatch.fnmatchcase(name, "%s_*%s" % (root, ext)):
            return True
        if fnmatch.fnmatchcase(name, "%s_*%s.gz" % (root, ext)):
            return True
    return False


def _matches_known_rotated(name: str, known_rotated_basenames: Optional[Iterable[str]]) -> bool:
    """Deletion whitelist gate.

    A candidate name must be a rotated copy of one of the supplied basenames.
    When no allowlist is supplied, nothing is eligible (fail-closed).
    """
    if not known_rotated_basenames:
        return False
    return _stem_matches_any(name, known_rotated_basenames)


# --------------------------------------------------------------------------
# Path safety
# --------------------------------------------------------------------------

def resolve_logs_root(log_dir: str) -> str:
    """Resolve the configured logs root exactly once."""
    return os.path.realpath(log_dir)


def _is_within(path: str, root: str) -> bool:
    """True iff ``path`` is inside (or equal to) ``root`` — no traversal."""
    try:
        common = os.path.commonpath([os.path.realpath(path), root])
    except ValueError:
        return False
    return common == root


def _norm_basenames(names: Optional[Iterable[str]]) -> frozenset:
    if not names:
        return frozenset()
    return frozenset(str(n) for n in names)


# --------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------

@dataclass
class Candidate:
    path: str
    name: str
    category: str
    reason: str = ""
    size: int = 0
    timestamp: Optional[datetime] = None
    age_days: Optional[float] = None
    compressed: bool = False
    expired: bool = False


def classify_retention_candidate(
    path: str,
    *,
    log_root: str,
    now: datetime,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    active_basenames: Optional[Iterable[str]] = None,
    protected_basenames: Optional[Iterable[str]] = None,
    state_runtime_basenames: Optional[Iterable[str]] = None,
    known_rotated_basenames: Optional[Iterable[str]] = None,
) -> Candidate:
    """Classify a single filesystem path into a retention category.

    Never follows symlinks; uses ``lstat`` first. Age is decided by the
    filename timestamp only. ``now`` must be a UTC-aware datetime.
    """
    name = os.path.basename(path)
    active = _norm_basenames(active_basenames)
    protected = _norm_basenames(protected_basenames)
    state = _norm_basenames(state_runtime_basenames)

    cand = Candidate(path=path, name=name, category=UNKNOWN_OR_UNPARSEABLE)

    # Confinement is checked without resolving through symlinks first: the
    # lexical parent must be inside root.
    if not _is_within(os.path.dirname(path), log_root):
        cand.category = UNKNOWN_OR_UNPARSEABLE
        cand.reason = "outside_log_root"
        return cand

    try:
        st = os.lstat(path)
    except OSError as exc:
        cand.category = UNKNOWN_OR_UNPARSEABLE
        cand.reason = "lstat_failed:%s" % type(exc).__name__
        return cand

    mode = st.st_mode
    if stat_mod.S_ISLNK(mode):
        cand.category = SYMLINK_OR_NONREGULAR
        cand.reason = "symlink"
        return cand
    if not stat_mod.S_ISREG(mode):
        cand.category = SYMLINK_OR_NONREGULAR
        cand.reason = "non_regular_file"
        return cand

    cand.size = st.st_size
    cand.compressed = _is_compressed_name(name)

    # Protected / state files win regardless of parseability. Protection is
    # stem-aware: a rotated copy (``base_<timestamp>.ext``) of a protected
    # basename is protected too, so promotion logs that get rotated are never
    # deleted. Active matching is exact-only: a *rotated* copy of an active
    # log is an ordinary archive and remains retention-eligible.
    if name in protected or _stem_matches_any(name, protected):
        cand.category = PROMOTION_PROTECTED
        cand.reason = "protected_basename"
        return cand
    if name in state or _stem_matches_any(name, state):
        cand.category = STATE_OR_RUNTIME_FILE
        cand.reason = "state_or_runtime_basename"
        return cand
    if name in active:
        cand.category = ACTIVE_LOG
        cand.reason = "active_basename"
        return cand

    ts = parse_rotated_log_timestamp(name)
    if ts is None:
        cand.category = UNKNOWN_OR_UNPARSEABLE
        cand.reason = "unparseable_timestamp"
        return cand

    if not _matches_known_rotated(name, known_rotated_basenames):
        cand.category = UNKNOWN_OR_UNPARSEABLE
        cand.reason = "basename_not_whitelisted"
        return cand

    cand.timestamp = ts
    age = now - ts
    cand.age_days = age.total_seconds() / 86400.0
    cutoff = _cutoff(now, retention_days)
    cand.expired = ts <= cutoff
    cand.category = ROTATED_COMPRESSED if cand.compressed else ROTATED_UNCOMPRESSED
    cand.reason = "expired" if cand.expired else "within_retention"
    return cand


def _cutoff(now: datetime, retention_days: int) -> datetime:
    """Cutoff instant. A file is expired when its timestamp <= cutoff.

    i.e. an age of exactly ``retention_days`` (or greater) is expired.
    """
    from datetime import timedelta

    return now - timedelta(days=float(retention_days))


# --------------------------------------------------------------------------
# Directory scan (bounded, no symlink follow)
# --------------------------------------------------------------------------

def _iter_regular_files(root: str) -> Iterator[str]:
    """Yield regular-file paths under ``root`` without following symlinks.

    Directories are descended; symlinked directories are NOT followed.
    Symlinked files and other non-regular entries are still yielded so the
    classifier can report them (it refuses them safely).
    """
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
                        else:
                            yield entry.path
                    except OSError:
                        yield entry.path
        except OSError:
            continue


# --------------------------------------------------------------------------
# Retention plan
# --------------------------------------------------------------------------

@dataclass
class RetentionPlan:
    log_root: str
    retention_days: int
    now: datetime
    gzip_uncompressed: bool
    would_compress: list = field(default_factory=list)
    would_delete: list = field(default_factory=list)
    would_skip_active: list = field(default_factory=list)
    would_skip_protected: list = field(default_factory=list)
    would_skip_state: list = field(default_factory=list)
    would_skip_unknown: list = field(default_factory=list)
    would_skip_symlink: list = field(default_factory=list)
    within_retention: list = field(default_factory=list)
    conflicts: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    scanned: int = 0
    bytes_reclaimable: int = 0
    bytes_compressed_estimate: int = 0
    known_rotated_basenames: Optional[frozenset] = None
    active_basenames: frozenset = field(default_factory=frozenset)
    protected_basenames: frozenset = field(default_factory=frozenset)
    state_runtime_basenames: frozenset = field(default_factory=frozenset)


def build_retention_plan(
    log_dir: str,
    *,
    now: Optional[datetime] = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    active_basenames: Optional[Iterable[str]] = None,
    protected_basenames: Optional[Iterable[str]] = None,
    state_runtime_basenames: Optional[Iterable[str]] = None,
    known_rotated_basenames: Optional[Iterable[str]] = None,
    gzip_uncompressed: bool = True,
) -> RetentionPlan:
    """Scan the archive subtree and produce a deterministic dry-run plan.

    No filesystem mutation occurs here. ``now`` defaults to current UTC.
    Results are sorted by path for determinism.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    log_root = resolve_logs_root(log_dir)
    known_rotated = _norm_basenames(known_rotated_basenames)
    plan = RetentionPlan(
        log_root=log_root,
        retention_days=retention_days,
        now=now,
        gzip_uncompressed=gzip_uncompressed,
        known_rotated_basenames=known_rotated,
        active_basenames=_norm_basenames(active_basenames),
        protected_basenames=_norm_basenames(protected_basenames),
        state_runtime_basenames=_norm_basenames(state_runtime_basenames),
    )

    archive_base = os.path.join(log_root, ARCHIVE_SUBDIR)
    archive_real = os.path.realpath(archive_base)
    if not _is_within(archive_real, log_root) or not os.path.isdir(archive_real):
        return plan

    candidates = []
    for path in _iter_regular_files(archive_real):
        plan.scanned += 1
        cand = classify_retention_candidate(
            path,
            log_root=log_root,
            now=now,
            retention_days=retention_days,
            active_basenames=active_basenames,
            protected_basenames=protected_basenames,
            state_runtime_basenames=state_runtime_basenames,
            known_rotated_basenames=known_rotated,
        )
        candidates.append(cand)

    for cand in sorted(candidates, key=lambda c: c.path):
        _place_candidate(plan, cand)

    # Stable, sorted output lists.
    for bucket in (
        plan.would_compress,
        plan.would_delete,
        plan.would_skip_active,
        plan.would_skip_protected,
        plan.would_skip_state,
        plan.would_skip_unknown,
        plan.would_skip_symlink,
        plan.within_retention,
    ):
        bucket.sort()
    return plan


def _place_candidate(plan: RetentionPlan, cand: Candidate) -> None:
    if cand.category == ACTIVE_LOG:
        plan.would_skip_active.append(cand.path)
    elif cand.category == PROMOTION_PROTECTED:
        plan.would_skip_protected.append(cand.path)
    elif cand.category == STATE_OR_RUNTIME_FILE:
        plan.would_skip_state.append(cand.path)
    elif cand.category == SYMLINK_OR_NONREGULAR:
        plan.would_skip_symlink.append(cand.path)
    elif cand.category == UNKNOWN_OR_UNPARSEABLE:
        plan.would_skip_unknown.append(cand.path)
    elif cand.category == ROTATED_COMPRESSED:
        if cand.expired:
            plan.would_delete.append(cand.path)
            plan.bytes_reclaimable += cand.size
        else:
            plan.within_retention.append(cand.path)
    elif cand.category == ROTATED_UNCOMPRESSED:
        if cand.expired:
            # Expired but still uncompressed: compress first, delete on the
            # next pass once it is a compressed archive past cutoff. We do
            # not delete an uncompressed rotated file directly.
            if plan.gzip_uncompressed:
                plan.would_compress.append(cand.path)
                plan.bytes_compressed_estimate += cand.size
            else:
                plan.within_retention.append(cand.path)
        else:
            if plan.gzip_uncompressed:
                plan.would_compress.append(cand.path)
                plan.bytes_compressed_estimate += cand.size
            else:
                plan.within_retention.append(cand.path)


# --------------------------------------------------------------------------
# Streaming gzip with verification
# --------------------------------------------------------------------------

@dataclass
class GzipResult:
    src: str
    dst: Optional[str] = None
    status: str = ""  # compressed | idempotent | conflict | error | skipped
    src_size: int = 0
    src_crc: int = 0
    removed_source: bool = False
    error: str = ""


def _hash_and_size(path: str, chunk_size: int) -> tuple:
    crc = 0
    size = 0
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            size += len(chunk)
            crc = zlib.crc32(chunk, crc)
    return size, crc & 0xFFFFFFFF


def verify_gzip_archive(
    gz_path: str,
    *,
    expected_size: Optional[int] = None,
    expected_crc: Optional[int] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> bool:
    """Fully decompress ``gz_path`` and optionally compare size/CRC."""
    crc = 0
    size = 0
    try:
        with gzip.open(gz_path, "rb") as handle:
            while True:
                chunk = handle.read(chunk_size)
                if not chunk:
                    break
                size += len(chunk)
                crc = zlib.crc32(chunk, crc)
    except (OSError, EOFError, gzip.BadGzipFile, zlib.error):
        return False
    if expected_size is not None and size != expected_size:
        return False
    if expected_crc is not None and (crc & 0xFFFFFFFF) != (expected_crc & 0xFFFFFFFF):
        return False
    return True


def _fsync_path(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _fsync_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    try:
        fd = os.open(parent, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def gzip_archive_streaming(
    src_path: str,
    *,
    log_root: Optional[str] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    remove_source: bool = True,
    dry_run: bool = True,
) -> GzipResult:
    """Compress ``src_path`` -> ``src_path.gz`` safely.

    Streaming, bounded reads. Writes ``.gz.tmp.<pid>``, fsyncs it, verifies
    the archive decompresses to the exact source size/CRC, atomically
    renames to ``.gz``, fsyncs the parent dir, and only then removes the
    source. On any failure the source is retained and the temp is cleaned.

    If the final ``.gz`` already exists it is verified for equivalence:
    equal -> idempotent (source removed, no rewrite); not equal ->
    ``conflict`` (both preserved, nothing deleted).
    """
    result = GzipResult(src=src_path)

    name = os.path.basename(src_path)
    if log_root is not None and not _is_within(os.path.dirname(src_path), log_root):
        result.status = "error"
        result.error = "outside_log_root"
        return result

    try:
        st = os.lstat(src_path)
    except OSError as exc:
        result.status = "error"
        result.error = "lstat_failed:%s" % type(exc).__name__
        return result
    if stat_mod.S_ISLNK(st.st_mode) or not stat_mod.S_ISREG(st.st_mode):
        result.status = "error"
        result.error = "not_regular_file"
        return result
    if _is_compressed_name(name):
        result.status = "skipped"
        result.error = "already_compressed"
        return result

    try:
        src_size, src_crc = _hash_and_size(src_path, chunk_size)
    except OSError as exc:
        result.status = "error"
        result.error = "read_failed:%s" % type(exc).__name__
        return result
    result.src_size = src_size
    result.src_crc = src_crc

    dst = src_path + ".gz"

    # Handle a pre-existing final archive.
    if os.path.exists(dst):
        if verify_gzip_archive(dst, expected_size=src_size, expected_crc=src_crc,
                               chunk_size=chunk_size):
            result.dst = dst
            result.status = "idempotent"
            if not dry_run and remove_source:
                try:
                    os.unlink(src_path)
                    result.removed_source = True
                except OSError as exc:
                    result.error = "unlink_failed:%s" % type(exc).__name__
            return result
        result.dst = dst
        result.status = "conflict"
        result.error = "existing_gz_differs"
        return result

    if dry_run:
        result.dst = dst
        result.status = "compressed"  # would-compress
        return result

    tmp = "%s.tmp.%d" % (dst, os.getpid())
    try:
        with open(src_path, "rb") as src, gzip.open(tmp, "wb") as gz:
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                gz.write(chunk)
        _fsync_path(tmp)
    except OSError as exc:
        _safe_unlink(tmp)
        result.status = "error"
        result.error = "compress_failed:%s" % type(exc).__name__
        return result

    if not verify_gzip_archive(tmp, expected_size=src_size, expected_crc=src_crc,
                               chunk_size=chunk_size):
        _safe_unlink(tmp)
        result.status = "error"
        result.error = "verify_failed"
        return result

    # Never clobber a valid archive that appeared concurrently.
    if os.path.exists(dst):
        _safe_unlink(tmp)
        result.dst = dst
        result.status = "conflict"
        result.error = "dst_appeared_during_compress"
        return result

    try:
        os.replace(tmp, dst)
        _fsync_parent_dir(dst)
    except OSError as exc:
        _safe_unlink(tmp)
        result.status = "error"
        result.error = "replace_failed:%s" % type(exc).__name__
        return result

    result.dst = dst
    result.status = "compressed"
    if remove_source:
        try:
            os.unlink(src_path)
            result.removed_source = True
        except OSError as exc:
            result.error = "unlink_failed:%s" % type(exc).__name__
    return result


def _safe_unlink(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


# --------------------------------------------------------------------------
# Pruning expired archives
# --------------------------------------------------------------------------

@dataclass
class DeleteResult:
    path: str
    status: str  # deleted | would_delete | skipped | error
    size: int = 0
    reason: str = ""


def prune_expired_archives(
    plan: RetentionPlan,
    *,
    dry_run: bool = True,
    max_files: Optional[int] = None,
    max_bytes: Optional[int] = None,
) -> list:
    """Delete the expired compressed archives named in ``plan.would_delete``.

    One file at a time, each re-validated immediately before unlink:
    inside root, regular file, not a symlink, still expired by filename
    timestamp. No wildcard/batch deletion. Returns per-file results.
    """
    results = []
    deleted_files = 0
    deleted_bytes = 0
    for path in plan.would_delete:
        if max_files is not None and deleted_files >= max_files:
            results.append(DeleteResult(path, "skipped", reason="max_files_cap"))
            continue

        # Re-validate at delete time.
        cand = classify_retention_candidate(
            path,
            log_root=plan.log_root,
            now=plan.now,
            retention_days=plan.retention_days,
            active_basenames=plan.active_basenames,
            protected_basenames=plan.protected_basenames,
            state_runtime_basenames=plan.state_runtime_basenames,
            known_rotated_basenames=plan.known_rotated_basenames,
        )
        if cand.category != ROTATED_COMPRESSED or not cand.expired:
            results.append(DeleteResult(path, "skipped", size=cand.size,
                                        reason="revalidation_failed:%s" % cand.reason))
            continue
        if max_bytes is not None and deleted_bytes + cand.size > max_bytes:
            results.append(DeleteResult(path, "skipped", size=cand.size,
                                        reason="max_bytes_cap"))
            continue

        if dry_run:
            results.append(DeleteResult(path, "would_delete", size=cand.size))
            deleted_files += 1
            deleted_bytes += cand.size
            continue

        try:
            os.unlink(path)
            results.append(DeleteResult(path, "deleted", size=cand.size))
            deleted_files += 1
            deleted_bytes += cand.size
        except OSError as exc:
            results.append(DeleteResult(path, "error", size=cand.size,
                                        reason=type(exc).__name__))
    return results


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

@dataclass
class RetentionResult:
    dry_run: bool
    plan: RetentionPlan
    compressed: list = field(default_factory=list)
    deleted: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    conflicts: list = field(default_factory=list)


def execute_retention_plan(
    plan: RetentionPlan,
    *,
    dry_run: bool = True,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_delete_files: Optional[int] = None,
    max_delete_bytes: Optional[int] = None,
) -> RetentionResult:
    """Execute (or dry-run) a retention plan: compress, then prune.

    Compression runs first so that files compressed this pass become
    eligible for deletion only on a subsequent pass (never same-pass).
    """
    result = RetentionResult(dry_run=dry_run, plan=plan)

    for src in plan.would_compress:
        gz = gzip_archive_streaming(
            src, log_root=plan.log_root, chunk_size=chunk_size, dry_run=dry_run,
        )
        result.compressed.append(gz)
        if gz.status == "conflict":
            result.conflicts.append(gz)
        elif gz.status == "error":
            result.errors.append(gz)

    if result.conflicts:
        conflict_dsts = {g.dst for g in result.conflicts if g.dst}
        plan.conflicts.extend(sorted(conflict_dsts))
        plan.would_delete = [path for path in plan.would_delete if path not in conflict_dsts]

    deletes = prune_expired_archives(
        plan,
        dry_run=dry_run,
        max_files=max_delete_files,
        max_bytes=max_delete_bytes,
    )
    result.deleted = deletes
    for d in deletes:
        if d.status == "error":
            result.errors.append(d)
    return result


def summarize_retention_result(result: RetentionResult) -> dict:
    """Flat, JSON-friendly summary of a retention run."""
    plan = result.plan
    compressed_ok = sum(1 for g in result.compressed
                        if g.status in ("compressed", "idempotent"))
    deleted_ok = sum(1 for d in result.deleted
                     if d.status in ("deleted", "would_delete"))
    return {
        "dry_run": result.dry_run,
        "retention_days": plan.retention_days,
        "now": plan.now.isoformat(),
        "scanned": plan.scanned,
        "would_compress": len(plan.would_compress),
        "would_delete": len(plan.would_delete),
        "would_skip_active": len(plan.would_skip_active),
        "would_skip_protected": len(plan.would_skip_protected),
        "would_skip_state": len(plan.would_skip_state),
        "would_skip_unknown": len(plan.would_skip_unknown),
        "would_skip_symlink": len(plan.would_skip_symlink),
        "within_retention": len(plan.within_retention),
        "conflicts": len(result.conflicts),
        "errors": len(result.errors),
        "bytes_reclaimable": plan.bytes_reclaimable,
        "bytes_compressed_estimate": plan.bytes_compressed_estimate,
        "compressed_ok": compressed_ok,
        "deleted_ok": deleted_ok,
    }
