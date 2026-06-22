"""
Single-instance process lock for the trading bot.

Prevents more than one bot process from running in the same working directory
(and therefore from both owning paper_state.json, which previously caused the
PermissionError file race and duplicate Telegram alerts).

Design goals:
  * fail-safe — if we cannot be sure no other bot is running, we refuse to start
  * dependency-free (no psutil) so no new dependency is introduced
  * never affect imports/tests — only acquire() is called, and only from main

The lock is a small JSON file (bot_instance.lock) storing pid, start time,
command and working directory.  acquire() is the only entry point used at
startup; release() is registered with atexit for normal shutdown.
"""

import os
import sys
import json
import time
import atexit

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCK_FILE = os.path.join(_BASE_DIR, "bot_instance.lock")

_acquired = False


def _pid_alive(pid):
    """
    Best-effort liveness check. Returns True if the pid is (or might be) running.

    Fail-safe: on any uncertainty we return True so a duplicate is refused
    rather than silently allowed.
    """
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False

    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if not handle:
                # Could not open: either it doesn't exist or access denied.
                # ERROR_INVALID_PARAMETER (87) means no such process.
                err = ctypes.get_last_error() if hasattr(ctypes, "get_last_error") else 0
                if err == 87:
                    return False
                # Access denied / unknown -> assume alive (fail-safe).
                return True
            exit_code = ctypes.c_ulong()
            ok = kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
            kernel32.CloseHandle(handle)
            STILL_ACTIVE = 259
            if not ok:
                return True
            return exit_code.value == STILL_ACTIVE
        except Exception:
            return True
    else:
        try:
            os.kill(pid, 0)
            return True
        except PermissionError:
            return True
        except OSError:
            return False
        except Exception:
            return True


def _read_lock():
    with open(LOCK_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_lock():
    payload = {
        "pid": os.getpid(),
        "start_time": time.time(),
        "start_time_human": time.strftime("%Y-%m-%d %H:%M:%S"),
        "command": " ".join(sys.argv),
        "cwd": os.getcwd(),
    }
    tmp = f"{LOCK_FILE}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, LOCK_FILE)


def acquire():
    """
    Acquire the single-instance lock.

    Returns True if acquired. Returns False if another live bot already owns it
    (caller should exit). Stale locks (dead pid) are reclaimed automatically.
    Unreadable lock files are treated fail-safe (refuse to start).
    """
    global _acquired

    if os.path.exists(LOCK_FILE):
        try:
            info = _read_lock()
        except Exception as e:
            print(
                f"[INSTANCE LOCK] FAIL-SAFE: lock file {LOCK_FILE} exists but cannot be read ({e}). "
                "Refusing to start. Stop any running bot and delete the lock if you are sure none is running."
            )
            return False

        other_pid = info.get("pid")
        other_cmd = str(info.get("command") or "")
        looks_like_bot = "main.py" in other_cmd or other_cmd == ""

        if other_pid == os.getpid():
            # Re-entry within the same process: already ours.
            _acquired = True
            atexit.register(release)
            return True

        if looks_like_bot and _pid_alive(other_pid):
            print(
                "[INSTANCE LOCK] Another bot instance is already running. Refusing to start.\n"
                f"  pid={other_pid} start={info.get('start_time_human')} "
                f"cwd={info.get('cwd')} cmd={other_cmd}"
            )
            return False

        # Stale lock (pid not alive) -> reclaim.
        print(
            f"[INSTANCE LOCK] Removing stale lock (pid={other_pid} not alive, "
            f"started {info.get('start_time_human')})."
        )
        try:
            os.remove(LOCK_FILE)
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"[INSTANCE LOCK] FAIL-SAFE: could not remove stale lock ({e}). Refusing to start.")
            return False

    try:
        _write_lock()
    except Exception as e:
        print(f"[INSTANCE LOCK] FAIL-SAFE: could not write lock file ({e}). Refusing to start.")
        return False

    _acquired = True
    atexit.register(release)
    print(f"[INSTANCE LOCK] Acquired (pid={os.getpid()}).")
    return True


def release():
    """Remove the lock file on normal shutdown, but only if we own it."""
    global _acquired
    if not _acquired:
        return
    try:
        if os.path.exists(LOCK_FILE):
            info = _read_lock()
            if info.get("pid") == os.getpid():
                os.remove(LOCK_FILE)
    except Exception:
        pass
    finally:
        _acquired = False
