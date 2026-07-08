#!/usr/bin/env python3
"""Request or cancel an in-bot PAPER DD rebaseline at the next flat window."""

import argparse
import json
import os
import time
from pathlib import Path


AUDIT_LOG = Path("logs") / "paper_dd_pause_events.jsonl"


def _load_config(path):
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("config.json is not an object")
    return data


def _write_config_atomic(path, data):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    os.replace(tmp, path)


def _append_audit(root, row):
    path = root / AUDIT_LOG
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Request/cancel pending PAPER-only DD rebaseline. Defaults to dry-run."
    )
    parser.add_argument("--apply", action="store_true", help="Persist the request/cancel")
    parser.add_argument("--dry-run", action="store_true", help="Preview only; default")
    parser.add_argument("--cancel", action="store_true", help="Cancel pending rebaseline")
    parser.add_argument("--reason", default="", help="Reason stored with the pending request")
    parser.add_argument("--operator", default="", help="Optional operator identifier")
    parser.add_argument("--root", default=".", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    config_path = root / "config.json"
    config = _load_config(config_path)

    before = {
        "paper_dd_rebaseline_pending": bool(config.get("paper_dd_rebaseline_pending", False)),
        "paper_dd_rebaseline_reason": config.get("paper_dd_rebaseline_reason", ""),
        "paper_dd_rebaseline_operator": config.get("paper_dd_rebaseline_operator", ""),
    }
    action = "CANCEL" if args.cancel else "REQUEST"
    updates = {
        "paper_dd_rebaseline_pending": False if args.cancel else True,
    }
    if args.cancel:
        updates["paper_dd_rebaseline_reason"] = config.get("paper_dd_rebaseline_reason", "")
        updates["paper_dd_rebaseline_operator"] = config.get("paper_dd_rebaseline_operator", "")
    else:
        updates["paper_dd_rebaseline_reason"] = args.reason
        updates["paper_dd_rebaseline_operator"] = args.operator

    print(f"mode={'APPLY' if args.apply else 'DRY_RUN'}")
    print("scope=PAPER_DD_REBASELINE_REQUEST_ONLY")
    print(f"action={action}")
    print(f"before={json.dumps(before, sort_keys=True)}")
    print(f"after={json.dumps(updates, sort_keys=True)}")

    if not args.apply:
        print("result=DRY_RUN_NO_WRITE")
        return 0

    config.update(updates)
    _write_config_atomic(config_path, config)
    if args.cancel:
        _append_audit(root, {
            "event_type": "PAPER_DD_PENDING_REBASELINE_CANCELLED",
            "ts": time.time(),
            "reason": args.reason or before.get("paper_dd_rebaseline_reason", ""),
            "operator": args.operator or before.get("paper_dd_rebaseline_operator", ""),
            "old_pending": before.get("paper_dd_rebaseline_pending"),
            "new_pending": False,
        })
    print("result=APPLIED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
