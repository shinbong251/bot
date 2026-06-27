#!/usr/bin/env python3
"""Read-only simulator for PAPER CONFIRM_SMC_RESEARCH gap R calibration."""


PASS = "PASS"
FAIL = "FAIL"
results = []


def calibration_fields(t):
    if str(t.get("entry_type") or "").upper() != "CONFIRM_SMC_RESEARCH":
        return {}
    raw_r = t.get("rr_real")
    close_reason = str(t.get("close_reason") or t.get("exit_type") or "").upper()
    configured_gap_r = t.get("sl_gap")
    is_gap_loss = close_reason == "SL" and configured_gap_r is not None and configured_gap_r > 0 and raw_r < -1.0
    cap_1_0 = raw_r
    cap_1_2 = raw_r
    if is_gap_loss and raw_r <= -1.2:
        cap_1_0 = max(raw_r, -1.0)
        cap_1_2 = max(raw_r, -1.2)
    return {
        "raw_realized_r": raw_r,
        "calibrated_realized_r": cap_1_0,
        "adjusted_realized_r": cap_1_0,
        "calibrated_r_cap_1_0": cap_1_0,
        "calibrated_r_cap_1_2": cap_1_2,
        "is_gap_loss": is_gap_loss,
        "gap_overcharge_r": round(cap_1_0 - raw_r, 6),
    }


def check(label, condition, detail=""):
    status = PASS if condition else FAIL
    results.append((label, status, detail))
    print(f"{status} {label}{' ' + detail if detail else ''}")


def run_case(label, trade, expected_10, expected_12, expected_raw=None, excluded=False):
    before_raw = trade.get("rr_real")
    out = calibration_fields(dict(trade))
    if excluded:
        check(label + " excluded", out == {}, f"out={out}")
        return
    check(label + " raw preserved", trade.get("rr_real") == before_raw)
    check(label + " raw field", out.get("raw_realized_r") == (expected_raw if expected_raw is not None else before_raw))
    check(label + " cap_1_0", out.get("calibrated_r_cap_1_0") == expected_10, f"got={out.get('calibrated_r_cap_1_0')}")
    check(label + " cap_1_2", out.get("calibrated_r_cap_1_2") == expected_12, f"got={out.get('calibrated_r_cap_1_2')}")


def main():
    base = {
        "entry_type": "CONFIRM_SMC_RESEARCH",
        "exit_type": "SL",
        "close_reason": "SL",
        "sl_gap": 0.5,
    }
    run_case("normal SL -1.0R", dict(base, rr_real=-1.0), -1.0, -1.0)
    run_case("gap SL -1.5R", dict(base, rr_real=-1.5), -1.0, -1.2)
    run_case("gap SL -1.3R", dict(base, rr_real=-1.3, sl_gap=0.3), -1.0, -1.2)
    run_case("BE 0R unchanged", dict(base, rr_real=0.0, exit_type="BE", close_reason="BE"), 0.0, 0.0)
    run_case("TRAIL +0.5R unchanged", dict(base, rr_real=0.5, exit_type="TRAIL", close_reason="TRAIL"), 0.5, 0.5)
    run_case("WIN +1R unchanged", dict(base, rr_real=1.0, exit_type="TP", close_reason="TP"), 1.0, 1.0)
    run_case("non-research paper trade excluded", dict(base, entry_type="CONFIRM", rr_real=-1.5), None, None, excluded=True)

    failed = [row for row in results if row[1] == FAIL]
    print(f"\nSUMMARY total={len(results)} failed={len(failed)}")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
