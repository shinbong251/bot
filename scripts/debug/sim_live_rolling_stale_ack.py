#!/usr/bin/env python3
"""Simulation-only executable spec for OPERATOR_GATED_STALE_ACK.

This is not production gate verification. It is a deterministic, stdlib-only
truth-table model for the proposed policy. It does not import production code,
read config/state files, touch network clients, or perform order actions.
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass, replace
from typing import Callable


NOW_TS = 2_000_000_000.0
FROZEN_LIVE_CLOSED_COUNT = 51
FROZEN_LAST_LIVE_CLOSE_KEY = "1782870085666"
FROZEN_LIVE_ROLLING_NET_R = -2.05
FROZEN_ACTIVE_EPOCH_NET_R = 4.29
FROZEN_CLOSE_AGE_HOURS = 228.5

ACK_MIN_NEWEST_CLOSE_AGE_HOURS = 120.0
ACK_MAX_DORMANT_LOSS_R = -5.0
HEALTH_ROW_MAX_AGE_SECONDS = 300.0
LIVE_ROLLING_FLOOR_R = -2.0

ACTION_BLOCK = "BLOCK_SCALE"
ACTION_ALLOW = "WARN_ALLOW_SCALE"
BLOCK_STALE_BINDER = "LIVE_SCALE_BLOCKED_STALE_ROLLING_BINDER"
BLOCK_HARD = "LIVE_SCALE_HARD_BLOCK"

ACK_OFF = "ack_off"
ACK_CONSUMED = "ack_consumed"
ACK_EXPIRED = "ack_expired"
ACK_NOT_STALE = "newest_close_not_stale_enough"
ACK_DEEP_LOSS = "dormant_loss_too_deep"
ACK_ACTIVE_EPOCH = "active_epoch_not_positive"
ACK_KEY = "last_live_close_key_mismatch"
ACK_COUNT = "live_closed_count_mismatch"
ACK_VALUE = "live_rolling_net_r_mismatch"


@dataclass(frozen=True)
class GateState:
    now_ts: float = NOW_TS
    last_live_close_key: str = FROZEN_LAST_LIVE_CLOSE_KEY
    live_closed_count: int = FROZEN_LIVE_CLOSED_COUNT
    live_rolling_net_r: float = FROZEN_LIVE_ROLLING_NET_R
    active_epoch_net_r: float = FROZEN_ACTIVE_EPOCH_NET_R
    newest_close_age_hours: float = FROZEN_CLOSE_AGE_HOURS
    paper_health: str = "RED"
    live_health: str = "RED"
    health_row_age_seconds: float = 10.0
    current_sl_missing_symbols: tuple[str, ...] = ()
    current_entry_unconfirmed_symbols: tuple[str, ...] = ()
    live_sl_sync_failure: bool = False
    active_micro_pause: bool = False
    current_non_stale_loss_streak: bool = False
    cap_pass: bool = True
    risk_pass: bool = True
    portfolio_pass: bool = True
    unrelated_paper_health_block: bool = False
    unrelated_live_health_block: bool = False


@dataclass
class AckState:
    enabled: bool = False
    last_live_close_key: str = FROZEN_LAST_LIVE_CLOSE_KEY
    live_closed_count: int = FROZEN_LIVE_CLOSED_COUNT
    live_rolling_net_r: float = FROZEN_LIVE_ROLLING_NET_R
    expires_ts: float = NOW_TS + 3600.0
    consumed: bool = False

    def serialize(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def deserialize(cls, payload: str) -> "AckState":
        return cls(**json.loads(payload))


@dataclass(frozen=True)
class RollingPolicy:
    branch: str
    positive_requirement_pass: bool
    rolling_floor_pass: bool
    stale_rolling_binder_active: bool


@dataclass(frozen=True)
class Decision:
    allowed: bool
    action: str
    reason: str
    ack_valid: bool
    ack_consumed: bool
    hard_blocks: tuple[str, ...]
    ack_reject_reasons: tuple[str, ...]
    rolling_policy: RollingPolicy


def valid_ack() -> AckState:
    return AckState(enabled=True)


def frozen_state(**overrides) -> GateState:
    return replace(GateState(), **overrides)


def derive_rolling_policy(state: GateState) -> RollingPolicy:
    """Derive rolling gate status from state data; no free binder override."""
    paper = state.paper_health.upper()
    if paper == "RED":
        branch = "paper_red_a3_path"
        positive_requirement_pass = state.active_epoch_net_r > 0
    elif paper == "GREEN":
        branch = "paper_green_rolling_floor_live_health_path"
        positive_requirement_pass = state.live_health.upper() in {"GREEN", "YELLOW", "RED"}
    else:
        branch = "paper_health_hard_block_path"
        positive_requirement_pass = False

    rolling_floor_pass = state.live_rolling_net_r > LIVE_ROLLING_FLOOR_R
    stale_rolling_binder_active = (
        not rolling_floor_pass
        and state.newest_close_age_hours >= ACK_MIN_NEWEST_CLOSE_AGE_HOURS
        and state.active_epoch_net_r > 0
        and paper in {"RED", "GREEN"}
    )
    return RollingPolicy(
        branch=branch,
        positive_requirement_pass=positive_requirement_pass,
        rolling_floor_pass=rolling_floor_pass,
        stale_rolling_binder_active=stale_rolling_binder_active,
    )


def _is_health_row_fresh(state: GateState) -> bool:
    return state.health_row_age_seconds <= HEALTH_ROW_MAX_AGE_SECONDS


def hard_blocks(state: GateState) -> tuple[str, ...]:
    blocks = []
    if not _is_health_row_fresh(state):
        blocks.append("health_row_stale")
    if state.current_sl_missing_symbols:
        blocks.append("sl_missing")
    if state.current_entry_unconfirmed_symbols:
        blocks.append("entry_unconfirmed")
    if state.live_sl_sync_failure:
        blocks.append("sl_sync_failure")
    if state.active_micro_pause:
        blocks.append("active_micro_pause")
    if state.current_non_stale_loss_streak:
        blocks.append("current_non_stale_loss_streak")
    if not state.cap_pass:
        blocks.append("cap_constraint")
    if not state.risk_pass:
        blocks.append("risk_constraint")
    if not state.portfolio_pass:
        blocks.append("portfolio_constraint")
    if state.unrelated_paper_health_block:
        blocks.append("unrelated_paper_health_block")
    if state.unrelated_live_health_block:
        blocks.append("unrelated_live_health_block")
    return tuple(blocks)


def ack_reject_reasons(state: GateState, ack: AckState | None) -> tuple[str, ...]:
    """Validate only ack snapshot/policy, not independent gate hard blocks."""
    reasons = []
    if ack is None or not ack.enabled:
        return (ACK_OFF,)
    if ack.consumed:
        reasons.append(ACK_CONSUMED)
    if ack.expires_ts <= state.now_ts:
        reasons.append(ACK_EXPIRED)
    if state.newest_close_age_hours < ACK_MIN_NEWEST_CLOSE_AGE_HOURS:
        reasons.append(ACK_NOT_STALE)
    if state.active_epoch_net_r <= 0:
        reasons.append(ACK_ACTIVE_EPOCH)
    if ack.last_live_close_key != state.last_live_close_key:
        reasons.append(ACK_KEY)
    if ack.live_closed_count != state.live_closed_count:
        reasons.append(ACK_COUNT)
    if ack.live_rolling_net_r != state.live_rolling_net_r:
        reasons.append(ACK_VALUE)
    if state.live_rolling_net_r <= ACK_MAX_DORMANT_LOSS_R:
        reasons.append(ACK_DEEP_LOSS)
    return tuple(reasons)


def evaluate_gate(state: GateState, ack: AckState | None) -> Decision:
    policy = derive_rolling_policy(state)
    blocks = hard_blocks(state)
    if blocks:
        return Decision(False, ACTION_BLOCK, BLOCK_HARD, False, False, blocks, (), policy)

    rejects = ack_reject_reasons(state, ack)
    ack_valid = not rejects

    if policy.rolling_floor_pass:
        return Decision(True, ACTION_ALLOW, "", ack_valid, False, (), rejects, policy)

    if not policy.stale_rolling_binder_active:
        return Decision(False, ACTION_BLOCK, BLOCK_STALE_BINDER, ack_valid, False, (), rejects, policy)

    if ack_valid and ack is not None:
        ack.consumed = True
        return Decision(True, ACTION_ALLOW, "", True, True, (), rejects, policy)

    return Decision(False, ACTION_BLOCK, BLOCK_STALE_BINDER, False, False, (), rejects, policy)


def _case_result(label: str, condition: bool, detail: str = "") -> bool:
    status = "PASS" if condition else "FAIL"
    print(f"{label:<46} {status:<4} {detail}")
    return condition


def _expect_block(decision: Decision, reason: str | None = None) -> bool:
    if decision.allowed is not False or decision.action != ACTION_BLOCK:
        return False
    return reason is None or decision.reason == reason


def _expect_allow(decision: Decision) -> bool:
    return decision.allowed is True and decision.action == ACTION_ALLOW


def case_off_by_default() -> tuple[bool, str]:
    default_ack = AckState()
    decision = evaluate_gate(frozen_state(), default_ack)
    ok = (
        default_ack.enabled is False
        and _expect_block(decision, BLOCK_STALE_BINDER)
        and decision.ack_reject_reasons == (ACK_OFF,)
        and not default_ack.consumed
    )
    return ok, f"default_enabled={default_ack.enabled} rejects={decision.ack_reject_reasons}"


def case_valid_ack_current_state() -> tuple[bool, str]:
    ack = valid_ack()
    decision = evaluate_gate(frozen_state(), ack)
    ok = (
        _expect_allow(decision)
        and decision.ack_consumed
        and ack.consumed
        and decision.rolling_policy.branch == "paper_red_a3_path"
        and decision.rolling_policy.stale_rolling_binder_active
    )
    return ok, f"branch={decision.rolling_policy.branch} consumed={ack.consumed}"


def case_recent_window() -> tuple[bool, str]:
    decision = evaluate_gate(frozen_state(newest_close_age_hours=6.0), valid_ack())
    ok = (
        _expect_block(decision, BLOCK_STALE_BINDER)
        and decision.ack_reject_reasons == (ACK_NOT_STALE,)
    )
    return ok, f"rejects={decision.ack_reject_reasons}"


def case_deep_loss() -> tuple[bool, str]:
    state = frozen_state(live_rolling_net_r=-6.0)
    ack = valid_ack()
    ack.live_rolling_net_r = -6.0
    decision = evaluate_gate(state, ack)
    ok = _expect_block(decision, BLOCK_STALE_BINDER) and decision.ack_reject_reasons == (ACK_DEEP_LOSS,)
    return ok, f"rejects={decision.ack_reject_reasons}"


def case_zero_and_negative_active_epoch() -> tuple[bool, str]:
    zero = evaluate_gate(frozen_state(active_epoch_net_r=0.0), valid_ack())
    negative = evaluate_gate(frozen_state(active_epoch_net_r=-0.01), valid_ack())
    ok = (
        _expect_block(zero, BLOCK_STALE_BINDER)
        and _expect_block(negative, BLOCK_STALE_BINDER)
        and zero.ack_reject_reasons == (ACK_ACTIVE_EPOCH,)
        and negative.ack_reject_reasons == (ACK_ACTIVE_EPOCH,)
    )
    return ok, f"zero={zero.ack_reject_reasons} negative={negative.ack_reject_reasons}"


def case_identity_mismatch() -> tuple[bool, str]:
    decision = evaluate_gate(frozen_state(last_live_close_key="1782870085999"), valid_ack())
    ok = _expect_block(decision, BLOCK_STALE_BINDER) and decision.ack_reject_reasons == (ACK_KEY,)
    return ok, f"rejects={decision.ack_reject_reasons}"


def case_count_mismatch() -> tuple[bool, str]:
    decision = evaluate_gate(frozen_state(live_closed_count=52), valid_ack())
    ok = _expect_block(decision, BLOCK_STALE_BINDER) and decision.ack_reject_reasons == (ACK_COUNT,)
    return ok, f"rejects={decision.ack_reject_reasons}"


def case_value_mismatch() -> tuple[bool, str]:
    decision = evaluate_gate(frozen_state(live_rolling_net_r=-1.95), valid_ack())
    ok = _expect_allow(decision) and decision.ack_reject_reasons == (ACK_VALUE,)
    return ok, f"normal_floor_pass={decision.rolling_policy.rolling_floor_pass} rejects={decision.ack_reject_reasons}"


def case_expiry() -> tuple[bool, str]:
    expired = valid_ack()
    expired.expires_ts = NOW_TS - 1.0
    at_now = valid_ack()
    at_now.expires_ts = NOW_TS
    expired_decision = evaluate_gate(frozen_state(), expired)
    at_now_decision = evaluate_gate(frozen_state(), at_now)
    ok = (
        _expect_block(expired_decision, BLOCK_STALE_BINDER)
        and _expect_block(at_now_decision, BLOCK_STALE_BINDER)
        and expired_decision.ack_reject_reasons == (ACK_EXPIRED,)
        and at_now_decision.ack_reject_reasons == (ACK_EXPIRED,)
    )
    return ok, f"expired={expired_decision.ack_reject_reasons} at_now={at_now_decision.ack_reject_reasons}"


def case_double_consume() -> tuple[bool, str]:
    ack = valid_ack()
    first = evaluate_gate(frozen_state(), ack)
    second = evaluate_gate(frozen_state(), ack)
    ok = (
        _expect_allow(first)
        and _expect_block(second, BLOCK_STALE_BINDER)
        and second.ack_reject_reasons == (ACK_CONSUMED,)
    )
    return ok, f"first={first.action} second={second.action} rejects={second.ack_reject_reasons}"


def case_new_close_after_ack() -> tuple[bool, str]:
    state = frozen_state(
        last_live_close_key="1782870090000",
        live_closed_count=52,
        live_rolling_net_r=-1.80,
        newest_close_age_hours=0.25,
    )
    decision = evaluate_gate(state, valid_ack())
    expected = (ACK_NOT_STALE, ACK_KEY, ACK_COUNT, ACK_VALUE)
    ok = (
        _expect_allow(decision)
        and decision.ack_reject_reasons == expected
        and decision.rolling_policy.rolling_floor_pass
        and not decision.ack_consumed
    )
    return ok, f"normal_floor_pass={decision.rolling_policy.rolling_floor_pass} rejects={decision.ack_reject_reasons}"


def case_restart_persistence() -> tuple[bool, str]:
    armed = valid_ack()
    armed_reload = AckState.deserialize(armed.serialize())
    armed_decision = evaluate_gate(frozen_state(), armed_reload)

    consumed = valid_ack()
    consumed_first = evaluate_gate(frozen_state(), consumed)
    consumed_payload = consumed.serialize()
    consumed_reload = AckState.deserialize(consumed_payload)
    consumed_decision = evaluate_gate(frozen_state(), consumed_reload)

    ok = (
        _expect_allow(armed_decision)
        and armed_decision.ack_consumed
        and _expect_allow(consumed_first)
        and consumed_payload.find('"consumed": true') != -1
        and consumed_reload.consumed is True
        and _expect_block(consumed_decision, BLOCK_STALE_BINDER)
        and consumed_decision.ack_reject_reasons == (ACK_CONSUMED,)
    )
    return ok, (
        f"armed_reload_initially_unconsumed={not json.loads(armed.serialize())['consumed']} "
        f"consumed_payload_flag={json.loads(consumed_payload)['consumed']} "
        f"replay_rejects={consumed_decision.ack_reject_reasons}"
    )


def case_paper_green_branch() -> tuple[bool, str]:
    green_ack = valid_ack()
    green = evaluate_gate(frozen_state(paper_health="GREEN", live_health="RED"), green_ack)
    paper_block_ack = valid_ack()
    paper_block = evaluate_gate(
        frozen_state(paper_health="GREEN", unrelated_paper_health_block=True),
        paper_block_ack,
    )
    live_block_ack = valid_ack()
    live_block = evaluate_gate(
        frozen_state(paper_health="GREEN", unrelated_live_health_block=True),
        live_block_ack,
    )
    ok = (
        _expect_allow(green)
        and green.rolling_policy.branch == "paper_green_rolling_floor_live_health_path"
        and green.rolling_policy.branch != "paper_red_a3_path"
        and green.rolling_policy.stale_rolling_binder_active
        and _expect_block(paper_block, BLOCK_HARD)
        and paper_block.hard_blocks == ("unrelated_paper_health_block",)
        and not paper_block_ack.consumed
        and _expect_block(live_block, BLOCK_HARD)
        and live_block.hard_blocks == ("unrelated_live_health_block",)
        and not live_block_ack.consumed
    )
    return ok, (
        f"green_branch={green.rolling_policy.branch} "
        f"paper_block={paper_block.hard_blocks} live_block={live_block.hard_blocks}"
    )


def case_hard_block_preservation() -> tuple[bool, str]:
    variants: tuple[tuple[str, str, GateState], ...] = (
        ("SL missing", "sl_missing", frozen_state(current_sl_missing_symbols=("ETHUSDT",))),
        ("entry unconfirmed", "entry_unconfirmed", frozen_state(current_entry_unconfirmed_symbols=("BTCUSDT",))),
        ("SL sync failure", "sl_sync_failure", frozen_state(live_sl_sync_failure=True)),
        ("stale health row", "health_row_stale", frozen_state(health_row_age_seconds=HEALTH_ROW_MAX_AGE_SECONDS + 1.0)),
        ("active micro-pause", "active_micro_pause", frozen_state(active_micro_pause=True)),
        ("current/non-stale loss streak", "current_non_stale_loss_streak", frozen_state(current_non_stale_loss_streak=True)),
        ("cap constraint", "cap_constraint", frozen_state(cap_pass=False)),
        ("risk constraint", "risk_constraint", frozen_state(risk_pass=False)),
        ("portfolio constraint", "portfolio_constraint", frozen_state(portfolio_pass=False)),
    )
    failed = []
    for name, expected_block, state in variants:
        ack = valid_ack()
        decision = evaluate_gate(state, ack)
        if not (
            _expect_block(decision, BLOCK_HARD)
            and decision.hard_blocks == (expected_block,)
            and decision.ack_reject_reasons == ()
            and ack.consumed is False
        ):
            failed.append((name, decision.hard_blocks, decision.ack_reject_reasons, ack.consumed))
    ok = not failed
    return ok, "9 independent hard blocks binding" if ok else f"failed={failed}"


def case_one_new_confirmed_close_rebind() -> tuple[bool, str]:
    stale_ack = valid_ack()
    post_close_state = frozen_state(
        last_live_close_key="1782870100000",
        live_closed_count=52,
        live_rolling_net_r=0.35,
        active_epoch_net_r=6.69,
        newest_close_age_hours=0.1,
        live_health="GREEN",
    )
    decision = evaluate_gate(post_close_state, stale_ack)
    stale_rejects = ack_reject_reasons(post_close_state, stale_ack)

    negative_control = derive_rolling_policy(frozen_state(live_rolling_net_r=FROZEN_LIVE_ROLLING_NET_R))
    ok = (
        _expect_allow(decision)
        and decision.ack_consumed is False
        and stale_ack.consumed is False
        and decision.rolling_policy.rolling_floor_pass
        and not decision.rolling_policy.stale_rolling_binder_active
        and stale_rejects == (ACK_NOT_STALE, ACK_KEY, ACK_COUNT, ACK_VALUE)
        and negative_control.stale_rolling_binder_active
        and not negative_control.rolling_floor_pass
    )
    return ok, (
        f"normal_floor_pass={decision.rolling_policy.rolling_floor_pass} "
        f"stale_ack_rejects={stale_rejects} "
        f"negative_control_binder={negative_control.stale_rolling_binder_active}"
    )


def case_boundary_coverage() -> tuple[bool, str]:
    age_boundary = evaluate_gate(frozen_state(newest_close_age_hours=ACK_MIN_NEWEST_CLOSE_AGE_HOURS), valid_ack())
    default_ack = AckState()
    default_decision = evaluate_gate(frozen_state(), default_ack)
    ok = (
        default_ack.enabled is False
        and default_decision.ack_reject_reasons == (ACK_OFF,)
        and _expect_allow(age_boundary)
        and age_boundary.ack_reject_reasons == ()
    )
    return ok, (
        f"default_enabled={default_ack.enabled} "
        f"age_120h_action={age_boundary.action} age_120h_rejects={age_boundary.ack_reject_reasons}"
    )


CASES: tuple[tuple[str, Callable[[], tuple[bool, str]]], ...] = (
    ("1. OFF_BY_DEFAULT", case_off_by_default),
    ("2. VALID_ACK_CURRENT_STATE", case_valid_ack_current_state),
    ("3. RECENT_WINDOW", case_recent_window),
    ("4. DEEP_LOSS", case_deep_loss),
    ("5. ZERO_AND_NEGATIVE_ACTIVE_EPOCH", case_zero_and_negative_active_epoch),
    ("6. IDENTITY_MISMATCH", case_identity_mismatch),
    ("7. COUNT_MISMATCH", case_count_mismatch),
    ("8. VALUE_MISMATCH", case_value_mismatch),
    ("9. EXPIRY", case_expiry),
    ("10. DOUBLE_CONSUME", case_double_consume),
    ("11. NEW_CLOSE_AFTER_ACK", case_new_close_after_ack),
    ("12. RESTART_PERSISTENCE", case_restart_persistence),
    ("13. PAPER_GREEN_BRANCH", case_paper_green_branch),
    ("14. HARD_BLOCK_PRESERVATION", case_hard_block_preservation),
    ("15. ONE_NEW_CONFIRMED_CLOSE_REBIND", case_one_new_confirmed_close_rebind),
    ("16. BOUNDARY_COVERAGE", case_boundary_coverage),
)


def main() -> int:
    print("SIM_LIVE_ROLLING_STALE_ACK_ONLY")
    print("SIMULATION_ONLY_EXECUTABLE_SPEC")
    print("NOT_PRODUCTION_GATE_VERIFICATION")
    print("mode=read_only production_imports=none config_reads=none order_actions=none")
    print()

    results = []
    for label, func in CASES:
        ok, detail = func()
        results.append(_case_result(label, ok, detail))

    hard_blocks_ok = case_hard_block_preservation()[0]
    one_shot_ok = case_double_consume()[0]
    persistence_ok = case_restart_persistence()[0]
    branch_ok = case_paper_green_branch()[0]
    rebind_ok = case_one_new_confirmed_close_rebind()[0]

    print()
    print(f"all_9_hard_blocks_independent={'PASS' if hard_blocks_ok else 'FAIL'}")
    print(f"one_shot_semantics={'PASS' if one_shot_ok else 'FAIL'}")
    print(f"consumed_persistence_round_trip={'PASS' if persistence_ok else 'FAIL'}")
    print(f"paper_red_green_branches_materially_distinct={'PASS' if branch_ok else 'FAIL'}")
    print(f"binder_derived_not_free_input={'PASS' if rebind_ok else 'FAIL'}")
    print("policy_consistency=PASS" if all(results) else "policy_consistency=FAIL")
    print("production_integration=NOT_TESTED")
    print("production_files_changed=NO_BY_SCRIPT")
    print("config_state_restart_order_actions=NONE")

    if all(results):
        print("\nSUMMARY PASS")
        return 0
    print("\nSUMMARY FAIL")
    return 1


if __name__ == "__main__":
    sys.exit(main())
