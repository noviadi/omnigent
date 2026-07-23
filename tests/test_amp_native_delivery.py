"""Delivery-state fault injection for the ``amp-native`` harness (AMP-NATIVE-001).

Covers the durable delivery journal (CAS transitions, fail-closed fsync), the
paste/Enter boundary ordering, restart reconciliation (no blind replay),
phase-aware failure handling (post-mutation -> recovery_required), the durable
cross-process set-once dedupe for the browser optimistic item, and the
real-event-path confirmation that wires a plugin mirror to a delivery.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import types
from pathlib import Path

import pytest

from omnigent import amp_native_bridge
from omnigent.amp_native_bridge import (
    InjectionHooks,
    InjectionPreconditionError,
    inject_user_message,
)
from omnigent.amp_native_delivery import (
    SCHEMA_VERSION,
    DeliveryJournal,
    DeliveryState,
)
from omnigent.inner import amp_native_executor as executor_module
from omnigent.inner.amp_native_executor import AmpNativeExecutor
from omnigent.runtime import external_item_dedupe

# --------------------------------------------------------------------------- #
# Test fixtures
# --------------------------------------------------------------------------- #


class _FakeTmux:
    """Stand-in for tmux so inject_user_message is deterministic in tests."""

    def __init__(self, bridge: Path) -> None:
        self.bridge = bridge
        self.paste_calls = 0
        self.enter_calls = 0
        (bridge / "tmux.json").write_text(
            json.dumps({"socket_path": str(bridge / "tmux.sock"), "tmux_target": "amp:0.0"}),
            encoding="utf-8",
        )

    def run(self, *args, **kwargs):
        del kwargs
        argv = list(args[0]) if args else []
        if "has-session" in argv:
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "paste-buffer" in argv:
            self.paste_calls += 1
        if "send-keys" in argv and "Enter" in argv:
            self.enter_calls += 1
        return subprocess.CompletedProcess(argv, 0, "", "")


@pytest.fixture
def fake_tmux(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> _FakeTmux:
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    fake = _FakeTmux(bridge)
    monkeypatch.setattr(amp_native_bridge.subprocess, "run", fake.run)
    return fake


def _hooks(raise_at: str) -> InjectionHooks:
    """Hooks that simulate the harness dying at one paste/Enter boundary."""

    def _die() -> None:
        raise RuntimeError(f"simulated termination at {raise_at}")

    return InjectionHooks(
        before_paste=_die if raise_at == "before_paste" else None,
        after_paste=_die if raise_at == "after_paste" else None,
        after_enter=_die if raise_at == "after_enter" else None,
    )


async def _noop_async(*args: object, **kwargs: object) -> None:
    return None


# --------------------------------------------------------------------------- #
# Journal state machine
# --------------------------------------------------------------------------- #


def test_journal_creates_versioned_pending_record(tmp_path: Path) -> None:
    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="hello\nworld", conversation_id="conv_1")
    assert record.schema_version == SCHEMA_VERSION
    assert record.state == DeliveryState.PENDING.value
    assert record.conversation_id == "conv_1"
    assert record.normalized_content_hash  # digest stored, not raw text
    assert record.created_at == record.updated_at
    assert (tmp_path / "bridge" / "delivery" / f"{record.delivery_id}.json").exists()


def test_hash_canonicalization_matches_injection_drops() -> None:
    """The digest uses the same canonical form injection delivers."""
    from omnigent.amp_native_delivery import normalized_content_hash

    # A vertical tab (0x0B) is dropped by injection; equivalent prompts share a digest.
    assert normalized_content_hash("a\x0bb") == normalized_content_hash("ab")
    assert normalized_content_hash("a\r\nb") == normalized_content_hash("a\nb")


def test_submission_started_is_durable_before_paste(fake_tmux: _FakeTmux) -> None:
    """submission_started is written + fsynced BEFORE any paste/Enter byte."""
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="ship it", conversation_id="conv_1")

    seen_started_before_paste = {"value": False}

    def _before_paste() -> None:
        reloaded = journal.get(record.delivery_id)
        seen_started_before_paste["value"] = (
            reloaded is not None and reloaded.state == DeliveryState.SUBMISSION_STARTED.value
        )

    inject_user_message(
        fake_tmux.bridge,
        "ship it",
        journal=journal,
        delivery_id=record.delivery_id,
        hooks=InjectionHooks(before_paste=_before_paste),
    )
    assert seen_started_before_paste["value"] is True
    assert fake_tmux.paste_calls == 1
    assert fake_tmux.enter_calls == 1


# --------------------------------------------------------------------------- #
# Fault injection at the paste/Enter boundaries
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("boundary", ["before_paste", "after_paste", "after_enter"])
def test_termination_during_injection_leaves_submission_started(
    fake_tmux: _FakeTmux, boundary: str
) -> None:
    """Termination at any paste/Enter boundary leaves submission_started durable."""
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="risky prompt", conversation_id="conv_1")
    with pytest.raises(RuntimeError, match="simulated termination"):
        inject_user_message(
            fake_tmux.bridge,
            "risky prompt",
            journal=journal,
            delivery_id=record.delivery_id,
            hooks=_hooks(boundary),
        )
    reloaded = journal.get(record.delivery_id)
    assert reloaded is not None
    assert reloaded.state == DeliveryState.SUBMISSION_STARTED.value


def test_termination_after_plugin_confirmation_stays_confirmed(
    fake_tmux: _FakeTmux,
) -> None:
    """After plugin confirmation, the record is terminal and survives restart."""
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="confirmed prompt", conversation_id="conv_1")
    inject_user_message(
        fake_tmux.bridge,
        "confirmed prompt",
        journal=journal,
        delivery_id=record.delivery_id,
    )
    journal.confirm(record.delivery_id, response_id="T-1:ev_42")
    reloaded = journal.get(record.delivery_id)
    assert reloaded is not None
    assert reloaded.state == DeliveryState.CONFIRMED.value
    assert reloaded.response_id == "T-1:ev_42"


# --------------------------------------------------------------------------- #
# Restart reconciliation, CAS safety, and no blind replay
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("boundary", ["before_paste", "after_paste", "after_enter"])
def test_restart_transitions_submission_started_to_recovery(
    fake_tmux: _FakeTmux, boundary: str
) -> None:
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="interrupted", conversation_id="conv_1")
    with pytest.raises(RuntimeError):
        inject_user_message(
            fake_tmux.bridge,
            "interrupted",
            journal=journal,
            delivery_id=record.delivery_id,
            hooks=_hooks(boundary),
        )
    recovered = journal.reconcile_on_start()
    assert len(recovered) == 1
    assert recovered[0].delivery_id == record.delivery_id
    assert recovered[0].state == DeliveryState.RECOVERY_REQUIRED.value
    assert recovered[0].reason is not None


def test_restart_does_not_regress_confirmed_record(fake_tmux: _FakeTmux) -> None:
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="ok", conversation_id="conv_1")
    journal.confirm(record.delivery_id, response_id="T-2:ev_9")
    recovered = journal.reconcile_on_start()
    assert recovered == []
    assert journal.get(record.delivery_id).state == DeliveryState.CONFIRMED.value


def test_cas_rejects_failed_after_submission_started(tmp_path: Path) -> None:
    """A post-mutation record can never regress to ``failed`` (replayable)."""
    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="x", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)
    # mark_failed is only valid from pending -> rejected (returns None).
    assert journal.mark_failed(record.delivery_id, reason="late tmux error") is None
    assert journal.get(record.delivery_id).state == DeliveryState.SUBMISSION_STARTED.value


def test_confirm_outstanding_rejects_wrong_thread(tmp_path: Path) -> None:
    """Confirmation validates the matching thread before marking confirmed."""
    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="x", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)
    outcome = journal.confirm_outstanding(response_id="T-999:ev_1", expected_thread_id="T-1")
    assert outcome.confirmed is False
    assert journal.get(record.delivery_id).state == DeliveryState.SUBMISSION_STARTED.value


def test_confirm_outstanding_confirms_matching_thread(tmp_path: Path) -> None:
    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="x", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)
    outcome = journal.confirm_outstanding(
        response_id="T-1:ev_7", expected_thread_id="T-1", confirmed_item_id="item_9"
    )
    assert outcome.confirmed is True
    reloaded = journal.get(record.delivery_id)
    assert reloaded.state == DeliveryState.CONFIRMED.value
    assert reloaded.response_id == "T-1:ev_7"
    assert reloaded.confirmed_item_id == "item_9"


def test_executor_does_not_blindly_replay_recovered_prompt(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    journal = DeliveryJournal(bridge)
    journal.create(content="once is enough", conversation_id="conv_1")
    journal.reconcile_on_start()

    executor = AmpNativeExecutor(bridge_dir=bridge)
    assert journal.load_all()[0].state == DeliveryState.RECOVERY_REQUIRED.value

    messages = [{"role": "user", "content": "once is enough"}]
    events = asyncio.run(_drive_turn(executor, messages))
    assert executor_module.RECOVERY_ERROR_CODE in " ".join(events)
    assert fake_tmux.paste_calls == 0
    assert fake_tmux.enter_calls == 0


def test_executor_injects_new_prompt_after_recovery(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    journal = DeliveryJournal(bridge)
    rec = journal.create(content="stuck prompt", conversation_id="conv_1")
    journal.mark_recovery_required(rec.delivery_id, reason="prior crash")

    executor = AmpNativeExecutor(bridge_dir=bridge)
    messages = [{"role": "user", "content": "a brand new question"}]
    outcome = asyncio.run(_first_event_name(executor, messages))
    assert outcome == "TurnComplete"
    assert fake_tmux.paste_calls == 1
    new_records = [r for r in journal.load_all() if r.delivery_id != rec.delivery_id]
    assert new_records
    assert new_records[-1].state == DeliveryState.SUBMISSION_STARTED.value


def test_post_paste_failure_becomes_recovery_not_replayable(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure AFTER paste becomes recovery_required; a retry never replays."""

    class _FaultyExecutor(AmpNativeExecutor):
        def _injection_hooks(self) -> InjectionHooks:
            return _hooks("after_paste")

    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    executor = _FaultyExecutor(bridge_dir=bridge)
    messages = [{"role": "user", "content": "maybe pasted"}]

    events = asyncio.run(_drive_turn(executor, messages))
    assert executor_module.RECOVERY_ERROR_CODE in " ".join(events)
    # Paste reached tmux, but the delivery is recovery_required, not failed.
    assert fake_tmux.paste_calls == 1
    record = next(r for r in executor.journal.load_all() if r.state != "pending")
    assert record.state == DeliveryState.RECOVERY_REQUIRED.value

    # A retry of the same prompt is blocked — no second paste.
    events = asyncio.run(_drive_turn(executor, messages))
    assert executor_module.RECOVERY_ERROR_CODE in " ".join(events)
    assert fake_tmux.paste_calls == 1


def test_live_queue_recovery_returns_typed_result_not_bare_false(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    journal = DeliveryJournal(bridge)
    rec = journal.create(content="blocked live", conversation_id="conv_1")
    journal.mark_recovery_required(rec.delivery_id, reason="prior crash")

    executor = AmpNativeExecutor(bridge_dir=bridge)
    result = asyncio.run(executor.enqueue_session_message("main", "blocked live"))
    assert (
        isinstance(result, executor_module.LiveQueueResult)
        if hasattr(executor_module, "LiveQueueResult")
        else True
    )
    assert bool(result) is False
    assert result.reason is not None
    assert executor_module.RECOVERY_ERROR_CODE in result.reason
    assert fake_tmux.paste_calls == 0


def test_concurrent_confirm_and_reconcile_do_not_clobber(tmp_path: Path) -> None:
    """A confirm racing a reconcile cannot regress a confirmed record."""
    import threading

    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="race", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)

    errors: list[BaseException] = []

    def _confirm() -> None:
        try:
            journal.confirm(record.delivery_id, response_id="T-1:ev_1")
        except BaseException as exc:
            errors.append(exc)

    def _reconcile() -> None:
        try:
            journal.reconcile_on_start()
        except BaseException as exc:
            errors.append(exc)

    t1 = threading.Thread(target=_confirm)
    t2 = threading.Thread(target=_reconcile)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert errors == []
    final = journal.get(record.delivery_id)
    # Either confirmed (won the race) or recovery_required; never pending, and a
    # confirmed record is never regressed by reconcile.
    assert final.state in {
        DeliveryState.CONFIRMED.value,
        DeliveryState.RECOVERY_REQUIRED.value,
    }


# --------------------------------------------------------------------------- #
# Fail-closed durability
# --------------------------------------------------------------------------- #


def test_fsync_failure_is_fail_closed_no_paste(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A raw fsync OSError is classified as a typed DurabilityError and the
    injection does not paste."""
    from omnigent.amp_native_delivery import DurabilityError

    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="durable?", conversation_id="conv_1")

    def _boom_fsync(_fd: int) -> None:
        raise OSError("simulated fsync failure")

    monkeypatch.setattr("omnigent.amp_native_delivery.os.fsync", _boom_fsync)
    with pytest.raises(DurabilityError, match="simulated fsync failure"):
        inject_user_message(
            fake_tmux.bridge,
            "durable?",
            journal=journal,
            delivery_id=record.delivery_id,
        )
    # No paste/Enter reached Amp because durability could not be established.
    assert fake_tmux.paste_calls == 0
    assert fake_tmux.enter_calls == 0


def test_concurrent_injection_pastes_at_most_once(fake_tmux: _FakeTmux) -> None:
    """Two callers racing the same delivery_id authorize exactly one paste.

    The P0 invariant is one paste per delivery_id. ``mark_submission_started``
    is non-idempotent: under its per-record fcntl CAS only one caller advances
    pending -> submission_started, so a second caller's submission-start returns
    None and the paste path treats that as a hard stop (InjectionPreconditionError).
    """
    import threading

    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="once only", conversation_id="conv_1")

    errors: list[BaseException] = []

    def _inject() -> None:
        try:
            inject_user_message(
                fake_tmux.bridge,
                "once only",
                journal=journal,
                delivery_id=record.delivery_id,
            )
        except InjectionPreconditionError as exc:
            errors.append(exc)
        except BaseException as exc:  # pragma: no cover - unexpected
            errors.append(exc)

    threads = [threading.Thread(target=_inject) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Exactly one paste+Enter reached Amp; the rest hit the hard stop.
    assert fake_tmux.paste_calls == 1
    assert fake_tmux.enter_calls == 1
    assert len(errors) == len(threads) - 1
    assert all(isinstance(e, InjectionPreconditionError) for e in errors)


def test_confirm_outstanding_no_op_when_response_id_already_confirmed(
    tmp_path: Path,
) -> None:
    """A duplicate response_id (retried plugin post) is a no-op, not a second
    confirmation of a different outstanding delivery."""
    journal = DeliveryJournal(tmp_path / "bridge")
    first = journal.create(content="first", conversation_id="T-1")
    journal.mark_submission_started(first.delivery_id)
    journal.confirm(first.delivery_id, response_id="T-1:ev_1", confirmed_item_id="item_1")
    # A second outstanding delivery exists that would otherwise match the thread.
    second = journal.create(content="second", conversation_id="T-1")
    journal.mark_submission_started(second.delivery_id)

    outcome = journal.confirm_outstanding(
        response_id="T-1:ev_1", expected_thread_id="T-1", confirmed_item_id="item_2"
    )
    # Already confirmed -> no-op; the duplicate must NOT confirm the second one.
    assert outcome.confirmed is False
    assert outcome.already_confirmed is True
    assert journal.get(second.delivery_id).state == DeliveryState.SUBMISSION_STARTED.value


def test_confirm_outstanding_refuses_wrong_content_on_singleton(tmp_path: Path) -> None:
    """A mirror whose content digest doesn't match must NOT confirm an unrelated
    singleton — refuse rather than guess."""
    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="the real prompt", conversation_id="T-1")
    journal.mark_submission_started(record.delivery_id)

    outcome = journal.confirm_outstanding(
        response_id="T-1:ev_2",
        expected_thread_id="T-1",
        confirmed_item_id="item_x",
        content="a completely different prompt",
    )
    assert outcome.confirmed is False
    assert journal.get(record.delivery_id).state == DeliveryState.SUBMISSION_STARTED.value


def test_failure_transition_durable_error_is_typed(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A DurabilityError while persisting the recovery transition must itself
    become a typed RECOVERY result — never a raw stack trace."""

    class _FaultyExecutor(AmpNativeExecutor):
        def _injection_hooks(self) -> InjectionHooks:
            return _hooks("after_paste")

    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    executor = _FaultyExecutor(bridge_dir=bridge)

    # The recovery transition's fsync fails after the paste already reached Amp.
    def _boom_mark(delivery_id: str, reason: str | None = None) -> None:
        from omnigent.amp_native_delivery import DurabilityError

        raise DurabilityError("simulated recovery-transition fsync failure")

    monkeypatch.setattr(executor.journal, "mark_recovery_required", _boom_mark)

    messages = [{"role": "user", "content": "maybe pasted"}]
    # Must not raise: the durability failure surfaces as the typed result.
    events = asyncio.run(_drive_turn(executor, messages))
    assert executor_module.RECOVERY_ERROR_CODE in " ".join(events)
    # The paste reached Amp; only the recovery write failed.
    assert fake_tmux.paste_calls == 1


def test_unreadable_journal_blocks_new_injection(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A corrupt/unknown-schema journal record must NEVER be treatable as absent:
    the harness refuses to inject (no paste) until the journal is reconciled."""
    import json

    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    journal = DeliveryJournal(bridge)
    stale = journal.create(content="unknown", conversation_id="conv_1")
    path = bridge / "delivery" / f"{stale.delivery_id}.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["schema_version"] = 99  # unknown future version
    path.write_text(json.dumps(data), encoding="utf-8")

    executor = AmpNativeExecutor(bridge_dir=bridge)
    assert executor.journal.unreadable_records()

    messages = [{"role": "user", "content": "a brand new question"}]
    events = asyncio.run(_drive_turn(executor, messages))
    assert executor_module.RECOVERY_ERROR_CODE in " ".join(events)
    # No paste reached Amp: corrupt state blocks new injection.
    assert fake_tmux.paste_calls == 0
    assert fake_tmux.enter_calls == 0


def test_in_flight_submission_started_blocks_same_process_retry(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A submission_started delivery (paste may have happened) blocks a same-
    process retry: no second paste, typed recovery result."""
    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    executor = AmpNativeExecutor(bridge_dir=bridge)

    # Simulate a delivery left in flight within the live process (e.g. a caller
    # died after submission_started but before the recovery transition landed).
    rec = executor.journal.create(content="in flight", conversation_id="conv_1")
    executor.journal.mark_submission_started(rec.delivery_id)

    events = asyncio.run(_drive_turn(executor, [{"role": "user", "content": "in flight"}]))
    assert executor_module.RECOVERY_ERROR_CODE in " ".join(events)
    # No second paste reached Amp.
    assert fake_tmux.paste_calls == 0
    assert fake_tmux.enter_calls == 0


def test_pending_delivery_blocks_same_process_retry(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pending (created but not yet submitted) delivery for the same prompt
    also blocks a retry: at most one non-terminal delivery per prompt."""
    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    executor = AmpNativeExecutor(bridge_dir=bridge)

    executor.journal.create(content="pending twin", conversation_id="conv_1")

    events = asyncio.run(_drive_turn(executor, [{"role": "user", "content": "pending twin"}]))
    assert executor_module.RECOVERY_ERROR_CODE in " ".join(events)
    assert fake_tmux.paste_calls == 0


def test_failed_delivery_allows_new_attempt_for_same_prompt(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed delivery (no byte reached Amp) has a proven outcome and a retry
    of the same prompt may create a new delivery and paste."""
    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    executor = AmpNativeExecutor(bridge_dir=bridge)

    rec = executor.journal.create(content="try again", conversation_id="conv_1")
    executor.journal.mark_failed(rec.delivery_id, reason="pre-paste failure")

    outcome = asyncio.run(executor.enqueue_session_message("main", "try again"))
    assert bool(outcome) is True
    assert fake_tmux.paste_calls == 1


def test_reconcile_durability_failure_surfaces_typed_recovery(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An fsync failure during startup reconcile must surface as a typed recovery
    result — no raw stack trace, no paste."""
    from omnigent.amp_native_delivery import DeliveryJournal

    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")

    # A non-terminal record exists so reconcile_on_start attempts a write.
    journal = DeliveryJournal(bridge)
    journal.create(content="left mid-flight", conversation_id="conv_1")
    journal.mark_submission_started(journal.load_all()[0].delivery_id)

    # The recovery-transition fsync fails during startup reconcile.
    def _boom_fsync(_fd: int) -> None:
        raise OSError("simulated fsync failure")

    monkeypatch.setattr("omnigent.amp_native_delivery.os.fsync", _boom_fsync)

    executor = AmpNativeExecutor(bridge_dir=bridge)
    assert executor.journal.reconcile_failures()

    # New injection must be refused without raising and without pasting.
    outcome = asyncio.run(executor.enqueue_session_message("main", "unrelated prompt"))
    assert bool(outcome) is False
    assert outcome.reason is not None
    assert executor_module.RECOVERY_ERROR_CODE in outcome.reason
    assert fake_tmux.paste_calls == 0


def test_strict_parsing_rejects_non_dict_record(tmp_path: Path) -> None:
    """A non-JSON-object record body is corruption, not absence."""
    import json

    from omnigent.amp_native_delivery import JournalCorruptError

    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="x", conversation_id="conv_1")
    path = tmp_path / "bridge" / "delivery" / f"{record.delivery_id}.json"
    path.write_text(json.dumps(["not", "an", "object"]), encoding="utf-8")
    with pytest.raises(JournalCorruptError, match="not a JSON object"):
        journal.get(record.delivery_id)
    assert journal.unreadable_records()


def test_strict_parsing_rejects_invalid_state(tmp_path: Path) -> None:
    """An invalid state enum value is corruption, not a silently accepted record."""
    import json

    from omnigent.amp_native_delivery import JournalCorruptError

    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="x", conversation_id="conv_1")
    path = tmp_path / "bridge" / "delivery" / f"{record.delivery_id}.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["state"] = "bogus_state"
    path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(JournalCorruptError, match="invalid state"):
        journal.get(record.delivery_id)
    assert journal.unreadable_records()


def test_confirm_outstanding_refuses_when_journal_corrupt(tmp_path: Path) -> None:
    """Confirmation must not proceed (or pick a singleton) while the journal holds
    a corrupt record — fail closed rather than risk confirming the wrong delivery."""
    import json

    from omnigent.amp_native_delivery import JournalCorruptError

    journal = DeliveryJournal(tmp_path / "bridge")
    good = journal.create(content="the real prompt", conversation_id="T-1")
    journal.mark_submission_started(good.delivery_id)
    # An unrelated corrupt entry exists alongside the good outstanding delivery.
    corrupt = journal.create(content="other", conversation_id="T-1")
    bad_path = tmp_path / "bridge" / "delivery" / f"{corrupt.delivery_id}.json"
    bad_data = json.loads(bad_path.read_text(encoding="utf-8"))
    bad_data["schema_version"] = 99
    bad_path.write_text(json.dumps(bad_data), encoding="utf-8")

    with pytest.raises(JournalCorruptError):
        journal.confirm_outstanding(
            response_id="T-1:ev_1",
            expected_thread_id="T-1",
            confirmed_item_id="item_1",
            content="the real prompt",
        )
    # The good delivery was NOT confirmed while the journal was corrupt.
    assert journal.get(good.delivery_id).state == DeliveryState.SUBMISSION_STARTED.value


# --------------------------------------------------------------------------- #
# Terminal-recreate survival + runner reconciliation
# --------------------------------------------------------------------------- #


def test_clear_inbox_does_not_wipe_delivery_state(tmp_path: Path) -> None:
    bridge = amp_native_bridge.prepare_bridge_dir("conv_1")
    journal = DeliveryJournal(bridge)
    record = journal.create(content="survive recreate", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)

    amp_native_bridge.prepare_bridge_dir("conv_1")
    amp_native_bridge.clear_inbox(bridge)

    reloaded = journal.get(record.delivery_id)
    assert reloaded is not None
    assert reloaded.state == DeliveryState.SUBMISSION_STARTED.value
    recovered = DeliveryJournal(bridge).reconcile_on_start()
    assert any(r.delivery_id == record.delivery_id for r in recovered)


def test_runner_recreate_reconciles_and_publishes_recovery(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The real recreate lifecycle reconciles the journal and surfaces recovery.

    Exercises ``_auto_create_amp_terminal`` itself (with the terminal-launch
    collaborators faked) rather than mirroring its reconcile loop, so the
    reconcile + publish wiring is covered by the production code path.
    """
    import omnigent.amp_native as amp_native_mod
    import omnigent.cli_auth as cli_auth
    import omnigent.runner._entry as runner_entry
    import omnigent.runner.app as runner_app

    bridge = tmp_path / "bridge"
    monkeypatch.setattr(amp_native_bridge, "bridge_dir_for_session_id", lambda _sid: bridge)
    journal = DeliveryJournal(bridge)
    journal.create(content="left mid-flight", conversation_id="conv_1")
    journal.mark_submission_started(journal.load_all()[0].delivery_id)

    published: list[dict[str, object]] = []

    def _publish(_sid: str, event: dict[str, object]) -> None:
        published.append(event)

    async def _fake_launch_config(*, session_id: str, server_client: object):
        del session_id, server_client
        return types.SimpleNamespace(
            server_url="http://server",
            external_session_id="T-1",
            workspace=tmp_path,
            terminal_launch_args=[],
        )

    monkeypatch.setattr(runner_app, "_pi_native_launch_config", _fake_launch_config)
    monkeypatch.setattr(runner_app, "_agent_os_env_from_spec", lambda spec: None)
    monkeypatch.setattr(runner_app, "session_resource_view_to_dict", lambda view: {})
    monkeypatch.setattr(runner_entry, "_make_auth_token_factory", lambda: None)
    monkeypatch.setattr(
        amp_native_bridge,
        "install_plugin_and_config",
        lambda *a, **k: (tmp_path / "p.ts", tmp_path / "c.json"),
    )
    monkeypatch.setattr(amp_native_mod, "build_amp_launch", lambda *a, **k: ["amp"])
    monkeypatch.setattr(cli_auth, "databricks_request_headers", lambda *a, **k: {})

    async def _launch_terminal(**kwargs: object) -> object:
        return types.SimpleNamespace()

    resource_registry = types.SimpleNamespace(
        launch_required_terminal=_launch_terminal,
        terminal_registry=None,
    )

    asyncio.run(
        runner_app._auto_create_amp_terminal(
            "conv_1",
            resource_registry,  # type: ignore[arg-type]
            _publish,
            server_client=object(),
        )
    )
    assert any(e.get("type") == "amp_native_delivery_recovery" for e in published)
    assert journal.load_all()[0].state == DeliveryState.RECOVERY_REQUIRED.value


# --------------------------------------------------------------------------- #
# Durable, cross-process set-once dedupe
# --------------------------------------------------------------------------- #


@pytest.fixture
def isolated_dedupe(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Redirect the dedupe marker root to a tmp bridge."""
    bridge = tmp_path / "dedupe_bridge"
    monkeypatch.setattr(external_item_dedupe, "bridge_dir_for_session_id", lambda _sid: bridge)
    external_item_dedupe.reset_for_tests()
    return bridge


def test_dedupe_claim_is_durable_set_once(isolated_dedupe: Path) -> None:
    """The first claim owns the key; a concurrent/second claim sees nothing yet,
    and after commit a later claim returns the durable item id."""
    claim = external_item_dedupe.Claim("conv_1", "T-1:ev_42")
    view = claim.enter()
    assert view.duplicate is False
    assert view.orphan is False
    claim.commit("item_7")
    claim.close()
    assert external_item_dedupe.is_claimed("conv_1", "T-1:ev_42") is True
    later = external_item_dedupe.Claim("conv_1", "T-1:ev_42")
    later_view = later.enter()
    assert later_view.duplicate is True
    assert later_view.item_id == "item_7"
    later.close()


def test_dedupe_claim_abort_lets_retry_proceed(isolated_dedupe: Path) -> None:
    """An owner that aborts its failed tentative claim lets a retry re-acquire."""
    claim = external_item_dedupe.Claim("conv_1", "T-1:ev_9")
    claim.enter()
    claim.abort()
    claim.close()
    again = external_item_dedupe.Claim("conv_1", "T-1:ev_9")
    view = again.enter()
    assert view.duplicate is False
    assert view.orphan is False
    again.close()


def test_dedupe_claim_recovers_orphan_against_store(isolated_dedupe: Path) -> None:
    """A tentative marker left by a crashed prior attempt is recovered via the
    store: if the item exists the orphan is promoted to committed."""
    marker_path = external_item_dedupe._marker_path("conv_1", "T-1:ev_5")
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    # Simulate a crash mid-append: a tentative marker with no item id.
    external_item_dedupe._write_marker(marker_path, {})
    claim = external_item_dedupe.Claim("conv_1", "T-1:ev_5")
    view = claim.enter()
    assert view.duplicate is False
    assert view.orphan is True
    # The store confirms the prior attempt did persist -> commit + duplicate.
    claim.commit("item_3")
    claim.close()
    assert external_item_dedupe.is_claimed("conv_1", "T-1:ev_5") is True


def test_dedupe_claim_serializes_concurrent_append(isolated_dedupe: Path) -> None:
    """Two threads racing acquire->append->commit for the same key cannot both
    append: the cross-process flock makes the second block until the first
    commits, after which it sees the durable duplicate (no active claim is ever
    deleted by a concurrent caller)."""
    import threading

    barrier = threading.Barrier(2)
    appended: list[str] = []
    results: list[external_item_dedupe.ClaimView] = []
    errors: list[BaseException] = []

    def _worker(item_id: str) -> None:
        try:
            claim = external_item_dedupe.Claim("conv_1", "T-1:ev_race")
            barrier.wait(timeout=5.0)
            view = claim.enter()
            if not view.duplicate:
                # Hold the critical section briefly so the other thread must
                # block on the flock rather than slipping in.
                threading.Event().wait(0.05)
                appended.append(item_id)
                claim.commit(item_id)
            else:
                results.append(view)
            claim.close()
        except BaseException as exc:
            errors.append(exc)

    t1 = threading.Thread(target=_worker, args=("item_a",))
    t2 = threading.Thread(target=_worker, args=("item_b",))
    t1.start()
    t2.start()
    t1.join(timeout=10.0)
    t2.join(timeout=10.0)
    assert errors == []
    # Exactly one append happened; the other claim saw the committed duplicate.
    assert len(appended) == 1
    assert len(results) == 1
    assert results[0].duplicate is True
    assert results[0].item_id == appended[0]


# --------------------------------------------------------------------------- #
# Server-side persistence: durable dedupe + real-event-path confirmation
# --------------------------------------------------------------------------- #


class _FakeStore:
    def __init__(self) -> None:
        self._items: list[object] = []

    def append(self, session_id: str, items: list[object]) -> list[object]:
        out = []
        for item in items:
            saved = types.SimpleNamespace(
                id=f"item_{len(self._items)}",
                type=getattr(item, "type", None),
                data=getattr(item, "data", None),
                response_id=getattr(item, "response_id", None),
            )
            self._items.append(saved)
            out.append(saved)
        return out

    def list_items(self, session_id: str, **kwargs: object):
        return types.SimpleNamespace(data=list(self._items))


def _patch_server_neutrals(monkeypatch: pytest.MonkeyPatch) -> None:
    import omnigent.server.routes.sessions as server_routes

    def _noop(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(server_routes, "_publish_external_conversation_item", _noop)
    monkeypatch.setattr(server_routes, "_drive_terminal_resolved_elicitation", _noop)
    monkeypatch.setattr(server_routes, "prepare_background_session_title", _noop)
    monkeypatch.setattr(server_routes, "_seed_missing_title_from_user_message", _noop_async)


def _user_mirror_body(response_id: str, text: str) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        data={
            "item_type": "message",
            "response_id": response_id,
            "item_data": {
                "role": "user",
                "content": [{"type": "input_text", "text": text}],
            },
        }
    )


def test_persist_dedupes_amp_user_mirror_and_confirms_delivery(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """First mirror persists + confirms the delivery; duplicate returns existing id."""
    import omnigent.server.routes.sessions as server_routes

    bridge = tmp_path / "bridge"
    monkeypatch.setattr(server_routes, "bridge_dir_for_session_id", lambda _sid: bridge)
    monkeypatch.setattr(external_item_dedupe, "bridge_dir_for_session_id", lambda _sid: bridge)
    external_item_dedupe.reset_for_tests()
    _patch_server_neutrals(monkeypatch)

    # An outstanding browser delivery awaiting plugin confirmation.
    journal = DeliveryJournal(bridge)
    record = journal.create(content="hello", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)

    conv = types.SimpleNamespace(
        labels={"omnigent.wrapper": "amp-native-ui"},
        external_session_id="T-1",
        title=None,
    )
    store = _FakeStore()
    body = _user_mirror_body("T-1:ev_42", "hello")

    first = asyncio.run(
        server_routes._persist_external_conversation_item(
            "conv_1",
            conv,
            body,
            store,  # type: ignore[arg-type]
        )
    )
    # The delivery was confirmed through the real event path (not a direct call).
    confirmed = journal.get(record.delivery_id)
    assert confirmed.state == DeliveryState.CONFIRMED.value
    assert confirmed.response_id == "T-1:ev_42"
    assert confirmed.confirmed_item_id == first

    # A duplicate plugin post returns the existing durable item, no second append.
    second = asyncio.run(
        server_routes._persist_external_conversation_item(
            "conv_1",
            conv,
            body,
            _FakeStore(),  # type: ignore[arg-type]
        )
    )
    assert second == first
    external_item_dedupe.reset_for_tests()


def test_persist_does_not_dedupe_non_amp_routes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A non-amp user mirror is not deduped (route unchanged)."""
    import omnigent.server.routes.sessions as server_routes

    external_item_dedupe.reset_for_tests()
    _patch_server_neutrals(monkeypatch)

    conv = types.SimpleNamespace(labels={}, external_session_id=None, title=None)
    store = _FakeStore()
    body = _user_mirror_body("T-1:ev_42", "hi")

    first = asyncio.run(
        server_routes._persist_external_conversation_item(
            "conv_1",
            conv,
            body,
            store,  # type: ignore[arg-type]
        )
    )
    second = asyncio.run(
        server_routes._persist_external_conversation_item(
            "conv_1",
            conv,
            body,
            store,  # type: ignore[arg-type]
        )
    )
    assert first != second  # both appended; no dedupe for non-amp
    assert len(store._items) == 2


# --------------------------------------------------------------------------- #
# async driving helpers
# --------------------------------------------------------------------------- #


async def _drive_turn(executor: AmpNativeExecutor, messages: list[dict]) -> list[str]:
    events: list[str] = []
    async for event in executor.run_turn(messages, [], ""):
        events.append(type(event).__name__)
        message = getattr(event, "message", "")
        if isinstance(message, str):
            events.append(message)
    return events


async def _first_event_name(executor: AmpNativeExecutor, messages: list[dict]) -> str:
    async for event in executor.run_turn(messages, [], ""):
        return type(event).__name__
    return "none"


# --------------------------------------------------------------------------- #
# Second-review hardening: late confirm after recovery, verified pre-paste
# transition, typed durability failures, schema validation, live-queue surfacing
# --------------------------------------------------------------------------- #


def test_late_confirmation_after_reconcile_resolves_recovery(tmp_path: Path) -> None:
    """A plugin confirmation arriving after a recreate reconcile moved the
    delivery to recovery_required still resolves it (idempotent, no double
    confirm)."""
    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="raced the recreate", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)
    # Restart reconcile races the mirror and moves the record to recovery.
    journal.reconcile_on_start()
    assert journal.get(record.delivery_id).state == DeliveryState.RECOVERY_REQUIRED.value

    # The late mirror (first confirmation for this response_id) still confirms
    # via content correlation, stamping response_id + the durable item id.
    outcome = journal.confirm_outstanding(
        response_id="T-1:ev_42",
        expected_thread_id="T-1",
        confirmed_item_id="item_9",
        content="raced the recreate",
    )
    assert outcome.confirmed is True
    reloaded = journal.get(record.delivery_id)
    assert reloaded.state == DeliveryState.CONFIRMED.value
    assert reloaded.response_id == "T-1:ev_42"
    assert reloaded.confirmed_item_id == "item_9"

    # A duplicate mirror does not re-confirm (already terminal).
    again = journal.confirm_outstanding(
        response_id="T-1:ev_42",
        expected_thread_id="T-1",
        confirmed_item_id="item_9",
        content="raced the recreate",
    )
    assert again.confirmed is False
    assert again.already_confirmed is True


def test_persist_confirms_delivery_even_on_duplicate_mirror(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A duplicate mirror still retries the (idempotent) confirmation so a
    delivery whose confirm step crashed after the dedupe marker committed can
    be resolved by a later post."""
    import omnigent.server.routes.sessions as server_routes

    bridge = tmp_path / "bridge"
    monkeypatch.setattr(server_routes, "bridge_dir_for_session_id", lambda _sid: bridge)
    monkeypatch.setattr(external_item_dedupe, "bridge_dir_for_session_id", lambda _sid: bridge)
    external_item_dedupe.reset_for_tests()
    _patch_server_neutrals(monkeypatch)

    journal = DeliveryJournal(bridge)
    record = journal.create(content="hello", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)

    conv = types.SimpleNamespace(
        labels={"omnigent.wrapper": "amp-native-ui"},
        external_session_id="T-1",
        title=None,
    )
    body = _user_mirror_body("T-1:ev_42", "hello")

    # First mirror appends but simulate the confirm step being lost: drop the
    # confirmed marker back to recovery_required so the delivery is unresolved.
    first = asyncio.run(
        server_routes._persist_external_conversation_item(
            "conv_1",
            conv,
            body,
            _FakeStore(),  # type: ignore[arg-type]
        )
    )
    assert journal.get(record.delivery_id).state == DeliveryState.CONFIRMED.value

    # Force the delivery back to an unresolved state (confirm was lost).
    import omnigent.amp_native_delivery as delivery

    delivery.DeliveryJournal(bridge).mark_recovery_required(record.delivery_id)

    # A duplicate mirror returns the existing item AND re-runs confirmation,
    # resolving the recovery_required delivery to confirmed.
    second = asyncio.run(
        server_routes._persist_external_conversation_item(
            "conv_1",
            conv,
            body,
            _FakeStore(),  # type: ignore[arg-type]
        )
    )
    assert second == first
    assert journal.get(record.delivery_id).state == DeliveryState.CONFIRMED.value
    external_item_dedupe.reset_for_tests()


def test_inject_aborts_when_delivery_is_terminal_no_paste(
    fake_tmux: _FakeTmux,
) -> None:
    """Injection requires a successful submission_started transition; a terminal
    or otherwise non-submittable record aborts before any tmux mutation."""
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="already resolved", conversation_id="conv_1")
    journal.confirm(record.delivery_id, response_id="T-1:ev_1")  # terminal

    with pytest.raises(InjectionPreconditionError, match="not in a submittable state"):
        inject_user_message(
            fake_tmux.bridge,
            "already resolved",
            journal=journal,
            delivery_id=record.delivery_id,
        )
    assert fake_tmux.paste_calls == 0
    assert fake_tmux.enter_calls == 0


def test_executor_emits_typed_failed_on_durability_error(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A durability failure (e.g. fsync) is classified and surfaced as a typed
    FAILED result — no raw crash and no paste."""
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(fake_tmux.bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")

    def _boom_fsync(_fd: int) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("omnigent.amp_native_delivery.os.fsync", _boom_fsync)
    executor = AmpNativeExecutor(bridge_dir=fake_tmux.bridge)
    messages = [{"role": "user", "content": "no disk space"}]

    events = asyncio.run(_drive_turn(executor, messages))
    assert executor_module.FAILED_ERROR_CODE in " ".join(events)
    assert executor_module.RECOVERY_ERROR_CODE not in " ".join(events)
    assert fake_tmux.paste_calls == 0
    assert fake_tmux.enter_calls == 0


def test_journal_rejects_unknown_schema_version(tmp_path: Path) -> None:
    """A record written by an unknown/future schema version is not interpreted
    with the current shape and is SURFACED (never silently dropped)."""
    import json

    from omnigent.amp_native_delivery import JournalCorruptError

    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="x", conversation_id="conv_1")
    path = tmp_path / "bridge" / "delivery" / f"{record.delivery_id}.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["schema_version"] = 99  # unknown future version
    path.write_text(json.dumps(data), encoding="utf-8")
    # get() propagates corruption (never treats it as absence).
    with pytest.raises(JournalCorruptError, match="unsupported schema_version"):
        journal.get(record.delivery_id)
    assert journal.load_all() == []
    # The unreadable record is surfaced, not treated as absent (no silent replay).
    assert journal.unreadable_records()


def test_live_queue_refusal_surfaces_on_session_event_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A typed live-queue refusal (recovery/failed) reaches the session event
    stream via the executor adapter, not only the logs."""
    from omnigent.inner.executor import LiveQueueResult
    from omnigent.runtime.harnesses._executor_adapter import ExecutorAdapter

    class _FakeCtx:
        def __init__(self, injections: list[object]) -> None:
            self._injections = list(injections)
            self.response_id = "resp_1"
            self.cancelled = types.SimpleNamespace(is_set=lambda: False)
            self.emitted: list[object] = []

        async def next_injection(self, timeout: float | None = None) -> object:
            del timeout
            return self._injections.pop(0) if self._injections else None

        def emit(self, event: object) -> None:
            self.emitted.append(event)

    class _FakeExecutor:
        async def enqueue_session_message(self, session_key: str, text: str) -> object:
            del session_key, text
            return LiveQueueResult(
                accepted=False,
                reason=(
                    f"[{executor_module.RECOVERY_ERROR_CODE}] delivery d was "
                    "interrupted; resubmission is blocked."
                ),
            )

    adapter = ExecutorAdapter(executor_factory=lambda: None)
    ctx = _FakeCtx([types.SimpleNamespace(input="steered text", injection_id=None)])
    asyncio.run(adapter._watch_injections(ctx, _FakeExecutor()))  # type: ignore[arg-type]

    refusal_events = [
        e
        for e in ctx.emitted
        if getattr(e, "type", None) == "response.output_item.done"
        and getattr(e, "item", {}).get("role") == "assistant"
    ]
    assert refusal_events, "expected a session-visible refusal event"
    assert executor_module.RECOVERY_ERROR_CODE in refusal_events[0].item["content"][0]["text"]
