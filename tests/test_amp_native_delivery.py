"""Delivery-state fault injection for the ``amp-native`` harness (AMP-NATIVE-001).

Covers the durable delivery journal, the paste/Enter boundary ordering, restart
reconciliation (no blind replay), terminal-recreate survival, and the
server-side set-once dedupe for the browser optimistic item.
"""

from __future__ import annotations

import json
import subprocess
import types
from pathlib import Path

import pytest

from omnigent import amp_native_bridge
from omnigent.amp_native_bridge import InjectionHooks, inject_user_message
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
        # has-session probe -> alive.
        if "has-session" in argv:
            return subprocess.CompletedProcess(argv, 0, "", "")
        # load-buffer / paste-buffer / send-keys -> record + succeed.
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
    # Persisted to the versioned delivery subdir (not the inbox).
    assert (tmp_path / "bridge" / "delivery" / f"{record.delivery_id}.json").exists()
    reloaded = json.loads(
        (tmp_path / "bridge" / "delivery" / f"{record.delivery_id}.json").read_text()
    )
    assert set(
        {
            "delivery_id",
            "conversation_id",
            "response_id",
            "normalized_content_hash",
            "state",
            "created_at",
            "updated_at",
        }
    ).issubset(reloaded)


def test_submission_started_is_durable_before_paste(fake_tmux: _FakeTmux) -> None:
    """submission_started is written + fsynced BEFORE any paste/Enter byte."""
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="ship it", conversation_id="conv_1")

    seen_started_before_paste = {"value": False}

    def _before_paste() -> None:
        # At this boundary the durable record must already be submission_started.
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
# Fault injection at the four termination points
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "boundary",
    ["before_paste", "after_paste", "after_enter"],
)
def test_termination_during_injection_leaves_recovery_state(
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
    # The record is durably submission_started (never reverted to pending).
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
    # Plugin mirrors the user message back; confirmation correlates by response_id.
    journal.confirm(record.delivery_id, response_id="T-1:ev_42")
    reloaded = journal.get(record.delivery_id)
    assert reloaded is not None
    assert reloaded.state == DeliveryState.CONFIRMED.value
    assert reloaded.response_id == "T-1:ev_42"


# --------------------------------------------------------------------------- #
# Restart reconciliation + no blind replay
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "boundary",
    ["before_paste", "after_paste", "after_enter"],
)
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
    """A confirmed (terminal) record is never moved to recovery_required."""
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="ok", conversation_id="conv_1")
    journal.confirm(record.delivery_id, response_id="T-2:ev_9")
    recovered = journal.reconcile_on_start()
    assert recovered == []
    assert journal.get(record.delivery_id).state == DeliveryState.CONFIRMED.value


def test_executor_does_not_blindly_replay_recovered_prompt(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A retry of a recovered prompt surfaces a typed result, never a replay."""
    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    # Simulate a prior process that died mid-injection.
    journal = DeliveryJournal(bridge)
    journal.create(content="once is enough", conversation_id="conv_1")
    journal.reconcile_on_start()  # pending -> recovery_required (no submission)

    executor = AmpNativeExecutor(bridge_dir=bridge)
    # The constructor's own reconcile-on-start also marks it recovery_required.
    assert journal.load_all()[0].state == DeliveryState.RECOVERY_REQUIRED.value

    messages = [{"role": "user", "content": "once is enough"}]

    async def _drive() -> list[str]:
        events = []
        async for event in executor.run_turn(messages, [], ""):
            events.append(type(event).__name__)
            md = getattr(event, "message", "")
            if isinstance(md, str):
                events.append(md)
        return events

    import asyncio

    events = asyncio.run(_drive())
    assert executor_module.RECOVERY_ERROR_CODE in " ".join(events)
    # No tmux paste/Enter reached Amp — no blind replay.
    assert fake_tmux.paste_calls == 0
    assert fake_tmux.enter_calls == 0


def test_executor_injects_new_prompt_after_recovery(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A genuinely different prompt still delivers; only the recovered one is blocked."""
    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    journal = DeliveryJournal(bridge)
    rec = journal.create(content="stuck prompt", conversation_id="conv_1")
    journal.mark_recovery_required(rec.delivery_id, reason="prior crash")

    executor = AmpNativeExecutor(bridge_dir=bridge)
    messages = [{"role": "user", "content": "a brand new question"}]

    async def _drive() -> str:
        async for event in executor.run_turn(messages, [], ""):
            return type(event).__name__
        return "none"

    import asyncio

    outcome = asyncio.run(_drive())
    assert outcome == "TurnComplete"
    assert fake_tmux.paste_calls == 1
    # The new delivery was advanced to submission_started before paste.
    new_records = [r for r in journal.load_all() if r.delivery_id != rec.delivery_id]
    assert new_records
    assert new_records[-1].state == DeliveryState.SUBMISSION_STARTED.value


# --------------------------------------------------------------------------- #
# Terminal-recreate survival
# --------------------------------------------------------------------------- #


def test_clear_inbox_does_not_wipe_delivery_state(tmp_path: Path) -> None:
    """Recreate (prepare_bridge_dir + clear_inbox) must preserve pending records."""
    bridge = amp_native_bridge.prepare_bridge_dir("conv_1")
    journal = DeliveryJournal(bridge)
    record = journal.create(content="survive recreate", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)

    # Simulate the runner recreate path: refresh the bridge + clear stale inbox.
    amp_native_bridge.prepare_bridge_dir("conv_1")
    amp_native_bridge.clear_inbox(bridge)

    reloaded = journal.get(record.delivery_id)
    assert reloaded is not None
    assert reloaded.state == DeliveryState.SUBMISSION_STARTED.value
    # Recovery still works after recreate — the pending record survived.
    recovered = DeliveryJournal(bridge).reconcile_on_start()
    assert any(r.delivery_id == record.delivery_id for r in recovered)


# --------------------------------------------------------------------------- #
# Server-side set-once dedupe for the browser optimistic item
# --------------------------------------------------------------------------- #


def test_external_item_dedupe_is_set_once() -> None:
    """A repeated plugin post (same response_id) cannot duplicate the durable item."""
    external_item_dedupe.reset_for_tests()
    assert external_item_dedupe.claim("conv_1", "T-1:ev_42") is True
    # Second claim for the same (conversation, response_id) is refused.
    assert external_item_dedupe.claim("conv_1", "T-1:ev_42") is False
    assert external_item_dedupe.is_claimed("conv_1", "T-1:ev_42") is True
    # A different response_id on the same conversation still claims.
    assert external_item_dedupe.claim("conv_1", "T-1:ev_43") is True
    # A different conversation is independent.
    assert external_item_dedupe.claim("conv_2", "T-1:ev_42") is True
    external_item_dedupe.reset_for_tests()


def test_persist_external_conversation_item_dedupes_by_response_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two posts with the same response_id persist exactly one durable item."""
    external_item_dedupe.reset_for_tests()

    persisted: list[object] = []

    class _FakeStore:
        def append(self, session_id: str, items: list[object]) -> list[object]:
            results = []
            for index, item in enumerate(items):
                # The real store assigns a primary key on append; mirror that
                # so the persist path's ``persisted.id`` access succeeds.
                saved = types.SimpleNamespace(
                    id=f"item_{index}",
                    type=getattr(item, "type", None),
                    data=getattr(item, "data", None),
                    response_id=getattr(item, "response_id", None),
                )
                persisted.append(saved)
                results.append(saved)
            return results

    class _Conv:
        labels: dict[str, str] = {}
        title = None

    import omnigent.server.routes.sessions as server_routes

    # Side-effecting helpers (publish, title seeding) are irrelevant to the
    # dedupe contract; neutralize them so the test asserts only append behavior.

    def _noop(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(server_routes, "_publish_external_conversation_item", _noop)
    monkeypatch.setattr(server_routes, "_drive_terminal_resolved_elicitation", _noop)
    monkeypatch.setattr(server_routes, "prepare_background_session_title", _noop)
    monkeypatch.setattr(server_routes, "_seed_missing_title_from_user_message", _noop_async)

    body = types.SimpleNamespace(
        data={
            "item_type": "message",
            "response_id": "T-1:ev_42",
            "item_data": {
                "role": "user",
                "content": [{"type": "input_text", "text": "hello"}],
            },
        }
    )
    import asyncio

    first = asyncio.run(
        server_routes._persist_external_conversation_item(
            "conv_1",
            _Conv(),
            body,
            _FakeStore(),  # type: ignore[arg-type]
        )
    )
    second = asyncio.run(
        server_routes._persist_external_conversation_item(
            "conv_1",
            _Conv(),
            body,
            _FakeStore(),  # type: ignore[arg-type]
        )
    )
    # Exactly one durable item was appended. The first post persisted it
    # (store-assigned id); the duplicate returned the claimed response_id
    # WITHOUT appending a second item.
    assert len(persisted) == 1
    assert first == "item_0"
    assert second == "T-1:ev_42"
    external_item_dedupe.reset_for_tests()


async def _noop_async(*args: object, **kwargs: object) -> None:
    return None
