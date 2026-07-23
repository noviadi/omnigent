"""Interactive Amp TUI wrapper."""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from tempfile import TemporaryDirectory

import click
import yaml

from omnigent._wrapper_labels import AMP_NATIVE_WRAPPER_VALUE, WRAPPER_LABEL_KEY
from omnigent.native_coding_agents import native_shell_terminal_spec


def resolve_amp_executable(
    *, env: Mapping[str, str] | None = None, which: Callable[[str], str | None] | None = None
) -> str:
    env = os.environ if env is None else env
    command = env.get("OMNIGENT_AMP_PATH", "").strip() or "amp"
    resolved = (which or shutil.which)(command)
    if resolved is None:
        raise click.ClickException(
            "Amp requires the 'amp' CLI on PATH; install it or set OMNIGENT_AMP_PATH."
        )
    return resolved


def build_amp_launch(
    args: Sequence[str],
    *,
    external_session_id: str | None = None,
    env: Mapping[str, str] | None = None,
    which: Callable[[str], str | None] | None = None,
) -> list[str]:
    executable = resolve_amp_executable(env=env, which=which)
    return [
        executable,
        *(["threads", "continue", external_session_id] if external_session_id else []),
        *args,
    ]


def _materialize_amp_agent_spec(tmpdir: Path) -> Path:
    target = tmpdir / "amp-native-ui.yaml"
    target.write_text(
        yaml.safe_dump(
            {
                "name": "amp-native-ui",
                "prompt": (
                    "Amp runs in the session terminal; browser messages use its "
                    "native plugin bridge."
                ),
                "executor": {"harness": "amp-native"},
                "spawn": True,
                "os_env": {"type": "caller_process", "cwd": ".", "sandbox": {"type": "none"}},
                "terminals": native_shell_terminal_spec(),
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return target


def run_amp_native(
    *,
    server: str | None,
    session_id: str | None,
    amp_args: tuple[str, ...],
    resume_picker: bool = False,
    auto_open_conversation: bool = False,
) -> None:
    if server is None:
        raise click.ClickException("Amp requires a resolved Omnigent server URL.")
    # Reuse the transport machinery, but scope every transport identifier to
    # Amp. The lower flow creates/binds the daemon runner, persists launch
    # arguments, ensures amp:main, waits for it, and attaches or reattaches.
    import omnigent.pi_native as transport

    with TemporaryDirectory(prefix="omnigent-amp-native-") as tmp:
        previous = (
            transport._AGENT_NAME,
            transport._TERMINAL_NAME,
            transport._WRAPPER_LABEL_VALUE,
            transport._SESSION_LABELS,
        )
        transport._AGENT_NAME = "amp-native-ui"
        transport._TERMINAL_NAME = "amp"
        transport._WRAPPER_LABEL_VALUE = AMP_NATIVE_WRAPPER_VALUE
        transport._SESSION_LABELS = {
            "omnigent.ui": "terminal",
            WRAPPER_LABEL_KEY: AMP_NATIVE_WRAPPER_VALUE,
        }
        try:
            transport._run_with_remote_server(
                server.rstrip("/"),
                _materialize_amp_agent_spec(Path(tmp)),
                session_id=session_id,
                resume_picker=resume_picker,
                pi_args=amp_args,
                auto_open_conversation=auto_open_conversation,
            )
        finally:
            (
                transport._AGENT_NAME,
                transport._TERMINAL_NAME,
                transport._WRAPPER_LABEL_VALUE,
                transport._SESSION_LABELS,
            ) = previous
