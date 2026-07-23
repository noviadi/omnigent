"""``harness: amp-native`` wrap for interactive Amp."""

from fastapi import FastAPI

from omnigent.inner.amp_native_executor import AmpNativeExecutor
from omnigent.runtime.harnesses._executor_adapter import ExecutorAdapter


def create_app() -> FastAPI:
    return ExecutorAdapter(executor_factory=AmpNativeExecutor).build()
