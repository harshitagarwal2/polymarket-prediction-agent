from __future__ import annotations

from pathlib import Path

from engine.runtime_bootstrap import build_current_state_read_adapter
from storage.current_projection import (
    PreviewRuntimeContext,
    build_preview_runtime_context as _build_preview_runtime_context,
)


def build_preview_runtime_context(
    opportunity_root: str | Path | None,
    *,
    policy=None,
) -> PreviewRuntimeContext:
    return _build_preview_runtime_context(
        opportunity_root,
        policy=policy,
        read_adapter=build_current_state_read_adapter(opportunity_root),
    )


__all__ = ["PreviewRuntimeContext", "build_preview_runtime_context"]
