from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from llm import advisory_context


class AdvisoryContextTests(unittest.TestCase):
    def test_build_preview_runtime_context_uses_current_state_read_adapter(self):
        current_state_adapter = object()
        expected_context = advisory_context.PreviewRuntimeContext((), ())

        with (
            patch.object(
                advisory_context,
                "build_current_state_read_adapter",
                return_value=current_state_adapter,
            ) as build_read_adapter,
            patch.object(
                advisory_context,
                "_build_preview_runtime_context",
                return_value=expected_context,
            ) as build_preview_context,
        ):
            context = advisory_context.build_preview_runtime_context(
                Path("runtime/data"),
            )

        self.assertIs(context, expected_context)
        build_read_adapter.assert_called_once_with(Path("runtime/data"))
        build_preview_context.assert_called_once_with(
            Path("runtime/data"),
            policy=None,
            read_adapter=current_state_adapter,
        )


if __name__ == "__main__":
    unittest.main()
