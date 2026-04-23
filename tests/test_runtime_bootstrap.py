from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from engine import runtime_bootstrap


class RuntimeBootstrapTests(unittest.TestCase):
    def test_build_current_state_read_adapter_defaults_to_file_backed_adapter(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            with (
                patch.object(
                    runtime_bootstrap.FileCurrentStateReadAdapter,
                    "from_opportunity_root",
                    return_value="file-adapter",
                ) as file_adapter_factory,
                patch.object(
                    runtime_bootstrap.ProjectedCurrentStateReadAdapter,
                    "from_root",
                    return_value="projected-adapter",
                ) as projected_adapter_factory,
            ):
                adapter = runtime_bootstrap.build_current_state_read_adapter(root)

        self.assertEqual(adapter, "file-adapter")
        file_adapter_factory.assert_called_once_with(root)
        projected_adapter_factory.assert_not_called()

    def test_build_current_state_read_adapter_prefers_projected_adapter_with_marker(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            postgres_root = root / "postgres"
            postgres_root.mkdir(parents=True, exist_ok=True)
            (postgres_root / "postgres.dsn").write_text(
                "postgresql://user:pass@localhost:5432/db",
                encoding="utf-8",
            )
            with (
                patch.object(
                    runtime_bootstrap.ProjectedCurrentStateReadAdapter,
                    "from_root",
                    return_value="projected-adapter",
                ) as projected_adapter_factory,
                patch.object(
                    runtime_bootstrap.FileCurrentStateReadAdapter,
                    "from_opportunity_root",
                    return_value="file-adapter",
                ) as file_adapter_factory,
            ):
                adapter = runtime_bootstrap.build_current_state_read_adapter(root)

        self.assertEqual(adapter, "projected-adapter")
        projected_adapter_factory.assert_called_once_with(root)
        file_adapter_factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
