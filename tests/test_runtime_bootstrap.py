from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from engine import runtime_bootstrap
from scripts import run_agent_loop


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

    def test_build_current_state_read_adapter_prefers_projected_adapter_with_env_dsn(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            with (
                patch.dict(
                    "os.environ",
                    {
                        "PREDICTION_MARKET_POSTGRES_DSN": "postgresql://user:pass@localhost:5432/db"
                    },
                    clear=False,
                ),
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

    def test_build_current_state_read_adapter_can_require_postgres_authority(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            with patch.dict("os.environ", {}, clear=True):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "runtime projected current-state reads requires Postgres authority",
                ):
                    runtime_bootstrap.build_current_state_read_adapter(
                        root,
                        require_postgres=True,
                    )


class RunAgentLoopPostgresAuthorityTests(unittest.TestCase):
    def test_validate_runtime_requires_postgres_for_live_mode_with_opportunity_root(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "runtime" / "events.jsonl"
            state = Path(temp_dir) / "runtime" / "safety-state.json"
            args = SimpleNamespace(
                venue="polymarket",
                fair_values_file=__file__,
                policy_file=None,
                journal=str(journal),
                state_file=str(state),
                mode="run",
                opportunity_root=str(Path(temp_dir) / "data"),
            )

            with patch.dict("os.environ", {"POLYMARKET_PRIVATE_KEY": "pk"}, clear=True):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "runtime projected current-state reads requires Postgres authority",
                ):
                    run_agent_loop.validate_runtime(args)

    def test_validate_runtime_keeps_preview_mode_file_backed_when_postgres_is_absent(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "runtime" / "events.jsonl"
            state = Path(temp_dir) / "runtime" / "safety-state.json"
            args = SimpleNamespace(
                venue="polymarket",
                fair_values_file=__file__,
                policy_file=None,
                journal=str(journal),
                state_file=str(state),
                mode="preview",
                opportunity_root=str(Path(temp_dir) / "data"),
            )

            with patch.dict("os.environ", {"POLYMARKET_PRIVATE_KEY": "pk"}, clear=True):
                run_agent_loop.validate_runtime(args)


if __name__ == "__main__":
    unittest.main()
