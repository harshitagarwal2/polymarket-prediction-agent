from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from engine import runtime_bootstrap
from scripts import run_agent_loop


class RuntimeBootstrapTests(unittest.TestCase):
    def test_build_current_state_read_adapter_defaults_to_file_backed_adapter(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            with (
                patch.dict(
                    "os.environ",
                    {
                        "PREDICTION_MARKET_POSTGRES_DSN": "",
                        "POSTGRES_DSN": "",
                        "DATABASE_URL": "",
                    },
                ),
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
    def test_validate_runtime_requires_opportunity_root_for_live_modes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "runtime" / "events.jsonl"
            state = Path(temp_dir) / "runtime" / "safety-state.json"
            args = SimpleNamespace(
                venue="polymarket",
                fair_values_file=None,
                policy_file=None,
                journal=str(journal),
                state_file=str(state),
                mode="run",
                opportunity_root=None,
            )

            with patch.dict("os.environ", {"POLYMARKET_PRIVATE_KEY": "pk"}, clear=True):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "opportunity root must be provided",
                ):
                    run_agent_loop.validate_runtime(args)

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

            with patch.dict(
                "os.environ",
                {
                    "POLYMARKET_PRIVATE_KEY": "pk",
                    "POLYMARKET_ROUTE_LABEL": "eu-proxy-1",
                    "POLYMARKET_GEO_COMPLIANCE_ACK": "true",
                },
                clear=True,
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "runtime projected current-state reads requires Postgres authority",
                ):
                    run_agent_loop.validate_runtime(args)

    def test_validate_runtime_allows_missing_fair_values_file_for_live_mode_with_postgres(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "runtime" / "events.jsonl"
            state = Path(temp_dir) / "runtime" / "safety-state.json"
            args = SimpleNamespace(
                venue="polymarket",
                fair_values_file=None,
                policy_file=None,
                journal=str(journal),
                state_file=str(state),
                mode="run",
                opportunity_root=str(Path(temp_dir) / "data"),
            )

            with (
                patch.dict(
                    "os.environ",
                    {
                        "POLYMARKET_PRIVATE_KEY": "pk",
                        "POLYMARKET_ROUTE_LABEL": "eu-proxy-1",
                        "POLYMARKET_GEO_COMPLIANCE_ACK": "true",
                    },
                    clear=True,
                ),
                patch.object(
                    run_agent_loop,
                    "build_current_state_read_adapter",
                    return_value=object(),
                ),
            ):
                run_agent_loop.validate_runtime(args)

    def test_build_adapter_derives_polymarket_live_user_markets_from_projected_state(
        self,
    ):
        adapter = SimpleNamespace(
            read_table=lambda table: {
                "pm-1": {"market_id": "pm-1"},
                "pm-2": {"market_id": "pm-2"},
            }
            if table == "fair_values"
            else {
                "pm-1": {
                    "market_id": "pm-1",
                    "raw_json": {"conditionId": "cond-1"},
                },
                "pm-2": {
                    "market_id": "pm-2",
                    "raw_json": {"conditionId": "cond-2"},
                },
            }
            if table == "polymarket_markets"
            else {}
        )

        args = SimpleNamespace(
            fair_values_file=None,
            polymarket_live_user_markets=None,
            polymarket_user_ws_host=None,
            mode="run",
            opportunity_root="runtime/data",
        )

        with (
            patch.dict("os.environ", {"POLYMARKET_PRIVATE_KEY": "pk"}, clear=False),
            patch.object(
                runtime_bootstrap,
                "build_current_state_read_adapter",
                return_value=adapter,
            ),
        ):
            built = runtime_bootstrap.build_adapter("polymarket", args)

        self.assertEqual(
            cast(Any, built).config.live_user_markets, ["cond-1", "cond-2"]
        )

    def test_build_adapter_can_load_polymarket_private_key_from_file(self):
        with tempfile.NamedTemporaryFile("w+") as handle:
            handle.write("file-private-key")
            handle.flush()
            args = SimpleNamespace(
                fair_values_file=None,
                polymarket_live_user_markets=None,
                polymarket_user_ws_host=None,
                mode="preview",
                opportunity_root=None,
            )

            with patch.dict(
                "os.environ",
                {
                    "POLYMARKET_PRIVATE_KEY": "",
                    "POLYMARKET_PRIVATE_KEY_FILE": handle.name,
                },
                clear=False,
            ):
                built = runtime_bootstrap.build_adapter("polymarket", args)

        self.assertEqual(cast(Any, built).config.private_key, "file-private-key")

    def test_build_adapter_can_load_polymarket_private_key_from_command(self):
        args = SimpleNamespace(
            fair_values_file=None,
            polymarket_live_user_markets=None,
            polymarket_user_ws_host=None,
            mode="preview",
            opportunity_root=None,
        )

        with (
            patch.dict(
                "os.environ",
                {
                    "POLYMARKET_PRIVATE_KEY": "",
                    "POLYMARKET_PRIVATE_KEY_COMMAND": "python -c \"print('cmd-private-key')\"",
                },
                clear=False,
            ),
            patch.object(runtime_bootstrap.subprocess, "run") as run,
        ):
            run.return_value = SimpleNamespace(stdout="cmd-private-key\n")
            built = runtime_bootstrap.build_adapter("polymarket", args)

        self.assertEqual(cast(Any, built).config.private_key, "cmd-private-key")

    def test_build_adapter_applies_polymarket_host_overrides_from_env(self):
        args = SimpleNamespace(
            fair_values_file=None,
            polymarket_live_user_markets=None,
            polymarket_user_ws_host=None,
            mode="preview",
            opportunity_root=None,
        )

        with patch.dict(
            "os.environ",
            {
                "POLYMARKET_PRIVATE_KEY": "pk",
                "POLYMARKET_CLOB_HOST": "https://private-clob.example.invalid",
                "POLYMARKET_DATA_API_HOST": "https://private-data.example.invalid",
            },
            clear=False,
        ):
            built = runtime_bootstrap.build_adapter("polymarket", args)

        self.assertEqual(
            cast(Any, built).config.host, "https://private-clob.example.invalid"
        )
        self.assertEqual(
            cast(Any, built).config.data_api_host,
            "https://private-data.example.invalid",
        )

    def test_validate_polymarket_live_routing_requires_route_label_and_ack(self):
        with patch.dict(
            "os.environ",
            {
                "POLYMARKET_ROUTE_LABEL": "",
                "POLYMARKET_GEO_COMPLIANCE_ACK": "false",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(RuntimeError, "POLYMARKET_ROUTE_LABEL"):
                runtime_bootstrap.validate_polymarket_live_routing(
                    context="run-agent-loop live mode"
                )

        with patch.dict(
            "os.environ",
            {
                "POLYMARKET_ROUTE_LABEL": "eu-proxy-1",
                "POLYMARKET_GEO_COMPLIANCE_ACK": "true",
            },
            clear=False,
        ):
            self.assertEqual(
                runtime_bootstrap.validate_polymarket_live_routing(
                    context="run-agent-loop live mode"
                ),
                "eu-proxy-1",
            )

    def test_validate_polymarket_private_order_flow_requires_non_default_host(self):
        with patch.dict(
            "os.environ",
            {
                "POLYMARKET_PRIVATE_ORDER_FLOW_REQUIRED": "true",
                "POLYMARKET_CLOB_HOST": runtime_bootstrap.PolymarketConfig.host,
            },
            clear=False,
        ):
            with self.assertRaisesRegex(
                RuntimeError, "non-default POLYMARKET_CLOB_HOST"
            ):
                runtime_bootstrap.validate_polymarket_private_order_flow(
                    context="run-agent-loop live mode"
                )

        with patch.dict(
            "os.environ",
            {
                "POLYMARKET_PRIVATE_ORDER_FLOW_REQUIRED": "true",
                "POLYMARKET_CLOB_HOST": "https://private-clob.example.invalid",
            },
            clear=False,
        ):
            runtime_bootstrap.validate_polymarket_private_order_flow(
                context="run-agent-loop live mode"
            )

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
