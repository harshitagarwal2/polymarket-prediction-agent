from __future__ import annotations

import subprocess
import sys
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import (
    build_current_state_fair_values,
    build_fair_values,
    build_inference_dataset,
    build_mappings,
    build_opportunities,
    build_sports_fair_values,
    build_training_dataset,
    train_models,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _project_script_targets() -> dict[str, str]:
    targets: dict[str, str] = {}
    in_project_scripts = False
    for raw_line in (REPO_ROOT / "pyproject.toml").read_text().splitlines():
        line = raw_line.strip()
        if line.startswith("["):
            in_project_scripts = line == "[project.scripts]"
            continue
        if not in_project_scripts or not line or line.startswith("#"):
            continue
        name, _, value = line.partition("=")
        if not value:
            continue
        targets[name.strip()] = value.strip().strip('"')
    return targets


class ConsoleScriptEntryPointsTests(unittest.TestCase):
    def test_benchmark_console_scripts_target_script_wrappers(self):
        targets = _project_script_targets()

        self.assertEqual(
            targets["run-sports-benchmark"], "scripts.run_sports_benchmark:main"
        )
        self.assertEqual(
            targets["run-sports-benchmark-suite"],
            "scripts.run_sports_benchmark_suite:main",
        )
        self.assertEqual(
            targets["prediction-market-sports-benchmark"],
            "scripts.run_sports_benchmark:main",
        )
        self.assertEqual(
            targets["prediction-market-sports-benchmark-suite"],
            "scripts.run_sports_benchmark_suite:main",
        )
        self.assertEqual(
            targets["run-polymarket-capture"],
            "scripts.run_polymarket_capture:main",
        )
        self.assertEqual(
            targets["run-current-projection"],
            "scripts.run_current_projection:main",
        )
        self.assertEqual(
            targets["run-replay-attribution"],
            "scripts.run_replay_attribution:main",
        )
        self.assertEqual(targets["build-mappings"], "scripts.build_mappings:main")
        self.assertEqual(
            targets["build-current-state-fair-values"],
            "scripts.build_current_state_fair_values:main",
        )
        self.assertEqual(
            targets["build-opportunities"], "scripts.build_opportunities:main"
        )
        self.assertEqual(
            targets["build-inference-dataset"],
            "scripts.build_inference_dataset:main",
        )
        self.assertEqual(
            targets["build-training-dataset"],
            "scripts.build_training_dataset:main",
        )

    def test_console_script_modules_import_without_sys_path_mutation(self):
        targets = _project_script_targets()
        modules = sorted(
            {
                entrypoint.partition(":")[0]
                for entrypoint in targets.values()
                if entrypoint.partition(":")[0].startswith("scripts.")
            }
        )
        self.assertTrue(modules)
        check = textwrap.dedent(
            f"""
            import importlib
            import json
            import sys

            modules = {modules!r}
            mutations = []

            for module_name in modules:
                before = list(sys.path)
                importlib.import_module(module_name)
                after = list(sys.path)
                if after != before:
                    mutations.append(
                        {{
                            "module": module_name,
                            "before": before[:3],
                            "after": after[:3],
                        }}
                    )

            if mutations:
                print(json.dumps(mutations, indent=2))
                raise SystemExit(1)
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", check],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(
            result.returncode,
            0,
            msg=f"stdout:\\n{result.stdout}\\nstderr:\\n{result.stderr}",
        )

    def test_research_entrypoint_scripts_delegate_to_package_modules(self):
        self.assertEqual(train_models.main.__module__, "research.train_cli")
        self.assertEqual(build_fair_values.main.__module__, "research.fair_values_cli")
        self.assertEqual(
            build_sports_fair_values.main.__module__,
            "research.fair_values_cli",
        )

    def test_builder_wrapper_entrypoints_delegate_to_ingest_live_data(self):
        self.assertEqual(build_mappings.main.__module__, "scripts.build_mappings")
        self.assertEqual(
            build_opportunities.main.__module__, "scripts.build_opportunities"
        )
        self.assertEqual(
            build_inference_dataset.main.__module__,
            "scripts.build_inference_dataset",
        )
        self.assertEqual(
            build_current_state_fair_values.main.__module__,
            "scripts.build_current_state_fair_values",
        )
        self.assertEqual(
            build_training_dataset.main.__module__,
            "scripts.build_training_dataset",
        )

    def test_serious_builder_wrappers_require_postgres_authority_by_default(self):
        with patch("scripts.build_mappings.ingest_live_data.main", return_value=0) as mappings:
            build_mappings.main(["--root", "runtime/data"])

        with patch(
            "scripts.build_opportunities.ingest_live_data.main", return_value=0
        ) as opportunities:
            build_opportunities.main(["--root", "runtime/data"])

        with patch(
            "scripts.build_inference_dataset.ingest_live_data.main", return_value=0
        ) as inference:
            build_inference_dataset.main(["--root", "runtime/data"])

        with patch(
            "scripts.build_current_state_fair_values.ingest_live_data.main",
            return_value=0,
        ) as fair_values:
            build_current_state_fair_values.main(["--root", "runtime/data"])

        with patch(
            "scripts.build_training_dataset.ingest_live_data.main", return_value=0
        ) as training:
            build_training_dataset.main(["--root", "runtime/data"])

        mappings.assert_called_once_with(
            ["build-mappings", "--require-postgres-authority", "--root", "runtime/data"]
        )
        opportunities.assert_called_once_with(
            [
                "build-opportunities",
                "--require-postgres-authority",
                "--root",
                "runtime/data",
            ]
        )
        inference.assert_called_once_with(
            [
                "build-inference-dataset",
                "--require-postgres-authority",
                "--root",
                "runtime/data",
            ]
        )
        fair_values.assert_called_once_with(
            [
                "build-fair-values",
                "--require-postgres-authority",
                "--root",
                "runtime/data",
            ]
        )
        training.assert_called_once_with(["build-training-dataset", "--root", "runtime/data"])


if __name__ == "__main__":
    unittest.main()
