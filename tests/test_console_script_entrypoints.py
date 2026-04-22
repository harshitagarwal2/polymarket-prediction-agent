from __future__ import annotations

import subprocess
import sys
import textwrap
import unittest
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
