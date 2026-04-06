from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DIAGRAMS_DIR = ROOT / "docs" / "diagrams"
RENDERED_DIR = DIAGRAMS_DIR / "rendered"


def extract_mermaid(markdown: str) -> str:
    match = re.search(r"```mermaid\n(.*?)\n```", markdown, re.DOTALL)
    if not match:
        raise ValueError("No Mermaid block found")
    return match.group(1)


def source_files() -> list[Path]:
    return sorted(
        path for path in DIAGRAMS_DIR.glob("*.md") if path.name != "README.md"
    )


def render_file(source: Path) -> Path:
    markdown = source.read_text(encoding="utf-8")
    mermaid = extract_mermaid(markdown)
    output_path = RENDERED_DIR / f"{source.stem}.svg"
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_input = Path(temp_dir) / f"{source.stem}.mmd"
        temp_input.write_text(mermaid, encoding="utf-8")
        subprocess.run(
            [
                "npx",
                "-y",
                "@mermaid-js/mermaid-cli",
                "-i",
                str(temp_input),
                "-o",
                str(output_path),
                "-b",
                "transparent",
            ],
            check=True,
            cwd=ROOT,
        )
    return output_path


def write_index(rendered: list[tuple[Path, Path]]) -> None:
    lines = ["# Rendered Diagrams", "", "Generated SVG artifacts:", ""]
    for source, output in rendered:
        lines.append(f"- `{source.name}` -> `rendered/{output.name}`")
    (DIAGRAMS_DIR / "RENDERED.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    RENDERED_DIR.mkdir(parents=True, exist_ok=True)
    rendered: list[tuple[Path, Path]] = []
    for source in source_files():
        output = render_file(source)
        rendered.append((source, output))
        print(f"rendered {source.name} -> {output.relative_to(ROOT)}")
    write_index(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
