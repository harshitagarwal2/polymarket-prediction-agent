from __future__ import annotations

import json
from pathlib import Path


def write_blend_config(weight: float, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {"model_generator": "blend", "model_blend_weight": float(weight)},
            indent=2,
            sort_keys=True,
        )
    )
    return path
