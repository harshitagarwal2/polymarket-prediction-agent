from __future__ import annotations

import argparse
import json

from research.schemas import load_benchmark_case
from research.train.train_blend import write_blend_config
from research.train.train_bt import write_bt_artifact
from research.train.train_elo import write_elo_artifact
from scripts.config_loader import load_config_file, nested_config_value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train lightweight research model artifacts."
    )
    parser.add_argument("--model", choices=("elo", "bt", "blend"), default=None)
    parser.add_argument("--output", required=True)
    parser.add_argument("--cases", nargs="*", default=[])
    parser.add_argument("--blend-weight", type=float, default=0.5)
    parser.add_argument("--config-file", default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config_file(args.config_file) if args.config_file else {}
    model = args.model
    if model is None:
        configured_model = nested_config_value(config, "research", "model_generator")
        model = str(configured_model) if isinstance(configured_model, str) else None
    if model is None:
        raise RuntimeError(
            "train_models requires --model or a config with research.model_generator"
        )
    blend_weight = args.blend_weight
    configured_weight = nested_config_value(config, "research", "model_blend_weight")
    if isinstance(configured_weight, (int, float)):
        blend_weight = float(configured_weight)
    if model == "blend":
        path = write_blend_config(blend_weight, args.output)
    else:
        cases = [load_benchmark_case(case_path) for case_path in args.cases]
        if model == "elo":
            path = write_elo_artifact(cases, args.output)
        else:
            path = write_bt_artifact(cases, args.output)
    print(json.dumps({"model": model, "output": str(path)}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
