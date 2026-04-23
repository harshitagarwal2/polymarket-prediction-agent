from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from engine.cli_output import add_quiet_flag, emit_json
from engine.config_loader import load_config_file, nested_config_value
from forecasting import ForecastModelRegistry
from forecasting.model_registry import build_model_registry_repository
from research.data.build_training_set import load_training_set_rows
from research.datasets import DatasetRegistry
from research.schemas import load_benchmark_case
from research.train.train_blend import write_blend_config
from research.train.train_bt import write_bt_artifact, write_bt_artifact_from_rows
from research.train.train_consensus import (
    write_consensus_artifact,
    write_consensus_artifact_from_rows,
)
from research.train.train_elo import write_elo_artifact, write_elo_artifact_from_rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train lightweight research model artifacts."
    )
    parser.add_argument(
        "--model",
        choices=("elo", "bt", "blend", "consensus"),
        default=None,
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--cases", nargs="*", default=[])
    parser.add_argument("--training-data", default=None)
    parser.add_argument("--training-dataset", default=None)
    parser.add_argument("--training-dataset-version", default=None)
    parser.add_argument("--dataset-root", default=None)
    parser.add_argument("--blend-weight", type=float, default=0.5)
    parser.add_argument("--half-life-seconds", type=float, default=3600.0)
    parser.add_argument("--config-file", default=None)
    parser.add_argument("--model-version", default="v1")
    parser.add_argument("--registry-root", default=None)
    add_quiet_flag(parser)
    return parser


def _load_training_row_payloads(args) -> list[dict[str, Any]]:
    if args.training_dataset not in (None, ""):
        dataset_root = (
            Path(args.dataset_root)
            if args.dataset_root not in (None, "")
            else Path("runtime/data/datasets")
        )
        registry = DatasetRegistry(dataset_root)
        return registry.read_rows(
            str(args.training_dataset),
            version=(
                str(args.training_dataset_version)
                if args.training_dataset_version not in (None, "")
                else None
            ),
        )
    if args.training_data not in (None, ""):
        return [row.to_payload() for row in load_training_set_rows(args.training_data)]
    return []


def main() -> None:
    args = build_parser().parse_args()
    if args.training_data and args.training_dataset:
        raise RuntimeError(
            "train_models accepts either --training-data or --training-dataset, not both"
        )
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
    elif model == "consensus":
        has_training_source = bool(args.training_data or args.training_dataset)
        training_rows = _load_training_row_payloads(args)
        if has_training_source:
            path = write_consensus_artifact_from_rows(
                training_rows,
                args.output,
                half_life_seconds=args.half_life_seconds,
                model_version=args.model_version,
            )
        else:
            path = write_consensus_artifact(
                args.output,
                half_life_seconds=args.half_life_seconds,
                model_version=args.model_version,
            )
    else:
        has_training_source = bool(args.training_data or args.training_dataset)
        training_rows = _load_training_row_payloads(args)
        if has_training_source:
            if model == "elo":
                path = write_elo_artifact_from_rows(training_rows, args.output)
            else:
                path = write_bt_artifact_from_rows(training_rows, args.output)
        else:
            cases = [load_benchmark_case(case_path) for case_path in args.cases]
            if model == "elo":
                path = write_elo_artifact(cases, args.output)
            else:
                path = write_bt_artifact(cases, args.output)
    artifact_payload = json.loads(Path(path).read_text(encoding="utf-8"))
    training_match_count = artifact_payload.get("training_match_count")
    metrics = (
        {"training_match_count": int(training_match_count)}
        if isinstance(training_match_count, int)
        else {}
    )
    feature_spec = {
        "model": model,
        "input": (
            "training-dataset"
            if args.training_dataset
            else "training-data"
            if args.training_data
            else "cases"
        ),
    }
    registry_root = (
        Path(args.registry_root)
        if args.registry_root
        else Path(path).resolve().parent / "model_registry"
    )
    registry = ForecastModelRegistry(
        repository=build_model_registry_repository(registry_root)
    )
    registry.persist_artifact(
        model_name=model,
        model_version=args.model_version,
        feature_spec=feature_spec,
        metrics=metrics,
        artifact_uri=str(path),
    )
    emit_json({"model": model, "output": str(path)}, quiet=args.quiet)


if __name__ == "__main__":
    main()
