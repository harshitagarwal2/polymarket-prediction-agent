from __future__ import annotations

from pathlib import Path

from research.benchmark_suite import run_walk_forward_benchmark_suite


def run_dataset_walk_forward(
    *,
    dataset_name: str,
    dataset_root: str | Path,
    version: str | None = None,
    min_train_size: int = 1,
    test_size: int = 1,
    step_size: int | None = None,
    max_splits: int | None = None,
    model_generator: str | None = None,
):
    return run_walk_forward_benchmark_suite(
        dataset_name=dataset_name,
        dataset_root=dataset_root,
        version=version,
        min_train_size=min_train_size,
        test_size=test_size,
        step_size=step_size,
        max_splits=max_splits,
        model_generator=model_generator,
    )
