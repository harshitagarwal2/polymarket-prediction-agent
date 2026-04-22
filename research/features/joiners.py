from __future__ import annotations

from collections.abc import Mapping


def merge_feature_sets(*feature_sets: Mapping[str, object]) -> dict[str, object]:
    merged: dict[str, object] = {}
    for feature_set in feature_sets:
        merged.update(feature_set)
    return merged


def merge_feature_namespaces(
    **named_feature_sets: Mapping[str, object],
) -> dict[str, object]:
    merged: dict[str, object] = {}
    for namespace, feature_set in named_feature_sets.items():
        for key, value in feature_set.items():
            merged[f"{namespace}_{key}"] = value
    return merged
