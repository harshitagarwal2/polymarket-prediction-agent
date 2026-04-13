from __future__ import annotations

from collections.abc import Mapping


def merge_feature_sets(*feature_sets: Mapping[str, object]) -> dict[str, object]:
    merged: dict[str, object] = {}
    for feature_set in feature_sets:
        merged.update(feature_set)
    return merged
