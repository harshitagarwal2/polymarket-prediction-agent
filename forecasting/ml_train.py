from __future__ import annotations

import math
from collections import defaultdict

from forecasting.ml_infer import LinearFeatureModelArtifact


def fit_linear_feature_model(
    rows: list[dict[str, float]],
    *,
    label_key: str = "label",
    model_name: str = "linear_feature_model",
    model_version: str = "v1",
) -> LinearFeatureModelArtifact:
    if not rows:
        raise ValueError("rows must not be empty")
    positives = [row for row in rows if float(row.get(label_key, 0.0)) >= 0.5]
    negatives = [row for row in rows if float(row.get(label_key, 0.0)) < 0.5]
    positive_rate = max(1e-6, min(1.0 - 1e-6, len(positives) / len(rows)))
    bias = math.log(positive_rate / (1.0 - positive_rate))
    feature_names = sorted(
        {
            key
            for row in rows
            for key in row
            if key != label_key and isinstance(row.get(key), (int, float))
        }
    )
    weights: dict[str, float] = {}
    centers: dict[str, float] = {}
    for feature_name in feature_names:
        positive_mean = (
            sum(float(row.get(feature_name, 0.0)) for row in positives) / len(positives)
            if positives
            else 0.0
        )
        negative_mean = (
            sum(float(row.get(feature_name, 0.0)) for row in negatives) / len(negatives)
            if negatives
            else 0.0
        )
        weights[feature_name] = positive_mean - negative_mean
        centers[feature_name] = (positive_mean + negative_mean) / 2.0
    return LinearFeatureModelArtifact(
        model_name=model_name,
        model_version=model_version,
        bias=bias,
        weights=weights,
        centers=centers,
    )


def training_rows_from_labeled_features(
    features: list[dict[str, float]],
    labels: list[int],
) -> list[dict[str, float]]:
    if len(features) != len(labels):
        raise ValueError("features and labels must have the same length")
    rows: list[dict[str, float]] = []
    for feature_row, label in zip(features, labels):
        row = defaultdict(float, feature_row)
        row["label"] = float(label)
        rows.append(dict(row))
    return rows
