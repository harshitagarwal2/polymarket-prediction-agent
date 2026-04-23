from research.eval.calibration_eval import evaluate_probability_calibration
from research.eval.closing_value import evaluate_closing_value
from research.eval.dm_test import compare_loss_differentials
from research.eval.execution_metrics import summarize_execution_metrics
from research.eval.metrics import score_forecasts

__all__ = [
    "compare_loss_differentials",
    "evaluate_closing_value",
    "evaluate_probability_calibration",
    "run_dataset_walk_forward",
    "score_forecasts",
    "summarize_execution_metrics",
    "write_walk_forward_report_artifacts",
]


def write_walk_forward_report_artifacts(*args, **kwargs):
    from research.eval.reports import write_walk_forward_report_artifacts as _impl

    return _impl(*args, **kwargs)


def run_dataset_walk_forward(*args, **kwargs):
    from research.eval.walk_forward import run_dataset_walk_forward as _impl

    return _impl(*args, **kwargs)
