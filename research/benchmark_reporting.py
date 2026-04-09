from __future__ import annotations

from research.benchmark_suite import BenchmarkSuiteReport


def _format_float(value: float | None, digits: int = 6) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def render_suite_markdown(report: BenchmarkSuiteReport) -> str:
    lines: list[str] = ["# Sports Benchmark Suite Summary", ""]
    aggregate = report.aggregate
    lines.extend(
        [
            "## Aggregate metrics",
            "",
            f"- Total cases: {aggregate.total_cases}",
            f"- Successful cases: {aggregate.successful_cases}",
            f"- Failed cases: {aggregate.failed_cases}",
            f"- Fair-value cases: {aggregate.fair_value_case_count}",
            f"- Replay cases: {aggregate.replay_case_count}",
            f"- Average Brier score: {_format_float(aggregate.average_brier_score)}",
            f"- Average log loss: {_format_float(aggregate.average_log_loss)}",
            f"- Average accuracy: {_format_float(aggregate.average_accuracy)}",
            f"- Average ECE: {_format_float(aggregate.average_expected_calibration_error)}",
            f"- Calibrated fair-value cases: {aggregate.calibrated_case_count}",
            f"- Average calibrated Brier score: {_format_float(aggregate.average_calibrated_brier_score)}",
            f"- Average calibrated log loss: {_format_float(aggregate.average_calibrated_log_loss)}",
            f"- Average calibrated accuracy: {_format_float(aggregate.average_calibrated_accuracy)}",
            f"- Average calibrated ECE: {_format_float(aggregate.average_calibrated_expected_calibration_error)}",
            f"- Average calibrated Brier improvement: {_format_float(aggregate.average_calibrated_brier_improvement)}",
            f"- Average calibrated log loss improvement: {_format_float(aggregate.average_calibrated_log_loss_improvement)}",
            f"- Average calibrated accuracy delta: {_format_float(aggregate.average_calibrated_accuracy_delta)}",
            f"- Average calibrated ECE improvement: {_format_float(aggregate.average_calibrated_expected_calibration_error_improvement)}",
            f"- Average replay net PnL: {_format_float(aggregate.average_replay_net_pnl, 4)}",
            f"- Average replay return %: {_format_float(aggregate.average_replay_return_pct, 4)}",
            f"- Edge ledger rows: {aggregate.edge_ledger_row_count}",
            "",
        ]
    )
    lines.extend(
        [
            "## Case table",
            "",
            "| Case | Fair-value Brier | Fair-value Log Loss | Replay Net PnL | Replay Return % |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for case in report.case_results:
        fair_value_score = (
            case.report.fair_value_report.forecast_score
            if case.report.fair_value_report is not None
            else None
        )
        calibrated_fair_value_score = (
            case.report.fair_value_report.calibrated_forecast_score
            if case.report.fair_value_report is not None
            else None
        )
        replay_score = (
            case.report.replay_report.score
            if case.report.replay_report is not None
            else None
        )
        lines.append(
            "| {case_name} | {brier} | {log_loss} | {net_pnl} | {return_pct} |".format(
                case_name=case.report.case_name,
                brier=_format_float(
                    fair_value_score.brier_score
                    if fair_value_score is not None
                    else None
                ),
                log_loss=_format_float(
                    fair_value_score.log_loss if fair_value_score is not None else None
                ),
                net_pnl=_format_float(
                    replay_score.net_pnl if replay_score is not None else None, 4
                ),
                return_pct=_format_float(
                    replay_score.return_pct if replay_score is not None else None, 4
                ),
            )
        )
    lines.append("")

    calibrated_cases = [
        case
        for case in report.case_results
        if case.report.fair_value_report is not None
        and case.report.fair_value_report.calibrated_forecast_score is not None
        and case.report.fair_value_report.calibration is not None
    ]
    if calibrated_cases:
        lines.extend(
            [
                "## Calibration deltas",
                "",
                "| Case | Raw Brier | Calibrated Brier | Brier Improvement | Raw Log Loss | Calibrated Log Loss | Log Loss Improvement | Raw ECE | Calibrated ECE | ECE Improvement |",
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for case in calibrated_cases:
            fair_value_report = case.report.fair_value_report
            if fair_value_report is None:
                continue
            raw_score = fair_value_report.forecast_score
            calibrated_score = fair_value_report.calibrated_forecast_score
            calibration = fair_value_report.calibration
            if (
                raw_score is None
                or calibrated_score is None
                or not isinstance(calibration, dict)
            ):
                continue
            metric_delta = calibration.get("metric_delta")
            lines.append(
                "| {case_name} | {raw_brier} | {calibrated_brier} | {brier_improvement} | {raw_log_loss} | {calibrated_log_loss} | {log_loss_improvement} | {raw_ece} | {calibrated_ece} | {ece_improvement} |".format(
                    case_name=case.report.case_name,
                    raw_brier=_format_float(raw_score.brier_score),
                    calibrated_brier=_format_float(calibrated_score.brier_score),
                    brier_improvement=_format_float(
                        metric_delta.get("brier_improvement")
                        if isinstance(metric_delta, dict)
                        else None
                    ),
                    raw_log_loss=_format_float(raw_score.log_loss),
                    calibrated_log_loss=_format_float(calibrated_score.log_loss),
                    log_loss_improvement=_format_float(
                        metric_delta.get("log_loss_improvement")
                        if isinstance(metric_delta, dict)
                        else None
                    ),
                    raw_ece=_format_float(raw_score.expected_calibration_error),
                    calibrated_ece=_format_float(
                        calibrated_score.expected_calibration_error
                    ),
                    ece_improvement=_format_float(
                        metric_delta.get("expected_calibration_error_improvement")
                        if isinstance(metric_delta, dict)
                        else None
                    ),
                )
            )
        lines.append("")

    if aggregate.fair_value_baseline_deltas:
        lines.extend(
            [
                "## Fair-value baseline deltas",
                "",
                "| Baseline | Case Count | Avg Brier Delta (primary - baseline) | Avg Log Loss Delta (primary - baseline) |",
                "|---|---:|---:|---:|",
            ]
        )
        for name, payload in sorted(aggregate.fair_value_baseline_deltas.items()):
            lines.append(
                f"| {name} | {payload['case_count']} | {_format_float(payload['average_brier_delta'])} | {_format_float(payload['average_log_loss_delta'])} |"
            )
        lines.append("")

    if aggregate.replay_baseline_deltas:
        lines.extend(
            [
                "## Replay baseline deltas",
                "",
                "| Baseline | Case Count | Avg Net PnL Delta (primary - baseline) | Avg Return % Delta (primary - baseline) |",
                "|---|---:|---:|---:|",
            ]
        )
        for name, payload in sorted(aggregate.replay_baseline_deltas.items()):
            lines.append(
                f"| {name} | {payload['case_count']} | {_format_float(payload['average_net_pnl_delta'], 4)} | {_format_float(payload['average_return_pct_delta'], 4)} |"
            )
        lines.append("")

    if report.failures:
        lines.extend(["## Failures", ""])
        for failure in report.failures:
            lines.append(f"- `{failure.case_path}` — {failure.error}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"
