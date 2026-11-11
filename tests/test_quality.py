import math
from datetime import UTC, date, datetime, timedelta

import pytest

from quality.checks import (
    FAIL,
    INSUFFICIENT,
    PASS,
    WARN,
    CheckResult,
    dlq_rate,
    duration_drift,
    freshness,
    psi,
    volume_zscore,
)
from quality.runner import evaluate, merge_statement

NOW = datetime(2026, 11, 12, 6, tzinfo=UTC)
TARGET = date(2026, 11, 11)


def days_before(target, values):
    """{target-n: v_0, ..., target-1: v_n-1}"""
    n = len(values)
    return {target - timedelta(days=n - i): v for i, v in enumerate(values)}


# freshness -------------------------------------------------------------------
def test_freshness_pass_and_fail():
    assert freshness(NOW - timedelta(hours=5), NOW, 26).status == PASS
    late = freshness(NOW - timedelta(hours=30), NOW, 26)
    assert late.status == FAIL and late.metric_value == 30.0
    assert freshness(None, NOW, 26).status == FAIL


# volume z-score --------------------------------------------------------------
def test_volume_normal_day_passes():
    counts = days_before(TARGET, [100, 104, 96, 101, 99, 103, 97, 100, 102, 98, 100, 101, 99, 100])
    counts[TARGET] = 101
    result = volume_zscore(counts, TARGET)
    assert result.status == PASS and abs(result.metric_value) < 1


def test_volume_collapse_fails():
    counts = days_before(TARGET, [100, 104, 96, 101, 99, 103, 97, 100, 102, 98, 100, 101, 99, 100])
    counts[TARGET] = 3  # the crawler broke
    result = volume_zscore(counts, TARGET)
    assert result.status == FAIL and result.metric_value < -3


def test_volume_missing_target_day_counts_as_zero():
    counts = days_before(TARGET, [50] * 7 + [48, 52] * 3 + [50])
    assert volume_zscore(counts, TARGET).status == FAIL


def test_volume_needs_history_before_judging():
    counts = days_before(TARGET, [100, 100, 100])
    counts[TARGET] = 0
    assert volume_zscore(counts, TARGET).status == INSUFFICIENT


def test_volume_flat_baseline_handles_zero_std():
    counts = days_before(TARGET, [10] * 14)
    counts[TARGET] = 10
    assert volume_zscore(counts, TARGET).status == PASS
    counts[TARGET] = 11
    result = volume_zscore(counts, TARGET)
    assert result.status == FAIL and result.metric_value is None  # infinite z


# drift -----------------------------------------------------------------------
def test_psi_identical_is_zero_and_known_value():
    assert psi([10, 20, 30], [1, 2, 3]) == pytest.approx(0.0)
    # shares (0.5, 0.5) -> (0.9, 0.1): 0.4*ln(1.8) + (-0.4)*ln(0.2)
    assert psi([50, 50], [90, 10]) == pytest.approx(0.4 * math.log(1.8) - 0.4 * math.log(0.2))


def test_duration_drift_levels():
    baseline = [0, 5, 60, 120, 140, 90, 40, 15, 5, 2]
    assert duration_drift(baseline, [0, 1, 6, 12, 14, 9, 4, 2, 1, 0]).status == PASS
    # a parsing bug (ms read as s) piles every game into the last bucket
    assert duration_drift(baseline, [0] * 9 + [50]).status == FAIL


def test_duration_drift_needs_rows():
    assert duration_drift([5] * 10, [1] * 10).status == INSUFFICIENT


# DLQ rate --------------------------------------------------------------------
def test_dlq_rate_threshold():
    assert dlq_rate(990, 5, 0.01).status == PASS
    high = dlq_rate(900, 100, 0.01)
    assert high.status == FAIL and high.metric_value == 0.1
    assert dlq_rate(None, None, 0.01).status == INSUFFICIENT


# runner ----------------------------------------------------------------------
def test_merge_statement_is_keyed_and_escapes_literals():
    results = [CheckResult("x", WARN, 1.5, 2.0, {"note": "it's a \\ test"})]
    sql = merge_statement("workspace", results, TARGET, NOW, "run-1")
    assert "MERGE INTO workspace.rift_quality.quality_results" in sql
    assert "ON t.check_name = s.check_name AND t.target_date = s.target_date" in sql
    assert "DATE'2026-11-11'" in sql
    assert "it\\'s a \\\\\\\\ test" in sql  # json-escaped backslash, then SQL-escaped


def test_evaluate_routes_queries_to_checks():
    def fake_sql(sql):
        if sql.startswith("DESCRIBE HISTORY"):
            return [{"timestamp": "2026-11-12T05:00:00.000Z"}]
        if "min(to_date" in sql:
            return [{"d": "2026-10-20"}]
        if "AS grp" in sql:  # drift (check first: it also contains "count(*) AS n")
            return [{"grp": g, "bucket": "5.0", "n": "40"} for g in ("target", "baseline")]
        if "count(*) AS n FROM" in sql:
            return [{"d": str(TARGET - timedelta(days=i)), "n": "100"} for i in range(15)]
        if "producer_metrics" in sql:
            return [{"valid": "1000", "rejected": "0"}]
        raise AssertionError(sql)

    results = {
        r.check_name: r
        for r in evaluate(
            fake_sql, "workspace", TARGET, now=NOW, freshness_slo_hours=26, dlq_rate_threshold=0.01
        )
    }
    assert {name: r.status for name, r in results.items()} == {
        "freshness_slo": PASS,
        "volume_zscore": PASS,
        "duration_drift_psi": PASS,
        "dlq_rejection_rate": PASS,
    }
    assert results["freshness_slo"].metric_value == 1.0
