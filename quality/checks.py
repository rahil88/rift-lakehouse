"""Quality checks as pure functions: numbers in, a CheckResult out. No I/O, so every threshold
and edge case is unit-tested. ``quality.runner`` fetches the numbers from Databricks.

Status levels: ``pass`` | ``warn`` | ``fail`` | ``insufficient_data``. Only ``fail`` fails the
Airflow task (the alert). ``insufficient_data`` is reported instead of a fake pass when there
isn't enough history to judge, e.g. the first days of the project.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

PASS, WARN, FAIL, INSUFFICIENT = "pass", "warn", "fail", "insufficient_data"


@dataclass(frozen=True)
class CheckResult:
    check_name: str
    status: str
    metric_value: float | None
    threshold: float | None
    details: dict[str, Any] = field(default_factory=dict)


def freshness(last_updated_at: datetime | None, now: datetime, slo_hours: float) -> CheckResult:
    """SLO: the gold fact table was rewritten within ``slo_hours``."""
    if last_updated_at is None:
        return CheckResult("freshness_slo", FAIL, None, slo_hours, {"reason": "gold never built"})
    age_h = (now - last_updated_at).total_seconds() / 3600
    status = PASS if age_h <= slo_hours else FAIL
    return CheckResult(
        "freshness_slo",
        status,
        round(age_h, 2),
        slo_hours,
        {"last_updated_at": last_updated_at.isoformat()},
    )


def volume_zscore(
    daily_counts: Mapping[date, int],
    target: date,
    *,
    baseline_days: int = 14,
    min_history: int = 7,
    warn_z: float = 2.0,
    fail_z: float = 3.0,
) -> CheckResult:
    """Row volume for ``target`` vs the trailing ``baseline_days`` (z-score).

    Days before the first day with any data are not history yet (project start); days after it
    with no rows count as 0, because a silent zero is exactly what this check should catch.
    """
    name = "volume_zscore"
    observed = int(daily_counts.get(target, 0))
    if not daily_counts:
        return CheckResult(name, INSUFFICIENT, observed, fail_z, {"reason": "no data"})
    first_day = min(daily_counts)
    baseline = [
        int(daily_counts.get(d, 0))
        for offset in range(baseline_days, 0, -1)
        if (d := date.fromordinal(target.toordinal() - offset)) >= first_day
    ]
    details: dict[str, Any] = {"observed": observed, "baseline_days": len(baseline)}
    if len(baseline) < min_history:
        details["reason"] = f"need {min_history} baseline days, have {len(baseline)}"
        return CheckResult(name, INSUFFICIENT, None, fail_z, details)
    mean = statistics.fmean(baseline)
    std = statistics.pstdev(baseline)
    details.update(mean=round(mean, 2), std=round(std, 2))
    if std == 0:
        z = 0.0 if observed == mean else math.copysign(math.inf, observed - mean)
    else:
        z = (observed - mean) / std
    status = FAIL if abs(z) > fail_z else WARN if abs(z) > warn_z else PASS
    metric = None if math.isinf(z) else round(z, 3)
    return CheckResult(name, status, metric, fail_z, details)


def psi(expected: Sequence[int], actual: Sequence[int], *, epsilon: float = 1e-4) -> float:
    """Population Stability Index between two bucketed distributions (same bucket order).

    PSI = sum((a_i - e_i) * ln(a_i / e_i)) over bucket shares. Empty buckets get ``epsilon``
    so the log is defined. Rule of thumb: < 0.1 stable, 0.1-0.25 moderate shift, > 0.25 major.
    """
    if len(expected) != len(actual):
        raise ValueError("bucket lists must have the same length")
    e_total, a_total = sum(expected), sum(actual)
    if e_total == 0 or a_total == 0:
        raise ValueError("both distributions need at least one observation")
    total = 0.0
    for e, a in zip(expected, actual, strict=True):
        e_share = max(e / e_total, epsilon)
        a_share = max(a / a_total, epsilon)
        total += (a_share - e_share) * math.log(a_share / e_share)
    return total


def duration_drift(
    baseline_buckets: Sequence[int],
    target_buckets: Sequence[int],
    *,
    min_rows: int = 30,
    warn_psi: float = 0.1,
    fail_psi: float = 0.25,
) -> CheckResult:
    """Distribution drift of game duration (5-minute buckets), target day vs trailing baseline.

    Game length is a good canary: a patch that changes game pace shows up here, and so does a
    parsing bug (e.g. ms read as seconds puts everything in the last bucket).
    """
    name = "duration_drift_psi"
    details: dict[str, Any] = {
        "baseline_rows": sum(baseline_buckets),
        "target_rows": sum(target_buckets),
    }
    if sum(baseline_buckets) < min_rows or sum(target_buckets) < min_rows:
        details["reason"] = f"need {min_rows} rows on each side"
        return CheckResult(name, INSUFFICIENT, None, fail_psi, details)
    value = psi(baseline_buckets, target_buckets)
    status = FAIL if value > fail_psi else WARN if value > warn_psi else PASS
    return CheckResult(name, status, round(value, 4), fail_psi, details)


def dlq_rate(valid: int | None, rejected: int | None, threshold: float) -> CheckResult:
    """Share of events the contract rejected for the day's collection run(s)."""
    name = "dlq_rejection_rate"
    if valid is None and rejected is None:
        return CheckResult(name, INSUFFICIENT, None, threshold, {"reason": "no producer run"})
    valid, rejected = int(valid or 0), int(rejected or 0)
    total = valid + rejected
    rate = rejected / total if total else 0.0
    status = FAIL if rate > threshold else PASS
    return CheckResult(
        name, status, round(rate, 6), threshold, {"valid": valid, "rejected": rejected}
    )
