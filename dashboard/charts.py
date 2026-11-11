"""Pure DataFrame -> Altair chart builders (unit-tested without a warehouse)."""

from __future__ import annotations

import altair as alt
import pandas as pd

STATUS_COLORS = {
    "pass": "#2e7d32",
    "warn": "#f9a825",
    "fail": "#c62828",
    "insufficient_data": "#9e9e9e",
}


def winrate_chart(df: pd.DataFrame, *, top_n: int = 40) -> alt.Chart:
    """Win rate per champion as a point with its 95% Wilson interval as an error bar.

    Sorted by the *lower* bound: champions are ranked by how confident we are that they win,
    so a 3-0 champion with a [0.44, 1.00] interval doesn't top the chart.
    """
    data = df.sort_values("win_rate_ci_lower", ascending=False).head(top_n)
    order = data["champion_name"].tolist()
    y = alt.Y("champion_name:N", sort=order, title=None)
    x_scale = alt.Scale(domain=[0, 1])
    tooltip = [
        alt.Tooltip("champion_name:N", title="Champion"),
        alt.Tooltip("games:Q", title="Games"),
        alt.Tooltip("wins:Q", title="Wins"),
        alt.Tooltip("win_rate:Q", title="Win rate", format=".1%"),
        alt.Tooltip("win_rate_ci_lower:Q", title="95% CI low", format=".1%"),
        alt.Tooltip("win_rate_ci_upper:Q", title="95% CI high", format=".1%"),
    ]
    bars = (
        alt.Chart(data)
        .mark_rule(strokeWidth=2, opacity=0.6)
        .encode(
            y=y,
            x=alt.X("win_rate_ci_lower:Q", scale=x_scale, title="Win rate (95% Wilson CI)"),
            x2="win_rate_ci_upper:Q",
            tooltip=tooltip,
        )
    )
    points = (
        alt.Chart(data)
        .mark_point(filled=True, size=60)
        .encode(
            y=y,
            x=alt.X("win_rate:Q", scale=x_scale, axis=alt.Axis(format="%")),
            size=alt.Size("games:Q", title="Games", scale=alt.Scale(range=[30, 300])),
            tooltip=tooltip,
        )
    )
    even = alt.Chart(pd.DataFrame({"x": [0.5]})).mark_rule(strokeDash=[4, 4]).encode(x="x:Q")
    return (bars + points + even).properties(height=max(200, 18 * len(data)))


def daily_volume_chart(df: pd.DataFrame) -> alt.Chart:
    """Matches per game day (input columns: game_date, matches)."""
    return (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("game_date:T", title="Game day (UTC)"),
            y=alt.Y("matches:Q", title="Matches"),
            tooltip=["game_date:T", "matches:Q"],
        )
        .properties(height=220)
    )


def quality_status_chart(df: pd.DataFrame) -> alt.Chart:
    """Grid of check status per data day (input columns: target_date, check_name, status)."""
    return (
        alt.Chart(df)
        .mark_rect(stroke="white")
        .encode(
            x=alt.X("target_date:T", title="Data day"),
            y=alt.Y("check_name:N", title=None),
            color=alt.Color(
                "status:N",
                scale=alt.Scale(domain=list(STATUS_COLORS), range=list(STATUS_COLORS.values())),
            ),
            tooltip=["check_name:N", "target_date:T", "status:N", "metric_value:Q"],
        )
        .properties(height=160)
    )
