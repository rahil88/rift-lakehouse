import pandas as pd

from dashboard.charts import daily_volume_chart, quality_status_chart, winrate_chart


def winrates():
    return pd.DataFrame(
        {
            "champion_name": ["Ahri", "Jinx", "Zed"],
            "games": [3, 120, 60],
            "wins": [3, 66, 27],
            "win_rate": [1.0, 0.55, 0.45],
            "win_rate_ci_lower": [0.4385, 0.4609, 0.3302],
            "win_rate_ci_upper": [1.0, 0.6357, 0.5741],
        }
    )


def test_winrate_chart_sorts_by_lower_bound_not_raw_win_rate():
    spec = winrate_chart(winrates()).to_dict()
    sort = spec["layer"][0]["encoding"]["y"]["sort"]
    assert sort == ["Jinx", "Ahri", "Zed"]  # 3-0 Ahri does not top the chart
    assert spec["layer"][0]["encoding"]["x2"]["field"] == "win_rate_ci_upper"


def test_other_charts_build():
    vol = pd.DataFrame({"game_date": pd.to_datetime(["2026-11-10"]), "matches": [20]})
    assert daily_volume_chart(vol).to_dict()["mark"]["type"] == "bar"
    checks = pd.DataFrame(
        {
            "target_date": pd.to_datetime(["2026-11-10"]),
            "check_name": ["freshness_slo"],
            "status": ["pass"],
            "metric_value": [1.0],
        }
    )
    assert quality_status_chart(checks).to_dict()["mark"]["type"] == "rect"
