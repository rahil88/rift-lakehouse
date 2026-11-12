"""Rift Lakehouse dashboard. Run with ``make dashboard`` (reads .env for Databricks access).

Two tabs:
* Champion win rates: by patch and role, with sample size and 95% Wilson intervals
* Pipeline health: quality check results, data volume, DLQ rejection rate, freshness
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from databricks import sql

from dashboard.charts import daily_volume_chart, quality_status_chart, winrate_chart

CATALOG = os.environ.get("DATABRICKS_CATALOG", "workspace")
GOLD, QUALITY, BRONZE = f"{CATALOG}.rift_gold", f"{CATALOG}.rift_quality", f"{CATALOG}.rift_bronze"

st.set_page_config(page_title="Rift Lakehouse", layout="wide")


@st.cache_resource
def _connection():  # noqa: ANN202
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


@st.cache_data(ttl=600, show_spinner="Querying the SQL warehouse...")
def query(statement: str) -> pd.DataFrame:
    with _connection().cursor() as cursor:
        cursor.execute(statement)
        return cursor.fetchall_arrow().to_pandas()


st.title("Rift Lakehouse")
st.caption(
    "Ranked solo (NA) matches from the Riot API, validated against a data contract before "
    "Kafka, modeled in Databricks with dbt."
)
tab_winrates, tab_health = st.tabs(["Champion win rates", "Pipeline health"])

with tab_winrates:
    patches = query(f"SELECT patch FROM {GOLD}.dim_patch ORDER BY patch_sort_key DESC")
    if patches.empty:
        st.info("No data in gold yet. Run the pipeline first.")
    else:
        c1, c2, c3 = st.columns(3)
        patch = c1.selectbox("Patch", patches["patch"].tolist())
        role = c2.selectbox("Role", ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"])
        min_games = c3.slider("Minimum games", 1, 100, 20)
        df = query(
            f"SELECT * FROM {GOLD}.mart_champion_winrate_by_patch_role "
            f"WHERE patch = '{patch}' AND role = '{role}'"  # values come from fixed lists
        )
        shown = df[df["games"] >= min_games]
        st.write(
            f"**{len(shown)}** champions with at least {min_games} games "
            f"({int(df['games'].sum())} {role} games on patch {patch}). "
            "Sorted by the lower bound of the 95% Wilson interval."
        )
        if shown.empty:
            st.warning("No champion meets the minimum; lower it to see small samples.")
        else:
            st.altair_chart(winrate_chart(shown), width="stretch")
            st.dataframe(
                shown.sort_values("win_rate_ci_lower", ascending=False)[
                    [
                        "champion_name",
                        "games",
                        "wins",
                        "win_rate",
                        "win_rate_ci_lower",
                        "win_rate_ci_upper",
                        "pick_share_in_role",
                    ]
                ],
                hide_index=True,
            )

with tab_health:
    checks = query(
        f"SELECT check_name, target_date, status, metric_value, threshold, details, checked_at "
        f"FROM {QUALITY}.quality_results WHERE target_date >= date_sub(current_date(), 14)"
    )
    if checks.empty:
        st.info("No quality results yet.")
    else:
        latest = checks.sort_values("target_date").groupby("check_name").tail(1)
        cols = st.columns(len(latest))
        for col, (_, row) in zip(cols, latest.iterrows(), strict=True):
            value = "-" if pd.isna(row["metric_value"]) else f"{row['metric_value']:.4g}"
            col.metric(f"{row['check_name']} · {row['status']}", value)
        st.altair_chart(quality_status_chart(checks), width="stretch")

    volume = query(
        f"SELECT game_date, count(DISTINCT match_id) AS matches FROM {GOLD}.fct_participant_match "
        "GROUP BY game_date ORDER BY game_date"
    )
    st.subheader("Matches per game day")
    st.altair_chart(daily_volume_chart(volume), width="stretch")

    st.subheader("Producer runs (contract validation)")
    st.dataframe(
        query(
            f"SELECT run_id, window_start, window_end, sum(valid) AS valid, "
            f"sum(rejected) AS rejected, round(sum(rejected) / sum(valid + rejected), 6) AS "
            f"rejection_rate FROM {BRONZE}.producer_metrics GROUP BY 1, 2, 3 "
            f"ORDER BY run_id DESC LIMIT 20"
        ),
        hide_index=True,
    )
