import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="FleetLake", page_icon="🚚", layout="wide")

BASE = os.path.join(os.path.dirname(__file__), "..", "data_lake", "curated")


@st.cache_data
def load_table(name: str) -> pd.DataFrame:
    con = duckdb.connect()
    glob_path = os.path.join(BASE, name, "dt=*", "*.parquet").replace("\\", "/")
    df = con.execute(f"SELECT * FROM read_parquet('{glob_path}', hive_partitioning=1)").fetchdf()
    con.close()
    df["dt"] = pd.to_datetime(df["dt"])
    return df


st.title("FleetLake : Fleet & Delivery Analytics")
st.caption(
    "A medallion-architecture data lake (raw → staging → curated) over synthetic "
    "vehicle telemetry, driver shifts, and delivery events. Every chart below reads "
    "directly from the curated, partitioned Parquet layer — the same tables Athena "
    "or BigQuery would query in the cloud deployment."
)

fleet_util = load_table("daily_fleet_utilization")
driver_perf = load_table("driver_performance")
sla = load_table("delivery_sla_compliance")

min_dt, max_dt = fleet_util["dt"].min(), fleet_util["dt"].max()
date_range = st.slider(
    "Date range",
    min_value=min_dt.to_pydatetime(),
    max_value=max_dt.to_pydatetime(),
    value=(min_dt.to_pydatetime(), max_dt.to_pydatetime()),
    format="YYYY-MM-DD",
)
mask = lambda df: (df["dt"] >= date_range[0]) & (df["dt"] <= date_range[1])
fleet_f, driver_f, sla_f = fleet_util[mask(fleet_util)], driver_perf[mask(driver_perf)], sla[mask(sla)]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Avg fleet active time", f"{fleet_f['active_time_pct'].mean():.1f}%")
col2.metric("Total distance driven", f"{fleet_f['distance_km'].sum():,.0f} km")
col3.metric("Avg SLA compliance", f"{sla_f['sla_compliance_pct'].mean():.1f}%")
col4.metric("Total deliveries", f"{sla_f['total_deliveries'].sum():,.0f}")

st.divider()

tab1, tab2, tab3 = st.tabs(["Fleet Utilization", "Driver Performance", "Delivery SLA"])

with tab1:
    st.subheader("Daily fleet active-time %")
    daily_avg = fleet_f.groupby("dt", as_index=False)["active_time_pct"].mean()
    st.plotly_chart(px.line(daily_avg, x="dt", y="active_time_pct", markers=True), use_container_width=True)

    st.subheader("Most underutilized vehicles (avg active-time %)")
    worst = (
        fleet_f.groupby("vehicle_id", as_index=False)["active_time_pct"]
        .mean()
        .sort_values("active_time_pct")
        .head(10)
    )
    st.plotly_chart(px.bar(worst, x="vehicle_id", y="active_time_pct"), use_container_width=True)

with tab2:
    st.subheader("Driver on-time rate leaderboard")
    leaderboard = (
        driver_f.groupby("driver_id", as_index=False)
        .agg(deliveries_completed=("deliveries_completed", "sum"),
             on_time_deliveries=("on_time_deliveries", "sum"))
    )
    leaderboard = leaderboard[leaderboard["deliveries_completed"] >= 5]
    leaderboard["on_time_rate_pct"] = (
        leaderboard["on_time_deliveries"] * 100 / leaderboard["deliveries_completed"]
    ).round(1)
    leaderboard = leaderboard.sort_values("on_time_rate_pct", ascending=False).head(10)
    st.plotly_chart(px.bar(leaderboard, x="driver_id", y="on_time_rate_pct"), use_container_width=True)
    st.dataframe(leaderboard, use_container_width=True, hide_index=True)

with tab3:
    st.subheader("SLA compliance % by city over time")
    st.plotly_chart(
        px.line(sla_f.sort_values("dt"), x="dt", y="sla_compliance_pct", color="city", markers=True),
        use_container_width=True,
    )

    st.subheader("Worst breach days")
    worst_sla = sla_f[sla_f["breached_deliveries"] > 0].sort_values("breached_deliveries", ascending=False).head(10)
    st.dataframe(
        worst_sla[["dt", "city", "breached_deliveries", "avg_breach_overage_minutes", "sla_compliance_pct"]],
        use_container_width=True, hide_index=True,
    )

st.divider()
st.caption(
    "Architecture: raw JSON (bronze) → cleaned/deduped partitioned Parquet (silver) → "
    "aggregated analysis-ready Parquet (gold), queried here via DuckDB. In the cloud "
    "deployment the same curated tables sit in S3 + Glue Data Catalog and are queried "
    "with Athena SQL — see the project README for the full architecture and a measured "
    "before/after query-cost comparison."
)
