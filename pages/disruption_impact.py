"""
======================
Disruption impact page
======================

Author: github.com/cyrilby
Last meaningful update: 14-10-2026
"""

# %% Setting up

import numpy as np
import plotly.express as px
import streamlit as st

from core.helpers import load_text, plot_or_not
from core.loader import get_filtered_data, set_page_filters
from core.storage import get_app_data

# Importing data and text strings used in the app
text = load_text()
conn = get_app_data()


# %% Common for all data-driven pages

# Page title displayed on top
st.header("Disruption impact")

# Source & filter operations data, deal with warnings, etc.
page_filters = set_page_filters()
data = get_filtered_data(conn, page_filters)

min_date = data.min_date
max_date = data.max_date
n_days = data.n_days
downtime_msg = data.downtime_msg
selected_period = data.selected_period
df = data.df

# Sourcing additional data on station impact
station_data = get_filtered_data(conn, page_filters, table_name="station_impact")
station_df = station_data.df

# Confirming the selected period
st.sidebar.markdown(
    f"**Note:** this selection covers the period between {selected_period}."
    + downtime_msg
)

# %% Specific page content

# =================
# DAILY MAINTENANCE
# =================

# Preparing data on the daily likelihood of maintenance
vars_for_group = ["status_en_short", "weekday"]
by_day_chance_mntn = df.copy()
by_day_chance_mntn["rows_for_status"] = by_day_chance_mntn.groupby(vars_for_group)[
    "date"
].transform("count")
by_day_chance_mntn["rows_for_day"] = by_day_chance_mntn.groupby("weekday")[
    "date"
].transform("count")
by_day_chance_mntn["status_pct"] = 100 * (
    by_day_chance_mntn["rows_for_status"] / by_day_chance_mntn["rows_for_day"]
)
by_day_chance_mntn["status_pct"] = np.round(by_day_chance_mntn["status_pct"], 1)
by_day_chance_mntn = by_day_chance_mntn.drop_duplicates(subset=vars_for_group)
by_day_chance_mntn = by_day_chance_mntn[
    by_day_chance_mntn["status_en_short"] == "Closed for maintenance"
].copy()
by_day_chance_mntn = by_day_chance_mntn.sort_values("weekday_n")
by_day_chance_mntn = by_day_chance_mntn.reset_index(drop=True)

# Creating a bar chart with the daily likelihood of maintenance
chart_by_day_chance_mntn = px.bar(
    by_day_chance_mntn,
    x="weekday",
    y="status_pct",
)
chart_by_day_chance_mntn.update_layout(
    title_text=f"Chance (%) of planned maintenance between {selected_period} by weekday"
)
chart_by_day_chance_mntn.update_xaxes(title_text="Weekday")
chart_by_day_chance_mntn.update_yaxes(title_text="% chance of disruption")

# Preparing data on the daily distribution of maintenance
vars_for_group = ["weekday"]
by_day_dist_mntn = df[df["status_en"] == "Closed for maintenance"].copy()
by_day_dist_mntn["rows_for_status"] = by_day_dist_mntn.groupby(vars_for_group)[
    "date"
].transform("count")
by_day_dist_mntn["status_pct"] = 100 * (
    by_day_dist_mntn["rows_for_status"] / len(by_day_dist_mntn)
)
by_day_dist_mntn["status_pct"] = np.round(by_day_dist_mntn["status_pct"], 1)
by_day_dist_mntn = by_day_dist_mntn.drop_duplicates(vars_for_group)
by_day_dist_mntn = by_day_dist_mntn.sort_values("rows_for_status")
by_day_dist_mntn = by_day_dist_mntn.reset_index(drop=True)

# Creating a doughnut chart with maintenance split by day
chart_by_day_dist_mntn = px.pie(
    by_day_dist_mntn, values="status_pct", names="weekday", hole=0.45
)
chart_by_day_dist_mntn.update_layout(
    title_text=f"Metro planned maintenance during the last {n_days} days split by weekday",
    legend_title="",
)

# =================
# DAILY DISRUPTIONS
# =================

# Preparing data on the daily likelihood of disruptions
vars_for_group = ["status_en_short", "weekday"]
by_day_chance_dsrpt = df.copy()
by_day_chance_dsrpt["rows_for_status"] = by_day_chance_dsrpt.groupby(vars_for_group)[
    "date"
].transform("count")
by_day_chance_dsrpt["rows_for_day"] = by_day_chance_dsrpt.groupby("weekday")[
    "date"
].transform("count")
by_day_chance_dsrpt["status_pct"] = 100 * (
    by_day_chance_dsrpt["rows_for_status"] / by_day_chance_dsrpt["rows_for_day"]
)
by_day_chance_dsrpt["status_pct"] = np.round(by_day_chance_dsrpt["status_pct"], 1)
by_day_chance_dsrpt = by_day_chance_dsrpt.drop_duplicates(vars_for_group)
by_day_chance_dsrpt = by_day_chance_dsrpt[
    by_day_chance_dsrpt["status_en_short"] == "Disruption"
].copy()
by_day_chance_dsrpt = by_day_chance_dsrpt.sort_values("weekday_n")
by_day_chance_dsrpt = by_day_chance_dsrpt.reset_index(drop=True)

# Preparing data on the daily distribution of disruptions
vars_for_group = ["weekday"]
ignore_these = ["Unknown", "Normal service", "Closed for maintenance"]
by_day_dist_dsrpt = df[~df["status_en_short"].isin(ignore_these)].copy()
by_day_dist_dsrpt["rows_for_status"] = by_day_dist_dsrpt.groupby(vars_for_group)[
    "date"
].transform("count")
by_day_dist_dsrpt["status_pct"] = 100 * (
    by_day_dist_dsrpt["rows_for_status"] / len(by_day_dist_dsrpt)
)
by_day_dist_dsrpt["status_pct"] = np.round(by_day_dist_dsrpt["status_pct"], 1)
by_day_dist_dsrpt = by_day_dist_dsrpt.drop_duplicates(vars_for_group)
by_day_dist_dsrpt = by_day_dist_dsrpt.sort_values("rows_for_status")
by_day_dist_dsrpt = by_day_dist_dsrpt.reset_index(drop=True)

# Creating a bar chart with the daily likelihood of unplanned disruptions
chart_by_day_chance_dsrpt = px.bar(
    by_day_chance_dsrpt,
    x="weekday",
    y="status_pct",
)
chart_by_day_chance_dsrpt.update_layout(
    title_text=f"Chance (%) of unplanned disruptions between {selected_period} by weekday"
)
chart_by_day_chance_dsrpt.update_xaxes(title_text="Weekday")
chart_by_day_chance_dsrpt.update_yaxes(title_text="% chance of disruption")

# Creating a doughnut chart with disruptions split by day
chart_by_day_dist_dsrpt = px.pie(
    by_day_dist_dsrpt, values="status_pct", names="weekday", hole=0.45
)
chart_by_day_dist_dsrpt.update_layout(
    title_text=f"Metro unplanned disruptions during the last {n_days} days split by weekday",
    legend_title="",
)

# ==================
# HOURLY MAINTENANCE
# ==================

# Preparing data on the hourly likelihood of maintenance
vars_for_group = ["status_en_short", "time_period"]
by_period_chance_mntn = df.copy()

by_period_chance_mntn["rows_for_status"] = by_period_chance_mntn.groupby(
    vars_for_group
)["date"].transform("count")
by_period_chance_mntn["rows_for_period"] = by_period_chance_mntn.groupby("time_period")[
    "date"
].transform("count")
by_period_chance_mntn["status_pct"] = 100 * (
    by_period_chance_mntn["rows_for_status"] / by_period_chance_mntn["rows_for_period"]
)
by_period_chance_mntn["status_pct"] = np.round(by_period_chance_mntn["status_pct"], 1)
by_period_chance_mntn = by_period_chance_mntn.drop_duplicates(subset=vars_for_group)
by_period_chance_mntn = by_period_chance_mntn[
    by_period_chance_mntn["status_en_short"] == "Closed for maintenance"
].copy()
by_period_chance_mntn = by_period_chance_mntn.sort_values(
    "time_period", ascending=False
)
by_period_chance_mntn = by_period_chance_mntn.reset_index(drop=True)

# Preparing data on the hourly distribution of maintenance
# (as split by a more detailed definition of day times)
vars_for_group = ["time_period"]
by_period_dist_mntn = df[df["status_en"] == "Closed for maintenance"].copy()
by_period_dist_mntn["rows_for_status"] = by_period_dist_mntn.groupby(vars_for_group)[
    "date"
].transform("count")
by_period_dist_mntn["status_pct"] = 100 * (
    by_period_dist_mntn["rows_for_status"] / len(by_period_dist_mntn)
)
by_period_dist_mntn["status_pct"] = np.round(by_period_dist_mntn["status_pct"], 1)
by_period_dist_mntn = by_period_dist_mntn.drop_duplicates(vars_for_group)
by_period_dist_mntn = by_period_dist_mntn.sort_values("rows_for_status")
by_period_dist_mntn = by_period_dist_mntn.reset_index(drop=True)

# Creating a bar chart with the hourly likelihood of maintenance
chart_by_period_chance_mntn = px.bar(
    by_period_chance_mntn,
    y="time_period",
    x="status_pct",
)
chart_by_period_chance_mntn.update_layout(
    title_text=f"Chance (%) of planned maintenance between {selected_period} by time period"
)
chart_by_period_chance_mntn.update_yaxes(title_text="Time period")
chart_by_period_chance_mntn.update_xaxes(title_text="% chance of disruption")

# Creating a doughnut chart with maintenance split by hour
chart_by_period_dist_mntn = px.pie(
    by_period_dist_mntn, values="status_pct", names="time_period", hole=0.45
)
chart_by_period_dist_mntn.update_layout(
    title_text=f"Metro planned maintenance during the last {n_days} days split by time of day",
    legend_title="",
)

# ==================
# HOURLY DISRUPTIONS
# ==================

# Preparing data on the hourly likelihood of disruptions
vars_for_group = ["status_en_short", "time_period"]
by_period_chance_dsrpt = df.copy()
by_period_chance_dsrpt["rows_for_status"] = by_period_chance_dsrpt.groupby(
    vars_for_group
)["date"].transform("count")
by_period_chance_dsrpt["rows_for_period"] = by_period_chance_dsrpt.groupby(
    "time_period"
)["date"].transform("count")
by_period_chance_dsrpt["status_pct"] = 100 * (
    by_period_chance_dsrpt["rows_for_status"]
    / by_period_chance_dsrpt["rows_for_period"]
)
by_period_chance_dsrpt["status_pct"] = np.round(by_period_chance_dsrpt["status_pct"], 1)
by_period_chance_dsrpt = by_period_chance_dsrpt.drop_duplicates(vars_for_group)
by_period_chance_dsrpt = by_period_chance_dsrpt[
    by_period_chance_dsrpt["status_en_short"] == "Disruption"
].copy()
by_period_chance_dsrpt = by_period_chance_dsrpt.sort_values(
    "time_period", ascending=False
)
by_period_chance_dsrpt = by_period_chance_dsrpt.reset_index(drop=True)

# Preparing data on the hourly distribution of disruptions
# (as split by a more detailed definition of day times)
# (excluding planned maintenance)
vars_for_group = ["time_period"]
ignore_these = ["Unknown", "Normal service", "Closed for maintenance"]
by_period_dist_dsrpt = df[~df["status_en_short"].isin(ignore_these)].copy()
by_period_dist_dsrpt["rows_for_status"] = by_period_dist_dsrpt.groupby(vars_for_group)[
    "date"
].transform("count")
by_period_dist_dsrpt["status_pct"] = 100 * (
    by_period_dist_dsrpt["rows_for_status"] / len(by_period_dist_dsrpt)
)
by_period_dist_dsrpt["status_pct"] = np.round(by_period_dist_dsrpt["status_pct"], 1)
by_period_dist_dsrpt = by_period_dist_dsrpt.drop_duplicates(vars_for_group)
by_period_dist_dsrpt = by_period_dist_dsrpt.sort_values("rows_for_status")
by_period_dist_dsrpt = by_period_dist_dsrpt.reset_index(drop=True)

# Creating a bar chart with the hourly likelihood of disruptions
chart_by_period_chance_dsrpt = px.bar(
    by_period_chance_dsrpt,
    y="time_period",
    x="status_pct",
)
chart_by_period_chance_dsrpt.update_layout(
    title_text=f"Chance (%) of unplanned disruptions between {selected_period} by time period"
)
chart_by_period_chance_dsrpt.update_yaxes(title_text="Time period")
chart_by_period_chance_dsrpt.update_xaxes(title_text="% chance of disruption")

# Creating a doughnut chart with disruptions split by hour
chart_by_period_dist_dsprt = px.pie(
    by_period_dist_dsrpt, values="status_pct", names="time_period", hole=0.45
)
chart_by_period_dist_dsprt.update_layout(
    title_text=f"Metro service disruption during the last {n_days} days split by time of day",
    legend_title="",
)

# =====================
# RUSH HOUR MAINTENANCE
# =====================

# Preparing data on the rush-hour likelihood of maintenance
vars_for_group = ["status_en_short", "official_rush_hour"]
by_rush_chance_mntn = df.copy()

by_rush_chance_mntn["rows_for_status"] = by_rush_chance_mntn.groupby(vars_for_group)[
    "date"
].transform("count")
by_rush_chance_mntn["rows_for_period"] = by_rush_chance_mntn.groupby(
    "official_rush_hour"
)["date"].transform("count")
by_rush_chance_mntn["status_pct"] = 100 * (
    by_rush_chance_mntn["rows_for_status"] / by_rush_chance_mntn["rows_for_period"]
)
by_rush_chance_mntn["status_pct"] = np.round(by_rush_chance_mntn["status_pct"], 1)
by_rush_chance_mntn = by_rush_chance_mntn.drop_duplicates(subset=vars_for_group)
by_rush_chance_mntn = by_rush_chance_mntn[
    by_rush_chance_mntn["status_en_short"] == "Closed for maintenance"
].copy()
by_rush_chance_mntn = by_rush_chance_mntn.sort_values(
    "official_rush_hour", ascending=False
)
by_rush_chance_mntn = by_rush_chance_mntn.reset_index(drop=True)

# Preparing data on the rush-hour distr of maintenance
# (as split by a more detailed definition of day times)
vars_for_group = ["official_rush_hour"]
by_rush_dist_mntn = df[df["status_en"] == "Closed for maintenance"].copy()
by_rush_dist_mntn["rows_for_status"] = by_rush_dist_mntn.groupby(vars_for_group)[
    "date"
].transform("count")
by_rush_dist_mntn["status_pct"] = 100 * (
    by_rush_dist_mntn["rows_for_status"] / len(by_rush_dist_mntn)
)
by_rush_dist_mntn["status_pct"] = np.round(by_rush_dist_mntn["status_pct"], 1)
by_rush_dist_mntn = by_rush_dist_mntn.drop_duplicates(vars_for_group)
by_rush_dist_mntn = by_rush_dist_mntn.sort_values("rows_for_status")
by_rush_dist_mntn = by_rush_dist_mntn.reset_index(drop=True)

# Creating a bar chart with the likelihood of maintenance by rush hour
chart_by_rush_chance_mntn = px.bar(
    by_rush_chance_mntn,
    y="official_rush_hour",
    x="status_pct",
)
chart_by_rush_chance_mntn.update_layout(
    title_text=f"Chance (%) of planned maintenance between {selected_period} by rush hour type"
)
chart_by_rush_chance_mntn.update_yaxes(title_text="Time period")
chart_by_rush_chance_mntn.update_xaxes(title_text="% chance of disruption")

# Creating a doughnut chart with maintenance split by rush hpur
chart_by_rush_dist_mntn = px.pie(
    by_rush_dist_mntn, values="status_pct", names="official_rush_hour", hole=0.45
)
chart_by_rush_dist_mntn.update_layout(
    title_text=f"Metro planned maintenance during the last {n_days} days split by rush hour type",
    legend_title="",
)

# =====================
# RUSH HOUR DISRUPTIONS
# =====================

# Preparing data on the rush-hour likelihood of disruptions
vars_for_group = ["status_en_short", "official_rush_hour"]
by_rush_chance_dsrpt = df.copy()
by_rush_chance_dsrpt["rows_for_status"] = by_rush_chance_dsrpt.groupby(vars_for_group)[
    "date"
].transform("count")
by_rush_chance_dsrpt["rows_for_period"] = by_rush_chance_dsrpt.groupby(
    "official_rush_hour"
)["date"].transform("count")
by_rush_chance_dsrpt["status_pct"] = 100 * (
    by_rush_chance_dsrpt["rows_for_status"] / by_rush_chance_dsrpt["rows_for_period"]
)
by_rush_chance_dsrpt["status_pct"] = np.round(by_rush_chance_dsrpt["status_pct"], 1)
by_rush_chance_dsrpt = by_rush_chance_dsrpt.drop_duplicates(vars_for_group)
by_rush_chance_dsrpt = by_rush_chance_dsrpt[
    by_rush_chance_dsrpt["status_en_short"] == "Disruption"
].copy()
by_rush_chance_dsrpt = by_rush_chance_dsrpt.sort_values(
    "official_rush_hour", ascending=False
)
by_rush_chance_dsrpt = by_rush_chance_dsrpt.reset_index(drop=True)

# Preparing data on the rush-hour distr of disruptions
# (as split by a more detailed definition of day times)
# (excluding planned maintenance)
vars_for_group = ["official_rush_hour"]
ignore_these = ["Unknown", "Normal service", "Closed for maintenance"]
by_rush_dist_dsrpt = df[~df["status_en_short"].isin(ignore_these)].copy()
by_rush_dist_dsrpt["rows_for_status"] = by_rush_dist_dsrpt.groupby(vars_for_group)[
    "date"
].transform("count")
by_rush_dist_dsrpt["status_pct"] = 100 * (
    by_rush_dist_dsrpt["rows_for_status"] / len(by_rush_dist_dsrpt)
)
by_rush_dist_dsrpt["status_pct"] = np.round(by_rush_dist_dsrpt["status_pct"], 1)
by_rush_dist_dsrpt = by_rush_dist_dsrpt.drop_duplicates(vars_for_group)
by_rush_dist_dsrpt = by_rush_dist_dsrpt.sort_values("rows_for_status")
by_rush_dist_dsrpt = by_rush_dist_dsrpt.reset_index(drop=True)

# Creating a bar chart with the rush-hour likelihood of disruptions
chart_by_rush_chance_dsrpt = px.bar(
    by_rush_chance_dsrpt,
    y="official_rush_hour",
    x="status_pct",
)
chart_by_rush_chance_dsrpt.update_layout(
    title_text=f"Chance (%) of unplanned disruptions between {selected_period} by rush hour type"
)
chart_by_rush_chance_dsrpt.update_yaxes(title_text="Time period")
chart_by_rush_chance_dsrpt.update_xaxes(title_text="% chance of disruption")

# Creating a doughnut chart with disruptions split by rush hour
chart_by_rush_dist_dsprt = px.pie(
    by_rush_dist_dsrpt, values="status_pct", names="official_rush_hour", hole=0.45
)
chart_by_rush_dist_dsprt.update_layout(
    title_text=f"Metro service disruption during the last {n_days} days split by rush hour type",
    legend_title="",
)

# =================
# AFFECTED STATIONS
# =================

# Preparing data on the 10 most impacted stations
most_impacted = station_df[station_df["status_en_short"] == "Disruption"].copy()
most_impacted["n_times_affected"] = most_impacted.groupby("station")[
    "timestamp"
].transform("nunique")
most_impacted = most_impacted.drop_duplicates("station")
most_impacted = most_impacted.sort_values("n_times_affected", ascending=False)
most_impacted = most_impacted.iloc[:10]
most_impacted = most_impacted.sort_values("n_times_affected")

# Getting the name of the most impacted station
most_imp_station_name = most_impacted["station"].iloc[-1]

# Preparing data on the 10 least impacted stations
least_impacted = station_df[station_df["status_en_short"] == "Disruption"].copy()
least_impacted["n_times_affected"] = least_impacted.groupby("station")[
    "timestamp"
].transform("nunique")
least_impacted = least_impacted.drop_duplicates("station")
least_impacted = least_impacted.sort_values("n_times_affected")
least_impacted = least_impacted.iloc[:10]
least_impacted = least_impacted.sort_values("n_times_affected", ascending=False)

# Creating a bar chart with the most often disrupted stations
chart_stations_most = px.bar(
    most_impacted,
    x="n_times_affected",
    y="station",
)
chart_stations_most.update_layout(
    title_text="Top 10 stations most often impacted by disruptions"
)
chart_stations_most.update_xaxes(title_text="Number of records with disruption")
chart_stations_most.update_yaxes(title_text="Station")

# Creating a bar chart with the least often disrupted stations
chart_stations_least = px.bar(
    least_impacted,
    x="n_times_affected",
    y="station",
)
chart_stations_least.update_layout(
    title_text="The 10 stations most rarely impacted by disruptions"
)
chart_stations_least.update_xaxes(title_text="Number of records with disruption")
chart_stations_least.update_yaxes(title_text="Station")

# =========== CHART DESCRIPTIONS ===========
chance_dist_disclaimer = text["chance_dist_disclaimer"]
rush_disclaimer = text["rush_disclaimer"]
stations_disclaimer = text["stations_disclaimer"]
desc_mntn_chance_day = text["desc_mntn_chance_day"]
desc_mntn_dist_day = text["desc_mntn_dist_day"]
desc_dsrpt_chance_day = text["desc_dsrpt_chance_day"]
desc_dsrpt_dist_day = text["desc_dsrpt_dist_day"]
desc_dsrpt_chance_period = text["desc_dsrpt_chance_period"]
desc_mntn_chance_period = text["desc_mntn_chance_period"]
desc_mntn_dist_period = text["desc_mntn_dist_period"]
desc_dsrpt_dist_period = text["desc_dsrpt_dist_period"]
desc_dsrpt_chance_rush = text["desc_dsrpt_chance_rush"]
desc_mntn_chance_rush = text["desc_mntn_chance_rush"]
desc_mntn_dist_rush = text["desc_mntn_dist_rush"]
desc_dsrpt_dist_rush = text["desc_dsrpt_dist_rush"]
desc_most_impacted = text["desc_most_impacted"]
desc_less_impacted = text["desc_less_impacted"]

# Getting the name of the most impacted day and the extent of the impact
if len(by_day_chance_mntn):
    tmp_day = by_day_chance_dsrpt[
        by_day_chance_dsrpt["status_pct"] == by_day_chance_dsrpt["status_pct"].max()
    ]
    most_imp_day_name = tmp_day["weekday"].iloc[0]
    most_imp_day_val = tmp_day["status_pct"].iloc[0]
    most_imp_day_val = round(most_imp_day_val, 1)
else:
    most_imp_day_name, most_imp_day_val = "-", 0

# Plotting the elements in the correct order,
# starting with KPIs on top of the page and proceeding with charts
msg_text = text["imp_page_desc"]
msg_text = msg_text.format(selected_period=selected_period)
st.markdown(msg_text)

metric1, metric2, metric3 = st.columns(3)
metric1.metric("Most impacted day", str(most_imp_day_name))
metric2.metric("Avg disruption % on that day", str(most_imp_day_val) + "%")
metric3.metric("Most impacted station", str(most_imp_station_name))

st.subheader("Daily impact of unplanned disruptions", divider="grey")
st.markdown(desc_dsrpt_chance_day)
plot_or_not(chart_by_day_chance_dsrpt, by_day_chance_dsrpt)
st.markdown(desc_dsrpt_dist_day)
plot_or_not(chart_by_day_dist_dsrpt, by_day_dist_dsrpt)
st.markdown(chance_dist_disclaimer)

st.subheader("Daily impact of planned maintenance", divider="grey")
st.markdown(desc_mntn_chance_day)
plot_or_not(chart_by_day_chance_mntn, by_day_chance_mntn)
st.markdown(desc_mntn_dist_day)
plot_or_not(chart_by_day_dist_mntn, by_day_dist_mntn)
st.markdown(chance_dist_disclaimer)

st.subheader("Hourly impact of unplanned disruptions", divider="grey")
st.markdown(desc_dsrpt_chance_period)
st.warning(rush_disclaimer)
plot_or_not(chart_by_period_chance_dsrpt, by_period_chance_dsrpt)
st.markdown(desc_dsrpt_dist_period)
plot_or_not(chart_by_period_dist_dsprt, by_period_dist_dsrpt)
st.markdown(chance_dist_disclaimer)

st.subheader("Hourly impact of planned maintenance", divider="grey")
st.markdown(desc_mntn_chance_period)
st.warning(rush_disclaimer)
plot_or_not(chart_by_period_chance_mntn, by_period_chance_mntn)
st.markdown(desc_mntn_dist_period)
plot_or_not(chart_by_period_dist_mntn, by_period_dist_mntn)
st.markdown(chance_dist_disclaimer)

st.subheader(
    "Impact of unplanned disruptions during official rush hour", divider="grey"
)
st.markdown(desc_dsrpt_chance_rush)
plot_or_not(chart_by_rush_chance_dsrpt, by_rush_chance_dsrpt)
st.markdown(desc_dsrpt_dist_rush)
plot_or_not(chart_by_rush_dist_dsprt, by_rush_dist_dsrpt)
st.markdown(chance_dist_disclaimer)

st.subheader("Impact of planned maintenance during official rush hour", divider="grey")
st.markdown(desc_mntn_chance_rush)
plot_or_not(chart_by_rush_chance_mntn, by_rush_chance_mntn)
st.markdown(desc_mntn_dist_rush)
plot_or_not(chart_by_rush_dist_mntn, by_rush_dist_mntn)
st.markdown(chance_dist_disclaimer)

st.subheader("Most impacted stations", divider="grey")
st.markdown(desc_most_impacted)
plot_or_not(chart_stations_most, most_impacted)
st.markdown(stations_disclaimer)

st.subheader("Least impacted stations", divider="grey")
st.markdown(desc_less_impacted)
plot_or_not(chart_stations_least, least_impacted)
st.markdown(stations_disclaimer)

# %%
