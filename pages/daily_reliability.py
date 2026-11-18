"""
======================
Daily reliability page
======================

Author: github.com/cyrilby
Last meaningful update: 14-10-2026
"""

# %% Setting up

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from plotly_calplot import calplot

from core.helpers import colors_for_calplot, get_period_string, load_text, plot_or_not
from core.loader import get_filtered_data, set_page_filters
from core.storage import get_app_data

# Importing data and text strings used in the app
text = load_text()
conn = get_app_data()


# %% Common for all data-driven pages

# Page title displayed on top
st.header("Daily availability")

# Source & filter data, deal with warnings, etc.
page_filters = set_page_filters()
data = get_filtered_data(conn, page_filters, apply_date_filter=False)

min_date = data.min_date
max_date = data.max_date
n_days = data.n_days
downtime_msg = data.downtime_msg
selected_period = data.selected_period
df = data.df

# Confirming the selected period
st.sidebar.markdown(
    f"**Note:** this selection covers the period between {selected_period}."
    + downtime_msg
)

# %% Specific page content

# Preparing data on daily disruption score where the score means:
# 0 = no disruptions recorded/status "Unknown" only
# 1 = at least 1 partial disruption recorded during that day
# 2 = at least 1 complete disruption recorded during that day
df["status_unknown"] = df["status_en"] == "Unknown"
df["normal_service"] = df["status_en"] == "Normal service"
df["complete_disruption"] = df["status_en"] == "Complete service disruption"
df["partial_disruption"] = ~df["status_en"].isin(
    [
        "Normal service",
        "Complete service disruption",
        "Closed for maintenance",
    ]
)
df["disruption_score"] = np.where(
    df["normal_service"],
    0,
    np.where(
        df["partial_disruption"],
        1,
        np.where(df["complete_disruption"], 2, 0),
    ),
)
df["disruption_score"] = df.groupby("date")["disruption_score"].transform("max")
df["disruption_score"] = np.where(df["status_unknown"], -1, df["disruption_score"])
df = df.drop_duplicates("date")
df = df.reset_index(drop=True)
df = df[["date", "disruption_score"]]
status_dict = {
    -1: "Unknown status",
    0: "Normal service",
    1: "Partial disruption",
    2: "Complete disruption",
}
df["interpretation"] = df["disruption_score"].map(status_dict)

# Confirming the selected period for the calendar chart
recent_reliability = df[(df["date"] >= min_date) & (df["date"] <= max_date)].copy()
selected_period = get_period_string(min_date, max_date)
date_range = pd.date_range(start=min_date, end=max_date)
date_range = pd.DataFrame(date_range, columns=["date"])

# Preparing a dataset with N of days without any disrruptions
# aggregated by month
hist_no_disr = df.copy()
hist_no_disr["month"] = hist_no_disr["date"].dt.to_period("M").dt.to_timestamp()
hist_no_disr["day_no_disr"] = hist_no_disr["disruption_score"] == 0
hist_no_disr["days_without_disruption"] = hist_no_disr.groupby("month")[
    "day_no_disr"
].transform("mean")
hist_no_disr["days_without_disruption"] = 100 * hist_no_disr["days_without_disruption"]
hist_no_disr["days_without_disruption"] = hist_no_disr["days_without_disruption"].round(
    1
)
hist_no_disr = hist_no_disr[["month", "days_without_disruption"]]
hist_no_disr = hist_no_disr.sort_values("month")
hist_no_disr = hist_no_disr.drop_duplicates()
hist_no_disr = hist_no_disr.reset_index(drop=True)

# Preparing a dataset with N of days with partial disruptions
# aggregated by month
hist_partial_disr = df.copy()
hist_partial_disr["month"] = (
    hist_partial_disr["date"].dt.to_period("M").dt.to_timestamp()
)
hist_partial_disr["day_disr"] = hist_partial_disr["disruption_score"] == 1
hist_partial_disr["days_with_disruption"] = hist_partial_disr.groupby("month")[
    "day_disr"
].transform("mean")
hist_partial_disr["days_with_disruption"] = (
    100 * hist_partial_disr["days_with_disruption"]
)
hist_partial_disr["days_with_disruption"] = hist_partial_disr[
    "days_with_disruption"
].round(1)
hist_partial_disr = hist_partial_disr[["month", "days_with_disruption"]]
hist_partial_disr = hist_partial_disr.sort_values("month")
hist_partial_disr = hist_partial_disr.drop_duplicates()
hist_partial_disr = hist_partial_disr.reset_index(drop=True)

# Preparing a dataset with N of days with partial disruptions
# aggregated by month
hist_complete_disr = df.copy()
hist_complete_disr["month"] = (
    hist_complete_disr["date"].dt.to_period("M").dt.to_timestamp()
)
hist_complete_disr["day_disr"] = hist_complete_disr["disruption_score"] == 2
hist_complete_disr["days_with_disruption"] = hist_complete_disr.groupby("month")[
    "day_disr"
].transform("mean")
hist_complete_disr["days_with_disruption"] = (
    100 * hist_complete_disr["days_with_disruption"]
)
hist_complete_disr["days_with_disruption"] = hist_complete_disr[
    "days_with_disruption"
].round(1)
hist_complete_disr = hist_complete_disr[["month", "days_with_disruption"]]
hist_complete_disr = hist_complete_disr.sort_values("month")
hist_complete_disr = hist_complete_disr.drop_duplicates()
hist_complete_disr = hist_complete_disr.reset_index(drop=True)

# Creating a line chart showing % of days without disruptions
# aggregated by month
pct_no_disr_chart = px.line(
    hist_no_disr,
    x="month",
    y="days_without_disruption",
)
pct_no_disr_chart.update_layout(
    title_text="% of days without any service disruptions by month"
)
pct_no_disr_chart.update_xaxes(title_text="Month")
pct_no_disr_chart.update_yaxes(title_text="% of days in month")

# Creating a line chart showing % of days with partial
# disruptions aggregated by month
pct_partial_disr_chart = px.line(
    hist_partial_disr,
    x="month",
    y="days_with_disruption",
)
pct_partial_disr_chart.update_layout(
    title_text="% of days with partial service disruption(s) by month"
)
pct_partial_disr_chart.update_xaxes(title_text="Month")
pct_partial_disr_chart.update_yaxes(title_text="% of days in month")

# Creating a line chart showing % of days with complete
# disruptions aggregated by month
pct_complete_disr_chart = px.line(
    hist_complete_disr,
    x="month",
    y="days_with_disruption",
)
pct_complete_disr_chart.update_layout(
    title_text="% of days with complete service disruption(s) by month"
)
pct_complete_disr_chart.update_xaxes(title_text="Month")
pct_complete_disr_chart.update_yaxes(title_text="% of days in month")

# Creating summary KPIs to show above chart
cal_kpi = recent_reliability.copy()
# KPI 1: % of days with normal service
cal_kpi_normal = recent_reliability[recent_reliability["disruption_score"] == 0]
cal_kpi_normal = len(cal_kpi_normal) / len(cal_kpi)
cal_kpi_normal = round(100 * cal_kpi_normal, 1)
# KPI 2: % of days with partial disruptions
cal_kpi_partial = recent_reliability[recent_reliability["disruption_score"] == 1]
cal_kpi_partial = len(cal_kpi_partial) / len(cal_kpi)
cal_kpi_partial = round(100 * cal_kpi_partial, 1)
# KPI 3: % of days with complete disruptions
cal_kpi_complete = recent_reliability[recent_reliability["disruption_score"] == 2]
cal_kpi_complete = len(cal_kpi_complete) / len(cal_kpi)
cal_kpi_complete = round(100 * cal_kpi_complete, 1)

# Recording metadata on the daily disruption score for use on chart
cal_start_month = recent_reliability["date"].dt.month.min()
cal_end_month = recent_reliability["date"].dt.month.max()
chart_height = 500  # if n_days <= 31 else None

# Getting unique statuses to be shown on calplot and choosing the right colors
custom_colors = colors_for_calplot(recent_reliability)

# Creating a calplot heatmap with the daily disruption score
cal_fig = calplot(
    recent_reliability,
    x="date",
    y="disruption_score",
    total_height=chart_height,
    colorscale=custom_colors,
    start_month=cal_start_month,
    end_month=cal_end_month,
    text="interpretation",
    title=f"Metro reliability between {selected_period}",
)

# Preparing messages describing the charts
cal_desc = text["gen_cal_desc"]

# Plotting the elements in the correct order,
# starting with KPIs on top of the page and proceeding with charts
msg_text = text["daily_page_desc"]
msg_text = msg_text.format(selected_period=selected_period)
st.markdown(msg_text)
cal_metric1, cal_metric2, cal_metric3 = st.columns(3)
cal_metric1.metric("% of days without any disruptions", str(cal_kpi_normal) + "%")
cal_metric2.metric("% of days with partial disruptions", str(cal_kpi_partial) + "%")
cal_metric3.metric("% of days with complete disruptions", str(cal_kpi_complete) + "%")

# Plotting a calendar-based overview
# Note: we also plot a new set of 3 calendar KPIs
st.subheader(f"Service reliability between {selected_period}", divider="grey")
st.markdown(cal_desc, unsafe_allow_html=True)
plot_or_not(cal_fig, recent_reliability)

# Plotting historical data aggregated by month
st.subheader("Days without any disruptions by month", divider="grey")
st.markdown(text["daily_page_hist_no_disr"])
plot_or_not(pct_no_disr_chart, hist_no_disr)

st.subheader("Days with partial disruption(s) by month", divider="grey")
st.markdown(text["daily_page_hist_no_disr"])  # WIP as of 22-03-2026
plot_or_not(pct_partial_disr_chart, hist_partial_disr)

st.subheader("Days with complete disruption(s) by month", divider="grey")
st.markdown(text["daily_page_hist_no_disr"])  # WIP as of 22-03-2026
plot_or_not(pct_complete_disr_chart, hist_complete_disr)


# %%
