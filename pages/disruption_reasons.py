"""
=======================
Disruption reasons page
=======================

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
st.header("Disruption reasons")

# Source & filter data, deal with warnings, etc.
page_filters = set_page_filters()
data = get_filtered_data(conn, page_filters)

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

# Preparing data on the detailed service status for disrupted service
vars_for_group = ["status_en"]
ignore_these = ["Unknown", "Normal service"]
detailed_status = df[~df["status_en_short"].isin(ignore_these)].copy()
detailed_status["rows_for_status"] = detailed_status.groupby(vars_for_group)[
    "date"
].transform("count")
detailed_status["status_pct"] = 100 * (
    detailed_status["rows_for_status"] / len(detailed_status)
)
detailed_status["status_pct"] = np.round(detailed_status["status_pct"], 1)
detailed_status = detailed_status.drop_duplicates(vars_for_group)
detailed_status = detailed_status.sort_values("rows_for_status")
detailed_status = detailed_status.reset_index(drop=True)

# Preparing KPI metrics for the page
# Note: depending on the period selected, not all statuses will be available
# in the data, so we need to check up on them before subsetting the data
check_maintn = "Maintenance/bus replacement" in detailed_status["status_en"].tolist()
check_delay = "Service running with delays" in detailed_status["status_en"].tolist()
check_stop = "Complete service disruption" in detailed_status["status_en"].tolist()
if check_maintn:
    pct_maintn = detailed_status[
        detailed_status["status_en"] == "Maintenance/bus replacement"
    ]["status_pct"].iloc[0]
else:
    pct_maintn = 0
if check_delay:
    pct_delay = detailed_status[
        detailed_status["status_en"] == "Service running with delays"
    ]["status_pct"].iloc[0]
else:
    pct_delay = 0
if check_stop:
    pct_stop = detailed_status[
        detailed_status["status_en"] == "Complete service disruption"
    ]["status_pct"].iloc[0]
else:
    pct_stop = 0

# Rounding off numbers to be used as KPIs
pct_maintn = round(pct_maintn, 1)
pct_delay = round(pct_delay, 1)
pct_stop = round(pct_stop, 1)

# Preparing data on the reasons behind the service disruptions
vars_for_group = ["reason"]
ignore_these = ["Unknown", "Normal service"]
reasons_split = df[~df["status_en_short"].isin(ignore_these)].copy()
reasons_split["rows_for_status"] = reasons_split.groupby(vars_for_group)[
    "date"
].transform("count")
reasons_split["status_pct"] = 100 * (
    reasons_split["rows_for_status"] / len(reasons_split)
)
reasons_split["status_pct"] = np.round(reasons_split["status_pct"], 1)
reasons_split = reasons_split.drop_duplicates(vars_for_group)
reasons_split = reasons_split.sort_values("rows_for_status")
reasons_split = reasons_split.reset_index(drop=True)

# Preparing data on selected reasons behind service disruptions
# Note: this excludes maitenance and unspecified reason
vars_for_group = ["reason"]
ignore_these = [
    "Unknown",
    "Unspecified",
    "Normal service",
    "Closed for maintenance",
]
reasons_det_split = df[~df["status_en_short"].isin(ignore_these)].copy()
reasons_det_split = reasons_det_split[
    ~reasons_det_split["reason"].isin(ignore_these)
].copy()
reasons_det_split["rows_for_status"] = reasons_det_split.groupby(vars_for_group)[
    "date"
].transform("count")
reasons_det_split["status_pct"] = 100 * (
    reasons_det_split["rows_for_status"] / len(reasons_det_split)
)
reasons_det_split["status_pct"] = np.round(reasons_det_split["status_pct"], 1)
reasons_det_split = reasons_det_split.drop_duplicates(vars_for_group)
reasons_det_split = reasons_det_split.sort_values("rows_for_status")
reasons_det_split = reasons_det_split.reset_index(drop=True)

# Creating a doughnut chart with the different kinds of disruptions
status_chart = px.pie(
    detailed_status, values="status_pct", names="status_en", hole=0.45
)
status_chart.update_layout(
    title_text=f"Metro service disruption kinds during the last {n_days} days",
    legend_title="",
)

# Creating a bar chart with the reasons behind the service disruptions
reasons_chart = px.bar(reasons_split, x="status_pct", y="reason")
reasons_chart.update_layout(
    title_text=f"Reasons behind service disruptions during the last {n_days} days"
)
reasons_chart.update_xaxes(title_text="% of time with disruptions")
reasons_chart.update_yaxes(title_text="Reason behind disruption")

# Creating a doughnut chart with some selected reasons behind the disruptions
reasons_det_chart = px.pie(
    reasons_det_split, values="status_pct", names="reason", hole=0.45
)
reasons_det_chart.update_layout(
    title_text=f"Major service impediments during the last {n_days} days",
    legend_title="",
)

# Preparing messages describing the charts
status_desc = text["rsn_status_desc"]
reasons_desc = text["rsn_reasons_desc"]
reasons_det_desc = text["rsn_reasons_det_desc"]

# Plotting the elements in the correct order,
# starting with KPIs on top of the page and proceeding with charts
msg_text = text["rsn_page_desc"]
msg_text = msg_text.format(selected_period=selected_period)
st.markdown(msg_text)

metric1, metric2, metric3 = st.columns(3)
metric1.metric("Maintenance", str(pct_maintn) + "%")
metric2.metric("Running with delays", str(pct_delay) + "%")
metric3.metric("Complete service disruptions", str(pct_stop) + "%")

st.subheader("Kinds of disruptions", divider="grey")
st.markdown(status_desc)
plot_or_not(status_chart, detailed_status)

st.subheader("Reasons behind service disruptions", divider="grey")
st.markdown(reasons_desc)
plot_or_not(reasons_chart, reasons_split)

st.markdown(reasons_det_desc)
plot_or_not(reasons_det_chart, reasons_det_split)


# %%
