"""
=============
Overview page
=============

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
st.header("Overview")

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

# Preparing data on the overall split by status
vars_for_group = ["status_en_short"]
overall_split = df.copy()
overall_split["rows_for_status"] = overall_split.groupby(vars_for_group)[
    "date"
].transform("count")
overall_split["status_pct"] = 100 * (
    overall_split["rows_for_status"] / len(overall_split)
)
overall_split["status_pct"] = np.round(overall_split["status_pct"], 1)
overall_split = overall_split.drop_duplicates(vars_for_group)
overall_split = overall_split.reset_index(drop=True)

# Preparing data on the detailed split by status
vars_for_group = ["status_en"]
detailed_split = df[df["status_en_short"] != "Normal service"].copy()
detailed_split["rows_for_status"] = detailed_split.groupby(vars_for_group)[
    "date"
].transform("count")
detailed_split["status_pct"] = 100 * (
    detailed_split["rows_for_status"] / len(detailed_split)
)
detailed_split["status_pct"] = np.round(detailed_split["status_pct"], 1)
detailed_split = detailed_split.drop_duplicates(vars_for_group)
detailed_split = detailed_split.sort_values("rows_for_status")
detailed_split = detailed_split.reset_index(drop=True)

# Preparing KPI metrics for the page
# Note: depending on the period selected, not all statuses will be available
# in the data, so we need to check up on them before subsetting the data
check_normal = "Normal service" in overall_split["status_en_short"].tolist()
check_disruption = "Disruption" in overall_split["status_en_short"].tolist()
check_unknown = "Unknown" in overall_split["status_en_short"].tolist()
if check_normal:
    pct_normal_service = overall_split[
        overall_split["status_en_short"] == "Normal service"
    ]["status_pct"].iloc[0]
else:
    pct_normal_service = 0
if check_disruption:
    pct_disruption = overall_split[overall_split["status_en_short"] == "Disruption"][
        "status_pct"
    ].iloc[0]
else:
    pct_disruption = 0
if check_unknown:
    pct_unknown = overall_split[overall_split["status_en_short"] == "Unknown"][
        "status_pct"
    ].iloc[0]
else:
    pct_unknown = 0

# Rounding off numbers to be used as KPIs
pct_normal_service = round(pct_normal_service, 1)
pct_disruption = round(pct_disruption, 1)
pct_unknown = round(pct_unknown, 1)

# Creating a doughnut chart with the overall status split
overall_chart = px.pie(
    overall_split, values="status_pct", names="status_en_short", hole=0.45
)
# overall_chart.update_traces(textinfo="percent+label") # adds data labels on chart
overall_chart.update_layout(
    title_text=f"CPH metro service status between {selected_period}",
    legend_title="",
    # autosize=False,
    # width=500,
    # height=500,
    # margin=dict(l=50, r=50, b=100, t=100, pad=4),
)

# Creating a doughnut chart with the detailed status split
detailed_chart = px.bar(detailed_split, x="status_pct", y="status_en")
detailed_chart.update_layout(
    title_text=f"Detailed service status between {selected_period} (excl. normal service)"
)
detailed_chart.update_xaxes(title_text="% of time with non-normal service status")
detailed_chart.update_yaxes(title_text="Detailed service status")

# Preparing messages describing the charts
overall_desc = text["gen_overall_desc"]
detailed_desc = text["gen_detailed_desc"]

# Plotting the elements in the correct order,
# starting with KPIs on top of the page and proceeding with charts
msg_text = text["gen_page_desc"]
msg_text = msg_text.format(selected_period=selected_period)
st.markdown(msg_text)

metric1, metric2, metric3 = st.columns(3)
metric1.metric("Normal service, pct of time", str(pct_normal_service) + "%")
metric2.metric("Disrupted service, pct of time", str(pct_disruption) + "%")
metric3.metric("Unknown status, pct of time", str(pct_unknown) + "%")

st.subheader("Overall service status", divider="grey")
st.markdown(overall_desc)
plot_or_not(overall_chart, overall_split)

st.subheader("Detailed service status", divider="grey")
st.markdown(detailed_desc)
plot_or_not(detailed_chart, detailed_split)


# %%
