"""
==================================================
App visualizing the CPH Metro's operational status
==================================================

Author: kirilboyanovbg[at]gmail.com
Last meaningful update: 02-04-2024

This script contains the source code of the Streamlit app accompanying
the CPH metro scraper tool. In here, we create a series of data visualizations
that help us get a better understanding of how often the metro breaks down,
when and where the impact is felt as well as what the reasons behind the
breakdowns are (if information on those is available).
"""

# %% Setting things up

# Importing relevant packages
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from azure.storage.blob import BlobClient
import io
import os


# Custom function to access Azure files annonymously
def read_blob_anonymously(blob_url: str, **kwargs):
    """
    Imports a file stored in Azure blob storage into Python's memory.
    Object type depends on the file itself and can range from a string,
    list or dict to a pandas data frame (this is auto detected based
    on the file extension).

    Args:
        blob_url (str): URL of the blob

    Raises:
        ValueError: if we try to read an unsupported file type

    Returns:
        Any: any object (if pickled), string (if txt), dict (if json) or
        otherwise pandas.DataFrame
    """
    # We use the file extension to determine the function used to read data
    _, extension = os.path.splitext(blob_url)

    # We download the blob as a Python object
    blob_client = BlobClient.from_blob_url(blob_url)
    obj = blob_client.download_blob().readall()

    # For objects assumed to be pandas df, we auto detect the file type
    # from the file extension and then call the appropriate pandas.read_X() fn
    if extension == ".csv":
        conv_obj = pd.read_csv(io.BytesIO(obj), **kwargs)
    elif extension in [".xlsx", ".xls", ".xlsm"]:
        conv_obj = pd.read_excel(io.BytesIO(obj), **kwargs)
    elif extension == ".html":
        conv_obj = pd.read_html(io.BytesIO(obj), **kwargs)
    elif extension == ".hdf":
        conv_obj = pd.read_hdf(io.BytesIO(obj), key="data", **kwargs)
    elif extension == ".stata":
        conv_obj = pd.read_stata(io.BytesIO(obj), **kwargs)
    elif extension == ".gbq":
        conv_obj = pd.read_gbq(io.BytesIO(obj), "my_dataset.my_table", **kwargs)
    elif extension == ".parquet":
        conv_obj = pd.read_parquet(io.BytesIO(obj), **kwargs)
    elif extension == ".pkl":
        conv_obj = pd.read_pickle(io.BytesIO(obj), **kwargs)
    elif extension in [".f", ".feather"]:
        conv_obj = pd.read_feather(io.BytesIO(obj), **kwargs)
    # For all other objects, we raise an error, though in theory, we could also
    # enable the direct import to a bytes IO object
    else:
        raise ValueError(f"Unsupported file extension: {extension}")
    # else:
    #    conv_obj = io.BytesIO(obj)
    return conv_obj


# %% Importing data for use in the app

# Importing pre-processed data & relevant mapping tables from Azure
operation_fmt = read_blob_anonymously(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/operation_fmt.parquet"
)
station_impact = read_blob_anonymously(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/station_impact.parquet"
)
mapping_stations = read_blob_anonymously(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/mapping_stations.pkl"
)
mapping_messages = read_blob_anonymously(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/mapping_messages.pkl"
)

# Correcting dtypes
operation_fmt["date"] = pd.to_datetime(operation_fmt["date"])

# Extracting values from the data to be used in slicers etc.
unique_dates = operation_fmt["date"].unique().tolist()
unique_last_n_days = operation_fmt["date_in_last_n_days"].unique().tolist()

n_days = len(unique_dates)
n_possible_days = np.arange(1, n_days + 1)

# Preparing a list of (N of) days to use as filter in the app
unique_dates.sort(reverse=True)
unique_last_n_days.sort()

# Getting info on the most recent date with data
cols_to_keep = ["timestamp", "line", "status_en", "status_dk"]
most_recent = operation_fmt[
    operation_fmt["timestamp"] == operation_fmt["timestamp"].max()
][cols_to_keep].copy()
most_recent = most_recent.reset_index(drop=True)
last_update = most_recent["timestamp"][0]
last_update_date = last_update.strftime("%d %B %Y")
last_update_time = last_update.strftime("%H:%M")

# Extracting the unique names of potentially impacted stations
unique_stations = station_impact["station"].unique().tolist()
unique_stations.sort()

# Getting unique intervals and days of time travel
unique_intervals = station_impact["hour_interval"].unique().tolist()
unique_intervals.sort()
unique_day_names = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

# Detecting whether there are any unmapped service status messages
# as well as the date(s) where data accuracy may be impacted due to lacking mapping
unmapped_msg = operation_fmt[
    (operation_fmt["status_en"] == "Unknown")
    & (operation_fmt["status_dk"] != "Unknown")
][["date", "status_en", "status_dk"]].copy()
unmapped_rows = len(unmapped_msg)
total_rows = len(operation_fmt)
unmappped_rows_pct = round(100 * ((unmapped_rows + 1) / total_rows), 1)
unmapped_msg = unmapped_msg.drop_duplicates(subset="status_dk")
unmapped_msg_n = len(unmapped_msg)
if unmapped_msg_n:
    unmapped_msg_date_min = unmapped_msg["date"].min()
    unmapped_msg_date_max = unmapped_msg["date"].max()
    if unmapped_msg_date_min == unmapped_msg_date_max:
        unmapped_msg_date_min = None
    else:
        unmapped_msg_date_min = unmapped_msg_date_min.strftime("%d %B %Y")
    unmapped_msg_date_max = unmapped_msg_date_max.strftime("%d %B %Y")

# Preparing a warning for the app's front page if there are unmapped entries
if unmapped_msg_n:
    if unmapped_msg_date_min:
        mapping_warning = f"""There are {unmapped_msg_n} metro status messages
        that have not yet been classified in terms of their impact on service
        status. This means that in the period {unmapped_msg_date_min}-
        {unmapped_msg_date_max}, the numbers in the 'Unknown' category may be
        overestimated and the numbers in the 'Normal service' and 'Disrupted
        service' categories may be underestimated. The share of records impacted by 
        this is lower than {unmappped_rows_pct}% across all historical data.
        """
    else:
        mapping_warning = f"""There are {unmapped_msg_n} metro status messages
        that have not yet been classified in terms of their impact on service
        status. This means that on {unmapped_msg_date_max}, the numbers in the
        'Unknown' category may be overestimated and the numbers in the 'Normal
        service' and 'Disrupted service' categories may be underestimated.
        The share of records impacted by this is lower than {unmappped_rows_pct}%
        across all historical data.
        """
else:
    mapping_warning = None


# %% Preparing some basic things for the app

# Setting up the web page title and icon of the app
st.set_page_config(page_title="CPH metro status", page_icon="🚇")

# Adding title to the app's sidebar
st.sidebar.title("CPH metro status")

# Adding a list of pages to the sidebar
options = st.sidebar.radio(
    "Pages",
    options=[
        "Welcome",
        "Overview",
        "Disruption reasons",
        "Disruption impact",
        "Disruption history",
        "Disruption calculator",
        "Info on data sources",
        "Legal disclaimer",
    ],
)


# Setting up sidebar filter for number of days to show
def filter_by_n_days(list_of_days):
    selected_n_days = st.sidebar.select_slider(
        "Number of recent days to show",
        list_of_days,
        value=30,  # showing last 30 days by default
    )
    return selected_n_days


# Setting up sidebar filter for day type (workday or weekend)
def filter_by_day_type():
    possible_day_types = ["Workdays", "Weekends"]
    selected_types = st.sidebar.multiselect(
        "Kinds of days to show", possible_day_types, default=possible_day_types
    )
    # If no option is selected, select all by default
    if not selected_types:
        selected_types = possible_day_types
        st.sidebar.warning(
            "Warning: Data will not be filtered for day type due to empty user selection."
        )

    return selected_types


# Setting up sidebar filter for hour type (regular or rush hour)
def filter_by_hour_type():
    possible_h_types = ["Regular hour", "Morning rush hour", "Afternoon rush hour"]
    selected_types = st.sidebar.multiselect(
        "Kinds of hours to show", possible_h_types, default=possible_h_types
    )
    # If no option is selected, select all by default
    if not selected_types:
        selected_types = possible_h_types
        st.sidebar.warning(
            "Warning: Data will not be filtered for hour type due to empty user selection."
        )

    return selected_types


# Setting up sidebar filter for metro line
def filter_by_line():
    possible_lines = ["M1", "M2", "M3", "M4"]
    selected_line = st.sidebar.multiselect(
        "Selected lines", possible_lines, default=possible_lines
    )
    # If no option is selected, select all by default
    if not selected_line:
        selected_line = possible_lines
        st.sidebar.warning(
            "Warning: Data will not be filtered for metro line due to empty user selection."
        )

    return selected_line


# %% Defining further custom functions for the app


def get_period_string(df: pd.DataFrame, date_col: str):
    """
    Uses a date field to get the period covered by the data that's
    being presented to the user.

    Args:
        df (pd.DataFrame): df containing the date field and the rest of the data
        date_col (str): name of the date field

    """
    min_date = df[date_col].min()
    max_date = df[date_col].max()
    min_date_str = min_date.strftime("%d %b %Y")
    max_date_str = max_date.strftime("%d %b %Y")
    selected_period = min_date_str + " - " + max_date_str
    return selected_period, min_date, max_date


def plot_or_not(plot, df: pd.DataFrame):
    """
    Checks whether the data frame used in a chart contains any rows.
    If there is at least 1 row of data available, it displays the plot.
    Otherwise, it displays a warning message to the user, prompting them
    to adjust their selection.

    Args:
        plot (): plotlly chart object
        df (pd.DataFrame): df used as the basis for the plotly chart
    """

    if len(df) >= 1:
        st.plotly_chart(plot)
    else:
        st.warning(
            """Warning: you have filtered the data to subsample which contains
                   no data that can be shown on this chart. Please adjust your
                   selection using the slicers in the sidebar to the left."""
        )


# %% Page: Welcome to the app


# Informs the user of the app's purpose
def show_homepage():
    st.header("Welcome to the CPH metro status app!")
    st.markdown(
        """This app presents information on the Copenhagen Metro's
    **operational status over time**, allowing to measure the impact of the
    disruptions caused to passengers on different lines, stations and different
    times of day."""
    )
    st.markdown(
        """**Please scroll down** to see the latest operational status
    as well as learn how to make the best use of the app."""
    )
    st.image(
        "resources/metro_map_with_stations.png"
    )  # image can be disabled in development phase

    # Displaying the latest service message from the metro's website
    st.subheader("Latest service status data", divider="rainbow")
    if mapping_warning:
        st.warning(mapping_warning)
    st.markdown(
        f"""The **latest data** on the CPH metro's service status were
            collected on **{last_update_date} at {last_update_time}** o'clock.
            A preview of the latest status for each line is shown below:"""
    )
    st.dataframe(most_recent)

    # Displaying more info on how the app can help the user
    st.subheader("How this app can help you", divider="rainbow")
    st.markdown(
        """This app focuses on keeping track of the Copenhagen Metro's service
        status and aggregating that data in meaningful ways, allowing you to
        **find answers to questions such as**:
        """
    )
    st.markdown(
        f"""
        - How often has the metro run according to schedule in the past (up to)
        {n_days} days?
        - How often has the metro experienced disruptions either due to
        maintenance or unexpected issues?
        - What were the main causes behind the disruptions that occurred
        in the selected time period?
        - Which days of the week saw the highest number of disruptions and which
       stations were most impacted by them?
       - Have there been any improvements/a worsening in the share of time
       where the metro ran according to schedule in the selected time period?
        """
    )

    # Displaying more info on how to use the app
    st.subheader("How to use this app", divider="rainbow")
    st.markdown("This app consists of the following two panes:")
    st.markdown(
        """
        - **The sidebar**: allows you to navigate between the different pages 
        included in the app as well as to apply different filters on the data, such
        as choosing how many days of data to include in the calculations that 
        generate the data shown on the charts.
        - **The main panel**: contains various charts, tables and text descriptions.
        """
    )
    st.markdown(
        "To **switch between different pages**, please click on the page title:"
    )
    st.image("resources/pages_examples.PNG")
    st.markdown("You will then be redirected to the desired page.")

    # Displaying more info on how the user can filter the data
    st.subheader("How to apply filters to the data", divider="rainbow")
    st.markdown(
        """To **apply a filter to the data**, please click on the filter
        you wish to apply and select the desired value(s). Please note that the
        filter that lets you decide how many days of historical data to use in
        the calculations supports *choosing only one value*. By default, it is set
        to display the last 30 days:
        """
    )
    st.image("resources/filter_recent_days.PNG")
    st.markdown(
        """All other filters support **choosing multiple values** at the
        same time. By default, all values are kept:"""
    )
    st.image("resources/filter_lines.PNG")
    st.markdown(
        """**Please note** that if you filter the data too much or remove all
        pre-made selections, there might not be enough data left to display on
        the charts and the app may revert to selecting something for you.
        Should that be the case, a **warning message** will be displayed
        such as the one below:
        """
    )
    st.image("resources/selection_warning.PNG")
    st.markdown(
        """To get rid of the warning, please revise your selection
        or refresh the web page to start over.
        """
    )
    st.divider()
    st.markdown("*Front page image source: www.copenhagenmap360.com*")


# %% Page: General service overview


def general_overview():
    st.header("Overview")

    # Detecting and confirming slicer selections
    n_days = filter_by_n_days(n_possible_days)
    day_types = filter_by_day_type()
    hour_types = filter_by_hour_type()
    selected_lines = filter_by_line()

    # Filtering and arranging the data
    data_to_display = operation_fmt[
        operation_fmt["date_in_last_n_days"] <= n_days
    ].copy()
    data_to_display = data_to_display[
        data_to_display["day_type"].isin(day_types)
    ].copy()
    data_to_display = data_to_display[
        data_to_display["official_rush_hour"].isin(hour_types)
    ].copy()
    data_to_display = data_to_display[
        data_to_display["line"].isin(selected_lines)
    ].copy()
    data_to_display = data_to_display.sort_values("date")
    data_to_display = data_to_display.reset_index(drop=True)

    # Confirming the selected period
    selected_period, min_date, max_date = get_period_string(data_to_display, "date")
    date_range = pd.date_range(start=min_date, end=max_date)
    date_range = pd.DataFrame(date_range, columns=["date"])
    st.sidebar.markdown(
        f"**Note:** this selection covers the period between {selected_period}."
    )

    # Preparing data on the overall split by status
    vars_for_group = ["status_en_short"]
    overall_split = data_to_display.copy()
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
    detailed_split = data_to_display[
        data_to_display["status_en_short"] != "Normal service"
    ].copy()
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
        pct_disruption = overall_split[
            overall_split["status_en_short"] == "Disruption"
        ]["status_pct"].iloc[0]
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
    overall_chart.update_traces(textinfo="percent+label")
    overall_chart.update_layout(
        title_text=f"CPH metro service status during the last {n_days} days",
        legend_title="",
        autosize=False,
        width=500,
        height=500,
        margin=dict(l=50, r=50, b=100, t=100, pad=4),
    )

    # Creating a doughnut chart with the detailed status split
    detailed_chart = px.bar(detailed_split, x="status_pct", y="status_en")
    detailed_chart.update_layout(
        title_text=f"Detailed service status during the last {n_days} days (excl. normal service)"
    )
    detailed_chart.update_xaxes(title_text="% of time with non-normal service status")
    detailed_chart.update_yaxes(title_text="Detailed service status")

    # Preparing messages describing the charts
    overall_desc = """The chart below shows the overall split between normal service
    operation, service disruptions and unknown status. As such, it indicates how
    prevalent service disruptions have been in the selected period relative to
    normal operation:"""
    detailed_desc = """The chart below zooms in on cases where the metro did not
    operate with a normal service and shows the split between the different kinds
    of other service messages. As such, it allows to understand whether service
    was impacted by e.g. planned maintenance, delays or a complete disruption:"""

    # Plotting the elements in the correct order,
    # starting with KPIs on top of the page and proceeding with charts
    if mapping_warning:
        st.warning(mapping_warning)
    st.markdown(
        f"""This page shows information on the **overall status of the metro
        between {selected_period}**, including the share of the time where service
        was running normally or with disruptions."""
    )

    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Normal service, pct of time", str(pct_normal_service) + "%")
    metric2.metric("Disrupted service, pct of time", str(pct_disruption) + "%")
    metric3.metric("Unknown status, pct of time", str(pct_unknown) + "%")

    st.subheader("Overall service status", divider="rainbow")
    st.markdown(overall_desc)
    plot_or_not(overall_chart, overall_split)
    # st.plotly_chart(overall_chart)

    st.subheader("Detailed service status", divider="rainbow")
    st.markdown(detailed_desc)
    plot_or_not(detailed_chart, detailed_split)
    # st.plotly_chart(detailed_chart)


# %% Page: service disruption insights


def disruption_reasons():
    st.header("Disruption reasons")

    # Detecting and confirming slicer selections
    n_days = filter_by_n_days(n_possible_days)
    day_types = filter_by_day_type()
    hour_types = filter_by_hour_type()
    selected_lines = filter_by_line()

    # Filtering and arranging the data
    data_to_display = operation_fmt[
        operation_fmt["date_in_last_n_days"] <= n_days
    ].copy()
    data_to_display = data_to_display[
        data_to_display["day_type"].isin(day_types)
    ].copy()
    data_to_display = data_to_display[
        data_to_display["official_rush_hour"].isin(hour_types)
    ].copy()
    data_to_display = data_to_display[
        data_to_display["line"].isin(selected_lines)
    ].copy()
    data_to_display = data_to_display.sort_values("date")
    data_to_display = data_to_display.reset_index(drop=True)

    # Confirming the selected period
    selected_period, min_date, max_date = get_period_string(data_to_display, "date")
    date_range = pd.date_range(start=min_date, end=max_date)
    date_range = pd.DataFrame(date_range, columns=["date"])
    st.sidebar.markdown(
        f"**Note:** this selection covers the period between {selected_period}."
    )

    # Preparing data on the detailed service status for disrupted service
    vars_for_group = ["status_en"]
    ignore_these = ["Unknown", "Normal service"]
    detailed_status = data_to_display[
        ~data_to_display["status_en_short"].isin(ignore_these)
    ].copy()
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
    check_maintn = (
        "Maintenance/bus replacement" in detailed_status["status_en"].tolist()
    )
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
    reasons_split = data_to_display[
        ~data_to_display["status_en_short"].isin(ignore_these)
    ].copy()
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
    ignore_these = ["Unknown", "Unspecified", "Normal service", "Maintenance"]
    reasons_det_split = data_to_display[
        ~data_to_display["status_en_short"].isin(ignore_these)
    ].copy()
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
    status_desc = """The chart below presents the split between different kinds
    of service disruptions, exclusive of normally running service and unknown
    status. As such, it allows to understand whether service disruptions
    manifested as e.g. planned maintenance, delays or a complete service stop:"""
    reasons_desc = """The chart below shows the different reasons given by the
    CPH metro team. As such, it gives an insight into whether service disruption
    is mostly due to e.g. technical issues or something else. Please note that
    a reason is not reported for all disruptions, which can inflate the relative
    importance of the "Unspecified" factor:"""
    reasons_det_desc = """The chart below presents all specified reasons behind
    the various service disruption, exluding maintenance. As such, it allows us
    to understand which areas the CPH metro should improve in to minimize the
    occurrence of future disruptions:"""

    # Plotting the elements in the correct order,
    # starting with KPIs on top of the page and proceeding with charts
    if mapping_warning:
        st.warning(mapping_warning)
    st.markdown(
        f"""This page contains insights on the **kinds of service disruptions between
            {selected_period}**, including the reasons behind those disruptions and
            some the CPH metro team's major impediments to running a normal service."""
    )

    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Maintenance", str(pct_maintn) + "%")
    metric2.metric("Running with delays", str(pct_delay) + "%")
    metric3.metric("Complete service disruptions", str(pct_stop) + "%")

    st.subheader("Kinds of disruptions", divider="rainbow")
    st.markdown(status_desc)
    plot_or_not(status_chart, detailed_status)
    # st.plotly_chart(status_chart)

    st.subheader("Reasons behind service disruptions", divider="rainbow")
    st.markdown(reasons_desc)
    plot_or_not(reasons_chart, reasons_split)
    # st.plotly_chart(reasons_chart)

    st.markdown(reasons_det_desc)
    plot_or_not(reasons_det_chart, reasons_det_split)
    # st.plotly_chart(reasons_det_chart)


# %% Page: service disruption impact


def disruption_impact():
    st.header("Disruption impact")

    # Detecting and confirming slicer selections
    n_days = filter_by_n_days(n_possible_days)
    day_types = filter_by_day_type()
    hour_types = filter_by_hour_type()
    selected_lines = filter_by_line()

    # Filtering and arranging the operations data
    data_to_display = operation_fmt[
        operation_fmt["date_in_last_n_days"] <= n_days
    ].copy()
    data_to_display = data_to_display[
        data_to_display["day_type"].isin(day_types)
    ].copy()
    data_to_display = data_to_display[
        data_to_display["official_rush_hour"].isin(hour_types)
    ].copy()
    data_to_display = data_to_display[
        data_to_display["line"].isin(selected_lines)
    ].copy()
    data_to_display = data_to_display.sort_values("date")
    data_to_display = data_to_display.reset_index(drop=True)

    # Filtering and arranging the station impact data
    st_data_to_display = station_impact[
        station_impact["date_in_last_n_days"] <= n_days
    ].copy()
    st_data_to_display = st_data_to_display[
        st_data_to_display["day_type"].isin(day_types)
    ].copy()
    st_data_to_display = st_data_to_display[
        st_data_to_display["line"].isin(selected_lines)
    ].copy()
    st_data_to_display = st_data_to_display.sort_values("date")
    st_data_to_display = st_data_to_display.reset_index(drop=True)

    # Confirming the selected period
    selected_period, min_date, max_date = get_period_string(data_to_display, "date")
    date_range = pd.date_range(start=min_date, end=max_date)
    date_range = pd.DataFrame(date_range, columns=["date"])
    st.sidebar.markdown(
        f"**Note:** this selection covers the period between {selected_period}."
    )

    # Preparing data on the daily likelihood of disruptions, incl. maintenance
    vars_for_group = ["status_en_short", "weekday"]
    by_day_with_mntn = data_to_display.copy()
    by_day_with_mntn["status_en_short"] = np.where(
        by_day_with_mntn["status_en"] == "Closed for maintenance",
        by_day_with_mntn["status_en_short"],
        "Normal service",
    )
    by_day_with_mntn["rows_for_status"] = by_day_with_mntn.groupby(vars_for_group)[
        "date"
    ].transform("count")
    by_day_with_mntn["rows_for_day"] = by_day_with_mntn.groupby("weekday")[
        "date"
    ].transform("count")
    by_day_with_mntn["status_pct"] = 100 * (
        by_day_with_mntn["rows_for_status"] / by_day_with_mntn["rows_for_day"]
    )
    by_day_with_mntn["status_pct"] = np.round(by_day_with_mntn["status_pct"], 1)
    by_day_with_mntn = by_day_with_mntn.drop_duplicates(subset=vars_for_group)
    by_day_with_mntn = by_day_with_mntn[
        by_day_with_mntn["status_en_short"] == "Disruption"
    ].copy()
    by_day_with_mntn = by_day_with_mntn.sort_values("weekday_n")
    by_day_with_mntn = by_day_with_mntn.reset_index(drop=True)

    # Getting the name of the most impacted day and the extent of the impact
    if len(by_day_with_mntn):
        most_imp_day_name = by_day_with_mntn["weekday"].iloc[0]
        most_imp_day_val = by_day_with_mntn["status_pct"].iloc[0]
        most_imp_day_val = round(most_imp_day_val, 1)
    else:
        most_imp_day_name, most_imp_day_val = "-", 0

    # Preparing data on the daily likelihood of disruptions, excl. maintenance
    vars_for_group = ["status_en_short", "weekday"]
    by_day_no_mntn = data_to_display.copy()
    by_day_no_mntn["status_en_short"] = np.where(
        by_day_no_mntn["status_en"] == "Closed for maintenance",
        "Normal service",
        by_day_no_mntn["status_en_short"],
    )
    by_day_no_mntn["rows_for_status"] = by_day_no_mntn.groupby(vars_for_group)[
        "date"
    ].transform("count")
    by_day_no_mntn["rows_for_day"] = by_day_no_mntn.groupby("weekday")[
        "date"
    ].transform("count")
    by_day_no_mntn["status_pct"] = 100 * (
        by_day_no_mntn["rows_for_status"] / by_day_no_mntn["rows_for_day"]
    )
    by_day_no_mntn["status_pct"] = np.round(by_day_no_mntn["status_pct"], 1)
    by_day_no_mntn = by_day_no_mntn.drop_duplicates(vars_for_group)
    by_day_no_mntn = by_day_no_mntn[
        by_day_no_mntn["status_en_short"] == "Disruption"
    ].copy()
    by_day_no_mntn = by_day_no_mntn.sort_values("weekday_n")
    by_day_no_mntn = by_day_no_mntn.reset_index(drop=True)

    # Preparing data on the 10 most impacted stations
    most_impacted = st_data_to_display[
        st_data_to_display["status_en_short"] == "Disruption"
    ].copy()
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
    least_impacted = st_data_to_display[
        st_data_to_display["status_en_short"] == "Disruption"
    ].copy()
    least_impacted["n_times_affected"] = least_impacted.groupby("station")[
        "timestamp"
    ].transform("nunique")
    least_impacted = least_impacted.drop_duplicates("station")
    least_impacted = least_impacted.sort_values("n_times_affected")
    least_impacted = least_impacted.iloc[:10]
    least_impacted = least_impacted.sort_values("n_times_affected", ascending=False)

    # Creating a bar chart with the daily likelihood of all kinds of disruptions
    chart_by_day_w_mntn = px.bar(
        by_day_with_mntn,
        x="weekday",
        y="status_pct",
    )
    chart_by_day_w_mntn.update_layout(
        title_text=f"Occurrence (%) of planned disruptions between {selected_period} by weekday"
    )
    chart_by_day_w_mntn.update_xaxes(title_text="Weekday")
    chart_by_day_w_mntn.update_yaxes(title_text="% chance of disruption")

    # Creating a bar chart with the daily likelihood of unplanned disruptions
    chart_by_day_no_mntn = px.bar(
        by_day_no_mntn,
        x="weekday",
        y="status_pct",
    )
    chart_by_day_no_mntn.update_layout(
        title_text=f"Occurrence (%) of unplanned disruptions between {selected_period} by weekday"
    )
    chart_by_day_no_mntn.update_xaxes(title_text="Weekday")
    chart_by_day_no_mntn.update_yaxes(title_text="% chance of disruption")

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

    # Preparing messages describing the charts
    w_mntn_desc = """The chart below shows the daily frequency at which **planned
    disruptions** (i.e. service stops due to maintenance) occur throughout the week.
    As such, it gives an insight into which days maintenance is most likely to
    take place on, enabling us to plan our journey using alternative means of
    transport:"""
    no_mntn_desc = """The chart below shows the daily frequency at which **unplanned
    disruptions** (e.g. due to unexpected technical issues) occur throughout the week.
    As such, it gives an insight into which days are most plagued by service
    issues, enabling us to plan our journey using alternative means of
    transport:"""
    st_most_desc = """The chart below lists the top 10 metro stations most frequently
    impacted by service disruptions. The number shown is the total number of records
    in the data where a disruption at the station was recorded (service status is
    checked once every 10 minutes). As such, it can be useful in finding out which
    stations to avoid so as to minimize the likelihood of suffering from service
    stops. *Please note* that as the names of impacted stations are not always
    explicitly mentioned in the service messages, the actual counts may be higher
    than those presented on the chart:"""
    st_least_desc = """The chart below lists the 10 metro stations least often
    impacted by service disruptions. The number shown is the total number of records
    in the data where a disruption at the station was recorded (service status is
    checked once every 10 minutes). *Please note* that as the names of impacted
    stations are not always explicitly mentioned in the service messages,
    the actual counts may be higher than those presented on the chart:"""

    # Plotting the elements in the correct order,
    # starting with KPIs on top of the page and proceeding with charts
    if mapping_warning:
        st.warning(mapping_warning)
    st.markdown(
        f"""This page contains insights on the **impact caused by service disruptions
            between {selected_period}**, including the frequency with each they occur
            throughout the week, and the names of the most and least
            impacted stations by the disruptions."""
    )

    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Most impacted day", str(most_imp_day_name))
    metric2.metric("Avg disruption % on that day", str(most_imp_day_val) + "%")
    metric3.metric("Most impacted station", str(most_imp_station_name))

    st.subheader("Daily frequency of planned disruptions", divider="rainbow")
    st.markdown(w_mntn_desc)
    plot_or_not(chart_by_day_w_mntn, by_day_with_mntn)
    # st.plotly_chart(chart_by_day_w_mntn)

    st.subheader("Daily frequency of unplanned disruptions", divider="rainbow")
    st.markdown(no_mntn_desc)
    plot_or_not(chart_by_day_no_mntn, by_day_no_mntn)
    # st.plotly_chart(chart_by_day_no_mntn)

    st.subheader("Most impacted stations", divider="rainbow")
    st.markdown(st_most_desc)
    plot_or_not(chart_stations_most, most_impacted)
    # st.plotly_chart(chart_stations_most)

    st.subheader("Least impacted stations", divider="rainbow")
    st.markdown(st_least_desc)
    plot_or_not(chart_stations_least, least_impacted)
    # st.plotly_chart(chart_stations_least)


# %% Page: daily history of service disruptions


def disruption_history():
    st.header("Disruption history")

    # Detecting and confirming slicer selections
    n_days = filter_by_n_days(n_possible_days)
    day_types = filter_by_day_type()
    hour_types = filter_by_hour_type()
    selected_lines = filter_by_line()

    # Filtering and arranging the data
    data_to_display = operation_fmt[
        operation_fmt["date_in_last_n_days"] <= n_days
    ].copy()
    data_to_display = data_to_display[
        data_to_display["day_type"].isin(day_types)
    ].copy()
    data_to_display = data_to_display[
        data_to_display["official_rush_hour"].isin(hour_types)
    ].copy()
    data_to_display = data_to_display[
        data_to_display["line"].isin(selected_lines)
    ].copy()
    data_to_display = data_to_display.sort_values("date")
    data_to_display = data_to_display.reset_index(drop=True)

    # Confirming the selected period
    selected_period, min_date, max_date = get_period_string(data_to_display, "date")
    date_range = pd.date_range(start=min_date, end=max_date)
    date_range = pd.DataFrame(date_range, columns=["date"])
    st.sidebar.markdown(
        f"**Note:** this selection covers the period between {selected_period}."
    )

    # Preparing data for daily disruptions
    daily_disruption = data_to_display.copy()
    vars_for_group = ["status_en_short", "date"]
    daily_disruption["rows_for_status"] = daily_disruption.groupby(vars_for_group)[
        "date"
    ].transform("count")
    daily_disruption["unique_msg"] = daily_disruption.groupby(vars_for_group)[
        "status_dk"
    ].transform("nunique")
    daily_disruption["rows_for_date"] = daily_disruption.groupby("date")[
        "date"
    ].transform("count")
    daily_disruption["status_pct"] = 100 * (
        daily_disruption["rows_for_status"] / daily_disruption["rows_for_date"]
    )
    daily_disruption["avg_disr_dur_hours"] = daily_disruption.groupby(vars_for_group)[
        "disruption_duration_hours"
    ].transform("mean")
    daily_disruption["status_pct"] = np.round(daily_disruption["status_pct"], 1)
    daily_disruption["avg_disr_dur_hours"] = np.round(
        daily_disruption["avg_disr_dur_hours"], 1
    )
    daily_disruption = daily_disruption.drop_duplicates(vars_for_group)
    daily_disruption = daily_disruption[
        daily_disruption["status_en_short"] == "Disruption"
    ].copy()
    daily_disruption = daily_disruption.reset_index(drop=True)
    daily_disruption = pd.merge(date_range, daily_disruption, on="date", how="left")
    daily_disruption = daily_disruption.set_index("date")
    daily_disruption = daily_disruption.asfreq("D")
    daily_disruption = daily_disruption.reset_index()
    daily_disruption["status_pct"] = daily_disruption["status_pct"].fillna(0)
    daily_disruption["unique_msg"] = daily_disruption["unique_msg"].fillna(0)
    daily_disruption["avg_disr_dur_hours"] = daily_disruption[
        "avg_disr_dur_hours"
    ].fillna(0)

    # Preparing data for the N of stations impacted by day
    n_total_stations = len(mapping_stations["station"].unique())
    daily_disr_stations = data_to_display.copy()
    daily_disr_stations["avg_n_impacted"] = daily_disr_stations.groupby("date")[
        "n_impacted_stations"
    ].transform("mean")
    daily_disr_stations["avg_pct_impacted"] = 100 * (
        daily_disr_stations["avg_n_impacted"] / n_total_stations
    )
    daily_disr_stations["avg_pct_impacted"] = np.round(
        daily_disr_stations["avg_pct_impacted"], 1
    )
    daily_disr_stations = daily_disr_stations.drop_duplicates("date")
    daily_disr_stations = daily_disr_stations.reset_index(drop=True)

    # Preparing KPI metrics for the page
    total_disruptions = int(daily_disruption["unique_msg"].sum())
    n_days = len(daily_disruption["date"].unique())
    avg_per_day = total_disruptions / n_days
    avg_duration = daily_disruption["avg_disr_dur_hours"].mean()

    # Rounding off numbers to be used as KPIs
    total_disruptions = round(total_disruptions, 0)
    avg_per_day = round(avg_per_day, 1)
    avg_duration = round(avg_duration, 1)

    # Creating a bar chart that shows N of service disruptions per day
    n_disr_chart = px.bar(
        daily_disruption,
        x="date",
        y="unique_msg",
    )
    n_disr_chart.update_layout(
        title_text=f"Disruptions per day during the last {n_days} days"
    )
    n_disr_chart.update_xaxes(title_text="Date")
    n_disr_chart.update_yaxes(title_text="Disruptions per day")

    # Creating a line chart that shows service disruptions as % of time
    pct_disr_chart = px.line(
        daily_disruption,
        x="date",
        y="status_pct",
    )
    pct_disr_chart.update_layout(
        title_text=f"Disruptions in the metro's service in the last {n_days} days, % of time"
    )
    pct_disr_chart.update_xaxes(title_text="Date")
    pct_disr_chart.update_yaxes(title_text="Disruptions, % of time")

    # Creating a line chart that shows the average duration of the disruptions
    h_disr_chart = px.line(
        daily_disruption,
        x="date",
        y="avg_disr_dur_hours",
    )
    h_disr_chart.update_layout(
        title_text=f"Average duration of disruptions in the last {n_days} days (measured in hours)"
    )
    h_disr_chart.update_xaxes(title_text="Date")
    h_disr_chart.update_yaxes(title_text="Average duration (hours)")

    # Creating a line chart that shows average % of stations impacted by day
    stations_chart_pct = px.line(
        daily_disr_stations,
        x="date",
        y="avg_pct_impacted",
    )
    stations_chart_pct.update_layout(
        title_text=f"Average % of impacted stations in the last {n_days} days"
    )
    stations_chart_pct.update_xaxes(title_text="Date")
    stations_chart_pct.update_yaxes(title_text="Average % of stations impacted")

    # Creating a bar chart that shows average N of stations impacted by day
    stations_chart = px.bar(
        daily_disr_stations,
        x="date",
        y="avg_n_impacted",
    )
    stations_chart.update_layout(
        title_text=f"Average number of impacted stations in the last {n_days} days"
    )
    stations_chart.update_xaxes(title_text="Date")
    stations_chart.update_yaxes(title_text="Average number of stations impacted")

    # Preparing messages describing the charts
    n_desc = """The chart below shows the number of unique service messages
    that can be classified as disruptions. As such, it can be used as an indicator
    of how many things went wrong on any given day:"""
    pct_desc = """The chart below shows the % of the time where a service
    message classified as disruption was displayed (checks are made once every 10
    minutes). As such, it can be used as an indicator of approximately how much of
    the day was plagued by disruptions relative to normally running service:"""
    h_desc = """The chart below shows the average duration of the various service
    disruptions measured in hours. As such, it can be used as an indicator of
    whether things were broken for a relatively short time or not:"""
    stations_desc_pct = """The chart below shows the average % of stations which were
    impacted by disruptions in the given period. As such, it can be used to
    evaluate the magnitude of the disruptions, however, it is important to note that
    impacted stations are not always mentioned in the service messages, so the share
    as presented below will probably be underestimated:"""
    stations_desc = """The chart below shows essentially the *same information* but
    rather than looking at the average percentage of impacted stations, we look at
    the actual number:"""

    # Plotting the elements in the correct order,
    # starting with KPIs on top of the page and proceeding with charts
    if mapping_warning:
        st.warning(mapping_warning)
    st.markdown(
        f"""This page shows a full daily history of **service disruptions between
            {selected_period}**, including the share of the time where service
            was running normally, the number and duration of all disruptions
            as well as the top reasons behind them."""
    )

    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Total disruptions", str(total_disruptions))
    metric2.metric("Average disruptions per day", str(avg_per_day))
    metric3.metric("Average duration", str(avg_duration) + "h")

    st.subheader("Number of disruptions", divider="rainbow")
    st.markdown(n_desc)
    plot_or_not(n_disr_chart, daily_disruption)
    # st.plotly_chart(n_disr_chart)

    st.subheader("Disruptions as % of time", divider="rainbow")
    st.markdown(pct_desc)
    plot_or_not(pct_disr_chart, daily_disruption)
    # st.plotly_chart(pct_disr_chart)

    st.subheader("Duration of disruptions", divider="rainbow")
    st.markdown(h_desc)
    plot_or_not(h_disr_chart, daily_disruption)
    # st.plotly_chart(h_disr_chart)

    st.subheader("Impacted stations", divider="rainbow")
    st.markdown(stations_desc_pct)
    plot_or_not(stations_chart_pct, daily_disr_stations)
    # st.plotly_chart(stations_chart_pct)

    st.markdown(stations_desc)
    plot_or_not(stations_chart, daily_disr_stations)
    # st.plotly_chart(stations_chart)


# %% Disruption calculator page


def disruption_calc():
    st.header("Disruption calculator")

    # Printing more info to the user on how to use this page
    st.markdown(
        """On this page, you can calculate the **probability that you will
    be impacted by service disruptions** depending on which station you intend on
    using as well as on when you intend on travelling."""
    )
    st.warning(
        """Note: Please use the filters in the sidebar to make the
        calculations more relevant for your trip."""
    )

    # Collecting input data from the user from custom sidebar filters
    selected_station = st.sidebar.selectbox(
        "Station", unique_stations, index=unique_stations.index("Kongens Nytorv")
    )
    selected_hour = st.sidebar.selectbox(
        "Time of travel", unique_intervals, index=unique_intervals.index("07-08")
    )
    selected_day = st.sidebar.selectbox(
        "Day of travel", unique_day_names, index=unique_day_names.index("Monday")
    )
    selected_n_days = st.sidebar.select_slider(
        "Number of recent days to base the probability calculations off",
        n_possible_days,
        value=30,  # last 30 days by default
    )

    # Aggregating the data by day, hour and station
    specific_impact = station_impact[
        station_impact["date_in_last_n_days"] <= selected_n_days
    ].copy()
    specific_impact = specific_impact[specific_impact["hour_interval"] == selected_hour]
    specific_impact = specific_impact[specific_impact["weekday"] == selected_day]
    specific_impact["disruption"] = specific_impact["status_en_short"] == "Disruption"
    specific_impact["total_rows"] = specific_impact.groupby(["station"])[
        "status_dk"
    ].transform("count")
    specific_impact["disruption_rows"] = specific_impact.groupby(["station"])[
        "disruption"
    ].transform("sum")
    specific_impact["disruption_chance_pct"] = np.round(
        100 * (specific_impact["disruption_rows"] / specific_impact["total_rows"]), 1
    )
    specific_impact = specific_impact.drop_duplicates(subset="station")

    # Sorting the stations by the likelihood of disruptions
    specific_impact = specific_impact.sort_values(
        "disruption_chance_pct", ascending=False
    )
    specific_impact = specific_impact.reset_index(drop=True)

    # Exctracting info on the filtered time period
    min_date = specific_impact["timestamp"].min().strftime("%d %B %Y")
    max_date = specific_impact["timestamp"].max().strftime("%d %B %Y")

    # Extracting key numbers to show as output
    disruption_pct_selected = specific_impact[
        specific_impact["station"] == selected_station
    ]["disruption_chance_pct"].iloc[0]
    disruption_pct_most = specific_impact[
        specific_impact["disruption_chance_pct"]
        == np.max(specific_impact["disruption_chance_pct"])
    ]["disruption_chance_pct"].iloc[0]
    disruption_name_most = specific_impact[
        specific_impact["disruption_chance_pct"]
        == np.max(specific_impact["disruption_chance_pct"])
    ]["station"].iloc[0]
    disruption_pct_least = specific_impact[
        specific_impact["disruption_chance_pct"]
        == np.min(specific_impact["disruption_chance_pct"])
    ]["disruption_chance_pct"].iloc[0]
    disruption_name_least = specific_impact[
        specific_impact["disruption_chance_pct"]
        == np.min(specific_impact["disruption_chance_pct"])
    ]["station"].iloc[0]

    # Printing the results to the user
    st.subheader(f"Chances of disruption at {selected_station}", divider="rainbow")
    st.markdown(
        f"""Based on historical data covering the period between **{min_date}
        and {max_date}**, it can be concluded that:"""
    )
    st.markdown(
        f"""
    - The chance of disruption at **{selected_station}** on **{selected_day}s** 
    between **{selected_hour} o'clock** is {disruption_pct_selected}%.
    - During the same time, the station **most likely** to experience disruption
    is **{disruption_name_most}** ({disruption_pct_most}%), while the station
    **least likely** to suffer from disruption is **{disruption_name_least}**
    ({disruption_pct_least}%).
    - *Please note* that the numbers below are calculated based on
    historical data and that it is not guaranteed that historical patterns will
    be repeated in the future (or on any particular given day).
    """
    )


# %% Info on data sources and method page


def method_info():
    st.header("Information on data collection & processing")
    st.markdown(
        """
    This page describes how the data that serves as the backbone of this app is
    collected from the **source** and what kind of **assumptions** are made in the
    calculations.
    """
    )
    st.subheader("Sourcing operational data", divider="rainbow")
    st.markdown(
        """The Copenhagen metro's [website](www.m.dk) provides **real-time
    information** on their operating status in the form of an **on-screen banner**."""
    )
    st.markdown(
        """
    - Data on the metro's operational status is sourced from their website
    **once every 10 minutes**, producing a total of up to 6 records per hour.
    In theory, the data could be downloaded every minute, giving us an even
    greater level of detail, however, the decision to limit the fetching to once
    every 10 minutes was made out of consideration for limiting the impact on
    the server side.
    - Data is fetched for each metro line where the relevant **status message**
    is recorded alongside a **timestamp** showing when the check was made.
    - Any newly downloaded data is **appended to a table** containing all previous
    historical records (this table is then further processed by adding custom
    calculated columns and such).
    - In some cases, it may not be possible to fetch any new data. Should that be
    the case, an **"Unknown" status** will be assigned to the respective timestamp.
    - When the data is aggregated to **calculate % of time** for e.g. normally
    running service or disruption, we divide the number of entries in each status
    group by the total number of entries in the selected time period. Any duration-
    related metrics are therefore **approximations rather than exact numbers**.
    - While it is only the metro team that has access to the most fine-grained data
    and therefore able to provide the exact numbers, due to the data in here being
    sourced at frequent and regular intervals, the **approximation** of the actual
    picture they provide can be considered to be **very good**.
    """
    )

    st.subheader("Interpreting status messages", divider="rainbow")
    st.markdown(
        """
    A separate **mapping table** that allows us to group the "raw" status messages
    into separate categories (e.g. "Normal service" or "Disruption") and that
    allows for recording the impact on stations in a standardised manner is
    **maintained manually**.
    """
    )
    st.markdown(
        """
    - Status messages are mapped by the author of this app approximately **once
    a week**. If there are any unmapped service messages, a **banner** will be displayed in the app, informing the user of the potential impact of the
    missing mapping (typically, missing mapping will lead to **more "Unknown"
    entries**, therefore underestimating the % of time where the metro was either
    running according to schedule or suffering from a disruption).
    - For the sake of **transparency**, the entire mapping table is printed below:
    """
    )
    st.dataframe(mapping_messages)


# %% Compulsory legal disclaimer


def legal_info():
    st.header("Legal disclaimer")
    st.markdown(
        """
    This application is intended for educational purposes only and is designed to 
    benefit the general public.
    
    **By using this application, you acknowledge and agree to the following terms**:
                
    1. The app **gives an insight** into the quality of service provided by the
    Copenhagen metro as measured through the reliability of its operation.
    2. The intent of the author is to **empower the general public** (which 
    provides funding for the service) and **decision-makers** (who may take
    measures to improve the quality of service).
    3. Due to the nature of the data collection, the numbers provided in this app
    are **approximations of the true numbers**, the latter only being in the
    possession of the Copenhagen metro team.
    4. The data is collected through the use of **web scraping**, where the metro's
    website is accessed once every 10 minutes so that the status messages can be
    recorded. By doing this, the author has strived to strike a balance between the 
    need to have fine-grained data and the duty to use public resources in a 
    responsible manner by not overwhelming the server.
    5. The data and insights provided by this application are **not intended for
    commercial use**.
    6. While every effort has been made to ensure the accuracy and reliability of
    the data, the creator of this application **does not guarantee** the accuracy,
    completeness, or suitability of the data for any particular purpose.
    7. The creator **shall not be held liable for** any loss, damage,
    or inconvenience arising as a consequence of any use of or the inability to
    use any information provided by this application.

    These terms were last revised on 02 April 2024.
"""
    )


# %% Allowing the user to switch between pages in the app

# Based on the page selected by the end user
if options == "Welcome":
    show_homepage()
elif options == "Overview":
    general_overview()
elif options == "Disruption reasons":
    disruption_reasons()
elif options == "Disruption impact":
    disruption_impact()
elif options == "Disruption history":
    disruption_history()
elif options == "Disruption calculator":
    disruption_calc()
elif options == "Info on data sources":
    method_info()
elif options == "Legal disclaimer":
    legal_info()
