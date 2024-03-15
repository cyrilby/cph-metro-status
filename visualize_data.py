"""
==================================================
App visualizing the CPH Metro's operational status
==================================================

Author: kirilboyanovbg[at]gmail.com
Last meaningful update: 15-03-2024

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
    ],
)


# Setting up sidebar filter for number of days to show
def filter_by_n_days(list_of_days):
    selected_n_days = st.sidebar.select_slider(
        "Number of recent days to show",
        list_of_days,
        value=np.max(list_of_days),
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

# Defining the app's welcome message
welcome_message = "This app presents information on the Copenhagen Metro's operational status over time, allowing to measure the impact of the disruptions caused to passengers on different lines, stations and different times of day."


# Informs the user of the app's purpose
def show_homepage(msg):
    n_days = filter_by_n_days(n_possible_days)
    st.header("Welcome to the CPH metro status app!")
    st.write(msg)
    st.markdown(
        f"""
                - The app contains operational data covering up to {n_days} days.
                - The latest date with available data is as of {last_update_date}.
                """
    )

    # Displaying a warning that the app is still under development
    st.subheader("This app is a work in progress")
    st.warning(
        """Warning: Please note that this app is still under active
               development as of 14 March 2024, so some errors and bugs are
               may occur during its use. Please check back again soon for
               the release of fixes and new features."""
    )

    # Displaying the latest service message from the metro's website
    st.subheader("Latest service status data")
    st.markdown(
        f"""The **latest data** on the CPH metro's service status were
            collected on {last_update_date} at {last_update_time} o'clock.
            A preview of the latest status for each line is shown below:"""
    )
    st.dataframe(most_recent)

    # Note to self: Replace the image below with a series of cards that help
    #           the user navigate to a relevant page for answering a specific
    #          question [WIP as of 12-03-2024]."""
    #
    # st.image("resources/app_image.jpg")  # image can be disabled in development phase


# %% Page: General service overview


def general_overview():
    st.header("Overview")

    # Detecting and confirming slicer selections
    n_days = filter_by_n_days(n_possible_days)
    day_types = filter_by_day_type()
    selected_lines = filter_by_line()

    # Filtering and arranging the data
    data_to_display = operation_fmt[
        operation_fmt["date_in_last_n_days"] <= n_days
    ].copy()
    data_to_display = data_to_display[
        data_to_display["day_type"].isin(day_types)
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
    st.markdown(
        f"""This page shows information on the **overall status of the metro
        between {selected_period}**, including the share of the time where service
        was running normally or with disruptions."""
    )

    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Normal service, pct of time", str(pct_normal_service) + "%")
    metric2.metric("Disrupted service, pct of time", str(pct_disruption) + "%")
    metric3.metric("Unknown status, pct of time", str(pct_unknown) + "%")

    st.subheader("Overall service status")
    st.markdown(overall_desc)
    plot_or_not(overall_chart, overall_split)
    # st.plotly_chart(overall_chart)

    st.subheader("Detailed service status")
    st.markdown(detailed_desc)
    plot_or_not(detailed_chart, detailed_split)
    # st.plotly_chart(detailed_chart)


# %% Page: service disruption insights


def disruption_reasons():
    st.header("Disruption reasons")

    # Detecting and confirming slicer selections
    n_days = filter_by_n_days(n_possible_days)
    day_types = filter_by_day_type()
    selected_lines = filter_by_line()

    # Filtering and arranging the data
    data_to_display = operation_fmt[
        operation_fmt["date_in_last_n_days"] <= n_days
    ].copy()
    data_to_display = data_to_display[
        data_to_display["day_type"].isin(day_types)
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
    st.markdown(
        f"""This page contains insights on the **kinds of service disruptions between
            {selected_period}**, including the reasons behind those disruptions and
            some the CPH metro team's major impediments to running a normal service."""
    )

    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Maintenance", str(pct_maintn) + "%")
    metric2.metric("Running with delays", str(pct_delay) + "%")
    metric3.metric("Complete service disruptions", str(pct_stop) + "%")

    st.subheader("Kinds of disruptions")
    st.markdown(status_desc)
    plot_or_not(status_chart, detailed_status)
    # st.plotly_chart(status_chart)

    st.subheader("Reasons behind service disruptions")
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
    selected_lines = filter_by_line()

    # Filtering and arranging the operations data
    data_to_display = operation_fmt[
        operation_fmt["date_in_last_n_days"] <= n_days
    ].copy()
    data_to_display = data_to_display[
        data_to_display["day_type"].isin(day_types)
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
        by_day_with_mntn["status_en"] == "Maintenance/bus replacement",
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
    by_day_with_mntn = by_day_with_mntn.drop_duplicates(vars_for_group)
    by_day_with_mntn = by_day_with_mntn[
        by_day_with_mntn["status_en_short"] == "Disruption"
    ].copy()
    by_day_with_mntn = by_day_with_mntn.sort_values("weekday_n")
    by_day_with_mntn = by_day_with_mntn.reset_index(drop=True)

    # Preparing data on the daily likelihood of disruptions, excl. maintenance
    vars_for_group = ["status_en_short", "weekday"]
    by_day_no_mntn = data_to_display.copy()
    by_day_no_mntn["status_en_short"] = np.where(
        by_day_no_mntn["status_en"] == "Maintenance/bus replacement",
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
    st.markdown(
        f"""This page contains insights on the **impact caused by service disruptions
            between {selected_period}**, including the frequency with each they occur
            throughout the week, and the names of the most and least
            impacted stations by the disruptions."""
    )

    st.warning(
        "WIP as of 13-03-2024: page is kind of done but I would like to include some KPIs on top to keep the design consistent with the remaining pages..."
    )

    st.subheader("Daily frequency of planned disruptions")
    st.markdown(w_mntn_desc)
    plot_or_not(chart_by_day_w_mntn, by_day_with_mntn)
    # st.plotly_chart(chart_by_day_w_mntn)

    st.subheader("Daily frequency of unplanned disruptions")
    st.markdown(no_mntn_desc)
    plot_or_not(chart_by_day_no_mntn, by_day_no_mntn)
    # st.plotly_chart(chart_by_day_no_mntn)

    st.subheader("Most impacted stations")
    st.markdown(st_most_desc)
    plot_or_not(chart_stations_most, most_impacted)
    # st.plotly_chart(chart_stations_most)

    st.subheader("Least impacted stations")
    st.markdown(st_least_desc)
    plot_or_not(chart_stations_least, least_impacted)
    # st.plotly_chart(chart_stations_least)


# %% Page: daily history of service disruptions


def disruption_history():
    st.header("Disruption history")

    # Detecting and confirming slicer selections
    n_days = filter_by_n_days(n_possible_days)
    day_types = filter_by_day_type()
    selected_lines = filter_by_line()

    # Filtering and arranging the data
    data_to_display = operation_fmt[
        operation_fmt["date_in_last_n_days"] <= n_days
    ].copy()
    data_to_display = data_to_display[
        data_to_display["day_type"].isin(day_types)
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

    st.subheader("Number of disruptions")
    st.markdown(n_desc)
    plot_or_not(n_disr_chart, daily_disruption)
    # st.plotly_chart(n_disr_chart)

    st.subheader("Disruptions as % of time")
    st.markdown(pct_desc)
    plot_or_not(pct_disr_chart, daily_disruption)
    # st.plotly_chart(pct_disr_chart)

    st.subheader("Duration of disruptions")
    st.markdown(h_desc)
    plot_or_not(h_disr_chart, daily_disruption)
    # st.plotly_chart(h_disr_chart)

    st.subheader("Impacted stations")
    st.markdown(stations_desc_pct)
    plot_or_not(stations_chart_pct, daily_disr_stations)
    # st.plotly_chart(stations_chart_pct)

    st.markdown(stations_desc)
    plot_or_not(stations_chart, daily_disr_stations)
    # st.plotly_chart(stations_chart)


# %% Allowing the user to switch between pages in the app

# Based on the page selected by the end user
if options == "Welcome":
    show_homepage(welcome_message)
elif options == "Overview":
    general_overview()
elif options == "Disruption reasons":
    disruption_reasons()
elif options == "Disruption impact":
    disruption_impact()
elif options == "Disruption history":
    disruption_history()
