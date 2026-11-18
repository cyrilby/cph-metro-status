"""
============================================
Helpers used by the various pages in the app
============================================

Author: github.com/cyrilby
Last meaningful update: 14-10-2026
"""

# %% Setting up

import numpy as np
import pandas as pd
import streamlit as st
import yaml

from core.storage import get_app_data


@st.cache_data
def load_text():
    with open("text.yaml", "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


conn = get_app_data()
text = load_text()

# %% Extracting unique values


def get_unique_days():

    # Extracting values from the data to be used in slicers etc.
    unique_vals = conn.execute(
        "SELECT DISTINCT date, date_in_last_n_days FROM operation_fmt"
    ).df()

    unique_dates = unique_vals["date"].unique().tolist()
    unique_last_n_days = unique_vals["date_in_last_n_days"].unique().tolist()

    n_days = len(unique_dates)
    n_possible_days = np.arange(1, n_days + 1)

    # Preparing a list of (N of) days to use as filter in the app
    unique_dates.sort(reverse=True)
    unique_last_n_days.sort()

    return unique_dates, unique_last_n_days, n_days, n_possible_days


def get_latest_status():
    # Getting info on the most recent date with data
    query = """
    SELECT
        timestamp,
        line,
        status_en,
        status_dk
    FROM operation_fmt
    WHERE 1=1
        AND timestamp = (
            SELECT MAX(timestamp)
            FROM operation_fmt)
    ORDER BY line;
    """
    most_recent = conn.execute(query).df()

    last_update = most_recent["timestamp"][0]
    last_update_date = last_update.strftime("%d %B %Y")
    last_update_time = last_update.strftime("%H:%M")

    return most_recent, last_update_date, last_update_time


def get_unique_stations() -> np.ndarray:

    # Extracting the unique names of potentially impacted stations
    query = """
    SELECT DISTINCT station
    FROM station_impact
    ORDER BY station;
    """
    unique_stations = conn.execute(query).fetchnumpy()

    return unique_stations


def get_unique_intervals() -> np.ndarray:

    # Getting unique intervals and days of time travel
    query = """
    SELECT DISTINCT hour_interval
    FROM station_impact
    ORDER BY hour_interval;
    """
    unique_intervals = conn.execute(query).fetchnumpy()

    return unique_intervals


def get_unique_day_names() -> list:

    unique_day_names = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]

    return unique_day_names


def deal_with_missing_mapping() -> None:

    # Importing user-maintained mapping tables
    # Note: these are loaded directly from the database
    query = "SELECT * FROM mapping_messages"
    mapping_status = conn.execute(query).df()

    # Detecting whether there are any unmapped service status messages
    # as well as the date(s) where data accuracy may be impacted due to lacking mapping
    query = """
    SELECT DISTINCT
        date,
        status_en,
        status_dk
    FROM operation_fmt
    WHERE status_dk NOT IN ?
    """
    mapped_entries = mapping_status["status_dk"].tolist()
    unmapped_msg = conn.execute(query, [mapped_entries]).df()

    query_total_rows = """
    SELECT COUNT(*)
    FROM operation_fmt;
    """
    unmapped_rows = len(unmapped_msg)
    total_rows = conn.execute(query_total_rows).fetchone()[0]
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
            mapping_warning = text["warning_mapping_many_dates"]
            mapping_warning = mapping_warning.format(
                unmapped_msg_n=unmapped_msg_n,
                unmapped_msg_date_min=unmapped_msg_date_min,
                unmapped_msg_date_max=unmapped_msg_date_max,
                unmappped_rows_pct=unmappped_rows_pct,
            )
        else:
            mapping_warning = text["warning_mapping_one_date"]
            mapping_warning = mapping_warning.format(
                unmapped_msg_n=unmapped_msg_n,
                unmapped_msg_date_max=unmapped_msg_date_max,
                unmappped_rows_pct=unmappped_rows_pct,
            )
    else:
        mapping_warning = None

    if mapping_warning:
        st.warning(unmapped_msg)


def deal_with_downtime(data_to_display: pd.DataFrame, selected_rows: list):
    """
    Filters the data based on excluding/including periods with system downtime.
    Prepares a message for the end user describing the situation and the filtering.
    """
    # Noting down if periods of system downtime are excluded
    downtime_days = len(
        data_to_display[data_to_display["system_downtime"]]["date"].unique()
    )
    data_to_display = data_to_display[
        data_to_display["system_downtime"].isin(selected_rows)
    ].copy()
    downtime_days_shown = len(
        data_to_display[data_to_display["system_downtime"]]["date"].unique()
    )

    # Generating a message reg. the presence of downtime for the end user
    if downtime_days > downtime_days_shown:
        downtime_msg = text["downtime_msg_hidden"]
        downtime_msg = downtime_msg.format(downtime_days=downtime_days)
    elif downtime_days:
        downtime_msg = text["downtime_msg_shown"]
        downtime_msg = downtime_msg.format(downtime_days=downtime_days)
    else:
        downtime_msg = downtime_msg = text["downtime_msg_none"]

    return data_to_display, downtime_msg


def get_period_string(min_date: pd.Timestamp, max_date: pd.Timestamp):
    """
    Uses a date field to get the period covered by the data
    that's being presented to the user.
    """

    min_date_str = min_date.strftime("%d %b %Y")
    max_date_str = max_date.strftime("%d %b %Y")
    selected_period = min_date_str + " - " + max_date_str
    return selected_period


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
        warning_text = text["warning_chart_no_data"]
        st.warning(warning_text)


def colors_for_calplot(data: pd.DataFrame) -> dict:
    """
    Gets all unique combinations of daily service statuses covered
    by the input data and returns a dictionary containing the right
    colors depending on what combinations are available based on the
    following values of the daily status variable:
    -1: should always be gray
    0: should always be green
    1: should always be yellow
    2: should always be red
    """

    # Specifying the colors to use on the chart
    gray = "#ededed"
    green = "#28b09c"
    yellow = "#fed16a"
    red = "#fe2b2a"

    # Specifying all possible combinations of statuses and desired colors
    desired_colors = {
        (-1,): [[0, gray], [1, gray]],
        (0,): [[0, green], [1, green]],
        (1,): [[0, yellow], [1, yellow]],
        (2,): [[0, red], [1, red]],
        (-1, 0): [[0, gray], [1, green]],
        (-1, 1): [[0, gray], [1, yellow]],
        (-1, 2): [[0, gray], [1, red]],
        (0, 1): [[0, green], [1, yellow]],
        (0, 2): [[0, green], [1, red]],
        (1, 2): [[0, yellow], [1, red]],
        (-1, 0, 1): [[0, gray], [0.5, green], [1, yellow]],
        (-1, 0, 2): [[0, gray], [0.5, green], [1, red]],
        (-1, 1, 2): [[0, gray], [0.5, yellow], [1, red]],
        (0, 1, 2): [[0, green], [0.5, yellow], [1, red]],
        (-1, 0, 1, 2): [[0, gray], [0.33, green], [0.66, yellow], [1, red]],
    }

    # Getting the unique values of the daily disruption score
    unique_scores = tuple(data["disruption_score"].sort_values().unique().tolist())

    # Returning the appropriate combination of colors to use on the chart
    return desired_colors.get(unique_scores, [])


# %%
