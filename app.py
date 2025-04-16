"""
==================================================
App visualizing the CPH Metro's operational status
==================================================

Author: kirilboyanovbg[at]gmail.com
Last meaningful update: 16-04-2025

This script contains the source code of the Streamlit
app accompanying the CPH metro scraper tool. In here,
we create a series of data visualizations that help us
get a better understanding of how often the metro breaks
down, when and where the impact is felt as well as what
the reasons behind the breakdowns are (if information
on those is available).
"""

# %% Setting things up

# Importing relevant packages
import pandas as pd
import numpy as np
import datetime as dt
import streamlit as st
import plotly.express as px
import plotly.io as pio
from plotly_calplot import calplot
import yaml

# Importing long text strings used in the app
with open("text.yaml", "r", encoding="utf-8") as file:
    text = yaml.safe_load(file)

# Importing links to mapping tables
with open("mapping_links.yaml", "r", encoding="utf-8") as file:
    mapping_links = yaml.safe_load(file)


# %% HTML and color customization

# Creating a custom color palette with the MG colors
# Using the "Classy" palette from: https://mycolor.space/?hex=%231EA2B5&sub=1
my_custom_palette = ["#1ea2b5", "#324b4f", "#95b0b5", "#9f8ac3", "#6b588d"]
my_template = pio.templates["plotly_white"].layout.template
my_template.layout.colorway = my_custom_palette
pio.templates["my_custom_template"] = my_template
pio.templates.default = "my_custom_template"


# Replacing the line on top of the app with a custom color
# Replace the gradient with a solid color (e.g., blue)
def customize_colors():
    """
    Allows for customization of streamlit top bar and the color
    of the links, which are both not directly editable via Python.
    Instead, this function uses HTML-based CSS injection to make
    the required changes.
    """

    # Change the color of the top bar
    st.markdown(
        """
        <style>
            div[data-testid="stDecoration"] {
                background: #1ea2b5;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Change the color of links (a href)
    st.markdown(
        """
        <style>
        div[data-testid="stMarkdownContainer"] a {
            color: #1ea2b5 !important;
        }
        div[data-testid="stMarkdownContainer"] a:hover {
            color: #1ea2b5 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# %% Importing data for use in the app

# Importing pre-processed data & relevant mapping tables from Azure
operation_fmt = pd.read_parquet(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/operation_fmt.parquet"
)
station_impact = pd.read_parquet(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/station_impact.parquet"
)
mapping_stations = pd.read_pickle(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/mapping_stations.pkl"
)
mapping_messages = pd.read_pickle(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/mapping_messages.pkl"
)
system_downtime = pd.read_csv(
    mapping_links["system_downtime"],
    parse_dates=["date", "last_modified"],
    date_format="%d/%m/%Y",
)

# Correcting dtypes
operation_fmt["date"] = pd.to_datetime(operation_fmt["date"])

# Getting the first & last date in the data (to use for filtering)
max_date_filter = operation_fmt["date"].max()
min_date_filter = operation_fmt["date"].min()

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

# Deprecated on 13-03-2025 as we transition to the new date picker
# # Setting up sidebar filter for number of days to show
# def filter_by_n_days(list_of_days):
#     selected_n_days = st.sidebar.select_slider(
#         "Number of recent days to show",
#         list_of_days,
#         value=30,  # showing last 30 days by default
#     )
#     return selected_n_days


# Sidebar filter that allows picking an interval between dates
def filter_by_date_range():
    # Default date range: Last 30 days
    default_start = max_date_filter - dt.timedelta(days=30)
    default_end = max_date_filter

    # Create a date input for start and end date
    selected_dates = st.sidebar.date_input(
        "Selected time period",
        (default_start, default_end),
        min_value=min_date_filter,
        max_value=max_date_filter,
        format="DD/MM/YYYY",
    )

    # If only 1 date is selected, we treat it as a start date
    # and set the end date to be 30 days later
    if len(selected_dates) == 0:
        start_date = default_start
        end_date = default_end
    elif len(selected_dates) == 1:
        start_date = selected_dates[0]
        end_date = start_date + dt.timedelta(days=30)
    else:
        start_date = selected_dates[0]
        end_date = selected_dates[1]

    # Convert dates to pandas datetime
    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)
    selected_start = start_date.strftime("%d %b %Y")
    selected_end = end_date.strftime("%d %b %Y")
    selected_period = (
        f"**Note:** showing data for the period {selected_start} - {selected_end}."
    )
    st.sidebar.markdown(selected_period)

    return start_date, end_date


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


# Setting up sidebar filter for system downtime exclusion
def filter_downtime():
    possible_choices = ["Including system downtime", "Excluding system downtime"]
    selected_choice = st.sidebar.selectbox(
        "Showing data",
        possible_choices,
        index=possible_choices.index("Including system downtime"),
    )
    if selected_choice == "Including system downtime":
        end_choice = [True, False]
    else:
        end_choice = [False]
    return end_choice


# Setting up a function that adds the MindGraph logo
def add_logo():
    """
    Adds the MindGraph logo to the upper left corner of the page.
    """
    st.logo(
        "https://mindgraph.dk/logo.svg",
        size="large",
        link="https://mindgraph.dk",
        icon_image="https://mindgraph.dk/favicon.svg",
    )


# %% Defining further custom functions for the app


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


# %% Page: Welcome to the app


# Informs the user of the app's purpose
def show_homepage():
    customize_colors()
    st.header("Welcome to the CPH metro status app!")
    add_logo()
    st.markdown(text["home_msg_1"])
    st.markdown(text["home_msg_2"])
    st.image(
        "resources/metro_map_with_stations.png"
    )  # image can be disabled in development phase

    # Displaying the latest service message from the metro's website
    st.subheader("Latest service status data", divider="grey")
    if mapping_warning:
        st.warning(mapping_warning)

    msg_text = text["home_msg_3"]
    msg_text = msg_text.format(
        last_update_date=last_update_date, last_update_time=last_update_time
    )
    st.markdown(msg_text)
    st.dataframe(most_recent)

    # Displaying more info on how the app can help the user
    st.subheader("How this app can help you", divider="grey")
    st.markdown(text["home_msg_4"])
    msg_text = text["home_msg_5"]
    msg_text = msg_text.format(n_days=n_days)
    st.markdown(msg_text)

    # Displaying more info on how to use the app
    st.subheader("How to use this app", divider="grey")
    st.markdown("This app consists of the following two panes:")
    st.markdown(text["home_msg_6"])
    # st.markdown(
    #     "To **switch between different pages**, please click on the page title:"
    # )
    st.image("resources/pages_examples.PNG")
    st.markdown("You will then be redirected to the desired page.")

    # Displaying more info on how the user can filter the data
    st.subheader("How to apply filters to the data", divider="grey")
    st.markdown(text["home_msg_7"])
    st.image("resources/filter_date.PNG")
    st.markdown(text["home_msg_8"])
    st.image("resources/filter_lines.PNG")
    st.markdown(text["home_msg_9"])
    st.image("resources/selection_warning.PNG")
    st.markdown(text["home_msg_10"])
    st.divider()
    st.markdown("*Front page image source: Metroselskabet (www.m.dk)*")


# %% Page: General service overview


def general_overview():
    customize_colors()
    st.header("Overview")
    add_logo()

    # Detecting and confirming slicer selections
    min_date, max_date = filter_by_date_range()
    day_types = filter_by_day_type()
    hour_types = filter_by_hour_type()
    selected_lines = filter_by_line()
    selected_rows = filter_downtime()

    # Filtering and arranging the data
    data_to_display = operation_fmt[
        (operation_fmt["date"] >= min_date) & (operation_fmt["date"] <= max_date)
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

    # Dealing with periods of system downtime
    data_to_display, downtime_msg = deal_with_downtime(data_to_display, selected_rows)

    # Confirming the selected period
    selected_period, min_date, max_date = get_period_string(data_to_display, "date")
    date_range = pd.date_range(start=min_date, end=max_date)
    date_range = pd.DataFrame(date_range, columns=["date"])
    st.sidebar.markdown(
        f"**Note:** this selection covers the period between {selected_period}."
        + downtime_msg
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

    # Preparing data on daily disruption score where the score means:
    # 0 = no disruptions recorded/status "Unknown" only
    # 1 = at least 1 partial disruption recorded during that day
    # 2 = at least 1 complete disruption recorded during that day
    cal_data = data_to_display.copy()
    cal_data["status_unknown"] = cal_data["status_en"] == "Unknown"
    cal_data["normal_service"] = cal_data["status_en"] == "Normal service"
    cal_data["complete_disruption"] = (
        cal_data["status_en"] == "Complete service disruption"
    )
    cal_data["partial_disruption"] = ~cal_data["status_en"].isin(
        [
            "Normal service",
            "Complete service disruption",
            "Closed for maintenance",
        ]
    )
    cal_data["disruption_score"] = np.where(
        cal_data["normal_service"],
        0,
        np.where(
            cal_data["partial_disruption"],
            1,
            np.where(cal_data["complete_disruption"], 2, 0),
        ),
    )
    cal_data["disruption_score"] = cal_data.groupby("date")[
        "disruption_score"
    ].transform("max")
    cal_data["disruption_score"] = np.where(
        cal_data["status_unknown"], -1, cal_data["disruption_score"]
    )
    cal_data = cal_data.drop_duplicates("date")
    cal_data = cal_data.reset_index(drop=True)
    cal_data = cal_data[["date", "disruption_score"]]
    status_dict = {
        -1: "Unknown status",
        0: "Normal service",
        1: "Partial disruption",
        2: "Complete disruption",
    }
    cal_data["interpretation"] = cal_data["disruption_score"].map(status_dict)

    # Recording metadata on the daily disruption score for use on chart
    cal_start_month = cal_data["date"].dt.month.min()
    cal_end_month = cal_data["date"].dt.month.max()
    chart_height = 500  # if n_days <= 31 else None

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

    # Getting unique statuses to be shown on calplot and choosing the right colors
    custom_colors = colors_for_calplot(cal_data)

    # Creating a calplot heatmap with the daily disruption score
    cal_fig = calplot(
        cal_data,
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
    overall_desc = text["gen_overall_desc"]
    detailed_desc = text["gen_detailed_desc"]
    cal_desc = text["gen_cal_desc"]

    # Plotting the elements in the correct order,
    # starting with KPIs on top of the page and proceeding with charts
    if mapping_warning:
        st.warning(mapping_warning)
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

    # Plotting a calendar-based overview
    st.subheader("Daily service reliability", divider="grey")
    st.markdown(cal_desc, unsafe_allow_html=True)
    plot_or_not(cal_fig, cal_data)


# %% Page: service disruption insights


def disruption_reasons():
    customize_colors()
    st.header("Disruption reasons")
    add_logo()

    # Detecting and confirming slicer selections
    min_date, max_date = filter_by_date_range()
    day_types = filter_by_day_type()
    hour_types = filter_by_hour_type()
    selected_lines = filter_by_line()
    selected_rows = filter_downtime()

    # Filtering and arranging the data
    data_to_display = operation_fmt[
        (operation_fmt["date"] >= min_date) & (operation_fmt["date"] <= max_date)
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

    # Dealing with periods of system downtime
    data_to_display, downtime_msg = deal_with_downtime(data_to_display, selected_rows)

    # Confirming the selected period
    selected_period, min_date, max_date = get_period_string(data_to_display, "date")
    date_range = pd.date_range(start=min_date, end=max_date)
    date_range = pd.DataFrame(date_range, columns=["date"])
    st.sidebar.markdown(
        f"**Note:** this selection covers the period between {selected_period}."
        + downtime_msg
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
    ignore_these = [
        "Unknown",
        "Unspecified",
        "Normal service",
        "Closed for maintenance",
    ]
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
    status_desc = text["rsn_status_desc"]
    reasons_desc = text["rsn_reasons_desc"]
    reasons_det_desc = text["rsn_reasons_det_desc"]

    # Plotting the elements in the correct order,
    # starting with KPIs on top of the page and proceeding with charts
    if mapping_warning:
        st.warning(mapping_warning)
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


# %% Page: service disruption impact


def disruption_impact():
    customize_colors()
    st.header("Disruption impact")
    add_logo()

    # Detecting and confirming slicer selections
    min_date, max_date = filter_by_date_range()
    day_types = filter_by_day_type()
    hour_types = filter_by_hour_type()
    selected_lines = filter_by_line()
    selected_rows = filter_downtime()

    # Filtering and arranging the operations data
    data_to_display = operation_fmt[
        (operation_fmt["date"] >= min_date) & (operation_fmt["date"] <= max_date)
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

    # Dealing with periods of system downtime
    data_to_display, downtime_msg = deal_with_downtime(data_to_display, selected_rows)

    # Confirming the selected period
    selected_period, min_date, max_date = get_period_string(data_to_display, "date")
    date_range = pd.date_range(start=min_date, end=max_date)
    date_range = pd.DataFrame(date_range, columns=["date"])
    st.sidebar.markdown(
        f"**Note:** this selection covers the period between {selected_period}."
        + downtime_msg
    )

    # =================
    # DAILY MAINTENANCE
    # =================

    # Preparing data on the daily likelihood of maintenance
    vars_for_group = ["status_en_short", "weekday"]
    by_day_chance_mntn = data_to_display.copy()
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
    by_day_dist_mntn = data_to_display[
        data_to_display["status_en"] == "Closed for maintenance"
    ].copy()
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
    by_day_chance_dsrpt = data_to_display.copy()
    by_day_chance_dsrpt["rows_for_status"] = by_day_chance_dsrpt.groupby(
        vars_for_group
    )["date"].transform("count")
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
    by_day_dist_dsrpt = data_to_display[
        ~data_to_display["status_en_short"].isin(ignore_these)
    ].copy()
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
    by_period_chance_mntn = data_to_display.copy()

    by_period_chance_mntn["rows_for_status"] = by_period_chance_mntn.groupby(
        vars_for_group
    )["date"].transform("count")
    by_period_chance_mntn["rows_for_period"] = by_period_chance_mntn.groupby(
        "time_period"
    )["date"].transform("count")
    by_period_chance_mntn["status_pct"] = 100 * (
        by_period_chance_mntn["rows_for_status"]
        / by_period_chance_mntn["rows_for_period"]
    )
    by_period_chance_mntn["status_pct"] = np.round(
        by_period_chance_mntn["status_pct"], 1
    )
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
    by_period_dist_mntn = data_to_display[
        data_to_display["status_en"] == "Closed for maintenance"
    ].copy()
    by_period_dist_mntn["rows_for_status"] = by_period_dist_mntn.groupby(
        vars_for_group
    )["date"].transform("count")
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
    by_period_chance_dsrpt = data_to_display.copy()
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
    by_period_chance_dsrpt["status_pct"] = np.round(
        by_period_chance_dsrpt["status_pct"], 1
    )
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
    by_period_dist_dsrpt = data_to_display[
        ~data_to_display["status_en_short"].isin(ignore_these)
    ].copy()
    by_period_dist_dsrpt["rows_for_status"] = by_period_dist_dsrpt.groupby(
        vars_for_group
    )["date"].transform("count")
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
    by_rush_chance_mntn = data_to_display.copy()

    by_rush_chance_mntn["rows_for_status"] = by_rush_chance_mntn.groupby(
        vars_for_group
    )["date"].transform("count")
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
    by_rush_dist_mntn = data_to_display[
        data_to_display["status_en"] == "Closed for maintenance"
    ].copy()
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
    by_rush_chance_dsrpt = data_to_display.copy()
    by_rush_chance_dsrpt["rows_for_status"] = by_rush_chance_dsrpt.groupby(
        vars_for_group
    )["date"].transform("count")
    by_rush_chance_dsrpt["rows_for_period"] = by_rush_chance_dsrpt.groupby(
        "official_rush_hour"
    )["date"].transform("count")
    by_rush_chance_dsrpt["status_pct"] = 100 * (
        by_rush_chance_dsrpt["rows_for_status"]
        / by_rush_chance_dsrpt["rows_for_period"]
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
    by_rush_dist_dsrpt = data_to_display[
        ~data_to_display["status_en_short"].isin(ignore_these)
    ].copy()
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
    if mapping_warning:
        st.warning(mapping_warning)
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

    st.subheader(
        "Impact of planned maintenance during official rush hour", divider="grey"
    )
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


# %% Page: daily history of service disruptions


def disruption_history():
    customize_colors()
    st.header("Disruption history")
    add_logo()

    # Detecting and confirming slicer selections
    min_date, max_date = filter_by_date_range()
    day_types = filter_by_day_type()
    hour_types = filter_by_hour_type()
    selected_lines = filter_by_line()
    selected_rows = filter_downtime()

    # Filtering and arranging the data
    data_to_display = operation_fmt[
        (operation_fmt["date"] >= min_date) & (operation_fmt["date"] <= max_date)
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

    # Dealing with periods of system downtime
    data_to_display, downtime_msg = deal_with_downtime(data_to_display, selected_rows)

    # Confirming the selected period
    selected_period, min_date, max_date = get_period_string(data_to_display, "date")
    date_range = pd.date_range(start=min_date, end=max_date)
    date_range = pd.DataFrame(date_range, columns=["date"])
    st.sidebar.markdown(
        f"**Note:** this selection covers the period between {selected_period}."
        + downtime_msg
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
    n_desc = text["hist_n_desc"]
    pct_desc = text["hist_pct_desc"]
    h_desc = text["hist_h_desc"]
    stations_desc_pct = text["hist_stations_desc_pct"]
    stations_desc = text["hist_stations_desc"]

    # Plotting the elements in the correct order,
    # starting with KPIs on top of the page and proceeding with charts
    if mapping_warning:
        st.warning(mapping_warning)
    msg_text = text["hist_page_desc"]
    msg_text = msg_text.format(selected_period=selected_period)
    st.markdown(msg_text)

    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Total disruptions", str(total_disruptions))
    metric2.metric("Average disruptions per day", str(avg_per_day))
    metric3.metric("Average duration", str(avg_duration) + "h")

    st.subheader("Number of disruptions", divider="grey")
    st.markdown(n_desc)
    plot_or_not(n_disr_chart, daily_disruption)

    st.subheader("Disruptions as % of time", divider="grey")
    st.markdown(pct_desc)
    plot_or_not(pct_disr_chart, daily_disruption)

    st.subheader("Duration of disruptions", divider="grey")
    st.markdown(h_desc)
    plot_or_not(h_disr_chart, daily_disruption)

    st.subheader("Impacted stations", divider="grey")
    st.markdown(stations_desc_pct)
    plot_or_not(stations_chart_pct, daily_disr_stations)

    st.markdown(stations_desc)
    plot_or_not(stations_chart, daily_disr_stations)


# %% Disruption calculator page


def disruption_calc():
    customize_colors()
    st.header("Disruption calculator")
    add_logo()

    # Printing more info to the user on how to use this page
    st.markdown(text["calc_page_desc"])
    st.warning(text["calc_warning"])

    # Getting number of unique days in the data
    n_days_hist = operation_fmt["date_in_last_n_days"].max()

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
        value=n_days_hist,  # all historical data by default
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
    st.subheader(f"Chances of disruption at {selected_station}", divider="grey")
    calc_res_intro = text["calc_res_intro"]
    calc_res_intro = calc_res_intro.format(min_date=min_date, max_date=max_date)
    st.markdown(calc_res_intro)

    calc_results = text["calc_results"]
    calc_results = calc_results.format(
        selected_station=selected_station,
        selected_day=selected_day,
        selected_hour=selected_hour,
        disruption_pct_selected=disruption_pct_selected,
        disruption_name_most=disruption_name_most,
        disruption_pct_most=disruption_pct_most,
        disruption_name_least=disruption_name_least,
        disruption_pct_least=disruption_pct_least,
    )
    st.markdown(calc_results)


# %% Info on data sources and method page


def method_info(mapping_messages, system_downtime):
    customize_colors()
    st.header("Information on data collection & processing")
    add_logo()
    st.markdown(text["meth_page_desc"])
    st.subheader("Sourcing operational data", divider="grey")
    st.markdown(text["meth_msg_1"])
    st.markdown(text["meth_msg_2"])

    st.subheader("Interpreting status messages", divider="grey")
    st.markdown(text["meth_msg_3"])
    st.markdown(text["meth_msg_4"])
    st.dataframe(mapping_messages)

    st.subheader("Dealing with system downtime", divider="grey")
    st.markdown(text["meth_msg_5"])
    st.image("resources/filter_downtime.PNG")

    st.markdown(text["meth_msg_6"])
    system_downtime = system_downtime.sort_values("date", ascending=False)
    system_downtime = system_downtime.drop(columns="last_modified")
    st.dataframe(system_downtime)


# %% Compulsory legal disclaimer


def legal_info():
    customize_colors()
    st.header("Legal disclaimer")
    add_logo()
    st.markdown(text["legal_disclaimer"])


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
    method_info(mapping_messages, system_downtime)
elif options == "Legal disclaimer":
    legal_info()
