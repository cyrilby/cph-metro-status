"""
=============================================================
Format & summarize data on the CPH Metro's operational status
=============================================================

Author: kirilboyanovbg[at]gmail.com
Last meaningful update: 05-11-2024

In this script, we import data on the Copenhagen Metro's operational
status collected at different timestamps, then add some information
on what the status recorded means, including some corrections that
help us improve the data quality, e.g. separating impacts for M1 and M2.
After this, we summarize the data in several different tables that
can then be used for e.g. visualization or analytical purposes.
"""

# %% Setting things up

# Importing relevant packages
import pandas as pd
import numpy as np
import datetime as dt
from datetime import time
import os
import sys

# Importing custom functions for working with ADLS storage
from azure_storage import (
    get_access,
    write_blob,
    delete_blob_if_exists,
)

# Detecting whether the OS the script is running is Linux or Windows
linux_os = sys.platform == "linux"

# Specifying the working directory
if linux_os:
    script_path = os.path.abspath(__name__)
else:
    script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)
os.chdir(script_dir)
print(f"Note: files will be saved under '{script_dir}'")

# Arranging for access to Azure cloud storage
azure_conn = get_access("credentials/azure_conn.txt")

# Importing raw data from Azure
operation_raw = pd.read_pickle(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/operation_raw.pkl"
)

# Importing mapping tables from Azure
mapping_status = pd.read_excel(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/mapping_tables.xlsx",
    sheet_name="status",
)
mapping_hours = pd.read_excel(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/mapping_tables.xlsx",
    sheet_name="hours",
)
mapping_rush = pd.read_excel(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/mapping_tables.xlsx",
    sheet_name="rush_hour",
)
mapping_stations = pd.read_excel(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/mapping_tables.xlsx",
    sheet_name="stations",
)
system_downtime = pd.read_excel(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/mapping_tables.xlsx",
    sheet_name="system_downtime",
)

# Default "Normal" status to be used in cases where messages displayed
# on screens are only meant to serve as warnings to passengers
mapping_normal_service = mapping_status[
    mapping_status["status_dk"] == "Vi kører efter planen"
]
ns_status_dk = mapping_normal_service["status_dk"].iloc[0]
ns_status_en = mapping_normal_service["status_en"].iloc[0]
ns_reason = mapping_normal_service["reason"].iloc[0]
ns_closed_for_maintenance = mapping_normal_service["closed_for_maintenance"].iloc[0]
ns_delayed = mapping_normal_service["delayed"].iloc[0]
ns_not_running = mapping_normal_service["not_running"].iloc[0]
ns_skipping_stations = mapping_normal_service["skipping_stations"].iloc[0]
ns_one_track_only = mapping_normal_service["one_track_only"].iloc[0]
ns_train_changing = mapping_normal_service["train_changing"].iloc[0]
ns_affected_stations = mapping_normal_service["affected_stations"].iloc[0]


# %% Defining custom functions


# Custom function to generate rounded timestamp for "now"
def round_now() -> dt.datetime:
    """
    Creates a timestamp showing current date and time while
    also rounding off the minutes to the nearest 0, 10, 20,
    30, 40 or 50 (by rounding down).

    Returns:
        dt.datetime: rounded off timestamp
    """
    now = dt.datetime.now()
    minute = (now.minute // 10) * 10
    return now.replace(minute=minute, second=0, microsecond=0)


# Note: pandas timestamps can be similarly rounded off using the line below:
# pd.to_datetime('2024-05-06 12:39:59').floor('10min')


# Function to convert "hour:minute" to a time object, returning np.nan if input is np.nan
def convert_to_time(x):
    if pd.isna(x):
        return np.nan
    hour, minute = map(int, x.split(":"))
    return time(hour, minute)


# Function to check if the timestamp's time is between validity_start_time
# and validity_end_time, considering intervals crossing midnight
def is_valid_time(row):
    # Handle NaN values in any of the necessary columns
    if (
        pd.isna(row["rounded_timestamp"])
        or pd.isna(row["validity_start"])
        or pd.isna(row["validity_end"])
    ):
        return np.nan

    start = convert_to_time(row["validity_start"])
    end = convert_to_time(row["validity_end"])
    timestamp_time = row["rounded_timestamp"].time()

    # If the interval doesn't cross midnight
    if start <= end:
        return start <= timestamp_time <= end
    # If the interval crosses midnight
    else:
        return timestamp_time >= start or timestamp_time <= end


# Apply to an existing dataframe with timestamp, validity_start, and validity_end columns
def apply_time_validation(df):
    # Ensure 'rounded_timestamp' column is in datetime format
    df["rounded_timestamp"] = pd.to_datetime(
        df["rounded_timestamp"], errors="coerce"
    )  # Coerce errors to NaT (similar to NaN for datetime)

    # Apply the validation check
    df["status_is_valid"] = df.apply(is_valid_time, axis=1)

    return df


# Function to remove duplicate entries in a string column
def rm_duplicate_str(text: str, sep: str = ", ") -> str:
    """
    Removes duplicate entries from a string representing
    a list.

    Args:
        text (str): input string containing duplicates
        sep (str, optional): separator for the individual
        items. Defaults to ", ".

    Returns:
        str: output string without duplicates
    """
    # Check if the input is NaN
    if pd.isna(text):
        return np.nan

    # Else, clean the string of duplicate entries
    items = text.split(sep)
    unique_items = set(items)
    return sep.join(unique_items)


# Function to validate the list of affected stations
def validate_stations(stations_str: str, line: str, sep: str = ", ") -> str:
    """
    Validates the list of affected stations represented as
    a list and removes stations that do not pertain to the
    line that is represented in the raw data.

    Args:
        stations_str (str): string representation of a list
        of affected stations, potentially containing superfluous
        information
        line (str): metroline (M1/M2/M3/M4)
        sep (str): separator substring. Defaults to ", ".

    Returns:
        str: string containing only the stations relevant
        for the respective metroline
    """

    # Checking if the input is NaN
    if pd.isna(stations_str):
        return np.nan

    # Comparison lists with valid stations per line
    valid_stations = {
        "M1": stations_m1,
        "M2": stations_m2,
        "M3": stations_m3,
        "M4": stations_m4,
    }

    # Converting the string of affected into a list
    affected_list = stations_str.split(sep)

    # Performing the check
    affected_list = [st for st in affected_list if st in valid_stations[line]]

    # Converting back to a string and returning
    affected_str = ", ".join(affected_list)
    return affected_str


# %% Ensuring we have rows for all datetimes covered by the data

"""
If there are issues with the web scraping, gaps in the data collection may occur.
To avoid underestimating the extent of the "Unknown" status message resulting from
any such gaps, we create a placeholder that ensures we have a timestamp for each
10 minutes between the oldest and the most recent timestamp in the raw data.
"""

print("Ensuring completeness of historical data in progress...")

# Getting the span of time that needs to be covered
oldest_timestamp = operation_raw["timestamp"].dt.floor("10min").min()
newest_timestamp = operation_raw["timestamp"].dt.floor("10min").max()

# Creating a range covering all possible rounded timestamps in the above period
time_range = pd.date_range(oldest_timestamp, newest_timestamp, freq="10min")

# Creating a placeholder df containing rows for all metro lines and timestamps
full_history = []
for line in operation_raw["line"].unique():
    temp_range = pd.DataFrame({"rounded_timestamp": time_range})
    temp_range["line"] = line
    full_history.append(temp_range)
full_history = pd.concat(full_history)

# Adding a rounded timestamp in the actual historical data
operation_fmt = operation_raw.copy()
operation_fmt["rounded_timestamp"] = operation_fmt["timestamp"].dt.floor("10min")

# Temporary quick sanity check of timestamps: nothing should be printed when run
# for time_hist in operation_fmt["rounded_timestamp"].unique().tolist():
#     if time_hist not in full_history["rounded_timestamp"].unique().tolist():
#         print(time_hist)

# Adding the actual historical data to the timestamps placeholder df
vars_for_merge = ["rounded_timestamp", "line"]
operation_fmt = pd.merge(full_history, operation_fmt, how="left", on=vars_for_merge)
n_unknown = operation_fmt["status"].isna().sum()
pct_unknown = round(100 * (n_unknown / len(operation_fmt)), 1)
print(
    f"Note: service status data missing in {n_unknown} rows ({pct_unknown}% of all data)."
)

# Ensuring we have no duplicate entries resulting from e.g. running
# the scraping more often than once every 10 minutes
operation_fmt = operation_fmt.drop_duplicates(subset=vars_for_merge)

# Adding "Unknown" status and using the rounded timestamp wherever relevant
operation_fmt["status"] = operation_fmt["status"].fillna("Unknown")
operation_fmt["timestamp"] = operation_fmt["timestamp"].fillna(
    operation_fmt["rounded_timestamp"]
)
print("The missing data will be replaced with 'Unknown' status in the output data.")

print("Ensuring completeness of historical data successfully completed.")


# %% Adding date and time-related information to the data

print("Adding date and time-related information to the data in progress...")

# Adding calculated columns related to date/time
operation_fmt["date"] = operation_fmt["timestamp"].dt.date
operation_fmt["day"] = operation_fmt["timestamp"].dt.day
operation_fmt["weekday"] = operation_fmt["timestamp"].dt.day_name()
operation_fmt["weekday_n"] = operation_fmt["timestamp"].dt.weekday
operation_fmt["day_type"] = np.where(
    operation_fmt["weekday"].isin(["Saturday", "Sunday"]), "Weekends", "Workdays"
)
operation_fmt["month"] = operation_fmt["timestamp"].dt.month_name()
operation_fmt["quarter"] = operation_fmt["timestamp"].dt.quarter
operation_fmt["week"] = operation_fmt["timestamp"].dt.isocalendar().week
operation_fmt["hour"] = operation_fmt["timestamp"].dt.hour
operation_fmt["time"] = operation_fmt["timestamp"].dt.time
operation_fmt["datetime"] = operation_fmt["timestamp"].dt.floor("h")
operation_fmt["eomonth"] = operation_fmt["date"] + pd.offsets.MonthEnd(0)

# Adding more info related to date/time
morning_rush_start = mapping_rush[mapping_rush["rush_hour"] == "Morning"]["start"].iloc[
    0
]
morning_rush_end = mapping_rush[mapping_rush["rush_hour"] == "Morning"]["end"].iloc[0]
afternoon_rush_start = mapping_rush[mapping_rush["rush_hour"] == "Afternoon"][
    "start"
].iloc[0]
afternoon_rush_end = mapping_rush[mapping_rush["rush_hour"] == "Afternoon"]["end"].iloc[
    0
]
conditions = [
    (operation_fmt["time"] >= morning_rush_start)
    & (operation_fmt["time"] < morning_rush_end),
    (operation_fmt["time"] >= afternoon_rush_start)
    & (operation_fmt["time"] < afternoon_rush_end),
    operation_fmt["time"].notna(),
]
values = ["Morning rush hour", "Afternoon rush hour", "Regular hour"]
operation_fmt["official_rush_hour"] = np.select(conditions, values)

# Custom mapping for time of day
operation_fmt = pd.merge(operation_fmt, mapping_hours, how="left", on="hour")

# Adding info on the data's recency
dates_recency = operation_fmt[["date"]].copy()
dates_recency = dates_recency.drop_duplicates(subset="date")
dates_recency = dates_recency.sort_values("date", ascending=False)
dates_recency = dates_recency.reset_index(drop=True)
dates_recency["date_in_last_n_days"] = dates_recency.index + 1
operation_fmt = pd.merge(operation_fmt, dates_recency, how="left", on="date")

# Renaming cols etc.
operation_fmt = operation_fmt.rename(columns={"status": "status_dk"})

# Adding information on known system downtime
# Note: helps exclude periods where the scraper tool did not work as
# expected from the data that is visualized in the Streamlit app
system_downtime = system_downtime[["date", "system_downtime_reason"]].copy()
system_downtime["date"] = system_downtime["date"].dt.date
operation_fmt = pd.merge(operation_fmt, system_downtime, how="left", on="date")
operation_fmt["system_downtime"] = operation_fmt["system_downtime_reason"].notna()
operation_fmt["system_downtime_reason"] = operation_fmt[
    "system_downtime_reason"
].fillna("Web scraper tool running normally")

print("Adding date and time-related information to the data successfully completed.")


# %% Checking the status mapping table for missing entries

"""
As time passes, new status messages may be added to the metro's website.
It is essential then that the mapping table be updated to reflect the
impact outlined by those new status messages.
"""

# Getting the unique status messages for comparison
raw_status = operation_fmt["status_dk"].unique()
mapped_status = mapping_status["status_dk"].unique()

# Checking for unmapped status messages and exporting them in a CSV file
unmapped_status = []
for stat in raw_status:
    if stat not in mapped_status:
        unmapped_status.append(stat)
unmapped_status = pd.DataFrame({"status_dk": unmapped_status})
n_unmapped = len(unmapped_status)

# Printing a confirmation
if n_unmapped:
    print(f"Note: There are {n_unmapped} unmapped status messages in the raw data.")
    print(
        "Please check the 'unmapped_status_messages.csv' file on Azure and add those messags to the 'mapping_tables.xslx' file in the same blob storage container."
    )
    write_blob(
        unmapped_status,
        azure_conn,
        "cph-metro-status",
        "unmapped_status_messages.xlsx",
        index=False,
    )
else:
    print("Note: all status messages from the metro's website are mapped.")
    delete_blob_if_exists(
        azure_conn, "cph-metro-status", "unmapped_status_messages.xlsx"
    )


# %% Adding status mapping & correcting impacted metro lines

"""
In the status mapping table, it is impossible to tell which lines have
been affected unless they are explicitly specified in the message.
However, that information can be inferred based on the raw data, where
metro lines are available. In here, we add that information so as to
better account for the impact of the various disruptions.
"""

print("Adding status mapping & correcting impacted metro lines in progress...")

# Replacing any potential NANs with "Unknown"
operation_fmt["status_dk"] = operation_fmt["status_dk"].fillna("Unknown")

# Adding more info on what the status means and which stations are affected
operation_fmt = pd.merge(operation_fmt, mapping_status, how="left", on="status_dk")

# In cases of "affected_stations" == "All_Relevant", we replace
# the generic description with actual metro lines from the raw data
operation_fmt["affected_stations"] = np.where(
    operation_fmt["affected_stations"] == "All_Relevant",
    operation_fmt["line"] + "_All",
    operation_fmt["affected_stations"],
)

print("Adding status mapping & correcting impacted metro lines successfully completed.")


# %% Adding further status-related information to the data

print("Adding further status-related information to the data in progress...")

# Before proceeding, we check if a status is only valid between certain hours
operation_fmt["confirm_validity"] = operation_fmt["valid_for_hours"].notna()
operation_fmt["validity_start"] = np.where(
    operation_fmt["confirm_validity"],
    operation_fmt["valid_for_hours"].str.split("-").str[0],
    np.nan,
)
operation_fmt["validity_end"] = np.where(
    operation_fmt["confirm_validity"],
    operation_fmt["valid_for_hours"].str.split("-").str[1],
    np.nan,
)
operation_fmt = apply_time_validation(operation_fmt)
operation_fmt["status_is_valid"] = np.where(
    operation_fmt["confirm_validity"], operation_fmt["status_is_valid"], True
)

# If the status is not valid within the given time frame, we
# use the default "Normal service" status instead
operation_fmt["status_dk"] = np.where(
    operation_fmt["status_is_valid"], operation_fmt["status_dk"], ns_status_dk
)
operation_fmt["status_en"] = np.where(
    operation_fmt["status_is_valid"], operation_fmt["status_en"], ns_status_en
)
operation_fmt["reason"] = np.where(
    operation_fmt["status_is_valid"], operation_fmt["reason"], ns_reason
)
operation_fmt["closed_for_maintenance"] = np.where(
    operation_fmt["status_is_valid"],
    operation_fmt["closed_for_maintenance"],
    ns_closed_for_maintenance,
)
operation_fmt["delayed"] = np.where(
    operation_fmt["status_is_valid"], operation_fmt["delayed"], ns_delayed
)
operation_fmt["not_running"] = np.where(
    operation_fmt["status_is_valid"], operation_fmt["not_running"], ns_not_running
)
operation_fmt["skipping_stations"] = np.where(
    operation_fmt["status_is_valid"],
    operation_fmt["skipping_stations"],
    ns_skipping_stations,
)
operation_fmt["one_track_only"] = np.where(
    operation_fmt["status_is_valid"], operation_fmt["one_track_only"], ns_one_track_only
)
operation_fmt["train_changing"] = np.where(
    operation_fmt["status_is_valid"], operation_fmt["train_changing"], ns_train_changing
)
operation_fmt["affected_stations"] = np.where(
    operation_fmt["status_is_valid"],
    operation_fmt["affected_stations"],
    ns_affected_stations,
)

# Removing temporary columns
cols_to_drop = [
    "confirm_validity",
    "validity_start",
    "validity_end",
    "valid_for_hours",
    "status_is_valid",
    "status_notes",
]
operation_fmt = operation_fmt.drop(columns=cols_to_drop)

# Adding dummies to measure impact on stations
# First, we get lists of all relevant stations
stations_all = mapping_stations["station"].unique()
stations_m1 = mapping_stations[mapping_stations["line"] == "M1"]["station"].unique()
stations_m2 = mapping_stations[mapping_stations["line"] == "M2"]["station"].unique()
stations_m3 = mapping_stations[mapping_stations["line"] == "M3"]["station"].unique()
stations_m4 = mapping_stations[mapping_stations["line"] == "M4"]["station"].unique()

# Second we create strings for all stations pertaining to each line and store
# the results in a dictionary that uses the same format as the input data
stations_m1_str = ", ".join(stations_m1)
stations_m2_str = ", ".join(stations_m2)
stations_m3_str = ", ".join(stations_m3)
stations_m4_str = ", ".join(stations_m4)
stations_dict = {
    "M1_All": stations_m1_str,
    "M2_All": stations_m2_str,
    "M3_All": stations_m3_str,
    "M4_All": stations_m4_str,
}

# Third, we mark all stations on a line if all of them are affected but
# their full names are not written explicitly
for key, val in zip(stations_dict.keys(), stations_dict.values()):
    operation_fmt["affected_stations"] = operation_fmt["affected_stations"] + np.where(
        operation_fmt["affected_stations"].str.contains(key), ", " + val.strip(), ""
    )

for key in stations_dict.keys():
    operation_fmt["affected_stations"] = (
        operation_fmt["affected_stations"]
        .str.replace(key + ", ", "", regex=False)
        .str.replace("  ", " ")
        .str.strip()
    )  # Remove double spaces and strip ends

operation_fmt["affected_stations"] = operation_fmt["affected_stations"].apply(
    lambda x: (
        ", ".join(set(item.strip() for item in str(x).split(",")))
        if pd.notnull(x)
        else np.nan
    )
)

# Ensuring we have no duplicate names in the stations string
operation_fmt["affected_stations"] = operation_fmt["affected_stations"].apply(
    rm_duplicate_str
)

# Verifying that impacted stations actually belong to the line
operation_fmt["affected_stations"] = operation_fmt.apply(
    lambda row: validate_stations(row["affected_stations"], row["line"]), axis=1
)

# Imputing 0 for all missing values in numerical columns where status
# is not "Unknown" (this reduces but does not entirely eliminate NANs)
numeric_cols = operation_fmt.select_dtypes(include=[np.number]).columns.tolist()
dates_cols = ["day", "quarter", "weekday", "week", "hour"]
numeric_cols = [col for col in numeric_cols if col not in dates_cols]

for col in numeric_cols:
    operation_fmt[col] = np.where(
        operation_fmt[col].notna(),
        operation_fmt[col],
        np.where(operation_fmt["status_en"] != "Unknown", 0, operation_fmt[col]),
    )

# Imputing "Unknown" for the "status_en" and "Unspecified" for the "reason" columns
operation_fmt["status_en"] = operation_fmt["status_en"].fillna("Unknown")
operation_fmt["reason"] = operation_fmt["reason"].fillna("Unspecified")

# Adding information on whether the added status is a disruption or not
disruption_msg = ["Normal service", "Unknown", np.nan]
operation_fmt["status_disruption"] = ~operation_fmt["status_en"].isin(disruption_msg)

# Creating a short status column
unchanged_msg = ["Normal service", "Unknown", "Closed for maintenance"]
operation_fmt["status_en_short"] = np.where(
    operation_fmt["status_en"].isin(unchanged_msg),
    operation_fmt["status_en"],
    "Disruption",
)


print("Adding further status-related information to the data successfully completed.")


# %% Estimating the duration of each disruption

"""
Note: based on the status message, it is possible to calculate an approximate
start and end time for each service disruption. As the data is only sourced
once every 10 minutes, the actual duration of the disruption may vary, so this
is only an approximation.
"""

print("Estimating the duration of each disruption in progress...")

# Keeping only the data we need for the calculation
id_cols = ["timestamp", "line", "status_dk"]
duration_est = operation_fmt[~operation_fmt["status_en"].isin(disruption_msg)].copy()
duration_est = duration_est[id_cols]
duration_est = duration_est.drop_duplicates(subset=id_cols)

# Sorting by line, status and timestamp
sort_cols = ["line", "status_dk", "timestamp"]
duration_est = duration_est.sort_values(by=sort_cols)

# We add 'line' to the condition to ensure changes are calculated per line
duration_est["status_change"] = (
    duration_est["status_dk"].ne(duration_est["status_dk"].shift())
) | (duration_est["line"].ne(duration_est["line"].shift()))

# Create a 'session' column that increments by 1 each time the status changes
duration_est["session"] = duration_est["status_change"].cumsum()

# Marking the start and end time of each status message
vars_group = ["line", "status_dk", "session"]
duration_est["disruption_start"] = duration_est.groupby(vars_group)[
    "timestamp"
].transform("min")
duration_est["disruption_end"] = duration_est.groupby(vars_group)[
    "timestamp"
].transform("max")

# Estimating the duration of each status
# Note: if difference is 0, we make assume the duration to be less than or = 10 min
# (this happens due to only collecting data every 10 minutes)
duration_est["disruption_duration"] = (
    duration_est["disruption_end"] - duration_est["disruption_start"]
)
duration_est["disruption_duration_min"] = (
    duration_est["disruption_duration"].dt.total_seconds() / 60
)
duration_est["disruption_duration_min"] = np.where(
    duration_est["disruption_duration_min"] < 10,
    10,
    duration_est["disruption_duration_min"],
)
duration_est["disruption_duration_hours"] = duration_est["disruption_duration_min"] / 60

# Keeping only relevant columns and adding them back to the main df
cols_to_keep = [
    "timestamp",
    "line",
    "status_dk",
    "disruption_start",
    "disruption_end",
    "disruption_duration_min",
    "disruption_duration_hours",
]
duration_est = duration_est[cols_to_keep]
operation_fmt = pd.merge(operation_fmt, duration_est, how="left", on=id_cols)

print("Estimating the duration of each disruption successfully completed.")


# %% Preparing a table with impacted stations

print("Preparing a table with impacted stations in progress...")

# Setting inauguration date for specific lines: this can be used to provide
# more accurate historical depictions of impacted stations on lines that have
# seen extensions in the period captured by the data
inauguration_dates = {
    "Enghave Brygge": "2024-06-30",
    "Havneholmen": "2024-06-30",
    "Mozarts Plads": "2024-06-30",
    "Ny Ellebjerg": "2024-06-30",
    "Sluseholmen": "2024-06-30",
}

# Df with unique stations for each line
unique_stations = mapping_stations[["line", "station"]].copy()

# List of timestamps in the data
unique_timestamps = operation_fmt["timestamp"].unique().tolist()

# Creating a placeholder where each station has 1 row per timestamp
station_impact = []
for timestamp in unique_timestamps:
    temp_data = unique_stations.copy()
    temp_data["timestamp"] = timestamp
    station_impact.append(temp_data)
station_impact = pd.concat(station_impact)

# Making sure historical data is represented correctly in cases of line extensions
station_impact["inauguration_date"] = station_impact["station"].map(inauguration_dates)
station_impact["inauguration_date"] = pd.to_datetime(
    station_impact["inauguration_date"]
)
station_impact["remove_row"] = (
    station_impact["timestamp"] < station_impact["inauguration_date"]
)
station_impact = station_impact[~station_impact["remove_row"]].copy()
station_impact = station_impact.drop(columns=["inauguration_date", "remove_row"])

# Getting info on whether service was impacted by timestamp, station and line
cols_to_keep = [
    "timestamp",
    "date",
    "date_in_last_n_days",
    "weekday",
    "day_type",
    "hour",
    "line",
    "status_dk",
    "status_en",
    "status_en_short",
    "affected_stations",
]
affected_stations = operation_fmt[cols_to_keep].copy()
affected_stations["affected_stations"] = affected_stations["affected_stations"].fillna(
    ""
)

# Merging the placeholder with the actual data and marking which stations were affected
id_cols = ["timestamp", "line"]
station_impact = pd.merge(station_impact, affected_stations, how="left", on=id_cols)
station_impact["station_impacted"] = station_impact.apply(
    lambda row: row["station"] in row["affected_stations"], axis=1
)

# Adding a "hour_interval" column to use in the Streamlit app's calculator page
station_impact["hour_lower"] = station_impact["hour"].astype(str)
station_impact["hour_lower"] = np.where(
    station_impact["hour_lower"].str.len() == 1,
    "0" + station_impact["hour_lower"],
    station_impact["hour_lower"],
)
station_impact["hour_upper"] = (station_impact["hour"] + 1).astype(str)
station_impact["hour_upper"] = np.where(
    station_impact["hour_upper"].str.len() == 1,
    "0" + station_impact["hour_upper"],
    station_impact["hour_upper"],
)
station_impact["hour_upper"] = np.where(
    station_impact["hour_upper"] == "24", "00", station_impact["hour_upper"]
)
station_impact["hour_interval"] = (
    station_impact["hour_lower"] + "-" + station_impact["hour_upper"]
)
station_impact = station_impact.drop(columns=["hour_lower", "hour_upper"])

# Aggregating the number of disrupted stations per timestamp and adding it to the main df
cols_to_add_to_main = ["n_impacted_stations", "pct_impacted_stations"]
station_impact["n_impacted_stations"] = station_impact.groupby(id_cols)[
    "station_impacted"
].transform("sum")
station_impact["total_stations"] = station_impact.groupby(id_cols)["station"].transform(
    "nunique"
)
station_impact["pct_impacted_stations"] = 100 * (
    station_impact["n_impacted_stations"] / station_impact["total_stations"]
)
n_impacted = station_impact[id_cols + cols_to_add_to_main].copy()
n_impacted = n_impacted.drop_duplicates(subset=id_cols)
operation_fmt = pd.merge(operation_fmt, n_impacted, how="left", on=id_cols)

# Cleaning up
cols_to_drop = [
    "affected_stations",
    "n_impacted_stations",
    "total_stations",
    "pct_impacted_stations",
]
station_impact = station_impact.drop(columns=cols_to_drop)

print("Preparing a table with impacted stations successfully completed.")


# %% Data preview and export

# Temp data preview
operation_fmt.head(5)
station_impact.head(5)

# Exporting formatted data locally
# operation_fmt.to_parquet("data/operation_fmt.parquet")
# station_impact.to_parquet("data/station_impact.parquet")
# mapping_stations.to_pickle("data/mapping_stations.pkl")

# Uploading data to Azure data lake storage
write_blob(operation_fmt, azure_conn, "cph-metro-status", "operation_fmt.parquet")
write_blob(station_impact, azure_conn, "cph-metro-status", "station_impact.parquet")
write_blob(mapping_stations, azure_conn, "cph-metro-status", "mapping_stations.pkl")
write_blob(mapping_status, azure_conn, "cph-metro-status", "mapping_messages.pkl")


print("Note: Data cleaned up and exported to Azure cloud storage.")

# %%
