"""
=============================================================
Format & summarize data on the CPH Metro's operational status
=============================================================

Author: kirilboyanovbg[at]gmail.com
Last meaningful update: 03-07-2024

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


# %% Adding status-related information to the data

print("Adding status-related information to the data in progress...")

# Replacing any potential NANs with "Unknown"
operation_fmt["status_dk"] = operation_fmt["status_dk"].fillna("Unknown")

# Adding more info on what the status means and which stations are affected
operation_fmt = pd.merge(operation_fmt, mapping_status, how="left", on="status_dk")

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
        operation_fmt["affected_stations"].str.contains(key), ", " + val, ""
    )
for key in stations_dict.keys():
    operation_fmt["affected_stations"] = operation_fmt["affected_stations"].str.replace(
        key + ", ", "", regex=False
    )
operation_fmt["affected_stations"] = operation_fmt["affected_stations"].apply(
    lambda x: ", ".join(set(str(x).split(","))) if pd.notnull(x) else np.nan
)


"""
# Finally, we check for impacts on explicitly named stations
station_cols = []
for station in stations_all:
    station_str = station.lower()
    station_str.replace(" ", "_")
    station_cols.append(station_str)
    operation_fmt["affected_" + station_str] = operation_fmt[
        "affected_stations"
    ].str.contains(station)
"""

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
unchanged_msg = ["Normal service", "Unknown"]
operation_fmt["status_en_short"] = np.where(
    operation_fmt["status_en"].isin(unchanged_msg),
    operation_fmt["status_en"],
    "Disruption",
)


print("Adding status-related information to the data successfully completed.")


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
