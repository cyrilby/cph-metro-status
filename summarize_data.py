"""
====================================================
Summarize data on the CPH Metro's operational status
====================================================

Author: kirilboyanovbg[at]gmail.com
Last meaningful update: 29-02-2024

In this script, we import data on the Copenhagen Metro's operational
status collected at different timestamps, then add some information
on what the status recorded means, then create various tables containing
aggregate data. These tables are then exported and can be used for
data visualization etc.
"""

# %% Setting things up

# Importing relevant packages
import pandas as pd
import numpy as np
import datetime as dt

# Importing raw data
operation_raw = pd.read_pickle("data/operation_raw.pkl")

# Importing mapping tables
mapping_status = pd.read_excel("data/mapping_tables.xlsx", sheet_name="status")
mapping_hours = pd.read_excel("data/mapping_tables.xlsx", sheet_name="hours")
mapping_rush = pd.read_excel("data/mapping_tables.xlsx", sheet_name="rush_hour")
mapping_stations = pd.read_excel("data/mapping_tables.xlsx", sheet_name="stations")


# %% Formatting data

# Adding calculated columns related to date/time
operation_fmt = operation_raw.copy()
operation_fmt["date"] = operation_fmt["timestamp"].dt.date
operation_fmt["day"] = operation_fmt["timestamp"].dt.quarter
operation_fmt["month"] = operation_fmt["timestamp"].dt.month_name()
operation_fmt["quarter"] = operation_fmt["timestamp"].dt.quarter
operation_fmt["weekday"] = operation_fmt["timestamp"].dt.day_of_week
operation_fmt["week"] = operation_fmt["timestamp"].dt.isocalendar().week
operation_fmt["hour"] = operation_fmt["timestamp"].dt.hour
operation_fmt["time"] = operation_fmt["timestamp"].dt.time

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

# Adding more info on what the status means and which stations are affected
operation_fmt = pd.merge(operation_fmt, mapping_status, how="left", on="status")

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


# Finally, we check for impacts on explicitly named stations
station_cols = []
for station in stations_all:
    station_str = station.lower()
    station_str.replace(" ", "_")
    station_cols.append(station_str)
    operation_fmt["affected_" + station_str] = operation_fmt[
        "affected_stations"
    ].str.contains(station)

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

# Temp data preview
operation_fmt.head(5)
