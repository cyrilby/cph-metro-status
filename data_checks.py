"""
==================================
Checking data collection & mapping
==================================

Author: github.com/cyrilby
Last meaningful update: 19-11-2026

This script is designed to check the latest status of the
raw data collection (web scraping) jobs as well as to quickly
check whether there may be any status messages that have not
yet been mapped to the standardized operational statuses.
"""

# %% Setting up

import os

import pandas as pd

from storage import connect_to_db, get_s3_access, manage_s3_file, remove_file

# Importing the credentials for working with object storage
storage_options, bucket = get_s3_access()

# Whether to check for the % of unknown status by date
check_daily_unknown = False


# %% Checking data collection status

# Getting and previewing raw data
operation_raw = pd.read_pickle(
    f"s3://{bucket}/operation_raw.pkl", storage_options=storage_options
)

# Replacing any potential NANs with "Unknown"
operation_raw["status"] = operation_raw["status"].fillna("Unknown")

print("Showing latest raw data from the CPH metro scraper tool:")
print(operation_raw.head(20))


# %% Checking the mapping completeness

# Importing user-maintained mapping tables
mapping_status = pd.read_excel(
    f"s3://{bucket}/mapping_tables.xlsx",
    storage_options=storage_options,
    sheet_name="status",
)

unmapped_entries = [
    status
    for status in operation_raw["status"].unique().tolist()
    if status not in mapping_status["status_dk"].tolist()
]
n_unmapped = len(unmapped_entries)

mapping_file = "data/unmapped_entries.xlsx"

if n_unmapped >= 1:
    print(f"Note: There are a total of {n_unmapped} unmapped status messages:\n")
    for entry in unmapped_entries:
        print(entry + "\n")

    # Exporting entries to local XLSX file for easier mapping
    unmapped_entries = pd.DataFrame({"status_dk": unmapped_entries})
    unmapped_entries.to_excel(mapping_file, index=False)

else:
    print("Note: all service status messages have been mapped.")

    # Remove local XLSX file if all messages are mapped
    if os.path.exists(mapping_file):
        os.remove(mapping_file)


# %% Check the % of unknown status entries by date (optional)

if check_daily_unknown:
    # Getting data the latest version of the clean data
    manage_s3_file("database.duckdb", "database.duckdb", "download")
    conn = connect_to_db()

    # Running a query to check the daily values
    query = """
    SELECT
        date,
        CASE
            WHEN status_en = 'Unknown' THEN 'Unknown'
            ELSE 'Known'
        END AS overall_status,
        COUNT(*) AS number_of_rows
    FROM operation_fmt
    GROUP BY
        date,
        CASE
            WHEN status_en = 'Unknown' THEN 'Unknown'
            ELSE 'Known'
        END;
    """
    daily_unknown = conn.execute(query).fetch_df()

    # Transforming the data into percentages
    daily_unknown["daily_rows"] = daily_unknown.groupby("date")[
        "number_of_rows"
    ].transform("sum")
    daily_unknown["pct_unknown"] = (
        daily_unknown["number_of_rows"] / daily_unknown["daily_rows"]
    )
    daily_unknown["pct_unknown"] *= 100
    daily_unknown = daily_unknown[daily_unknown["overall_status"] == "Unknown"]

    # Previewing the daily share of unknown entries
    cols_keep = ["pct_unknown", "date"]
    sort_asc = [False, False]
    daily_unknown = daily_unknown[cols_keep]
    daily_unknown = daily_unknown.sort_values(cols_keep, ascending=sort_asc)
    daily_unknown = daily_unknown.reset_index(drop=True)
    daily_unknown

    # Cleaning up
    remove_file("database.duckdb")


# %%
