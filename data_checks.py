"""
==================================
Checking data collection & mapping
==================================

Author: github.com/cyrilby
Last meaningful update: 04-10-2026
"""

# %% Setting up

import os
import yaml
import pandas as pd
from storage import get_s3_access

# Importing the credentials for working with object storage
storage_options, bucket = get_s3_access()


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

# Importing links to mapping tables
with open("mapping_links.yaml", "r", encoding="utf-8") as file:
    mapping_links = yaml.safe_load(file)

# Importing user-maintained mapping tables
mapping_status = pd.read_csv(mapping_links["mapping_status"])

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


# %%
