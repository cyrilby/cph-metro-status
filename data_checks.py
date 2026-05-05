# Quick check of CPH metro scraper data
# Kiril, 12-01-2025


# %% Setting up

import pandas as pd

# Setting up access to object storage
SCW_BUCKET_NAME = "cph-metro-status"
SCW_ACCESS_KEY = "SCW7853FFRVQ76ZF7YJN"
SCW_SECRET_KEY = "38f4c08c-7a8b-4446-aadd-8282770fa9b1"
bucket = SCW_BUCKET_NAME

storage_options = {
    "key": SCW_ACCESS_KEY,
    "secret": SCW_SECRET_KEY,
    "client_kwargs": {
        "endpoint_url": "https://s3.fr-par.scw.cloud",
        "region_name": "fr-par",
    },
}

# %% Checking data collection

# Getting and previewing raw data
operation_raw = pd.read_pickle(
    f"s3://{bucket}/operation_raw.pkl",
    storage_options=storage_options,
)

# Replacing any potential NANs with "Unknown"
operation_raw["status"] = operation_raw["status"].fillna("Unknown")

print("Showing latest raw data from the CPH metro scraper tool:")
print(operation_raw.head(20))


# %% Checking mapping

# Checking for unmapped entries and previewing those
mapping_link = "https://docs.google.com/spreadsheets/d/1iu7QcYav9865SLSBXGuZWNKUd0ywCrSC0M2vAink1yM/gviz/tq?tqx=out:csv"  # noqa
mapping_status = pd.read_csv(mapping_link)

unmapped_entries = [
    status
    for status in operation_raw["status"].unique().tolist()
    if status not in mapping_status["status_dk"].tolist()
]
n_unmapped = len(unmapped_entries)

if n_unmapped >= 1:
    print(f"\nNote: There are a total of {n_unmapped} unmapped status messages:\n")
    for entry in unmapped_entries:
        print(entry)

    # Also exporting a local copy of the unmapped entries
    unmapped_entries = pd.DataFrame({"status_dk": unmapped_entries})
    unmapped_entries.to_excel("unmapped_entries.xlsx", index=False)

else:
    print("Note: all service status messages have been mapped.")

# %%
