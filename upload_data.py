"""
=========================================
Uploading data to Azure data lake storage
=========================================

Author: kirilboyanovbg[at]gmail.com
Last meaningful update: 14-03-2024

This script is designed to refresh all output data tables in the
relevant Azure data lake blob so that we can enable subsequent queries
to get the data and use it for e.g. data visualization purposes.
"""

# %% Setting things up

# Importing relevant packages
import pandas as pd

# Importing custom functions for working with ADLS storage
from azure_storage import get_access, write_blob


# %% Uploading local data to Azure cloud storage

print("Uploading local data to Azure cloud storage in progress...")

# Importing data
operation_raw = pd.read_pickle("data/operation_raw.pkl")
operation_fmt = pd.read_parquet("data/operation_fmt.parquet")
station_impact = pd.read_parquet("data/station_impact.parquet")
mapping_stations = pd.read_pickle("data/mapping_stations.pkl")

# Getting access to the cloud
azure_conn = get_access("credentials/azure_conn.txt")

# Uploading files to the cloud
write_blob(operation_raw, azure_conn, "cph-metro-status", "operation_raw.pkl")
write_blob(operation_fmt, azure_conn, "cph-metro-status", "operation_fmt.parquet")
write_blob(station_impact, azure_conn, "cph-metro-status", "station_impact.parquet")
write_blob(mapping_stations, azure_conn, "cph-metro-status", "mapping_stations.pkl")

# Confirming success
print("Uploading local data to Azure cloud storage successfully completed.")


# %%
