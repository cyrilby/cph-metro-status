"""
=============================
Creating weekly data back-ups
=============================

Author: github.com/cyrilby
Last meaningful update: 22-02-2026

Quite simple - a script design to import the latest raw
data from cloud storage, then create a copy of the file
in the "backup" folder with a timestamp. This could be
scheduled e.g. once a week to ensure an accidental file
corruption does not result in losing all the data we have.
"""

# %% Setting things up

# Importing relevant packages
import pandas as pd
import datetime as dt
from storage import get_s3_access

# Importing the credentials for working with object storage
storage_options, bucket = get_s3_access()


# %% Creating the back-up

print("Creating back-up of raw data in progress...")

# Importing the raw data
operation_raw = pd.read_pickle(
    f"s3://{bucket}/operation_raw.pkl", storage_options=storage_options
)

# Adding the current date's time stamp
today = dt.date.today().strftime("%Y-%m-%d")

# Saving the file to the "backup" folder
operation_raw.to_pickle(
    f"s3://{bucket}/backup/operation_raw_{today}.pkl", storage_options=storage_options
)

print(f"Note: Back-up successfully created as of {today}.")

# %%
