"""
===========================================
Providing access to Scaleway object storage
===========================================

Author: github.com/cyrilby
Last meaningful update: 16-02-2026

This script contains the helpful get_s3_access() function,
which makes it super easy to work with reading/writing pandas
data frames to and from Scaleway S3-compatible object storage.
"""

# ==========================================
# Provides access to Scaleway object storage
# ==========================================

# %% Setting up

import pandas as pd
import os
from dotenv import load_dotenv
import streamlit as st
from typing import Tuple


# %% Function that provides the access


def get_s3_access() -> Tuple[dict, str]:
    """
    Imports access key and secret key for reading/writing
    from and to Scaleway object storage, then returns a
    dictionary that can be used directly in combination
    with pandas' standard commands for reading and writing
    data frames. Also returns the name of the bucket.
    """

    try:
        # Imporing secrets from local .ENV file
        load_dotenv()
        SCW_ACCESS_KEY = os.getenv("SCW_ACCESS_KEY")
        SCW_SECRET_KEY = os.getenv("SCW_SECRET_KEY")
        SCW_BUCKET_NAME = os.getenv("SCW_BUCKET_NAME")

    except Exception:
        # Getting secrets from Streamlit Cloud
        SCW_ACCESS_KEY = st.secrets["SCW_ACCESS_KEY"]
        SCW_SECRET_KEY = st.secrets["SCW_SECRET_KEY"]
        SCW_BUCKET_NAME = st.secrets["SCW_BUCKET_NAME"]

    # Preparing config for interaction with object storage
    storage_options = {
        "key": SCW_ACCESS_KEY,
        "secret": SCW_SECRET_KEY,
        "client_kwargs": {
            "endpoint_url": "https://s3.fr-par.scw.cloud",
            "region_name": "fr-par",
        },
    }

    return storage_options, SCW_BUCKET_NAME


# %% Examples of reading/writing data

if __name__ == "__main__":
    # Getting access to the storage
    storage_options, bucket = get_s3_access()

    # Creating a dummy df
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    df = df * 1000

    # Test with writing a CSV file
    df.to_csv(f"s3://{bucket}/data.csv", storage_options=storage_options, index=False)

    # Test with writing a Parquet file
    df.to_parquet(
        f"s3://{bucket}/data.parquet",
        storage_options=storage_options,
    )

    # Test with re-importing the data from the storage
    df2 = pd.read_csv(f"s3://{bucket}/data.csv", storage_options=storage_options)


# %%
