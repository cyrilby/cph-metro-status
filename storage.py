"""
================================================
Providing access to Scaleway object storage etc.
================================================

Author: github.com/cyrilby
Last meaningful update: 19-11-2026

This script contains the helpful get_s3_access() function,
which makes it super easy to work with reading/writing pandas
data frames to and from Scaleway S3-compatible object storage.
"""

# %% Setting up

import os
from typing import Literal, Tuple

import boto3
import duckdb
from dotenv import load_dotenv

# %% Function that provides access to files


def get_s3_access() -> Tuple[dict, str]:
    """
    Imports access key and secret key for reading/writing
    from and to Scaleway object storage, then returns a
    dictionary that can be used directly in combination
    with pandas' standard commands for reading and writing
    data frames. Also returns the name of the bucket.
    """

    # Imporing secrets from local .ENV file
    load_dotenv()
    SCW_ACCESS_KEY = os.getenv("SCW_ACCESS_KEY")
    SCW_SECRET_KEY = os.getenv("SCW_SECRET_KEY")
    SCW_BUCKET_NAME = os.getenv("SCW_BUCKET_NAME")

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


# %% Functions that allow for upload/download of files


def manage_s3_file(
    local_file: str,
    remote_file: str,
    file_mode: Literal["upload", "donwload"] = "download",
) -> Tuple[dict, str]:
    """
    Imports access key and secret key for reading/writing
    from and to Scaleway object storage, then uploads a file
    to S3 storage.
    """

    # Imporing secrets from local .ENV file
    load_dotenv()
    SCW_ACCESS_KEY = os.getenv("SCW_ACCESS_KEY")
    SCW_SECRET_KEY = os.getenv("SCW_SECRET_KEY")
    SCW_BUCKET_NAME = os.getenv("SCW_BUCKET_NAME")

    # Preparing config for interacting with S3 storage
    s3_client = boto3.client(
        "s3",
        endpoint_url="https://s3.fr-par.scw.cloud",
        aws_access_key_id=SCW_ACCESS_KEY,
        aws_secret_access_key=SCW_SECRET_KEY,
        region_name="fr-par",
    )

    if file_mode == "upload":
        s3_client.upload_file(local_file, SCW_BUCKET_NAME, remote_file)
    else:
        s3_client.download_file(SCW_BUCKET_NAME, remote_file, local_file)


# %% Function to connect to local DuckDB


def connect_to_db(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect("database.duckdb", read_only=read_only)

    return conn


# %% Function to remove local file


def remove_file(file_name: str) -> None:

    if os.path.exists(file_name):
        os.remove(file_name)


# %%
