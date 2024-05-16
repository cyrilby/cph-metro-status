import io
import pickle
import json
import pandas as pd
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from typing import Any


def get_access(filepath: str) -> str:
    """
    Imports an ADSL connection string including the associated
    access key from a local file stored in the same folder.

    Args:
        filepath (str): path to a TXT file containing
        the connection string

    Returns:
        str: the connection string to pass on to other functions
        such as read_blob() and write_blob()
    """
    with open(filepath, "r") as file:
        conn_string = file.read().replace("\\n", "")
    return conn_string


def read_blob(
    connection_string: str, container_name: str, blob_name: str, **kwargs
) -> Any:
    """
    Imports a file stored in Azure blob storage into Python's memory.
    Object type depends on the file itself and can range from a string,
    list or dict to a pandas data frame (this is auto detected based
    on the file extension).

    Args:
        connection_string (str): connection string in the format provided
        by the get_access() function
        container_name (str): name of the container where the file is stored
        blob_name (str): path to the file inside the container itself

    Raises:
        ValueError: if we try to read an unsupported file type

    Returns:
        Any: any object (if pickled), string (if txt), dict (if json) or
        otherwise pandas.DataFrame
    """
    # We use the file extension to determine the function used to read data
    _, extension = os.path.splitext(blob_name)

    # We download the blob as a Python object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)
    obj = blob_client.download_blob().readall()

    # For non-data frame objects, we handle json, pickle and txt files
    # (if a pickle file is a df, it will directly be imported as such)
    if extension == ".txt":
        conv_obj = obj.decode("utf-8")
    elif extension in [".pkl", ".pickle"]:
        conv_obj = pickle.load(io.BytesIO(obj))
    elif extension == ".json":
        conv_obj = json.loads(obj.decode("utf8"))
    # For objects assumed to be pandas df, we auto detect the file type
    # from the file extension and then call the appropriate pandas.read_X() fn
    elif extension == ".csv":
        conv_obj = pd.read_csv(io.BytesIO(obj), **kwargs)
    elif extension in [".xlsx", ".xls", ".xlsm"]:
        conv_obj = pd.read_excel(io.BytesIO(obj), **kwargs)
    elif extension == ".html":
        conv_obj = pd.read_html(io.BytesIO(obj), **kwargs)
    elif extension == ".hdf":
        conv_obj = pd.read_hdf(io.BytesIO(obj), key="data", **kwargs)
    elif extension == ".stata":
        conv_obj = pd.read_stata(io.BytesIO(obj), **kwargs)
    elif extension == ".gbq":
        conv_obj = pd.read_gbq(io.BytesIO(obj), "my_dataset.my_table", **kwargs)
    elif extension == ".parquet":
        conv_obj = pd.read_parquet(io.BytesIO(obj), **kwargs)
    elif extension in [".f", ".feather"]:
        conv_obj = pd.read_feather(io.BytesIO(obj), **kwargs)
    # For all other objects, we raise an error, though in theory, we could also
    # enable the direct import to a bytes IO object
    else:
        raise ValueError(f"Unsupported file extension: {extension}")
    # else:
    #    conv_obj = io.BytesIO(obj)
    return conv_obj


# Custom function to access Azure files annonymously
def read_blob_anonymously(blob_url: str, **kwargs):
    """
    Imports a file stored in Azure blob storage into Python's memory.
    Object type depends on the file itself and can range from a string,
    list or dict to a pandas data frame (this is auto detected based
    on the file extension).

    Args:
        blob_url (str): URL of the blob

    Raises:
        ValueError: if we try to read an unsupported file type

    Returns:
        Any: any object (if pickled), string (if txt), dict (if json) or
        otherwise pandas.DataFrame
    """
    # We use the file extension to determine the function used to read data
    _, extension = os.path.splitext(blob_url)

    # We download the blob as a Python object
    blob_client = BlobClient.from_blob_url(blob_url)
    obj = blob_client.download_blob().readall()

    # For objects assumed to be pandas df, we auto detect the file type
    # from the file extension and then call the appropriate pandas.read_X() fn
    if extension == ".csv":
        conv_obj = pd.read_csv(io.BytesIO(obj), **kwargs)
    elif extension in [".xlsx", ".xls", ".xlsm"]:
        conv_obj = pd.read_excel(io.BytesIO(obj), **kwargs)
    elif extension == ".html":
        conv_obj = pd.read_html(io.BytesIO(obj), **kwargs)
    elif extension == ".hdf":
        conv_obj = pd.read_hdf(io.BytesIO(obj), key="data", **kwargs)
    elif extension == ".stata":
        conv_obj = pd.read_stata(io.BytesIO(obj), **kwargs)
    elif extension == ".gbq":
        conv_obj = pd.read_gbq(io.BytesIO(obj), "my_dataset.my_table", **kwargs)
    elif extension == ".parquet":
        conv_obj = pd.read_parquet(io.BytesIO(obj), **kwargs)
    elif extension == ".pkl":
        conv_obj = pd.read_pickle(io.BytesIO(obj), **kwargs)
    elif extension in [".f", ".feather"]:
        conv_obj = pd.read_feather(io.BytesIO(obj), **kwargs)
    # For all other objects, we raise an error, though in theory, we could also
    # enable the direct import to a bytes IO object
    else:
        raise ValueError(f"Unsupported file extension: {extension}")
    # else:
    #    conv_obj = io.BytesIO(obj)
    return conv_obj


def write_blob(
    obj: Any,
    connection_string: str,
    container_name: str,
    blob_name: str,
    overwrite: bool = True,
    **kwargs,
) -> None:
    """_summary_

    Args:
        obj (Any): any object (if pickled), string (if txt), dict (if json) or
        otherwise pandas.DataFrame
        connection_string (str): connection string in the format provided
        by the get_access() function
        container_name (str): name of the container where the file is stored
        blob_name (str): path to the file inside the container itself
        overwrite (bool, optional): whether or not to overwrite the original
        file contained in the blob (if it exists). Defaults to True.

    Raises:
        ValueError: if we try to write to an unsupported file type
    """
    # We use the file extension to determine the function used to write data
    _, extension = os.path.splitext(blob_name)
    conv_obj = io.BytesIO()

    if type(obj) is pd.DataFrame:
        # For data frames, we auto detect the file type from the extension
        # and use the appropriate pandas.to_X() function to write the file
        if extension == ".csv":
            obj.to_csv(conv_obj, **kwargs)
        elif extension in [".xlsx", ".xls", ".xlsm"]:
            obj.to_excel(conv_obj, **kwargs)
        elif extension == ".json":
            obj.to_json(conv_obj, **kwargs)
        elif extension == ".html":
            obj.to_html(conv_obj, **kwargs)
        elif extension in [".pkl", ".pickle"]:
            obj.to_pickle(conv_obj, **kwargs)
        elif extension == ".hdf":
            obj.to_hdf(conv_obj, key="data", **kwargs)
        elif extension == ".stata":
            obj.to_stata(conv_obj, **kwargs)
        elif extension == ".gbq":
            obj.to_gbq(conv_obj, "my_dataset.my_table", **kwargs)
        elif extension == ".parquet":
            obj.to_parquet(conv_obj, **kwargs)
        elif extension in [".f", ".feather"]:
            obj.to_feather(conv_obj, **kwargs)
        else:
            raise ValueError(
                f"Unsupported file extension for storing data frame objects: {extension}"
            )
    else:
        # For non-data frame objects, we handle .json, .pickle and .txt files
        if extension == ".json":
            json_obj = json.dumps(obj)
            conv_obj.write(json_obj.encode("utf-8"))
        elif extension in [".pkl", ".pickle"]:
            pickle.dump(obj, conv_obj)
        elif extension == ".txt":
            conv_obj.write(obj.encode("utf-8"))
        else:
            raise ValueError(
                f"Unsupported file extension for storing non-data frame objects: {extension}"
            )

    # We reset bytes object as it is necessary before writing
    conv_obj.seek(0)

    # We upload the blob object to the cloud
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)
    blob_client.upload_blob(conv_obj, overwrite=overwrite)


def append_to_blob(
    local_df: pd.DataFrame,
    connection_string: str,
    container_name: str,
    blob_name: str,
    id_vars: list = [],
) -> None:
    """
    Takes a pandas dataframe containing model outputs and appends
    it to the relevant parquet file already stored in Azure. Keeps
    unique rows based on "id_vars". If there is no blob with the
    specified name, it will be created but it will of course only
    contain the new rows.

    Args:
        local_df (pd.DataFrame): df whose rows are to be uploaded to Azure
        connection_string (str): connection string to use for Azure
        container_name (str): name of storage container in Azure
        blob_name (str): name of the file as stored in Azure
        id_vars (list): variables identifying unique rows to avoid
        saving duplicate entries to Azure (none by default)
    """

    # Create a blob client using the local blob_name as name
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)

    # Check if the blob exists
    if blob_client.exists():
        # Importing data previously uploaded to Azure
        old_data = read_blob(connection_string, container_name, blob_name)

        # Unifying previously uploaded data with new data and making sure
        # we don't keep any duplicate entries in Azure
        local_df = pd.concat([local_df, old_data])

        # Making sure we don't have duplicate rows in Azure, if ID vars are
        # specified by the user
        if id_vars:
            local_df.drop_duplicates(subset=id_vars, inplace=True)
            local_df.reset_index(inplace=True, drop=True)

    # Exporting to a parquet file
    write_blob(
        local_df,
        connection_string,
        container_name,
        blob_name,
    )


def filter_blob(
    connection_string: str, container_name: str, blob_name: str, filters: dict, **kwargs
) -> None:
    """
    Downloads a file from Azure blob storage, filters the data by multiple columns
    so that it only keeps values in the corresponding lists of acceptable values,
    and then re-uploads the filtered data.

    Args:
        connection_string (str): connection string in the format provided
        by the get_access() function
        container_name (str): name of the container where the file is stored
        blob_name (str): path to the file inside the container itself
        filters (dict): dictionary of column names and corresponding lists of acceptable values
    """

    # Create a blob client using the local blob_name as name
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)

    # Check if the blob exists
    if blob_client.exists():
        # Importing data previously uploaded to Azure
        df = read_blob(connection_string, container_name, blob_name, **kwargs)

        # Filter the data by multiple columns so that it only keeps values in the corresponding lists of acceptable values
        for column, acceptable_values in filters.items():
            df = df[df[column].isin(acceptable_values)]

        # Exporting to a parquet file
        write_blob(
            df,
            connection_string,
            container_name,
            blob_name,
        )
    else:
        print(f"The blob {blob_name} does not exist in the container {container_name}.")


def delete_blob_if_exists(
    connection_string: str, container_name: str, blob_name: str
) -> None:
    """
    Deletes a file in Azure blob storage if the file exists
    in the specified container/directory.

    Args:
        connection_string (str): connection string in the format provided
        by the get_access() function
        container_name (str): name of the container where the file is stored
        blob_name (str): path to the file inside the container itself
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        blob_client = blob_service_client.get_blob_client(container_name, blob_name)

        if blob_client.exists():
            blob_client.delete_blob()
            print(f"Blob '{blob_name}' deleted.")
        else:
            print(f"Blob '{blob_name}' does not exist. No action taken.")

    except Exception as e:
        print(f"An exception occurred: {e}")
