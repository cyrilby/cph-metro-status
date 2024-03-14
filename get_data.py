"""
==============================================
Get data on the CPH Metro's operational status
==============================================

Author: kirilboyanovbg[at]gmail.com
Last meaningful update: 14-03-2024

This script is designed to automatically collect data on the operational
status of the Copenhagen Metro and record disruptions. In practice, this
happens by scraping the Metro's website, locating the relevant information
and then storing it in a local *.pkl and *.csv file.

Note: do not edit the CSV file in Excel as it may mess up the formatting.
"""

# %% Setting things up

# Importing relevant packages
import pandas as pd
import numpy as np
import datetime as dt
import os
import sys
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.options import Options
import requests

# Setting up browser options for use in conjuction with Selenium
edge_options = Options()
edge_options.use_chromium = True
edge_options.add_argument("--headless")  # ensuring GUI is off
edge_options.add_argument("--no-sandbox")
edge_options.add_argument("--disable-dev-shm-usage")

# Specifying file path for storing data
data_filepath = "data/operation_raw.pkl"

# Specifying the working directory
work_dir = "C:/Users/admkbo/OneDrive - Maersk Broker/Documents/M scraper/"
os.chdir(work_dir)

# Specifying the URL of the website
url = "https://m.dk/"

# Specifying what normal operation looks like when data is scraped and formatted
normal_status = ["M1", "M2", "M3", "M4", "Alt kører efter planen"]

# Specifying valid metro lines
metro_lines = normal_status[:4]


# %% Defining custom functions


# Custom function to check if file exists
def file_exists(filepath: str) -> bool:
    """
    Checks whether a local file exists.

    Args:
        filepath (str): filepath to check

    Returns:
        bool: whether or not the file exists
    """
    return os.path.isfile(filepath)


# Custom function to scrape the HTML content of a web page
def scrape_website(url: str) -> BeautifulSoup:
    """
    Opens the specified URL address using Selenium and if it is successful,
    downloads the HTML code of the web page. Returns a BS object that can
    then be searched for various HTML tags.

    Args:
        url (str): URL of the website we're trying to srape

    Returns:
        BeautifulSoup: a BS object that can be searched for HTML tags
    """
    # Loading the web page using a web browser interface
    driver = webdriver.Edge(options=edge_options)
    driver.get(url)

    # Converting the object to BS4 HTML object
    html = driver.page_source
    if html:
        soup = BeautifulSoup(html, "html.parser")
        print("Request successful - HTML content downloaded.")
    else:
        soup = None
        print("Request failed - no new data downloaded.")
    driver.quit()
    return soup


# Custom function to remove duplicates from list while preserving the original order
def remove_duplicates(input_list: list) -> list:
    """
    Removes duplicates from list while preserving the original order
    of the list.

    Args:
        input_list (list): input list containing duplicates.

    Returns:
        list: output list without duplicates.
    """
    return list(dict.fromkeys(input_list))


# Custom function to scrape current operational status
def scrape_status_from_web() -> pd.DataFrame:
    """
    Scrapes operational status data from the Metro's website.
    Returns a dataframe with a status for each metro line.

    Returns:
        pd.DataFrame: df with status for each line.
    """
    # Getting the contents of a website and creating a timestamp
    html_content = scrape_website(url)

    # If data is returned, finding the relevant HTML code
    if html_content:
        operation_status = html_content.find_all(
            "div", class_="operation-data__changes"
        )
    else:
        operation_status = None

    # Extracting info on lines and current operational status
    if operation_status:
        current_status_divs = html_content.find_all(
            "div", class_="operation-data__changes"
        )
        current_status = []
        for div in current_status_divs:
            current_status.extend(div.find_all("span"))
        current_status = [status.get_text(strip=True) for status in current_status]
        current_status = remove_duplicates(current_status)
    else:
        current_status = None

    # If everything is running normally, the list will look exactly as the one below
    if current_status:
        if current_status == normal_status:
            current_status = pd.DataFrame({"line": metro_lines})
            current_status["status"] = normal_status[4]
            current_status["timestamp"] = timestamp
        else:
            current_status = pd.DataFrame({"line": current_status})
            current_status["is_status"] = ~current_status["line"].isin(metro_lines)
            current_status["status"] = np.where(
                current_status["is_status"], current_status["line"], np.nan
            )
            current_status["status"].bfill(inplace=True)
            current_status = current_status[
                current_status["line"].isin(metro_lines)
            ].copy()
            current_status.drop(columns=["is_status"], inplace=True)
            current_status["timestamp"] = timestamp
    else:
        current_status = pd.DataFrame()
    return current_status


# %% Importing previously stored data

# If a file with operational status history exists, we will import it and
# then append any new data to the bottom of it
if file_exists(data_filepath):
    print(
        """Note: Historical data detected. Any new scraped data
          will be appended to the bottom of the historical data."""
    )
    operation_raw = pd.read_pickle(data_filepath)
    operation_raw["timestamp"] = pd.to_datetime(
        operation_raw["timestamp"], format="%Y-%m-%d %H:%M:%S.%f"
    )
else:
    print("Note: No previously recorded historical data detected.")
    operation_raw = pd.DataFrame()


# %% Scraping the Copenhagen Metro's website for new data

# Creating a timestamp for use in the data collection
timestamp = dt.datetime.now()
formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")

# Downloding data on operational status
# Note: we try to download data up to 5 times
max_runs, run_number = 5, 0
while run_number <= max_runs:
    current_status = scrape_status_from_web()
    run_number += 1
    if not current_status.empty:
        break

# If no data was downloaded after 5 consecutive attempts, we use "Unknown" status
if current_status.empty:
    current_status = pd.DataFrame({"line": metro_lines})
    current_status["status"] = "Unknown"
    current_status["timestamp"] = timestamp

# Ensuring the timestamp is formatted consistently
current_status["timestamp"] = pd.to_datetime(current_status["timestamp"])

# Printing data on the current operational status
print(f"Current operational status as of '{formatted_timestamp}':\n")
print(current_status.head(4))
print("\n")

# Appending the data to any previously recorded historical data and formatting
operation_raw = pd.concat([operation_raw, current_status])
operation_raw.sort_values(["timestamp", "line"], ascending=[False, True], inplace=True)
operation_raw.reset_index(inplace=True, drop=True)
operation_raw = operation_raw[["timestamp", "line", "status"]]

# Exporting and confirming success
operation_raw.to_pickle("data/operation_raw.pkl")
operation_raw.to_csv("data/operation_raw.csv", index=False)
print(
    f"""Data on the metro's operational status successfully scraped
    and exported to '{data_filepath}' as of {formatted_timestamp}."""
)
sys.exit()

# Prompting data cleaning & export to Azure data lake storage
exec(open("summarize_data.py").read())

# %%
