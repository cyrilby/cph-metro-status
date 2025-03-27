"""
==============================================
Get data on the CPH Metro's operational status
==============================================

Author: kirilboyanovbg[at]gmail.com
Last meaningful update: 27-03-2025

This script is designed to automatically collect data on the operational
status of the Copenhagen Metro and record disruptions. In practice, this
happens by scraping the Metro's website, locating the relevant information
and then storing it in a local *.pkl and *.csv file.

Note: do not edit the CSV file in Excel as it may mess up the formatting.

Note 2: this script uses Google Chrome instead of MS Edge and requires
that the corresponding chromedriver is downloaded from here:
https://developer.chrome.com/docs/chromedriver/downloads
"""

# %% Setting things up

# Importing relevant packages
import pandas as pd
import numpy as np
import datetime as dt
import time
import os
import sys
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import subprocess

# Importing custom functions for working with ADLS storage
from azure_storage import get_access, write_blob

# Setting up browser options for use in conjuction with Selenium
chrome_options = Options()
chrome_options.use_chromium = True
# chrome_options.add_argument("--headless")  # ensuring GUI is off
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Detecting whether the OS the script is running is Linux or Windows
linux_os = sys.platform == "linux"

# Specifying the working directory
if linux_os:
    script_path = os.path.abspath(__name__)
else:
    script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)
os.chdir(script_dir)
print(f"Note: files will be saved under '{script_dir}'")

# Specifying the URL of the website
url = "https://m.dk/da/drift-og-service/status-og-planlagte-driftsaendringer/"

# Specifying what normal operation looks like when data is scraped and formatted
normal_status = [
    ["M1", "M2", "M3", "M4", "Alt kører efter planen"],
    ["M1", "M2", "Vi kører efter planen", "M3", "M4", "Vi kører efter planen"],
]

# Specifying valid metro lines
metro_lines = ["M1", "M2", "M3", "M4"]

# Specifying which status messages must be kept even if repeated on the web page
allowed_exceptions = ["Alt kører efter planen", "Vi kører efter planen"]


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
        url (str): URL of the website we're trying to scrape

    Returns:
        BeautifulSoup: a BS object that can be searched for HTML tags
    """
    try:
        # Loading the web page using a web browser interface
        if linux_os:
            service = Service("/usr/bin/chromedriver")
            driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            service = Service("chromedriver-win64/chromedriver.exe")
            driver = webdriver.Chrome(service=service, options=chrome_options)

        driver.get(url)

        # Wait for the "Accepter alle" button to appear and click it
        try:
            accept_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "coi-banner__accept"))
            )
            accept_button.click()
            time.sleep(5)
            print("Cookie banner accepted. Refreshing page...")
            driver.refresh()
            time.sleep(10)
        except Exception as e:
            print(f"Cookie banner not found or not clickable: {e}")

        # Converting the object to BS4 HTML object
        html = driver.page_source
        if html:
            soup = BeautifulSoup(html, "html.parser")
            print("Request successful - HTML content downloaded.")
        else:
            soup = None
            print("Request failed - no new data downloaded.")

    except Exception as e:
        print(f"An error occurred: {e}")
        soup = None

    finally:
        # Wait before closing the browser
        if "driver" in locals():
            driver.quit()
        if not linux_os:
            subprocess.run("kill_chrome.bat", shell=True)

    # Returning
    return soup


# Custom function to remove duplicates from list while preserving the original order
def remove_duplicates(input_list: list, exceptions: list = []) -> list:
    """
    Removes duplicates from list while preserving the original order
    of the list. Allows certain duplicates supplied to the function as
    the "exceptions" list.

    Args:
        input_list (list): input list containing duplicates.
        exceptions (list): list of elements allowed to be duplicated.

    Returns:
        list: output list without duplicates, except for elements in the exceptions list.
    """
    output_list = []
    for i in input_list:
        if i in output_list and i not in exceptions:
            continue
        output_list.append(i)
    return output_list


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
        operation_status = html_content.find(
            "div", class_="flex flex-col gap-xxs mb-xs"
        )
    else:
        operation_status = None

    # Extracting info on lines and current operational status
    # =============================
    # 1) ASSUMING NORMAL OPERATIONS
    # =============================
    # Color-to-line mapping
    color_map = {"#008D41": "M1", "#FFC600": "M2", "#FF0A0A": "M3", "#009CD3": "M4"}

    # Extract all <svg> elements
    svg_elements = operation_status.find_all("svg")

    # Initialize dictionary to hold found lines
    lines_found = []

    for svg in svg_elements:
        path = svg.find("path")
        if path and path.has_attr("fill"):
            fill_color = path["fill"]
            if fill_color in color_map:
                lines_found.append(color_map[fill_color])

    # Get status text (assuming it's in the last <div> with text)
    status_text = operation_status.get_text(strip=True)

    # Build result dictionary and transform to df
    current_status = {line: status_text for line in lines_found}
    if current_status:
        current_status = pd.DataFrame(
            current_status.items(), columns=["line", "status"]
        )
        current_status["timestamp"] = timestamp
    else:
        current_status = pd.DataFrame()
    return current_status


# %% Importing previously stored data

# If a file with operational status history exists, we will import it and
# then append any new data to the bottom of it
operation_raw = pd.read_pickle(
    "https://freelanceprojects.blob.core.windows.net/cph-metro-status/operation_raw.pkl"
)


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
operation_raw_updated = pd.concat(
    [operation_raw, current_status.astype(operation_raw.dtypes)]
)
operation_raw_updated = operation_raw_updated.sort_values(
    ["timestamp", "line"], ascending=[False, True]
)
operation_raw_updated = operation_raw_updated.reset_index(drop=True)
operation_raw_updated = operation_raw_updated[["timestamp", "line", "status"]]

# Exporting raw data locally
# operation_raw_updated.to_pickle("data/operation_raw.pkl")
# operation_raw_updated.to_csv("data/operation_raw.csv", index=False)

# Exporting raw data to Azure and confirming success
azure_conn = get_access("credentials/azure_conn.txt")
write_blob(operation_raw_updated, azure_conn, "cph-metro-status", "operation_raw.pkl")
# write_blob(
#     operation_raw, azure_conn, "cph-metro-status", "operation_raw.csv", index=False
# )
print(
    f"""Data on the metro's operational status successfully scraped
    and exported to Azure cloud storage as of {formatted_timestamp}."""
)

# Prompting data cleaning & export to Azure data lake storage
# Note: disabled due to causing issues with too many background processes
# exec(open("summarize_data.py").read())
sys.exit()

# %%
