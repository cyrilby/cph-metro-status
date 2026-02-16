"""
==============================================
Get data on the CPH Metro's operational status
==============================================

Author: github.com/cyrilby
Last meaningful update: 16-02-2026

This script is designed to automatically collect data on the
operational status of the Copenhagen Metro and record disruptions.
In practice, this happens by scraping the Metro's website, locating
the relevant information and then storing it in a *.pkl format in a cloud storage zone.
"""

# %% Setting things up

# Importing relevant packages
import pandas as pd
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
import subprocess
from storage import get_s3_access

# Setting up browser options for use in conjuction with Selenium
chrome_options = Options()
chrome_options.use_chromium = True
chrome_options.add_argument("--headless")  # ensuring GUI is off
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Detecting whether the OS the script is running is Linux or Windows
linux_os = sys.platform == "linux"

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
allowed_exceptions = [
    "Alt kører efter planen",
    "Vi kører efter planen",
    "Normal togdrift",
]

# Importing the credentials for working with object storage
storage_options, bucket = get_s3_access()


# %% Defining custom functions


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
    print("Scraping HTML code from website in progress...")
    try:
        # Loading the web page using a web browser interface
        if linux_os:
            # service = Service("/usr/bin/chromedriver")
            service = Service("/usr/bin/chromedriver")
            driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            service = Service()
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


# Custom function to scrape current operational status
def scrape_status_from_web() -> pd.DataFrame:
    """
    Scrapes operational status data from the Metro's website.
    Returns a dataframe with a status for each metro line.

    Returns:
        pd.DataFrame: df with status for each line.
    """
    try:
        # Getting the contents of a website and creating a timestamp
        html_content = scrape_website(url)

        # If data is returned, finding the relevant HTML code
        if html_content:
            operation_status = html_content.find("div", class_="pr-s")
        else:
            operation_status = None

        # Metro lines are always listed in an ascending order
        lines = ["M1", "M2", "M3", "M4"]

        # Status messages are grouped for M1&M2 or M3&M4
        message_divs = operation_status.find_all(
            "div", class_="flex items-center text-white"
        )
        status_texts = [div.get_text(strip=True) for div in message_divs]

        # If only 1 status is available, it applies to all lines;
        # if 2 statuses are available, they apply to the groups
        # of metro lines; if something else is returned, it is
        # likely an error in the code that needs to be fixed
        if len(status_texts) == 1:
            status = 4 * status_texts
        elif len(status_texts) == 2:
            status = 2 * [status_texts[0]] + 2 * [status_texts[1]]
        else:
            status = 4 * ["Unknown"]

        # Transform the results in a df and add timestamp
        current_status = pd.DataFrame({"line": lines, "status": status})
        current_status["timestamp"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return current_status

    except Exception as e:
        # Create a timestamp for the log file
        timestamp = time.strftime("%Y-%m-%d_%H_%M_%S")
        log_filename = f"logs/failure_{timestamp}.html"

        # Ensure the logs directory exists
        os.makedirs("logs", exist_ok=True)

        # Save the HTML content to a file
        with open(log_filename, "w", encoding="utf-8") as f:
            f.write(str(html_content))

        print(f"An error occurred. HTML content saved to {log_filename}")
        raise e  # Re-raise the exception after logging


# %% Importing previously stored data

# If a file with operational status history exists, we will import it and
# then append any new data to the bottom of it
operation_raw = pd.read_pickle(
    f"s3://{bucket}/operation_raw.pkl", storage_options=storage_options
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


# %% Combining newly scraped data with previously scraped data

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

# Exporting raw data to cloud storage and confirming success
operation_raw_updated.to_pickle(
    f"s3://{bucket}/operation_raw.pkl",
    storage_options=storage_options,
)

print(
    f"""Data on the metro's operational status successfully scraped
    and exported to cloud storage as of {formatted_timestamp}."""
)

# %%
