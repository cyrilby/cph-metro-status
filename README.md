[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://cph-metro.streamlit.app/)
[![MindGraph](https://img.shields.io/badge/Product%20of%20-MindGraph.dk-1ea2b5?logo=https://mindgraph.dk/favicon.png")](https://mindgraph.dk)
[![RelatedRepos](https://img.shields.io/badge/related-repos-yellow)](https://github.com/cyrilby/cph-metro-status-app)

# Copenhagen metro operations data tool

* Author: Kiril (GitHub@cyrilby)
* Website: [mindgraph.dk](https://mindgraph.dk)
* Last meaningful update: 19-11-2026

This project is centered around automatically gathering data from the [Copenhagen Metro's website](https://m.dk) and monitoring its operations over time in order to find out how often (and when) things are most likely to break. This repository contains the code behing the **data sourcing and cleaning**.

To facilitate the communication of the insights gathered, the data is presented visually using a [Streamlit app](https://cph-metro.streamlit.app/). The code behind the app can be found in [this related repository](https://github.com/cyrilby/cph-metro-status-app).

The project consists of four scripts, which are described shortly below.

## Requirements

- Python 3.12.10
- Python packages as specified in the `requirements.txt` file

## Functionalities

- The **raw data** fetched in the process is then stored in a `pickle` file which is then written to S3-compatible storage.
  - The web scraping of the data requires the use of a browser instance (implemented using the `selenium` package) to download the correct data.
- The data collected via the web scraper is **enriched** by performing a standardized set of additional calculations and manually maintained mapping tables, with the clean data being exported to a `DuckDB` database file, also stored in S3-compatible storage.
  - In this part, the focus is on standardizing the operational status messages and assessing their implications for passengers.
- **Insights** based on the collected clean data are also presented in a Streamlit app, which is contained in a [separate repository](https://github.com/cyrilby/cph-metro-status-app).

## Description of the individual scripts

### web_scraping.py

This script is designed to automatically collect data on the operational status of the Copenhagen Metro and record disruptions. In practice, this happens by scraping the Metro's website, locating the relevant information and then storing it in the cloud as a `*.pkl` file.

### data_cleaning.py

In this script, we import data on the Copenhagen Metro's operational status collected at different timestamps, then add some information on what the status recorded means, then create various tables containing aggregate data. These tables are then exported to cloud storage and can be used for data visualization etc.

### data_backup.py

This simple script is designed to import the latest raw data from cloud storage, then create a copy of the file in the `backup` folder with a timestamp. This operation is scheduled a few times a week to ensure an accidental file corruption does not result in losing all the data we have.

### data_checks.py

This script is designed to check the latest status of the raw data collection (web scraping) jobs as well as to quickly check whether there may be any status messages that have not yet been mapped to the standardized operational statuses.