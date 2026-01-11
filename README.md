[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://cph-metro.streamlit.app/)
[![MindGraph](https://img.shields.io/badge/Product%20of%20-MindGraph.dk-1ea2b5?logo=https://mindgraph.dk/favicon.png")](https://mindgraph.dk)

# Copenhagen metro operations monitoring tool

* Author: Kiril (GitHub@cyrilby)
* Website: [mindgraph.dk](https://mindgraph.dk)
* Last meaningful update: 11-01-2026

This project is centered around automatically gathering data from the [Copenhagen Metro's website](https://m.dk) and monitoring its operations over time in order to find out how often (and when) things are most likely to break. To facilitate the communication of the insights gathered, the data is presented visually using a [Streamlit app](https://cph-metro.streamlit.app/).

The project consists of four scripts, which are described shortly below.

## Current status

* The raw data fetched in the process is then stored in a `pickle` file, after which point it is subjected to further data processing which standardizes the operational status messages and details their implications for passengers.
* The web scraping of the data requires the use of a browser instance (implemented using the `selenium` package) to download the correct data.
* Finally, some data visualizations are prepared based on the processed data and are made available via the official [Streamlit app](https://cph-metro.streamlit.app/).
* All data used in the process are stored on Azure, with free public read-only access. The data are also presented in a Streamlit app, as described in the next section.

## app.py

This script contains the source code of the Streamlit app accompanying the CPH metro scraper tool. In here, we create a series of data visualizations that help us get a better understanding of how often the metro breaks down, when and where the impact is felt as well as what the reasons behind the breakdowns are (if information on those is available). The streamlit app can then be run locally or accessed through [its website](https://cph-metro.streamlit.app/).

## web_scraping.py

This script is designed to automatically collect data on the operational status of the Copenhagen Metro and record disruptions. In practice, this happens by scraping the Metro's website, locating the relevant information and then storing it on Azure as a `*.pkl` file.

## data_cleaning.py

In this script, we import data on the Copenhagen Metro's operational status collected at different timestamps, then add some information on what the status recorded means, then create various tables containing aggregate data. These tables are then exported to Azure and can be used for data visualization etc.

## Jupyter notebooks for sanity checks

There are two Jupyter notebooks saved under the `notebooks` folder that can help with the following kinds of sanity checks:

- Check for the latest operational status, including any unmapped messages from the metro's operational status records (useful to check whether the scraper is running as intended and whether the mapping tables are fully up-to-date)
- Check the HTML code of the web page scraped in case the website has been edited (useful to making adjustments to the scraping code)