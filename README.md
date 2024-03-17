# Copenhagen metro operations monitoring tool

* Author: kirilboyanovbg[at]gmail.com
* Last meaningful update: 17-03-2024

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://cph-metro.streamlit.app/)

This project is centered around automatically gathering data from the [Copenhagen Metro's website](https://m.dk) and monitoring its operations over time in order to find out how often (and when) things are most likely to break. To facilitate the communication of the insights gathered, the data is presented visually using a [Streamlit app](https://cph-metro.streamlit.app/) (the app is still under active development as of 17-03-2024).

The project consists of four scripts, which are described shortly below.

## Current status

* The raw data fetched in the process is then stored in a `parquet` file, after which point it is subjected to further data processing which standardizes the operational status messages and details their implications for passengers.
* The web scraping of the data requires the use of a browser instance (implemented using the `selenium` package) to download the correct data.
* Finally, some data visualizations are prepared based on the processed data. Please note that the latter is in the idea phase as of 05-03-2024.

## get_data.py

This script is designed to automatically collect data on the operational status of the Copenhagen Metro and record disruptions. In practice, this happens by scraping the Metro's website, locating the relevant information and then storing it in local `*.csv` and `*.pkl` files.

## summarize_data.py

In this script, we import data on the Copenhagen Metro's operational status collected at different timestamps, then add some information on what the status recorded means, then create various tables containing aggregate data. These tables are then exported and can be used for data visualization etc.

## upload_data.py

This script is designed to refresh all output data tables in the relevant Azure data lake blob so that we can enable subsequent queries to get the data and use it for e.g. data visualization purposes. The data stored on Azure is then imported for use in the Streamlit app associated with the project.

## visualize_data.py

This script contains the source code of the Streamlit app accompanying the CPH metro scraper tool. In here, we create a series of data visualizations that help us get a better understanding of how often the metro breaks down, when and where the impact is felt as well as what the reasons behind the breakdowns are (if information on those is available). The streamlit app can then be run locally or accessed through [its website](https://cph-metro.streamlit.app/).
