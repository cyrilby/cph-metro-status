[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://cph-metro.streamlit.app/)

# Copenhagen metro operations monitoring tool

* Author: kirilboyanovbg[at]gmail.com
* Last meaningful update: 05-12-2024

This project is centered around automatically gathering data from the [Copenhagen Metro's website](https://m.dk) and monitoring its operations over time in order to find out how often (and when) things are most likely to break. To facilitate the communication of the insights gathered, the data is presented visually using a [Streamlit app](https://cph-metro.streamlit.app/).

The project consists of four scripts, which are described shortly below.

## Current status

* The raw data fetched in the process is then stored in a `pickle` file, after which point it is subjected to further data processing which standardizes the operational status messages and details their implications for passengers.
* The web scraping of the data requires the use of a browser instance (implemented using the `selenium` package) to download the correct data.
* Finally, some data visualizations are prepared based on the processed data and are made available via the official [Streamlit app](https://cph-metro.streamlit.app/).
* All data used in the process are stored on Azure, with free public read-only access. The data are also presented in a Streamlit app, as described in the next section.

## app.py

This script contains the source code of the Streamlit app accompanying the CPH metro scraper tool. In here, we create a series of data visualizations that help us get a better understanding of how often the metro breaks down, when and where the impact is felt as well as what the reasons behind the breakdowns are (if information on those is available). The streamlit app can then be run locally or accessed through [its website](https://cph-metro.streamlit.app/).

## get_data.py and get_data_chrome.py

This script is designed to automatically collect data on the operational status of the Copenhagen Metro and record disruptions. In practice, this happens by scraping the Metro's website, locating the relevant information and then storing it on Azure as a `*.pkl` file.

If using **Microsoft Edge**, there is no need to download or install any additional software. However, if using **Google Chrome**, the user must update the files in the `chromedriver-win64` folder with a version that matches the version of Chrome installed on the system. The newest `chromedriver` can be downloaded from [this page](https://chromedriver.chromium.org/downloads).

**Please note** that the `get_data_chrome.py` script also closes all running instances of Chrome once the scraping is done. This is to ensure that Windows Task Scheduler won't launch too many concurrent instances of Chrome, which may eventually lead to crashing the OS, however, this is not a suitable solution if Google Chrome is the default browser on the system.

## summarize_data.py

In this script, we import data on the Copenhagen Metro's operational status collected at different timestamps, then add some information on what the status recorded means, then create various tables containing aggregate data. These tables are then exported to Azure and can be used for data visualization etc.