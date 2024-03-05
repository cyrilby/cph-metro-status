# Copenhagen Metro operation monitoring tool

* Author: kirilboyanovbg[at]gmail.com
* Last meaningful update: 20-02-2024

This project is centered around automatically gathering data from the [Copenhagen Metro's website](https://m.dk) and monitoring its operations over time in order to find out how often (and when) things are most likely to break. The project consists of two scripts, which are described shortly below.

## get_data.py

This script is designed to automatically collect data on the operational status of the Copenhagen Metro and record disruptions. In practice, this happens by scraping the Metro's website, locating the relevant information and then storing it in a local `*.csv` file.

### bug fix from 20-02-2024

Due to an issue with the website, whenever the data was loaded using `BeautifulSoup`, it was incomplete and always showed the same status message. This issue was fixed by using `Selenium` instead, which launches a hidden window and loads the data through a web browser before parsing the HTML contents. With it, the status collected from the tool matches the actual status displayed when visiting the Metro's website.

As a consequence of this bug, the **data collection has been restarted**, meaning that all previous historical data have been deleted.

## summarize_data.py

In this script, we import data on the Copenhagen Metro's operational status collected at different timestamps, then add some information on what the status recorded means, then create various tables containing aggregate data. These tables are then exported and can be used for data visualization etc.