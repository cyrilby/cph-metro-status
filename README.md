# Operational status of the Copenhagen metro

This project contains a web scraper that fetches current operational status of the Copenhagen metro as well as a tool that helps to visualize the data. The web scraping of the data requires the use of a browser instance (implemented using the `selenium` package) to download the correct data.

The raw data fetched in the process is then stored in a `parquet` file, after which point it is subjected to further data processing which standardizes the operational status messages and details their implications for passengers. Finally, some data visualizations are prepared based on the processed data. Please note that the latter is in the idea phase as of 05-03-2024.
