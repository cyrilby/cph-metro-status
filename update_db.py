"""
===============================
Updating the project's database
===============================

Author: kirilboyanovbg[at]gmail.com
Last meaningful update: 05-03-2024

This script is designed to refresh all tables in the SQLite database
created for the project. Tables are updated by truncating them and
then inserting all relevant data, thereby keeping only the most recent
version of the data in them.
"""

# %% Setting things up

# Importing relevant packages
import pandas as pd
import numpy as np
import datetime as dt
import sqlite3

# URL for the database
db_url = "data/project_data.db"

# Setting up connection to the databse
conn = sqlite3.connect(db_url)
cursor = conn.cursor()


# %% Creating relevant tables in the database file

# Note: this is a one-time operation: therefore, the code is commented out
# for any subsequent runs of the script

# Creating relevant tables
# create_table_raw = ' '.join(open('queries/create_table_raw.sql').read().splitlines())
# cursor.execute(create_table_raw)


# %% Updating table with raw data

print("Updating table with raw data in progress...")

# Importing the latest data
operation_raw = pd.read_pickle("data/operation_raw.pkl")

# Updating data in the database
operation_raw.to_sql("raw.StatusUpdates", conn, if_exists="replace", index=False)

print("Updating table with raw data successfully completed.")


# %% Previewing and using data from SQLite database

# Execute the query
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

# Fetch all the results
tables = cursor.fetchall()
for table in tables:
    print(table[0])

# Example of reading data back
read_table_raw = " ".join(open("queries/read_table_raw.sql").read().splitlines())
temp_data = pd.read_sql(read_table_raw, conn)


# %% Closing the connection to the database

conn.close()
