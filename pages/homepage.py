"""
============
Welcome page
============

Author: github.com/cyrilby
Last meaningful update: 14-10-2026
"""

# %% Setting up

import streamlit as st

from core.helpers import (
    deal_with_missing_mapping,
    get_latest_status,
    get_unique_days,
    load_text,
)

# Importing long text strings used in the app
text = load_text()


# %% Page content

# Page title displayed on top
st.header("Welcome to the CPH metro status app!")

# Checking for missing mapping entries
deal_with_missing_mapping()

# Informs the user of the app's purpose
st.markdown(text["home_msg_1"])
st.markdown(text["home_msg_2"])
st.image("resources/metro_map_with_stations.png")

# Fetching relevant bits of data
_, _, n_days, _ = get_unique_days()

# Displaying the latest service message from the metro's website
st.subheader("Latest service status data", divider="grey")

most_recent, last_update_date, last_update_time = get_latest_status()
msg_text = text["home_msg_3"]
msg_text = msg_text.format(
    last_update_date=last_update_date, last_update_time=last_update_time
)
st.markdown(msg_text)
st.dataframe(most_recent)

# Displaying more info on how the app can help the user
st.subheader("How this app can help you", divider="grey")
st.markdown(text["home_msg_4"])
msg_text = text["home_msg_5"]
msg_text = msg_text.format(n_days=n_days)
st.markdown(msg_text)

# Displaying more info on how to use the app
st.subheader("How to use this app", divider="grey")
st.markdown("This app consists of the following two panes:")
st.markdown(text["home_msg_6"])
# st.markdown(
#     "To **switch between different pages**, please click on the page title:"
# )
st.image("resources/pages_examples.PNG")
st.markdown("You will then be redirected to the desired page.")

# Displaying more info on how the user can filter the data
st.subheader("How to apply filters to the data", divider="grey")
st.markdown(text["home_msg_7"])
st.image("resources/filter_date.PNG")
st.markdown(text["home_msg_8"])
st.image("resources/filter_lines.PNG")
st.markdown(text["home_msg_9"])
st.image("resources/selection_warning.PNG")
st.markdown(text["home_msg_10"])
st.divider()
st.markdown("*Front page image source: Metroselskabet (www.m.dk)*")


# %%
