"""
============================================
Defining custom theme and colors for the app
============================================

Author: github.com/cyrilby
Last meaningful update: 13-10-2026
"""

import plotly.io as pio
import streamlit as st


def add_logo():
    """
    Adds the MindGraph logo to the upper left corner of the page.
    """
    st.logo(
        "https://mindgraph.dk/logo.svg",
        size="large",
        link="https://mindgraph.dk",
        icon_image="https://mindgraph.dk/favicon.svg",
    )


def apply_theme():
    # Defining custom color palette
    my_custom_palette = ["#1ea2b5", "#324b4f", "#95b0b5", "#9f8ac3", "#6b588d"]

    # Transforming it into a plotly palette
    my_template = pio.templates["plotly_white"].layout.template
    my_template.layout.colorway = my_custom_palette
    pio.templates["my_custom_template"] = my_template
    pio.templates.default = "my_custom_template"


# Replacing the line on top of the app with a custom color
# Replace the gradient with a solid color (e.g., blue)
def customize_colors():
    """
    Allows for customization of streamlit top bar and the color
    of the links, which are both not directly editable via Python.
    Instead, this function uses HTML-based CSS injection to make
    the required changes.
    """

    # Change the color of the top bar
    st.markdown(
        """
        <style>
            div[data-testid="stDecoration"] {
                background: #1ea2b5;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Change the color of links (a href)
    st.markdown(
        """
        <style>
        div[data-testid="stMarkdownContainer"] a {
            color: #1ea2b5 !important;
        }
        div[data-testid="stMarkdownContainer"] a:hover {
            color: #1ea2b5 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
