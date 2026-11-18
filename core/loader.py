from dataclasses import dataclass

import pandas as pd

from core.filters import (
    filter_by_date_range,
    filter_by_day_type,
    filter_by_hour_type,
    filter_by_line,
    filter_downtime,
)
from core.helpers import (
    deal_with_downtime,
    deal_with_missing_mapping,
    get_period_string,
)


@dataclass
class PageData:
    df: pd.DataFrame
    min_date: object
    max_date: object
    n_days: int
    selected_period: str
    downtime_msg: str


@dataclass
class PageFilters:
    min_date: object
    max_date: object
    day_types: list
    hour_types: list
    selected_lines: list
    selected_rows: list


def set_page_filters(apply_date_filter: bool = True):

    # Setting up sidebar filters for the data
    min_date, max_date = filter_by_date_range()
    day_types = filter_by_day_type()
    hour_types = filter_by_hour_type()
    selected_lines = filter_by_line()
    selected_rows = filter_downtime()

    # Checking for missing mapping entries
    deal_with_missing_mapping()

    return PageFilters(
        min_date=min_date,
        max_date=max_date,
        day_types=day_types,
        hour_types=hour_types,
        selected_lines=selected_lines,
        selected_rows=selected_rows,
    )


def get_filtered_data(
    conn,
    page_filters,
    table_name: str = "operation_fmt",
    apply_date_filter: bool = True,
) -> PageData:

    # Extracting info from page filters
    min_date = page_filters.min_date
    max_date = page_filters.max_date
    day_types = page_filters.day_types
    hour_types = page_filters.hour_types
    selected_lines = page_filters.selected_lines
    selected_rows = page_filters.selected_rows

    # Confirming the selected period
    selected_period = get_period_string(min_date, max_date)

    # Preparing params and optional date filter
    if apply_date_filter:
        date_filter = """
        AND date >= ?
        AND date <= ?
        """
        params = [
            min_date.strftime("%Y-%m-%d"),
            max_date.strftime("%Y-%m-%d"),
            day_types,
            hour_types,
            selected_lines,
        ]
    else:
        date_filter = ""
        params = [day_types, hour_types, selected_lines]

    # WIP: Deal with missing rush hour (and maybe other)  columns
    # in the station_impact table in the DB: consider having
    # thesefilters at the bottom of the query and params.appenx(x) if table_name = "operation_fmt"

    # Querying the database for data
    query = f"""
        SELECT *
        FROM {table_name}
        WHERE 1=1
            {date_filter}
            AND day_type IN ?
            AND official_rush_hour IN ?
            AND line IN ?
        ORDER BY line, date;
    """
    df = conn.execute(query, params).df()

    # Correcting certain dtypes from the database
    # Note: these are needed because of inconsistencies between
    # different versions of pandas and some other pkgs
    df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ns]")

    # Confirming the number of days fetched
    n_days = df["date"].nunique()

    # Checking for system downtime
    df, downtime_msg = deal_with_downtime(df, selected_rows)

    return PageData(
        df=df,
        min_date=min_date,
        max_date=max_date,
        n_days=n_days,
        selected_period=selected_period,
        downtime_msg=downtime_msg,
    )
