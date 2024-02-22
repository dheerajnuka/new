#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 12:51:09 2023

@author: aho20
"""

import pandas as pd
import numpy as np
import uuid


import streamlit as st
import time
import datetime
from datetime import timedelta

# plotting libraries
import altair as alt

from streamlit_extras.switch_page_button import switch_page

# Set page configuration.
st.set_page_config(
    page_title="Anomaly Snapshots",
    # page_icon="🧊",
    layout="wide",
)

# This is to setup dynamic row creation for the number of sensors we're looking at
if "rows" not in st.session_state:
    st.session_state["rows"] = []


def add_row():
    element_id = uuid.uuid4()
    st.session_state["rows"].append(str(element_id))


# Read from anomaly detection csv to get the list of anomaly sensors
anomalies = pd.read_csv("/files/anomaly-detection.csv")
anomalies = anomalies.drop(columns=["Unnamed: 0"])
sensor_list = anomalies.columns.tolist()
sensor_perc_change = []

# Create redirect dict each time, if it is empty, we do not trigger
if "redirect" not in st.session_state:
    st.session_state.redirect = {}

# Iterate through the sensors first to generate the percentage change
for sensorname in sensor_list:
    sensorname_plain = sensorname.replace(" ", "_")

    plot_anomaly = pd.read_csv(
        f"/files/{sensorname_plain}_plot_anomaly.csv.gz", parse_dates=["ds"]
    )

    # for each sensor find the number of anomalies generated in the past week
    anomalies_pastweek = plot_anomaly[
        plot_anomaly["ds"]
        >= pd.to_datetime(datetime.date.today()) - pd.to_timedelta("7day")
    ]

    # for each sensor find the number of anomalies generated 2 weeks before
    anomalies_weekbefore = plot_anomaly[
        (
            plot_anomaly["ds"]
            >= pd.to_datetime(datetime.date.today()) - pd.to_timedelta("14day")
        )
        & (
            plot_anomaly["ds"]
            < pd.to_datetime(datetime.date.today()) - pd.to_timedelta("7day")
        )
    ]

    # find the sums for each week
    anomalies_pastweek_sum = float(anomalies_pastweek.trend.sum())
    anomalies_weekbefore_sum = float(anomalies_weekbefore.trend.sum())

    # calculate percentage change
    try:
        # this will throw out a divide by 0 error if not caught, this is for anomalies that have risen up from 0
        if anomalies_weekbefore_sum == 0 and anomalies_pastweek_sum != 0:
            perc_change = 100
        else:
            perc_change = round(
                (
                    (anomalies_pastweek_sum - anomalies_weekbefore_sum)
                    / anomalies_weekbefore_sum
                )
                * 100
            )
    except Exception:
        perc_change = -np.inf

    # append the percentage changes as a list
    sensor_perc_change.append((sensorname, perc_change, round(anomalies_pastweek_sum)))

# sort by descending
sensor_perc_change.sort(key=lambda i: i[1], reverse=True)

# add the number of rows as anomalies
for r in sensor_perc_change:
    add_row()

st.title("Sensor Anomaly Snapshot")

# Display the sensor, anomaly plot, percentage change and button to redirect for analysis
for (sensorname, perc_change, anomalies_pastweek_sum) in sensor_perc_change:
    # Read the preloaded files for each anomlay sensor
    sensorname_plain = sensorname.replace(" ", "_")
    dftimeseries = pd.read_csv(
        f"/files/{sensorname_plain}_dftimeseries.csv.gz", parse_dates=["ds"]
    )
    series_frame = pd.read_csv(
        f"/files/{sensorname_plain}_series_frame.csv.gz", parse_dates=["ds"]
    )
    plot_anomaly = pd.read_csv(
        f"/files/{sensorname_plain}_plot_anomaly.csv.gz", parse_dates=["ds"]
    )

    # if the percentage change is negative infinity, change it to no change, otherwise display change
    if np.isneginf(perc_change):
        perc_change = None
    else:
        perc_change = f"{perc_change} %"

    # Create row with 4 columns to hold data
    row_container = st.empty()
    row_columns = row_container.columns([0.1, 0.2, 0.6, 0.1])

    # first column is the sensorname
    row_columns[0].write(f"{sensorname}")

    # second column display the metric of anomaly counts
    row_columns[1].metric(
        label=f"Anomaly Count {sensorname}",
        value=f"{anomalies_pastweek_sum}",
        delta=perc_change,
        delta_color="off",
    )

    try:
        # Export the systemids that are anomalous
        df = pd.read_csv(
            f"/files/{sensorname_plain}_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
            parse_dates=["RESULTTIME", "RESULTDATE"],
        )
        anomalies_week = df[
            df["RESULTTIME"]
            >= pd.to_datetime(datetime.date.today()) - pd.to_timedelta("7day")
        ]
        row_columns[1].download_button(
            "Export System IDs",
            data=anomalies_week.to_csv(),
            file_name="system_ids.csv",
            key=f"{sensorname_plain}_export_ids",
        )
    except FileNotFoundError:
        row_columns[1].write("No Systems Found")

    # third column. Create slider so that users can adjust the dates that they want. Slider has to go before the charts
    values = row_columns[2].slider(
        "Select a range of values",
        dftimeseries.ds.dt.date.max() - timedelta(days=170),
        dftimeseries.ds.dt.date.max(),
        (
            dftimeseries.ds.dt.date.max() - timedelta(days=170),
            dftimeseries.ds.dt.date.max(),
        ),
        key=f"{sensorname_plain}_anomaly_plotting",
    )

    # user slider values to filter the dataframes
    dftimeseries = dftimeseries[
        (dftimeseries["ds"] >= pd.to_datetime(values[0]))
        & (dftimeseries["ds"] <= pd.to_datetime(values[1]))
    ]
    series_frame = series_frame[
        (series_frame["ds"] >= pd.to_datetime(values[0]))
        & (series_frame["ds"] <= pd.to_datetime(values[1]))
    ]
    plot_anomaly = plot_anomaly[
        (plot_anomaly["ds"] >= pd.to_datetime(values[0]))
        & (plot_anomaly["ds"] <= pd.to_datetime(values[1]))
    ]

    # create 3 plots to plot the time series, trend and anomalies
    a = (
        alt.Chart(dftimeseries)
        .mark_line()
        .encode(x="ds:T", y="y", text="ds", color=alt.value("blue"))
    )
    b = (
        alt.Chart(series_frame)
        .mark_line()
        .encode(x="ds:T", y="trend", color=alt.value("green"))
    )
    c = (
        alt.Chart(
            plot_anomaly,
            title=f"""Device Sensor ({sensorname}) Anomaly Detection - Isolation Forest""",
        )
        .mark_point(filled=True)
        .encode(
            x=alt.X("ds:T", title="Date"),
            y=alt.Y("trend", title="Sensor (Count)"),
            color=alt.value("red"),
        )
    )

    # Combine all three layers to make one chart
    d = alt.layer(a, b, c)

    # third column:display chart
    row_columns[2].altair_chart(d, use_container_width=True)

    # create button to direct to new page for analysis of selected dates
    row_columns[3].write("Go to analyze selected dates")
    redirect_bt = row_columns[3].button(":arrow_right:", key=f"{sensorname_plain}")
    if redirect_bt:
        st.session_state.redirect = {
            "from_date": values[0],
            "to_date": values[1],
            "sensor": sensorname,
        }
        switch_page("sensorscape_st")
