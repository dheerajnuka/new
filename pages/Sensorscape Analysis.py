# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 15:39:32 2023

@author: umuruga3
"""

import streamlit as st
import plotly.express as px
import pandas as pd
import altair as alt
import datetime
from datetime import date, timedelta
import time
import os

# from Home import test_import_cache
from Data_Backend import Data_process

# import logging,warnings
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import random

# change 8/7 Snowpark integration for performance enhancement
from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
from snowflake.snowpark.window import Window


st.set_page_config(
    page_title="SensorScape Dashboard", page_icon="Active", layout="wide"
)

# read the preloaded files
@st.cache_data(show_spinner=False, ttl=timedelta(days=1))
def read_preloaded_files(sensorname):
    sensorname = sensorname.replace(" ", "_")
    df_patch = pd.read_csv(f"/files/{sensorname}_sql_patch_query.csv.gz")
    df_controller = pd.read_csv(f"/files/{sensorname}_sql_controller_query.csv.gz")
    df_software = pd.read_csv(f"/files/{sensorname}_sql_software_query.csv.gz")
    df_software_change = pd.read_csv(f"/files/{sensorname}_sql_softchange_query.csv.gz")
    df_bios = pd.read_csv(f"/files/{sensorname}_sql_bios_query.csv.gz")
    df_appfaults_7days = pd.read_csv(
        f"/files/{sensorname}_7day_sql_appfaults_query.csv.gz"
    )
    df_appfaults_30days = pd.read_csv(
        f"/files/{sensorname}_30day_sql_appfaults_query.csv.gz"
    )
    df_appusage = pd.read_csv(f"/files/{sensorname}_sql_appusage_query.csv.gz")
    df_sys_mft = pd.read_csv(f"/files/{sensorname}_sys_mft_query.csv.gz")
    df_sys_model = pd.read_csv(f"/files/{sensorname}_sys_model_query.csv.gz")
    df_sys_location = pd.read_csv(f"/files/{sensorname}_sys_location_query.csv.gz")
    df_sys_info = pd.read_csv(f"/files/{sensorname}_sys_info_query.csv.gz")
    df_jobtitle = pd.read_csv(f"/files/{sensorname}_sys_jobtitle_query.csv.gz")
    df_hs_7days = pd.read_csv(f"/files/{sensorname}_7day_sys_hs_query.csv.gz")
    df_hs_30days = pd.read_csv(f"/files/{sensorname}_30day_sys_hs_query.csv.gz")

    return (
        df_patch,
        df_controller,
        df_software,
        df_software_change,
        df_bios,
        df_appfaults_7days,
        df_appfaults_30days,
        df_appusage,
        df_sys_mft,
        df_sys_model,
        df_sys_location,
        df_sys_info,
        df_jobtitle,
        df_hs_7days,
        df_hs_30days,
    )


def process_data_comparison(df_wguid, wguid_list, date_from, date_to):
    # Get data for comparison report
    my_bar.progress(20, text="Data retrival in progress: Patch Update")
    df_sensors = Data_process.fetch_comparison_sensors(
        wguid_list=wguid_list, date_from=date_from, date_to=date_to
    )
    my_bar.progress(40, text="Data retrival in progress: Controller Update")
    df_apps = Data_process.fetch_comparison_apps(
        wguid_list=wguid_list, date_from=date_from, date_to=date_to
    )
    my_bar.progress(60, text="Data retrival in progress: Installed Software")
    df_drivers = Data_process.fetch_comparison_drivers(
        wguid_list=wguid_list, date_from=date_from, date_to=date_to
    )
    my_bar.progress(80, text="Data retrival in progress: Software Changes")
    df_patch, df_software = Data_process.fetch_comparison_software_patch_upd(
        wguid_list=wguid_list, date_from=date_from, date_to=date_to
    )
    my_bar.progress(99, text="Preparing dashboard...")
    time.sleep(1)
    my_bar.empty()

    # machine/id labels
    input_machine_1 = df_wguid.iloc[0, 1]
    input_machine_2 = df_wguid.iloc[1, 1]

    # create header for comparison report
    c1, c2, c3 = st.columns(3)
    with st.container():
        with c1:
            st.header("Input Machine 1: " + input_machine_1)
        with c2:
            st.header("")
        with c3:
            st.header("Input Machine 2: " + input_machine_2)

    diff1, sim, diff2 = st.columns(3)
    with st.container():
        with diff1:
            st.header("Machine 1 Differences")
        with sim:
            st.header("Machine Similarities")
        with diff2:
            st.header("Machine 2 Differences")

    height_pixels = 200

    # Find Similar and Different Sensors
    df = pd.pivot_table(
        df_sensors,
        index="WGUID",
        columns="Sensor Name",
        values="SENSORSYSTEMID",
        aggfunc="count",
    )
    df_similar = df.loc[:, ~df.isnull().any()]
    df_diff = df.loc[:, df.isnull().any()]
    # display the sensor results
    st.divider()
    st.subheader("Sensor")
    c4, c5, c6 = st.columns(3)
    with st.container():
        with c4:
            if df_diff.shape[1] != 0:
                df = df_diff.loc[[df_wguid.iloc[0, 0]]]
                st.dataframe(
                    data=df.loc[:, (df >= 1).any()].columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
            # elif df_similar.shape[1] != 0 and df_wguid.iloc[0, 0] in df_similar.index:
            #     st.dataframe(
            #         data=df_similar.columns,
            #         use_container_width=True,
            #         hide_index=True,
            #         height=height_pixels,
            #     )
        with c5:
            if df_similar.shape[1] != 0:
                st.dataframe(
                    data=df_similar.columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
        with c6:
            if df_diff.shape[0] != 0:
                df = df_diff.loc[[df_wguid.iloc[1, 0]]]
                st.dataframe(
                    data=df.loc[:, (df >= 1).any()].columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
            # elif df_similar.shape[0] != 0 and df_wguid.iloc[1, 0] in df_similar.index:
            #     st.dataframe(
            #         data=df_similar.columns,
            #         use_container_width=True,
            #         hide_index=True,
            #         height=height_pixels,
            #     )

    # Find Similar and Different Installed Applications
    df = pd.pivot_table(
        df_apps,
        index="WGUID",
        columns="Installed Application",
        values="PACKAGE_ID",
        aggfunc="count",
    )
    df_similar = df.loc[:, ~df.isnull().any()]
    df_diff = df.loc[:, df.isnull().any()]

    # display the Installed Application results
    st.divider()
    st.subheader("Installed Applications")
    c7, c8, c9 = st.columns(3)
    # st.write(df_wguid)
    # st.write(df_diff, df_similar)
    # st.write(df_diff.index)
    with st.container():
        with c7:
            if df_diff.shape[1] != 0:
                df = df_diff.loc[[df_wguid.iloc[0, 0]]]
                # st.write(df)
                st.dataframe(
                    data=df.loc[:, (df >= 1).any()].columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
            # elif df_similar.shape[1] != 0 and df_wguid.iloc[0, 0] in df_similar.index:
            #     st.dataframe(
            #         data=df_similar.columns,
            #         use_container_width=True,
            #         hide_index=True,
            #         height=height_pixels,
            #     )

        with c8:
            if df_similar.shape[1] != 0:
                st.dataframe(
                    data=df_similar.columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
        with c9:
            if df_diff.shape[1] != 0:
                df = df_diff.loc[[df_wguid.iloc[1, 0]]]
                st.dataframe(
                    data=df.loc[:, (df >= 1).any()].columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
            # elif df_similar.shape[1] != 0 and df_wguid.iloc[1, 0] in df_similar.index:
            #     st.dataframe(
            #         data=df_similar.columns,
            #         use_container_width=True,
            #         hide_index=True,
            #         height=height_pixels,
            #     )

    # Find Similar and Different Drivers
    df = pd.pivot_table(
        df_drivers,
        index="WGUID",
        columns="Driver Name",
        values="CLASSGUID",
        aggfunc="count",
    )
    df_similar = df.loc[:, ~df.isnull().any()]
    df_diff = df.loc[:, df.isnull().any()]
    #     st.write(df_similar.columns,df_wguid.iloc[0,0] in df_similar.columns)

    # display the Drivers results
    st.divider()
    st.subheader("Drivers")
    c10, c11, c12 = st.columns(3)

    with st.container():
        with c10:
            if df_diff.shape[1] != 0:
                df = df_diff.loc[[df_wguid.iloc[0, 0]]]
                st.dataframe(
                    data=df.loc[:, (df >= 1).any()].columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
            # elif df_similar.shape[1] != 0 and df_wguid.iloc[0, 0] in df_similar.index:
            #     st.dataframe(
            #         data=df_similar.columns,
            #         use_container_width=True,
            #         hide_index=True,
            #         height=height_pixels,
            #     )

        with c11:
            if df_similar.shape[1] != 0:
                st.dataframe(
                    data=df_similar.columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
        with c12:
            if df_diff.shape[1] != 0:
                df = df_diff.loc[[df_wguid.iloc[1, 0]]]
                st.dataframe(
                    data=df.loc[:, (df >= 1).any()].columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
            # elif df_similar.shape[1] != 0 and df_wguid.iloc[1, 0] in df_similar.index:
            #     st.dataframe(
            #         data=df_similar.columns,
            #         use_container_width=True,
            #         hide_index=True,
            #         height=height_pixels,
            #     )

    # Find Similar and Different Patches
    df = pd.pivot_table(
        df_patch,
        index="WGUID",
        columns="Patch Name",
        values="PATCH_ID",
        aggfunc="count",
    )
    df_similar = df.loc[:, ~df.isnull().any()]
    df_diff = df.loc[:, df.isnull().any()]

    # display the Patches results
    st.divider()
    st.subheader("Patches")
    c13, c14, c15 = st.columns(3)

    with st.container():
        with c13:
            if df_diff.shape[1] != 0:
                df = df_diff.loc[[df_wguid.iloc[0, 0]]]
                st.dataframe(
                    data=df.loc[:, (df >= 1).any()].columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
            # elif df_similar.shape[1] != 0 and df_wguid.iloc[0, 0] in df_similar.index:
            #     st.dataframe(
            #         data=df_similar.columns,
            #         use_container_width=True,
            #         hide_index=True,
            #         height=height_pixels,
            #     )

        with c14:
            if df_similar.shape[1] != 0:
                st.dataframe(
                    data=df_similar.columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
        with c15:
            if df_diff.shape[1] != 0:
                df = df_diff.loc[[df_wguid.iloc[1, 0]]]
                st.dataframe(
                    data=df.loc[:, (df >= 1).any()].columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
            # elif df_similar.shape[1] != 0 and df_wguid.iloc[1, 0] in df_similar.index:
            #     st.dataframe(
            #         data=df_similar.columns,
            #         use_container_width=True,
            #         hide_index=True,
            #         height=height_pixels,
            #     )

    # Find Similar and Different Software Changes
    df = pd.pivot_table(
        df_software,
        index="WGUID",
        columns="Software Update Name",
        values="CHANGECLASS",
        aggfunc="count",
    )
    df_similar = df.loc[:, ~df.isnull().any()]
    df_diff = df.loc[:, df.isnull().any()]

    # display the Software Changes results
    st.divider()
    st.subheader("Software Changes")
    c16, c17, c18 = st.columns(3)

    with st.container():
        with c16:
            if df_diff.shape[1] != 0:
                df = df_diff.loc[[df_wguid.iloc[0, 0]]]
                st.dataframe(
                    data=df.loc[:, (df >= 1).any()].columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
            # elif df_similar.shape[1] != 0 and df_wguid.iloc[0, 0] in df_similar.index:
            #     st.dataframe(
            #         data=df_similar.columns,
            #         use_container_width=True,
            #         hide_index=True,
            #         height=height_pixels,
            #     )

        with c17:
            if df_similar.shape[1] != 0:
                st.dataframe(
                    data=df_similar.columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
        with c18:
            if df_diff.shape[1] != 0:
                df = df_diff.loc[[df_wguid.iloc[1, 0]]]
                st.dataframe(
                    data=df.loc[:, (df >= 1).any()].columns,
                    use_container_width=True,
                    hide_index=True,
                    height=height_pixels,
                )
            # elif df_similar.shape[1] != 0 and df_wguid.iloc[1, 0] in df_similar.index:
            #     st.dataframe(
            #         data=df_similar.columns,
            #         use_container_width=True,
            #         hide_index=True,
            #         height=height_pixels,
            # )


def process_data(sensorname, selection, df_6mnth=None):
    # If running the default 30 day look back, read from preloaded files, otherwise run queries.
    if st.session_state.preload:
        (
            df_patch,
            df_controller,
            df_software,
            df_software_change,
            df_bios,
            df_appfaults_7days,
            df_appfaults_30days,
            df_appusage,
            df_sys_mft,
            df_sys_model,
            df_sys_location,
            df_sys_info,
            df_jobtitle,
            df_hs_7days,
            df_hs_30days,
        ) = read_preloaded_files(sensorname)
    else:
        if len(wguid_list) > 16000:
            (
                df_patch,
                df_controller,
                df_software,
                df_software_change,
                df_bios,
                df_appfaults_7days,
                df_appfaults_30days,
                df_appusage,
                df_sys_mft,
                df_sys_model,
                df_sys_location,
                df_sys_info,
                df_jobtitle,
                df_hs_7days,
                df_hs_30days,
            ) = Data_process.fetch_data_large_wguid(
                sensorname, date_from, date_to, preload=False
            )
        else:
            (
                df_patch,
                df_controller,
                df_software,
                df_software_change,
                df_bios,
                df_appfaults_7days,
                df_appfaults_30days,
                df_appusage,
                df_sys_mft,
                df_sys_model,
                df_sys_location,
                df_sys_info,
                df_jobtitle,
                df_hs_7days,
                df_hs_30days,
            ) = Data_process.fetch_data(
                sensorname, wguid_list, date_from, date_to, preload=False
            )
    # df_patch,df_controller,df_software,df_software_change,df_bios,df_appfaults_7days,df_appfaults_30days,df_appusage,df_sys_mft,df_sys_model,df_sys_location,df_sys_info,df_jobtitle,df_hs_7days,df_hs_30days = dp.data_processing(df_ptch,df_contrl,df_software_per,df_softchange,df_bioschange,df_appfaults,df_appusge,df_sys_info,df_sys_job,df_sys_hs)
    my_bar.progress(99, text="Preparing dashboard...")
    time.sleep(1)
    my_bar.empty()

    # change 8/7 - Optum color palette
    optum_theme = [
        "#8061BC",
        "#D13F44",
        "#FF612B",
        "#F5B700",
        "#007C89",
        "#999999",
        "#BA5B3F",
        "#b3c763",
        "#0d88e6",
        "#00b7c7",
        "#8be04e",
        "#ebdc78",
        "#f46a9b",
        "#b3d4ff",
        "#00bfa0",
        "#fd7f6f",
        "#beb9db",
        "#fdcce5",
        "#8bd3c7",
    ]

    if selection == "By Machine":
        st.write("""## System Data Analysis""")
        st.write("This page shows the data analysis for the given systems")
    else:
        st.write("""## SensorScape Data Analysis""")
        sensorname = sensorname.replace(":", "\:")
        st.write(
            f"This page shows the data analysis for the sensor  **{sensorname}** from **{date_from}** to **{date_to}**"
        )

        # change 8-7 to add more information about sensors
        if ":" not in sensorname:
            with st.expander("See Sensor Description"):
                sensor_description = sensor_def.loc[
                    sensor_def["SENSORNAME"] == sensorname, "DESCRIPTION"
                ].item()
                st.write(sensor_description)

    # if we're not analyzing by machine wguids, then we want to display a line chart of sensor counts over time.
    if selection != "By Machine":
        with st.container():
            st.write(
                """ 
                         ### Sensor Timeline
                         """
            )
            sensor_line_chart = (
                alt.Chart(df_6mnth)
                .mark_line()
                .encode(x="DATE", y="Count", color=alt.value("#FF612B"))
            )
            st.altair_chart(sensor_line_chart, theme=None, use_container_width=True)
    # if analyzing by machines, display breakdown of sensors and the sensor timeline
    else:
        with st.container():
            st.write(
                """
                         ### Sensors
                         """
            )
            # Show a bar chart of sensors from the list of systems provided descending
            df_sensor_count = (
                df.reset_index().SENSORNAME.value_counts().to_frame().reset_index()
            )
            df_sensor_count.columns = ["SENSORNAME", "count"]
            df_sensor_count = df_sensor_count.sort_values(by=["count"], ascending=False)
            chart = (
                alt.Chart(df_sensor_count)
                .mark_bar()
                .encode(
                    x=alt.X("SENSORNAME", sort="-y", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("count"),
                    color=alt.Color("SENSORNAME", title=None, legend=None),
                    tooltip=["SENSORNAME", "count"],
                )
            )
            st.altair_chart(chart, theme="streamlit", use_container_width=True)
            st.write(
                """ 
                         ### Sensor Timeline
                         """
            )
            (m1, m2) = st.columns(2)
            with m1:

                df_6mnth.SENSORNAME.unique()
                option = st.selectbox(
                    "Select a sensor to view the timeline",
                    df_sensor_count.SENSORNAME.unique(),
                    placeholder=df_sensor_count.SENSORNAME.unique()[0],
                )
                sensor_timeline_filter = df_6mnth[df_6mnth["SENSORNAME"] == option]
                sensor_timeline_filter["DATE"] = pd.to_datetime(
                    sensor_timeline_filter["DATE"]
                )
                sensor_timeline_filter["DATE"] = sensor_timeline_filter[
                    "DATE"
                ].dt.strftime("%b %d %Y")
                sensor_line_chart = (
                    alt.Chart(sensor_timeline_filter)
                    .mark_line()
                    .encode(
                        alt.X("DATE", axis=alt.Axis(labelAngle=-45)),
                        y="Count",
                        color=alt.value("#FF612B"),
                    )
                )
                st.altair_chart(sensor_line_chart, theme=None, use_container_width=True)

    (c5, c6) = st.columns([3, 3])

    with st.container():

        with c5:
            st.write(
                """
                      ##### Employee Location
                      """
            )

            if len(df_sys_location) != 0:

                threshold = 0.05
                df_sys_location_grp = df_sys_location.groupby("LOCATION").sum()
                df_proportions = df_sys_location_grp / df_sys_location_grp.sum()

                df_below_thresh_mask = df_proportions < threshold
                df_plot_data = df_proportions[~df_below_thresh_mask]
                df_plot_data.loc["Other"] = df_proportions[df_below_thresh_mask].sum()
                df_plot_data.dropna(inplace=True)
                df_plot_data = df_plot_data * df_sys_location_grp.sum()
                df_plot_data.rename(
                    columns={"WGUID COUNT": "System Count"}, inplace=True
                )
                df_plot_data["City"] = df_plot_data.index

                application_colors = random.sample(optum_theme, k=df_plot_data.shape[0])

                fig = px.pie(
                    df_plot_data,
                    values="System Count",
                    color_discrete_sequence=application_colors,
                    names="City",
                    height=400,
                    width=400,
                )
                # fig.update_layout(margin=dict(l=50, r=50, t=10, b=0),)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.write(""" No Data Available""")

        with c6:

            st.write(
                """
                     ##### Employee Designation
                     """
            )

            if len(df_jobtitle) != 0:

                threshold = 0.05
                # df_jobtitle.to_excel(r'jobexcel_steam.xlsx')
                df_jobtitle_grp = df_jobtitle.groupby("JOB TITLE").sum()
                df_proportions = df_jobtitle_grp / df_jobtitle_grp.sum()

                df_below_thresh_mask = df_proportions < threshold
                df_plot_data = df_proportions[~df_below_thresh_mask]
                df_plot_data.loc["Other"] = df_proportions[df_below_thresh_mask].sum()
                df_plot_data.dropna(inplace=True)
                df_plot_data = df_plot_data * df_jobtitle_grp.sum()
                df_plot_data.rename(
                    columns={"WGUID COUNT": "System Count"}, inplace=True
                )
                df_plot_data["Designation"] = df_plot_data.index

                application_colors = random.sample(optum_theme, k=df_plot_data.shape[0])

                fig = px.pie(
                    df_plot_data,
                    values="System Count",
                    color_discrete_sequence=application_colors,
                    names="Designation",
                    height=400,
                    width=400,
                )
                # fig.update_layout(margin=dict(l=50, r=50, t=10, b=0),)
                st.plotly_chart(fig, use_container_width=True)
            else:

                st.write(
                    """
                      No Data available
                     """
                )

    c1, c2 = st.columns([2, 8])

    with st.container():

        with c1:
            st.image("image/laptop3.png", use_column_width="Auto")
            st.metric("Number of Systems:", len(wguid_list))

        with c2:
            st.write(
                """
                     #### System Health score 
                     """
            )

            df_hs_7days["Type"] = "Last 7 Days"
            df_hs_30days["Type"] = "Last 30 days"
            hs_merged = pd.concat([df_hs_7days, df_hs_30days])
            hs_merged.rename(
                columns={
                    "HEALTH_SCORE": "Health Score Category",
                    "WGUID_Count": "System Count",
                },
                inplace=True,
            )

            # change 7/27 to make chat more intuitive and interactive
            point = alt.selection_point(encodings=["y"])
            bar_chart = (
                alt.Chart(hs_merged)
                .mark_bar()
                .encode(
                    y=alt.Y("Type", title=None),
                    x="System Count",
                    color=alt.condition(
                        point,
                        alt.Color("Type:N", title=None, legend=None).scale(
                            range=["#007C89", "#FF612B"]
                        ),
                        alt.value("lightgray"),
                    ),
                    row="Health Score Category:N",
                )
                .add_selection(point)
            )

            st.altair_chart(bar_chart, use_container_width=True, theme=None)

    # change - create export for date range
    if "RESULTTIME" in df.columns:
        from_side, to_side, export_side = st.columns(3)
        with st.container():
            with from_side:
                from_date = from_side.date_input("From", default_date)
            with to_side:
                to_date = to_side.date_input("To", today)
            with export_side:
                date_from_download = pd.to_datetime(from_date)
                date_to_download = pd.to_datetime(to_date)
                csv = df.loc[:, ["SENSORSYSTEMID", "SENSORNAME", "RESULTTIME"]].rename(
                    columns={"RESULTTIME": "date"}
                )
                csv = csv[
                    (csv["date"] >= date_from_download)
                    & (csv["date"] <= date_to_download)
                ]
                st.write("")
                st.write("")
                # st.write(csv)
                export_side.download_button(
                    "Export System IDs",
                    data=csv.to_csv(date_format="%Y-%m-%d %H:%M:%S"),
                    file_name="system_ids.csv",
                )

    (
        c7,
        c8,
    ) = st.columns([3, 3])

    with st.container():
        with c7:
            st.write(
                """
                      ##### System Manufacture Year
                      """
            )

            if len(df_sys_mft) != 0:

                df_sys_mft["MANUFACTURED YEAR"] = pd.to_datetime(
                    df_sys_mft["MANUFACTURED YEAR"], format="%Y"
                ).dt.year
                df_sys_mft.rename(columns={"WGUID COUNT": "SYSTEM COUNT"}, inplace=True)
                bar_chart = (
                    alt.Chart(df_sys_mft)
                    .mark_bar(size=20, strokeWidth=50)
                    .encode(
                        y="SYSTEM COUNT",
                        x="MANUFACTURED YEAR",
                        color=alt.value("#007C89"),
                    )
                )

                st.altair_chart(bar_chart)
            else:
                st.write(""" No Data Available""")

        with c8:
            st.write(
                """
                     ##### System Model
                     """
            )

            if len(df_sys_model) != 0:
                threshold = 0.05

                df_sys_model = df_sys_model.groupby("MODEL").sum()
                df_proportions = df_sys_model / df_sys_model.sum()

                df_below_thresh_mask = df_proportions < threshold
                df_plot_data = df_proportions[~df_below_thresh_mask]
                df_plot_data.loc["Other"] = df_proportions[df_below_thresh_mask].sum()
                df_plot_data.dropna(inplace=True)
                df_plot_data = df_plot_data * df_sys_model.sum()
                df_plot_data.rename(
                    columns={"WGUID COUNT": "System Count"}, inplace=True
                )
                df_plot_data["MODEL"] = df_plot_data.index

                application_colors = random.sample(optum_theme, k=df_plot_data.shape[0])

                fig = px.pie(
                    df_plot_data,
                    values="System Count",
                    color_discrete_sequence=application_colors,
                    names="MODEL",
                    height=400,
                    width=600,
                )
                # fig.update_layout(margin=dict(l=50, r=50, t=10, b=0),)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.write(""" No Data Available""")

    # change - add filter for system model and system year and pass the relevant wguids to the system percentage graphs
    selection = st.selectbox(
        label="Select to filter by System Manufacture Year, System Model or None",
        options=(None, "System Model", "System Manufacture Year"),
    )
    wguid_filter = []

    if selection == "System Model":
        model_selection = st.selectbox("Select Model", df_sys_model.index)
        wguid_filter = df_sys_info[df_sys_info["MODEL"] == model_selection][
            "WGUID"
        ].tolist()
    elif selection == "System Manufacture Year":
        year_selection = st.selectbox("Select Year", df_sys_mft["MANUFACTURED YEAR"])
        df_sys_info["INVOICEDT_YEAR"] = pd.to_datetime(df_sys_info["INVOICEDT"]).dt.year
        wguid_filter = df_sys_info[df_sys_info["INVOICEDT_YEAR"] == year_selection][
            "WGUID"
        ].tolist()

    # change filter by system model or system year if the filter is not empty 8/4
    if len(wguid_filter) != 0:
        (
            df_patch,
            df_controller,
            df_software,
            df_software_change,
            df_bios,
            df_appfaults_7days,
            df_appfaults_30days,
            df_appusage,
            df_sys_mft,
            df_sys_model,
            df_sys_location,
            df_sys_info,
            df_jobtitle,
            df_hs_7days,
            df_hs_30days,
        ) = Data_process.fetch_data(
            sensorname, wguid_filter, date_from, date_to, preload=False
        )

    c1, c2 = st.columns(2)  # just to highlight these are different cols

    with st.container():
        with c1:
            st.write(
                """
                 ##### Application and its percentage of system presence 
                 """
            )

            app_expander = st.expander("See Details")
            app_expander.write(
                "This chart shows the percentage of each application occurring among sensor-triggered systems."
            )
            if len(df_software) != 0:

                # Get Top nth percentages
                df_software = df_software.head(15)

                df_software = df_software.sort_values(
                    by="SYSTEM_PERCENTAGE", ascending=False
                ).reset_index()

                df_software.fillna(
                    "", inplace=True
                )  # fill blank fields with empty strings
                df_software["APP"] = (
                    df_software["PACKAGENAME"] + " " + df_software["VERSION"]
                )
                # df_software.rename(columns={'packagename':'PACKAGE'},inplace=True)
                bar_chart = (
                    alt.Chart(df_software)
                    .mark_bar()
                    .encode(x=alt.Y("SYSTEM_PERCENTAGE"), y=alt.X("APP", sort="x"))
                )

                #                 st.altair_chart(bar_chart)

                # randomly select colors from optum color palette
                application_colors = random.sample(optum_theme, k=df_software.shape[0])

                # radial chart for system percentage
                df_software["shortened_name"] = (
                    df_software.PACKAGENAME.str[0:15] + "..."
                )
                base = alt.Chart(df_software).encode(
                    theta=alt.Theta("SYSTEM_PERCENTAGE:Q", stack=True),
                    radius=alt.Radius("SYSTEM_PERCENTAGE"),
                    order=alt.Order("SYSTEM_PERCENTAGE:Q"),
                    color=alt.Color("PACKAGENAME", title=None, legend=None).scale(
                        range=application_colors,
                        domain=df_software["PACKAGENAME"].tolist(),
                    ),
                    tooltip=["APP", "SYSTEM_PERCENTAGE"],
                    text="shortened_name",
                )
                adjust_arc = base.mark_arc(innerRadius=20, stroke="#fff")
                text_obj = base.mark_text(radiusOffset=50, angle=10, align="center")
                chart = adjust_arc + text_obj

                st.altair_chart(chart, theme=None, use_container_width=True)
                app_expander.dataframe(
                    df_software.nlargest(3, "SYSTEM_PERCENTAGE").loc[
                        :, ["APP", "VERSION", "SYSTEM_PERCENTAGE"]
                    ],
                    hide_index=True,
                )
            else:
                st.write(""" No Data Available""")

        with c2:
            st.write(
                """
                 ##### Software changes and its percentage of system affected 
                 """
            )

            software_expander = st.expander("See Details")
            software_expander.write(
                "This chart shows the percentage of each software change occurring among sensor-triggered systems."
            )
            if len(df_software_change) != 0:

                # Get Top nth percentages
                df_software_change = df_software_change.head(15)

                df_software_change = df_software_change.sort_values(
                    by="SYSTEM_PERCENTAGE", ascending=False
                ).reset_index()
                df_software_change.fillna(
                    "", inplace=True
                )  # fill blank fields with empty strings
                df_software_change["SOFTWARE CHANGE"] = (
                    df_software_change["SOFTWARE"]
                    + " "
                    + df_software_change["NEWVERSION"]
                )
                bar_chart = (
                    alt.Chart(df_software_change)
                    .mark_bar()
                    .encode(
                        x=alt.Y("SYSTEM_PERCENTAGE"),
                        y=alt.X("SOFTWARE CHANGE", sort="-x"),
                    )
                )

                #                 st.altair_chart(bar_chart)

                # randomly select colors from optum color palette
                df_software_change["shortened_name"] = (
                    df_software_change["SOFTWARE"].str[0:15] + "..."
                )
                application_colors = random.sample(
                    optum_theme, k=df_software_change.shape[0]
                )

                base = alt.Chart(df_software_change).encode(
                    theta=alt.Theta("SYSTEM_PERCENTAGE:Q", stack=True),
                    radius=alt.Radius("SYSTEM_PERCENTAGE"),
                    order=alt.Order("SYSTEM_PERCENTAGE:Q"),
                    color=alt.Color("SOFTWARE CHANGE", title=None, legend=None).scale(
                        range=application_colors,
                        domain=df_software_change["SOFTWARE CHANGE"].tolist(),
                    ),
                    tooltip=["SOFTWARE CHANGE", "SYSTEM_PERCENTAGE"],
                    text="shortened_name",
                )
                adjust_arc = base.mark_arc(innerRadius=20, stroke="#fff")
                text_obj = base.mark_text(radiusOffset=50, angle=10, align="center")
                chart = adjust_arc + text_obj

                st.altair_chart(chart, theme=None, use_container_width=True)
                software_expander.dataframe(
                    df_software_change.nlargest(3, "SYSTEM_PERCENTAGE").loc[
                        :, ["SOFTWARE CHANGE", "NEWVERSION", "SYSTEM_PERCENTAGE"]
                    ],
                    hide_index=True,
                )
            else:
                st.write(""" No Data Available""")

    c3, c4, c5 = st.columns(3)
    with st.container():
        with c3:

            st.write(
                """
                 ##### Patch changes and its percentage of system affected 
                 """
            )
            patch_expander = st.expander("See Details")
            patch_expander.write(
                "This chart shows the percentage of each patch change occurring among sensor-triggered systems."
            )
            if len(df_patch) != 0:

                # Get Top nth percentages
                df_patch = df_patch.head(15)

                df_patch.fillna(
                    "", inplace=True
                )  # fill blank fields with empty strings
                df_patch = df_patch.sort_values(
                    by="SYSTEM_PERCENTAGE", ascending=False
                ).reset_index()
                df_patch.rename(columns={"PATCHNAME": "PATCH"}, inplace=True)
                bar_chart = (
                    alt.Chart(df_patch)
                    .mark_bar()
                    .encode(x=alt.Y("SYSTEM_PERCENTAGE"), y=alt.X("PATCH", sort="-x"))
                )

                #                 st.altair_chart(bar_chart)

                # randomly select colors from optum color palette
                df_patch["shortened_name"] = df_patch["PATCH"].str[0:15] + "..."
                application_colors = random.sample(optum_theme, k=df_patch.shape[0])

                base = alt.Chart(df_patch).encode(
                    theta=alt.Theta("SYSTEM_PERCENTAGE:Q", stack=True),
                    radius=alt.Radius("SYSTEM_PERCENTAGE"),
                    order=alt.Order("SYSTEM_PERCENTAGE:Q"),
                    color=alt.Color("PATCH", title=None, legend=None).scale(
                        range=application_colors, domain=df_patch["PATCH"].tolist()
                    ),
                    tooltip=["PATCH", "SYSTEM_PERCENTAGE"],
                    text="shortened_name",
                )
                adjust_arc = base.mark_arc(innerRadius=20, stroke="#fff")
                text_obj = base.mark_text(radiusOffset=50, angle=10, align="center")
                chart = adjust_arc + text_obj

                st.altair_chart(chart, theme=None, use_container_width=True)
                patch_expander.dataframe(
                    df_patch.nlargest(3, "SYSTEM_PERCENTAGE").loc[
                        :, ["PATCH", "SYSTEM_PERCENTAGE"]
                    ],
                    hide_index=True,
                )
            else:
                st.write(""" No Data Available""")
        with c4:
            st.write(
                """
                     ##### Controller changes and its percentage of system affected 
                     """
            )
            controller_expander = st.expander("See Details")
            controller_expander.write(
                "This chart shows the percentage of each controller change occurring among sensor-triggered systems."
            )
            if len(df_controller) != 0:

                # Get Top nth percentages
                df_controller = df_controller.head(15)

                df_controller.fillna(
                    "", inplace=True
                )  # fill blank fields with empty strings
                df_controller = df_controller.sort_values(
                    by="SYSTEM_PERCENTAGE", ascending=False
                ).reset_index()
                bar_chart = (
                    alt.Chart(df_controller)
                    .mark_bar()
                    .encode(
                        x=alt.Y("SYSTEM_PERCENTAGE"), y=alt.X("CONTROLLER", sort="-x")
                    )
                )

                #                     st.altair_chart(bar_chart)

                df_controller["shortened_name"] = (
                    df_controller["CONTROLLER"].str[0:15] + "..."
                )
                application_colors = random.sample(
                    optum_theme, k=df_controller.shape[0]
                )

                base = alt.Chart(df_controller).encode(
                    theta=alt.Theta("SYSTEM_PERCENTAGE:Q", stack=True),
                    radius=alt.Radius("SYSTEM_PERCENTAGE"),
                    order=alt.Order("SYSTEM_PERCENTAGE:Q"),
                    color=alt.Color("CONTROLLER", title=None, legend=None).scale(
                        range=application_colors,
                        domain=df_controller["CONTROLLER"].tolist(),
                    ),
                    tooltip=["CONTROLLER", "SYSTEM_PERCENTAGE"],
                    text="shortened_name",
                )
                adjust_arc = base.mark_arc(innerRadius=20, stroke="#fff")
                text_obj = base.mark_text(radiusOffset=50, angle=10, align="center")
                chart = adjust_arc + text_obj

                st.altair_chart(chart, theme=None, use_container_width=True)
                controller_expander.dataframe(
                    df_controller.nlargest(3, "SYSTEM_PERCENTAGE").loc[
                        :, ["CONTROLLER", "SYSTEM_PERCENTAGE"]
                    ],
                    hide_index=True,
                )
            else:
                st.write(""" No Data Available""")
        with c5:

            st.write(
                """
                 ##### BIOS update and its percentage of system affected 
                 """
            )
            bios_expander = st.expander("See Details")
            bios_expander.write(
                "This chart shows the percentage of each BIOS update occurring among sensor-triggered systems."
            )
            if len(df_bios) != 0:

                # Get Top nth percentages
                df_bios = df_bios.head(15)

                df_bios.fillna("", inplace=True)  # fill blank fields with empty strings
                df_bios["BIOS"] = df_bios["NAME"] + "_" + df_bios["DATE"].astype("str")
                df_bios = df_bios.sort_values(
                    by="SYSTEM_PERCENTAGE", ascending=False
                ).reset_index()
                bar_chart = (
                    alt.Chart(df_bios)
                    .mark_bar()
                    .encode(x=alt.Y("SYSTEM_PERCENTAGE"), y=alt.X("BIOS", sort="-x"))
                )

                #                 st.altair_chart(bar_chart)
                df_bios["shortened_name"] = df_bios["BIOS"].str[0:15] + "..."
                application_colors = random.sample(optum_theme, k=df_bios.shape[0])

                base = alt.Chart(df_bios).encode(
                    theta=alt.Theta("SYSTEM_PERCENTAGE:Q", stack=True),
                    radius=alt.Radius("SYSTEM_PERCENTAGE"),
                    order=alt.Order("SYSTEM_PERCENTAGE:Q"),
                    color=alt.Color("BIOS", title=None, legend=None).scale(
                        range=application_colors, domain=df_bios["BIOS"].tolist()
                    ),
                    tooltip=["BIOS", "SYSTEM_PERCENTAGE"],
                    text="shortened_name",
                )
                adjust_arc = base.mark_arc(innerRadius=20, stroke="#fff")
                text_obj = base.mark_text(radiusOffset=50, angle=10, align="center")
                chart = adjust_arc + text_obj

                st.altair_chart(chart, theme=None, use_container_width=True)
                bios_expander.dataframe(
                    df_bios.nlargest(3, "SYSTEM_PERCENTAGE").loc[
                        :, ["BIOS", "DATE", "SYSTEM_PERCENTAGE"]
                    ],
                    hide_index=True,
                )
            else:
                st.write(""" No Data Available""")

    (
        c9,
        c10,
    ) = st.columns([3, 3])

    with st.container():
        #         with c9:
        st.write(
            """
                      ##### Application Faults
                      """
        )

        df_appfaults_7days["DAYS"] = "Last 7 Days"
        df_appfaults_30days["DAYS"] = "Last 30 days"
        df_appfaults_merged = pd.concat([df_appfaults_7days, df_appfaults_30days])
        df_appfaults_merged.rename(
            columns={
                "HEALTH_SCORE": "Health Score Category",
                "WGUID_Count": "System Count",
            },
            inplace=True,
        )

        bar_chart = (
            alt.Chart(df_appfaults_merged)
            .mark_bar()
            .encode(
                y=alt.X("APP", axis=alt.Axis(labelAngle=90), sort="-x"),
                xOffset="DAYS",
                x=alt.Y("SYSTEM_PERCENTAGE"),
                color="DAYS",
            )
            .configure_view(
                stroke=None,
            )
        )

        #             st.altair_chart(bar_chart)

        # df_appfaults_merged['APP'] = df_appfaults_merged['APP'].str.split(' ', 1)
        df_appfaults_merged["shortened_name"] = (
            df_appfaults_merged["APP"].str[0:8] + "..."
        )
        point = alt.selection_point(encodings=["y"])
        bar_chart = (
            alt.Chart(df_appfaults_merged)
            .mark_bar()
            .encode(
                y=alt.X("DAYS", sort="-x"),
                #                     xOffset='DAYS',
                x=alt.Y("SYSTEM_PERCENTAGE"),
                color=alt.condition(
                    point,
                    alt.Color("DAYS:N", title=None, legend=None).scale(
                        range=["#007C89", "#FF612B"]
                    ),
                    alt.value("lightgray"),
                ),
                row="shortened_name",
                tooltip=["APP", "SYSTEM_PERCENTAGE"],
            )
            .add_selection(point)
            .interactive()
        )

        st.altair_chart(bar_chart, use_container_width=True, theme=None)
    with st.container():
        #         with c10:
        st.write(
            """
                 ##### Application Usage
                 """
        )

        if len(df_appusage) != 0:
            df_appusage = df_appusage.sort_values(
                by="SYSTEM_PERCENTAGE", ascending=False
            ).reset_index()
            df_appusage.rename(columns={"APPLICATIONNAME": "APP"}, inplace=True)
            bar_chart = (
                alt.Chart(df_appusage)
                .mark_bar()
                .encode(
                    y=alt.Y("SYSTEM_PERCENTAGE"),
                    x=alt.X("APP", sort="-y", axis=alt.Axis(labelAngle=-45)),
                    color=alt.value("#FF612B"),
                )
            )

            st.altair_chart(bar_chart)
        else:
            st.write(""" No Data Available""")


st.sidebar.markdown("## Selection")
st.sidebar.markdown(
    "Choose either **By Sensor** or **By Machine** option to perform the data analysis"
)

radio_selection = st.sidebar.radio(
    label="**Select an option**", options=["By Sensor", "By Machine"]
)

# change 8/7 read Sensor_Definition file into dataframe
sensor_def = pd.read_excel("Sensor_Definition.xlsx")

sensor_list = [
    "Shutdown of Updating Application Failed",
    "Frequent Application Starts",
    "Restart Failed Due To Application",
    "IE Browser Extensions Disabled",
    "Google Chrome Outdated",
    "Unused Software",
    "Outlook Is In Offline Mode",
    "Application Restart Failed",
    "IE Compatibility Mode Disabled",
    "Increasing Application Load Time",
    "Application Fault",
    "Application Hang",
    "Outlook Errors Present",
    "Frequent Application Faults",
    "Long Add In Load Time",
    "Add-Ins Not Loading",
    "Application Crash After Software Change",
    "Extended High Application CPU Usage",
    "Application Hang Not Waiting For Resources",
    "Critical Application Hang",
    "Application Connectivity Problem",
    "Correlated Application Crashes",
    "Application Hang Correlated With High Add-In Load Time",
    "Stuck Logon",
    "Fast Startup Enabled",
    "Main Path Boot Time",
    "Post Boot Time",
    "Increased Boot Times",
    "Slow Login Time",
    "Host High CPU Core Utilization",
    "Extended High CPU Use",
    "High Kernel Mode CPU Use",
    "Host High CPU Contention",
    "Kernel Panic Detected",
    "Extended High Service CPU Usage",
    "CPU Throttling",
    "CPU Queuing Health Impact",
    "Excessive Temp Folder Size",
    "Hard Drive Health Issues",
    "Percentage Fragmented IO",
    "Missing Network Drive",
    "C Drive Low Disk Space",
    "Excessive OST Space",
    "Low Available Disk Space",
    "Low Mapped Drive Space",
    "Disk Problems with Low Demand",
    "Extended High Application IOPS Usage",
    "High Disk Activity",
    "Excessive Downloads Folder Size",
    "Excessive Desktop Folder Size",
    "High Disk Latency",
    "Excessive Profiles Folder Size",
    "Excessive Recycle Bin Size",
    "Major CPU Issues",
    "Major Disk Issues",
    "Major Network Issues",
    "Major Latency Issues",
    "Major Virtual Memory Issues ",
    "Major Software Installation Issues",
    "Major Event Issues",
    "Major Fault Issues",
    "Real Time CPU Impact",
    "Real Time Memory Impact",
    "Real Time Disk Impact",
    "Real-Time Network Issues",
    "Real Time Latency Impact",
    "Real-Time Virtual Memory Issues",
    "Real-Time Software Installation Issues",
    "Real-Time Fault Issues",
    "Extended Low Available Memory",
    "Low Available Memory",
    "High Page Faults",
    "Memory Leak Detected",
    "GDI Object Leak Detected",
    "Handle Leak Detected",
    "User Object Leak Detected",
    "Low Pagefile Space",
    "Memory Performance Impact",
    "Non-Paged Pool Leak Detected",
    "Paged Pool Leak Detected",
    "Thread Leak Detected",
    "Extended High Application Memory Usage",
    "Outlook Blocking Image Download",
    "OneDrive - Faults",
    "PowerPoint - Hangs",
    "PowerPoint - Excessive Memory",
    "Excel Macros Disabled",
    "PowerPoint - Excessive Disk Utilization",
    "Outlook MS Team Plugin Missing",
    "Outlook - Hangs",
    "Outlook - Excessive CPU",
    "Outlook Developer Tools Disabled",
    "Skype for Business High Latency",
    "Outlook Auto Archiving Enabled",
    "Teams Addin Disabled",
    "Teams - Excessive IOPS",
    "Word - Hangs",
    "OneDrive Sync Issues",
    "Outlook - Faults",
    "Teams - Faults",
    "Excel Formula Bar Disabled",
    "Excel - Excessive IOPS",
    "IE Pop-Up Blocker Not Working",
    "Excel - Excessive Memory",
    "Skype for Business - Faults",
    "Teams - Hangs",
    "Word - Excessive IOPS",
    "Windows and Outlook Search Issues",
    "Word - Excessive Memory",
    "Outlook - Excessive IOPS",
    "Outlook - Excessive Memory",
    "Excel Developer Tools Disabled",
    "MS Office Outdated",
    "PowerPoint Developer Tools Disabled",
    "Teams - Excessive Memory",
    "Word - Excessive CPU",
    "Excel - Faults",
    "Outlook Macros Disabled",
    "Outlook Search Indexing Incomplete",
    "Teams - Excessive CPU",
    "Word - Faults",
    "Excel - Excessive CPU",
    "Outlook Not Displaying Reminders",
    "Outlook Auto Complete Enabled",
    "Word Macros Disabled",
    "PowerPoint - Faults",
    "PowerPoint Macros Disabled",
    "Excel - Hangs",
    "OneDrive - Hangs",
    "PowerPoint - Excessive CPU",
    "Teams - Latency Impact",
    "Word Developer Tools Disabled",
    "OneDrive - Excessive Memory",
    "OneDrive - Excessive Disk Utilization",
    "Long Profile Process Time",
    "Unusual Profile Growth",
    "RDP Graphics Module Failure",
    "RDP TCP Read Failed",
    "RDP TCP Write Failed",
    "Possible Battery Issues",
    "Boot time exceed limit",
    "VPN Unknown Frequent Disconnect",
    "Application Latency Exceed Limit",
    "Application faults exceed limit",
    "VPN Lost Service Frequent Disconnect",
    "Application Average High CPU Exceed Limit",
    "VPN User Requested Frequent Disconnect",
    "VPN Idle Timeout Frequent Disconnect",
    "VPN Connection Preempted Frequent Disconnect",
    "Logoff time exceed limit",
    "Poor WIFI Quality",
    "Application Average High Active CPU Exceed Limit",
    "Application Average High Memory Exceed Limit",
    "Page Load Time Exceeded",
    "VPN  DfltAccessPolicy Frequent Disconnect",
    "VPN Internal Error Frequent Disconnect",
    "Application Hang Correlated With High Add-In Load Time:Critical Application Hang",
    "CPU Throttling:Critical Application Hang",
    "Application Connectivity Problem:Critical Application Hang",
    "CPU Throttling:Real Time CPU Impact",
    "CPU Throttling:Real Time Latency Impact",
    "Major Fault Issues:Real-Time Fault Issues",
]

# keep sensorname, date to and date from as session variables
today = date.today()
default_date = today - timedelta(days=30)

# Initiialize date session variables
if "date_from" not in st.session_state:
    st.session_state.date_from = default_date
else:
    st.session_state.date_from = default_date

if "date_to" not in st.session_state:
    st.session_state.date_to = today
else:
    st.session_state.date_to = today

# Initiialize sensor session variable
if "sensorname" not in st.session_state:
    st.session_state.sensorname = sensor_list[0]
else:
    st.session_state.sensorname = sensor_list[0]

# Initialize redirect session variable
if "redirect" not in st.session_state:
    st.session_state.redirect = {}
else:
    st.session_state.redirect = {}

# if redirecting from snapshot page, store the transfered variables
if bool(st.session_state.redirect):
    st.session_state.date_from = st.session_state.redirect["from_date"]
    st.session_state.date_to = st.session_state.redirect["to_date"]
    st.session_state.sensorname = st.session_state.redirect["sensor"]
    st.session_state.preload = False
    st.session_state.clicked = True
    radio_selection = "By Sensor"
    concat_sensorname = st.session_state.sensorname.replace(" ", "_")
    df = pd.read_csv(
        f"/files/{concat_sensorname}_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
        parse_dates=["RESULTTIME", "RESULTDATE"],
    )
    df = df[
        (df["RESULTTIME"] >= pd.to_datetime(st.session_state.date_from))
        & (df["RESULTTIME"] <= pd.to_datetime(st.session_state.date_to))
    ]
    df_6mnth = pd.read_csv(
        f"/files/{concat_sensorname}_df_6mnth_filter_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
        parse_dates=["DATE"],
    )
    df_6mnth = df_6mnth[
        (df_6mnth["DATE"] >= pd.to_datetime(st.session_state.date_from))
        & (df_6mnth["DATE"] <= pd.to_datetime(st.session_state.date_to))
    ]
    wguid_list = list(set(df.WGUID.to_list()))
    st.session_state.redirect = {}

if radio_selection == "By Machine":
    # holder = st.sidebar.empty()

    radio_selection_file = st.sidebar.radio(
        label="**By**", options=["MSID", "Hostname"]
    )

    # st.write(radio_selection_file)

    # if radio_selection_file == 'Hostname':
    #    st.write(radio_selection_file)

    uploaded_file = st.sidebar.file_uploader(
        label=f"Upload your csv or excel file with {radio_selection_file}(s)",
        type=["csv", "xlsx"],
    )
    # if uploaded_file is not None:
    # st.sidebar.write(f"{uploaded_file}")
    # holder.empty()
    today = date.today()
    default_date = today - timedelta(days=30)
    date_from = st.sidebar.date_input("Date Since", default_date)

    date_to = st.sidebar.date_input("Date To", today)  # change for date range 7/25

else:
    holder1 = st.empty()
    initial_index = sensor_list.index(st.session_state.sensorname)
    sensorname = st.sidebar.selectbox("Select Sensor", sensor_list, index=initial_index)
    date_from = st.session_state.date_from
    date_from = st.sidebar.date_input("Date Since", st.session_state.date_from)

    # change for date range 7/25
    date_to = st.session_state.date_to
    date_to = st.sidebar.date_input("Date To", st.session_state.date_to)

submit = st.sidebar.button("Submit")

# change 8/3 - set the session state to be false unless clicked, then it will be forever set True
if "clicked" not in st.session_state:
    st.session_state.clicked = True
if submit == True:
    st.session_state.clicked = True

# Set the session state to use preloaded date by default
if "preload" not in st.session_state:
    st.session_state.preload = True
else:
    st.session_state.preload = True

# if user decides to not use preloaded dates, then set preload session state to False
if date_from != default_date or date_to != today:
    st.session_state.preload = False
else:
    st.session_state.preload = True

# Create redirect dict each time, if it is empty, we do not trigger
if "redirect" not in st.session_state:
    st.session_state.redirect = {}

wguid_list = None


if st.session_state.clicked:
    # change 8/1 Connection Initialize
    Data_process.snowflake_engine = Data_process.setup_snowflake_connection()
    holder2 = st.empty()
    # my_bar = holder2.progress(0, text="Data retrival in progress. Please wait.")

    if radio_selection == "By Machine":
        if uploaded_file is not None:
            st.session_state.preload = False
            sensorname = None
            my_bar = holder2.progress(0, text="Data retrival in progress. Please wait.")
            df = pd.read_excel(uploaded_file, header=None)
            input_list = df[0].to_list()
            input_list = list(set(input_list))
            print(input_list)
            if radio_selection_file == "Hostname":
                input_list = [i[0:15] for i in input_list]
                domain = ".MS.DS.UHC.COM"
                input_list = [i + domain for i in input_list]

            if radio_selection_file == "MSID":
                invalid = False
                prefix = "MS\\"
                for i in range(len(input_list)):
                    if prefix not in input_list[i]:
                        input_list[i] = prefix + input_list[i]

            print(input_list)
            df_wguid = Data_process.fetch_wguid_file(radio_selection_file, input_list)

            wguid_list = df_wguid.WGUID.to_list()

            if len(wguid_list) == 0:
                st.error(
                    "Error: No systems found in the selected file. Please select the correct file."
                )
                my_bar.empty()

            elif len(wguid_list) > 16000:
                st.error(
                    "Error: Limitation to process high number of Systems. Please have less than 16000 systems."
                )
                my_bar.empty()

            # change 7/28 add comparison report
            elif len(wguid_list) == 2:
                process_data_comparison(df_wguid, wguid_list, date_from, date_to)

            else:
                my_bar.empty()  # change 8/4 set progress bar to empty before starting data processing
                df_6mnth, df = Data_process.fetch_sensors_wguid_file(
                    wguid_list, date_from, date_to
                )
                process_data(radio_selection, df_6mnth)

    else:
        my_bar = holder2.progress(0, text="Data retrival in progress. Please wait.")
        concat_sensorname = sensorname.replace(" ", "_")

        # Read file with list of empty sensors. First check if no_result_sensor is in there
        if os.path.isfile("/files/no_result_sensor.csv"):
            try:
                no_result_sensor = pd.read_csv("/files/no_result_sensor.csv").set_index(
                    "Unnamed: 0"
                )
            except Exception as e:
                print(e)
                st.session_state.preload = False
        # Check if the _SENSORSCAPE_SENSOR_RESULTS_DAILY file is there for that sensor
        elif os.path.isfile(
            f"/files/{concat_sensorname}_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz"
        ):
            st.session_state.preload = True
        else:
            st.session_state.preload = False

        # Fetch impacted WGUID
        # if the user selects a preload-qualified date range, run the preload
        if st.session_state.preload and wguid_list is None:
            try:
                df = pd.read_csv(
                    f"/files/{concat_sensorname}_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
                    parse_dates=["RESULTTIME", "RESULTDATE"],
                )

                df_6mnth = pd.read_csv(
                    f"/files/{concat_sensorname}_df_6mnth_filter_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
                    parse_dates=["DATE"],
                )
                wguid_list = list(set(df.WGUID.to_list()))
            except Exception as e:
                # check if sensorname is in the list of emptpy sensor values, otherwise run normal snowflake query(as a failsafe)
                if sensorname in no_result_sensor.values:
                    wguid_list = []
                else:
                    df_6mnth, df = Data_process.fetch_wguid(
                        sensorname, date_from, date_to, preload=False
                    )
                    wguid_list = list(set(df.WGUID.to_list()))
                    st.session_state.preload = False

        # if user selects outside of date range, run normal snowpark calls to snowflake database
        else:
            df_6mnth, df = Data_process.fetch_wguid(
                sensorname, date_from, date_to, preload=False
            )
            wguid_list = list(set(df.WGUID.to_list()))

        my_bar.progress(10, text="Data retrival in progress. Please wait.")

        # instead of pulling data into memory, if it is 0 count, throw error
        if len(wguid_list) == 0:
            st.error(
                "Error: No system impacted for the selected sensor and date. Please select other sensor and date."
            )
            my_bar.empty()
        # if list is too large
        elif len(wguid_list) > 16000 and not st.session_state.preload:
            df_6mnth, df = Data_process.fetch_wguid_large_wguid(
                sensorname, date_from, date_to, preload=False
            )
            my_bar.empty()  # change 8/4 set progress bar to empty before starting data processing
            process_data(sensorname, radio_selection, df_6mnth)
        else:
            # this is for general in case it is still in a snowpark dataframe
            try:
                df = df.to_pandas()
                my_bar.empty()  # change 8/4 set progress bar to empty before starting data processing
                process_data(sensorname, radio_selection, df_6mnth)
            except Exception as ex:
                my_bar.empty()  # change 8/4 set progress bar to empty before starting data processing
                process_data(sensorname, radio_selection, df_6mnth)
