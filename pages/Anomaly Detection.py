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
import schedule

# 9/27 add Dask dataframes for larger computations
import dask.dataframe as dd

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
    page_title="Anomaly Detection Analysis", page_icon="Active", layout="wide"
)


@st.cache_data(show_spinner=False, ttl=timedelta(days=1))
def read_preloaded_files(sensorname):
    sensorname = sensorname.replace(" ", "_")
    df_patch = pd.read_csv(f"/files/{sensorname}_anomaly_sql_patch_query.csv.gz")
    df_controller = pd.read_csv(
        f"/files/{sensorname}_anomaly_sql_controller_query.csv.gz"
    )
    df_software = pd.read_csv(f"/files/{sensorname}_anomaly_sql_software_query.csv.gz")
    df_software_change = pd.read_csv(
        f"/files/{sensorname}_anomaly_sql_softchange_query.csv.gz"
    )
    df_bios = pd.read_csv(f"/files/{sensorname}_anomaly_sql_bios_query.csv.gz")
    df_appfaults_7days = pd.read_csv(
        f"/files/{sensorname}_anomaly_7day_sql_appfaults_query.csv.gz"
    )
    df_appfaults_30days = pd.read_csv(
        f"/files/{sensorname}_anomaly_30day_sql_appfaults_query.csv.gz"
    )
    df_appusage = pd.read_csv(f"/files/{sensorname}_anomaly_sql_appusage_query.csv.gz")
    df_sys_mft = pd.read_csv(f"/files/{sensorname}_anomaly_sys_mft_query.csv.gz")
    df_sys_model = pd.read_csv(f"/files/{sensorname}_anomaly_sys_model_query.csv.gz")
    df_sys_location = pd.read_csv(
        f"/files/{sensorname}_anomaly_sys_location_query.csv.gz"
    )
    df_sys_info = pd.read_csv(f"/files/{sensorname}_anomaly_sys_info_query.csv.gz")
    df_jobtitle = pd.read_csv(f"/files/{sensorname}_anomaly_sys_jobtitle_query.csv.gz")
    df_hs_7days = pd.read_csv(f"/files/{sensorname}_anomaly_7day_sys_hs_query.csv.gz")
    df_hs_30days = pd.read_csv(f"/files/{sensorname}_anomaly_30day_sys_hs_query.csv.gz")

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


def process_data(selection, df_6mnth=None):
    if st.session_state.anomaly_preload:
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
        st.write("""## Anomaly Detection Analysis""")
        st.write(
            f"This page shows analysis of the anomaly systems for the sensor  **{sensorname}** from **{date_from}** to **{date_to}**"
        )

        # change 8-7 to add more information
        with st.expander("See Sensor Description"):
            sensor_description = sensor_def.loc[
                sensor_def["SENSORNAME"] == sensorname, "DESCRIPTION"
            ].item()
            st.write(sensor_description)

    # if selection != "By Machine":
    #     with st.container():
    #         st.write(
    #             """
    #                      ### Sensor Timeline
    #                      """
    #         )
    #         sensor_line_chart = (
    #             alt.Chart(df_6mnth)
    #             .mark_line()
    #             .encode(x="DATE", y="Count", color=alt.value("#FF612B"))
    #         )
    #         st.altair_chart(sensor_line_chart, theme=None, use_container_width=True)

    (
        c5,
        c6,
    ) = st.columns([3, 3])

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
                application_colors.append("#007C89")
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
                application_colors.append("#007C89")
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
                csv = df.loc[:, ["SENSORSYSTEMID", "RESULTTIME"]].rename(
                    columns={"RESULTTIME": "date"}
                )
                csv = csv[
                    (csv["date"] >= date_from_download)
                    & (csv["date"] <= date_to_download)
                ]
                st.write("")
                st.write("")
                export_side.download_button(
                    "Export System IDs", data=csv.to_csv(), file_name="system_ids.csv"
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
                application_colors.append("#007C89")
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

                # randomly select colors from optum color palette
                application_colors = random.sample(optum_theme, k=df_software.shape[0])
                application_colors.append("#007C89")
                # radial chart for system percentage
                df_software["shortened_name"] = (
                    df_software.PACKAGENAME.str[0:15] + "..."
                )
                base = alt.Chart(df_software).encode(
                    theta=alt.Theta("SYSTEM_PERCENTAGE:Q", stack=True),
                    radius=alt.Radius("SYSTEM_PERCENTAGE"),
                    color=alt.Color("SYSTEM_PERCENTAGE", title=None, legend=None).scale(
                        range=application_colors
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
                application_colors.append("#007C89")
                base = alt.Chart(df_software_change).encode(
                    theta=alt.Theta("SYSTEM_PERCENTAGE:Q", stack=True),
                    radius=alt.Radius("SYSTEM_PERCENTAGE"),
                    color=alt.Color("SYSTEM_PERCENTAGE", title=None, legend=None).scale(
                        range=application_colors
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
                application_colors.append("#007C89")
                base = alt.Chart(df_patch).encode(
                    theta=alt.Theta("SYSTEM_PERCENTAGE:Q", stack=True),
                    radius=alt.Radius("SYSTEM_PERCENTAGE"),
                    color=alt.Color("SYSTEM_PERCENTAGE", title=None, legend=None).scale(
                        range=application_colors
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
                application_colors.append("#007C89")
                base = alt.Chart(df_controller).encode(
                    theta=alt.Theta("SYSTEM_PERCENTAGE:Q", stack=True),
                    radius=alt.Radius("SYSTEM_PERCENTAGE"),
                    color=alt.Color("SYSTEM_PERCENTAGE", title=None, legend=None).scale(
                        range=application_colors
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
                application_colors.append("#007C89")
                base = alt.Chart(df_bios).encode(
                    theta=alt.Theta("SYSTEM_PERCENTAGE:Q", stack=True),
                    radius=alt.Radius("SYSTEM_PERCENTAGE"),
                    color=alt.Color("SYSTEM_PERCENTAGE", title=None, legend=None).scale(
                        range=application_colors
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


holder1 = st.empty()

# read anomaly detection csv. It should contain all sensors with the dates they experienced anomalies
anomalies = pd.read_csv("/files/anomaly-detection.csv")
anomalies = anomalies.drop(columns=["Unnamed: 0"])
sensor_list = anomalies.columns.tolist()

# change 8/7 read Sensor_Definition file into dataframe
sensor_def = pd.read_excel("Sensor_Definition.xlsx")

sensorname = st.sidebar.selectbox("Select Sensor", sensor_list)
today = date.today()
default_date = today - timedelta(days=30)
date_from = default_date
# date_from = st.sidebar.date_input("Date Since", default_date)

#     change for date range 7/25
date_to = today
# date_to = st.sidebar.date_input("Date To", today)
submit = st.sidebar.button("Submit")

# change 8/3 - set the session state to be false unless clicked, then it will be forever set True
if "clicked" not in st.session_state:
    st.session_state.clicked = False
if submit == True:
    st.session_state.clicked = True

# Set the session state to use preloaded date by default
if "anomaly_preload" not in st.session_state:
    st.session_state.anomaly_preload = True

st.session_state.anomaly_preload = True

if st.session_state.clicked:
    # change 8/1 Connection Initialize
    Data_process.snowflake_engine = Data_process.setup_snowflake_connection()
    holder2 = st.empty()

    my_bar = holder2.progress(0, text="Data retrival in progress. Please wait.")
    concat_sensorname = sensorname.replace(" ", "_")

    # Get only the dates of the anomalies for the sensor
    date_param = ",".join(
        ("'" + str(n) + "'" for n in anomalies[sensorname].dropna().tolist())
    )

    # get the lower end of the date range
    first_date = anomalies[sensorname].dropna().sort_values()[0]
    # get the upper end of the date range
    last_date = (
        anomalies[sensorname]
        .dropna()
        .sort_values()[len(anomalies[sensorname].dropna().sort_values()) - 1]
    )
    date_from = pd.to_datetime(first_date).date()
    date_to = pd.to_datetime(last_date).date()

    # fetch the anomaly wguids for the affected sensor
    if st.session_state.anomaly_preload:
        try:
            df = pd.read_csv(
                f"/files/{concat_sensorname}_anomaly_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
                parse_dates=["RESULTTIME", "RESULTDATE"],
            )

            df_6mnth = pd.read_csv(
                f"/files/{concat_sensorname}_anomaly_df_6mnth_filter_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
                parse_dates=["DATE"],
            )
        except Exception as e:
            # check if sensorname is in the list of empty sensor values, otherwise run normal snowflake query(as a failsafe)
            df_6mnth, df = Data_process.fetch_anomaly_wguid(
                sensorname, date_param, date_from, date_to, preload=False
            )
            st.session_state.anomaly_preload = False

    my_bar.progress(10, text="Data retrival in progress. Please wait.")
    wguid_list = df.WGUID.to_list()
    wguid_list = list(set(wguid_list))

    # Right now, we use the old method of snowpark to pull the info, future work will be to optimize this,
    # which is possible due to the load.py method of preloading files
    if len(wguid_list) == 0:
        st.error(
            "Error: No system impacted for the selected sensor and date. Please select other sensor and date."
        )
        my_bar.empty()
    elif len(wguid_list) > 16000 and not st.session_state.anomaly_preload:
        df_6mnth, df = Data_process.fetch_anomaly_wguid_large_wguid(
            sensorname, date_param, preload=False
        )
        my_bar.empty()  # change 8/4 set progress bar to empty before starting data processing
        process_data("By Sensor", df_6mnth)
    else:
        my_bar.empty()  # change 8/4 set progress bar to empty before starting data processing
        process_data("By Sensor", df_6mnth)
