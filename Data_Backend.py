import streamlit as st
import plotly.express as px
import pandas as pd
import altair as alt
import datetime
from datetime import date, timedelta
import time
from itertools import combinations

# import logging,warnings
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# change 8/7 Snowpark integration for performance enhancement
from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
from snowflake.snowpark.window import Window

cosensor_list = [
    "Application Hang Correlated With High Add-In Load Time",
    "Critical Application Hang",
    "CPU Throttling",
    "Critical Application Hang",
    "Application Connectivity Problem",
    "Critical Application Hang",
    "CPU Throttling",
    "Real Time CPU Impact",
    "CPU Throttling",
    "Real Time Latency Impact",
    "Major Fault Issues",
    "Real-Time Fault Issues",
]

# This is the Data Process class. This class holds all the methods the dashboard uses to make queries to Snowflake, transform them and return the data.
# Both the preload process and the on-requested process uses these methods.
# The methods in this class heavily utilize Snowpark for mainly these performance reasons:
#   - The Snowpark API allows us to use Snowpark to query, transform and send data without having to bring it into our client-side memory. This means all of these computations happen on Snowflake(server side)
#   - This way we can reduce the amount of memory that we hold in our Kubernetes deployment and reduce the load we have on our data centers
class Data_process:
    # store the snowflake engine request here.
    snowflake_engine = None

    # store the data returned by the methods as global variables
    df_all = None
    df_patch_all = None
    df_controller_all = None
    df_software_all = None
    df_software_change_all = None
    df_bios_all = None
    df_appfaults_all = None
    df_appusage_all = None
    df_sys_info_all = None
    df_jobtitle_all = None
    df_sys_hs_all = None

    # This method sets up the snowflake connection to our database and returns an engine that will be used
    # to drive all other Snowpark
    @st.cache_resource(show_spinner=False, ttl=timedelta(hours=2))
    def setup_snowflake_connection():
        try:

            with open("rsa_key.p8", "rb") as key:
                p_key = serialization.load_pem_private_key(
                    key.read(), password="optum".encode(), backend=default_backend()
                )

                pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )

                connection_parameters = {
                    "account": "uhgdwaas.east-us-2.azure",
                    "warehouse": "EUX_PRD_LOADER_LARGE_WH",
                    "database": "EUX_PRD_EUDL_DB",
                    "role": "AR_PRD_EUDL_DB_OPTUM_ROLE",
                    "schema": "DATA",
                    "user": "eudl_db@optum.com",
                    "private_key": pkb,
                }

                # set global snowflake_engine to created engine1
                snowflake_engine = Session.builder.configs(
                    connection_parameters
                ).create()

                return snowflake_engine
        except Exception as err:
            print("Snowflake connection failed:", err)
            #             audit.Logger.info(f"Snowflake connection failed to DATA schema: {err}")
            return (False, err)

    @st.cache_data(show_spinner=False, ttl=timedelta(days=1))
    def _fetch_wguid(sensorname, date_from, date_to, preload):

        sixmnth_bckdt = datetime.date.today() - pd.offsets.DateOffset(months=6)
        sixmnth_bckdt = "'" + str(sixmnth_bckdt) + "'"
        today_date = "'" + str(datetime.date.today()) + "'"
        date_from = "'" + str(date_from) + "'"
        date_to = "'" + str(date_to) + "'"  # added date to 7/24

        # change 7/24 replace query with snowflake view
        # modify query to use either this or cosensors depending on what sensor is passed

        # if sensor contains ":" then it is a cosensor and lookup from sensorresults table
        if ":" in sensorname:
            sensor_param = ",".join("'" + str(n) + "'" for n in list(cosensor_list))
            sql_query = f""" select WGUID,
                    SYSTEMID as SENSORSYSTEMID,SENSORNAME,
                    RESULTTIME from sys_sensorresults where systemid not like 'V%'
                    and SENSORNAME IN ({sensor_param}) and  RESULTTIME >= {date_from} and RESULTTIME <= {date_to}
                    and SEVERITY >= 8 """
            df1 = Data_process.snowflake_engine.sql(sql_query).to_pandas()

            # Cosensor logic from Murugan
            df2 = (
                df1.groupby(["WGUID", "SENSORSYSTEMID", "RESULTTIME"])["SENSORNAME"]
                .apply(lambda x: list(combinations(set(x), 2)))
                .explode()
                .reset_index()
                .dropna()
            )
            df2["SENSORNAME"] = df2["SENSORNAME"].apply(lambda x: ":".join(sorted(x)))
            df2 = df2[df2["SENSORNAME"] == sensorname]
            df_6mnth = df2

            # Final transformation for dashboard
            df_6mnth["RESULTDATE"] = pd.to_datetime(df_6mnth.RESULTTIME).dt.date
            df = df_6mnth

            df_6mnth_grp = (
                df_6mnth.groupby("RESULTDATE")
                .size()
                .to_frame()
                .reset_index()
                .rename(columns={0: "Count"})
            )
            df_6mnth_grp["RESULTDATE"] = pd.to_datetime(df_6mnth_grp["RESULTDATE"])
            df_6mnth_filter = df_6mnth_grp[df_6mnth_grp.RESULTDATE.dt.dayofweek < 5]
            df_6mnth_filter.rename(columns={"RESULTDATE": "DATE"}, inplace=True)
        else:
            sensorname = "'" + sensorname + "'"
            sql_query = f""" select WGUID,SENSORNAME,
                    SENSORSYSTEMID,
                    RESULTTIME
                    from EUX_PRD_EUDL_DB.DATA.SENSORSCAPE_SENSOR_RESULTS_DAILY where SENSORNAME = {sensorname} and  RESULTTIME >= {date_from} and RESULTTIME <= {date_to}"""
            df_6mnth = Data_process.snowflake_engine.sql(sql_query)

            # change 8/11 replace with snowflake methods
            df_6mnth = df_6mnth.with_column("RESULTDATE", F.to_date("RESULTTIME"))
            df = df_6mnth
            df_6mnth_grp = df_6mnth.group_by("RESULTDATE").count()
            df_6mnth_filter = df_6mnth_grp.filter(
                F.dayofweek("RESULTDATE").in_(1, 2, 3, 4, 5)
            )
            df_6mnth_filter = df_6mnth_filter.to_pandas().rename(
                columns={"RESULTDATE": "DATE", "COUNT": "Count"}
            )
            if not preload:
                df = df.to_pandas()

        return df_6mnth_filter, df

    @st.cache_data(show_spinner=False, ttl=timedelta(days=1))
    def _fetch_anomaly_wguid(sensorname, date_param):

        # if sensor contains ":" then it is a cosensor and lookup from sensorresults table
        if ":" in sensorname:
            sensor_param = ",".join("'" + str(n) + "'" for n in list(cosensor_list))
            sql_query = f""" select WGUID,
                    SYSTEMID as SENSORSYSTEMID,SENSORNAME,
                    RESULTTIME from sys_sensorresults where systemid not like 'V%'
                    and SENSORNAME IN ({sensor_param}) and to_date(RESULTTIME) IN ({date_param})
                    and SEVERITY >= 8 """
            df1 = Data_process.snowflake_engine.sql(sql_query).to_pandas()

            # Cosensor logic from Murugan
            df2 = (
                df1.groupby(["WGUID", "RESULTTIME"])["SENSORNAME"]
                .apply(lambda x: list(combinations(set(x), 2)))
                .explode()
                .reset_index()
                .dropna()
            )
            df2["SENSORNAME"] = df2["SENSORNAME"].apply(lambda x: ":".join(sorted(x)))
            df2 = df2[df2["SENSORNAME"] == sensorname]
            df_6mnth = df2

            # Final transformation for dashboard
            df_6mnth["RESULTDATE"] = pd.to_datetime(df_6mnth.RESULTTIME).dt.date
            df = df_6mnth

            df_6mnth_grp = (
                df_6mnth.groupby("RESULTDATE")
                .size()
                .to_frame()
                .reset_index()
                .rename(columns={0: "Count"})
            )
            df_6mnth_grp["RESULTDATE"] = pd.to_datetime(df_6mnth_grp["RESULTDATE"])
            df_6mnth_filter = df_6mnth_grp[df_6mnth_grp.RESULTDATE.dt.dayofweek < 5]
            df_6mnth_filter.rename(columns={"RESULTDATE": "DATE"}, inplace=True)
        else:
            sensorname = "'" + sensorname + "'"
            # change 7/24 replace query with snowflake view
            sql_query = f""" select WGUID,SENSORNAME,
                        SENSORSYSTEMID,
                        RESULTTIME
                        from EUX_PRD_EUDL_DB.DATA.SENSORSCAPE_SENSOR_RESULTS_DAILY where SENSORNAME = {sensorname} and to_date(RESULTTIME) IN ({date_param})"""

            # change 8/11 replace with snowflake methods
            df_6mnth = Data_process.snowflake_engine.sql(sql_query)
            df_6mnth = df_6mnth.with_column("RESULTDATE", F.to_date("RESULTTIME"))
            df = df_6mnth
            df_6mnth_grp = df_6mnth.group_by("RESULTDATE").count()
            df_6mnth_filter = df_6mnth_grp.filter(
                F.dayofweek("RESULTDATE").in_(1, 2, 3, 4, 5)
            )
            df_6mnth_filter = df_6mnth_filter.to_pandas().rename(
                columns={"RESULTDATE": "DATE", "COUNT": "Count"}
            )
            df = df.to_pandas()

        return df_6mnth_filter, df

    @st.cache_data(show_spinner=False, ttl=timedelta(days=1))
    def _fetch_anomaly_wguid_large_wguid(sensorname, date_param):

        # if sensor contains ":" then it is a cosensor and lookup from sensorresults table
        if ":" in sensorname:
            sensor_param = ",".join("'" + str(n) + "'" for n in list(cosensor_list))
            sql_query = f""" 
                    create or replace temporary table large_wguid_sensorscape_temp as 
                    select WGUID,
                    SYSTEMID as SENSORSYSTEMID,SENSORNAME,
                    RESULTTIME from sys_sensorresults where systemid not like 'V%'
                    and SENSORNAME IN ({sensor_param}) and to_date(RESULTTIME) IN ({date_param})
                    and SEVERITY >= 8 """

            a = Data_process.snowflake_engine.sql(sql_query).collect()

            # select results from temp table
            sql_query = (
                f"""select * from  EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp"""
            )
            df1 = Data_process.snowflake_engine.sql(sql_query).to_pandas()

            # Cosensor logic from Murugan
            df2 = (
                df1.groupby(["WGUID", "RESULTTIME"])["SENSORNAME"]
                .apply(lambda x: list(combinations(set(x), 2)))
                .explode()
                .reset_index()
                .dropna()
            )
            df2["SENSORNAME"] = df2["SENSORNAME"].apply(lambda x: ":".join(sorted(x)))
            df2 = df2[df2["SENSORNAME"] == sensorname]
            df_6mnth = df2

            # Final transformation for dashboard
            df_6mnth["RESULTDATE"] = pd.to_datetime(df_6mnth.RESULTTIME).dt.date
            df = df_6mnth

            df_6mnth_grp = (
                df_6mnth.groupby("RESULTDATE")
                .size()
                .to_frame()
                .reset_index()
                .rename(columns={0: "Count"})
            )
            df_6mnth_grp["RESULTDATE"] = pd.to_datetime(df_6mnth_grp["RESULTDATE"])
            df_6mnth_filter = df_6mnth_grp[df_6mnth_grp.RESULTDATE.dt.dayofweek < 5]
            df_6mnth_filter.rename(columns={"RESULTDATE": "DATE"}, inplace=True)
        else:
            sensorname = "'" + sensorname + "'"
            # create a temporary table to hold the wguids
            sql_query = f""" create or replace temporary table large_wguid_sensorscape_temp as 
                        select WGUID,
                        SENSORSYSTEMID,
                        RESULTTIME
                        from EUX_PRD_EUDL_DB.DATA.SENSORSCAPE_SENSOR_RESULTS_DAILY where SENSORNAME = {sensorname} and to_date(RESULTTIME) IN ({date_param})"""
            a = Data_process.snowflake_engine.sql(sql_query).collect()

            # select results from temp table
            sql_query = (
                f"""select * from  EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp"""
            )
            # change 8/11 replace with snowflake methods
            df_6mnth = Data_process.snowflake_engine.sql(sql_query)
            df_6mnth = df_6mnth.with_column("RESULTDATE", F.to_date("RESULTTIME"))
            df = df_6mnth
            df_6mnth_grp = df_6mnth.group_by("RESULTDATE").count()
            df_6mnth_filter = df_6mnth_grp.filter(
                F.dayofweek("RESULTDATE").in_(1, 2, 3, 4, 5)
            )
            df_6mnth_filter = df_6mnth_filter.to_pandas().rename(
                columns={"RESULTDATE": "DATE", "COUNT": "Count"}
            )
            df = df.to_pandas()

        return df_6mnth_filter, df

    def fetch_anomaly_wguid(sensorname, date_param, date_from, date_to, preload=False):
        if preload:
            return Data_process._fetch_anomaly_wguid(sensorname, date_param)
        else:
            key = f"fetch_anomaly_wguid_sensor_{sensorname}_{date_from}_{date_to}"
            if key not in st.session_state:
                st.session_state[key] = Data_process._fetch_anomaly_wguid(
                    sensorname, date_param
                )
            return st.session_state[key]

    def fetch_anomaly_wguid_large_wguid(sensorname, date_param, preload=False):
        if preload:
            return Data_process._fetch_anomaly_wguid_large_wguid(sensorname, date_param)
        else:
            key = f"fetch_anomaly_large_wguid_sensor_{sensorname}_{date_param}"
            if key not in st.session_state:
                st.session_state[key] = Data_process._fetch_anomaly_wguid_large_wguid(
                    sensorname, date_param
                )
            return st.session_state[key]

    @st.cache_data(show_spinner=False, ttl=timedelta(days=1))
    def fetch_wguid_file(radio_selection_file, input_list):

        # st.write(input_list)
        input_list = ",".join(("'" + str(n) + "'" for n in input_list))
        # time.sleep(10)
        # time.sleep(15)

        # change to escape sequence 7/24
        input_list = input_list.replace("\\", "\\\\")

        if radio_selection_file == "Hostname":
            sql_query = f"""select wguid,systemid from sys_user_info where systemid in ({input_list})"""

        if radio_selection_file == "MSID":
            # change to make in original query 7/24
            sql_query = f"""select wguid,username from sys_user_info where username in ({input_list})"""

        df_wguid = Data_process.snowflake_engine.sql(sql_query).to_pandas()

        return df_wguid

    @st.cache_data(show_spinner=False, ttl=timedelta(days=1))
    def fetch_sensors_wguid_file(wguid_list, date_from, date_to):
        # Format the parameters for querying
        date_from = "'" + str(date_from) + "'"
        date_to = "'" + str(date_to) + "'"
        wguid_param = ",".join(("'" + str(n) + "'" for n in wguid_list))

        # Get data for the requested sensors
        sensor_param = ",".join("'" + str(n) + "'" for n in list(cosensor_list))
        sql_query = f""" select WGUID,
                    SENSORSYSTEMID,
                    SENSORNAME,
                    RESULTTIME
                    from EUX_PRD_EUDL_DB.DATA.SENSORSCAPE_SENSOR_RESULTS_DAILY where WGUID IN ({wguid_param}) and  RESULTTIME >= {date_from} and RESULTTIME <= {date_to}
                    UNION
                    select WGUID,
                    SYSTEMID as SENSORSYSTEMID,SENSORNAME,
                    RESULTTIME from sys_sensorresults where systemid not like 'V%'
                    and SENSORNAME IN ({sensor_param}) and WGUID IN ({wguid_param}) and RESULTTIME >= {date_from} and RESULTTIME <= {date_to}
                    and SEVERITY >= 8
                    """

        # Query and convert dates
        df_6mnth = Data_process.snowflake_engine.sql(sql_query)
        df_6mnth = df_6mnth.with_column("RESULTDATE", F.to_date("RESULTTIME"))

        # Convert the conversion to pandas
        df = df_6mnth.to_pandas()

        # Create counts by sensor and date
        df_6mnth_grp = df_6mnth.group_by(["SENSORNAME", "RESULTDATE"]).count()
        df_6mnth_filter = df_6mnth_grp.filter(
            F.dayofweek("RESULTDATE").in_(1, 2, 3, 4, 5)
        )
        df_6mnth_filter = df_6mnth_filter.to_pandas().rename(
            columns={"RESULTDATE": "DATE", "COUNT": "Count"}
        )

        return df_6mnth_filter, df

    # change 8/11 function to handle percentage processing
    def per_process(df, *grouping, threshold=50, file=True):
        if file:
            df = df.fillna(" ")
            # st.write(df.to_pandas())
            df_per = df.group_by(grouping).count()
            wguid_nunique = df.select("WGUID").distinct().count()
            df_per = df_per.with_column(
                "SYSTEM_PERCENTAGE", (df_per.col("COUNT") / wguid_nunique) * 100
            )
            df_per = df_per.filter(df_per.col("SYSTEM_PERCENTAGE") > threshold)
            df_per = df_per.drop("COUNT")
            return df_per.to_pandas()

        else:
            # grouping = list(grouping)
            # df_per = (
            #     df.groupby(grouping, dropna=False)[grouping[0]]
            #     .count()
            #     .to_frame()
            #     .rename(columns={grouping[0]: "System_Count"})
            # )
            # df_per["SYSTEM_PERCENTAGE"] = (
            #     df_per["System_Count"] / df.WGUID.nunique()
            # ) * 100
            # df_per = df_per[df_per.SYSTEM_PERCENTAGE > threshold]
            # df_per.sort_values(by="SYSTEM_PERCENTAGE", ascending=False, inplace=True)
            # df_per = df_per.drop("System_Count", axis=1).reset_index()
            # return df_per
            grouping = list(grouping)
            df_per = (
                df.groupby(grouping, dropna=False)[grouping[0]]
                .count()
                .to_frame()
                .rename(columns={grouping[0]: "System_Count"})
            )
            df_per["SYSTEM_PERCENTAGE"] = (
                df_per["System_Count"] / df.WGUID.nunique()
            ) * 100
            df_per = df_per[df_per.SYSTEM_PERCENTAGE > threshold]
            df_per.sort_values(by="SYSTEM_PERCENTAGE", ascending=False, inplace=True)
            df_per = df_per.drop("System_Count", axis=1).reset_index()
            return df_per.compute()

    # change 8/4 refactoring data retrieval from snowflake to improve performance
    @st.cache_data(show_spinner=False, ttl=timedelta(days=1))
    def _fetch_data(wguid_list, date_from, date_to, preload):
        start_time = time.time()
        date_from = "'" + str(date_from) + "'"
        date_to = "'" + str(date_to) + "'"
        wguid_param = ",".join(("'" + str(n) + "'" for n in wguid_list))
        hold = st.empty()
        my_bar = hold.progress(0)

        # Patch
        # start_time = time.time()
        my_bar.progress(20, text="Data retrival in progress: Patch Update")
        sql_patch_query = f"""select wguid,systemid,patchname,installdate from EUX_PRD_EUDL_DB.DATA.SYS_PATCHES
                        where wguid in ({wguid_param}) and installdate >= {date_from} and installdate <= {date_to}
                        """
        df_patch = Data_process.snowflake_engine.sql(sql_patch_query)
        df_patch_per = Data_process.per_process(df_patch, "PATCHNAME")
        df_patch_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_patch_per["SYSTEM_PERCENTAGE"]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # Controller
        # start_time = time.time()
        my_bar.progress(30, text="Data retrival in progress: Controller Update")

        sql_controller_query = f"""select wguid,systemid,caption as controller,manufacturer,devicestatus,datemodified from EUX_PRD_EUDL_DB.DATA.SYS_CONTROLLER
                     WHERE wguid in ({wguid_param}) and datemodified >= {date_from} and datemodified <= {date_to} 
                    """

        df_contrl = Data_process.snowflake_engine.sql(sql_controller_query)
        df_contrl_per = Data_process.per_process(df_contrl, "CONTROLLER")
        df_contrl_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_contrl_per["SYSTEM_PERCENTAGE"]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # Software
        # start_time = time.time()
        my_bar.progress(35, text="Data retrival in progress: Installed Software")
        sql_software_query = f"""select wguid,systemId,packagename,version,installdate from EUX_PRD_EUDL_DB.DATA.SYS_SOFTWARE
                         where wguid in ({wguid_param}) and installdate >= {date_from} and installdate <= {date_to}"""
        df_software = Data_process.snowflake_engine.sql(sql_software_query)
        df_software_per = Data_process.per_process(
            df_software, "PACKAGENAME", "VERSION"
        )
        df_software_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_software_per["SYSTEM_PERCENTAGE"]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # Software Version changes
        # start_time = time.time()
        my_bar.progress(40, text="Data retrival in progress: Software Changes")
        sql_softchange_query = f"""select wguid,SOFTWARE,newversion,changetime from EUX_PRD_EUDL_DB.DATA.SENSORSCAPE_SOFTWARECHANGES_RESULTS_DAILY
                    where wguid in ({wguid_param}) and changetime >= {date_from} and changetime <= {date_to} 
                    """

        df_softchange = Data_process.snowflake_engine.sql(sql_softchange_query)
        df_softchange = df_softchange.with_column(
            "rownum",
            F.row_number().over(
                Window.partition_by(("WGUID", "SOFTWARE")).order_by(
                    df_softchange.col("CHANGETIME").desc()
                )
            ),
        )
        df_softchange = df_softchange.filter(df_softchange.col("ROWNUM") == 1)
        df_softchange_per = Data_process.per_process(
            df_softchange, "SOFTWARE", "NEWVERSION"
        )
        df_softchange_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_softchange_per["SYSTEM_PERCENTAGE"]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # BIOS Details
        # start_time = time.time()
        my_bar.progress(50, text="Data retrival in progress: BIOS Update")
        sql_bios_query = f"""select wguid,systemid,name,date from EUX_PRD_EUDL_DB.DATA.SYS_BIOS
                    where wguid in ({wguid_param})
                    """
        df_bioschange = Data_process.snowflake_engine.sql(sql_bios_query)
        df_bioschange_per = Data_process.per_process(
            df_bioschange, "NAME", "DATE", threshold=5
        )
        df_bioschange_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_bioschange_per["SYSTEM_PERCENTAGE"]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # App faults
        # start_time = time.time()
        my_bar.progress(55, text="Data retrival in progress: Application Fault")
        sql_appfaults_query = f"""select WGUID,VWTIME,APPNAME,CONCAT_WS(' ',APPNAME,APPVER) as APP from EUX_PRD_EUDL_DB.DATA.SYS_APPFAULTSDAILY
                    where wguid in ({wguid_param}) and VWTIME >= current_date - 30 and VWTIME <= current_date + 1
                    """
        df_appfaults = Data_process.snowflake_engine.sql(sql_appfaults_query)
        df_appfaults = df_appfaults.with_column(
            "rownum",
            F.row_number().over(
                Window.partition_by(("WGUID", "APPNAME")).order_by(
                    df_appfaults.col("VWTIME").desc()
                )
            ),
        )
        df_appfaults = df_appfaults.filter(df_appfaults.col("ROWNUM") == 1)
        # df_appfaults = df_appfaults.with_column("APP",F.concat_ws(F.lit(' '),df_appfaults.col("APPNAME"),df_appfaults.col("APPVER")))
        df_appfaults_7days = df_appfaults.filter(
            df_appfaults.col("VWTIME")
            >= F.dateadd("day", F.lit(-7), F.current_timestamp())
        )
        # df_appfaults_30days = df_appfaults.filter(df_appfaults.col("VWTIME") >= F.dateadd("day",F.lit(-30),F.current_timestamp()))
        df_appfaults_30days = df_appfaults

        df_appfaults_7days_per = Data_process.per_process(
            df_appfaults_7days, "APP", threshold=5
        )
        df_appfaults_7days_per["DAYS"] = "LAST 7 DAYS"
        df_appfaults_7days_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_appfaults_7days_per["SYSTEM_PERCENTAGE"]
        )

        df_appfaults_30days_per = Data_process.per_process(
            df_appfaults_30days, "APP", threshold=5
        )
        df_appfaults_30days_per["DAYS"] = "LAST 30 DAYS"
        df_appfaults_30days_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_appfaults_30days_per["SYSTEM_PERCENTAGE"]
        )
        df_appfaults_30days_per = df_appfaults_30days_per[
            : df_appfaults_7days_per.shape[0]
        ]
        end_time = time.time()
        # st.write(end_time - start_time)

        # App usage
        # start_time = time.time()
        my_bar.progress(60, text="Data retrival in progress: Application Usage")
        sql_appusage_query = f"""select WGUID,APPLICATIONNAME,STARTTIME,ENDTIME from EUX_PRD_EUDL_DB.DATA.sensorscape_application_results_daily
                    where wguid in ({wguid_param}) and STARTTIME>= {date_from} and STARTTIME<= {date_to}
                    """

        df_appusage = Data_process.snowflake_engine.sql(sql_appusage_query)
        df_appusage = df_appusage.with_column(
            "rownum",
            F.row_number().over(
                Window.partition_by(("WGUID", "APPLICATIONNAME")).order_by(
                    df_appusage.col("ENDTIME").desc()
                )
            ),
        )
        df_appusage = df_appusage.filter(df_appusage.col("ROWNUM") == 1)
        df_appusage_per = Data_process.per_process(df_appusage, "APPLICATIONNAME")
        df_appusage_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_appusage_per["SYSTEM_PERCENTAGE"]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # Job Title
        # start_time = time.time()
        my_bar.progress(70, text="Data retrival in progress: Employee Job Title")
        sys_job_title = f""" select lk.wguid,lk.jobtitle,alt.name,alt.createddate from lk_users lk join alt_uhgcomputer alt on alt.name = lk.systemid  where wguid in ({wguid_param})"""

        df_sys_job = Data_process.snowflake_engine.sql(sys_job_title).to_pandas()
        end_time = time.time()
        # st.write(end_time - start_time)

        # System info data
        # start_time = time.time()
        my_bar.progress(65, text="Data retrival in progress: System Model")
        sys_info_query = f""" select con.wguid,con.manufacturer,con.model,lam.hostname,lam.invoicedt,lam.assetlocation,loc.loc_remark,loc.country 
                            from lam lam 
                            join sys_system_config con on split_part(con.systemid,'.',1) = lam.hostname 
                            left join sn_locations loc on loc.loc_id = lam.assetlocation
                            where con.wguid in ({wguid_param})"""

        df_sys_info = Data_process.snowflake_engine.sql(sys_info_query).to_pandas()
        # System Manufactured Date
        df_sys_mft = df_sys_info.dropna(subset=["INVOICEDT"])
        df_sys_mft["INVOICEDT_YEAR"] = pd.to_datetime(df_sys_mft["INVOICEDT"]).dt.year
        df_sys_mft = (
            df_sys_mft["INVOICEDT_YEAR"]
            .value_counts(ascending=True)
            .to_frame()
            .reset_index()
            .rename(
                columns={"index": "MANUFACTURED YEAR", "INVOICEDT_YEAR": "WGUID COUNT"}
            )
        )

        # System Model
        df_sys_model = (
            df_sys_info["MODEL"]
            .value_counts()
            .to_frame()
            .reset_index()
            .rename(columns={"index": "MODEL", "MODEL": "WGUID COUNT"})
        )

        # System Location
        df_sys_location = df_sys_info.dropna(subset=["LOC_REMARK"]).reset_index()
        df_sys_location["CITY"] = (
            df_sys_location.LOC_REMARK.str.split("-").str[1]
            + ","
            + df_sys_location.COUNTRY
        )
        df_sys_location = (
            df_sys_location.CITY.value_counts()
            .to_frame()
            .reset_index()
            .rename(columns={"index": "LOCATION", "CITY": "WGUID COUNT"})
        )

        # Job Processing
        df_sys_jobtitle = (
            df_sys_job.JOBTITLE.value_counts()
            .to_frame()
            .reset_index()
            .rename(columns={"index": "JOB TITLE", "JOBTITLE": "WGUID COUNT"})[0:20]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # Health Score check for summary
        # start_time = time.time()
        my_bar.progress(80, text="Data retrival in progress: System Health Score")
        sys_hs_query = f"""select wguid,vwtime,wscore
                        from sys_healthdaily
                        where vwtime >= current_date - 30 and vwtime <= current_date + 1 and activestatus = 1
                        and wguid in ({wguid_param})"""

        df_sys_hs = Data_process.snowflake_engine.sql(sys_hs_query).to_pandas()
        df_sys_hs_7days = df_sys_hs[
            df_sys_hs.VWTIME >= datetime.datetime.now() - pd.to_timedelta("7day")
        ]
        df_sys_hs_30days = df_sys_hs[
            df_sys_hs.VWTIME >= datetime.datetime.now() - pd.to_timedelta("30day")
        ]

        df_sys_hs_7days_sum = (
            df_sys_hs_7days.groupby("WGUID")["WSCORE"].agg("mean").to_frame()
        )
        df_sys_hs_30days_sum = (
            df_sys_hs_30days.groupby("WGUID")["WSCORE"].agg("mean").to_frame()
        )
        df_sys_hs_7days_sum["HEALTH_SCORE"] = pd.cut(
            df_sys_hs_7days_sum["WSCORE"],
            bins=[0, 80, 90, 97, float("Inf")],
            labels=["Poor", "Fair", "Good", "Excellent"],
        )
        df_sys_hs_30days_sum["HEALTH_SCORE"] = pd.cut(
            df_sys_hs_30days_sum["WSCORE"],
            bins=[0, 80, 90, 97, float("Inf")],
            labels=["Poor", "Fair", "Good", "Excellent"],
        )

        df_sys_hs_30days_sum["WGUID"] = df_sys_hs_30days_sum.index
        df_sys_hs_7days_sum["WGUID"] = df_sys_hs_7days_sum.index
        df_sys_hs_30days_sum1 = (
            df_sys_hs_30days_sum.groupby(by="HEALTH_SCORE", as_index=False)
            .agg({"WGUID": pd.Series.nunique})
            .rename(columns={"WGUID": "WGUID_Count"})
        )
        df_sys_hs_30days_sum2 = (
            df_sys_hs_30days_sum.groupby(by="HEALTH_SCORE", as_index=False)
            .agg({"WGUID": set})
            .rename(columns={"WGUID": "WGUIDs"})
        )
        df_sys_hs_30days_sum_merged = pd.merge(
            df_sys_hs_30days_sum1, df_sys_hs_30days_sum2, on="HEALTH_SCORE"
        )

        df_sys_hs_7days_sum1 = (
            df_sys_hs_7days_sum.groupby(by="HEALTH_SCORE", as_index=False)
            .agg({"WGUID": pd.Series.nunique})
            .rename(columns={"WGUID": "WGUID_Count"})
        )
        df_sys_hs_7days_sum2 = (
            df_sys_hs_7days_sum.groupby(by="HEALTH_SCORE", as_index=False)
            .agg({"WGUID": set})
            .rename(columns={"WGUID": "WGUIDs"})
        )
        df_sys_hs_7days_sum_merged = pd.merge(
            df_sys_hs_7days_sum1, df_sys_hs_7days_sum2, on="HEALTH_SCORE"
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        my_bar.empty()

        end_time = time.time()
        # st.write(end_time - start_time)
        return (
            df_patch_per,
            df_contrl_per,
            df_software_per,
            df_softchange_per,
            df_bioschange_per,
            df_appfaults_7days_per,
            df_appfaults_30days_per,
            df_appusage_per,
            df_sys_mft,
            df_sys_model,
            df_sys_location,
            df_sys_info,
            df_sys_jobtitle,
            df_sys_hs_7days_sum_merged,
            df_sys_hs_30days_sum_merged,
        )

    def fetch_wguid(sensorname, date_from, date_to, preload=False):
        if preload:
            return Data_process._fetch_wguid(sensorname, date_from, date_to, preload)
        else:
            key = f"fetch_wguid_sensor_{sensorname}_{date_from}_{date_to}"
            if key not in st.session_state:
                st.session_state[key] = Data_process._fetch_wguid(
                    sensorname, date_from, date_to, preload
                )
            return st.session_state[key]

    def fetch_data(sensorname, wguid_list, date_from, date_to, preload=False):
        if preload:
            return Data_process._fetch_data(wguid_list, date_from, date_to, preload)
        else:
            key = f"fetch_data_sensor_{sensorname}_{wguid_list}_{date_from}_{date_to}"
            if key not in st.session_state:
                st.session_state[key] = Data_process._fetch_data(
                    wguid_list, date_from, date_to, preload
                )
            return st.session_state[key]

    # 9/20 creating this process to handle large sensor results
    @st.cache_data(show_spinner=False, ttl=timedelta(days=1))
    def _fetch_wguid_large_wguid(sensorname, date_from, date_to, preload):

        sixmnth_bckdt = datetime.date.today() - pd.offsets.DateOffset(months=6)
        sixmnth_bckdt = "'" + str(sixmnth_bckdt) + "'"
        today_date = "'" + str(datetime.date.today()) + "'"
        date_from = "'" + str(date_from) + "'"
        date_to = "'" + str(date_to) + "'"  # added date to 7/24

        # if sensor contains ":" then it is a cosensor and lookup from sensorresults table
        if ":" in sensorname:
            sensor_param = ",".join("'" + str(n) + "'" for n in list(cosensor_list))
            sql_query = f""" 
                    create or replace temporary table large_wguid_sensorscape_temp as 
                    select WGUID,
                    SYSTEMID as SENSORSYSTEMID,SENSORNAME,
                    RESULTTIME from sys_sensorresults where systemid not like 'V%'
                    and SENSORNAME IN ({sensor_param}) and  RESULTTIME >= {date_from} and RESULTTIME <= {date_to}
                    and SEVERITY >= 8 """
            a = Data_process.snowflake_engine.sql(sql_query).collect()

            # select results from temp table
            sql_query = (
                f"""select * from  EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp"""
            )
            df1 = Data_process.snowflake_engine.sql(sql_query).to_pandas()

            # Cosensor logic from Murugan
            df2 = (
                df1.groupby(["WGUID", "SENSORSYSTEMID", "RESULTTIME"])["SENSORNAME"]
                .apply(lambda x: list(combinations(set(x), 2)))
                .explode()
                .reset_index()
                .dropna()
            )
            df2["SENSORNAME"] = df2["SENSORNAME"].apply(lambda x: ":".join(sorted(x)))
            df2 = df2[df2["SENSORNAME"] == sensorname]
            df_6mnth = df2

            # Final transformation for dashboard
            df_6mnth["RESULTDATE"] = pd.to_datetime(df_6mnth.RESULTTIME).dt.date
            df = df_6mnth

            df_6mnth_grp = (
                df_6mnth.groupby("RESULTDATE")
                .size()
                .to_frame()
                .reset_index()
                .rename(columns={0: "Count"})
            )
            df_6mnth_grp["RESULTDATE"] = pd.to_datetime(df_6mnth_grp["RESULTDATE"])
            df_6mnth_filter = df_6mnth_grp[df_6mnth_grp.RESULTDATE.dt.dayofweek < 5]
            df_6mnth_filter.rename(columns={"RESULTDATE": "DATE"}, inplace=True)
        else:
            sensorname = "'" + sensorname + "'"
            # create a temporary table to hold the wguids
            sql_query = f""" create or replace temporary table large_wguid_sensorscape_temp as 
                        select WGUID,
                        SENSORSYSTEMID,SENSORNAME,
                        RESULTTIME
                        from EUX_PRD_EUDL_DB.DATA.SENSORSCAPE_SENSOR_RESULTS_DAILY where SENSORNAME = {sensorname} and RESULTTIME >= {date_from} and RESULTTIME <= {date_to}"""
            a = Data_process.snowflake_engine.sql(sql_query).collect()

            # select results from temp table
            sql_query = (
                f"""select * from  EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp"""
            )
            # change 8/11 replace with snowflake methods
            df_6mnth = Data_process.snowflake_engine.sql(sql_query)
            df_6mnth = df_6mnth.with_column("RESULTDATE", F.to_date("RESULTTIME"))
            df = df_6mnth
            df_6mnth_grp = df_6mnth.group_by("RESULTDATE").count()
            df_6mnth_filter = df_6mnth_grp.filter(
                F.dayofweek("RESULTDATE").in_(1, 2, 3, 4, 5)
            )
            df_6mnth_filter = df_6mnth_filter.to_pandas().rename(
                columns={"RESULTDATE": "DATE", "COUNT": "Count"}
            )
            if not preload:
                df = df.to_pandas()
        return df_6mnth_filter, df

    # 9/20 creating this process to handle large sensor results
    @st.cache_data(show_spinner=False, ttl=timedelta(days=1))
    def _fetch_data_large_wguid(date_from, date_to):
        start_time = time.time()
        date_from = "'" + str(date_from) + "'"
        date_to = "'" + str(date_to) + "'"
        hold = st.empty()
        my_bar = hold.progress(0)

        # Patch
        # start_time = time.time()
        my_bar.progress(20, text="Data retrival in progress: Patch Update")
        sql_patch_query = f"""select wguid,systemid,patchname,installdate from EUX_PRD_EUDL_DB.DATA.SYS_PATCHES
                        where wguid in (select wguid from EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp) and installdate >= {date_from} and installdate <= {date_to}
                        """
        df_patch = Data_process.snowflake_engine.sql(sql_patch_query)
        df_patch_per = Data_process.per_process(df_patch, "PATCHNAME")
        df_patch_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_patch_per["SYSTEM_PERCENTAGE"]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # Controller
        # start_time = time.time()
        my_bar.progress(30, text="Data retrival in progress: Controller Update")

        sql_controller_query = f"""select wguid,systemid,caption as controller,manufacturer,devicestatus,datemodified from EUX_PRD_EUDL_DB.DATA.SYS_CONTROLLER
                     WHERE wguid in (select wguid from EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp) and datemodified >= {date_from} and datemodified <= {date_to} 
                    """

        df_contrl = Data_process.snowflake_engine.sql(sql_controller_query)
        df_contrl_per = Data_process.per_process(df_contrl, "CONTROLLER")
        df_contrl_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_contrl_per["SYSTEM_PERCENTAGE"]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # Software
        # start_time = time.time()
        my_bar.progress(35, text="Data retrival in progress: Installed Software")
        sql_software_query = f"""select wguid,systemId,packagename,version,installdate from EUX_PRD_EUDL_DB.DATA.SYS_SOFTWARE
                         where wguid in (select wguid from EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp) and installdate >= {date_from} and installdate <= {date_to}"""

        df_software = Data_process.snowflake_engine.sql(sql_software_query)
        df_software_per = Data_process.per_process(
            df_software, "PACKAGENAME", "VERSION"
        )
        df_software_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_software_per["SYSTEM_PERCENTAGE"]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # Software Version changes
        # start_time = time.time()
        my_bar.progress(40, text="Data retrival in progress: Software Changes")
        sql_softchange_query = f"""select wguid,SOFTWARE,newversion,changetime from EUX_PRD_EUDL_DB.DATA.SENSORSCAPE_SOFTWARECHANGES_RESULTS_DAILY
                    where wguid in (select wguid from EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp) and changetime >= {date_from} and changetime <= {date_to} 
                    """

        df_softchange = Data_process.snowflake_engine.sql(sql_softchange_query)
        df_softchange = df_softchange.with_column(
            "rownum",
            F.row_number().over(
                Window.partition_by(("WGUID", "SOFTWARE")).order_by(
                    df_softchange.col("CHANGETIME").desc()
                )
            ),
        )
        df_softchange = df_softchange.filter(df_softchange.col("ROWNUM") == 1)
        df_softchange_per = Data_process.per_process(
            df_softchange, "SOFTWARE", "NEWVERSION"
        )
        df_softchange_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_softchange_per["SYSTEM_PERCENTAGE"]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # BIOS Details
        # start_time = time.time()
        my_bar.progress(50, text="Data retrival in progress: BIOS Update")
        sql_bios_query = f"""select wguid,systemid,name,date from EUX_PRD_EUDL_DB.DATA.SYS_BIOS
                    where wguid in (select wguid from EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp)
                    """
        df_bioschange = Data_process.snowflake_engine.sql(sql_bios_query)
        df_bioschange_per = Data_process.per_process(
            df_bioschange, "NAME", "DATE", threshold=5
        )
        df_bioschange_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_bioschange_per["SYSTEM_PERCENTAGE"]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # App faults
        # start_time = time.time()
        my_bar.progress(55, text="Data retrival in progress: Application Fault")
        sql_appfaults_query = f"""select WGUID,VWTIME,APPNAME,CONCAT_WS(' ',APPNAME,APPVER) as APP from EUX_PRD_EUDL_DB.DATA.SYS_APPFAULTSDAILY
                    where wguid in (select wguid from EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp) and VWTIME>= current_date - 30
                    """
        df_appfaults = Data_process.snowflake_engine.sql(sql_appfaults_query)
        df_appfaults = df_appfaults.with_column(
            "rownum",
            F.row_number().over(
                Window.partition_by(("WGUID", "APPNAME")).order_by(
                    df_appfaults.col("VWTIME").desc()
                )
            ),
        )
        df_appfaults = df_appfaults.filter(df_appfaults.col("ROWNUM") == 1)
        # df_appfaults = df_appfaults.with_column("APP",F.concat_ws(F.lit(' '),df_appfaults.col("APPNAME"),df_appfaults.col("APPVER")))
        df_appfaults_7days = df_appfaults.filter(
            df_appfaults.col("VWTIME")
            >= F.dateadd("day", F.lit(-7), F.current_timestamp())
        )
        # df_appfaults_30days = df_appfaults.filter(df_appfaults.col("VWTIME") >= F.dateadd("day",F.lit(-30),F.current_timestamp()))
        df_appfaults_30days = df_appfaults

        df_appfaults_7days_per = Data_process.per_process(
            df_appfaults_7days, "APP", threshold=5
        )
        df_appfaults_7days_per["DAYS"] = "LAST 7 DAYS"
        df_appfaults_7days_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_appfaults_7days_per["SYSTEM_PERCENTAGE"]
        )

        df_appfaults_30days_per = Data_process.per_process(
            df_appfaults_30days, "APP", threshold=5
        )
        df_appfaults_30days_per["DAYS"] = "LAST 30 DAYS"
        df_appfaults_30days_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_appfaults_30days_per["SYSTEM_PERCENTAGE"]
        )
        df_appfaults_30days_per = df_appfaults_30days_per[
            : df_appfaults_7days_per.shape[0]
        ]
        end_time = time.time()
        # st.write(end_time - start_time)

        # App usage
        # start_time = time.time()
        my_bar.progress(60, text="Data retrival in progress: Application Usage")
        sql_appusage_query = f"""select WGUID,APPLICATIONNAME,STARTTIME,ENDTIME from EUX_PRD_EUDL_DB.DATA.sensorscape_application_results_daily
                    where wguid in (select wguid from EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp) and STARTTIME>= {date_from} and STARTTIME<= {date_to}
                    """
        df_appusage = Data_process.snowflake_engine.sql(sql_appusage_query)
        df_appusage = df_appusage.with_column(
            "rownum",
            F.row_number().over(
                Window.partition_by(("WGUID", "APPLICATIONNAME")).order_by(
                    df_appusage.col("ENDTIME").desc()
                )
            ),
        )
        df_appusage = df_appusage.filter(df_appusage.col("ROWNUM") == 1)
        df_appusage_per = Data_process.per_process(df_appusage, "APPLICATIONNAME")
        df_appusage_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
            df_appusage_per["SYSTEM_PERCENTAGE"]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # Job Title
        # start_time = time.time()
        my_bar.progress(70, text="Data retrival in progress: Employee Job Title")
        sys_job_title = f""" select lk.wguid,lk.jobtitle,alt.name,alt.createddate from lk_users lk join alt_uhgcomputer alt on alt.name = lk.systemid  where wguid in (select wguid from EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp)"""

        df_sys_job = Data_process.snowflake_engine.sql(sys_job_title).to_pandas()
        end_time = time.time()
        # st.write(end_time - start_time)

        # System info data
        # start_time = time.time()
        my_bar.progress(65, text="Data retrival in progress: System Model")
        sys_info_query = f""" select con.wguid,con.manufacturer,con.model,lam.hostname,lam.invoicedt,lam.assetlocation,loc.loc_remark,loc.country 
                            from lam lam 
                            join sys_system_config con on split_part(con.systemid,'.',1) = lam.hostname 
                            left join sn_locations loc on loc.loc_id = lam.assetlocation
                            where con.wguid in (select wguid from EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp)"""

        df_sys_info = Data_process.snowflake_engine.sql(sys_info_query).to_pandas()
        # System Manufactured Date
        df_sys_mft = df_sys_info.dropna(subset=["INVOICEDT"])
        df_sys_mft["INVOICEDT_YEAR"] = pd.to_datetime(df_sys_mft["INVOICEDT"]).dt.year
        df_sys_mft = (
            df_sys_mft["INVOICEDT_YEAR"]
            .value_counts(ascending=True)
            .to_frame()
            .reset_index()
            .rename(
                columns={"index": "MANUFACTURED YEAR", "INVOICEDT_YEAR": "WGUID COUNT"}
            )
        )

        # System Model
        df_sys_model = (
            df_sys_info["MODEL"]
            .value_counts()
            .to_frame()
            .reset_index()
            .rename(columns={"index": "MODEL", "MODEL": "WGUID COUNT"})
        )

        # System Location
        df_sys_location = df_sys_info.dropna(subset=["LOC_REMARK"]).reset_index()
        df_sys_location["CITY"] = (
            df_sys_location.LOC_REMARK.str.split("-").str[1]
            + ","
            + df_sys_location.COUNTRY
        )
        df_sys_location = (
            df_sys_location.CITY.value_counts()
            .to_frame()
            .reset_index()
            .rename(columns={"index": "LOCATION", "CITY": "WGUID COUNT"})
        )

        # Job Processing
        df_sys_jobtitle = (
            df_sys_job.JOBTITLE.value_counts()
            .to_frame()
            .reset_index()
            .rename(columns={"index": "JOB TITLE", "JOBTITLE": "WGUID COUNT"})[0:20]
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        # Health Score check for summary
        # start_time = time.time()
        my_bar.progress(80, text="Data retrival in progress: System Health Score")
        sys_hs_query = f"""select wguid,vwtime,wscore
                        from sys_healthdaily
                        where vwtime >= current_date - 30 and activestatus = 1
                        and wguid in (select wguid from EUX_PRD_EUDL_DB.DATA.large_wguid_sensorscape_temp)"""

        df_sys_hs = Data_process.snowflake_engine.sql(sys_hs_query).to_pandas()
        df_sys_hs_7days = df_sys_hs[
            df_sys_hs.VWTIME >= datetime.datetime.now() - pd.to_timedelta("7day")
        ]
        df_sys_hs_30days = df_sys_hs[
            df_sys_hs.VWTIME >= datetime.datetime.now() - pd.to_timedelta("30day")
        ]

        df_sys_hs_7days_sum = (
            df_sys_hs_7days.groupby("WGUID")["WSCORE"].agg("mean").to_frame()
        )
        df_sys_hs_30days_sum = (
            df_sys_hs_30days.groupby("WGUID")["WSCORE"].agg("mean").to_frame()
        )
        df_sys_hs_7days_sum["HEALTH_SCORE"] = pd.cut(
            df_sys_hs_7days_sum["WSCORE"],
            bins=[0, 80, 90, 97, float("Inf")],
            labels=["Poor", "Fair", "Good", "Excellent"],
        )
        df_sys_hs_30days_sum["HEALTH_SCORE"] = pd.cut(
            df_sys_hs_30days_sum["WSCORE"],
            bins=[0, 80, 90, 97, float("Inf")],
            labels=["Poor", "Fair", "Good", "Excellent"],
        )

        df_sys_hs_30days_sum["WGUID"] = df_sys_hs_30days_sum.index
        df_sys_hs_7days_sum["WGUID"] = df_sys_hs_7days_sum.index
        df_sys_hs_30days_sum1 = (
            df_sys_hs_30days_sum.groupby(by="HEALTH_SCORE", as_index=False)
            .agg({"WGUID": pd.Series.nunique})
            .rename(columns={"WGUID": "WGUID_Count"})
        )
        df_sys_hs_30days_sum2 = (
            df_sys_hs_30days_sum.groupby(by="HEALTH_SCORE", as_index=False)
            .agg({"WGUID": set})
            .rename(columns={"WGUID": "WGUIDs"})
        )
        df_sys_hs_30days_sum_merged = pd.merge(
            df_sys_hs_30days_sum1, df_sys_hs_30days_sum2, on="HEALTH_SCORE"
        )

        df_sys_hs_7days_sum1 = (
            df_sys_hs_7days_sum.groupby(by="HEALTH_SCORE", as_index=False)
            .agg({"WGUID": pd.Series.nunique})
            .rename(columns={"WGUID": "WGUID_Count"})
        )
        df_sys_hs_7days_sum2 = (
            df_sys_hs_7days_sum.groupby(by="HEALTH_SCORE", as_index=False)
            .agg({"WGUID": set})
            .rename(columns={"WGUID": "WGUIDs"})
        )
        df_sys_hs_7days_sum_merged = pd.merge(
            df_sys_hs_7days_sum1, df_sys_hs_7days_sum2, on="HEALTH_SCORE"
        )
        end_time = time.time()
        # st.write(end_time - start_time)

        my_bar.empty()

        end_time = time.time()
        # st.write(end_time - start_time)
        return (
            df_patch_per,
            df_contrl_per,
            df_software_per,
            df_softchange_per,
            df_bioschange_per,
            df_appfaults_7days_per,
            df_appfaults_30days_per,
            df_appusage_per,
            df_sys_mft,
            df_sys_model,
            df_sys_location,
            df_sys_info,
            df_sys_jobtitle,
            df_sys_hs_7days_sum_merged,
            df_sys_hs_30days_sum_merged,
        )

    def fetch_wguid_large_wguid(sensorname, date_from, date_to, preload=False):
        if preload:
            return Data_process._fetch_wguid_large_wguid(
                sensorname, date_from, date_to, preload
            )
        else:
            key = f"fetch_large_wguid_sensor_{sensorname}_{date_from}_{date_to}"
            if key not in st.session_state:
                st.session_state[key] = Data_process._fetch_wguid_large_wguid(
                    sensorname, date_from, date_to, preload
                )
            return st.session_state[key]

    def fetch_data_large_wguid(sensorname, date_from, date_to, preload=False):
        if preload:
            return Data_process._fetch_data_large_wguid(date_from, date_to)
        else:
            key = f"fetch_data_large_wguid_sensor_{sensorname}_{date_from}_{date_to}"
            if key not in st.session_state:
                st.session_state[key] = Data_process._fetch_data_large_wguid(
                    date_from, date_to
                )
            return st.session_state[key]

    @st.cache_data(show_spinner=False, ttl=timedelta(days=1))
    def fetch_comparison_sensors(wguid_list, date_from, date_to):
        date_from = "'" + str(date_from) + "'"
        date_to = "'" + str(date_to) + "'"
        wguid_list = ",".join(("'" + str(n) + "'" for n in wguid_list))

        today_date = "'" + str(datetime.date.today()) + "'"

        # from the inputted list of wguids, find the unique corresponding sensors in the past 6mnths
        sql_query = f"""select wguid,ssensor.SENSORNAME as "Sensor Name", SENSORSYSTEMID,RESULTTIME as "Date"
        from EUX_PRD_EUDL_DB.DATA.SENSORSCAPE_SENSOR_RESULTS_DAILY ssensor
        where ssensor.wguid in ({wguid_list})
        and RESULTTIME > {date_from} and RESULTTIME <= {date_to}"""

        # store and return query in dataframe
        df_sensor_6mnth = Data_process.snowflake_engine.sql(sql_query).to_pandas()
        return df_sensor_6mnth

    @st.cache_data(show_spinner=False, ttl=timedelta(days=1))
    def fetch_comparison_apps(wguid_list, date_from, date_to):
        wguid_list = ",".join(("'" + str(n) + "'" for n in wguid_list))
        date_from = "'" + str(date_from) + "'"
        date_to = "'" + str(date_to) + "'"

        # Get installed software for specific wguids
        sql_query = f""" select wguid,packagename as "Installed Application",package_id,installdate as "Date"
        from EUX_PRD_EUDL_DB.DATA.SYS_SOFTWARE
        where wguid in ({wguid_list})
        and installdate >= {date_from}
        and installdate <= {date_to}"""

        # store and return query in dataframe
        df_app_6mnth = Data_process.snowflake_engine.sql(sql_query).to_pandas()
        return df_app_6mnth

    @st.cache_data(show_spinner=False, ttl=timedelta(days=1))
    def fetch_comparison_drivers(wguid_list, date_from, date_to):
        wguid_list = ",".join(("'" + str(n) + "'" for n in wguid_list))
        date_from = "'" + str(date_from) + "'"
        date_to = "'" + str(date_to) + "'"

        # Get installed software for specific wguids
        sql_query = f""" select 
        wguid,caption as "Driver Name",CLASSGUID, adddate as "Date"
        from sys_devicemanager 
        where wguid in ({wguid_list})
        and adddate >= {date_from}
        and adddate <= {date_to}"""

        # store and return query in dataframe
        df_drive_6mnth = Data_process.snowflake_engine.sql(sql_query).to_pandas()
        return df_drive_6mnth

    @st.cache_data(show_spinner=False, ttl=timedelta(days=1))
    def fetch_comparison_software_patch_upd(wguid_list, date_from, date_to):
        wguid_list = ",".join(("'" + str(n) + "'" for n in wguid_list))
        date_from = "'" + str(date_from) + "'"
        date_to = "'" + str(date_to) + "'"

        # Get patch & software updates for specific wguids
        sql_query_patch = f"""select wguid,patch_id,patchname as "Patch Name", installdate as "Date"
        from sys_patches
        where wguid in ({wguid_list})
        and installdate >= {date_from}
        and installdate <= {date_to}"""

        sql_query_changes = f""" select wguid,changedesc as "Software Update Name",changeclass, changetime as "Date"
        from sys_changesdaily
        where wguid in ({wguid_list})
        and changetime >= {date_from}
        and changetime <= {date_to}"""

        # store and return query in dataframe
        df_patch_6mnth = Data_process.snowflake_engine.sql(sql_query_patch).to_pandas()
        df_software_6mnth = Data_process.snowflake_engine.sql(
            sql_query_changes
        ).to_pandas()

        return df_patch_6mnth, df_software_6mnth


#  everything below here are old pieces of code in case it needs to be reused

# change 10/5 to handle inputs from file
# @st.cache_data(show_spinner=False, persist=True)
# def fetch_data_file(_arg, wguid_list, date_from, date_to):
#     start_time = time.time()
#     date_from = "'" + str(date_from) + "'"
#     date_to = "'" + str(date_to) + "'"
#     wguid_param = ",".join(("'" + str(n) + "'" for n in wguid_list))
#     hold = st.empty()
#     my_bar = hold.progress(0)

#     # Patch
#     # start_time = time.time()
#     my_bar.progress(20, text="Data retrival in progress: Patch Update")
#     sql_patch_query = f"""select wguid,systemid,patchname,installdate from EUX_PRD_EUDL_DB.DATA.SYS_PATCHES
#                     where wguid in ({wguid_param}) and installdate >= {date_from} and installdate <= {date_to}
#                     """
#     df_patch = Data_process.snowflake_engine.sql(sql_patch_query)
#     df_patch_per = Data_process.per_process(df_patch, "PATCHNAME", file=True)
#     df_patch_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
#         df_patch_per["SYSTEM_PERCENTAGE"]
#     )
#     end_time = time.time()
#     # st.write(end_time - start_time)

#     # Controller
#     # start_time = time.time()
#     my_bar.progress(30, text="Data retrival in progress: Controller Update")

#     sql_controller_query = f"""select wguid,systemid,caption as controller,manufacturer,devicestatus,datemodified from EUX_PRD_EUDL_DB.DATA.SYS_CONTROLLER
#                  WHERE wguid in ({wguid_param}) and datemodified >= {date_from} and datemodified <= {date_to}
#                 """

#     df_contrl = Data_process.snowflake_engine.sql(sql_controller_query)
#     df_contrl_per = Data_process.per_process(df_contrl, "CONTROLLER", file=True)
#     df_contrl_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
#         df_contrl_per["SYSTEM_PERCENTAGE"]
#     )
#     end_time = time.time()
#     # st.write(end_time - start_time)

#     # Software
#     # start_time = time.time()
#     my_bar.progress(35, text="Data retrival in progress: Installed Software")
#     sql_software_query = f"""select wguid,systemId,packagename,version,installdate from EUX_PRD_EUDL_DB.DATA.SYS_SOFTWARE
#                      where wguid in ({wguid_param}) and installdate >= {date_from} and installdate <= {date_to}"""

#     df_software = Data_process.snowflake_engine.sql(sql_software_query)
#     df_software_per = Data_process.per_process(
#         df_software, "PACKAGENAME", "VERSION", file=True
#     )
#     df_software_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
#         df_software_per["SYSTEM_PERCENTAGE"]
#     )
#     end_time = time.time()
#     # st.write(end_time - start_time)

#     # Software Version changes
#     # start_time = time.time()
#     my_bar.progress(40, text="Data retrival in progress: Software Changes")
#     sql_softchange_query = f"""select wguid,SOFTWARE,newversion,changetime from EUX_PRD_EUDL_DB.DATA.SENSORSCAPE_SOFTWARECHANGES_RESULTS_DAILY
#                 where wguid in ({wguid_param}) and changetime >= {date_from} and changetime <= {date_to}
#                 """

#     df_softchange = Data_process.snowflake_engine.sql(sql_softchange_query)
#     df_softchange = df_softchange.with_column(
#         "rownum",
#         F.row_number().over(
#             Window.partition_by(("WGUID", "SOFTWARE")).order_by(
#                 df_softchange.col("CHANGETIME").desc()
#             )
#         ),
#     )
#     df_softchange = df_softchange.filter(df_softchange.col("ROWNUM") == 1)
#     df_softchange_per = Data_process.per_process(
#         df_softchange, "SOFTWARE", "NEWVERSION", file=True
#     )
#     df_softchange_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
#         df_softchange_per["SYSTEM_PERCENTAGE"]
#     )
#     end_time = time.time()
#     # st.write(end_time - start_time)

#     # BIOS Details
#     # start_time = time.time()
#     my_bar.progress(50, text="Data retrival in progress: BIOS Update")
#     sql_bios_query = f"""select wguid,systemid,name,date from EUX_PRD_EUDL_DB.DATA.SYS_BIOS
#                 where wguid in ({wguid_param})
#                 """
#     df_bioschange = Data_process.snowflake_engine.sql(sql_bios_query)
#     df_bioschange_per = Data_process.per_process(
#         df_bioschange, "NAME", "DATE", threshold=5, file=True
#     )
#     df_bioschange_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
#         df_bioschange_per["SYSTEM_PERCENTAGE"]
#     )
#     end_time = time.time()
#     # st.write(end_time - start_time)

#     # App faults
#     # start_time = time.time()
#     my_bar.progress(55, text="Data retrival in progress: Application Fault")
#     sql_appfaults_query = f"""select WGUID,VWTIME,APPNAME,CONCAT_WS(' ',APPNAME,APPVER) as APP from EUX_PRD_EUDL_DB.DATA.SYS_APPFAULTSDAILY
#                 where wguid in ({wguid_param}) and VWTIME>= current_date - 30
#                 """
#     df_appfaults = Data_process.snowflake_engine.sql(sql_appfaults_query)
#     df_appfaults = df_appfaults.with_column(
#         "rownum",
#         F.row_number().over(
#             Window.partition_by(("WGUID", "APPNAME")).order_by(
#                 df_appfaults.col("VWTIME").desc()
#             )
#         ),
#     )
#     df_appfaults = df_appfaults.filter(df_appfaults.col("ROWNUM") == 1)
#     # df_appfaults = df_appfaults.with_column("APP",F.concat_ws(F.lit(' '),df_appfaults.col("APPNAME"),df_appfaults.col("APPVER")))
#     df_appfaults_7days = df_appfaults.filter(
#         df_appfaults.col("VWTIME")
#         >= F.dateadd("day", F.lit(-7), F.current_timestamp())
#     )
#     # df_appfaults_30days = df_appfaults.filter(df_appfaults.col("VWTIME") >= F.dateadd("day",F.lit(-30),F.current_timestamp()))
#     df_appfaults_30days = df_appfaults

#     df_appfaults_7days_per = Data_process.per_process(
#         df_appfaults_7days, "APP", threshold=5, file=True
#     )
#     df_appfaults_7days_per["DAYS"] = "LAST 7 DAYS"
#     df_appfaults_7days_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
#         df_appfaults_7days_per["SYSTEM_PERCENTAGE"]
#     )

#     df_appfaults_30days_per = Data_process.per_process(
#         df_appfaults_30days, "APP", threshold=5, file=True
#     )
#     df_appfaults_30days_per["DAYS"] = "LAST 30 DAYS"
#     df_appfaults_30days_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
#         df_appfaults_30days_per["SYSTEM_PERCENTAGE"]
#     )
#     df_appfaults_30days_per = df_appfaults_30days_per[
#         : df_appfaults_7days_per.shape[0]
#     ]
#     end_time = time.time()
#     # st.write(end_time - start_time)

#     # App usage
#     # start_time = time.time()
#     my_bar.progress(60, text="Data retrival in progress: Application Usage")
#     sql_appusage_query = f"""select WGUID,APPLICATIONNAME,STARTTIME,ENDTIME from EUX_PRD_EUDL_DB.DATA.sensorscape_application_results_daily
#                 where wguid in ({wguid_param}) and STARTTIME>= {date_from} and STARTTIME<= {date_to}
#                 """
#     df_appusage = Data_process.snowflake_engine.sql(sql_appusage_query)
#     df_appusage = df_appusage.with_column(
#         "rownum",
#         F.row_number().over(
#             Window.partition_by(("WGUID", "APPLICATIONNAME")).order_by(
#                 df_appusage.col("ENDTIME").desc()
#             )
#         ),
#     )
#     df_appusage = df_appusage.filter(df_appusage.col("ROWNUM") == 1)
#     df_appusage_per = Data_process.per_process(
#         df_appusage, "APPLICATIONNAME", file=True
#     )
#     df_appusage_per["SYSTEM_PERCENTAGE"] = pd.to_numeric(
#         df_appusage_per["SYSTEM_PERCENTAGE"]
#     )
#     end_time = time.time()
#     # st.write(end_time - start_time)

#     # Job Title
#     # start_time = time.time()
#     my_bar.progress(70, text="Data retrival in progress: Employee Job Title")
#     sys_job_title = f""" select lk.wguid,lk.jobtitle,alt.name,alt.createddate from lk_users lk join alt_uhgcomputer alt on alt.name = lk.systemid  where wguid in ({wguid_param})"""

#     df_sys_job = Data_process.snowflake_engine.sql(sys_job_title).to_pandas()
#     end_time = time.time()
#     # st.write(end_time - start_time)

#     # System info data
#     # start_time = time.time()
#     my_bar.progress(65, text="Data retrival in progress: System Model")
#     sys_info_query = f""" select con.wguid,con.manufacturer,con.model,lam.hostname,lam.invoicedt,lam.assetlocation,loc.loc_remark,loc.country
#                         from lam lam
#                         join sys_system_config con on split_part(con.systemid,'.',1) = lam.hostname
#                         left join sn_locations loc on loc.loc_id = lam.assetlocation
#                         where con.wguid in ({wguid_param})"""

#     df_sys_info = Data_process.snowflake_engine.sql(sys_info_query).to_pandas()
#     # System Manufactured Date
#     df_sys_mft = df_sys_info.dropna(subset=["INVOICEDT"])
#     df_sys_mft["INVOICEDT_YEAR"] = pd.to_datetime(df_sys_mft["INVOICEDT"]).dt.year
#     df_sys_mft = (
#         df_sys_mft["INVOICEDT_YEAR"]
#         .value_counts(ascending=True)
#         .to_frame()
#         .reset_index()
#         .rename(
#             columns={"index": "MANUFACTURED YEAR", "INVOICEDT_YEAR": "WGUID COUNT"}
#         )
#     )

#     # System Model
#     df_sys_model = (
#         df_sys_info["MODEL"]
#         .value_counts()
#         .to_frame()
#         .reset_index()
#         .rename(columns={"index": "MODEL", "MODEL": "WGUID COUNT"})
#     )

#     # System Location
#     df_sys_location = df_sys_info.dropna(subset=["LOC_REMARK"]).reset_index()
#     df_sys_location["CITY"] = (
#         df_sys_location.LOC_REMARK.str.split("-").str[1]
#         + ","
#         + df_sys_location.COUNTRY
#     )
#     df_sys_location = (
#         df_sys_location.CITY.value_counts()
#         .to_frame()
#         .reset_index()
#         .rename(columns={"index": "LOCATION", "CITY": "WGUID COUNT"})
#     )

#     # Job Processing
#     df_sys_jobtitle = (
#         df_sys_job.JOBTITLE.value_counts()
#         .to_frame()
#         .reset_index()
#         .rename(columns={"index": "JOB TITLE", "JOBTITLE": "WGUID COUNT"})[0:20]
#     )
#     end_time = time.time()
#     # st.write(end_time - start_time)

#     # Health Score check for summary
#     # start_time = time.time()
#     my_bar.progress(80, text="Data retrival in progress: System Health Score")
#     sys_hs_query = f"""select wguid,vwtime,wscore
#                     from sys_healthdaily
#                     where vwtime >= current_date - 30 and activestatus = 1
#                     and wguid in ({wguid_param})"""

#     df_sys_hs = Data_process.snowflake_engine.sql(sys_hs_query).to_pandas()
#     df_sys_hs_7days = df_sys_hs[
#         df_sys_hs.VWTIME >= datetime.datetime.now() - pd.to_timedelta("7day")
#     ]
#     df_sys_hs_30days = df_sys_hs[
#         df_sys_hs.VWTIME >= datetime.datetime.now() - pd.to_timedelta("30day")
#     ]

#     df_sys_hs_7days_sum = (
#         df_sys_hs_7days.groupby("WGUID")["WSCORE"].agg("mean").to_frame()
#     )
#     df_sys_hs_30days_sum = (
#         df_sys_hs_30days.groupby("WGUID")["WSCORE"].agg("mean").to_frame()
#     )
#     df_sys_hs_7days_sum["HEALTH_SCORE"] = pd.cut(
#         df_sys_hs_7days_sum["WSCORE"],
#         bins=[0, 80, 90, 97, float("Inf")],
#         labels=["Poor", "Fair", "Good", "Excellent"],
#     )
#     df_sys_hs_30days_sum["HEALTH_SCORE"] = pd.cut(
#         df_sys_hs_30days_sum["WSCORE"],
#         bins=[0, 80, 90, 97, float("Inf")],
#         labels=["Poor", "Fair", "Good", "Excellent"],
#     )

#     df_sys_hs_30days_sum["WGUID"] = df_sys_hs_30days_sum.index
#     df_sys_hs_7days_sum["WGUID"] = df_sys_hs_7days_sum.index
#     df_sys_hs_30days_sum1 = (
#         df_sys_hs_30days_sum.groupby(by="HEALTH_SCORE", as_index=False)
#         .agg({"WGUID": pd.Series.nunique})
#         .rename(columns={"WGUID": "WGUID_Count"})
#     )
#     df_sys_hs_30days_sum2 = (
#         df_sys_hs_30days_sum.groupby(by="HEALTH_SCORE", as_index=False)
#         .agg({"WGUID": set})
#         .rename(columns={"WGUID": "WGUIDs"})
#     )
#     df_sys_hs_30days_sum_merged = pd.merge(
#         df_sys_hs_30days_sum1, df_sys_hs_30days_sum2, on="HEALTH_SCORE"
#     )

#     df_sys_hs_7days_sum1 = (
#         df_sys_hs_7days_sum.groupby(by="HEALTH_SCORE", as_index=False)
#         .agg({"WGUID": pd.Series.nunique})
#         .rename(columns={"WGUID": "WGUID_Count"})
#     )
#     df_sys_hs_7days_sum2 = (
#         df_sys_hs_7days_sum.groupby(by="HEALTH_SCORE", as_index=False)
#         .agg({"WGUID": set})
#         .rename(columns={"WGUID": "WGUIDs"})
#     )
#     df_sys_hs_7days_sum_merged = pd.merge(
#         df_sys_hs_7days_sum1, df_sys_hs_7days_sum2, on="HEALTH_SCORE"
#     )
#     end_time = time.time()
#     # st.write(end_time - start_time)

#     my_bar.empty()

#     end_time = time.time()
#     # st.write(end_time - start_time)
#     return (
#         df_patch_per,
#         df_contrl_per,
#         df_software_per,
#         df_softchange_per,
#         df_bioschange_per,
#         df_appfaults_7days_per,
#         df_appfaults_30days_per,
#         df_appusage_per,
#         df_sys_mft,
#         df_sys_model,
#         df_sys_location,
#         df_sys_info,
#         df_sys_jobtitle,
#         df_sys_hs_7days_sum_merged,
#         df_sys_hs_30days_sum_merged,
#     )

# def fetch_patch(self,wguid_list,date_from):

#     date_from = "'" + str(date_from) + "'"
#     wguid_param = ','.join(("'" + str(n) +"'" for n in wguid_list))
#     #Patch
#     sql_patch_query = f'''select wguid,systemid,patchname,installdate from EUX_PRD_EUDL_DB.DATA.SYS_PATCHES
#                     where wguid in ({wguid_param}) and installdate >= {date_from}
#                     order by installdate'''
#     df_patch=pd.read_sql(sql_patch_query,self.engine1)

#     df_patch_per = df_patch.groupby('patchname')['patchname'].count().to_frame().rename(columns={'patchname':'System_Count'})
#     df_patch_per['SYSTEM_PERCENTAGE'] = df_patch_per['System_Count']/df_patch.wguid.nunique() * 100
#     df_patch_per = df_patch_per[df_patch_per.SYSTEM_PERCENTAGE>50]
#     df_patch_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False,inplace=True)
#     df_patch_per = df_patch_per.drop('System_Count',axis=1).reset_index()

#     return df_patch_per

# def fetch_controller(self,wguid_list,date_from):
#     date_from = "'" + str(date_from) + "'"
#     wguid_param = ','.join(("'" + str(n) +"'" for n in wguid_list))

#     #Controller
#     sql_controller_query = f'''select wguid,systemid,caption as controller,manufacturer,devicestatus,datemodified from EUX_PRD_EUDL_DB.DATA.SYS_CONTROLLER
#                  WHERE wguid in ({wguid_param}) and datemodified >= {date_from}
#                 order by datemodified'''

#     df_contrl=pd.read_sql(sql_controller_query,self.engine1)

#     df_contrl_per = df_contrl.groupby('controller')['controller'].count().to_frame().rename(columns={'controller':'System_Count'})
#     df_contrl_per['SYSTEM_PERCENTAGE'] = df_contrl_per['System_Count']/df_contrl.wguid.nunique()* 100
#     df_contrl_per = df_contrl_per[df_contrl_per.SYSTEM_PERCENTAGE > 50]
#     df_contrl_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False,inplace=True)
#     df_contrl_per = df_contrl_per.drop('System_Count',axis=1).reset_index()
#     df_contrl_per.rename(columns={'controller':'CONTROLLER'},inplace=True)

#     return df_contrl_per

# def fetch_software(self,wguid_list,date_from):

#     date_from = "'" + str(date_from) + "'"
#     wguid_param = ','.join(("'" + str(n) +"'" for n in wguid_list))

#     #Software
#     sql_software_query = f'''select wguid,systemId,packagename,version,installdate from EUX_PRD_EUDL_DB.DATA.SYS_SOFTWARE
#                      where wguid in ({wguid_param}) and installdate >= {date_from} order by installdate'''

#     df_software=pd.read_sql(sql_software_query,self.engine1)
#     df_software_per = df_software.groupby(['packagename','version'])['packagename'].count().to_frame().rename(columns={'packagename':'System_Count'})
#     df_software_per['SYSTEM_PERCENTAGE'] = df_software_per['System_Count']/df_software.wguid.nunique()* 100
#     df_software_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False)
#     df_software_per = df_software_per[df_software_per.SYSTEM_PERCENTAGE > 50]
#     df_software_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False,inplace=True)
#     df_software_per = df_software_per.drop('System_Count',axis=1).reset_index()
#     return df_software_per

# def fetch_software_changes(self,wguid_list,date_from):

#     date_from = "'" + str(date_from) + "'"
#     wguid_param = ','.join(("'" + str(n) +"'" for n in wguid_list))

#     # Software Version changes
#     sql_softchange_query = f'''select wguid,systemname,changedesc as SOFTWARE,changeclass,originalversion,newversion,changetime,changetypevalue from EUX_PRD_EUDL_DB.DATA.SYS_CHANGESDAILY
#                 where wguid in ({wguid_param}) and changetime >= {date_from}
#                 order by CHANGETIME'''

#     df_softchange=pd.read_sql(sql_softchange_query,self.engine1)
#     df_softchange = df_softchange.loc[df_softchange.groupby(['wguid','software'])['changetime'].idxmax()]
#     df_softchange_per = df_softchange.groupby(['software','newversion','changeclass'])['software'].count().to_frame().rename(columns={'software':'System_Count'})
#     df_softchange_per['SYSTEM_PERCENTAGE'] = df_softchange_per['System_Count']/df_softchange.wguid.nunique()* 100
#     df_softchange_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False)
#     df_softchange_per = df_softchange_per[df_softchange_per.SYSTEM_PERCENTAGE > 50]
#     df_softchange_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False,inplace=True)
#     df_softchange_per = df_softchange_per.drop('System_Count',axis=1).reset_index()
#     return df_softchange_per

# def fetch_bios(self,wguid_list,date_from):

#     date_from = "'" + str(date_from) + "'"
#     wguid_param = ','.join(("'" + str(n) +"'" for n in wguid_list))

#     # BIOS Details
#     sql_bios_query = f'''select wguid,systemid,name,date from EUX_PRD_EUDL_DB.DATA.SYS_BIOS
#                 where wguid in ({wguid_param})
#                 order by date'''
#     df_bioschange=pd.read_sql(sql_bios_query,self.engine1)

#     df_bioschange_per = df_bioschange.groupby(['name','date'])['name'].count().to_frame().rename(columns={'name':'System_Count'})
#     df_bioschange_per['SYSTEM_PERCENTAGE'] = df_bioschange_per['System_Count']/df_bioschange.wguid.nunique()* 100
#     df_bioschange_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False)
#     df_bioschange_per = df_bioschange_per[df_bioschange_per.SYSTEM_PERCENTAGE > 5]
#     df_bioschange_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False,inplace=True)
#     df_bioschange_per = df_bioschange_per.drop('System_Count',axis=1).reset_index()
#     return df_bioschange_per

# def fetch_appfaults(self,wguid_list,date_from):

#     date_from = "'" + str(date_from) + "'"
#     wguid_param = ','.join(("'" + str(n) +"'" for n in wguid_list))

#     # App faults
#     sql_appfaults_query = f'''select * from EUX_PRD_EUDL_DB.DATA.SYS_APPFAULTSDAILY
#                 where wguid in ({wguid_param}) and VWTIME>= {date_from}
#                 '''
#     df_appfaults = pd.read_sql(sql_appfaults_query,self.engine1)
#     df_appfaults = df_appfaults.loc[df_appfaults.groupby(['wguid','appname'])['vwtime'].idxmax()]
#     df_appfaults['APP'] = df_appfaults['appname'] + ' ' + df_appfaults['appver']
#     df_appfaults_7days = df_appfaults[df_appfaults.vwtime>=datetime.datetime.now() - pd.to_timedelta("7day")]
#     df_appfaults_30days = df_appfaults[df_appfaults.vwtime>=datetime.datetime.now() - pd.to_timedelta("30day")]

#     df_appfaults_7days_per = df_appfaults_7days.groupby('APP')['APP'].count().to_frame().rename(columns={'APP':'System_Count'})
#     df_appfaults_7days_per['SYSTEM_PERCENTAGE'] = df_appfaults_7days_per['System_Count']/df_appfaults_7days.wguid.nunique()* 100
#     df_appfaults_7days_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False)
#     df_appfaults_7days_per = df_appfaults_7days_per[df_appfaults_7days_per.SYSTEM_PERCENTAGE > 5]
#     df_appfaults_7days_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False,inplace=True)
#     df_appfaults_7days_per = df_appfaults_7days_per.drop('System_Count',axis=1).reset_index()
#     df_appfaults_7days_per['DAYS'] = 'LAST 7 DAYS'

#     df_appfaults_30days_per = df_appfaults_30days.groupby('APP')['APP'].count().to_frame().rename(columns={'APP':'System_Count'})
#     df_appfaults_30days_per['SYSTEM_PERCENTAGE'] = df_appfaults_30days_per['System_Count']/df_appfaults_30days.wguid.nunique()* 100
#     df_appfaults_30days_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False)
#     df_appfaults_30days_per = df_appfaults_30days_per[df_appfaults_30days_per.SYSTEM_PERCENTAGE > 5]
#     df_appfaults_30days_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False,inplace=True)
#     df_appfaults_30days_per = df_appfaults_30days_per.drop('System_Count',axis=1).reset_index()
#     df_appfaults_30days_per['DAYS'] = 'LAST 30 DAYS'
#     df_appfaults_30days_per = df_appfaults_30days_per[:df_appfaults_7days_per.shape[0]]
#     return df_appfaults_7days_per,df_appfaults_30days_per

# def fetch_appusage(self,wguid_list,date_from):

#     date_from = "'" + str(date_from) + "'"
#     wguid_param = ','.join(("'" + str(n) +"'" for n in wguid_list))

#     sql_appusage_query = f'''select WGUID,APPLICATIONNAME,STARTTIME,ENDTIME from EUX_PRD_EUDL_DB.DATA.SYS_APPLICATION
#                 where wguid in ({wguid_param}) and STARTTIME>= {date_from} AND USERNAME NOT LIKE '%SYSTEM%'
#                 '''
#     df_appusage=pd.read_sql(sql_appusage_query,self.engine1)

#     df_appusage = df_appusage.loc[df_appusage.groupby(['wguid','applicationname'])['endtime'].idxmax()]

#     df_appusage_per = df_appusage.groupby('applicationname')['applicationname'].count().to_frame().rename(columns={'applicationname':'System_Count'})
#     df_appusage_per['SYSTEM_PERCENTAGE'] = df_appusage_per['System_Count']/df_appusage.wguid.nunique()* 100
#     df_appusage_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False)
#     df_appusage_per = df_appusage_per[df_appusage_per.SYSTEM_PERCENTAGE > 50]
#     df_appusage_per.sort_values(by='SYSTEM_PERCENTAGE',ascending=False,inplace=True)
#     df_appusage_per = df_appusage_per.drop('System_Count',axis=1).reset_index()

#     return df_appusage_per

# def fetch_systeminfo(self,wguid_list,date_from):

#     date_from = "'" + str(date_from) + "'"
#     wguid_param = ','.join(("'" + str(n) +"'" for n in wguid_list))

#     # System info data
#     sys_info_query = f''' select con.wguid,con.manufacturer,con.model,lam.hostname,lam.invoicedt,lam.assetlocation,loc.loc_remark,loc.country
#                         from lam lam
#                         join sys_system_config con on split_part(con.systemid,'.',1) = lam.hostname
#                         left join sn_locations loc on loc.loc_id = lam.assetlocation
#                         where con.wguid in ({wguid_param})'''
#     df_sys_info = pd.read_sql(sys_info_query, self.engine1)
#     #df_sys_info.to_excel(r'sys_info.xlsx')

#     #System Manufactured Date
#     df_sys_mft = df_sys_info.dropna(subset=['invoicedt'])
#     df_sys_mft['INVOICEDT_YEAR'] = pd.to_datetime(df_sys_mft['invoicedt']).dt.year
#     df_sys_mft = df_sys_mft['INVOICEDT_YEAR'].value_counts(ascending=True).to_frame().reset_index().rename(columns={'index':'MANUFACTURED YEAR','INVOICEDT_YEAR':'WGUID COUNT'})
#     #df_sys_mft.to_excel(r'mft.xlsx')

#     # System Model
#     df_sys_model = df_sys_info['model'].value_counts().to_frame().reset_index().rename(columns={'index':'model','model':'WGUID COUNT'})
#     #df_sys_model.to_excel(r'model_raw.xlsx')

#     # System Location
#     df_sys_location = df_sys_info.dropna(subset=['loc_remark']).reset_index()
#     df_sys_location['CITY'] = df_sys_location.loc_remark.str.split('-').str[1] + ',' + df_sys_location.country
#     df_sys_location = df_sys_location.CITY.value_counts().to_frame().reset_index().rename(columns={'index':'LOCATION','CITY':'WGUID COUNT'})

#     return df_sys_mft,df_sys_model,df_sys_location

# def fetch_jobtitle(self,wguid_list,date_from):

#     date_from = "'" + str(date_from) + "'"
#     wguid_param = ','.join(("'" + str(n) +"'" for n in wguid_list))

#     # jOB Title
#     sys_job_title = f''' select lk.wguid,lk.jobtitle,alt.name,alt.createddate from lk_users lk join alt_uhgcomputer alt on alt.name = lk.systemid  where wguid in ({wguid_param})'''
#     df_sys_job = pd.read_sql(sys_job_title, self.engine1)

#     #df_sys_job.to_excel(r'jobraw.xlsx')
#     df_sys_jobtitle = df_sys_job.jobtitle.value_counts().to_frame().reset_index().rename(columns={'index':'JOB TITLE','jobtitle':'WGUID COUNT'})[0:20]

#     return df_sys_jobtitle

# def fetch_health_data(self,wguid_list,date_from):

#     date_from = "'" + str(date_from) + "'"
#     wguid_param = ','.join(("'" + str(n) +"'" for n in wguid_list))

#     # Health Score check for summary
#     sys_hs_query = f'''select wguid,vwtime,wscore
#                     from sys_healthdaily
#                     where vwtime >= current_date - 30 and activestatus = 1
#                     and wguid in ({wguid_param})'''

#     df_sys_hs = pd.read_sql(sys_hs_query, self.engine1)

#     df_sys_hs_7days = df_sys_hs[df_sys_hs.vwtime>=datetime.datetime.now() - pd.to_timedelta("7day")]
#     df_sys_hs_30days = df_sys_hs[df_sys_hs.vwtime>=datetime.datetime.now() - pd.to_timedelta("30day")]

#     df_sys_hs_7days_sum = df_sys_hs_7days.groupby('wguid')['wscore'].agg('mean').to_frame()
#     df_sys_hs_30days_sum = df_sys_hs_30days.groupby('wguid')['wscore'].agg('mean').to_frame()

#     df_sys_hs_7days_sum['HEALTH_SCORE'] = pd.cut(df_sys_hs_7days_sum['wscore'], bins=[0, 80, 90, 97, float('Inf')], labels=['Poor', 'Fair', 'Good','Excellent'])
#     df_sys_hs_30days_sum['HEALTH_SCORE']= pd.cut(df_sys_hs_30days_sum['wscore'], bins=[0, 80, 90, 97, float('Inf')], labels=['Poor', 'Fair', 'Good','Excellent'])

#     df_sys_hs_30days_sum['WGUID'] = df_sys_hs_30days_sum.index
#     df_sys_hs_7days_sum['WGUID'] = df_sys_hs_7days_sum.index
#     df_sys_hs_30days_sum1 = df_sys_hs_30days_sum.groupby(by='HEALTH_SCORE',as_index=False).agg({'WGUID': pd.Series.nunique}).rename(columns={'WGUID':'WGUID_Count'})
#     df_sys_hs_30days_sum2 = df_sys_hs_30days_sum.groupby(by='HEALTH_SCORE',as_index=False).agg({'WGUID': set}).rename(columns={'WGUID':'WGUIDs'})
#     df_sys_hs_30days_sum_merged = pd.merge(df_sys_hs_30days_sum1,df_sys_hs_30days_sum2,on='HEALTH_SCORE')

#     df_sys_hs_7days_sum1 = df_sys_hs_7days_sum.groupby(by='HEALTH_SCORE',as_index=False).agg({'WGUID': pd.Series.nunique}).rename(columns={'WGUID':'WGUID_Count'})
#     df_sys_hs_7days_sum2 = df_sys_hs_7days_sum.groupby(by='HEALTH_SCORE',as_index=False).agg({'WGUID': set}).rename(columns={'WGUID':'WGUIDs'})
#     df_sys_hs_7days_sum_merged = pd.merge(df_sys_hs_7days_sum1,df_sys_hs_7days_sum2,on='HEALTH_SCORE')
#     return df_sys_hs_7days_sum_merged,df_sys_hs_30days_sum_merged
