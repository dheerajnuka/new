#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 16 12:51:09 2023

@author: arakshi3
"""

import logging, warnings
import sys, os
import glob
import pandas as pd
import numpy as np
import schedule

import time
import datetime
from datetime import datetime, timedelta, timezone, date

from Data_Backend import Data_process

import time

from cryptography.hazmat.backends import default_backend

# from cryptography.hazmat.primitives.asymmetric import rsa
# from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization


# change 8/7 Snowpark integration for performance enhancement
from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
from snowflake.snowpark.window import Window

from configparser import ConfigParser

# plotting libraries
import seaborn as sb
import matplotlib.pyplot as plt
from matplotlib import style

print("starting anomaly")

sensorlist = [
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


def snowflake_connection(audit, schema="DATA"):

    try:
        # authenticate to snowflake using eudl rsa key
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
                "schema": schema,
                "user": "eudl_db@optum.com",
                "private_key": pkb,
                "insecure_mode": True,
            }

            # set global snowflake_engine to created engine1. this is to create snowpark engine for server side computation
            engine = Session.builder.configs(connection_parameters).create()

        return (True, engine)
    except Exception as err:
        print("Snowflake connection failed:", err)
        audit.Logger.info(f"Snowflake connection failed to DATA schema: {err}")
        return (False, "")


# Audit logging
class Audit:
    # create log for process
    warnings.filterwarnings("ignore")
    start = datetime.now()
    logging.basicConfig(
        filename="/files/Snowflake_pipeline_log.log",
        format="%(levelname)s %(asctime)s %(filename)s:%(lineno)s %(funcName)s() %(message)s",
        level=logging.INFO,
    )
    Logger = logging.getLogger()
    Logger.info("**** Process started at %s ****", start)


# Get connection object for snowflake and Systrack
def connection(audit):
    # make the connection to the DATA and WORKSPACE schemas
    print("Connection string")
    snowflake_conn_status, snowflake_engine = snowflake_connection(
        audit, "DATA"
    )  # Snowflake Authentication
    snowflake_conn_status_write, snowflake_engine_write = snowflake_connection(
        audit, "WORKSPACE"
    )  # Snowflake Authentication

    # Handle exception
    if not snowflake_conn_status or (not snowflake_conn_status_write):
        audit.Logger.info("Application/Database connection failed")
        return False, "", "", ""

    audit.Logger.info("Snowflake connection established successfully")
    return True, snowflake_engine_write, snowflake_engine


def snowflake_query_getdata(audit, snowflake_engine, sqlquery):
    # using snowpark, load the data into
    dfsqlquery1 = snowflake_engine.sql(sqlquery)
    if dfsqlquery1.count() == 0:
        audit.Logger.info("snowflake_query_getquery: No data found")
        return dfsqlquery1

    return dfsqlquery1


def snowflake_query_getstring(startdate, enddate):
    # Snowflake query for sensor results. Very large query result. Do not try to attempt to bring all this into memory at once
    sensor_list = ",".join(("'" + str(n) + "'" for n in sensorlist))
    sqlquery = f"""
    select
    ssensor.WGUID ,   ssensor.SYSTEMID,   ssensor.SENSORNAME, 
    ssensor.SEVERITY, ssensor.RESULTTIME, ssensor.INSTANCEVALUE,
    sconfig.SYSTEMID as "SCONFIG_SYSTEMID", sconfig.TOTALPHYSICALMEMORY/10e6,sconfig.MODEL,
    sdefinition.SENSORCATEGORY, sdefinition.DESCRIPTION
    from EUX_PRD_EUDL_DB.DATA.SYS_SENSORRESULTS ssensor
    left join EUX_PRD_EUDL_DB.DATA.SYS_SYSTEM_CONFIG sconfig on ssensor.WGUID = sconfig.WGUID
    left join EUX_PRD_EUDL_DB.DATA.SYS_SENSORINFORMATION sdefinition on ssensor.sensorid =  sdefinition.RULEID
    where
    --ssensor.SENSORNAME like '%Application%' and 
    ssensor.SEVERITY > 4 and
    ssensor.systemid not like 'V%' and
    ssensor.RESULTTIME > '{startdate}'  and 
    ssensor.RESULTTIME < '{enddate}'    and 
    ssensor.sensorname in ({sensor_list})
    UNION ALL
    select
    ssensor.WGUID ,   ssensor.SENSORSYSTEMID as SYSTEMID,   ssensor.SENSORNAME, 
    ssensor.SEVERITY, ssensor.RESULTTIME, ssensor.INSTANCEVALUE,
    sconfig.SYSTEMID as "SCONFIG_SYSTEMID", sconfig.TOTALPHYSICALMEMORY/10e6,sconfig.MODEL,
    sdefinition.SENSORCATEGORY, sdefinition.DESCRIPTION
    from DATA.SENSORSCAPE_DERIVED_SENSORS ssensor
    left join DATA.SYS_SYSTEM_CONFIG sconfig on ssensor.WGUID = sconfig.WGUID
    left join DATA.SYS_SENSORINFORMATION sdefinition on ssensor.sensorid =  sdefinition.RULEID
    where 
    ssensor.SEVERITY > 4 and
    ssensor.SENSORSYSTEMID not like 'V%' and
    ssensor.RESULTTIME > '{startdate}'  and 
    ssensor.RESULTTIME < '{enddate}'    and 
    ssensor.sensorname in ({sensor_list})
    """
    return sqlquery


def snowflake_getData(startdate, enddate):
    # Logging object
    audit = Audit()

    try:
        # get the query for the start and end date
        sqlquery = snowflake_query_getstring(startdate, enddate)

        # make the connection and get the engine
        connection_flg, snowflake_engine_write, snowflake_engine = connection(audit)

        # get the query results stored lazily in snowpark dataframe
        dfsqlquery_final = snowflake_query_getdata(audit, snowflake_engine, sqlquery)

        return dfsqlquery_final
    except Exception as ex:
        trace = []
        tb = ex.__traceback__
        while tb is not None:
            trace.append(
                {
                    "filename": tb.tb_frame.f_code.co_filename,
                    "name": tb.tb_frame.f_code.co_name,
                    "lineno": tb.tb_lineno,
                }
            )
            tb = tb.tb_next
        audit.Logger.info(
            "**** Failure/Error at %s ****",
            str({"type": type(ex).__name__, "message": str(ex), "trace": trace}),
        )


def eda_queryData(dfsqlquery):
    # normally here we do eda, however we only need the function to transform the date for preload purposes
    dfsqlquery["date"] = [d.date() for d in dfsqlquery["RESULTTIME"]]
    return dfsqlquery


def getsubsensor_queryData(dfsqlquery, sensorname):
    # filter out for sensorname
    return dfsqlquery.filter(dfsqlquery.col("SENSORNAME") == sensorname)


def timeseries_queryData(dfsqlquery):
    # get the time series of the data
    dftimeseries = dfsqlquery.groupby("date")["date"].size()
    dftimeseries = dftimeseries.reset_index(name="y")
    dftimeseries.rename(columns={"date": "ds"}, inplace=True)
    dftimeseries["ds"] = pd.to_datetime(dftimeseries["ds"])

    return dftimeseries


def plot_queryDataforindividualsensor(dfsqlquery, sensorname):
    # sensorname = 'Long Add In Load Time'
    print(f"Plotting data for {sensorname}")
    # dfsqlquerysensor =   dfsqlquery[dfsqlquery['sensorname'] == sensorname]
    # setting style for graphs
    style.use("ggplot")
    plt.rcParams["figure.figsize"] = (20, 10)

    # Single line chart
    fig1 = dfsqlquerysensor.groupby("date")["date"].size().plot(kind="line", color="r")
    plt.title(f"Time series plot for {sensorname} sensors", color="black")
    plt.xlabel("Date", color="black")
    plt.ylabel("Sensor Count", color="black")
    plt.xticks(color="black")
    plt.yticks(color="black")
    # plt.savefig('linechart_single.png')
    plt.show()

    dftimeseries = dfsqlquerysensor.groupby("date")["date"].size()
    fig2 = dftimeseries.plot(kind="line", color="r")
    plt.title(f"Time series plot for {sensorname}", color="black")
    plt.xlabel("Date", color="black")
    plt.ylabel("Sensor Count", color="black")
    plt.xticks(color="black")
    plt.yticks(color="black")
    # plt.savefig('linechart_single.png')
    plt.show()
    mean = dftimeseries.agg("mean")
    sigma = dftimeseries.agg("std")
    plt.figure(figsize=[12, 5])
    plt.plot(dftimeseries, marker="o", linestyle="-", color="b")
    plt.fill_between(
        dftimeseries.index,
        np.max(mean - 3 * sigma, 0),
        mean + 3 * sigma,
        alpha=0.2,
        color="r",
        label="Predicted interval",
    )
    plt.show()

    import statsmodels.api as sm

    dftimeseries = dftimeseries.reset_index(name="frequency")
    print(dftimeseries.dtypes)
    result = sm.tsa.seasonal_decompose(
        dftimeseries["frequency"], model="additive", period=7
    )
    # result.trend[1:200].plot()
    result.plot()


def analyze_stationarity(dftimeseries, title):
    # continue to transform data and find mean and standard deviation
    pd.options.display.float_format = "{:.8f}".format
    fig, ax = plt.subplots(2, 1, figsize=(16, 8))

    rolmean = dftimeseries.y.rolling(window=7).mean()
    rolstd = dftimeseries.y.rolling(window=7).std()
    ax[0].plot(dftimeseries, label=title)
    ax[0].plot(rolmean, label="rolling mean")
    ax[0].plot(rolstd, label="rolling std (x10)")
    ax[0].set_title("30-day window")
    ax[0].legend()

    rolmean = dftimeseries.y.rolling(window=30).mean()
    rolstd = dftimeseries.y.rolling(window=30).std()
    ax[1].plot(dftimeseries, label=title)
    ax[1].plot(rolmean, label="rolling mean")
    ax[1].plot(rolstd, label="rolling std (x10)")
    ax[1].set_title("30-day window")
    ax[1].legend()


def detect_anomalies_with_prophet(series):
    from prophet import Prophet
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    import plotly.offline as py

    py.init_notebook_mode()

    # Fitting with default parameters
    # model = Prophet(daily_seasonality=True)

    # prepare expected column names
    dftimeseries.rename(columns={"date": "ds", "frequency": "y"}, inplace=True)
    dftimeseries["ds"] = pd.to_datetime(dftimeseries["ds"])
    print(dftimeseries.dtypes)
    # define the model
    model = Prophet()
    # fit the model
    dftimeseries.head()
    model.fit(dftimeseries)
    predict = model.predict(dftimeseries)
    plt.plot(dftimeseries["ds"], dftimeseries["y"])
    plt.plot(predict["ds"], predict["trend"])
    plt.plot(predict["ds"], predict["yhat_lower"])
    plt.plot(predict["ds"], predict["yhat_upper"])

    future = model.make_future_dataframe(periods=14)
    future.tail()
    forecast = model.predict(future)
    forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail()
    fig1 = model.plot(forecast)
    fig2 = model.plot_components(predict)

    from prophet.plot import plot_plotly, plot_components_plotly

    plot_plotly(model, forecast)
    plot_components_plotly(model, forecast)


def seasonal_decompose(dftimeseries):
    # run season decompose for the data and find the trend for the data.

    # Set the timestamp column as the index and convert to a series
    series = dftimeseries.set_index("ds")["y"].squeeze()
    import statsmodels.api as sm

    decomposition = sm.tsa.seasonal_decompose(series, model="additive", period=7)

    trend = decomposition.trend
    seasonal = decomposition.seasonal
    residual = decomposition.resid

    return trend, seasonal, residual


import pandas as pd
from sklearn.ensemble import IsolationForest


def detect_anomalies_with_isolation_forest(sensorname_plain, series, dftimeseries):
    # find anomalies using the iso forest algorithm

    # Convert the series to a 2D NumPy array
    data = series.values.reshape(-1, 1)

    # Create an instance of the IsolationForest class
    model = IsolationForest(n_estimators=100, contamination=0.1, random_state=42)

    run_name = "iso_forest_run_" + str(time.time()).replace(".", "_")
    model.fit(data)
    anomalies = model.predict(data)
    anomalies_series = pd.Series(anomalies, index=series.index)

    # These next lines have to be added to Anomaly-detection.py
    plot_anomaly = series[anomalies == -1].to_frame().reset_index()
    series_frame = series.to_frame().reset_index()

    dftimeseries.to_csv(
        f"/files/{sensorname_plain}_dftimeseries.csv.gz",
        index=False,
        compression="gzip",
    )
    series_frame.to_csv(
        f"/files/{sensorname_plain}_series_frame.csv.gz",
        index=False,
        compression="gzip",
    )
    plot_anomaly.to_csv(
        f"/files/{sensorname_plain}_plot_anomaly.csv.gz",
        index=False,
        compression="gzip",
    )
    return anomalies_series


import numpy as np
import pandas as pd


# function active because it lets snowflake handle much of the computations without stressing the k8s cluster
def create_preload_files(sensorname, date_from, date_to, date_param):
    sensorname_plain = sensorname.replace(" ", "_")
    validConnection = True
    while validConnection:
        try:
            # fetch the anomaly wguids for the affected sensor
            df_6mnth, df = Data_process.fetch_anomaly_wguid(
                sensorname, date_param, date_from, date_to, preload=True
            )
            validConnection = False
            break
        except Exception as ex:
            validConnection = True
            print(f"Caught exception {ex}. Sleeping and then trying again")
            time.sleep(5000)
            Data_process.snowflake_engine = Data_process.setup_snowflake_connection()

    wguid_list = df.WGUID.to_list()
    wguid_list = list(set(wguid_list))
    if len(wguid_list) == 0:
        return False
    else:
        if len(wguid_list) > 16000:
            df_6mnth, df = Data_process.fetch_anomaly_wguid_large_wguid(
                sensorname, date_param, preload=True
            )
            df.to_csv(
                f"/files/{sensorname_plain}_anomaly_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
                index=False,
                compression="gzip",
            )
            df_6mnth.to_csv(
                f"/files/{sensorname_plain}_anomaly_df_6mnth_filter_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
                index=False,
                compression="gzip",
            )
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
                sensorname, date_from, date_to, preload=True
            )
        else:
            df.to_csv(
                f"/files/{sensorname_plain}_anomaly_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
                index=False,
                compression="gzip",
            )
            df_6mnth.to_csv(
                f"/files/{sensorname_plain}_anomaly_df_6mnth_filter_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
                index=False,
                compression="gzip",
            )
            # wguid_list = df.to_pandas().WGUID.to_list()
            # wguid_list = list(set(wguid_list))
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
                sensorname,
                wguid_list,
                date_from - timedelta(days=7),
                date_to,
                preload=True,
            )

        df_patch.to_csv(
            f"/files/{sensorname_plain}_anomaly_sql_patch_query.csv.gz",
            index=False,
            compression="gzip",
        )

        df_controller.to_csv(
            f"/files/{sensorname_plain}_anomaly_sql_controller_query.csv.gz",
            index=False,
            compression="gzip",
        )

        df_software.to_csv(
            f"/files/{sensorname_plain}_anomaly_sql_software_query.csv.gz",
            index=False,
            compression="gzip",
        )

        df_software_change.to_csv(
            f"/files/{sensorname_plain}_anomaly_sql_softchange_query.csv.gz",
            index=False,
            compression="gzip",
        )

        df_bios.to_csv(
            f"/files/{sensorname_plain}_anomaly_sql_bios_query.csv.gz",
            index=False,
            compression="gzip",
        )

        df_appfaults_7days.to_csv(
            f"/files/{sensorname_plain}_anomaly_7day_sql_appfaults_query.csv.gz",
            index=False,
            compression="gzip",
        )
        df_appfaults_30days.to_csv(
            f"/files/{sensorname_plain}_anomaly_30day_sql_appfaults_query.csv.gz",
            index=False,
            compression="gzip",
        )

        df_appusage.to_csv(
            f"/files/{sensorname_plain}_anomaly_sql_appusage_query.csv.gz",
            index=False,
            compression="gzip",
        )

        df_sys_info.to_csv(
            f"/files/{sensorname_plain}_anomaly_sys_info_query.csv.gz",
            index=False,
            compression="gzip",
        )
        df_sys_mft.to_csv(
            f"/files/{sensorname_plain}_anomaly_sys_mft_query.csv.gz",
            index=False,
            compression="gzip",
        )
        df_sys_model.to_csv(
            f"/files/{sensorname_plain}_anomaly_sys_model_query.csv.gz",
            index=False,
            compression="gzip",
        )
        df_sys_location.to_csv(
            f"/files/{sensorname_plain}_anomaly_sys_location_query.csv.gz",
            index=False,
            compression="gzip",
        )
        df_jobtitle.to_csv(
            f"/files/{sensorname_plain}_anomaly_sys_jobtitle_query.csv.gz",
            index=False,
            compression="gzip",
        )

        df_hs_30days.to_csv(
            f"/files/{sensorname_plain}_anomaly_30day_sys_hs_query.csv.gz",
            index=False,
            compression="gzip",
        )
        df_hs_7days.to_csv(
            f"/files/{sensorname_plain}_anomaly_7day_sys_hs_query.csv.gz",
            index=False,
            compression="gzip",
        )
    return True


def gen_anomalies():
    # get the main query data using the dates and store into snowpark dataframe
    startdate = date.today() - timedelta(days=365)
    startdate = startdate.strftime("%Y-%m-%d")
    enddate = date.today().strftime("%Y-%m-%d")
    dfsqlquery = snowflake_getData(startdate, enddate)
    anomaly_dict = dict()
    # print("anomaly data loaded")

    # for each sensor in the list, filter and transform the data
    for sensorname in sensorlist:
        sensorname_plain = sensorname.replace(" ", "_")
        dfsubsqlquerysensor = getsubsensor_queryData(dfsqlquery, sensorname)
        # print("finished subsensor")

        # to improve performance, we skip any sensors that produce no results
        if dfsubsqlquerysensor.count() != 0:
            # print(f"count: {dfsubsqlquerysensor.count()}")
            dfsubsqlquerysensor = dfsubsqlquerysensor.to_pandas()
            dfsubsqlquerysensor = eda_queryData(dfsubsqlquerysensor)
            dftimeseries = timeseries_queryData(dfsubsqlquerysensor)
            # print("finish timeseries query data")

            # must have minimum 14 points of data for modeling
            if dftimeseries.shape[0] < 14:
                # print(f"{sensorname} skipped")
                continue
            else:
                # print("starting seasonal decompose")
                trend, seasonal, residual = seasonal_decompose(dftimeseries)

                # get the predictions wit the iso forest
                anomalies_predicts = detect_anomalies_with_isolation_forest(
                    sensorname_plain, trend.dropna(), dftimeseries
                )

                # update the dictionary with the predictions
                anomaly_dict.update(
                    {
                        sensorname: anomalies_predicts[lambda x: x == -1]
                        .index.strftime("%Y-%m-%d")
                        .tolist()
                    }
                )
                # print(f"{sensorname} anomaly_dict updated")
    # convert sensor anomalies to Dataframe and export the file.
    df = pd.DataFrame.from_dict(anomaly_dict, orient="index")
    df = df.transpose()
    df = df.dropna(axis=1, how="all")
    df.to_csv("/files/anomaly-detection.csv")
    print("finished anomaly preload")


def emptydir():
    files = glob.glob("/files/*")
    for f in files:
        os.remove(f)


def anomaly_daily_preload():
    emptydir()
    gen_anomalies()

    # setup connection and current day for preload files
    date_to = date.today()
    date_from = date_to - timedelta(days=30)
    audit = Audit()
    Data_process.snowflake_engine = Data_process.setup_snowflake_connection()
    no_result = []

    # read anomaly detection csv. It should contain all sensors with the dates they experienced anomalies
    anomalies = pd.read_csv("/files/anomaly-detection.csv")
    anomalies = anomalies.drop(columns=["Unnamed: 0"])
    sensor_list = anomalies.columns.tolist()

    # iterate through each sensor and generate its files
    for sensorname in sensor_list:
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

        # create the files and store if files created or not
        file_created = create_preload_files(sensorname, date_from, date_to, date_param)

        # if file not created, append it to list of files of no results
        if not file_created:
            no_result.append(sensorname)

    # convert files of no results to series and export. This way we can refer to this results instead of querying for them later
    no_result_sensor = pd.Series(no_result)
    no_result_sensor.to_csv("/files/anomaly_no_result_sensor.csv")
    print("Finished daily anomaly preload")


# create a schedule to run every day at 07:00 AM UTC/1:00 AM MST.
schedule.every().day.at("07:00", "UTC").do(anomaly_daily_preload)
print(schedule.get_jobs())
print("starting anomaly detection job")
# anomaly_daily_preload()
# we set it like this so that it won't accidentally get run if imported
if __name__ == "__main__":
    while True:
        # print("running anomaly-detection.py job")
        schedule.run_pending()
        time.sleep(1)
