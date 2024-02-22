import pandas as pd
import datetime
from datetime import date, timedelta
import time
import os

import schedule

# change 8/7 Snowpark integration for performance enhancement
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *

from Data_Backend import Data_process


pd.options.mode.chained_assignment = None

# function active because it lets snowflake handle much of the computations without stressing the k8s cluster
def create_preload_files(sensorname, date_from, date_to):
    print(Data_process.snowflake_engine)
    sensorname_plain = sensorname.replace(" ", "_")
    validConnection = True
    data_count = None

    while validConnection:
        try:
            df_6mnth, df = Data_process.fetch_wguid(
                sensorname, date_from, date_to, preload=True
            )
            validConnection = False
            break
        except Exception as ex:
            validConnection = True
            print(f"Caught exception {ex}. Sleeping and then trying again")
            time.sleep(5000)
            Data_process.snowflake_engine = Data_process.setup_snowflake_connection()

    print(sensorname_plain)
    print(type(df))

    # Check for the kind of dataframe it is, and store the count
    if str(type(df)) == "<class 'snowflake.snowpark.dataframe.DataFrame'>":
        data_count = df.count()
        if df.count() == 0:
            return False
    elif str(type(df)) == "<class 'pandas.core.frame.DataFrame'>":
        data_count = df.shape[0]
        if df.shape[0] == 0:
            return False

    # use the large wguid function for large count sizes
    if data_count > 16000:
        df_6mnth, df = Data_process.fetch_wguid_large_wguid(
            sensorname, date_from, date_to, preload=True
        )

        # check dataframe and convert accordingly
        if str(type(df)) == "<class 'pandas.core.frame.DataFrame'>":
            df.to_csv(
                f"/files/{sensorname_plain}_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
                index=False,
                compression="gzip",
            )
        else:
            df.to_pandas().to_csv(
                f"/files/{sensorname_plain}_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
                index=False,
                compression="gzip",
            )
        df_6mnth.to_csv(
            f"/files/{sensorname_plain}_df_6mnth_filter_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
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
        if str(type(df)) == "<class 'pandas.core.frame.DataFrame'>":
            df.to_csv(
                f"/files/{sensorname_plain}_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
                index=False,
                compression="gzip",
            )
            wguid_list = df.WGUID.to_list()

        else:
            df.to_pandas().to_csv(
                f"/files/{sensorname_plain}_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
                index=False,
                compression="gzip",
            )
            wguid_list = df.to_pandas().WGUID.to_list()

        df_6mnth.to_csv(
            f"/files/{sensorname_plain}_df_6mnth_filter_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz",
            index=False,
            compression="gzip",
        )
        wguid_list = list(set(wguid_list))
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
            sensorname, wguid_list, date_from, date_to, preload=True
        )

    df_patch.to_csv(
        f"/files/{sensorname_plain}_sql_patch_query.csv.gz",
        index=False,
        compression="gzip",
    )

    df_controller.to_csv(
        f"/files/{sensorname_plain}_sql_controller_query.csv.gz",
        index=False,
        compression="gzip",
    )

    df_software.to_csv(
        f"/files/{sensorname_plain}_sql_software_query.csv.gz",
        index=False,
        compression="gzip",
    )

    df_software_change.to_csv(
        f"/files/{sensorname_plain}_sql_softchange_query.csv.gz",
        index=False,
        compression="gzip",
    )

    df_bios.to_csv(
        f"/files/{sensorname_plain}_sql_bios_query.csv.gz",
        index=False,
        compression="gzip",
    )

    df_appfaults_7days.to_csv(
        f"/files/{sensorname_plain}_7day_sql_appfaults_query.csv.gz",
        index=False,
        compression="gzip",
    )
    df_appfaults_30days.to_csv(
        f"/files/{sensorname_plain}_30day_sql_appfaults_query.csv.gz",
        index=False,
        compression="gzip",
    )

    df_appusage.to_csv(
        f"/files/{sensorname_plain}_sql_appusage_query.csv.gz",
        index=False,
        compression="gzip",
    )

    df_sys_info.to_csv(
        f"/files/{sensorname_plain}_sys_info_query.csv.gz",
        index=False,
        compression="gzip",
    )
    df_sys_mft.to_csv(
        f"/files/{sensorname_plain}_sys_mft_query.csv.gz",
        index=False,
        compression="gzip",
    )
    df_sys_model.to_csv(
        f"/files/{sensorname_plain}_sys_model_query.csv.gz",
        index=False,
        compression="gzip",
    )
    df_sys_location.to_csv(
        f"/files/{sensorname_plain}_sys_location_query.csv.gz",
        index=False,
        compression="gzip",
    )
    df_jobtitle.to_csv(
        f"/files/{sensorname_plain}_sys_jobtitle_query.csv.gz",
        index=False,
        compression="gzip",
    )

    df_hs_30days.to_csv(
        f"/files/{sensorname_plain}_30day_sys_hs_query.csv.gz",
        index=False,
        compression="gzip",
    )
    df_hs_7days.to_csv(
        f"/files/{sensorname_plain}_7day_sys_hs_query.csv.gz",
        index=False,
        compression="gzip",
    )
    return True


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


def daily_preload():
    # setup connection and current day for preload files
    date_to = date.today()
    date_from = date_to - timedelta(days=30)
    Data_process.snowflake_engine = Data_process.setup_snowflake_connection()
    no_result = []

    # iterate through each sensor and generate its files
    for sensorname in sensor_list:
        validConnection = True
        while validConnection:
            try:
                # create the files and store if files created or not
                file_created = create_preload_files(sensorname, date_from, date_to)
                validConnection = False
                break
            except Exception as ex:
                validConnection = True
                print(f"Caught exception {ex}. Sleeping and then trying again")
                time.sleep(5000)
                file_created = create_preload_files(sensorname, date_from, date_to)

        # if file not created, append it to list of files of no results
        if not file_created:
            no_result.append(sensorname)

    # convert files of no results to series and export. This way we can refer to this results instead of querying for them later
    no_result_sensor = pd.Series(no_result)
    no_result_sensor.to_csv("/files/no_result_sensor.csv")
    print("Finished daily preload")


def check_daily_preload():
    if os.path.isfile("/files/no_result_sensor.csv"):
        return

    # setup connection and current day for preload files
    date_to = date.today()
    date_from = date_to - timedelta(days=30)
    Data_process.snowflake_engine = Data_process.setup_snowflake_connection()
    no_result = []

    # iterate through each sensor and generate its files
    for sensorname in sensor_list:
        if os.path.isfile("/files/no_result_sensor.csv"):
            return

        validConnection = True

        # Check if preload files already created, if not create them
        sensorname_plain = sensorname.replace(" ", "_")

        # There should be the  _SENSORSCAPE_SENSOR_RESULTS_DAILY file there for the sensor. If so,continue to the next sensor
        if os.path.isfile(
            f"/files/{sensorname_plain}_SENSORSCAPE_SENSOR_RESULTS_DAILY.csv.gz"
        ):
            continue

        while validConnection:
            try:
                # create the files and store if files created or not
                file_created = create_preload_files(sensorname, date_from, date_to)
                validConnection = False
                break
            except Exception as ex:
                validConnection = True
                print(f"Caught exception {ex}. Sleeping and then trying again")
                time.sleep(5000)
                file_created = create_preload_files(sensorname, date_from, date_to)

        # if file not created, append it to list of files of no results
        if not file_created:
            no_result.append(sensorname)

    # convert files of no results to series and export. This way we can refer to this results instead of querying for them later
    no_result_sensor = pd.Series(no_result)
    no_result_sensor.to_csv("/files/no_result_sensor.csv")
    print("check finished")


# create schedule to run this every day at 10:00 UTC/4:00 AM MST
schedule.every().day.at("07:10", "UTC").do(daily_preload)

# create a check to make sure all files are there
schedule.every(6).hours.do(check_daily_preload)

print(schedule.get_jobs())

if __name__ == "__main__":
    while True:
        # print("running load.py job")
        schedule.run_pending()
        time.sleep(1)
