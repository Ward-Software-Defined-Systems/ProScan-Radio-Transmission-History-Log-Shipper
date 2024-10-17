#!/usr/bin/python3

import sys
import os
import signal
import pytz
from datetime import datetime
import time
import csv
import json
import pandas as pd
import pymsteams
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceExistsError
from azure.data.tables import UpdateMode
from dotenv import load_dotenv

load_dotenv()

# Teams Webhook
# When set to '<TEAMS WEBHOOK>', escalating to Teams is disabled.
# Teams messages are only sent when no routing information or an exception is returned.
teams_webhook = '<TEAMS WEBHOOK>'  # os.getenv("TEAMS_WEBHOOK")


def main(argv):
    if len(argv) < 2:
        help_and_exit(argv[0])
        filename = "History Log.csv"
    else:
        filename = argv[1]

    # Import Radio Transmissions From 'History Log.csv'
    csv.field_size_limit(sys.maxsize)
    df = pd.read_csv(filename, skiprows=2, skipinitialspace=True, quoting=csv.QUOTE_NONE, engine='python',
                     on_bad_lines='skip').fillna('0')

    ship_to_azure_cosmosdb(df)
    ship_to_influxdb(df)
    # ship_to_azure_table(df)


def handler(signum, frame):
    # Signal handler
    # I am really only using the to catch Ctrl-C and prompt
    # This can be expanded and a more "graceful" exit can be performed
    # TODO: Add additional signal handling

    print('\nSIGNUM: ' + str(signum) + '\n' + 'FRAME: ' + str(frame) + '\n')
    res = input("Ctrl-c was pressed. Do you want to exit? y/n ")
    if res == 'y':
        exit(1)


signal.signal(signal.SIGINT, handler)


def help_and_exit(prog):
    print('Usage: ' + prog + ' history-log.csv')
    print('Radio Transmission log was not specified, using default "History Log.csv"')
    # exit(1)


def ship_to_influxdb(df):
    # ETL for ProScan (SDS200) 'History Log.csv'

    notification = 'CSV Radio Transmissions Imported --> ship_to_influxdb(): ' + str(df.shape[0]) + " (" + str(datetime.now()) + ")"
    print(notification)
    if teams_webhook != '<TEAMS WEBHOOK>':
        notification_to_teams(notification)

    # Initializing InfluxDB Client
    write_client = influxdb_client.InfluxDBClient(url=os.environ.get('INFLUXDB_URL'),
                                                  token=os.environ.get('INFLUXDB_TOKEN'),
                                                  org=os.environ.get('INFLUXDB_ORG'))

    # Defining the write api
    write_api = write_client.write_api(write_options=SYNCHRONOUS)

    count = 0
    for index, row in df.iterrows():

        # Convert ProScan Timestamp and Format to UTC Zulu format (Required for InfluxDB)
        local = pytz.timezone("America/Los_Angeles")
        naive = datetime.strptime(row['Start Date / Time'], "%m/%d/%y %H:%M:%S")
        local_dt = local.localize(naive, is_dst=None)
        utc_dt = local_dt.astimezone(pytz.utc)
        influx_db_time_utc_zformat = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Convert ProScan Duration from HOURS:MINUTES:SECONDS to MINUTES
        h, m, s = str(row['Duration']).split(':')
        duration = float((int(h) * 60) + int(m) + (int(s) / 60))

        # Convert remove '-' from STR types
        if isinstance(row['Talk Group'], str):
            talk_group = row['Talk Group'].replace('-', '')
        else:
            talk_group = row['Talk Group']

        # Create Radio Transmission Point/Measurement and write to InfluxDB
        # Used explicit casts for each data type, some are redundant and could be excluded
        point = (
            Point("transmission").time(influx_db_time_utc_zformat)
            .tag("Talk_Group", str(talk_group))
            .tag("Tone", str(row['Tone']))
            .tag("Mod", str(row['Mod']))
            .tag("System_Site", str(row['System / Site']))
            .tag("Department", str(row['Department']))
            .tag("Channel", str(row['Channel']))
            .tag("System_Type", str(row['System Type']))
            .tag("Digital_Status", str(row['Digital Status']))
            .tag("Service_Type", str(row['Service Type']))
            .tag("Number_Tune", str(row['Number Tune']))
            .field("Freq", float(str(row['Frequency']).replace('/', '')))
            .field("UID", int(row['UID']))
            .field("HITs", int(row['Hits']))
            .field("Duration", float(duration))
            .field("RSSI", float(row['RSSI']))
        )
        write_api.write(bucket=os.environ.get('INFLUXDB_BUCKET'), org=os.environ.get('INFLUXDB_ORG'), record=point)
        count += 1
        # time.sleep(1)  # separate points by 1 second

    notification = 'Radio Transmissions to InfluxDB: ' + str(count) + " (" + str(datetime.now()) + ")"
    print(notification)
    if teams_webhook != '<TEAMS WEBHOOK>':
        notification_to_teams(notification)


def ship_to_azure_table(df):
    # ETL for ProScan (SDS200) 'History Log.csv'

    notification = 'CSV Radio Transmissions Imported --> ship_to_azure_table(): ' + str(df.shape[0]) + " (" + str(
        datetime.now()) + ")"
    print(notification)
    if teams_webhook != '<TEAMS WEBHOOK>':
        notification_to_teams(notification)

    # Initializing Azure Table Service Endpoint
    credential = AzureNamedKeyCredential(os.environ.get('AZURE_STORAGE_ACCOUNT'),
                                         os.environ.get('AZURE_STORAGE_ACCOUNT_KEY'))
    service = TableServiceClient(endpoint=os.environ.get('AZURE_STORAGE_ACCOUNT_TABLE_SERVICE_URL'), credential=credential)
    table_client = service.get_table_client(table_name=os.environ.get('AZURE_STORAGE_ACCOUNT_TABLE'))

    count = 0
    for index, row in df.iterrows():

        # Convert ProScan Timestamp and Format to UTC Zulu format and 12 Hour AM/PM
        local = pytz.timezone("America/Los_Angeles")
        local_formatted = datetime.strptime(row['Start Date / Time'], "%m/%d/%y %H:%M:%S")
        twelve_hour_dt = local_formatted.strftime("%m/%d/%y %I:%M:%S %p")
        local_dt = local.localize(local_formatted, is_dst=None)
        utc_dt = local_dt.astimezone(pytz.utc)
        time_utc_zformat = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Convert ProScan Duration from HOURS:MINUTES:SECONDS to MINUTES
        h, m, s = str(row['Duration']).split(':')
        duration = float((int(h) * 60) + int(m) + (int(s) / 60))

        # Convert remove '-' from STR types
        if isinstance(row['Talk Group'], str):
            talk_group = row['Talk Group'].replace('-', '')
        else:
            talk_group = row['Talk Group']

        # Create Radio Transmission entity and write to Azure Storage Table
        # Used explicit casts for each data type, some are redundant and could be excluded
        transmission = {
            'PartitionKey': 'transmission',
            'RowKey': time_utc_zformat,
            'Date_Time': twelve_hour_dt,
            'Talk_Group': int(talk_group),
            'Tone': str(row['Tone']),
            'Mod': str(row['Mod']),
            'System_Site': str(row['System / Site']),
            'Department': str(row['Department']),
            'Channel': str(row['Channel']),
            'System_Type': str(row['System Type']),
            'Digital_Status': str(row['Digital Status']),
            'Service_Type': str(row['Service Type']),
            'Number_Tune': str(row['Number Tune']),
            'Freq': float(str(row['Frequency']).replace('/', '')),
            'UID': int(row['UID']),
            'HITs': int(row['Hits']),
            'Duration': float(duration),
            'RSSI': float(row['RSSI'])
        }
        try:
            # upsert_entity(...) Not Implemented for Azure Storage Tables
            entity = table_client.create_entity(entity=transmission)
            count += 1
            # time.sleep(1)  # separate points by 1 second
        except ResourceExistsError:
            continue

    notification = 'Radio Transmissions to Azure Storage Table: ' + str(count) + " (" + str(datetime.now()) + ")"
    print(notification)
    if teams_webhook != '<TEAMS WEBHOOK>':
        notification_to_teams(notification)


def ship_to_azure_cosmosdb(df):
    # ETL for ProScan (SDS200) 'History Log.csv'

    notification = 'CSV Radio Transmissions Imported --> ship_to_azure_cosmosdb(): ' + str(df.shape[0]) + " (" + str(
        datetime.now()) + ")"
    print(notification)
    if teams_webhook != '<TEAMS WEBHOOK>':
        notification_to_teams(notification)

    # Initializing Azure CosmosDB Table Service Endpoint
    service = TableServiceClient.from_connection_string(os.environ.get('AZURE_COSMOSDB_CONNECTION_STR'))
    table_client = service.get_table_client(table_name=os.environ.get('AZURE_COSMOSDB_TABLE'))

    count = 0
    for index, row in df.iterrows():

        # Convert ProScan Timestamp and Format to UTC Zulu format and 12 Hour AM/PM
        local = pytz.timezone("America/Los_Angeles")
        local_formatted = datetime.strptime(row['Start Date / Time'], "%m/%d/%y %H:%M:%S")
        twelve_hour_dt = local_formatted.strftime("%m/%d/%y %I:%M:%S %p")
        local_dt = local.localize(local_formatted, is_dst=None)
        utc_dt = local_dt.astimezone(pytz.utc)
        time_utc_zformat = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Convert ProScan Duration from HOURS:MINUTES:SECONDS to MINUTES
        h, m, s = str(row['Duration']).split(':')
        duration = float((int(h) * 60) + int(m) + (int(s) / 60))

        # Convert remove '-' from STR types
        if isinstance(row['Talk Group'], str):
            talk_group = row['Talk Group'].replace('-', '')
        else:
            talk_group = row['Talk Group']

        # Create Radio Transmission entity and write to CosmosDB Table
        # Used explicit casts for each data type, some are redundant and could be excluded
        transmission = {
            'PartitionKey': 'transmission',
            'RowKey': time_utc_zformat,
            'Date_Time': twelve_hour_dt,
            'Talk_Group': int(talk_group),
            'Tone': str(row['Tone']),
            'Mod': str(row['Mod']),
            'System_Site': str(row['System / Site']),
            'Department': str(row['Department']),
            'Channel': str(row['Channel']),
            'System_Type': str(row['System Type']),
            'Digital_Status': str(row['Digital Status']),
            'Service_Type': str(row['Service Type']),
            'Number_Tune': str(row['Number Tune']),
            'Freq': float(str(row['Frequency']).replace('/', '')),
            'UID': int(row['UID']),
            'HITs': int(row['Hits']),
            'Duration': float(duration),
            'RSSI': float(row['RSSI'])
        }
        entity = table_client.upsert_entity(mode=UpdateMode.REPLACE, entity=transmission)
        count += 1
        # time.sleep(1)  # separate points by 1 second

    notification = 'Radio Transmissions to CosmosDB Table: ' + str(count) + " (" + str(datetime.now()) + ")"
    print(notification)
    if teams_webhook != '<TEAMS WEBHOOK>':
        notification_to_teams(notification)


def notification_to_teams(message):
    # Send message to Teams channel
    # Used for when no routing information is returned or an exception occurs
    # TODO: Add additional formatting such as tittle and sections
    teams_message = pymsteams.connectorcard(teams_webhook)
    teams_message.text(message)
    teams_message.send()


if __name__ == '__main__':
    main(sys.argv)
