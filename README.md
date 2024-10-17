# ProScan Radio Transmission History Log Shipper #

ProScan Radio History Log Shipper: ETL to InfluxDB/Azure Storage Table/CosmosDB Table

### Summary: ###

* Version 2
* Reads the specified or default history .csv file, converts and formats data before shipping to InfluxDB Bucket or Azure Storage Table or CosmosDB Table.
* Sends Microsoft 365 Teams notifications pre and post ETL (disabled by default).

A .env file is used for all of the InfluxDB and Azure secrets, you can create your own .env matching the preexisting
os.environ.get(...) calls.

Currently, I am evaluating the performance of InfluxDB Vs. Azure Storage Table Vs. CosmosDB Table and both ship_to_influxdb() + ship_to_azure_cosmosdb() are enabled in main() (you can choose one the shipping function based on your use case).

* ship_to_azure_cosmosdb() offers the best performance and flexibility for the given data set.
* ship_to_influxdb() is enabled for side-by-side comparisons.

### InfluxDB Point/Measurement Details: ###

InfluxDB differs from other databases and introduces Tags/Fields... You can think of Tags as indexed metadata and
fields are the values associated with your "measurement", in my use case the measurements are radio transmissions and
each transmission has the following tags/fields associated with it:

* Tags
    * Talk Group
    * Tone
    * Mod (Modulation)
    * System Site
    * Department
    * Channel
    * System Type
    * Digital Status
    * Service Type
    * Number Tune
* Fields
    * Freq (Frequency)
    * UID
    * HITs
    * Duration
    * RSSI

### Additional Modules ###

* InfluxDB Client
* FlightSQL-DBAPI
* Azure-Data-Tables
* PyMSTeams

### Run: ###

When no filename is provided the default "History Log Small - Debug.csv" is used.

* ./ProScanHistoryLog_to_Storage.py [FILENAME]
* ./ProScanHistoryLog_to_Storage.py
* python3 ./ProScanHistoryLog_to_Storage.py