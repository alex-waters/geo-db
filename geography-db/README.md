# README #

This repo holds the necessary files to create a database of geographic data that the team can use.

## osm2pgsql
We use this command line app to import data from a .pbf file into a PostGIS database.
Much of the database design is set within the app itself but we have our style file, called `ds-team.style` which defines the data to be imported.

See here for more info on style files : https://wiki.openstreetmap.org/wiki/Osm2pgsql#Import_style

And see here for info on the osm2pgsql database design : https://github.com/openstreetmap/osm2pgsql/blob/master/docs/pgsql.md#database-layout

For information on Tags and Keys in OSM data, see the `ds-team.style` file and:
https://taginfo.openstreetmap.org

There are various valid ways of setting up an OSM database. The steps to create the one for this repo are in `osm_tiger_setup/` and are:
 - create a Postgres database
 - install the extensions in `extensions_db.sql`
 - run `osm_import.sh` or look at instructions to use Databricks below
 - continue below

### roadnames

osm2pgsql creates a large table called `raw_usa_line` which contains all roads and other line geometries. Querying this table can take 
some time so after completing the osm2pgsql build, run `roadnames_build.sql` to create a faster table.

## Tiger house numbers
OSM data is not particularly rich when it comes to house numbers for the USA. To overcome this problem, we collect house numbers from the US Census Bureau.
This data is called Tiger.

We could use [Nominatim](https://github.com/openstreetmap/Nominatim/tree/master/data-sources/us-tiger) to process the source data but the easiest way to get the data ready for PostGIS is to take a preprocessed file from OSM.

On a Linux machine you can run
```
wget https://nominatim.org/data/tiger2019-nominatim-preprocessed.tar.gz
tar xf tiger2019-nominatim-preprocessed.tar.gz
```

This will leave you with a directory of ~3300 .sql files. However, the SQL contained within uses a custom function called `tiger_line_import()` which needs to be created in the database.

Due to the complexity of importing the Nominatim copy of Tiger into a PostGIS database, we have a backup file of a working Tiger import database called `tiger_import_bkp.sql`.

#### Tiger import database
The OSM product Nominatim is very good for handling Tiger data. We could take the whole Nominatim app and use that however this 
has thousands of lines of PHP and wide ranging functionality that we do not need. Therefore we have implemented some of the functionality
of the backend database and then taken a backup of it (excluding any data) using `reference/dump_tigerdb.sh`.

To import the Tiger data into that database, the database needs to be restored with `restore_tigerdb.sh` and then all the Tiger .sql files can be executed by it. An easy way to do this
concurrently is to have a Spark cluster open multiple connections from each CPU core. The file `tiger_data_write.py` will do this for you.

The output should be a database table called `location_property_tiger` which can then be moved to any other database.

## Running OSM and Tiger setup from a Databricks driver
It is possible to run through the process of collecting OSM and Tiger data followed by importing them into databases using a Databricks cluster driver.
As of March 2020, these drivers are EC2 machines running Ubuntu 16.04

In the cluster config, it is sensible to set the worker nodes to zero so that you don't run redundant instances.

There is a full notebook of commands at `databricks_driver/osm_tiger_build.ipynb`. The commands are extremely similar to what is in this repo, they've just been tweaked for Databricks.

## Geocoding

Once we have our OSM and Tiger data ready for use, we can query the data for addresses.
Following the approach of Nominatim, there are two separate queries; one for a road name and the other for a house number.

The source of the latlong locations to use for address finding isn't the goal of this repo, they can come from wherever is best 
for a particular use. The first running of address finding using geography-db had a data source explained in `geocoding_queries/home_geohashes_create.sql`


## USPS

To get an accurate zip4 code for addresses, we use the USPS address database. Using `usps_addresses/` the steps to set the data up are:
 - get the USPS zipped file
 - ETL the files with `zip4_build.py`
 - import the two csv files into the database
 - run `full_street_name.sql` in the database
 - search for the addresses returned by `geocoding_queries/` with `join_osm_tiger.py`



# Resource management

Some of the processes executed by this repo are resource hungry. This section details the cases where you need to look out for this.

#### First OSM build
It takes time for PostGIS to process all the data in a large osm.pbf file. The osm2pgsql app will consume resources on *both* the machine it is run on 
as well as the remote database. The use of an 8 core CPU and 61GB of RAM was found to be sufficient.

#### Geocoding 
If you are sending millions of locations to the database in a single run, resource consumption by the database will be high. However do note that it's the database and
not Spark doing the hard work so don't worry about Spark cluster resource. If anything, constrain your cluster CPU count so that it doesn't overload the database.

If you cause Spark to open too many connections to the database, the queries will queue up and the database may never recover itself from a point of total resource consumption.

The use of a 60 core RDS Postgres 11 instance with a Spark cluster of nine machines, each with eight CPU cores; was found to be sufficient.