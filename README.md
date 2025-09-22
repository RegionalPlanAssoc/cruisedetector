# cruisedetector
## Introduction
This manual describes how to use the Cruise Detector, a GPS cruising identification model developed by the Federal Highway Administration.  Any user with a working knowledge of GIS and simple database skills should be able to implement the system with the aid of information presented here.

This tool uses GPS data to estimate the proportion of trips that are cruising for parking.

## Software Requirements
A combination of Python and PostgreSQL. In Cruise Detector, Python is used only to pass commands to PostgreSQL, which handles the bulk of the geoprocessing.

### Python
It is recommended to create a new Python environment through Anaconda to run Cruise Detector. Ultimately, the Python environment must have the following packages installed:
            
You can use Anaconda Navigator to manage your Python environments. 

In Anaconda Prompt, you can solve for an environment with Cruise Detector’s required dependencies:

```
conda create -n cruise_env -c conda-forge python=3.8 scipy=1.8 numpy pandas gpxpy psycopg2 sqlalchemy docopt
```

* `-n cruise_env` names the environment “cruise_env”.
* `-c conda-forge` specifies to pull the packages from the conda-forge channel 
* `python=3.8` and `scipy=1.8` specify for Anaconda to solve for a new environment with Python 3.8 and Scipy 1.8.
* `pandas gpxpy psycopg2 sqlalchemy docopt` installs the other required packages with versions that follow based on the conditions specified.

### PostgreSQL

#### Java
The file path to Java should be added to your operating system’s PATH environmental variable list to be found by its dependents.
1. `C:\Program Files (x86)\Common Files\Oracle\Java\javapath`
2. `C:\Program Files (x86)\Common Files\Oracle\Java\java8path`

After installation or changing your environmental variable, you will most likely need to restart your machine before the changes propagate.

#### PostgreSQL
If you use the installer, the other dependencies (pgAdmin4, PostGIS, and pgRouting can be installed with PostgreSQL. 
On the ‘Select Components’ window, check both pgAdmin 4 and StackBuilder.
Launch StackBuilder after you finish installing PostgreSQL
When prompted to ‘Please select the applications you would like to install’ under ‘Spatial Extensions’, check ‘PostGIS … Bundle for PostgreSQL …’ to install both PostGIS and pgRouting.

You will be prompted to create a password - make sure you remember this password in order to access your databases!

After installing Postgres, you should add the filepath to your system’s PATH environmental variable list, as you did with Java. \
`C:\Program Files\PostgreSQL\17\bin`

##### Setting up a Database
###### During Installation
The installer will prompt you to create a new database, which by default will be named something like `postgis_35_sample`. If you are creating this for the first time, you may name this database `cruisedb` or a similar identifiable name.

###### After Installation
In pgadmin4, you can see you may already have database ‘postgres’ and ‘postgis_35_sample’ based on the version of postgis. Right-click ‘Databases’ and click ‘Create’ > ‘Database…’. To the right of ‘Database’, name this database ‘cruisedb’ or a similar identifiable name.

#### Using pgAdmin4
###### Query Tool
Some lines of pSQL are offered in this readme to troubleshoot errors. These can be run using the ‘Query Tool’ within pgAdmin4. Right-click on the database you are using for cruise detector (i.e. `cruisedb`) and click on the ‘Query Tool’ toward the bottom of the list.

###### Create Extensions in your database
To get started, create extensions for Postgis and Pgrouting by running the following line using the Query Tool within your project's PostgreSQL database:
```
CREATE EXTENSION postgis;
CREATE EXTENSION pgrouting;
```

##### Configuring pga_hba.conf
By default on Windows, `pga_hba.conf` can be found at:
`"C:\Program Files\PostgreSQL\17\data\pg_hba.conf"`

In the Query Tool in pgAdmin4, you can use the command to find the `pg_hba.conf` configuration file.  
```
SHOW hba_file;
```

###### Set Authentication Requirements to Trust

After setting up the postgres database, removing the password requirement allows the tool to run smoother.
Toward the bottom of this file, you may change all authentication requirements in this `pg_hba.conf` file to `trust`.

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# "local" is for Unix domain socket connections only
local   all             all                                     trust
# IPv4 local connections:
host    all             all             127.0.0.1/32            trust
# IPv6 local connections:
host    all             all             ::1/128                 trust
# Allow replication connections from localhost, by a user with the
# replication privilege.
local   replication     all                                     trust
host    replication     all             127.0.0.1/32            trust
host    replication     all             ::1/128                 trust
```

##### Configuring postgresql.conf
###### Raise the Memory Limit
By default on Windows, `postgresql.conf` can be found at: \
`C:/Program Files/PostgreSQL/17/data/postgresql.conf.`

In the Query Tool pgAdmin4, you can use the command to find the configuration 
```
SHOW config_file;
```

In this configuration file increase the memory limit by uncommenting and increasing the `shared_buffers`, `temp buffers`, `work_mem`, and `maintenance_work_mem`. 

```
# - Memory -

shared_buffers = 500MB			# min 128kB [Default: 128MB, Adam: 500MB] # (change requires restart)
#huge_pages = try			# on, off, or try # (change requires restart)
#huge_page_size = 0			# zero for system default # (change requires restart)
temp_buffers = 64MB			# min 800kB [Default: 8MB, Adam: 64MB]
#max_prepared_transactions = 0		# zero disables the feature # (change requires restart)
# Caution: it is not advisable to set max_prepared_transactions nonzero unless
# you actively intend to use prepared transactions.
work_mem = 100MB			# min 64kB [Default: 4MB, Adam: 100MB]
#hash_mem_multiplier = 2.0		# 1-1000.0 multiplier on hash table work_mem
maintenance_work_mem = 100MB		# min 64kB [Default: 64MB, Adam: 100MB]
```

Under checkpoints, increase `max_wal_size` and `min_wal_size`.
```
# - Checkpoints -

#checkpoint_timeout = 5min		# range 30s-1d
#checkpoint_completion_target = 0.9	# checkpoint target duration, 0.0 - 1.0
#checkpoint_flush_after = 0		# measured in pages, 0 disables
#checkpoint_warning = 30s		# 0 disables
max_wal_size = 2GB
min_wal_size = 80MB
```

If you get bad memory allocation errors such as `GEOSBuffer: std::bad_alloc`, make sure that the memory limits are set at or above the aforementioned limits or higher. 

### osm2po 
Use the [osm2po](http://osm2po.de/) tool to import the OpenStreetMap data into the database. Make a couple of changes to the osm2po config file to accurately reflect [turn restrictions](http://gis.stackexchange.com/questions/41393/does-osm2po-take-into-consideration-turn-restrictions) and one-way streets by un-commenting the following lines:

1. `postp.0.class = de.cm.osm2po.plugins.postp.PgRoutingWriter`
2. `postp.1.class = de.cm.osm2po.plugins.postp.PgVertexWriter `
3. `graph.build.excludeWrongWays = true`

To allow service streets to be included in the analysis, uncomment the following line:

`#wtr.tag.highway.service =        1,  51, 5,   car|bike`


### cruising. 
Configuration parameters are located in the cruising.py file. Open cruising.py and set the parameters for host, file paths, regions, spatial reference systems, and number of CPU cores used for processing. The config file also contains multiple parameters to calibrate trace generation from GPS data and identify cruising.
pgMapMatch. Open config_template.py and make changes to the pgInfo parameter for your postgres database connection. If you’ve removed the password, make sure requirePassword is set to False. Save the file as config.py.

### pgMapMatch
Clone the cruising and pgMapMatch repositories, and add a folder titled “output” to store logs.

## Environment
Create a base directory, and download the [cruising](https://github.com/RegionalPlanAssoc/cruisedetector) and [pgMapMatch](https://github.com/amillb/pgMapMatch) repositories to that directory, and download the [sample location data](https://drive.google.com/file/d/1R1Vu1DW4EewiQ7_Wezf4C62bZDhUzjfp/view?usp=sharing) to the cruising folder. Add a folder titled `output` to store logs, and unzip the sampleLocationData folder if using. Download osm2po to a directory with no spaces in its path.

## Data Requirements and Format
### Street Network
The street network should be in pbf format. An extract for specific geographic areas can be obtained from [geofabrik.de](https://www.geofabrik.de/). The extract should be saved to the osm2po base path, if that is different from the project base bath.
### Census Boundaries
Census tract or block group boundaries are used to aggregate results for analysis purposes after pgMapMatch has been run on the GPS trace data. Census geographies can be obtained in shapefile format from the US Census Bureau’s [TIGERweb](https://tigerweb.geo.census.gov/tigerweb/) database. 
### GPS Data
#### Location Data
Data formats may vary by vendor, but the raw GPS location data must be a table containing a minimum of **device ID, timestamp, latitude and longitude, and horizontal accuracy**. The `cruising_importLocationData.py` script is based on one specific vendor’s data and may require alteration to match the format and data structure of the location data obtained.

The following list is an incomplete list of location data vendors.  Inclusion in the list is not an endorsement.

- Vera set
- Quadrant
- Onemata
- Lifesight

#### Trip Data
Trip data that has been pre-processed into traces by the vendor can also be used. The imported PostgreSQL table must contain:
- **trip_id**: unique ID for each trace
- **lines_geom**: Linestring M geometry of the trace
- **start_geom**: Point M geometry of the start point
- **end_geom**: Point M geometry of the end point

Possible vendors for trip data include: 
- StreetLight
- AirSage
- INRIX
- TomTom

As with the location data, the list is not comprehensive, not an endorsement and not a guarantee the vendor will make usable data available.  A collaboration with these firms may be necessary to access their data and information.

## Load the Data
To get started, create extensions for Postgis and Pgrouting by running the following query in your project's PostgreSQL database:
```
CREATE EXTENSION postgis;
CREATE EXTENSION pgrouting;
```
Next, run the following in your Python IDE:
```
import sys
sys.path.append('[yourBasePath]/cruising') ## change this to your base path
sys.path.append('[yourBasePath]')
from cruising import *
from cruising_importLocationData import *
```
### Import Street Network
Run `loadTables(region='[yourRegion]')` with your region as specified in `cruising.py`, which will import the osm street network and turn restriction table into the database. The field names for the streets table should match those in the `pgMapMatch/config.py`. Make sure the SRS of the streets table matches the SRS you will be using for the location or trip data, and create indexes and spatial indexes have been created. Depending on the imported network, it may improve performance to clip the street network to a convex hull around the study area.

To run the sample data, you will need to download the [Washington State osm.pbf](https://download.geofabrik.de/north-america/us/washington.html) file to your osm2po path and run `loadTables(region='wa')`. The sample data is comprised of GPS point data, which will be used to generate traces, that can then be analyzed for cruising.
### Import Census Boundaries
Use PostGIS to import the census boundary files to your database, and reproject the data to the SRS you are using. Use a spatial join to add the tract or block group ID to the streets table.
### Import GPS Data
The `cruising_importLocationData.py` script is based on a specific data vendor and may require alteration to match the format and data structure of the location data obtained. 

To import the sample data, set the sample data directory and name for the imported location data table `points_table` and output trace table `output_table`. Run the following code to import the table:
```
points_table = 'samplepoints'
trace_table = 'sampletraces'
iT = importTable(points_table, 'sampleLocationData', schema = 'public', region = 'wa', forceUpdate=True)
iT.createTable()
iT.importCSV()
```
To generate traces from the sample data, run the following code:
```
pts = pointData(points_table, trace_table, schema = 'public', region = 'wa', forceUpdate=True)
pts.geocodePoints()
pts.processPoints()
pts.generateTraces()
pts.generateUniqueIDs()
```
### Map-matching from user-generated traces
Once the trace table is created, from either the sample dataset or the user’s data, it can be mapmatched by running the following code:
```
trace_table = "[yourTraceTable]" ##same as the trace_table in the previous section
tt = traceTable(trace_table, schema = '[yourSchema]', region = '[yourRegion]', forceUpdate=True) ## change the table, schema, etc. 
tt.runall()
```
This may take several hours, even with the sample data.
## Results and Interpretation
Once the trips have been processed the data output can be analyzed with a spreadsheet, python, or any statistical package and GIS. See the data dictionary here.
