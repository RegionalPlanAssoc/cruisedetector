# cruisedetector

## Introduction
This manual describes how to use the Cruise Detector, a GPS cruising identification model developed by the Federal Highway Administration.  Any user with a working knowledge of GIS and simple database skills should be able to implement the system with the aid of information presented here.

This tool uses GPS data to estimate the proportion of trips that are cruising for parking.

#### FHWA Final Report
Rachel Weinberger, Adam Millard-Ball, Tayo Fabusuyi, Ellis Calvin,
Jazymyn Blackburn, Michelle Neuner. "Parking Cruising Analysis Methodology: Final Project Report," Report No. FHWA-HOP-23-004. U.S. Department of Transportation, Federal Highway Administration. March 2023. <[https://ops-dr.fhwa.dot.gov/publications/fhwahop23004](https://ops-dr.fhwa.dot.gov/publications/fhwahop23004)>

#### Further Background on Methodology
Rachel R. Weinberger, Adam Millard-Ball, Robert C. Hampshire. "Parking search caused congestion: Where’s all the fuss?" Transportation Research Part C: Emerging Technologies, Volume 120, 2020, 102781, ISSN 0968-090X, https://doi.org/10.1016/j.trc.2020.102781. (https://www.sciencedirect.com/science/article/pii/S0968090X20306914)


## Hardware Requirements

### Memory

Cruise Detector involves processes that are memory-intensive. At least 32GB of RAM is recommended. Processes will take much longer with only 16GB of RAM. The sample location data around 18 hours to run one a machine with 32GB of RAM and over 80 hours to run on a machine with 16GB of RAM.

### Disk Space

PostgreSQL uses substantial disk space to complete operations. Have at least 25GB of free storage space on the drive PostgreSQL is installed on before running the sample location data or similarly sized files. 

## Software Requirements
A combination of Python and PostgreSQL is used in Cruise Detector. Python is used primarily to pass commands to PostgreSQL, which handles the bulk of the actual geoprocessing.

You may need administrator privileges to install and run Cruise Detector and many of the software components below. When opening any of software below such as Anaconda Prompt or pgAdmin, be sure to right-click and "Run as administrator" if you do not have these permissions enabled by default on your machine.

### Python Libraries
It is recommended to create a new Python environment through Anaconda to run Cruise Detector using the line below this tables. Cruise Detector make use of various Python libraries and may lost compatible with future versions. If there are any issues with deprecated functions or missing arguments, revert to the "Recommended Version." Ultimately, the Python environment must have the following packages installed:
| # | Requirement |  Minimum Version | Recommended Version | What does it do? | Compatibility Notes |
| --- | --------- | ---------------- | ------------------- | ---------------- | ------------------- |
| 1 | numpy | 1.11.3+ | 1.24.4 | Provides tools for numerical and array computing. |
| 1a | scipy | 0.19.0+ | 1.8.1 | Extends numpy with various algorithms for scientific computing and statistics. | In scipy > 1.8, the [`dok_matrix._update()` direct update method](https://github.com/scipy/scipy/issues/8338) is deprecated. As of August 2025, pgMapMatch has been [fixed](https://github.com/amillb/pgMapMatch/issues/30) to not rely on this method.
| 1b | pandas | 0.19.2+ | 2.0.3 | Allows data manipulation, analysis, and cleaning based on dataframes, a tabular data structure based on numpy's numerical arrays. | In pandas > 1.5, [iteritems() is removed](https://github.com/pandas-dev/pandas/pull/45321).  As of September 2025, pgMapMatch has been [fixed](https://github.com/amillb/pgMapMatch/pull/34) to replace `iteritems()` with its duplicate `items()`
| 2 | gpxpy | 1.1.2+ | 1.6.2 | Allows parsing and manipulating of GPX files, an XML-based format for GPS tracks. |
| 3 | psycopg2 | 2.5.2+ | 2.9.9 | Adapts PostgreSQL databases to allow Python scripts to connect to and interact with them. |
| 4 | sqlalchemy | 1.1.6+ | 2.0.32 |  Python SQL toolkit and Object Relational Mapper that facilitates use of SQL in Python. |
| 5 | docopt | 0.6.1+ | 0.6.2 | Creates the command-line interface for pgMapMatch and cruising. |

You can use [Anaconda Navigator](https://www.anaconda.com/products/navigator) to manage your Python environments. By running the following line in Anaconda Prompt, you can solve for an environment with all of Cruise Detector’s required dependencies:

```
conda create -n cruise_env -c conda-forge python=3.8 scipy=1.8 numpy pandas gpxpy psycopg2 sqlalchemy docopt
```

* `-n cruise_env` names the environment “cruise_env”.
* `-c conda-forge` specifies to pull the packages from the conda-forge channel 
* `python=3.8` and `scipy=1.8` specify for Anaconda to solve for a new environment with Python 3.8 and Scipy 1.8.
* `pandas gpxpy psycopg2 sqlalchemy docopt` installs the other required packages with versions that follow based on the conditions specified.

To install or upgrade individual packages, you may use `conda install` followed by the package name in Anaconda prompt as usual. Alternatively, `pip install` provides more flexibility but can potentially install libraries in an incorrect location.

### PostgreSQL

| # | Requirement | Minimum Version | What is it? | What does it do? |
| --- | --------- | --------------- | ----------- | ---------------- |
| 1 | [Java](https://www.java.com/) | [8+](https://www.java.com/en/download/) | Programming language and computing platform. | Necessary to `assert` any commands to PostgreSQL and to use the osm2po tool. |
| 2 | [PostgreSQL](https://www.postgresql.org/) | [13+](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads) | An object-relational database system that extends SQL language. | Handles most of the geoprocessing with pSQL command passed from Python.  |
| *2a** | [PostGIS](https://postgis.net/) | [3.2+](https://postgis.net/documentation/getting_started/install_windows/released_versions/) | Spatial database extension that "spatially enables" PostgreSQL databases. | Provides support for geospatial data in PostgreSQL. |
| *2b** | [pgRouting](https://github.com/pgRouting/pgrouting) | [3.3.0+](https://github.com/pgRouting/pgrouting/wiki/Notes-on-Download,-Installation-and-building-pgRouting) | Plug-in for PostgreSQL. | Extends PostGIS/PostgreSQL geodatabases to provide geospatial routing and network analysis functionality for osm2po to create and pgMapMatch to use routed street networks. |
| *2c** | [pgAdmin](https://www.pgadmin.org/) | [4](https://www.pgadmin.org/download/pgadmin-4-windows/) | An adminstrative interface for PostgreSQL. | Allows you to reference the outputs and operations from PostgreSQL and to run short commands using the Query Tool. |

**PostGIS, pgRouting, and pgAdmin can be installed more cleanly within the PostgreSQL installer. See instructions below.*

#### Java
You must have Java installed to pass commands to PostgreSQL with `assert` to perform most of the geoprocessing required for this script and for osm2po convert street network into a routable format. Although you may be able to install PostGreSQL without Java, you will get a blank `AssertionError:` with no other information for any `assert` command in the script if Java is not installed, since there is nowhere for Python to pass the command.

The version of Java to install will depend on your version of PostgreSQL. For [PostgreSQL 13+](https://www.postgresql.org/), you will most likely need [Java 8+](https://www.java.com/en/download/) or higher.

After installation, the file path to Java should be added to your operating system’s PATH environmental variable list to be found by any of its dependents.
 
1. `C:\Program Files (x86)\Common Files\Oracle\Java\javapath`
2. `C:\Program Files (x86)\Common Files\Oracle\Java\java8path`

To edit your system's environment variable list in Windows:
1. Go to your start menu, search for "System Environmental Variables" and look for "Edit the system Environment Variables".
2. In the *System Properties* dialogue that pops up, click the "Environment Variables..." button in the lower right corner.
3. In the *Environment Variables* that pops up, in the lower portion titled *System Variables*, click on `Path` to highlight the row and then click the "Edit..." button in the lower right corner.
4. Click the "New" button toward the top-right and add both the filepaths above.

After adding the path to Java to your environmental variables, you will most likely need to restart your machine before the changes will propagate.

#### PostgreSQL
If you use the installer, several of the other dependencies (**pgAdmin4**, **PostGIS**, and **pgRouting**) can be installed with PostgreSQL. 
1. On the *‘Select Components’* window, check both **pgAdmin 4** and **StackBuilder**.
2. Launch **StackBuilder** after you finish installing PostgreSQL.
3. When prompted to "Please select the applications you would like to install", under *‘Spatial Extensions’*, check *‘PostGIS … Bundle for PostgreSQL …’* to install both **PostGIS** and **pgRouting**.

During the installation process, you will be prompted to create a password - make sure you remember this password in order to access your databases!

After installing Postgres, you should add the filepath to your system’s PATH environmental variable list, as you did with Java. \
`C:\Program Files\PostgreSQL\17\bin`

##### Setting up a Database
###### During Installation
The installer will prompt you to create a new database, which by default will be named something like `postgis_35_sample`. If you are creating this for the first time, you may name this database `cruisedb` or a similar identifiable name.

###### After Installation
In pgadmin4, you can see you may already have database `postgres` and `postgis_35_sample` based on the version of postgis. Right-click *‘Databases’* and click *‘Create’* > *‘Database…’*. To the right of ‘Database’, name this database `cruisedb` or a similar identifiable name.

##### Using pgAdmin4
###### Query Tool
Some lines of pSQL are provided in this readme to install extensions, find installation files, or troubleshoot errors. These can be run using the ‘Query Tool’ within pgAdmin4. Right-click on the database you are using for cruise detector (i.e. `cruisedb`) and click on the ‘Query Tool’ toward the bottom of the list.

###### Create Extensions in your database
To get started, create extensions for Postgis and Pgrouting by running the following line using the Query Tool within your project's PostgreSQL database:
```
CREATE EXTENSION postgis;
CREATE EXTENSION pgrouting;
```

##### Configuring `pga_hba.conf`
By default on Windows, `pga_hba.conf` can be found at:
`"C:\Program Files\PostgreSQL\17\data\pg_hba.conf"`

In the Query Tool in pgAdmin4, you can use the command to find the `pg_hba.conf` configuration file.  
```
SHOW hba_file;
```
To edit the `postgresql.conf` configuration file in the following steps, you may right-click this file and open this file in any text editor, such as Notepad, Notepad++, Atom, or VSCode.

###### Set Authentication Requirements to Trust
After setting up the postgres database, removing the password requirement allows the tool to run smoother.
Toward the bottom of this `pg_hba.conf` configuration file, you may change all authentication requirements to `trust`.

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

##### Configuring `postgresql.conf`
By default on Windows, `postgresql.conf` can be found at: \
`C:/Program Files/PostgreSQL/17/data/postgresql.conf.`

In the Query Tool in pgAdmin4, you can use the command to find the  `postgresql.conf` configuration file.
```
SHOW config_file;
```
To edit the `postgresql.conf` configuration file in the following steps, you may right-click this file and open this file in any text editor, such as Notepad, Notepad++, Atom, or VSCode.

###### Raise the Memory Limits
In this `postgresql.conf` configuration file, under the heading `# - Memory -`, increase the memory limit by uncommenting and increasing the `shared_buffers`, `temp buffers`, `work_mem`, and `maintenance_work_mem`. 
```
# - Memory -

shared_buffers = 128MB              # min 128kB [Default: 128MB, Increase: 500MB]
                                    # (change requires restart)
#huge_pages = try                   # on, off, or try
                                    # (change requires restart)
#huge_page_size = 0                 # zero for system default
                                    # (change requires restart)
temp_buffers = 8MB                  # min 800kB [Default: 8MB, Increase: 64MB]
#max_prepared_transactions = 0      # zero disables the feature
                                    # (change requires restart)
# Caution: it is not advisable to set max_prepared_transactions nonzero unless
# you actively intend to use prepared transactions.
work_mem = 100MB                    # min 64kB [Default: 4MB, Increase: 100MB]
#hash_mem_multiplier = 2.0          # 1-1000.0 multiplier on hash table work_mem
maintenance_work_mem = 100MB        # min 64kB [Default: 64MB, Increase: 100MB]
```
| Default | Increase |
| ------- | -------- |
| `shared_buffers = 128MB` | `shared_buffers = 500MB` |
| `temp_buffers = 8MB` | `temp_buffers = 64MB` |
| `work_mem = 4MB` | `work_mem = 100MB` |
| `maintenance_work_mem = 64MB` | `maintenance_work_mem = 100MB` |

###### Raise the WAL Segment Size of Checkpoints
Further down the `postgresql.conf` configuration file, under the heading `# - Checkpoints -`, increase `max_wal_size` and `min_wal_size` if necessary:
```
# - Checkpoints -

#checkpoint_timeout = 5min		# range 30s-1d
#checkpoint_completion_target = 0.9	# checkpoint target duration, 0.0 - 1.0
#checkpoint_flush_after = 0		# measured in pages, 0 disables
#checkpoint_warning = 30s		# 0 disables
max_wal_size = 2GB                              # [Default: 1GB, Increase: 2GB]
min_wal_size = 80MB                             # [Default: 80MB] 
```
| Default | Increase |
| ------- | -------- |
| `max_wal_size = 1GB` | `max_wal_size = 2GB` |
| `min_wal_size = 80MB` | `min_wal_size = 80MB` |

If you get bad allocation errors related to memory such as `GEOSBuffer: std::bad_alloc`, first make sure that the memory and checkpoints variables are uncommented and set at or above the aforementioned limits or higher. 

### Cruise Detector Base Directory 

Make a new folder that will serve as a base directory for the following components, named something identifiable such as `C:\cruisebase\`. 
* It is recommended that all filepaths involved should have no spaces in the path (i.e., do NOT use `G:\My Drive`) to guarantee the filepath is not misread by PostgreSQL.
* For the same reason, you may wish have the base directory as close to your drive letter (i.e. `C:\`) as possible.

You will ultimately download [osm2po](http://osm2po.de/), [cruising](https://github.com/RegionalPlanAssoc/cruisedetector), and [pgMapMatch](https://github.com/amillb/pgMapMatch) repositories as well as the input data to that directory. 

| # | Requirement | Minimum Version | What is it? | What does it do? |
| --- | --------- | --------------- | ----------- | ---------------- |
| 1 | [osm2po](http://osm2po.de/) | [5.5+](https://osm2po.de/releases/) | Converter and routing engine. | Parses street networks from OpenStreetMap XML data and makes them into routable topology and graph files. |
| 2 | [pgMapMatch](https://github.com/amillb/pgMapMatch) | *Latest* | Python script. | Matches GPS traces to routes along a street network. |
| 3 | [cruisedetector](https://github.com/RegionalPlanAssoc/cruisedetector) | *Latest* | Python script. | Detects and analyzes matched GPS traces for cruising-for-parking behavior. |

You may use [GitHub Desktop](https://desktop.github.com/download/) to download cruising and pgMapMatch with version control. More instructions for each of these respositories below.

You will add the following data to this base directory (i.e. `C:\cruisebase\`) as well:
| # | Data | Note |
| --- | --- | --- |
| 4 | Street Network | Download the **street network data** as an `.osm.pbf` (Open Street Maps Protocolbuffer Binary Format) file from [geofabrik.de](https://www.geofabrik.de/). The sample location data is located within Seattle and thus corresponds to [Washington State osm.pbf](https://download.geofabrik.de/north-america/us/washington.html) street network. More information is in the next section, 'Data Requirements'.
| 5 | GPS Traces | Download the **GPS traces** as a `.gz` compressed CSV. You may use [sample location data](https://drive.google.com/file/d/1R1Vu1DW4EewiQ7_Wezf4C62bZDhUzjfp/view?usp=sharing) from Quadrant to the cruising folder. You will need to extract the outermost `.zip` archive using 7-Zip or "Extract all..." in Windows, but you do NOT need to unzip the .gz files within this file. More information is in the next section, 'Data Requirements'.
| 6 | Output Folder | An empty folder titled `output` to store logs.

#### osm2po 
Download [osm2po](http://osm2po.de/) to the folder. 
* This will be downloaded as a .zip archive, which you must extract using 7-Zip, "Extract all..." in Windows, or another decompressor.
* You will have to use the path to the osm2po folder (i.e. `C:\cruisebase\osm2po-5.5.16`) to configure `cruising.py`, so as a reminder, it is recommended that the final filepath to access the executable file (i.e. `C:\cruisebase\osm2po-5.5.16\osm2po-core-5.5.16-signed.jar`) should have no spaces.

##### Configuring `osm2po.config`

You can find `osm2po.config` within the unzipped osm2po folder (i.e. `C:\cruisebase\osm2po-5.5.16`).

Make a couple of changes to the `osm2po.config` configuration file to accurately reflect [turn restrictions](http://gis.stackexchange.com/questions/41393/does-osm2po-take-into-consideration-turn-restrictions) and one-way streets by un-commenting the following lines:

1. `postp.0.class = de.cm.osm2po.plugins.postp.PgRoutingWriter`
2. `postp.1.class = de.cm.osm2po.plugins.postp.PgVertexWriter `
3. `graph.build.excludeWrongWays = true`

To allow service streets to be included in the routing of the network, also uncomment the following line:

4. `#wtr.tag.highway.service =        1,  51, 5,   car|bike`

#### pgMapMatch
Download or clone the [pgMapMatch](https://github.com/amillb/pgMapMatch) repository to the base directory.

To clone the pgMapMatch repository:
1. After installing GitHub Desktop, got to 'File' > 'Clone repository' or use the shortcut CTRL+Shift+O.
2. Go to the right-most tab named 'URL' to install using the URL.
3. For the first field, "Repository URL or GitHub username and repository", paste in `https://github.com/amillb/pgMapMatch` or `amillb/pgMapMatch`.
4. For the second field, "Local path", use the filepath to the base directory followed by `pgMapMatch`, which GitHub Desktop should add by default (i.e. `C:\cruisebase\pgMapMatch`).
5. You should have a the `pgMapMatch` folder when you access the base directory using File Explorer. To update at a later time, in GitHub Desktop, click 'Fetch Origin' near the top right and 'Pull'. 

Alternatively, you may download to the base directory without using GitHub Desktop by going to the [pgMapMatch](https://github.com/amillb/pgMapMatch) repository in browser, clicking the green 'Code' button, and 'Download ZIP' to the base directory. After unzipping the repository, be sure to rename the folder to `pgMapMatch` from `pgMapMatch-main` or any other name.

##### Configuring `config.py`
In the pgMapMatch folder (i.e. `C:\cruisebase\pgMapMatch\`):
1. Copy `config_template.py`.
2. Rename the copy `config.py`.
3. Open `config.py` in a text editor of your choice.

###### Changes to `pgInfo`
Make and save changes to the `pgInfo` dictionary to allow connection to your Postgres database connection. The five values for these keys will vary based your PostgreSQL installation:
1. **db**. The name you chose for PostGreSQL database you created for Cruise Detector, i.e. `cruisedb`
2. **schema**. In pgAdmin, under the dropdown for your chosen PostGreSQL database, there should be another dropdown 'Schemas' with three red diamonds as an icon. You output tables will be stored under `Tables` under the schema you specify here. You can use the default schema, `public` here. 
3. **user**. This is your username for postgres. Unless you changed your username during your Postgres installation, this will be `postgres` by default.
4. **host**. This is the hostname to connect to your database. By default, for a local installation of PostgreSQL on your own computer like you performed, this will be `localhost`.
5. **requirePassword**. If you’ve removed the password as suggested during PostgreSQL installation, set `requirePassword` to `False`. 
```
# postgres connection information
pgInfo = {'db': 'cruisedb', # [Default: 'your_database_name']
          'schema': 'public',    # schema with GPS traces and streets table [Default: 'your_schema_name']
          'user': 'postgres', # [Default: 'your_postgres_username']
          'host': 'localhost', # [Default: 'localhost_or_IP_address']
          'requirePassword': False # Prompt for password? Normally, False for localhost [Default: True]
          }
```
| Default | Example Value |
| ------- | ------ |
| `'db': 'your_database_name'` | `'db': 'cruisedb'` |
| `'schema': 'your_schema_name'` | `'schema': 'public'` |
| `'user': 'your_postgres_username'` | `'user': 'postgres'` |
| `'host': 'localhost_or_IP_address'` | `'host': 'localhost'` |
| `'requirePassword': True` | `'requirePassword': False` |

###### Changes to `travelCostReverseCol`
The default column headings in this file are for the pgMapMatch sample data, which uses [different column names](https://github.com/amillb/pgMapMatch/pull/31). Under the heading `# column identifiers for the PostGIS table of streets` toward the bottom, change the value for `travelCostReverseCol` from `reverse_co` to `reverse_cost`:
```
# column identifiers for the PostGIS table of streets
# the default values here are compatible with osm2po
streetIdCol = 'id'          # unique id for street edge (i.e. segment or block)
streetGeomCol = 'geom_way'  # geometry column (LineString) for street edge
startNodeCol = 'source'     # id of node at which the street edge starts
endNodeCol = 'target'       # id of node at which the street edge ends
travelCostCol = 'cost'      # generalized cost to go from startNode to endNode. Expressed in hours to traverse the edge (see https://gis.stackexchange.com/questions/198200/how-are-cost-and-reverse-cost-computed-in-pgrouting)
travelCostReverseCol = 'reverse_cost'  # generalized cost to go from endNode to startNode. Can be same as travelCostCol if you have no one-way streets. [Formerly  'reverse_co']
streetLengthCol = 'km'      # length of street, in km
speedLimitCol = 'kmh'       # speed limit on street, in km per hour
```
| Default | Rename |
| ------- | ------ |
| `travelCostReverseCol = 'reverse_co'` | `travelCostReverseCol = 'reverse_cost'` |

#### cruisedetector
Download or clone the [cruisedetector](https://github.com/RegionalPlanAssoc/cruisedetector) repository to the base directory.

To clone the pgMapMatch repository:
1. After installing GitHub Desktop, got to 'File' > 'Clone repository' or use the shortcut CTRL+Shift+O.
2. Go to the right-most tab named 'URL' to install using the URL.
3. For the first field, "Repository URL or GitHub username and repository", paste in `https://github.com/RegionalPlanAssoc/cruisedetector` or `RegionalPlanAssoc/cruisedetector`.
4. For the second field, "Local path", use the filepath to the base directory followed by `cruisedetector`, which GitHub Desktop should add by default (i.e. `C:\cruisebase\cruisedetector`).
5. You should have a the `cruisedetector` folder when you access the base directory using File Explorer. To update at a later time, in GitHub Desktop, click 'Fetch Origin' near the top right and 'Pull'. 

Alternatively, you may download to the base directory without using GitHub Desktop by going to the cruisedetector repository in browser, clicking the green 'Code' button, and 'Download ZIP' to the base directory. After unzipping the repository, be sure to rename the folder to `cruisedetector` from `cruisedetector-main` or any other name.

##### Configuring `cruising.py`
Configuration parameters for `cruising.py` are located in the header of `cruising.py`, including the host, file paths, regions, spatial reference systems, and number of CPU cores used for processing. The primary changes to make for 1. and 6., which you should update with the filepath for your Cruise Detector base directory.
```
"""
Defaults that the user should change
"""
# 1. Basepath and output folder for log
basePath = 'C:/cruisebase/'

# 2. Which regions to load streets and other data for
defaults = {}
defaults['regions'] = ['sf','mi','wa','il']

# 3. Dictionary of coordinate reference systems for each region
# the crs (SRID) should be recognized by PostGIS
# if your region is missing, add it to the dictionary
crs = {'ca':'3493','sf':'3493','mi':'2809','wa':'2855'}

# 4. Path for log file. Default is in your current directory
logPath = '.'

# 5. You may need to add a directory to your sys.path
sys.path.append(basePath)

# 6. Path for osm2po with no spaces
osm2poPath = "C:/cruisebase/"
osm2poVersion = '5.5.16' # '5.5.1'

# 7. Location of mapmatching coefficient file (in this git repo). You shouldn't need to change this.
# https://stackoverflow.com/questions/3718657/how-do-you-properly-determine-the-current-script-directory
repoPath = Path(globals().get("__file__", "./_")).absolute().parent
coeffFn = str(repoPath) + '/mapmatching_coefficients.txt'

# 8. Specify number of processing cores to be used
cores = 4
```

##### Configuring `cruising_importLocationData.py`
Configuration parameters for `cruising_importLocationData.py` are located in the header of `cruising_importLocationData.py`, and allow you to calibrate trace generation from GPS data and cruising identification. You should not need to make any changes here.
```
#Set variables

h_acc_Var = 65 #max horizontal accuracy threshold
ping_tot_Var = 10 #min number of pings per device
trip_ping_tot_Var = 10 #min number of pings per trip
trip_ping_avg_Var = 90 #max average interval of pings in a trip, in seconds
speed_Var = 130 #max plausible speed between pings, in km/hr
trip_start_Var = 600 #pause in pings to start a new trip
duration_Var = 300 #min duration for a trip from start to end, in seconds
```

## Data Requirements and Format
### Street Network
A base street network will be required in order to route a shortest path as the end a trip and compare it to the actual path.

The street network should be in [OpenStreetMap's Protocolbuffer Binary Format (.pbf)](https://wiki.openstreetmap.org/wiki/PBF_Format). Extracts for broad geographic areas can be obtained from [geofabrik.de](https://www.geofabrik.de/). The extract should be saved to the the project base directory `C:\cruisebase\`, the same parent directory where you installed osm2po.

The field names for the streets table should match those in the `pgMapMatch/config.py`. Make sure the Spatial Reference System (SRS) of the streets table matches the SRS you will be using for the location or trip data, and any necessary indexes and spatial indexes have been created. Depending on the imported network, it may improve performance to clip the input street network to a convex hull around a specific study area.

For the example workflow in this readme, the [street network in Washington State (washington-latest.osm.pbf)](https://download.geofabrik.de/north-america/us/washington.html) is used.

### Input Trip Data
#### Location or Ping Data
Primarily, trips are generated using the timestamped locations of a driver, which is typically derived from the pings of mobile phone locations by cellular signaling data vendors.

Data formats will vary by vendor, but at minimum, the raw ping data must contain the following variables:
- **device ID**: unique ID for a collection of pings
- **timestamp**:  date and time of day recorded for the ping.
- **latitude**: numeric coordinate in degrees of the north-south position of the ping from the equator.
- **longitude**: numeric coordinate in degrees of the east-west position of the ping from the Prime Meridian.
- **horizontal accuracy**: estimated error between the reported point and the true geographic location.

The following list is a non-exhaustive list of location data vendors.  Inclusion in the list is not an endorsement.

- Vera set
- Quadrant
- Onemata
- Lifesight

The example workflow in this readme and the `cruising_importLocationData.py` script included in this repository have been written based on data from Quadrant containg pings in Seattle, Wa. Thus, your workflow and version of the `cruising_importLocationData.py` script may require alterations to match the format and structure of the data if an alternative source is used for this script.


#### Trip Paths
Alternatively, trip paths that have already been pre-processed into traces by the vendor may also be used. This data would substitute directly for the trace table (`sampletraces`) table, and the generation of traces from the point table (`samplepoints`) would be skipped.

The imported PostgreSQL table must contain:
- **trip_id**: Unique ID for each trace
- **lines_geom**: Linestring M geometry of the trace
- **start_geom**: Point M geometry of the start point
- **end_geom**: Point M geometry of the end point

Possible vendors for trip data include: 
- StreetLight
- AirSage
- INRIX
- TomTom

As with the location data, the list is not comprehensive, not an endorsement nor a guarantee the vendor will make usable data available. An agreement or a collaboration with these firms may be necessary to access their data and information.

## Expected Directory Structure
After installation and obtaining the data, the base directory should be structured as follows if using all the example names provided above.

```
C:\cruisebase\
│──  cruisedetector\
│   │── cruising.py`
│   │── cruising_importLocationData.py
│   │── cruising_setup.py
│   └── [other files]
│── osm2po-5.5.16\
│   │── osm2po-core-5.5.16-signed.jar
│   └── osm2po.config
│── washington-latest.osm.pbf
│── pgMapMatch\
│   │── config.py
│   └── [other files]
│── sampleLocationData
│   │── ... .gz
│   │── ... .gz
│   └── ...
└── output\
```

## Example Workflow in your Python IDE

### Import Cruise Detector
Run the following to import the cruise detector scripts into Python, allowing use the function in the subsequent steps.
```
import sys
yourBasePath = 'C:/cruisebase' ## change this to your base path
sys.path.append(f'{yourBasePath}/cruisedetector') 
sys.path.append(yourBasePath)
from cruising import *
from cruising_importLocationData import *
```
### Import Street Network
You will first need toimport the osm street network and turn restriction table into the database by running: 

```
loadTables(region='[yourRegion]')
```

#### [yourRegion]
You will substitute `[yourRegion]` with the abbreviation of the region specified in the list and dictionary in the Defaults of `cruising.py`. For example, if you are using the [Washington State osm.pbf](https://download.geofabrik.de/north-america/us/washington.html), you would run `loadTables(region='wa')`. 

```
# 2. Which regions to load streets and other data for
defaults = {}
defaults['regions'] = ['sf','mi','wa','il']

# 3. Dictionary of coordinate reference systems for each region
# the crs (SRID) should be recognized by PostGIS
# if your region is missing, add it to the dictionary
crs = {'ca':'3493','sf':'3493','mi':'2809','wa':'2855'}
```
Your should add an abbreviation for the region and corresponding spatial reference system to the list and dictionary if it is not already included. This abbreviation will be used as a prefix for most the tables that are specific to the region or spatial reference system. For example, for Washington State, you should have several new tables in your PostgreSQL database that you can view under 'Tables' in pgAdmin.
- `wa_streets`
- `wa_turn_restrictions`
- `spatial_ref_sys`

### Generate Traces
In this step, the ping point data will be used to generate traces, that can then be analyzed for cruising. Most of the functions used in this section are from the `cruising_importLocationData.py` script. Remember that the `cruising_importLocationData.py` script is based on a specific data vendor and may require alteration to match the format and data structure of the location data obtained. 

#### Table Names
Set the names you would like to use for the input ping point data table (`points_table`) and output trace table (`trace_table`). These names will be used within your PostgreSQL database, and you can find the tables under these name in pgAdmin.
```
points_table = 'samplepoints'
trace_table = 'sampletraces'
```

#### Import Ping Locations
Then, using these names and the filepath to your input ping data in `importTable()` to import the table into you PostgreSQL database:
```
iT = importTable(points_table, '[pathToPingData]', schema = '[yourSchema]', region = '[yourRegion]', forceUpdate=True)
iT.createTable()
iT.importCSV()
```

You should have this new table in your PostgreSQL database:
- `samplepoints`

#### Generate Traces
To generate traces from the points table, run the following code:
```
pts = pointData(points_table, trace_table, schema = '[yourSchema]', region = '[yourRegion]', forceUpdate=True)
pts.geocodePoints() # Produces `raw_points_1` table
pts.processPoints() # Produces `tmp_withlags` table
pts.generateTraces()
pts.generateUniqueIDs()
```

You should have these new tables in your PostgreSQL database:
- `sample_traces`

### Map-matching the Traces
Once the trace table is generated, the can be map-matched by running the following code. Please note that this step may take several hours.
```
tt = traceTable(trace_table, schema = '[yourSchema]', region = '[yourRegion]', forceUpdate=True) ## change the table, schema, etc. 
tt.runall()
```

## Results and Interpretation
Once the trips have been processed, you should have several new fields added to `sampletraces`. *See definitions for all of fields for all fields in the data dictionary [here](https://github.com/RegionalPlanAssoc/cruisedetector/blob/main/data_dictionary.csv) or in repository.*

| Field | Description | Interpretation |
| ----- | ----------- | -------------- |
| cruise | | |
| high_cruise | | |
