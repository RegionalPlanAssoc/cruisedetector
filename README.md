# cruisedetector
## Introduction
This manual describes how to use the Cruise Detector, a GPS cruising identification model developed by the Federal Highway Administration.  Any user with a working knowledge of GIS and simple database skills should be able to implement the system with the aid of information presented here.

This tool uses GPS data to estimate the proportion of trips that are cruising for parking.

## Software Requirements
### Installation
Install [PostgreSQL 13+](https://www.postgresql.org/), [PostGIS 3.2+](https://postgis.net/) along with pgrouting 3.3.0+. We also recommend using [pgadmin 4](https://www.pgadmin.org/). You will also need to install [osm2po](http://osm2po.de/) and [Java](https://www.java.com/en/download/) 8+ to import a street network.

The following Python packages must also be installed: 
numpy 1.11.3+
scipy 0.19.0+
pandas 0.19.2+
gpxpy 1.1.2+
psycopg2 2.5.2+
sqlalchemy 1.1.6+
docopt 0.6.1+

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
## Config File Changes
You will need to change the configuration settings for Postgres, pgMapMatch, osm2po, and the cruising tool itself.
 
**Postgres**. After setting up the postgres database, removing the password requirement allows the tool to run smoother. This can be done by changing authentication requirements in the `pg_hba.conf` file to `trust`.

Clone the cruising and pgMapMatch repositories, and add a folder titled “output” to store logs.

**cruising**.  Configuration parameters are located in the cruising.py file. Open cruising.py and set the parameters for host, file paths, regions, spatial reference systems, and number of CPU cores used for processing.  The config file also contains multiple parameters to calibrate trace generation from GPS data and identify cruising.

**pgMapMatch**. Open `config_template.py` and make changes to the `pgInfo` parameter for your postgres database connection. If you’ve removed the password, make sure `requirePassword` is set to `False`. Save the file as `config.py`.

**osm2po**. Use the [osm2po](http://osm2po.de/) tool to import the OpenStreetMap data into the database. Make a couple of changes to the osm2po config file to accurately reflect [turn restrictions](http://gis.stackexchange.com/questions/41393/does-osm2po-take-into-consideration-turn-restrictions) and one-way streets by un-commenting the following lines:

```
postp.0.class = de.cm.osm2po.plugins.postp.PgRoutingWriter

postp.1.class = de.cm.osm2po.plugins.postp.PgVertexWriter 

graph.build.excludeWrongWays = true
```

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
