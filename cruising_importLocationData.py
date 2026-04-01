import csv, glob
# import time, os, datetime, 
import numpy as np
import pandas as pd
from io import StringIO
import pgMapMatch.tools as  mmt
from pgMapMatch.config import *
from cruising import *

import geopandas as gpd
import shapely.geometry
import shapely.wkb

# from cruisedetector.config import *
# try:
#     from cruisedetector.config import *
# except ModuleNotFoundError:
#     raise Warning('config.py not found.')

### MOVED TO CONFIG.PY
#Set variables used in this script from config.py
# h_acc_Var = 65 #max horizontal accuracy threshold
# ping_tot_Var = 10 #min number of pings per device
# trip_ping_tot_Var = 10 #min number of pings per trip
# trip_ping_avg_Var = 90 #max average interval of pings in a trip, in seconds
# speed_Var = 130 #max plausible speed between pings, in km/hr
# trip_start_Var = 600 #pause in pings to start a new trip
# duration_Var = 300 #min duration for a trip from start to end, in seconds

## MODULE-LEVEL FUNCTIONS
# Function to convert any PointM geometry to a 2D Point by dropping M coordinates
# For simple geopandas shapefile export, since shapefiles don't support M values. If the geometry is not a Point, it will be returned unchanged.
def to_2d_point(geom):
    if geom is None:
        return None
    # Check if the geometry is any of type Point (e.g., Point or PointM)
    if 'Point' in geom.geom_type:
        # Explicitly create a new Point using only the X and Y coordinates, without M.
        return shapely.geometry.Point(geom.x, geom.y)
    return geom

# Function to convert any LineString geometry to a 2D LineString by dropping M and Z coordinates
# For simple geopandas shapefile export, since shapefiles don't support M or Z values. If the geometry is not a LineString, it will be returned unchanged
def to_2d_linestring(geom):
    if geom is None:
        return None
    # Check if the geometry is any type of LineString (e.g., LineString, LineString M, LineString Z, LineString ZM)
    if 'LineString' in geom.geom_type:
        # Explicitly create a new LineString using only the X and Y coordinates
        return shapely.geometry.LineString([(coord[0], coord[1]) for coord in geom.coords])
    return geom

#import table to database
class importTable():
    def __init__(self,points_table,points_table_header,file_dir,region=None,nCores=12,schema=None,logFn=None,forceUpdate=False):
        self.table = points_table   # postgres traces table name
        self.table_header = points_table_header # header row number, list of heading names, or
        self.file_dir = file_dir
        self.region=region

        self.nCores = nCores  # if None, no parallelization will be done
        global paths
        # print("Getting .gz files from: ", file_dir)
        self.logFn = logPath+self.table+'_log.log' if logFn is None else logFn
        if 'pgLogin' not in globals(): # initialize connection
            global pgLogin  # make it available for parallel instances
            pgLogin = mmt.getPgLogin(user=pgInfo['user'], db=pgInfo['db'], host=pgInfo['host'], requirePassword=pgInfo['requirePassword'], forceUpdate=False)
            pgLogin['schema'] = schema if schema is not None else 'poc' if 'sl_' in table else 'parking'
        self.pgLogin = pgLogin # shouldn't be necessary, but needed for Dylan
        self.db = mmt.dbConnection(pgLogin=pgLogin, logger=logFn)
        self.forceUpdate = forceUpdate
        self.ids = None
        self.nPings = None
        self.writeLog('\n____________Importing CSVs from .gz files in "%s"____________\n' % (self.file_dir))

    def writeLog(self,txt):
        assert isinstance(txt, str)
        currentTime = datetime.datetime.now().strftime("%I:%M%p %B %d, %Y")
        with open(self.logFn,'a') as f:
            f.write(currentTime+':\t '+txt)
            print(currentTime+':\t '+txt)

    def createTable(self):
        fname = 'createTable()'
        print(f'\n____________Executing {fname}____________')
        fstart = time.perf_counter()
        print(f'{datetime.datetime.now()} (+{fstart} seconds)')

        print(f"Creating '{self.table}' points table in database {self.pgLogin['db']}...\n")
        self.db.execute('DROP TABLE IF EXISTS %s' % (self.table))
        self.db.execute('''
        --begin-sql
        CREATE TABLE %s (
                device_id VARCHAR(50),
                id_type VARCHAR(50),
                latitude FLOAT,
                longitude FLOAT,
                h_acc FLOAT,
                timestamp BIGINT
                )
        --end-sql
        ''' % (self.table))
        print(f"Created table '{self.table} in database {self.pgLogin['db']}.\n")

        fend = time.perf_counter()
        print(f'{datetime.datetime.now()} ({fend} seconds)')
        print(f'____________Finished {fname} after {fend - fstart} seconds____________\n')

    def importCSV(self):
        fname = 'importCSV()'
        print(f'\n____________Executing {fname}____________')
        fstart = time.perf_counter()
        print(f'{datetime.datetime.now()} (+{fstart} seconds)')

        #for importing compressed csvs
        print(f"Populating '{self.table}' points table with .gz files from: \n{self.file_dir}")
        csv_list = glob.glob("%s/*.gz" % (self.file_dir))
        print(f"Found {len(csv_list)} total .gz files:")
        for i,csv_file in enumerate(csv_list):
            print(f"\t[{i}] {csv_file}")

        #the table structure depends on the data source
        #change the headings below to match the headings for your data
        points_table_header = self.table_header
        if isinstance(points_table_header, int):
            header_row_number = points_table_header
            header_names = None
            print(f"Using row [{points_table_header}] as header.")
        elif isinstance(points_table_header, list):
            header_row_number = None
            header_names = points_table_header
            print(f"Using list as header: \t{points_table_header}")
        elif points_table_header == 'quadrant':
            header_row_number = None
            header_names = [
                'device_id', 'id_type', 'latitude', 'longitude', 'h_acc', 'timestamp', 'ip_address', 'device_os', 'device_os_v',
                'user_agent', 'country_code', 'source_id', 'publisher_id', 'app_id', 'location_cont', 'geohash'
                ]
            print(f"Using default quadrant names: \t{header_names}")
        else:
          print("points_table_header must be an integer row number, list of headings, or string name of an accepted data provider.")

        # load each of the CSVs
        print("Loading CSV .gz files into database...")
        for i,csv_file in enumerate(csv_list):
            df = pd.read_csv(csv_file,compression='gzip', header=header_row_number, sep=',', quotechar='"', names=header_names)
            print(f"\t[{i}] {df.shape}")

            if isinstance(points_table_column_renames, dict):
                df = df.rename(columns=points_table_column_renames)

            buffer = StringIO()
            df.to_csv(buffer, columns=('device_id', 'id_type', 'latitude', 'longitude', 'h_acc', 'timestamp'),
                      index=False, index_label=None, header=False)
            buffer.seek(0)

            self.db.copy_from(buffer, self.table)

        # create unique id
        print(f"Adding unique id column 'gid'...")
        self.db.execute('ALTER TABLE %s ADD COLUMN gid BIGSERIAL' % (self.table))

        fend = time.perf_counter()
        print(f'{datetime.datetime.now()} ({fend} seconds)')
        print(f'____________Finished {fname} after {fend - fstart} seconds____________\n')


#process imported location point data
class pointData():
    def __init__(self,points_table,output_table,region=None,nCores=12,schema=None,logFn=None,forceUpdate=False):
        self.table = points_table   # postgres points table name
        self.output_table = output_table
        self.region=region

        self.nCores = nCores  # if None, no parallelization will be done
        global crs
        self.crs = crs[self.region]
        global paths
        self.logFn = logPath+self.table+'_log.log' if logFn is None else logFn
        if 'pgLogin' not in globals(): # initialize connection
            global pgLogin  # make it available for parallel instances
            pgLogin = mmt.getPgLogin(user=pgInfo['user'], db=pgInfo['db'], host=pgInfo['host'], requirePassword=pgInfo['requirePassword'], forceUpdate=False)
            pgLogin['schema'] = schema if schema is not None else 'poc' if 'sl_' in table else 'parking'
        self.pgLogin = pgLogin # shouldn't be necessary, but needed for Dylan
        self.db = mmt.dbConnection(pgLogin=pgLogin, logger=logFn)
        self.forceUpdate = forceUpdate
        self.ids = None
        self.nPings = None
        # ensure index completeness
        for tn, idx in [(self.table, 'gid')]:
            self.db.execute('CREATE INDEX IF NOT EXISTS {tn}_{idx}_idx ON {tn} ({idx});'.format(idx=idx, tn=tn))
        self.writeLog('\n____________PROCESSING POINTS table for %s____________\n' % (self.table))

    def writeLog(self,txt):
        assert isinstance(txt, str)
        currentTime = datetime.datetime.now().strftime("%I:%M%p %B %d, %Y")
        with open(self.logFn,'a') as f:
            f.write(currentTime+':\t: '+txt)
            print(currentTime+':\t: '+txt)

    def resetTraceTable(self):
        self.db.execute('DROP TABLE IF EXISTS %s' % (self.output_table))

    #geocode points with timestamp to PointM geometry
    def geocodePoints(self):
        fname = 'geocodePoints()'
        print(f'\n____________Executing {fname}____________')
        fstart = time.perf_counter()
        print(f'{datetime.datetime.now()} (+{fstart} seconds)')

        ## RAW POINTS 1
        print(f"\nCreating 'raw_points_1' points table in database {self.pgLogin['db']}...\n")
        print("Creating PointM geometry for points in 'raw_points_1' with timestamp as M value, and filtering by horizontal accuracy...")
        self.db.execute('DROP TABLE IF EXISTS raw_points_1')
        self.db.execute('''
            --begin-sql
            CREATE TABLE raw_points_1 AS
                SELECT    *,
                        to_timestamp(timestamp / 1000) AT TIME ZONE 'UTC' AS timestamp2,
                        ST_SetSRID (
                            ST_MakePointM (longitude, latitude, timestamp / 1000),
                            4326
                        ) AS geom -- Note it's important to convert timestamp into seconds before converting to geom
                FROM      %s
                WHERE     h_acc < %d
            --end-sql
        ''' % (self.table, h_acc_Var))

        print("Re-projecting geocoded points in 'raw_points_1'")
        self.db.execute('''
            --begin-sql
            ALTER TABLE raw_points_1
                ALTER COLUMN geom TYPE Geometry (PointM, %s) USING ST_Transform (geom, %s)
            --end-sql
        '''  % (self.crs, self.crs))
        print(f"Geocoded points in 'raw_points_1' with PointM geometry in database {self.pgLogin['db']}.\n")

        fend = time.perf_counter()
        print(f'{datetime.datetime.now()} ({fend} seconds)')
        print(f'____________Finished {fname} after {fend - fstart} seconds____________\n')

    #process geocoded points into traces
    def processPoints(self):
        fname = 'processPoints()'
        print(f'\n____________Executing {fname}____________')
        fstart = time.perf_counter()
        print(f'{datetime.datetime.now()} (+{fstart} seconds)')
        
        ## TEMPORARY POINTS WITH LAGS
        #calculate intervals and distance between points
        print(f'\n{datetime.datetime.now()} (+{time.perf_counter()} seconds)')
        print(f"Creating 'tmp_withlags' points table in database {self.pgLogin['db']}...\n")
        print("Calculating time and distance intervals between points...")
        self.db.execute('DROP TABLE IF EXISTS tmp_withlags')
        self.db.execute('''
            --begin-sql
            CREATE TABLE tmp_withlags AS
            SELECT    *,
                    ABS(
                        EXTRACT(
                        EPOCH
                        FROM      t0.epoch_time
                        ) 
                        - 
                        EXTRACT(
                        EPOCH
                        FROM      t0.lag_epoch_time
                        )
                    ) AS time_diff_second,
                    ST_Distance (lag_geom, geom) AS distance_diff_degree
            FROM      (
                    SELECT    *,
                                lag(t1.geom, 1) OVER (
                                    partition by t1.device_id
                                    ORDER BY  t1.epoch_time
                                ) as lag_geom,
                                lag(t1.epoch_time, 1) OVER (
                                    partition by t1.device_id
                                    ORDER BY  t1.epoch_time
                                ) as lag_epoch_time
                    FROM      (
                                SELECT  gid,
                                        s1.device_id,
                                        point_count,
                                        timestamp,
                                        to_timestamp(CAST(timestamp as bigint) / 1000) AT TIME ZONE 'UTC' as epoch_time,
                                        geom
                                FROM    raw_points_1 s1
                                LEFT JOIN (
                                        SELECT    device_id,
                                                  COUNT(device_id) AS point_count
                                        FROM      raw_points_1
                                        GROUP BY  device_id
                                        ) s2 ON s1.device_id = s2.device_id
                                ) t1
                    ) t0
            --end-sql
        ''')
        
        ## TEMPORARY START POINTS
        #identify starting points for traces
        print(f'\n{datetime.datetime.now()} (+{time.perf_counter()} seconds)')
        print(f"Creating 'tmp_startpoints' points table in database {self.pgLogin['db']}...\n")
        print("Identifying starting points for traces in 'tmp_startpoints'...")
        self.db.execute('DROP TABLE IF EXISTS tmp_startpoints')
        self.db.execute('''
            --begin-sql
            CREATE TABLE tmp_startpoints AS
                SELECT    gid,
                          True as startpt
                FROM      tmp_withlags
                WHERE     lag_epoch_time is Null
                AND       point_count > 1
                OR        time_diff_second > % d
            --end-sql
        ''' % (trip_start_Var))

        ## TEMPORARY POINTS ALL
        # starting points and lag points
        print(f'\n{datetime.datetime.now()} (+{time.perf_counter()} seconds)')
        print(f"Creating 'tmp_pointall' points table in database {self.pgLogin['db']}...\n")
        print("Joining 'tmp_withlags' and 'tmp_startpoints' into 'tmp_pointall'...")
        self.db.execute('DROP TABLE IF EXISTS tmp_pointall')
        self.db.execute('''
            --begin-sql
            CREATE TABLE tmp_pointall AS
                SELECT t1.*,t2.startpt
                FROM tmp_withlags t1
                LEFT JOIN tmp_startpoints t2
                       ON t1.gid = t2.gid
                ORDER BY device_id,epoch_time
            --end-sql
            ''')

        # self.db.execute('DROP TABLE tmp_withlags')
        # self.db.execute('DROP TABLE tmp_startpoints')

        ## TEMPORARY POINTS 1
        #calculate speed between pings
        print(f'\n{datetime.datetime.now()} (+{time.perf_counter()} seconds)')
        print(f"Creating 'tmp_pointdrop1' points table in database {self.pgLogin['db']}...\n")
        print("Calculating speed between points in 'tmp_pointdrop1' and filtering out points with implausible speeds or zero distance...")
        self.db.execute('DROP TABLE IF EXISTS tmp_pointdrop1')
        self.db.execute('''
            --begin-sql
            CREATE TABLE tmp_pointdrop1 AS
                SELECT  *,
                        CASE
                            WHEN time_diff_second > 0 
                            THEN (distance_diff_degree / 1000) / (time_diff_second / 3600)
                            ELSE 0
                        END AS speed
                FROM      tmp_pointall
                WHERE     point_count > 2
                AND       (
                                distance_diff_degree IS NULL
                            OR  distance_diff_degree <> 0
                          )
            --end-sql
        ''')
        # self.db.execute('DROP TABLE tmp_pointall')

        ## TEMPORARY POINTS 2
        print(f'\n{datetime.datetime.now()} (+{time.perf_counter()} seconds)')
        print(f"Creating 'tmp_pointdrop2' points table in database {self.pgLogin['db']}...\n")
        print("Filtering points in 'tmp_pointdrop1' into 'tmp_pointdrop2' by dropping points with implausible speeds and creating temporary trip ids based on starting points...")
        self.db.execute('DROP TABLE IF EXISTS tmp_pointdrop2')

        ## ORIGINAL SPEED FILTER
        self.db.execute('''
            --begin-sql
            CREATE TABLE tmp_pointdrop2 AS
                SELECT    *,
                        SUM(
                        CASE
                                    WHEN startpt is null THEN 0
                                    ELSE 1
                        END
                        ) OVER (
                        order by  device_id,
                                    epoch_time
                        ) as tmp_trip_id
                FROM      tmp_pointdrop1
                WHERE     speed < % d
            --end-sql
        ''' % (speed_Var))

        ## WIP (2026-03-25) Standard Deviation Bypass for INRIX
        #create unique trip_id for each trace
        # filter by speed OR
        # if the pings are within standard deviations of the overall mean of trip speeds
        # self.db.execute('''CREATE TABLE tmp_pointdrop2 AS
        #     SELECT
        #         *,
        #         SUM(CASE WHEN startpt IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY device_id, epoch_time) AS tmp_trip_id
        #     FROM (
        #         SELECT
        #             *,
        #             AVG(speed) OVER (PARTITION BY device_id) AS mean_speed_device,
        #             STDDEV(speed) OVER (PARTITION BY device_id) AS std_dev_speed_device
        #         FROM
        #             tmp_pointdrop1
        #     ) AS with_stats
        #     WHERE
        #         speed < %d
        #         OR (speed BETWEEN (mean_speed_device - std_dev_speed_device) AND (mean_speed_device + std_dev_speed_device))''' % (speed_Var))
        # self.db.execute('DROP TABLE tmp_pointdrop1')

        ## TEMPORARY POINTS 3
        #count points in each trace
        print(f'\n{datetime.datetime.now()} (+{time.perf_counter()} seconds)')
        print(f"Creating 'tmp_pointdrop3' points table in database {self.pgLogin['db']}...\n")
        print("Counting points in each trace in 'tmp_pointdrop3' to prepare for final filtering...")
        self.db.execute('DROP TABLE IF EXISTS tmp_pointdrop3')
        self.db.execute('''
            --begin-sql
            CREATE TABLE tmp_pointdrop3 AS
                SELECT    t1.*,
                        t2.point_count2
                FROM      tmp_pointdrop2 t1
                LEFT JOIN (
                        SELECT    tmp_trip_id,
                                    COUNT(tmp_trip_id) AS point_count2
                        FROM      tmp_pointdrop2
                        GROUP BY  tmp_trip_id
                        ) t2 ON t1.tmp_trip_id = t2.tmp_trip_id
            --end-sql
        ''')
        # self.db.execute('DROP TABLE tmp_pointdrop2')

        ## QUADRANT_POINT4TRACE
        #filter processed points table into final points to create traces
        print(f'\n{datetime.datetime.now()} (+{time.perf_counter()} seconds)')
        print(f"Creating 'quadrant_point4trace' points table in database {self.pgLogin['db']}...\n")
        print("Filtering processed points in 'tmp_pointdrop3' into 'quadrant_point4trace' by dropping traces with fewer than %d points..." % (trip_ping_tot_Var))
        self.db.execute('DROP TABLE IF EXISTS quadrant_point4trace')
        self.db.execute('''
            --begin-sql
            CREATE TABLE quadrant_point4trace AS
                SELECT    *,
                        SUM(
                            CASE
                                    WHEN startpt is null THEN 0
                                    ELSE 1
                            END
                        ) OVER (
                            order by    device_id,
                                        epoch_time
                        ) as trip_id
                FROM      tmp_pointdrop3
                WHERE     point_count2 > %d
            --end-sql
        ''' % (trip_ping_tot_Var))
        # self.db.execute('DROP TABLE tmp_pointdrop3')

        ## TEMPORARY POINTS WITH LAGS 2
        #re-calculate time and distance intervals between points, since some have been dropped
        print(f'\n{datetime.datetime.now()} (+{time.perf_counter()} seconds)')
        print(f"Creating 'tmp_withlags2' points table in database {self.pgLogin['db']}...\n")
        print("Re-calculating time and distance intervals between points in 'tmp_withlags2' after dropping points with implausible speeds and traces with too few points...")   
        self.db.execute('DROP TABLE IF EXISTS tmp_withlags2')
        self.db.execute('''
            --begin-sql
                CREATE TABLE tmp_withlags2 AS
                SELECT    *,
                        ABS(
                            EXTRACT(
                                EPOCH
                                FROM      t0.epoch_time
                            ) 
                            - 
                            EXTRACT(
                                EPOCH
                                FROM      t0.lag_epoch_time2
                            )
                        ) AS time_diff_second2,
                        ST_Distance (lag_geom2, geom) AS distance_diff_degree2
                FROM    (
                        SELECT    *,
                                    lag(t1.geom, 1) OVER (
                                        partition by t1.device_id
                                        ORDER BY  t1.epoch_time
                                    ) as lag_geom2,
                                    lag(t1.epoch_time, 1) OVER (
                                        partition by t1.device_id
                                        ORDER BY  t1.epoch_time
                                    ) as lag_epoch_time2
                        FROM      (
                                    SELECT  gid,
                                            s1.device_id,
                                            s1.point_count,
                                            point_count3,
                                            timestamp,
                                            epoch_time,
                                            geom,
                                            lag_geom,
                                            lag_epoch_time,
                                            time_diff_second,
                                            distance_diff_degree,
                                            startpt,
                                            speed,
                                            tmp_trip_id,
                                            point_count2,
                                            trip_id
                                    FROM      quadrant_point4trace s1
                                    LEFT JOIN (
                                            SELECT    device_id,
                                                      COUNT(device_id) AS point_count3
                                            FROM      quadrant_point4trace
                                            GROUP BY  device_id
                                            ) s2 ON s1.device_id = s2.device_id
                                    ) t1
                        ) t0
            --end-sql
        ''')
        
        fend = time.perf_counter()
        print(f'{datetime.datetime.now()} ({fend} seconds)')
        print(f'____________Finished {fname} after {fend - fstart} seconds____________\n')

    #generate traces from processed and filtered points
    def generateTraces(self):
        fname = 'generateTraces()'
        print(f'\n____________Executing {fname}____________')
        fstart = time.perf_counter()
        print(f'{datetime.datetime.now()} (+{fstart} seconds)')

        ## QUADRANT TRACES 1
        print(f'\n{datetime.datetime.now()} (+{time.perf_counter()} seconds)')
        print(f"Creating 'quadrant_traces_1' traces table in database {self.pgLogin['db']}...\n")
        print("Generating traces in 'quadrant_traces_1' by connecting 'tmp_with_lags2' points with linestrings and calculating trace-level attributes...")
        self.db.execute('DROP TABLE IF EXISTS quadrant_traces_1')
        self.db.execute('''
            --begin-sql
            CREATE TABLE quadrant_traces_1 AS
                SELECT  t1.device_id,
                        t1.trip_id,
                        t2.avg_pingtime,
                        t2.ping_count,
                        t2.trip_distance,
                        t2.trip_duration,
                        t2.avg_speed,
                        ST_MakeLine (
                            t1.geom
                            ORDER BY  t1.timestamp
                        ) AS lines_geom
                FROM      tmp_withlags2 t1
                LEFT JOIN (
                        SELECT      trip_id,
                                    SUM(time_diff_second) / (Count(time_diff_second) -1) as avg_pingtime,
                                    AVG(point_count2) as ping_count,
                                    SUM(distance_diff_degree) AS trip_distance,
                                    ABS(
                                        EXTRACT(
                                            EPOCH
                                            FROM      MAX(epoch_time)
                                        ) 
                                        - 
                                        EXTRACT(
                                            EPOCH
                                            FROM      MIN(epoch_time)
                                        )
                                    ) AS trip_duration,
                                    CASE
                                            WHEN SUM(time_diff_second2) > 0 
                                            THEN SUM(distance_diff_degree2) / SUM(time_diff_second2)
                                            ELSE NULL
                                    END AS avg_speed
                        FROM      tmp_withlags2
                        WHERE     startpt IS NULL
                        GROUP BY  trip_id
                        ) t2 ON t1.trip_id = t2.trip_id
                GROUP BY    t1.device_id,
                            t1.trip_id,
                            t2.avg_pingtime,
                            t2.ping_count,
                            t2.trip_distance,
                            t2.trip_duration,
                            t2.avg_speed
                ORDER BY  trip_id
            --end-sql
            ''')

        ## QUADRANT TRACES USABLE 1
        print(f'\n{datetime.datetime.now()} (+{time.perf_counter()} seconds)')
        print(f"Creating 'quadrant_traces_usable_1' traces table in database {self.pgLogin['db']}...\n")
        print("Filtering traces in 'quadrant_traces_1' into 'quadrant_traces_usable_1' by calculating start and end points and distances and preparing for final filtering...")
        self.db.execute('DROP TABLE IF EXISTS quadrant_traces_usable_1')
        self.db.execute('''
            --begin-sql
            CREATE TABLE quadrant_traces_usable_1 AS
                SELECT  trip_id,
                        lines_geom,
                        ST_SetSRID (ST_Force2D (ST_StartPoint (lines_geom)), %s) AS start_geom,
                        ST_SetSRID (ST_Force2D (ST_EndPoint (lines_geom)), %s) AS end_geom,
                        avg_pingtime,
                        ping_count,
                        trip_distance,
                        avg_pingtime * ping_count AS trip_duration,
                        avg_speed,
                        ST_Distance (
                            ST_SetSRID (ST_Force2D (ST_StartPoint (lines_geom)), %s),
                            ST_SetSRID (ST_Force2D (ST_EndPoint (lines_geom)), %s)
                        ) AS trip_od_distance
                FROM    quadrant_traces_1
            --end-sql
        ''' % (self.crs, self.crs, self.crs, self.crs))

        ## OUTPUT TRACES TABLE
        #Append traces to trace table where traces meet min requirements
        print(f'\n{datetime.datetime.now()} (+{time.perf_counter()} seconds)')
        print(f"Creating '{self.output_table}' traces table in database {self.pgLogin['db']}...\n")
        print(f"Creating blank '{self.output_table}' table...")
        self.db.execute('DROP TABLE IF EXISTS %s' % (self.output_table))
        self.db.execute('''
            --begin-sql
            CREATE TABLE IF NOT EXISTS %s (
                    trip_id bigint,
                    lines_geom geometry,
                    start_geom geometry,
                    end_geom geometry,
                    avg_pingtime DOUBLE PRECISION,
                    ping_count DOUBLE PRECISION,
                    trip_distance DOUBLE PRECISION,
                    trip_duration DOUBLE PRECISION,
                    avg_speed DOUBLE PRECISION,
                    trip_od_distance DOUBLE PRECISION
                    )
            --end-sql
        ''' % (self.output_table))

        print("Filtering traces in 'quadrant_traces_usable_1' into final output table '%s' by applying final filters for trace usability and plausibility..." % (self.output_table))    
        column_order = 'trip_id, lines_geom, start_geom, end_geom, avg_pingtime, ping_count, trip_distance, trip_duration, avg_speed, trip_od_distance'
        self.db.execute('''
        --begin-sql
        INSERT INTO %s (%s)
            SELECT    %s
            FROM      quadrant_traces_usable_1
            WHERE     avg_pingtime <= %d
            AND       trip_duration >= %d
            AND       trip_od_distance > 400
            AND       lines_geom IS NOT NULL
            AND       ST_GeometryType (lines_geom) = 'ST_LineString'
        --end-sql
        ''' % (self.output_table, column_order, column_order, trip_ping_avg_Var, duration_Var))

        ## Temporary Trace Table Deletion
        ## Uncomment to save disk space, otherwise leave for debugging.
        # self.db.execute('DROP TABLE tmp_withlags2')
        # self.db.execute('DROP TABLE raw_points_1')
        # self.db.execute('DROP TABLE quadrant_point4trace')
        # self.db.execute('DROP TABLE quadrant_traces_1')
        # self.db.execute('DROP TABLE quadrant_traces_usable_1')

        fend = time.perf_counter()
        print(f'{datetime.datetime.now()} ({fend} seconds)')
        print(f'____________Finished {fname} after {fend - fstart} seconds____________\n')

    def generateUniqueIDs(self):
        self.db.execute('ALTER TABLE %s DROP COLUMN trip_id;' % (self.output_table))
        self.db.execute('ALTER TABLE %s ADD COLUMN trip_id bigserial;' % (self.output_table))

    
    
    def exportAllSchemaTables(self, export_filedir, export_schema=None, export_fileext='csv', export_fileparam = None, include_header = True):
        fname = 'exportAllSchemaTables()'
        print(f'\n____________Executing {fname}____________')
        fstart = time.perf_counter()
        print(f'{datetime.datetime.now()} (+{fstart} seconds)')

        # Schema to export tables from. If None, defaults to schema in pgLogin from pgMapMatch config.
        if export_schema is None:
            export_schema = self.pgLogin['schema']
            print(f"export_schema not provided, defaulting to schema in pgLogin: '{export_schema}'")
        
        # Dictionary of accepted psql COPY file format parameters per file extension.
        # See: https://www.postgresql.org/docs/current/sql-copy.html#SQL-COPY-FILE-FORMATS
        export_fileext_fileparams = {
            'csv':'CSV',
            'bin':'BINARY',
            'txt':'TEXT'
            }

        # Decide on the fileparam based on the extension.
        if export_fileparam is None:
            if export_fileext not in export_fileext_fileparams.keys():
                raise ValueError(f"export_fileext must be one of {export_fileext_fileparams.keys()} or export_fileparam must be provided with a valid string of file parameters for the psql COPY command. \n See https://www.postgresql.org/docs/current/sql-copy.html#SQL-COPY-FILE-FORMATS for accepted file format parameters.")
            else:
                export_fileparam = export_fileext_fileparams[export_fileext]
        
        # Run the function if it exists, otherwise log an error.
        if os.path.exists(export_filedir):
            self.writeLog(f"Exporting tables from schema '{export_schema}' to '{export_filedir}' with file extension '{export_fileext}' and file parameters '{export_fileparam}'...")
            self.db.execute(f"""
                --begin-sql
                DO $$
                DECLARE
                    tbl_name text; --sql
                    cmd text; --sql
                BEGIN
                    FOR tbl_name IN (SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '{export_schema}')
                    LOOP
                        -- Construct the client-side copy command.
                        -- Replace 'C:/Temp/' with your desired local directory (ensure the directory exists).
                        -- The filename will be the table name followed by '.csv'.
                        cmd := 'COPY ' || tbl_name || ' TO ''{export_filedir}' || tbl_name || '.{export_fileext}'' WITH (FORMAT {export_fileparam}, HEADER true)'; --sql
                        
                        -- Execute the command
                        EXECUTE cmd; --sql
                    END LOOP; --sql
                END $$;
                """)
        else:
            self.writeLog(f"Export failed: directory '{export_filedir}' does not exist. Please create the directory and try again.")

        fend = time.perf_counter()
        print(f'{datetime.datetime.now()} ({fend} seconds)')
        print(f'____________Finished {fname} after {fend - fstart} seconds____________\n')
    