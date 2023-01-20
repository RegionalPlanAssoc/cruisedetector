import datetime, csv, glob
import pandas as pd
from io import StringIO
import pgMapMatch.tools as  mmt
from pgMapMatch.config import *
from cruising import *

#Set variables

h_acc_Var = 65 #max horizontal accuracy threshold
ping_tot_Var = 10 #min number of pings per device
trip_ping_tot_Var = 10 #min number of pings per trip
trip_ping_avg_Var = 90 #max average interval of pings in a trip, in seconds
speed_Var = 130 #max plausible speed between pings, in km/hr
trip_start_Var = 600 #pause in pings to start a new trip
duration_Var = 300 #min duration for a trip from start to end, in seconds


#import table to database
class importTable():
    def __init__(self,points_table,file_dir,region=None,nCores=12,schema=None,logFn=None,forceUpdate=False):
        self.table = points_table   # postgres traces table name
        self.file_dir = file_dir
        self.region=region

        self.nCores = nCores  # if None, no parallelization will be done  
        global paths
        print(file_dir)
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
        self.writeLog('\n____________Importing CSV from %s____________\n' % (self.file_dir))
    
    def writeLog(self,txt):
        assert isinstance(txt, str)
        currentTime = datetime.datetime.now().strftime("%I:%M%p %B %d, %Y")
        with open(self.logFn,'a') as f:
            f.write(currentTime+':\t: '+txt)
            print(currentTime+':\t: '+txt)

    def createTable(self):
        self.db.execute('DROP TABLE IF EXISTS %s' % (self.table))

        self.db.execute('''CREATE TABLE %s (
            device_id VARCHAR(50),id_type VARCHAR(50),latitude FLOAT,longitude FLOAT,h_acc FLOAT,
            timestamp BIGINT)''' % (self.table))

    def importCSV(self):
        #for importing compressed csvs
        csv_list = glob.glob("%s/*.gz" % (basePath+'cruising/'+self.file_dir))
        
        print(csv_list)
        #the table structure depends on the data source
        #change the headings below to match the headings for your data
        for csv_file in csv_list:
            df = pd.read_csv(csv_file,compression='gzip', header=None, sep=',', quotechar='"', names=[
                'device_id', 'id_type', 'latitude', 'longitude', 'h_acc', 'timestamp', 'ip_address', 'device_os', 'device_os_v',
                'user_agent', 'country_code', 'source_id', 'publisher_id', 'app_id', 'location_cont', 'geohash'])
            buffer = StringIO()
            df.to_csv(buffer, columns=('device_id', 'id_type', 'latitude', 'longitude', 'h_acc', 'timestamp'),
                      index=False, index_label=None, header=False)
            buffer.seek(0)
    
            self.db.copy_from(buffer, self.table)
        
        #create unique id
        self.db.execute('ALTER TABLE %s ADD COLUMN gid BIGSERIAL' % (self.table))

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
        self.db.execute('DROP TABLE IF EXISTS raw_points_1')

        self.db.execute('''CREATE TABLE raw_points_1 AS 
        SELECT *, to_timestamp(timestamp / 1000) AT TIME ZONE 'UTC' AS timestamp2, ST_SetSRID(ST_MakePointM(longitude,latitude,timestamp / 1000),4326) AS geom -- Note it's important to convert timestamp into seconds before converting to geom
        FROM %s
        WHERE h_acc < %d''' % (self.table, h_acc_Var))
        self.db.execute('''ALTER TABLE raw_points_1
                            ALTER COLUMN geom TYPE Geometry(PointM, %s) USING ST_Transform(geom,%s)'''  % (self.crs, self.crs))

    #process geocoded points into traces                            
    def processPoints(self):
        self.db.execute('DROP TABLE IF EXISTS tmp_withlags')
        
        #calculate intervals and distance between points
        self.db.execute('''CREATE TABLE tmp_withlags AS 
        SELECT *, 
        ABS(EXTRACT(EPOCH FROM t0.epoch_time) - EXTRACT(EPOCH FROM t0.lag_epoch_time)) AS time_diff_second, 
        ST_Distance(lag_geom, geom) AS distance_diff_degree FROM (SELECT *, lag(t1.geom, 1) OVER (partition by t1.device_id ORDER BY t1.epoch_time) as lag_geom, 
                                                                  lag(t1.epoch_time, 1) OVER (partition by t1.device_id ORDER BY t1.epoch_time) as lag_epoch_time 
                                                                  FROM (SELECT gid,s1.device_id,point_count,timestamp,to_timestamp(CAST(timestamp as bigint)/1000) AT TIME ZONE 'UTC' as epoch_time,geom 
                                                                        FROM raw_points_1 s1 
                                                                        LEFT JOIN 
                                                                        (SELECT device_id, COUNT(device_id) AS point_count FROM raw_points_1 GROUP BY device_id) s2 
                                                                        ON s1.device_id = s2.device_id) t1 ) t0''')
        self.db.execute('DROP TABLE IF EXISTS tmp_startpoints')
        #identify starting points for traces            
        self.db.execute('''CREATE TABLE tmp_startpoints AS 
            SELECT gid, True as startpt
            FROM tmp_withlags  
            WHERE lag_epoch_time is Null AND point_count > 1
            OR time_diff_second > %d''' % (trip_start_Var))

        self.db.execute('DROP TABLE IF EXISTS tmp_pointall')
        self.db.execute('''CREATE TABLE tmp_pointall AS
            SELECT t1.*,t2.startpt
            FROM tmp_withlags t1
            LEFT JOIN tmp_startpoints t2
            ON t1.gid = t2.gid
            ORDER BY device_id,epoch_time''')

        self.db.execute('DROP TABLE tmp_withlags')
        self.db.execute('DROP TABLE tmp_startpoints')
        
        #calculate speed between pings
        self.db.execute('DROP TABLE IF EXISTS tmp_pointdrop1')        
        self.db.execute('''CREATE TABLE tmp_pointdrop1 AS
            SELECT *, 
            CASE WHEN time_diff_second > 0 THEN (distance_diff_degree / 1000) / (time_diff_second / 3600) 
            ELSE 0 END AS speed
            FROM tmp_pointall
            WHERE
            point_count > 2
            AND (distance_diff_degree IS NULL OR distance_diff_degree <> 0)''')
        self.db.execute('DROP TABLE tmp_pointall')

        #create unique trip_id for each trace
        self.db.execute('DROP TABLE IF EXISTS tmp_pointdrop2')
        self.db.execute('''CREATE TABLE tmp_pointdrop2 AS
            SELECT *, SUM(CASE WHEN startpt is null THEN 0 ELSE 1 END) OVER (order by device_id, epoch_time) as tmp_trip_id
            FROM tmp_pointdrop1
            WHERE speed < %d''' % (speed_Var))
        self.db.execute('DROP TABLE tmp_pointdrop1')

        #count points in each trace
        self.db.execute('DROP TABLE IF EXISTS tmp_pointdrop3')
        self.db.execute('''CREATE TABLE tmp_pointdrop3 AS
            SELECT t1.*, t2.point_count2
            FROM tmp_pointdrop2 t1
            LEFT JOIN
            (SELECT tmp_trip_id, COUNT(tmp_trip_id) AS point_count2 FROM tmp_pointdrop2 GROUP BY tmp_trip_id) t2
            ON t1.tmp_trip_id = t2.tmp_trip_id''')
        self.db.execute('DROP TABLE tmp_pointdrop2')

        #filter processed points table into final points to create traces
        self.db.execute('DROP TABLE IF EXISTS quadrant_point4trace')
        self.db.execute('''CREATE TABLE quadrant_point4trace AS
            SELECT *, SUM(CASE WHEN startpt is null THEN 0 ELSE 1 END) OVER (order by device_id, epoch_time) as trip_id
            FROM tmp_pointdrop3
            WHERE point_count2 > %d''' % (trip_ping_tot_Var))
        self.db.execute('DROP TABLE tmp_pointdrop3')
        
        #re-calculate time and distance intervals between points, since some have been dropped
        self.db.execute('DROP TABLE IF EXISTS tmp_withlags2')
        self.db.execute('''CREATE TABLE tmp_withlags2 AS 
        SELECT *, 
        	ABS(EXTRACT(EPOCH FROM t0.epoch_time) - EXTRACT(EPOCH FROM t0.lag_epoch_time2)) AS time_diff_second2, 
        	ST_Distance(lag_geom2, geom) AS distance_diff_degree2 
				FROM (SELECT *, 
					  	lag(t1.geom, 1) OVER (partition by t1.device_id ORDER BY t1.epoch_time) as lag_geom2, 
						lag(t1.epoch_time, 1) OVER (partition by t1.device_id ORDER BY t1.epoch_time) as lag_epoch_time2 
						FROM (SELECT gid,s1.device_id,s1.point_count,point_count3,timestamp,epoch_time,geom,lag_geom,lag_epoch_time,time_diff_second,
							  		distance_diff_degree,startpt,speed,tmp_trip_id,point_count2,trip_id
								FROM quadrant_point4trace s1 
								LEFT JOIN 
									(SELECT device_id, 
											COUNT(device_id) AS point_count3 
											FROM quadrant_point4trace 
									 		GROUP BY device_id) s2 
								ON s1.device_id = s2.device_id) t1 ) t0''')

    #generate traces from processed and filtered points
    def generateTraces(self):
        self.db.execute('DROP TABLE IF EXISTS quadrant_traces_1')
        self.db.execute('''CREATE TABLE quadrant_traces_1 AS
        	SELECT t1.device_id, t1.trip_id, t2.avg_pingtime, t2.ping_count, t2.trip_distance, t2.trip_duration,t2.avg_speed, ST_MakeLine(t1.geom ORDER BY t1.timestamp) AS lines_geom
        	FROM tmp_withlags2 t1
        		LEFT JOIN 
        			(SELECT trip_id, 
        				SUM(time_diff_second)/(Count(time_diff_second)-1) as avg_pingtime,
        				AVG(point_count2) as ping_count,
        				SUM(distance_diff_degree) AS trip_distance,
        				ABS(EXTRACT(EPOCH FROM MAX(epoch_time)) - EXTRACT(EPOCH FROM MIN(epoch_time))) AS trip_duration,
        			 	CASE WHEN SUM(time_diff_second2) > 0 THEN SUM(distance_diff_degree2) / SUM(time_diff_second2) ELSE NULL END AS avg_speed
        			FROM tmp_withlags2
        			WHERE startpt IS NULL
        			GROUP BY trip_id) t2
        		ON t1.trip_id = t2.trip_id
        		GROUP BY t1.device_id, t1.trip_id,t2.avg_pingtime,t2.ping_count,t2.trip_distance,t2.trip_duration,t2.avg_speed
        		ORDER BY trip_id''')

        self.db.execute('DROP TABLE IF EXISTS quadrant_traces_usable_1')
        self.db.execute('''CREATE TABLE quadrant_traces_usable_1 AS
            SELECT trip_id, lines_geom, 
            ST_SetSRID(ST_Force2D(ST_StartPoint(lines_geom)),%s) AS start_geom, 
            ST_SetSRID(ST_Force2D(ST_EndPoint(lines_geom)),%s) AS end_geom, 
            avg_pingtime, ping_count, trip_distance, 
            avg_pingtime * ping_count AS trip_duration,
            avg_speed,
            ST_Distance(ST_SetSRID(ST_Force2D(ST_StartPoint(lines_geom)),%s), ST_SetSRID(ST_Force2D(ST_EndPoint(lines_geom)),%s)) AS trip_od_distance
            FROM quadrant_traces_1''' % (self.crs, self.crs, self.crs, self.crs))

        #Append traces to trace table where traces meet min requirements
        #self.db.execute('DROP TABLE IF EXISTS %s' % (self.output_table))
        self.db.execute('''CREATE TABLE IF NOT EXISTS %s (
            trip_id bigint, 
            lines_geom geometry, 
            start_geom geometry, 
            end_geom geometry, 
            avg_pingtime DOUBLE PRECISION, 
            ping_count DOUBLE PRECISION, 
            trip_distance DOUBLE PRECISION, 
            trip_duration DOUBLE PRECISION,
            avg_speed DOUBLE PRECISION,
            trip_od_distance DOUBLE PRECISION)''' % (self.output_table))

        self.db.execute('''INSERT INTO %s 
            SELECT *
            FROM quadrant_traces_usable_1
            WHERE avg_pingtime <= %d AND trip_duration >= %d AND trip_od_distance > 400''' % (self.output_table, trip_ping_avg_Var, duration_Var))
                    

        self.db.execute('DROP TABLE tmp_withlags2')
        self.db.execute('DROP TABLE raw_points_1')
        self.db.execute('DROP TABLE quadrant_point4trace')
        self.db.execute('DROP TABLE quadrant_traces_1')
        self.db.execute('DROP TABLE quadrant_traces_usable_1')

    def generateUniqueIDs(self):
        self.db.execute('ALTER TABLE %s DROP COLUMN trip_id;' % (self.output_table))
        self.db.execute('ALTER TABLE %s ADD COLUMN trip_id bigserial;' % (self.output_table))


