#!/usr/bin/python3
"""
Estimate the prevalence of cruising in a dataset of GPS traces
Code by: Adam Millard-Ball and Ellis Calvin
Method developed by: Adam Millard-Ball, Rachel Weinberger, and Robert Hampshire
Support from: Federal Highway Administration
May 2022

For details, see  https://doi.org/10.1016/j.trc.2020.102781
"""

import sys, os, platform, time, subprocess, datetime, math
global defaults
if sys.version_info < (3, 0):
    sys.stdout.write("Sorry, requires Python 3. You are running Python 2.\n")
    sys.exit(1)
from pathlib import Path

""" 
Defaults that the user should change
"""
# 1. Basepath and output folder for log
basePath = 'G:/Shared drives/Projects/3090_Cruising for Parking/Data/testing/'

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
osm2poPath = 'C:/Downloads/'
osm2poVersion = '5.5.1'

# 7. Location of mapmatching coefficient file (in this git repo). You shouldn't need to change this.
# https://stackoverflow.com/questions/3718657/how-do-you-properly-determine-the-current-script-directory
repoPath = Path(globals().get("__file__", "./_")).absolute().parent 
coeffFn = str(repoPath) + '/mapmatching_coefficients.txt'

# 8. Specify number of processing cores to be used
cores = 4

import numpy as np
import pandas as pd
import multiprocessing
from collections import OrderedDict, defaultdict
import pgMapMatch.mapmatcher as mm
import pgMapMatch.tools as  mmt
from pgMapMatch.config import *
#from cruising_importLocationData import *
# from cruising_setup import *

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# key parameters
maxDistThres = 1400 # maximum distance that the trace can get from the endpoint, once it enters the 400m buffer
rollSecs = 30       # speed threshold for identification of walking segment at end of trip, and number of secs over which speed is calculated
wSpeed = 6          # assumed maximum walking speed (km/h)
bufferThresh = 0.5  # to be considered cruising, at least this fraction of the last portion of the trip must be within the 400m buffer
qualityCutoff = 0.9 # To be retained, a trip must have at least this probability of being good 
r = '400'           # the buffer radius (meters)
rd = str(int(r)*2)  # radius of donut
mapmatch_timeout  = 300        # timeout for each individual postgres query, in seconds. Making it shorter will skip long and stubborn traces

def loadTables(region=None):
    """
    Loads all the base tables
    Reprojects if needed
    SF is state plane zone III (NAD 83), EPSG 3493 (meters), 3494 (ft)
    """
    if region is not None: assert region in defaults['regions']
    regions = defaults['regions'] if region is None else [region]
    
    db = mmt.dbConnection(pgLogin=pgInfo)
    engine = mmt.getPgEngine(pgInfo)

    for region in regions:    
        for table in ['osm_2po_4pgr', 'osm_2po_vertex', 'turn_restrictions', 'streets', 'curblines', 'off_street']:
            db.execute('DROP TABLE IF EXISTS %s_%s;' % (region,table))
        if region=='sf':
            for table in ['sfpark_blocks','sf_meters','sensors', 'pr_full_predictions']:
                db.execute('DROP TABLE IF EXISTS %s;' % (table))
    
    """
    1. Load OSM street network using osm2po
    
    osm2po downloaded June 9, 2015 from osm2po.de
        See http://planet.qgis.org/planet/tag/osm2po/
        Make following changes to osm2po config file (for the first one, see http://gis.stackexchange.com/questions/41393/does-osm2po-take-into-consideration-turn-restrictions)
            1. postp.1.class = de.cm.osm2po.plugins.postp.PgVertexWriter 
            2. graph.build.excludeWrongWays = true
    """
    os.chdir(osm2poPath)
    
    osmDict = {'sf':'san-francisco_california.osm.pbf', 'mi':'michigan-latest.osm.pbf', 'ca':'california-latest.osm.pbf', 'wa':'washington-latest.osm.pbf'}
    for region in regions:
        assert os.system("java -Xmx5g -jar %sosm2po-%s/osm2po-core-%s-signed.jar tileSize=x cmd=c prefix=%s_osm %s" % (osm2poPath, osm2poVersion, osm2poVersion, region, osm2poPath+osmDict[region]))==0

    # table of streets
    for region in regions:
        st_table = region+'_streets'
        print ('Loading streets for %s into table %s' % (region, st_table))
        assert os.system("""psql -d %s -h %s -U %s -q -f %s%s_osm/%s_osm_2po_4pgr.sql""" % (pgInfo['db'], pgInfo['host'], pgInfo['user'], osm2poPath, region, region))==0
        # table of turn restrictions
        assert os.system("psql -d %s -h %s -U %s -q -f %s%s_osm/%s_osm_2po_vertex.sql" % (pgInfo['db'], pgInfo['host'], pgInfo['user'], osm2poPath, region, region))==0
        #assert os.system("rm -r %s%s_osm" % (osm2poPath, region))==0

        # rename and project to 3494
        db.fix_permissions_of_new_table('%s_osm_2po_4pgr' % region)
        db.fix_permissions_of_new_table('%s_osm_2po_vertex' % region)
        db.execute("ALTER TABLE %s_osm_2po_4pgr RENAME TO %s;" % (region, st_table))
        db.execute("ALTER TABLE %s ALTER COLUMN geom_way TYPE Geometry(LineString, %s) USING ST_Transform(geom_way, %s);" % (st_table, crs[region], crs[region]))
        db.execute("ALTER TABLE %s_osm_2po_vertex ALTER COLUMN geom_vertex TYPE Geometry(Point, %s) USING ST_Transform(geom_vertex, %s);" % (region, crs[region], crs[region]))
    
    
    for region in regions:
        st_table = region+'_streets'
        rest_table = region+'_turn_restrictions'
        db.execute('CREATE INDEX %s_spidx ON %s USING GIST (geom_way);' % (st_table, st_table))
        db.execute('CREATE UNIQUE INDEX %s_idx ON %s (id);' % (st_table, st_table))
        db.execute('CREATE INDEX %s_source_idx ON %s (source);' % (st_table, st_table))
        db.execute('CREATE INDEX %s_target_idx ON %s (target);' % (st_table, st_table))

        # parse the turn restrictions table, and recreate this as a table suitable for pg routing
        # see http://gis.stackexchange.com/questions/41393/does-osm2po-take-into-consideration-turn-restrictions
        #   and http://gis.stackexchange.com/questions/69424/is-there-any-working-example-for-pgr-trsp
        restrictions = db.execfetch('''SELECT restrictions, array_agg(r.id::text) AS edge_ids
                                             FROM %s_osm_2po_vertex AS n, %s AS r
                                                WHERE restrictions IS NOT NULL
                                                AND (r.source=n.id OR r.target=n.id)
                                            GROUP BY n.id, restrictions;''' % (region, st_table))
        noTurns = []
        for restriction, edge_ids in restrictions:
            # get list of pairs of edges with + or - prefix
            rList = restriction.replace('+', ' +').replace('-', ' -').split()
            # no turn restrictions are easy
            noTurns+= [tuple(rr.strip('-').split('_')) for rr in rList if rr.startswith('-')]
            # for only turns, we need to have restrictions for all other edges
            for rr in [ss for ss in rList if ss.startswith('+')]:
                e1, e2 = rr.strip('+').split('_')
                noTurns+=[(e1, ee) for ee in edge_ids if ee!=e1 and ee!=e2]

        # dtype and field names must match what pgr_trsp expects
        noTurns = pd.DataFrame(noTurns, columns=['source_id', 'target_id'])
        noTurns.target_id = noTurns.target_id.astype('int32')
        noTurns['to_cost'] = 100000
        noTurns.to_sql(rest_table, engine, schema=pgInfo['schema'], if_exists='replace', index=True)
        db.fix_permissions_of_new_table(rest_table)
        db.execute('DROP TABLE %s_osm_2po_vertex' % region)
    
    
    
    print ("Done loading tables")
    return




class traceTable():
    def __init__(self, table, region='ca', nCores=cores, schema='public', logFn=None, forceUpdate=False):
        """
        table: name of the Postgres table with the GPS traces
        region: the prefix of the streets and other input tables (e.g. ca_streets)
        nCores: the number of parallel processing cores. If None, no parallelization will be done  
        schema: the Postgres schema where the above tables are contained
        logFn: name of the output log file 
        forceUpdate: if True, the analysis will run from scratch, e.g. deleting the tables and columns already created
                     Otherwise, it will try and pick up from where it left off (but this can be unstable)
        """
        self.table = table
        self.region = region
        self.nCores = nCores 
        try:
            self.srs = crs[self.region]
        except:
            raise Exception('Coordinate reference system for this region not set.\nPlease add your region to the crs dictionary in cruising.py')

        self.streets = self.region+'_streets'

        # optional tables
        self.curblinesName = self.region+'_curblines'
        self.offstreetName = self.region+'_off_street'

        self.logFn = logPath+'/'+self.table+'_log.log' if logFn is None else logFn
        if 'pgLogin' not in globals(): # initialize connection
            global pgLogin  # make it available for parallel instances
            pgLogin = mmt.getPgLogin(user=pgInfo['user'], db=pgInfo['db'], host=pgInfo['host'], requirePassword=pgInfo['requirePassword'], forceUpdate=False)
            pgLogin['schema'] = schema
        self.pgLogin = pgLogin # shouldn't be necessary
        self.db = mmt.dbConnection(pgLogin=pgLogin, logger=logFn)
        self.forceUpdate = forceUpdate
        self.ids = None
        self.nPings = None

        if schema!=mm.pgInfo['schema']:
            raise Warning('The schema in your pgMapMatch config file is {}. This does not match the schema passed to cruising.py: {}.\nThis may cause problems - please check!'.format(mm.pgInfo['schema'], schema))

        if table not in self.db.list_tables():
            raise Exception('''Cannot find the trace table {}\nIt should be in the public schema, or which ever schema you specify'''.format(table))

        if self.streets not in self.db.list_tables():
            raise Exception('''Cannot find the streets table {}\nIt should be in the public schema, or which ever schema you specify'''.format(self.streets))

        if self.streets not in self.db.list_tables():
            raise Exception('''Cannot find the turn restrictions table {}_turn_restrictions. \nIt should be in the public schema, or which ever schema you specify'''.format(self.region))


        requiredCols = ['trip_id', 'lines_geom', 'end_geom']
        if not all([cc in self.db.list_columns_in_table(table) for cc in requiredCols]):
            raise Exception('Missing column from {}. These columns are required: {}'.format(table, ','.join(requiredCols)))

        # ensure index completeness
        for tn, idx in [(self.streets, 'id'), (self.table, 'trip_id')]:
            self.db.execute('CREATE INDEX IF NOT EXISTS {tn}_{idx}_idx ON {tn} ({idx});'.format(idx=idx, tn=tn))

        self.writeLog('\n____________PROCESSING TRACES table %s____________\n' % (self.table))
   
    def writeLog(self, txt):
        """
        Writes txt to the log file, with the timestamp
        """ 
        assert isinstance(txt, str)
        currentTime = datetime.datetime.now().strftime("%I:%M%p %B %d, %Y")
        with open(self.logFn,'a') as f:
            f.write(currentTime+':\t: '+txt)
            print(currentTime+':\t: '+txt)
            
    def getIds(self):
        """Gets the ids of each trip (i.e., GPS trace), if they don't already exist in self.ids"""
        if self.ids is None:
            ids = self.db.execfetch('SELECT trip_id FROM %s' % (self.table))
            self.ids = sorted([ii[0] for ii in ids])   
        return self.ids
        
    def getNPings(self,geom='lbuff_geom'):
        """Get the number of GPS pings within the buffer for each trip"""
        result = self.db.execfetch('SELECT trip_id, COALESCE(ST_NPoints(%s),0) FROM %s ORDER BY trip_id;' % (geom, self.table))
        self.nPings = OrderedDict((rr[0],rr[1]) for rr in result)
        return self.nPings

    def dropErrantPings(self):
        if 'lines_original' in self.db.list_columns_in_table(self.table):
            if self.forceUpdate: 
                self.db.execute('UPDATE %s SET lines_geom=lines_original' % (self.table))
                self.db.execute('ALTER TABLE %s DROP COLUMN lines_original' % (self.table))
                self.db.execute('ALTER TABLE %s DROP COLUMN IF EXISTS lines_tmp' % (self.table))
            else:
                self.writeLog('Errant pings already dropped. Skipping')
                return
        self.db.execute("SELECT AddGeometryColumn('%s','lines_original',%s,'LineStringM',3);" % (self.table, self.srs))
        self.db.execute("SELECT AddGeometryColumn('%s','lines_tmp',%s,'LineStringM',3);" % (self.table, self.srs))
        
        # Drop first point of lines where the 'true' starting point exists. This is because GPS error is highest with the first point
        self.db.execute('UPDATE %s SET lines_original = lines_geom' % self.table)
        if 'start_good' in self.db.list_columns_in_table(self.table): # only for SL traces
            self.db.execute('''UPDATE %s SET lines_tmp = 
                               CASE WHEN start_good is True AND ST_NPoints(lines_geom)>2 THEN ST_RemovePoint(lines_geom, 0)
                               ELSE lines_geom END;''' % self.table)
        
            # Drop pings that are over a speed threshold
            tc = mm.traceCleaner(self.table,'trip_id','lines_tmp', 'lines_geom', logFn=None)  # don't log because file is large!        
            tc.fetchAndDrop()
            self.db.execute('ALTER TABLE %s DROP COLUMN lines_tmp;' % self.table)
        else:
            tc = mm.traceCleaner(self.table,'trip_id','lines_original', 'lines_geom', logFn=None)
            tc.fetchAndDrop()
        
    def createLotPolygons(self):
        """Create temporary feature for off-street parking lots and service roads, 
        so that we can exclude them from the end of the trip"""
        self.writeLog('Creating lot polygons')
        if 'lotpolygons' in self.db.list_tables():
            if self.forceUpdate:
                self.db.execute('DROP TABLE IF EXISTS lotpolygons')
            else:
                self.writeLog('lotpolygons table already exists. Skipping')
                return
        
        if self.offstreetName in self.db.list_tables():
            offst_sql = 'SELECT ST_Union(ST_Buffer(geom,10)) As uniongeom FROM {} UNION'.format(self.offstreetName)
        else:
            offst_sql = ''
            self.writeLog('Warning: (optional) off-street parking table {} not found'.format(self.offstreetName))
        streetsClip = '''SELECT * FROM %(sts)s
							WHERE ST_Within(geom_way, (SELECT ST_SetSRID(ST_Extent(lines_geom), %(srs)s) as table_extent FROM sampletraces))''' % {'sts':self.streets, 'srs':self.srs}


        cmd = '''CREATE TABLE lotpolygons AS 
                 (SELECT ST_Difference(lotgeom_big, streetbuffer) AS lotgeom FROM
                    (SELECT ST_Union(uniongeom) AS lotgeom_big FROM
                        (%(offst_sql)s
                         SELECT ST_Buffer(ST_Collect(geom_way), 10) AS uniongeom FROM (%(sts)s) AS streets WHERE clazz=51) t1) t2,
                    (SELECT ST_Buffer(ST_Collect(geom_way), 5) AS streetbuffer FROM (%(sts)s) AS streets WHERE clazz!=51) t3);
                    ''' % {'offst_sql':offst_sql, 'sts':streetsClip}
        self.db.execute(cmd)
        self.db.create_indices('lotpolygons', geom='lotgeom')
    
    def truncateAllLines(self):
        """Wrapper for truncateLine()
        It is too slow to truncate all lines at once, so we loop over each trace
        It populates a pandas dataframe with the metrics for each trace
        and then uploads the whole dataframe at once
        For some reason (why?) this is more efficient that doing it within postgres
        """ 

        self.writeLog('Truncating all lines to buffer')
        self.writeLog('...getting geometries')
        colNames  = ['npings','id_first', 'id_firstx2', 'id_walk', 'id_park', 'maxspeed', 'speed', 'donutspeed', 'walkspeed', 
                    'pingtime_meanall', 'pingtime_maxall', 'pingtime_meanbuf', 'pingtime_maxbuf', 'npingsbuf', 'npingsdonut', 'npingswalk', 'pingtime_meanwalk', 
                    'pt_maxwalk', 'npingspark']
        vectNames = ['lbuff_geom','lineswalk_geom','lineslot_geom','linesall_geom','startpt_geom', 'enterlot_geom','park_geom']
        currentCols = self.db.list_columns_in_table(self.table)
        if any([cc in currentCols for cc in colNames+vectNames]):
            if self.forceUpdate: 
                dropTxt = ', '.join(['DROP COLUMN IF EXISTS '+cc for cc in colNames+vectNames])
                self.db.execute('ALTER TABLE %s %s;' % (self.table, dropTxt))
            else:
                self.writeLog('Lines already truncated. Skipping')
                return
        nPings = self.getNPings('lines_geom')

        # this is the heart of the function - loop over ides to populate the dataframe
        ids = [ii for ii in self.getIds() if nPings[ii]>1]
        if self.nCores is None: # do in serial
            df = pd.DataFrame([self.truncateLine(id) for id in ids], columns=['trip_id']+colNames).set_index('trip_id')
        else:
            dbtmp = self.db  # can't pass a pyscopg2 object to multiprocessing :(
            self.db = None 
            df = pd.DataFrame(apply_multiprocessing(self.truncateLine, ids, self.nCores)).T
            df.columns=['trip_id']+colNames
            for col in df.columns:
                dt = 'int64' if col=='trip_id' else 'float64'
                try:
                    df[col] = df[col].astype(dt)
                except:
                    print('did not convert column  {}'.format(col))
            df.set_index('trip_id', inplace=True)
            self.db = dbtmp # restore the connection 
        self.writeLog('...writing to database')
        self.db.update_table_from_array(df,self.table,joinOnIndices=True)

        # Now use the ping id information to extract the relevant portion of the linestring
        self.db.execute('DROP TABLE IF EXISTS %s_tmpmerge;' % self.table)  
        self.writeLog('...writing temporary table')
        cmd = '''CREATE TABLE %(table)s_tmpmerge AS 
                 WITH allpts AS (SELECT trip_id, id_first, id_park, id_walk, ST_DumpPoints(lines_geom) AS dp FROM %(table)s)
                 SELECT t1.trip_id, lbuff_geom, lineswalk_geom, lineslot_geom, linesall_geom,
                        ST_StartPoint(lbuff_geom)  AS startpt_geom, ST_EndPoint(lbuff_geom) AS enterlot_geom,
                        ST_EndPoint(lineslot_geom) AS park_geom
                 FROM
                    (SELECT trip_id, ST_MakeLine((dp).geom ORDER BY (dp).path[1]) AS lbuff_geom
                        FROM allpts WHERE (dp).path[1]>=id_first AND (dp).path[1]<=id_park
                        GROUP BY trip_id) As t1,
                    (SELECT trip_id, ST_MakeLine((dp).geom ORDER BY (dp).path[1]) AS lineslot_geom
                        FROM allpts WHERE (dp).path[1]>=id_park AND (dp).path[1]<=id_walk
                        GROUP BY trip_id) As t2,
                    (SELECT trip_id, ST_MakeLine((dp).geom ORDER BY (dp).path[1]) AS lineswalk_geom
                        FROM allpts WHERE (dp).path[1]>=id_walk
                        GROUP BY trip_id) As t3,
                    (SELECT trip_id, ST_MakeLine((dp).geom ORDER BY (dp).path[1]) AS linesall_geom
                        FROM allpts WHERE (dp).path[1]>=id_first
                        GROUP BY trip_id) As t4
                  WHERE t1.trip_id = t2.trip_id AND t1.trip_id = t3.trip_id AND t1.trip_id = t4.trip_id;
                ''' % {'table':self.table}  
        self.db.execute(cmd)  

        # if lineslot_geom is Null (no park segment), park_geom is the same as enterlot_geom
        cmd = '''UPDATE %s_tmpmerge SET park_geom = enterlot_geom WHERE park_geom IS Null;''' % (self.table)
        self.db.execute(cmd)
        
        self.writeLog('...merging')
        self.db.merge_table_into_table(self.table+'_tmpmerge', self.table, 'trip_id')
        self.db.execute('DROP TABLE %s_tmpmerge;' % self.table)  
        
        for geom in ['startpt_geom','park_geom','enterlot_geom','lbuff_geom']:
            self.db.create_indices(self.table, geom=geom)
        self.writeLog('...done')
 
    def truncateLine(self, id):
        """Extract the portion of linestring after it enters the 400m buffer
        We can't just do an intersect, because the travel path might go out of the buffer afterwards 
        Also take the opportunity to calculate lots of related metrics"""

        # Get dataframe into pandas. This is more flexible than SQL. 
        try:
            db = mmt.dbConnection(pgLogin=self.pgLogin, verbose=False) # thread safe for parallelization
            cmd = '''SELECT (dp).path[1] AS ptid, ST_M((dp).geom) AS pingtime,
                        ST_Distance(end_geom, (dp).geom) AS disttoend,
                        ST_Intersects(lotgeom, (dp).geom) AS in_lot,
                        (ST_Distance((dp).geom, lag((dp).geom, 1) OVER (PARTITION BY trip_id ORDER BY (dp).path[1]))) AS distdelta,
                        (ST_Distance((dp).geom, lag((dp).geom, 2) OVER (PARTITION BY trip_id ORDER BY (dp).path[1])))::float AS distdelta2,
                        (ST_M((dp).geom) - lag(ST_M((dp).geom), 1) OVER (PARTITION BY trip_id ORDER BY (dp).path[1])) AS timedelta,
                        (ST_M((dp).geom) - lag(ST_M((dp).geom), 2) OVER (PARTITION BY trip_id ORDER BY (dp).path[1])) AS timedelta2
                        FROM (SELECT trip_id, end_geom, ST_DumpPoints(lines_geom) AS dp 
                                    FROM %s WHERE trip_id=%s) AS t1, lotpolygons;''' % (self.table, id)
            pointsDf = db.execfetchDf(cmd) 
            pointsDf['timestamp'] = pd.to_datetime(pointsDf.pingtime.apply(lambda x: np.nan if pd.isnull(x) else datetime.datetime.fromtimestamp(x)))
        
            # Smooth out distances for high-resolution traces
            pointsDf.loc[(pointsDf.timedelta==1) & (pointsDf.timedelta2==2), 'distdelta'] = pointsDf.distdelta2.astype(float)
        
            # Create lagged values to calculate rolling speed
            # idea is to smooth the speeds so that the speeds are less an artefact of GPS error
            if len(pointsDf)==1:
                return [id,1]+[np.nan]*18

            pointsDf.set_index('timestamp', inplace=True)       
            if not(pointsDf.index.is_unique):  # some pings have same timestamp, so group by that
                pointsDf = pointsDf.groupby(level=0).agg({'ptid':max, 'disttoend':max, 'in_lot':min, 'timedelta':sum, 'distdelta':sum})
            pointsDf.loc[pointsDf.timedelta==0, 'timedelta'] = np.nan # first entry can be zero in pathological cases
                    
            rollDist = pointsDf.distdelta.resample('S').mean().rolling(min_periods=1,window=rollSecs).sum().reindex(pointsDf.index)
            rollTime = pointsDf.timedelta.resample('S').mean().rolling(min_periods=1,window=rollSecs).sum().reindex(pointsDf.index)
              
            rollSpeed =  rollDist/1000./rollTime*60*60
            rollSpeed.loc[pointsDf.timedelta.cumsum()<rollSecs] = np.nan  # gets rid of speeds that are too high because only a few secs are included
            pointsDf['rollspeed'] = rollSpeed
            pointsDf['speed'] = pointsDf.distdelta/pointsDf.timedelta/1000.*60*60 # speed at each ping
        
            # Calculate metrics for the trip as a whole
            id_first = pointsDf[pointsDf.disttoend<=int(r)].ptid.min() # id of first point that is within the buffer
            id_first = max(1, id_first-1) # get the point before it, so we capture the whole length within 400m
            id_firstx2 = pointsDf[pointsDf.disttoend<=int(r)*2].ptid.min() # id of first point that is within the donut
            id_firstx2 = max(1, id_firstx2-1) 
                
            # let's also define id_walk as the portion within the parking lot
            id_walk  = pointsDf[pointsDf.rollspeed>wSpeed].ptid.max() # id of point that marks the transition to walk
            if pd.isnull(id_walk): id_walk = 1   # entire trace is within walking distance
        
            tmpDf = pointsDf.loc[pointsDf.ptid<=id_walk,['in_lot','ptid']].loc[::-1] # note the [::-1] is to reverse the order
            id_park= tmpDf[tmpDf.in_lot.cummin()==True].ptid.min()
            if pd.isnull(id_park) or id_park>id_walk: id_park = id_walk   

            bufMask = (pointsDf.ptid>id_first) & (pointsDf.ptid<=id_walk)   # points within 400m and until the walk segment starts
            donutMask = (pointsDf.ptid>id_firstx2) & (pointsDf.ptid<=id_first+1)   # points within 800m and until the 400m radius starts. 
            walkMask = pointsDf.ptid>id_walk
            parkMask = (pointsDf.ptid>id_park) & (pointsDf.ptid<=id_walk)
        
            # sampling resolution (pt = pingtime)
            pt_mean = pointsDf.timedelta.mean()
            pt_max  = pointsDf.timedelta.max()
            npings, npingsbuf, npingsdonut, npingswalk, npingspark = len(pointsDf), bufMask.sum(), donutMask.sum(), walkMask.sum(), parkMask.sum()

            pt_meanbuf = pointsDf[bufMask].timedelta.mean()
            pt_maxbuf  = pointsDf[bufMask].timedelta.max()
            pt_meanwalk = pointsDf[walkMask].timedelta.mean()
            pt_maxwalk = pointsDf[walkMask].timedelta.max()
            maxspeed   = pointsDf[bufMask].speed.max() # max speed in 400m buffer
            speed      = pointsDf[bufMask].speed.mean()
            donutspeed = pointsDf[donutMask].speed.mean()
            walkspeed  = pointsDf[walkMask].speed.mean() if walkMask.sum()>0 else 'Null'
            
            return [id, npings, id_first, id_firstx2, id_walk, id_park, maxspeed, speed, donutspeed, walkspeed, pt_mean, pt_max, pt_meanbuf, pt_maxbuf, npingsbuf, npingsdonut, npingswalk, pt_meanwalk, pt_maxwalk, npingspark]
        except:
            print('Failed on id {}'.format(id))
            return [id]+[np.nan]*19

    def addTimeStamps(self):
        """Calculate basic data on time/date of endpoint"""
        cols = [('endtime', 'timestamp with time zone'), ('endhour', 'int'), ('endminute', 'int'),
                ('weekday', 'boolean')]
        if 'endtime' in self.db.list_columns_in_table(self.table):
            if self.forceUpdate: 
                dropTxt = ', '.join(['DROP COLUMN IF EXISTS '+cc[0] for cc in cols])
                self.db.execute('ALTER TABLE %s %s;' % (self.table, dropTxt))
            else:
                self.writeLog('Timestamps already added. Skipping')
                return

        self.db.addColumns(cols, self.table, dropOld=True)
        
        self.db.execute('UPDATE %s SET endtime = to_timestamp(ST_M(ST_EndPoint(lbuff_geom)));' % (self.table))
        
        self.db.execute('UPDATE %s SET endhour = EXTRACT(hour FROM endtime), endminute = EXTRACT(minute FROM endtime)' % (self.table))
        self.db.execute('UPDATE %s SET weekday = False;' % self.table)
        self.db.execute('UPDATE %s SET weekday = True WHERE EXTRACT(dow FROM endtime)>0 AND EXTRACT(dow FROM endtime)<6;' % self.table)
    
        # now reverse that for holidays.
        # only metered holidays are New Year, Thanksgiving and Christmas
        cmd = '''UPDATE %s SET weekday = False 
                        WHERE to_char(endtime, 'MM-DD') in ('01-01', '12-25') 
                              OR to_char(endtime, 'YY-MM-DD') IN 
                              ('2013-11-28', '2014-11-27', '2015-11-26', '2016-11-24', '2017-11-23', 
                               '2018-11-22', '2019-11-28', '2020-11-26', '2021-11-25', '2022-11-24', 
                               '2023-11-23', '2024-11-28', '2025-11-27')''' % self.table
        self.db.execute(cmd)

    def mapMatchinParallel(self, chunksize=1000):
        """Parallelized version of self.mapMatch()"""
        if 'matched_line' in self.db.list_columns_in_table(self.table) and not self.forceUpdate:
            self.writeLog('Map matched geometry column already exists. Skipping')
            return        
        newCols = ['matched_line', 'lbuff_geom_cleaned', 'edge_ids','match_score']
        for col in newCols:
            self.db.execute('ALTER TABLE {} DROP COLUMN IF EXISTS {};'.format(self.table, col))

        # There are economies of scale in a mapmatcher instance, so split into chunks of chunksize
        nPings = self.getNPings()
        chunkIndices = np.array_split(list(nPings.keys()), math.ceil(len(nPings)/chunksize))
        # list of OrderedDicts, each of which will be passed to self.mapMatch()
        subDicts = [OrderedDict((rr,nPings[rr]) for rr in chunk ) for chunk in chunkIndices ]
        print('Starting parallel mapmatching')

        # need to pre-create the columns, because otherwise the different parallel threads will get confused as to who is doing it
        self.db.execute("SELECT AddGeometryColumn('%s','matched_line',%s,'LineString',2);" % (self.table, self.srs))
        self.db.execute("SELECT AddGeometryColumn('%s','lbuff_geom_cleaned',%s,'LineStringM',3);" % (self.table, self.srs))
        self.db.addColumns([('edge_ids', 'int[]'), ('match_score', 'real')], self.table)

        result = apply_multiprocessing(mapMatch_wrapper, zip(subDicts, [self.streets]*len(subDicts),[self.table]*len(subDicts), [self.db.default_schema]*len(subDicts)), self.nCores)
        failed_chunks = [ii for ii, rr in result.items() if rr!=0]
        if len(failed_chunks)==0:
            print('All mapmatching chunks succeeded!')
        else:
            print('Redoing {} of {} failed chunks in serial'.format(len(subDicts), len(failed_chunks)))
            for ii in failed_chunks: 
                result = mapMatch_wrapper(subDicts[ii], self.streets, self.table)
                success = 'succeeded' if result==0 else 'failed'
                print('Chunk {} {}'.format(ii, success))

    def mapMatchinSerial(self):
        # create a db connection object with a timeout    
        mmtdb = mmt.dbConnection(pgLogin=self.pgLogin, timeout=mapmatch_timeout, verbose=False)

        mapMatcher = mm.mapMatcher(self.streets, self.table, 'trip_id', 'lbuff_geom', db=mmtdb, verbose=False, cleanedGeomName='lbuff_geom_cleaned',qualityModelFn='mapmatching_coefficients.txt')
        mapMatcher.db.verbose=False
        if 'matched_line' in mmtdb.list_columns_in_table(self.table) and not self.forceUpdate:
            self.writeLog('Map matched geometry column already exists. Skipping')
            return -1
        nPings = self.getNPings()
        assert isinstance(nPings, OrderedDict)
        
        starttime=time.time()
        for ii,id in enumerate(nPings):
            if nPings[id]>=3:  # need at least 3 points to match a trace
                if ii%100==0: self.writeLog('Matching trace %s (#%d of %d)' % (id,ii,len(nPings)))
                try:
                    mapMatcher.matchPostgresTrace(id)
                    if mapMatcher.matchStatus==0:
                        mapMatcher.writeMatchToPostgres()
                except Exception as e:
                    self.writeLog('***FAILED ON trace %s (#%d of %d)***' % (id,ii,len(nPings)))
                    self.writeLog(str(e))
            else:
                self.writeLog('Cannot map match trace %s - too few points' % (id))

        print('Mapmatching took %d seconds, of which:' % (time.time()-starttime))
        for k,v in mapMatcher.timing.items(): 
            if k!='median_times': print('\t%s: %d seconds' % (k,v))
    
        return 0
                
    def addMapMatchedSupplementaryData(self):
        """Given the map matched results, add new columns that will speed subsequent processing"""
        # add quality columns
        mapMatcher = mm.mapMatcher(self.streets, self.table, 'trip_id', 'lbuff_geom', db=self.db, cleanedGeomName='lbuff_geom_cleaned',qualityModelFn=coeffFn)
        mapMatcher.addQualityColumns(['pingtime_max','pingtime_mean','gpsdist','matchdist'], forceUpdate=True)

        # add supplementary data
        cols = [('edge_id_end','int'),('block_ids', 'text[]'),('ids_repeat', 'int')]

        if 'edge_id_end' in self.db.list_columns_in_table(self.table):
            if self.forceUpdate: 
                dropTxt = ', '.join(['DROP COLUMN IF EXISTS '+cc[0] for cc in cols])
                self.db.execute('ALTER TABLE %s %s;' % (self.table, dropTxt))
            else:
                self.writeLog('Map matched supplementary data already added. Skipping')
                return
                
        self.db.addColumns(cols, self.table, skipIfExists=True)
        
        # add repeated ids
        self.db.execute('UPDATE %s SET ids_repeat=0 WHERE array_length(edge_ids, 1)>0' % self.table)
        cmd = '''UPDATE %(table)s t SET ids_repeat=t4.ids_repeat FROM (
                    SELECT trip_id, SUM(edgecount) AS ids_repeat FROM (
                        SELECT trip_id, count(*) AS edgecount FROM 
                             (SELECT trip_id, unnest(edge_ids) AS eid FROM %(table)s) t2
                        GROUP BY trip_id, eid) t3
                    WHERE  edgecount>1 GROUP BY trip_id) t4
                 WHERE t.trip_id=t4.trip_id''' % {'table':self.table}
        self.db.execute(cmd)
        
        # add id of last block
        self.db.execute('UPDATE %s SET edge_id_end=edge_ids[array_length(edge_ids, 1)];' % self.table)
        
        return

    def calcAllNetworkDistances(self):
        if 'netwkdist' in self.db.list_columns_in_table(self.table):
            if self.forceUpdate: 
                self.db.execute('ALTER TABLE %s DROP COLUMN netwkdist;' % (self.table))
            else:
                print('Network distances already calculated. Skipping')
                return
        
        # avoid calculating network distance for trips where map-matching failed
        ids = self.db.execfetch('SELECT trip_id FROM %s WHERE matched_line IS NOT Null;' % (self.table))
        ids = sorted([ii[0] for ii in ids])

        if self.nCores is None:
            df = pd.DataFrame([self.calcNetworkDistance(id) for id in ids], columns=['trip_id','netwkdist']).set_index('trip_id')
        else:  # in parallel
            dbtmp = self.db  # can't pass a pyscopg2 object to multiprocessing :(
            self.db = None 
            result = apply_multiprocessing(self.calcNetworkDistance, ids, self.nCores)
            #df = pd.DataFrame(result.values(), columns=['trip_id','netwkdist']).set_index('trip_id') # not robust to failures
            df = pd.DataFrame(result, index=['trip_id','netwkdist']).T
            df_failed = df[df.trip_id==-1]
            df = df[df.trip_id!=-1]  # these are trips that failed
            print('{} trips out of {} failed. Here are the first few:'.format(len(df_failed), len(df)))
            print(df_failed.head())
            df.set_index('trip_id', inplace=True)

            self.db = dbtmp # restore the connection 
        self.db.update_table_from_array(df,self.table,joinOnIndices=True)
        self.db.execute('DROP TABLE tmp_for_insertion_%s' % self.table)
        # some errors
        self.db.execute('UPDATE %s SET netwkdist=Null WHERE netwkdist>1e6' % self.table)

    def calcNetworkDistance(self,id):
        """Returns network distance from start edge to end edge"""
        # Uses tsrp (the second SELECT in pgr_trsp() gives the turn restrictions table)
        # This includes fractional edges. We uses the ratio of the cost from pgr and the cost of the edge
        #   to calculate the fraction of the edge length that we should include in the total length
        # This could still be optimized somewhat, e.g. http://gis.facetedlifes.com/questions/16886/how-can-i-optimize-pgrouting-for-speed
        # Maybe parallelize the pgrouting call, or use a WHERE clause in the pgr function 
        #   (e.g. http://gis.stackexchange.com/questions/72208/how-to-filter-the-graph-on-which-i-want-to-find-the-shortest-path)
        #   or https://github.com/pgRouting/pgrouting/issues/291
        # but we'd have to nest this in a function
        # right now, scales fairly linearly at 0.06/sec per trip

        db = mmt.dbConnection(pgLogin=self.pgLogin, verbose=False) # thread safe for parallelization
        cmd = '''SELECT trip_id, (SELECT SUM(pgr.cost/r3.cost*ST_Length(r3.geom_way)) AS length 
                    FROM pgr_trsp('SELECT id::int4, source::int4, target::int4, cost::float8, reverse_cost::float8 FROM %(sts)s', 
                            edge_id_start, stfr, edge_id_end, endfr, True, True,
                            'SELECT to_cost::float8, target_id::int4,source_id::text AS via_path FROM %(region)s_turn_restrictions') as pgr, 
                             %(sts)s as r3 WHERE id2=r3.id) FROM (SELECT trip_id, edge_ids[1] AS edge_id_start, edge_id_end, 
                          ST_LineLocatePoint(r1.geom_way, t.startpt_geom) AS stfr,
                          ST_LineLocatePoint(r2.geom_way, ST_EndPoint(t.lbuff_geom)) AS endfr
                       FROM %(sts)s AS r1, %(sts)s AS r2, %(table)s as t
                        WHERE t.edge_ids[1] = r1.id AND t.edge_id_end = r2.id
                            AND edge_ids[1] is not Null AND edge_id_end is not null AND trip_id=%(id)s) AS trips;
                            ''' % {'sts':self.streets, 'region':self.region, 'table':self.table, 'id':id}
        try:
            dist = db.execfetch(cmd)[0][1]
        except:  # some trips fail with an error because path not found
           dist = np.nan
        return (id, dist)

    def addOtherDistances(self):
        cols = [('max_dist', 'real'), ('walklength', 'real'), ('walkdist', 'real'), ('parkdist','real'),
                ('dist_ratio','real'), ('frc_inbuffer','real'), ('start_end_dist','real'), ('cruise_time','real'),
                ('cruise','boolean'), ('high_cruise','boolean'),]
        if 'max_dist' in self.db.list_columns_in_table(self.table):
            if self.forceUpdate: 
                dropTxt = ', '.join(['DROP COLUMN IF EXISTS '+cc[0] for cc in cols])
                self.db.execute('ALTER TABLE %s %s;' % (self.table, dropTxt))
            else:
                self.writeLog('Other distances already added. Skipping')
                return
        self.db.addColumns(cols, self.table, dropOld=True)

        self.writeLog('\tCalculating distances and ratios')
        
        # max distance from end point
        cmd = '''UPDATE %s t1 SET max_dist = distance FROM
                (SELECT trip_id, MAX(ST_Distance((dp).geom, end_geom)) AS distance FROM 
                    (SELECT trip_id, end_geom, ST_DumpPoints(lbuff_geom) AS dp FROM %s) AS pts
                GROUP BY trip_id) AS t2
            WHERE t1.trip_id=t2.trip_id;''' % (self.table, self.table)
        self.db.execute(cmd)

        # Walk segment length, and distances from start of 400m buffer to end, using (i) the GPS trace, (ii) map-matching
        cmd = '''UPDATE %s SET  walklength = ST_Distance(end_geom, park_geom), 
                                walkdist = ST_Length(lineswalk_geom),
                                parkdist = ST_Length(lineslot_geom);''' % self.table
        self.db.execute(cmd)
        
        # Set distance ratio to be a minimum of one - if they found an 'illegal' shorter route, this is OK
        cmd = '''UPDATE %s SET dist_ratio = GREATEST(matchdist / netwkdist, 1) WHERE netwkdist >0;''' % self.table
        self.db.execute(cmd)
    
        # Calculate the fraction of matched_line400 that lies within the 400m buffer
        cmd = '''UPDATE %s SET frc_inbuffer = ST_Length(ST_Intersection(matched_line, ST_Buffer(end_geom, %s))) / matchdist WHERE matchdist>0;''' % (self.table, r) 
        self.db.execute(cmd)

        # Euclidean distance (m) between start and end of the line (entire trace, not just the 400m buffer). Null if we don't have the true start
        if 'start_good' in self.db.list_columns_in_table(self.table):
            cmd = '''UPDATE %s SET start_end_dist = ST_Distance(start_geom, end_geom) WHERE start_good=True;''' % (self.table)
        elif 'start_geom' in self.db.list_columns_in_table(self.table):
            cmd = '''UPDATE %s SET start_end_dist = ST_Distance(start_geom, end_geom);''' % (self.table)
        else: # nn_traces don't have start_geom
            cmd = '''UPDATE %s SET start_end_dist = ST_Distance(ST_StartPoint(lines_geom), end_geom);''' % (self.table)        
        self.db.execute(cmd)
    
        # Any evidence of cruising?
        cmd = 'UPDATE %s SET cruise = False WHERE dist_ratio is not Null;' % self.table
        self.db.execute(cmd)
        cmd = '''UPDATE %s SET cruise = True WHERE (matchdist - netwkdist >5 OR ids_repeat>0) 
                     AND max_dist <= %s AND frc_inbuffer>%s;''' % (self.table, maxDistThres, bufferThresh)
        self.db.execute(cmd)
    
        cmd = 'UPDATE %s SET high_cruise = False WHERE dist_ratio is not Null;' % (self.table)
        self.db.execute(cmd)
        cmd = 'UPDATE %s SET high_cruise = True WHERE matchdist - netwkdist>200 AND cruise = True;' % (self.table)
        self.db.execute(cmd)
        #cmd = 'UPDATE %s SET ids_repeat = 0 WHERE high_cruise=False AND ids_repeat>0;'
        #self.db.execute(cmd) 

        # Calculate cruising time, as excess travel / speed 
        cmd = '''UPDATE {} SET cruise_time =
                    CASE WHEN high_cruise is True THEN GREATEST((matchdist - netwkdist) / (matchdist / (ST_M(ST_EndPoint(lbuff_geom)) - ST_M(ST_StartPoint(lbuff_geom)))),0)
                    WHEN high_cruise is False THEN 0 ELSE Null END;'''.format(self.table) 
        self.db.execute(cmd)

    def addParkingInfo(self):
        """DISTANCE TO PARKING (meters, off-street) AND CURB, 
        plus other geographic data (end block group)"""

        cols = [('bg', 'varchar'), ('end_clazz', 'int'), 
                ('near_lot_dist', 'real'), ('curb_dist','real')]

        if 'bg' in self.db.list_columns_in_table(self.table):
            if self.forceUpdate: 
                dropTxt = ', '.join(['DROP COLUMN IF EXISTS '+cc[0] for cc in cols])
                self.db.execute('ALTER TABLE %s %s;' % (self.table, dropTxt))
            else:
                self.writeLog('Parking info already added. Skipping')
                return
        self.db.addColumns(cols, self.table, dropOld=True)
        
        # End block group
        self.writeLog('\tFinding end census block group')
        cmd = '''UPDATE %s t1 SET bg = t2.bg FROM
                    (SELECT c1.bg, trip_id FROM %s, %s_bgs c1 WHERE ST_Intersects(end_geom, geom)) AS t2
                 WHERE t1.trip_id = t2.trip_id;''' % (self.table, self.table,self.region)
        try:
            self.db.execute(cmd)
        except:
            self.writeLog('\tCannot identify census block group. Perhaps the table is missing? Skipping.')

        # OSM class of last street edge (e.g. is it a parking lot alley?)
        cmd = '''UPDATE %s t1 SET end_clazz = clazz
                    FROM %s WHERE edge_id_end=id;''' % (self.table, self.streets)
        self.db.execute(cmd)

        # Distance to closest off-street lot
        self.writeLog('\tFinding closest off-street lot to end point')
        cmd = '''UPDATE %s t1 SET near_lot_dist = dist 
                 FROM (SELECT DISTINCT ON (pt.trip_id) pt.trip_id, 
                              ST_Distance(l.geom, pt.park_geom) AS dist
                    FROM %s as pt, %s_off_street as l
                    WHERE ST_DWithin(l.geom, pt.park_geom, 100)
                    ORDER BY pt.trip_id, dist) AS t2
                    WHERE t1.trip_id = t2.trip_id;''' % (self.table, self.table, self.region)  
        try:
            self.db.execute(cmd)
        except:
            self.writeLog('\tCannot identify closest off-street parking lot. Perhaps the table is missing? Skipping.')
    
        # Last known position - distance from curb ROW (negative if further from street centerline than curb ROW)
        # The ST_ClosestPoint() gets the distance from the centerline to the closest point on the curb to the end point
        self.writeLog('\tCalculating distance from curb')
        cmd = '''WITH dists AS 
                    (SELECT DISTINCT ON (pt.trip_id) trip_id,
                        ST_Distance(pt.park_geom, r.geom_way) as pt_centerline_dist,
                        ST_Distance(pt.park_geom, c.geom) as pt_curb_dist,
                        ST_Distance(ST_ClosestPoint(c.geom, pt.park_geom), r.geom_way) AS curb_centerline_dist
                    FROM %s AS pt, %s AS r, %s_curblines AS c
                  WHERE r.id = pt.edge_id_end AND ST_DWithin(c.geom, pt.park_geom, 200)
                   ORDER BY pt.trip_id, pt_curb_dist)
                UPDATE %s AS t1 SET curb_dist = curbdist FROM (
                SELECT trip_id, 
                    CASE WHEN pt_centerline_dist < curb_centerline_dist THEN pt_curb_dist 
                    ELSE pt_curb_dist*-1 END AS curbdist
                    FROM dists) AS dd
                WHERE t1.trip_id=dd.trip_id;''' % (self.table, self.streets, self.region, self.table)
        try:
            self.db.execute(cmd)
        except:
            self.writeLog('\tCannot calculate distance from curb. Perhaps the table is missing? Skipping.')

    def defineUsableTrips(self):
        """Set use_trip to be False where the trip ends on a freeway, or when match_score<qualityCutoff"""
        
        self.db.addColumns([('use_trip','boolean'),('end_clazz','int')], self.table, skipIfExists=True)
        self.db.execute('''UPDATE {} SET end_clazz = clazz FROM {} WHERE id = edge_id_end'''.format(self.table, self.streets))
        self.db.execute('UPDATE %s SET use_trip=False;' % self.table)
        self.db.execute('''UPDATE %s SET use_trip=True 
                             WHERE match_score>%s AND end_clazz!=11 AND pingtime_mean<=30 AND pingtime_max<=60;''' % (self.table, qualityCutoff)) 

    def runall(self):  
        """This is the sequence of functions that the analysis runs through"""
        
        self.dropErrantPings()
        self.createLotPolygons()
        self.truncateAllLines()
        if self.nCores is None:
            self.mapMatchinSerial()
        else:
            self.mapMatchinParallel()
        self.addMapMatchedSupplementaryData()
        self.addTimeStamps()
        self.calcAllNetworkDistances()
        self.addOtherDistances()
        self.addParkingInfo()
        self.defineUsableTrips()

def apply_multiprocessing(input_function, input_list, pool_size=5):
    """Handles multiprocessing pools gracefully, allows interrupts
    https://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool/1408476#1408476"""
    pool = multiprocessing.Pool(processes=pool_size, maxtasksperchild=10)

    try:
        jobs = {}
        for ii, value in enumerate(input_list):  # enumeration is a way to deal with unhashable types
            args = value if isinstance(value,list) or isinstance(value, tuple) else [value]
            jobs[ii] = pool.apply_async(input_function, args)

        results = {}
        for value, result in jobs.items():
            try:
                results[value] = result.get()
            except KeyboardInterrupt:
                print ('Interrupted by user')
                pool.terminate()
                break
            except Exception as e:
                print(e)
                results[value] = -1
        return results
    except Exception:
        raise
    finally:
        pool.close()
        pool.join()

def mapMatch_wrapper(pingDict, streetsTn, traceTn, schema='public'):
    """Wrapper for mapmatcher that avoids the problem with pickling objects in parallel"""
    pgLogin = mmt.getPgLogin(user=pgInfo['user'], db=pgInfo['db'], host=pgInfo['host'], requirePassword=pgInfo['requirePassword'], forceUpdate=False)
    pgLogin['schema'] = schema

    #print('Entering mapMatch wrapper')
    db = mmt.dbConnection(pgLogin=pgLogin, timeout=mapmatch_timeout, verbose=False)
    #print('db connection')
    mapMatcher = mm.mapMatcher(streetsTn, traceTn, 'trip_id', 'lbuff_geom', db=db, verbose=False, cleanedGeomName='lbuff_geom_cleaned',qualityModelFn=coeffFn)
    #print('mapMatcher part')
    assert isinstance(pingDict, OrderedDict)
    #print('assert isinstance')

    starttime=time.time()
    for ii,id in enumerate(pingDict):
        if pingDict[id]>=3:
            try:
                mapMatcher.matchPostgresTrace(id)
                #print('match postgres trace', id)
                if mapMatcher.matchStatus==0:
                    mapMatcher.writeMatchToPostgres()
                    #print('write match to postgres', id)
            except Exception as e:
                print('***FAILED ON trace %s (#%d of %d)***' % (id,ii,len(pingDict)))
                print(str(e))
    print('Finishing mapmatching chunk (through trace %d) in %d seconds' % (id, time.time()-starttime))

    return 0

if __name__ == '__main__':
    """
    How to call from the command line
    python cruising.py trace_table_name region_abbrev

    For example:
    python cruising.py mytrips ca
    """
    if len(sys.argv)!=3:
        raise Exception ("Call %s with the PostgreSQL table name of your GPS traces and the region abbreviation!" % sys.argv[0])
    table = sys.argv[1].lower()
    region = sys.argv[2].lower()
    
    tt = traceTable(table, region)
    tt.runall()
