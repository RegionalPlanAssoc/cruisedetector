"""
Supporting functions for SFpark_cruising.py
These mainly load the database or various tables
They are put here to get them out of the way

"""
import os, sqlalchemy, platform, datetime, sys
import pandas as pd
import numpy as np
from parking_config import *
import pgMapMatch.tools as mmt
from pgMapMatch.config import *

def setupdbase():
    """ 
    these are just some notes - what to type in pgadmin3 or command line

    create a tablespace: http://linfiniti.com/2011/05/working-with-tablespaces-in-postgis/
    CREATE TABLESPACE parking LOCATION '/Volumes/GIS data/PostGIS/parking/';
    then create the database parking
    CREATE DATABASE parking 
    WITH OWNER = postgres ENCODING = 'UTF8'
    LC_COLLATE = 'en_US' LC_CTYPE = 'en_US'
    TABLESPACE = parking

    CREATE EXTENSION postgis;
    CREATE EXTENSION pgrouting;
    
    #create the roles as well (amb, robert, rachel - their passwords are all cruising
    #amb's password is matt's old one    
    CREATE ROLE amb LOGIN PASSWORD xxxx
    CREATE ROLE robert LOGIN PASSWORD 'cruising'
    CREATE ROLE rachel LOGIN PASSWORD 'cruising'
    CREATE ROLE josh LOGIN PASSWORD 'cruising'
    CREATE ROLE dave LOGIN PASSWORD 'cruising'
    
    # create a group giving us all rights to tables in parking
    # however, only the table owner (amb) can drop tables
    CREATE ROLE parkingusers NOLOGIN; 
    GRANT parkingusers TO amb, robert, rachel, josh, dave;
    GRANT ALL PRIVILEGES ON                  SCHEMA public TO parkingusers;
    GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO parkingusers;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO parkingusers;

    CREATE SCHEMA parking AUTHORIZATION parkingusers;
    ALTER DATABASE parking SET search_path = parking, public;


    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO parkingusers;
    ALTER DEFAULT PRIVILEGES IN SCHEMA poc GRANT ALL PRIVILEGES ON TABLES TO parkingusers;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO parkingusers;
    ALTER DEFAULT PRIVILEGES IN SCHEMA poc GRANT ALL PRIVILEGES ON SEQUENCES TO parkingusers;

    """
    return

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
        for table in ['osm_2po_4pgr', 'osm_2po_vertex', 'turn_restrictions', 'streets', 'curblines', 'off_street', 'tracts', 'zipcodes']:
            db.execute('DROP TABLE IF EXISTS %s_%s;' % (region,table))
        if region=='sf':
            for table in ['sfpark_blocks','sf_meters','sensors', 'pr_full_predictions']:
                db.execute('DROP TABLE IF EXISTS %s;' % (table))
    
    """
    1. Load OSM street network using osm2po
    .osm.pbf file downloaded June 9, 2015 from MapZen metro extracts
        https://s3.amazonaws.com/metro-extracts.mapzen.com/san-francisco_california.osm.pbf
        
    Michigan version downloaded October 7, 2015 from http://download.geofabrik.de/north-america/us/michigan.html
    California version downloaded December 9, 2015 from http://download.geofabrik.de/north-america/us/california-latest.osm.pbf
    
    osm2po downloaded June 9, 2015 from osm2po.de
        See http://planet.qgis.org/planet/tag/osm2po/
        Make following changes to osm2po config file (for the first one, see http://gis.stackexchange.com/questions/41393/does-osm2po-take-into-consideration-turn-restrictions)
            1. postp.1.class = de.cm.osm2po.plugins.postp.PgVertexWriter 
            2. graph.build.excludeWrongWays = true
    """
    os.chdir(paths['root'])
    
    osmDict = {'sf':'san-francisco_california.osm.pbf', 'mi':'michigan-latest.osm.pbf', 'ca':'california-latest.osm.pbf'}
    for region in regions:
        assert os.system("java -Xmx5g -jar '%sosm2po-5.0.0/osm2po-core-5.0.0-signed.jar' tileSize=x cmd=c prefix='%s_osm' '%s'" % (paths['root'], region, paths['input']+osmDict[region]))==0

    # table of streets
    for region in regions:
        st_table = region+'_streets'
        print 'Loading streets for %s into table %s' % (region, st_table)
        assert os.system("""psql -d %s -h %s -U %s -q -f '%s%s_osm/%s_osm_2po_4pgr.sql'""" % (pgInfo['db'], pgInfo['host'], pgInfo['user'], paths['root'], region, region))==0
        # table of turn restrictions
        assert os.system("psql -d %s -h %s -U %s -q -f '%s%s_osm/%s_osm_2po_vertex.sql'" % (pgInfo['db'], pgInfo['host'], pgInfo['user'], paths['root'], region, region))==0
        assert os.system("rm -r '%s%s_osm'" % (paths['root'], region))==0

        # rename and project to 3494
        db.fix_permissions_of_new_table('%s_osm_2po_4pgr' % region)
        db.fix_permissions_of_new_table('%s_osm_2po_vertex' % region)
        db.execute("ALTER TABLE %s_osm_2po_4pgr RENAME TO %s;" % (region, st_table))
        db.execute("ALTER TABLE %s ALTER COLUMN geom_way TYPE Geometry(LineString, %s) USING ST_Transform(geom_way, %s);" % (st_table, defaults['srs'][region], defaults['srs'][region]))
        db.execute("ALTER TABLE %s_osm_2po_vertex ALTER COLUMN geom_vertex TYPE Geometry(Point, %s) USING ST_Transform(geom_vertex, %s);" % (region, defaults['srs'][region], defaults['srs'][region]))
    
    if 'sf' in regions:
        # add some missing SF streets manually (these are streets that are closed for construction in the current OSM edition, but existed before)
        # 4th St, Fremont St ramp and Stockton St
        # format is (startnode, endnode, cost, reverse_cost
        newStreets = [(65282779, 601267947, 0.001, 1000000), (601267947, 65284950, 0.001, 1000000),  
                    (300763323, 65317521, 0.001, 1000000), 
                    (1271001343, 2936165726, 0.001,1000000), (2936165726, 65317939, 0.001,1000000), (65317939, 65371286, 0.001,1000000), 
                    (1578907668, 65317939, 0.002, 1000000), (65317939, 1580501214, 0.002, 1000000) ]
        newNodes = [(65317939, 37.7866373, -122.4064095)]
        streetsToDrop = '(290052332)'
        db.execute('SELECT max(id) FROM sf_streets')
        maxId = db.fetchall()[0][0] + 1
        db.execute('SELECT max(id) FROM sf_osm_2po_vertex')
        maxNode = db.fetchall()[0][0] + 1
    
        for ii, (osm_id, lat, lon) in enumerate(newNodes):
            db.execute('INSERT INTO sf_osm_2po_vertex (id, osm_id) VALUES (%s, %s);' % (maxNode+ii, osm_id))
            db.execute('UPDATE sf_osm_2po_vertex SET geom_vertex = ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s),4326), %s) WHERE id=%s;' % (lon, lat, defaults['srs']['sf'], maxNode+ii))
    
        for ii, (source, target, cost, rev_cost) in enumerate(newStreets):
            db.execute('INSERT INTO sf_streets (id, cost, reverse_cost) VALUES (%s, %s, %s)' % (maxId+ii, cost, rev_cost))
            db.execute('''UPDATE sf_streets SET geom_way = ST_MakeLine(snode, tnode), source = sid, target=tid,
                                              km = ST_Length(ST_MakeLine(snode, tnode))/1000, kmh = 40
                                FROM (SELECT geom_vertex as snode, id as sid FROM sf_osm_2po_vertex WHERE osm_id=%s) AS s,
                                     (SELECT geom_vertex as tnode, id as tid FROM sf_osm_2po_vertex WHERE osm_id=%s) AS t
                           WHERE id = %s''' % (source, target, maxId+ii))
        db.execute('DELETE FROM sf_streets WHERE osm_id IN %s' % (streetsToDrop))

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
        noTurns.to_sql(rest_table, engine, schema='parking', if_exists='replace', index=True)
        db.fix_permissions_of_new_table(rest_table)
        db.execute('DROP TABLE %s_osm_2po_vertex' % region)
    
    """
    2. Import other data
    """
    # import curb lines
    if 'sf' in regions:
        print 'Importing curblines and restricting streets to SF area'
        assert os.system("shp2pgsql -s 3494:%s -I -e '%sCurblines/cityfeatures' 'sf_curblines' | psql -q -h %s -d %s -U %s" % (defaults['srs']['sf'], paths['input'], pgInfo['host'], pgInfo['db'], pgInfo['user'] ))==0
        db.fix_permissions_of_new_table('sf_curblines')
        db.execute('CREATE INDEX sf_curblines_spidx ON sf_curblines USING GIST (geom);')
        # restrict streets to those within SF county (use extent of curblines for this purpose) and Ann Arbor census tracts
        cmd = '''DELETE FROM sf_streets WHERE id IN 
                    (SELECT id FROM sf_streets, 
                        (SELECT ST_Buffer(ST_SetSRID(ST_Extent(sf_curblines.geom)::geometry, %s), 500) AS bbox FROM sf_curblines) AS r 
                    WHERE NOT(bbox&&geom_way))''' % (defaults['srs']['sf'])
        db.execute(cmd)
        
        print('Importing curb regulations, TAZs and block groups')
        assert os.system("shp2pgsql -s 4269:%s -I -e '%sSF_blockgroups/SF_blockgroups' 'sf_blockgroups' | psql -q -h %s -d %s -U %s" % (defaults['srs']['sf'], paths['input'], pgInfo['host'], pgInfo['db'], pgInfo['user'] ))==0
        db.execute('CREATE INDEX sf_blockgroups_spidx ON sf_blockgroups USING GIST (geom);')
        db.fix_permissions_of_new_table('sf_blockgroups')
        assert os.system("shp2pgsql -s 6420:%s -I -e '%sSF_TAZs/TAZ2454.shp' 'sf_tazs' | psql -q -h %s -d %s -U %s" % (defaults['srs']['sf'], paths['input'], pgInfo['host'], pgInfo['db'], pgInfo['user'] ))==0
        db.execute('CREATE INDEX sf_tazs_spidx ON sf_tazs USING GIST (geom);')
        db.fix_permissions_of_new_table('sf_tazs')
        assert os.system("shp2pgsql -s 4326:%s -I -e '%sSF_regulations/geo_export_309c1882-f37d-4a38-8f4d-5671848760c2.shp' 'sf_regulations' | psql -q -h %s -d %s -U %s" % (defaults['srs']['sf'], paths['input'], pgInfo['host'], pgInfo['db'], pgInfo['user'] ))==0
        db.execute('CREATE INDEX sf_regulations_spidx ON sf_regulations USING GIST (geom);')
        db.fix_permissions_of_new_table('sf_regulations')        
        

    if 'mi' in regions:
        assert os.system("shp2pgsql -s 3593:%s -I -e '%sMichigan/EdgeOfPavement_1' 'mi_curblines' | psql -q -h %s -d %s -U %s" % (defaults['srs']['mi'], paths['input'], pgInfo['host'], pgInfo['db'], pgInfo['user'] ))==0
        db.fix_permissions_of_new_table('mi_curblines')
        db.execute('CREATE INDEX mi_curblines_spidx ON mi_curblines USING GIST (geom);')

    # import tracts and zipcodes
    print 'Importing census tracts and zipcodes'
    if 'sf' in regions:
        assert os.system("shp2pgsql -s 3493:%s -I -e '%sSF_zipcodes/SF_zipcodes' 'sf_zipcodes' | psql -q -h %s -d %s -U %s" % (defaults['srs']['sf'], paths['input'], pgInfo['host'], pgInfo['db'], pgInfo['user'] ))==0
        assert os.system("shp2pgsql -s 3494:%s -I -e '%sSF_CensusTracts2010/SF_CensusTracts2010' 'sf_tracts' | psql -q -h %s -d %s -U %s" % (defaults['srs']['sf'], paths['input'], pgInfo['host'], pgInfo['db'], pgInfo['user'] ))==0
        db.execute('ALTER TABLE sf_tracts RENAME COLUMN geoid10 TO geoid;')
        db.fix_permissions_of_new_table('sf_zipcodes')
        db.fix_permissions_of_new_table('sf_tracts')
        db.execute('CREATE INDEX sf_zipcodes_spidx ON sf_zipcodes USING GIST (geom);')
        db.execute('CREATE INDEX sf_tracts_spidx ON sf_tracts USING GIST (geom);')

    if 'mi' in regions:
        assert os.system("shp2pgsql -s 4326:%s -I -e '%sAnnArborTracts/AnnArborTracts' 'mi_tracts' | psql -q -h %s -d %s -U %s" % (defaults['srs']['mi'], paths['input'], pgInfo['host'], pgInfo['db'], pgInfo['user'] ))==0
        db.fix_permissions_of_new_table('mi_tracts')
        
        db.execute('CREATE INDEX mi_tracts_spidx ON mi_tracts USING GIST (geom);')
        cmd = '''DELETE FROM mi_streets WHERE id IN 
                    (SELECT id FROM mi_streets, 
                        (SELECT ST_Buffer(ST_SetSRID(ST_Extent(mi_tracts.geom)::geometry, %s), 1500) AS bbox FROM mi_tracts) AS r 
                    WHERE NOT(bbox&&geom_way))''' % (defaults['srs']['mi'])
        db.execute(cmd)

    if 'sf' in regions:
        # create table for parking meters, then import and create the geometry from the (lat, long) text field
        print 'Importing meters, lots and sensor data'
        db.execute("""CREATE TABLE sf_meters (
          post_id text, ms_id text, ms_spaceid int, cap_color text, meter_type text, smart_mete text, activesens text,
          jurisdicti text, on_off_str text, osp_id int, street_num int, streetname text, street_seg int,
          ratearea text, sfparkarea text, location text); """)
        assert os.system('''psql -d %s -U %s -c "\\copy sf_meters FROM '%sParking_meters.csv' HEADER CSV"''' % (pgInfo['db'], pgInfo['user'], paths['input']))==0
        db.execute('''ALTER TABLE sf_meters ADD COLUMN blockid text, ADD COLUMN geom geometry(POINT)''')
        db.execute('''UPDATE sf_meters SET blockid = left(post_id,3) || substring(post_id from 5 for 2)''')
        db.execute('''UPDATE sf_meters SET geom = ST_Transform(ST_GeomFromText(
                'POINT(' || left(split_part(location, ' ',2),-1) || ' ' || right(split_part(location, ', ',1),-1) || ')', 4326), %s)''' % defaults['srs']['sf'])
        db.execute('CREATE INDEX sf_meters_spidx ON sf_meters USING GIST (geom);') 
       
       
    # load off-street parking facilities
    if 'sf' in regions:
        # delete large parcels
        assert os.system("shp2pgsql -s 3494:%s -I -e '%sOff-street/OffStreetFacilities_Parcels' 'sf_off_street' | psql -q -h %s -d %s -U %s" % (defaults['srs']['sf'], paths['input'], pgInfo['host'], pgInfo['db'], pgInfo['user'] ))==0
        db.fix_permissions_of_new_table('sf_off_street')
        db.execute('SELECT COUNT(*) FROM sf_off_street WHERE ST_Area(geom)>250000')
        print '\tDropping %s large off-street lots (>250000 m2)' % db.fetchall()[0][0]
        db.execute('DELETE FROM sf_off_street WHERE ST_Area(geom)>250000')  
        db.execute('CREATE INDEX sf_off_street_spidx ON sf_off_street USING GIST (geom);')

    if 'mi' in regions:
        # Michigan lots
        assert os.system("shp2pgsql -s 3593:%s -I -e '%sMichigan/OffStreetParking_Merged' 'mi_off_street' | psql -q -h %s -d %s -U %s" % (defaults['srs']['mi'], paths['input'], pgInfo['host'], pgInfo['db'], pgInfo['user'] ))==0
        db.fix_permissions_of_new_table('mi_off_street')
        db.execute('CREATE INDEX mi_off_street_spidx ON mi_off_street USING GIST (geom);')
        db.execute('ALTER TABLE mi_streets ADD COLUMN sfpark_id text')  # for completeness
        
    if 'sf' in regions:
        # import SFpark blocks and add the sfpark block id to the streets table
        assert os.system("shp2pgsql -s 4326:%s -I -e '%sBlocksShapefile/BlocksShapefile' 'sfpark_blocks' | psql -q -h %s -d %s -U %s" % (defaults['srs']['sf'], paths['input'], pgInfo['host'], pgInfo['db'], pgInfo['user'] ))==0
        db.fix_permissions_of_new_table('sfpark_blocks')
        # The 3 excluded block ids look like errors in the SFpark block id map. None have any sensor data attached
        db.execute('ALTER TABLE sf_streets ADD COLUMN sfpark_id text')
        db.execute('''UPDATE sf_streets t1 SET sfpark_id = blockid  FROM
                            (SELECT id, lpad(CAST(block_id::integer AS text), 5, '0') AS blockid
                            FROM sf_streets, sfpark_blocks
                            WHERE ST_DWithin(geom_way, geom, 10) AND ST_Contains(geom, ST_Line_Substring(geom_way, 0.2, 0.8))
                                 AND NOT (block_id in (56848, 44312, 36506))) as t2
                        WHERE t1.id = t2.id''')
        # now we can drop the sfpark blocks table
        db.execute('DROP TABLE sfpark_blocks')

        print 'Loading sensor data'    
        db.execute('''CREATE TABLE sensors (
            block_id text, street_name text, block_num text, street_block text, area_type text, pm_district_name text,
            ratetext text, start_time_dt text, tot_time bigint, tot_occupied_time bigint, tot_vacant_time bigint, 
            tot_unknown_time bigint, op_time bigint, op_occupied_time bigint, op_vacant_time bigint, op_unknown_time bigint,
            nonop_time bigint, nonop_occupied_time bigint, nonop_vacant_time bigint, nonop_unknown_time bigint,
            gmp_time bigint, gmp_occupied_time bigint, gmp_vacant_time bigint, gmp_unknown_time bigint, comm_time bigint,
            comm_occupied_time bigint, comm_vacant_time bigint, comm_unknown_time bigint, 
            cal_month_name text, cal_year text, cal_date text, day_type text, time_of_day int);''')
        sensorFns = [ff for ff in os.listdir(paths['input']+'sensorData/') if ff.endswith('.csv')]
        for sensorFn in sensorFns:
            print ('''psql -d %s -U %s -c "\\copy sensors FROM '%ssensorData/%s' HEADER CSV"''' % (pgInfo['db'], pgInfo['user'], paths['input'], sensorFn))
            assert os.system('''psql -d %s -U %s -c "\\copy sensors FROM '%ssensorData/%s' HEADER CSV"''' % (pgInfo['db'], pgInfo['user'], paths['input'], sensorFn))==0

        # zfill block_id (this just affects the off-street lot)
        db.execute("UPDATE sensors SET block_id = lpad(block_id, 5, '0')")
        # delete time from date field to avoid confusion (meaningless as time is always 12am(
        db.execute("UPDATE sensors SET cal_date = left(cal_date, 9)")
    
        print 'Adding columns for sensor data...'
        db.execute('''ALTER TABLE sensors ADD COLUMN id SERIAL PRIMARY KEY, 
                            ADD COLUMN caldate date, ADD COLUMN rate real,
                            ADD COLUMN sensor_time timestamp with time zone, 
                            ADD COLUMN tot_occ_pc real, ADD COLUMN gmp_occ_pc real, 
                            ADD COLUMN tot_pr_full real, ADD COLUMN gmp_pr_full real''')
        print 'Processing sensor data...'
        db.execute("UPDATE sensors SET rate = nullif(ratetext,'')::real")    
        db.execute("UPDATE sensors SET caldate = to_date(cal_date, 'DD-MON-YY')")
        db.execute("UPDATE sensors SET sensor_time = to_timestamp(start_time_dt || ' America/Los_Angeles', 'DD-Mon-YY HH12.MI.SS.MSUS AM')")
        db.execute('UPDATE sensors SET tot_occ_pc =  tot_occupied_time / nullif((tot_occupied_time + tot_vacant_time), 0)::real')
        db.execute('UPDATE sensors SET gmp_occ_pc =  gmp_occupied_time / nullif((gmp_occupied_time + gmp_vacant_time), 0)::real')

        # these are redundant now - date and time formatted columns exist
        db.execute('ALTER TABLE sensors DROP COLUMN cal_date, DROP COLUMN ratetext')

        db.execute('CREATE INDEX sensors_block_id_idx ON sensors (block_id);') 
        db.execute('CREATE INDEX sensors_sensor_time_idx ON sensors (sensor_time);') 

        # import predictions to new table
        # predictions.pandas was produced for Transportation Research Part A paper (lookup of predictions for block size and average occupancy)
        print 'Creating predictions lookup'
        predictions = pd.read_pickle(paths['input'] + 'predictions.pandas').reset_index()
        predictions.rename(columns={'Pr_full':'pr_full'}, inplace=True)
        predictions.to_sql('pr_full_predictions', engine, schema='public', if_exists='replace', index=False)
        db.fix_permissions_of_new_table('pr_full_predictions')

        # now use the predictions as a lookup for the sensor data
        for stype in ['gmp', 'tot']:
            cmd = '''UPDATE sensors t1 SET %s_pr_full=pr_full FROM (
                          SELECT id, pr_full FROM (
                            SELECT pr_full, capacity, pc_occ, id, round(%s_occ_pc*100) AS occ, round(%s_time/3600) AS n 
                            FROM sensors, pr_full_predictions) AS t3
                        WHERE capacity=n and pc_occ=occ) AS t2
                    WHERE t1.id=t2.id ''' % (stype, stype, stype)
            db.execute(cmd)
    
    # Change privileges (this should be done by default, but it seems not)
    db.execute('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s TO parkingusers;' % pgInfo['schema'])
    
    print "Done loading tables"
    return
    
def loadStreetLight():
    """ 
    Note: the loops are for when we had poc.nn_trips_full and poc.nn_trips. Now we just have one, so the loop is redundant
    """
     
    print "\nLoading Streetlight data..."
    db = mmt.dbConnection(pgLogin=pgInfo)
    
    # Note that only postgres can load the dump, so we load as postgres and then change table owner to amb
    cmds = ['DROP SCHEMA IF EXISTS poc CASCADE;', 'CREATE SCHEMA poc;', 'DROP SCHEMA IF EXISTS sl CASCADE;',
            'DROP TABLE IF EXISTS sl_traces;',
            'GRANT ALL PRIVILEGES ON SCHEMA poc TO parkingusers;', 
            'GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA poc TO parkingusers;',
            'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA poc TO parkingusers;']
    for cmd in cmds:
        assert os.system('psql -q -d parking -U postgres -c "%s"' % cmd)==0

    for dmpFn in ['poc_nn_trips_full']: #['poc_nn_trips_full', 'poc_nn_trips']:
        #os.system('psql -q -d parking -U postgres < "/Users/amb/Documents/Research/Parking/StreetLight Sample/%s.dmp"' % dmpFn)
        assert os.system('gunzip -c "/Volumes/GIS data/CruisingOverflow/StreetLight/%s.dmp.gz" | psql -q -d parking -U postgres' % dmpFn)==0
    
    tableNameLookup = {'sl_traces': 'nn_trips_full'}
    for table in ['sl_traces']: #['nn_trips_full','nn_trips']:
        logF = open(paths['output'] + table.lower() + '_log.txt', 'w')
        logF.writelines('\nLoading StreetLight traces on ' +str(datetime.datetime.now())+'\n')
        
        assert os.system('psql -q -d %s -U postgres -c "ALTER TABLE poc.%s OWNER TO amb"' % (pgInfo['db'], tableNameLookup[table]))==0
        db.fix_permissions_of_new_table('poc.%s' % tableNameLookup[table])
        db.execute('ALTER TABLE poc.%s RENAME TO %s' % (tableNameLookup[table], table))
        db.execute('ALTER TABLE poc.%s SET SCHEMA %s' % (table, pgInfo['schema']))
        
        # reproject
        for geomCol, geomType in [('lines_geom', 'LineStringM'), ('start_geom','Point'), ('end_geom','Point')]:
            db.execute("""ALTER TABLE %s 
                    ALTER COLUMN %s TYPE Geometry(%s, %s) USING ST_Transform(%s, %s);""" % (table, geomCol, geomType, defaults['srs']['sf'], geomCol, defaults['srs']['sf']))
        
        # reindex trip_id column so that it is unique. This will also be the index.
        db.execute('ALTER TABLE %s RENAME COLUMN trip_id TO trip_id_sl;' % table)
        db.execute('ALTER TABLE %s ADD COLUMN    trip_id SERIAL PRIMARY KEY;' % table)
        
        # delete rows that are not "true ends", and are taxis or commercial vehicles
        db.execute('SELECT count(*) FROM %s;' % table)
        nRows = db.fetchall()[0][0]
        db.execute("DELETE FROM %s WHERE end_good is FALSE;" % table)
        db.execute('SELECT count(*) FROM %s;' % table)
        nRows2 = db.fetchall()[0][0]
        outText = 'Dropped %d of %d rows that are not the true trip end for table %s' % (nRows-nRows2, nRows, table)
        print outText
        logF.writelines(outText+'\n')

        db.execute("DELETE FROM %s WHERE provider_type !='PERS';" % table)
        db.execute('SELECT count(*) FROM %s;' % table)
        nRows3 = db.fetchall()[0][0]
        outText = 'Dropped %d of %d rows that are not personal vehicles for table %s' % (nRows2-nRows3, nRows2, table)  
        print outText
        logF.writelines(outText+'\n')
        
        db.execute("DELETE FROM %s WHERE extract('year' from to_timestamp(ST_M(ST_StartPoint(lines_geom))))<2013;" % table)
        db.execute('SELECT count(*) FROM %s;' % table)
        nRows4 = db.fetchall()[0][0]
        outText = 'Dropped %d of %d rows with dates<2013 for table %s' % (nRows3-nRows4, nRows3, table)
        print outText
        logF.writelines(outText+'\n')
        
        # drop trip ends that are outside SF county (we might want to revisit this)
        cmd = '''DELETE FROM %s WHERE trip_id IN 
                    (SELECT trip_id FROM %s, 
                        (SELECT ST_Buffer(ST_SetSRID(ST_Extent(sf_curblines.geom)::geometry, %s), 500) AS bbox FROM sf_curblines) AS r 
                    WHERE NOT(bbox&&end_geom))''' % (table, table, defaults['srs']['sf'])         
        db.execute(cmd)
        db.execute('SELECT count(*) FROM %s;' % table)
        outText = 'Dropped %d of %d rows that do not end in SF for table %s' % (nRows4-db.fetchall()[0][0], nRows4, table)
        print outText
        logF.writelines(outText+'\n')
        
        # create indexes - spatial index on lines and end_geom (a regular index on  trip_id already exists)
        db.execute('CREATE INDEX %s_lines_spidx ON %s USING GIST (lines_geom);' % (table, table))
        db.execute('CREATE INDEX %s_ends_spidx ON %s USING GIST (end_geom);' % (table, table))

    print "...done loading Streetlight data"
    return
    
def loadSurveyTraces():
    """
    Load the survey traces done in SF
    Create two tables: one with the full data (one row per record), and one to match the Streetlight (one per trip)
    Also load in the shapefile of destinations
    
    Most of the processing is done in process_traces, except we also calculate some fields that are unique to this dataset
    - distance from start of cruise phase to parking location and actual destination
    - distance from parking location to actual destination
    - speed of cruise portion of the trip (cruise_speed)
    - network distance for the cruise portion of the trip 
    - ratio and difference for actual:network distance for the cruise portion 
    - edge id of starting point of cruise (edge_id_startcruise)
    The line string for the cruise phase is cruise_geom
    """
    print "\nLoading survey traces into postgres..."
    db = mmt.dbConnection(pgLogin=pgInfo)
    engine = mmt.getPgEngine(pgInfo)
    
    # create tables
    for table in ['nn_traces_fulltable','nn_traces','dests']:
        db.execute('DROP TABLE IF EXISTS %s;' % table)
        
    # load destinations
    os.system("shp2pgsql -s 4326:%s -I -e '%straces/Dest_Addr/Task3_1_Dest_Addr_Update' 'dests' | psql -q -h %s -d parking -U %s" % (defaults['srs']['sf'], paths['input'], pgInfo['host'], pgInfo['user'] ))
    db.fix_permissions_of_new_table('dests')
    db.execute('CREATE INDEX dests_spidx ON dests USING GIST (geom);')

    # load the full trace data
    tracesFns = [ff for ff in os.listdir(paths['input']+'traces') if ff.startswith('Data Log') and ff.endswith('csv')]
    guide = pd.read_excel(paths['input']+'traces/TraceGuide.xlsx', skiprows=2)
    # we can add to this as needed - small set for now
    colsToUse = {'Frame Number':'framenum', 'Frame Time (ms)': 'frametime', 'Forward Acceleration': 'forward_accel', 
        'GPS Latitude.1':'lat', 'GPS Longitude.1':'lon'}
    bigDf = pd.DataFrame()
    lineStrings, cruiseStrings, endPoints, dests, traceids = [], [], [], [], []
    for ii, fn in enumerate(tracesFns):
        # two files have a non-standard format
        if '12_57' in fn: continue  # not a valid sample
        skiprows = 0 if fn=='Data Log May 26 2015 02_51 PM.csv' else 1
        fnSpt = fn.split(' ')
        tracedate = fnSpt[2]+' '+fnSpt[3]+', '+fnSpt[4]
        tracetime = fnSpt[5][:2]+':'+fnSpt[5][3:]+' '+fnSpt[6][:2] #+' America/Los_Angeles' # America/Los_Angeles is time zone
        df = pd.read_csv(paths['input']+'traces/' + fn, skiprows=skiprows)[colsToUse.keys()]
        df['tracedate'] = tracedate
        df['tracetime'] = tracetime
        df.rename(columns=colsToUse, inplace=True)
        starttime = pd.Period(tracedate + ' ' + tracetime, 'L') # L is ms
        df['timestamp'] = df.frametime.apply(lambda x: starttime+x)
        df['timestampstr'] = df.timestamp.astype(str) + ' America/Los_Angeles' 

        # get destination and start of cruise from guide dataframe
        guideRow = guide[(guide.Trace==fnSpt[5]) & (guide.Date.dt.day==int(fnSpt[3]))]
        assert len(guideRow)<2
        if len(guideRow)==1:
            dest = guideRow.Video.values[0]
            dest = '' if dest.startswith('G') or dest.startswith('No Video') else dest.split()[0].split('_')[1]
            df['destination'] = dest
        else:
            df['destination'] = ''
        if len(guideRow)==1 and pd.notnull(guideRow.iloc[0]['Elapsed Time']):        
            cruiseStart = guideRow['Elapsed Time'].astype(str).values[0].split(':')
            cruiseStart = int(cruiseStart[0])*60*60+int(cruiseStart[1])*60+int(cruiseStart[2])
            df['cruise'] = df.frametime.apply(lambda x: x>=cruiseStart*1000).astype(bool)
            cruiseData = True
        else:
            df.cruise=np.nan
            cruiseData = False
            
        bigDf = pd.concat([bigDf, df])
        
        # resample to lower resolution and create the linestring
        # note this is not time-zone aware!
        df = df[(pd.notnull(df.lat)) & (pd.notnull(df.lon))]
        endPoint = 'POINT (' + str(df.iloc[-1].lon) + ' ' + str(df.iloc[-1].lat) + ')'
        endPoints+=[endPoint]
        df = df.set_index('timestamp').resample('1S').mean()
        df = df[(pd.notnull(df.lat)) & (pd.notnull(df.lon))]  # some nans introduced during resampling
        dests+=[dest]
        traceids+=[tracedate + ' ' + tracetime] 
        coords = zip(df.lon.values.astype(str), df.lat.values.astype(str), df.index.to_timestamp().to_period('S').astype(int).astype(str))
        lineString = 'LINESTRING M (' +', '.join([' '.join(cc) for cc in coords])+')'
        lineStrings+=[lineString]
        
        if cruiseData and df.cruise.sum()>0:
            coords = zip(df[df.cruise==True].lon.values.astype(str), df[df.cruise==True].lat.values.astype(str), df[df.cruise==True].index.to_timestamp().to_period('S').astype(int).astype(str))
            cruiseString = 'LINESTRING M (' +', '.join([' '.join(cc) for cc in coords])+')'
            cruiseStrings+=[cruiseString]
        else:
            cruiseStrings+=['']


    bigDf.drop('timestamp', axis=1, inplace=True)
    print '\nUploading traces to postgres....'
    bigDf.to_sql('nn_traces_fulltable', engine, schema='public', if_exists='replace', index=False, 
        dtype={'tracedate': sqlalchemy.Date, 'tracetime': sqlalchemy.Time, 'timestampstr': sqlalchemy.TIMESTAMP(timezone=True)})
    db.fix_permissions_of_new_table('nn_traces_fulltable')
    print '\t...done'

    tripDf = pd.DataFrame({'geomwkt':lineStrings, 'end_geomwkt':endPoints, 'cruisewkt':cruiseStrings, 'dest':dests, 'traceid':traceids })
    tripDf.index.name='trip_id'
    tripDf.to_sql('nn_traces', engine, schema='public', if_exists='replace', index=True)  
    db.fix_permissions_of_new_table('nn_traces')

    db.execute('''ALTER TABLE nn_traces ADD COLUMN lines_geom geometry(LineStringM), 
                                         ADD COLUMN end_geom geometry(Point)''')
    db.execute('UPDATE nn_traces SET lines_geom=ST_Transform(ST_GeomFromText(geomWKT,4326), %s)' % defaults['srs']['sf'])
    db.execute('UPDATE nn_traces SET end_geom=ST_Transform(ST_GeomFromText(end_geomwkt,4326), %s)' % defaults['srs']['sf'])
    
    db.execute('CREATE UNIQUE INDEX nn_traces_idx ON nn_traces (trip_id);')
    db.execute('CREATE INDEX nn_traces_lines_spidx ON nn_traces USING GIST (lines_geom);')
    db.execute('CREATE INDEX nn_traces_ends_spidx ON nn_traces USING GIST (end_geom);')

    db.execute('GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO parkingusers;')

    print "...done. Loaded nn_traces_fulltable and nn_traces tables"
    return

def loadVideoTrips():
    """
    Load data from Robert's video trips
    This is very similar to the process in loadSurveyTraces()
    """
    print "\nLoading video traces into postgres..."
    db = mmt.dbConnection(pgLogin=pgInfo)
    engine = mmt.getPgEngine(pgInfo)

    db.execute('DROP TABLE IF EXISTS video_traces')
    
    #df = pd.read_csv(paths['input']+'videoTripsUpdate.csv')
    #df = pd.read_csv(paths['input']+'all_aa_10_14.csv') # for Robert
    #df = pd.read_csv(paths['input']+'aa_downtown_all.csv',skipfooter=2)  # last two lines are bad
    df = pd.read_csv(paths['input']+'aa_threeMonths.csv',skipfooter=2)
    
    # Trip is not unique across devices, so create a new tripId field
    assert df.Trip.max()<=11465  #and df.Trip.dtype=='int64'
    df.rename(columns={'\xef\xbb\xbfDevice':'Device'}, inplace=True)
    df = df[(df.Latitude!=0) & (df.Longitude!=0)]  # missing data is coded as zero

    df['tripId'] = df.Device.astype(int)*20000+df.Trip.astype(int)
    df.set_index('tripId', inplace=True)
    df.drop('Trip', axis=1, inplace=True)
    
    # get rid of microseconds
    df.LocalDateTime = df.LocalDateTime.str[:-4]  # need this line for aa_downtown_all and videoTripsUpdate
    #df.LocalDateTime = df.LocalDateTime.str[:-2]
    df['epoch'] = pd.to_datetime(df.LocalDateTime).astype(int)/1000000000
    lineStrings, endPoints, tripIds, devices, startPoints = [], [], [], [], []
    for tripId in df.index.unique():
        #print len(df.ix[tripId])
        if len(df.ix[tripId]) > 20:
            try:
                df2 = df.ix[tripId]
                df2.sort_values(by='epoch')
                tripIds += [tripId]
                #print str(df2.iloc[-1].Longitude),str(df2.iloc[-1].Latitude)
                endPoint = 'POINT (' + str(df2.iloc[-1].Longitude) + ' ' + str(df2.iloc[-1].Latitude) + ')'
                endPoints+=[endPoint]
                
                startPoint = 'POINT (' + str(df2.iloc[0].Longitude) + ' ' + str(df2.iloc[0].Latitude) + ')'
                startPoints +=[startPoint]
                
                assert len(df2.Device.unique())==1
                devices += [df2.iloc[0].Device]
        
                coords = zip(df2.Longitude.values.astype(str), df2.Latitude.values.astype(str), df2.epoch.astype(int).values.astype(str))
                lineString = 'LINESTRING M (' +', '.join([' '.join(cc) for cc in coords])+')'
                lineStrings+=[lineString]
            except AttributeError:
                lineStrings+=[]
                endPoints+=[]
                devices += []
                startPoints +=[]
                #tripIds += []
                
                print 'Attribute Error'
                continue
                
    tripDf = pd.DataFrame({'geomwkt':lineStrings, 'end_geomwkt':endPoints, 'trip_id':tripIds, 'device':devices, 'start_geomwkt':startPoints})
    tripDf.to_sql('video_traces', engine, schema='parking', if_exists='replace', index=False)  
    db.fix_permissions_of_new_table('video_traces')

    db.execute('''ALTER TABLE video_traces ADD COLUMN lines_geom geometry(LineStringM, %s), 
                                            ADD COLUMN start_geom geometry(Point, %s),
                                            ADD COLUMN end_geom geometry(Point, %s)''' % (defaults['srs']['mi'], defaults['srs']['mi'], defaults['srs']['mi']))
    db.execute('UPDATE video_traces SET lines_geom=ST_Transform(ST_GeomFromText(geomWKT,4326), %s)' % defaults['srs']['mi'])
    db.execute('UPDATE video_traces SET end_geom=ST_Transform(ST_GeomFromText(end_geomwkt,4326), %s)' % defaults['srs']['mi'])
    db.execute('UPDATE video_traces SET start_geom=ST_Transform(ST_GeomFromText(start_geomwkt,4326), %s)' % defaults['srs']['mi'])

    db.execute('CREATE UNIQUE INDEX video_idx ON video_traces (trip_id);')
    db.execute('CREATE INDEX video_lines_spidx ON video_traces USING GIST (lines_geom);')
    db.execute('CREATE INDEX video_ends_spidx ON video_traces USING GIST (end_geom);')
    
    # Drop trips that do not end within Ann Arbor
    db.execute('SELECT COUNT(*) FROM video_traces, (SELECT ST_Union(geom) AS tractgeom FROM mi_tracts) t1 WHERE ST_Disjoint(end_geom, tractgeom);')
    print 'Dropping %d trips not within Ann Arbor' % (db.fetchall()[0][0])
    db.execute('DELETE FROM video_traces USING (SELECT ST_Union(geom) AS tractgeom FROM mi_tracts) t1  WHERE ST_Disjoint(end_geom, tractgeom);')

    db.execute('GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA parking TO parkingusers;')
    db.execute('ALTER TABLE video_traces OWNER TO parkingusers;')


    print "...done. Loaded video traces table"
    
def loadTSDC():
    """
    Loads the Caltrans GPS traces into a temporary table, where we can do analysis
    """
    print "\nLoading Caltrans data..."

    db = mmt.dbConnection(pgLogin=pgInfo)
    
    db.execute('DROP TABLE IF EXISTS public.caltrans_traces_sf')
    db.execute('DROP TABLE IF EXISTS public.caltrans_traces_ca')

    if 0:  # this grabs the original gps traces
        suf = 'sf'
        db.execute('''CREATE TABLE public.caltrans_traces AS (                
                        SELECT sampno, vehno, end_ts AS endtime, ST_Transform(geom, 3493) AS lines_geom
                        FROM original_raw.gpstrips_v, (SELECT ST_Union(geom_way) as allstreets FROM sf_streets) t1
                        WHERE ST_DWithin(allstreets, ST_Transform(ST_Endpoint(geom), 3493), 100));''')
    if 0: # this grabs and merges the map_matched trace. But there seems to be a lot of problems with the map matching. Theirs is worse than ours.
        suf = 'sf'
        db.execute('''CREATE TABLE public.caltrans_traces AS (                
                            WITH allstreets AS (SELECT ST_Union(geom_way) AS street_geom from sf_streets),
                        sftrips AS (SELECT gpstripid, sampno, vehno FROM original_raw.v_gpstrips, allstreets
                        WHERE ST_DWithin(street_geom, ST_Transform(ST_Endpoint(geom), 3493), 200)) 
                        SELECT gpstripid, sampno, vehno, MAX(end_time) AS endtime, 
                        ST_Transform(ST_Linemerge(ST_Collect(ST_Linemerge(geom) ORDER BY featureorder)), 3493) AS lines_geom
                        FROM (SELECT t1.gpstripid, t1.sampno, t1.vehno, featureorder, end_time,
                                  CASE WHEN direction = 1 THEN ST_Linemerge(geom) ELSE ST_Reverse(ST_Linemerge(geom)) END as geom
                            FROM original_raw.v_gpslinks AS t1, sftrips AS t2
                            WHERE  t1.gpstripid=t2.gpstripid AND t1.sampno=t2.sampno AND t1.vehno=t2.vehno) t3
                        GROUP BY gpstripid, sampno, vehno);''')   
    
    if 1:
        # grab the original point level data, which lets us do our own map matching
        for region in ['ca','sf']:
            where_clause = '' if region=='ca' else 'WHERE ST_DWithin(street_geom, ST_Transform(ST_Endpoint(geom), 3493), 200)'
            db.execute('''CREATE TABLE public.caltrans_traces_%s AS (
                         WITH allstreets AS (SELECT ST_Union(geom_way) AS street_geom from sf_streets),
                              sftrips AS (SELECT gpstripid, sampno, vehno FROM original_raw.v_gpstrips, allstreets %s)
                        SELECT gpstripid, sampno, vehno, endtime, ST_Transform(line, 3493) AS lines_geom FROM
                            (SELECT t1.gpstripid, t1.sampno, t1.vehno, MAX(time_local) AS endtime, 
                                ST_MakeLine(ST_SetSRID(ST_MakePointM(longitude, latitude, cast(extract(epoch from time_local) as integer)), 4326) ORDER BY localid) AS line
                                FROM original_raw.v_points t1, sftrips AS t2
                                WHERE  t1.gpstripid=t2.gpstripid AND t1.sampno=t2.sampno AND t1.vehno=t2.vehno
                                GROUP BY t1.gpstripid, t1.sampno, t1.vehno) t3);''' % (region, where_clause))
                                
            db.execute('''ALTER TABLE caltrans_traces_%s ADD COLUMN trip_id SERIAL UNIQUE, ADD COLUMN end_geom geometry(PointM, %s), 
                        ADD COLUMN start_geom geometry(PointM, %s);''' % (region, defaults['srs'][region], defaults['srs'][region]))
            db.execute('UPDATE caltrans_traces_%s SET start_geom=ST_Startpoint(lines_geom);' % region)
            db.execute('UPDATE caltrans_traces_%s SET end_geom=ST_Endpoint(lines_geom);' % region)
            db.execute('CREATE INDEX caltrans_%s_lines_spidx ON caltrans_traces_%s USING GIST (lines_geom);' % (region, region))
            db.execute('CREATE INDEX caltrans_%s_ends_spidx ON caltrans_traces_%s USING GIST (end_geom);' % (region, region))
            db.execute('CREATE INDEX caltrans_%s_trip_id_idx ON caltrans_traces_%s (trip_id);' % (region, region))
            db.execute('GRANT ALL ON TABLE caltrans_traces_%s TO caltrans_user;' % region)
    
    return

def estimateMapMatchingModel():
    """
    in progress. need to formalize this in the workflow - create traces_test and then estimate the model
    """
    qp=mm.qualityPredictor('traces_qaqc', 'trip_id', 'lbuff_geom', path=paths['output'])
    qp.estimateQualityModel()
    
if __name__ == '__main__':
    runmode= 'default' if len(sys.argv)<2 else sys.argv[1].lower()
    
    if runmode in ['default','tables']:
        loadTables()
    if runmode in ['default','streetlight']:
        loadStreetLight()
    if runmode in ['default','survey']:
        loadSurveyTraces()
    if runmode in ['default','video']:
        loadVideoTrips()
    if runmode in ['tsdc']:
        loadTSDC()


    
