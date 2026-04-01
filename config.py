# -*- coding: utf-8 -*-

"""
Configuration file for cruisedetector
Save this file as `config.py`.
"""

""" 
===================
CONFIGURATION FOR:
cruising.py
===================
"""

"""
Defaults that the user should change
"""
import os, sys
from pathlib import Path

# 1. Basepath and output folder for log
# basePath = 'G:/Shared drives/Projects/3090_Cruising for Parking/Data/testing/'
basePath = 'C:/cruise_base/'

# 2. Which regions to load streets and other data for
defaults = {}
defaults['regions'] = ['sf','mi','wa','il','nyc']

# 3. Dictionary of coordinate reference systems for each region
# the crs (SRID) should be recognized by PostGIS
# if your region is missing, add it to the dictionary
# Per pgMapMatch requirements, these thresholds and your unit system must be in meters!
crs = {
     'ca':'3493',
     'sf':'3493',
     'mi':'2809',
     'wa':'2855', 
     'nyc':'32118' # '2263' but with meters
     }

# 4. Path for log file. Default is in your current directory
logPath = '.'

# 5. You may need to add a directory to your sys.path
sys.path.append(basePath)

# 6. Path for osm2po with no spaces
osm2poPath = "C:/cruise_base/" #'C:/Downloads/'
osm2poVersion = '5.5.16' # '5.5.1'

# 7. Location of mapmatching coefficient file (in this git repo). You shouldn't need to change this.
# https://stackoverflow.com/questions/3718657/how-do-you-properly-determine-the-current-script-directory
repoPath = Path(globals().get("__file__", "./_")).absolute().parent
coeffFn = str(repoPath) + '/mapmatching_coefficients.txt'

# 8. Specify number of processing cores to be used
cores = 4

"""
Key Parameters for cruising.py
Adjust for your context, but the defaults should be reasonable for most contexts. 
Per pgMapMatch requirements, these thresholds and your unit system must be in meters!
See comments for details.
"""
maxDistThres = 1400 # maximum distance that the trace can get from the endpoint, once it enters the 400m buffer
rollSecs = 30       # speed threshold for identification of walking segment at end of trip, and number of secs over which speed is calculated
wSpeed = 6          # assumed maximum walking speed (km/h)
bufferThresh = 0.5  # to be considered cruising, at least this fraction of the last portion of the trip must be within the 400m buffer
qualityCutoff = 0.9 # To be retained, a trip must have at least this probability of being good
r = '400'           # the buffer radius (meters)
rd = str(int(r)*2)  # radius of donut
mapmatch_timeout  = 300        # timeout for each individual postgres query, in seconds. Making it shorter will skip long and stubborn traces


""" 
===================
CONFIGURATION FOR:
cruising_importLocationData.py
===================
"""
### Column Names for Quadrant Data (Sample)
# device_id_NAME = 'device_id'
# id_type_NAME = 'id_type'
# latitude_NAME = 'latitude'
# longitude_NAME = 'longitude'
# h_acc_NAME = 'h_acc'
# timestamp_NAME = 'timestamp'

# points_table_column_renames = {
#      device_id_NAME:'device_id',
#      id_type_NAME:'id_type',
#      latitude_NAME:'latitude',
#      longitude_NAME:'longitude',
#      h_acc_NAME:'h_acc',
#      timestamp_NAME:'timestamp'
# }

### Column Names for INRIX Data
device_id_NAME = 'DEVICE_ID'
id_type_NAME = 'PROVIDER_TYPE'
latitude_NAME = 'LAT'
longitude_NAME = 'LON'
h_acc_NAME = 'h_acc'
timestamp_NAME = 'timestamp'

points_table_column_renames = {
     device_id_NAME:'device_id',
     id_type_NAME:'id_type',
     latitude_NAME:'latitude',
     longitude_NAME:'longitude',
     h_acc_NAME:'h_acc',
     timestamp_NAME:'timestamp'
}

# ### Custom Column Names for Other Data Provider
# device_id_NAME =
# id_type_NAME =
# latitude_NAME =
# longitude_NAME =
# h_acc_NAME =
# timestamp_NAME =
#
# points_table_column_renames = {
#      device_id_NAME:'device_id',
#      id_type_NAME:'id_type',
#      latitude_NAME:'latitude',
#      longitude_NAME:'longitude',
#      h_acc_NAME:'h_acc',
#      timestamp_NAME:'timestamp'
# }

"""
Key Parameters for cruising_importLocationData.py
Adjust for your context, but the defaults should be reasonable for most contexts. 
Per pgMapMatch requirements, these thresholds and your unit system must be in meters!
See comments for details.
"""
h_acc_Var = 65 #max horizontal accuracy threshold
ping_tot_Var = 10 #min number of pings per device
trip_ping_tot_Var = 10 #min number of pings per trip
trip_ping_avg_Var = 90 #max average interval of pings in a trip, in seconds
speed_Var = 180 # max plausible speed between pings, in projection units per second. (Default: 130 km/hr)
trip_start_Var = 600 # pause in pings to start a new trip
duration_Var = 300 # min duration for a trip from start to end, in seconds