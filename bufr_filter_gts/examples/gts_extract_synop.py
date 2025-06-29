#! /usr/bin/env python3
# run with "gts_extract_bufr.py 2018040318 [GTS_path] [SQL_path] > 2018040318.log & "
#import bufr_filter_gts.synop_extractor as synop
import synop_extractor as synop
import sys
import datetime as dt

GTS_path = "GTS"
SQL_path = "."
BUFR_path = "."

if len(sys.argv) < 2 :
  print("ERROR: YYYYMMDDHH must be provided!")
else :
  if len(sys.argv) > 2 :
    GTS_path = str(sys.argv[2])
  if len(sys.argv) > 3 :
    SQL_path = str(sys.argv[3])

  datestr = str(sys.argv[1])
  print('Date string: ' + datestr)
  mydate = dt.datetime.strptime(datestr, "%Y%m%d%H")

  synop.update_sqlite(mydate, SQL_path, GTS_path, obs_window_size=60)
  synop.bufr_make_output(mydate, SQL_path, BUFR_path)


