## basic GTS-SYNOP observation monitor
## - scan the GTS directories every N minutes
## - make an SQLite database of all available messages
## - basic filtering for empty, corrupted data
## Alex Deckmyn, 2018
## 2019-11-08 ported to python3

#import numpy
from eccodes import *
import datetime as dt
import os
import sqlite3

# time window: e.g. 19:30 -- 20:29
def obs_window(cycledate, nmin=30) :
  return(cycledate - dt.timedelta(hours=0, minutes=nmin-1),
         cycledate + dt.timedelta(hours=0, minutes=nmin))

def sqlite_filename(cycle_date, SQL_path) :
  filename = os.path.join(SQL_path, 'synop_' + cycle_date.strftime('%Y%m%d%H%M') + '.sqlite' ) 
  return filename 

def output_filename(cycle_date, BUFR_path) :
  filename = os.path.join(BUFR_path, 'synop_' + cycle_date.strftime('%Y%m%d%H%M') + '.BUFR' ) 
  return filename 

print_debug_on = bool(os.environ.get('BUFR_EXTRACTOR_DEBUG'))
def print_debug(*args) :
  # NOT SO NICE: reading environment every time!
#  print_dedug_on = bool(os.environ.get('BUFR_EXTRACTOR_DEBUG'))
  if print_debug_on :
    print(*args)
##################

# return TRUE if the header is useful
def gts_filter(gtsheader) :
  # only BUFR-SYNOP
  if gtsheader['TT'] != 'IS' :
    return False
  # only northern hemisphere, Europe...
  if not gtsheader['AA'][1] in ['A', 'D', 'N', 'X']:
    return False
#  if gtsheaders['AA'][1] in ['B', 'C', 'E', 'F', 'G', 'H','I', 'J', 'K', 'L', 'S', 'T']:
#    return False

  return True


def get_gts_headers(filename) :
## there is always only 1 GTS message in a file (you could get header from filename, too)
## we also count the number of BUFR messages
  keylist=["TT","AA","II","CCCC","YY","GG","gg","BBB"]
  f1 = open(filename)

  GTS_header={}
  msg = codes_gts_new_from_file(f1)
  if msg is None :
    print_debug('... no GTS header found')
    f1.close()
    return None
# It may happen that a GTS message can not be decoded by ecCodes (mal-formed header?)
  for key in keylist :
    GTS_header[key] = codes_get(msg, key)
  codes_release(msg)
  f1.close()
  return GTS_header

def gts_from_filename(fullname) :
# only works for TTAAII_CCCC_YYGGgg[_BBB]
  filename = os.path.basename(fullname)
  if len(filename) != 18 and len(filename) != 22 :
    return None
  if filename[6] != '_' or filename[11] != '_' :
    return None

  gtsheader = { 'TT' : filename[0:2], 'AA' : filename[2:4], 'II' : filename[4:6], \
                'CCCC' : filename[7:11], 'count' : 1, 'BBB' : 'NNN', \
                'YY' : filename[12:14], 'GG' : filename[14:16], 'gg' : filename[16:18] }
  if len(filename) == 22 :
    if filename[18] != '_' :
      return None
    gtsheader['BBB'] = filename[19:22]

  return gtsheader

# GTS tags only give day-of-the-month
# so you must derive month and year by looking at current date
# SHould I also consider hour ? Naaah...
def gts_date(gts, basedate) :
  # BUG: this will give an error if YY=31 and basedate is 30
  # because you get e.g. 31th of April -> big crash
  # this can happen on 31 May in 23h directory...
  # so we give 1 day margin
  if int(gts['YY']) <= basedate.day + 1 :
    gyear = basedate.year
    gmonth = basedate.month
  else :
    if basedate.month > 1 :
      gyear = basedate.year
      gmonth = basedate.month - 1
    else :
      gmonth = basedate.month - 1
      gyear = basedate.year - 1
  try : 
    result = dt.datetime(gyear, gmonth, int(gts['YY']), int(gts['GG']), int(gts['gg']))
  except :
    print_debug("... GTS Date error"+" "+str(gyear)+" "+str(gmonth)+" "+gts['YY']+" "+gts['GG']+" "+gts['gg'])
    result = None
  return result

# TODO: this fails for too many files:w
# What to do if blockNumber, stationNumber are not defined for the subsets?
# rounding for lat/lon (not so important)
# sometimes GTS header seems corrupted, but the rest is OK...
def parse_subsets (filename) :
  if not os.path.exists(filename) :
    return 1
  f1 = open(filename)

  try :
    bufr_count = codes_count_in_file(f1)
  except CodesInternalError as err :
    print_debug("... error counting BUFR messages")
    bufr_count = 0

  if bufr_count == 0 :
    print_debug('... No valid BUFR message')
    return None
#    return {count:0}
  if bufr_count > 1 :
    print_debug('... More than 1 BUFR message')
    return None
#    return {count:bufr_count}

# we must allow identifiers for land stations, buoys, ships...
  subset_keys=["longitude", "latitude"]
  SID_keys = [ "blockNumber", "stationNumber",
               "shipOrMobileLandStationIdentifier",
               'buoyOrPlatformIdentifier',
               'stationaryBuoyPlatformIdentifierEGCManBuoys']
  main_keys=["typicalDate", "typicalTime"]

  bufr_labels = None
  f1.seek(0)
  while 1 :
    try :
      bmsg = codes_bufr_new_from_file(f1)
      if bmsg is None :
        break
#      bufr_count += 1
#      if bufr_count > 1 :
#        print_debug("More than 1 BUFR message in file !")
      subset_count = codes_get(bmsg, "numberOfSubsets")
#      print_debug("BUFR message has "+str(subset_count)+" submessages")
      compressed = codes_get(bmsg, 'compressedData')
      codes_set(bmsg, 'unpack', 1)

      bufr_labels=[ {'subset':i+1} for i in range(subset_count) ]
      for key in main_keys :
        bkey = codes_get(bmsg, key)
        for i in range(subset_count) : bufr_labels[i][key] = bkey

      for key in subset_keys :
        try :
          if compressed or subset_count == 1 :
            bkey = codes_get(bmsg, key)
            bufr_labels[0][key] = bkey
          else :
            for i in range(subset_count) :
#              bkey = codes_get(bmsg, '/subsetNumber=%d/%s' % (i+1, key))
# sometimes this doesn't work ("subsetNumber" not defined?), but the following does:
              bkey = codes_get(bmsg, '#%d#%s' % (i+1, key))
              bufr_labels[i][key] = bkey
        except CodesInternalError as err :
          print_debug('... error reading BUFR key ' + key)
          for i in range(subset_count) : bufr_labels[i][key] = None

      for i in range(subset_count) : bufr_labels[i]['SID'] = ''
# stationNumber can be combined with blockNumber, but then it MUST be 3 characters
      for key in SID_keys :
        try :
          if not codes_is_defined(bmsg, key) : continue
          if subset_count == 1 :
            bkey = codes_get(bmsg, key)
            if key=='blockNumber' :
              bkey =  '%02d' % bkey
            if key=='stationNumber' :
              bkey = '%03d' % bkey
            if key=='buoyOrPlatformIdentifier' :
              bkey = '%05d' % bkey
            bufr_labels[0]['SID'] += bkey

          else :
            if compressed :
              klen = codes_get_size(bmsg, key)
              if klen == 1 :
                bkey = codes_get(bmsg, key)
                if key=='blockNumber' : bkey =  '%02d' % bkey
                if key=='stationNumber' : bkey = '%03d' % bkey
                if key=='buoyOrPlatformIdentifier' : bkey = '%05d' % bkey
                for i in range(subset_count) : bufr_labels[i]['SID'] += bkey
              else :
                bkey = codes_get_array(bmsg, key)
                if key=='blockNumber' :
                  bkey = [ '%02d' % bkey[i] for i in range(subset_count)]
                if key=='stationNumber' :
                  bkey = [ '%03d' % bkey[i] for i in range(subset_count)]
                if key=='buoyOrPlatformIdentifier' :
                  bkey = [ '%05d' % bkey[i] for i in range(subset_count)]
                for i in range(subset_count) : bufr_labels[i]['SID'] += bkey[i]

            else :
              for i in range(subset_count) :
# BUG for shipOrMobileLandStationIdentifier :
#                bkey = codes_get(bmsg, '/subsetNumber=%d/%s' % (i+1, key))
                bkey = codes_get(bmsg, '#%d#%s' % (i+1, key))
                if key=='blockNumber' :
                  bkey = '%0*d' % (2, bkey)
                if key=='stationNumber' :
                  bkey = '%03d' % bkey
                if key=='buoyOrPlatformIdentifier' :
                  bkey = '%05d' % bkey
                bufr_labels[i]['SID'] += bkey

        except CodesInternalError as err :
          print_debug('... error reading BUFR key ' + key)

      codes_release(bmsg)
    except CodesInternalError as err :
      print_debug("... error reading BUFR message")
      codes_release(bmsg)

  f1.close()
  return bufr_labels  



def parse_file(fullname, mindate, maxdate) :
  print_debug('Parsing: '+fullname)
  gtsheader = get_gts_headers(fullname)
  # local files may not have a transmission sequence number
  # so ecCodes can not read the GTS headers
  if gtsheader is None :
    print_debug('... Trying GTS from file name')
    gtsheader = gts_from_filename(fullname)
    if gtsheader is None :
      print_debug('... No valid GTS header ')
      return None

  gdt = gts_date(gtsheader, maxdate)
  if gdt is None :
    print_debug('... No valid date retrieved.')
    return None

  if gdt < mindate or gdt > maxdate :
    print_debug ('... Not in time window')
    return None

  if not gts_filter(gtsheader) :
    print_debug('... Not in Europe')
    return None

  gtsheader['TIMESTAMP'] = gdt.strftime('%Y%m%d-%H%M%S')
  bufrlist = parse_subsets(fullname)

  if bufrlist is None :
    return None

  subcount = len(bufrlist)
#      if subcount > 1 : print_debug "SUBSETS YEAHA"
#      print_debug sublist
  return {'subcount':subcount, 'gtsheader':gtsheader, 'bufrlist':bufrlist}

# you have two entries with the same keys
# which one has priority?
# we use the BBB tag
# criteria:
#   same : dupliate message, so it doesn't matter
#   CCA wins over orginal, looses from CCB etc
#   ???
# Can we assume that "ammendments" etc will not have the same stationNumber key?read first
# We do NOT assume that the messages are always read in order of arrival
# so the correction may be read first...
def gts_priority(row1, row2) :
# return True if row1 has "priority" on row2 OR if they are equal
#
  if row2[0:2]=='CC' :
    if row1[0:2]=='CC' :
      return row2 <= row1
    else :
      return False
  else :
    return True


def update_sqlite(cycle_date, SQL_path, GTS_path, obs_window_size=60) :
# if SQLite already exists:
#   read meta-table for last dir read, min/maxdate, 
# else:
#   calculate min/maxdate, first-dir = mindate
#   create meta-table & data-table  
  sqlitefile = sqlite_filename(cycle_date, SQL_path)
  print('========================')
  print('= SYNOP GTS monitor    =')
  print('========================')
  print('Writing GTS data to ' + sqlitefile)
  begintime = dt.datetime.today().strftime("%Y%m%d %H:%M:%S")
# you create the file by just opening it
  db = sqlite3.connect(sqlitefile)
  meta = check_create_metatable(db, cycle_date, obs_window_size)
  obslist = check_create_datatable(db)

  current = dt.datetime.strptime(meta['lastdir'], '%Y%m%d%H')
  # To get all GTS messages for a particular date/time, we need to look in all GTS input directories that
  #   were created afterwards!
  # Also, to avoid missing a few files that arrive just around "real time", 
  #   we also look in the directory started 1h before current time.
  # we start in the same directory we finished last time (some double work...)
  while 1 :
    newdir = current.strftime('%Y%m%d%H')
    gtsdir = os.path.join(GTS_path, newdir)
    current = current + dt.timedelta(hours=1)

    if current > cycle_date + dt.timedelta(hours=48) : 
      print('Stopping GTS parsing at 48h after obs date')
      break

    if not os.path.exists(gtsdir) :
      if current > dt.datetime.utcnow() :
         print("Directory " + newdir +" doesn't exist yet.")
         break
      else :
         print("Directory " + newdir +" doesn't exist.")
         continue

    print("Scanning directory "+newdir)
    # 1. update the "last visited" directory (only for ">")
    db.execute("UPDATE meta SET lastdir=?",(newdir,))
    db.commit()
    # 2. get the full list of BUFR messages
    file_list = os.listdir(gtsdir)

    # some query templates :
# THIS DOESN'T WORK... (no parameters allowed in VIEW)
    select_gts = "CREATE TEMP VIEW gts as SELECT * FROM data WHERE \
                  TT=':TT' AND AA=':AA' AND II=':II' \
                  AND CCCC=':CCCC' AND TIMESTAMP=':TIMESTAMP'"
# this works
#                  "CREATE TEMP VIEW gts as SELECT * FROM data WHERE \
#                  TT='IS' AND AA='NN' AND II='01' \
#                  AND CCCC='EGRR' AND TIMESTAMP='20180228-220000'"
    sql_test = "SELECT * FROM data WHERE \
                  TT=:TT AND AA=:AA AND II=:II \
                  AND CCCC=:CCCC AND TIMESTAMP=:TIMESTAMP"

    select_sid = "SELECT * from gts WHERE SID=:SID"
    write_obs = "INSERT INTO data VALUES ( \
                   :TT, :AA, :II, :CCCC,\
                   :TIMESTAMP, :BBB, \
                   :SID, :filename, :subset)"
    replace_obs = "UPDATE data SET \
                   filename=:filename, subset=:subset,\
                   BBB=:BBB WHERE SID=:SID AND \
                   TT=:TT AND AA=:AA AND II=:II \
                   AND CCCC=:CCCC AND TIMESTAMP=:TIMESTAMP"

 
    for filename in file_list :
      fullname = os.path.join(gtsdir, filename)
      flist = parse_file(fullname, meta['mindate'], meta['maxdate'])
      if flist is None : 
#        print '   ---> nothing to do'
        continue
    # 3. now compare to the already existing obs
      # first extract a smaller table 'gts' for the current GTS header
      # TTAAII CCCC YYGGgg
      select_gts = "CREATE TEMP VIEW gts as SELECT * FROM data WHERE \
                  TT='%s' AND AA='%s' AND II='%s' \
                  AND CCCC='%s' AND TIMESTAMP='%s' " % \
                  tuple(flist['gtsheader'][i] for i in ['TT','AA','II','CCCC','TIMESTAMP'])

      db.execute(select_gts)
      db.commit()

      for i in range(flist['subcount']) :
# COMPARE SPEED: find in dictionary or in SQLite
#        z1 = db.executemany("SELECT * from gts WHERE key= ?", [(fsub['key'])])
#        x1 = z1.fetchall()
# BUG: [i] may not work if subcount=1?
        z1 = db.execute(select_sid, flist['bufrlist'][i])
        x1 = z1.fetchall()
#        print 'i=%d len(x1)=%d' %(i,len(x1))
        if len(x1) > 1 :
           print_debug("ERROR: more than 1 instance for found")
           return 1
        if len(x1) > 0 :
#          print_debug 'PRIORITY CHECK subset %d' % i
          if not gts_priority(x1[0][1], flist['gtsheader']['BBB']) :
            print_debug("REPLACING %d" % i)
            allkeys = {'filename':fullname}
            allkeys.update(flist['gtsheader'])
            allkeys.update(flist['bufrlist'][i])
            db.execute(replace_obs, allkeys)

        else :
          print_debug("ADDING %d" % i)
          allkeys = {'filename':fullname}
          allkeys.update(flist['gtsheader'])
          allkeys.update(flist['bufrlist'][i])
          db.execute(write_obs, allkeys)

      db.execute('DROP VIEW gts')
      db.commit()
  print('begin: '+begintime)
  print('end: '+dt.datetime.today().strftime("%Y%m%d %H:%M:%S"))
  print('= SQLITE FINISHED =')

def check_create_metatable(db, cycle_date, obs_window_size=60) :
  check_meta = "SELECT name FROM sqlite_master WHERE type='table' AND name='meta';"
  z1 = db.execute(check_meta)
  x1 = z1.fetchall()

  if len(x1) < 1 :
    table_def_meta = 'CREATE TABLE IF NOT EXISTS meta ( \
                       cycledate date, mindate datetime, \
                       maxdate date, lastdir VARCHAR[8])'
    db.execute(table_def_meta)
    db.commit()
    (mindate, maxdate) = obs_window(cycle_date, obs_window_size/2)
    lastdir = mindate.strftime('%Y%m%d%H')
    value_template = '(' + 3*'?,' + '?)'
    db.execute("INSERT INTO meta VALUES " + value_template,\
                   (cycle_date, mindate, maxdate, lastdir))
    db.commit()

  # now read meta data 
  get_meta = "SELECT * FROM meta"
  z2 = db.execute(get_meta)
  x2 = z2.fetchall()
  colnames = list(zip(*z2.description))[0]
  result = dict(list(zip(*[colnames, x2[0] ])))
  result['cycledate'] = dt.datetime.strptime(result['cycledate'], "%Y-%m-%d %H:%M:%S")
  result['mindate'] = dt.datetime.strptime(result['mindate'], "%Y-%m-%d %H:%M:%S")
  result['maxdate'] = dt.datetime.strptime(result['maxdate'], "%Y-%m-%d %H:%M:%S")
  return result

def check_create_datatable(db) :
  #keylist=["TT","AA","II","CCCC","YY","GG","gg","BBB"]
  # CONSIDER : TTAAII CCCC YYGGgg BBB
  # table names GG and gg are the same...
  table_def_data = 'CREATE TABLE IF NOT EXISTS data ( \
                    TT VARCHAR[2], AA VARCHAR[2], II VARCHAR[2], \
                    CCCC VARCHAR[4],\
                    TIMESTAMP VARCHAR[15],\
                    BBB VARCHAR[3], \
                    SID VARCHAR,\
                    filename VARCHAR, subset INTEGER)'
  db.execute(table_def_data)
  db.commit()

  # now read data (?)
  get_data = "SELECT * FROM data"
  z2 = db.execute(get_data)
  x2 = z2.fetchall()
  if len(x2) > 0 :
    colnames = list(zip(*z2.description))[0]
    mykey = list(zip(*x2))[0]
    result = dict((y,x) for x in x2[0] for y in mykey)
  else :
    result = None
  return result

##########################################################
# to be run regularly: clean up old messages in SQLite file
def cleanup_sqlite(filename, mindate) :
  # remove all entries prior to mindate
  table_remove_old = "DELETE FROM data \
                      WHERE TIMESTAMP < '%s'"
  
  db.execute(table_remove_old)
  db.commit()

##########################################################
# to be run for every required data set
def bufr_extract(filename, subsetnr, outfile) :
  if not os.path.exists(filename) :
    print('input file does not exist: ' + filename)
    return 1
  f1 = open(filename)
  bmsg = codes_bufr_new_from_file(f1)

#  print "%s %d" %(filename,subsetnr)

  subset_count = codes_get(bmsg, "numberOfSubsets")
  if subsetnr > subset_count :
    print('subset number too big')
    return 1
  codes_set(bmsg, 'unpack', 1)
  try :
    if subset_count > 1 :
      codes_set(bmsg, "extractSubset", subsetnr)
      codes_set(bmsg,'doExtractSubsets',1)
      bmsg2 = codes_clone(bmsg)
      codes_write(bmsg2, outfile)
      codes_release(bmsg2)
    else :
      codes_write(bmsg, outfile)
  except CodesInternalError as err :
    print('I tried but failed: \n   '+filename)
    sys.stderr.write(err.msg + '\n')
 
  codes_release(bmsg)
  f1.close()

def bufr_make_output(cycle_date, SQL_path, BUFR_path) :
  sqlitefile = sqlite_filename(cycle_date, SQL_path)
  print('========================')
  print('= SYNOP BUFR extractor =')
  print('========================')
  print('Reading SQLite entries from '+sqlitefile)
  begintime = dt.datetime.today().strftime("%Y%m%d %H:%M:%S")
  if not os.path.exists(sqlitefile) :
    print('ERROR: file '+sqlitefile+' not found')
    return 1

  db = sqlite3.connect(sqlitefile)
  outfile = output_filename(cycle_date, BUFR_path)
# TODO
  bufrfile = open(outfile, "ab")
  print('Writing BUFR messages to '+outfile)

  alldata = db.execute('SELECT * from data')
  msgcount = 0
  while 1 :
    nextcase = alldata.fetchone()
    if nextcase is None :
      break
    msgcount = msgcount + 1
#    print "message %d" % msgcount
    # this return a tuple, so we must know the column numbers
    (filename, subset) = (nextcase[7], nextcase[8])
    bufr_extract(filename, subset, bufrfile)

  bufrfile.close()
  print('extracted %i BUFR messages' % msgcount)
  print('begin: '+begintime)
  print('end: '+dt.datetime.today().strftime("%Y%m%d %H:%M:%S"))
  print('= BUFR FINISHED =')


