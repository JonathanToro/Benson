from __future__ import division, print_function
from requests import get
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import cPickle as pk
import sys
import os
import os.path
import sqlite3
from collections import defaultdict


# Grabs all data from MTA website in chunks of 50 files.
# Takes one argument on command line N (an integer) and
# processes MTA files 50*N:50*N+50


assert len(sys.argv) > 1

N = int(sys.argv[-1])

LOGLEVEL = 0
DB_FILE = '../datasets/mta{}.db'
STATION_INFO_FILE = '../datasets/mta_station_info.pk'
OVERWRITE = True

if OVERWRITE:
    for fp in (DB_FILE, STATION_INFO_FILE):
        if os.path.exists(fp):
            os.unlink(fp)


def debug(*args):
    if LOGLEVEL < 1:
        print('DEBUG: ' + ' '.join(map(str, args)))


def info(*args):
    if LOGLEVEL < 2:
        print('INFO: ' + ' '.join(map(str, args)))


def warning(*args):
    if LOGLEVEL < 3:
        print('WARNING: ' + ' '.join(map(str, args)))

def create_db(n):
    db = sqlite3.connect(DB_FILE.format(n), detect_types=sqlite3.PARSE_DECLTYPES)
    c = db.cursor()
    # C/A_UNIT_SCP,DATETIME,DESC,ENTRIES,EXITS
    c.execute(
        'create table mta (key text,date timestamp, desc text, entries int, exits int)')
    return db,c



# # MTA Turnstile Data File Formats
#
# ## Before 2014
#
# C/A,UNIT,SCP,DATE1,TIME1,DESC1,ENTRIES1,EXITS1,DATE2,TIME2,DESC2,ENTRIES2,EXITS2,DATE3,TIME3,DESC3,ENTRIES3,EXITS3,DATE4,TIME4,DESC4,ENTRIES4,EXITS4,DATE5,TIME5,DESC5,ENTRIES5,EXITS5,DATE6,TIME6,DESC6,ENTRIES6,EXITS6,DATE7,TIME7,DESC7,ENTRIES7,EXITS7,DATE8,TIME8,DESC8,ENTRIES8,EXITS8
#
#
# C/A = Control Area (A002)
# UNIT = Remote Unit for a station (R051)
# SCP = Subunit Channel Position represents an specific address for a device (02-00-00)
# DATEn = Represents the date (MM-DD-YY)
# TIMEn = Represents the time (hh:mm:ss) for a scheduled audit event
# DEScn = Represent the "REGULAR" scheduled audit event (occurs every 4 hours)
# ENTRIESn = The comulative entry register value for a device
# EXISTn = The cumulative exit register value for a device
#
# ## After 2014
#
# C/A,UNIT,SCP,STATION,LINENAME,DIVISION,DATE,TIME,DESC,ENTRIES,EXITS
#
#
# C/A      = Control Area (A002)
# UNIT     = Remote Unit for a station (R051)
# SCP      = Subunit Channel Position represents an specific address for a device (02-00-00)
# STATION  = Represents the station name the device is located at
# LINENAME = Represents all train lines that can be boarded at this station
#            Normally lines are represented by one character.  LINENAME 456NQR repersents train server for 4, 5, 6, N, Q, and R trains.
# DIVISION = Represents the Line originally the station belonged to BMT, IRT, or IND
# DATE     = Represents the date (MM-DD-YY)
# TIME     = Represents the time (hh:mm:ss) for a scheduled audit event
# DESc     = Represent the "REGULAR" scheduled audit event (Normally occurs every 4 hours)
#            1. Audits may occur more that 4 hours due to planning, or troubleshooting activities.
#            2. Additionally, there may be a "RECOVR AUD" entry: This refers to a missed audit that was recovered.
# ENTRIES  = The comulative entry register value for a device
# EXIST    = The cumulative exit register value for a device
#

# # Challenge Set 1:  MTA Turnstile Data
#
# #### Exercise 1.1
#
# - Open up a new IPython notebook
# - Download a few MTA turnstile data files
# - Open up a file, use csv reader to read it, make a python dict where
#   there is a key for each (C/A, UNIT, SCP, STATION). These are the
#   first four columns. The value for this key should be a list of
#   lists. Each list in the list is the rest of the columns in a
#   row. For example, one key-value pair should look like
#
#
# {    ('A002','R051','02-00-00','LEXINGTON AVE'):
# [
# ['NQR456', 'BMT', '01/03/2015', '03:00:00', 'REGULAR', '0004945474', '0001675324'],
# ['NQR456', 'BMT', '01/03/2015', '07:00:00', 'REGULAR', '0004945478', '0001675333'],
# ['NQR456', 'BMT', '01/03/2015', '11:00:00', 'REGULAR', '0004945515', '0001675364'],
# ...
# ]
# }


OLD_HEADERS = 'C/A,UNIT,SCP,DATE1,TIME1,DESC1,ENTRIES1,EXITS1,DATE2,TIME2,DESC2,ENTRIES2,EXITS2,DATE3,TIME3,DESC3,ENTRIES3,EXITS3,DATE4,TIME4,DESC4,ENTRIES4,EXITS4,DATE5,TIME5,DESC5,ENTRIES5,EXITS5,DATE6,TIME6,DESC6,ENTRIES6,EXITS6,DATE7,TIME7,DESC7,ENTRIES7,EXITS7,DATE8,TIME8,DESC8,ENTRIES8,EXITS8'.split(
    ',')
HEADERS = 'C/A,UNIT,SCP,STATION,LINENAME,DIVISION,DATE,TIME,DESC,ENTRIES,EXITS'.split(
    ',')
LOGLEVEL = 1

# Note: Old headers don't include station name.
# could retrieve from 'http://web.mta.info/developers/resources/nyct/turnstile/Remote-Booth-Station.xls'
# or just keep track of (C/A,UNIT,SCP) and station name from later datasets.

# for strptime
date_fmt = '%m/%d/%Y'
alt_date_fmt = '%m-%d-%y'
time_fmt = '%H:%M:%S'
num_links = None  # set to int to limit number of datafiles to process


def get_data_links(url='http://web.mta.info/developers/turnstile.html'):
    info('Getting datalinks.')
    resp = get(url)
    assert resp.ok
    soup = BeautifulSoup(resp.content, 'lxml')
    links = [a.attrs.get('href', '') for a in soup.findAll('a')]
    data_links = sorted(['http://web.mta.info/developers/' +
                         a for a in links if a.endswith('txt')
                         and 'Field_Description' not in a])
    return data_links


def process_row(row, c):
    # C/A,UNIT,SCP,DATE,TIME,DESC,ENTRIES,EXITS
    if len(row) != 8:
        warning(row)
    else:
        key = ';'.join(row[:3])
        date, time, desc, entries, exits = row[3:]
        try:
            dt = datetime.datetime.strptime(date + ' ' + time,
                                            date_fmt + ' ' + time_fmt)
        except ValueError:
            dt = datetime.datetime.strptime(date + ' ' + time,
                                            alt_date_fmt + ' ' + time_fmt)
        entries = int(entries)
        exits = int(exits)
        c.execute('insert into mta values(?,?,?,?,?)',
                  (key, dt, desc, entries, exits))


def parse_data(url,c):
    debug('Loading ' + url)
    resp = get(url)
    assert resp.ok
    station_info = set()
    raw_data = [[_.strip() for _ in line.split(',')]
                for line in resp.content.strip().split('\n')]
    if raw_data[0] == HEADERS:
        raw_data = raw_data[1:]
        for row in raw_data:
            station_info.add(tuple(row[:6]))
            row = row[:3] + row[6:]
            try:
                process_row(row,c)
            except:
                warning('Exception:', url)
                return False
    else:
        for old_row in raw_data:
            # convert single rows from old format into multiple rows
            for row in [old_row[:3] + old_row[3 + 5 * i:8 + 5 * i]
                        for i in xrange(8)]:
                if len(row) == 3:
                    # not all rows have 8 timepoints
                    continue
                if len(row) < 8:
                    warning('Unexpected number of fields.', url)
                    return False
                try:
                    process_row(row,c)
                except:
                    warning('Exception', url)
                    return False
    return station_info

if __name__ == '__main__':
    db,c = create_db(N)
    count = 0
    data_links = get_data_links()
    all_station_info = set()
    for i, link in enumerate(data_links[50*N:50*(N+1)]):
        new_info = parse_data(link,c)
        if new_info is False:
            db.rollback()
            continue
        all_station_info.update(new_info)
        info('{}: {}/50'.format(link, i))
        db.commit()

    db.close()

    with open(STATION_INFO_FILE, 'wb') as f:
        pk.dump(all_station_info, f, -1)
