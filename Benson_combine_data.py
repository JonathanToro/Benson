from __future__ import division, print_function
from sqlalchemy import create_engine
from glob import glob
import os,os.path
import datetime
import pandas as pd
from pandas.io.pytables import HDFStore

HDF = False
# For only testing using subset of data
START = 0
CUTOFF = None

DB_FILES = glob('../datasets/mta[0-9].db')
db_files = DB_FILES[START:CUTOFF]

OUT_FILE = '../datasets/mta.h5'
INFO_FILE = '../datasets/Remote-Booth-Station.xls'

if os.path.exists(OUT_FILE):
    os.unlink(OUT_FILE)
#td_zero = datetime.datetime(2016,1,1) - datetime.datetime(2016,1,1)
store = HDFStore(OUT_FILE,complevel=9, complib='zlib')


def get_info(info_file=INFO_FILE):
	info = pd.read_excel(INFO_FILE)
	return dict(zip(info[['Remote','Booth']].apply(
							lambda t: tuple(t),
							axis=1).values,
					info.Station.values))

def get_sql(fp):
    # get dataframe from MTA sqlite file
    print(fp)
    engine =create_engine('sqlite:///{}'.format(fp))
    with engine.connect() as conn,conn.begin():
        df = pd.read_sql_table('mta',conn)
    df.info()
    return df

def save_sql(data,fp):
    # get dataframe from MTA sqlite file
    print(fp)
    engine =create_engine('sqlite:///{}'.format(fp))
    with engine.connect() as conn,conn.begin():
        df.to_sql('mta',conn,if_exists='replace',index=False,chunksize=1000000)
    return True

def get_hdf(fp=OUT_FILE):
    # so cool
    with HDFStore(fp) as store:
        # HDFStore keys are a path structure (allow nesting!!!), so need to remove '/'
        # from beginning to match original key format.
        panel = pd.Panel.from_dict({key.strip('/'):store[key] for key in store.keys()})
    return panel


def good_dates(x,y=datetime.timedelta(0),info=get_info()):
    # will filter out diff values < 0 and nan (first diff)
    # because nan cmp anything is False
    # N.B. This filters out whole TURNSTILES.
    # Need to see which are being removed,
    # may be popular ones.
    #
    # Alt idea: sort if dates aren't in order?
    # Save to extra csv files?
    #
    d = x.date.diff()
    try:
        val = d.min() >= y
        if not val:
            key = x.head(1).key.values[0]
            station = info.get(tuple(key.split(';')[:2]),'UNKNOWN')
            name = '../datasets/' + station + '_' + key.replace(';','_') + '.csv'
            print(name)
            x.to_csv(name)
        return val
    except:
        key = x.head(1).key.values[0]
        station = info.get(tuple(key.split(';')[:2]),'UNKNOWN')
        name = '../datasets/' + station + '_' + key.replace(';','_') + '.csv'
        print(name)
        x.to_csv(name)
        return False

def remove_blips(x):
    '''
    Should remove all single outliers and one adjacent data point...

    Not many, so removes anything with outlier
    in entrances or exits.

    e.g.  ------^------
    or    ------v------

    will get tripped up by consecutive outliers
    e.g. _____/-------\_______

    '''
    de = x.entries.diff()
    de2 = x.entries.shift(-1) - x.entries
    dx = x.exits.diff()
    dx2 = x.exits.shift(-1) - x.exits
    return x[(de >= 0) & (de2 >= 0) & (dx >= 0) & (dx2 >= 0)]

def max_day(x):
    '''
    Take all values for a turnstile for a day
    and return the maximum value.
    '''
    name = x.head(1).key.values[0].replace(';','_') # make name valid Python identifier
    print(name)
    x['day'] = x['date'].apply(lambda d: datetime.datetime(d.year,d.month,d.day))
    new_df = x[['day','entries','exits']].groupby('day').max()
    store.put(name,new_df)
    return new_df


df = None
for db in db_files:
    if df is None:
        df = get_sql(db)
    else:
        df = pd.concat([df,get_sql(db)])


#df = df[:10000]
print('Total Database:\n')
df.info()
print('\n\n\n')
print('Original:',len(df))
df = df.groupby('key').filter(good_dates)
print('Removed Bad Dates:',len(df))
df = df.groupby('key').apply(remove_blips)
print('Removed Negative Diffs:',len(df))
df = df.groupby('key').apply(max_day)
print(df.head())
'''
if HDF is True:
    df.to_hdf('../datasets/mta.hdf','mta')
    print('Saved output to ../datasets/mta.hdf')
else:
    save_sql(df,'../datasets/mta_day.db')
    print('Saved output to ../datasets/mta_day.db')
'''






# Cleanup

store.close()
