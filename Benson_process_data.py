from __future__ import division, print_function
from multiprocessing import Pool,cpu_count

import pandas as pd
import numpy as np
import datetime

def process_group(t):
    name,group = t
    agg_subgroups = group.groupby('day')[['key','entries','exits']].max()
    return agg_subgroups


def applyParallel(dfGrouped,func=lambda x: x.max()):
    p = Pool(cpu_count())
    print('Created pool with %s processes.' % cpu_count())
    ret_list = p.map(process_group,[name_group for name_group in dfGrouped])
    p.close()
    print('Combining pool output.')
    return pd.concat(ret_list)

hdf_file = '../datasets/mta.hdf'

print('Loading data.')
df = pd.read_hdf(hdf_file)
#df = df.head(100000)

print('Creating day column.')
df['day'] = df['date'].apply(lambda d: (d.year,d.month,d.day))

print('Grouping by key and day')
grouped = df.groupby('key')

print('Multiprocessing Pool to calc max per day.')
test = applyParallel(grouped)

print('Saving output to file.')
test.to_hdf('../datasets/mta_days.hdf','mta')

print(test.head())

print('Done.')
