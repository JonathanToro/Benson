import pandas as pd
from pandas.io.pytables import HDFStore

# Still learning about HDF, may have 
# been more efficient to go straight
# to pickle.


INPUT = '../datasets/mta.h5'
OUTPUT = '../datasets/mta.pk'

with HDFStore(INPUT) as store:
    data = pd.Panel.from_dict({key.strip('/'):store[key] for key in store.keys()})

print(data.shape)

data.to_pickle(OUTPUT)
