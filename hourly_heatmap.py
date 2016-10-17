import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# calculate hourly traffic on target stations using data from
# Mar - May 2015 and 2016

all_data = pd.concat([data15, data16], ignore_index=True)

fourhour_data = all_data.sort_values(by=['key', 'date']).reset_index()
fourhour_data.drop('index', axis=1, inplace=True)

fourhour_data['month'] = fourhour_data['date'].map(lambda x: x.month)
fourhour_data = fourhour_data[fourhour_data['month'].isin([3, 4, 5])]
fourhour_data['hour'] = fourhour_data['date'].map(lambda x: x.hour)
fourhour_data['weekday'] = fourhour_data['date'].map(lambda x: x.weekday())
fourhour_data = fourhour_data[fourhour_data['weekday'] < 5]

fourhour_data['CumCount'] = fourhour_data['entries'] + fourhour_data['exits']
fourhour_data['Count'] = fourhour_data['CumCount'] - fourhour_data['CumCount'].shift(1)

fourhour_data['key_1'] = fourhour_data['key'].shift(1)
key_col = fourhour_data.key.tolist()
key1_col = fourhour_data.key_1.tolist()
key_diff = []
for id, key in enumerate(key_col):
    if key == key1_col[id]:
        key_diff.append(False)
    else:
        key_diff.append(True)
key_df = pd.DataFrame({'keydiff': key_diff})
fourhour_data = pd.concat([fourhour_data, key_df], axis=1)

fourhour_data = fourhour_data[~fourhour_data['keydiff']]

fourhour_data['Count'] = fourhour_data['Count'].fillna(0)
fourhour_data = fourhour_data[fourhour_data['Count'] > 0]
fourhour_data = fourhour_data[fourhour_data['Count'] < 3e3]

fourhour_data = fourhour_data[['key', 'date', 'hour', 'Count']]
fourhour_data['Count'] /= 4

hourly_arr = [fourhour_data.copy()]
for i in range(3):
    hourly = fourhour_data.copy()
    hourly['date'] -= datetime.timedelta(hours=i+1)
    hourly['hour'] -= i + 1
    hourly['hour'] %= 24
    hourly_arr.append(hourly)


hourly_data = pd.concat(hourly_arr, ignore_index=True).reset_index()
hourly_count = hourly_data[['key', 'hour', 'Count']].groupby(['key', 'hour']).sum().reset_index()

station_hourly_data = pd.merge(hourly_count, map_df, on='key')
station_hourly = station_hourly_data[['Station', 'hour', 'Count']].groupby(['Station', 'hour']).mean().reset_index()

targets = ['14 ST-UNION SQ LNQR456', '59 ST-COLUMBUS ABCD1', '72 ST 123',
           '66 ST-LINCOLN 1', '49 ST-7 AVE NQR']
by_hour = station_hourly[station_hourly['Station'].isin(targets)]
by_hour.to_csv('byhour.csv', index=False)

# seaborn heatmap
by_hour['hour'] = by_hour['hour'].map(int)
my_hour = by_hour.pivot('Station', 'hour', 'Count')

sns.palplot(sns.color_palette('Blues'))
ax = plt.axes()
sns.heatmap(my_hour, ax=ax, vmin=5000, cmap='YlGnBu', xticklabels=2, linewidths=.3)
ax.set_title('Target Stations Hourly Traffic')


