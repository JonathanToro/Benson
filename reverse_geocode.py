from requests import get
from json import loads
from time import sleep
import pandas as pd


# find mapping from stations to zip codes given the station coordinates
# google geocode API
URL = 'https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lng}&key={key}'
KEY = 'AIzaSyA_vA6CudRvb5zORFf_xbMhtk_dGHNxzaA'

def get_zip_from_lat_lng(lat,
						 lng,
						 delay = 1,
						 url = URL,
						 key = KEY,):
	# don't overwhelm server
	sleep(delay)
    resp = get(url.format(lat=lat,lng=lng,key=key))
    assert resp.ok
	# gets JSON object
    data = loads(resp.text)
	# pulls zipcode from first match
    return [d['long_name'] for d in data['results'][0]['address_components'] if 'postal_code' in d['types']][0]

# example
# get_zip_from_lat_lng(40.77503600,-73.9120340)

subways= pd.read_csv('mta_subway1station.csv')

cols = ['Station_Name', 'Station_Longitude', 'Station_Latitude']
cols = ['id','Station_Name']
stations = subways[cols]

route_cols = []
[route_cols.append(''.join('Route'+str(x))) for x in range(1,12)]
stations = subways[cols+route_cols]

unique = stations.drop_duplicates()
unique.drop(route_cols, inplace=True, axis = 1)
unique.to_csv('station.csv', index=False)
# unique['id'] = unique.index

station_map = {}
for _, station in unique.iterrows():
    station_map[station.Station_Name] = get_zip_from_lat_lng(station.Station_Latitude, station.Station_Longitude)


station_df = pd.DataFrame({ 'Station_Name':list(station_map.keys()), 'Zip':list(station_map.values())})
station_df.to_csv('station_zip.csv', index=False)
