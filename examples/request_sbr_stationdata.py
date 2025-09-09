"""
This script shows how to retrieve meteorological data from a specific station using the SBR API.
"""
from webhandler.meteo.SBR import SBR
import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('username', help = 'username')
parser.add_argument('password', help = 'password')
args = parser.parse_args()

with SBR(args.username, args.password) as client:
    data = client.get_stationdata(
        station_id="103",
        start=datetime.datetime(2025, 3, 20, 0, 0),
        end=datetime.datetime(2025, 4, 2, 14, 0),
        type = 'meteo'
    )
    
print(data)
print(data.shape)
print(data.dtypes)