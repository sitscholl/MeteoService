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
    data = client.run(
        station_id="103",
        start=datetime.datetime(2025, 9, 1, 0, 0),
        end=datetime.datetime(2025, 9, 9, 14, 0),
        type = 'meteo'
    )
    
print(data)
print(data.shape)
print(data.dtypes)