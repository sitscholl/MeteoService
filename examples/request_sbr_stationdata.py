from webhandler.SBR_requests import SBR
import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('username', help = 'username')
parser.add_argument('password', help = 'password')
args = parser.parse_args()

with SBR(args.username, args.password) as client:
    data = client.get_stationdata(station_id = '103', start=datetime.datetime(2025, 3, 31, 0, 0), end=datetime.datetime(2025, 4, 2, 14, 0))
print(data.request.url)
with open('fetch_response.html', "w", encoding = 'utf-8') as file: 
    file.write(data.text)
