#!/usr/bin/env python
# coding: utf-8


import urllib.request
import json
import csv

filepath = 'https://api-core.bixi.com/gbfs/en/station_status.json'

request = urllib.request.Request(filepath)
response = urllib.request.urlopen(request)
elevations = response.read()
data = json.loads(elevations)

records = [['station_id', 'num_bikes_available', 'num_ebikes_available', 'num_bikes_disabled', 'num_docks_available', 'num_docks_disabled', 'is_installed', 'is_renting', 'is_returning', 'last_reported', 'eightd_has_available_keys']]

for station in data['data']['stations']:
    station_id = station['station_id']
    num_bikes_available = station['num_bikes_available']
    num_ebikes_available = station['num_ebikes_available']
    num_bikes_disabled = station['num_bikes_disabled']
    num_docks_available = station['num_docks_available']
    num_docks_disabled = station['num_docks_disabled']
    is_installed = station['is_installed']
    is_renting  = station['is_renting']
    is_returning = station['is_returning']
    last_reported = station['last_reported']
    eightd_has_available_keys = station['eightd_has_available_keys']

    record = [station_id, num_bikes_available, num_ebikes_available, num_bikes_disabled, num_docks_available, num_docks_disabled, is_installed, is_renting, is_returning, last_reported, eightd_has_available_keys]
    records.append(record)


with open("station_status.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerows(records)
