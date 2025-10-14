#!/usr/bin/env python3
import os
import json
import urllib.request
import urllib.parse
import subprocess
from pathlib import Path

# === Config ===
CWD = Path.cwd()

version = input("Enter version number: ").strip()
if not version:
    version = "1"

JSON_LOCATION = CWD / "response.json"
BUS_ROUTES_SQL = CWD / f"../public/full_db/sg_bus_routes_v{version}.sql"
BUS_STOPS_SQL = CWD / f"../public/full_db/sg_bus_stop_details_v{version}.sql"

ACCOUNT_KEY = "EPEcmrGzRWeN4824xfuvoQ=="

# === Utils ===
def reset_file(path: Path):
    path.write_text("")

def write_bytes(path: Path, data: bytes, mode="wb"):
    with open(path, mode) as f:
        f.write(data)


def append_text(path: Path, text: str):
    with open(path, "a", encoding="utf-8") as f:
        f.write(text)

# === Network ===
def fetch_to_file(url: str, skip: int, outfile: Path):
    params = {"$skip": str(skip)}
    query = urllib.parse.urlencode(params)
    full_url = f"{url}?{query}"

    req = urllib.request.Request(full_url)
    req.add_header("Accept-Charset", "UTF-8")
    req.add_header("AccountKey", ACCOUNT_KEY)

    try:
        with urllib.request.urlopen(req) as response:
            data = response.read()
        write_bytes(outfile, data)
    except Exception as e:
        print(f"Error fetching {url}: {e}")

# === Parsing ===
def parse_bus_routes(json_file: Path, sql_file: Path) -> int:
    with open(json_file, "r", encoding="utf-8") as f:
        root = json.load(f)

    items = root.get("value", [])
    for item in items:
        service_no = item.get("ServiceNo")
        direction = item.get("Direction")
        stop_sequence = item.get("StopSequence")
        bus_stop_code = item.get("BusStopCode")
        distance = item.get("Distance", 0)

        sql_line = f"'{service_no}',{direction},{stop_sequence},'{bus_stop_code}',{distance}\n"
        append_text(sql_file, sql_line)

    return len(items)

def parse_bus_stops(json_file: Path, sql_file: Path) -> int:
    with open(json_file, "r", encoding="utf-8") as f:
        root = json.load(f)

    items = root.get("value", [])
    for item in items:
        code = item.get("BusStopCode")
        name = item.get("Description", "").replace("'", "''")
        road = item.get("RoadName", "").replace("'", "''")
        lat = item.get("Latitude")
        lon = item.get("Longitude")

        sql_line = f"'{code}','{name}','{road}','','',{lat},{lon}\n"
        append_text(sql_file, sql_line)

    return len(items)

# === Core logic ===
def fetch_bus_routes():
    for f in [BUS_ROUTES_SQL, JSON_LOCATION]:
        reset_file(f)

    total = 0
    while True:
        fetch_to_file("https://datamall2.mytransport.sg/ltaodataservice/BusRoutes", total, JSON_LOCATION)
        count = parse_bus_routes(JSON_LOCATION, BUS_ROUTES_SQL)
        total += count
        print(f"Total bus route records fetched: {total}")
        if count == 0:
            break


def fetch_bus_stops():
    for f in [BUS_STOPS_SQL, JSON_LOCATION]:
        reset_file(f)

    total = 0
    while True:
        fetch_to_file("https://datamall2.mytransport.sg/ltaodataservice/BusStops", total, JSON_LOCATION)
        count = parse_bus_stops(JSON_LOCATION, BUS_STOPS_SQL)
        total += count
        print(f"Total bus stop details fetched: {total}")
        if count < 500:
            break


def main():
    fetch_bus_routes()
    fetch_bus_stops()

if __name__ == "__main__":
    main()
