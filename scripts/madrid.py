#!/usr/bin/env python3
import json
import urllib.request
from urllib.error import HTTPError
from pathlib import Path

# === Config ===
CWD = Path.cwd()

version = input("Enter version number (e.g. 1 or 2): ").strip()
if not version:
    version = "1"

JSON_LOCATION = CWD / "response.json"

BUS_ROUTES_SQL = CWD / f"../public/full_db/md_bus_routes_v{version}.sql"
BUS_STOPS_SQL = CWD / f"../public/full_db/md_bus_stop_details_v{version}.sql"

CLIENT_ID = "efe93f83-6f7a-4767-b93d-b6071bea2cb5"
PASS_KEY = "C0D1043F83F93354BA165C8FEFB9240692C5EB5E61148CE18074015E82212D5AE9D7B846391180879657CE7C9D1FA7F22CD34BDB3E558BDF1BF5EC484ED088BC"

# === Utils ===
def reset_file(path: Path):
    path.write_text("")

def append_text(path: Path, text: str):
    with open(path, "a", encoding="utf-8") as f:
        f.write(text)

def fetch_json(url: str, headers=None, method="GET"):
    req = urllib.request.Request(url, method=method)
    req.add_header("Accept-Charset", "UTF-8")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req) as response:
            data = response.read()
            return json.loads(data.decode("utf-8"))
    except HTTPError as e:
        print(f"HTTPError fetching {url}: {e.code} {e.reason}")
        return {}

# === EMT API ===
def get_access_token():
    url = "https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/"
    headers = {"X-ClientId": CLIENT_ID, "passKey": PASS_KEY}
    root = fetch_json(url, headers=headers)
    if not root or "data" not in root:
        raise RuntimeError("Failed to obtain access token")
    token = root["data"][0]["accessToken"]
    print(f"Access token: {token}")
    return token

def fetch_all_stops(access_token: str):
    url = "https://openapi.emtmadrid.es/v1/transport/busemtmad/stops/list/"
    headers = {"accessToken": access_token}
    root = fetch_json(url, headers=headers, method="POST")
    return root.get("data", [])

def fetch_bus_route(access_token: str, bus_number: str, direction: int):
    url = f"https://openapi.emtmadrid.es/v1/transport/busemtmad/lines/{bus_number}/stops/{direction}/"
    headers = {"accessToken": access_token}
    return fetch_json(url, headers=headers)

# === SQL Initialization ===
def init_sql_files():
    reset_file(BUS_ROUTES_SQL)
    reset_file(BUS_STOPS_SQL)

# === Helpers ===
def add_space_before_capitals(s: str) -> str:
    result = ""
    for i, ch in enumerate(s):
        result += ch
        if i + 1 < len(s) and s[i + 1].isupper() and not s[i].isspace() and not s[i].isupper():
            result += " "
    return result

# === Core ===
def process_routes_and_stops():
    access_token = get_access_token()
    bus_stop_code_to_road = {}
    all_bus_numbers = get_all_bus_numbers(access_token)
    print(f"Found {len(all_bus_numbers)} bus numbers.")

    for idx, bus_number in enumerate(sorted(all_bus_numbers), 1):
        for direction in [1, 2]:
            root = fetch_bus_route(access_token, bus_number, direction)
            if not root or "data" not in root:
                continue

            data = root["data"][0]
            service_no = data["label"]
            stops = data["stops"]

            for seq, stop in enumerate(stops, 1):
                stop_code = stop["stop"]
                road = (stop.get("postalAddress") or "").replace("'", "''")
                if "Pº" in road:
                    road = road.split("Pº", 1)[-1].split(",")[0].strip()
                bus_stop_code_to_road[stop_code] = road

                append_text(BUS_ROUTES_SQL, f"'{service_no}',{direction},{seq},'{stop_code}',NULL\n")

        print(f"[{idx}/{len(all_bus_numbers)}] {bus_number} done")

    process_bus_stops(access_token, bus_stop_code_to_road)

def get_all_bus_numbers(access_token: str):
    all_stops = fetch_all_stops(access_token)
    bus_numbers = set()
    for item in all_stops:
        for line in item.get("lines", []):
            if "/" in line:
                trimmed = line.split("/", 1)[0]
                trimmed = trimmed.lstrip("0")
                bus_numbers.add(trimmed)
    return list(bus_numbers)

def process_bus_stops(access_token: str, bus_stop_code_to_road: dict):
    all_stops = fetch_all_stops(access_token)
    for i, item in enumerate(all_stops, 1):
        code = item["node"]
        name = add_space_before_capitals(item["name"].replace("'", "''"))
        road = bus_stop_code_to_road.get(code, "")
        geometry = item.get("geometry", {}).get("coordinates", [0.0, 0.0])
        lon, lat = geometry[0], geometry[1]

        append_text(BUS_STOPS_SQL,f"'{code}','{name}','{road}','','',{lat},{lon}\n")

        if i % 100 == 0 or i == len(all_stops):
            print(f"[{i}/{len(all_stops)}] Stops processed")

# === Main ===
def main():
    init_sql_files()
    process_routes_and_stops()
    print("✅ Madrid bus data successfully written to SQL files.")

if __name__ == "__main__":
    main()
