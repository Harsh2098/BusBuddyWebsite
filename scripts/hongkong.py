#!/usr/bin/env python3
import json
import urllib.request
from pathlib import Path

# === Config ===
CWD = Path.cwd()

version = input("Enter version number (e.g. 1 or 2): ").strip()
if not version:
    version = "1"

JSON_LOCATION = CWD / "response.json"

HK_BUS_ROUTES_SQL = CWD / f"../public/full_db/hk_bus_routes_v{version}.sql"
HK_BUS_STOP_DETAILS_SQL = CWD / f"../public/full_db/hk_bus_stop_details_v{version}.sql"
HK_BUS_STOP_NUMBERS_SQL = CWD / f"../public/full_db/hk_bus_stop_bus_numbers_details_v{version}.sql"

# === Utils ===
def reset_file(path: Path):
    path.write_text("")

def append_text(path: Path, text: str):
    with open(path, "a", encoding="utf-8") as f:
        f.write(text)

def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url)
    req.add_header("Accept-Charset", "UTF-8")
    with urllib.request.urlopen(req) as response:
        data = response.read()
        return json.loads(data.decode("utf-8"))

# === Core Fetchers ===
def get_all_bus_numbers():
    companies = ["CTB", "NWFB"]
    bus_numbers = []
    for company in companies:
        url = f"https://rt.data.gov.hk/v1/transport/citybus-nwfb/route/{company}"
        root = fetch_json(url)
        for item in root.get("data", []):
            route = item.get("route")
            if route:
                bus_numbers.append((route, company))
    return bus_numbers

def get_bus_route_stops(bus_number: str, company: str, direction: str):
    url = f"https://rt.data.gov.hk/v1/transport/citybus-nwfb/route-stop/{company}/{bus_number}/{direction}"
    root = fetch_json(url)
    return root.get("data", [])

def get_bus_stop_details(stop_code: str):
    url = f"https://rt.data.gov.hk/v1/transport/citybus-nwfb/stop/{stop_code}"
    root = fetch_json(url)
    return root.get("data", {})

# === SQL Writers ===
def init_sql_files():
    reset_file(HK_BUS_ROUTES_SQL)
    reset_file(HK_BUS_STOP_DETAILS_SQL)
    reset_file(HK_BUS_STOP_NUMBERS_SQL)

def process_routes_and_stops():
    all_bus_numbers = get_all_bus_numbers()
    bus_stop_codes = set()

    print(f"Total bus numbers found: {len(all_bus_numbers)}")

    for i, (bus_number, company) in enumerate(all_bus_numbers, 1):
        for direction in ["inbound", "outbound"]:
            items = get_bus_route_stops(bus_number, company, direction)
            if not items:
                continue

            for item in items:
                service_no = item.get("route")
                dir_int = 1 if item.get("dir") == "I" else 2
                stop_seq = item.get("seq")
                stop_code = item.get("stop")
                operator = item.get("co")
                distance = 0

                bus_stop_codes.add(stop_code)

                append_text(HK_BUS_ROUTES_SQL, f"'{service_no}',{dir_int},{stop_seq},'{stop_code}',{distance}\n")

                append_text(HK_BUS_STOP_NUMBERS_SQL, f"'{stop_code}','{service_no}',{dir_int},'{operator}'\n")

        print(f"[{i}/{len(all_bus_numbers)}] Processed {bus_number} ({company})")

    return bus_stop_codes

def process_bus_stop_details(bus_stop_codes):
    print(f"Fetching details for {len(bus_stop_codes)} unique stops...")

    for i, code in enumerate(sorted(bus_stop_codes), 1):
        item = get_bus_stop_details(code)
        if not item:
            continue

        name_en = item.get("name_en", "")
        name_sc = item.get("name_sc", "")

        name_en_parts = [p.strip().replace("'", "''") for p in name_en.split(",")]
        name_sc_parts = [p.strip().replace("'", "''") for p in name_sc.split(",")]

        name = name_en_parts[0] if len(name_en_parts) > 0 else ""
        road = name_en_parts[1] if len(name_en_parts) > 1 else ""
        cname = name_sc_parts[0] if len(name_sc_parts) > 0 else ""
        croad = name_sc_parts[1] if len(name_sc_parts) > 1 else ""

        lat = item.get("lat")
        lon = item.get("long")

        append_text(HK_BUS_STOP_DETAILS_SQL,
            f"'{code}','{name}','{road}','{cname}','{croad}',{lat},{lon}\n"
        )

        if i % 50 == 0 or i == len(bus_stop_codes):
            print(f"[{i}/{len(bus_stop_codes)}] Stops processed")

# === Main ===
def main():
    init_sql_files()
    bus_stop_codes = process_routes_and_stops()
    process_bus_stop_details(bus_stop_codes)
    print("✅ Hong Kong bus data successfully written to SQL files.")

if __name__ == "__main__":
    main()
