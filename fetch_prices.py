#!/usr/bin/env python3
"""
Forbes Australia — Fuel Price Tracker
Fetches live station-level data from FuelCheck NSW API every 6 hours.
Outputs:
  data/stations-nsw.json  — all stations with prices (for the map)
  data/prices.json        — city averages (for the overview block)
"""

import requests
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ─── CONFIG ───────────────────────────────────────────────────────────────────

API_KEY    = os.environ.get("NSW_API_KEY", "")
API_SECRET = os.environ.get("NSW_API_SECRET", "")
BASE_URL   = "https://api.onegov.nsw.gov.au/FuelCheckApp/v1/fuel"

STATIONS_FILE = os.path.join(os.path.dirname(__file__), "data", "stations-nsw.json")
PRICES_FILE   = os.path.join(os.path.dirname(__file__), "data", "prices.json")

FUEL_LABELS = {
    "U91": "Unleaded 91",
    "E10": "E10 Unleaded",
    "U95": "Premium 95",
    "U98": "Premium 98",
    "DL":  "Diesel",
    "PDL": "Premium Diesel",
    "E85": "Ethanol E85",
    "LPG": "LPG",
}

# City-to-suburb mapping for city average calculations
CITY_SUBURBS = {
    "Sydney":    ["Sydney", "Parramatta", "Bondi", "Chatswood", "Penrith", "Liverpool",
                  "Blacktown", "Hurstville", "Hornsby", "Campbelltown", "Manly"],
    "Newcastle": ["Newcastle", "Maitland", "Cessnock", "Lake Macquarie", "Charlestown"],
    "Wollongong":["Wollongong", "Shellharbour", "Kiama", "Nowra"],
    "Central Coast":["Gosford", "Wyong", "Tuggerah", "The Entrance"],
}

EXCISE_BEFORE_CPL = 52.6
EXCISE_AFTER_CPL  = 26.3


# ─── AUTH ─────────────────────────────────────────────────────────────────────

def auth_headers():
    import base64
    token = base64.b64encode(f"{API_KEY}:{API_SECRET}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "apikey":        API_KEY,
        "Content-Type":  "application/json",
    }


# ─── API CALLS ────────────────────────────────────────────────────────────────

def fetch_stations():
    print("  Fetching station reference data...")
    resp = requests.get(f"{BASE_URL}/GetReferenceData", headers=auth_headers(), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # Reference data contains stations, fueltypes, brands
    stations = data.get("stations", [])
    print(f"  → {len(stations)} stations")
    return stations

def fetch_prices():
    print("  Fetching live prices...")
    resp = requests.get(f"{BASE_URL}/prices", headers=auth_headers(), timeout=30)
    resp.raise_for_status()
    prices = resp.json().get("prices", [])
    print(f"  → {len(prices)} price entries")
    return prices


# ─── DATA PROCESSING ──────────────────────────────────────────────────────────

def merge(stations, prices):
    """
    Merge station details with their current prices.
    API prices are in cents/litre (e.g. 244.0 = $2.44/L).
    We store as dollars for clean display.
    """
    # Index prices by station code
    price_index = defaultdict(dict)
    for p in prices:
        code  = str(p.get("stationcode") or p.get("stationid") or p.get("code") or "")
        ftype = p.get("fueltype", "").upper()
        raw   = p.get("price", 0)
        if code and ftype and raw:
            price_index[code][ftype] = round(float(raw) / 100, 3)

    result = []
    for s in stations:
        code = str(s.get("stationid") or s.get("code") or s.get("id") or "")
        loc  = s.get("location", {})
        lat  = (s.get("latitude") or s.get("lat")
                or loc.get("latitude") or loc.get("lat"))
        lng  = (s.get("longitude") or s.get("lng")
                or loc.get("longitude") or loc.get("lng"))

        if not lat or not lng:
            continue

        station_prices = price_index.get(code, {})
        if not station_prices:
            continue  # skip stations with no prices

        result.append({
            "code":     code,
            "name":     s.get("name", "").strip(),
            "brand":    s.get("brand", "").strip(),
            "address":  s.get("address", "").strip(),
            "suburb":   s.get("suburb", "").strip(),
            "postcode": str(s.get("postcode", "")).strip(),
            "state":    s.get("state", "NSW"),
            "lat":      round(float(lat), 6),
            "lng":      round(float(lng), 6),
            "prices":   station_prices,
        })

    return result

def city_averages(stations):
    """Calculate average prices by major city for the overview block."""
    cities = {}
    for city, keywords in CITY_SUBURBS.items():
        city_stations = [
            s for s in stations
            if any(k.lower() in s["suburb"].lower() for k in keywords)
        ]
        if not city_stations:
            continue
        avgs = {}
        for ftype in ["U91", "E10", "DL", "U95", "U98"]:
            vals = [s["prices"][ftype] for s in city_stations if ftype in s["prices"]]
            if vals:
                avgs[ftype] = round(sum(vals) / len(vals), 3)
        cities[city] = avgs
    return cities


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    if not API_KEY or not API_SECRET:
        print("ERROR: NSW_API_KEY and NSW_API_SECRET environment variables required.", file=sys.stderr)
        sys.exit(1)

    now = datetime.now(timezone(timedelta(hours=10)))  # AEST
    print(f"[{now.strftime('%Y-%m-%d %H:%M AEST')}] Fetching FuelCheck NSW...")

    os.makedirs(os.path.dirname(STATIONS_FILE), exist_ok=True)

    try:
        raw_stations = fetch_stations()
        raw_prices   = fetch_prices()
    except requests.HTTPError as e:
        print(f"API error: {e}", file=sys.stderr)
        sys.exit(1)

    stations = merge(raw_stations, raw_prices)
    print(f"  → {len(stations)} stations with prices after merge")

    # ── stations-nsw.json (map data) ──────────────────────────────────────────
    stations_out = {
        "updated_at":     now.isoformat(),
        "station_count":  len(stations),
        "fuel_types":     list(FUEL_LABELS.keys()),
        "fuel_labels":    FUEL_LABELS,
        "stations":       stations,
    }
    with open(STATIONS_FILE, "w") as f:
        json.dump(stations_out, f, separators=(",", ":"))  # compact for size
    size_kb = os.path.getsize(STATIONS_FILE) / 1024
    print(f"  Saved {STATIONS_FILE} ({size_kb:.0f} KB)")

    # ── prices.json (overview block) ──────────────────────────────────────────
    avgs = city_averages(stations)

    # National averages (all NSW stations)
    nat = {}
    for ftype in ["U91", "E10", "DL", "U95", "U98"]:
        vals = [s["prices"][ftype] for s in stations if ftype in s["prices"]]
        if vals:
            nat[ftype] = round(sum(vals) / len(vals), 3)

    prices_out = {
        "updated_at": now.isoformat(),
        "excise": {
            "before_cpl": EXCISE_BEFORE_CPL,
            "after_cpl":  EXCISE_AFTER_CPL,
            "cut_cpl":    round(EXCISE_BEFORE_CPL - EXCISE_AFTER_CPL, 1),
            "effective_date": "2026-04-01",
        },
        "reserves": {
            "petrol_days":   37,
            "diesel_days":   30,
            "jet_fuel_days": 29,
            "as_of":         "2026-03-28",
            "source":        "DISR / ACCC",
        },
        "national": nat,
        "cities":   avgs,
    }
    with open(PRICES_FILE, "w") as f:
        json.dump(prices_out, f, indent=2)
    print(f"  Saved {PRICES_FILE}")

    if "U91" in nat:
        print(f"\n  NSW avg ULP 91: ${nat['U91']:.3f}/L")
    if "DL" in nat:
        print(f"  NSW avg Diesel: ${nat['DL']:.3f}/L")
    print("Done.")


if __name__ == "__main__":
    main()
