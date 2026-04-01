#!/usr/bin/env python3
"""
Forbes Australia — Fuel Price Tracker
Fetches live station-level data from FuelCheck NSW API every 6 hours.
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

EXCISE_BEFORE_CPL = 52.6
EXCISE_AFTER_CPL  = 26.3


# ─── AUTH ─────────────────────────────────────────────────────────────────────

def get_access_token():
    """
    API.NSW uses OAuth2 client credentials.
    Step 1: exchange API key + secret for a Bearer access token.
    """
    import base64
    basic = base64.b64encode(f"{API_KEY}:{API_SECRET}".encode()).decode()
    token_url = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
    resp = requests.post(
        token_url,
        params={"grant_type": "client_credentials"},
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type":  "application/x-www-form-urlencoded",
        },
        timeout=15,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise ValueError(f"No access_token in response: {resp.text[:200]}")
    print(f"  ✓ Access token obtained")
    return token

def auth_headers(access_token=None):
    headers = {
        "apikey":        API_KEY,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    return headers


# ─── API DISCOVERY ────────────────────────────────────────────────────────────

# Try multiple possible base URL patterns used by API.NSW
CANDIDATE_BASES = [
    "https://api.onegov.nsw.gov.au/FuelCheckApp/v1/fuel",
    "https://api.onegov.nsw.gov.au/FuelCheckApp/v2/fuel",
    "https://api.onegov.nsw.gov.au/FuelCheck/v1/fuel",
    "https://api.onegov.nsw.gov.au/FuelPrices/v1/fuel",
    "https://api.onegov.nsw.gov.au/fuelcheck/v1/fuel",
]

PRICE_ENDPOINTS    = ["prices", "GetAllPrices", "getAllPrices", "getallprices"]
REFERENCE_ENDPOINTS = ["GetReferenceData", "getReferenceData", "getreferencedata",
                        "reference", "stations", "Stations"]


def try_get(url, token):
    """Attempt a GET request. Returns (response, error_string)."""
    try:
        r = requests.get(url, headers=auth_headers(token), timeout=20)
        print(f"    {r.status_code} → {url}")
        if r.status_code in (404, 401, 403):
            return None, f"{r.status_code} at {url}"
        r.raise_for_status()
        return r, None
    except requests.HTTPError as e:
        return None, str(e)
    except Exception as e:
        return None, str(e)


def discover_and_fetch(token):
    """
    Probe candidate URL combinations to find the working prices endpoint.
    Returns (working_url, parsed_json) or raises SystemExit.
    """
    print("  Probing API endpoints...")
    for base in CANDIDATE_BASES:
        for ep in PRICE_ENDPOINTS:
            url = f"{base}/{ep}"
            resp, err = try_get(url, token)
            if resp is not None:
                try:
                    data = resp.json()
                    if isinstance(data, dict) and ("prices" in data or "stations" in data):
                        print(f"  ✓ Found working endpoint: {url}")
                        return url, data
                except Exception:
                    pass

    print("\nERROR: Could not find a working API endpoint.", file=sys.stderr)
    sys.exit(1)


def fetch_reference_data(base_url, token):
    """Try to fetch station reference data (lat/lng). Returns station index dict."""
    base = base_url.rsplit("/", 1)[0]
    for ep in REFERENCE_ENDPOINTS:
        url = f"{base}/{ep}"
        resp, _ = try_get(url, token)
        if resp is None:
            continue
        try:
            data = resp.json()
            stations = (data.get("stations") or data.get("Stations") or
                        data.get("stationlist") or [])
            if stations:
                print(f"  ✓ Reference data from: {url} ({len(stations)} stations)")
                return build_station_index(stations)
        except Exception:
            pass
    print("  ⚠ No reference data endpoint found — will use data embedded in prices")
    return {}


def build_station_index(stations):
    idx = {}
    for s in stations:
        code = str(s.get("stationid") or s.get("code") or s.get("id") or "")
        if not code:
            continue
        loc = s.get("location") or {}
        lat = (s.get("latitude") or s.get("lat") or
               loc.get("latitude") or loc.get("lat"))
        lng = (s.get("longitude") or s.get("lng") or
               loc.get("longitude") or loc.get("lng"))
        idx[code] = {
            "name":     (s.get("stationname") or s.get("name") or "").strip(),
            "brand":    (s.get("brand") or "").strip(),
            "address":  (s.get("address") or "").strip(),
            "suburb":   (s.get("suburb") or "").strip(),
            "postcode": str(s.get("postcode") or "").strip(),
            "state":    (s.get("state") or "NSW").strip(),
            "lat":      float(lat) if lat else None,
            "lng":      float(lng) if lng else None,
        }
    return idx


# ─── MERGE ────────────────────────────────────────────────────────────────────

def merge(prices_data, station_index):
    """
    Build station list with prices.
    Prices are stored in cents/litre by the API — we convert to dollars.
    """
    prices = prices_data.get("prices") or prices_data.get("Prices") or []

    # Group prices by station, also extract any embedded station data
    by_station = defaultdict(dict)
    embedded   = {}

    for p in prices:
        code  = str(p.get("stationcode") or p.get("stationid") or p.get("code") or "")
        ftype = (p.get("fueltype") or p.get("FuelType") or "").upper()
        raw   = p.get("price") or p.get("Price") or 0

        if not code or not ftype:
            continue

        # Convert cents → dollars (handles both 244.0 and 2440 formats)
        price_dollars = float(raw)
        if price_dollars > 10:          # it's in cents (e.g. 244.0 or 2440)
            price_dollars = price_dollars / (10 if price_dollars < 1000 else 100)
        by_station[code][ftype] = round(price_dollars, 3)

        # Capture any location data embedded in the price record
        if code not in embedded:
            loc = p.get("location") or {}
            lat = (p.get("latitude") or p.get("lat") or
                   loc.get("latitude") or loc.get("lat"))
            lng = (p.get("longitude") or p.get("lng") or
                   loc.get("longitude") or loc.get("lng"))
            if lat and lng:
                embedded[code] = {
                    "name":     (p.get("stationname") or p.get("name") or "").strip(),
                    "brand":    (p.get("brand") or "").strip(),
                    "address":  (p.get("address") or "").strip(),
                    "suburb":   (p.get("suburb") or "").strip(),
                    "postcode": str(p.get("postcode") or "").strip(),
                    "state":    (p.get("state") or "NSW"),
                    "lat":      float(lat),
                    "lng":      float(lng),
                }

    # Merge: prefer station_index (reference data), fall back to embedded
    result = []
    for code, station_prices in by_station.items():
        info = station_index.get(code) or embedded.get(code)
        if not info:
            continue
        lat, lng = info.get("lat"), info.get("lng")
        if not lat or not lng:
            continue
        result.append({
            "code":     code,
            "name":     info["name"],
            "brand":    info["brand"],
            "address":  info["address"],
            "suburb":   info["suburb"],
            "postcode": info["postcode"],
            "state":    info["state"],
            "lat":      round(lat, 6),
            "lng":      round(lng, 6),
            "prices":   station_prices,
        })

    return result


# ─── CITY AVERAGES ────────────────────────────────────────────────────────────

CITY_KEYWORDS = {
    "Sydney":       ["sydney","parramatta","bondi","chatswood","penrith","liverpool",
                     "blacktown","hurstville","hornsby","campbelltown","manly","randwick"],
    "Newcastle":    ["newcastle","maitland","cessnock","charlestown"],
    "Wollongong":   ["wollongong","shellharbour","kiama"],
    "Central Coast":["gosford","wyong","tuggerah"],
}

def city_averages(stations):
    cities = {}
    for city, keywords in CITY_KEYWORDS.items():
        subset = [s for s in stations
                  if any(k in s["suburb"].lower() for k in keywords)]
        if not subset:
            continue
        avgs = {}
        for ft in ["U91", "E10", "DL", "U95", "U98"]:
            vals = [s["prices"][ft] for s in subset if ft in s["prices"]]
            if vals:
                avgs[ft] = round(sum(vals) / len(vals), 3)
        cities[city] = avgs
    return cities


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    if not API_KEY or not API_SECRET:
        print("ERROR: NSW_API_KEY and NSW_API_SECRET required.", file=sys.stderr)
        sys.exit(1)

    now = datetime.now(timezone(timedelta(hours=10)))
    print(f"[{now.strftime('%Y-%m-%d %H:%M AEST')}] Fetching FuelCheck NSW...\n")

    os.makedirs(os.path.dirname(STATIONS_FILE), exist_ok=True)

    # Step 1: get OAuth2 Bearer token
    print("Getting access token...")
    try:
        token = get_access_token()
    except Exception as e:
        print(f"ERROR getting access token: {e}", file=sys.stderr)
        sys.exit(1)
    print()

    # Step 2: discover the working prices endpoint
    working_url, prices_data = discover_and_fetch(token)
    print()

    # Step 3: try to get reference data (station lat/lng)
    station_index = fetch_reference_data(working_url, token)
    print()

    # Merge into stations list
    stations = merge(prices_data, station_index)
    print(f"Merged: {len(stations)} stations with location + prices\n")

    if not stations:
        print("ERROR: No stations with location data. Cannot build map.", file=sys.stderr)
        print("Prices data keys:", list(prices_data.keys()), file=sys.stderr)
        sys.exit(1)

    # stations-nsw.json
    stations_out = {
        "updated_at":    now.isoformat(),
        "station_count": len(stations),
        "fuel_types":    list(FUEL_LABELS.keys()),
        "fuel_labels":   FUEL_LABELS,
        "stations":      stations,
    }
    with open(STATIONS_FILE, "w") as f:
        json.dump(stations_out, f, separators=(",", ":"))
    print(f"Saved {STATIONS_FILE} ({os.path.getsize(STATIONS_FILE)//1024} KB)")

    # prices.json
    nat = {}
    for ft in ["U91", "E10", "DL", "U95", "U98"]:
        vals = [s["prices"][ft] for s in stations if ft in s["prices"]]
        if vals:
            nat[ft] = round(sum(vals) / len(vals), 3)

    prices_out = {
        "updated_at": now.isoformat(),
        "excise": {
            "before_cpl": EXCISE_BEFORE_CPL,
            "after_cpl":  EXCISE_AFTER_CPL,
            "cut_cpl":    round(EXCISE_BEFORE_CPL - EXCISE_AFTER_CPL, 1),
        },
        "reserves": {
            "petrol_days": 37, "diesel_days": 30, "jet_fuel_days": 29,
            "as_of": "2026-03-28",
        },
        "national": nat,
        "cities":   city_averages(stations),
    }
    with open(PRICES_FILE, "w") as f:
        json.dump(prices_out, f, indent=2)
    print(f"Saved {PRICES_FILE}")

    if "U91" in nat:
        print(f"\nNSW avg ULP 91: ${nat['U91']:.3f}/L")
    if "DL" in nat:
        print(f"NSW avg Diesel: ${nat['DL']:.3f}/L")
    print("\nDone.")


if __name__ == "__main__":
    main()
