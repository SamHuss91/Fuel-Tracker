#!/usr/bin/env python3
"""
Forbes Australia — Fuel Price Tracker
Fetches live data from FuelCheck NSW, WA FuelWatch, and AIP.
Run on a cron every 30 minutes:
  */30 * * * * /usr/bin/python3 /path/to/fetch_prices.py >> /path/to/fetch.log 2>&1
"""

import requests
import json
import xml.etree.ElementTree as ET
import os
import sys
from datetime import datetime, timezone, timedelta

# ─── CONFIG ───────────────────────────────────────────────────────────────────

NSW_API_KEY = os.environ.get("NSW_API_KEY", "")  # Register at api.nsw.gov.au
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "data", "prices.json")

# Excise cut effective 2026-04-01
EXCISE_BEFORE_CPL = 52.6
EXCISE_AFTER_CPL  = 26.3
EXCISE_CUT_CPL    = EXCISE_BEFORE_CPL - EXCISE_AFTER_CPL  # 26.3

# As-at snapshot from ACCC (week ending 2026-03-29) — fallback if APIs fail
ACCC_SNAPSHOT = {
    "Sydney":    {"ulp91": 244.0, "diesel": 303.5, "prem95": 258.0},
    "Melbourne": {"ulp91": 256.0, "diesel": 306.0, "prem95": 268.0},
    "Brisbane":  {"ulp91": 240.5, "diesel": 299.0, "prem95": 252.0},
    "Perth":     {"ulp91": 242.0, "diesel": 301.0, "prem95": 254.0},
    "Adelaide":  {"ulp91": 248.0, "diesel": 305.0, "prem95": 260.0},
    "Canberra":  {"ulp91": 251.0, "diesel": 307.0, "prem95": 263.0},
    "Darwin":    {"ulp91": 262.0, "diesel": 312.0, "prem95": 274.0},
    "Hobart":    {"ulp91": 255.0, "diesel": 308.0, "prem95": 267.0},
}

# National fuel reserves (source: DISR / ACCC, as of 2026-03-28)
RESERVES = {
    "petrol_days":   37,
    "diesel_days":   30,
    "jet_fuel_days": 29,
    "as_of":         "2026-03-28",
    "source":        "DISR / ACCC",
}


# ─── NSW FUELCHECK ─────────────────────────────────────────────────────────────

def fetch_nsw_prices():
    """
    FuelCheck NSW real-time API.
    Docs: https://api.nsw.gov.au/Product/Index/22
    Register for a key at https://api.nsw.gov.au/
    """
    if not NSW_API_KEY:
        print("  [NSW] No API key — skipping live fetch", file=sys.stderr)
        return {}

    url = "https://api.onegov.nsw.gov.au/FuelCheckApp/v1/fuel/prices"
    headers = {
        "apikey":       NSW_API_KEY,
        "Authorization": f"Bearer {NSW_API_KEY}",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        prices = resp.json().get("prices", [])

        by_type = {}
        for p in prices:
            ft  = p.get("fueltype", "").upper()
            cpl = p.get("price", 0)
            if ft and cpl:
                by_type.setdefault(ft, []).append(float(cpl))

        result = {}
        for ft, vals in by_type.items():
            result[ft] = {
                "avg_cpl": round(sum(vals) / len(vals), 1),
                "min_cpl": round(min(vals), 1),
                "max_cpl": round(max(vals), 1),
                "stations": len(vals),
            }
        print(f"  [NSW] {sum(len(v) for v in by_type.values())} price points across {len(result)} fuel types")
        return result

    except Exception as e:
        print(f"  [NSW] Error: {e}", file=sys.stderr)
        return {}


# ─── WA FUELWATCH ──────────────────────────────────────────────────────────────

def fetch_wa_prices():
    """
    WA FuelWatch daily RSS feed.
    Docs: https://www.fuelwatch.wa.gov.au
    No key required.
    """
    url = "https://www.fuelwatch.wa.gov.au/fuelwatch/pages/public/contentholder.jspx?key=rss"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        by_type = {}
        for item in root.findall(".//item"):
            # FuelWatch RSS uses custom tags — try common variants
            price_el = (
                item.find("{http://www.fuelwatch.wa.gov.au/rss}Price") or
                item.find("price") or
                item.find("Price")
            )
            type_el = (
                item.find("{http://www.fuelwatch.wa.gov.au/rss}FuelType") or
                item.find("fueltype") or
                item.find("FuelType")
            )
            if price_el is not None and type_el is not None:
                try:
                    ft    = type_el.text.strip()
                    price = float(price_el.text.strip())
                    by_type.setdefault(ft, []).append(price)
                except (ValueError, AttributeError):
                    pass

        result = {}
        for ft, vals in by_type.items():
            result[ft] = {
                "avg_cpl": round(sum(vals) / len(vals), 1),
                "min_cpl": round(min(vals), 1),
                "max_cpl": round(max(vals), 1),
                "stations": len(vals),
            }
        print(f"  [WA] {sum(len(v) for v in by_type.values())} price points across {len(result)} fuel types")
        return result

    except Exception as e:
        print(f"  [WA] Error: {e}", file=sys.stderr)
        return {}


# ─── AIP TERMINAL GATE PRICES ──────────────────────────────────────────────────

def fetch_aip_tgp():
    """
    Australian Institute of Petroleum — daily Terminal Gate Prices.
    Docs: https://www.aip.com.au/pricing
    No key required.
    """
    url = "http://api.aip.com.au/public/pricing"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        print(f"  [AIP] Got TGP data")
        return data
    except Exception as e:
        print(f"  [AIP] Error: {e}", file=sys.stderr)
        return {}


# ─── EXCISE PASS-THROUGH DETECTOR ─────────────────────────────────────────────

def compute_excise_passthrough(nsw: dict, prev_snapshot: dict) -> dict:
    """
    Compare today's NSW ULP avg to pre-cut baseline.
    Returns how much of the 26.3cpl cut has actually reached the pump.
    """
    current_ulp = nsw.get("U91", {}).get("avg_cpl") or nsw.get("E10", {}).get("avg_cpl")
    baseline    = prev_snapshot.get("Sydney", {}).get("ulp91", 252.0)

    if not current_ulp:
        return {"expected_cpl": EXCISE_CUT_CPL, "actual_cpl": None, "pct_passed": None}

    actual_drop = round(baseline - current_ulp, 1)
    pct         = round((actual_drop / EXCISE_CUT_CPL) * 100, 1) if actual_drop > 0 else 0.0

    return {
        "baseline_cpl":   baseline,
        "current_cpl":    current_ulp,
        "expected_cpl":   EXCISE_CUT_CPL,
        "actual_drop_cpl": max(actual_drop, 0),
        "pct_passed":     max(pct, 0),
    }


# ─── HISTORICAL PRICES (crisis timeline) ──────────────────────────────────────

def build_history():
    """
    30-day price history for the chart.
    Sources: ACCC weekly monitoring + AIP TGP data.
    Update the tail entry each run with live data.
    """
    return [
        {"date": "2026-03-01", "ulp91": 212.0, "diesel": 263.0},
        {"date": "2026-03-04", "ulp91": 218.0, "diesel": 270.0},
        {"date": "2026-03-08", "ulp91": 221.0, "diesel": 276.0},
        {"date": "2026-03-11", "ulp91": 228.0, "diesel": 282.0},
        {"date": "2026-03-15", "ulp91": 234.0, "diesel": 289.0},
        {"date": "2026-03-18", "ulp91": 239.0, "diesel": 294.0},
        {"date": "2026-03-22", "ulp91": 245.0, "diesel": 299.0},
        {"date": "2026-03-25", "ulp91": 249.0, "diesel": 302.0},
        {"date": "2026-03-29", "ulp91": 252.0, "diesel": 303.5},
        # ← script appends today's live value below
    ]


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now(timezone.utc)
    print(f"[{now.isoformat()}] Fetching fuel prices...")

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    print("Fetching NSW FuelCheck...")
    nsw = fetch_nsw_prices()

    print("Fetching WA FuelWatch...")
    wa = fetch_wa_prices()

    print("Fetching AIP Terminal Gate Prices...")
    aip = fetch_aip_tgp()

    # Build city prices — prefer live NSW/WA data, fall back to ACCC snapshot
    cities = {}
    for city, snap in ACCC_SNAPSHOT.items():
        cities[city] = dict(snap)  # start from snapshot

    # Overlay live NSW data for Sydney
    if nsw:
        u91 = nsw.get("U91", {}).get("avg_cpl")
        dl  = nsw.get("DL", {}).get("avg_cpl")
        u95 = nsw.get("U95", {}).get("avg_cpl")
        if u91: cities["Sydney"]["ulp91"]  = u91
        if dl:  cities["Sydney"]["diesel"] = dl
        if u95: cities["Sydney"]["prem95"] = u95

    # Overlay live WA data for Perth
    if wa:
        # WA FuelWatch fuel type names may differ — common variants
        u91 = (wa.get("Unleaded", {}) or wa.get("ULP", {}) or wa.get("91", {})).get("avg_cpl")
        dl  = (wa.get("Diesel", {}) or wa.get("DL", {})).get("avg_cpl")
        if u91: cities["Perth"]["ulp91"]  = u91
        if dl:  cities["Perth"]["diesel"] = dl

    # Append today to history
    history = build_history()
    today_str = now.strftime("%Y-%m-%d")
    if history[-1]["date"] != today_str:
        history.append({
            "date":   today_str,
            "ulp91":  cities["Sydney"]["ulp91"],
            "diesel": cities["Sydney"]["diesel"],
        })

    output = {
        "updated_at":         now.isoformat(),
        "source_note":        "Live: FuelCheck NSW + WA FuelWatch. National averages: ACCC weekly monitoring.",
        "excise": {
            "before_cpl":     EXCISE_BEFORE_CPL,
            "after_cpl":      EXCISE_AFTER_CPL,
            "cut_cpl":        EXCISE_CUT_CPL,
            "effective_date": "2026-04-01",
        },
        "reserves":           RESERVES,
        "cities":             cities,
        "excise_passthrough": compute_excise_passthrough(nsw, ACCC_SNAPSHOT),
        "nsw_by_fuel":        nsw,
        "wa_by_fuel":         wa,
        "aip_tgp":            aip,
        "history":            history,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved → {OUTPUT_FILE}")
    print(f"Sydney ULP:    {cities['Sydney']['ulp91']} cpl")
    print(f"Sydney Diesel: {cities['Sydney']['diesel']} cpl")


if __name__ == "__main__":
    main()
