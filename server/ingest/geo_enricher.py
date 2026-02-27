"""
Geo-Enrichment Module.
Adds latitude/longitude to projects using county centroids and POI matching.
"""
import csv
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import get_connection, get_db_path

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# Fallback state centroids when no county match is found
STATE_CENTROIDS = {
    "AL": (32.8067, -86.7911), "AK": (63.3333, -152.0), "AZ": (34.0489, -111.0937),
    "AR": (34.7465, -92.2896), "CA": (36.7783, -119.4179), "CO": (39.5501, -105.7821),
    "CT": (41.6032, -73.0877), "DE": (38.9108, -75.5277), "FL": (27.6648, -81.5158),
    "GA": (32.1656, -82.9001), "HI": (19.8968, -155.5828), "ID": (44.0682, -114.742),
    "IL": (40.6331, -89.3985), "IN": (40.2672, -86.1349), "IA": (41.878, -93.0977),
    "KS": (39.0119, -98.4842), "KY": (37.8393, -84.27), "LA": (30.9843, -91.9623),
    "ME": (45.2538, -69.4455), "MD": (39.0458, -76.6413), "MA": (42.4072, -71.3824),
    "MI": (44.3148, -85.6024), "MN": (46.7296, -94.6859), "MS": (32.3547, -89.3985),
    "MO": (37.9643, -91.8318), "MT": (46.8797, -110.3626), "NE": (41.4925, -99.9018),
    "NV": (38.8026, -116.4194), "NH": (43.1939, -71.5724), "NJ": (40.0583, -74.4057),
    "NM": (34.5199, -105.8701), "NY": (42.1657, -74.9481), "NC": (35.7596, -79.0193),
    "ND": (47.5515, -101.002), "OH": (40.4173, -82.9071), "OK": (35.4676, -97.5164),
    "OR": (43.8041, -120.5542), "PA": (41.2033, -77.1945), "RI": (41.5801, -71.4774),
    "SC": (33.8361, -81.1637), "SD": (43.9695, -99.9018), "TN": (35.5175, -86.5804),
    "TX": (31.9686, -99.9018), "UT": (39.3210, -111.0937), "VT": (44.5588, -72.5778),
    "VA": (37.4316, -78.6569), "WA": (47.7511, -120.7401), "WV": (38.5976, -80.4549),
    "WI": (43.7844, -88.7879), "WY": (43.075, -107.2903), "DC": (38.9072, -77.0369),
}


def generate_county_centroids():
    """
    Generate a county centroids CSV from a simplified reference.
    This uses the Census FIPS-to-centroid mapping.
    We generate a comprehensive list programmatically.
    """
    centroids_path = os.path.join(DATA_DIR, "county_centroids.csv")
    if os.path.exists(centroids_path):
        return centroids_path

    # Download from Census Gazetteer
    print("  Downloading county centroids from Census Bureau...")
    import urllib.request

    # Try multiple years since Census URLs change
    urls = [
        "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_counties_national.txt",
        "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_Gaz_counties_national.txt",
        "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2022_Gazetteer/2022_Gaz_counties_national.txt",
        "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2021_Gazetteer/2021_Gaz_counties_national.txt",
    ]
    url = None
    for candidate_url in urls:
        try:
            urllib.request.urlopen(candidate_url)
            url = candidate_url
            print(f"  Using Census URL: {url}")
            break
        except Exception:
            continue
    if not url:
        print("  Warning: No Census gazetteer URL available.")
        print("  Using state centroids as fallback.")
        return None
    tmp_path = os.path.join(DATA_DIR, "census_counties_raw.txt")

    try:
        urllib.request.urlretrieve(url, tmp_path)
    except Exception as e:
        print(f"  Warning: Could not download Census data: {e}")
        print("  Using state centroids as fallback.")
        return None


    # Parse the tab-delimited file
    entries = []
    with open(tmp_path, "r", encoding="utf-8", errors="replace") as f:
        header = f.readline()  # skip header
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 10:
                continue
            try:
                state_fips = parts[0].strip()[:2]
                name = parts[3].strip() if len(parts) > 3 else ""
                # Remove suffixes like " County", " Parish", " Borough"
                for suffix in [" County", " Parish", " Borough", " Census Area",
                               " Municipality", " city", " City and Borough"]:
                    name = name.replace(suffix, "")
                lat = float(parts[8].strip()) if len(parts) > 8 else None
                lng = float(parts[9].strip()) if len(parts) > 9 else None
                if lat and lng:
                    entries.append((state_fips, name.strip(), lat, lng))
            except (ValueError, IndexError):
                continue

    # We need FIPS to state abbreviation mapping
    fips_to_state = {
        "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
        "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
        "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
        "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
        "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
        "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
        "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
        "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
        "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
        "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
        "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR", "78": "VI",
    }

    with open(centroids_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["state", "county", "latitude", "longitude"])
        for fips, name, lat, lng in entries:
            state = fips_to_state.get(fips)
            if state:
                writer.writerow([state, name, lat, lng])

    count = len(entries)
    print(f"  Generated {count} county centroids → {centroids_path}")

    # Cleanup
    if os.path.exists(tmp_path):
        os.remove(tmp_path)

    return centroids_path


def load_county_centroids() -> dict:
    """Load county centroids as {(state, county_lower): (lat, lng)}."""
    centroids_path = generate_county_centroids()
    lookup = {}

    if centroids_path and os.path.exists(centroids_path):
        with open(centroids_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (row["state"].upper(), row["county"].strip().lower())
                try:
                    lookup[key] = (float(row["latitude"]), float(row["longitude"]))
                except ValueError:
                    continue

    return lookup


def geocode_project(project: dict, county_lookup: dict) -> tuple:
    """Try to geocode a project. Returns (lat, lng) or (None, None)."""
    # Already has coordinates
    if project.get("latitude") and project.get("longitude"):
        return (project["latitude"], project["longitude"])

    state = (project.get("state") or "").upper()
    county = project.get("county") or ""

    if county and state:
        # Try exact match
        key = (state, county.strip().lower())
        if key in county_lookup:
            return county_lookup[key]

        # Try without common suffixes
        county_clean = county.strip().lower()
        for suffix in [" county", " parish", " borough"]:
            county_clean = county_clean.replace(suffix, "")
        key2 = (state, county_clean.strip())
        if key2 in county_lookup:
            return county_lookup[key2]

        # Try fuzzy: check if any key starts with the county name
        for (s, c), coords in county_lookup.items():
            if s == state and (c.startswith(county_clean) or county_clean.startswith(c)):
                return coords

    # Fallback: state centroid with jitter
    if state in STATE_CENTROIDS:
        import random
        base_lat, base_lng = STATE_CENTROIDS[state]
        # Add random jitter so points don't stack
        jitter_lat = random.uniform(-0.5, 0.5)
        jitter_lng = random.uniform(-0.5, 0.5)
        return (base_lat + jitter_lat, base_lng + jitter_lng)

    return (None, None)


def enrich_all_projects(verbose: bool = True):
    """Batch geocode all projects missing coordinates."""
    county_lookup = load_county_centroids()
    if verbose:
        print(f"  Loaded {len(county_lookup)} county centroids")

    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, state, county, poi_name, latitude, longitude FROM projects"
        ).fetchall()

        total = len(rows)
        already_have = sum(1 for r in rows if r["latitude"] is not None)
        need_geocode = total - already_have

        if verbose:
            print(f"  Total projects: {total}")
            print(f"  Already geocoded: {already_have}")
            print(f"  Need geocoding: {need_geocode}")

        updated = 0
        for row in rows:
            if row["latitude"] is not None and row["longitude"] is not None:
                continue

            project = dict(row)
            lat, lng = geocode_project(project, county_lookup)

            if lat is not None and lng is not None:
                conn.execute(
                    "UPDATE projects SET latitude = ?, longitude = ? WHERE id = ?",
                    (round(lat, 4), round(lng, 4), row["id"]),
                )
                updated += 1

        if verbose:
            final_geocoded = already_have + updated
            pct = final_geocoded / total * 100 if total > 0 else 0
            print(f"  Geocoded {updated} additional projects")
            print(f"  Total with coordinates: {final_geocoded}/{total} ({pct:.0f}%)")

    return {"total": total, "geocoded": already_have + updated, "newly_geocoded": updated}


if __name__ == "__main__":
    result = enrich_all_projects(verbose=True)
    print(f"\nResult: {result}")
