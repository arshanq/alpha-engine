"""
gridstatus Interconnection Queue Puller.
Pulls live queue data from ISOs and normalizes into our schema.
"""
import sys
import os
import time
from datetime import datetime

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import upsert_projects_batch, log_ingestion, init_db

# Technology normalization map
# gridstatus returns various strings per ISO — we canonicalize
TECH_MAP = {
    # Solar variants
    "solar": "Solar",
    "solar photovoltaic": "Solar",
    "solar pv": "Solar",
    "photovoltaic": "Solar",
    "solar thermal": "Solar",
    # Wind variants
    "wind": "Wind",
    "onshore wind": "Wind",
    "offshore wind": "Wind",
    "wind turbine": "Wind",
    # Storage variants
    "storage": "Battery Storage",
    "battery": "Battery Storage",
    "battery storage": "Battery Storage",
    "energy storage": "Battery Storage",
    "bess": "Battery Storage",
    "batteries": "Battery Storage",
    "battery energy storage": "Battery Storage",
    "standalone storage": "Battery Storage",
    "co-located storage": "Battery Storage",
    # Gas variants
    "gas": "Natural Gas",
    "natural gas": "Natural Gas",
    "gas ct": "Natural Gas",
    "gas cc": "Natural Gas",
    "gas turbine": "Natural Gas",
    "combustion turbine": "Natural Gas",
    "combined cycle": "Natural Gas",
    "ct": "Natural Gas",
    "cc": "Natural Gas",
    "ccgt": "Natural Gas",
    # Nuclear
    "nuclear": "Nuclear",
    # Hybrid
    "hybrid": "Hybrid",
    "solar + storage": "Hybrid",
    "solar+storage": "Hybrid",
    "wind + storage": "Hybrid",
    "wind+storage": "Hybrid",
    "solar & storage": "Hybrid",
    # Hydro
    "hydro": "Hydro",
    "hydroelectric": "Hydro",
    "pumped storage": "Hydro",
    "pump storage": "Hydro",
    # Coal
    "coal": "Coal",
    # Other
    "other": "Other",
    "unknown": "Other",
    "fuel cell": "Other",
    "biomass": "Other",
    "geothermal": "Other",
    "waste heat": "Other",
    "diesel": "Other",
    "oil": "Other",
    "landfill gas": "Other",
    "hydrogen": "Other",
}

# Status normalization map
STATUS_MAP = {
    "active": "Active",
    "operational": "Operational",
    "in service": "Operational",
    "completed": "Operational",
    "commercial operation": "Operational",
    "ia executed": "Active",
    "ia pending": "Active",
    "withdrawn": "Withdrawn",
    "deactivated": "Withdrawn",
    "annulled": "Withdrawn",
    "retracted": "Withdrawn",
    "suspended": "Suspended",
    "on hold": "Suspended",
    # ERCOT-specific
    "ia fully executed": "Active",
    "gia executed": "Active",
    "construction": "Active",
    "under construction": "Active",
    # PJM-specific
    "engineering and procurement": "Active",
    "feasibility study": "Active",
    "system impact study": "Active",
    "facilities study": "Active",
    "in queue": "Active",
    "phase 1": "Active",
    "phase 2": "Active",
    "phase 3": "Active",
    # CAISO-specific
    "cluster study": "Active",
    "pending": "Active",
    "not yet started": "Active",
    "queue": "Active",
}


def normalize_technology(raw: str) -> str:
    """Normalize technology string to canonical form."""
    if pd.isna(raw) or not raw:
        return "Other"
    raw_lower = str(raw).strip().lower()

    # Check direct map first
    if raw_lower in TECH_MAP:
        return TECH_MAP[raw_lower]

    # Substring matching for compound types
    if "solar" in raw_lower and "storage" in raw_lower:
        return "Hybrid"
    if "wind" in raw_lower and "storage" in raw_lower:
        return "Hybrid"
    if "solar" in raw_lower:
        return "Solar"
    if "wind" in raw_lower:
        return "Wind"
    if "storage" in raw_lower or "battery" in raw_lower or "bess" in raw_lower:
        return "Battery Storage"
    if "gas" in raw_lower or "combustion" in raw_lower or "combined cycle" in raw_lower:
        return "Natural Gas"
    if "nuclear" in raw_lower:
        return "Nuclear"
    if "hydro" in raw_lower or "pump" in raw_lower:
        return "Hydro"
    if "coal" in raw_lower:
        return "Coal"

    return "Other"


def normalize_status(raw: str) -> str:
    """Normalize status string to canonical form."""
    if pd.isna(raw) or not raw:
        return "Active"
    raw_lower = str(raw).strip().lower()

    if raw_lower in STATUS_MAP:
        return STATUS_MAP[raw_lower]

    # Substring matching
    if "withdraw" in raw_lower or "cancel" in raw_lower:
        return "Withdrawn"
    if "active" in raw_lower or "study" in raw_lower or "queue" in raw_lower:
        return "Active"
    if "operat" in raw_lower or "service" in raw_lower or "complet" in raw_lower:
        return "Operational"
    if "suspend" in raw_lower or "hold" in raw_lower:
        return "Suspended"

    return "Active"


# Valid US state codes
VALID_US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
}

# State code corrections
STATE_FIXES = {
    "TE": "TX",   # ERCOT uses TE for Texas
    "TEX": "TX",
    "TEXAS": "TX",
    "CALIF": "CA",
    "CALIFORNIA": "CA",
    "NEW YORK": "NY",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "OHIO": "OH",
    "MAINE": "ME",
    "MASS": "MA",
    "MASSACHUSETTS": "MA",
    "CONN": "CT",
    "CONNECTICUT": "CT",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSOURI": "MO",
    "WISCONSIN": "WI",
    "ARKANSAS": "AR",
    "KANSAS": "KS",
    "LOUISIANA": "LA",
    "MISSISSIPPI": "MS",
    "NEBRASKA": "NE",
    "OKLAHOMA": "OK",
    "NORTH DAKOTA": "ND",
    "SOUTH DAKOTA": "SD",
    "MONTANA": "MT",
    "WYOMING": "WY",
    "NEW MEXICO": "NM",
    "COLORADO": "CO",
    "NEVADA": "NV",
    "VERMONT": "VT",
    "NEW HAMPSHIRE": "NH",
    "RHODE ISLAND": "RI",
}


def normalize_state(raw: str) -> str:
    """Normalize state string to 2-char US state code. Returns None for non-US."""
    if not raw:
        return None
    cleaned = raw.strip().upper()

    # Check fixes first
    if cleaned in STATE_FIXES:
        return STATE_FIXES[cleaned]

    # Take first 2 chars
    code = cleaned[:2]

    # Check fixes again with 2-char
    if code in STATE_FIXES:
        return STATE_FIXES[code]

    # Valid US state?
    if code in VALID_US_STATES:
        return code

    return None


def safe_str(val):
    """Convert a value to string, handling NaN/None."""
    if pd.isna(val) or val is None:
        return None
    return str(val).strip()


def safe_float(val):
    """Convert a value to float, handling NaN/None."""
    if pd.isna(val) or val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_date(val):
    """Convert a value to ISO date string."""
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    try:
        return pd.Timestamp(val).strftime("%Y-%m-%d")
    except Exception:
        return str(val)[:10] if val else None


def pull_iso(iso_name: str, verbose: bool = True) -> pd.DataFrame:
    """Pull interconnection queue from a single ISO."""
    import gridstatus

    iso_classes = {
        "PJM": gridstatus.PJM,
        "MISO": gridstatus.MISO,
        "ERCOT": gridstatus.Ercot,
        "CAISO": gridstatus.CAISO,
        "NYISO": gridstatus.NYISO,
        "SPP": gridstatus.SPP,
        "ISONE": gridstatus.ISONE,
    }

    if iso_name.upper() not in iso_classes:
        raise ValueError(f"Unknown ISO: {iso_name}. Available: {list(iso_classes.keys())}")

    iso_cls = iso_classes[iso_name.upper()]
    iso = iso_cls()

    if verbose:
        print(f"  Pulling {iso_name}...")

    start = time.time()
    df = iso.get_interconnection_queue()
    elapsed = time.time() - start

    if verbose:
        print(f"  {iso_name}: {len(df)} records in {elapsed:.1f}s")
        print(f"  Columns: {list(df.columns)}")
        print(f"  Sample row:\n{df.iloc[0].to_dict() if len(df) > 0 else 'EMPTY'}")

    return df


import urllib.parse

def generate_project_url(iso_name: str, queue_id: str, project_name: str = None, developer: str = None) -> str:
    """Generate a targeted Google Search link for a project."""
    if not queue_id:
        return ""
        
    query = f'"{iso_name.upper()}" "{queue_id}"'
    if project_name and len(str(project_name)) > 2:
        query += f' "{project_name}"'
    elif developer and len(str(developer)) > 2:
        query += f' "{developer}"'
        
    encoded_query = urllib.parse.quote_plus(query)
    return f"https://www.google.com/search?q={encoded_query}"


def dataframe_to_projects(df: pd.DataFrame, iso_name: str) -> list[dict]:
    """Convert a gridstatus DataFrame to our project schema."""
    projects = []

    # Detect column names (gridstatus standardizes these)
    col_map = {}
    for col in df.columns:
        col_lower = col.lower().strip()
        if "queue" in col_lower and "id" in col_lower:
            col_map["queue_id"] = col
        elif col_lower in ("capacity (mw)", "capacity_mw", "capacity"):
            col_map["capacity_mw"] = col
        elif col_lower in ("summer capacity (mw)", "summer_capacity_mw"):
            col_map["summer_capacity_mw"] = col
        elif col_lower in ("winter capacity (mw)", "winter_capacity_mw"):
            col_map["winter_capacity_mw"] = col
        elif col_lower in ("county",):
            col_map["county"] = col
        elif col_lower in ("state",):
            col_map["state"] = col
        elif col_lower in ("interconnection location", "poi_name", "point of interconnection",
                           "gen interconnection point of receipt", "proposed point of interconnection"):
            col_map["poi_name"] = col
        elif col_lower in ("generation type", "type", "fuel", "technology",
                           "generation fuel", "fuel type"):
            col_map["technology"] = col
        elif col_lower in ("status",):
            col_map["status"] = col
        elif col_lower in ("queue date", "queue_date", "request date",
                           "date entered queue", "application date"):
            col_map["queue_date"] = col
        elif col_lower in ("proposed completion date", "proposed_cod",
                           "proposed online date", "expected completion",
                           "projected cod", "projected in-service date",
                           "proposed in-service date"):
            col_map["proposed_cod"] = col
        elif col_lower in ("withdrawn date", "withdrawal date", "withdrawal_date"):
            col_map["withdrawal_date"] = col
        elif col_lower in ("actual completion date", "actual_cod",
                           "commercial operation date", "actual in-service date"):
            col_map["actual_cod"] = col
        elif col_lower in ("project name", "project_name", "name"):
            col_map["project_name"] = col
        elif col_lower in ("interconnecting entity", "developer", "applicant",
                           "entity"):
            col_map["developer"] = col
        elif col_lower in ("latitude", "lat"):
            col_map["latitude"] = col
        elif col_lower in ("longitude", "lng", "lon", "long"):
            col_map["longitude"] = col

    for idx, row in df.iterrows():
        queue_id_raw = safe_str(row.get(col_map.get("queue_id", ""), None))
        if not queue_id_raw:
            queue_id_raw = f"{iso_name}-AUTO-{idx}"

        # Prefix ISO to queue_id for global uniqueness
        queue_id = f"{iso_name}-{queue_id_raw}" if not queue_id_raw.startswith(iso_name) else queue_id_raw

        project = {
            "queue_id": queue_id,
            "iso": iso_name.upper(),
            "project_name": safe_str(row.get(col_map.get("project_name", ""), None)),
            "developer": safe_str(row.get(col_map.get("developer", ""), None)),
            "county": normalize_county(safe_str(row.get(col_map.get("county", ""), None))),
            "state": normalize_state(safe_str(row.get(col_map.get("state", ""), None))),
            "poi_name": safe_str(row.get(col_map.get("poi_name", ""), None)),
            "capacity_mw": safe_float(row.get(col_map.get("capacity_mw", ""), None)),
            "summer_capacity_mw": safe_float(row.get(col_map.get("summer_capacity_mw", ""), None)),
            "winter_capacity_mw": safe_float(row.get(col_map.get("winter_capacity_mw", ""), None)),
            "technology": normalize_technology(
                safe_str(row.get(col_map.get("technology", ""), None))
            ),
            "status": normalize_status(
                safe_str(row.get(col_map.get("status", ""), None))
            ),
            "queue_date": safe_date(row.get(col_map.get("queue_date", ""), None)),
            "proposed_cod": safe_date(row.get(col_map.get("proposed_cod", ""), None)),
            "withdrawal_date": safe_date(row.get(col_map.get("withdrawal_date", ""), None)),
            "actual_cod": safe_date(row.get(col_map.get("actual_cod", ""), None)),
            "latitude": safe_float(row.get(col_map.get("latitude", ""), None)),
            "longitude": safe_float(row.get(col_map.get("longitude", ""), None)),
            "project_url": generate_project_url(
                iso_name, 
                queue_id_raw,
                safe_str(row.get(col_map.get("project_name", ""), None)),
                safe_str(row.get(col_map.get("developer", ""), None))
            ),
            "data_source": "gridstatus",
        }

        # Clean up: skip projects with no capacity
        if project["capacity_mw"] is None or project["capacity_mw"] <= 0:
            continue

        # State normalization
        if project["state"]:
            project["state"] = normalize_state(project["state"])

        # Skip projects with no valid US state
        if not project["state"]:
            continue

        projects.append(project)

    return projects


def ingest_iso(iso_name: str, verbose: bool = True) -> dict:
    """Pull and ingest a single ISO's queue data."""
    started_at = datetime.utcnow().isoformat()

    try:
        df = pull_iso(iso_name, verbose=verbose)

        if verbose:
            print(f"\n  Column mapping analysis for {iso_name}:")
            for col in df.columns:
                null_pct = df[col].isna().sum() / len(df) * 100 if len(df) > 0 else 0
                print(f"    {col}: {null_pct:.0f}% null ({df[col].dtype})")

        projects = dataframe_to_projects(df, iso_name)

        if verbose:
            print(f"\n  Converted {len(projects)} valid projects (from {len(df)} raw rows)")
            techs = {}
            statuses = {}
            for p in projects:
                techs[p["technology"]] = techs.get(p["technology"], 0) + 1
                statuses[p["status"]] = statuses.get(p["status"], 0) + 1
            print(f"  Technology distribution: {techs}")
            print(f"  Status distribution: {statuses}")
            has_coords = sum(1 for p in projects if p["latitude"] is not None)
            print(f"  Has coordinates: {has_coords}/{len(projects)} ({has_coords/len(projects)*100:.0f}%)")

        upsert_projects_batch(projects)

        log_ingestion(
            iso=iso_name, source="gridstatus",
            records_ingested=len(projects), records_updated=0,
            status="success", started_at=started_at,
        )

        return {
            "iso": iso_name,
            "status": "success",
            "records": len(projects),
            "raw_rows": len(df),
        }

    except Exception as e:
        log_ingestion(
            iso=iso_name, source="gridstatus",
            records_ingested=0, records_updated=0,
            status="failed", error_message=str(e),
            started_at=started_at,
        )
        if verbose:
            print(f"  ERROR pulling {iso_name}: {e}")
            import traceback
            traceback.print_exc()
        return {
            "iso": iso_name,
            "status": "failed",
            "error": str(e),
        }


def ingest_all_isos(isos=None, verbose=True) -> list[dict]:
    """Pull and ingest all ISOs sequentially."""
    if isos is None:
        isos = ["CAISO", "NYISO", "PJM", "MISO", "ERCOT", "SPP", "ISONE"]

    init_db()
    results = []

    print(f"\n{'='*60}")
    print(f"  gridstatus Ingestion Pipeline")
    print(f"  ISOs: {isos}")
    print(f"  Started: {datetime.utcnow().isoformat()}")
    print(f"{'='*60}\n")

    for iso_name in isos:
        print(f"\n--- {iso_name} ---")
        result = ingest_iso(iso_name, verbose=verbose)
        results.append(result)
        print(f"  Result: {result['status']} "
              f"({result.get('records', 0)} records)")

    # Summary
    print(f"\n{'='*60}")
    print(f"  Ingestion Complete")
    total = sum(r.get("records", 0) for r in results)
    success = sum(1 for r in results if r["status"] == "success")
    print(f"  {success}/{len(results)} ISOs succeeded, {total} total records")
    print(f"{'='*60}\n")

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pull interconnection queue data")
    parser.add_argument("--iso", type=str, help="Specific ISO to pull (e.g., CAISO)")
    parser.add_argument("--all", action="store_true", help="Pull all ISOs")
    parser.add_argument("--quiet", action="store_true", help="Less verbose output")
    args = parser.parse_args()

    if args.iso:
        init_db()
        result = ingest_iso(args.iso.upper(), verbose=not args.quiet)
        print(f"\nResult: {result}")
    elif args.all:
        results = ingest_all_isos(verbose=not args.quiet)
    else:
        # Default: pull just CAISO as a quick test
        init_db()
        result = ingest_iso("CAISO", verbose=True)
        print(f"\nResult: {result}")
