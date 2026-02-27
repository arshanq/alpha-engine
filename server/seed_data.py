"""
Seed Data Generator for Infrastructure Alpha Engine.
Generates ~500 realistic synthetic interconnection queue records
spread across PJM, MISO, ERCOT, CAISO, and NYISO.
"""
import json
import random
import os
from datetime import datetime, timedelta

random.seed(42)

ISOS = {
    "PJM": {
        "states": ["PA", "NJ", "MD", "VA", "WV", "OH", "IN", "IL", "DE", "NC", "KY"],
        "counties": {
            "PA": ["Lancaster", "Chester", "York", "Adams", "Dauphin", "Berks", "Montgomery"],
            "NJ": ["Burlington", "Mercer", "Middlesex", "Monmouth", "Ocean"],
            "MD": ["Frederick", "Washington", "Carroll", "Harford", "Cecil"],
            "VA": ["Loudoun", "Prince William", "Fauquier", "Culpeper", "Spotsylvania"],
            "WV": ["Berkeley", "Jefferson", "Hampshire", "Hardy", "Grant"],
            "OH": ["Franklin", "Delaware", "Licking", "Fairfield", "Pickaway"],
            "IN": ["Marion", "Hamilton", "Hendricks", "Johnson", "Boone"],
            "IL": ["Will", "Grundy", "LaSalle", "DeKalb", "Lee"],
            "DE": ["Kent", "Sussex", "New Castle"],
            "NC": ["Mecklenburg", "Wake", "Durham", "Guilford"],
            "KY": ["Fayette", "Scott", "Woodford", "Jessamine"],
        },
        "coords_center": (40.0, -77.0),
        "coord_spread": (3.5, 5.0),
    },
    "MISO": {
        "states": ["MN", "WI", "IA", "MO", "IL", "IN", "MI", "ND", "SD", "MT", "AR", "LA", "MS", "TX"],
        "counties": {
            "MN": ["Hennepin", "Ramsey", "Dakota", "Anoka", "Washington", "Scott"],
            "WI": ["Dane", "Milwaukee", "Waukesha", "Rock", "Dodge"],
            "IA": ["Polk", "Story", "Dallas", "Warren", "Boone", "Jasper"],
            "MO": ["Jackson", "Clay", "Platte", "Cass", "Ray"],
            "IL": ["McLean", "Champaign", "Sangamon", "Macon", "Peoria"],
            "IN": ["Tippecanoe", "Vigo", "Vanderburgh", "Allen"],
            "MI": ["Wayne", "Oakland", "Washtenaw", "Ingham"],
            "ND": ["Cass", "Burleigh", "Grand Forks", "Ward"],
            "SD": ["Minnehaha", "Lincoln", "Pennington"],
            "MT": ["Yellowstone", "Cascade", "Gallatin"],
            "AR": ["Pulaski", "Benton", "Washington"],
            "LA": ["East Baton Rouge", "Caddo", "Calcasieu"],
            "MS": ["Hinds", "Harrison", "DeSoto"],
            "TX": ["Harris", "Dallas", "Tarrant"],
        },
        "coords_center": (42.0, -90.0),
        "coord_spread": (6.0, 8.0),
    },
    "ERCOT": {
        "states": ["TX"],
        "counties": {
            "TX": [
                "Harris", "Dallas", "Tarrant", "Bexar", "Travis", "Collin",
                "Denton", "Hidalgo", "El Paso", "Fort Bend", "Williamson",
                "Montgomery", "Brazoria", "Lubbock", "Webb", "McLennan",
                "Midland", "Ector", "Taylor", "Nueces", "Cameron", "Hays",
                "Bell", "Galveston", "Smith", "Brazos", "Tom Green",
            ],
        },
        "coords_center": (31.5, -99.0),
        "coord_spread": (4.0, 5.0),
    },
    "CAISO": {
        "states": ["CA"],
        "counties": {
            "CA": [
                "Los Angeles", "San Bernardino", "Riverside", "San Diego",
                "Orange", "Kern", "Fresno", "Sacramento", "Alameda",
                "Contra Costa", "Tulare", "San Joaquin", "Stanislaus",
                "Santa Clara", "Imperial", "Kings", "Madera", "Merced",
            ],
        },
        "coords_center": (36.5, -119.5),
        "coord_spread": (4.0, 3.0),
    },
    "NYISO": {
        "states": ["NY"],
        "counties": {
            "NY": [
                "Suffolk", "Nassau", "Westchester", "Erie", "Monroe",
                "Onondaga", "Orange", "Rockland", "Albany", "Dutchess",
                "Saratoga", "Rensselaer", "Schenectady", "Ulster",
                "Columbia", "Greene", "Sullivan", "Delaware",
            ],
        },
        "coords_center": (42.5, -75.5),
        "coord_spread": (2.0, 3.0),
    },
}

TECHNOLOGIES = [
    ("Solar", 0.45),
    ("Wind", 0.18),
    ("Battery Storage", 0.20),
    ("Natural Gas", 0.08),
    ("Hybrid (Solar+Storage)", 0.06),
    ("Nuclear", 0.01),
    ("Other", 0.02),
]

STATUSES = [
    ("Active", 0.55),
    ("Withdrawn", 0.30),
    ("Operational", 0.10),
    ("Suspended", 0.05),
]

DEVELOPERS = [
    "Apex Clean Energy", "NextEra Energy", "Invenergy", "EDF Renewables",
    "Avangrid Renewables", "Pattern Energy", "Enel Green Power",
    "Clearway Energy", "BHE Renewables", "Orsted", "Hecate Energy",
    "Savion", "Tenaska", "Longroad Energy", "Scout Clean Energy",
    "Pine Gate Renewables", "Silicon Ranch", "Arevon", "Lightsource BP",
    "EDPR", "RWE", "AES Clean Energy", "Connexus Energy", "Unknown Developer",
    "Leeward Renewable Energy", "TotalEnergies", "Intersect Power",
    "Terra-Gen", "Geenex Solar", "Sol Systems",
]


def weighted_choice(items_weights):
    items, weights = zip(*items_weights)
    return random.choices(items, weights=weights, k=1)[0]


def generate_queue_id(iso, idx):
    prefixes = {"PJM": "AF", "MISO": "J", "ERCOT": "INR", "CAISO": "QUE", "NYISO": "Q"}
    prefix = prefixes.get(iso, "X")
    return f"{prefix}-{idx:04d}"


def compute_success_probability(project):
    """Heuristic scoring based on LBNL 'Queued Up' patterns."""
    score = 0.14  # base rate from LBNL data

    tech = project["technology"]
    if tech == "Solar":
        score += 0.05
    elif tech == "Battery Storage":
        score += 0.08
    elif tech == "Hybrid (Solar+Storage)":
        score += 0.12
    elif tech == "Natural Gas":
        score += 0.15
    elif tech == "Nuclear":
        score -= 0.05
    elif tech == "Wind":
        score += 0.03

    mw = project["capacity_mw"]
    if mw < 50:
        score += 0.06
    elif mw < 200:
        score += 0.03
    elif mw > 500:
        score -= 0.04
    if mw > 1000:
        score -= 0.06

    dev = project["developer"]
    top_devs = {"NextEra Energy", "Invenergy", "EDF Renewables", "Clearway Energy", "Orsted", "BHE Renewables"}
    if dev in top_devs:
        score += 0.10

    queue_days = project["queue_days"]
    if queue_days < 365:
        score += 0.02
    elif queue_days > 1095:
        score -= 0.05

    if project["status"] == "Operational":
        score = 1.0
    elif project["status"] == "Withdrawn":
        score = 0.0
    elif project["status"] == "Suspended":
        score *= 0.3

    return round(max(0.01, min(0.98, score)), 2)


def compute_workforce(project):
    """Estimate construction workers needed based on MW and technology."""
    mw = project["capacity_mw"]
    tech = project["technology"]
    base_workers_per_mw = {
        "Solar": 0.8, "Wind": 1.2, "Battery Storage": 0.4,
        "Natural Gas": 1.5, "Hybrid (Solar+Storage)": 0.9,
        "Nuclear": 3.0, "Other": 0.6,
    }
    workers = int(mw * base_workers_per_mw.get(tech, 0.8) * random.uniform(0.8, 1.3))
    electricians = int(workers * random.uniform(0.25, 0.40))
    duration_years = max(1, int(mw / 200 * random.uniform(0.8, 1.5)))
    return {
        "total_workers": max(10, workers),
        "electricians_needed": max(5, electricians),
        "construction_duration_years": min(8, duration_years),
    }


def generate_projects():
    projects = []
    idx = 0

    iso_distribution = {
        "PJM": 140, "MISO": 130, "ERCOT": 100, "CAISO": 80, "NYISO": 50,
    }

    for iso, count in iso_distribution.items():
        config = ISOS[iso]
        for _ in range(count):
            idx += 1
            state = random.choice(config["states"])
            county = random.choice(config["counties"].get(state, ["Unknown"]))
            tech = weighted_choice(TECHNOLOGIES)
            status = weighted_choice(STATUSES)

            if tech == "Solar":
                mw = random.choice([*range(10, 100, 5)] * 3 + [*range(100, 500, 25)] * 2 + [*range(500, 2000, 100)])
            elif tech == "Wind":
                mw = random.choice([*range(50, 300, 25)] * 3 + [*range(300, 800, 50)])
            elif tech == "Battery Storage":
                mw = random.choice([*range(25, 200, 25)] * 3 + [*range(200, 600, 50)])
            elif tech == "Natural Gas":
                mw = random.choice([*range(100, 500, 50)] + [*range(500, 2000, 100)])
            elif tech == "Nuclear":
                mw = random.choice([300, 450, 600, 900, 1100, 1400])
            else:
                mw = random.randint(20, 400)

            lat = config["coords_center"][0] + random.uniform(-config["coord_spread"][0], config["coord_spread"][0])
            lng = config["coords_center"][1] + random.uniform(-config["coord_spread"][1], config["coord_spread"][1])

            queue_date = datetime(2018, 1, 1) + timedelta(days=random.randint(0, 2500))
            queue_days = (datetime(2026, 2, 25) - queue_date).days

            poi_name = f"{county} Sub-{random.randint(100, 999)}"
            developer = random.choice(DEVELOPERS)

            project = {
                "id": idx,
                "queue_id": generate_queue_id(iso, idx),
                "iso": iso,
                "state": state,
                "county": county,
                "technology": tech,
                "capacity_mw": mw,
                "status": status,
                "latitude": round(lat, 4),
                "longitude": round(lng, 4),
                "queue_date": queue_date.strftime("%Y-%m-%d"),
                "queue_days": queue_days,
                "poi_name": poi_name,
                "developer": developer,
                "voltage_kv": random.choice([69, 115, 138, 230, 345, 500]),
                "estimated_cod": (queue_date + timedelta(days=random.randint(730, 2555))).strftime("%Y-%m-%d"),
            }

            project["success_probability"] = compute_success_probability(project)
            project["is_phantom"] = (
                project["success_probability"] < 0.20
                and project["status"] == "Active"
                and project["queue_days"] > 730
            )
            project["workforce"] = compute_workforce(project)

            projects.append(project)

    return projects


def generate_state_summaries(projects):
    """Aggregate stats by state."""
    from collections import defaultdict

    state_data = defaultdict(lambda: {
        "total_mw": 0, "project_count": 0, "active_count": 0,
        "operational_mw": 0, "queue_days_list": [], "success_scores": [],
        "technologies": defaultdict(int), "isos": set(),
    })

    for p in projects:
        s = state_data[p["state"]]
        s["total_mw"] += p["capacity_mw"]
        s["project_count"] += 1
        if p["status"] == "Active":
            s["active_count"] += 1
        if p["status"] == "Operational":
            s["operational_mw"] += p["capacity_mw"]
        s["queue_days_list"].append(p["queue_days"])
        s["success_scores"].append(p["success_probability"])
        s["technologies"][p["technology"]] += 1
        s["isos"].add(p["iso"])

    summaries = {}
    for state, d in state_data.items():
        sorted_days = sorted(d["queue_days_list"])
        median_days = sorted_days[len(sorted_days) // 2] if sorted_days else 0
        avg_success = sum(d["success_scores"]) / len(d["success_scores"]) if d["success_scores"] else 0
        summaries[state] = {
            "state": state,
            "total_mw": d["total_mw"],
            "total_gw": round(d["total_mw"] / 1000, 2),
            "project_count": d["project_count"],
            "active_count": d["active_count"],
            "operational_mw": d["operational_mw"],
            "median_queue_days": median_days,
            "avg_success_probability": round(avg_success, 2),
            "top_technology": max(d["technologies"], key=d["technologies"].get),
            "isos": list(d["isos"]),
        }

    return summaries


if __name__ == "__main__":
    projects = generate_projects()
    summaries = generate_state_summaries(projects)

    data_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(data_dir, exist_ok=True)

    with open(os.path.join(data_dir, "projects.json"), "w") as f:
        json.dump(projects, f, indent=2)

    with open(os.path.join(data_dir, "state_summaries.json"), "w") as f:
        json.dump(summaries, f, indent=2)

    # Also write a GeoJSON for the frontend
    features = []
    for p in projects:
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [p["longitude"], p["latitude"]],
            },
            "properties": {k: v for k, v in p.items() if k not in ("latitude", "longitude", "workforce")},
            "id": p["id"],
        })
        # flatten workforce into properties
        for wk, wv in p["workforce"].items():
            features[-1]["properties"][wk] = wv

    geojson = {"type": "FeatureCollection", "features": features}
    with open(os.path.join(data_dir, "projects.geojson"), "w") as f:
        json.dump(geojson, f)

    print(f"Generated {len(projects)} projects across {len(summaries)} states")
    print(f"Files written to {data_dir}/")

    # Print some stats
    active = sum(1 for p in projects if p["status"] == "Active")
    phantom = sum(1 for p in projects if p.get("is_phantom"))
    total_mw = sum(p["capacity_mw"] for p in projects)
    print(f"  Active: {active}, Phantom: {phantom}, Total MW: {total_mw:,}")
