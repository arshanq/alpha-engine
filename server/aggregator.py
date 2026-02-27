"""
Aggregation Module for Infrastructure Alpha Engine.
Computes and caches state/county-level summaries and workforce surge detection.
"""
import sys
import os
import json
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from db import get_connection, save_state_summaries


def aggregate_by_state(verbose: bool = True) -> dict:
    """Compute per-state aggregation and cache in DB."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT state, iso, capacity_mw, status, success_probability,
                   queue_date, technology, workforce_total, workforce_electricians
            FROM projects
            WHERE state IS NOT NULL AND state != ''
        """).fetchall()

    state_data = defaultdict(lambda: {
        "total_mw": 0, "project_count": 0, "active_count": 0,
        "operational_mw": 0, "queue_days_list": [], "success_scores": [],
        "technologies": defaultdict(int), "isos": set(),
    })

    now = datetime.utcnow()

    for r in rows:
        s = state_data[r["state"]]
        mw = r["capacity_mw"] or 0
        s["total_mw"] += mw
        s["project_count"] += 1

        if r["status"] == "Active":
            s["active_count"] += 1
        if r["status"] == "Operational":
            s["operational_mw"] += mw

        if r["queue_date"]:
            try:
                qd = datetime.strptime(r["queue_date"][:10], "%Y-%m-%d")
                s["queue_days_list"].append((now - qd).days)
            except (ValueError, TypeError):
                pass

        if r["success_probability"] is not None:
            s["success_scores"].append(r["success_probability"])

        if r["technology"]:
            s["technologies"][r["technology"]] += 1

        if r["iso"]:
            s["isos"].add(r["iso"])

    summaries = {}
    for state, d in state_data.items():
        sorted_days = sorted(d["queue_days_list"])
        median_days = sorted_days[len(sorted_days) // 2] if sorted_days else 0
        avg_success = (
            sum(d["success_scores"]) / len(d["success_scores"])
            if d["success_scores"] else 0
        )
        top_tech = max(d["technologies"], key=d["technologies"].get) if d["technologies"] else ""

        summaries[state] = {
            "state": state,
            "total_mw": round(d["total_mw"], 1),
            "total_gw": round(d["total_mw"] / 1000, 2),
            "project_count": d["project_count"],
            "active_count": d["active_count"],
            "operational_mw": round(d["operational_mw"], 1),
            "avg_success": round(avg_success, 2),
            "median_queue_days": median_days,
            "top_technology": top_tech,
            "isos": sorted(list(d["isos"])),
        }

    # Save to DB
    save_state_summaries(summaries)

    if verbose:
        print(f"  Aggregated {len(summaries)} states from {sum(d['project_count'] for d in summaries.values())} projects")
        top_5 = sorted(summaries.values(), key=lambda x: x["total_mw"], reverse=True)[:5]
        for s in top_5:
            print(f"    {s['state']}: {s['total_gw']} GW, {s['project_count']} projects, "
                  f"avg success {s['avg_success']:.0%}")

    return summaries


def aggregate_by_county(verbose: bool = True) -> dict:
    """Compute per-county aggregation and cache in DB."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT state, county, capacity_mw, status, success_probability,
                   technology, workforce_total, workforce_electricians
            FROM projects
            WHERE state IS NOT NULL AND county IS NOT NULL
                  AND state != '' AND county != ''
        """).fetchall()

    county_data = defaultdict(lambda: {
        "total_mw": 0, "project_count": 0, "active_count": 0,
        "success_scores": [], "technologies": defaultdict(int),
        "workforce_total": 0, "workforce_electricians": 0,
    })

    for r in rows:
        key = (r["state"], r["county"])
        d = county_data[key]
        mw = r["capacity_mw"] or 0
        d["total_mw"] += mw
        d["project_count"] += 1

        if r["status"] == "Active":
            d["active_count"] += 1
        if r["success_probability"] is not None:
            d["success_scores"].append(r["success_probability"])
        if r["technology"]:
            d["technologies"][r["technology"]] += 1
        if r["status"] == "Active":
            d["workforce_total"] += r["workforce_total"] or 0
            d["workforce_electricians"] += r["workforce_electricians"] or 0

    # Save to DB
    now = datetime.utcnow().isoformat()
    with get_connection() as conn:
        for (state, county), d in county_data.items():
            avg_success = (
                sum(d["success_scores"]) / len(d["success_scores"])
                if d["success_scores"] else 0
            )
            top_tech = max(d["technologies"], key=d["technologies"].get) if d["technologies"] else ""

            conn.execute("""
                INSERT INTO county_summaries (state, county, total_mw, project_count,
                    active_count, avg_success, top_technology,
                    workforce_total, workforce_electricians, last_computed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(state, county) DO UPDATE SET
                    total_mw=excluded.total_mw, project_count=excluded.project_count,
                    active_count=excluded.active_count, avg_success=excluded.avg_success,
                    top_technology=excluded.top_technology,
                    workforce_total=excluded.workforce_total,
                    workforce_electricians=excluded.workforce_electricians,
                    last_computed=excluded.last_computed
            """, (
                state, county, round(d["total_mw"], 1), d["project_count"],
                d["active_count"], round(avg_success, 2), top_tech,
                d["workforce_total"], d["workforce_electricians"], now,
            ))

    if verbose:
        print(f"  Aggregated {len(county_data)} counties")

    return dict(county_data)


def detect_workforce_surges(min_workers: int = 200, verbose: bool = True) -> list[dict]:
    """Identify counties with significant construction labor demand."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT state, county, workforce_total, workforce_electricians,
                   total_mw, project_count, active_count
            FROM county_summaries
            WHERE workforce_total >= ?
            ORDER BY workforce_total DESC
        """, (min_workers,)).fetchall()

    surges = [dict(r) for r in rows]

    if verbose:
        print(f"  Found {len(surges)} workforce surge zones (>{min_workers} workers)")
        for s in surges[:5]:
            print(f"    {s['county']}, {s['state']}: {s['workforce_total']} workers, "
                  f"{s['workforce_electricians']} electricians, {s['total_mw']} MW")

    return surges


def refresh_all_summaries(verbose: bool = True):
    """Recompute all cached aggregations."""
    print("\n--- Aggregation ---")
    state_summaries = aggregate_by_state(verbose=verbose)
    county_summaries = aggregate_by_county(verbose=verbose)
    surges = detect_workforce_surges(verbose=verbose)

    return {
        "states": len(state_summaries),
        "counties": len(county_summaries),
        "surge_zones": len(surges),
    }


if __name__ == "__main__":
    result = refresh_all_summaries(verbose=True)
    print(f"\nResult: {result}")
