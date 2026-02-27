"""
Pipeline Orchestrator for Infrastructure Alpha Engine.
Single-command runner: Pull → Geocode → Score → Aggregate → Ready.
"""
import sys
import os
import time
import argparse
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from db import init_db, get_project_count, get_project_count_by_iso


def run_pipeline(
    isos: list[str] = None,
    skip_pull: bool = False,
    skip_geocode: bool = False,
    skip_score: bool = False,
    skip_aggregate: bool = False,
    verbose: bool = True,
):
    """
    Run the full data pipeline:
    1. Pull interconnection queues from ISOs
    2. Geocode projects (add lat/lng)
    3. Score projects (success probability, phantom flags, workforce)
    4. Aggregate summaries (state, county, surge zones)
    """
    start_time = time.time()

    print(f"\n{'='*60}")
    print(f"  Infrastructure Alpha Engine — Data Pipeline")
    print(f"  Started: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*60}")

    # Initialize database
    init_db()

    results = {}

    # Step 1: Ingest from gridstatus
    if not skip_pull:
        print(f"\n{'─'*40}")
        print(f"  STEP 1/4: Pulling ISO Queue Data")
        print(f"{'─'*40}")
        from ingest.gridstatus_puller import ingest_all_isos
        pull_results = ingest_all_isos(
            isos=isos or ["CAISO", "NYISO", "PJM", "MISO", "ERCOT", "SPP", "ISONE"],
            verbose=verbose,
        )
        results["pull"] = pull_results
    else:
        print("\n  STEP 1/4: Skipped (--skip-pull)")
        results["pull"] = "skipped"

    # Report DB state
    total = get_project_count()
    by_iso = get_project_count_by_iso()
    print(f"\n  Database: {total} total projects")
    for iso, stats in sorted(by_iso.items()):
        print(f"    {iso}: {stats['count']} projects, {stats['total_mw']:,.0f} MW")

    # Step 2: Geocode
    if not skip_geocode:
        print(f"\n{'─'*40}")
        print(f"  STEP 2/4: Geocoding Projects")
        print(f"{'─'*40}")
        from ingest.geo_enricher import enrich_all_projects
        geo_result = enrich_all_projects(verbose=verbose)
        results["geocode"] = geo_result
    else:
        print("\n  STEP 2/4: Skipped (--skip-geocode)")
        results["geocode"] = "skipped"

    # Step 3: Score
    if not skip_score:
        print(f"\n{'─'*40}")
        print(f"  STEP 3/4: Scoring Projects")
        print(f"{'─'*40}")
        from scoring import score_all_projects
        score_result = score_all_projects(verbose=verbose)
        results["score"] = score_result
    else:
        print("\n  STEP 3/4: Skipped (--skip-score)")
        results["score"] = "skipped"

    # Step 4: Aggregate
    if not skip_aggregate:
        print(f"\n{'─'*40}")
        print(f"  STEP 4/4: Computing Aggregations")
        print(f"{'─'*40}")
        from aggregator import refresh_all_summaries
        agg_result = refresh_all_summaries(verbose=verbose)
        results["aggregate"] = agg_result
    else:
        print("\n  STEP 4/4: Skipped (--skip-aggregate)")
        results["aggregate"] = "skipped"

    elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print(f"  Pipeline Complete!")
    print(f"  Total time: {elapsed:.1f}s")
    print(f"  Database: {get_project_count()} projects")
    print(f"{'='*60}\n")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Alpha Engine data pipeline")
    parser.add_argument("--iso", nargs="+", help="Specific ISOs to pull (e.g., CAISO PJM)")
    parser.add_argument("--skip-pull", action="store_true", help="Skip ISO data pull")
    parser.add_argument("--skip-geocode", action="store_true", help="Skip geocoding")
    parser.add_argument("--skip-score", action="store_true", help="Skip scoring")
    parser.add_argument("--skip-aggregate", action="store_true", help="Skip aggregation")
    parser.add_argument("--quiet", action="store_true", help="Less verbose")
    args = parser.parse_args()

    run_pipeline(
        isos=args.iso,
        skip_pull=args.skip_pull,
        skip_geocode=args.skip_geocode,
        skip_score=args.skip_score,
        skip_aggregate=args.skip_aggregate,
        verbose=not args.quiet,
    )
