"""
FastAPI Backend for Infrastructure Alpha Engine.
Serves interconnection queue data from SQLite database.
Supports both live API data and seed data fallback.
"""
import json
import os
import sys
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.dirname(__file__))

from db import (
    init_db, get_projects, get_projects_geojson,
    get_state_summaries, get_project_count, get_project_count_by_iso,
    get_ingest_status,
)

app = FastAPI(title="Alpha Engine API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def load_json(filename):
    """Load a JSON file from the data directory (fallback for seed data)."""
    filepath = os.path.join(DATA_DIR, filename)
    if os.path.exists(filepath):
        with open(filepath) as f:
            return json.load(f)
    return None


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup."""
    init_db()


@app.get("/api/queue")
def get_queue(
    iso: Optional[str] = Query(None, description="Filter by ISO"),
    state: Optional[str] = Query(None, description="Filter by state"),
    technology: Optional[str] = Query(None, description="Filter by technology"),
    status: Optional[str] = Query(None, description="Filter by status"),
    hide_phantom: bool = Query(False, description="Hide phantom-load projects"),
):
    """Return all interconnection queue projects with optional filters."""
    # Try DB first
    projects = get_projects(
        iso=iso, state=state, technology=technology,
        status=status, hide_phantom=hide_phantom,
    )

    if projects:
        total_mw = sum(p.get("capacity_mw", 0) or 0 for p in projects)
        return {
            "count": len(projects),
            "total_mw": total_mw,
            "source": "database",
            "projects": projects,
        }

    # Fallback to seed data
    seed = load_json("projects.json")
    if seed:
        if iso:
            seed = [p for p in seed if p["iso"] == iso.upper()]
        if state:
            seed = [p for p in seed if p["state"] == state.upper()]
        if technology:
            seed = [p for p in seed if p["technology"].lower() == technology.lower()]
        if status:
            seed = [p for p in seed if p["status"].lower() == status.lower()]
        if hide_phantom:
            seed = [p for p in seed if not p.get("is_phantom", False)]
        return {
            "count": len(seed),
            "total_mw": sum(p["capacity_mw"] for p in seed),
            "source": "seed",
            "projects": seed,
        }

    return {"count": 0, "total_mw": 0, "source": "empty", "projects": []}


@app.get("/api/queue/geojson")
def get_queue_geojson(
    iso: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    technology: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    hide_phantom: bool = Query(False),
):
    """Return all projects as GeoJSON for map rendering."""
    # Try DB first
    geojson = get_projects_geojson(
        iso=iso, state=state, technology=technology,
        status=status, hide_phantom=hide_phantom,
    )

    if geojson["features"]:
        return geojson

    # Fallback to seed GeoJSON
    seed_geojson = load_json("projects.geojson")
    if seed_geojson:
        return seed_geojson

    return {"type": "FeatureCollection", "features": []}


@app.get("/api/queue/summary")
def get_queue_summary():
    """Return aggregated stats by state."""
    # Try DB first
    summaries = get_state_summaries()
    if summaries:
        return summaries

    # Fallback to seed data
    seed = load_json("state_summaries.json")
    if seed:
        return seed

    return {}


@app.get("/api/ingest/status")
def ingest_status():
    """Show ingestion status — last pull time per ISO, DB stats."""
    return {
        "total_projects": get_project_count(),
        "by_iso": get_project_count_by_iso(),
        "last_ingestions": get_ingest_status(),
    }


@app.post("/api/ingest/pull/{iso}")
async def pull_single_iso(iso: str, background_tasks: BackgroundTasks):
    """Trigger a live pull for a specific ISO (runs in background)."""
    def do_pull():
        try:
            from ingest.gridstatus_puller import ingest_iso
            ingest_iso(iso.upper(), verbose=True)
            # Re-score and re-aggregate after pull
            from scoring import score_all_projects
            from aggregator import refresh_all_summaries
            score_all_projects(verbose=False)
            refresh_all_summaries(verbose=False)
        except Exception as e:
            print(f"Background pull failed for {iso}: {e}")

    background_tasks.add_task(do_pull)
    return {"status": "started", "iso": iso.upper(), "message": "Pull started in background"}


@app.post("/api/ingest/pull-all")
async def pull_all_isos(background_tasks: BackgroundTasks):
    """Trigger a full refresh of all ISOs (runs in background)."""
    def do_pull_all():
        try:
            from run_pipeline import run_pipeline
            run_pipeline(verbose=True)
        except Exception as e:
            print(f"Background full pull failed: {e}")

    background_tasks.add_task(do_pull_all)
    return {"status": "started", "message": "Full pipeline started in background"}


@app.get("/api/health")
def health():
    """Health check."""
    return {
        "status": "ok",
        "version": "2.0.0",
        "database": get_project_count(),
        "timestamp": datetime.utcnow().isoformat(),
    }
