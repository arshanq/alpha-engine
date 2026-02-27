"""
SQLite Database Layer for Infrastructure Alpha Engine.
Manages schema, CRUD operations, and query helpers.
"""
import sqlite3
import json
import os
from datetime import datetime
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "data", "alpha_engine.db"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id TEXT UNIQUE NOT NULL,
    iso TEXT NOT NULL,
    project_name TEXT,
    developer TEXT,
    county TEXT,
    state TEXT,
    poi_name TEXT,
    capacity_mw REAL,
    summer_capacity_mw REAL,
    winter_capacity_mw REAL,
    technology TEXT,
    status TEXT,
    queue_date TEXT,
    proposed_cod TEXT,
    withdrawal_date TEXT,
    actual_cod TEXT,
    latitude REAL,
    longitude REAL,
    success_probability REAL,
    is_phantom INTEGER DEFAULT 0,
    workforce_total INTEGER,
    workforce_electricians INTEGER,
    construction_years INTEGER,
    raw_extra TEXT,
    last_updated TEXT,
    data_source TEXT
);

CREATE TABLE IF NOT EXISTS state_summaries (
    state TEXT PRIMARY KEY,
    total_mw REAL,
    total_gw REAL,
    project_count INTEGER,
    active_count INTEGER,
    operational_mw REAL,
    avg_success REAL,
    median_queue_days INTEGER,
    top_technology TEXT,
    isos TEXT,
    last_computed TEXT
);

CREATE TABLE IF NOT EXISTS county_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state TEXT,
    county TEXT,
    total_mw REAL,
    project_count INTEGER,
    active_count INTEGER,
    avg_success REAL,
    top_technology TEXT,
    workforce_total INTEGER,
    workforce_electricians INTEGER,
    last_computed TEXT,
    UNIQUE(state, county)
);

CREATE TABLE IF NOT EXISTS ingest_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    iso TEXT,
    source TEXT,
    records_ingested INTEGER,
    records_updated INTEGER,
    started_at TEXT,
    completed_at TEXT,
    status TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS success_model (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    technology TEXT,
    iso TEXT,
    mw_bucket TEXT,
    completion_rate REAL,
    sample_size INTEGER,
    UNIQUE(technology, iso, mw_bucket)
);

CREATE INDEX IF NOT EXISTS idx_projects_iso ON projects(iso);
CREATE INDEX IF NOT EXISTS idx_projects_state ON projects(state);
CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status);
CREATE INDEX IF NOT EXISTS idx_projects_technology ON projects(technology);
CREATE INDEX IF NOT EXISTS idx_projects_coords ON projects(latitude, longitude);
"""


def get_db_path():
    """Get the database path, ensuring the directory exists."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return DB_PATH


@contextmanager
def get_connection():
    """Context manager for database connections."""
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create all tables and indexes."""
    with get_connection() as conn:
        conn.executescript(SCHEMA)
    print(f"Database initialized at {get_db_path()}")


def upsert_project(data: dict):
    """Insert or update a project by queue_id."""
    data["last_updated"] = datetime.utcnow().isoformat()

    # Separate known fields from extra
    known_fields = {
        "queue_id", "iso", "project_name", "developer", "county", "state",
        "poi_name", "capacity_mw", "summer_capacity_mw", "winter_capacity_mw",
        "technology", "status", "queue_date", "proposed_cod", "withdrawal_date",
        "actual_cod", "latitude", "longitude", "success_probability", "is_phantom",
        "workforce_total", "workforce_electricians", "construction_years",
        "last_updated", "data_source",
    }

    extra = {k: v for k, v in data.items() if k not in known_fields and v is not None}
    clean = {k: v for k, v in data.items() if k in known_fields}
    if extra:
        clean["raw_extra"] = json.dumps(extra, default=str)

    fields = list(clean.keys())
    placeholders = ", ".join(["?"] * len(fields))
    updates = ", ".join([f"{f}=excluded.{f}" for f in fields if f != "queue_id"])

    sql = f"""
        INSERT INTO projects ({', '.join(fields)})
        VALUES ({placeholders})
        ON CONFLICT(queue_id) DO UPDATE SET {updates}
    """

    with get_connection() as conn:
        conn.execute(sql, [clean.get(f) for f in fields])


def upsert_projects_batch(projects: list[dict]):
    """Batch upsert multiple projects."""
    now = datetime.utcnow().isoformat()

    known_fields = {
        "queue_id", "iso", "project_name", "developer", "county", "state",
        "poi_name", "capacity_mw", "summer_capacity_mw", "winter_capacity_mw",
        "technology", "status", "queue_date", "proposed_cod", "withdrawal_date",
        "actual_cod", "latitude", "longitude", "success_probability", "is_phantom",
        "workforce_total", "workforce_electricians", "construction_years",
        "last_updated", "data_source", "raw_extra",
    }

    rows = []
    for data in projects:
        data["last_updated"] = now
        extra = {k: v for k, v in data.items() if k not in known_fields and v is not None}
        clean = {k: v for k, v in data.items() if k in known_fields}
        if extra:
            clean["raw_extra"] = json.dumps(extra, default=str)
        rows.append(clean)

    if not rows:
        return

    # Use the union of all keys
    all_fields = sorted(set().union(*(r.keys() for r in rows)))
    placeholders = ", ".join(["?"] * len(all_fields))
    updates = ", ".join([f"{f}=excluded.{f}" for f in all_fields if f != "queue_id"])

    sql = f"""
        INSERT INTO projects ({', '.join(all_fields)})
        VALUES ({placeholders})
        ON CONFLICT(queue_id) DO UPDATE SET {updates}
    """

    with get_connection() as conn:
        for row in rows:
            conn.execute(sql, [row.get(f) for f in all_fields])


def get_projects(
    iso=None, state=None, technology=None, status=None,
    hide_phantom=False, limit=None,
):
    """Query projects with optional filters."""
    conditions = []
    params = []

    if iso:
        conditions.append("iso = ?")
        params.append(iso.upper())
    if state:
        conditions.append("state = ?")
        params.append(state.upper())
    if technology:
        conditions.append("technology = ?")
        params.append(technology)
    if status:
        conditions.append("status = ?")
        params.append(status)
    if hide_phantom:
        conditions.append("is_phantom = 0")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    limit_clause = f"LIMIT {limit}" if limit else ""

    sql = f"SELECT * FROM projects {where} ORDER BY capacity_mw DESC {limit_clause}"

    with get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def get_projects_geojson(
    iso=None, state=None, technology=None, status=None,
    hide_phantom=False,
):
    """Build GeoJSON FeatureCollection from projects."""
    projects = get_projects(iso=iso, state=state, technology=technology,
                            status=status, hide_phantom=hide_phantom)

    features = []
    for p in projects:
        lat = p.get("latitude")
        lng = p.get("longitude")
        if lat is None or lng is None:
            continue

        props = {k: v for k, v in p.items()
                 if k not in ("latitude", "longitude", "raw_extra", "id")}

        # Parse queue_days from queue_date
        if p.get("queue_date"):
            try:
                qd = datetime.strptime(p["queue_date"][:10], "%Y-%m-%d")
                props["queue_days"] = (datetime.utcnow() - qd).days
            except (ValueError, TypeError):
                props["queue_days"] = 0
        else:
            props["queue_days"] = 0

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lng, lat]},
            "properties": props,
            "id": p["id"],
        })

    return {"type": "FeatureCollection", "features": features}


def get_state_summaries():
    """Return cached state summaries."""
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM state_summaries ORDER BY total_mw DESC").fetchall()
        result = {}
        for r in rows:
            d = dict(r)
            d["isos"] = json.loads(d.get("isos", "[]")) if d.get("isos") else []
            result[d["state"]] = d
        return result


def save_state_summaries(summaries: dict):
    """Save computed state summaries to DB."""
    with get_connection() as conn:
        for state, data in summaries.items():
            isos_json = json.dumps(data.get("isos", []))
            conn.execute("""
                INSERT INTO state_summaries (state, total_mw, total_gw, project_count,
                    active_count, operational_mw, avg_success, median_queue_days,
                    top_technology, isos, last_computed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(state) DO UPDATE SET
                    total_mw=excluded.total_mw, total_gw=excluded.total_gw,
                    project_count=excluded.project_count, active_count=excluded.active_count,
                    operational_mw=excluded.operational_mw, avg_success=excluded.avg_success,
                    median_queue_days=excluded.median_queue_days,
                    top_technology=excluded.top_technology, isos=excluded.isos,
                    last_computed=excluded.last_computed
            """, (
                state, data.get("total_mw", 0), data.get("total_gw", 0),
                data.get("project_count", 0), data.get("active_count", 0),
                data.get("operational_mw", 0), data.get("avg_success", 0),
                data.get("median_queue_days", 0), data.get("top_technology", ""),
                isos_json, datetime.utcnow().isoformat(),
            ))


def log_ingestion(iso: str, source: str, records_ingested: int,
                  records_updated: int = 0, status: str = "success",
                  error_message: str = None, started_at: str = None):
    """Record an ingestion event."""
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO ingest_log (iso, source, records_ingested, records_updated,
                started_at, completed_at, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            iso, source, records_ingested, records_updated,
            started_at or datetime.utcnow().isoformat(),
            datetime.utcnow().isoformat(),
            status, error_message,
        ))


def get_ingest_status():
    """Get latest ingestion status per ISO."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT iso, source, records_ingested, records_updated,
                   completed_at, status, error_message
            FROM ingest_log
            WHERE id IN (
                SELECT MAX(id) FROM ingest_log GROUP BY iso
            )
            ORDER BY completed_at DESC
        """).fetchall()
        return [dict(r) for r in rows]


def get_project_count():
    """Get total project count."""
    with get_connection() as conn:
        row = conn.execute("SELECT COUNT(*) as cnt FROM projects").fetchone()
        return row["cnt"]


def get_project_count_by_iso():
    """Get project count grouped by ISO."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT iso, COUNT(*) as cnt, SUM(capacity_mw) as total_mw FROM projects GROUP BY iso"
        ).fetchall()
        return {r["iso"]: {"count": r["cnt"], "total_mw": r["total_mw"]} for r in rows}


def save_success_model(model_data: list[dict]):
    """Save success model reference rates."""
    with get_connection() as conn:
        for entry in model_data:
            conn.execute("""
                INSERT INTO success_model (technology, iso, mw_bucket, completion_rate, sample_size)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(technology, iso, mw_bucket) DO UPDATE SET
                    completion_rate=excluded.completion_rate,
                    sample_size=excluded.sample_size
            """, (
                entry["technology"], entry["iso"], entry["mw_bucket"],
                entry["completion_rate"], entry["sample_size"],
            ))


def get_success_model():
    """Load the success model reference rates."""
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM success_model").fetchall()
        return [dict(r) for r in rows]


if __name__ == "__main__":
    init_db()
    print(f"Project count: {get_project_count()}")
