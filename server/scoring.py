"""
Scoring Engine for Infrastructure Alpha Engine.
Computes success probabilities, phantom-load flags, and workforce estimates.
"""
import sys
import os
import math
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from db import get_connection, get_success_model, save_success_model


# Default base rates by technology (from LBNL "Queued Up" 2024 report patterns)
DEFAULT_BASE_RATES = {
    "Solar": 0.14,
    "Wind": 0.12,
    "Battery Storage": 0.20,
    "Natural Gas": 0.25,
    "Nuclear": 0.05,
    "Hybrid": 0.18,
    "Hydro": 0.15,
    "Coal": 0.08,
    "Other": 0.10,
}

# MW-based adjustments
MW_ADJUSTMENTS = {
    (0, 50): 0.06,       # Small projects complete more often
    (50, 200): 0.03,
    (200, 500): 0.00,
    (500, 1000): -0.04,
    (1000, float("inf")): -0.08,  # Very large projects withdraw more
}

# Queue age adjustments (days)
AGE_ADJUSTMENTS = {
    (0, 365): 0.04,          # Fresh in queue
    (365, 730): 0.02,        # 1-2 years
    (730, 1095): 0.00,       # 2-3 years
    (1095, 1825): -0.05,     # 3-5 years — stale
    (1825, float("inf")): -0.10,  # 5+ years — very stale
}

# Workers per MW by technology (DOE construction labor estimates)
WORKERS_PER_MW = {
    "Solar": 0.8,
    "Wind": 1.2,
    "Battery Storage": 0.4,
    "Natural Gas": 1.5,
    "Hybrid": 0.9,
    "Nuclear": 3.0,
    "Hydro": 2.0,
    "Coal": 1.8,
    "Other": 0.6,
}


def get_mw_bucket(mw: float) -> str:
    """Get MW bucket label for model lookup."""
    if mw < 50:
        return "<50"
    elif mw < 200:
        return "50-200"
    elif mw < 500:
        return "200-500"
    elif mw < 1000:
        return "500-1000"
    else:
        return "1000+"


def build_success_model_from_data(verbose: bool = True) -> list[dict]:
    """
    Build success probability model from historical data in the DB.
    Uses completion/withdrawal ratios grouped by technology × ISO × MW bucket.
    """
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT technology, iso, capacity_mw, status
            FROM projects
            WHERE status IN ('Operational', 'Withdrawn')
        """).fetchall()

    if not rows:
        if verbose:
            print("  No historical data available. Using default base rates.")
        return []

    # Group by segment
    segments = {}
    for r in rows:
        tech = r["technology"] or "Other"
        iso = r["iso"]
        mw_bucket = get_mw_bucket(r["capacity_mw"] or 0)
        key = (tech, iso, mw_bucket)

        if key not in segments:
            segments[key] = {"completed": 0, "withdrawn": 0, "total": 0}

        segments[key]["total"] += 1
        if r["status"] == "Operational":
            segments[key]["completed"] += 1
        elif r["status"] == "Withdrawn":
            segments[key]["withdrawn"] += 1

    model_data = []
    for (tech, iso, mw_bucket), counts in segments.items():
        total = counts["total"]
        if total >= 3:  # Minimum sample size
            rate = counts["completed"] / total
            model_data.append({
                "technology": tech,
                "iso": iso,
                "mw_bucket": mw_bucket,
                "completion_rate": round(rate, 3),
                "sample_size": total,
            })

    if verbose and model_data:
        print(f"  Built success model with {len(model_data)} segments from {len(rows)} historical records")

    # Save to DB
    if model_data:
        save_success_model(model_data)

    return model_data


def compute_success_probability(
    technology: str,
    iso: str,
    capacity_mw: float,
    status: str,
    queue_date: str = None,
    developer: str = None,
    model: dict = None,
) -> float:
    """
    Compute success probability for a single project.
    Uses segment model + adjustments for queue age and project characteristics.
    """
    # Short-circuit for known outcomes
    if status == "Operational":
        return 1.0
    if status == "Withdrawn":
        return 0.0
    if status == "Suspended":
        # Suspended projects have reduced base rates
        pass

    # Step 1: Get base rate from model or defaults
    mw_bucket = get_mw_bucket(capacity_mw or 0)

    base_rate = DEFAULT_BASE_RATES.get(technology, 0.10)

    if model:
        # Try exact segment match
        key = (technology, iso, mw_bucket)
        if key in model:
            base_rate = model[key]
        else:
            # Try tech+ISO match (any MW bucket)
            for (t, i, m), rate in model.items():
                if t == technology and i == iso:
                    base_rate = rate
                    break

    score = base_rate

    # Step 2: MW adjustment
    for (lo, hi), adj in MW_ADJUSTMENTS.items():
        if lo <= (capacity_mw or 0) < hi:
            score += adj
            break

    # Step 3: Queue age adjustment
    if queue_date:
        try:
            qd = datetime.strptime(queue_date[:10], "%Y-%m-%d")
            queue_days = (datetime.utcnow() - qd).days
            for (lo, hi), adj in AGE_ADJUSTMENTS.items():
                if lo <= queue_days < hi:
                    score += adj
                    break
        except (ValueError, TypeError):
            pass

    # Step 4: Suspended penalty
    if status == "Suspended":
        score *= 0.3

    # Clamp
    return round(max(0.01, min(0.98, score)), 2)


def flag_phantom(
    status: str,
    success_probability: float,
    queue_date: str = None,
    capacity_mw: float = 0,
) -> bool:
    """
    Flag a project as phantom load.
    Phantom = Active + low success + stale queue age.
    """
    if status != "Active":
        return False
    if success_probability >= 0.20:
        return False

    # Must be stale (>2 years in queue)
    if queue_date:
        try:
            qd = datetime.strptime(queue_date[:10], "%Y-%m-%d")
            queue_days = (datetime.utcnow() - qd).days
            if queue_days < 730:
                return False
        except (ValueError, TypeError):
            pass

    return True


def compute_workforce(technology: str, capacity_mw: float) -> dict:
    """Estimate construction labor demand."""
    base = WORKERS_PER_MW.get(technology, 0.8)
    # Add some natural variation
    import random
    variation = random.uniform(0.85, 1.15)

    total_workers = max(10, int(capacity_mw * base * variation))
    electricians = max(5, int(total_workers * random.uniform(0.25, 0.40)))
    duration_years = max(1, min(8, int(capacity_mw / 200 * random.uniform(0.8, 1.4))))

    return {
        "workforce_total": total_workers,
        "workforce_electricians": electricians,
        "construction_years": duration_years,
    }


def score_all_projects(verbose: bool = True):
    """Batch-score all projects in the database."""
    # First, try to build model from historical data
    model_data = build_success_model_from_data(verbose=verbose)

    # Convert model to lookup dict
    model_lookup = {}
    for entry in model_data:
        key = (entry["technology"], entry["iso"], entry["mw_bucket"])
        model_lookup[key] = entry["completion_rate"]

    # Also load from DB if previously persisted
    if not model_lookup:
        db_model = get_success_model()
        for entry in db_model:
            key = (entry["technology"], entry["iso"], entry["mw_bucket"])
            model_lookup[key] = entry["completion_rate"]

    if verbose:
        print(f"  Success model has {len(model_lookup)} segments")

    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, technology, iso, capacity_mw, status, queue_date, developer FROM projects"
        ).fetchall()

        if verbose:
            print(f"  Scoring {len(rows)} projects...")

        import random
        random.seed(42)  # Reproducible workforce estimates

        scored = 0
        phantom_count = 0
        for row in rows:
            prob = compute_success_probability(
                technology=row["technology"] or "Other",
                iso=row["iso"],
                capacity_mw=row["capacity_mw"] or 0,
                status=row["status"] or "Active",
                queue_date=row["queue_date"],
                developer=row["developer"],
                model=model_lookup,
            )

            is_phantom = flag_phantom(
                status=row["status"] or "Active",
                success_probability=prob,
                queue_date=row["queue_date"],
                capacity_mw=row["capacity_mw"] or 0,
            )

            workforce = compute_workforce(
                technology=row["technology"] or "Other",
                capacity_mw=row["capacity_mw"] or 0,
            )

            conn.execute("""
                UPDATE projects SET
                    success_probability = ?,
                    is_phantom = ?,
                    workforce_total = ?,
                    workforce_electricians = ?,
                    construction_years = ?
                WHERE id = ?
            """, (
                prob, int(is_phantom),
                workforce["workforce_total"],
                workforce["workforce_electricians"],
                workforce["construction_years"],
                row["id"],
            ))

            scored += 1
            if is_phantom:
                phantom_count += 1

        if verbose:
            print(f"  Scored {scored} projects, {phantom_count} flagged as phantom")

    return {"scored": scored, "phantom": phantom_count}


if __name__ == "__main__":
    result = score_all_projects(verbose=True)
    print(f"\nResult: {result}")
