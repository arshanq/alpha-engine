import sqlite3
import os
import re

DB_PATH = os.path.join(os.path.dirname(__file__), "server", "data", "alpha_engine.db")

def normalize_county(county: str) -> str:
    if not county:
        return None
    s = str(county).strip()
    clean = re.sub(r'(?i)\s+(CO\.?|COUNTY|PARISH)$', '', s)
    return clean.strip().upper()

def sanitize_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, county FROM projects WHERE county IS NOT NULL")
    rows = cursor.fetchall()
    
    updates = []
    for r in rows:
        clean = normalize_county(r["county"])
        # Only update if the sanitized version is different
        # We explicitly uppercase here to match our backend convention
        if clean != r["county"]:
            updates.append((clean, r["id"]))
            
    if updates:
        cursor.executemany("UPDATE projects SET county = ? WHERE id = ?", updates)
        conn.commit()
    
    conn.close()
    
    print(f"Successfully sanitized {len(updates)} county names in the database.")

if __name__ == "__main__":
    sanitize_db()
