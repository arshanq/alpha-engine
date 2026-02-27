import sqlite3
import os
import urllib.parse

DB_PATH = os.path.join(os.path.dirname(__file__), "server", "data", "alpha_engine.db")

def generate_project_url(iso_name: str, queue_id_full: str, project_name: str = None, developer: str = None) -> str:
    if not queue_id_full:
        return ""
        
    query = f'"{iso_name.upper()}" "{queue_id_full}"'
    if project_name and len(str(project_name)) > 2:
        query += f' "{project_name}"'
    elif developer and len(str(developer)) > 2:
        query += f' "{developer}"'
        
    encoded_query = urllib.parse.quote_plus(query)
    return f"https://www.google.com/search?q={encoded_query}"

def backfill():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, iso, queue_id, project_name, developer FROM projects")
    rows = cursor.fetchall()
    
    updates = []
    for r in rows:
        url = generate_project_url(r["iso"], r["queue_id"], r["project_name"], r["developer"])
        updates.append((url, r["id"]))
        
    cursor.executemany("UPDATE projects SET project_url = ? WHERE id = ?", updates)
    conn.commit()
    conn.close()
    
    print(f"Successfully backfilled {len(updates)} project URLs as Google Search links.")

if __name__ == "__main__":
    backfill()
