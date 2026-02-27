import sqlite3
import json

conn = sqlite3.connect("server/data/alpha_engine.db")
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("SELECT iso, queue_id, raw_extra FROM projects LIMIT 5000")
rows = cursor.fetchall()

url_cols = set()
found_urls = 0

for r in rows:
    if not r["raw_extra"]: continue
    extra = json.loads(r["raw_extra"])
    for k, v in extra.items():
        if isinstance(v, str) and "http" in v.lower():
            url_cols.add(k)
            found_urls += 1
            if found_urls < 5:
                print(f"{r['iso']} ({r['queue_id']}): {k} -> {v}")

print(f"\nFound {found_urls} rows with URLs in extra columns.")
print(f"URL columns: {url_cols}")
conn.close()
