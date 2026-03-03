import sqlite3, pandas as pd

conn = sqlite3.connect("crypto_data.db")
df = pd.read_sql("SELECT * FROM alt_cycle_data", conn)
df.to_csv("alt_cycle_export.csv", index=False)
print(f"Exported: {len(df)} rows")
conn.close()
