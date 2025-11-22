import psycopg2
from datetime import datetime ,timezone
from dotenv import load_dotenv
import os

conn = psycopg2.connect(
	 host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT"),
    dbname=os.getenv("PG_DBNAME"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
)

cur = conn.cursor()

cur.execute("""
    INSERT INTO weather_point (obs_time, lat, lon, temp_k, rh, wind_u, wind_v)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
""", (
    datetime.now(timezone.utc),  # obs_time: 今のUTC
    35.0,                        # lat
    139.0,                       # lon
    293.15,                      # temp_k (約20℃)
    60.0,                        # rh
    -1.2,                        # wind_u
    3.9,                         # wind_v
))

new_id = cur.fetchone()[0]
conn.commit()
cur.close()
conn.close()

print("INSERT した id:", new_id)
