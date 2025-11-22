import psycopg2
from datetime import datetime ,timezone

conn = psycopg2.connect(
	host="localhost",
	port=5432,
	dbname="weatherdb",
	user="kesaitou",
	password="dg1201",
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
