import psycopg2
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
CREATE TABLE IF NOT EXISTS weather_point (
    id           SERIAL PRIMARY KEY,
    obs_time     TIMESTAMPTZ,
    lat          DOUBLE PRECISION,
    lon          DOUBLE PRECISION,
    temp_k       REAL,
    rh           REAL,
    wind_u       REAL,
    wind_v       REAL
);
"""
)

conn.commit()
cur.close()
conn.close()

print("wether_point テーブル作成")