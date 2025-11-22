import psycopg2

conn = psycopg2.connect(
	host="localhost",
	port=5432,
	dbname="weatherdb",
	user="kesaitou",
	password="dg1201",
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