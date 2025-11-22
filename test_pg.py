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
cur.execute("SELECT 1;")
print(cur.fetchone())

cur.close()
conn.close()
