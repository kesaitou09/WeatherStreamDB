import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="weatherdb",
    user="kesaitou",
    password="dg1201",
)

cur = conn.cursor()
cur.execute("SELECT 1;")
print(cur.fetchone())

cur.close()
conn.close()
