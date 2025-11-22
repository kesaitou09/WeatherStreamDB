import os
import psycopg2


def main():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "weatherdb"),
        user=os.getenv("DB_USER", "kesaitou"),
        password=os.getenv("DB_PASSWORD", "dg1201"),
    )
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    print(cur.fetchone())  # => (1,)
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()