import os
from pathlib import Path
from datetime import datetime
import numpy as np
import psycopg2

DB_SETTINGS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "weatherdb"),
    "user": os.getenv("DB_USER", "kesaitou"),
    "password": os.getenv("DB_PASSWORD", "dg1201"),
}

GRIB_DIR = Path("/data/grib")