import os
from pathlib import Path
from datetime import datetime
import numpy as np
import xarray as xr
import psycopg2


DB_SETTINGS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "weatherdb"),
    "user": os.getenv("DB_USER", "kesaitou"),
    "password": os.getenv("DB_PASSWORD", "dg1201"),
}

GRIB_DIR = Path("/data/grib")  # docker-compose でマウントしたパス


def ensure_table(conn):
    """grib_raw テーブルがなければ作る"""
    create_sql = """
    CREATE TABLE IF NOT EXISTS grib_raw (
        id          BIGSERIAL PRIMARY KEY,
        dataset     TEXT,
        run_time    TIMESTAMPTZ,
        valid_time  TIMESTAMPTZ,
        level_type  TEXT,
        level_value REAL,
        lat         REAL,
        lon         REAL,
        param       TEXT,
        value       REAL
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()


def np_to_datetime(val) -> datetime:
    """numpy.datetime64 → Python datetime に変換"""
    # np.datetime64 -> ns -> datetime
    return val.astype("datetime64[ns]").astype(datetime)


def ingest_one_file(conn, grib_path: Path, dataset_name: str = "SAMPLE"):
    """1つの GRIB ファイルを読み込んで DB に INSERT する"""

    print(f"[INFO] ingesting {grib_path}")

    # cfgrib で GRIB を開く
    ds = xr.open_dataset(grib_path, engine="cfgrib")

    # とりあえず最初の変数1つだけを対象にする（後で増やせる）
    if not ds.data_vars:
        print(f"[WARN] no data_vars in {grib_path.name}, skip")
        return

    var_name = list(ds.data_vars)[0]
    da = ds[var_name]
    print(f"[INFO] using variable: {var_name}, dims={da.dims}")

    # time / latitude / longitude がある前提の簡易版
    if not {"time", "latitude", "longitude"}.issubset(da.coords):
        print(f"[WARN] time/latitude/longitude not all present, skip")
        return

    conn.autocommit = False
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO grib_raw (
            dataset, run_time, valid_time,
            level_type, level_value,
            lat, lon, param, value
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    # ここでは run_time は「ファイル名から推定できないので、とりあえず valid_time の最初」を入れておく
    first_time = da["time"].values[0]
    run_time = np_to_datetime(first_time)

    # とりあえず地上データ想定
    level_type = "surface"
    level_value = 0.0

    rows = []

    # ★最初は「time の最初の1ステップ」だけを取り込む（いきなり全部やるとデータ量がデカいので）
    t0 = da["time"].values[0]
    valid_time = np_to_datetime(t0)

    slice_da = da.sel(time=t0)  # dims: (latitude, longitude)
    lats = slice_da["latitude"].values
    lons = slice_da["longitude"].values
    values = slice_da.values  # 2D array

    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            val = float(values[i, j])
            rows.append(
                (
                    dataset_name,
                    run_time,
                    valid_time,
                    level_type,
                    level_value,
                    float(lat),
                    float(lon),
                    var_name,
                    val,
                )
            )

    # まとめて INSERT
    cur.executemany(insert_sql, rows)
    conn.commit()
    cur.close()

    print(f"[INFO] inserted {len(rows)} rows from {grib_path.name}")


def main():
    # DB 接続
    conn = psycopg2.connect(**DB_SETTINGS)
    ensure_table(conn)

    # /data/grib 配下の .grib / .grib2 を全部なめる
    if not GRIB_DIR.exists():
        print(f"[ERROR] GRIB_DIR {GRIB_DIR} not found")
        return

    grib_files = sorted(
        [p for p in GRIB_DIR.iterdir() if p.suffix in [".grib", ".grib2"]]
    )

    if not grib_files:
        print(f"[WARN] no GRIB files in {GRIB_DIR}")
        return

    for path in grib_files:
        try:
            ingest_one_file(conn, path)
        except Exception as e:
            print(f"[ERROR] failed to ingest {path.name}: {e}")

    conn.close()


if __name__ == "__main__":
    main()
