import os
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymysql
from dotenv import load_dotenv
from sqlalchemy import bindparam, text

from database import dw_engine, oltp_engine

load_dotenv()

DW_USER = os.getenv("DW_USER", "root")
DW_PASSWORD = os.getenv("DW_PASSWORD", "")
DW_HOST = os.getenv("DW_HOST", "localhost")
DW_PORT = int(os.getenv("DW_PORT", "3306"))
DW_NAME = os.getenv("DW_NAME", "aviation_dw")

BASE_DIR = Path(__file__).resolve().parent
SQL_FILE = BASE_DIR / "sql" / "schema.sql"
CSV_FILE = BASE_DIR / "data" / "flight.csv"


def setup_database():
    print("\n" + "=" * 70)
    print(" AVIATION DATA WAREHOUSE - DATABASE SETUP")
    print("=" * 70)

    if not SQL_FILE.exists():
        raise FileNotFoundError(
            f"SQL file not found: {SQL_FILE}"
        )

    print("\n1. Connecting to MySQL...")

    connection = pymysql.connect(
        host=DW_HOST,
        port=DW_PORT,
        user=DW_USER,
        password=DW_PASSWORD,
        autocommit=True,
        client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS
    )

    print(" -> MySQL connection successful.")

    try:
        print("\n2. Reading schema.sql...")

        with open(SQL_FILE, "r", encoding="utf-8") as file:
            sql_script = file.read()

        print(" -> SQL script loaded.")

        print("\n3. Creating database and tables...")

        with connection.cursor() as cursor:
            cursor.execute(sql_script)

            while cursor.nextset():
                pass

        print(f" -> Database '{DW_NAME}' and tables are ready.")

    finally:
        connection.close()

    print("\n" + "=" * 70)
    print(" DATABASE SETUP COMPLETED SUCCESSFULLY")
    print("=" * 70 + "\n")


# =========================================================
# SCHEMA MIGRATION (additive only, idempotent, explicit)
# =========================================================

def _connect():
    return pymysql.connect(
        host=DW_HOST,
        port=DW_PORT,
        user=DW_USER,
        password=DW_PASSWORD,
        database=DW_NAME,
        autocommit=True,
    )


def _column_exists(conn, table, column):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            """,
            (DW_NAME, table, column),
        )
        return cur.fetchone()[0] > 0


def _constraint_exists(conn, table, name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.table_constraints
            WHERE table_schema = %s
              AND table_name = %s
              AND constraint_name = %s
            """,
            (DW_NAME, table, name),
        )
        return cur.fetchone()[0] > 0


def _index_exists(conn, table, name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.statistics
            WHERE table_schema = %s
              AND table_name = %s
              AND index_name = %s
            """,
            (DW_NAME, table, name),
        )
        return cur.fetchone()[0] > 0


def migrate_schema():
    print("\n" + "=" * 70)
    print(" SCHEMA MIGRATION (additive, idempotent)")
    print("=" * 70)

    conn = _connect()
    try:
        adds = []

        if not _column_exists(conn, "fact_flight", "source_system"):
            adds.append(
                "ADD COLUMN source_system VARCHAR(20) NULL AFTER flight_key"
            )

        if not _column_exists(conn, "fact_flight", "source_flight_id"):
            adds.append(
                "ADD COLUMN source_flight_id VARCHAR(50) NULL AFTER source_system"
            )

        if not _column_exists(conn, "fact_flight", "scheduled_departure"):
            adds.append(
                "ADD COLUMN scheduled_departure DATETIME NULL AFTER flight_number"
            )

        if adds:
            with conn.cursor() as cur:
                cur.execute(
                    "ALTER TABLE fact_flight " + ", ".join(adds)
                )
            print(f" -> Added columns: {len(adds)}")
        else:
            print(" -> Columns already present.")

        if not _constraint_exists(conn, "fact_flight", "uq_fact_source"):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    ALTER TABLE fact_flight
                    ADD CONSTRAINT uq_fact_source
                    UNIQUE (source_system, source_flight_id)
                    """
                )
            print(" -> Added unique constraint uq_fact_source.")
        else:
            print(" -> Unique constraint already present.")

        if not _index_exists(conn, "fact_flight", "idx_fact_scheduled_departure"):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    ALTER TABLE fact_flight
                    ADD INDEX idx_fact_scheduled_departure (scheduled_departure)
                    """
                )
            print(" -> Added index idx_fact_scheduled_departure.")
        else:
            print(" -> Index already present.")

    finally:
        conn.close()

    print(" SCHEMA MIGRATION COMPLETE")
    print("=" * 70 + "\n")


def migrate_weather_schema():
    """Additive, idempotent migration for historical weather identity.

    Existing dim_weather rows (CSV-derived and legacy) keep their surrogate
    ``weather_key`` and receive NULL for every new column, so the unique key
    and foreign key never conflict with legacy data.
    """

    print("\n" + "=" * 70)
    print(" WEATHER SCHEMA MIGRATION (additive, idempotent)")
    print("=" * 70)

    conn = _connect()
    try:
        adds = []

        weather_columns = [
            ("airport_key", "ADD COLUMN airport_key INT NULL"),
            ("observation_time", "ADD COLUMN observation_time DATETIME NULL"),
            ("relative_humidity", "ADD COLUMN relative_humidity DECIMAL(5,2) NULL"),
            ("cloud_cover", "ADD COLUMN cloud_cover DECIMAL(5,2) NULL"),
            ("weather_code", "ADD COLUMN weather_code SMALLINT NULL"),
            ("visibility_source", "ADD COLUMN visibility_source VARCHAR(50) NULL"),
            ("source_dataset", "ADD COLUMN source_dataset VARCHAR(50) NULL"),
            ("source_model", "ADD COLUMN source_model VARCHAR(50) NULL"),
            ("source_timezone", "ADD COLUMN source_timezone VARCHAR(50) NULL"),
        ]

        for column, ddl in weather_columns:
            if not _column_exists(conn, "dim_weather", column):
                adds.append(ddl)

        if adds:
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE dim_weather " + ", ".join(adds))
            print(f" -> Added columns: {len(adds)}")
        else:
            print(" -> Columns already present.")

        if not _index_exists(
            conn, "dim_weather", "idx_weather_airport_time"
        ):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    ALTER TABLE dim_weather
                    ADD INDEX idx_weather_airport_time
                    (airport_key, observation_time)
                    """
                )
            print(" -> Added index idx_weather_airport_time.")
        else:
            print(" -> Index already present.")

        if not _constraint_exists(
            conn, "dim_weather", "uq_weather_airport_time"
        ):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    ALTER TABLE dim_weather
                    ADD CONSTRAINT uq_weather_airport_time
                    UNIQUE (
                        airport_key, observation_time,
                        source_dataset, source_model
                    )
                    """
                )
            print(" -> Added unique constraint uq_weather_airport_time.")
        else:
            print(" -> Unique constraint already present.")

        if not _constraint_exists(
            conn, "dim_weather", "fk_weather_airport"
        ):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    ALTER TABLE dim_weather
                    ADD CONSTRAINT fk_weather_airport
                    FOREIGN KEY (airport_key)
                    REFERENCES dim_airport (airport_key)
                    """
                )
            print(" -> Added foreign key fk_weather_airport.")
        else:
            print(" -> Foreign key already present.")

    finally:
        conn.close()

    print(" WEATHER SCHEMA MIGRATION COMPLETE")
    print("=" * 70 + "\n")


# =========================================================
# BACKUPS (external dump + in-database snapshot)
# =========================================================

def _latest_backup_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name LIKE 'fact_flight_backup_%%'
            ORDER BY table_name DESC
            LIMIT 1
            """,
            (DW_NAME,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def create_backups():
    print("\n" + "=" * 70)
    print(" CREATING BACKUPS (external dump + in-database snapshot)")
    print("=" * 70)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dump_path = os.path.join(
        tempfile.gettempdir(), f"aviation_dw_backup_{ts}.sql"
    )

    env = os.environ.copy()
    if DW_PASSWORD:
        env["MYSQL_PWD"] = DW_PASSWORD

    cmd = [
        "mysqldump",
        "--single-transaction",
        "--routines",
        "--triggers",
        "--set-gtid-purged=OFF",
        "-h", DW_HOST,
        "-P", str(DW_PORT),
        "-u", DW_USER,
        DW_NAME,
    ]

    with open(dump_path, "wb") as handle:
        subprocess.run(cmd, stdout=handle, env=env, check=True)

    size = os.path.getsize(dump_path)
    if size == 0:
        raise RuntimeError("mysqldump produced an empty file.")

    conn = _connect()
    snapshots = {}
    try:
        for table in ("fact_flight", "dim_weather"):
            snapshot = f"{table}_backup_{ts}"
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {snapshot}")
                cur.execute(f"CREATE TABLE {snapshot} LIKE {table}")
                cur.execute(
                    f"INSERT INTO {snapshot} SELECT * FROM {table}"
                )
                cur.execute(f"SELECT COUNT(*) FROM {snapshot}")
                snap_count = cur.fetchone()[0]
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                source_count = cur.fetchone()[0]
            if snap_count != source_count:
                raise RuntimeError(
                    f"Snapshot row mismatch for {table}: "
                    f"{snap_count} != {source_count}"
                )
            snapshots[table] = snapshot
    finally:
        conn.close()

    print(f" -> External dump : {dump_path} ({size} bytes)")
    for table, snapshot in snapshots.items():
        print(f" -> Snapshot table: {snapshot}")
    print(" BACKUP COMPLETE")
    print("=" * 70 + "\n")

    return dump_path, snapshots


# =========================================================
# LEGACY RECONCILIATION + REPAIR (explicit, destructive)
# =========================================================

def _load_reconciliation():
    from etl import _hhmm_to_minutes

    df = pd.read_csv(CSV_FILE)
    df["date_key"] = (
        pd.to_datetime(df["flight_date"], errors="coerce")
        .dt.strftime("%Y%m%d")
        .astype("Int64")
    )

    base_date = pd.to_datetime(df["flight_date"], errors="coerce").dt.normalize()
    dep_min = _hhmm_to_minutes(df["departure_time"])
    arr_min = _hhmm_to_minutes(df["arrival_time"])
    overnight = (
        (arr_min < dep_min) & arr_min.notna() & dep_min.notna()
    )

    df["scheduled_departure_ts"] = base_date + pd.to_timedelta(
        _hhmm_to_minutes(df["scheduled_departure"]), unit="m"
    )
    df["departure_time_ts"] = base_date + pd.to_timedelta(dep_min, unit="m")
    df["arrival_time_ts"] = base_date + pd.to_timedelta(
        arr_min + overnight.astype("float64") * 1440, unit="m"
    )

    with dw_engine.connect() as conn:
        conn.execute(text("SET SESSION TRANSACTION READ ONLY"))
        try:
            airports = pd.read_sql(
                text("SELECT airport_key, airport_code FROM dim_airport"), conn
            )
            routes = pd.read_sql(
                text(
                    """
                    SELECT route_key, origin_airport_key, destination_airport_key
                    FROM dim_route
                    """
                ),
                conn,
            )
            fact_csv = pd.read_sql(
                text(
                    """
                    SELECT flight_key, date_key, airline_key, route_key, weather_key,
                           flight_number, scheduled_departure, departure_time,
                           arrival_time, delay_minutes, arrival_delay_minutes,
                           flight_duration_minutes, distance_km, passenger_count,
                           cancelled_flag, diverted_flag, source_system,
                           source_flight_id
                    FROM fact_flight
                    WHERE weather_key IS NOT NULL
                    """
                ),
                conn,
            )
            fact_oltp = pd.read_sql(
                text(
                    """
                    SELECT flight_key, flight_number, date_key, route_key,
                           scheduled_departure, source_system, source_flight_id
                    FROM fact_flight
                    WHERE weather_key IS NULL
                    """
                ),
                conn,
            )
        finally:
            conn.execute(text("SET SESSION TRANSACTION READ WRITE"))

    airport_map = dict(zip(airports["airport_code"], airports["airport_key"]))
    route_map = {
        (r.origin_airport_key, r.destination_airport_key): r.route_key
        for r in routes.itertuples()
    }

    df["origin_key"] = df["origin_airport"].map(airport_map)
    df["destination_key"] = df["destination_airport"].map(airport_map)
    df["route_key"] = [
        route_map.get((o, d))
        for o, d in zip(df["origin_key"], df["destination_key"])
    ]
    df["nk"] = (
        df["flight_number"].astype(str)
        + "|" + df["date_key"].astype(str)
        + "|" + df["route_key"].astype(str)
    )

    fact_csv["nk"] = (
        fact_csv["flight_number"].astype(str)
        + "|" + fact_csv["date_key"].astype(str)
        + "|" + fact_csv["route_key"].astype(str)
    )

    return df, fact_csv, fact_oltp


def _none_if_nat(value):
    return None if value is None or pd.isna(value) else value


def _business_signature(frame):
    cols = [
        "date_key", "airline_key", "route_key", "weather_key",
        "flight_number", "departure_time", "arrival_time",
        "delay_minutes", "arrival_delay_minutes",
        "flight_duration_minutes", "distance_km", "passenger_count",
        "cancelled_flag", "diverted_flag",
    ]
    return (
        frame[cols]
        .astype(object)
        .where(frame[cols].notna(), "\x00")
        .astype(str)
        .agg("|".join, axis=1)
    )


def repair_legacy_facts(confirm=False):
    print("\n" + "=" * 70)
    print(" LEGACY FACT RECONCILIATION + REPAIR")
    print("=" * 70)

    if not confirm:
        print(" [!] Destructive repair requires --confirm. Aborting.")
        return False

    conn = _connect()
    try:
        snapshot = _latest_backup_table(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_flight")
            fact_count = cur.fetchone()[0]
            if snapshot:
                cur.execute(f"SELECT COUNT(*) FROM {snapshot}")
                snap_count = cur.fetchone()[0]
            else:
                snap_count = None
    finally:
        conn.close()

    if not snapshot:
        print(" [!] No backup snapshot table found. Run backup first. Aborting.")
        return False

    if snap_count != fact_count:
        print(
            f" [!] Backup snapshot ({snap_count}) does not match fact_flight "
            f"({fact_count}). Aborting."
        )
        return False

    print(f" -> Backup verified: {snapshot} ({snap_count} rows).")

    with dw_engine.connect() as probe:
        probe.execute(text("SET SESSION TRANSACTION READ ONLY"))
        try:
            already = probe.execute(
                text(
                    "SELECT COUNT(*) FROM fact_flight "
                    "WHERE source_flight_id IS NOT NULL"
                )
            ).scalar()
        finally:
            probe.execute(text("SET SESSION TRANSACTION READ WRITE"))

    if already:
        print(
            f" [!] {already} rows already carry source identity; "
            "repair appears complete. Aborting to avoid double work."
        )
        return False

    df, fact_csv, fact_oltp = _load_reconciliation()

    print(
        f" -> Source CSV rows: {len(df)} | "
        f"CSV warehouse rows: {len(fact_csv)} | "
        f"OLTP warehouse rows: {len(fact_oltp)}"
    )

    if df["nk"].duplicated().any():
        print(" [!] Source natural keys are not unique. Aborting.")
        return False

    csv_groups = fact_csv.groupby("nk")
    sizes = csv_groups.size()
    if sizes.max() > 2:
        print(" [!] A CSV match group has more than 2 rows. Aborting.")
        return False

    signatures = fact_csv.assign(sig=_business_signature(fact_csv))
    for nk, group in signatures.groupby("nk"):
        if group["sig"].nunique() > 1:
            print(
                f" [!] Non-identical duplicates detected for {nk}. Aborting."
            )
            return False

    csv_keepers = []
    csv_redundant = []
    for nk, group in fact_csv.groupby("nk"):
        ordered = group.sort_values("flight_key")["flight_key"].tolist()
        csv_keepers.append(ordered[0])
        csv_redundant.extend(ordered[1:])

    source_by_nk = (
        df.set_index("nk")[["flight_id", "scheduled_departure_ts",
                            "departure_time_ts", "arrival_time_ts"]]
    )

    if len(csv_keepers) != len(df):
        print(
            f" [!] Keeper count ({len(csv_keepers)}) != source rows "
            f"({len(df)}). Aborting."
        )
        return False

    keeper_rows = []
    unused = set(csv_keepers)
    for nk, group in fact_csv.groupby("nk"):
        want = source_by_nk.loc[nk]
        keeper = group.sort_values("flight_key")["flight_key"].iloc[0]
        keeper_rows.append(
            {
                "flight_key": int(keeper),
                "source_flight_id": str(int(want["flight_id"])),
                "scheduled_departure": _none_if_nat(
                    want["scheduled_departure_ts"]
                ),
                "departure_time": _none_if_nat(want["departure_time_ts"]),
                "arrival_time": _none_if_nat(want["arrival_time_ts"]),
            }
        )
        unused.discard(int(keeper))

    assigned_sources = [row["source_flight_id"] for row in keeper_rows]
    if len(set(assigned_sources)) != len(assigned_sources):
        print(" [!] A source flight_id was assigned to multiple keepers. Aborting.")
        return False

    with oltp_engine.connect() as oltp_conn:
        oltp_conn.execute(text("SET SESSION TRANSACTION READ ONLY"))
        try:
            src_flights = pd.read_sql(
                text(
                    """
                    SELECT flight_id, flight_number, scheduled_departure
                    FROM flights
                    """
                ),
                oltp_conn,
            )
        finally:
            oltp_conn.execute(text("SET SESSION TRANSACTION READ WRITE"))

    if src_flights["flight_number"].duplicated().any():
        print(
            " [!] OLTP source flight_number is not unique; cannot map "
            "safely. Aborting."
        )
        return False

    oltp_keepers = []
    oltp_redundant = []
    for _, flight in src_flights.iterrows():
        group = fact_oltp[
            fact_oltp["flight_number"].astype(str)
            == str(flight["flight_number"]).strip()
        ]
        if group.empty:
            print(
                f" [!] OLTP flight_number {flight['flight_number']} missing "
                "in warehouse. Aborting."
            )
            return False
        ordered = group.sort_values("flight_key")["flight_key"].tolist()
        oltp_keepers.append(
            {
                "flight_key": int(ordered[0]),
                "source_flight_id": str(int(flight["flight_id"])),
                "scheduled_departure": _none_if_nat(
                    flight["scheduled_departure"]
                ),
            }
        )
        oltp_redundant.extend(ordered[1:])

    print(
        f" -> CSV keepers: {len(keeper_rows)} | "
        f"CSV redundant: {len(csv_redundant)} | "
        f"OLTP keepers: {len(oltp_keepers)} | "
        f"OLTP redundant: {len(oltp_redundant)}"
    )

    deleted_keys = [int(k) for k in csv_redundant] + [
        int(k) for k in oltp_redundant
    ]

    try:
        with dw_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE fact_flight
                    SET source_system = 'csv',
                        source_flight_id = :source_flight_id,
                        scheduled_departure = :scheduled_departure,
                        departure_time = :departure_time,
                        arrival_time = :arrival_time
                    WHERE flight_key = :flight_key
                    """
                ),
                keeper_rows,
            )

            conn.execute(
                text(
                    """
                    UPDATE fact_flight
                    SET source_system = 'oltp',
                        source_flight_id = :source_flight_id,
                        scheduled_departure = :scheduled_departure
                    WHERE flight_key = :flight_key
                    """
                ),
                oltp_keepers,
            )

            if deleted_keys:
                conn.execute(
                    text(
                        "DELETE FROM fact_flight WHERE flight_key IN :keys"
                    ).bindparams(bindparam("keys", expanding=True)),
                    {"keys": deleted_keys},
                )

            csv_count = conn.execute(
                text("SELECT COUNT(*) FROM fact_flight WHERE weather_key IS NOT NULL")
            ).scalar()
            oltp_count = conn.execute(
                text("SELECT COUNT(*) FROM fact_flight WHERE weather_key IS NULL")
            ).scalar()
            null_identity = conn.execute(
                text("SELECT COUNT(*) FROM fact_flight WHERE source_flight_id IS NULL")
            ).scalar()
            duplicates = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT source_system, source_flight_id
                        FROM fact_flight
                        GROUP BY source_system, source_flight_id
                        HAVING COUNT(*) > 1
                    ) t
                    """
                )
            ).scalar()

            if csv_count != 500:
                raise RuntimeError(f"CSV count assertion failed: {csv_count}")
            if oltp_count != len(src_flights):
                raise RuntimeError(f"OLTP count assertion failed: {oltp_count}")
            if null_identity != 0:
                raise RuntimeError(
                    f"Unresolved identity assertion failed: {null_identity}"
                )
            if duplicates != 0:
                raise RuntimeError(
                    f"Duplicate source identity assertion failed: {duplicates}"
                )

            print(
                f" -> Post-repair: CSV={csv_count}, OLTP={oltp_count}, "
                f"unresolved={null_identity}, duplicate identities={duplicates}"
            )
    except Exception as error:
        print(f" [!] Repair failed and was rolled back: {error}")
        return False

    print(" LEGACY REPAIR COMPLETE")
    print("=" * 70 + "\n")
    return True


if __name__ == "__main__":

    mode = sys.argv[1] if len(sys.argv) > 1 else "setup"

    if mode == "migrate":
        migrate_schema()
    elif mode == "migrate-weather":
        migrate_weather_schema()
    elif mode == "backup":
        create_backups()
    elif mode == "repair":
        repair_legacy_facts(confirm="--confirm" in sys.argv)
    else:
        setup_database()
