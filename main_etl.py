import time
from etl import run_etl as run_csv_etl
from weather_etl import run_weather_etl
from oltp_etl import run_oltp_etl

def run_master_pipeline():
    print("\n" + "#" * 70)
    print(" EXECUTING MASTER ETL: SYNCING 3 HETEROGENEOUS DATA SOURCES")
    print("#" * 70)
    start_time = time.time()

    stages = [
        (
            "[1/3] Ingesting Synthetic CSV Data...",
            run_csv_etl,
            "CSV ETL",
        ),
        (
            "[2/3] Ingesting Live Open-Meteo Weather API Data...",
            run_weather_etl,
            "Weather ETL",
        ),
        (
            "[3/3] Ingesting Aiven Cloud MySQL OLTP Data...",
            run_oltp_etl,
            "OLTP ETL",
        ),
    ]

    failures = []

    for label, stage, name in stages:
        print(f"\n>>> {label}")
        try:
            stage()
        except Exception as e:
            failures.append((name, e))
            print(f"[!] Error in {name}: {e}")

    elapsed = round(time.time() - start_time, 2)

    print("\n" + "#" * 70)

    if failures:
        print(
            f" PIPELINE COMPLETED WITH {len(failures)} FAILED STAGE(S) "
            f"IN {elapsed}s"
        )
        for name, error in failures:
            print(f"   - {name}: {error}")
    else:
        print(
            f" ALL SOURCES SYNCED SUCCESSFULLY TO AVIATION_DW IN {elapsed}s"
        )

    print("#" * 70 + "\n")

    return not failures

if __name__ == "__main__":
    run_master_pipeline()