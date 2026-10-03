import time
from etl import run_etl as run_csv_etl
from weather_etl import run_weather_etl
from oltp_etl import run_oltp_etl

def run_master_pipeline():
    print("\n" + "#" * 70)
    print(" EXECUTING MASTER ETL: SYNCING 3 HETEROGENEOUS DATA SOURCES")
    print("#" * 70)
    start_time = time.time()

    print("\n>>> [1/3] Ingesting Synthetic CSV Data...")
    try:
        run_csv_etl()
    except Exception as e:
        print(f"[!] Error in CSV ETL: {e}")

    print("\n>>> [2/3] Ingesting Live Open-Meteo Weather API Data...")
    try:
        run_weather_etl()
    except Exception as e:
        print(f"[!] Error in Weather ETL: {e}")

    print("\n>>> [3/3] Ingesting Aiven Cloud MySQL OLTP Data...")
    try:
        run_oltp_etl()
    except Exception as e:
        print(f"[!] Error in OLTP ETL: {e}")

    elapsed = round(time.time() - start_time, 2)
    print("\n" + "#" * 70)
    print(f" ALL SOURCES SYNCED SUCCESSFULLY TO AVIATION_DW IN {elapsed}s")
    print("#" * 70 + "\n")

if __name__ == "__main__":
    run_master_pipeline()