import os
from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CA_PATH = os.path.join(BASE_DIR, "ca.pem")

# ==========================================
# 1. LOCAL DATA WAREHOUSE (Destination)
# ==========================================
DW_USER = os.getenv("DW_USER", "root")
DW_PASSWORD = os.getenv("DW_PASSWORD", "")
DW_HOST = os.getenv("DW_HOST", "localhost")
DW_PORT = os.getenv("DW_PORT", "3306")
DW_DB_NAME = os.getenv("DW_NAME", "aviation_dw")

encoded_dw_pass = quote_plus(DW_PASSWORD)
dw_conn_str = f"mysql+pymysql://{DW_USER}:{encoded_dw_pass}@{DW_HOST}:{DW_PORT}/{DW_DB_NAME}"
dw_engine = create_engine(dw_conn_str)

# ==========================================
# 2. AIVEN CLOUD OLTP (Source)
# ==========================================
OLTP_USER = os.getenv("OLTP_USER", "avnadmin")
OLTP_PASSWORD = os.getenv("OLTP_PASSWORD", "")
OLTP_HOST = os.getenv("OLTP_HOST", "")
OLTP_PORT = int(os.getenv("OLTP_PORT", 3306))
OLTP_DB_NAME = os.getenv("OLTP_NAME", "aviation_management_system")

encoded_oltp_pass = quote_plus(OLTP_PASSWORD)
oltp_conn_str = f"mysql+pymysql://{OLTP_USER}:{encoded_oltp_pass}@{OLTP_HOST}:{OLTP_PORT}/{OLTP_DB_NAME}"

oltp_engine = create_engine(
    oltp_conn_str,
    connect_args={"ssl": {"ca": CA_PATH}}
)

# ==========================================
# 3. SHARED ANALYTICS HELPER
# ==========================================
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)

def execute_analytics_query(title: str, query: str):
    print("\n" + "=" * 80)
    print(f" ANALYTICS REPORT: {title}")
    print("=" * 80)
    with dw_engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
        if df.empty:
            print("[!] No records found.")
        else:
            print(df.to_string(index=False))
    print("-" * 80 + "\n")