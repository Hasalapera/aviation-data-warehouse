import os
from pathlib import Path

import pymysql
from dotenv import load_dotenv

load_dotenv()

DW_USER = os.getenv("DW_USER", "root")
DW_PASSWORD = os.getenv("DW_PASSWORD", "")
DW_HOST = os.getenv("DW_HOST", "localhost")
DW_PORT = int(os.getenv("DW_PORT", "3306"))
DW_NAME = os.getenv("DW_NAME", "aviation_dw")

BASE_DIR = Path(__file__).resolve().parent
SQL_FILE = BASE_DIR / "sql" / "schema.sql"


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


if __name__ == "__main__":
    setup_database()