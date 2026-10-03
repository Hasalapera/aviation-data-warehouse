import os
import sys
import subprocess
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
VENV_DIR = BASE_DIR / "venv"


def run_command(command):

    print("\n" + "=" * 70)
    print("RUNNING:", " ".join(command))
    print("=" * 70)

    subprocess.check_call(
        command,
        cwd=BASE_DIR
    )


def get_python():

    if sys.platform == "win32":
        return str(VENV_DIR / "Scripts" / "python.exe")

    return str(VENV_DIR / "bin" / "python")


def create_virtual_environment():

    if VENV_DIR.exists():

        print("\n[1/4] Virtual environment already exists.")

        return

    print("\n[1/4] Creating virtual environment...")

    run_command(
        [
            sys.executable,
            "-m",
            "venv",
            str(VENV_DIR)
        ]
    )

    print(" -> Virtual environment created.")


def install_dependencies(python):

    print("\n[2/4] Installing Python dependencies...")

    requirements_file = BASE_DIR / "requirements.txt"

    run_command(
        [
            python,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "pip"
        ]
    )

    run_command(
        [
            python,
            "-m",
            "pip",
            "install",
            "-r",
            str(requirements_file)
        ]
    )

    print(" -> Dependencies installed.")


def setup_database(python):

    print("\n[3/4] Setting up Data Warehouse database...")

    run_command(
        [
            python,
            "setup_database.py"
        ]
    )

    print(" -> Database setup completed.")


def run_etl(python):

    print("\n[4/4] Running master ETL pipeline...")

    run_command(
        [
            python,
            "main_etl.py"
        ]
    )

    print(" -> ETL completed.")


def main():

    print("\n")
    print("#" * 70)
    print(" AVIATION DATA WAREHOUSE - PROJECT SETUP")
    print("#" * 70)

    create_virtual_environment()

    python = get_python()

    install_dependencies(python)

    setup_database(python)

    run_etl(python)

    print("\n")
    print("#" * 70)
    print(" PROJECT SETUP AND ETL COMPLETED SUCCESSFULLY")
    print("#" * 70)


if __name__ == "__main__":
    main()