"""Generate deterministic synthetic enplaned passenger counts for the CSV.

This is a one-off data-generation utility for data/flight.csv. It appends the
passenger_count column without rewriting any existing field, row order, or
line ending. The generated values are SYNTHETIC and must never be presented as
observed passenger counts.

Usage:
    venv/bin/python scripts/generate_synthetic_passengers.py
    venv/bin/python scripts/generate_synthetic_passengers.py --execute

The default mode is a dry run. --execute writes the file atomically.
"""

import argparse
import csv
import hashlib
import math
import os
import sys
import tempfile
from collections import Counter
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "flight.csv"

SEED = "aviation_dw_synthetic_pax_v1"
LF_STEPS = (0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90)

BAND_CAPACITY = {
    "short": 100,
    "medium": 297,
    "long": 396,
}

REQUIRED_COLUMNS = (
    "flight_id",
    "distance_km",
    "cancelled_flag",
    "diverted_flag",
)


def _fail(message):
    raise SystemExit(f"[STOP] {message}")


def _half_up(value):
    return int(math.floor(value + 0.5))


def _load_factor(flight_id):
    digest = hashlib.sha256(f"{SEED}:{flight_id}".encode("utf-8")).hexdigest()
    fraction = int(digest[:8], 16) / (2 ** 32)
    index = min(int(fraction * len(LF_STEPS)), len(LF_STEPS) - 1)
    return LF_STEPS[index]


def _distance_band(distance_km):
    distance = float(distance_km)
    if not math.isfinite(distance) or distance < 0:
        _fail(f"invalid distance_km: {distance_km!r}")
    if distance < 3000:
        return "short"
    if distance < 6000:
        return "medium"
    return "long"


def _split_line_ending(line):
    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith("\n"):
        return line[:-1], "\n"
    return line, ""


def _parse_row(line, expected_columns, line_number):
    content, _ = _split_line_ending(line)
    try:
        fields = next(csv.reader([content]))
    except csv.Error as error:
        _fail(f"cannot parse CSV line {line_number}: {error}")
    if len(fields) != expected_columns:
        _fail(
            f"line {line_number} has {len(fields)} columns; "
            f"expected {expected_columns}"
        )
    return fields


def build_rows():
    if not CSV_PATH.exists():
        _fail(f"CSV not found: {CSV_PATH}")

    with open(CSV_PATH, "r", encoding="utf-8", newline="") as handle:
        lines = handle.readlines()

    if not lines:
        _fail("CSV is empty")

    header_content, header_ending = _split_line_ending(lines[0])
    try:
        header = next(csv.reader([header_content]))
    except csv.Error as error:
        _fail(f"cannot parse CSV header: {error}")

    if "passenger_count" in header:
        _fail("CSV already contains passenger_count; refusing to append twice")

    missing = [column for column in REQUIRED_COLUMNS if column not in header]
    if missing:
        _fail(f"CSV is missing required columns: {missing}")

    indexes = {column: header.index(column) for column in REQUIRED_COLUMNS}
    expected_columns = len(header)

    output_lines = [header_content + ",passenger_count" + header_ending]
    values = []
    bands = Counter()
    statuses = Counter()
    seen_ids = set()

    for line_number, line in enumerate(lines[1:], start=2):
        if not line.strip():
            _fail(f"unexpected blank line at line {line_number}")

        fields = _parse_row(line, expected_columns, line_number)
        content, ending = _split_line_ending(line)

        flight_id = fields[indexes["flight_id"]].strip()
        if not flight_id:
            _fail(f"empty flight_id at line {line_number}")
        if flight_id in seen_ids:
            _fail(f"duplicate flight_id {flight_id!r} at line {line_number}")
        seen_ids.add(flight_id)

        cancelled = fields[indexes["cancelled_flag"]].strip()
        diverted = fields[indexes["diverted_flag"]].strip()
        if cancelled not in ("0", "1") or diverted not in ("0", "1"):
            _fail(
                f"unexpected status flags at line {line_number}: "
                f"cancelled={cancelled!r}, diverted={diverted!r}"
            )
        if cancelled == "1" and diverted == "1":
            _fail(f"both cancelled and diverted at line {line_number}")
        statuses["cancelled" if cancelled == "1" else "operated"] += 1
        statuses["diverted" if diverted == "1" else "not_diverted"] += 1

        band = _distance_band(fields[indexes["distance_km"]].strip())
        bands[band] += 1

        if cancelled == "1":
            passenger_count = 0
        else:
            capacity = BAND_CAPACITY[band]
            passenger_count = _half_up(capacity * _load_factor(flight_id))
            if passenger_count <= 0 or passenger_count > capacity:
                _fail(
                    f"generated passenger_count out of range for "
                    f"flight_id {flight_id!r}: {passenger_count}"
                )

        values.append(passenger_count)
        output_lines.append(f"{content},{passenger_count}{ending}")

    if len(values) != 500:
        _fail(f"expected 500 data rows, found {len(values)}")
    if len(seen_ids) != len(values):
        _fail("flight_id values are not unique")

    return output_lines, values, bands, statuses


def _print_summary(values, bands, statuses):
    distribution = Counter(values)
    print(f" -> Seed: {SEED}")
    print(f" -> Load-factor steps: {list(LF_STEPS)}")
    print(f" -> CSV rows: {len(values)}")
    print(f" -> Band counts: short={bands['short']} medium={bands['medium']} "
          f"long={bands['long']}")
    print(f" -> Cancelled rows: {statuses['cancelled']} (value 0)")
    print(f" -> Diverted rows: {statuses['diverted']} (normal generation)")
    print(f" -> Passenger total: {sum(values)}")
    print(f" -> Passenger min/max: {min(values)}/{max(values)}")
    print(f" -> Distinct passenger values: {len(distribution)}")
    print(" -> Distribution:")
    for value in sorted(distribution):
        print(f"      {value:>3}: {distribution[value]}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="write data/flight.csv atomically (default is dry run)",
    )
    args = parser.parse_args(argv)

    output_lines, values, bands, statuses = build_rows()
    _print_summary(values, bands, statuses)

    if not args.execute:
        print("\n -> DRY RUN: no file written")
        print(" -> re-run with --execute to append passenger_count")
        return 0

    directory = str(CSV_PATH.parent)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        newline="",
        dir=directory,
        prefix=".flight.csv.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temp_path = handle.name
        handle.writelines(output_lines)
        handle.flush()
        os.fsync(handle.fileno())

    try:
        os.replace(temp_path, CSV_PATH)
    except Exception:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise

    print(f"\n -> WROTE: {CSV_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
