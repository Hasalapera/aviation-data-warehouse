"""Explicit flight-timestamp semantics and safe UTC conversions.

No source contract currently proves how CSV or OLTP flight timestamps should
be interpreted. Both are therefore marked ``unresolved`` and must not be
silently treated as airport-local or UTC.

Only an explicit, verified declaration of ``origin_local`` or ``utc`` enables
timestamp conversion. Local wall-clock values that fall in a DST gap or a
repeated DST hour are rejected instead of guessed.
"""

from datetime import datetime, timezone

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


UTC = timezone.utc

UNRESOLVED = "unresolved"
ORIGIN_LOCAL = "origin_local"
UTC_TIMESTAMP = "utc"

SUPPORTED_SEMANTICS = frozenset({ORIGIN_LOCAL, UTC_TIMESTAMP})

# Evidence (or absence of evidence) for each flight source is recorded here.
# Change a value only when the source contract is documented and verified.
FLIGHT_TIMESTAMP_SEMANTICS = {
    "csv": UNRESOLVED,
    "oltp": UNRESOLVED,
}

SEMANTICS_EVIDENCE = {
    "csv": (
        "Synthetic CSV source; no documented timezone contract. HHMM values "
        "are converted to naive DATETIME by etl.py."
    ),
    "oltp": (
        "Naive DATETIME source columns with no timezone metadata or "
        "documented convention; server UTC setting is not a value contract."
    ),
}


class TimestampSemanticsError(ValueError):
    """Base error for unsafe or invalid timestamp interpretation."""


class UnresolvedTimestampSemantics(TimestampSemanticsError):
    """Raised when a source has no verified timestamp convention."""


class InvalidTimestamp(TimestampSemanticsError):
    """Raised when a timestamp value cannot be interpreted safely."""


class NonexistentLocalTime(TimestampSemanticsError):
    """Raised for a local wall-clock time skipped by a DST transition."""


class AmbiguousLocalTime(TimestampSemanticsError):
    """Raised for a local wall-clock time repeated by a DST transition."""


def source_semantics(source_system, overrides=None):
    """Return the declared semantics for one source system.

    ``overrides`` is intended for tests or a future verified configuration.
    An explicit override still has to use a supported executable value.
    """

    source = str(source_system or "").strip().lower()
    mapping = FLIGHT_TIMESTAMP_SEMANTICS
    if overrides is not None:
        mapping = {**FLIGHT_TIMESTAMP_SEMANTICS, **overrides}
    semantics = mapping.get(source, UNRESOLVED)
    if semantics not in SUPPORTED_SEMANTICS:
        return UNRESOLVED
    return semantics


def utc_zone(timezone_name):
    """Return a validated ``ZoneInfo`` or raise ``InvalidTimestamp``."""

    try:
        return ZoneInfo(str(timezone_name))
    except (ZoneInfoNotFoundError, TypeError, ValueError) as error:
        raise InvalidTimestamp(
            f"invalid IANA timezone: {timezone_name!r}"
        ) from error


def as_utc(value):
    """Return an aware UTC datetime and reject missing/invalid values."""

    if value is None:
        raise InvalidTimestamp("timestamp is missing")
    try:
        stamp = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(str(value))
        )
    except (TypeError, ValueError) as error:
        raise InvalidTimestamp(f"invalid timestamp: {value!r}") from error
    if stamp.tzinfo is None:
        return stamp.replace(tzinfo=UTC)
    return stamp.astimezone(UTC)


def utc_naive(value):
    """Return a naive UTC datetime suitable for the warehouse DATETIME."""

    return as_utc(value).replace(tzinfo=None)


def local_to_utc(value, timezone_name):
    """Convert a naive airport-local wall-clock value to aware UTC.

    Raises ``NonexistentLocalTime`` for spring-forward gaps and
    ``AmbiguousLocalTime`` for repeated fall-back times. Never guesses.
    """

    if not isinstance(value, datetime) or value.tzinfo is not None:
        raise InvalidTimestamp(
            "local timestamp must be a timezone-naive datetime"
        )
    tzinfo = utc_zone(timezone_name)
    candidates = []
    for fold in (0, 1):
        aware = value.replace(tzinfo=tzinfo, fold=fold)
        utc_value = aware.astimezone(UTC)
        round_trip = utc_value.astimezone(tzinfo).replace(tzinfo=None)
        if round_trip == value and utc_value not in candidates:
            candidates.append(utc_value)

    if not candidates:
        raise NonexistentLocalTime(
            f"{value} does not exist in {timezone_name} (DST gap)"
        )
    if len(candidates) > 1:
        raise AmbiguousLocalTime(
            f"{value} is ambiguous in {timezone_name} (DST fall-back)"
        )
    return candidates[0]


def local_hour_to_utc(value, timezone_name):
    """Convert a naive local hour to aware UTC without changing the hour."""

    if not isinstance(value, datetime) or value.tzinfo is not None:
        raise InvalidTimestamp(
            "local timestamp must be a timezone-naive datetime"
        )
    return local_to_utc(value.replace(minute=0, second=0, microsecond=0),
                        timezone_name)


def utc_to_local(value, timezone_name):
    """Convert a UTC instant to a naive airport-local wall-clock value."""

    return as_utc(value).astimezone(utc_zone(timezone_name)).replace(
        tzinfo=None
    )


def flight_utc_hour(value, timezone_name, semantics):
    """Return the exact UTC hour for a flight timestamp, or raise/return None.

    ``None`` means the source semantics are unresolved and no conversion is
    permitted. DST gaps and repeated hours raise explicit errors.
    """

    if not isinstance(value, datetime):
        raise InvalidTimestamp("scheduled_departure must be a datetime")
    if value.tzinfo is not None:
        if semantics == ORIGIN_LOCAL:
            raise InvalidTimestamp(
                "origin_local semantics require a timezone-naive timestamp"
            )
        value = value.astimezone(UTC).replace(tzinfo=None)

    if semantics == UTC_TIMESTAMP:
        return as_utc(value).replace(minute=0, second=0, microsecond=0)
    if semantics == ORIGIN_LOCAL:
        return local_hour_to_utc(value, timezone_name)
    return None
