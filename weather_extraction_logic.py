"""
weather_extraction_logic
=========================
Pure extraction logic for the weather_daily_extract DAG, deliberately kept
free of any Airflow imports (no `airflow.decorators`, no `Variable`, no
hooks) so it can be imported and exercised directly -- in a plain Python
script or REPL, with only `pendulum` and `requests` installed -- without
Airflow present in the environment at all.

The DAG file (weather_daily_extract_dag.py) imports from this module and
wraps it with the Airflow-specific orchestration: task decoration, the
`Variable.get()` lookup for the API key, and the Snowflake write.
"""
from __future__ import annotations

import pendulum
import requests

ONE_CALL_DAILY_URL = "https://api.openweathermap.org/data/4.0/onecall/timeline/1day"
REQUEST_TIMEOUT_SECONDS = 30

# Location dimension seed. lat/lon are decimal degrees; timezone is the IANA
# name used both to compute each location's local midnight and (eventually)
# to populate dim_location in the warehouse.
LOCATIONS = [
    {"location_id": "renningen", "city_name": "Renningen", "lat": 48.7697, "lon": 8.9387, "timezone": "Europe/Berlin"},
    {"location_id": "gerlingen", "city_name": "Gerlingen", "lat": 48.7984, "lon": 9.0624, "timezone": "Europe/Berlin"},
    {"location_id": "leonberg", "city_name": "Leonberg", "lat": 48.8000, "lon": 9.0167, "timezone": "Europe/Berlin"},
    {"location_id": "stuttgart", "city_name": "Stuttgart", "lat": 48.7784, "lon": 9.1800, "timezone": "Europe/Berlin"},
    {"location_id": "accra", "city_name": "Accra", "lat": 5.6037, "lon": -0.1870, "timezone": "Africa/Accra"},
]


def target_date_for_location(logical_date: pendulum.DateTime, location_tz: str) -> pendulum.DateTime:
    """Local midnight of the day BEFORE `logical_date`, in the location's
    own timezone.

    `logical_date` is required, with no default -- callers (Airflow or a
    manual test) must always supply a real date explicitly, so a forgotten
    argument fails immediately instead of silently testing against `None`
    or today's date.
    """
    tz = pendulum.timezone(location_tz)
    run_date_local = logical_date.in_timezone(tz)
    return run_date_local.subtract(days=1).start_of("day")


def fetch_daily_record(lat: float, lon: float, start_ts: int, api_key: str) -> dict:
    params = {
        "lat": lat,
        "lon": lon,
        "start": start_ts,
        "units": "metric",
        "lang": "en",
        "appid": api_key,
    }
    response = requests.get(ONE_CALL_DAILY_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()

    records = payload.get("data") or []
    if not records:
        raise ValueError(f"OpenWeather returned no records for start={start_ts}, lat={lat}, lon={lon}")

    # The endpoint always returns up to 10 forward records from `start`; we
    # only asked for one day, so keep the first and discard the rest.
    return records[0]


def build_weather_record(
    location: dict, logical_date: pendulum.DateTime, api_key: str
) -> tuple[pendulum.DateTime, int, dict]:
    """Compute the target date, call the API, and validate the response.

    Both `logical_date` and `api_key` are required, with no defaults -- a
    forgotten argument fails loudly and immediately, rather than silently
    testing against `None`, today's date, or a missing key.

    Returns (target_date, start_ts, day_record) so the caller (the Airflow
    task, or a manual test script) has everything needed to proceed --
    e.g. to pass on to the Snowflake insert, or just to print and inspect.
    """
    target_date = target_date_for_location(logical_date, location["timezone"])
    start_ts = int(pendulum.datetime(target_date.year, target_date.month, target_date.day, tz="UTC").timestamp())

    day_record = fetch_daily_record(location["lat"], location["lon"], start_ts, api_key)

    # Sanity check: confirm the record we got back actually corresponds
    # to the local calendar day we asked for. Catches silent drift from
    # DST edge cases, API changes, or an off-by-one in the start param --
    # better to fail loudly here than land a mislabeled row.
    returned_local_date = (
        pendulum.from_timestamp(day_record["dt"], tz="UTC")
        .in_timezone(location["timezone"])
        .date()
    )
    if returned_local_date != target_date.date():
        raise ValueError(
            f"{location['city_name']}: expected record for {target_date.date()} "
            f"but API returned {returned_local_date}"
        )

    return target_date, start_ts, day_record
