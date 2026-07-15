"""
weather_daily_extract
======================

Pulls the *previous local calendar day's* daily-aggregate weather for a fixed
set of locations from the OpenWeather One Call API 4.0 (`/onecall/timeline/1day`)
and lands the raw response into a Snowflake landing table.

Design decisions (see conversation history for the full rationale):

* One shared DAG schedule (05:00 Europe/Berlin), but each location's
  "yesterday" is computed using ITS OWN IANA timezone, not the DAG's. This
  matters because Accra (Africa/Accra, UTC+0, no DST) and the four Baden-
  Württemberg cities (Europe/Berlin, UTC+1/+2 with DST) don't share an offset
  year-round.
* "Yesterday" is derived from the DAG run's logical date, not `datetime.now()`,
  so that backfills/reruns for a past date always pull the weather for that
  date -- not whatever "yesterday" happens to be when the task executes.
* The API's `1day` timeline endpoint doesn't accept a record-count parameter;
  given a `start` timestamp it always returns up to 10 forward records. We
  only want one, so we request with `start` = local midnight of the target
  day and keep only `data[0]`, discarding the 9 records after it.
* Raw landing is append-only (see weather_raw_ddl.sql for rationale) --
  dedup/"latest wins" logic belongs in the dbt staging layer, not here.
* Dynamic task mapping (`.expand()`) gives each location its own task
  instance in the Airflow UI: independent retries, independent logs, and a
  single city's API hiccup doesn't fail the other four.

Requires: apache-airflow-providers-snowflake, requests
Airflow Variable required: `openweather_api_key`
Airflow Connection required: `snowflake_default` (or update SNOWFLAKE_CONN_ID)
Snowflake objects required: run weather_raw_ddl.sql first (creates the
WEATHER_ANALYTICS database, RAW/BRONZE/SILVER/GOLD schemas, WEATHER_WH
warehouse, and the RAW.RAW_WEATHER_DAILY landing table this DAG writes to).
"""

from __future__ import annotations

import json
import logging

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID = "snowflake_default"
# Explicit database/schema/warehouse rather than relying on the connection's
# defaults -- now that WEATHER_ANALYTICS and AIRLINE_ANALYTICS both exist in
# the same account, an unqualified connection context is ambiguous.
SNOWFLAKE_DATABASE = "WEATHER_ANALYTICS"
SNOWFLAKE_SCHEMA = "RAW"
SNOWFLAKE_WAREHOUSE = "WEATHER_WH"
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

INSERT_RAW_SQL = """
    INSERT INTO WEATHER_ANALYTICS.RAW.RAW_WEATHER_DAILY
        (LOCATION_ID, CITY_NAME, REQUEST_LAT, REQUEST_LON, REQUEST_TIMEZONE,
         TARGET_DATE, START_PARAM_UTC, RAW_PAYLOAD)
    SELECT
        %(location_id)s,
        %(city_name)s,
        %(lat)s,
        %(lon)s,
        %(timezone)s,
        %(target_date)s,
        %(start_param_utc)s,
        PARSE_JSON(%(raw_payload)s)
"""


def _target_date_for_location(logical_date: pendulum.DateTime, location_tz: str) -> pendulum.DateTime:
    """Local midnight of the day BEFORE the DAG run's logical date, in the
    location's own timezone.

    Using the logical date (fixed per DAG run) rather than `pendulum.now()`
    keeps this deterministic across retries and backfills.
    """
    tz = pendulum.timezone(location_tz)
    run_date_local = logical_date.in_timezone(tz)
    return run_date_local.subtract(days=1).start_of("day")


def _fetch_daily_record(lat: float, lon: float, start_ts: int, api_key: str) -> dict:
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


def _insert_raw_row(location: dict, target_date: pendulum.DateTime, start_ts: int, day_record: dict) -> None:
    hook = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )
    hook.run(
        INSERT_RAW_SQL,
        parameters={
            "location_id": location["location_id"],
            "city_name": location["city_name"],
            "lat": location["lat"],
            "lon": location["lon"],
            "timezone": location["timezone"],
            "target_date": target_date.to_date_string(),
            "start_param_utc": target_date.in_timezone("UTC").to_datetime_string(),
            "raw_payload": json.dumps(day_record),
        },
    )


@dag(
    dag_id="weather_daily_extract",
    description="Daily pull of the previous day's weather per location from OpenWeather One Call API 4.0",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2026, 7, 1, tz="Europe/Berlin"),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["weather", "extraction", "openweather"],
)
def weather_daily_extract():
    @task
    def extract_and_load(location: dict, logical_date: pendulum.DateTime = None) -> None:
        target_date = _target_date_for_location(logical_date, location["timezone"])
        start_ts = int(target_date.in_timezone("UTC").timestamp())

        api_key = Variable.get("openweather_api_key")
        day_record = _fetch_daily_record(location["lat"], location["lon"], start_ts, api_key)

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

        _insert_raw_row(location, target_date, start_ts, day_record)
        logger.info("Loaded %s weather for %s", location["city_name"], target_date.to_date_string())

    extract_and_load.expand(location=LOCATIONS)


weather_daily_extract()
