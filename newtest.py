import os
import pendulum
from weather_extraction_logic import LOCATIONS, target_date_for_location, fetch_daily_record

api_key = os.environ["OPENWEATHER_API_KEY"]
logical_date = pendulum.now("Europe/Berlin")

location = LOCATIONS[0]  # Renningen
target_date = target_date_for_location(logical_date, location["timezone"])
start_ts = int(target_date.in_timezone("UTC").timestamp())

print("target_date (Berlin local midnight):", target_date)
print("start_ts sent to API, as UTC:", pendulum.from_timestamp(start_ts, tz="UTC"))

day_record = fetch_daily_record(location["lat"], location["lon"], start_ts, api_key)
print("day_record['dt'] as UTC:", pendulum.from_timestamp(day_record["dt"], tz="UTC"))
print("day_record['dt'] as Berlin local:", pendulum.from_timestamp(day_record["dt"], tz="UTC").in_timezone("Europe/Berlin"))