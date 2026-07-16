# quick_test.py
import pendulum
from weather_extraction_logic import LOCATIONS, build_weather_record

logical_date = pendulum.now("Europe/Berlin")
api_key = "your-real-openweather-key"

target_date, start_ts, day_record = build_weather_record(LOCATIONS[0], logical_date, api_key)
print(target_date.to_date_string())
print(day_record)
