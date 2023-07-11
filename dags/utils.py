from datetime import date
from time import mktime

import requests


def read_from_api(url: str) -> dict:
    response = requests.get(url)
    weather_data = response.json()
    return weather_data

def process_weather_data(hourly_forecast: dict, hourly_units: dict) -> dict:
    processed_data = {}
    return processed_data

def to_seconds_since_epoch(input_date: str) -> int:
    return int(mktime(date.fromisoformat(input_date).timetuple()))