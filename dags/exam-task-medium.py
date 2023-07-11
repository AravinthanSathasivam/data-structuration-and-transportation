# DST project assignment
# Team : Aravinthan Sathasivam & Dinesh Murugan rasaratnan
import json
from datetime import datetime
from airflow.decorators import dag, task
from utils import read_from_api, process_weather_data


@dag(
    start_date=datetime(2022, 1, 12),
    schedule_interval="0 1 * * *",
    catchup=False
)
def Exam_task_medium():
    # Read data from the weather API
    @task
    def read_weather_forecast(latitude: float, longitude: float, date: str, weather_type: str) -> dict:
        URL = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&date={date}&hourly={weather_type}"
        weather_data = read_from_api(URL)
        return {"weather": weather_data}

    @task
    def transform_data(weather: dict) -> dict:
        hourly_forecast = weather['weather']['hourly']
        hourly_units = weather['weather']['hourly_units']
        # Perform transformation on weather data
        processed_data = process_weather_data(hourly_forecast, hourly_units)
        return processed_data

    # Write processed data to JSON file
    @task
    def write_toJSON(processed_data: dict) -> None:
        with open('./dags/weather_data.json', "w") as f:
            json.dump(processed_data, f)

    latitude = 52.52
    longitude = 13.41
    date = "2022-07-01"
    weather_type = "temperature_2m"

    weather_forecast = read_weather_forecast(latitude, longitude, date, weather_type)
    transformed_data = transform_data(weather_forecast)
    print(transformed_data)
    write_toJSON(transformed_data)


_ = Exam_task_medium()
