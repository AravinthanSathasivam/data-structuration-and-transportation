# DST project assignment
# Team : Aravinthan Sathasivam & Dinesh Murugan rasaratnan

import json
from datetime import datetime
import requests
from airflow.decorators import dag, task

@dag(
    # DAG start date
    start_date=datetime(2022, 1, 12),
    schedule_interval="0 1 * * *",
    catchup=False
)
def Exam_task_easy():
    URL = "https://opensky-network.org/api/flights/departure?airport=LFPG&begin=1669852800&end=1669939200"

    @task(multiple_outputs=True)
    def read_flights() -> dict:
        r = requests.get(URL)
        flights = r.json()
        flightsString = json.dumps(flights)
        print(flightsString)
        return {"flights": flights}

    # Write Json file
    @task
    def write_toJSON(flights: dict) -> None:
        with open('./dags/flights.json', "w") as f:
            json.dump(flights["flights"], f)

    # Execute read_flights task
    flights = read_flights()
    # Execute write_toJSON task
    write_toJSON(flights)

_ = Exam_task_easy()
