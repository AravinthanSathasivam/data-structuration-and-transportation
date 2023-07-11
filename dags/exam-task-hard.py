# DST project assignment
# Team : Aravinthan Sathasivam & Dinesh Murugan rasaratnan
import sqlite3
from dataclasses import dataclass
from airflow.decorators import dag, task
from datetime import datetime, date, timedelta
import requests
from utils import to_seconds_since_epoch


# Class - flight data
@dataclass
class FlightData:
    icao24: str
    firstSeen: int
    estDepartureAirport: str
    lastSeen: int
    estArrivalAirport: str
    callsign: str
    estDepartureAirportHorizDistance: int
    estDepartureAirportVertDistance: int
    estArrivalAirportHorizDistance: int
    estArrivalAirportVertDistance: int
    departureAirportCandidatesCount: int
    arrivalAirportCandidatesCount: int


@dag(
    start_date=datetime(2023, 1, 11),
    schedule_interval="0 1 * * *",
    catchup=False
)
def Exam_task_hard():
    BASE_URL = "https://opensky-network.org/api"

    # Task to fetch flight data
    @task
    def fetch_Flight_Data(ds=None) -> list:
        if ds is None:
            ds = date.today() - timedelta(days=7)
        one_day = ds + timedelta(days=1)
        params = {
            "airport": "LFPG",
            "begin": to_seconds_since_epoch(str(ds)),
            "end": to_seconds_since_epoch(str(one_day))
        }
        response = requests.get(f"{BASE_URL}/flights/departure", params=params)
        flightData = response.json()
        return flightData

    # Task to store flight data in SQLite database
    @task
    def store_Flight_Data(flights: list, ds=None) -> None:
        if ds is None:
            ds = date.today().isoformat()
        conn = sqlite3.connect('your_database.db')
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS flights (
            icao24 TEXT,
            firstSeen INTEGER,
            estDepartureAirport TEXT,
            lastSeen INTEGER,
            estArrivalAirport TEXT,
            callsign TEXT,
            estDepartureAirportHorizDistance INTEGER,
            estDepartureAirportVertDistance INTEGER,
            estArrivalAirportHorizDistance INTEGER,
            estArrivalAirportVertDistance INTEGER,
            departureAirportCandidatesCount INTEGER,
            arrivalAirportCandidatesCount INTEGER,
            ds TEXT
        )
        ''')

        for flight in flights:
            cursor.execute(
                '''INSERT INTO flights (icao24, firstSeen, estDepartureAirport, lastSeen, estArrivalAirport, callsign, estDepartureAirportHorizDistance, estDepartureAirportVertDistance, estArrivalAirportHorizDistance, estArrivalAirportVertDistance, departureAirportCandidatesCount, arrivalAirportCandidatesCount, ds) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (flight['icao24'], flight['firstSeen'], flight['estDepartureAirport'], flight['lastSeen'],
                 flight['estArrivalAirport'], flight['callsign'], flight['estDepartureAirportHorizDistance'],
                 flight['estDepartureAirportVertDistance'], flight['estArrivalAirportHorizDistance'],
                 flight['estArrivalAirportVertDistance'], flight['departureAirportCandidatesCount'],
                 flight['arrivalAirportCandidatesCount'], ds))
        conn.commit()

        cursor.execute("SELECT * FROM flights")
        rows = cursor.fetchall()

        for row in rows:
            print(row)

        cursor.execute('''DROP TABLE flights''')
        conn.close()

    ds = date.today() - timedelta(days=7)
    flights = fetch_Flight_Data(ds=ds)
    store_Flight_Data(flights=flights, ds=ds.isoformat())


_ = Exam_task_hard()