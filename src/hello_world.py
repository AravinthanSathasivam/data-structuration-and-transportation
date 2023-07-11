from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime

@dag(
    schedule_interval=None,  # only triggered manually
    start_date=datetime(2023, 1, 11),
    catchup=False
)
def hello_operator_dag():
    def say_hello() -> str:
        return "Hello, world!"

    hello_task = PythonOperator(task_id="hello", python_callable=say_hello)

    return hello_task

dag = hello_operator_dag()
