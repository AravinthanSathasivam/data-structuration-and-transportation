from datetime import datetime

from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

# Define the DAG
with DAG(
    'exercise17',
    schedule_interval=None,
    start_date=datetime(2023, 1, 11),
    catchup=False
) as dag:

    # Define the SQL file paths
    drop_table_sql = "sql/drop_table.sql"
    create_table_sql = "sql/create_table.sql"
    add_data_sql = "sql/add_data.sql"

    # Task 1: Drop table
    drop_table = SqliteOperator(
        task_id="drop_table",
        sqlite_conn_id="sqlite_default",
        sql=drop_table_sql
    )

    # Task 2: Create table
    create_table = SqliteOperator(
        task_id="create_table",
        sqlite_conn_id="sqlite_default",
        sql=create_table_sql
    )

    # Task 3: Add data
    add_data = SqliteOperator(
        task_id="add_data",
        sqlite_conn_id="sqlite_default",
        sql=add_data_sql
    )

    # Define the task dependencies
    drop_table >> create_table >> add_data
