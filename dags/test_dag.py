from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello from DBT + Airflow repo!")

with DAG(
    dag_id="dbt_airflow_test",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )
