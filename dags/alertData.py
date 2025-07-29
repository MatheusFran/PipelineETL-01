from airflow import DAG
import datetime
from include.sendEmail import send_email
from include.selectData import select_data
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="alert_Data",
    start_date=datetime(year=2025, month=7, day=30, hour=7, minute=30),
    schedule="@weekly",
    catchup=True,
    max_active_runs=1,
)as dag:

    select_data = PythonOperator(
        dag=dag,
        task_id="select_data",
        python_callable=select_data,
    )

    send_email = PythonOperator(
        dag=dag,
        task_id='send_email',
        python_callable=send_email,
    )


    select_data >> send_email