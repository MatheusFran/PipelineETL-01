from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator
from include.extractFunction import ExtractData
from include.transformFunction import TransformData
from include.loadFunction import LoadData

with DAG(
    dag_id="futebol_etl",
    start_date=datetime(year=2025, month=7, day=30, hour=7, minute=0),
    schedule="@daily",
    catchup=True,
    max_active_runs=2,
    render_template_as_native_obj=True
) as dag:

    extract_data = PythonOperator(
        dag=dag,
        task_id="extract_data",
        python_callable= ExtractData,
    )

    transform_data = PythonOperator(
        dag=dag,
        task_id="transform_data",
        python_callable=TransformData,
    )

    load_data = PythonOperator(
        dag=dag,
        task_id="load_data",
        python_callable=LoadData,
    )

    extract_data >> transform_data >> load_data