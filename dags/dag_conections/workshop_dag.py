from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys 
import os
#sys.path.append(os.path.abspath("/opt/airflow/dags/dag_connections/"))
from dag_conections.etl import read_csv, transform, extract_sql, transform_sql, merge # load, store




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 3),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'workshop2_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
) as dag:


    read_csv_task = PythonOperator(
        task_id='read_csv_task',
        python_callable=read_csv,
        provide_context = True,
        )

    read_db_task = PythonOperator(
        task_id='read_db_task',
        python_callable=extract_sql,
        provide_context = True,
        )

    transform_csv_task = PythonOperator(
        task_id='transform_csv_task',
        python_callable=transform,
        provide_context = True,
)

    transform_db_task = PythonOperator(
        task_id='transform_db_task',
        python_callable=transform_sql,
        provide_context = True,
        )
    
    merge_task = PythonOperator(
        task_id='merge_task',
        python_callable=merge,
        provide_context = True,
        )
    
    # load_task = PythonOperator(
    #     task_id='load_task',
    #     python_callable=load,
    #     provide_context = True,
    #     )
    
    # store_task = PythonOperator(
    #     task_id='store_task',
    #     python_callable=store,
    #     provide_context = True,
    #     )

    read_csv_task >> transform_csv_task >> merge_task #>> load_task >> store_task
    read_db_task >> transform_db_task >> merge_task