from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def saludar_mundo():
    print("Hola Diego, Soy Airflow y estoy vivo.")

with DAG(
    dag_id='mi_primer_dag_oficial',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    tarea_saludo = PythonOperator(
        task_id='tarea_imprimir_hola',
        python_callable=saludar_mundo
    )
