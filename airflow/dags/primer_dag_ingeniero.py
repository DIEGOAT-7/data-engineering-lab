from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# 1. Definir la función Python (Lo que queremos hacer)
def saludar_mundo():
    print("¡Hola Diego! Soy Airflow y estoy vivo.")

# 2. Configurar el DAG (El contenedor del proceso)
with DAG(
    dag_id='mi_primer_dag_oficial',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, # None significa "Solo corre cuando yo le dé Play manual"
    catchup=False
) as dag:

    # 3. Definir la Tarea
    tarea_saludo = PythonOperator(
        task_id='tarea_imprimir_hola',
        python_callable=saludar_mundo
    )

    # Aquí definimos el orden (si hubiera más tareas)
    tarea_saludo