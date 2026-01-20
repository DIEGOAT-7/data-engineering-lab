from airflow import DAG
# Usamos la importación moderna para Airflow 3.0 (adiós advertencia amarilla)
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from faker import Faker

def generar_y_guardar_empleados():
    fake = Faker('es_ES')
    registros = []

    print("Generando datos falsos...")
    for _ in range(100):
        # --- AQUÍ ESTÁ LA MAGIA DE LA GUILLOTINA ---
        # [:50] significa: "Toma desde la letra 0 hasta la 50 e ignora el resto"
        nombre = fake.name()[:50] 
        puesto = fake.job()[:50]
        
        fecha = fake.date_between(start_date='-1y', end_date='today')
        registros.append((nombre, puesto, fecha))
    
    # Conexión y carga
    pg_hook = PostgresHook(postgres_conn_id='postgres_lab')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    sql = """INSERT INTO empleados (nombre, puesto, fecha_ingreso) VALUES (%s, %s, 
%s)"""
    
    print(f"Insertando {len(registros)} empleados...")
    cursor.executemany(sql, registros)
    
    connection.commit()
    cursor.close()
    connection.close()
    print("¡Éxito! Datos recortados y guardados.")

with DAG(
    dag_id='contratar_personal_masivo',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    tarea_contratar = PythonOperator(
        task_id='generar_insertar_faker',
        python_callable=generar_y_guardar_empleados
    )
