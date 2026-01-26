from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from faker import Faker
from datetime import datetime, timedelta

def generar_y_guardar_empleados():
    fake = Faker('es_ES')
    registros = []

    print("Generando datos falsos...")
    for _ in range(100):
       
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
    # Poner fecha de inicio: Hace 7 días exactos
    start_date=datetime.now() - timedelta(days=7),
    # Ejecutar: Una vez al día
    schedule='@daily',
    # Catchup True: "Rellena los días perdidos desde el start_date hasta hoy"
    catchup=True,
    tags=['RRHH', 'Ingesta']
) as dag:

    tarea_contratar = PythonOperator(
        task_id='generar_insertar_faker',
        python_callable=generar_y_guardar_empleados
    )
