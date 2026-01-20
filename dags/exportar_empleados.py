from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import csv
import os

def exportar_a_csv():
    # 1. Definir dónde queremos guardar el archivo
    # Usamos os.path.expanduser para asegurar que encuentre tu carpeta de usuario
    ruta_archivo = os.path.expanduser('~/Desktop/data-engineering-lab/reporte_empleados.csv')
    
    # 2. Conectarse a la Base de Datos
    pg_hook = PostgresHook(postgres_conn_id='postgres_lab')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    # 3. Ejecutar la consulta (Query)
    cursor.execute("SELECT * FROM empleados")
    registros = cursor.fetchall() # Traer todos los datos
    
    # Obtener los nombres de las columnas (id, nombre, puesto, etc.)
    nombres_columnas = [desc[0] for desc in cursor.description]
    
    # 4. Escribir el archivo CSV
    print(f"Escribiendo reporte en: {ruta_archivo}")
    
    with open(ruta_archivo, 'w', newline='', encoding='utf-8') as archivo_csv:
        escritor = csv.writer(archivo_csv)
        
        # Escribir encabezados
        escritor.writerow(nombres_columnas)
        
        # Escribir los datos (las 101 filas)
        escritor.writerows(registros)
        
    cursor.close()
    connection.close()
    print("¡Reporte generado exitosamente!")

with DAG(
    dag_id='exportar_empleados_csv',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    tarea_exportar = PythonOperator(
        task_id='generar_reporte_csv',
        python_callable=exportar_a_csv
    )
