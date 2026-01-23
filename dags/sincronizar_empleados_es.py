from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from elasticsearch import Elasticsearch, helpers
from datetime import datetime

# Definimos la conexión a Elasticsearch (Nombre del contenedor en Docker)
ES_HOST = "http://localhost:9200"

def transferir_postgres_a_elastic():
    # 1. EXTRAER (Extract) desde Postgres
    print("Iniciando extracción desde Postgres...")
    pg_hook = PostgresHook(postgres_conn_id='postgres_lab')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    cursor.execute("SELECT id, nombre, puesto, fecha_ingreso FROM empleados")
    empleados = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    print(f"Se encontraron {len(empleados)} empleados en Postgres.")

    # 2. TRANSFORMAR (Transform) al formato de Elastic
    # Elastic necesita una lista de diccionarios (JSONs)
    actions = []
    for emp in empleados:
        # Creamos un diccionario zip(columnas, valores)
        doc = dict(zip(columns, emp))
        
        # Convertimos fechas a string (Elastic a veces se queja con objetos date)
        if 'fecha_ingreso' in doc and doc['fecha_ingreso']:
            doc['fecha_ingreso'] = str(doc['fecha_ingreso'])

        # Preparamos la acción para el Bulk API
        action = {
            "_index": "empleados_search", # El índice donde buscaremos
            "_source": doc
        }
        actions.append(action)

    # 3. CARGAR (Load) hacia Elasticsearch
    print(f"Conectando a Elasticsearch en {ES_HOST}...")
    es = Elasticsearch(hosts=[ES_HOST])
    
    # Verificar si estamos conectados
    if not es.ping():
        raise ValueError("¡No se pudo conectar a Elasticsearch!")

    print("Insertando datos masivamente...")
    success, errors = helpers.bulk(es, actions)
    
    cursor.close()
    connection.close()
    print(f"¡Éxito! Documentos indexados: {success}")

with DAG(
    dag_id='sincronizar_empleados_es',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    tarea_sincronizar = PythonOperator(
        task_id='etl_postgres_elastic',
        python_callable=transferir_postgres_a_elastic
    )
