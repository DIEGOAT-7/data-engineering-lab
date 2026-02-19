from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import random
import uuid

# Airflow corre local en tu Mac, así que apuntamos a localhost
ES_HOST = "http://localhost:9200"

def generar_eventos_clickstream():
    print(f"Conectando a Elasticsearch en {ES_HOST}...")
    es = Elasticsearch(hosts=[ES_HOST])
    
    # Validar conexión
    if not es.ping():
        raise ValueError("¡No se pudo conectar a Elasticsearch!")

    acciones = ['page_view', 'add_to_cart', 'remove_from_cart', 'checkout', 'purchase']
    dispositivos = ['mobile', 'desktop', 'tablet']
    
    eventos = []
    cantidad_eventos = 500 # Generaremos 500 eventos por cada ejecución
    
    print(f"Generando {cantidad_eventos} eventos de e-commerce...")
    
    for _ in range(cantidad_eventos):
        # Simulamos comportamiento de usuarios en una tienda
        evento = {
            "evento_id": str(uuid.uuid4()),
            "usuario_id": random.randint(1000, 9999),
            "tipo_accion": random.choices(acciones, weights=[60, 20, 5, 10, 5])[0], # Ponderamos para que haya más 'views' que 'purchases'
            "dispositivo": random.choice(dispositivos),
            "producto_id": f"PROD-{random.randint(1, 50)}",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Preparamos el formato para el Bulk Insert de Elastic
        action = {
            "_index": "ecommerce_clickstream",
            "_source": evento
        }
        eventos.append(action)

    print("Inyectando eventos en tiempo real...")
    success, errors = helpers.bulk(es, eventos)
    print(f"¡Éxito! {success} eventos de clickstream indexados.")

with DAG(
    dag_id='simulador_clickstream_ecommerce',
    start_date=datetime(2026, 2, 18),
    schedule='@hourly', # Este DAG está diseñado para correr cada hora
    catchup=False,
    tags=['E-commerce', 'Real-time', 'Elasticsearch']
) as dag:

    tarea_clickstream = PythonOperator(
        task_id='inyectar_trafico_web',
        python_callable=generar_eventos_clickstream
    )
