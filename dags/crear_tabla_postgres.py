from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

# Definición del DAG
with DAG(
    dag_id='crear_tabla_empleados',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    # --- INICIO DEL BLOQUE INDENTADO (4 espacios) ---
    
    # Tarea 1: Crear Tabla
    crear_tabla = SQLExecuteQueryOperator(
        task_id='crear_tabla_en_postgres',
        conn_id='postgres_lab',
        sql="""
            CREATE TABLE IF NOT EXISTS empleados (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(50),
                puesto VARCHAR(50),
                fecha_ingreso DATE
            );
        """
    )

    # Tarea 2: Insertar Jefe
    insertar_jefe = SQLExecuteQueryOperator(
        task_id='insertar_jefe',
        conn_id='postgres_lab',
        sql="""
            INSERT INTO empleados (nombre, puesto, fecha_ingreso)
            VALUES ('Diego Ortiz', 'Head of Data', '2026-01-18');
        """
    )

    # Orden de ejecución
    crear_tabla >> insertar_jefe

    
