# Data Engineering Lab: End-to-End ETL with Airflow & Docker 

Este repositorio contiene una implementación práctica de **Data Pipelines** orquestados con **Apache Airflow** y contenerizados con **Docker**. El proyecto simula un 
entorno empresarial de ingeniería de datos, desde la generación de datos sintéticos hasta la extracción de reportes de negocio.

![alt text](ss_airlfow.png)

##  Arquitectura del Proyecto

El sistema utiliza una arquitectura contenerizada donde **Airflow** actúa como el cerebro orquestador que conecta servicios aislados a través de una red Docker interna.

```mermaid
graph LR
    A[Generador Faker] -->|Batch Insert| B(PostgreSQL DW)
    B -->|Extraction SQL| C{Apache Airflow}
    C -->|Transformation JSON| D[Elasticsearch]
    D -->|Analytics| E[Kibana Dashboard]
    
    subgraph Docker Network
    B
    D
    E
    end
    
    subgraph Host / Orchestration
    C
    end

El stack tecnológico está diseñado para ser modular y escalable:

* **Orquestación:** Apache Airflow 2.x (Python Operators, Postgres Hooks).
* **Contenerización:** Docker & Docker Compose.
* **Base de Datos:** PostgreSQL 13.
* **Lenguaje:** Python 3.x.
* **Librerías Clave:** `Faker` (Data Generation), `Pandas`.

## ️ Pipelines Automatizados (DAGs)

1.  **`crear_tabla_empleados`**:
    * Inicialización DDL (Data Definition Language).
    * Crea esquemas y tablas relacionales en PostgreSQL de forma idempotente.

2.  **`contratar_personal_masivo`**:
    * **Extracción:** Generación de Mock Data con la librería `Faker`.
    * **Transformación:** Limpieza de strings y validación de tipos de datos (Python).
    * **Carga:** Inserción masiva optimizada (`executemany`) en PostgreSQL.

3.  **`exportar_empleados_csv`**:
    * Proceso de **Reverse ETL**.
    * Extrae datos procesados del Data Warehouse y genera reportes planos (.csv) para consumo de los stakeholders.

4.  **`sincronizar_empleados_es.py`**:
    * Pipeline Híbrido.
    * El pipeline no solo mueve datos; cruza fronteras entre tecnologías (SQL relacional a NoSQL documental) y entre entornos (mi Mac local a un contenedor Docker).

##  Cómo ejecutar este proyecto

1.  **Levantar Infraestructura:**
    ```bash
    docker compose up -d
    ```

2.  **Configurar Entorno Python:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install apache-airflow faker apache-airflow-providers-postgres
    ```

3.  **Iniciar Airflow:**
    ```bash
    export AIRFLOW_HOME=~/airflow
    airflow standalone
    ```

##  Autor
**Diego Ortiz** - *Data Engineer*
