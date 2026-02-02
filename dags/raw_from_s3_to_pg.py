import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.sdk import task 
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

OWNER = "e.novikau"
DAG_ID = "raw_from_s3_to_pg"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# DuckDB
PASSWORD = Variable.get("pg_password")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

DEFAULT_ARGS = {
    "owner": OWNER,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

@task(retries=2, retry_delay=pendulum.duration(minutes=5))
def get_and_transfer_s3_data_to_ods_pg(
    data_interval_start=None,
    data_interval_end=None,
):
    start_date = data_interval_start.format("YYYY-MM-DD")
    end_date = data_interval_end.format("YYYY-MM-DD")
    
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()
    
    con.sql(
        f"""
        SET TIMEZONE = 'UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;
        
        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD '{PASSWORD}'
        );
        
        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
        
        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        (
            time,
            latitude,
            longitude,
            depth,
            mag,
            mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontal_error,
            depth_error,
            mag_error,
            mag_nst,
            status,
            location_source,
            mag_source
        )
        SELECT 
            time,
            latitude,
            longitude,
            depth,
            mag,
            magType AS mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontalError AS horizontal_error,
            depthError AS depth_error,
            magError AS mag_error,
            magNst AS mag_nst,
            status,
            locationSource AS location_source,
            magSource AS mag_source
        FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """
    )
    con.close()
    logging.info(f'✅ Download for date success: {start_date}')
    
with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 1, 25, tz="Europe/Moscow"),
    schedule="0 5 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=["s3", "ods", "pg"],
    description=SHORT_DESCRIPTION,
    max_active_tasks=1,
    max_active_runs=1
) as dag:
    dag.doc_md = LONG_DESCRIPTION
    
    start = EmptyOperator(task_id = "start")
    
    sensor_on_layer = ExternalTaskSensor(
        task_id = 'sensor_on_raw_layer',
        external_dag_id= 'raw_from_api_to_s3',
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  # длительность работы сенсора
        poke_interval=60,  # частота проверки
    )
    
    get_and_transfer_s3_data_to_ods_pg = get_and_transfer_s3_data_to_ods_pg()
    
    end = EmptyOperator(task_id = "end")
    
    start >> sensor_on_layer >> get_and_transfer_s3_data_to_ods_pg >> end