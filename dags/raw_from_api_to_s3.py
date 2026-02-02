import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.sdk import task 
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator

OWNER = "e.novikau"
DAG_ID = "raw_from_api_to_s3"

#Слои в S3
LAYER = "raw"
SOURCE = "earthquake"

#S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

DEFAULT_ARGS = {
    "owner": OWNER,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


@task
def get_and_transfer_api_data_to_s3(
    data_interval_start=None,
    data_interval_end=None,
):
    start_date = data_interval_start.format("YYYY-MM-DD")
    
    if data_interval_end and data_interval_end != data_interval_start:
        end_date = data_interval_end.format("YYYY-MM-DD")
    else:
        # Если end_date равен start_date, добавляем 1 день
        end_date = data_interval_start.add(days=1).format("YYYY-MM-DD")

    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    logging.info(f'Downloading from: https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}')

    con = duckdb.connect()
    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY (
            SELECT *
            FROM read_csv_auto(
                'https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}'
            )
        )
        TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """
    )
    con.close()

    logging.info(f"✅ Download for date success: {start_date}")


with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 1, 25, tz="Europe/Moscow"),
    schedule="0 5 * * *",   # schedule вместо schedule_interval
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")
    load = get_and_transfer_api_data_to_s3()
    end = EmptyOperator(task_id="end")

    start >> load >> end
    



# 2026-01-25
# https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime=2026-01-25&endtime=2026-01-26