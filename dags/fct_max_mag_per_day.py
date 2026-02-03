import pendulum
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

OWNER = "e.novikau"
DAG_ID = "fct_max_mag_per_day"

LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "dm"
TARGET_TABLE = "fct_max_mag_per_day"

PG_CONNECT = "postgres_dwh"

DEFAULT_ARGS = {
    "owner": OWNER,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 1, 25, tz="Europe/Moscow"),
    default_args=DEFAULT_ARGS,
    tags=["ods", "dm", "pg"],
    schedule="0 5 * * *",
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    sensor_on_pg_layer = ExternalTaskSensor(
        task_id="sensor_on_pg_layer",
        external_dag_id="raw_from_s3_to_pg",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000, 
        poke_interval=60,
    )
    
    drop_stg_if_exist = SQLExecuteQueryOperator(
        task_id="drop_tmp_if_exist",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP  TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """
    )
    
    create_stg_table = SQLExecuteQueryOperator(
        task_id="create_stg_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE TABLE stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}" AS 
        SELECT cast(time AS date) AS date, max(mag) as max_mag
        FROM  ods.fct_earthquake
        WHERE time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
        GROUP BY 1;
        """
    )
    
    drop_from_target_table = SQLExecuteQueryOperator(
        task_id="drop_from_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DELETE FROM {SCHEMA}.{TARGET_TABLE}
        WHERE date IN
        (
            SELECT date FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        )
        """,
    )
    
    insert_into_target_table = SQLExecuteQueryOperator(
        task_id="insert_into_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE}
        SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    drop_stg_table_after = SQLExecuteQueryOperator(
        task_id="drop_stg_table_after",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    end = EmptyOperator(
        task_id="end",
    )
    
    (
            start >>
            sensor_on_pg_layer >>
            drop_stg_if_exist >>
            create_stg_table >>
            drop_from_target_table >>
            insert_into_target_table >>
            drop_stg_table_after >>
            end
    )
    
    
    
    
    