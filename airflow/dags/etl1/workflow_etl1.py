from asyncio import Task

from pendulum import duration, datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import dag, task, task_group
from airflow.sdk import get_current_context
from airflow.utils.edgemodifier import Label
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

#from utils.notifications import MyTaskNotifier, dag_failed, dag_success
from etl1.etl import extraction

default_args = {
    'owner': 'cryptobot',
    'retries': 3,
    'retry_delay': duration(minutes=5),
    #'on_retry_callback': MyTaskNotifier(),
    #'on_failure_callback': MyTaskNotifier()
}

def empty_table():
    return SQLExecuteQueryOperator(
        task_id='empty_table',
        conn_id='SNOW_DB',
        sql='TRUNCATE TABLE CRYPTOBOT_DB.RAW.RAW_OHCLV;',
        return_last=True,
        show_return_value_in_logs=True
    )

@task_group()
def ingestion_tg():

    data_ingestion() >> wait_task()


@task(multiple_outputs=True)
def data_ingestion():

    rows, timestamp =  extraction.main()

    return {'rows': rows, 'timestamp': timestamp}

def wait_task():

    return TimeDeltaSensor(
        task_id='wait_task',
        delta=duration(seconds=20)
    )

@task.sensor(poke_interval=20,
             retry_delay=20,
             timeout=600)
def check_loading_status():

    ti = get_current_context()['ti']
    timestamp = ti.xcom_pull(task_ids='ingestion_tg.data_ingestion', key='timestamp')
    hook = SnowflakeHook(snowflake_conn_id='SNOW_DB')
    result = hook.get_first(f"""SELECT DISTINCT STATUS FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                                TABLE_NAME=>'RAW.RAW_OHCLV', 
                                START_TIME=>DATEADD(hours, -1, CURRENT_TIMESTAMP()),
                                PIPE_NAME=>'COMMON.LOAD_RAW_DATA'
                                )) 
                                WHERE FILE_NAME LIKE '%{timestamp}%';""")

    return result[0] == 'LOADED'

@task_group()
def data_check_tg():

    db_rows_count() >> check_rows_count()

def db_rows_count():

    return SQLExecuteQueryOperator(
        task_id='db_rows_count',
        conn_id='SNOW_DB',
        sql='SELECT COUNT(*) FROM CRYPTOBOT_DB.RAW.RAW_OHCLV;',
        return_last=True,
        show_return_value_in_logs=True
    )

@task(retries=0)
def check_rows_count():

    context = get_current_context()
    ti = context["ti"]
    rows_expected = ti.xcom_pull(task_ids='ingestion_tg.data_ingestion', key='rows')
    rows_ingested = ti.xcom_pull(task_ids='data_check_tg.db_rows_count', key='return_value')[0][0]
    print(rows_ingested)
    if rows_expected != rows_ingested :
        raise Exception(f"Nombre de lignes chargees ({rows_ingested}) != nombre de lignes de l'API ({rows_expected})")

    print(f"Nombre de lignes = {rows_ingested}")

@dag(
    description='Cryptobot workflow for ETL1',
    schedule=duration(hours=2),
    start_date=datetime(2025, 5, 27),
    catchup=False,
    tags=['cryptobot', 'etl1'],
    #on_success_callback=dag_success,
    #on_failure_callback=dag_failed,
    default_args=default_args)
def etl1():


    (
    (EmptyOperator(task_id="start"), empty_table())
    >> ingestion_tg()
    >> Label('data loading')
    >> check_loading_status()
    >> data_check_tg()
    >> EmptyOperator(task_id="end")
     )

etl1()
