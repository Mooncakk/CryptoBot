import os

from pendulum import duration, datetime

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from utils.notifications import MyTaskNotifier, dag_failed, dag_success
from etl1.etl import extraction, transformation, load

DAG_PATH = f'{os.getenv("AIRFLOW_HOME")}/dags'
os.chdir(DAG_PATH)

default_args = {
    'owner': 'cryptobot',
    'retries': 3,
    'retry_delay': duration(minutes=5),
    'on_retry_callback': MyTaskNotifier(),
    'on_failure_callback': MyTaskNotifier()
}

with DAG(
    dag_id='cryptobot_ETL1',
    description='Cryptobot workflow for ETL1',
    schedule=duration(hours=2),
    start_date=datetime(2025, 5, 27),
    catchup=False,
    tags=['cryptobot', 'etl1'],
    on_success_callback=dag_success,
    on_failure_callback=dag_failed,
    default_args=default_args

) as my_dag:

    data_collection=PythonOperator(
        task_id='data_collection',
        python_callable=extraction.main,
    )

    data_processing=PythonOperator(
        task_id='data_processing',
        python_callable=transformation.main
    )

    data_loading=PythonOperator(
        task_id='data_loading',
        python_callable=load.main
    )


    trigger_cryptobot_bot = TriggerDagRunOperator(
        task_id='trigger_cryptobot_bot',
        trigger_dag_id='cryptobot_bot',
        wait_for_completion=True,
        allowed_states=['success', 'failed'],
        failed_states=None,
        poke_interval=30

    )


data_collection >> Label('Extract data') >> data_processing
data_processing >> Label('Processed data') >> data_loading
data_loading >> Label('Load data') >> trigger_cryptobot_bot
