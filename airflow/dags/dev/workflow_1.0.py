import os
from datetime import timedelta, datetime

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.edgemodifier import Label

from etl import extraction, transformation, load

DAG_PATH = f'{os.getenv("AIRFLOW_HOME")}/dags'
os.chdir(DAG_PATH)

def test():
    print('hello world')
    return 'hello world'

with DAG(
    dag_id='crytpobot',
    description='Cryptobot worflow',
    schedule=timedelta(hours=2),
    start_date=datetime(2025, 5, 27),
    catchup=False,
    tags=['cryptobot'],
    default_args={
            'owner': 'cryptobot'
        }

) as my_dag:

    init_tasks = EmptyOperator(
        task_id='init_tasks')

    _error=EmptyOperator(
        task_id='error',
        trigger_rule='one_failed'
    )

    data_collection=PythonOperator(
        task_id='data_collection',
        python_callable=extraction.main,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    data_processing=PythonOperator(
        task_id='data_processing',
        python_callable=transformation.main,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    data_loading=PythonOperator(
        task_id='data_loading',
        python_callable=load.main,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    bot=EmptyOperator(
        task_id='bot'
    )


init_tasks >> data_collection
data_collection >> Label('No errors') >> data_processing
data_collection >> Label('Errors found') >> _error

data_processing >> Label('No errors') >> data_loading
data_processing >> Label('Errors found') >>_error

data_loading >> Label('No errors') >> bot
data_loading >> Label('Errors found') >>_error

bot >> Label("Errors found") >> _error
