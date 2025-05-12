from datetime import timedelta

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator

def test():
    print('hello world')

with DAG(
    dag_id='crytpobot',
    description='Cryptobot worflow',
    schedule=timedelta(hours=2),
    catchup=False,
    tags=['cryptobot']

) as my_dag:

    task_1=PythonOperator(
        task_id='extraction',
        python_callable=test
    )


