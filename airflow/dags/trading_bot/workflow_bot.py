import os

from pendulum import duration, datetime
import asyncio
import functools

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from utils.notifications import MyTaskNotifier, dag_failed, dag_success
from trading_bot.bot import trading_bot


DAG_PATH = f'{os.getenv("AIRFLOW_HOME")}/dags'
os.chdir(DAG_PATH)


def run_async(async_func):
    """A decorator for running asynchronous functions"""

    @functools.wraps(async_func)
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(async_func(*args, **kwargs))
    return wrapper

@run_async
async def bot():
    """Run the trading bot"""

    await trading_bot.main()


default_args = {
    "owner": "cryptobot",
    "retries": 3,
    "retry_delay": duration(minutes=1),
    "on_retry_callback": MyTaskNotifier(),
    "on_failure_callback": MyTaskNotifier(),
}


with DAG(
    dag_id='cryptobot_bot',
    description='CryptoBot workflow for trading bot',
    schedule=None,
    start_date=datetime(2025, 5, 27),
    catchup=False,
    tags=['cryptobot', 'bot'],
    on_success_callback=dag_success,
    on_failure_callback=dag_failed,
    default_args=default_args

) as my_dag:

    bot=PythonOperator(
        task_id='bot',
        python_callable=bot,
        default_args=default_args,
        retry_exponential_backoff=True,
        max_retry_delay=duration(minutes=5),
    )


    trigger_cryptobot_etl2 = TriggerDagRunOperator(
        task_id="trigger_cryptobot_etl2",
        trigger_dag_id="cryptobot_ETL2",
        retry_delay=duration(minutes=5),
        wait_for_completion=True,
        allowed_states=["success", "failed"],
        poke_interval=30,
    )

bot >> Label('Run bot') >> trigger_cryptobot_etl2