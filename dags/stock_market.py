from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from datetime import datetime
from stock_price.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, _load_csv_to_postgres
import requests

SYMBOL = "aapl"

@dag(
    start_date = datetime(2024,1,1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
    on_success_callback = SlackNotifier(
        slack_conn_id='Slack',
        text = "The DAG stock_market has succeded",
        channel = 'testproject'
    ),
    on_failure_callback = SlackNotifier(
        slack_conn_id='Slack',
        text = "The DAG stock_market has failed",
        channel = 'testproject'
    )
)

def stock_market():
    @task.sensor(poke_interval=30, timeout=300, mode='poke')

    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection("stock_api")
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done = condition, xcom_value=url)

    get_stock_prices = PythonOperator(
            task_id= 'get_stock_prices',
            python_callable = _get_stock_prices,
            op_kwargs={"url": '{{ task_instance.xcom_pull(task_ids="is_api_available") }}', "symbol": SYMBOL}
            )
    # @task
    # def get_stock_prices(url, symbol):
    #     return _get_stock_prices(url=url, symbol=symbol)

    store_prices = PythonOperator(
        task_id = 'store_prices',
        python_callable= _store_prices,
        op_kwargs = {"stock" : '{{task_instance.xcom_pull(task_ids="get_stock_prices")}}'}
    )

    format_prices = DockerOperator(
        task_id = 'format_prices',
        image = 'airflow/stock_price',
        container_name = 'format_price',
        api_version = 'auto',
        auto_remove = True,
        docker_url = 'tcp://docker-proxy:2375',
        network_mode ='container:spark-master',
        tty = True,
        xcom_all = False,
        mount_tmp_dir=False,
        environment = {'SPARK_APPLICATION_ARGS': '{{task_instance.xcom_pull(task_ids="store_prices")}}'}
    )

    get_formatted_csv = PythonOperator(
        task_id = 'get_formatted_csv',
        python_callable = _get_formatted_csv,
        op_kwargs = {
            'path' : '{{task_instance.xcom_pull(task_ids="store_prices")}}'
        }
    )

    load_csv_to_postgres = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=_load_csv_to_postgres,
        op_kwargs={
            'path' : '{{task_instance.xcom_pull(task_ids="store_prices")}}',
            'table_name': 'appl_stock'
        }
    )

    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_csv_to_postgres

stock_market()