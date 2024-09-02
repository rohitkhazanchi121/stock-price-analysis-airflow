from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowNotFoundException
import requests
import json
from minio import Minio
from io import BytesIO
import pandas as pd


BUCKET_NAME = 'stock-market'

def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint = minio.extra_dejson['endpoint_url'].split("//")[1],
        access_key = minio.login,
        secret_key = minio.password,
        secure=False
    )
    return client


def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])

    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    
    client = _get_minio_client()

    

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')

    objw = client.put_object(
        bucket_name = BUCKET_NAME,
        object_name = f'{symbol}/prices.json',
        data = BytesIO(data),
        length= len(data)
    )

    return f'{objw.bucket_name}/{symbol}'

def _get_formatted_csv(path):

    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"

    objects = client.list_objects(BUCKET_NAME, prefix = prefix_name, recursive=True)

    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
        
    raise AirflowNotFoundException("the csv file does not exist")


# Function to load CSV into PostgreSQL
def _load_csv_to_postgres(path: str, table_name: str):
    
    file_path = _get_formatted_csv(path)

    client = _get_minio_client()

    if len(file_path)>0:
        csv_object = client.get_object(BUCKET_NAME, file_path)

        body = csv_object.read()
        df = pd.read_csv(BytesIO(body))

        # Close the response
        csv_object.close()
        csv_object.release_conn()


    
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres')  # Ensure you have this connection in Airflow
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    #cursor.execute(f"DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (timestamp1 TEXT, close numeric, high numeric, open numeric, volume numeric, date text)")

    # Insert DataFrame into PostgreSQL
    for i, row in df.iterrows():
        print(i, row)
        # Adjust this SQL based on your table schema
        cursor.execute(
            f"INSERT INTO {table_name} (timestamp1, close , high , open, volume, date) VALUES (%s, %s,%s,%s,%s,%s)",
            (row['timestamp'], row['close'], row['high'], row['open'], row['volume'],row['date'])
        )
    
    conn.commit()
    cursor.close()
    conn.close()
