from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import time
import requests
import os
import csv

default_args = {
    'start_date': datetime(2024, 9, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

def collect_upbit_data():
    upbit_url = 'https://api.upbit.com/v1/ticker'
    params = {'markets': 'KRW-BTC'}

    collected_data = []

    start_time = time.time()
    while time.time() - start_time < 60:
        res = requests.get(upbit_url, params=params)
        data = res.json()[0]

        csv_data = [data['market'], data['trade_date'], data['trade_time'], data['trade_price']]
        collected_data.append(csv_data)

        time.sleep(5)

    # file save
    now = datetime.now()
    file_name = now.strftime('%H%M%S') + '.csv'
    BASE = os.path.expanduser('~/dmf/dataset/bitcoin')
    file_path = f'{BASE}/{file_name}'

    os.makedirs(BASE, exist_ok=True)

    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(collected_data)

with DAG(
    '02_bitcoin_data' ,
    default_args=default_args,
    schedule='* * * * *',
    # schedule=timedelta(minutes=1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='collect_bitcoin',
        python_callable=collect_upbit_data
    )