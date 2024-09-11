from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import os 
import random 
import csv


default_args = {
    'start_date' : datetime(2024,9,10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def generate_random_review():
    now = datetime.now()
    file_name = now.strftime('%H%M%S') + '.csv'
    BASE = os.path.expanduser('~/dmf/dataset/review')

    file_path = f'{BASE}/{file_name}'

    review_data = []
    for _ in range(20) : 
        movie_id = random.randint(1,1000)
        user_id = random.randint(1,100)
        rating = random.randint(1,5)
        review_data.append([user_id, movie_id, rating])

    os.makedirs(BASE, exist_ok=True)
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['user_id','movie_id','rating'])
        writer.writerows(review_data)



with DAG(
    "01_review_generate",
    default_args=default_args,
    schedule = '* * * * *',
    catchup = False,
) as dag:
    t1 = PythonOperator(
        task_id = 'gerate_review_data',
        python_callable=generate_random_review
    )