from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os

default_args = {
    'start_date': datetime(2024, 9, 10),
}

S3_BUCKET = 'dmf-s3-lhs'
S3_PREFIX = 'bit_files/'

BASE = os.path.expanduser('~/dmf/dataset/bitcoin')
hook = S3Hook('s3_default')

def upload_to_s3():
    files_to_upload = [f for f in os.listdir(BASE) if os.path.isfile(os.path.join(BASE, f))]
    
    if not files_to_upload:
        print("No files to upload")
        return

    for file_name in files_to_upload:
        local_file_path = os.path.join(BASE, file_name)
        s3_file_path = S3_PREFIX + file_name

        hook.load_file(filename=local_file_path, key=s3_file_path, bucket_name=S3_BUCKET, replace=True)

def remove_files():
    files_to_delete = [f for f in os.listdir(BASE) if os.path.isfile(os.path.join(BASE, f))]

    if not files_to_delete:
        print("No files to delete")
        return

    for file_name in files_to_delete:
        local_file_path = os.path.join(BASE, file_name)
        s3_file_path = S3_PREFIX + file_name

        try:
            if hook.check_for_key(key=s3_file_path, bucket_name=S3_BUCKET):
                os.remove(local_file_path)
        except Exception as e:
            print(f"Error")


with DAG(
    '03_upload_to_s3',
    default_args=default_args,
    schedule=timedelta(minutes=5),
    catchup=False
) as dag:

    # s3에 업로드
    t1 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

    # 업로드된 로컬 파일 삭제
    t2 = PythonOperator(
        task_id='remove_files',
        python_callable=remove_files
    )


    t1 >> t2