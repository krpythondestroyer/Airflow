from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator



with DAG(
    dag_id = '00_intro',
    description = 'first DAG',
    schedule = timedelta(minutes=1),
    # schedule = '* * * * *',
    catchup = False,
    start_date = datetime(2024,9,10),

) as dag:


    t1 = BashOperator(
        task_id = 'print_date',
        bash_command = 'date',

    )
    t2 = BashOperator(
        task_id = 'sleep',
        bash_command = 'sleep 5',
        
    )

    t3 = BashOperator(
        task_id = 'print',
        bash_command = 'echo "hello"',

    )


t1 >> t2 >> t3