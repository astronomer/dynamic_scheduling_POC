
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='new_home_sales',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['child_event'],
) as dag:

    print_event = BashOperator(
        task_id='print_event',
        bash_command="echo 'Event New Home Sales triggered!'",
    )
