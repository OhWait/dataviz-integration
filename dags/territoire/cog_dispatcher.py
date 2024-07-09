import os
import re
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

def extract_year_from_folder(folder_name):
    pattern = r'cog_ensemble_(\d{4})_csv'
    match = re.match(pattern, folder_name)
    if match:
        year = match.group(1)
        if year.isdigit() and len(year) == 4:
            return year
    return None

def find_and_trigger_subdag(**context):
    base_directory_path = '/opt/airflow/upload/territoire'

    for folder_name in os.listdir(base_directory_path):
        folder_path = os.path.join(base_directory_path, folder_name)
        if os.path.isdir(folder_path):
            year = extract_year_from_folder(folder_name)
            trigger_dags(folder_path, year, context)

def trigger_dags(folder_path, year, context):
    if (len(year) == 0):
        return

    dags = ['commune', 'canton', 'arrondissement']

    for dag in dags:
        trigger = TriggerDagRunOperator(
            task_id=f'trigger_{dag}_dag_{year}',
            trigger_dag_id=f'import_{dag}_data',
            conf={
                'year': year,
                'folder_path': folder_path
            },
            wait_for_completion=True,
            dag=context['dag'],
        )
        trigger.execute(context=context)
        
with DAG(
    'cog_dispatcher',
    default_args=default_args,
    description='Import commune and canton data from CSV files',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:
    find_and_trigger_subdag_task = PythonOperator(
        task_id='find_and_trigger_subdag',
        python_callable=find_and_trigger_subdag,
        provide_context=True,
    )
