from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

def identify_and_trigger_dags(**context):
    directory_path = '/opt/airflow/upload/insee/td/'
    files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
    
    previous_task = None
    
    for file_name in files:
        parts = file_name.split('_')
        if len(parts) < 3:
            continue
        
        table_name = parts[2].lower()
        millesime = parts[-1].split('.')[0]
        full_path = os.path.join(directory_path, file_name)
        context['task_instance'].xcom_push(key=file_name, value=full_path)

        trigger_dag_run = TriggerDagRunOperator(
            task_id=f'trigger_{table_name}_dag_{millesime}',
            trigger_dag_id=f'insee_td_{table_name}',
            conf={
                'file_path': f'{directory_path}{file_name}',
                'millesime': millesime,
                'table_name': table_name,
            },
            wait_for_completion=True,
            dag=context['dag'],  # Ajout du paramètre dag
        )
        trigger_dag_run.execute(context)
        
        if previous_task:
            previous_task >> trigger_dag_run
        else:
            start >> trigger_dag_run
        
        previous_task = trigger_dag_run

with DAG(
    'insee_td_dispatcher',
    default_args=default_args,
    description='Identify table and trigger corresponding DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:
    start = PythonOperator(
        task_id='start',
        python_callable=lambda: print("Starting task creation..."),
    )
    
    PythonOperator(
        task_id='identify_and_trigger_dags',
        python_callable=identify_and_trigger_dags,
        provide_context=True,
    )
