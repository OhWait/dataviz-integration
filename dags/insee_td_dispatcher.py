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

def identify_table(**context):
    directory_path = '/opt/airflow/upload/insee/td/'
    files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]

    for file_name in files:
        parts = file_name.split('_')
        if len(parts) < 3:
            continue
        
        table_name = parts[2].lower()
        context['task_instance'].xcom_push(key='file_name', value=file_name)

        if table_name == 'pop1a':
            return 'trigger_pop1a_dag'
        elif table_name == 'pop1b':
            return 'trigger_pop1b_dag'

with DAG(
    'insee_td_dispatcher',
    default_args=default_args,
    description='Identify table and trigger corresponding DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:
    identify_task = PythonOperator(
        task_id='identify_table',
        python_callable=identify_table,
        provide_context=True,
    )

    trigger_pop1a_dag = TriggerDagRunOperator(
        task_id='trigger_pop1a_dag',
        trigger_dag_id='transform_clean_pop1a_dag',
        conf={"file_name": "{{ task_instance.xcom_pull(task_ids='identify_table', key='file_name') }}"},
        wait_for_completion=True
    )

    trigger_pop1b_dag = TriggerDagRunOperator(
        task_id='trigger_pop1b_dag',
        trigger_dag_id='transform_clean_pop1b_dag',
        conf={"file_name": "{{ task_instance.xcom_pull(task_ids='identify_table', key='file_name') }}"},
        wait_for_completion=True
    )

    identify_task >> [trigger_pop1a_dag, trigger_pop1b_dag]
