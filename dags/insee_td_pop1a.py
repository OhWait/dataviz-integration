from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import psycopg2
import os
import tempfile

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

def transform_clean_pop1a(file_name, **kwargs):
    try:
        directory_path = '/opt/airflow/upload/insee/td/'
        file_path = os.path.join(directory_path, file_name)
        
        # Transformation and cleaning logic
        df = pd.read_csv(file_path, sep=';', dtype=str)
        df.columns = [col.lower() for col in df.columns]

        required_columns = ['nivgeo', 'codgeo', 'sexe', 'agepyr10', 'nb']
        df = df[required_columns]
        df['nb'] = df['nb'].str.replace(',', '.').astype(float)
        df = df.dropna(subset=required_columns)
        
        millesime = file_name.split('_')[-1].split('.')[0]
        df['millesime'] = millesime

        cleaned_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        df.to_csv(cleaned_file.name, index=False)
        return cleaned_file.name

    except Exception as e:
        raise

def load_to_db(file_path, **kwargs):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        cursor = conn.cursor()

        with open(file_path, 'r') as f:
            next(f)
            cursor.copy_expert(f"COPY insee.pop1a FROM STDIN WITH CSV HEADER", f)
        
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        raise

with DAG(
    'insee_td_pop1a',
    default_args=default_args,
    description='Transform, clean, map, and integrate data for pop1a',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:
    transform_clean_task = PythonOperator(
        task_id='transform_clean_pop1a',
        python_callable=transform_clean_pop1a,
        provide_context=True,
        op_args=['{{ dag_run.conf["file_name"] }}'],
    )

    load_task = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db,
        provide_context=True,
        op_args=['{{ task_instance.xcom_pull(task_ids="transform_clean_pop1a") }}'],
    )

    transform_clean_task >> load_task
