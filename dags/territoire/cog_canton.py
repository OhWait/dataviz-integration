import sys
sys.path.append('/opt/airflow/plugins/territoire_utils')
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from territoire_utils import clean_table, insert_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

def process_files(folder_path, year):
    files = [f for f in os.listdir(folder_path) if 'canton' in f.lower() and f.endswith('.csv')]

    print(f"{files} found in {folder_path}")

    for file_name in files:
        file_path = os.path.join(folder_path, file_name)
        process_canton_file(file_path, year)

def process_canton_file(file_path, year):
    try:
        df = pd.read_csv(file_path, dtype=str)
        df.columns = df.columns.str.lower()

        required_columns = ['can', 'ncc', 'nccenr', 'libelle']

        if not set(required_columns).issubset(df.columns):
            print(f"File {file_path} does not contain all required columns and will be ignored.")
            return
        
        df = df[df['can'].notna() & (df['can'] != '')]
        df['annee'] = year
        
        columns_order = ['annee', 'can', 'dep', 'reg', 'compct', 'burcentral', 'tncc', 'ncc', 'nccenr', 'libelle', 'typect']
        df = df.reindex(columns=columns_order, fill_value=None)

        clean_table('canton', year)
        insert_data(df, 'canton')

        os.remove(file_path)
        print(f"Successfully deleted file {file_path}.")
        
    except Exception as e:
        print(f"Failed to process file {file_path}: {e}")
        raise

with DAG(
    'import_canton_data',
    default_args=default_args,
    description='Import canton data from CSV files',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:
    process_files_task = PythonOperator(
        task_id='process_files',
        python_callable=process_files,
        provide_context=True,
        op_args=['{{ dag_run.conf["folder_path"] }}', '{{ dag_run.conf["year"] }}'],
    )

    process_files_task
