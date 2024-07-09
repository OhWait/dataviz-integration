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
    files = [f for f in os.listdir(folder_path) if 'commune' in f.lower() and f.endswith('.csv')]
    
    print(f"{files} found in {folder_path}")
    
    for file_name in files:
        file_path = os.path.join(folder_path, file_name)
        process_comm_file(file_path, year)

def process_comm_file(file_path, year):
    try:
        df = pd.read_csv(file_path, dtype=str)
        df.columns = df.columns.str.lower()

        required_columns = ['typecom', 'com', 'ncc', 'nccenr', 'libelle']
        optional_columns = ['reg', 'dep', 'ctcd', 'arr', 'tncc', 'can', 'comparent']

        if not set(required_columns).issubset(df.columns):
            print(f"File {file_path} does not contain all required columns and will be ignored.")
            return
        
        df = df.where(pd.notnull(df), None)
        df['annee'] = year
        
        for col in optional_columns:
            if col not in df.columns:
                df[col] = None

        columns_order = ['annee', 'typecom', 'com', 'reg', 'dep', 'ctcd', 'arr', 'tncc', 'ncc', 'nccenr', 'libelle', 'can', 'comparent']
        df = df.reindex(columns=columns_order, fill_value=None)

        clean_table('commune', year)
        insert_data(df, 'commune')

        os.remove(file_path)
        print(f"Successfully deleted file {file_path}.")
        
    except Exception as e:
        print(f"Failed to process file {file_path}: {e}")
        raise

with DAG(
    'import_commune_data',
    default_args=default_args,
    description='Import commune data from CSV files',
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
