import os
import re
import pandas as pd
import psycopg2
import tempfile
import shutil
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

territoires = [
    {
        'table_name': 'commune',
        'required_columns': ['typecom', 'com', 'ncc', 'nccenr', 'libelle'],
        'pg_columns': ['annee', 'typecom', 'com', 'reg', 'dep', 'ctcd', 'arr', 'tncc', 'ncc', 'nccenr', 'libelle', 'can', 'comparent'],
    },
    {
        'table_name': 'canton',
        'required_columns': ['can', 'ncc', 'nccenr', 'libelle'],
        'pg_columns': ['annee', 'can', 'dep', 'reg', 'compct', 'burcentral', 'tncc', 'ncc', 'nccenr', 'libelle', 'typect'],
    },
    {
        'table_name': 'arrondissement',
        'required_columns': ['arr', 'ncc', 'nccenr', 'libelle'],
        'pg_columns': ['annee', 'arr', 'dep', 'reg', 'cheflieu', 'tncc', 'ncc', 'nccenr', 'libelle'],
    },
    {
        'table_name': 'departement',
        'required_columns': ['dep', 'ncc', 'nccenr', 'libelle'],
        'pg_columns': ['annee', 'dep', 'reg', 'cheflieu', 'tncc', 'ncc', 'nccenr', 'libelle'],
    },
    {
        'table_name': 'region',
        'required_columns': ['reg', 'ncc', 'nccenr', 'libelle'],
        'pg_columns': ['annee', 'reg', 'cheflieu', 'tncc', 'ncc', 'nccenr', 'libelle'],
    }
]

def get_folders(**kwargs):
    base_directory_path = '/opt/airflow/upload/territoire'
    folders = []

    for folder_name in os.listdir(base_directory_path):
        folder_path = os.path.join(base_directory_path, folder_name)

        if os.path.isdir(folder_path):
            folders.append(folder_path)
            
    return folders

def extract_year_from_folder(folder_name):
    pattern = r'cog_ensemble_(\d{4})_csv'
    match = re.match(pattern, folder_name)
    if match:
        return match.group(1)
    return None

def process_file_by_territoire(territoire, folder_path, year, year_status):
    files = [f for f in os.listdir(folder_path) if territoire['table_name'] in f.lower() and f.endswith('.csv')]

    print(f"{files} found in {folder_path}")

    for file_name in files:
        file_path = os.path.join(folder_path, file_name)
        result = process_file(territoire, file_path, year, year_status)
        
        print(f"Statut : {result}, process_file_by_territoire({territoire}, {year})")
        year_status[year] = result

def process_file(territoire, file_path, year, year_status):
    try:
        df = pd.read_csv(file_path, dtype=str)
        df.columns = df.columns.str.lower()
        
        required_columns = territoire['required_columns']
        table_name = territoire['table_name']
        pg_columns = territoire['pg_columns']

        if not set(required_columns).issubset(df.columns):
            print(f"File {file_path} does not contain all required columns and will be ignored.")
            return False
        
        df['annee'] = year
        
        df = df.dropna(subset=required_columns)
        df = df.reindex(columns=pg_columns, fill_value=None)

        clean_table(table_name, year)
        insert_data(df, table_name)

        os.remove(file_path)
        print(f"Successfully deleted file {file_path}.")
        
        return True
        
    except Exception as e:
        print(f"Failed to process file {file_path}: {e}")
        return False

def clean_table(table_name, year):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        cursor = conn.cursor()
        
        delete_sql = f"DELETE FROM territoire.{table_name} WHERE annee = %s"
        cursor.execute(delete_sql, (year,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"Successfully cleaned data for year {year} in PostgreSQL table {table_name}.")
        
    except Exception as e:
        print(f"Failed to clean data for year {year} in PostgreSQL table {table_name}: {e}")
        raise

def insert_data(df, table_name):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        cursor = conn.cursor()
    
        transformed_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        df.to_csv(transformed_file.name, index=False, na_rep='')

        print(df.head())

        copy_sql = f"""
            COPY territoire.{table_name}
            FROM STDIN WITH CSV HEADER
            DELIMITER AS ','
            NULL AS ''
        """

        with open(transformed_file.name, 'r') as f:
            cursor.copy_expert(copy_sql, f)

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully inserted data into PostgreSQL table {table_name}.")
        
        # Suppression du fichier temporaire
        os.remove(transformed_file.name)
        print(f"Successfully deleted temporary file {transformed_file.name}.")
        
    except Exception as e:
        print(f"Failed to insert data into PostgreSQL table {table_name}: {e}")
        raise

def find_and_trigger_subdag(**kwargs):
    folders = get_folders()
    year_status = {}

    for folder in folders:
        folder_name = os.path.basename(folder)
        year = extract_year_from_folder(folder_name)
        
        if year and year.isdigit() and len(year) == 4:
            print(f"{year} found, processing files in {folder}")
            year_status[year] = True
            for territoire in territoires:
                process_file_by_territoire(territoire, folder, year, year_status)
        
            print(f"Status : {year_status}")
                
            # Check if all tasks for the year were successful
            if year_status.get(year):
                # Remove the directory
                try:
                    shutil.rmtree(folder)
                    print(f"Successfully deleted directory {folder}")
                except OSError as e:
                    print(f"Failed to delete directory {folder}: {e}")
        
        else:
            print(f"Year is missing or invalid in {folder}, skipping")

with DAG(
    'cog_integration',
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

    find_and_trigger_subdag_task
