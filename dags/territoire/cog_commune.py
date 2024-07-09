import os
import re
import pandas as pd
import psycopg2
import tempfile
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

def find_and_process_files(**context):
    base_directory_path = '/opt/airflow/upload/territoire'
    
    # Rechercher les dossiers correspondant à 'cog_ensemble_YYYY_csv'
    for folder_name in os.listdir(base_directory_path):
        folder_path = os.path.join(base_directory_path, folder_name)
        if os.path.isdir(folder_path):
            year = extract_year_from_folder(folder_name)
            if year:
                process_files_in_folder(folder_path, year)

def extract_year_from_folder(folder_name):
    pattern = r'cog_ensemble_(\d{4})_csv'
    match = re.match(pattern, folder_name)
    if match:
        return match.group(1)
    return None

def process_files_in_folder(folder_path, year):
    files = [f for f in os.listdir(folder_path) if 'commune' in f.lower() and f.endswith('.csv')]
    for file_name in files:
        file_path = os.path.join(folder_path, file_name)
        process_file(file_path, year)

def process_file(file_path, year):
    try:
        df = pd.read_csv(file_path, dtype=str)
        
        required_columns = {'TYPECOM', 'COM', 'NCC', 'NCCENR', 'LIBELLE'}
        if not required_columns.issubset(df.columns):
            print(f"File {file_path} does not contain the required columns and will be ignored.")
            return
        
        print(df.head())
        
        # Transform
        df = df.dropna(subset=list(required_columns))
        df = df[df['TYPECOM'] == 'COM']  # Filtrer uniquement les lignes où TYPECOM est exactement 'COM'
        df = df.where(pd.notnull(df), None)
        df = df.rename(columns={"COM": "codgeo"})
        df['annee'] = year

        # Remapping columns to match the order of PostgreSQL table
        columns_order = ['annee', 'TYPECOM', 'codgeo', 'REG', 'DEP', 'CTCD', 'ARR', 'TNCC', 'NCC', 'NCCENR', 'LIBELLE', 'CAN', 'COMPARENT']
        df = df[columns_order]
        
        # Nettoyage de la table pour l'année spécifiée
        clean_table(year)
        
        # Insertion des données dans la base de données
        insert_data(df)

        # Si l'insertion réussit, supprimer le fichier
        os.remove(file_path)
        print(f"Successfully deleted file {file_path}.")
        
    except Exception as e:
        # Journaliser l'erreur et la remonter pour déclencher une nouvelle tentative ou un échec
        print(f"Failed to process file {file_path}: {e}")
        raise

def clean_table(year):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        cursor = conn.cursor()
        
        delete_sql = "DELETE FROM territoire.commune WHERE annee = %s"
        cursor.execute(delete_sql, (year,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"Successfully cleaned data for year {year} in PostgreSQL table.")
        
    except Exception as e:
        print(f"Failed to clean data for year {year} in PostgreSQL table: {e}")
        raise

def insert_data(df):
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
        df.to_csv(transformed_file.name, index=False)

        # Générer dynamiquement la commande COPY avec les colonnes du DataFrame
        copy_sql = """
            COPY territoire.commune
            FROM STDIN WITH CSV HEADER
            DELIMITER AS ','
            NULL AS 'None'
        """
        
        with open(transformed_file.name, 'r') as f:
            cursor.copy_expert(copy_sql, f)

        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"Successfully inserted data into PostgreSQL table.")
        
        # Suppression du fichier temporaire
        os.remove(transformed_file.name)
        print(f"Successfully deleted temporary file {transformed_file.name}.")
        
    except Exception as e:
        print(f"Failed to insert data into PostgreSQL table: {e}")
        raise

with DAG(
    'import_commune_data',
    default_args=default_args,
    description='Import commune data from CSV files',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:
    find_and_process_files_task = PythonOperator(
        task_id='find_and_process_files',
        python_callable=find_and_process_files,
        provide_context=True,
    )

    find_and_process_files_task
