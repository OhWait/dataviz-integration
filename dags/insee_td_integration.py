from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

def process_file(file_path, pg_user, pg_password, pg_host, pg_port, pg_db):
    # Extract table name and millesime from file name
    file_name = os.path.basename(file_path)
    parts = file_name.split('_')
    table_name = parts[2].lower()
    millesime = parts[-1].split('.')[0]
    
    # Read the file
    df = pd.read_csv(file_path, sep=';', dtype=str)
    
    # Map columns based on file
    if 'LIBGEO' in df.columns:
        df = df[['NIVGEO', 'CODGEO', 'SEXE', 'AGEPYR10', 'NB']]
        df.columns = ['nivgeo', 'codgeo', 'sexe', 'agepyr10', 'nb']
    else:
        df.columns = ['nivgeo', 'codgeo', 'agepyr10', 'sexe', 'nb']
    
    # Convert 'NB' column to numeric
    df['nb'] = df['nb'].str.replace(',', '.').astype(float)
    
    # Add the millesime column
    df['millesime'] = millesime
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=pg_db,
        user=pg_user,
        password=pg_password,
        host=pg_host,
        port=pg_port
    )
    cursor = conn.cursor()
    
    # Insert data into PostgreSQL
    for _, row in df.iterrows():
        cursor.execute(
            f"""
            INSERT INTO insee.{table_name} (millesime, nivgeo, codgeo, agepyr10, sexe, nb)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, tuple(row)
        )
    
    conn.commit()
    cursor.close()
    conn.close()

def get_files():
    # List of files to process
    return [os.path.join('/upload/insee/td', f) for f in os.listdir('/upload/insee/td')]

# Define the DAG
with DAG(
    'insee_data_integration',
    default_args=default_args,
    description='DAG for integrating INSEE data into PostgreSQL',
    schedule_interval=None,  # This DAG is triggered manually or by sensors
    start_date=days_ago(1),  # Start date of DAG execution
    tags=['INSEE', 'data_integration']
) as dag:
    
    # Define a FileSensor to monitor the directory
    monitor_directory_task = FileSensor(
        task_id='monitor_insee_directory',
        filepath='/upload/insee/td',
        poke_interval=300,  # Check every 5 minutes
        timeout=600,  # Timeout after 10 minutes
    )

    # Get the list of files to process dynamically
    files = get_files()

    # Create tasks dynamically for each file
    for file in files:
        process_task = PythonOperator(
            task_id=f'process_{os.path.basename(file)}',
            python_callable=process_file,
            op_kwargs={
                'file_path': file,
                'pg_user': 'postgres',
                'pg_password': 'postgres',
                'pg_host': 'host.docker.internal',
                'pg_port': 5432,
                'pg_db': 'your_existing_db'
            },
            dag=dag
        )

        # Set dependencies
        monitor_directory_task >> process_task
