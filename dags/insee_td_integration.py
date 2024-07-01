from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import os
import pandas as pd
import psycopg2
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

def pendulum_days_ago(days):
    return pendulum.today('UTC').add(days=-days)

def prepare_table(millesime, table_name):
    try:
        print(f"Cleaning table insee.{table_name} for millesime {millesime}")
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        cursor = conn.cursor()

        # Clean the table for the given millesime
        cursor.execute(f"DELETE FROM insee.{table_name} WHERE millesime = %s", (millesime,))
        
        # Create partition for the millesime
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS insee.{table_name}_{millesime} PARTITION OF insee.{table_name}
            FOR VALUES FROM ('{millesime}') TO ('{int(millesime) + 1}')
        """)

        cursor.close()
        conn.close()
        print(f"Successfully cleaned table insee.{table_name} for millesime {millesime}")

    except Exception as e:
        print(f"Error cleaning table insee.{table_name} for millesime {millesime}: {str(e)}")
        raise

def process_file(file_path):
    try:
        print(f"Processing file: {file_path}")
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
            df.columns = ['nivgeo', 'codgeo', 'agepyr10', 'sexe', 'nb']
        elif 'REG' in df.columns and 'DEP' in df.columns:
            df = df[['NIVEAU', 'CODGEO', 'C_AGEPYR', 'C_SEXE', 'NB']]
            df.columns = ['nivgeo', 'codgeo', 'agepyr10', 'sexe', 'nb']
        else:
            df = df[['NIVEAU', 'CODGEO', 'C_AGEPYR10', 'C_SEXE', 'NB']]
            df.columns = ['nivgeo', 'codgeo', 'agepyr10', 'sexe', 'nb']
        
        # Convert 'NB' column to numeric
        df['nb'] = df['nb'].str.replace(',', '.').astype(float)
        
        # Add the millesime column
        df['millesime'] = millesime
        
        # Split the dataframe into batches of 1000 rows
        batches = [df.iloc[i:i + 1000] for i in range(0, len(df), 1000)]
        
        # Create tasks for each batch
        for idx, batch in enumerate(batches):
            process_batch(batch, table_name, millesime, idx)

        # Remove the file after successful processing
        os.remove(file_path)
        print(f"Successfully removed file: {file_path}")

    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        raise

def process_batch(batch, table_name, millesime, batch_idx):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        cursor = conn.cursor()

        tuples = [(
            row['millesime'], 
            row['nivgeo'], 
            row['codgeo'], 
            row['agepyr10'], 
            row['sexe'], 
            row['nb']
        ) for index, row in batch.iterrows()]

        cursor.executemany(
            f"""
            INSERT INTO insee.{table_name} (millesime, nivgeo, codgeo, agepyr10, sexe, nb)
            VALUES 
            """, tuples
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully processed and inserted batch {batch_idx} for millesime {millesime}")

    except Exception as e:
        print(f"Error processing batch {batch_idx} for millesime {millesime}: {str(e)}")
        print(
            f"""
            INSERT INTO insee.{table_name} (millesime, nivgeo, codgeo, agepyr10, sexe, nb)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, 
            tuples
        )
        raise

def get_files(directory_path):
    return [os.path.join(directory_path, f) for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]

def create_tasks(dag):
    # Get the list of files to process dynamically
    files = get_files('/opt/airflow/upload/insee/td/')
    
    previous_task = None
    
    for file in files:
        file_name = os.path.basename(file)
        parts = file_name.split('_')
        table_name = parts[2].lower()
        millesime = parts[-1].split('.')[0]
        
        prepare_task = PythonOperator(
            task_id=f'clean_{table_name}_{millesime}',
            python_callable=prepare_table,
            op_args=[millesime, table_name],
            dag=dag
        )

        process_task = PythonOperator(
            task_id=f'process_{file_name}',
            python_callable=process_file,
            op_args=[file],
            dag=dag,
            pool='insee_processing_pool'
        )
        
        if previous_task:
            previous_task >> prepare_task
        else:
            start >> prepare_task
        
        prepare_task >> process_task
        previous_task = process_task

# Define the DAG
with DAG(
    'insee_td_integration',
    default_args=default_args,
    description='DAG for integrating INSEE data into PostgreSQL',
    schedule=None,
    start_date=pendulum_days_ago(1),  # Start date of DAG execution
    catchup=False,
    tags=['INSEE', 'data_integration']
) as dag:
    start = PythonOperator(
        task_id='start',
        python_callable=lambda: print("Starting task creation..."),
    )

    create_tasks(dag)
