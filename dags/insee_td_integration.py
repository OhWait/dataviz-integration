from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import os
import pandas as pd
import psycopg2
import pendulum
import tempfile

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
        
        cursor.execute(f"DELETE FROM insee.{table_name} WHERE millesime = %s", (millesime,))
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS insee.{table_name}_{millesime} 
            PARTITION OF insee.{table_name} 
            FOR VALUES FROM (%s) TO (%s)
            """, (millesime, int(millesime) + 1))
        conn.commit()

        cursor.close()
        conn.close()
        print(f"Successfully cleaned table insee.{table_name} for millesime {millesime}")

    except Exception as e:
        print(f"Error cleaning table insee.{table_name} for millesime {millesime}: {str(e)}")
        raise

def transform_file(file_path):
    try:
        print(f"Transforming file: {file_path}")
        # Extract table name and millesime from file name
        file_name = os.path.basename(file_path)
        parts = file_name.split('_')
        millesime = parts[-1].split('.')[0]

        # Open the file with ISO-8859-1 encoding and handle non UTF-8 characters
        with open(file_path, 'r', encoding='ISO-8859-1') as f:
            lines = f.read()

        # Replace non UTF-8 characters and write to a temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
        temp_file.write(lines.encode('utf-8', 'ignore').decode('utf-8', 'ignore'))
        temp_file.close()

        # Read the cleaned temporary file into a DataFrame
        df = pd.read_csv(temp_file.name, sep=';', dtype=str)

        # Log the first few rows of the DataFrame for debugging
        print(df.head())

        # Map columns based on file
        if 'NIVGEO' in df.columns and 'CODGEO' in df.columns and 'SEXE' in df.columns and 'AGEPYR10' in df.columns and 'NB' in df.columns:
            df = df[['NIVGEO', 'CODGEO', 'SEXE', 'AGEPYR10', 'NB']]
            df.columns = ['nivgeo', 'codgeo', 'sexe', 'agepyr10', 'nb']
        elif 'NIVEAU' in df.columns and 'CODGEO' in df.columns and 'C_AGEPYR' in df.columns and 'C_SEXE' in df.columns and 'NB' in df.columns:
            df = df[['NIVEAU', 'CODGEO', 'C_AGEPYR', 'C_SEXE', 'NB']]
            df.columns = ['nivgeo', 'codgeo', 'agepyr10', 'sexe', 'nb']
        elif 'NIVEAU' in df.columns and 'CODGEO' in df.columns and 'C_AGEPYR10' in df.columns and 'C_SEXE' in df.columns and 'NB' in df.columns:
            df = df[['NIVEAU', 'CODGEO', 'C_AGEPYR10', 'C_SEXE', 'NB']]
            df.columns = ['nivgeo', 'codgeo', 'agepyr10', 'sexe', 'nb']
        else:
            raise KeyError("Required columns not found in DataFrame")

        # Convert 'NB' column to numeric
        df['nb'] = df['nb'].str.replace(',', '.').astype(float)
        df = df.dropna(subset=['nivgeo', 'codgeo', 'agepyr10', 'sexe', 'nb'])

        # Add the millesime column
        df['millesime'] = millesime

        # Log the transformed DataFrame columns for debugging
        print(df.columns)
        print(df.head())

        # Save the transformed DataFrame to a temporary CSV file
        transformed_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        df.to_csv(transformed_file.name, index=False, columns=['millesime', 'nivgeo', 'codgeo', 'sexe', 'agepyr10', 'nb'])

        # Return the path to the transformed file
        return transformed_file.name

    except Exception as e:
        print(f"Error transforming file {file_path}: {str(e)}")
        raise
    finally:
        try:
            # Clean up temporary files if they exist
            if 'temp_file' in locals() and temp_file:
                os.remove(temp_file.name)
        except Exception as cleanup_error:
            print(f"Error cleaning up temporary files: {str(cleanup_error)}")

def process_file(file_path):
    try:
        transformed_file = transform_file(file_path)
        parts = file_path.split('_')
        table_name = f"insee.{parts[2].lower()}"
        
        print(f"Processing file: {transformed_file}")
        print(f"Table name: {table_name}")

        # Insert data into PostgreSQL using COPY
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        cursor = conn.cursor()

        # Read the transformed CSV file and insert data into PostgreSQL
        with open(transformed_file, 'r') as f:
            next(f)  # Skip header line
            cursor.copy_expert(f"COPY {table_name} FROM STDIN CSV HEADER", f)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"Data inserted into table {table_name} successfully.")

    except Exception as e:
        print(f"Error processing file {transformed_file}: {str(e)}")
        raise

def get_files(directory_path):
    return [os.path.join(directory_path, f) for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]

def create_tasks(dag):
    # Get the list of files to process dynamically
    files = get_files('/opt/airflow/upload/insee/td/')
    
    previous_task = None
    
    for file_path in files:
        file_name = os.path.basename(file_path)
        parts = file_name.split('_')
        
        if len(parts) < 3:
            print(f"Skipping file {file_path} because file name format is incorrect.")
            continue # Skip files that do not match expected format
        
        table_name = parts[2].lower()
        millesime = parts[-1].split('.')[0]
    
        prepare_task = PythonOperator(
            task_id=f'clean_{table_name}_{millesime}',
            python_callable=prepare_table,
            op_args=[millesime, table_name],
            dag=dag,
        )

        process_task = PythonOperator(
            task_id=f'process_{file_name}',
            python_callable=process_file,
            op_args=[file_path],
            dag=dag,
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
    schedule_interval=None,
    start_date=pendulum_days_ago(1),  # Start date of DAG execution
    catchup=False,
    tags=['INSEE', 'data_integration']
) as dag:
    start = PythonOperator(
        task_id='start',
        python_callable=lambda: print("Starting task creation..."),
    )

    create_tasks(dag)