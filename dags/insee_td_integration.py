from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
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

def process_file(file_path, pg_user, pg_password, pg_host, pg_port, pg_db):
    try:
        print(f"Processing file: {file_path} with user {pg_user} on db {pg_db}")
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

    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        raise

def get_files(folder_path):
    # List of files to process
    return [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

# Define the DAG
with DAG(
    'insee_td_integration',
    default_args=default_args,
    description='DAG for integrating INSEE data into PostgreSQL',
    schedule=None,
    start_date=pendulum_days_ago(1),  # Start date of DAG execution
    tags=['INSEE', 'data_integration']
) as dag:
    print(f"HELLO - INSEE_TD_INTEGRATION")
    def create_tasks():
        file_path = '/opt/airflow/upload/insee/td/'
        
        # Get the list of files to process dynamically
        files = get_files(file_path)
    
        print(f"[CREATE_TASK] Files found: {files}")

        # Create tasks dynamically for each file
        tasks = []
        for file in files:
            process_task = PythonOperator(
                task_id=f'process_{os.path.basename(file)}',
                python_callable=process_file,
                op_kwargs={
                    'file_path': file,
                    'pg_user': os.getenv('POSTGRES_USER'),
                    'pg_password': os.getenv('POSTGRES_PASSWORD'),
                    'pg_host': os.getenv('POSTGRES_HOST'),
                    'pg_port': os.getenv('POSTGRES_PORT'),
                    'pg_db': os.getenv('POSTGRES_DB')
                }
            )
            tasks.append(process_task)
        return tasks

    # Define a PythonOperator to create tasks dynamically
    dynamic_tasks_creation = PythonOperator(
        task_id='dynamic_tasks_creation',
        python_callable=create_tasks,
    )

    # Dummy operator to set dependencies
    from airflow.operators.empty import EmptyOperator
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Set dependencies
    start >> dynamic_tasks_creation >> end
