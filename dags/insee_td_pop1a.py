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

def transform_clean_pop1a(file_path, **kwargs):
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

        # Transform
        df['millesime'] = millesime
        df['agepyr10'] = df['agepyr10'].str.zfill(2)

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

def load_to_db(file_path, **kwargs):
    try:
        print("pop1a - load")
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        cursor = conn.cursor()

        with open(file_path, 'r') as f:
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
    print("INSEE TD POP1A script")

    prepare_table_task = PythonOperator(
        task_id='prepare_table_pop1a',
        python_callable=prepare_table,
        provide_context=True,
        op_args=['{{ dag_run.conf["millesime"] }}', '{{ dag_run.conf["table_name"] }}'],
    )

    transform_clean_task = PythonOperator(
        task_id='transform_clean_pop1a',
        python_callable=transform_clean_pop1a,
        provide_context=True,
        op_args=['{{ dag_run.conf["file_path"] }}'],
    )

    load_task = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db,
        provide_context=True,
        op_args=['{{ task_instance.xcom_pull(task_ids="transform_clean_pop1a") }}'],
    )

    prepare_table_task >> transform_clean_task >> load_task
