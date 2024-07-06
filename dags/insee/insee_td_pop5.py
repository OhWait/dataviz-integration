import sys
sys.path.append('/opt/airflow/plugins/insee_utils')
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from insee_utils import prepare_table, load_to_db
import pandas as pd
import os
import tempfile

table_name = 'pop5'

pg_columns = [
    'millesime',
    'nivgeo',
    'codgeo',
    'sexe',
    'ageq65',
    'tactr',
    'nb',
]

file_required = [
    'nivgeo',
    'codgeo',
    'sexe',
    'ageq65',
    'tactr',
    'nb',
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

def transform_clean_pop5(file_path, **kwargs):
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
        if 'NIVEAU' in df.columns and 'C_SEXE' in df.columns and 'C_AGEQ65' in df.columns and 'C_TACTR' in df.columns:
            df = df[['NIVEAU', 'CODGEO', 'C_SEXE', 'C_AGEQ65', 'C_TACTR', 'NB']]
            df.columns = ['nivgeo', 'codgeo', 'sexe', 'ageq65', 'tactr', 'nb']
        elif 'NIVGEO' in df.columns and 'SEXE' in df.columns and 'AGEQ65' in df.columns and 'TACTR' in df.columns:
            df = df[['NIVGEO', 'CODGEO', 'SEXE', 'AGEQ65', 'TACTR', 'NB']]
            df.columns = ['nivgeo', 'codgeo', 'sexe', 'ageq65', 'tactr', 'nb']
        else:
            raise KeyError("Required columns not found in DataFrame")

        # Transform
        df['nb'] = df['nb'].str.replace(',', '.').astype(float)
        df = df.dropna(subset=file_required)
        df['millesime'] = millesime
        df['ageq65'] = df['ageq65'].str.zfill(3)

        # Log the transformed DataFrame columns for debugging
        print(df.columns)
        print(df.head())

        # Save the transformed DataFrame to a temporary CSV file
        transformed_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        df.to_csv(transformed_file.name, index=False, columns=pg_columns)

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

with DAG(
    'insee_td_pop5',
    default_args=default_args,
    description='Transform, clean, map, and integrate data for pop5',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:
    prepare_table_task = PythonOperator(
        task_id='prepare_table_pop5',
        python_callable=prepare_table,
        op_args=['{{ dag_run.conf["millesime"] }}', table_name],
    )

    transform_clean_task = PythonOperator(
        task_id='transform_clean_pop5',
        python_callable=transform_clean_pop5,
        provide_context=True,
        op_args=['{{ dag_run.conf["file_path"] }}'],
    )

    load_task = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db,
        op_args=[
            '{{ task_instance.xcom_pull(task_ids="transform_clean_pop5") }}', 
            table_name
        ],
    )

    prepare_table_task >> transform_clean_task >> load_task