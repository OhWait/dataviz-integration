import pandas as pd
import psycopg2
import os

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

def load_to_db(file_path, table_name):
    try:
        print(f"Loading data into table insee.{table_name} from path {file_path}")
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT')
        )
        cursor = conn.cursor()

        with open(file_path, 'r') as f:
            cursor.copy_expert(f"COPY insee.{table_name} FROM STDIN WITH CSV HEADER", f)

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error loading data into table insee.{table_name}: {str(e)}")
        raise
