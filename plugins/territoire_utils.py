import os
import psycopg2
import tempfile

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
