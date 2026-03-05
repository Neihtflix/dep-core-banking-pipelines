import boto3
import snowflake.connector
import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# ======= Configuration ==========
load_dotenv()
TABLES = ["customers", "accounts", "transactions"]

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_DOCKER")  # Use Docker endpoint for Airflow container
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DATA_DIR = os.getenv("MINIO_LOCAL_DATA_DIR", "/tmp/minio_downloads")

# Snowflake Configuration
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# ======= Functions ==========
def download_from_minio():
    """Download files from MinIO to local directory."""
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    
    local_files = {}
    for table in TABLES:
        prefix = f"{table}/"
        response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
        objects = response.get("Contents", [])
        local_files[table] = []
        for obj in objects:
            key = obj["Key"]
            local_path = os.path.join(LOCAL_DATA_DIR, os.path.basename(key))
            s3.download_file(MINIO_BUCKET, key, local_path)
            local_files[table] = local_files.get(table, []) + [local_path]
            local_files[table].append(local_path)
    return local_files

def load_into_snowflake(**kwargs):
    """Load data from local files into Snowflake."""
    local_files = kwargs['ti'].xcom_pull(task_ids='download_from_minio')
    if not local_files:
        print("No files to load into Snowflake.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor()
    
    for table, files in local_files.items():
        if not files:
            print(f"No files found for table {table}. Skipping.")
            continue

        for f in files:
            cur.execute(f"PUT file://{f} @%{table}")
            print(f"Loaded {f} into Snowflake stage @%{table}")

        copy_sql = f"""
            COPY INTO {table}
            FROM @%{table}
            FILE_FORMAT = (TYPE = PARQUET)
            ON_ERROR = 'CONTINUE'
        """
        cur.execute(copy_sql)
        print(f"Loaded data from stage @{table} into Snowflake table {table}")

    cur.close()
    conn.close()

# ======= Airflow DAG ==========
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='minio_to_snowflake',
    default_args=default_args,
    description='Transfer data from MinIO to Snowflake BRONZE tables',
    schedule="@hourly",  
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    
    download_task = PythonOperator(
        task_id='download_from_minio',
        python_callable=download_from_minio
    )
    
    load_task = PythonOperator(
        task_id='load_into_snowflake',
        python_callable=load_into_snowflake
    )
    
    download_task >> load_task