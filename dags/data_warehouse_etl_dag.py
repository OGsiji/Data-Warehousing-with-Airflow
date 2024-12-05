import sys
import os

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
print("Current Python Path:", sys.path)


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_pipeline.extract import load_datasets
from src.data_pipeline.transform import clean_data
from src.data_pipeline.load import (
    create_schema, 
    load_dimensions, 
    load_fact_table
)
from src.config.config import Config


# Default DAG arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'email_on_failure': False,
    'email_on_retry': False,
}

# DAG definition
with DAG(
    'data_warehouse_etl_standard',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['data_warehouse', 'etl']
) as dag:
    # Define tasks
    load_datasets_task = PythonOperator(
        task_id='load_datasets',
        python_callable=load_datasets,
        op_kwargs={
            'clickup_path': Config.CLICKUP_PATH,
            'float_path': Config.FLOAT_PATH
        }
    )
    
    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )
    
    create_schema_task = PythonOperator(
        task_id='create_schema',
        python_callable=create_schema
    )
    
    load_dimensions_task = PythonOperator(
        task_id='load_dimensions',
        python_callable=load_dimensions
    )
    
    load_fact_table_task = PythonOperator(
        task_id='load_fact_table',
        python_callable=load_fact_table
    )

    # Task dependencies
    load_datasets_task >> clean_data_task >> create_schema_task >> load_dimensions_task >> load_fact_table_task