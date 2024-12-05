from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import logging

def load_datasets(**kwargs):
    """Load raw datasets from CSV files"""
    clickup_path = kwargs['clickup_path']
    float_path = kwargs['float_path']
    
    clickup_df = pd.read_csv(clickup_path)
    float_df = pd.read_csv(float_path)
    
    return clickup_df, float_df

def clean_data(**kwargs):
    """Data cleaning and transformation"""
    ti = kwargs['ti']
    clickup_df, float_df = ti.xcom_pull(task_ids='load_datasets')
    
    # Standardize column names
    clickup_df.columns = [col.lower().replace(' ', '_') for col in clickup_df.columns]
    float_df.columns = [col.lower().replace(' ', '_') for col in float_df.columns]
    
    # Handle missing values
    clickup_df.dropna(subset=['client', 'project', 'hours'], inplace=True)
    float_df.dropna(subset=['client', 'project', 'estimated_hours'], inplace=True)
    
    # Type conversions
    clickup_df['date'] = pd.to_datetime(clickup_df['date'])
    float_df['start_date'] = pd.to_datetime(float_df['start_date'])
    float_df['end_date'] = pd.to_datetime(float_df['end_date'])
    
    return clickup_df, float_df

def create_schema(**kwargs):
    """Create dimensional schema in SQLite"""
    engine = create_engine('sqlite:///datawarehouse.db')
    
    with engine.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_project (
                project_key INTEGER PRIMARY KEY,
                client_name TEXT,
                project_name TEXT
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_employee (
                employee_key INTEGER PRIMARY KEY,
                name TEXT,
                role TEXT
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_task_tracking (
                task_key INTEGER PRIMARY KEY,
                project_key INTEGER,
                employee_key INTEGER,
                date DATE,
                hours REAL,
                billable BOOLEAN,
                task_description TEXT,
                FOREIGN KEY(project_key) REFERENCES dim_project(project_key),
                FOREIGN KEY(employee_key) REFERENCES dim_employee(employee_key)
            )
        """)
def load_dimensions(**kwargs):
    """Load dimension tables"""
    ti = kwargs['ti']
    clickup_df, float_df = ti.xcom_pull(task_ids='clean_data')
    engine = create_engine('sqlite:///datawarehouse.db')
    
    try:
        # Project dimension
        projects = pd.concat([
            clickup_df[['client', 'project']],
            float_df[['client', 'project']]
        ]).drop_duplicates()
        projects['project_key'] = range(1, len(projects) + 1)
        
        with engine.connect() as connection:
            projects.to_sql('dim_project', con=connection.connection, if_exists='replace', index=False)
        
        # Employee dimension
        employees = pd.concat([
            clickup_df[['name']],
            float_df[['name', 'role']]
        ]).drop_duplicates()
        employees['employee_key'] = range(1, len(employees) + 1)

        with engine.connect() as connection:
            employees.to_sql('dim_employee', con=connection.connection, if_exists='replace', index=False)
    
    except Exception as e:
        logging.error(f"Error loading dimensions: {e}")
        raise e

    return projects, employees


def load_fact_table(**kwargs):
    """Load fact table with combined data"""
    ti = kwargs['ti']
    clickup_df, float_df = ti.xcom_pull(task_ids='clean_data')
    projects, employees = ti.xcom_pull(task_ids='load_dimensions')
    engine = create_engine('sqlite:///datawarehouse.db')

    try:    
        # Merge datasets
        merged_df = pd.merge(
            clickup_df, float_df, 
            on=['name', 'project'], 
            how='outer'
        )
        merged_df.rename(columns={'client_x': 'client'}, inplace=True)

        
        # Join with dimension tables to get keys
        merged_df = merged_df.merge(
            projects[['client', 'project', 'project_key']], 
            on=['client', 'project']
        )
        merged_df = merged_df.merge(
            employees[['name', 'employee_key']], 
            on='name'
        )

        merged_df.rename(columns={'task_x': 'task'}, inplace=True)
        # Select and rename columns for fact table
        fact_table = merged_df[[
            'project_key', 
            'employee_key', 
            'date', 
            'hours', 
            'billable', 
            'task'
        ]].rename(columns={'task': 'task_description'})
        
        with engine.connect() as connection:
            fact_table.to_sql('fact_task_tracking', con=connection.connection, if_exists='replace', index=False)

    except Exception as e:
        logging.error(f"Error loading fact table: {e}")
        raise e
    
    
# Airflow DAG Configuration
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'data_warehouse_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    load_datasets_task = PythonOperator(
        task_id='load_datasets',
        python_callable=load_datasets,
        op_kwargs={
            'clickup_path': '/Users/sijibomijoshua/Downloads/Airflow Local/ClickUp - clickup.csv.csv',
            'float_path': '/Users/sijibomijoshua/Downloads/Airflow Local/Float - allocations.csv.csv'
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

    # Task Dependencies
    load_datasets_task >> clean_data_task >> create_schema_task >> load_dimensions_task >> load_fact_table_task