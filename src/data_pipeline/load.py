import pandas as pd
import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from src.config.config import Config
from src.models.dimensional_models import (
    Base, DimProject, DimEmployee, FactTaskTracking
)

def create_schema(**kwargs):
    """
    Create database schema using SQLAlchemy ORM
    """
    try:
        # Create engine
        engine = create_engine(Config.DB_CONNECTION_STRING)
        
        # Create all tables defined in Base
        Base.metadata.create_all(engine)
        
        # Verify schema creation
        inspector = inspect(engine)
        created_tables = inspector.get_table_names()
        
        logging.info(f"Created tables: {created_tables}")
        
        return True
    except Exception as e:
        logging.error(f"Schema creation error: {e}")
        raise

def load_dimensions(**kwargs):
    """
    Load dimension tables with sophisticated loading strategy
    """
    try:
        # Retrieve cleaned datasets
        ti = kwargs['ti']
        clickup_df, float_df = ti.xcom_pull(task_ids='clean_data')
        
        # Create engine and session
        engine = create_engine(Config.DB_CONNECTION_STRING)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Process projects
        unique_projects = pd.concat([
            clickup_df[['client', 'project']],
            float_df[['client', 'project']]
        ]).drop_duplicates()
        
        # Bulk insert projects
        project_entities = [
            DimProject(
                client_name=row['client'], 
                project_name=row['project']
            ) for _, row in unique_projects.iterrows()
        ]
        
        # Process employees
        unique_employees = pd.concat([
            clickup_df[['name']],
            float_df[['name', 'role']]
        ]).drop_duplicates()
        
        employee_entities = [
            DimEmployee(
                name=row['name'], 
                role=row.get('role', 'Unspecified')
            ) for _, row in unique_employees.iterrows()
        ]
        
        # Bulk insert and commit
        session.bulk_save_objects(project_entities)
        session.bulk_save_objects(employee_entities)
        session.commit()
        
        logging.info(f"Loaded {len(project_entities)} projects")
        logging.info(f"Loaded {len(employee_entities)} employees")
        
        return True
    except Exception as e:
        logging.error(f"Dimension loading error: {e}")
        raise

def load_fact_table(**kwargs):
    """
    Load fact table with comprehensive data integration
    """
    try:
        # Retrieve datasets
        ti = kwargs['ti']
        clickup_df, float_df = ti.xcom_pull(task_ids='clean_data')
        
        # Create engine and session
        engine = create_engine(Config.DB_CONNECTION_STRING)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Merge datasets
        merged_df = pd.merge(
            clickup_df, 
            float_df, 
            on=['name', 'project'], 
            how='outer'
        )
        
        # Prepare fact table entities
        fact_entities = [
            FactTaskTracking(
                project_id=row.get('project_id'),
                employee_id=row.get('employee_id'),
                date=row['date'],
                hours=row['hours'],
                billable=row.get('billable', 'No').lower() == 'yes',
                task_description=row.get('task', 'Unspecified')
            ) for _, row in merged_df.iterrows()
        ]
        
        # Bulk insert and commit
        session.bulk_save_objects(fact_entities)
        session.commit()
        
        logging.info(f"Loaded {len(fact_entities)} fact table records")
        
        return True
    except Exception as e:
        logging.error(f"Fact table loading error: {e}")
        raise