import pytest
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.config.config import Config
from src.data_pipeline.load import (
    create_schema, 
    load_dimensions, 
    load_fact_table
)
from src.models.dimensional_models import Base, DimProject, DimEmployee, FactTaskTracking

class MockTaskInstance:
    """
    Mock Airflow TaskInstance for testing
    """
    def xcom_pull(self, task_ids):
        # Create mock dataframes for testing
        clickup_df = pd.DataFrame({
            'client': ['Client 1', 'Client 1'],
            'project': ['Website', 'Mobile App'],
            'name': ['John Doe', 'Jane Smith'],
            'date': ['2023-07-01', '2023-07-02'],
            'hours': [8, 6],
            'billable': [True, False],
            'task': ['Development', 'Design']
        })
        
        float_df = pd.DataFrame({
            'client': ['Client 1', 'Client 2'],
            'project': ['Website', 'Marketing'],
            'name': ['John Doe', 'Bob Johnson'],
            'start_date': ['2023-07-01', '2023-07-02'],
            'end_date': ['2023-07-31', '2023-07-31'],
            'estimated_hours': [40, 20],
            'role': ['Developer', 'Manager']
        })
        
        return clickup_df, float_df

@pytest.fixture
def test_engine():
    """
    Create a test database engine
    """
    # Use an in-memory SQLite database for testing
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return engine

@pytest.fixture
def test_session(test_engine):
    """
    Create a test database session
    """
    Session = sessionmaker(bind=test_engine)
    return Session()

def test_create_schema(test_engine):
    """
    Test schema creation
    """
    # Create schema
    result = create_schema()
    
    # Verify tables are created
    inspector = test_engine.dialect.get_inspector(test_engine)
    table_names = inspector.get_table_names()
    
    assert result is True
    assert 'dim_project' in table_names
    assert 'dim_employee' in table_names
    assert 'fact_task_tracking' in table_names

def test_load_dimensions(test_session):
    """
    Test loading of dimension tables
    """
    # Mock Airflow context
    kwargs = {'ti': MockTaskInstance()}
    
    # Load dimensions
    load_dimensions(**kwargs)
    
    # Verify project dimensions
    projects = test_session.query(DimProject).all()
    assert len(projects) > 0
    
    # Verify employee dimensions
    employees = test_session.query(DimEmployee).all()
    assert len(employees) > 0
    
    # Check specific dimension entries
    project_names = [p.project_name for p in projects]
    assert 'Website' in project_names
    assert 'Mobile App' in project_names
    
    employee_names = [e.name for e in employees]
    assert 'John Doe' in employee_names
    assert 'Jane Smith' in employee_names

def test_load_fact_table(test_session):
    """
    Test loading of fact table
    """
    # First, load dimensions to satisfy foreign key constraints
    kwargs = {'ti': MockTaskInstance()}
    load_dimensions(**kwargs)
    
    # Load fact table
    load_fact_table(**kwargs)
    
    # Verify fact table entries
    fact_entries = test_session.query(FactTaskTracking).all()
    assert len(fact_entries) > 0
    
    # Check fact table data integrity
    for entry in fact_entries:
        assert entry.project_id is not None
        assert entry.employee_id is not None
        assert entry.hours is not None
        assert entry.date is not None

def test_load_dimensions_duplicate_handling(test_session):
    """
    Test handling of duplicate dimension entries
    """
    # Mock Airflow context
    kwargs = {'ti': MockTaskInstance()}
    
    # Load dimensions twice
    load_dimensions(**kwargs)
    load_dimensions(**kwargs)
    
    # Verify no duplicate entries
    projects = test_session.query(DimProject).all()
    employees = test_session.query(DimEmployee).all()
    
    # Check for unique project names
    project_names = [p.project_name for p in projects]
    assert len(project_names) == len(set(project_names))
    
    # Check for unique employee names
    employee_names = [e.name for e in employees]
    assert len(employee_names) == len(set(employee_names))

def test_load_fact_table_foreign_key_integrity(test_session):
    """
    Test foreign key integrity in fact table
    """
    # Load dimensions first
    kwargs = {'ti': MockTaskInstance()}
    load_dimensions(**kwargs)
    
    # Load fact table
    load_fact_table(**kwargs)
    
    # Verify foreign key relationships
    fact_entries = test_session.query(FactTaskTracking).all()
    for entry in fact_entries:
        # Verify project exists
        project = test_session.query(DimProject).get(entry.project_id)
        assert project is not None
        
        # Verify employee exists
        employee = test_session.query(DimEmployee).get(entry.employee_id)
        assert employee is not None

def test_error_handling():
    """
    Test error scenarios in loading process
    """
    # Create mock with invalid data
    class InvalidMockTaskInstance:
        def xcom_pull(self, task_ids):
            return None, None
    
    kwargs = {'ti': InvalidMockTaskInstance()}
    
    # These should raise exceptions
    with pytest.raises(Exception):
        load_dimensions(**kwargs)
    
    with pytest.raises(Exception):
        load_fact_table(**kwargs)