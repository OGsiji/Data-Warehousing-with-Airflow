import pytest
import pandas as pd
from src.data_pipeline.transform import clean_data, merge_datasets

def create_mock_dataframes():
    """
    Create mock dataframes for testing
    """
    clickup_df = pd.DataFrame({
        'client': ['Client 1', 'Client 2'],
        'project': ['Website', 'App'],
        'name': ['John', 'Jane'],
        'date': ['2023-07-01', '2023-07-02'],
        'hours': [8, 6],
        'billable': [True, False]
    })
    
    float_df = pd.DataFrame({
        'client': ['Client 1', 'Client 3'],
        'project': ['Website', 'Marketing'],
        'name': ['John', 'Bob'],
        'start_date': ['2023-07-01', '2023-07-02'],
        'end_date': ['2023-07-31', '2023-07-31'],
        'estimated_hours': [40, 20]
    })
    
    return clickup_df, float_df

def test_clean_data_basic_transformation():
    """
    Test basic data cleaning transformations
    """
    clickup_df, float_df = create_mock_dataframes()
    
    # Simulate Airflow context
    class MockTaskInstance:
        def xcom_pull(self, task_ids):
            return clickup_df, float_df
    
    kwargs = {'ti': MockTaskInstance()}
    
    cleaned_clickup, cleaned_float = clean_data(**kwargs)
    
    # Check column names are normalized
    assert 'client' in cleaned_clickup.columns
    assert 'start_date' in cleaned_float.columns
    
    # Check date conversions
    assert pd.api.types.is_datetime64_any_dtype(cleaned_clickup['date'])
    assert pd.api.types.is_datetime64_any_dtype(cleaned_float['start_date'])

def test_merge_datasets():
    """
    Test dataset merging logic
    """
    clickup_df, float_df = create_mock_dataframes()
    
    merged_df = merge_datasets(clickup_df, float_df)
    
    # Check merge results
    assert not merged_df.empty
    assert len(merged_df) >= len(clickup_df)
    assert len(merged_df) >= len(float_df)
    
    # Verify common columns are preserved
    expected_columns = [
        'client', 'project', 'name', 
        'date', 'hours', 'billable'
    ]
    for col in expected_columns:
        assert col in merged_df.columns