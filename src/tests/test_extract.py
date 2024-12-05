import pytest
import pandas as pd
from src.data_pipeline.extract import load_datasets
from unittest.mock import patch

def test_load_datasets_success():
    """
    Test successful dataset loading
    """
    # Create mock CSV files
    with patch('pandas.read_csv') as mock_read_csv:
        mock_clickup_data = pd.DataFrame({
            'Client': ['Client 1'],
            'Project': ['Website Development'],
            'Name': ['Isabella Rodriguez']
        })
        mock_float_data = pd.DataFrame({
            'Client': ['Client 1'],
            'Project': ['Website Development'],
            'Name': ['John Smith']
        })
        
        mock_read_csv.side_effect = [
            mock_clickup_data, 
            mock_float_data
        ]
        
        # Call the function
        clickup_df, float_df = load_datasets(
            clickup_path='mock_clickup.csv', 
            float_path='mock_float.csv'
        )
        
        # Assertions
        assert not clickup_df.empty
        assert not float_df.empty
        assert len(clickup_df) == 1
        assert len(float_df) == 1

def test_load_datasets_file_not_found():
    """
    Test file not found scenario
    """
    with pytest.raises(FileNotFoundError):
        load_datasets(
            clickup_path='/non/existent/path/clickup.csv',
            float_path='/non/existent/path/float.csv'
        )