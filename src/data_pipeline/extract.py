import pandas as pd
import logging
from src.config.config import Config

def load_datasets(clickup_path=None, float_path=None, **kwargs):
    """
    Load raw datasets from CSV files with enhanced error handling
    
    Args:
        clickup_path (str, optional): Path to ClickUp CSV
        float_path (str, optional): Path to Float CSV
    
    Returns:
        tuple: Loaded DataFrames for ClickUp and Float
    """
    try:
        # Use provided paths or fall back to config
        clickup_path = clickup_path or Config.CLICKUP_PATH
        float_path = float_path or Config.FLOAT_PATH
        
        # Load datasets
        clickup_df = pd.read_csv(clickup_path)
        float_df = pd.read_csv(float_path)
        
        # Log dataset details
        logging.info(f"ClickUp Dataset: {len(clickup_df)} rows")
        logging.info(f"Float Dataset: {len(float_df)} rows")
        
        return clickup_df, float_df
    
    except FileNotFoundError as e:
        logging.error(f"Dataset loading error: {e}")
        raise
    except pd.errors.EmptyDataError:
        logging.error("One or both CSV files are empty")
        raise ValueError("Empty datasets cannot be processed")