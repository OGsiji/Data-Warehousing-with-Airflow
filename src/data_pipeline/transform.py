import pandas as pd
import logging
from typing import Tuple

def clean_data(**kwargs):
    """
    Comprehensive data cleaning and transformation function
    
    Returns:
        Tuple of cleaned DataFrames
    """
    try:
        # Retrieve datasets from XCom
        ti = kwargs['ti']
        clickup_df, float_df = ti.xcom_pull(task_ids='load_datasets')
        
        # Standardize column names
        def normalize_columns(df):
            df.columns = [
                col.lower().strip().replace(' ', '_') 
                for col in df.columns
            ]
            return df
        
        clickup_df = normalize_columns(clickup_df)
        float_df = normalize_columns(float_df)
        
        # Data type conversions
        clickup_df['date'] = pd.to_datetime(clickup_df['date'])
        float_df['start_date'] = pd.to_datetime(float_df['start_date'])
        float_df['end_date'] = pd.to_datetime(float_df['end_date'])
        
        # Handle missing values
        clickup_df.dropna(
            subset=['client', 'project', 'hours'], 
            inplace=True
        )
        float_df.dropna(
            subset=['client', 'project', 'estimated_hours'], 
            inplace=True
        )
        
        # Data validation
        def validate_data(df, source):
            """Perform data validation checks"""
            # Check for negative hours
            if 'hours' in df.columns:
                if (df['hours'] < 0).any():
                    logging.warning(f"Negative hours found in {source} dataset")
                    df['hours'] = df['hours'].abs()
            elif "Estimated Hours" in df.columns:
                if (df["Estimated Hours"] < 0).any():
                    logging.warning(f"Negative hours found in {source} dataset")
                    df['hours'] = df['hours'].abs()
            else:
                pass
            
            # Check date ranges
            if 'date' in df.columns:
                current_year = pd.Timestamp.now().year
                mask = (df['date'].dt.year > current_year - 2) & \
                       (df['date'].dt.year <= current_year)
                if not mask.all():
                    logging.warning(f"Suspicious dates in {source} dataset")
            
            return df
        
        clickup_df = validate_data(clickup_df, 'ClickUp')
        float_df = validate_data(float_df, 'Float')
        
        # Logging
        logging.info(f"ClickUp data cleaned: {len(clickup_df)} rows")
        logging.info(f"Float data cleaned: {len(float_df)} rows")
        
        return clickup_df, float_df
    
    except Exception as e:
        logging.error(f"Data transformation error: {e}")
        raise

def merge_datasets(clickup_df: pd.DataFrame, float_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge datasets with sophisticated merging strategy
    
    Args:
        clickup_df (pd.DataFrame): ClickUp dataset
        float_df (pd.DataFrame): Float dataset
    
    Returns:
        pd.DataFrame: Merged dataset
    """
    try:
        # Merge on common columns with intelligent strategy
        merged_df = pd.merge(
            clickup_df, 
            float_df, 
            on=['name', 'project', 'client'], 
            how='outer',
            suffixes=('_clickup', '_float')
        )
        
        # Resolve conflicting columns
        def resolve_conflicts(row):
            # Example conflict resolution logic
            row['hours'] = row.get('hours_clickup', row.get('hours_float', 0))
            row['billable'] = row.get('billable_clickup', row.get('billable_float', False))
            return row
        
        merged_df = merged_df.apply(resolve_conflicts, axis=1)
        
        return merged_df
    
    except Exception as e:
        logging.error(f"Dataset merging error: {e}")
        raise