import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    # Database Configuration
    DB_CONNECTION_STRING = os.getenv(
        'DB_CONNECTION_STRING'
    )
    
    # Data Source Paths
    CLICKUP_PATH = os.getenv(
        'CLICKUP_PATH'
    )
    
    FLOAT_PATH = os.getenv(
        'FLOAT_PATH'
    )
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Data Quality Thresholds
    MAX_ALLOWED_MISSING_VALUES = 0.1  # 10%
    MAX_ALLOWED_DUPLICATE_PERCENTAGE = 0.05  # 5%

    @classmethod
    def validate_config(cls):
        """
        Validate configuration settings
        """
        # Add configuration validation logic
        if not os.path.exists(cls.CLICKUP_PATH):
            raise FileNotFoundError(f"ClickUp CSV not found: {cls.CLICKUP_PATH}")
        
        if not os.path.exists(cls.FLOAT_PATH):
            raise FileNotFoundError(f"Float CSV not found: {cls.FLOAT_PATH}")