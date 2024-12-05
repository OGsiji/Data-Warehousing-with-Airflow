# Data Warehouse ETL Pipeline

## Project Overview
This project implements an ETL (Extract, Transform, Load) pipeline for consolidating time tracking and project data from ClickUp and Float.

## Prerequisites
- Python 3.8+
- Apache Airflow
- SQLAlchemy
- pandas

## Installation
1. Clone the repository
2. Create a virtual environment
3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration
- Create a `.env` file with the following variables:
  - `DB_CONNECTION_STRING`
  - `CLICKUP_PATH`
  - `FLOAT_PATH`
  - `LOG_LEVEL`

## Project Structure
- `dags/`: Airflow DAG definitions
- `src/`: Source code
  - `config/`: Configuration management
  - `data_pipeline/`: ETL logic
  - `models/`: Data models
- `tests/`: Unit and integration tests
- `data/`: Raw and processed data storage

## Running the Pipeline
```bash
airflow dags trigger data_warehouse_etl
```

## Testing
```bash
python -m pytest tests/
```

## Data Quality Checks
- Missing value validation
- Duplicate record detection
- Schema validation

## Logging
Configured log levels allow detailed tracking of ETL processes.