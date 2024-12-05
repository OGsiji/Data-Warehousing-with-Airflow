Here's the updated project structure and README with the clarification that there are **two DAGs** inside: `data_warehouse_etl_dag.py` (standard) and `dim_process.py` (basic).

---

# Data Warehouse ETL Pipeline

## Project Overview
Welcome to the **Data Warehouse ETL Pipeline**! This project implements a robust and scalable ETL (Extract, Transform, Load) pipeline to seamlessly consolidate time-tracking and project data from **ClickUp** and **Float**. We focus on data quality and best practices to ensure the pipeline is efficient, easy to maintain, and deploy.

## DAGs Overview
This repository contains **two DAGs** that perform similar ETL operations, but with different configurations and specifications:

1. **Basic DAG (`dim_process.py`)**: This is the basic version of the ETL pipeline DAG. It demonstrates a simple, functional ETL process for those who are new to Airflow or need a lightweight solution. It's easy to understand and extend for custom use cases.

2. **Standard DAG (`data_warehouse_etl_dag.py`)**: This is a more **advanced, robust DAG** designed with **best practices** in mind. It includes a structured arrangement, optimized code, and proper software specifications for scalability and maintainability. This DAG follows the industry-standard workflow patterns, ensuring that the ETL process is efficient, reusable, and production-ready.

Both DAGs perform the same operations, but the standard one incorporates a more **organized structure**, **error handling**, **logging**, and **monitoring capabilities** to ensure smooth execution at scale. Whether you're a beginner or a pro, both DAGs demonstrate the power and flexibility of Airflow in building ETL pipelines.



## DAGs Overview

This repository contains **two DAGs** that perform similar ETL operations, but with different configurations and specifications:

1. **Basic DAG (`dim_process.py`)**: This is the basic version of the ETL pipeline DAG. It demonstrates a simple, functional ETL process for those who are new to Airflow or need a lightweight solution. It's easy to understand and extend for custom use cases.

   ![Basic DAG](images/airflow2.png)  <!-- Replace with actual image path -->

2. **Standard DAG (`data_warehouse_etl_dag.py`)**: This is a more **advanced, robust DAG** designed with **best practices** in mind. It includes a structured arrangement, optimized code, and proper software specifications for scalability and maintainability. This DAG follows the industry-standard workflow patterns, ensuring that the ETL process is efficient, reusable, and production-ready.

   ![Standard DAG](images/airflow1.png)  <!-- Replace with actual image path -->

Both DAGs perform the same operations, but the standard one incorporates a more **organized structure**, **error handling**, **logging**, and **monitoring capabilities** to ensure smooth execution at scale. Whether you're a beginner or a pro, both DAGs demonstrate the power and flexibility of Airflow in building ETL pipelines.


## Prerequisites
Before you start, make sure you have the following installed:

- **Python 3.8+**
- **Apache Airflow** (with necessary configurations)
- **SQLAlchemy**
- **pandas**

## Installation

To get started, follow these steps:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/OGsiji/Data-Warehousing-with-Airflow.git
   cd Data-Warehousing-with-Airflow
   ```

2. **Create a virtual environment**:
   ```bash
   python3 -m venv airflow_env
   source airflow_env/bin/activate  # On Windows, use airflow_env\Scripts\activate
   ```

3. **Install the required dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure your environment** by setting up the `.env` file:
   - `DB_CONNECTION_STRING` - Your database connection string.
   - `CLICKUP_PATH` - Path to ClickUp data.
   - `FLOAT_PATH` - Path to Float data.
   - `LOG_LEVEL` - Desired log level for monitoring.

## Project Structure

Here’s a breakdown of the updated project directory:

```
project_root/
├── dags/                  # Airflow DAG definitions
│   ├── data_warehouse_etl_dag.py    # Standard DAG (optimized, production-ready)
│   └── dim_process.py              # Basic DAG (simple, lightweight solution)
├── plugins/               # Custom plugins and operators
│   └── operators/
│       └── data_quality_operator.py
├── src/                   # Source code for ETL logic and configuration
│   ├── __init__.py
│   ├── config/            # Configuration management
│   │   ├── __init__.py
│   │   └── config.py
│   ├── data_pipeline/     # ETL logic (Extract, Transform, Load)
│   │   ├── __init__.py
│   │   ├── extract.py
│   │   ├── transform.py
│   │   └── load.py
│   └── models/            # Data models (for structuring the processed data)
│       ├── __init__.py
│       ├── base.py
│       ├── project_models.py
│       └── dimensional_models.py
├── tests/                 # Unit and integration tests for the ETL process
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── data/                  # Raw and processed data
│   ├── raw/
│   └── processed/
├── logs/                  # Airflow logs
├── requirements.txt       # Python dependencies
├── setup.py               # Package setup file
├── README.md              # This README
└── .env                   # Environment variables for the pipeline
```

## Running the Pipeline

Once everything is set up, you can trigger the DAG manually from the command line with:

```bash
airflow dags trigger data_warehouse_etl  # For the Standard DAG
```

Or

```bash
airflow dags trigger dim_process  # For the Basic DAG
```

This will start the ETL process, pulling data from ClickUp and Float, transforming it, and loading it into the data warehouse.

## Testing

To run the tests and ensure everything is functioning as expected:

```bash
python -m pytest tests/
```

## Data Quality Checks

Data integrity is crucial in any ETL pipeline, and this project includes several built-in **data quality checks**:

- **Missing Value Validation**: Ensure that there are no missing or null values in critical columns.
- **Duplicate Record Detection**: Identify and handle duplicate records during transformation.
- **Schema Validation**: Verify that the incoming data matches the expected schema before processing.

## Logging

Logging is configured to track detailed information about each step in the ETL process. The log level is configurable, and you can change it in the `.env` file to **debug**, **info**, **warning**, **error**, or **critical** based on your monitoring needs.

## Why This Project is Exciting

This pipeline is **scalable**, **robust**, and built with **best practices** in mind. Whether you're new to Airflow or a seasoned pro, you'll find this project useful for learning and applying the core principles of ETL processes in real-world environments.

Additionally, with the **standard DAG**'s optimized arrangement and **industry-standard specifications**, you'll be able to confidently run and manage data pipelines at scale—perfect for handling growing datasets and ensuring data accuracy.

**Start building, learning, and contributing to scalable data pipelines with this project—you're one step closer to mastering the art of ETL in Apache Airflow!**

---

This version of the README includes both DAGs with a clear distinction between the basic and standard ones, ensuring clarity and excitement about the project. Let me know if you need further modifications!