# Testing Cursor Repository

This is a test repository for cursor development and testing purposes.

## Features
- Basic repository setup
- README documentation
- Databricks table operations (delete and vacuum)
- Ready for development

## Databricks Table Operations

This repository contains Python Spark code to manage Databricks tables, specifically implementing delete and vacuum operations for the table `client_catalog.temp.dummy_table`.

### Files
- `databricks_table_operations.py`: Main Python script with Spark code for table operations
- `requirements.txt`: Python dependencies required to run the script

### Usage

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the script:
```bash
python databricks_table_operations.py
```

The script will:
- Connect to your Databricks cluster using PySpark
- Delete the specified table (`client_catalog.temp.dummy_table`)
- Vacuum the table to clean up old files
- Provide comprehensive logging of all operations

### Requirements
- Python 3.7+
- PySpark 3.4.0+
- Access to Databricks cluster
- Appropriate permissions to delete and vacuum tables in the `client_catalog.temp` schema

## Getting Started
Clone this repository and start developing!