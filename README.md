# Spark Table Size Calculator

This project implements a Spark-based job to calculate the size of all tables in Databricks, as specified in Jira ticket DTP-472.

## Overview

The `spark_table_size_calculator.py` script connects to Databricks and calculates comprehensive size information for all tables across all databases/schemas. It provides:

- Table size in bytes, MB, and GB
- Row counts for each table
- Table metadata (location, creation time, last modified)
- Support for different table types (managed tables, external tables, views)
- Comprehensive error handling and logging
- Multiple output formats (JSON, CSV, Parquet)

## Features

- **Comprehensive Coverage**: Processes all databases and tables in the Databricks catalog
- **Multiple Size Metrics**: Provides size in bytes, MB, and GB for easy interpretation
- **Row Count Analysis**: Includes row counts for each table
- **Metadata Extraction**: Captures table location, creation time, and last access information
- **Flexible Output**: Supports JSON, CSV, and Parquet output formats
- **Error Handling**: Robust error handling with detailed logging
- **Performance Optimized**: Uses Spark's adaptive query execution for optimal performance

## Requirements

- Python 3.7+
- PySpark 3.4.0+
- Databricks Connect (for Databricks integration)
- Access to Databricks workspace

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Configure Databricks connection (if using Databricks Connect):
```bash
databricks configure --token
```

## Usage

### Basic Usage

Run the script with default configuration:
```bash
python spark_table_size_calculator.py
```

### Configuration

The script can be configured by modifying the `config` dictionary in the `main()` function:

```python
config = {
    "output_path": "/tmp/table_sizes.json",  # Output file path
    "output_format": "json",                 # Output format: json, csv, parquet
    "databases": None                        # List of specific databases, or None for all
}
```

### Custom Database Selection

To process only specific databases:
```python
config = {
    "databases": ["database1", "database2", "database3"]
}
```

### Output Formats

The script supports three output formats:

1. **JSON** (default): Human-readable format with nested structure
2. **CSV**: Tabular format suitable for Excel or other tools
3. **Parquet**: Efficient binary format for large datasets

## Output Structure

The script generates a comprehensive report including:

### Console Output
- Total tables processed
- Total size across all tables
- Average table size
- Top 10 largest tables with details

### File Output
Each table entry includes:
```json
{
  "database_name": "example_db",
  "table_name": "example_table",
  "table_type": "MANAGED",
  "size_bytes": 1048576,
  "size_mb": 1.0,
  "size_gb": 0.001,
  "row_count": 1000,
  "location": "dbfs:/mnt/warehouse/example_db.db/example_table",
  "created_time": "2025-09-01 10:00:00",
  "last_modified": "2025-09-01 12:00:00"
}
```

## Architecture

### Core Components

1. **TableSizeCalculator**: Main class that orchestrates the size calculation process
2. **TableSizeInfo**: Data class to hold table size information
3. **Database Discovery**: Automatically discovers all databases in the catalog
4. **Table Enumeration**: Lists all tables and views in each database
5. **Size Calculation**: Calculates actual or estimated table sizes
6. **Result Processing**: Formats and saves results in multiple formats

### Size Calculation Methods

The script uses multiple approaches to calculate table sizes:

1. **Exact Size**: For managed tables, uses Spark's built-in statistics
2. **File System Analysis**: For external tables, analyzes underlying files
3. **Estimation**: When exact size is unavailable, estimates based on schema and row count

## Error Handling

The script includes comprehensive error handling:

- **Database Access Errors**: Continues processing other databases if one fails
- **Table Access Errors**: Skips problematic tables and continues with others
- **Size Calculation Errors**: Falls back to estimation when exact calculation fails
- **Output Errors**: Provides detailed error messages for troubleshooting

## Logging

The script provides detailed logging at multiple levels:

- **INFO**: Progress updates and summary information
- **WARNING**: Non-critical issues (e.g., estimation fallbacks)
- **ERROR**: Critical errors that prevent processing
- **DEBUG**: Detailed debugging information

## Performance Considerations

- Uses Spark's adaptive query execution for optimal performance
- Processes databases sequentially to avoid overwhelming the cluster
- Implements efficient size calculation methods
- Supports parallel processing within each database

## Troubleshooting

### Common Issues

1. **Connection Errors**: Ensure Databricks credentials are properly configured
2. **Permission Errors**: Verify access to all databases and tables
3. **Memory Issues**: For large catalogs, consider processing specific databases
4. **Output Errors**: Ensure write permissions to the output directory

### Debug Mode

Enable debug logging by modifying the logging level:
```python
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

This implementation was created for Jira ticket DTP-472. For modifications or improvements:

1. Follow the existing code structure and patterns
2. Add comprehensive error handling for new features
3. Update documentation for any new functionality
4. Test with various table types and sizes

## License

This project is created for internal use as specified in DTP-472.