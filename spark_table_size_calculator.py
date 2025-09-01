#!/usr/bin/env python3
"""
Spark Job to Calculate Table Sizes in Databricks

This script connects to Databricks and calculates the size of all tables
across all databases/schemas. It provides comprehensive size information
including storage used, row counts, and metadata.

Author: Generated for DTP-472
Date: 2025-09-01
"""

import logging
import json
import sys
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum as spark_sum, count
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType


@dataclass
class TableSizeInfo:
    """Data class to hold table size information"""
    database_name: str
    table_name: str
    table_type: str
    size_bytes: int
    size_mb: float
    size_gb: float
    row_count: Optional[int]
    location: Optional[str]
    created_time: Optional[str]
    last_modified: Optional[str]


class TableSizeCalculator:
    """Main class for calculating table sizes in Databricks"""
    
    def __init__(self, spark_session: SparkSession, config: Dict = None):
        """
        Initialize the TableSizeCalculator
        
        Args:
            spark_session: Active Spark session
            config: Configuration dictionary
        """
        self.spark = spark_session
        self.config = config or {}
        self.logger = self._setup_logging()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def get_all_databases(self) -> List[str]:
        """Get list of all databases in the catalog"""
        try:
            self.logger.info("Fetching all databases...")
            databases_df = self.spark.sql("SHOW DATABASES")
            databases = [row.databaseName for row in databases_df.collect()]
            self.logger.info(f"Found {len(databases)} databases: {databases}")
            return databases
        except Exception as e:
            self.logger.error(f"Error fetching databases: {str(e)}")
            raise
    
    def get_tables_in_database(self, database_name: str) -> List[Tuple[str, str]]:
        """
        Get all tables in a specific database
        
        Args:
            database_name: Name of the database
            
        Returns:
            List of tuples (table_name, table_type)
        """
        try:
            self.logger.info(f"Fetching tables in database: {database_name}")
            
            # Use the database
            self.spark.sql(f"USE {database_name}")
            
            # Get all tables and views
            tables_df = self.spark.sql("SHOW TABLES")
            
            tables = []
            for row in tables_df.collect():
                table_name = row.tableName
                table_type = row.tableType if hasattr(row, 'tableType') else 'TABLE'
                tables.append((table_name, table_type))
            
            self.logger.info(f"Found {len(tables)} tables in {database_name}")
            return tables
            
        except Exception as e:
            self.logger.error(f"Error fetching tables from {database_name}: {str(e)}")
            return []
    
    def get_table_size_info(self, database_name: str, table_name: str, table_type: str) -> Optional[TableSizeInfo]:
        """
        Get size information for a specific table
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            table_type: Type of the table (TABLE, VIEW, etc.)
            
        Returns:
            TableSizeInfo object or None if error
        """
        try:
            full_table_name = f"{database_name}.{table_name}"
            self.logger.debug(f"Calculating size for table: {full_table_name}")
            
            # Get table details
            describe_df = self.spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}")
            describe_info = {row.col_name: row.data_type for row in describe_df.collect() 
                           if row.col_name in ['Location', 'Created Time', 'Last Access']}
            
            # Get row count
            try:
                count_df = self.spark.sql(f"SELECT COUNT(*) as row_count FROM {full_table_name}")
                row_count = count_df.collect()[0].row_count
            except Exception as e:
                self.logger.warning(f"Could not get row count for {full_table_name}: {str(e)}")
                row_count = None
            
            # Get table size using DESCRIBE TABLE EXTENDED
            size_bytes = 0
            try:
                # Try to get size from table properties
                size_info_df = self.spark.sql(f"""
                    SELECT 
                        CASE 
                            WHEN table_type = 'MANAGED' THEN 
                                (SELECT SUM(size_bytes) FROM (
                                    SELECT size_bytes FROM (
                                        DESCRIBE TABLE EXTENDED {full_table_name}
                                    ) WHERE col_name = 'Statistics'
                                ))
                            ELSE 0
                        END as size_bytes
                """)
                
                # Alternative approach: use file system information
                if size_bytes == 0:
                    # For external tables, we might need to check the file system
                    # This is a simplified approach
                    size_bytes = self._estimate_table_size(full_table_name)
                    
            except Exception as e:
                self.logger.warning(f"Could not get exact size for {full_table_name}: {str(e)}")
                size_bytes = self._estimate_table_size(full_table_name)
            
            # Convert bytes to MB and GB
            size_mb = size_bytes / (1024 * 1024) if size_bytes > 0 else 0
            size_gb = size_bytes / (1024 * 1024 * 1024) if size_bytes > 0 else 0
            
            return TableSizeInfo(
                database_name=database_name,
                table_name=table_name,
                table_type=table_type,
                size_bytes=size_bytes,
                size_mb=round(size_mb, 2),
                size_gb=round(size_gb, 4),
                row_count=row_count,
                location=describe_info.get('Location'),
                created_time=describe_info.get('Created Time'),
                last_modified=describe_info.get('Last Access')
            )
            
        except Exception as e:
            self.logger.error(f"Error getting size info for {database_name}.{table_name}: {str(e)}")
            return None
    
    def _estimate_table_size(self, full_table_name: str) -> int:
        """
        Estimate table size when exact size is not available
        
        Args:
            full_table_name: Full table name (database.table)
            
        Returns:
            Estimated size in bytes
        """
        try:
            # Get table schema to estimate size
            schema_df = self.spark.sql(f"DESCRIBE {full_table_name}")
            column_count = schema_df.count()
            
            # Get row count
            count_df = self.spark.sql(f"SELECT COUNT(*) as row_count FROM {full_table_name}")
            row_count = count_df.collect()[0].row_count
            
            # Rough estimation: assume average 100 bytes per column per row
            estimated_size = row_count * column_count * 100
            
            return estimated_size
            
        except Exception as e:
            self.logger.warning(f"Could not estimate size for {full_table_name}: {str(e)}")
            return 0
    
    def calculate_all_table_sizes(self, databases: Optional[List[str]] = None) -> List[TableSizeInfo]:
        """
        Calculate sizes for all tables across specified databases
        
        Args:
            databases: List of database names to process. If None, processes all databases.
            
        Returns:
            List of TableSizeInfo objects
        """
        if databases is None:
            databases = self.get_all_databases()
        
        all_table_sizes = []
        total_tables = 0
        
        for database in databases:
            self.logger.info(f"Processing database: {database}")
            
            try:
                tables = self.get_tables_in_database(database)
                total_tables += len(tables)
                
                for table_name, table_type in tables:
                    table_info = self.get_table_size_info(database, table_name, table_type)
                    if table_info:
                        all_table_sizes.append(table_info)
                        
            except Exception as e:
                self.logger.error(f"Error processing database {database}: {str(e)}")
                continue
        
        self.logger.info(f"Completed processing {total_tables} tables across {len(databases)} databases")
        return all_table_sizes
    
    def save_results(self, table_sizes: List[TableSizeInfo], output_path: str, format: str = "json"):
        """
        Save results to file
        
        Args:
            table_sizes: List of TableSizeInfo objects
            output_path: Output file path
            format: Output format (json, csv, parquet)
        """
        try:
            if format.lower() == "json":
                # Convert to JSON
                results = [asdict(info) for info in table_sizes]
                with open(output_path, 'w') as f:
                    json.dump(results, f, indent=2, default=str)
                    
            elif format.lower() == "csv":
                # Convert to CSV
                import pandas as pd
                df = pd.DataFrame([asdict(info) for info in table_sizes])
                df.to_csv(output_path, index=False)
                
            elif format.lower() == "parquet":
                # Convert to Parquet using Spark
                df = self.spark.createDataFrame([asdict(info) for info in table_sizes])
                df.write.mode("overwrite").parquet(output_path)
                
            else:
                raise ValueError(f"Unsupported format: {format}")
                
            self.logger.info(f"Results saved to {output_path} in {format} format")
            
        except Exception as e:
            self.logger.error(f"Error saving results: {str(e)}")
            raise
    
    def print_summary(self, table_sizes: List[TableSizeInfo]):
        """Print a summary of the results"""
        if not table_sizes:
            self.logger.info("No table size information found")
            return
        
        total_size_bytes = sum(info.size_bytes for info in table_sizes)
        total_size_gb = total_size_bytes / (1024 * 1024 * 1024)
        
        print("\n" + "="*80)
        print("TABLE SIZE CALCULATION SUMMARY")
        print("="*80)
        print(f"Total Tables Processed: {len(table_sizes)}")
        print(f"Total Size: {total_size_gb:.2f} GB ({total_size_bytes:,} bytes)")
        print(f"Average Table Size: {total_size_gb/len(table_sizes):.4f} GB")
        
        # Top 10 largest tables
        largest_tables = sorted(table_sizes, key=lambda x: x.size_bytes, reverse=True)[:10]
        print(f"\nTop 10 Largest Tables:")
        print("-" * 80)
        print(f"{'Database':<20} {'Table':<30} {'Size (GB)':<12} {'Rows':<15}")
        print("-" * 80)
        
        for table in largest_tables:
            row_count_str = f"{table.row_count:,}" if table.row_count else "N/A"
            print(f"{table.database_name:<20} {table.table_name:<30} {table.size_gb:<12.4f} {row_count_str:<15}")
        
        print("="*80)


def create_spark_session(app_name: str = "TableSizeCalculator") -> SparkSession:
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def main():
    """Main function"""
    # Configuration
    config = {
        "output_path": "/tmp/table_sizes.json",
        "output_format": "json",
        "databases": None  # None means all databases
    }
    
    # Create Spark session
    spark = create_spark_session("DTP-472-TableSizeCalculator")
    
    try:
        # Initialize calculator
        calculator = TableSizeCalculator(spark, config)
        
        # Calculate table sizes
        print("Starting table size calculation...")
        table_sizes = calculator.calculate_all_table_sizes(config.get("databases"))
        
        # Print summary
        calculator.print_summary(table_sizes)
        
        # Save results
        if table_sizes:
            calculator.save_results(table_sizes, config["output_path"], config["output_format"])
            print(f"\nResults saved to: {config['output_path']}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()