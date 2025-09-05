"""
Databricks Table Operations - Delete and Vacuum

This script provides functionality to delete and vacuum a Databricks table
using PySpark. Specifically designed to work with the table:
client_catalog.temp.dummy_table

Author: Generated for Linear Issue COM-5
Date: 2025
"""

from pyspark.sql import SparkSession
import logging
import sys
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabricksTableManager:
    """
    A class to manage Databricks table operations including delete and vacuum.
    """
    
    def __init__(self, app_name: str = "DatabricksTableOperations"):
        """
        Initialize the Spark session for Databricks operations.
        
        Args:
            app_name: Name of the Spark application
        """
        try:
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .getOrCreate()
            logger.info("Spark session initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {str(e)}")
            raise
    
    def delete_table(self, table_name: str) -> bool:
        """
        Delete a Databricks table.
        
        Args:
            table_name: Full table name (catalog.schema.table)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"Attempting to delete table: {table_name}")
            
            # Check if table exists
            if not self._table_exists(table_name):
                logger.warning(f"Table {table_name} does not exist")
                return True
            
            # Delete the table
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"Successfully deleted table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete table {table_name}: {str(e)}")
            return False
    
    def vacuum_table(self, table_name: str, retention_hours: int = 168) -> bool:
        """
        Vacuum a Databricks table to remove old files.
        
        Args:
            table_name: Full table name (catalog.schema.table)
            retention_hours: Number of hours to retain files (default 168 hours = 7 days)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"Attempting to vacuum table: {table_name}")
            
            # Check if table exists
            if not self._table_exists(table_name):
                logger.warning(f"Table {table_name} does not exist, skipping vacuum")
                return True
            
            # Vacuum the table
            vacuum_sql = f"VACUUM {table_name} RETAIN {retention_hours} HOURS"
            self.spark.sql(vacuum_sql)
            logger.info(f"Successfully vacuumed table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to vacuum table {table_name}: {str(e)}")
            return False
    
    def delete_and_vacuum_table(self, table_name: str, retention_hours: int = 168) -> bool:
        """
        Delete and then vacuum a Databricks table.
        
        Args:
            table_name: Full table name (catalog.schema.table)
            retention_hours: Number of hours to retain files during vacuum
            
        Returns:
            bool: True if both operations successful, False otherwise
        """
        logger.info(f"Starting delete and vacuum operation for table: {table_name}")
        
        # First vacuum to clean up old files before deletion
        vacuum_success = self.vacuum_table(table_name, retention_hours)
        
        # Then delete the table
        delete_success = self.delete_table(table_name)
        
        if delete_success and vacuum_success:
            logger.info(f"Successfully completed delete and vacuum for table: {table_name}")
            return True
        else:
            logger.error(f"Failed to complete delete and vacuum for table: {table_name}")
            return False
    
    def _table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in Databricks.
        
        Args:
            table_name: Full table name (catalog.schema.table)
            
        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            result = self.spark.sql(f"SHOW TABLES IN {table_name.rsplit('.', 1)[0]} LIKE '{table_name.split('.')[-1]}'")
            return result.count() > 0
        except Exception as e:
            logger.warning(f"Could not check if table exists: {str(e)}")
            return False
    
    def close(self):
        """Close the Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session closed")


def main():
    """
    Main function to execute the delete and vacuum operation for the specified table.
    """
    # Target table as specified in the Linear issue
    TARGET_TABLE = "client_catalog.temp.dummy_table"
    
    # Initialize table manager
    table_manager = None
    
    try:
        # Create table manager
        table_manager = DatabricksTableManager()
        
        # Execute delete and vacuum operation
        success = table_manager.delete_and_vacuum_table(TARGET_TABLE)
        
        if success:
            logger.info("Operation completed successfully!")
            sys.exit(0)
        else:
            logger.error("Operation failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)
        
    finally:
        # Clean up resources
        if table_manager:
            table_manager.close()


if __name__ == "__main__":
    main()