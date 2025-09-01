"""
Configuration file for Spark Table Size Calculator

This module contains configuration settings and constants used throughout
the table size calculation process.
"""

import os
from typing import Dict, List, Optional


class Config:
    """Configuration class for the table size calculator"""
    
    # Default configuration values
    DEFAULT_OUTPUT_PATH = "/tmp/table_sizes.json"
    DEFAULT_OUTPUT_FORMAT = "json"
    DEFAULT_LOG_LEVEL = "INFO"
    
    # Supported output formats
    SUPPORTED_FORMATS = ["json", "csv", "parquet"]
    
    # Spark configuration
    SPARK_CONFIG = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }
    
    # Table size estimation constants
    ESTIMATED_BYTES_PER_COLUMN_PER_ROW = 100
    MIN_ESTIMATED_SIZE_BYTES = 1024  # 1KB minimum
    
    # Logging configuration
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    @classmethod
    def get_config(cls, custom_config: Optional[Dict] = None) -> Dict:
        """
        Get configuration with optional custom overrides
        
        Args:
            custom_config: Custom configuration dictionary
            
        Returns:
            Complete configuration dictionary
        """
        config = {
            "output_path": os.getenv("OUTPUT_PATH", cls.DEFAULT_OUTPUT_PATH),
            "output_format": os.getenv("OUTPUT_FORMAT", cls.DEFAULT_OUTPUT_FORMAT),
            "log_level": os.getenv("LOG_LEVEL", cls.DEFAULT_LOG_LEVEL),
            "databases": None,  # Will be set to all databases if None
            "spark_config": cls.SPARK_CONFIG.copy(),
            "estimated_bytes_per_column_per_row": cls.ESTIMATED_BYTES_PER_COLUMN_PER_ROW,
            "min_estimated_size_bytes": cls.MIN_ESTIMATED_SIZE_BYTES
        }
        
        # Apply custom configuration overrides
        if custom_config:
            config.update(custom_config)
        
        # Validate configuration
        cls._validate_config(config)
        
        return config
    
    @classmethod
    def _validate_config(cls, config: Dict) -> None:
        """
        Validate configuration parameters
        
        Args:
            config: Configuration dictionary to validate
            
        Raises:
            ValueError: If configuration is invalid
        """
        # Validate output format
        if config.get("output_format") not in cls.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported output format: {config.get('output_format')}. "
                           f"Supported formats: {cls.SUPPORTED_FORMATS}")
        
        # Validate log level
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if config.get("log_level") not in valid_log_levels:
            raise ValueError(f"Invalid log level: {config.get('log_level')}. "
                           f"Valid levels: {valid_log_levels}")
        
        # Validate output path
        output_path = config.get("output_path")
        if not output_path:
            raise ValueError("Output path cannot be empty")
        
        # Check if output directory exists and is writable
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir, exist_ok=True)
            except OSError as e:
                raise ValueError(f"Cannot create output directory {output_dir}: {e}")


class DatabaseConfig:
    """Configuration for database-specific settings"""
    
    # Common database names to exclude (if needed)
    EXCLUDED_DATABASES = [
        "information_schema",
        "sys",
        "performance_schema"
    ]
    
    # Database-specific settings
    DATABASE_SETTINGS = {
        "default": {
            "timeout_seconds": 300,
            "max_retries": 3
        }
    }
    
    @classmethod
    def get_database_settings(cls, database_name: str) -> Dict:
        """
        Get settings for a specific database
        
        Args:
            database_name: Name of the database
            
        Returns:
            Database-specific settings
        """
        return cls.DATABASE_SETTINGS.get(database_name, cls.DATABASE_SETTINGS["default"])
    
    @classmethod
    def should_exclude_database(cls, database_name: str) -> bool:
        """
        Check if a database should be excluded from processing
        
        Args:
            database_name: Name of the database
            
        Returns:
            True if database should be excluded
        """
        return database_name.lower() in [db.lower() for db in cls.EXCLUDED_DATABASES]


class TableConfig:
    """Configuration for table-specific settings"""
    
    # Table types to process
    SUPPORTED_TABLE_TYPES = ["TABLE", "VIEW", "EXTERNAL"]
    
    # Size calculation methods
    SIZE_CALCULATION_METHODS = {
        "MANAGED": "exact",
        "EXTERNAL": "file_system",
        "VIEW": "estimation"
    }
    
    @classmethod
    def get_size_calculation_method(cls, table_type: str) -> str:
        """
        Get the size calculation method for a table type
        
        Args:
            table_type: Type of the table
            
        Returns:
            Size calculation method
        """
        return cls.SIZE_CALCULATION_METHODS.get(table_type.upper(), "estimation")
    
    @classmethod
    def is_supported_table_type(cls, table_type: str) -> bool:
        """
        Check if a table type is supported
        
        Args:
            table_type: Type of the table
            
        Returns:
            True if table type is supported
        """
        return table_type.upper() in cls.SUPPORTED_TABLE_TYPES