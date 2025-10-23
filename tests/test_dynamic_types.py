"""Tests for dynamic type handling in Azure Log Analytics."""

import pytest
from singer_sdk import typing as th
from tap_azure_log_analytics.client import AzureLogAnalyticsStream


class MockTap:
    """Mock tap for testing."""
    
    def __init__(self):
        import logging
        import time
        self.logger = logging.getLogger("mock_tap")
        self.metrics_logger = logging.getLogger("mock_tap.metrics")
        self.name = "mock_tap"
        self.config = {}
        self.state = {}
        self.initialized_at = int(time.time() * 1000)


class MockTable:
    """Mock table for testing."""
    
    def __init__(self, columns, column_types, rows):
        self.columns = columns
        self.columns_types = column_types
        self.rows = rows


class TestDynamicTypeHandling:
    """Test simplified dynamic type handling - all dynamic types treated as strings."""
    
    def test_dynamic_type_maps_to_string(self):
        """Test that dynamic types are mapped to string type."""
        stream = AzureLogAnalyticsStream(MockTap(), name="test_stream")
        
        result = stream._map_column_type("dynamic")
        assert isinstance(result, th.StringType)
    
    def test_schema_generation_with_dynamic_types(self):
        """Test schema generation with dynamic columns treated as strings."""
        stream = AzureLogAnalyticsStream(MockTap(), name="test_stream")
        
        # Mock table with dynamic columns
        table = MockTable(
            columns=["id", "properties", "tags"],
            column_types=["int", "dynamic", "dynamic"],
            rows=[
                [1, {"key1": "value1", "key2": 123}, ["tag1", "tag2", "tag3"]],
                [2, {"key3": "value3"}, ["tag4"]],
                [3, None, []]
            ]
        )
        
        schema = stream._generate_schema_from_results([table])
        
        # Check that schema was generated
        assert "properties" in schema
        assert "type" in schema
        
        # Check column types
        properties = schema["properties"]
        
        # ID should be integer
        assert "integer" in properties["id"]["type"]
        
        # Dynamic columns should be strings
        assert "string" in properties["properties"]["type"]
        assert "string" in properties["tags"]["type"]
    
    def test_map_column_type_basic_types(self):
        """Test _map_column_type for various column types."""
        stream = AzureLogAnalyticsStream(MockTap(), name="test_stream")
        
        # Test dynamic type
        result = stream._map_column_type("dynamic")
        assert isinstance(result, th.StringType)
        
        # Test string type
        result = stream._map_column_type("string")
        assert isinstance(result, th.StringType)
        
        # Test integer type
        result = stream._map_column_type("int")
        assert isinstance(result, th.IntegerType)
        
        # Test boolean type
        result = stream._map_column_type("bool")
        assert isinstance(result, th.BooleanType)
        
        # Test unknown type defaults to string
        result = stream._map_column_type("unknown")
        assert isinstance(result, th.StringType)
