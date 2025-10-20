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
    """Test dynamic type analysis and mapping."""
    
    def test_dynamic_object_type(self):
        """Test dynamic type that contains objects."""
        stream = AzureLogAnalyticsStream(MockTap(), name="test_stream")
        
        # Test with object data
        sample_data = [
            {"key1": "value1", "key2": 123},
            {"key3": "value3", "key4": True},
            None
        ]
        
        result = stream._analyze_dynamic_type(sample_data)
        assert isinstance(result, th.ObjectType)
        assert result.additional_properties is True
    
    def test_dynamic_array_of_objects(self):
        """Test dynamic type that contains arrays of objects."""
        stream = AzureLogAnalyticsStream(MockTap(), name="test_stream")
        
        sample_data = [
            [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}],
            [{"status": "active", "count": 5}],
            []
        ]
        
        result = stream._analyze_dynamic_type(sample_data)
        assert isinstance(result, th.ArrayType)
        assert isinstance(result.wrapped_type, th.ObjectType)
        assert result.wrapped_type.additional_properties is True
    
    def test_dynamic_array_of_primitives(self):
        """Test dynamic type that contains arrays of primitives."""
        stream = AzureLogAnalyticsStream(MockTap(), name="test_stream")
        
        sample_data = [
            ["item1", "item2", "item3"],
            [1, 2, 3],
            [True, False],
            []
        ]
        
        result = stream._analyze_dynamic_type(sample_data)
        assert isinstance(result, th.ArrayType)
        assert result.wrapped_type is th.StringType
    
    def test_dynamic_mixed_content(self):
        """Test dynamic type with mixed content (arrays and objects)."""
        stream = AzureLogAnalyticsStream(MockTap(), name="test_stream")
        
        sample_data = [
            [{"id": 1}, {"id": 2}],  # Array of objects
            {"single": "object"},     # Single object
            ["primitive", "array"]    # Array of primitives
        ]
        
        result = stream._analyze_dynamic_type(sample_data)
        # Should prioritize arrays when both are present
        assert isinstance(result, th.ArrayType)
        assert isinstance(result.wrapped_type, th.ObjectType)
    
    def test_dynamic_no_sample_data(self):
        """Test dynamic type with no sample data."""
        stream = AzureLogAnalyticsStream(MockTap(), name="test_stream")
        
        result = stream._analyze_dynamic_type(None)
        assert isinstance(result, th.ObjectType)
        assert result.additional_properties is True
        
        result = stream._analyze_dynamic_type([])
        assert isinstance(result, th.ObjectType)
        assert result.additional_properties is True
    
    def test_dynamic_primitive_values(self):
        """Test dynamic type with primitive values only."""
        stream = AzureLogAnalyticsStream(MockTap(), name="test_stream")
        
        sample_data = ["string_value", 123, True, 45.67]
        
        result = stream._analyze_dynamic_type(sample_data)
        assert isinstance(result, th.StringType)
    
    def test_schema_generation_with_dynamic_types(self):
        """Test schema generation with dynamic columns."""
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
        
        # Check dynamic column types
        properties = schema["properties"]
        
        # ID should be integer (may be nullable)
        assert "integer" in properties["id"]["type"]
        
        # Properties should be object (based on sample data)
        assert "object" in properties["properties"]["type"]
        assert properties["properties"]["additionalProperties"] is True
        
        # Tags should be array of strings (based on sample data)
        assert "array" in properties["tags"]["type"]
        assert "string" in properties["tags"]["items"]["type"]
    
    def test_map_column_type_with_sample_data(self):
        """Test _map_column_type with sample data for dynamic types."""
        stream = AzureLogAnalyticsStream(MockTap(), name="test_stream")
        
        # Test dynamic with object sample data
        result = stream._map_column_type("dynamic", [{"key": "value"}])
        assert isinstance(result, th.ObjectType)
        
        # Test dynamic with array sample data
        result = stream._map_column_type("dynamic", [["item1", "item2"]])
        assert isinstance(result, th.ArrayType)
        assert result.wrapped_type is th.StringType
        
        # Test non-dynamic type (should ignore sample data)
        result = stream._map_column_type("string", [{"key": "value"}])
        assert isinstance(result, th.StringType)
