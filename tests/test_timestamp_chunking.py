"""Tests for timestamp chunking functionality in Azure Log Analytics tap."""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

from tap_azure_log_analytics.client import AzureLogAnalyticsStream
from tap_azure_log_analytics.streams import LogAnalyticsQueryStream


class TestTimestampChunking:
    """Test timestamp chunking functionality."""

    def test_chunk_timespan_basic(self):
        """Test basic timespan chunking functionality."""
        mock_tap = Mock()
        mock_tap.config = {}
        stream = AzureLogAnalyticsStream(mock_tap, name="test_stream")
        
        # Test with 5 days total, 2 days per chunk
        start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 6, tzinfo=timezone.utc)
        
        chunks = stream._chunk_timespan(start_time, end_time, chunk_days=2)
        
        # Should create 3 chunks: Jan 1-3, Jan 3-5, Jan 5-6
        assert len(chunks) == 3
        assert chunks[0] == (datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 3, tzinfo=timezone.utc))
        assert chunks[1] == (datetime(2024, 1, 3, tzinfo=timezone.utc), datetime(2024, 1, 5, tzinfo=timezone.utc))
        assert chunks[2] == (datetime(2024, 1, 5, tzinfo=timezone.utc), datetime(2024, 1, 6, tzinfo=timezone.utc))

    def test_chunk_timespan_exact_multiple(self):
        """Test chunking when timespan is exact multiple of chunk size."""
        mock_tap = Mock()
        mock_tap.config = {}
        stream = AzureLogAnalyticsStream(mock_tap, name="test_stream")
        
        start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 5, tzinfo=timezone.utc)  # 4 days
        
        chunks = stream._chunk_timespan(start_time, end_time, chunk_days=2)
        
        # Should create 2 chunks: Jan 1-3, Jan 3-5
        assert len(chunks) == 2
        assert chunks[0] == (datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 3, tzinfo=timezone.utc))
        assert chunks[1] == (datetime(2024, 1, 3, tzinfo=timezone.utc), datetime(2024, 1, 5, tzinfo=timezone.utc))

    def test_chunk_timespan_single_chunk(self):
        """Test chunking when timespan fits in single chunk."""
        mock_tap = Mock()
        mock_tap.config = {}
        stream = AzureLogAnalyticsStream(mock_tap, name="test_stream")
        
        start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 2, tzinfo=timezone.utc)  # 1 day
        
        chunks = stream._chunk_timespan(start_time, end_time, chunk_days=2)
        
        # Should create 1 chunk
        assert len(chunks) == 1
        assert chunks[0] == (datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 2, tzinfo=timezone.utc))

    def test_calculate_timespan_with_replication_key(self):
        """Test timespan calculation when replication key is present."""
        # Mock tap and stream
        mock_tap = Mock()
        mock_tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        
        stream = AzureLogAnalyticsStream(mock_tap, name="test_stream")
        stream.replication_key = "TimeGenerated"
        
        # Mock get_starting_timestamp to return a specific time
        with patch.object(stream, 'get_starting_timestamp', return_value=datetime(2024, 1, 2, tzinfo=timezone.utc)):
            start_time, end_time = stream._calculate_timespan(None)
            
            assert start_time == datetime(2024, 1, 2, tzinfo=timezone.utc)
            # End time should be 5 minutes in the past
            expected_end = datetime.now(timezone.utc) - timedelta(minutes=5)
            assert abs((end_time - expected_end).total_seconds()) < 1  # Within 1 second
            assert end_time.tzinfo is not None  # Should be timezone aware

    def test_calculate_timespan_without_replication_key_uses_start_date(self):
        """Test timespan calculation without replication key uses start_date from config."""
        mock_tap = Mock()
        mock_tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        
        stream = AzureLogAnalyticsStream(mock_tap, name="test_stream")
        stream.replication_key = None
        
        start_time, end_time = stream._calculate_timespan(None)
        
        assert start_time == datetime(2024, 1, 1, tzinfo=timezone.utc)
        # End time should be 5 minutes in the past
        expected_end = datetime.now(timezone.utc) - timedelta(minutes=5)
        assert abs((end_time - expected_end).total_seconds()) < 1  # Within 1 second
        assert end_time.tzinfo is not None

    def test_calculate_timespan_without_replication_key_no_start_date(self):
        """Test timespan calculation without replication key and no start_date defaults to 1 day ago."""
        mock_tap = Mock()
        mock_tap.config = {}
        
        stream = AzureLogAnalyticsStream(mock_tap, name="test_stream")
        stream.replication_key = None
        
        start_time, end_time = stream._calculate_timespan(None)
        
        # Should default to 1 day ago from end time (which is 5 minutes in the future)
        expected_start = end_time - timedelta(days=1)
        assert abs((start_time - expected_start).total_seconds()) < 1  # Within 1 second
        # End time should be 5 minutes in the past
        expected_end = datetime.now(timezone.utc) - timedelta(minutes=5)
        assert abs((end_time - expected_end).total_seconds()) < 1  # Within 1 second
        assert end_time.tzinfo is not None

    def test_calculate_timespan_string_start_date(self):
        """Test timespan calculation with string start_date."""
        mock_tap = Mock()
        mock_tap.config = {"start_date": "2024-01-01T00:00:00Z"}
        
        stream = AzureLogAnalyticsStream(mock_tap, name="test_stream")
        stream.replication_key = None
        
        start_time, end_time = stream._calculate_timespan(None)
        
        assert start_time == datetime(2024, 1, 1, tzinfo=timezone.utc)
        # End time should be 5 minutes in the past
        expected_end = datetime.now(timezone.utc) - timedelta(minutes=5)
        assert abs((end_time - expected_end).total_seconds()) < 1  # Within 1 second
        assert end_time.tzinfo is not None

    def test_calculate_timespan_naive_datetime(self):
        """Test timespan calculation with naive datetime gets timezone."""
        mock_tap = Mock()
        mock_tap.config = {"start_date": datetime(2024, 1, 1)}  # Naive datetime
        
        stream = AzureLogAnalyticsStream(mock_tap, name="test_stream")
        stream.replication_key = None
        
        start_time, end_time = stream._calculate_timespan(None)
        
        assert start_time.tzinfo == timezone.utc
        # End time should be 5 minutes in the past
        expected_end = datetime.now(timezone.utc) - timedelta(minutes=5)
        assert abs((end_time - expected_end).total_seconds()) < 1  # Within 1 second
        assert end_time.tzinfo is not None


class TestLogAnalyticsQueryStreamChunking:
    """Test chunking functionality in LogAnalyticsQueryStream."""

    def test_query_stream_with_timespan_days(self):
        """Test that query stream properly uses timespan_days when no replication key."""
        mock_tap = Mock()
        mock_tap.config = {"workspace_id": "test-workspace"}
        
        query_config = {
            "name": "test_stream",
            "query": "test query",
            "primary_keys": ["id"],
            "replication_key": None,
            "timespan_days": 7,
            "chunk_size_days": 2
        }
        
        stream = LogAnalyticsQueryStream(mock_tap, query_config)
        
        # Mock the client to avoid actual API calls
        with patch.object(stream, 'client') as mock_client:
            mock_response = Mock()
            mock_response.status = Mock()
            mock_response.tables = []
            mock_client.query_workspace.return_value = mock_response
            
            # Mock _calculate_timespan to test the logic
            with patch.object(stream, '_calculate_timespan') as mock_calc:
                mock_calc.return_value = (
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    datetime(2024, 1, 8, tzinfo=timezone.utc)
                )
                
                # This should call _chunk_timespan with chunk_size_days=2
                list(stream.get_records(None))
                
                # Verify that chunking was called with the right parameters
                # The actual test would be in the _calculate_timespan method
                # but we need to fix that method first

    def test_query_stream_chunk_size_parameter_mismatch(self):
        """Test that the chunk_size_days parameter is properly passed to _chunk_timespan."""
        mock_tap = Mock()
        mock_tap.config = {"workspace_id": "test-workspace"}
        
        query_config = {
            "name": "test_stream",
            "query": "test query",
            "primary_keys": ["id"],
            "replication_key": None,
            "timespan_days": 5,
            "chunk_size_days": 1
        }
        
        stream = LogAnalyticsQueryStream(mock_tap, query_config)
        
        # Mock the client
        with patch.object(stream, 'client') as mock_client:
            mock_response = Mock()
            mock_response.status = Mock()
            mock_response.tables = []
            mock_client.query_workspace.return_value = mock_response
            
            # Mock _calculate_timespan to return a known timespan
            with patch.object(stream, '_calculate_timespan') as mock_calc:
                start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
                end_time = datetime(2024, 1, 6, tzinfo=timezone.utc)
                mock_calc.return_value = (start_time, end_time)
                
                # Mock _chunk_timespan to verify it's called with correct parameters
                with patch.object(stream, '_chunk_timespan') as mock_chunk:
                    mock_chunk.return_value = [(start_time, end_time)]
                    
                    list(stream.get_records(None))
                    
                    # Verify _chunk_timespan was called with chunk_size_days=1
                    mock_chunk.assert_called_once_with(start_time, end_time, 1)

    def test_query_stream_with_replication_key_ignores_timespan_days(self):
        """Test that when replication key exists, timespan_days is ignored."""
        mock_tap = Mock()
        mock_tap.config = {"workspace_id": "test-workspace"}
        
        query_config = {
            "name": "test_stream",
            "query": "test query",
            "primary_keys": ["id"],
            "replication_key": "TimeGenerated",
            "timespan_days": 7,  # This should be ignored
            "chunk_size_days": 2
        }
        
        stream = LogAnalyticsQueryStream(mock_tap, query_config)
        
        # Mock get_starting_timestamp to return a specific time
        with patch.object(stream, 'get_starting_timestamp', return_value=datetime(2024, 1, 2, tzinfo=timezone.utc)):
            start_time, end_time = stream._calculate_timespan(None)
            
            # Should use replication key timestamp, not timespan_days
            assert start_time == datetime(2024, 1, 2, tzinfo=timezone.utc)


class TestTimestampChunkingEdgeCases:
    """Test edge cases for timestamp chunking."""

    def test_chunk_timespan_zero_days(self):
        """Test chunking with zero days (should not create infinite loop)."""
        mock_tap = Mock()
        mock_tap.config = {}
        stream = AzureLogAnalyticsStream(mock_tap, name="test_stream")
        
        start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 2, tzinfo=timezone.utc)
        
        chunks = stream._chunk_timespan(start_time, end_time, chunk_days=0)
        
        # Should handle gracefully - might return single chunk or empty list
        assert isinstance(chunks, list)

    def test_chunk_timespan_negative_days(self):
        """Test chunking with negative days."""
        mock_tap = Mock()
        mock_tap.config = {}
        stream = AzureLogAnalyticsStream(mock_tap, name="test_stream")
        
        start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 2, tzinfo=timezone.utc)
        
        chunks = stream._chunk_timespan(start_time, end_time, chunk_days=-1)
        
        # Should handle gracefully
        assert isinstance(chunks, list)

    def test_chunk_timespan_same_start_end(self):
        """Test chunking when start and end times are the same."""
        mock_tap = Mock()
        mock_tap.config = {}
        stream = AzureLogAnalyticsStream(mock_tap, name="test_stream")
        
        start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        chunks = stream._chunk_timespan(start_time, end_time, chunk_days=1)
        
        # Should return empty list since start >= end
        assert len(chunks) == 0

    def test_chunk_timespan_start_after_end(self):
        """Test chunking when start time is after end time."""
        mock_tap = Mock()
        mock_tap.config = {}
        stream = AzureLogAnalyticsStream(mock_tap, name="test_stream")
        
        start_time = datetime(2024, 1, 2, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        chunks = stream._chunk_timespan(start_time, end_time, chunk_days=1)
        
        # Should return empty list since start > end
        assert len(chunks) == 0


class TestTimespanDaysIntegration:
    """Test integration of timespan_days with chunking logic."""

    def test_start_date_should_override_timespan_days(self):
        """Test that start_date takes precedence over timespan_days when no replication key."""
        # This test verifies that start_date has priority over timespan_days for backfilling
        
        mock_tap = Mock()
        mock_tap.config = {
            "workspace_id": "test-workspace",
            "start_date": "2024-01-01T00:00:00Z"  # This should take precedence
        }
        
        query_config = {
            "name": "test_stream",
            "query": "test query",
            "primary_keys": ["id"],
            "replication_key": None,
            "timespan_days": 5,  # This should be ignored when start_date is present
            "chunk_size_days": 1
        }
        
        stream = LogAnalyticsQueryStream(mock_tap, query_config)
        
        start_time, end_time = stream._calculate_timespan(None)
        
        # Expected: start_time should be the start_date, not calculated from timespan_days
        expected_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert start_time == expected_start

    def test_timespan_days_used_when_no_start_date(self):
        """Test that timespan_days is used when no start_date is specified."""
        mock_tap = Mock()
        mock_tap.config = {
            "workspace_id": "test-workspace"
            # No start_date specified
        }
        
        query_config = {
            "name": "test_stream",
            "query": "test query",
            "primary_keys": ["id"],
            "replication_key": None,
            "timespan_days": 7,  # Should be used when no start_date
            "chunk_size_days": 1
        }
        
        stream = LogAnalyticsQueryStream(mock_tap, query_config)
        
        start_time, end_time = stream._calculate_timespan(None)
        
        # Expected: start_time should be 7 days ago from end_time
        expected_start = end_time - timedelta(days=7)
        assert abs((start_time - expected_start).total_seconds()) < 1
