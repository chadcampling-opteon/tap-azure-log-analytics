"""Stream type classes for tap-azure-log-analytics."""

from __future__ import annotations

import typing as t
from importlib import resources

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.streams import Stream
from azure.monitor.query import LogsQueryStatus

from tap_azure_log_analytics.client import AzureLogAnalyticsStream


class LogAnalyticsQueryStream(AzureLogAnalyticsStream):
    """Dynamic stream for Azure Log Analytics queries."""

    def __init__(self, tap: t.Any, query_config: dict[str, t.Any]) -> None:
        """Initialize the stream with query configuration.

        Args:
            tap: The tap instance.
            query_config: Configuration dictionary containing query details.
        """
        self.query_config = query_config

        super().__init__(tap, name=query_config["name"])

        self.primary_keys = query_config.get("primary_keys", [])
        self.replication_key = query_config.get("replication_key")

    @property
    def schema(self) -> dict[str, t.Any]:
        """Return the dynamically generated schema.

        Returns:
            JSON schema for the stream.
        """
        if self._schema is None:
            self._schema = self._generate_schema()
        return self._schema

    def _generate_schema(self) -> dict[str, t.Any]:
        """Generate schema from query results.

        Returns:
            JSON schema for the stream.
        """
        # Execute a sample query to get schema
        query = self.query_config.get("query", "")
        if not query:
            return th.PropertiesList().to_dict()

        try:
            # Get a small sample to determine schema
            from datetime import datetime, timedelta, timezone

            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=1)  # Small sample

            response = self.client.query_workspace(
                workspace_id=self.config["workspace_id"],
                query=query,
                timespan=(start_time, end_time),
            )

            if response.status in [LogsQueryStatus.SUCCESS, LogsQueryStatus.PARTIAL]:
                tables = (
                    response.tables
                    if response.status == LogsQueryStatus.SUCCESS
                    else response.partial_data
                )
                return self._generate_schema_from_results(tables)
            else:
                self.logger.warning(f"Failed to get schema for {self.name}: {response.status}")
                return th.PropertiesList().to_dict()

        except Exception as e:
            self.logger.warning(f"Error generating schema for {self.name}: {e}")
            return th.PropertiesList().to_dict()
