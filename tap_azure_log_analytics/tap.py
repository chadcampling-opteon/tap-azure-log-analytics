"""AzureLogAnalytics tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_azure_log_analytics import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapAzureLogAnalytics(Tap):
    """AzureLogAnalytics tap class."""

    name = "tap-azure-log-analytics"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "workspace_id",
            th.StringType(nullable=False),
            required=True,
            title="Workspace ID",
            description="Azure Log Analytics Workspace ID",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
        th.Property(
            "endpoint",
            th.StringType(nullable=False),
            title="Azure Endpoint",
            default="https://api.loganalytics.io",
            description="Azure cloud endpoint (e.g., https://api.loganalytics.io for public cloud)",
        ),
        th.Property(
            "queries",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True),
                    th.Property("query", th.StringType, required=True),
                    th.Property("primary_keys", th.ArrayType(th.StringType), nullable=True),
                    th.Property("replication_key", th.StringType, nullable=True),
                    th.Property("timespan_days", th.IntegerType, nullable=True),
                    th.Property("chunk_size_days", th.IntegerType, nullable=True),
                ),
                nullable=False,
            ),
            required=True,
            title="Queries",
            description="Array of query configurations for each stream",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.LogAnalyticsQueryStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        streams_list = []
        queries = self.config.get("queries", [])
        
        for query_config in queries:
            # Validate required fields
            if not all(key in query_config for key in ["name", "query"]):
                self.logger.warning(f"Skipping query with missing required fields: {query_config}")
                continue
                
            stream = streams.LogAnalyticsQueryStream(self, query_config)
            streams_list.append(stream)
            
        return streams_list


if __name__ == "__main__":
    TapAzureLogAnalytics.cli()
