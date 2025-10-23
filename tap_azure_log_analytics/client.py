"""Azure Log Analytics client handling, including AzureLogAnalyticsStream base class."""

from __future__ import annotations

import logging
import sys
import typing as t
from datetime import datetime, timedelta, timezone
from functools import cached_property
from importlib import resources

from azure.core.exceptions import HttpResponseError
from azure.core.credentials import AzureKeyCredential
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from singer_sdk import typing as th
from singer_sdk.streams import Stream

from tap_azure_log_analytics.auth import AzureLogAnalyticsAuthenticator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

# Also configure azure http logging for broader Azure SDK verbosity control
azure_http_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
azure_http_logger.setLevel(logging.WARNING)


class AzureLogAnalyticsStream(Stream):
    """Azure Log Analytics stream class."""

    def __init__(self, tap: t.Any, name: str | None = None) -> None:
        """Initialize the stream.

        Args:
            tap: The tap instance.
            name: The stream name.
        """
        super().__init__(tap, name=name)
        self._client: LogsQueryClient | None = None
        self._schema: dict[str, t.Any] | None = None

    @cached_property
    def client(self) -> LogsQueryClient:
        """Get the Azure Log Analytics client.

        Returns:
            LogsQueryClient instance.
        """
        if self._client is None:
            workspace_id = self.config.get("workspace_id")
            authenticator = AzureLogAnalyticsAuthenticator(workspace_id=workspace_id)
            credential = authenticator.credential

            # Handle different credential types appropriately
            if isinstance(credential, AzureKeyCredential):
                # AzureKeyCredential is not a TokenCredential, so we need to use
                # a TokenCredential for the LogsQueryClient constructor, but the
                # actual authentication will be handled by the authentication_policy
                from azure.identity import DefaultAzureCredential

                self._client = LogsQueryClient(
                    credential=DefaultAzureCredential(),
                    authentication_policy=authenticator.authentication_policy,
                    endpoint=self.config.get("endpoint", "https://api.loganalytics.io"),
                )
            else:
                # DefaultAzureCredential is a TokenCredential, so we can use it directly
                self._client = LogsQueryClient(
                    credential=credential,
                    authentication_policy=authenticator.authentication_policy,
                    endpoint=self.config.get("endpoint", "https://api.loganalytics.io"),
                )
        return self._client

    def _map_column_type(self, column_type: str) -> th.JSONTypeHelper:
        """Map Azure Log Analytics column type to Singer type.

        Args:
            column_type: The Azure column type.

        Returns:
            Singer type helper.
        """
        type_mapping = {
            "string": th.StringType(),
            "guid": th.UUIDType(),
            "long": th.IntegerType(),
            "int": th.IntegerType(),
            "real": th.NumberType(),
            "decimal": th.DecimalType(),
            "bool": th.BooleanType(),
            "datetime": th.DateTimeType(),
            "timespan": th.StringType(),
            "dynamic": th.StringType(),  # Treat dynamic types as strings
        }

        return type_mapping.get(column_type.lower(), th.StringType())  # type: ignore[return-value]

    def _generate_schema_from_results(self, tables: list[t.Any]) -> dict[str, t.Any]:
        """Generate schema from query results.

        Args:
            tables: List of tables from query results.

        Returns:
            JSON schema for the stream.
        """
        if not tables:
            return th.PropertiesList().to_dict()

        # Use the first table to determine schema
        table = tables[0]
        properties = th.PropertiesList()

        # Handle Azure Log Analytics table structure with separate columns and column_types
        column_names = table.columns
        column_types = table.columns_types

        for name, col_type in zip(column_names, column_types):
            singer_type = self._map_column_type(col_type)
            properties.append(
                th.Property(
                    name,
                    singer_type,
                )
            )

        return properties.to_dict()

    def _calculate_timespan(self, context: Context | None) -> tuple[datetime, datetime]:
        """Calculate the timespan for the query.

        Args:
            context: The stream context.

        Returns:
            Tuple of (start_time, end_time).
        """
        # Get end time (default to now - 5 minutes to ensure all data has landed)
        end_time = datetime.now(timezone.utc) - timedelta(minutes=5)

        # Get start time from replication key state, start_date, or timespan_days
        if self.replication_key:
            start_time = self.get_starting_timestamp(context)
        else:
            # First check for start_date from config (for backfilling)
            start_time = self.config.get("start_date")

            if start_time is not None:
                # Convert string to datetime if needed
                if isinstance(start_time, str):
                    try:
                        from dateutil.parser import parse  # type: ignore[import-untyped]

                        start_time = parse(start_time)
                    except ImportError:
                        # Fallback to datetime.fromisoformat if dateutil is not available
                        start_time = datetime.fromisoformat(start_time)
            else:
                # Fall back to timespan_days if no start_date specified
                timespan_days = getattr(self, "query_config", {}).get("timespan_days")
                if timespan_days:
                    # Use timespan_days to calculate start time
                    start_time = end_time - timedelta(days=timespan_days)
                else:
                    # Default to 1 day ago if no start time specified
                    start_time = end_time - timedelta(days=1)

        # Ensure timezone awareness
        if start_time is not None and start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)

        # Ensure we have a valid start_time
        if start_time is None:
            start_time = end_time - timedelta(days=1)

        return start_time, end_time

    def _chunk_timespan(
        self, start_time: datetime, end_time: datetime, chunk_days: int = 1
    ) -> list[tuple[datetime, datetime]]:
        """Split timespan into chunks to handle 500k row limit and memory constraints.

        Args:
            start_time: Start of the timespan.
            end_time: End of the timespan.
            chunk_days: Number of days per chunk.

        Returns:
            List of (start, end) tuples for each chunk.
        """
        # Handle edge cases
        if start_time >= end_time:
            return []

        if chunk_days <= 0:
            # Invalid chunk size, return single chunk
            return [(start_time, end_time)]

        chunks = []
        current_start = start_time

        while current_start < end_time:
            current_end = min(current_start + timedelta(days=chunk_days), end_time)
            chunks.append((current_start, current_end))
            current_start = current_end

        return chunks

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """Get records from Azure Log Analytics.

        Args:
            context: The stream context.

        Yields:
            Records from the query.
        """
        # Get query configuration
        query = getattr(self, "query_config", {}).get("query", "")
        if not query:
            self.logger.error("No query configured for stream")
            return

        # Calculate timespan
        start_time, end_time = self._calculate_timespan(context)

        # Get chunk size from config
        chunk_days = getattr(self, "query_config", {}).get("chunk_size_days", 1)

        # Split into chunks if needed
        chunks = self._chunk_timespan(start_time, end_time, chunk_days)

        for chunk_start, chunk_end in chunks:
            self.logger.info(f"Querying {self.name} from {chunk_start} to {chunk_end}")

            try:
                # Execute query
                response = self.client.query_workspace(
                    workspace_id=self.config["workspace_id"],
                    query=query,
                    timespan=(chunk_start, chunk_end),
                )

                # Handle response
                if response.status == LogsQueryStatus.SUCCESS:
                    for table in response.tables:
                        for row in table.rows:
                            # Convert row to dict using column names
                            record = dict(zip(table.columns, row))
                            yield record

                elif response.status == LogsQueryStatus.PARTIAL:
                    self.logger.warning(
                        f"Partial results for {self.name} in chunk {chunk_start} to {chunk_end}"
                    )
                    for table in response.partial_data:
                        for row in table.rows:
                            record = dict(zip(table.columns, row))
                            yield record

            except HttpResponseError as e:
                self.logger.error(f"Error querying {self.name}: {e}")
                raise
