"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_azure_log_analytics.tap import TapAzureLogAnalytics

SAMPLE_CONFIG = {
    "workspace_id": "DEMO_WORKSPACE",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "queries": [
        {
            "name": "test_stream",
            "query": "AppExceptions | order by TimeGenerated | take 100",
            "primary_keys": ["TimeGenerated", "OperationId"],
            "replication_key": None,
            "timespan_days": 1,
            "chunk_size_days": 1
        }
    ]
}


# Run standard built-in tap tests from the SDK:
TestTapAzureLogAnalytics = get_tap_test_class(
    tap_class=TapAzureLogAnalytics,
    config=SAMPLE_CONFIG,
)


# TODO: Create additional tests as appropriate for your tap.
