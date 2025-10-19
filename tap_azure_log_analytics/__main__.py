"""AzureLogAnalytics entry point."""

from __future__ import annotations

from tap_azure_log_analytics.tap import TapAzureLogAnalytics

TapAzureLogAnalytics.cli()
