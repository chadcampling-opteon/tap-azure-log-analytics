# tap-azure-log-analytics

`tap-azure-log-analytics` is a Singer tap for Azure Log Analytics that enables you to extract data from Azure Log Analytics workspaces using KQL (Kusto Query Language) queries.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Features

- **Flexible Query Configuration**: Define custom KQL queries for each data stream
- **Automatic Schema Discovery**: Dynamically generates schemas from query results
- **Dynamic Type Handling**: Intelligently maps Azure's `dynamic` columns to appropriate Singer types
- **Timestamp Chunking**: Automatically handles large datasets by splitting queries into time-based chunks
- **Multi-Cloud Support**: Works with Azure Public, Government, and China clouds
- **Incremental Replication**: Supports incremental data extraction using replication keys
- **Azure Authentication**: Uses Azure's DefaultAzureCredential for seamless authentication

## Installation


### From Source

```bash
uv tool install git+https://github.com/chadcampling-opteon/tap-azure-log-analytics.git@main
```

## Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `workspace_id` | string | Azure Log Analytics Workspace ID (required) |
| `queries` | array | Array of query configurations for each stream (required) |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `start_date` | string | null | Initial date to start extracting data from (ISO 8601 format) |
| `endpoint` | string | `https://api.loganalytics.io` | Azure cloud endpoint |
| `stream_maps` | object | null | Stream mapping configuration |
| `flattening_enabled` | boolean | null | Enable schema flattening |
| `batch_config` | object | null | Batch processing configuration |

### Query Configuration

Each query in the `queries` array supports the following properties:

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `name` | string | Yes | Stream name for the query |
| `query` | string | Yes | KQL query to execute |
| `primary_keys` | array | No | Array of column names that form the primary key |
| `replication_key` | string | No | Column name for incremental replication |
| `timespan_days` | integer | No | Number of days to query when no replication key |
| `chunk_size_days` | integer | No | Days per chunk for large datasets (default: 1) |

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Azure Authentication and Authorization

This tap uses Azure's `DefaultAzureCredential` for authentication, which automatically tries multiple authentication methods in order:

1. **Environment variables** (for service principals)
2. **Managed Identity** (when running in Azure)
3. **Azure CLI** (when logged in via `az login`)
4. **Azure PowerShell** (when logged in via `Connect-AzAccount`)
5. **Visual Studio Code** (when logged in via Azure extension)
6. **Azure PowerShell Core** (when logged in via `Connect-AzAccount`)

#### Required Azure Permissions

Your Azure identity needs the following permissions on the Log Analytics workspace:

- **Log Analytics Reader** role, or
- **Reader** role with **Log Analytics Reader** permissions

#### Authentication Methods

**Option 1: Azure CLI (Recommended for development)**
```bash
az login
az account set --subscription "your-subscription-id"
```

**Option 2: Service Principal (Recommended for production)**
```bash
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
export AZURE_TENANT_ID="your-tenant-id"
```

**Option 3: Managed Identity (When running in Azure)**
No additional configuration needed - the tap will automatically use the managed identity.

#### Different Azure Clouds

For different Azure clouds, set the appropriate endpoint:

- **Azure Public Cloud**: `https://api.loganalytics.io` (default)
- **Azure Government**: `https://api.loganalytics.us`
- **Azure China**: `https://api.loganalytics.cn`

## Usage Examples

### Basic Configuration

```json
{
  "workspace_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "endpoint": "https://api.loganalytics.io",
  "start_date": "2024-01-01T00:00:00Z",
  "queries": [
    {
      "name": "azure_activity",
      "query": "AzureActivity | project TimeGenerated, OperationName, ResourceGroup, Category",
      "primary_keys": ["TimeGenerated", "OperationName"],
      "replication_key": "TimeGenerated",
      "chunk_size_days": 1
    },
    {
      "name": "heartbeat",
      "query": "Heartbeat | project TimeGenerated, Computer, OSType, Version",
      "primary_keys": ["TimeGenerated", "Computer"],
      "replication_key": "TimeGenerated",
      "timespan_days": 7,
      "chunk_size_days": 1
    }
  ]
}
```

### Advanced Query Examples

**Security Events**
```json
{
  "name": "security_events",
  "query": "SecurityEvent | where EventID in (4624, 4625, 4634) | project TimeGenerated, Computer, EventID, AccountName",
  "primary_keys": ["TimeGenerated", "Computer", "EventID"],
  "replication_key": "TimeGenerated",
  "chunk_size_days": 1
}
```

**Performance Counters**
```json
{
  "name": "perf_counters",
  "query": "Perf | where CounterName == 'Processor Time' | project TimeGenerated, Computer, CounterName, CounterValue",
  "primary_keys": ["TimeGenerated", "Computer", "CounterName"],
  "replication_key": "TimeGenerated",
  "chunk_size_days": 1
}
```

**Custom Logs with Dynamic Columns**
```json
{
  "name": "custom_logs",
  "query": "MyCustomLog | project TimeGenerated, Computer, Message, Properties",
  "primary_keys": ["TimeGenerated", "Computer"],
  "replication_key": "TimeGenerated",
  "chunk_size_days": 1
}
```

### Executing the Tap Directly

```bash
# Discover available streams
tap-azure-log-analytics --config config.json --discover > catalog.json

# Run the tap
tap-azure-log-analytics --config config.json --catalog catalog.json

# Run with state for incremental replication
tap-azure-log-analytics --config config.json --catalog catalog.json --state state.json
```

## Schema Discovery and Dynamic Types

This tap automatically discovers schemas by executing sample queries and analyzing the results. It intelligently handles Azure's `dynamic` columns by:

1. **Analyzing sample data** to determine the structure
2. **Mapping to appropriate Singer types**:
   - Arrays of objects → `ArrayType(ObjectType)`
   - Arrays of primitives → `ArrayType(StringType)`
   - Single objects → `ObjectType`
   - Primitive values → `StringType`

3. **Handling mixed content** by prioritizing the most complex structure found

## Performance Tuning

### Chunk Size Configuration

For large datasets, adjust `chunk_size_days` based on your data volume:

- **High volume** (millions of rows): Use `chunk_size_days: 1`
- **Medium volume** (hundreds of thousands): Use `chunk_size_days: 3-7`
- **Low volume** (thousands): Use `chunk_size_days: 30` or larger

### Timespan Configuration

- **Incremental replication**: Use `replication_key` for automatic incremental updates
- **One-time backfill**: Use `start_date` to specify the start time
- **Regular snapshots**: Use `timespan_days` for fixed time windows

## Troubleshooting

### Common Issues

**Authentication Errors**
```
Error: Authentication failed
```
- Ensure you're logged in via `az login`
- Check that your identity has Log Analytics Reader permissions
- Verify the workspace ID is correct

**Query Timeout**
```
Error: Query timeout
```
- Reduce `chunk_size_days` to process smaller time windows
- Optimize your KQL query for better performance
- Check if the query returns too many rows (limit with `take` clause)

**Schema Discovery Issues**
```
Warning: Failed to get schema
```
- Ensure your query returns data in the specified time range
- Check that the query syntax is valid
- Verify you have permissions to read the tables

**Large Dataset Handling**
- Use smaller `chunk_size_days` values
- Add `| take 1000` to your queries for testing

### Debug Mode

Enable debug logging to troubleshoot issues:

```bash
export TAP_AZURE_LOG_ANALYTICS_LOG_LEVEL=DEBUG
tap-azure-log-analytics --config config.json --discover
```

## Testing with Meltano

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

### Install Meltano

```bash
# Install meltano
uv tool install meltano
# Initialize meltano within this directory
cd tap-azure-log-analytics
meltano install
```

### Run ELT Pipeline

```bash
# Test invocation:
meltano invoke tap-azure-log-analytics --version

# OR run a test ELT pipeline:
meltano run tap-azure-log-analytics target-jsonl
```

## Developer Resources

### Initialize your Development Environment

Prerequisites:

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and then run:

```bash
uv run pytest
```

You can also test the `tap-azure-log-analytics` CLI interface directly using `uv run`:

```bash
uv run tap-azure-log-analytics --help
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
