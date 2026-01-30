# Keboola Storage Driver - Python Protocol Buffers

This directory contains automatically generated Python code from Protocol Buffer definitions for the Keboola Storage Driver.

## Installation

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

Or install protobuf directly:

```bash
pip install protobuf
```

### 2. Add to Python Path

Add the `generated-py` directory to your Python path:

```python
import sys
sys.path.insert(0, '/path/to/generated-py')
```

Or set the `PYTHONPATH` environment variable:

```bash
export PYTHONPATH="/path/to/generated-py:$PYTHONPATH"
```

## Quick Start

### Basic Usage

```python
import sys
sys.path.insert(0, '/path/to/generated-py')

from proto import common_pb2

# Create a driver request
request = common_pb2.DriverRequest()
request.features.append('my-feature')
request.runtimeOptions.runId = 'run-123'
request.runtimeOptions.queryTags['branchId'] = 'main'

# Serialize to binary
binary_data = request.SerializeToString()

# Deserialize from binary
request2 = common_pb2.DriverRequest()
request2.ParseFromString(binary_data)

print(f"Run ID: {request2.runtimeOptions.runId}")
```

### Creating Table Commands

```python
from proto import table_pb2

# Create a table
cmd = table_pb2.CreateTableCommand()
cmd.path.extend(['project-123', 'bucket-456'])
cmd.tableName = 'users'
cmd.primaryKeysNames.append('user_id')

# Add column definition
col = cmd.columns.add()
col.name = 'user_id'
col.type = 'INTEGER'
col.nullable = False

col = cmd.columns.add()
col.name = 'email'
col.type = 'STRING'
col.nullable = False
```

### Working with BigQuery Backend

```python
from proto import table_pb2
from proto.backend import bigQuery_pb2

# Create table with BigQuery-specific options
cmd = table_pb2.CreateTableCommand()
cmd.tableName = 'events'

# Configure BigQuery metadata
bq_meta = table_pb2.CreateTableCommand.BigQueryTableMeta()
bq_meta.requirePartitionFilter = True

# Set time partitioning
bq_meta.timePartitioning.type = 'DAY'
bq_meta.timePartitioning.field = 'event_timestamp'

# Set clustering
bq_meta.clustering.fields.append('user_id')
bq_meta.clustering.fields.append('event_type')

# Pack into the command's meta field
cmd.meta.Pack(bq_meta)
```

### JSON Serialization

```python
from proto import common_pb2
from google.protobuf import json_format

# Create message
request = common_pb2.DriverRequest()
request.runtimeOptions.runId = 'test-123'

# Convert to JSON
json_str = json_format.MessageToJson(request)
print(json_str)

# Parse from JSON
request2 = common_pb2.DriverRequest()
json_format.Parse(json_str, request2)
```

## Available Modules

### `proto.common_pb2`
Core message types:
- `DriverRequest` - Main request wrapper for all driver commands
- `DriverResponse` - Main response wrapper
- `RuntimeOptions` - Runtime configuration (runId, queryTags, meta)
- `LogMessage` - Structured logging messages (PSR-3 compliant)

### `proto.table_pb2`
Table operations:
- `CreateTableCommand` / `DropTableCommand`
- `AddColumnCommand` / `AlterColumnCommand` / `DropColumnCommand`
- `AddPrimaryKeyCommand` / `DropPrimaryKeyCommand`
- `TableImportFromFileCommand` / `TableImportResponse`
- `TableImportFromTableCommand`
- `TableExportToFileCommand` / `TableExportToFileResponse`
- `PreviewTableCommand` / `PreviewTableResponse`
- `DeleteTableRowsCommand` / `DeleteTableRowsResponse`
- `ImportExportShared` - Shared types for import/export operations

### `proto.bucket_pb2`
Bucket operations and management

### `proto.workspace_pb2`
Workspace operations and management

### `proto.project_pb2`
Project-level operations

### `proto.credentials_pb2`
Credentials handling for different backends

### `proto.info_pb2`
Information and metadata queries

### `proto.executeQuery_pb2`
Query execution commands

### `proto.backend_pb2`
Generic backend operations

### `proto.backend.bigQuery_pb2`
BigQuery-specific types:
- `TimePartitioning` - Time-based partitioning configuration
- `RangePartitioning` - Range-based partitioning configuration
- `Clustering` - Clustering configuration

## gRPC Server Example

This package includes a complete example implementation of a gRPC server for the StorageDriverService.

### Features

- **Pure gRPC server** with command routing
- **5 example command handlers**: CreateTable, PreviewTable, ImportFromFile, ExecuteQuery, ObjectInfo
- **Mock storage backend** for demonstration
- **Client library** for easy interaction
- **Comprehensive documentation** and examples

### Quick Start

```bash
# Install gRPC dependencies
pip install -r requirements-grpc.txt

# Start the server
python3 -m examples.grpc_server

# In another terminal, run the examples
python3 -m examples.client.client_examples
```

### Usage Example

```python
from examples.client.grpc_client import StorageDriverClient

with StorageDriverClient("localhost:50051") as client:
    # Create a table
    client.create_table(
        path=['my-project', 'my-bucket'],
        table_name='users',
        columns=[
            {'name': 'user_id', 'type': 'INTEGER', 'nullable': False},
            {'name': 'email', 'type': 'STRING', 'nullable': False},
        ],
        primary_keys=['user_id']
    )

    # Import data
    response = client.import_from_file(
        path=['my-project', 'my-bucket'],
        table_name='users',
        file_provider="S3",
        file_path={'root': 'bucket', 'path': 'data', 'fileName': 'users.csv'}
    )
    print(f"Imported {response.importedRowsCount} rows")

    # Preview data
    preview = client.preview_table(
        path=['my-project', 'my-bucket'],
        table_name='users',
        limit=10
    )
```

### Documentation

See **[examples/README.md](examples/README.md)** for complete documentation including:
- Architecture overview
- Adding new commands
- Integrating real backends (BigQuery, Snowflake, etc.)
- Production considerations
- Testing
- Troubleshooting

## File Structure

```
generated-py/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
└── proto/
    ├── __init__.py
    ├── backend/
    │   ├── __init__.py
    │   └── bigQuery_pb2.py   # BigQuery backend types
    ├── backend_pb2.py        # Generic backend types
    ├── bucket_pb2.py         # Bucket operations
    ├── common_pb2.py         # Core request/response types
    ├── credentials_pb2.py    # Credentials management
    ├── executeQuery_pb2.py   # Query execution
    ├── info_pb2.py           # Information queries
    ├── project_pb2.py        # Project operations
    ├── service_pb2.py        # Service operations
    ├── table_pb2.py          # Table operations
    └── workspace_pb2.py      # Workspace operations
```

## Regenerating Python Code

This directory is automatically generated from `.proto` files. To regenerate after updating proto definitions:

```bash
cd /path/to/php-storage-driver-common
composer protobuf
```

Or run protoc directly:

```bash
protoc $(find proto -iname "*.proto") --python_out=generated-py
```

**Note:** After regeneration, the `__init__.py` files, `requirements.txt`, and this `README.md` should be preserved or recreated.

## Additional Resources

- [Protocol Buffers Documentation](https://developers.google.com/protocol-buffers/docs/pythontutorial)
- [Python API Reference](https://developers.google.com/protocol-buffers/docs/reference/python-generated)

## Version

Generated from proto definitions in: `php-storage-driver-common/proto/`

Python protobuf version required: `>=3.21.0`
