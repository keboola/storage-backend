# StorageDriverService gRPC Server Example

This directory contains an educational example implementation of the `StorageDriverService` as a pure gRPC server in Python.

## Overview

The implementation demonstrates:
- gRPC server setup and lifecycle management
- Command routing using `google.protobuf.Any` wrapper pattern
- Handler pattern for processing different command types
- Mock storage backend for demonstration
- Client library for easy interaction
- Error handling and logging patterns

## Architecture

```
┌─────────────┐          ┌──────────────────┐          ┌───────────────┐
│   Client    │─────────▶│   gRPC Server    │─────────▶│   Handlers    │
│             │          │  (Servicer)      │          │               │
└─────────────┘          └──────────────────┘          └───────┬───────┘
                                                                 │
                                                                 ▼
                                                        ┌────────────────┐
                                                        │ Mock Storage   │
                                                        │   Backend      │
                                                        └────────────────┘
```

### Components

1. **gRPC Server** (`grpc_server.py`)
   - Implements `StorageDriverService`
   - Routes commands to appropriate handlers
   - Manages server lifecycle

2. **Handlers** (`handlers/`)
   - `base_handler.py` - Abstract base with common logic
   - `table_handler.py` - Table operations (Create, Preview, Import)
   - `query_handler.py` - Query execution
   - `info_handler.py` - Metadata queries

3. **Storage Backend** (`storage/mock_storage.py`)
   - In-memory storage for demonstration
   - Shows integration patterns for real backends

4. **Client Library** (`client/grpc_client.py`)
   - Convenient Python client wrapper
   - Handles protobuf wrapping/unwrapping

5. **Configuration** (`config.py`, `utils.py`)
   - Server configuration
   - Logging and utility functions

## Prerequisites

```bash
# Install dependencies
cd generated-py
pip install -r requirements-grpc.txt
```

Or use the provided virtual environment:
```bash
cd generated-py
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

## Quick Start

### 1. Start the Server

```bash
cd generated-py
python3 -m examples.grpc_server

# Or with custom settings:
python3 -m examples.grpc_server --port 8080 --log-level DEBUG
```

The server will start on `localhost:50051` by default.

### 2. Run the Client Examples

In another terminal:

```bash
cd generated-py
python3 -m examples.client.client_examples
```

This will run several examples demonstrating:
- Creating tables
- Importing data
- Previewing table data
- Executing queries
- Getting object metadata
- Error handling

## Implemented Commands

This example implements 5 key commands:

| Command | Handler | Description |
|---------|---------|-------------|
| `CreateTableCommand` | `CreateTableHandler` | Create a new table with schema |
| `PreviewTableCommand` | `PreviewTableHandler` | Query table data with filters |
| `TableImportFromFileCommand` | `TableImportFromFileHandler` | Import data from S3/GCS/ABS |
| `ExecuteQueryCommand` | `ExecuteQueryHandler` | Execute ad-hoc SQL queries |
| `ObjectInfoCommand` | `ObjectInfoHandler` | Get object metadata |

These commands cover the main patterns needed for other operations.

## Usage Examples

### Using the Client Library

```python
from examples.client.grpc_client import StorageDriverClient

# Connect to server
with StorageDriverClient("localhost:50051") as client:
    # Create a table
    client.create_table(
        path=['my-project', 'my-bucket'],
        table_name='users',
        columns=[
            {'name': 'user_id', 'type': 'INTEGER', 'nullable': False},
            {'name': 'email', 'type': 'STRING', 'nullable': False},
            {'name': 'age', 'type': 'INTEGER', 'nullable': True},
        ],
        primary_keys=['user_id'],
        run_id='my-run-123'
    )

    # Import data
    import_response = client.import_from_file(
        path=['my-project', 'my-bucket'],
        table_name='users',
        file_provider="S3",
        file_path={
            'root': 'my-bucket',
            'path': 'data',
            'fileName': 'users.csv'
        }
    )
    print(f"Imported {import_response.importedRowsCount} rows")

    # Preview data
    preview = client.preview_table(
        path=['my-project', 'my-bucket'],
        table_name='users',
        limit=10
    )
    for row in preview.rows:
        print(row)

    # Get metadata
    info = client.get_object_info(
        path=['my-project', 'my-bucket'],
        object_name='users'
    )
    table_info = info.tableInfo[0]
    print(f"Table has {len(table_info.columns)} columns")
```

### Direct gRPC Usage

```python
import grpc
from proto import service_pb2_grpc, common_pb2, table_pb2

# Connect
channel = grpc.insecure_channel('localhost:50051')
stub = service_pb2_grpc.StorageDriverServiceStub(channel)

# Create command
cmd = table_pb2.CreateTableCommand()
cmd.path.extend(['my-project', 'my-bucket'])
cmd.tableName = 'users'

col = cmd.columns.add()
col.name = 'user_id'
col.type = 'INTEGER'
col.nullable = False

# Wrap in DriverRequest
request = common_pb2.DriverRequest()
request.command.Pack(cmd)
request.runtimeOptions.runId = 'my-run-123'

# Execute
response = stub.Execute(request)

# Check logs
for msg in response.messages:
    print(f"[{msg.level}] {msg.message}")

channel.close()
```

## Adding New Commands

To add support for a new command:

### 1. Create a Handler

```python
# examples/handlers/my_handler.py
from examples.handlers.base_handler import BaseCommandHandler
from proto import my_command_pb2

class MyCommandHandler(BaseCommandHandler):
    def handle(self, command, credentials, runtime_options):
        # Validate inputs
        self.validate_runtime_options(runtime_options)

        # Process command
        result = self.storage.do_something(...)

        # Log
        self.log_info("Command executed successfully")

        # Return response
        response = my_command_pb2.MyCommandResponse()
        # ... populate response
        return response
```

### 2. Register in Server

```python
# examples/grpc_server.py - in _route_command():
elif command_type_name == 'MyCommand':
    handler = MyCommandHandler(self.storage)
    command = my_command_pb2.MyCommand()
    command_any.Unpack(command)
    return handler, command
```

### 3. Add Client Method (Optional)

```python
# examples/client/grpc_client.py
def my_command(self, param1, param2, run_id=None):
    """Execute MyCommand."""
    cmd = my_command_pb2.MyCommand()
    cmd.param1 = param1
    cmd.param2 = param2

    response = self._execute(cmd, run_id=run_id)

    # Unpack and return
    my_response = my_command_pb2.MyCommandResponse()
    response.commandResponse.Unpack(my_response)
    return my_response
```

## Integrating Real Backends

The mock storage backend (`storage/mock_storage.py`) should be replaced with real database connectors:

### BigQuery Example

```python
from google.cloud import bigquery

class BigQueryBackend:
    def __init__(self, credentials):
        self.client = bigquery.Client(credentials=credentials)

    def create_table(self, path, table_name, columns, primary_keys):
        dataset_id = path[0]
        table_id = f"{dataset_id}.{table_name}"

        # Build schema
        schema = []
        for col in columns:
            field = bigquery.SchemaField(
                col.name,
                self._map_type(col.type),
                mode="REQUIRED" if not col.nullable else "NULLABLE"
            )
            schema.append(field)

        # Create table
        table = bigquery.Table(table_id, schema=schema)
        table = self.client.create_table(table)

    def _map_type(self, proto_type):
        mapping = {
            'INTEGER': 'INTEGER',
            'STRING': 'STRING',
            'FLOAT': 'FLOAT',
            'TIMESTAMP': 'TIMESTAMP',
            # ... more mappings
        }
        return mapping.get(proto_type, 'STRING')
```

### Snowflake Example

```python
import snowflake.connector

class SnowflakeBackend:
    def __init__(self, credentials):
        self.conn = snowflake.connector.connect(
            user=credentials.user,
            password=credentials.password,
            account=credentials.account,
            warehouse=credentials.warehouse,
            database=credentials.database,
            schema=credentials.schema
        )

    def create_table(self, path, table_name, columns, primary_keys):
        # Build CREATE TABLE statement
        col_defs = []
        for col in columns:
            nullable = "NULL" if col.nullable else "NOT NULL"
            col_def = f"{col.name} {col.type} {nullable}"
            col_defs.append(col_def)

        if primary_keys:
            pk_def = f"PRIMARY KEY ({', '.join(primary_keys)})"
            col_defs.append(pk_def)

        sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"

        # Execute
        cursor = self.conn.cursor()
        cursor.execute(sql)
```

## Testing

### Unit Tests

```bash
cd generated-py
python3 -m pytest examples/tests/test_handlers.py
```

### Integration Tests

```bash
# Start server in background
python3 -m examples.grpc_server &
SERVER_PID=$!

# Run tests
python3 -m pytest examples/tests/test_integration.py

# Stop server
kill $SERVER_PID
```

## Configuration

Server configuration can be customized via `config.py`:

```python
from examples.config import get_config

config = get_config(
    host="0.0.0.0",
    port=8080,
    max_workers=20,
    log_level="DEBUG",
    enable_reflection=True
)
```

Or via command-line arguments:

```bash
python3 -m examples.grpc_server \
    --host 0.0.0.0 \
    --port 8080 \
    --workers 20 \
    --log-level DEBUG
```

## Production Considerations

This is an educational example. For production use, consider:

### Security
- [ ] Add authentication (mTLS, JWT tokens)
- [ ] Implement authorization checks
- [ ] Encrypt credentials in transit and at rest
- [ ] Input validation and sanitization
- [ ] Rate limiting

### Performance
- [ ] Connection pooling to backends
- [ ] Caching frequently accessed data
- [ ] Batch operations where possible
- [ ] Optimize protobuf message sizes
- [ ] Monitor and tune thread pool size

### Reliability
- [ ] Health checks
- [ ] Graceful degradation
- [ ] Circuit breakers for backend connections
- [ ] Retry logic with exponential backoff
- [ ] Proper transaction handling

### Observability
- [ ] Structured logging
- [ ] Metrics (Prometheus, StatsD)
- [ ] Distributed tracing (OpenTelemetry)
- [ ] Request/response logging
- [ ] Error tracking (Sentry)

### Deployment
- [ ] Docker containerization
- [ ] Kubernetes deployment
- [ ] Load balancing
- [ ] Auto-scaling
- [ ] CI/CD pipeline

## Troubleshooting

### Server won't start

Check if the port is already in use:
```bash
lsof -i :50051
```

Use a different port:
```bash
python3 -m examples.grpc_server --port 8080
```

### Client connection refused

Ensure the server is running:
```bash
ps aux | grep grpc_server
```

Check server logs for errors.

### Import errors

Make sure you're in the `generated-py` directory:
```bash
cd /path/to/generated-py
python3 -m examples.grpc_server
```

Or set PYTHONPATH:
```bash
export PYTHONPATH="/path/to/generated-py:$PYTHONPATH"
```

### gRPC errors

Enable debug logging:
```bash
export GRPC_VERBOSITY=DEBUG
export GRPC_TRACE=all
python3 -m examples.grpc_server --log-level DEBUG
```

## Resources

- [gRPC Python Documentation](https://grpc.io/docs/languages/python/)
- [Protocol Buffers Python Tutorial](https://developers.google.com/protocol-buffers/docs/pythontutorial)
- [Protocol Buffers Any Type](https://developers.google.com/protocol-buffers/docs/proto3#any)
- [gRPC Best Practices](https://grpc.io/docs/guides/performance/)

## License

This example is part of the Keboola Storage Driver Common package.
