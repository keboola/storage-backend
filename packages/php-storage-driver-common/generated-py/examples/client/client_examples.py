"""
Example client usage for StorageDriverService.

This module demonstrates how to use the StorageDriverClient to interact
with the gRPC server.

Usage:
    # Start the server first:
    python3 examples/grpc_server.py

    # Then run the examples:
    python3 examples/client/client_examples.py
"""

import logging
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from examples.client.grpc_client import StorageDriverClient
from examples.utils import setup_logging


def example_create_and_query_table():
    """
    Example: Create a table, import data, and query it.
    """
    print("\n" + "="*60)
    print("Example: Create Table, Import Data, and Query")
    print("="*60)

    with StorageDriverClient("localhost:50051") as client:
        # 1. Create a table
        print("\n1. Creating table 'users'...")
        client.create_table(
            path=['demo-project', 'demo-bucket'],
            table_name='users',
            columns=[
                {'name': 'user_id', 'type': 'INTEGER', 'nullable': False},
                {'name': 'email', 'type': 'STRING', 'nullable': False},
                {'name': 'age', 'type': 'INTEGER', 'nullable': True},
                {'name': 'created_at', 'type': 'TIMESTAMP', 'nullable': True},
            ],
            primary_keys=['user_id'],
            run_id='example-run-001'
        )
        print("✓ Table created successfully")

        # 2. Import data from file
        print("\n2. Importing data from file...")
        import_response = client.import_from_file(
            path=['demo-project', 'demo-bucket'],
            table_name='users',
            file_provider="S3",
            file_path={
                'root': 'my-bucket',
                'path': 'data/users',
                'fileName': 'users.csv'
            },
            run_id='example-run-001'
        )
        print(f"✓ Imported {import_response.importedRowsCount} rows")
        print(f"  Total rows in table: {import_response.tableRowsCount}")
        print(f"  Table size: {import_response.tableSizeBytes} bytes")

        # 3. Preview table data
        print("\n3. Previewing table data...")
        preview_response = client.preview_table(
            path=['demo-project', 'demo-bucket'],
            table_name='users',
            columns=['user_id', 'email', 'age'],
            limit=5,
            run_id='example-run-001'
        )
        print(f"✓ Retrieved {len(preview_response.rows)} rows")
        print(f"  Columns: {', '.join(preview_response.columns)}")

        # Display first few rows
        print("\n  Sample data:")
        for i, row in enumerate(preview_response.rows[:3]):
            values = []
            for col in row.columns:
                if col.value.HasField('string_value'):
                    values.append(f"{col.columnName}={col.value.string_value}")
                elif col.value.HasField('number_value'):
                    values.append(f"{col.columnName}={col.value.number_value}")
            print(f"    Row {i+1}: {', '.join(values)}")

        # 4. Get table info
        print("\n4. Getting table metadata...")
        info_response = client.get_object_info(
            path=['demo-project', 'demo-bucket'],
            object_name='users',
            run_id='example-run-001'
        )
        if info_response.tableInfo:
            table_info = info_response.tableInfo[0]
            print(f"✓ Table: {table_info.tableName}")
            print(f"  Columns: {len(table_info.columns)}")
            print(f"  Primary keys: {', '.join(table_info.primaryKeysNames)}")
            print(f"  Rows: {table_info.rowsCount}")


def example_execute_query():
    """
    Example: Execute ad-hoc SQL queries.
    """
    print("\n" + "="*60)
    print("Example: Execute SQL Query")
    print("="*60)

    with StorageDriverClient("localhost:50051") as client:
        print("\nExecuting query...")
        query_response = client.execute_query(
            sql="SELECT * FROM demo_project.demo_bucket.users WHERE age > 18 LIMIT 10",
            run_id='example-run-002'
        )
        print("✓ Query executed successfully")


def example_multiple_tables():
    """
    Example: Working with multiple tables.
    """
    print("\n" + "="*60)
    print("Example: Multiple Tables")
    print("="*60)

    with StorageDriverClient("localhost:50051") as client:
        # Create orders table
        print("\n1. Creating 'orders' table...")
        client.create_table(
            path=['demo-project', 'demo-bucket'],
            table_name='orders',
            columns=[
                {'name': 'order_id', 'type': 'INTEGER', 'nullable': False},
                {'name': 'user_id', 'type': 'INTEGER', 'nullable': False},
                {'name': 'total', 'type': 'DECIMAL', 'nullable': False},
                {'name': 'status', 'type': 'STRING', 'nullable': False},
            ],
            primary_keys=['order_id'],
            run_id='example-run-003'
        )
        print("✓ Orders table created")

        # Import data into orders
        print("\n2. Importing orders data...")
        import_response = client.import_from_file(
            path=['demo-project', 'demo-bucket'],
            table_name='orders',
            file_provider="S3",
            file_path={
                'root': 'my-bucket',
                'path': 'data/orders',
                'fileName': 'orders.csv'
            },
            run_id='example-run-003'
        )
        print(f"✓ Imported {import_response.importedRowsCount} rows into orders")


def example_error_handling():
    """
    Example: Error handling.
    """
    print("\n" + "="*60)
    print("Example: Error Handling")
    print("="*60)

    with StorageDriverClient("localhost:50051") as client:
        # Try to preview a non-existent table
        print("\n1. Attempting to preview non-existent table...")
        try:
            preview_response = client.preview_table(
                path=['demo-project', 'demo-bucket'],
                table_name='non_existent_table',
                limit=10
            )
        except Exception as e:
            print(f"✓ Caught expected error: {e}")

        # Try to create a table with invalid name
        print("\n2. Attempting to create table with invalid parameters...")
        try:
            client.create_table(
                path=['demo-project', 'demo-bucket'],
                table_name='',  # Invalid: empty name
                columns=[
                    {'name': 'col1', 'type': 'STRING', 'nullable': False}
                ]
            )
        except Exception as e:
            print(f"✓ Caught expected error: {e}")


def main():
    """
    Run all examples.
    """
    # Set up logging
    setup_logging(logging.INFO)

    print("\n" + "="*60)
    print("StorageDriverService gRPC Client Examples")
    print("="*60)
    print("\nMake sure the gRPC server is running:")
    print("  python3 examples/grpc_server.py")
    print("\nPress Enter to continue...")
    input()

    try:
        # Run examples
        example_create_and_query_table()
        example_execute_query()
        example_multiple_tables()
        example_error_handling()

        print("\n" + "="*60)
        print("All examples completed successfully!")
        print("="*60 + "\n")

    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        print("\nMake sure the gRPC server is running:")
        print("  python3 examples/grpc_server.py")
        sys.exit(1)


if __name__ == "__main__":
    main()
