<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\ToFinal;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeException;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\FullImporter;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Tests\Keboola\Db\ImportExportFunctional\Snowflake\SnowflakeBaseTestCase;

class FullImporterValidationTest extends SnowflakeBaseTestCase
{
    public const TESTS_PREFIX = 'import_export_test_';
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'schema';
    public const TEST_STAGING_TABLE = '__temp_stagingTable';
    public const TEST_DESTINATION_TABLE = self::TESTS_PREFIX . 'destination';

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema(self::TEST_SCHEMA);
        $this->createSchema(self::TEST_SCHEMA);
    }

    /**
     * @param array<array{name: string, type?: string, length?: string, nullable?: bool}> $columns
     * @param string[] $primaryKeys
     * @return SnowflakeTableDefinition
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    private function createStagingTableDefinition(array $columns, array $primaryKeys = []): SnowflakeTableDefinition
    {
        $columnDefinitions = [];
        foreach ($columns as $column) {
            $columnDefinitions[] = new SnowflakeColumn(
                $column['name'],
                new Snowflake(
                    $column['type'] ?? Snowflake::TYPE_VARCHAR,
                    [
                        'length' => $column['length'] ?? '50',
                        'nullable' => $column['nullable'] ?? true,
                    ],
                ),
            );
        }

        return new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection($columnDefinitions),
            $primaryKeys,
        );
    }

    /**
     * @param array<array{name: string, type?: string, length?: string, nullable?: bool}> $columns
     * @param string[] $primaryKeys
     * @return SnowflakeTableDefinition
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    private function createDestinationTableDefinition(array $columns, array $primaryKeys = []): SnowflakeTableDefinition
    {
        $columnDefinitions = [];
        foreach ($columns as $column) {
            $columnDefinitions[] = new SnowflakeColumn(
                $column['name'],
                new Snowflake(
                    $column['type'] ?? Snowflake::TYPE_VARCHAR,
                    [
                        'length' => $column['length'] ?? '30',
                        'nullable' => $column['nullable'] ?? true,
                    ],
                ),
            );
        }

        return new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_DESTINATION_TABLE,
            false,
            new ColumnCollection($columnDefinitions),
            $primaryKeys,
        );
    }

    private function createTableFromDefinition(SnowflakeTableDefinition $definition): void
    {
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($definition));
    }

    public function testValidateTableDefinitionsMatchSuccess(): void
    {
        // Create staging table with columns col1 and col2
        $stagingDef = $this->createStagingTableDefinition(
            [
                ['name' => 'col1'],
                ['name' => 'col2'],
            ],
            ['col1'],
        );
        $this->createTableFromDefinition($stagingDef);

        // Create destination table with the same columns and primary keys
        $destinationDef = $this->createDestinationTableDefinition(
            [
                ['name' => 'col1'],
                ['name' => 'col2'],
            ],
            ['col1'],
        );
        $this->createTableFromDefinition($destinationDef);

        // Create the importer and call doFullLoadWithCTAS through the importToTable method
        $importer = new FullImporter($this->connection);
        $options = new SnowflakeImportOptions(
            [],
            false,
            false,
            0,
            SnowflakeImportOptions::SAME_TABLES_NOT_REQUIRED,
            SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            [],
            ['ctas-om'], // Enable CTAS feature
        );

        // This should not throw an exception
        $result = $importer->importToTable(
            $stagingDef,
            $destinationDef,
            $options,
            new ImportState('testTable'),
        );

        // Verify the import was successful
        self::assertNotNull($result);
    }

    public function testValidateTableDefinitionsMatchColumnCountMismatch(): void
    {
        // Create staging table with columns col1 and col2
        $stagingDef = $this->createStagingTableDefinition(
            [
                ['name' => 'col1'],
                ['name' => 'col2'],
            ],
            ['col1'],
        );
        $this->createTableFromDefinition($stagingDef);

        // Create destination table with an extra column
        $destinationDef = $this->createDestinationTableDefinition(
            [
                ['name' => 'col1'],
                ['name' => 'col2'],
                ['name' => 'col3'],
            ],
            ['col1'],
        );
        $this->createTableFromDefinition($destinationDef);

        // Create the importer
        $importer = new FullImporter($this->connection);
        $options = new SnowflakeImportOptions(
            [],
            false,
            false,
            0,
            SnowflakeImportOptions::SAME_TABLES_NOT_REQUIRED,
            SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            [],
            features: ['ctas-om'], // Enable CTAS feature
        );

        // This should throw an exception about column count mismatch
        $this->expectException(SnowflakeException::class);
        $this->expectExceptionMessage('Column count mismatch between staging (2) and destination (3) tables');

        $importer->importToTable(
            $stagingDef,
            $destinationDef,
            $options,
            new ImportState('testTable'),
        );
    }

    public function testValidateTableDefinitionsMatchColumnNamesMismatch(): void
    {
        // Create staging table with columns col1 and col2
        $stagingDef = $this->createStagingTableDefinition(
            [
                ['name' => 'col1'],
                ['name' => 'col2'],
            ],
            ['col1'],
        );
        $this->createTableFromDefinition($stagingDef);

        // Create destination table with different column names
        $destinationDef = $this->createDestinationTableDefinition(
            [
                ['name' => 'col1'],
                ['name' => 'different_col2'],
            ],
            ['col1'],
        );
        $this->createTableFromDefinition($destinationDef);

        // Create the importer
        $importer = new FullImporter($this->connection);
        $options = new SnowflakeImportOptions(
            [],
            false,
            false,
            0,
            SnowflakeImportOptions::SAME_TABLES_NOT_REQUIRED,
            SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            [],
            features: ['ctas-om'], // Enable CTAS feature
        );

        // This should throw an exception about column names mismatch
        $this->expectException(SnowflakeException::class);
        $this->expectExceptionMessage('Column names do not match between staging and destination tables');

        $importer->importToTable(
            $stagingDef,
            $destinationDef,
            $options,
            new ImportState('testTable'),
        );
    }

    public function testValidateTableDefinitionsMatchPrimaryKeysMismatch(): void
    {
        // Create staging table with columns col1 and col2, primary key col1
        $stagingDef = $this->createStagingTableDefinition(
            [
                ['name' => 'col1'],
                ['name' => 'col2'],
            ],
            ['col1'],
        );
        $this->createTableFromDefinition($stagingDef);

        // Create destination table with different primary keys
        $destinationDef = $this->createDestinationTableDefinition(
            [
                ['name' => 'col1'],
                ['name' => 'col2'],
            ],
            ['col1', 'col2'],
        );
        $this->createTableFromDefinition($destinationDef);

        // Create the importer
        $importer = new FullImporter($this->connection);
        $options = new SnowflakeImportOptions(
            [],
            false,
            false,
            0,
            SnowflakeImportOptions::SAME_TABLES_NOT_REQUIRED,
            SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            [],
            features: ['ctas-om'], // Enable CTAS feature
        );

        // This should throw an exception about primary keys mismatch
        $this->expectException(SnowflakeException::class);
        $this->expectExceptionMessage('Primary keys do not match between staging and destination tables');

        $importer->importToTable(
            $stagingDef,
            $destinationDef,
            $options,
            new ImportState('testTable'),
        );
    }

    public function testValidateTableDefinitionsMatchDataTypeMismatch(): void
    {
        // Create staging table with columns col1 and col2, col1 is VARCHAR
        $stagingDef = $this->createStagingTableDefinition(
            [
                ['name' => 'col1', 'type' => Snowflake::TYPE_VARCHAR],
                ['name' => 'col2'],
            ],
            ['col1'],
        );
        $this->createTableFromDefinition($stagingDef);

        // Create destination table with different data type for col1
        $destinationDef = $this->createDestinationTableDefinition(
            [
                ['name' => 'col1', 'type' => Snowflake::TYPE_NUMBER],
                ['name' => 'col2'],
            ],
            ['col1'],
        );
        $this->createTableFromDefinition($destinationDef);

        // Create the importer
        $importer = new FullImporter($this->connection);
        $options = new SnowflakeImportOptions(
            [],
            false,
            false,
            0,
            SnowflakeImportOptions::SAME_TABLES_NOT_REQUIRED,
            SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            [],
            features: ['ctas-om'], // Enable CTAS feature
        );

        // This should throw an exception about data type mismatch
        $this->expectException(SnowflakeException::class);
        $this->expectExceptionMessage(
            'Data type mismatch for column col1: staging has VARCHAR, destination has NUMBER',
        );

        $importer->importToTable(
            $stagingDef,
            $destinationDef,
            $options,
            new ImportState('testTable'),
        );
    }

    public function testValidateTableDefinitionsMatchWithTimestampColumn(): void
    {
        // Create staging table with columns col1 and col2
        $stagingDef = $this->createStagingTableDefinition(
            [
                ['name' => 'col1'],
                ['name' => 'col2'],
            ],
            ['col1'],
        );
        $this->createTableFromDefinition($stagingDef);

        // Create destination table with the same columns plus _timestamp
        $destinationDef = $this->createDestinationTableDefinition(
            [
                ['name' => 'col1'],
                ['name' => 'col2'],
                ['name' => '_timestamp', 'type' => Snowflake::TYPE_TIMESTAMP_NTZ, 'length' => '9'],
            ],
            ['col1'],
        );
        $this->createTableFromDefinition($destinationDef);

        // Create the importer
        $importer = new FullImporter($this->connection);
        $options = new SnowflakeImportOptions(
            [],
            false,
            false,
            0,
            SnowflakeImportOptions::SAME_TABLES_NOT_REQUIRED,
            SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            [],
            features: ['ctas-om'], // Enable CTAS feature
        );

        // This should not throw an exception because _timestamp is handled specially
        $result = $importer->importToTable(
            $stagingDef,
            $destinationDef,
            $options,
            new ImportState('testTable'),
        );

        // Verify the import was successful
        self::assertNotNull($result);
    }
}
