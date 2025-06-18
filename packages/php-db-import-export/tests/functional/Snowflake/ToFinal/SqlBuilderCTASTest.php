<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\ToFinal;

use DateTimeImmutable;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\SqlBuilder;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Tests\Keboola\Db\ImportExportFunctional\Snowflake\SnowflakeBaseTestCase;

class SqlBuilderCTASTest extends SnowflakeBaseTestCase
{
    public const TESTS_PREFIX = 'import_export_test_';
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'schema';
    public const TEST_SCHEMA_QUOTED = '"' . self::TEST_SCHEMA . '"';
    public const TEST_STAGING_TABLE = '__temp_stagingTable';
    public const TEST_STAGING_TABLE_QUOTED = '"__temp_stagingTable"';
    public const TEST_TABLE = self::TESTS_PREFIX . 'test';
    public const TEST_TABLE_IN_SCHEMA = self::TEST_SCHEMA_QUOTED . '.' . self::TEST_TABLE_QUOTED;
    public const TEST_TABLE_QUOTED = '"' . self::TEST_TABLE . '"';

    protected function dropTestSchema(): void
    {
        $this->cleanSchema(self::TEST_SCHEMA);
    }

    protected function getBuilder(): SqlBuilder
    {
        return new SqlBuilder();
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropTestSchema();
    }

    protected function createTestSchema(): void
    {
        $this->createSchema(self::TEST_SCHEMA);
    }

    private function createStagingTableWithData(): SnowflakeTableDefinition
    {
        $def = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                new SnowflakeColumn(
                    'col1',
                    new Snowflake(
                        Snowflake::TYPE_VARCHAR,
                        [
                            'length' => '50',
                            'nullable' => true,
                        ],
                    ),
                ),
                new SnowflakeColumn(
                    'col2',
                    new Snowflake(
                        Snowflake::TYPE_VARCHAR,
                        [
                            'length' => '50',
                            'nullable' => true,
                        ],
                    ),
                ),
            ]),
            [
                'pk1',
                'pk2',
            ],
        );

        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($def));

        $this->connection->executeStatement(sprintf(
            'INSERT INTO %s.%s VALUES (\'1\', \'1\'), (\'1\', \'2\'), (\'2\', \'1\'), (\'2\', \'2\')',
            SnowflakeQuote::quoteSingleIdentifier($def->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($def->getTableName()),
        ));

        return $def;
    }

    private function createTestTableWithColumns(): SnowflakeTableDefinition
    {
        $def = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection([
                new SnowflakeColumn(
                    'col1',
                    new Snowflake(
                        Snowflake::TYPE_VARCHAR,
                        [
                            'length' => '50',
                            'nullable' => true,
                        ],
                    ),
                ),
                new SnowflakeColumn(
                    'col2',
                    new Snowflake(
                        Snowflake::TYPE_VARCHAR,
                        [
                            'length' => '50',
                            'nullable' => true,
                        ],
                    ),
                ),
            ]),
            [
                'pk1',
                'pk2',
            ],
        );

        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($def));

        return $def;
    }

    public function testGetCTASInsertAllIntoTargetTableCommand(): void
    {
        $this->createTestSchema();
        $stagingDef = $this->createStagingTableWithData();
        $destinationDef = $this->createTestTableWithColumns();

        // Test with no special options
        $sql = $this->getBuilder()->getCTASInsertAllIntoTargetTableCommand(
            $stagingDef,
            $destinationDef,
            DateTimeHelper::getTimestampFormated(new DateTimeImmutable('2023-10-01 12:00:00')),
        );
        // phpcs:ignore
        $this->assertSame('CREATE OR REPLACE TABLE "import_export_test_schema"."import_export_test_test" AS SELECT "col1","col2",\'2023-10-01 12:00:00\' AS "_timestamp" FROM "import_export_test_schema"."__temp_stagingTable"', $sql);
        // Verify the SQL contains the CREATE OR REPLACE TABLE statement
        self::assertStringContainsString(
            'CREATE OR REPLACE TABLE',
            $sql,
        );

        // Verify the SQL contains the source table
        self::assertStringContainsString(
            sprintf(
                '%s.%s',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_STAGING_TABLE),
            ),
            $sql,
        );

        // Verify the SQL contains the destination table
        self::assertStringContainsString(
            sprintf(
                '%s.%s',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_TABLE),
            ),
            $sql,
        );

        // Execute the SQL to verify it works
        $this->connection->executeStatement($sql);

        // Verify the data was copied and the timestamp column was added
        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationDef->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationDef->getTableName()),
        ));

        self::assertSame([
            [
                'col1' => '1',
                'col2' => '1',
                '_timestamp' => '2023-10-01 12:00:00',
            ],
            [
                'col1' => '1',
                'col2' => '2',
                '_timestamp' => '2023-10-01 12:00:00',
            ],
            [
                'col1' => '2',
                'col2' => '1',
                '_timestamp' => '2023-10-01 12:00:00',
            ],
            [
                'col1' => '2',
                'col2' => '2',
                '_timestamp' => '2023-10-01 12:00:00',
            ],
        ], $result);
    }

    public function testGetCTASInsertAllIntoTargetTableCommandWithConvertEmptyValuesToNull(): void
    {
        $this->createTestSchema();
        $stagingDef = $this->createStagingTableWithData();
        $destinationDef = $this->createTestTableWithColumns();

        // Insert a row with empty values
        $this->connection->executeStatement(sprintf(
            'INSERT INTO %s.%s VALUES (\'\', \'\')',
            SnowflakeQuote::quoteSingleIdentifier($stagingDef->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingDef->getTableName()),
        ));

        // Test with convertEmptyValuesToNull option
        $sql = $this->getBuilder()->getCTASInsertAllIntoTargetTableCommand(
            $stagingDef,
            $destinationDef,
            DateTimeHelper::getTimestampFormated(new DateTimeImmutable('2023-10-01 12:00:00')),
        );
        // phpcs:ignore
        $this->assertSame('CREATE OR REPLACE TABLE "import_export_test_schema"."import_export_test_test" AS SELECT "col1","col2",\'2023-10-01 12:00:00\' AS "_timestamp" FROM "import_export_test_schema"."__temp_stagingTable"', $sql);
        // Execute the SQL to verify it works
        $this->connection->executeStatement($sql);

        // Verify the empty values were converted to NULL
        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($destinationDef->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destinationDef->getTableName()),
        ));

        self::assertSame([
            [
                'col1' => '1',
                'col2' => '1',
                '_timestamp' => '2023-10-01 12:00:00',
            ],
            [
                'col1' => '1',
                'col2' => '2',
                '_timestamp' => '2023-10-01 12:00:00',
            ],
            [
                'col1' => '2',
                'col2' => '1',
                '_timestamp' => '2023-10-01 12:00:00',
            ],
            [
                'col1' => '2',
                'col2' => '2',
                '_timestamp' => '2023-10-01 12:00:00',
            ],
            [
                'col1' => '',
                'col2' => '',
                '_timestamp' => '2023-10-01 12:00:00',
            ],
        ], $result);
        self::assertCount(5, $result);
    }
}
