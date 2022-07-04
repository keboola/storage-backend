<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\ToFinal;

use DateTime;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\SqlBuilder;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use Tests\Keboola\Db\ImportExportFunctional\Snowflake\SnowflakeBaseTestCase;

class SqlBuilderTest extends SnowflakeBaseTestCase
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

    public function testGetDedupCommand(): void
    {
        $this->createTestSchema();
        $stageDef = $this->createStagingTableWithData();

        $deduplicationDef = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            '__temp_tempTable',
            true,
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
            ]),
            [
                'pk1',
                'pk2',
            ]
        );
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($deduplicationDef));

        $sql = $this->getBuilder()->getDedupCommand(
            $stageDef,
            $deduplicationDef,
            $deduplicationDef->getPrimaryKeysNames()
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import_export_test_schema"."__temp_tempTable" ("col1", "col2") SELECT a."col1",a."col2" FROM (SELECT "col1", "col2", ROW_NUMBER() OVER (PARTITION BY "pk1","pk2" ORDER BY "pk1","pk2") AS "_row_number_" FROM "import_export_test_schema"."__temp_stagingTable") AS a WHERE a."_row_number_" = 1',
            $sql
        );
        $this->connection->executeStatement($sql);
        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($deduplicationDef->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($deduplicationDef->getTableName())
        ));

        self::assertCount(2, $result);
    }

    private function createStagingTableWithData(bool $includeEmptyValues = false): SnowflakeTableDefinition
    {
        $def = $this->getStagingTableDefinition();
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($def));

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (2,2,\'2\',\'2\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED
            )
        );

        if ($includeEmptyValues) {
            $this->connection->executeStatement(
                sprintf(
                    'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (2,2,\'\',NULL)',
                    self::TEST_SCHEMA_QUOTED,
                    self::TEST_STAGING_TABLE_QUOTED
                )
            );
        }

        return $def;
    }

    private function getDummyImportOptions(): SnowflakeImportOptions
    {
        return new SnowflakeImportOptions([]);
    }

    public function testGetDeleteOldItemsCommand(): void
    {
        $this->createTestSchema();

        $tableDefinition = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection([
                new SnowflakeColumn(
                    'id',
                    new Snowflake(
                        Snowflake::TYPE_INT
                    )
                ),
                SnowflakeColumn::createGenericColumn('pk1'),
                SnowflakeColumn::createGenericColumn('pk2'),
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2']
        );
        $tableSql = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getTableName())
        );
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($tableDefinition));
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","pk1","pk2","col1","col2") VALUES (1,1,1,\'1\',\'1\')',
                $tableSql
            )
        );
        $stagingTableDefinition = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            false,
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('pk1'),
                SnowflakeColumn::createGenericColumn('pk2'),
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2']
        );
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($stagingTableDefinition));
        $stagingTableSql = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName())
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                $stagingTableSql
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("pk1","pk2","col1","col2") VALUES (2,1,\'1\',\'1\')',
                $stagingTableSql
            )
        );

        $sql = $this->getBuilder()->getDeleteOldItemsCommand(
            $stagingTableDefinition,
            $tableDefinition
        );

        self::assertEquals(
        // phpcs:ignore
            'DELETE FROM "import_export_test_schema"."__temp_stagingTable" "src" USING "import_export_test_schema"."import_export_test_test" AS "dest" WHERE "dest"."pk1" = COALESCE("src"."pk1", \'\') AND "dest"."pk2" = COALESCE("src"."pk2", \'\') ',
            $sql
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            $stagingTableSql
        ));

        self::assertCount(1, $result);
        self::assertSame([
            [
                'pk1' => '2',
                'pk2' => '1',
                'col1' => '1',
                'col2' => '1',
            ],
        ], $result);
    }

    private function assertTableNotExists(string $schemaName, string $tableName): void
    {
        try {
            (new SnowflakeTableReflection($this->connection, $schemaName, $tableName))->getTableStats();
            self::fail(sprintf(
                'Table "%s.%s" is expected to not exist.',
                $schemaName,
                $tableName
            ));
        } catch (TableNotExistsReflectionException $e) {
        }
    }

    public function testGetDropTableIfExistsCommand(): void
    {
        $this->createTestSchema();
        self::assertTableNotExists(self::TEST_SCHEMA, self::TEST_TABLE);

        // try to drop not existing table
        $sql = $this->getBuilder()->getDropTableIfExistsCommand(self::TEST_SCHEMA, self::TEST_TABLE);
        self::assertEquals(
        // phpcs:ignore
            'DROP TABLE IF EXISTS "import_export_test_schema"."import_export_test_test"',
            $sql
        );
        $this->connection->executeStatement($sql);

        // create table
        $this->createTestTable();

        // try to drop not existing table
        $sql = $this->getBuilder()->getDropTableIfExistsCommand(self::TEST_SCHEMA, self::TEST_TABLE);
        self::assertEquals(
        // phpcs:ignore
            'DROP TABLE IF EXISTS "import_export_test_schema"."import_export_test_test"',
            $sql
        );
        $this->connection->executeStatement($sql);

        self::assertTableNotExists(self::TEST_SCHEMA, self::TEST_TABLE);
    }

    protected function createTestTable(): void
    {
        $table = self::TEST_TABLE_IN_SCHEMA;
        $this->connection->executeStatement(<<<EOT
CREATE TABLE $table (
    id int NOT NULL
)
EOT
        );
    }

    public function testGetInsertAllIntoTargetTableCommand(): void
    {
        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            []
        );

        // no convert values no timestamp
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $this->getDummyImportOptions(),
            '2020-01-01 00:00:00'
        );

        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import_export_test_schema"."import_export_test_test" ("col1", "col2") (SELECT CAST(COALESCE("col1", \'\') AS VARCHAR (4000)) AS "col1",CAST(COALESCE("col2", \'\') AS VARCHAR (4000)) AS "col2" FROM "import_export_test_schema"."__temp_stagingTable" AS "src")',
            $sql
        );

        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        self::assertEqualsCanonicalizing([
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '2',
                'col2' => '2',
            ],
            [
                'id' => null,
                'col1' => '',
                'col2' => '',
            ],
        ], $result);
    }

    protected function createTestTableWithColumns(
        bool $includeTimestamp = false,
        bool $includePrimaryKey = false
    ): SnowflakeTableDefinition {
        $columns = [];
        $pks = [];
        if ($includePrimaryKey) {
            $pks[] = 'id';
            $columns[] = new SnowflakeColumn(
                'id',
                new Snowflake(Snowflake::TYPE_INT)
            );
        } else {
            $columns[] = $this->createNullableGenericColumn('id');
        }
        $columns[] = $this->createNullableGenericColumn('col1');
        $columns[] = $this->createNullableGenericColumn('col2');

        if ($includeTimestamp) {
            $columns[] = new SnowflakeColumn(
                '_timestamp',
                new Snowflake(Snowflake::TYPE_TIMESTAMP)
            );
        }

        $tableDefinition = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection($columns),
            $pks
        );
        $this->connection->executeStatement(
            (new SnowflakeTableQueryBuilder())->getCreateTableCommandFromDefinition($tableDefinition)
        );

        return $tableDefinition;
    }

    private function createNullableGenericColumn(string $columnName): SnowflakeColumn
    {
        $definition = new Snowflake(
            Snowflake::TYPE_VARCHAR,
            [
                'length' => '4000', // should be changed to max in future
                'nullable' => true,
            ]
        );

        return new SnowflakeColumn(
            $columnName,
            $definition
        );
    }

    public function testGetInsertAllIntoTargetTableCommandConvertToNull(): void
    {
        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            []
        );

        // convert col1 to null
        $options = new SnowflakeImportOptions(['col1']);
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import_export_test_schema"."import_export_test_test" ("col1", "col2") (SELECT IFF("col1" = \'\', NULL, "col1"),CAST(COALESCE("col2", \'\') AS VARCHAR (4000)) AS "col2" FROM "import_export_test_schema"."__temp_stagingTable" AS "src")',
            $sql
        );
        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        self::assertEqualsCanonicalizing([
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '2',
                'col2' => '2',
            ],
            [
                'id' => null,
                'col1' => null,
                'col2' => '',
            ],
        ], $result);
    }

    public function testGetInsertAllIntoTargetTableCommandConvertToNullWithTimestamp(): void
    {
        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            []
        );

        // use timestamp
        $options = new SnowflakeImportOptions(['col1'], false, true);
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import_export_test_schema"."import_export_test_test" ("col1", "col2", "_timestamp") (SELECT IFF("col1" = \'\', NULL, "col1"),CAST(COALESCE("col2", \'\') AS VARCHAR (4000)) AS "col2",\'2020-01-01 00:00:00\' FROM "import_export_test_schema"."__temp_stagingTable" AS "src")',
            $sql
        );
        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        foreach ($result as $item) {
            self::assertArrayHasKey('id', $item);
            self::assertArrayHasKey('col1', $item);
            self::assertArrayHasKey('col2', $item);
            self::assertArrayHasKey('_timestamp', $item);
        }
    }

    public function testGetTruncateTableWithDeleteCommand(): void
    {
        $this->createTestSchema();
        $this->createStagingTableWithData();

        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        self::assertEquals(3, $ref->getRowsCount());

        $sql = $this->getBuilder()->getTruncateTableWithDeleteCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        self::assertEquals(
            'DELETE FROM "import_export_test_schema"."__temp_stagingTable"',
            $sql
        );
        $this->connection->executeStatement($sql);
        self::assertEquals(0, $ref->getRowsCount());
    }

    public function testGetUpdateWithPkCommand(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1']
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            []
        );

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2") VALUES (1,\'2\',\'1\')',
                self::TEST_TABLE_IN_SCHEMA
            )
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        self::assertEquals([
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '1',
            ],
        ], $result);

        // no convert values no timestamp
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $this->getDummyImportOptions(),
            '2020-01-01 00:00:00'
        );
        self::assertEquals(
        // phpcs:ignore
            'UPDATE "import_export_test_schema"."import_export_test_test" AS "dest" SET "col1" = COALESCE("src"."col1", \'\'), "col2" = COALESCE("src"."col2", \'\') FROM "import_export_test_schema"."__temp_stagingTable" AS "src" WHERE "dest"."col1" = COALESCE("src"."col1", \'\')  AND (COALESCE(TO_VARCHAR("dest"."col1"), \'\') != COALESCE("src"."col1", \'\') OR COALESCE(TO_VARCHAR("dest"."col2"), \'\') != COALESCE("src"."col2", \'\'))',
            $sql
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        self::assertEquals([
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '2',
            ],
        ], $result);
    }

    public function testGetUpdateWithPkCommandConvertValues(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1']
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            []
        );

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2") VALUES (1,\'\',\'1\')',
                self::TEST_TABLE_IN_SCHEMA
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2") VALUES (1,\'2\',\'\')',
                self::TEST_TABLE_IN_SCHEMA
            )
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        self::assertEqualsCanonicalizing([
            [
                'id' => '1',
                'col1' => '',
                'col2' => '1',
            ],
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '',
            ],
        ], $result);

        $options = new SnowflakeImportOptions(['col1']);

        // converver values
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            '2020-01-01 00:00:00'
        );
        self::assertEquals(
        // phpcs:ignore
            'UPDATE "import_export_test_schema"."import_export_test_test" AS "dest" SET "col1" = IFF("src"."col1" = \'\', NULL, "src"."col1"), "col2" = COALESCE("src"."col2", \'\') FROM "import_export_test_schema"."__temp_stagingTable" AS "src" WHERE "dest"."col1" = COALESCE("src"."col1", \'\')  AND (COALESCE(TO_VARCHAR("dest"."col1"), \'\') != COALESCE("src"."col1", \'\') OR COALESCE(TO_VARCHAR("dest"."col2"), \'\') != COALESCE("src"."col2", \'\'))',
            $sql
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        self::assertEqualsCanonicalizing([
            [
                'id' => '1',
                'col1' => null,
                'col2' => '',
            ],
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '2',
            ],
        ], $result);
    }

    public function testGetUpdateWithPkCommandConvertValuesWithTimestamp(): void
    {
        $timestampInit = new DateTime('2020-01-01 00:00:01');
        $timestampSet = new DateTime('2020-01-01 01:01:01');
        $this->createTestSchema();
        $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);

        // create fake destination and say that there is pk on col1
        $fakeDestination = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1']
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            []
        );

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2","_timestamp") VALUES (1,\'\',\'1\',\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT)
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2","_timestamp") VALUES (1,\'2\',\'\',\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT)
            )
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        self::assertEqualsCanonicalizing([
            [
                'id' => '1',
                'col1' => '',
                'col2' => '1',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],
        ], $result);

        // use timestamp
        $options = new SnowflakeImportOptions(['col1'], false, true);
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            $timestampSet->format(DateTimeHelper::FORMAT)
        );

        self::assertEquals(
        // phpcs:ignore
            'UPDATE "import_export_test_schema"."import_export_test_test" AS "dest" SET "col1" = IFF("src"."col1" = \'\', NULL, "src"."col1"), "col2" = COALESCE("src"."col2", \'\'), "_timestamp" = \'2020-01-01 01:01:01\' FROM "import_export_test_schema"."__temp_stagingTable" AS "src" WHERE "dest"."col1" = COALESCE("src"."col1", \'\')  AND (COALESCE(TO_VARCHAR("dest"."col1"), \'\') != COALESCE("src"."col1", \'\') OR COALESCE(TO_VARCHAR("dest"."col2"), \'\') != COALESCE("src"."col2", \'\'))',
            $sql
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        foreach ($result as $item) {
            self::assertArrayHasKey('id', $item);
            self::assertArrayHasKey('col1', $item);
            self::assertArrayHasKey('col2', $item);
            self::assertArrayHasKey('_timestamp', $item);
            self::assertIsString($item['_timestamp']);
            self::assertSame(
                $timestampSet->format(DateTimeHelper::FORMAT),
                (new DateTime($item['_timestamp']))->format(DateTimeHelper::FORMAT)
            );
        }
    }

    private function getStagingTableDefinition(): SnowflakeTableDefinition
    {
        return new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('pk1'),
                $this->createNullableGenericColumn('pk2'),
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            []
        );
    }
}
