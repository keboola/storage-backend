<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use DateTime;
use Doctrine\DBAL\Exception;
use Keboola\Datatype\Definition\Exasol;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;

class SqlBuilderTest extends ExasolBaseTestCase
{
    public const TESTS_PREFIX = 'import-export-test_';
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'schema';
    public const TEST_SCHEMA_QUOTED = '"' . self::TEST_SCHEMA . '"';
    public const TEST_STAGING_TABLE = 'stagingTable';
    public const TEST_STAGING_TABLE_QUOTED = '"stagingTable"';
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

        $deduplicationDef = new ExasolTableDefinition(
            self::TEST_SCHEMA,
            'tempTable',
            true,
            new ColumnCollection([
                ExasolColumn::createGenericColumn('col1'),
                ExasolColumn::createGenericColumn('col2'),
            ]),
            [
                'pk1',
                'pk2',
            ],
        );
        $qb = new ExasolTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($deduplicationDef));

        $sql = $this->getBuilder()->getDedupCommand(
            $stageDef,
            $deduplicationDef,
            $deduplicationDef->getPrimaryKeysNames(),
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import-export-test_schema"."tempTable" ("col1", "col2") SELECT a."col1",a."col2" FROM (SELECT "col1", "col2", ROW_NUMBER() OVER (PARTITION BY "pk1","pk2" ORDER BY "pk1","pk2") AS "_row_number_" FROM "import-export-test_schema"."stagingTable") AS a WHERE a."_row_number_" = 1',
            $sql,
        );
        $this->connection->executeStatement($sql);
        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s',
            ExasolQuote::quoteSingleIdentifier($deduplicationDef->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($deduplicationDef->getTableName()),
        ));

        self::assertCount(2, $result);
    }

    private function createStagingTableWithData(bool $includeEmptyValues = false): ExasolTableDefinition
    {
        $def = $this->getStagingTableDefinition();
        $qb = new ExasolTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($def));

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED,
            ),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED,
            ),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (2,2,\'2\',\'2\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED,
            ),
        );

        if ($includeEmptyValues) {
            $this->connection->executeStatement(
                sprintf(
                    'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (2,2,\'\',NULL)',
                    self::TEST_SCHEMA_QUOTED,
                    self::TEST_STAGING_TABLE_QUOTED,
                ),
            );
        }

        return $def;
    }

    private function getDummyImportOptions(): ExasolImportOptions
    {
        return new ExasolImportOptions([]);
    }

    public function testGetDeleteOldItemsCommand(): void
    {
        $this->createTestSchema();

        $tableDefinition = new ExasolTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection([
                new ExasolColumn(
                    'id',
                    new Exasol(
                        Exasol::TYPE_INT,
                    ),
                ),
                ExasolColumn::createGenericColumn('pk1'),
                ExasolColumn::createGenericColumn('pk2'),
                ExasolColumn::createGenericColumn('col1'),
                ExasolColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2'],
        );
        $tableSql = sprintf(
            '%s.%s',
            ExasolQuote::quoteSingleIdentifier($tableDefinition->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($tableDefinition->getTableName()),
        );
        $qb = new ExasolTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($tableDefinition));
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","pk1","pk2","col1","col2") VALUES (1,1,1,\'1\',\'1\')',
                $tableSql,
            ),
        );
        $stagingTableDefinition = new ExasolTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            false,
            new ColumnCollection([
                ExasolColumn::createGenericColumn('pk1'),
                ExasolColumn::createGenericColumn('pk2'),
                ExasolColumn::createGenericColumn('col1'),
                ExasolColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2'],
        );
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($stagingTableDefinition));
        $stagingTableSql = sprintf(
            '%s.%s',
            ExasolQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                $stagingTableSql,
            ),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("pk1","pk2","col1","col2") VALUES (2,1,\'1\',\'1\')',
                $stagingTableSql,
            ),
        );

        $sql = $this->getBuilder()->getDeleteOldItemsCommand(
            $stagingTableDefinition,
            $tableDefinition,
        );

        self::assertEquals(
        // phpcs:ignore
            'DELETE FROM "import-export-test_schema"."stagingTable" WHERE EXISTS (SELECT * FROM "import-export-test_schema"."import-export-test_test" WHERE COALESCE("import-export-test_schema"."import-export-test_test"."pk1", \'KBC_$#\') = COALESCE("import-export-test_schema"."stagingTable"."pk1", \'KBC_$#\') AND COALESCE("import-export-test_schema"."import-export-test_test"."pk2", \'KBC_$#\') = COALESCE("import-export-test_schema"."stagingTable"."pk2", \'KBC_$#\'))',
            $sql,
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            $stagingTableSql,
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
            (new ExasolTableReflection($this->connection, $schemaName, $tableName))->getTableStats();
            self::fail(sprintf(
                'Table "%s.%s" is expected to not exist.',
                $schemaName,
                $tableName,
            ));
        } catch (Exception $e) {
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
            'DROP TABLE IF EXISTS "import-export-test_schema"."import-export-test_test"',
            $sql,
        );
        $this->connection->executeStatement($sql);

        // create table
        $this->createTestTable();

        // try to drop not existing table
        $sql = $this->getBuilder()->getDropTableIfExistsCommand(self::TEST_SCHEMA, self::TEST_TABLE);
        self::assertEquals(
        // phpcs:ignore
            'DROP TABLE IF EXISTS "import-export-test_schema"."import-export-test_test"',
            $sql,
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
EOT,);
    }

    public function testGetInsertAllIntoTargetTableCommand(): void
    {
        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

        // create fake stage and say that there is less columns
        $fakeStage = new ExasolTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        // no convert values no timestamp
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $this->getDummyImportOptions(),
            '2020-01-01 00:00:00',
        );

        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import-export-test_schema"."import-export-test_test" ("col1", "col2") (SELECT CAST(COALESCE("col1", \'\') AS NVARCHAR (4000)) AS "col1",CAST(COALESCE("col2", \'\') AS NVARCHAR (4000)) AS "col2" FROM "import-export-test_schema"."stagingTable" AS "src")',
            $sql,
        );

        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
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
        bool $includePrimaryKey = false,
    ): ExasolTableDefinition {
        $columns = [];
        $pks = [];
        if ($includePrimaryKey) {
            $pks[] = 'id';
            $columns[] = new ExasolColumn(
                'id',
                new Exasol(Exasol::TYPE_INT),
            );
        } else {
            $columns[] = $this->createNullableGenericColumn('id');
        }
        $columns[] = $this->createNullableGenericColumn('col1');
        $columns[] = $this->createNullableGenericColumn('col2');

        if ($includeTimestamp) {
            $columns[] = new ExasolColumn(
                '_timestamp',
                new Exasol(Exasol::TYPE_TIMESTAMP),
            );
        }

        $tableDefinition = new ExasolTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection($columns),
            $pks,
        );
        $this->connection->executeStatement(
            (new ExasolTableQueryBuilder())->getCreateTableCommandFromDefinition($tableDefinition),
        );

        return $tableDefinition;
    }

    private function createNullableGenericColumn(string $columnName): ExasolColumn
    {
        $definition = new Exasol(
            Exasol::TYPE_NVARCHAR,
            [
                'length' => '4000', // should be changed to max in future
                'nullable' => true,
            ],
        );

        return new ExasolColumn(
            $columnName,
            $definition,
        );
    }

    public function testGetInsertAllIntoTargetTableCommandConvertToNull(): void
    {
        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new ExasolTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        // convert col1 to null
        $options = new ExasolImportOptions(['col1']);
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00',
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import-export-test_schema"."import-export-test_test" ("col1", "col2") (SELECT NULLIF("col1", \'\'),CAST(COALESCE("col2", \'\') AS NVARCHAR (4000)) AS "col2" FROM "import-export-test_schema"."stagingTable" AS "src")',
            $sql,
        );
        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
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
        $fakeStage = new ExasolTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        // use timestamp
        $options = new ExasolImportOptions(['col1'], false, true);
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00',
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import-export-test_schema"."import-export-test_test" ("col1", "col2", "_timestamp") (SELECT NULLIF("col1", \'\'),CAST(COALESCE("col2", \'\') AS NVARCHAR (4000)) AS "col2",\'2020-01-01 00:00:00\' FROM "import-export-test_schema"."stagingTable" AS "src")',
            $sql,
        );
        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
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

        $ref = new ExasolTableReflection($this->connection, self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        self::assertEquals(3, $ref->getRowsCount());

        $sql = $this->getBuilder()->getTruncateTableWithDeleteCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        self::assertEquals(
            'DELETE FROM "import-export-test_schema"."stagingTable"',
            $sql,
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
        $fakeDestination = new ExasolTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1'],
        );
        // create fake stage and say that there is less columns
        $fakeStage = new ExasolTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2") VALUES (1,\'2\',\'1\')',
                self::TEST_TABLE_IN_SCHEMA,
            ),
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '1',
            ],
        ], $result);

        // no convert values no timestamp
        $sql = $this->getBuilder()->getUpdateWithPkCommandSubstitute(
            $fakeStage,
            $fakeDestination,
            $this->getDummyImportOptions(),
            '2020-01-01 00:00:00',
        );
        self::assertEquals(
        // phpcs:ignore
            'UPDATE "import-export-test_schema"."import-export-test_test" AS "dest" SET "col2" = "src"."col2" FROM (SELECT DISTINCT * FROM "import-export-test_schema"."stagingTable") AS "src","import-export-test_schema"."import-export-test_test" AS "dest" WHERE COALESCE("dest"."col1", \'KBC_$#\') = COALESCE("src"."col1", \'KBC_$#\') AND (COALESCE(CAST("dest"."col1" AS NVARCHAR (4000)), \'KBC_$#\') != COALESCE("src"."col1", \'KBC_$#\') OR COALESCE(CAST("dest"."col2" AS NVARCHAR (4000)), \'KBC_$#\') != COALESCE("src"."col2", \'KBC_$#\')) ',
            $sql,
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
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
        $fakeDestination = new ExasolTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1'],
        );
        // create fake stage and say that there is less columns
        $fakeStage = new ExasolTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2") VALUES (1,\'\',\'1\')',
                self::TEST_TABLE_IN_SCHEMA,
            ),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2") VALUES (1,\'2\',\'\')',
                self::TEST_TABLE_IN_SCHEMA,
            ),
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
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

        $options = new ExasolImportOptions(['col1']);

        // converver values
        $sql = $this->getBuilder()->getUpdateWithPkCommandSubstitute(
            $fakeStage,
            $fakeDestination,
            $options,
            '2020-01-01 00:00:00',
        );
        self::assertEquals(
        // phpcs:ignore
            'UPDATE "import-export-test_schema"."import-export-test_test" AS "dest" SET "col2" = "src"."col2" FROM (SELECT DISTINCT * FROM "import-export-test_schema"."stagingTable") AS "src","import-export-test_schema"."import-export-test_test" AS "dest" WHERE COALESCE("dest"."col1", \'KBC_$#\') = COALESCE("src"."col1", \'KBC_$#\') AND (COALESCE(CAST("dest"."col1" AS NVARCHAR (4000)), \'KBC_$#\') != COALESCE("src"."col1", \'KBC_$#\') OR COALESCE(CAST("dest"."col2" AS NVARCHAR (4000)), \'KBC_$#\') != COALESCE("src"."col2", \'KBC_$#\')) ',
            $sql,
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
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
        $fakeDestination = new ExasolTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1'],
        );
        // create fake stage and say that there is less columns
        $fakeStage = new ExasolTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2","_timestamp") VALUES (1,\'\',\'1\',\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT),
            ),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2","_timestamp") VALUES (1,\'2\',\'\',\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT),
            ),
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEqualsCanonicalizing([
            [
                'id' => '1',
                'col1' => '',
                'col2' => '1',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT) . '.000000',
            ],
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT) . '.000000',
            ],
        ], $result);

        // use timestamp
        $options = new ExasolImportOptions(['col1'], false, true);
        $sql = $this->getBuilder()->getUpdateWithPkCommandSubstitute(
            $fakeStage,
            $fakeDestination,
            $options,
            $timestampSet->format(DateTimeHelper::FORMAT) . '.000',
        );

        self::assertEquals(
        // phpcs:ignore
            'UPDATE "import-export-test_schema"."import-export-test_test" AS "dest" SET "col2" = "src"."col2", "_timestamp" = \'2020-01-01 01:01:01.000\' FROM (SELECT DISTINCT * FROM "import-export-test_schema"."stagingTable") AS "src","import-export-test_schema"."import-export-test_test" AS "dest" WHERE COALESCE("dest"."col1", \'KBC_$#\') = COALESCE("src"."col1", \'KBC_$#\') AND (COALESCE(CAST("dest"."col1" AS NVARCHAR (4000)), \'KBC_$#\') != COALESCE("src"."col1", \'KBC_$#\') OR COALESCE(CAST("dest"."col2" AS NVARCHAR (4000)), \'KBC_$#\') != COALESCE("src"."col2", \'KBC_$#\')) ',
            $sql,
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        foreach ($result as $item) {
            self::assertArrayHasKey('id', $item);
            self::assertArrayHasKey('col1', $item);
            self::assertArrayHasKey('col2', $item);
            self::assertArrayHasKey('_timestamp', $item);
            self::assertSame(
                $timestampSet->format(DateTimeHelper::FORMAT),
                (new DateTime($item['_timestamp']))->format(DateTimeHelper::FORMAT),
            );
        }
    }

    private function getStagingTableDefinition(): ExasolTableDefinition
    {
        return new ExasolTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('pk1'),
                $this->createNullableGenericColumn('pk2'),
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );
    }
}
