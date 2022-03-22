<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Teradata;

use DateTime;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Exception\DriverException;
use Keboola\Datatype\Definition\Teradata;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;

class SqlBuilderTest extends TeradataBaseTestCase
{
    public const TESTS_PREFIX = 'import-export-test_';
    public const TEST_DB = self::TESTS_PREFIX . 'schema';
    public const TEST_DB_QUOTED = '"' . self::TEST_DB . '"';
    public const TEST_STAGING_TABLE = 'stagingTable';
    public const TEST_STAGING_TABLE_QUOTED = '"stagingTable"';
    public const TEST_TABLE = self::TESTS_PREFIX . 'test';
    public const TEST_TABLE_IN_DB = self::TEST_DB_QUOTED . '.' . self::TEST_TABLE_QUOTED;
    public const TEST_TABLE_QUOTED = '"' . self::TEST_TABLE . '"';

    protected function dropTestDb(): void
    {
        $this->cleanDatabase(self::TEST_DB);
    }

    protected function getBuilder(): SqlBuilder
    {
        return new SqlBuilder();
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropTestDb();
    }

    protected function createTestDb(): void
    {
        $this->createDatabase(self::TEST_DB);
    }

    public function testGetDedupCommand(): void
    {
        $this->markTestSkipped('not impl');
        $this->createTestDb();
        $stageDef = $this->createStagingTableWithData();

        $deduplicationDef = new TeradataTableDefinition(
            self::TEST_DB,
            'tempTable',
            true,
            new ColumnCollection([
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
            ]),
            [
                'pk1',
                'pk2',
            ]
        );
        $qb = new TeradataTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($deduplicationDef));

        $sql = $this->getBuilder()->getDedupCommand(
            $stageDef,
            $deduplicationDef,
            $deduplicationDef->getPrimaryKeysNames()
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import-export-test_schema"."tempTable" ("col1", "col2") SELECT a."col1",a."col2" FROM (SELECT "col1", "col2", ROW_NUMBER() OVER (PARTITION BY "pk1","pk2" ORDER BY "pk1","pk2") AS "_row_number_" FROM "import-export-test_schema"."stagingTable") AS a WHERE a."_row_number_" = 1',
            $sql
        );
        $this->connection->executeStatement($sql);
        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s',
            TeradataQuote::quoteSingleIdentifier(self::TEST_DB),
            TeradataQuote::quoteSingleIdentifier($deduplicationDef->getTableName())
        ));

        self::assertCount(2, $result);
    }

    private function createStagingTableWithData(bool $includeEmptyValues = false): TeradataTableDefinition
    {
        $def = $this->getStagingTableDefinition();
        $qb = new TeradataTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($def));

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                self::TEST_DB_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                self::TEST_DB_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (2,2,\'2\',\'2\')',
                self::TEST_DB_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED
            )
        );

        if ($includeEmptyValues) {
            $this->connection->executeStatement(
                sprintf(
                    'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (2,2,\'\',NULL)',
                    self::TEST_DB_QUOTED,
                    self::TEST_STAGING_TABLE_QUOTED
                )
            );
        }

        return $def;
    }

    public function testGetDeleteOldItemsCommand(): void
    {
        $this->createTestDb();

        $tableDefinition = new TeradataTableDefinition(
            self::TEST_DB,
            self::TEST_TABLE,
            false,
            new ColumnCollection([
                new TeradataColumn(
                    'id',
                    new Teradata(
                        Teradata::TYPE_INT
                    )
                ),
                TeradataColumn::createGenericColumn('pk1'),
                TeradataColumn::createGenericColumn('pk2'),
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2']
        );
        $tableSql = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier(self::TEST_DB),
            TeradataQuote::quoteSingleIdentifier($tableDefinition->getTableName())
        );
        $qb = new TeradataTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($tableDefinition));
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","pk1","pk2","col1","col2") VALUES (1,1,1,\'1\',\'1\')',
                $tableSql
            )
        );
        $stagingTableDefinition = new TeradataTableDefinition(
            self::TEST_DB,
            self::TEST_STAGING_TABLE,
            false,
            new ColumnCollection([
                TeradataColumn::createGenericColumn('pk1'),
                TeradataColumn::createGenericColumn('pk2'),
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2']
        );
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($stagingTableDefinition));
        $stagingTableSql = sprintf(
            '%s.%s',
            // TODO where the getDbName came from
            TeradataQuote::quoteSingleIdentifier(self::TEST_DB),
            TeradataQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName())
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
            'DELETE FROM "import-export-test_schema"."stagingTable" WHERE EXISTS (SELECT * FROM "import-export-test_schema"."import-export-test_test" WHERE COALESCE("import-export-test_schema"."import-export-test_test"."pk1", \'KBC_$#\') = COALESCE("import-export-test_schema"."stagingTable"."pk1", \'KBC_$#\') AND COALESCE("import-export-test_schema"."import-export-test_test"."pk2", \'KBC_$#\') = COALESCE("import-export-test_schema"."stagingTable"."pk2", \'KBC_$#\'))',
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
            (new TeradataTableReflection($this->connection, $schemaName, $tableName))->getTableStats();
            self::fail(sprintf(
                'Table "%s.%s" is expected to not exist.',
                $schemaName,
                $tableName
            ));
        } catch (Exception $e) {
        }
    }

    public function testGetDropTableIfExistsCommand(): void
    {
        $this->createTestDb();
        $this->assertTableNotExists(self::TEST_DB, self::TEST_TABLE);

        // check that it cannot find non-existing table
        $sql = $this->getBuilder()->getTableExistsCommand(self::TEST_DB, self::TEST_TABLE);
        self::assertEquals(
        // phpcs:ignore
            "SELECT COUNT(*) FROM DBC.Tables WHERE DatabaseName = 'import-export-test_schema' AND TableName = 'import-export-test_test';", $sql
        );
        $this->assertEquals(0, $this->connection->fetchOne($sql));

        // try to drop not existing table
        try {
            $sql = $this->getBuilder()->getDropTableUnsafe(self::TEST_DB, self::TEST_TABLE);
            self::assertEquals(
            // phpcs:ignore
                'DROP TABLE "import-export-test_schema"."import-export-test_test"',
                $sql
            );
            $this->connection->executeStatement($sql);
        } catch (DriverException $e) {
            $this->assertContains('Base table or view not found', $e->getMessage());
        }

        // create table
        $this->initSingleTable(self::TEST_DB, self::TEST_TABLE);

        // check that the table exists already
        $sql = $this->getBuilder()->getTableExistsCommand(self::TEST_DB, self::TEST_TABLE);
        $this->assertEquals(1, $this->connection->fetchOne($sql));

        // drop existing table
        $sql = $this->getBuilder()->getDropTableUnsafe(self::TEST_DB, self::TEST_TABLE);
        $this->connection->executeStatement($sql);

        // check that the table doesn't exist anymore
        $sql = $this->getBuilder()->getTableExistsCommand(self::TEST_DB, self::TEST_TABLE);
        $this->assertEquals(0, $this->connection->fetchOne($sql));
    }

    public function testGetInsertAllIntoTargetTableCommand(): void
    {
        $this->createTestDb();
        $destination = $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

        // create fake stage and say that there is less columns
        $fakeStage = new TeradataTableDefinition(
            self::TEST_DB,
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
            $this->getImportOptions(),
            '2020-01-01 00:00:00'
        );

        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import-export-test_schema"."import-export-test_test" ("col1", "col2") SELECT CAST(COALESCE("col1", \'\') AS VARCHAR (4000)) AS "col1",CAST(COALESCE("col2", \'\') AS VARCHAR (4000)) AS "col2" FROM "import-export-test_schema"."stagingTable" AS "src"',
            $sql
        );

        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s',
            TeradataQuote::quoteSingleIdentifier(self::TEST_DB),
            TeradataQuote::quoteSingleIdentifier(self::TEST_TABLE),
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
    ): TeradataTableDefinition {
        $columns = [];
        $pks = [];
        if ($includePrimaryKey) {
            $pks[] = 'id';
            $columns[] = new TeradataColumn(
                'id',
                new Teradata(Teradata::TYPE_INT)
            );
        } else {
            $columns[] = $this->createNullableGenericColumn('id');
        }
        $columns[] = $this->createNullableGenericColumn('col1');
        $columns[] = $this->createNullableGenericColumn('col2');

        if ($includeTimestamp) {
            $columns[] = new TeradataColumn(
                '_timestamp',
                new Teradata(Teradata::TYPE_TIMESTAMP)
            );
        }

        $tableDefinition = new TeradataTableDefinition(
            self::TEST_DB,
            self::TEST_TABLE,
            false,
            new ColumnCollection($columns),
            $pks
        );
        $this->connection->executeStatement(
            (new TeradataTableQueryBuilder())->getCreateTableCommandFromDefinition($tableDefinition)
        );

        return $tableDefinition;
    }

    private function createNullableGenericColumn(string $columnName): TeradataColumn
    {
        $definition = new Teradata(
            Teradata::TYPE_VARCHAR,
            [
                'length' => '4000', // should be changed to max in future
                'nullable' => true,
            ]
        );

        return new TeradataColumn(
            $columnName,
            $definition
        );
    }

    public function testGetInsertAllIntoTargetTableCommandConvertToNull(): void
    {
        $this->createTestDb();
        $destination = $this->createTestTableWithColumns();

        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new TeradataTableDefinition(
            self::TEST_DB,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            []
        );

        // convert col1 to null
        $options = $this->getImportOptions(['col1']);
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import-export-test_schema"."import-export-test_test" ("col1", "col2") SELECT NULLIF("col1", \'\'),CAST(COALESCE("col2", \'\') AS VARCHAR (4000)) AS "col2" FROM "import-export-test_schema"."stagingTable" AS "src"',
            $sql
        );
        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_DB
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
        $this->createTestDb();
        $destination = $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new TeradataTableDefinition(
            self::TEST_DB,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            []
        );

        // use timestamp
        $options = $this->getImportOptions(['col1'], false, true);
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import-export-test_schema"."import-export-test_test" ("col1", "col2", "_timestamp") SELECT NULLIF("col1", \'\'),CAST(COALESCE("col2", \'\') AS VARCHAR (4000)) AS "col2",\'2020-01-01 00:00:00\' FROM "import-export-test_schema"."stagingTable" AS "src"',
            $sql
        );
        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_DB
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
        $this->createTestDb();
        $this->createStagingTableWithData();

        $ref = new TeradataTableReflection($this->connection, self::TEST_DB, self::TEST_STAGING_TABLE);
        self::assertEquals(3, $ref->getRowsCount());

        $sql = $this->getBuilder()->getTruncateTableWithDeleteCommand(self::TEST_DB, self::TEST_STAGING_TABLE);
        self::assertEquals(
            'DELETE FROM "import-export-test_schema"."stagingTable"',
            $sql
        );
        $this->connection->executeStatement($sql);
        self::assertEquals(0, $ref->getRowsCount());
    }

    private function getStagingTableDefinition(): TeradataTableDefinition
    {
        return new TeradataTableDefinition(
            self::TEST_DB,
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
