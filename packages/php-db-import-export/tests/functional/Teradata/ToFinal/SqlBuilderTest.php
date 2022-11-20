<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Teradata\ToFinal;

use DateTime;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Exception\DriverException;
use Keboola\Datatype\Definition\Teradata;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\Teradata\TeradataBaseTestCase;

class SqlBuilderTest extends TeradataBaseTestCase
{
    public const TESTS_PREFIX = 'import-export-test_';
    public const TEST_DB = self::TESTS_PREFIX . 'schema';
    public const TEST_STAGING_TABLE = 'stagingTable';
    public const TEST_TABLE = self::TESTS_PREFIX . 'test';

    // helpers
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropTestDb();
    }

    protected function dropTestDb(): void
    {
        $this->cleanDatabase($this->getTestDBName());
    }

    protected function createTestDb(): void
    {
        $this->createDatabase($this->getTestDBName());
    }

    protected function getTestDBName(): string
    {
        $buildPrefix = '';
        if (getenv('BUILD_PREFIX') !== false) {
            $buildPrefix = getenv('BUILD_PREFIX');
        }

        return $buildPrefix . self::TEST_DB;
    }

    protected function getBuilder(): SqlBuilder
    {
        return new SqlBuilder();
    }

    // TODO do we need it?
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

    private function createStagingTableWithData(bool $includeEmptyValues = false): TeradataTableDefinition
    {
        $def = $this->getStagingTableDefinition();
        $qb = new TeradataTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($def));

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
                TeradataQuote::quoteSingleIdentifier(self::TEST_STAGING_TABLE)
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
                TeradataQuote::quoteSingleIdentifier(self::TEST_STAGING_TABLE)
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (2,2,\'2\',\'2\')',
                TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
                TeradataQuote::quoteSingleIdentifier(self::TEST_STAGING_TABLE)
            )
        );

        if ($includeEmptyValues) {
            $this->connection->executeStatement(
                sprintf(
                    'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (2,2,\'\',NULL)',
                    TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
                    TeradataQuote::quoteSingleIdentifier(self::TEST_STAGING_TABLE)
                )
            );
        }

        return $def;
    }

    private function getStagingTableDefinition(): TeradataTableDefinition
    {
        return new TeradataTableDefinition(
            $this->getTestDBName(),
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

    private function createNullableGenericColumn(string $columnName): TeradataColumn
    {
        $definition = new Teradata(
            Teradata::TYPE_VARCHAR,
            [
                'length' => '50', // should be changed to max in future
                'nullable' => true,
            ]
        );

        return new TeradataColumn(
            $columnName,
            $definition
        );
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
            $this->getTestDBName(),
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

    // tests
    public function testGetDropTableIfExistsCommand(): void
    {
        $this->createTestDb();
        $this->assertTableNotExists($this->getTestDBName(), self::TEST_TABLE);

        // check that it cannot find non-existing table
        $sql = $this->getBuilder()->getTableExistsCommand($this->getTestDBName(), self::TEST_TABLE);
        self::assertEquals(
        // phpcs:ignore
            sprintf(
                "SELECT COUNT(*) FROM DBC.TablesVX WHERE DatabaseName = %s AND TableName = 'import-export-test_test';",
                TeradataQuote::quote($this->getTestDBName())
            ),
            $sql
        );
        $this->assertEquals(0, $this->connection->fetchOne($sql));

        // try to drop not existing table
        try {
            $sql = $this->getBuilder()->getDropTableUnsafe($this->getTestDBName(), self::TEST_TABLE);
            self::assertEquals(
            // phpcs:ignore
                sprintf('DROP TABLE %s."import-export-test_test"', TeradataQuote::quoteSingleIdentifier($this->getTestDBName())),
                $sql
            );
            $this->connection->executeStatement($sql);
        } catch (DriverException $e) {
            $this->assertStringContainsString('import-export-test_test\' does not exist', $e->getMessage());
        }

        // create table
        $this->initSingleTable($this->getTestDBName(), self::TEST_TABLE);

        // check that the table exists already
        $sql = $this->getBuilder()->getTableExistsCommand($this->getTestDBName(), self::TEST_TABLE);
        $this->assertEquals(1, $this->connection->fetchOne($sql));

        // drop existing table
        $sql = $this->getBuilder()->getDropTableUnsafe($this->getTestDBName(), self::TEST_TABLE);
        $this->connection->executeStatement($sql);

        // check that the table doesn't exist anymore
        $sql = $this->getBuilder()->getTableExistsCommand($this->getTestDBName(), self::TEST_TABLE);
        $this->assertEquals(0, $this->connection->fetchOne($sql));
    }

    public function testGetTruncateTableWithDeleteCommand(): void
    {
        $this->createTestDb();
        $this->createStagingTableWithData();

        $ref = new TeradataTableReflection($this->connection, $this->getTestDBName(), self::TEST_STAGING_TABLE);
        self::assertEquals(3, $ref->getRowsCount());

        $sql = $this->getBuilder()->getTruncateTableWithDeleteCommand($this->getTestDBName(), self::TEST_STAGING_TABLE);
        self::assertEquals(
            sprintf(
                'DELETE %s."stagingTable" ALL',
                TeradataQuote::quoteSingleIdentifier($this->getTestDBName())
            ),
            $sql
        );
        $this->connection->executeStatement($sql);
        self::assertEquals(0, $ref->getRowsCount());
    }

    // dedup command
    public function testGetDedupCommand(): void
    {
        $this->createTestDb();
        $stageDef = $this->createStagingTableWithData();

        $deduplicationDef = new TeradataTableDefinition(
            $this->getTestDBName(),
            '__temp_tempTable',
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
        $testDbName = TeradataQuote::quoteSingleIdentifier($this->getTestDBName());
        self::assertEquals(
        // phpcs:ignore
            sprintf('INSERT INTO %s."__temp_tempTable" ("col1", "col2") SELECT a."col1",a."col2" FROM (SELECT "col1", "col2", ROW_NUMBER() OVER (PARTITION BY "pk1","pk2" ORDER BY "pk1","pk2") AS "_row_number_" FROM %s."stagingTable") AS a WHERE a."_row_number_" = 1', $testDbName, $testDbName),
            $sql
        );
        $this->connection->executeStatement($sql);
        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s',
            TeradataQuote::quoteSingleIdentifier($deduplicationDef->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($deduplicationDef->getTableName())
        ));

        self::assertCount(2, $result);
    }

    // insert all command
    public function testGetInsertAllIntoTargetTableCommand(): void
    {
        $this->createTestDb();
        $destination = $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

        // create fake stage and say that there is less columns
        $fakeStage = new TeradataTableDefinition(
            $this->getTestDBName(),
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
            sprintf(
            // phpcs:ignore
                'INSERT INTO %s."import-export-test_test" ("col1", "col2") SELECT CAST(COALESCE("col1", \'\') as VARCHAR (50)) AS "col1",CAST(COALESCE("col2", \'\') as VARCHAR (50)) AS "col2" FROM %s."stagingTable" AS "src"',
                TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
                TeradataQuote::quoteSingleIdentifier($this->getTestDBName())
            ),
            $sql
        );

        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s',
            TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
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

    public function testGetInsertAllIntoTargetTableCommandConvertToNull(): void
    {
        $this->createTestDb();
        $destination = $this->createTestTableWithColumns();

        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new TeradataTableDefinition(
            $this->getTestDBName(),
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
            sprintf('INSERT INTO %s."import-export-test_test" ("col1", "col2") SELECT NULLIF("col1", \'\'),CAST(COALESCE("col2", \'\') as VARCHAR (50)) AS "col2" FROM %s."stagingTable" AS "src"',
                TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
                TeradataQuote::quoteSingleIdentifier($this->getTestDBName())
            ),
            $sql
        );
        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s',
            TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
            TeradataQuote::quoteSingleIdentifier(self::TEST_TABLE)
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
            $this->getTestDBName(),
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
            sprintf(
            // phpcs:ignore
                'INSERT INTO %s."import-export-test_test" ("col1", "col2", "_timestamp") SELECT NULLIF("col1", \'\'),CAST(COALESCE("col2", \'\') as VARCHAR (50)) AS "col2",\'2020-01-01 00:00:00\' FROM %s."stagingTable" AS "src"',
                TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
                TeradataQuote::quoteSingleIdentifier($this->getTestDBName())
            ),
            $sql
        );
        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            sprintf(
                'SELECT * FROM %s.%s',
                TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
                TeradataQuote::quoteSingleIdentifier(self::TEST_TABLE)
            )
        ));

        foreach ($result as $item) {
            self::assertArrayHasKey('id', $item);
            self::assertArrayHasKey('col1', $item);
            self::assertArrayHasKey('col2', $item);
            self::assertArrayHasKey('_timestamp', $item);
        }
    }

    // update command
    public function testGetUpdateWithPkCommand(): void
    {
        $this->createTestDb();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new TeradataTableDefinition(
            $this->getTestDBName(),
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1']
        );
        // create fake stage and say that there is less columns
        $fakeStage = new TeradataTableDefinition(
            $this->getTestDBName(),
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
                'INSERT INTO %s.%s("id","col1","col2") VALUES (1,\'2\',\'1\')',
                TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
                TeradataQuote::quoteSingleIdentifier(self::TEST_TABLE)
            )
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s',
            TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
            TeradataQuote::quoteSingleIdentifier(self::TEST_TABLE)
        ));

        self::assertEquals([
            [
                'id' => '   1',
                'col1' => '2',
                'col2' => '1',
            ],
        ], $result);

        // no convert values no timestamp
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $this->getImportOptions(),
            '2020-01-01 00:00:00'
        );
        $dest = sprintf('"%s"."%s"', $this->getTestDBName(), self::TEST_TABLE);
        $expectedSql = sprintf(
        // phpcs:ignore
            'UPDATE %s FROM (SELECT a."col1",a."col2" FROM (SELECT "col1", "col2", ROW_NUMBER() OVER (PARTITION BY "col1" ORDER BY "col1") AS "_row_number_" FROM %s.%s) AS a WHERE a."_row_number_" = 1) "src" SET "col1" = COALESCE("src"."col1", \'\'), "col2" = COALESCE("src"."col2", \'\') WHERE %s."col1" = COALESCE("src"."col1", \'\') AND (COALESCE(CAST(%s."col1" AS VARCHAR(32000)), \'\') <> COALESCE("src"."col1", \'\') OR COALESCE(CAST(%s."col2" AS VARCHAR(32000)), \'\') <> COALESCE("src"."col2", \'\'))',
            $dest,
            TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
            TeradataQuote::quoteSingleIdentifier(self::TEST_STAGING_TABLE),
            $dest,
            $dest,
            $dest
        );

        self::assertEquals($expectedSql, $sql);
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s',
            TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
            TeradataQuote::quoteSingleIdentifier(self::TEST_TABLE)
        ));

        self::assertEquals([
            [
                'id' => '   1',
                'col1' => '2',
                'col2' => '2',
            ],
        ], $result);
    }

    public function testGetUpdateWithPkCommandRequireSameTables(): void
    {
        $this->createTestDb();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new TeradataTableDefinition(
            $this->getTestDBName(),
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1']
        );
        // create fake stage and say that there is less columns
        $fakeStage = new TeradataTableDefinition(
            $this->getTestDBName(),
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            []
        );
        $dest = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
            TeradataQuote::quoteSingleIdentifier(self::TEST_TABLE)
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s ("id","col1","col2") VALUES (1,\'2\',\'1\')',
                $dest
            )
        );

        $result = $this->connection->fetchAllAssociative(sprintf('SELECT * FROM %s', $dest));

        self::assertEquals([
            [
                'id' => '   1',
                'col1' => '2',
                'col2' => '1',
            ],
        ], $result);

        // no convert values no timestamp
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $this->getImportOptions(
                [],
                false,
                false,
                0,
                ImportOptions::SAME_TABLES_NOT_REQUIRED,
                ImportOptions::NULL_MANIPULATION_SKIP //<- skipp null manipulation
            ),
            '2020-01-01 00:00:00'
        );

        $expectedSql = sprintf(
        // phpcs:ignore
            'UPDATE %s FROM (SELECT a."col1",a."col2" FROM (SELECT "col1", "col2", ROW_NUMBER() OVER (PARTITION BY "col1" ORDER BY "col1") AS "_row_number_" FROM %s.%s) AS a WHERE a."_row_number_" = 1) "src" SET "col1" = "src"."col1", "col2" = "src"."col2" WHERE %s."col1" = "src"."col1" AND (%s."col1" <> "src"."col1" OR %s."col2" <> "src"."col2")',
            $dest,
            TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
            TeradataQuote::quoteSingleIdentifier(self::TEST_STAGING_TABLE),
            $dest,
            $dest,
            $dest
        );

        self::assertEquals($expectedSql, $sql);
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf('SELECT * FROM %s', $dest));

        self::assertEquals([
            [
                'id' => '   1',
                'col1' => '2',
                'col2' => '2',
            ],
        ], $result);
    }

    public function testGetUpdateWithPkCommandConvertValues(): void
    {
        $this->createTestDb();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new TeradataTableDefinition(
            $this->getTestDBName(),
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1']
        );
        // create fake stage and say that there is less columns
        $fakeStage = new TeradataTableDefinition(
            $this->getTestDBName(),
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            []
        );

        $dest = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
            TeradataQuote::quoteSingleIdentifier(self::TEST_TABLE)
        );

        $this->connection->executeStatement(
            sprintf('INSERT INTO %s ("id","col1","col2") VALUES (1,\'\',\'1\')', $dest)
        );
        $this->connection->executeStatement(
            sprintf('INSERT INTO %s ("id","col1","col2") VALUES (1,\'2\',\'\')', $dest)
        );

        $result = $this->connection->fetchAllAssociative(sprintf('SELECT * FROM %s', $dest));

        self::assertEqualsCanonicalizing([
            [
                'id' => '   1',
                'col1' => '',
                'col2' => '1',
            ],
            [
                'id' => '   1',
                'col1' => '2',
                'col2' => '',
            ],
        ], $result);

        $options = $this->getImportOptions(['col1']);

        // converver values
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            '2020-01-01 00:00:00'
        );

        $expectedSql = sprintf(
        // phpcs:ignore
            'UPDATE %s FROM (SELECT a."col1",a."col2" FROM (SELECT "col1", "col2", ROW_NUMBER() OVER (PARTITION BY "col1" ORDER BY "col1") AS "_row_number_" FROM %s.%s) AS a WHERE a."_row_number_" = 1) "src" SET "col1" = CASE WHEN "src"."col1" = \'\' THEN NULL ELSE "src"."col1" END, "col2" = COALESCE("src"."col2", \'\') WHERE %s."col1" = COALESCE("src"."col1", \'\') AND (COALESCE(CAST(%s."col1" AS VARCHAR(32000)), \'\') <> COALESCE("src"."col1", \'\') OR COALESCE(CAST(%s."col2" AS VARCHAR(32000)), \'\') <> COALESCE("src"."col2", \'\'))',
            $dest,
            TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
            TeradataQuote::quoteSingleIdentifier(self::TEST_STAGING_TABLE),
            $dest,
            $dest,
            $dest
        );
        self::assertEquals(
        // phpcs:ignore
            $expectedSql,
            $sql
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf('SELECT * FROM %s', $dest));

        self::assertEqualsCanonicalizing([
            [
                'id' => '   1',
                'col1' => null,
                'col2' => '',
            ],
            [
                'id' => '   1',
                'col1' => '2',
                'col2' => '2',
            ],
        ], $result);
    }

    public function testGetUpdateWithPkCommandConvertValuesWithTimestamp(): void
    {
        $timestampInit = new DateTime('2020-01-01 00:00:01');
        $timestampSet = new DateTime('2020-01-01 01:01:01');
        $this->createTestDb();
        $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);

        // create fake destination and say that there is pk on col1
        $fakeDestination = new TeradataTableDefinition(
            $this->getTestDBName(),
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1']
        );
        // create fake stage and say that there is less columns
        $fakeStage = new TeradataTableDefinition(
            $this->getTestDBName(),
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            []
        );

        $dest = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
            TeradataQuote::quoteSingleIdentifier(self::TEST_TABLE)
        );

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s ("id","col1","col2","_timestamp") VALUES (1,\'\',\'1\',\'%s\')',
                $dest,
                $timestampInit->format(DateTimeHelper::FORMAT)
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s ("id","col1","col2","_timestamp") VALUES (1,\'2\',\'\',\'%s\')',
                $dest,
                $timestampInit->format(DateTimeHelper::FORMAT)
            )
        );

        $result = $this->connection->fetchAllAssociative(sprintf('SELECT * FROM %s', $dest));
        self::assertEqualsCanonicalizing([
            [
                'id' => '   1',
                'col1' => '',
                'col2' => '1',
                '_timestamp' => $timestampInit->format('Y-m-d H:i:s.u'),
            ],
            [
                'id' => '   1',
                'col1' => '2',
                'col2' => '',
                '_timestamp' => $timestampInit->format('Y-m-d H:i:s.u'),
            ],
        ], $result);

        // use timestamp
        $options = $this->getImportOptions(['col1'], false, true);
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            $timestampSet->format(DateTimeHelper::FORMAT)
        );
        $expectedSql = sprintf(
        // phpcs:ignore
            'UPDATE %s FROM (SELECT a."col1",a."col2" FROM (SELECT "col1", "col2", ROW_NUMBER() OVER (PARTITION BY "col1" ORDER BY "col1") AS "_row_number_" FROM %s.%s) AS a WHERE a."_row_number_" = 1) "src" SET "col1" = CASE WHEN "src"."col1" = \'\' THEN NULL ELSE "src"."col1" END, "col2" = COALESCE("src"."col2", \'\'), "_timestamp" = \'2020-01-01 01:01:01\' WHERE %s."col1" = COALESCE("src"."col1", \'\') AND (COALESCE(CAST(%s."col1" AS VARCHAR(32000)), \'\') <> COALESCE("src"."col1", \'\') OR COALESCE(CAST(%s."col2" AS VARCHAR(32000)), \'\') <> COALESCE("src"."col2", \'\'))',
            $dest,
            TeradataQuote::quoteSingleIdentifier($this->getTestDBName()),
            TeradataQuote::quoteSingleIdentifier(self::TEST_STAGING_TABLE),
            $dest,
            $dest,
            $dest
        );

        self::assertEquals($expectedSql, $sql);

        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf('SELECT * FROM %s', $dest));

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

    // delete old items
    public function testGetDeleteOldItemsCommand(): void
    {
        $this->createTestDb();

        $tableDefinition = new TeradataTableDefinition(
            $this->getTestDbName(),
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
        $storageTable = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($tableDefinition->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($tableDefinition->getTableName())
        );
        $qb = new TeradataTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($tableDefinition));
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s ("id","pk1","pk2","col1","col2") VALUES (1,1,1,\'1\',\'1\')',
                $storageTable
            )
        );
        $stagingTableDefinition = new TeradataTableDefinition(
            $this->getTestDbName(),
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
        $stagingTable = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName())
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s ("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                $stagingTable
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s ("pk1","pk2","col1","col2") VALUES (2,1,\'1\',\'1\')',
                $stagingTable
            )
        );

        $sql = $this->getBuilder()->getDeleteOldItemsCommand(
            $stagingTableDefinition,
            $tableDefinition,
            $this->getSimpleImportOptions()
        );

        $expectedSql = sprintf(
        // phpcs:ignore
            'DELETE %s FROM %s AS "joined" WHERE %s."pk1" = COALESCE("joined"."pk1", \'\') AND %s."pk2" = COALESCE("joined"."pk2", \'\')',
            $stagingTable,
            $storageTable,
            $stagingTable,
            $stagingTable,
        );
        self::assertEquals(
            $expectedSql,
            $sql
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            $stagingTable
        ));

        self::assertCount(1, $result);
        self::assertSame([
            [
                'pk1' => '   2',
                'pk2' => '   1',
                'col1' => '1',
                'col2' => '1',
            ],
        ], $result);
    }

    public function testGetDeleteOldItemsCommandRequireSameTables(): void
    {
        $this->createTestDb();

        $tableDefinition = new TeradataTableDefinition(
            $this->getTestDbName(),
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
        $storageTable = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($tableDefinition->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($tableDefinition->getTableName())
        );
        $qb = new TeradataTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($tableDefinition));
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s ("id","pk1","pk2","col1","col2") VALUES (1,1,1,\'1\',\'1\')',
                $storageTable
            )
        );
        $stagingTableDefinition = new TeradataTableDefinition(
            $this->getTestDbName(),
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
        $stagingTable = sprintf(
            '%s.%s',
            TeradataQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName())
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                $stagingTable
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("pk1","pk2","col1","col2") VALUES (2,1,\'1\',\'1\')',
                $stagingTable
            )
        );

        $sql = $this->getBuilder()->getDeleteOldItemsCommand(
            $stagingTableDefinition,
            $tableDefinition,
            $this->getImportOptions(
                [],
                false,
                false,
                0,
                ImportOptions::SAME_TABLES_NOT_REQUIRED,
                ImportOptions::NULL_MANIPULATION_SKIP //<- skipp null manipulation
            )
        );

        $expectedSql = sprintf(
        // phpcs:ignore
            'DELETE %s FROM %s AS "joined" WHERE %s."pk1" = "joined"."pk1" AND %s."pk2" = "joined"."pk2"',
            $stagingTable,
            $storageTable,
            $stagingTable,
            $stagingTable,
        );
        self::assertEquals(
            $expectedSql,
            $sql
        );

        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            $stagingTable
        ));

        self::assertCount(1, $result);
        self::assertSame([
            [
                'pk1' => '   2',
                'pk2' => '   1',
                'col1' => '1',
                'col2' => '1',
            ],
        ], $result);
    }
}
