<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery;

use Exception;
use Google\Cloud\Core\Exception\NotFoundException;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\SqlBuilder;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Throwable;

class SqlBuildTest extends BigqueryBaseTestCase
{
    public const TESTS_PREFIX = 'import_export_test_';
    public const TEST_DB = self::TESTS_PREFIX . 'schema';
    public const TEST_DB_QUOTED = '`' . self::TEST_DB . '`';
    public const TEST_STAGING_TABLE = 'stagingTable';
    public const TEST_STAGING_TABLE_QUOTED = '`stagingTable`';
    public const TEST_TABLE = self::TESTS_PREFIX . 'test';
    public const TEST_TABLE_IN_DB = self::TEST_DB_QUOTED . '.' . self::TEST_TABLE_QUOTED;
    public const TEST_TABLE_QUOTED = '`' . self::TEST_TABLE . '`';

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
        $this->markTestSkipped('not implemented');
    }

    private function createStagingTableWithData(bool $includeEmptyValues = false): BigqueryTableDefinition
    {
        $def = $this->getStagingTableDefinition();
        $qb = new BigqueryTableQueryBuilder();
        $this->bqClient->runQuery($this->bqClient->query($qb->getCreateTableCommandFromDefinition($def)));

        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'1\',\'1\',\'1\',\'1\')',
                self::TEST_DB_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED
            )
        ));
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'1\',\'1\',\'1\',\'1\')',
                self::TEST_DB_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED
            )
        ));
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'2\',\'2\',\'2\',\'2\')',
                self::TEST_DB_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED
            )
        ));

        if ($includeEmptyValues) {
            $this->bqClient->runQuery($this->bqClient->query(
                sprintf(
                    'INSERT INTO %s.%s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'2\',\'2\',\'\',NULL)',
                    self::TEST_DB_QUOTED,
                    self::TEST_STAGING_TABLE_QUOTED
                )
            ));
        }

        return $def;
    }

    public function testGetDeleteOldItemsCommand(): void
    {
        $this->markTestSkipped('not implemented');
    }

    private function assertTableNotExists(string $schemaName, string $tableName): void
    {
        try {
            (new BigqueryTableReflection($this->bqClient, $schemaName, $tableName))->getTableStats();
            self::fail(sprintf(
                'Table "%s.%s" is expected to not exist.',
                $schemaName,
                $tableName
            ));
        } catch (Throwable $e) {
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
            "SELECT COUNT(*) AS count FROM `import_export_test_schema`.INFORMATION_SCHEMA.TABLES WHERE table_name = 'import_export_test_test';", $sql
        );
        $queryResults = $this->bqClient->runQuery($this->bqClient->query($sql));
        $current = (array) $queryResults->getIterator()->current();
        $this->assertEquals(0, $current['count']);

        // try to drop not existing table
        try {
            $sql = $this->getBuilder()->getDropTableUnsafe(self::TEST_DB, self::TEST_TABLE);
            self::assertEquals(
            // phpcs:ignore
                'DROP TABLE `import_export_test_schema`.`import_export_test_test`',
                $sql
            );
            $this->bqClient->runQuery($this->bqClient->query($sql));
        } catch (NotFoundException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertStringContainsString('import_export_test_test', $e->getMessage());
        }

        // create table
        $this->initSingleTable(self::TEST_DB, self::TEST_TABLE);

        // check that the table exists already
        $sql = $this->getBuilder()->getTableExistsCommand(self::TEST_DB, self::TEST_TABLE);
        $queryResults = $this->bqClient->runQuery($this->bqClient->query($sql));
        $current = (array) $queryResults->getIterator()->current();
        $this->assertEquals(1, $current['count']);

        // drop existing table
        $sql = $this->getBuilder()->getDropTableUnsafe(self::TEST_DB, self::TEST_TABLE);
        $this->bqClient->runQuery($this->bqClient->query($sql));

        // check that the table doesn't exist anymore
        $sql = $this->getBuilder()->getTableExistsCommand(self::TEST_DB, self::TEST_TABLE);
        $queryResults = $this->bqClient->runQuery($this->bqClient->query($sql));
        $current = (array) $queryResults->getIterator()->current();
        $this->assertEquals(0, $current['count']);
    }

    public function testGetInsertAllIntoTargetTableCommand(): void
    {
        $this->createTestDb();
        $destination = $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

        // create fake stage and say that there is less columns
        $fakeStage = new BigqueryTableDefinition(
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
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`) SELECT CAST(COALESCE(`col1`, \'\') as STRING) AS `col1`,CAST(COALESCE(`col2`, \'\') as STRING) AS `col2` FROM `import_export_test_schema`.`stagingTable` AS `src`',
            $sql
        );

        $out = $this->bqClient->runQuery($this->bqClient->query($sql));
        self::assertEquals(4, $out->info()['numDmlAffectedRows']);

        $result = $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'SELECT * FROM %s.%s',
            BigqueryQuote::quoteSingleIdentifier(self::TEST_DB),
            BigqueryQuote::quoteSingleIdentifier(self::TEST_TABLE),
        )));

        $queryResult = array_map(static function (array $row) {
            return array_map(static function ($column) {
                return $column;
            }, array_values($row));
        }, iterator_to_array($result->getIterator()));
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
        ], $queryResult);
    }

    protected function createTestTableWithColumns(
        bool $includeTimestamp = false,
        bool $includePrimaryKey = false
    ): BigqueryTableDefinition {
        $columns = [];
        $pks = [];
        if ($includePrimaryKey) {
            $pks[] = 'id';
            $columns[] = new BigqueryColumn(
                'id',
                new Bigquery(Bigquery::TYPE_INT)
            );
        } else {
            $columns[] = $this->createNullableGenericColumn('id');
        }
        $columns[] = $this->createNullableGenericColumn('col1');
        $columns[] = $this->createNullableGenericColumn('col2');

        if ($includeTimestamp) {
            $columns[] = new BigqueryColumn(
                '_timestamp',
                new Bigquery(Bigquery::TYPE_TIMESTAMP)
            );
        }

        $tableDefinition = new BigqueryTableDefinition(
            self::TEST_DB,
            self::TEST_TABLE,
            false,
            new ColumnCollection($columns),
            $pks
        );
        $this->bqClient->runQuery($this->bqClient->query(
            (new BigqueryTableQueryBuilder())->getCreateTableCommandFromDefinition($tableDefinition)
        ));

        return $tableDefinition;
    }

    private function createNullableGenericColumn(string $columnName): BigqueryColumn
    {
        $definition = new Bigquery(
            Bigquery::TYPE_STRING,
            [
                'length' => '50', // should be changed to max in future
                'nullable' => true,
            ]
        );

        return new BigqueryColumn(
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
        $fakeStage = new BigqueryTableDefinition(
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
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`) SELECT NULLIF(`col1`, \'\'),CAST(COALESCE(`col2`, \'\') as STRING) AS `col2` FROM `import_export_test_schema`.`stagingTable` AS `src`',
            $sql
        );
        $out = $this->bqClient->runQuery($this->bqClient->query($sql));
        self::assertEquals(4, $out->info()['numDmlAffectedRows']);

        $result = $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_DB
        )));

        $queryResult = array_map(static function (array $row) {
            return array_map(static function ($column) {
                return $column;
            }, array_values($row));
        }, iterator_to_array($result->getIterator()));
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
        ], $queryResult);
    }

    public function testGetInsertAllIntoTargetTableCommandConvertToNullWithTimestamp(): void
    {
        $this->createTestDb();
        $destination = $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new BigqueryTableDefinition(
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
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`, `_timestamp`) SELECT NULLIF(`col1`, \'\'),CAST(COALESCE(`col2`, \'\') as STRING) AS `col2`,CAST(\'2020-01-01 00:00:00\' as TIMESTAMP) FROM `import_export_test_schema`.`stagingTable` AS `src`',
            $sql
        );
        $out = $this->bqClient->runQuery($this->bqClient->query($sql));
        self::assertEquals(4, $out->info()['numDmlAffectedRows']);

        $result = $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_DB
        )));

        /** @var array<string, string> $item */
        foreach ($result as $item) {
            self::assertArrayHasKey('id', $item);
            self::assertArrayHasKey('col1', $item);
            self::assertArrayHasKey('col2', $item);
            self::assertArrayHasKey('_timestamp', $item);
        }
    }

    public function testGetTruncateTableCommand(): void
    {
        $this->createTestDb();
        $this->createStagingTableWithData();

        $ref = new BigqueryTableReflection($this->bqClient, self::TEST_DB, self::TEST_STAGING_TABLE);
        self::assertEquals(3, $ref->getRowsCount());

        $sql = $this->getBuilder()->getTruncateTable(self::TEST_DB, self::TEST_STAGING_TABLE);
        self::assertEquals(
            'TRUNCATE TABLE `import_export_test_schema`.`stagingTable`',
            $sql
        );
        $this->bqClient->runQuery($this->bqClient->query($sql));
        self::assertEquals(0, $ref->getRowsCount());
    }

    private function getStagingTableDefinition(): BigqueryTableDefinition
    {
        return new BigqueryTableDefinition(
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
