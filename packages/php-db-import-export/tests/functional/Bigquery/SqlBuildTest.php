<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery;

use DateTime;
use Generator;
use Google\Cloud\Core\Exception\NotFoundException;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
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
                self::TEST_STAGING_TABLE_QUOTED,
            ),
        ));
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'1\',\'1\',\'1\',\'1\')',
                self::TEST_DB_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED,
            ),
        ));
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'2\',\'2\',\'2\',\'2\')',
                self::TEST_DB_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED,
            ),
        ));

        if ($includeEmptyValues) {
            $this->bqClient->runQuery($this->bqClient->query(
                sprintf(
                    'INSERT INTO %s.%s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'2\',\'2\',\'\',NULL)',
                    self::TEST_DB_QUOTED,
                    self::TEST_STAGING_TABLE_QUOTED,
                ),
            ));
        }

        return $def;
    }

    private function assertTableNotExists(string $schemaName, string $tableName): void
    {
        try {
            (new BigqueryTableReflection($this->bqClient, $schemaName, $tableName))->getTableStats();
            self::fail(sprintf(
                'Table "%s.%s" is expected to not exist.',
                $schemaName,
                $tableName,
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
            "SELECT COUNT(*) AS count FROM `import_export_test_schema`.INFORMATION_SCHEMA.TABLES WHERE `table_type` != 'VIEW' AND table_name = 'import_export_test_test';", $sql
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
                $sql,
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

    public function getInsertAllIntoTargetTableCommandProvider(): Generator
    {
        yield 'typed' => [
            BigqueryImportOptions::USING_TYPES_USER,
            // phpcs:ignore
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`) SELECT `col1`,`col2` FROM `import_export_test_schema`.`stagingTable` AS `src`',
        ];
        yield 'string' => [
            BigqueryImportOptions::USING_TYPES_STRING,
            // phpcs:ignore
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`) SELECT CAST(COALESCE(`col1`, \'\') as STRING) AS `col1`,CAST(COALESCE(`col2`, \'\') as STRING) AS `col2` FROM `import_export_test_schema`.`stagingTable` AS `src`',
        ];
    }

    /**
     * @param BigqueryImportOptions::USING_TYPES_* $usingTypes
     * @dataProvider getInsertAllIntoTargetTableCommandProvider
     */
    public function testGetInsertAllIntoTargetTableCommand(string $usingTypes, string $expectedSql): void
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
            [],
        );

        // no convert values no timestamp
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            new BigqueryImportOptions(
                [],
                false,
                false,
                BigqueryImportOptions::SKIP_NO_LINE,
                $usingTypes,
            ),
            '2020-01-01 00:00:00',
        );

        self::assertEquals($expectedSql, $sql);

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
        bool $includePrimaryKey = false,
    ): BigqueryTableDefinition {
        $columns = [];
        $pks = [];
        if ($includePrimaryKey) {
            $pks[] = 'id';
            $columns[] = new BigqueryColumn(
                'id',
                new Bigquery(Bigquery::TYPE_INT),
            );
        } else {
            $columns[] = $this->createNullableGenericColumn('id');
        }
        $columns[] = $this->createNullableGenericColumn('col1');
        $columns[] = $this->createNullableGenericColumn('col2');

        if ($includeTimestamp) {
            $columns[] = new BigqueryColumn(
                '_timestamp',
                new Bigquery(Bigquery::TYPE_TIMESTAMP),
            );
        }

        $tableDefinition = new BigqueryTableDefinition(
            self::TEST_DB,
            self::TEST_TABLE,
            false,
            new ColumnCollection($columns),
            $pks,
        );
        $this->bqClient->runQuery($this->bqClient->query(
            (new BigqueryTableQueryBuilder())->getCreateTableCommandFromDefinition($tableDefinition),
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
            ],
        );

        return new BigqueryColumn(
            $columnName,
            $definition,
        );
    }

    /**
     * @return Generator<string, array{
     *     BigqueryImportOptions::USING_TYPES_*,
     *     string
     * }>
     */
    public function getInsertAllIntoTargetTableCommandConvertToNullProvider(): Generator
    {
        yield 'typed' => [ // nothing is converted
            BigqueryImportOptions::USING_TYPES_USER,
            // phpcs:ignore
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`) SELECT `col1`,`col2` FROM `import_export_test_schema`.`stagingTable` AS `src`'
        ];
        yield 'string' => [
            BigqueryImportOptions::USING_TYPES_STRING,
            // phpcs:ignore
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`) SELECT NULLIF(`col1`, \'\'),CAST(COALESCE(`col2`, \'\') as STRING) AS `col2` FROM `import_export_test_schema`.`stagingTable` AS `src`',
        ];
    }

    /**
     * @param BigqueryImportOptions::USING_TYPES_* $usingTypes
     * @dataProvider getInsertAllIntoTargetTableCommandConvertToNullProvider
     */
    public function testGetInsertAllIntoTargetTableCommandConvertToNull(string $usingTypes, string $expectedSql): void
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
            [],
        );

        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            new BigqueryImportOptions(
                ['col1'], // convert col1 to null
                false,
                false,
                BigqueryImportOptions::SKIP_NO_LINE,
                $usingTypes,
            ),
            '2020-01-01 00:00:00',
        );
        self::assertEquals($expectedSql, $sql);
        $out = $this->bqClient->runQuery($this->bqClient->query($sql));
        self::assertEquals(4, $out->info()['numDmlAffectedRows']);

        $result = $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_DB,
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

    /**
     * @return Generator<string, array{
     *     BigqueryImportOptions::USING_TYPES_*,
     *     string
     * }>
     */
    public function getInsertAllIntoTargetTableCommandConvertToNullWithTimestampProvider(): Generator
    {
        yield 'typed' => [ // nothing is converted
            BigqueryImportOptions::USING_TYPES_USER,
            // phpcs:ignore
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`, `_timestamp`) SELECT `col1`,`col2`,CAST(\'2020-01-01 00:00:00\' as TIMESTAMP) FROM `import_export_test_schema`.`stagingTable` AS `src`'
        ];
        yield 'string' => [
            BigqueryImportOptions::USING_TYPES_STRING,
            // phpcs:ignore
            'INSERT INTO `import_export_test_schema`.`import_export_test_test` (`col1`, `col2`, `_timestamp`) SELECT NULLIF(`col1`, \'\'),CAST(COALESCE(`col2`, \'\') as STRING) AS `col2`,CAST(\'2020-01-01 00:00:00\' as TIMESTAMP) FROM `import_export_test_schema`.`stagingTable` AS `src`',
        ];
    }

    /**
     * @param BigqueryImportOptions::USING_TYPES_* $usingTypes
     * @dataProvider getInsertAllIntoTargetTableCommandConvertToNullWithTimestampProvider
     */
    public function testGetInsertAllIntoTargetTableCommandConvertToNullWithTimestamp(
        string $usingTypes,
        string $expectedSql,
    ): void {
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
            [],
        );

        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            new BigqueryImportOptions(
                ['col1'],
                false,
                true, // use timestamp
                BigqueryImportOptions::SKIP_NO_LINE,
                $usingTypes,
            ),
            '2020-01-01 00:00:00',
        );
        self::assertEquals($expectedSql, $sql);
        $out = $this->bqClient->runQuery($this->bqClient->query($sql));
        self::assertEquals(4, $out->info()['numDmlAffectedRows']);

        $result = $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_DB,
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
            $sql,
        );
        $this->bqClient->runQuery($this->bqClient->query($sql));
        $ref->refresh();
        self::assertEquals(0, $ref->getRowsCount());
    }

    /**
     * @return Generator<string, array{
     *     BigqueryImportOptions::USING_TYPES_*,
     *     string
     * }>
     */
    public function getUpdateWithPkCommandProvider(): Generator
    {
        yield 'typed' => [ // nothing is converted
            BigqueryImportOptions::USING_TYPES_USER,
            // phpcs:ignore
            'UPDATE `import_export_test_schema`.`import_export_test_test` AS `dest` SET `col1` = `src`.`col1`, `col2` = `src`.`col2` FROM `import_export_test_schema`.`stagingTable` AS `src` WHERE `dest`.`col1` = `src`.`col1` '
        ];
        yield 'string' => [
            BigqueryImportOptions::USING_TYPES_STRING,
            // phpcs:ignore
            'UPDATE `import_export_test_schema`.`import_export_test_test` AS `dest` SET `col1` = COALESCE(`src`.`col1`, \'\'), `col2` = COALESCE(`src`.`col2`, \'\') FROM `import_export_test_schema`.`stagingTable` AS `src` WHERE `dest`.`col1` = COALESCE(`src`.`col1`, \'\') ',
        ];
    }

    /**
     * @param BigqueryImportOptions::USING_TYPES_* $usingTypes
     * @dataProvider getUpdateWithPkCommandProvider
     */
    public function testGetUpdateWithPkCommand(string $usingTypes, string $expectedSql): void
    {
        $this->createTestDb();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new BigqueryTableDefinition(
            self::TEST_DB,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1'],
        );
        // create fake stage and say that there is less columns
        $fakeStage = new BigqueryTableDefinition(
            self::TEST_DB,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s(`id`,`col1`,`col2`) VALUES (\'1\',\'2\',\'1\')',
                self::TEST_DB_QUOTED,
                self::TEST_TABLE_QUOTED,
            ),
        ));

        $result = $this->fetchTable(self::TEST_DB_QUOTED, self::TEST_TABLE_QUOTED);
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
            new BigqueryImportOptions(
                [],
                false,
                false,
                BigqueryImportOptions::SKIP_NO_LINE,
                $usingTypes,
            ),
            '2020-01-01 00:00:00',
        );
        self::assertEquals($expectedSql, $sql);
        $this->bqClient->runQuery($this->bqClient->query($sql));

        $result = $this->fetchTable(self::TEST_DB_QUOTED, self::TEST_TABLE_QUOTED);
        self::assertEquals([
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '2',
            ],
        ], $result);
    }

    /**
     * @return Generator<string, array{
     *     BigqueryImportOptions::USING_TYPES_*,
     *     string
     * }>
     */
    public function getUpdateWithPkCommandConvertValuesProvider(): Generator
    {
        yield 'typed' => [ // nothing is converted
            BigqueryImportOptions::USING_TYPES_USER,
            // phpcs:ignore
            'UPDATE `import_export_test_schema`.`import_export_test_test` AS `dest` SET `col1` = `src`.`col1`, `col2` = `src`.`col2` FROM `import_export_test_schema`.`stagingTable` AS `src` WHERE `dest`.`col1` = `src`.`col1` '
        ];
        yield 'string' => [
            BigqueryImportOptions::USING_TYPES_STRING,
            // phpcs:ignore
            'UPDATE `import_export_test_schema`.`import_export_test_test` AS `dest` SET `col1` = IF(`src`.`col1` = \'\', NULL, `src`.`col1`), `col2` = COALESCE(`src`.`col2`, \'\') FROM `import_export_test_schema`.`stagingTable` AS `src` WHERE `dest`.`col1` = COALESCE(`src`.`col1`, \'\') ',
        ];
    }

    /**
     * @param BigqueryImportOptions::USING_TYPES_* $usingTypes
     * @dataProvider getUpdateWithPkCommandConvertValuesProvider
     */
    public function testGetUpdateWithPkCommandConvertValues(string $usingTypes, string $expectedSql): void
    {
        $this->createTestDb();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new BigqueryTableDefinition(
            self::TEST_DB,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1'],
        );
        // create fake stage and say that there is less columns
        $fakeStage = new BigqueryTableDefinition(
            self::TEST_DB,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s(`id`,`col1`,`col2`) VALUES (\'1\',\'\',\'1\')',
                self::TEST_DB_QUOTED,
                self::TEST_TABLE_QUOTED,
            ),
        ));
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s(`id`,`col1`,`col2`) VALUES (\'1\',\'2\',\'\')',
                self::TEST_DB_QUOTED,
                self::TEST_TABLE_QUOTED,
            ),
        ));

        $result = $this->fetchTable(self::TEST_DB_QUOTED, self::TEST_TABLE_QUOTED);

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

        $options = new BigqueryImportOptions(['col1'], false, false, BigqueryImportOptions::SKIP_NO_LINE, $usingTypes);

        // converver values
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            '2020-01-01 00:00:00',
        );
        self::assertEquals($expectedSql, $sql);
        $this->bqClient->runQuery($this->bqClient->query($sql));

        $result = $this->fetchTable(self::TEST_DB_QUOTED, self::TEST_TABLE_QUOTED);

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

    /**
     * @return Generator<string, array{
     *     BigqueryImportOptions::USING_TYPES_*,
     *     string
     * }>
     */
    public function getUpdateWithPkCommandConvertValuesWithTimestampProvider(): Generator
    {
        yield 'typed' => [ // nothing is converted
            BigqueryImportOptions::USING_TYPES_USER,
            // phpcs:ignore
            'UPDATE `import_export_test_schema`.`import_export_test_test` AS `dest` SET `col1` = `src`.`col1`, `col2` = `src`.`col2`, `_timestamp` = \'2020-01-01 01:01:01\' FROM `import_export_test_schema`.`stagingTable` AS `src` WHERE `dest`.`col1` = `src`.`col1` ',
        ];
        yield 'string' => [
            BigqueryImportOptions::USING_TYPES_STRING,
            // phpcs:ignore
            'UPDATE `import_export_test_schema`.`import_export_test_test` AS `dest` SET `col1` = IF(`src`.`col1` = \'\', NULL, `src`.`col1`), `col2` = COALESCE(`src`.`col2`, \'\'), `_timestamp` = \'2020-01-01 01:01:01\' FROM `import_export_test_schema`.`stagingTable` AS `src` WHERE `dest`.`col1` = COALESCE(`src`.`col1`, \'\') ',
        ];
    }

    /**
     * @param BigqueryImportOptions::USING_TYPES_* $usingTypes
     * @dataProvider getUpdateWithPkCommandConvertValuesWithTimestampProvider
     */
    public function testGetUpdateWithPkCommandConvertValuesWithTimestamp(string $usingTypes, string $expectedSql): void
    {
        $timestampInit = new DateTime('2020-01-01 00:00:01');
        $timestampSet = new DateTime('2020-01-01 01:01:01');
        $this->createTestDb();
        $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);

        // create fake destination and say that there is pk on col1
        $fakeDestination = new BigqueryTableDefinition(
            self::TEST_DB,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1'],
        );
        // create fake stage and say that there is less columns
        $fakeStage = new BigqueryTableDefinition(
            self::TEST_DB,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s(`id`,`col1`,`col2`,`_timestamp`) VALUES (\'1\',\'\',\'1\',\'%s\')',
                self::TEST_DB_QUOTED,
                self::TEST_TABLE_QUOTED,
                $timestampInit->format(DateTimeHelper::FORMAT),
            ),
        ));
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s(`id`,`col1`,`col2`,`_timestamp`) VALUES (\'1\',\'2\',\'\',\'%s\')',
                self::TEST_DB_QUOTED,
                self::TEST_TABLE_QUOTED,
                $timestampInit->format(DateTimeHelper::FORMAT),
            ),
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
        ], $this->fetchTable(self::TEST_DB_QUOTED, self::TEST_TABLE_QUOTED));

        // use timestamp
        $options = new BigqueryImportOptions(
            ['col1'],
            false,
            true,
            BigqueryImportOptions::SKIP_NO_LINE,
            $usingTypes,
        );
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            $timestampSet->format(DateTimeHelper::FORMAT),
        );

        self::assertEquals($expectedSql, $sql);
        $this->bqClient->runQuery($this->bqClient->query($sql));
        $result = $this->fetchTable(self::TEST_DB_QUOTED, self::TEST_TABLE_QUOTED);

        foreach ($result as $item) {
            self::assertArrayHasKey('id', $item);
            self::assertArrayHasKey('col1', $item);
            self::assertArrayHasKey('col2', $item);
            self::assertArrayHasKey('_timestamp', $item);
            self::assertIsString($item['_timestamp']);
            self::assertSame(
                $timestampSet->format(DateTimeHelper::FORMAT),
                (new DateTime($item['_timestamp']))->format(DateTimeHelper::FORMAT),
            );
        }
    }

    public function nullManipulationWithTimestampFeatures(): Generator
    {
//        yield 'default' => [
//            'default',
//            // phpcs:ignore
//            'UPDATE `import_export_test_schema`.`import_export_test_test` AS `dest` SET `col1` = `src`.`col1`, `col2` = `src`.`col2`, `_timestamp` = \'2020-01-01 01:01:01\' FROM `import_export_test_schema`.`stagingTable` AS `src` WHERE `dest`.`col1` = `src`.`col1` ',
//            true,
//        ];
        yield 'new-native-types' => [
            'new-native-types',
            // phpcs:ignore
            'UPDATE `import_export_test_schema`.`import_export_test_test` AS `dest` SET `col1` = `src`.`col1`, `col2` = `src`.`col2`, `_timestamp` = \'2020-01-01 01:01:01\' FROM `import_export_test_schema`.`stagingTable` AS `src` WHERE `dest`.`col1` IS NOT DISTINCT FROM `src`.`col1`  AND (`dest`.`col1` IS DISTINCT FROM `src`.`col1` OR `dest`.`col2` IS DISTINCT FROM `src`.`col2`)',
            false,
        ];
        yield 'native-types_timestamp-bc' => [
            'native-types_timestamp-bc',
            // phpcs:ignore
            'UPDATE `import_export_test_schema`.`import_export_test_test` AS `dest` SET `col1` = `src`.`col1`, `col2` = `src`.`col2`, `_timestamp` = \'2020-01-01 01:01:01\' FROM `import_export_test_schema`.`stagingTable` AS `src` WHERE `dest`.`col1` IS NOT DISTINCT FROM `src`.`col1`  AND (`dest`.`col1` IS DISTINCT FROM `src`.`col1` OR `dest`.`col2` IS DISTINCT FROM `src`.`col2`)',
            false,
        ];
    }

    /**
     * @dataProvider nullManipulationWithTimestampFeatures
     */
    public function testGetUpdateWithPkCommandNullManipulationWithTimestampFeatures(
        string $feature,
        string $expectedSQL,
        bool $expectedTimestampChange,
    ): void {
        $timestampInit = new DateTime('2020-01-01 00:00:01');
        $timestampSet = new DateTime('2020-01-01 01:01:01');
        $this->createTestDb();
        $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'3\',\'3\',\'\',NULL)',
                self::TEST_DB_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED,
            ),
        ));

        // create fake destination and say that there is pk on col1
        $fakeDestination = new BigqueryTableDefinition(
            self::TEST_DB,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1'],
        );
        // create fake stage and say that there is less columns
        $fakeStage = new BigqueryTableDefinition(
            self::TEST_DB,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s(`id`,`col1`,`col2`,`_timestamp`) VALUES (\'1\',\'1\',\'1\',\'%s\')',
                self::TEST_DB_QUOTED,
                self::TEST_TABLE_QUOTED,
                $timestampInit->format(DateTimeHelper::FORMAT),
            ),
        ));
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s(`id`,`col1`,`col2`,`_timestamp`) VALUES (\'3\',\'3\',NULL,\'%s\')',
                self::TEST_DB_QUOTED,
                self::TEST_TABLE_QUOTED,
                $timestampInit->format(DateTimeHelper::FORMAT),
            ),
        ));

        self::assertEqualsCanonicalizing([
            [
                'id' => '1',
                'col1' => '1',
                'col2' => '1',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],
            [
                'id' => '3',
                'col1' => '3',
                'col2' => null,
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],
        ], $this->fetchTable(self::TEST_DB_QUOTED, self::TEST_TABLE_QUOTED));

        // use timestamp
        $options = new BigQueryImportOptions(
            convertEmptyValuesToNull: [],
            isIncremental: false,
            useTimestamp: true,
            numberOfIgnoredLines: 0,
            usingTypes: BigQueryImportOptions::USING_TYPES_USER,
            importAsNull: BigQueryImportOptions::DEFAULT_IMPORT_AS_NULL,
            features: [$feature],
        );
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            $timestampSet->format(DateTimeHelper::FORMAT),
        );

        self::assertEquals($expectedSQL, $sql);
        $this->bqClient->runQuery($this->bqClient->query($sql));
        $result = $this->fetchTable(self::TEST_DB_QUOTED, self::TEST_TABLE_QUOTED);

        $assertTimestamp = $timestampInit;
        if ($expectedTimestampChange) {
            $assertTimestamp = $timestampSet;
        }

        // timestamp was updated to $timestampSet but there is same row as in stage table so no other value is updated
        self::assertEqualsCanonicalizing([
            [
                'id' => '1',
                'col1' => '1',
                'col2' => '1',
                '_timestamp' => $assertTimestamp->format(DateTimeHelper::FORMAT),
            ], // timestamp is not update when row has same value
            [
                'id' => '3',
                'col1' => '3',
                'col2' => null,
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],  // timestamp is not update when row has same value and there is null
        ], $result);
    }


    /**
     * @return Generator<string, array{
     *     BigqueryImportOptions::USING_TYPES_*,
     *     string
     * }>
     */
    public function getDeleteOldItemsCommandProvider(): Generator
    {
        yield 'typed' => [ // nothing is converted
            BigqueryImportOptions::USING_TYPES_USER,
            // phpcs:ignore
            'DELETE `import_export_test_schema`.`stagingTable` AS `src` WHERE EXISTS (SELECT * FROM `import_export_test_schema`.`import_export_test_test` AS `dest` WHERE `dest`.`pk1` = COALESCE(`src`.`pk1`, \'\') AND `dest`.`pk2` = COALESCE(`src`.`pk2`, \'\') )'
        ];
        yield 'string' => [
            BigqueryImportOptions::USING_TYPES_STRING,
            // phpcs:ignore
            'DELETE `import_export_test_schema`.`stagingTable` AS `src` WHERE EXISTS (SELECT * FROM `import_export_test_schema`.`import_export_test_test` AS `dest` WHERE `dest`.`pk1` = COALESCE(`src`.`pk1`, \'\') AND `dest`.`pk2` = COALESCE(`src`.`pk2`, \'\') )'
        ];
    }

    /**
     * @param BigqueryImportOptions::USING_TYPES_* $usingTypes
     * @dataProvider getDeleteOldItemsCommandProvider
     */
    public function testGetDeleteOldItemsCommand(string $usingTypes, string $expectedSql): void
    {
        $this->createTestDb();

        $tableDefinition = new BigqueryTableDefinition(
            self::TEST_DB,
            self::TEST_TABLE,
            false,
            new ColumnCollection([
                new BigqueryColumn(
                    'id',
                    new Bigquery(
                        Bigquery::TYPE_INT,
                    ),
                ),
                BigqueryColumn::createGenericColumn('pk1'),
                BigqueryColumn::createGenericColumn('pk2'),
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2'],
        );
        $tableSql = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($tableDefinition->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($tableDefinition->getTableName()),
        );
        $qb = new BigqueryTableQueryBuilder();
        $this->bqClient->runQuery($this->bqClient->query($qb->getCreateTableCommandFromDefinition($tableDefinition)));
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s(`id`,`pk1`,`pk2`,`col1`,`col2`) VALUES (1,\'1\',\'1\',\'1\',\'1\')',
                $tableSql,
            ),
        ));
        $stagingTableDefinition = new BigqueryTableDefinition(
            self::TEST_DB,
            self::TEST_STAGING_TABLE,
            false,
            new ColumnCollection([
                BigqueryColumn::createGenericColumn('pk1'),
                BigqueryColumn::createGenericColumn('pk2'),
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2'],
        );
        $this->bqClient->runQuery($this->bqClient->query(
            $qb->getCreateTableCommandFromDefinition($stagingTableDefinition),
        ));
        $stagingTableSql = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
        );
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'1\',\'1\',\'1\',\'1\')',
                $stagingTableSql,
            ),
        ));
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'INSERT INTO %s(`pk1`,`pk2`,`col1`,`col2`) VALUES (\'2\',\'1\',\'1\',\'1\')',
                $stagingTableSql,
            ),
        ));

        $sql = $this->getBuilder()->getDeleteOldItemsCommand(
            $stagingTableDefinition,
            $tableDefinition,
            $this->getSimpleImportOptions(),
        );

        self::assertEquals($expectedSql, $sql);
        $this->bqClient->runQuery($this->bqClient->query($sql));

        $result = $this->fetchTable($stagingTableDefinition->getSchemaName(), $stagingTableDefinition->getTableName());

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
            [],
        );
    }
}
