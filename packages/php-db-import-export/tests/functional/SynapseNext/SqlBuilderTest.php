<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\SynapseNext;

use DateTime;
use Keboola\Datatype\Definition\Synapse;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;

class SqlBuilderTest extends SynapseBaseTestCase
{
    public const TESTS_PREFIX = 'import-export-test-ng_';
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'schema';
    public const TEST_SCHEMA_QUOTED = '[' . self::TEST_SCHEMA . ']';
    public const TEST_STAGING_TABLE = '#stagingTable';
    public const TEST_TABLE = self::TESTS_PREFIX . 'test';
    public const TEST_TABLE_IN_SCHEMA = self::TEST_SCHEMA_QUOTED . '.' . self::TEST_TABLE_QUOTED;
    public const TEST_TABLE_QUOTED = '[' . self::TEST_TABLE . ']';

    protected function dropTestSchema(): void
    {
        $this->connection->exec(sprintf('DROP SCHEMA %s', self::TEST_SCHEMA_QUOTED));
    }

    protected function getBuilder(): SqlBuilder
    {
        return new SqlBuilder();
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema(self::TEST_SCHEMA);
    }

    protected function createTestSchema(): void
    {
        $this->connection->exec(sprintf('CREATE SCHEMA %s', self::TEST_SCHEMA_QUOTED));
    }

    public function testGetDedupCommand(): void
    {
        $this->createTestSchema();
        $stageDef = $this->createStagingTableWithData();

        $deduplicationDef = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            '#tempTable',
            true,
            new ColumnCollection([
                SynapseColumn::createGenericColumn('col1'),
                SynapseColumn::createGenericColumn('col2'),
            ]),
            [
                'pk1',
                'pk2',
            ],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX)
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->exec($qb->getCreateTableCommandFromDefinition($deduplicationDef));

        $sql = $this->getBuilder()->getDedupCommand(
            $stageDef,
            $deduplicationDef,
            $deduplicationDef->getPrimaryKeysNames()
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO [import-export-test-ng_schema].[#tempTable] ([col1], [col2]) SELECT a.[col1],a.[col2] FROM (SELECT [col1], [col2], ROW_NUMBER() OVER (PARTITION BY [pk1],[pk2] ORDER BY [pk1],[pk2]) AS "_row_number_" FROM [import-export-test-ng_schema].[#stagingTable]) AS a WHERE a."_row_number_" = 1',
            $sql
        );
        $this->connection->exec($sql);
        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s.%s',
            SynapseQuote::quoteSingleIdentifier($deduplicationDef->getSchemaName()),
            SynapseQuote::quoteSingleIdentifier($deduplicationDef->getTableName())
        ));

        self::assertCount(2, $result);
    }

    private function createStagingTableWithData(bool $includeEmptyValues = false): SynapseTableDefinition
    {
        $def = $this->getStagingTableDefinition();
        $qb = new SynapseTableQueryBuilder();
        $this->connection->exec($qb->getCreateTableCommandFromDefinition($def));

        $this->connection->exec(
            sprintf(
                'INSERT INTO %s.%s([pk1],[pk2],[col1],[col2]) VALUES (1,1,\'1\',\'1\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE
            )
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s.%s([pk1],[pk2],[col1],[col2]) VALUES (1,1,\'1\',\'1\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE
            )
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s.%s([pk1],[pk2],[col1],[col2]) VALUES (2,2,\'2\',\'2\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE
            )
        );

        if ($includeEmptyValues) {
            $this->connection->exec(
                sprintf(
                    'INSERT INTO %s.%s([pk1],[pk2],[col1],[col2]) VALUES (2,2,\'\',NULL)',
                    self::TEST_SCHEMA_QUOTED,
                    self::TEST_STAGING_TABLE
                )
            );
        }

        return $def;
    }

    private function getDummyImportOptions(): SynapseImportOptions
    {
        return new SynapseImportOptions([]);
    }

    public function testGetDeleteOldItemsCommand(): void
    {
        $this->createTestSchema();

        $tableDefinition = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection([
                new SynapseColumn(
                    'id',
                    new Synapse(
                        Synapse::TYPE_INT
                    )
                ),
                SynapseColumn::createGenericColumn('pk1'),
                SynapseColumn::createGenericColumn('pk2'),
                SynapseColumn::createGenericColumn('col1'),
                SynapseColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX)
        );
        $tableSql = sprintf(
            '%s.%s',
            SynapseQuote::quoteSingleIdentifier($tableDefinition->getSchemaName()),
            SynapseQuote::quoteSingleIdentifier($tableDefinition->getTableName())
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->exec($qb->getCreateTableCommandFromDefinition($tableDefinition));
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[pk1],[pk2],[col1],[col2]) VALUES (1,1,1,\'1\',\'1\')',
                $tableSql
            )
        );
        $stagingTableDefinition = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            false,
            new ColumnCollection([
                SynapseColumn::createGenericColumn('pk1'),
                SynapseColumn::createGenericColumn('pk2'),
                SynapseColumn::createGenericColumn('col1'),
                SynapseColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX)
        );
        $this->connection->exec($qb->getCreateTableCommandFromDefinition($stagingTableDefinition));
        $stagingTableSql = sprintf(
            '%s.%s',
            SynapseQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SynapseQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName())
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([pk1],[pk2],[col1],[col2]) VALUES (1,1,\'1\',\'1\')',
                $stagingTableSql
            )
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([pk1],[pk2],[col1],[col2]) VALUES (2,1,\'1\',\'1\')',
                $stagingTableSql
            )
        );

        $sql = $this->getBuilder()->getDeleteOldItemsCommand(
            $stagingTableDefinition,
            $tableDefinition
        );

        $this->assertEquals(
        // phpcs:ignore
            'DELETE [import-export-test-ng_schema].[#stagingTable] WHERE EXISTS (SELECT * FROM [import-export-test-ng_schema].[import-export-test-ng_test] WHERE [import-export-test-ng_schema].[import-export-test-ng_test].[pk1] = COALESCE([import-export-test-ng_schema].[#stagingTable].[pk1], \'\') AND [import-export-test-ng_schema].[import-export-test-ng_test].[pk2] = COALESCE([import-export-test-ng_schema].[#stagingTable].[pk2], \'\'))',
            $sql
        );
        $this->connection->exec($sql);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            $stagingTableSql
        ));

        $this->assertCount(1, $result);
        $this->assertSame([
            [
                'pk1' => '2',
                'pk2' => '1',
                'col1' => '1',
                'col2' => '1',
            ],
        ], $result);
    }

    public function testGetDropCommand(): void
    {
        $this->createTestSchema();
        $this->createTestTable();
        $sql = $this->getBuilder()->getDropCommand(self::TEST_SCHEMA, self::TEST_TABLE);

        $this->assertEquals(
            'DROP TABLE [import-export-test-ng_schema].[import-export-test-ng_test]',
            $sql
        );

        $this->connection->exec($sql);

        $this->assertTableNotExists(self::TEST_SCHEMA, self::TEST_TABLE);
    }

    private function assertTableNotExists(string $schemaName, string $tableName): void
    {
        try {
            (new SynapseTableReflection($this->connection, $schemaName, $tableName))->getObjectId();
            self::fail(sprintf(
                'Table "%s.%s" is expected to not exist.',
                $schemaName,
                $tableName
            ));
        } catch (TableNotExistsReflectionException $e) {
            $this->assertEquals(sprintf('Table "%s.%s" does not exist.', $schemaName, $tableName), $e->getMessage());
        }
    }

    public function testGetDropTableIfExistsCommand(): void
    {
        $this->assertTableNotExists(self::TEST_SCHEMA, self::TEST_TABLE);

        // try to drop not existing table
        $sql = $this->getBuilder()->getDropTableIfExistsCommand(self::TEST_SCHEMA, self::TEST_TABLE);
        $this->assertEquals(
        // phpcs:ignore
            'IF OBJECT_ID (N\'[import-export-test-ng_schema].[import-export-test-ng_test]\', N\'U\') IS NOT NULL DROP TABLE [import-export-test-ng_schema].[import-export-test-ng_test]',
            $sql
        );
        $this->connection->exec($sql);

        // create table
        $this->createTestSchema();
        $this->createTestTable();

        // try to drop not existing table
        $sql = $this->getBuilder()->getDropTableIfExistsCommand(self::TEST_SCHEMA, self::TEST_TABLE);
        $this->assertEquals(
        // phpcs:ignore
            'IF OBJECT_ID (N\'[import-export-test-ng_schema].[import-export-test-ng_test]\', N\'U\') IS NOT NULL DROP TABLE [import-export-test-ng_schema].[import-export-test-ng_test]',
            $sql
        );
        $this->connection->exec($sql);

        $this->assertTableNotExists(self::TEST_SCHEMA, self::TEST_TABLE);
    }

    protected function createTestTable(): void
    {
        $table = self::TEST_TABLE_IN_SCHEMA;
        $this->connection->exec(<<<EOT
CREATE TABLE $table (
    id int NOT NULL
)
WITH
    (
      PARTITION ( id RANGE LEFT FOR VALUES ( )),
      CLUSTERED COLUMNSTORE INDEX
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
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // no convert values no timestamp
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $this->getDummyImportOptions(),
            '2020-01-01 00:00:00'
        );

        $this->assertEquals(
        // phpcs:ignore
            'INSERT INTO [import-export-test-ng_schema].[import-export-test-ng_test] ([col1], [col2]) (SELECT CAST(COALESCE([col1], \'\') as NVARCHAR(4000)) AS [col1],CAST(COALESCE([col2], \'\') as NVARCHAR(4000)) AS [col2] FROM [import-export-test-ng_schema].[#stagingTable] AS [src])',
            $sql
        );

        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
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

    public function testGetInsertAllIntoTargetTableCommandNotString(): void
    {
        $col2 = new SynapseColumn(
            'col2',
            new Synapse(
                Synapse::TYPE_NUMERIC
            )
        );

        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns(false, false, $col2);
        $this->createStagingTableWithData(true);

        // create fake stage with missing id column and numeric col2
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $col2,
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // no convert values no timestamp
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $this->getDummyImportOptions(),
            '2020-01-01 00:00:00'
        );

        $this->assertEquals(
        // phpcs:ignore
            'INSERT INTO [import-export-test-ng_schema].[import-export-test-ng_test] ([col1], [col2]) (SELECT CAST(COALESCE([col1], \'\') as NVARCHAR(4000)) AS [col1],CAST([col2] as NUMERIC) AS [col2] FROM [import-export-test-ng_schema].[#stagingTable] AS [src])',
            $sql
        );

        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
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
                'col2' => null,
            ],
        ], $result);
    }

    protected function createTestTableWithColumns(
        bool $includeTimestamp = false,
        bool $includePrimaryKey = false,
        ?SynapseColumn $overwriteColumn2 = null
    ): SynapseTableDefinition {
        $tableDefinition = $this->getTestTableWithColumnsDefinition(
            $includeTimestamp,
            $includePrimaryKey,
            $overwriteColumn2
        );
        $this->connection->exec(
            (new SynapseTableQueryBuilder())->getCreateTableCommandFromDefinition($tableDefinition)
        );

        return $tableDefinition;
    }

    private function createGenericColumn(string $columnName, bool $nullable = true): SynapseColumn
    {
        $definition = new Synapse(
            Synapse::TYPE_NVARCHAR,
            [
                'length' => '4000', // should be changed to max in future
                'nullable' => $nullable,
            ]
        );

        return new SynapseColumn(
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
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // convert col1 to null
        $options = new SynapseImportOptions(['col1']);
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
        // phpcs:ignore
            'INSERT INTO [import-export-test-ng_schema].[import-export-test-ng_test] ([col1], [col2]) (SELECT NULLIF([col1], \'\'),CAST(COALESCE([col2], \'\') as NVARCHAR(4000)) AS [col2] FROM [import-export-test-ng_schema].[#stagingTable] AS [src])',
            $sql
        );
        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
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
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // use timestamp
        $options = new SynapseImportOptions(['col1'], false, true);
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
        // phpcs:ignore
            'INSERT INTO [import-export-test-ng_schema].[import-export-test-ng_test] ([col1], [col2], [_timestamp]) (SELECT NULLIF([col1], \'\'),CAST(COALESCE([col2], \'\') as NVARCHAR(4000)) AS [col2],\'2020-01-01 00:00:00\' FROM [import-export-test-ng_schema].[#stagingTable] AS [src])',
            $sql
        );
        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        foreach ($result as $item) {
            $this->assertArrayHasKey('id', $item);
            $this->assertArrayHasKey('col1', $item);
            $this->assertArrayHasKey('col2', $item);
            $this->assertArrayHasKey('_timestamp', $item);
        }
    }

    public function testGetRenameTableCommand(): void
    {
        $renameTo = 'newTable';
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $sql = $this->getBuilder()->getRenameTableCommand(self::TEST_SCHEMA, self::TEST_TABLE, $renameTo);

        $this->assertEquals(
            'RENAME OBJECT [import-export-test-ng_schema].[import-export-test-ng_test] TO [newTable]',
            $sql
        );

        $this->connection->exec($sql);

        $this->assertTableNotExists(self::TEST_SCHEMA, self::TEST_TABLE);

        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, $renameTo);
        $this->assertIsString($ref->getObjectId());
    }

    public function testGetTruncateTableWithDeleteCommand(): void
    {
        $this->createTestSchema();
        $this->createStagingTableWithData();

        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $this->assertEquals(3, $ref->getRowsCount());

        $sql = $this->getBuilder()->getTruncateTableWithDeleteCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $this->assertEquals(
            'DELETE FROM [import-export-test-ng_schema].[#stagingTable]',
            $sql
        );
        $this->connection->exec($sql);
        $this->assertEquals(0, $ref->getRowsCount());
    }

    public function testGetUpdateWithPkCommand(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            ['col1'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[col1],[col2]) VALUES (1,\'2\',\'1\')',
                self::TEST_TABLE_IN_SCHEMA
            )
        );

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEquals([
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
        $this->assertEquals(
        // phpcs:ignore
            'UPDATE [import-export-test-ng_schema].[import-export-test-ng_test] SET [col2] = COALESCE([src].[col2], \'\') FROM [import-export-test-ng_schema].[#stagingTable] AS [src] WHERE [import-export-test-ng_schema].[import-export-test-ng_test].[col1] = COALESCE([src].[col1], \'\') AND (COALESCE(CAST([import-export-test-ng_schema].[import-export-test-ng_test].[col1] AS NVARCHAR(4000)), \'\') != COALESCE([src].[col1], \'\') OR COALESCE(CAST([import-export-test-ng_schema].[import-export-test-ng_test].[col2] AS NVARCHAR(4000)), \'\') != COALESCE([src].[col2], \'\')) ',
            $sql
        );
        $this->connection->exec($sql);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEquals([
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '2',
            ],
        ], $result);
    }

    public function testGetUpdateWithPkCommandNotString(): void
    {
        $col2 = new SynapseColumn(
            'col2',
            new Synapse(
                Synapse::TYPE_NUMERIC
            )
        );

        $this->createTestSchema();
        $this->createTestTableWithColumns(false, false, $col2);
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $col2,
            ]),
            ['col1'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $col2,
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[col1],[col2]) VALUES (1,\'2\',\'1\')',
                self::TEST_TABLE_IN_SCHEMA
            )
        );

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEquals([
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
        $this->assertEquals(
        // phpcs:ignore
            'UPDATE [import-export-test-ng_schema].[import-export-test-ng_test] SET [col2] = [src].[col2] FROM [import-export-test-ng_schema].[#stagingTable] AS [src] WHERE [import-export-test-ng_schema].[import-export-test-ng_test].[col1] = COALESCE([src].[col1], \'\') AND (COALESCE(CAST([import-export-test-ng_schema].[import-export-test-ng_test].[col1] AS NVARCHAR(4000)), \'\') != COALESCE([src].[col1], \'\') OR CAST([import-export-test-ng_schema].[import-export-test-ng_test].[col2] AS NUMERIC) != [src].[col2]) ',
            $sql
        );
        $this->connection->exec($sql);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEquals([
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
        $fakeDestination = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            ['col1'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[col1],[col2]) VALUES (1,\'\',\'1\')',
                self::TEST_TABLE_IN_SCHEMA
            )
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[col1],[col2]) VALUES (1,\'2\',\'\')',
                self::TEST_TABLE_IN_SCHEMA
            )
        );

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
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

        $options = new SynapseImportOptions(['col1']);

        // converver values
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
        // phpcs:ignore
            'UPDATE [import-export-test-ng_schema].[import-export-test-ng_test] SET [col2] = COALESCE([src].[col2], \'\') FROM [import-export-test-ng_schema].[#stagingTable] AS [src] WHERE [import-export-test-ng_schema].[import-export-test-ng_test].[col1] = COALESCE([src].[col1], \'\') AND (COALESCE(CAST([import-export-test-ng_schema].[import-export-test-ng_test].[col1] AS NVARCHAR(4000)), \'\') != COALESCE([src].[col1], \'\') OR COALESCE(CAST([import-export-test-ng_schema].[import-export-test-ng_test].[col2] AS NVARCHAR(4000)), \'\') != COALESCE([src].[col2], \'\')) ',
            $sql
        );
        $this->connection->exec($sql);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
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
        $fakeDestination = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            ['col1'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[col1],[col2],[_timestamp]) VALUES (1,\'\',\'1\',\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT)
            )
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[col1],[col2],[_timestamp]) VALUES (1,\'2\',\'\',\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT)
            )
        );

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
            [
                'id' => '1',
                'col1' => '',
                'col2' => '1',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT) . '.000',
            ],
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT) . '.000',
            ],
        ], $result);

        // use timestamp
        $options = new SynapseImportOptions(['col1'], false, true);
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            $timestampSet->format(DateTimeHelper::FORMAT) . '.000'
        );

        $this->assertEquals(
        // phpcs:ignore
            'UPDATE [import-export-test-ng_schema].[import-export-test-ng_test] SET [col2] = COALESCE([src].[col2], \'\'), [_timestamp] = \'2020-01-01 01:01:01.000\' FROM [import-export-test-ng_schema].[#stagingTable] AS [src] WHERE [import-export-test-ng_schema].[import-export-test-ng_test].[col1] = COALESCE([src].[col1], \'\') AND (COALESCE(CAST([import-export-test-ng_schema].[import-export-test-ng_test].[col1] AS NVARCHAR(4000)), \'\') != COALESCE([src].[col1], \'\') OR COALESCE(CAST([import-export-test-ng_schema].[import-export-test-ng_test].[col2] AS NVARCHAR(4000)), \'\') != COALESCE([src].[col2], \'\')) ',
            $sql
        );
        $this->connection->exec($sql);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        foreach ($result as $item) {
            $this->assertArrayHasKey('id', $item);
            $this->assertArrayHasKey('col1', $item);
            $this->assertArrayHasKey('col2', $item);
            $this->assertArrayHasKey('_timestamp', $item);
            $this->assertSame(
                $timestampSet->format(DateTimeHelper::FORMAT),
                (new DateTime($item['_timestamp']))->format(DateTimeHelper::FORMAT)
            );
        }
    }

    public function testTransaction(): void
    {
        $this->createTestSchema();
        $this->createTestTable();

        $sql = $this->getBuilder()->getBeginTransaction();
        self::assertSame('BEGIN TRANSACTION', $sql);
        $this->connection->exec($sql);

        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id]) VALUES (1)',
                self::TEST_TABLE_IN_SCHEMA
            )
        );

        $sql = $this->getBuilder()->getCommitTransaction();
        self::assertSame('COMMIT', $sql);
        $this->connection->exec($sql);
    }

    /**
     * @dataProvider ctasDedupProvider
     */
    public function testGetCtasDedupCommand(
        SynapseTableDefinition $stage,
        SynapseTableDefinition $destination,
        SynapseImportOptions $options,
        string $expectedSql,
        bool $isTimestampExpected = true
    ): void {
        $this->createTestSchema();
        $this->createStagingTableWithData(true);

        $sql = $this->getBuilder()->getCtasDedupCommand(
            $stage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
            $expectedSql,
            $sql
        );
        $out = $this->connection->executeStatement($sql);
        $this->assertEquals(2, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        foreach ($result as $item) {
            $this->assertArrayHasKey('pk1', $item);
            $this->assertArrayHasKey('pk2', $item);
            $this->assertArrayHasKey('col1', $item);
            $this->assertArrayHasKey('col2', $item);
            if ($isTimestampExpected) {
                $this->assertArrayHasKey('_timestamp', $item);
            } else {
                $this->assertArrayNotHasKey('_timestamp', $item);
            }
        }

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );
        /** @var SynapseColumn[] $timestampColumns */
        $timestampColumns = array_filter(iterator_to_array($ref->getColumnsDefinitions()), function (
            SynapseColumn $column
        ) {
            return $column->getColumnName() === '_timestamp';
        });
        if ($options->useTimestamp()) {
            self::assertCount(1, $timestampColumns);
            /** @var SynapseColumn $timestampColumn */
            $timestampColumn = array_shift($timestampColumns);
            self::assertSame(Synapse::TYPE_DATETIME2, $timestampColumn->getColumnDefinition()->getType());
        }

        $resultColumns = $ref->getColumnsDefinitions();
        $this->assertColumnsDefinitions($resultColumns, $destination, $options);
    }

    /**
     * @return \Generator<string, array{
     *  0: SynapseTableDefinition,
     *  1: SynapseTableDefinition,
     *  2: SynapseImportOptions,
     *  3: string,
     *  4?: bool
     * }>
     */
    public function ctasDedupProvider(): \Generator
    {
        $stage = $this->getStagingTableDefinition();

        yield 'testGetCtasDedupCommandWithNotStringType' => [
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                $stage->getTableName(),
                $stage->isTemporary(),
                new ColumnCollection([
                    $this->createGenericColumn('pk1'),
                    $this->createGenericColumn('pk2'),
                    new SynapseColumn(
                        'col1',
                        new Synapse(
                            Synapse::TYPE_INT
                        )
                    ),
                    $this->createGenericColumn('col2'),
                ]),
                $stage->getPrimaryKeysNames(),
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                [],
                false,
                true,
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TEMP_TABLE_HEAP,
                SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
                SynapseImportOptions::TABLE_TYPES_CAST
            ),
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[pk1],a.[pk2],a.[col1],a.[col2],a.[_timestamp] FROM (SELECT COALESCE(CAST([pk1] as NVARCHAR(4000)), \'\') AS [pk1],COALESCE(CAST([pk2] as NVARCHAR(4000)), \'\') AS [pk2],CAST([col1] as NVARCHAR(4000)) AS [col1],COALESCE(CAST([col2] as NVARCHAR(4000)), \'\') AS [col2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_timestamp], ROW_NUMBER() OVER (PARTITION BY [pk1],[pk2] ORDER BY [pk1],[pk2]) AS "_row_number_" FROM [import-export-test-ng_schema].[#stagingTable]) AS a WHERE a."_row_number_" = 1',
        ];

        yield 'testGetCtasDedupCommandWithNotStringTypeNotNullable' => [
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                $stage->getTableName(),
                $stage->isTemporary(),
                new ColumnCollection([
                    $this->createGenericColumn('pk1'),
                    $this->createGenericColumn('pk2'),
                    new SynapseColumn(
                        'col1',
                        new Synapse(
                            Synapse::TYPE_INT
                        )
                    ),
                    $this->createGenericColumn('col2'),
                ]),
                $stage->getPrimaryKeysNames(),
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                new ColumnCollection([
                    $this->createGenericColumn('pk1'),
                    $this->createGenericColumn('pk2'),
                    $this->createGenericColumn('col1'),
                    $this->createGenericColumn('col2', false),
                ]),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                [],
                false,
                true,
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TEMP_TABLE_HEAP,
                SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
                SynapseImportOptions::TABLE_TYPES_CAST
            ),
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[pk1],a.[pk2],a.[col1],a.[col2],a.[_timestamp] FROM (SELECT COALESCE(CAST([pk1] as NVARCHAR(4000)), \'\') AS [pk1],COALESCE(CAST([pk2] as NVARCHAR(4000)), \'\') AS [pk2],CAST([col1] as NVARCHAR(4000)) AS [col1],ISNULL(CAST([col2] as NVARCHAR(4000)), \'\') AS [col2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_timestamp], ROW_NUMBER() OVER (PARTITION BY [pk1],[pk2] ORDER BY [pk1],[pk2]) AS "_row_number_" FROM [import-export-test-ng_schema].[#stagingTable]) AS a WHERE a."_row_number_" = 1',
        ];

        yield 'testGetCtasDedupCommandWithHashDistribution' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_HASH, ['pk1']),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                ['col1'],
                false,
                true,            // use timestamp
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TEMP_TABLE_HEAP,
                SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
                SynapseImportOptions::TABLE_TYPES_CAST // cast values
            ),
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=HASH([pk1]),HEAP) AS SELECT a.[pk1],a.[pk2],a.[col1],a.[col2],a.[_timestamp] FROM (SELECT COALESCE(CAST([pk1] as NVARCHAR(4000)), \'\') AS [pk1],COALESCE(CAST([pk2] as NVARCHAR(4000)), \'\') AS [pk2],CAST(NULLIF([col1], \'\') as NVARCHAR(4000)) AS [col1],COALESCE(CAST([col2] as NVARCHAR(4000)), \'\') AS [col2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_timestamp], ROW_NUMBER() OVER (PARTITION BY [pk1],[pk2] ORDER BY [pk1],[pk2]) AS "_row_number_" FROM [import-export-test-ng_schema].[#stagingTable]) AS a WHERE a."_row_number_" = 1',
        ];

        yield 'testGetCtasDedupCommandWithHashDistributionNoCasting' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_HASH, ['pk1']),
                $stage->getTableIndex()
            ),

            new SynapseImportOptions(
                ['col1'],
                false,
                true, // use timestamp
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TEMP_TABLE_HEAP,
                SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
                SynapseImportOptions::TABLE_TYPES_PRESERVE // dont cast values
            ),
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=HASH([pk1]),HEAP) AS SELECT a.[pk1],a.[pk2],a.[col1],a.[col2],a.[_timestamp] FROM (SELECT COALESCE([pk1], \'\') AS [pk1],COALESCE([pk2], \'\') AS [pk2],NULLIF([col1], \'\') AS [col1],COALESCE([col2], \'\') AS [col2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_timestamp], ROW_NUMBER() OVER (PARTITION BY [pk1],[pk2] ORDER BY [pk1],[pk2]) AS "_row_number_" FROM [import-export-test-ng_schema].[#stagingTable]) AS a WHERE a."_row_number_" = 1',
        ];

        yield 'testGetCtasDedupCommandWithTimestampNullConvert' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                ['col1'],
                false,
                true, // use timestamp
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TEMP_TABLE_HEAP,
                SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
                SynapseImportOptions::TABLE_TYPES_CAST // cast values
            ),
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[pk1],a.[pk2],a.[col1],a.[col2],a.[_timestamp] FROM (SELECT COALESCE(CAST([pk1] as NVARCHAR(4000)), \'\') AS [pk1],COALESCE(CAST([pk2] as NVARCHAR(4000)), \'\') AS [pk2],CAST(NULLIF([col1], \'\') as NVARCHAR(4000)) AS [col1],COALESCE(CAST([col2] as NVARCHAR(4000)), \'\') AS [col2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_timestamp], ROW_NUMBER() OVER (PARTITION BY [pk1],[pk2] ORDER BY [pk1],[pk2]) AS "_row_number_" FROM [import-export-test-ng_schema].[#stagingTable]) AS a WHERE a."_row_number_" = 1',
        ];

        yield 'testGetCtasDedupCommandWithTimestampNullConvertNoCasting' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                ['col1'],
                false,
                true, // use timestamp
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TEMP_TABLE_HEAP,
                SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
                SynapseImportOptions::TABLE_TYPES_PRESERVE // don't cast values
            ),
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[pk1],a.[pk2],a.[col1],a.[col2],a.[_timestamp] FROM (SELECT COALESCE([pk1], \'\') AS [pk1],COALESCE([pk2], \'\') AS [pk2],NULLIF([col1], \'\') AS [col1],COALESCE([col2], \'\') AS [col2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_timestamp], ROW_NUMBER() OVER (PARTITION BY [pk1],[pk2] ORDER BY [pk1],[pk2]) AS "_row_number_" FROM [import-export-test-ng_schema].[#stagingTable]) AS a WHERE a."_row_number_" = 1',
        ];

        yield 'testGetCtasDedupCommandNoTimestampNullConvert' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                ['col1'],
                false,
                false, // don't use timestamp
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TEMP_TABLE_HEAP,
                SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
                SynapseImportOptions::TABLE_TYPES_CAST
            ),
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[pk1],a.[pk2],a.[col1],a.[col2] FROM (SELECT COALESCE(CAST([pk1] as NVARCHAR(4000)), \'\') AS [pk1],COALESCE(CAST([pk2] as NVARCHAR(4000)), \'\') AS [pk2],CAST(NULLIF([col1], \'\') as NVARCHAR(4000)) AS [col1],COALESCE(CAST([col2] as NVARCHAR(4000)), \'\') AS [col2], ROW_NUMBER() OVER (PARTITION BY [pk1],[pk2] ORDER BY [pk1],[pk2]) AS "_row_number_" FROM [import-export-test-ng_schema].[#stagingTable]) AS a WHERE a."_row_number_" = 1',
            false,
        ];

        yield 'testGetCtasDedupCommandNoTimestampNullConvertNoCasting' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                $stage->getColumnsDefinitions(),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                ['col1'],
                false,
                false, // don't use timestamp
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TEMP_TABLE_HEAP,
                SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
                SynapseImportOptions::TABLE_TYPES_PRESERVE // don't cast
            ),
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[pk1],a.[pk2],a.[col1],a.[col2] FROM (SELECT COALESCE([pk1], \'\') AS [pk1],COALESCE([pk2], \'\') AS [pk2],NULLIF([col1], \'\') AS [col1],COALESCE([col2], \'\') AS [col2], ROW_NUMBER() OVER (PARTITION BY [pk1],[pk2] ORDER BY [pk1],[pk2]) AS "_row_number_" FROM [import-export-test-ng_schema].[#stagingTable]) AS a WHERE a."_row_number_" = 1',
            false,
        ];

        yield 'testGetCtasDedupCommandWithTimestampInSource' => [
            $stage,
            new SynapseTableDefinition(
                $stage->getSchemaName(),
                self::TEST_TABLE,
                false,
                // add _timestamp column to destination
                new ColumnCollection(array_merge(
                    iterator_to_array($stage->getColumnsDefinitions()),
                    [SynapseColumn::createGenericColumn('_timestamp')]
                )),
                ['pk1', 'pk2'],
                $stage->getTableDistribution(),
                $stage->getTableIndex()
            ),
            new SynapseImportOptions(
                [],
                false,
                true,
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TEMP_TABLE_HEAP,
                SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
                SynapseImportOptions::TABLE_TYPES_CAST
            ),
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,HEAP) AS SELECT a.[pk1],a.[pk2],a.[col1],a.[col2],a.[_timestamp] FROM (SELECT COALESCE(CAST([pk1] as NVARCHAR(4000)), \'\') AS [pk1],COALESCE(CAST([pk2] as NVARCHAR(4000)), \'\') AS [pk2],COALESCE(CAST([col1] as NVARCHAR(4000)), \'\') AS [col1],COALESCE(CAST([col2] as NVARCHAR(4000)), \'\') AS [col2],CAST(\'2020-01-01 00:00:00\' as DATETIME2) AS [_timestamp], ROW_NUMBER() OVER (PARTITION BY [pk1],[pk2] ORDER BY [pk1],[pk2]) AS "_row_number_" FROM [import-export-test-ng_schema].[#stagingTable]) AS a WHERE a."_row_number_" = 1',
        ];
    }

    private function getStagingTableDefinition(): SynapseTableDefinition
    {
        return new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('pk1'),
                $this->createGenericColumn('pk2'),
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
    }

    /**
     * @return \Generator<string, array{SynapseImportOptions::TABLE_TYPES_*, string}>
     */
    public function getCTASInsertAllIntoTargetTableCommandProvider(): \Generator
    {
        yield 'no type casting' => [
            SynapseImportOptions::TABLE_TYPES_PRESERVE,
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT COALESCE([col1], \'\') AS [col1],COALESCE([col2], \'\') AS [col2],ISNULL([pk1], \'\') AS [pk1] FROM [import-export-test-ng_schema].[#stagingTable]',
        ];
        yield 'type casting' => [
            SynapseImportOptions::TABLE_TYPES_CAST,
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT COALESCE(CAST([col1] as NVARCHAR(4000)), \'\') AS [col1],COALESCE(CAST([col2] as NVARCHAR(4000)), \'\') AS [col2],ISNULL(CAST([pk1] as NVARCHAR(4000)), \'\') AS [pk1] FROM [import-export-test-ng_schema].[#stagingTable]',
        ];
    }

    /**
     * @dataProvider getCTASInsertAllIntoTargetTableCommandProvider
     * @param SynapseImportOptions::TABLE_TYPES_* $cast
     */
    public function testGetCTASInsertAllIntoTargetTableCommand(string $cast, string $expectedSQL): void
    {
        $this->createTestSchema();
        $destination = $this->getTestTableWithColumnsDefinition(
            false,
            false,
            null,
            true
        );
        $this->createStagingTableWithData(true);

        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createGenericColumn('col2'),
                $this->createGenericColumn('pk1', false),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // no convert values no timestamp
        $importOptions = new SynapseImportOptions(
            [],
            false,
            false,
            0,
            SynapseImportOptions::CREDENTIALS_SAS,
            SynapseImportOptions::TEMP_TABLE_HEAP,
            SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
            $cast
        );
        $sql = $this->getBuilder()->getCTASInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $importOptions,
            '2020-01-01 00:00:00'
        );

        $this->assertEquals(
        // phpcs:ignore
            $expectedSQL,
            $sql
        );

        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
            [
                'col1' => '1',
                'col2' => '1',
                'pk1' => '1',
            ],
            [
                'col1' => '1',
                'col2' => '1',
                'pk1' => '1',
            ],
            [
                'col1' => '2',
                'col2' => '2',
                'pk1' => '2',
            ],
            [
                'col1' => '',
                'col2' => '',
                'pk1' => '2',
            ],
        ], $result);

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );
        $resultColumns = $ref->getColumnsDefinitions();
        $this->assertColumnsDefinitions($resultColumns, $destination, $importOptions);
    }

    /**
     * @return \Generator<string, array{SynapseImportOptions::TABLE_TYPES_*, string}>
     */
    public function getCTASInsertAllIntoTargetTableCommandNotStringProvider(): \Generator
    {
        yield 'no type casting' => [
            SynapseImportOptions::TABLE_TYPES_PRESERVE,
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT COALESCE([col1], \'\') AS [col1],[col2] AS [col2],ISNULL([pk1], \'\') AS [pk1] FROM [import-export-test-ng_schema].[#stagingTable]',
        ];
        yield 'type casting' => [
            SynapseImportOptions::TABLE_TYPES_CAST,
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT COALESCE(CAST([col1] as NVARCHAR(4000)), \'\') AS [col1],CAST([col2] as NUMERIC(18,0)) AS [col2],ISNULL(CAST([pk1] as NVARCHAR(4000)), \'\') AS [pk1] FROM [import-export-test-ng_schema].[#stagingTable]',
        ];
    }

    /**
     * @dataProvider getCTASInsertAllIntoTargetTableCommandNotStringProvider
     * @param SynapseImportOptions::TABLE_TYPES_* $cast
     */
    public function testGetCTASInsertAllIntoTargetTableCommandNotString(string $cast, string $expectedSQL): void
    {
        $col2 = new SynapseColumn(
            'col2',
            new Synapse(
                Synapse::TYPE_NUMERIC,
                [
                    'length' => '18,0',
                ]
            )
        );

        $this->createTestSchema();
        $destination = $this->getTestTableWithColumnsDefinition(
            false,
            false,
            $col2,
            true
        );
        $this->createStagingTableWithData(true);

        // create fake stage with missing id column and numeric col2
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $col2,
                $this->createGenericColumn('pk1', false),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // no convert values no timestamp
        $importOptions = new SynapseImportOptions(
            [],
            false,
            false,
            0,
            SynapseImportOptions::CREDENTIALS_SAS,
            SynapseImportOptions::TEMP_TABLE_HEAP,
            SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
            $cast
        );
        $sql = $this->getBuilder()->getCTASInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $importOptions,
            '2020-01-01 00:00:00'
        );

        $this->assertEquals(
        // phpcs:ignore
            $expectedSQL,
            $sql
        );

        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
            [
                'col1' => '1',
                'col2' => '1',
                'pk1' => '1',
            ],
            [
                'col1' => '1',
                'col2' => '1',
                'pk1' => '1',
            ],
            [
                'col1' => '2',
                'col2' => '2',
                'pk1' => '2',
            ],
            [
                'col1' => '',
                'col2' => null,
                'pk1' => '2',
            ],
        ], $result);

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );
        $resultColumns = $ref->getColumnsDefinitions();
        $this->assertColumnsDefinitions($resultColumns, $destination, $importOptions);
    }

    /**
     * @return \Generator<string, array{SynapseImportOptions::TABLE_TYPES_*, string}>
     */
    public function getCTASInsertAllIntoTargetTableCommandConvertToNullProvider(): \Generator
    {
        yield 'no type casting' => [
            SynapseImportOptions::TABLE_TYPES_PRESERVE,
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT NULLIF([col1], \'\') AS [col1],[col2] AS [col2],ISNULL([pk1], \'\') AS [pk1] FROM [import-export-test-ng_schema].[#stagingTable]',
        ];
        yield 'type casting' => [
            SynapseImportOptions::TABLE_TYPES_CAST,
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT CAST(NULLIF([col1], \'\') as NVARCHAR(4000)) AS [col1],CAST([col2] as NVARCHAR(4000)) AS [col2],ISNULL(CAST([pk1] as NVARCHAR(4000)), \'\') AS [pk1] FROM [import-export-test-ng_schema].[#stagingTable]',
        ];
    }

    /**
     * @dataProvider getCTASInsertAllIntoTargetTableCommandConvertToNullProvider
     * @param SynapseImportOptions::TABLE_TYPES_* $cast
     */
    public function testGetCTASInsertAllIntoTargetTableCommandConvertToNull(string $cast, string $expectedSQL): void
    {
        $this->createTestSchema();
        $this->createStagingTableWithData(true);
        $destination = $this->getTestTableWithColumnsDefinition(
            false,
            false,
            null,
            true
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createIntColumn('col2'),
                $this->createGenericColumn('pk1', false),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // convert col1 to null
        $options = new SynapseImportOptions(
            ['col1'],
            false,
            false,
            0,
            SynapseImportOptions::CREDENTIALS_SAS,
            SynapseImportOptions::TEMP_TABLE_HEAP,
            SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
            $cast
        );
        $sql = $this->getBuilder()->getCTASInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
            $expectedSQL,
            $sql
        );
        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        $this->assertEqualsCanonicalizing([
            [
                'col1' => '1',
                'col2' => '1',
                'pk1' => '1',
            ],
            [
                'col1' => '1',
                'col2' => '1',
                'pk1' => '1',
            ],
            [
                'col1' => '2',
                'col2' => '2',
                'pk1' => '2',
            ],
            [
                'col1' => null,
                'col2' => '',
                'pk1' => '2',
            ],
        ], $result);

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );
        $resultColumns = $ref->getColumnsDefinitions();
        $this->assertColumnsDefinitions($resultColumns, $destination, $options);
    }

    /**
     * @return \Generator<string, array{SynapseImportOptions::TABLE_TYPES_*, string}>
     */
    public function getCTASInsertAllIntoTargetTableCommandConvertToNullWithTimestampProvider(): \Generator
    {
        yield 'no type casting' => [
            SynapseImportOptions::TABLE_TYPES_PRESERVE,
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT NULLIF([col1], \'\') AS [col1],[col2] AS [col2],ISNULL([pk1], \'\') AS [pk1],\'2020-01-01 00:00:00\' AS _timestamp FROM [import-export-test-ng_schema].[#stagingTable]',
        ];
        yield 'type casting' => [
            SynapseImportOptions::TABLE_TYPES_CAST,
            // phpcs:ignore
            'CREATE TABLE [import-export-test-ng_schema].[import-export-test-ng_test] WITH (DISTRIBUTION=ROUND_ROBIN,CLUSTERED COLUMNSTORE INDEX) AS SELECT CAST(NULLIF([col1], \'\') as NVARCHAR(4000)) AS [col1],CAST([col2] as NVARCHAR(4000)) AS [col2],ISNULL(CAST([pk1] as NVARCHAR(4000)), \'\') AS [pk1],\'2020-01-01 00:00:00\' AS _timestamp FROM [import-export-test-ng_schema].[#stagingTable]',
        ];
    }

    /**
     * @dataProvider getCTASInsertAllIntoTargetTableCommandConvertToNullWithTimestampProvider
     * @param SynapseImportOptions::TABLE_TYPES_* $cast
     */
    public function testGetCTASInsertAllIntoTargetTableCommandConvertToNullWithTimestamp(
        string $cast,
        string $expectedSQL
    ): void {
        $this->createTestSchema();
        $destination = $this->getTestTableWithColumnsDefinition(
            true,
            false,
            null,
            true
        );
        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createGenericColumn('col1'),
                $this->createIntColumn('col2'),
                $this->createGenericColumn('pk1', false),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );

        // use timestamp
        $options = new SynapseImportOptions(
            ['col1'],
            false,
            true,
            0,
            SynapseImportOptions::CREDENTIALS_SAS,
            SynapseImportOptions::TEMP_TABLE_HEAP,
            SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
            $cast
        );
        $sql = $this->getBuilder()->getCTASInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
            $expectedSQL,
            $sql
        );
        $out = $this->connection->exec($sql);
        $this->assertEquals(4, $out);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA
        ));

        foreach ($result as $item) {
            $this->assertArrayHasKey('col1', $item);
            $this->assertArrayHasKey('col2', $item);
            $this->assertArrayHasKey('_timestamp', $item);
        }

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );
        $resultColumns = $ref->getColumnsDefinitions();
        $this->assertColumnsDefinitions($resultColumns, $destination, $options);
    }

    private function createIntColumn(string $columnName): SynapseColumn
    {
        $definition = new Synapse(
            Synapse::TYPE_INT,
            [
                'nullable' => false,
            ]
        );

        return new SynapseColumn(
            $columnName,
            $definition
        );
    }

    protected function getTestTableWithColumnsDefinition(
        bool $includeTimestamp = false,
        bool $includePrimaryKey = false,
        ?SynapseColumn $overwriteColumn2 = null,
        bool $includeNotNullableColumn = false
    ): SynapseTableDefinition {
        $columns = [];
        $pks = [];
        if ($includePrimaryKey) {
            $pks[] = 'id';
            $columns[] = new SynapseColumn(
                'id',
                new Synapse(Synapse::TYPE_INT)
            );
        } else {
            $columns[] = $this->createGenericColumn('id');
        }
        $columns[] = $this->createGenericColumn('col1');
        if ($overwriteColumn2 === null) {
            $columns[] = $this->createGenericColumn('col2');
        } else {
            $columns[] = $overwriteColumn2;
        }

        if ($includeTimestamp) {
            $columns[] = new SynapseColumn(
                '_timestamp',
                new Synapse(Synapse::TYPE_DATETIME)
            );
        }

        if ($includeNotNullableColumn) {
            $columns[] = $this->createGenericColumn('pk1', false);
        }

        $tableDefinition = new SynapseTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection($columns),
            $pks,
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX)
        );
        return $tableDefinition;
    }

    private function assertColumnsDefinitions(
        ColumnCollection $resultColumns,
        SynapseTableDefinition $destination,
        SynapseImportOptions $options
    ): void {
        /** @var SynapseColumn $resultCol */
        foreach ($resultColumns as $resultCol) {
            if ($resultCol->getColumnName() === ToStageImporterInterface::TIMESTAMP_COLUMN_NAME) {
                continue;
            }
            $expectedCol = null;
            /** @var SynapseColumn $col */
            foreach ($destination->getColumnsDefinitions() as $col) {
                if ($col->getColumnName() === $resultCol->getColumnName()) {
                    $expectedCol = $col;
                    break;
                }
            }
            $this->assertNotNull($expectedCol);
            if ($options->getCastValueTypes()) {
                $this->assertSame(
                    $expectedCol->getColumnDefinition()->getSQLDefinition(),
                    $resultCol->getColumnDefinition()->getSQLDefinition(),
                    sprintf('Column "%s" definition not match.', $resultCol->getColumnName())
                );
            } else {
                $this->assertSame(
                    $expectedCol->getColumnDefinition()->isNullable(),
                    $resultCol->getColumnDefinition()->isNullable(),
                    sprintf('Column "%s" nullability not match.', $resultCol->getColumnName())
                );
            }
        }
    }
}
