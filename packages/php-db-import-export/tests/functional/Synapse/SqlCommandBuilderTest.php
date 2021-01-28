<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use DateTime;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\Synapse\Table;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;

class SqlCommandBuilderTest extends SynapseBaseTestCase
{
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

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema(self::TEST_SCHEMA);
    }

    public function testGetCreateStagingTableCommandHEAP(): void
    {
        $this->createTestSchema();
        $sql = $this->qb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            '#' . self::TEST_TABLE,
            [
                'col1',
                'col2',
            ],
            new SynapseImportOptions()
        );

        $this->assertEquals(
        // phpcs:ignore
            'CREATE TABLE [import-export-test_schema].[#import-export-test_test] ([col1] nvarchar(max), [col2] nvarchar(max)) WITH (HEAP, LOCATION = USER_DB)',
            $sql
        );
        $this->connection->exec($sql);
    }

    public function testGetCreateStagingTableCommandCOLUMNSTORE(): void
    {
        $this->createTestSchema();
        $sql = $this->qb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            '#' . self::TEST_TABLE,
            [
                'col1',
                'col2',
            ],
            new SynapseImportOptions(
                [],
                false,
                false,
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TEMP_TABLE_COLUMNSTORE
            )
        );

        $this->assertEquals(
        // phpcs:ignore
            'CREATE TABLE [import-export-test_schema].[#import-export-test_test] ([col1] nvarchar(4000), [col2] nvarchar(4000)) WITH (CLUSTERED COLUMNSTORE INDEX)',
            $sql
        );
        $this->connection->exec($sql);
    }

    public function testGetCreateStagingTableCommandCLUSTEREDINDEX(): void
    {
        $this->createTestSchema();
        $sql = $this->qb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            '#' . self::TEST_TABLE,
            [
                'col1',
                'col2',
            ],
            new SynapseImportOptions(
                [],
                false,
                false,
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TEMP_TABLE_CLUSTERED_INDEX
            )
        );

        $this->assertEquals(
        // phpcs:ignore
            'CREATE TABLE [import-export-test_schema].[#import-export-test_test] ([col1] nvarchar(4000), [col2] nvarchar(4000)) WITH (CLUSTERED INDEX([col1], [col2]))',
            $sql
        );
        $this->connection->exec($sql);
    }

    public function testGetCreateStagingTableCommandHEAP4000(): void
    {
        $this->createTestSchema();
        $sql = $this->qb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            '#' . self::TEST_TABLE,
            [
                'col1',
                'col2',
            ],
            new SynapseImportOptions(
                [],
                false,
                false,
                0,
                SynapseImportOptions::CREDENTIALS_SAS,
                SynapseImportOptions::TEMP_TABLE_HEAP_4000
            )
        );

        $this->assertEquals(
        // phpcs:ignore
            'CREATE TABLE [import-export-test_schema].[#import-export-test_test] ([col1] nvarchar(4000), [col2] nvarchar(4000)) WITH (HEAP, LOCATION = USER_DB)',
            $sql
        );
        $this->connection->exec($sql);
    }

    protected function createTestSchema(): void
    {
        $this->connection->exec(sprintf('CREATE SCHEMA %s', self::TEST_SCHEMA_QUOTED));
    }

    public function testGetDedupCommand(): void
    {
        $this->createTestSchema();
        $this->createStagingTableWithData();

        $sql = $this->qb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            '#tempTable',
            [
                'pk1',
                'pk2',
                'col1',
                'col2',
            ],
            new SynapseImportOptions()
        );
        $this->connection->exec($sql);

        $sql = $this->qb->getDedupCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            [
                'pk1',
                'pk2',
            ],
            self::TEST_STAGING_TABLE,
            '#tempTable'
        );
        $this->assertEquals(
        // phpcs:ignore
            'INSERT INTO [import-export-test_schema].[#tempTable] ([col1], [col2]) SELECT a.[col1],a.[col2] FROM (SELECT [col1], [col2], ROW_NUMBER() OVER (PARTITION BY [pk1],[pk2] ORDER BY [pk1],[pk2]) AS "_row_number_" FROM [import-export-test_schema].[#stagingTable]) AS a WHERE a."_row_number_" = 1',
            $sql
        );
        $this->connection->exec($sql);
        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s.[%s]',
            self::TEST_SCHEMA_QUOTED,
            '#tempTable'
        ));

        $this->assertCount(2, $result);
    }

    private function createStagingTableWithData(bool $includeEmptyValues = false): void
    {
        $this->connection->exec($this->qb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            [
                'pk1',
                'pk2',
                'col1',
                'col2',
            ],
            new SynapseImportOptions()
        ));
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
    }

    private function getDummySource(): SourceInterface
    {
        return new class implements SourceInterface {
            public function getColumnsNames(): array
            {
                return ['col1', 'col2'];
            }
        };
    }

    private function getDummyTableDestination(): Table
    {
        return new Table(self::TEST_SCHEMA, self::TEST_TABLE);
    }

    private function getDummyImportOptions(): SynapseImportOptions
    {
        return new SynapseImportOptions([]);
    }

    public function testGetDeleteOldItemsCommand(): void
    {
        $this->createTestSchema();
        $table = self::TEST_TABLE_IN_SCHEMA;
        $this->connection->exec(<<<EOT
CREATE TABLE $table (
    [id] INT PRIMARY KEY NONCLUSTERED NOT ENFORCED,
    [pk1] nvarchar(4000),
    [pk2] nvarchar(4000),
    [col1] nvarchar(4000),
    [col2] nvarchar(4000)
)
WITH
    (
      PARTITION ( id RANGE LEFT FOR VALUES ( )),
      CLUSTERED COLUMNSTORE INDEX
    )
EOT
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id],[pk1],[pk2],[col1],[col2]) VALUES (1,1,1,\'1\',\'1\')',
                $table
            )
        );

        $this->connection->exec($this->qb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            [
                'pk1',
                'pk2',
                'col1',
                'col2',
            ],
            new SynapseImportOptions()
        ));
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s.%s([pk1],[pk2],[col1],[col2]) VALUES (1,1,\'1\',\'1\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE
            )
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s.%s([pk1],[pk2],[col1],[col2]) VALUES (2,1,\'1\',\'1\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE
            )
        );

        $sql = $this->qb->getDeleteOldItemsCommand(
            $this->getDummyTableDestination(),
            self::TEST_STAGING_TABLE,
            [
                'pk1',
                'pk2',
            ]
        );

        $this->assertEquals(
        // phpcs:ignore
            'DELETE [import-export-test_schema].[#stagingTable] WHERE EXISTS (SELECT * FROM [import-export-test_schema].[import-export-test_test] WHERE [import-export-test_schema].[import-export-test_test].[pk1] = COALESCE([import-export-test_schema].[#stagingTable].[pk1], \'\') AND [import-export-test_schema].[import-export-test_test].[pk2] = COALESCE([import-export-test_schema].[#stagingTable].[pk2], \'\'))',
            $sql
        );
        $this->connection->exec($sql);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s.[%s]',
            self::TEST_SCHEMA_QUOTED,
            self::TEST_STAGING_TABLE
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
        $sql = $this->qb->getDropCommand(self::TEST_SCHEMA, self::TEST_TABLE);

        $this->assertEquals(
            'DROP TABLE [import-export-test_schema].[import-export-test_test]',
            $sql
        );

        $this->connection->exec($sql);

        $tableId = $this->connection->fetchColumn(
            $this->qb->getTableObjectIdCommand(self::TEST_SCHEMA, self::TEST_TABLE)
        );
        $this->assertFalse($tableId);
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
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

        // no convert values no timestamp
        $sql = $this->qb->getInsertAllIntoTargetTableCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $this->getDummyImportOptions(),
            self::TEST_STAGING_TABLE,
            '2020-01-01 00:00:00'
        );

        $this->assertEquals(
        // phpcs:ignore
            'INSERT INTO [import-export-test_schema].[import-export-test_test] ([col1], [col2]) (SELECT CAST(COALESCE([col1], \'\') as nvarchar(4000)) AS [col1],CAST(COALESCE([col2], \'\') as nvarchar(4000)) AS [col2] FROM [import-export-test_schema].[#stagingTable] AS [src])',
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

    protected function createTestTableWithColumns(bool $includeTimestamp = false, bool $includePrimaryKey = false): void
    {
        $table = self::TEST_TABLE_IN_SCHEMA;
        $timestampDeclaration = '';
        if ($includeTimestamp) {
            $timestampDeclaration = ',_timestamp datetime';
        }
        $idDeclaration = 'id varchar';
        if ($includePrimaryKey) {
            $idDeclaration = 'id INT PRIMARY KEY NONCLUSTERED NOT ENFORCED';
        }

        $this->connection->exec(<<<EOT
CREATE TABLE $table (
    $idDeclaration,
    col1 varchar,
    col2 varchar
    $timestampDeclaration
)
WITH
    (
      PARTITION ( id RANGE LEFT FOR VALUES ( )),
      CLUSTERED COLUMNSTORE INDEX
    )
EOT
        );
    }

    public function testGetInsertAllIntoTargetTableCommandConvertToNull(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

        // convert col1 to null
        $options = new SynapseImportOptions(['col1']);
        $sql = $this->qb->getInsertAllIntoTargetTableCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $options,
            self::TEST_STAGING_TABLE,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
        // phpcs:ignore
            'INSERT INTO [import-export-test_schema].[import-export-test_test] ([col1], [col2]) (SELECT NULLIF([col1], \'\'),CAST(COALESCE([col2], \'\') as nvarchar(4000)) AS [col2] FROM [import-export-test_schema].[#stagingTable] AS [src])',
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
        $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);

        // use timestamp
        $options = new SynapseImportOptions(['col1'], false, true);
        $sql = $this->qb->getInsertAllIntoTargetTableCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $options,
            self::TEST_STAGING_TABLE,
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
        // phpcs:ignore
            'INSERT INTO [import-export-test_schema].[import-export-test_test] ([col1], [col2], [_timestamp]) (SELECT NULLIF([col1], \'\'),CAST(COALESCE([col2], \'\') as nvarchar(4000)) AS [col2],\'2020-01-01 00:00:00\' FROM [import-export-test_schema].[#stagingTable] AS [src])',
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
        $sql = $this->qb->getRenameTableCommand(self::TEST_SCHEMA, self::TEST_TABLE, $renameTo);

        $this->assertEquals(
            'RENAME OBJECT [import-export-test_schema].[import-export-test_test] TO [newTable]',
            $sql
        );

        $this->connection->exec($sql);

        $expectedFalse = $this->connection->fetchColumn(
            $this->qb->getTableObjectIdCommand(self::TEST_SCHEMA, self::TEST_TABLE)
        );

        $this->assertFalse($expectedFalse);

        $expectedFalse = $this->connection->fetchColumn(
            $this->qb->getTableObjectIdCommand(self::TEST_SCHEMA, $renameTo)
        );

        $this->assertNotFalse($expectedFalse);
    }

    public function testGetTableItemsCountCommand(): void
    {
        $this->createTestSchema();
        $this->createStagingTableWithData();
        $sql = $this->qb->getTableItemsCountCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $this->assertEquals(
            'SELECT COUNT(*) AS [count] FROM [import-export-test_schema].[#stagingTable]',
            $sql
        );
        $response = $this->connection->fetchColumn($sql);

        $this->assertEquals(3, (int) $response);

        $response = $this->connection->fetchAll($sql);

        $this->assertSame([
            [
                'count' => '3',
            ],
        ], $response);
    }

    public function testGetTableObjectIdCommand(): void
    {
        $this->createTestSchema();
        $this->createTestTable();

        $sql = $this->qb->getTableObjectIdCommand(self::TEST_SCHEMA, self::TEST_TABLE);
        $this->assertEquals(
        // phpcs:ignore
            'SELECT [object_id] FROM sys.tables WHERE schema_name(schema_id) = \'import-export-test_schema\' AND NAME = \'import-export-test_test\'',
            $sql
        );
        $response = $this->connection->fetchAll($sql);
        $this->assertCount(1, $response);
        $this->assertArrayHasKey('object_id', $response[0]);

        $response = $this->connection->fetchColumn(
            $this->qb->getTableObjectIdCommand(self::TEST_SCHEMA, self::TEST_TABLE)
        );

        $this->assertIsString($response);

        // non existing table
        $response = $this->connection->fetchColumn(
            $this->qb->getTableObjectIdCommand(self::TEST_SCHEMA, 'I do not exists')
        );

        $this->assertFalse($response);
    }

    public function testGetTablePrimaryKey(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns(false, true);

        $response = $this->qb->getTablePrimaryKey(self::TEST_SCHEMA, self::TEST_TABLE);

        $this->assertCount(1, $response);
        $this->assertEquals('id', $response[0]);
    }

    public function testGetTruncateTableCommand(): void
    {
        $this->createTestSchema();
        $this->createStagingTableWithData();

        $sql = $this->qb->getTableItemsCountCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $response = $this->connection->fetchColumn($sql);
        $this->assertEquals(3, (int) $response);

        $sql = $this->qb->getTruncateTableCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $this->assertEquals(
            'TRUNCATE TABLE [import-export-test_schema].[#stagingTable]',
            $sql
        );
        $this->connection->exec($sql);

        $sql = $this->qb->getTableItemsCountCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $response = $this->connection->fetchColumn($sql);
        $this->assertEquals(0, (int) $response);
    }

    public function testGetTruncateTableWithDeleteCommand(): void
    {
        $this->createTestSchema();
        $this->createStagingTableWithData();

        $sql = $this->qb->getTableItemsCountCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $response = $this->connection->fetchColumn($sql);
        $this->assertEquals(3, (int) $response);

        $sql = $this->qb->getTruncateTableWithDeleteCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $this->assertEquals(
            'DELETE FROM [import-export-test_schema].[#stagingTable]',
            $sql
        );
        $this->connection->exec($sql);

        $sql = $this->qb->getTableItemsCountCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $response = $this->connection->fetchColumn($sql);
        $this->assertEquals(0, (int) $response);
    }

    public function testGetUpdateWithPkCommand(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

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
        $sql = $this->qb->getUpdateWithPkCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $this->getDummyImportOptions(),
            self::TEST_STAGING_TABLE,
            ['col1'],
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
        // phpcs:ignore
            'UPDATE [import-export-test_schema].[import-export-test_test] SET [col1] = COALESCE([src].[col1], \'\'), [col2] = COALESCE([src].[col2], \'\') FROM [import-export-test_schema].[#stagingTable] AS [src] WHERE [import-export-test_schema].[import-export-test_test].[col1] = COALESCE([src].[col1], \'\') AND (COALESCE(CAST([import-export-test_schema].[import-export-test_test].[col1] AS varchar), \'\') != COALESCE([src].[col1], \'\') OR COALESCE(CAST([import-export-test_schema].[import-export-test_test].[col2] AS varchar), \'\') != COALESCE([src].[col2], \'\')) ',
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
        $sql = $this->qb->getUpdateWithPkCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $options,
            self::TEST_STAGING_TABLE,
            ['col1'],
            '2020-01-01 00:00:00'
        );
        $this->assertEquals(
        // phpcs:ignore
            'UPDATE [import-export-test_schema].[import-export-test_test] SET [col1] = NULLIF([src].[col1], \'\'), [col2] = COALESCE([src].[col2], \'\') FROM [import-export-test_schema].[#stagingTable] AS [src] WHERE [import-export-test_schema].[import-export-test_test].[col1] = COALESCE([src].[col1], \'\') AND (COALESCE(CAST([import-export-test_schema].[import-export-test_test].[col1] AS varchar), \'\') != COALESCE([src].[col1], \'\') OR COALESCE(CAST([import-export-test_schema].[import-export-test_test].[col2] AS varchar), \'\') != COALESCE([src].[col2], \'\')) ',
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
        $sql = $this->qb->getUpdateWithPkCommand(
            $this->getDummySource(),
            $this->getDummyTableDestination(),
            $options,
            self::TEST_STAGING_TABLE,
            ['col1'],
            $timestampSet->format(DateTimeHelper::FORMAT) . '.000'
        );

        $this->assertEquals(
        // phpcs:ignore
            'UPDATE [import-export-test_schema].[import-export-test_test] SET [col1] = NULLIF([src].[col1], \'\'), [col2] = COALESCE([src].[col2], \'\'), [_timestamp] = \'2020-01-01 01:01:01.000\' FROM [import-export-test_schema].[#stagingTable] AS [src] WHERE [import-export-test_schema].[import-export-test_test].[col1] = COALESCE([src].[col1], \'\') AND (COALESCE(CAST([import-export-test_schema].[import-export-test_test].[col1] AS varchar), \'\') != COALESCE([src].[col1], \'\') OR COALESCE(CAST([import-export-test_schema].[import-export-test_test].[col2] AS varchar), \'\') != COALESCE([src].[col2], \'\')) ',
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

    /**
     * hpcs:disable SlevomatCodingStandard.TypeHints.TypeHintDeclaration
     * @doesNotPerformAssertions
     */
    public function testTransaction(): void
    {
        $this->createTestSchema();
        $this->createTestTable();

        $this->connection->exec(
            $this->qb->getBeginTransaction()
        );

        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id]) VALUES (1)',
                self::TEST_TABLE_IN_SCHEMA
            )
        );

        $this->connection->exec(
            $this->qb->getCommitTransaction()
        );
    }
}
