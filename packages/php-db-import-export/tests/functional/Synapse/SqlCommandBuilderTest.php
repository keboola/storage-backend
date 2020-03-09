<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use DateTime;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\Synapse\Table;

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

    public function testGetCreateStagingTableCommand(): void
    {
        $this->createTestSchema();
        $sql = $this->qb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            '#' . self::TEST_TABLE,
            [
                'col1',
                'col2',
            ]
        );

        $this->connection->exec($sql);
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
            ]
        );
        $this->connection->exec($sql);

        $sql = $this->qb->getDedupCommand(
            $this->getDummyTableDestination(),
            $this->getDummyImportOptions(),
            [
                'pk1',
                'pk2',
            ],
            self::TEST_STAGING_TABLE,
            '#tempTable'
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
            ]
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

    private function getDummyTableDestination(): Table
    {
        return new Table(self::TEST_SCHEMA, self::TEST_TABLE);
    }

    private function getDummyImportOptions(): ImportOptions
    {
        return new ImportOptions([], ['col1', 'col2']);
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
            ]
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
        $this->connection->exec($sql);

        $result = $this->connection->fetchAll(sprintf(
            'SELECT * FROM %s.[%s]',
            self::TEST_SCHEMA_QUOTED,
            self::TEST_STAGING_TABLE
        ));

        $this->assertCount(1, $result);
        $this->assertSame([[
            'pk1'=> '2',
            'pk2'=> '1',
            'col1'=> '1',
            'col2'=> '1',
        ]], $result);
    }

    public function testGetDropCommand(): void
    {
        $this->createTestSchema();
        $this->createTestTable();
        $sql = $this->qb->getDropCommand(self::TEST_SCHEMA, self::TEST_TABLE);
        $this->connection->exec($sql);

        $tableId = $this->connection->fetchColumn(
            $this->qb->getTableObjectIdCommand(self::TEST_SCHEMA, self::TEST_TABLE)
        );
        $this->assertFalse($tableId);
    }

    public function testGetInsertAllIntoTargetTableCommand(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

        // no convert values no timestamp
        $sql = $this->qb->getInsertAllIntoTargetTableCommand(
            $this->getDummyTableDestination(),
            $this->getDummyImportOptions(),
            self::TEST_STAGING_TABLE
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

    public function testGetInsertAllIntoTargetTableCommandConvertToNull(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

        // convert col1 to null
        $options = new ImportOptions(['col1'], [
            'col1',
            'col2',
        ]);
        $sql = $this->qb->getInsertAllIntoTargetTableCommand(
            $this->getDummyTableDestination(),
            $options,
            self::TEST_STAGING_TABLE
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
        $options = new ImportOptions(['col1'], [
            'col1',
            'col2',
        ], false, true);
        $sql = $this->qb->getInsertAllIntoTargetTableCommand(
            $this->getDummyTableDestination(),
            $options,
            self::TEST_STAGING_TABLE
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

    public function testGetTableColumns(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();

        $response = $this->qb->getTableColumns(self::TEST_SCHEMA, self::TEST_TABLE);

        $this->assertCount(3, $response);
        $this->assertEqualsCanonicalizing(['id', 'col1', 'col2'], $response);
    }

    public function testGetTableColumnsCommand(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();

        /** @var string $tableId */
        $tableId = $this->connection->fetchColumn(
            $this->qb->getTableObjectIdCommand(self::TEST_SCHEMA, self::TEST_TABLE)
        );

        $response = $this->connection->fetchAll(
            $this->qb->getTableColumnsCommand($tableId)
        );

        $this->assertCount(3, $response);
        $this->assertEqualsCanonicalizing(
            [
                ['name' => 'id'],
                ['name' => 'col1'],
                ['name' => 'col2'],
            ],
            $response
        );
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

    public function testGetTableItemsCountCommand(): void
    {
        $this->createTestSchema();
        $this->createStagingTableWithData();
        $sql = $this->qb->getTableItemsCountCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
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

        $response = $this->connection->fetchAll(
            $this->qb->getTableObjectIdCommand(self::TEST_SCHEMA, self::TEST_TABLE)
        );
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
            $this->getDummyTableDestination(),
            $this->getDummyImportOptions(),
            self::TEST_STAGING_TABLE,
            ['col1']
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

        $options = new ImportOptions(['col1'], [
            'col1',
            'col2',
        ]);

        // converver values
        $sql = $this->qb->getUpdateWithPkCommand(
            $this->getDummyTableDestination(),
            $options,
            self::TEST_STAGING_TABLE,
            ['col1']
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
        $timestampNow = new DateTime('now');
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
        $options = new ImportOptions(['col1'], [
            'col1',
            'col2',
        ], false, true);
        $sql = $this->qb->getUpdateWithPkCommand(
            $this->getDummyTableDestination(),
            $options,
            self::TEST_STAGING_TABLE,
            ['col1']
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
            $this->assertTrue($timestampNow < new DateTime($item['_timestamp']));
        }
    }

    public function testTransaction(): void
    {
        $this->createTestSchema();
        $this->createTestTable();

        $this->connection->exec(
            $this->qb->getBeginTransaction()
        );

        $this->insertDataToTestTable();

        $this->connection->exec(
            $this->qb->getCommitTransaction()
        );
    }

    protected function createTestSchema(): void
    {
        $this->connection->exec(sprintf('CREATE SCHEMA %s', self::TEST_SCHEMA_QUOTED));
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

    protected function insertDataToTestTable(): void
    {
        $this->connection->exec(
            sprintf(
                'INSERT INTO %s([id]) VALUES (1)',
                self::TEST_TABLE_IN_SCHEMA
            )
        );
    }
}
