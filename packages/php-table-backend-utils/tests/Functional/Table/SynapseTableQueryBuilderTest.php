<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Table;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Driver\PDOException;
use Keboola\Datatype\Definition\Synapse;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\ReflectionException;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Tests\Keboola\TableBackendUtils\Functional\SynapseBaseCase;

/**
 * @covers SynapseTableQueryBuilder
 */
class SynapseTableQueryBuilderTest extends SynapseBaseCase
{
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'qb-schema';
    public const TEST_SCHEMA_2 = self::TESTS_PREFIX . 'qb-schema2';
    public const TEST_STAGING_TABLE = '#stagingTable';
    public const TEST_STAGING_TABLE_2 = '#stagingTable2';
    public const TEST_TABLE = self::TESTS_PREFIX . 'test';
    public const TEST_TABLE_2 = self::TESTS_PREFIX . 'test2';

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema(self::TEST_SCHEMA);
        $this->dropAllWithinSchema(self::TEST_SCHEMA_2);
    }

    public function testGetCreateTempTableCommand(): void
    {
        $this->createTestSchema();
        $qb = new SynapseTableQueryBuilder($this->connection);
        $sql = $qb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            '#' . self::TEST_TABLE,
            new ColumnCollection([
                SynapseColumn::createGenericColumn('col1'),
                SynapseColumn::createGenericColumn('col2'),
            ])
        );

        $this->assertEquals(
        // phpcs:ignore
            'CREATE TABLE [utils-test_qb-schema].[#utils-test_test] ([col1] nvarchar(4000) NOT NULL DEFAULT \'\', [col2] nvarchar(4000) NOT NULL DEFAULT \'\') WITH (HEAP, LOCATION = USER_DB)',
            $sql
        );
        $this->connection->exec($sql);
        // try to create same table
        $this->expectException(DBALException::class);
        $this->connection->exec($sql);
    }

    private function createTestSchema(): void
    {
        $this->connection->exec($this->schemaQb->getCreateSchemaCommand(self::TEST_SCHEMA));
        $this->connection->exec($this->schemaQb->getCreateSchemaCommand(self::TEST_SCHEMA_2));
    }

    public function testGetCreateTableCommand(): void
    {
        $this->createTestSchema();
        $cols = [
            SynapseColumn::createGenericColumn('col1'),
            SynapseColumn::createGenericColumn('col2'),
        ];
        $qb = new SynapseTableQueryBuilder($this->connection);
        $sql = $qb->getCreateTableCommand(self::TEST_SCHEMA, self::TEST_TABLE, new ColumnCollection($cols));
        $this->assertEquals(
        // phpcs:ignore
            'CREATE TABLE [utils-test_qb-schema].[utils-test_test] ([col1] nvarchar(4000) NOT NULL DEFAULT \'\', [col2] nvarchar(4000) NOT NULL DEFAULT \'\')',
            $sql
        );
        $this->connection->exec($sql);
        $ref = $this->getSynapseTableReflection();
        $this->assertNotNull($ref->getObjectId());
        $this->assertEqualsCanonicalizing(['col1', 'col2'], $ref->getColumnsNames());

        $this->expectException(DBALException::class);
        $this->connection->exec($sql);
    }

    private function getSynapseTableReflection(
        string $schema = self::TEST_SCHEMA,
        string $table = self::TEST_TABLE
    ): SynapseTableReflection {
        return new SynapseTableReflection($this->connection, $schema, $table);
    }

    public function testGetCreateTableCommandWithTimestamp(): void
    {
        $this->createTestSchema();
        $cols = [
            SynapseColumn::createGenericColumn('col1'),
            SynapseColumn::createGenericColumn('col2'),
            new SynapseColumn('_timestamp', new Synapse(Synapse::TYPE_DATETIME2)),
        ];
        $qb = new SynapseTableQueryBuilder($this->connection);
        $sql = $qb->getCreateTableCommand(self::TEST_SCHEMA, self::TEST_TABLE, new ColumnCollection($cols));
        $this->assertEquals(
        // phpcs:ignore
            'CREATE TABLE [utils-test_qb-schema].[utils-test_test] ([col1] nvarchar(4000) NOT NULL DEFAULT \'\', [col2] nvarchar(4000) NOT NULL DEFAULT \'\', [_timestamp] datetime2)',
            $sql
        );
        $this->connection->exec($sql);
        $ref = $this->getSynapseTableReflection();
        $this->assertNotNull($ref->getObjectId());
        $this->assertEqualsCanonicalizing(['col1', 'col2', '_timestamp'], $ref->getColumnsNames());
    }

    public function testGetCreateTableCommandWithTimestampAndPrimaryKeys(): void
    {
        $this->createTestSchema();
        $cols = [
            new SynapseColumn('pk1', new Synapse(Synapse::TYPE_INT)),
            SynapseColumn::createGenericColumn('col1'),
            SynapseColumn::createGenericColumn('col2'),
            new SynapseColumn('_timestamp', new Synapse(Synapse::TYPE_DATETIME2)),
        ];
        $qb = new SynapseTableQueryBuilder($this->connection);
        $sql = $qb->getCreateTableCommand(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            new ColumnCollection($cols),
            ['pk1', 'col1']
        );
        $this->assertEquals(
        // phpcs:ignore
            'CREATE TABLE [utils-test_qb-schema].[utils-test_test] ([pk1] int, [col1] nvarchar(4000) NOT NULL DEFAULT \'\', [col2] nvarchar(4000) NOT NULL DEFAULT \'\', [_timestamp] datetime2, PRIMARY KEY NONCLUSTERED([pk1],[col1]) NOT ENFORCED)',
            $sql
        );
        $this->connection->exec($sql);
        $ref = $this->getSynapseTableReflection();
        $this->assertNotNull($ref->getObjectId());
        $this->assertEqualsCanonicalizing(['pk1', 'col1', 'col2', '_timestamp'], $ref->getColumnsNames());
    }

    public function testGetDropCommand(): void
    {
        $this->createTestSchema();
        $this->createTestTable();
        $qb = new SynapseTableQueryBuilder($this->connection);
        $sql = $qb->getDropTableCommand(self::TEST_SCHEMA, self::TEST_TABLE);

        $this->assertEquals(
            'DROP TABLE [utils-test_qb-schema].[utils-test_test]',
            $sql
        );

        $this->connection->exec($sql);

        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, self::TEST_TABLE_2);
        $this->assertNotNull($ref->getObjectId());

        $ref = $this->getSynapseTableReflection();
        $this->expectException(ReflectionException::class);
        $ref->getObjectId();
    }

    private function createTestTable(): void
    {
        $table = [
            sprintf('[%s].[%s]', self::TEST_SCHEMA, self::TEST_TABLE),
            sprintf('[%s].[%s]', self::TEST_SCHEMA, self::TEST_TABLE_2),
        ];
        foreach ($table as $t) {
            $this->connection->exec(<<<EOT
CREATE TABLE $t (
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
    }

    public function testGetRenameTableCommand(): void
    {
        $renameTo = 'newTable';
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $qb = new SynapseTableQueryBuilder($this->connection);
        $sql = $qb->getRenameTableCommand(self::TEST_SCHEMA, self::TEST_TABLE, $renameTo);

        $this->assertEquals(
            'RENAME OBJECT [utils-test_qb-schema].[utils-test_test] TO [newTable]',
            $sql
        );

        $this->connection->exec($sql);

        $ref = $this->getSynapseTableReflection(self::TEST_SCHEMA, $renameTo);
        $this->assertNotFalse($ref->getObjectId());

        $ref = $this->getSynapseTableReflection();
        $this->expectException(ReflectionException::class);
        $ref->getObjectId();
    }

    private function createTestTableWithColumns(bool $includeTimestamp = false, bool $includePrimaryKey = false): void
    {
        $table = sprintf('[%s].[%s]', self::TEST_SCHEMA, self::TEST_TABLE);
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

    public function testGetTruncateTableCommand(): void
    {
        $this->createTestSchema();
        $this->createCreateTempTableCommandWithData();

        $ref = $this->getSynapseTableReflection(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $ref2 = $this->getSynapseTableReflection(self::TEST_SCHEMA, self::TEST_STAGING_TABLE_2);
        $this->assertEquals(3, $ref->getRowsCount());
        $this->assertEquals(3, $ref2->getRowsCount());

        $qb = new SynapseTableQueryBuilder($this->connection);
        $sql = $qb->getTruncateTableCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $this->assertEquals(
            'TRUNCATE TABLE [utils-test_qb-schema].[#stagingTable]',
            $sql
        );
        $this->connection->exec($sql);

        $this->assertEquals(0, $ref->getRowsCount());
        $this->assertEquals(3, $ref2->getRowsCount());
    }

    private function createCreateTempTableCommandWithData(bool $includeEmptyValues = false): void
    {
        foreach ([self::TEST_STAGING_TABLE, self::TEST_STAGING_TABLE_2] as $t) {
            $this->connection->exec($this->tableQb->getCreateTempTableCommand(
                self::TEST_SCHEMA,
                $t,
                new ColumnCollection([
                    SynapseColumn::createGenericColumn('pk1'),
                    SynapseColumn::createGenericColumn('pk2'),
                    SynapseColumn::createGenericColumn('col1'),
                    SynapseColumn::createGenericColumn('col2'),
                ])
            ));
            $this->connection->exec(
                sprintf(
                    'INSERT INTO [%s].[%s]([pk1],[pk2],[col1],[col2]) VALUES (1,1,\'1\',\'1\')',
                    self::TEST_SCHEMA,
                    $t
                )
            );
            $this->connection->exec(
                sprintf(
                    'INSERT INTO [%s].[%s]([pk1],[pk2],[col1],[col2]) VALUES (1,1,\'1\',\'1\')',
                    self::TEST_SCHEMA,
                    $t
                )
            );
            $this->connection->exec(
                sprintf(
                    'INSERT INTO [%s].[%s]([pk1],[pk2],[col1],[col2]) VALUES (2,2,\'2\',\'2\')',
                    self::TEST_SCHEMA,
                    $t
                )
            );

            if ($includeEmptyValues) {
                $this->connection->exec(
                    sprintf(
                        'INSERT INTO [%s].[%s]([pk1],[pk2],[col1],[col2]) VALUES (2,2,\'\',NULL)',
                        self::TEST_SCHEMA,
                        $t
                    )
                );
            }
        }
    }
}
