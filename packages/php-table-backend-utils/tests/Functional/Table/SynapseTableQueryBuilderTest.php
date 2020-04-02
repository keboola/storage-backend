<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Table;

use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\ReflectionException;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Tests\Keboola\TableBackendUtils\Functional\SynapseBaseCase;

class SynapseTableQueryBuilderTest extends SynapseBaseCase
{
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'qb-schema';
    public const TEST_STAGING_TABLE = '#stagingTable';
    public const TEST_TABLE = self::TESTS_PREFIX . 'test';

    protected function dropTestSchema(): void
    {
        $this->connection->exec($this->schemaQb->getDropSchemaCommand(self::TEST_SCHEMA));
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema(self::TEST_SCHEMA);
    }

    public function testGetCreateTempTableCommand(): void
    {
        $this->createTestSchema();
        $sql = $this->tableQb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            '#' . self::TEST_TABLE,
            [
                SynapseColumn::createGenericColumn('col1'),
                SynapseColumn::createGenericColumn('col2'),
            ]
        );

        $this->assertEquals(
        // phpcs:ignore
            'CREATE TABLE [utils-test_qb-schema].[#utils-test_test] ([col1] nvarchar(4000) NOT NULL DEFAULT \'\', [col2] nvarchar(4000) NOT NULL DEFAULT \'\') WITH (HEAP, LOCATION = USER_DB)',
            $sql
        );
        $this->connection->exec($sql);
    }

    protected function createTestSchema(): void
    {
        $this->connection->exec($this->schemaQb->getCreateSchemaCommand(self::TEST_SCHEMA));
    }

    public function testGetDropCommand(): void
    {
        $this->createTestSchema();
        $this->createTestTable();
        $sql = $this->tableQb->getDropTableCommand(self::TEST_SCHEMA, self::TEST_TABLE);

        $this->assertEquals(
            'DROP TABLE [utils-test_qb-schema].[utils-test_test]',
            $sql
        );

        $this->connection->exec($sql);

        $ref = $this->getSynapseTableReflection();
        $this->expectException(ReflectionException::class);
        $ref->getObjectId();
    }

    protected function createTestTable(): void
    {
        $table = sprintf('[%s].[%s]', self::TEST_SCHEMA, self::TEST_TABLE);
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

    private function getSynapseTableReflection(
        string $schema = self::TEST_SCHEMA,
        string $table = self::TEST_TABLE
    ): SynapseTableReflection {
        return new SynapseTableReflection($this->connection, $schema, $table);
    }

    public function testGetRenameTableCommand(): void
    {
        $renameTo = 'newTable';
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $sql = $this->tableQb->getRenameTableCommand(self::TEST_SCHEMA, self::TEST_TABLE, $renameTo);

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

    protected function createTestTableWithColumns(bool $includeTimestamp = false, bool $includePrimaryKey = false): void
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
        $this->assertEquals(3, $ref->getRowsCount());

        $sql = $this->tableQb->getTruncateTableCommand(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        $this->assertEquals(
            'TRUNCATE TABLE [utils-test_qb-schema].[#stagingTable]',
            $sql
        );
        $this->connection->exec($sql);

        $this->assertEquals(0, $ref->getRowsCount());
    }

    private function createCreateTempTableCommandWithData(bool $includeEmptyValues = false): void
    {
        $this->connection->exec($this->tableQb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            [
                SynapseColumn::createGenericColumn('pk1'),
                SynapseColumn::createGenericColumn('pk2'),
                SynapseColumn::createGenericColumn('col1'),
                SynapseColumn::createGenericColumn('col2'),
            ]
        ));
        $this->connection->exec(
            sprintf(
                'INSERT INTO [%s].[%s]([pk1],[pk2],[col1],[col2]) VALUES (1,1,\'1\',\'1\')',
                self::TEST_SCHEMA,
                self::TEST_STAGING_TABLE
            )
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO [%s].[%s]([pk1],[pk2],[col1],[col2]) VALUES (1,1,\'1\',\'1\')',
                self::TEST_SCHEMA,
                self::TEST_STAGING_TABLE
            )
        );
        $this->connection->exec(
            sprintf(
                'INSERT INTO [%s].[%s]([pk1],[pk2],[col1],[col2]) VALUES (2,2,\'2\',\'2\')',
                self::TEST_SCHEMA,
                self::TEST_STAGING_TABLE
            )
        );

        if ($includeEmptyValues) {
            $this->connection->exec(
                sprintf(
                    'INSERT INTO [%s].[%s]([pk1],[pk2],[col1],[col2]) VALUES (2,2,\'\',NULL)',
                    self::TEST_SCHEMA,
                    self::TEST_STAGING_TABLE
                )
            );
        }
    }
}
