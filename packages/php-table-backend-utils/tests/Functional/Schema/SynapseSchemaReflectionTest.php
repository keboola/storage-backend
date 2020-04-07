<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Schema;

use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Schema\SynapseSchemaReflection;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Tests\Keboola\TableBackendUtils\Functional\SynapseBaseCase;

/**
 * @covers SynapseSchemaReflection
 */
class SynapseSchemaReflectionTest extends SynapseBaseCase
{
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'ref-schema-schema';

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema(self::TEST_SCHEMA);
        $this->createTestSchema();
    }

    protected function createTestSchema(): void
    {
        $this->connection->exec($this->schemaQb->getCreateSchemaCommand(self::TEST_SCHEMA));
    }

    public function testGetTablesNames(): void
    {
        $ref = new SynapseSchemaReflection($this->connection, self::TEST_SCHEMA);
        $tables = $ref->getTablesNames();
        $this->assertEmpty($tables);

        $qb = new SynapseTableQueryBuilder($this->connection);
        $this->connection->exec($qb->getCreateTableCommand(
            self::TEST_SCHEMA,
            'table1',
            new ColumnCollection([SynapseColumn::createGenericColumn('col1')])
        ));
        $this->connection->exec($qb->getCreateTableCommand(
            self::TEST_SCHEMA,
            'table2',
            new ColumnCollection([SynapseColumn::createGenericColumn('col1')])
        ));

        $tables = $ref->getTablesNames();
        $this->assertCount(2, $tables);
        $this->assertEqualsCanonicalizing([
            'table1',
            'table2',
        ], $tables);
    }

    public function testGetViewsNames(): void
    {
        $ref = new SynapseSchemaReflection($this->connection, self::TEST_SCHEMA);
        $tables = $ref->getViewsNames();
        $this->assertEmpty($tables);

        $qb = new SynapseTableQueryBuilder($this->connection);
        $this->connection->exec($qb->getCreateTableCommand(
            self::TEST_SCHEMA,
            'table1',
            new ColumnCollection([SynapseColumn::createGenericColumn('col1')])
        ));

        $this->connection->exec(sprintf(
            'CREATE VIEW [%s].[view1] AS SELECT [col1] FROM [%s].[table1]',
            self::TEST_SCHEMA,
            self::TEST_SCHEMA
        ));
        $this->connection->exec(sprintf(
            'CREATE VIEW [%s].[view2] AS SELECT [col1] FROM [%s].[table1]',
            self::TEST_SCHEMA,
            self::TEST_SCHEMA
        ));

        $tables = $ref->getViewsNames();
        $this->assertCount(2, $tables);
        $this->assertEqualsCanonicalizing([
            'view1',
            'view2',
        ], $tables);
    }
}
