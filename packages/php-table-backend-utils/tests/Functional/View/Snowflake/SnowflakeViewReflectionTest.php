<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\View\Snowflake;

use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Keboola\TableBackendUtils\View\Snowflake\SnowflakeViewReflection;
use Tests\Keboola\TableBackendUtils\Functional\Connection\Snowflake\SnowflakeBaseCase;

/**
 * @covers SnowflakeViewReflection
 */
class SnowflakeViewReflectionTest extends SnowflakeBaseCase
{

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema(self::TEST_SCHEMA);
    }

    public function testGetDependentViews(): void
    {
        $this->initTable();
        $this->initView(self::VIEW_GENERIC, self::TABLE_GENERIC);

        $ref = new SnowflakeViewReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);

        self::assertCount(0, $ref->getDependentViews());

        $secondViewName = self::VIEW_GENERIC . '-2';
        $this->initView($secondViewName, self::VIEW_GENERIC);

        $dependentViews = $ref->getDependentViews();
        self::assertCount(1, $dependentViews);

        self::assertSame([
            'schema_name' => self::TEST_SCHEMA,
            'name' => $secondViewName,
        ], $dependentViews[0]);
    }


    private function initView(string $viewName, string $parentName): void
    {
        $this->connection->executeQuery(
            sprintf(
                'CREATE VIEW %s AS SELECT * FROM %s.%s;',
                SnowflakeQuote::quoteSingleIdentifier($viewName),
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier($parentName)
            )
        );
    }

    public function testGetViewDefinition(): void
    {
        $this->initTable();
        $this->initView(self::VIEW_GENERIC, self::TABLE_GENERIC);
        $viewRef = new SnowflakeViewReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertEquals(
            'CREATE VIEW "utilsTest_refView" AS SELECT * FROM "utilsTest_refTableSchema"."utilsTest_refTab";',
            $viewRef->getViewDefinition()
        );
    }


    public function testRefreshView(): void
    {
        // create table A
        $this->initTable();
        // create view V from table A
        $this->initView(self::VIEW_GENERIC, self::TABLE_GENERIC);
        // add new column to table A
        $this->connection->executeQuery(sprintf(
            'ALTER TABLE %s ADD COLUMN "xxx" VARCHAR(300) NULL;',
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
        ));
        // check that table A has new column (3->4)
        $tableRef = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertCount(4, $tableRef->getColumnsNames());

        // check that view V has not the new column yet
        $viewTableRef = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertCount(3, $viewTableRef->getColumnsNames());

        // refresh the view V
        $viewRef = new SnowflakeViewReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        $viewRef->refreshView();

        // check that view V has the new column
        $tableRef = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertCount(4, $tableRef->getColumnsNames());
    }
}
