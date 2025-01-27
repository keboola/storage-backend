<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Snowflake\Schema;

use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Schema\Snowflake\SnowflakeSchemaReflection;
use Tests\Keboola\TableBackendUtils\Functional\Snowflake\SnowflakeBaseCase;
use function PHPUnit\Framework\assertEquals;

class SnowflakeSchemaReflectionTest extends SnowflakeBaseCase
{
    private SnowflakeSchemaReflection $schemaRef;

    public function setUp(): void
    {
        parent::setUp();
        $this->schemaRef = new SnowflakeSchemaReflection($this->connection, self::TEST_SCHEMA);

        $this->cleanSchema(self::TEST_SCHEMA);
    }

    public function testListTables(): void
    {
        $this->initTable();

        // create transient table
        $this->connection->executeQuery(
            sprintf(
                'CREATE OR REPLACE TRANSIENT TABLE %s.%s (
            "id" INTEGER,
    "first_name" VARCHAR(100),
    "last_name" VARCHAR(100)
);',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier('transient_table'),
            ),
        );

        // create temporary table
        $this->connection->executeQuery(
            sprintf(
                'CREATE OR REPLACE TEMPORARY TABLE %s.%s (
            "id" INTEGER,
    "first_name" VARCHAR(100),
    "last_name" VARCHAR(100)
);',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier('temporary_table'),
            ),
        );

        $tables = $this->schemaRef->getTablesNames();
        self::assertContains(self::TABLE_GENERIC, $tables);
        self::assertContains('transient_table', $tables);
        self::assertContains('temporary_table', $tables);
    }

    public function testListViews(): void
    {
        $this->initTable();

        $tableName = self::TABLE_GENERIC;
        $schemaName = self::TEST_SCHEMA;
        $viewName = self::VIEW_GENERIC;
        $sql = sprintf(
            '
CREATE VIEW %s.%s AS
     SELECT   "first_name",
              "last_name" 
     FROM %s.%s;
',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($viewName),
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
        );
        $this->connection->executeQuery($sql);
        self::assertSame([$viewName], $this->schemaRef->getViewsNames());
    }

    public function testGetDefinitions(): void
    {
        $this->initTable();

        // create transient table
        $this->connection->executeQuery(
            sprintf(
                'CREATE OR REPLACE TRANSIENT TABLE %s.%s (
            "id" INTEGER,
    "first_name" VARCHAR(100),
    "last_name" VARCHAR(100)
);',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier('transient_table'),
            ),
        );

        // create temporary table
        $this->connection->executeQuery(
            sprintf(
                'CREATE OR REPLACE TEMPORARY TABLE %s.%s (
            "id" INTEGER,
    "first_name" VARCHAR(100),
    "last_name" VARCHAR(100)
);',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier('temporary_table'),
            ),
        );

        $tableName = self::TABLE_GENERIC;
        $schemaName = self::TEST_SCHEMA;
        $viewName = self::VIEW_GENERIC;
        $sql = sprintf(
            '
CREATE VIEW %s.%s AS
     SELECT   "first_name",
              "last_name" 
     FROM %s.%s;
',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($viewName),
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
        );
        $this->connection->executeQuery($sql);

        $definitions = $this->schemaRef->getDefinitions();

        self::assertCount(4, $definitions);

        $temporaryTableKey = 'temporary_table';
        self::assertEquals('temporary_table', $definitions[$temporaryTableKey]->getTableName());
        self::assertEquals(3, $definitions[$temporaryTableKey]->getColumnsDefinitions()->count());
        self::assertTrue($definitions[$temporaryTableKey]->isTemporary());
        self::assertEquals('table', $definitions[$temporaryTableKey]->getTableType()->value);

        $transientTableKey = 'transient_table';
        self::assertEquals('transient_table', $definitions[$transientTableKey]->getTableName());
        self::assertEquals(3, $definitions[$transientTableKey]->getColumnsDefinitions()->count());
        self::assertFalse($definitions[$transientTableKey]->isTemporary());
        self::assertEquals('table', $definitions[$transientTableKey]->getTableType()->value);

        $genericTableKey = self::TABLE_GENERIC;
        self::assertEquals(self::TABLE_GENERIC, $definitions[$genericTableKey]->getTableName());
        self::assertEquals(3, $definitions[$genericTableKey]->getColumnsDefinitions()->count());
        self::assertFalse($definitions[$genericTableKey]->isTemporary());
        self::assertEquals('table', $definitions[$genericTableKey]->getTableType()->value);

        $genericViewKey = self::VIEW_GENERIC;
        self::assertEquals(self::VIEW_GENERIC, $definitions[$genericViewKey]->getTableName());
        self::assertEquals(2, $definitions[$genericViewKey]->getColumnsDefinitions()->count());
        self::assertFalse($definitions[$genericViewKey]->isTemporary());
        self::assertEquals('view', $definitions[$genericViewKey]->getTableType()->value);
    }

    public function testGetDefinitionsWithEmptySchema(): void
    {
        $this->createSchema(self::TEST_SCHEMA);
        $definitions = $this->schemaRef->getDefinitions();

        self::assertCount(0, $definitions);
    }
}
