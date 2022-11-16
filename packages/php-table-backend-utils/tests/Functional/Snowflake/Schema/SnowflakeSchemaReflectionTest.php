<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Snowflake\Schema;

use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Schema\Snowflake\SnowflakeSchemaReflection;
use Tests\Keboola\TableBackendUtils\Functional\Snowflake\SnowflakeBaseCase;

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
                SnowflakeQuote::quoteSingleIdentifier('transient_table')
            )
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
                SnowflakeQuote::quoteSingleIdentifier('temporary_table')
            )
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
            SnowflakeQuote::quoteSingleIdentifier($tableName)
        );
        $this->connection->executeQuery($sql);
        self::assertSame([$viewName], $this->schemaRef->getViewsNames());
    }
}
