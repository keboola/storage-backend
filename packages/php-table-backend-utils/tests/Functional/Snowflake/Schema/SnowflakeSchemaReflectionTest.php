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
        self::assertSame([self::TABLE_GENERIC], $this->schemaRef->getTablesNames());
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
