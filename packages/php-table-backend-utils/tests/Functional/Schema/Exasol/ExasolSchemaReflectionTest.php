<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Schema\Exasol;

use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Schema\Exasol\ExasolSchemaReflection;
use Tests\Keboola\TableBackendUtils\Functional\Exasol\ExasolBaseCase;

class ExasolSchemaReflectionTest extends ExasolBaseCase
{
    /** @var ExasolSchemaReflection */
    private $schemaRef;

    public function setUp(): void
    {
        parent::setUp();
        $this->schemaRef = new ExasolSchemaReflection($this->connection, self::TEST_SCHEMA);

        $this->cleanDatabase(self::TEST_SCHEMA);
        $this->createDatabase(self::TEST_SCHEMA);
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
            ExasolQuote::quoteSingleIdentifier($schemaName),
            ExasolQuote::quoteSingleIdentifier($viewName),
            ExasolQuote::quoteSingleIdentifier($schemaName),
            ExasolQuote::quoteSingleIdentifier($tableName)
        );
        $this->connection->executeQuery($sql);
        self::assertSame([$viewName], $this->schemaRef->getViewsNames());
    }
}
