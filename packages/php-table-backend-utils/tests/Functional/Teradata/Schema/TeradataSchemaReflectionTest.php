<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Teradata\Schema;

use Keboola\TableBackendUtils\Schema\Teradata\TeradataSchemaReflection;
use Tests\Keboola\TableBackendUtils\Functional\Teradata\TeradataBaseCase;

class TeradataSchemaReflectionTest extends TeradataBaseCase
{
    private TeradataSchemaReflection $schemaRef;

    public function setUp(): void
    {
        parent::setUp();
        $this->schemaRef = new TeradataSchemaReflection($this->connection, $this->getDatabaseName());

        $this->cleanDatabase($this->getDatabaseName());
        $this->createDatabase($this->getDatabaseName());
    }

    public function testListTables(): void
    {
        $this->initTable();
        // CREATE NO PRIMARY INDEX TABLE
        $dbName = $this->getDatabaseName();
        $sql = <<<EOT
CREATE MULTISET TABLE $dbName."nopitable",
FALLBACK ("amount" VARCHAR (32000) CHARACTER SET UNICODE) NO PRIMARY INDEX;
EOT;
        $this->connection->executeStatement($sql);
        $expectedTables = [self::TABLE_GENERIC, 'nopitable'];
        $actualTables = $this->schemaRef->getTablesNames();
        $this->assertCount(0, array_diff($expectedTables, $actualTables));
        $this->assertCount(0, array_diff($actualTables, $expectedTables));
    }

    public function testListViews(): void
    {
        $this->initTable();

        $tableName = self::TABLE_GENERIC;
        $dbName = $this->getDatabaseName();
        $viewName = self::VIEW_GENERIC;
        $sql = <<<EOT
CREATE VIEW $dbName.$viewName AS
     SELECT   first_name(TITLE 'FirstName'),
              last_name(TITLE 'LastName')
     FROM $dbName.$tableName;
EOT;
        $this->connection->executeQuery($sql);
        self::assertSame([$viewName], $this->schemaRef->getViewsNames());
    }
}
