<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Schema\Teradata;

use Keboola\TableBackendUtils\Schema\Teradata\TeradataSchemaReflection;
use Tests\Keboola\TableBackendUtils\Functional\Teradata\TeradataBaseCase;

class TeradataSchemaReflectionTest extends TeradataBaseCase
{
    /** @var TeradataSchemaReflection */
    private $schemaRef;

    public function setUp(): void
    {
        parent::setUp();
        $this->schemaRef = new TeradataSchemaReflection($this->connection, self::TEST_DATABASE);

        $this->cleanDatabase(self::TEST_DATABASE);
        $this->createDatabase(self::TEST_DATABASE);
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
        $dbName = self::TEST_DATABASE;
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
