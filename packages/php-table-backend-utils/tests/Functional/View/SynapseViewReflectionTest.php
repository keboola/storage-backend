<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\View;

use Keboola\TableBackendUtils\View\SynapseViewReflection;
use Tests\Keboola\TableBackendUtils\Functional\SynapseBaseCase;

/**
 * @covers SynapseViewReflection
 */
class SynapseViewReflectionTest extends SynapseBaseCase
{
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'ref-table-schema';
    // tables
    public const TABLE_GENERIC = self::TESTS_PREFIX . 'ref';
    //views
    public const VIEW_GENERIC = self::TESTS_PREFIX . 'ref-view';

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema(self::TEST_SCHEMA);
        $this->createTestSchema();
    }

    private function createTestSchema(): void
    {
        $this->connection->exec($this->schemaQb->getCreateSchemaCommand(self::TEST_SCHEMA));
    }

    public function testGetDependentViews(): void
    {
        $this->initTable();
        $this->initView(self::VIEW_GENERIC, self::TABLE_GENERIC);

        $ref = new SynapseViewReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);

        $this->assertCount(0, $ref->getDependentViews());

        $secondViewName = self::VIEW_GENERIC . '-2';
        $this->initView($secondViewName, self::VIEW_GENERIC);

        $dependentViews = $ref->getDependentViews();
        $this->assertCount(1, $dependentViews);

        $this->assertSame([
            'schema_name' => self::TEST_SCHEMA,
            'name' => $secondViewName,
        ], $dependentViews[0]);
    }

    private function initTable(): void
    {
        $this->connection->exec(
            sprintf(
                'CREATE TABLE [%s].[%s] (
          [int_def] INT NOT NULL DEFAULT 0,
          [var_def] nvarchar(1000) NOT NULL DEFAULT (\'\'),
          [num_def] NUMERIC(10,5) DEFAULT ((1.00)),
          [_time] datetime2 NOT NULL DEFAULT \'2020-02-01 00:00:00\'
        );',
                self::TEST_SCHEMA,
                self::TABLE_GENERIC
            )
        );
    }

    private function initView(string $viewName, string $parentName): void
    {
        $this->connection->exec(
            sprintf(
                'CREATE VIEW [%s].[%s] AS SELECT * FROM [%s].[%s];',
                self::TEST_SCHEMA,
                $viewName,
                self::TEST_SCHEMA,
                $parentName
            )
        );
    }
}
