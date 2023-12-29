<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Synapse\View;

use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Keboola\TableBackendUtils\View\InvalidViewDefinitionException;
use Keboola\TableBackendUtils\View\SynapseViewReflection;
use Tests\Keboola\TableBackendUtils\Functional\Synapse\SynapseBaseCase;

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
        $this->connection->executeStatement($this->schemaQb->getCreateSchemaCommand(self::TEST_SCHEMA));
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
        $this->connection->executeStatement(
            sprintf(
                'CREATE TABLE [%s].[%s] (
          [int_def] INT NOT NULL DEFAULT 0,
          [var_def] nvarchar(1000) NOT NULL DEFAULT (\'\'),
          [num_def] NUMERIC(10,5) DEFAULT ((1.00)),
          [_time] datetime2 NOT NULL DEFAULT \'2020-02-01 00:00:00\'
        );',
                self::TEST_SCHEMA,
                self::TABLE_GENERIC,
            ),
        );
    }

    private function initView(string $viewName, string $parentName): void
    {
        $this->connection->executeStatement(
            sprintf(
                'CREATE VIEW [%s].[%s] AS SELECT * FROM [%s].[%s];',
                self::TEST_SCHEMA,
                $viewName,
                self::TEST_SCHEMA,
                $parentName,
            ),
        );
    }

    public function testGetViewDefinition(): void
    {
        $this->initTable();
        $this->initView(self::VIEW_GENERIC, self::TABLE_GENERIC);
        $viewRef = new SynapseViewReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertEquals(
        // phpcs:disable
            <<< EOT
CREATE VIEW [utils-test_ref-table-schema].[utils-test_ref-view]\r\nAS SELECT * FROM [utils-test_ref-table-schema].[utils-test_ref];
EOT
            // phpcs:enable
            ,
            $viewRef->getViewDefinition(),
        );

        // try same with lowe case
        $this->connection->executeStatement(
            sprintf(
                'CREATE VIEW [%s].[%s] AS select * from [%s].[%s];',
                self::TEST_SCHEMA,
                '[utils-test_ref-view2',
                self::TEST_SCHEMA,
                self::TABLE_GENERIC,
            ),
        );

        $viewRef = new SynapseViewReflection($this->connection, self::TEST_SCHEMA, '[utils-test_ref-view2');
        self::assertEquals(
        // phpcs:disable
            <<< EOT
CREATE VIEW [utils-test_ref-table-schema].[[utils-test_ref-view2]\r\nAS select * from [utils-test_ref-table-schema].[utils-test_ref];
EOT
            // phpcs:enable
            ,
            $viewRef->getViewDefinition(),
        );
    }

    public function testGetViewDefinitionCannotBeObtained(): void
    {
        $this->initHugeView();
        $viewRef = new SynapseViewReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        $this->expectException(InvalidViewDefinitionException::class);
        // phpcs:ignore
        $this->expectExceptionMessage('Definition of view "utils-test_ref-view" in schema "utils-test_ref-table-schema"cannot be obtained from Synapse or it\'s invalid.');
        $viewRef->getViewDefinition();
    }

    private function initHugeView(): void
    {
        $cols = [];
        $colsNames = [];
        for ($i = 0; $i < 150; $i++) {
            $colsName = sprintf('my_most_favourite_long_int_col%d', $i);
            $colsNames[] = $colsName;
            $cols[] = sprintf(
                '[%s] INT NOT NULL DEFAULT 0',
                $colsName,
            );
        }

        $this->connection->executeStatement(sprintf(
            'CREATE TABLE [%s].[%s] (%s);',
            self::TEST_SCHEMA,
            self::TABLE_GENERIC,
            implode(',', $cols),
        ));
        $this->connection->executeStatement(sprintf(
            'CREATE VIEW [%s].[%s] AS SELECT %s FROM [%s].[%s];',
            self::TEST_SCHEMA,
            self::VIEW_GENERIC,
            implode(',', $colsNames),
            self::TEST_SCHEMA,
            self::TABLE_GENERIC,
        ));
    }

    public function testRefreshView(): void
    {
        $this->initTable();
        $this->initView(self::VIEW_GENERIC, self::TABLE_GENERIC);
        // add new column
        $this->connection->executeStatement(sprintf(
            'ALTER TABLE [%s].[%s] ADD [xxx] varchar NULL;',
            self::TEST_SCHEMA,
            self::TABLE_GENERIC,
        ));
        $tableRef = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertCount(4, $tableRef->getColumnsNames());
        $viewRef = new SynapseViewReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        $viewRef->refreshView();
        $tableRef = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertCount(5, $tableRef->getColumnsNames());
    }

    public function testRefreshViewViewDefinitionCannotBeObtained(): void
    {
        $this->initHugeView();
        $viewRef = new SynapseViewReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        $this->expectException(InvalidViewDefinitionException::class);
        // phpcs:ignore
        $this->expectExceptionMessage('Definition of view "utils-test_ref-view" in schema "utils-test_ref-table-schema"cannot be obtained from Synapse or it\'s invalid.');
        $viewRef->refreshView();
    }
}
