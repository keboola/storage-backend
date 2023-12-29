<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Exasol\View;

use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
use Keboola\TableBackendUtils\View\Exasol\ExasolViewReflection;
use Keboola\TableBackendUtils\View\InvalidViewDefinitionException;
use Tests\Keboola\TableBackendUtils\Functional\Exasol\ExasolBaseCase;

/**
 * @covers ExasolViewReflection
 */
class ExasolViewReflectionTest extends ExasolBaseCase
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

        $ref = new ExasolViewReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);

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
                'CREATE VIEW %s.%s AS SELECT * FROM %s.%s;',
                ExasolQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                ExasolQuote::quoteSingleIdentifier($viewName),
                ExasolQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                ExasolQuote::quoteSingleIdentifier($parentName),
            ),
        );
    }

    public function testGetViewDefinition(): void
    {
        $this->initTable();
        $this->initView(self::VIEW_GENERIC, self::TABLE_GENERIC);
        $viewRef = new ExasolViewReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertEquals(
        // phpcs:disable
            <<< EOT
CREATE VIEW "utilsTest_refTableSchema"."utilsTest_refView" AS SELECT * FROM "utilsTest_refTableSchema"."utilsTest_refTab";
EOT
            // phpcs:enable
            ,
            $viewRef->getViewDefinition(),
        );
    }

    public function testGetViewDefinitionForNotExistingView(): void
    {
        $viewRef = new ExasolViewReflection($this->connection, self::TEST_SCHEMA, 'notExistingView');
        $this->expectException(InvalidViewDefinitionException::class);
        $this->expectExceptionMessage('View "notExistingView" in schema "utilsTest_refTableSchema" does not exists.');
        $viewRef->getViewDefinition();
    }

    public function testRefreshView(): void
    {
        $this->initTable();
        $this->initView(self::VIEW_GENERIC, self::TABLE_GENERIC);
        // add new column
        $this->connection->executeQuery(sprintf(
            'ALTER TABLE %s.%s ADD "xxx" VARCHAR(300) NULL;',
            ExasolQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            ExasolQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
        ));
        $tableRef = new ExasolTableReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        // the view is updated as soon as it gets compiled again
        self::assertCount(4, $tableRef->getColumnsNames());
        $viewRef = new ExasolViewReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        $viewRef->refreshView();
        $tableRef = new ExasolTableReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertCount(4, $tableRef->getColumnsNames());
    }
}
