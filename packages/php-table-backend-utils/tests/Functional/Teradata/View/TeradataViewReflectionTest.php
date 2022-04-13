<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Teradata\View;

use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\View\Teradata\TeradataViewReflection;
use Tests\Keboola\TableBackendUtils\Functional\Teradata\TeradataBaseCase;

/**
 * @covers TeradataViewReflection
 */
class TeradataViewReflectionTest extends TeradataBaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase($this->getDatabaseName());
        $this->createDatabase($this->getDatabaseName());
    }

    public function testGetDependentViews(): void
    {
        $this->initTable();
        $this->initView();

        $ref = new TeradataViewReflection($this->connection, $this->getDatabaseName(), self::VIEW_GENERIC);

        self::assertCount(0, $ref->getDependentViews());

        $secondViewName = self::VIEW_GENERIC . '-2';
        $this->initView($secondViewName, self::VIEW_GENERIC);

        $dependentViews = $ref->getDependentViews();

        self::assertCount(1, $dependentViews);

        self::assertSame([
            'schema_name' => $this->getDatabaseName(),
            'name' => $secondViewName,
        ], $dependentViews[0]);
    }

    private function initView(
        string $viewName = self::VIEW_GENERIC,
        string $parentName = self::TABLE_GENERIC
    ): void {
        $this->connection->executeQuery(
            sprintf(
                'CREATE VIEW %s.%s AS SELECT * FROM %s.%s',
                TeradataQuote::quoteSingleIdentifier($this->getDatabaseName()),
                TeradataQuote::quoteSingleIdentifier($viewName),
                TeradataQuote::quoteSingleIdentifier($this->getDatabaseName()),
                TeradataQuote::quoteSingleIdentifier($parentName),
            )
        );
    }
}
