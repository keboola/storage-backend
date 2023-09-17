<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Bigquery\View;

use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\View\Bigquery\BigqueryViewReflection;
use Tests\Keboola\TableBackendUtils\Functional\Bigquery\BigqueryBaseCase;

class BigqueryViewReflectionTest extends BigqueryBaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDataset(self::TEST_SCHEMA);
    }

    public function testGetDependentViews(): void
    {
        $this->initTable();
        $this->initView(self::VIEW_GENERIC, self::TABLE_GENERIC);

        $ref = new BigqueryViewReflection($this->bqClient, self::TEST_SCHEMA, self::VIEW_GENERIC);

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
        $sql = sprintf(
            'CREATE VIEW %s.%s AS SELECT * FROM %s.%s;',
            BigqueryQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            BigqueryQuote::quoteSingleIdentifier($viewName),
            BigqueryQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            BigqueryQuote::quoteSingleIdentifier($parentName),
        );
        $query = $this->bqClient->query($sql);
        $this->bqClient->runQuery($query);
    }

    public function testGetViewDefinition(): void
    {
        $this->initTable();
        $this->initView(self::VIEW_GENERIC, self::TABLE_GENERIC);
        $viewRef = new BigqueryViewReflection($this->bqClient, self::TEST_SCHEMA, self::VIEW_GENERIC);
        // CREATE VIEW `rb-bq-backend-utils.utilsTest_refTableSchema.utilsTest_refView`\n
        // OPTIONS(\n
        //  expiration_timestamp=TIMESTAMP "2023-11-14T10:36:33.397Z"\n
        // )\n
        // AS SELECT * FROM `utilsTest_refTableSchema`.`utilsTest_refTab`;
        $dataset = $this->bqClient->dataset(self::TEST_SCHEMA);
        self::assertStringContainsString(
            sprintf(
                'CREATE VIEW `%s.%s.utilsTest_refView`',
                $dataset->identity()['projectId'],
                $dataset->identity()['datasetId'],
            ),
            $viewRef->getViewDefinition()
        );
        self::assertStringContainsString(
            'AS SELECT * FROM `utilsTest_refTableSchema`.`utilsTest_refTab`',
            $viewRef->getViewDefinition()
        );
    }

    public function testRefreshView(): void
    {
        // create table A
        $this->initTable();
        // create view V from table A
        $this->initView(self::VIEW_GENERIC, self::TABLE_GENERIC);
        // add new column to table A
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'ALTER TABLE %s.%s ADD COLUMN `xxx` STRING(300);',
            BigqueryQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
        )));
        // check that table A has new column (3->4)
        $tableRef = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertCount(4, $tableRef->getColumnsNames());

        // check that view V has not the new column yet
        $viewTableRef = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertCount(3, $viewTableRef->getColumnsNames());

        // refresh the view V
        $viewRef = new BigqueryViewReflection($this->bqClient, self::TEST_SCHEMA, self::VIEW_GENERIC);
        $viewRef->refreshView();

        // check that view V has the new column
        $tableRef = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertCount(4, $tableRef->getColumnsNames());
    }
}
