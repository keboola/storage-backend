<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Bigquery\Table;

use Generator;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Tests\Keboola\TableBackendUtils\Functional\Bigquery\BigqueryBaseCase;

class BigqueryTableQueryBuilderTest extends BigqueryBaseCase
{
    private BigqueryTableQueryBuilder $qb;

    public function setUp(): void
    {
        $this->qb = new BigqueryTableQueryBuilder();
        parent::setUp();

        $this->cleanSchema(self::TEST_SCHEMA);
    }

    /**
     * @param BigqueryColumn[] $columns
     * @param string[] $primaryKeys
     * @param string[] $expectedColumnNames
     * @param string[] $expectedPKs
     * @dataProvider createTableTestSqlProvider
     */
    public function testGetCreateCommand(
        array $columns,
        array $primaryKeys,
        array $expectedColumnNames,
        array $expectedPKs,
        string $expectedSql
    ): void {
        $this->cleanSchema(self::TEST_SCHEMA);
        $this->createSchema(self::TEST_SCHEMA);

        $sql = $this->qb->getCreateTableCommand(
            self::TEST_SCHEMA,
            self::TABLE_GENERIC,
            new ColumnCollection($columns),
            [] // primary keys aren't supported in BQ
        );

        self::assertSame($expectedSql, $sql);

        $query = $this->bqClient->query($sql);
        $this->bqClient->runQuery($query);

        // test table properties
        $tableReflection = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertSame($expectedColumnNames, $tableReflection->getColumnsNames());
        self::assertFalse($tableReflection->isTemporary());
    }

    /**
     * @return \Generator<string, mixed, mixed, mixed>
     */
    public function createTableTestSqlProvider(): Generator
    {
        $testDb = self::TEST_SCHEMA;
        $tableName = self::TABLE_GENERIC;

        yield 'no keys' => [
            'cols' => [
                BigqueryColumn::createGenericColumn('col1'),
                BigqueryColumn::createGenericColumn('col2'),
            ],
            'primaryKeys' => [],
            'expectedColumnNames' => ['col1', 'col2'],
            'expectedPrimaryKeys' => [],
            'query' => <<<EOT
CREATE TABLE `$testDb`.`$tableName` 
(
`col1` STRING DEFAULT '' NOT NULL,
`col2` STRING DEFAULT '' NOT NULL
);
EOT
            ,
        ];
    }
}
