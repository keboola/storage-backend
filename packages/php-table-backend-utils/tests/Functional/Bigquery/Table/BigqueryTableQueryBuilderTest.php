<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Bigquery\Table;

use Generator;
use Google\Cloud\BigQuery\Exception\JobException;
use Google\Cloud\Core\Exception\NotFoundException;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use Tests\Keboola\TableBackendUtils\Functional\Bigquery\BigqueryBaseCase;

class BigqueryTableQueryBuilderTest extends BigqueryBaseCase
{
    private BigqueryTableQueryBuilder $qb;

    public function setUp(): void
    {
        $this->qb = new BigqueryTableQueryBuilder();
        parent::setUp();

        $this->cleanDataset(self::TEST_SCHEMA);
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
        string $expectedSql,
    ): void {
        $this->cleanDataset(self::TEST_SCHEMA);
        $this->createDataset(self::TEST_SCHEMA);

        $sql = $this->qb->getCreateTableCommand(
            self::TEST_SCHEMA,
            self::TABLE_GENERIC,
            new ColumnCollection($columns),
            $primaryKeys,
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

        yield 'single primary key' => [
            'cols' => [
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('name'),
            ],
            'primaryKeys' => ['id'],
            'expectedColumnNames' => ['id', 'name'],
            'expectedPrimaryKeys' => ['id'],
            'query' => <<<EOT
CREATE TABLE `$testDb`.`$tableName` 
(
`id` STRING DEFAULT '' NOT NULL,
`name` STRING DEFAULT '' NOT NULL,
PRIMARY KEY (`id`) NOT ENFORCED
);
EOT
            ,
        ];

        yield 'composite primary key' => [
            'cols' => [
                BigqueryColumn::createGenericColumn('id'),
                BigqueryColumn::createGenericColumn('type'),
                BigqueryColumn::createGenericColumn('name'),
            ],
            'primaryKeys' => ['id', 'type'],
            'expectedColumnNames' => ['id', 'type', 'name'],
            'expectedPrimaryKeys' => ['id', 'type'],
            'query' => <<<EOT
CREATE TABLE `$testDb`.`$tableName` 
(
`id` STRING DEFAULT '' NOT NULL,
`type` STRING DEFAULT '' NOT NULL,
`name` STRING DEFAULT '' NOT NULL,
PRIMARY KEY (`id`,`type`) NOT ENFORCED
);
EOT
            ,
        ];
    }

    public function testInvalidPrimaryKeyThrowsException(): void
    {
        $this->cleanDataset(self::TEST_SCHEMA);
        $this->createDataset(self::TEST_SCHEMA);

        $columns = [
            BigqueryColumn::createGenericColumn('col1'),
            BigqueryColumn::createGenericColumn('col2'),
        ];

        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage('Trying to set "nonexistent" as PKs but not present in columns');

        $this->qb->getCreateTableCommand(
            self::TEST_SCHEMA,
            self::TABLE_GENERIC,
            new ColumnCollection($columns),
            ['nonexistent'],
        );
    }

    public function testGetDropTableCommand(): void
    {
        $testDb = $this->getDatasetName();
        $testTable = self::TABLE_GENERIC;
        $this->initTable();

        // reflection to the table
        $ref = new BigqueryTableReflection($this->bqClient, $testDb, $testTable);

        // get, test and run query
        $sql = $this->qb->getDropTableCommand($this->getDatasetName(), self::TABLE_GENERIC);
        self::assertEquals("DROP TABLE `$testDb`.`$testTable`", $sql);
        $this->bqClient->runQuery($this->bqClient->query($sql));

        // test NON existence of old table via counting
        $this->expectException(TableNotExistsReflectionException::class);
        $ref->getRowsCount();
    }

    public function testAddAndDropColumn(): void
    {
        $this->cleanDataset(self::TEST_SCHEMA);
        $this->createDataset(self::TEST_SCHEMA);

        $columns = [BigqueryColumn::createGenericColumn('col1'),
            BigqueryColumn::createGenericColumn('col2')];

        $sql = $this->qb->getCreateTableCommand(
            self::TEST_SCHEMA,
            self::TABLE_GENERIC,
            new ColumnCollection($columns),
            [],
        );

        $this->bqClient->runQuery($this->bqClient->query($sql));

        // add column
        $sql = $this->qb->getAddColumnCommand(
            self::TEST_SCHEMA,
            self::TABLE_GENERIC,
            new BigqueryColumn('col3', new Bigquery(
                Bigquery::TYPE_STRING,
            )),
        );
        $this->assertEquals(
            sprintf(
                'ALTER TABLE `%s`.`%s` ADD COLUMN `col3` STRING',
                self::TEST_SCHEMA,
                self::TABLE_GENERIC,
            ),
            $sql,
        );
        $this->bqClient->runQuery($this->bqClient->query($sql));

        $tableReflection = new BigqueryTableReflection(
            $this->bqClient,
            self::TEST_SCHEMA,
            self::TABLE_GENERIC,
        );
        self::assertSame(['col1', 'col2', 'col3'], $tableReflection->getColumnsNames());

        // drop column
        $sql = $this->qb->getDropColumnCommand(self::TEST_SCHEMA, self::TABLE_GENERIC, 'col2');
        $this->assertEquals(sprintf(
            'ALTER TABLE `%s`.`%s` DROP COLUMN `col2`',
            self::TEST_SCHEMA,
            self::TABLE_GENERIC,
        ), $sql);
        $this->bqClient->runQuery($this->bqClient->query($sql));

        $tableReflection = new BigqueryTableReflection(
            $this->bqClient,
            self::TEST_SCHEMA,
            self::TABLE_GENERIC,
        );
        self::assertSame(['col1', 'col3'], $tableReflection->getColumnsNames());
    }
}
