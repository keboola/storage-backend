<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Bigquery\Schema;

use Generator;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\ReflectionException;
use Keboola\TableBackendUtils\Schema\Bigquery\BigquerySchemaReflection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Tests\Keboola\TableBackendUtils\Functional\Bigquery\BigqueryBaseCase;

class BigquerySchemaReflectionTest extends BigqueryBaseCase
{
    private BigquerySchemaReflection $schemaRef;

    private BigqueryTableQueryBuilder $qb;

    public function setUp(): void
    {
        $this->qb = new BigqueryTableQueryBuilder();
        parent::setUp();
        $this->schemaRef = new BigquerySchemaReflection($this->bqClient, $this->getDatasetName());

        $this->cleanDataset($this->getDatasetName());
        $this->createDataset($this->getDatasetName());
    }

    public function testListTables(): void
    {
        $this->initTable($this->getDatasetName(), self::TABLE_GENERIC, false);
        $dbName = $this->getDatasetName();
        $sql = <<<EOT
CREATE TABLE $dbName.`nopitable` (`amount` STRING (32000));
EOT;
        $this->bqClient->runQuery($this->bqClient->query($sql));
        $sql = <<<EOT
CREATE VIEW $dbName.`nopiview` AS SELECT * FROM $dbName.`nopitable`;
EOT;
        $this->bqClient->runQuery($this->bqClient->query($sql));
        $expectedTables = [self::TABLE_GENERIC, 'nopitable'];
        $actualTables = $this->schemaRef->getTablesNames();
        $this->assertCount(0, array_diff($expectedTables, $actualTables));
        $this->assertCount(0, array_diff($actualTables, $expectedTables));
    }

    public function testListViews(): void
    {
        $this->initTable($this->getDatasetName(), self::TABLE_GENERIC, false);
        $dbName = $this->getDatasetName();
        $sql = <<<EOT
CREATE TABLE $dbName.`nopitable` (`amount` STRING (32000));
EOT;
        $this->bqClient->runQuery($this->bqClient->query($sql));
        $sql = <<<EOT
CREATE VIEW $dbName.`nopiview` AS SELECT * FROM $dbName.`nopitable`;
EOT;
        $this->bqClient->runQuery($this->bqClient->query($sql));
        $expectedTables = ['nopiview'];
        $actualTables = $this->schemaRef->getViewsNames();
        $this->assertCount(0, array_diff($expectedTables, $actualTables));
        $this->assertCount(0, array_diff($actualTables, $expectedTables));
    }


    public function testListTablesOnNonExistingDatasetThrowException(): void
    {
        $this->initTable($this->getDatasetName(), self::TABLE_GENERIC, false);

        $this->expectException(ReflectionException::class);
        $this->expectExceptionMessage('Dataset "notExisting" not found.');
        $schemaRef = new BigquerySchemaReflection($this->bqClient, 'notExisting');
        $schemaRef->getTablesNames();
    }

    /**
     * @dataProvider createTableTestFromDefinitionSqlProvider
     */
    public function testGetCreateTableCommandFromDefinition(
        BigqueryTableDefinition $definition,
        string $expectedSql,
        bool $createPrimaryKeys,
    ): void {
        $this->cleanDataset($this->getDatasetName());
        $this->createDataset($this->getDatasetName());
        $sql = $this->qb->getCreateTableCommandFromDefinition($definition, $createPrimaryKeys);
        self::assertSame($expectedSql, $sql);
        $this->bqClient->runQuery($this->bqClient->query($sql));

        // test table properties
        $tableReflection = new BigqueryTableReflection(
            $this->bqClient,
            $this->getDatasetName(),
            self::TABLE_GENERIC,
        );
        self::assertSame($definition->getColumnsNames(), $tableReflection->getColumnsNames());
        if ($createPrimaryKeys) {
            self::assertSame($definition->getPrimaryKeysNames(), $tableReflection->getPrimaryKeysNames());
        } else {
            self::assertSame([], $tableReflection->getPrimaryKeysNames());
        }
    }

    /**
     * @return \Generator<string, array{
     *     definition: BigqueryTableDefinition,
     *     query: string,
     *     createPrimaryKeys: bool
     * }>
     */
    public function createTableTestFromDefinitionSqlProvider(): Generator
    {
        $testDb = $this->getDatasetName();
        $tableName = self::TABLE_GENERIC;

        yield 'no keys' => [
            'definition' => new BigqueryTableDefinition(
                $testDb,
                $tableName,
                false,
                new ColumnCollection(
                    [
                        BigqueryColumn::createGenericColumn('col1'),
                        BigqueryColumn::createGenericColumn('col2'),
                    ],
                ),
                [],
            ),
            'query' => <<<EOT
CREATE TABLE `$testDb`.`$tableName` 
(
`col1` STRING DEFAULT '' NOT NULL,
`col2` STRING DEFAULT '' NOT NULL
);
EOT
            ,
            'createPrimaryKeys' => true,
        ];

        yield 'single primary key' => [
            'definition' => new BigqueryTableDefinition(
                $testDb,
                $tableName,
                false,
                new ColumnCollection(
                    [
                        BigqueryColumn::createGenericColumn('id'),
                        BigqueryColumn::createGenericColumn('name'),
                    ],
                ),
                ['id'],
            ),
            'query' => <<<EOT
CREATE TABLE `$testDb`.`$tableName` 
(
`id` STRING DEFAULT '' NOT NULL,
`name` STRING DEFAULT '' NOT NULL,
PRIMARY KEY (`id`) NOT ENFORCED
);
EOT
            ,
            'createPrimaryKeys' => true,
        ];

        yield 'composite primary key' => [
            'definition' => new BigqueryTableDefinition(
                $testDb,
                $tableName,
                false,
                new ColumnCollection(
                    [
                        BigqueryColumn::createGenericColumn('id'),
                        BigqueryColumn::createGenericColumn('type'),
                        BigqueryColumn::createGenericColumn('name'),
                    ],
                ),
                ['id', 'type'],
            ),
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
            'createPrimaryKeys' => true,
        ];

        yield 'primary keys not created when flag is false' => [
            'definition' => new BigqueryTableDefinition(
                $testDb,
                $tableName,
                false,
                new ColumnCollection(
                    [
                        BigqueryColumn::createGenericColumn('id'),
                        BigqueryColumn::createGenericColumn('name'),
                    ],
                ),
                ['id'],
            ),
            'query' => <<<EOT
CREATE TABLE `$testDb`.`$tableName` 
(
`id` STRING DEFAULT '' NOT NULL,
`name` STRING DEFAULT '' NOT NULL
);
EOT
            ,
            'createPrimaryKeys' => false,
        ];
    }
}
