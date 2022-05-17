<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Exasol\Table;

use Doctrine\DBAL\Exception as DBALException;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
use Tests\Keboola\TableBackendUtils\Functional\Exasol\ExasolBaseCase;

/**
 * @covers ExasolTableQueryBuilder
 * @uses   ColumnCollection
 */
class ExasolTableQueryBuilderTest extends ExasolBaseCase
{
    private \Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder $qb;

    public function setUp(): void
    {
        $this->qb = new ExasolTableQueryBuilder();
        parent::setUp();

        $this->cleanSchema(self::TEST_SCHEMA);
    }

    public function testGetRenameTableCommand(): void
    {
        $testDb = self::TEST_SCHEMA;
        $testTable = self::TABLE_GENERIC;
        $testTableNew = 'newName';
        $this->initTable();

        // reflection to old table
        $refOld = new ExasolTableReflection($this->connection, $testDb, $testTable);

        // get, test and run command
        $sql = $this->qb->getRenameTableCommand(self::TEST_SCHEMA, self::TABLE_GENERIC, $testTableNew);
        self::assertEquals("RENAME TABLE \"{$testDb}\".\"{$testTable}\" TO \"{$testDb}\".\"{$testTableNew}\"", $sql);
        $this->connection->executeQuery($sql);

        // reflection to new table and check the existence via counting
        $refNew = new ExasolTableReflection($this->connection, $testDb, $testTableNew);
        $refNew->getRowsCount();

        // test NON existence of old table via counting
        $this->expectException(DBALException::class);
        $refOld->getRowsCount();
    }

    public function testGetTruncateTableCommand(): void
    {
        $testDb = self::TEST_SCHEMA;
        $testTable = self::TABLE_GENERIC;
        $this->initTable();

        // reflection to the table
        $ref = new ExasolTableReflection($this->connection, $testDb, $testTable);

        // check that table is empty
        self::assertEquals(0, $ref->getRowsCount());

        // insert some data, table wont be empty
        $this->insertRowToTable($testDb, $testTable, 1, 'franta', 'omacka');
        self::assertEquals(1, $ref->getRowsCount());

        // get, test and run query
        $sql = $this->qb->getTruncateTableCommand(self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertEquals("TRUNCATE TABLE \"{$testDb}\".\"{$testTable}\"", $sql);
        $this->connection->executeQuery($sql);

        // check that table is empty again
        self::assertEquals(0, $ref->getRowsCount());
    }

    public function testGetDropTableCommand(): void
    {
        $testDb = self::TEST_SCHEMA;
        $testTable = self::TABLE_GENERIC;
        $this->initTable();

        // reflection to the table
        $ref = new ExasolTableReflection($this->connection, $testDb, $testTable);

        // get, test and run query
        $sql = $this->qb->getDropTableCommand(self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertEquals("DROP TABLE \"{$testDb}\".\"{$testTable}\"", $sql);
        $this->connection->executeQuery($sql);

        // test NON existence of old table via counting
        $this->expectException(DBALException::class);
        $ref->getRowsCount();
    }

    /**
     * @param ExasolColumn[] $columns
     * @param string[] $primaryKeys
     * @param string[] $expectedColumnNames
     * @param string[] $expectedPKs
     * @throws DBALException
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
            $primaryKeys
        );
        self::assertSame($expectedSql, $sql);
        $this->connection->executeQuery($sql);

        // test table properties
        $tableReflection = new ExasolTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertSame($expectedColumnNames, $tableReflection->getColumnsNames());
        self::assertSame($expectedPKs, $tableReflection->getPrimaryKeysNames());
    }

    /**
     * @return \Generator<string, mixed, mixed, mixed>
     */
    public function createTableTestSqlProvider(): \Generator
    {
        $testDb = self::TEST_SCHEMA;
        $tableName = self::TABLE_GENERIC;

        yield 'no keys' => [
            'cols' => [
                ExasolColumn::createGenericColumn('col1'),
                ExasolColumn::createGenericColumn('col2'),
            ],
            'primaryKeys' => [],
            'expectedColumnNames' => ['col1', 'col2'],
            'expectedPrimaryKeys' => [],
            'query' => <<<EOT
CREATE TABLE "$testDb"."$tableName"
(
"col1" VARCHAR (2000000) DEFAULT '' NOT NULL,
"col2" VARCHAR (2000000) DEFAULT '' NOT NULL
);
EOT
            ,
        ];
        yield 'with single pk' => [
            'cols' => [
                ExasolColumn::createGenericColumn('col1'),
                ExasolColumn::createGenericColumn('col2'),
            ],
            'primaryKeys' => ['col1'],
            'expectedColumnNames' => ['col1', 'col2'],
            'expectedPrimaryKeys' => ['col1'],
            'query' => <<<EOT
CREATE TABLE "$testDb"."$tableName"
(
"col1" VARCHAR (2000000) DEFAULT '' NOT NULL,
"col2" VARCHAR (2000000) DEFAULT '' NOT NULL,
CONSTRAINT PRIMARY KEY ("col1")
);
EOT
            ,
        ];
        yield 'with multiple pks' => [
            'cols' => [
                ExasolColumn::createGenericColumn('col1'),
                ExasolColumn::createGenericColumn('col2'),
            ],
            'primaryKeys' => ['col1', 'col2'],
            'expectedColumnNames' => ['col1', 'col2'],
            'expectedPrimaryKeys' => ['col1', 'col2'],
            'query' => <<<EOT
CREATE TABLE "$testDb"."$tableName"
(
"col1" VARCHAR (2000000) DEFAULT '' NOT NULL,
"col2" VARCHAR (2000000) DEFAULT '' NOT NULL,
CONSTRAINT PRIMARY KEY ("col1","col2")
);
EOT
            ,
        ];
    }

    /**
     * @return \Generator<string, array{
     *     definition: ExasolTableDefinition,
     *     query: string,
     *     createPrimaryKeys: bool
     * }>
     */
    public function createTableTestFromDefinitionSqlProvider(): \Generator
    {
        $testDb = self::TEST_SCHEMA;
        $tableName = self::TABLE_GENERIC;

        yield 'no keys' => [
            'definition' => new ExasolTableDefinition(
                $testDb,
                $tableName,
                false,
                new ColumnCollection(
                    [
                        ExasolColumn::createGenericColumn('col1'),
                        ExasolColumn::createGenericColumn('col2'),
                    ]
                ),
                []
            ),
            'query' => <<<EOT
CREATE TABLE "$testDb"."$tableName"
(
"col1" VARCHAR (2000000) DEFAULT '' NOT NULL,
"col2" VARCHAR (2000000) DEFAULT '' NOT NULL
);
EOT
            ,
            'createPrimaryKeys' => true,
        ];
        yield 'with single pk' => [
            'definition' => new ExasolTableDefinition(
                $testDb,
                $tableName,
                false,
                new ColumnCollection(
                    [
                        ExasolColumn::createGenericColumn('col1'),
                        ExasolColumn::createGenericColumn('col2'),
                    ]
                ),
                ['col1']
            ),
            'query' => <<<EOT
CREATE TABLE "$testDb"."$tableName"
(
"col1" VARCHAR (2000000) DEFAULT '' NOT NULL,
"col2" VARCHAR (2000000) DEFAULT '' NOT NULL,
CONSTRAINT PRIMARY KEY ("col1")
);
EOT
            ,
            'createPrimaryKeys' => true,
        ];
        yield 'with multiple pks' => [
            'definition' => new ExasolTableDefinition(
                $testDb,
                $tableName,
                false,
                new ColumnCollection(
                    [
                        ExasolColumn::createGenericColumn('col1'),
                        ExasolColumn::createGenericColumn('col2'),
                    ]
                ),
                ['col1', 'col2']
            ),
            'query' => <<<EOT
CREATE TABLE "$testDb"."$tableName"
(
"col1" VARCHAR (2000000) DEFAULT '' NOT NULL,
"col2" VARCHAR (2000000) DEFAULT '' NOT NULL,
CONSTRAINT PRIMARY KEY ("col1","col2")
);
EOT
            ,
            'createPrimaryKeys' => true,
        ];

        yield 'with multiple pks no definition' => [
            'definition' => new ExasolTableDefinition(
                $testDb,
                $tableName,
                false,
                new ColumnCollection(
                    [
                        ExasolColumn::createGenericColumn('col1'),
                        ExasolColumn::createGenericColumn('col2'),
                    ]
                ),
                ['col1', 'col2']
            ),
            'query' => <<<EOT
CREATE TABLE "$testDb"."$tableName"
(
"col1" VARCHAR (2000000) DEFAULT '' NOT NULL,
"col2" VARCHAR (2000000) DEFAULT '' NOT NULL
);
EOT
            ,
            'createPrimaryKeys' => false,
        ];
    }

    /**
     * @dataProvider createTableTestFromDefinitionSqlProvider
     */
    public function testGetCreateTableCommandFromDefinition(
        ExasolTableDefinition $definition,
        string $expectedSql,
        bool $createPrimaryKeys
    ): void {
        $this->cleanSchema(self::TEST_SCHEMA);
        $this->createSchema(self::TEST_SCHEMA);
        $sql = $this->qb->getCreateTableCommandFromDefinition($definition, $createPrimaryKeys);
        self::assertSame($expectedSql, $sql);
        $this->connection->executeQuery($sql);

        // test table properties
        $tableReflection = new ExasolTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertSame($definition->getColumnsNames(), $tableReflection->getColumnsNames());
        if ($createPrimaryKeys === true) {
            self::assertSame($definition->getPrimaryKeysNames(), $tableReflection->getPrimaryKeysNames());
        } else {
            self::assertSame([], $tableReflection->getPrimaryKeysNames());
        }
    }
}
