<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Table\Teradata;

use Doctrine\DBAL\Exception as DBALException;
use Keboola\Datatype\Definition\Teradata;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Tests\Keboola\TableBackendUtils\Functional\Teradata\TeradataBaseCase;

/**
 * @covers SynapseTableReflection
 * @uses   ColumnCollection
 */
class TeradataTableQueryBuilderTest extends TeradataBaseCase
{
    /** @var TeradataTableQueryBuilder */
    private $qb;

    public function setUp(): void
    {
        $this->qb = new TeradataTableQueryBuilder();
        parent::setUp();

        $this->cleanDatabase(self::TEST_DATABASE);
        $this->createDatabase(self::TEST_DATABASE);
    }

    public function testGetRenameTableCommand(): void
    {
        $testDb = self::TEST_DATABASE;
        $testTable = self::TABLE_GENERIC;
        $testTableNew = 'newName';
        $this->initTable();

        // reflection to old table
        $refOld = new TeradataTableReflection($this->connection, $testDb, $testTable);

        // get, test and run command
        $sql = $this->qb->getRenameTableCommand(self::TEST_DATABASE, self::TABLE_GENERIC, $testTableNew);
        self::assertEquals("RENAME TABLE \"{$testDb}\".\"{$testTable}\" AS \"{$testDb}\".\"{$testTableNew}\"", $sql);
        $this->connection->executeQuery($sql);

        // reflection to new table and check the existence via counting
        $refNew = new TeradataTableReflection($this->connection, $testDb, $testTableNew);
        $refNew->getRowsCount();

        // test NON existence of old table via counting
        $this->expectException(DBALException::class);
        $refOld->getRowsCount();
    }

    public function testGetTruncateTableCommand(): void
    {
        $testDb = self::TEST_DATABASE;
        $testTable = self::TABLE_GENERIC;
        $this->initTable();

        // reflection to the table
        $ref = new TeradataTableReflection($this->connection, $testDb, $testTable);

        // check that table is empty
        self::assertEquals(0, $ref->getRowsCount());

        // insert some data, table wont be empty
        $this->insertRowToTable($testDb, $testTable, 1, 'franta', 'omacka');
        self::assertEquals(1, $ref->getRowsCount());

        // get, test and run query
        $sql = $this->qb->getTruncateTableCommand(self::TEST_DATABASE, self::TABLE_GENERIC);
        self::assertEquals("DELETE \"{$testDb}\".\"{$testTable}\" ALL", $sql);
        $this->connection->executeQuery($sql);

        // check that table is empty again
        self::assertEquals(0, $ref->getRowsCount());
    }

    public function testGetDropTableCommand(): void
    {
        $testDb = self::TEST_DATABASE;
        $testTable = self::TABLE_GENERIC;
        $this->initTable();

        // reflection to the table
        $ref = new TeradataTableReflection($this->connection, $testDb, $testTable);

        // get, test and run query
        $sql = $this->qb->getDropTableCommand(self::TEST_DATABASE, self::TABLE_GENERIC);
        self::assertEquals("DROP TABLE \"{$testDb}\".\"{$testTable}\"", $sql);
        $this->connection->executeQuery($sql);

        // test NON existence of old table via counting
        $this->expectException(DBALException::class);
        $ref->getRowsCount();
    }

    /**
     * @param TeradataColumn[] $columns
     * @param string[] $PKs
     * @param string[] $expectedColumnNames
     * @param string[] $expectedPKs
     * @param string $expectedSql
     * @throws DBALException
     * @dataProvider createTableTestSqlProvider
     */
    public function testGetCreateCommand(
        array $columns,
        array $PKs,
        array $expectedColumnNames,
        array $expectedPKs,
        string $expectedSql
    ): void {
        $sql = $this->qb->getCreateTableCommand(self::TEST_DATABASE, self::TABLE_GENERIC, new ColumnCollection($columns), $PKs);
        self::assertSame($expectedSql, $sql);
        $this->connection->executeQuery($sql);

        // test table properties
        $tableReflection = new TeradataTableReflection($this->connection, self::TEST_DATABASE, self::TABLE_GENERIC);
        self::assertSame($tableReflection->getColumnsNames(), $expectedColumnNames);
        self::assertSame($tableReflection->getPrimaryKeysNames(), $expectedPKs);
    }

    /**
     * @param TeradataColumn[] $columns
     * @param string[] $PKs
     * @param string $exceptionString
     * @dataProvider createTableInvalidPKsProvider
     * @throws \Exception
     */
    public function testGetCreateCommandWithInvalidPks(array $columns, array $PKs, string $exceptionString): void
    {
        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage($exceptionString);
        $this->qb->getCreateTableCommand(self::TEST_DATABASE, self::TABLE_GENERIC, new ColumnCollection($columns), $PKs);
        self::fail('Should fail because of invalid PKs');
    }

    /**
     * @return \Generator<string, mixed, mixed, mixed>
     */
    public function createTableTestSqlProvider(): \Generator
    {
        $testDb = self::TEST_DATABASE;
        $tableName = self::TABLE_GENERIC;

        yield 'no keys' => [
            'cols' => [
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
            ],
            'primaryKeys' => [],
            'expectedColumnNames' => ['col1', 'col2'],
            'expectedPrimaryKeys' => [],
            'query' => <<<EOT
CREATE MULTISET TABLE "$testDb"."$tableName", FALLBACK
("col1" VARCHAR(4000) NOT NULL DEFAULT '',
"col2" VARCHAR(4000) NOT NULL DEFAULT '');
EOT
            ,
        ];
        yield 'with single pk' => [
            'cols' => [
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
            ],
            'primaryKeys' => ['col1'],
            'expectedColumnNames' => ['col1', 'col2'],
            'expectedPrimaryKeys' => ['col1'],
            'query' => <<<EOT
CREATE MULTISET TABLE "$testDb"."$tableName", FALLBACK
("col1" VARCHAR(4000) NOT NULL DEFAULT '',
"col2" VARCHAR(4000) NOT NULL DEFAULT '',
PRIMARY KEY ("col1"));
EOT
            ,
        ];
        yield 'with multiple pks' => [
            'cols' => [
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
            ],
            'primaryKeys' => ['col1', 'col2'],
            'expectedColumnNames' => ['col1', 'col2'],
            'expectedPrimaryKeys' => ['col1', 'col2'],
            'query' => <<<EOT
CREATE MULTISET TABLE "$testDb"."$tableName", FALLBACK
("col1" VARCHAR(4000) NOT NULL DEFAULT '',
"col2" VARCHAR(4000) NOT NULL DEFAULT '',
PRIMARY KEY ("col1","col2"));
EOT
            ,
        ];
    }

    /**
     * @return \Generator<string, mixed, mixed, mixed>
     */
    public function createTableInvalidPKsProvider(): \Generator
    {
        yield 'key of ouf columns' => [
            'cols' => [
                TeradataColumn::createGenericColumn('col1'),
                TeradataColumn::createGenericColumn('col2'),
            ],
            'primaryKeys' => ['colNotExisting'],
            'exceptionString' => 'Trying to set colNotExisting as PKs but not present in columns',
        ];
        yield 'pk on disallowed type' => [
            'cols' => [
                new TeradataColumn('col1', new Teradata(Teradata::TYPE_BLOB, ['nullable' => false])),
            ],
            'primaryKeys' => ['col1'],
            'exceptionString' => 'Trying to set PK on column col1 but type BLOB is not supported for PK',
        ];
        yield 'pk on nullable type' => [
            'cols' => [
                new TeradataColumn('col1', new Teradata(Teradata::TYPE_VARCHAR, ['nullable' => true])),
            ],
            'primaryKeys' => ['col1'],
            'exceptionString' => 'Trying to set PK on column col1 but this column is nullable',
        ];
    }
}
