<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Table\Teradata;

use Generator;
use Keboola\Datatype\Definition\Teradata;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Tests\Keboola\TableBackendUtils\Functional\Teradata\TeradataBaseCase;

/**
 * @covers TeradataTableReflection
 * @uses   ColumnCollection
 */
class TeradataTableReflectionTest extends TeradataBaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase(self::TEST_DATABASE);
        $this->createDatabase(self::TEST_DATABASE);
    }

    public function testGetTableColumnsNames(): void
    {
        $this->initTable();
        $ref = new TeradataTableReflection($this->connection, self::TEST_DATABASE, self::TABLE_GENERIC);

        self::assertSame([
            'id',
            'first_name',
            'last_name',
        ], $ref->getColumnsNames());
    }

    public function testGetPrimaryKeysNames(): void
    {
        $this->initTable();
        $this->connection->executeQuery(
            sprintf(
                'ALTER TABLE %s.%s ADD PRIMARY KEY (id)',
                self::TEST_DATABASE,
                self::TABLE_GENERIC
            )
        );
        $ref = new TeradataTableReflection($this->connection, self::TEST_DATABASE, self::TABLE_GENERIC);
        self::assertEquals(['id'], $ref->getPrimaryKeysNames());
    }

    public function testGetRowsCount(): void
    {
        $this->initTable();
        $ref = new TeradataTableReflection($this->connection, self::TEST_DATABASE, self::TABLE_GENERIC);
        self::assertEquals(0, $ref->getRowsCount());
        $data = [
            [1, 'franta', 'omacka'],
            [2, 'pepik', 'knedla'],
        ];
        foreach ($data as $item) {
            $this->insertRowToTable(self::TEST_DATABASE, self::TABLE_GENERIC, ...$item);
        }
        self::assertEquals(2, $ref->getRowsCount());
    }

    /**
     * @dataProvider tableColsDataProvider
     */
    public function testColumnDefinition(
        string $sqlDef,
        string $expectedSqlDefinition,
        string $expectedType,
        ?string $expectedDefault,
        ?string $expectedLength,
        ?string $expectedNullable
    ): void {
        $sql = sprintf(
            'CREATE MULTISET TABLE %s.%s ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      "firstColumn" INT,
      "column" %s
     );',
            self::TEST_DATABASE,
            self::TABLE_GENERIC,
            $sqlDef
        );

        $this->connection->executeQuery($sql);
        $ref = new TeradataTableReflection($this->connection, self::TEST_DATABASE, self::TABLE_GENERIC);
        /** @var TeradataColumn $column */
        $iterator = $ref->getColumnsDefinitions()->getIterator();
        $iterator->next();
        $column = $iterator->current();
        /** @var Teradata $definition */
        $definition = $column->getColumnDefinition();
        self::assertEquals($expectedLength, $definition->getLength());
        self::assertEquals($expectedDefault, $definition->getDefault());
        self::assertEquals($expectedType, $definition->getType());
        self::assertEquals($expectedNullable, $definition->isNullable());
        self::assertEquals($expectedSqlDefinition, $definition->getSQLDefinition());
    }


    public function tableColsDataProvider(): Generator
    {
        yield 'INTEGER' => [
            'INTEGER', // sql which goes to table
            'INTEGER', // expected sql from getSQLDefinition
            'INTEGER', // type
            null, // default
            4, // length
            true, // nullable
        ];

        yield 'INT' => [
            'INT',
            'INTEGER',
            'INTEGER',
            null,
            4,
            true,
        ];

        yield 'INTEGER NOT NULL DEFAULT' => [
            'INTEGER NOT NULL DEFAULT 5',
            'INTEGER NOT NULL DEFAULT 5',
            'INTEGER', // type
            5, // default
            4, // length
            false, // nullable
        ];
        yield 'CHAR WITH LENGTH' => [
            'CHAR (20)', // SQL to create column
            'CHAR (20)', // expected SQL
            'CHAR', // type
            null, // default
            20, // length
            true, // nullable
        ];
        yield 'BYTEINT' => [
            'BYTEINT',
            'BYTEINT',
            'BYTEINT',
            null,
            1,
            true,
        ];
        yield 'BIGINT' => [
            'BIGINT',
            'BIGINT',
            'BIGINT',
            null,
            8,
            true,
        ];
        yield 'SMALLINT' => [
            'SMALLINT',
            'SMALLINT',
            'SMALLINT',
            null,
            2,
            true,
        ];
        yield 'DECIMAL' => [
            'DECIMAL (10,10)',
            'DECIMAL (10,10)',
            'DECIMAL',
            null,
            '10,10',
            true,
        ];
        yield 'FLOAT' => [
            'FLOAT',
            'FLOAT',
            'FLOAT',
            null,
            8,
            true,
        ];
        yield 'NUMBER' => [
            'NUMBER (12,10)',
            'NUMBER (12,10)',
            'NUMBER',
            null,
            '12,10',
            true,
        ];
        yield 'BYTE' => [
            'BYTE (50)',
            'BYTE (50)',
            'BYTE',
            null,
            50,
            true,
        ];
        yield 'VARBYTE' => [
            'VARBYTE (32000)',
            'VARBYTE (32000)',
            'VARBYTE',
            null,
            32000,
            true,
        ];
        yield 'BLOB' => [
            'BLOB (2M)',
            'BLOB (2097152)',
            'BLOB',
            null,
            '2097152',
            true,
        ];

        yield 'DATE' => [
            'DATE',
            'DATE',
            'DATE',
            null,
            '4',
            true,
        ];
        yield 'TIME' => [
            'TIME (6)',
            'TIME (6)',
            'TIME',
            null,
            '6',
            true,
        ];
        yield 'TIMESTAMP' => [
            'TIMESTAMP (1)',
            'TIMESTAMP (1)',
            'TIMESTAMP',
            null,
            '1',
            true,
        ];
        yield 'TIME WITH ZONE' => [
            'TIME (5) WITH TIME ZONE',
            'TIME (5) WITH TIME ZONE',
            'TIME_WITH_ZONE',
            null,
            '5',
            true,
        ];
        // no length -> default
        yield 'TIMESTAMP WITH ZONE with default length' => [
            'TIMESTAMP WITH TIME ZONE',
            'TIMESTAMP (6) WITH TIME ZONE',
            'TIMESTAMP_WITH_ZONE',
            null,
            '6',
            true,
        ];
        // 0 length
        yield 'TIMESTAMP WITH ZONE with zero length' => [
            'TIMESTAMP (0) WITH TIME ZONE',
            'TIMESTAMP (0) WITH TIME ZONE',
            'TIMESTAMP_WITH_ZONE',
            null,
            '0',
            true,
        ];

        yield 'CHAR' => [
            'CHAR (32000)',
            'CHAR (32000)',
            'CHAR',
            null,
            32000,
            true,
        ];
        yield 'VARCHAR' => [
            'VARCHAR (32000)',
            'VARCHAR (32000)',
            'VARCHAR',
            null,
            32000,
            true,
        ];
        yield 'LONG VARCHAR' => [
            'VARCHAR (64000)',
            'VARCHAR (64000)',
            'VARCHAR',
            null,
            64000,
            true,
        ];
        yield 'LONG VARCHAR with UNICODE' => [
            'VARCHAR (32000) CHARACTER SET UNICODE',
            'VARCHAR (32000)',
            'VARCHAR',
            null,
            32000,
            true,
        ];
        yield 'CLOB' => [
            'CLOB (2M)',
            'CLOB (2097152)',
            'CLOB',
            null,
            '2097152',
            true,
        ];
        yield 'CLOB with Unicode' => [
            'CLOB (2M)  CHARACTER SET UNICODE',
            'CLOB (2097152)',
            'CLOB',
            null,
            '2097152',
            true,
        ];
        yield 'PERIOD(DATE)' => [
            'PERIOD(DATE)',
            'PERIOD(DATE)',
            'PERIOD(DATE)',
            null,
            '8',
            true,
        ];
        yield 'PERIOD(TIME)' => [
            'PERIOD(TIME)',
            'PERIOD(TIME (6))',
            'PERIOD(TIME)',
            null,
            '6',
            true,
        ];
        yield 'PERIOD(TIME) with fraction' => [
            'PERIOD(TIME (2))',
            'PERIOD(TIME (2))',
            'PERIOD(TIME)',
            null,
            '2',
            true,
        ];
    }

    public function testGetTableStats(): void
    {
        $this->initTable();
        $ref = new TeradataTableReflection($this->connection, self::TEST_DATABASE, self::TABLE_GENERIC);

        $stats1 = $ref->getTableStats();
        self::assertEquals(0, $stats1->getRowsCount());
        self::assertGreaterThan(1024, $stats1->getDataSizeBytes()); // empty tables take up some space

        $this->insertRowToTable(self::TEST_DATABASE, self::TABLE_GENERIC, 1, 'lojza', 'lopata');
        $this->insertRowToTable(self::TEST_DATABASE, self::TABLE_GENERIC, 2, 'karel', 'motycka');

        $stats2 = $ref->getTableStats();
        self::assertEquals(2, $stats2->getRowsCount());
        self::assertGreaterThan($stats1->getDataSizeBytes(), $stats2->getDataSizeBytes());
    }
}
