<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Table\Teradata;

use Generator;
use Keboola\Datatype\Definition\Teradata;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Tests\Keboola\TableBackendUtils\Functional\Teradata\TeradataBaseCase;

/**
 * @covers SynapseTableReflection
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
      "column" %s
     );',
            self::TEST_DATABASE,
            self::TABLE_GENERIC,
            $sqlDef
        );

        $this->connection->executeQuery($sql);
        $ref = new TeradataTableReflection($this->connection, self::TEST_DATABASE, self::TABLE_GENERIC);
        /** @var TeradataColumn $column */
        $column = $ref->getColumnsDefinitions()->getIterator()->current();
        /** @var Teradata $definition */
        $definition = $column->getColumnDefinition();
        self::assertEquals($expectedLength, $definition->getLength());
        self::assertEquals($expectedDefault, $definition->getDefault());
        self::assertEquals($expectedType, $definition->getType());
        self::assertEquals($expectedNullable, $definition->isNullable());
        self::assertEquals($expectedSqlDefinition, $definition->getSQLDefinition());
    }

    /**
     * @return Generator<array{
     *     string,
     *     string,
     *     string,
     *     ?mixed,
     *     ?integer,
     *     bool
     * }>
     */
    public function tableColsDataProvider(): Generator
    {
        // TODO add more scenarios
        yield 'INTEGER' => [
            'INTEGER',
            'INTEGER',
            'INTEGER', // type
            null, // default
            4, // length
            true, // nullable
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
            'CHAR(20)',
            'CHAR(20)',
            'CHAR', // type
            null, // default
            20, // length
            true, // nullable
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
