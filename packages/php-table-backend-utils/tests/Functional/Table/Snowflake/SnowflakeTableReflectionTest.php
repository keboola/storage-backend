<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Table\Snowflake;

use Generator;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Keboola\TableBackendUtils\Table\TableStats;
use Tests\Keboola\TableBackendUtils\Functional\Connection\Snowflake\SnowflakeBaseCase;

/**
 * @covers SnowflakeTableReflection
 * @uses   ColumnCollection
 */
class SnowflakeTableReflectionTest extends SnowflakeBaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema(self::TEST_SCHEMA);
    }

    public function testGetTableColumnsNames(): void
    {
        $this->initTable();
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);

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
                'ALTER TABLE %s.%s ADD PRIMARY KEY ("id")',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
            )
        );
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertEquals(['id'], $ref->getPrimaryKeysNames());
    }

    public function testGetRowsCount(): void
    {
        $this->initTable();
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertEquals(0, $ref->getRowsCount());
        $data = [
            [1, 'franta', 'omacka'],
            [2, 'pepik', 'knedla'],
        ];
        foreach ($data as $item) {
            $this->insertRowToTable(self::TEST_SCHEMA, self::TABLE_GENERIC, ...$item);
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
        $this->cleanSchema(self::TEST_SCHEMA);
        $this->createSchema(self::TEST_SCHEMA);
        $sql = sprintf(
            '
            CREATE OR REPLACE TABLE %s.%s (
      "firstColumn" INT,
      "column" %s
);',
            SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
            $sqlDef
        );

        $this->connection->executeQuery($sql);
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        /** @var Generator<SnowflakeColumn> $iterator */
        $iterator = $ref->getColumnsDefinitions()->getIterator();
        $iterator->next();
        $column = $iterator->current();
        /** @var Snowflake $definition */
        $definition = $column->getColumnDefinition();
        self::assertEquals($expectedLength, $definition->getLength());
        self::assertEquals($expectedDefault, $definition->getDefault());
        self::assertEquals($expectedType, $definition->getType());
        self::assertEquals($expectedNullable, $definition->isNullable());
        self::assertEquals($expectedSqlDefinition, $definition->getSQLDefinition());
    }

    /**
     * @return Generator<string,array<mixed>>
     */
    public function tableColsDataProvider(): Generator
    {
        // ints
        yield 'INTEGER' => [
            'INTEGER', // sql which goes to table
            'NUMBER(38,0)', // expected sql from getSQLDefinition
            'NUMBER', // expected type from db
            null, // default
            '38,0', // length
            true, // nullable
        ];

        // TODO snlkf types
    }
}
