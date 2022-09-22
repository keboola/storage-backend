<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Exasol\Table;

use Generator;
use Keboola\Datatype\Definition\Exasol;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
use Keboola\TableBackendUtils\Table\TableStats;
use Tests\Keboola\TableBackendUtils\Functional\Exasol\ExasolBaseCase;

/**
 * @covers ExasolTableReflection
 * @uses   ColumnCollection
 */
class ExasolTableReflectionTest extends ExasolBaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema(self::TEST_SCHEMA);
    }

    public function testGetTableColumnsNames(): void
    {
        $this->initTable();
        $ref = new ExasolTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);

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
                'ALTER TABLE %s.%s ADD CONSTRAINT PRIMARY KEY ("id")',
                ExasolQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                ExasolQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
            )
        );
        $ref = new ExasolTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertEquals(['id'], $ref->getPrimaryKeysNames());
    }

    public function testGetRowsCount(): void
    {
        $this->initTable();
        $ref = new ExasolTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
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
        bool $expectedNullable
    ): void {
        $this->cleanSchema(self::TEST_SCHEMA);
        $this->createSchema(self::TEST_SCHEMA);
        $sql = sprintf(
            '
            CREATE OR REPLACE TABLE %s.%s (
      "firstColumn" INT,
      "column" %s
);',
            ExasolQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            ExasolQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
            $sqlDef
        );

        $this->connection->executeQuery($sql);
        $ref = new ExasolTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        /** @var Generator<ExasolColumn> $iterator */
        $iterator = $ref->getColumnsDefinitions()->getIterator();
        $iterator->next();
        $column = $iterator->current();
        /** @var Exasol $definition */
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
            'DECIMAL (18,0)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '18,0', // length
            true, // nullable
        ];

        yield 'INT' => [
            'INT', // sql which goes to table
            'DECIMAL (18,0)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '18,0', // length
            true, // nullable
        ];

        yield 'BIGINT' => [
            'BIGINT', // sql which goes to table
            'DECIMAL (36,0)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '36,0', // length
            true, // nullable
        ];

        // booleans
        yield 'BOOL' => [
            'BOOL', // sql which goes to table
            'BOOLEAN', // expected sql from getSQLDefinition
            'BOOLEAN', // expected type from db
            null, // default
            '1', // length
            true, // nullable
        ];
        yield 'BOOLEAN' => [
            'BOOLEAN', // sql which goes to table
            'BOOLEAN', // expected sql from getSQLDefinition
            'BOOLEAN', // expected type from db
            null, // default
            '1', // length
            true, // nullable
        ];

        // decimals
        yield 'DEC' => [
            'DEC', // sql which goes to table
            'DECIMAL (18,0)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '18,0', // length
            true, // nullable
        ];
        yield 'DEC (p)' => [
            'DEC (9)', // sql which goes to table
            'DECIMAL (9,0)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '9,0', // length
            true, // nullable
        ];
        yield 'DEC (p,s)' => [
            'DEC (9,5)', // sql which goes to table
            'DECIMAL (9,5)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '9,5', // length
            true, // nullable
        ];

        yield 'DECIMAL (p)' => [
            'DECIMAL (9)', // sql which goes to table
            'DECIMAL (9,0)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '9,0', // length
            true, // nullable
        ];
        yield 'DECIMAL (p,s)' => [
            'DECIMAL (12,5)', // sql which goes to table
            'DECIMAL (12,5)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '12,5', // length
            true, // nullable
        ];

        yield 'NUMBER (p)' => [
            'NUMBER (10,0)', // sql which goes to table
            'DECIMAL (10,0)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '10,0', // length
            true, // nullable
        ];
        yield 'NUMBER (p,s)' => [
            'NUMBER (10,5)', // sql which goes to table
            'DECIMAL (10,5)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '10,5', // length
            true, // nullable
        ];
        yield 'NUMERIC (p)' => [
            'NUMERIC (10,0)', // sql which goes to table
            'DECIMAL (10,0)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '10,0', // length
            true, // nullable
        ];
        yield 'NUMERIC (p,s)' => [
            'NUMERIC (10,5)', // sql which goes to table
            'DECIMAL (10,5)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '10,5', // length
            true, // nullable
        ];
        yield 'SHORTINT' => [
            'SHORTINT', // sql which goes to table
            'DECIMAL (9,0)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '9,0', // length
            true, // nullable
        ];
        yield 'SMALLINT' => [
            'SMALLINT', // sql which goes to table
            'DECIMAL (9,0)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '9,0', // length
            true, // nullable
        ];
        yield 'TINYINT' => [
            'TINYINT', // sql which goes to table
            'DECIMAL (3,0)', // expected sql from getSQLDefinition
            'DECIMAL', // expected type from db
            null, // default
            '3,0', // length
            true, // nullable
        ];

        // doubles
        yield 'DOUBLE' => [
            'DOUBLE', // sql which goes to table
            'DOUBLE PRECISION', // expected sql from getSQLDefinition
            'DOUBLE PRECISION', // expected type from db
            null, // default
            '64', // length
            true, // nullable
        ];
        yield 'DOUBLE PRECISION' => [
            'DOUBLE PRECISION', // sql which goes to table
            'DOUBLE PRECISION', // expected sql from getSQLDefinition
            'DOUBLE PRECISION', // expected type from db
            null, // default
            '64', // length
            true, // nullable
        ];
        yield 'FLOAT' => [
            'FLOAT', // sql which goes to table
            'DOUBLE PRECISION', // expected sql from getSQLDefinition
            'DOUBLE PRECISION', // expected type from db
            null, // default
            '64', // length
            true, // nullable
        ];
        yield 'NUMBER' => [
            'NUMBER', // sql which goes to table
            'DOUBLE PRECISION', // expected sql from getSQLDefinition
            'DOUBLE PRECISION', // expected type from db
            null, // default
            '64', // length
            true, // nullable
        ];
        yield 'REAL' => [
            'REAL', // sql which goes to table
            'DOUBLE PRECISION', // expected sql from getSQLDefinition
            'DOUBLE PRECISION', // expected type from db
            null, // default
            '64', // length
            true, // nullable
        ];

        // hash types
        yield 'HASHTYPE' => [
            'HASHTYPE', // sql which goes to table
            'HASHTYPE (16 BYTE)', // expected sql from getSQLDefinition
            'HASHTYPE', // expected type from db
            null, // default
            '16', // length
            true, // nullable
        ];
        yield 'HASHTYPE n bit' => [
            'HASHTYPE (24 BIT)', // sql which goes to table
            'HASHTYPE (3 BYTE)', // expected sql from getSQLDefinition
            'HASHTYPE', // expected type from db
            null, // default
            '3', // length
            true, // nullable
        ];
        yield 'HASHTYPE n byte' => [
            'HASHTYPE (10 BYTE)', // sql which goes to table
            'HASHTYPE (10 BYTE)', // expected sql from getSQLDefinition
            'HASHTYPE', // expected type from db
            null, // default
            '10', // length
            true, // nullable
        ];

        // chars
        yield 'CHAR (n)' => [
            'CHAR (300)', // sql which goes to table
            'CHAR (300)', // expected sql from getSQLDefinition
            'CHAR', // expected type from db
            null, // default
            '300', // length
            true, // nullable
        ];

        yield 'NCHAR' => [
            'NCHAR (300)', // sql which goes to table
            'CHAR (300)', // expected sql from getSQLDefinition
            'CHAR', // expected type from db
            null, // default
            '300', // length
            true, // nullable
        ];

        yield 'CHAR (1)' => [
            'CHAR', // sql which goes to table
            'CHAR (1)', // expected sql from getSQLDefinition
            'CHAR', // expected type from db
            null, // default
            '1', // length
            true, // nullable
        ];
        yield 'CHAR (300)' => [
            'CHAR (300)', // sql which goes to table
            'CHAR (300)', // expected sql from getSQLDefinition
            'CHAR', // expected type from db
            null, // default
            '300', // length
            true, // nullable
        ];

        yield 'CHARACTER (2000)' => [
            'CHARACTER (2000)', // sql which goes to table
            'CHAR (2000)', // expected sql from getSQLDefinition
            'CHAR', // expected type from db
            null, // default
            '2000', // length
            true, // nullable
        ];
        yield 'CHARACTER (1)' => [
            'CHARACTER', // sql which goes to table
            'CHAR (1)', // expected sql from getSQLDefinition
            'CHAR', // expected type from db
            null, // default
            '1', // length
            true, // nullable
        ];

        // varchars
        yield 'LONG VARCHAR' => [
            'LONG VARCHAR', // sql which goes to table
            'VARCHAR (2000000)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '2000000', // length
            true, // nullable
        ];
        yield 'NVARCHAR (n)' => [
            'NVARCHAR (30000)', // sql which goes to table
            'VARCHAR (30000)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '30000', // length
            true, // nullable
        ];
        yield 'NVARCHAR2 (n)' => [
            'NVARCHAR2 (30000)', // sql which goes to table
            'VARCHAR (30000)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '30000', // length
            true, // nullable
        ];
        yield 'VARCHAR2 (n)' => [
            'VARCHAR2 (30000)', // sql which goes to table
            'VARCHAR (30000)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '30000', // length
            true, // nullable
        ];
        yield 'VARCHAR (n)' => [
            'VARCHAR (30000)', // sql which goes to table
            'VARCHAR (30000)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '30000', // length
            true, // nullable
        ];

        yield 'CHAR VARYING (n)' => [
            'CHAR VARYING (5000)', // sql which goes to table
            'VARCHAR (5000)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '5000', // length
            true, // nullable
        ];

        yield 'CHARACTER VARYING (n)' => [
            'CHARACTER VARYING (6000)', // sql which goes to table
            'VARCHAR (6000)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '6000', // length
            true, // nullable
        ];

        // clob
        yield 'CHARACTER LARGE OBJECT' => [
            'CHARACTER LARGE OBJECT', // sql which goes to table
            'VARCHAR (2000000)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '2000000', // length
            true, // nullable
        ];
        yield 'CHARACTER LARGE OBJECT (n)' => [
            'CHARACTER LARGE OBJECT (4000)', // sql which goes to table
            'VARCHAR (4000)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '4000', // length
            true, // nullable
        ];

        yield 'CLOB' => [
            'CLOB', // sql which goes to table
            'VARCHAR (2000000)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '2000000', // length
            true, // nullable
        ];
        yield 'CLOB (n)' => [
            'CLOB (5000)', // sql which goes to table
            'VARCHAR (5000)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '5000', // length
            true, // nullable
        ];

        // date and time
        yield 'DATE' => [
            'DATE', // sql which goes to table
            'DATE', // expected sql from getSQLDefinition
            'DATE', // expected type from db
            null, // default
            '10', // length
            true, // nullable
        ];
        yield 'TIMESTAMP' => [
            'TIMESTAMP', // sql which goes to table
            'TIMESTAMP', // expected sql from getSQLDefinition
            'TIMESTAMP', // expected type from db
            null, // default
            '29', // length
            true, // nullable
        ];
        yield 'TIMESTAMP WITH LOCAL TIME ZONE' => [
            'TIMESTAMP WITH LOCAL TIME ZONE', // sql which goes to table
            'TIMESTAMP WITH LOCAL TIME ZONE', // expected sql from getSQLDefinition
            'TIMESTAMP WITH LOCAL TIME ZONE', // expected type from db
            null, // default
            '29', // length
            true, // nullable
        ];

        // intervals
        yield 'INTERVAL YEAR [(p)] TO MONTH' => [
            'INTERVAL YEAR(5) TO MONTH', // sql which goes to table
            'INTERVAL YEAR(5) TO MONTH', // expected sql from getSQLDefinition
            'INTERVAL YEAR TO MONTH', // expected type from db
            null, // default
            '5', // length
            true, // nullable
        ];
        yield 'INTERVAL YEAR TO MONTH' => [
            'INTERVAL YEAR(5) TO MONTH', // sql which goes to table
            'INTERVAL YEAR(5) TO MONTH', // expected sql from getSQLDefinition
            'INTERVAL YEAR TO MONTH', // expected type from db
            null, // default
            '5', // length
            true, // nullable
        ];
        yield 'INTERVAL DAY [(p)] TO SECOND [(fp)]' => [
            'INTERVAL DAY(5) TO SECOND(4)', // sql which goes to table
            'INTERVAL DAY(5) TO SECOND(4)', // expected sql from getSQLDefinition
            'INTERVAL DAY TO SECOND', // expected type from db
            null, // default
            '5,4', // length
            true, // nullable
        ];

        // geometry
        yield 'GEOMETRY [(srid)]' => [
            'GEOMETRY (100)', // sql which goes to table
            'GEOMETRY (8000000)', // expected sql from getSQLDefinition
            'GEOMETRY', // expected type from db
            null, // default
            '8000000', // length
            true, // nullable
        ];
        yield 'GEOMETRY' => [
            'GEOMETRY', // sql which goes to table
            'GEOMETRY (8000000)', // expected sql from getSQLDefinition
            'GEOMETRY', // expected type from db
            null, // default
            '8000000', // length
            true, // nullable
        ];
    }

    public function testGetTableStats(): void
    {
        $this->initTable();
        $ref = new ExasolTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);

        $stats1 = $ref->getTableStats();
        self::assertEquals(0, $stats1->getRowsCount());
        self::assertEquals(0, $stats1->getDataSizeBytes()); // empty tables take up some space

        $this->insertRowToTable(self::TEST_SCHEMA, self::TABLE_GENERIC, 1, 'lojza', 'lopata');
        $this->insertRowToTable(self::TEST_SCHEMA, self::TABLE_GENERIC, 2, 'karel', 'motycka');

        /** @var TableStats $stats2 */
        $stats2 = $ref->getTableStats();
        self::assertEquals(2, $stats2->getRowsCount());
        self::assertGreaterThan($stats1->getDataSizeBytes(), $stats2->getDataSizeBytes());
    }

    public function testGetDependentViews(): void
    {
        $this->initTable();

        $ref = new ExasolTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);

        self::assertCount(0, $ref->getDependentViews());

        $this->initView();

        $dependentViews = $ref->getDependentViews();
        self::assertCount(1, $dependentViews);

        self::assertSame([
            'schema_name' => self::TEST_SCHEMA,
            'name' => self::VIEW_GENERIC,
        ], $dependentViews[0]);
    }

    private function initView(): void
    {
        $this->connection->executeQuery(
            sprintf(
                'CREATE VIEW %s.%s AS SELECT * FROM %s.%s;',
                ExasolQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                ExasolQuote::quoteSingleIdentifier(self::VIEW_GENERIC),
                ExasolQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                ExasolQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
            )
        );
    }


    public function testIfTableExists(): void
    {
        $this->initTable();

        $ref = new ExasolTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertTrue($ref->exists());
    }

    public function testIfSchemaDoesNotExists(): void
    {
        $ref = new ExasolTableReflection($this->connection, 'noSchema', 'notExisting');
        self::assertFalse($ref->exists());
    }

    public function testIfTableDoesNotExists(): void
    {
        $this->initTable();

        $ref = new ExasolTableReflection($this->connection, self::TEST_SCHEMA, 'notExisting');
        self::assertFalse($ref->exists());
    }
}
