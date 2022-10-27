<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Bigquery\Table;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Tests\Keboola\TableBackendUtils\Functional\Bigquery\BigqueryBaseCase;

class BigqueryTableReflectionTest extends BigqueryBaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema(self::TEST_SCHEMA);
    }

    public function testGetTableColumnsNames(): void
    {
        $this->initTable();
        $ref = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, self::TABLE_GENERIC);

        self::assertSame([
            'id',
            'first_name',
            'last_name',
        ], $ref->getColumnsNames());
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
      `firstColumn` INT,
      `column` %s
);',
            BigqueryQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
            $sqlDef
        );

        $query = $this->bqClient->query($sql);
        $this->bqClient->runQuery($query);
        $ref = new BigqueryTableReflection($this->bqClient, self::TEST_SCHEMA, self::TABLE_GENERIC);
        /** @var Generator<BigqueryColumn> $iterator */
        $iterator = $ref->getColumnsDefinitions()->getIterator();
        $iterator->next();
        $column = $iterator->current();
        /** @var Bigquery $definition */
        $definition = $column->getColumnDefinition();
        self::assertEquals($expectedLength, $definition->getLength(), 'length doesnt match');
        self::assertEquals($expectedDefault, $definition->getDefault(), 'default value doesnt match');
        self::assertEquals($expectedType, $definition->getType(), 'type doesnt match');
        self::assertEquals($expectedNullable, $definition->isNullable(), 'nullable flag doesnt match');
        self::assertEquals($expectedSqlDefinition, $definition->getSQLDefinition(), 'SQL definition doesnt match');
    }

    /**
     * @return Generator<string,array<mixed>>
     */
    public function tableColsDataProvider(): Generator
    {
        yield 'DECIMAL' => [
            'DECIMAL(29,0)', // sql which goes to table
            'NUMERIC(29)', // expected sql from getSQLDefinition
            'NUMERIC', // expected type from db
            null, // default
            '29', // length
            true, // nullable
        ];
        yield 'NUMERIC' => [
            'NUMERIC(30,2)', // sql which goes to table
            'NUMERIC(30,2)', // expected sql from getSQLDefinition
            'NUMERIC', // expected type from db
            null, // default
            '30,2', // length
            true, // nullable
        ];
        yield 'BIGDECIMAL' => [
            'BIGDECIMAL(29,0)', // sql which goes to table
            'BIGNUMERIC(29)', // expected sql from getSQLDefinition
            'BIGNUMERIC', // expected type from db
            null, // default
            '29', // length
            true, // nullable
        ];
        yield 'BIGNUMERIC' => [
            'BIGNUMERIC(30,2)', // sql which goes to table
            'BIGNUMERIC(30,2)', // expected sql from getSQLDefinition
            'BIGNUMERIC', // expected type from db
            null, // default
            '30,2', // length
            true, // nullable
        ];
        yield 'INT' => [
            'INT', // sql which goes to table
            'INT64', // expected sql from getSQLDefinition
            'INT64', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'INTEGER' => [
            'INTEGER', // sql which goes to table
            'INT64', // expected sql from getSQLDefinition
            'INT64', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'BIGINT' => [
            'BIGINT', // sql which goes to table
            'INT64', // expected sql from getSQLDefinition
            'INT64', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'SMALLINT' => [
            'SMALLINT', // sql which goes to table
            'INT64', // expected sql from getSQLDefinition
            'INT64', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'TINYINT' => [
            'TINYINT', // sql which goes to table
            'INT64', // expected sql from getSQLDefinition
            'INT64', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'BYTEINT' => [
            'BYTEINT', // sql which goes to table
            'INT64', // expected sql from getSQLDefinition
            'INT64', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'FLOAT64' => [
            'FLOAT64', // sql which goes to table
            'FLOAT64', // expected sql from getSQLDefinition
            'FLOAT64', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'VARCHAR' => [
            'STRING', // sql which goes to table
            'STRING', // expected sql from getSQLDefinition
            'STRING', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'STRING' => [
            'STRING(16777216)', // sql which goes to table
            'STRING(16777216)', // expected sql from getSQLDefinition
            'STRING', // expected type from db
            null, // default
            '16777216', // length
            true, // nullable
        ];
        yield 'BOOL' => [
            'BOOL', // sql which goes to table
            'BOOL', // expected sql from getSQLDefinition
            'BOOL', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'DATE' => [
            'DATE', // sql which goes to table
            'DATE', // expected sql from getSQLDefinition
            'DATE', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'DATETIME' => [
            'DATETIME', // sql which goes to table
            'DATETIME', // expected sql from getSQLDefinition
            'DATETIME', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'TIME' => [
            'TIME', // sql which goes to table
            'TIME', // expected sql from getSQLDefinition
            'TIME', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'TIMESTAMP' => [
            'TIMESTAMP', // sql which goes to table
            'TIMESTAMP', // expected sql from getSQLDefinition
            'TIMESTAMP', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'BYTES' => [
            'BYTES(8388608)', // sql which goes to table
            'BYTES(8388608)', // expected sql from getSQLDefinition
            'BYTES', // expected type from db
            null, // default
            '8388608', // length
            true, // nullable
        ];
        yield 'GEOGRAPHY' => [
            'GEOGRAPHY', // sql which goes to table
            'GEOGRAPHY', // expected sql from getSQLDefinition
            'GEOGRAPHY', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'INTERVAL' => [
            'INTERVAL', // sql which goes to table
            'INTERVAL', // expected sql from getSQLDefinition
            'INTERVAL', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'JSON' => [
            'JSON', // sql which goes to table
            'JSON', // expected sql from getSQLDefinition
            'JSON', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
    }
}
