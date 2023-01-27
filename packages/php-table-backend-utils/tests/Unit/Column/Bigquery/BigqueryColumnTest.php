<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column\Bigquery;

use Generator;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use PHPUnit\Framework\TestCase;

/**
 * @covers BigqueryColumn
 */
class BigqueryColumnTest extends TestCase
{
    public function testCreateGenericColumn(): void
    {
        $col = BigqueryColumn::createGenericColumn('myCol');
        self::assertEquals('myCol', $col->getColumnName());
        self::assertEquals('STRING DEFAULT \'\' NOT NULL', $col->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('STRING', $col->getColumnDefinition()->getType());
        self::assertEquals('\'\'', $col->getColumnDefinition()->getDefault());
    }

    public function testCreateFromDB(): void
    {
        $data = [
            'table_catalog' => 'project',
            'table_schema' => 'bucket_dataset',
            'table_name' => 'table_name',
            'column_name' => 'first_name',
            'ordinal_position' => 1,
            'is_nullable' => 'YES',
            'data_type' => 'STRING(50)',
            'is_hidden' => 'NO',
            'is_system_defined' => 'NO',
            'is_partitioning_column' => 'NO',
            'clustering_ordinal_position' => null,
            'collation_name' => 'NULL',
            'column_default' => 'NULL',
            'rounding_mode' => null,
        ];

        $column = BigqueryColumn::createFromDB($data);
        self::assertEquals('first_name', $column->getColumnName());
        self::assertEquals('STRING(50)', $column->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('STRING', $column->getColumnDefinition()->getType());
        self::assertEquals('', $column->getColumnDefinition()->getDefault());
        self::assertEquals('50', $column->getColumnDefinition()->getLength());
    }

    public function testCreateFromDBNotNullInt(): void
    {
        $data = [
            'table_catalog' => 'project',
            'table_schema' => 'bucket_dataset',
            'table_name' => 'table_name',
            'column_name' => 'age',
            'ordinal_position' => 1,
            'is_nullable' => 'NO',
            'data_type' => 'NUMERIC(38,9)',
            'is_hidden' => 'NO',
            'is_system_defined' => 'NO',
            'is_partitioning_column' => 'NO',
            'clustering_ordinal_position' => null,
            'collation_name' => 'NULL',
            'column_default' => '18',
            'rounding_mode' => null,
        ];

        $column = BigqueryColumn::createFromDB($data);
        self::assertEquals('age', $column->getColumnName());
        self::assertEquals('NUMERIC(38,9) DEFAULT 18 NOT NULL', $column->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('NUMERIC', $column->getColumnDefinition()->getType());
        self::assertEquals('18', $column->getColumnDefinition()->getDefault());
        self::assertEquals('38,9', $column->getColumnDefinition()->getLength());
    }

    public function testCreateTimestampColumn(): void
    {
        $col = BigqueryColumn::createTimestampColumn();
        $this->assertEquals('_timestamp', $col->getColumnName());
        $this->assertEquals('TIMESTAMP', $col->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('TIMESTAMP', $col->getColumnDefinition()->getType());
        $this->assertEquals(null, $col->getColumnDefinition()->getDefault());
        $this->assertEquals(null, $col->getColumnDefinition()->getLength());
    }

    public function testCreateTimestampColumnNonDefaultName(): void
    {
        $col = BigqueryColumn::createTimestampColumn('_kbc_timestamp');
        $this->assertEquals('_kbc_timestamp', $col->getColumnName());
        $this->assertEquals('TIMESTAMP', $col->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('TIMESTAMP', $col->getColumnDefinition()->getType());
        $this->assertEquals(null, $col->getColumnDefinition()->getDefault());
        $this->assertEquals(null, $col->getColumnDefinition()->getLength());
    }

    public function repeatedDataTypeProvider(): Generator
    {
        yield 'array int' => [
            'ARRAY<INT64>',
            'ARRAY',
            'INT64',
        ];

        yield 'array bytes' => [
            'ARRAY<BYTES(5)>',
            'ARRAY',
            'BYTES(5)',
        ];

        yield 'array struct array int' => [
            'ARRAY<STRUCT<x ARRAY<INT64>>>',
            'ARRAY',
            'STRUCT<x ARRAY<INT64>>',
        ];

        yield 'array struct array struct array int' => [
            'ARRAY<STRUCT<x ARRAY<STRUCT<x_z ARRAY<INT64>>>>>',
            'ARRAY',
            'STRUCT<x ARRAY<STRUCT<x_z ARRAY<INT64>>>>',
        ];

        yield 'struct int' => [
            'STRUCT<INT64>',
            'STRUCT',
            'INT64',
        ];

        yield 'struct bytes' => [
            'STRUCT<xZ BYTES(10)>',
            'STRUCT',
            'xZ BYTES(10)',
        ];

        yield 'struct of struct' => [
            'STRUCT<x STRUCT<y INT64, z INT64>>',
            'STRUCT',
            'x STRUCT<y INT64, z INT64>',
        ];

        yield 'struct array' => [
            'STRUCT<x_y ARRAY<INT64>>',
            'STRUCT',
            'x_y ARRAY<INT64>',
        ];
    }

    /**
     * @dataProvider repeatedDataTypeProvider
     */
    public function testCreateArrayColumn(string $dataType, string $expectedType, string $expectedLength): void
    {
        $data = [
            'table_catalog' => 'project',
            'table_schema' => 'bucket_dataset',
            'table_name' => 'table_name',
            'column_name' => 'age',
            'ordinal_position' => 1,
            'is_nullable' => 'NO',
            'data_type' => $dataType,
            'is_hidden' => 'NO',
            'is_system_defined' => 'NO',
            'is_partitioning_column' => 'NO',
            'clustering_ordinal_position' => null,
            'collation_name' => 'NULL',
            'column_default' => '',
            'rounding_mode' => null,
        ];

        $column = BigqueryColumn::createFromDB($data);
        self::assertEquals('age', $column->getColumnName());
        self::assertEquals($dataType, $column->getColumnDefinition()->getSQLDefinition());
        self::assertEquals($expectedType, $column->getColumnDefinition()->getType());
        self::assertEquals('', $column->getColumnDefinition()->getDefault());
        self::assertEquals($expectedLength, $column->getColumnDefinition()->getLength());
    }
}
