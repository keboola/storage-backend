<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column\Bigquery;

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
}
