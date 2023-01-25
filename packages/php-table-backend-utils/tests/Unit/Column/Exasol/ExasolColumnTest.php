<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column\Exasol;

use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use PHPUnit\Framework\TestCase;

/**
 * @covers ExasolColumn
 */
class ExasolColumnTest extends TestCase
{
    public function testCreateGenericColumn(): void
    {
        $col = ExasolColumn::createGenericColumn('myCol');
        self::assertEquals('myCol', $col->getColumnName());
        self::assertEquals("VARCHAR (2000000) DEFAULT '' NOT NULL", $col->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('VARCHAR', $col->getColumnDefinition()->getType());
        self::assertEquals('\'\'', $col->getColumnDefinition()->getDefault());
    }

    public function testCreateFromDB(): void
    {
        $data = [
            'COLUMN_NAME' => 'A',
            'COLUMN_IS_NULLABLE' => '1',
            'COLUMN_MAXSIZE' => '20',
            'COLUMN_NUM_PREC' => null,
            'COLUMN_NUM_SCALE' => null,
            'COLUMN_DEFAULT' => null,
            'COLUMN_TYPE' => 'VARCHAR(20) UTF8',
            'TYPE_NAME' => 'VARCHAR',
        ];

        $column = ExasolColumn::createFromDB($data);
        self::assertEquals('A', $column->getColumnName());
        self::assertEquals('VARCHAR (20)', $column->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('VARCHAR', $column->getColumnDefinition()->getType());
        self::assertEquals('', $column->getColumnDefinition()->getDefault());
        self::assertEquals('20', $column->getColumnDefinition()->getLength());
    }

    public function testCreateFromDBNotNullInt(): void
    {

        $data = [
            'COLUMN_NAME' => 'B',
            'COLUMN_IS_NULLABLE' => '0',
            'COLUMN_MAXSIZE' => '24',
            'COLUMN_NUM_PREC' => '24',
            'COLUMN_NUM_SCALE' => '4',
            'COLUMN_DEFAULT' => '10',
            'COLUMN_TYPE' => 'DECIMAL(24,4)',
            'TYPE_NAME' => 'DECIMAL',
        ];

        $column = ExasolColumn::createFromDB($data);
        self::assertEquals('B', $column->getColumnName());
        self::assertEquals('DECIMAL (24,4) DEFAULT 10 NOT NULL', $column->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('DECIMAL', $column->getColumnDefinition()->getType());
        self::assertEquals('10', $column->getColumnDefinition()->getDefault());
        self::assertEquals('24,4', $column->getColumnDefinition()->getLength());
    }

    public function testCreateTimestampColumn(): void
    {
        $col = ExasolColumn::createTimestampColumn();
        $this->assertEquals('_timestamp', $col->getColumnName());
        $this->assertEquals('TIMESTAMP', $col->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('TIMESTAMP', $col->getColumnDefinition()->getType());
        $this->assertEquals(null, $col->getColumnDefinition()->getDefault());
        $this->assertEquals(null, $col->getColumnDefinition()->getLength());
    }

    public function testCreateTimestampColumnNonDefaultName(): void
    {
        $col = ExasolColumn::createTimestampColumn('_kbc_timestamp');
        $this->assertEquals('_kbc_timestamp', $col->getColumnName());
        $this->assertEquals('TIMESTAMP', $col->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('TIMESTAMP', $col->getColumnDefinition()->getType());
        $this->assertEquals(null, $col->getColumnDefinition()->getDefault());
        $this->assertEquals(null, $col->getColumnDefinition()->getLength());
    }
}
