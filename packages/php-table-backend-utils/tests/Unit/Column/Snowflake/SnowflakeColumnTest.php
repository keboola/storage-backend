<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column\Snowflake;

use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use PHPUnit\Framework\TestCase;

/**
 * @covers SnowflakeColumn
 */
class SnowflakeColumnTest extends TestCase
{
    public function testCreateGenericColumn(): void
    {
        $col = SnowflakeColumn::createGenericColumn('myCol');
        self::assertEquals('myCol', $col->getColumnName());
        self::assertEquals('VARCHAR NOT NULL DEFAULT \'\'', $col->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('VARCHAR', $col->getColumnDefinition()->getType());
        self::assertEquals('\'\'', $col->getColumnDefinition()->getDefault());
    }

    public function testCreateFromDB(): void
    {
        $data = [
            'Row' => '1',
            'name' => 'first_name',
            'type' => 'VARCHAR(16777216)',
            'kind' => 'COLUMN',
            'null?' => 'Y',
            'default' => '',
            'primary key' => 'N',
            'unique key' => '',
            'check' => '',
            'expression' => '',
            'comment' => '',
            'policy name' => '',
        ];

        $column = SnowflakeColumn::createFromDB($data);
        self::assertEquals('first_name', $column->getColumnName());
        self::assertEquals('VARCHAR (16777216)', $column->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('VARCHAR', $column->getColumnDefinition()->getType());
        self::assertEquals('', $column->getColumnDefinition()->getDefault());
        self::assertEquals('16777216', $column->getColumnDefinition()->getLength());
    }

    public function testVarcharCollate(): void
    {
        $data = [
            'Row' => '1',
            'name' => 'first_name',
            'type' => 'VARCHAR(16777216) COLLATE \'cs\'',
            'kind' => 'COLUMN',
            'null?' => 'Y',
            'default' => '',
            'primary key' => 'N',
            'unique key' => '',
            'check' => '',
            'expression' => '',
            'comment' => '',
            'policy name' => '',
        ];

        $column = SnowflakeColumn::createFromDB($data);
        var_dump($column->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('first_name', $column->getColumnName());
        self::assertEquals('VARCHAR (16777216)', $column->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('VARCHAR', $column->getColumnDefinition()->getType());
        self::assertEquals('', $column->getColumnDefinition()->getDefault());
        self::assertEquals('16777216', $column->getColumnDefinition()->getLength());
    }

    public function testCreateFromDBNotNullInt(): void
    {
        $data = [
            'Row' => '1',
            'name' => 'age',
            'type' => 'NUMBER(38,0)',
            'kind' => 'COLUMN',
            'null?' => 'N',
            'default' => '18',
            'primary key' => 'N',
            'unique key' => '',
            'check' => '',
            'expression' => '',
            'comment' => '',
            'policy name' => '',
        ];

        $column = SnowflakeColumn::createFromDB($data);
        self::assertEquals('age', $column->getColumnName());
        self::assertEquals('NUMBER (38,0) NOT NULL DEFAULT 18', $column->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('NUMBER', $column->getColumnDefinition()->getType());
        self::assertEquals('18', $column->getColumnDefinition()->getDefault());
        self::assertEquals('38,0', $column->getColumnDefinition()->getLength());
    }

    public function testCreateTimestampColumn(): void
    {
        $col = SnowflakeColumn::createTimestampColumn();
        $this->assertEquals('_timestamp', $col->getColumnName());
        $this->assertEquals('TIMESTAMP_NTZ', $col->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('TIMESTAMP_NTZ', $col->getColumnDefinition()->getType());
        $this->assertEquals(null, $col->getColumnDefinition()->getDefault());
        $this->assertEquals(null, $col->getColumnDefinition()->getLength());
    }

    public function testCreateTimestampColumnNonDefaultName(): void
    {
        $col = SnowflakeColumn::createTimestampColumn('_kbc_timestamp');
        $this->assertEquals('_kbc_timestamp', $col->getColumnName());
        $this->assertEquals('TIMESTAMP_NTZ', $col->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('TIMESTAMP_NTZ', $col->getColumnDefinition()->getType());
        $this->assertEquals(null, $col->getColumnDefinition()->getDefault());
        $this->assertEquals(null, $col->getColumnDefinition()->getLength());
    }
}
