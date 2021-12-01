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
        self::assertEquals('VARCHAR NOT NULL', $col->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('VARCHAR', $col->getColumnDefinition()->getType());
        self::assertEquals('\'\'', $col->getColumnDefinition()->getDefault());
    }

    public function testCreateFromDB()
    {
        $data = [
            "Row" => "1",
            "name" => "first_name",
            "type" => "VARCHAR(16777216)",
            "kind" => "COLUMN",
            "null?" => "Y",
            "default" => "N",
            "primary key" => "N",
            "unique key" => "",
            "check" => "",
            "expression" => "",
            "comment" => "",
            "policy name" => "",
        ];

        $column = SnowflakeColumn::createFromDB($data);
        self::assertEquals('first_name', $column->getColumnName());
        self::assertEquals('VARCHAR(16777216)', $column->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('VARCHAR', $column->getColumnDefinition()->getType());
        self::assertEquals('', $column->getColumnDefinition()->getDefault());
        self::assertEquals('16777216', $column->getColumnDefinition()->getLength());
    }

    public function testCreateFromDBNotNullInt()
    {
        $data = [
            "Row" => "1",
            "name" => "age",
            "type" => "NUMBER(38,0)",
            "kind" => "COLUMN",
            "null?" => "N",
            "default" => "18",
            "primary key" => "N",
            "unique key" => "",
            "check" => "",
            "expression" => "",
            "comment" => "",
            "policy name" => "",
        ];

        $column = SnowflakeColumn::createFromDB($data);
        self::assertEquals('age', $column->getColumnName());
        self::assertEquals('NUMBER(38,0) NOT NULL', $column->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('NUMBER', $column->getColumnDefinition()->getType());
        self::assertEquals('18', $column->getColumnDefinition()->getDefault());
        self::assertEquals('38,0', $column->getColumnDefinition()->getLength());
    }
}
