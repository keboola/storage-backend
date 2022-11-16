<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column\Teradata;

use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use PHPUnit\Framework\TestCase;

/**
 * @covers TeradataColumn
 */
class TeradataColumnTest extends TestCase
{
    public function testCreateGenericColumn(): void
    {
        $col = TeradataColumn::createGenericColumn('myCol');
        self::assertEquals('myCol', $col->getColumnName());
        self::assertEquals(
            'VARCHAR (32000) NOT NULL DEFAULT \'\' CHARACTER SET UNICODE',
            $col->getColumnDefinition()->getSQLDefinition()
        );
        self::assertEquals('VARCHAR', $col->getColumnDefinition()->getType());
        self::assertEquals('\'\'', $col->getColumnDefinition()->getDefault());
    }

    public function testCreateFromDB(): void
    {
        $data = [];

        $column = TeradataColumn::createFromDB($data);
        self::assertEquals('tmp', $column->getColumnName());
        self::assertEquals(
            'VARCHAR (32000) NOT NULL DEFAULT \'\' CHARACTER SET UNICODE',
            $column->getColumnDefinition()->getSQLDefinition()
        );
        self::assertEquals('VARCHAR', $column->getColumnDefinition()->getType());
        self::assertEquals('\'\'', $column->getColumnDefinition()->getDefault());
        self::assertEquals('32000', $column->getColumnDefinition()->getLength());
    }

    public function testCreateFromDBNotNullInt(): void
    {
        $data = [];

        $column = TeradataColumn::createFromDB($data);
        self::assertEquals('tmp', $column->getColumnName());
        self::assertEquals(
            'VARCHAR (32000) NOT NULL DEFAULT \'\' CHARACTER SET UNICODE',
            $column->getColumnDefinition()->getSQLDefinition()
        );
        self::assertEquals('VARCHAR', $column->getColumnDefinition()->getType());
        self::assertEquals('\'\'', $column->getColumnDefinition()->getDefault());
        self::assertEquals('32000', $column->getColumnDefinition()->getLength());
    }

    public function testCreateTimestampColumn(): void
    {
        $col = TeradataColumn::createTimestampColumn();
        $this->assertEquals('_timestamp', $col->getColumnName());
        $this->assertEquals('TIMESTAMP (6)', $col->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('TIMESTAMP', $col->getColumnDefinition()->getType());
        $this->assertEquals(null, $col->getColumnDefinition()->getDefault());
        $this->assertEquals(null, $col->getColumnDefinition()->getLength());
    }

    public function testCreateTimestampColumnNonDefaultName(): void
    {
        $col = TeradataColumn::createTimestampColumn('_kbc_timestamp');
        $this->assertEquals('_kbc_timestamp', $col->getColumnName());
        $this->assertEquals('TIMESTAMP (6)', $col->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('TIMESTAMP', $col->getColumnDefinition()->getType());
        $this->assertEquals(null, $col->getColumnDefinition()->getDefault());
        $this->assertEquals(null, $col->getColumnDefinition()->getLength());
    }
}
