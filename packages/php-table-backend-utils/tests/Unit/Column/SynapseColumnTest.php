<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column;

use Keboola\Datatype\Definition\Synapse;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use PHPUnit\Framework\TestCase;

/**
 * @covers SynapseColumn
 */
class SynapseColumnTest extends TestCase
{
    public function testCreateFromDBSimpleType(): void
    {
        $col = SynapseColumn::createFromDB([
            'column_name' => 'myCol',
            'column_type' => Synapse::TYPE_DATETIME,
            'column_precision' => 'whatever',
            'column_length' => 'whatever',
            'column_scale' => 'whatever',
            'column_is_nullable' => 'true',
            'column_default' => '(NOW())',
        ]);
        $this->assertEquals('myCol', $col->getColumnName());
        $this->assertEquals('DATETIME NOT NULL DEFAULT NOW()', $col->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('DATETIME', $col->getColumnDefinition()->getType());
        $this->assertEquals('NOW()', $col->getColumnDefinition()->getDefault());
        $this->assertEquals(null, $col->getColumnDefinition()->getLength());
    }

    public function testCreateFromDBLengthType(): void
    {
        $col = SynapseColumn::createFromDB([
            'column_name' => 'myCol',
            'column_type' => Synapse::TYPE_NVARCHAR,
            'column_precision' => 'whatever',
            'column_length' => '4000',
            'column_scale' => 'whatever',
            'column_is_nullable' => 'true',
            'column_default' => null,
        ]);
        $this->assertEquals('myCol', $col->getColumnName());
        $this->assertEquals('NVARCHAR(2000) NOT NULL', $col->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('NVARCHAR', $col->getColumnDefinition()->getType());
        $this->assertEquals('', $col->getColumnDefinition()->getDefault());
        $this->assertEquals('2000', $col->getColumnDefinition()->getLength());
    }

    public function testCreateFromDBComplexLengthType(): void
    {
        $col = SynapseColumn::createFromDB([
            'column_name' => 'myCol',
            'column_type' => Synapse::TYPE_DECIMAL,
            'column_precision' => '20',
            'column_length' => 'whatever',
            'column_scale' => '10',
            'column_is_nullable' => 'true',
            'column_default' => '((1))',
        ]);
        $this->assertEquals('myCol', $col->getColumnName());
        $this->assertEquals('DECIMAL(20,10) NOT NULL DEFAULT 1', $col->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('DECIMAL', $col->getColumnDefinition()->getType());
        $this->assertEquals('1', $col->getColumnDefinition()->getDefault());
        $this->assertEquals('20,10', $col->getColumnDefinition()->getLength());
    }

    public function testCreateGenericColumn(): void
    {
        $col = SynapseColumn::createGenericColumn('myCol');
        $this->assertEquals('myCol', $col->getColumnName());
        $this->assertEquals('NVARCHAR(4000) NOT NULL DEFAULT \'\'', $col->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('NVARCHAR', $col->getColumnDefinition()->getType());
        $this->assertEquals('\'\'', $col->getColumnDefinition()->getDefault());
        $this->assertEquals('4000', $col->getColumnDefinition()->getLength());
    }
}
