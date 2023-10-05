<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column\Bigquery;

use Generator;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\SQLtoRestDatatypeConverter;
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
            'name' => 'first_name',
            'type' => 'STRING',
            'mode' => 'NULLABLE',
            'fields' => [],
            'description' => 'string',
            'maxLength' => '50',
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
            'name' => 'age',
            'type' => 'NUMERIC',
            'mode' => 'REQUIRED',
            'fields' => [],
            'description' => 'string',
            'maxLength' => 'string',
            'precision' => '38',
            'scale' => '9',
            'roundingMode' => 'ROUNDING_MODE_UNSPECIFIED',
            'collation' => 'string',
            'defaultValueExpression' => '18',
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
            'dataToExtend' => [
                'mode' => 'REPEATED',
                'type' => 'INTEGER',
            ],
            'expectedSqlDefinition' => 'ARRAY<INTEGER>',
            'expectedType' => 'ARRAY',
            'expectedLength' => 'INTEGER',
        ];

        yield 'array bytes' => [
            'dataToExtend' => [
                'mode' => 'REPEATED',
                'type' => 'BYTES',
                'maxLength' => '5',
            ],
            'expectedSqlDefinition' => 'ARRAY<BYTES(5)>',
            'expectedType' => 'ARRAY',
            'expectedLength' => 'BYTES(5)',
        ];

        yield 'array struct array int' => [
            'dataToExtend' => [
                'mode' => 'REPEATED',
                'type' => 'RECORD',
                'fields' => [
                    [
                        'name' => 'x',
                        'type' => 'INTEGER',
                        'mode' => 'REPEATED',
                    ],
                ],
            ],
            'expectedSqlDefinition' => 'ARRAY<STRUCT<x ARRAY<INTEGER>>>',
            'expectedType' => 'ARRAY',
            'expectedLength' => 'STRUCT<x ARRAY<INTEGER>>',
        ];

        yield 'array struct array struct array int' => [
            'dataToExtend' => [
                'mode' => 'REPEATED',
                'type' => 'RECORD',
                'fields' => [
                    [
                        'name' => 'x',
                        'type' => 'RECORD',
                        'mode' => 'REPEATED',
                        'fields' => [
                            [
                                'name' => 'x_z',
                                'type' => 'INTEGER',
                                'mode' => 'REPEATED',
                            ],
                        ],
                    ],
                ],
            ],
            'expectedSqlDefinition' => 'ARRAY<STRUCT<x ARRAY<STRUCT<x_z ARRAY<INTEGER>>>>>',
            'expectedType' => 'ARRAY',
            'expectedLength' => 'STRUCT<x ARRAY<STRUCT<x_z ARRAY<INTEGER>>>>',
        ];

        yield 'struct int' => [
            'dataToExtend' => [
                'type' => 'RECORD',
                'fields' => [
                    [
                        'name' => 'y',
                        'type' => 'INTEGER',
                    ],
                ],
            ],
            'expectedSqlDefinition' => 'STRUCT<y INTEGER>',
            'expectedType' => 'STRUCT',
            'expectedLength' => 'y INTEGER',
        ];

        yield 'struct bytes' => [
            'dataToExtend' => [
                'type' => 'RECORD',
                'fields' => [
                    [
                        'name' => 'xZ',
                        'type' => 'BYTES',
                        'maxLength' => '10',
                    ],
                ],
            ],
            'expectedSqlDefinition' => 'STRUCT<xZ BYTES(10)>',
            'expectedType' => 'STRUCT',
            'expectedLength' => 'xZ BYTES(10)',
        ];

        yield 'struct of struct' => [
            'dataToExtend' => [
                'type' => 'RECORD',
                'fields' => [
                    [
                        'name' => 'x',
                        'type' => 'RECORD',
                        'fields' => [
                            [
                                'name' => 'y',
                                'type' => 'INTEGER',
                            ],
                            [
                                'name' => 'z',
                                'type' => 'INTEGER',
                            ],
                        ],
                    ],
                ],
            ],
            'expectedSqlDefinition' => 'STRUCT<x STRUCT<y INTEGER,z INTEGER>>',
            'expectedType' => 'STRUCT',
            'expectedLength' => 'x STRUCT<y INTEGER,z INTEGER>',
        ];

        yield 'struct array' => [
            'dataToExtend' => [
                'type' => 'RECORD',
                'fields' => [
                    [
                        'name' => 'x_y',
                        'type' => 'INTEGER',
                        'mode' => 'REPEATED',
                    ],
                ],
            ],
            'expectedSqlDefinition' => 'STRUCT<x_y ARRAY<INTEGER>>',
            'expectedType' => 'STRUCT',
            'expectedLength' => 'x_y ARRAY<INTEGER>',
        ];
    }

    /**
     * @param array<mixed> $dataToExtend
     * @dataProvider repeatedDataTypeProvider
     */
    public function testCreateArrayColumn(
        array $dataToExtend,
        string $expectedSqlDefinition,
        string $expectedType,
        string $expectedLength
    ): void {
        $data = [
            'name' => 'age',
        ];

        $data = array_merge($data, $dataToExtend);

        //@phpstan-ignore-next-line
        $column = BigqueryColumn::createFromDB($data);
        self::assertEquals('age', $column->getColumnName());
        self::assertEquals($expectedSqlDefinition, $column->getColumnDefinition()->getSQLDefinition());
        self::assertEquals($expectedType, $column->getColumnDefinition()->getType());
        self::assertEquals('', $column->getColumnDefinition()->getDefault());
        self::assertEquals($expectedLength, $column->getColumnDefinition()->getLength());
        $this->assertEqualsCanonicalizing($data, SQLtoRestDatatypeConverter::convertColumnToRestFormat($column));
    }
}
