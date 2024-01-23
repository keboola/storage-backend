<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column\Bigquery\Parser;

use Generator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\SQLtoRestDatatypeConverter;
use PHPUnit\Framework\TestCase;

class SQLtoRestDatatypeConverterTest extends TestCase
{
    public function definitions(): Generator
    {
        yield 'myCol STRING' => [
            'type' => Bigquery::TYPE_STRING,
            'length' => null,
            'default' => null,
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'STRING',
            ],
        ];
        yield 'myCol STRING NOT NULL' => [
            'type' => Bigquery::TYPE_STRING,
            'length' => null,
            'default' => null,
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'STRING',
                'mode' => 'REQUIRED',
            ],
        ];
        yield 'myCol STRING DEFAULT \'test\'' => [
            'type' => Bigquery::TYPE_STRING,
            'length' => null,
            'default' => '\'test\'',
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'STRING',
                'defaultValueExpression' => '\'test\'',
            ],
        ];
        yield 'STRING(123544646)' => [
            'type' => Bigquery::TYPE_STRING,
            'length' => '123544646',
            'default' => null,
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'STRING',
                'maxLength' => '123544646',
            ],
        ];
        yield 'STRING(123544646) NOT NULL' => [
            'type' => Bigquery::TYPE_STRING,
            'length' => '123544646',
            'default' => null,
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'STRING',
                'maxLength' => '123544646',
                'mode' => 'REQUIRED',
            ],
        ];
        yield 'STRING(123544646) DEFAULT \'test\'' => [
            'type' => Bigquery::TYPE_STRING,
            'length' => '123544646',
            'default' => '\'test\'',
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'STRING',
                'maxLength' => '123544646',
                'defaultValueExpression' => '\'test\'',
            ],
        ];
        yield 'STRING(123544646) DEFAULT \'test\' NOT NULL' => [
            'type' => Bigquery::TYPE_STRING,
            'length' => '123544646',
            'default' => '\'test\'',
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'STRING',
                'maxLength' => '123544646',
                'mode' => 'REQUIRED',
                'defaultValueExpression' => '\'test\'',
            ],
        ];
        yield 'NUMERIC(10,9)' => [
            'type' => Bigquery::TYPE_NUMERIC,
            'length' => '10,9',
            'default' => null,
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'NUMERIC',
                'precision' => '10',
                'scale' => '9',
            ],
        ];
        yield 'NUMERIC(10,9) NOT NULL' => [
            'type' => Bigquery::TYPE_NUMERIC,
            'length' => '10,9',
            'default' => null,
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'NUMERIC',
                'precision' => '10',
                'scale' => '9',
                'mode' => 'REQUIRED',
            ],
        ];
        yield 'NUMERIC(10,9) DEFAULT \'test\'' => [
            'type' => Bigquery::TYPE_NUMERIC,
            'length' => '10,9',
            'default' => '\'test\'',
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'NUMERIC',
                'precision' => '10',
                'scale' => '9',
                'defaultValueExpression' => '\'test\'',
            ],
        ];
        yield 'NUMERIC(10,9) DEFAULT \'test\' NOT NULL' => [
            'type' => Bigquery::TYPE_NUMERIC,
            'length' => '10,9',
            'default' => '\'test\'',
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'NUMERIC',
                'precision' => '10',
                'scale' => '9',
                'mode' => 'REQUIRED',
                'defaultValueExpression' => '\'test\'',
            ],
        ];
        yield 'NUMERIC(10)' => [
            'type' => Bigquery::TYPE_NUMERIC,
            'length' => '10',
            'default' => null,
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'NUMERIC',
                'precision' => '10',
            ],
        ];
        yield 'NUMERIC(10) NOT NULL' => [
            'type' => Bigquery::TYPE_NUMERIC,
            'length' => '10',
            'default' => null,
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'NUMERIC',
                'precision' => '10',
                'mode' => 'REQUIRED',
            ],
        ];
        yield 'NUMERIC(10) DEFAULT \'test\'' => [
            'type' => Bigquery::TYPE_NUMERIC,
            'length' => '10',
            'default' => '\'test\'',
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'NUMERIC',
                'precision' => '10',
                'defaultValueExpression' => '\'test\'',
            ],
        ];
        yield 'NUMERIC(10) DEFAULT \'test\' NOT NULL' => [
            'type' => Bigquery::TYPE_NUMERIC,
            'length' => '10',
            'default' => '\'test\'',
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'NUMERIC',
                'precision' => '10',
                'mode' => 'REQUIRED',
                'defaultValueExpression' => '\'test\'',
            ],
        ];
        yield 'NUMERIC' => [
            'type' => Bigquery::TYPE_NUMERIC,
            'length' => null,
            'default' => null,
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'NUMERIC',
            ],
        ];
        yield 'NUMERIC NOT NULL' => [
            'type' => Bigquery::TYPE_NUMERIC,
            'length' => null,
            'default' => null,
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'NUMERIC',
                'mode' => 'REQUIRED',
            ],
        ];
        yield 'NUMERIC DEFAULT \'test\'' => [
            'type' => Bigquery::TYPE_NUMERIC,
            'length' => null,
            'default' => '\'test\'',
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'NUMERIC',
                'defaultValueExpression' => '\'test\'',
            ],
        ];
        yield 'NUMERIC DEFAULT \'test\' NOT NULL' => [
            'type' => Bigquery::TYPE_NUMERIC,
            'length' => null,
            'default' => '\'test\'',
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'NUMERIC',
                'mode' => 'REQUIRED',
                'defaultValueExpression' => '\'test\'',
            ],
        ];
        yield 'myCol STRUCT<' => [ // it will fail in BQ but not in parser
            'type' => Bigquery::TYPE_STRUCT,
            'length' => '<',
            'default' => null,
            'nullable' => false,
            'expected' => ['name' => 'myCol', 'type' => 'RECORD', 'fields' => []],
        ];
        yield 'myCol STRUCT<>' => [ // it will fail in BQ but not in parser
            'type' => Bigquery::TYPE_STRUCT,
            'length' => '<>',
            'default' => null,
            'nullable' => false,
            'expected' => ['name' => 'myCol', 'type' => 'RECORD', 'fields' => []],
        ];
        yield 'ARRAY<STRING>' => [
            'type' => Bigquery::TYPE_ARRAY,
            'length' => 'STRING',
            'default' => null,
            'nullable' => false,
            'expected' => ['name' => 'myCol', 'type' => 'STRING', 'mode' => 'REPEATED',],
        ];
        yield 'ARRAY<STRING(123245)>' => [
            'type' => Bigquery::TYPE_ARRAY,
            'length' => 'STRING(123245)',
            'default' => null,
            'nullable' => false,
            'expected' => ['name' => 'myCol', 'type' => 'STRING', 'mode' => 'REPEATED', 'maxLength' => '123245',],
        ];
        yield 'ARRAY<NUMERIC(10,10)>' => [
            'type' => Bigquery::TYPE_ARRAY,
            'length' => 'STRING(123245)',
            'default' => null,
            'nullable' => false,
            'expected' => ['name' => 'myCol', 'type' => 'STRING', 'mode' => 'REPEATED', 'maxLength' => '123245',],
        ];
        yield 'ARRAY<STRUCT<x NUMERIC>>' => [
            'type' => Bigquery::TYPE_ARRAY,
            'length' => 'STRUCT<x NUMERIC>',
            'default' => null,
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'RECORD',
                'mode' => 'REPEATED',
                'fields' => ['0' => ['name' => 'x', 'type' => 'NUMERIC',],],
            ],
        ];
        yield 'ARRAY<STRUCT<x NUMERIC(10,10)>>' => [
            'type' => Bigquery::TYPE_ARRAY,
            'length' => 'STRUCT<x NUMERIC(10,10)>',
            'default' => null,
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'RECORD',
                'mode' => 'REPEATED',
                'fields' => ['0' => ['name' => 'x', 'type' => 'NUMERIC', 'precision' => '10', 'scale' => '10',],],
            ],
        ];
        //phpcs:ignore
        yield 'ARRAY<STRUCT<x NUMERIC(10,10),y ARRAY<NUMERIC(10,10)>, z STRUCT<x NUMERIC(10,10),y ARRAY<NUMERIC(10,10)>>>' => [
            'type' => Bigquery::TYPE_ARRAY,
            //phpcs:ignore
            'length' => 'STRUCT<x NUMERIC(10,10),y ARRAY<NUMERIC(10,10)>, z STRUCT<x NUMERIC(10,10),y ARRAY<NUMERIC(10,10)>>',
            'default' => null,
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'RECORD',
                'mode' => 'REPEATED',
                'fields' => [
                    '0' => ['name' => 'x', 'type' => 'NUMERIC', 'precision' => '10', 'scale' => '10',],
                    '1' => [
                        'name' => 'y',
                        'type' => 'NUMERIC',
                        'mode' => 'REPEATED',
                        'precision' => '10',
                        'scale' => '10',
                    ],
                    '2' => [
                        'name' => 'z',
                        'type' => 'RECORD',
                        'fields' => [
                            '0' => ['name' => 'x', 'type' => 'NUMERIC', 'precision' => '10', 'scale' => '10',],
                            '1' => [
                                'name' => 'y',
                                'type' => 'NUMERIC',
                                'mode' => 'REPEATED',
                                'precision' => '10',
                                'scale' => '10',
                            ],
                        ],
                    ],
                ],
            ],
        ];
        yield 'STRUCT<t NUMERIC(10,10)>' => [
            'type' => Bigquery::TYPE_STRUCT,
            'length' => 't NUMERIC(10,10)',
            'default' => null,
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'RECORD',
                'fields' => ['0' => ['name' => 't', 'type' => 'NUMERIC', 'precision' => '10', 'scale' => '10',],],
            ],
        ];
        yield 'STRUCT<t NUMERIC(10)>' => [
            'type' => Bigquery::TYPE_STRUCT,
            'length' => 't NUMERIC(10)',
            'default' => null,
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'RECORD',
                'fields' => ['0' => ['name' => 't', 'type' => 'NUMERIC', 'precision' => '10',],],
            ],
        ];
        yield 'STRUCT<t NUMERIC>' => [
            'type' => Bigquery::TYPE_STRUCT,
            'length' => 't NUMERIC',
            'default' => null,
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'RECORD',
                'fields' => ['0' => ['name' => 't', 'type' => 'NUMERIC',],],
            ],
        ];
        yield 'STRUCT<x STRUCT<y INTEGER,z INTEGER>>' => [
            'type' => Bigquery::TYPE_STRUCT,
            'length' => 'x STRUCT<y INTEGER,z INTEGER>',
            'default' => null,
            'nullable' => false,
            'expected' => [
                'name' => 'myCol',
                'type' => 'RECORD',
                'fields' => [
                    '0' => [
                        'name' => 'x',
                        'type' => 'RECORD',
                        'fields' => [
                            '0' => ['name' => 'y', 'type' => 'INTEGER',],
                            '1' => ['name' => 'z', 'type' => 'INTEGER',],
                        ],
                    ],
                ],
            ],
        ];
        // aliases
        yield 'INT' => [
            'type' => Bigquery::TYPE_INT,
            'length' => null,
            'default' => null,
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'INT64',
            ],
        ];
        yield 'SMALLINT' => [
            'type' => Bigquery::TYPE_SMALLINT,
            'length' => null,
            'default' => null,
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'INT64',
            ],
        ];
        yield 'BIGINT' => [
            'type' => Bigquery::TYPE_BIGINT,
            'length' => null,
            'default' => null,
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'INT64',
            ],
        ];
        yield 'TINYINT' => [
            'type' => Bigquery::TYPE_TINYINT,
            'length' => null,
            'default' => null,
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'INT64',
            ],
        ];
        yield 'BYTEINT' => [
            'type' => Bigquery::TYPE_BYTEINT,
            'length' => null,
            'default' => null,
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'INT64',
            ],
        ];
        yield 'DECIMAL' => [
            'type' => Bigquery::TYPE_DECIMAL,
            'length' => null,
            'default' => null,
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'NUMERIC',
            ],
        ];
        yield 'BIGDECIMAL' => [
            'type' => Bigquery::TYPE_BIGDECIMAL,
            'length' => null,
            'default' => null,
            'nullable' => true,
            'expected' => [
                'name' => 'myCol',
                'type' => 'BIGNUMERIC',
            ],
        ];
    }

    /**
     * @dataProvider definitions
     * @param array<mixed> $expected
     */
    public function test(
        string $type,
        ?string $length,
        ?string $default,
        bool $nullable,
        array $expected,
    ): void {
        $options = [
            'nullable' => $nullable,
        ];
        if ($length !== null) {
            $options['length'] = $length;
        }
        if ($default !== null) {
            $options['default'] = $default;
        }
        $col = new BigqueryColumn('myCol', new Bigquery(
            $type,
            $options,
        ));
        $rest = SQLtoRestDatatypeConverter::convertColumnToRestFormat($col);
        self::assertSame($expected, $rest);
    }

    public function definitionsErrors(): Generator
    {
        yield 'myCol ARRAY<>' => [
            'type' => Bigquery::TYPE_ARRAY,
            'length' => '',
            'expected' => 'Invalid column "myCol" definition "ARRAY". STRUCT|ARRAY type must have definition.',
        ];
        yield 'myCol STRUCT<>' => [
            'type' => Bigquery::TYPE_STRUCT,
            'length' => '',
            'expected' => 'Invalid column "myCol" definition "STRUCT". STRUCT|ARRAY type must have definition.',
        ];
        yield 'myCol ARRAY<<>' => [
            'type' => Bigquery::TYPE_ARRAY,
            'length' => '<',
            // phpcs:ignore
            'expected' => 'Invalid column "myCol" definition "ARRAY<<>". Unexpected token "<" for field "myCol". Name or type of field is expected.',
        ];
        yield 'myCol ARRAY<<>>' => [
            'type' => Bigquery::TYPE_ARRAY,
            'length' => '<>', // <> is added automatically
            // phpcs:ignore
            'expected' => 'Invalid column "myCol" definition "ARRAY<<>>". Unexpected token "<" for field "myCol". Name or type of field is expected.',
        ];
        yield 'myCol ARRAY<<xxx, xxx>>' => [
            'type' => Bigquery::TYPE_ARRAY,
            'length' => '<xxx, xxx>',
            // phpcs:ignore
            'expected' => 'Invalid column "myCol" definition "ARRAY<<xxx, xxx>>". Unexpected token "<" for field "myCol". Name or type of field is expected.',
        ];
        yield 'myCol ARRAY<STRING(123245>' => [
            'type' => Bigquery::TYPE_ARRAY,
            'length' => 'STRING(123245',
            // phpcs:ignore
            'expected' => 'Invalid column "myCol" definition "ARRAY<STRING(123245>". Unexpected token on position "18" in "(123245>". Closing parenthesis not found.',
        ];
        yield 'myCol ARRAY<STRING(123245) invalid' => [
            'type' => Bigquery::TYPE_ARRAY,
            'length' => 'STRING(123245) invalid',
            // phpcs:ignore
            'expected' => 'Invalid column "myCol" definition "ARRAY<STRING(123245) invalid>". Unexpected token on position "27" in "invalid>". Expected "," followed by next field or end of ARRAY|STRUCT.',
        ];
    }

    /**
     * @dataProvider definitionsErrors
     */
    public function testErrors(
        string $type,
        ?string $length,
        string $expectedError,
    ): void {
        $options = [
            'length' => $length,
        ];
        $col = new BigqueryColumn('myCol', new Bigquery(
            $type,
            $options,
        ));
        try {
            SQLtoRestDatatypeConverter::convertColumnToRestFormat($col);
            $this->fail('it should fail');
        } catch (InvalidLengthException $e) {
            $this->assertSame($expectedError, $e->getMessage());
        }
    }

    public function testLongName(): void
    {
        $options = [
            'nullable' => true,
            'length' => 'STRUCT<array_struct_array-int_array-string ARRAY<NUMERIC(10)>>',
        ];
        $col = new BigqueryColumn('array_struct_array-int_array-string', new Bigquery(
            Bigquery::TYPE_ARRAY,
            $options,
        ));
        $rest = SQLtoRestDatatypeConverter::convertColumnToRestFormat($col);
        self::assertSame([
            'name' => 'array_struct_array-int_array-string',
            'type' => 'RECORD',
            'mode' => 'REPEATED',
            'fields' => [
                '0' => [
                    'name' => 'array_struct_array-int_array-string',
                    'type' => 'NUMERIC',
                    'mode' => 'REPEATED',
                    'precision' => '10',
                ],
            ],
        ], $rest);
    }
}
