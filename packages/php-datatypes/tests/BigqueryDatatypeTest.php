<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use PHPUnit\Framework\TestCase;
use Throwable;

class BigqueryDatatypeTest extends TestCase
{
    /**
     * @return array<int, mixed[]>
     */
    public function invalidLengths(): array
    {
        return [
            ['string', 'notANumber'],
            ['string', '0'],
            ['string', 9223372036854775808],

            ['bytes', 'notANumber'],
            ['bytes', '0'],
            ['bytes', 9223372036854775808],

            ['decimal', 'notANumber'],
            ['decimal', '0'],
            ['decimal', '100'],
            ['decimal', '38,8'],
            ['decimal', '38'],

            ['numeric', 'notANumber'],
            ['numeric', '0'],
            ['numeric', '24,10'],
            ['numeric', '38,8'],

            ['bignumeric', 'notANumber'],
            ['bignumeric', '0'],
            ['bignumeric', '100'],
            ['bignumeric', '75,30'],

            ['bigdecimal', 'notANumber'],
            ['bigdecimal', '0'],
            ['bigdecimal', '78,10'],
            ['bigdecimal', '75,30'],
            ['bigdecimal', '78'],

            ['bool', 'anyLength'],
            ['bytes', 'anyLength'],
            ['date', 'anyLength'],
            ['datetime', 'anyLength'],
            ['time', 'anyLength'],
            ['timestamp', 'anyLength'],
            ['geography', 'anyLength'],
            ['interval', 'anyLength'],
            ['json', 'anyLength'],
            ['int64', 'anyLength'],
            ['int', 'anyLength'],
            ['smallint', 'anyLength'],
            ['integer', 'anyLength'],
            ['bigint', 'anyLength'],
            ['tinyint', 'anyLength'],
            ['byteint', 'anyLength'],
            ['float64', 'anyLength'],
        ];
    }

    /**
     * @dataProvider invalidLengths
     * @param string|int|null $length
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    public function testInvalidLengths(string $type, $length, ?array $extraOption = []): void
    {
        $options = $extraOption;
        $options['length'] = $length;

        $this->expectException(InvalidLengthException::class);
        new Bigquery($type, $options);
    }

    public function testBasetypes(): void
    {
        foreach (Bigquery::TYPES as $type) {
            $basetype = (new Bigquery($type))->getBasetype();
            switch ($type) {
                case 'INT64':
                case 'INT':
                case 'SMALLINT':
                case 'INTEGER':
                case 'BIGINT':
                case 'TINYINT':
                case 'BYTEINT':
                    $this->assertEquals(BaseType::INTEGER, $basetype);
                    break;
                case 'NUMERIC':
                case 'DECIMAL':
                case 'BIGNUMERIC':
                case 'BIGDECIMAL':
                    $this->assertEquals(BaseType::NUMERIC, $basetype);
                    break;
                case 'FLOAT64':
                    $this->assertEquals(BaseType::FLOAT, $basetype);
                    break;
                case 'BOOL':
                    $this->assertEquals(BaseType::BOOLEAN, $basetype);
                    break;
                case 'DATE':
                    $this->assertEquals(BaseType::DATE, $basetype);
                    break;
                case 'DATETIME':
                case 'TIME':
                case 'TIMESTAMP':
                    $this->assertEquals(BaseType::TIMESTAMP, $basetype);
                    break;
                default:
                    $this->assertEquals(BaseType::STRING, $basetype);
                    break;
            }
        }
    }

    public function testInvalidOption(): void
    {
        try {
            new Bigquery('numeric', ['myoption' => 'value']);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidOptionException::class, get_class($e));
        }
    }

    public function testInvalidType(): void
    {
        $this->expectException(InvalidTypeException::class);
        new Bigquery('UNKNOWN');
    }

    public function testGetTypeByBasetype(): void
    {
        $this->assertSame('BOOL', Bigquery::getTypeByBasetype('BOOLEAN'));

        $this->assertSame('STRING', Bigquery::getTypeByBasetype('STRING'));

        // not only upper case
        $this->assertSame('BOOL', Bigquery::getTypeByBasetype('Boolean'));

        $this->expectException(InvalidTypeException::class);
        $this->expectExceptionMessage('Base type "FOO" is not valid.');
        Bigquery::getTypeByBasetype('foo');
    }

    /**
     * @return array<int, mixed[]>
     */
    public function expectedSqlDefinitions(): array
    {
        $tests = [];

        foreach (['numeric', 'decimal'] as $type) {
            $tests[] = [
                $type,
                ['length' => ''],
                $type,
            ];
            $tests[] = [
                $type,
                [],
                $type,
            ];
            $tests[] = [
                $type,
                ['length' => '30,2'],
                $type . '(30,2)',
            ];
            $tests[] = [
                $type,
                ['length' => '30,2', 'default' => '10.00'],
                $type . '(30,2) DEFAULT 10.00',
            ];
            $tests[] = [
                $type,
                ['length' => '30,2', 'default' => '10.00', 'nullable' => false],
                $type . '(30,2) DEFAULT 10.00 NOT NULL',
            ];
        }

        foreach (['bignumeric', 'bigdecimal'] as $type) {
            $tests[] = [
                $type,
                ['length' => ''],
                $type,
            ];
            $tests[] = [
                $type,
                [],
                $type,
            ];
            $tests[] = [
                $type,
                ['length' => '76,38'],
                $type . '(76,38)',
            ];
            $tests[] = [
                $type,
                ['length' => '76,38', 'default' => '10.00'],
                $type . '(76,38) DEFAULT 10.00',
            ];
            $tests[] = [
                $type,
                ['length' => '76,38', 'default' => '10.00', 'nullable' => false],
                $type . '(76,38) DEFAULT 10.00 NOT NULL',
            ];
        }

        foreach (['bytes', 'string'] as $type) {
            $tests[] = [
                $type,
                ['length' => ''],
                $type,
            ];
            $tests[] = [
                $type,
                [],
                $type,
            ];
            $tests[] = [
                $type,
                ['default' => '\'\'', 'nullable' => false],
                $type . ' DEFAULT \'\' NOT NULL',
            ];
            $tests[] = [
                $type,
                ['default' => ''],
                $type,
            ];
            $tests[] = [
                $type,
                ['default' => '', 'nullable' => false],
                $type . ' NOT NULL',
            ];
            $tests[] = [
                $type,
                ['length' => '1000'],
                $type . '(1000)',
            ];
        }
        // todo mozno resit viac tu validaciu v zanoreni
        $tests[] = [
            'array',
            ['length' => 'INT64'],
            'array<INT64>',
        ];

        $tests[] = [
            'array',
            ['length' => 'BYTES(5)'],
            'array<BYTES(5)>',
        ];

        $tests[] = [
            'array',
            ['length' => 'STRUCT<INT64, INT64>'],
            'array<STRUCT<INT64, INT64>>',
        ];

        $tests[] = [
            'array',
            ['length' => 'STRUCT<ARRAY<INT64>>'],
            'array<STRUCT<ARRAY<INT64>>>',
        ];

        $tests[] = [
            'struct',
            ['length' => 'INT64'],
            'struct<INT64>',
        ];

        $tests[] = [
            'struct',
            ['length' => 'x BYTES(10)'],
            'struct<x BYTES(10)>',
        ];

        $tests[] = [
            'struct',
            ['length' => 'x STRUCT<y INT64, z INT64>'],
            'struct<x STRUCT<y INT64, z INT64>>',
        ];

        $tests[] = [
            'struct',
            ['length' => 'inner_array ARRAY<INT64>'],
            'struct<inner_array ARRAY<INT64>>',
        ];

        return $tests;
    }

    /**
     * @dataProvider expectedSqlDefinitions
     */
    public function testSqlDefinition(string $type, ?array $options, string $expectedDefinition): void
    {
        $definition = new Bigquery($type, $options);
        self::assertEquals($expectedDefinition, $definition->getSQLDefinition());
    }

    /**
     * @return array<int, array<int, int|string|null>>
     */
    public function validLengths(): array
    {
        return [
            ['bytes', null],
            ['bytes', ''],
            ['bytes', '1'],
            ['bytes', '42'],
            ['bytes', 9223372036854775807],
            ['bytes', '9223372036854775807'],

            ['string', null],
            ['string', ''],
            ['string', '1'],
            ['string', '42'],
            ['string', '9223372036854775807'],

            ['numeric', null],
            ['numeric', ''],
            ['numeric', '30,2'],
            ['numeric', '38,9'],
            ['numeric', '25'],

            ['bignumeric', null],
            ['bignumeric', ''],
            ['bignumeric', '38,9'],
            ['bignumeric', '76,38'],

            ['bool', null],
            ['date', null],
            ['datetime', null],
            ['time', null],
            ['timestamp', null],
            ['geography', null],
            ['interval', null],
            ['json', null],
            ['int64', null],
            ['int', null],
            ['smallint', null],
            ['integer', null],
            ['bigint', null],
            ['tinyint', null],
            ['byteint', null],
            ['float64', null],
        ];
    }

    /**
     * @dataProvider validLengths
     * @param string|int|null $length
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    public function testValidLengths(string $type, $length): void
    {
        $options = [];
        if ($length !== null) {
            $options['length'] = $length;
        }
        new Bigquery($type, $options);
        $this->expectNotToPerformAssertions();
    }

    public function testFieldAsArray(): void
    {
        $def = new Bigquery('ARRAY', [
            'fieldAsArray' => [
                'name' => 'test',
                'type' => 'STRING',
            ],
        ]);

        $this->assertSame([
            'name' => 'test',
            'type' => 'STRING',
        ], $def->getFieldAsArray());
    }
}
