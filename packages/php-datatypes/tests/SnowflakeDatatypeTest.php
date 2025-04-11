<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use Generator;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\Snowflake;
use Throwable;

class SnowflakeDatatypeTest extends BaseDatatypeTestCase
{
    public function testValid(): void
    {
        new Snowflake('VARCHAR', ['length' => '50']);
        $this->expectNotToPerformAssertions();
    }

    public function testInvalidType(): void
    {
        try {
            new Snowflake('UNKNOWN');
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidTypeException::class, get_class($e));
        }
    }

    public function testValidNumericLengths(): void
    {
        new Snowflake('numeric');
        new Snowflake('NUMERIC');
        new Snowflake('NUMERIC', ['length' => '']);
        new Snowflake('NUMERIC', ['length' => []]);
        new Snowflake('INTEGER', ['length' => '']);
        new Snowflake('INTEGER', ['length' => []]);
        new Snowflake('NUMERIC', ['length' => '38,0']);
        new Snowflake('NUMERIC', ['length' => '38,38']);
        new Snowflake('NUMERIC', ['length' => '38']);
        new Snowflake('NUMERIC', [
            'length' => [
                'numeric_precision' => '38',
                'numeric_scale' => '0',
            ],
        ]);
        new Snowflake('NUMERIC', [
            'length' => [
                'numeric_precision' => '38',
                'numeric_scale' => '38',
            ],
        ]);
        new Snowflake('NUMERIC', [
            'length' => [
                'numeric_precision' => '38',
            ],
        ]);
        new Snowflake('NUMERIC', [
            'length' => [
                'numeric_scale' => '38',
            ],
        ]);
        $this->expectNotToPerformAssertions();
    }

    /**
     * @dataProvider invalidNumericLengths
     * @param string|int|null $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    public function testInvalidNumericLengths($length): void
    {
        try {
            new Snowflake('NUMERIC', ['length' => $length]);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    public function testValidDateTimeLengths(): void
    {
        new Snowflake('datetime');
        new Snowflake('DATETIME');
        new Snowflake('DATETIME', ['length' => '']);
        new Snowflake('TIMESTAMP', ['length' => '']);
        new Snowflake('TIMESTAMP_LTZ', ['length' => '4']);
        new Snowflake('TIMESTAMP_TZ', ['length' => '0']);
        new Snowflake('TIMESTAMP_NTZ', ['length' => '9']);
        new Snowflake('TIME', ['length' => '9']);
        $this->expectNotToPerformAssertions();
    }

    public function testValidBinaryLengths(): void
    {
        new Snowflake('binary');
        new Snowflake('varbinary');
        new Snowflake('VARBINARY');
        new Snowflake('OBJECT');
        new Snowflake('ARRAY');
        new Snowflake('GEOGRAPHY');
        new Snowflake('GEOMETRY');
        new Snowflake('BINARY', ['length' => '']);
        new Snowflake('VARBINARY', ['length' => '']);
        new Snowflake('BINARY', ['length' => '1']);
        new Snowflake('VARBINARY', ['length' => '1']);
        new Snowflake('BINARY', ['length' => '8388608']);
        new Snowflake('VARBINARY', ['length' => '8388608']);
        $this->expectNotToPerformAssertions();
    }

    public function testSqlDefinition(): void
    {
        $definition = new Snowflake('NUMERIC', ['length' => '', 'nullable' => false]);
        $this->assertSame('NUMERIC NOT NULL', $definition->getSQLDefinition());

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => false]);
        $this->assertSame('NUMERIC (10,10) NOT NULL', $definition->getSQLDefinition());

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => false]);
        $this->assertSame('NUMERIC (10,10) NOT NULL', $definition->getSQLDefinition());

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => true]);
        $this->assertSame('NUMERIC (10,10)', $definition->getSQLDefinition());

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => true, 'default' => '10']);
        $this->assertSame('NUMERIC (10,10) DEFAULT 10', $definition->getSQLDefinition());

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => true, 'default' => '']);
        $this->assertSame('NUMERIC (10,10)', $definition->getSQLDefinition());

        $definition = new Snowflake('VARCHAR', ['length' => '10', 'nullable' => true, 'default' => '\'\'']);
        $this->assertSame('VARCHAR (10) DEFAULT \'\'', $definition->getSQLDefinition());

        $definition = new Snowflake('NVARCHAR', ['length' => '10', 'nullable' => true, 'default' => '\'\'']);
        $this->assertSame('NVARCHAR (10) DEFAULT \'\'', $definition->getSQLDefinition());

        $definition = new Snowflake('NVARCHAR2', ['length' => '10', 'nullable' => true, 'default' => '\'\'']);
        $this->assertSame('NVARCHAR2 (10) DEFAULT \'\'', $definition->getSQLDefinition());

        $definition = new Snowflake('TIMESTAMP_TZ', ['length' => '0']);
        $this->assertSame('TIMESTAMP_TZ (0)', $definition->getSQLDefinition());

        $definition = new Snowflake('TIMESTAMP_TZ', ['length' => '9']);
        $this->assertSame('TIMESTAMP_TZ (9)', $definition->getSQLDefinition());

        $definition = new Snowflake('TIMESTAMP_TZ', ['length' => '']);
        $this->assertSame('TIMESTAMP_TZ', $definition->getSQLDefinition());

        $definition = new Snowflake('TIMESTAMP_TZ');
        $this->assertSame('TIMESTAMP_TZ', $definition->getSQLDefinition());
    }

    public function testTypeOnlySqlDefinition(): void
    {
        $definition = new Snowflake('NUMERIC', ['length' => '', 'nullable' => false]);
        $this->assertSame('NUMERIC', $definition->getTypeOnlySQLDefinition());

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => false]);
        $this->assertSame('NUMERIC (10,10)', $definition->getTypeOnlySQLDefinition());

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => false, 'default' => '10']);
        $this->assertSame('NUMERIC (10,10)', $definition->getTypeOnlySQLDefinition());

        $definition = new Snowflake('TIMESTAMP_TZ', ['length' => '0']);
        $this->assertSame('TIMESTAMP_TZ (0)', $definition->getTypeOnlySQLDefinition());

        $definition = new Snowflake('TIMESTAMP_TZ', ['length' => '9']);
        $this->assertSame('TIMESTAMP_TZ (9)', $definition->getTypeOnlySQLDefinition());

        $definition = new Snowflake('TIMESTAMP_TZ', ['length' => '']);
        $this->assertSame('TIMESTAMP_TZ', $definition->getTypeOnlySQLDefinition());

        $definition = new Snowflake('TIMESTAMP_TZ');
        $this->assertSame('TIMESTAMP_TZ', $definition->getTypeOnlySQLDefinition());
    }

    /**
     * @dataProvider invalidDateTimeLengths
     * @param string|int|null $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    public function testInvalidDateTimeLengths($length): void
    {
        try {
            new Snowflake('DATETIME', ['length' => $length]);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    public function testInvalidOption(): void
    {
        try {
            new Snowflake('NUMERIC', ['myoption' => 'value']);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidOptionException::class, get_class($e));
        }
    }

    public function testInvalidLengthOption(): void
    {
        $this->expectException(InvalidOptionException::class);
        $this->expectExceptionMessage('Length option "invalidOption" not supported');
        new Snowflake('NUMERIC', ['length' => ['invalidOption' => '123']]);
    }

    public function testValidCharacterLengths(): void
    {
        new Snowflake('string');
        new Snowflake('STRING');
        new Snowflake('STRING', ['length' => '']);
        new Snowflake('STRING', ['length' => '1']);
        new Snowflake('STRING', ['length' => '134217728']);
        new Snowflake('STRING', [
            'length' => [
                'character_maximum' => '134217728',
            ],
        ]);
        new Snowflake('STRING', [
            'length' => [],
        ]);
        $this->expectNotToPerformAssertions();
    }

    /**
     * @dataProvider invalidCharacterLengths
     * @param string|int|null $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    public function testInvalidCharacterLengths($length): void
    {
        try {
            new Snowflake('STRING', ['length' => $length]);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    /**
     * @dataProvider invalidBinaryLengths
     * @param string|int|null $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    public function testInvalidBinaryLengths($length): void
    {
        try {
            new Snowflake('BINARY', ['length' => $length]);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }

        try {
            new Snowflake('VARBINARY', ['length' => $length]);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    public function basetypeProvider(): Generator
    {
        yield Snowflake::TYPE_INT => [
            new Snowflake(Snowflake::TYPE_INT, $this->getTypeDefaultOptions(Snowflake::TYPE_INT)),
            'INTEGER',
        ];
        yield Snowflake::TYPE_INTEGER => [
            new Snowflake(Snowflake::TYPE_INTEGER, $this->getTypeDefaultOptions(Snowflake::TYPE_INTEGER)),
            'INTEGER',
        ];
        yield Snowflake::TYPE_BIGINT => [
            new Snowflake(Snowflake::TYPE_BIGINT, $this->getTypeDefaultOptions(Snowflake::TYPE_BIGINT)),
            'INTEGER',
        ];
        yield Snowflake::TYPE_SMALLINT => [
            new Snowflake(Snowflake::TYPE_SMALLINT, $this->getTypeDefaultOptions(Snowflake::TYPE_SMALLINT)),
            'INTEGER',
        ];
        yield Snowflake::TYPE_TINYINT => [
            new Snowflake(Snowflake::TYPE_TINYINT, $this->getTypeDefaultOptions(Snowflake::TYPE_TINYINT)),
            'INTEGER',
        ];
        yield Snowflake::TYPE_BYTEINT => [
            new Snowflake(Snowflake::TYPE_BYTEINT, $this->getTypeDefaultOptions(Snowflake::TYPE_BYTEINT)),
            'INTEGER',
        ];
        yield Snowflake::TYPE_NUMBER => [
            new Snowflake(Snowflake::TYPE_NUMBER, $this->getTypeDefaultOptions(Snowflake::TYPE_NUMBER)),
            'NUMERIC',
        ];
        yield Snowflake::TYPE_DECIMAL => [
            new Snowflake(Snowflake::TYPE_DECIMAL, $this->getTypeDefaultOptions(Snowflake::TYPE_DECIMAL)),
            'NUMERIC',
        ];
        yield Snowflake::TYPE_DEC => [
            new Snowflake(Snowflake::TYPE_DEC, $this->getTypeDefaultOptions(Snowflake::TYPE_DEC)),
            'NUMERIC',
        ];
        yield Snowflake::TYPE_NUMERIC => [
            new Snowflake(Snowflake::TYPE_NUMERIC, $this->getTypeDefaultOptions(Snowflake::TYPE_NUMERIC)),
            'NUMERIC',
        ];
        yield Snowflake::TYPE_FLOAT => [
            new Snowflake(Snowflake::TYPE_FLOAT, $this->getTypeDefaultOptions(Snowflake::TYPE_FLOAT)),
            'FLOAT',
        ];
        yield Snowflake::TYPE_FLOAT4 => [
            new Snowflake(Snowflake::TYPE_FLOAT4, $this->getTypeDefaultOptions(Snowflake::TYPE_FLOAT4)),
            'FLOAT',
        ];
        yield Snowflake::TYPE_FLOAT8 => [
            new Snowflake(Snowflake::TYPE_FLOAT8, $this->getTypeDefaultOptions(Snowflake::TYPE_FLOAT8)),
            'FLOAT',
        ];
        yield Snowflake::TYPE_DOUBLE => [
            new Snowflake(Snowflake::TYPE_DOUBLE, $this->getTypeDefaultOptions(Snowflake::TYPE_DOUBLE)),
            'FLOAT',
        ];
        yield Snowflake::TYPE_DOUBLE_PRECISION => [
            new Snowflake(
                Snowflake::TYPE_DOUBLE_PRECISION,
                $this->getTypeDefaultOptions(Snowflake::TYPE_DOUBLE_PRECISION),
            ),
            'FLOAT',
        ];
        yield Snowflake::TYPE_REAL => [
            new Snowflake(Snowflake::TYPE_REAL, $this->getTypeDefaultOptions(Snowflake::TYPE_REAL)),
            'FLOAT',
        ];
        yield Snowflake::TYPE_BOOLEAN => [
            new Snowflake(Snowflake::TYPE_BOOLEAN, $this->getTypeDefaultOptions(Snowflake::TYPE_BOOLEAN)),
            'BOOLEAN',
        ];
        yield Snowflake::TYPE_DATE => [
            new Snowflake(Snowflake::TYPE_DATE, $this->getTypeDefaultOptions(Snowflake::TYPE_DATE)),
            'DATE',
        ];
        yield Snowflake::TYPE_DATETIME => [
            new Snowflake(Snowflake::TYPE_DATETIME, $this->getTypeDefaultOptions(Snowflake::TYPE_DATETIME)),
            'TIMESTAMP',
        ];
        yield Snowflake::TYPE_TIMESTAMP => [
            new Snowflake(Snowflake::TYPE_TIMESTAMP, $this->getTypeDefaultOptions(Snowflake::TYPE_TIMESTAMP)),
            'TIMESTAMP',
        ];
        yield Snowflake::TYPE_TIMESTAMP_NTZ => [
            new Snowflake(Snowflake::TYPE_TIMESTAMP_NTZ, $this->getTypeDefaultOptions(Snowflake::TYPE_TIMESTAMP_NTZ)),
            'TIMESTAMP',
        ];
        yield Snowflake::TYPE_TIMESTAMP_LTZ => [
            new Snowflake(Snowflake::TYPE_TIMESTAMP_LTZ, $this->getTypeDefaultOptions(Snowflake::TYPE_TIMESTAMP_LTZ)),
            'TIMESTAMP',
        ];
        yield Snowflake::TYPE_TIMESTAMP_TZ => [
            new Snowflake(Snowflake::TYPE_TIMESTAMP_TZ, $this->getTypeDefaultOptions(Snowflake::TYPE_TIMESTAMP_TZ)),
            'TIMESTAMP',
        ];

        $testedTypes = [
            Snowflake::TYPE_INT,
            Snowflake::TYPE_INTEGER,
            Snowflake::TYPE_BIGINT,
            Snowflake::TYPE_SMALLINT,
            Snowflake::TYPE_TINYINT,
            Snowflake::TYPE_BYTEINT,
            Snowflake::TYPE_NUMBER,
            Snowflake::TYPE_DECIMAL,
            Snowflake::TYPE_DEC,
            Snowflake::TYPE_NUMERIC,
            Snowflake::TYPE_FLOAT,
            Snowflake::TYPE_FLOAT4,
            Snowflake::TYPE_FLOAT8,
            Snowflake::TYPE_DOUBLE,
            Snowflake::TYPE_DOUBLE_PRECISION,
            Snowflake::TYPE_REAL,
            Snowflake::TYPE_BOOLEAN,
            Snowflake::TYPE_DATE,
            Snowflake::TYPE_DATETIME,
            Snowflake::TYPE_TIMESTAMP,
            Snowflake::TYPE_TIMESTAMP_NTZ,
            Snowflake::TYPE_TIMESTAMP_LTZ,
            Snowflake::TYPE_TIMESTAMP_TZ,
        ];
        foreach (Snowflake::TYPES as $type) {
            if (!in_array($type, $testedTypes, true)) {
                yield $type => [
                    new Snowflake($type, $this->getTypeDefaultOptions($type)),
                    'STRING',
                ];
            }
        }

        yield Snowflake::TYPE_NUMBER.' with 38,0 length' => [
            new Snowflake(Snowflake::TYPE_NUMBER, ['length' => '38,0']),
            'INTEGER',
        ];
    }

    /**
     * @dataProvider basetypeProvider
     */
    public function testBasetypes(
        Snowflake $type,
        string $expectedBasetype,
    ): void {
        $this->assertEquals($expectedBasetype, $type->getBasetype());
    }

    public function testVariant(): void
    {
        new Snowflake('VARIANT');
        $this->expectNotToPerformAssertions();
    }

    /**
     * @return array<int, array<string>>
     */
    public function invalidNumericLengths(): array
    {
        return [
            ['notANumber'],
            ['0,0'],
            ['39,0'],
            ['-10,-5'],
            ['-5,-10'],
            ['38,a'],
            ['a,38'],
            ['a,a'],
        ];
    }

    /**
     * @return array<int, array<string>>
     */
    public function invalidCharacterLengths(): array
    {
        return [
            ['a'],
            ['0'],
            ['134217729'],
            ['-1'],
        ];
    }

    /**
     * @return array<int, array<string>>
     */
    public function invalidDateTimeLengths(): array
    {
        return [
            ['notANumber'],
            ['0,0'],
            ['-1'],
            ['10'],
            ['a'],
            ['a,a'],
        ];
    }

    /**
     * @return array<int, array<string>>
     */
    public function invalidBinaryLengths(): array
    {
        return [
            ['a'],
            ['0'],
            ['8388609'],
            ['-1'],
        ];
    }

    public static function getTestedClass(): string
    {
        return Snowflake::class;
    }

    public static function provideTestGetTypeByBasetype(): Generator
    {
        yield BaseType::BOOLEAN => [
            'basetype' => BaseType::BOOLEAN,
            'expectedType' => 'BOOLEAN',
        ];

        yield BaseType::DATE => [
            'basetype' => BaseType::DATE,
            'expectedType' => 'DATE',
        ];

        yield BaseType::FLOAT => [
            'basetype' => BaseType::FLOAT,
            'expectedType' => 'FLOAT',
        ];

        yield BaseType::INTEGER => [
            'basetype' => BaseType::INTEGER,
            'expectedType' => 'INTEGER',
        ];

        yield BaseType::NUMERIC => [
            'basetype' => BaseType::NUMERIC,
            'expectedType' => 'NUMBER',
        ];

        yield BaseType::STRING => [
            'basetype' => BaseType::STRING,
            'expectedType' => 'VARCHAR',
        ];

        yield BaseType::TIMESTAMP => [
            'basetype' => BaseType::TIMESTAMP,
            'expectedType' => 'TIMESTAMP',
        ];

        yield 'invalidBaseType' => [
            'basetype' => 'invalidBaseType',
            'expectedType' => null,
            'expectToFail' => true,
        ];
    }

    public static function provideTestGetDefinitionForBasetype(): Generator
    {
        yield BaseType::BOOLEAN => [
            'basetype' => BaseType::BOOLEAN,
            'expectedColumnDefinition' => new Snowflake('BOOLEAN'),
        ];

        yield BaseType::DATE => [
            'basetype' => BaseType::DATE,
            'expectedColumnDefinition' => new Snowflake('DATE'),
        ];

        yield BaseType::FLOAT => [
            'basetype' => BaseType::FLOAT,
            'expectedColumnDefinition' => new Snowflake('FLOAT'),
        ];

        yield BaseType::INTEGER => [
            'basetype' => BaseType::INTEGER,
            'expectedColumnDefinition' => new Snowflake('INTEGER'),
        ];

        yield BaseType::NUMERIC => [
            'basetype' => BaseType::NUMERIC,
            'expectedColumnDefinition' => new Snowflake('NUMERIC', ['length' => '38,9']),
        ];

        yield BaseType::STRING => [
            'basetype' => BaseType::STRING,
            'expectedColumnDefinition' => new Snowflake('VARCHAR'),
        ];

        yield BaseType::TIMESTAMP => [
            'basetype' => BaseType::TIMESTAMP,
            'expectedColumnDefinition' => new Snowflake('TIMESTAMP'),
        ];

        yield 'invalidBaseType' => [
            'basetype' => 'invalidBaseType',
            'expectedColumnDefinition' => null,
            'expectToFail' => true,
        ];
    }

    /**
     * @param array<string, mixed> $expectedArray
     * @dataProvider arrayFromLengthProvider
     */
    public function testArrayFromLength(string $type, ?string $length, array $expectedArray): void
    {
        $definition = new Snowflake($type, ['length' => $length]);
        $this->assertSame($expectedArray, $definition->getArrayFromLength());
    }

    /**
     * @dataProvider provideTestGetTypeFromAlias
     */
    public function testBackendBasetypeFromAlias(string $type, string $expectedType): void
    {

        $definition = new Snowflake($type, $this->getTypeDefaultOptions($type));
        $this->assertSame($expectedType, $definition->getBackendBasetype());
    }

    public function arrayFromLengthProvider(): Generator
    {
        yield 'simple' => [
            'VARCHAR',
            '10',
            ['character_maximum' => '10'],
        ];
        yield 'decimal' => [
            'NUMERIC',
            '38,2',
            ['numeric_precision' => 38, 'numeric_scale' => 2],
        ];
        yield 'with zero scale' => [
            'NUMERIC',
            '38,0',
            ['numeric_precision' => 38, 'numeric_scale' => 0],
        ];
        yield 'with null length' => [
            'NUMERIC',
            null,
            ['numeric_precision' => 38, 'numeric_scale' => 0],
        ];
        yield 'numeric with int length' => [
            'NUMERIC',
            '10',
            ['numeric_precision' => 10, 'numeric_scale' => 0],
        ];
    }

    public function provideTestGetTypeFromAlias(): Generator
    {
        foreach (Snowflake::TYPES as $type) {
            switch ($type) {
                case Snowflake::TYPE_NVARCHAR:
                case Snowflake::TYPE_NVARCHAR2:
                case Snowflake::TYPE_CHAR:
                case Snowflake::TYPE_CHARACTER:
                case Snowflake::TYPE_CHAR_VARYING:
                case Snowflake::TYPE_CHARACTER_VARYING:
                case Snowflake::TYPE_NCHAR_VARYING:
                case Snowflake::TYPE_NCHAR:
                case Snowflake::TYPE_STRING:
                case Snowflake::TYPE_TEXT:
                    $expectedType = Snowflake::TYPE_VARCHAR;
                    break;
                case Snowflake::TYPE_DEC:
                case Snowflake::TYPE_DECIMAL:
                case Snowflake::TYPE_NUMERIC:
                case Snowflake::TYPE_INT:
                case Snowflake::TYPE_INTEGER:
                case Snowflake::TYPE_BIGINT:
                case Snowflake::TYPE_SMALLINT:
                case Snowflake::TYPE_TINYINT:
                case Snowflake::TYPE_BYTEINT:
                    $expectedType = Snowflake::TYPE_NUMBER;
                    break;
                case Snowflake::TYPE_FLOAT:
                case Snowflake::TYPE_FLOAT4:
                case Snowflake::TYPE_FLOAT8:
                case Snowflake::TYPE_DOUBLE:
                case Snowflake::TYPE_DOUBLE_PRECISION:
                case Snowflake::TYPE_REAL:
                    $expectedType = Snowflake::TYPE_FLOAT;
                    break;
                case Snowflake::TYPE_VARBINARY:
                    $expectedType = Snowflake::TYPE_BINARY;
                    break;
                case Snowflake::TYPE_DATETIME:
                    $expectedType = Snowflake::TYPE_TIMESTAMP_NTZ;
                    break;
                default:
                    $expectedType = $type;
                    break;
            }

            yield $type => [
                'type' => $type,
                'expectedType' => $expectedType,
            ];
        }
    }

    /**
     * @return array{
     *      length?:string|null|array,
     *      nullable?:bool,
     *      default?:string|null
     *  }
     */
    private function getTypeDefaultOptions(string $type): array
    {
        $options = [];
        if ($type === Snowflake::TYPE_VECTOR) {
            // VECTOR don't have any meaningfully default option
            $options = [
                'length' => 'INT,3',
            ];
        }

        return $options;
    }
}
