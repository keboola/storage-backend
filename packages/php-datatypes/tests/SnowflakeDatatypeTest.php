<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\Snowflake;
use PHPUnit\Framework\TestCase;
use Throwable;

class SnowflakeDatatypeTest extends TestCase
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
        $this->assertSame($definition->getSQLDefinition(), 'NUMERIC NOT NULL');

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => false]);
        $this->assertSame($definition->getSQLDefinition(), 'NUMERIC (10,10) NOT NULL');

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => false]);
        $this->assertSame($definition->getSQLDefinition(), 'NUMERIC (10,10) NOT NULL');

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => true]);
        $this->assertSame($definition->getSQLDefinition(), 'NUMERIC (10,10)');

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => true, 'default' => '10']);
        $this->assertSame($definition->getSQLDefinition(), 'NUMERIC (10,10) DEFAULT 10');

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => true, 'default' => '']);
        $this->assertSame($definition->getSQLDefinition(), 'NUMERIC (10,10)');

        $definition = new Snowflake('VARCHAR', ['length' => '10', 'nullable' => true, 'default' => '\'\'']);
        $this->assertSame($definition->getSQLDefinition(), 'VARCHAR (10) DEFAULT \'\'');

        $definition = new Snowflake('TIMESTAMP_TZ', ['length' => '0']);
        $this->assertSame($definition->getSQLDefinition(), 'TIMESTAMP_TZ (0)');

        $definition = new Snowflake('TIMESTAMP_TZ', ['length' => '9']);
        $this->assertSame($definition->getSQLDefinition(), 'TIMESTAMP_TZ (9)');

        $definition = new Snowflake('TIMESTAMP_TZ', ['length' => '']);
        $this->assertSame($definition->getSQLDefinition(), 'TIMESTAMP_TZ');

        $definition = new Snowflake('TIMESTAMP_TZ');
        $this->assertSame($definition->getSQLDefinition(), 'TIMESTAMP_TZ');
    }

    public function testTypeOnlySqlDefinition(): void
    {
        $definition = new Snowflake('NUMERIC', ['length' => '', 'nullable' => false]);
        $this->assertSame($definition->getTypeOnlySQLDefinition(), 'NUMERIC');

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => false]);
        $this->assertSame($definition->getTypeOnlySQLDefinition(), 'NUMERIC (10,10)');

        $definition = new Snowflake('NUMERIC', ['length' => '10,10', 'nullable' => false, 'default' => '10']);
        $this->assertSame($definition->getTypeOnlySQLDefinition(), 'NUMERIC (10,10)');

        $definition = new Snowflake('TIMESTAMP_TZ', ['length' => '0']);
        $this->assertSame($definition->getTypeOnlySQLDefinition(), 'TIMESTAMP_TZ (0)');

        $definition = new Snowflake('TIMESTAMP_TZ', ['length' => '9']);
        $this->assertSame($definition->getTypeOnlySQLDefinition(), 'TIMESTAMP_TZ (9)');

        $definition = new Snowflake('TIMESTAMP_TZ', ['length' => '']);
        $this->assertSame($definition->getTypeOnlySQLDefinition(), 'TIMESTAMP_TZ');

        $definition = new Snowflake('TIMESTAMP_TZ');
        $this->assertSame($definition->getTypeOnlySQLDefinition(), 'TIMESTAMP_TZ');
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
        new Snowflake('STRING', ['length' => '16777216']);
        new Snowflake('STRING', [
            'length' => [
                'character_maximum' => '16777216',
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

    public function testBasetypes(): void
    {
        foreach (Snowflake::TYPES as $type) {
            $basetype = (new Snowflake($type))->getBasetype();
            switch ($type) {
                case 'INT':
                case 'INTEGER':
                case 'BIGINT':
                case 'SMALLINT':
                case 'TINYINT':
                case 'BYTEINT':
                    $this->assertEquals('INTEGER', $basetype);
                    break;
                case 'NUMBER':
                case 'DECIMAL':
                case 'NUMERIC':
                    $this->assertEquals('NUMERIC', $basetype);
                    break;
                case 'FLOAT':
                case 'FLOAT4':
                case 'FLOAT8':
                case 'DOUBLE':
                case 'DOUBLE PRECISION':
                case 'REAL':
                    $this->assertEquals('FLOAT', $basetype);
                    break;
                case 'BOOLEAN':
                    $this->assertEquals('BOOLEAN', $basetype);
                    break;
                case 'DATE':
                    $this->assertEquals('DATE', $basetype);
                    break;
                case 'DATETIME':
                case 'TIMESTAMP':
                case 'TIMESTAMP_NTZ':
                case 'TIMESTAMP_LTZ':
                case 'TIMESTAMP_TZ':
                    $this->assertEquals('TIMESTAMP', $basetype);
                    break;
                default:
                    $this->assertEquals('STRING', $basetype);
                    break;
            }
        }
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
            ['16777217'],
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

    public function testGetTypeByBasetype(): void
    {
        $this->assertSame('BOOLEAN', Snowflake::getTypeByBasetype('BOOLEAN'));

        $this->assertSame('VARCHAR', Snowflake::getTypeByBasetype('STRING'));

        // not only upper case
        $this->assertSame('BOOLEAN', Snowflake::getTypeByBasetype('Boolean'));

        $this->expectException(InvalidTypeException::class);
        $this->expectExceptionMessage('Base type "FOO" is not valid.');
        Snowflake::getTypeByBasetype('foo');
    }
}
