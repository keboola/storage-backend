<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use Generator;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\Synapse;
use PHPUnit\Framework\TestCase;
use Throwable;

class SynapseDatatypeTest extends TestCase
{
    /**
     * @return array<int, array<class-string<\datetime>|string>>
     */
    public function invalidLengths(): array
    {
        return [
            ['float', 'notANumber'],
            ['float', '0'],
            ['float', '54'],

            ['real', 'notANumber'],
            ['real', '0'],
            ['real', '10'],
            ['real', '54'],

            ['decimal', 'notANumber'],
            ['decimal', '0,0'],
            ['decimal', '39,0'],
            ['decimal', '-10,-5'],
            ['decimal', '-5,-10'],
            ['decimal', '38,a'],
            ['decimal', 'a,38'],
            ['decimal', 'a,a'],
            ['decimal', '16,32'],

            ['numeric', 'notANumber'],
            ['numeric', '0,0'],
            ['numeric', '39,0'],
            ['numeric', '-10,-5'],
            ['numeric', '-5,-10'],
            ['numeric', '38,a'],
            ['numeric', 'a,38'],
            ['numeric', 'a,a'],
            ['decimal', '16,32'],

            ['nvarchar', 'notANumber'],
            ['nvarchar', '0'],
            ['nvarchar', '4001'],
            ['nvarchar', '1.0'],

            ['nchar', 'notANumber'],
            ['nchar', '0'],
            ['nchar', '4001'],

            ['varchar', 'notANumber'],
            ['varchar', '0'],
            ['varchar', '8001'],

            ['char', 'notANumber'],
            ['char', '0'],
            ['char', '8001'],

            ['varbinary', 'notANumber'],
            ['varbinary', '0'],
            ['varbinary', '8001'],

            ['binary', 'notANumber'],
            ['binary', '0'],
            ['binary', '8001'],

            ['datetimeoffset', 'notANumber'],
            ['datetimeoffset', '-1'],
            ['datetimeoffset', '8'],

            ['datetime2', 'notANumber'],
            ['datetime2', '-1'],
            ['datetime2', '8'],

            ['time', 'notANumber'],
            ['time', '-1'],
            ['time', '8'],

            ['money', 'anyLength'],
            ['smallmoney', 'anyLength'],
            ['bigint', 'anyLength'],
            ['int', 'anyLength'],
            ['smallint', 'anyLength'],
            ['tinyint', 'anyLength'],
            ['bit', 'anyLength'],
            ['uniqueidentifier', 'anyLength'],
            ['date', 'anyLength'],
            ['datetime', 'anyLength'],

        ];
    }

    public function testBasetypes(): void
    {
        foreach (Synapse::TYPES as $type) {
            $basetype = (new Synapse($type))->getBasetype();
            switch ($type) {
                case 'BIGINT':
                case 'INT':
                case 'SMALLINT':
                case 'TINYINT':
                    $this->assertEquals(BaseType::INTEGER, $basetype);
                    break;
                case 'DECIMAL':
                case 'NUMERIC':
                    $this->assertEquals(BaseType::NUMERIC, $basetype);
                    break;
                case 'FLOAT':
                case 'REAL':
                    $this->assertEquals(BaseType::FLOAT, $basetype);
                    break;
                case 'BIT':
                    $this->assertEquals(BaseType::BOOLEAN, $basetype);
                    break;
                case 'DATE':
                    $this->assertEquals(BaseType::DATE, $basetype);
                    break;
                case 'DATETIMEOFFSET':
                case 'DATETIME':
                case 'DATETIME2':
                case 'SMALLDATETIME':
                case 'TIME':
                    $this->assertEquals(BaseType::TIMESTAMP, $basetype);
                    break;
                default:
                    $this->assertEquals(BaseType::STRING, $basetype);
                    break;
            }
        }
    }

    /**
     * @dataProvider invalidLengths
     * @param string|int|null $length
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    public function testInvalidLengths(string $type, $length): void
    {
        $options = [];
        if ($length !== null) {
            $options['length'] = $length;
        }
        $this->expectException(InvalidLengthException::class);
        new Synapse($type, $options);
    }

    public function testInvalidOption(): void
    {
        try {
            new Synapse('numeric', ['myoption' => 'value']);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidOptionException::class, get_class($e));
        }
    }

    public function testInvalidType(): void
    {
        $this->expectException(InvalidTypeException::class);
        new Synapse('UNKNOWN');
    }

    /**
     * @dataProvider expectedSqlDefinitions
     */
    public function testSqlDefinition(string $type, ?array $options, string $expectedDefinition): void
    {
        $definition = new Synapse($type, $options);
        $this->assertEquals($expectedDefinition, $definition->getSQLDefinition());
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
                $type . '(38,0)',
            ];
            $tests[] = [
                $type,
                [],
                $type . '(38,0)',
            ];
            $tests[] = [
                $type,
                ['length' => '35,2'],
                $type . '(35,2)',
            ];
            $tests[] = [
                $type,
                ['length' => '35,2', 'default' => '(10.00)'],
                $type . '(35,2) DEFAULT (10.00)',
            ];
            $tests[] = [
                $type,
                ['length' => '35,2', 'default' => '(10.00)', 'nullable' => false],
                $type . '(35,2) NOT NULL DEFAULT (10.00)',
            ];
        }

        foreach (['float'] as $type) {
            $tests[] = [
                $type,
                ['length' => ''],
                $type . '(53)',
            ];
            $tests[] = [
                $type,
                [],
                $type . '(53)',
            ];
            $tests[] = [
                $type,
                ['length' => '1'],
                $type . '(1)',
            ];
            $tests[] = [
                $type,
                ['length' => '1', 'default' => '5'],
                $type . '(1) DEFAULT 5',
            ];
        }

        foreach (['nvarchar', 'nchar'] as $type) {
            $tests[] = [
                $type,
                ['length' => ''],
                $type . '(4000)',
            ];
            $tests[] = [
                $type,
                [],
                $type . '(4000)',
            ];
            $tests[] = [
                $type,
                ['length' => '1000'],
                $type . '(1000)',
            ];
            $tests[] = [
                $type,
                ['length' => '1', 'default' => '\'some string\''],
                $type . '(1) DEFAULT \'some string\'',
            ];
        }

        foreach (['binary', 'char', 'varbinary', 'varchar'] as $type) {
            $tests[] = [
                $type,
                ['length' => ''],
                $type . '(8000)',
            ];
            $tests[] = [
                $type,
                [],
                $type . '(8000)',
            ];
            $tests[] = [
                $type,
                ['length' => '1000'],
                $type . '(1000)',
            ];
        }

        $tests[] = [
            'datetime2',
            ['length' => '0'],
            'datetime2(0)',
        ];
        $tests[] = [
            'datetime2',
            ['length' => '7'],
            'datetime2(7)',
        ];
        $tests[] = [
            'datetime2',
            ['length' => ''],
            'datetime2',
        ];
        $tests[] = [
            'datetime2',
            ['length' => '', 'default'=>'NOW()'],
            'datetime2 DEFAULT NOW()',
        ];

        return $tests;
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
        new Synapse($type, $options);
        $this->expectNotToPerformAssertions();
    }

    /**
     * @return array<int, array<class-string<\datetime>|string|null>>
     */
    public function validLengths(): array
    {
        return [
            ['float', null],
            ['float', ''],
            ['float', '1'],
            ['float', '42'],
            ['float', '53'],

            ['real', null],

            ['decimal', null],
            ['decimal', ''],
            ['decimal', '38,0'],
            ['decimal', '38,38'],
            ['decimal', '38'],

            ['numeric', null],
            ['numeric', ''],
            ['numeric', '38,0'],
            ['numeric', '38,38'],
            ['numeric', '38'],

            ['nvarchar', null],
            ['nvarchar', ''],
            ['nvarchar', 'max'],
            ['nvarchar', 'MAX'],
            ['nvarchar', '1'],
            ['nvarchar', '4000'],

            ['nchar', null],
            ['nchar', ''],
            ['nchar', '1'],
            ['nchar', '4000'],

            ['varchar', null],
            ['varchar', ''],
            ['varchar', 'max'],
            ['varchar', 'MAX'],
            ['varchar', '1'],
            ['varchar', '8000'],

            ['varbinary', null],
            ['varbinary', ''],
            ['varbinary', 'max'],
            ['varbinary', 'MAX'],
            ['varbinary', '1'],
            ['varbinary', '8000'],

            ['binary', null],
            ['binary', ''],
            ['binary', '1'],
            ['binary', '8000'],

            ['char', null],
            ['char', ''],
            ['char', '1'],
            ['char', '8000'],

            ['datetimeoffset', null],
            ['datetimeoffset', ''],
            ['datetimeoffset', '0'],
            ['datetimeoffset', '7'],

            ['datetime2', null],
            ['datetime2', ''],
            ['datetime2', '0'],

            ['time', null],
            ['time', ''],
            ['time', '0'],

            ['money', null],
            ['smallmoney', null],
            ['bigint', null],
            ['int', null],
            ['smallint', null],
            ['tinyint', null],
            ['bit', null],
            ['uniqueidentifier', null],
            ['date', null],
            ['datetime', null],
        ];
    }

    public function typesForLengthsTest(): Generator
    {
        yield 'TYPE_FLOAT' => [
            Synapse::TYPE_FLOAT,
            53,
        ];
        yield 'TYPE_DECIMAL' => [
            Synapse::TYPE_DECIMAL,
            '38,0',
        ];
        yield 'TYPE_NUMERIC' => [
            Synapse::TYPE_NUMERIC,
            '38,0',
        ];
        yield 'TYPE_NCHAR' => [
            Synapse::TYPE_NCHAR,
            4000,
        ];
        yield 'TYPE_NVARCHAR' => [
            Synapse::TYPE_NVARCHAR,
            4000,
        ];
        yield 'TYPE_BINARY' => [
            Synapse::TYPE_BINARY,
            8000,
        ];
        yield 'TYPE_CHAR' => [
            Synapse::TYPE_CHAR,
            8000,
        ];
        yield 'TYPE_VARBINARY' => [
            Synapse::TYPE_VARBINARY,
            8000,
        ];
        yield 'TYPE_VARCHAR' => [
            Synapse::TYPE_VARCHAR,
            8000,
        ];
        yield 'TYPE_REAL' => [
            Synapse::TYPE_REAL,
            null,
        ];
    }

    /**
     * @dataProvider typesForLengthsTest
     * @param string|int|null $expectedLength
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    public function testDefaultLengths(string $type, $expectedLength): void
    {
        $def = new Synapse($type, []);
        $this->assertSame($expectedLength, $def->getDefaultLength());
    }

    public function testGetTypeByBasetype(): void
    {
        $this->assertSame('BIT', Synapse::getTypeByBasetype('BOOLEAN'));

        $this->assertSame('NVARCHAR', Synapse::getTypeByBasetype('STRING'));

        // not only upper case
        $this->assertSame('BIT', Synapse::getTypeByBasetype('Boolean'));

        $this->expectException(InvalidTypeException::class);
        $this->expectExceptionMessage('Base type "FOO" is not valid.');
        Synapse::getTypeByBasetype('foo');
    }
}
