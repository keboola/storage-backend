<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\MySQL;
use PHPUnit\Framework\TestCase;
use Throwable;

class MySQLDatatypeTest extends TestCase
{
    public function testValid(): void
    {
        new MySQL('VARCHAR', ['length' => '50']);
        $this->expectNotToPerformAssertions();
    }

    public function testInvalidType(): void
    {
        try {
            new MySQL('UNKNOWN');
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidTypeException::class, get_class($e));
        }
    }

    public function testInvalidOption(): void
    {
        try {
            new MySQL('NUMERIC', ['myoption' => 'value']);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidOptionException::class, get_class($e));
        }
    }

    public function testValidNumericLengths(): void
    {
        new MySQL('numeric');
        new MySQL('NUMERIC');
        new MySQL('NUMERIC', ['length' => '']);
        new MySQL('NUMERIC', ['length' => '65,0']);
        new MySQL('NUMERIC', ['length' => '65']);
        new MySQL('NUMERIC', ['length' => '10,10']);
        new MySQL('NUMERIC', [
            'length' => [
                'numeric_precision' => '38',
                'numeric_scale' => '0',
            ],
        ]);
        new MySQL('NUMERIC', [
            'length' => [
                'numeric_precision' => '20',
                'numeric_scale' => '20',
            ],
        ]);
        new MySQL('NUMERIC', [
            'length' => [
                'numeric_precision' => '20',
            ],
        ]);
        new MySQL('NUMERIC', [
            'length' => [
                'numeric_scale' => '20',
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
            new MySQL('NUMERIC', ['length' => $length]);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    public function testValidIntegerLengths(): void
    {
        new MySQL('integer');
        new MySQL('INTEGER');
        new MySQL('INTEGER', ['length' => '']);
        new MySQL('INTEGER', ['length' => '255']);
        $this->expectNotToPerformAssertions();
    }

    /**
     * @dataProvider invalidIntegerLengths
     * @param string|int|null $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    public function testInvalidIntegerLengths($length): void
    {
        try {
            new MySQL('INTEGER', ['length' => $length]);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }


    public function testValidFloatLengths(): void
    {
        new MySQL('float');
        new MySQL('FLOAT');
        new MySQL('FLOAT', ['length' => '']);
        new MySQL('FLOAT', ['length' => '255']);
        new MySQL('FLOAT', ['length' => '255,0']);
        $this->expectNotToPerformAssertions();
    }

    /**
     * @dataProvider invalidFloatLengths
     * @param string|int|null $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    public function testInvalidFloatLengths($length): void
    {
        try {
            new MySQL('FLOAT', ['length' => $length]);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }


    public function testValidVariableCharacterLengths(): void
    {
        new MySQL('varchar', ['length' => '1']);
        new MySQL('VARCHAR', ['length' => '1']);
        new MySQL('VARCHAR', ['length' => '4294967295']);
        new MySQL('VARCHAR', [
            'length' => [
                'character_maximum' => '16777216',
            ],
        ]);
        $this->expectNotToPerformAssertions();
    }

    public function testValidFixedCharacterLengths(): void
    {
        new MySQL('char');
        new MySQL('CHAR');
        new MySQL('CHAR', ['length' => '']);
        new MySQL('CHAR', ['length' => '1']);
        new MySQL('CHAR', ['length' => '255']);
        $this->expectNotToPerformAssertions();
    }

    public function testVariableCharacterWithoutLength(): void
    {
        try {
            new MySQL('VARCHAR');
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    /**
     * @dataProvider invalidVariableCharacterLengths
     * @param string|int|null $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    public function testInvalidVariableCharacterLengths($length): void
    {
        try {
            new MySQL('VARCHAR', ['length' => $length]);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    /**
     * @dataProvider invalidFixedCharacterLengths
     * @param string|int|null $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    public function testInvalidFixedCharacterLengths($length): void
    {
        try {
            new MySQL('CHAR', ['length' => $length]);
            $this->fail('Exception not caught');
        } catch (Throwable $e) {
            $this->assertEquals(InvalidLengthException::class, get_class($e));
        }
    }

    public function testBasetypes(): void
    {
        foreach (MySQL::TYPES as $type) {
            if ($type === 'VARCHAR') {
                $basetype = (new MySQL($type, ['length' => 255]))->getBasetype();
            } else {
                $basetype = (new MySQL($type))->getBasetype();
            }
            switch ($type) {
                case 'INT':
                case 'INTEGER':
                case 'BIGINT':
                case 'SMALLINT':
                case 'TINYINT':
                case 'MEDIUMINT':
                    $this->assertEquals('INTEGER', $basetype);
                    break;
                case 'NUMERIC':
                case 'DECIMAL':
                case 'DEC':
                case 'FIXED':
                    $this->assertEquals('NUMERIC', $basetype);
                    break;
                case 'FLOAT':
                case 'DOUBLE PRECISION':
                case 'REAL':
                case 'DOUBLE':
                    $this->assertEquals('FLOAT', $basetype);
                    break;
                case 'DATE':
                    $this->assertEquals('DATE', $basetype);
                    break;
                case 'DATETIME':
                case 'TIMESTAMP':
                    $this->assertEquals('TIMESTAMP', $basetype);
                    break;
                default:
                    $this->assertEquals('STRING', $basetype);
                    break;
            }
        }
    }

    /**
     * @return array<int, array<string>>
     */
    public function invalidNumericLengths(): array
    {
        return [
            ['notANumber'],
            ['0,0'],
            ['66,0'],
            ['-10,-5'],
            ['-5,-10'],
            ['66,a'],
            ['a,66'],
            ['a,a'],
            ['66,66'],
        ];
    }

    /**
     * @return array<int, array<string>>
     */
    public function invalidIntegerLengths(): array
    {
        return [
            ['notANumber'],
            ['-1'],
            ['256'],
        ];
    }

    /**
     * @return array<int, array<string>>
     */
    public function invalidFixedCharacterLengths(): array
    {
        return [
            ['a'],
            ['0'],
            ['256'],
            ['-1'],
        ];
    }


    /**
     * @return array<int, array<string>>
     */
    public function invalidVariableCharacterLengths(): array
    {
        return [
            [''],
            ['a'],
            ['0'],
            ['4294967296'],
            ['-1'],
        ];
    }

    /**
     * @return array<int, array<string>>
     */
    public function invalidVariableIntegerLengths(): array
    {
        return [
            ['-1'],
            ['256'],
            ['a'],
        ];
    }

    /**
     * @return array<int, array<string>>
     */
    public function invalidFloatLengths(): array
    {
        return [
            ['notANumber'],
            ['0,0'],
            ['256,0'],
            ['-10,-5'],
            ['-5,-10'],
            ['256,a'],
            ['a,256'],
            ['a,a'],
            ['10,10'],
            ['256,256'],
        ];
    }
}
