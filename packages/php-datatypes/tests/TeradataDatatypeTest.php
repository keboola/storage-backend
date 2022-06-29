<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use Generator;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\Teradata;
use PHPUnit\Framework\TestCase;
use Throwable;

class TeradataDatatypeTest extends TestCase
{
    /**
     * @dataProvider typesProvider
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    public function testBasetypes(string $type): void
    {
        $basetype = (new Teradata($type))->getBasetype();

        switch (strtoupper($type)) {
            case 'BYTEINT':
            case 'INTEGER':
            case 'INT':
            case 'BIGINT':
            case 'SMALLINT':
                self::assertSame(BaseType::INTEGER, $basetype);
                break;
            case 'DECIMAL':
            case 'DEC':
            case 'NUMERIC':
            case 'NUMBER':
                self::assertSame(BaseType::NUMERIC, $basetype);
                break;
            case 'FLOAT':
            case 'DOUBLE PRECISION':
            case 'REAL':
                self::assertSame(BaseType::FLOAT, $basetype);
                break;
            case 'DATE':
                self::assertSame(BaseType::DATE, $basetype);
                break;
            case 'TIME':
            case 'TIME_WITH_ZONE':
            case 'TIMESTAMP':
            case 'TIMESTAMP_WITH_ZONE':
                self::assertSame(BaseType::TIMESTAMP, $basetype);
                break;
            default:
                self::assertSame(BaseType::STRING, $basetype);
                break;
        }
    }

    /**
     * @return Generator<int, array<string,string>>
     */
    public function typesProvider(): Generator
    {
        foreach (Teradata::TYPES as $type) {
            yield [$type => $type];
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
    public function testInvalidLengths(string $type, $length, ?array $extraOption = []): void
    {
        $options = $extraOption;
        $options['length'] = $length;

        $this->expectException(InvalidLengthException::class);
        new Teradata($type, $options);
    }

    public function testInvalidOption(): void
    {
        try {
            new Teradata('numeric', ['myoption' => 'value']);
            self::fail('Exception not caught');
        } catch (Throwable $e) {
            self::assertEquals(InvalidOptionException::class, get_class($e));
        }
    }

    public function testInvalidType(): void
    {
        $this->expectException(InvalidTypeException::class);
        new Teradata('UNKNOWN');
    }

    /**
     * @dataProvider expectedSqlDefinitions
     */
    public function testSqlDefinition(string $type, ?array $options, string $expectedDefinition): void
    {
        $definition = new Teradata($type, $options);
        $suffix = in_array($type, Teradata::CHARACTER_SET_TYPES) ? ' CHARACTER SET UNICODE' : '';
        self::assertEquals($expectedDefinition . $suffix, $definition->getSQLDefinition());
    }

    /**
     * @dataProvider expectedSqlDefinitions
     * @param array<mixed>|null $options
     */
    public function testBuildTypeWithLength(string $type, ?array $options, string $expectedDefinition): void
    {
        $definition = new Teradata($type, $options);
        self::assertEquals($expectedDefinition, $definition->buildTypeWithLength());
    }


    public function testSqlDefinitionWhenLatin(): void
    {
        $definition = new Teradata('VARCHAR', ['isLatin' => true]);
        self::assertEquals('VARCHAR (64000) CHARACTER SET LATIN', $definition->getSQLDefinition());
    }

    /**
     * @return array<string, array<array{}|string>>
     */
    public function expectedSqlDefinitions(): array
    {
        return [
            'BYTEINT' => ['BYTEINT', [], 'BYTEINT'],
            'BIGINT' => ['BIGINT', [], 'BIGINT'],
            'SMALLINT' => ['SMALLINT', [], 'SMALLINT'],
            'INTEGER' => ['INTEGER', [], 'INTEGER'],
            'INT' => ['INT', [], 'INT'],

            'DECIMAL' => ['DECIMAL', [], 'DECIMAL (38,19)'],
            'NUMERIC' => ['NUMERIC', [], 'NUMERIC (38,19)'],
            'DEC' => ['DEC', [], 'DEC (38,19)'],
            'FLOAT' => ['FLOAT', [], 'FLOAT'],
            'DOUBLE PRECISION' => ['DOUBLE PRECISION', [], 'DOUBLE PRECISION'],
            'REAL' => ['REAL', [], 'REAL'],
            'NUMBER' => ['NUMBER', [], 'NUMBER (38,19)'],
            'BYTE' => ['BYTE', [], 'BYTE (64000)'],
            'VARBYTE' => ['VARBYTE', [], 'VARBYTE (64000)'],
            'BLOB' => ['BLOB', [], 'BLOB (1G)'],
            'BINARY LARGE OBJECT' => ['BINARY LARGE OBJECT', [], 'BINARY LARGE OBJECT (1G)'],
            'DATE' => ['DATE', [], 'DATE'],
            'TIME' => ['TIME', [], 'TIME (6)'],
            'TIMESTAMP' => ['TIMESTAMP', [], 'TIMESTAMP (6)'],
            'TIME_WITH_ZONE' => ['TIME_WITH_ZONE', [], 'TIME (6) WITH TIME ZONE'],
            'TIMESTAMP_WITH_ZONE' => ['TIMESTAMP_WITH_ZONE', [], 'TIMESTAMP (6) WITH TIME ZONE'],
            'CHAR' => ['CHAR', [], 'CHAR (32000)'],
            'CHARACTER' => ['CHARACTER', [], 'CHARACTER (32000)'],
            'VARCHAR' => ['VARCHAR', [], 'VARCHAR (32000)'],
            'CHAR VARYING' => ['CHAR VARYING', [], 'CHAR VARYING (32000)'],
            'CHARACTER VARYING' => ['CHARACTER VARYING', [], 'CHARACTER VARYING (32000)'],
            'VARGRAPHIC' => ['VARGRAPHIC', [], 'VARGRAPHIC (32000)'],
            'LONG VARCHAR' => ['LONG VARCHAR', [], 'LONG VARCHAR'],
            'LONG VARGRAPHIC' => ['LONG VARGRAPHIC', [], 'LONG VARGRAPHIC'],
            'CLOB' => ['CLOB', [], 'CLOB (999M)'],
            'CHARACTER LARGE OBJECT' => ['CHARACTER LARGE OBJECT', [], 'CHARACTER LARGE OBJECT (999M)'],
            'PERIOD(DATE)' => ['PERIOD(DATE)', [], 'PERIOD(DATE)'],
            'PERIOD(TIME)' => ['PERIOD(TIME)', [], 'PERIOD(TIME (6))'],
            'PERIOD(TIMESTAMP)' => ['PERIOD TIMESTAMP', [], 'PERIOD(TIMESTAMP (6))'],
            'PERIOD(TIME WITH TIME ZONE' => ['PERIOD TIME WITH_ZONE', [], 'PERIOD(TIME (6) WITH TIME ZONE)'],
            'PERIOD(TIMESTAMP WITH TIME ZONE' => [
                'PERIOD TIMESTAMP WITH_ZONE',
                [],
                'PERIOD(TIMESTAMP (6) WITH TIME ZONE)',
            ],
            'INTERVAL SECOND' => ['INTERVAL SECOND', [], 'INTERVAL SECOND (4,6)'],
            'INTERVAL MINUTE' => ['INTERVAL MINUTE', [], 'INTERVAL MINUTE (4)'],
            'INTERVAL MINUTE TO SECOND' => ['INTERVAL MINUTE TO SECOND', [], 'INTERVAL MINUTE (4) TO SECOND (6)'],
            'INTERVAL HOUR' => ['INTERVAL HOUR', [], 'INTERVAL HOUR (4)'],
            'INTERVAL HOUR TO SECOND' => ['INTERVAL HOUR TO SECOND', [], 'INTERVAL HOUR (4) TO SECOND (6)'],
            'INTERVAL HOUR TO MINUTE' => ['INTERVAL HOUR TO MINUTE', [], 'INTERVAL HOUR (4) TO MINUTE'],
            'INTERVAL DAY' => ['INTERVAL DAY', [], 'INTERVAL DAY (4)'],
            'INTERVAL DAY TO SECOND' => ['INTERVAL DAY TO SECOND', [], 'INTERVAL DAY (4) TO SECOND (6)'],
            'INTERVAL DAY TO MINUTE' => ['INTERVAL DAY TO MINUTE', [], 'INTERVAL DAY (4) TO MINUTE'],
            'INTERVAL DAY TO HOUR' => ['INTERVAL DAY TO HOUR', [], 'INTERVAL DAY (4) TO HOUR'],
            'INTERVAL MONTH' => ['INTERVAL MONTH', [], 'INTERVAL MONTH (4)'],
            'INTERVAL YEAR' => ['INTERVAL YEAR', [], 'INTERVAL YEAR (4)'],
            'INTERVAL YEAR TO MONTH' => ['INTERVAL YEAR TO MONTH', [], 'INTERVAL YEAR (4) TO MONTH'],
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
    public function testValidLengths(string $type, $length, ?array $extraOptions = []): void
    {
        $options = $extraOptions;
        $options['length'] = $length;
        new Teradata($type, $options);
        $this->expectNotToPerformAssertions();
    }

    /**
     * @return array<int, mixed[]>
     */
    public function validLengths(): array
    {
        return [
            ['BYTEINT', null],
            ['BIGINT', null],
            ['SMALLINT', null],
            ['INTEGER', null],
            ['INT', null],
            ['DECIMAL', null],
            ['NUMERIC', null],
            ['DEC', null],
            ['DECIMAL', '24,24'],
            ['FLOAT', null],
            ['DOUBLE PRECISION', null],
            ['REAL', null],
            ['NUMBER', null],
            ['NUMBER', '24,24'],
            // comes from HELP TABLE
            ['NUMBER', '-128,-128'],
            ['BYTE', null],
            ['BYTE', '5000'],
            ['VARBYTE', null],
            ['BLOB', null],
            ['BLOB', '10K'],
            ['BLOB', '20M'],
            ['BLOB', '200000'],
            ['BINARY LARGE OBJECT', null],
            ['DATE', null],
            ['TIME', null],
            ['TIME', '5'],
            ['TIMESTAMP', '5'],
            ['TIMESTAMP', null],
            ['TIME_WITH_ZONE', null],
            ['TIME_WITH_ZONE', '6'],
            ['TIMESTAMP_WITH_ZONE', null],
            ['TIMESTAMP_WITH_ZONE', '6'],
            ['CHAR', null],
            ['CHAR', '20000', ['isLatin' => false]],
            ['CHAR', '50000', ['isLatin' => true]],
            ['CHARACTER', null],
            ['VARCHAR', null],
            ['CHAR VARYING', null],
            ['CHARACTER VARYING', null],
            ['VARGRAPHIC', null],
            ['LONG VARCHAR', null],
            ['LONG VARGRAPHIC', null],
            ['CLOB', null],
            ['CLOB', '10000'],
            ['CLOB', '10K'],
            ['CLOB', '20M'],
            ['CLOB', '20     M'],
            ['CLOB', '1G', ['isLatin' => true]],
            ['CHARACTER LARGE OBJECT', null],
            ['PERIOD(DATE)', null],
            ['PERIOD(TIME)', null],
            ['PERIOD(TIME)', '5'],
            ['PERIOD TIMESTAMP', null],
            ['PERIOD TIMESTAMP', '5'],
            ['PERIOD TIME WITH_ZONE', null],
            ['PERIOD TIME WITH_ZONE', '5'],
            ['PERIOD TIMESTAMP WITH_ZONE', null],
            ['PERIOD TIMESTAMP WITH_ZONE', '5'],
            ['INTERVAL SECOND', null],
            ['INTERVAL SECOND', '4,6'],
            ['INTERVAL SECOND', '4'],
            ['INTERVAL MINUTE', null],
            ['INTERVAL MINUTE', '4'],
            ['INTERVAL MINUTE TO SECOND', '4,5'],
            ['INTERVAL MINUTE TO SECOND', '4'],
            ['INTERVAL HOUR', null],
            ['INTERVAL HOUR', '4'],
            ['INTERVAL HOUR TO SECOND', null],
            ['INTERVAL HOUR TO SECOND', '4,5'],
            ['INTERVAL HOUR TO SECOND', '4'],
            ['INTERVAL HOUR TO MINUTE', null],
            ['INTERVAL HOUR TO MINUTE', '4'],
            ['INTERVAL DAY', null],
            ['INTERVAL DAY', '4'],
            ['INTERVAL DAY TO SECOND', null],
            ['INTERVAL DAY TO SECOND', '4,5'],
            ['INTERVAL DAY TO SECOND', '4'],
            ['INTERVAL DAY TO MINUTE', null],
            ['INTERVAL DAY TO MINUTE', '4'],
            ['INTERVAL DAY TO HOUR', null],
            ['INTERVAL DAY TO HOUR', '4'],
            ['INTERVAL MONTH', null],
            ['INTERVAL YEAR', null],
            ['INTERVAL YEAR', '4'],
            ['INTERVAL YEAR TO MONTH', null],
            ['INTERVAL YEAR TO MONTH', '4'],
        ];
    }

    /**
     * @return array<int, mixed[]>
     */
    public function invalidLengths(): array
    {
        return [
            ['DECIMAL', '100'],
            ['NUMBER', '24,100'],
            ['BYTE', '555000'],
            ['BLOB', '20G'],
            ['BLOB', '200000000000000'],
            ['TIME', '8'],
            ['TIMESTAMP', '8'],
            ['TIME_WITH_ZONE', '8'],
            ['TIMESTAMP_WITH_ZONE', '8'],
            ['CHAR', '1000000'],
            ['CHAR', '50000', ['isLatin' => false]],
            ['CHAR', '500000', ['isLatin' => true]],
            ['CLOB', '20 G'],
            ['CLOB', '20F'],
            ['CLOB', '1G', ['isLatin' => false]],
            ['PERIOD(TIME)', '7'],
            ['PERIOD TIMESTAMP', '8'],
            ['PERIOD TIME WITH_ZONE', '8'],
            ['PERIOD TIMESTAMP WITH_ZONE', '8'],
            ['INTERVAL SECOND', '5,6'],
            ['INTERVAL SECOND', '5'],
            ['INTERVAL MINUTE', '5'],
            ['INTERVAL MINUTE TO SECOND', '4,9'],
            ['INTERVAL MINUTE TO SECOND', '5'],
            ['INTERVAL HOUR', '6'],
            ['INTERVAL HOUR TO SECOND', '4,8'],
            ['INTERVAL HOUR TO SECOND', '5'],
            ['INTERVAL HOUR TO MINUTE', '5'],
            ['INTERVAL DAY', '6'],
            ['INTERVAL DAY TO SECOND', '4,8'],
            ['INTERVAL DAY TO SECOND', '8'],
            ['INTERVAL DAY TO MINUTE', '5'],
            ['INTERVAL DAY TO HOUR', '5'],
            ['INTERVAL YEAR', '5'],
            ['INTERVAL YEAR TO MONTH', '5'],
        ];
    }
}
