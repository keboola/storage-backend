<?php

namespace Keboola\DatatypeTest;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Exasol;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;

class ExasolDatatypeTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @dataProvider typesProvider
     * @param $type
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    public function testBasetypes($type)
    {
        $basetype = (new Exasol($type))->getBasetype();

        switch (strtoupper($type)) {
            case 'BOOLEAN':
            case 'BOOL':
                self::assertSame(BaseType::BOOLEAN, $basetype);
                break;
            case 'DEC':
            case 'DECIMAL':
            case 'NUMERIC':
                self::assertSame(BaseType::NUMERIC, $basetype);
                break;
            case 'INT':
            case 'INTEGER':
            case 'SHORTINT':
            case 'SMALLINT':
            case 'TINYINT':
            case 'BIGINT':
                self::assertSame(BaseType::INTEGER, $basetype);
                break;
            case 'FLOAT':
            case 'REAL':
            case 'DOUBLE':
            case 'DOUBLE PRECISION':
            case 'NUMBER':
                self::assertSame(BaseType::FLOAT, $basetype);
                break;
            case 'DATE':
                self::assertSame(BaseType::DATE, $basetype);
                break;
            case 'TIMESTAMP':
            case 'TIMESTAMP WITH LOCAL TIME ZONE':
                self::assertSame(BaseType::TIMESTAMP, $basetype);
                break;
            default:
                self::assertSame(BaseType::STRING, $basetype);
                break;
        }
    }

    public function testDefaultTypeForNumberBasedOnLength()
    {
        self::assertSame(
            BaseType::NUMERIC,
            (new Exasol('NUMBER', ['length' => '10,5']))->getBasetype()
        );
        self::assertSame(
            BaseType::FLOAT,
            (new Exasol('NUMBER', ['length' => null]))->getBasetype()
        );
    }

    public function typesProvider()
    {
        foreach (Exasol::TYPES as $type) {
            yield [$type => $type];
        }
    }

    /**
     * @dataProvider invalidLengths
     *
     * @param string $type
     * @param string|null $length
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    public function testInvalidLengths($type, $length, $extraOption = [])
    {
        $options = $extraOption;
        $options['length'] = $length;

        $this->expectException(InvalidLengthException::class);
        new Exasol($type, $options);
    }

    public function testInvalidOption()
    {
        try {
            new Exasol('numeric', ['myoption' => 'value']);
            self::fail('Exception not caught');
        } catch (\Exception $e) {
            self::assertEquals(InvalidOptionException::class, get_class($e));
        }
    }

    public function testInvalidType()
    {
        $this->expectException(InvalidTypeException::class);
        new Exasol('UNKNOWN');
    }

    /**
     * @param string     $type
     * @param array|null $options
     * @param string     $expectedDefinition
     *
     * @dataProvider expectedSqlDefinitions
     */
    public function testSqlDefinition($type, $options, $expectedDefinition)
    {
        $definition = new Exasol($type, $options);
        self::assertEquals($expectedDefinition, $definition->getSQLDefinition());
    }

    public function expectedSqlDefinitions()
    {
        return [
            'DECIMAL' => ['DECIMAL', [], 'DECIMAL (36,36)'],
            'DEC' => ['DEC', [], 'DEC (36,36)'],
            'NUMBER' => ['NUMBER', [], 'DOUBLE PRECISION'],
            'NUMBER WITH LENGTH' => ['NUMBER', ['length' => '20,20'], 'NUMBER (20,20)'],
            'NUMERIC' => ['NUMERIC', [], 'NUMERIC (36,36)'],

            'CHAR' => ['CHAR', [], 'CHAR (2000)'],
            'NCHAR' => ['NCHAR', [], 'NCHAR (2000)'],

            'VARCHAR' => ['VARCHAR', [], 'VARCHAR (2000000)'],
            'CHAR VARYING' => ['CHAR VARYING', [], 'CHAR VARYING (2000000)'],
            'CHARACTER LARGE OBJECT' => ['CHARACTER LARGE OBJECT', [], 'CHARACTER LARGE OBJECT (2000000)'],
            'CHARACTER VARYING' => ['CHARACTER VARYING', [], 'CHARACTER VARYING (2000000)'],
            'CLOB' => ['CLOB', [], 'CLOB (2000000)'],
            'NVARCHAR' => ['NVARCHAR', [], 'NVARCHAR (2000000)'],
            'NVARCHAR2' => ['NVARCHAR2', [], 'NVARCHAR2 (2000000)'],
            'VARCHAR2' => ['VARCHAR2', [], 'VARCHAR2 (2000000)'],
            'GEOMETRY' => ['GEOMETRY', [], 'GEOMETRY (4294967295)'],
            'HASHTYPE' => ['HASHTYPE', [], 'HASHTYPE (1024 BYTE)'],
            'BOOLEAN' => ['BOOLEAN', [], 'BOOLEAN'],
            'BOOL' => ['BOOL', [], 'BOOL'],
            'DATE' => ['DATE', [], 'DATE'],
            'TIMESTAMP' => ['TIMESTAMP', [], 'TIMESTAMP'],
            'TIMESTAMP WITH LOCAL TIME ZONE' => [
                'TIMESTAMP WITH LOCAL TIME ZONE',
                [],
                'TIMESTAMP WITH LOCAL TIME ZONE',
            ],
            'BIGINT' => ['BIGINT', [], 'BIGINT'],
            'INT' => ['INT', [], 'INT'],
            'INTEGER' => ['INTEGER', [], 'INTEGER'],
            'SHORTINT' => ['SHORTINT', [], 'SHORTINT'],
            'TINYINT' => ['TINYINT', [], 'TINYINT'],
            'DOUBLE' => ['DOUBLE', [], 'DOUBLE'],
            'FLOAT' => ['FLOAT', [], 'FLOAT'],
            'REAL' => ['REAL', [], 'REAL'],
        ];
    }

    /**
     * @dataProvider validLengths
     * @param string      $type
     * @param string|null $length
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    public function testValidLengths($type, $length, $extraOptions = [])
    {
        $options = $extraOptions;
        $options['length'] = $length;
        new Exasol($type, $options);
    }

    public function invalidLengths()
    {
        $out = [];
        $combinations = [
            'decimals' => [
                'lengths' => ['10,', '50', '-1', '40,50'],
                'types' => [
                    'DECIMAL',
                    'DEC',
                    'NUMBER',
                    'NUMERIC',
                ],
            ],
            'chars' => [
                'lengths' => [-1, 4000],
                'types' => [
                    'CHAR',
                    'NCHAR',
                ],
            ],
            'varchars' => [
                'lengths' => [-1, 5000000],
                'types' => [
                    'VARCHAR',
                    'CHAR VARYING',
                    'CHARACTER LARGE OBJECT',
                    'CHARACTER VARYING',
                    'CLOB',
                    'NVARCHAR',
                    'NVARCHAR2',
                    'VARCHAR2',
                ],
            ],
        ];

        foreach ($combinations as $definitions) {
            foreach ($definitions['types'] as $type) {
                foreach ($definitions['lengths'] as $length) {
                    $out["$type-$length"] = [$type, $length];
                }
            }
        }
        $out['GEOMETRY--1000'] = ['GEOMETRY', '-1000'];
        $out['GEOMETRY-50000xxx'] = ['GEOMETRY', '5000000000'];

        $out['HASHTYPE-1BYTE'] = ['HASHTYPE', '0BYTE'];
        $out['HASHTYPE-2000     BYTE'] = ['HASHTYPE', '2000     BYTE'];
        $out['HASHTYPE-2000'] = ['HASHTYPE', '2000'];
        $out['HASHTYPE-100B'] = ['HASHTYPE', '100B'];

        $out['HASHTYPE-5 BIT'] = ['HASHTYPE', '5 BIT'];
        $out['HASHTYPE-100b'] = ['HASHTYPE', '100b'];
        $out['HASHTYPE-10000 BIT'] = ['HASHTYPE', '10000 BIT'];


        $out['INTERVAL_YEAR TO MONTH 12'] = ['INTERVAL_YEAR TO MONTH', '12'];
        $out['INTERVAL DAY TO SECOND 0 0'] = ['INTERVAL DAY TO SECOND', '0,0'];

        return $out;
    }

    public function validLengths()
    {
        $out = [];
        $combinations = [
            'decimals' => [
                'lengths' => [null, '20,20', '20,10', '10', '8', '1', '36,36'],
                'types' => [
                    'DECIMAL',
                    'DEC',
                    'NUMBER',
                    'NUMERIC',
                ],
            ],
            'chars' => [
                'lengths' => [null, 1, 20, 200],
                'types' => [
                    'CHAR',
                    'NCHAR',
                ],
            ],
            'varchars' => [
                'lengths' => [null, 1, 1000, 2000000],
                'types' => [
                    'VARCHAR',
                    'CHAR VARYING',
                    'CHARACTER LARGE OBJECT',
                    'CHARACTER VARYING',
                    'CLOB',
                    'NVARCHAR',
                    'NVARCHAR2',
                    'VARCHAR2',
                ],
            ],
        ];

        foreach ($combinations as $definitions) {
            foreach ($definitions['types'] as $type) {
                foreach ($definitions['lengths'] as $length) {
                    $out["$type-$length"] = [$type, $length];
                }
            }
        }
        $out['GEOMETRY-'] = ['GEOMETRY', null];
        $out['GEOMETRY-1000'] = ['GEOMETRY', '1000'];
        $out['GEOMETRY-40000'] = ['GEOMETRY', '40000'];

        $out['HASHTYPE-1BYTE'] = ['HASHTYPE', '1BYTE'];
        $out['HASHTYPE-256	BYTE'] = ['HASHTYPE', '256	BYTE'];
        $out['HASHTYPE-1024     BYTE'] = ['HASHTYPE', '1024     BYTE'];

        $out['HASHTYPE-1 BIT'] = ['HASHTYPE', '8 BIT'];
        $out['HASHTYPE-5000 BIT'] = ['HASHTYPE', '5000 BIT'];
        $out['HASHTYPE-8192 BIT'] = ['HASHTYPE', '8192 BIT'];

        $out['INTERVAL_YEAR TO MONTH null'] = ['INTERVAL_YEAR TO MONTH', null];
        $out['INTERVAL_YEAR TO MONTH 1'] = ['INTERVAL_YEAR TO MONTH', '1'];
        $out['INTERVAL_YEAR TO MONTH 9'] = ['INTERVAL_YEAR TO MONTH', '9'];

        $out['INTERVAL_YEAR TO MONTH null'] = ['INTERVAL_YEAR TO MONTH', null];
        $out['INTERVAL DAY TO SECOND 1 9'] = ['INTERVAL DAY TO SECOND', '1,9'];
        $out['INTERVAL DAY TO SECOND 9 0'] = ['INTERVAL DAY TO SECOND', '9,0'];

        return $out;
    }
}
