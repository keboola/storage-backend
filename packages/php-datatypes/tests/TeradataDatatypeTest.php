<?php

namespace Keboola\DatatypeTest;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\Teradata;

class TeradataDatatypeTest extends \PHPUnit_Framework_TestCase
{
    public function invalidLengths()
    {
        return [
            ['datetime', 'anyLength'],
        ];
    }

    public function testBasetypes()
    {
        //        TODO
    }

    /**
     * //     * @dataProvider invalidLengths
     *
     * @param  string $type
     * @param  string|null $length
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    //    public function testInvalidLengths($type, $length)
    public function testInvalidLengths()
    {
        // TODO
    }

    public function testInvalidOption()
    {
        try {
            new Teradata('numeric', ['myoption' => 'value']);
            self::fail('Exception not caught');
        } catch (\Exception $e) {
            self::assertEquals(InvalidOptionException::class, get_class($e));
        }
    }

    public function testInvalidType()
    {
        $this->expectException(InvalidTypeException::class);
        new Teradata('UNKNOWN');
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
        $definition = new Teradata($type, $options);
        self::assertEquals($expectedDefinition, $definition->getSQLDefinition());
    }

    public function expectedSqlDefinitions()
    {
        // TODO more types
        return [
            'BYTEINT' => ['BYTEINT', [], 'BYTEINT'],
            'BIGINT' => ['BIGINT', [], 'BIGINT'],
            'SMALLINT' => ['SMALLINT', [], 'SMALLINT'],
            'INTEGER' => ['INTEGER', [], 'INTEGER'],
            'INT' => ['INT', [], 'INT'],

            'DECIMAL' => ['DECIMAL', [], 'DECIMAL (38,38)'],
            'NUMERIC' => ['NUMERIC', [], 'NUMERIC (38,38)'],
            'DEC' => ['DEC', [], 'DEC (38,38)'],
            'FLOAT' => ['FLOAT', [], 'FLOAT'],
            'DOUBLE PRECISION' => ['DOUBLE PRECISION', [], 'DOUBLE PRECISION'],
            'REAL' => ['REAL', [], 'REAL'],
            'NUMBER' => ['NUMBER', [], 'NUMBER (38,38)'],
            'BYTE' => ['BYTE', [], 'BYTE (64000)'],
            'VARBYTE' => ['VARBYTE', [], 'VARBYTE (64000)'],
            'BLOB' => ['BLOB', [], 'BLOB (1G)'],
            'BINARY LARGE OBJECT' => ['BINARY LARGE OBJECT', [], 'BINARY LARGE OBJECT (1G)'],
            'DATE' => ['DATE', [], 'DATE'],
            'TIME' => ['TIME', [], 'TIME (6)'],
            'TIMESTAMP' => ['TIMESTAMP', [], 'TIMESTAMP (6)'],
            'TIME_WITH_ZONE' => ['TIME_WITH_ZONE', [], 'TIME (6) WITH TIME ZONE'],
            'TIMESTAMP_WITH_ZONE' => ['TIMESTAMP_WITH_ZONE', [], 'TIMESTAMP (6) WITH TIME ZONE'],
            'CHAR' => ['CHAR', [], 'CHAR (64000)'],
            'CHARACTER' => ['CHARACTER', [], 'CHARACTER (64000)'],
            'VARCHAR' => ['VARCHAR', [], 'VARCHAR (64000)'],
            'CHAR VARYING' => ['CHAR VARYING', [], 'CHAR VARYING (64000)'],
            'CHARACTER VARYING' => ['CHARACTER VARYING', [], 'CHARACTER VARYING (64000)'],
            'VARGRAPHIC' => ['VARGRAPHIC', [], 'VARGRAPHIC (64000)'],
            'LONG VARCHAR' => ['LONG VARCHAR', [], 'LONG VARCHAR'],
            'LONG VARGRAPHIC' => ['LONG VARGRAPHIC', [], 'LONG VARGRAPHIC'],
            'CLOB' => ['CLOB', [], 'CLOB (1G)'],
            'CHARACTER LARGE OBJECT' => ['CHARACTER LARGE OBJECT', [], 'CHARACTER LARGE OBJECT (1G)'],
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
     * @param string      $type
     * @param string|null $length
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    public function testValidLengths($type, $length)
    {
        $options = [];
        if ($length !== null) {
            $options['length'] = $length;
        }
        new Teradata($type, $options);
    }

    public function validLengths()
    {
        // TODO more types
        return [
            ['INTEGER', null],
        ];
    }
}

