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
     * @param string $type
     * @param string|null $length
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
     * @param string $type
     * @param array|null $options
     * @param string $expectedDefinition
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
            'basic integer' => ['INTEGER', [], 'INTEGER'],
        ];
    }

    /**
     * @dataProvider validLengths
     * @param string $type
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
