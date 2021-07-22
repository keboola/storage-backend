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
                self::assertSame(BaseType::BOOLEAN, $basetype);
                break;
            case 'DECIMAL':
                self::assertSame(BaseType::NUMERIC, $basetype);
                break;
//                self::assertSame(BaseType::FLOAT, $basetype);
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
            'CHAR' => ['CHAR', [], 'CHAR (2000)'],
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

    public function validLengths()
    {
        return [
            ['CHAR', 100],
        ];
    }

    public function invalidLengths()
    {
        return [
            ['CHAR', '3000'],

        ];
    }
}
