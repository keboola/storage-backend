<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use Generator;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\Oracle;

class OracleDatatypeTest extends BaseDatatypeTestCase
{
    public function testInvalidType(): void
    {
        $this->expectException(InvalidTypeException::class);
        new Oracle('INVALID_TYPE');
    }

    public function testInvalidOption(): void
    {
        $this->expectException(InvalidOptionException::class);
        new Oracle(Oracle::TYPE_NUMBER, ['invalidOption' => 'value']);
    }

    public function testValidTypeAndDefaultOptions(): void
    {
        $datatype = new Oracle(Oracle::TYPE_NUMBER);
        $this->assertEquals(Oracle::TYPE_NUMBER, $datatype->toArray()['type']);
    }

    public function testBasetypes(): void
    {
        foreach (Oracle::TYPES as $type) {
            $datatype = new Oracle($type);
            $basetype = $datatype->getBasetype();
            $this->assertContains($basetype, BaseType::TYPES);
        }
    }

    public function testGetSQLDefinition(): void
    {
        $datatype = new Oracle(Oracle::TYPE_VARCHAR2, ['length' => 100, 'nullable' => false, 'default' => 'test']);
        $this->assertEquals('VARCHAR2(100) NOT NULL DEFAULT test', $datatype->getSQLDefinition());
    }

    public static function getTestedClass(): string
    {
        return Oracle::class;
    }

    public static function provideTestGetTypeByBasetype(): Generator
    {
        yield BaseType::STRING => [
            'basetype' => BaseType::STRING,
            'expectedType' => 'VARCHAR2',
        ];

        foreach ([BaseType::NUMERIC, BaseType::FLOAT, BaseType::BOOLEAN, BaseType::INTEGER] as $basetype) {
            yield $basetype => [
                'basetype' => $basetype,
                'expectedType' => 'NUMBER',
            ];
        }

        foreach ([BaseType::TIMESTAMP, BaseType::DATE] as $basetype) {
            yield $basetype => [
                'basetype' => $basetype,
                'expectedType' => 'DATE',
            ];
        }

        yield 'invalidBaseType' => [
            'basetype' => 'invalidBaseType',
            'expectedType' => null,
            'expectToFail' => true,
        ];
    }

    public static function provideTestGetDefinitionForBasetype(): Generator
    {
        yield BaseType::STRING => [
            'basetype' => BaseType::STRING,
            'expectedColumnDefinition' => new Oracle('VARCHAR2'),
        ];

        foreach ([BaseType::NUMERIC, BaseType::FLOAT, BaseType::BOOLEAN, BaseType::INTEGER] as $basetype) {
            yield $basetype => [
                'basetype' => $basetype,
                'expectedColumnDefinition' => new Oracle('NUMBER'),
            ];
        }

        foreach ([BaseType::TIMESTAMP, BaseType::DATE] as $basetype) {
            yield $basetype => [
                'basetype' => $basetype,
                'expectedColumnDefinition' => new Oracle('DATE'),
            ];
        }

        yield 'invalidBaseType' => [
            'basetype' => 'invalidBaseType',
            'expectedColumnDefinition' => null,
            'expectToFail' => true,
        ];
    }
}
