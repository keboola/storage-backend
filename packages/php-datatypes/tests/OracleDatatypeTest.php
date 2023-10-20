<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use Generator;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use Keboola\Datatype\Definition\Oracle;
use PHPUnit\Framework\TestCase;

class OracleDatatypeTest extends TestCase
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

    public function testGetTypeByBasetype(): void
    {
        foreach (BaseType::TYPES as $basetype) {
            $type = Oracle::getTypeByBasetype($basetype);
            $this->assertContains($type, Oracle::TYPES);
        }
    }

    public function testGetSQLDefinition(): void
    {
        $datatype = new Oracle(Oracle::TYPE_VARCHAR2, ['length' => 100, 'nullable' => false, 'default' => 'test']);
        $this->assertEquals('VARCHAR2(100) NOT NULL DEFAULT test', $datatype->getSQLDefinition());
    }
}
