<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use Generator;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
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

    /**
     * @dataProvider invalidLengthsProvider
     */
    public function testInvalidLengths(string $type, int|string|null $length): void
    {
        $this->expectException(InvalidLengthException::class);
        new Oracle($type, ['length' => $length]);
    }

    /**
     * @return \Generator<string, array<string, int|string>>
     */
    public function invalidLengthsProvider(): Generator
    {
        yield Oracle::TYPE_CHAR => ['type' => Oracle::TYPE_CHAR, 'length' => 2500];
        yield Oracle::TYPE_RAW => ['type' => Oracle::TYPE_RAW, 'length' => 2500];
        yield Oracle::TYPE_NUMBER => ['type' => Oracle::TYPE_NUMBER, 'length' => '40,2'];
    }

    public function testBasetypes(): void
    {
        foreach (Oracle::TYPES as $type) {
            $datatype = new Oracle($type);
            $basetype = $datatype->getBasetype();
            $this->assertContains($basetype, ['string', 'numeric', 'datetime', 'binary']);
        }
    }

    public function testGetTypeByBasetype(): void
    {
        $this->assertEquals(Oracle::TYPE_VARCHAR2, Oracle::getTypeByBasetype('string'));
        $this->assertEquals(Oracle::TYPE_NUMBER, Oracle::getTypeByBasetype('numeric'));
        $this->assertEquals(Oracle::TYPE_DATE, Oracle::getTypeByBasetype('datetime'));
        $this->assertEquals(Oracle::TYPE_BLOB, Oracle::getTypeByBasetype('binary'));
    }

    public function testGetSQLDefinition(): void
    {
        $datatype = new Oracle(Oracle::TYPE_VARCHAR2, ['length' => 100, 'nullable' => false, 'default' => 'test']);
        $this->assertEquals('VARCHAR2(100) NOT NULL DEFAULT test', $datatype->getSQLDefinition());
    }
}
