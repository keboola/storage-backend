<?php
namespace Keboola\DataypeTest;

use Keboola\Datatype\Definition\Common;

class CommonDatatypeTest extends \PHPUnit_Framework_TestCase
{
    public function testConstructor()
    {
        $datatype = new Common("VARCHAR");
        $this->assertEquals("VARCHAR", $datatype->getType());
        $this->assertEquals("", $datatype->getLength());
        $this->assertEquals(true, $datatype->isNullable());

        $datatype = new Common("VARCHAR", "50", false);
        $this->assertEquals("50", $datatype->getLength());
        $this->assertEquals(false, $datatype->isNullable());
    }

    public function testSQLDefinition()
    {
        $datatype = new Common("VARCHAR");
        $this->assertEquals("VARCHAR", $datatype->getSQLDefinition());

        $datatype = new Common("VARCHAR", "50", true);
        $this->assertEquals("VARCHAR(50)", $datatype->getSQLDefinition());
    }

    public function testToArray()
    {
        $datatype = new Common("VARCHAR");
        $this->assertEquals(["type" => "VARCHAR", "length" => "", "nullable" => true], $datatype->toArray());

        $datatype = new Common("VARCHAR", "50", true);
        $this->assertEquals(["type" => "VARCHAR", "length" => "50", "nullable" => true], $datatype->toArray());
    }

    public function testToMetadata()
    {
        $datatype = new Common("VARCHAR");

        $md = $datatype->toMetadata();
        foreach ($md as $mdat) {
            $this->assertArrayHasKey('key', $mdat);
            $this->assertArrayHasKey('value', $mdat);
            if ($mdat['key'] === "KBC.datatype.type") {
                $this->assertEquals('VARCHAR', $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.length') {
                $this->fail("unspecified length should not create metadata.");
            } else if ($mdat['key'] === 'KBC.datatype.nullable') {
                $this->assertEquals(false, $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.format') {
                $this->fail("unspecified format should not create metadata.");
            } else if ($mdat['key'] === 'KBC.datatype.default') {
                $this->assertEquals("", $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.basetype') {
                $this->assertEquals('STRING', $mdat['value']);
            }
        }

        $datatype = new Common("DATE", "10", false, "1970-01-01", "Y-m-d");
        $md = $datatype->toMetadata();
        foreach ($md as $mdat) {
            $this->assertArrayHasKey('key', $mdat);
            $this->assertArrayHasKey('value', $mdat);
            if ($mdat['key'] === "KBC.datatype.type") {
                $this->assertEquals('DATE', $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.length') {
                $this->assertEquals("10", $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.nullable') {
                $this->assertEquals(false, $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.format') {
                $this->assertEquals("Y-m-d", $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.default') {
                $this->assertEquals("1970-01-01", $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.basetype') {
                $this->assertEquals('DATE', $mdat['value']);
            }
        }
    }

    public function testGetBasetype() {
        $this->assertEquals("DATE", (new Common("date"))->getBasetype());
        $this->assertEquals("DATE", (new Common("DATE"))->getBasetype());
        $this->assertEquals("TIMESTAMP", (new Common("DateTime"))->getBasetype());
        $this->assertEquals("TIMESTAMP", (new Common("timestamp"))->getBasetype());
        $this->assertEquals("STRING", (new Common("dattim"))->getBasetype());

        $this->assertEquals("INTEGER", (new Common("int16"))->getBasetype());
        $this->assertEquals("INTEGER", (new Common("int"))->getBasetype());
        $this->assertEquals("INTEGER", (new Common("INTEGER"))->getBasetype());
        $this->assertEquals("FLOAT", (new Common("float8"))->getBasetype());
        $this->assertEquals("FLOAT", (new Common("REAL"))->getBasetype());
        $this->assertEquals("FLOAT", (new Common("double percision"))->getBasetype());
        $this->assertEquals("NUMERIC", (new Common("number"))->getBasetype());
        $this->assertEquals("NUMERIC", (new Common("DECIMAL"))->getBasetype());
        $this->assertEquals("NUMERIC", (new Common("numeric"))->getBasetype());

        $this->assertEquals("BOOLEAN", (new Common("BOOL"))->getBasetype());
        $this->assertEquals("BOOLEAN", (new Common("boolean"))->getBasetype());

        $this->assertEquals("STRING", (new Common("anythingelse"))->getBasetype());
    }
}
