<?php
namespace Keboola\DataypeTest;

use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Redshift;
use Keboola\Datatype\Definition\Snowflake;

class CommonDatatypeTest extends \PHPUnit_Framework_TestCase
{
    public function testConstructor()
    {
        $datatype = new Snowflake("VARCHAR");
        $this->assertEquals("VARCHAR", $datatype->getType());
        $this->assertEquals("", $datatype->getLength());
        $this->assertEquals(true, $datatype->isNullable());

        $datatype = new Snowflake("VARCHAR", ["length" => "50", "nullable" => false]);
        $this->assertEquals("50", $datatype->getLength());
        $this->assertEquals(false, $datatype->isNullable());
    }

    public function testSQLDefinition()
    {
        $datatype = new Snowflake("VARCHAR");
        $this->assertEquals("VARCHAR", $datatype->getSQLDefinition());

        $datatype = new Snowflake("VARCHAR", ["length" => "50"]);
        $this->assertEquals("VARCHAR(50)", $datatype->getSQLDefinition());
    }

    public function testToArray()
    {
        $datatype = new Snowflake("VARCHAR");
        $this->assertEquals(
            ["type" => "VARCHAR", "length" => "", "nullable" => true, "default" => "NULL"],
            $datatype->toArray()
        );
        $datatype = new Snowflake("VARCHAR", ['length' => "50", 'nullable' => false]);
        $this->assertEquals(
            ["type" => "VARCHAR", "length" => "50", "nullable" => false, "default" => ""],
            $datatype->toArray()
        );
    }

    public function testToMetadata()
    {
        $datatype = new Snowflake("VARCHAR");

        $md = $datatype->toMetadata();
        foreach ($md as $mdat) {
            $this->assertArrayHasKey('key', $mdat);
            $this->assertArrayHasKey('value', $mdat);
            if ($mdat['key'] === "KBC.datatype.type") {
                $this->assertEquals('VARCHAR', $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.length') {
                $this->fail("unspecified length should not create metadata.");
            } else if ($mdat['key'] === 'KBC.datatype.nullable') {
                $this->assertEquals(true, $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.format') {
                $this->fail("unspecified format should not create metadata.");
            } else if ($mdat['key'] === 'KBC.datatype.default') {
                $this->assertEquals("NULL", $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.basetype') {
                $this->assertEquals('STRING', $mdat['value']);
            }
        }

        $datatype = new Snowflake("NUMERIC", [
            "length" => "10,0",
            "nullable" => false,
            "default" => "0"
        ]);
        $md = $datatype->toMetadata();
        foreach ($md as $mdat) {
            $this->assertArrayHasKey('key', $mdat);
            $this->assertArrayHasKey('value', $mdat);
            if ($mdat['key'] === "KBC.datatype.type") {
                $this->assertEquals('NUMERIC', $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.length') {
                $this->assertEquals("10,0", $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.nullable') {
                $this->assertEquals(false, $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.default') {
                $this->assertEquals("0", $mdat['value']);
            } else if ($mdat['key'] === 'KBC.datatype.basetype') {
                $this->assertEquals('NUMERIC', $mdat['value']);
            }
        }
    }

}
